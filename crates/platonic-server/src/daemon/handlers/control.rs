use crate::{
    AppError, AppResult,
    config::{Config, ProviderKind},
    daemon::{
        protocol::{
            CAPABILITIES, DaemonStatusDaemon, DaemonStatusModel, DaemonStatusParams,
            DaemonStatusProviderKind, DaemonStatusResult, DaemonStatusSession,
            DaemonStatusTokenUsage, DaemonStatusTrust, DaemonStatusUsage,
            ERROR_DAEMON_SHUTTING_DOWN, ERROR_INTERNAL, ERROR_NOT_FOUND, ERROR_WORKSPACE_MISMATCH,
            ERROR_WORKSPACE_UNREGISTERED, Envelope, HelloParams, HelloResult, ProtocolResponse,
            ShutdownIfIdleResult, ShutdownIfIdleResultName,
        },
        runtime::{DaemonRuntime, ShutdownIfIdleDecision},
    },
    ledger::PersistedTokenUsage,
};
use std::path::{Path, PathBuf};

pub(super) fn handle_hello(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: HelloParams,
) -> Envelope {
    let request_root = match PathBuf::from(&params.workspace_root).canonicalize() {
        Ok(root) if root == runtime.paths.workspace_root => root,
        Ok(root) => {
            return Envelope::error(
                request.id,
                Some("hello".into()),
                ERROR_WORKSPACE_MISMATCH,
                format!(
                    "workspace_root mismatch: expected {}, got {}",
                    runtime.paths.workspace_root.display(),
                    root.display()
                ),
            );
        }
        Err(error) => {
            return Envelope::error(
                request.id,
                Some("hello".into()),
                ERROR_WORKSPACE_MISMATCH,
                format!("workspace_root cannot be resolved: {error}"),
            );
        }
    };
    let legacy_workspace_id = match crate::paths::workspace_id(&request_root) {
        Ok(workspace_id) => workspace_id,
        Err(error) => {
            return Envelope::error(
                request.id,
                Some("hello".into()),
                ERROR_WORKSPACE_MISMATCH,
                format!("workspace_id cannot be derived: {error}"),
            );
        }
    };
    if params.workspace_id != runtime.paths.workspace_id
        && params.workspace_id != legacy_workspace_id
    {
        return Envelope::error(
            request.id,
            Some("hello".into()),
            ERROR_WORKSPACE_MISMATCH,
            format!(
                "workspace_id mismatch: expected {}, got {}",
                runtime.paths.workspace_id, params.workspace_id
            ),
        );
    }

    let store = match runtime.paths.server_store() {
        Ok(store) => store,
        Err(error) => return store_error(request.id, "hello", error),
    };
    match store.workspace_by_root(&runtime.paths.workspace_root.to_string_lossy()) {
        Ok(Some(record))
            if record.id == runtime.paths.workspace_id
                && Path::new(&record.ledger_path) == runtime.paths.ledger_path => {}
        Ok(Some(_)) => {
            return Envelope::error(
                request.id,
                Some("hello".into()),
                ERROR_INTERNAL,
                "workspace runtime does not match its registry record",
            );
        }
        Ok(None) => {
            return Envelope::error(
                request.id,
                Some("hello".into()),
                ERROR_WORKSPACE_UNREGISTERED,
                format!(
                    "workspace is not registered: {}; run platonic workspace create <name> \"{}\"",
                    runtime.paths.workspace_root.display(),
                    runtime.paths.workspace_root.display()
                ),
            );
        }
        Err(error) => return store_error(request.id, "hello", error),
    }

    Envelope::typed_response(
        request.id,
        ProtocolResponse::Hello(HelloResult {
            daemon_version: platonic_protocol::PLATONIC_DIAGNOSTIC_IDENTITY.into(),
            workspace_id: runtime.paths.workspace_id.clone(),
            ledger_path: runtime.paths.ledger_path.to_string_lossy().into_owned(),
            capabilities: CAPABILITIES.to_vec(),
            daemon_scope: None,
        }),
    )
}

pub(super) fn handle_daemon_status(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: DaemonStatusParams,
) -> Envelope {
    match daemon_status(runtime, params) {
        Ok(status) => Envelope::typed_response(request.id, ProtocolResponse::DaemonStatus(status)),
        Err(error) => match error {
            AppError::SessionNotFound(session_id) => Envelope::error(
                request.id,
                Some("daemon.status".into()),
                ERROR_NOT_FOUND,
                format!("session not found: {session_id}"),
            ),
            _ => Envelope::error(
                request.id,
                Some("daemon.status".into()),
                ERROR_INTERNAL,
                "daemon status readback failed",
            ),
        },
    }
}

fn daemon_status(
    runtime: &DaemonRuntime,
    params: DaemonStatusParams,
) -> AppResult<DaemonStatusResult> {
    let config = Config::load(
        &runtime.paths.workspace_root,
        params.config_path.as_deref().map(Path::new),
    )?;
    let requested_session_id = params.session_id.clone();
    let persisted = match crate::ledger::default_sqlite_session_status(
        &runtime.paths.default_ledger(),
        requested_session_id.as_deref(),
    ) {
        Err(AppError::SessionNotFound(_))
            if requested_session_id
                .as_deref()
                .is_some_and(|session_id| runtime.has_runtime_session(session_id)) =>
        {
            None
        }
        result => result?,
    };
    let ledger_path = runtime.paths.ledger_path.to_string_lossy().into_owned();
    let (served_model, session, usage, trust) = match persisted {
        Some(status) => {
            let usage = DaemonStatusUsage {
                last_run: protocol_usage(status.last_run_usage),
                session: protocol_usage(status.session_usage),
            };
            let trust = DaemonStatusTrust {
                approval_granted_count: status.approval_granted_count,
                approval_denied_count: status.approval_denied_count,
                shell_session_grant: runtime.has_shell_session_grant(&status.session_id),
                approval_profile: runtime.approval_profile(&status.session_id),
            };
            let session = DaemonStatusSession {
                session_id: Some(status.session_id),
                latest_run_id: Some(status.latest_run_id),
                human_turn_count: status.human_turn_count,
                ledger_path,
                core_event_count: status.core_event_count,
            };
            (status.served_model, session, usage, trust)
        }
        None => {
            let live_session_id =
                requested_session_id.filter(|session_id| runtime.has_runtime_session(session_id));
            let trust =
                live_session_id
                    .as_deref()
                    .map_or_else(DaemonStatusTrust::default, |session_id| DaemonStatusTrust {
                        shell_session_grant: runtime.has_shell_session_grant(session_id),
                        approval_profile: runtime.approval_profile(session_id),
                        ..DaemonStatusTrust::default()
                    });
            (
                None,
                DaemonStatusSession {
                    session_id: live_session_id,
                    latest_run_id: None,
                    human_turn_count: 0,
                    ledger_path,
                    core_event_count: 0,
                },
                DaemonStatusUsage {
                    last_run: DaemonStatusTokenUsage::default(),
                    session: DaemonStatusTokenUsage::default(),
                },
                trust,
            )
        }
    };
    let (package_version, build_commit, build_date_utc) = build_identity_parts();
    let provider_kind = match config.provider.kind {
        ProviderKind::OpenAi => DaemonStatusProviderKind::OpenAi,
        ProviderKind::OpenRouter => DaemonStatusProviderKind::OpenRouter,
    };

    Ok(DaemonStatusResult {
        model: DaemonStatusModel {
            requested_alias: config.provider.model,
            served_model,
            provider_kind,
            key_present: std::env::var_os(&config.provider.api_key_env).is_some(),
        },
        daemon: DaemonStatusDaemon {
            package_version,
            build_commit,
            build_date_utc,
            uptime_ms: runtime.uptime_ms(),
            endpoint_path: runtime.paths.socket_path.to_string_lossy().into_owned(),
            workspace_id: runtime.paths.workspace_id.clone(),
        },
        session,
        usage,
        trust,
    })
}

fn protocol_usage(usage: PersistedTokenUsage) -> DaemonStatusTokenUsage {
    DaemonStatusTokenUsage {
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        unknown_response_count: usage.unknown_response_count,
    }
}

fn build_identity_parts() -> (String, Option<String>, Option<String>) {
    let package_version = platonic_protocol::PLATONIC_PRODUCT_VERSION.into();
    let build_commit = known_build_part(Some(platonic_protocol::PLATONIC_BUILD_COMMIT));
    let build_date_utc = known_build_part(Some(platonic_protocol::PLATONIC_BUILD_DATE));
    (package_version, build_commit, build_date_utc)
}

fn known_build_part(part: Option<&str>) -> Option<String> {
    part.filter(|part| *part != "unknown").map(str::to_owned)
}

pub(super) fn handle_shutdown_if_idle(runtime: &DaemonRuntime, request: Envelope) -> Envelope {
    match runtime.shutdown_if_idle() {
        ShutdownIfIdleDecision::Shutdown => Envelope::typed_response(
            request.id,
            ProtocolResponse::DaemonShutdownIfIdle(ShutdownIfIdleResult {
                result: ShutdownIfIdleResultName::Shutdown,
            }),
        ),
        ShutdownIfIdleDecision::RefusedActive => Envelope::typed_response(
            request.id,
            ProtocolResponse::DaemonShutdownIfIdle(ShutdownIfIdleResult {
                result: ShutdownIfIdleResultName::RefusedActive,
            }),
        ),
        ShutdownIfIdleDecision::AlreadyShuttingDown => {
            shutting_down_response(request.id, "daemon.shutdown_if_idle")
        }
    }
}

pub(super) fn shutting_down_response(request_id: Option<String>, method: &'static str) -> Envelope {
    Envelope::error(
        request_id,
        Some(method.into()),
        ERROR_DAEMON_SHUTTING_DOWN,
        "daemon shutdown is already in progress",
    )
}

pub(super) fn store_error(
    request_id: Option<String>,
    method: &'static str,
    error: AppError,
) -> Envelope {
    Envelope::error(
        request_id,
        Some(method.into()),
        ERROR_INTERNAL,
        error.to_string(),
    )
}
