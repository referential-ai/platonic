use crate::{
    AppError, AppResult, ApprovalMode, RunEvent, RunLedger, RunOptions, RunOutcome, RunSession,
    app::ExternalApprovalOutcome,
    config::{Config, ProviderKind},
    daemon::{
        protocol::{
            AgentCreateParams, AgentCreateResult, AgentListParams, AgentListResult,
            AgentStatusParams, AgentStatusResult, AgentSummary, ApprovalDecideParams,
            ApprovalDecision, ApprovalDecisionName, CAPABILITIES, CommandAcceptedResult,
            DaemonStatusDaemon, DaemonStatusModel, DaemonStatusParams, DaemonStatusProviderKind,
            DaemonStatusResult, DaemonStatusSession, DaemonStatusTokenUsage, DaemonStatusTrust,
            DaemonStatusUsage, ERROR_DAEMON_SHUTTING_DOWN, ERROR_INTERNAL, ERROR_ISSUE_PREP_FAILED,
            ERROR_LAGGED, ERROR_MALFORMED_REQUEST, ERROR_NOT_FOUND, ERROR_OVERLOAD,
            ERROR_RUN_FAILED, ERROR_SESSIONS_LIST_FAILED, ERROR_THREAD_AUTHORITY_EXCEEDED,
            ERROR_THREAD_AUTHORITY_FAILED, ERROR_THREAD_EVENTS_FAILED, ERROR_THREAD_LIST_FAILED,
            ERROR_THREAD_SEND_FAILED, ERROR_THREAD_SPAWN_FAILED, ERROR_THREAD_STATUS_FAILED,
            ERROR_THREAD_STOP_FAILED, ERROR_WORKSPACE_BROKEN, ERROR_WORKSPACE_MISMATCH,
            ERROR_WORKSPACE_UNREGISTERED, Envelope, EventsStreamParams, EventsStreamResult,
            HelloParams, HelloResult, IssuePrepResult, IssuePrepStartParams, IssuePrepStartResult,
            MessageAppendParams, ModelIdentityStatus, ProtocolErrorCode, ProtocolRequest,
            ProtocolResponse, RunCancelParams, RunStartParams, RunStartResult, RunStateName,
            SessionSummary, SessionsListResult, ShutdownIfIdleResult, ShutdownIfIdleResultName,
            ThreadApprovalPolicy, ThreadAuthorityParams, ThreadAuthorityResult, ThreadEventsParams,
            ThreadListResult, ThreadSendParams, ThreadSpawnDecision, ThreadSpawnParams,
            ThreadSpawnResult, ThreadStatus, ThreadStatusParams, ThreadStatusResult,
            ThreadStopParams, ThreadStopResult, TranscriptReadParams, TranscriptReadResult,
            TypedRun, TypedTranscript, TypedTranscriptEntry, WorkspaceCreateParams,
            WorkspaceCreateResult, WorkspaceHealthName, WorkspaceListParams, WorkspaceListResult,
            WorkspaceStatusParams, WorkspaceStatusResult, WorkspaceSummary, decode_request,
        },
        runtime::{
            DaemonRuntime, IssuePrepAdmissionError, RunAdmissionError, RunRecord,
            ShutdownIfIdleDecision, ThreadEventsError, ThreadRunBindError, ThreadSendAdmission,
            ThreadSpawnAdmissionError, ThreadSpawnClaimError, ThreadStopError, ThreadTurnBinding,
            approval_handler,
        },
    },
    issue_prep::{IssuePrepOptions, IssuePrepOutcome, run_issue_prep},
    ledger::{PersistedTokenUsage, SessionRunRecords, SqliteLedger},
    model::RunOverrides,
    new_run_id, new_session_id,
    paths::DefaultSqlitePath,
    replay::{format_readback, format_session_readback},
    server_store::{AgentRecord, ServerStore},
    thread_authority::{
        THREAD_SPAWN_APPROVAL_REASON, ThreadAuthorityDraft, ThreadAuthorityDraftParams,
        ThreadAuthorityError, ThreadSpawnApprovalRecord, ThreadSpawnDecisionName, ThreadStopRecord,
        authority_working_directory, legacy_status_authority, new_spawn_id, new_thread_turn_id,
        now_ms, thread_spawn_effect, validate_child_authority,
    },
    tool_catalog::{SHELL_EXEC, effect_for_tool, is_known_tool},
};
use platonic_core::{
    ActorId, AgentId, EffectClass, HarnessEvent, ReadbackEntry, RecordedEvent, RunReadback, TurnId,
};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, atomic::Ordering, mpsc},
    thread,
    time::Duration,
};

const DEFAULT_EVENT_LIMIT: usize = 64;
const MAX_EVENT_LIMIT: usize = 128;
const MAX_THREAD_EVENT_WAIT_MS: u64 = 1_000;
const LATEST_QUESTION_MAX_CHARS: usize = 120;
const EVENT_COLLECTOR_PANIC: &str = "daemon event collector panicked";
const THREAD_STOP_WAIT: Duration = Duration::from_secs(10);

pub(super) fn handle_line(runtime: &DaemonRuntime, line: &str) -> Envelope {
    match decode_request(line) {
        Ok(request) => handle_request(runtime, request),
        Err(error) => *error,
    }
}

pub(super) fn handle_request(runtime: &DaemonRuntime, request: Envelope) -> Envelope {
    match request
        .params
        .clone()
        .expect("decoded request carries typed params")
    {
        ProtocolRequest::Hello(params) => handle_hello(runtime, request, params),
        ProtocolRequest::RunStart(params) => handle_run_start(runtime, request, params),
        ProtocolRequest::MessageAppend(params) => handle_message_append(runtime, request, params),
        ProtocolRequest::IssuePrepStart(params) => {
            handle_issue_prep_start(runtime, request, params)
        }
        ProtocolRequest::EventsStream(params) => handle_events_stream(runtime, request, params),
        ProtocolRequest::ApprovalDecide(params) => handle_approval_decide(runtime, request, params),
        ProtocolRequest::RunCancel(params) => handle_run_cancel(runtime, request, params),
        ProtocolRequest::SessionsList => handle_sessions_list(runtime, request),
        ProtocolRequest::TranscriptRead(params) => handle_transcript_read(runtime, request, params),
        ProtocolRequest::DaemonStatus(params) => handle_daemon_status(runtime, request, params),
        ProtocolRequest::DaemonShutdownIfIdle => handle_shutdown_if_idle(runtime, request),
        ProtocolRequest::ThreadSpawn(params) => handle_thread_spawn(runtime, request, params),
        ProtocolRequest::ThreadList => handle_thread_list(runtime, request),
        ProtocolRequest::ThreadStatus(params) => handle_thread_status(runtime, request, params),
        ProtocolRequest::ThreadAuthority(params) => {
            handle_thread_authority(runtime, request, params)
        }
        ProtocolRequest::ThreadSend(params) => handle_thread_send(runtime, request, params),
        ProtocolRequest::ThreadEvents(params) => handle_thread_events(runtime, request, params),
        ProtocolRequest::ThreadStop(params) => handle_thread_stop(runtime, request, params),
        ProtocolRequest::WorkspaceCreate(params) => {
            handle_workspace_create(runtime, request, params)
        }
        ProtocolRequest::WorkspaceList(params) => handle_workspace_list(runtime, request, params),
        ProtocolRequest::WorkspaceStatus(params) => {
            handle_workspace_status(runtime, request, params)
        }
        ProtocolRequest::AgentCreate(params) => handle_agent_create(runtime, request, params),
        ProtocolRequest::AgentList(params) => handle_agent_list(runtime, request, params),
        ProtocolRequest::AgentStatus(params) => handle_agent_status(runtime, request, params),
    }
}

fn handle_hello(runtime: &DaemonRuntime, request: Envelope, params: HelloParams) -> Envelope {
    if params.workspace_id != runtime.paths.workspace_id {
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

    match PathBuf::from(&params.workspace_root).canonicalize() {
        Ok(root) if root == runtime.paths.workspace_root => {}
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
    }

    let store = match runtime.paths.server_store() {
        Ok(store) => store,
        Err(error) => return store_error(request.id, "hello", error),
    };
    match store.workspace_by_root(&runtime.paths.workspace_root.to_string_lossy()) {
        Ok(Some(_)) => {}
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
            daemon_version: platonic_protocol::BUILD_IDENTITY.into(),
            workspace_id: runtime.paths.workspace_id.clone(),
            ledger_path: runtime.paths.ledger_path.to_string_lossy().into_owned(),
            capabilities: CAPABILITIES.to_vec(),
            daemon_scope: None,
        }),
    )
}

fn handle_daemon_status(
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
    let persisted = crate::ledger::default_sqlite_session_status(
        &runtime.paths.default_ledger(),
        params.session_id.as_deref(),
    )?;
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
        None => (
            None,
            DaemonStatusSession {
                session_id: None,
                latest_run_id: None,
                human_turn_count: 0,
                ledger_path,
                core_event_count: 0,
            },
            DaemonStatusUsage {
                last_run: DaemonStatusTokenUsage::default(),
                session: DaemonStatusTokenUsage::default(),
            },
            DaemonStatusTrust::default(),
        ),
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
    let mut parts = platonic_protocol::BUILD_IDENTITY.split_whitespace();
    let package_version = parts.next().unwrap_or("unknown").into();
    let build_commit = known_build_part(parts.next());
    let build_date_utc = known_build_part(parts.next());
    (package_version, build_commit, build_date_utc)
}

fn known_build_part(part: Option<&str>) -> Option<String> {
    part.filter(|part| *part != "unknown").map(str::to_owned)
}

#[derive(Debug)]
enum ThreadSpawnFailure {
    ShuttingDown,
    Malformed(String),
    Unregistered(String),
    NotFound(String),
    WorkspaceBroken(String),
    Authority(ThreadAuthorityError),
    Overload(String),
    Conflict(String),
    Persistence,
}

fn handle_thread_spawn(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: ThreadSpawnParams,
) -> Envelope {
    match thread_spawn(runtime, params) {
        Ok(result) => Envelope::typed_response(request.id, ProtocolResponse::ThreadSpawn(result)),
        Err(ThreadSpawnFailure::ShuttingDown) => shutting_down_response(request.id, "thread.spawn"),
        Err(ThreadSpawnFailure::Malformed(message)) => Envelope::error(
            request.id,
            Some("thread.spawn".into()),
            ERROR_MALFORMED_REQUEST,
            message,
        ),
        Err(ThreadSpawnFailure::Unregistered(message)) => Envelope::error(
            request.id,
            Some("thread.spawn".into()),
            ERROR_WORKSPACE_UNREGISTERED,
            message,
        ),
        Err(ThreadSpawnFailure::NotFound(message)) => Envelope::error(
            request.id,
            Some("thread.spawn".into()),
            ERROR_NOT_FOUND,
            message,
        ),
        Err(ThreadSpawnFailure::WorkspaceBroken(message)) => Envelope::error(
            request.id,
            Some("thread.spawn".into()),
            ERROR_WORKSPACE_BROKEN,
            message,
        ),
        Err(ThreadSpawnFailure::Authority(error)) => Envelope::error(
            request.id,
            Some("thread.spawn".into()),
            ERROR_THREAD_AUTHORITY_EXCEEDED,
            error.to_string(),
        ),
        Err(ThreadSpawnFailure::Overload(message)) => Envelope::error(
            request.id,
            Some("thread.spawn".into()),
            ERROR_OVERLOAD,
            message,
        ),
        Err(ThreadSpawnFailure::Conflict(message)) => Envelope::error(
            request.id,
            Some("thread.spawn".into()),
            ERROR_THREAD_SPAWN_FAILED,
            message,
        ),
        Err(ThreadSpawnFailure::Persistence) => Envelope::error(
            request.id,
            Some("thread.spawn".into()),
            ERROR_THREAD_SPAWN_FAILED,
            "thread spawn could not be persisted",
        ),
    }
}

fn thread_spawn(
    runtime: &DaemonRuntime,
    params: ThreadSpawnParams,
) -> Result<ThreadSpawnResult, ThreadSpawnFailure> {
    match params {
        ThreadSpawnParams::Start {
            parent_thread_id,
            cwd,
            model,
            reasoning_effort,
            approval_policy,
        } => start_thread_spawn(
            runtime,
            parent_thread_id,
            Path::new(&cwd),
            model,
            reasoning_effort,
            approval_policy,
        ),
        ThreadSpawnParams::Decide { spawn_id, approval } => {
            decide_thread_spawn(runtime, &spawn_id, approval)
        }
    }
}

fn start_thread_spawn(
    runtime: &DaemonRuntime,
    parent_thread_id: Option<String>,
    cwd: &Path,
    model: String,
    reasoning_effort: crate::model::ReasoningEffort,
    approval_policy: crate::daemon::protocol::ThreadApprovalPolicy,
) -> Result<ThreadSpawnResult, ThreadSpawnFailure> {
    if runtime.shutdown_accepted() {
        return Err(ThreadSpawnFailure::ShuttingDown);
    }
    if !cwd.is_absolute() {
        return Err(ThreadSpawnFailure::Malformed(
            "thread cwd must be an absolute path".into(),
        ));
    }
    let mut store = runtime
        .paths
        .server_store()
        .map_err(|_| ThreadSpawnFailure::Persistence)?;
    match store
        .workspace_by_root(&runtime.paths.workspace_root.to_string_lossy())
        .map_err(|_| ThreadSpawnFailure::Persistence)?
    {
        Some(workspace) if workspace.health() == crate::server_store::WorkspaceHealth::Present => {}
        Some(workspace) => {
            return Err(ThreadSpawnFailure::WorkspaceBroken(format!(
                "workspace directory is missing: {}",
                workspace.id
            )));
        }
        None => {
            return Err(ThreadSpawnFailure::Unregistered(format!(
                "workspace is not registered: {}; run platonic workspace create",
                runtime.paths.workspace_root.display()
            )));
        }
    }
    let config = Config::load(cwd, None)
        .map_err(|error| ThreadSpawnFailure::Malformed(error.to_string()))?;
    let writable = config.tools.enabled.iter().any(|tool| {
        matches!(
            effect_for_tool(tool),
            EffectClass::WorkspaceWrite | EffectClass::ExternalSideEffect
        )
    });
    let network = config
        .tools
        .enabled
        .iter()
        .any(|tool| matches!(effect_for_tool(tool), EffectClass::Network));
    let draft = ThreadAuthorityDraft::new(ThreadAuthorityDraftParams {
        parent_thread_id,
        cwd,
        model,
        reasoning_effort,
        approval_policy,
        agent_id: AgentId::new("plato")
            .map_err(|error| ThreadSpawnFailure::Malformed(error.to_string()))?,
        toolset: config.tools.enabled,
        writable,
        network,
    })
    .map_err(|error| ThreadSpawnFailure::Malformed(error.to_string()))?;
    let parent = read_live_parent(runtime, &store, &draft)?;
    let auto_grant = parent.as_ref().is_some_and(|parent| {
        parent.approval_policy == crate::daemon::protocol::ThreadApprovalPolicy::Yolo
    });
    let spawn_id = new_spawn_id();
    runtime
        .reserve_thread_spawn(spawn_id.clone(), draft.clone())
        .map_err(|error| match error {
            ThreadSpawnAdmissionError::ShuttingDown => ThreadSpawnFailure::ShuttingDown,
            ThreadSpawnAdmissionError::Duplicate => {
                ThreadSpawnFailure::Conflict("duplicate thread spawn reservation".into())
            }
        })?;

    if auto_grant {
        let pending = runtime
            .claim_thread_spawn(&spawn_id)
            .expect("newly reserved thread spawn can be claimed");
        return resolve_thread_spawn(
            runtime,
            &mut store,
            pending,
            ThreadSpawnDecision::Grant {
                actor: "yolo".into(),
            },
        );
    }

    Ok(ThreadSpawnResult::ApprovalRequired {
        spawn_id,
        thread_id: draft.thread_id,
        effect: thread_spawn_effect(),
        reason: THREAD_SPAWN_APPROVAL_REASON.into(),
    })
}

fn decide_thread_spawn(
    runtime: &DaemonRuntime,
    spawn_id: &str,
    decision: ThreadSpawnDecision,
) -> Result<ThreadSpawnResult, ThreadSpawnFailure> {
    if runtime.shutdown_accepted() {
        return Err(ThreadSpawnFailure::ShuttingDown);
    }
    let mut store = runtime
        .paths
        .server_store()
        .map_err(|_| ThreadSpawnFailure::Persistence)?;
    if let Some(existing) = store
        .thread_spawn_approval(spawn_id)
        .map_err(|_| ThreadSpawnFailure::Persistence)?
    {
        return persisted_thread_spawn_result(runtime, &store, existing, &decision);
    }
    let pending = runtime
        .claim_thread_spawn(spawn_id)
        .map_err(|error| match error {
            ThreadSpawnClaimError::NotFound => {
                ThreadSpawnFailure::NotFound(format!("pending thread spawn not found: {spawn_id}"))
            }
            ThreadSpawnClaimError::WrongWorkspace => ThreadSpawnFailure::NotFound(format!(
                "pending thread spawn belongs to another workspace: {spawn_id}"
            )),
            ThreadSpawnClaimError::DecisionInProgress => ThreadSpawnFailure::Overload(format!(
                "thread spawn decision is already in progress: {spawn_id}"
            )),
        })?;
    resolve_thread_spawn(runtime, &mut store, pending, decision)
}

fn resolve_thread_spawn(
    runtime: &DaemonRuntime,
    store: &mut ServerStore,
    pending: crate::daemon::runtime::PendingThreadSpawn,
    decision: ThreadSpawnDecision,
) -> Result<ThreadSpawnResult, ThreadSpawnFailure> {
    let result = resolve_thread_spawn_inner(runtime, store, &pending, decision);
    if result.is_err() {
        runtime.release_thread_spawn_claim(&pending.spawn_id);
    }
    result
}

fn resolve_thread_spawn_inner(
    runtime: &DaemonRuntime,
    store: &mut ServerStore,
    pending: &crate::daemon::runtime::PendingThreadSpawn,
    decision: ThreadSpawnDecision,
) -> Result<ThreadSpawnResult, ThreadSpawnFailure> {
    let decided_at_ms = now_ms();
    let approval = ThreadSpawnApprovalRecord::from_decision(
        pending.spawn_id.clone(),
        pending.draft.thread_id.clone(),
        &decision,
        decided_at_ms,
    )
    .map_err(|error| ThreadSpawnFailure::Malformed(error.to_string()))?;
    match decision {
        ThreadSpawnDecision::Grant { actor } => {
            read_live_parent(runtime, store, &pending.draft)?;
            let authority = pending
                .draft
                .complete(actor, decided_at_ms)
                .map_err(|error| ThreadSpawnFailure::Malformed(error.to_string()))?;
            let durable = store
                .persist_thread_spawn(&approval, Some(&authority))
                .map_err(|_| ThreadSpawnFailure::Persistence)?
                .expect("granted spawn persistence returns durable authority");
            let authority = durable.record().clone();
            runtime.complete_thread_spawn(&pending.spawn_id, durable);
            Ok(ThreadSpawnResult::Spawned {
                thread: joined_thread_status(runtime, authority)
                    .map_err(|_| ThreadSpawnFailure::Persistence)?,
            })
        }
        ThreadSpawnDecision::Deny { actor, reason } => {
            store
                .persist_thread_spawn(&approval, None)
                .map_err(|_| ThreadSpawnFailure::Persistence)?;
            runtime.complete_thread_spawn_without_authority(&pending.spawn_id);
            Ok(ThreadSpawnResult::Denied {
                spawn_id: pending.spawn_id.clone(),
                thread_id: pending.draft.thread_id.clone(),
                actor,
                reason,
            })
        }
        ThreadSpawnDecision::Cancel { actor } => {
            store
                .persist_thread_spawn(&approval, None)
                .map_err(|_| ThreadSpawnFailure::Persistence)?;
            runtime.complete_thread_spawn_without_authority(&pending.spawn_id);
            Ok(ThreadSpawnResult::Canceled {
                spawn_id: pending.spawn_id.clone(),
                thread_id: pending.draft.thread_id.clone(),
                actor,
            })
        }
    }
}

fn persisted_thread_spawn_result(
    runtime: &DaemonRuntime,
    store: &ServerStore,
    approval: ThreadSpawnApprovalRecord,
    requested: &ThreadSpawnDecision,
) -> Result<ThreadSpawnResult, ThreadSpawnFailure> {
    if !approval.matches(requested) {
        return Err(ThreadSpawnFailure::Conflict(format!(
            "thread spawn {} already has a different durable decision",
            approval.spawn_id
        )));
    }
    match approval.decision {
        ThreadSpawnDecisionName::Granted => {
            let authority = store
                .thread_authority(&approval.thread_id)
                .map_err(|_| ThreadSpawnFailure::Persistence)?
                .ok_or(ThreadSpawnFailure::Persistence)?;
            Ok(ThreadSpawnResult::Spawned {
                thread: joined_thread_status(runtime, authority)
                    .map_err(|_| ThreadSpawnFailure::Persistence)?,
            })
        }
        ThreadSpawnDecisionName::Denied => Ok(ThreadSpawnResult::Denied {
            spawn_id: approval.spawn_id,
            thread_id: approval.thread_id,
            actor: approval.actor,
            reason: approval.reason.ok_or(ThreadSpawnFailure::Persistence)?,
        }),
        ThreadSpawnDecisionName::Canceled => Ok(ThreadSpawnResult::Canceled {
            spawn_id: approval.spawn_id,
            thread_id: approval.thread_id,
            actor: approval.actor,
        }),
    }
}

fn read_live_parent(
    runtime: &DaemonRuntime,
    store: &ServerStore,
    draft: &ThreadAuthorityDraft,
) -> Result<Option<crate::daemon::protocol::ThreadAuthorityRecord>, ThreadSpawnFailure> {
    let Some(parent_thread_id) = draft.parent_thread_id.as_deref() else {
        return Ok(None);
    };
    let parent = store
        .thread_authority(parent_thread_id)
        .map_err(|_| ThreadSpawnFailure::Persistence)?
        .ok_or_else(|| {
            ThreadSpawnFailure::NotFound(format!(
                "parent thread authority not found: {parent_thread_id}"
            ))
        })?;
    if !runtime.thread_is_loaded(parent_thread_id) {
        return Err(ThreadSpawnFailure::NotFound(format!(
            "parent thread is not loaded: {parent_thread_id}"
        )));
    }
    validate_child_authority(&parent, draft).map_err(ThreadSpawnFailure::Authority)?;
    Ok(Some(parent))
}

fn handle_thread_list(runtime: &DaemonRuntime, request: Envelope) -> Envelope {
    match crate::server_store::thread_authorities(&runtime.paths.server_db_path).and_then(
        |authorities| {
            authorities
                .into_iter()
                .map(|authority| joined_thread_status(runtime, authority))
                .collect::<AppResult<Vec<_>>>()
        },
    ) {
        Ok(threads) => Envelope::typed_response(
            request.id,
            ProtocolResponse::ThreadList(ThreadListResult { threads }),
        ),
        Err(_) => Envelope::error(
            request.id,
            Some("thread.list".into()),
            ERROR_THREAD_LIST_FAILED,
            "thread list readback failed",
        ),
    }
}

fn handle_thread_status(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: ThreadStatusParams,
) -> Envelope {
    match crate::server_store::thread_authority(&runtime.paths.server_db_path, &params.thread_id)
        .and_then(|authority| {
            authority
                .map(|authority| joined_thread_status(runtime, authority))
                .transpose()
        }) {
        Ok(Some(thread)) => Envelope::typed_response(
            request.id,
            ProtocolResponse::ThreadStatus(ThreadStatusResult { thread }),
        ),
        Ok(None) => Envelope::error(
            request.id,
            Some("thread.status".into()),
            ERROR_NOT_FOUND,
            format!("thread not found: {}", params.thread_id),
        ),
        Err(_) => Envelope::error(
            request.id,
            Some("thread.status".into()),
            ERROR_THREAD_STATUS_FAILED,
            "thread status readback failed",
        ),
    }
}

fn handle_thread_authority(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: ThreadAuthorityParams,
) -> Envelope {
    match crate::server_store::thread_authority(&runtime.paths.server_db_path, &params.thread_id) {
        Ok(Some(authority)) => Envelope::typed_response(
            request.id,
            ProtocolResponse::ThreadAuthority(ThreadAuthorityResult { authority }),
        ),
        Ok(None) => Envelope::error(
            request.id,
            Some("thread.authority".into()),
            ERROR_NOT_FOUND,
            format!("thread authority not found: {}", params.thread_id),
        ),
        Err(_) => Envelope::error(
            request.id,
            Some("thread.authority".into()),
            ERROR_THREAD_AUTHORITY_FAILED,
            "thread authority readback failed",
        ),
    }
}

fn handle_thread_send(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: ThreadSendParams,
) -> Envelope {
    if runtime.shutdown_accepted() {
        return shutting_down_response(request.id, "thread.send");
    }
    if let Err(message) = validate_thread_send(&params) {
        return Envelope::error(
            request.id,
            Some("thread.send".into()),
            ERROR_MALFORMED_REQUEST,
            message,
        );
    }
    let authority = match crate::server_store::thread_authority(
        &runtime.paths.server_db_path,
        &params.thread_id,
    ) {
        Ok(Some(authority)) => authority,
        Ok(None) => {
            return Envelope::error(
                request.id,
                Some("thread.send".into()),
                ERROR_NOT_FOUND,
                format!("thread not found: {}", params.thread_id),
            );
        }
        Err(_) => {
            return Envelope::error(
                request.id,
                Some("thread.send".into()),
                ERROR_THREAD_SEND_FAILED,
                "thread authority readback failed",
            );
        }
    };
    match crate::server_store::thread_stop(&runtime.paths.server_db_path, &params.thread_id) {
        Ok(Some(_)) => {
            return Envelope::error(
                request.id,
                Some("thread.send".into()),
                ERROR_NOT_FOUND,
                format!("thread not found: {}", params.thread_id),
            );
        }
        Ok(None) => {}
        Err(_) => {
            return Envelope::error(
                request.id,
                Some("thread.send".into()),
                ERROR_THREAD_SEND_FAILED,
                "thread stop readback failed",
            );
        }
    }
    let admission = runtime.send_thread(
        &params.thread_id,
        params.controller_id,
        params.turn_id.as_deref(),
        params.message.clone(),
        new_thread_turn_id(),
    );
    let (receipt, turn) = match admission {
        ThreadSendAdmission::ShuttingDown => {
            return shutting_down_response(request.id, "thread.send");
        }
        ThreadSendAdmission::Stopped => {
            return Envelope::error(
                request.id,
                Some("thread.send".into()),
                ERROR_NOT_FOUND,
                format!("thread not found: {}", params.thread_id),
            );
        }
        ThreadSendAdmission::Started { receipt, turn } => (receipt, turn),
        ThreadSendAdmission::Steered { receipt } | ThreadSendAdmission::Rejected { receipt } => {
            return Envelope::typed_response(request.id, ProtocolResponse::ThreadSend(receipt));
        }
    };
    let session_id = thread_session_id(&params.thread_id);
    let session = match crate::ledger::default_sqlite_session_status(
        &runtime.paths.default_ledger(),
        Some(&session_id),
    ) {
        Ok(Some(_)) => RunSession::Continue { session_id },
        Err(AppError::SessionNotFound(_)) => RunSession::Fresh { session_id },
        Ok(None) => RunSession::Fresh { session_id },
        Err(_) => {
            runtime.abort_thread_turn(&turn);
            return Envelope::error(
                request.id,
                Some("thread.send".into()),
                ERROR_THREAD_SEND_FAILED,
                "thread session readback failed",
            );
        }
    };
    let Some(workspace_root) = authority_working_directory(&authority).map(Path::to_path_buf)
    else {
        runtime.abort_thread_turn(&turn);
        return Envelope::error(
            request.id,
            Some("thread.send".into()),
            ERROR_THREAD_SEND_FAILED,
            "thread authority has no working directory",
        );
    };
    let context = ThreadRunContext {
        workspace_root,
        approval_policy: authority.approval_policy,
        turn: turn.clone(),
    };
    let response = start_run(
        runtime,
        StartRunRequest {
            request_id: request.id.clone(),
            question: params.message,
            session,
            config_path: None,
            overrides: RunOverrides {
                model: Some(authority.model),
                reasoning_effort: Some(authority.reasoning_effort),
            },
            wait: Some(false),
            thread_context: Some(context),
        },
    );
    if let Err(response) = response {
        runtime.abort_thread_turn(&turn);
        return *response;
    }
    Envelope::typed_response(request.id, ProtocolResponse::ThreadSend(receipt))
}

fn validate_thread_send(params: &ThreadSendParams) -> Result<(), String> {
    ActorId::new(params.thread_id.clone()).map_err(|error| error.to_string())?;
    ActorId::new(params.controller_id.clone()).map_err(|error| error.to_string())?;
    if let Some(turn_id) = &params.turn_id {
        TurnId::new(turn_id.clone()).map_err(|error| error.to_string())?;
    }
    if params.message.trim().is_empty() {
        return Err("thread.send message must not be empty".into());
    }
    Ok(())
}

fn handle_thread_events(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: ThreadEventsParams,
) -> Envelope {
    let ThreadEventsParams {
        thread_id,
        from_offset,
        limit,
        wait_ms,
    } = params;
    if ActorId::new(thread_id.clone()).is_err() {
        return Envelope::error(
            request.id,
            Some("thread.events".into()),
            ERROR_MALFORMED_REQUEST,
            "thread.events thread_id is invalid",
        );
    }
    let limit = limit.unwrap_or(DEFAULT_EVENT_LIMIT);
    if limit == 0 || limit > MAX_EVENT_LIMIT {
        return Envelope::error(
            request.id,
            Some("thread.events".into()),
            ERROR_MALFORMED_REQUEST,
            format!("thread.events limit must be between 1 and {MAX_EVENT_LIMIT}"),
        );
    }
    let wait_ms = wait_ms.unwrap_or(0);
    if wait_ms > MAX_THREAD_EVENT_WAIT_MS {
        return Envelope::error(
            request.id,
            Some("thread.events".into()),
            ERROR_MALFORMED_REQUEST,
            format!("thread.events wait_ms must not exceed {MAX_THREAD_EVENT_WAIT_MS}"),
        );
    }
    match crate::server_store::thread_authority(&runtime.paths.server_db_path, &thread_id) {
        Ok(Some(_)) => {}
        Ok(None) => {
            return Envelope::error(
                request.id,
                Some("thread.events".into()),
                ERROR_NOT_FOUND,
                format!("thread not found: {thread_id}"),
            );
        }
        Err(_) => {
            return Envelope::error(
                request.id,
                Some("thread.events".into()),
                ERROR_THREAD_EVENTS_FAILED,
                "thread authority readback failed",
            );
        }
    }
    match crate::server_store::thread_stop(&runtime.paths.server_db_path, &thread_id) {
        Ok(Some(_)) => {
            return Envelope::error(
                request.id,
                Some("thread.events".into()),
                ERROR_NOT_FOUND,
                format!("thread not found: {thread_id}"),
            );
        }
        Ok(None) => {}
        Err(_) => {
            return Envelope::error(
                request.id,
                Some("thread.events".into()),
                ERROR_THREAD_EVENTS_FAILED,
                "thread stop readback failed",
            );
        }
    }
    match runtime.thread_events(
        &thread_id,
        from_offset,
        limit,
        std::time::Duration::from_millis(wait_ms),
    ) {
        Ok(result) => Envelope::typed_response(request.id, ProtocolResponse::ThreadEvents(result)),
        Err(ThreadEventsError::Lagged { first_offset }) => Envelope::error(
            request.id,
            Some("thread.events".into()),
            ERROR_LAGGED,
            format!(
                "requested thread events were evicted; first available offset is {first_offset}"
            ),
        ),
        Err(ThreadEventsError::Stopped) => Envelope::error(
            request.id,
            Some("thread.events".into()),
            ERROR_NOT_FOUND,
            format!("thread not found: {thread_id}"),
        ),
    }
}

fn handle_thread_stop(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: ThreadStopParams,
) -> Envelope {
    let mut store = match runtime.paths.server_store() {
        Ok(store) => store,
        Err(_) => {
            return Envelope::error(
                request.id,
                Some("thread.stop".into()),
                ERROR_THREAD_STOP_FAILED,
                "thread stop ledger could not be opened",
            );
        }
    };
    let authority = match store.thread_authority(&params.thread_id) {
        Ok(Some(authority)) => authority,
        Ok(None) => {
            return Envelope::error(
                request.id,
                Some("thread.stop".into()),
                ERROR_NOT_FOUND,
                format!("thread not found: {}", params.thread_id),
            );
        }
        Err(_) => {
            return Envelope::error(
                request.id,
                Some("thread.stop".into()),
                ERROR_THREAD_STOP_FAILED,
                "thread stop authority readback failed",
            );
        }
    };
    let validated =
        match ThreadStopRecord::new(authority.thread_id.clone(), params.actor, None, now_ms()) {
            Ok(record) => record,
            Err(error) => {
                return Envelope::error(
                    request.id,
                    Some("thread.stop".into()),
                    ERROR_MALFORMED_REQUEST,
                    error.to_string(),
                );
            }
        };
    match store.thread_stop(&authority.thread_id) {
        Ok(Some(stop)) => {
            return Envelope::typed_response(
                request.id,
                ProtocolResponse::ThreadStop(thread_stop_result(stop, true)),
            );
        }
        Ok(None) => {}
        Err(_) => {
            return Envelope::error(
                request.id,
                Some("thread.stop".into()),
                ERROR_THREAD_STOP_FAILED,
                "thread stop readback failed",
            );
        }
    }

    let target = match runtime.begin_thread_stop(&authority.thread_id) {
        Ok(target) => target,
        Err(ThreadStopError::InProgress) => {
            return Envelope::error(
                request.id,
                Some("thread.stop".into()),
                ERROR_OVERLOAD,
                format!(
                    "thread stop is already in progress: {}",
                    authority.thread_id
                ),
            );
        }
        Err(ThreadStopError::AlreadyStopped) => {
            return match store.thread_stop(&authority.thread_id) {
                Ok(Some(stop)) => Envelope::typed_response(
                    request.id,
                    ProtocolResponse::ThreadStop(thread_stop_result(stop, true)),
                ),
                _ => Envelope::error(
                    request.id,
                    Some("thread.stop".into()),
                    ERROR_THREAD_STOP_FAILED,
                    "thread stopped without a durable stop record",
                ),
            };
        }
    };

    if let Some(run) = target.run.as_ref() {
        let _ = run.request_cancel();
        if !run.wait_for_terminal(THREAD_STOP_WAIT) {
            runtime.abort_thread_stop(&authority.thread_id);
            return Envelope::error(
                request.id,
                Some("thread.stop".into()),
                ERROR_THREAD_STOP_FAILED,
                format!(
                    "thread child did not reach zero residual within the stop bound: {}",
                    authority.thread_id
                ),
            );
        }
    }

    let stop = ThreadStopRecord::new(
        authority.thread_id.clone(),
        validated.actor,
        target.turn_id,
        now_ms(),
    )
    .expect("thread stop inputs were validated before lifecycle execution");
    let (stop, inserted) = match store.persist_thread_stop(&stop) {
        Ok(result) => result,
        Err(_) => {
            runtime.abort_thread_stop(&authority.thread_id);
            return Envelope::error(
                request.id,
                Some("thread.stop".into()),
                ERROR_THREAD_STOP_FAILED,
                "thread stop could not be persisted",
            );
        }
    };
    runtime.complete_thread_stop(&authority.thread_id);
    Envelope::typed_response(
        request.id,
        ProtocolResponse::ThreadStop(thread_stop_result(stop, !inserted)),
    )
}

fn thread_stop_result(stop: ThreadStopRecord, already_stopped: bool) -> ThreadStopResult {
    if already_stopped {
        ThreadStopResult::AlreadyStopped {
            thread_id: stop.thread_id,
            stopped_turn_id: stop.stopped_turn_id,
            stopped_at_ms: stop.occurred_at_ms,
        }
    } else {
        ThreadStopResult::Stopped {
            thread_id: stop.thread_id,
            stopped_turn_id: stop.stopped_turn_id,
            stopped_at_ms: stop.occurred_at_ms,
        }
    }
}

fn thread_session_id(thread_id: &str) -> String {
    format!("session_{thread_id}")
}

fn joined_thread_status(
    runtime: &DaemonRuntime,
    authority: crate::daemon::protocol::ThreadAuthorityRecord,
) -> AppResult<ThreadStatus> {
    let live = runtime.thread_live_state(&authority.thread_id);
    Ok(ThreadStatus {
        authority: legacy_status_authority(&authority)?,
        live,
    })
}

fn handle_run_start(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: RunStartParams,
) -> Envelope {
    let request_id = request.id;
    match start_run(
        runtime,
        StartRunRequest {
            request_id: request_id.clone(),
            question: params.question,
            session: RunSession::Fresh {
                session_id: new_session_id(),
            },
            config_path: params.config_path,
            overrides: params.overrides,
            wait: params.wait,
            thread_context: None,
        },
    ) {
        Ok(result) => Envelope::typed_response(request_id, ProtocolResponse::RunStart(result)),
        Err(response) => *response,
    }
}

fn handle_message_append(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: MessageAppendParams,
) -> Envelope {
    if runtime.shutdown_accepted() {
        return shutting_down_response(request.id, "message.append");
    }
    let session_id = match params.session_id {
        Some(session_id) => session_id,
        None => match latest_session_id(runtime) {
            Ok(session_id) => session_id,
            Err(error) => {
                if runtime.shutdown_accepted() {
                    return shutting_down_response(request.id, "message.append");
                }
                return Envelope::error(
                    request.id,
                    Some("message.append".into()),
                    ERROR_NOT_FOUND,
                    error,
                );
            }
        },
    };
    let request_id = request.id;
    match start_run(
        runtime,
        StartRunRequest {
            request_id: request_id.clone(),
            question: params.message,
            session: RunSession::Continue { session_id },
            config_path: params.config_path,
            overrides: params.overrides,
            wait: params.wait,
            thread_context: None,
        },
    ) {
        Ok(result) => Envelope::typed_response(request_id, ProtocolResponse::MessageAppend(result)),
        Err(response) => *response,
    }
}

fn handle_issue_prep_start(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: IssuePrepStartParams,
) -> Envelope {
    let _reservation = match runtime.reserve_issue_prep() {
        Ok(reservation) => reservation,
        Err(IssuePrepAdmissionError::ShuttingDown) => {
            return shutting_down_response(request.id, "issue-prep.start");
        }
        Err(IssuePrepAdmissionError::Active) => {
            return Envelope::error(
                request.id,
                Some("issue-prep.start".into()),
                ERROR_OVERLOAD,
                "issue prep is already active",
            );
        }
    };
    let run_id = match new_run_id() {
        Ok(run_id) => run_id,
        Err(error) => {
            return Envelope::error(
                request.id,
                Some("issue-prep.start".into()),
                ERROR_ISSUE_PREP_FAILED,
                error.to_string(),
            );
        }
    };
    let run_dir = runtime
        .paths
        .workspace_root
        .join(".plato")
        .join("issue-prep")
        .join(run_id.to_string());
    let options = IssuePrepOptions {
        workspace_root: runtime.paths.workspace_root.clone(),
        config_path: params.config_path.map(PathBuf::from),
        run_dir: run_dir.clone(),
        input: params.input,
    };
    match run_issue_prep(options) {
        Ok(outcome) => Envelope::typed_response(
            request.id,
            ProtocolResponse::IssuePrepStart(IssuePrepStartResult {
                run_dir: run_dir.to_string_lossy().into_owned(),
                outcome: match outcome {
                    IssuePrepOutcome::Candidate { markdown } => {
                        IssuePrepResult::Candidate { markdown }
                    }
                    IssuePrepOutcome::Blocked { stage, reasons } => {
                        IssuePrepResult::Blocked { stage, reasons }
                    }
                },
            }),
        ),
        Err(error) => Envelope::error(
            request.id,
            Some("issue-prep.start".into()),
            ERROR_ISSUE_PREP_FAILED,
            format!("{error}; run directory: {}", run_dir.display()),
        ),
    }
}

#[derive(Clone, Debug)]
struct ThreadRunContext {
    workspace_root: PathBuf,
    approval_policy: ThreadApprovalPolicy,
    turn: ThreadTurnBinding,
}

struct StartRunRequest {
    request_id: Option<String>,
    question: String,
    session: RunSession,
    config_path: Option<String>,
    overrides: RunOverrides,
    wait: Option<bool>,
    thread_context: Option<ThreadRunContext>,
}

struct ThreadTurnDriver {
    context: ThreadRunContext,
    session_id: String,
    config_path: Option<String>,
    overrides: RunOverrides,
}

fn start_run(
    runtime: &DaemonRuntime,
    request: StartRunRequest,
) -> Result<RunStartResult, Box<Envelope>> {
    let StartRunRequest {
        request_id,
        question,
        session,
        config_path,
        overrides,
        wait,
        thread_context,
    } = request;
    let method = match (&thread_context, &session) {
        (Some(_), _) => "thread.send",
        (None, RunSession::Fresh { .. }) => "run.start",
        (None, RunSession::Continue { .. }) => "message.append",
    };
    let error_code = if thread_context.is_some() {
        ERROR_THREAD_SEND_FAILED
    } else {
        ERROR_RUN_FAILED
    };
    let session_id = session.session_id().to_string();
    let run_id = match new_run_id() {
        Ok(run_id) => run_id,
        Err(error) => {
            return Err(Box::new(Envelope::error(
                request_id,
                Some(method.into()),
                error_code,
                error.to_string(),
            )));
        }
    };
    let run_id_string = run_id.to_string();
    let record = Arc::new(match &thread_context {
        Some(context) => RunRecord::new_for_thread(
            run_id_string.clone(),
            session_id.clone(),
            runtime.paths.ledger_path.clone(),
            context.turn.clone(),
        ),
        None => RunRecord::new(
            run_id_string.clone(),
            session_id.clone(),
            runtime.paths.ledger_path.clone(),
        ),
    });
    match runtime.reserve_run(record.clone()) {
        Ok(()) => {}
        Err(RunAdmissionError::ShuttingDown) => {
            return Err(Box::new(shutting_down_response(request_id, method)));
        }
        Err(RunAdmissionError::SessionActive { run_id }) => {
            return Err(Box::new(Envelope::error(
                request_id,
                Some(method.into()),
                ERROR_OVERLOAD,
                format!(
                    "session already has an active run: {} ({run_id})",
                    record.session_id
                ),
            )));
        }
    }
    if let Some(context) = thread_context.as_ref()
        && let Err(error) = runtime.bind_thread_run(&context.turn, record.clone())
    {
        runtime.release_run_reservation(&record);
        runtime.abort_thread_turn(&context.turn);
        return Err(Box::new(match error {
            ThreadRunBindError::Stopping | ThreadRunBindError::NotLoaded => Envelope::error(
                request_id,
                Some(method.into()),
                ERROR_NOT_FOUND,
                format!("thread not found: {}", context.turn.thread_id),
            ),
            ThreadRunBindError::RunActive => Envelope::error(
                request_id,
                Some(method.into()),
                ERROR_OVERLOAD,
                format!(
                    "thread already has an active run: {}",
                    context.turn.thread_id
                ),
            ),
        }));
    }

    let (event_sender, event_receiver) = mpsc::channel::<RunEvent>();
    let event_collector = spawn_event_collector(record.clone(), event_receiver);
    let continuation_config_path = config_path.clone();
    let continuation_overrides = overrides.clone();
    let options = RunOptions {
        question,
        config_path: config_path.map(PathBuf::from),
        overrides,
        ledger: RunLedger::DefaultSqlite(runtime.paths.default_ledger()),
        workspace_root: thread_context.as_ref().map_or_else(
            || runtime.paths.workspace_root.clone(),
            |context| context.workspace_root.clone(),
        ),
        approval_mode: match thread_context
            .as_ref()
            .map(|context| context.approval_policy)
        {
            Some(ThreadApprovalPolicy::Yolo) => ApprovalMode::from_yolo(true),
            _ => ApprovalMode::external_with_actor(
                "daemon",
                approval_handler(runtime.clone(), record.clone()),
            ),
        },
        run_id: Some(run_id),
        session: Some(session),
        event_sender: Some(event_sender),
        stream_to_stderr: false,
        cancel: Some(record.cancel.clone()),
        voice_interruption_context: None,
    };

    if wait.unwrap_or(false) {
        match run_to_completion(runtime, &record, options, event_collector) {
            Ok(_) => Ok(run_start_result(&record)),
            Err(error) => Err(Box::new(Envelope::error(
                request_id,
                Some(method.into()),
                error_code,
                error.to_string(),
            ))),
        }
    } else {
        let worker_runtime = runtime.clone();
        let worker_record = record.clone();
        match thread_context {
            Some(context) => {
                thread::spawn(move || {
                    drive_thread_turn(
                        worker_runtime,
                        worker_record,
                        options,
                        event_collector,
                        ThreadTurnDriver {
                            context,
                            session_id,
                            config_path: continuation_config_path,
                            overrides: continuation_overrides,
                        },
                    );
                });
            }
            None => {
                thread::spawn(move || {
                    let _ = run_to_completion(
                        &worker_runtime,
                        &worker_record,
                        options,
                        event_collector,
                    );
                });
            }
        }
        Ok(run_start_result(&record))
    }
}

fn drive_thread_turn(
    runtime: DaemonRuntime,
    record: Arc<RunRecord>,
    options: RunOptions,
    event_collector: thread::JoinHandle<()>,
    driver: ThreadTurnDriver,
) {
    let _ = run_to_completion(&runtime, &record, options, event_collector);
    while let Some(message) = runtime.next_thread_message(&driver.context.turn) {
        let _ = start_run(
            &runtime,
            StartRunRequest {
                request_id: None,
                question: message,
                session: RunSession::Continue {
                    session_id: driver.session_id.clone(),
                },
                config_path: driver.config_path.clone(),
                overrides: driver.overrides.clone(),
                wait: Some(true),
                thread_context: Some(driver.context.clone()),
            },
        );
    }
    runtime.abort_thread_turn(&driver.context.turn);
}

fn run_to_completion(
    runtime: &DaemonRuntime,
    record: &RunRecord,
    options: RunOptions,
    event_collector: thread::JoinHandle<()>,
) -> AppResult<RunOutcome> {
    #[cfg(test)]
    let completion = RunCompletion::Published(crate::run_question(options));
    #[cfg(not(test))]
    let completion = match crate::app::prepare_run(&options) {
        Ok((prepared, recorder)) => {
            let approval_mode = options.approval_mode;
            match (options.event_sender, options.cancel) {
                (Some(event_sender), Some(cancel)) => {
                    RunCompletion::Supervised(Box::new(crate::daemon::run_child::run_supervised(
                        prepared,
                        recorder,
                        approval_mode,
                        event_sender,
                        cancel,
                    )))
                }
                (None, _) => RunCompletion::Published(Err(AppError::SupervisedRun(
                    "daemon run omitted its event transport".into(),
                ))),
                (_, None) => RunCompletion::Published(Err(AppError::SupervisedRun(
                    "daemon run omitted its cancellation token".into(),
                ))),
            }
        }
        Err(error) => RunCompletion::Published(Err(error)),
    };
    finish_run_after_event_collection(runtime, record, completion, event_collector)
}

enum RunCompletion {
    Published(AppResult<RunOutcome>),
    Supervised(Box<crate::daemon::run_child::SupervisedRunCompletion>),
}

impl From<AppResult<RunOutcome>> for RunCompletion {
    fn from(outcome: AppResult<RunOutcome>) -> Self {
        Self::Published(outcome)
    }
}

impl From<crate::daemon::run_child::SupervisedRunCompletion> for RunCompletion {
    fn from(completion: crate::daemon::run_child::SupervisedRunCompletion) -> Self {
        Self::Supervised(Box::new(completion))
    }
}

fn finish_run_after_event_collection(
    runtime: &DaemonRuntime,
    record: &RunRecord,
    completion: impl Into<RunCompletion>,
    event_collector: thread::JoinHandle<()>,
) -> AppResult<RunOutcome> {
    let completion = match (completion.into(), event_collector.join()) {
        (completion, Ok(())) => completion,
        (RunCompletion::Published(_), Err(_)) => {
            RunCompletion::Published(Err(AppError::RunFailed(EVENT_COLLECTOR_PANIC.into())))
        }
        (RunCompletion::Supervised(completion), Err(_)) => RunCompletion::Supervised(Box::new(
            (*completion).override_failure(AppError::RunFailed(EVENT_COLLECTOR_PANIC.into())),
        )),
    };
    let outcome = match completion {
        RunCompletion::Published(outcome) => outcome,
        RunCompletion::Supervised(completion) => {
            let (outcome, terminal) = (*completion).publish();
            match terminal {
                Ok(recorded) => {
                    collect_run_event(record, RunEvent::Ledger(recorded));
                    outcome
                }
                Err(error) => Err(error),
            }
        }
    };
    match &outcome {
        Ok(outcome) => runtime.finish_run(
            record,
            outcome.final_answer.clone(),
            outcome.completion_claim.clone(),
        ),
        Err(error) => runtime.finish_run_with_error(record, error),
    }
    outcome
}

fn handle_shutdown_if_idle(runtime: &DaemonRuntime, request: Envelope) -> Envelope {
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

fn shutting_down_response(request_id: Option<String>, method: &'static str) -> Envelope {
    Envelope::error(
        request_id,
        Some(method.into()),
        ERROR_DAEMON_SHUTTING_DOWN,
        "daemon shutdown is already in progress",
    )
}

fn run_start_result(record: &RunRecord) -> RunStartResult {
    let status = record.status();
    RunStartResult {
        run_id: record.run_id.clone(),
        session_id: record.session_id.clone(),
        ledger_path: record.ledger_path.to_string_lossy().into_owned(),
        status: status.state,
        final_answer: status.final_answer,
        completion_claim: status.completion_claim,
    }
}

fn handle_events_stream(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: EventsStreamParams,
) -> Envelope {
    let record = match find_run(runtime, &params.run_id) {
        Ok(record) => record,
        Err(error) => return error_response(request.id, "events.stream", error),
    };
    let limit = params.limit.unwrap_or(DEFAULT_EVENT_LIMIT);
    if limit > MAX_EVENT_LIMIT {
        return Envelope::error(
            request.id,
            Some("events.stream".into()),
            ERROR_OVERLOAD,
            format!("event stream limit exceeds maximum {MAX_EVENT_LIMIT}: {limit}"),
        );
    }
    let result = {
        // Terminal status is published only after collection, so snapshot status before events.
        let status = record.status.lock().expect("run status lock poisoned");
        let buffer = record.events.lock().expect("event buffer lock poisoned");
        #[cfg(test)]
        record.wait_during_event_snapshot();
        let from_offset = params.from_offset.unwrap_or(buffer.next_offset);
        if from_offset < buffer.first_offset {
            return Envelope::error(
                request.id,
                Some("events.stream".into()),
                ERROR_LAGGED,
                format!(
                    "requested offset {from_offset} is no longer buffered; first available is {}",
                    buffer.first_offset
                ),
            );
        }
        let start = (from_offset - buffer.first_offset) as usize;
        let events = buffer
            .events
            .iter()
            .skip(start)
            .take(limit)
            .cloned()
            .collect::<Vec<_>>();
        let next_offset = from_offset + events.len() as u64;
        EventsStreamResult {
            run_id: record.run_id.clone(),
            from_offset,
            next_offset,
            status: status.state,
            events,
        }
    };
    Envelope::typed_response(request.id, ProtocolResponse::EventsStream(result))
}

fn handle_approval_decide(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: ApprovalDecideParams,
) -> Envelope {
    let record = match find_run(runtime, &params.run_id) {
        Ok(record) => record,
        Err(error) => return error_response(request.id, "approval.decide", error),
    };
    let mut approvals = record.approvals.lock().expect("approvals lock poisoned");
    let status = record.status.lock().expect("run status lock poisoned");
    if record.cancel.load(Ordering::SeqCst) || status.state != RunStateName::Running {
        return Envelope::error(
            request.id,
            Some("approval.decide".into()),
            ERROR_NOT_FOUND,
            format!("pending approval not found: {}", params.tool_call_id),
        );
    }
    let pending = match approvals.get_mut(&params.tool_call_id) {
        Some(pending) => pending,
        None => {
            return Envelope::error(
                request.id,
                Some("approval.decide".into()),
                ERROR_NOT_FOUND,
                format!("pending approval not found: {}", params.tool_call_id),
            );
        }
    };
    if pending.request.run_id.as_str() != record.run_id
        || pending.session_id != record.session_id
        || pending.request.call_id.as_str() != params.tool_call_id
    {
        return Envelope::error(
            request.id,
            Some("approval.decide".into()),
            ERROR_NOT_FOUND,
            format!("pending approval not found: {}", params.tool_call_id),
        );
    }
    if let Some(existing) = &pending.decision {
        if existing.decision == params.decision {
            return Envelope::typed_response(
                request.id,
                ProtocolResponse::ApprovalDecide(CommandAcceptedResult {
                    run_id: record.run_id.clone(),
                    status: status.state,
                }),
            );
        }
        return Envelope::error(
            request.id,
            Some("approval.decide".into()),
            ERROR_NOT_FOUND,
            format!("pending approval not found: {}", params.tool_call_id),
        );
    }
    let outcome = match params.decision {
        ApprovalDecision::Grant => ExternalApprovalOutcome::Granted { actor: "daemon" },
        ApprovalDecision::GrantSession => {
            if pending.request.tool_name != SHELL_EXEC
                || pending.request.effect != EffectClass::ExternalSideEffect
            {
                return Envelope::error(
                    request.id,
                    Some("approval.decide".into()),
                    ERROR_NOT_FOUND,
                    format!("pending approval not found: {}", params.tool_call_id),
                );
            }
            runtime.install_shell_session_grant(&record.session_id);
            ExternalApprovalOutcome::Granted {
                actor: "tui_session_grant",
            }
        }
        ApprovalDecision::Deny => ExternalApprovalOutcome::Denied {
            actor: "daemon",
            reason: params
                .reason
                .unwrap_or_else(|| "approval denied by daemon client".into()),
        },
    };
    // Record the answer beside the question, so the decision survives the
    // daemon exactly as the request does (#435). A failure here is reported
    // rather than swallowed: an unrecorded decision would leave the approval
    // looking unanswered forever.
    if let Err(error) =
        record_approval_decision(runtime, &record.run_id, &params.tool_call_id, &outcome)
    {
        return Envelope::error(
            request.id,
            Some("approval.decide".into()),
            ERROR_INTERNAL,
            format!("approval decision could not be recorded: {error}"),
        );
    }
    pending.decision = Some(crate::daemon::runtime::PendingApprovalDecision {
        decision: params.decision,
        outcome,
    });
    record.approval_changed.notify_all();
    drop(status);
    drop(approvals);
    Envelope::typed_response(
        request.id,
        ProtocolResponse::ApprovalDecide(CommandAcceptedResult {
            run_id: record.run_id.clone(),
            status: record.status().state,
        }),
    )
}

fn workspace_summary(record: &crate::server_store::WorkspaceRecord) -> WorkspaceSummary {
    WorkspaceSummary {
        id: record.id.clone(),
        name: record.name.clone(),
        root: record.root.clone(),
        ledger_path: record.ledger_path.clone(),
        created_at_ms: record.created_at_ms,
        health: match record.health() {
            crate::server_store::WorkspaceHealth::Present => WorkspaceHealthName::Present,
            crate::server_store::WorkspaceHealth::Broken => WorkspaceHealthName::Broken,
        },
    }
}

/// `workspace.create` — name a directory so the server knows it deliberately.
///
/// The directory must exist. Creating a workspace over a missing directory
/// would mint a record that is broken the moment it is written, which is worse
/// than refusing (P021).
fn handle_workspace_create(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: WorkspaceCreateParams,
) -> Envelope {
    let name = params.name.trim();
    if name.is_empty() {
        return Envelope::error(
            request.id,
            Some("workspace.create".into()),
            ERROR_MALFORMED_REQUEST,
            "workspace name must not be empty",
        );
    }
    let root = std::path::Path::new(&params.root);
    if !root.is_dir() {
        return Envelope::error(
            request.id,
            Some("workspace.create".into()),
            ERROR_MALFORMED_REQUEST,
            format!("workspace root is not a directory: {}", params.root),
        );
    }
    let root = match root.canonicalize() {
        Ok(root) => root,
        Err(error) => {
            return Envelope::error(
                request.id,
                Some("workspace.create".into()),
                ERROR_MALFORMED_REQUEST,
                format!("workspace root could not be resolved: {error}"),
            );
        }
    };
    let mut store = match runtime.paths.server_store() {
        Ok(store) => store,
        Err(error) => return store_error(request.id, "workspace.create", error),
    };
    match store.workspace_by_name(name) {
        Ok(Some(_)) => {
            return Envelope::error(
                request.id,
                Some("workspace.create".into()),
                ERROR_MALFORMED_REQUEST,
                format!("workspace already exists: {name}"),
            );
        }
        Ok(None) => {}
        Err(error) => return store_error(request.id, "workspace.create", error),
    }
    match store.workspace_by_root(&root.to_string_lossy()) {
        Ok(Some(existing)) => {
            return Envelope::error(
                request.id,
                Some("workspace.create".into()),
                ERROR_MALFORMED_REQUEST,
                format!(
                    "workspace root is already registered as {}: {}",
                    existing.name,
                    root.display()
                ),
            );
        }
        Ok(None) => {}
        Err(error) => return store_error(request.id, "workspace.create", error),
    }
    let now_ms = crate::thread_authority::now_ms();
    let ledger_path = match crate::paths::default_sqlite_path(&root) {
        Ok(path) => path,
        Err(error) => return store_error(request.id, "workspace.create", error),
    };
    match store.register_workspace(
        &crate::server_store::mint_workspace_id(name, now_ms),
        name,
        &root.to_string_lossy(),
        &ledger_path.to_string_lossy(),
        now_ms,
    ) {
        Ok((record, true)) => Envelope::typed_response(
            request.id,
            ProtocolResponse::WorkspaceCreate(WorkspaceCreateResult {
                workspace: workspace_summary(&record),
            }),
        ),
        Ok((existing, false)) if existing.name == name => Envelope::error(
            request.id,
            Some("workspace.create".into()),
            ERROR_MALFORMED_REQUEST,
            format!("workspace already exists: {name}"),
        ),
        Ok((existing, false)) => Envelope::error(
            request.id,
            Some("workspace.create".into()),
            ERROR_MALFORMED_REQUEST,
            format!(
                "workspace root is already registered as {}: {}",
                existing.name,
                root.display()
            ),
        ),
        Err(error) => store_error(request.id, "workspace.create", error),
    }
}

/// `workspace.list` — every registered workspace, broken ones included.
fn handle_workspace_list(
    runtime: &DaemonRuntime,
    request: Envelope,
    _params: WorkspaceListParams,
) -> Envelope {
    let store = match runtime.paths.server_store() {
        Ok(store) => store,
        Err(error) => return store_error(request.id, "workspace.list", error),
    };
    match store.workspaces() {
        Ok(records) => Envelope::typed_response(
            request.id,
            ProtocolResponse::WorkspaceList(WorkspaceListResult {
                workspaces: records.iter().map(workspace_summary).collect(),
            }),
        ),
        Err(error) => store_error(request.id, "workspace.list", error),
    }
}

/// `workspace.status` — one workspace by minted id.
fn handle_workspace_status(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: WorkspaceStatusParams,
) -> Envelope {
    let store = match runtime.paths.server_store() {
        Ok(store) => store,
        Err(error) => return store_error(request.id, "workspace.status", error),
    };
    match store.workspace(&params.workspace_id) {
        Ok(Some(record)) => Envelope::typed_response(
            request.id,
            ProtocolResponse::WorkspaceStatus(WorkspaceStatusResult {
                workspace: workspace_summary(&record),
            }),
        ),
        Ok(None) => Envelope::error(
            request.id,
            Some("workspace.status".into()),
            ERROR_NOT_FOUND,
            format!("workspace not found: {}", params.workspace_id),
        ),
        Err(error) => store_error(request.id, "workspace.status", error),
    }
}

fn agent_summary(record: &AgentRecord) -> AgentSummary {
    AgentSummary {
        id: record.id.clone(),
        workspace_id: record.workspace_id.clone(),
        model: record.model.clone(),
        reasoning_effort: record.reasoning_effort,
        approval_policy: record.approval_policy,
        toolset: record.toolset.clone(),
        created_at_ms: record.created_at_ms,
    }
}

fn handle_agent_create(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: AgentCreateParams,
) -> Envelope {
    if params.model.trim().is_empty() {
        return Envelope::error(
            request.id,
            Some("agent.create".into()),
            ERROR_MALFORMED_REQUEST,
            "agent model must not be empty",
        );
    }
    if params.toolset.is_empty() {
        return Envelope::error(
            request.id,
            Some("agent.create".into()),
            ERROR_MALFORMED_REQUEST,
            "agent toolset must not be empty",
        );
    }
    if let Some(tool) = params.toolset.iter().find(|tool| !is_known_tool(tool)) {
        return Envelope::error(
            request.id,
            Some("agent.create".into()),
            ERROR_MALFORMED_REQUEST,
            format!("unknown tool in agent toolset: {tool}"),
        );
    }
    let store = match runtime.paths.server_store() {
        Ok(store) => store,
        Err(error) => return store_error(request.id, "agent.create", error),
    };
    match store.workspace(&params.workspace_id) {
        Ok(Some(workspace))
            if workspace.health() == crate::server_store::WorkspaceHealth::Present => {}
        Ok(Some(_)) => {
            return Envelope::error(
                request.id,
                Some("agent.create".into()),
                ERROR_WORKSPACE_BROKEN,
                format!("workspace directory is missing: {}", params.workspace_id),
            );
        }
        Ok(None) => {
            return Envelope::error(
                request.id,
                Some("agent.create".into()),
                ERROR_NOT_FOUND,
                format!("workspace not found: {}", params.workspace_id),
            );
        }
        Err(error) => return store_error(request.id, "agent.create", error),
    }
    match store.agent(&params.agent_id) {
        Ok(Some(_)) => {
            return Envelope::error(
                request.id,
                Some("agent.create".into()),
                ERROR_MALFORMED_REQUEST,
                format!("agent already exists: {}", params.agent_id),
            );
        }
        Ok(None) => {}
        Err(error) => return store_error(request.id, "agent.create", error),
    }
    let record = AgentRecord {
        id: params.agent_id,
        workspace_id: params.workspace_id,
        model: params.model,
        reasoning_effort: params.reasoning_effort,
        approval_policy: params.approval_policy,
        toolset: params.toolset,
        created_at_ms: now_ms(),
    };
    match store.register_agent(&record) {
        Ok(true) => Envelope::typed_response(
            request.id,
            ProtocolResponse::AgentCreate(AgentCreateResult {
                agent: agent_summary(&record),
            }),
        ),
        Ok(false) => Envelope::error(
            request.id,
            Some("agent.create".into()),
            ERROR_MALFORMED_REQUEST,
            format!("agent already exists: {}", record.id),
        ),
        Err(error) => store_error(request.id, "agent.create", error),
    }
}

fn handle_agent_list(
    runtime: &DaemonRuntime,
    request: Envelope,
    _params: AgentListParams,
) -> Envelope {
    let store = match runtime.paths.server_store() {
        Ok(store) => store,
        Err(error) => return store_error(request.id, "agent.list", error),
    };
    match store.agents() {
        Ok(records) => Envelope::typed_response(
            request.id,
            ProtocolResponse::AgentList(AgentListResult {
                agents: records.iter().map(agent_summary).collect(),
            }),
        ),
        Err(error) => store_error(request.id, "agent.list", error),
    }
}

fn handle_agent_status(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: AgentStatusParams,
) -> Envelope {
    let store = match runtime.paths.server_store() {
        Ok(store) => store,
        Err(error) => return store_error(request.id, "agent.status", error),
    };
    match store.agent(&params.agent_id) {
        Ok(Some(record)) => Envelope::typed_response(
            request.id,
            ProtocolResponse::AgentStatus(AgentStatusResult {
                agent: agent_summary(&record),
            }),
        ),
        Ok(None) => Envelope::error(
            request.id,
            Some("agent.status".into()),
            ERROR_NOT_FOUND,
            format!("agent not found: {}", params.agent_id),
        ),
        Err(error) => store_error(request.id, "agent.status", error),
    }
}

fn record_approval_decision(
    runtime: &DaemonRuntime,
    run_id: &str,
    call_id: &str,
    outcome: &ExternalApprovalOutcome,
) -> AppResult<()> {
    let (granted, actor, reason) = match outcome {
        ExternalApprovalOutcome::Granted { actor } => (true, (*actor).to_owned(), None),
        ExternalApprovalOutcome::Denied { actor, reason } => {
            (false, (*actor).to_owned(), Some(reason.clone()))
        }
    };
    runtime.paths.server_store()?.resolve_tool_call_approval(
        run_id,
        call_id,
        &crate::server_store::ToolCallApprovalDecision {
            granted,
            actor,
            reason,
            decided_at_ms: crate::thread_authority::now_ms(),
        },
    )?;
    Ok(())
}

fn handle_run_cancel(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: RunCancelParams,
) -> Envelope {
    let record = match find_run(runtime, &params.run_id) {
        Ok(record) => record,
        Err(error) => return error_response(request.id, "run.cancel", error),
    };
    let Some(status) = record.request_cancel() else {
        return error_response(
            request.id,
            "run.cancel",
            format!("run is not active: {}", record.run_id),
        );
    };
    Envelope::typed_response(
        request.id,
        ProtocolResponse::RunCancel(CommandAcceptedResult {
            run_id: record.run_id.clone(),
            status,
        }),
    )
}

fn handle_sessions_list(runtime: &DaemonRuntime, request: Envelope) -> Envelope {
    match session_summaries(runtime) {
        Ok(sessions) => Envelope::typed_response(
            request.id,
            ProtocolResponse::SessionsList(SessionsListResult { sessions }),
        ),
        Err(error) => Envelope::error(
            request.id,
            Some("sessions.list".into()),
            ERROR_SESSIONS_LIST_FAILED,
            error.to_string(),
        ),
    }
}

fn session_summaries(runtime: &DaemonRuntime) -> crate::AppResult<Vec<SessionSummary>> {
    let ledger_path = runtime.paths.ledger_path.clone();
    let mut sessions =
        crate::ledger::default_sqlite_session_summaries(&runtime.paths.default_ledger())?
            .into_iter()
            .map(|session| SessionSummary {
                session_id: session.session_id,
                run_id: session.run_id,
                status: session.status,
                latest_question: latest_question_preview(&session.latest_question),
                first_question: session.first_question,
                updated_at_ms: session.updated_at_ms,
                ledger_path: ledger_path.to_string_lossy().into_owned(),
            })
            .collect::<Vec<_>>();

    let state = runtime.state.lock().expect("runtime state lock poisoned");
    let active_sessions = state
        .runs
        .values()
        .filter_map(|record| {
            let status = record.status();
            if !matches!(
                status.state,
                RunStateName::Running | RunStateName::CancelRequested
            ) {
                return None;
            }
            Some(SessionSummary {
                session_id: record.session_id.clone(),
                run_id: record.run_id.clone(),
                status: status.state,
                latest_question: String::new(),
                first_question: String::new(),
                updated_at_ms: 0,
                ledger_path: record.ledger_path.to_string_lossy().into_owned(),
            })
        })
        .collect::<Vec<_>>();

    for session in &mut sessions {
        if session.status == RunStateName::Running
            && !active_sessions
                .iter()
                .any(|active| active.session_id == session.session_id)
        {
            session.status = RunStateName::Interrupted;
        }
    }

    for summary in active_sessions {
        if let Some(existing) = sessions
            .iter_mut()
            .find(|session| session.session_id == summary.session_id)
        {
            existing.run_id = summary.run_id;
            existing.status = summary.status;
            existing.ledger_path = summary.ledger_path;
        } else {
            // wait=false runs can be visible before begin_session_run persists the question.
            sessions.insert(0, summary);
        }
    }

    Ok(sessions)
}

fn latest_question_preview(question: &str) -> String {
    let line = question.lines().next().unwrap_or_default();
    if line.chars().count() <= LATEST_QUESTION_MAX_CHARS {
        return line.to_owned();
    }
    format!(
        "{}...",
        line.chars()
            .take(LATEST_QUESTION_MAX_CHARS)
            .collect::<String>()
    )
}

fn handle_transcript_read(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: TranscriptReadParams,
) -> Envelope {
    let transcript = if let Some(run_id) = params.run_id {
        read_run_transcript(&runtime.paths.default_ledger(), &run_id)
    } else if let Some(session_id) = params.session_id {
        read_session_transcript(&runtime.paths.default_ledger(), &session_id)
    } else {
        return Envelope::error(
            request.id,
            Some("transcript.read".into()),
            ERROR_MALFORMED_REQUEST,
            "run_id or session_id is required",
        );
    };
    match transcript {
        Ok(mut transcript) => {
            transcript.pending_approval = runtime_pending_approval(runtime, &transcript.run_id);
            Envelope::typed_response(request.id, ProtocolResponse::TranscriptRead(transcript))
        }
        Err(error) => Envelope::error(
            request.id,
            Some("transcript.read".into()),
            transcript_error_code(&error),
            error.to_string(),
        ),
    }
}

fn runtime_pending_approval(
    runtime: &DaemonRuntime,
    run_id: &str,
) -> Option<crate::daemon::protocol::PendingApprovalSnapshot> {
    let record = runtime
        .state
        .lock()
        .expect("runtime state lock poisoned")
        .runs
        .get(run_id)
        .cloned();
    match record.and_then(|record| record.pending_approval()) {
        Some(snapshot) => Some(snapshot),
        // The run is not loaded, which is the state after a restart. The
        // approval outlived its daemon, so read it from disk (#435).
        None => restored_pending_approval(runtime, run_id),
    }
}

fn restored_pending_approval(
    runtime: &DaemonRuntime,
    run_id: &str,
) -> Option<crate::daemon::protocol::PendingApprovalSnapshot> {
    let store = runtime.paths.server_store().ok()?;
    let approval = store
        .pending_tool_call_approvals()
        .ok()?
        .into_iter()
        .find(|approval| approval.run_id == run_id)?;
    Some(crate::daemon::protocol::PendingApprovalSnapshot {
        run_id: approval.run_id,
        tool_call_id: approval.call_id,
        tool_name: approval.tool_name,
        effect: approval.effect,
        reason: Some(approval.reason),
        input_preview: approval.input_preview,
        approval_preview: approval.approval_preview,
        diff_preview: approval.diff_preview,
    })
}

fn read_run_transcript(path: &DefaultSqlitePath, run_id: &str) -> AppResult<TranscriptReadResult> {
    if std::fs::symlink_metadata(path.as_path())
        .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
    {
        return Err(AppError::RunNotFound(run_id.into()));
    }
    let run = SqliteLedger::open_default_readonly(path)?.read_session_run(run_id)?;
    let readback = RunReadback::from_events(&run.records)?;
    let transcript = format_readback(&readback);
    Ok(TranscriptReadResult {
        run_id: run.run_id.clone(),
        status: run.status,
        final_answer: run.final_answer.clone(),
        transcript,
        typed: Some(TypedTranscript {
            runs: vec![typed_run(&run, readback.entries)],
        }),
        pending_approval: None,
        completion_claim: None,
    })
}

fn read_session_transcript(
    path: &DefaultSqlitePath,
    session_id: &str,
) -> AppResult<TranscriptReadResult> {
    if std::fs::symlink_metadata(path.as_path())
        .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
    {
        return Err(AppError::SessionNotFound(session_id.into()));
    }
    let session = SqliteLedger::open_default_readonly(path)?.read_session(session_id)?;
    let latest = session
        .runs
        .last()
        .ok_or_else(|| AppError::SessionNotFound(session_id.into()))?;
    let transcript = format_session_readback(&session)?;
    let typed_runs = session
        .runs
        .iter()
        .map(|run| {
            let readback = RunReadback::from_events(&run.records)?;
            Ok(typed_run(run, readback.entries))
        })
        .collect::<AppResult<Vec<_>>>()?;
    Ok(TranscriptReadResult {
        run_id: latest.run_id.clone(),
        status: latest.status,
        final_answer: latest.final_answer.clone(),
        transcript,
        typed: Some(TypedTranscript { runs: typed_runs }),
        pending_approval: None,
        completion_claim: None,
    })
}

fn typed_run(run: &SessionRunRecords, readback_entries: Vec<ReadbackEntry>) -> TypedRun {
    TypedRun {
        run_id: run.run_id.clone(),
        session_index: run.session_index,
        status: run.status,
        model_status: model_status(&run.records),
        entries: typed_entries(&run.question, readback_entries),
    }
}

fn model_status(records: &[RecordedEvent]) -> Option<ModelIdentityStatus> {
    records.iter().rev().find_map(|record| match &record.event {
        HarnessEvent::ModelRequested { model, .. } => Some(ModelIdentityStatus::Requested {
            model: model.to_string(),
        }),
        HarnessEvent::ModelResponded { served_model, .. } => Some(ModelIdentityStatus::Responded {
            served_model: served_model.as_ref().map(ToString::to_string),
        }),
        _ => None,
    })
}

fn typed_entries(
    question: &str,
    readback_entries: Vec<ReadbackEntry>,
) -> Vec<TypedTranscriptEntry> {
    let mut entries = Vec::with_capacity(readback_entries.len() + 1);
    entries.push(TypedTranscriptEntry::User {
        text: question.into(),
    });
    for entry in readback_entries {
        let entry = match entry {
            ReadbackEntry::ContextFragment { .. }
            | ReadbackEntry::ContextCompacted { .. }
            | ReadbackEntry::ModelFailed { .. }
            | ReadbackEntry::ToolProposalsRejected { .. } => continue,
            ReadbackEntry::ModelMessage { message, .. } => TypedTranscriptEntry::Assistant {
                text: message.content,
            },
            ReadbackEntry::ToolCall { call, .. } => TypedTranscriptEntry::ToolCall {
                call_id: call.id.to_string(),
                tool: call.tool.to_string(),
                input: call.input,
            },
            ReadbackEntry::ToolResult { result } => TypedTranscriptEntry::ToolResult {
                call_id: result.call_id.to_string(),
                summary: result.summary,
            },
            ReadbackEntry::PolicyDenied { call_id, reason } => TypedTranscriptEntry::PolicyDenied {
                call_id: call_id.to_string(),
                reason,
            },
            ReadbackEntry::ApprovalGranted { call_id, actor_id } => {
                TypedTranscriptEntry::Approval {
                    call_id: call_id.to_string(),
                    decision: ApprovalDecisionName::Granted,
                    actor_id: actor_id.to_string(),
                    reason: None,
                }
            }
            ReadbackEntry::ApprovalDenied {
                call_id,
                actor_id,
                reason,
            } => TypedTranscriptEntry::Approval {
                call_id: call_id.to_string(),
                decision: ApprovalDecisionName::Denied,
                actor_id: actor_id.to_string(),
                reason: Some(reason),
            },
            ReadbackEntry::ToolFailed { call_id, reason } => TypedTranscriptEntry::ToolFailed {
                call_id: call_id.to_string(),
                error: reason,
            },
        };
        entries.push(entry);
    }
    entries
}

fn transcript_error_code(error: &AppError) -> ProtocolErrorCode {
    match error {
        AppError::RunNotFound(_)
        | AppError::SessionNotFound(_)
        | AppError::NoSqliteRuns
        | AppError::NoSqliteSessions => ERROR_NOT_FOUND,
        _ => ERROR_INTERNAL,
    }
}

fn latest_session_id(runtime: &DaemonRuntime) -> Result<String, String> {
    crate::ledger::latest_default_sqlite_session_id(&runtime.paths.default_ledger()).map_err(
        |error| match error {
            crate::AppError::NoSqliteSessions | crate::AppError::NoSqliteRuns => {
                "no previous session exists".into()
            }
            error => error.to_string(),
        },
    )
}

fn spawn_event_collector(
    record: Arc<RunRecord>,
    receiver: mpsc::Receiver<RunEvent>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        for event in receiver {
            collect_run_event(&record, event);
        }
    })
}

fn collect_run_event(record: &RunRecord, event: RunEvent) {
    match event {
        RunEvent::Ledger(recorded) => record.push_recorded_event(recorded),
        RunEvent::AssistantDelta(delta) => record.push_assistant_delta(delta),
    }
}

fn find_run(runtime: &DaemonRuntime, run_id: &str) -> Result<Arc<RunRecord>, String> {
    runtime
        .state
        .lock()
        .expect("runtime state lock poisoned")
        .runs
        .get(run_id)
        .cloned()
        .ok_or_else(|| format!("run not found: {run_id}"))
}

/// A failure to reach the server-wide store is internal, not a missing record.
fn store_error(request_id: Option<String>, method: &'static str, error: AppError) -> Envelope {
    Envelope::error(
        request_id,
        Some(method.into()),
        ERROR_INTERNAL,
        error.to_string(),
    )
}

fn error_response(request_id: Option<String>, method: &'static str, message: String) -> Envelope {
    Envelope::error(request_id, Some(method.into()), ERROR_NOT_FOUND, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ApprovalRequest, AssistantDeltaEvent,
        daemon::protocol::StreamEvent,
        daemon::runtime::{PendingApproval, PendingApprovalDecision},
    };
    use platonic_core::{
        ActorId, ContextFragment, ContextLane, EffectClass, HarnessEvent, Message, MessageRole,
        ModelName, RecordedEvent, ResultVisibility, RunId, ToolCall, ToolCallId, ToolName,
        ToolResult, TurnId,
    };
    #[cfg(target_os = "linux")]
    use platonic_core::{AgentId, ContextPack};
    use serde_json::json;
    #[cfg(target_os = "linux")]
    use std::os::unix::fs::PermissionsExt;
    use std::{
        sync::{Arc, Barrier},
        time::Duration,
    };

    fn response_result<T: serde::de::DeserializeOwned>(response: &Envelope) -> T {
        let response = serde_json::to_value(response.result.as_ref().unwrap()).unwrap();
        serde_json::from_value(response["result"].clone()).unwrap()
    }

    fn bare_thread_test_runtime() -> (tempfile::TempDir, DaemonRuntime) {
        let root = tempfile::tempdir().unwrap();
        let workspace_root = root.path().join("workspace");
        std::fs::create_dir(&workspace_root).unwrap();
        let ledger_path = root
            .path()
            .join("state")
            .join("platonic")
            .join("workspaces")
            .join("thread-tests")
            .join("agent.db");
        let runtime = DaemonRuntime::new(crate::daemon::server::DaemonPaths {
            workspace_root: workspace_root.canonicalize().unwrap(),
            workspace_id: "thread-tests".into(),
            socket_path: root.path().join("agent.sock"),
            lock_path: root.path().join("agent.lock"),
            server_db_path: root.path().join("server.db"),
            ledger_path,
        });
        (root, runtime)
    }

    fn thread_test_runtime() -> (tempfile::TempDir, DaemonRuntime) {
        let (root, runtime) = bare_thread_test_runtime();
        runtime
            .paths
            .server_store()
            .unwrap()
            .register_workspace(
                "workspace-thread-tests",
                "thread-tests",
                &runtime.paths.workspace_root.to_string_lossy(),
                &runtime.paths.ledger_path.to_string_lossy(),
                1,
            )
            .unwrap();
        (root, runtime)
    }

    fn workspace_request(id: &str, method: &str, params: serde_json::Value) -> Envelope {
        serde_json::from_value(json!({
            "v": 1,
            "id": id,
            "kind": "request",
            "method": method,
            "params": params,
        }))
        .unwrap()
    }

    fn handle_workspace_line(
        runtime: &DaemonRuntime,
        id: &str,
        method: &str,
        params: serde_json::Value,
    ) -> Envelope {
        handle_line(
            runtime,
            &json!({
                "v": 1,
                "id": id,
                "kind": "request",
                "method": method,
                "params": params,
            })
            .to_string(),
        )
    }

    /// A workspace can be created, listed and inspected end to end, keeps a
    /// stable identity when it moves, and is reported broken rather than
    /// omitted when its directory vanishes (P021).
    #[test]
    fn workspace_is_created_listed_inspected_and_stays_itself_when_moved_or_broken() {
        let (root, runtime) = bare_thread_test_runtime();
        let first = root.path().join("first");
        std::fs::create_dir(&first).unwrap();

        let created = handle_request(
            &runtime,
            workspace_request(
                "c1",
                "workspace.create",
                json!({"name": "alpha", "root": first.to_string_lossy()}),
            ),
        );
        assert_eq!(
            created.kind,
            crate::daemon::protocol::EnvelopeKind::Response
        );
        let created: WorkspaceCreateResult = response_result(&created);
        let id = created.workspace.id.clone();
        assert!(id.starts_with("ws-"), "id should be minted, got {id}");
        assert_eq!(created.workspace.name, "alpha");
        assert_eq!(created.workspace.health, WorkspaceHealthName::Present);

        // The same name twice is refused rather than silently duplicated.
        let duplicate = handle_request(
            &runtime,
            workspace_request(
                "c2",
                "workspace.create",
                json!({"name": "alpha", "root": first.to_string_lossy()}),
            ),
        );
        assert_eq!(duplicate.kind, crate::daemon::protocol::EnvelopeKind::Error);

        let duplicate_root = handle_request(
            &runtime,
            workspace_request(
                "c2-root",
                "workspace.create",
                json!({"name": "beta", "root": first.to_string_lossy()}),
            ),
        );
        let duplicate_root = duplicate_root.error.unwrap();
        assert_eq!(duplicate_root.code, ERROR_MALFORMED_REQUEST);
        assert!(
            duplicate_root
                .message
                .contains("already registered as alpha")
        );

        // A root that is not a directory is refused, never registered broken.
        let missing = handle_request(
            &runtime,
            workspace_request(
                "c3",
                "workspace.create",
                json!({"name": "ghost", "root": root.path().join("nope").to_string_lossy()}),
            ),
        );
        assert_eq!(missing.kind, crate::daemon::protocol::EnvelopeKind::Error);

        let listed = handle_request(
            &runtime,
            workspace_request("l1", "workspace.list", json!({})),
        );
        let listed: WorkspaceListResult = response_result(&listed);
        assert_eq!(
            listed
                .workspaces
                .iter()
                .map(|workspace| workspace.name.as_str())
                .collect::<Vec<_>>(),
            ["alpha"]
        );

        let status = handle_request(
            &runtime,
            workspace_request("s1", "workspace.status", json!({"workspace_id": id})),
        );
        let status: WorkspaceStatusResult = response_result(&status);
        assert_eq!(status.workspace, created.workspace);

        // Moving the directory keeps the identity: a relocation, not a new
        // workspace and not a reset history.
        let second = root.path().join("second");
        std::fs::rename(&first, &second).unwrap();
        let store = runtime.paths.server_store().unwrap();
        assert!(
            store
                .relocate_workspace(&id, &second.to_string_lossy())
                .unwrap()
        );
        drop(store);
        let moved = handle_request(
            &runtime,
            workspace_request("s2", "workspace.status", json!({"workspace_id": id})),
        );
        let moved: WorkspaceStatusResult = response_result(&moved);
        assert_eq!(moved.workspace.id, id);
        assert_eq!(
            moved.workspace.created_at_ms,
            created.workspace.created_at_ms
        );
        assert_eq!(moved.workspace.health, WorkspaceHealthName::Present);

        // A vanished directory is reported broken, never omitted.
        std::fs::remove_dir_all(&second).unwrap();
        let broken = handle_request(
            &runtime,
            workspace_request("l2", "workspace.list", json!({})),
        );
        let broken: WorkspaceListResult = response_result(&broken);
        assert_eq!(broken.workspaces.len(), 1);
        assert_eq!(broken.workspaces[0].health, WorkspaceHealthName::Broken);
        assert_eq!(broken.workspaces[0].id, id);
    }

    #[test]
    fn concurrent_workspace_create_returns_one_typed_duplicate_without_a_second_row() {
        let (root, runtime) = bare_thread_test_runtime();
        let workspace_root = root.path().join("concurrent-workspace");
        std::fs::create_dir(&workspace_root).unwrap();
        drop(runtime.paths.server_store().unwrap());
        let barrier = Arc::new(Barrier::new(3));
        let handles =
            [("alpha", "create-alpha"), ("beta", "create-beta")].map(|(name, request_id)| {
                let runtime = runtime.clone();
                let workspace_root = workspace_root.clone();
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    handle_request(
                        &runtime,
                        workspace_request(
                            request_id,
                            "workspace.create",
                            json!({"name": name, "root": workspace_root.to_string_lossy()}),
                        ),
                    )
                })
            });
        barrier.wait();
        let responses: Vec<_> = handles
            .map(|handle| handle.join().unwrap())
            .into_iter()
            .collect();

        assert_eq!(
            responses
                .iter()
                .filter(|response| {
                    response.kind == crate::daemon::protocol::EnvelopeKind::Response
                })
                .count(),
            1
        );
        let errors: Vec<_> = responses
            .iter()
            .filter_map(|response| response.error.as_ref())
            .collect();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].code, ERROR_MALFORMED_REQUEST);
        assert!(
            errors[0]
                .message
                .contains("workspace root is already registered")
        );
        assert_eq!(
            runtime
                .paths
                .server_store()
                .unwrap()
                .workspaces()
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn agent_create_list_and_status_are_typed_and_fail_before_partial_rows() {
        let (root, runtime) = bare_thread_test_runtime();
        let workspace_root = root.path().join("agent-workspace");
        std::fs::create_dir(&workspace_root).unwrap();
        let created_workspace = handle_request(
            &runtime,
            workspace_request(
                "wc",
                "workspace.create",
                json!({"name": "alpha", "root": workspace_root.to_string_lossy()}),
            ),
        );
        let workspace: WorkspaceCreateResult = response_result(&created_workspace);
        let workspace_id = workspace.workspace.id;
        let create_params = json!({
            "agent_id": "builder",
            "workspace_id": workspace_id,
            "model": "gpt-5.6-sol",
            "reasoning_effort": "xhigh",
            "approval_policy": "prompt",
            "toolset": ["file.read", "file.write"]
        });

        let created = handle_request(
            &runtime,
            workspace_request("ac", "agent.create", create_params.clone()),
        );
        let created: AgentCreateResult = response_result(&created);
        assert_eq!(created.agent.id.as_str(), "builder");
        assert_eq!(created.agent.workspace_id, workspace_id);
        assert_eq!(created.agent.model, "gpt-5.6-sol");
        assert_eq!(
            created.agent.reasoning_effort,
            crate::daemon::protocol::ReasoningEffort::Xhigh
        );
        assert_eq!(created.agent.approval_policy, ThreadApprovalPolicy::Prompt);
        assert_eq!(created.agent.toolset, ["file.read", "file.write"]);

        let listed = handle_request(&runtime, workspace_request("al", "agent.list", json!({})));
        let listed: AgentListResult = response_result(&listed);
        assert_eq!(listed.agents, [created.agent.clone()]);
        let status = handle_request(
            &runtime,
            workspace_request("as", "agent.status", json!({"agent_id": "builder"})),
        );
        let status: AgentStatusResult = response_result(&status);
        assert_eq!(status.agent, created.agent);

        let duplicate = handle_request(
            &runtime,
            workspace_request("dup", "agent.create", create_params),
        );
        assert_eq!(duplicate.error.unwrap().code, ERROR_MALFORMED_REQUEST);
        let invalid_id = handle_workspace_line(
            &runtime,
            "bad-id",
            "agent.create",
            json!({
                "agent_id": " ", "workspace_id": workspace_id,
                "model": "gpt-5.6-sol", "reasoning_effort": "none",
                "approval_policy": "prompt", "toolset": ["file.read"]
            }),
        );
        assert_eq!(invalid_id.error.unwrap().code, ERROR_MALFORMED_REQUEST);
        let invalid_tool = handle_request(
            &runtime,
            workspace_request(
                "bad-tool",
                "agent.create",
                json!({
                    "agent_id": "bad-tool", "workspace_id": workspace_id,
                    "model": "gpt-5.6-sol", "reasoning_effort": "none",
                    "approval_policy": "prompt", "toolset": ["credential.store"]
                }),
            ),
        );
        assert_eq!(invalid_tool.error.unwrap().code, ERROR_MALFORMED_REQUEST);
        let missing_workspace = handle_request(
            &runtime,
            workspace_request(
                "missing-workspace",
                "agent.create",
                json!({
                    "agent_id": "orphan", "workspace_id": "ws-missing",
                    "model": "gpt-5.6-sol", "reasoning_effort": "none",
                    "approval_policy": "prompt", "toolset": ["file.read"]
                }),
            ),
        );
        assert_eq!(missing_workspace.error.unwrap().code, ERROR_NOT_FOUND);
        let unknown_field = handle_workspace_line(
            &runtime,
            "unknown",
            "agent.create",
            json!({
                "agent_id": "unknown", "workspace_id": workspace_id,
                "model": "gpt-5.6-sol", "reasoning_effort": "none",
                "approval_policy": "prompt", "toolset": ["file.read"],
                "credential": "must-not-land"
            }),
        );
        assert_eq!(unknown_field.error.unwrap().code, ERROR_MALFORMED_REQUEST);
        let missing_agent = handle_request(
            &runtime,
            workspace_request(
                "missing-agent",
                "agent.status",
                json!({"agent_id": "missing"}),
            ),
        );
        assert_eq!(missing_agent.error.unwrap().code, ERROR_NOT_FOUND);

        std::fs::remove_dir_all(&workspace_root).unwrap();
        let broken_workspace = handle_request(
            &runtime,
            workspace_request(
                "broken",
                "agent.create",
                json!({
                    "agent_id": "broken", "workspace_id": workspace_id,
                    "model": "gpt-5.6-sol", "reasoning_effort": "none",
                    "approval_policy": "prompt", "toolset": ["file.read"]
                }),
            ),
        );
        assert_eq!(broken_workspace.error.unwrap().code, ERROR_WORKSPACE_BROKEN);

        let listed = handle_request(&runtime, workspace_request("al2", "agent.list", json!({})));
        let listed: AgentListResult = response_result(&listed);
        assert_eq!(listed.agents.len(), 1);
    }

    /// Unknown fields are rejected on the way in, not ignored.
    #[test]
    fn workspace_params_reject_unknown_fields() {
        let (root, runtime) = thread_test_runtime();
        let dir = root.path().join("ws");
        std::fs::create_dir(&dir).unwrap();
        let response = handle_workspace_line(
            &runtime,
            "bad",
            "workspace.create",
            json!({"name": "a", "root": dir.to_string_lossy(), "extra": true}),
        );
        assert_eq!(response.kind, crate::daemon::protocol::EnvelopeKind::Error);
        assert_eq!(response.error.unwrap().code, ERROR_MALFORMED_REQUEST);
    }

    #[test]
    fn thread_spawn_rejects_an_unregistered_workspace_with_the_attach_error() {
        let (_root, runtime) = bare_thread_test_runtime();
        let response = handle_request(
            &runtime,
            workspace_request(
                "spawn",
                "thread.spawn",
                json!({
                    "action": "start",
                    "parent_thread_id": null,
                    "cwd": runtime.paths.workspace_root.to_string_lossy(),
                    "model": "gpt-5.6-sol",
                    "reasoning_effort": "none",
                    "approval_policy": "prompt"
                }),
            ),
        );
        let error = response.error.unwrap();
        assert_eq!(error.code, ERROR_WORKSPACE_UNREGISTERED);
        assert!(error.message.contains("platonic workspace create"));
        assert!(
            runtime
                .paths
                .server_store()
                .unwrap()
                .thread_authorities()
                .unwrap()
                .is_empty()
        );
    }

    /// After a restart the run is gone from memory, but the approval it was
    /// blocked on is not. The existing snapshot field must report it, so a
    /// client returning to the terminal sees what is waiting (#435).
    #[test]
    fn restored_pending_approval_is_reported_when_the_run_is_not_loaded() {
        let (_root, runtime) = thread_test_runtime();
        let store = runtime.paths.server_store().unwrap();
        store
            .persist_tool_call_approval(&crate::server_store::ToolCallApprovalRecord {
                run_id: "run_restored".into(),
                call_id: "call_restored".into(),
                session_id: "session_restored".into(),
                tool_name: "shell_exec".into(),
                effect: platonic_core::EffectClass::ExternalSideEffect,
                reason: "writes outside the workspace".into(),
                input_preview: Some("git push".into()),
                approval_preview: Some("shell_exec: git push".into()),
                diff_preview: None,
                requested_at_ms: 4_200,
                decision: None,
            })
            .unwrap();
        drop(store);

        // Nothing is loaded: this is exactly the post-restart state.
        assert!(runtime.state.lock().unwrap().runs.is_empty());
        let snapshot = runtime_pending_approval(&runtime, "run_restored")
            .expect("restored approval should be reported");
        assert_eq!(snapshot.run_id, "run_restored");
        assert_eq!(snapshot.tool_call_id, "call_restored");
        assert_eq!(snapshot.tool_name, "shell_exec");
        assert_eq!(
            snapshot.effect,
            platonic_core::EffectClass::ExternalSideEffect
        );
        assert_eq!(
            snapshot.reason.as_deref(),
            Some("writes outside the workspace")
        );
        assert_eq!(snapshot.input_preview.as_deref(), Some("git push"));

        // A decided approval is no longer waiting on anyone.
        runtime
            .paths
            .server_store()
            .unwrap()
            .resolve_tool_call_approval(
                "run_restored",
                "call_restored",
                &crate::server_store::ToolCallApprovalDecision {
                    granted: false,
                    actor: "stdin".into(),
                    reason: Some("no".into()),
                    decided_at_ms: 4_300,
                },
            )
            .unwrap();
        assert!(runtime_pending_approval(&runtime, "run_restored").is_none());
    }

    fn start_thread(
        runtime: &DaemonRuntime,
        parent_thread_id: Option<String>,
        cwd: &Path,
        approval_policy: crate::daemon::protocol::ThreadApprovalPolicy,
    ) -> Result<ThreadSpawnResult, ThreadSpawnFailure> {
        thread_spawn(
            runtime,
            ThreadSpawnParams::Start {
                parent_thread_id,
                cwd: cwd.to_string_lossy().into_owned(),
                model: "gpt-5.6-sol".into(),
                reasoning_effort: crate::daemon::protocol::ReasoningEffort::Xhigh,
                approval_policy,
            },
        )
    }

    fn pending_spawn(result: ThreadSpawnResult) -> (String, String) {
        match result {
            ThreadSpawnResult::ApprovalRequired {
                spawn_id,
                thread_id,
                effect,
                reason,
            } => {
                assert_eq!(effect, EffectClass::WorkspaceWrite);
                assert_eq!(reason, THREAD_SPAWN_APPROVAL_REASON);
                (spawn_id, thread_id)
            }
            unexpected => panic!("expected approval-required spawn, got {unexpected:?}"),
        }
    }

    fn decide_thread(
        runtime: &DaemonRuntime,
        spawn_id: &str,
        approval: ThreadSpawnDecision,
    ) -> Result<ThreadSpawnResult, ThreadSpawnFailure> {
        thread_spawn(
            runtime,
            ThreadSpawnParams::Decide {
                spawn_id: spawn_id.into(),
                approval,
            },
        )
    }

    fn grant_thread(runtime: &DaemonRuntime, spawn_id: &str, actor: &str) -> ThreadStatus {
        match decide_thread(
            runtime,
            spawn_id,
            ThreadSpawnDecision::Grant {
                actor: actor.into(),
            },
        )
        .unwrap()
        {
            ThreadSpawnResult::Spawned { thread } => thread,
            unexpected => panic!("expected spawned thread, got {unexpected:?}"),
        }
    }

    fn stop_thread(runtime: &DaemonRuntime, thread_id: &str, actor: &str) -> Envelope {
        handle_line(
            runtime,
            &format!(
                r#"{{"v":1,"id":"stop","kind":"request","method":"thread.stop","params":{{"thread_id":"{thread_id}","actor":"{actor}"}}}}"#
            ),
        )
    }

    #[test]
    fn thread_spawn_becomes_live_only_after_complete_authority_is_durable() {
        let (_root, runtime) = thread_test_runtime();
        let (spawn_id, thread_id) = pending_spawn(
            start_thread(
                &runtime,
                None,
                &runtime.paths.workspace_root,
                crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
            )
            .unwrap(),
        );
        let store = runtime.paths.server_store().unwrap();
        assert!(store.thread_authority(&thread_id).unwrap().is_none());
        assert!(!runtime.thread_is_loaded(&thread_id));
        drop(store);

        let status = grant_thread(&runtime, &spawn_id, "stdin");
        assert_eq!(status.authority.thread_id, thread_id);
        assert_eq!(status.authority.spawning_actor, "stdin");
        assert_eq!(status.authority.parent_thread_id, None);
        assert_eq!(
            status.authority.cwd,
            runtime.paths.workspace_root.to_string_lossy()
        );
        let authority_response = handle_line(
            &runtime,
            &format!(
                r#"{{"v":1,"id":"authority","kind":"request","method":"thread.authority","params":{{"thread_id":"{thread_id}"}}}}"#
            ),
        );
        let authority: ThreadAuthorityResult = response_result(&authority_response);
        let authority = authority.authority;
        assert_eq!(authority.agent_id, Some(AgentId::new("plato").unwrap()));
        assert_eq!(
            authority.granted_paths,
            vec![crate::daemon::protocol::ThreadGrantedPath {
                path: runtime.paths.workspace_root.to_string_lossy().into_owned(),
                writable: true,
            }]
        );
        assert_eq!(
            authority.toolset,
            [
                "file.read",
                "file.list",
                "file.write",
                "file.edit",
                "shell.exec",
                "web.fetch",
            ]
        );
        assert!(authority.worktrees.is_empty());
        assert!(authority.network);
        assert_eq!(status.authority.model, "gpt-5.6-sol");
        assert_eq!(
            status.authority.reasoning_effort,
            crate::daemon::protocol::ReasoningEffort::Xhigh
        );
        assert_eq!(
            status.authority.approval_policy,
            crate::daemon::protocol::ThreadApprovalPolicy::Prompt
        );
        assert!(status.authority.created_at_ms > 0);
        assert_eq!(
            status.live,
            crate::daemon::protocol::ThreadLiveState {
                loaded: true,
                current_turn_id: None,
                last_activity_at_ms: Some(authority.created_at_ms),
            }
        );
        let store = runtime.paths.server_store().unwrap();
        assert_eq!(store.thread_authority(&thread_id).unwrap(), Some(authority));
        let approval = store.thread_spawn_approval(&spawn_id).unwrap().unwrap();
        assert_eq!(approval.decision, ThreadSpawnDecisionName::Granted);
        assert_eq!(approval.actor, status.authority.spawning_actor);
    }

    #[test]
    fn thread_spawn_denial_and_cancellation_leave_no_live_authority() {
        for (case, decision, expected) in [
            (
                "denied",
                ThreadSpawnDecision::Deny {
                    actor: "reviewer".into(),
                    reason: "not admitted".into(),
                },
                ThreadSpawnDecisionName::Denied,
            ),
            (
                "canceled",
                ThreadSpawnDecision::Cancel {
                    actor: "stdin".into(),
                },
                ThreadSpawnDecisionName::Canceled,
            ),
        ] {
            let (_root, runtime) = thread_test_runtime();
            let (spawn_id, thread_id) = pending_spawn(
                start_thread(
                    &runtime,
                    None,
                    &runtime.paths.workspace_root,
                    crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
                )
                .unwrap(),
            );
            let result = decide_thread(&runtime, &spawn_id, decision).unwrap();
            assert!(matches!(
                (&result, case),
                (ThreadSpawnResult::Denied { .. }, "denied")
                    | (ThreadSpawnResult::Canceled { .. }, "canceled")
            ));
            let store = runtime.paths.server_store().unwrap();
            let approval = store.thread_spawn_approval(&spawn_id).unwrap().unwrap();
            assert_eq!(approval.decision, expected);
            assert!(store.thread_authority(&thread_id).unwrap().is_none());
            assert!(!runtime.thread_is_loaded(&thread_id));
        }
    }

    fn admit_test_thread_turn(
        runtime: &DaemonRuntime,
        thread_id: &str,
        controller_id: &str,
        turn_id: &str,
    ) -> ThreadTurnBinding {
        match runtime.send_thread(
            thread_id,
            controller_id.into(),
            None,
            "test turn".into(),
            turn_id.into(),
        ) {
            ThreadSendAdmission::Started { turn, .. } => turn,
            admission => panic!("test thread turn was not admitted: {admission:?}"),
        }
    }

    #[test]
    fn idle_thread_stop_is_durable_idempotent_and_leaves_sibling_untouched() {
        let (_root, runtime) = thread_test_runtime();
        let mut threads = Vec::new();
        for _ in 0..2 {
            let (spawn_id, _) = pending_spawn(
                start_thread(
                    &runtime,
                    None,
                    &runtime.paths.workspace_root,
                    crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
                )
                .unwrap(),
            );
            threads.push(grant_thread(&runtime, &spawn_id, "stdin"));
        }
        let target_id = threads[0].authority.thread_id.clone();
        let sibling_id = threads[1].authority.thread_id.clone();
        let sibling_before = runtime.thread_live_state(&sibling_id);

        let stopped = stop_thread(&runtime, &target_id, "operator");
        let stopped: ThreadStopResult = response_result(&stopped);
        let stopped_at_ms = match stopped {
            ThreadStopResult::Stopped {
                thread_id,
                stopped_turn_id,
                stopped_at_ms,
            } => {
                assert_eq!(thread_id, target_id);
                assert_eq!(stopped_turn_id, None);
                stopped_at_ms
            }
            unexpected => panic!("expected stopped result, got {unexpected:?}"),
        };
        assert_eq!(
            runtime.thread_live_state(&target_id),
            crate::daemon::protocol::ThreadLiveState {
                loaded: false,
                current_turn_id: None,
                last_activity_at_ms: None,
            }
        );
        assert_eq!(runtime.thread_live_state(&sibling_id), sibling_before);
        let store = runtime.paths.server_store().unwrap();
        let durable = store.thread_stop(&target_id).unwrap().unwrap();
        assert_eq!(durable.actor, "operator");
        assert_eq!(durable.stopped_turn_id, None);
        assert_eq!(durable.occurred_at_ms, stopped_at_ms);
        drop(store);

        let repeated = stop_thread(&runtime, &target_id, "other_operator");
        assert_eq!(
            response_result::<ThreadStopResult>(&repeated),
            ThreadStopResult::AlreadyStopped {
                thread_id: target_id.clone(),
                stopped_turn_id: None,
                stopped_at_ms,
            }
        );
        assert!(runtime.thread_is_stopped(&target_id));
        assert_eq!(runtime.thread_live_state(&sibling_id), sibling_before);
    }

    #[test]
    fn active_thread_stop_cancels_bound_run_before_unloading_only_that_thread() {
        let (_root, runtime) = thread_test_runtime();
        let mut threads = Vec::new();
        for _ in 0..2 {
            let (spawn_id, _) = pending_spawn(
                start_thread(
                    &runtime,
                    None,
                    &runtime.paths.workspace_root,
                    crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
                )
                .unwrap(),
            );
            threads.push(grant_thread(&runtime, &spawn_id, "stdin"));
        }
        let target_id = threads[0].authority.thread_id.clone();
        let sibling_id = threads[1].authority.thread_id.clone();
        let sibling_before = runtime.thread_live_state(&sibling_id);
        let turn = admit_test_thread_turn(&runtime, &target_id, "controller", "turn_active");
        let record = Arc::new(RunRecord::new_for_thread(
            "run_thread_stop".into(),
            "session_thread_stop".into(),
            runtime.paths.ledger_path.clone(),
            turn.clone(),
        ));
        runtime.reserve_run(record.clone()).unwrap();
        runtime.bind_thread_run(&turn, record.clone()).unwrap();
        let worker_runtime = runtime.clone();
        let worker_record = record.clone();
        let worker = thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            while !worker_record.cancel.load(Ordering::SeqCst) {
                assert!(
                    std::time::Instant::now() < deadline,
                    "thread stop did not request child cancellation"
                );
                thread::yield_now();
            }
            worker_runtime.finish_run_with_error(&worker_record, &AppError::RunCanceled);
        });

        let stopped = stop_thread(&runtime, &target_id, "operator");
        worker.join().unwrap();
        assert_eq!(
            response_result::<ThreadStopResult>(&stopped),
            ThreadStopResult::Stopped {
                thread_id: target_id.clone(),
                stopped_turn_id: Some("turn_active".into()),
                stopped_at_ms: runtime
                    .paths
                    .server_store()
                    .unwrap()
                    .thread_stop(&target_id)
                    .unwrap()
                    .unwrap()
                    .occurred_at_ms,
            }
        );
        assert_eq!(record.status().state, RunStateName::Canceled);
        assert!(!runtime.thread_is_loaded(&target_id));
        assert_eq!(runtime.thread_live_state(&sibling_id), sibling_before);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn thread_stop_supervised_child_matrix_reaches_zero_residual() {
        for wedged in [false, true] {
            assert_thread_stop_supervised_child(wedged);
        }
    }

    #[cfg(target_os = "linux")]
    fn assert_thread_stop_supervised_child(wedged: bool) {
        use crate::{
            app::prepare_run,
            daemon::run_child::{
                SupervisedTestLaunch, TerminalStageBarriers, run_supervised_for_test,
            },
        };

        let (_root, runtime) = thread_test_runtime();
        let mut threads = Vec::new();
        for _ in 0..2 {
            let (spawn_id, _) = pending_spawn(
                start_thread(
                    &runtime,
                    None,
                    &runtime.paths.workspace_root,
                    crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
                )
                .unwrap(),
            );
            threads.push(grant_thread(&runtime, &spawn_id, "stdin"));
        }
        let target_id = threads[0].authority.thread_id.clone();
        let sibling_id = threads[1].authority.thread_id.clone();
        let case = if wedged { "wedged" } else { "active" };
        let run_id = RunId::new(format!("run_thread_stop_{case}")).unwrap();
        let turn =
            admit_test_thread_turn(&runtime, &target_id, "controller", &format!("turn_{case}"));
        let record = Arc::new(RunRecord::new_for_thread(
            run_id.to_string(),
            format!("session_thread_stop_{case}"),
            runtime.paths.ledger_path.clone(),
            turn.clone(),
        ));
        runtime.reserve_run(record.clone()).unwrap();
        runtime.bind_thread_run(&turn, record.clone()).unwrap();
        let sibling_turn = admit_test_thread_turn(
            &runtime,
            &sibling_id,
            "sibling_controller",
            &format!("turn_sibling_{case}"),
        );
        let sibling = Arc::new(RunRecord::new_for_thread(
            format!("run_sibling_{case}"),
            format!("session_sibling_{case}"),
            runtime.paths.ledger_path.clone(),
            sibling_turn.clone(),
        ));
        runtime.reserve_run(sibling.clone()).unwrap();
        runtime
            .bind_thread_run(&sibling_turn, sibling.clone())
            .unwrap();

        std::fs::write(
            runtime.paths.workspace_root.join("plato.toml"),
            r#"[provider]
kind = "open_ai"
model = "test-model"
api_key_env = "PATH"
base_url = "http://127.0.0.1:1"

[limits]
token_budget = 4000
max_output_tokens = 64
max_turns = 1

[tools]
enabled = ["file.read"]
"#,
        )
        .unwrap();
        let (prepared, recorder) = prepare_run(&RunOptions {
            question: "exercise thread.stop child ownership".into(),
            config_path: Some(runtime.paths.workspace_root.join("plato.toml")),
            overrides: Default::default(),
            ledger: RunLedger::DefaultSqlite(runtime.paths.default_ledger()),
            workspace_root: runtime.paths.workspace_root.clone(),
            approval_mode: ApprovalMode::Deny { actor: "test" },
            run_id: Some(run_id.clone()),
            session: Some(RunSession::Fresh {
                session_id: record.session_id.clone(),
            }),
            event_sender: None,
            stream_to_stderr: false,
            cancel: None,
            voice_interruption_context: None,
        })
        .unwrap();
        let run_started = json!({
            "kind": "record",
            "request_id": 1,
            "operation": {
                "operation": "event",
                "event": {"event": "run_started", "run_id": run_id, "agent_id": "plato"}
            }
        });
        let terminal = json!({
            "kind": "record",
            "request_id": 2,
            "operation": {
                "operation": "fail",
                "run_id": run_id,
                "error": crate::ledger::RUN_CANCELED_REASON,
                "canceled": true
            }
        });
        let canceled = json!({
            "kind": "result",
            "request_id": 3,
            "result": {"status": "canceled"}
        });
        let descendant_pid_path = runtime.paths.workspace_root.join(format!("{case}.pid"));
        let fixture = runtime.paths.workspace_root.join(format!("{case}-child"));
        let body = if wedged {
            format!(
                r#"trap '' TERM
IFS= read -r _
printf '{{"kind":"ready","request_id":0,"pid":%s}}\n' "$$"
IFS= read -r _
printf '%s\n' '{run_started}'
IFS= read -r _
/bin/sh -c 'trap "" TERM; while :; do :; done' &
printf '%s\n' "$!" > '{descendant_pid_path}.tmp'
mv '{descendant_pid_path}.tmp' '{descendant_pid_path}'
IFS= read -r _
while :; do :; done
"#,
                descendant_pid_path = descendant_pid_path.display(),
            )
        } else {
            format!(
                r#"IFS= read -r _
printf '{{"kind":"ready","request_id":0,"pid":%s}}\n' "$$"
IFS= read -r _
printf '%s\n' '{run_started}'
IFS= read -r _
IFS= read -r _
printf '%s\n' '{terminal}'
IFS= read -r _
printf '%s\n' '{canceled}'
IFS= read -r _
"#,
            )
        };
        std::fs::write(&fixture, format!("#!/bin/sh\n{body}")).unwrap();
        std::fs::set_permissions(&fixture, std::fs::Permissions::from_mode(0o700)).unwrap();

        let (event_sender, event_receiver) = mpsc::channel();
        let event_collector = spawn_event_collector(record.clone(), event_receiver);
        let (ready_sender, ready_receiver) = mpsc::channel();
        let terminal_reached = Arc::new(Barrier::new(2));
        let terminal_release = Arc::new(Barrier::new(2));
        let terminal_driver = (!wedged).then(|| {
            let reached = terminal_reached.clone();
            let release = terminal_release.clone();
            thread::spawn(move || {
                reached.wait();
                release.wait();
            })
        });
        let worker_runtime = runtime.clone();
        let worker_record = record.clone();
        let worker_cancel = record.cancel.clone();
        let (outcome_sender, outcome_receiver) = mpsc::channel();
        let worker = thread::spawn(move || {
            let completion = run_supervised_for_test(
                prepared,
                recorder,
                ApprovalMode::Deny { actor: "test" },
                event_sender,
                worker_cancel,
                SupervisedTestLaunch {
                    executable: fixture,
                    ready_child: ready_sender,
                    terminal_stage_barriers: TerminalStageBarriers {
                        reached: terminal_reached,
                        release: terminal_release,
                    },
                },
            );
            let outcome = finish_run_after_event_collection(
                &worker_runtime,
                &worker_record,
                completion,
                event_collector,
            );
            outcome_sender.send(outcome).unwrap();
        });
        let child_pid = ready_receiver.recv_timeout(Duration::from_secs(2)).unwrap();
        let descendant_pid = wedged.then(|| {
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            while !descendant_pid_path.exists() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "wedged descendant pid was not published"
                );
                thread::yield_now();
            }
            std::fs::read_to_string(&descendant_pid_path)
                .unwrap()
                .trim()
                .parse::<u32>()
                .unwrap()
        });

        let stopped = stop_thread(&runtime, &target_id, "operator");
        assert!(matches!(
            response_result::<ThreadStopResult>(&stopped),
            ThreadStopResult::Stopped {
                stopped_turn_id: Some(ref turn_id),
                ..
            } if turn_id == &format!("turn_{case}")
        ));
        assert!(matches!(
            outcome_receiver
                .recv_timeout(Duration::from_secs(5))
                .unwrap(),
            Err(AppError::RunCanceled)
        ));
        worker.join().unwrap();
        if let Some(driver) = terminal_driver {
            driver.join().unwrap();
        }
        assert!(!Path::new(&format!("/proc/{child_pid}")).exists());
        if let Some(descendant_pid) = descendant_pid {
            assert!(!Path::new(&format!("/proc/{descendant_pid}")).exists());
        }
        assert!(!runtime.thread_is_loaded(&target_id));
        assert_eq!(
            runtime
                .thread_live_state(&sibling_id)
                .current_turn_id
                .as_deref(),
            Some(format!("turn_sibling_{case}").as_str())
        );
        assert!(!sibling.cancel.load(Ordering::SeqCst));
        assert_eq!(sibling.status().state, RunStateName::Running);
        runtime.finish_run_with_error(&sibling, &AppError::RunCanceled);
    }

    #[test]
    fn thread_activity_is_live_monotone_and_absent_after_restart() {
        let (_root, runtime) = thread_test_runtime();
        let (spawn_id, _) = pending_spawn(
            start_thread(
                &runtime,
                None,
                &runtime.paths.workspace_root,
                crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
            )
            .unwrap(),
        );
        let thread = grant_thread(&runtime, &spawn_id, "stdin");
        let thread_id = thread.authority.thread_id.clone();
        let later = thread.authority.created_at_ms.saturating_add(1_000);
        runtime.note_thread_activity_at(&thread_id, later);
        runtime.note_thread_activity_at(&thread_id, later.saturating_sub(500));
        assert_eq!(
            runtime.thread_live_state(&thread_id).last_activity_at_ms,
            Some(later)
        );

        let restarted = DaemonRuntime::new(runtime.paths.clone());
        assert_eq!(
            restarted.thread_live_state(&thread_id),
            crate::daemon::protocol::ThreadLiveState {
                loaded: false,
                current_turn_id: None,
                last_activity_at_ms: None,
            }
        );
        let columns = rusqlite::Connection::open(&runtime.paths.server_db_path)
            .unwrap()
            .prepare("PRAGMA table_info(thread_authorities)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(columns.len(), 13);
        assert!(!columns.iter().any(|column| column.contains("activity")));
    }

    #[test]
    fn thread_spawn_persistence_failure_releases_claim_without_live_thread() {
        let (_root, runtime) = thread_test_runtime();
        let (spawn_id, thread_id) = pending_spawn(
            start_thread(
                &runtime,
                None,
                &runtime.paths.workspace_root,
                crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
            )
            .unwrap(),
        );
        // The authority table lives in the server-wide store now, so the
        // injected failure has to be planted there to exercise the same path.
        drop(runtime.paths.server_store().unwrap());
        let connection = rusqlite::Connection::open(&runtime.paths.server_db_path).unwrap();
        connection
            .execute_batch(
                "CREATE TRIGGER fail_thread_authority_insert
                 BEFORE INSERT ON thread_authorities
                 BEGIN SELECT RAISE(ABORT, 'injected authority failure'); END;",
            )
            .unwrap();
        drop(connection);

        assert!(matches!(
            decide_thread(
                &runtime,
                &spawn_id,
                ThreadSpawnDecision::Grant {
                    actor: "stdin".into()
                }
            ),
            Err(ThreadSpawnFailure::Persistence)
        ));
        let store = runtime.paths.server_store().unwrap();
        assert!(store.thread_authority(&thread_id).unwrap().is_none());
        assert!(store.thread_spawn_approval(&spawn_id).unwrap().is_none());
        assert!(!runtime.thread_is_loaded(&thread_id));
        drop(store);

        let connection = rusqlite::Connection::open(&runtime.paths.server_db_path).unwrap();
        connection
            .execute_batch("DROP TRIGGER fail_thread_authority_insert")
            .unwrap();
        drop(connection);
        assert_eq!(
            grant_thread(&runtime, &spawn_id, "stdin")
                .authority
                .thread_id,
            thread_id
        );
    }

    #[test]
    fn spawned_thread_never_exceeds_parent_policy_or_cwd_authority() {
        let (root, runtime) = thread_test_runtime();
        let child_dir = runtime.paths.workspace_root.join("child");
        std::fs::create_dir(&child_dir).unwrap();
        let outside_dir = root.path().join("outside");
        std::fs::create_dir(&outside_dir).unwrap();
        let (spawn_id, _) = pending_spawn(
            start_thread(
                &runtime,
                None,
                &runtime.paths.workspace_root,
                crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
            )
            .unwrap(),
        );
        let parent = grant_thread(&runtime, &spawn_id, "stdin");

        assert!(matches!(
            start_thread(
                &runtime,
                Some(parent.authority.thread_id.clone()),
                &child_dir,
                crate::daemon::protocol::ThreadApprovalPolicy::Yolo,
            ),
            Err(ThreadSpawnFailure::Authority(
                ThreadAuthorityError::ApprovalPolicy { .. }
            ))
        ));
        assert!(matches!(
            start_thread(
                &runtime,
                Some(parent.authority.thread_id),
                &outside_dir,
                crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
            ),
            Err(ThreadSpawnFailure::Authority(
                ThreadAuthorityError::WorkingDirectory { .. }
            ))
        ));
        let store = runtime.paths.server_store().unwrap();
        assert_eq!(store.thread_authorities().unwrap().len(), 1);
    }

    #[test]
    fn spawned_thread_rejects_a_toolset_superset_at_the_typed_gate() {
        let (_root, runtime) = thread_test_runtime();
        let child_dir = runtime.paths.workspace_root.join("child");
        std::fs::create_dir(&child_dir).unwrap();
        std::fs::write(
            runtime.paths.workspace_root.join("plato.toml"),
            "[tools]\nenabled = [\"file.read\"]\n",
        )
        .unwrap();
        std::fs::write(
            child_dir.join("plato.toml"),
            "[tools]\nenabled = [\"file.read\", \"file.write\"]\n",
        )
        .unwrap();
        let (spawn_id, _) = pending_spawn(
            start_thread(
                &runtime,
                None,
                &runtime.paths.workspace_root,
                crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
            )
            .unwrap(),
        );
        let parent = grant_thread(&runtime, &spawn_id, "stdin");

        assert!(matches!(
            start_thread(
                &runtime,
                Some(parent.authority.thread_id),
                &child_dir,
                crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
            ),
            Err(ThreadSpawnFailure::Authority(
                ThreadAuthorityError::Toolset { .. }
            ))
        ));
        assert_eq!(
            runtime
                .paths
                .server_store()
                .unwrap()
                .thread_authorities()
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn yolo_parent_auto_grants_child_with_exact_actor() {
        let (_root, runtime) = thread_test_runtime();
        let child_dir = runtime.paths.workspace_root.join("child");
        std::fs::create_dir(&child_dir).unwrap();
        let (spawn_id, _) = pending_spawn(
            start_thread(
                &runtime,
                None,
                &runtime.paths.workspace_root,
                crate::daemon::protocol::ThreadApprovalPolicy::Yolo,
            )
            .unwrap(),
        );
        let parent = grant_thread(&runtime, &spawn_id, "stdin");
        let child = match start_thread(
            &runtime,
            Some(parent.authority.thread_id),
            &child_dir,
            crate::daemon::protocol::ThreadApprovalPolicy::Yolo,
        )
        .unwrap()
        {
            ThreadSpawnResult::Spawned { thread } => thread,
            unexpected => panic!("expected auto-granted child, got {unexpected:?}"),
        };
        assert_eq!(child.authority.spawning_actor, "yolo");
        let connection = rusqlite::Connection::open(&runtime.paths.server_db_path).unwrap();
        let approvals = connection
            .query_row(
                "SELECT COUNT(*) FROM thread_spawn_approvals WHERE actor = 'yolo' AND decision = 'granted'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap();
        assert_eq!(approvals, 1);
    }

    #[test]
    fn thread_list_and_status_keep_clientless_orphans_after_restart() {
        let (_root, runtime) = thread_test_runtime();
        let child_dir = runtime.paths.workspace_root.join("child");
        std::fs::create_dir(&child_dir).unwrap();
        let (spawn_id, _) = pending_spawn(
            start_thread(
                &runtime,
                None,
                &runtime.paths.workspace_root,
                crate::daemon::protocol::ThreadApprovalPolicy::Yolo,
            )
            .unwrap(),
        );
        let parent = grant_thread(&runtime, &spawn_id, "stdin");
        let child = match start_thread(
            &runtime,
            Some(parent.authority.thread_id.clone()),
            &child_dir,
            crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
        )
        .unwrap()
        {
            ThreadSpawnResult::Spawned { thread } => thread,
            unexpected => panic!("expected auto-granted child, got {unexpected:?}"),
        };

        let restarted = DaemonRuntime::new(runtime.paths.clone());
        assert!(matches!(
            start_thread(
                &restarted,
                Some(parent.authority.thread_id.clone()),
                &child_dir,
                crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
            ),
            Err(ThreadSpawnFailure::NotFound(message)) if message.contains("not loaded")
        ));
        let list = handle_line(
            &restarted,
            r#"{"v":1,"id":"list","kind":"request","method":"thread.list"}"#,
        );
        let listed: ThreadListResult = response_result(&list);
        assert_eq!(listed.threads.len(), 2);
        assert!(listed.threads.iter().all(|thread| !thread.live.loaded));
        assert!(listed.threads.iter().any(|thread| {
            thread.authority.thread_id == child.authority.thread_id
                && thread.authority.parent_thread_id.as_deref()
                    == Some(parent.authority.thread_id.as_str())
        }));

        let status = handle_line(
            &restarted,
            &format!(
                r#"{{"v":1,"id":"status","kind":"request","method":"thread.status","params":{{"thread_id":"{}"}}}}"#,
                child.authority.thread_id
            ),
        );
        let status: ThreadStatusResult = response_result(&status);
        assert_eq!(status.thread.authority, child.authority);
        assert!(!status.thread.live.loaded);
    }

    #[test]
    fn duplicate_thread_decision_is_idempotent_and_conflicts_fail_closed() {
        let (_root, runtime) = thread_test_runtime();
        let (spawn_id, thread_id) = pending_spawn(
            start_thread(
                &runtime,
                None,
                &runtime.paths.workspace_root,
                crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
            )
            .unwrap(),
        );
        let first = grant_thread(&runtime, &spawn_id, "stdin");
        let duplicate = grant_thread(&runtime, &spawn_id, "stdin");
        assert_eq!(duplicate, first);
        assert!(matches!(
            decide_thread(
                &runtime,
                &spawn_id,
                ThreadSpawnDecision::Deny {
                    actor: "stdin".into(),
                    reason: "changed".into(),
                }
            ),
            Err(ThreadSpawnFailure::Conflict(message)) if message.contains("different durable decision")
        ));
        let store = runtime.paths.server_store().unwrap();
        assert_eq!(store.thread_authorities().unwrap().len(), 1);
        assert_eq!(
            legacy_status_authority(
                &store
                    .thread_authority(&thread_id)
                    .unwrap()
                    .expect("granted authority is durable")
            )
            .unwrap(),
            first.authority
        );
    }

    #[test]
    fn malformed_thread_requests_fail_before_reservation() {
        let (_root, runtime) = thread_test_runtime();
        assert!(matches!(
            start_thread(
                &runtime,
                None,
                Path::new("relative"),
                crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
            ),
            Err(ThreadSpawnFailure::Malformed(message)) if message.contains("absolute")
        ));
        let response = handle_line(
            &runtime,
            r#"{"v":1,"id":"bad","kind":"request","method":"thread.spawn","params":{"action":"start","parent_thread_id":null,"cwd":"/tmp","model":"gpt-5.6-sol","reasoning_effort":"xhigh","approval_policy":"prompt","extra":true}}"#,
        );
        assert_eq!(response.error.unwrap().code, ERROR_MALFORMED_REQUEST);
        let store = runtime.paths.server_store().unwrap();
        assert!(store.thread_authorities().unwrap().is_empty());
    }

    #[test]
    fn thread_authority_params_reject_unknown_fields_before_readback() {
        let (_root, runtime) = thread_test_runtime();
        let response = handle_line(
            &runtime,
            r#"{"v":1,"id":"bad","kind":"request","method":"thread.authority","params":{"thread_id":"thread_1","future":true}}"#,
        );
        assert_eq!(response.error.unwrap().code, ERROR_MALFORMED_REQUEST);
    }

    #[test]
    fn denied_and_stale_thread_sends_leave_authority_ledger_and_turn_unchanged() {
        let (_root, runtime) = thread_test_runtime();
        let (spawn_id, thread_id) = pending_spawn(
            start_thread(
                &runtime,
                None,
                &runtime.paths.workspace_root,
                crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
            )
            .unwrap(),
        );
        let authority = grant_thread(&runtime, &spawn_id, "stdin").authority;
        let malformed = handle_line(
            &runtime,
            &format!(
                r#"{{"v":1,"id":"bad","kind":"request","method":"thread.send","params":{{"thread_id":"{thread_id}","controller_id":"","message":"no"}}}}"#
            ),
        );
        assert_eq!(malformed.error.unwrap().code, ERROR_MALFORMED_REQUEST);

        let stale = handle_line(
            &runtime,
            &format!(
                r#"{{"v":1,"id":"stale","kind":"request","method":"thread.send","params":{{"thread_id":"{thread_id}","controller_id":"controller_a","turn_id":"thread_turn_stale","message":"no"}}}}"#
            ),
        );
        assert_eq!(
            response_result::<crate::daemon::protocol::ThreadSendResult>(&stale),
            crate::daemon::protocol::ThreadSendResult::Rejected {
                thread_id: thread_id.clone(),
                turn_id: None,
                reason: crate::daemon::protocol::ThreadSendRejectedReason::TurnMismatch,
            }
        );

        let invalid_events = handle_line(
            &runtime,
            &format!(
                r#"{{"v":1,"id":"events","kind":"request","method":"thread.events","params":{{"thread_id":"{thread_id}","limit":0}}}}"#
            ),
        );
        assert_eq!(invalid_events.error.unwrap().code, ERROR_MALFORMED_REQUEST);
        assert_eq!(runtime.thread_live_state(&thread_id).current_turn_id, None);
        let store = runtime.paths.server_store().unwrap();
        assert_eq!(
            legacy_status_authority(
                &store
                    .thread_authority(&thread_id)
                    .unwrap()
                    .expect("granted authority is durable")
            )
            .unwrap(),
            authority
        );
        // The workspace ledger holds session_runs; the server store holds
        // thread state. This assertion is about the former.
        let connection = rusqlite::Connection::open(
            SqliteLedger::open_or_create_default(&runtime.paths.default_ledger())
                .map(|_| &runtime.paths.ledger_path)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM session_runs", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0
        );
    }

    #[test]
    fn terminal_status_waits_for_collected_events_on_success_failure_and_cancellation() {
        assert_terminal_waits_for_collector(
            "success",
            Ok(RunOutcome {
                run_id: RunId::new("run_success").unwrap(),
                final_answer: "done".into(),
                completion_claim: None,
            }),
            RunStateName::Finished,
        );
        assert_terminal_waits_for_collector(
            "failure",
            Err(AppError::RunFailed("provider failed".into())),
            RunStateName::Failed,
        );
        assert_terminal_waits_for_collector(
            "cancellation",
            Err(AppError::RunCanceled),
            RunStateName::Canceled,
        );
    }

    #[test]
    fn collector_handle_drains_in_order_without_changing_retention() {
        let record = test_run_record("retention");
        let (event_sender, event_receiver) = mpsc::channel();
        let event_collector = spawn_event_collector(record.clone(), event_receiver);
        let sent = crate::daemon::runtime::MAX_EVENT_BUFFER as u64 + 2;
        for delta_index in 0..sent {
            event_sender
                .send(test_delta("retention", delta_index))
                .unwrap();
        }
        drop(event_sender);

        event_collector.join().unwrap();

        let buffer = record.events.lock().unwrap();
        assert_eq!(buffer.first_offset, 2);
        assert_eq!(buffer.next_offset, sent);
        assert_eq!(
            buffer
                .events
                .iter()
                .map(|event| event.offset)
                .collect::<Vec<_>>(),
            (2..sent).collect::<Vec<_>>()
        );
    }

    #[test]
    fn collector_panic_becomes_typed_run_failure() {
        let runtime = test_runtime();
        let record = test_run_record("collector_panic");
        runtime.reserve_run(record.clone()).unwrap();
        let event_collector = thread::spawn(|| panic!("injected collector panic"));

        let error = finish_run_after_event_collection(
            &runtime,
            &record,
            Ok(RunOutcome {
                run_id: RunId::new("run_collector_panic").unwrap(),
                final_answer: "must not publish".into(),
                completion_claim: None,
            }),
            event_collector,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            AppError::RunFailed(ref reason) if reason == EVENT_COLLECTOR_PANIC
        ));
        assert_eq!(
            record.status(),
            crate::daemon::runtime::RunStatus {
                state: RunStateName::Failed,
                final_answer: None,
                error: Some(format!("run did not finish: {EVENT_COLLECTOR_PANIC}")),
                completion_claim: None,
            }
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn supervised_terminal_intent_stays_nonterminal_until_cleanup_completes() {
        assert_supervised_terminal_publication(false);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn supervised_cleanup_failure_replaces_staged_success_before_publication() {
        assert_supervised_terminal_publication(true);
    }

    #[test]
    fn event_snapshot_before_collector_cannot_publish_a_stale_terminal_page() {
        let runtime = test_runtime();
        let record = test_run_record("snapshot_before_collector");
        runtime.reserve_run(record.clone()).unwrap();
        let snapshot_reached = Arc::new(Barrier::new(2));
        let snapshot_release = Arc::new(Barrier::new(2));
        record.set_event_snapshot_barriers(snapshot_reached.clone(), snapshot_release.clone());

        let stream_runtime = runtime.clone();
        let stream_record = record.clone();
        let (stream_sender, stream_receiver) = mpsc::channel();
        let stream = thread::spawn(move || {
            stream_sender
                .send(stream_run(
                    &stream_runtime,
                    "stream_before_collector",
                    &stream_record.run_id,
                ))
                .unwrap();
        });
        snapshot_reached.wait();

        let (event_sender, event_receiver) = mpsc::channel();
        event_sender
            .send(test_delta("snapshot_before_collector", 0))
            .unwrap();
        drop(event_sender);
        let event_collector = spawn_event_collector(record.clone(), event_receiver);
        let finisher_runtime = runtime.clone();
        let finisher_record = record.clone();
        let finish_started = Arc::new(Barrier::new(2));
        let started = finish_started.clone();
        let (finished_sender, finished_receiver) = mpsc::channel();
        let finisher = thread::spawn(move || {
            started.wait();
            event_collector.join().unwrap();
            finisher_runtime.finish_run(&finisher_record, "done".into(), None);
            finished_sender.send(()).unwrap();
        });
        finish_started.wait();
        assert!(matches!(
            finished_receiver.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));

        snapshot_release.wait();
        let before_collector = stream_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        stream.join().unwrap();
        assert_eq!(before_collector.status, RunStateName::Running);
        assert_eq!(before_collector.from_offset, 0);
        assert_eq!(before_collector.next_offset, 0);
        assert!(before_collector.events.is_empty());

        finished_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        finisher.join().unwrap();
        let terminal = stream_run(&runtime, "stream_terminal", &record.run_id);
        assert_eq!(terminal.status, RunStateName::Finished);
        assert_eq!(terminal.from_offset, 0);
        assert_eq!(terminal.next_offset, 1);
        assert!(matches!(
            terminal.events.as_slice(),
            [crate::daemon::protocol::BufferedStreamEvent {
                offset: 0,
                event: StreamEvent::AssistantDelta { delta_index: 0, .. },
            }]
        ));
    }

    #[test]
    fn run_cancel_stores_one_idempotent_transition_and_immediate_readback() {
        let runtime = test_runtime();
        let record = test_run_record("cancel_idempotent");
        runtime.reserve_run(record.clone()).unwrap();

        let first = cancel_run(&runtime, "cancel_1", &record.run_id);
        let first_result: CommandAcceptedResult = response_result(&first);

        assert_eq!(first.kind, crate::daemon::protocol::EnvelopeKind::Response);
        assert_eq!(first_result.run_id, record.run_id);
        assert_eq!(first_result.status, RunStateName::CancelRequested);
        assert_eq!(record.status().state, RunStateName::CancelRequested);
        assert!(record.cancel.load(Ordering::SeqCst));

        let readback = stream_run(&runtime, "stream_1", &record.run_id);
        assert_eq!(readback.status, RunStateName::CancelRequested);
        assert_eq!(readback.from_offset, 0);
        assert_eq!(readback.next_offset, 1);
        assert!(matches!(
            readback.events.as_slice(),
            [crate::daemon::protocol::BufferedStreamEvent {
                offset: 0,
                event: StreamEvent::Canceled { run_id },
            }] if run_id == &record.run_id
        ));

        let duplicate = cancel_run(&runtime, "cancel_2", &record.run_id);
        let duplicate_result: CommandAcceptedResult = response_result(&duplicate);
        assert_eq!(
            duplicate.kind,
            crate::daemon::protocol::EnvelopeKind::Response
        );
        assert_eq!(duplicate_result, first_result);

        let duplicate_readback = stream_run(&runtime, "stream_2", &record.run_id);
        assert_eq!(duplicate_readback.status, RunStateName::CancelRequested);
        assert_eq!(duplicate_readback.next_offset, 1);
        assert_eq!(duplicate_readback.events, readback.events);
    }

    #[test]
    fn run_cancel_rejects_every_terminal_status_without_side_effects() {
        let runtime = test_runtime();

        for terminal in [
            RunStateName::Finished,
            RunStateName::Failed,
            RunStateName::Canceled,
        ] {
            let record = test_run_record(terminal.as_str());
            record.status.lock().unwrap().state = terminal;
            runtime.reserve_run(record.clone()).unwrap();

            let response = cancel_run(&runtime, "cancel_terminal", &record.run_id);

            assert_eq!(response.kind, crate::daemon::protocol::EnvelopeKind::Error);
            let error = response.error.unwrap();
            assert_eq!(error.code, ERROR_NOT_FOUND);
            assert_eq!(
                error.message,
                format!("run is not active: {}", record.run_id)
            );
            assert_eq!(record.status().state, terminal);
            assert!(!record.cancel.load(Ordering::SeqCst));
            assert!(record.events.lock().unwrap().events.is_empty());
        }
    }

    #[test]
    fn grant_session_is_exact_idempotent_and_fail_closed_before_installation() {
        for (case, tool_name, effect) in [
            ("file_write", "file.write", EffectClass::WorkspaceWrite),
            ("shell_network", SHELL_EXEC, EffectClass::Network),
            ("shell_secret", SHELL_EXEC, EffectClass::SecretAccess),
            ("shell_write", SHELL_EXEC, EffectClass::WorkspaceWrite),
            (
                "computer",
                "computer.click",
                EffectClass::ExternalSideEffect,
            ),
            ("unknown", "unknown.tool", EffectClass::ExternalSideEffect),
        ] {
            let runtime = test_runtime();
            let record = test_run_record(case);
            record.approvals.lock().unwrap().insert(
                "call_1".into(),
                PendingApproval::new(
                    record.session_id.clone(),
                    test_approval_request(&record.run_id, "call_1", tool_name, effect),
                ),
            );
            runtime.reserve_run(record.clone()).unwrap();

            let response = decide_session_grant(&runtime, case, &record.run_id, "call_1");

            assert_eq!(response.kind, crate::daemon::protocol::EnvelopeKind::Error);
            assert_eq!(response.error.unwrap().code, ERROR_NOT_FOUND);
            assert_eq!(runtime.session_tool_grant_count(), 0);
            assert_eq!(record.approvals.lock().unwrap()["call_1"].decision, None);
        }

        let runtime = test_runtime();
        let record = test_run_record("exact");
        record.approvals.lock().unwrap().insert(
            "call_1".into(),
            PendingApproval::new(
                record.session_id.clone(),
                test_approval_request(
                    &record.run_id,
                    "call_1",
                    SHELL_EXEC,
                    EffectClass::ExternalSideEffect,
                ),
            ),
        );
        runtime.reserve_run(record.clone()).unwrap();

        record
            .approvals
            .lock()
            .unwrap()
            .get_mut("call_1")
            .unwrap()
            .session_id = "session_other".into();
        let mismatched_session =
            decide_session_grant(&runtime, "wrong_session", &record.run_id, "call_1");
        assert_eq!(
            mismatched_session.kind,
            crate::daemon::protocol::EnvelopeKind::Error
        );
        assert_eq!(runtime.session_tool_grant_count(), 0);
        assert_eq!(record.approvals.lock().unwrap()["call_1"].decision, None);
        record
            .approvals
            .lock()
            .unwrap()
            .get_mut("call_1")
            .unwrap()
            .session_id = record.session_id.clone();

        let mismatched_run = decide_session_grant(&runtime, "wrong_run", "run_missing", "call_1");
        let mismatched_call =
            decide_session_grant(&runtime, "wrong_call", &record.run_id, "call_missing");
        assert_eq!(
            mismatched_run.kind,
            crate::daemon::protocol::EnvelopeKind::Error
        );
        assert_eq!(
            mismatched_call.kind,
            crate::daemon::protocol::EnvelopeKind::Error
        );
        assert_eq!(runtime.session_tool_grant_count(), 0);
        assert_eq!(record.approvals.lock().unwrap()["call_1"].decision, None);

        let first = decide_session_grant(&runtime, "first", &record.run_id, "call_1");
        let duplicate = decide_session_grant(&runtime, "duplicate", &record.run_id, "call_1");
        assert_eq!(first.kind, crate::daemon::protocol::EnvelopeKind::Response);
        assert_eq!(
            duplicate.kind,
            crate::daemon::protocol::EnvelopeKind::Response
        );
        assert_eq!(runtime.session_tool_grant_count(), 1);
        assert!(runtime.has_shell_session_grant(&record.session_id));
        assert!(!runtime.has_shell_session_grant("session_other"));
        assert_eq!(
            record.approvals.lock().unwrap()["call_1"].decision,
            Some(PendingApprovalDecision {
                decision: ApprovalDecision::GrantSession,
                outcome: ExternalApprovalOutcome::Granted {
                    actor: "tui_session_grant"
                },
            })
        );

        let conflicting = handle_line(
            &runtime,
            &format!(
                r#"{{"v":1,"id":"conflicting","kind":"request","method":"approval.decide","params":{{"run_id":"{}","tool_call_id":"call_1","decision":"deny"}}}}"#,
                record.run_id
            ),
        );
        assert_eq!(
            conflicting.kind,
            crate::daemon::protocol::EnvelopeKind::Error
        );
        assert_eq!(runtime.session_tool_grant_count(), 1);
        assert_eq!(
            record.approvals.lock().unwrap()["call_1"]
                .decision
                .as_ref()
                .map(|decision| decision.decision),
            Some(ApprovalDecision::GrantSession)
        );
    }

    #[test]
    fn canceled_and_terminal_session_grants_reject_before_mutation() {
        for (case, state, canceled) in [
            ("cancel_requested", RunStateName::CancelRequested, true),
            ("finished", RunStateName::Finished, false),
            ("failed", RunStateName::Failed, false),
            ("canceled", RunStateName::Canceled, true),
            ("interrupted", RunStateName::Interrupted, false),
        ] {
            let runtime = test_runtime();
            let record = test_run_record(case);
            record.approvals.lock().unwrap().insert(
                "call_1".into(),
                PendingApproval::new(
                    record.session_id.clone(),
                    test_approval_request(
                        &record.run_id,
                        "call_1",
                        SHELL_EXEC,
                        EffectClass::ExternalSideEffect,
                    ),
                ),
            );
            record.status.lock().unwrap().state = state;
            record.cancel.store(canceled, Ordering::SeqCst);
            runtime
                .state
                .lock()
                .unwrap()
                .runs
                .insert(record.run_id.clone(), record.clone());

            let response = decide_session_grant(&runtime, case, &record.run_id, "call_1");

            assert_eq!(response.kind, crate::daemon::protocol::EnvelopeKind::Error);
            assert_eq!(response.error.unwrap().code, ERROR_NOT_FOUND);
            assert_eq!(runtime.session_tool_grant_count(), 0);
            assert_eq!(record.approvals.lock().unwrap()["call_1"].decision, None);
        }
    }

    #[test]
    fn session_grant_decision_linearizes_before_concurrent_cancel_without_lost_wakeup() {
        let runtime = test_runtime();
        let record = test_run_record("grant_cancel");
        runtime.reserve_run(record.clone()).unwrap();
        let waiter = spawn_shell_approval_waiter(&runtime, &record);
        wait_for_pending_approval(&record);

        let install_reached = Arc::new(Barrier::new(2));
        let install_release = Arc::new(Barrier::new(2));
        runtime
            .set_session_grant_install_barriers(install_reached.clone(), install_release.clone());
        let decision_runtime = runtime.clone();
        let decision_run_id = record.run_id.clone();
        let (decision_sender, decision_receiver) = mpsc::channel();
        let decision = thread::spawn(move || {
            decision_sender
                .send(decide_session_grant(
                    &decision_runtime,
                    "grant_cancel",
                    &decision_run_id,
                    "call_1",
                ))
                .unwrap();
        });
        install_reached.wait();

        let cancel_runtime = runtime.clone();
        let cancel_run_id = record.run_id.clone();
        let (cancel_sender, cancel_receiver) = mpsc::channel();
        let cancel = thread::spawn(move || {
            cancel_sender
                .send(cancel_run(&cancel_runtime, "cancel_race", &cancel_run_id))
                .unwrap();
        });
        assert!(matches!(
            cancel_receiver.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        install_release.wait();

        let decision_response = decision_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        let cancel_response = cancel_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        decision.join().unwrap();
        cancel.join().unwrap();
        assert_eq!(
            decision_response.kind,
            crate::daemon::protocol::EnvelopeKind::Response
        );
        assert_eq!(
            cancel_response.kind,
            crate::daemon::protocol::EnvelopeKind::Response
        );
        assert_eq!(
            waiter.join().unwrap(),
            ExternalApprovalOutcome::Granted {
                actor: "tui_session_grant"
            }
        );
        assert_eq!(runtime.session_tool_grant_count(), 1);
        assert_eq!(record.status().state, RunStateName::CancelRequested);
        assert_eq!(
            record.approvals.lock().unwrap()["call_1"]
                .decision
                .as_ref()
                .map(|decision| decision.decision),
            Some(ApprovalDecision::GrantSession)
        );
    }

    #[test]
    fn session_grant_decision_linearizes_before_concurrent_terminal_without_lost_wakeup() {
        let runtime = test_runtime();
        let record = test_run_record("grant_terminal");
        runtime.reserve_run(record.clone()).unwrap();
        let waiter = spawn_shell_approval_waiter(&runtime, &record);
        wait_for_pending_approval(&record);

        let install_reached = Arc::new(Barrier::new(2));
        let install_release = Arc::new(Barrier::new(2));
        runtime
            .set_session_grant_install_barriers(install_reached.clone(), install_release.clone());
        let decision_runtime = runtime.clone();
        let decision_run_id = record.run_id.clone();
        let (decision_sender, decision_receiver) = mpsc::channel();
        let decision = thread::spawn(move || {
            decision_sender
                .send(decide_session_grant(
                    &decision_runtime,
                    "grant_terminal",
                    &decision_run_id,
                    "call_1",
                ))
                .unwrap();
        });
        install_reached.wait();

        let finish_runtime = runtime.clone();
        let finish_record = record.clone();
        let (finish_sender, finish_receiver) = mpsc::channel();
        let finisher = thread::spawn(move || {
            finish_runtime.finish_run(&finish_record, "done".into(), None);
            finish_sender.send(()).unwrap();
        });
        assert!(matches!(
            finish_receiver.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        install_release.wait();

        let decision_response = decision_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        finish_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        decision.join().unwrap();
        finisher.join().unwrap();
        assert_eq!(
            decision_response.kind,
            crate::daemon::protocol::EnvelopeKind::Response
        );
        assert_eq!(
            waiter.join().unwrap(),
            ExternalApprovalOutcome::Granted {
                actor: "tui_session_grant"
            }
        );
        assert_eq!(runtime.session_tool_grant_count(), 1);
        assert_eq!(record.status().state, RunStateName::Finished);
        assert_eq!(
            record.approvals.lock().unwrap()["call_1"]
                .decision
                .as_ref()
                .map(|decision| decision.decision),
            Some(ApprovalDecision::GrantSession)
        );
    }

    #[test]
    fn run_cancel_and_finish_linearize_in_both_barrier_schedules() {
        let cancel_first_runtime = test_runtime();
        let cancel_first_record = test_run_record("cancel_first");
        cancel_first_runtime
            .reserve_run(cancel_first_record.clone())
            .unwrap();
        let collector_waiting = Arc::new(Barrier::new(2));
        let collector_release = Arc::new(Barrier::new(2));
        let waiting = collector_waiting.clone();
        let release = collector_release.clone();
        let event_collector = thread::spawn(move || {
            waiting.wait();
            release.wait();
        });
        collector_waiting.wait();
        let finish_started = Arc::new(Barrier::new(2));
        let started = finish_started.clone();
        let finisher_runtime = cancel_first_runtime.clone();
        let finisher_record = cancel_first_record.clone();
        let (finished_sender, finished_receiver) = mpsc::channel();
        let finisher = thread::spawn(move || {
            started.wait();
            let result = finish_run_after_event_collection(
                &finisher_runtime,
                &finisher_record,
                Ok(RunOutcome {
                    run_id: RunId::new("run_cancel_first").unwrap(),
                    final_answer: "done".into(),
                    completion_claim: None,
                }),
                event_collector,
            );
            finished_sender.send(result).unwrap();
        });
        finish_started.wait();
        assert!(matches!(
            finished_receiver.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));

        let cancel = cancel_run(
            &cancel_first_runtime,
            "cancel_first",
            &cancel_first_record.run_id,
        );
        assert_eq!(cancel.kind, crate::daemon::protocol::EnvelopeKind::Response);
        assert_eq!(
            cancel_first_record.status().state,
            RunStateName::CancelRequested
        );
        collector_release.wait();
        finished_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();
        finisher.join().unwrap();

        let status = cancel_first_record.status();
        assert_eq!(status.state, RunStateName::Finished);
        assert_eq!(status.final_answer.as_deref(), Some("done"));
        assert_eq!(status.error, None);
        assert!(cancel_first_record.cancel.load(Ordering::SeqCst));
        assert_eq!(cancel_first_record.events.lock().unwrap().events.len(), 1);

        let finish_first_runtime = test_runtime();
        let finish_first_record = test_run_record("finish_first");
        finish_first_runtime
            .reserve_run(finish_first_record.clone())
            .unwrap();
        let finish_started = Arc::new(Barrier::new(2));
        let started = finish_started.clone();
        let finisher_runtime = finish_first_runtime.clone();
        let finisher_record = finish_first_record.clone();
        let (finished_sender, finished_receiver) = mpsc::channel();
        let finisher = thread::spawn(move || {
            started.wait();
            finisher_runtime.finish_run(&finisher_record, "done".into(), None);
            finished_sender.send(()).unwrap();
        });
        finish_started.wait();
        finished_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap();

        let cancel = cancel_run(
            &finish_first_runtime,
            "finish_first",
            &finish_first_record.run_id,
        );
        finisher.join().unwrap();
        assert_eq!(cancel.kind, crate::daemon::protocol::EnvelopeKind::Error);
        assert_eq!(cancel.error.unwrap().code, ERROR_NOT_FOUND);
        let status = finish_first_record.status();
        assert_eq!(status.state, RunStateName::Finished);
        assert_eq!(status.final_answer.as_deref(), Some("done"));
        assert_eq!(status.error, None);
        assert!(!finish_first_record.cancel.load(Ordering::SeqCst));
        assert!(finish_first_record.events.lock().unwrap().events.is_empty());
    }

    fn assert_terminal_waits_for_collector(
        case: &'static str,
        outcome: AppResult<RunOutcome>,
        expected_status: RunStateName,
    ) {
        let runtime = test_runtime();
        let record = test_run_record(case);
        runtime.reserve_run(record.clone()).unwrap();
        let (event_sender, event_receiver) = mpsc::channel();
        for delta_index in 0..2 {
            event_sender.send(test_delta(case, delta_index)).unwrap();
        }
        drop(event_sender);

        let collected = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let collector_record = record.clone();
        let collector_collected = collected.clone();
        let collector_release = release.clone();
        let event_collector = thread::spawn(move || {
            let pending = event_receiver.into_iter().collect::<Vec<_>>();
            collector_collected.wait();
            collector_release.wait();
            for event in pending {
                collect_run_event(&collector_record, event);
            }
        });
        collected.wait();

        let finish_started = Arc::new(Barrier::new(2));
        let finisher_started = finish_started.clone();
        let finisher_runtime = runtime.clone();
        let finisher_record = record.clone();
        let (finished_sender, finished_receiver) = mpsc::channel();
        let finisher = thread::spawn(move || {
            finisher_started.wait();
            let result = finish_run_after_event_collection(
                &finisher_runtime,
                &finisher_record,
                outcome,
                event_collector,
            );
            finished_sender.send(result).unwrap();
        });
        finish_started.wait();

        assert_eq!(record.status().state, RunStateName::Running);
        let buffer = record.events.lock().unwrap();
        assert_eq!(buffer.first_offset, 0);
        assert_eq!(buffer.next_offset, 0);
        assert!(buffer.events.is_empty());
        drop(buffer);
        assert!(
            matches!(
                finished_receiver.recv_timeout(Duration::from_millis(100)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ),
            "{case} terminal status published before collector release"
        );

        release.wait();
        let result = finished_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        finisher.join().unwrap();
        let status = record.status();
        assert_eq!(status.state, expected_status);
        match expected_status {
            RunStateName::Finished => {
                assert_eq!(result.unwrap().final_answer, "done");
                assert_eq!(status.final_answer.as_deref(), Some("done"));
                assert_eq!(status.error, None);
            }
            RunStateName::Failed => {
                assert!(matches!(
                    result,
                    Err(AppError::RunFailed(ref reason)) if reason == "provider failed"
                ));
                assert_eq!(status.final_answer, None);
                assert_eq!(
                    status.error.as_deref(),
                    Some("run did not finish: provider failed")
                );
            }
            RunStateName::Canceled => {
                assert!(matches!(result, Err(AppError::RunCanceled)));
                assert_eq!(status.final_answer, None);
                assert_eq!(
                    status.error.as_deref(),
                    Some("run did not finish: run canceled")
                );
            }
            unexpected => panic!("unexpected terminal test status: {unexpected}"),
        }

        let buffer = record.events.lock().unwrap();
        assert_eq!(buffer.first_offset, 0);
        assert_eq!(buffer.next_offset, 2);
        assert_eq!(
            buffer
                .events
                .iter()
                .map(|event| event.offset)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
    }

    fn cancel_run(runtime: &DaemonRuntime, request_id: &str, run_id: &str) -> Envelope {
        handle_line(
            runtime,
            &format!(
                r#"{{"v":1,"id":"{request_id}","kind":"request","method":"run.cancel","params":{{"run_id":"{run_id}"}}}}"#
            ),
        )
    }

    fn decide_session_grant(
        runtime: &DaemonRuntime,
        request_id: &str,
        run_id: &str,
        call_id: &str,
    ) -> Envelope {
        handle_line(
            runtime,
            &format!(
                r#"{{"v":1,"id":"{request_id}","kind":"request","method":"approval.decide","params":{{"run_id":"{run_id}","tool_call_id":"{call_id}","decision":"grant_session"}}}}"#
            ),
        )
    }

    fn test_approval_request(
        run_id: &str,
        call_id: &str,
        tool_name: &str,
        effect: EffectClass,
    ) -> ApprovalRequest {
        ApprovalRequest {
            run_id: RunId::new(run_id).unwrap(),
            call_id: ToolCallId::new(call_id).unwrap(),
            tool_name: tool_name.into(),
            effect,
            reason: "test approval required".into(),
            input_preview: Some("{}".into()),
            approval_preview: None,
            diff_preview: None,
        }
    }

    fn spawn_shell_approval_waiter(
        runtime: &DaemonRuntime,
        record: &Arc<RunRecord>,
    ) -> thread::JoinHandle<ExternalApprovalOutcome> {
        let decide = approval_handler(runtime.clone(), record.clone());
        let request = test_approval_request(
            &record.run_id,
            "call_1",
            SHELL_EXEC,
            EffectClass::ExternalSideEffect,
        );
        thread::spawn(move || decide(request).unwrap())
    }

    fn wait_for_pending_approval(record: &RunRecord) {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while record.pending_approval().is_none() {
            assert!(
                std::time::Instant::now() < deadline,
                "approval did not become pending"
            );
            thread::yield_now();
        }
    }

    #[cfg(target_os = "linux")]
    fn assert_supervised_terminal_publication(cleanup_failure: bool) {
        use crate::{
            app::prepare_run,
            daemon::run_child::{
                SupervisedTestLaunch, TerminalStageBarriers, run_supervised_for_test,
            },
        };

        let root = tempfile::tempdir().unwrap();
        let root_path = root.path().to_path_buf();
        let workspace = root.path().join("w");
        std::fs::create_dir(&workspace).unwrap();
        let config_path = workspace.join("plato.toml");
        std::fs::write(
            &config_path,
            r#"[provider]
kind = "open_ai"
model = "test-model"
api_key_env = "PATH"
base_url = "http://127.0.0.1:1"

[limits]
token_budget = 4000
max_output_tokens = 64
max_turns = 1

[tools]
enabled = ["file.read"]
"#,
        )
        .unwrap();
        let runtime = DaemonRuntime::new(crate::daemon::server::DaemonPaths {
            workspace_root: workspace.canonicalize().unwrap(),
            workspace_id: "terminal-order".into(),
            socket_path: root.path().join("a.sock"),
            lock_path: root.path().join("a.lock"),
            ledger_path: root
                .path()
                .join("state/platonic/workspaces/terminal-order/agent.db"),
            server_db_path: root.path().join("state/platonic/server.db"),
        });
        let case = if cleanup_failure {
            "cleanup"
        } else {
            "success"
        };
        let run_id = RunId::new(format!("run_terminal_{case}")).unwrap();
        let record = Arc::new(RunRecord::new(
            run_id.to_string(),
            format!("session_terminal_{case}"),
            runtime.paths.ledger_path.clone(),
        ));
        runtime.reserve_run(record.clone()).unwrap();
        let (prepared, recorder) = prepare_run(&RunOptions {
            question: "prove terminal publication ordering".into(),
            config_path: Some(config_path),
            overrides: Default::default(),
            ledger: RunLedger::DefaultSqlite(runtime.paths.default_ledger()),
            workspace_root: runtime.paths.workspace_root.clone(),
            approval_mode: ApprovalMode::Deny { actor: "test" },
            run_id: Some(run_id.clone()),
            session: Some(RunSession::Fresh {
                session_id: record.session_id.clone(),
            }),
            event_sender: None,
            stream_to_stderr: false,
            cancel: None,
            voice_interruption_context: None,
        })
        .unwrap();

        let turn_id = TurnId::new("turn_terminal").unwrap();
        let nonterminal_events = vec![
            HarnessEvent::RunStarted {
                run_id: run_id.clone(),
                agent_id: AgentId::new("plato").unwrap(),
            },
            HarnessEvent::ContextBuilt {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                context: ContextPack {
                    token_budget: 4000,
                    fragments: vec![],
                },
            },
            HarnessEvent::ModelRequested {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                step: 0,
                model: ModelName::new("test-model").unwrap(),
            },
            HarnessEvent::ModelResponded {
                run_id: run_id.clone(),
                turn_id,
                step: 0,
                output: Message {
                    role: MessageRole::Assistant,
                    content: "done".into(),
                },
                proposed_calls: vec![],
                served_model: None,
                usage: None,
            },
        ];
        let nonterminal = nonterminal_events
            .into_iter()
            .enumerate()
            .map(|(index, event)| {
                json!({
                    "kind": "record",
                    "request_id": index + 1,
                    "operation": {"operation": "event", "event": event}
                })
            })
            .map(|message| format!("printf '%s\\n' '{message}'\nIFS= read -r _\n"))
            .collect::<String>();
        let terminal = json!({
            "kind": "record",
            "request_id": 5,
            "operation": {
                "operation": "finish",
                "run_id": run_id,
                "final_answer": "done"
            }
        });
        let result = json!({
            "kind": "result",
            "request_id": 6,
            "result": {
                "status": "finished",
                "outcome": {"run_id": run_id, "final_answer": "done"}
            }
        });
        let fifo = root.path().join("r");
        let descendant_pid_path = root.path().join("d.pid");
        let fixture = root.path().join("child");
        let stderr = if cleanup_failure {
            "printf 'cleanup drain failure\\n' >&2"
        } else {
            ":"
        };
        std::fs::write(
            &fixture,
            format!(
                r#"#!/bin/sh
mkfifo '{fifo}'
/bin/sh -c 'IFS= read -r _ < "$1"' sh '{fifo}' &
printf '%s\n' "$!" > '{descendant_pid_path}'
IFS= read -r _
printf '{{"kind":"ready","request_id":0,"pid":%s}}\n' "$$"
IFS= read -r _
{nonterminal}
printf '%s\n' '{terminal}'
IFS= read -r _
printf 'release\n' > '{fifo}'
wait
rm -f '{fifo}'
printf '%s\n' '{result}'
IFS= read -r _
{stderr}
"#,
                fifo = fifo.display(),
                descendant_pid_path = descendant_pid_path.display(),
            ),
        )
        .unwrap();
        std::fs::set_permissions(&fixture, std::fs::Permissions::from_mode(0o700)).unwrap();

        let (event_sender, event_receiver) = mpsc::channel();
        let event_collector = spawn_event_collector(record.clone(), event_receiver);
        let terminal_reached = Arc::new(Barrier::new(2));
        let terminal_release = Arc::new(Barrier::new(2));
        let (ready_sender, ready_receiver) = mpsc::channel();
        let (finished_sender, finished_receiver) = mpsc::channel();
        let worker_runtime = runtime.clone();
        let worker_record = record.clone();
        let reached = terminal_reached.clone();
        let release = terminal_release.clone();
        let worker = thread::spawn(move || {
            let completion = run_supervised_for_test(
                prepared,
                recorder,
                ApprovalMode::Deny { actor: "test" },
                event_sender,
                Arc::new(std::sync::atomic::AtomicBool::new(false)),
                SupervisedTestLaunch {
                    executable: fixture,
                    ready_child: ready_sender,
                    terminal_stage_barriers: TerminalStageBarriers { reached, release },
                },
            );
            let outcome = finish_run_after_event_collection(
                &worker_runtime,
                &worker_record,
                completion,
                event_collector,
            );
            finished_sender.send(outcome).unwrap();
        });

        let child_pid = ready_receiver.recv_timeout(Duration::from_secs(2)).unwrap();
        terminal_reached.wait();
        let descendant_pid = std::fs::read_to_string(&descendant_pid_path)
            .unwrap()
            .trim()
            .parse::<u32>()
            .unwrap();
        let before = read_run_transcript(&runtime.paths.default_ledger(), run_id.as_str()).unwrap();
        assert_eq!(before.status, RunStateName::Running);
        assert_eq!(before.final_answer, None);
        assert_eq!(record.status().state, RunStateName::Running);
        assert_eq!(
            runtime.shutdown_if_idle(),
            ShutdownIfIdleDecision::RefusedActive
        );
        let before_records = SqliteLedger::open_default_readonly(&runtime.paths.default_ledger())
            .unwrap()
            .read_session_run(run_id.as_str())
            .unwrap();
        assert_eq!(terminal_count(&before_records.records), 0);

        terminal_release.wait();
        let outcome = finished_receiver
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        worker.join().unwrap();
        assert!(!Path::new(&format!("/proc/{child_pid}")).exists());
        assert!(!Path::new(&format!("/proc/{descendant_pid}")).exists());
        assert!(!fifo.exists());

        let after = read_run_transcript(&runtime.paths.default_ledger(), run_id.as_str()).unwrap();
        let after_records = SqliteLedger::open_default_readonly(&runtime.paths.default_ledger())
            .unwrap()
            .read_session_run(run_id.as_str())
            .unwrap();
        assert_eq!(
            terminal_count(&after_records.records),
            1,
            "outcome={outcome:?} transcript={after:?} records={:?}",
            after_records.records
        );
        let buffered_terminals = record
            .events
            .lock()
            .unwrap()
            .events
            .iter()
            .filter(|event| {
                matches!(
                    &event.event,
                    StreamEvent::Ledger {
                        record: RecordedEvent {
                            event: HarnessEvent::RunFinished { .. }
                                | HarnessEvent::RunFailed { .. },
                            ..
                        }
                    }
                )
            })
            .count();
        assert_eq!(buffered_terminals, 1);

        if cleanup_failure {
            let reason = match outcome {
                Err(AppError::SupervisedRun(reason)) => reason,
                unexpected => panic!("unexpected cleanup outcome: {unexpected:?}"),
            };
            assert!(reason.contains("cleanup drain failure"));
            assert_eq!(after.status, RunStateName::Failed);
            assert_eq!(after.final_answer, None);
            assert!(matches!(
                after_records.records.last().map(|record| &record.event),
                Some(HarnessEvent::RunFailed { reason: recorded, .. }) if recorded == &reason
            ));
        } else {
            assert_eq!(outcome.unwrap().final_answer, "done");
            assert_eq!(after.status, RunStateName::Finished);
            assert_eq!(after.final_answer.as_deref(), Some("done"));
            assert!(matches!(
                after_records.records.last().map(|record| &record.event),
                Some(HarnessEvent::RunFinished { .. })
            ));
        }
        assert_eq!(runtime.shutdown_if_idle(), ShutdownIfIdleDecision::Shutdown);

        drop(before_records);
        drop(after_records);
        drop(record);
        drop(runtime);
        drop(root);
        assert!(!root_path.exists());
    }

    #[cfg(target_os = "linux")]
    fn terminal_count(records: &[RecordedEvent]) -> usize {
        records
            .iter()
            .filter(|record| {
                matches!(
                    record.event,
                    HarnessEvent::RunFinished { .. } | HarnessEvent::RunFailed { .. }
                )
            })
            .count()
    }

    fn stream_run(runtime: &DaemonRuntime, request_id: &str, run_id: &str) -> EventsStreamResult {
        let response = handle_line(
            runtime,
            &format!(
                r#"{{"v":1,"id":"{request_id}","kind":"request","method":"events.stream","params":{{"run_id":"{run_id}","from_offset":0,"limit":128}}}}"#
            ),
        );
        assert_eq!(
            response.kind,
            crate::daemon::protocol::EnvelopeKind::Response
        );
        response_result(&response)
    }

    fn test_runtime() -> DaemonRuntime {
        DaemonRuntime::new(crate::daemon::server::DaemonPaths {
            workspace_root: PathBuf::from("/tmp/workspace"),
            workspace_id: "workspace-1".into(),
            socket_path: PathBuf::from("/tmp/agent.sock"),
            lock_path: PathBuf::from("/tmp/agent.lock"),
            ledger_path: PathBuf::from("/tmp/agent.db"),
            server_db_path: PathBuf::from("/tmp/platonic-server.db"),
        })
    }

    fn test_run_record(case: &str) -> Arc<RunRecord> {
        Arc::new(RunRecord::new(
            format!("run_{case}"),
            format!("session_{case}"),
            PathBuf::from("/tmp/agent.db"),
        ))
    }

    fn test_delta(case: &str, delta_index: u64) -> RunEvent {
        RunEvent::AssistantDelta(AssistantDeltaEvent {
            run_id: RunId::new(format!("run_{case}")).unwrap(),
            turn_id: TurnId::new("turn_1").unwrap(),
            step: 0,
            delta_index,
            text: format!("delta {delta_index}"),
        })
    }

    #[test]
    fn typed_entries_omit_context_compaction() {
        let entries = typed_entries(
            "current question",
            vec![ReadbackEntry::ContextCompacted {
                turn_id: TurnId::new("turn_1").unwrap(),
                estimated_tokens_before: 321,
                estimated_tokens_after: 123,
                dropped_turn_start: 0,
                dropped_turn_end_exclusive: 2,
            }],
        );

        assert_eq!(
            entries,
            vec![TypedTranscriptEntry::User {
                text: "current question".into()
            }]
        );
    }

    #[test]
    fn model_status_follows_latest_durable_request_or_response() {
        let run_id = RunId::new("run_1").unwrap();
        let turn_id = TurnId::new("turn_1").unwrap();
        let record = |seq, event| RecordedEvent {
            seq,
            occurred_at_ms: seq,
            event,
        };
        let mut records = vec![record(
            0,
            HarnessEvent::ModelRequested {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                step: 0,
                model: ModelName::new("~openai/gpt-latest").unwrap(),
            },
        )];
        assert_eq!(
            model_status(&records),
            Some(ModelIdentityStatus::Requested {
                model: "~openai/gpt-latest".into()
            })
        );

        records.push(record(
            1,
            HarnessEvent::ModelResponded {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                step: 0,
                output: Message {
                    role: MessageRole::Assistant,
                    content: "tool call".into(),
                },
                proposed_calls: vec![],
                served_model: Some(ModelName::new("openai/gpt-5.2-2026-08-01").unwrap()),
                usage: None,
            },
        ));
        assert_eq!(
            model_status(&records),
            Some(ModelIdentityStatus::Responded {
                served_model: Some("openai/gpt-5.2-2026-08-01".into())
            })
        );

        records.push(record(
            2,
            HarnessEvent::ModelRequested {
                run_id: run_id.clone(),
                turn_id: TurnId::new("turn_2").unwrap(),
                step: 1,
                model: ModelName::new("~openai/gpt-latest").unwrap(),
            },
        ));
        assert!(matches!(
            model_status(&records),
            Some(ModelIdentityStatus::Requested { .. })
        ));

        records.push(record(
            3,
            HarnessEvent::ModelResponded {
                run_id,
                turn_id: TurnId::new("turn_2").unwrap(),
                step: 1,
                output: Message {
                    role: MessageRole::Assistant,
                    content: "done".into(),
                },
                proposed_calls: vec![],
                served_model: None,
                usage: None,
            },
        ));
        assert_eq!(
            model_status(&records),
            Some(ModelIdentityStatus::Responded { served_model: None })
        );
    }

    #[test]
    fn context_compaction_uses_v1_ledger_stream_envelope() {
        let runtime = DaemonRuntime::new(crate::daemon::server::DaemonPaths {
            workspace_root: PathBuf::from("/tmp/workspace"),
            workspace_id: "workspace-1".into(),
            socket_path: PathBuf::from("/tmp/agent.sock"),
            lock_path: PathBuf::from("/tmp/agent.lock"),
            ledger_path: PathBuf::from("/tmp/agent.db"),
            server_db_path: PathBuf::from("/tmp/platonic-server.db"),
        });
        let record = Arc::new(RunRecord::new(
            "run_1".into(),
            "session_1".into(),
            PathBuf::from("/tmp/agent.db"),
        ));
        runtime.reserve_run(record.clone()).unwrap();
        record.push_recorded_event(RecordedEvent {
            seq: 1,
            occurred_at_ms: 42,
            event: HarnessEvent::ContextCompacted {
                run_id: RunId::new("run_1").unwrap(),
                turn_id: TurnId::new("turn_1").unwrap(),
                estimated_tokens_before: 321,
                estimated_tokens_after: 123,
                dropped_turn_start: 0,
                dropped_turn_end_exclusive: 2,
            },
        });

        let response = handle_events_stream(
            &runtime,
            Envelope {
                v: 1,
                id: Some("stream_1".into()),
                kind: crate::daemon::protocol::EnvelopeKind::Request,
                method: Some("events.stream".into()),
                params: None,
                result: None,
                error: None,
            },
            EventsStreamParams {
                run_id: "run_1".into(),
                from_offset: Some(0),
                limit: Some(1),
            },
        );

        assert_eq!(response.v, 1);
        let result: EventsStreamResult = response_result(&response);
        assert_eq!(result.events.len(), 1);
        let wire = serde_json::to_value(&result.events[0]).unwrap();
        assert_eq!(wire["event"]["kind"], "ledger");
        assert_eq!(
            wire["event"]["record"]["event"],
            json!({
                "event": "context_compacted",
                "run_id": "run_1",
                "turn_id": "turn_1",
                "estimated_tokens_before": 321,
                "estimated_tokens_after": 123,
                "dropped_turn_start": 0,
                "dropped_turn_end_exclusive": 2
            })
        );
    }

    #[test]
    fn typed_entries_map_all_human_readback_facts_in_order() {
        let turn_id = TurnId::new("turn_1").unwrap();
        let call_id = ToolCallId::new("call_1").unwrap();
        let entries = typed_entries(
            "do work",
            vec![
                ReadbackEntry::ContextFragment {
                    turn_id: turn_id.clone(),
                    fragment: ContextFragment {
                        lane: ContextLane::CurrentTask,
                        source: "user".into(),
                        content: "diagnostic context".into(),
                        estimated_tokens: 2,
                    },
                },
                ReadbackEntry::ModelMessage {
                    turn_id: turn_id.clone(),
                    message: Message {
                        role: MessageRole::Assistant,
                        content: "working".into(),
                    },
                    served_model: None,
                },
                ReadbackEntry::ToolCall {
                    turn_id,
                    call: ToolCall {
                        id: call_id.clone(),
                        tool: ToolName::new("file.write").unwrap(),
                        effect: EffectClass::WorkspaceWrite,
                        input: json!({"path": "out.txt", "content": "done"}),
                    },
                },
                ReadbackEntry::ApprovalGranted {
                    call_id: call_id.clone(),
                    actor_id: ActorId::new("human_1").unwrap(),
                },
                ReadbackEntry::ToolResult {
                    result: ToolResult {
                        call_id: call_id.clone(),
                        summary: "wrote out.txt".into(),
                        data: json!({"bytes": 4}),
                        artifacts: vec![],
                        visibility: ResultVisibility::Both,
                    },
                },
                ReadbackEntry::ApprovalDenied {
                    call_id: ToolCallId::new("call_2").unwrap(),
                    actor_id: ActorId::new("human_2").unwrap(),
                    reason: "not now".into(),
                },
                ReadbackEntry::PolicyDenied {
                    call_id: ToolCallId::new("call_3").unwrap(),
                    reason: "secret access denied".into(),
                },
                ReadbackEntry::ToolFailed {
                    call_id: ToolCallId::new("call_4").unwrap(),
                    reason: "tool crashed".into(),
                },
            ],
        );

        assert_eq!(
            entries,
            vec![
                TypedTranscriptEntry::User {
                    text: "do work".into()
                },
                TypedTranscriptEntry::Assistant {
                    text: "working".into()
                },
                TypedTranscriptEntry::ToolCall {
                    call_id: "call_1".into(),
                    tool: "file.write".into(),
                    input: json!({"path": "out.txt", "content": "done"}),
                },
                TypedTranscriptEntry::Approval {
                    call_id: "call_1".into(),
                    decision: ApprovalDecisionName::Granted,
                    actor_id: "human_1".into(),
                    reason: None,
                },
                TypedTranscriptEntry::ToolResult {
                    call_id: "call_1".into(),
                    summary: "wrote out.txt".into(),
                },
                TypedTranscriptEntry::Approval {
                    call_id: "call_2".into(),
                    decision: ApprovalDecisionName::Denied,
                    actor_id: "human_2".into(),
                    reason: Some("not now".into()),
                },
                TypedTranscriptEntry::PolicyDenied {
                    call_id: "call_3".into(),
                    reason: "secret access denied".into(),
                },
                TypedTranscriptEntry::ToolFailed {
                    call_id: "call_4".into(),
                    error: "tool crashed".into(),
                },
            ]
        );
    }
}
