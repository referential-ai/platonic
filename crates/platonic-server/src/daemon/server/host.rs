use super::{DaemonPaths, reconcile::reconcile_thread_repositories};
use crate::{
    AppResult,
    daemon::{
        handlers::{handle_line, handle_request, reconcile_one_shot_run_roots},
        protocol::{
            ERROR_INTERNAL, ERROR_MALFORMED_REQUEST, ERROR_WORKSPACE_MISMATCH,
            ERROR_WORKSPACE_UNREGISTERED, Envelope, EnvelopeKind, HelloParams, ProtocolErrorCode,
            ProtocolMethod, ProtocolRequest, ProtocolResponse, decode_request,
        },
        runtime::{DaemonRuntime, RuntimeState},
    },
};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex, atomic::AtomicBool},
    time::Instant,
};

const HOST_DAEMON_SCOPE: &str = "host";

#[derive(Clone, Debug)]
pub(super) struct HostRuntime {
    pub(super) socket_path: PathBuf,
    started_at: Instant,
    state: Arc<Mutex<RuntimeState>>,
    pub(super) control_runtime: DaemonRuntime,
    workspaces: Arc<Mutex<HashMap<PathBuf, DaemonRuntime>>>,
}

impl HostRuntime {
    pub(super) fn new(socket_path: PathBuf) -> AppResult<Self> {
        let max_spawn_depth = crate::config::server_max_spawn_depth()?;
        let require_confinement = crate::config::server_require_confinement()?;
        let confinement_support = crate::confinement::detect_support();
        let started_at = Instant::now();
        let state = Arc::new(Mutex::new(RuntimeState::default()));
        let stop_requested = Arc::new(AtomicBool::new(false));
        let control_paths =
            DaemonPaths::resolve(&std::env::current_dir()?, Some(socket_path.clone()))?;
        reconcile_thread_repositories(&control_paths.server_db_path)?;
        reconcile_one_shot_run_roots(&control_paths.server_db_path)?;
        let control_runtime = DaemonRuntime::new_shared(
            control_paths,
            max_spawn_depth,
            require_confinement,
            confinement_support,
            started_at,
            Arc::clone(&state),
            Arc::clone(&stop_requested),
        );
        Ok(Self {
            socket_path,
            started_at,
            state,
            control_runtime,
            workspaces: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    fn workspace_runtime(
        &self,
        params: &HelloParams,
    ) -> Result<DaemonRuntime, (ProtocolErrorCode, String)> {
        let workspace_root = PathBuf::from(&params.workspace_root)
            .canonicalize()
            .map_err(|error| {
                (
                    ERROR_WORKSPACE_MISMATCH,
                    format!("workspace_root cannot be resolved: {error}"),
                )
            })?;
        let paths = DaemonPaths::resolve(&workspace_root, Some(self.socket_path.clone()))
            .map_err(|error| (ERROR_INTERNAL, error.to_string()))?;
        if !paths.is_registered() {
            return Err((
                ERROR_WORKSPACE_UNREGISTERED,
                format!(
                    "workspace is not registered: {}; run platonic workspace create <name> \"{}\"",
                    workspace_root.display(),
                    workspace_root.display()
                ),
            ));
        }
        let legacy_workspace_id = crate::paths::workspace_id(&workspace_root).map_err(|error| {
            (
                ERROR_WORKSPACE_MISMATCH,
                format!("workspace_id cannot be derived: {error}"),
            )
        })?;
        if params.workspace_id != paths.workspace_id && params.workspace_id != legacy_workspace_id {
            return Err((
                ERROR_WORKSPACE_MISMATCH,
                format!(
                    "workspace_id mismatch: expected {}, got {}",
                    paths.workspace_id, params.workspace_id
                ),
            ));
        }

        let mut workspaces = self
            .workspaces
            .lock()
            .expect("host workspace runtime lock poisoned");
        if let Some(runtime) = workspaces.get(&workspace_root) {
            return Ok(runtime.clone());
        }

        crate::ledger::interrupt_orphaned_default_sqlite_runs(&paths.default_ledger())
            .map_err(|error| (ERROR_INTERNAL, error.to_string()))?;
        let runtime = DaemonRuntime::new_shared(
            paths,
            self.control_runtime.max_spawn_depth(),
            self.control_runtime.require_confinement(),
            self.control_runtime.confinement_support(),
            self.started_at,
            Arc::clone(&self.state),
            Arc::clone(&self.control_runtime.stop_requested),
        );
        crate::daemon::returns::reconcile_workspace(&runtime)
            .map_err(|error| (ERROR_INTERNAL, error.to_string()))?;
        workspaces.insert(workspace_root, runtime.clone());
        Ok(runtime)
    }
}

pub(super) fn handle_host_line(
    host: &HostRuntime,
    workspace_runtime: &mut Option<DaemonRuntime>,
    line: &str,
) -> Envelope {
    if let Some(runtime) = workspace_runtime {
        return add_host_scope(handle_line(runtime, line));
    }

    let request = match decode_request(line) {
        Ok(request) => request,
        Err(error) => return *error,
    };
    if request.method.is_some_and(is_control_method) {
        return handle_request(&host.control_runtime, request);
    }
    if request.method != Some(ProtocolMethod::Hello) {
        let method = request.method;
        return Envelope::error(
            request.id,
            method.map(|method| method.to_string()),
            ERROR_MALFORMED_REQUEST,
            format!(
                "host daemon requires hello before {}",
                method.map_or("request", ProtocolMethod::as_str)
            ),
        );
    }
    let params = match request.params.as_ref() {
        Some(ProtocolRequest::Hello(params)) => params.clone(),
        _ => unreachable!("hello request carries hello params"),
    };
    let runtime = match host.workspace_runtime(&params) {
        Ok(runtime) => runtime,
        Err((code, message)) => {
            return Envelope::error(request.id, Some("hello".into()), code, message);
        }
    };
    let response = add_host_scope(handle_request(&runtime, request));
    if response.kind == EnvelopeKind::Response {
        *workspace_runtime = Some(runtime);
    }
    response
}

fn is_control_method(method: ProtocolMethod) -> bool {
    matches!(
        method,
        ProtocolMethod::DaemonShutdownIfIdle
            | ProtocolMethod::WorkspaceCreate
            | ProtocolMethod::WorkspaceList
            | ProtocolMethod::WorkspaceStatus
            | ProtocolMethod::ProfileCreate
            | ProtocolMethod::ProfileList
            | ProtocolMethod::ProfileStatus
            | ProtocolMethod::ProfileUpdate
    )
}

fn add_host_scope(mut response: Envelope) -> Envelope {
    if let Some(ProtocolResponse::Hello(result)) = response.result.as_mut() {
        result.daemon_scope = Some(HOST_DAEMON_SCOPE.into());
    }
    response
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::paths;
    use std::fs;

    #[test]
    fn host_startup_removes_only_abandoned_one_shot_run_roots() {
        let root = tempfile::tempdir().unwrap();
        let home = root.path().join("home");
        let workspace = root.path().join("workspace");
        fs::create_dir(&home).unwrap();
        fs::create_dir(&workspace).unwrap();
        fs::write(workspace.join("preserved.txt"), "workspace\n").unwrap();

        temp_env::with_vars(
            [("HOME", Some(home.as_os_str())), ("PLATO_CONFIG", None)],
            || {
                paths::with_test_xdg(root.path(), || {
                    let server_db = crate::paths::server_db_path().unwrap();
                    let abandoned =
                        crate::paths::one_shot_run_root(&server_db, "run_abandoned").unwrap();
                    fs::create_dir_all(abandoned.join("scratch")).unwrap();
                    fs::write(abandoned.join("scratch/residue.txt"), "residue\n").unwrap();

                    let _host = HostRuntime::new(root.path().join("host.sock")).unwrap();

                    assert!(!abandoned.exists());
                    assert_eq!(
                        fs::read_to_string(workspace.join("preserved.txt")).unwrap(),
                        "workspace\n"
                    );
                });
            },
        );
    }

    #[test]
    fn host_runtime_freezes_and_carries_configured_spawn_depth() {
        let root = tempfile::tempdir().unwrap();
        let home = root.path().join("home");
        let config_path = home.join(".config/plato/config.toml");
        let workspace = root.path().join("workspace");
        fs::create_dir(&workspace).unwrap();
        fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        fs::write(
            &config_path,
            "[limits]\nmax_spawn_depth = 2\n[confinement]\nrequire = true\n",
        )
        .unwrap();

        temp_env::with_vars(
            [("HOME", Some(home.as_os_str())), ("PLATO_CONFIG", None)],
            || {
                paths::with_test_xdg(root.path(), || {
                    let host = HostRuntime::new(root.path().join("host.sock")).unwrap();
                    assert_eq!(host.control_runtime.max_spawn_depth(), 2);
                    assert!(host.control_runtime.require_confinement());

                    fs::write(
                        &config_path,
                        "[limits]\nmax_spawn_depth = 99\n[confinement]\nrequire = false\n",
                    )
                    .unwrap();
                    fs::write(
                        workspace.join("plato.toml"),
                        "[limits]\nmax_spawn_depth = 77\n",
                    )
                    .unwrap();
                    host.control_runtime
                        .paths
                        .server_store()
                        .unwrap()
                        .register_workspace(
                            "ws-host-depth",
                            "host-depth",
                            &workspace.canonicalize().unwrap().to_string_lossy(),
                            &root.path().join("ledger.db").to_string_lossy(),
                            1,
                        )
                        .unwrap();
                    let workspace_runtime = host
                        .workspace_runtime(&HelloParams {
                            workspace_root: workspace.to_string_lossy().into_owned(),
                            workspace_id: "ws-host-depth".into(),
                        })
                        .unwrap();

                    assert_eq!(host.control_runtime.max_spawn_depth(), 2);
                    assert_eq!(workspace_runtime.max_spawn_depth(), 2);
                    assert!(workspace_runtime.require_confinement());
                });
            },
        );
    }
}
