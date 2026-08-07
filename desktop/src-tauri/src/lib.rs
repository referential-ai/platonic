use plato_daemon_client::{
    ClientError,
    client::{DaemonClient, DaemonConnectionConfig},
    paths,
};
use plato_protocol::{
    ApprovalDecisionName, BufferedStreamEvent, CommandAcceptedResult, EventsStreamResult,
    HarnessEvent, HelloResult, PendingApprovalSnapshot, PolicyDecision, RunStartResult,
    RunStateName, SessionSummary, StreamEvent, TranscriptReadResult, TypedRun,
    TypedTranscriptEntry,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};
use tauri::Manager;
use tauri_plugin_dialog::DialogExt;

#[cfg(windows)]
mod lifecycle;
#[cfg(unix)]
mod unix_lifecycle;
#[cfg(all(test, unix))]
mod unix_proof;
#[cfg(all(test, windows))]
mod windows_installer_proof;
#[cfg(all(test, windows))]
mod windows_proof;

const REQUIRED_CAPABILITIES: [&str; 10] = [
    "hello",
    "run.start",
    "message.append",
    "events.stream",
    "approval.decide",
    "run.cancel",
    "sessions.list",
    "transcript.read",
    "transcript.read.typed",
    "transcript.read.pending_approval",
];
const EVENT_PAGE_SIZE: usize = 128;
const INPUT_PREVIEW_MAX_CHARS: usize = 2_000;
#[cfg(any(windows, unix))]
const DAEMON_ATTACH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
#[cfg(windows)]
const DAEMON_ATTACH_CANCEL_GRACE: std::time::Duration = std::time::Duration::from_millis(100);
#[cfg(any(windows, unix))]
const DAEMON_ATTACH_RETRY: std::time::Duration = std::time::Duration::from_millis(50);

struct DesktopState {
    workspace_file: PathBuf,
    lifecycle: Arc<Mutex<DesktopLifecycle>>,
    launch: DaemonLaunch,
}

#[derive(Clone, Debug, Default)]
struct DaemonLaunch {
    #[cfg(any(windows, unix))]
    executable: Option<PathBuf>,
}

impl DaemonLaunch {
    #[cfg(windows)]
    fn installed() -> Result<Self, std::io::Error> {
        Ok(Self {
            executable: Some(lifecycle::sibling_daemon_executable()?),
        })
    }

    #[cfg(unix)]
    fn installed() -> Result<Self, std::io::Error> {
        Ok(Self {
            executable: Some(unix_lifecycle::sibling_daemon_executable()?),
        })
    }

    #[cfg(not(any(windows, unix)))]
    fn installed() -> Result<Self, std::io::Error> {
        Ok(Self::default())
    }
}

#[derive(Default)]
struct DesktopLifecycle {
    workspace_root: Option<PathBuf>,
    #[cfg(windows)]
    workspace_instance: Option<lifecycle::WorkspaceInstance>,
    #[cfg(any(windows, unix))]
    spawned_daemon: Option<SpawnedDaemon>,
}

struct PreparedWorkspace {
    workspace_root: PathBuf,
    #[cfg(windows)]
    workspace_id: String,
    #[cfg(windows)]
    instance: Option<lifecycle::WorkspaceInstance>,
}

#[cfg(any(windows, unix))]
struct SpawnedDaemon {
    workspace_id: String,
    child: std::process::Child,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct DesktopError {
    code: String,
    message: String,
}

impl DesktopError {
    fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }

    fn daemon(context: &str, error: impl Into<ClientError>) -> Self {
        match error.into() {
            ClientError::DaemonResponse(error) => Self::new(error.code, error.message),
            ClientError::DaemonProtocol(message) => {
                Self::new("incompatible_daemon", format!("{context}: {message}"))
            }
            ClientError::Json(error) => Self::new(
                "incompatible_daemon",
                format!("{context}: invalid daemon response: {error}"),
            ),
            ClientError::Io(error) => {
                Self::new("daemon_unavailable", format!("{context}: {error}"))
            }
            error => Self::new("desktop_error", format!("{context}: {error}")),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(
    tag = "state",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
enum BootstrapView {
    NeedsWorkspace {
        reason: Option<String>,
    },
    Ready {
        workspace_root: String,
        daemon_version: String,
        sessions: Vec<DesktopSession>,
        selected_run: Option<DesktopRun>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct DesktopSession {
    session_id: String,
    run_id: String,
    status: RunStateName,
    latest_question: String,
}

impl From<SessionSummary> for DesktopSession {
    fn from(session: SessionSummary) -> Self {
        Self {
            session_id: session.session_id,
            run_id: session.run_id,
            status: session.status,
            latest_question: session.latest_question,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct DesktopRun {
    run_id: String,
    session_index: u64,
    status: RunStateName,
    entries: Vec<DesktopEntry>,
}

impl TryFrom<TypedRun> for DesktopRun {
    type Error = DesktopError;

    fn try_from(run: TypedRun) -> Result<Self, Self::Error> {
        let mut assistant_step = 0_u32;
        let mut entries = Vec::with_capacity(run.entries.len());
        for entry in run.entries {
            let step =
                matches!(entry, TypedTranscriptEntry::Assistant { .. }).then_some(assistant_step);
            if step.is_some() {
                assistant_step = assistant_step.checked_add(1).ok_or_else(|| {
                    DesktopError::new(
                        "incompatible_daemon",
                        "typed transcript contains too many assistant steps",
                    )
                })?;
            }
            entries.push(DesktopEntry::from_typed(entry, step));
        }
        Ok(Self {
            run_id: run.run_id,
            session_index: run.session_index,
            status: run.status,
            entries,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
enum DesktopEntry {
    User {
        text: String,
    },
    Assistant {
        step: u32,
        text: String,
    },
    ToolCall {
        call_id: String,
        tool: String,
        input_preview: String,
    },
    ToolResult {
        call_id: String,
        summary: String,
    },
    Approval {
        call_id: String,
        decision: ApprovalDecisionName,
        actor_id: String,
        reason: Option<String>,
    },
    PolicyDenied {
        call_id: String,
        reason: String,
    },
    ToolFailed {
        call_id: String,
        error: String,
    },
}

impl DesktopEntry {
    fn from_typed(entry: TypedTranscriptEntry, assistant_step: Option<u32>) -> Self {
        match entry {
            TypedTranscriptEntry::User { text } => Self::User { text },
            TypedTranscriptEntry::Assistant { text } => Self::Assistant {
                step: assistant_step.expect("assistant step assigned before conversion"),
                text,
            },
            TypedTranscriptEntry::ToolCall {
                call_id,
                tool,
                input,
            } => Self::ToolCall {
                call_id,
                tool,
                input_preview: json_preview(&input),
            },
            TypedTranscriptEntry::ToolResult { call_id, summary } => {
                Self::ToolResult { call_id, summary }
            }
            TypedTranscriptEntry::Approval {
                call_id,
                decision,
                actor_id,
                reason,
            } => Self::Approval {
                call_id,
                decision,
                actor_id,
                reason,
            },
            TypedTranscriptEntry::PolicyDenied { call_id, reason } => {
                Self::PolicyDenied { call_id, reason }
            }
            TypedTranscriptEntry::ToolFailed { call_id, error } => {
                Self::ToolFailed { call_id, error }
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct DesktopTranscript {
    runs: Vec<DesktopRun>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct DesktopPendingApproval {
    run_id: String,
    tool_call_id: String,
    tool_name: String,
    effect: String,
    reason: Option<String>,
    input_preview: Option<String>,
    approval_preview: Option<String>,
    diff_preview: Option<String>,
}

impl TryFrom<PendingApprovalSnapshot> for DesktopPendingApproval {
    type Error = DesktopError;

    fn try_from(snapshot: PendingApprovalSnapshot) -> Result<Self, Self::Error> {
        let effect = serde_json::to_value(snapshot.effect)
            .ok()
            .and_then(|value| value.as_str().map(ToOwned::to_owned))
            .ok_or_else(|| {
                DesktopError::new(
                    "incompatible_daemon",
                    "pending approval effect is not a wire string",
                )
            })?;
        Ok(Self {
            run_id: snapshot.run_id,
            tool_call_id: snapshot.tool_call_id,
            tool_name: snapshot.tool_name,
            effect,
            reason: snapshot.reason,
            input_preview: snapshot.input_preview,
            approval_preview: snapshot.approval_preview,
            diff_preview: snapshot.diff_preview,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct DesktopSubmission {
    run_id: String,
    session_id: String,
    status: RunStateName,
}

impl From<RunStartResult> for DesktopSubmission {
    fn from(result: RunStartResult) -> Self {
        Self {
            run_id: result.run_id,
            session_id: result.session_id,
            status: result.status,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct DesktopCommandStatus {
    run_id: String,
    status: RunStateName,
}

impl From<CommandAcceptedResult> for DesktopCommandStatus {
    fn from(result: CommandAcceptedResult) -> Self {
        Self {
            run_id: result.run_id,
            status: result.status,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DesktopApprovalDecision {
    Grant,
    Deny,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct DesktopEventPage {
    run_id: String,
    from_offset: u64,
    next_offset: u64,
    status: RunStateName,
    events: Vec<DesktopEvent>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
enum DesktopEvent {
    AssistantDelta {
        offset: u64,
        step: u32,
        delta_index: u64,
        text: String,
    },
    AssistantCommitted {
        offset: u64,
        step: u32,
        text: String,
    },
    ToolCall {
        offset: u64,
        call_id: String,
        tool: String,
        input_preview: String,
    },
    ToolResult {
        offset: u64,
        call_id: String,
        summary: String,
    },
    Approval {
        offset: u64,
        call_id: String,
        decision: ApprovalDecisionName,
        actor_id: String,
        reason: Option<String>,
    },
    PolicyDenied {
        offset: u64,
        call_id: String,
        reason: String,
    },
    ToolFailed {
        offset: u64,
        call_id: String,
        error: String,
    },
    ApprovalRequested {
        offset: u64,
        tool_call_id: String,
    },
    CancelRequested {
        offset: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct DesktopRecovery {
    anchor_offset: u64,
    run: DesktopRun,
    pending_approval: Option<DesktopPendingApproval>,
    page: DesktopEventPage,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SavedWorkspace {
    workspace_root: String,
}

enum SavedWorkspaceState {
    Missing,
    Invalid(String),
    Ready(PathBuf),
}

impl DesktopLifecycle {
    fn prepare_workspace(&self, workspace_root: &Path) -> Result<PreparedWorkspace, DesktopError> {
        #[cfg(windows)]
        {
            let workspace_id = paths::workspace_id(workspace_root)
                .map_err(|error| DesktopError::daemon("Workspace is invalid", error))?;
            if self
                .workspace_instance
                .as_ref()
                .is_some_and(|instance| instance.workspace_id() == workspace_id)
            {
                return Ok(PreparedWorkspace {
                    workspace_root: workspace_root.to_path_buf(),
                    workspace_id,
                    instance: None,
                });
            }
            let instance = lifecycle::WorkspaceInstance::acquire(&workspace_id).map_err(
                |error| match error {
                    lifecycle::WorkspaceInstanceError::AlreadyOpen { .. } => DesktopError::new(
                        "desktop_already_open",
                        "This workspace is already open in another Plato Agent desktop window",
                    ),
                    lifecycle::WorkspaceInstanceError::Io(error) => DesktopError::new(
                        "desktop_single_instance_failed",
                        format!("Unable to secure this desktop workspace: {error}"),
                    ),
                },
            )?;
            Ok(PreparedWorkspace {
                workspace_root: workspace_root.to_path_buf(),
                workspace_id,
                instance: Some(instance),
            })
        }
        #[cfg(not(windows))]
        {
            Ok(PreparedWorkspace {
                workspace_root: workspace_root.to_path_buf(),
            })
        }
    }

    fn commit_workspace(&mut self, prepared: PreparedWorkspace) {
        #[cfg(any(windows, unix))]
        if self.workspace_root.as_ref() != Some(&prepared.workspace_root) {
            #[cfg(unix)]
            reap_detached_daemon(self);
            #[cfg(windows)]
            {
                self.spawned_daemon = None;
            }
        }
        #[cfg(windows)]
        {
            if let Some(instance) = prepared.instance {
                debug_assert_eq!(instance.workspace_id(), prepared.workspace_id);
                self.workspace_instance = Some(instance);
            }
        }
        self.workspace_root = Some(prepared.workspace_root);
    }
}

fn lock_lifecycle(
    lifecycle: &Mutex<DesktopLifecycle>,
) -> Result<MutexGuard<'_, DesktopLifecycle>, DesktopError> {
    lifecycle.lock().map_err(|_| {
        DesktopError::new(
            "desktop_lifecycle_failed",
            "Desktop lifecycle state is unavailable",
        )
    })
}

fn selected_workspace(lifecycle: &Mutex<DesktopLifecycle>) -> Result<PathBuf, DesktopError> {
    lock_lifecycle(lifecycle)?
        .workspace_root
        .clone()
        .ok_or_else(|| {
            DesktopError::new("workspace_not_selected", "No valid workspace is selected")
        })
}

#[tauri::command]
async fn bootstrap(state: tauri::State<'_, DesktopState>) -> Result<BootstrapView, DesktopError> {
    let workspace_file = state.workspace_file.clone();
    let lifecycle = state.lifecycle.clone();
    let launch = state.launch.clone();
    tauri::async_runtime::spawn_blocking(move || {
        bootstrap_with_lifecycle(&workspace_file, &lifecycle, &launch, None)
    })
    .await
    .map_err(worker_error)?
}

#[tauri::command]
async fn pick_workspace(
    app: tauri::AppHandle,
    state: tauri::State<'_, DesktopState>,
) -> Result<Option<BootstrapView>, DesktopError> {
    let selected =
        tauri::async_runtime::spawn_blocking(move || app.dialog().file().blocking_pick_folder())
            .await
            .map_err(worker_error)?;
    let Some(selected) = selected else {
        return Ok(None);
    };
    let selected = selected.into_path().map_err(|error| {
        DesktopError::new(
            "invalid_workspace",
            format!("Workspace picker returned an invalid path: {error}"),
        )
    })?;
    let workspace_file = state.workspace_file.clone();
    let lifecycle = state.lifecycle.clone();
    let launch = state.launch.clone();
    tauri::async_runtime::spawn_blocking(move || {
        let selected = canonical_workspace(&selected)?;
        let mut lifecycle = lock_lifecycle(&lifecycle)?;
        let prepared = lifecycle.prepare_workspace(&selected)?;
        persist_canonical_workspace(&workspace_file, &selected)?;
        lifecycle.commit_workspace(prepared);
        attach_or_spawn_workspace(&selected, None, &mut lifecycle, &launch).map(Some)
    })
    .await
    .map_err(worker_error)?
}

#[tauri::command]
async fn read_run(
    run_id: String,
    state: tauri::State<'_, DesktopState>,
) -> Result<DesktopRun, DesktopError> {
    let workspace_root = selected_workspace(&state.lifecycle)?;
    tauri::async_runtime::spawn_blocking(move || {
        read_run_from_workspace(&workspace_root, &run_id, None)
    })
    .await
    .map_err(worker_error)?
}

#[tauri::command]
async fn list_sessions(
    state: tauri::State<'_, DesktopState>,
) -> Result<Vec<DesktopSession>, DesktopError> {
    let workspace_root = selected_workspace(&state.lifecycle)?;
    tauri::async_runtime::spawn_blocking(move || {
        with_workspace_client(&workspace_root, None, |client| {
            client
                .sessions_list()
                .map(|sessions| sessions.into_iter().map(DesktopSession::from).collect())
                .map_err(|error| DesktopError::daemon("Unable to list daemon sessions", error))
        })
    })
    .await
    .map_err(worker_error)?
}

#[tauri::command]
async fn read_session(
    session_id: String,
    state: tauri::State<'_, DesktopState>,
) -> Result<DesktopTranscript, DesktopError> {
    let workspace_root = selected_workspace(&state.lifecycle)?;
    tauri::async_runtime::spawn_blocking(move || {
        read_session_from_workspace(&workspace_root, &session_id, None)
    })
    .await
    .map_err(worker_error)?
}

#[tauri::command]
async fn submit_message(
    message: String,
    session_id: Option<String>,
    state: tauri::State<'_, DesktopState>,
) -> Result<DesktopSubmission, DesktopError> {
    let workspace_root = selected_workspace(&state.lifecycle)?;
    tauri::async_runtime::spawn_blocking(move || {
        submit_message_from_workspace(&workspace_root, message, session_id, None)
    })
    .await
    .map_err(worker_error)?
}

#[tauri::command]
async fn poll_run(
    run_id: String,
    from_offset: u64,
    state: tauri::State<'_, DesktopState>,
) -> Result<DesktopEventPage, DesktopError> {
    let workspace_root = selected_workspace(&state.lifecycle)?;
    tauri::async_runtime::spawn_blocking(move || {
        poll_run_from_workspace(&workspace_root, &run_id, from_offset, None)
    })
    .await
    .map_err(worker_error)?
}

#[tauri::command]
async fn recover_run(
    run_id: String,
    state: tauri::State<'_, DesktopState>,
) -> Result<DesktopRecovery, DesktopError> {
    let workspace_root = selected_workspace(&state.lifecycle)?;
    tauri::async_runtime::spawn_blocking(move || {
        recover_run_from_workspace(&workspace_root, &run_id, None)
    })
    .await
    .map_err(worker_error)?
}

#[tauri::command]
async fn decide_approval(
    run_id: String,
    tool_call_id: String,
    decision: DesktopApprovalDecision,
    reason: Option<String>,
    state: tauri::State<'_, DesktopState>,
) -> Result<DesktopCommandStatus, DesktopError> {
    let workspace_root = selected_workspace(&state.lifecycle)?;
    tauri::async_runtime::spawn_blocking(move || {
        decide_approval_from_workspace(
            &workspace_root,
            &run_id,
            &tool_call_id,
            decision,
            reason,
            None,
        )
    })
    .await
    .map_err(worker_error)?
}

#[tauri::command]
async fn cancel_run(
    run_id: String,
    state: tauri::State<'_, DesktopState>,
) -> Result<DesktopCommandStatus, DesktopError> {
    let workspace_root = selected_workspace(&state.lifecycle)?;
    tauri::async_runtime::spawn_blocking(move || {
        cancel_run_from_workspace(&workspace_root, &run_id, None)
    })
    .await
    .map_err(worker_error)?
}

fn worker_error(error: impl std::fmt::Display) -> DesktopError {
    DesktopError::new("desktop_worker", format!("Desktop worker failed: {error}"))
}

#[cfg(all(test, unix))]
fn bootstrap_from_store(
    workspace_file: &Path,
    socket_path: Option<PathBuf>,
) -> Result<BootstrapView, DesktopError> {
    match load_saved_workspace(workspace_file) {
        SavedWorkspaceState::Missing => Ok(BootstrapView::NeedsWorkspace { reason: None }),
        SavedWorkspaceState::Invalid(reason) => Ok(BootstrapView::NeedsWorkspace {
            reason: Some(reason),
        }),
        SavedWorkspaceState::Ready(workspace_root) => {
            connect_workspace(&workspace_root, socket_path)
        }
    }
}

fn bootstrap_with_lifecycle(
    workspace_file: &Path,
    lifecycle: &Mutex<DesktopLifecycle>,
    launch: &DaemonLaunch,
    socket_path: Option<PathBuf>,
) -> Result<BootstrapView, DesktopError> {
    let mut lifecycle = lock_lifecycle(lifecycle)?;
    let workspace_root = match lifecycle.workspace_root.clone() {
        Some(workspace_root) => workspace_root,
        None => match load_saved_workspace(workspace_file) {
            SavedWorkspaceState::Missing => {
                return Ok(BootstrapView::NeedsWorkspace { reason: None });
            }
            SavedWorkspaceState::Invalid(reason) => {
                return Ok(BootstrapView::NeedsWorkspace {
                    reason: Some(reason),
                });
            }
            SavedWorkspaceState::Ready(workspace_root) => workspace_root,
        },
    };
    let prepared = lifecycle.prepare_workspace(&workspace_root)?;
    lifecycle.commit_workspace(prepared);
    attach_or_spawn_workspace(&workspace_root, socket_path, &mut lifecycle, launch)
}

#[cfg(all(test, unix))]
fn connect_workspace(
    workspace_root: &Path,
    socket_path: Option<PathBuf>,
) -> Result<BootstrapView, DesktopError> {
    let config = DaemonConnectionConfig::resolve(workspace_root, socket_path)
        .map_err(|error| DesktopError::daemon("Workspace is invalid", error))?;
    try_attach_workspace_until(&config, std::time::Instant::now() + DAEMON_ATTACH_TIMEOUT)
}

fn finish_attach_workspace(
    config: &DaemonConnectionConfig,
    mut client: DaemonClient,
    deadline: Option<std::time::Instant>,
) -> Result<BootstrapView, DesktopError> {
    #[cfg(windows)]
    let _ = deadline;
    #[cfg(unix)]
    refresh_attach_timeout(config, &mut client, deadline)?;
    let hello = client
        .hello(&config.workspace_root)
        .map_err(|error| DesktopError::daemon("Daemon hello failed", error))?;
    validate_hello(&config.workspace_root, &hello)?;
    let daemon_version = hello.daemon_version;
    #[cfg(unix)]
    refresh_attach_timeout(config, &mut client, deadline)?;
    let session_summaries = client
        .sessions_list()
        .map_err(|error| DesktopError::daemon("Unable to list daemon sessions", error))?;
    #[cfg(unix)]
    refresh_attach_timeout(config, &mut client, deadline)?;
    let selected_run = session_summaries
        .first()
        .map(|session| read_typed_run(&mut client, &session.run_id))
        .transpose()?;
    Ok(BootstrapView::Ready {
        workspace_root: config.workspace_root.to_string_lossy().into_owned(),
        daemon_version,
        sessions: session_summaries
            .into_iter()
            .map(DesktopSession::from)
            .collect(),
        selected_run,
    })
}

#[cfg(windows)]
fn try_attach_workspace_until(
    config: &DaemonConnectionConfig,
    deadline: std::time::Instant,
) -> Result<BootstrapView, DesktopError> {
    let client = connect_client(config)?;
    let worker_config = config.clone();
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    let worker = std::thread::spawn(move || {
        let _ = sender.send(finish_attach_workspace(&worker_config, client, None));
    });
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    match receiver.recv_timeout(remaining) {
        Ok(result) => {
            worker.join().map_err(|_| attach_worker_error())?;
            result
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            worker.join().map_err(|_| attach_worker_error())?;
            Err(attach_worker_error())
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            let _ = lifecycle::cancel_synchronous_io(&worker);
            match receiver.recv_timeout(DAEMON_ATTACH_CANCEL_GRACE) {
                Ok(result) => {
                    worker.join().map_err(|_| attach_worker_error())?;
                    return result;
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    worker.join().map_err(|_| attach_worker_error())?;
                    return Err(attach_worker_error());
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            }
            Err(DesktopError::new(
                "daemon_unavailable",
                format!(
                    "Daemon attach timed out at {}",
                    config.socket_path.display()
                ),
            ))
        }
    }
}

#[cfg(unix)]
fn try_attach_workspace_until(
    config: &DaemonConnectionConfig,
    deadline: std::time::Instant,
) -> Result<BootstrapView, DesktopError> {
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    if remaining.is_zero() {
        return Err(DesktopError::new(
            "daemon_unavailable",
            format!(
                "Daemon attach timed out at {}",
                config.socket_path.display()
            ),
        ));
    }
    let client = DaemonClient::connect_with_timeout(&config.socket_path, remaining)
        .map_err(|error| DesktopError::daemon("Unable to connect to plato-agentd", error))?;
    finish_attach_workspace(config, client, Some(deadline))
}

#[cfg(unix)]
fn refresh_attach_timeout(
    config: &DaemonConnectionConfig,
    client: &mut DaemonClient,
    deadline: Option<std::time::Instant>,
) -> Result<(), DesktopError> {
    let deadline = deadline.expect("Unix attach always has a deadline");
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    if remaining.is_zero() {
        return Err(DesktopError::new(
            "daemon_unavailable",
            format!(
                "Daemon attach timed out at {}",
                config.socket_path.display()
            ),
        ));
    }
    client
        .set_timeout(remaining)
        .map_err(|error| DesktopError::daemon("Unable to bound daemon attach", error))
}

#[cfg(windows)]
fn attach_worker_error() -> DesktopError {
    DesktopError::new("desktop_worker", "Daemon attach worker failed")
}

fn attach_or_spawn_workspace(
    workspace_root: &Path,
    socket_path: Option<PathBuf>,
    lifecycle: &mut DesktopLifecycle,
    launch: &DaemonLaunch,
) -> Result<BootstrapView, DesktopError> {
    let config = DaemonConnectionConfig::resolve(workspace_root, socket_path)
        .map_err(|error| DesktopError::daemon("Workspace is invalid", error))?;
    let initial =
        try_attach_workspace_until(&config, std::time::Instant::now() + DAEMON_ATTACH_TIMEOUT);
    match initial {
        Ok(view) => {
            #[cfg(unix)]
            reap_detached_daemon(lifecycle);
            Ok(view)
        }
        Err(error) if error.code == "daemon_unavailable" => {
            #[cfg(any(windows, unix))]
            {
                start_and_attach_workspace(&config, lifecycle, launch, error)
            }
            #[cfg(not(any(windows, unix)))]
            {
                let _ = (lifecycle, launch);
                Err(error)
            }
        }
        Err(error) => Err(error),
    }
}

#[cfg(any(windows, unix))]
fn start_and_attach_workspace(
    config: &DaemonConnectionConfig,
    lifecycle: &mut DesktopLifecycle,
    launch: &DaemonLaunch,
    initial_error: DesktopError,
) -> Result<BootstrapView, DesktopError> {
    let workspace_id = paths::workspace_id(&config.workspace_root)
        .map_err(|error| DesktopError::daemon("Workspace is invalid", error))?;
    let mut last_error = initial_error.message;
    let mut child_status = None;
    let mut should_spawn = true;
    if let Some(spawned) = lifecycle.spawned_daemon.as_mut()
        && spawned.workspace_id == workspace_id
    {
        match spawned.child.try_wait() {
            Ok(None) => should_spawn = false,
            Ok(Some(status)) => child_status = Some(format!("previous child exited with {status}")),
            Err(error) => {
                #[cfg(unix)]
                reap_detached_daemon(lifecycle);
                return Err(daemon_start_error(
                    config,
                    format!("unable to inspect the previous daemon child: {error}"),
                ));
            }
        }
    }
    if should_spawn {
        #[cfg(unix)]
        reap_detached_daemon(lifecycle);
        #[cfg(windows)]
        {
            lifecycle.spawned_daemon = None;
        }
        let executable = launch.executable.as_deref().ok_or_else(|| {
            daemon_start_error(config, "the packaged daemon sidecar path is unavailable")
        })?;
        #[cfg(windows)]
        let child = lifecycle::spawn_detached_daemon(
            executable,
            &config.workspace_root,
            Some(&config.socket_path),
        );
        #[cfg(unix)]
        let child = {
            let user_path = unix_lifecycle::user_launch_path().map_err(|error| {
                daemon_start_error(
                    config,
                    format!(
                        "unable to establish the user launch PATH before starting {}: {error}",
                        executable.display()
                    ),
                )
            })?;
            unix_lifecycle::spawn_detached_daemon(
                executable,
                &config.workspace_root,
                Some(&config.socket_path),
                &user_path,
            )
        };
        let child = child.map_err(|error| {
            daemon_start_error(
                config,
                format!("unable to start {}: {error}", executable.display()),
            )
        })?;
        lifecycle.spawned_daemon = Some(SpawnedDaemon {
            workspace_id: workspace_id.clone(),
            child,
        });
    }

    let deadline = std::time::Instant::now() + DAEMON_ATTACH_TIMEOUT;
    loop {
        if std::time::Instant::now() >= deadline {
            let detail = match child_status {
                Some(status) => format!("{last_error}; {status}"),
                None => last_error,
            };
            #[cfg(unix)]
            reap_detached_daemon(lifecycle);
            return Err(daemon_start_error(config, detail));
        }
        match try_attach_workspace_until(config, deadline) {
            Ok(view) => {
                #[cfg(unix)]
                reap_detached_daemon(lifecycle);
                return Ok(view);
            }
            Err(error) => {
                if error.code != "daemon_unavailable" {
                    #[cfg(unix)]
                    reap_detached_daemon(lifecycle);
                    return Err(error);
                }
                last_error = error.message;
            }
        }

        if let Some(spawned) = lifecycle.spawned_daemon.as_mut()
            && spawned.workspace_id == workspace_id
        {
            match spawned.child.try_wait() {
                Ok(Some(status)) => {
                    child_status = Some(format!("daemon child exited with {status}"));
                    lifecycle.spawned_daemon = None;
                }
                Ok(None) => {}
                Err(error) => {
                    child_status = Some(format!("unable to inspect daemon child: {error}"));
                    #[cfg(unix)]
                    reap_detached_daemon(lifecycle);
                    #[cfg(windows)]
                    {
                        lifecycle.spawned_daemon = None;
                    }
                }
            }
        }
        std::thread::sleep(DAEMON_ATTACH_RETRY);
    }
}

#[cfg(unix)]
fn reap_detached_daemon(lifecycle: &mut DesktopLifecycle) {
    let Some(spawned) = lifecycle.spawned_daemon.take() else {
        return;
    };
    let mut child = spawned.child;
    std::thread::spawn(move || {
        let _ = child.wait();
    });
}

#[cfg(any(windows, unix))]
fn daemon_start_error(
    config: &DaemonConnectionConfig,
    detail: impl std::fmt::Display,
) -> DesktopError {
    let lock = paths::default_lock_path(&config.workspace_root)
        .map(|path| path.display().to_string())
        .unwrap_or_else(|error| format!("<unresolved: {error}>"));
    DesktopError::new(
        "daemon_start_failed",
        format!(
            "Unable to start plato-agentd: {detail}. Endpoint: {}. Lock: {lock}",
            config.socket_path.display()
        ),
    )
}

fn read_run_from_workspace(
    workspace_root: &Path,
    run_id: &str,
    socket_path: Option<PathBuf>,
) -> Result<DesktopRun, DesktopError> {
    let config = DaemonConnectionConfig::resolve(workspace_root, socket_path)
        .map_err(|error| DesktopError::daemon("Workspace is invalid", error))?;
    let mut client = connect_client(&config)?;
    let hello = client
        .hello(&config.workspace_root)
        .map_err(|error| DesktopError::daemon("Daemon hello failed", error))?;
    validate_hello(&config.workspace_root, &hello)?;
    read_typed_run(&mut client, run_id)
}

fn validate_hello(workspace_root: &Path, hello: &HelloResult) -> Result<(), DesktopError> {
    let expected_workspace_id = paths::workspace_id(workspace_root)
        .map_err(|error| DesktopError::daemon("Workspace is invalid", error))?;
    if hello.workspace_id != expected_workspace_id {
        return Err(DesktopError::new(
            "incompatible_daemon",
            format!(
                "Incompatible daemon: expected workspace {expected_workspace_id}, got {}",
                hello.workspace_id
            ),
        ));
    }
    require_capabilities(&hello.capabilities)
}

fn require_capabilities(capabilities: &[String]) -> Result<(), DesktopError> {
    if let Some(missing) = REQUIRED_CAPABILITIES.iter().find(|required| {
        !capabilities
            .iter()
            .any(|capability| capability == **required)
    }) {
        return Err(DesktopError::new(
            "incompatible_daemon",
            format!("Incompatible daemon: missing required capability {missing}"),
        ));
    }
    Ok(())
}

fn read_typed_run(client: &mut DaemonClient, run_id: &str) -> Result<DesktopRun, DesktopError> {
    let transcript = client
        .transcript_read(run_id)
        .map_err(|error| DesktopError::daemon(&format!("Unable to read run {run_id}"), error))?;
    extract_typed_run(run_id, transcript)
}

fn extract_typed_run(
    expected_run_id: &str,
    transcript: TranscriptReadResult,
) -> Result<DesktopRun, DesktopError> {
    if transcript.run_id != expected_run_id {
        return Err(DesktopError::new(
            "incompatible_daemon",
            format!(
                "Incompatible daemon: requested run {expected_run_id}, got {}",
                transcript.run_id
            ),
        ));
    }
    let typed = transcript.typed.ok_or_else(|| {
        DesktopError::new(
            "incompatible_daemon",
            "Incompatible daemon: transcript.read returned no typed payload",
        )
    })?;
    if typed.runs.len() != 1 {
        return Err(DesktopError::new(
            "incompatible_daemon",
            format!(
                "Incompatible daemon: exact-run transcript returned {} runs",
                typed.runs.len()
            ),
        ));
    }
    let run = typed.runs.into_iter().next().expect("length checked");
    if run.run_id != expected_run_id {
        return Err(DesktopError::new(
            "incompatible_daemon",
            format!(
                "Incompatible daemon: requested run {expected_run_id}, got {}",
                run.run_id
            ),
        ));
    }
    DesktopRun::try_from(run)
}

fn connect_client(config: &DaemonConnectionConfig) -> Result<DaemonClient, DesktopError> {
    DaemonClient::connect_with_timeout(&config.socket_path, DAEMON_ATTACH_TIMEOUT)
        .map_err(|error| DesktopError::daemon("Unable to connect to plato-agentd", error))
}

fn with_workspace_client<T>(
    workspace_root: &Path,
    socket_path: Option<PathBuf>,
    run: impl FnOnce(&mut DaemonClient) -> Result<T, DesktopError>,
) -> Result<T, DesktopError> {
    let config = DaemonConnectionConfig::resolve(workspace_root, socket_path)
        .map_err(|error| DesktopError::daemon("Workspace is invalid", error))?;
    let mut client = connect_client(&config)?;
    let hello = client
        .hello(&config.workspace_root)
        .map_err(|error| DesktopError::daemon("Daemon hello failed", error))?;
    validate_hello(&config.workspace_root, &hello)?;
    run(&mut client)
}

fn read_session_from_workspace(
    workspace_root: &Path,
    session_id: &str,
    socket_path: Option<PathBuf>,
) -> Result<DesktopTranscript, DesktopError> {
    with_workspace_client(workspace_root, socket_path, |client| {
        let transcript = client
            .transcript_read_session(session_id)
            .map_err(|error| {
                DesktopError::daemon(&format!("Unable to read session {session_id}"), error)
            })?;
        let latest_run_id = transcript.run_id;
        let typed = transcript.typed.ok_or_else(|| {
            DesktopError::new(
                "incompatible_daemon",
                "Incompatible daemon: transcript.read returned no typed payload",
            )
        })?;
        if typed.runs.is_empty() {
            return Err(DesktopError::new(
                "incompatible_daemon",
                "Incompatible daemon: session transcript returned no runs",
            ));
        }
        let mut runs = typed
            .runs
            .into_iter()
            .map(DesktopRun::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        runs.sort_by_key(|run| run.session_index);
        if runs
            .windows(2)
            .any(|pair| pair[0].session_index == pair[1].session_index)
        {
            return Err(DesktopError::new(
                "incompatible_daemon",
                "Incompatible daemon: session transcript contains duplicate session indexes",
            ));
        }
        for (index, run) in runs.iter().enumerate() {
            if runs[..index].iter().any(|prior| prior.run_id == run.run_id) {
                return Err(DesktopError::new(
                    "incompatible_daemon",
                    format!(
                        "Incompatible daemon: session transcript repeats run {}",
                        run.run_id
                    ),
                ));
            }
        }
        if runs.last().map(|run| run.run_id.as_str()) != Some(latest_run_id.as_str()) {
            return Err(DesktopError::new(
                "incompatible_daemon",
                format!(
                    "Incompatible daemon: session transcript latest run is not {latest_run_id}"
                ),
            ));
        }
        Ok(DesktopTranscript { runs })
    })
}

fn submit_message_from_workspace(
    workspace_root: &Path,
    message: String,
    session_id: Option<String>,
    socket_path: Option<PathBuf>,
) -> Result<DesktopSubmission, DesktopError> {
    with_workspace_client(workspace_root, socket_path, |client| {
        let expected_session_id = session_id.clone();
        let result = match session_id {
            Some(session_id) => {
                client.message_append_to_session(message, Some(session_id), None, false)
            }
            None => client.run_start(message, None, false),
        }
        .map_err(|error| DesktopError::daemon("Unable to submit message", error))?;
        if let Some(expected_session_id) = expected_session_id
            && result.session_id != expected_session_id
        {
            return Err(DesktopError::new(
                "incompatible_daemon",
                format!(
                    "Incompatible daemon: appended session {expected_session_id}, got {}",
                    result.session_id
                ),
            ));
        }
        Ok(DesktopSubmission::from(result))
    })
}

fn decide_approval_from_workspace(
    workspace_root: &Path,
    run_id: &str,
    tool_call_id: &str,
    decision: DesktopApprovalDecision,
    reason: Option<String>,
    socket_path: Option<PathBuf>,
) -> Result<DesktopCommandStatus, DesktopError> {
    with_workspace_client(workspace_root, socket_path, |client| {
        let result = match decision {
            DesktopApprovalDecision::Grant => client.approval_grant(run_id, tool_call_id),
            DesktopApprovalDecision::Deny => client.approval_deny(
                run_id,
                tool_call_id,
                reason.unwrap_or_else(|| "approval denied by desktop client".into()),
            ),
        };
        let result =
            result.map_err(|error| DesktopError::daemon("Unable to decide approval", error))?;
        if result.run_id != run_id {
            return Err(DesktopError::new(
                "incompatible_daemon",
                format!(
                    "Incompatible daemon: decided approval for {run_id}, got {}",
                    result.run_id
                ),
            ));
        }
        Ok(DesktopCommandStatus::from(result))
    })
}

fn cancel_run_from_workspace(
    workspace_root: &Path,
    run_id: &str,
    socket_path: Option<PathBuf>,
) -> Result<DesktopCommandStatus, DesktopError> {
    with_workspace_client(workspace_root, socket_path, |client| {
        let result = client
            .run_cancel(run_id)
            .map_err(|error| DesktopError::daemon("Unable to cancel run", error))?;
        if result.run_id != run_id {
            return Err(DesktopError::new(
                "incompatible_daemon",
                format!(
                    "Incompatible daemon: canceled run {run_id}, got {}",
                    result.run_id
                ),
            ));
        }
        Ok(DesktopCommandStatus::from(result))
    })
}

fn poll_run_from_workspace(
    workspace_root: &Path,
    run_id: &str,
    from_offset: u64,
    socket_path: Option<PathBuf>,
) -> Result<DesktopEventPage, DesktopError> {
    with_workspace_client(workspace_root, socket_path, |client| {
        let page = client
            .events_stream(run_id, Some(from_offset), EVENT_PAGE_SIZE)
            .map_err(|error| DesktopError::daemon("Unable to poll run events", error))?;
        normalize_event_page(run_id, page)
    })
}

fn recover_run_from_workspace(
    workspace_root: &Path,
    run_id: &str,
    socket_path: Option<PathBuf>,
) -> Result<DesktopRecovery, DesktopError> {
    with_workspace_client(workspace_root, socket_path, |client| {
        let anchor = client
            .events_stream(run_id, None, EVENT_PAGE_SIZE)
            .map_err(|error| DesktopError::daemon("Unable to anchor run recovery", error))?;
        validate_stream_run(run_id, &anchor)?;
        let anchor_offset = anchor.next_offset;

        let transcript = client.transcript_read(run_id).map_err(|error| {
            DesktopError::daemon(&format!("Unable to recover run {run_id}"), error)
        })?;
        let pending_approval = transcript
            .pending_approval
            .clone()
            .map(DesktopPendingApproval::try_from)
            .transpose()?;
        if let Some(pending) = &pending_approval
            && pending.run_id != run_id
        {
            return Err(DesktopError::new(
                "incompatible_daemon",
                format!(
                    "Incompatible daemon: pending approval belongs to {}, expected {run_id}",
                    pending.run_id
                ),
            ));
        }
        let run = extract_typed_run(run_id, transcript)?;

        let page = client
            .events_stream(run_id, Some(anchor_offset), EVENT_PAGE_SIZE)
            .map_err(|error| DesktopError::daemon("Unable to continue run recovery", error))?;
        let page = normalize_event_page(run_id, page)?;
        Ok(DesktopRecovery {
            anchor_offset,
            run,
            pending_approval,
            page,
        })
    })
}

#[cfg(test)]
fn workspace_from_store(workspace_file: &Path) -> Result<PathBuf, DesktopError> {
    match load_saved_workspace(workspace_file) {
        SavedWorkspaceState::Ready(workspace_root) => Ok(workspace_root),
        SavedWorkspaceState::Missing | SavedWorkspaceState::Invalid(_) => Err(DesktopError::new(
            "workspace_not_selected",
            "No valid workspace is selected",
        )),
    }
}

#[cfg(all(test, windows))]
fn with_saved_client<T>(
    workspace_file: &Path,
    socket_path: Option<PathBuf>,
    run: impl FnOnce(&mut DaemonClient) -> Result<T, DesktopError>,
) -> Result<T, DesktopError> {
    let workspace_root = workspace_from_store(workspace_file)?;
    with_workspace_client(&workspace_root, socket_path, run)
}

#[cfg(all(test, unix))]
fn read_session_from_store(
    workspace_file: &Path,
    session_id: &str,
    socket_path: Option<PathBuf>,
) -> Result<DesktopTranscript, DesktopError> {
    let workspace_root = workspace_from_store(workspace_file)?;
    read_session_from_workspace(&workspace_root, session_id, socket_path)
}

#[cfg(all(test, unix))]
fn submit_message_from_store(
    workspace_file: &Path,
    message: String,
    session_id: Option<String>,
    socket_path: Option<PathBuf>,
) -> Result<DesktopSubmission, DesktopError> {
    let workspace_root = workspace_from_store(workspace_file)?;
    submit_message_from_workspace(&workspace_root, message, session_id, socket_path)
}

#[cfg(all(test, unix))]
fn decide_approval_from_store(
    workspace_file: &Path,
    run_id: &str,
    tool_call_id: &str,
    decision: DesktopApprovalDecision,
    reason: Option<String>,
    socket_path: Option<PathBuf>,
) -> Result<DesktopCommandStatus, DesktopError> {
    let workspace_root = workspace_from_store(workspace_file)?;
    decide_approval_from_workspace(
        &workspace_root,
        run_id,
        tool_call_id,
        decision,
        reason,
        socket_path,
    )
}

#[cfg(all(test, unix))]
fn cancel_run_from_store(
    workspace_file: &Path,
    run_id: &str,
    socket_path: Option<PathBuf>,
) -> Result<DesktopCommandStatus, DesktopError> {
    let workspace_root = workspace_from_store(workspace_file)?;
    cancel_run_from_workspace(&workspace_root, run_id, socket_path)
}

#[cfg(all(test, unix))]
fn poll_run_from_store(
    workspace_file: &Path,
    run_id: &str,
    from_offset: u64,
    socket_path: Option<PathBuf>,
) -> Result<DesktopEventPage, DesktopError> {
    let workspace_root = workspace_from_store(workspace_file)?;
    poll_run_from_workspace(&workspace_root, run_id, from_offset, socket_path)
}

#[cfg(all(test, unix))]
fn recover_run_from_store(
    workspace_file: &Path,
    run_id: &str,
    socket_path: Option<PathBuf>,
) -> Result<DesktopRecovery, DesktopError> {
    let workspace_root = workspace_from_store(workspace_file)?;
    recover_run_from_workspace(&workspace_root, run_id, socket_path)
}

fn normalize_event_page(
    expected_run_id: &str,
    page: EventsStreamResult,
) -> Result<DesktopEventPage, DesktopError> {
    validate_stream_run(expected_run_id, &page)?;
    if page.next_offset < page.from_offset {
        return Err(DesktopError::new(
            "incompatible_daemon",
            "Incompatible daemon: events.stream next_offset precedes from_offset",
        ));
    }
    let event_count = u64::try_from(page.events.len()).map_err(|_| {
        DesktopError::new(
            "incompatible_daemon",
            "Incompatible daemon: events.stream page is too large",
        )
    })?;
    if page.from_offset.checked_add(event_count) != Some(page.next_offset) {
        return Err(DesktopError::new(
            "incompatible_daemon",
            "Incompatible daemon: events.stream offsets do not match its page length",
        ));
    }
    let mut events = Vec::new();
    for (index, buffered) in page.events.into_iter().enumerate() {
        let expected_offset = page.from_offset + index as u64;
        if buffered.offset != expected_offset {
            return Err(DesktopError::new(
                "incompatible_daemon",
                format!(
                    "Incompatible daemon: event offset {} is not expected offset {expected_offset}",
                    buffered.offset
                ),
            ));
        }
        if let Some(event) = buffered_event_into_desktop(buffered) {
            events.push(event);
        }
    }
    Ok(DesktopEventPage {
        run_id: page.run_id,
        from_offset: page.from_offset,
        next_offset: page.next_offset,
        status: page.status,
        events,
    })
}

fn validate_stream_run(
    expected_run_id: &str,
    page: &EventsStreamResult,
) -> Result<(), DesktopError> {
    if page.run_id == expected_run_id {
        return Ok(());
    }
    Err(DesktopError::new(
        "incompatible_daemon",
        format!(
            "Incompatible daemon: requested events for {expected_run_id}, got {}",
            page.run_id
        ),
    ))
}

fn buffered_event_into_desktop(buffered: BufferedStreamEvent) -> Option<DesktopEvent> {
    let offset = buffered.offset;
    match buffered.event {
        StreamEvent::AssistantDelta {
            step,
            delta_index,
            text,
            ..
        } => Some(DesktopEvent::AssistantDelta {
            offset,
            step,
            delta_index,
            text,
        }),
        StreamEvent::ApprovalRequested { tool_call_id, .. } => {
            Some(DesktopEvent::ApprovalRequested {
                offset,
                tool_call_id,
            })
        }
        StreamEvent::Canceled { .. } => Some(DesktopEvent::CancelRequested { offset }),
        StreamEvent::Ledger { record } => ledger_event_into_desktop(record.event, offset),
        StreamEvent::Unknown(_) => None,
    }
}

fn ledger_event_into_desktop(event: HarnessEvent, offset: u64) -> Option<DesktopEvent> {
    match event {
        HarnessEvent::ModelResponded { step, output, .. } => {
            Some(DesktopEvent::AssistantCommitted {
                offset,
                step,
                text: output.content,
            })
        }
        HarnessEvent::ToolCallProposed { call, .. } => Some(DesktopEvent::ToolCall {
            offset,
            call_id: call.id.to_string(),
            tool: call.tool.to_string(),
            input_preview: json_preview(&call.input),
        }),
        HarnessEvent::PolicyEvaluated {
            call_id,
            decision: PolicyDecision::Deny { reason },
            ..
        } => Some(DesktopEvent::PolicyDenied {
            offset,
            call_id: call_id.to_string(),
            reason,
        }),
        HarnessEvent::PolicyEvaluated {
            decision: PolicyDecision::Allow | PolicyDecision::RequireApproval { .. },
            ..
        } => None,
        HarnessEvent::ApprovalGranted {
            call_id, actor_id, ..
        } => Some(DesktopEvent::Approval {
            offset,
            call_id: call_id.to_string(),
            decision: ApprovalDecisionName::Granted,
            actor_id: actor_id.to_string(),
            reason: None,
        }),
        HarnessEvent::ApprovalDenied {
            call_id,
            actor_id,
            reason,
            ..
        } => Some(DesktopEvent::Approval {
            offset,
            call_id: call_id.to_string(),
            decision: ApprovalDecisionName::Denied,
            actor_id: actor_id.to_string(),
            reason: Some(reason),
        }),
        HarnessEvent::ToolFinished { result, .. } => Some(DesktopEvent::ToolResult {
            offset,
            call_id: result.call_id.to_string(),
            summary: result.summary,
        }),
        HarnessEvent::ToolFailed {
            call_id, reason, ..
        } => Some(DesktopEvent::ToolFailed {
            offset,
            call_id: call_id.to_string(),
            error: reason,
        }),
        HarnessEvent::RunStarted { .. }
        | HarnessEvent::ContextBuilt { .. }
        | HarnessEvent::ContextCompacted { .. }
        | HarnessEvent::ModelRequested { .. }
        | HarnessEvent::ModelFailed { .. }
        | HarnessEvent::ToolProposalsRejected { .. }
        | HarnessEvent::ToolStarted { .. }
        | HarnessEvent::RunFinished { .. }
        | HarnessEvent::RunFailed { .. } => None,
    }
}

fn json_preview(value: &Value) -> String {
    let encoded = serde_json::to_string(value).expect("JSON value serializes");
    if encoded.chars().count() <= INPUT_PREVIEW_MAX_CHARS {
        return encoded;
    }
    format!(
        "{}...",
        encoded
            .chars()
            .take(INPUT_PREVIEW_MAX_CHARS)
            .collect::<String>()
    )
}

fn load_saved_workspace(workspace_file: &Path) -> SavedWorkspaceState {
    let bytes = match fs::read(workspace_file) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => return SavedWorkspaceState::Missing,
        Err(error) => {
            return SavedWorkspaceState::Invalid(format!(
                "Saved workspace could not be read: {error}"
            ));
        }
    };
    let saved = match serde_json::from_slice::<SavedWorkspace>(&bytes) {
        Ok(saved) => saved,
        Err(_) => {
            return SavedWorkspaceState::Invalid("Saved workspace is invalid".into());
        }
    };
    let path = PathBuf::from(saved.workspace_root);
    match path.canonicalize() {
        Ok(path) if path.is_dir() => SavedWorkspaceState::Ready(path),
        _ => SavedWorkspaceState::Invalid("Saved workspace no longer exists".into()),
    }
}

fn canonical_workspace(workspace_root: &Path) -> Result<PathBuf, DesktopError> {
    let canonical = workspace_root.canonicalize().map_err(|error| {
        DesktopError::new(
            "invalid_workspace",
            format!("Workspace cannot be resolved: {error}"),
        )
    })?;
    if !canonical.is_dir() {
        return Err(DesktopError::new(
            "invalid_workspace",
            "Selected workspace is not a directory",
        ));
    }
    Ok(canonical)
}

fn persist_canonical_workspace(
    workspace_file: &Path,
    canonical: &Path,
) -> Result<(), DesktopError> {
    let workspace_root = canonical.to_str().ok_or_else(|| {
        DesktopError::new("invalid_workspace", "Workspace path must be valid UTF-8")
    })?;
    if let Some(parent) = workspace_file.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            DesktopError::new(
                "workspace_save_failed",
                format!("Workspace selection could not be saved: {error}"),
            )
        })?;
    }
    let temporary = workspace_file.with_extension(format!("json.{}.tmp", std::process::id()));
    let bytes = serde_json::to_vec(&SavedWorkspace {
        workspace_root: workspace_root.into(),
    })
    .map_err(|error| {
        DesktopError::new(
            "workspace_save_failed",
            format!("Workspace selection could not be encoded: {error}"),
        )
    })?;
    fs::write(&temporary, bytes).map_err(|error| {
        DesktopError::new(
            "workspace_save_failed",
            format!("Workspace selection could not be saved: {error}"),
        )
    })?;
    replace_workspace_file(&temporary, workspace_file).map_err(|error| {
        DesktopError::new(
            "workspace_save_failed",
            format!("Workspace selection could not be saved: {error}"),
        )
    })?;
    Ok(())
}

fn replace_workspace_file(from: &Path, to: &Path) -> std::io::Result<()> {
    #[cfg(windows)]
    {
        lifecycle::replace_file(from, to)
    }
    #[cfg(not(windows))]
    {
        fs::rename(from, to)
    }
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    #[cfg(windows)]
    drop(
        plato_daemon_client::installer_gate::InstallerStartupGate::acquire()
            .expect("Plato Agent installation or update is in progress"),
    );
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .setup(|app| {
            let workspace_file = app.path().app_data_dir()?.join("workspace.json");
            app.manage(DesktopState {
                workspace_file,
                lifecycle: Arc::new(Mutex::new(DesktopLifecycle::default())),
                launch: DaemonLaunch::installed()?,
            });
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            bootstrap,
            pick_workspace,
            read_run,
            list_sessions,
            read_session,
            submit_message,
            poll_run,
            recover_run,
            decide_approval,
            cancel_run
        ])
        .run(tauri::generate_context!())
        .expect("error while running Plato Agent desktop");
}

#[cfg(all(test, any(unix, windows)))]
mod daemon_deadline_tests {
    use super::*;
    use plato_protocol::{Envelope, EnvelopeKind, PROTOCOL_VERSION};
    use serde_json::json;
    use std::{
        io::{BufRead, BufReader, Write},
        path::{Path, PathBuf},
        sync::mpsc,
        thread,
        time::{Duration, Instant},
    };

    const DEADLINE_EARLY_TOLERANCE: Duration = Duration::from_millis(250);
    const DEADLINE_LATE_TOLERANCE: Duration = Duration::from_secs(1);
    const NEAR_DEADLINE_DELAY: Duration = Duration::from_millis(2_500);
    const OUTER_WATCHDOG: Duration = Duration::from_secs(8);

    #[test]
    fn normal_desktop_hello_byte_drip_cannot_extend_the_deadline() {
        let fixture = DeadlineFixture::new("hello-byte-drip");
        let listener = bind_endpoint(&fixture.endpoint.path);
        let server = thread::spawn(move || {
            let mut stream = accept_endpoint(&listener);
            let mut reader = BufReader::new(clone_stream(&stream));
            let hello = read_request(&mut reader);
            assert_eq!(hello.method.as_deref(), Some("hello"));
            for _ in 0..20 {
                if stream.write_all(b" ").is_err() || stream.flush().is_err() {
                    break;
                }
                thread::sleep(Duration::from_millis(200));
            }
        });
        let workspace_root = fixture.workspace_root.clone();
        let socket_path = fixture.endpoint.path.clone();

        let (result, elapsed) = run_with_watchdog(move || {
            with_workspace_client(&workspace_root, Some(socket_path), |_| Ok(()))
        });

        assert_bounded_daemon_unavailable(result, elapsed);
        server.join().unwrap();
    }

    #[test]
    fn normal_desktop_read_stalled_newline_releases_its_worker() {
        let fixture = DeadlineFixture::new("read-stalled-newline");
        let listener = bind_endpoint(&fixture.endpoint.path);
        let workspace_id = fixture.workspace_id.clone();
        let (release, released) = mpsc::channel();
        let server = thread::spawn(move || {
            let mut stream = accept_endpoint(&listener);
            let mut reader = BufReader::new(clone_stream(&stream));
            answer_hello(&mut reader, &mut stream, &workspace_id, Duration::ZERO);
            let request = read_request(&mut reader);
            assert_eq!(request.method.as_deref(), Some("transcript.read"));
            stream.write_all(b"{\"v\":1").unwrap();
            stream.flush().unwrap();
            released.recv_timeout(OUTER_WATCHDOG).unwrap();
        });
        let workspace_root = fixture.workspace_root.clone();
        let socket_path = fixture.endpoint.path.clone();

        let (result, elapsed) = run_with_watchdog(move || {
            read_run_from_workspace(&workspace_root, "run_read", Some(socket_path))
        });

        assert_bounded_daemon_unavailable(result, elapsed);
        release.send(()).unwrap();
        server.join().unwrap();
    }

    #[test]
    fn normal_desktop_mutation_stalled_newline_has_no_false_success_or_retry() {
        let fixture = DeadlineFixture::new("mutation-stalled-newline");
        let listener = bind_endpoint(&fixture.endpoint.path);
        let workspace_id = fixture.workspace_id.clone();
        let (release, released) = mpsc::channel();
        let server = thread::spawn(move || {
            let mut stream = accept_endpoint(&listener);
            let mut reader = BufReader::new(clone_stream(&stream));
            answer_hello(&mut reader, &mut stream, &workspace_id, Duration::ZERO);
            let request = read_request(&mut reader);
            assert_eq!(request.method.as_deref(), Some("run.start"));
            assert_eq!(request.params.as_ref().unwrap()["question"], "mutate once");
            stream.write_all(b"{").unwrap();
            stream.flush().unwrap();
            released.recv_timeout(OUTER_WATCHDOG).unwrap();

            let mut unexpected_retry = String::new();
            let retry_read = reader.read_line(&mut unexpected_retry);
            assert!(
                !matches!(retry_read, Ok(bytes) if bytes > 0),
                "desktop retried a mutation after its response timed out"
            );
        });
        let workspace_root = fixture.workspace_root.clone();
        let socket_path = fixture.endpoint.path.clone();

        let (result, elapsed) = run_with_watchdog(move || {
            submit_message_from_workspace(
                &workspace_root,
                "mutate once".into(),
                None,
                Some(socket_path),
            )
        });

        assert_bounded_daemon_unavailable(result, elapsed);
        release.send(()).unwrap();
        server.join().unwrap();
    }

    #[test]
    fn normal_desktop_mutation_stalled_write_releases_its_worker() {
        let fixture = DeadlineFixture::new("mutation-stalled-write");
        let listener = bind_endpoint(&fixture.endpoint.path);
        let workspace_id = fixture.workspace_id.clone();
        let (release, released) = mpsc::channel();
        let server = thread::spawn(move || {
            let mut stream = accept_endpoint(&listener);
            let mut reader = BufReader::new(clone_stream(&stream));
            answer_hello(&mut reader, &mut stream, &workspace_id, Duration::ZERO);
            released.recv_timeout(OUTER_WATCHDOG).unwrap();
        });
        let workspace_root = fixture.workspace_root.clone();
        let socket_path = fixture.endpoint.path.clone();
        let message = "x".repeat(8 * 1024 * 1024);

        let (result, elapsed) = run_with_watchdog(move || {
            submit_message_from_workspace(&workspace_root, message, None, Some(socket_path))
        });

        assert_bounded_daemon_unavailable(result, elapsed);
        release.send(()).unwrap();
        server.join().unwrap();
    }

    #[test]
    fn normal_desktop_read_accepts_near_deadline_hello_and_command_responses() {
        let fixture = DeadlineFixture::new("near-deadline-success");
        let listener = bind_endpoint(&fixture.endpoint.path);
        let workspace_id = fixture.workspace_id.clone();
        let server = thread::spawn(move || {
            let mut stream = accept_endpoint(&listener);
            let mut reader = BufReader::new(clone_stream(&stream));
            answer_hello(&mut reader, &mut stream, &workspace_id, NEAR_DEADLINE_DELAY);
            let request = read_request(&mut reader);
            assert_eq!(request.method.as_deref(), Some("transcript.read"));
            thread::sleep(NEAR_DEADLINE_DELAY);
            write_response(
                &mut stream,
                request.id,
                "transcript.read",
                json!({
                    "run_id": "run_read",
                    "status": "finished",
                    "final_answer": "near deadline",
                    "transcript": "near deadline",
                    "typed": {"runs": [{
                        "run_id": "run_read",
                        "session_index": 0,
                        "status": "finished",
                        "entries": []
                    }]},
                    "pending_approval": null
                }),
            );
        });
        let workspace_root = fixture.workspace_root.clone();
        let socket_path = fixture.endpoint.path.clone();

        let (result, elapsed) = run_with_watchdog(move || {
            read_run_from_workspace(&workspace_root, "run_read", Some(socket_path))
        });

        let run = result.unwrap();
        assert_eq!(run.run_id, "run_read");
        assert!(
            elapsed > DAEMON_ATTACH_TIMEOUT,
            "fresh hello and command budgets shared one deadline: {elapsed:?}"
        );
        server.join().unwrap();
    }

    fn assert_bounded_daemon_unavailable<T: std::fmt::Debug>(
        result: Result<T, DesktopError>,
        elapsed: Duration,
    ) {
        let error = result.expect_err("stalled daemon request reported success");
        assert_eq!(error.code, "daemon_unavailable", "{error:?}");
        assert!(
            elapsed >= DAEMON_ATTACH_TIMEOUT - DEADLINE_EARLY_TOLERANCE,
            "daemon request timed out before its budget: {elapsed:?}"
        );
        assert!(
            elapsed < DAEMON_ATTACH_TIMEOUT + DEADLINE_LATE_TOLERANCE,
            "daemon request exceeded its budget plus scheduler tolerance: {elapsed:?}"
        );
    }

    fn run_with_watchdog<T, F>(run: F) -> (T, Duration)
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let started = Instant::now();
        let (sender, receiver) = mpsc::sync_channel(1);
        let worker = thread::spawn(move || sender.send(run()).unwrap());
        let result = receiver
            .recv_timeout(OUTER_WATCHDOG)
            .expect("desktop blocking worker outlived the outer watchdog");
        let elapsed = started.elapsed();
        worker.join().unwrap();
        (result, elapsed)
    }

    fn answer_hello<R: BufRead, W: Write>(
        reader: &mut R,
        writer: &mut W,
        workspace_id: &str,
        delay: Duration,
    ) {
        let hello = read_request(reader);
        assert_eq!(hello.method.as_deref(), Some("hello"));
        thread::sleep(delay);
        write_response(
            writer,
            hello.id,
            "hello",
            json!({
                "daemon_version": "0.1.0",
                "workspace_id": workspace_id,
                "ledger_path": "/work/agent.db",
                "capabilities": REQUIRED_CAPABILITIES
            }),
        );
    }

    fn read_request<R: BufRead>(reader: &mut R) -> Envelope {
        let mut line = String::new();
        assert_ne!(reader.read_line(&mut line).unwrap(), 0);
        let envelope: Envelope = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(envelope.kind, EnvelopeKind::Request);
        envelope
    }

    fn write_response<W: Write>(writer: &mut W, id: Option<String>, method: &str, result: Value) {
        let response = Envelope {
            v: PROTOCOL_VERSION,
            id,
            kind: EnvelopeKind::Response,
            method: Some(method.into()),
            params: None,
            result: Some(result),
            error: None,
        };
        serde_json::to_writer(writer.by_ref(), &response).unwrap();
        writer.write_all(b"\n").unwrap();
        writer.flush().unwrap();
    }

    struct DeadlineFixture {
        _workspace: tempfile::TempDir,
        endpoint: TestEndpoint,
        workspace_root: PathBuf,
        workspace_id: String,
    }

    impl DeadlineFixture {
        fn new(name: &str) -> Self {
            let workspace = tempfile::tempdir().unwrap();
            let workspace_root = workspace.path().canonicalize().unwrap();
            let workspace_id = paths::workspace_id(&workspace_root).unwrap();
            Self {
                _workspace: workspace,
                endpoint: TestEndpoint::new(name),
                workspace_root,
                workspace_id,
            }
        }
    }

    struct TestEndpoint {
        path: PathBuf,
        _directory: Option<tempfile::TempDir>,
    }

    impl TestEndpoint {
        fn new(name: &str) -> Self {
            #[cfg(unix)]
            {
                let directory = tempfile::tempdir().unwrap();
                let path = directory.path().join(format!("{name}.sock"));
                Self {
                    path,
                    _directory: Some(directory),
                }
            }
            #[cfg(windows)]
            {
                Self {
                    path: PathBuf::from(format!(
                        r"\\.\pipe\plato-agent-desktop-{name}-{}",
                        std::process::id()
                    )),
                    _directory: None,
                }
            }
        }
    }

    #[cfg(unix)]
    type TestListener = std::os::unix::net::UnixListener;
    #[cfg(unix)]
    type TestStream = std::os::unix::net::UnixStream;
    #[cfg(windows)]
    type TestListener = interprocess::local_socket::Listener;
    #[cfg(windows)]
    type TestStream = interprocess::local_socket::Stream;

    #[cfg(unix)]
    fn bind_endpoint(path: &Path) -> TestListener {
        TestListener::bind(path).unwrap()
    }

    #[cfg(windows)]
    fn bind_endpoint(path: &Path) -> TestListener {
        use interprocess::local_socket::{GenericFilePath, ListenerOptions, prelude::*};

        ListenerOptions::new()
            .name(path.as_os_str().to_fs_name::<GenericFilePath>().unwrap())
            .create_sync()
            .unwrap()
    }

    #[cfg(unix)]
    fn accept_endpoint(listener: &TestListener) -> TestStream {
        listener.accept().unwrap().0
    }

    #[cfg(windows)]
    fn accept_endpoint(listener: &TestListener) -> TestStream {
        use interprocess::local_socket::prelude::*;

        listener.accept().unwrap()
    }

    #[cfg(unix)]
    fn clone_stream(stream: &TestStream) -> TestStream {
        stream.try_clone().unwrap()
    }

    #[cfg(windows)]
    fn clone_stream(stream: &TestStream) -> TestStream {
        interprocess::TryClone::try_clone(stream).unwrap()
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use plato_protocol::{Envelope, EnvelopeKind, PROTOCOL_VERSION};
    use serde_json::json;
    use std::{
        io::{BufRead, BufReader, Write},
        os::unix::net::{UnixListener, UnixStream},
        process::Command,
        thread,
        time::{Duration, Instant},
    };

    #[test]
    fn missing_invalid_and_persisted_workspaces_have_stable_states() {
        let state = tempfile::tempdir().unwrap();
        let workspace_file = state.path().join("app/workspace.json");
        assert!(matches!(
            load_saved_workspace(&workspace_file),
            SavedWorkspaceState::Missing
        ));
        assert_eq!(
            bootstrap_from_store(&workspace_file, None).unwrap(),
            BootstrapView::NeedsWorkspace { reason: None }
        );

        let workspace = tempfile::tempdir().unwrap();
        let canonical = canonical_workspace(workspace.path()).unwrap();
        persist_canonical_workspace(&workspace_file, &canonical).unwrap();
        assert!(matches!(
            load_saved_workspace(&workspace_file),
            SavedWorkspaceState::Ready(path) if path == canonical
        ));

        drop(workspace);
        assert!(matches!(
            load_saved_workspace(&workspace_file),
            SavedWorkspaceState::Invalid(reason) if reason == "Saved workspace no longer exists"
        ));
        assert_eq!(
            bootstrap_from_store(&workspace_file, None).unwrap(),
            BootstrapView::NeedsWorkspace {
                reason: Some("Saved workspace no longer exists".into())
            }
        );
    }

    #[test]
    fn files_cannot_be_persisted_as_workspaces() {
        let state = tempfile::tempdir().unwrap();
        let workspace_file = state.path().join("workspace.json");
        let file = state.path().join("not-a-workspace");
        fs::write(&file, "text").unwrap();

        let error = canonical_workspace(&file).unwrap_err();

        assert_eq!(
            error,
            DesktopError::new("invalid_workspace", "Selected workspace is not a directory")
        );
        assert!(!workspace_file.exists());
    }

    #[test]
    fn each_shell_keeps_its_selected_workspace_in_memory() {
        let state = tempfile::tempdir().unwrap();
        let first_root = tempfile::tempdir().unwrap();
        let second_root = tempfile::tempdir().unwrap();
        let workspace_file = state.path().join("workspace.json");
        let first = Mutex::new(DesktopLifecycle::default());
        let second = Mutex::new(DesktopLifecycle::default());

        for (lifecycle, root) in [(&first, first_root.path()), (&second, second_root.path())] {
            let root = canonical_workspace(root).unwrap();
            let mut lifecycle = lifecycle.lock().unwrap();
            let prepared = lifecycle.prepare_workspace(&root).unwrap();
            lifecycle.commit_workspace(prepared);
            persist_canonical_workspace(&workspace_file, &root).unwrap();
        }

        assert_eq!(
            selected_workspace(&first).unwrap(),
            first_root.path().canonicalize().unwrap()
        );
        assert_eq!(
            selected_workspace(&second).unwrap(),
            second_root.path().canonicalize().unwrap()
        );
        assert!(matches!(
            load_saved_workspace(&workspace_file),
            SavedWorkspaceState::Ready(root)
                if root == second_root.path().canonicalize().unwrap()
        ));
    }

    #[test]
    fn connected_daemon_that_never_answers_is_bounded() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let (release, released) = std::sync::mpsc::channel();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut request = String::new();
            let mut reader = BufReader::new(stream);
            reader.read_line(&mut request).unwrap();
            assert!(!request.is_empty());
            released.recv_timeout(Duration::from_secs(2)).unwrap();
            drop(reader);
        });
        let config = DaemonConnectionConfig::resolve(workspace.path(), Some(socket_path)).unwrap();

        let started = Instant::now();
        let error =
            try_attach_workspace_until(&config, Instant::now() + Duration::from_millis(200))
                .unwrap_err();

        assert_eq!(error.code, "daemon_unavailable");
        assert!(started.elapsed() < Duration::from_secs(1));
        release.send(()).unwrap();
        server.join().unwrap();
    }

    #[test]
    fn connected_daemon_that_drips_a_response_cannot_extend_the_deadline() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = String::new();
            BufReader::new(stream.try_clone().unwrap())
                .read_line(&mut request)
                .unwrap();
            assert!(!request.is_empty());
            for _ in 0..20 {
                if stream.write_all(b" ").is_err() {
                    break;
                }
                thread::sleep(Duration::from_millis(50));
            }
        });
        let config = DaemonConnectionConfig::resolve(workspace.path(), Some(socket_path)).unwrap();

        let started = Instant::now();
        let error =
            try_attach_workspace_until(&config, Instant::now() + Duration::from_millis(200))
                .unwrap_err();

        assert_eq!(error.code, "daemon_unavailable");
        assert!(started.elapsed() < Duration::from_secs(1));
        server.join().unwrap();
    }

    #[test]
    fn late_initial_attach_hands_the_tracked_child_to_a_reaper() {
        let workspace = tempfile::tempdir().unwrap();
        let workspace_root = canonical_workspace(workspace.path()).unwrap();
        let workspace_id = paths::workspace_id(&workspace_root).unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let server_workspace_id = workspace_id.clone();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut writer = stream.try_clone().unwrap();
            let mut reader = BufReader::new(stream);
            answer_hello(&mut reader, &mut writer, server_workspace_id);
            let sessions = read_request(&mut reader);
            write_response(
                &mut writer,
                sessions.id,
                "sessions.list",
                json!({"sessions": []}),
            );
        });
        let child = Command::new("/bin/sh")
            .args(["-c", "sleep 0.1"])
            .spawn()
            .unwrap();
        let child_id = child.id();
        let mut lifecycle = DesktopLifecycle {
            workspace_root: Some(workspace_root.clone()),
            spawned_daemon: Some(SpawnedDaemon {
                workspace_id,
                child,
            }),
        };

        let view = attach_or_spawn_workspace(
            &workspace_root,
            Some(socket_path),
            &mut lifecycle,
            &DaemonLaunch::default(),
        )
        .unwrap();

        assert!(matches!(view, BootstrapView::Ready { .. }));
        assert!(lifecycle.spawned_daemon.is_none());
        server.join().unwrap();
        wait_for_process_gone(child_id);
    }

    #[test]
    fn workspace_switch_hands_the_previous_child_to_a_reaper() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let first = canonical_workspace(first.path()).unwrap();
        let second = canonical_workspace(second.path()).unwrap();
        let child = Command::new("/bin/sh")
            .args(["-c", "sleep 0.1"])
            .spawn()
            .unwrap();
        let child_id = child.id();
        let mut lifecycle = DesktopLifecycle {
            workspace_root: Some(first.clone()),
            spawned_daemon: Some(SpawnedDaemon {
                workspace_id: paths::workspace_id(&first).unwrap(),
                child,
            }),
        };

        let prepared = lifecycle.prepare_workspace(&second).unwrap();
        lifecycle.commit_workspace(prepared);

        assert_eq!(lifecycle.workspace_root.as_deref(), Some(second.as_path()));
        assert!(lifecycle.spawned_daemon.is_none());
        wait_for_process_gone(child_id);
    }

    #[test]
    fn timed_out_sidecar_is_reaped_without_another_bootstrap() {
        use std::os::unix::fs::PermissionsExt;

        let workspace = tempfile::tempdir().unwrap();
        let workspace_root = canonical_workspace(workspace.path()).unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let pid_file = socket_dir.path().join("sidecar.pid");
        let sidecar = socket_dir.path().join("slow-sidecar");
        fs::write(
            &sidecar,
            format!(
                "#!/bin/sh\nprintf '%s' $$ > '{}'\nsleep 4\n",
                pid_file.display()
            ),
        )
        .unwrap();
        let mut permissions = fs::metadata(&sidecar).unwrap().permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&sidecar, permissions).unwrap();
        let mut lifecycle = DesktopLifecycle::default();

        let error = attach_or_spawn_workspace(
            &workspace_root,
            Some(socket_path),
            &mut lifecycle,
            &DaemonLaunch {
                executable: Some(sidecar),
            },
        )
        .unwrap_err();

        assert_eq!(error.code, "daemon_start_failed");
        assert!(lifecycle.spawned_daemon.is_none());
        let pid = fs::read_to_string(pid_file).unwrap().parse().unwrap();
        wait_for_process_gone(pid);
    }

    #[test]
    fn missing_packaged_sidecar_fails_closed_with_runtime_paths() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let missing = socket_dir.path().join("missing-plato-agentd");
        let launch = DaemonLaunch {
            executable: Some(missing.clone()),
        };

        let error = attach_or_spawn_workspace(
            workspace.path(),
            Some(socket_path.clone()),
            &mut DesktopLifecycle::default(),
            &launch,
        )
        .unwrap_err();

        assert_eq!(error.code, "daemon_start_failed");
        assert!(error.message.contains(missing.to_string_lossy().as_ref()));
        assert!(
            error
                .message
                .contains(socket_path.to_string_lossy().as_ref())
        );
        let lock = paths::default_lock_path(workspace.path()).unwrap();
        assert!(error.message.contains(lock.to_string_lossy().as_ref()));
        assert!(!socket_path.exists());
        assert!(!lock.exists());
    }

    #[test]
    fn capability_manifest_exposes_only_the_ten_typed_commands() {
        let capability: Value =
            serde_json::from_str(include_str!("../capabilities/main.json")).unwrap();
        let commands = [
            "bootstrap",
            "pick_workspace",
            "read_run",
            "list_sessions",
            "read_session",
            "submit_message",
            "poll_run",
            "recover_run",
            "decide_approval",
            "cancel_run",
        ];
        assert_eq!(
            capability["permissions"],
            json!([
                "allow-bootstrap",
                "allow-pick-workspace",
                "allow-read-run",
                "allow-list-sessions",
                "allow-read-session",
                "allow-submit-message",
                "allow-poll-run",
                "allow-recover-run",
                "allow-decide-approval",
                "allow-cancel-run"
            ])
        );
        let serialized = serde_json::to_string(&capability).unwrap();
        for forbidden in ["dialog:", "fs:", "shell:", "http:", "core:", "remote"] {
            assert!(!serialized.contains(forbidden), "found {forbidden}");
        }
        let build = include_str!("../build.rs");
        for command in commands {
            assert!(build.contains(&format!("\"{command}\"")));
        }
    }

    #[test]
    fn desktop_bridge_returns_only_typed_presentation_data() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let workspace_id = paths::workspace_id(workspace.path()).unwrap();
        let expected_workspace_id = workspace_id.clone();
        let handle = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut writer = stream.try_clone().unwrap();
            let mut reader = BufReader::new(stream);
            let hello = read_request(&mut reader);
            write_response(
                &mut writer,
                hello.id,
                "hello",
                json!({
                    "daemon_version": "0.1.0",
                    "workspace_id": expected_workspace_id,
                    "ledger_path": "/secret/ledger.db",
                    "capabilities": REQUIRED_CAPABILITIES
                }),
            );
            let sessions = read_request(&mut reader);
            write_response(
                &mut writer,
                sessions.id,
                "sessions.list",
                json!({
                    "sessions": [{
                        "session_id": "session_1",
                        "run_id": "run_1",
                        "status": "finished",
                        "latest_question": "hello",
                        "ledger_path": "/secret/ledger.db"
                    }]
                }),
            );
            let transcript = read_request(&mut reader);
            assert_eq!(transcript.params.unwrap()["run_id"], "run_1");
            write_response(
                &mut writer,
                transcript.id,
                "transcript.read",
                json!({
                    "run_id": "run_1",
                    "status": "finished",
                    "final_answer": "hi",
                    "transcript": "POISON_LEGACY_TRANSCRIPT",
                    "typed": {"runs": [{
                        "run_id": "run_1",
                        "session_index": 0,
                        "status": "finished",
                        "entries": [
                            {"kind": "user", "text": "hello"},
                            {"kind": "assistant", "text": "hi"}
                        ]
                    }]}
                }),
            );
        });

        let view = connect_workspace(workspace.path(), Some(socket_path)).unwrap();
        handle.join().unwrap();
        let serialized = serde_json::to_string(&view).unwrap();

        assert!(serialized.contains(workspace.path().to_str().unwrap()));
        assert!(serialized.contains("\"kind\":\"assistant\""));
        for forbidden in [
            "POISON_LEGACY_TRANSCRIPT",
            "/secret/ledger.db",
            "ledgerPath",
            "socketPath",
            "transcript",
        ] {
            assert!(!serialized.contains(forbidden), "found {forbidden}");
        }
    }

    #[test]
    fn missing_typed_capability_stops_before_session_or_transcript_reads() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let launch = DaemonLaunch {
            executable: Some(socket_dir.path().join("must-not-start")),
        };
        let listener = UnixListener::bind(&socket_path).unwrap();
        let workspace_id = paths::workspace_id(workspace.path()).unwrap();
        let capabilities = REQUIRED_CAPABILITIES
            .iter()
            .filter(|capability| **capability != "transcript.read.typed")
            .copied()
            .collect::<Vec<_>>();
        let handle = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut writer = stream.try_clone().unwrap();
            let mut reader = BufReader::new(stream);
            let hello = read_request(&mut reader);
            write_response(
                &mut writer,
                hello.id,
                "hello",
                json!({
                    "daemon_version": "old",
                    "workspace_id": workspace_id,
                    "ledger_path": "/tmp/ledger.db",
                    "capabilities": capabilities
                }),
            );
        });

        let error = attach_or_spawn_workspace(
            workspace.path(),
            Some(socket_path),
            &mut DesktopLifecycle::default(),
            &launch,
        )
        .unwrap_err();
        handle.join().unwrap();

        assert_eq!(
            error,
            DesktopError::new(
                "incompatible_daemon",
                "Incompatible daemon: missing required capability transcript.read.typed"
            )
        );
    }

    #[test]
    fn hello_validation_rejects_a_different_workspace() {
        let workspace = tempfile::tempdir().unwrap();
        let hello = HelloResult {
            daemon_version: "0.1.0".into(),
            workspace_id: "other-workspace".into(),
            ledger_path: "/secret/ledger.db".into(),
            capabilities: REQUIRED_CAPABILITIES
                .iter()
                .map(ToString::to_string)
                .collect(),
        };

        let error = validate_hello(workspace.path(), &hello).unwrap_err();

        assert_eq!(error.code, "incompatible_daemon");
        assert!(
            error
                .message
                .starts_with("Incompatible daemon: expected workspace ")
        );
        assert!(error.message.ends_with(", got other-workspace"));
        assert!(!error.message.contains("ledger.db"));
    }

    #[test]
    fn exact_run_typed_payload_fails_closed_on_missing_or_wrong_boundaries() {
        let base = TranscriptReadResult {
            run_id: "run_1".into(),
            status: RunStateName::Finished,
            final_answer: Some("answer".into()),
            transcript: "legacy".into(),
            typed: None,
            pending_approval: None,
            completion_claim: None,
        };
        assert_eq!(
            extract_typed_run("run_1", base.clone())
                .unwrap_err()
                .message,
            "Incompatible daemon: transcript.read returned no typed payload"
        );

        let mut multiple = base.clone();
        multiple.typed = Some(plato_protocol::TypedTranscript {
            runs: vec![typed_run("run_1"), typed_run("run_2")],
        });
        assert_eq!(
            extract_typed_run("run_1", multiple).unwrap_err().message,
            "Incompatible daemon: exact-run transcript returned 2 runs"
        );

        let mut wrong = base;
        wrong.typed = Some(plato_protocol::TypedTranscript {
            runs: vec![typed_run("run_2")],
        });
        assert_eq!(
            extract_typed_run("run_1", wrong).unwrap_err().message,
            "Incompatible daemon: requested run run_1, got run_2"
        );
    }

    #[test]
    fn typed_runs_assign_every_assistant_step_before_display_filtering() {
        let run = TypedRun {
            run_id: "run_1".into(),
            session_index: 2,
            status: RunStateName::Finished,
            model_status: None,
            entries: vec![
                TypedTranscriptEntry::User {
                    text: "question".into(),
                },
                TypedTranscriptEntry::Assistant {
                    text: String::new(),
                },
                TypedTranscriptEntry::ToolCall {
                    call_id: "call_1".into(),
                    tool: "file.read".into(),
                    input: json!({"path": "README.md"}),
                },
                TypedTranscriptEntry::Assistant {
                    text: "answer".into(),
                },
            ],
        };

        let run = DesktopRun::try_from(run).unwrap();

        assert!(matches!(
            &run.entries[1],
            DesktopEntry::Assistant { step: 0, text } if text.is_empty()
        ));
        assert!(matches!(
            &run.entries[3],
            DesktopEntry::Assistant { step: 1, text } if text == "answer"
        ));
        let serialized = serde_json::to_value(run).unwrap();
        assert_eq!(
            serialized["entries"][2]["inputPreview"],
            r#"{"path":"README.md"}"#
        );
        assert!(serialized["entries"][2].get("input").is_none());
    }

    #[test]
    fn presentation_event_fixtures_preserve_every_mapped_and_ignored_variant() {
        const OFFSET: u64 = 41;
        let fixtures = vec![
            (
                "assistant_delta",
                buffered_event(
                    OFFSET,
                    json!({
                        "kind": "assistant_delta",
                        "run_id": "run_1",
                        "turn_id": "turn_1",
                        "step": 2,
                        "delta_index": 7,
                        "text": "hel"
                    }),
                ),
                Some(json!({
                    "kind": "assistant_delta",
                    "offset": OFFSET,
                    "step": 2,
                    "deltaIndex": 7,
                    "text": "hel"
                })),
            ),
            (
                "run_started",
                ledger_event(
                    OFFSET,
                    json!({
                        "event": "run_started",
                        "run_id": "run_1",
                        "agent_id": "agent_1"
                    }),
                ),
                None,
            ),
            (
                "context_built",
                ledger_event(
                    OFFSET,
                    json!({
                        "event": "context_built",
                        "run_id": "run_1",
                        "turn_id": "turn_1",
                        "context": {
                            "token_budget": 8,
                            "fragments": [{
                                "lane": "current_task",
                                "source": "user",
                                "content": "question",
                                "estimated_tokens": 2
                            }]
                        }
                    }),
                ),
                None,
            ),
            (
                "context_compacted",
                ledger_event(
                    OFFSET,
                    json!({
                        "event": "context_compacted",
                        "run_id": "run_1",
                        "turn_id": "turn_1",
                        "estimated_tokens_before": 12,
                        "estimated_tokens_after": 8,
                        "dropped_turn_start": 0,
                        "dropped_turn_end_exclusive": 1
                    }),
                ),
                None,
            ),
            (
                "model_requested",
                ledger_event(
                    OFFSET,
                    json!({
                        "event": "model_requested",
                        "run_id": "run_1",
                        "turn_id": "turn_1",
                        "step": 2,
                        "model": "model_1"
                    }),
                ),
                None,
            ),
            (
                "model_failed",
                ledger_event(
                    OFFSET,
                    json!({
                        "event": "model_failed",
                        "run_id": "run_1",
                        "turn_id": "turn_1",
                        "step": 2,
                        "reason": "retryable failure"
                    }),
                ),
                None,
            ),
            (
                "model_responded",
                ledger_event(
                    OFFSET,
                    json!({
                        "event": "model_responded",
                        "run_id": "run_1",
                        "turn_id": "turn_1",
                        "step": 2,
                        "output": {"role": "assistant", "content": "hello"},
                        "proposed_calls": [],
                        "usage": {"input_tokens": 3, "output_tokens": 1}
                    }),
                ),
                Some(json!({
                    "kind": "assistant_committed",
                    "offset": OFFSET,
                    "step": 2,
                    "text": "hello"
                })),
            ),
            (
                "tool_proposals_rejected",
                ledger_event(
                    OFFSET,
                    json!({
                        "event": "tool_proposals_rejected",
                        "run_id": "run_1",
                        "turn_id": "turn_1",
                        "reason": "invalid proposal"
                    }),
                ),
                None,
            ),
            (
                "tool_call_proposed",
                ledger_event(
                    OFFSET,
                    json!({
                        "event": "tool_call_proposed",
                        "run_id": "run_1",
                        "turn_id": "turn_1",
                        "call": {
                            "id": "call_1",
                            "tool": "file.read",
                            "effect": "read_only",
                            "input": {"path": "README.md"}
                        }
                    }),
                ),
                Some(json!({
                    "kind": "tool_call",
                    "offset": OFFSET,
                    "callId": "call_1",
                    "tool": "file.read",
                    "inputPreview": r#"{"path":"README.md"}"#
                })),
            ),
            (
                "policy_allow",
                ledger_event(
                    OFFSET,
                    json!({
                        "event": "policy_evaluated",
                        "run_id": "run_1",
                        "call_id": "call_1",
                        "decision": {"decision": "allow"}
                    }),
                ),
                None,
            ),
            (
                "policy_require_approval",
                ledger_event(
                    OFFSET,
                    json!({
                        "event": "policy_evaluated",
                        "run_id": "run_1",
                        "call_id": "call_2",
                        "decision": {
                            "decision": "require_approval",
                            "reason": "operator confirmation required"
                        }
                    }),
                ),
                None,
            ),
            (
                "policy_deny",
                ledger_event(
                    OFFSET,
                    json!({
                        "event": "policy_evaluated",
                        "run_id": "run_1",
                        "call_id": "call_3",
                        "decision": {"decision": "deny", "reason": "not permitted"}
                    }),
                ),
                Some(json!({
                    "kind": "policy_denied",
                    "offset": OFFSET,
                    "callId": "call_3",
                    "reason": "not permitted"
                })),
            ),
            (
                "approval_granted",
                ledger_event(
                    OFFSET,
                    json!({
                        "event": "approval_granted",
                        "run_id": "run_1",
                        "call_id": "call_1",
                        "actor_id": "human_1"
                    }),
                ),
                Some(json!({
                    "kind": "approval",
                    "offset": OFFSET,
                    "callId": "call_1",
                    "decision": "granted",
                    "actorId": "human_1",
                    "reason": null
                })),
            ),
            (
                "approval_denied",
                ledger_event(
                    OFFSET,
                    json!({
                        "event": "approval_denied",
                        "run_id": "run_1",
                        "call_id": "call_2",
                        "actor_id": "human_2",
                        "reason": "not now"
                    }),
                ),
                Some(json!({
                    "kind": "approval",
                    "offset": OFFSET,
                    "callId": "call_2",
                    "decision": "denied",
                    "actorId": "human_2",
                    "reason": "not now"
                })),
            ),
            (
                "tool_started",
                ledger_event(
                    OFFSET,
                    json!({
                        "event": "tool_started",
                        "run_id": "run_1",
                        "call_id": "call_1"
                    }),
                ),
                None,
            ),
            (
                "tool_finished",
                ledger_event(
                    OFFSET,
                    json!({
                        "event": "tool_finished",
                        "run_id": "run_1",
                        "result": {
                            "call_id": "call_1",
                            "summary": "read file",
                            "data": {"secret_raw": true},
                            "artifacts": ["artifact_1"],
                            "visibility": "both"
                        }
                    }),
                ),
                Some(json!({
                    "kind": "tool_result",
                    "offset": OFFSET,
                    "callId": "call_1",
                    "summary": "read file"
                })),
            ),
            (
                "tool_failed",
                ledger_event(
                    OFFSET,
                    json!({
                        "event": "tool_failed",
                        "run_id": "run_1",
                        "call_id": "call_3",
                        "reason": "execution failed"
                    }),
                ),
                Some(json!({
                    "kind": "tool_failed",
                    "offset": OFFSET,
                    "callId": "call_3",
                    "error": "execution failed"
                })),
            ),
            (
                "run_finished",
                ledger_event(OFFSET, json!({"event": "run_finished", "run_id": "run_1"})),
                None,
            ),
            (
                "run_failed",
                ledger_event(
                    OFFSET,
                    json!({
                        "event": "run_failed",
                        "run_id": "run_1",
                        "reason": "terminal failure"
                    }),
                ),
                None,
            ),
            (
                "approval_requested",
                buffered_event(
                    OFFSET,
                    json!({
                        "kind": "approval_requested",
                        "run_id": "run_1",
                        "tool_call_id": "call_4",
                        "tool_name": "file.write",
                        "effect": "workspace_write",
                        "reason": "approval needed"
                    }),
                ),
                Some(json!({
                    "kind": "approval_requested",
                    "offset": OFFSET,
                    "toolCallId": "call_4"
                })),
            ),
            (
                "canceled",
                buffered_event(OFFSET, json!({"kind": "canceled", "run_id": "run_1"})),
                Some(json!({"kind": "cancel_requested", "offset": OFFSET})),
            ),
            (
                "unknown_stream_event",
                buffered_event(
                    OFFSET,
                    json!({
                        "kind": "future_event",
                        "run_id": "run_1",
                        "payload": {"answer": 42}
                    }),
                ),
                None,
            ),
        ];

        for (name, before, after) in fixtures {
            let actual = buffered_event_into_desktop(before)
                .map(|event| serde_json::to_value(event).unwrap());
            assert_eq!(actual, after, "fixture {name}");
        }
    }

    #[test]
    fn unknown_event_is_ignored_without_stalling_the_page_offset() {
        let page = EventsStreamResult {
            run_id: "run_1".into(),
            from_offset: 4,
            next_offset: 5,
            status: RunStateName::Running,
            events: vec![buffered_event(
                4,
                json!({
                    "kind": "future_event",
                    "run_id": "run_1",
                    "payload": {"answer": 42}
                }),
            )],
        };

        let page = normalize_event_page("run_1", page).unwrap();

        assert_eq!(page.from_offset, 4);
        assert_eq!(page.next_offset, 5);
        assert!(page.events.is_empty());
    }

    #[test]
    fn event_page_boundaries_keep_exact_typed_errors() {
        let fixtures = vec![
            (
                "wrong run",
                EventsStreamResult {
                    run_id: "run_other".into(),
                    from_offset: 4,
                    next_offset: 4,
                    status: RunStateName::Running,
                    events: vec![],
                },
                "Incompatible daemon: requested events for run_1, got run_other",
            ),
            (
                "reversed offsets",
                EventsStreamResult {
                    run_id: "run_1".into(),
                    from_offset: 5,
                    next_offset: 4,
                    status: RunStateName::Running,
                    events: vec![],
                },
                "Incompatible daemon: events.stream next_offset precedes from_offset",
            ),
            (
                "page length mismatch",
                EventsStreamResult {
                    run_id: "run_1".into(),
                    from_offset: 4,
                    next_offset: 5,
                    status: RunStateName::Running,
                    events: vec![],
                },
                "Incompatible daemon: events.stream offsets do not match its page length",
            ),
            (
                "offset overflow",
                EventsStreamResult {
                    run_id: "run_1".into(),
                    from_offset: u64::MAX,
                    next_offset: u64::MAX,
                    status: RunStateName::Running,
                    events: vec![buffered_event(u64::MAX, json!({"kind": "future_event"}))],
                },
                "Incompatible daemon: events.stream offsets do not match its page length",
            ),
            (
                "noncontiguous event offset",
                EventsStreamResult {
                    run_id: "run_1".into(),
                    from_offset: 5,
                    next_offset: 6,
                    status: RunStateName::Running,
                    events: vec![buffered_event(6, json!({"kind": "future_event"}))],
                },
                "Incompatible daemon: event offset 6 is not expected offset 5",
            ),
        ];

        for (name, page, message) in fixtures {
            assert_eq!(
                normalize_event_page("run_1", page).unwrap_err(),
                DesktopError::new("incompatible_daemon", message),
                "fixture {name}"
            );
        }
    }

    #[test]
    fn session_read_returns_all_runs_in_session_index_order() {
        let fixture = bridge_fixture();
        let listener = UnixListener::bind(&fixture.socket_path).unwrap();
        let workspace_id = fixture.workspace_id.clone();
        let handle = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut writer = stream.try_clone().unwrap();
            let mut reader = BufReader::new(stream);
            answer_hello(&mut reader, &mut writer, workspace_id);

            let transcript = read_request(&mut reader);
            assert_eq!(transcript.method.as_deref(), Some("transcript.read"));
            assert_eq!(
                transcript.params.as_ref().unwrap()["session_id"],
                "session_1"
            );
            assert!(transcript.params.as_ref().unwrap()["run_id"].is_null());
            write_response(
                &mut writer,
                transcript.id,
                "transcript.read",
                json!({
                    "run_id": "run_2",
                    "status": "running",
                    "final_answer": null,
                    "transcript": "POISON",
                    "typed": {"runs": [
                        typed_run_json("run_2", 1, "running", "second"),
                        typed_run_json("run_1", 0, "finished", "first")
                    ]},
                    "pending_approval": null
                }),
            );
        });

        let transcript = read_session_from_store(
            &fixture.workspace_file,
            "session_1",
            Some(fixture.socket_path),
        )
        .unwrap();
        handle.join().unwrap();

        assert_eq!(transcript.runs.len(), 2);
        assert_eq!(transcript.runs[0].run_id, "run_1");
        assert_eq!(transcript.runs[1].run_id, "run_2");
        assert_eq!(transcript.runs[1].status, RunStateName::Running);
        assert!(
            !serde_json::to_string(&transcript)
                .unwrap()
                .contains("POISON")
        );
    }

    #[test]
    fn composer_uses_new_or_selected_session_and_never_waits() {
        let fixture = bridge_fixture();
        let listener = UnixListener::bind(&fixture.socket_path).unwrap();
        let workspace_id = fixture.workspace_id.clone();
        let handle = thread::spawn(move || {
            for (method, message, session_id, run_id) in [
                ("run.start", "new question", None, "run_1"),
                ("message.append", "follow up", Some("session_1"), "run_2"),
            ] {
                let (stream, _) = listener.accept().unwrap();
                let mut writer = stream.try_clone().unwrap();
                let mut reader = BufReader::new(stream);
                answer_hello(&mut reader, &mut writer, workspace_id.clone());

                let request = read_request(&mut reader);
                assert_eq!(request.method.as_deref(), Some(method));
                let params = request.params.as_ref().unwrap();
                let message_field = if method == "run.start" {
                    "question"
                } else {
                    "message"
                };
                assert_eq!(params[message_field], message);
                assert_eq!(params["wait"], false);
                match session_id {
                    Some(session_id) => assert_eq!(params["session_id"], session_id),
                    None => assert!(params.get("session_id").is_none()),
                }
                write_response(
                    &mut writer,
                    request.id,
                    method,
                    json!({
                        "run_id": run_id,
                        "session_id": "session_1",
                        "ledger_path": "/secret/ledger.db",
                        "status": "running",
                        "final_answer": null
                    }),
                );
            }
        });

        let started = submit_message_from_store(
            &fixture.workspace_file,
            "new question".into(),
            None,
            Some(fixture.socket_path.clone()),
        )
        .unwrap();
        let appended = submit_message_from_store(
            &fixture.workspace_file,
            "follow up".into(),
            Some("session_1".into()),
            Some(fixture.socket_path),
        )
        .unwrap();
        handle.join().unwrap();

        assert_eq!(started.run_id, "run_1");
        assert_eq!(appended.run_id, "run_2");
        assert_eq!(started.status, RunStateName::Running);
    }

    #[test]
    fn command_responses_must_match_the_requested_session_or_run() {
        let fixture = bridge_fixture();
        let listener = UnixListener::bind(&fixture.socket_path).unwrap();
        let workspace_id = fixture.workspace_id.clone();
        let handle = thread::spawn(move || {
            for (method, result) in [
                (
                    "message.append",
                    json!({
                        "run_id": "run_2",
                        "session_id": "session_other",
                        "ledger_path": "/secret/ledger.db",
                        "status": "running",
                        "final_answer": null
                    }),
                ),
                (
                    "approval.decide",
                    json!({"run_id": "run_other", "status": "running"}),
                ),
                (
                    "run.cancel",
                    json!({"run_id": "run_other", "status": "cancel_requested"}),
                ),
            ] {
                let (stream, _) = listener.accept().unwrap();
                let mut writer = stream.try_clone().unwrap();
                let mut reader = BufReader::new(stream);
                answer_hello(&mut reader, &mut writer, workspace_id.clone());
                let request = read_request(&mut reader);
                assert_eq!(request.method.as_deref(), Some(method));
                write_response(&mut writer, request.id, method, result);
            }
        });

        let append_error = submit_message_from_store(
            &fixture.workspace_file,
            "follow up".into(),
            Some("session_1".into()),
            Some(fixture.socket_path.clone()),
        )
        .unwrap_err();
        let approval_error = decide_approval_from_store(
            &fixture.workspace_file,
            "run_1",
            "call_1",
            DesktopApprovalDecision::Grant,
            None,
            Some(fixture.socket_path.clone()),
        )
        .unwrap_err();
        let cancel_error =
            cancel_run_from_store(&fixture.workspace_file, "run_1", Some(fixture.socket_path))
                .unwrap_err();
        handle.join().unwrap();

        assert_eq!(append_error.code, "incompatible_daemon");
        assert_eq!(
            append_error.message,
            "Incompatible daemon: appended session session_1, got session_other"
        );
        assert_eq!(approval_error.code, "incompatible_daemon");
        assert_eq!(
            approval_error.message,
            "Incompatible daemon: decided approval for run_1, got run_other"
        );
        assert_eq!(cancel_error.code, "incompatible_daemon");
        assert_eq!(
            cancel_error.message,
            "Incompatible daemon: canceled run run_1, got run_other"
        );
    }

    #[test]
    fn poll_requests_a_full_page_and_keeps_every_delta_key() {
        let fixture = bridge_fixture();
        let listener = UnixListener::bind(&fixture.socket_path).unwrap();
        let workspace_id = fixture.workspace_id.clone();
        let handle = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut writer = stream.try_clone().unwrap();
            let mut reader = BufReader::new(stream);
            answer_hello(&mut reader, &mut writer, workspace_id);

            let request = read_request(&mut reader);
            assert_eq!(request.method.as_deref(), Some("events.stream"));
            assert_eq!(request.params.as_ref().unwrap()["from_offset"], 0);
            assert_eq!(request.params.as_ref().unwrap()["limit"], EVENT_PAGE_SIZE);
            let events = (0..EVENT_PAGE_SIZE as u64)
                .map(|offset| {
                    buffered_event(
                        offset,
                        json!({
                            "kind": "assistant_delta",
                            "run_id": "run_1",
                            "turn_id": "turn_1",
                            "step": 0,
                            "delta_index": offset,
                            "text": "x"
                        }),
                    )
                })
                .collect::<Vec<_>>();
            write_response(
                &mut writer,
                request.id,
                "events.stream",
                json!({
                    "run_id": "run_1",
                    "from_offset": 0,
                    "next_offset": EVENT_PAGE_SIZE,
                    "status": "running",
                    "events": events
                }),
            );
        });

        let page = poll_run_from_store(
            &fixture.workspace_file,
            "run_1",
            0,
            Some(fixture.socket_path),
        )
        .unwrap();
        handle.join().unwrap();

        assert_eq!(page.events.len(), EVENT_PAGE_SIZE);
        assert_eq!(page.next_offset, EVENT_PAGE_SIZE as u64);
        assert!(matches!(
            page.events.last(),
            Some(DesktopEvent::AssistantDelta {
                offset: 127,
                delta_index: 127,
                ..
            })
        ));
    }

    #[test]
    fn lag_recovery_anchors_then_snapshots_then_continues_from_anchor() {
        let fixture = bridge_fixture();
        let listener = UnixListener::bind(&fixture.socket_path).unwrap();
        let workspace_id = fixture.workspace_id.clone();
        let handle = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut writer = stream.try_clone().unwrap();
            let mut reader = BufReader::new(stream);
            answer_hello(&mut reader, &mut writer, workspace_id);

            let anchor = read_request(&mut reader);
            assert_eq!(anchor.method.as_deref(), Some("events.stream"));
            assert!(anchor.params.as_ref().unwrap()["from_offset"].is_null());
            write_response(
                &mut writer,
                anchor.id,
                "events.stream",
                json!({
                    "run_id": "run_1",
                    "from_offset": 4,
                    "next_offset": 4,
                    "status": "running",
                    "events": []
                }),
            );

            let transcript = read_request(&mut reader);
            assert_eq!(transcript.method.as_deref(), Some("transcript.read"));
            assert_eq!(transcript.params.as_ref().unwrap()["run_id"], "run_1");
            write_response(
                &mut writer,
                transcript.id,
                "transcript.read",
                json!({
                    "run_id": "run_1",
                    "status": "running",
                    "final_answer": null,
                    "transcript": "POISON",
                    "typed": {"runs": [typed_run_json("run_1", 0, "running", "hello")]},
                    "pending_approval": {
                        "run_id": "run_1",
                        "tool_call_id": "call_1",
                        "tool_name": "file.write",
                        "effect": "workspace_write",
                        "reason": "approval needed",
                        "input_preview": "{path: out.txt}"
                    }
                }),
            );

            let continued = read_request(&mut reader);
            assert_eq!(continued.method.as_deref(), Some("events.stream"));
            assert_eq!(continued.params.as_ref().unwrap()["from_offset"], 4);
            write_response(
                &mut writer,
                continued.id,
                "events.stream",
                json!({
                    "run_id": "run_1",
                    "from_offset": 4,
                    "next_offset": 5,
                    "status": "running",
                    "events": [buffered_event(4, json!({
                        "kind": "approval_requested",
                        "run_id": "run_1",
                        "tool_call_id": "call_1",
                        "tool_name": "file.write",
                        "effect": "workspace_write",
                        "reason": "approval needed"
                    }))]
                }),
            );
        });

        let recovery =
            recover_run_from_store(&fixture.workspace_file, "run_1", Some(fixture.socket_path))
                .unwrap();
        handle.join().unwrap();

        assert_eq!(recovery.anchor_offset, 4);
        assert_eq!(recovery.run.run_id, "run_1");
        assert_eq!(
            recovery
                .pending_approval
                .as_ref()
                .map(|pending| pending.tool_call_id.as_str()),
            Some("call_1")
        );
        assert_eq!(recovery.page.from_offset, 4);
        assert!(matches!(
            recovery.page.events.as_slice(),
            [DesktopEvent::ApprovalRequested { offset: 4, tool_call_id }] if tool_call_id == "call_1"
        ));
    }

    #[test]
    fn protocol_errors_keep_typed_code_and_message() {
        let error = DesktopError::daemon(
            "Unable to decide approval",
            ClientError::DaemonResponse(plato_protocol::ProtocolError {
                code: "not_found".into(),
                message: "pending approval not found: call_1".into(),
            }),
        );

        assert_eq!(
            serde_json::to_value(error).unwrap(),
            json!({
                "code": "not_found",
                "message": "pending approval not found: call_1"
            })
        );
    }

    #[test]
    fn raced_approval_error_and_cancel_status_stay_typed() {
        let fixture = bridge_fixture();
        let listener = UnixListener::bind(&fixture.socket_path).unwrap();
        let workspace_id = fixture.workspace_id.clone();
        let handle = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut writer = stream.try_clone().unwrap();
            let mut reader = BufReader::new(stream);
            answer_hello(&mut reader, &mut writer, workspace_id.clone());
            let approval = read_request(&mut reader);
            assert_eq!(approval.method.as_deref(), Some("approval.decide"));
            assert_eq!(approval.params.as_ref().unwrap()["run_id"], "run_1");
            assert_eq!(approval.params.as_ref().unwrap()["tool_call_id"], "call_1");
            write_error(
                &mut writer,
                approval.id,
                "approval.decide",
                "not_found",
                "pending approval not found: call_1",
            );

            let (stream, _) = listener.accept().unwrap();
            let mut writer = stream.try_clone().unwrap();
            let mut reader = BufReader::new(stream);
            answer_hello(&mut reader, &mut writer, workspace_id);
            let cancel = read_request(&mut reader);
            assert_eq!(cancel.method.as_deref(), Some("run.cancel"));
            assert_eq!(cancel.params.as_ref().unwrap()["run_id"], "run_1");
            write_response(
                &mut writer,
                cancel.id,
                "run.cancel",
                json!({"run_id": "run_1", "status": "cancel_requested"}),
            );
        });

        let error = decide_approval_from_store(
            &fixture.workspace_file,
            "run_1",
            "call_1",
            DesktopApprovalDecision::Grant,
            None,
            Some(fixture.socket_path.clone()),
        )
        .unwrap_err();
        let canceled =
            cancel_run_from_store(&fixture.workspace_file, "run_1", Some(fixture.socket_path))
                .unwrap();
        handle.join().unwrap();

        assert_eq!(error.code, "not_found");
        assert_eq!(error.message, "pending approval not found: call_1");
        assert_eq!(canceled.status, RunStateName::CancelRequested);
    }

    #[test]
    fn all_run_state_names_cross_the_bridge_as_typed_wire_values() {
        for status in [
            RunStateName::Running,
            RunStateName::Finished,
            RunStateName::Failed,
            RunStateName::Canceled,
            RunStateName::CancelRequested,
            RunStateName::Interrupted,
        ] {
            let page = DesktopEventPage {
                run_id: "run_1".into(),
                from_offset: 0,
                next_offset: 0,
                status,
                events: vec![],
            };
            assert_eq!(
                serde_json::to_value(page).unwrap()["status"],
                status.as_str()
            );
        }
    }

    fn typed_run(run_id: &str) -> TypedRun {
        TypedRun {
            run_id: run_id.into(),
            session_index: 0,
            status: RunStateName::Finished,
            model_status: None,
            entries: vec![],
        }
    }

    struct BridgeFixture {
        _state: tempfile::TempDir,
        _workspace: tempfile::TempDir,
        workspace_file: PathBuf,
        socket_path: PathBuf,
        workspace_id: String,
    }

    fn bridge_fixture() -> BridgeFixture {
        let state = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let workspace_file = state.path().join("workspace.json");
        let canonical = canonical_workspace(workspace.path()).unwrap();
        persist_canonical_workspace(&workspace_file, &canonical).unwrap();
        let socket_path = state.path().join("agent.sock");
        let workspace_id = paths::workspace_id(workspace.path()).unwrap();
        BridgeFixture {
            _state: state,
            _workspace: workspace,
            workspace_file,
            socket_path,
            workspace_id,
        }
    }

    fn answer_hello(
        reader: &mut BufReader<UnixStream>,
        writer: &mut UnixStream,
        workspace_id: String,
    ) {
        let hello = read_request(reader);
        assert_eq!(hello.method.as_deref(), Some("hello"));
        write_response(
            writer,
            hello.id,
            "hello",
            json!({
                "daemon_version": "0.1.0",
                "workspace_id": workspace_id,
                "ledger_path": "/secret/ledger.db",
                "capabilities": REQUIRED_CAPABILITIES
            }),
        );
    }

    fn typed_run_json(run_id: &str, session_index: u64, status: &str, assistant: &str) -> Value {
        json!({
            "run_id": run_id,
            "session_index": session_index,
            "status": status,
            "entries": [
                {"kind": "user", "text": "question"},
                {"kind": "assistant", "text": assistant}
            ]
        })
    }

    fn buffered_event(offset: u64, event: Value) -> BufferedStreamEvent {
        serde_json::from_value(json!({"offset": offset, "event": event})).unwrap()
    }

    fn ledger_event(offset: u64, event: Value) -> BufferedStreamEvent {
        buffered_event(
            offset,
            json!({
                "kind": "ledger",
                "record": {
                    "seq": offset,
                    "occurred_at_ms": offset,
                    "event": event
                }
            }),
        )
    }

    fn read_request(reader: &mut BufReader<UnixStream>) -> Envelope {
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        let envelope: Envelope = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(envelope.kind, EnvelopeKind::Request);
        envelope
    }

    fn wait_for_process_gone(pid: u32) {
        let pid = rustix::process::Pid::from_raw(pid as i32).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match rustix::process::test_kill_process(pid) {
                Err(rustix::io::Errno::SRCH) => return,
                Ok(()) | Err(rustix::io::Errno::PERM) => {}
                Err(error) => panic!("cannot inspect child process: {error}"),
            }
            assert!(Instant::now() < deadline, "child process was not reaped");
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn write_response(writer: &mut UnixStream, id: Option<String>, method: &str, result: Value) {
        let response = Envelope {
            v: PROTOCOL_VERSION,
            id,
            kind: EnvelopeKind::Response,
            method: Some(method.into()),
            params: None,
            result: Some(result),
            error: None,
        };
        serde_json::to_writer(writer.by_ref(), &response).unwrap();
        writer.write_all(b"\n").unwrap();
        writer.flush().unwrap();
    }

    fn write_error(
        writer: &mut UnixStream,
        id: Option<String>,
        method: &str,
        code: &str,
        message: &str,
    ) {
        let response = Envelope::error(id, Some(method.into()), code, message);
        serde_json::to_writer(writer.by_ref(), &response).unwrap();
        writer.write_all(b"\n").unwrap();
        writer.flush().unwrap();
    }
}
