//! Daemon-backed one-shot execution for Plato Agent clients.

use crate::{AppError, AppResult};
use platonic_client::{ClientError, client::DaemonClient, paths};
use platonic_core::{RecordedEvent, RunId, TurnId};
use platonic_protocol::{
    ApprovalDecision, CompletionClaim, ERROR_WORKSPACE_UNREGISTERED, RunOverrides, RunStateName,
    StreamEvent,
};
use serde::{Deserialize, Serialize};
use std::{
    io::{self, BufRead, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::Sender,
    },
    thread,
    time::{Duration, Instant},
};

const CONNECT_TIMEOUT: Duration = Duration::from_millis(250);
const ENSURE_TIMEOUT: Duration = Duration::from_secs(5);
const EVENT_POLL: Duration = Duration::from_millis(50);
const EVENT_PAGE: usize = 128;

/// Client-side approval behavior for a one-shot run.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ApprovalMode {
    /// Ask on stdin before deciding an effect.
    #[default]
    Prompt,
    /// Grant only server-classified yolo-eligible requests.
    AutoApprove,
    /// Deny each requested effect without prompting.
    Deny,
}

/// One transient assistant text fragment received from the server.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AssistantDeltaEvent {
    /// Run producing the fragment.
    pub run_id: RunId,
    /// Turn producing the fragment.
    pub turn_id: TurnId,
    /// Model step producing the fragment.
    pub step: u32,
    /// Contiguous fragment index within the step.
    pub delta_index: u64,
    /// Text fragment.
    pub text: String,
}

/// Client-observable event used by presentation layers such as voice.
#[derive(Clone, Debug, PartialEq)]
pub enum RunEvent {
    /// Durable kernel event received over the wire.
    Ledger(RecordedEvent),
    /// Transient assistant text fragment.
    AssistantDelta(AssistantDeltaEvent),
}

/// Inputs for one daemon-backed client run.
#[derive(Clone, Debug)]
pub struct RunOptions {
    /// User question.
    pub question: String,
    /// Optional server configuration path.
    pub config_path: Option<PathBuf>,
    /// Optional model overrides.
    pub overrides: RunOverrides,
    /// Workspace selected by the client.
    pub workspace_root: PathBuf,
    /// Existing session to continue, or `None` for a fresh session.
    pub session_id: Option<String>,
    /// Continue the latest workspace session when no explicit session is set.
    pub continue_latest: bool,
    /// Client approval behavior.
    pub approval_mode: ApprovalMode,
    /// Optional exclusive presentation event sink.
    pub event_sender: Option<Sender<RunEvent>>,
    /// Whether assistant deltas are written to stderr as they arrive.
    pub stream_to_stderr: bool,
    /// Shared client cancellation request.
    pub cancel: Option<Arc<AtomicBool>>,
}

/// Successful one-shot client result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RunOutcome {
    /// Server-minted run identifier.
    pub run_id: RunId,
    /// Final assistant answer.
    pub final_answer: String,
    /// Optional worker completion claim carried by the protocol.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_claim: Option<CompletionClaim>,
    /// Daemon-owned ledger path used for offline replay.
    pub ledger_path: PathBuf,
    /// Whether the server delivered at least one assistant delta.
    #[serde(default)]
    pub streamed: bool,
}

/// Connects to the host server for `workspace_root`, starting `platonic serve`
/// when the endpoint is not already available.
pub fn ensure_server(workspace_root: &Path) -> AppResult<DaemonClient> {
    let workspace_root = workspace_root.canonicalize()?;
    let socket_path = paths::host_socket_path()?;
    if let Some(client) = connect_ready(&workspace_root, &socket_path)? {
        return Ok(client);
    }

    let executable = server_binary()?;
    let mut command = Command::new(&executable);
    command
        .arg("serve")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let mut child = command.spawn().map_err(|error| {
        AppError::Config(format!("cannot start {}: {error}", executable.display()))
    })?;

    let deadline = Instant::now() + ENSURE_TIMEOUT;
    loop {
        if let Some(client) = connect_ready(&workspace_root, &socket_path)? {
            return Ok(client);
        }
        if let Some(status) = child.try_wait()? {
            // A concurrent client can win the host lock while our child exits.
            if let Some(client) = connect_ready(&workspace_root, &socket_path)? {
                return Ok(client);
            }
            return Err(AppError::Config(format!(
                "{} serve exited before readiness with {status}",
                executable.display()
            )));
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(AppError::Config(format!(
                "{} serve did not become ready within {} ms",
                executable.display(),
                ENSURE_TIMEOUT.as_millis()
            )));
        }
        thread::sleep(EVENT_POLL);
    }
}

fn connect_ready(workspace_root: &Path, socket_path: &Path) -> AppResult<Option<DaemonClient>> {
    let mut client = match DaemonClient::connect_with_timeout(socket_path, CONNECT_TIMEOUT) {
        Ok(client) => client,
        Err(platonic_client::ClientError::Io(_)) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    client.hello(workspace_root)?;
    client.clear_request_timeout()?;
    Ok(Some(client))
}

/// Ensures a local server, asking once before registering an unrecognized directory.
pub fn ensure_server_interactive(
    workspace_root: &Path,
    input: &mut dyn BufRead,
    errors: &mut dyn Write,
) -> AppResult<DaemonClient> {
    match ensure_server(workspace_root) {
        Ok(client) => return Ok(client),
        Err(AppError::DaemonResponse(error)) if error.code == ERROR_WORKSPACE_UNREGISTERED => {}
        Err(error) => return Err(error),
    }
    attach_server_interactive(workspace_root, &paths::host_socket_path()?, input, errors)
}

/// Attaches to one local endpoint, asking once before registering its directory.
pub fn attach_server_interactive(
    workspace_root: &Path,
    socket_path: &Path,
    input: &mut dyn BufRead,
    errors: &mut dyn Write,
) -> AppResult<DaemonClient> {
    let workspace_root = workspace_root.canonicalize()?;
    let mut client = DaemonClient::connect_with_timeout(socket_path, CONNECT_TIMEOUT)?;
    let registration_error = match client.hello(&workspace_root) {
        Ok(_) => {
            client.clear_request_timeout()?;
            return Ok(client);
        }
        Err(ClientError::DaemonResponse(error)) if error.code == ERROR_WORKSPACE_UNREGISTERED => {
            error
        }
        Err(error) => return Err(error.into()),
    };
    let default_name = workspace_root
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("workspace");
    let Some(name) = prompt_workspace_name(default_name, input, errors)? else {
        return Err(AppError::DaemonResponse(registration_error));
    };

    client.workspace_create(name, workspace_root.clone())?;
    client.hello(&workspace_root)?;
    client.clear_request_timeout()?;
    Ok(client)
}

fn prompt_workspace_name(
    default_name: &str,
    input: &mut dyn BufRead,
    errors: &mut dyn Write,
) -> AppResult<Option<String>> {
    write!(
        errors,
        "Workspace name [{default_name}] (Enter to create, n to decline): "
    )?;
    errors.flush()?;
    let mut answer = String::new();
    if input.read_line(&mut answer)? == 0 {
        return Ok(None);
    }
    let name = answer.trim();
    match name.to_ascii_lowercase().as_str() {
        "" | "y" | "yes" => Ok(Some(default_name.to_owned())),
        "n" | "no" => Ok(None),
        _ => Ok(Some(name.to_owned())),
    }
}

fn server_binary() -> AppResult<PathBuf> {
    if let Some(path) = std::env::var_os("PLATONIC_BIN").filter(|path| !path.is_empty()) {
        return Ok(PathBuf::from(path));
    }
    let current = std::env::current_exe()?;
    let parent = current
        .parent()
        .ok_or_else(|| AppError::Config(format!("cannot find sibling of {}", current.display())))?;
    let names = ["platonic", "platonic-real"];
    for name in names {
        let sibling = parent.join(name);
        if sibling.is_file() {
            return Ok(sibling);
        }
    }
    Err(AppError::Config(format!(
        "cannot find the platonic server binary beside {}",
        current.display()
    )))
}

/// Runs one question through the ensured server and consumes its typed event
/// stream until a terminal result is available.
pub fn run_question(options: RunOptions) -> AppResult<RunOutcome> {
    if options.question.trim().is_empty() {
        return Err(AppError::Config("question is empty".into()));
    }
    let mut client = ensure_server(&options.workspace_root)?;
    let config_path = options.config_path.as_ref().map(|path| {
        if path.is_absolute() {
            path.clone()
        } else {
            options.workspace_root.join(path)
        }
    });
    let config_path = config_path.map(|path| path.to_string_lossy().into_owned());
    let started = match options.session_id {
        Some(session_id) => client.message_append_to_session_with_overrides(
            options.question,
            Some(session_id),
            config_path,
            options.overrides,
            false,
        )?,
        None if options.continue_latest => client.message_append_to_session_with_overrides(
            options.question,
            None,
            config_path,
            options.overrides,
            false,
        )?,
        None => client.run_start_with_overrides(
            options.question,
            config_path,
            options.overrides,
            false,
        )?,
    };
    let run_id = RunId::new(started.run_id.clone())?;
    let mut next_offset = Some(0);
    let mut canceled = false;
    let mut streamed = false;

    loop {
        if !canceled
            && options
                .cancel
                .as_ref()
                .is_some_and(|cancel| cancel.load(Ordering::Acquire))
        {
            client.run_cancel(&started.run_id)?;
            canceled = true;
        }

        let page = client.events_stream(&started.run_id, next_offset, EVENT_PAGE)?;
        next_offset = Some(page.next_offset);
        for buffered in page.events {
            match buffered.event {
                StreamEvent::Ledger { record } => {
                    send_event(&options.event_sender, RunEvent::Ledger(record))?;
                }
                StreamEvent::AssistantDelta {
                    run_id,
                    turn_id,
                    step,
                    delta_index,
                    text,
                } => {
                    streamed = true;
                    if options.stream_to_stderr {
                        eprint!("{text}");
                        io::stderr().flush()?;
                    }
                    send_event(
                        &options.event_sender,
                        RunEvent::AssistantDelta(AssistantDeltaEvent {
                            run_id: RunId::new(run_id)?,
                            turn_id: TurnId::new(turn_id)?,
                            step,
                            delta_index,
                            text,
                        }),
                    )?;
                }
                StreamEvent::ApprovalRequested {
                    run_id,
                    tool_call_id,
                    tool_name,
                    reason,
                    yolo_eligible,
                    ..
                } => decide_approval(
                    &mut client,
                    options.approval_mode,
                    &run_id,
                    &tool_call_id,
                    &tool_name,
                    &reason,
                    yolo_eligible,
                )?,
                StreamEvent::Canceled { .. }
                | StreamEvent::CompletionClaimed { .. }
                | StreamEvent::Unknown(_) => {}
            }
        }

        match page.status {
            RunStateName::Running | RunStateName::CancelRequested => thread::sleep(EVENT_POLL),
            RunStateName::Finished => {
                if options.stream_to_stderr {
                    eprintln!();
                }
                let transcript = client.transcript_read(&started.run_id)?;
                let final_answer = transcript.final_answer.ok_or_else(|| {
                    AppError::RunFailed("finished run had no final answer".into())
                })?;
                return Ok(RunOutcome {
                    run_id,
                    final_answer,
                    completion_claim: started.completion_claim,
                    ledger_path: PathBuf::from(started.ledger_path),
                    streamed,
                });
            }
            RunStateName::Canceled => return Err(AppError::RunCanceled),
            RunStateName::Failed | RunStateName::Interrupted => {
                return Err(AppError::RunFailed(page.status.to_string()));
            }
        }
    }
}

fn send_event(sender: &Option<Sender<RunEvent>>, event: RunEvent) -> AppResult<()> {
    if let Some(sender) = sender {
        sender
            .send(event)
            .map_err(|_| AppError::Config("run event receiver closed".into()))?;
    }
    Ok(())
}

fn decide_approval(
    client: &mut DaemonClient,
    mode: ApprovalMode,
    run_id: &str,
    tool_call_id: &str,
    tool_name: &str,
    reason: &str,
    yolo_eligible: bool,
) -> AppResult<()> {
    let yolo_grant = mode == ApprovalMode::AutoApprove && yolo_eligible;
    let decision = match mode {
        ApprovalMode::AutoApprove if yolo_grant => ApprovalDecision::Grant,
        ApprovalMode::AutoApprove | ApprovalMode::Prompt => {
            eprint!("approve {tool_name} ({reason})? [y/N] ");
            io::stderr().flush()?;
            let mut answer = String::new();
            io::stdin().lock().read_line(&mut answer)?;
            if matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes") {
                ApprovalDecision::Grant
            } else {
                ApprovalDecision::Deny
            }
        }
        ApprovalMode::Deny => ApprovalDecision::Deny,
    };
    match decision {
        ApprovalDecision::Grant if yolo_grant => {
            client.approval_grant_as(run_id, tool_call_id, "yolo".into())?;
        }
        ApprovalDecision::Grant => {
            client.approval_grant(run_id, tool_call_id)?;
        }
        ApprovalDecision::GrantSession => {
            client.approval_grant_session(run_id, tool_call_id)?;
        }
        ApprovalDecision::Deny => {
            client.approval_deny(run_id, tool_call_id, "approval denied by stdin".into())?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::prompt_workspace_name;
    use std::io::Cursor;

    #[test]
    fn interactive_workspace_prompt_creates_on_enter_and_accepts_a_name() {
        let mut output = Vec::new();
        assert_eq!(
            prompt_workspace_name("alpha", &mut Cursor::new("\n"), &mut output).unwrap(),
            Some("alpha".into())
        );
        assert_eq!(
            prompt_workspace_name("alpha", &mut Cursor::new("beta\n"), &mut output).unwrap(),
            Some("beta".into())
        );
        assert!(
            String::from_utf8(output)
                .unwrap()
                .contains("Workspace name [alpha]")
        );
    }

    #[test]
    fn interactive_workspace_prompt_decline_and_eof_do_not_create() {
        let mut output = Vec::new();
        assert_eq!(
            prompt_workspace_name("alpha", &mut Cursor::new("n\n"), &mut output).unwrap(),
            None
        );
        assert_eq!(
            prompt_workspace_name("alpha", &mut Cursor::new(Vec::<u8>::new()), &mut output)
                .unwrap(),
            None
        );
    }
}
