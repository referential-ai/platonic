use crate::{ActiveRunView, ApprovalModalView, TranscriptState, TranscriptView, TuiState};
use plato_daemon_client::{
    ClientError, ClientResult,
    client::{DaemonClient, DaemonConnectionConfig},
};
use plato_protocol::{
    ApprovalDecisionName, CommandAcceptedResult, DaemonStatusResult, ERROR_LAGGED, ERROR_OVERLOAD,
    ERROR_UNSUPPORTED_VERSION, ERROR_WORKSPACE_MISMATCH, EventsStreamResult, IssuePrepResult,
    IssuePrepStartResult, RunStartResult, RunStateName, StreamEvent,
};
use std::{
    collections::HashMap,
    sync::mpsc::{self, Receiver, Sender},
    thread,
    time::{Duration, Instant},
};

use super::{
    app::{push_live_event, send_command, start_next_queued},
    state::approval_from_snapshot,
};

pub(super) const ACTIVE_POLL_INTERVAL: Duration = Duration::from_millis(200);
pub(super) const DAEMON_CLIENT_TIMEOUT: Duration = Duration::from_secs(3);
pub(super) const EVENT_LIMIT: usize = 128;

pub(super) fn load_state(config: &DaemonConnectionConfig, run_id: Option<&str>) -> TuiState {
    match load_connected_state(config, run_id, None) {
        Ok(state) => state,
        Err(error) => TuiState::disconnected(
            config.workspace_root.to_string_lossy().into_owned(),
            config.socket_path.to_string_lossy().into_owned(),
            error.to_string(),
        ),
    }
}

fn load_connected_state(
    config: &DaemonConnectionConfig,
    run_id: Option<&str>,
    session_id: Option<&str>,
) -> ClientResult<TuiState> {
    let mut client = connect_daemon(config, DAEMON_CLIENT_TIMEOUT)?;
    let hello = client.hello(&config.workspace_root)?;
    let sessions = client.sessions_list()?;
    let selected_session_id = session_id
        .map(str::to_owned)
        .or_else(|| {
            run_id.and_then(|run_id| {
                sessions
                    .iter()
                    .find(|session| session.run_id == run_id)
                    .map(|session| session.session_id.clone())
            })
        })
        .or_else(|| sessions.first().map(|session| session.session_id.clone()));
    let (transcript, approval) =
        if let Some(session_id) = session_id.or(selected_session_id.as_deref()) {
            match client.transcript_read_session(session_id) {
                Ok(transcript) => loaded_transcript_state(transcript),
                Err(error) => (
                    TranscriptState::Unavailable {
                        run_id: session_id.to_owned(),
                        error: error.to_string(),
                    },
                    None,
                ),
            }
        } else {
            match run_id {
                Some(run_id) => match client.transcript_read(run_id) {
                    Ok(transcript) => loaded_transcript_state(transcript),
                    Err(error) => (
                        TranscriptState::Unavailable {
                            run_id: run_id.to_owned(),
                            error: error.to_string(),
                        },
                        None,
                    ),
                },
                None => (TranscriptState::None, None),
            }
        };
    let mut state = TuiState::connected(
        config.workspace_root.to_string_lossy().into_owned(),
        config.socket_path.to_string_lossy().into_owned(),
        hello,
        sessions,
        transcript,
    );
    state.selected_session_id = selected_session_id;
    state.approval = approval;
    let active_session = state.selected_session_id.as_deref().and_then(|session_id| {
        state.sessions.iter().find(|session| {
            session.session_id == session_id
                && matches!(
                    session.status,
                    RunStateName::Running | RunStateName::CancelRequested
                )
        })
    });
    if let Some(session) = active_session {
        state.active_run = Some(ActiveRunView::new(session.run_id.clone(), session.status));
    }
    Ok(state)
}

fn loaded_transcript_state(
    transcript: plato_protocol::TranscriptReadResult,
) -> (TranscriptState, Option<ApprovalModalView>) {
    let approval = transcript
        .pending_approval
        .clone()
        .map(approval_from_snapshot);
    (
        TranscriptState::Loaded(TranscriptView::from(transcript)),
        approval,
    )
}

fn load_selected_session_state(config: &DaemonConnectionConfig, session_id: &str) -> TuiState {
    match load_connected_state(config, None, Some(session_id)) {
        Ok(state) => state,
        Err(error) => TuiState::disconnected(
            config.workspace_root.to_string_lossy().into_owned(),
            config.socket_path.to_string_lossy().into_owned(),
            error.to_string(),
        ),
    }
}

#[derive(Debug)]
pub(super) struct UiRuntime {
    pub(super) active_run_id: Option<String>,
    pub(super) config_path: Option<String>,
    pub(super) next_offset: u64,
    pub(super) poll_in_flight: bool,
    pub(super) polling: bool,
    pub(super) last_poll: Instant,
    pub(super) tool_inputs: HashMap<String, String>,
    pub(super) active_since: Option<Instant>,
}

impl UiRuntime {
    pub(super) fn from_state(state: &TuiState, config_path: Option<String>) -> Self {
        Self {
            active_run_id: state.active_run.as_ref().map(|run| run.run_id.clone()),
            config_path,
            next_offset: 0,
            poll_in_flight: false,
            polling: state.active_run.as_ref().is_some_and(|run| {
                matches!(
                    run.status,
                    RunStateName::Running | RunStateName::CancelRequested
                )
            }),
            last_poll: Instant::now(),
            tool_inputs: HashMap::new(),
            active_since: state.active_run.as_ref().map(|_| Instant::now()),
        }
    }

    fn sync_from_state(&mut self, state: &TuiState) {
        self.active_run_id = state.active_run.as_ref().map(|run| run.run_id.clone());
        self.polling = state.active_run.as_ref().is_some_and(|run| {
            matches!(
                run.status,
                RunStateName::Running | RunStateName::CancelRequested
            )
        });
        self.next_offset = 0;
        self.poll_in_flight = false;
        self.last_poll = Instant::now();
        self.tool_inputs.clear();
        self.active_since = state.active_run.as_ref().map(|_| Instant::now());
    }
}

#[derive(Debug)]
pub(super) enum ClientCommand {
    Load {
        run_id: Option<String>,
    },
    LoadSession {
        session_id: String,
    },
    DaemonStatus {
        session_id: Option<String>,
        config_path: Option<String>,
    },
    RunStart {
        question: String,
        config_path: Option<String>,
    },
    MessageAppend {
        message: String,
        session_id: String,
        config_path: Option<String>,
    },
    IssuePrepStart {
        input: String,
        config_path: Option<String>,
    },
    PollEvents {
        run_id: String,
        from_offset: Option<u64>,
    },
    ApprovalGrant {
        run_id: String,
        tool_call_id: String,
    },
    ApprovalDeny {
        run_id: String,
        tool_call_id: String,
        reason: String,
    },
    RunCancel {
        run_id: String,
    },
}

#[derive(Debug)]
pub(super) enum ClientEvent {
    Loaded(Box<TuiState>),
    StatusLoaded(Box<DaemonStatusResult>),
    RunStarted(RunStartResult),
    IssuePrepFinished(IssuePrepStartResult),
    EventsPolled(EventsStreamResult),
    ApprovalDecided {
        result: CommandAcceptedResult,
        tool_call_id: String,
        decision: ApprovalDecisionName,
    },
    RunCanceled(CommandAcceptedResult),
    Failed {
        operation: ClientOperation,
        error: ClientError,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClientOperation {
    DaemonStatus,
    RunStart,
    MessageAppend,
    IssuePrepStart,
    EventsStream,
    ApprovalDecide,
    RunCancel,
}

impl ClientOperation {
    fn method(self) -> &'static str {
        match self {
            Self::DaemonStatus => "daemon.status",
            Self::RunStart => "run.start",
            Self::MessageAppend => "message.append",
            Self::IssuePrepStart => "issue-prep.start",
            Self::EventsStream => "events.stream",
            Self::ApprovalDecide => "approval.decide",
            Self::RunCancel => "run.cancel",
        }
    }
}

pub(super) fn spawn_client_worker(
    config: DaemonConnectionConfig,
) -> (Sender<ClientCommand>, Receiver<ClientEvent>) {
    let (command_sender, command_receiver) = mpsc::channel();
    let (event_sender, event_receiver) = mpsc::channel();
    thread::spawn(move || {
        for command in command_receiver {
            let event = handle_client_command(&config, command);
            if event_sender.send(event).is_err() {
                break;
            }
        }
    });
    (command_sender, event_receiver)
}

fn handle_client_command(config: &DaemonConnectionConfig, command: ClientCommand) -> ClientEvent {
    match command {
        ClientCommand::Load { run_id } => {
            ClientEvent::Loaded(Box::new(load_state(config, run_id.as_deref())))
        }
        ClientCommand::LoadSession { session_id } => {
            ClientEvent::Loaded(Box::new(load_selected_session_state(config, &session_id)))
        }
        ClientCommand::DaemonStatus {
            session_id,
            config_path,
        } => with_client(config, |client| {
            client.daemon_status(session_id, config_path)
        })
        .map_or_else(failed_event(ClientOperation::DaemonStatus), |status| {
            ClientEvent::StatusLoaded(Box::new(status))
        }),
        ClientCommand::RunStart {
            question,
            config_path,
        } => with_client(config, |client| {
            client.run_start(question, config_path, false)
        })
        .map_or_else(
            failed_event(ClientOperation::RunStart),
            ClientEvent::RunStarted,
        ),
        ClientCommand::MessageAppend {
            message,
            session_id,
            config_path,
        } => with_client(config, |client| {
            client.message_append_to_session(message, Some(session_id), config_path, false)
        })
        .map_or_else(
            failed_event(ClientOperation::MessageAppend),
            ClientEvent::RunStarted,
        ),
        ClientCommand::IssuePrepStart { input, config_path } => {
            let result = (|| {
                let mut client = connect_daemon(config, DAEMON_CLIENT_TIMEOUT)?;
                client.hello(&config.workspace_root)?;
                client.clear_request_timeout()?;
                client.issue_prep_start(input, config_path)
            })();
            result.map_or_else(
                failed_event(ClientOperation::IssuePrepStart),
                ClientEvent::IssuePrepFinished,
            )
        }
        ClientCommand::PollEvents {
            run_id,
            from_offset,
        } => with_client(config, |client| {
            client.events_stream(&run_id, from_offset, EVENT_LIMIT)
        })
        .map_or_else(
            failed_event(ClientOperation::EventsStream),
            ClientEvent::EventsPolled,
        ),
        ClientCommand::ApprovalGrant {
            run_id,
            tool_call_id,
        } => {
            let result = with_client(config, |client| {
                client.approval_grant(&run_id, &tool_call_id)
            });
            result.map_or_else(failed_event(ClientOperation::ApprovalDecide), |result| {
                ClientEvent::ApprovalDecided {
                    result,
                    tool_call_id,
                    decision: ApprovalDecisionName::Granted,
                }
            })
        }
        ClientCommand::ApprovalDeny {
            run_id,
            tool_call_id,
            reason,
        } => {
            let result = with_client(config, |client| {
                client.approval_deny(&run_id, &tool_call_id, reason)
            });
            result.map_or_else(failed_event(ClientOperation::ApprovalDecide), |result| {
                ClientEvent::ApprovalDecided {
                    result,
                    tool_call_id,
                    decision: ApprovalDecisionName::Denied,
                }
            })
        }
        ClientCommand::RunCancel { run_id } => {
            with_client(config, |client| client.run_cancel(&run_id)).map_or_else(
                failed_event(ClientOperation::RunCancel),
                ClientEvent::RunCanceled,
            )
        }
    }
}

fn with_client<T>(
    config: &DaemonConnectionConfig,
    run: impl FnOnce(&mut DaemonClient) -> ClientResult<T>,
) -> ClientResult<T> {
    let mut client = connect_daemon(config, DAEMON_CLIENT_TIMEOUT)?;
    client.hello(&config.workspace_root)?;
    run(&mut client)
}

pub(super) fn connect_daemon(
    config: &DaemonConnectionConfig,
    timeout: Duration,
) -> ClientResult<DaemonClient> {
    DaemonClient::connect_with_timeout(&config.socket_path, timeout)
}

fn failed_event(operation: ClientOperation) -> impl FnOnce(ClientError) -> ClientEvent {
    move |error| ClientEvent::Failed { operation, error }
}

pub(super) fn drain_client_events(
    state: &mut TuiState,
    runtime: &mut UiRuntime,
    events: &Receiver<ClientEvent>,
    commands: &Sender<ClientCommand>,
) {
    while let Ok(event) = events.try_recv() {
        match event {
            ClientEvent::Loaded(loaded) => {
                apply_loaded_state(state, *loaded);
                runtime.sync_from_state(state);
            }
            ClientEvent::StatusLoaded(status) => {
                state.status_modal = Some(*status);
                state.status_message = Some("status opened".into());
            }
            ClientEvent::RunStarted(result) => {
                apply_run_response(state, runtime, result, "run started")
            }
            ClientEvent::IssuePrepFinished(result) => {
                state.issue_prep_started_at = None;
                match result.outcome {
                    IssuePrepResult::Candidate { markdown } => {
                        push_live_event(state, crate::LiveEventLine::assistant(None, markdown));
                        push_live_event(
                            state,
                            crate::LiveEventLine::status(
                                None,
                                format!("issue-prep artifacts: {}", result.run_dir),
                            ),
                        );
                        state.status_message =
                            Some(format!("issue ready; artifacts: {}", result.run_dir));
                    }
                    IssuePrepResult::Blocked { stage, reasons } => {
                        let reason_text = if reasons.is_empty() {
                            String::new()
                        } else {
                            format!(":\n- {}", reasons.join("\n- "))
                        };
                        push_live_event(
                            state,
                            crate::LiveEventLine::warning(
                                None,
                                format!(
                                    "issue prep blocked at {stage}{reason_text}\nartifacts: {}",
                                    result.run_dir
                                ),
                            ),
                        );
                        state.status_message = Some(format!(
                            "issue prep blocked at {stage}; artifacts: {}",
                            result.run_dir
                        ));
                    }
                }
                state.reset_scroll();
                start_next_queued(commands, state, runtime);
            }
            ClientEvent::EventsPolled(result) => {
                apply_events_result(state, runtime, commands, result)
            }
            ClientEvent::ApprovalDecided {
                result,
                tool_call_id,
                decision,
            } => {
                state.status_message =
                    Some(format!("approval decision sent for {}", result.run_id));
                state.approval = None;
                state.active_run = Some(ActiveRunView::new(result.run_id.clone(), result.status));
                let decision = match decision {
                    ApprovalDecisionName::Granted => "granted",
                    ApprovalDecisionName::Denied => "denied",
                };
                push_live_event(
                    state,
                    crate::LiveEventLine::approval(
                        None,
                        format!("approval {decision} {tool_call_id}"),
                    )
                    .with_run_id(result.run_id),
                );
            }
            ClientEvent::RunCanceled(result) => {
                state.status_message = Some(format!("cancel requested for {}", result.run_id));
                state.cancel_requested = true;
                state.approval = None;
                state.active_run = Some(ActiveRunView::new(result.run_id.clone(), result.status));
                push_live_event(
                    state,
                    crate::LiveEventLine::status(
                        None,
                        format!("cancel requested: {}", result.run_id),
                    )
                    .with_run_id(result.run_id),
                );
            }
            ClientEvent::Failed { operation, error } => {
                runtime.poll_in_flight = false;
                let connection_error = is_connection_error(&error);
                let lagged = matches!(
                    &error,
                    ClientError::DaemonResponse(error) if error.code == ERROR_LAGGED
                );
                let overloaded = matches!(
                    &error,
                    ClientError::DaemonResponse(error) if error.code == ERROR_OVERLOAD
                );
                let message = error.to_string();
                if operation == ClientOperation::EventsStream && lagged {
                    state.stream_warning = Some(format!("{message}; resuming at current tip"));
                    if let Some(run_id) = runtime.active_run_id.clone() {
                        poll_events_from(runtime, commands, run_id, None);
                    }
                } else if operation == ClientOperation::EventsStream && overloaded {
                    state.stream_warning = Some(message);
                } else {
                    if connection_error {
                        runtime.polling = false;
                        state.connection = crate::ConnectionState::Disconnected {
                            error: message.clone(),
                        };
                    }
                    let failure = format!("{} failed: {message}", operation.method());
                    state.status_message = Some(failure.clone());
                    match operation {
                        ClientOperation::RunCancel => {
                            state.cancel_requested = false;
                        }
                        ClientOperation::IssuePrepStart => {
                            state.issue_prep_started_at = None;
                            push_live_event(state, crate::LiveEventLine::warning(None, failure));
                            if !connection_error {
                                start_next_queued(commands, state, runtime);
                            }
                        }
                        ClientOperation::RunStart
                        | ClientOperation::MessageAppend
                        | ClientOperation::EventsStream
                        | ClientOperation::ApprovalDecide
                        | ClientOperation::DaemonStatus => {}
                    }
                }
            }
        }
    }
}

pub(super) fn apply_loaded_state(state: &mut TuiState, mut loaded: TuiState) {
    let matching_selected_run = matches!(
        (
            state.selected_session_id.as_deref(),
            selected_run_id(state),
            loaded.selected_session_id.as_deref(),
            selected_run_id(&loaded),
        ),
        (
            Some(current_session),
            Some(current_run),
            Some(loaded_session),
            Some(loaded_run),
        ) if current_session == loaded_session && current_run == loaded_run
    );
    loaded.composer = std::mem::take(&mut state.composer);
    loaded.composer_cursor = state.composer_cursor;
    loaded.composer_kill_buffer = state.composer_kill_buffer.clone();
    loaded.slash_popup = state.slash_popup.clone();
    loaded.queued_messages = std::mem::take(&mut state.queued_messages);
    loaded.issue_prep_started_at = state.issue_prep_started_at;
    loaded.input_history = std::mem::take(&mut state.input_history);
    loaded.history_index = state.history_index;
    loaded.help_visible = state.help_visible;
    loaded.status_modal = state.status_modal.clone();
    loaded.display_mode = state.display_mode;
    if loaded.status_message.is_none() {
        loaded.status_message = state.status_message.clone();
    }
    if matching_selected_run {
        if loaded.stream_warning.is_none() {
            loaded.stream_warning = state.stream_warning.clone();
        }
        if loaded.live_events.is_empty() {
            loaded.live_events = std::mem::take(&mut state.live_events);
            loaded.history_rows.live_events = std::mem::take(&mut state.history_rows.live_events);
        }
        loaded.scroll_offset = state.scroll_offset;
        loaded.conversation_scroll_offset = state.conversation_scroll_offset;
        loaded.audit_scroll_offset = state.audit_scroll_offset;
        if loaded.active_model.is_none() {
            loaded.active_model = state.active_model.clone();
        }
        if loaded.active_run_elapsed_secs.is_none() {
            loaded.active_run_elapsed_secs = state.active_run_elapsed_secs;
        }
        if loaded.approval.is_none() {
            loaded.approval = state.approval.clone();
        }
        loaded.cancel_requested = state.cancel_requested;
    }
    *state = loaded;
}

fn selected_run_id(state: &TuiState) -> Option<&str> {
    state
        .active_run
        .as_ref()
        .map(|run| run.run_id.as_str())
        .or_else(|| {
            let selected_session_id = state.selected_session_id.as_deref()?;
            state
                .sessions
                .iter()
                .find(|session| session.session_id == selected_session_id)
                .map(|session| session.run_id.as_str())
        })
        .or(match &state.transcript {
            TranscriptState::Loaded(transcript) => Some(transcript.run_id.as_str()),
            TranscriptState::Unavailable { run_id, .. } => Some(run_id.as_str()),
            TranscriptState::None => None,
        })
}

pub(super) fn apply_run_response(
    state: &mut TuiState,
    runtime: &mut UiRuntime,
    result: RunStartResult,
    message: &'static str,
) {
    let run_id = result.run_id.clone();
    let status = result.status;
    state.selected_session_id = Some(result.session_id.clone());
    state.status_message = Some(format!("{message}: {run_id}"));
    state.stream_warning = None;
    state.cancel_requested = false;
    state.approval = None;
    state.active_run = Some(ActiveRunView::new(run_id.clone(), status));
    state.bind_latest_user_to_run(&run_id);
    push_live_event(
        state,
        crate::LiveEventLine::status(None, format!("{message}: {run_id}"))
            .with_run_id(run_id.clone()),
    );
    state.reset_scroll();
    runtime.active_run_id = Some(run_id);
    runtime.next_offset = 0;
    runtime.poll_in_flight = false;
    runtime.polling = status == RunStateName::Running;
    runtime.last_poll = Instant::now() - ACTIVE_POLL_INTERVAL;
    runtime.tool_inputs.clear();
    runtime.active_since = Some(Instant::now());
}

pub(super) fn apply_events_result(
    state: &mut TuiState,
    runtime: &mut UiRuntime,
    commands: &Sender<ClientCommand>,
    result: EventsStreamResult,
) {
    runtime.poll_in_flight = false;
    runtime.next_offset = result.next_offset;
    let needs_catch_up =
        result.events.len() == EVENT_LIMIT && result.next_offset > result.from_offset;
    let active = matches!(
        result.status,
        RunStateName::Running | RunStateName::CancelRequested
    );
    runtime.polling = active || needs_catch_up;
    state.stream_warning = None;
    state.active_run = Some(ActiveRunView::new(result.run_id.clone(), result.status));
    for buffered in result.events {
        let event = &buffered.event;
        if let Some(model) = crate::model_from_event(event) {
            state.active_model = Some(model);
        }
        if let Some((call_id, input_preview)) = crate::tool_input_preview_from_event(event) {
            runtime
                .tool_inputs
                .insert(call_id.clone(), input_preview.clone());
            if let Some(approval) = state.approval.as_mut()
                && approval.tool_call_id == call_id
            {
                approval.input_preview = input_preview;
            }
        }
        if let Some(approval) = crate::approval_from_event(
            event,
            match event {
                StreamEvent::ApprovalRequested { tool_call_id, .. } => {
                    runtime.tool_inputs.get(tool_call_id).cloned()
                }
                _ => None,
            },
        ) {
            state.approval = Some(approval);
        }
        let line = crate::live_event_line(&buffered);
        let line = if line.run_id.is_some() {
            line
        } else {
            line.with_run_id(result.run_id.clone())
        };
        push_live_event(state, line);
    }
    if needs_catch_up {
        maybe_poll_events_now(runtime, commands);
    } else if !active {
        runtime.active_since = None;
        send_command(
            commands,
            ClientCommand::Load {
                run_id: Some(result.run_id),
            },
            state,
        );
        start_next_queued(commands, state, runtime);
    }
}

pub(super) fn maybe_poll_events(runtime: &mut UiRuntime, commands: &Sender<ClientCommand>) {
    if !runtime.polling || runtime.poll_in_flight {
        return;
    }
    if runtime.last_poll.elapsed() < ACTIVE_POLL_INTERVAL {
        return;
    }
    maybe_poll_events_now(runtime, commands);
}

fn maybe_poll_events_now(runtime: &mut UiRuntime, commands: &Sender<ClientCommand>) {
    let Some(run_id) = runtime.active_run_id.clone() else {
        return;
    };
    poll_events_from(runtime, commands, run_id, Some(runtime.next_offset));
}

fn poll_events_from(
    runtime: &mut UiRuntime,
    commands: &Sender<ClientCommand>,
    run_id: String,
    from_offset: Option<u64>,
) {
    if commands
        .send(ClientCommand::PollEvents {
            run_id,
            from_offset,
        })
        .is_ok()
    {
        runtime.poll_in_flight = true;
        runtime.last_poll = Instant::now();
    } else {
        runtime.polling = false;
    }
}

pub(super) fn is_connection_error(error: &ClientError) -> bool {
    match error {
        ClientError::Io(_) | ClientError::DaemonProtocol(_) => true,
        ClientError::DaemonResponse(error) => matches!(
            error.code.as_str(),
            ERROR_UNSUPPORTED_VERSION | ERROR_WORKSPACE_MISMATCH
        ),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConnectionState, TranscriptState, render_snapshot};
    use plato_daemon_client::transport;
    use plato_protocol::{
        ERROR_ISSUE_PREP_FAILED, Envelope, EnvelopeKind, HelloResult, PROTOCOL_VERSION,
        ProtocolError,
    };
    use serde_json::json;
    use std::{
        collections::VecDeque,
        io::{BufRead, BufReader, Write},
        path::PathBuf,
        sync::{
            Arc, Mutex,
            mpsc::{self, RecvTimeoutError},
        },
        thread::{self, JoinHandle},
    };

    const OUTER_WATCHDOG: Duration = Duration::from_secs(10);
    const DEADLINE_MARGIN: Duration = Duration::from_millis(100);

    #[test]
    fn two_session_identity_matrix_polls_only_the_selected_running_session() {
        let harness = ScriptedDaemon::start("two-session-identity", |workspace_id| {
            vec![
                hello_reply(workspace_id),
                ScriptedReply::result(
                    "sessions.list",
                    json!({
                        "sessions": [
                            {
                                "session_id": "session_finished",
                                "run_id": "run_finished",
                                "status": "finished",
                                "latest_question": "finished selected",
                                "ledger_path": "/work/agent.db"
                            },
                            {
                                "session_id": "session_running",
                                "run_id": "run_running",
                                "status": "running",
                                "latest_question": "other running",
                                "ledger_path": "/work/agent.db"
                            }
                        ]
                    }),
                ),
                ScriptedReply::result(
                    "transcript.read",
                    json!({
                        "run_id": "run_finished",
                        "status": "finished",
                        "final_answer": "selected answer",
                        "transcript": "[turn_finished] user: finished selected\n\
                                       [turn_finished] assistant: selected answer\n"
                    }),
                ),
                hello_reply(workspace_id),
                ScriptedReply::result(
                    "sessions.list",
                    json!({
                        "sessions": [
                            {
                                "session_id": "session_finished",
                                "run_id": "run_finished",
                                "status": "finished",
                                "latest_question": "finished selected",
                                "ledger_path": "/work/agent.db"
                            },
                            {
                                "session_id": "session_running",
                                "run_id": "run_running",
                                "status": "running",
                                "latest_question": "other running",
                                "ledger_path": "/work/agent.db"
                            }
                        ]
                    }),
                ),
                ScriptedReply::result(
                    "transcript.read",
                    json!({
                        "run_id": "run_running",
                        "status": "running",
                        "final_answer": null,
                        "transcript": "[turn_running] user: other running\n"
                    }),
                ),
            ]
        });

        let finished = load_connected_state(&harness.config, None, None).unwrap();
        assert_eq!(
            finished.selected_session_id.as_deref(),
            Some("session_finished")
        );
        assert!(finished.active_run.is_none());
        let finished_output = render_snapshot(&finished, 100, 24).unwrap();
        assert!(finished_output.contains("selected answer"));
        assert!(!finished_output.contains("run_running"));

        let (commands, command_receiver) = mpsc::channel();
        let mut finished_runtime = UiRuntime::from_state(&finished, None);
        finished_runtime.last_poll = Instant::now() - ACTIVE_POLL_INTERVAL;
        maybe_poll_events(&mut finished_runtime, &commands);
        assert!(command_receiver.try_recv().is_err());

        let running = load_connected_state(&harness.config, None, Some("session_running")).unwrap();
        assert_eq!(
            running.selected_session_id.as_deref(),
            Some("session_running")
        );
        assert_eq!(
            running
                .active_run
                .as_ref()
                .map(|active| active.run_id.as_str()),
            Some("run_running")
        );
        let mut running_runtime = UiRuntime::from_state(&running, None);
        running_runtime.last_poll = Instant::now() - ACTIVE_POLL_INTERVAL;
        maybe_poll_events(&mut running_runtime, &commands);
        assert!(matches!(
            command_receiver.try_recv().unwrap(),
            ClientCommand::PollEvents {
                run_id,
                from_offset: Some(0),
            } if run_id == "run_running"
        ));

        let requests = harness.finish();
        let transcript_targets = requests
            .iter()
            .filter(|request| request.method.as_deref() == Some("transcript.read"))
            .map(|request| {
                request
                    .params
                    .as_ref()
                    .and_then(|params| params.get("session_id"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            transcript_targets,
            vec!["session_finished", "session_running"]
        );
    }

    #[test]
    fn cancel_requested_remains_active_until_a_terminal_stream_status() {
        let workspace = tempfile::tempdir().unwrap();
        let config = DaemonConnectionConfig {
            workspace_root: workspace.path().to_owned(),
            socket_path: workspace.path().join("agent.sock"),
        };
        let mut state = connected_state(&config);
        state.active_run = Some(ActiveRunView::new(
            "run_canceling".into(),
            RunStateName::CancelRequested,
        ));
        let mut runtime = UiRuntime::from_state(&state, None);
        let (commands, command_receiver) = mpsc::channel();

        assert!(runtime.polling);
        apply_events_result(
            &mut state,
            &mut runtime,
            &commands,
            EventsStreamResult {
                run_id: "run_canceling".into(),
                from_offset: 0,
                next_offset: 0,
                status: RunStateName::CancelRequested,
                events: vec![],
            },
        );

        assert!(runtime.polling);
        assert!(runtime.active_since.is_some());
        assert_eq!(
            state.active_run.as_ref().map(|run| run.status),
            Some(RunStateName::CancelRequested)
        );
        assert!(command_receiver.try_recv().is_err());

        apply_events_result(
            &mut state,
            &mut runtime,
            &commands,
            EventsStreamResult {
                run_id: "run_canceling".into(),
                from_offset: 0,
                next_offset: 0,
                status: RunStateName::Canceled,
                events: vec![],
            },
        );

        assert!(!runtime.polling);
        assert!(runtime.active_since.is_none());
        assert!(matches!(
            command_receiver.try_recv().unwrap(),
            ClientCommand::Load {
                run_id: Some(run_id)
            } if run_id == "run_canceling"
        ));
    }

    #[test]
    fn lagged_reconnect_retains_pending_approval_through_failed_grant_retry() {
        assert_lagged_approval_retry(true);
    }

    #[test]
    fn lagged_reconnect_retains_pending_approval_through_failed_deny_retry() {
        assert_lagged_approval_retry(false);
    }

    #[test]
    fn operation_failure_state_matrix_is_exhaustive() {
        let workspace = tempfile::tempdir().unwrap();
        let config = DaemonConnectionConfig {
            workspace_root: workspace.path().to_owned(),
            socket_path: workspace.path().join("agent.sock"),
        };
        for operation in [
            ClientOperation::RunStart,
            ClientOperation::MessageAppend,
            ClientOperation::IssuePrepStart,
            ClientOperation::EventsStream,
            ClientOperation::ApprovalDecide,
            ClientOperation::RunCancel,
        ] {
            let (commands, _command_receiver) = mpsc::channel();
            let mut state = connected_state(&config);
            state.issue_prep_started_at = Some(Instant::now());
            state.cancel_requested = true;
            state.approval = Some(ApprovalModalView {
                run_id: "run_selected".into(),
                tool_call_id: "call_selected".into(),
                tool_name: "file.edit".into(),
                effect: "workspace_write".into(),
                reason: "review selected edit".into(),
                input_preview: "{}".into(),
                approval_preview: None,
                diff_preview: None,
            });
            let mut runtime = UiRuntime::from_state(&state, None);

            apply_event(
                &commands,
                ClientEvent::Failed {
                    operation,
                    error: ClientError::DaemonResponse(ProtocolError {
                        code: "test_failure".into(),
                        message: "expected failure".into(),
                    }),
                },
                &mut state,
                &mut runtime,
            );

            let expected_status = format!(
                "{} failed: daemon protocol error test_failure: expected failure",
                operation.method()
            );
            assert_eq!(
                state.status_message.as_deref(),
                Some(expected_status.as_str())
            );
            assert_eq!(
                state.cancel_requested,
                operation != ClientOperation::RunCancel
            );
            assert_eq!(
                state.issue_prep_started_at.is_none(),
                operation == ClientOperation::IssuePrepStart
            );
            assert_eq!(
                state.live_events.len(),
                usize::from(operation == ClientOperation::IssuePrepStart)
            );
            assert_eq!(
                state
                    .approval
                    .as_ref()
                    .map(|approval| approval.tool_call_id.as_str()),
                Some("call_selected")
            );
        }
    }

    fn assert_lagged_approval_retry(grant: bool) {
        let name = if grant {
            "lagged-grant-retry"
        } else {
            "lagged-deny-retry"
        };
        let harness = ScriptedDaemon::start(name, |workspace_id| {
            vec![
                hello_reply(workspace_id),
                ScriptedReply::result(
                    "sessions.list",
                    json!({
                        "sessions": [{
                            "session_id": "session_selected",
                            "run_id": "run_selected",
                            "status": "running",
                            "latest_question": "approve selected work",
                            "ledger_path": "/work/agent.db"
                        }]
                    }),
                ),
                ScriptedReply::result(
                    "transcript.read",
                    json!({
                        "run_id": "run_selected",
                        "status": "running",
                        "final_answer": null,
                        "transcript": "[turn_selected] user: approve selected work\n",
                        "pending_approval": {
                            "run_id": "run_selected",
                            "tool_call_id": "call_selected",
                            "tool_name": "file.edit",
                            "effect": "workspace_write",
                            "reason": "review selected edit",
                            "input_preview": "{\"path\":\"selected.txt\"}",
                            "approval_preview": "edit selected.txt",
                            "diff_preview": "-old selected\n+new selected\n"
                        }
                    }),
                ),
                hello_reply(workspace_id),
                ScriptedReply::error(
                    "events.stream",
                    ERROR_LAGGED,
                    "offset is no longer buffered",
                ),
                hello_reply(workspace_id),
                ScriptedReply::result(
                    "events.stream",
                    json!({
                        "run_id": "run_selected",
                        "from_offset": 12,
                        "next_offset": 12,
                        "status": "running",
                        "events": []
                    }),
                ),
                hello_reply(workspace_id),
                ScriptedReply::error(
                    "approval.decide",
                    "temporarily_unavailable",
                    "retry the exact decision",
                ),
                hello_reply(workspace_id),
                ScriptedReply::result(
                    "approval.decide",
                    json!({
                        "run_id": "run_selected",
                        "status": "running"
                    }),
                ),
            ]
        });
        let mut state = load_connected_state(&harness.config, None, None).unwrap();
        let approval = state.approval.clone().expect("approval snapshot");
        assert_eq!(approval.run_id, "run_selected");
        assert_eq!(approval.tool_call_id, "call_selected");
        assert_eq!(approval.tool_name, "file.edit");
        assert_eq!(approval.effect, "workspace_write");
        assert_eq!(approval.reason, "review selected edit");
        assert_eq!(approval.input_preview, r#"{"path":"selected.txt"}"#);
        assert_eq!(
            approval.approval_preview.as_deref(),
            Some("edit selected.txt")
        );
        assert_eq!(
            approval.diff_preview.as_deref(),
            Some("-old selected\n+new selected\n")
        );
        state.live_events.push(
            crate::LiveEventLine::warning(Some(11), "approval pending file.edit (workspace_write)")
                .with_run_id("run_selected"),
        );

        let (commands, events) = spawn_client_worker(harness.config.clone());
        let mut runtime = UiRuntime::from_state(&state, None);
        commands
            .send(ClientCommand::PollEvents {
                run_id: "run_selected".into(),
                from_offset: Some(0),
            })
            .unwrap();
        let lagged = events.recv_timeout(OUTER_WATCHDOG).unwrap();
        apply_event(&commands, lagged, &mut state, &mut runtime);
        let empty_tip = events.recv_timeout(OUTER_WATCHDOG).unwrap();
        apply_event(&commands, empty_tip, &mut state, &mut runtime);
        assert_eq!(state.approval.as_ref(), Some(&approval));

        let decision = || {
            if grant {
                ClientCommand::ApprovalGrant {
                    run_id: approval.run_id.clone(),
                    tool_call_id: approval.tool_call_id.clone(),
                }
            } else {
                ClientCommand::ApprovalDeny {
                    run_id: approval.run_id.clone(),
                    tool_call_id: approval.tool_call_id.clone(),
                    reason: "denied by plato-tui".into(),
                }
            }
        };
        commands.send(decision()).unwrap();
        let failed = events.recv_timeout(OUTER_WATCHDOG).unwrap();
        apply_event(&commands, failed, &mut state, &mut runtime);
        assert_eq!(state.approval.as_ref(), Some(&approval));
        assert!(
            !state
                .live_events
                .iter()
                .any(|event| event.kind == crate::LiveEventKind::Approval)
        );

        commands.send(decision()).unwrap();
        let succeeded = events.recv_timeout(OUTER_WATCHDOG).unwrap();
        assert_eq!(state.approval.as_ref(), Some(&approval));
        apply_event(&commands, succeeded, &mut state, &mut runtime);
        assert!(state.approval.is_none());
        assert!(state.live_events.iter().any(|event| {
            event.kind == crate::LiveEventKind::Approval
                && event.run_id.as_deref() == Some("run_selected")
                && event.text
                    == format!(
                        "approval {} call_selected",
                        if grant { "granted" } else { "denied" }
                    )
        }));
        let resolved = render_snapshot(&state, 100, 24).unwrap();
        assert!(resolved.contains("Trace  approval | running"));
        assert!(!resolved.contains("Trace  warning"));

        drop(commands);
        let requests = harness.finish();
        let stream_requests = requests
            .iter()
            .filter(|request| request.method.as_deref() == Some("events.stream"))
            .collect::<Vec<_>>();
        assert_eq!(stream_requests.len(), 2);
        assert_eq!(
            stream_requests[0].params.as_ref().unwrap()["from_offset"],
            0
        );
        assert!(
            stream_requests[1]
                .params
                .as_ref()
                .unwrap()
                .get("from_offset")
                .is_none(),
            "lag recovery must resume at the current tip"
        );
        let decisions = requests
            .iter()
            .filter(|request| request.method.as_deref() == Some("approval.decide"))
            .collect::<Vec<_>>();
        assert_eq!(decisions.len(), 2);
        for request in decisions {
            let params = request.params.as_ref().unwrap();
            assert_eq!(params["run_id"], "run_selected");
            assert_eq!(params["tool_call_id"], "call_selected");
            assert_eq!(params["decision"], if grant { "grant" } else { "deny" });
            if grant {
                assert_eq!(params["reason"], serde_json::Value::Null);
            } else {
                assert_eq!(params["reason"], "denied by plato-tui");
            }
        }
    }

    #[test]
    fn issue_prep_waits_past_short_deadline_for_delayed_success() {
        let harness =
            DelayedIssuePrepHarness::start("delayed-success", DelayedIssuePrepReply::Candidate);
        let (commands, events) = spawn_client_worker(harness.config.clone());
        let mut state = connected_state(&harness.config);
        state.issue_prep_started_at = Some(Instant::now());
        let mut runtime = UiRuntime::from_state(&state, None);
        commands
            .send(ClientCommand::IssuePrepStart {
                input: "prepare a bounded issue".into(),
                config_path: Some("plato.toml".into()),
            })
            .unwrap();

        let event = harness.wait_past_short_deadline(&events);
        apply_event(&commands, event, &mut state, &mut runtime);

        assert!(matches!(
            state.connection,
            ConnectionState::Connected { .. }
        ));
        assert!(state.issue_prep_started_at.is_none());
        assert_eq!(
            state.status_message.as_deref(),
            Some("issue ready; artifacts: /work/.plato/issue-prep/run_1")
        );
        let output = render_snapshot(&state, 100, 24).unwrap();
        assert!(output.contains("Prepared issue"));

        let live_events = state.live_events.clone();
        let (_sender, empty_events) = mpsc::channel();
        drain_client_events(&mut state, &mut runtime, &empty_events, &commands);
        assert!(state.issue_prep_started_at.is_none());
        assert_eq!(state.live_events, live_events);
    }

    #[test]
    fn issue_prep_waits_past_short_deadline_for_delayed_typed_error() {
        let harness =
            DelayedIssuePrepHarness::start("delayed-error", DelayedIssuePrepReply::TypedError);
        let (commands, events) = spawn_client_worker(harness.config.clone());
        let mut state = connected_state(&harness.config);
        state.issue_prep_started_at = Some(Instant::now());
        let mut runtime = UiRuntime::from_state(&state, None);
        commands
            .send(ClientCommand::IssuePrepStart {
                input: "prepare a bounded issue".into(),
                config_path: None,
            })
            .unwrap();

        let event = harness.wait_past_short_deadline(&events);
        apply_event(&commands, event, &mut state, &mut runtime);

        assert!(matches!(
            state.connection,
            ConnectionState::Connected { .. }
        ));
        assert!(state.issue_prep_started_at.is_none());
        assert_eq!(
            state.status_message.as_deref(),
            Some(
                "issue-prep.start failed: daemon protocol error issue_prep_failed: \
                 provider failed"
            )
        );
        assert_eq!(
            state
                .live_events
                .iter()
                .filter(|event| event.text.contains("provider failed"))
                .count(),
            1
        );
        let output = render_snapshot(&state, 100, 24).unwrap();
        assert!(output.contains("provider failed"));

        let live_events = state.live_events.clone();
        let (_sender, empty_events) = mpsc::channel();
        drain_client_events(&mut state, &mut runtime, &empty_events, &commands);
        assert!(state.issue_prep_started_at.is_none());
        assert_eq!(state.live_events, live_events);
    }

    fn apply_event(
        commands: &Sender<ClientCommand>,
        event: ClientEvent,
        state: &mut TuiState,
        runtime: &mut UiRuntime,
    ) {
        let (event_sender, event_receiver) = mpsc::channel();
        event_sender.send(event).unwrap();
        drain_client_events(state, runtime, &event_receiver, commands);
    }

    fn connected_state(config: &DaemonConnectionConfig) -> TuiState {
        TuiState::connected(
            config.workspace_root.to_string_lossy().into_owned(),
            config.socket_path.to_string_lossy().into_owned(),
            HelloResult {
                daemon_version: env!("CARGO_PKG_VERSION").into(),
                workspace_id: plato_daemon_client::paths::workspace_id(&config.workspace_root)
                    .unwrap(),
                ledger_path: "/work/agent.db".into(),
                capabilities: vec!["hello".into(), "issue-prep.start".into()],
            },
            Vec::new(),
            TranscriptState::None,
        )
    }

    enum ScriptedReply {
        Result {
            method: &'static str,
            result: serde_json::Value,
        },
        Error {
            method: &'static str,
            code: &'static str,
            message: &'static str,
        },
    }

    impl ScriptedReply {
        fn result(method: &'static str, result: serde_json::Value) -> Self {
            Self::Result { method, result }
        }

        fn error(method: &'static str, code: &'static str, message: &'static str) -> Self {
            Self::Error {
                method,
                code,
                message,
            }
        }

        fn method(&self) -> &'static str {
            match self {
                Self::Result { method, .. } | Self::Error { method, .. } => method,
            }
        }

        fn response(self, request: &Envelope) -> Envelope {
            match self {
                Self::Result { method, result } => {
                    Envelope::response(request.id.clone(), Some(method.into()), result)
                }
                Self::Error {
                    method,
                    code,
                    message,
                } => Envelope::error(request.id.clone(), Some(method.into()), code, message),
            }
        }
    }

    fn hello_reply(workspace_id: &str) -> ScriptedReply {
        ScriptedReply::result(
            "hello",
            json!({
                "daemon_version": env!("CARGO_PKG_VERSION"),
                "workspace_id": workspace_id,
                "ledger_path": "/work/agent.db",
                "capabilities": [
                    "hello",
                    "sessions.list",
                    "transcript.read",
                    "transcript.read.pending_approval",
                    "events.stream",
                    "approval.decide"
                ]
            }),
        )
    }

    struct ScriptedDaemon {
        config: DaemonConnectionConfig,
        requests: Arc<Mutex<Vec<Envelope>>>,
        server: JoinHandle<()>,
        _workspace: tempfile::TempDir,
        _endpoint: TestEndpoint,
    }

    impl ScriptedDaemon {
        fn start(name: &str, replies: impl FnOnce(&str) -> Vec<ScriptedReply>) -> Self {
            let workspace = tempfile::tempdir().unwrap();
            let endpoint = TestEndpoint::new(name);
            let listener = transport::bind(&endpoint.path).unwrap();
            let config =
                DaemonConnectionConfig::resolve(workspace.path(), Some(endpoint.path.clone()))
                    .unwrap();
            let workspace_id =
                plato_daemon_client::paths::workspace_id(&config.workspace_root).unwrap();
            let replies = VecDeque::from(replies(&workspace_id));
            let requests = Arc::new(Mutex::new(Vec::new()));
            let server_requests = Arc::clone(&requests);
            let server =
                thread::spawn(move || serve_scripted_daemon(listener, replies, server_requests));
            Self {
                config,
                requests,
                server,
                _workspace: workspace,
                _endpoint: endpoint,
            }
        }

        fn finish(self) -> Vec<Envelope> {
            self.server.join().unwrap();
            self.requests.lock().unwrap().clone()
        }
    }

    fn serve_scripted_daemon(
        listener: transport::Listener,
        mut replies: VecDeque<ScriptedReply>,
        requests: Arc<Mutex<Vec<Envelope>>>,
    ) {
        while !replies.is_empty() {
            let mut stream = transport::accept(&listener).unwrap();
            transport::set_deadline(&mut stream, Instant::now() + OUTER_WATCHDOG).unwrap();
            let mut reader = BufReader::new(transport::try_clone(&stream).unwrap());
            loop {
                let mut line = String::new();
                let read = reader.read_line(&mut line).unwrap();
                if read == 0 {
                    break;
                }
                let request: Envelope = serde_json::from_str(line.trim()).unwrap();
                let reply = replies.pop_front().expect("unexpected daemon request");
                assert_eq!(request.method.as_deref(), Some(reply.method()));
                let response = reply.response(&request);
                write_envelope(&mut stream, &response).unwrap();
                requests.lock().unwrap().push(request);
                if replies.is_empty() {
                    return;
                }
            }
        }
    }

    #[derive(Clone, Copy)]
    enum DelayedIssuePrepReply {
        Candidate,
        TypedError,
    }

    struct DelayedIssuePrepHarness {
        config: DaemonConnectionConfig,
        request_seen: Receiver<()>,
        release: Sender<()>,
        server: JoinHandle<()>,
        _workspace: tempfile::TempDir,
        _endpoint: TestEndpoint,
    }

    impl DelayedIssuePrepHarness {
        fn start(name: &str, reply: DelayedIssuePrepReply) -> Self {
            let workspace = tempfile::tempdir().unwrap();
            let endpoint = TestEndpoint::new(name);
            let listener = transport::bind(&endpoint.path).unwrap();
            let config =
                DaemonConnectionConfig::resolve(workspace.path(), Some(endpoint.path.clone()))
                    .unwrap();
            let workspace_id =
                plato_daemon_client::paths::workspace_id(&config.workspace_root).unwrap();
            let (request_seen_sender, request_seen) = mpsc::channel();
            let (release, release_receiver) = mpsc::channel();
            let server = thread::spawn(move || {
                let mut stream = transport::accept(&listener).unwrap();
                transport::set_deadline(&mut stream, Instant::now() + OUTER_WATCHDOG).unwrap();
                let mut reader = BufReader::new(transport::try_clone(&stream).unwrap());

                let hello = read_request(&mut reader);
                assert_eq!(hello.method.as_deref(), Some("hello"));
                write_envelope(
                    &mut stream,
                    &Envelope::response(
                        hello.id,
                        Some("hello".into()),
                        json!({
                            "daemon_version": env!("CARGO_PKG_VERSION"),
                            "workspace_id": workspace_id,
                            "ledger_path": "/work/agent.db",
                            "capabilities": ["hello", "issue-prep.start"]
                        }),
                    ),
                )
                .unwrap();

                let issue_prep = read_request(&mut reader);
                assert_eq!(issue_prep.method.as_deref(), Some("issue-prep.start"));
                assert_eq!(
                    issue_prep.params.as_ref().unwrap()["input"],
                    "prepare a bounded issue"
                );
                request_seen_sender.send(()).unwrap();
                release_receiver.recv_timeout(OUTER_WATCHDOG).unwrap();

                let response = match reply {
                    DelayedIssuePrepReply::Candidate => Envelope::response(
                        issue_prep.id,
                        Some("issue-prep.start".into()),
                        json!({
                            "run_dir": "/work/.plato/issue-prep/run_1",
                            "outcome": {
                                "status": "candidate",
                                "markdown": "# Prepared issue"
                            }
                        }),
                    ),
                    DelayedIssuePrepReply::TypedError => Envelope {
                        v: PROTOCOL_VERSION,
                        id: issue_prep.id,
                        kind: EnvelopeKind::Error,
                        method: Some("issue-prep.start".into()),
                        params: None,
                        result: None,
                        error: Some(ProtocolError {
                            code: ERROR_ISSUE_PREP_FAILED.into(),
                            message: "provider failed".into(),
                        }),
                    },
                };
                let _ = write_envelope(&mut stream, &response);
            });
            Self {
                config,
                request_seen,
                release,
                server,
                _workspace: workspace,
                _endpoint: endpoint,
            }
        }

        fn wait_past_short_deadline(self, events: &Receiver<ClientEvent>) -> ClientEvent {
            self.request_seen.recv_timeout(OUTER_WATCHDOG).unwrap();
            let premature = events.recv_timeout(DAEMON_CLIENT_TIMEOUT + DEADLINE_MARGIN);
            self.release.send(()).unwrap();
            let (crossed_deadline, event) = match premature {
                Err(RecvTimeoutError::Timeout) => {
                    (true, events.recv_timeout(OUTER_WATCHDOG).unwrap())
                }
                Ok(event) => (false, event),
                Err(error) => panic!("client event channel failed: {error}"),
            };
            let server_result = self.server.join();

            assert!(
                crossed_deadline,
                "issue prep completed before release: {event:?}"
            );
            server_result.unwrap();
            event
        }
    }

    fn read_request(reader: &mut BufReader<transport::Stream>) -> Envelope {
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        serde_json::from_str(line.trim()).unwrap()
    }

    fn write_envelope(writer: &mut transport::Stream, envelope: &Envelope) -> std::io::Result<()> {
        let bytes = serde_json::to_vec(envelope).unwrap();
        writer.write_all(&bytes)?;
        writer.write_all(b"\n")?;
        writer.flush()
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
                        r"\\.\pipe\plato-agent-tui-{name}-{}",
                        std::process::id()
                    )),
                    _directory: None,
                }
            }
        }
    }
}
