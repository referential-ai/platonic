use crate::{
    AppError, AppResult,
    daemon::client::{DaemonClient, DaemonConnectionConfig},
    daemon::protocol::{
        CommandAcceptedResult, ERROR_LAGGED, ERROR_OVERLOAD, ERROR_UNSUPPORTED_VERSION,
        ERROR_WORKSPACE_MISMATCH, EventsStreamResult, IssuePrepResult, IssuePrepStartResult,
        RunStartResult, RunStateName, StreamEvent,
    },
    tui::{ActiveRunView, TranscriptState, TranscriptView, TuiState},
};
use std::{
    collections::HashMap,
    sync::mpsc::{self, Receiver, Sender},
    thread,
    time::{Duration, Instant},
};

use super::app::{push_live_event, send_command, start_next_queued};

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
) -> AppResult<TuiState> {
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
    let transcript = if let Some(session_id) = session_id.or(selected_session_id.as_deref()) {
        match client.transcript_read_session(session_id) {
            Ok(transcript) => TranscriptState::Loaded(TranscriptView::from(transcript)),
            Err(error) => TranscriptState::Unavailable {
                run_id: session_id.to_owned(),
                error: error.to_string(),
            },
        }
    } else {
        match run_id {
            Some(run_id) => match client.transcript_read(run_id) {
                Ok(transcript) => TranscriptState::Loaded(TranscriptView::from(transcript)),
                Err(error) => TranscriptState::Unavailable {
                    run_id: run_id.to_owned(),
                    error: error.to_string(),
                },
            },
            None => TranscriptState::None,
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
    let active_session = state
        .selected_session_id
        .as_deref()
        .and_then(|session_id| {
            state.sessions.iter().find(|session| {
                session.session_id == session_id && session.status == RunStateName::Running
            })
        })
        .or_else(|| {
            state
                .sessions
                .iter()
                .find(|session| session.status == RunStateName::Running)
        });
    if let Some(session) = active_session {
        state.active_run = Some(ActiveRunView::new(session.run_id.clone(), session.status));
    }
    Ok(state)
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
            polling: state
                .active_run
                .as_ref()
                .is_some_and(|run| run.status == RunStateName::Running),
            last_poll: Instant::now(),
            tool_inputs: HashMap::new(),
            active_since: state.active_run.as_ref().map(|_| Instant::now()),
        }
    }

    fn sync_from_state(&mut self, state: &TuiState) {
        self.active_run_id = state.active_run.as_ref().map(|run| run.run_id.clone());
        self.polling = state
            .active_run
            .as_ref()
            .is_some_and(|run| run.status == RunStateName::Running);
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
    RunStarted(RunStartResult),
    IssuePrepFinished(IssuePrepStartResult),
    EventsPolled(EventsStreamResult),
    ApprovalDecided(CommandAcceptedResult),
    RunCanceled(CommandAcceptedResult),
    Failed {
        context: &'static str,
        error: crate::AppError,
    },
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
        ClientCommand::RunStart {
            question,
            config_path,
        } => with_client(config, |client| {
            client.run_start(question, config_path, false)
        })
        .map_or_else(failed_event("run.start"), ClientEvent::RunStarted),
        ClientCommand::MessageAppend {
            message,
            session_id,
            config_path,
        } => with_client(config, |client| {
            client.message_append_to_session(message, Some(session_id), config_path, false)
        })
        .map_or_else(failed_event("message.append"), ClientEvent::RunStarted),
        ClientCommand::IssuePrepStart { input, config_path } => {
            with_client(config, |client| client.issue_prep_start(input, config_path)).map_or_else(
                failed_event("issue-prep.start"),
                ClientEvent::IssuePrepFinished,
            )
        }
        ClientCommand::PollEvents {
            run_id,
            from_offset,
        } => with_client(config, |client| {
            client.events_stream(&run_id, from_offset, EVENT_LIMIT)
        })
        .map_or_else(failed_event("events.stream"), ClientEvent::EventsPolled),
        ClientCommand::ApprovalGrant {
            run_id,
            tool_call_id,
        } => with_client(config, |client| {
            client.approval_grant(&run_id, &tool_call_id)
        })
        .map_or_else(
            failed_event("approval.decide"),
            ClientEvent::ApprovalDecided,
        ),
        ClientCommand::ApprovalDeny {
            run_id,
            tool_call_id,
            reason,
        } => with_client(config, |client| {
            client.approval_deny(&run_id, &tool_call_id, reason)
        })
        .map_or_else(
            failed_event("approval.decide"),
            ClientEvent::ApprovalDecided,
        ),
        ClientCommand::RunCancel { run_id } => {
            with_client(config, |client| client.run_cancel(&run_id))
                .map_or_else(failed_event("run.cancel"), ClientEvent::RunCanceled)
        }
    }
}

fn with_client<T>(
    config: &DaemonConnectionConfig,
    run: impl FnOnce(&mut DaemonClient) -> AppResult<T>,
) -> AppResult<T> {
    let mut client = connect_daemon(config, DAEMON_CLIENT_TIMEOUT)?;
    client.hello(&config.workspace_root)?;
    run(&mut client)
}

pub(super) fn connect_daemon(
    config: &DaemonConnectionConfig,
    timeout: Duration,
) -> AppResult<DaemonClient> {
    DaemonClient::connect_with_timeout(&config.socket_path, timeout)
}

fn failed_event(context: &'static str) -> impl FnOnce(crate::AppError) -> ClientEvent {
    move |error| ClientEvent::Failed { context, error }
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
            ClientEvent::RunStarted(result) => {
                apply_run_response(state, runtime, result, "run started")
            }
            ClientEvent::IssuePrepFinished(result) => {
                state.issue_prep_started_at = None;
                match result.outcome {
                    IssuePrepResult::Candidate { markdown } => {
                        push_live_event(
                            state,
                            crate::tui::LiveEventLine::assistant(None, markdown),
                        );
                        push_live_event(
                            state,
                            crate::tui::LiveEventLine::status(
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
                            crate::tui::LiveEventLine::warning(
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
                state.scroll_offset = 0;
                start_next_queued(commands, state, runtime);
            }
            ClientEvent::EventsPolled(result) => {
                apply_events_result(state, runtime, commands, result)
            }
            ClientEvent::ApprovalDecided(result) => {
                state.status_message =
                    Some(format!("approval decision sent for {}", result.run_id));
                state.approval = None;
                state.active_run = Some(ActiveRunView::new(result.run_id, result.status));
            }
            ClientEvent::RunCanceled(result) => {
                state.status_message = Some(format!("cancel requested for {}", result.run_id));
                state.cancel_requested = true;
                state.approval = None;
                state.active_run = Some(ActiveRunView::new(result.run_id.clone(), result.status));
                push_live_event(
                    state,
                    crate::tui::LiveEventLine::status(
                        None,
                        format!("cancel requested: {}", result.run_id),
                    ),
                );
            }
            ClientEvent::Failed { context, error } => {
                runtime.poll_in_flight = false;
                let connection_error = is_connection_error(&error);
                let lagged = matches!(
                    &error,
                    AppError::DaemonResponse(error) if error.code == ERROR_LAGGED
                );
                let overloaded = matches!(
                    &error,
                    AppError::DaemonResponse(error) if error.code == ERROR_OVERLOAD
                );
                let message = error.to_string();
                if context == "events.stream" && lagged {
                    state.stream_warning = Some(format!("{message}; resuming at current tip"));
                    if let Some(run_id) = runtime.active_run_id.clone() {
                        poll_events_from(runtime, commands, run_id, None);
                    }
                } else if context == "events.stream" && overloaded {
                    state.stream_warning = Some(message);
                } else {
                    if connection_error {
                        runtime.polling = false;
                        state.connection = crate::tui::ConnectionState::Disconnected {
                            error: message.clone(),
                        };
                    }
                    if context == "run.cancel" {
                        state.cancel_requested = false;
                    }
                    let failure = format!("{context} failed: {message}");
                    state.status_message = Some(failure.clone());
                    if context == "issue-prep.start" {
                        state.issue_prep_started_at = None;
                        push_live_event(state, crate::tui::LiveEventLine::warning(None, failure));
                        if !connection_error {
                            start_next_queued(commands, state, runtime);
                        }
                    }
                }
            }
        }
    }
}

pub(super) fn apply_loaded_state(state: &mut TuiState, mut loaded: TuiState) {
    loaded.composer = std::mem::take(&mut state.composer);
    loaded.composer_cursor = state.composer_cursor;
    loaded.composer_kill_buffer = state.composer_kill_buffer.clone();
    loaded.slash_popup = state.slash_popup.clone();
    loaded.queued_messages = std::mem::take(&mut state.queued_messages);
    loaded.issue_prep_started_at = state.issue_prep_started_at;
    loaded.input_history = std::mem::take(&mut state.input_history);
    loaded.history_index = state.history_index;
    loaded.help_visible = state.help_visible;
    if loaded.status_message.is_none() {
        loaded.status_message = state.status_message.clone();
    }
    if loaded.stream_warning.is_none() {
        loaded.stream_warning = state.stream_warning.clone();
    }
    if loaded.live_events.is_empty() {
        loaded.live_events = std::mem::take(&mut state.live_events);
        loaded.history_rows.live_events = std::mem::take(&mut state.history_rows.live_events);
    }
    loaded.scroll_offset = state.scroll_offset;
    if loaded.active_model.is_none() {
        loaded.active_model = state.active_model.clone();
    }
    if loaded.active_run_elapsed_secs.is_none() {
        loaded.active_run_elapsed_secs = state.active_run_elapsed_secs;
    }
    if loaded.active_run.as_ref().map(|run| &run.run_id)
        == state.active_run.as_ref().map(|run| &run.run_id)
    {
        loaded.cancel_requested = state.cancel_requested;
    }
    *state = loaded;
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
    push_live_event(
        state,
        crate::tui::LiveEventLine::status(None, format!("{message}: {run_id}")),
    );
    state.scroll_offset = 0;
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
    runtime.polling = result.status == RunStateName::Running || needs_catch_up;
    state.stream_warning = None;
    state.active_run = Some(ActiveRunView::new(result.run_id.clone(), result.status));
    for buffered in result.events {
        let event = &buffered.event;
        if let Some(model) = crate::tui::model_from_event(event) {
            state.active_model = Some(model);
        }
        if let Some((call_id, input_preview)) = crate::tui::tool_input_preview_from_event(event) {
            runtime
                .tool_inputs
                .insert(call_id.clone(), input_preview.clone());
            if let Some(approval) = state.approval.as_mut()
                && approval.tool_call_id == call_id
            {
                approval.input_preview = input_preview;
            }
        }
        if let Some(approval) = crate::tui::approval_from_event(
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
        let line = crate::tui::live_event_line(&buffered);
        push_live_event(state, line);
    }
    if needs_catch_up {
        maybe_poll_events_now(runtime, commands);
    } else if result.status != RunStateName::Running {
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

pub(super) fn is_connection_error(error: &AppError) -> bool {
    match error {
        AppError::Io(_) | AppError::DaemonLockHeld { .. } | AppError::DaemonProtocol(_) => true,
        AppError::DaemonResponse(error) => matches!(
            error.code.as_str(),
            ERROR_UNSUPPORTED_VERSION | ERROR_WORKSPACE_MISMATCH
        ),
        _ => false,
    }
}
