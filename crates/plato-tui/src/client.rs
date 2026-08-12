use crate::{
    ActiveRunView, ApprovalModalView, ThreadAttachment, TranscriptState, TranscriptView, TuiState,
    VoiceControl, VoiceControlRequest, VoiceControlResponse,
};
use platonic_client::{
    ClientError, ClientResult,
    client::{DaemonClient, DaemonConnectionConfig},
};
use platonic_protocol::{
    ApprovalDecisionName, ApprovalProfile, BufferedStreamEvent, CommandAcceptedResult,
    DaemonStatusResult, ERROR_LAGGED, ERROR_OVERLOAD, ERROR_UNSUPPORTED_VERSION,
    ERROR_WORKSPACE_MISMATCH, EventsStreamResult, HarnessEvent, IssuePrepResult,
    IssuePrepStartResult, RunOverrides, RunStartResult, RunStateName,
    SessionApprovalProfileSetResult, StreamEvent, ThreadEventsResult, ThreadListResult,
    ThreadSendResult,
};
use std::{
    collections::HashMap,
    ops::Deref,
    sync::mpsc::{self, Sender},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[cfg(test)]
use std::sync::mpsc::Receiver;

use super::{
    app::{
        UiEvent, push_live_event, push_live_event_at, select_fresh_session, send_command,
        start_next_queued,
    },
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

pub(super) fn load_thread_state(
    config: &DaemonConnectionConfig,
    attachment: &ThreadAttachment,
) -> TuiState {
    match load_connected_thread_state(config, attachment) {
        Ok(state) => state,
        Err(error) => TuiState::disconnected(
            config.workspace_root.to_string_lossy().into_owned(),
            config.socket_path.to_string_lossy().into_owned(),
            error.to_string(),
        ),
    }
}

fn load_connected_thread_state(
    config: &DaemonConnectionConfig,
    attachment: &ThreadAttachment,
) -> ClientResult<TuiState> {
    let mut client = connect_daemon(config, DAEMON_CLIENT_TIMEOUT)?;
    let hello = client.hello(&config.workspace_root)?;
    let status = client.thread_status(attachment.thread_id.clone())?;
    let sessions = client.sessions_list()?;
    let session_id = format!("session_{}", attachment.thread_id);
    let approval_profile = client
        .daemon_status(Some(session_id.clone()), None)?
        .trust
        .approval_profile;
    let (transcript, approval) = if sessions
        .iter()
        .any(|session| session.session_id == session_id)
    {
        match client.transcript_read_session(&session_id) {
            Ok(transcript) => loaded_transcript_state(transcript),
            Err(error) => (
                TranscriptState::Unavailable {
                    run_id: session_id.clone(),
                    error: error.to_string(),
                },
                None,
            ),
        }
    } else {
        (TranscriptState::None, None)
    };
    let mut state = TuiState::connected(
        config.workspace_root.to_string_lossy().into_owned(),
        config.socket_path.to_string_lossy().into_owned(),
        hello,
        sessions,
        transcript,
    );
    state.selected_session_id = Some(session_id.clone());
    state.selected_thread_id = Some(attachment.thread_id.clone());
    state.approval_profile = approval_profile;
    state.approval = approval;
    state.status_message = Some(format!("attached to thread {}", attachment.thread_id));
    if status.thread.live.current_turn_id.is_some()
        && let Some(session) = state
            .sessions
            .iter()
            .find(|session| session.session_id == session_id)
    {
        state.active_run = Some(ActiveRunView::new(session.run_id.clone(), session.status));
    }
    Ok(state)
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
    state.approval_profile = match state.selected_session_id.as_deref() {
        Some(session_id) => {
            client
                .daemon_status(Some(session_id.into()), None)?
                .trust
                .approval_profile
        }
        None => ApprovalProfile::Prompt,
    };
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
    transcript: platonic_protocol::TranscriptReadResult,
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

#[derive(Debug)]
pub(super) struct UiRuntime {
    pub(super) active_run_id: Option<String>,
    pub(super) config_path: Option<String>,
    pub(super) next_offset: u64,
    pub(super) poll_in_flight: bool,
    pub(super) polling: bool,
    pub(super) last_poll: Instant,
    pub(super) tool_inputs: HashMap<String, String>,
    pub(super) active_timer: ActiveTimer,
    pub(super) thread: Option<ThreadAttachment>,
    pub(super) thread_next_offset: Option<u64>,
    pub(super) thread_turn_id: Option<String>,
}

#[derive(Debug, Default)]
pub(super) struct ActiveTimer {
    accumulated: Duration,
    running_since: Option<Instant>,
    paused: bool,
}

impl ActiveTimer {
    pub(super) fn started_at(now: Instant, elapsed: Duration) -> Self {
        Self {
            accumulated: elapsed,
            running_since: Some(now),
            paused: false,
        }
    }

    fn from_state_at(state: &TuiState, now: Instant) -> Self {
        let mut timer = Self::default();
        if state.active_run.is_some() {
            timer.start_at(
                now,
                Duration::from_secs(state.active_run_elapsed_secs.unwrap_or(0)),
            );
            timer.set_paused_at(state.approval.is_some(), now);
        }
        timer
    }

    pub(super) fn start_at(&mut self, now: Instant, elapsed: Duration) {
        *self = Self::started_at(now, elapsed);
    }

    pub(super) fn stop(&mut self) {
        *self = Self::default();
    }

    pub(super) fn set_paused_at(&mut self, paused: bool, now: Instant) {
        if paused == self.paused || !self.is_active() {
            return;
        }
        if paused {
            if let Some(started) = self.running_since.take() {
                self.accumulated += now.saturating_duration_since(started);
            }
        } else {
            self.running_since = Some(now);
        }
        self.paused = paused;
    }

    pub(super) fn elapsed_at(&self, now: Instant) -> Option<Duration> {
        self.is_active().then(|| {
            self.accumulated
                + self
                    .running_since
                    .map(|started| now.saturating_duration_since(started))
                    .unwrap_or_default()
        })
    }

    pub(super) fn is_active(&self) -> bool {
        self.running_since.is_some() || self.paused
    }
}

impl UiRuntime {
    pub(super) fn from_state(state: &TuiState, config_path: Option<String>) -> Self {
        let now = Instant::now();
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
            last_poll: now,
            tool_inputs: HashMap::new(),
            active_timer: ActiveTimer::from_state_at(state, now),
            thread: None,
            thread_next_offset: None,
            thread_turn_id: None,
        }
    }

    pub(super) fn attach_thread(&mut self, attachment: Option<ThreadAttachment>) {
        self.thread = attachment;
        self.thread_next_offset = None;
        self.thread_turn_id = None;
        if self.thread.is_some() {
            self.polling = true;
            self.last_poll = Instant::now() - ACTIVE_POLL_INTERVAL;
        }
    }

    fn sync_from_state(&mut self, state: &TuiState) {
        self.active_run_id = state.active_run.as_ref().map(|run| run.run_id.clone());
        self.polling = self.thread.is_some()
            || state.active_run.as_ref().is_some_and(|run| {
                matches!(
                    run.status,
                    RunStateName::Running | RunStateName::CancelRequested
                )
            });
        self.next_offset = 0;
        self.thread_next_offset = None;
        self.thread_turn_id = None;
        self.poll_in_flight = false;
        let now = Instant::now();
        self.last_poll = now;
        self.tool_inputs.clear();
        self.active_timer = ActiveTimer::from_state_at(state, now);
    }

    pub(super) fn poll_deadline(&self) -> Option<Instant> {
        (self.polling
            && !self.poll_in_flight
            && (self.active_run_id.is_some() || self.thread.is_some()))
        .then_some(self.last_poll + ACTIVE_POLL_INTERVAL)
    }

    pub(super) fn is_thread_attached(&self) -> bool {
        self.thread.is_some()
    }

    pub(super) fn thread_send_command(&self, message: String) -> Option<ClientCommand> {
        let thread = self.thread.as_ref()?;
        Some(ClientCommand::ThreadSend {
            thread_id: thread.thread_id.clone(),
            controller_id: thread.controller_id.clone(),
            turn_id: self.thread_turn_id.clone(),
            message,
        })
    }
}

#[derive(Debug)]
pub(super) enum ClientCommand {
    Load {
        run_id: Option<String>,
    },
    ThreadList,
    LoadThread {
        attachment: ThreadAttachment,
    },
    DaemonStatus {
        session_id: Option<String>,
        config_path: Option<String>,
    },
    RunStart {
        question: String,
        config_path: Option<String>,
        approval_profile: ApprovalProfile,
    },
    MessageAppend {
        message: String,
        session_id: String,
        config_path: Option<String>,
        approval_profile: Option<ApprovalProfile>,
    },
    ApprovalProfileSet {
        session_id: String,
        profile: ApprovalProfile,
    },
    VoiceSet {
        enabled: bool,
    },
    VoiceResetForNewSession,
    ThreadSend {
        thread_id: String,
        controller_id: String,
        turn_id: Option<String>,
        message: String,
    },
    IssuePrepStart {
        input: String,
        config_path: Option<String>,
    },
    PollEvents {
        run_id: String,
        from_offset: Option<u64>,
    },
    PollThreadEvents {
        thread_id: String,
        from_offset: Option<u64>,
    },
    ApprovalGrant {
        run_id: String,
        tool_call_id: String,
    },
    ApprovalGrantSession {
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
    ThreadLoaded {
        state: Box<TuiState>,
        attachment: ThreadAttachment,
    },
    ThreadsLoaded(ThreadListResult),
    StatusLoaded(Box<DaemonStatusResult>),
    ApprovalProfileSet(SessionApprovalProfileSetResult),
    VoiceSet(VoiceControlResponse),
    VoiceResetForNewSession(VoiceControlResponse),
    RunStarted(RunStartResult),
    ThreadSent(ThreadSendResult),
    IssuePrepFinished(IssuePrepStartResult),
    EventsPolled(EventsStreamResult),
    ThreadEventsPolled(ThreadEventsResult),
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
    ThreadList,
    ApprovalProfileSet,
    RunStart,
    MessageAppend,
    ThreadSend,
    IssuePrepStart,
    EventsStream,
    ThreadEvents,
    ApprovalDecide,
    RunCancel,
}

impl ClientOperation {
    fn method(self) -> &'static str {
        match self {
            Self::DaemonStatus => "daemon.status",
            Self::ThreadList => "thread.list",
            Self::ApprovalProfileSet => "session.approval_profile.set",
            Self::RunStart => "run.start",
            Self::MessageAppend => "message.append",
            Self::ThreadSend => "thread.send",
            Self::IssuePrepStart => "issue-prep.start",
            Self::EventsStream => "events.stream",
            Self::ThreadEvents => "thread.events",
            Self::ApprovalDecide => "approval.decide",
            Self::RunCancel => "run.cancel",
        }
    }
}

#[cfg(test)]
pub(super) fn spawn_client_worker(
    config: DaemonConnectionConfig,
) -> (Sender<ClientCommand>, Receiver<ClientEvent>) {
    let (command_sender, command_receiver) = mpsc::channel();
    let (event_sender, event_receiver) = mpsc::channel();
    thread::spawn(move || {
        for command in command_receiver {
            let event = handle_client_command(&config, None, None, command);
            if event_sender.send(event).is_err() {
                break;
            }
        }
    });
    (command_sender, event_receiver)
}

pub(super) fn spawn_client_worker_to(
    config: DaemonConnectionConfig,
    attachment: Option<ThreadAttachment>,
    voice: Option<VoiceControl>,
    event_sender: Sender<UiEvent>,
) -> ClientWorker {
    let (command_sender, command_receiver) = mpsc::channel();
    let worker = thread::spawn(move || {
        for command in command_receiver {
            let event =
                handle_client_command(&config, attachment.as_ref(), voice.as_ref(), command);
            if event_sender.send(UiEvent::Daemon(Box::new(event))).is_err() {
                break;
            }
        }
    });
    ClientWorker {
        commands: Some(command_sender),
        worker: Some(worker),
    }
}

pub(super) struct ClientWorker {
    commands: Option<Sender<ClientCommand>>,
    worker: Option<JoinHandle<()>>,
}

impl Deref for ClientWorker {
    type Target = Sender<ClientCommand>;

    fn deref(&self) -> &Self::Target {
        self.commands
            .as_ref()
            .expect("live client worker retains its command sender")
    }
}

impl Drop for ClientWorker {
    fn drop(&mut self) {
        self.commands.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn handle_client_command(
    config: &DaemonConnectionConfig,
    attachment: Option<&ThreadAttachment>,
    voice: Option<&VoiceControl>,
    command: ClientCommand,
) -> ClientEvent {
    match command {
        ClientCommand::Load { run_id } => ClientEvent::Loaded(Box::new(match attachment {
            Some(attachment) => load_thread_state(config, attachment),
            None => load_state(config, run_id.as_deref()),
        })),
        ClientCommand::ThreadList => with_client(config, DaemonClient::thread_list).map_or_else(
            failed_event(ClientOperation::ThreadList),
            ClientEvent::ThreadsLoaded,
        ),
        ClientCommand::LoadThread { attachment } => ClientEvent::ThreadLoaded {
            state: Box::new(load_thread_state(config, &attachment)),
            attachment,
        },
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
            approval_profile,
        } => with_client(config, |client| {
            client.run_start_with_overrides_and_profile(
                question,
                config_path,
                RunOverrides::default(),
                Some(approval_profile),
                false,
            )
        })
        .map_or_else(
            failed_event(ClientOperation::RunStart),
            ClientEvent::RunStarted,
        ),
        ClientCommand::MessageAppend {
            message,
            session_id,
            config_path,
            approval_profile,
        } => with_client(config, |client| {
            client.message_append_to_session_with_overrides_and_profile(
                message,
                Some(session_id),
                config_path,
                RunOverrides::default(),
                approval_profile,
                false,
            )
        })
        .map_or_else(
            failed_event(ClientOperation::MessageAppend),
            ClientEvent::RunStarted,
        ),
        ClientCommand::ApprovalProfileSet {
            session_id,
            profile,
        } => with_client(config, |client| {
            client.session_approval_profile_set(session_id, profile)
        })
        .map_or_else(
            failed_event(ClientOperation::ApprovalProfileSet),
            ClientEvent::ApprovalProfileSet,
        ),
        ClientCommand::VoiceSet { enabled } => ClientEvent::VoiceSet(match voice {
            Some(voice) => voice.request(if enabled {
                VoiceControlRequest::Enable
            } else {
                VoiceControlRequest::Disable
            }),
            None => VoiceControlResponse::Failed(
                "voice configuration is unavailable: no client voice control".into(),
            ),
        }),
        ClientCommand::VoiceResetForNewSession => {
            ClientEvent::VoiceResetForNewSession(match voice {
                Some(voice) => voice.request(VoiceControlRequest::Disable),
                None => VoiceControlResponse::AlreadyDisabled,
            })
        }
        ClientCommand::ThreadSend {
            thread_id,
            controller_id,
            turn_id,
            message,
        } => with_client(config, |client| {
            client.thread_send(thread_id, controller_id, turn_id, message)
        })
        .map_or_else(
            failed_event(ClientOperation::ThreadSend),
            ClientEvent::ThreadSent,
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
        ClientCommand::PollThreadEvents {
            thread_id,
            from_offset,
        } => with_client(config, |client| {
            client.thread_events(thread_id, from_offset, EVENT_LIMIT, 0)
        })
        .map_or_else(
            failed_event(ClientOperation::ThreadEvents),
            ClientEvent::ThreadEventsPolled,
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
        ClientCommand::ApprovalGrantSession {
            run_id,
            tool_call_id,
        } => {
            let result = with_client(config, |client| {
                client.approval_grant_session(&run_id, &tool_call_id)
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

#[cfg(test)]
pub(super) fn drain_client_events(
    state: &mut TuiState,
    runtime: &mut UiRuntime,
    events: &Receiver<ClientEvent>,
    commands: &Sender<ClientCommand>,
) {
    while let Ok(event) = events.try_recv() {
        apply_client_event(state, runtime, event, commands);
    }
}

pub(super) fn apply_client_event(
    state: &mut TuiState,
    runtime: &mut UiRuntime,
    event: ClientEvent,
    commands: &Sender<ClientCommand>,
) {
    match event {
        ClientEvent::Loaded(loaded) => {
            apply_loaded_state(state, *loaded);
            runtime.sync_from_state(state);
        }
        ClientEvent::ThreadLoaded {
            state: loaded,
            attachment,
        } => {
            apply_loaded_state(state, *loaded);
            runtime.attach_thread(Some(attachment));
            runtime.sync_from_state(state);
        }
        ClientEvent::ThreadsLoaded(result) => {
            state.threads = result.threads;
            if let Some(picker) = state.session_picker.as_mut() {
                picker.selected = state
                    .selected_thread_id
                    .as_deref()
                    .and_then(|thread_id| {
                        state
                            .threads
                            .iter()
                            .position(|thread| thread.authority.thread_id == thread_id)
                    })
                    .unwrap_or(0)
                    .min(state.threads.len().saturating_sub(1));
            }
            state.status_message = Some("thread picker loaded".into());
        }
        ClientEvent::StatusLoaded(status) => {
            if state.selected_session_id.is_some()
                && status.session.session_id == state.selected_session_id
            {
                state.approval_profile = status.trust.approval_profile;
            }
            state.status_modal = Some(*status);
            state.status_message = Some("status opened".into());
        }
        ClientEvent::ApprovalProfileSet(result) => {
            if state.selected_session_id.as_deref() == Some(result.session_id.as_str()) {
                state.approval_profile = result.profile;
                state.status_message =
                    Some(format!("session approval profile: {}", result.profile));
            } else {
                state.status_message = Some(format!(
                    "session {} approval profile: {}",
                    result.session_id, result.profile
                ));
            }
        }
        ClientEvent::VoiceSet(response) => apply_voice_response(state, response),
        ClientEvent::VoiceResetForNewSession(response) => match response {
            VoiceControlResponse::Disabled | VoiceControlResponse::AlreadyDisabled => {
                select_fresh_session(state);
            }
            response => {
                apply_voice_response(state, response);
            }
        },
        ClientEvent::RunStarted(result) => {
            apply_run_response(state, runtime, result, "run started")
        }
        ClientEvent::ThreadSent(result) => apply_thread_send_result(state, runtime, result),
        ClientEvent::IssuePrepFinished(result) => {
            state.issue_prep_started_at = None;
            state.issue_prep_elapsed_secs = None;
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
            start_next_queued(commands, state, runtime);
        }
        ClientEvent::EventsPolled(result) => apply_events_result(state, runtime, commands, result),
        ClientEvent::ThreadEventsPolled(result) => {
            apply_thread_events_result(state, runtime, result)
        }
        ClientEvent::ApprovalDecided {
            result,
            tool_call_id,
            decision,
        } => {
            state.status_message = Some(format!("approval decision sent for {}", result.run_id));
            state.approval = None;
            state.approval_scroll_offset = 0;
            state.active_run = Some(ActiveRunView::new(result.run_id.clone(), result.status));
            let decision = match decision {
                ApprovalDecisionName::Granted => "granted",
                ApprovalDecisionName::Denied => "denied",
            };
            push_live_event(
                state,
                crate::LiveEventLine::approval(None, format!("approval {decision} {tool_call_id}"))
                    .with_run_id(result.run_id),
            );
        }
        ClientEvent::RunCanceled(result) => {
            state.status_message = Some(format!("cancel requested for {}", result.run_id));
            state.cancel_requested = true;
            state.approval = None;
            state.approval_scroll_offset = 0;
            state.active_run = Some(ActiveRunView::new(result.run_id.clone(), result.status));
            push_live_event(
                state,
                crate::LiveEventLine::status(None, format!("cancel requested: {}", result.run_id))
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
            if matches!(
                operation,
                ClientOperation::EventsStream | ClientOperation::ThreadEvents
            ) && lagged
            {
                state.stream_warning = Some(format!("{message}; resuming at current tip"));
                if operation == ClientOperation::ThreadEvents {
                    poll_thread_events_from(runtime, commands, None);
                } else if let Some(run_id) = runtime.active_run_id.clone() {
                    poll_events_from(runtime, commands, run_id, None);
                }
            } else if matches!(
                operation,
                ClientOperation::EventsStream | ClientOperation::ThreadEvents
            ) && overloaded
            {
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
                        state.issue_prep_elapsed_secs = None;
                        push_live_event(state, crate::LiveEventLine::warning(None, failure));
                        if !connection_error {
                            start_next_queued(commands, state, runtime);
                        }
                    }
                    ClientOperation::RunStart
                    | ClientOperation::MessageAppend
                    | ClientOperation::ThreadList
                    | ClientOperation::ApprovalProfileSet
                    | ClientOperation::ThreadSend
                    | ClientOperation::EventsStream
                    | ClientOperation::ThreadEvents
                    | ClientOperation::ApprovalDecide
                    | ClientOperation::DaemonStatus => {}
                }
            }
        }
    }
}

fn apply_voice_response(state: &mut TuiState, response: VoiceControlResponse) {
    let message: String = match response {
        VoiceControlResponse::Enabled => "voice enabled".into(),
        VoiceControlResponse::AlreadyEnabled => "voice already enabled".into(),
        VoiceControlResponse::Disabled => "voice disabled".into(),
        VoiceControlResponse::AlreadyDisabled => "voice already disabled".into(),
        VoiceControlResponse::Denied => "voice grant denied".into(),
        VoiceControlResponse::Failed(error) => {
            state.status_message = Some(error.clone());
            push_live_event(state, crate::LiveEventLine::warning(None, error));
            return;
        }
    };
    state.status_message = Some(message.clone());
    push_live_event(state, crate::LiveEventLine::status(None, message));
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
    loaded.slash_popup = state.slash_popup.clone();
    loaded.queued_messages = std::mem::take(&mut state.queued_messages);
    loaded.issue_prep_started_at = state.issue_prep_started_at;
    loaded.issue_prep_elapsed_secs = state.issue_prep_elapsed_secs;
    loaded.motion_mode = state.motion_mode;
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
            loaded.streaming = std::mem::take(&mut state.streaming);
            loaded.history_rows.live_events = std::mem::take(&mut state.history_rows.live_events);
        }
        if loaded.active_model.is_none() {
            loaded.active_model = state.active_model.clone();
        }
        if loaded.active_run_elapsed_secs.is_none() {
            loaded.active_run_elapsed_secs = state.active_run_elapsed_secs;
        }
        loaded.working_elapsed_millis = state.working_elapsed_millis;
        if loaded.approval.is_none() {
            loaded.approval = state.approval.clone();
        }
        if loaded.approval == state.approval {
            loaded.approval_scroll_offset = state.approval_scroll_offset;
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
    state.approval_scroll_offset = 0;
    state.active_run = Some(ActiveRunView::new(run_id.clone(), status));
    state.bind_latest_user_to_run(&run_id);
    push_live_event(
        state,
        crate::LiveEventLine::status(None, format!("{message}: {run_id}"))
            .with_run_id(run_id.clone()),
    );
    runtime.active_run_id = Some(run_id);
    runtime.next_offset = 0;
    runtime.poll_in_flight = false;
    runtime.polling = status == RunStateName::Running;
    runtime.last_poll = Instant::now() - ACTIVE_POLL_INTERVAL;
    runtime.tool_inputs.clear();
    runtime
        .active_timer
        .start_at(Instant::now(), Duration::ZERO);
}

fn apply_thread_send_result(
    state: &mut TuiState,
    runtime: &mut UiRuntime,
    result: ThreadSendResult,
) {
    match result {
        ThreadSendResult::Started { turn_id, .. } => {
            runtime.thread_turn_id = Some(turn_id.clone());
            runtime.polling = true;
            runtime.last_poll = Instant::now() - ACTIVE_POLL_INTERVAL;
            runtime
                .active_timer
                .start_at(Instant::now(), Duration::ZERO);
            state.status_message = Some(format!("thread turn started: {turn_id}"));
        }
        ThreadSendResult::Steered { turn_id, .. } => {
            runtime.thread_turn_id = Some(turn_id.clone());
            state.status_message = Some(format!("thread turn steered: {turn_id}"));
        }
        ThreadSendResult::Rejected {
            turn_id, reason, ..
        } => {
            runtime.thread_turn_id = turn_id;
            let reason = serde_json::to_value(reason)
                .ok()
                .and_then(|value| value.as_str().map(str::to_owned))
                .unwrap_or_else(|| "rejected".into());
            let message = format!("thread send rejected: {reason}");
            state.status_message = Some(message.clone());
            push_live_event(state, crate::LiveEventLine::warning(None, message));
        }
    }
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
    apply_buffered_stream_events(state, runtime, result.events, Some(&result.run_id));
    if needs_catch_up {
        maybe_poll_events_now(runtime, commands);
    } else if !active {
        state.finalize_streaming(Some(&result.run_id));
        state.active_run_elapsed_secs = runtime
            .active_timer
            .elapsed_at(Instant::now())
            .map(|elapsed| elapsed.as_secs());
        runtime.active_timer.stop();
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

fn apply_thread_events_result(
    state: &mut TuiState,
    runtime: &mut UiRuntime,
    result: ThreadEventsResult,
) {
    if runtime
        .thread
        .as_ref()
        .is_none_or(|thread| thread.thread_id != result.thread_id)
    {
        return;
    }
    runtime.poll_in_flight = false;
    runtime.thread_next_offset = Some(result.next_offset);
    runtime.thread_turn_id = result.current_turn_id;
    runtime.polling = true;
    state.stream_warning = None;
    let events = result
        .events
        .into_iter()
        .map(|event| BufferedStreamEvent {
            offset: event.offset,
            event: event.event,
        })
        .collect();
    if let Some((run_id, status)) = apply_buffered_stream_events(state, runtime, events, None) {
        runtime.active_run_id = Some(run_id.clone());
        state.active_run = Some(ActiveRunView::new(run_id.clone(), status));
        state.bind_latest_user_to_run(&run_id);
        if matches!(
            status,
            RunStateName::Finished | RunStateName::Failed | RunStateName::Canceled
        ) {
            state.finalize_streaming(Some(&run_id));
            state.active_run_elapsed_secs = runtime
                .active_timer
                .elapsed_at(Instant::now())
                .map(|elapsed| elapsed.as_secs());
            runtime.active_timer.stop();
        } else if !runtime.active_timer.is_active() {
            runtime
                .active_timer
                .start_at(Instant::now(), Duration::ZERO);
        }
    }
}

fn apply_buffered_stream_events(
    state: &mut TuiState,
    runtime: &mut UiRuntime,
    events: Vec<BufferedStreamEvent>,
    fallback_run_id: Option<&str>,
) -> Option<(String, RunStateName)> {
    let arrived_at = Instant::now();
    let mut observed_run = None;
    for buffered in events {
        let event = &buffered.event;
        if let Some(run) = run_state_from_event(event) {
            observed_run = Some(run);
        }
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
            state.approval_scroll_offset = 0;
        }
        let line = crate::live_event_line(&buffered);
        let line = match fallback_run_id {
            Some(run_id) if line.run_id.is_none() => line.with_run_id(run_id),
            _ => line,
        };
        push_live_event_at(state, line, arrived_at);
    }
    observed_run
}

fn run_state_from_event(event: &StreamEvent) -> Option<(String, RunStateName)> {
    match event {
        StreamEvent::Ledger { record } => {
            let status = match &record.event {
                HarnessEvent::RunFinished { .. } => RunStateName::Finished,
                HarnessEvent::RunFailed { .. } => RunStateName::Failed,
                _ => RunStateName::Running,
            };
            Some((record.event.run_id().to_string(), status))
        }
        StreamEvent::AssistantDelta { run_id, .. }
        | StreamEvent::ApprovalRequested { run_id, .. } => {
            Some((run_id.clone(), RunStateName::Running))
        }
        StreamEvent::Canceled { run_id } => Some((run_id.clone(), RunStateName::Canceled)),
        StreamEvent::CompletionClaimed { run_id, .. } => {
            Some((run_id.clone(), RunStateName::Running))
        }
        StreamEvent::Unknown(_) => None,
    }
}

#[cfg(test)]
pub(super) fn maybe_poll_events(runtime: &mut UiRuntime, commands: &Sender<ClientCommand>) {
    maybe_poll_events_at(runtime, commands, Instant::now());
}

pub(super) fn maybe_poll_events_at(
    runtime: &mut UiRuntime,
    commands: &Sender<ClientCommand>,
    now: Instant,
) {
    if !runtime.polling || runtime.poll_in_flight {
        return;
    }
    if now.saturating_duration_since(runtime.last_poll) < ACTIVE_POLL_INTERVAL {
        return;
    }
    maybe_poll_events_now_at(runtime, commands, now);
}

fn maybe_poll_events_now(runtime: &mut UiRuntime, commands: &Sender<ClientCommand>) {
    maybe_poll_events_now_at(runtime, commands, Instant::now());
}

fn maybe_poll_events_now_at(
    runtime: &mut UiRuntime,
    commands: &Sender<ClientCommand>,
    now: Instant,
) {
    if runtime.thread.is_some() {
        poll_thread_events_from_at(runtime, commands, runtime.thread_next_offset, now);
        return;
    }
    let Some(run_id) = runtime.active_run_id.clone() else {
        return;
    };
    poll_events_from_at(runtime, commands, run_id, Some(runtime.next_offset), now);
}

fn poll_thread_events_from(
    runtime: &mut UiRuntime,
    commands: &Sender<ClientCommand>,
    from_offset: Option<u64>,
) {
    poll_thread_events_from_at(runtime, commands, from_offset, Instant::now());
}

fn poll_thread_events_from_at(
    runtime: &mut UiRuntime,
    commands: &Sender<ClientCommand>,
    from_offset: Option<u64>,
    now: Instant,
) {
    let Some(thread) = runtime.thread.as_ref() else {
        return;
    };
    if commands
        .send(ClientCommand::PollThreadEvents {
            thread_id: thread.thread_id.clone(),
            from_offset,
        })
        .is_ok()
    {
        runtime.poll_in_flight = true;
        runtime.last_poll = now;
    } else {
        runtime.polling = false;
    }
}

fn poll_events_from(
    runtime: &mut UiRuntime,
    commands: &Sender<ClientCommand>,
    run_id: String,
    from_offset: Option<u64>,
) {
    poll_events_from_at(runtime, commands, run_id, from_offset, Instant::now());
}

fn poll_events_from_at(
    runtime: &mut UiRuntime,
    commands: &Sender<ClientCommand>,
    run_id: String,
    from_offset: Option<u64>,
    now: Instant,
) {
    if commands
        .send(ClientCommand::PollEvents {
            run_id,
            from_offset,
        })
        .is_ok()
    {
        runtime.poll_in_flight = true;
        runtime.last_poll = now;
    } else {
        runtime.polling = false;
    }
}

pub(super) fn is_connection_error(error: &ClientError) -> bool {
    match error {
        ClientError::Io(_) | ClientError::DaemonProtocol(_) => true,
        ClientError::DaemonResponse(error) => matches!(
            error.code,
            ERROR_UNSUPPORTED_VERSION | ERROR_WORKSPACE_MISMATCH
        ),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConnectionState, TranscriptState, render_snapshot};
    use platonic_client::transport;
    use platonic_protocol::{
        CAPABILITY_HELLO, CAPABILITY_ISSUE_PREP_START, ERROR_INTERNAL, ERROR_ISSUE_PREP_FAILED,
        Envelope, EnvelopeKind, HelloResult, PROTOCOL_VERSION, ProtocolError, ProtocolErrorCode,
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
    fn client_worker_drop_waits_for_voice_shutdown() {
        let (request_sender, requests) = mpsc::channel();
        let (response_sender, responses) = mpsc::channel();
        let (observed_sender, observed) = mpsc::channel();
        let voice_worker = thread::spawn(move || {
            let request = requests.recv().unwrap();
            observed_sender.send(request).unwrap();
            response_sender
                .send(VoiceControlResponse::AlreadyDisabled)
                .unwrap();
        });
        let voice = VoiceControl::new(request_sender, responses, voice_worker);
        let workspace = tempfile::tempdir().unwrap();
        let config = DaemonConnectionConfig::resolve(
            workspace.path(),
            Some(workspace.path().join("agent.sock")),
        )
        .unwrap();
        let (event_sender, _) = mpsc::channel();

        drop(spawn_client_worker_to(
            config,
            None,
            Some(voice),
            event_sender,
        ));

        assert_eq!(observed.recv().unwrap(), VoiceControlRequest::Shutdown);
        assert!(observed.try_recv().is_err());
    }

    fn request_params_value(request: &Envelope) -> serde_json::Value {
        let request = serde_json::to_value(request.params.as_ref().unwrap()).unwrap();
        request.get("params").cloned().unwrap()
    }

    #[test]
    fn thread_picker_request_uses_thread_list_and_keeps_loaded_and_unloaded_threads() {
        let harness = ScriptedDaemon::start("thread-list", |workspace_id| {
            vec![
                hello_reply(workspace_id),
                ScriptedReply::result(
                    "thread.list",
                    json!({
                        "threads": [
                            {
                                "authority": {
                                    "thread_id": "thread_loaded",
                                    "parent_thread_id": null,
                                    "spawning_actor": "test",
                                    "cwd": "/work/loaded",
                                    "model": "test-model",
                                    "reasoning_effort": "none",
                                    "approval_policy": "prompt",
                                    "created_at_ms": 42
                                },
                                "live": {"loaded": true, "current_turn_id": "turn_1"}
                            },
                            {
                                "authority": {
                                    "thread_id": "thread_unloaded",
                                    "parent_thread_id": null,
                                    "spawning_actor": "test",
                                    "cwd": "/work/unloaded",
                                    "model": "test-model",
                                    "reasoning_effort": "none",
                                    "approval_policy": "prompt",
                                    "created_at_ms": 41
                                },
                                "live": {"loaded": false, "current_turn_id": null}
                            }
                        ]
                    }),
                ),
            ]
        });

        let ClientEvent::ThreadsLoaded(result) =
            handle_client_command(&harness.config, None, None, ClientCommand::ThreadList)
        else {
            panic!("expected thread list result")
        };

        assert!(result.threads[0].live.loaded);
        assert!(!result.threads[1].live.loaded);
        let requests = harness.finish();
        assert_eq!(requests[1].method.as_deref(), Some("thread.list"));
    }

    #[test]
    fn thread_attachment_loads_exact_session_and_polls_from_live_tip() {
        let harness = ScriptedDaemon::start("thread-attachment", |workspace_id| {
            vec![
                hello_reply(workspace_id),
                ScriptedReply::result(
                    "thread.status",
                    json!({
                        "thread": {
                            "authority": {
                                "thread_id": "thread_selected",
                                "parent_thread_id": null,
                                "spawning_actor": "local_tui",
                                "cwd": "/work",
                                "model": "test-model",
                                "reasoning_effort": "none",
                                "approval_policy": "prompt",
                                "created_at_ms": 42
                            },
                            "live": {
                                "loaded": true,
                                "current_turn_id": "thread_turn_active"
                            }
                        }
                    }),
                ),
                ScriptedReply::result(
                    "sessions.list",
                    json!({
                        "sessions": [{
                            "session_id": "session_thread_selected",
                            "run_id": "run_selected",
                            "status": "running",
                            "first_question": "inspect the workspace",
                            "latest_question": "inspect the workspace",
                            "created_at_ms": 42,
                            "updated_at_ms": 43,
                            "ledger_path": "/work/agent.db"
                        }]
                    }),
                ),
                daemon_status_reply("session_thread_selected"),
                ScriptedReply::result(
                    "transcript.read",
                    json!({
                        "run_id": "run_selected",
                        "status": "running",
                        "final_answer": null,
                        "transcript": "[turn_selected] user: inspect the workspace\n"
                    }),
                ),
            ]
        });
        let attachment = ThreadAttachment {
            thread_id: "thread_selected".into(),
            controller_id: "controller_remote".into(),
        };

        let state = load_connected_thread_state(&harness.config, &attachment).unwrap();
        assert_eq!(
            state.selected_session_id.as_deref(),
            Some("session_thread_selected")
        );
        assert_eq!(state.selected_thread_id.as_deref(), Some("thread_selected"));
        assert_eq!(
            state.active_run.as_ref().map(|run| run.run_id.as_str()),
            Some("run_selected")
        );

        let (commands, received) = mpsc::channel();
        let mut runtime = UiRuntime::from_state(&state, None);
        runtime.attach_thread(Some(attachment));
        maybe_poll_events(&mut runtime, &commands);
        assert!(matches!(
            received.recv().unwrap(),
            ClientCommand::PollThreadEvents {
                thread_id,
                from_offset: None,
            } if thread_id == "thread_selected"
        ));
        let requests = harness.finish();
        assert_eq!(requests[1].method.as_deref(), Some("thread.status"));
        assert_eq!(
            request_params_value(&requests[1])["thread_id"],
            "thread_selected"
        );
    }

    #[test]
    fn thread_attachment_uses_controller_turn_and_surfaces_refusal() {
        let workspace = tempfile::tempdir().unwrap();
        let config = DaemonConnectionConfig {
            workspace_root: workspace.path().to_owned(),
            socket_path: workspace.path().join("agent.sock"),
        };
        let mut state = connected_state(&config);
        let mut runtime = UiRuntime::from_state(&state, None);
        runtime.attach_thread(Some(ThreadAttachment {
            thread_id: "thread_selected".into(),
            controller_id: "controller_remote".into(),
        }));
        runtime.thread_turn_id = Some("thread_turn_active".into());
        assert!(matches!(
            runtime.thread_send_command("observe this".into()).unwrap(),
            ClientCommand::ThreadSend {
                thread_id,
                controller_id,
                turn_id: Some(turn_id),
                message,
            } if thread_id == "thread_selected"
                && controller_id == "controller_remote"
                && turn_id == "thread_turn_active"
                && message == "observe this"
        ));

        apply_thread_send_result(
            &mut state,
            &mut runtime,
            ThreadSendResult::Rejected {
                thread_id: "thread_selected".into(),
                turn_id: Some("thread_turn_active".into()),
                reason: platonic_protocol::ThreadSendRejectedReason::ControllerOwned,
            },
        );
        assert_eq!(
            state.status_message.as_deref(),
            Some("thread send rejected: controller_owned")
        );
        assert!(
            state
                .live_events
                .iter()
                .any(|event| event.text.contains("controller_owned"))
        );
    }

    #[test]
    fn switched_attachment_ignores_late_events_from_the_previous_thread() {
        let workspace = tempfile::tempdir().unwrap();
        let config = DaemonConnectionConfig {
            workspace_root: workspace.path().to_owned(),
            socket_path: workspace.path().join("agent.sock"),
        };
        let mut state = connected_state(&config);
        let mut runtime = UiRuntime::from_state(&state, None);
        runtime.attach_thread(Some(ThreadAttachment {
            thread_id: "thread_new".into(),
            controller_id: "controller_new".into(),
        }));

        apply_thread_events_result(
            &mut state,
            &mut runtime,
            ThreadEventsResult {
                thread_id: "thread_old".into(),
                from_offset: 0,
                next_offset: 4,
                current_turn_id: Some("turn_old".into()),
                events: vec![],
            },
        );

        assert_eq!(runtime.thread_next_offset, None);
        assert_eq!(runtime.thread_turn_id, None);
    }

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
                daemon_status_reply("session_finished"),
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
                daemon_status_reply("session_running"),
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
                request_params_value(request)["session_id"]
                    .as_str()
                    .unwrap()
                    .to_owned()
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
        push_live_event_at(
            &mut state,
            crate::LiveEventLine::assistant_delta(Some(1), "partial cancel mid-tok")
                .with_run_id("run_canceling"),
            Instant::now(),
        );

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
        assert!(runtime.active_timer.is_active());
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
        assert!(!runtime.active_timer.is_active());
        assert_eq!(state.live_events.len(), 1);
        assert_eq!(state.live_events[0].text, "partial cancel mid-tok");
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
            ClientOperation::ThreadSend,
            ClientOperation::IssuePrepStart,
            ClientOperation::EventsStream,
            ClientOperation::ThreadEvents,
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
                        code: ERROR_INTERNAL,
                        message: "expected failure".into(),
                    }),
                },
                &mut state,
                &mut runtime,
            );

            let expected_status = format!(
                "{} failed: daemon protocol error internal_error: expected failure",
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
                daemon_status_reply("session_selected"),
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
                    ERROR_OVERLOAD,
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
        assert_eq!(request_params_value(stream_requests[0])["from_offset"], 0);
        assert!(
            request_params_value(stream_requests[1])
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
            let params = request_params_value(request);
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
                workspace_id: platonic_client::paths::workspace_id(&config.workspace_root).unwrap(),
                ledger_path: "/work/agent.db".into(),
                capabilities: vec![CAPABILITY_HELLO, CAPABILITY_ISSUE_PREP_START],
                daemon_scope: None,
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
            code: ProtocolErrorCode,
            message: &'static str,
        },
    }

    impl ScriptedReply {
        fn result(method: &'static str, result: serde_json::Value) -> Self {
            Self::Result { method, result }
        }

        fn error(method: &'static str, code: ProtocolErrorCode, message: &'static str) -> Self {
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
                    "thread.list",
                    "transcript.read",
                    "transcript.read.pending_approval",
                    "events.stream",
                    "approval.decide"
                ]
            }),
        )
    }

    fn daemon_status_reply(session_id: &str) -> ScriptedReply {
        ScriptedReply::result(
            "daemon.status",
            json!({
                "model": {
                    "requested_alias": "test-model",
                    "served_model": null,
                    "provider_kind": "open_ai",
                    "key_present": false
                },
                "daemon": {
                    "package_version": env!("CARGO_PKG_VERSION"),
                    "build_commit": null,
                    "build_date_utc": null,
                    "uptime_ms": 1,
                    "endpoint_path": "/tmp/agent.sock",
                    "workspace_id": "test-workspace"
                },
                "session": {
                    "session_id": session_id,
                    "latest_run_id": null,
                    "human_turn_count": 0,
                    "ledger_path": "/work/agent.db",
                    "core_event_count": 0
                },
                "usage": {
                    "last_run": {
                        "input_tokens": 0,
                        "output_tokens": 0,
                        "unknown_response_count": 0
                    },
                    "session": {
                        "input_tokens": 0,
                        "output_tokens": 0,
                        "unknown_response_count": 0
                    }
                },
                "trust": {
                    "approval_granted_count": 0,
                    "approval_denied_count": 0,
                    "shell_session_grant": false,
                    "approval_profile": "prompt"
                }
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
                platonic_client::paths::workspace_id(&config.workspace_root).unwrap();
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
                platonic_client::paths::workspace_id(&config.workspace_root).unwrap();
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
                    request_params_value(&issue_prep)["input"],
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
                            code: ERROR_ISSUE_PREP_FAILED,
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
        }
    }
}
