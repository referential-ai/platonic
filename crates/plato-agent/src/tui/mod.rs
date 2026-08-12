pub use plato_tui::{
    ActiveRunView, ApprovalModalView, ConnectionState, LiveEventKind, LiveEventLine,
    SessionPickerView, ThreadAttachment, TranscriptState, TranscriptView, TuiOptions, TuiState,
    VOICE_CONTROL_CAPACITY, VoiceControl, VoiceControlEvent, VoiceControlRequest,
    VoiceControlResponse, approval_from_event, live_event_line, model_from_event, render,
    render_snapshot, tool_input_preview_from_event,
};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, RecvTimeoutError},
    },
    thread,
    time::Duration,
};

use crate::voice::{VoiceActivation, VoiceActivationChange, VoiceGrant, VoiceSessionEvent};

/// Starts the concrete client-side activation worker for one explicit voice config path.
pub fn voice_control(config_path: Option<&Path>) -> crate::AppResult<VoiceControl> {
    let mut activation = VoiceActivation::from_explicit_config(config_path);
    let (request_sender, requests) = mpsc::sync_channel(VOICE_CONTROL_CAPACITY);
    let (response_sender, responses) = mpsc::channel();
    let (event_sender, events) = mpsc::channel();
    let abandon = Arc::new(AtomicBool::new(false));
    let worker_abandon = Arc::clone(&abandon);
    let worker = thread::Builder::new()
        .name("plato-voice-activation".into())
        .spawn(move || {
            loop {
                if worker_abandon.swap(false, Ordering::AcqRel)
                    && let Some(session) = activation.session_mut()
                {
                    let _ = session.abandon_run();
                    let _ = event_sender.send(VoiceControlEvent::Failed(
                        "voice event queue overflowed; narration abandoned".into(),
                    ));
                }
                let request = match requests.recv_timeout(Duration::from_millis(1)) {
                    Ok(request) => Some(request),
                    Err(RecvTimeoutError::Timeout) => None,
                    Err(RecvTimeoutError::Disconnected) => break,
                };
                let mut shutdown = false;
                if let Some(request) = request {
                    shutdown = matches!(request, VoiceControlRequest::Shutdown);
                    let response = match request {
                        VoiceControlRequest::Enable {
                            active_run_id,
                            capture_idle,
                        } => {
                            let result = activation.enable(VoiceGrant::Granted);
                            if matches!(
                                result,
                                Ok(VoiceActivationChange::Enabled)
                                    | Ok(VoiceActivationChange::AlreadyEnabled)
                            ) && let Some(session) = activation.session_mut()
                                && let Err(error) = match active_run_id {
                                    Some(run_id) => session.observe_run(&run_id),
                                    None if capture_idle => session.arm_capture(),
                                    None => session.submission_started(),
                                }
                            {
                                let _ = activation.disable();
                                Some(VoiceControlResponse::Failed(error.to_string()))
                            } else {
                                Some(activation_response(result))
                            }
                        }
                        VoiceControlRequest::Disable | VoiceControlRequest::Shutdown => {
                            Some(activation_response(activation.disable()))
                        }
                        VoiceControlRequest::Cancel { run_id } => {
                            Some(match activation.session_mut() {
                                Some(session) => match session.cancel_run(&run_id) {
                                    Ok(()) => VoiceControlResponse::Silenced,
                                    Err(error) => VoiceControlResponse::Failed(error.to_string()),
                                },
                                None => VoiceControlResponse::AlreadyDisabled,
                            })
                        }
                        VoiceControlRequest::SubmissionStarted => {
                            apply_session_request(&mut activation, &event_sender, |session| {
                                session.submission_started()
                            });
                            None
                        }
                        VoiceControlRequest::RunObserved { run_id } => {
                            apply_session_request(&mut activation, &event_sender, |session| {
                                session.observe_run(&run_id)
                            });
                            None
                        }
                        VoiceControlRequest::Stream(event) => {
                            apply_session_request(&mut activation, &event_sender, |session| {
                                session.accept_stream_event(event)
                            });
                            None
                        }
                        VoiceControlRequest::Terminal { run_id, status } => {
                            apply_session_request(&mut activation, &event_sender, |session| {
                                session.observe_terminal(&run_id, status)
                            });
                            None
                        }
                        VoiceControlRequest::SubmissionFailed => {
                            apply_session_request(&mut activation, &event_sender, |session| {
                                session.submission_failed()
                            });
                            None
                        }
                        VoiceControlRequest::Loaded { active_run_id } => {
                            apply_session_request(&mut activation, &event_sender, |session| {
                                session.observe_loaded_run(active_run_id.as_deref())
                            });
                            None
                        }
                        VoiceControlRequest::CommitAcknowledged { run_id } => {
                            apply_session_event_request(
                                &mut activation,
                                &event_sender,
                                |session| session.acknowledge_commit(&run_id),
                            );
                            None
                        }
                        VoiceControlRequest::CommitFailed { run_id } => {
                            apply_session_event_request(
                                &mut activation,
                                &event_sender,
                                |session| session.retry_commit(&run_id),
                            );
                            None
                        }
                    };
                    if let Some(response) = response
                        && response_sender.send(response).is_err()
                    {
                        break;
                    }
                }
                if shutdown {
                    break;
                }
                let poll_error = if let Some(session) = activation.session_mut() {
                    match session.poll_bridge() {
                        Ok(updates) => {
                            for update in updates {
                                if event_sender.send(control_event(update)).is_err() {
                                    return;
                                }
                            }
                            if let Err(error) = session.arm_capture() {
                                let _ = session.abandon_run();
                                Some(error.to_string())
                            } else {
                                None
                            }
                        }
                        Err(error) => {
                            let _ = session.abandon_run();
                            Some(error.to_string())
                        }
                    }
                } else {
                    None
                };
                if let Some(error) = poll_error {
                    let _ = activation.disable();
                    let _ = event_sender.send(VoiceControlEvent::Failed(error));
                }
            }
        })?;
    Ok(VoiceControl::new(
        request_sender,
        responses,
        events,
        abandon,
        worker,
    ))
}

fn activation_response(
    result: Result<VoiceActivationChange, crate::voice::VoiceActivationError>,
) -> VoiceControlResponse {
    match result {
        Ok(VoiceActivationChange::Enabled) => VoiceControlResponse::Enabled,
        Ok(VoiceActivationChange::AlreadyEnabled) => VoiceControlResponse::AlreadyEnabled,
        Ok(VoiceActivationChange::Denied) => VoiceControlResponse::Denied,
        Ok(VoiceActivationChange::Disabled) => VoiceControlResponse::Disabled,
        Ok(VoiceActivationChange::AlreadyDisabled) => VoiceControlResponse::AlreadyDisabled,
        Err(error) => VoiceControlResponse::Failed(error.to_string()),
    }
}

fn apply_session_request(
    activation: &mut VoiceActivation,
    events: &mpsc::Sender<VoiceControlEvent>,
    request: impl FnOnce(&mut crate::voice::VoiceSession) -> Result<(), crate::voice::VoiceError>,
) {
    if let Some(session) = activation.session_mut()
        && let Err(error) = request(session)
    {
        let _ = session.abandon_run();
        let _ = events.send(VoiceControlEvent::Failed(error.to_string()));
    }
}

fn apply_session_event_request(
    activation: &mut VoiceActivation,
    events: &mpsc::Sender<VoiceControlEvent>,
    request: impl FnOnce(
        &mut crate::voice::VoiceSession,
    ) -> Result<Option<VoiceSessionEvent>, crate::voice::VoiceError>,
) {
    if let Some(session) = activation.session_mut() {
        match request(session) {
            Ok(Some(event)) => {
                let _ = events.send(control_event(event));
            }
            Ok(None) => {}
            Err(error) => {
                let _ = session.abandon_run();
                let _ = events.send(VoiceControlEvent::Failed(error.to_string()));
            }
        }
    }
}

fn control_event(event: VoiceSessionEvent) -> VoiceControlEvent {
    match event {
        VoiceSessionEvent::Captured {
            transcript,
            prior_interrupted_run_id,
        } => VoiceControlEvent::Captured {
            transcript,
            prior_interrupted_run_id,
        },
        VoiceSessionEvent::CancelRun { run_id } => VoiceControlEvent::CancelRun { run_id },
        VoiceSessionEvent::Commit { run_id, events } => {
            VoiceControlEvent::Commit { run_id, events }
        }
    }
}

pub fn run_tui(options: TuiOptions) -> crate::AppResult<()> {
    plato_tui::run_tui(options).map_err(Into::into)
}
