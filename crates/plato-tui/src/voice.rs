use std::{
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread::JoinHandle,
};

use platonic_protocol::{RunStateName, StreamEvent, VoiceEvent};

/// Exact fixed client-to-voice event capacity.
pub const VOICE_CONTROL_CAPACITY: usize = 128;

/// Exact activation requests understood by the client-owned voice worker.
#[derive(Clone, Debug, PartialEq)]
pub enum VoiceControlRequest {
    /// Open configured models and devices after the user's explicit command.
    Enable {
        /// Run already active in the TUI, if its identifier is known.
        active_run_id: Option<String>,
        /// Whether hands-free capture may arm immediately.
        capture_idle: bool,
    },
    /// Stop, flush, and close the current voice session.
    Disable,
    /// Disarm hands-free capture before an ordinary daemon submission.
    SubmissionStarted,
    /// Bind the pending captured question to a daemon-minted run.
    RunObserved {
        /// Exact admitted run.
        run_id: String,
    },
    /// Feed one existing daemon event to narration without blocking polling.
    Stream(StreamEvent),
    /// Finish one daemon run after its complete event page is observed.
    Terminal {
        /// Exact terminal run.
        run_id: String,
        /// Authoritative daemon terminal state.
        status: RunStateName,
    },
    /// Silence local audio before plain Ctrl-C reaches `run.cancel`.
    Cancel {
        /// Exact run being canceled.
        run_id: String,
    },
    /// Discard an unbound capture after daemon admission failed.
    SubmissionFailed,
    /// Reconcile abandoned narration after the ordinary TUI reload path reconnects.
    Loaded {
        /// Run still active after reload, or none when the old run became terminal.
        active_run_id: Option<String>,
    },
    /// Release a pending next utterance after the server committed its prior facts.
    CommitAcknowledged {
        /// Exact committed run.
        run_id: String,
    },
    /// Retry the unchanged in-memory batch after a commit failure.
    CommitFailed {
        /// Exact rejected or unacknowledged run.
        run_id: String,
    },
    /// Stop the current session and terminate the client-owned worker.
    Shutdown,
}

/// Asynchronous voice outcomes consumed by the ordinary TUI reducer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VoiceControlEvent {
    /// One final utterance is ready for normal composer routing.
    Captured {
        /// Exact final transcript.
        transcript: String,
        /// Server-verifiable prior interruption, when barge-in produced this turn.
        prior_interrupted_run_id: Option<String>,
    },
    /// Local playback is silent and the daemon run should now be canceled.
    CancelRun {
        /// Exact active run.
        run_id: String,
    },
    /// One exact raw batch is ready for the existing commit method.
    Commit {
        /// Terminal run receiving the batch.
        run_id: String,
        /// Complete raw voice event batch.
        events: Vec<VoiceEvent>,
    },
    /// Voice failed closed without changing the daemon text run.
    Failed(String),
}

/// Result of one client-owned voice activation request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VoiceControlResponse {
    /// One voice session was opened.
    Enabled,
    /// Voice was already enabled; no second session was opened.
    AlreadyEnabled,
    /// One voice session was stopped and closed.
    Disabled,
    /// Voice was already off; no teardown was repeated.
    AlreadyDisabled,
    /// The local activation grant was declined.
    Denied,
    /// Configuration, model, or device activation failed closed.
    Failed(String),
    /// Local playback reached silence before remote cancellation.
    Silenced,
}

struct VoiceControlInner {
    requests: mpsc::SyncSender<VoiceControlRequest>,
    responses: Mutex<mpsc::Receiver<VoiceControlResponse>>,
    events: Mutex<Option<mpsc::Receiver<VoiceControlEvent>>>,
    abandon: Arc<AtomicBool>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for VoiceControlInner {
    fn drop(&mut self) {
        if self.requests.send(VoiceControlRequest::Shutdown).is_ok()
            && let Ok(responses) = self.responses.get_mut()
        {
            let _ = responses.recv();
        }
        if let Ok(worker) = self.worker.get_mut()
            && let Some(worker) = worker.take()
        {
            let _ = worker.join();
        }
    }
}

/// Session-local channel to the concrete Plato Agent voice worker.
#[derive(Clone)]
pub struct VoiceControl {
    inner: Arc<VoiceControlInner>,
}

impl VoiceControl {
    /// Joins an exact request/response pair to its owning voice worker.
    pub fn new(
        requests: mpsc::SyncSender<VoiceControlRequest>,
        responses: mpsc::Receiver<VoiceControlResponse>,
        events: mpsc::Receiver<VoiceControlEvent>,
        abandon: Arc<AtomicBool>,
        worker: JoinHandle<()>,
    ) -> Self {
        Self {
            inner: Arc::new(VoiceControlInner {
                requests,
                responses: Mutex::new(responses),
                events: Mutex::new(Some(events)),
                abandon,
                worker: Mutex::new(Some(worker)),
            }),
        }
    }

    pub(crate) fn request(&self, request: VoiceControlRequest) -> VoiceControlResponse {
        if self.inner.requests.send(request).is_err() {
            return VoiceControlResponse::Failed("voice activation worker stopped".into());
        }
        self.inner
            .responses
            .lock()
            .ok()
            .and_then(|responses| responses.recv().ok())
            .unwrap_or_else(|| {
                VoiceControlResponse::Failed("voice activation worker stopped".into())
            })
    }

    pub(crate) fn try_notify(&self, request: VoiceControlRequest) -> bool {
        match self.inner.requests.try_send(request) {
            Ok(()) => true,
            Err(_) => {
                self.abandon_current();
                false
            }
        }
    }

    pub(crate) fn abandon_current(&self) {
        self.inner.abandon.store(true, Ordering::Release);
    }

    pub(crate) fn take_events(&self) -> Option<mpsc::Receiver<VoiceControlEvent>> {
        self.inner
            .events
            .lock()
            .ok()
            .and_then(|mut events| events.take())
    }
}

impl fmt::Debug for VoiceControl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("VoiceControl")
    }
}

impl PartialEq for VoiceControl {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl Eq for VoiceControl {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn final_control_drop_sends_one_shutdown_and_joins_the_worker() {
        let (request_sender, requests) =
            mpsc::sync_channel::<VoiceControlRequest>(VOICE_CONTROL_CAPACITY);
        let (response_sender, responses) = mpsc::channel();
        let (_event_sender, events) = mpsc::channel();
        let abandon = Arc::new(AtomicBool::new(false));
        let (observed_sender, observed) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            for request in requests {
                observed_sender.send(request.clone()).unwrap();
                let response = match request {
                    VoiceControlRequest::Enable { .. } => VoiceControlResponse::Enabled,
                    VoiceControlRequest::Disable => VoiceControlResponse::Disabled,
                    VoiceControlRequest::Cancel { .. } => VoiceControlResponse::Silenced,
                    VoiceControlRequest::Shutdown => VoiceControlResponse::AlreadyDisabled,
                    VoiceControlRequest::SubmissionStarted
                    | VoiceControlRequest::RunObserved { .. }
                    | VoiceControlRequest::Stream(_)
                    | VoiceControlRequest::Terminal { .. }
                    | VoiceControlRequest::SubmissionFailed
                    | VoiceControlRequest::Loaded { .. }
                    | VoiceControlRequest::CommitAcknowledged { .. }
                    | VoiceControlRequest::CommitFailed { .. } => continue,
                };
                response_sender.send(response).unwrap();
                if request == VoiceControlRequest::Shutdown {
                    break;
                }
            }
        });
        let control = VoiceControl::new(request_sender, responses, events, abandon, worker);
        let retained = control.clone();

        let enable = VoiceControlRequest::Enable {
            active_run_id: None,
            capture_idle: true,
        };
        assert_eq!(
            control.request(enable.clone()),
            VoiceControlResponse::Enabled
        );
        assert_eq!(observed.recv().unwrap(), enable);
        drop(control);
        assert!(observed.try_recv().is_err());
        drop(retained);
        assert_eq!(observed.recv().unwrap(), VoiceControlRequest::Shutdown);
        assert!(observed.try_recv().is_err());
    }

    #[test]
    fn fixed_voice_queue_fails_closed_instead_of_blocking_daemon_events() {
        let (request_sender, requests) =
            mpsc::sync_channel::<VoiceControlRequest>(VOICE_CONTROL_CAPACITY);
        let (response_sender, responses) = mpsc::channel();
        let (_event_sender, events) = mpsc::channel();
        let abandon = Arc::new(AtomicBool::new(false));
        let observed_abandon = Arc::clone(&abandon);
        let (release_sender, release) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            release.recv().unwrap();
            for request in requests {
                if request == VoiceControlRequest::Shutdown {
                    response_sender
                        .send(VoiceControlResponse::AlreadyDisabled)
                        .unwrap();
                    break;
                }
            }
        });
        let control = VoiceControl::new(request_sender, responses, events, abandon, worker);

        for _ in 0..VOICE_CONTROL_CAPACITY {
            assert!(control.try_notify(VoiceControlRequest::SubmissionStarted));
        }
        assert!(!control.try_notify(VoiceControlRequest::SubmissionStarted));
        assert!(observed_abandon.load(Ordering::Acquire));

        release_sender.send(()).unwrap();
        drop(control);
    }
}
