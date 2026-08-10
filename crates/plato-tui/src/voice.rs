use std::{
    fmt,
    sync::{Arc, Mutex, mpsc},
    thread::JoinHandle,
};

/// Exact activation requests understood by the client-owned voice worker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VoiceControlRequest {
    /// Open configured models and devices after the user's explicit command.
    Enable,
    /// Stop, flush, and close the current voice session.
    Disable,
    /// Stop the current session and terminate the client-owned worker.
    Shutdown,
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
}

struct VoiceControlInner {
    requests: mpsc::Sender<VoiceControlRequest>,
    responses: Mutex<mpsc::Receiver<VoiceControlResponse>>,
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
        requests: mpsc::Sender<VoiceControlRequest>,
        responses: mpsc::Receiver<VoiceControlResponse>,
        worker: JoinHandle<()>,
    ) -> Self {
        Self {
            inner: Arc::new(VoiceControlInner {
                requests,
                responses: Mutex::new(responses),
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
        let (request_sender, requests) = mpsc::channel();
        let (response_sender, responses) = mpsc::channel();
        let (observed_sender, observed) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            for request in requests {
                observed_sender.send(request).unwrap();
                let response = match request {
                    VoiceControlRequest::Enable => VoiceControlResponse::Enabled,
                    VoiceControlRequest::Disable => VoiceControlResponse::Disabled,
                    VoiceControlRequest::Shutdown => VoiceControlResponse::AlreadyDisabled,
                };
                response_sender.send(response).unwrap();
                if request == VoiceControlRequest::Shutdown {
                    break;
                }
            }
        });
        let control = VoiceControl::new(request_sender, responses, worker);
        let retained = control.clone();

        assert_eq!(
            control.request(VoiceControlRequest::Enable),
            VoiceControlResponse::Enabled
        );
        assert_eq!(observed.recv().unwrap(), VoiceControlRequest::Enable);
        drop(control);
        assert!(observed.try_recv().is_err());
        drop(retained);
        assert_eq!(observed.recv().unwrap(), VoiceControlRequest::Shutdown);
        assert!(observed.try_recv().is_err());
    }
}
