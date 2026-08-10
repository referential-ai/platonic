pub use plato_tui::{
    ActiveRunView, ApprovalModalView, ConnectionState, LiveEventKind, LiveEventLine,
    SessionPickerView, ThreadAttachment, TranscriptState, TranscriptView, TuiOptions, TuiState,
    VoiceControl, VoiceControlRequest, VoiceControlResponse, approval_from_event, live_event_line,
    model_from_event, render, render_snapshot, tool_input_preview_from_event,
};
use std::{path::Path, sync::mpsc, thread};

use crate::voice::{VoiceActivation, VoiceActivationChange, VoiceGrant};

/// Starts the concrete client-side activation worker for one explicit voice config path.
pub fn voice_control(config_path: Option<&Path>) -> crate::AppResult<VoiceControl> {
    let mut activation = VoiceActivation::from_explicit_config(config_path);
    let (request_sender, requests) = mpsc::channel();
    let (response_sender, responses) = mpsc::channel();
    let worker = thread::Builder::new()
        .name("plato-voice-activation".into())
        .spawn(move || {
            for request in requests {
                let shutdown = request == VoiceControlRequest::Shutdown;
                let result = match request {
                    VoiceControlRequest::Enable => activation.enable(VoiceGrant::Granted),
                    VoiceControlRequest::Disable | VoiceControlRequest::Shutdown => {
                        activation.disable()
                    }
                };
                let response = match result {
                    Ok(VoiceActivationChange::Enabled) => VoiceControlResponse::Enabled,
                    Ok(VoiceActivationChange::AlreadyEnabled) => {
                        VoiceControlResponse::AlreadyEnabled
                    }
                    Ok(VoiceActivationChange::Denied) => VoiceControlResponse::Denied,
                    Ok(VoiceActivationChange::Disabled) => VoiceControlResponse::Disabled,
                    Ok(VoiceActivationChange::AlreadyDisabled) => {
                        VoiceControlResponse::AlreadyDisabled
                    }
                    Err(error) => VoiceControlResponse::Failed(error.to_string()),
                };
                if response_sender.send(response).is_err() || shutdown {
                    break;
                }
            }
        })?;
    Ok(VoiceControl::new(request_sender, responses, worker))
}

pub fn run_tui(options: TuiOptions) -> crate::AppResult<()> {
    plato_tui::run_tui(options).map_err(Into::into)
}
