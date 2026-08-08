pub use plato_tui::{
    ActiveRunView, ApprovalModalView, ConnectionState, LiveEventKind, LiveEventLine,
    SessionPickerView, ThreadAttachment, TranscriptState, TranscriptView, TuiOptions, TuiState,
    approval_from_event, live_event_line, model_from_event, render, render_snapshot,
    tool_input_preview_from_event,
};

pub fn run_tui(options: TuiOptions) -> crate::AppResult<()> {
    plato_tui::run_tui(options).map_err(Into::into)
}
