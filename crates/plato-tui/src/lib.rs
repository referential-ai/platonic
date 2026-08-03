//! Terminal client library for Plato Agent.
//!
//! This crate owns terminal state, daemon-event reduction, rendering, and the
//! terminal loop. Provider, daemon, policy, approval, and persistence semantics
//! remain with `plato-agentd`.

#![deny(unsafe_code)]
#![warn(missing_docs)]

mod app;
mod client;
mod color;
mod commands;
mod markdown;
mod modal;
mod render;
mod state;

pub use app::{TuiOptions, run_tui};
pub use modal::{
    ApprovalModalView, approval_from_event, live_event_line, model_from_event,
    tool_input_preview_from_event,
};
pub use render::{render, render_snapshot};
pub use state::{
    ActiveRunView, ConnectionState, LiveEventKind, LiveEventLine, SessionPickerView,
    TranscriptState, TranscriptView, TuiState,
};
