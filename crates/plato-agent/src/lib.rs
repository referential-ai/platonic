#![deny(unsafe_code)]

//! Plato Agent client distribution.
//!
//! This crate owns client presentation and local peripherals. Server runtime
//! semantics live behind `platonic-protocol` and are reached through
//! `platonic-client`; this crate never links the server implementation.

mod error;
pub mod offline;
pub mod run;
pub mod tui;
pub mod voice;

pub use error::{AppError, AppResult};
pub use platonic_protocol::{
    ReasoningEffort, RunOverrides, VOICE_EVENT_VERSION, VoiceEvent, VoiceEventEnvelope,
};
pub use run::{
    ApprovalMode, AssistantDeltaEvent, RunEvent, RunOptions, RunOutcome, attach_server_interactive,
    ensure_server, ensure_server_interactive, run_question,
};
pub use voice::{
    CapturedRunOutcome, NarratedRunOutcome, NarratedSentenceReport, NarrationReport, VoiceError,
    VoiceRunError, VoiceSession, VoiceSessionShutdown,
};
