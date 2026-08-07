#![deny(unsafe_code)]

//! Plato Agent: the client distribution built on Platonic.
//!
//! Clients, curated agent configuration, and the voice subsystem. The server
//! itself lives in `platonic-server`, which this crate depends on so the
//! one-shot and replay paths keep working without a running daemon.

pub mod discord_gateway;
pub mod tui;
pub mod voice;

pub use platonic_server::{
    AppError, AppResult, ApprovalMode, ApprovalRequest, AssistantDeltaEvent, IssuePrepOptions,
    IssuePrepOutcome, ReasoningEffort, RunEvent, RunLedger, RunOptions, RunOutcome, RunOverrides,
    RunSession, VOICE_EVENT_VERSION, VoiceEvent, VoiceEventEnvelope, app, config, daemon, error,
    issue_prep, ledger, model, new_run_id, new_session_id, paths, provider, replay,
    replay_default_sqlite, replay_file, replay_sqlite, replay_sqlite_session, run_issue_prep,
    run_question, tool_catalog, tools, voice_session,
};
pub use voice::{
    CapturedRunOutcome, NarratedRunOutcome, NarratedSentenceReport, NarrationReport, VoiceError,
    VoiceRunError, VoiceSession, VoiceSessionShutdown,
};
