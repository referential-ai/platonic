#![deny(unsafe_code)]

pub mod app;
pub mod config;
pub mod daemon;
pub mod discord_gateway;
pub mod error;
pub mod ledger;
pub mod model;
pub mod paths;
pub mod provider;
pub mod replay;
pub mod tool_catalog;
pub mod tools;
pub mod tui;
pub mod voice;

pub use tools::github::issues as issue_prep;

#[cfg(windows)]
mod windows_security;

pub use app::{
    ApprovalMode, ApprovalRequest, AssistantDeltaEvent, RunEvent, RunLedger, RunOptions,
    RunOutcome, RunSession, new_run_id, new_session_id, run_question,
};
pub use error::{AppError, AppResult};
pub use issue_prep::{IssuePrepOptions, IssuePrepOutcome, run_issue_prep};
pub use model::{ReasoningEffort, RunOverrides};
pub use replay::{replay_default_sqlite, replay_file, replay_sqlite, replay_sqlite_session};
pub use voice::{
    CapturedRunOutcome, NarratedRunOutcome, NarratedSentenceReport, NarrationReport, VoiceError,
    VoiceRunError, VoiceSession, VoiceSessionShutdown,
};
