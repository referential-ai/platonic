#![deny(unsafe_code)]

//! Platonic agent server.
//!
//! Owns workspaces, agents, threads, tools, providers, the ledger, policy,
//! approvals, and the protocol. `platonic-core` supplies the sans-IO run
//! state machine; this crate instantiates one per thread and performs every
//! effect it asks for.

pub mod app;
pub mod config;
mod confinement;
pub mod daemon;
pub mod error;
pub mod gateway;
pub mod ledger;
pub mod model;
pub mod paths;
pub mod provider;
pub mod replay;
mod server_store;
mod thread_authority;
mod thread_repository;
pub mod tool_catalog;
pub mod tools;

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
pub use platonic_protocol::{VOICE_EVENT_VERSION, VoiceEvent, VoiceEventEnvelope};
pub use replay::{replay_default_sqlite, replay_file, replay_sqlite, replay_sqlite_session};
