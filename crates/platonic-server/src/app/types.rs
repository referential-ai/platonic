use super::session::RunSession;
use crate::{AppResult, model::RunOverrides, paths::DefaultSqlitePath};
use platonic_core::{EffectClass, RecordedEvent, RunId, ToolCallId, TurnId};
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    path::PathBuf,
    sync::{Arc, atomic::AtomicBool, mpsc::Sender},
};

#[derive(Clone, Debug)]
pub struct RunOptions {
    pub question: String,
    pub config_path: Option<PathBuf>,
    pub overrides: RunOverrides,
    pub ledger: RunLedger,
    pub workspace_root: PathBuf,
    pub approval_mode: ApprovalMode,
    pub run_id: Option<RunId>,
    pub session: Option<RunSession>,
    pub event_sender: Option<Sender<RunEvent>>,
    pub stream_to_stderr: bool,
    pub cancel: Option<Arc<AtomicBool>>,
    /// Root-owned, one-turn voice interruption note; ordinary runs leave this absent.
    pub voice_interruption_context: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RunOutcome {
    pub run_id: RunId,
    pub final_answer: String,
    /// Additive-optional completion claim from a worker thread.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_claim: Option<platonic_protocol::CompletionClaim>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RunEvent {
    Ledger(RecordedEvent),
    AssistantDelta(AssistantDeltaEvent),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AssistantDeltaEvent {
    pub run_id: RunId,
    pub turn_id: TurnId,
    pub step: u32,
    pub delta_index: u64,
    pub text: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunLedger {
    Jsonl(PathBuf),
    Sqlite(PathBuf),
    DefaultSqlite(DefaultSqlitePath),
}

#[derive(Clone, Default)]
pub enum ApprovalMode {
    #[default]
    Prompt,
    AutoApprove,
    Deny {
        actor: &'static str,
    },
    External(ApprovalHandler),
}

#[derive(Clone)]
pub struct ApprovalHandler {
    pub(super) actor: &'static str,
    pub(super) decide:
        Arc<dyn Fn(ApprovalRequest) -> AppResult<ExternalApprovalOutcome> + Send + Sync>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ExternalApprovalOutcome {
    Granted { actor: String },
    Denied { actor: String, reason: String },
}

impl fmt::Debug for ApprovalMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Prompt => formatter.write_str("Prompt"),
            Self::AutoApprove => formatter.write_str("AutoApprove"),
            Self::Deny { actor } => formatter
                .debug_struct("Deny")
                .field("actor", actor)
                .finish(),
            Self::External(handler) => formatter
                .debug_struct("External")
                .field("actor", &handler.actor)
                .finish_non_exhaustive(),
        }
    }
}

impl fmt::Debug for ApprovalHandler {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApprovalHandler")
            .field("actor", &self.actor)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ApprovalRequest {
    pub run_id: RunId,
    pub call_id: ToolCallId,
    pub tool_name: String,
    pub effect: EffectClass,
    pub reason: String,
    pub input_preview: Option<String>,
    pub approval_preview: Option<String>,
    pub diff_preview: Option<String>,
    pub yolo_eligible: bool,
}
