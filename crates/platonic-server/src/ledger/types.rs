use platonic_core::RecordedEvent;
use platonic_protocol::RunStateName;
use serde::{Deserialize, Serialize};

pub(super) const LEGACY_LEDGER_VERSION: u32 = 1;
pub const LEDGER_VERSION: u32 = 2;

pub(super) fn supported_ledger_version(version: u32) -> bool {
    matches!(version, LEGACY_LEDGER_VERSION | LEDGER_VERSION)
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LedgerLine {
    pub v: u32,
    pub record: RecordedEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionTurn {
    pub question: String,
    pub final_answer: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SessionRunRecords {
    pub run_id: String,
    pub session_index: u64,
    pub question: String,
    pub status: RunStateName,
    pub final_answer: Option<String>,
    pub records: Vec<RecordedEvent>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SessionRecords {
    pub session_id: String,
    pub runs: Vec<SessionRunRecords>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedSessionSummary {
    pub session_id: String,
    pub run_id: String,
    pub status: RunStateName,
    pub latest_question: String,
    pub first_question: String,
    pub updated_at_ms: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct PersistedTokenUsage {
    pub(crate) input_tokens: u64,
    pub(crate) output_tokens: u64,
    pub(crate) unknown_response_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PersistedSessionStatus {
    pub(crate) session_id: String,
    pub(crate) latest_run_id: String,
    pub(crate) human_turn_count: u64,
    pub(crate) core_event_count: u64,
    pub(crate) served_model: Option<String>,
    pub(crate) last_run_usage: PersistedTokenUsage,
    pub(crate) session_usage: PersistedTokenUsage,
    pub(crate) approval_granted_count: u64,
    pub(crate) approval_denied_count: u64,
}
