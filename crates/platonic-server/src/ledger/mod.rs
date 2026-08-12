mod jsonl;
mod recorder;
mod replay;
mod sqlite;
mod types;
#[cfg(unix)]
mod unix;

pub use jsonl::{JsonlEventRecorder, read_records};
pub(crate) use jsonl::{read_voice_events_from_jsonl, run_jsonl_path};
pub use recorder::EventRecorder;
pub(crate) use recorder::{RUN_CANCELED_REASON, RunEventRecorder};
pub(crate) use replay::default_sqlite_session_status;
pub use replay::{
    default_sqlite_session_summaries, latest_default_sqlite_session_id, latest_sqlite_session_id,
    read_latest_sqlite_session, read_sqlite_records, read_sqlite_session, sqlite_session_summaries,
};
pub use sqlite::{
    SqliteEventRecorder, SqliteLedger, interrupt_orphaned_default_sqlite_runs,
    interrupt_orphaned_sqlite_runs,
};
pub(crate) use sqlite::{row_u64, sqlite_i64};
#[allow(unused_imports)]
pub(crate) use types::PersistedSessionStatus;
pub(crate) use types::PersistedTokenUsage;
pub use types::{
    LEDGER_VERSION, LedgerLine, PersistedSessionSummary, SessionRecords, SessionRunRecords,
    SessionTurn,
};
