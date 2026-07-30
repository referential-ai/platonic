use crate::daemon::protocol::ProtocolError;
use std::path::PathBuf;

pub type AppResult<T> = Result<T, AppError>;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("provider api key env var {0} is not set")]
    MissingApiKey(String),

    #[error("config error: {0}")]
    Config(String),

    #[error("provider error: {0}")]
    Provider(String),

    #[error("provider completion POST returned http 429 before response body")]
    ProviderCompletionRateLimited { retry_after_seconds: Option<f64> },

    #[error("tool error: {0}")]
    Tool(String),

    #[error("ledger version mismatch: expected {expected}, actual {actual}")]
    LedgerVersion { expected: u32, actual: u32 },

    #[error("ledger path is empty")]
    EmptyLedger,

    #[error("ledger already exists: {0}")]
    LedgerExists(PathBuf),

    #[error("ledger conflict for run {run_id} seq {seq}")]
    LedgerConflict { run_id: String, seq: u64 },

    #[error("sqlite ledger has no runs")]
    NoSqliteRuns,

    #[error("run not found in sqlite ledger: {0}")]
    RunNotFound(String),

    #[error("sqlite ledger has no sessions")]
    NoSqliteSessions,

    #[error("session not found in sqlite ledger: {0}")]
    SessionNotFound(String),

    #[error("session already has an active run: {session_id} ({run_id})")]
    SessionActive { session_id: String, run_id: String },

    #[error("question is empty")]
    EmptyQuestion,

    #[error("run did not finish: run canceled")]
    RunCanceled,

    #[error("run did not finish: {0}")]
    RunFailed(String),

    #[error("issue-prep artifact conflict: {0}")]
    IssuePrepArtifactConflict(PathBuf),

    #[error("issue-prep blocked at {stage}: {reasons}; see {run_dir}")]
    IssuePrepBlocked {
        stage: String,
        reasons: String,
        run_dir: PathBuf,
    },

    #[error("daemon lock held at {path}: {owner}")]
    DaemonLockHeld { path: PathBuf, owner: String },

    #[error("daemon protocol error: {0}")]
    DaemonProtocol(String),

    #[error("daemon protocol error {}: {}", .0.code, .0.message)]
    DaemonResponse(ProtocolError),

    #[error("daemon control error: {0}")]
    DaemonControl(String),

    #[error("path escapes workspace: {0}")]
    PathEscapesWorkspace(PathBuf),

    #[error("workspace memory exceeds the {max_bytes}-byte limit: {path}")]
    PlatonicMemoryTooLarge { path: PathBuf, max_bytes: usize },

    #[error("workspace memory is not valid UTF-8: {0}")]
    PlatonicMemoryInvalidUtf8(PathBuf),

    #[error("workspace memory target is not a regular file: {0}")]
    PlatonicMemoryNotRegular(PathBuf),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("toml parse error: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("core error: {0}")]
    Core(#[from] platonic_core::Error),
}
