use crate::offline::OfflineError;
use platonic_client::ClientError;
use platonic_protocol::ProtocolError;
use std::path::PathBuf;

/// Result type for Plato Agent client operations.
pub type AppResult<T> = Result<T, AppError>;

/// Failure returned by the client distribution.
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    /// Client configuration or command selection is invalid.
    #[error("config error: {0}")]
    Config(String),
    /// A daemon response violated the local protocol contract.
    #[error("daemon protocol error: {0}")]
    DaemonProtocol(String),
    /// The daemon returned a typed protocol error response.
    #[error("daemon protocol error {}: {}", .0.code, .0.message)]
    DaemonResponse(ProtocolError),
    /// Daemon control validation or execution failed.
    #[error("daemon control error: {0}")]
    DaemonControl(String),
    /// A completed run was canceled.
    #[error("run did not finish: run canceled")]
    RunCanceled,
    /// A run ended without a successful answer.
    #[error("run did not finish: {0}")]
    RunFailed(String),
    /// Issue preparation stopped on a structural finding.
    #[error("issue-prep blocked at {stage}: {reasons}; see {run_dir}")]
    IssuePrepBlocked {
        /// Pipeline stage that blocked.
        stage: String,
        /// Joined bounded reasons.
        reasons: String,
        /// Artifact directory containing the evidence.
        run_dir: PathBuf,
    },
    /// Local filesystem or IPC I/O failed.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// Protocol or output JSON encoding failed.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    /// Offline replay failed.
    #[error(transparent)]
    Offline(#[from] OfflineError),
    /// The shared typed kernel rejected a protocol identifier or replay stream.
    #[error("core error: {0}")]
    Core(#[from] platonic_core::Error),
}

impl From<ClientError> for AppError {
    fn from(error: ClientError) -> Self {
        match error {
            ClientError::Config(message) => Self::Config(message),
            ClientError::DaemonProtocol(message) => Self::DaemonProtocol(message),
            ClientError::DaemonResponse(error) => Self::DaemonResponse(error),
            ClientError::DaemonControl(message) => Self::DaemonControl(message),
            ClientError::Io(error) => Self::Io(error),
            ClientError::Json(error) => Self::Json(error),
        }
    }
}
