use platonic_client::ClientError;

/// Result type returned by Discord gateway operations.
pub type GatewayResult<T> = Result<T, GatewayError>;

/// Failure returned by Discord REST, WebSocket, or daemon-bridge work.
#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    /// Discord rejected a request or violated its gateway contract.
    #[error("provider error: {0}")]
    Discord(String),

    /// A daemon run reached an invalid terminal readback state.
    #[error("run did not finish: {0}")]
    RunFailed(String),

    /// Daemon identity, capability, or readback violated the gateway contract.
    #[error("daemon protocol error: {0}")]
    DaemonProtocol(String),

    /// The local model override was empty after trimming.
    #[error("core error: ModelName cannot be empty")]
    EmptyModelName,

    /// A daemon client operation failed.
    #[error(transparent)]
    Client(#[from] ClientError),

    /// Gateway thread or socket I/O failed.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Discord payload encoding or decoding failed.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use platonic_protocol::{ERROR_NOT_FOUND, ProtocolError};

    #[test]
    fn display_matches_the_historical_root_errors() {
        assert_eq!(
            GatewayError::Discord("discord failed".into()).to_string(),
            "provider error: discord failed"
        );
        assert_eq!(
            GatewayError::RunFailed("missing answer".into()).to_string(),
            "run did not finish: missing answer"
        );
        assert_eq!(
            GatewayError::DaemonProtocol("bad response".into()).to_string(),
            "daemon protocol error: bad response"
        );
        assert_eq!(
            GatewayError::EmptyModelName.to_string(),
            "core error: ModelName cannot be empty"
        );
        assert_eq!(
            GatewayError::from(ClientError::DaemonResponse(ProtocolError {
                code: ERROR_NOT_FOUND,
                message: "missing".into(),
            }))
            .to_string(),
            "daemon protocol error not_found: missing"
        );
        assert_eq!(
            GatewayError::from(std::io::Error::other("closed")).to_string(),
            "io error: closed"
        );
    }
}
