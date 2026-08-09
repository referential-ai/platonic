#[cfg(test)]
use platonic_protocol::ERROR_NOT_FOUND;
use platonic_protocol::ProtocolError;

/// Result type returned by daemon client operations.
pub type ClientResult<T> = Result<T, ClientError>;

/// Failure returned by daemon discovery, connection, or request handling.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// Endpoint discovery could not resolve required host configuration.
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

    /// Local IPC or filesystem I/O failed.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Protocol JSON encoding or decoding failed.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_matches_the_historical_root_errors() {
        assert_eq!(
            ClientError::Config("missing runtime".into()).to_string(),
            "config error: missing runtime"
        );
        assert_eq!(
            ClientError::DaemonProtocol("bad response".into()).to_string(),
            "daemon protocol error: bad response"
        );
        assert_eq!(
            ClientError::DaemonResponse(ProtocolError {
                code: ERROR_NOT_FOUND,
                message: "missing".into(),
            })
            .to_string(),
            "daemon protocol error not_found: missing"
        );
        assert_eq!(
            ClientError::DaemonControl("invalid lock".into()).to_string(),
            "daemon control error: invalid lock"
        );
        assert_eq!(
            ClientError::from(std::io::Error::other("closed")).to_string(),
            "io error: closed"
        );
    }
}
