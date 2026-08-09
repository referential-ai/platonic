use plato_agent::AppError;
use platonic_client::{
    ClientError,
    client::DaemonConnectionConfig,
    lock::{LOCK_VERSION, LockMetadata},
    paths, transport,
};
use platonic_protocol::{ERROR_NOT_FOUND, ProtocolError};
use std::path::Path;

#[test]
fn historical_client_transport_lock_and_path_imports_share_extracted_owners() {
    let workspace = tempfile::tempdir().unwrap();
    let config = DaemonConnectionConfig::resolve(workspace.path(), None).unwrap();
    assert_eq!(
        paths::workspace_id(&config.workspace_root).unwrap(),
        platonic_client::paths::workspace_id(&config.workspace_root).unwrap()
    );
    assert_eq!(
        paths::default_socket_path(&config.workspace_root).unwrap(),
        platonic_client::paths::default_socket_path(&config.workspace_root).unwrap()
    );
    let _: fn(&Path) -> std::io::Result<transport::Listener> = transport::bind;
    let metadata =
        LockMetadata::for_workspace(&config.workspace_root, &config.socket_path).unwrap();
    assert_eq!(metadata.v, LOCK_VERSION);
}

#[cfg(windows)]
#[test]
fn historical_installer_gate_path_shares_the_extracted_owner() {
    let _: fn() -> std::io::Result<platonic_client::installer_gate::InstallerStartupGate> =
        platonic_client::installer_gate::InstallerStartupGate::acquire;
}

#[test]
fn root_error_conversion_preserves_variants_and_display() {
    let config: AppError = ClientError::Config("missing runtime".into()).into();
    assert!(matches!(config, AppError::Config(ref message) if message == "missing runtime"));
    assert_eq!(config.to_string(), "config error: missing runtime");

    let protocol: AppError = ClientError::DaemonProtocol("bad response".into()).into();
    assert!(matches!(protocol, AppError::DaemonProtocol(ref message) if message == "bad response"));
    assert_eq!(protocol.to_string(), "daemon protocol error: bad response");

    let response: AppError = ClientError::DaemonResponse(ProtocolError {
        code: ERROR_NOT_FOUND,
        message: "missing".into(),
    })
    .into();
    assert!(matches!(
        response,
        AppError::DaemonResponse(ProtocolError { ref code, ref message })
            if *code == ERROR_NOT_FOUND && message == "missing"
    ));
    assert_eq!(
        response.to_string(),
        "daemon protocol error not_found: missing"
    );
}
