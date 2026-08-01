use plato_agent::{AppError, daemon, paths};
use plato_daemon_client::{ClientError, client::DaemonConnectionConfig};
use plato_protocol::ProtocolError;
use std::path::Path;

#[test]
fn historical_client_transport_lock_and_path_imports_share_extracted_owners() {
    let workspace = tempfile::tempdir().unwrap();
    let config: DaemonConnectionConfig =
        daemon::client::DaemonConnectionConfig::resolve(workspace.path(), None).unwrap();
    assert_eq!(
        paths::workspace_id(&config.workspace_root).unwrap(),
        plato_daemon_client::paths::workspace_id(&config.workspace_root).unwrap()
    );
    assert_eq!(
        paths::default_socket_path(&config.workspace_root).unwrap(),
        plato_daemon_client::paths::default_socket_path(&config.workspace_root).unwrap()
    );
    assert_eq!(
        paths::default_lock_path(&config.workspace_root).unwrap(),
        plato_daemon_client::paths::default_lock_path(&config.workspace_root).unwrap()
    );

    let _: fn(&Path) -> std::io::Result<daemon::transport::Listener> = daemon::transport::bind;
    let metadata: plato_daemon_client::lock::LockMetadata =
        daemon::lock::LockMetadata::for_workspace(&config.workspace_root, &config.socket_path)
            .unwrap();
    assert_eq!(metadata.v, plato_daemon_client::lock::LOCK_VERSION);
}

#[cfg(windows)]
#[test]
fn historical_installer_gate_path_shares_the_extracted_owner() {
    let _: fn() -> std::io::Result<plato_daemon_client::installer_gate::InstallerStartupGate> =
        daemon::installer_gate::InstallerStartupGate::acquire;
}

#[test]
fn root_error_conversion_preserves_variants_and_display() {
    let protocol: AppError = ClientError::DaemonProtocol("bad response".into()).into();
    assert!(matches!(
        protocol,
        AppError::DaemonProtocol(ref message) if message == "bad response"
    ));
    assert_eq!(protocol.to_string(), "daemon protocol error: bad response");

    let response: AppError = ClientError::DaemonResponse(ProtocolError {
        code: "not_found".into(),
        message: "missing".into(),
    })
    .into();
    assert!(matches!(
        response,
        AppError::DaemonResponse(ProtocolError { ref code, ref message })
            if code == "not_found" && message == "missing"
    ));
    assert_eq!(
        response.to_string(),
        "daemon protocol error not_found: missing"
    );

    let control: AppError = ClientError::DaemonControl("invalid lock".into()).into();
    assert!(matches!(
        control,
        AppError::DaemonControl(ref message) if message == "invalid lock"
    ));
    assert_eq!(control.to_string(), "daemon control error: invalid lock");

    let io: AppError = ClientError::from(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "request timed out",
    ))
    .into();
    assert!(matches!(
        io,
        AppError::Io(ref error) if error.kind() == std::io::ErrorKind::TimedOut
    ));
    assert_eq!(io.to_string(), "io error: request timed out");
}
