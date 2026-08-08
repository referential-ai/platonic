#![cfg(unix)]

use platonic_server::daemon::server::DaemonServer;
use std::{
    env,
    fs::{self, Permissions},
    os::unix::fs::{MetadataExt, PermissionsExt, symlink},
    path::{Path, PathBuf},
    process::Command,
};

const CHILD_ENV: &str = "PLATO_TEST_UNIX_RUNTIME_FALLBACK_CHILD";
const CHILD_PROOF_ENV: &str = "PLATO_TEST_UNIX_RUNTIME_FALLBACK_PROOF";
const PRIVATE_DIRECTORY_MODE: u32 = 0o700;

#[test]
fn no_xdg_runtime_fallback_is_private_and_safe() {
    let temp_root = tempfile::tempdir().unwrap();
    let proof = temp_root.path().join("proof");
    let output = Command::new(env::current_exe().unwrap())
        .args(["--ignored", "--exact", "no_xdg_runtime_fallback_child"])
        .env(CHILD_ENV, "1")
        .env(CHILD_PROOF_ENV, &proof)
        .env_remove("XDG_RUNTIME_DIR")
        .env("XDG_STATE_HOME", temp_root.path().join("state"))
        .env("TMPDIR", temp_root.path())
        .env("USER", "spoofed-name")
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "child proof failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(proof.exists(), "child proof did not run");
}

#[test]
#[ignore = "runs in an isolated child process"]
fn no_xdg_runtime_fallback_child() {
    assert_eq!(env::var(CHILD_ENV).as_deref(), Ok("1"));
    let temp_root = PathBuf::from(env::var_os("TMPDIR").unwrap());
    let runtime_home = fallback_runtime_home(&temp_root);

    let workspace = tempfile::tempdir().unwrap();
    fs::create_dir(&runtime_home).unwrap();
    fs::set_permissions(&runtime_home, Permissions::from_mode(0o755)).unwrap();

    let first = DaemonServer::bind(workspace.path(), None).unwrap();
    let socket_path = first.paths().socket_path.clone();
    assert!(socket_path.starts_with(runtime_home.join("platonic")));
    assert!(socket_path.exists());
    assert_eq!(mode(&runtime_home), PRIVATE_DIRECTORY_MODE);
    assert_eq!(
        fs::symlink_metadata(&runtime_home).unwrap().uid(),
        rustix::process::geteuid().as_raw()
    );
    drop(first);

    let second = DaemonServer::bind(workspace.path(), None).unwrap();
    assert_eq!(second.paths().socket_path, socket_path);
    assert!(socket_path.exists());
    assert_eq!(mode(&runtime_home), PRIVATE_DIRECTORY_MODE);
    drop(second);
    fs::remove_dir_all(&runtime_home).unwrap();

    let workspace = tempfile::tempdir().unwrap();
    let target = temp_root.join("target");
    fs::create_dir(&target).unwrap();
    symlink(&target, &runtime_home).unwrap();

    let error = DaemonServer::bind(workspace.path(), None).unwrap_err();

    assert!(error.to_string().contains("not a real directory"));
    assert!(!target.join("platonic").exists());
    fs::remove_file(&runtime_home).unwrap();

    let workspace = tempfile::tempdir().unwrap();
    fs::write(&runtime_home, b"not a directory").unwrap();

    let error = DaemonServer::bind(workspace.path(), None).unwrap_err();

    assert!(error.to_string().contains("not a real directory"));
    assert!(!runtime_home.join("platonic").exists());
    fs::write(env::var_os(CHILD_PROOF_ENV).unwrap(), b"ok").unwrap();
}

fn fallback_runtime_home(temp_root: &Path) -> PathBuf {
    temp_root.join(format!(
        "plato-agent-{}",
        rustix::process::geteuid().as_raw()
    ))
}

fn mode(path: &Path) -> u32 {
    fs::symlink_metadata(path).unwrap().permissions().mode() & 0o777
}
