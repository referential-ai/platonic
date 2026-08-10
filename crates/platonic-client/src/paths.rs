//! Host endpoint discovery and compatibility workspace identity.

use crate::ClientResult;
use sha2::{Digest, Sha256};
use std::{
    fmt::Write as _,
    path::{Path, PathBuf},
};

/// Returns the stable host-scoped local daemon endpoint.
#[cfg(unix)]
pub fn host_socket_path() -> ClientResult<PathBuf> {
    Ok(runtime_home()?
        .join("platonic")
        .join("host")
        .join("agent.sock"))
}

/// Returns the stable host-scoped daemon lock path.
pub fn host_lock_path() -> ClientResult<PathBuf> {
    Ok(runtime_home()?
        .join("platonic")
        .join("host")
        .join("agent.lock"))
}

/// Returns the server-wide registry path for offline workspace resolution.
pub fn server_db_path() -> ClientResult<PathBuf> {
    Ok(state_home()?.join("platonic").join("server.db"))
}

/// Returns the stable identity for a canonical workspace path.
pub fn workspace_id(workspace_root: &Path) -> ClientResult<String> {
    let canonical = workspace_root.canonicalize()?;
    Ok(workspace_id_from_canonical_path(&canonical))
}

fn workspace_id_from_canonical_path(path: &Path) -> String {
    let basename = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("workspace");
    let slug = slug(basename);
    format!("{slug}-{}", hash16(path))
}

fn slug(value: &str) -> String {
    let mut output = String::new();
    let mut last_was_dash = false;
    for character in value.chars().flat_map(char::to_lowercase) {
        if character.is_ascii_alphanumeric() {
            output.push(character);
            last_was_dash = false;
        } else if !last_was_dash && !output.is_empty() {
            output.push('-');
            last_was_dash = true;
        }
    }
    while output.ends_with('-') {
        output.pop();
    }
    if output.is_empty() {
        "workspace".into()
    } else {
        output
    }
}

fn hash16(path: &Path) -> String {
    let digest = Sha256::digest(path_bytes(path));
    let mut output = String::with_capacity(16);
    for byte in &digest[..8] {
        write!(&mut output, "{byte:02x}").expect("writing to a string cannot fail");
    }
    output
}

#[cfg(unix)]
fn path_bytes(path: &Path) -> &[u8] {
    use std::os::unix::ffi::OsStrExt;
    path.as_os_str().as_bytes()
}

/// Returns the platform daemon runtime home.
#[cfg(unix)]
pub fn runtime_home() -> ClientResult<PathBuf> {
    Ok(runtime_home_and_fallback().0)
}

#[cfg(unix)]
fn state_home() -> ClientResult<PathBuf> {
    if let Some(value) = std::env::var_os("XDG_STATE_HOME").filter(|value| !value.is_empty()) {
        return Ok(PathBuf::from(value));
    }
    let home = std::env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .ok_or_else(|| crate::ClientError::Config("HOME is required for ledger replay".into()))?;
    Ok(PathBuf::from(home).join(".local").join("state"))
}

/// Returns the Unix runtime home and whether it is the system-temp fallback.
#[cfg(unix)]
pub fn runtime_home_and_fallback() -> (PathBuf, bool) {
    match std::env::var_os("XDG_RUNTIME_DIR").filter(|value| !value.is_empty()) {
        Some(value) => (PathBuf::from(value), false),
        None => (
            std::env::temp_dir().join(format!(
                "plato-agent-{}",
                rustix::process::geteuid().as_raw()
            )),
            true,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_id_uses_slug_and_hash16() {
        let id = workspace_id_from_canonical_path(Path::new("/tmp/Platonic Workspace"));

        #[cfg(unix)]
        assert_eq!(id, "platonic-workspace-d9c8fc148a872529");
    }

    #[cfg(unix)]
    #[test]
    fn server_database_path_uses_the_server_state_root() {
        let state_home = tempfile::tempdir().unwrap();
        temp_env::with_var(
            "XDG_STATE_HOME",
            Some(state_home.path().as_os_str()),
            || {
                assert_eq!(
                    server_db_path().unwrap(),
                    state_home.path().join("platonic/server.db")
                );
            },
        );
    }

    #[cfg(unix)]
    #[test]
    fn host_paths_are_stable_and_outside_workspace_directories() {
        let runtime_home = tempfile::tempdir().unwrap();
        temp_env::with_var(
            "XDG_RUNTIME_DIR",
            Some(runtime_home.path().as_os_str()),
            || {
                let socket_path = host_socket_path().unwrap();
                let lock_path = host_lock_path().unwrap();

                assert_eq!(
                    socket_path,
                    runtime_home
                        .path()
                        .join("platonic")
                        .join("host")
                        .join("agent.sock")
                );
                assert_eq!(lock_path, socket_path.with_file_name("agent.lock"));
                assert!(
                    !socket_path
                        .components()
                        .any(|part| part.as_os_str() == "workspaces")
                );
            },
        );
    }
}
