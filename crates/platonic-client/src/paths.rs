//! Workspace identity and daemon endpoint discovery.

#[cfg(windows)]
use crate::ClientError;
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

/// Returns the stable host-scoped local daemon endpoint.
#[cfg(windows)]
pub fn host_socket_path() -> ClientResult<PathBuf> {
    Ok(PathBuf::from(r"\\.\pipe\plato-agent-host"))
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

/// Returns the default local daemon endpoint for a workspace.
#[cfg(unix)]
pub fn default_socket_path(workspace_root: &Path) -> ClientResult<PathBuf> {
    Ok(runtime_home()?
        .join("platonic")
        .join("workspaces")
        .join(workspace_id(workspace_root)?)
        .join("agent.sock"))
}

/// Returns the default local daemon endpoint for a workspace.
#[cfg(windows)]
pub fn default_socket_path(workspace_root: &Path) -> ClientResult<PathBuf> {
    Ok(PathBuf::from(format!(
        r"\\.\pipe\plato-agent-{}",
        workspace_id(workspace_root)?
    )))
}

/// Returns the default daemon lock path for a workspace.
pub fn default_lock_path(workspace_root: &Path) -> ClientResult<PathBuf> {
    Ok(runtime_home()?
        .join("platonic")
        .join("workspaces")
        .join(workspace_id(workspace_root)?)
        .join("agent.lock"))
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
    #[cfg(windows)]
    let slug: String = slug.chars().take(64).collect();
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

#[cfg(windows)]
fn path_bytes(path: &Path) -> Vec<u8> {
    use std::os::windows::ffi::OsStrExt;

    path.as_os_str()
        .encode_wide()
        .flat_map(u16::to_le_bytes)
        .collect()
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

/// Returns the Windows daemon runtime home.
#[cfg(windows)]
pub fn runtime_home() -> ClientResult<PathBuf> {
    local_app_data("default daemon runtime path")
}

#[cfg(windows)]
fn state_home() -> ClientResult<PathBuf> {
    local_app_data("default ledger replay path")
}

#[cfg(windows)]
fn local_app_data(purpose: &str) -> ClientResult<PathBuf> {
    let value = std::env::var_os("LOCALAPPDATA")
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ClientError::Config(format!("LOCALAPPDATA is required for {purpose}")))?;
    Ok(PathBuf::from(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_id_uses_slug_and_hash16() {
        let id = workspace_id_from_canonical_path(Path::new("/tmp/Platonic Workspace"));

        #[cfg(unix)]
        assert_eq!(id, "platonic-workspace-d9c8fc148a872529");
        #[cfg(windows)]
        assert_eq!(id, "platonic-workspace-bd545284429294c3");
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

    #[cfg(unix)]
    #[test]
    fn default_socket_and_lock_paths_use_workspace_directory() {
        let workspace = tempfile::tempdir().unwrap();
        let runtime_home = tempfile::tempdir().unwrap();
        temp_env::with_var(
            "XDG_RUNTIME_DIR",
            Some(runtime_home.path().as_os_str()),
            || {
                let socket_path = default_socket_path(workspace.path()).unwrap();
                let lock_path = default_lock_path(workspace.path()).unwrap();

                assert!(
                    socket_path
                        .components()
                        .any(|component| component.as_os_str() == "platonic")
                );
                assert!(
                    socket_path
                        .components()
                        .any(|component| component.as_os_str() == "workspaces")
                );
                assert_eq!(socket_path.file_name().unwrap(), "agent.sock");
                assert_eq!(lock_path.file_name().unwrap(), "agent.lock");
                assert_eq!(socket_path.parent(), lock_path.parent());
            },
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_host_paths_are_stable_and_outside_workspace_directories() {
        let local_app_data = tempfile::tempdir().unwrap();
        temp_env::with_var(
            "LOCALAPPDATA",
            Some(local_app_data.path().as_os_str()),
            || {
                assert_eq!(
                    host_socket_path().unwrap(),
                    PathBuf::from(r"\\.\pipe\plato-agent-host")
                );
                assert_eq!(
                    host_lock_path().unwrap(),
                    local_app_data
                        .path()
                        .join("platonic")
                        .join("host")
                        .join("agent.lock")
                );
            },
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_paths_use_local_app_data_and_workspace_pipe() {
        let workspace = tempfile::tempdir().unwrap();
        let local_app_data = tempfile::tempdir().unwrap();
        temp_env::with_var(
            "LOCALAPPDATA",
            Some(local_app_data.path().as_os_str()),
            || {
                let workspace_id = workspace_id(workspace.path()).unwrap();
                let workspace_dir = local_app_data
                    .path()
                    .join("platonic")
                    .join("workspaces")
                    .join(&workspace_id);

                assert_eq!(
                    default_socket_path(workspace.path()).unwrap(),
                    PathBuf::from(format!(r"\\.\pipe\plato-agent-{workspace_id}"))
                );
                assert_eq!(
                    default_lock_path(workspace.path()).unwrap(),
                    workspace_dir.join("agent.lock")
                );
            },
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_pipe_endpoint_is_bounded_for_long_workspace_names() {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace-".repeat(20));
        std::fs::create_dir(&workspace).unwrap();

        let endpoint = default_socket_path(&workspace).unwrap();
        let endpoint = endpoint.to_string_lossy();
        let workspace_id = workspace_id(&workspace).unwrap();

        assert_eq!(endpoint, format!(r"\\.\pipe\plato-agent-{workspace_id}"));
        assert!(endpoint.encode_utf16().count() <= 102);
        assert!(workspace_id.len() <= 81);
    }

    #[cfg(windows)]
    #[test]
    fn windows_paths_require_local_app_data() {
        let workspace = tempfile::tempdir().unwrap();
        temp_env::with_var_unset("LOCALAPPDATA", || {
            let error = default_lock_path(workspace.path()).unwrap_err();

            assert!(error.to_string().contains("LOCALAPPDATA"));
        });
    }
}
