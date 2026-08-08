use crate::{AppError, AppResult};
#[cfg(windows)]
pub(crate) use platonic_client::paths::runtime_home;
#[cfg(unix)]
pub(crate) use platonic_client::paths::runtime_home_and_fallback;
pub use platonic_client::paths::{
    default_lock_path, default_socket_path, host_lock_path, host_socket_path, workspace_id,
};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DefaultSqlitePath {
    path: PathBuf,
}

impl DefaultSqlitePath {
    pub fn as_path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn from_path(path: PathBuf) -> Self {
        Self { path }
    }
}

/// Absolute path to the server-wide state root, independent of any workspace.
///
/// Holds the workspace registry and every table that spans workspaces. A
/// per-workspace ledger lives under `workspaces/<id>/` beneath this root.
pub fn server_state_root() -> AppResult<PathBuf> {
    let root = state_home()?.join("platonic");
    adopt_legacy_state_root(&root)?;
    Ok(root)
}

/// Move state written under the old `plato-agent` root to the current one.
///
/// The server was renamed to Platonic, and the state root followed. Without
/// this, every ledger a user already has would become invisible rather than
/// merely misfiled. Renaming the directory is atomic, and it happens only when
/// the old root exists and the new one does not, so it runs at most once and
/// never overwrites current state.
fn adopt_legacy_state_root(root: &Path) -> AppResult<()> {
    let Some(parent) = root.parent() else {
        return Ok(());
    };
    let legacy = parent.join("plato-agent");
    if !legacy.is_dir() || root.exists() {
        return Ok(());
    }
    match std::fs::rename(&legacy, root) {
        Ok(()) => Ok(()),
        // Another process may have adopted it first; that is success, not failure.
        Err(_) if root.exists() => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// Absolute path to the server-wide database.
///
/// D005 requires every thread to be enumerable, including clientless threads
/// and orphans. Thread authority therefore cannot live in a per-workspace
/// ledger: an orphan in a workspace nobody has opened would be invisible.
pub fn server_db_path() -> AppResult<PathBuf> {
    Ok(server_state_root()?.join("server.db"))
}

pub fn default_sqlite_path(workspace_root: &Path) -> AppResult<PathBuf> {
    Ok(default_sqlite(workspace_root)?.path)
}

pub fn default_sqlite(workspace_root: &Path) -> AppResult<DefaultSqlitePath> {
    let state_root = server_state_root()?;
    let path = state_root
        .join("workspaces")
        .join(workspace_id(workspace_root)?)
        .join("agent.db");
    Ok(DefaultSqlitePath { path })
}

#[cfg(unix)]
fn state_home() -> AppResult<PathBuf> {
    if let Some(value) = std::env::var_os("XDG_STATE_HOME")
        && !value.is_empty()
    {
        return Ok(PathBuf::from(value));
    }
    let home = std::env::var_os("HOME")
        .ok_or_else(|| AppError::Config("HOME is required for default --db path".into()))?;
    Ok(PathBuf::from(home).join(".local").join("state"))
}

#[cfg(windows)]
fn state_home() -> AppResult<PathBuf> {
    local_app_data("default --db path")
}

#[cfg(windows)]
fn local_app_data(purpose: &str) -> AppResult<PathBuf> {
    let value = std::env::var_os("LOCALAPPDATA")
        .filter(|value| !value.is_empty())
        .ok_or_else(|| AppError::Config(format!("LOCALAPPDATA is required for {purpose}")))?;
    Ok(PathBuf::from(value))
}

#[cfg(test)]
pub(crate) fn with_test_xdg<T>(root: &Path, run: impl FnOnce() -> T) -> T {
    #[cfg(unix)]
    {
        let state_home = root.join("xdg-state");
        let runtime_home = root.join("xdg-runtime");
        temp_env::with_vars(
            [
                ("XDG_STATE_HOME", Some(state_home.as_os_str())),
                ("XDG_RUNTIME_DIR", Some(runtime_home.as_os_str())),
            ],
            run,
        )
    }
    #[cfg(windows)]
    {
        let local_app_data = root.join("local-app-data");
        temp_env::with_var("LOCALAPPDATA", Some(local_app_data.as_os_str()), run)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A user who already has ledgers under the old root keeps them. The
    /// rename must not be a silent data loss.
    #[cfg(unix)]
    #[test]
    fn existing_state_under_the_legacy_root_is_adopted_once_and_never_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        with_test_xdg(dir.path(), || {
            let state_home = dir.path().join("xdg-state");
            let legacy_ledger = state_home
                .join("plato-agent")
                .join("workspaces")
                .join("workspace-abc")
                .join("agent.db");
            std::fs::create_dir_all(legacy_ledger.parent().unwrap()).unwrap();
            std::fs::write(&legacy_ledger, b"original ledger bytes").unwrap();

            let root = server_state_root().unwrap();
            assert_eq!(root, state_home.join("platonic"));
            let adopted = root
                .join("workspaces")
                .join("workspace-abc")
                .join("agent.db");
            assert_eq!(std::fs::read(&adopted).unwrap(), b"original ledger bytes");
            assert!(!state_home.join("plato-agent").exists());

            // A second legacy root appearing later must not clobber current state.
            std::fs::create_dir_all(state_home.join("plato-agent")).unwrap();
            std::fs::write(state_home.join("plato-agent").join("stray"), b"stray").unwrap();
            assert_eq!(server_state_root().unwrap(), root);
            assert_eq!(std::fs::read(&adopted).unwrap(), b"original ledger bytes");
        });
    }

    #[test]
    fn default_sqlite_path_uses_workspace_directory() {
        let dir = tempfile::tempdir().unwrap();
        with_test_xdg(dir.path(), || {
            let path = default_sqlite_path(dir.path()).unwrap();

            assert!(
                path.components()
                    .any(|component| component.as_os_str() == "platonic")
            );
            assert!(
                path.components()
                    .any(|component| component.as_os_str() == "workspaces")
            );
            assert_eq!(path.file_name().unwrap(), "agent.db");
        });
    }

    #[cfg(unix)]
    #[test]
    fn default_socket_and_lock_paths_use_workspace_directory() {
        let dir = tempfile::tempdir().unwrap();
        with_test_xdg(dir.path(), || {
            let socket_path = default_socket_path(dir.path()).unwrap();
            let lock_path = default_lock_path(dir.path()).unwrap();

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
        });
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
                assert_eq!(
                    default_sqlite_path(workspace.path()).unwrap(),
                    workspace_dir.join("agent.db")
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
