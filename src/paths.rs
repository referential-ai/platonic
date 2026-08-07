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

pub fn default_sqlite_path(workspace_root: &Path) -> AppResult<PathBuf> {
    Ok(default_sqlite(workspace_root)?.path)
}

pub fn default_sqlite(workspace_root: &Path) -> AppResult<DefaultSqlitePath> {
    let state_root = state_home()?.join("plato-agent");
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

    #[test]
    fn default_sqlite_path_uses_workspace_directory() {
        let dir = tempfile::tempdir().unwrap();
        with_test_xdg(dir.path(), || {
            let path = default_sqlite_path(dir.path()).unwrap();

            assert!(
                path.components()
                    .any(|component| component.as_os_str() == "plato-agent")
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
                    .any(|component| component.as_os_str() == "plato-agent")
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
                    .join("plato-agent")
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
