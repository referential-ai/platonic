use crate::{AppResult, paths};
use std::path::{Path, PathBuf};

pub mod client;
#[cfg(windows)]
pub mod control;
mod handlers;
#[cfg(windows)]
pub mod installer_gate;
pub mod lock;
pub mod protocol;
mod runtime;
pub mod server;
pub(crate) mod transport;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DaemonPaths {
    pub workspace_root: PathBuf,
    pub workspace_id: String,
    pub socket_path: PathBuf,
    pub lock_path: PathBuf,
    pub ledger_path: PathBuf,
}

impl DaemonPaths {
    pub fn resolve(workspace_root: &Path, socket_path: Option<PathBuf>) -> AppResult<Self> {
        let workspace_root = workspace_root.canonicalize()?;
        let workspace_id = paths::workspace_id(&workspace_root)?;
        let socket_path = socket_path.unwrap_or(paths::default_socket_path(&workspace_root)?);
        Ok(Self {
            lock_path: paths::default_lock_path(&workspace_root)?,
            ledger_path: paths::default_sqlite_path(&workspace_root)?,
            workspace_root,
            workspace_id,
            socket_path,
        })
    }

    pub(crate) fn default_ledger(&self) -> paths::DefaultSqlitePath {
        paths::DefaultSqlitePath::from_path(self.ledger_path.clone())
    }
}

pub fn wake_listener(endpoint: &std::path::Path) {
    transport::wake(endpoint);
}
