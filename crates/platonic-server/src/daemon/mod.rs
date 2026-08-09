use crate::{AppResult, paths};
use std::path::{Path, PathBuf};

pub use platonic_client::{client, transport};
mod child_process;
#[cfg(windows)]
pub mod control;
mod handlers;
#[cfg(windows)]
pub use platonic_client::installer_gate;
pub mod lock;
pub use platonic_protocol as protocol;
mod run_child;
mod runtime;
pub mod server;

pub use run_child::run_stdio_child;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DaemonPaths {
    pub workspace_root: PathBuf,
    pub workspace_id: String,
    pub socket_path: PathBuf,
    pub lock_path: PathBuf,
    pub ledger_path: PathBuf,
    /// The host's server-wide store. One per host, not one per workspace —
    /// D005 requires threads to be enumerable from outside their workspace.
    pub server_db_path: PathBuf,
}

impl DaemonPaths {
    pub fn resolve(workspace_root: &Path, socket_path: Option<PathBuf>) -> AppResult<Self> {
        Self::provisional(workspace_root, socket_path)?.resolve_workspace_record()
    }

    pub(crate) fn provisional(
        workspace_root: &Path,
        socket_path: Option<PathBuf>,
    ) -> AppResult<Self> {
        let workspace_root = workspace_root.canonicalize()?;
        let socket_path = socket_path.unwrap_or(paths::default_socket_path(&workspace_root)?);
        Ok(Self {
            lock_path: paths::default_lock_path(&workspace_root)?,
            ledger_path: PathBuf::new(),
            server_db_path: paths::server_db_path()?,
            workspace_id: paths::workspace_id(&workspace_root)?,
            workspace_root,
            socket_path,
        })
    }

    pub(crate) fn resolve_workspace_record(self) -> AppResult<Self> {
        let store = self.server_store()?;
        let record = store.workspace_by_root(&self.workspace_root.to_string_lossy())?;
        match record {
            Some(record) => Ok(self.with_workspace_record(&record)),
            None => Ok(self),
        }
    }

    pub(crate) fn with_workspace_record(
        &self,
        record: &crate::server_store::WorkspaceRecord,
    ) -> Self {
        Self {
            workspace_root: self.workspace_root.clone(),
            workspace_id: record.id.clone(),
            socket_path: self.socket_path.clone(),
            lock_path: self.lock_path.clone(),
            ledger_path: PathBuf::from(&record.ledger_path),
            server_db_path: self.server_db_path.clone(),
        }
    }

    pub(crate) fn is_registered(&self) -> bool {
        !self.ledger_path.as_os_str().is_empty()
    }

    pub(crate) fn default_ledger(&self) -> paths::DefaultSqlitePath {
        paths::DefaultSqlitePath::from_path(self.ledger_path.clone())
    }

    pub(crate) fn server_store(&self) -> AppResult<crate::server_store::ServerStore> {
        crate::server_store::ServerStore::open_or_create(&self.server_db_path)
    }
}

pub fn wake_listener(endpoint: &std::path::Path) {
    transport::wake(endpoint);
}
