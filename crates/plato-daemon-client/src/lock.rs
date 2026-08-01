//! Workspace daemon lock metadata shared by hosts and clients.

use crate::ClientResult;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Current on-disk daemon lock metadata version.
pub const LOCK_VERSION: u32 = 1;

/// Diagnostic identity written into a workspace daemon lock.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LockMetadata {
    /// Lock metadata format version.
    pub v: u32,
    /// Daemon process identifier.
    pub pid: u32,
    /// Absolute daemon executable path when available.
    pub executable: Option<String>,
    /// Canonical workspace root.
    pub workspace_root: String,
    /// Stable identity derived from the canonical workspace root.
    pub workspace_id: String,
    /// Local daemon endpoint path.
    pub socket_path: String,
}

impl LockMetadata {
    /// Builds metadata for the current process and workspace.
    pub fn for_workspace(workspace_root: &Path, socket_path: &Path) -> ClientResult<Self> {
        Ok(Self {
            v: LOCK_VERSION,
            pid: std::process::id(),
            executable: std::env::current_exe()
                .ok()
                .map(|path| path.to_string_lossy().into_owned()),
            workspace_root: workspace_root
                .canonicalize()?
                .to_string_lossy()
                .into_owned(),
            workspace_id: crate::paths::workspace_id(workspace_root)?,
            socket_path: socket_path.to_string_lossy().into_owned(),
        })
    }

    /// Formats the lock owner identity used by root lock-conflict errors.
    pub fn owner_summary(&self) -> String {
        let executable = self.executable.as_deref().unwrap_or("unknown executable");
        format!(
            "pid={}, executable={}, workspace_id={}, socket_path={}",
            self.pid, executable, self.workspace_id, self.socket_path
        )
    }
}
