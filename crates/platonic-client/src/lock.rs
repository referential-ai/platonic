//! Host process lock metadata shared by servers and local lifecycle clients.

use serde::{Deserialize, Serialize};
use std::path::Path;

/// Current on-disk daemon lock metadata version.
pub const LOCK_VERSION: u32 = 2;

/// Diagnostic identity written into the host server process lock.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LockMetadata {
    /// Lock metadata format version.
    pub v: u32,
    /// Daemon process identifier.
    pub pid: u32,
    /// Absolute daemon executable path when available.
    pub executable: Option<String>,
    /// Stable host-local server endpoint.
    pub endpoint: String,
}

impl LockMetadata {
    /// Builds metadata for the current process and host endpoint.
    pub fn for_host(endpoint: &Path) -> Self {
        Self {
            v: LOCK_VERSION,
            pid: std::process::id(),
            executable: std::env::current_exe()
                .ok()
                .map(|path| path.to_string_lossy().into_owned()),
            endpoint: endpoint.to_string_lossy().into_owned(),
        }
    }

    /// Formats the process identity used by lock-conflict errors.
    pub fn owner_summary(&self) -> String {
        let executable = self.executable.as_deref().unwrap_or("unknown executable");
        format!(
            "pid={}, executable={}, endpoint={}",
            self.pid, executable, self.endpoint
        )
    }
}
