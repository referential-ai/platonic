use platonic_core::{AgentId, EffectClass};
use platonic_protocol::{ReasoningEffort, ThreadApprovalPolicy, ThreadAuthorityRecord};
use std::path::Path;

/// A registered workspace: a named directory the server knows about.
///
/// The name is the handle operators use; the root is the directory it wears.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkspaceRecord {
    /// Minted once and never derived from the path (P021). A workspace that
    /// moves keeps its identity and its history; only `root` changes.
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) root: String,
    pub(crate) ledger_path: String,
    pub(crate) created_at_ms: u64,
}

/// One immutable configured agent profile in server-wide state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AgentRecord {
    pub(crate) id: AgentId,
    pub(crate) workspace_id: String,
    pub(crate) model: String,
    pub(crate) reasoning_effort: ReasoningEffort,
    pub(crate) approval_policy: ThreadApprovalPolicy,
    pub(crate) toolset: Vec<String>,
    pub(crate) created_at_ms: u64,
}

/// Whether a registered workspace's directory is still where the registry says.
///
/// A workspace whose directory has vanished is reported broken, never omitted
/// and never auto-removed (P021): its ledger is retained and spawning into it
/// fails at the gate rather than silently resurrecting an empty workspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkspaceHealth {
    Present,
    Broken,
}

/// A tool-call approval as it exists on disk: what was asked, and — once a
/// client decides — what was answered.
///
/// An approval outlives the daemon that requested it. The run it belongs to
/// does not: its child process dies with the daemon, so the run is recorded
/// interrupted while the approval stays readable and resolvable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ToolCallApprovalRecord {
    pub(crate) run_id: String,
    pub(crate) call_id: String,
    pub(crate) session_id: String,
    pub(crate) tool_name: String,
    pub(crate) effect: EffectClass,
    pub(crate) reason: String,
    pub(crate) input_preview: Option<String>,
    pub(crate) approval_preview: Option<String>,
    pub(crate) diff_preview: Option<String>,
    pub(crate) requested_at_ms: u64,
    pub(crate) decision: Option<ToolCallApprovalDecision>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ToolCallApprovalDecision {
    pub(crate) granted: bool,
    pub(crate) actor: String,
    pub(crate) reason: Option<String>,
    pub(crate) decided_at_ms: u64,
}

/// The first attributed request to cancel one run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RunCancellationRecord {
    pub(crate) run_id: String,
    pub(crate) actor: String,
    pub(crate) requested_at_ms: u64,
}

/// A thread authority record proven to be durably written.
///
/// The type exists so a caller cannot mistake an in-memory record for one that
/// survived the write D005 requires before the first turn executes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DurableThreadAuthority(pub(super) ThreadAuthorityRecord);

impl DurableThreadAuthority {
    pub(crate) fn record(&self) -> &ThreadAuthorityRecord {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BranchClaimRecord {
    pub(crate) workspace_id: String,
    pub(crate) repo: String,
    pub(crate) branch: String,
    pub(crate) thread_id: String,
    pub(crate) claimed_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("branch {branch} in repository {repo} is already claimed by live thread {thread_id}")]
pub(crate) struct BranchClaimConflict {
    pub(crate) repo: String,
    pub(crate) branch: String,
    pub(crate) thread_id: String,
}

/// Mint a workspace id that does not depend on where the workspace lives.
///
/// Deliberately not derived from the path, unlike `paths::workspace_id` (P021):
/// a workspace that moves must keep its identity and its history rather than
/// silently becoming a new, empty one.
pub(crate) fn mint_workspace_id(name: &str, created_at_ms: u64) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(name.as_bytes());
    hasher.update(b"\0");
    hasher.update(created_at_ms.to_be_bytes());
    let digest = hasher.finalize();
    let hex: String = digest
        .iter()
        .take(8)
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("ws-{hex}")
}

impl WorkspaceRecord {
    /// A workspace is broken when the directory it points at is gone.
    ///
    /// Checked at read time rather than stored, because the filesystem can
    /// change without the server running; a cached flag would lie.
    pub(crate) fn health(&self) -> WorkspaceHealth {
        if Path::new(&self.root).is_dir() {
            WorkspaceHealth::Present
        } else {
            WorkspaceHealth::Broken
        }
    }
}
