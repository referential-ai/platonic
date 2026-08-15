use platonic_core::{AgentId, EffectClass, ProfileId};
use platonic_protocol::{
    ReasoningEffort, ThreadApprovalPolicy, ThreadAuthorityRecord, ThreadRepositoryRequest,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

pub(crate) const MAX_PROFILE_MARKDOWN_BYTES: usize = 128 * 1024;
pub(crate) const MAX_PROFILE_SKILL_REFS: usize = 64;
pub(crate) const MAX_PROFILE_REVISION_BYTES: usize = 512 * 1024;
pub(crate) const MAX_PROFILE_REVISION_ACTOR_BYTES: usize = 256;
pub(crate) const MAX_PROFILE_LIST_ENTRIES: usize = 100;
pub(crate) const MAX_CHILD_RETURN_PAYLOAD_BYTES: usize = 64 * 1024;
pub(crate) const MAX_UNCONSUMED_NONTERMINAL_RETURNS: usize = 128;

static PROFILE_ID_SEQUENCE: AtomicU64 = AtomicU64::new(0);

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

/// The only content admitted to one immutable profile revision in phase 1.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProfileRevisionContent {
    pub(crate) instructions_markdown: String,
    pub(crate) memory_markdown: String,
    pub(crate) skill_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ProfileValidationError {
    #[error("profile instructions exceed the {MAX_PROFILE_MARKDOWN_BYTES}-byte limit")]
    InstructionsTooLarge,
    #[error("profile memory exceeds the {MAX_PROFILE_MARKDOWN_BYTES}-byte limit")]
    MemoryTooLarge,
    #[error("profile skill references exceed the {MAX_PROFILE_SKILL_REFS}-entry limit")]
    TooManySkillRefs,
    #[error("profile revision exceeds the {MAX_PROFILE_REVISION_BYTES}-byte serialized limit")]
    RevisionTooLarge,
    #[error("profile revision could not be serialized: {0}")]
    Serialization(String),
}

/// Stable profile metadata. Content lives in immutable revision rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProfileRecord {
    pub(crate) id: ProfileId,
    pub(crate) workspace_id: String,
    pub(crate) display_name: String,
    pub(crate) model: String,
    pub(crate) reasoning_effort: ReasoningEffort,
    pub(crate) approval_policy: ThreadApprovalPolicy,
    pub(crate) toolset: Vec<String>,
    pub(crate) current_revision: u64,
    pub(crate) home_thread_id: Option<String>,
    pub(crate) imported_agent_id: Option<String>,
    pub(crate) created_at_ms: u64,
}

/// One immutable profile-content revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProfileRevisionRecord {
    pub(crate) profile_id: ProfileId,
    pub(crate) revision: u64,
    pub(crate) parent_revision: Option<u64>,
    pub(crate) actor: String,
    pub(crate) created_at_ms: u64,
    pub(crate) content_hash: String,
    pub(crate) content: ProfileRevisionContent,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ChildReturnKind {
    Progress,
    Question,
    Result,
    Failed,
}

impl ChildReturnKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Progress => "progress",
            Self::Question => "question",
            Self::Result => "result",
            Self::Failed => "failed",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "progress" => Some(Self::Progress),
            "question" => Some(Self::Question),
            "result" => Some(Self::Result),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }

    pub(crate) const fn is_terminal(self) -> bool {
        matches!(self, Self::Result | Self::Failed)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ParentAnswerKind {
    Answer,
    FollowUp,
}

impl ParentAnswerKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Answer => "answer",
            Self::FollowUp => "follow_up",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "answer" => Some(Self::Answer),
            "follow_up" => Some(Self::FollowUp),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DeliveryState {
    Available,
    Reserved,
    Consumed,
}

impl DeliveryState {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "available" => Some(Self::Available),
            "reserved" => Some(Self::Reserved),
            "consumed" => Some(Self::Consumed),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct ChildReturnRecord {
    pub(crate) message_id: String,
    pub(crate) sequence: u64,
    pub(crate) spawn_id: String,
    pub(crate) profile_id: ProfileId,
    pub(crate) parent_thread_id: String,
    pub(crate) child_thread_id: String,
    pub(crate) source_run_id: Option<String>,
    pub(crate) source_turn_id: Option<String>,
    pub(crate) kind: ChildReturnKind,
    pub(crate) payload: String,
    pub(crate) artifact_refs: Vec<String>,
    pub(crate) truncated: bool,
    pub(crate) created_at_ms: u64,
    pub(crate) profile_revision: u64,
    pub(crate) state: DeliveryState,
    pub(crate) reserved_by_run_id: Option<String>,
    pub(crate) reserved_by_turn_id: Option<String>,
    pub(crate) consumed_by_run_id: Option<String>,
    pub(crate) consumed_by_turn_id: Option<String>,
    pub(crate) consumed_at_ms: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct ParentAnswerRecord {
    pub(crate) message_id: String,
    pub(crate) sequence: u64,
    pub(crate) spawn_id: String,
    pub(crate) profile_id: ProfileId,
    pub(crate) parent_thread_id: String,
    pub(crate) child_thread_id: String,
    pub(crate) source_run_id: String,
    pub(crate) source_turn_id: String,
    pub(crate) kind: ParentAnswerKind,
    pub(crate) payload: String,
    pub(crate) created_at_ms: u64,
    pub(crate) profile_revision: u64,
    pub(crate) state: DeliveryState,
    pub(crate) reserved_by_run_id: Option<String>,
    pub(crate) reserved_by_turn_id: Option<String>,
    pub(crate) consumed_by_run_id: Option<String>,
    pub(crate) consumed_by_turn_id: Option<String>,
    pub(crate) consumed_at_ms: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ThreadRunAdmission {
    pub(crate) run_id: String,
    pub(crate) workspace_id: String,
    pub(crate) profile_id: ProfileId,
    pub(crate) thread_id: String,
    pub(crate) thread_turn_id: String,
    pub(crate) profile_revision: u64,
    pub(crate) created_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ChildReturnDraft {
    pub(crate) message_id: String,
    pub(crate) workspace_id: String,
    pub(crate) child_thread_id: String,
    pub(crate) source_run_id: Option<String>,
    pub(crate) source_turn_id: Option<String>,
    pub(crate) kind: ChildReturnKind,
    pub(crate) payload: String,
    pub(crate) artifact_refs: Vec<String>,
    pub(crate) created_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ParentAnswerDraft {
    pub(crate) message_id: String,
    pub(crate) workspace_id: String,
    pub(crate) parent_thread_id: String,
    pub(crate) child_thread_id: String,
    pub(crate) source_run_id: String,
    pub(crate) source_turn_id: String,
    pub(crate) kind: ParentAnswerKind,
    pub(crate) payload: String,
    pub(crate) created_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PersistChildReturnResult {
    Stored(ChildReturnRecord),
    Replayed(ChildReturnRecord),
    Rejected { code: String, reason: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PersistParentAnswerResult {
    Stored(ParentAnswerRecord),
    Replayed(ParentAnswerRecord),
    Rejected { code: String, reason: String },
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProfileHomeProposal {
    pub(crate) repositories: Vec<ThreadRepositoryRequest>,
    pub(crate) working_repository: String,
    pub(crate) working_subdir: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HomeReservationState {
    Pending,
    Granted,
    Denied,
    Canceled,
}

impl HomeReservationState {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Granted => "granted",
            Self::Denied => "denied",
            Self::Canceled => "canceled",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "granted" => Some(Self::Granted),
            "denied" => Some(Self::Denied),
            "canceled" => Some(Self::Canceled),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HomeReservationRecord {
    pub(crate) id: String,
    pub(crate) workspace_id: String,
    pub(crate) profile_id: ProfileId,
    pub(crate) idempotency_key: String,
    pub(crate) proposal: ProfileHomeProposal,
    pub(crate) draft: crate::thread_authority::ThreadAuthorityDraft,
    pub(crate) state: HomeReservationState,
    pub(crate) decided_by: Option<String>,
    pub(crate) reason: Option<String>,
    pub(crate) created_at_ms: u64,
    pub(crate) decided_at_ms: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ReserveProfileHomeResult {
    Reserved(HomeReservationRecord),
    Replayed(HomeReservationRecord),
    Conflict(String),
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

pub(crate) fn mint_profile_id(
    workspace_id: &str,
    display_name: &str,
    created_at_ms: u64,
) -> ProfileId {
    let mut hasher = Sha256::new();
    hasher.update(workspace_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(display_name.as_bytes());
    hasher.update(b"\0");
    hasher.update(created_at_ms.to_be_bytes());
    hasher.update(std::process::id().to_be_bytes());
    hasher.update(
        PROFILE_ID_SEQUENCE
            .fetch_add(1, Ordering::Relaxed)
            .to_be_bytes(),
    );
    let digest = hasher.finalize();
    let hex: String = digest
        .iter()
        .take(12)
        .map(|byte| format!("{byte:02x}"))
        .collect();
    ProfileId::new(format!("profile-{hex}")).expect("minted profile ids are non-empty")
}

impl ProfileRevisionContent {
    pub(crate) fn empty() -> Self {
        Self {
            instructions_markdown: String::new(),
            memory_markdown: String::new(),
            skill_refs: Vec::new(),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), ProfileValidationError> {
        if self.instructions_markdown.len() > MAX_PROFILE_MARKDOWN_BYTES {
            return Err(ProfileValidationError::InstructionsTooLarge);
        }
        if self.memory_markdown.len() > MAX_PROFILE_MARKDOWN_BYTES {
            return Err(ProfileValidationError::MemoryTooLarge);
        }
        if self.skill_refs.len() > MAX_PROFILE_SKILL_REFS {
            return Err(ProfileValidationError::TooManySkillRefs);
        }
        if self.serialized()?.len() > MAX_PROFILE_REVISION_BYTES {
            return Err(ProfileValidationError::RevisionTooLarge);
        }
        Ok(())
    }

    pub(crate) fn content_hash(&self) -> Result<String, ProfileValidationError> {
        let digest = Sha256::digest(self.serialized()?);
        Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
    }

    fn serialized(&self) -> Result<Vec<u8>, ProfileValidationError> {
        serde_json::to_vec(self)
            .map_err(|error| ProfileValidationError::Serialization(error.to_string()))
    }
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
