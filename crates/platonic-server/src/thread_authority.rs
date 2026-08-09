use crate::{AppError, AppResult};
use platonic_core::{ActorId, AgentId, EffectClass, ModelName, ToolName};
use platonic_protocol::{
    ReasoningEffort, ThreadApprovalPolicy, ThreadAuthorityRecord, ThreadGrantedPath,
    ThreadSpawnDecision, ThreadStatusAuthority, ThreadWorktree,
};
use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

pub(crate) const THREAD_SPAWN_APPROVAL_REASON: &str =
    "thread.spawn requires approval before authority is created";

static THREAD_ID_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(crate) struct ThreadAuthorityDraftParams<'a> {
    pub(crate) parent_thread_id: Option<String>,
    pub(crate) cwd: &'a Path,
    pub(crate) model: String,
    pub(crate) reasoning_effort: ReasoningEffort,
    pub(crate) approval_policy: ThreadApprovalPolicy,
    pub(crate) agent_id: AgentId,
    pub(crate) toolset: Vec<String>,
    pub(crate) writable: bool,
    pub(crate) network: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ThreadAuthorityDraft {
    pub(crate) thread_id: String,
    pub(crate) parent_thread_id: Option<String>,
    pub(crate) cwd: String,
    pub(crate) agent_id: AgentId,
    pub(crate) model: String,
    pub(crate) reasoning_effort: ReasoningEffort,
    pub(crate) approval_policy: ThreadApprovalPolicy,
    pub(crate) toolset: Vec<String>,
    pub(crate) worktrees: Vec<ThreadWorktree>,
    pub(crate) granted_paths: Vec<ThreadGrantedPath>,
    pub(crate) network: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ThreadSpawnDecisionName {
    Granted,
    Denied,
    Canceled,
}

impl ThreadSpawnDecisionName {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Granted => "granted",
            Self::Denied => "denied",
            Self::Canceled => "canceled",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "granted" => Some(Self::Granted),
            "denied" => Some(Self::Denied),
            "canceled" => Some(Self::Canceled),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ThreadSpawnApprovalRecord {
    pub(crate) spawn_id: String,
    pub(crate) thread_id: String,
    pub(crate) decision: ThreadSpawnDecisionName,
    pub(crate) actor: String,
    pub(crate) reason: Option<String>,
    pub(crate) occurred_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ThreadStopRecord {
    pub(crate) thread_id: String,
    pub(crate) actor: String,
    pub(crate) stopped_turn_id: Option<String>,
    pub(crate) occurred_at_ms: u64,
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub(crate) enum ThreadAuthorityError {
    #[error("child approval policy {child} exceeds parent policy {parent}")]
    ApprovalPolicy {
        parent: ThreadApprovalPolicy,
        child: ThreadApprovalPolicy,
    },
    #[error("child cwd {child} is outside parent cwd {parent}")]
    WorkingDirectory { parent: String, child: String },
    #[error("child writable path {child} exceeds parent path authority")]
    WritablePath { child: String },
    #[error("child toolset exceeds parent toolset: {excess:?}")]
    Toolset { excess: Vec<String> },
    #[error("child network authority exceeds parent network authority")]
    Network,
    #[error("child spawn depth exceeds server maximum {maximum}")]
    SpawnDepth { maximum: u32 },
}

impl ThreadAuthorityDraft {
    pub(crate) fn new(params: ThreadAuthorityDraftParams<'_>) -> AppResult<Self> {
        let ThreadAuthorityDraftParams {
            parent_thread_id,
            cwd,
            model,
            reasoning_effort,
            approval_policy,
            agent_id,
            toolset,
            writable,
            network,
        } = params;
        ModelName::new(model.clone())?;
        for tool in &toolset {
            ToolName::new(tool.clone())?;
        }
        let cwd = canonical_directory(cwd)?;
        let cwd = cwd.to_string_lossy().into_owned();
        Ok(Self {
            thread_id: generated_id("thread"),
            parent_thread_id,
            cwd: cwd.clone(),
            agent_id,
            model,
            reasoning_effort,
            approval_policy,
            toolset,
            worktrees: Vec::new(),
            granted_paths: vec![ThreadGrantedPath {
                path: cwd,
                writable,
            }],
            network,
        })
    }

    pub(crate) fn complete(
        &self,
        spawning_actor: String,
        created_at_ms: u64,
    ) -> AppResult<ThreadAuthorityRecord> {
        ActorId::new(spawning_actor.clone())?;
        Ok(ThreadAuthorityRecord {
            thread_id: self.thread_id.clone(),
            parent_thread_id: self.parent_thread_id.clone(),
            spawning_actor,
            agent_id: Some(self.agent_id.clone()),
            model: self.model.clone(),
            reasoning_effort: self.reasoning_effort,
            approval_policy: self.approval_policy,
            toolset: self.toolset.clone(),
            worktrees: self.worktrees.clone(),
            granted_paths: self.granted_paths.clone(),
            network: self.network,
            created_at_ms,
        })
    }
}

impl ThreadSpawnApprovalRecord {
    pub(crate) fn from_decision(
        spawn_id: String,
        thread_id: String,
        decision: &ThreadSpawnDecision,
        occurred_at_ms: u64,
    ) -> AppResult<Self> {
        let (decision, actor, reason) = match decision {
            ThreadSpawnDecision::Grant { actor } => {
                (ThreadSpawnDecisionName::Granted, actor.clone(), None)
            }
            ThreadSpawnDecision::Deny { actor, reason } => (
                ThreadSpawnDecisionName::Denied,
                actor.clone(),
                Some(reason.clone()),
            ),
            ThreadSpawnDecision::Cancel { actor } => {
                (ThreadSpawnDecisionName::Canceled, actor.clone(), None)
            }
        };
        ActorId::new(actor.clone())?;
        if reason
            .as_ref()
            .is_some_and(|reason| reason.trim().is_empty())
        {
            return Err(AppError::Config(
                "thread.spawn denial reason cannot be empty".into(),
            ));
        }
        Ok(Self {
            spawn_id,
            thread_id,
            decision,
            actor,
            reason,
            occurred_at_ms,
        })
    }

    pub(crate) fn matches(&self, decision: &ThreadSpawnDecision) -> bool {
        match decision {
            ThreadSpawnDecision::Grant { actor } => {
                self.decision == ThreadSpawnDecisionName::Granted
                    && self.actor == *actor
                    && self.reason.is_none()
            }
            ThreadSpawnDecision::Deny { actor, reason } => {
                self.decision == ThreadSpawnDecisionName::Denied
                    && self.actor == *actor
                    && self.reason.as_deref() == Some(reason)
            }
            ThreadSpawnDecision::Cancel { actor } => {
                self.decision == ThreadSpawnDecisionName::Canceled
                    && self.actor == *actor
                    && self.reason.is_none()
            }
        }
    }
}

impl ThreadStopRecord {
    pub(crate) fn new(
        thread_id: String,
        actor: String,
        stopped_turn_id: Option<String>,
        occurred_at_ms: u64,
    ) -> AppResult<Self> {
        ActorId::new(thread_id.clone())?;
        ActorId::new(actor.clone())?;
        if let Some(turn_id) = stopped_turn_id.as_ref() {
            ActorId::new(turn_id.clone())?;
        }
        Ok(Self {
            thread_id,
            actor,
            stopped_turn_id,
            occurred_at_ms,
        })
    }
}

pub(crate) fn validate_child_authority(
    parent: &ThreadAuthorityRecord,
    child: &ThreadAuthorityDraft,
) -> Result<(), ThreadAuthorityError> {
    if !parent.approval_policy.permits(child.approval_policy) {
        return Err(ThreadAuthorityError::ApprovalPolicy {
            parent: parent.approval_policy,
            child: child.approval_policy,
        });
    }
    let excess = child
        .toolset
        .iter()
        .filter(|tool| !parent.toolset.contains(tool))
        .cloned()
        .collect::<Vec<_>>();
    if !excess.is_empty() {
        return Err(ThreadAuthorityError::Toolset { excess });
    }
    if child.network && !parent.network {
        return Err(ThreadAuthorityError::Network);
    }
    validate_child_path(parent, &child.cwd, !child.worktrees.is_empty())?;
    for worktree in &child.worktrees {
        validate_child_path(parent, &worktree.path, true)?;
    }
    for grant in &child.granted_paths {
        validate_child_path(parent, &grant.path, grant.writable)?;
    }
    Ok(())
}

fn validate_child_path(
    parent: &ThreadAuthorityRecord,
    child_path: &str,
    writable: bool,
) -> Result<(), ThreadAuthorityError> {
    let child = Path::new(child_path);
    let worktree_match = parent
        .worktrees
        .iter()
        .any(|worktree| child.starts_with(Path::new(&worktree.path)));
    let grant_matches = parent
        .granted_paths
        .iter()
        .filter(|grant| child.starts_with(Path::new(&grant.path)))
        .collect::<Vec<_>>();
    if !worktree_match && grant_matches.is_empty() {
        return Err(ThreadAuthorityError::WorkingDirectory {
            parent: authority_working_directory(parent)
                .map(|path| path.to_string_lossy().into_owned())
                .unwrap_or_else(|| "<no granted path>".into()),
            child: child_path.into(),
        });
    }
    if writable && !worktree_match && !grant_matches.iter().any(|grant| grant.writable) {
        return Err(ThreadAuthorityError::WritablePath {
            child: child_path.into(),
        });
    }
    Ok(())
}

pub(crate) fn validate_complete_authority(authority: &ThreadAuthorityRecord) -> AppResult<()> {
    ActorId::new(authority.thread_id.clone())?;
    if let Some(parent_thread_id) = authority.parent_thread_id.as_ref() {
        ActorId::new(parent_thread_id.clone())?;
        if parent_thread_id == &authority.thread_id {
            return Err(AppError::Config(
                "thread authority cannot name itself as parent".into(),
            ));
        }
    }
    ActorId::new(authority.spawning_actor.clone())?;
    if authority.agent_id.is_none() {
        return Err(AppError::Config(
            "new thread authority requires an agent id".into(),
        ));
    }
    ModelName::new(authority.model.clone())?;
    for tool in &authority.toolset {
        ToolName::new(tool.clone())?;
    }
    if authority.worktrees.is_empty() && authority.granted_paths.is_empty() {
        return Err(AppError::Config(
            "thread authority requires a worktree or granted path".into(),
        ));
    }
    for worktree in &authority.worktrees {
        if worktree.repo.trim().is_empty() || worktree.branch.trim().is_empty() {
            return Err(AppError::Config(
                "thread authority worktree repo and branch must not be empty".into(),
            ));
        }
        validate_authority_path(&worktree.path)?;
    }
    for grant in &authority.granted_paths {
        validate_authority_path(&grant.path)?;
    }
    Ok(())
}

pub(crate) fn authority_working_directory(authority: &ThreadAuthorityRecord) -> Option<&Path> {
    authority
        .worktrees
        .first()
        .map(|worktree| Path::new(&worktree.path))
        .or_else(|| {
            authority
                .granted_paths
                .first()
                .map(|grant| Path::new(&grant.path))
        })
}

pub(crate) fn legacy_status_authority(
    authority: &ThreadAuthorityRecord,
) -> AppResult<ThreadStatusAuthority> {
    let cwd = authority_working_directory(authority)
        .ok_or_else(|| AppError::Config("thread authority has no compatibility cwd".into()))?;
    Ok(ThreadStatusAuthority {
        thread_id: authority.thread_id.clone(),
        parent_thread_id: authority.parent_thread_id.clone(),
        spawning_actor: authority.spawning_actor.clone(),
        cwd: cwd.to_string_lossy().into_owned(),
        model: authority.model.clone(),
        reasoning_effort: authority.reasoning_effort,
        approval_policy: authority.approval_policy,
        created_at_ms: authority.created_at_ms,
    })
}

pub(crate) fn thread_spawn_effect() -> EffectClass {
    EffectClass::WorkspaceWrite
}

pub(crate) fn new_spawn_id() -> String {
    generated_id("spawn")
}

pub(crate) fn new_thread_turn_id() -> String {
    generated_id("thread_turn")
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().try_into().unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn generated_id(prefix: &str) -> String {
    let millis = now_ms();
    format!(
        "{prefix}_{millis}_{}_{}",
        std::process::id(),
        THREAD_ID_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    )
}

fn canonical_directory(path: &Path) -> AppResult<PathBuf> {
    let canonical = path.canonicalize()?;
    if !canonical.is_dir() {
        return Err(AppError::Config(format!(
            "thread cwd is not a directory: {}",
            canonical.display()
        )));
    }
    Ok(canonical)
}

fn validate_authority_path(path: &str) -> AppResult<()> {
    let canonical = canonical_directory(Path::new(path))?;
    if canonical.to_string_lossy() != path {
        return Err(AppError::Config(
            "thread authority paths must be canonical".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn authority(cwd: &Path, policy: ThreadApprovalPolicy) -> ThreadAuthorityRecord {
        ThreadAuthorityRecord {
            thread_id: "thread_parent".into(),
            parent_thread_id: None,
            spawning_actor: "stdin".into(),
            agent_id: Some(AgentId::new("plato").unwrap()),
            model: "gpt-parent".into(),
            reasoning_effort: ReasoningEffort::High,
            approval_policy: policy,
            toolset: vec!["file.read".into(), "file.write".into()],
            worktrees: Vec::new(),
            granted_paths: vec![ThreadGrantedPath {
                path: canonical_directory(cwd)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned(),
                writable: true,
            }],
            network: false,
            created_at_ms: 1,
        }
    }

    #[test]
    fn legacy_status_cwd_prefers_worktree_then_granted_path() {
        let root = tempfile::tempdir().unwrap();
        let worktree = root.path().join("worktree");
        std::fs::create_dir(&worktree).unwrap();
        let mut authority = authority(root.path(), ThreadApprovalPolicy::Prompt);
        authority.worktrees.push(ThreadWorktree {
            repo: "repo".into(),
            branch: "branch".into(),
            path: worktree.to_string_lossy().into_owned(),
        });

        assert_eq!(
            legacy_status_authority(&authority).unwrap().cwd,
            worktree.to_string_lossy()
        );
        authority.worktrees.clear();
        assert_eq!(
            legacy_status_authority(&authority).unwrap().cwd,
            root.path().canonicalize().unwrap().to_string_lossy()
        );
        authority.granted_paths.clear();
        assert!(legacy_status_authority(&authority).is_err());
    }

    #[test]
    fn child_authority_never_expands_policy_or_cwd() {
        let root = tempfile::tempdir().unwrap();
        let child_dir = root.path().join("child");
        std::fs::create_dir(&child_dir).unwrap();
        let outside = tempfile::tempdir().unwrap();

        for (parent_policy, child_policy, allowed) in [
            (
                ThreadApprovalPolicy::Prompt,
                ThreadApprovalPolicy::Prompt,
                true,
            ),
            (
                ThreadApprovalPolicy::Prompt,
                ThreadApprovalPolicy::Yolo,
                false,
            ),
            (
                ThreadApprovalPolicy::Yolo,
                ThreadApprovalPolicy::Prompt,
                true,
            ),
            (ThreadApprovalPolicy::Yolo, ThreadApprovalPolicy::Yolo, true),
        ] {
            let draft = ThreadAuthorityDraft::new(ThreadAuthorityDraftParams {
                parent_thread_id: Some("thread_parent".into()),
                cwd: &child_dir,
                model: "gpt-child".into(),
                reasoning_effort: ReasoningEffort::Xhigh,
                approval_policy: child_policy,
                agent_id: AgentId::new("plato").unwrap(),
                toolset: vec!["file.read".into()],
                writable: false,
                network: false,
            })
            .unwrap();
            assert_eq!(
                validate_child_authority(&authority(root.path(), parent_policy), &draft).is_ok(),
                allowed
            );
        }

        let outside_draft = ThreadAuthorityDraft::new(ThreadAuthorityDraftParams {
            parent_thread_id: Some("thread_parent".into()),
            cwd: outside.path(),
            model: "gpt-child".into(),
            reasoning_effort: ReasoningEffort::Xhigh,
            approval_policy: ThreadApprovalPolicy::Prompt,
            agent_id: AgentId::new("plato").unwrap(),
            toolset: vec!["file.read".into()],
            writable: false,
            network: false,
        })
        .unwrap();
        assert!(matches!(
            validate_child_authority(
                &authority(root.path(), ThreadApprovalPolicy::Yolo),
                &outside_draft
            ),
            Err(ThreadAuthorityError::WorkingDirectory { .. })
        ));
    }

    #[test]
    fn child_toolset_must_be_a_subset_of_parent_toolset() {
        let root = tempfile::tempdir().unwrap();
        let parent = authority(root.path(), ThreadApprovalPolicy::Yolo);

        for (toolset, expected) in [
            (Vec::<String>::new(), Ok(())),
            (vec!["file.read".into()], Ok(())),
            (vec!["file.write".into(), "file.read".into()], Ok(())),
            (
                vec!["file.read".into(), "web.fetch".into()],
                Err(ThreadAuthorityError::Toolset {
                    excess: vec!["web.fetch".into()],
                }),
            ),
        ] {
            let child = ThreadAuthorityDraft::new(ThreadAuthorityDraftParams {
                parent_thread_id: Some("thread_parent".into()),
                cwd: root.path(),
                model: "gpt-child".into(),
                reasoning_effort: ReasoningEffort::High,
                approval_policy: ThreadApprovalPolicy::Prompt,
                agent_id: AgentId::new("plato").unwrap(),
                toolset,
                writable: true,
                network: false,
            })
            .unwrap();
            assert_eq!(validate_child_authority(&parent, &child), expected);
        }
    }

    #[test]
    fn child_authority_never_expands_writable_or_network_grants() {
        let root = tempfile::tempdir().unwrap();
        let mut parent = authority(root.path(), ThreadApprovalPolicy::Yolo);
        parent.granted_paths[0].writable = false;
        parent.toolset.push("web.fetch".into());

        let writable = ThreadAuthorityDraft::new(ThreadAuthorityDraftParams {
            parent_thread_id: Some("thread_parent".into()),
            cwd: root.path(),
            model: "gpt-child".into(),
            reasoning_effort: ReasoningEffort::High,
            approval_policy: ThreadApprovalPolicy::Prompt,
            agent_id: AgentId::new("plato").unwrap(),
            toolset: vec!["file.write".into()],
            writable: true,
            network: false,
        })
        .unwrap();
        assert!(matches!(
            validate_child_authority(&parent, &writable),
            Err(ThreadAuthorityError::WritablePath { .. })
        ));

        let network = ThreadAuthorityDraft::new(ThreadAuthorityDraftParams {
            parent_thread_id: Some("thread_parent".into()),
            cwd: root.path(),
            model: "gpt-child".into(),
            reasoning_effort: ReasoningEffort::High,
            approval_policy: ThreadApprovalPolicy::Prompt,
            agent_id: AgentId::new("plato").unwrap(),
            toolset: vec!["web.fetch".into()],
            writable: false,
            network: true,
        })
        .unwrap();
        assert_eq!(
            validate_child_authority(&parent, &network),
            Err(ThreadAuthorityError::Network)
        );
    }
}
