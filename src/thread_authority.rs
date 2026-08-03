use crate::{AppError, AppResult};
use plato_protocol::{
    ReasoningEffort, ThreadApprovalPolicy, ThreadAuthorityRecord, ThreadSpawnDecision,
};
use platonic_core::{ActorId, EffectClass, ModelName};
use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

pub(crate) const THREAD_SPAWN_APPROVAL_REASON: &str =
    "thread.spawn requires approval before authority is created";

static THREAD_ID_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ThreadAuthorityDraft {
    pub(crate) thread_id: String,
    pub(crate) parent_thread_id: Option<String>,
    pub(crate) cwd: String,
    pub(crate) model: String,
    pub(crate) reasoning_effort: ReasoningEffort,
    pub(crate) approval_policy: ThreadApprovalPolicy,
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
}

impl ThreadAuthorityDraft {
    pub(crate) fn new(
        parent_thread_id: Option<String>,
        cwd: &Path,
        model: String,
        reasoning_effort: ReasoningEffort,
        approval_policy: ThreadApprovalPolicy,
    ) -> AppResult<Self> {
        ModelName::new(model.clone())?;
        let cwd = canonical_directory(cwd)?;
        Ok(Self {
            thread_id: generated_id("thread"),
            parent_thread_id,
            cwd: cwd.to_string_lossy().into_owned(),
            model,
            reasoning_effort,
            approval_policy,
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
            cwd: self.cwd.clone(),
            model: self.model.clone(),
            reasoning_effort: self.reasoning_effort,
            approval_policy: self.approval_policy,
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
    let parent_cwd = Path::new(&parent.cwd);
    let child_cwd = Path::new(&child.cwd);
    if !child_cwd.starts_with(parent_cwd) {
        return Err(ThreadAuthorityError::WorkingDirectory {
            parent: parent.cwd.clone(),
            child: child.cwd.clone(),
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
    ModelName::new(authority.model.clone())?;
    let canonical_cwd = canonical_directory(Path::new(&authority.cwd))?;
    if canonical_cwd.to_string_lossy() != authority.cwd {
        return Err(AppError::Config(
            "thread authority cwd must be canonical".into(),
        ));
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn authority(cwd: &Path, policy: ThreadApprovalPolicy) -> ThreadAuthorityRecord {
        ThreadAuthorityRecord {
            thread_id: "thread_parent".into(),
            parent_thread_id: None,
            spawning_actor: "stdin".into(),
            cwd: canonical_directory(cwd)
                .unwrap()
                .to_string_lossy()
                .into_owned(),
            model: "gpt-parent".into(),
            reasoning_effort: ReasoningEffort::High,
            approval_policy: policy,
            created_at_ms: 1,
        }
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
            let draft = ThreadAuthorityDraft::new(
                Some("thread_parent".into()),
                &child_dir,
                "gpt-child".into(),
                ReasoningEffort::Xhigh,
                child_policy,
            )
            .unwrap();
            assert_eq!(
                validate_child_authority(&authority(root.path(), parent_policy), &draft).is_ok(),
                allowed
            );
        }

        let outside_draft = ThreadAuthorityDraft::new(
            Some("thread_parent".into()),
            outside.path(),
            "gpt-child".into(),
            ReasoningEffort::Xhigh,
            ThreadApprovalPolicy::Prompt,
        )
        .unwrap();
        assert!(matches!(
            validate_child_authority(
                &authority(root.path(), ThreadApprovalPolicy::Yolo),
                &outside_draft
            ),
            Err(ThreadAuthorityError::WorkingDirectory { .. })
        ));
    }
}
