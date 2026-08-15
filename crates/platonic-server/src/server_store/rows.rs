use super::types::{
    AgentRecord, ChildReturnKind, ChildReturnRecord, DeliveryState, ParentAnswerKind,
    ParentAnswerRecord, ProfileRecord, ProfileRevisionContent, ProfileRevisionRecord,
    ThreadRunAdmission, ToolCallApprovalDecision, ToolCallApprovalRecord, WorkspaceRecord,
};
use crate::{
    AppError, AppResult,
    ledger::row_u64,
    thread_authority::{LegacyReason, ThreadProfileAuthority},
};
use platonic_core::{AgentId, EffectClass, ProfileId};
use platonic_protocol::{
    ReasoningEffort, ThreadApprovalPolicy, ThreadAuthorityRecord, ThreadGrantedPath, ThreadKind,
    ThreadWorktree,
};
use rusqlite::types::Type;

pub(super) fn thread_authority_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ThreadAuthorityRecord> {
    let legacy_cwd: Option<String> = row.get(3)?;
    let agent_value: Option<String> = row.get(4)?;
    let agent_id = agent_value
        .map(AgentId::new)
        .transpose()
        .map_err(|error| invalid_thread_column(4, error.to_string()))?;
    let reasoning_value: String = row.get(6)?;
    let reasoning_effort = ReasoningEffort::parse(&reasoning_value).ok_or_else(|| {
        invalid_thread_column(6, format!("unknown reasoning effort: {reasoning_value}"))
    })?;
    let policy_value: String = row.get(7)?;
    let approval_policy = ThreadApprovalPolicy::parse(&policy_value).ok_or_else(|| {
        invalid_thread_column(7, format!("unknown approval policy: {policy_value}"))
    })?;
    let toolset = row
        .get::<_, Option<String>>(8)?
        .map(|value| {
            serde_json::from_str::<Vec<String>>(&value)
                .map_err(|error| invalid_thread_column(8, error.to_string()))
        })
        .transpose()?
        .unwrap_or_default();
    let worktrees = row
        .get::<_, Option<String>>(9)?
        .map(|value| {
            serde_json::from_str::<Vec<ThreadWorktree>>(&value)
                .map_err(|error| invalid_thread_column(9, error.to_string()))
        })
        .transpose()?
        .unwrap_or_default();
    let granted_paths = row
        .get::<_, Option<String>>(10)?
        .map(|value| {
            serde_json::from_str::<Vec<ThreadGrantedPath>>(&value)
                .map_err(|error| invalid_thread_column(10, error.to_string()))
        })
        .transpose()?
        .unwrap_or_else(|| {
            legacy_cwd
                .clone()
                .map(|path| {
                    vec![ThreadGrantedPath {
                        path,
                        writable: true,
                    }]
                })
                .unwrap_or_default()
        });
    let network = match row.get::<_, Option<i64>>(11)? {
        None | Some(0) => false,
        Some(1) => true,
        Some(value) => {
            return Err(invalid_thread_column(
                11,
                format!("invalid network flag: {value}"),
            ));
        }
    };
    let profile_id = row
        .get::<_, Option<String>>(13)?
        .map(ProfileId::new)
        .transpose()
        .map_err(|error| invalid_thread_column(13, error.to_string()))?;
    let profile_revision = row
        .get::<_, Option<i64>>(14)?
        .map(|value| {
            value.try_into().map_err(|_| {
                invalid_thread_column(14, format!("negative profile revision: {value}"))
            })
        })
        .transpose()?;
    let kind_value: String = row.get(15)?;
    let thread_kind = ThreadKind::parse(&kind_value)
        .ok_or_else(|| invalid_thread_column(15, format!("unknown thread kind: {kind_value}")))?;
    Ok(ThreadAuthorityRecord {
        thread_id: row.get(0)?,
        parent_thread_id: row.get(1)?,
        spawning_actor: row.get(2)?,
        cwd: legacy_cwd,
        agent_id,
        profile_id,
        profile_revision,
        thread_kind,
        model: row.get(5)?,
        reasoning_effort,
        approval_policy,
        toolset,
        worktrees,
        granted_paths,
        network,
        created_at_ms: row_u64(row, 12, "thread created_at_ms")?,
    })
}

pub(super) fn thread_profile_authority_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ThreadProfileAuthority> {
    let profile_id = row
        .get::<_, Option<String>>(1)?
        .map(ProfileId::new)
        .transpose()
        .map_err(|error| invalid_thread_column(1, error.to_string()))?;
    let profile_revision = row
        .get::<_, Option<i64>>(2)?
        .map(|value| {
            value.try_into().map_err(|_| {
                invalid_thread_column(2, format!("negative profile revision: {value}"))
            })
        })
        .transpose()?;
    let kind_value: String = row.get(3)?;
    let thread_kind = ThreadKind::parse(&kind_value)
        .ok_or_else(|| invalid_thread_column(3, format!("unknown thread kind: {kind_value}")))?;
    let reason_value: Option<String> = row.get(4)?;
    let legacy_reason = reason_value
        .map(|value| {
            LegacyReason::parse(&value)
                .ok_or_else(|| invalid_thread_column(4, format!("unknown legacy reason: {value}")))
        })
        .transpose()?;
    Ok(ThreadProfileAuthority {
        workspace_id: row.get(0)?,
        profile_id,
        profile_revision,
        thread_kind,
        legacy_reason,
    })
}

pub(super) fn invalid_thread_column(index: usize, message: String) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        index,
        Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            message,
        )),
    )
}

pub(super) fn child_return_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ChildReturnRecord> {
    let profile_id = ProfileId::new(row.get::<_, String>(3)?)
        .map_err(|error| invalid_thread_column(3, error.to_string()))?;
    let kind_text: String = row.get(8)?;
    let kind = ChildReturnKind::parse(&kind_text).ok_or_else(|| {
        invalid_thread_column(8, format!("unknown child return kind: {kind_text}"))
    })?;
    let artifact_refs = serde_json::from_str::<Vec<String>>(&row.get::<_, String>(10)?)
        .map_err(|error| invalid_thread_column(10, error.to_string()))?;
    let truncated = sqlite_bool(row, 11, "child return truncated")?;
    let state_text: String = row.get(14)?;
    let state = DeliveryState::parse(&state_text).ok_or_else(|| {
        invalid_thread_column(14, format!("unknown child return state: {state_text}"))
    })?;
    Ok(ChildReturnRecord {
        sequence: row_u64(row, 0, "child return sequence")?,
        message_id: row.get(1)?,
        spawn_id: row.get(2)?,
        profile_id,
        parent_thread_id: row.get(4)?,
        child_thread_id: row.get(5)?,
        source_run_id: row.get(6)?,
        source_turn_id: row.get(7)?,
        kind,
        payload: row.get(9)?,
        artifact_refs,
        truncated,
        created_at_ms: row_u64(row, 12, "child return created_at_ms")?,
        profile_revision: row_u64(row, 13, "child return profile_revision")?,
        state,
        reserved_by_run_id: row.get(15)?,
        reserved_by_turn_id: row.get(16)?,
        consumed_by_run_id: row.get(17)?,
        consumed_by_turn_id: row.get(18)?,
        consumed_at_ms: row
            .get::<_, Option<i64>>(19)?
            .map(|value| value.max(0) as u64),
    })
}

pub(super) fn parent_answer_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ParentAnswerRecord> {
    let profile_id = ProfileId::new(row.get::<_, String>(3)?)
        .map_err(|error| invalid_thread_column(3, error.to_string()))?;
    let kind_text: String = row.get(8)?;
    let kind = ParentAnswerKind::parse(&kind_text).ok_or_else(|| {
        invalid_thread_column(8, format!("unknown parent answer kind: {kind_text}"))
    })?;
    let state_text: String = row.get(12)?;
    let state = DeliveryState::parse(&state_text).ok_or_else(|| {
        invalid_thread_column(12, format!("unknown parent answer state: {state_text}"))
    })?;
    Ok(ParentAnswerRecord {
        sequence: row_u64(row, 0, "parent answer sequence")?,
        message_id: row.get(1)?,
        spawn_id: row.get(2)?,
        profile_id,
        parent_thread_id: row.get(4)?,
        child_thread_id: row.get(5)?,
        source_run_id: row.get(6)?,
        source_turn_id: row.get(7)?,
        kind,
        payload: row.get(9)?,
        created_at_ms: row_u64(row, 10, "parent answer created_at_ms")?,
        profile_revision: row_u64(row, 11, "parent answer profile_revision")?,
        state,
        reserved_by_run_id: row.get(13)?,
        reserved_by_turn_id: row.get(14)?,
        consumed_by_run_id: row.get(15)?,
        consumed_by_turn_id: row.get(16)?,
        consumed_at_ms: row
            .get::<_, Option<i64>>(17)?
            .map(|value| value.max(0) as u64),
    })
}

pub(super) fn thread_run_admission_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ThreadRunAdmission> {
    let profile_id = ProfileId::new(row.get::<_, String>(2)?)
        .map_err(|error| invalid_thread_column(2, error.to_string()))?;
    Ok(ThreadRunAdmission {
        run_id: row.get(0)?,
        workspace_id: row.get(1)?,
        profile_id,
        thread_id: row.get(3)?,
        thread_turn_id: row.get(4)?,
        profile_revision: row_u64(row, 5, "thread run profile_revision")?,
        created_at_ms: row_u64(row, 6, "thread run created_at_ms")?,
    })
}

fn sqlite_bool(row: &rusqlite::Row<'_>, index: usize, name: &str) -> rusqlite::Result<bool> {
    match row.get::<_, i64>(index)? {
        0 => Ok(false),
        1 => Ok(true),
        value => Err(invalid_thread_column(
            index,
            format!("invalid {name} flag: {value}"),
        )),
    }
}

pub(super) fn effect_to_text(effect: &EffectClass) -> AppResult<String> {
    match serde_json::to_value(effect) {
        Ok(serde_json::Value::String(text)) => Ok(text),
        _ => Err(AppError::Config("effect class is not a string".into())),
    }
}

fn effect_from_text(text: &str) -> AppResult<EffectClass> {
    serde_json::from_value(serde_json::Value::String(text.to_owned()))
        .map_err(|_| AppError::Config(format!("unknown effect class: {text}")))
}

/// The outer Result is the row read; the inner one is the effect class, which
/// can only be validated after the text leaves SQLite.
pub(super) fn tool_call_approval_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<AppResult<ToolCallApprovalRecord>> {
    let effect_text: String = row.get(4)?;
    let decision: Option<String> = row.get(10)?;
    let decided_by: Option<String> = row.get(11)?;
    let decision_reason: Option<String> = row.get(12)?;
    let decided_at_ms: Option<i64> = row.get(13)?;
    let record = ToolCallApprovalRecord {
        run_id: row.get(0)?,
        call_id: row.get(1)?,
        session_id: row.get(2)?,
        tool_name: row.get(3)?,
        effect: EffectClass::ReadOnly,
        reason: row.get(5)?,
        input_preview: row.get(6)?,
        approval_preview: row.get(7)?,
        diff_preview: row.get(8)?,
        requested_at_ms: row_u64(row, 9, "approval requested_at_ms")?,
        decision: match (decision, decided_by, decided_at_ms) {
            (Some(decision), Some(actor), Some(at_ms)) => Some(ToolCallApprovalDecision {
                granted: decision == "granted",
                actor,
                reason: decision_reason,
                decided_at_ms: at_ms.max(0) as u64,
            }),
            _ => None,
        },
    };
    Ok(effect_from_text(&effect_text).map(|effect| ToolCallApprovalRecord { effect, ..record }))
}

pub(super) fn workspace_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<WorkspaceRecord> {
    Ok(WorkspaceRecord {
        id: row.get(0)?,
        name: row.get(1)?,
        root: row.get(2)?,
        ledger_path: row.get(3)?,
        created_at_ms: row_u64(row, 4, "workspace created_at_ms")?,
    })
}

pub(super) fn agent_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<AgentRecord> {
    let id = AgentId::new(row.get::<_, String>(0)?)
        .map_err(|error| invalid_agent_column(0, error.to_string()))?;
    let reasoning_value: String = row.get(3)?;
    let reasoning_effort = ReasoningEffort::parse(&reasoning_value).ok_or_else(|| {
        invalid_agent_column(3, format!("unknown reasoning effort: {reasoning_value}"))
    })?;
    let policy_value: String = row.get(4)?;
    let approval_policy = ThreadApprovalPolicy::parse(&policy_value).ok_or_else(|| {
        invalid_agent_column(4, format!("unknown approval policy: {policy_value}"))
    })?;
    let toolset = serde_json::from_str::<Vec<String>>(&row.get::<_, String>(5)?)
        .map_err(|error| invalid_agent_column(5, error.to_string()))?;
    Ok(AgentRecord {
        id,
        workspace_id: row.get(1)?,
        model: row.get(2)?,
        reasoning_effort,
        approval_policy,
        toolset,
        created_at_ms: row_u64(row, 6, "agent created_at_ms")?,
    })
}

pub(super) fn profile_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ProfileRecord> {
    let id = ProfileId::new(row.get::<_, String>(0)?)
        .map_err(|error| invalid_profile_column(0, error.to_string()))?;
    let reasoning_value: String = row.get(4)?;
    let reasoning_effort = ReasoningEffort::parse(&reasoning_value).ok_or_else(|| {
        invalid_profile_column(4, format!("unknown reasoning effort: {reasoning_value}"))
    })?;
    let policy_value: String = row.get(5)?;
    let approval_policy = ThreadApprovalPolicy::parse(&policy_value).ok_or_else(|| {
        invalid_profile_column(5, format!("unknown approval policy: {policy_value}"))
    })?;
    let toolset = serde_json::from_str::<Vec<String>>(&row.get::<_, String>(6)?)
        .map_err(|error| invalid_profile_column(6, error.to_string()))?;
    Ok(ProfileRecord {
        id,
        workspace_id: row.get(1)?,
        display_name: row.get(2)?,
        model: row.get(3)?,
        reasoning_effort,
        approval_policy,
        toolset,
        current_revision: row_u64(row, 7, "profile current_revision")?,
        home_thread_id: row.get(8)?,
        imported_agent_id: row.get(9)?,
        created_at_ms: row_u64(row, 10, "profile created_at_ms")?,
    })
}

pub(super) fn profile_revision_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ProfileRevisionRecord> {
    let profile_id = ProfileId::new(row.get::<_, String>(0)?)
        .map_err(|error| invalid_profile_column(0, error.to_string()))?;
    let content = ProfileRevisionContent {
        instructions_markdown: row.get(6)?,
        memory_markdown: row.get(7)?,
        skill_refs: serde_json::from_str(&row.get::<_, String>(8)?)
            .map_err(|error| invalid_profile_column(8, error.to_string()))?,
    };
    content
        .validate()
        .map_err(|error| invalid_profile_column(8, error.to_string()))?;
    let content_hash: String = row.get(5)?;
    if content
        .content_hash()
        .map_err(|error| invalid_profile_column(5, error.to_string()))?
        != content_hash
    {
        return Err(invalid_profile_column(
            5,
            "profile revision content hash mismatch".into(),
        ));
    }
    Ok(ProfileRevisionRecord {
        profile_id,
        revision: row_u64(row, 1, "profile revision")?,
        parent_revision: row
            .get::<_, Option<i64>>(2)?
            .map(|value| {
                value.try_into().map_err(|_| {
                    invalid_profile_column(2, format!("negative parent revision: {value}"))
                })
            })
            .transpose()?,
        actor: row.get(3)?,
        created_at_ms: row_u64(row, 4, "profile revision created_at_ms")?,
        content_hash,
        content,
    })
}

fn invalid_agent_column(index: usize, message: String) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(index, Type::Text, message.into())
}

fn invalid_profile_column(index: usize, message: String) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(index, Type::Text, message.into())
}
