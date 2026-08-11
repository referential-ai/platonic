use super::types::{
    AgentRecord, ToolCallApprovalDecision, ToolCallApprovalRecord, WorkspaceRecord,
};
use crate::{AppError, AppResult, ledger::row_u64};
use platonic_core::{AgentId, EffectClass};
use platonic_protocol::{
    ReasoningEffort, ThreadApprovalPolicy, ThreadAuthorityRecord, ThreadGrantedPath, ThreadWorktree,
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
    Ok(ThreadAuthorityRecord {
        thread_id: row.get(0)?,
        parent_thread_id: row.get(1)?,
        spawning_actor: row.get(2)?,
        agent_id,
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

fn invalid_agent_column(index: usize, message: String) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(index, Type::Text, message.into())
}
