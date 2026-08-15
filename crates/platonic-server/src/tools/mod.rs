mod approval;
pub(crate) mod computer;
mod files;
pub mod github;
mod logical;
mod memory;
mod shell;
pub mod web;

pub use approval::{
    ApprovalOutcome, approval_command_preview, approval_diff_preview, approval_input_preview,
    ask_for_approval,
};
pub(crate) use logical::{
    LogicalReadErrorCode, LogicalReadRequest, LogicalReadResult, LogicalReadToolHandler,
    LogicalReadToolOutput, MAX_LOGICAL_READ_SERIALIZED_BYTES, ProfileContentView,
    ProfileEventEntry, ProfileFilesystemIsolation, ProfileReadInput, ProfileReadResult,
    ProfileRevisionMetadata, ProfileRevisionView, ProfileThreadMetadata, ProfileTranscriptEntry,
    ThreadEventsResult, ThreadHistoryInput, ThreadTranscriptResult, ThreadTreeInput,
    ThreadTreeResult,
};
pub(crate) use memory::{
    PLATONIC_MEMORY_FILENAME, PLATONIC_MEMORY_MAX_BYTES, targets_platonic_memory,
};
pub(crate) use shell::supervised_run_child_env;

use computer::ComputerToolHandler;
use files::{list_directory, read_file, write_file};
use shell::shell_exec;

use crate::tool_catalog::{
    COMPUTER_OBSERVE, COMPUTER_WINDOWS, FILE_EDIT, FILE_LIST, FILE_READ, FILE_WRITE, SHELL_EXEC,
    THREAD_ANSWER, THREAD_RETURN, THREAD_SPAWN, WEB_FETCH, is_logical_read_tool,
};
use crate::{AppError, AppResult};
use platonic_core::{ResultVisibility, ToolResult};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    path::Path,
    sync::{Arc, atomic::AtomicBool},
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ThreadSpawnToolInput {
    pub(crate) cwd: String,
    pub(crate) model: Option<String>,
    pub(crate) reasoning_effort: Option<platonic_protocol::ReasoningEffort>,
    pub(crate) approval_policy: Option<platonic_protocol::ThreadApprovalPolicy>,
    pub(crate) toolset: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) repositories: Option<Vec<platonic_protocol::ThreadRepositoryRequest>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ThreadReturnToolKind {
    Progress,
    Question,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ThreadReturnToolInput {
    pub(crate) kind: ThreadReturnToolKind,
    pub(crate) payload: String,
    #[serde(default)]
    pub(crate) artifact_refs: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub(crate) enum ThreadReturnToolOutput {
    Delivered { message_id: String, replayed: bool },
    Rejected { code: String, reason: String },
}

impl ThreadReturnToolOutput {
    pub(crate) fn is_rejected(&self) -> bool {
        matches!(self, Self::Rejected { .. })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ParentAnswerToolKind {
    Answer,
    FollowUp,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ParentAnswerToolInput {
    pub(crate) child_thread_id: String,
    pub(crate) kind: ParentAnswerToolKind,
    pub(crate) payload: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub(crate) enum ParentAnswerToolOutput {
    Delivered { message_id: String, replayed: bool },
    Rejected { code: String, reason: String },
}

impl ParentAnswerToolOutput {
    pub(crate) fn is_rejected(&self) -> bool {
        matches!(self, Self::Rejected { .. })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub(crate) enum ThreadSpawnToolOutput {
    Spawned { thread_id: String },
    Rejected { code: String, reason: String },
}

impl ThreadSpawnToolOutput {
    pub(crate) fn is_rejected(&self) -> bool {
        matches!(self, Self::Rejected { .. })
    }
}

#[derive(Clone)]
pub(crate) struct ThreadSpawnToolHandler {
    execute:
        Arc<dyn Fn(ThreadSpawnToolInput, String) -> AppResult<ThreadSpawnToolOutput> + Send + Sync>,
}

impl std::fmt::Debug for ThreadSpawnToolHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ThreadSpawnToolHandler")
            .finish_non_exhaustive()
    }
}

impl ThreadSpawnToolHandler {
    pub(crate) fn new(
        execute: impl Fn(ThreadSpawnToolInput, String) -> AppResult<ThreadSpawnToolOutput>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            execute: Arc::new(execute),
        }
    }

    pub(crate) fn execute(
        &self,
        input: ThreadSpawnToolInput,
        approving_actor: String,
    ) -> AppResult<ThreadSpawnToolOutput> {
        (self.execute)(input, approving_actor)
    }
}

#[derive(Clone)]
pub(crate) struct ThreadReturnToolHandler {
    execute: Arc<
        dyn Fn(
                ThreadReturnToolInput,
                platonic_core::ToolCallId,
            ) -> AppResult<ThreadReturnToolOutput>
            + Send
            + Sync,
    >,
}

impl std::fmt::Debug for ThreadReturnToolHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ThreadReturnToolHandler")
            .finish_non_exhaustive()
    }
}

impl ThreadReturnToolHandler {
    pub(crate) fn new(
        execute: impl Fn(
            ThreadReturnToolInput,
            platonic_core::ToolCallId,
        ) -> AppResult<ThreadReturnToolOutput>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            execute: Arc::new(execute),
        }
    }

    pub(crate) fn execute(
        &self,
        input: ThreadReturnToolInput,
        call_id: platonic_core::ToolCallId,
    ) -> AppResult<ThreadReturnToolOutput> {
        (self.execute)(input, call_id)
    }
}

#[derive(Clone)]
pub(crate) struct ParentAnswerToolHandler {
    execute: Arc<
        dyn Fn(
                ParentAnswerToolInput,
                platonic_core::ToolCallId,
            ) -> AppResult<ParentAnswerToolOutput>
            + Send
            + Sync,
    >,
}

impl std::fmt::Debug for ParentAnswerToolHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ParentAnswerToolHandler")
            .finish_non_exhaustive()
    }
}

impl ParentAnswerToolHandler {
    pub(crate) fn new(
        execute: impl Fn(
            ParentAnswerToolInput,
            platonic_core::ToolCallId,
        ) -> AppResult<ParentAnswerToolOutput>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            execute: Arc::new(execute),
        }
    }

    pub(crate) fn execute(
        &self,
        input: ParentAnswerToolInput,
        call_id: platonic_core::ToolCallId,
    ) -> AppResult<ParentAnswerToolOutput> {
        (self.execute)(input, call_id)
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RunToolHandlers {
    pub(crate) thread_spawn: Option<ThreadSpawnToolHandler>,
    pub(crate) logical_read: Option<LogicalReadToolHandler>,
    pub(crate) thread_return: Option<ThreadReturnToolHandler>,
    pub(crate) parent_answer: Option<ParentAnswerToolHandler>,
}

#[derive(Debug)]
pub struct ToolExecutionContext<'a> {
    pub workspace_root: &'a Path,
    pub provider_api_key_env: Option<&'a str>,
    pub cancel: Option<&'a AtomicBool>,
    pub(crate) thread_spawn: Option<&'a ThreadSpawnToolHandler>,
    pub(crate) logical_read: Option<&'a LogicalReadToolHandler>,
    pub(crate) thread_return: Option<&'a ThreadReturnToolHandler>,
    pub(crate) parent_answer: Option<&'a ParentAnswerToolHandler>,
    pub(crate) computer: Option<&'a mut ComputerToolHandler>,
    pub(crate) approving_actor: Option<&'a str>,
}

impl<'a> ToolExecutionContext<'a> {
    pub fn new(workspace_root: &'a Path) -> Self {
        Self {
            workspace_root,
            provider_api_key_env: None,
            cancel: None,
            thread_spawn: None,
            logical_read: None,
            thread_return: None,
            parent_answer: None,
            computer: None,
            approving_actor: None,
        }
    }
}

pub fn execute_tool(
    workspace_root: &Path,
    call_id: platonic_core::ToolCallId,
    tool_name: &str,
    input: Value,
) -> AppResult<ToolResult> {
    execute_tool_with_context(
        ToolExecutionContext::new(workspace_root),
        call_id,
        tool_name,
        input,
    )
}

pub fn execute_tool_with_context(
    context: ToolExecutionContext<'_>,
    call_id: platonic_core::ToolCallId,
    tool_name: &str,
    input: Value,
) -> AppResult<ToolResult> {
    match tool_name {
        FILE_READ => read_file(context.workspace_root, call_id, input),
        FILE_LIST => list_directory(context.workspace_root, call_id, input),
        FILE_WRITE => write_file(context.workspace_root, call_id, input, "wrote", "to"),
        FILE_EDIT => write_file(context.workspace_root, call_id, input, "edited", "at"),
        SHELL_EXEC => shell_exec(context, call_id, input),
        WEB_FETCH => web::fetch(call_id, input, context.cancel),
        _ if is_logical_read_tool(tool_name) => {
            logical::execute(context.logical_read, call_id, tool_name, input)
        }
        THREAD_SPAWN => spawn_thread(context, call_id, input),
        THREAD_RETURN => return_to_parent(context, call_id, input),
        THREAD_ANSWER => answer_child(context, call_id, input),
        COMPUTER_WINDOWS | COMPUTER_OBSERVE => context
            .computer
            .ok_or_else(|| AppError::Tool("computer_disabled".into()))?
            .execute(call_id, tool_name, input),
        _ => Err(AppError::Tool(format!("unknown tool: {tool_name}"))),
    }
}

fn return_to_parent(
    context: ToolExecutionContext<'_>,
    call_id: platonic_core::ToolCallId,
    input: Value,
) -> AppResult<ToolResult> {
    let input: ThreadReturnToolInput = serde_json::from_value(input)?;
    let output = context
        .thread_return
        .ok_or_else(|| AppError::Tool("thread.return requires a child thread".into()))?
        .execute(input, call_id.clone())?;
    let rejected = output.is_rejected();
    Ok(ToolResult {
        call_id,
        summary: if rejected {
            "child return rejected".into()
        } else {
            "returned child data to immediate parent".into()
        },
        data: serde_json::to_value(output)?,
        artifacts: vec![],
        visibility: ResultVisibility::Both,
    })
}

fn answer_child(
    context: ToolExecutionContext<'_>,
    call_id: platonic_core::ToolCallId,
    input: Value,
) -> AppResult<ToolResult> {
    let input: ParentAnswerToolInput = serde_json::from_value(input)?;
    let output = context
        .parent_answer
        .ok_or_else(|| AppError::Tool("thread.answer requires a profile thread".into()))?
        .execute(input, call_id.clone())?;
    let rejected = output.is_rejected();
    Ok(ToolResult {
        call_id,
        summary: if rejected {
            "parent answer rejected".into()
        } else {
            "sent attributed data to immediate child".into()
        },
        data: serde_json::to_value(output)?,
        artifacts: vec![],
        visibility: ResultVisibility::Both,
    })
}

fn spawn_thread(
    context: ToolExecutionContext<'_>,
    call_id: platonic_core::ToolCallId,
    input: Value,
) -> AppResult<ToolResult> {
    let input: ThreadSpawnToolInput = serde_json::from_value(input)?;
    let handler = context
        .thread_spawn
        .ok_or_else(|| AppError::Tool("thread.spawn requires a coordinator thread".into()))?;
    let actor = context
        .approving_actor
        .ok_or_else(|| AppError::Tool("thread.spawn requires an approving actor".into()))?;
    let output = handler.execute(input, actor.into())?;
    let rejected = output.is_rejected();
    Ok(ToolResult {
        call_id,
        summary: if rejected {
            "thread spawn rejected".into()
        } else {
            "spawned worker thread".into()
        },
        data: serde_json::to_value(output)?,
        artifacts: vec![],
        visibility: ResultVisibility::Both,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use platonic_core::ToolCallId;
    use serde_json::json;
    use std::sync::Mutex;
    #[test]
    fn thread_spawn_executes_only_with_host_handler_and_approving_actor() {
        let root = tempfile::tempdir().unwrap();
        let observed = Arc::new(Mutex::new(None));
        let captured = Arc::clone(&observed);
        let handler = ThreadSpawnToolHandler::new(move |input, actor| {
            *captured.lock().unwrap() = Some((input, actor));
            Ok(ThreadSpawnToolOutput::Spawned {
                thread_id: "thread_worker".into(),
            })
        });
        let result = execute_tool_with_context(
            ToolExecutionContext {
                workspace_root: root.path(),
                provider_api_key_env: None,
                cancel: None,
                thread_spawn: Some(&handler),
                logical_read: None,
                thread_return: None,
                parent_answer: None,
                computer: None,
                approving_actor: Some("reviewer"),
            },
            ToolCallId::new("call_spawn").unwrap(),
            THREAD_SPAWN,
            json!({"cwd": root.path()}),
        )
        .unwrap();

        assert_eq!(
            result.data,
            json!({"status": "spawned", "thread_id": "thread_worker"})
        );
        let (input, actor) = observed.lock().unwrap().clone().unwrap();
        assert_eq!(input.cwd, root.path().to_string_lossy());
        assert_eq!(actor, "reviewer");

        let error = execute_tool_with_context(
            ToolExecutionContext {
                workspace_root: root.path(),
                provider_api_key_env: None,
                cancel: None,
                thread_spawn: Some(&handler),
                logical_read: None,
                thread_return: None,
                parent_answer: None,
                computer: None,
                approving_actor: None,
            },
            ToolCallId::new("call_without_actor").unwrap(),
            THREAD_SPAWN,
            json!({"cwd": root.path()}),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            AppError::Tool(message) if message.contains("approving actor")
        ));
    }
}
