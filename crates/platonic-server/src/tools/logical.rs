use crate::{AppError, AppResult};
use platonic_core::{RecordedEvent, ResultVisibility, ToolCallId, ToolResult};
use platonic_protocol::{RunStateName, ThreadConfinement, ThreadKind, TypedTranscriptEntry};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

pub(crate) const MAX_LOGICAL_READ_SERIALIZED_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProfileReadInput {
    pub(crate) profile_id: Option<String>,
    pub(crate) revision: Option<u64>,
    pub(crate) cursor: Option<String>,
    pub(crate) limit: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ThreadTreeInput {
    pub(crate) profile_id: Option<String>,
    pub(crate) cursor: Option<String>,
    pub(crate) limit: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ThreadHistoryInput {
    pub(crate) thread_id: Option<String>,
    pub(crate) run_id: Option<String>,
    pub(crate) cursor: Option<String>,
    pub(crate) limit: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "read")]
pub(crate) enum LogicalReadRequest {
    Profile(ProfileReadInput),
    ThreadTree(ThreadTreeInput),
    ThreadEvents(ThreadHistoryInput),
    ThreadTranscript(ThreadHistoryInput),
}

impl LogicalReadRequest {
    pub(crate) fn from_tool(tool_name: &str, input: Value) -> AppResult<Self> {
        use crate::tool_catalog::{
            PROFILE_READ, THREAD_EVENTS_READ, THREAD_TRANSCRIPT_READ, THREAD_TREE_READ,
        };

        match tool_name {
            PROFILE_READ => Ok(Self::Profile(serde_json::from_value(input)?)),
            THREAD_TREE_READ => Ok(Self::ThreadTree(serde_json::from_value(input)?)),
            THREAD_EVENTS_READ => Ok(Self::ThreadEvents(serde_json::from_value(input)?)),
            THREAD_TRANSCRIPT_READ => Ok(Self::ThreadTranscript(serde_json::from_value(input)?)),
            _ => Err(AppError::Tool(format!(
                "unknown profile read tool: {tool_name}"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LogicalReadErrorCode {
    InvalidRequest,
    CrossProfile,
    MembershipDenied,
    NotFound,
    ReadFailed,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub(crate) enum LogicalReadToolOutput {
    Ok {
        result: Box<LogicalReadResult>,
    },
    Error {
        code: LogicalReadErrorCode,
        message: String,
    },
}

impl LogicalReadToolOutput {
    pub(crate) fn error(code: LogicalReadErrorCode, message: impl Into<String>) -> Self {
        Self::Error {
            code,
            message: message.into(),
        }
    }

    pub(crate) fn is_error(&self) -> bool {
        matches!(self, Self::Error { .. })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub(crate) enum LogicalReadResult {
    Profile(ProfileReadResult),
    ThreadTree(ThreadTreeResult),
    ThreadEvents(ThreadEventsResult),
    ThreadTranscript(ThreadTranscriptResult),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ProfileRevisionMetadata {
    pub(crate) revision: u64,
    pub(crate) parent_revision: Option<u64>,
    pub(crate) actor: String,
    pub(crate) created_at_ms: u64,
    pub(crate) content_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ProfileContentView {
    pub(crate) instructions_markdown: String,
    pub(crate) memory_markdown: String,
    pub(crate) skill_refs: Vec<String>,
    pub(crate) truncated: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ProfileRevisionView {
    pub(crate) metadata: ProfileRevisionMetadata,
    pub(crate) content: ProfileContentView,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ProfileReadResult {
    pub(crate) profile_id: String,
    pub(crate) current_revision: u64,
    pub(crate) selected: ProfileRevisionView,
    pub(crate) revisions: Vec<ProfileRevisionMetadata>,
    pub(crate) truncated: bool,
    pub(crate) next_cursor: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProfileFilesystemIsolation {
    Confined,
    Unconfined,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ProfileThreadMetadata {
    pub(crate) thread_id: String,
    pub(crate) parent_thread_id: Option<String>,
    pub(crate) profile_revision: u64,
    pub(crate) thread_kind: ThreadKind,
    pub(crate) created_at_ms: u64,
    pub(crate) stopped_at_ms: Option<u64>,
    pub(crate) confinement: ThreadConfinement,
    pub(crate) profile_filesystem_isolation: ProfileFilesystemIsolation,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ThreadTreeResult {
    pub(crate) profile_id: String,
    pub(crate) threads: Vec<ProfileThreadMetadata>,
    pub(crate) truncated: bool,
    pub(crate) next_cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "entry")]
pub(crate) enum ProfileEventEntry {
    Event {
        session_index: u64,
        run_id: String,
        record: RecordedEvent,
    },
    Omitted {
        session_index: u64,
        run_id: String,
        sequence: u64,
        event: String,
        serialized_bytes: usize,
        reason: String,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct ThreadEventsResult {
    pub(crate) thread_id: String,
    pub(crate) entries: Vec<ProfileEventEntry>,
    pub(crate) truncated: bool,
    pub(crate) next_cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "entry")]
pub(crate) enum ProfileTranscriptEntry {
    Transcript {
        session_index: u64,
        run_id: String,
        status: RunStateName,
        value: TypedTranscriptEntry,
    },
    Omitted {
        session_index: u64,
        run_id: String,
        status: RunStateName,
        serialized_bytes: usize,
        reason: String,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct ThreadTranscriptResult {
    pub(crate) thread_id: String,
    pub(crate) entries: Vec<ProfileTranscriptEntry>,
    pub(crate) truncated: bool,
    pub(crate) next_cursor: Option<String>,
}

#[derive(Clone)]
pub(crate) struct LogicalReadToolHandler {
    execute: Arc<dyn Fn(LogicalReadRequest) -> AppResult<LogicalReadToolOutput> + Send + Sync>,
}

impl std::fmt::Debug for LogicalReadToolHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LogicalReadToolHandler")
            .finish_non_exhaustive()
    }
}

impl LogicalReadToolHandler {
    pub(crate) fn new(
        execute: impl Fn(LogicalReadRequest) -> AppResult<LogicalReadToolOutput> + Send + Sync + 'static,
    ) -> Self {
        Self {
            execute: Arc::new(execute),
        }
    }

    pub(crate) fn execute(&self, request: LogicalReadRequest) -> AppResult<LogicalReadToolOutput> {
        (self.execute)(request)
    }
}

pub(super) fn execute(
    handler: Option<&LogicalReadToolHandler>,
    call_id: ToolCallId,
    tool_name: &str,
    input: Value,
) -> AppResult<ToolResult> {
    let request = LogicalReadRequest::from_tool(tool_name, input)?;
    let output = handler
        .ok_or_else(|| AppError::Tool("profile reads require a profile thread".into()))?
        .execute(request)?;
    let is_error = output.is_error();
    Ok(ToolResult {
        call_id,
        summary: if is_error {
            "profile read denied".into()
        } else {
            "read bounded profile state".into()
        },
        data: serde_json::to_value(output)?,
        artifacts: vec![],
        visibility: ResultVisibility::Both,
    })
}
