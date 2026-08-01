//! Version 1 daemon wire types shared by Plato Agent clients and servers.
//!
//! This crate owns serialization and validation for the newline-delimited JSON
//! protocol. It performs no I/O and contains no runtime or application policy.

#![deny(unsafe_code)]
#![warn(missing_docs)]

use platonic_core::{EffectClass, RecordedEvent};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use serde_json::Value;
use std::fmt;

/// Provider reasoning effort carried by daemon run overrides.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningEffort {
    /// Disable provider reasoning.
    None,
    /// Request minimal reasoning.
    Minimal,
    /// Request low reasoning effort.
    Low,
    /// Request medium reasoning effort.
    Medium,
    /// Request high reasoning effort.
    High,
    /// Request extra-high reasoning effort.
    Xhigh,
    /// Request the provider's maximum reasoning effort.
    Max,
}

impl ReasoningEffort {
    /// Returns the exact wire value for this effort.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Minimal => "minimal",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Xhigh => "xhigh",
            Self::Max => "max",
        }
    }

    /// Parses an exact provider reasoning-effort wire value.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "none" => Some(Self::None),
            "minimal" => Some(Self::Minimal),
            "low" => Some(Self::Low),
            "medium" => Some(Self::Medium),
            "high" => Some(Self::High),
            "xhigh" => Some(Self::Xhigh),
            "max" => Some(Self::Max),
            _ => None,
        }
    }
}

impl fmt::Display for ReasoningEffort {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.pad(self.as_str())
    }
}

/// Per-run model settings supplied by a daemon client.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunOverrides {
    /// Optional model identifier replacing the configured default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Optional reasoning effort replacing the configured default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,
}

impl RunOverrides {
    /// Returns whether neither override is set.
    pub fn is_empty(&self) -> bool {
        self.model.is_none() && self.reasoning_effort.is_none()
    }
}

/// Current daemon protocol version.
pub const PROTOCOL_VERSION: u32 = 1;

/// Capability name for the initial daemon handshake.
pub const CAPABILITY_HELLO: &str = "hello";
/// Capability name for starting a fresh run.
pub const CAPABILITY_RUN_START: &str = "run.start";
/// Capability name for appending a message to a session.
pub const CAPABILITY_MESSAGE_APPEND: &str = "message.append";
/// Capability name for starting issue preparation.
pub const CAPABILITY_ISSUE_PREP_START: &str = "issue-prep.start";
/// Capability name for streaming buffered run events.
pub const CAPABILITY_EVENTS_STREAM: &str = "events.stream";
/// Capability name for deciding a pending approval.
pub const CAPABILITY_APPROVAL_DECIDE: &str = "approval.decide";
/// Capability name for requesting run cancellation.
pub const CAPABILITY_RUN_CANCEL: &str = "run.cancel";
/// Capability name for listing sessions.
pub const CAPABILITY_SESSIONS_LIST: &str = "sessions.list";
/// Capability name for reading a transcript.
pub const CAPABILITY_TRANSCRIPT_READ: &str = "transcript.read";
/// Capability name for typed transcript readback.
pub const CAPABILITY_TRANSCRIPT_READ_TYPED: &str = "transcript.read.typed";
/// Capability name for pending-approval transcript readback.
pub const CAPABILITY_TRANSCRIPT_READ_PENDING_APPROVAL: &str = "transcript.read.pending_approval";
/// Capability name for shutting down an idle daemon.
pub const CAPABILITY_DAEMON_SHUTDOWN_IF_IDLE: &str = "daemon.shutdown_if_idle";

/// Capabilities advertised by a protocol v1 daemon, in wire order.
pub const CAPABILITIES: [&str; 12] = [
    CAPABILITY_HELLO,
    CAPABILITY_RUN_START,
    CAPABILITY_MESSAGE_APPEND,
    CAPABILITY_ISSUE_PREP_START,
    CAPABILITY_EVENTS_STREAM,
    CAPABILITY_APPROVAL_DECIDE,
    CAPABILITY_RUN_CANCEL,
    CAPABILITY_SESSIONS_LIST,
    CAPABILITY_TRANSCRIPT_READ,
    CAPABILITY_TRANSCRIPT_READ_TYPED,
    CAPABILITY_TRANSCRIPT_READ_PENDING_APPROVAL,
    CAPABILITY_DAEMON_SHUTDOWN_IF_IDLE,
];

/// Error code returned once daemon shutdown has begun.
pub const ERROR_DAEMON_SHUTTING_DOWN: &str = "daemon_shutting_down";
/// Error code returned for an invalid request envelope or parameters.
pub const ERROR_MALFORMED_REQUEST: &str = "malformed_request";
/// Error code returned when a requested event offset is no longer retained.
pub const ERROR_LAGGED: &str = "lagged";
/// Error code returned for an unexpected daemon failure.
pub const ERROR_INTERNAL: &str = "internal_error";
/// Error code returned when issue preparation fails.
pub const ERROR_ISSUE_PREP_FAILED: &str = "issue_prep_failed";
/// Error code returned when the requested resource does not exist.
pub const ERROR_NOT_FOUND: &str = "not_found";
/// Error code returned when the daemon cannot admit more work.
pub const ERROR_OVERLOAD: &str = "overload";
/// Error code returned when a run fails.
pub const ERROR_RUN_FAILED: &str = "run_failed";
/// Error code returned when sessions cannot be listed.
pub const ERROR_SESSIONS_LIST_FAILED: &str = "sessions_list_failed";
/// Error code returned for an unknown method.
pub const ERROR_UNSUPPORTED_METHOD: &str = "unsupported_method";
/// Error code returned for an unsupported protocol version.
pub const ERROR_UNSUPPORTED_VERSION: &str = "unsupported_version";
/// Error code returned when client and daemon workspaces differ.
pub const ERROR_WORKSPACE_MISMATCH: &str = "workspace_mismatch";

/// Wire name for a daemon run lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStateName {
    /// The run is executing or waiting for approval.
    Running,
    /// The run completed successfully.
    Finished,
    /// The run ended with a failure.
    Failed,
    /// The run ended after cancellation.
    Canceled,
    /// Cancellation has been requested but is not yet terminal.
    CancelRequested,
    /// Daemon recovery closed a previously running run.
    Interrupted,
}

impl RunStateName {
    /// Returns the exact lifecycle wire value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Finished => "finished",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
            Self::CancelRequested => "cancel_requested",
            Self::Interrupted => "interrupted",
        }
    }
}

impl fmt::Display for RunStateName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.pad(self.as_str())
    }
}

/// Kind discriminator for a protocol envelope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvelopeKind {
    /// A client request.
    Request,
    /// A successful daemon response.
    Response,
    /// A daemon event.
    Event,
    /// A daemon error response.
    Error,
}

/// Top-level versioned protocol envelope.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Envelope {
    /// Protocol version.
    pub v: u32,
    /// Request identifier, when the message belongs to a request.
    pub id: Option<String>,
    /// Envelope kind.
    pub kind: EnvelopeKind,
    /// Method name for requests and their responses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    /// Untyped request parameters decoded by the selected method.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
    /// Untyped result encoded from the selected method's result type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    /// Structured protocol error for an error response.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ProtocolError>,
}

impl Envelope {
    /// Builds a successful response around a JSON result.
    pub fn response(id: Option<String>, method: Option<String>, result: Value) -> Self {
        Self {
            v: PROTOCOL_VERSION,
            id,
            kind: EnvelopeKind::Response,
            method,
            params: None,
            result: Some(result),
            error: None,
        }
    }

    /// Serializes a typed result and builds a successful response.
    pub fn response_from<T: Serialize>(
        id: Option<String>,
        method: Option<String>,
        result: T,
    ) -> Self {
        Self::response(
            id,
            method,
            serde_json::to_value(result).expect("protocol result serializes"),
        )
    }

    /// Builds an error response with the supplied code and message.
    pub fn error(
        id: Option<String>,
        method: Option<String>,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            v: PROTOCOL_VERSION,
            id,
            kind: EnvelopeKind::Error,
            method,
            params: None,
            result: None,
            error: Some(ProtocolError {
                code: code.into(),
                message: message.into(),
            }),
        }
    }
}

/// Structured error returned in a protocol error envelope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, thiserror::Error)]
#[error("{code}: {message}")]
#[serde(deny_unknown_fields)]
pub struct ProtocolError {
    /// Stable machine-readable error code.
    pub code: String,
    /// Human-readable error detail.
    pub message: String,
}

/// Parameters for the initial workspace handshake.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HelloParams {
    /// Canonical workspace root selected by the client.
    pub workspace_root: String,
    /// Stable identifier derived from the workspace root.
    pub workspace_id: String,
}

/// Daemon identity and capability readback returned by `hello`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HelloResult {
    /// Daemon package version.
    pub daemon_version: String,
    /// Workspace identifier served by the daemon.
    pub workspace_id: String,
    /// Daemon-owned ledger path.
    pub ledger_path: String,
    /// Advertised protocol capabilities.
    pub capabilities: Vec<String>,
}

/// Parameters for starting a fresh run.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunStartParams {
    /// User question that starts the run.
    pub question: String,
    /// Optional configuration path for this run.
    #[serde(default)]
    pub config_path: Option<String>,
    /// Optional model settings for this run.
    #[serde(default, skip_serializing_if = "RunOverrides::is_empty")]
    pub overrides: RunOverrides,
    /// Whether the request waits for the run's terminal result.
    #[serde(default)]
    pub wait: Option<bool>,
}

/// Result returned after a fresh run is admitted or completed.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RunStartResult {
    /// Identifier of the admitted run.
    pub run_id: String,
    /// Identifier of the run's session.
    pub session_id: String,
    /// Ledger path recording the run.
    pub ledger_path: String,
    /// Current lifecycle state.
    pub status: RunStateName,
    /// Final assistant answer when the run finished successfully.
    pub final_answer: Option<String>,
}

/// Parameters for appending a user message to a session.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageAppendParams {
    /// User message to append.
    pub message: String,
    /// Target session, or the latest session when omitted.
    #[serde(default)]
    pub session_id: Option<String>,
    /// Optional configuration path for the continued run.
    #[serde(default)]
    pub config_path: Option<String>,
    /// Optional model settings for the continued run.
    #[serde(default, skip_serializing_if = "RunOverrides::is_empty")]
    pub overrides: RunOverrides,
    /// Whether the request waits for the run's terminal result.
    #[serde(default)]
    pub wait: Option<bool>,
}

/// Parameters for starting the fixed issue-preparation pipeline.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IssuePrepStartParams {
    /// Rough issue text to prepare.
    pub input: String,
    /// Optional configuration path for model access.
    #[serde(default)]
    pub config_path: Option<String>,
}

/// Result returned by the issue-preparation pipeline.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IssuePrepStartResult {
    /// Directory containing the pipeline artifacts.
    pub run_dir: String,
    /// Candidate or blocked pipeline outcome.
    pub outcome: IssuePrepResult,
}

/// Typed outcome of issue preparation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum IssuePrepResult {
    /// The pipeline produced a candidate issue.
    Candidate {
        /// Prepared issue Markdown.
        markdown: String,
    },
    /// The pipeline stopped on structural findings.
    Blocked {
        /// Pipeline stage that blocked.
        stage: String,
        /// Reasons the stage could not produce an acceptable result.
        reasons: Vec<String>,
    },
}

/// Parameters for reading a page of buffered run events.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventsStreamParams {
    /// Run whose events should be read.
    pub run_id: String,
    /// First event offset to return, or the current tip when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_offset: Option<u64>,
    /// Maximum number of events to return.
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Stream event paired with its run-local offset.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BufferedStreamEvent {
    /// Contiguous run-local event offset.
    pub offset: u64,
    /// Typed or forward-compatible stream event.
    pub event: StreamEvent,
}

/// Event carried by `events.stream`.
#[derive(Clone, Debug, PartialEq)]
pub enum StreamEvent {
    /// Durable ledger event.
    Ledger {
        /// Recorded harness event.
        record: RecordedEvent,
    },
    /// Transient assistant text delta.
    AssistantDelta {
        /// Run producing the delta.
        run_id: String,
        /// Turn producing the delta.
        turn_id: String,
        /// Model step producing the delta.
        step: u32,
        /// Delta sequence within the step.
        delta_index: u64,
        /// Assistant text fragment.
        text: String,
    },
    /// Approval request emitted while a run is paused.
    ApprovalRequested {
        /// Run waiting for approval.
        run_id: String,
        /// Tool call waiting for approval.
        tool_call_id: String,
        /// Requested tool name.
        tool_name: String,
        /// Effect class subject to policy.
        effect: EffectClass,
        /// Reason approval is required.
        reason: String,
        /// Optional proposed file diff.
        diff_preview: Option<String>,
        /// Optional concise approval summary.
        approval_preview: Option<String>,
    },
    /// Cancellation observation for a run.
    Canceled {
        /// Canceled run identifier.
        run_id: String,
    },
    /// Unrecognized future event preserved without modification.
    Unknown(Value),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum KnownStreamEvent {
    Ledger {
        record: RecordedEvent,
    },
    AssistantDelta {
        run_id: String,
        turn_id: String,
        step: u32,
        delta_index: u64,
        text: String,
    },
    ApprovalRequested {
        run_id: String,
        tool_call_id: String,
        tool_name: String,
        effect: EffectClass,
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        diff_preview: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        approval_preview: Option<String>,
    },
    Canceled {
        run_id: String,
    },
}

impl Serialize for StreamEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Ledger { record } => KnownStreamEvent::Ledger {
                record: record.clone(),
            }
            .serialize(serializer),
            Self::AssistantDelta {
                run_id,
                turn_id,
                step,
                delta_index,
                text,
            } => KnownStreamEvent::AssistantDelta {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                step: *step,
                delta_index: *delta_index,
                text: text.clone(),
            }
            .serialize(serializer),
            Self::ApprovalRequested {
                run_id,
                tool_call_id,
                tool_name,
                effect,
                reason,
                diff_preview,
                approval_preview,
            } => KnownStreamEvent::ApprovalRequested {
                run_id: run_id.clone(),
                tool_call_id: tool_call_id.clone(),
                tool_name: tool_name.clone(),
                effect: effect.clone(),
                reason: reason.clone(),
                diff_preview: diff_preview.clone(),
                approval_preview: approval_preview.clone(),
            }
            .serialize(serializer),
            Self::Canceled { run_id } => KnownStreamEvent::Canceled {
                run_id: run_id.clone(),
            }
            .serialize(serializer),
            Self::Unknown(value) => value.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for StreamEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let kind = value
            .get("kind")
            .and_then(Value::as_str)
            .ok_or_else(|| D::Error::custom("stream event kind must be a string"))?;
        match kind {
            "ledger" | "assistant_delta" | "approval_requested" | "canceled" => {
                serde_json::from_value::<KnownStreamEvent>(value)
                    .map(StreamEvent::from)
                    .map_err(D::Error::custom)
            }
            _ => Ok(Self::Unknown(value)),
        }
    }
}

impl From<KnownStreamEvent> for StreamEvent {
    fn from(event: KnownStreamEvent) -> Self {
        match event {
            KnownStreamEvent::Ledger { record } => Self::Ledger { record },
            KnownStreamEvent::AssistantDelta {
                run_id,
                turn_id,
                step,
                delta_index,
                text,
            } => Self::AssistantDelta {
                run_id,
                turn_id,
                step,
                delta_index,
                text,
            },
            KnownStreamEvent::ApprovalRequested {
                run_id,
                tool_call_id,
                tool_name,
                effect,
                reason,
                diff_preview,
                approval_preview,
            } => Self::ApprovalRequested {
                run_id,
                tool_call_id,
                tool_name,
                effect,
                reason,
                diff_preview,
                approval_preview,
            },
            KnownStreamEvent::Canceled { run_id } => Self::Canceled { run_id },
        }
    }
}

/// Page of buffered stream events and its continuation offset.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventsStreamResult {
    /// Run whose events were read.
    pub run_id: String,
    /// First requested event offset.
    pub from_offset: u64,
    /// Offset to use for the next page.
    pub next_offset: u64,
    /// Current run state after the page was read.
    pub status: RunStateName,
    /// Events in contiguous offset order.
    pub events: Vec<BufferedStreamEvent>,
}

/// Client decision for a pending approval request.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecision {
    /// Permit the requested tool call.
    Grant,
    /// Refuse the requested tool call.
    Deny,
}

/// Parameters for deciding a pending approval request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalDecideParams {
    /// Run waiting for the decision.
    pub run_id: String,
    /// Tool call waiting for the decision.
    pub tool_call_id: String,
    /// Grant or deny decision.
    pub decision: ApprovalDecision,
    /// Optional human reason for the decision.
    #[serde(default)]
    pub reason: Option<String>,
}

/// Parameters for requesting run cancellation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunCancelParams {
    /// Run to cancel.
    pub run_id: String,
}

/// Result returned after a run mutation is accepted.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommandAcceptedResult {
    /// Run affected by the command.
    pub run_id: String,
    /// Run state after the command was accepted.
    pub status: RunStateName,
}

/// Outcome name for an idle-daemon shutdown request.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShutdownIfIdleResultName {
    /// The idle daemon began graceful shutdown.
    Shutdown,
    /// The daemon remained active because work was in progress.
    RefusedActive,
}

/// Result returned by `daemon.shutdown_if_idle`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ShutdownIfIdleResult {
    /// Shutdown or active-work refusal outcome.
    pub result: ShutdownIfIdleResultName,
}

/// Result returned by `sessions.list`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SessionsListResult {
    /// Sessions ordered by daemon readback policy.
    pub sessions: Vec<SessionSummary>,
}

/// Summary of the latest run in a session.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SessionSummary {
    /// Session identifier.
    pub session_id: String,
    /// Latest run identifier.
    pub run_id: String,
    /// Latest run state.
    pub status: RunStateName,
    /// Latest user question, possibly presentation-truncated by the daemon.
    pub latest_question: String,
    /// Ledger containing the session.
    pub ledger_path: String,
}

/// Parameters for reading one run or session transcript.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TranscriptReadParams {
    /// Optional run selector.
    #[serde(default)]
    pub run_id: Option<String>,
    /// Optional session selector.
    #[serde(default)]
    pub session_id: Option<String>,
}

/// Legacy and typed transcript readback for a run or session.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TranscriptReadResult {
    /// Selected or latest run identifier.
    pub run_id: String,
    /// Selected or latest run state.
    pub status: RunStateName,
    /// Final assistant answer when the selected run finished successfully.
    pub final_answer: Option<String>,
    /// Legacy rendered transcript.
    pub transcript: String,
    /// Additive structured transcript readback.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub typed: Option<TypedTranscript>,
    /// Additive snapshot of an approval currently awaiting a decision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_approval: Option<PendingApprovalSnapshot>,
}

/// Complete readback of an approval currently awaiting a decision.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PendingApprovalSnapshot {
    /// Run waiting for approval.
    pub run_id: String,
    /// Tool call waiting for approval.
    pub tool_call_id: String,
    /// Requested tool name.
    pub tool_name: String,
    /// Effect class subject to policy.
    pub effect: EffectClass,
    /// Optional reason approval is required.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Optional bounded tool-input preview.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_preview: Option<String>,
    /// Optional concise approval summary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_preview: Option<String>,
    /// Optional proposed file diff.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diff_preview: Option<String>,
}

/// Structured transcript containing one or more ordered runs.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TypedTranscript {
    /// Runs included in the readback.
    pub runs: Vec<TypedRun>,
}

/// Structured transcript entries for one run.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TypedRun {
    /// Run identifier.
    pub run_id: String,
    /// Zero-based run position within the session.
    pub session_index: u64,
    /// Run state.
    pub status: RunStateName,
    /// Ordered transcript entries.
    pub entries: Vec<TypedTranscriptEntry>,
}

/// Durable approval decision name rendered in a typed transcript.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecisionName {
    /// Approval was granted.
    Granted,
    /// Approval was denied.
    Denied,
}

/// One ordered entry in a typed transcript.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum TypedTranscriptEntry {
    /// User chat message.
    User {
        /// Message text.
        text: String,
    },
    /// Final assistant chat message.
    Assistant {
        /// Message text.
        text: String,
    },
    /// Tool invocation proposed by the model.
    ToolCall {
        /// Host-minted tool call identifier.
        call_id: String,
        /// Tool name.
        tool: String,
        /// Structured tool input.
        input: Value,
    },
    /// Successful tool result.
    ToolResult {
        /// Host-minted tool call identifier.
        call_id: String,
        /// Concise result summary.
        summary: String,
    },
    /// Human approval decision.
    Approval {
        /// Host-minted tool call identifier.
        call_id: String,
        /// Granted or denied outcome.
        decision: ApprovalDecisionName,
        /// Actor that made the decision.
        actor_id: String,
        /// Optional decision reason.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    /// Tool call refused by policy before execution.
    PolicyDenied {
        /// Host-minted tool call identifier.
        call_id: String,
        /// Policy denial reason.
        reason: String,
    },
    /// Tool call that failed during execution.
    ToolFailed {
        /// Host-minted tool call identifier.
        call_id: String,
        /// Tool failure detail.
        error: String,
    },
}

/// Decodes and validates a protocol request envelope from one JSON line.
///
/// Validation failures are returned as ready-to-send protocol error envelopes.
pub fn decode_request(line: &str) -> Result<Envelope, Box<Envelope>> {
    let envelope = serde_json::from_str::<Envelope>(line).map_err(|error| {
        Box::new(Envelope::error(
            None,
            None,
            ERROR_MALFORMED_REQUEST,
            format!("request is not a valid protocol envelope: {error}"),
        ))
    })?;

    if envelope.v != PROTOCOL_VERSION {
        return Err(Box::new(Envelope::error(
            envelope.id,
            envelope.method,
            ERROR_UNSUPPORTED_VERSION,
            format!("unsupported protocol version: {}", envelope.v),
        )));
    }
    if envelope.kind != EnvelopeKind::Request {
        return Err(Box::new(Envelope::error(
            envelope.id,
            envelope.method,
            ERROR_MALFORMED_REQUEST,
            "envelope kind must be request",
        )));
    }
    if envelope.method.is_none() {
        return Err(Box::new(Envelope::error(
            envelope.id,
            None,
            ERROR_MALFORMED_REQUEST,
            "request method is required",
        )));
    }

    Ok(envelope)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::de::DeserializeOwned;
    use serde_json::json;

    #[test]
    fn run_state_names_keep_wire_values() {
        let cases = [
            (RunStateName::Running, "running"),
            (RunStateName::Finished, "finished"),
            (RunStateName::Failed, "failed"),
            (RunStateName::Canceled, "canceled"),
            (RunStateName::CancelRequested, "cancel_requested"),
            (RunStateName::Interrupted, "interrupted"),
        ];

        for (state, wire_value) in cases {
            assert_eq!(state.as_str(), wire_value);
            assert_eq!(state.to_string(), wire_value);
            assert_eq!(serde_json::to_value(state).unwrap(), wire_value);
            assert_eq!(
                serde_json::from_value::<RunStateName>(wire_value.into()).unwrap(),
                state
            );
        }
    }

    #[test]
    fn reasoning_effort_uses_the_provider_wire_values() {
        for (wire, effort) in [
            ("none", ReasoningEffort::None),
            ("minimal", ReasoningEffort::Minimal),
            ("low", ReasoningEffort::Low),
            ("medium", ReasoningEffort::Medium),
            ("high", ReasoningEffort::High),
            ("xhigh", ReasoningEffort::Xhigh),
            ("max", ReasoningEffort::Max),
        ] {
            assert_eq!(ReasoningEffort::parse(wire), Some(effort));
            assert_eq!(effort.as_str(), wire);
            assert_eq!(effort.to_string(), wire);
            assert_eq!(serde_json::to_value(effort).unwrap(), json!(wire));
        }
        assert_eq!(ReasoningEffort::parse("default"), None);
    }

    #[test]
    fn capability_names_and_error_codes_keep_exact_v1_literals() {
        assert_eq!(
            CAPABILITIES,
            [
                "hello",
                "run.start",
                "message.append",
                "issue-prep.start",
                "events.stream",
                "approval.decide",
                "run.cancel",
                "sessions.list",
                "transcript.read",
                "transcript.read.typed",
                "transcript.read.pending_approval",
                "daemon.shutdown_if_idle",
            ]
        );
        assert_eq!(
            [
                ERROR_DAEMON_SHUTTING_DOWN,
                ERROR_MALFORMED_REQUEST,
                ERROR_LAGGED,
                ERROR_INTERNAL,
                ERROR_ISSUE_PREP_FAILED,
                ERROR_NOT_FOUND,
                ERROR_OVERLOAD,
                ERROR_RUN_FAILED,
                ERROR_SESSIONS_LIST_FAILED,
                ERROR_UNSUPPORTED_METHOD,
                ERROR_UNSUPPORTED_VERSION,
                ERROR_WORKSPACE_MISMATCH,
            ],
            [
                "daemon_shutting_down",
                "malformed_request",
                "lagged",
                "internal_error",
                "issue_prep_failed",
                "not_found",
                "overload",
                "run_failed",
                "sessions_list_failed",
                "unsupported_method",
                "unsupported_version",
                "workspace_mismatch",
            ]
        );

        let error = ProtocolError {
            code: ERROR_RUN_FAILED.into(),
            message: "synthetic failure".into(),
        };
        assert_eq!(error.to_string(), "run_failed: synthetic failure");
    }

    #[test]
    fn request_and_error_envelopes_keep_exact_v1_bytes() {
        const RUN_CANCEL_REQUEST: &str = r#"{"v":1,"id":"cancel_1","kind":"request","method":"run.cancel","params":{"run_id":"run_1"}}"#;
        const RUN_FAILED_RESPONSE: &str = r#"{"v":1,"id":"run_1","kind":"error","method":"run.start","error":{"code":"run_failed","message":"synthetic failure"}}"#;

        let request = decode_request(RUN_CANCEL_REQUEST).unwrap();
        assert_eq!(serde_json::to_string(&request).unwrap(), RUN_CANCEL_REQUEST);

        let response = Envelope::error(
            Some("run_1".into()),
            Some("run.start".into()),
            ERROR_RUN_FAILED,
            "synthetic failure",
        );
        assert_eq!(
            serde_json::to_string(&response).unwrap(),
            RUN_FAILED_RESPONSE
        );
    }

    #[test]
    fn strict_request_and_error_dtos_reject_unknown_fields() {
        assert_unknown_field_rejected::<Envelope>(json!({
            "v": 1,
            "id": "req_1",
            "kind": "request",
            "method": "hello",
            "future": true
        }));
        assert_unknown_field_rejected::<ProtocolError>(json!({
            "code": "internal_error",
            "message": "failed",
            "future": true
        }));
        assert_unknown_field_rejected::<HelloParams>(json!({
            "workspace_root": "/tmp/work",
            "workspace_id": "work-1234",
            "future": true
        }));
        assert_unknown_field_rejected::<RunStartParams>(json!({
            "question": "hello",
            "future": true
        }));
        assert_unknown_field_rejected::<MessageAppendParams>(json!({
            "message": "hello",
            "future": true
        }));
        assert_unknown_field_rejected::<IssuePrepStartParams>(json!({
            "input": "rough issue",
            "future": true
        }));
        assert_unknown_field_rejected::<EventsStreamParams>(json!({
            "run_id": "run_1",
            "future": true
        }));
        assert_unknown_field_rejected::<ApprovalDecideParams>(json!({
            "run_id": "run_1",
            "tool_call_id": "call_1",
            "decision": "grant",
            "future": true
        }));
        assert_unknown_field_rejected::<RunCancelParams>(json!({
            "run_id": "run_1",
            "future": true
        }));
        assert_unknown_field_rejected::<TranscriptReadParams>(json!({
            "run_id": "run_1",
            "future": true
        }));
        assert_unknown_field_rejected::<RunOverrides>(json!({
            "model": "openai/gpt-5",
            "future": true
        }));
    }

    fn assert_unknown_field_rejected<T: DeserializeOwned>(value: Value) {
        let error = serde_json::from_value::<T>(value)
            .err()
            .expect("DTO accepted an unknown field");
        assert!(error.to_string().contains("unknown field `future`"));
    }

    #[test]
    fn approval_decisions_keep_exact_v1_wire_values() {
        let cases = [
            (ApprovalDecision::Grant, "grant"),
            (ApprovalDecision::Deny, "deny"),
        ];

        for (decision, wire_value) in cases {
            assert_eq!(serde_json::to_value(decision).unwrap(), wire_value);
            assert_eq!(
                serde_json::from_value::<ApprovalDecision>(wire_value.into()).unwrap(),
                decision
            );
        }
        assert!(serde_json::from_value::<ApprovalDecision>(json!("granted")).is_err());
    }

    #[test]
    fn run_overrides_are_additive_and_omitted_by_default() {
        let legacy: RunStartParams = serde_json::from_value(json!({
            "question": "hello",
            "wait": false
        }))
        .unwrap();
        assert_eq!(legacy.overrides, RunOverrides::default());
        assert!(
            serde_json::to_value(legacy)
                .unwrap()
                .get("overrides")
                .is_none()
        );

        let current = RunStartParams {
            question: "hello".into(),
            config_path: None,
            overrides: RunOverrides {
                model: Some("openai/gpt-5".into()),
                reasoning_effort: Some(ReasoningEffort::High),
            },
            wait: Some(false),
        };
        assert_eq!(
            serde_json::to_value(current).unwrap()["overrides"],
            json!({
                "model": "openai/gpt-5",
                "reasoning_effort": "high"
            })
        );
    }

    #[test]
    fn issue_prep_result_keeps_typed_wire_shape() {
        let result = IssuePrepStartResult {
            run_dir: "/work/.plato/issue-prep/run_1".into(),
            outcome: IssuePrepResult::Blocked {
                stage: "review".into(),
                reasons: vec!["acceptance is not testable".into()],
            },
        };

        let wire = serde_json::to_value(&result).unwrap();

        assert_eq!(
            wire,
            json!({
                "run_dir": "/work/.plato/issue-prep/run_1",
                "outcome": {
                    "status": "blocked",
                    "stage": "review",
                    "reasons": ["acceptance is not testable"]
                }
            })
        );
        assert_eq!(
            serde_json::from_value::<IssuePrepStartResult>(wire).unwrap(),
            result
        );
    }

    #[test]
    fn decodes_request_envelope() {
        let envelope = decode_request(
            r#"{"v":1,"id":"req_1","kind":"request","method":"hello","params":{"workspace_root":"/tmp/work","workspace_id":"work-1234"}}"#,
        )
        .unwrap();

        assert_eq!(envelope.id.as_deref(), Some("req_1"));
        assert_eq!(envelope.method.as_deref(), Some("hello"));
    }

    #[test]
    fn rejects_unsupported_version_with_typed_error() {
        let error =
            decode_request(r#"{"v":2,"id":"req_1","kind":"request","method":"hello","params":{}}"#)
                .unwrap_err();

        assert_eq!(error.kind, EnvelopeKind::Error);
        assert_eq!(
            error.error.unwrap().code,
            ERROR_UNSUPPORTED_VERSION.to_string()
        );
    }

    #[test]
    fn response_serializes_without_request_params() {
        let response = Envelope::response(
            Some("req_1".into()),
            Some("hello".into()),
            serde_json::json!({"workspace_id":"work-1234"}),
        );

        let raw = serde_json::to_string(&response).unwrap();

        assert!(raw.contains(r#""kind":"response""#));
        assert!(!raw.contains("params"));
    }

    #[test]
    fn typed_transcript_keeps_exact_wire_shape_and_both_compat_directions() {
        let current = TranscriptReadResult {
            run_id: "run_1".into(),
            status: RunStateName::Finished,
            final_answer: Some("done".into()),
            transcript: "legacy replay".into(),
            typed: Some(TypedTranscript {
                runs: vec![TypedRun {
                    run_id: "run_1".into(),
                    session_index: 0,
                    status: RunStateName::Finished,
                    entries: vec![
                        TypedTranscriptEntry::User {
                            text: "do work".into(),
                        },
                        TypedTranscriptEntry::Assistant {
                            text: "working".into(),
                        },
                        TypedTranscriptEntry::ToolCall {
                            call_id: "call_1".into(),
                            tool: "file.write".into(),
                            input: json!({"path": "out.txt", "content": "done"}),
                        },
                        TypedTranscriptEntry::ToolResult {
                            call_id: "call_1".into(),
                            summary: "wrote out.txt".into(),
                        },
                        TypedTranscriptEntry::Approval {
                            call_id: "call_1".into(),
                            decision: ApprovalDecisionName::Granted,
                            actor_id: "human_1".into(),
                            reason: None,
                        },
                        TypedTranscriptEntry::Approval {
                            call_id: "call_2".into(),
                            decision: ApprovalDecisionName::Denied,
                            actor_id: "human_2".into(),
                            reason: Some("not now".into()),
                        },
                        TypedTranscriptEntry::PolicyDenied {
                            call_id: "call_3".into(),
                            reason: "secret access denied".into(),
                        },
                        TypedTranscriptEntry::ToolFailed {
                            call_id: "call_4".into(),
                            error: "tool crashed".into(),
                        },
                    ],
                }],
            }),
            pending_approval: None,
        };

        let wire = serde_json::to_value(&current).unwrap();
        assert_eq!(
            wire,
            json!({
                "run_id": "run_1",
                "status": "finished",
                "final_answer": "done",
                "transcript": "legacy replay",
                "typed": {
                    "runs": [{
                        "run_id": "run_1",
                        "session_index": 0,
                        "status": "finished",
                        "entries": [
                            {"kind": "user", "text": "do work"},
                            {"kind": "assistant", "text": "working"},
                            {
                                "kind": "tool_call",
                                "call_id": "call_1",
                                "tool": "file.write",
                                "input": {"path": "out.txt", "content": "done"}
                            },
                            {
                                "kind": "tool_result",
                                "call_id": "call_1",
                                "summary": "wrote out.txt"
                            },
                            {
                                "kind": "approval",
                                "call_id": "call_1",
                                "decision": "granted",
                                "actor_id": "human_1"
                            },
                            {
                                "kind": "approval",
                                "call_id": "call_2",
                                "decision": "denied",
                                "actor_id": "human_2",
                                "reason": "not now"
                            },
                            {
                                "kind": "policy_denied",
                                "call_id": "call_3",
                                "reason": "secret access denied"
                            },
                            {
                                "kind": "tool_failed",
                                "call_id": "call_4",
                                "error": "tool crashed"
                            }
                        ]
                    }]
                }
            })
        );

        #[derive(Deserialize)]
        struct LegacyTranscriptReadResult {
            run_id: String,
            status: RunStateName,
            final_answer: Option<String>,
            transcript: String,
        }

        let legacy_client: LegacyTranscriptReadResult =
            serde_json::from_value(wire).expect("legacy clients ignore typed");
        assert_eq!(legacy_client.run_id, "run_1");
        assert_eq!(legacy_client.status, RunStateName::Finished);
        assert_eq!(legacy_client.final_answer.as_deref(), Some("done"));
        assert_eq!(legacy_client.transcript, "legacy replay");

        let current_client: TranscriptReadResult = serde_json::from_value(json!({
            "run_id": "run_1",
            "status": "finished",
            "final_answer": "done",
            "transcript": "legacy replay"
        }))
        .expect("current clients decode typed-less daemon responses");
        assert_eq!(current_client.typed, None);
        assert_eq!(current_client.pending_approval, None);
    }

    #[test]
    fn pending_approval_snapshot_keeps_exact_additive_wire_shape() {
        let current = TranscriptReadResult {
            run_id: "run_1".into(),
            status: RunStateName::Running,
            final_answer: None,
            transcript: "partial replay".into(),
            typed: None,
            pending_approval: Some(PendingApprovalSnapshot {
                run_id: "run_1".into(),
                tool_call_id: "call_1".into(),
                tool_name: "file.write".into(),
                effect: EffectClass::WorkspaceWrite,
                reason: Some("file.write requires approval".into()),
                input_preview: Some(r#"{"path":"out.txt"}"#.into()),
                approval_preview: Some("write out.txt".into()),
                diff_preview: Some("--- a/out.txt\n+++ b/out.txt\n".into()),
            }),
        };

        let wire = serde_json::to_value(&current).unwrap();
        assert_eq!(
            wire,
            json!({
                "run_id": "run_1",
                "status": "running",
                "final_answer": null,
                "transcript": "partial replay",
                "pending_approval": {
                    "run_id": "run_1",
                    "tool_call_id": "call_1",
                    "tool_name": "file.write",
                    "effect": "workspace_write",
                    "reason": "file.write requires approval",
                    "input_preview": "{\"path\":\"out.txt\"}",
                    "approval_preview": "write out.txt",
                    "diff_preview": "--- a/out.txt\n+++ b/out.txt\n"
                }
            })
        );

        #[derive(Deserialize)]
        struct LegacyTranscriptReadResult {
            run_id: String,
            status: RunStateName,
            transcript: String,
        }

        let decoded: TranscriptReadResult = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(decoded, current);

        let legacy: LegacyTranscriptReadResult = serde_json::from_value(wire).unwrap();
        assert_eq!(legacy.run_id, "run_1");
        assert_eq!(legacy.status, RunStateName::Running);
        assert_eq!(legacy.transcript, "partial replay");

        let minimal = serde_json::to_value(PendingApprovalSnapshot {
            run_id: "run_2".into(),
            tool_call_id: "call_2".into(),
            tool_name: "shell.exec".into(),
            effect: EffectClass::ExternalSideEffect,
            reason: None,
            input_preview: None,
            approval_preview: None,
            diff_preview: None,
        })
        .unwrap();
        assert_eq!(
            minimal,
            json!({
                "run_id": "run_2",
                "tool_call_id": "call_2",
                "tool_name": "shell.exec",
                "effect": "external_side_effect"
            })
        );
    }

    #[test]
    fn stream_event_known_variants_keep_the_exact_wire_shape() {
        let fixtures = [
            json!({
                "kind": "ledger",
                "record": {
                    "seq": 3,
                    "occurred_at_ms": 42,
                    "event": {
                        "event": "run_finished",
                        "run_id": "run_1"
                    }
                }
            }),
            json!({
                "kind": "assistant_delta",
                "run_id": "run_1",
                "turn_id": "turn_1",
                "step": 2,
                "delta_index": 7,
                "text": "hello"
            }),
            json!({
                "kind": "approval_requested",
                "run_id": "run_1",
                "tool_call_id": "call_1",
                "tool_name": "file.edit",
                "effect": "workspace_write",
                "reason": "approval required",
                "diff_preview": "--- a/file\n+++ b/file\n",
                "approval_preview": "edit file"
            }),
            json!({
                "kind": "approval_requested",
                "run_id": "run_1",
                "tool_call_id": "call_2",
                "tool_name": "file.write",
                "effect": "workspace_write",
                "reason": "approval required"
            }),
            json!({
                "kind": "canceled",
                "run_id": "run_1"
            }),
        ];

        for fixture in fixtures {
            let event: StreamEvent = serde_json::from_value(fixture.clone()).unwrap();
            assert_eq!(serde_json::to_value(event).unwrap(), fixture);
        }
    }

    #[test]
    fn unknown_stream_event_preserves_its_complete_payload_and_offset() {
        let fixture = json!({
            "offset": 9,
            "event": {
                "kind": "future_event",
                "run_id": "run_1",
                "nested": {"answer": 42},
                "optional": null
            }
        });

        let buffered: BufferedStreamEvent = serde_json::from_value(fixture.clone()).unwrap();
        assert!(matches!(
            &buffered.event,
            StreamEvent::Unknown(value) if value == &fixture["event"]
        ));
        assert_eq!(serde_json::to_value(buffered).unwrap(), fixture);
    }

    #[test]
    fn malformed_known_stream_events_fail_decode() {
        let malformed = [
            json!({"kind": "ledger", "record": {}}),
            json!({
                "kind": "assistant_delta",
                "run_id": "run_1",
                "turn_id": "turn_1",
                "step": 0,
                "delta_index": 0
            }),
            json!({
                "kind": "approval_requested",
                "run_id": "run_1",
                "tool_call_id": "call_1",
                "tool_name": "file.write",
                "effect": "workspace_write"
            }),
            json!({"kind": "canceled"}),
            json!({"payload": "missing kind"}),
        ];

        for fixture in malformed {
            assert!(serde_json::from_value::<StreamEvent>(fixture).is_err());
        }
    }
}
