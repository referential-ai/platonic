//! Version 1 daemon wire types shared by Plato Agent clients and servers.
//!
//! This crate owns serialization and validation for the newline-delimited JSON
//! protocol. It performs no I/O and contains no runtime or application policy.

#![deny(unsafe_code)]
#![warn(missing_docs)]

use platonic_core::{EffectClass, RecordedEvent};
pub use platonic_core::{HarnessEvent, PolicyDecision};
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

/// Package version, full source commit, and UTC build date embedded at compile time.
///
/// Repository builds that do not provide deploy provenance remain visibly
/// unknown instead of resembling a dated release build.
pub const BUILD_IDENTITY: &str = match option_env!("PLATO_BUILD_IDENTITY") {
    Some(identity) => identity,
    None => concat!(env!("CARGO_PKG_VERSION"), " unknown unknown"),
};

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
/// Capability name for authoritative daemon status readback.
pub const CAPABILITY_DAEMON_STATUS: &str = "daemon.status";
/// Capability name for shutting down an idle daemon.
pub const CAPABILITY_DAEMON_SHUTDOWN_IF_IDLE: &str = "daemon.shutdown_if_idle";
/// Capability name for durably creating a thread authority record.
pub const CAPABILITY_THREAD_SPAWN: &str = "thread.spawn";
/// Capability name for listing durable threads with live daemon state.
pub const CAPABILITY_THREAD_LIST: &str = "thread.list";
/// Capability name for reading one durable thread with live daemon state.
pub const CAPABILITY_THREAD_STATUS: &str = "thread.status";
/// Capability name for starting or steering one daemon-owned thread turn.
pub const CAPABILITY_THREAD_SEND: &str = "thread.send";
/// Capability name for observing retained live thread events.
pub const CAPABILITY_THREAD_EVENTS: &str = "thread.events";

/// Capabilities advertised by a protocol v1 daemon, in wire order.
pub const CAPABILITIES: [&str; 18] = [
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
    CAPABILITY_DAEMON_STATUS,
    CAPABILITY_DAEMON_SHUTDOWN_IF_IDLE,
    CAPABILITY_THREAD_SPAWN,
    CAPABILITY_THREAD_LIST,
    CAPABILITY_THREAD_STATUS,
    CAPABILITY_THREAD_SEND,
    CAPABILITY_THREAD_EVENTS,
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
/// Error code returned when requested thread authority exceeds its parent.
pub const ERROR_THREAD_AUTHORITY_EXCEEDED: &str = "thread_authority_exceeded";
/// Error code returned when live thread event observation fails.
pub const ERROR_THREAD_EVENTS_FAILED: &str = "thread_events_failed";
/// Error code returned when durable thread enumeration fails.
pub const ERROR_THREAD_LIST_FAILED: &str = "thread_list_failed";
/// Error code returned when thread spawn admission or persistence fails.
pub const ERROR_THREAD_SPAWN_FAILED: &str = "thread_spawn_failed";
/// Error code returned when an admitted thread send cannot start its run.
pub const ERROR_THREAD_SEND_FAILED: &str = "thread_send_failed";
/// Error code returned when one thread status cannot be read.
pub const ERROR_THREAD_STATUS_FAILED: &str = "thread_status_failed";
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
    /// Daemon package version and build provenance.
    pub daemon_version: String,
    /// Workspace identifier served by the daemon.
    pub workspace_id: String,
    /// Daemon-owned ledger path.
    pub ledger_path: String,
    /// Advertised protocol capabilities.
    pub capabilities: Vec<String>,
}

/// Parameters for one authoritative daemon status readback.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonStatusParams {
    /// Selected session, or the latest persisted session when omitted.
    #[serde(default)]
    pub session_id: Option<String>,
    /// Optional explicit configuration path resolved under the run-start rules.
    #[serde(default)]
    pub config_path: Option<String>,
}

/// Authoritative read-only status returned by `daemon.status`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonStatusResult {
    /// Effective model and provider facts.
    pub model: DaemonStatusModel,
    /// Current daemon identity facts.
    pub daemon: DaemonStatusDaemon,
    /// Selected persisted-session facts.
    pub session: DaemonStatusSession,
    /// Provider-reported token usage facts.
    pub usage: DaemonStatusUsage,
    /// Persisted approval facts.
    pub trust: DaemonStatusTrust,
}

/// Effective model facts returned by `daemon.status`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonStatusModel {
    /// Model or alias selected by the effective current configuration.
    pub requested_alias: String,
    /// Provider-reported model from the latest selected-session response.
    pub served_model: Option<String>,
    /// Configured provider kind.
    pub provider_kind: DaemonStatusProviderKind,
    /// Whether the configured provider key environment variable is present.
    pub key_present: bool,
}

/// Provider kind returned by `daemon.status`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonStatusProviderKind {
    /// OpenAI-compatible direct OpenAI provider.
    OpenAi,
    /// OpenRouter provider.
    OpenRouter,
}

impl DaemonStatusProviderKind {
    /// Returns the exact provider-kind wire value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OpenAi => "open_ai",
            Self::OpenRouter => "open_router",
        }
    }
}

impl fmt::Display for DaemonStatusProviderKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.pad(self.as_str())
    }
}

/// Current daemon identity facts returned by `daemon.status`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonStatusDaemon {
    /// Daemon package version.
    pub package_version: String,
    /// Full source commit from build provenance, when known.
    pub build_commit: Option<String>,
    /// UTC build date from build provenance, when known.
    pub build_date_utc: Option<String>,
    /// Monotonic process uptime in milliseconds.
    pub uptime_ms: u64,
    /// Daemon socket or named-pipe endpoint path.
    pub endpoint_path: String,
    /// Workspace identifier served by the daemon.
    pub workspace_id: String,
}

/// Selected persisted-session facts returned by `daemon.status`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonStatusSession {
    /// Explicitly selected or latest persisted session, or none.
    pub session_id: Option<String>,
    /// Latest run in the selected session, or none.
    pub latest_run_id: Option<String>,
    /// Number of persisted runs and human questions in the selected session.
    pub human_turn_count: u64,
    /// Daemon-owned SQLite ledger path.
    pub ledger_path: String,
    /// Number of persisted core events in the selected session.
    pub core_event_count: u64,
}

/// Last-run and session-cumulative usage returned by `daemon.status`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonStatusUsage {
    /// Usage aggregated across responses in the latest selected-session run.
    pub last_run: DaemonStatusTokenUsage,
    /// Usage aggregated across every response in the selected session.
    pub session: DaemonStatusTokenUsage,
}

/// Known token subtotals plus the count of responses with unknown usage.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonStatusTokenUsage {
    /// Sum of known provider-reported input tokens.
    pub input_tokens: u64,
    /// Sum of known provider-reported output tokens.
    pub output_tokens: u64,
    /// Number of model responses whose usage was unknown.
    pub unknown_response_count: u64,
}

/// Persisted approval facts returned by `daemon.status`.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonStatusTrust {
    /// Number of persisted `approval_granted` events.
    pub approval_granted_count: u64,
    /// Number of persisted `approval_denied` events.
    pub approval_denied_count: u64,
    /// Whether the selected session has a live daemon-lifetime `shell.exec` grant.
    #[serde(default)]
    pub shell_session_grant: bool,
}

/// Immutable startup approval policy carried by a thread authority record.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadApprovalPolicy {
    /// Effects requiring approval pause for an explicit actor decision.
    Prompt,
    /// Eligible workspace-write effects follow the existing yolo auto-grant rules.
    Yolo,
}

impl ThreadApprovalPolicy {
    /// Returns the exact wire and persistence value for this policy.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Prompt => "prompt",
            Self::Yolo => "yolo",
        }
    }

    /// Parses an exact thread approval-policy value.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "prompt" => Some(Self::Prompt),
            "yolo" => Some(Self::Yolo),
            _ => None,
        }
    }

    /// Returns whether this parent policy permits the requested child policy.
    pub const fn permits(self, child: Self) -> bool {
        matches!(
            (self, child),
            (Self::Yolo, _) | (Self::Prompt, Self::Prompt)
        )
    }
}

impl fmt::Display for ThreadApprovalPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.pad(self.as_str())
    }
}

/// Complete immutable authority written before a spawned thread becomes live.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadAuthorityRecord {
    /// Stable daemon-minted thread identifier.
    pub thread_id: String,
    /// Durable parent thread, or none for a locally approved root thread.
    pub parent_thread_id: Option<String>,
    /// Actor whose approval admitted this spawn.
    pub spawning_actor: String,
    /// Canonical working directory bounding workspace access.
    pub cwd: String,
    /// Exact model requested for the thread.
    pub model: String,
    /// Exact provider reasoning effort requested for the thread.
    pub reasoning_effort: ReasoningEffort,
    /// Immutable startup approval policy.
    pub approval_policy: ThreadApprovalPolicy,
    /// Authority creation time in Unix milliseconds.
    pub created_at_ms: u64,
}

/// Transient daemon state joined to a durable thread authority record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadLiveState {
    /// Whether this daemon process currently has the thread loaded.
    pub loaded: bool,
    /// Active turn identifier, or none while the loaded thread is idle.
    pub current_turn_id: Option<String>,
}

/// One immutable thread authority record joined with current daemon state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadStatus {
    /// Durable authority facts.
    pub authority: ThreadAuthorityRecord,
    /// Transient state queried from the serving daemon.
    pub live: ThreadLiveState,
}

/// Typed decision resolving a prompting `thread.spawn` request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "decision")]
pub enum ThreadSpawnDecision {
    /// Grant the pending spawn.
    Grant {
        /// Actor granting the effect.
        actor: String,
    },
    /// Deny the pending spawn.
    Deny {
        /// Actor denying the effect.
        actor: String,
        /// Human-readable denial reason.
        reason: String,
    },
    /// Cancel the pending spawn without granting authority.
    Cancel {
        /// Actor canceling the prompt.
        actor: String,
    },
}

/// Parameters for starting or resolving a `thread.spawn` effect.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "action")]
pub enum ThreadSpawnParams {
    /// Start one spawn admission.
    Start {
        /// Parent thread, or none for a locally approved root thread.
        parent_thread_id: Option<String>,
        /// Requested working directory.
        cwd: String,
        /// Requested model.
        model: String,
        /// Requested reasoning effort.
        reasoning_effort: ReasoningEffort,
        /// Requested immutable approval policy.
        approval_policy: ThreadApprovalPolicy,
    },
    /// Resolve a spawn waiting for explicit approval.
    Decide {
        /// Daemon-minted pending spawn identifier.
        spawn_id: String,
        /// Grant, deny, or cancel decision with its exact actor.
        approval: ThreadSpawnDecision,
    },
}

/// Typed outcome returned by `thread.spawn`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum ThreadSpawnResult {
    /// The spawning policy requires an explicit decision.
    ApprovalRequired {
        /// Daemon-minted pending spawn identifier.
        spawn_id: String,
        /// Thread identifier reserved by this pending spawn.
        thread_id: String,
        /// Typed effect evaluated for spawn admission.
        effect: EffectClass,
        /// Policy reason presented to the approving actor.
        reason: String,
    },
    /// Authority is durable and the thread is now loaded.
    Spawned {
        /// Complete durable and live readback.
        thread: ThreadStatus,
    },
    /// Approval was durably denied and no authority record exists.
    Denied {
        /// Resolved pending spawn identifier.
        spawn_id: String,
        /// Thread identifier that was not admitted.
        thread_id: String,
        /// Actor denying the spawn.
        actor: String,
        /// Durable denial reason.
        reason: String,
    },
    /// Approval was durably canceled and no authority record exists.
    Canceled {
        /// Resolved pending spawn identifier.
        spawn_id: String,
        /// Thread identifier that was not admitted.
        thread_id: String,
        /// Actor canceling the spawn.
        actor: String,
    },
}

/// Result returned by `thread.list`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadListResult {
    /// Every durable thread in the selected authority ledger.
    pub threads: Vec<ThreadStatus>,
}

/// Parameters for one `thread.status` readback.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadStatusParams {
    /// Thread to read.
    pub thread_id: String,
}

/// Result returned by `thread.status`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadStatusResult {
    /// Complete durable and live readback.
    pub thread: ThreadStatus,
}

/// Parameters for starting or steering one daemon-owned thread turn.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadSendParams {
    /// Durable thread receiving the message.
    pub thread_id: String,
    /// Live controller identity claiming or retaining this turn.
    pub controller_id: String,
    /// Exact active turn expected for a steer, or none when starting from idle.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    /// User text to submit to the thread.
    pub message: String,
}

/// Stable reason an otherwise well-formed thread send was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadSendRejectedReason {
    /// A different controller owns the active turn.
    ControllerOwned,
    /// The supplied turn expectation does not match current live state.
    TurnMismatch,
    /// The bounded continuation queue cannot accept another steer.
    QueueFull,
}

/// Typed receipt returned by `thread.send`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum ThreadSendResult {
    /// An idle thread accepted a new external turn.
    Started {
        /// Exact durable thread receiving the message.
        thread_id: String,
        /// Daemon-minted external turn identifier.
        turn_id: String,
    },
    /// The owning controller atomically queued a continuation.
    Steered {
        /// Exact durable thread receiving the message.
        thread_id: String,
        /// Exact external turn retained by the steer.
        turn_id: String,
    },
    /// Live controller or turn arbitration rejected the send without mutation.
    Rejected {
        /// Exact durable thread targeted by the send.
        thread_id: String,
        /// Current active turn, or none while the thread is idle.
        turn_id: Option<String>,
        /// Stable rejection reason.
        reason: ThreadSendRejectedReason,
    },
}

/// Parameters for reading retained events from one live thread.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadEventsParams {
    /// Durable thread whose live events should be observed.
    pub thread_id: String,
    /// First thread-local offset, or the current tip when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_offset: Option<u64>,
    /// Maximum number of events to return.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Bounded long-poll wait when no event is immediately available.
    #[serde(default)]
    pub wait_ms: Option<u64>,
}

/// One retained thread event paired with its thread-local offset and turn.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BufferedThreadEvent {
    /// Contiguous thread-local event offset.
    pub offset: u64,
    /// External thread turn that owns this event.
    pub turn_id: String,
    /// Existing typed or forward-compatible daemon run event.
    pub event: StreamEvent,
}

/// Retained event page returned by `thread.events`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadEventsResult {
    /// Exact durable thread whose events were read.
    pub thread_id: String,
    /// First requested thread-local offset.
    pub from_offset: u64,
    /// Offset to use for the next page.
    pub next_offset: u64,
    /// Current external turn, or none after controller release.
    pub current_turn_id: Option<String>,
    /// Events in contiguous thread-local order.
    pub events: Vec<BufferedThreadEvent>,
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
    /// Permit this `shell.exec` call and later calls in the same daemon session.
    GrantSession,
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
    /// Exact first user question in the session.
    #[serde(default)]
    pub first_question: String,
    /// Session update time from the ledger, in Unix milliseconds.
    #[serde(default)]
    pub updated_at_ms: u64,
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

/// Latest requested-or-responded model identity state for one durable run.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum ModelIdentityStatus {
    /// The host-selected model has been requested and no later response is recorded.
    Requested {
        /// Host-selected model or alias sent to the provider.
        model: String,
    },
    /// A model response is durable, with optional provider-reported identity.
    Responded {
        /// Provider-reported served model, or unknown when omitted.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        served_model: Option<String>,
    },
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
    /// Latest model identity state reconstructed from the durable ledger.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_status: Option<ModelIdentityStatus>,
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
                "daemon.status",
                "daemon.shutdown_if_idle",
                "thread.spawn",
                "thread.list",
                "thread.status",
                "thread.send",
                "thread.events",
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
                ERROR_THREAD_AUTHORITY_EXCEEDED,
                ERROR_THREAD_EVENTS_FAILED,
                ERROR_THREAD_LIST_FAILED,
                ERROR_THREAD_SPAWN_FAILED,
                ERROR_THREAD_SEND_FAILED,
                ERROR_THREAD_STATUS_FAILED,
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
                "thread_authority_exceeded",
                "thread_events_failed",
                "thread_list_failed",
                "thread_spawn_failed",
                "thread_send_failed",
                "thread_status_failed",
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
    fn thread_approval_policy_keeps_exact_literals_and_never_expands() {
        for (wire, policy) in [
            ("prompt", ThreadApprovalPolicy::Prompt),
            ("yolo", ThreadApprovalPolicy::Yolo),
        ] {
            assert_eq!(ThreadApprovalPolicy::parse(wire), Some(policy));
            assert_eq!(policy.as_str(), wire);
            assert_eq!(policy.to_string(), wire);
            assert_eq!(serde_json::to_value(policy).unwrap(), json!(wire));
        }
        assert_eq!(ThreadApprovalPolicy::parse("auto"), None);
        assert!(ThreadApprovalPolicy::Prompt.permits(ThreadApprovalPolicy::Prompt));
        assert!(!ThreadApprovalPolicy::Prompt.permits(ThreadApprovalPolicy::Yolo));
        assert!(ThreadApprovalPolicy::Yolo.permits(ThreadApprovalPolicy::Prompt));
        assert!(ThreadApprovalPolicy::Yolo.permits(ThreadApprovalPolicy::Yolo));
    }

    #[test]
    fn thread_management_fixtures_keep_exact_v1_bytes() {
        const SPAWN_START_REQUEST: &str = r#"{"v":1,"id":"spawn_start_1","kind":"request","method":"thread.spawn","params":{"action":"start","approval_policy":"prompt","cwd":"/tmp/work","model":"gpt-5.6-sol","parent_thread_id":"thread_parent","reasoning_effort":"xhigh"}}"#;
        const SPAWN_DECIDE_REQUEST: &str = r#"{"v":1,"id":"spawn_decide_1","kind":"request","method":"thread.spawn","params":{"action":"decide","approval":{"actor":"stdin","decision":"grant"},"spawn_id":"spawn_1"}}"#;
        const SPAWN_REQUIRED_RESPONSE: &str = r#"{"v":1,"id":"spawn_start_1","kind":"response","method":"thread.spawn","result":{"effect":"workspace_write","reason":"thread.spawn requires approval","spawn_id":"spawn_1","status":"approval_required","thread_id":"thread_1"}}"#;
        const STATUS_RESPONSE: &str = r#"{"v":1,"id":"status_1","kind":"response","method":"thread.status","result":{"thread":{"authority":{"approval_policy":"prompt","created_at_ms":42,"cwd":"/tmp/work","model":"gpt-5.6-sol","parent_thread_id":"thread_parent","reasoning_effort":"xhigh","spawning_actor":"stdin","thread_id":"thread_1"},"live":{"current_turn_id":null,"loaded":true}}}}"#;
        const LIST_RESPONSE: &str = r#"{"v":1,"id":"list_1","kind":"response","method":"thread.list","result":{"threads":[{"authority":{"approval_policy":"prompt","created_at_ms":42,"cwd":"/tmp/work","model":"gpt-5.6-sol","parent_thread_id":"thread_parent","reasoning_effort":"xhigh","spawning_actor":"stdin","thread_id":"thread_1"},"live":{"current_turn_id":null,"loaded":false}}]}}"#;
        const SEND_START_REQUEST: &str = r#"{"v":1,"id":"send_1","kind":"request","method":"thread.send","params":{"controller_id":"terminal_a","message":"inspect it","thread_id":"thread_1"}}"#;
        const SEND_STEER_REQUEST: &str = r#"{"v":1,"id":"send_2","kind":"request","method":"thread.send","params":{"controller_id":"terminal_a","message":"also summarize","thread_id":"thread_1","turn_id":"thread_turn_1"}}"#;
        const SEND_STARTED_RESPONSE: &str = r#"{"v":1,"id":"send_1","kind":"response","method":"thread.send","result":{"status":"started","thread_id":"thread_1","turn_id":"thread_turn_1"}}"#;
        const SEND_STEERED_RESPONSE: &str = r#"{"v":1,"id":"send_2","kind":"response","method":"thread.send","result":{"status":"steered","thread_id":"thread_1","turn_id":"thread_turn_1"}}"#;
        const SEND_REJECTED_RESPONSE: &str = r#"{"v":1,"id":"send_3","kind":"response","method":"thread.send","result":{"reason":"controller_owned","status":"rejected","thread_id":"thread_1","turn_id":"thread_turn_1"}}"#;
        const EVENTS_REQUEST: &str = r#"{"v":1,"id":"events_1","kind":"request","method":"thread.events","params":{"from_offset":0,"limit":128,"thread_id":"thread_1","wait_ms":1000}}"#;
        const EVENTS_RESPONSE: &str = r#"{"v":1,"id":"events_1","kind":"response","method":"thread.events","result":{"current_turn_id":"thread_turn_1","events":[],"from_offset":0,"next_offset":0,"thread_id":"thread_1"}}"#;

        for fixture in [SPAWN_START_REQUEST, SPAWN_DECIDE_REQUEST] {
            let request = decode_request(fixture).unwrap();
            let params =
                serde_json::from_value::<ThreadSpawnParams>(request.params.clone().unwrap())
                    .unwrap();
            assert_eq!(serde_json::to_string(&request).unwrap(), fixture);
            assert!(matches!(
                params,
                ThreadSpawnParams::Start { .. } | ThreadSpawnParams::Decide { .. }
            ));
        }

        let approval_required = Envelope::response(
            Some("spawn_start_1".into()),
            Some("thread.spawn".into()),
            serde_json::to_value(ThreadSpawnResult::ApprovalRequired {
                spawn_id: "spawn_1".into(),
                thread_id: "thread_1".into(),
                effect: EffectClass::WorkspaceWrite,
                reason: "thread.spawn requires approval".into(),
            })
            .unwrap(),
        );
        assert_eq!(
            serde_json::to_string(&approval_required).unwrap(),
            SPAWN_REQUIRED_RESPONSE
        );

        let thread = ThreadStatus {
            authority: ThreadAuthorityRecord {
                thread_id: "thread_1".into(),
                parent_thread_id: Some("thread_parent".into()),
                spawning_actor: "stdin".into(),
                cwd: "/tmp/work".into(),
                model: "gpt-5.6-sol".into(),
                reasoning_effort: ReasoningEffort::Xhigh,
                approval_policy: ThreadApprovalPolicy::Prompt,
                created_at_ms: 42,
            },
            live: ThreadLiveState {
                loaded: true,
                current_turn_id: None,
            },
        };
        let status = Envelope::response(
            Some("status_1".into()),
            Some("thread.status".into()),
            serde_json::to_value(ThreadStatusResult {
                thread: thread.clone(),
            })
            .unwrap(),
        );
        assert_eq!(serde_json::to_string(&status).unwrap(), STATUS_RESPONSE);

        let mut unloaded = thread;
        unloaded.live.loaded = false;
        let list = Envelope::response(
            Some("list_1".into()),
            Some("thread.list".into()),
            serde_json::to_value(ThreadListResult {
                threads: vec![unloaded],
            })
            .unwrap(),
        );
        assert_eq!(serde_json::to_string(&list).unwrap(), LIST_RESPONSE);

        for fixture in [SEND_START_REQUEST, SEND_STEER_REQUEST] {
            let request = decode_request(fixture).unwrap();
            serde_json::from_value::<ThreadSendParams>(request.params.clone().unwrap()).unwrap();
            assert_eq!(serde_json::to_string(&request).unwrap(), fixture);
        }
        let events_request = decode_request(EVENTS_REQUEST).unwrap();
        serde_json::from_value::<ThreadEventsParams>(events_request.params.clone().unwrap())
            .unwrap();
        assert_eq!(
            serde_json::to_string(&events_request).unwrap(),
            EVENTS_REQUEST
        );

        for (id, result, fixture) in [
            (
                "send_1",
                ThreadSendResult::Started {
                    thread_id: "thread_1".into(),
                    turn_id: "thread_turn_1".into(),
                },
                SEND_STARTED_RESPONSE,
            ),
            (
                "send_2",
                ThreadSendResult::Steered {
                    thread_id: "thread_1".into(),
                    turn_id: "thread_turn_1".into(),
                },
                SEND_STEERED_RESPONSE,
            ),
            (
                "send_3",
                ThreadSendResult::Rejected {
                    thread_id: "thread_1".into(),
                    turn_id: Some("thread_turn_1".into()),
                    reason: ThreadSendRejectedReason::ControllerOwned,
                },
                SEND_REJECTED_RESPONSE,
            ),
        ] {
            let response =
                Envelope::response_from(Some(id.into()), Some("thread.send".into()), result);
            assert_eq!(serde_json::to_string(&response).unwrap(), fixture);
        }
        let events = Envelope::response_from(
            Some("events_1".into()),
            Some("thread.events".into()),
            ThreadEventsResult {
                thread_id: "thread_1".into(),
                from_offset: 0,
                next_offset: 0,
                current_turn_id: Some("thread_turn_1".into()),
                events: Vec::new(),
            },
        );
        assert_eq!(serde_json::to_string(&events).unwrap(), EVENTS_RESPONSE);
    }

    #[test]
    fn thread_request_dtos_reject_unknown_fields() {
        for value in [
            json!({
                "action": "start",
                "parent_thread_id": null,
                "cwd": "/tmp/work",
                "model": "gpt-5.6-sol",
                "reasoning_effort": "xhigh",
                "approval_policy": "prompt",
                "extra": true
            }),
            json!({
                "action": "decide",
                "spawn_id": "spawn_1",
                "approval": {"decision": "grant", "actor": "stdin"},
                "extra": true
            }),
        ] {
            assert!(serde_json::from_value::<ThreadSpawnParams>(value).is_err());
        }
        assert!(
            serde_json::from_value::<ThreadStatusParams>(
                json!({"thread_id": "thread_1", "extra": true})
            )
            .is_err()
        );
        assert_unknown_field_rejected::<ThreadSendParams>(json!({
            "thread_id": "thread_1",
            "controller_id": "terminal_a",
            "message": "hello",
            "future": true
        }));
        assert_unknown_field_rejected::<ThreadEventsParams>(json!({
            "thread_id": "thread_1",
            "future": true
        }));
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
    fn daemon_status_known_and_unknown_fixtures_keep_exact_v1_bytes() {
        const STATUS_REQUEST: &str = r#"{"v":1,"id":"status_1","kind":"request","method":"daemon.status","params":{"config_path":"config/plato.toml","session_id":"session_1"}}"#;
        const STATUS_KNOWN_RESPONSE: &str = r#"{"v":1,"id":"status_1","kind":"response","method":"daemon.status","result":{"daemon":{"build_commit":"0123456789abcdef0123456789abcdef01234567","build_date_utc":"2026-08-01","endpoint_path":"/tmp/agent.sock","package_version":"0.1.0","uptime_ms":42,"workspace_id":"work-1234"},"model":{"key_present":true,"provider_kind":"open_router","requested_alias":"~openai/gpt-latest","served_model":"openai/gpt-5.5-2026-08-01"},"session":{"core_event_count":17,"human_turn_count":2,"latest_run_id":"run_2","ledger_path":"/tmp/agent.db","session_id":"session_1"},"trust":{"approval_denied_count":1,"approval_granted_count":2,"shell_session_grant":true},"usage":{"last_run":{"input_tokens":7,"output_tokens":3,"unknown_response_count":1},"session":{"input_tokens":17,"output_tokens":8,"unknown_response_count":2}}}}"#;
        const STATUS_UNKNOWN_RESPONSE: &str = r#"{"v":1,"id":"status_2","kind":"response","method":"daemon.status","result":{"daemon":{"build_commit":null,"build_date_utc":null,"endpoint_path":"/tmp/agent.sock","package_version":"0.1.0","uptime_ms":0,"workspace_id":"work-1234"},"model":{"key_present":false,"provider_kind":"open_ai","requested_alias":"gpt-5.5","served_model":null},"session":{"core_event_count":0,"human_turn_count":0,"latest_run_id":null,"ledger_path":"/tmp/agent.db","session_id":null},"trust":{"approval_denied_count":0,"approval_granted_count":0,"shell_session_grant":false},"usage":{"last_run":{"input_tokens":0,"output_tokens":0,"unknown_response_count":0},"session":{"input_tokens":0,"output_tokens":0,"unknown_response_count":0}}}}"#;

        let request = decode_request(STATUS_REQUEST).unwrap();
        let params: DaemonStatusParams =
            serde_json::from_value(request.params.clone().unwrap()).unwrap();
        assert_eq!(params.session_id.as_deref(), Some("session_1"));
        assert_eq!(params.config_path.as_deref(), Some("config/plato.toml"));
        assert_eq!(serde_json::to_string(&request).unwrap(), STATUS_REQUEST);

        for fixture in [STATUS_KNOWN_RESPONSE, STATUS_UNKNOWN_RESPONSE] {
            let envelope: Envelope = serde_json::from_str(fixture).unwrap();
            let result: DaemonStatusResult =
                serde_json::from_value(envelope.result.clone().unwrap()).unwrap();
            let rebuilt = Envelope::response_from(envelope.id, envelope.method, result);
            assert_eq!(serde_json::to_string(&rebuilt).unwrap(), fixture);
        }

        let legacy: DaemonStatusTrust =
            serde_json::from_str(r#"{"approval_granted_count":2,"approval_denied_count":1}"#)
                .unwrap();
        assert!(!legacy.shell_session_grant);
    }

    #[test]
    fn daemon_status_provider_kinds_keep_exact_wire_values() {
        for (kind, wire_value) in [
            (DaemonStatusProviderKind::OpenAi, "open_ai"),
            (DaemonStatusProviderKind::OpenRouter, "open_router"),
        ] {
            assert_eq!(kind.as_str(), wire_value);
            assert_eq!(kind.to_string(), wire_value);
            assert_eq!(serde_json::to_value(kind).unwrap(), wire_value);
        }
    }

    #[test]
    fn session_summary_keeps_exact_wire_fields_and_legacy_defaults() {
        const SUMMARY: &str = r#"{"session_id":"session_1","run_id":"run_2","status":"finished","latest_question":"approved, go ahead","first_question":"review the release","updated_at_ms":123456,"ledger_path":"/tmp/agent.db"}"#;
        let summary = SessionSummary {
            session_id: "session_1".into(),
            run_id: "run_2".into(),
            status: RunStateName::Finished,
            latest_question: "approved, go ahead".into(),
            first_question: "review the release".into(),
            updated_at_ms: 123_456,
            ledger_path: "/tmp/agent.db".into(),
        };

        assert_eq!(serde_json::to_string(&summary).unwrap(), SUMMARY);
        assert_eq!(
            serde_json::from_str::<SessionSummary>(SUMMARY).unwrap(),
            summary
        );

        let legacy: SessionSummary = serde_json::from_value(json!({
            "session_id": "session_legacy",
            "run_id": "run_legacy",
            "status": "finished",
            "latest_question": "legacy question",
            "ledger_path": "/tmp/legacy.db"
        }))
        .unwrap();
        assert!(legacy.first_question.is_empty());
        assert_eq!(legacy.updated_at_ms, 0);
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
        assert_unknown_field_rejected::<DaemonStatusParams>(json!({
            "session_id": "session_1",
            "future": true
        }));
        assert_unknown_field_rejected::<DaemonStatusResult>(json!({
            "model": {
                "requested_alias": "gpt-5.5",
                "served_model": null,
                "provider_kind": "open_ai",
                "key_present": false
            },
            "daemon": {
                "package_version": "0.1.0",
                "build_commit": null,
                "build_date_utc": null,
                "uptime_ms": 0,
                "endpoint_path": "/tmp/agent.sock",
                "workspace_id": "work-1234"
            },
            "session": {
                "session_id": null,
                "latest_run_id": null,
                "human_turn_count": 0,
                "ledger_path": "/tmp/agent.db",
                "core_event_count": 0
            },
            "usage": {
                "last_run": {
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "unknown_response_count": 0
                },
                "session": {
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "unknown_response_count": 0
                }
            },
            "trust": {
                "approval_granted_count": 0,
                "approval_denied_count": 0,
                "shell_session_grant": false
            },
            "future": true
        }));
        assert_unknown_field_rejected::<DaemonStatusModel>(json!({
            "requested_alias": "gpt-5.5",
            "served_model": null,
            "provider_kind": "open_ai",
            "key_present": false,
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
            (ApprovalDecision::GrantSession, "grant_session"),
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
        assert!(serde_json::from_value::<ApprovalDecision>(json!("grant_workspace")).is_err());
    }

    #[test]
    fn approval_decide_params_keep_literal_v1_compatible_fixtures() {
        for (fixture, expected) in [
            (
                r#"{"run_id":"run_1","tool_call_id":"call_1","decision":"grant","reason":null}"#,
                ApprovalDecision::Grant,
            ),
            (
                r#"{"run_id":"run_1","tool_call_id":"call_1","decision":"grant_session","reason":null}"#,
                ApprovalDecision::GrantSession,
            ),
            (
                r#"{"run_id":"run_1","tool_call_id":"call_1","decision":"deny","reason":"not now"}"#,
                ApprovalDecision::Deny,
            ),
        ] {
            let params: ApprovalDecideParams = serde_json::from_str(fixture).unwrap();
            assert_eq!(params.decision, expected);
            assert_eq!(serde_json::to_string(&params).unwrap(), fixture);
        }
        for unknown in ["allow", "grant_network", "grant_tool"] {
            assert!(
                serde_json::from_value::<ApprovalDecision>(json!(unknown)).is_err(),
                "accepted unknown approval decision {unknown}"
            );
        }
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
                    model_status: None,
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
    fn model_identity_status_keeps_exact_known_unknown_and_requested_wire_shapes() {
        let fixtures = [
            (
                ModelIdentityStatus::Requested {
                    model: "~openai/gpt-latest".into(),
                },
                json!({
                    "state": "requested",
                    "model": "~openai/gpt-latest"
                }),
            ),
            (
                ModelIdentityStatus::Responded {
                    served_model: Some("openai/gpt-5.2-2026-08-01".into()),
                },
                json!({
                    "state": "responded",
                    "served_model": "openai/gpt-5.2-2026-08-01"
                }),
            ),
            (
                ModelIdentityStatus::Responded { served_model: None },
                json!({"state": "responded"}),
            ),
        ];

        for (status, wire) in fixtures {
            assert_eq!(serde_json::to_value(&status).unwrap(), wire);
            assert_eq!(
                serde_json::from_value::<ModelIdentityStatus>(wire).unwrap(),
                status
            );
        }

        let legacy_run: TypedRun = serde_json::from_value(json!({
            "run_id": "run_1",
            "session_index": 0,
            "status": "finished",
            "entries": []
        }))
        .unwrap();
        assert_eq!(legacy_run.model_status, None);
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
