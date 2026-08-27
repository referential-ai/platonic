//! Version 2 daemon wire types shared by Plato Agent clients and servers.
//!
//! This crate owns serialization and validation for the newline-delimited JSON
//! protocol. It performs no I/O and contains no runtime or application policy.

#![deny(unsafe_code)]
#![warn(missing_docs)]

pub use platonic_core::{AgentId, HarnessEvent, PolicyDecision, ProfileId};
use platonic_core::{EffectClass, RecordedEvent};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer, de::Error as _, ser::SerializeStruct,
};
use serde_json::Value;
use std::fmt;

mod voice;

pub use voice::{VOICE_EVENT_VERSION, VoiceEvent, VoiceEventEnvelope};

/// Provider reasoning effort carried by daemon run overrides.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningEffort {
    /// Disable provider reasoning.
    #[default]
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
pub const PROTOCOL_VERSION: u32 = 2;

/// Maximum JSON payload bytes in one protocol NDJSON line before decode.
pub const MAX_PROTOCOL_LINE_BYTES: usize = 1024 * 1024;

/// Plato Agent package version and optional build provenance embedded at compile time.
///
/// Repository builds that do not provide deploy provenance remain visibly
/// unknown instead of resembling a dated release build.
pub const PLATO_BUILD_IDENTITY: &str = match option_env!("PLATO_BUILD_IDENTITY") {
    Some(identity) => identity,
    None => concat!(env!("CARGO_PKG_VERSION"), " unknown unknown"),
};

/// Platonic product version, independent of workspace crate versions.
pub const PLATONIC_PRODUCT_VERSION: &str = env!("PLATONIC_PRODUCT_VERSION");

/// Exact source commit embedded in the Platonic product build.
pub const PLATONIC_BUILD_COMMIT: &str = env!("PLATONIC_BUILD_COMMIT");

/// UTC build date embedded in the Platonic product build.
pub const PLATONIC_BUILD_DATE: &str = env!("PLATONIC_BUILD_DATE");

/// Platonic product version and provenance as accepted by the server CLI.
pub const PLATONIC_BUILD_IDENTITY: &str = env!("PLATONIC_BUILD_IDENTITY");

/// Locked Platonic diagnostic identity, including the product command name.
pub const PLATONIC_DIAGNOSTIC_IDENTITY: &str = env!("PLATONIC_DIAGNOSTIC_IDENTITY");

/// One capability advertised during the protocol v2 handshake.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum Capability {
    /// Initial daemon handshake.
    #[serde(rename = "hello")]
    Hello,
    /// Start a fresh run.
    #[serde(rename = "run.start")]
    RunStart,
    /// Append a message to a session.
    #[serde(rename = "message.append")]
    MessageAppend,
    /// Start issue preparation.
    #[serde(rename = "issue-prep.start")]
    IssuePrepStart,
    /// Stream buffered run events.
    #[serde(rename = "events.stream")]
    EventsStream,
    /// Commit one complete voice event batch.
    #[serde(rename = "voice.events.commit")]
    VoiceEventsCommit,
    /// Read one committed voice event batch.
    #[serde(rename = "voice.events.read")]
    VoiceEventsRead,
    /// Decide a pending approval.
    #[serde(rename = "approval.decide")]
    ApprovalDecide,
    /// Request run cancellation.
    #[serde(rename = "run.cancel")]
    RunCancel,
    /// List sessions.
    #[serde(rename = "sessions.list")]
    SessionsList,
    /// Read a transcript.
    #[serde(rename = "transcript.read")]
    TranscriptRead,
    /// Read a typed transcript.
    #[serde(rename = "transcript.read.typed")]
    TranscriptReadTyped,
    /// Read a pending approval in a transcript.
    #[serde(rename = "transcript.read.pending_approval")]
    TranscriptReadPendingApproval,
    /// Read authoritative daemon status.
    #[serde(rename = "daemon.status")]
    DaemonStatus,
    /// Set one daemon-lifetime session approval profile.
    #[serde(rename = "session.approval_profile.set")]
    SessionApprovalProfileSet,
    /// Shut down an idle daemon.
    #[serde(rename = "daemon.shutdown_if_idle")]
    DaemonShutdownIfIdle,
    /// Durably create a thread authority record.
    #[serde(rename = "thread.spawn")]
    ThreadSpawn,
    /// List durable threads with live state.
    #[serde(rename = "thread.list")]
    ThreadList,
    /// Read one durable thread with live state.
    #[serde(rename = "thread.status")]
    ThreadStatus,
    /// Read one complete immutable thread authority record.
    #[serde(rename = "thread.authority")]
    ThreadAuthority,
    /// Start or steer one daemon-owned thread turn.
    #[serde(rename = "thread.send")]
    ThreadSend,
    /// Observe retained live thread events.
    #[serde(rename = "thread.events")]
    ThreadEvents,
    /// Stop one durable thread and its active child process.
    #[serde(rename = "thread.stop")]
    ThreadStop,
    /// Resolve or create one profile's home thread.
    #[serde(rename = "profile.open")]
    ProfileOpen,
    /// Register one named workspace.
    #[serde(rename = "workspace.create")]
    WorkspaceCreate,
    /// List every registered workspace.
    #[serde(rename = "workspace.list")]
    WorkspaceList,
    /// Read one registered workspace.
    #[serde(rename = "workspace.status")]
    WorkspaceStatus,
    /// Create one workspace-bound profile.
    #[serde(rename = "profile.create")]
    ProfileCreate,
    /// List workspace-bound profiles.
    #[serde(rename = "profile.list")]
    ProfileList,
    /// Read one workspace-bound profile.
    #[serde(rename = "profile.status")]
    ProfileStatus,
    /// Update one profile's defaults and content revision.
    #[serde(rename = "profile.update")]
    ProfileUpdate,
}

impl Capability {
    /// Returns the exact capability wire value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hello => "hello",
            Self::RunStart => "run.start",
            Self::MessageAppend => "message.append",
            Self::IssuePrepStart => "issue-prep.start",
            Self::EventsStream => "events.stream",
            Self::VoiceEventsCommit => "voice.events.commit",
            Self::VoiceEventsRead => "voice.events.read",
            Self::ApprovalDecide => "approval.decide",
            Self::RunCancel => "run.cancel",
            Self::SessionsList => "sessions.list",
            Self::TranscriptRead => "transcript.read",
            Self::TranscriptReadTyped => "transcript.read.typed",
            Self::TranscriptReadPendingApproval => "transcript.read.pending_approval",
            Self::DaemonStatus => "daemon.status",
            Self::SessionApprovalProfileSet => "session.approval_profile.set",
            Self::DaemonShutdownIfIdle => "daemon.shutdown_if_idle",
            Self::ThreadSpawn => "thread.spawn",
            Self::ThreadList => "thread.list",
            Self::ThreadStatus => "thread.status",
            Self::ThreadAuthority => "thread.authority",
            Self::ThreadSend => "thread.send",
            Self::ThreadEvents => "thread.events",
            Self::ThreadStop => "thread.stop",
            Self::ProfileOpen => "profile.open",
            Self::WorkspaceCreate => "workspace.create",
            Self::WorkspaceList => "workspace.list",
            Self::WorkspaceStatus => "workspace.status",
            Self::ProfileCreate => "profile.create",
            Self::ProfileList => "profile.list",
            Self::ProfileStatus => "profile.status",
            Self::ProfileUpdate => "profile.update",
        }
    }
}

impl fmt::Display for Capability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.pad(self.as_str())
    }
}

/// Capability name for the initial daemon handshake.
pub const CAPABILITY_HELLO: Capability = Capability::Hello;
/// Capability name for starting a fresh run.
pub const CAPABILITY_RUN_START: Capability = Capability::RunStart;
/// Capability name for appending a message to a session.
pub const CAPABILITY_MESSAGE_APPEND: Capability = Capability::MessageAppend;
/// Capability name for starting issue preparation.
pub const CAPABILITY_ISSUE_PREP_START: Capability = Capability::IssuePrepStart;
/// Capability name for streaming buffered run events.
pub const CAPABILITY_EVENTS_STREAM: Capability = Capability::EventsStream;
/// Capability name for committing one complete voice event batch.
pub const CAPABILITY_VOICE_EVENTS_COMMIT: Capability = Capability::VoiceEventsCommit;
/// Capability name for reading one committed voice event batch.
pub const CAPABILITY_VOICE_EVENTS_READ: Capability = Capability::VoiceEventsRead;
/// Capability name for deciding a pending approval.
pub const CAPABILITY_APPROVAL_DECIDE: Capability = Capability::ApprovalDecide;
/// Capability name for requesting run cancellation.
pub const CAPABILITY_RUN_CANCEL: Capability = Capability::RunCancel;
/// Capability name for listing sessions.
pub const CAPABILITY_SESSIONS_LIST: Capability = Capability::SessionsList;
/// Capability name for reading a transcript.
pub const CAPABILITY_TRANSCRIPT_READ: Capability = Capability::TranscriptRead;
/// Capability name for typed transcript readback.
pub const CAPABILITY_TRANSCRIPT_READ_TYPED: Capability = Capability::TranscriptReadTyped;
/// Capability name for pending-approval transcript readback.
pub const CAPABILITY_TRANSCRIPT_READ_PENDING_APPROVAL: Capability =
    Capability::TranscriptReadPendingApproval;
/// Capability name for authoritative daemon status readback.
pub const CAPABILITY_DAEMON_STATUS: Capability = Capability::DaemonStatus;
/// Capability name for setting one daemon-lifetime session approval profile.
pub const CAPABILITY_SESSION_APPROVAL_PROFILE_SET: Capability =
    Capability::SessionApprovalProfileSet;
/// Capability name for shutting down an idle daemon.
pub const CAPABILITY_DAEMON_SHUTDOWN_IF_IDLE: Capability = Capability::DaemonShutdownIfIdle;
/// Capability name for durably creating a thread authority record.
pub const CAPABILITY_THREAD_SPAWN: Capability = Capability::ThreadSpawn;
/// Capability name for listing durable threads with live daemon state.
pub const CAPABILITY_THREAD_LIST: Capability = Capability::ThreadList;
/// Capability name for reading one durable thread with live daemon state.
pub const CAPABILITY_THREAD_STATUS: Capability = Capability::ThreadStatus;
/// Capability name for reading one complete immutable thread authority record.
pub const CAPABILITY_THREAD_AUTHORITY: Capability = Capability::ThreadAuthority;
/// Capability name for starting or steering one daemon-owned thread turn.
pub const CAPABILITY_THREAD_SEND: Capability = Capability::ThreadSend;
/// Capability name for observing retained live thread events.
pub const CAPABILITY_THREAD_EVENTS: Capability = Capability::ThreadEvents;
/// Capability name for stopping one durable thread and its active child process.
pub const CAPABILITY_THREAD_STOP: Capability = Capability::ThreadStop;
/// Capability name for resolving or creating one profile's home thread.
pub const CAPABILITY_PROFILE_OPEN: Capability = Capability::ProfileOpen;
/// Capability name for registering one named workspace.
pub const CAPABILITY_WORKSPACE_CREATE: Capability = Capability::WorkspaceCreate;
/// Capability name for listing every registered workspace.
pub const CAPABILITY_WORKSPACE_LIST: Capability = Capability::WorkspaceList;
/// Capability name for reading one registered workspace.
pub const CAPABILITY_WORKSPACE_STATUS: Capability = Capability::WorkspaceStatus;
/// Capability name for creating one workspace-bound profile.
pub const CAPABILITY_PROFILE_CREATE: Capability = Capability::ProfileCreate;
/// Capability name for listing workspace-bound profiles.
pub const CAPABILITY_PROFILE_LIST: Capability = Capability::ProfileList;
/// Capability name for reading one workspace-bound profile.
pub const CAPABILITY_PROFILE_STATUS: Capability = Capability::ProfileStatus;
/// Capability name for updating one profile's defaults and content revision.
pub const CAPABILITY_PROFILE_UPDATE: Capability = Capability::ProfileUpdate;

/// Capabilities advertised by a protocol v2 daemon, in wire order.
pub const CAPABILITIES: [Capability; 31] = [
    CAPABILITY_HELLO,
    CAPABILITY_RUN_START,
    CAPABILITY_MESSAGE_APPEND,
    CAPABILITY_ISSUE_PREP_START,
    CAPABILITY_EVENTS_STREAM,
    CAPABILITY_VOICE_EVENTS_COMMIT,
    CAPABILITY_VOICE_EVENTS_READ,
    CAPABILITY_APPROVAL_DECIDE,
    CAPABILITY_RUN_CANCEL,
    CAPABILITY_SESSIONS_LIST,
    CAPABILITY_TRANSCRIPT_READ,
    CAPABILITY_TRANSCRIPT_READ_TYPED,
    CAPABILITY_TRANSCRIPT_READ_PENDING_APPROVAL,
    CAPABILITY_DAEMON_STATUS,
    CAPABILITY_SESSION_APPROVAL_PROFILE_SET,
    CAPABILITY_DAEMON_SHUTDOWN_IF_IDLE,
    CAPABILITY_THREAD_SPAWN,
    CAPABILITY_THREAD_LIST,
    CAPABILITY_THREAD_STATUS,
    CAPABILITY_THREAD_AUTHORITY,
    CAPABILITY_THREAD_SEND,
    CAPABILITY_THREAD_EVENTS,
    CAPABILITY_THREAD_STOP,
    CAPABILITY_PROFILE_OPEN,
    CAPABILITY_WORKSPACE_CREATE,
    CAPABILITY_WORKSPACE_LIST,
    CAPABILITY_WORKSPACE_STATUS,
    CAPABILITY_PROFILE_CREATE,
    CAPABILITY_PROFILE_LIST,
    CAPABILITY_PROFILE_STATUS,
    CAPABILITY_PROFILE_UPDATE,
];

/// Stable machine-readable protocol error code.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolErrorCode {
    /// Daemon shutdown has begun.
    DaemonShuttingDown,
    /// The request envelope or parameters are invalid.
    MalformedRequest,
    /// A requested event offset is no longer retained.
    Lagged,
    /// An unexpected daemon failure occurred.
    InternalError,
    /// Issue preparation failed.
    IssuePrepFailed,
    /// The requested resource does not exist.
    NotFound,
    /// The daemon cannot admit more work.
    Overload,
    /// A run failed.
    RunFailed,
    /// A different immutable voice event batch was already committed.
    VoiceEventsConflict,
    /// Sessions could not be listed.
    SessionsListFailed,
    /// Requested thread authority exceeds its parent.
    ThreadAuthorityExceeded,
    /// A repository branch already has a live thread claimant.
    ThreadBranchClaimConflict,
    /// Server policy requires confinement that this host cannot provide.
    ThreadConfinementUnavailable,
    /// One complete thread authority could not be read.
    ThreadAuthorityFailed,
    /// Live thread event observation failed.
    ThreadEventsFailed,
    /// Durable thread enumeration failed.
    ThreadListFailed,
    /// Thread spawn admission or persistence failed.
    ThreadSpawnFailed,
    /// An admitted thread send could not start its run.
    ThreadSendFailed,
    /// One thread status could not be read.
    ThreadStatusFailed,
    /// A thread could not be stopped and recorded.
    ThreadStopFailed,
    /// A profile-home proposal conflicts with an existing reservation or home.
    ProfileOpenConflict,
    /// A profile home could not be resolved or persisted.
    ProfileOpenFailed,
    /// The method is not supported.
    UnsupportedMethod,
    /// The protocol version is not supported.
    UnsupportedVersion,
    /// Client and daemon workspaces differ.
    WorkspaceMismatch,
    /// A directory has not been registered as a workspace.
    WorkspaceUnregistered,
    /// A registered workspace directory has vanished.
    WorkspaceBroken,
}

impl ProtocolErrorCode {
    /// Returns the exact error-code wire value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DaemonShuttingDown => "daemon_shutting_down",
            Self::MalformedRequest => "malformed_request",
            Self::Lagged => "lagged",
            Self::InternalError => "internal_error",
            Self::IssuePrepFailed => "issue_prep_failed",
            Self::NotFound => "not_found",
            Self::Overload => "overload",
            Self::RunFailed => "run_failed",
            Self::VoiceEventsConflict => "voice_events_conflict",
            Self::SessionsListFailed => "sessions_list_failed",
            Self::ThreadAuthorityExceeded => "thread_authority_exceeded",
            Self::ThreadBranchClaimConflict => "thread_branch_claim_conflict",
            Self::ThreadConfinementUnavailable => "thread_confinement_unavailable",
            Self::ThreadAuthorityFailed => "thread_authority_failed",
            Self::ThreadEventsFailed => "thread_events_failed",
            Self::ThreadListFailed => "thread_list_failed",
            Self::ThreadSpawnFailed => "thread_spawn_failed",
            Self::ThreadSendFailed => "thread_send_failed",
            Self::ThreadStatusFailed => "thread_status_failed",
            Self::ThreadStopFailed => "thread_stop_failed",
            Self::ProfileOpenConflict => "profile_open_conflict",
            Self::ProfileOpenFailed => "profile_open_failed",
            Self::UnsupportedMethod => "unsupported_method",
            Self::UnsupportedVersion => "unsupported_version",
            Self::WorkspaceMismatch => "workspace_mismatch",
            Self::WorkspaceUnregistered => "workspace_unregistered",
            Self::WorkspaceBroken => "workspace_broken",
        }
    }
}

impl fmt::Display for ProtocolErrorCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.pad(self.as_str())
    }
}

/// Error code returned once daemon shutdown has begun.
pub const ERROR_DAEMON_SHUTTING_DOWN: ProtocolErrorCode = ProtocolErrorCode::DaemonShuttingDown;
/// Error code returned for an invalid request envelope or parameters.
pub const ERROR_MALFORMED_REQUEST: ProtocolErrorCode = ProtocolErrorCode::MalformedRequest;
/// Error code returned when a requested event offset is no longer retained.
pub const ERROR_LAGGED: ProtocolErrorCode = ProtocolErrorCode::Lagged;
/// Error code returned for an unexpected daemon failure.
pub const ERROR_INTERNAL: ProtocolErrorCode = ProtocolErrorCode::InternalError;
/// Error code returned when issue preparation fails.
pub const ERROR_ISSUE_PREP_FAILED: ProtocolErrorCode = ProtocolErrorCode::IssuePrepFailed;
/// Error code returned when the requested resource does not exist.
pub const ERROR_NOT_FOUND: ProtocolErrorCode = ProtocolErrorCode::NotFound;
/// Error code returned when the daemon cannot admit more work.
pub const ERROR_OVERLOAD: ProtocolErrorCode = ProtocolErrorCode::Overload;
/// Error code returned when a run fails.
pub const ERROR_RUN_FAILED: ProtocolErrorCode = ProtocolErrorCode::RunFailed;
/// Error code returned when a different immutable voice event batch already exists.
pub const ERROR_VOICE_EVENTS_CONFLICT: ProtocolErrorCode = ProtocolErrorCode::VoiceEventsConflict;
/// Error code returned when sessions cannot be listed.
pub const ERROR_SESSIONS_LIST_FAILED: ProtocolErrorCode = ProtocolErrorCode::SessionsListFailed;
/// Error code returned when requested thread authority exceeds its parent.
pub const ERROR_THREAD_AUTHORITY_EXCEEDED: ProtocolErrorCode =
    ProtocolErrorCode::ThreadAuthorityExceeded;
/// Error code returned when a repository branch already has a live claimant.
pub const ERROR_THREAD_BRANCH_CLAIM_CONFLICT: ProtocolErrorCode =
    ProtocolErrorCode::ThreadBranchClaimConflict;
/// Error code returned when required thread confinement is unavailable.
pub const ERROR_THREAD_CONFINEMENT_UNAVAILABLE: ProtocolErrorCode =
    ProtocolErrorCode::ThreadConfinementUnavailable;
/// Error code returned when one complete thread authority cannot be read.
pub const ERROR_THREAD_AUTHORITY_FAILED: ProtocolErrorCode =
    ProtocolErrorCode::ThreadAuthorityFailed;
/// Error code returned when live thread event observation fails.
pub const ERROR_THREAD_EVENTS_FAILED: ProtocolErrorCode = ProtocolErrorCode::ThreadEventsFailed;
/// Error code returned when durable thread enumeration fails.
pub const ERROR_THREAD_LIST_FAILED: ProtocolErrorCode = ProtocolErrorCode::ThreadListFailed;
/// Error code returned when thread spawn admission or persistence fails.
pub const ERROR_THREAD_SPAWN_FAILED: ProtocolErrorCode = ProtocolErrorCode::ThreadSpawnFailed;
/// Error code returned when an admitted thread send cannot start its run.
pub const ERROR_THREAD_SEND_FAILED: ProtocolErrorCode = ProtocolErrorCode::ThreadSendFailed;
/// Error code returned when one thread status cannot be read.
pub const ERROR_THREAD_STATUS_FAILED: ProtocolErrorCode = ProtocolErrorCode::ThreadStatusFailed;
/// Error code returned when a thread cannot be stopped and recorded.
pub const ERROR_THREAD_STOP_FAILED: ProtocolErrorCode = ProtocolErrorCode::ThreadStopFailed;
/// Error code returned when a profile-home proposal conflicts with durable state.
pub const ERROR_PROFILE_OPEN_CONFLICT: ProtocolErrorCode = ProtocolErrorCode::ProfileOpenConflict;
/// Error code returned when a profile home cannot be resolved or persisted.
pub const ERROR_PROFILE_OPEN_FAILED: ProtocolErrorCode = ProtocolErrorCode::ProfileOpenFailed;
/// Error code returned for an unknown method.
pub const ERROR_UNSUPPORTED_METHOD: ProtocolErrorCode = ProtocolErrorCode::UnsupportedMethod;
/// Error code returned for an unsupported protocol version.
pub const ERROR_UNSUPPORTED_VERSION: ProtocolErrorCode = ProtocolErrorCode::UnsupportedVersion;
/// Error code returned when client and daemon workspaces differ.
pub const ERROR_WORKSPACE_MISMATCH: ProtocolErrorCode = ProtocolErrorCode::WorkspaceMismatch;
/// Error code returned when a directory has not been registered as a workspace.
pub const ERROR_WORKSPACE_UNREGISTERED: ProtocolErrorCode =
    ProtocolErrorCode::WorkspaceUnregistered;
/// Error code returned when a registered workspace directory has vanished.
pub const ERROR_WORKSPACE_BROKEN: ProtocolErrorCode = ProtocolErrorCode::WorkspaceBroken;

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
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
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

/// Closed protocol v2 method set shared by requests and responses.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum ProtocolMethod {
    /// Initial daemon handshake.
    #[serde(rename = "hello")]
    Hello,
    /// Start a fresh run.
    #[serde(rename = "run.start")]
    RunStart,
    /// Append a message to a session.
    #[serde(rename = "message.append")]
    MessageAppend,
    /// Start issue preparation.
    #[serde(rename = "issue-prep.start")]
    IssuePrepStart,
    /// Stream buffered run events.
    #[serde(rename = "events.stream")]
    EventsStream,
    /// Commit one complete voice event batch.
    #[serde(rename = "voice.events.commit")]
    VoiceEventsCommit,
    /// Read one committed voice event batch.
    #[serde(rename = "voice.events.read")]
    VoiceEventsRead,
    /// Decide a pending approval.
    #[serde(rename = "approval.decide")]
    ApprovalDecide,
    /// Request run cancellation.
    #[serde(rename = "run.cancel")]
    RunCancel,
    /// List sessions.
    #[serde(rename = "sessions.list")]
    SessionsList,
    /// Read a transcript.
    #[serde(rename = "transcript.read")]
    TranscriptRead,
    /// Read authoritative daemon status.
    #[serde(rename = "daemon.status")]
    DaemonStatus,
    /// Set one daemon-lifetime session approval profile.
    #[serde(rename = "session.approval_profile.set")]
    SessionApprovalProfileSet,
    /// Shut down an idle daemon.
    #[serde(rename = "daemon.shutdown_if_idle")]
    DaemonShutdownIfIdle,
    /// Durably create a thread authority record.
    #[serde(rename = "thread.spawn")]
    ThreadSpawn,
    /// List durable threads with live state.
    #[serde(rename = "thread.list")]
    ThreadList,
    /// Read one durable thread with live state.
    #[serde(rename = "thread.status")]
    ThreadStatus,
    /// Read one complete immutable thread authority record.
    #[serde(rename = "thread.authority")]
    ThreadAuthority,
    /// Start or steer one daemon-owned thread turn.
    #[serde(rename = "thread.send")]
    ThreadSend,
    /// Observe retained live thread events.
    #[serde(rename = "thread.events")]
    ThreadEvents,
    /// Stop one durable thread and its active child process.
    #[serde(rename = "thread.stop")]
    ThreadStop,
    /// Resolve or create one profile's home thread.
    #[serde(rename = "profile.open")]
    ProfileOpen,
    /// Register one named workspace.
    #[serde(rename = "workspace.create")]
    WorkspaceCreate,
    /// List every registered workspace.
    #[serde(rename = "workspace.list")]
    WorkspaceList,
    /// Read one registered workspace.
    #[serde(rename = "workspace.status")]
    WorkspaceStatus,
    /// Create one workspace-bound profile.
    #[serde(rename = "profile.create")]
    ProfileCreate,
    /// List workspace-bound profiles.
    #[serde(rename = "profile.list")]
    ProfileList,
    /// Read one workspace-bound profile.
    #[serde(rename = "profile.status")]
    ProfileStatus,
    /// Update one profile's defaults and content revision.
    #[serde(rename = "profile.update")]
    ProfileUpdate,
}

impl ProtocolMethod {
    /// Returns the exact method wire value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hello => "hello",
            Self::RunStart => "run.start",
            Self::MessageAppend => "message.append",
            Self::IssuePrepStart => "issue-prep.start",
            Self::EventsStream => "events.stream",
            Self::VoiceEventsCommit => "voice.events.commit",
            Self::VoiceEventsRead => "voice.events.read",
            Self::ApprovalDecide => "approval.decide",
            Self::RunCancel => "run.cancel",
            Self::SessionsList => "sessions.list",
            Self::TranscriptRead => "transcript.read",
            Self::DaemonStatus => "daemon.status",
            Self::SessionApprovalProfileSet => "session.approval_profile.set",
            Self::DaemonShutdownIfIdle => "daemon.shutdown_if_idle",
            Self::ThreadSpawn => "thread.spawn",
            Self::ThreadList => "thread.list",
            Self::ThreadStatus => "thread.status",
            Self::ThreadAuthority => "thread.authority",
            Self::ThreadSend => "thread.send",
            Self::ThreadEvents => "thread.events",
            Self::ThreadStop => "thread.stop",
            Self::ProfileOpen => "profile.open",
            Self::WorkspaceCreate => "workspace.create",
            Self::WorkspaceList => "workspace.list",
            Self::WorkspaceStatus => "workspace.status",
            Self::ProfileCreate => "profile.create",
            Self::ProfileList => "profile.list",
            Self::ProfileStatus => "profile.status",
            Self::ProfileUpdate => "profile.update",
        }
    }
}

impl fmt::Display for ProtocolMethod {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.pad(self.as_str())
    }
}

impl std::ops::Deref for ProtocolMethod {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl From<&str> for ProtocolMethod {
    fn from(value: &str) -> Self {
        Self::parse(value).expect("known protocol method")
    }
}

impl From<String> for ProtocolMethod {
    fn from(value: String) -> Self {
        Self::from(value.as_str())
    }
}

impl ProtocolMethod {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "hello" => Some(Self::Hello),
            "run.start" => Some(Self::RunStart),
            "message.append" => Some(Self::MessageAppend),
            "issue-prep.start" => Some(Self::IssuePrepStart),
            "events.stream" => Some(Self::EventsStream),
            "voice.events.commit" => Some(Self::VoiceEventsCommit),
            "voice.events.read" => Some(Self::VoiceEventsRead),
            "approval.decide" => Some(Self::ApprovalDecide),
            "run.cancel" => Some(Self::RunCancel),
            "sessions.list" => Some(Self::SessionsList),
            "transcript.read" => Some(Self::TranscriptRead),
            "daemon.status" => Some(Self::DaemonStatus),
            "session.approval_profile.set" => Some(Self::SessionApprovalProfileSet),
            "daemon.shutdown_if_idle" => Some(Self::DaemonShutdownIfIdle),
            "thread.spawn" => Some(Self::ThreadSpawn),
            "thread.list" => Some(Self::ThreadList),
            "thread.status" => Some(Self::ThreadStatus),
            "thread.authority" => Some(Self::ThreadAuthority),
            "thread.send" => Some(Self::ThreadSend),
            "thread.events" => Some(Self::ThreadEvents),
            "thread.stop" => Some(Self::ThreadStop),
            "profile.open" => Some(Self::ProfileOpen),
            "workspace.create" => Some(Self::WorkspaceCreate),
            "workspace.list" => Some(Self::WorkspaceList),
            "workspace.status" => Some(Self::WorkspaceStatus),
            "profile.create" => Some(Self::ProfileCreate),
            "profile.list" => Some(Self::ProfileList),
            "profile.status" => Some(Self::ProfileStatus),
            "profile.update" => Some(Self::ProfileUpdate),
            _ => None,
        }
    }
}

/// Closed protocol v2 request set with method-specific parameters.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "method", content = "params")]
pub enum ProtocolRequest {
    /// Initial daemon handshake.
    #[serde(rename = "hello")]
    Hello(HelloParams),
    /// Start a fresh run.
    #[serde(rename = "run.start")]
    RunStart(RunStartParams),
    /// Append a message to a session.
    #[serde(rename = "message.append")]
    MessageAppend(MessageAppendParams),
    /// Start issue preparation.
    #[serde(rename = "issue-prep.start")]
    IssuePrepStart(IssuePrepStartParams),
    /// Stream buffered run events.
    #[serde(rename = "events.stream")]
    EventsStream(EventsStreamParams),
    /// Commit one complete voice event batch.
    #[serde(rename = "voice.events.commit")]
    VoiceEventsCommit(VoiceEventsCommitParams),
    /// Read one committed voice event batch.
    #[serde(rename = "voice.events.read")]
    VoiceEventsRead(VoiceEventsReadParams),
    /// Decide a pending approval.
    #[serde(rename = "approval.decide")]
    ApprovalDecide(ApprovalDecideParams),
    /// Request run cancellation.
    #[serde(rename = "run.cancel")]
    RunCancel(RunCancelParams),
    /// List sessions.
    #[serde(rename = "sessions.list")]
    SessionsList,
    /// Read a transcript.
    #[serde(rename = "transcript.read")]
    TranscriptRead(TranscriptReadParams),
    /// Read authoritative daemon status.
    #[serde(rename = "daemon.status")]
    DaemonStatus(DaemonStatusParams),
    /// Set one daemon-lifetime session approval profile.
    #[serde(rename = "session.approval_profile.set")]
    SessionApprovalProfileSet(SessionApprovalProfileSetParams),
    /// Shut down an idle daemon.
    #[serde(rename = "daemon.shutdown_if_idle")]
    DaemonShutdownIfIdle,
    /// Durably create a thread authority record.
    #[serde(rename = "thread.spawn")]
    ThreadSpawn(ThreadSpawnParams),
    /// List durable threads with live state.
    #[serde(rename = "thread.list")]
    ThreadList,
    /// Read one durable thread with live state.
    #[serde(rename = "thread.status")]
    ThreadStatus(ThreadStatusParams),
    /// Read one complete immutable thread authority record.
    #[serde(rename = "thread.authority")]
    ThreadAuthority(ThreadAuthorityParams),
    /// Start or steer one daemon-owned thread turn.
    #[serde(rename = "thread.send")]
    ThreadSend(ThreadSendParams),
    /// Observe retained live thread events.
    #[serde(rename = "thread.events")]
    ThreadEvents(ThreadEventsParams),
    /// Stop one durable thread and its active child process.
    #[serde(rename = "thread.stop")]
    ThreadStop(ThreadStopParams),
    /// Resolve or create one profile's home thread.
    #[serde(rename = "profile.open")]
    ProfileOpen(ProfileOpenParams),
    /// Register one named workspace.
    #[serde(rename = "workspace.create")]
    WorkspaceCreate(WorkspaceCreateParams),
    /// List every registered workspace.
    #[serde(rename = "workspace.list")]
    WorkspaceList(WorkspaceListParams),
    /// Read one registered workspace.
    #[serde(rename = "workspace.status")]
    WorkspaceStatus(WorkspaceStatusParams),
    /// Create one workspace-bound profile.
    #[serde(rename = "profile.create")]
    ProfileCreate(ProfileCreateParams),
    /// List workspace-bound profiles.
    #[serde(rename = "profile.list")]
    ProfileList(ProfileListParams),
    /// Read one workspace-bound profile.
    #[serde(rename = "profile.status")]
    ProfileStatus(ProfileStatusParams),
    /// Update one profile's defaults and content revision.
    #[serde(rename = "profile.update")]
    ProfileUpdate(ProfileUpdateParams),
}

impl ProtocolRequest {
    /// Returns this request's closed method discriminator.
    pub const fn method(&self) -> ProtocolMethod {
        match self {
            Self::Hello(_) => ProtocolMethod::Hello,
            Self::RunStart(_) => ProtocolMethod::RunStart,
            Self::MessageAppend(_) => ProtocolMethod::MessageAppend,
            Self::IssuePrepStart(_) => ProtocolMethod::IssuePrepStart,
            Self::EventsStream(_) => ProtocolMethod::EventsStream,
            Self::VoiceEventsCommit(_) => ProtocolMethod::VoiceEventsCommit,
            Self::VoiceEventsRead(_) => ProtocolMethod::VoiceEventsRead,
            Self::ApprovalDecide(_) => ProtocolMethod::ApprovalDecide,
            Self::RunCancel(_) => ProtocolMethod::RunCancel,
            Self::SessionsList => ProtocolMethod::SessionsList,
            Self::TranscriptRead(_) => ProtocolMethod::TranscriptRead,
            Self::DaemonStatus(_) => ProtocolMethod::DaemonStatus,
            Self::SessionApprovalProfileSet(_) => ProtocolMethod::SessionApprovalProfileSet,
            Self::DaemonShutdownIfIdle => ProtocolMethod::DaemonShutdownIfIdle,
            Self::ThreadSpawn(_) => ProtocolMethod::ThreadSpawn,
            Self::ThreadList => ProtocolMethod::ThreadList,
            Self::ThreadStatus(_) => ProtocolMethod::ThreadStatus,
            Self::ThreadAuthority(_) => ProtocolMethod::ThreadAuthority,
            Self::ThreadSend(_) => ProtocolMethod::ThreadSend,
            Self::ThreadEvents(_) => ProtocolMethod::ThreadEvents,
            Self::ThreadStop(_) => ProtocolMethod::ThreadStop,
            Self::ProfileOpen(_) => ProtocolMethod::ProfileOpen,
            Self::WorkspaceCreate(_) => ProtocolMethod::WorkspaceCreate,
            Self::WorkspaceList(_) => ProtocolMethod::WorkspaceList,
            Self::WorkspaceStatus(_) => ProtocolMethod::WorkspaceStatus,
            Self::ProfileCreate(_) => ProtocolMethod::ProfileCreate,
            Self::ProfileList(_) => ProtocolMethod::ProfileList,
            Self::ProfileStatus(_) => ProtocolMethod::ProfileStatus,
            Self::ProfileUpdate(_) => ProtocolMethod::ProfileUpdate,
        }
    }

    fn decode(method: ProtocolMethod, params: Option<Value>) -> serde_json::Result<Self> {
        match method {
            ProtocolMethod::Hello => decode_params(params, method).map(Self::Hello),
            ProtocolMethod::RunStart => decode_params(params, method).map(Self::RunStart),
            ProtocolMethod::MessageAppend => decode_params(params, method).map(Self::MessageAppend),
            ProtocolMethod::IssuePrepStart => {
                decode_params(params, method).map(Self::IssuePrepStart)
            }
            ProtocolMethod::EventsStream => decode_params(params, method).map(Self::EventsStream),
            ProtocolMethod::VoiceEventsCommit => {
                decode_params(params, method).map(Self::VoiceEventsCommit)
            }
            ProtocolMethod::VoiceEventsRead => {
                decode_params(params, method).map(Self::VoiceEventsRead)
            }
            ProtocolMethod::ApprovalDecide => {
                decode_params(params, method).map(Self::ApprovalDecide)
            }
            ProtocolMethod::RunCancel => decode_params(params, method).map(Self::RunCancel),
            ProtocolMethod::SessionsList => {
                decode_empty_params(params, method).map(|()| Self::SessionsList)
            }
            ProtocolMethod::TranscriptRead => {
                decode_params(params, method).map(Self::TranscriptRead)
            }
            ProtocolMethod::DaemonStatus => decode_params(params, method).map(Self::DaemonStatus),
            ProtocolMethod::SessionApprovalProfileSet => {
                decode_params(params, method).map(Self::SessionApprovalProfileSet)
            }
            ProtocolMethod::DaemonShutdownIfIdle => {
                decode_empty_params(params, method).map(|()| Self::DaemonShutdownIfIdle)
            }
            ProtocolMethod::ThreadSpawn => decode_params(params, method).map(Self::ThreadSpawn),
            ProtocolMethod::ThreadList => {
                decode_empty_params(params, method).map(|()| Self::ThreadList)
            }
            ProtocolMethod::ThreadStatus => decode_params(params, method).map(Self::ThreadStatus),
            ProtocolMethod::ThreadAuthority => {
                decode_params(params, method).map(Self::ThreadAuthority)
            }
            ProtocolMethod::ThreadSend => decode_params(params, method).map(Self::ThreadSend),
            ProtocolMethod::ThreadEvents => decode_params(params, method).map(Self::ThreadEvents),
            ProtocolMethod::ThreadStop => decode_params(params, method).map(Self::ThreadStop),
            ProtocolMethod::ProfileOpen => decode_params(params, method).map(Self::ProfileOpen),
            ProtocolMethod::WorkspaceCreate => {
                decode_params(params, method).map(Self::WorkspaceCreate)
            }
            ProtocolMethod::WorkspaceList => decode_params(params, method).map(Self::WorkspaceList),
            ProtocolMethod::WorkspaceStatus => {
                decode_params(params, method).map(Self::WorkspaceStatus)
            }
            ProtocolMethod::ProfileCreate => decode_params(params, method).map(Self::ProfileCreate),
            ProtocolMethod::ProfileList => decode_params(params, method).map(Self::ProfileList),
            ProtocolMethod::ProfileStatus => decode_params(params, method).map(Self::ProfileStatus),
            ProtocolMethod::ProfileUpdate => decode_params(params, method).map(Self::ProfileUpdate),
        }
    }

    fn serialize_params<S>(&self, envelope: &mut S) -> Result<(), S::Error>
    where
        S: SerializeStruct,
    {
        match self {
            Self::Hello(params) => serialize_sorted_field(envelope, "params", params),
            Self::RunStart(params) => serialize_sorted_field(envelope, "params", params),
            Self::MessageAppend(params) => serialize_sorted_field(envelope, "params", params),
            Self::IssuePrepStart(params) => serialize_sorted_field(envelope, "params", params),
            Self::EventsStream(params) => serialize_sorted_field(envelope, "params", params),
            Self::VoiceEventsCommit(params) => serialize_sorted_field(envelope, "params", params),
            Self::VoiceEventsRead(params) => serialize_sorted_field(envelope, "params", params),
            Self::ApprovalDecide(params) => serialize_sorted_field(envelope, "params", params),
            Self::RunCancel(params) => serialize_sorted_field(envelope, "params", params),
            Self::SessionsList => Ok(()),
            Self::TranscriptRead(params) => serialize_sorted_field(envelope, "params", params),
            Self::DaemonStatus(params) => serialize_sorted_field(envelope, "params", params),
            Self::SessionApprovalProfileSet(params) => {
                serialize_sorted_field(envelope, "params", params)
            }
            Self::DaemonShutdownIfIdle => Ok(()),
            Self::ThreadSpawn(params) => serialize_sorted_field(envelope, "params", params),
            Self::ThreadList => Ok(()),
            Self::ThreadStatus(params) => serialize_sorted_field(envelope, "params", params),
            Self::ThreadAuthority(params) => serialize_sorted_field(envelope, "params", params),
            Self::ThreadSend(params) => serialize_sorted_field(envelope, "params", params),
            Self::ThreadEvents(params) => serialize_sorted_field(envelope, "params", params),
            Self::ThreadStop(params) => serialize_sorted_field(envelope, "params", params),
            Self::ProfileOpen(params) => serialize_sorted_field(envelope, "params", params),
            Self::WorkspaceCreate(params) => serialize_sorted_field(envelope, "params", params),
            Self::WorkspaceList(params) => serialize_sorted_field(envelope, "params", params),
            Self::WorkspaceStatus(params) => serialize_sorted_field(envelope, "params", params),
            Self::ProfileCreate(params) => serialize_sorted_field(envelope, "params", params),
            Self::ProfileList(params) => serialize_sorted_field(envelope, "params", params),
            Self::ProfileStatus(params) => serialize_sorted_field(envelope, "params", params),
            Self::ProfileUpdate(params) => serialize_sorted_field(envelope, "params", params),
        }
    }
}

/// Closed protocol v2 response set with method-specific results.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "method", content = "result")]
pub enum ProtocolResponse {
    /// Initial daemon handshake result.
    #[serde(rename = "hello")]
    Hello(HelloResult),
    /// Fresh-run result.
    #[serde(rename = "run.start")]
    RunStart(RunStartResult),
    /// Session-append result.
    #[serde(rename = "message.append")]
    MessageAppend(RunStartResult),
    /// Issue-preparation result.
    #[serde(rename = "issue-prep.start")]
    IssuePrepStart(IssuePrepStartResult),
    /// Buffered run-event page.
    #[serde(rename = "events.stream")]
    EventsStream(EventsStreamResult),
    /// Committed voice event batch.
    #[serde(rename = "voice.events.commit")]
    VoiceEventsCommit(VoiceEventsResult),
    /// Voice event batch readback.
    #[serde(rename = "voice.events.read")]
    VoiceEventsRead(VoiceEventsResult),
    /// Approval-decision receipt.
    #[serde(rename = "approval.decide")]
    ApprovalDecide(CommandAcceptedResult),
    /// Cancellation receipt.
    #[serde(rename = "run.cancel")]
    RunCancel(CommandAcceptedResult),
    /// Session list.
    #[serde(rename = "sessions.list")]
    SessionsList(SessionsListResult),
    /// Transcript readback.
    #[serde(rename = "transcript.read")]
    TranscriptRead(TranscriptReadResult),
    /// Authoritative daemon status.
    #[serde(rename = "daemon.status")]
    DaemonStatus(DaemonStatusResult),
    /// Updated daemon-lifetime session approval profile.
    #[serde(rename = "session.approval_profile.set")]
    SessionApprovalProfileSet(SessionApprovalProfileSetResult),
    /// Idle-shutdown result.
    #[serde(rename = "daemon.shutdown_if_idle")]
    DaemonShutdownIfIdle(ShutdownIfIdleResult),
    /// Thread-spawn result.
    #[serde(rename = "thread.spawn")]
    ThreadSpawn(ThreadSpawnResult),
    /// Durable thread list.
    #[serde(rename = "thread.list")]
    ThreadList(ThreadListResult),
    /// Durable and live thread status.
    #[serde(rename = "thread.status")]
    ThreadStatus(ThreadStatusResult),
    /// Immutable thread authority.
    #[serde(rename = "thread.authority")]
    ThreadAuthority(ThreadAuthorityResult),
    /// Thread-send receipt.
    #[serde(rename = "thread.send")]
    ThreadSend(ThreadSendResult),
    /// Retained thread-event page.
    #[serde(rename = "thread.events")]
    ThreadEvents(ThreadEventsResult),
    /// Thread-stop result.
    #[serde(rename = "thread.stop")]
    ThreadStop(ThreadStopResult),
    /// Profile-home resolution or admission result.
    #[serde(rename = "profile.open")]
    ProfileOpen(ProfileOpenResult),
    /// Workspace-creation result.
    #[serde(rename = "workspace.create")]
    WorkspaceCreate(WorkspaceCreateResult),
    /// Workspace list.
    #[serde(rename = "workspace.list")]
    WorkspaceList(WorkspaceListResult),
    /// Workspace status.
    #[serde(rename = "workspace.status")]
    WorkspaceStatus(WorkspaceStatusResult),
    /// Profile-creation result.
    #[serde(rename = "profile.create")]
    ProfileCreate(ProfileCreateResult),
    /// Profile list.
    #[serde(rename = "profile.list")]
    ProfileList(ProfileListResult),
    /// Profile status.
    #[serde(rename = "profile.status")]
    ProfileStatus(ProfileStatusResult),
    /// Profile-update result.
    #[serde(rename = "profile.update")]
    ProfileUpdate(ProfileUpdateResult),
}

impl ProtocolResponse {
    /// Returns this response's closed method discriminator.
    pub const fn method(&self) -> ProtocolMethod {
        match self {
            Self::Hello(_) => ProtocolMethod::Hello,
            Self::RunStart(_) => ProtocolMethod::RunStart,
            Self::MessageAppend(_) => ProtocolMethod::MessageAppend,
            Self::IssuePrepStart(_) => ProtocolMethod::IssuePrepStart,
            Self::EventsStream(_) => ProtocolMethod::EventsStream,
            Self::VoiceEventsCommit(_) => ProtocolMethod::VoiceEventsCommit,
            Self::VoiceEventsRead(_) => ProtocolMethod::VoiceEventsRead,
            Self::ApprovalDecide(_) => ProtocolMethod::ApprovalDecide,
            Self::RunCancel(_) => ProtocolMethod::RunCancel,
            Self::SessionsList(_) => ProtocolMethod::SessionsList,
            Self::TranscriptRead(_) => ProtocolMethod::TranscriptRead,
            Self::DaemonStatus(_) => ProtocolMethod::DaemonStatus,
            Self::SessionApprovalProfileSet(_) => ProtocolMethod::SessionApprovalProfileSet,
            Self::DaemonShutdownIfIdle(_) => ProtocolMethod::DaemonShutdownIfIdle,
            Self::ThreadSpawn(_) => ProtocolMethod::ThreadSpawn,
            Self::ThreadList(_) => ProtocolMethod::ThreadList,
            Self::ThreadStatus(_) => ProtocolMethod::ThreadStatus,
            Self::ThreadAuthority(_) => ProtocolMethod::ThreadAuthority,
            Self::ThreadSend(_) => ProtocolMethod::ThreadSend,
            Self::ThreadEvents(_) => ProtocolMethod::ThreadEvents,
            Self::ThreadStop(_) => ProtocolMethod::ThreadStop,
            Self::ProfileOpen(_) => ProtocolMethod::ProfileOpen,
            Self::WorkspaceCreate(_) => ProtocolMethod::WorkspaceCreate,
            Self::WorkspaceList(_) => ProtocolMethod::WorkspaceList,
            Self::WorkspaceStatus(_) => ProtocolMethod::WorkspaceStatus,
            Self::ProfileCreate(_) => ProtocolMethod::ProfileCreate,
            Self::ProfileList(_) => ProtocolMethod::ProfileList,
            Self::ProfileStatus(_) => ProtocolMethod::ProfileStatus,
            Self::ProfileUpdate(_) => ProtocolMethod::ProfileUpdate,
        }
    }

    fn decode(method: ProtocolMethod, result: Option<Value>) -> serde_json::Result<Self> {
        match method {
            ProtocolMethod::Hello => decode_result(result, method).map(Self::Hello),
            ProtocolMethod::RunStart => decode_result(result, method).map(Self::RunStart),
            ProtocolMethod::MessageAppend => decode_result(result, method).map(Self::MessageAppend),
            ProtocolMethod::IssuePrepStart => {
                decode_result(result, method).map(Self::IssuePrepStart)
            }
            ProtocolMethod::EventsStream => decode_result(result, method).map(Self::EventsStream),
            ProtocolMethod::VoiceEventsCommit => {
                decode_result(result, method).map(Self::VoiceEventsCommit)
            }
            ProtocolMethod::VoiceEventsRead => {
                decode_result(result, method).map(Self::VoiceEventsRead)
            }
            ProtocolMethod::ApprovalDecide => {
                decode_result(result, method).map(Self::ApprovalDecide)
            }
            ProtocolMethod::RunCancel => decode_result(result, method).map(Self::RunCancel),
            ProtocolMethod::SessionsList => decode_result(result, method).map(Self::SessionsList),
            ProtocolMethod::TranscriptRead => {
                decode_result(result, method).map(Self::TranscriptRead)
            }
            ProtocolMethod::DaemonStatus => decode_result(result, method).map(Self::DaemonStatus),
            ProtocolMethod::SessionApprovalProfileSet => {
                decode_result(result, method).map(Self::SessionApprovalProfileSet)
            }
            ProtocolMethod::DaemonShutdownIfIdle => {
                decode_result(result, method).map(Self::DaemonShutdownIfIdle)
            }
            ProtocolMethod::ThreadSpawn => decode_result(result, method).map(Self::ThreadSpawn),
            ProtocolMethod::ThreadList => decode_result(result, method).map(Self::ThreadList),
            ProtocolMethod::ThreadStatus => decode_result(result, method).map(Self::ThreadStatus),
            ProtocolMethod::ThreadAuthority => {
                decode_result(result, method).map(Self::ThreadAuthority)
            }
            ProtocolMethod::ThreadSend => decode_result(result, method).map(Self::ThreadSend),
            ProtocolMethod::ThreadEvents => decode_result(result, method).map(Self::ThreadEvents),
            ProtocolMethod::ThreadStop => decode_result(result, method).map(Self::ThreadStop),
            ProtocolMethod::ProfileOpen => decode_result(result, method).map(Self::ProfileOpen),
            ProtocolMethod::WorkspaceCreate => {
                decode_result(result, method).map(Self::WorkspaceCreate)
            }
            ProtocolMethod::WorkspaceList => decode_result(result, method).map(Self::WorkspaceList),
            ProtocolMethod::WorkspaceStatus => {
                decode_result(result, method).map(Self::WorkspaceStatus)
            }
            ProtocolMethod::ProfileCreate => decode_result(result, method).map(Self::ProfileCreate),
            ProtocolMethod::ProfileList => decode_result(result, method).map(Self::ProfileList),
            ProtocolMethod::ProfileStatus => decode_result(result, method).map(Self::ProfileStatus),
            ProtocolMethod::ProfileUpdate => decode_result(result, method).map(Self::ProfileUpdate),
        }
    }

    fn serialize_result<S>(&self, envelope: &mut S) -> Result<(), S::Error>
    where
        S: SerializeStruct,
    {
        match self {
            Self::Hello(result) => serialize_sorted_field(envelope, "result", result),
            Self::RunStart(result) => serialize_sorted_field(envelope, "result", result),
            Self::MessageAppend(result) => serialize_sorted_field(envelope, "result", result),
            Self::IssuePrepStart(result) => serialize_sorted_field(envelope, "result", result),
            Self::EventsStream(result) => serialize_sorted_field(envelope, "result", result),
            Self::VoiceEventsCommit(result) => serialize_sorted_field(envelope, "result", result),
            Self::VoiceEventsRead(result) => serialize_sorted_field(envelope, "result", result),
            Self::ApprovalDecide(result) => serialize_sorted_field(envelope, "result", result),
            Self::RunCancel(result) => serialize_sorted_field(envelope, "result", result),
            Self::SessionsList(result) => serialize_sorted_field(envelope, "result", result),
            Self::TranscriptRead(result) => serialize_sorted_field(envelope, "result", result),
            Self::DaemonStatus(result) => serialize_sorted_field(envelope, "result", result),
            Self::SessionApprovalProfileSet(result) => {
                serialize_sorted_field(envelope, "result", result)
            }
            Self::DaemonShutdownIfIdle(result) => {
                serialize_sorted_field(envelope, "result", result)
            }
            Self::ThreadSpawn(result) => serialize_sorted_field(envelope, "result", result),
            Self::ThreadList(result) => serialize_sorted_field(envelope, "result", result),
            Self::ThreadStatus(result) => serialize_sorted_field(envelope, "result", result),
            Self::ThreadAuthority(result) => serialize_sorted_field(envelope, "result", result),
            Self::ThreadSend(result) => serialize_sorted_field(envelope, "result", result),
            Self::ThreadEvents(result) => serialize_sorted_field(envelope, "result", result),
            Self::ThreadStop(result) => serialize_sorted_field(envelope, "result", result),
            Self::ProfileOpen(result) => serialize_sorted_field(envelope, "result", result),
            Self::WorkspaceCreate(result) => serialize_sorted_field(envelope, "result", result),
            Self::WorkspaceList(result) => serialize_sorted_field(envelope, "result", result),
            Self::WorkspaceStatus(result) => serialize_sorted_field(envelope, "result", result),
            Self::ProfileCreate(result) => serialize_sorted_field(envelope, "result", result),
            Self::ProfileList(result) => serialize_sorted_field(envelope, "result", result),
            Self::ProfileStatus(result) => serialize_sorted_field(envelope, "result", result),
            Self::ProfileUpdate(result) => serialize_sorted_field(envelope, "result", result),
        }
    }
}

fn serialize_sorted_field<S, T>(
    envelope: &mut S,
    name: &'static str,
    value: &T,
) -> Result<(), S::Error>
where
    S: SerializeStruct,
    T: Serialize,
{
    let value = serde_json::to_value(value).map_err(serde::ser::Error::custom)?;
    envelope.serialize_field(name, &value)
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn approval_profile_is_prompt(profile: &ApprovalProfile) -> bool {
    *profile == ApprovalProfile::Prompt
}

/// Top-level versioned protocol envelope.
#[derive(Clone, Debug, PartialEq)]
pub struct Envelope {
    /// Protocol version.
    pub v: u32,
    /// Request identifier, when the message belongs to a request.
    pub id: Option<String>,
    /// Envelope kind.
    pub kind: EnvelopeKind,
    /// Method name for requests and their responses.
    pub method: Option<ProtocolMethod>,
    /// Typed request selected by `method`.
    pub params: Option<ProtocolRequest>,
    /// Typed response selected by `method`.
    pub result: Option<ProtocolResponse>,
    /// Structured protocol error for an error response.
    pub error: Option<ProtocolError>,
}

impl Envelope {
    /// Builds a request around its typed method-specific parameters.
    pub fn request(id: Option<String>, request: ProtocolRequest) -> Self {
        let method = request.method();
        Self {
            v: PROTOCOL_VERSION,
            id,
            kind: EnvelopeKind::Request,
            method: Some(method),
            params: Some(request),
            result: None,
            error: None,
        }
    }

    /// Builds a successful response around a typed method-specific result.
    pub fn typed_response(id: Option<String>, response: ProtocolResponse) -> Self {
        let method = response.method();
        Self {
            v: PROTOCOL_VERSION,
            id,
            kind: EnvelopeKind::Response,
            method: Some(method),
            params: None,
            result: Some(response),
            error: None,
        }
    }

    /// Serializes a result through the closed response variant for `method`.
    pub fn response<T: Serialize>(id: Option<String>, method: Option<String>, result: T) -> Self {
        let method = ProtocolMethod::from(method.expect("response method is required"));
        let result = serde_json::to_value(result).expect("protocol result serializes");
        let response = ProtocolResponse::decode(method, Some(result))
            .expect("result matches the response method");
        Self::typed_response(id, response)
    }

    /// Serializes a typed result and builds a successful response.
    pub fn response_from<T: Serialize>(
        id: Option<String>,
        method: Option<String>,
        result: T,
    ) -> Self {
        Self::response(id, method, result)
    }

    /// Builds an error response with the supplied code and message.
    pub fn error(
        id: Option<String>,
        method: Option<String>,
        code: ProtocolErrorCode,
        message: impl Into<String>,
    ) -> Self {
        Self {
            v: PROTOCOL_VERSION,
            id,
            kind: EnvelopeKind::Error,
            method: method.map(ProtocolMethod::from),
            params: None,
            result: None,
            error: Some(ProtocolError {
                code,
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
    pub code: ProtocolErrorCode,
    /// Human-readable error detail.
    pub message: String,
}

impl Serialize for Envelope {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut envelope = serializer.serialize_struct("Envelope", 7)?;
        envelope.serialize_field("v", &self.v)?;
        envelope.serialize_field("id", &self.id)?;
        envelope.serialize_field("kind", &self.kind)?;
        if let Some(method) = self.method {
            envelope.serialize_field("method", &method)?;
        }
        if let Some(request) = &self.params {
            request.serialize_params(&mut envelope)?;
        }
        if let Some(response) = &self.result {
            response.serialize_result(&mut envelope)?;
        }
        if let Some(error) = &self.error {
            envelope.serialize_field("error", error)?;
        }
        envelope.end()
    }
}

impl<'de> Deserialize<'de> for Envelope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        WireEnvelope::deserialize(deserializer)?
            .into_envelope()
            .map_err(D::Error::custom)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireEnvelope {
    v: u32,
    id: Option<String>,
    kind: EnvelopeKind,
    #[serde(default)]
    method: Option<ProtocolMethod>,
    #[serde(default)]
    params: Option<Value>,
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    error: Option<ProtocolError>,
}

#[derive(Deserialize)]
struct VersionedEnvelope {
    v: u32,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    method: Option<Value>,
}

impl WireEnvelope {
    fn into_envelope(self) -> serde_json::Result<Envelope> {
        let Self {
            v,
            id,
            kind,
            method,
            params,
            result,
            error,
        } = self;
        match kind {
            EnvelopeKind::Request => {
                let method = required_method(method)?;
                reject_field(result.is_some(), "result", EnvelopeKind::Request)?;
                reject_field(error.is_some(), "error", EnvelopeKind::Request)?;
                let request = ProtocolRequest::decode(method, params)?;
                Ok(Envelope {
                    v,
                    id,
                    kind,
                    method: Some(method),
                    params: Some(request),
                    result: None,
                    error: None,
                })
            }
            EnvelopeKind::Response => {
                let method = required_method(method)?;
                reject_field(params.is_some(), "params", EnvelopeKind::Response)?;
                reject_field(error.is_some(), "error", EnvelopeKind::Response)?;
                let response = ProtocolResponse::decode(method, result)?;
                Ok(Envelope {
                    v,
                    id,
                    kind,
                    method: Some(method),
                    params: None,
                    result: Some(response),
                    error: None,
                })
            }
            EnvelopeKind::Error => {
                reject_field(params.is_some(), "params", EnvelopeKind::Error)?;
                reject_field(result.is_some(), "result", EnvelopeKind::Error)?;
                let error = error.ok_or_else(|| serde_json::Error::custom("error is required"))?;
                Ok(Envelope {
                    v,
                    id,
                    kind,
                    method,
                    params: None,
                    result: None,
                    error: Some(error),
                })
            }
            EnvelopeKind::Event => {
                reject_field(params.is_some(), "params", EnvelopeKind::Event)?;
                reject_field(result.is_some(), "result", EnvelopeKind::Event)?;
                reject_field(error.is_some(), "error", EnvelopeKind::Event)?;
                Ok(Envelope {
                    v,
                    id,
                    kind,
                    method,
                    params: None,
                    result: None,
                    error: None,
                })
            }
        }
    }
}

fn required_method(method: Option<ProtocolMethod>) -> serde_json::Result<ProtocolMethod> {
    method.ok_or_else(|| serde_json::Error::custom("method is required"))
}

fn reject_field(present: bool, field: &str, kind: EnvelopeKind) -> serde_json::Result<()> {
    if present {
        return Err(serde_json::Error::custom(format!(
            "{field} is not valid for a {kind:?} envelope"
        )));
    }
    Ok(())
}

fn decode_params<T>(params: Option<Value>, method: ProtocolMethod) -> serde_json::Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let params =
        params.ok_or_else(|| serde_json::Error::custom(format!("{method} params are required")))?;
    serde_json::from_value(params)
}

fn decode_empty_params(params: Option<Value>, method: ProtocolMethod) -> serde_json::Result<()> {
    match params {
        None | Some(Value::Null) => Ok(()),
        Some(Value::Object(fields)) if fields.is_empty() => Ok(()),
        Some(_) => Err(serde_json::Error::custom(format!(
            "{method} params must be empty"
        ))),
    }
}

fn decode_result<T>(result: Option<Value>, method: ProtocolMethod) -> serde_json::Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let result =
        result.ok_or_else(|| serde_json::Error::custom(format!("{method} result is required")))?;
    serde_json::from_value(result)
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
    /// Platonic product identity and build provenance.
    pub daemon_version: String,
    /// Workspace identifier served by the daemon.
    pub workspace_id: String,
    /// Daemon-owned ledger path.
    pub ledger_path: String,
    /// Advertised protocol capabilities.
    pub capabilities: Vec<Capability>,
    /// Daemon runtime scope, present for the host-wide server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub daemon_scope: Option<String>,
}

/// Daemon-lifetime approval posture for one local session.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalProfile {
    /// Effects requiring approval pause for an explicit actor decision.
    #[default]
    Prompt,
    /// Eligible workspace writes and exact `shell.exec` calls are auto-granted.
    Yolo,
}

impl ApprovalProfile {
    /// Returns the exact approval-profile wire value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Prompt => "prompt",
            Self::Yolo => "yolo",
        }
    }
}

impl fmt::Display for ApprovalProfile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.pad(self.as_str())
    }
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

/// Parameters for setting one daemon-lifetime session approval profile.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionApprovalProfileSetParams {
    /// Exact session whose live profile changes.
    pub session_id: String,
    /// New live approval profile.
    pub profile: ApprovalProfile,
}

/// Result of setting one daemon-lifetime session approval profile.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionApprovalProfileSetResult {
    /// Exact session whose live profile changed.
    pub session_id: String,
    /// Authoritative live approval profile after the mutation.
    pub profile: ApprovalProfile,
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
    /// Effective provider wire protocol.
    pub provider_protocol: DaemonStatusProviderProtocol,
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

/// Provider wire protocol returned by `daemon.status`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonStatusProviderProtocol {
    /// OpenAI-compatible Chat Completions protocol.
    ChatCompletions,
    /// OpenAI-compatible Responses protocol.
    Responses,
}

impl DaemonStatusProviderProtocol {
    /// Returns the exact provider-protocol wire value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ChatCompletions => "chat_completions",
            Self::Responses => "responses",
        }
    }
}

impl fmt::Display for DaemonStatusProviderProtocol {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.pad(self.as_str())
    }
}

/// Current daemon identity facts returned by `daemon.status`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonStatusDaemon {
    /// Platonic product release version.
    pub package_version: String,
    /// Full source commit from build provenance, when known.
    pub build_commit: Option<String>,
    /// UTC build date from build provenance, when known.
    pub build_date_utc: Option<String>,
    /// Monotonic process uptime in milliseconds.
    pub uptime_ms: u64,
    /// Daemon socket endpoint path.
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
    /// Live daemon-lifetime approval profile for the selected session.
    #[serde(default, skip_serializing_if = "approval_profile_is_prompt")]
    pub approval_profile: ApprovalProfile,
}

/// Immutable startup approval policy carried by a thread authority record.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadApprovalPolicy {
    /// Effects requiring approval pause for an explicit actor decision.
    #[default]
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

/// Durable lifecycle role of one thread authority.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadKind {
    /// The profile's single active-tree root.
    Home,
    /// A same-profile descendant of the home thread.
    Child,
    /// Historical authority retained only for replay and audit.
    #[default]
    Legacy,
}

impl ThreadKind {
    /// Returns the exact wire and persistence value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Home => "home",
            Self::Child => "child",
            Self::Legacy => "legacy",
        }
    }

    /// Parses an exact thread-kind value.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "home" => Some(Self::Home),
            "child" => Some(Self::Child),
            "legacy" => Some(Self::Legacy),
            _ => None,
        }
    }
}

fn thread_kind_is_legacy(kind: &ThreadKind) -> bool {
    *kind == ThreadKind::Legacy
}

/// One server-created worktree granted to a thread.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadWorktree {
    /// Repository identity within the thread's workspace.
    pub repo: String,
    /// Branch checked out for this thread.
    pub branch: String,
    /// Canonical path where the worktree was created.
    pub path: String,
}

/// One repository and optional existing branch requested at thread spawn.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadRepositoryRequest {
    /// Workspace-relative repository name, with `.` naming the workspace root.
    pub repo: String,
    /// Existing branch to claim, or none for a fresh thread-named branch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
}

/// Filesystem write-confinement selected immutably when a thread is admitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadConfinement {
    /// Linux Landlock write confinement is applied before the run child starts work.
    Landlock,
    /// This host cannot confine the thread and server policy permits fallback.
    None,
}

impl ThreadConfinement {
    /// Returns the exact wire and persistence value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Landlock => "landlock",
            Self::None => "none",
        }
    }

    /// Parses an exact confinement value.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "landlock" => Some(Self::Landlock),
            "none" => Some(Self::None),
            _ => None,
        }
    }
}

/// One host path granted to a thread independently of confinement mechanism.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadGrantedPath {
    /// Canonical host path granted to the thread.
    pub path: String,
    /// Whether the thread may write beneath this path.
    pub writable: bool,
}

/// Complete immutable authority written before a spawned thread becomes live.
///
/// Profile classification fields default to legacy when decoding older v1
/// records.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ThreadAuthorityRecord {
    /// Stable daemon-minted thread identifier.
    pub thread_id: String,
    /// Durable parent thread, or none for a locally approved root thread.
    pub parent_thread_id: Option<String>,
    /// Actor whose approval admitted this spawn.
    pub spawning_actor: String,
    /// Exact working directory selected for this authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// Historical agent identity, absent on profile-based authorities.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    /// Workspace-bound profile identity, absent only on unscoped legacy records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_id: Option<ProfileId>,
    /// Profile revision resolved when this authority was created.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_revision: Option<u64>,
    /// Durable home, child, or legacy classification.
    #[serde(default, skip_serializing_if = "thread_kind_is_legacy")]
    pub thread_kind: ThreadKind,
    /// Exact model requested for the thread.
    pub model: String,
    /// Exact provider reasoning effort requested for the thread.
    pub reasoning_effort: ReasoningEffort,
    /// Immutable startup approval policy.
    pub approval_policy: ThreadApprovalPolicy,
    /// Exact internal tool names available after spawn-time resolution.
    #[serde(default)]
    pub toolset: Vec<String>,
    /// Server-created repository worktrees assigned to this thread.
    #[serde(default)]
    pub worktrees: Vec<ThreadWorktree>,
    /// Mechanism-independent host paths granted to this thread.
    #[serde(default)]
    pub granted_paths: Vec<ThreadGrantedPath>,
    /// Whether this thread is granted network access.
    #[serde(default)]
    pub network: bool,
    /// Authority creation time in Unix milliseconds.
    pub created_at_ms: u64,
}

/// Protocol-v2 authority projection used by thread spawn, list, and status.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadStatusAuthority {
    /// Stable daemon-minted thread identifier.
    pub thread_id: String,
    /// Durable parent thread, or none for a locally approved root thread.
    pub parent_thread_id: Option<String>,
    /// Actor whose approval admitted this spawn.
    pub spawning_actor: String,
    /// Workspace-bound profile identity, absent only on unscoped legacy records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_id: Option<ProfileId>,
    /// Profile revision resolved when this authority was created.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_revision: Option<u64>,
    /// Durable home, child, or legacy classification.
    #[serde(default, skip_serializing_if = "thread_kind_is_legacy")]
    pub thread_kind: ThreadKind,
    /// Root home thread for this profile tree, absent only for legacy authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub home_thread_id: Option<String>,
    /// Canonical compatibility working directory for this thread.
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
    /// Daemon-lifetime epoch paired with every live event cursor.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub live_epoch_id: String,
    /// Whether this daemon process currently has the thread loaded.
    pub loaded: bool,
    /// Active turn identifier, or none while the loaded thread is idle.
    pub current_turn_id: Option<String>,
    /// Latest daemon-observed activity time, absent when the thread is not loaded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_activity_at_ms: Option<u64>,
}

/// One immutable thread authority record joined with current daemon state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadStatus {
    /// Protocol-v2 durable authority projection.
    pub authority: ThreadStatusAuthority,
    /// Transient state queried from the serving daemon.
    pub live: ThreadLiveState,
    /// Unconsumed spawn-edge messages waiting for a future turn.
    #[serde(default)]
    pub return_availability: ThreadReturnAvailability,
}

/// Counts of durable spawn-edge messages available to a future thread turn.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadReturnAvailability {
    /// Child returns waiting for this parent thread.
    pub child_returns: u64,
    /// Parent answers waiting for this child thread.
    pub parent_answers: u64,
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
        /// Required same-profile parent thread.
        parent_thread_id: String,
        /// Requested working directory.
        cwd: String,
        /// Requested model.
        model: String,
        /// Requested reasoning effort.
        reasoning_effort: ReasoningEffort,
        /// Requested immutable approval policy.
        approval_policy: ThreadApprovalPolicy,
        /// Repositories to assign, or empty to infer the repository containing `cwd`.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        repositories: Vec<ThreadRepositoryRequest>,
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

/// Decision resolving one pending profile-home reservation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "decision")]
pub enum ProfileOpenDecision {
    /// Grant the reserved home authority.
    Grant,
    /// Deny the reservation with an operator-visible reason.
    Deny {
        /// Human-readable denial reason.
        reason: String,
    },
    /// Cancel the reservation without creating authority.
    Cancel,
}

/// Parameters for resolving, starting, or deciding one profile home.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "action")]
pub enum ProfileOpenParams {
    /// Read the existing home designation without mutation.
    Resolve {
        /// Profile whose home should be resolved.
        profile_id: ProfileId,
    },
    /// Reserve one idempotent home proposal.
    Start {
        /// Profile whose home should be created.
        profile_id: ProfileId,
        /// Caller-stable idempotency key for this exact proposal.
        idempotency_key: String,
        /// Repositories assigned to the home authority.
        repositories: Vec<ThreadRepositoryRequest>,
        /// Repository containing the initial working directory.
        working_repository: String,
        /// Relative directory beneath `working_repository`.
        #[serde(default = "default_working_subdir")]
        working_subdir: String,
    },
    /// Resolve one pending reservation.
    Decide {
        /// Server-minted reservation identifier.
        home_reservation_id: String,
        /// Grant, deny, or cancel decision.
        decision: ProfileOpenDecision,
    },
}

fn default_working_subdir() -> String {
    ".".into()
}

/// Typed outcome returned by `profile.open`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum ProfileOpenResult {
    /// The profile exists but has no durable home designation.
    NoHome {
        /// Exact profile that was resolved.
        profile_id: ProfileId,
    },
    /// The proposal is reserved and requires an explicit decision.
    ApprovalRequired {
        /// Exact profile awaiting home admission.
        profile_id: ProfileId,
        /// Server-minted pending reservation identifier.
        home_reservation_id: String,
        /// Thread identifier reserved for the proposed home.
        thread_id: String,
        /// Typed effect evaluated for admission.
        effect: EffectClass,
        /// Policy reason presented to the operator.
        reason: String,
    },
    /// The durable home was created or resolved.
    Opened {
        /// Exact profile owning the home.
        profile_id: ProfileId,
        /// Durable authority joined with current daemon state.
        thread: Box<ThreadStatus>,
        /// Whether this request committed the home authority.
        created: bool,
    },
    /// The reservation was durably denied.
    Denied {
        /// Exact profile whose reservation was denied.
        profile_id: ProfileId,
        /// Resolved reservation identifier.
        home_reservation_id: String,
        /// Thread identifier that was not admitted.
        thread_id: String,
        /// Durable denial reason.
        reason: String,
    },
    /// The reservation was durably canceled.
    Canceled {
        /// Exact profile whose reservation was canceled.
        profile_id: ProfileId,
        /// Resolved reservation identifier.
        home_reservation_id: String,
        /// Thread identifier that was not admitted.
        thread_id: String,
    },
}

/// Whether a registered workspace's directory is still present.
///
/// A broken workspace is reported, never omitted: its ledger is retained and
/// spawning into it fails at the gate rather than silently creating a new,
/// empty workspace at the same name (P021).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceHealthName {
    /// The registered directory is present.
    Present,
    /// The registered directory is gone; the ledger is retained.
    Broken,
}

/// One registered workspace as reported over the wire.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceSummary {
    /// Server-minted identity, stable across moves and never path-derived.
    pub id: String,
    /// The handle an operator uses. Unique among workspaces.
    pub name: String,
    /// Where the workspace currently lives.
    pub root: String,
    /// Where this workspace's ledger lives.
    pub ledger_path: String,
    /// When the workspace was first registered.
    pub created_at_ms: u64,
    /// Whether the registered directory is still present.
    pub health: WorkspaceHealthName,
}

/// Parameters for `workspace.create`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceCreateParams {
    /// Operator-chosen handle. Must be unique and must not be empty.
    pub name: String,
    /// Directory the workspace names. Must exist at creation.
    pub root: String,
}

/// Result returned by `workspace.create`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceCreateResult {
    /// The workspace that was created.
    pub workspace: WorkspaceSummary,
}

/// Parameters for `workspace.list`. Empty today, present so the method has a
/// place to grow a filter without a breaking change.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceListParams {}

/// Result returned by `workspace.list`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceListResult {
    /// Every registered workspace, broken ones included.
    pub workspaces: Vec<WorkspaceSummary>,
}

/// Parameters for one `workspace.status` readback.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceStatusParams {
    /// Workspace to read, by minted id.
    pub workspace_id: String,
}

/// Result returned by `workspace.status`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceStatusResult {
    /// The workspace that was read.
    pub workspace: WorkspaceSummary,
}

/// Versioned profile context authored by an operator.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileContent {
    /// Markdown instructions selected into bounded turn context.
    #[serde(default)]
    pub instructions_markdown: String,
    /// Markdown memory selected into bounded turn context.
    #[serde(default)]
    pub memory_markdown: String,
    /// Immutable server-resolved context references.
    #[serde(default)]
    pub skill_refs: Vec<String>,
}

/// One immutable profile-content revision.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileRevision {
    /// Monotonic revision number.
    pub revision: u64,
    /// Previous revision, absent only for revision one.
    pub parent_revision: Option<u64>,
    /// Server-derived actor that authored this revision.
    pub actor: String,
    /// Revision creation time in Unix milliseconds.
    pub created_at_ms: u64,
    /// Lowercase SHA-256 hash of the serialized content.
    pub content_hash: String,
    /// Exact content stored for this revision.
    pub content: ProfileContent,
}

/// One workspace-bound profile as reported over the wire.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileSummary {
    /// Stable server-minted profile identity.
    pub id: ProfileId,
    /// Operator-authored name, unique inside the workspace.
    pub display_name: String,
    /// Hard workspace binding, resolved before any thread runs.
    pub workspace_id: String,
    /// Default model used by future threads.
    pub model: String,
    /// Default provider reasoning effort used by future threads.
    pub reasoning_effort: ReasoningEffort,
    /// Default approval policy used by future threads.
    pub approval_policy: ThreadApprovalPolicy,
    /// Default validated internal tool names used by future threads.
    pub toolset: Vec<String>,
    /// Current profile-content revision.
    pub current_revision: u64,
    /// The profile's durable home thread, once opened.
    pub home_thread_id: Option<String>,
    /// Whether the bound workspace directory is present.
    pub workspace_health: WorkspaceHealthName,
    /// When the profile was created, in Unix milliseconds.
    pub created_at_ms: u64,
}

/// Complete profile status with its current content revision.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileStatus {
    /// Stable metadata and future-thread defaults.
    pub profile: ProfileSummary,
    /// Current immutable content revision.
    pub revision: ProfileRevision,
}

/// Parameters for `profile.create`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileCreateParams {
    /// Existing present workspace to bind permanently.
    pub workspace_id: String,
    /// Operator-authored name, unique inside the workspace.
    pub display_name: String,
    /// Default model, or the resolved provider model when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Default provider reasoning effort.
    #[serde(default)]
    pub reasoning_effort: ReasoningEffort,
    /// Default approval policy.
    #[serde(default)]
    pub approval_policy: ThreadApprovalPolicy,
    /// Default toolset, or the resolved server toolset when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub toolset: Option<Vec<String>>,
    /// Initial profile content.
    #[serde(default)]
    pub content: ProfileContent,
    /// Optional authorized configuration path used for provider readiness.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_path: Option<String>,
}

/// Result returned by `profile.create`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileCreateResult {
    /// The profile and revision that were created.
    pub status: ProfileStatus,
}

/// Parameters for `profile.list`.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileListParams {
    /// Optional workspace filter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    /// Requested result ceiling; defaults to 50 and may not exceed 100.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

/// Result returned by `profile.list`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileListResult {
    /// Profiles in creation order.
    pub profiles: Vec<ProfileSummary>,
    /// Whether more profiles exist beyond the requested ceiling.
    pub truncated: bool,
}

/// Parameters for `profile.status`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileStatusParams {
    /// Profile to read.
    pub profile_id: ProfileId,
}

/// Result returned by `profile.status`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileStatusResult {
    /// Complete current profile status.
    pub status: ProfileStatus,
}

/// Parameters for `profile.update`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileUpdateParams {
    /// Profile to update.
    pub profile_id: ProfileId,
    /// Default model used by future threads.
    pub model: String,
    /// Default provider reasoning effort used by future threads.
    pub reasoning_effort: ReasoningEffort,
    /// Default approval policy used by future threads.
    pub approval_policy: ThreadApprovalPolicy,
    /// Default toolset used by future threads.
    pub toolset: Vec<String>,
    /// Complete new profile content revision.
    pub content: ProfileContent,
}

/// Result returned by `profile.update`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileUpdateResult {
    /// Complete profile status after the update.
    pub status: ProfileStatus,
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

/// Parameters for one complete `thread.authority` readback.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadAuthorityParams {
    /// Thread whose immutable authority should be read.
    pub thread_id: String,
}

/// Result returned by `thread.authority`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadAuthorityResult {
    /// Complete twelve-field immutable authority record.
    pub authority: ThreadAuthorityRecord,
    /// Immutable confinement fact, absent only for records created before confinement shipped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confinement: Option<ThreadConfinement>,
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
    /// Prior interrupted run whose committed server facts should inform this turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prior_interrupted_run_id: Option<String>,
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
    /// Daemon epoch paired with `from_offset`, when continuing a live cursor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_epoch_id: Option<String>,
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

/// Why a live thread cursor must restart at the returned offset.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadEventsResetReason {
    /// The cursor belongs to a prior daemon lifetime.
    EpochChanged,
    /// The cursor fell behind the retained in-memory buffer.
    Lagged,
}

/// Retained event page returned by `thread.events`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadEventsResult {
    /// Exact durable thread whose events were read.
    pub thread_id: String,
    /// Daemon-lifetime epoch paired with this page's offsets.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub live_epoch_id: String,
    /// Typed reset reason, absent for an ordinary contiguous page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reset: Option<ThreadEventsResetReason>,
    /// First requested thread-local offset.
    pub from_offset: u64,
    /// Offset to use for the next page.
    pub next_offset: u64,
    /// Current external turn, or none after controller release.
    pub current_turn_id: Option<String>,
    /// Events in contiguous thread-local order.
    pub events: Vec<BufferedThreadEvent>,
}

/// Parameters for stopping one durable thread.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadStopParams {
    /// Durable thread to stop.
    pub thread_id: String,
    /// Actor requesting the ledger-recorded stop.
    pub actor: String,
}

/// Typed outcome returned by `thread.stop`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum ThreadStopResult {
    /// This request stopped the thread and recorded the action.
    Stopped {
        /// Exact durable thread that was stopped.
        thread_id: String,
        /// Active turn terminated by the stop, or none for an idle thread.
        stopped_turn_id: Option<String>,
        /// Durable stop time in Unix milliseconds.
        stopped_at_ms: u64,
    },
    /// The thread was already durably stopped by an earlier request.
    AlreadyStopped {
        /// Exact durable thread that was already stopped.
        thread_id: String,
        /// Turn terminated by the original stop, when one was active.
        stopped_turn_id: Option<String>,
        /// Durable original stop time in Unix milliseconds.
        stopped_at_ms: u64,
    },
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
    /// Optional approval profile applied atomically to the fresh session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_profile: Option<ApprovalProfile>,
    /// Whether the request waits for the run's terminal result.
    #[serde(default)]
    pub wait: Option<bool>,
}

/// Claimed outcome for a worker thread's completion claim.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum CompletionOutcome {
    /// The worker claims to have completed the assigned work.
    Done,
    /// The worker claims the assigned work is blocked.
    Blocked {
        /// Reason the worker claims to be blocked.
        reason: String,
    },
}

/// A worker thread's self-reported completion claim.
///
/// This is an additive-optional protocol result: absent for non-worker runs
/// or when the worker makes no claim. The type makes the claim parseable,
/// never true — a claim is distinct from coordinator-verified completion.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompletionClaim {
    /// Claimed outcome.
    pub outcome: CompletionOutcome,
    /// Base commit the work started from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base: Option<String>,
    /// Head commit the work produced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head: Option<String>,
    /// Repository paths changed by the work.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changed_paths: Vec<String>,
    /// Pull request identifier for the work.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pr: Option<String>,
    /// CI check run identifiers for the work.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub checks: Vec<String>,
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
    /// Additive-optional completion claim from the worker thread.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_claim: Option<CompletionClaim>,
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
    /// Optional approval profile replacing the existing session's live profile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_profile: Option<ApprovalProfile>,
    /// Prior interrupted run whose committed server facts should inform this turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prior_interrupted_run_id: Option<String>,
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

/// Parameters for committing one complete raw voice event batch.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VoiceEventsCommitParams {
    /// Durable run receiving the batch.
    pub run_id: String,
    /// Raw client-observed events; the server assigns durable envelopes.
    pub events: Vec<VoiceEvent>,
}

/// Parameters for reading one run's committed voice event batch.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VoiceEventsReadParams {
    /// Durable run whose batch should be read.
    pub run_id: String,
}

/// Server-minted voice event envelopes returned by commit and readback.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VoiceEventsResult {
    /// Durable run owning the batch.
    pub run_id: String,
    /// Zero-based authoritative envelopes, or an empty readback.
    pub events: Vec<VoiceEventEnvelope>,
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
        /// Whether the server's exact yolo predicate permits this call.
        yolo_eligible: bool,
    },
    /// Cancellation observation for a run.
    Canceled {
        /// Canceled run identifier.
        run_id: String,
    },
    /// Completion claim produced by a worker thread at turn end.
    CompletionClaimed {
        /// Run whose worker produced the claim.
        run_id: String,
        /// The claim itself.
        claim: CompletionClaim,
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
        #[serde(default, skip_serializing_if = "is_false")]
        yolo_eligible: bool,
    },
    Canceled {
        run_id: String,
    },
    CompletionClaimed {
        /// Run whose worker produced the claim.
        run_id: String,
        /// The claim itself.
        claim: CompletionClaim,
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
                yolo_eligible,
            } => KnownStreamEvent::ApprovalRequested {
                run_id: run_id.clone(),
                tool_call_id: tool_call_id.clone(),
                tool_name: tool_name.clone(),
                effect: effect.clone(),
                reason: reason.clone(),
                diff_preview: diff_preview.clone(),
                approval_preview: approval_preview.clone(),
                yolo_eligible: *yolo_eligible,
            }
            .serialize(serializer),
            Self::Canceled { run_id } => KnownStreamEvent::Canceled {
                run_id: run_id.clone(),
            }
            .serialize(serializer),
            Self::CompletionClaimed { run_id, claim } => KnownStreamEvent::CompletionClaimed {
                run_id: run_id.clone(),
                claim: claim.clone(),
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
            "ledger" | "assistant_delta" | "approval_requested" | "canceled"
            | "completion_claimed" => serde_json::from_value::<KnownStreamEvent>(value)
                .map(StreamEvent::from)
                .map_err(D::Error::custom),
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
                yolo_eligible,
            } => Self::ApprovalRequested {
                run_id,
                tool_call_id,
                tool_name,
                effect,
                reason,
                diff_preview,
                approval_preview,
                yolo_eligible,
            },
            KnownStreamEvent::Canceled { run_id } => Self::Canceled { run_id },
            KnownStreamEvent::CompletionClaimed { run_id, claim } => {
                Self::CompletionClaimed { run_id, claim }
            }
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
    /// Attributed actor supplied by the already-trusted local client boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
}

/// Parameters for requesting run cancellation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunCancelParams {
    /// Run to cancel.
    pub run_id: String,
    /// Attributed actor supplied by an already-trusted local client boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
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
    /// Additive-optional completion claim from the run's worker thread.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_claim: Option<CompletionClaim>,
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
    decode_request_for_version(line, PROTOCOL_VERSION)
}

fn decode_request_for_version(
    line: &str,
    supported_version: u32,
) -> Result<Envelope, Box<Envelope>> {
    let versioned = serde_json::from_str::<VersionedEnvelope>(line).map_err(|error| {
        Box::new(Envelope::error(
            None,
            None,
            ERROR_MALFORMED_REQUEST,
            format!("request is not a valid protocol envelope: {error}"),
        ))
    })?;
    if versioned.v != supported_version {
        let mut response = Envelope::error(
            versioned.id,
            versioned
                .method
                .as_ref()
                .and_then(Value::as_str)
                .and_then(ProtocolMethod::parse)
                .map(|method| method.to_string()),
            ERROR_UNSUPPORTED_VERSION,
            format!("unsupported protocol version: {}", versioned.v),
        );
        response.v = supported_version;
        return Err(Box::new(response));
    }

    let wire = serde_json::from_str::<WireEnvelope>(line).map_err(|error| {
        Box::new(Envelope::error(
            None,
            None,
            ERROR_MALFORMED_REQUEST,
            format!("request is not a valid protocol envelope: {error}"),
        ))
    })?;

    if wire.kind != EnvelopeKind::Request {
        return Err(Box::new(Envelope::error(
            wire.id,
            wire.method.map(|method| method.to_string()),
            ERROR_MALFORMED_REQUEST,
            "envelope kind must be request",
        )));
    }
    let method = match wire.method {
        Some(method) => method,
        None => {
            return Err(Box::new(Envelope::error(
                wire.id,
                None,
                ERROR_MALFORMED_REQUEST,
                "request method is required",
            )));
        }
    };
    if wire.result.is_some() || wire.error.is_some() {
        return Err(Box::new(Envelope::error(
            wire.id,
            Some(method.to_string()),
            ERROR_MALFORMED_REQUEST,
            "request contains a response-only field",
        )));
    }
    let request = ProtocolRequest::decode(method, wire.params).map_err(|error| {
        Box::new(Envelope::error(
            wire.id.clone(),
            Some(method.to_string()),
            ERROR_MALFORMED_REQUEST,
            format!("{method} params are invalid: {error}"),
        ))
    })?;

    Ok(Envelope {
        v: wire.v,
        id: wire.id,
        kind: wire.kind,
        method: Some(method),
        params: Some(request),
        result: None,
        error: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::de::DeserializeOwned;
    use serde_json::json;

    #[test]
    fn product_and_client_build_identities_remain_distinct_and_truthful() {
        assert_eq!(
            PLATONIC_BUILD_IDENTITY,
            format!("{PLATONIC_PRODUCT_VERSION} ({PLATONIC_BUILD_COMMIT}, {PLATONIC_BUILD_DATE})")
        );
        assert_eq!(
            PLATONIC_DIAGNOSTIC_IDENTITY,
            format!("platonic {PLATONIC_BUILD_IDENTITY}")
        );
        assert_eq!(
            PLATO_BUILD_IDENTITY.split_whitespace().next(),
            Some(env!("CARGO_PKG_VERSION"))
        );
    }

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
    fn capability_names_and_error_codes_keep_exact_v2_literals() {
        assert_eq!(MAX_PROTOCOL_LINE_BYTES, 1_048_576);
        assert_eq!(
            CAPABILITIES.map(Capability::as_str),
            [
                "hello",
                "run.start",
                "message.append",
                "issue-prep.start",
                "events.stream",
                "voice.events.commit",
                "voice.events.read",
                "approval.decide",
                "run.cancel",
                "sessions.list",
                "transcript.read",
                "transcript.read.typed",
                "transcript.read.pending_approval",
                "daemon.status",
                "session.approval_profile.set",
                "daemon.shutdown_if_idle",
                "thread.spawn",
                "thread.list",
                "thread.status",
                "thread.authority",
                "thread.send",
                "thread.events",
                "thread.stop",
                "profile.open",
                "workspace.create",
                "workspace.list",
                "workspace.status",
                "profile.create",
                "profile.list",
                "profile.status",
                "profile.update",
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
                ERROR_VOICE_EVENTS_CONFLICT,
                ERROR_SESSIONS_LIST_FAILED,
                ERROR_THREAD_AUTHORITY_EXCEEDED,
                ERROR_THREAD_AUTHORITY_FAILED,
                ERROR_THREAD_EVENTS_FAILED,
                ERROR_THREAD_LIST_FAILED,
                ERROR_THREAD_SPAWN_FAILED,
                ERROR_THREAD_SEND_FAILED,
                ERROR_THREAD_STATUS_FAILED,
                ERROR_THREAD_STOP_FAILED,
                ERROR_PROFILE_OPEN_CONFLICT,
                ERROR_PROFILE_OPEN_FAILED,
                ERROR_UNSUPPORTED_METHOD,
                ERROR_UNSUPPORTED_VERSION,
                ERROR_WORKSPACE_MISMATCH,
                ERROR_WORKSPACE_UNREGISTERED,
                ERROR_WORKSPACE_BROKEN,
            ]
            .map(ProtocolErrorCode::as_str),
            [
                "daemon_shutting_down",
                "malformed_request",
                "lagged",
                "internal_error",
                "issue_prep_failed",
                "not_found",
                "overload",
                "run_failed",
                "voice_events_conflict",
                "sessions_list_failed",
                "thread_authority_exceeded",
                "thread_authority_failed",
                "thread_events_failed",
                "thread_list_failed",
                "thread_spawn_failed",
                "thread_send_failed",
                "thread_status_failed",
                "thread_stop_failed",
                "profile_open_conflict",
                "profile_open_failed",
                "unsupported_method",
                "unsupported_version",
                "workspace_mismatch",
                "workspace_unregistered",
                "workspace_broken",
            ]
        );

        let error = ProtocolError {
            code: ERROR_RUN_FAILED,
            message: "synthetic failure".into(),
        };
        assert_eq!(error.to_string(), "run_failed: synthetic failure");
    }

    #[test]
    fn every_current_method_keeps_exact_v2_request_and_response_bytes() {
        const REQUESTS: &[(ProtocolMethod, &str)] = &[
            (
                ProtocolMethod::Hello,
                r#"{"v":2,"id":"hello_1","kind":"request","method":"hello","params":{"workspace_id":"work-1","workspace_root":"/work"}}"#,
            ),
            (
                ProtocolMethod::RunStart,
                r#"{"v":2,"id":"run_1","kind":"request","method":"run.start","params":{"config_path":null,"question":"hello","wait":false}}"#,
            ),
            (
                ProtocolMethod::MessageAppend,
                r#"{"v":2,"id":"append_1","kind":"request","method":"message.append","params":{"config_path":null,"message":"again","session_id":"session-1","wait":false}}"#,
            ),
            (
                ProtocolMethod::IssuePrepStart,
                r#"{"v":2,"id":"prep_1","kind":"request","method":"issue-prep.start","params":{"config_path":null,"input":"rough issue"}}"#,
            ),
            (
                ProtocolMethod::EventsStream,
                r#"{"v":2,"id":"events_1","kind":"request","method":"events.stream","params":{"from_offset":0,"limit":64,"run_id":"run-1"}}"#,
            ),
            (
                ProtocolMethod::VoiceEventsCommit,
                r#"{"v":2,"id":"voice_commit_1","kind":"request","method":"voice.events.commit","params":{"events":[{"event":"voice_spoken","interrupted_at":null,"run_id":"run-1","sentence_count":2,"ttfa_ms":287,"turn_id":"turn-1"}],"run_id":"run-1"}}"#,
            ),
            (
                ProtocolMethod::VoiceEventsRead,
                r#"{"v":2,"id":"voice_read_1","kind":"request","method":"voice.events.read","params":{"run_id":"run-1"}}"#,
            ),
            (
                ProtocolMethod::ApprovalDecide,
                r#"{"v":2,"id":"approval_1","kind":"request","method":"approval.decide","params":{"decision":"grant","reason":null,"run_id":"run-1","tool_call_id":"call-1"}}"#,
            ),
            (
                ProtocolMethod::RunCancel,
                r#"{"v":2,"id":"cancel_1","kind":"request","method":"run.cancel","params":{"run_id":"run-1"}}"#,
            ),
            (
                ProtocolMethod::SessionsList,
                r#"{"v":2,"id":"sessions_1","kind":"request","method":"sessions.list"}"#,
            ),
            (
                ProtocolMethod::TranscriptRead,
                r#"{"v":2,"id":"transcript_1","kind":"request","method":"transcript.read","params":{"run_id":"run-1","session_id":null}}"#,
            ),
            (
                ProtocolMethod::DaemonStatus,
                r#"{"v":2,"id":"status_1","kind":"request","method":"daemon.status","params":{"config_path":null,"session_id":null}}"#,
            ),
            (
                ProtocolMethod::SessionApprovalProfileSet,
                r#"{"v":2,"id":"profile_1","kind":"request","method":"session.approval_profile.set","params":{"profile":"yolo","session_id":"session-1"}}"#,
            ),
            (
                ProtocolMethod::DaemonShutdownIfIdle,
                r#"{"v":2,"id":"shutdown_1","kind":"request","method":"daemon.shutdown_if_idle"}"#,
            ),
            (
                ProtocolMethod::ThreadSpawn,
                r#"{"v":2,"id":"spawn_1","kind":"request","method":"thread.spawn","params":{"action":"start","approval_policy":"prompt","cwd":"/work","model":"gpt-5","parent_thread_id":"thread-home","reasoning_effort":"high"}}"#,
            ),
            (
                ProtocolMethod::ThreadList,
                r#"{"v":2,"id":"threads_1","kind":"request","method":"thread.list"}"#,
            ),
            (
                ProtocolMethod::ThreadStatus,
                r#"{"v":2,"id":"thread_status_1","kind":"request","method":"thread.status","params":{"thread_id":"thread-1"}}"#,
            ),
            (
                ProtocolMethod::ThreadAuthority,
                r#"{"v":2,"id":"authority_1","kind":"request","method":"thread.authority","params":{"thread_id":"thread-1"}}"#,
            ),
            (
                ProtocolMethod::ThreadSend,
                r#"{"v":2,"id":"send_1","kind":"request","method":"thread.send","params":{"controller_id":"terminal","message":"inspect","thread_id":"thread-1"}}"#,
            ),
            (
                ProtocolMethod::ThreadEvents,
                r#"{"v":2,"id":"thread_events_1","kind":"request","method":"thread.events","params":{"from_offset":0,"limit":64,"thread_id":"thread-1","wait_ms":1000}}"#,
            ),
            (
                ProtocolMethod::ThreadStop,
                r#"{"v":2,"id":"stop_1","kind":"request","method":"thread.stop","params":{"actor":"terminal","thread_id":"thread-1"}}"#,
            ),
            (
                ProtocolMethod::ProfileOpen,
                r#"{"v":2,"id":"profile_open_1","kind":"request","method":"profile.open","params":{"action":"resolve","profile_id":"profile-1"}}"#,
            ),
            (
                ProtocolMethod::WorkspaceCreate,
                r#"{"v":2,"id":"workspace_create_1","kind":"request","method":"workspace.create","params":{"name":"alpha","root":"/work"}}"#,
            ),
            (
                ProtocolMethod::WorkspaceList,
                r#"{"v":2,"id":"workspace_list_1","kind":"request","method":"workspace.list","params":{}}"#,
            ),
            (
                ProtocolMethod::WorkspaceStatus,
                r#"{"v":2,"id":"workspace_status_1","kind":"request","method":"workspace.status","params":{"workspace_id":"workspace-1"}}"#,
            ),
            (
                ProtocolMethod::ProfileCreate,
                r#"{"v":2,"id":"profile_create_1","kind":"request","method":"profile.create","params":{"approval_policy":"prompt","content":{"instructions_markdown":"","memory_markdown":"","skill_refs":[]},"display_name":"builder","model":"gpt-5","reasoning_effort":"high","toolset":["file.read"],"workspace_id":"workspace-1"}}"#,
            ),
            (
                ProtocolMethod::ProfileList,
                r#"{"v":2,"id":"profile_list_1","kind":"request","method":"profile.list","params":{}}"#,
            ),
            (
                ProtocolMethod::ProfileStatus,
                r#"{"v":2,"id":"profile_status_1","kind":"request","method":"profile.status","params":{"profile_id":"profile-builder"}}"#,
            ),
            (
                ProtocolMethod::ProfileUpdate,
                r#"{"v":2,"id":"profile_update_1","kind":"request","method":"profile.update","params":{"approval_policy":"prompt","content":{"instructions_markdown":"build","memory_markdown":"","skill_refs":[]},"model":"gpt-5","profile_id":"profile-builder","reasoning_effort":"high","toolset":["file.read"]}}"#,
            ),
        ];
        const RESPONSES: &[(ProtocolMethod, &str)] = &[
            (
                ProtocolMethod::Hello,
                r#"{"v":2,"id":"hello_1","kind":"response","method":"hello","result":{"capabilities":["hello"],"daemon_version":"0.2.0 test test","ledger_path":"/state/agent.db","workspace_id":"work-1"}}"#,
            ),
            (
                ProtocolMethod::RunStart,
                r#"{"v":2,"id":"run_1","kind":"response","method":"run.start","result":{"final_answer":null,"ledger_path":"/state/agent.db","run_id":"run-1","session_id":"session-1","status":"running"}}"#,
            ),
            (
                ProtocolMethod::MessageAppend,
                r#"{"v":2,"id":"append_1","kind":"response","method":"message.append","result":{"final_answer":"done","ledger_path":"/state/agent.db","run_id":"run-2","session_id":"session-1","status":"finished"}}"#,
            ),
            (
                ProtocolMethod::IssuePrepStart,
                r#"{"v":2,"id":"prep_1","kind":"response","method":"issue-prep.start","result":{"outcome":{"markdown":"Prepared issue","status":"candidate"},"run_dir":"/work/.plato/issue-prep/run-1"}}"#,
            ),
            (
                ProtocolMethod::EventsStream,
                r#"{"v":2,"id":"events_1","kind":"response","method":"events.stream","result":{"events":[],"from_offset":0,"next_offset":0,"run_id":"run-1","status":"running"}}"#,
            ),
            (
                ProtocolMethod::VoiceEventsCommit,
                r#"{"v":2,"id":"voice_commit_1","kind":"response","method":"voice.events.commit","result":{"events":[{"event":{"event":"voice_spoken","interrupted_at":null,"run_id":"run-1","sentence_count":2,"ttfa_ms":287,"turn_id":"turn-1"},"sequence":0,"v":1}],"run_id":"run-1"}}"#,
            ),
            (
                ProtocolMethod::VoiceEventsRead,
                r#"{"v":2,"id":"voice_read_1","kind":"response","method":"voice.events.read","result":{"events":[{"event":{"event":"voice_spoken","interrupted_at":null,"run_id":"run-1","sentence_count":2,"ttfa_ms":287,"turn_id":"turn-1"},"sequence":0,"v":1}],"run_id":"run-1"}}"#,
            ),
            (
                ProtocolMethod::ApprovalDecide,
                r#"{"v":2,"id":"approval_1","kind":"response","method":"approval.decide","result":{"run_id":"run-1","status":"running"}}"#,
            ),
            (
                ProtocolMethod::RunCancel,
                r#"{"v":2,"id":"cancel_1","kind":"response","method":"run.cancel","result":{"run_id":"run-1","status":"cancel_requested"}}"#,
            ),
            (
                ProtocolMethod::SessionsList,
                r#"{"v":2,"id":"sessions_1","kind":"response","method":"sessions.list","result":{"sessions":[]}}"#,
            ),
            (
                ProtocolMethod::TranscriptRead,
                r#"{"v":2,"id":"transcript_1","kind":"response","method":"transcript.read","result":{"final_answer":"done","run_id":"run-1","status":"finished","transcript":"[turn-1] assistant: done\n"}}"#,
            ),
            (
                ProtocolMethod::DaemonStatus,
                r#"{"v":2,"id":"status_1","kind":"response","method":"daemon.status","result":{"daemon":{"build_commit":null,"build_date_utc":null,"endpoint_path":"/tmp/agent.sock","package_version":"0.2.0","uptime_ms":0,"workspace_id":"work-1"},"model":{"key_present":false,"provider_kind":"open_ai","provider_protocol":"chat_completions","requested_alias":"gpt-5","served_model":null},"session":{"core_event_count":0,"human_turn_count":0,"latest_run_id":null,"ledger_path":"/state/agent.db","session_id":null},"trust":{"approval_denied_count":0,"approval_granted_count":0,"shell_session_grant":false},"usage":{"last_run":{"input_tokens":0,"output_tokens":0,"unknown_response_count":0},"session":{"input_tokens":0,"output_tokens":0,"unknown_response_count":0}}}}"#,
            ),
            (
                ProtocolMethod::SessionApprovalProfileSet,
                r#"{"v":2,"id":"profile_1","kind":"response","method":"session.approval_profile.set","result":{"profile":"yolo","session_id":"session-1"}}"#,
            ),
            (
                ProtocolMethod::DaemonShutdownIfIdle,
                r#"{"v":2,"id":"shutdown_1","kind":"response","method":"daemon.shutdown_if_idle","result":{"result":"shutdown"}}"#,
            ),
            (
                ProtocolMethod::ThreadSpawn,
                r#"{"v":2,"id":"spawn_1","kind":"response","method":"thread.spawn","result":{"actor":"terminal","reason":"denied","spawn_id":"spawn-1","status":"denied","thread_id":"thread-1"}}"#,
            ),
            (
                ProtocolMethod::ThreadList,
                r#"{"v":2,"id":"threads_1","kind":"response","method":"thread.list","result":{"threads":[]}}"#,
            ),
            (
                ProtocolMethod::ThreadStatus,
                r#"{"v":2,"id":"thread_status_1","kind":"response","method":"thread.status","result":{"thread":{"authority":{"approval_policy":"prompt","created_at_ms":42,"cwd":"/work","home_thread_id":"thread-home","model":"gpt-5","parent_thread_id":null,"profile_id":"profile-1","profile_revision":1,"reasoning_effort":"high","spawning_actor":"terminal","thread_id":"thread-home","thread_kind":"home"},"live":{"current_turn_id":null,"live_epoch_id":"epoch-1","loaded":false},"return_availability":{"child_returns":0,"parent_answers":0}}}}"#,
            ),
            (
                ProtocolMethod::ThreadAuthority,
                r#"{"v":2,"id":"authority_1","kind":"response","method":"thread.authority","result":{"authority":{"agent_id":"plato","approval_policy":"prompt","created_at_ms":42,"granted_paths":[{"path":"/work","writable":true}],"model":"gpt-5","network":false,"parent_thread_id":null,"reasoning_effort":"high","spawning_actor":"terminal","thread_id":"thread-1","toolset":["file.read"],"worktrees":[]}}}"#,
            ),
            (
                ProtocolMethod::ThreadSend,
                r#"{"v":2,"id":"send_1","kind":"response","method":"thread.send","result":{"status":"started","thread_id":"thread-1","turn_id":"turn-1"}}"#,
            ),
            (
                ProtocolMethod::ThreadEvents,
                r#"{"v":2,"id":"thread_events_1","kind":"response","method":"thread.events","result":{"current_turn_id":null,"events":[],"from_offset":0,"next_offset":0,"thread_id":"thread-1"}}"#,
            ),
            (
                ProtocolMethod::ThreadStop,
                r#"{"v":2,"id":"stop_1","kind":"response","method":"thread.stop","result":{"status":"stopped","stopped_at_ms":43,"stopped_turn_id":null,"thread_id":"thread-1"}}"#,
            ),
            (
                ProtocolMethod::ProfileOpen,
                r#"{"v":2,"id":"profile_open_1","kind":"response","method":"profile.open","result":{"profile_id":"profile-1","status":"no_home"}}"#,
            ),
            (
                ProtocolMethod::WorkspaceCreate,
                r#"{"v":2,"id":"workspace_create_1","kind":"response","method":"workspace.create","result":{"workspace":{"created_at_ms":41,"health":"present","id":"workspace-1","ledger_path":"/state/agent.db","name":"alpha","root":"/work"}}}"#,
            ),
            (
                ProtocolMethod::WorkspaceList,
                r#"{"v":2,"id":"workspace_list_1","kind":"response","method":"workspace.list","result":{"workspaces":[]}}"#,
            ),
            (
                ProtocolMethod::WorkspaceStatus,
                r#"{"v":2,"id":"workspace_status_1","kind":"response","method":"workspace.status","result":{"workspace":{"created_at_ms":41,"health":"present","id":"workspace-1","ledger_path":"/state/agent.db","name":"alpha","root":"/work"}}}"#,
            ),
            (
                ProtocolMethod::ProfileCreate,
                r#"{"v":2,"id":"profile_create_1","kind":"response","method":"profile.create","result":{"status":{"profile":{"approval_policy":"prompt","created_at_ms":42,"current_revision":1,"display_name":"builder","home_thread_id":null,"id":"profile-builder","model":"gpt-5","reasoning_effort":"high","toolset":["file.read"],"workspace_health":"present","workspace_id":"workspace-1"},"revision":{"actor":"host_operator","content":{"instructions_markdown":"","memory_markdown":"","skill_refs":[]},"content_hash":"hash","created_at_ms":42,"parent_revision":null,"revision":1}}}}"#,
            ),
            (
                ProtocolMethod::ProfileList,
                r#"{"v":2,"id":"profile_list_1","kind":"response","method":"profile.list","result":{"profiles":[],"truncated":false}}"#,
            ),
            (
                ProtocolMethod::ProfileStatus,
                r#"{"v":2,"id":"profile_status_1","kind":"response","method":"profile.status","result":{"status":{"profile":{"approval_policy":"prompt","created_at_ms":42,"current_revision":1,"display_name":"builder","home_thread_id":null,"id":"profile-builder","model":"gpt-5","reasoning_effort":"high","toolset":["file.read"],"workspace_health":"present","workspace_id":"workspace-1"},"revision":{"actor":"host_operator","content":{"instructions_markdown":"","memory_markdown":"","skill_refs":[]},"content_hash":"hash","created_at_ms":42,"parent_revision":null,"revision":1}}}}"#,
            ),
            (
                ProtocolMethod::ProfileUpdate,
                r#"{"v":2,"id":"profile_update_1","kind":"response","method":"profile.update","result":{"status":{"profile":{"approval_policy":"prompt","created_at_ms":42,"current_revision":2,"display_name":"builder","home_thread_id":null,"id":"profile-builder","model":"gpt-5","reasoning_effort":"high","toolset":["file.read"],"workspace_health":"present","workspace_id":"workspace-1"},"revision":{"actor":"host_operator","content":{"instructions_markdown":"build","memory_markdown":"","skill_refs":[]},"content_hash":"hash-2","created_at_ms":43,"parent_revision":1,"revision":2}}}}"#,
            ),
        ];

        assert_eq!(REQUESTS.len(), 29);
        assert_eq!(RESPONSES.len(), REQUESTS.len());
        for ((request_method, request_fixture), (response_method, response_fixture)) in
            REQUESTS.iter().zip(RESPONSES)
        {
            assert_eq!(request_method, response_method);
            let request = decode_request(request_fixture).unwrap();
            assert_eq!(request.method.as_ref(), Some(request_method));
            assert_eq!(serde_json::to_string(&request).unwrap(), *request_fixture);

            let response: Envelope = serde_json::from_str(response_fixture).unwrap();
            assert_eq!(response.method.as_ref(), Some(response_method));
            assert_eq!(
                response.result.as_ref().map(ProtocolResponse::method),
                Some(*response_method)
            );
            assert_eq!(serde_json::to_string(&response).unwrap(), *response_fixture);
        }
    }

    #[test]
    fn run_cancel_actor_is_additive_and_legacy_compatible() {
        let legacy: RunCancelParams = serde_json::from_str(r#"{"run_id":"run-1"}"#).unwrap();
        assert_eq!(legacy.actor, None);
        assert_eq!(
            serde_json::to_string(&legacy).unwrap(),
            r#"{"run_id":"run-1"}"#
        );

        let attributed = RunCancelParams {
            run_id: "run-1".into(),
            actor: Some("remote_laptop".into()),
        };
        assert_eq!(
            serde_json::to_string(&attributed).unwrap(),
            r#"{"run_id":"run-1","actor":"remote_laptop"}"#
        );
    }

    #[test]
    fn unknown_methods_and_unknown_params_fail_at_the_envelope_boundary() {
        let unknown_method =
            r#"{"v":2,"id":"future_1","kind":"request","method":"future.run","params":{}}"#;
        assert!(serde_json::from_str::<Envelope>(unknown_method).is_err());
        let error = decode_request(unknown_method).unwrap_err();
        assert_eq!(error.error.unwrap().code, ERROR_MALFORMED_REQUEST);

        let unknown_param = r#"{"v":2,"id":"cancel_1","kind":"request","method":"run.cancel","params":{"future":true,"run_id":"run-1"}}"#;
        assert!(serde_json::from_str::<Envelope>(unknown_param).is_err());
        let error = decode_request(unknown_param).unwrap_err();
        assert_eq!(error.method, Some(ProtocolMethod::RunCancel));
        assert_eq!(error.error.unwrap().code, ERROR_MALFORMED_REQUEST);

        let client_minted_envelope = r#"{"v":2,"id":"voice_1","kind":"request","method":"voice.events.commit","params":{"events":[{"event":"voice_spoken","run_id":"run-1","turn_id":"turn-1","ttfa_ms":1,"sentence_count":1,"interrupted_at":null,"sequence":0}],"run_id":"run-1"}}"#;
        let error = decode_request(client_minted_envelope).unwrap_err();
        assert_eq!(error.method, Some(ProtocolMethod::VoiceEventsCommit));
        assert_eq!(error.error.unwrap().code, ERROR_MALFORMED_REQUEST);

        let unknown_result = r#"{"v":2,"id":"voice_1","kind":"response","method":"voice.events.read","result":{"events":[],"future":true,"run_id":"run-1"}}"#;
        assert!(serde_json::from_str::<Envelope>(unknown_result).is_err());
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
    fn profile_control_fixtures_keep_exact_v2_bytes() {
        let fixtures = [
            r#"{"v":2,"id":"create","kind":"request","method":"profile.create","params":{"approval_policy":"prompt","content":{"instructions_markdown":"","memory_markdown":"","skill_refs":[]},"display_name":"builder","model":"gpt-5.6-sol","reasoning_effort":"xhigh","toolset":["file.read"],"workspace_id":"ws-alpha"}}"#,
            r#"{"v":2,"id":"list","kind":"request","method":"profile.list","params":{"limit":10,"workspace_id":"ws-alpha"}}"#,
            r#"{"v":2,"id":"status","kind":"request","method":"profile.status","params":{"profile_id":"profile-builder"}}"#,
            r#"{"v":2,"id":"update","kind":"request","method":"profile.update","params":{"approval_policy":"yolo","content":{"instructions_markdown":"Build it.","memory_markdown":"Remember it.","skill_refs":["skill:rust"]},"model":"gpt-5.6-sol","profile_id":"profile-builder","reasoning_effort":"high","toolset":["file.read"]}}"#,
        ];
        for fixture in fixtures {
            let request = decode_request(fixture).unwrap();
            assert!(matches!(
                request.params,
                Some(
                    ProtocolRequest::ProfileCreate(_)
                        | ProtocolRequest::ProfileList(_)
                        | ProtocolRequest::ProfileStatus(_)
                        | ProtocolRequest::ProfileUpdate(_)
                )
            ));
            assert_eq!(serde_json::to_string(&request).unwrap(), fixture);
        }
    }

    #[test]
    fn thread_management_fixtures_keep_exact_v2_bytes() {
        const SPAWN_START_REQUEST: &str = r#"{"v":2,"id":"spawn_start_1","kind":"request","method":"thread.spawn","params":{"action":"start","approval_policy":"prompt","cwd":"/tmp/work","model":"gpt-5.6-sol","parent_thread_id":"thread_parent","reasoning_effort":"xhigh"}}"#;
        const SPAWN_DECIDE_REQUEST: &str = r#"{"v":2,"id":"spawn_decide_1","kind":"request","method":"thread.spawn","params":{"action":"decide","approval":{"actor":"stdin","decision":"grant"},"spawn_id":"spawn_1"}}"#;
        const SPAWN_REQUIRED_RESPONSE: &str = r#"{"v":2,"id":"spawn_start_1","kind":"response","method":"thread.spawn","result":{"effect":"workspace_write","reason":"thread.spawn requires approval","spawn_id":"spawn_1","status":"approval_required","thread_id":"thread_1"}}"#;
        const STATUS_RESPONSE: &str = r#"{"v":2,"id":"status_1","kind":"response","method":"thread.status","result":{"thread":{"authority":{"approval_policy":"prompt","created_at_ms":42,"cwd":"/tmp/work","home_thread_id":"thread_home","model":"gpt-5.6-sol","parent_thread_id":"thread_parent","profile_id":"profile_builder","profile_revision":1,"reasoning_effort":"xhigh","spawning_actor":"stdin","thread_id":"thread_1","thread_kind":"child"},"live":{"current_turn_id":null,"last_activity_at_ms":47,"live_epoch_id":"live_epoch_1","loaded":true},"return_availability":{"child_returns":2,"parent_answers":1}}}}"#;
        const LIST_RESPONSE: &str = r#"{"v":2,"id":"list_1","kind":"response","method":"thread.list","result":{"threads":[{"authority":{"approval_policy":"prompt","created_at_ms":42,"cwd":"/tmp/work","home_thread_id":"thread_home","model":"gpt-5.6-sol","parent_thread_id":"thread_parent","profile_id":"profile_builder","profile_revision":1,"reasoning_effort":"xhigh","spawning_actor":"stdin","thread_id":"thread_1","thread_kind":"child"},"live":{"current_turn_id":null,"live_epoch_id":"live_epoch_1","loaded":false},"return_availability":{"child_returns":2,"parent_answers":1}}]}}"#;
        const AUTHORITY_REQUEST: &str = r#"{"v":2,"id":"authority_1","kind":"request","method":"thread.authority","params":{"thread_id":"thread_1"}}"#;
        const AUTHORITY_RESPONSE: &str = r#"{"v":2,"id":"authority_1","kind":"response","method":"thread.authority","result":{"authority":{"approval_policy":"prompt","created_at_ms":42,"cwd":"/tmp/work","granted_paths":[{"path":"/tmp/work","writable":true}],"model":"gpt-5.6-sol","network":false,"parent_thread_id":"thread_parent","profile_id":"profile_builder","profile_revision":1,"reasoning_effort":"xhigh","spawning_actor":"stdin","thread_id":"thread_1","thread_kind":"child","toolset":["file.read","file.write"],"worktrees":[]}}}"#;
        const SEND_START_REQUEST: &str = r#"{"v":2,"id":"send_1","kind":"request","method":"thread.send","params":{"controller_id":"terminal_a","message":"inspect it","thread_id":"thread_1"}}"#;
        const SEND_STEER_REQUEST: &str = r#"{"v":2,"id":"send_2","kind":"request","method":"thread.send","params":{"controller_id":"terminal_a","message":"also summarize","thread_id":"thread_1","turn_id":"thread_turn_1"}}"#;
        const SEND_STARTED_RESPONSE: &str = r#"{"v":2,"id":"send_1","kind":"response","method":"thread.send","result":{"status":"started","thread_id":"thread_1","turn_id":"thread_turn_1"}}"#;
        const SEND_STEERED_RESPONSE: &str = r#"{"v":2,"id":"send_2","kind":"response","method":"thread.send","result":{"status":"steered","thread_id":"thread_1","turn_id":"thread_turn_1"}}"#;
        const SEND_REJECTED_RESPONSE: &str = r#"{"v":2,"id":"send_3","kind":"response","method":"thread.send","result":{"reason":"controller_owned","status":"rejected","thread_id":"thread_1","turn_id":"thread_turn_1"}}"#;
        const EVENTS_REQUEST: &str = r#"{"v":2,"id":"events_1","kind":"request","method":"thread.events","params":{"from_offset":0,"limit":128,"thread_id":"thread_1","wait_ms":1000}}"#;
        const EVENTS_RESPONSE: &str = r#"{"v":2,"id":"events_1","kind":"response","method":"thread.events","result":{"current_turn_id":"thread_turn_1","events":[],"from_offset":0,"live_epoch_id":"live_epoch_1","next_offset":0,"thread_id":"thread_1"}}"#;
        const STOP_REQUEST: &str = r#"{"v":2,"id":"stop_1","kind":"request","method":"thread.stop","params":{"actor":"stdin","thread_id":"thread_1"}}"#;
        const STOP_RESPONSE: &str = r#"{"v":2,"id":"stop_1","kind":"response","method":"thread.stop","result":{"status":"stopped","stopped_at_ms":52,"stopped_turn_id":"turn_1","thread_id":"thread_1"}}"#;

        for fixture in [SPAWN_START_REQUEST, SPAWN_DECIDE_REQUEST] {
            let request = decode_request(fixture).unwrap();
            assert_eq!(serde_json::to_string(&request).unwrap(), fixture);
            assert!(matches!(
                request.params.as_ref(),
                Some(ProtocolRequest::ThreadSpawn(
                    ThreadSpawnParams::Start { .. } | ThreadSpawnParams::Decide { .. }
                ))
            ));
        }
        let stop_request = decode_request(STOP_REQUEST).unwrap();
        assert!(matches!(
            stop_request.params.as_ref(),
            Some(ProtocolRequest::ThreadStop(_))
        ));
        assert_eq!(serde_json::to_string(&stop_request).unwrap(), STOP_REQUEST);

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

        let authority = ThreadAuthorityRecord {
            thread_id: "thread_1".into(),
            parent_thread_id: Some("thread_parent".into()),
            spawning_actor: "stdin".into(),
            cwd: Some("/tmp/work".into()),
            agent_id: None,
            profile_id: Some(ProfileId::new("profile_builder").unwrap()),
            profile_revision: Some(1),
            thread_kind: ThreadKind::Child,
            model: "gpt-5.6-sol".into(),
            reasoning_effort: ReasoningEffort::Xhigh,
            approval_policy: ThreadApprovalPolicy::Prompt,
            toolset: vec!["file.read".into(), "file.write".into()],
            worktrees: Vec::new(),
            granted_paths: vec![ThreadGrantedPath {
                path: "/tmp/work".into(),
                writable: true,
            }],
            network: false,
            created_at_ms: 42,
        };
        let thread = ThreadStatus {
            authority: ThreadStatusAuthority {
                thread_id: "thread_1".into(),
                parent_thread_id: Some("thread_parent".into()),
                spawning_actor: "stdin".into(),
                profile_id: Some(ProfileId::new("profile_builder").unwrap()),
                profile_revision: Some(1),
                thread_kind: ThreadKind::Child,
                home_thread_id: Some("thread_home".into()),
                cwd: "/tmp/work".into(),
                model: "gpt-5.6-sol".into(),
                reasoning_effort: ReasoningEffort::Xhigh,
                approval_policy: ThreadApprovalPolicy::Prompt,
                created_at_ms: 42,
            },
            live: ThreadLiveState {
                live_epoch_id: "live_epoch_1".into(),
                loaded: true,
                current_turn_id: None,
                last_activity_at_ms: Some(47),
            },
            return_availability: ThreadReturnAvailability {
                child_returns: 2,
                parent_answers: 1,
            },
        };
        let legacy = serde_json::from_str::<ThreadAuthorityRecord>(
            r#"{"thread_id":"thread_legacy","parent_thread_id":null,"spawning_actor":"stdin","cwd":"/tmp/legacy","model":"gpt-5.6-sol","reasoning_effort":"xhigh","approval_policy":"prompt","created_at_ms":41}"#,
        )
        .unwrap();
        assert!(legacy.agent_id.is_none());
        assert!(legacy.toolset.is_empty());
        assert!(legacy.worktrees.is_empty());
        assert!(legacy.granted_paths.is_empty());
        assert!(!legacy.network);

        let authority_request = decode_request(AUTHORITY_REQUEST).unwrap();
        let Some(ProtocolRequest::ThreadAuthority(params)) = authority_request.params.as_ref()
        else {
            panic!("expected thread.authority request")
        };
        assert_eq!(params.thread_id, "thread_1");
        assert_eq!(
            serde_json::to_string(&authority_request).unwrap(),
            AUTHORITY_REQUEST
        );
        let authority_response = Envelope::response_from(
            Some("authority_1".into()),
            Some("thread.authority".into()),
            ThreadAuthorityResult {
                authority: authority.clone(),
                confinement: None,
            },
        );
        assert_eq!(
            serde_json::to_string(&authority_response).unwrap(),
            AUTHORITY_RESPONSE
        );

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
        unloaded.live.last_activity_at_ms = None;
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
            assert!(matches!(
                request.params.as_ref(),
                Some(ProtocolRequest::ThreadSend(_))
            ));
            assert_eq!(serde_json::to_string(&request).unwrap(), fixture);
        }
        let events_request = decode_request(EVENTS_REQUEST).unwrap();
        assert!(matches!(
            events_request.params.as_ref(),
            Some(ProtocolRequest::ThreadEvents(_))
        ));
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
                live_epoch_id: "live_epoch_1".into(),
                reset: None,
                from_offset: 0,
                next_offset: 0,
                current_turn_id: Some("thread_turn_1".into()),
                events: Vec::new(),
            },
        );
        assert_eq!(serde_json::to_string(&events).unwrap(), EVENTS_RESPONSE);

        let stop = Envelope::response_from(
            Some("stop_1".into()),
            Some("thread.stop".into()),
            ThreadStopResult::Stopped {
                thread_id: "thread_1".into(),
                stopped_turn_id: Some("turn_1".into()),
                stopped_at_ms: 52,
            },
        );
        assert_eq!(serde_json::to_string(&stop).unwrap(), STOP_RESPONSE);
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
        assert_unknown_field_rejected::<ThreadAuthorityParams>(json!({
            "thread_id": "thread_1",
            "future": true
        }));
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
        assert_unknown_field_rejected::<ThreadStopParams>(json!({
            "thread_id": "thread_1",
            "actor": "stdin",
            "future": true
        }));
    }

    #[test]
    fn request_and_error_envelopes_keep_exact_v2_bytes() {
        const RUN_CANCEL_REQUEST: &str = r#"{"v":2,"id":"cancel_1","kind":"request","method":"run.cancel","params":{"run_id":"run_1"}}"#;
        const RUN_FAILED_RESPONSE: &str = r#"{"v":2,"id":"run_1","kind":"error","method":"run.start","error":{"code":"run_failed","message":"synthetic failure"}}"#;

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
    fn daemon_status_known_and_unknown_fixtures_keep_exact_v2_bytes() {
        const STATUS_REQUEST: &str = r#"{"v":2,"id":"status_1","kind":"request","method":"daemon.status","params":{"config_path":"config/plato.toml","session_id":"session_1"}}"#;
        const STATUS_KNOWN_RESPONSE: &str = r#"{"v":2,"id":"status_1","kind":"response","method":"daemon.status","result":{"daemon":{"build_commit":"0123456789abcdef0123456789abcdef01234567","build_date_utc":"2026-08-01","endpoint_path":"/tmp/agent.sock","package_version":"0.1.0","uptime_ms":42,"workspace_id":"work-1234"},"model":{"key_present":true,"provider_kind":"open_router","provider_protocol":"responses","requested_alias":"~openai/gpt-latest","served_model":"openai/gpt-5.5-2026-08-01"},"session":{"core_event_count":17,"human_turn_count":2,"latest_run_id":"run_2","ledger_path":"/tmp/agent.db","session_id":"session_1"},"trust":{"approval_denied_count":1,"approval_granted_count":2,"shell_session_grant":true},"usage":{"last_run":{"input_tokens":7,"output_tokens":3,"unknown_response_count":1},"session":{"input_tokens":17,"output_tokens":8,"unknown_response_count":2}}}}"#;
        const STATUS_UNKNOWN_RESPONSE: &str = r#"{"v":2,"id":"status_2","kind":"response","method":"daemon.status","result":{"daemon":{"build_commit":null,"build_date_utc":null,"endpoint_path":"/tmp/agent.sock","package_version":"0.1.0","uptime_ms":0,"workspace_id":"work-1234"},"model":{"key_present":false,"provider_kind":"open_ai","provider_protocol":"chat_completions","requested_alias":"gpt-5.5","served_model":null},"session":{"core_event_count":0,"human_turn_count":0,"latest_run_id":null,"ledger_path":"/tmp/agent.db","session_id":null},"trust":{"approval_denied_count":0,"approval_granted_count":0,"shell_session_grant":false},"usage":{"last_run":{"input_tokens":0,"output_tokens":0,"unknown_response_count":0},"session":{"input_tokens":0,"output_tokens":0,"unknown_response_count":0}}}}"#;

        let request = decode_request(STATUS_REQUEST).unwrap();
        let Some(ProtocolRequest::DaemonStatus(params)) = request.params.as_ref() else {
            panic!("expected daemon.status request")
        };
        assert_eq!(params.session_id.as_deref(), Some("session_1"));
        assert_eq!(params.config_path.as_deref(), Some("config/plato.toml"));
        assert_eq!(serde_json::to_string(&request).unwrap(), STATUS_REQUEST);

        for fixture in [STATUS_KNOWN_RESPONSE, STATUS_UNKNOWN_RESPONSE] {
            let envelope: Envelope = serde_json::from_str(fixture).unwrap();
            let Some(ProtocolResponse::DaemonStatus(result)) = envelope.result else {
                panic!("expected daemon.status response")
            };
            let rebuilt =
                Envelope::typed_response(envelope.id, ProtocolResponse::DaemonStatus(result));
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
    fn daemon_status_provider_protocols_keep_exact_wire_values() {
        for (protocol, wire_value) in [
            (
                DaemonStatusProviderProtocol::ChatCompletions,
                "chat_completions",
            ),
            (DaemonStatusProviderProtocol::Responses, "responses"),
        ] {
            assert_eq!(protocol.as_str(), wire_value);
            assert_eq!(protocol.to_string(), wire_value);
            assert_eq!(serde_json::to_value(protocol).unwrap(), wire_value);
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
        assert_unknown_field_rejected::<SessionApprovalProfileSetParams>(json!({
            "session_id": "session_1",
            "profile": "yolo",
            "future": true
        }));
        assert_unknown_field_rejected::<DaemonStatusResult>(json!({
            "model": {
                "requested_alias": "gpt-5.5",
                "served_model": null,
                "provider_kind": "open_ai",
                "provider_protocol": "chat_completions",
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
            "provider_protocol": "chat_completions",
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
    fn approval_profiles_keep_exact_closed_wire_values() {
        for (profile, wire_value) in [
            (ApprovalProfile::Prompt, "prompt"),
            (ApprovalProfile::Yolo, "yolo"),
        ] {
            assert_eq!(profile.as_str(), wire_value);
            assert_eq!(profile.to_string(), wire_value);
            assert_eq!(serde_json::to_value(profile).unwrap(), wire_value);
            assert_eq!(
                serde_json::from_value::<ApprovalProfile>(wire_value.into()).unwrap(),
                profile
            );
        }
        for unknown in ["auto", "on", "off", "YOLO"] {
            assert!(serde_json::from_value::<ApprovalProfile>(json!(unknown)).is_err());
        }
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
            assert_eq!(params.actor, None);
            assert_eq!(serde_json::to_string(&params).unwrap(), fixture);
        }
        let attributed: ApprovalDecideParams = serde_json::from_str(
            r#"{"run_id":"run_1","tool_call_id":"call_1","decision":"grant","reason":null,"actor":"jerome"}"#,
        )
        .unwrap();
        assert_eq!(attributed.actor.as_deref(), Some("jerome"));
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
        assert_eq!(legacy.approval_profile, None);
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
            approval_profile: None,
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
    fn approval_profiles_are_additive_on_start_and_append() {
        let start: RunStartParams = serde_json::from_value(json!({
            "question": "hello",
            "approval_profile": "yolo"
        }))
        .unwrap();
        assert_eq!(start.approval_profile, Some(ApprovalProfile::Yolo));

        let legacy_append: MessageAppendParams = serde_json::from_value(json!({
            "message": "again",
            "session_id": "session_1"
        }))
        .unwrap();
        assert_eq!(legacy_append.approval_profile, None);
        assert!(
            serde_json::to_value(legacy_append)
                .unwrap()
                .get("approval_profile")
                .is_none()
        );

        let append: MessageAppendParams = serde_json::from_value(json!({
            "message": "again",
            "session_id": "session_1",
            "approval_profile": "prompt"
        }))
        .unwrap();
        assert_eq!(append.approval_profile, Some(ApprovalProfile::Prompt));
    }

    #[test]
    fn prior_interruption_reference_is_typed_only_on_session_and_thread_continuations() {
        let legacy_append: MessageAppendParams = serde_json::from_value(json!({
            "message": "again",
            "session_id": "session_1"
        }))
        .unwrap();
        assert_eq!(legacy_append.prior_interrupted_run_id, None);
        assert!(
            serde_json::to_value(legacy_append)
                .unwrap()
                .get("prior_interrupted_run_id")
                .is_none()
        );

        let append: MessageAppendParams = serde_json::from_value(json!({
            "message": "same utterance",
            "session_id": "session_1",
            "prior_interrupted_run_id": "run_prior"
        }))
        .unwrap();
        assert_eq!(
            append.prior_interrupted_run_id.as_deref(),
            Some("run_prior")
        );

        let thread: ThreadSendParams = serde_json::from_value(json!({
            "thread_id": "thread_1",
            "controller_id": "terminal_a",
            "message": "same utterance",
            "prior_interrupted_run_id": "run_prior"
        }))
        .unwrap();
        assert_eq!(
            thread.prior_interrupted_run_id.as_deref(),
            Some("run_prior")
        );

        assert!(
            serde_json::from_value::<RunStartParams>(json!({
                "question": "fresh",
                "prior_interrupted_run_id": "run_prior"
            }))
            .is_err()
        );
    }

    #[test]
    fn daemon_status_trust_defaults_legacy_profiles_to_prompt() {
        let legacy: DaemonStatusTrust = serde_json::from_value(json!({
            "approval_granted_count": 0,
            "approval_denied_count": 0,
            "shell_session_grant": false
        }))
        .unwrap();
        assert_eq!(legacy.approval_profile, ApprovalProfile::Prompt);
        assert!(
            serde_json::to_value(legacy)
                .unwrap()
                .get("approval_profile")
                .is_none()
        );

        let yolo = DaemonStatusTrust {
            approval_profile: ApprovalProfile::Yolo,
            ..DaemonStatusTrust::default()
        };
        assert_eq!(
            serde_json::to_value(yolo).unwrap()["approval_profile"],
            "yolo"
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
            r#"{"v":2,"id":"req_1","kind":"request","method":"hello","params":{"workspace_root":"/tmp/work","workspace_id":"work-1234"}}"#,
        )
        .unwrap();

        assert_eq!(envelope.id.as_deref(), Some("req_1"));
        assert_eq!(envelope.method.as_deref(), Some("hello"));
    }

    #[test]
    fn v1_and_v2_reject_each_other_before_method_dispatch() {
        let v1_to_v2 = decode_request(
            r#"{"v":1,"id":"legacy","kind":"request","method":"agent.create","params":{}}"#,
        )
        .unwrap_err();
        assert_eq!(
            serde_json::to_string(&v1_to_v2).unwrap(),
            r#"{"v":2,"id":"legacy","kind":"error","error":{"code":"unsupported_version","message":"unsupported protocol version: 1"}}"#
        );

        let v2_to_v1 = decode_request_for_version(
            r#"{"v":2,"id":"current","kind":"request","method":"hello","params":{"workspace_id":"work","workspace_root":"/tmp/work"}}"#,
            1,
        )
        .unwrap_err();
        assert_eq!(
            serde_json::to_string(&v2_to_v1).unwrap(),
            r#"{"v":1,"id":"current","kind":"error","method":"hello","error":{"code":"unsupported_version","message":"unsupported protocol version: 2"}}"#
        );
    }

    #[test]
    fn response_serializes_without_request_params() {
        let response = Envelope::response(
            Some("req_1".into()),
            Some("hello".into()),
            HelloResult {
                daemon_version: "0.2.0 test test".into(),
                workspace_id: "work-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![CAPABILITY_HELLO],
                daemon_scope: None,
            },
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
            completion_claim: None,
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
    fn completion_claim_roundtrips_and_absent_stays_compatible() {
        let full = CompletionClaim {
            outcome: CompletionOutcome::Done,
            base: Some("abc123".into()),
            head: Some("def456".into()),
            changed_paths: vec!["src/main.rs".into()],
            pr: Some("#42".into()),
            checks: vec!["ci-789".into()],
        };
        let wire = serde_json::to_value(&full).unwrap();
        assert_eq!(
            wire,
            json!({
                "outcome": {"kind": "done"},
                "base": "abc123",
                "head": "def456",
                "changed_paths": ["src/main.rs"],
                "pr": "#42",
                "checks": ["ci-789"]
            })
        );
        assert_eq!(
            serde_json::from_value::<CompletionClaim>(wire).unwrap(),
            full
        );

        let minimal = CompletionClaim {
            outcome: CompletionOutcome::Done,
            base: None,
            head: None,
            changed_paths: vec![],
            pr: None,
            checks: vec![],
        };
        let wire = serde_json::to_value(&minimal).unwrap();
        assert_eq!(wire, json!({"outcome": {"kind": "done"}}));
        assert_eq!(
            serde_json::from_value::<CompletionClaim>(wire).unwrap(),
            minimal
        );

        let blocked = CompletionClaim {
            outcome: CompletionOutcome::Blocked {
                reason: "waiting for review".into(),
            },
            base: None,
            head: None,
            changed_paths: vec![],
            pr: None,
            checks: vec![],
        };
        let wire = serde_json::to_value(&blocked).unwrap();
        assert_eq!(
            wire,
            json!({"outcome": {"kind": "blocked", "reason": "waiting for review"}})
        );

        // Absent claim in RunStartResult: legacy wire decodes cleanly.
        let legacy: RunStartResult = serde_json::from_value(json!({
            "run_id": "run_1",
            "session_id": "s1",
            "ledger_path": "/tmp/db",
            "status": "finished",
            "final_answer": "done"
        }))
        .unwrap();
        assert_eq!(legacy.completion_claim, None);

        // Absent claim in TranscriptReadResult
        let legacy_transcript: TranscriptReadResult = serde_json::from_value(json!({
            "run_id": "run_1",
            "status": "finished",
            "final_answer": "done",
            "transcript": "text"
        }))
        .unwrap();
        assert_eq!(legacy_transcript.completion_claim, None);
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
            completion_claim: None,
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
                "kind": "approval_requested",
                "run_id": "run_1",
                "tool_call_id": "call_3",
                "tool_name": "shell.exec",
                "effect": "external_side_effect",
                "reason": "approval required",
                "yolo_eligible": true
            }),
            json!({
                "kind": "canceled",
                "run_id": "run_1"
            }),
            json!({
                "kind": "completion_claimed",
                "run_id": "run_1",
                "claim": {
                    "outcome": {"kind": "done"},
                    "base": "abc123",
                    "head": "def456",
                    "changed_paths": ["src/main.rs"],
                    "pr": "#42",
                    "checks": ["ci-789"]
                }
            }),
            json!({
                "kind": "completion_claimed",
                "run_id": "run_2",
                "claim": {
                    "outcome": {"kind": "blocked", "reason": "waiting for review"}
                }
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
            json!({"kind": "completion_claimed", "run_id": "run_1"}),
            json!({"payload": "missing kind"}),
        ];

        for fixture in malformed {
            assert!(serde_json::from_value::<StreamEvent>(fixture).is_err());
        }
    }
}
