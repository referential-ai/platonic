//! Platonic Core: typed harness primitives for disciplined agent execution.
//!
//! The crate intentionally starts as a small kernel, not an agent app. It models
//! runs, context packs, tool calls, policy decisions, and event-log entries. The
//! default stance is: every side effect is typed, policy-gated, and recorded.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod context;
pub mod error;
pub mod event;
pub mod ids;
pub mod message;
pub mod policy;
pub mod projection;
pub mod run;
pub mod tool;

pub use context::{ContextFragment, ContextLane, ContextPack};
pub use error::Error;
pub use event::{HarnessEvent, ModelUsage, RecordedEvent};
pub use ids::{ActorId, AgentId, ArtifactId, ModelName, RunId, ToolCallId, ToolName, TurnId};
pub use message::{Message, MessageRole};
pub use policy::{EffectClass, PolicyDecision};
pub use projection::{ReadbackEntry, RunReadback};
pub use run::{RunCommand, RunPhase, RunState};
pub use tool::{ResultVisibility, ToolCall, ToolProposal, ToolResult};
