//! Error types shared across Platonic Core modules.

/// Core error type for Platonic primitives.
#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub enum Error {
    /// Identifier constructor received an empty value.
    #[error("{0} cannot be empty")]
    EmptyIdentifier(&'static str),

    /// A token budget would be exceeded.
    #[error("context budget exceeded: used {used}, budget {budget}")]
    ContextBudgetExceeded {
        /// Estimated tokens, saturated at `u32::MAX` for reporting.
        used: u32,
        /// Maximum tokens allowed by the context pack.
        budget: u32,
    },

    /// A context compaction range was empty or reversed.
    #[error("invalid compaction range: {start}..{end_exclusive}")]
    InvalidCompactionRange {
        /// Zero-based start position in the prior-turn list.
        start: u64,
        /// Exclusive end position in the prior-turn list.
        end_exclusive: u64,
    },

    /// A recorded event sequence number was not the expected next value.
    #[error("event sequence mismatch: expected {expected}, actual {actual}")]
    SequenceMismatch {
        /// Next contiguous sequence number required by the run state.
        expected: u64,
        /// Sequence number carried by the rejected record.
        actual: u64,
    },

    /// An event for one run was applied to another run.
    #[error("run id mismatch: expected {expected}, actual {actual}")]
    RunIdMismatch {
        /// Run already bound to the state machine.
        expected: String,
        /// Run named by the rejected event.
        actual: String,
    },

    /// An event was not legal in the current run phase.
    #[error("invalid transition from {phase} on {event}")]
    InvalidTransition {
        /// Stable diagnostic name of the phase that rejected the event.
        phase: &'static str,
        /// Stable diagnostic name of the rejected event.
        event: &'static str,
    },

    /// A model response did not match the pending model request.
    #[error("model step mismatch: expected {expected}, actual {actual}")]
    StepMismatch {
        /// Step of the pending model request.
        expected: u32,
        /// Step carried by the rejected event.
        actual: u32,
    },

    /// A turn-scoped event did not match the pending turn.
    #[error("turn mismatch: expected {expected}, actual {actual}")]
    TurnMismatch {
        /// Turn currently awaiting the event.
        expected: String,
        /// Turn named by the rejected event.
        actual: String,
    },

    /// A new turn reused the previous concluded turn id.
    #[error("turn id was reused: {turn_id}")]
    TurnReused {
        /// Reused turn identifier.
        turn_id: String,
    },

    /// A tool event did not match the pending tool call.
    #[error("tool call mismatch: expected {expected}, actual {actual}")]
    ToolCallMismatch {
        /// Tool call currently awaiting the event.
        expected: String,
        /// Tool call named by the rejected event.
        actual: String,
    },

    /// A validated tool call did not match any pending model proposal.
    #[error("tool call was not proposed by the model")]
    UnproposedToolCall,
}
