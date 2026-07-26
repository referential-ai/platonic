//! Pure run state machine.

use crate::{
    ContextPack, Error, HarnessEvent, PolicyDecision, RecordedEvent, RunId, ToolCall, ToolCallId,
    ToolProposal, TurnId,
};

/// Host command requested by the run state machine.
#[derive(Clone, Debug, PartialEq)]
pub enum RunCommand {
    /// Ask the host to make a model request.
    RequestModel {
        /// Turn whose bounded context is ready.
        turn_id: TurnId,
        /// Monotonic model step to record with the request and response.
        step: u32,
        /// Validated context to submit to the model provider.
        context: ContextPack,
    },
    /// Ask the host to obtain approval for a tool call.
    AwaitApproval {
        /// Pending call requiring an approval decision.
        call_id: ToolCallId,
        /// Policy explanation to present to the approver.
        reason: String,
    },
    /// Ask the host to execute a validated tool call.
    ExecuteTool {
        /// Approved or policy-allowed call ready for execution.
        call: ToolCall,
    },
}

/// Current phase of one run.
#[derive(Clone, Debug, PartialEq)]
pub enum RunPhase {
    /// No `run_started` event has been applied.
    NotStarted,
    /// The run is waiting for context to be built for the next model turn.
    ReadyForContext,
    /// Context is built and the next command is a model request.
    ReadyToRequestModel {
        /// Turn whose context is ready.
        turn_id: TurnId,
        /// Monotonic model step expected in the request and response events.
        step: u32,
        /// Budget-validated context to send to the model.
        context: ContextPack,
    },
    /// A model request was recorded; the run is waiting for the response.
    AwaitingModelResponse {
        /// Turn awaiting a model response.
        turn_id: TurnId,
        /// Model step the response must match.
        step: u32,
    },
    /// The model response contained at least one tool proposal.
    AwaitingToolCall {
        /// Turn that produced the proposals.
        turn_id: TurnId,
        /// Model-authored proposals awaiting host validation and classification.
        proposals: Vec<ToolProposal>,
    },
    /// A validated tool call is waiting for policy evaluation.
    AwaitingPolicy {
        /// Validated and effect-classified call to evaluate.
        call: ToolCall,
    },
    /// Policy requires approval before tool execution.
    AwaitingApproval {
        /// Call withheld until approval is recorded.
        call: ToolCall,
        /// Policy explanation for requiring approval.
        reason: String,
    },
    /// Policy denied the tool call; the turn is concluded and may continue or fail.
    PolicyDenied {
        /// Call rejected by policy.
        call_id: ToolCallId,
        /// Recorded policy explanation.
        reason: String,
    },
    /// A human or host actor denied the pending approval; the turn is concluded and may continue or fail.
    ApprovalDenied {
        /// Call denied before execution.
        call_id: ToolCallId,
        /// Recorded approval denial explanation.
        reason: String,
    },
    /// The tool call may be executed.
    ReadyToExecuteTool {
        /// Policy-allowed or approved call ready for host execution.
        call: ToolCall,
    },
    /// Tool execution has started.
    ToolRunning {
        /// Call that crossed the host execution boundary.
        call_id: ToolCallId,
    },
    /// A tool failed; the turn is concluded and may continue or fail.
    ToolFailed {
        /// Call whose execution failed.
        call_id: ToolCallId,
        /// Recorded host failure explanation.
        reason: String,
    },
    /// The current turn completed successfully; the host may finish or continue.
    TurnConcluded,
    /// The run finished successfully.
    Finished,
    /// The run finished unsuccessfully.
    Failed {
        /// Durable terminal failure explanation.
        reason: String,
    },
}

/// Durable state derived by replaying one run's ordered event ledger.
///
/// The state machine performs no IO. Hosts inspect [`Self::pending_command`], perform
/// the requested effect, and then apply the resulting recorded event.
///
/// # Examples
///
/// A tool turn advances only after the proposed call is validated, approved,
/// executed, and recorded:
///
/// ```
/// use platonic_core::*;
/// use serde_json::json;
///
/// # fn main() -> Result<(), Error> {
/// let run_id = RunId::new("run-1")?;
/// let turn_id = TurnId::new("turn-1")?;
/// let call_id = ToolCallId::new("call-1")?;
/// let tool = ToolName::new("file.write")?;
/// let input = json!({"path": "note.txt", "content": "done"});
/// let proposal = ToolProposal {
///     tool: tool.clone(),
///     input: input.clone(),
/// };
/// let call = ToolCall {
///     id: call_id.clone(),
///     tool,
///     effect: EffectClass::WorkspaceWrite,
///     input,
/// };
///
/// let events = vec![
///     HarnessEvent::RunStarted {
///         run_id: run_id.clone(),
///         agent_id: AgentId::new("agent-1")?,
///     },
///     HarnessEvent::ContextBuilt {
///         run_id: run_id.clone(),
///         turn_id: turn_id.clone(),
///         context: ContextPack { token_budget: 10, fragments: vec![] },
///     },
///     HarnessEvent::ModelRequested {
///         run_id: run_id.clone(),
///         turn_id: turn_id.clone(),
///         step: 0,
///         model: ModelName::new("model-1")?,
///     },
///     HarnessEvent::ModelResponded {
///         run_id: run_id.clone(),
///         turn_id: turn_id.clone(),
///         step: 0,
///         output: Message {
///             role: MessageRole::Assistant,
///             content: "I will write the file.".into(),
///         },
///         proposed_calls: vec![proposal],
///         usage: ModelUsage { input_tokens: 3, output_tokens: 5 },
///     },
///     HarnessEvent::ToolCallProposed {
///         run_id: run_id.clone(),
///         turn_id,
///         call: call.clone(),
///     },
///     HarnessEvent::PolicyEvaluated {
///         run_id: run_id.clone(),
///         call_id: call_id.clone(),
///         decision: PolicyDecision::RequireApproval {
///             reason: "workspace write".into(),
///         },
///     },
///     HarnessEvent::ApprovalGranted {
///         run_id: run_id.clone(),
///         call_id: call_id.clone(),
///         actor_id: ActorId::new("human-1")?,
///     },
///     HarnessEvent::ToolStarted {
///         run_id: run_id.clone(),
///         call_id: call_id.clone(),
///     },
///     HarnessEvent::ToolFinished {
///         run_id: run_id.clone(),
///         result: ToolResult {
///             call_id,
///             summary: "wrote note.txt".into(),
///             data: json!({}),
///             artifacts: vec![],
///             visibility: ResultVisibility::Both,
///         },
///     },
///     HarnessEvent::RunFinished { run_id },
/// ];
///
/// let mut state = RunState::new();
/// for (seq, event) in events.into_iter().enumerate() {
///     state.apply(&RecordedEvent {
///         seq: seq as u64,
///         occurred_at_ms: 0,
///         event,
///     })?;
///     if seq == 5 {
///         assert!(matches!(state.pending_command(), Some(RunCommand::AwaitApproval { .. })));
///     }
///     if seq == 6 {
///         assert!(matches!(state.pending_command(), Some(RunCommand::ExecuteTool { .. })));
///     }
/// }
/// assert_eq!(state.phase(), &RunPhase::Finished);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct RunState {
    run_id: Option<RunId>,
    next_seq: u64,
    next_model_step: u32,
    last_turn_id: Option<TurnId>,
    pending_compaction_turn_id: Option<TurnId>,
    phase: RunPhase,
}

impl Default for RunState {
    fn default() -> Self {
        Self::new()
    }
}

impl RunState {
    /// Creates an unbound state expecting sequence zero and `run_started`.
    pub fn new() -> Self {
        Self {
            run_id: None,
            next_seq: 0,
            next_model_step: 0,
            last_turn_id: None,
            pending_compaction_turn_id: None,
            phase: RunPhase::NotStarted,
        }
    }

    /// Returns the bound run id, or `None` before `run_started` is applied.
    pub fn run_id(&self) -> Option<&RunId> {
        self.run_id.as_ref()
    }

    /// Returns the next contiguous per-run sequence number.
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Returns the phase derived from all successfully applied events.
    pub fn phase(&self) -> &RunPhase {
        &self.phase
    }

    /// Derives the pending host IO command without mutating run state.
    pub fn pending_command(&self) -> Option<RunCommand> {
        match &self.phase {
            RunPhase::ReadyToRequestModel {
                turn_id,
                step,
                context,
            } => Some(RunCommand::RequestModel {
                turn_id: turn_id.clone(),
                step: *step,
                context: context.clone(),
            }),
            RunPhase::AwaitingApproval { call, reason } => Some(RunCommand::AwaitApproval {
                call_id: call.id.clone(),
                reason: reason.clone(),
            }),
            RunPhase::ReadyToExecuteTool { call } => {
                Some(RunCommand::ExecuteTool { call: call.clone() })
            }
            _ => None,
        }
    }

    /// Validates and applies one event, advancing the sequence only on success.
    pub fn apply(&mut self, record: &RecordedEvent) -> Result<(), Error> {
        if record.seq != self.next_seq {
            return Err(Error::SequenceMismatch {
                expected: self.next_seq,
                actual: record.seq,
            });
        }

        if let Some(expected) = &self.run_id {
            let actual = record.event.run_id();
            if actual != expected {
                return Err(Error::RunIdMismatch {
                    expected: expected.to_string(),
                    actual: actual.to_string(),
                });
            }
        }

        self.apply_event(&record.event)?;
        self.next_seq += 1;
        Ok(())
    }

    fn apply_event(&mut self, event: &HarnessEvent) -> Result<(), Error> {
        if let Some(expected_turn_id) = &self.pending_compaction_turn_id {
            match event {
                HarnessEvent::ContextBuilt { turn_id, .. } => {
                    ensure_turn(expected_turn_id, turn_id)?;
                }
                HarnessEvent::RunFailed { .. } => {}
                _ => return Err(invalid(&self.phase, event)),
            }
        }

        match (&self.phase, event) {
            (RunPhase::NotStarted, HarnessEvent::RunStarted { run_id, .. }) => {
                self.run_id = Some(run_id.clone());
                self.phase = RunPhase::ReadyForContext;
                Ok(())
            }
            (
                RunPhase::ReadyForContext
                | RunPhase::TurnConcluded
                | RunPhase::PolicyDenied { .. }
                | RunPhase::ApprovalDenied { .. }
                | RunPhase::ToolFailed { .. },
                HarnessEvent::ContextBuilt {
                    turn_id, context, ..
                },
            ) => {
                self.start_turn(turn_id, context)?;
                self.pending_compaction_turn_id = None;
                Ok(())
            }
            (
                RunPhase::ReadyForContext
                | RunPhase::TurnConcluded
                | RunPhase::PolicyDenied { .. }
                | RunPhase::ApprovalDenied { .. }
                | RunPhase::ToolFailed { .. },
                HarnessEvent::ContextCompacted {
                    turn_id,
                    dropped_turn_start,
                    dropped_turn_end_exclusive,
                    ..
                },
            ) => {
                ensure_compaction_range(*dropped_turn_start, *dropped_turn_end_exclusive)?;
                self.pending_compaction_turn_id = Some(turn_id.clone());
                Ok(())
            }
            (
                RunPhase::ReadyToRequestModel {
                    turn_id,
                    step,
                    context: _,
                },
                HarnessEvent::ModelRequested {
                    turn_id: actual_turn_id,
                    step: actual_step,
                    ..
                },
            ) => {
                ensure_turn(turn_id, actual_turn_id)?;
                ensure_step(*step, *actual_step)?;
                self.phase = RunPhase::AwaitingModelResponse {
                    turn_id: turn_id.clone(),
                    step: *step,
                };
                Ok(())
            }
            (
                RunPhase::AwaitingModelResponse { turn_id, step },
                HarnessEvent::ModelResponded {
                    turn_id: actual_turn_id,
                    step: actual_step,
                    proposed_calls,
                    ..
                },
            ) => {
                ensure_turn(turn_id, actual_turn_id)?;
                ensure_step(*step, *actual_step)?;
                self.next_model_step += 1;
                self.phase = if proposed_calls.is_empty() {
                    RunPhase::TurnConcluded
                } else {
                    RunPhase::AwaitingToolCall {
                        turn_id: turn_id.clone(),
                        proposals: proposed_calls.clone(),
                    }
                };
                Ok(())
            }
            (
                RunPhase::AwaitingToolCall { turn_id, proposals },
                HarnessEvent::ToolCallProposed {
                    turn_id: actual_turn_id,
                    call,
                    ..
                },
            ) => {
                ensure_turn(turn_id, actual_turn_id)?;
                ensure_proposed(proposals, call)?;
                self.phase = RunPhase::AwaitingPolicy { call: call.clone() };
                Ok(())
            }
            (
                RunPhase::AwaitingPolicy { call },
                HarnessEvent::PolicyEvaluated {
                    call_id, decision, ..
                },
            ) => {
                ensure_call(&call.id, call_id)?;
                match decision {
                    PolicyDecision::Allow => {
                        self.phase = RunPhase::ReadyToExecuteTool { call: call.clone() };
                    }
                    PolicyDecision::RequireApproval { reason } => {
                        self.phase = RunPhase::AwaitingApproval {
                            call: call.clone(),
                            reason: reason.clone(),
                        };
                    }
                    PolicyDecision::Deny { reason } => {
                        self.phase = RunPhase::PolicyDenied {
                            call_id: call.id.clone(),
                            reason: reason.clone(),
                        };
                    }
                }
                Ok(())
            }
            (
                RunPhase::AwaitingApproval { call, .. },
                HarnessEvent::ApprovalGranted { call_id, .. },
            ) => {
                ensure_call(&call.id, call_id)?;
                self.phase = RunPhase::ReadyToExecuteTool { call: call.clone() };
                Ok(())
            }
            (
                RunPhase::AwaitingApproval { call, .. },
                HarnessEvent::ApprovalDenied {
                    call_id, reason, ..
                },
            ) => {
                ensure_call(&call.id, call_id)?;
                self.phase = RunPhase::ApprovalDenied {
                    call_id: call.id.clone(),
                    reason: reason.clone(),
                };
                Ok(())
            }
            (RunPhase::ReadyToExecuteTool { call }, HarnessEvent::ToolStarted { call_id, .. }) => {
                ensure_call(&call.id, call_id)?;
                self.phase = RunPhase::ToolRunning {
                    call_id: call.id.clone(),
                };
                Ok(())
            }
            (RunPhase::ToolRunning { call_id }, HarnessEvent::ToolFinished { result, .. }) => {
                ensure_call(call_id, &result.call_id)?;
                self.phase = RunPhase::TurnConcluded;
                Ok(())
            }
            (
                RunPhase::ToolRunning { call_id },
                HarnessEvent::ToolFailed {
                    call_id: actual_call_id,
                    reason,
                    ..
                },
            ) => {
                ensure_call(call_id, actual_call_id)?;
                self.phase = RunPhase::ToolFailed {
                    call_id: call_id.clone(),
                    reason: reason.clone(),
                };
                Ok(())
            }
            (RunPhase::TurnConcluded, HarnessEvent::RunFinished { .. }) => {
                self.phase = RunPhase::Finished;
                Ok(())
            }
            (phase, HarnessEvent::RunFailed { reason, .. }) if phase.can_fail() => {
                self.pending_compaction_turn_id = None;
                self.phase = RunPhase::Failed {
                    reason: reason.clone(),
                };
                Ok(())
            }
            (RunPhase::Finished | RunPhase::Failed { .. }, _) => Err(invalid(&self.phase, event)),
            _ => Err(invalid(&self.phase, event)),
        }
    }

    fn start_turn(&mut self, turn_id: &TurnId, context: &ContextPack) -> Result<(), Error> {
        ensure_new_turn(self.last_turn_id.as_ref(), turn_id)?;
        context.validate_budget()?;
        self.last_turn_id = Some(turn_id.clone());
        self.phase = RunPhase::ReadyToRequestModel {
            turn_id: turn_id.clone(),
            step: self.next_model_step,
            context: context.clone(),
        };
        Ok(())
    }
}

impl RunPhase {
    fn name(&self) -> &'static str {
        match self {
            Self::NotStarted => "not_started",
            Self::ReadyForContext => "ready_for_context",
            Self::ReadyToRequestModel { .. } => "ready_to_request_model",
            Self::AwaitingModelResponse { .. } => "awaiting_model_response",
            Self::AwaitingToolCall { .. } => "awaiting_tool_call",
            Self::AwaitingPolicy { .. } => "awaiting_policy",
            Self::AwaitingApproval { .. } => "awaiting_approval",
            Self::PolicyDenied { .. } => "policy_denied",
            Self::ApprovalDenied { .. } => "approval_denied",
            Self::ReadyToExecuteTool { .. } => "ready_to_execute_tool",
            Self::ToolRunning { .. } => "tool_running",
            Self::ToolFailed { .. } => "tool_failed",
            Self::TurnConcluded => "turn_concluded",
            Self::Finished => "finished",
            Self::Failed { .. } => "failed",
        }
    }

    fn can_fail(&self) -> bool {
        !matches!(
            self,
            Self::NotStarted | Self::Finished | Self::Failed { .. }
        )
    }
}

fn ensure_turn(expected: &TurnId, actual: &TurnId) -> Result<(), Error> {
    if expected == actual {
        return Ok(());
    }
    Err(Error::TurnMismatch {
        expected: expected.to_string(),
        actual: actual.to_string(),
    })
}

fn ensure_new_turn(previous: Option<&TurnId>, actual: &TurnId) -> Result<(), Error> {
    if previous.is_some_and(|previous| previous == actual) {
        return Err(Error::TurnReused {
            turn_id: actual.to_string(),
        });
    }
    Ok(())
}

fn ensure_compaction_range(start: u64, end_exclusive: u64) -> Result<(), Error> {
    if start < end_exclusive {
        return Ok(());
    }
    Err(Error::InvalidCompactionRange {
        start,
        end_exclusive,
    })
}

fn ensure_step(expected: u32, actual: u32) -> Result<(), Error> {
    if expected == actual {
        return Ok(());
    }
    Err(Error::StepMismatch { expected, actual })
}

fn ensure_call(expected: &ToolCallId, actual: &ToolCallId) -> Result<(), Error> {
    if expected == actual {
        return Ok(());
    }
    Err(Error::ToolCallMismatch {
        expected: expected.to_string(),
        actual: actual.to_string(),
    })
}

fn ensure_proposed(proposals: &[ToolProposal], call: &ToolCall) -> Result<(), Error> {
    if proposals
        .iter()
        .any(|proposal| proposal.tool == call.tool && proposal.input == call.input)
    {
        return Ok(());
    }
    Err(Error::UnproposedToolCall)
}

fn invalid(phase: &RunPhase, event: &HarnessEvent) -> Error {
    Error::InvalidTransition {
        phase: phase.name(),
        event: event.name(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ActorId, AgentId, ContextFragment, ContextLane, EffectClass, Message, MessageRole,
        ModelName, ModelUsage, ResultVisibility, ToolName, ToolProposal, ToolResult,
    };
    use serde_json::json;

    fn run_id() -> RunId {
        RunId::new("run_1").unwrap()
    }

    fn other_run_id() -> RunId {
        RunId::new("run_2").unwrap()
    }

    fn agent_id() -> AgentId {
        AgentId::new("agent_1").unwrap()
    }

    fn turn_id() -> TurnId {
        TurnId::new("turn_1").unwrap()
    }

    fn other_turn_id() -> TurnId {
        TurnId::new("turn_2").unwrap()
    }

    fn third_turn_id() -> TurnId {
        TurnId::new("turn_3").unwrap()
    }

    fn call_id() -> ToolCallId {
        ToolCallId::new("call_1").unwrap()
    }

    fn actor_id() -> ActorId {
        ActorId::new("human_1").unwrap()
    }

    fn context(tokens: u32) -> ContextPack {
        context_with_content(tokens, "read README")
    }

    fn context_with_content(tokens: u32, content: &str) -> ContextPack {
        ContextPack {
            token_budget: 100,
            fragments: vec![ContextFragment {
                lane: ContextLane::CurrentTask,
                source: "user".into(),
                content: content.into(),
                estimated_tokens: tokens,
            }],
        }
    }

    fn proposal() -> ToolProposal {
        ToolProposal {
            tool: ToolName::new("file.read").unwrap(),
            input: json!({ "path": "README.md" }),
        }
    }

    fn write_proposal() -> ToolProposal {
        ToolProposal {
            tool: ToolName::new("file.write").unwrap(),
            input: json!({ "path": "README.md", "content": "updated" }),
        }
    }

    fn call(effect: EffectClass) -> ToolCall {
        ToolCall {
            id: call_id(),
            tool: ToolName::new("file.read").unwrap(),
            effect,
            input: json!({ "path": "README.md" }),
        }
    }

    fn write_call(effect: EffectClass) -> ToolCall {
        ToolCall {
            id: call_id(),
            tool: ToolName::new("file.write").unwrap(),
            effect,
            input: json!({ "path": "README.md", "content": "updated" }),
        }
    }

    fn result() -> ToolResult {
        ToolResult {
            call_id: call_id(),
            summary: "read README".into(),
            data: json!({ "bytes": 123 }),
            artifacts: vec![],
            visibility: ResultVisibility::Both,
        }
    }

    fn usage() -> ModelUsage {
        ModelUsage {
            input_tokens: 50,
            output_tokens: 10,
        }
    }

    fn rec(seq: u64, event: HarnessEvent) -> RecordedEvent {
        RecordedEvent {
            seq,
            occurred_at_ms: 1_700_000_000_000 + seq,
            event,
        }
    }

    fn start_event(seq: u64) -> RecordedEvent {
        rec(
            seq,
            HarnessEvent::RunStarted {
                run_id: run_id(),
                agent_id: agent_id(),
            },
        )
    }

    fn context_event(seq: u64) -> RecordedEvent {
        context_event_for(seq, turn_id(), "read README")
    }

    fn second_context_event(seq: u64) -> RecordedEvent {
        context_event_for(seq, other_turn_id(), "tool result: read README")
    }

    fn third_context_event(seq: u64) -> RecordedEvent {
        context_event_for(seq, third_turn_id(), "tool failed: retry differently")
    }

    fn context_event_for(seq: u64, turn_id: TurnId, content: &str) -> RecordedEvent {
        rec(
            seq,
            HarnessEvent::ContextBuilt {
                run_id: run_id(),
                turn_id,
                context: context_with_content(10, content),
            },
        )
    }

    fn compaction_event(seq: u64, turn_id: TurnId) -> RecordedEvent {
        compaction_event_for(seq, turn_id, 0, 2)
    }

    fn compaction_event_for(
        seq: u64,
        turn_id: TurnId,
        dropped_turn_start: u64,
        dropped_turn_end_exclusive: u64,
    ) -> RecordedEvent {
        rec(
            seq,
            HarnessEvent::ContextCompacted {
                run_id: run_id(),
                turn_id,
                estimated_tokens_before: 160,
                estimated_tokens_after: 80,
                dropped_turn_start,
                dropped_turn_end_exclusive,
            },
        )
    }

    fn model_requested(seq: u64) -> RecordedEvent {
        model_requested_for(seq, turn_id(), 0)
    }

    fn second_model_requested(seq: u64) -> RecordedEvent {
        model_requested_for(seq, other_turn_id(), 1)
    }

    fn third_model_requested(seq: u64) -> RecordedEvent {
        model_requested_for(seq, third_turn_id(), 1)
    }

    fn model_requested_for(seq: u64, turn_id: TurnId, step: u32) -> RecordedEvent {
        rec(
            seq,
            HarnessEvent::ModelRequested {
                run_id: run_id(),
                turn_id,
                step,
                model: ModelName::new("claude-fable-5").unwrap(),
            },
        )
    }

    fn model_responded(seq: u64) -> RecordedEvent {
        model_responded_with(
            seq,
            turn_id(),
            0,
            "I should read the file.",
            vec![proposal()],
        )
    }

    fn model_responded_with(
        seq: u64,
        turn_id: TurnId,
        step: u32,
        output: &str,
        proposed_calls: Vec<ToolProposal>,
    ) -> RecordedEvent {
        rec(
            seq,
            HarnessEvent::ModelResponded {
                run_id: run_id(),
                turn_id,
                step,
                output: Message {
                    role: MessageRole::Assistant,
                    content: output.into(),
                },
                proposed_calls,
                usage: usage(),
            },
        )
    }

    fn second_model_answer(seq: u64) -> RecordedEvent {
        model_responded_with(seq, other_turn_id(), 1, "The README was read.", vec![])
    }

    fn third_model_answer(seq: u64) -> RecordedEvent {
        model_responded_with(seq, third_turn_id(), 1, "Recovered from failure.", vec![])
    }

    fn tool_proposed(seq: u64, effect: EffectClass) -> RecordedEvent {
        rec(
            seq,
            HarnessEvent::ToolCallProposed {
                run_id: run_id(),
                turn_id: turn_id(),
                call: call(effect),
            },
        )
    }

    fn unproposed_tool(seq: u64) -> RecordedEvent {
        rec(
            seq,
            HarnessEvent::ToolCallProposed {
                run_id: run_id(),
                turn_id: turn_id(),
                call: ToolCall {
                    id: call_id(),
                    tool: ToolName::new("file.write").unwrap(),
                    effect: EffectClass::WorkspaceWrite,
                    input: json!({ "path": "README.md", "content": "surprise" }),
                },
            },
        )
    }

    fn require_approval(seq: u64) -> RecordedEvent {
        rec(
            seq,
            HarnessEvent::PolicyEvaluated {
                run_id: run_id(),
                call_id: call_id(),
                decision: PolicyDecision::RequireApproval {
                    reason: "workspace write needs approval".into(),
                },
            },
        )
    }

    fn deny_policy(seq: u64) -> RecordedEvent {
        rec(
            seq,
            HarnessEvent::PolicyEvaluated {
                run_id: run_id(),
                call_id: call_id(),
                decision: PolicyDecision::Deny {
                    reason: "policy denied".into(),
                },
            },
        )
    }

    fn allow_policy(seq: u64) -> RecordedEvent {
        rec(
            seq,
            HarnessEvent::PolicyEvaluated {
                run_id: run_id(),
                call_id: call_id(),
                decision: PolicyDecision::Allow,
            },
        )
    }

    fn approval_granted(seq: u64) -> RecordedEvent {
        rec(
            seq,
            HarnessEvent::ApprovalGranted {
                run_id: run_id(),
                call_id: call_id(),
                actor_id: actor_id(),
            },
        )
    }

    fn tool_started(seq: u64) -> RecordedEvent {
        rec(
            seq,
            HarnessEvent::ToolStarted {
                run_id: run_id(),
                call_id: call_id(),
            },
        )
    }

    fn tool_finished(seq: u64) -> RecordedEvent {
        rec(
            seq,
            HarnessEvent::ToolFinished {
                run_id: run_id(),
                result: result(),
            },
        )
    }

    fn tool_failed(seq: u64) -> RecordedEvent {
        rec(
            seq,
            HarnessEvent::ToolFailed {
                run_id: run_id(),
                call_id: call_id(),
                reason: "tool crashed".into(),
            },
        )
    }

    fn base_until_approval_required() -> Vec<RecordedEvent> {
        vec![
            start_event(0),
            context_event(1),
            model_requested(2),
            model_responded(3),
            tool_proposed(4, EffectClass::WorkspaceWrite),
            require_approval(5),
        ]
    }

    fn apply_all(events: &[RecordedEvent]) -> RunState {
        let mut state = RunState::new();
        for event in events {
            state.apply(event).unwrap();
        }
        state
    }

    #[test]
    fn compaction_precedes_matching_context_in_every_context_phase() {
        let concluded = apply_all(&[
            start_event(0),
            context_event(1),
            model_requested(2),
            model_responded_with(3, turn_id(), 0, "done", vec![]),
        ]);
        let policy_denied = apply_all(&[
            start_event(0),
            context_event(1),
            model_requested(2),
            model_responded(3),
            tool_proposed(4, EffectClass::ReadOnly),
            deny_policy(5),
        ]);
        let approval_denied = apply_all(&[
            start_event(0),
            context_event(1),
            model_requested(2),
            model_responded(3),
            tool_proposed(4, EffectClass::WorkspaceWrite),
            require_approval(5),
            rec(
                6,
                HarnessEvent::ApprovalDenied {
                    run_id: run_id(),
                    call_id: call_id(),
                    actor_id: actor_id(),
                    reason: "not approved".into(),
                },
            ),
        ]);
        let tool_failed_state = apply_all(&[
            start_event(0),
            context_event(1),
            model_requested(2),
            model_responded(3),
            tool_proposed(4, EffectClass::ReadOnly),
            allow_policy(5),
            tool_started(6),
            tool_failed(7),
        ]);

        for mut state in [
            apply_all(&[start_event(0)]),
            concluded,
            policy_denied,
            approval_denied,
            tool_failed_state,
        ] {
            let phase = state.phase().clone();
            let seq = state.next_seq();

            state
                .apply(&compaction_event(seq, other_turn_id()))
                .unwrap();

            assert_eq!(state.phase(), &phase);
            assert!(state.pending_command().is_none());
            state.apply(&second_context_event(seq + 1)).unwrap();
            assert!(matches!(
                state.phase(),
                RunPhase::ReadyToRequestModel { turn_id, .. } if turn_id == &other_turn_id()
            ));
        }
    }

    #[test]
    fn pending_compaction_rejects_duplicate_wrong_turn_and_other_events() {
        let mut state = apply_all(&[start_event(0)]);
        state.apply(&compaction_event(1, turn_id())).unwrap();

        let mut duplicate = state.clone();
        assert_eq!(
            duplicate
                .apply(&compaction_event(2, turn_id()))
                .unwrap_err(),
            Error::InvalidTransition {
                phase: "ready_for_context",
                event: "context_compacted"
            }
        );

        let mut wrong_turn = state.clone();
        assert_eq!(
            wrong_turn.apply(&second_context_event(2)).unwrap_err(),
            Error::TurnMismatch {
                expected: "turn_1".into(),
                actual: "turn_2".into()
            }
        );

        let mut wrong_event = state.clone();
        assert_eq!(
            wrong_event.apply(&model_requested(2)).unwrap_err(),
            Error::InvalidTransition {
                phase: "ready_for_context",
                event: "model_requested"
            }
        );

        state.apply(&context_event(2)).unwrap();
        state.apply(&model_requested(3)).unwrap();
        state
            .apply(&model_responded_with(4, turn_id(), 0, "done", vec![]))
            .unwrap();
        state.apply(&compaction_event(5, other_turn_id())).unwrap();
        assert_eq!(state.phase(), &RunPhase::TurnConcluded);
    }

    #[test]
    fn compaction_range_must_drop_at_least_one_turn() {
        let state = apply_all(&[start_event(0)]);

        for (start, end_exclusive) in [(0, 0), (3, 2)] {
            let mut attempted = state.clone();
            assert_eq!(
                attempted
                    .apply(&compaction_event_for(1, turn_id(), start, end_exclusive))
                    .unwrap_err(),
                Error::InvalidCompactionRange {
                    start,
                    end_exclusive
                }
            );
            assert_eq!(attempted.next_seq(), 1);
        }
    }

    #[test]
    fn pending_compaction_may_end_with_run_failure() {
        let mut state = apply_all(&[start_event(0)]);
        state.apply(&compaction_event(1, turn_id())).unwrap();

        state
            .apply(&rec(
                2,
                HarnessEvent::RunFailed {
                    run_id: run_id(),
                    reason: "context build failed".into(),
                },
            ))
            .unwrap();

        assert_eq!(
            state.phase(),
            &RunPhase::Failed {
                reason: "context build failed".into()
            }
        );
        assert!(state.pending_command().is_none());
    }

    #[test]
    fn compaction_is_rejected_out_of_phase_and_after_terminal() {
        let states = [
            ("not_started", RunState::new()),
            (
                "ready_to_request_model",
                apply_all(&[start_event(0), context_event(1)]),
            ),
            (
                "finished",
                apply_all(&[
                    start_event(0),
                    context_event(1),
                    model_requested(2),
                    model_responded_with(3, turn_id(), 0, "done", vec![]),
                    rec(4, HarnessEvent::RunFinished { run_id: run_id() }),
                ]),
            ),
            (
                "failed",
                apply_all(&[
                    start_event(0),
                    rec(
                        1,
                        HarnessEvent::RunFailed {
                            run_id: run_id(),
                            reason: "failed".into(),
                        },
                    ),
                ]),
            ),
        ];

        for (phase, mut state) in states {
            let seq = state.next_seq();
            assert_eq!(
                state
                    .apply(&compaction_event(seq, other_turn_id()))
                    .unwrap_err(),
                Error::InvalidTransition {
                    phase,
                    event: "context_compacted"
                }
            );
        }
    }

    #[test]
    fn happy_path_emits_expected_commands_and_finishes() {
        let events = vec![
            start_event(0),
            context_event(1),
            model_requested(2),
            model_responded(3),
            tool_proposed(4, EffectClass::WorkspaceWrite),
            require_approval(5),
            approval_granted(6),
            rec(
                7,
                HarnessEvent::ToolStarted {
                    run_id: run_id(),
                    call_id: call_id(),
                },
            ),
            rec(
                8,
                HarnessEvent::ToolFinished {
                    run_id: run_id(),
                    result: result(),
                },
            ),
            rec(9, HarnessEvent::RunFinished { run_id: run_id() }),
        ];

        let mut state = RunState::new();
        state.apply(&events[0]).unwrap();
        state.apply(&events[1]).unwrap();
        assert!(matches!(
            state.pending_command(),
            Some(RunCommand::RequestModel { step: 0, .. })
        ));
        state.apply(&events[2]).unwrap();
        state.apply(&events[3]).unwrap();
        state.apply(&events[4]).unwrap();
        state.apply(&events[5]).unwrap();
        assert!(matches!(
            state.pending_command(),
            Some(RunCommand::AwaitApproval { .. })
        ));
        state.apply(&events[6]).unwrap();
        assert!(matches!(
            state.pending_command(),
            Some(RunCommand::ExecuteTool { .. })
        ));
        for event in &events[7..] {
            state.apply(event).unwrap();
        }

        assert_eq!(state.phase(), &RunPhase::Finished);
        assert_eq!(state.next_seq(), 10);
        assert_eq!(apply_all(&events), state);
    }

    #[test]
    fn tool_result_can_feed_a_second_model_turn() {
        let events = vec![
            start_event(0),
            context_event(1),
            model_requested(2),
            model_responded(3),
            tool_proposed(4, EffectClass::ReadOnly),
            allow_policy(5),
            tool_started(6),
            tool_finished(7),
            second_context_event(8),
            second_model_requested(9),
            second_model_answer(10),
            rec(11, HarnessEvent::RunFinished { run_id: run_id() }),
        ];

        let mut state = RunState::new();
        for event in &events[..9] {
            state.apply(event).unwrap();
        }
        assert!(matches!(
            state.pending_command(),
            Some(RunCommand::RequestModel { step: 1, .. })
        ));

        for event in &events[9..] {
            state.apply(event).unwrap();
        }

        assert_eq!(state.phase(), &RunPhase::Finished);
        assert_eq!(state.next_seq(), 12);
        assert_eq!(apply_all(&events), state);
    }

    #[test]
    fn policy_denial_can_feed_a_second_model_turn_without_tool_execution() {
        let events = vec![
            start_event(0),
            context_event(1),
            model_requested(2),
            model_responded(3),
            tool_proposed(4, EffectClass::SecretAccess),
            deny_policy(5),
            second_context_event(6),
            second_model_requested(7),
            second_model_answer(8),
            rec(9, HarnessEvent::RunFinished { run_id: run_id() }),
        ];

        let mut state = RunState::new();
        for event in &events[..6] {
            state.apply(event).unwrap();
        }
        assert!(state.pending_command().is_none());
        assert!(matches!(state.phase(), RunPhase::PolicyDenied { .. }));

        for event in &events[6..] {
            state.apply(event).unwrap();
        }

        assert_eq!(state.phase(), &RunPhase::Finished);
        assert_eq!(apply_all(&events), state);
    }

    #[test]
    fn approval_denial_can_feed_a_second_model_turn_without_tool_execution() {
        let mut events = base_until_approval_required();
        events.push(rec(
            6,
            HarnessEvent::ApprovalDenied {
                run_id: run_id(),
                call_id: call_id(),
                actor_id: actor_id(),
                reason: "no".into(),
            },
        ));
        events.push(second_context_event(7));
        events.push(second_model_requested(8));
        events.push(second_model_answer(9));
        events.push(rec(10, HarnessEvent::RunFinished { run_id: run_id() }));

        let mut state = RunState::new();
        for event in &events[..7] {
            state.apply(event).unwrap();
        }
        assert!(state.pending_command().is_none());
        assert!(matches!(state.phase(), RunPhase::ApprovalDenied { .. }));

        for event in &events[7..] {
            state.apply(event).unwrap();
        }

        assert_eq!(state.phase(), &RunPhase::Finished);
        assert_eq!(apply_all(&events), state);
    }

    #[test]
    fn tool_failure_can_feed_a_second_model_turn() {
        let events = vec![
            start_event(0),
            context_event(1),
            model_requested(2),
            model_responded(3),
            tool_proposed(4, EffectClass::ReadOnly),
            allow_policy(5),
            tool_started(6),
            tool_failed(7),
            third_context_event(8),
            third_model_requested(9),
            third_model_answer(10),
            rec(11, HarnessEvent::RunFinished { run_id: run_id() }),
        ];

        let state = apply_all(&events);
        assert_eq!(state.phase(), &RunPhase::Finished);
    }

    #[test]
    fn concluded_turn_cannot_reuse_turn_id() {
        let events = [
            start_event(0),
            context_event(1),
            model_requested(2),
            model_responded(3),
            tool_proposed(4, EffectClass::ReadOnly),
            allow_policy(5),
            tool_started(6),
            tool_finished(7),
        ];
        let mut state = apply_all(&events);

        let err = state
            .apply(&context_event_for(8, turn_id(), "reuse turn id"))
            .unwrap_err();
        assert_eq!(
            err,
            Error::TurnReused {
                turn_id: "turn_1".into()
            }
        );
    }

    #[test]
    fn second_turn_step_mismatches_are_rejected() {
        let events = [
            start_event(0),
            context_event(1),
            model_requested(2),
            model_responded(3),
            tool_proposed(4, EffectClass::ReadOnly),
            allow_policy(5),
            tool_started(6),
            tool_finished(7),
            second_context_event(8),
        ];
        let mut state = apply_all(&events);

        let err = state
            .apply(&model_requested_for(9, other_turn_id(), 0))
            .unwrap_err();
        assert_eq!(
            err,
            Error::StepMismatch {
                expected: 1,
                actual: 0
            }
        );
    }

    #[test]
    fn multiple_proposals_are_recorded_but_only_one_is_consumed_per_turn() {
        let mut state = RunState::new();
        for event in &[
            start_event(0),
            context_event(1),
            model_requested(2),
            model_responded_with(
                3,
                turn_id(),
                0,
                "I can read or write.",
                vec![proposal(), write_proposal()],
            ),
        ] {
            state.apply(event).unwrap();
        }

        assert!(matches!(
            state.phase(),
            RunPhase::AwaitingToolCall { proposals, .. } if proposals.len() == 2
        ));

        state
            .apply(&rec(
                4,
                HarnessEvent::ToolCallProposed {
                    run_id: run_id(),
                    turn_id: turn_id(),
                    call: write_call(EffectClass::WorkspaceWrite),
                },
            ))
            .unwrap();

        let err = state.apply(&tool_proposed(5, EffectClass::ReadOnly));
        assert_eq!(
            err,
            Err(Error::InvalidTransition {
                phase: "awaiting_policy",
                event: "tool_call_proposed"
            })
        );
    }

    #[test]
    fn approval_denial_never_emits_execute_tool() {
        let mut events = base_until_approval_required();
        events.push(rec(
            6,
            HarnessEvent::ApprovalDenied {
                run_id: run_id(),
                call_id: call_id(),
                actor_id: actor_id(),
                reason: "no".into(),
            },
        ));
        events.push(rec(
            7,
            HarnessEvent::RunFailed {
                run_id: run_id(),
                reason: "approval denied".into(),
            },
        ));

        let mut state = RunState::new();
        for event in &events[..7] {
            state.apply(event).unwrap();
        }
        assert!(state.pending_command().is_none());
        assert!(matches!(state.phase(), RunPhase::ApprovalDenied { .. }));
        state.apply(&events[7]).unwrap();
        assert!(matches!(state.phase(), RunPhase::Failed { .. }));
    }

    #[test]
    fn policy_denial_never_emits_execute_tool() {
        let events = [
            start_event(0),
            context_event(1),
            model_requested(2),
            model_responded(3),
            tool_proposed(4, EffectClass::SecretAccess),
            deny_policy(5),
            rec(
                6,
                HarnessEvent::RunFailed {
                    run_id: run_id(),
                    reason: "policy denied".into(),
                },
            ),
        ];

        let mut state = RunState::new();
        for event in &events[..6] {
            state.apply(event).unwrap();
        }
        assert!(state.pending_command().is_none());
        assert!(matches!(state.phase(), RunPhase::PolicyDenied { .. }));
        state.apply(&events[6]).unwrap();
        assert!(matches!(state.phase(), RunPhase::Failed { .. }));
    }

    #[test]
    fn tool_failure_records_failure_before_run_failure() {
        let mut events = base_until_approval_required();
        events.push(approval_granted(6));
        events.push(rec(
            7,
            HarnessEvent::ToolStarted {
                run_id: run_id(),
                call_id: call_id(),
            },
        ));
        events.push(rec(
            8,
            HarnessEvent::ToolFailed {
                run_id: run_id(),
                call_id: call_id(),
                reason: "tool crashed".into(),
            },
        ));
        events.push(rec(
            9,
            HarnessEvent::RunFailed {
                run_id: run_id(),
                reason: "tool crashed".into(),
            },
        ));

        let state = apply_all(&events);
        assert!(matches!(state.phase(), RunPhase::Failed { .. }));
    }

    #[test]
    fn out_of_order_sequences_are_rejected() {
        let mut state = RunState::new();
        let err = state.apply(&start_event(1)).unwrap_err();
        assert_eq!(
            err,
            Error::SequenceMismatch {
                expected: 0,
                actual: 1
            }
        );
    }

    #[test]
    fn duplicate_sequences_are_rejected() {
        let mut state = RunState::new();
        state.apply(&start_event(0)).unwrap();
        let err = state.apply(&context_event(0)).unwrap_err();
        assert_eq!(
            err,
            Error::SequenceMismatch {
                expected: 1,
                actual: 0
            }
        );
    }

    #[test]
    fn run_id_mismatches_are_rejected() {
        let mut state = RunState::new();
        state.apply(&start_event(0)).unwrap();
        let err = state
            .apply(&rec(
                1,
                HarnessEvent::ContextBuilt {
                    run_id: other_run_id(),
                    turn_id: turn_id(),
                    context: context(10),
                },
            ))
            .unwrap_err();
        assert_eq!(
            err,
            Error::RunIdMismatch {
                expected: "run_1".into(),
                actual: "run_2".into()
            }
        );
    }

    #[test]
    fn step_mismatches_are_rejected() {
        let mut state = RunState::new();
        state.apply(&start_event(0)).unwrap();
        state.apply(&context_event(1)).unwrap();

        let err = state
            .apply(&rec(
                2,
                HarnessEvent::ModelRequested {
                    run_id: run_id(),
                    turn_id: turn_id(),
                    step: 1,
                    model: ModelName::new("claude-fable-5").unwrap(),
                },
            ))
            .unwrap_err();
        assert_eq!(
            err,
            Error::StepMismatch {
                expected: 0,
                actual: 1
            }
        );
    }

    #[test]
    fn turn_mismatches_are_rejected() {
        let mut state = RunState::new();
        state.apply(&start_event(0)).unwrap();
        state.apply(&context_event(1)).unwrap();

        let err = state
            .apply(&rec(
                2,
                HarnessEvent::ModelRequested {
                    run_id: run_id(),
                    turn_id: other_turn_id(),
                    step: 0,
                    model: ModelName::new("claude-fable-5").unwrap(),
                },
            ))
            .unwrap_err();
        assert_eq!(
            err,
            Error::TurnMismatch {
                expected: "turn_1".into(),
                actual: "turn_2".into()
            }
        );
    }

    #[test]
    fn illegal_event_order_is_rejected() {
        let mut state = RunState::new();
        state.apply(&start_event(0)).unwrap();
        let err = state.apply(&model_requested(1)).unwrap_err();
        assert_eq!(
            err,
            Error::InvalidTransition {
                phase: "ready_for_context",
                event: "model_requested"
            }
        );
    }

    #[test]
    fn tool_call_must_match_model_proposal() {
        let mut state = RunState::new();
        for event in &[
            start_event(0),
            context_event(1),
            model_requested(2),
            model_responded(3),
        ] {
            state.apply(event).unwrap();
        }

        let err = state.apply(&unproposed_tool(4));
        assert_eq!(err, Err(Error::UnproposedToolCall));
    }

    #[test]
    fn terminal_runs_emit_no_commands_and_reject_more_events() {
        let mut events = base_until_approval_required();
        events.push(approval_granted(6));
        events.push(rec(
            7,
            HarnessEvent::ToolStarted {
                run_id: run_id(),
                call_id: call_id(),
            },
        ));
        events.push(rec(
            8,
            HarnessEvent::ToolFinished {
                run_id: run_id(),
                result: result(),
            },
        ));
        events.push(rec(9, HarnessEvent::RunFinished { run_id: run_id() }));

        let mut state = apply_all(&events);
        assert!(state.pending_command().is_none());
        let err = state.apply(&rec(10, HarnessEvent::RunFinished { run_id: run_id() }));
        assert!(matches!(err, Err(Error::InvalidTransition { .. })));
    }

    #[test]
    fn run_failed_closes_any_started_non_terminal_phase() {
        let mut state = RunState::new();
        state.apply(&start_event(0)).unwrap();
        state.apply(&context_event(1)).unwrap();
        state.apply(&model_requested(2)).unwrap();

        state
            .apply(&rec(
                3,
                HarnessEvent::RunFailed {
                    run_id: run_id(),
                    reason: "model request failed".into(),
                },
            ))
            .unwrap();

        assert!(matches!(state.phase(), RunPhase::Failed { .. }));
        assert!(state.pending_command().is_none());
    }

    #[test]
    fn context_budget_is_validated_before_model_request() {
        let mut state = RunState::new();
        state.apply(&start_event(0)).unwrap();
        let over_budget = rec(
            1,
            HarnessEvent::ContextBuilt {
                run_id: run_id(),
                turn_id: turn_id(),
                context: ContextPack {
                    token_budget: 5,
                    fragments: context(10).fragments,
                },
            },
        );

        assert!(matches!(
            state.apply(&over_budget),
            Err(Error::ContextBudgetExceeded {
                used: 10,
                budget: 5
            })
        ));
        assert!(state.pending_command().is_none());
    }
}
