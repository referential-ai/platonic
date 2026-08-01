//! Pure run state machine.

use crate::{
    ContextPack, Error, HarnessEvent, PolicyDecision, RecordedEvent, RunId, ToolCall, ToolCallId,
    ToolProposal, TurnId,
};
use std::collections::BTreeSet;

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
        /// Budget-validated context retained if the request fails.
        context: ContextPack,
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
/// The state machine performs no IO. Hosts inspect [`Self::pending_command`] for
/// requested effects and [`Self::pending_compaction_turn`] for the narrow
/// post-compaction context gate, then apply the resulting recorded event.
/// A [`HarnessEvent::ModelFailed`] restores the identical pending model command
/// without advancing its step.
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
///         served_model: None,
///         usage: Some(ModelUsage { input_tokens: 3, output_tokens: 5 }),
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
    used_turn_ids: BTreeSet<TurnId>,
    used_tool_call_ids: BTreeSet<ToolCallId>,
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
            used_turn_ids: BTreeSet::new(),
            used_tool_call_ids: BTreeSet::new(),
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

    /// Returns the turn whose compacted context must be built next.
    ///
    /// After an accepted [`HarnessEvent::ContextCompacted`], [`Self::phase`]
    /// intentionally remains the surrounding stable phase and
    /// [`Self::pending_command`] returns `None`. While this returns `Some`, only
    /// a matching [`HarnessEvent::ContextBuilt`] or terminal
    /// [`HarnessEvent::RunFailed`] can be accepted. Either accepted event clears
    /// the pending turn.
    pub fn pending_compaction_turn(&self) -> Option<&TurnId> {
        self.pending_compaction_turn_id.as_ref()
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
                ensure_new_turn(&self.used_turn_ids, turn_id)?;
                self.pending_compaction_turn_id = Some(turn_id.clone());
                Ok(())
            }
            (
                RunPhase::ReadyToRequestModel {
                    turn_id,
                    step,
                    context,
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
                    context: context.clone(),
                };
                Ok(())
            }
            (
                RunPhase::AwaitingModelResponse {
                    turn_id,
                    step,
                    context,
                },
                HarnessEvent::ModelFailed {
                    turn_id: actual_turn_id,
                    step: actual_step,
                    ..
                },
            ) => {
                ensure_turn(turn_id, actual_turn_id)?;
                ensure_step(*step, *actual_step)?;
                self.phase = RunPhase::ReadyToRequestModel {
                    turn_id: turn_id.clone(),
                    step: *step,
                    context: context.clone(),
                };
                Ok(())
            }
            (
                RunPhase::AwaitingModelResponse { turn_id, step, .. },
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
                ensure_new_tool_call(&self.used_tool_call_ids, &call.id)?;
                self.used_tool_call_ids.insert(call.id.clone());
                self.phase = RunPhase::AwaitingPolicy { call: call.clone() };
                Ok(())
            }
            (
                RunPhase::AwaitingToolCall { turn_id, .. },
                HarnessEvent::ToolProposalsRejected {
                    turn_id: actual_turn_id,
                    reason,
                    ..
                },
            ) => {
                ensure_turn(turn_id, actual_turn_id)?;
                ensure_tool_proposals_rejection_reason(reason)?;
                self.phase = RunPhase::TurnConcluded;
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
        ensure_new_turn(&self.used_turn_ids, turn_id)?;
        context.validate_budget()?;
        self.used_turn_ids.insert(turn_id.clone());
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

fn ensure_new_turn(used_turn_ids: &BTreeSet<TurnId>, actual: &TurnId) -> Result<(), Error> {
    if used_turn_ids.contains(actual) {
        return Err(Error::TurnReused {
            turn_id: actual.to_string(),
        });
    }
    Ok(())
}

fn ensure_tool_proposals_rejection_reason(reason: &str) -> Result<(), Error> {
    if reason.trim().is_empty() {
        return Err(Error::EmptyToolProposalsRejectionReason);
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

fn ensure_new_tool_call(
    used_tool_call_ids: &BTreeSet<ToolCallId>,
    actual: &ToolCallId,
) -> Result<(), Error> {
    if used_tool_call_ids.contains(actual) {
        return Err(Error::ToolCallReused {
            call_id: actual.to_string(),
        });
    }
    Ok(())
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
mod tests;
