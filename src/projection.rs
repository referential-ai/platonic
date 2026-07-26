//! Pure readback projections derived from recorded run events.

use crate::{
    ActorId, ContextFragment, Error, HarnessEvent, Message, PolicyDecision, RecordedEvent,
    RunPhase, RunState, ToolCall, ToolCallId, ToolResult, TurnId,
};

/// Replay-validated readback for one run ledger.
#[derive(Clone, Debug, PartialEq)]
pub struct RunReadback {
    /// Chronological entries useful for replay output.
    pub entries: Vec<ReadbackEntry>,
    /// Final run phase after replaying all events.
    pub final_phase: RunPhase,
    /// Next expected sequence number after replay.
    pub next_seq: u64,
}

impl RunReadback {
    /// Replays an ordered ledger and rejects the first invalid recorded event.
    pub fn from_events(events: &[RecordedEvent]) -> Result<Self, Error> {
        let mut state = RunState::new();
        let mut entries = Vec::new();

        for record in events {
            state.apply(record)?;
            collect_entry(&record.event, &mut entries);
        }

        Ok(Self {
            entries,
            final_phase: state.phase().clone(),
            next_seq: state.next_seq(),
        })
    }
}

/// One deterministic readback entry projected from the ledger.
#[derive(Clone, Debug, PartialEq)]
pub enum ReadbackEntry {
    /// Prior turns omitted from the next model context.
    ContextCompacted {
        /// Turn whose context reflects the compaction.
        turn_id: TurnId,
        /// Estimated tokens before prior turns were dropped.
        estimated_tokens_before: u32,
        /// Estimated tokens after prior turns were dropped.
        estimated_tokens_after: u32,
        /// Zero-based first dropped prior-turn position.
        dropped_turn_start: u64,
        /// Exclusive end of the dropped prior-turn range.
        dropped_turn_end_exclusive: u64,
    },
    /// One host-built context fragment that entered a model turn.
    ContextFragment {
        /// Turn that received the fragment.
        turn_id: TurnId,
        /// Exact fragment recorded in the turn context.
        fragment: ContextFragment,
    },
    /// Model response message.
    ModelMessage {
        /// Turn that received the response.
        turn_id: TurnId,
        /// Normalized model-authored message.
        message: Message,
    },
    /// Host-validated tool call consumed for a turn.
    ToolCall {
        /// Turn that proposed the call.
        turn_id: TurnId,
        /// Validated and effect-classified call.
        call: ToolCall,
    },
    /// Structured tool result.
    ToolResult {
        /// Exact result recorded after execution.
        result: ToolResult,
    },
    /// Policy denied a tool call before execution.
    PolicyDenied {
        /// Call rejected by policy.
        call_id: ToolCallId,
        /// Recorded policy explanation.
        reason: String,
    },
    /// Approval granted a tool call before execution.
    ApprovalGranted {
        /// Call approved for execution.
        call_id: ToolCallId,
        /// Human or host actor that granted approval.
        actor_id: ActorId,
    },
    /// Approval denied a tool call before execution.
    ApprovalDenied {
        /// Call denied before execution.
        call_id: ToolCallId,
        /// Human or host actor that denied approval.
        actor_id: ActorId,
        /// Recorded denial explanation.
        reason: String,
    },
    /// Tool execution failed.
    ToolFailed {
        /// Call whose execution failed.
        call_id: ToolCallId,
        /// Recorded host failure explanation.
        reason: String,
    },
}

fn collect_entry(event: &HarnessEvent, entries: &mut Vec<ReadbackEntry>) {
    match event {
        HarnessEvent::ContextCompacted {
            turn_id,
            estimated_tokens_before,
            estimated_tokens_after,
            dropped_turn_start,
            dropped_turn_end_exclusive,
            ..
        } => {
            entries.push(ReadbackEntry::ContextCompacted {
                turn_id: turn_id.clone(),
                estimated_tokens_before: *estimated_tokens_before,
                estimated_tokens_after: *estimated_tokens_after,
                dropped_turn_start: *dropped_turn_start,
                dropped_turn_end_exclusive: *dropped_turn_end_exclusive,
            });
        }
        HarnessEvent::ContextBuilt {
            turn_id, context, ..
        } => {
            entries.extend(context.fragments.iter().map(|fragment| {
                ReadbackEntry::ContextFragment {
                    turn_id: turn_id.clone(),
                    fragment: fragment.clone(),
                }
            }));
        }
        HarnessEvent::ModelResponded {
            turn_id, output, ..
        } => {
            entries.push(ReadbackEntry::ModelMessage {
                turn_id: turn_id.clone(),
                message: output.clone(),
            });
        }
        HarnessEvent::ToolCallProposed { turn_id, call, .. } => {
            entries.push(ReadbackEntry::ToolCall {
                turn_id: turn_id.clone(),
                call: call.clone(),
            });
        }
        HarnessEvent::ToolFinished { result, .. } => {
            entries.push(ReadbackEntry::ToolResult {
                result: result.clone(),
            });
        }
        HarnessEvent::PolicyEvaluated {
            call_id,
            decision: PolicyDecision::Deny { reason },
            ..
        } => {
            entries.push(ReadbackEntry::PolicyDenied {
                call_id: call_id.clone(),
                reason: reason.clone(),
            });
        }
        HarnessEvent::ApprovalGranted {
            call_id, actor_id, ..
        } => {
            entries.push(ReadbackEntry::ApprovalGranted {
                call_id: call_id.clone(),
                actor_id: actor_id.clone(),
            });
        }
        HarnessEvent::ApprovalDenied {
            call_id,
            actor_id,
            reason,
            ..
        } => {
            entries.push(ReadbackEntry::ApprovalDenied {
                call_id: call_id.clone(),
                actor_id: actor_id.clone(),
                reason: reason.clone(),
            });
        }
        HarnessEvent::ToolFailed {
            call_id, reason, ..
        } => {
            entries.push(ReadbackEntry::ToolFailed {
                call_id: call_id.clone(),
                reason: reason.clone(),
            });
        }
        HarnessEvent::RunStarted { .. }
        | HarnessEvent::ModelRequested { .. }
        | HarnessEvent::PolicyEvaluated {
            decision: PolicyDecision::Allow | PolicyDecision::RequireApproval { .. },
            ..
        }
        | HarnessEvent::ToolStarted { .. }
        | HarnessEvent::RunFinished { .. }
        | HarnessEvent::RunFailed { .. } => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentId, ContextFragment, ContextLane, ContextPack, EffectClass, MessageRole, ModelName,
        ModelUsage, ResultVisibility, RunId, ToolName, ToolProposal,
    };
    use serde_json::json;

    fn run_id() -> RunId {
        RunId::new("run_1").unwrap()
    }

    fn agent_id() -> AgentId {
        AgentId::new("agent_1").unwrap()
    }

    fn turn_id() -> TurnId {
        TurnId::new("turn_1").unwrap()
    }

    fn second_turn_id() -> TurnId {
        TurnId::new("turn_2").unwrap()
    }

    fn call_id() -> ToolCallId {
        ToolCallId::new("call_1").unwrap()
    }

    fn actor_id() -> ActorId {
        ActorId::new("human_1").unwrap()
    }

    fn usage() -> ModelUsage {
        ModelUsage {
            input_tokens: 20,
            output_tokens: 8,
        }
    }

    fn rec(seq: u64, event: HarnessEvent) -> RecordedEvent {
        RecordedEvent {
            seq,
            occurred_at_ms: 1_700_000_000_000 + seq,
            event,
        }
    }

    fn context(turn_id: TurnId, content: &str) -> HarnessEvent {
        HarnessEvent::ContextBuilt {
            run_id: run_id(),
            turn_id,
            context: ContextPack {
                token_budget: 100,
                fragments: vec![ContextFragment {
                    lane: ContextLane::CurrentTask,
                    source: "user".into(),
                    content: content.into(),
                    estimated_tokens: 10,
                }],
            },
        }
    }

    fn model_requested(turn_id: TurnId, step: u32) -> HarnessEvent {
        HarnessEvent::ModelRequested {
            run_id: run_id(),
            turn_id,
            step,
            model: ModelName::new("claude-fable-5").unwrap(),
        }
    }

    fn model_responded(
        turn_id: TurnId,
        step: u32,
        content: &str,
        proposed_calls: Vec<ToolProposal>,
    ) -> HarnessEvent {
        HarnessEvent::ModelResponded {
            run_id: run_id(),
            turn_id,
            step,
            output: Message {
                role: MessageRole::Assistant,
                content: content.into(),
            },
            proposed_calls,
            usage: usage(),
        }
    }

    fn proposal() -> ToolProposal {
        ToolProposal {
            tool: ToolName::new("file.read").unwrap(),
            input: json!({ "path": "README.md" }),
        }
    }

    fn call() -> ToolCall {
        ToolCall {
            id: call_id(),
            tool: ToolName::new("file.read").unwrap(),
            effect: EffectClass::ReadOnly,
            input: json!({ "path": "README.md" }),
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

    fn start_event(seq: u64) -> RecordedEvent {
        rec(
            seq,
            HarnessEvent::RunStarted {
                run_id: run_id(),
                agent_id: agent_id(),
            },
        )
    }

    #[test]
    fn compaction_precedes_its_context_fragments_in_readback() {
        let events = vec![
            start_event(0),
            rec(
                1,
                HarnessEvent::ContextCompacted {
                    run_id: run_id(),
                    turn_id: turn_id(),
                    estimated_tokens_before: 160,
                    estimated_tokens_after: 80,
                    dropped_turn_start: 0,
                    dropped_turn_end_exclusive: 2,
                },
            ),
            rec(2, context(turn_id(), "Keep recent turns")),
            rec(3, model_requested(turn_id(), 0)),
            rec(4, model_responded(turn_id(), 0, "done", vec![])),
            rec(5, HarnessEvent::RunFinished { run_id: run_id() }),
        ];

        let readback = RunReadback::from_events(&events).unwrap();

        assert_eq!(readback.final_phase, RunPhase::Finished);
        assert_eq!(readback.next_seq, 6);
        assert_eq!(
            readback.entries,
            vec![
                ReadbackEntry::ContextCompacted {
                    turn_id: turn_id(),
                    estimated_tokens_before: 160,
                    estimated_tokens_after: 80,
                    dropped_turn_start: 0,
                    dropped_turn_end_exclusive: 2,
                },
                ReadbackEntry::ContextFragment {
                    turn_id: turn_id(),
                    fragment: ContextFragment {
                        lane: ContextLane::CurrentTask,
                        source: "user".into(),
                        content: "Keep recent turns".into(),
                        estimated_tokens: 10,
                    },
                },
                ReadbackEntry::ModelMessage {
                    turn_id: turn_id(),
                    message: Message {
                        role: MessageRole::Assistant,
                        content: "done".into(),
                    },
                },
            ]
        );
    }

    #[test]
    fn one_turn_ledger_projects_context_message_and_final_state() {
        let events = vec![
            start_event(0),
            rec(1, context(turn_id(), "What is in README?")),
            rec(2, model_requested(turn_id(), 0)),
            rec(3, model_responded(turn_id(), 0, "It is a README.", vec![])),
            rec(4, HarnessEvent::RunFinished { run_id: run_id() }),
        ];

        let readback = RunReadback::from_events(&events).unwrap();

        assert_eq!(readback.final_phase, RunPhase::Finished);
        assert_eq!(readback.next_seq, 5);
        assert_eq!(
            readback.entries,
            vec![
                ReadbackEntry::ContextFragment {
                    turn_id: turn_id(),
                    fragment: ContextFragment {
                        lane: ContextLane::CurrentTask,
                        source: "user".into(),
                        content: "What is in README?".into(),
                        estimated_tokens: 10,
                    },
                },
                ReadbackEntry::ModelMessage {
                    turn_id: turn_id(),
                    message: Message {
                        role: MessageRole::Assistant,
                        content: "It is a README.".into(),
                    },
                },
            ]
        );
    }

    #[test]
    fn two_turn_ledger_projects_tool_result_continuation() {
        let events = vec![
            start_event(0),
            rec(1, context(turn_id(), "Read README")),
            rec(2, model_requested(turn_id(), 0)),
            rec(
                3,
                model_responded(turn_id(), 0, "I will read it.", vec![proposal()]),
            ),
            rec(
                4,
                HarnessEvent::ToolCallProposed {
                    run_id: run_id(),
                    turn_id: turn_id(),
                    call: call(),
                },
            ),
            rec(
                5,
                HarnessEvent::PolicyEvaluated {
                    run_id: run_id(),
                    call_id: call_id(),
                    decision: PolicyDecision::Allow,
                },
            ),
            rec(
                6,
                HarnessEvent::ToolStarted {
                    run_id: run_id(),
                    call_id: call_id(),
                },
            ),
            rec(
                7,
                HarnessEvent::ToolFinished {
                    run_id: run_id(),
                    result: result(),
                },
            ),
            rec(8, context(second_turn_id(), "Tool result: read README")),
            rec(9, model_requested(second_turn_id(), 1)),
            rec(
                10,
                model_responded(second_turn_id(), 1, "README was read.", vec![]),
            ),
            rec(11, HarnessEvent::RunFinished { run_id: run_id() }),
        ];

        let readback = RunReadback::from_events(&events).unwrap();

        assert_eq!(readback.final_phase, RunPhase::Finished);
        assert_eq!(readback.next_seq, 12);
        assert_eq!(
            readback.entries,
            vec![
                ReadbackEntry::ContextFragment {
                    turn_id: turn_id(),
                    fragment: ContextFragment {
                        lane: ContextLane::CurrentTask,
                        source: "user".into(),
                        content: "Read README".into(),
                        estimated_tokens: 10,
                    },
                },
                ReadbackEntry::ModelMessage {
                    turn_id: turn_id(),
                    message: Message {
                        role: MessageRole::Assistant,
                        content: "I will read it.".into(),
                    },
                },
                ReadbackEntry::ToolCall {
                    turn_id: turn_id(),
                    call: call(),
                },
                ReadbackEntry::ToolResult { result: result() },
                ReadbackEntry::ContextFragment {
                    turn_id: second_turn_id(),
                    fragment: ContextFragment {
                        lane: ContextLane::CurrentTask,
                        source: "user".into(),
                        content: "Tool result: read README".into(),
                        estimated_tokens: 10,
                    },
                },
                ReadbackEntry::ModelMessage {
                    turn_id: second_turn_id(),
                    message: Message {
                        role: MessageRole::Assistant,
                        content: "README was read.".into(),
                    },
                },
            ]
        );
    }

    #[test]
    fn denials_and_failures_are_projected_without_tool_results() {
        let policy_denied = vec![
            start_event(0),
            rec(1, context(turn_id(), "Read secret")),
            rec(2, model_requested(turn_id(), 0)),
            rec(
                3,
                model_responded(turn_id(), 0, "I will read it.", vec![proposal()]),
            ),
            rec(
                4,
                HarnessEvent::ToolCallProposed {
                    run_id: run_id(),
                    turn_id: turn_id(),
                    call: call(),
                },
            ),
            rec(
                5,
                HarnessEvent::PolicyEvaluated {
                    run_id: run_id(),
                    call_id: call_id(),
                    decision: PolicyDecision::Deny {
                        reason: "not allowed".into(),
                    },
                },
            ),
        ];
        let policy_readback = RunReadback::from_events(&policy_denied).unwrap();
        assert!(
            policy_readback
                .entries
                .contains(&ReadbackEntry::PolicyDenied {
                    call_id: call_id(),
                    reason: "not allowed".into(),
                })
        );
        assert!(
            !policy_readback
                .entries
                .iter()
                .any(|entry| matches!(entry, ReadbackEntry::ToolResult { .. }))
        );

        let approval_denied = vec![
            start_event(0),
            rec(1, context(turn_id(), "Write README")),
            rec(2, model_requested(turn_id(), 0)),
            rec(
                3,
                model_responded(turn_id(), 0, "I will write it.", vec![proposal()]),
            ),
            rec(
                4,
                HarnessEvent::ToolCallProposed {
                    run_id: run_id(),
                    turn_id: turn_id(),
                    call: call(),
                },
            ),
            rec(
                5,
                HarnessEvent::PolicyEvaluated {
                    run_id: run_id(),
                    call_id: call_id(),
                    decision: PolicyDecision::RequireApproval {
                        reason: "approval needed".into(),
                    },
                },
            ),
            rec(
                6,
                HarnessEvent::ApprovalDenied {
                    run_id: run_id(),
                    call_id: call_id(),
                    actor_id: actor_id(),
                    reason: "no".into(),
                },
            ),
        ];
        let approval_readback = RunReadback::from_events(&approval_denied).unwrap();
        assert!(
            approval_readback
                .entries
                .contains(&ReadbackEntry::ApprovalDenied {
                    call_id: call_id(),
                    actor_id: actor_id(),
                    reason: "no".into(),
                })
        );
        assert!(
            !approval_readback
                .entries
                .iter()
                .any(|entry| matches!(entry, ReadbackEntry::ToolResult { .. }))
        );

        let tool_failed = vec![
            start_event(0),
            rec(1, context(turn_id(), "Read README")),
            rec(2, model_requested(turn_id(), 0)),
            rec(
                3,
                model_responded(turn_id(), 0, "I will read it.", vec![proposal()]),
            ),
            rec(
                4,
                HarnessEvent::ToolCallProposed {
                    run_id: run_id(),
                    turn_id: turn_id(),
                    call: call(),
                },
            ),
            rec(
                5,
                HarnessEvent::PolicyEvaluated {
                    run_id: run_id(),
                    call_id: call_id(),
                    decision: PolicyDecision::Allow,
                },
            ),
            rec(
                6,
                HarnessEvent::ToolStarted {
                    run_id: run_id(),
                    call_id: call_id(),
                },
            ),
            rec(
                7,
                HarnessEvent::ToolFailed {
                    run_id: run_id(),
                    call_id: call_id(),
                    reason: "tool crashed".into(),
                },
            ),
        ];
        let failure_readback = RunReadback::from_events(&tool_failed).unwrap();
        assert!(
            failure_readback
                .entries
                .contains(&ReadbackEntry::ToolFailed {
                    call_id: call_id(),
                    reason: "tool crashed".into(),
                })
        );
        assert!(
            !failure_readback
                .entries
                .iter()
                .any(|entry| matches!(entry, ReadbackEntry::ToolResult { .. }))
        );
    }

    #[test]
    fn approval_grants_are_projected_with_actor() {
        let events = vec![
            start_event(0),
            rec(1, context(turn_id(), "Read README")),
            rec(2, model_requested(turn_id(), 0)),
            rec(
                3,
                model_responded(turn_id(), 0, "I will read it.", vec![proposal()]),
            ),
            rec(
                4,
                HarnessEvent::ToolCallProposed {
                    run_id: run_id(),
                    turn_id: turn_id(),
                    call: call(),
                },
            ),
            rec(
                5,
                HarnessEvent::PolicyEvaluated {
                    run_id: run_id(),
                    call_id: call_id(),
                    decision: PolicyDecision::RequireApproval {
                        reason: "approval needed".into(),
                    },
                },
            ),
            rec(
                6,
                HarnessEvent::ApprovalGranted {
                    run_id: run_id(),
                    call_id: call_id(),
                    actor_id: actor_id(),
                },
            ),
        ];

        let readback = RunReadback::from_events(&events).unwrap();

        assert_eq!(
            readback.final_phase,
            RunPhase::ReadyToExecuteTool { call: call() }
        );
        assert!(readback.entries.contains(&ReadbackEntry::ApprovalGranted {
            call_id: call_id(),
            actor_id: actor_id(),
        }));
    }

    #[test]
    fn invalid_ledger_returns_replay_error() {
        let events = vec![start_event(1)];

        let err = RunReadback::from_events(&events).unwrap_err();
        assert_eq!(
            err,
            Error::SequenceMismatch {
                expected: 0,
                actual: 1
            }
        );
    }
}
