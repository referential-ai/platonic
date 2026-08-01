use super::*;
use crate::{
    ActorId, AgentId, ContextFragment, ContextLane, EffectClass, Message, MessageRole, ModelName,
    ModelUsage, ResultVisibility, ToolName, ToolProposal, ToolResult,
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

fn other_call_id() -> ToolCallId {
    ToolCallId::new("call_2").unwrap()
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

fn usage() -> Option<ModelUsage> {
    Some(ModelUsage {
        input_tokens: 50,
        output_tokens: 10,
    })
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

fn model_failed_for(seq: u64, turn_id: TurnId, step: u32) -> RecordedEvent {
    rec(
        seq,
        HarnessEvent::ModelFailed {
            run_id: run_id(),
            turn_id,
            step,
            reason: "provider unavailable".into(),
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
    model_responded_with_served_model(seq, turn_id, step, output, proposed_calls, None)
}

fn model_responded_with_served_model(
    seq: u64,
    turn_id: TurnId,
    step: u32,
    output: &str,
    proposed_calls: Vec<ToolProposal>,
    served_model: Option<ModelName>,
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
            served_model,
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

fn tool_proposals_rejected(seq: u64, turn_id: TurnId, reason: &str) -> RecordedEvent {
    rec(
        seq,
        HarnessEvent::ToolProposalsRejected {
            run_id: run_id(),
            turn_id,
            reason: reason.into(),
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
fn pending_compaction_accessor_tracks_acceptance_constraint() {
    let mut state = apply_all(&[start_event(0)]);
    assert_eq!(state.pending_compaction_turn(), None);

    state.apply(&compaction_event(1, turn_id())).unwrap();
    assert_eq!(state.pending_compaction_turn(), Some(&turn_id()));

    let pending = state.clone();
    assert!(matches!(
        state.apply(&second_context_event(2)),
        Err(Error::TurnMismatch { .. })
    ));
    assert_eq!(state, pending);

    state.apply(&context_event(2)).unwrap();
    assert_eq!(state.pending_compaction_turn(), None);

    let mut failed = apply_all(&[start_event(0)]);
    failed.apply(&compaction_event(1, turn_id())).unwrap();
    failed
        .apply(&rec(
            2,
            HarnessEvent::RunFailed {
                run_id: run_id(),
                reason: "context build failed".into(),
            },
        ))
        .unwrap();
    assert_eq!(failed.pending_compaction_turn(), None);
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
fn model_failure_reemits_identical_request_and_step_advances_only_on_response() {
    let mut state = apply_all(&[start_event(0), context_event(1)]);
    let original_request = RunCommand::RequestModel {
        turn_id: turn_id(),
        step: 0,
        context: context(10),
    };
    assert_eq!(state.pending_command(), Some(original_request.clone()));

    state.apply(&model_requested(2)).unwrap();
    assert!(state.pending_command().is_none());
    state.apply(&model_failed_for(3, turn_id(), 0)).unwrap();
    assert_eq!(state.pending_command(), Some(original_request));

    state.apply(&model_requested(4)).unwrap();
    state
        .apply(&model_responded_with(5, turn_id(), 0, "done", vec![]))
        .unwrap();
    state.apply(&second_context_event(6)).unwrap();
    assert_eq!(
        state.pending_command(),
        Some(RunCommand::RequestModel {
            turn_id: other_turn_id(),
            step: 1,
            context: context_with_content(10, "tool result: read README"),
        })
    );

    state.apply(&second_model_requested(7)).unwrap();
    state.apply(&second_model_answer(8)).unwrap();
    state
        .apply(&rec(9, HarnessEvent::RunFinished { run_id: run_id() }))
        .unwrap();
    assert_eq!(state.phase(), &RunPhase::Finished);
}

#[test]
fn served_model_does_not_change_run_state_or_pending_commands() {
    let awaiting = apply_all(&[start_event(0), context_event(1), model_requested(2)]);

    for proposed_calls in [vec![], vec![proposal()]] {
        let mut unknown = awaiting.clone();
        unknown
            .apply(&model_responded_with_served_model(
                3,
                turn_id(),
                0,
                "done",
                proposed_calls.clone(),
                None,
            ))
            .unwrap();

        let mut known = awaiting.clone();
        known
            .apply(&model_responded_with_served_model(
                3,
                turn_id(),
                0,
                "done",
                proposed_calls,
                Some(ModelName::new("provider/model-2026-07-31").unwrap()),
            ))
            .unwrap();

        assert_eq!(known, unknown);
        assert_eq!(known.pending_command(), unknown.pending_command());
    }
}

#[test]
fn model_failure_requires_an_awaiting_request_with_matching_turn_and_step() {
    let mut before_request = apply_all(&[start_event(0), context_event(1)]);
    assert_eq!(
        before_request
            .apply(&model_failed_for(2, turn_id(), 0))
            .unwrap_err(),
        Error::InvalidTransition {
            phase: "ready_to_request_model",
            event: "model_failed",
        }
    );

    let awaiting = apply_all(&[start_event(0), context_event(1), model_requested(2)]);
    let mut wrong_turn = awaiting.clone();
    assert_eq!(
        wrong_turn
            .apply(&model_failed_for(3, other_turn_id(), 0))
            .unwrap_err(),
        Error::TurnMismatch {
            expected: "turn_1".into(),
            actual: "turn_2".into(),
        }
    );
    assert_eq!(wrong_turn, awaiting);

    let mut wrong_step = awaiting.clone();
    assert_eq!(
        wrong_step
            .apply(&model_failed_for(3, turn_id(), 1))
            .unwrap_err(),
        Error::StepMismatch {
            expected: 0,
            actual: 1,
        }
    );
    assert_eq!(wrong_step, awaiting);
}

#[test]
fn replay_after_model_failure_reproduces_the_pending_request() {
    let state = apply_all(&[
        start_event(0),
        context_event(1),
        model_requested(2),
        model_failed_for(3, turn_id(), 0),
    ]);

    assert_eq!(
        state.phase(),
        &RunPhase::ReadyToRequestModel {
            turn_id: turn_id(),
            step: 0,
            context: context(10),
        }
    );
    assert_eq!(
        state.pending_command(),
        Some(RunCommand::RequestModel {
            turn_id: turn_id(),
            step: 0,
            context: context(10),
        })
    );
    assert_eq!(state.next_seq(), 4);
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
fn distinct_ids_preserve_pending_commands_and_replay_state() {
    let second_call = ToolCall {
        id: other_call_id(),
        ..call(EffectClass::ReadOnly)
    };
    let events = vec![
        start_event(0),
        context_event(1),
        model_requested(2),
        model_responded(3),
        tool_proposed(4, EffectClass::SecretAccess),
        deny_policy(5),
        second_context_event(6),
        second_model_requested(7),
        model_responded_with(
            8,
            other_turn_id(),
            1,
            "I should read the file again.",
            vec![proposal()],
        ),
        rec(
            9,
            HarnessEvent::ToolCallProposed {
                run_id: run_id(),
                turn_id: other_turn_id(),
                call: second_call.clone(),
            },
        ),
        rec(
            10,
            HarnessEvent::PolicyEvaluated {
                run_id: run_id(),
                call_id: other_call_id(),
                decision: PolicyDecision::Allow,
            },
        ),
    ];

    let mut state = RunState::new();
    for event in &events[..7] {
        state.apply(event).unwrap();
    }
    assert_eq!(
        state.pending_command(),
        Some(RunCommand::RequestModel {
            turn_id: other_turn_id(),
            step: 1,
            context: context_with_content(10, "tool result: read README"),
        })
    );

    for event in &events[7..] {
        state.apply(event).unwrap();
    }
    assert_eq!(
        state.pending_command(),
        Some(RunCommand::ExecuteTool { call: second_call })
    );
    assert_eq!(state.next_seq(), 11);
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
fn earlier_turn_id_cannot_be_reused_after_a_distinct_turn() {
    let events = [
        start_event(0),
        context_event(1),
        model_requested(2),
        model_responded_with(3, turn_id(), 0, "first turn done", vec![]),
        second_context_event(4),
        second_model_requested(5),
        second_model_answer(6),
    ];
    let mut state = apply_all(&events);
    let unchanged = state.clone();

    let mut compacted = state.clone();
    assert_eq!(
        compacted
            .apply(&compaction_event(7, turn_id()))
            .unwrap_err(),
        Error::TurnReused {
            turn_id: "turn_1".into()
        }
    );
    assert_eq!(compacted, unchanged);

    let err = state
        .apply(&context_event_for(7, turn_id(), "reuse turn id"))
        .unwrap_err();
    assert_eq!(
        err,
        Error::TurnReused {
            turn_id: "turn_1".into()
        }
    );
    assert_eq!(state, unchanged);
}

#[test]
fn tool_call_id_cannot_be_reused_in_a_later_turn() {
    let events = [
        start_event(0),
        context_event(1),
        model_requested(2),
        model_responded(3),
        tool_proposed(4, EffectClass::SecretAccess),
        deny_policy(5),
        second_context_event(6),
        second_model_requested(7),
        model_responded_with(
            8,
            other_turn_id(),
            1,
            "I should read the file again.",
            vec![proposal()],
        ),
    ];
    let mut state = apply_all(&events);
    let unchanged = state.clone();

    let err = state
        .apply(&rec(
            9,
            HarnessEvent::ToolCallProposed {
                run_id: run_id(),
                turn_id: other_turn_id(),
                call: call(EffectClass::ReadOnly),
            },
        ))
        .unwrap_err();
    assert_eq!(
        err,
        Error::ToolCallReused {
            call_id: "call_1".into()
        }
    );
    assert_eq!(state, unchanged);
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
fn whole_proposal_batch_rejection_concludes_turn_and_allows_a_later_turn() {
    let mut state = apply_all(&[
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
    ]);

    assert!(matches!(
        state.phase(),
        RunPhase::AwaitingToolCall { proposals, .. } if proposals.len() == 2
    ));

    state
        .apply(&tool_proposals_rejected(
            4,
            turn_id(),
            "proposal schema invalid",
        ))
        .unwrap();
    assert_eq!(state.phase(), &RunPhase::TurnConcluded);
    assert!(state.pending_command().is_none());

    for event in &[
        second_context_event(5),
        second_model_requested(6),
        second_model_answer(7),
    ] {
        state.apply(event).unwrap();
    }
    assert_eq!(state.phase(), &RunPhase::TurnConcluded);
}

#[test]
fn proposal_batch_rejection_requires_the_pending_turn() {
    let mut state = apply_all(&[
        start_event(0),
        context_event(1),
        model_requested(2),
        model_responded(3),
    ]);

    assert_eq!(
        state
            .apply(&tool_proposals_rejected(
                4,
                other_turn_id(),
                "proposal schema invalid",
            ))
            .unwrap_err(),
        Error::TurnMismatch {
            expected: "turn_1".into(),
            actual: "turn_2".into(),
        }
    );
    assert_eq!(state.next_seq(), 4);
}

#[test]
fn proposal_batch_rejection_requires_a_non_empty_reason() {
    let state = apply_all(&[
        start_event(0),
        context_event(1),
        model_requested(2),
        model_responded(3),
    ]);

    for reason in ["", "   "] {
        let mut attempted = state.clone();
        assert_eq!(
            attempted
                .apply(&tool_proposals_rejected(4, turn_id(), reason))
                .unwrap_err(),
            Error::EmptyToolProposalsRejectionReason
        );
        assert_eq!(attempted.next_seq(), 4);
    }
}

#[test]
fn proposal_batch_rejection_is_rejected_outside_awaiting_tool_call() {
    let mut state = apply_all(&[start_event(0), context_event(1)]);

    assert_eq!(
        state
            .apply(&tool_proposals_rejected(
                2,
                turn_id(),
                "proposal schema invalid",
            ))
            .unwrap_err(),
        Error::InvalidTransition {
            phase: "ready_to_request_model",
            event: "tool_proposals_rejected",
        }
    );
    assert_eq!(state.next_seq(), 2);
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
fn tool_call_id_mismatches_are_rejected() {
    let mut state = apply_all(&[
        start_event(0),
        context_event(1),
        model_requested(2),
        model_responded(3),
        tool_proposed(4, EffectClass::ReadOnly),
    ]);

    assert_eq!(
        state
            .apply(&rec(
                5,
                HarnessEvent::PolicyEvaluated {
                    run_id: run_id(),
                    call_id: ToolCallId::new("call_2").unwrap(),
                    decision: PolicyDecision::Allow,
                },
            ))
            .unwrap_err(),
        Error::ToolCallMismatch {
            expected: "call_1".into(),
            actual: "call_2".into(),
        }
    );
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
