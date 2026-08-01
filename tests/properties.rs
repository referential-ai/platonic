//! Bounded run-state invariants over syntactically valid ledger records.
//!
//! Each property runs 256 cases over sequences of at most 32 records. A fixed
//! suite seed and a 2,048-iteration shrink cap keep CI runs deterministic and
//! bounded. On failure, proptest emits and persists the exact failing case seed
//! so rerunning the named test reproduces the minimized input.

use platonic_core::{
    ActorId, AgentId, ArtifactId, ContextFragment, ContextLane, ContextPack, EffectClass, Error,
    HarnessEvent, Message, MessageRole, ModelName, ModelUsage, PolicyDecision, RecordedEvent,
    ResultVisibility, RunId, RunState, ToolCall, ToolCallId, ToolName, ToolProposal, ToolResult,
    TurnId,
};
use proptest::{
    prelude::*,
    test_runner::{Config as ProptestConfig, RngSeed},
};
use serde_json::Value;

const CASES: u32 = 256;
const MAX_SEQUENCE_LEN: usize = 32;
const MAX_SHRINK_ITERS: u32 = 2_048;
const REPRODUCTION_SEED: u64 = 0x51_2026_0801;

fn property_config() -> ProptestConfig {
    ProptestConfig {
        cases: CASES,
        max_shrink_iters: MAX_SHRINK_ITERS,
        rng_seed: RngSeed::Fixed(REPRODUCTION_SEED),
        ..ProptestConfig::default()
    }
}

fn bounded_id(prefix: &'static str) -> BoxedStrategy<String> {
    (0_u8..=3)
        .prop_map(move |suffix| format!("{prefix}_{suffix}"))
        .boxed()
}

fn bounded_text() -> BoxedStrategy<String> {
    prop::collection::vec(0x20_u8..=0x7e, 0..=16)
        .prop_map(|bytes| String::from_utf8(bytes).expect("ASCII bytes are valid UTF-8"))
        .boxed()
}

fn run_id() -> BoxedStrategy<RunId> {
    bounded_id("run")
        .prop_map(|value| RunId::new(value).expect("generated run ids are non-empty"))
        .boxed()
}

fn turn_id() -> BoxedStrategy<TurnId> {
    bounded_id("turn")
        .prop_map(|value| TurnId::new(value).expect("generated turn ids are non-empty"))
        .boxed()
}

fn agent_id() -> BoxedStrategy<AgentId> {
    bounded_id("agent")
        .prop_map(|value| AgentId::new(value).expect("generated agent ids are non-empty"))
        .boxed()
}

fn call_id() -> BoxedStrategy<ToolCallId> {
    bounded_id("call")
        .prop_map(|value| ToolCallId::new(value).expect("generated call ids are non-empty"))
        .boxed()
}

fn artifact_id() -> BoxedStrategy<ArtifactId> {
    bounded_id("artifact")
        .prop_map(|value| ArtifactId::new(value).expect("generated artifact ids are non-empty"))
        .boxed()
}

fn tool_name() -> BoxedStrategy<ToolName> {
    bounded_id("tool")
        .prop_map(|value| ToolName::new(value).expect("generated tool names are non-empty"))
        .boxed()
}

fn model_name() -> BoxedStrategy<ModelName> {
    bounded_id("model")
        .prop_map(|value| ModelName::new(value).expect("generated model names are non-empty"))
        .boxed()
}

fn actor_id() -> BoxedStrategy<ActorId> {
    bounded_id("actor")
        .prop_map(|value| ActorId::new(value).expect("generated actor ids are non-empty"))
        .boxed()
}

fn context_lane() -> BoxedStrategy<ContextLane> {
    prop_oneof![
        Just(ContextLane::SystemContract),
        Just(ContextLane::CurrentTask),
        Just(ContextLane::ToolSchemas),
        Just(ContextLane::RecentTurns),
        Just(ContextLane::RetrievedContext),
        Just(ContextLane::ArtifactSummary),
        Just(ContextLane::Policy),
    ]
    .boxed()
}

fn message_role() -> BoxedStrategy<MessageRole> {
    prop_oneof![
        Just(MessageRole::System),
        Just(MessageRole::User),
        Just(MessageRole::Assistant),
        Just(MessageRole::Tool),
    ]
    .boxed()
}

fn effect_class() -> BoxedStrategy<EffectClass> {
    prop_oneof![
        Just(EffectClass::ReadOnly),
        Just(EffectClass::WorkspaceWrite),
        Just(EffectClass::Network),
        Just(EffectClass::ExternalSideEffect),
        Just(EffectClass::SecretAccess),
    ]
    .boxed()
}

fn result_visibility() -> BoxedStrategy<ResultVisibility> {
    prop_oneof![
        Just(ResultVisibility::Model),
        Just(ResultVisibility::User),
        Just(ResultVisibility::Both),
    ]
    .boxed()
}

fn json_value() -> BoxedStrategy<Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i16>().prop_map(|value| Value::from(i64::from(value))),
        bounded_text().prop_map(Value::String),
        prop::collection::vec(bounded_text(), 0..=3)
            .prop_map(|values| { Value::Array(values.into_iter().map(Value::String).collect()) }),
        (bounded_text(), any::<i16>()).prop_map(|(key, value)| {
            Value::Object([(key, Value::from(i64::from(value)))].into_iter().collect())
        }),
    ]
    .boxed()
}

fn context_fragment() -> BoxedStrategy<ContextFragment> {
    (context_lane(), bounded_text(), bounded_text(), 0_u32..=256)
        .prop_map(
            |(lane, source, content, estimated_tokens)| ContextFragment {
                lane,
                source,
                content,
                estimated_tokens,
            },
        )
        .boxed()
}

fn context_pack() -> BoxedStrategy<ContextPack> {
    (
        0_u32..=512,
        prop::collection::vec(context_fragment(), 0..=4),
    )
        .prop_map(|(token_budget, fragments)| ContextPack {
            token_budget,
            fragments,
        })
        .boxed()
}

fn message() -> BoxedStrategy<Message> {
    (message_role(), bounded_text())
        .prop_map(|(role, content)| Message { role, content })
        .boxed()
}

fn model_usage() -> BoxedStrategy<Option<ModelUsage>> {
    prop::option::of(
        (any::<u16>(), any::<u16>()).prop_map(|(input_tokens, output_tokens)| ModelUsage {
            input_tokens: u32::from(input_tokens),
            output_tokens: u32::from(output_tokens),
        }),
    )
    .boxed()
}

fn policy_decision() -> BoxedStrategy<PolicyDecision> {
    prop_oneof![
        Just(PolicyDecision::Allow),
        bounded_text().prop_map(|reason| PolicyDecision::RequireApproval { reason }),
        bounded_text().prop_map(|reason| PolicyDecision::Deny { reason }),
    ]
    .boxed()
}

fn tool_proposal() -> BoxedStrategy<ToolProposal> {
    (tool_name(), json_value())
        .prop_map(|(tool, input)| ToolProposal { tool, input })
        .boxed()
}

fn tool_call() -> BoxedStrategy<ToolCall> {
    (call_id(), tool_name(), effect_class(), json_value())
        .prop_map(|(id, tool, effect, input)| ToolCall {
            id,
            tool,
            effect,
            input,
        })
        .boxed()
}

fn tool_result() -> BoxedStrategy<ToolResult> {
    (
        call_id(),
        bounded_text(),
        json_value(),
        prop::collection::vec(artifact_id(), 0..=3),
        result_visibility(),
    )
        .prop_map(
            |(call_id, summary, data, artifacts, visibility)| ToolResult {
                call_id,
                summary,
                data,
                artifacts,
                visibility,
            },
        )
        .boxed()
}

fn harness_event() -> BoxedStrategy<HarnessEvent> {
    prop_oneof![
        (run_id(), agent_id())
            .prop_map(|(run_id, agent_id)| HarnessEvent::RunStarted { run_id, agent_id }),
        (run_id(), turn_id(), context_pack()).prop_map(|(run_id, turn_id, context)| {
            HarnessEvent::ContextBuilt {
                run_id,
                turn_id,
                context,
            }
        }),
        (
            run_id(),
            turn_id(),
            any::<u16>(),
            any::<u16>(),
            0_u8..=8,
            0_u8..=8,
        )
            .prop_map(
                |(
                    run_id,
                    turn_id,
                    estimated_tokens_before,
                    estimated_tokens_after,
                    dropped_turn_start,
                    dropped_turn_end_exclusive,
                )| HarnessEvent::ContextCompacted {
                    run_id,
                    turn_id,
                    estimated_tokens_before: u32::from(estimated_tokens_before),
                    estimated_tokens_after: u32::from(estimated_tokens_after),
                    dropped_turn_start: u64::from(dropped_turn_start),
                    dropped_turn_end_exclusive: u64::from(dropped_turn_end_exclusive),
                },
            ),
        (run_id(), turn_id(), any::<u16>(), model_name()).prop_map(
            |(run_id, turn_id, step, model)| HarnessEvent::ModelRequested {
                run_id,
                turn_id,
                step: u32::from(step),
                model,
            },
        ),
        (run_id(), turn_id(), any::<u16>(), bounded_text()).prop_map(
            |(run_id, turn_id, step, reason)| HarnessEvent::ModelFailed {
                run_id,
                turn_id,
                step: u32::from(step),
                reason,
            },
        ),
        (
            run_id(),
            turn_id(),
            any::<u16>(),
            message(),
            prop::collection::vec(tool_proposal(), 0..=3),
            prop::option::of(model_name()),
            model_usage(),
        )
            .prop_map(
                |(run_id, turn_id, step, output, proposed_calls, served_model, usage)| {
                    HarnessEvent::ModelResponded {
                        run_id,
                        turn_id,
                        step: u32::from(step),
                        output,
                        proposed_calls,
                        served_model,
                        usage,
                    }
                },
            ),
        (run_id(), turn_id(), bounded_text()).prop_map(|(run_id, turn_id, reason)| {
            HarnessEvent::ToolProposalsRejected {
                run_id,
                turn_id,
                reason,
            }
        }),
        (run_id(), turn_id(), tool_call()).prop_map(|(run_id, turn_id, call)| {
            HarnessEvent::ToolCallProposed {
                run_id,
                turn_id,
                call,
            }
        }),
        (run_id(), call_id(), policy_decision()).prop_map(|(run_id, call_id, decision)| {
            HarnessEvent::PolicyEvaluated {
                run_id,
                call_id,
                decision,
            }
        },),
        (run_id(), call_id(), actor_id()).prop_map(|(run_id, call_id, actor_id)| {
            HarnessEvent::ApprovalGranted {
                run_id,
                call_id,
                actor_id,
            }
        }),
        (run_id(), call_id(), actor_id(), bounded_text()).prop_map(
            |(run_id, call_id, actor_id, reason)| HarnessEvent::ApprovalDenied {
                run_id,
                call_id,
                actor_id,
                reason,
            },
        ),
        (run_id(), call_id())
            .prop_map(|(run_id, call_id)| HarnessEvent::ToolStarted { run_id, call_id }),
        (run_id(), tool_result())
            .prop_map(|(run_id, result)| HarnessEvent::ToolFinished { run_id, result }),
        (run_id(), call_id(), bounded_text()).prop_map(|(run_id, call_id, reason)| {
            HarnessEvent::ToolFailed {
                run_id,
                call_id,
                reason,
            }
        }),
        run_id().prop_map(|run_id| HarnessEvent::RunFinished { run_id }),
        (run_id(), bounded_text())
            .prop_map(|(run_id, reason)| HarnessEvent::RunFailed { run_id, reason }),
    ]
    .boxed()
}

fn recorded_event() -> BoxedStrategy<RecordedEvent> {
    (
        0_u64..=MAX_SEQUENCE_LEN as u64,
        any::<u64>(),
        harness_event(),
    )
        .prop_map(|(seq, occurred_at_ms, event)| RecordedEvent {
            seq,
            occurred_at_ms,
            event,
        })
        .boxed()
}

fn mixed_event_sequence() -> BoxedStrategy<Vec<RecordedEvent>> {
    // Contiguous records reach phase validation instead of stopping at sequence checks.
    prop::collection::vec((any::<u64>(), harness_event()), 0..=MAX_SEQUENCE_LEN)
        .prop_map(|records| {
            records
                .into_iter()
                .enumerate()
                .map(|(seq, (occurred_at_ms, event))| RecordedEvent {
                    seq: seq as u64,
                    occurred_at_ms,
                    event,
                })
                .collect()
        })
        .boxed()
}

fn fold_until_first_error(records: &[RecordedEvent]) -> (RunState, Option<(usize, Error)>) {
    let mut state = RunState::new();
    for (index, record) in records.iter().enumerate() {
        if let Err(error) = state.apply(record) {
            return (state, Some((index, error)));
        }
    }
    (state, None)
}

proptest! {
    #![proptest_config(property_config())]

    #[test]
    fn syntactically_valid_mixed_event_sequences_never_panic(
        records in mixed_event_sequence(),
    ) {
        let mut state = RunState::new();
        for record in &records {
            let _ = state.apply(record);
        }
    }

    #[test]
    fn rejected_events_leave_run_state_unchanged(records in mixed_event_sequence()) {
        let mut state = RunState::new();
        for record in &records {
            let before = state.clone();
            if state.apply(record).is_err() {
                prop_assert_eq!(&state, &before);
            }
        }
    }

    #[test]
    fn folding_the_same_sequence_has_the_same_state_and_first_error(
        records in mixed_event_sequence(),
    ) {
        let first = fold_until_first_error(&records);
        let second = fold_until_first_error(&records);
        prop_assert_eq!(first, second);
    }

    #[test]
    fn recorded_events_survive_json_round_trips(record in recorded_event()) {
        let encoded = serde_json::to_vec(&record).expect("generated records serialize");
        let decoded: RecordedEvent =
            serde_json::from_slice(&encoded).expect("serialized records deserialize");
        prop_assert_eq!(decoded, record);
    }
}
