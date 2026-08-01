//! Durable harness event ledger types.

use crate::{
    ActorId, AgentId, ContextPack, Message, ModelName, PolicyDecision, RunId, ToolCall, ToolCallId,
    ToolProposal, ToolResult, TurnId,
};
use serde::{Deserialize, Serialize};

/// Token usage reported by a model provider.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelUsage {
    /// Prompt-side tokens used by the request.
    pub input_tokens: u32,
    /// Completion-side tokens emitted by the model.
    pub output_tokens: u32,
}

/// One durable event with host-supplied ordering metadata.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordedEvent {
    /// Per-run sequence number, contiguous from zero.
    pub seq: u64,
    /// Host-supplied wall-clock timestamp in milliseconds since Unix epoch.
    pub occurred_at_ms: u64,
    /// Recorded run fact.
    pub event: HarnessEvent,
}

/// Durable event log entries. Transcript, metrics, replay, and audit views are derived from this log.
///
/// The tagged JSON schema rejects unknown fields rather than discarding durable data.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "event")]
pub enum HarnessEvent {
    /// Binds a fresh state machine to one run and agent.
    RunStarted {
        /// Durable run identifier shared by every later event.
        run_id: RunId,
        /// Agent identity selected by the host for this run.
        agent_id: AgentId,
    },
    /// Records the exact bounded context selected for a model turn.
    ContextBuilt {
        /// Run receiving the context.
        run_id: RunId,
        /// New turn identifier; concluded turn identifiers cannot be reused.
        turn_id: TurnId,
        /// Lane-labeled context that must pass budget validation.
        context: ContextPack,
    },
    /// Records prior turns omitted from the next model context.
    ContextCompacted {
        /// Run receiving the compacted context.
        run_id: RunId,
        /// Turn whose context will reflect the compaction.
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
    /// Records that a model request crossed the provider boundary.
    ModelRequested {
        /// Run making the request.
        run_id: RunId,
        /// Turn whose context is being submitted.
        turn_id: TurnId,
        /// Monotonic model step expected by the run state.
        step: u32,
        /// Host-selected model used for the request.
        model: ModelName,
    },
    /// Records that the pending model request failed without terminating the run.
    ModelFailed {
        /// Run containing the pending request.
        run_id: RunId,
        /// Turn of the pending model request.
        turn_id: TurnId,
        /// Model step of the pending request.
        step: u32,
        /// Durable host-reported failure reason.
        reason: String,
    },
    /// Records the normalized result returned by the pending model request.
    ModelResponded {
        /// Run receiving the response.
        run_id: RunId,
        /// Turn of the pending model request.
        turn_id: TurnId,
        /// Model step of the pending request.
        step: u32,
        /// Normalized model-authored message.
        output: Message,
        /// Unvalidated model proposals; an empty list concludes the turn.
        proposed_calls: Vec<ToolProposal>,
        /// Provider-reported model that served the response, when available.
        ///
        /// This is an auditable response fact. The host-selected request model
        /// remains recorded separately on [`HarnessEvent::ModelRequested`].
        /// Unknown values are omitted so pre-field 0.3.0 records round-trip
        /// without gaining a field.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        served_model: Option<ModelName>,
        /// Provider-reported token usage, or `None` when usage is unknown.
        ///
        /// Reported zero counts remain known usage rather than `None`.
        usage: Option<ModelUsage>,
    },
    /// Rejects every unvalidated tool proposal in the pending model response.
    ToolProposalsRejected {
        /// Run containing the pending proposals.
        run_id: RunId,
        /// Turn that produced the pending proposals.
        turn_id: TurnId,
        /// Non-empty host explanation for rejecting the whole proposal batch.
        reason: String,
    },
    /// Records a model proposal after host validation and effect classification.
    ToolCallProposed {
        /// Run containing the proposal.
        run_id: RunId,
        /// Turn that produced the proposal.
        turn_id: TurnId,
        /// Host-validated call; its tool and input must match a pending proposal.
        call: ToolCall,
    },
    /// Records the policy decision for the pending validated call.
    PolicyEvaluated {
        /// Run containing the call.
        run_id: RunId,
        /// Pending call evaluated by policy.
        call_id: ToolCallId,
        /// Durable allow, approval, or denial decision.
        decision: PolicyDecision,
    },
    /// Records who granted a pending approval.
    ApprovalGranted {
        /// Run containing the call.
        run_id: RunId,
        /// Pending call approved for execution.
        call_id: ToolCallId,
        /// Human or host actor that granted approval.
        actor_id: ActorId,
    },
    /// Records who denied a pending approval and why.
    ApprovalDenied {
        /// Run containing the call.
        run_id: RunId,
        /// Pending call denied before execution.
        call_id: ToolCallId,
        /// Human or host actor that denied approval.
        actor_id: ActorId,
        /// Durable denial reason for audit and continuation context.
        reason: String,
    },
    /// Records that the host began executing the approved call.
    ToolStarted {
        /// Run containing the call.
        run_id: RunId,
        /// Approved call that crossed the execution boundary.
        call_id: ToolCallId,
    },
    /// Records the structured result returned by the running call.
    ToolFinished {
        /// Run containing the call.
        run_id: RunId,
        /// Result whose call id must match the running call.
        result: ToolResult,
    },
    /// Records that the running call failed without a result.
    ToolFailed {
        /// Run containing the call.
        run_id: RunId,
        /// Running call that failed.
        call_id: ToolCallId,
        /// Durable host-reported failure reason.
        reason: String,
    },
    /// Terminates a concluded turn as a successful run.
    RunFinished {
        /// Run that completed.
        run_id: RunId,
    },
    /// Terminates any started, nonterminal run as failed.
    RunFailed {
        /// Run that failed.
        run_id: RunId,
        /// Durable terminal failure reason.
        reason: String,
    },
}

impl HarnessEvent {
    /// Returns the owning run id without validating event order or phase.
    pub fn run_id(&self) -> &RunId {
        match self {
            Self::RunStarted { run_id, .. }
            | Self::ContextBuilt { run_id, .. }
            | Self::ContextCompacted { run_id, .. }
            | Self::ModelRequested { run_id, .. }
            | Self::ModelFailed { run_id, .. }
            | Self::ModelResponded { run_id, .. }
            | Self::ToolProposalsRejected { run_id, .. }
            | Self::ToolCallProposed { run_id, .. }
            | Self::PolicyEvaluated { run_id, .. }
            | Self::ApprovalGranted { run_id, .. }
            | Self::ApprovalDenied { run_id, .. }
            | Self::ToolStarted { run_id, .. }
            | Self::ToolFinished { run_id, .. }
            | Self::ToolFailed { run_id, .. }
            | Self::RunFinished { run_id }
            | Self::RunFailed { run_id, .. } => run_id,
        }
    }

    /// Returns the stable snake-case event name used in transition diagnostics.
    pub fn name(&self) -> &'static str {
        match self {
            Self::RunStarted { .. } => "run_started",
            Self::ContextBuilt { .. } => "context_built",
            Self::ContextCompacted { .. } => "context_compacted",
            Self::ModelRequested { .. } => "model_requested",
            Self::ModelFailed { .. } => "model_failed",
            Self::ModelResponded { .. } => "model_responded",
            Self::ToolProposalsRejected { .. } => "tool_proposals_rejected",
            Self::ToolCallProposed { .. } => "tool_call_proposed",
            Self::PolicyEvaluated { .. } => "policy_evaluated",
            Self::ApprovalGranted { .. } => "approval_granted",
            Self::ApprovalDenied { .. } => "approval_denied",
            Self::ToolStarted { .. } => "tool_started",
            Self::ToolFinished { .. } => "tool_finished",
            Self::ToolFailed { .. } => "tool_failed",
            Self::RunFinished { .. } => "run_finished",
            Self::RunFailed { .. } => "run_failed",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{HarnessEvent, ModelUsage, RecordedEvent};
    use crate::{
        ActorId, AgentId, ArtifactId, ContextFragment, ContextLane, ContextPack, EffectClass,
        Message, MessageRole, ModelName, PolicyDecision, ResultVisibility, RunId, ToolCall,
        ToolCallId, ToolName, ToolProposal, ToolResult, TurnId,
    };
    use serde_json::json;

    const ACCEPTED_V0_2_0_HARNESS_EVENT_JSON: &str =
        r#"{"event":"run_started","run_id":"run_1","agent_id":"agent_1"}"#;
    const REJECTED_UNKNOWN_FIELD_HARNESS_EVENT_JSON: &str =
        r#"{"event":"run_started","run_id":"run_1","agent_id":"agent_1","future_field":true}"#;
    const PRE_FIELD_V0_3_0_MODEL_RESPONDED_JSON: &str = r#"{"seq":7,"occurred_at_ms":1700000000000,"event":{"event":"model_responded","run_id":"run_1","turn_id":"turn_1","step":1,"output":{"role":"assistant","content":"Done."},"proposed_calls":[],"usage":{"input_tokens":12,"output_tokens":4}}}"#;

    #[test]
    fn harness_event_json_schema_is_fail_closed_and_v0_2_0_compatible() {
        let accepted: HarnessEvent =
            serde_json::from_str(ACCEPTED_V0_2_0_HARNESS_EVENT_JSON).unwrap();
        assert_eq!(
            accepted,
            HarnessEvent::RunStarted {
                run_id: RunId::new("run_1").unwrap(),
                agent_id: AgentId::new("agent_1").unwrap(),
            }
        );

        let error = serde_json::from_str::<HarnessEvent>(REJECTED_UNKNOWN_FIELD_HARNESS_EVENT_JSON)
            .unwrap_err();
        assert!(error.to_string().contains("unknown field `future_field`"));
    }

    #[test]
    fn recorded_event_json_fixtures_are_bidirectional() {
        let run_id = RunId::new("run_1").unwrap();
        let turn_id = TurnId::new("turn_1").unwrap();
        let call_id = ToolCallId::new("call_1").unwrap();
        let tool = ToolName::new("file.read").unwrap();

        let events = [
            HarnessEvent::RunStarted {
                run_id: run_id.clone(),
                agent_id: AgentId::new("agent_1").unwrap(),
            },
            HarnessEvent::ContextBuilt {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                context: ContextPack {
                    token_budget: 2_048,
                    fragments: vec![
                        ContextFragment {
                            lane: ContextLane::CurrentTask,
                            source: "user".into(),
                            content: "Summarize README.md".into(),
                            estimated_tokens: 5,
                        },
                        ContextFragment {
                            lane: ContextLane::RetrievedContext,
                            source: "README.md".into(),
                            content: "Platonic core".into(),
                            estimated_tokens: 3,
                        },
                    ],
                },
            },
            HarnessEvent::ContextCompacted {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                estimated_tokens_before: 4_096,
                estimated_tokens_after: 2_048,
                dropped_turn_start: 0,
                dropped_turn_end_exclusive: 3,
            },
            HarnessEvent::ModelRequested {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                step: 1,
                model: ModelName::new("model_1").unwrap(),
            },
            HarnessEvent::ModelFailed {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                step: 1,
                reason: "provider unavailable".into(),
            },
            HarnessEvent::ModelResponded {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                step: 1,
                output: Message {
                    role: MessageRole::Assistant,
                    content: "Reading the file.".into(),
                },
                proposed_calls: vec![ToolProposal {
                    tool: tool.clone(),
                    input: json!({ "path": "README.md" }),
                }],
                served_model: Some(ModelName::new("provider/model_1-2026-07-31").unwrap()),
                usage: Some(ModelUsage {
                    input_tokens: 12,
                    output_tokens: 4,
                }),
            },
            HarnessEvent::ToolProposalsRejected {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                reason: "proposal schema invalid".into(),
            },
            HarnessEvent::ToolCallProposed {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                call: ToolCall {
                    id: call_id.clone(),
                    tool: tool.clone(),
                    effect: EffectClass::ReadOnly,
                    input: json!({ "path": "README.md" }),
                },
            },
            HarnessEvent::PolicyEvaluated {
                run_id: run_id.clone(),
                call_id: call_id.clone(),
                decision: PolicyDecision::RequireApproval {
                    reason: "workspace write requires approval".into(),
                },
            },
            HarnessEvent::ApprovalGranted {
                run_id: run_id.clone(),
                call_id: call_id.clone(),
                actor_id: ActorId::new("actor_1").unwrap(),
            },
            HarnessEvent::ApprovalDenied {
                run_id: run_id.clone(),
                call_id: call_id.clone(),
                actor_id: ActorId::new("actor_1").unwrap(),
                reason: "not approved".into(),
            },
            HarnessEvent::ToolStarted {
                run_id: run_id.clone(),
                call_id: call_id.clone(),
            },
            HarnessEvent::ToolFinished {
                run_id: run_id.clone(),
                result: ToolResult {
                    call_id: call_id.clone(),
                    summary: "read 13 bytes".into(),
                    data: json!({ "contents": "Platonic core" }),
                    artifacts: vec![ArtifactId::new("artifact_1").unwrap()],
                    visibility: ResultVisibility::Both,
                },
            },
            HarnessEvent::ToolFailed {
                run_id: run_id.clone(),
                call_id,
                reason: "file not found".into(),
            },
            HarnessEvent::RunFinished {
                run_id: run_id.clone(),
            },
            HarnessEvent::RunFailed {
                run_id,
                reason: "model unavailable".into(),
            },
        ];

        for event in events {
            let name = event.name();
            let fixture = match &event {
                HarnessEvent::RunStarted { .. } => json!({
                    "seq": 7,
                    "occurred_at_ms": 1_700_000_000_000_u64,
                    "event": {
                        "event": "run_started",
                        "run_id": "run_1",
                        "agent_id": "agent_1"
                    }
                }),
                HarnessEvent::ContextBuilt { .. } => json!({
                    "seq": 7,
                    "occurred_at_ms": 1_700_000_000_000_u64,
                    "event": {
                        "event": "context_built",
                        "run_id": "run_1",
                        "turn_id": "turn_1",
                        "context": {
                            "token_budget": 2_048,
                            "fragments": [
                                {
                                    "lane": "current_task",
                                    "source": "user",
                                    "content": "Summarize README.md",
                                    "estimated_tokens": 5
                                },
                                {
                                    "lane": "retrieved_context",
                                    "source": "README.md",
                                    "content": "Platonic core",
                                    "estimated_tokens": 3
                                }
                            ]
                        }
                    }
                }),
                HarnessEvent::ContextCompacted { .. } => json!({
                    "seq": 7,
                    "occurred_at_ms": 1_700_000_000_000_u64,
                    "event": {
                        "event": "context_compacted",
                        "run_id": "run_1",
                        "turn_id": "turn_1",
                        "estimated_tokens_before": 4_096,
                        "estimated_tokens_after": 2_048,
                        "dropped_turn_start": 0,
                        "dropped_turn_end_exclusive": 3
                    }
                }),
                HarnessEvent::ModelRequested { .. } => json!({
                    "seq": 7,
                    "occurred_at_ms": 1_700_000_000_000_u64,
                    "event": {
                        "event": "model_requested",
                        "run_id": "run_1",
                        "turn_id": "turn_1",
                        "step": 1,
                        "model": "model_1"
                    }
                }),
                HarnessEvent::ModelFailed { .. } => json!({
                    "seq": 7,
                    "occurred_at_ms": 1_700_000_000_000_u64,
                    "event": {
                        "event": "model_failed",
                        "run_id": "run_1",
                        "turn_id": "turn_1",
                        "step": 1,
                        "reason": "provider unavailable"
                    }
                }),
                HarnessEvent::ModelResponded { .. } => json!({
                    "seq": 7,
                    "occurred_at_ms": 1_700_000_000_000_u64,
                    "event": {
                        "event": "model_responded",
                        "run_id": "run_1",
                        "turn_id": "turn_1",
                        "step": 1,
                        "output": {
                            "role": "assistant",
                            "content": "Reading the file."
                        },
                        "proposed_calls": [
                            {
                                "tool": "file.read",
                                "input": { "path": "README.md" }
                            }
                        ],
                        "served_model": "provider/model_1-2026-07-31",
                        "usage": {
                            "input_tokens": 12,
                            "output_tokens": 4
                        }
                    }
                }),
                HarnessEvent::ToolProposalsRejected { .. } => json!({
                    "seq": 7,
                    "occurred_at_ms": 1_700_000_000_000_u64,
                    "event": {
                        "event": "tool_proposals_rejected",
                        "run_id": "run_1",
                        "turn_id": "turn_1",
                        "reason": "proposal schema invalid"
                    }
                }),
                HarnessEvent::ToolCallProposed { .. } => json!({
                    "seq": 7,
                    "occurred_at_ms": 1_700_000_000_000_u64,
                    "event": {
                        "event": "tool_call_proposed",
                        "run_id": "run_1",
                        "turn_id": "turn_1",
                        "call": {
                            "id": "call_1",
                            "tool": "file.read",
                            "effect": "read_only",
                            "input": { "path": "README.md" }
                        }
                    }
                }),
                HarnessEvent::PolicyEvaluated { .. } => json!({
                    "seq": 7,
                    "occurred_at_ms": 1_700_000_000_000_u64,
                    "event": {
                        "event": "policy_evaluated",
                        "run_id": "run_1",
                        "call_id": "call_1",
                        "decision": {
                            "decision": "require_approval",
                            "reason": "workspace write requires approval"
                        }
                    }
                }),
                HarnessEvent::ApprovalGranted { .. } => json!({
                    "seq": 7,
                    "occurred_at_ms": 1_700_000_000_000_u64,
                    "event": {
                        "event": "approval_granted",
                        "run_id": "run_1",
                        "call_id": "call_1",
                        "actor_id": "actor_1"
                    }
                }),
                HarnessEvent::ApprovalDenied { .. } => json!({
                    "seq": 7,
                    "occurred_at_ms": 1_700_000_000_000_u64,
                    "event": {
                        "event": "approval_denied",
                        "run_id": "run_1",
                        "call_id": "call_1",
                        "actor_id": "actor_1",
                        "reason": "not approved"
                    }
                }),
                HarnessEvent::ToolStarted { .. } => json!({
                    "seq": 7,
                    "occurred_at_ms": 1_700_000_000_000_u64,
                    "event": {
                        "event": "tool_started",
                        "run_id": "run_1",
                        "call_id": "call_1"
                    }
                }),
                HarnessEvent::ToolFinished { .. } => json!({
                    "seq": 7,
                    "occurred_at_ms": 1_700_000_000_000_u64,
                    "event": {
                        "event": "tool_finished",
                        "run_id": "run_1",
                        "result": {
                            "call_id": "call_1",
                            "summary": "read 13 bytes",
                            "data": { "contents": "Platonic core" },
                            "artifacts": ["artifact_1"],
                            "visibility": "both"
                        }
                    }
                }),
                HarnessEvent::ToolFailed { .. } => json!({
                    "seq": 7,
                    "occurred_at_ms": 1_700_000_000_000_u64,
                    "event": {
                        "event": "tool_failed",
                        "run_id": "run_1",
                        "call_id": "call_1",
                        "reason": "file not found"
                    }
                }),
                HarnessEvent::RunFinished { .. } => json!({
                    "seq": 7,
                    "occurred_at_ms": 1_700_000_000_000_u64,
                    "event": {
                        "event": "run_finished",
                        "run_id": "run_1"
                    }
                }),
                HarnessEvent::RunFailed { .. } => json!({
                    "seq": 7,
                    "occurred_at_ms": 1_700_000_000_000_u64,
                    "event": {
                        "event": "run_failed",
                        "run_id": "run_1",
                        "reason": "model unavailable"
                    }
                }),
            };

            let expected = RecordedEvent {
                seq: 7,
                occurred_at_ms: 1_700_000_000_000,
                event,
            };
            let decoded: RecordedEvent = serde_json::from_value(fixture.clone()).unwrap();

            assert_eq!(decoded, expected, "failed to decode {name} fixture");
            assert_eq!(
                serde_json::to_value(&expected).unwrap(),
                fixture,
                "failed to encode {name} fixture"
            );
        }
    }

    #[test]
    fn model_response_usage_json_fixtures_are_bidirectional() {
        let fixtures = [
            (
                "known",
                json!({
                    "seq": 7,
                    "occurred_at_ms": 1_700_000_000_000_u64,
                    "event": {
                        "event": "model_responded",
                        "run_id": "run_1",
                        "turn_id": "turn_1",
                        "step": 1,
                        "output": {
                            "role": "assistant",
                            "content": "Done."
                        },
                        "proposed_calls": [],
                        "usage": {
                            "input_tokens": 12,
                            "output_tokens": 4
                        }
                    }
                }),
                Some(ModelUsage {
                    input_tokens: 12,
                    output_tokens: 4,
                }),
            ),
            (
                "zero",
                json!({
                    "seq": 7,
                    "occurred_at_ms": 1_700_000_000_000_u64,
                    "event": {
                        "event": "model_responded",
                        "run_id": "run_1",
                        "turn_id": "turn_1",
                        "step": 1,
                        "output": {
                            "role": "assistant",
                            "content": "Done."
                        },
                        "proposed_calls": [],
                        "usage": {
                            "input_tokens": 0,
                            "output_tokens": 0
                        }
                    }
                }),
                Some(ModelUsage {
                    input_tokens: 0,
                    output_tokens: 0,
                }),
            ),
            (
                "unknown",
                json!({
                    "seq": 7,
                    "occurred_at_ms": 1_700_000_000_000_u64,
                    "event": {
                        "event": "model_responded",
                        "run_id": "run_1",
                        "turn_id": "turn_1",
                        "step": 1,
                        "output": {
                            "role": "assistant",
                            "content": "Done."
                        },
                        "proposed_calls": [],
                        "usage": null
                    }
                }),
                None,
            ),
        ];

        for (name, fixture, usage) in fixtures {
            let expected = RecordedEvent {
                seq: 7,
                occurred_at_ms: 1_700_000_000_000,
                event: HarnessEvent::ModelResponded {
                    run_id: RunId::new("run_1").unwrap(),
                    turn_id: TurnId::new("turn_1").unwrap(),
                    step: 1,
                    output: Message {
                        role: MessageRole::Assistant,
                        content: "Done.".into(),
                    },
                    proposed_calls: vec![],
                    served_model: None,
                    usage,
                },
            };
            let decoded: RecordedEvent = serde_json::from_value(fixture.clone()).unwrap();

            assert_eq!(decoded, expected, "failed to decode {name} usage fixture");
            assert_eq!(
                serde_json::to_value(&expected).unwrap(),
                fixture,
                "failed to encode {name} usage fixture"
            );
        }
    }

    #[test]
    fn served_model_json_fixtures_are_bidirectional_and_v0_3_0_compatible() {
        let fixtures = [
            (
                "known",
                json!({
                    "seq": 7,
                    "occurred_at_ms": 1_700_000_000_000_u64,
                    "event": {
                        "event": "model_responded",
                        "run_id": "run_1",
                        "turn_id": "turn_1",
                        "step": 1,
                        "output": {
                            "role": "assistant",
                            "content": "Done."
                        },
                        "proposed_calls": [],
                        "served_model": "openai/gpt-5.2-2026-07-31",
                        "usage": null
                    }
                }),
                Some(ModelName::new("openai/gpt-5.2-2026-07-31").unwrap()),
            ),
            (
                "unknown",
                json!({
                    "seq": 7,
                    "occurred_at_ms": 1_700_000_000_000_u64,
                    "event": {
                        "event": "model_responded",
                        "run_id": "run_1",
                        "turn_id": "turn_1",
                        "step": 1,
                        "output": {
                            "role": "assistant",
                            "content": "Done."
                        },
                        "proposed_calls": [],
                        "usage": null
                    }
                }),
                None,
            ),
        ];

        for (name, fixture, served_model) in fixtures {
            let expected = RecordedEvent {
                seq: 7,
                occurred_at_ms: 1_700_000_000_000,
                event: HarnessEvent::ModelResponded {
                    run_id: RunId::new("run_1").unwrap(),
                    turn_id: TurnId::new("turn_1").unwrap(),
                    step: 1,
                    output: Message {
                        role: MessageRole::Assistant,
                        content: "Done.".into(),
                    },
                    proposed_calls: vec![],
                    served_model,
                    usage: None,
                },
            };
            let decoded: RecordedEvent = serde_json::from_value(fixture.clone()).unwrap();

            assert_eq!(decoded, expected, "failed to decode {name} fixture");
            assert_eq!(
                serde_json::to_value(&expected).unwrap(),
                fixture,
                "failed to encode {name} fixture"
            );
        }

        let pre_field_fixture =
            serde_json::from_str::<serde_json::Value>(PRE_FIELD_V0_3_0_MODEL_RESPONDED_JSON)
                .unwrap();
        let decoded: RecordedEvent = serde_json::from_value(pre_field_fixture.clone()).unwrap();
        let expected = RecordedEvent {
            seq: 7,
            occurred_at_ms: 1_700_000_000_000,
            event: HarnessEvent::ModelResponded {
                run_id: RunId::new("run_1").unwrap(),
                turn_id: TurnId::new("turn_1").unwrap(),
                step: 1,
                output: Message {
                    role: MessageRole::Assistant,
                    content: "Done.".into(),
                },
                proposed_calls: vec![],
                served_model: None,
                usage: Some(ModelUsage {
                    input_tokens: 12,
                    output_tokens: 4,
                }),
            },
        };

        assert_eq!(decoded, expected);
        assert_eq!(serde_json::to_value(expected).unwrap(), pre_field_fixture);
    }

    #[test]
    fn served_model_reuses_model_name_validation() {
        let mut fixture = json!({
            "seq": 7,
            "occurred_at_ms": 1_700_000_000_000_u64,
            "event": {
                "event": "model_responded",
                "run_id": "run_1",
                "turn_id": "turn_1",
                "step": 1,
                "output": {
                    "role": "assistant",
                    "content": "Done."
                },
                "proposed_calls": [],
                "served_model": " ",
                "usage": null
            }
        });
        let error = serde_json::from_value::<RecordedEvent>(fixture.clone()).unwrap_err();

        assert!(error.to_string().contains("ModelName cannot be empty"));

        fixture["event"]["served_model"] = json!(7);
        assert!(serde_json::from_value::<RecordedEvent>(fixture).is_err());
    }
}
