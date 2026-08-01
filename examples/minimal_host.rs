use platonic_core::{
    ActorId, AgentId, ContextFragment, ContextLane, ContextPack, EffectClass, HarnessEvent,
    Message, MessageRole, ModelName, ModelUsage, PolicyDecision, RecordedEvent, ResultVisibility,
    RunCommand, RunId, RunPhase, RunReadback, RunState, ToolCall, ToolCallId, ToolName,
    ToolProposal, ToolResult, TurnId,
};
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let run_id = RunId::new("run-1")?;
    let turn_id = TurnId::new("turn-1")?;
    let call_id = ToolCallId::new("call-1")?;
    let tool = ToolName::new("file.write")?;
    let input = json!({"path": "note.txt", "content": "done"});

    let context = ContextPack {
        token_budget: 32,
        fragments: vec![ContextFragment {
            lane: ContextLane::CurrentTask,
            source: "user".into(),
            content: "Write done to note.txt".into(),
            estimated_tokens: 6,
        }],
    };
    context.validate_budget()?;

    let proposal = ToolProposal {
        tool: tool.clone(),
        input: input.clone(),
    };
    let call = ToolCall {
        id: call_id.clone(),
        tool,
        effect: EffectClass::WorkspaceWrite,
        input,
    };

    let events = vec![
        HarnessEvent::RunStarted {
            run_id: run_id.clone(),
            agent_id: AgentId::new("agent-1")?,
        },
        HarnessEvent::ContextBuilt {
            run_id: run_id.clone(),
            turn_id: turn_id.clone(),
            context,
        },
        HarnessEvent::ModelRequested {
            run_id: run_id.clone(),
            turn_id: turn_id.clone(),
            step: 0,
            model: ModelName::new("model-1")?,
        },
        HarnessEvent::ModelResponded {
            run_id: run_id.clone(),
            turn_id: turn_id.clone(),
            step: 0,
            output: Message {
                role: MessageRole::Assistant,
                content: "I will write the file.".into(),
            },
            proposed_calls: vec![proposal],
            served_model: None,
            usage: Some(ModelUsage {
                input_tokens: 6,
                output_tokens: 6,
            }),
        },
        HarnessEvent::ToolCallProposed {
            run_id: run_id.clone(),
            turn_id,
            call,
        },
        HarnessEvent::PolicyEvaluated {
            run_id: run_id.clone(),
            call_id: call_id.clone(),
            decision: PolicyDecision::RequireApproval {
                reason: "workspace write".into(),
            },
        },
        HarnessEvent::ApprovalGranted {
            run_id: run_id.clone(),
            call_id: call_id.clone(),
            actor_id: ActorId::new("human-1")?,
        },
        HarnessEvent::ToolStarted {
            run_id: run_id.clone(),
            call_id: call_id.clone(),
        },
        HarnessEvent::ToolFinished {
            run_id: run_id.clone(),
            result: ToolResult {
                call_id,
                summary: "simulated write of note.txt".into(),
                data: json!({"bytes": 4}),
                artifacts: vec![],
                visibility: ResultVisibility::Both,
            },
        },
        HarnessEvent::RunFinished { run_id },
    ];

    let mut state = RunState::new();
    let mut ledger = Vec::new();

    for (seq, event) in events.into_iter().enumerate() {
        let record = RecordedEvent {
            seq: seq as u64,
            occurred_at_ms: seq as u64,
            event,
        };
        state.apply(&record)?;

        match seq {
            1 => assert!(matches!(
                state.pending_command(),
                Some(RunCommand::RequestModel { step: 0, .. })
            )),
            5 => assert!(matches!(
                state.pending_command(),
                Some(RunCommand::AwaitApproval { .. })
            )),
            6 => assert!(matches!(
                state.pending_command(),
                Some(RunCommand::ExecuteTool { .. })
            )),
            _ => {}
        }

        println!("{}", serde_json::to_string(&record)?);
        ledger.push(record);
    }

    let readback = RunReadback::from_events(&ledger)?;
    assert_eq!(readback.final_phase, RunPhase::Finished);

    eprintln!(
        "replay: phase={:?} events={} entries={}",
        readback.final_phase,
        ledger.len(),
        readback.entries.len()
    );

    Ok(())
}
