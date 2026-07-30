use crate::{
    AppError, AppResult,
    ledger::{self, SqliteLedger},
    paths::DefaultSqlitePath,
};
use platonic_core::{MessageRole, ReadbackEntry, RunReadback};
use std::path::Path;

pub fn replay_file(path: &Path) -> AppResult<String> {
    let records = ledger::read_records(path)?;
    let readback = RunReadback::from_events(&records)?;
    Ok(format_readback(&readback))
}

pub fn replay_sqlite(path: &Path, run_id: Option<&str>) -> AppResult<String> {
    if let Some(run_id) = run_id {
        let records = ledger::read_sqlite_records(path, Some(run_id))?;
        let readback = RunReadback::from_events(&records)?;
        return Ok(format_readback(&readback));
    }

    match ledger::read_latest_sqlite_session(path) {
        Ok(session) => format_session_readback(&session),
        Err(AppError::NoSqliteSessions) => {
            let records = ledger::read_sqlite_records(path, None)?;
            let readback = RunReadback::from_events(&records)?;
            Ok(format_readback(&readback))
        }
        Err(error) => Err(error),
    }
}

pub fn replay_default_sqlite(path: &DefaultSqlitePath, run_id: Option<&str>) -> AppResult<String> {
    let ledger = SqliteLedger::open_default_readonly(path)?;
    if let Some(run_id) = run_id {
        let records = ledger.read_run(run_id)?;
        let readback = RunReadback::from_events(&records)?;
        return Ok(format_readback(&readback));
    }

    match ledger.read_latest_session() {
        Ok(session) => format_session_readback(&session),
        Err(AppError::NoSqliteSessions) => {
            let (_, records) = ledger.read_latest_run()?;
            let readback = RunReadback::from_events(&records)?;
            Ok(format_readback(&readback))
        }
        Err(error) => Err(error),
    }
}

pub fn replay_sqlite_session(path: &Path, session_id: &str) -> AppResult<String> {
    let session = ledger::read_sqlite_session(path, session_id)?;
    format_session_readback(&session)
}

pub fn format_readback(readback: &RunReadback) -> String {
    let mut lines = Vec::new();
    lines.push(format!("final_phase: {:?}", readback.final_phase));
    lines.push(format!("next_seq: {}", readback.next_seq));

    for entry in &readback.entries {
        match entry {
            ReadbackEntry::ContextFragment { turn_id, fragment } => {
                lines.push(format!(
                    "[{turn_id}] context {:?} {}: {}",
                    fragment.lane, fragment.source, fragment.content
                ));
            }
            ReadbackEntry::ModelMessage { turn_id, message } => {
                let role = match message.role {
                    MessageRole::System => "system",
                    MessageRole::User => "user",
                    MessageRole::Assistant => "assistant",
                    MessageRole::Tool => "tool",
                };
                lines.push(format!("[{turn_id}] {role}: {}", message.content));
            }
            ReadbackEntry::ToolCall { turn_id, call } => {
                lines.push(format!(
                    "[{turn_id}] tool_call {} {}",
                    call.tool, call.input
                ));
            }
            ReadbackEntry::ToolResult { result } => {
                lines.push(format!(
                    "tool_result {}: {}",
                    result.call_id, result.summary
                ));
            }
            ReadbackEntry::PolicyDenied { call_id, reason } => {
                lines.push(format!("policy_denied {call_id}: {reason}"));
            }
            ReadbackEntry::ApprovalGranted { call_id, actor_id } => {
                lines.push(format!("approval_granted {call_id} by {actor_id}"));
            }
            ReadbackEntry::ApprovalDenied {
                call_id,
                actor_id,
                reason,
            } => {
                lines.push(format!("approval_denied {call_id} by {actor_id}: {reason}"));
            }
            ReadbackEntry::ToolFailed { call_id, reason } => {
                lines.push(format!("tool_failed {call_id}: {reason}"));
            }
            ReadbackEntry::ContextCompacted { .. }
            | ReadbackEntry::ModelFailed { .. }
            | ReadbackEntry::ToolProposalsRejected { .. } => {}
        }
    }

    lines.join("\n")
}

pub(crate) fn format_session_readback(session: &ledger::SessionRecords) -> AppResult<String> {
    let mut lines = vec![format!("session_id: {}", session.session_id)];
    for run in &session.runs {
        lines.push(format!("run_id: {}", run.run_id));
        let readback = RunReadback::from_events(&run.records)?;
        lines.push(format_readback(&readback));
    }
    Ok(lines.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ledger::SqliteLedger;
    use platonic_core::{
        ActorId, AgentId, ContextPack, EffectClass, HarnessEvent, Message, MessageRole, ModelName,
        ModelUsage, PolicyDecision, ResultVisibility, RunId, ToolCall, ToolCallId, ToolName,
        ToolProposal, ToolResult, TurnId,
    };
    use serde_json::json;

    #[test]
    fn sqlite_replay_without_run_reads_latest_session() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent.db");
        let mut ledger = SqliteLedger::open_or_create(&path).unwrap();
        let run_1 = RunId::new("run_1").unwrap();
        let run_2 = RunId::new("run_2").unwrap();

        ledger
            .begin_session_run("session_1", &run_1, "first", true)
            .unwrap();
        ledger
            .append(
                "run_1",
                &record(
                    0,
                    HarnessEvent::RunStarted {
                        run_id: run_1.clone(),
                        agent_id: AgentId::new("plato").unwrap(),
                    },
                ),
            )
            .unwrap();
        ledger
            .append(
                "run_1",
                &record(
                    1,
                    HarnessEvent::RunFailed {
                        run_id: run_1.clone(),
                        reason: "synthetic failure".into(),
                    },
                ),
            )
            .unwrap();
        ledger.finish_session_run(&run_1, "first answer").unwrap();
        ledger
            .begin_session_run("session_1", &run_2, "second", false)
            .unwrap();
        ledger
            .append(
                "run_2",
                &record(
                    0,
                    HarnessEvent::RunStarted {
                        run_id: run_2.clone(),
                        agent_id: AgentId::new("plato").unwrap(),
                    },
                ),
            )
            .unwrap();
        ledger
            .append(
                "run_2",
                &record(
                    1,
                    HarnessEvent::RunFailed {
                        run_id: run_2.clone(),
                        reason: "synthetic failure".into(),
                    },
                ),
            )
            .unwrap();
        ledger.finish_session_run(&run_2, "second answer").unwrap();

        let replay = replay_sqlite(&path, None).unwrap();

        assert!(replay.contains("session_id: session_1"));
        assert!(replay.contains("run_id: run_1"));
        assert!(replay.contains("run_id: run_2"));
        assert_eq!(replay.matches("final_phase: Failed").count(), 2);
    }

    #[test]
    fn replay_shows_shell_exec_success_path() {
        let run_id = RunId::new("run_1").unwrap();
        let call_id = ToolCallId::new("call_1").unwrap();
        let mut records = shell_tool_prefix(&run_id, &call_id);
        records.extend([
            record(
                5,
                HarnessEvent::PolicyEvaluated {
                    run_id: run_id.clone(),
                    call_id: call_id.clone(),
                    decision: PolicyDecision::RequireApproval {
                        reason: "shell.exec requires explicit local approval".into(),
                    },
                },
            ),
            record(
                6,
                HarnessEvent::ApprovalGranted {
                    run_id: run_id.clone(),
                    call_id: call_id.clone(),
                    actor_id: ActorId::new("stdin").unwrap(),
                },
            ),
            record(
                7,
                HarnessEvent::ToolStarted {
                    run_id: run_id.clone(),
                    call_id: call_id.clone(),
                },
            ),
            record(
                8,
                HarnessEvent::ToolFinished {
                    run_id: run_id.clone(),
                    result: ToolResult {
                        call_id: call_id.clone(),
                        summary: "shell.exec exited 0 in 1ms".into(),
                        data: json!({"exit_code": 0}),
                        artifacts: vec![],
                        visibility: ResultVisibility::Both,
                    },
                },
            ),
            record(9, HarnessEvent::RunFinished { run_id }),
        ]);
        let readback = RunReadback::from_events(&records).unwrap();

        let replay = format_readback(&readback);

        assert!(replay.contains("tool_call shell.exec"));
        assert!(replay.contains("approval_granted call_1 by stdin"));
        assert!(replay.contains("tool_result call_1: shell.exec exited 0"));
        assert!(replay.contains("final_phase: Finished"));
    }

    #[test]
    fn replay_shows_shell_exec_failure_path() {
        let run_id = RunId::new("run_1").unwrap();
        let call_id = ToolCallId::new("call_1").unwrap();
        let mut records = shell_tool_prefix(&run_id, &call_id);
        records.extend([
            record(
                5,
                HarnessEvent::PolicyEvaluated {
                    run_id: run_id.clone(),
                    call_id: call_id.clone(),
                    decision: PolicyDecision::RequireApproval {
                        reason: "shell.exec requires explicit local approval".into(),
                    },
                },
            ),
            record(
                6,
                HarnessEvent::ApprovalGranted {
                    run_id: run_id.clone(),
                    call_id: call_id.clone(),
                    actor_id: ActorId::new("stdin").unwrap(),
                },
            ),
            record(
                7,
                HarnessEvent::ToolStarted {
                    run_id: run_id.clone(),
                    call_id: call_id.clone(),
                },
            ),
            record(
                8,
                HarnessEvent::ToolFailed {
                    run_id: run_id.clone(),
                    call_id,
                    reason: "shell.exec timed out after 1s".into(),
                },
            ),
            record(
                9,
                HarnessEvent::RunFailed {
                    run_id,
                    reason: "shell.exec timed out after 1s".into(),
                },
            ),
        ]);
        let readback = RunReadback::from_events(&records).unwrap();

        let replay = format_readback(&readback);

        assert!(replay.contains("tool_failed call_1: shell.exec timed out after 1s"));
        assert!(replay.contains("final_phase: Failed"));
    }

    fn shell_call(call_id: ToolCallId) -> ToolCall {
        ToolCall {
            id: call_id,
            tool: ToolName::new("shell.exec").unwrap(),
            effect: EffectClass::ExternalSideEffect,
            input: json!({"command": "cargo test"}),
        }
    }

    fn shell_tool_prefix(
        run_id: &RunId,
        call_id: &ToolCallId,
    ) -> Vec<platonic_core::RecordedEvent> {
        let turn_id = TurnId::new("turn_1").unwrap();
        vec![
            record(
                0,
                HarnessEvent::RunStarted {
                    run_id: run_id.clone(),
                    agent_id: AgentId::new("plato").unwrap(),
                },
            ),
            record(
                1,
                HarnessEvent::ContextBuilt {
                    run_id: run_id.clone(),
                    turn_id: turn_id.clone(),
                    context: ContextPack {
                        token_budget: 4000,
                        fragments: vec![],
                    },
                },
            ),
            record(
                2,
                HarnessEvent::ModelRequested {
                    run_id: run_id.clone(),
                    turn_id: turn_id.clone(),
                    step: 0,
                    model: ModelName::new("test-model").unwrap(),
                },
            ),
            record(
                3,
                HarnessEvent::ModelResponded {
                    run_id: run_id.clone(),
                    turn_id: turn_id.clone(),
                    step: 0,
                    output: Message {
                        role: MessageRole::Assistant,
                        content: String::new(),
                    },
                    proposed_calls: vec![ToolProposal {
                        tool: ToolName::new("shell.exec").unwrap(),
                        input: json!({"command": "cargo test"}),
                    }],
                    usage: Some(ModelUsage {
                        input_tokens: 0,
                        output_tokens: 0,
                    }),
                },
            ),
            record(
                4,
                HarnessEvent::ToolCallProposed {
                    run_id: run_id.clone(),
                    turn_id,
                    call: shell_call(call_id.clone()),
                },
            ),
        ]
    }

    fn record(seq: u64, event: HarnessEvent) -> platonic_core::RecordedEvent {
        platonic_core::RecordedEvent {
            seq,
            occurred_at_ms: seq,
            event,
        }
    }
}
