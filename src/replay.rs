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
    let ledger = SqliteLedger::open_readonly(path)?;
    replay_open_sqlite(&ledger, run_id)
}

pub fn replay_default_sqlite(path: &DefaultSqlitePath, run_id: Option<&str>) -> AppResult<String> {
    let ledger = SqliteLedger::open_default_readonly(path)?;
    replay_open_sqlite(&ledger, run_id)
}

fn replay_open_sqlite(ledger: &SqliteLedger, run_id: Option<&str>) -> AppResult<String> {
    if let Some(run_id) = run_id {
        let records = ledger.read_run(run_id)?;
        let readback = RunReadback::from_events(&records)?;
        let mut output = format_readback(&readback);
        for envelope in ledger.read_voice_events(run_id)? {
            output.push_str("\nvoice_event: ");
            output.push_str(&serde_json::to_string(&envelope)?);
        }
        return Ok(output);
    }

    if ledger.is_legacy_schema() {
        let (_, records) = ledger.read_latest_run()?;
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
            ReadbackEntry::ContextCompacted {
                turn_id,
                estimated_tokens_before,
                estimated_tokens_after,
                dropped_turn_start,
                dropped_turn_end_exclusive,
            } => {
                lines.push(format!(
                    "[{turn_id}] context_compacted estimated_tokens={estimated_tokens_before}->{estimated_tokens_after} dropped_turns={dropped_turn_start}..{dropped_turn_end_exclusive}"
                ));
            }
            ReadbackEntry::ContextFragment { turn_id, fragment } => {
                lines.push(format!(
                    "[{turn_id}] context {:?} {}: {}",
                    fragment.lane, fragment.source, fragment.content
                ));
            }
            ReadbackEntry::ModelMessage {
                turn_id, message, ..
            } => {
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
            ReadbackEntry::ModelFailed { .. } | ReadbackEntry::ToolProposalsRejected { .. } => {}
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
    use crate::{ledger::SqliteLedger, voice_session::VoiceEvent};
    use platonic_core::{
        ActorId, AgentId, ContextPack, EffectClass, HarnessEvent, Message, MessageRole, ModelName,
        ModelUsage, PolicyDecision, ResultVisibility, RunId, ToolCall, ToolCallId, ToolName,
        ToolProposal, ToolResult, TurnId,
    };
    use rusqlite::{Connection, params};
    use serde_json::json;

    const V1_JSONL_FIXTURE: &str = concat!(
        r#"{"v":1,"record":{"seq":0,"occurred_at_ms":0,"event":{"event":"run_started","run_id":"run_v1","agent_id":"plato"}}}"#,
        "\n",
        r#"{"v":1,"record":{"seq":1,"occurred_at_ms":1,"event":{"event":"context_built","run_id":"run_v1","turn_id":"turn_1","context":{"fragments":[],"token_budget":4000}}}}"#,
        "\n",
        r#"{"v":1,"record":{"seq":2,"occurred_at_ms":2,"event":{"event":"model_requested","run_id":"run_v1","turn_id":"turn_1","step":0,"model":"test-model"}}}"#,
        "\n",
        r#"{"v":1,"record":{"seq":3,"occurred_at_ms":3,"event":{"event":"model_responded","run_id":"run_v1","turn_id":"turn_1","step":0,"output":{"role":"assistant","content":"old answer"},"proposed_calls":[],"usage":{"input_tokens":8,"output_tokens":3}}}}"#,
        "\n",
        r#"{"v":1,"record":{"seq":4,"occurred_at_ms":4,"event":{"event":"run_finished","run_id":"run_v1"}}}"#,
        "\n",
    );
    const V1_SQLITE_SCHEMA: &str = r#"
        CREATE TABLE ledger_events (
          run_id TEXT NOT NULL,
          seq INTEGER NOT NULL,
          occurred_at_ms INTEGER NOT NULL,
          v INTEGER NOT NULL,
          event_json TEXT NOT NULL,
          PRIMARY KEY (run_id, seq)
        );
        PRAGMA user_version = 1;
    "#;

    #[test]
    fn replay_reads_v1_jsonl_and_maps_usage_object_to_known() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v1-events.jsonl");
        std::fs::write(&path, V1_JSONL_FIXTURE).unwrap();

        let records = ledger::read_records(&path).unwrap();
        assert_v1_usage_is_known(&records);

        let replay = replay_file(&path).unwrap();
        assert!(replay.contains("final_phase: Finished"));
        assert!(replay.contains("[turn_1] assistant: old answer"));
    }

    #[test]
    fn replay_reads_v1_sqlite_and_maps_usage_object_to_known() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v1-events.db");
        write_v1_sqlite_fixture(&path);
        let bytes_before = std::fs::read(&path).unwrap();

        let records = ledger::read_sqlite_records(&path, Some("run_v1")).unwrap();
        assert_v1_usage_is_known(&records);

        let replay = replay_sqlite(&path, None).unwrap();
        assert!(replay.contains("final_phase: Finished"));
        assert!(replay.contains("[turn_1] assistant: old answer"));
        assert_eq!(replay_sqlite(&path, Some("run_v1")).unwrap(), replay);
        assert_eq!(std::fs::read(&path).unwrap(), bytes_before);
    }

    #[test]
    fn write_open_migrates_literal_v1_once_and_preserves_events() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v1-events.db");
        write_v1_sqlite_fixture(&path);
        let replay_before = replay_sqlite(&path, None).unwrap();
        let selected_before = replay_sqlite(&path, Some("run_v1")).unwrap();

        let ledger = SqliteLedger::open_or_create(&path).unwrap();
        let records = ledger.read_run("run_v1").unwrap();
        assert_v1_usage_is_known(&records);
        drop(ledger);

        let connection = Connection::open(&path).unwrap();
        let schema_version: u32 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(schema_version, 3);
        let tables = connection
            .prepare(
                "SELECT name FROM sqlite_schema
                 WHERE type = 'table'
                 ORDER BY name ASC",
            )
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            tables,
            ["ledger_events", "session_runs", "sessions", "voice_events"]
        );
        let envelope_versions = connection
            .prepare("SELECT v FROM ledger_events ORDER BY seq ASC")
            .unwrap()
            .query_map([], |row| row.get::<_, u32>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(envelope_versions, vec![1; 5]);
        drop(connection);

        let bytes_after_migration = std::fs::read(&path).unwrap();
        drop(SqliteLedger::open_or_create(&path).unwrap());
        assert_eq!(std::fs::read(&path).unwrap(), bytes_after_migration);
        assert_eq!(replay_sqlite(&path, None).unwrap(), replay_before);
        assert_eq!(
            replay_sqlite(&path, Some("run_v1")).unwrap(),
            selected_before
        );
    }

    #[test]
    fn replay_rejects_future_schema_before_table_queries_without_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v4-events.db");
        let connection = Connection::open(&path).unwrap();
        connection.pragma_update(None, "user_version", 4).unwrap();
        drop(connection);
        let bytes_before = std::fs::read(&path).unwrap();

        assert!(matches!(
            replay_sqlite(&path, None),
            Err(AppError::SqliteSchemaVersion {
                expected: 3,
                actual: 4
            })
        ));
        assert_eq!(std::fs::read(&path).unwrap(), bytes_before);
    }

    #[test]
    fn replay_preserves_typed_future_row_envelope_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("future-envelope.db");
        drop(SqliteLedger::open_or_create(&path).unwrap());
        let connection = Connection::open(&path).unwrap();
        connection
            .execute(
                "INSERT INTO ledger_events (run_id, seq, occurred_at_ms, v, event_json)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    "run_future",
                    0,
                    0,
                    3,
                    r#"{"event":"run_started","run_id":"run_future","agent_id":"plato"}"#
                ],
            )
            .unwrap();
        drop(connection);

        assert!(matches!(
            replay_sqlite(&path, Some("run_future")),
            Err(AppError::LedgerVersion {
                expected: 2,
                actual: 3
            })
        ));
    }

    #[test]
    fn sqlite_v3_replay_preserves_latest_session_and_exact_run_selection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent.db");
        let mut ledger = SqliteLedger::open_or_create(&path).unwrap();
        let run_0 = RunId::new("run_0").unwrap();
        let run_1 = RunId::new("run_1").unwrap();
        let run_2 = RunId::new("run_2").unwrap();

        ledger
            .begin_session_run("session_0", &run_0, "older", true)
            .unwrap();
        ledger
            .append(
                "run_0",
                &record(
                    0,
                    HarnessEvent::RunStarted {
                        run_id: run_0.clone(),
                        agent_id: AgentId::new("plato").unwrap(),
                    },
                ),
            )
            .unwrap();
        ledger
            .fail_session_run(&run_0, "older synthetic failure", false)
            .unwrap();
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
            .fail_session_run(&run_1, "synthetic failure", false)
            .unwrap();
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
                    HarnessEvent::ContextBuilt {
                        run_id: run_2.clone(),
                        turn_id: TurnId::new("turn_2").unwrap(),
                        context: ContextPack {
                            fragments: vec![],
                            token_budget: 4_000,
                        },
                    },
                ),
            )
            .unwrap();
        ledger
            .fail_session_run(&run_2, "synthetic failure", false)
            .unwrap();

        let exact = replay_sqlite(&path, Some("run_1")).unwrap();
        let latest = replay_sqlite(&path, None).unwrap();

        assert_eq!(
            exact,
            "final_phase: Failed { reason: \"synthetic failure\" }\nnext_seq: 2"
        );
        assert!(latest.contains("session_id: session_1"));
        assert!(!latest.contains("session_id: session_0"));
        assert!(!latest.contains("run_id: run_0"));
        assert!(latest.contains("run_id: run_1"));
        assert!(latest.contains("run_id: run_2"));
        assert!(latest.contains("next_seq: 2"));
        assert!(latest.contains("next_seq: 3"));
        assert_eq!(latest.matches("final_phase: Failed").count(), 2);
    }

    #[test]
    fn twenty_turn_voice_ttfa_readback_is_exact_readonly_and_interruption_aware() {
        const AU2_DEVICE_TTFA_US: [u64; 20] = [
            63_856, 63_647, 52_563, 58_177, 57_994, 63_259, 46_904, 52_527, 57_985, 52_907, 48_019,
            52_296, 53_022, 63_165, 52_605, 52_654, 52_653, 52_711, 58_470, 57_639,
        ];
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("voice-events.db");
        let mut ledger = SqliteLedger::open_or_create(&path).unwrap();
        let expected_ttfa = AU2_DEVICE_TTFA_US
            .iter()
            .map(|ttfa_us| ttfa_us / 1_000)
            .collect::<Vec<_>>();
        let mut expected_streams = Vec::new();

        for (turn, ttfa_ms) in expected_ttfa.iter().copied().enumerate() {
            let run_id = RunId::new(format!("run_voice_{turn:02}")).unwrap();
            let turn_id = TurnId::new("turn_1").unwrap();
            append_finished_run(&mut ledger, &run_id, &turn_id, turn);
            let interrupted = turn % 5 == 4;
            let mut events = Vec::new();
            if turn == 0 {
                events.push(VoiceEvent::VoiceCaptured {
                    run_id: run_id.clone(),
                    turn_id: turn_id.clone(),
                    transcript_sha256: "b".repeat(64),
                    transcript_bytes: 17,
                    transcript_span_ms: 920,
                    input_frames: 44_160,
                    output_frames: 14_720,
                    vad_start_sample: 160,
                    vad_speech_end_sample: 13_120,
                    vad_close_sample: 14_720,
                    vad_close_to_final_us: 110_000,
                    normalization_resampling_us: 1_100,
                });
            }
            events.push(VoiceEvent::VoiceSpoken {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                ttfa_ms,
                sentence_count: if interrupted { 2 } else { 3 },
                interrupted_at: interrupted.then_some(1),
            });
            if interrupted {
                events.push(VoiceEvent::VoiceInterrupted {
                    run_id: run_id.clone(),
                    turn_id,
                    spoken_prefix: format!("audible prefix {turn}"),
                    delta_index: u64::try_from(turn).unwrap() + 10,
                });
            }
            expected_streams.push((
                run_id.to_string(),
                ledger.append_voice_events(&events).unwrap(),
            ));
        }
        drop(ledger);

        let connection = Connection::open(&path).unwrap();
        let rows_before = (
            connection
                .query_row("SELECT COUNT(*) FROM ledger_events", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            connection
                .query_row("SELECT COUNT(*) FROM voice_events", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
        );
        drop(connection);
        let bytes_before = std::fs::read(&path).unwrap();
        let readonly = SqliteLedger::open_readonly(&path).unwrap();
        let mut observed_ttfa = Vec::new();

        for (run_id, expected) in &expected_streams {
            let first = replay_sqlite(&path, Some(run_id)).unwrap();
            let second = replay_sqlite(&path, Some(run_id)).unwrap();
            assert_eq!(first, second);
            for envelope in expected {
                assert!(first.contains(&format!(
                    "voice_event: {}",
                    serde_json::to_string(envelope).unwrap()
                )));
            }
            let readback = readonly.read_voice_events(run_id).unwrap();
            assert_eq!(&readback, expected);
            observed_ttfa.extend(
                readback
                    .iter()
                    .filter_map(|envelope| match &envelope.event {
                        VoiceEvent::VoiceSpoken { ttfa_ms, .. } => Some(*ttfa_ms),
                        VoiceEvent::VoiceCaptured { .. } | VoiceEvent::VoiceInterrupted { .. } => {
                            None
                        }
                    }),
            );
        }
        drop(readonly);

        assert_eq!(observed_ttfa, expected_ttfa);
        assert_eq!(std::fs::read(&path).unwrap(), bytes_before);
        let connection = Connection::open(&path).unwrap();
        let rows_after = (
            connection
                .query_row("SELECT COUNT(*) FROM ledger_events", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            connection
                .query_row("SELECT COUNT(*) FROM voice_events", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
        );
        assert_eq!(rows_after, rows_before);
    }

    #[test]
    fn replay_formats_context_compaction_with_all_fields() {
        let run_id = RunId::new("run_1").unwrap();
        let turn_id = TurnId::new("turn_1").unwrap();
        let records = vec![
            record(
                0,
                HarnessEvent::RunStarted {
                    run_id: run_id.clone(),
                    agent_id: AgentId::new("plato").unwrap(),
                },
            ),
            record(
                1,
                HarnessEvent::ContextCompacted {
                    run_id: run_id.clone(),
                    turn_id: turn_id.clone(),
                    estimated_tokens_before: 321,
                    estimated_tokens_after: 123,
                    dropped_turn_start: 0,
                    dropped_turn_end_exclusive: 2,
                },
            ),
            record(
                2,
                HarnessEvent::ContextBuilt {
                    run_id: run_id.clone(),
                    turn_id,
                    context: ContextPack {
                        token_budget: 500,
                        fragments: vec![],
                    },
                },
            ),
            record(
                3,
                HarnessEvent::RunFailed {
                    run_id,
                    reason: "synthetic failure".into(),
                },
            ),
        ];
        let readback = RunReadback::from_events(&records).unwrap();

        let replay = format_readback(&readback);

        assert_eq!(replay, format_readback(&readback));
        assert!(replay.lines().any(|line| {
            line == "[turn_1] context_compacted estimated_tokens=321->123 dropped_turns=0..2"
        }));
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

    fn append_finished_run(
        ledger: &mut SqliteLedger,
        run_id: &RunId,
        turn_id: &TurnId,
        turn: usize,
    ) {
        let events = [
            HarnessEvent::RunStarted {
                run_id: run_id.clone(),
                agent_id: AgentId::new("plato").unwrap(),
            },
            HarnessEvent::ContextBuilt {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                context: ContextPack {
                    token_budget: 4_000,
                    fragments: vec![],
                },
            },
            HarnessEvent::ModelRequested {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                step: 0,
                model: ModelName::new("test-model").unwrap(),
            },
            HarnessEvent::ModelResponded {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                step: 0,
                output: Message {
                    role: MessageRole::Assistant,
                    content: format!("voice answer {turn}"),
                },
                proposed_calls: vec![],
                served_model: None,
                usage: Some(ModelUsage {
                    input_tokens: 4,
                    output_tokens: 3,
                }),
            },
            HarnessEvent::RunFinished {
                run_id: run_id.clone(),
            },
        ];
        for (sequence, event) in events.into_iter().enumerate() {
            ledger
                .append(
                    run_id.as_str(),
                    &record(u64::try_from(sequence).unwrap(), event),
                )
                .unwrap();
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
                    served_model: None,
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

    fn assert_v1_usage_is_known(records: &[platonic_core::RecordedEvent]) {
        assert!(matches!(
            &records[3].event,
            HarnessEvent::ModelResponded {
                served_model: None,
                usage: Some(ModelUsage {
                    input_tokens: 8,
                    output_tokens: 3,
                }),
                ..
            }
        ));
    }

    fn write_v1_sqlite_fixture(path: &Path) {
        let connection = Connection::open(path).unwrap();
        connection.execute_batch(V1_SQLITE_SCHEMA).unwrap();
        for line in V1_JSONL_FIXTURE.lines() {
            let line: serde_json::Value = serde_json::from_str(line).unwrap();
            let record = &line["record"];
            connection
                .execute(
                    "INSERT INTO ledger_events (run_id, seq, occurred_at_ms, v, event_json)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        "run_v1",
                        record["seq"].as_i64().unwrap(),
                        record["occurred_at_ms"].as_i64().unwrap(),
                        line["v"].as_i64().unwrap(),
                        serde_json::to_string(&record["event"]).unwrap(),
                    ],
                )
                .unwrap();
        }
    }
}
