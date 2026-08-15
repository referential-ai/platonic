//! Read-only local ledger replay for clients that do not have a server binary.

use platonic_core::{HarnessEvent, MessageRole, ReadbackEntry, RecordedEvent, RunReadback};
use platonic_protocol::{VOICE_EVENT_VERSION, VoiceEvent, VoiceEventEnvelope};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde::Deserialize;
use std::{
    fs,
    path::{Path, PathBuf},
};

const LEDGER_VERSION: u32 = 2;
const SQLITE_SCHEMA_VERSION: u32 = 5;

/// Failure returned by read-only ledger replay.
#[derive(Debug, thiserror::Error)]
pub enum OfflineError {
    /// The ledger envelope revision is newer than this client.
    #[error("ledger version mismatch: expected {expected}, actual {actual}")]
    LedgerVersion {
        /// Highest supported revision.
        expected: u32,
        /// Revision read from the ledger.
        actual: u32,
    },
    /// The SQLite schema revision is newer than this client.
    #[error("sqlite schema version mismatch: expected {expected}, actual {actual}")]
    SqliteSchemaVersion {
        /// Highest supported schema revision.
        expected: u32,
        /// Schema revision read from SQLite.
        actual: u32,
    },
    /// The requested ledger contains no runs.
    #[error("sqlite ledger has no runs")]
    NoRuns,
    /// The selected run does not exist.
    #[error("run not found in sqlite ledger: {0}")]
    RunNotFound(String),
    /// The current directory has no workspace registry record.
    #[error("workspace is not registered: {0}; run platonic workspace create")]
    WorkspaceUnregistered(PathBuf),
    /// Local filesystem I/O failed.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// JSON decoding failed.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    /// SQLite readback failed.
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// A voice companion stream violated its durable envelope contract.
    #[error("voice event contract failed: {0}")]
    VoiceEventContract(String),
    /// The sans-I/O kernel rejected the durable event stream.
    #[error("core error: {0}")]
    Core(#[from] platonic_core::Error),
}

/// Result type returned by offline replay operations.
pub type OfflineResult<T> = Result<T, OfflineError>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LedgerLine {
    v: u32,
    record: RecordedEvent,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct VoiceLedgerLine {
    voice_events: Vec<VoiceEventEnvelope>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum PersistedLedgerLine {
    Record(LedgerLine),
    Voice(VoiceLedgerLine),
}

/// Resolves a workspace ledger through the server registry without contacting it.
pub fn workspace_ledger_path(
    server_db_path: &Path,
    workspace_root: &Path,
) -> OfflineResult<PathBuf> {
    let workspace_root = workspace_root.canonicalize()?;
    let connection = Connection::open_with_flags(server_db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    connection
        .query_row(
            "SELECT ledger_path FROM workspaces WHERE root = ?1
             ORDER BY created_at_ms, name LIMIT 1",
            params![workspace_root.to_string_lossy()],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(PathBuf::from)
        .ok_or(OfflineError::WorkspaceUnregistered(workspace_root))
}

/// Replays one JSONL ledger without contacting or linking the server.
pub fn replay_file(path: &Path) -> OfflineResult<String> {
    let (records, voice_events) = read_jsonl(path)?;
    format_jsonl(&records, &voice_events)
}

fn format_jsonl(
    records: &[RecordedEvent],
    voice_events: &[VoiceEventEnvelope],
) -> OfflineResult<String> {
    let mut output = format_records(records)?;
    for envelope in voice_events {
        output.push_str("\nvoice_event: ");
        output.push_str(&serde_json::to_string(envelope)?);
    }
    Ok(output)
}

/// Replays a selected or latest SQLite run without contacting or linking the server.
pub fn replay_sqlite(path: &Path, run_id: Option<&str>) -> OfflineResult<String> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let schema_version: u32 =
        connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if schema_version > SQLITE_SCHEMA_VERSION {
        return Err(OfflineError::SqliteSchemaVersion {
            expected: SQLITE_SCHEMA_VERSION,
            actual: schema_version,
        });
    }

    if let Some(run_id) = run_id {
        return format_ledger_run(path, &connection, schema_version, run_id);
    }

    if schema_version >= 2
        && let Some(session_id) = connection
            .query_row(
                "SELECT session_id FROM sessions ORDER BY updated_at_ms DESC, session_id DESC LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?
    {
        let mut statement = connection.prepare(
            "SELECT run_id FROM session_runs WHERE session_id = ?1 ORDER BY session_index ASC",
        )?;
        let run_ids = statement
            .query_map(params![session_id], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        let mut output = vec![format!("session_id: {session_id}")];
        for run_id in run_ids {
            output.push(format!("run_id: {run_id}"));
            output.push(format_ledger_run(
                path,
                &connection,
                schema_version,
                &run_id,
            )?);
        }
        return Ok(output.join("\n"));
    }

    let run_id = latest_run_id(&connection)?.ok_or(OfflineError::NoRuns)?;
    format_ledger_run(path, &connection, schema_version, &run_id)
}

fn latest_run_id(connection: &Connection) -> OfflineResult<Option<String>> {
    Ok(connection
        .query_row(
            "SELECT run_id FROM ledger_events GROUP BY run_id ORDER BY MAX(occurred_at_ms) DESC, run_id DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()?)
}

fn format_ledger_run(
    sqlite_path: &Path,
    connection: &Connection,
    schema_version: u32,
    run_id: &str,
) -> OfflineResult<String> {
    let jsonl_path = run_jsonl_path(sqlite_path, run_id)?;
    if jsonl_path.exists() {
        let (records, voice_events) = read_jsonl(&jsonl_path)?;
        validate_record_run_ids(&records, run_id)?;
        return format_jsonl(&records, &voice_events);
    }
    let records = read_run(connection, run_id)?;
    if records.is_empty() {
        return Err(OfflineError::RunNotFound(run_id.into()));
    }
    let mut output = format_records(&records)?;
    if schema_version >= 3 {
        let mut statement = connection.prepare(
            "SELECT sequence, v, event_json FROM voice_events WHERE run_id = ?1 ORDER BY sequence ASC",
        )?;
        let events = statement
            .query_map(params![run_id], |row| {
                let sequence = u64::try_from(row.get::<_, i64>(0)?).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::new(error),
                    )
                })?;
                let v = row.get::<_, u32>(1)?;
                let event_json = row.get::<_, String>(2)?;
                let event = serde_json::from_str::<VoiceEvent>(&event_json).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        2,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?;
                Ok(VoiceEventEnvelope { v, sequence, event })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for envelope in events {
            if envelope.v != VOICE_EVENT_VERSION {
                return Err(OfflineError::LedgerVersion {
                    expected: VOICE_EVENT_VERSION,
                    actual: envelope.v,
                });
            }
            output.push_str("\nvoice_event: ");
            output.push_str(&serde_json::to_string(&envelope)?);
        }
    }
    Ok(output)
}

fn run_jsonl_path(sqlite_path: &Path, run_id: &str) -> OfflineResult<PathBuf> {
    let run_id = platonic_core::RunId::new(run_id.to_owned()).map_err(OfflineError::Core)?;
    let ledger_directory = sqlite_path.parent().ok_or_else(|| {
        OfflineError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "workspace ledger has no directory: {}",
                sqlite_path.display()
            ),
        ))
    })?;
    Ok(ledger_directory
        .join("runs")
        .join(format!("{run_id}.jsonl")))
}

fn read_jsonl(path: &Path) -> OfflineResult<(Vec<RecordedEvent>, Vec<VoiceEventEnvelope>)> {
    let bytes = fs::read(path)?;
    let committed_len = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |index| index + 1);
    let lines = parse_jsonl_lines(&bytes[..committed_len])?;
    let mut records = Vec::new();
    let mut voice_events = Vec::new();
    let mut voice_batch_seen = false;
    for line in lines {
        match line {
            PersistedLedgerLine::Record(line) => records.push(line.record),
            PersistedLedgerLine::Voice(line) => {
                if voice_batch_seen {
                    return Err(OfflineError::VoiceEventContract(
                        "JSONL run contains multiple voice event batches".into(),
                    ));
                }
                voice_batch_seen = true;
                voice_events = line.voice_events;
            }
        }
    }
    for (index, envelope) in voice_events.iter().enumerate() {
        let expected = u64::try_from(index).unwrap_or(u64::MAX);
        if envelope.v != VOICE_EVENT_VERSION {
            return Err(OfflineError::LedgerVersion {
                expected: VOICE_EVENT_VERSION,
                actual: envelope.v,
            });
        }
        if envelope.sequence != expected {
            return Err(OfflineError::VoiceEventContract(format!(
                "voice event sequence was {}, expected {expected}",
                envelope.sequence
            )));
        }
        envelope
            .event
            .validate()
            .map_err(OfflineError::VoiceEventContract)?;
        if let Some(record) = records.first()
            && envelope.event.run_id() != record.event.run_id()
        {
            return Err(OfflineError::VoiceEventContract(format!(
                "voice event belongs to {}, expected {}",
                envelope.event.run_id(),
                record.event.run_id()
            )));
        }
    }
    validate_voice_event_keys(&records, &voice_events)?;
    Ok((records, voice_events))
}

fn validate_record_run_ids(records: &[RecordedEvent], selected_run_id: &str) -> OfflineResult<()> {
    for record in records {
        let actual = record.event.run_id().as_str();
        if actual != selected_run_id {
            return Err(OfflineError::Core(platonic_core::Error::RunIdMismatch {
                expected: selected_run_id.into(),
                actual: actual.into(),
            }));
        }
    }
    Ok(())
}

fn validate_voice_event_keys(
    records: &[RecordedEvent],
    envelopes: &[VoiceEventEnvelope],
) -> OfflineResult<()> {
    for envelope in envelopes {
        let turn_id = envelope.event.turn_id();
        if !records
            .iter()
            .any(|record| harness_event_turn_id(&record.event) == Some(turn_id))
        {
            return Err(OfflineError::VoiceEventContract(format!(
                "voice event turn {turn_id} is absent from core run {}",
                envelope.event.run_id()
            )));
        }
    }
    Ok(())
}

fn harness_event_turn_id(event: &HarnessEvent) -> Option<&platonic_core::TurnId> {
    match event {
        HarnessEvent::ContextBuilt { turn_id, .. }
        | HarnessEvent::ContextCompacted { turn_id, .. }
        | HarnessEvent::ModelRequested { turn_id, .. }
        | HarnessEvent::ModelFailed { turn_id, .. }
        | HarnessEvent::ModelResponded { turn_id, .. }
        | HarnessEvent::ToolProposalsRejected { turn_id, .. }
        | HarnessEvent::ToolCallProposed { turn_id, .. } => Some(turn_id),
        HarnessEvent::RunStarted(_)
        | HarnessEvent::PolicyEvaluated { .. }
        | HarnessEvent::ApprovalGranted { .. }
        | HarnessEvent::ApprovalDenied { .. }
        | HarnessEvent::ToolStarted { .. }
        | HarnessEvent::ToolFinished { .. }
        | HarnessEvent::ToolFailed { .. }
        | HarnessEvent::RunFinished { .. }
        | HarnessEvent::RunFailed { .. } => None,
    }
}

fn parse_jsonl_lines(bytes: &[u8]) -> OfflineResult<Vec<PersistedLedgerLine>> {
    let mut lines = Vec::new();
    for bytes in bytes.split(|byte| *byte == b'\n') {
        if bytes.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let line = serde_json::from_slice::<PersistedLedgerLine>(bytes)?;
        if let PersistedLedgerLine::Record(line) = &line
            && !matches!(line.v, 1 | LEDGER_VERSION)
        {
            return Err(OfflineError::LedgerVersion {
                expected: LEDGER_VERSION,
                actual: line.v,
            });
        }
        lines.push(line);
    }
    Ok(lines)
}

fn read_run(connection: &Connection, run_id: &str) -> OfflineResult<Vec<RecordedEvent>> {
    let mut statement = connection.prepare(
        "SELECT seq, occurred_at_ms, v, event_json FROM ledger_events WHERE run_id = ?1 ORDER BY seq ASC",
    )?;
    Ok(statement
        .query_map(params![run_id], |row| {
            let seq = u64::try_from(row.get::<_, i64>(0)?).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Integer,
                    Box::new(error),
                )
            })?;
            let occurred_at_ms = u64::try_from(row.get::<_, i64>(1)?).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    1,
                    rusqlite::types::Type::Integer,
                    Box::new(error),
                )
            })?;
            let version = row.get::<_, u32>(2)?;
            if !matches!(version, 1 | LEDGER_VERSION) {
                return Err(rusqlite::Error::InvalidQuery);
            }
            let event_json = row.get::<_, String>(3)?;
            let event = serde_json::from_str(&event_json).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    3,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?;
            Ok(RecordedEvent {
                seq,
                occurred_at_ms,
                event,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?)
}

fn format_records(records: &[RecordedEvent]) -> OfflineResult<String> {
    let readback = RunReadback::from_events(records)?;
    let mut lines = vec![
        format!("final_phase: {:?}", readback.final_phase),
        format!("next_seq: {}", readback.next_seq),
    ];
    for entry in &readback.entries {
        match entry {
            ReadbackEntry::ContextCompacted {
                turn_id,
                estimated_tokens_before,
                estimated_tokens_after,
                dropped_turn_start,
                dropped_turn_end_exclusive,
            } => lines.push(format!(
                "[{turn_id}] context_compacted estimated_tokens={estimated_tokens_before}->{estimated_tokens_after} dropped_turns={dropped_turn_start}..{dropped_turn_end_exclusive}"
            )),
            ReadbackEntry::ContextFragment { turn_id, fragment } => lines.push(format!(
                "[{turn_id}] context {:?} {}: {}",
                fragment.lane, fragment.source, fragment.content
            )),
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
                lines.push(format!("[{turn_id}] tool_call {} {}", call.tool, call.input));
            }
            ReadbackEntry::ToolResult { result } => {
                lines.push(format!("tool_result {}: {}", result.call_id, result.summary));
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
            } => lines.push(format!("approval_denied {call_id} by {actor_id}: {reason}")),
            ReadbackEntry::ToolFailed { call_id, reason } => {
                lines.push(format!("tool_failed {call_id}: {reason}"));
            }
            ReadbackEntry::ModelFailed { .. } | ReadbackEntry::ToolProposalsRejected { .. } => {}
        }
    }
    Ok(lines.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use platonic_core::{AgentId, ContextPack, RunId, TurnId};
    use serde_json::{Value, json};

    fn started_record(run_id: &str) -> RecordedEvent {
        RecordedEvent {
            seq: 0,
            occurred_at_ms: 0,
            event: HarnessEvent::RunStarted(platonic_core::RunStartedEvent {
                run_id: RunId::new(run_id).unwrap(),
                identity: platonic_core::RunIdentity::LegacyAgent {
                    agent_id: AgentId::new("plato").unwrap(),
                },
            }),
        }
    }

    fn context_record(run_id: &str, turn_id: &str) -> RecordedEvent {
        RecordedEvent {
            seq: 1,
            occurred_at_ms: 1,
            event: HarnessEvent::ContextBuilt {
                run_id: RunId::new(run_id).unwrap(),
                turn_id: TurnId::new(turn_id).unwrap(),
                context: ContextPack {
                    fragments: vec![],
                    token_budget: 4_000,
                },
            },
        }
    }

    fn ledger_line(record: &RecordedEvent) -> Value {
        json!({"v": LEDGER_VERSION, "record": record})
    }

    fn write_committed_lines(path: &Path, lines: &[Value]) {
        let mut bytes = Vec::new();
        for line in lines {
            serde_json::to_writer(&mut bytes, line).unwrap();
            bytes.push(b'\n');
        }
        fs::write(path, bytes).unwrap();
    }

    #[test]
    fn jsonl_reader_ignores_a_valid_unterminated_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let committed = started_record("run_tail");
        let uncommitted = RecordedEvent {
            seq: 1,
            occurred_at_ms: 1,
            event: HarnessEvent::RunFailed {
                run_id: RunId::new("run_tail").unwrap(),
                reason: "complete JSON without commit marker".into(),
            },
        };
        let mut bytes = serde_json::to_vec(&ledger_line(&committed)).unwrap();
        bytes.push(b'\n');
        bytes.extend(serde_json::to_vec(&ledger_line(&uncommitted)).unwrap());
        fs::write(&path, bytes).unwrap();

        let (records, voice_events) = read_jsonl(&path).unwrap();
        assert_eq!(records, vec![committed]);
        assert!(voice_events.is_empty());
    }

    #[test]
    fn newline_terminated_wrong_version_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        write_committed_lines(
            &path,
            &[json!({"v": 3, "record": started_record("run_version")})],
        );

        assert!(matches!(
            read_jsonl(&path),
            Err(OfflineError::LedgerVersion {
                expected: LEDGER_VERSION,
                actual: 3
            })
        ));
    }

    #[test]
    fn selected_run_jsonl_rejects_records_for_another_run() {
        let dir = tempfile::tempdir().unwrap();
        let sqlite_path = dir.path().join("ledger.db");
        drop(Connection::open(&sqlite_path).unwrap());
        let jsonl_path = run_jsonl_path(&sqlite_path, "run_selected").unwrap();
        fs::create_dir_all(jsonl_path.parent().unwrap()).unwrap();
        write_committed_lines(&jsonl_path, &[ledger_line(&started_record("run_other"))]);

        assert!(matches!(
            replay_sqlite(&sqlite_path, Some("run_selected")),
            Err(OfflineError::Core(platonic_core::Error::RunIdMismatch {
                expected,
                actual
            })) if expected == "run_selected" && actual == "run_other"
        ));
    }

    #[test]
    fn jsonl_voice_read_rejects_version_unknown_fields_and_missing_core_turn() {
        let run_id = RunId::new("run_voice_corrupt").unwrap();
        let event = VoiceEvent::VoiceSpoken {
            run_id: run_id.clone(),
            turn_id: TurnId::new("turn_missing").unwrap(),
            ttfa_ms: 1,
            sentence_count: 1,
            interrupted_at: None,
        };
        let envelope = VoiceEventEnvelope::revision_one(0, event);
        let valid_voice = json!({"voice_events": [envelope]});
        let mut future_version = valid_voice.clone();
        future_version["voice_events"][0]["v"] = json!(2);
        let mut unknown_field = valid_voice.clone();
        unknown_field["unexpected"] = json!(true);

        for corruption in [future_version, unknown_field] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("events.jsonl");
            write_committed_lines(
                &path,
                &[
                    ledger_line(&started_record(run_id.as_str())),
                    ledger_line(&context_record(run_id.as_str(), "turn_1")),
                    corruption,
                ],
            );
            assert!(matches!(read_jsonl(&path), Err(OfflineError::Json(_))));
        }

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        write_committed_lines(
            &path,
            &[
                ledger_line(&started_record(run_id.as_str())),
                ledger_line(&context_record(run_id.as_str(), "turn_1")),
                valid_voice,
            ],
        );
        assert!(matches!(
            read_jsonl(&path),
            Err(OfflineError::VoiceEventContract(message))
                if message.contains("turn_missing is absent from core run run_voice_corrupt")
        ));
    }
}
