use super::{
    jsonl::{read_records, read_voice_events_from_jsonl, run_jsonl_path},
    sqlite::{SqliteLedger, VOICE_EVENT_SQLITE_SCHEMA_VERSION, row_u64, session_exists_in},
    types::{
        LEDGER_VERSION, PersistedSessionStatus, PersistedSessionSummary, PersistedTokenUsage,
        SessionRecords, SessionRunRecords, SessionTurn, supported_ledger_version,
    },
};
use crate::{AppError, AppResult, paths::DefaultSqlitePath};
use platonic_core::{HarnessEvent, RecordedEvent, RunState};
use platonic_protocol::{RunStateName, VOICE_EVENT_VERSION, VoiceEvent, VoiceEventEnvelope};
use rusqlite::{Connection, OptionalExtension, params, types::Type};
use std::{fs, path::Path};

impl SqliteLedger {
    pub fn read_run(&self, run_id: &str) -> AppResult<Vec<RecordedEvent>> {
        read_run_from(&self.connection, &self.path, run_id)
    }

    /// Reads and validates one selected run's ordered voice companion stream.
    pub fn read_voice_events(&self, run_id: &str) -> AppResult<Vec<VoiceEventEnvelope>> {
        let jsonl_path = run_jsonl_path(&self.path, run_id)?;
        if jsonl_path.exists() {
            return read_voice_events_from_jsonl(&jsonl_path, run_id);
        }
        if self.schema_version < VOICE_EVENT_SQLITE_SCHEMA_VERSION {
            return Ok(Vec::new());
        }
        let envelopes = read_voice_events_from(&self.connection, run_id)?;
        let events = envelopes
            .iter()
            .map(|envelope| envelope.event.clone())
            .collect::<Vec<_>>();
        validate_voice_event_stream(&events)?;
        if !envelopes.is_empty() {
            let records = read_run_records_from(&self.connection, run_id)?;
            validate_voice_event_keys(&records, run_id, &envelopes)?;
        }
        Ok(envelopes)
    }

    pub fn read_latest_run(&self) -> AppResult<(String, Vec<RecordedEvent>)> {
        let run_id = self
            .connection
            .query_row(
                "SELECT run_id
                 FROM ledger_events
                 GROUP BY run_id
                 ORDER BY MAX(occurred_at_ms) DESC, run_id DESC
                 LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .ok_or(AppError::NoSqliteRuns)?;
        let records = self.read_run(&run_id)?;
        Ok((run_id, records))
    }

    pub fn latest_session_id(&self) -> AppResult<String> {
        self.connection
            .query_row(
                "SELECT session_id
                 FROM sessions
                 ORDER BY updated_at_ms DESC, session_id DESC
                 LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .ok_or(AppError::NoSqliteSessions)
    }

    pub fn session_turns(&self, session_id: &str) -> AppResult<Vec<SessionTurn>> {
        if !self.session_exists(session_id)? {
            return Err(AppError::SessionNotFound(session_id.into()));
        }
        let mut statement = self.connection.prepare(
            "SELECT question, final_answer
             FROM session_runs
             WHERE session_id = ?1 AND status = ?2 AND final_answer IS NOT NULL
             ORDER BY session_index ASC",
        )?;
        Ok(statement
            .query_map(
                params![session_id, RunStateName::Finished.as_str()],
                |row| {
                    Ok(SessionTurn {
                        question: row.get(0)?,
                        final_answer: row.get(1)?,
                    })
                },
            )?
            .collect::<Result<Vec<_>, _>>()?)
    }

    pub fn read_session(&self, session_id: &str) -> AppResult<SessionRecords> {
        let transaction = self.connection.unchecked_transaction()?;
        if !session_exists_in(&transaction, session_id)? {
            return Err(AppError::SessionNotFound(session_id.into()));
        }
        let runs = {
            let mut statement = transaction.prepare(
                "SELECT run_id, session_index, question, status, final_answer
                 FROM session_runs
                 WHERE session_id = ?1
                 ORDER BY session_index ASC",
            )?;
            statement
                .query_map(params![session_id], session_run_metadata_from_row)?
                .collect::<Result<Vec<_>, _>>()?
        };
        let runs = runs
            .into_iter()
            .map(|run| {
                Ok(SessionRunRecords {
                    records: read_run_from(&transaction, &self.path, &run.run_id)?,
                    run_id: run.run_id,
                    session_index: run.session_index,
                    question: run.question,
                    status: run.status,
                    final_answer: run.final_answer,
                })
            })
            .collect::<AppResult<Vec<_>>>()?;
        transaction.commit()?;
        Ok(SessionRecords {
            session_id: session_id.into(),
            runs,
        })
    }

    pub fn read_latest_session(&self) -> AppResult<SessionRecords> {
        let session_id = self.latest_session_id()?;
        self.read_session(&session_id)
    }

    pub(crate) fn read_session_run(&self, run_id: &str) -> AppResult<SessionRunRecords> {
        let transaction = self.connection.unchecked_transaction()?;
        let run = transaction
            .query_row(
                "SELECT run_id, session_index, question, status, final_answer
                 FROM session_runs
                 WHERE run_id = ?1",
                params![run_id],
                session_run_metadata_from_row,
            )
            .optional()?
            .ok_or_else(|| AppError::RunNotFound(run_id.into()))?;
        let records = read_run_from(&transaction, &self.path, &run.run_id)?;
        transaction.commit()?;
        Ok(SessionRunRecords {
            records,
            run_id: run.run_id,
            session_index: run.session_index,
            question: run.question,
            status: run.status,
            final_answer: run.final_answer,
        })
    }

    pub fn session_summaries(&self) -> AppResult<Vec<PersistedSessionSummary>> {
        let mut statement = self.connection.prepare(
            "SELECT s.session_id, sr.run_id, sr.status, sr.question,
                    (
                      SELECT first_run.question
                      FROM session_runs first_run
                      WHERE first_run.session_id = s.session_id
                      ORDER BY first_run.session_index ASC
                      LIMIT 1
                    ),
                    s.updated_at_ms
             FROM sessions s
             JOIN session_runs sr ON sr.session_id = s.session_id
             WHERE sr.session_index = (
               SELECT MAX(session_index)
               FROM session_runs
               WHERE session_id = s.session_id
             )
             ORDER BY s.updated_at_ms DESC, s.session_id DESC",
        )?;
        Ok(statement
            .query_map([], |row| {
                Ok(PersistedSessionSummary {
                    session_id: row.get(0)?,
                    run_id: row.get(1)?,
                    status: status_from_row(row, 2)?,
                    latest_question: row.get(3)?,
                    first_question: row.get(4)?,
                    updated_at_ms: row_u64(row, 5, "updated_at_ms")?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?)
    }

    pub(crate) fn session_status(
        &self,
        session_id: Option<&str>,
    ) -> AppResult<Option<PersistedSessionStatus>> {
        let session = match session_id {
            Some(session_id) => self.read_session(session_id)?,
            None => match self.read_latest_session() {
                Ok(session) => session,
                Err(AppError::NoSqliteSessions) => return Ok(None),
                Err(error) => return Err(error),
            },
        };
        Ok(Some(project_session_status(&session)?))
    }
}

fn project_session_status(session: &SessionRecords) -> AppResult<PersistedSessionStatus> {
    let latest_run = session
        .runs
        .last()
        .ok_or_else(|| AppError::SessionNotFound(session.session_id.clone()))?;
    let mut status = PersistedSessionStatus {
        session_id: session.session_id.clone(),
        latest_run_id: latest_run.run_id.clone(),
        human_turn_count: session.runs.len() as u64,
        core_event_count: 0,
        served_model: None,
        last_run_usage: PersistedTokenUsage::default(),
        session_usage: PersistedTokenUsage::default(),
        approval_granted_count: 0,
        approval_denied_count: 0,
    };

    for run in &session.runs {
        let is_latest_run = run.run_id == latest_run.run_id;
        status.core_event_count += run.records.len() as u64;
        for record in &run.records {
            match &record.event {
                HarnessEvent::ModelResponded {
                    served_model,
                    usage,
                    ..
                } => {
                    status.served_model = served_model.as_ref().map(ToString::to_string);
                    observe_usage(&mut status.session_usage, usage.as_ref());
                    if is_latest_run {
                        observe_usage(&mut status.last_run_usage, usage.as_ref());
                    }
                }
                HarnessEvent::ApprovalGranted { .. } => status.approval_granted_count += 1,
                HarnessEvent::ApprovalDenied { .. } => status.approval_denied_count += 1,
                _ => {}
            }
        }
    }
    Ok(status)
}

fn observe_usage(aggregate: &mut PersistedTokenUsage, usage: Option<&platonic_core::ModelUsage>) {
    match usage {
        Some(usage) => {
            aggregate.input_tokens += u64::from(usage.input_tokens);
            aggregate.output_tokens += u64::from(usage.output_tokens);
        }
        None => aggregate.unknown_response_count += 1,
    }
}

fn read_run_from(
    connection: &Connection,
    sqlite_path: &Path,
    run_id: &str,
) -> AppResult<Vec<RecordedEvent>> {
    let jsonl_path = run_jsonl_path(sqlite_path, run_id)?;
    let records = if jsonl_path.exists() {
        let records = read_records(&jsonl_path)?;
        validate_record_run_ids(&records, run_id)?;
        records
    } else {
        read_run_records_from(connection, run_id)?
    };
    if records.is_empty() {
        Err(AppError::RunNotFound(run_id.into()))
    } else {
        Ok(records)
    }
}

pub(super) fn validate_record_run_ids(
    records: &[RecordedEvent],
    selected_run_id: &str,
) -> AppResult<()> {
    for record in records {
        let actual = record.event.run_id().as_str();
        if actual != selected_run_id {
            return Err(AppError::Core(platonic_core::Error::RunIdMismatch {
                expected: selected_run_id.into(),
                actual: actual.into(),
            }));
        }
    }
    Ok(())
}

pub(super) fn read_run_records_from(
    connection: &Connection,
    run_id: &str,
) -> AppResult<Vec<RecordedEvent>> {
    let mut statement = connection.prepare(
        "SELECT seq, occurred_at_ms, v, event_json
             FROM ledger_events
             WHERE run_id = ?1
             ORDER BY seq ASC",
    )?;
    let mut rows = statement.query(params![run_id])?;
    let mut records = Vec::new();
    while let Some(row) = rows.next()? {
        records.push(sqlite_record_from_row(row)?);
    }
    Ok(records)
}

pub(super) fn read_voice_events_from(
    connection: &Connection,
    selected_run_id: &str,
) -> AppResult<Vec<VoiceEventEnvelope>> {
    let mut statement = connection.prepare(
        "SELECT run_id, turn_id, sequence, v, event_json
         FROM voice_events
         WHERE run_id = ?1
         ORDER BY sequence ASC",
    )?;
    let mut rows = statement.query(params![selected_run_id])?;
    let mut envelopes = Vec::new();
    while let Some(row) = rows.next()? {
        let run_id = row.get::<_, String>(0)?;
        let turn_id = row.get::<_, String>(1)?;
        let sequence = row_u64(row, 2, "voice sequence")?;
        let version = row.get::<_, u32>(3)?;
        if version != VOICE_EVENT_VERSION {
            return Err(AppError::VoiceEventVersion {
                expected: VOICE_EVENT_VERSION,
                actual: version,
            });
        }
        let event = serde_json::from_str::<VoiceEvent>(&row.get::<_, String>(4)?)?;
        event.validate().map_err(AppError::VoiceEventContract)?;
        if event.run_id().as_str() != run_id || event.turn_id().as_str() != turn_id {
            return Err(AppError::VoiceEventContract(format!(
                "voice event columns disagree with payload at run {selected_run_id} sequence {sequence}"
            )));
        }
        let expected_sequence = u64::try_from(envelopes.len()).map_err(|_| {
            AppError::VoiceEventContract("voice event sequence overflowed u64".into())
        })?;
        if sequence != expected_sequence {
            return Err(AppError::VoiceEventContract(format!(
                "voice event sequence was {sequence}, expected {expected_sequence} for run {selected_run_id}"
            )));
        }
        envelopes.push(VoiceEventEnvelope {
            v: version,
            sequence,
            event,
        });
    }
    Ok(envelopes)
}

pub(super) fn first_voice_difference(
    existing: &[VoiceEventEnvelope],
    proposed: &[VoiceEventEnvelope],
) -> u64 {
    existing
        .iter()
        .zip(proposed)
        .position(|(left, right)| left != right)
        .or_else(|| {
            (existing.len() != proposed.len()).then_some(existing.len().min(proposed.len()))
        })
        .and_then(|index| u64::try_from(index).ok())
        .unwrap_or(u64::MAX)
}

pub(super) fn validate_voice_event_stream(events: &[VoiceEvent]) -> AppResult<()> {
    let mut capture_seen = false;
    let mut interruption_seen = false;
    for (index, event) in events.iter().enumerate() {
        match event {
            VoiceEvent::VoiceCaptured { .. } => {
                if capture_seen || index != 0 {
                    return Err(AppError::VoiceEventContract(
                        "voice capture must appear at most once and first".into(),
                    ));
                }
                capture_seen = true;
            }
            VoiceEvent::VoiceSpoken {
                run_id,
                turn_id,
                interrupted_at,
                ..
            } => {
                if interruption_seen {
                    return Err(AppError::VoiceEventContract(
                        "voice interruption must terminate its companion stream".into(),
                    ));
                }
                if interrupted_at.is_some()
                    && !matches!(
                        events.get(index + 1),
                        Some(VoiceEvent::VoiceInterrupted {
                            run_id: interrupted_run,
                            turn_id: interrupted_turn,
                            ..
                        }) if interrupted_run == run_id && interrupted_turn == turn_id
                    )
                {
                    return Err(AppError::VoiceEventContract(
                        "interrupted voice speech must be followed by its exact interruption fact"
                            .into(),
                    ));
                }
            }
            VoiceEvent::VoiceInterrupted {
                run_id, turn_id, ..
            } => {
                let paired = matches!(
                    index.checked_sub(1).and_then(|previous| events.get(previous)),
                    Some(VoiceEvent::VoiceSpoken {
                        run_id: spoken_run,
                        turn_id: spoken_turn,
                        interrupted_at: Some(_),
                        ..
                    }) if spoken_run == run_id && spoken_turn == turn_id
                );
                if interruption_seen || !paired || index + 1 != events.len() {
                    return Err(AppError::VoiceEventContract(
                        "voice interruption must be paired with the preceding spoken fact and terminate the stream"
                            .into(),
                    ));
                }
                interruption_seen = true;
            }
        }
    }
    Ok(())
}

pub(super) fn validate_voice_event_keys(
    records: &[RecordedEvent],
    run_id: &str,
    envelopes: &[VoiceEventEnvelope],
) -> AppResult<()> {
    if records.is_empty() {
        return Err(AppError::RunNotFound(run_id.into()));
    }
    for envelope in envelopes {
        let turn_id = envelope.event.turn_id();
        if !records
            .iter()
            .any(|record| harness_event_turn_id(&record.event) == Some(turn_id))
        {
            return Err(AppError::VoiceEventContract(format!(
                "voice event turn {turn_id} is absent from core run {run_id}"
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
        HarnessEvent::RunStarted { .. }
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

pub(super) fn replay_records(records: &[RecordedEvent]) -> AppResult<RunState> {
    let mut state = RunState::new();
    for record in records {
        state.apply(record)?;
    }
    Ok(state)
}

pub(super) fn status_from_row(
    row: &rusqlite::Row<'_>,
    index: usize,
) -> rusqlite::Result<RunStateName> {
    let value: String = row.get(index)?;
    serde_json::from_value(serde_json::Value::String(value)).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(index, Type::Text, Box::new(error))
    })
}
struct SessionRunMetadata {
    run_id: String,
    session_index: u64,
    question: String,
    status: RunStateName,
    final_answer: Option<String>,
}

fn session_run_metadata_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionRunMetadata> {
    Ok(SessionRunMetadata {
        run_id: row.get(0)?,
        session_index: row_u64(row, 1, "session_index")?,
        question: row.get(2)?,
        status: status_from_row(row, 3)?,
        final_answer: row.get(4)?,
    })
}

fn sqlite_record_from_row(row: &rusqlite::Row<'_>) -> AppResult<RecordedEvent> {
    let version: u32 = row.get(2)?;
    if !supported_ledger_version(version) {
        return Err(AppError::LedgerVersion {
            expected: LEDGER_VERSION,
            actual: version,
        });
    }
    let event_json: String = row.get(3)?;
    let event = serde_json::from_str(&event_json).map_err(|error| {
        AppError::Sqlite(rusqlite::Error::FromSqlConversionFailure(
            3,
            Type::Text,
            Box::new(error),
        ))
    })?;
    Ok(RecordedEvent {
        seq: row_u64(row, 0, "seq")?,
        occurred_at_ms: row_u64(row, 1, "occurred_at_ms")?,
        event,
    })
}
pub fn read_sqlite_records(path: &Path, run_id: Option<&str>) -> AppResult<Vec<RecordedEvent>> {
    let ledger = SqliteLedger::open_readonly(path)?;
    match run_id {
        Some(run_id) => ledger.read_run(run_id),
        None => ledger.read_latest_run().map(|(_, records)| records),
    }
}

pub fn latest_sqlite_session_id(path: &Path) -> AppResult<String> {
    if !path.exists() {
        return Err(AppError::NoSqliteSessions);
    }
    SqliteLedger::open_readonly(path)?.latest_session_id()
}

pub fn latest_default_sqlite_session_id(path: &DefaultSqlitePath) -> AppResult<String> {
    if fs::symlink_metadata(path.as_path())
        .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
    {
        return Err(AppError::NoSqliteSessions);
    }
    SqliteLedger::open_default_readonly(path)?.latest_session_id()
}

pub fn read_latest_sqlite_session(path: &Path) -> AppResult<SessionRecords> {
    SqliteLedger::open_readonly(path)?.read_latest_session()
}

pub fn read_sqlite_session(path: &Path, session_id: &str) -> AppResult<SessionRecords> {
    SqliteLedger::open_readonly(path)?.read_session(session_id)
}

pub fn sqlite_session_summaries(path: &Path) -> AppResult<Vec<PersistedSessionSummary>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    SqliteLedger::open_readonly(path)?.session_summaries()
}

pub fn default_sqlite_session_summaries(
    path: &DefaultSqlitePath,
) -> AppResult<Vec<PersistedSessionSummary>> {
    if fs::symlink_metadata(path.as_path())
        .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
    {
        return Ok(Vec::new());
    }
    SqliteLedger::open_default_readonly(path)?.session_summaries()
}

pub(crate) fn default_sqlite_session_status(
    path: &DefaultSqlitePath,
    session_id: Option<&str>,
) -> AppResult<Option<PersistedSessionStatus>> {
    if fs::symlink_metadata(path.as_path())
        .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
    {
        return match session_id {
            Some(session_id) => Err(AppError::SessionNotFound(session_id.into())),
            None => Ok(None),
        };
    }
    SqliteLedger::open_default_readonly(path)?.session_status(session_id)
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ledger::sqlite::tests::{append_response_prefix, started_record};
    use platonic_core::{
        ActorId, AgentId, HarnessEvent, Message, MessageRole, ModelName, ModelUsage,
        PolicyDecision, RunId, ToolCallId, TurnId,
    };
    use std::fs;

    #[test]
    fn sqlite_reads_latest_run_when_run_is_omitted() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.db");
        let mut ledger = SqliteLedger::open_or_create(&path).unwrap();
        ledger
            .append("run_old", &started_record("run_old", 0, 10))
            .unwrap();
        ledger
            .append("run_new", &started_record("run_new", 0, 20))
            .unwrap();

        let (run_id, records) = ledger.read_latest_run().unwrap();

        assert_eq!(run_id, "run_new");
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn sqlite_sessions_track_finished_turns_and_latest_session() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.db");
        let mut ledger = SqliteLedger::open_or_create(&path).unwrap();
        let run_id = RunId::new("run_1").unwrap();

        let turns = ledger
            .begin_session_run("session_1", &run_id, "hello", true)
            .unwrap();
        assert!(turns.is_empty());
        append_response_prefix(&mut ledger, &run_id, "hi", 0);
        ledger.finish_session_run(&run_id, "hi").unwrap();

        assert_eq!(ledger.latest_session_id().unwrap(), "session_1");
        assert_eq!(
            ledger.session_turns("session_1").unwrap(),
            vec![SessionTurn {
                question: "hello".into(),
                final_answer: "hi".into(),
            }]
        );
    }

    #[test]
    fn sqlite_status_projects_exact_multi_run_usage_trust_and_session_facts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.db");
        let mut ledger = SqliteLedger::open_or_create(&path).unwrap();
        ledger
            .connection
            .execute_batch(
                r#"
                INSERT INTO sessions (session_id, created_at_ms, updated_at_ms)
                VALUES ('session_1', 10, 20);
                INSERT INTO session_runs
                  (session_id, run_id, session_index, question, final_answer, status, error, created_at_ms, updated_at_ms)
                VALUES
                  ('session_1', 'run_1', 0, 'first question', 'first answer', 'finished', NULL, 10, 10),
                  ('session_1', 'run_2', 1, 'second question', 'second answer', 'finished', NULL, 20, 20);
                "#,
            )
            .unwrap();

        append_status_records(
            &mut ledger,
            "run_1",
            vec![
                status_started_event("run_1"),
                status_response_event(
                    "run_1",
                    "turn_1",
                    0,
                    Some("served-old"),
                    Some(ModelUsage {
                        input_tokens: 10,
                        output_tokens: 5,
                    }),
                ),
                status_response_event("run_1", "turn_1", 1, Some("served-older"), None),
                HarnessEvent::ApprovalGranted {
                    run_id: RunId::new("run_1").unwrap(),
                    call_id: ToolCallId::new("call_granted_1").unwrap(),
                    actor_id: ActorId::new("human").unwrap(),
                },
                HarnessEvent::PolicyEvaluated {
                    run_id: RunId::new("run_1").unwrap(),
                    call_id: ToolCallId::new("call_pending_only").unwrap(),
                    decision: PolicyDecision::RequireApproval {
                        reason: "approval required".into(),
                    },
                },
            ],
        );
        append_status_records(
            &mut ledger,
            "run_2",
            vec![
                status_started_event("run_2"),
                status_response_event(
                    "run_2",
                    "turn_2",
                    0,
                    None,
                    Some(ModelUsage {
                        input_tokens: 0,
                        output_tokens: 0,
                    }),
                ),
                status_response_event("run_2", "turn_2", 1, Some("served-candidate"), None),
                status_response_event(
                    "run_2",
                    "turn_2",
                    2,
                    Some("served-latest"),
                    Some(ModelUsage {
                        input_tokens: 7,
                        output_tokens: 3,
                    }),
                ),
                HarnessEvent::ApprovalGranted {
                    run_id: RunId::new("run_2").unwrap(),
                    call_id: ToolCallId::new("call_granted_2").unwrap(),
                    actor_id: ActorId::new("human").unwrap(),
                },
                HarnessEvent::ApprovalDenied {
                    run_id: RunId::new("run_2").unwrap(),
                    call_id: ToolCallId::new("call_denied_1").unwrap(),
                    actor_id: ActorId::new("human").unwrap(),
                    reason: "not now".into(),
                },
                HarnessEvent::ApprovalDenied {
                    run_id: RunId::new("run_2").unwrap(),
                    call_id: ToolCallId::new("call_denied_2").unwrap(),
                    actor_id: ActorId::new("human").unwrap(),
                    reason: "still no".into(),
                },
            ],
        );

        assert_eq!(
            ledger.session_status(None).unwrap(),
            Some(PersistedSessionStatus {
                session_id: "session_1".into(),
                latest_run_id: "run_2".into(),
                human_turn_count: 2,
                core_event_count: 12,
                served_model: Some("served-latest".into()),
                last_run_usage: PersistedTokenUsage {
                    input_tokens: 7,
                    output_tokens: 3,
                    unknown_response_count: 1,
                },
                session_usage: PersistedTokenUsage {
                    input_tokens: 17,
                    output_tokens: 8,
                    unknown_response_count: 2,
                },
                approval_granted_count: 2,
                approval_denied_count: 2,
            })
        );
        assert!(matches!(
            ledger.session_status(Some("missing-session")),
            Err(AppError::SessionNotFound(session_id)) if session_id == "missing-session"
        ));
    }

    #[test]
    fn sqlite_session_records_keep_run_metadata_in_session_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.db");
        let mut ledger = SqliteLedger::open_or_create(&path).unwrap();
        let first = RunId::new("run_1").unwrap();
        let second = RunId::new("run_2").unwrap();

        ledger
            .begin_session_run("session_1", &first, "first question", true)
            .unwrap();
        append_response_prefix(&mut ledger, &first, "first answer", 10);
        ledger.finish_session_run(&first, "first answer").unwrap();
        ledger
            .begin_session_run("session_1", &second, "second question", false)
            .unwrap();
        ledger
            .append("run_2", &started_record("run_2", 0, 20))
            .unwrap();
        ledger
            .fail_session_run(&second, "synthetic failure", false)
            .unwrap();

        let session = ledger.read_session("session_1").unwrap();
        assert_eq!(session.runs.len(), 2);
        assert_eq!(session.runs[0].run_id, "run_1");
        assert_eq!(session.runs[0].session_index, 0);
        assert_eq!(session.runs[0].question, "first question");
        assert_eq!(session.runs[0].status, RunStateName::Finished);
        assert_eq!(
            session.runs[0].final_answer.as_deref(),
            Some("first answer")
        );
        assert_eq!(session.runs[0].records[0].event.run_id().as_str(), "run_1");
        assert_eq!(session.runs[1].run_id, "run_2");
        assert_eq!(session.runs[1].session_index, 1);
        assert_eq!(session.runs[1].question, "second question");
        assert_eq!(session.runs[1].status, RunStateName::Failed);
        assert_eq!(session.runs[1].final_answer, None);
        assert_eq!(session.runs[1].records[0].event.run_id().as_str(), "run_2");
    }

    #[test]
    fn sqlite_session_summaries_report_first_and_latest_session_questions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.db");
        let mut ledger = SqliteLedger::open_or_create(&path).unwrap();
        let first_run = RunId::new("run_1").unwrap();
        let second_run = RunId::new("run_2").unwrap();

        ledger
            .begin_session_run("session_1", &first_run, "first question", true)
            .unwrap();
        append_response_prefix(&mut ledger, &first_run, "first answer", 0);
        ledger
            .finish_session_run(&first_run, "first answer")
            .unwrap();
        ledger
            .begin_session_run("session_1", &second_run, "second question", false)
            .unwrap();
        let updated_at_ms = ledger
            .connection
            .query_row(
                "SELECT updated_at_ms FROM sessions WHERE session_id = 'session_1'",
                [],
                |row| row_u64(row, 0, "updated_at_ms"),
            )
            .unwrap();

        assert_eq!(
            ledger.session_summaries().unwrap(),
            vec![PersistedSessionSummary {
                session_id: "session_1".into(),
                run_id: "run_2".into(),
                status: RunStateName::Running,
                latest_question: "second question".into(),
                first_question: "first question".into(),
                updated_at_ms,
            }]
        );
    }

    #[test]
    fn sqlite_session_summaries_preserve_newest_first_lifecycle_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.db");
        let ledger = SqliteLedger::open_or_create(&path).unwrap();
        ledger
            .connection
            .execute_batch(
                r#"
                INSERT INTO sessions (session_id, created_at_ms, updated_at_ms) VALUES
                  ('session_new', 40, 400),
                  ('session_continued', 30, 300),
                  ('session_interrupted', 20, 200),
                  ('session_failed', 10, 100);
                INSERT INTO session_runs
                  (session_id, run_id, session_index, question, final_answer, status, error, created_at_ms, updated_at_ms)
                VALUES
                  ('session_new', 'run_new', 0, 'new question', NULL, 'running', NULL, 40, 400),
                  ('session_continued', 'run_continued_1', 0, 'continued first question', 'first answer', 'finished', NULL, 30, 30),
                  ('session_continued', 'run_continued_2', 1, 'approved, go ahead', 'second answer', 'finished', NULL, 300, 300),
                  ('session_interrupted', 'run_interrupted', 0, 'interrupted question', NULL, 'interrupted', 'daemon restarted', 20, 200),
                  ('session_failed', 'run_failed', 0, 'failed question', NULL, 'failed', 'provider failed', 10, 100);
                "#,
            )
            .unwrap();

        let summaries = ledger.session_summaries().unwrap();

        assert_eq!(
            summaries
                .iter()
                .map(|summary| (summary.session_id.as_str(), summary.status))
                .collect::<Vec<_>>(),
            vec![
                ("session_new", RunStateName::Running),
                ("session_continued", RunStateName::Finished),
                ("session_interrupted", RunStateName::Interrupted),
                ("session_failed", RunStateName::Failed),
            ]
        );
        assert_eq!(summaries[1].first_question, "continued first question");
        assert_eq!(summaries[1].latest_question, "approved, go ahead");
        assert_eq!(
            summaries
                .iter()
                .map(|summary| summary.updated_at_ms)
                .collect::<Vec<_>>(),
            vec![400, 300, 200, 100]
        );
    }

    #[test]
    fn sqlite_session_summary_read_is_readonly_and_never_creates_a_missing_database() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing.db");
        assert!(sqlite_session_summaries(&missing).unwrap().is_empty());
        assert!(!missing.exists());

        let path = dir.path().join("events.db");
        let mut ledger = SqliteLedger::open_or_create(&path).unwrap();
        ledger
            .begin_session_run(
                "session_1",
                &RunId::new("run_1").unwrap(),
                "first question",
                true,
            )
            .unwrap();
        let schema_version = ledger.user_version().unwrap();
        drop(ledger);
        let bytes_before = fs::read(&path).unwrap();

        let summaries = sqlite_session_summaries(&path).unwrap();

        assert_eq!(summaries[0].first_question, "first question");
        assert_eq!(fs::read(&path).unwrap(), bytes_before);
        let connection = Connection::open(&path).unwrap();
        assert_eq!(
            connection
                .pragma_query_value::<u32, _>(None, "user_version", |row| row.get(0))
                .unwrap(),
            schema_version
        );
    }

    #[test]
    fn latest_session_id_read_does_not_create_database() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing.db");

        let error = latest_sqlite_session_id(&path).unwrap_err();

        assert!(matches!(error, AppError::NoSqliteSessions));
        assert!(!path.exists());
    }
    fn append_status_records(ledger: &mut SqliteLedger, run_id: &str, events: Vec<HarnessEvent>) {
        for (seq, event) in events.into_iter().enumerate() {
            ledger
                .append(
                    run_id,
                    &RecordedEvent {
                        seq: seq as u64,
                        occurred_at_ms: 100 + seq as u64,
                        event,
                    },
                )
                .unwrap();
        }
    }

    fn status_started_event(run_id: &str) -> HarnessEvent {
        HarnessEvent::RunStarted {
            run_id: RunId::new(run_id).unwrap(),
            agent_id: AgentId::new("plato").unwrap(),
        }
    }

    fn status_response_event(
        run_id: &str,
        turn_id: &str,
        step: u32,
        served_model: Option<&str>,
        usage: Option<ModelUsage>,
    ) -> HarnessEvent {
        HarnessEvent::ModelResponded {
            run_id: RunId::new(run_id).unwrap(),
            turn_id: TurnId::new(turn_id).unwrap(),
            step,
            output: Message {
                role: MessageRole::Assistant,
                content: format!("response {step}"),
            },
            proposed_calls: vec![],
            served_model: served_model.map(|model| ModelName::new(model).unwrap()),
            usage,
        }
    }
}
