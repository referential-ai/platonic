use crate::{AppError, AppResult, paths::DefaultSqlitePath};
use platonic_core::{HarnessEvent, MessageRole, RecordedEvent, RunId, RunPhase, RunState};
use platonic_protocol::RunStateName;
use platonic_protocol::{VOICE_EVENT_VERSION, VoiceEvent, VoiceEventEnvelope};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params, types::Type};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::{
    io::{Error, ErrorKind},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::PathBuf,
};

const LEGACY_LEDGER_VERSION: u32 = 1;
pub const LEDGER_VERSION: u32 = 2;
const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const LEGACY_SQLITE_SCHEMA_VERSION: u32 = 1;
const SESSION_SQLITE_SCHEMA_VERSION: u32 = 2;
const VOICE_EVENT_SQLITE_SCHEMA_VERSION: u32 = 3;
// Versions 4 and 5 added thread authority and thread stop tables here. Those
// tables now live in the server-wide store, because a thread must be
// enumerable from outside the workspace it runs in (D005). The numbers stay
// spent so an existing workspace ledger still opens at its recorded version.
const THREAD_STOP_SQLITE_SCHEMA_VERSION: u32 = 5;
const SQLITE_SCHEMA_VERSION: u32 = THREAD_STOP_SQLITE_SCHEMA_VERSION;
pub(crate) const RUN_CANCELED_REASON: &str = "run canceled";
const ORPHANED_RUN_ERROR: &str = "daemon restarted before run completed";
#[cfg(unix)]
const PRIVATE_DIRECTORY_MODE: u32 = 0o700;
#[cfg(unix)]
const PRIVATE_FILE_MODE: u32 = 0o600;
#[cfg(unix)]
const SIDECAR_HARDEN_ATTEMPTS: usize = 8;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LedgerLine {
    pub v: u32,
    pub record: RecordedEvent,
}

pub enum EventRecorder {
    Jsonl(JsonlEventRecorder),
    Sqlite(SqliteEventRecorder),
}

pub(crate) trait RunEventRecorder {
    fn record(&mut self, event: HarnessEvent) -> AppResult<RecordedEvent>;
    fn finish_run(&mut self, run_id: &RunId, final_answer: &str) -> AppResult<RecordedEvent>;
    fn fail_run(&mut self, run_id: &RunId, error: &str, canceled: bool)
    -> AppResult<RecordedEvent>;
}

impl EventRecorder {
    pub fn create_jsonl(path: &Path) -> AppResult<Self> {
        Ok(Self::Jsonl(JsonlEventRecorder::create(path)?))
    }

    pub fn create_sqlite(path: &Path, run_id: &RunId) -> AppResult<Self> {
        Ok(Self::Sqlite(SqliteEventRecorder::create(path, run_id)?))
    }

    pub fn create_default_sqlite(path: &DefaultSqlitePath, run_id: &RunId) -> AppResult<Self> {
        Ok(Self::Sqlite(SqliteEventRecorder::create_default(
            path, run_id,
        )?))
    }

    pub(crate) fn from_session_sqlite(ledger: SqliteLedger, run_id: &RunId) -> Self {
        Self::Sqlite(SqliteEventRecorder::from_session(ledger, run_id))
    }

    pub fn record(&mut self, event: HarnessEvent) -> AppResult<RecordedEvent> {
        match self {
            Self::Jsonl(recorder) => recorder.record(event),
            Self::Sqlite(recorder) => recorder.record(event),
        }
    }

    pub(crate) fn finish_run(
        &mut self,
        run_id: &RunId,
        final_answer: &str,
    ) -> AppResult<RecordedEvent> {
        match self {
            Self::Jsonl(recorder) => recorder.record(HarnessEvent::RunFinished {
                run_id: run_id.clone(),
            }),
            Self::Sqlite(recorder) => recorder.finish_run(final_answer),
        }
    }

    pub(crate) fn fail_run(
        &mut self,
        run_id: &RunId,
        error: &str,
        canceled: bool,
    ) -> AppResult<RecordedEvent> {
        match self {
            Self::Jsonl(recorder) => recorder.record(HarnessEvent::RunFailed {
                run_id: run_id.clone(),
                reason: error.into(),
            }),
            Self::Sqlite(recorder) => recorder.fail_run(error, canceled),
        }
    }
}

impl RunEventRecorder for EventRecorder {
    fn record(&mut self, event: HarnessEvent) -> AppResult<RecordedEvent> {
        EventRecorder::record(self, event)
    }

    fn finish_run(&mut self, run_id: &RunId, final_answer: &str) -> AppResult<RecordedEvent> {
        EventRecorder::finish_run(self, run_id, final_answer)
    }

    fn fail_run(
        &mut self,
        run_id: &RunId,
        error: &str,
        canceled: bool,
    ) -> AppResult<RecordedEvent> {
        EventRecorder::fail_run(self, run_id, error, canceled)
    }
}

pub struct JsonlEventRecorder {
    writer: BufWriter<File>,
    state: RunState,
}

impl JsonlEventRecorder {
    pub fn create(path: &Path) -> AppResult<Self> {
        if path.as_os_str().is_empty() {
            return Err(AppError::EmptyLedger);
        }

        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::AlreadyExists {
                    AppError::LedgerExists(path.into())
                } else {
                    AppError::Io(error)
                }
            })?;
        Ok(Self {
            writer: BufWriter::new(file),
            state: RunState::new(),
        })
    }

    pub fn record(&mut self, event: HarnessEvent) -> AppResult<RecordedEvent> {
        let record = next_record(&mut self.state, event)?;
        let line = LedgerLine {
            v: LEDGER_VERSION,
            record: record.clone(),
        };
        serde_json::to_writer(&mut self.writer, &line)?;
        self.writer.write_all(b"\n")?;
        self.writer.flush()?;
        Ok(record)
    }
}

pub struct SqliteEventRecorder {
    ledger: SqliteLedger,
    run_id: RunId,
    state: RunState,
    session_run_open: bool,
    terminal_attempted: bool,
}

impl SqliteEventRecorder {
    pub fn create(path: &Path, run_id: &RunId) -> AppResult<Self> {
        Ok(Self {
            ledger: SqliteLedger::open_or_create(path)?,
            run_id: run_id.clone(),
            state: RunState::new(),
            session_run_open: false,
            terminal_attempted: false,
        })
    }

    pub fn create_default(path: &DefaultSqlitePath, run_id: &RunId) -> AppResult<Self> {
        Ok(Self {
            ledger: SqliteLedger::open_or_create_default(path)?,
            run_id: run_id.clone(),
            state: RunState::new(),
            session_run_open: false,
            terminal_attempted: false,
        })
    }

    fn from_session(ledger: SqliteLedger, run_id: &RunId) -> Self {
        Self {
            ledger,
            run_id: run_id.clone(),
            state: RunState::new(),
            session_run_open: true,
            terminal_attempted: false,
        }
    }

    pub fn record(&mut self, event: HarnessEvent) -> AppResult<RecordedEvent> {
        if self.session_run_open
            && matches!(
                event,
                HarnessEvent::RunFinished { .. } | HarnessEvent::RunFailed { .. }
            )
        {
            return Err(AppError::Config(
                "SQLite session terminal events require an atomic session outcome".into(),
            ));
        }
        let mut next_state = self.state.clone();
        let record = next_record(&mut next_state, event)?;
        self.ledger.append(self.run_id.as_str(), &record)?;
        self.state = next_state;
        Ok(record)
    }

    fn finish_run(&mut self, final_answer: &str) -> AppResult<RecordedEvent> {
        self.record_terminal(
            HarnessEvent::RunFinished {
                run_id: self.run_id.clone(),
            },
            RunStateName::Finished,
            Some(final_answer),
            None,
        )
    }

    fn fail_run(&mut self, error: &str, canceled: bool) -> AppResult<RecordedEvent> {
        let status = if canceled {
            RunStateName::Canceled
        } else {
            RunStateName::Failed
        };
        self.record_terminal(
            HarnessEvent::RunFailed {
                run_id: self.run_id.clone(),
                reason: error.into(),
            },
            status,
            None,
            Some(error),
        )
    }

    fn record_terminal(
        &mut self,
        event: HarnessEvent,
        status: RunStateName,
        final_answer: Option<&str>,
        error: Option<&str>,
    ) -> AppResult<RecordedEvent> {
        if !self.session_run_open {
            return self.record(event);
        }

        self.terminal_attempted = true;
        let mut next_state = self.state.clone();
        let record = next_record(&mut next_state, event)?;
        self.ledger
            .commit_session_terminal(&self.run_id, &record, status, final_answer, error)?;
        self.state = next_state;
        self.session_run_open = false;
        Ok(record)
    }
}

impl Drop for SqliteEventRecorder {
    fn drop(&mut self) {
        if self.session_run_open && !self.terminal_attempted {
            let _ = self.fail_run("run ended before session status was closed", false);
        }
    }
}

pub struct SqliteLedger {
    connection: Connection,
    schema_version: u32,
    #[cfg(test)]
    terminal_fault: Option<TerminalFaultBoundary>,
    #[cfg(test)]
    voice_fault: Option<VoiceFaultBoundary>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionTurn {
    pub question: String,
    pub final_answer: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SessionRunRecords {
    pub run_id: String,
    pub session_index: u64,
    pub question: String,
    pub status: RunStateName,
    pub final_answer: Option<String>,
    pub records: Vec<RecordedEvent>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SessionRecords {
    pub session_id: String,
    pub runs: Vec<SessionRunRecords>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedSessionSummary {
    pub session_id: String,
    pub run_id: String,
    pub status: RunStateName,
    pub latest_question: String,
    pub first_question: String,
    pub updated_at_ms: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct PersistedTokenUsage {
    pub(crate) input_tokens: u64,
    pub(crate) output_tokens: u64,
    pub(crate) unknown_response_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PersistedSessionStatus {
    pub(crate) session_id: String,
    pub(crate) latest_run_id: String,
    pub(crate) human_turn_count: u64,
    pub(crate) core_event_count: u64,
    pub(crate) served_model: Option<String>,
    pub(crate) last_run_usage: PersistedTokenUsage,
    pub(crate) session_usage: PersistedTokenUsage,
    pub(crate) approval_granted_count: u64,
    pub(crate) approval_denied_count: u64,
}

impl SqliteLedger {
    pub fn open_or_create(path: &Path) -> AppResult<Self> {
        if path.as_os_str().is_empty() {
            return Err(AppError::EmptyLedger);
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut connection = Connection::open(path)?;
        configure_sqlite_connection(&connection)?;
        migrate_sqlite(&mut connection)?;
        configure_sqlite_journal_mode(&connection)?;
        Ok(Self {
            connection,
            schema_version: SQLITE_SCHEMA_VERSION,
            #[cfg(test)]
            terminal_fault: None,
            #[cfg(test)]
            voice_fault: None,
        })
    }

    pub fn open_or_create_default(path: &DefaultSqlitePath) -> AppResult<Self> {
        #[cfg(unix)]
        {
            open_private_default_sqlite(path, true)
        }
        #[cfg(not(unix))]
        {
            Self::open_or_create(path.as_path())
        }
    }

    pub fn open_readonly(path: &Path) -> AppResult<Self> {
        let connection =
            Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        configure_sqlite_connection(&connection)?;
        let schema_version = read_sqlite_schema_version(&connection)?;
        Ok(Self {
            connection,
            schema_version,
            #[cfg(test)]
            terminal_fault: None,
            #[cfg(test)]
            voice_fault: None,
        })
    }

    pub fn open_default_readonly(path: &DefaultSqlitePath) -> AppResult<Self> {
        #[cfg(unix)]
        {
            open_private_default_sqlite(path, false)
        }
        #[cfg(not(unix))]
        {
            Self::open_readonly(path.as_path())
        }
    }

    pub fn append(&mut self, run_id: &str, record: &RecordedEvent) -> AppResult<()> {
        append_record_in(&self.connection, run_id, record)
    }

    pub fn read_run(&self, run_id: &str) -> AppResult<Vec<RecordedEvent>> {
        read_run_from(&self.connection, run_id)
    }

    /// Atomically persists one complete, immutable per-run voice companion stream.
    pub fn append_voice_events(
        &mut self,
        events: &[VoiceEvent],
    ) -> AppResult<Vec<VoiceEventEnvelope>> {
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let run_id = events[0].run_id().as_str().to_owned();
        for event in events {
            event.validate().map_err(AppError::VoiceEventContract)?;
            if event.run_id().as_str() != run_id {
                return Err(AppError::VoiceEventContract(
                    "one companion commit contained multiple run IDs".into(),
                ));
            }
        }
        validate_voice_event_stream(events)?;
        let envelopes = events
            .iter()
            .cloned()
            .enumerate()
            .map(|(sequence, event)| {
                let sequence = u64::try_from(sequence).map_err(|_| {
                    AppError::VoiceEventContract("voice event sequence overflowed u64".into())
                })?;
                Ok(VoiceEventEnvelope::revision_one(sequence, event))
            })
            .collect::<AppResult<Vec<_>>>()?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        validate_voice_event_keys(&transaction, &run_id, &envelopes)?;
        let existing = read_voice_events_from(&transaction, &run_id)?;
        if !existing.is_empty() {
            if existing == envelopes {
                transaction.commit()?;
                return Ok(envelopes);
            }
            let sequence = first_voice_difference(&existing, &envelopes);
            return Err(AppError::VoiceLedgerConflict { run_id, sequence });
        }

        for envelope in &envelopes {
            let event_json = serde_json::to_string(&envelope.event)?;
            transaction.execute(
                "INSERT INTO voice_events (run_id, turn_id, sequence, v, event_json)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    envelope.event.run_id().as_str(),
                    envelope.event.turn_id().as_str(),
                    sqlite_i64(envelope.sequence, "voice sequence")?,
                    envelope.v,
                    event_json
                ],
            )?;
            #[cfg(test)]
            inject_voice_fault(
                &mut self.voice_fault,
                VoiceFaultBoundary::AfterFirstInsert,
                envelope.sequence == 0,
            )?;
        }
        #[cfg(test)]
        inject_voice_fault(
            &mut self.voice_fault,
            VoiceFaultBoundary::BeforeCommit,
            true,
        )?;
        transaction.commit()?;
        Ok(envelopes)
    }

    /// Reads and validates one selected run's ordered voice companion stream.
    pub fn read_voice_events(&self, run_id: &str) -> AppResult<Vec<VoiceEventEnvelope>> {
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
            validate_voice_event_keys(&self.connection, run_id, &envelopes)?;
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

    pub(crate) fn is_legacy_schema(&self) -> bool {
        self.schema_version == LEGACY_SQLITE_SCHEMA_VERSION
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

    pub fn begin_session_run(
        &mut self,
        session_id: &str,
        run_id: &RunId,
        question: &str,
        create_session: bool,
    ) -> AppResult<Vec<SessionTurn>> {
        let now = sqlite_i64(now_ms(), "occurred_at_ms")?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let exists = session_exists_in(&transaction, session_id)?;
        if !exists && !create_session {
            return Err(AppError::SessionNotFound(session_id.into()));
        }
        if let Some(active_run_id) = active_run_in(&transaction, session_id)? {
            return Err(AppError::SessionActive {
                session_id: session_id.into(),
                run_id: active_run_id,
            });
        }
        if !exists {
            transaction.execute(
                "INSERT INTO sessions (session_id, created_at_ms, updated_at_ms)
                 VALUES (?1, ?2, ?3)",
                params![session_id, now, now],
            )?;
        }
        let session_index: i64 = transaction.query_row(
            "SELECT COALESCE(MAX(session_index) + 1, 0)
             FROM session_runs
             WHERE session_id = ?1",
            params![session_id],
            |row| row.get(0),
        )?;
        transaction.execute(
            "INSERT INTO session_runs
               (session_id, run_id, session_index, question, final_answer, status, error, created_at_ms, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4, NULL, ?5, NULL, ?6, ?7)",
            params![
                session_id,
                run_id.to_string(),
                session_index,
                question,
                RunStateName::Running.as_str(),
                now,
                now
            ],
        )?;
        touch_session(&transaction, session_id, now)?;
        transaction.commit()?;
        self.session_turns(session_id)
    }

    pub fn finish_session_run(&mut self, run_id: &RunId, final_answer: &str) -> AppResult<()> {
        let (records, mut state) = self.replay_run_state(run_id)?;
        let record = match state.phase() {
            RunPhase::Finished => {
                let durable_answer = final_answer_from_records(run_id, &records)?;
                if durable_answer != final_answer {
                    return Err(AppError::Config(format!(
                        "finished session run {run_id} answer does not match its ledger"
                    )));
                }
                records
                    .last()
                    .expect("replayed run contains a terminal event")
                    .clone()
            }
            _ => next_record(
                &mut state,
                HarnessEvent::RunFinished {
                    run_id: run_id.clone(),
                },
            )?,
        };
        self.commit_session_terminal(
            run_id,
            &record,
            RunStateName::Finished,
            Some(final_answer),
            None,
        )
    }

    pub fn fail_session_run(
        &mut self,
        run_id: &RunId,
        error: &str,
        canceled: bool,
    ) -> AppResult<()> {
        let status = if canceled {
            RunStateName::Canceled
        } else {
            RunStateName::Failed
        };
        let (records, mut state) = self.replay_run_state(run_id)?;
        let record = match state.phase() {
            RunPhase::Failed { reason } => {
                if reason != error {
                    return Err(AppError::Config(format!(
                        "failed session run {run_id} error does not match its ledger"
                    )));
                }
                records
                    .last()
                    .expect("replayed run contains a terminal event")
                    .clone()
            }
            _ => next_record(
                &mut state,
                HarnessEvent::RunFailed {
                    run_id: run_id.clone(),
                    reason: error.into(),
                },
            )?,
        };
        self.commit_session_terminal(run_id, &record, status, None, Some(error))
    }

    pub fn interrupt_running_session_runs(&mut self, error: &str) -> AppResult<usize> {
        self.recover_running_session_runs(error)
    }

    fn recover_running_session_runs(&mut self, error: &str) -> AppResult<usize> {
        let now = sqlite_i64(now_ms(), "occurred_at_ms")?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let running_runs = {
            let mut statement = transaction.prepare(
                "SELECT session_id, run_id
                 FROM session_runs
                 WHERE status = ?1
                 ORDER BY session_id, session_index",
            )?;
            statement
                .query_map(params![RunStateName::Running.as_str()], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        for (session_id, run_id) in &running_runs {
            let records = read_run_records_from(&transaction, run_id)?;
            if records.is_empty() {
                return Err(AppError::Config(format!(
                    "running session run {run_id} has no ledger events"
                )));
            }
            let mut state = replay_records(&records)?;
            let (status, final_answer, stored_error) = match state.phase() {
                RunPhase::Finished => (
                    RunStateName::Finished,
                    Some(final_answer_from_records_str(run_id, &records)?),
                    None,
                ),
                RunPhase::Failed { reason } => {
                    (failure_status(reason), None, Some(reason.as_str()))
                }
                _ => {
                    let record = next_record(
                        &mut state,
                        HarnessEvent::RunFailed {
                            run_id: RunId::new(run_id.clone())?,
                            reason: error.into(),
                        },
                    )?;
                    append_record_in(&transaction, run_id, &record)?;
                    (RunStateName::Interrupted, None, Some(error))
                }
            };
            update_running_session_outcome(
                &transaction,
                run_id,
                status,
                final_answer,
                stored_error,
                now,
            )?;
            touch_session(&transaction, session_id, now)?;
        }
        transaction.commit()?;
        Ok(running_runs.len())
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
                    records: read_run_from(&transaction, &run.run_id)?,
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
        let records = read_run_from(&transaction, &run.run_id)?;
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

    fn session_exists(&self, session_id: &str) -> AppResult<bool> {
        session_exists_in(&self.connection, session_id)
    }

    fn replay_run_state(&self, run_id: &RunId) -> AppResult<(Vec<RecordedEvent>, RunState)> {
        let records = self.read_run(run_id.as_str())?;
        let state = replay_records(&records)?;
        Ok((records, state))
    }

    fn commit_session_terminal(
        &mut self,
        run_id: &RunId,
        record: &RecordedEvent,
        status: RunStateName,
        final_answer: Option<&str>,
        error: Option<&str>,
    ) -> AppResult<()> {
        let now = sqlite_i64(now_ms(), "occurred_at_ms")?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let session_id: String = transaction
            .query_row(
                "SELECT session_id
             FROM session_runs
             WHERE run_id = ?1 AND status = ?2",
                params![run_id.as_str(), RunStateName::Running.as_str()],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| AppError::RunNotFound(run_id.to_string()))?;
        #[cfg(test)]
        inject_terminal_fault(
            &mut self.terminal_fault,
            TerminalFaultBoundary::BeforeEventInsert,
        )?;
        append_record_in(&transaction, run_id.as_str(), record)?;
        #[cfg(test)]
        inject_terminal_fault(
            &mut self.terminal_fault,
            TerminalFaultBoundary::AfterEventInsert,
        )?;
        #[cfg(test)]
        inject_terminal_fault(
            &mut self.terminal_fault,
            TerminalFaultBoundary::BeforeOutcomeUpdate,
        )?;
        update_running_session_outcome(
            &transaction,
            run_id.as_str(),
            status,
            final_answer,
            error,
            now,
        )?;
        #[cfg(test)]
        inject_terminal_fault(
            &mut self.terminal_fault,
            TerminalFaultBoundary::AfterOutcomeUpdate,
        )?;
        #[cfg(test)]
        inject_terminal_fault(
            &mut self.terminal_fault,
            TerminalFaultBoundary::BeforeSessionTouch,
        )?;
        touch_session(&transaction, &session_id, now)?;
        #[cfg(test)]
        inject_terminal_fault(
            &mut self.terminal_fault,
            TerminalFaultBoundary::AfterSessionTouch,
        )?;
        transaction.commit()?;
        Ok(())
    }

    #[cfg(test)]
    fn inject_terminal_fault_at(&mut self, boundary: TerminalFaultBoundary) {
        self.terminal_fault = Some(boundary);
    }

    #[cfg(test)]
    fn inject_voice_fault_at(&mut self, boundary: VoiceFaultBoundary) {
        self.voice_fault = Some(boundary);
    }

    #[cfg(test)]
    fn user_version(&self) -> AppResult<u32> {
        let version: u32 = self
            .connection
            .pragma_query_value(None, "user_version", |row| row.get(0))?;
        Ok(version)
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

fn append_record_in(
    connection: &Connection,
    run_id: &str,
    record: &RecordedEvent,
) -> AppResult<()> {
    let event_json = serde_json::to_string(&record.event)?;
    let seq = sqlite_i64(record.seq, "seq")?;
    let occurred_at_ms = sqlite_i64(record.occurred_at_ms, "occurred_at_ms")?;
    let inserted = connection.execute(
        "INSERT OR IGNORE INTO ledger_events (run_id, seq, occurred_at_ms, v, event_json)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![run_id, seq, occurred_at_ms, LEDGER_VERSION, event_json],
    )?;
    if inserted == 1 {
        return Ok(());
    }

    let existing = connection.query_row(
        "SELECT occurred_at_ms, v, event_json
         FROM ledger_events
         WHERE run_id = ?1 AND seq = ?2",
        params![run_id, seq],
        |row| {
            Ok(ExistingEvent {
                occurred_at_ms: row_u64(row, 0, "occurred_at_ms")?,
                version: row.get(1)?,
                event_json: row.get(2)?,
            })
        },
    )?;
    if existing.occurred_at_ms == record.occurred_at_ms
        && existing.version == LEDGER_VERSION
        && existing.event_json == event_json
    {
        Ok(())
    } else {
        Err(AppError::LedgerConflict {
            run_id: run_id.into(),
            seq: record.seq,
        })
    }
}

fn read_run_from(connection: &Connection, run_id: &str) -> AppResult<Vec<RecordedEvent>> {
    let records = read_run_records_from(connection, run_id)?;
    if records.is_empty() {
        Err(AppError::RunNotFound(run_id.into()))
    } else {
        Ok(records)
    }
}

fn read_run_records_from(connection: &Connection, run_id: &str) -> AppResult<Vec<RecordedEvent>> {
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

fn read_voice_events_from(
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

fn first_voice_difference(existing: &[VoiceEventEnvelope], proposed: &[VoiceEventEnvelope]) -> u64 {
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

fn validate_voice_event_stream(events: &[VoiceEvent]) -> AppResult<()> {
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

fn validate_voice_event_keys(
    connection: &Connection,
    run_id: &str,
    envelopes: &[VoiceEventEnvelope],
) -> AppResult<()> {
    let records = read_run_records_from(connection, run_id)?;
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

fn replay_records(records: &[RecordedEvent]) -> AppResult<RunState> {
    let mut state = RunState::new();
    for record in records {
        state.apply(record)?;
    }
    Ok(state)
}

fn final_answer_from_records<'a>(
    run_id: &RunId,
    records: &'a [RecordedEvent],
) -> AppResult<&'a str> {
    final_answer_from_records_str(run_id.as_str(), records)
}

fn final_answer_from_records_str<'a>(
    run_id: &str,
    records: &'a [RecordedEvent],
) -> AppResult<&'a str> {
    records
        .iter()
        .rev()
        .find_map(|record| match &record.event {
            HarnessEvent::ModelResponded { output, .. }
                if output.role == MessageRole::Assistant =>
            {
                Some(output.content.as_str())
            }
            _ => None,
        })
        .ok_or_else(|| {
            AppError::Config(format!(
                "finished session run {run_id} has no final assistant answer"
            ))
        })
}

fn failure_status(reason: &str) -> RunStateName {
    if reason == RUN_CANCELED_REASON {
        RunStateName::Canceled
    } else {
        RunStateName::Failed
    }
}

fn update_running_session_outcome(
    connection: &Connection,
    run_id: &str,
    status: RunStateName,
    final_answer: Option<&str>,
    error: Option<&str>,
    now: i64,
) -> AppResult<()> {
    let updated = connection.execute(
        "UPDATE session_runs
         SET status = ?2, final_answer = ?3, error = ?4, updated_at_ms = ?5
         WHERE run_id = ?1 AND status = ?6",
        params![
            run_id,
            status.as_str(),
            final_answer,
            error,
            now,
            RunStateName::Running.as_str()
        ],
    )?;
    if updated == 0 {
        return Err(AppError::RunNotFound(run_id.into()));
    }
    Ok(())
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TerminalFaultBoundary {
    BeforeEventInsert,
    AfterEventInsert,
    BeforeOutcomeUpdate,
    AfterOutcomeUpdate,
    BeforeSessionTouch,
    AfterSessionTouch,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VoiceFaultBoundary {
    AfterFirstInsert,
    BeforeCommit,
}

#[cfg(test)]
fn inject_terminal_fault(
    configured: &mut Option<TerminalFaultBoundary>,
    boundary: TerminalFaultBoundary,
) -> AppResult<()> {
    if *configured == Some(boundary) {
        *configured = None;
        return Err(AppError::Config(format!(
            "injected terminal fault at {boundary:?}"
        )));
    }
    Ok(())
}

#[cfg(test)]
fn inject_voice_fault(
    configured: &mut Option<VoiceFaultBoundary>,
    boundary: VoiceFaultBoundary,
    reached: bool,
) -> AppResult<()> {
    if reached && *configured == Some(boundary) {
        *configured = None;
        return Err(AppError::Config(format!(
            "injected voice transaction fault at {boundary:?}"
        )));
    }
    Ok(())
}

fn status_from_row(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<RunStateName> {
    let value: String = row.get(index)?;
    serde_json::from_value(serde_json::Value::String(value)).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(index, Type::Text, Box::new(error))
    })
}

struct ExistingEvent {
    occurred_at_ms: u64,
    version: u32,
    event_json: String,
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

pub(crate) fn sqlite_i64(value: u64, field: &str) -> AppResult<i64> {
    value
        .try_into()
        .map_err(|_| AppError::Config(format!("ledger {field} exceeds sqlite integer: {value}")))
}

pub(crate) fn row_u64(row: &rusqlite::Row<'_>, index: usize, field: &str) -> rusqlite::Result<u64> {
    let value: i64 = row.get(index)?;
    value.try_into().map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            Type::Integer,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("ledger {field} is negative: {value}"),
            )),
        )
    })
}

fn session_exists_in(connection: &Connection, session_id: &str) -> AppResult<bool> {
    Ok(connection
        .query_row(
            "SELECT 1 FROM sessions WHERE session_id = ?1 LIMIT 1",
            params![session_id],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

fn active_run_in(connection: &Connection, session_id: &str) -> AppResult<Option<String>> {
    Ok(connection
        .query_row(
            "SELECT run_id
             FROM session_runs
             WHERE session_id = ?1 AND status = ?2
             ORDER BY session_index ASC
             LIMIT 1",
            params![session_id, RunStateName::Running.as_str()],
            |row| row.get::<_, String>(0),
        )
        .optional()?)
}

fn touch_session(
    transaction: &rusqlite::Transaction<'_>,
    session_id: &str,
    now: i64,
) -> rusqlite::Result<()> {
    transaction.execute(
        "UPDATE sessions SET updated_at_ms = ?2 WHERE session_id = ?1",
        params![session_id, now],
    )?;
    Ok(())
}

#[cfg(unix)]
fn open_private_default_sqlite(
    location: &DefaultSqlitePath,
    create: bool,
) -> AppResult<SqliteLedger> {
    prepare_private_directories(location)?;
    let database = restrict_private_file(location.as_path(), create)?;
    restrict_existing_sidecars(location.as_path())?;

    let flags = if create {
        rusqlite::OpenFlags::default() | rusqlite::OpenFlags::SQLITE_OPEN_NOFOLLOW
    } else {
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NOFOLLOW
    };
    let mut connection = Connection::open_with_flags(location.as_path(), flags)?;
    verify_open_file(
        location.as_path(),
        &database,
        PRIVATE_FILE_MODE,
        current_uid(),
    )?;
    let schema_version = if create {
        configure_sqlite_connection(&connection)?;
        migrate_sqlite(&mut connection)?;
        configure_sqlite_journal_mode(&connection)?;
        SQLITE_SCHEMA_VERSION
    } else {
        configure_sqlite_connection(&connection)?;
        read_sqlite_schema_version(&connection)?
    };
    restrict_existing_sidecars(location.as_path())?;
    verify_open_file(
        location.as_path(),
        &database,
        PRIVATE_FILE_MODE,
        current_uid(),
    )?;
    Ok(SqliteLedger {
        connection,
        schema_version,
        #[cfg(test)]
        terminal_fault: None,
        #[cfg(test)]
        voice_fault: None,
    })
}

#[cfg(unix)]
fn prepare_private_directories(location: &DefaultSqlitePath) -> std::io::Result<()> {
    let workspace_directory = location
        .as_path()
        .parent()
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "default ledger has no parent"))?;
    let workspaces_directory = workspace_directory.parent().ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidInput,
            "default ledger workspace directory has no parent",
        )
    })?;
    let state_root = workspaces_directory.parent().ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidInput,
            "default ledger workspaces directory has no parent",
        )
    })?;
    if location.as_path().file_name() != Some(std::ffi::OsStr::new("ledger.db"))
        || workspaces_directory.file_name() != Some(std::ffi::OsStr::new("workspaces"))
        || state_root.file_name() != Some(std::ffi::OsStr::new("platonic"))
    {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "default ledger path does not match the app state layout",
        ));
    }
    let state_home = state_root
        .parent()
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "state root has no parent"))?;
    fs::create_dir_all(state_home)?;

    for directory in [state_root, workspaces_directory, workspace_directory] {
        restrict_private_directory(directory, current_uid())?;
    }
    Ok(())
}

#[cfg(unix)]
fn restrict_private_directory(path: &Path, expected_uid: u32) -> std::io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => verify_metadata(path, &metadata, true, expected_uid)?,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            match fs::DirBuilder::new()
                .mode(PRIVATE_DIRECTORY_MODE)
                .create(path)
            {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
        Err(error) => return Err(error),
    }
    fs::set_permissions(path, fs::Permissions::from_mode(PRIVATE_DIRECTORY_MODE))?;
    let metadata = fs::symlink_metadata(path)?;
    verify_metadata(path, &metadata, true, expected_uid)?;
    verify_mode(path, &metadata, PRIVATE_DIRECTORY_MODE)
}

#[cfg(unix)]
fn restrict_private_file(path: &Path, create: bool) -> std::io::Result<File> {
    let expected_uid = current_uid();
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            verify_metadata(path, &metadata, false, expected_uid)?;
            fs::set_permissions(path, fs::Permissions::from_mode(PRIVATE_FILE_MODE))?;
        }
        Err(error) if error.kind() == ErrorKind::NotFound && create => {}
        Err(error) => return Err(error),
    }

    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32);
    if create {
        options.write(true).create(true).mode(PRIVATE_FILE_MODE);
    }
    let file = options.open(path)?;
    file.set_permissions(fs::Permissions::from_mode(PRIVATE_FILE_MODE))?;
    verify_open_file(path, &file, PRIVATE_FILE_MODE, expected_uid)?;
    Ok(file)
}

#[cfg(unix)]
fn restrict_existing_sidecars(database: &Path) -> std::io::Result<()> {
    for suffix in ["-journal", "-wal", "-shm"] {
        let mut sidecar = database.as_os_str().to_os_string();
        sidecar.push(suffix);
        let path = PathBuf::from(sidecar);
        restrict_existing_sidecar(&path)?;
    }
    Ok(())
}

#[cfg(unix)]
fn restrict_existing_sidecar(path: &Path) -> std::io::Result<()> {
    for attempt in 0..SIDECAR_HARDEN_ATTEMPTS {
        match restrict_private_file(path, false) {
            Ok(_) => return Ok(()),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                if sidecar_is_absent(path)? {
                    return Ok(());
                }
                if attempt + 1 == SIDECAR_HARDEN_ATTEMPTS {
                    return Err(Error::new(
                        ErrorKind::PermissionDenied,
                        format!("SQLite sidecar kept changing: {}", path.display()),
                    ));
                }
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("sidecar hardening loop always returns")
}

#[cfg(unix)]
fn sidecar_is_absent(path: &Path) -> std::io::Result<bool> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(true),
        Ok(_) => Ok(false),
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
fn verify_open_file(
    path: &Path,
    file: &File,
    expected_mode: u32,
    expected_uid: u32,
) -> std::io::Result<()> {
    let open_metadata = file.metadata()?;
    verify_metadata(path, &open_metadata, false, expected_uid)?;
    verify_mode(path, &open_metadata, expected_mode)?;
    let path_metadata = fs::symlink_metadata(path)?;
    verify_metadata(path, &path_metadata, false, expected_uid)?;
    verify_mode(path, &path_metadata, expected_mode)?;
    if open_metadata.dev() != path_metadata.dev() || open_metadata.ino() != path_metadata.ino() {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            format!("ledger path changed while opening: {}", path.display()),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn verify_metadata(
    path: &Path,
    metadata: &fs::Metadata,
    directory: bool,
    expected_uid: u32,
) -> std::io::Result<()> {
    let expected_type = if directory {
        "directory"
    } else {
        "regular file"
    };
    let actual_type_matches = if directory {
        metadata.file_type().is_dir()
    } else {
        metadata.file_type().is_file()
    };
    if !actual_type_matches || metadata.file_type().is_symlink() {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            format!(
                "private state path is not a {expected_type}: {}",
                path.display()
            ),
        ));
    }
    if metadata.uid() != expected_uid {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            format!(
                "private state path is not owned by the current user: {}",
                path.display()
            ),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn verify_mode(path: &Path, metadata: &fs::Metadata, expected: u32) -> std::io::Result<()> {
    let actual = metadata.permissions().mode() & 0o777;
    if actual == expected {
        return Ok(());
    }
    Err(Error::new(
        ErrorKind::PermissionDenied,
        format!(
            "unsafe permissions on {}: expected {expected:04o}, got {actual:04o}",
            path.display()
        ),
    ))
}

#[cfg(unix)]
fn current_uid() -> u32 {
    rustix::process::geteuid().as_raw()
}

fn configure_sqlite_connection(connection: &Connection) -> AppResult<()> {
    connection.busy_timeout(SQLITE_BUSY_TIMEOUT)?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    Ok(())
}

fn configure_sqlite_journal_mode(connection: &Connection) -> AppResult<()> {
    let journal_mode: String =
        connection.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
    if journal_mode != "wal" {
        connection.pragma_update(None, "journal_mode", "WAL")?;
    }
    Ok(())
}

fn read_sqlite_schema_version(connection: &Connection) -> AppResult<u32> {
    let actual: u32 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if actual > SQLITE_SCHEMA_VERSION {
        return Err(AppError::SqliteSchemaVersion {
            expected: SQLITE_SCHEMA_VERSION,
            actual,
        });
    }
    Ok(actual)
}

fn migrate_sqlite(connection: &mut Connection) -> AppResult<()> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let version: u32 = transaction.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version > SQLITE_SCHEMA_VERSION {
        return Err(AppError::Config(format!(
            "unsupported sqlite schema version: {version}"
        )));
    }
    if version < LEGACY_SQLITE_SCHEMA_VERSION {
        transaction.execute_batch(
            r#"
            CREATE TABLE ledger_events (
              run_id TEXT NOT NULL,
              seq INTEGER NOT NULL,
              occurred_at_ms INTEGER NOT NULL,
              v INTEGER NOT NULL,
              event_json TEXT NOT NULL,
              PRIMARY KEY (run_id, seq)
            );
            "#,
        )?;
    }
    if version < SESSION_SQLITE_SCHEMA_VERSION {
        create_session_tables(&transaction)?;
    }
    if version < VOICE_EVENT_SQLITE_SCHEMA_VERSION {
        create_voice_event_table(&transaction)?;
    }
    if version < SQLITE_SCHEMA_VERSION {
        transaction.pragma_update(None, "user_version", SQLITE_SCHEMA_VERSION)?;
    }
    transaction.commit()?;
    Ok(())
}

fn create_session_tables(connection: &Connection) -> AppResult<()> {
    connection.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS sessions (
          session_id TEXT PRIMARY KEY,
          created_at_ms INTEGER NOT NULL,
          updated_at_ms INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS session_runs (
          session_id TEXT NOT NULL,
          run_id TEXT PRIMARY KEY,
          session_index INTEGER NOT NULL,
          question TEXT NOT NULL,
          final_answer TEXT,
          status TEXT NOT NULL,
          error TEXT,
          created_at_ms INTEGER NOT NULL,
          updated_at_ms INTEGER NOT NULL,
          UNIQUE(session_id, session_index)
        );

        CREATE INDEX IF NOT EXISTS session_runs_session_index
          ON session_runs(session_id, session_index);
        "#,
    )?;
    Ok(())
}

fn create_voice_event_table(connection: &Connection) -> AppResult<()> {
    connection.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS voice_events (
          run_id TEXT NOT NULL,
          turn_id TEXT NOT NULL,
          sequence INTEGER NOT NULL,
          v INTEGER NOT NULL,
          event_json TEXT NOT NULL,
          PRIMARY KEY (run_id, sequence)
        );
        "#,
    )?;
    Ok(())
}

fn next_record(state: &mut RunState, event: HarnessEvent) -> AppResult<RecordedEvent> {
    let record = RecordedEvent {
        seq: state.next_seq(),
        occurred_at_ms: now_ms(),
        event,
    };
    state.apply(&record)?;
    Ok(record)
}

fn supported_ledger_version(version: u32) -> bool {
    matches!(version, LEGACY_LEDGER_VERSION | LEDGER_VERSION)
}

pub fn read_records(path: &Path) -> AppResult<Vec<RecordedEvent>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut records = Vec::new();

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let line: LedgerLine = serde_json::from_str(&line)?;
        if !supported_ledger_version(line.v) {
            return Err(AppError::LedgerVersion {
                expected: LEDGER_VERSION,
                actual: line.v,
            });
        }
        records.push(line.record);
    }

    Ok(records)
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

pub fn interrupt_orphaned_sqlite_runs(path: &Path) -> AppResult<usize> {
    if !path.exists() {
        return Ok(0);
    }
    SqliteLedger::open_or_create(path)?.interrupt_running_session_runs(ORPHANED_RUN_ERROR)
}

pub fn interrupt_orphaned_default_sqlite_runs(path: &DefaultSqlitePath) -> AppResult<usize> {
    if fs::symlink_metadata(path.as_path())
        .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
    {
        return Ok(0);
    }
    SqliteLedger::open_or_create_default(path)?.interrupt_running_session_runs(ORPHANED_RUN_ERROR)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().try_into().unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use platonic_core::{
        ActorId, AgentId, ContextPack, HarnessEvent, Message, MessageRole, ModelName, ModelUsage,
        PolicyDecision, RunId, RunReadback, ToolCallId, TurnId,
    };
    use serde_json::Value;
    use std::{
        sync::atomic::{AtomicBool, Ordering},
        thread,
        time::Instant,
    };

    #[cfg(unix)]
    use std::process::Command;

    #[cfg(unix)]
    fn default_location(root: &Path) -> DefaultSqlitePath {
        DefaultSqlitePath::from_path(
            root.join("state")
                .join("platonic")
                .join("workspaces")
                .join("workspace-1")
                .join("ledger.db"),
        )
    }

    #[cfg(unix)]
    fn mode(path: &Path) -> u32 {
        fs::symlink_metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[cfg(unix)]
    fn set_mode(path: &Path, mode: u32) {
        fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
    }

    fn captured_voice_event(run_id: &str, turn_id: &str) -> VoiceEvent {
        VoiceEvent::VoiceCaptured {
            run_id: RunId::new(run_id).unwrap(),
            turn_id: TurnId::new(turn_id).unwrap(),
            transcript_sha256: "a".repeat(64),
            transcript_bytes: 14,
            transcript_span_ms: 800,
            input_frames: 38_400,
            output_frames: 12_800,
            vad_start_sample: 320,
            vad_speech_end_sample: 11_200,
            vad_close_sample: 12_800,
            vad_close_to_final_us: 105_000,
            normalization_resampling_us: 900,
        }
    }

    fn completed_voice_events(run_id: &str) -> Vec<VoiceEvent> {
        vec![
            captured_voice_event(run_id, "turn_1"),
            VoiceEvent::VoiceSpoken {
                run_id: RunId::new(run_id).unwrap(),
                turn_id: TurnId::new("turn_1").unwrap(),
                ttfa_ms: 289,
                sentence_count: 2,
                interrupted_at: Some(1),
            },
            VoiceEvent::VoiceInterrupted {
                run_id: RunId::new(run_id).unwrap(),
                turn_id: TurnId::new("turn_1").unwrap(),
                spoken_prefix: "This prefix was audible".into(),
                delta_index: 7,
            },
        ]
    }

    fn append_voice_core_keys(ledger: &mut SqliteLedger, run_id: &str, turn_id: &str) {
        ledger
            .append(run_id, &started_record(run_id, 0, 0))
            .unwrap();
        ledger
            .append(
                run_id,
                &RecordedEvent {
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
                },
            )
            .unwrap();
    }

    #[test]
    fn opens_sqlite_ledger_with_wal_full_and_default_autocheckpoint() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.db");
        let ledger = SqliteLedger::open_or_create(&path).unwrap();

        let journal_mode: String = ledger
            .connection
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .unwrap();
        let synchronous: u32 = ledger
            .connection
            .pragma_query_value(None, "synchronous", |row| row.get(0))
            .unwrap();
        let autocheckpoint: u32 = ledger
            .connection
            .pragma_query_value(None, "wal_autocheckpoint", |row| row.get(0))
            .unwrap();

        assert_eq!(journal_mode, "wal");
        assert_eq!(synchronous, 2);
        assert_eq!(autocheckpoint, 1_000);
    }

    #[test]
    fn releasing_held_reader_allows_checkpoint_to_reduce_wal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.db");
        let mut writer = SqliteLedger::open_or_create(&path).unwrap();
        writer
            .append("run_checkpoint", &started_record("run_checkpoint", 0, 0))
            .unwrap();

        let reader = SqliteLedger::open_readonly(&path).unwrap();
        reader.connection.execute_batch("BEGIN").unwrap();
        let visible_rows: u32 = reader
            .connection
            .query_row("SELECT COUNT(*) FROM ledger_events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(visible_rows, 1);

        writer
            .append("run_checkpoint", &started_record("run_checkpoint", 1, 1))
            .unwrap();

        let mut wal_path = path.as_os_str().to_os_string();
        wal_path.push("-wal");
        let wal_path = PathBuf::from(wal_path);
        let wal_len_held = fs::metadata(&wal_path).unwrap().len();
        let (_, log_frames, checkpointed_frames): (u32, u32, u32) = writer
            .connection
            .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .unwrap();

        assert!(checkpointed_frames < log_frames);

        reader.connection.execute_batch("COMMIT").unwrap();
        drop(reader);

        let checkpoint: (u32, u32, u32) = writer
            .connection
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .unwrap();
        let wal_len_released = fs::metadata(&wal_path).unwrap().len();

        assert_eq!(checkpoint, (0, 0, 0));
        assert!(wal_len_released < wal_len_held);
    }

    #[cfg(unix)]
    #[test]
    fn default_sqlite_creation_ignores_permissive_umask() {
        const CHILD: &str = "PLATO_TEST_LEDGER_PERMISSIVE_UMASK";
        if std::env::var_os(CHILD).is_none() {
            let output = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "ledger::tests::default_sqlite_creation_ignores_permissive_umask",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "child failed:\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }

        rustix::process::umask(rustix::fs::Mode::empty());
        let root = tempfile::tempdir().unwrap();
        let location = default_location(root.path());
        drop(SqliteLedger::open_or_create_default(&location).unwrap());

        let workspace_directory = location.as_path().parent().unwrap();
        let workspaces_directory = workspace_directory.parent().unwrap();
        let state_root = workspaces_directory.parent().unwrap();
        assert_eq!(mode(state_root), PRIVATE_DIRECTORY_MODE);
        assert_eq!(mode(workspaces_directory), PRIVATE_DIRECTORY_MODE);
        assert_eq!(mode(workspace_directory), PRIVATE_DIRECTORY_MODE);
        assert_eq!(mode(location.as_path()), PRIVATE_FILE_MODE);
    }

    #[cfg(unix)]
    #[test]
    fn default_sqlite_tightens_existing_paths_and_preserves_content_on_reopen() {
        let root = tempfile::tempdir().unwrap();
        let location = default_location(root.path());
        fs::create_dir_all(location.as_path().parent().unwrap()).unwrap();
        let connection = Connection::open(location.as_path()).unwrap();
        connection
            .execute_batch("CREATE TABLE proof (value TEXT); INSERT INTO proof VALUES ('kept');")
            .unwrap();
        drop(connection);

        for directory in [
            location
                .as_path()
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .parent()
                .unwrap(),
            location.as_path().parent().unwrap().parent().unwrap(),
            location.as_path().parent().unwrap(),
        ] {
            set_mode(directory, 0o755);
        }
        set_mode(location.as_path(), 0o644);

        for _ in 0..2 {
            let ledger = SqliteLedger::open_or_create_default(&location).unwrap();
            let value: String = ledger
                .connection
                .query_row("SELECT value FROM proof", [], |row| row.get(0))
                .unwrap();
            assert_eq!(value, "kept");
        }
        assert_eq!(mode(location.as_path()), PRIVATE_FILE_MODE);
    }

    #[cfg(unix)]
    #[test]
    fn default_sqlite_rejects_symlinks_and_wrong_types() {
        use std::os::unix::fs::symlink;

        let symlink_root = tempfile::tempdir().unwrap();
        let location = default_location(symlink_root.path());
        fs::create_dir_all(location.as_path().parent().unwrap()).unwrap();
        let target = symlink_root.path().join("target.db");
        fs::write(&target, []).unwrap();
        symlink(&target, location.as_path()).unwrap();
        assert!(SqliteLedger::open_or_create_default(&location).is_err());
        assert!(SqliteLedger::open_default_readonly(&location).is_err());

        let directory_root = tempfile::tempdir().unwrap();
        let location = default_location(directory_root.path());
        fs::create_dir_all(location.as_path()).unwrap();
        assert!(SqliteLedger::open_or_create_default(&location).is_err());

        let sidecar_root = tempfile::tempdir().unwrap();
        let location = default_location(sidecar_root.path());
        drop(SqliteLedger::open_or_create_default(&location).unwrap());
        let mut journal = location.as_path().as_os_str().to_os_string();
        journal.push("-journal");
        symlink(&target, PathBuf::from(journal)).unwrap();
        assert!(SqliteLedger::open_or_create_default(&location).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn private_state_verifier_rejects_foreign_owner() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("agent.db");
        fs::write(&file, []).unwrap();
        let metadata = fs::symlink_metadata(&file).unwrap();
        let foreign_uid = if current_uid() == u32::MAX {
            current_uid() - 1
        } else {
            current_uid() + 1
        };

        let error = verify_metadata(&file, &metadata, false, foreign_uid).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::PermissionDenied);
        assert!(error.to_string().contains("not owned"));
    }

    #[cfg(unix)]
    #[test]
    fn default_sqlite_sidecars_are_private() {
        let root = tempfile::tempdir().unwrap();
        let location = default_location(root.path());
        let ledger = SqliteLedger::open_or_create_default(&location).unwrap();
        ledger
            .connection
            .execute_batch("CREATE TABLE proof (value INTEGER); INSERT INTO proof VALUES (1);")
            .unwrap();
        ledger
            .connection
            .execute_batch("BEGIN IMMEDIATE; UPDATE proof SET value = 2;")
            .unwrap();

        for suffix in ["-wal", "-shm"] {
            let mut sidecar = location.as_path().as_os_str().to_os_string();
            sidecar.push(suffix);
            let sidecar = PathBuf::from(sidecar);
            assert!(sidecar.is_file());
            assert_eq!(mode(&sidecar), PRIVATE_FILE_MODE);
        }
        ledger.connection.execute_batch("ROLLBACK").unwrap();
        drop(ledger);

        for suffix in ["-journal", "-wal", "-shm"] {
            let mut sidecar = location.as_path().as_os_str().to_os_string();
            sidecar.push(suffix);
            fs::write(PathBuf::from(sidecar), []).unwrap();
        }
        restrict_existing_sidecars(location.as_path()).unwrap();
        for suffix in ["-journal", "-wal", "-shm"] {
            let mut sidecar = location.as_path().as_os_str().to_os_string();
            sidecar.push(suffix);
            assert_eq!(mode(&PathBuf::from(sidecar)), PRIVATE_FILE_MODE);
        }
    }

    #[cfg(unix)]
    #[test]
    fn missing_or_reappeared_sidecars_are_rechecked() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let sidecar = root.path().join("agent.db-journal");
        assert!(sidecar_is_absent(&sidecar).unwrap());
        restrict_existing_sidecar(&sidecar).unwrap();

        fs::write(&sidecar, []).unwrap();
        set_mode(&sidecar, 0o644);
        assert!(!sidecar_is_absent(&sidecar).unwrap());
        restrict_existing_sidecar(&sidecar).unwrap();
        assert_eq!(mode(&sidecar), PRIVATE_FILE_MODE);

        fs::remove_file(&sidecar).unwrap();
        let target = root.path().join("target");
        fs::write(&target, []).unwrap();
        symlink(&target, &sidecar).unwrap();
        assert_eq!(
            restrict_existing_sidecar(&sidecar).unwrap_err().kind(),
            ErrorKind::PermissionDenied
        );

        fs::remove_file(&sidecar).unwrap();
        fs::create_dir(&sidecar).unwrap();
        assert_eq!(
            restrict_existing_sidecar(&sidecar).unwrap_err().kind(),
            ErrorKind::PermissionDenied
        );
    }

    #[cfg(unix)]
    #[test]
    fn explicit_sqlite_path_keeps_caller_managed_permissions() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("custom");
        let path = parent.join("agent.db");
        fs::create_dir(&parent).unwrap();
        drop(SqliteLedger::open_or_create(&path).unwrap());
        set_mode(&parent, 0o755);
        set_mode(&path, 0o644);

        drop(SqliteLedger::open_or_create(&path).unwrap());

        assert_eq!(mode(&parent), 0o755);
        assert_eq!(mode(&path), 0o644);
    }

    static FIRST_SESSION_BUSY: AtomicBool = AtomicBool::new(false);
    static SECOND_SESSION_BUSY: AtomicBool = AtomicBool::new(false);

    fn wait_first_session_writer(_: i32) -> bool {
        FIRST_SESSION_BUSY.store(true, Ordering::SeqCst);
        thread::yield_now();
        true
    }

    fn wait_second_session_writer(_: i32) -> bool {
        SECOND_SESSION_BUSY.store(true, Ordering::SeqCst);
        thread::yield_now();
        true
    }

    #[test]
    fn writes_and_reads_versioned_jsonl_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let mut recorder = JsonlEventRecorder::create(&path).unwrap();

        recorder
            .record(HarnessEvent::RunStarted {
                run_id: RunId::new("run_1").unwrap(),
                agent_id: AgentId::new("plato").unwrap(),
            })
            .unwrap();

        let records = read_records(&path).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].seq, 0);
        let raw: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(raw["v"], LEDGER_VERSION);
    }

    #[test]
    fn rejects_wrong_ledger_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        std::fs::write(&path, r#"{"v":3,"record":{"seq":0,"occurred_at_ms":0,"event":{"event":"run_started","run_id":"run_1","agent_id":"plato"}}}"#).unwrap();

        assert!(matches!(
            read_records(&path),
            Err(AppError::LedgerVersion {
                expected: LEDGER_VERSION,
                actual: 3
            })
        ));
    }

    #[test]
    fn new_jsonl_and_sqlite_ledgers_write_v2_with_null_unknown_usage() {
        let dir = tempfile::tempdir().unwrap();
        let jsonl_path = dir.path().join("events.jsonl");
        let sqlite_path = dir.path().join("events.db");
        let run_id = RunId::new("run_unknown_usage").unwrap();
        let mut jsonl = JsonlEventRecorder::create(&jsonl_path).unwrap();
        let mut sqlite = SqliteEventRecorder::create(&sqlite_path, &run_id).unwrap();

        for event in response_run_events(&run_id, None) {
            jsonl.record(event.clone()).unwrap();
            sqlite.record(event).unwrap();
        }
        drop(jsonl);
        drop(sqlite);

        let jsonl_lines = std::fs::read_to_string(&jsonl_path).unwrap();
        let jsonl_response = jsonl_lines
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .find(|line| line["record"]["event"]["event"] == "model_responded")
            .unwrap();
        assert_eq!(jsonl_response["v"], LEDGER_VERSION);
        assert!(jsonl_response["record"]["event"]["usage"].is_null());

        let connection = Connection::open(&sqlite_path).unwrap();
        let (version, event_json): (u32, String) = connection
            .query_row(
                "SELECT v, event_json
                 FROM ledger_events
                 WHERE run_id = ?1 AND seq = 3",
                params![run_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(version, LEDGER_VERSION);
        let sqlite_response: Value = serde_json::from_str(&event_json).unwrap();
        assert!(sqlite_response["usage"].is_null());

        for records in [
            read_records(&jsonl_path).unwrap(),
            read_sqlite_records(&sqlite_path, Some(run_id.as_str())).unwrap(),
        ] {
            assert!(matches!(
                &records[3].event,
                HarnessEvent::ModelResponded { usage: None, .. }
            ));
        }
    }

    #[test]
    fn refuses_to_overwrite_existing_jsonl_ledger() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        std::fs::write(&path, "").unwrap();

        assert!(matches!(
            JsonlEventRecorder::create(&path),
            Err(AppError::LedgerExists(_))
        ));
    }

    #[test]
    fn migrates_empty_sqlite_database() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.db");
        let ledger = SqliteLedger::open_or_create(&path).unwrap();

        assert_eq!(ledger.user_version().unwrap(), SQLITE_SCHEMA_VERSION);
        let voice_schema = ledger
            .connection
            .query_row(
                "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 'voice_events'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap();
        assert_eq!(
            voice_schema,
            "CREATE TABLE voice_events (\n          run_id TEXT NOT NULL,\n          turn_id TEXT NOT NULL,\n          sequence INTEGER NOT NULL,\n          v INTEGER NOT NULL,\n          event_json TEXT NOT NULL,\n          PRIMARY KEY (run_id, sequence)\n        )"
        );
    }

    #[test]
    fn authority_migration_is_additive_idempotent_and_readonly_v2_safe() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v2-events.db");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                r#"
                CREATE TABLE ledger_events (
                  run_id TEXT NOT NULL,
                  seq INTEGER NOT NULL,
                  occurred_at_ms INTEGER NOT NULL,
                  v INTEGER NOT NULL,
                  event_json TEXT NOT NULL,
                  PRIMARY KEY (run_id, seq)
                );
                PRAGMA user_version = 2;
                "#,
            )
            .unwrap();
        create_session_tables(&connection).unwrap();
        connection
            .execute(
                "INSERT INTO sessions (session_id, created_at_ms, updated_at_ms) VALUES ('kept', 1, 1)",
                [],
            )
            .unwrap();
        drop(connection);

        let bytes_before_read = fs::read(&path).unwrap();
        let readonly = SqliteLedger::open_readonly(&path).unwrap();
        assert!(readonly.read_voice_events("run_absent").unwrap().is_empty());
        drop(readonly);
        assert_eq!(fs::read(&path).unwrap(), bytes_before_read);

        let migrated = SqliteLedger::open_or_create(&path).unwrap();
        assert_eq!(migrated.user_version().unwrap(), SQLITE_SCHEMA_VERSION);
        let kept: String = migrated
            .connection
            .query_row("SELECT session_id FROM sessions", [], |row| row.get(0))
            .unwrap();
        assert_eq!(kept, "kept");
        assert!(migrated.read_voice_events("run_absent").unwrap().is_empty());
        drop(migrated);

        let bytes_after_migration = fs::read(&path).unwrap();
        drop(SqliteLedger::open_or_create(&path).unwrap());
        assert_eq!(fs::read(&path).unwrap(), bytes_after_migration);
    }

    #[test]
    fn sqlite_append_is_idempotent_and_conflict_checked() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.db");
        let mut ledger = SqliteLedger::open_or_create(&path).unwrap();
        let record = started_record("run_1", 0, 10);

        ledger.append("run_1", &record).unwrap();
        ledger.append("run_1", &record).unwrap();

        let mut changed = record.clone();
        changed.occurred_at_ms = 11;
        assert!(matches!(
            ledger.append("run_1", &changed),
            Err(AppError::LedgerConflict { .. })
        ));
    }

    #[test]
    fn voice_companion_commit_is_ordered_idempotent_and_conflict_checked() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.db");
        let mut ledger = SqliteLedger::open_or_create(&path).unwrap();
        let events = completed_voice_events("run_voice");
        append_voice_core_keys(&mut ledger, "run_voice", "turn_1");

        let committed = ledger.append_voice_events(&events).unwrap();
        assert_eq!(
            committed
                .iter()
                .map(|envelope| envelope.sequence)
                .collect::<Vec<_>>(),
            [0, 1, 2]
        );
        assert_eq!(ledger.read_voice_events("run_voice").unwrap(), committed);
        let bytes_after_commit = fs::read(&path).unwrap();
        assert_eq!(ledger.append_voice_events(&events).unwrap(), committed);
        assert_eq!(fs::read(&path).unwrap(), bytes_after_commit);

        let mut changed = events.clone();
        let VoiceEvent::VoiceSpoken { ttfa_ms, .. } = &mut changed[1] else {
            unreachable!()
        };
        *ttfa_ms += 1;
        assert!(matches!(
            ledger.append_voice_events(&changed),
            Err(AppError::VoiceLedgerConflict {
                run_id,
                sequence: 1
            }) if run_id == "run_voice"
        ));
        assert_eq!(ledger.read_voice_events("run_voice").unwrap(), committed);
    }

    #[test]
    fn voice_companion_rejects_orphan_and_misordered_interruption_facts() {
        let run_id = RunId::new("run_voice").unwrap();
        let turn_id = TurnId::new("turn_1").unwrap();
        let spoken = VoiceEvent::VoiceSpoken {
            run_id: run_id.clone(),
            turn_id: turn_id.clone(),
            ttfa_ms: 280,
            sentence_count: 1,
            interrupted_at: Some(0),
        };
        let interrupted = VoiceEvent::VoiceInterrupted {
            run_id,
            turn_id,
            spoken_prefix: "audible".into(),
            delta_index: 2,
        };

        assert!(validate_voice_event_stream(std::slice::from_ref(&spoken)).is_err());
        assert!(validate_voice_event_stream(std::slice::from_ref(&interrupted)).is_err());
        assert!(validate_voice_event_stream(&[interrupted, spoken]).is_err());
    }

    #[test]
    fn voice_companion_faults_roll_back_the_entire_stream_before_reopen() {
        for boundary in [
            VoiceFaultBoundary::AfterFirstInsert,
            VoiceFaultBoundary::BeforeCommit,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join(format!("voice-{boundary:?}.db"));
            let events = completed_voice_events("run_fault");
            let mut ledger = SqliteLedger::open_or_create(&path).unwrap();
            append_voice_core_keys(&mut ledger, "run_fault", "turn_1");
            ledger.inject_voice_fault_at(boundary);

            assert!(matches!(
                ledger.append_voice_events(&events),
                Err(AppError::Config(message)) if message.contains("injected voice transaction fault")
            ));
            let rows: i64 = ledger
                .connection
                .query_row("SELECT COUNT(*) FROM voice_events", [], |row| row.get(0))
                .unwrap();
            assert_eq!(rows, 0);
            drop(ledger);

            let mut reopened = SqliteLedger::open_or_create(&path).unwrap();
            assert!(reopened.read_voice_events("run_fault").unwrap().is_empty());
            let committed = reopened.append_voice_events(&events).unwrap();
            drop(reopened);
            assert_eq!(
                SqliteLedger::open_readonly(&path)
                    .unwrap()
                    .read_voice_events("run_fault")
                    .unwrap(),
                committed
            );
        }
    }

    #[test]
    fn voice_companion_read_rejects_version_sequence_and_key_corruption() {
        let corruptions = [
            (
                "UPDATE voice_events SET v = 2 WHERE sequence = 0",
                "voice event version mismatch",
            ),
            (
                "UPDATE voice_events SET sequence = 4 WHERE sequence = 2",
                "voice event sequence was 4, expected 2",
            ),
            (
                "UPDATE voice_events SET turn_id = 'turn_wrong' WHERE sequence = 1",
                "voice event columns disagree with payload",
            ),
        ];
        for (sql, expected) in corruptions {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("events.db");
            let mut ledger = SqliteLedger::open_or_create(&path).unwrap();
            append_voice_core_keys(&mut ledger, "run_corrupt", "turn_1");
            ledger
                .append_voice_events(&completed_voice_events("run_corrupt"))
                .unwrap();
            ledger.connection.execute(sql, []).unwrap();
            assert!(
                ledger
                    .read_voice_events("run_corrupt")
                    .unwrap_err()
                    .to_string()
                    .contains(expected)
            );
        }
    }

    #[test]
    fn voice_companion_read_rejects_durable_cross_row_contract_corruption_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let unpaired_path = dir.path().join("unpaired-interruption.db");
        let mut ledger = SqliteLedger::open_or_create(&unpaired_path).unwrap();
        append_voice_core_keys(&mut ledger, "run_unpaired", "turn_1");
        ledger
            .append_voice_events(&completed_voice_events("run_unpaired"))
            .unwrap();
        ledger
            .connection
            .execute("DELETE FROM voice_events WHERE sequence = 2", [])
            .unwrap();
        drop(ledger);

        let error = SqliteLedger::open_readonly(&unpaired_path)
            .unwrap()
            .read_voice_events("run_unpaired")
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("interrupted voice speech must be followed")
        );

        let capture_path = dir.path().join("repeated-capture.db");
        let mut ledger = SqliteLedger::open_or_create(&capture_path).unwrap();
        append_voice_core_keys(&mut ledger, "run_capture", "turn_1");
        ledger
            .append_voice_events(&completed_voice_events("run_capture"))
            .unwrap();
        let repeated_capture =
            serde_json::to_string(&captured_voice_event("run_capture", "turn_1")).unwrap();
        ledger
            .connection
            .execute(
                "UPDATE voice_events SET event_json = ?1 WHERE sequence = 1",
                params![repeated_capture],
            )
            .unwrap();
        drop(ledger);

        let error = SqliteLedger::open_readonly(&capture_path)
            .unwrap()
            .read_voice_events("run_capture")
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("voice capture must appear at most once and first")
        );
    }

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
    fn sqlite_concurrent_sessions_avoid_deferred_write_upgrade_race() {
        FIRST_SESSION_BUSY.store(false, Ordering::SeqCst);
        SECOND_SESSION_BUSY.store(false, Ordering::SeqCst);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.db");
        let mut first_ledger = SqliteLedger::open_or_create(&path).unwrap();
        let mut second_ledger = SqliteLedger::open_or_create(&path).unwrap();
        first_ledger
            .connection
            .busy_handler(Some(wait_first_session_writer))
            .unwrap();
        second_ledger
            .connection
            .busy_handler(Some(wait_second_session_writer))
            .unwrap();

        let mut blocker_ledger = SqliteLedger::open_or_create(&path).unwrap();
        // A RESERVED lock makes DEFERRED readers fail on upgrade while IMMEDIATE writers wait.
        let blocker = blocker_ledger
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let first_handle = thread::spawn(move || {
            let run_id = RunId::new("run_1").unwrap();
            let result = first_ledger.begin_session_run("session_1", &run_id, "question one", true);
            (first_ledger, result)
        });
        let second_handle = thread::spawn(move || {
            let run_id = RunId::new("run_2").unwrap();
            let result =
                second_ledger.begin_session_run("session_2", &run_id, "question two", true);
            (second_ledger, result)
        });

        let deadline = Instant::now() + SQLITE_BUSY_TIMEOUT;
        loop {
            let both_waiting = FIRST_SESSION_BUSY.load(Ordering::SeqCst)
                && SECOND_SESSION_BUSY.load(Ordering::SeqCst);
            if both_waiting
                || first_handle.is_finished()
                || second_handle.is_finished()
                || Instant::now() >= deadline
            {
                break;
            }
            thread::yield_now();
        }
        let first_waited = FIRST_SESSION_BUSY.load(Ordering::SeqCst);
        let second_waited = SECOND_SESSION_BUSY.load(Ordering::SeqCst);
        blocker.commit().unwrap();

        let (mut first_ledger, first_result) = first_handle.join().unwrap();
        let (mut second_ledger, second_result) = second_handle.join().unwrap();
        assert!(
            first_waited && second_waited,
            "both writers must wait before reading: first={first_result:?}, second={second_result:?}"
        );
        assert!(first_result.unwrap().is_empty());
        assert!(second_result.unwrap().is_empty());

        for (ledger, run_id, occurred_at_ms, answer) in [
            (&mut first_ledger, "run_1", 10, "answer one"),
            (&mut second_ledger, "run_2", 20, "answer two"),
        ] {
            let run_id = RunId::new(run_id).unwrap();
            append_response_prefix(ledger, &run_id, answer, occurred_at_ms);
            ledger.finish_session_run(&run_id, answer).unwrap();
        }
        drop(first_ledger);
        drop(second_ledger);

        let ledger = SqliteLedger::open_readonly(&path).unwrap();
        for (session_id, run_id, question, answer) in [
            ("session_1", "run_1", "question one", "answer one"),
            ("session_2", "run_2", "question two", "answer two"),
        ] {
            let session = ledger.read_session(session_id).unwrap();
            assert_eq!(session.runs.len(), 1);
            assert_eq!(session.runs[0].run_id, run_id);
            assert_eq!(session.runs[0].question, question);
            assert_eq!(session.runs[0].status, RunStateName::Finished);
            assert_eq!(session.runs[0].final_answer.as_deref(), Some(answer));
            assert_eq!(session.runs[0].records.len(), 5);
            assert!(
                session.runs[0]
                    .records
                    .iter()
                    .all(|record| record.event.run_id().as_str() == run_id)
            );
        }
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

    #[test]
    fn sqlite_terminal_outcomes_and_orphan_recovery_preserve_continuation_truth() {
        const RECOVERY_ERROR: &str = "daemon restarted";

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.db");
        let mut ledger = SqliteLedger::open_or_create(&path).unwrap();
        let finished = RunId::new("run_finished").unwrap();
        let failed = RunId::new("run_failed").unwrap();
        let canceled = RunId::new("run_canceled").unwrap();
        let interrupted = RunId::new("run_interrupted").unwrap();

        ledger
            .begin_session_run("session_1", &finished, "finished question", true)
            .unwrap();
        append_response_prefix(&mut ledger, &finished, "finished answer", 0);
        ledger
            .finish_session_run(&finished, "finished answer")
            .unwrap();

        ledger
            .begin_session_run("session_1", &failed, "failed question", false)
            .unwrap();
        ledger
            .append(failed.as_str(), &started_record(failed.as_str(), 0, 10))
            .unwrap();
        ledger
            .fail_session_run(&failed, "synthetic failure", false)
            .unwrap();

        ledger
            .begin_session_run("session_1", &canceled, "canceled question", false)
            .unwrap();
        ledger
            .append(canceled.as_str(), &started_record(canceled.as_str(), 0, 20))
            .unwrap();
        ledger
            .fail_session_run(&canceled, RUN_CANCELED_REASON, true)
            .unwrap();

        ledger
            .begin_session_run("session_1", &interrupted, "interrupted question", false)
            .unwrap();
        ledger
            .append(
                interrupted.as_str(),
                &started_record(interrupted.as_str(), 0, 30),
            )
            .unwrap();
        drop(ledger);

        let mut ledger = SqliteLedger::open_or_create(&path).unwrap();
        assert_eq!(
            ledger.recover_running_session_runs(RECOVERY_ERROR).unwrap(),
            1
        );
        assert_eq!(
            session_outcome(&ledger, finished.as_str()),
            (RunStateName::Finished, Some("finished answer".into()), None)
        );
        assert_eq!(
            session_outcome(&ledger, failed.as_str()),
            (RunStateName::Failed, None, Some("synthetic failure".into()))
        );
        assert_eq!(
            session_outcome(&ledger, canceled.as_str()),
            (
                RunStateName::Canceled,
                None,
                Some(RUN_CANCELED_REASON.into())
            )
        );
        assert_eq!(
            session_outcome(&ledger, interrupted.as_str()),
            (RunStateName::Interrupted, None, Some(RECOVERY_ERROR.into()))
        );

        for (run_id, expected_reason) in [
            (&failed, "synthetic failure"),
            (&canceled, RUN_CANCELED_REASON),
            (&interrupted, RECOVERY_ERROR),
        ] {
            let records = ledger.read_run(run_id.as_str()).unwrap();
            assert_eq!(
                records
                    .iter()
                    .filter(|record| matches!(record.event, HarnessEvent::RunFailed { .. }))
                    .count(),
                1
            );
            assert!(matches!(
                RunReadback::from_events(&records).unwrap().final_phase,
                RunPhase::Failed { reason } if reason == expected_reason
            ));
        }
        assert!(matches!(
            RunReadback::from_events(&ledger.read_run(finished.as_str()).unwrap())
                .unwrap()
                .final_phase,
            RunPhase::Finished
        ));
        assert_eq!(
            ledger.session_turns("session_1").unwrap(),
            vec![SessionTurn {
                question: "finished question".into(),
                final_answer: "finished answer".into(),
            }]
        );

        let interrupted_records = ledger.read_run(interrupted.as_str()).unwrap();
        assert_eq!(
            ledger.recover_running_session_runs(RECOVERY_ERROR).unwrap(),
            0
        );
        assert_eq!(
            ledger.read_run(interrupted.as_str()).unwrap(),
            interrupted_records
        );

        let follow_up = RunId::new("run_follow_up").unwrap();
        assert_eq!(
            ledger
                .begin_session_run("session_1", &follow_up, "follow up", false)
                .unwrap(),
            vec![SessionTurn {
                question: "finished question".into(),
                final_answer: "finished answer".into(),
            }]
        );
        ledger
            .append(
                follow_up.as_str(),
                &started_record(follow_up.as_str(), 0, 40),
            )
            .unwrap();
        ledger
            .fail_session_run(&follow_up, "follow up stopped", false)
            .unwrap();
    }

    #[test]
    fn sqlite_recovery_reconciles_existing_terminal_events_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.db");
        let mut ledger = SqliteLedger::open_or_create(&path).unwrap();
        let finished = RunId::new("run_existing_finished").unwrap();
        let failed = RunId::new("run_existing_failed").unwrap();
        let canceled = RunId::new("run_existing_canceled").unwrap();

        ledger
            .begin_session_run("session_finished", &finished, "question", true)
            .unwrap();
        append_response_prefix(&mut ledger, &finished, "durable answer", 0);
        ledger
            .append(
                finished.as_str(),
                &RecordedEvent {
                    seq: 4,
                    occurred_at_ms: 4,
                    event: HarnessEvent::RunFinished {
                        run_id: finished.clone(),
                    },
                },
            )
            .unwrap();

        for (session_id, run_id, reason) in [
            ("session_failed", &failed, "durable failure"),
            ("session_canceled", &canceled, RUN_CANCELED_REASON),
        ] {
            ledger
                .begin_session_run(session_id, run_id, "question", true)
                .unwrap();
            ledger
                .append(run_id.as_str(), &started_record(run_id.as_str(), 0, 10))
                .unwrap();
            ledger
                .append(
                    run_id.as_str(),
                    &RecordedEvent {
                        seq: 1,
                        occurred_at_ms: 11,
                        event: HarnessEvent::RunFailed {
                            run_id: run_id.clone(),
                            reason: reason.into(),
                        },
                    },
                )
                .unwrap();
        }
        drop(ledger);

        let mut ledger = SqliteLedger::open_or_create(&path).unwrap();
        assert_eq!(
            ledger
                .recover_running_session_runs("unused recovery error")
                .unwrap(),
            3
        );
        assert_eq!(
            session_outcome(&ledger, finished.as_str()),
            (RunStateName::Finished, Some("durable answer".into()), None)
        );
        assert_eq!(
            session_outcome(&ledger, failed.as_str()),
            (RunStateName::Failed, None, Some("durable failure".into()))
        );
        assert_eq!(
            session_outcome(&ledger, canceled.as_str()),
            (
                RunStateName::Canceled,
                None,
                Some(RUN_CANCELED_REASON.into())
            )
        );
        for run_id in [&finished, &failed, &canceled] {
            let records = ledger.read_run(run_id.as_str()).unwrap();
            assert_eq!(
                records
                    .iter()
                    .filter(|record| matches!(
                        record.event,
                        HarnessEvent::RunFinished { .. } | HarnessEvent::RunFailed { .. }
                    ))
                    .count(),
                1
            );
        }
        assert_eq!(
            ledger
                .recover_running_session_runs("unused recovery error")
                .unwrap(),
            0
        );
    }

    #[test]
    fn sqlite_terminal_statement_faults_roll_back_both_truths_before_recovery() {
        const RECOVERY_ERROR: &str = "recovered after injected terminal fault";
        let boundaries = [
            TerminalFaultBoundary::BeforeEventInsert,
            TerminalFaultBoundary::AfterEventInsert,
            TerminalFaultBoundary::BeforeOutcomeUpdate,
            TerminalFaultBoundary::AfterOutcomeUpdate,
            TerminalFaultBoundary::BeforeSessionTouch,
            TerminalFaultBoundary::AfterSessionTouch,
        ];

        for (boundary_index, boundary) in boundaries.into_iter().enumerate() {
            for (outcome_index, outcome) in
                ["finished", "failed", "canceled"].into_iter().enumerate()
            {
                let dir = tempfile::tempdir().unwrap();
                let path = dir
                    .path()
                    .join(format!("terminal-{boundary_index}-{outcome_index}.db"));
                let mut ledger = SqliteLedger::open_or_create(&path).unwrap();
                let run_id = RunId::new(format!("run_{boundary_index}_{outcome_index}")).unwrap();
                ledger
                    .begin_session_run("session_1", &run_id, "question", true)
                    .unwrap();
                let prefix_len = if outcome == "finished" {
                    append_response_prefix(&mut ledger, &run_id, "answer", 0);
                    4
                } else {
                    ledger
                        .append(run_id.as_str(), &started_record(run_id.as_str(), 0, 0))
                        .unwrap();
                    1
                };
                ledger.inject_terminal_fault_at(boundary);

                let result = match outcome {
                    "finished" => ledger.finish_session_run(&run_id, "answer"),
                    "failed" => ledger.fail_session_run(&run_id, "terminal failure", false),
                    "canceled" => ledger.fail_session_run(&run_id, RUN_CANCELED_REASON, true),
                    _ => unreachable!(),
                };
                assert!(
                    result.is_err(),
                    "{outcome} unexpectedly crossed {boundary:?}"
                );
                assert_eq!(ledger.read_run(run_id.as_str()).unwrap().len(), prefix_len);
                assert_eq!(
                    session_outcome(&ledger, run_id.as_str()),
                    (RunStateName::Running, None, None)
                );
                drop(ledger);

                let mut ledger = SqliteLedger::open_or_create(&path).unwrap();
                assert_eq!(
                    ledger.recover_running_session_runs(RECOVERY_ERROR).unwrap(),
                    1
                );
                let records = ledger.read_run(run_id.as_str()).unwrap();
                assert_eq!(records.len(), prefix_len + 1);
                assert_eq!(
                    records
                        .iter()
                        .filter(|record| matches!(
                            record.event,
                            HarnessEvent::RunFinished { .. } | HarnessEvent::RunFailed { .. }
                        ))
                        .count(),
                    1
                );
                assert!(matches!(
                    RunReadback::from_events(&records).unwrap().final_phase,
                    RunPhase::Failed { reason } if reason == RECOVERY_ERROR
                ));
                assert_eq!(
                    session_outcome(&ledger, run_id.as_str()),
                    (RunStateName::Interrupted, None, Some(RECOVERY_ERROR.into()))
                );
                assert_eq!(
                    ledger.recover_running_session_runs(RECOVERY_ERROR).unwrap(),
                    0
                );
                assert_eq!(ledger.read_run(run_id.as_str()).unwrap(), records);
            }
        }
    }

    #[test]
    fn sqlite_session_begin_rejects_active_run() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.db");
        let mut ledger = SqliteLedger::open_or_create(&path).unwrap();

        ledger
            .begin_session_run(
                "session_1",
                &RunId::new("run_active").unwrap(),
                "hello",
                true,
            )
            .unwrap();
        let error = ledger
            .begin_session_run(
                "session_1",
                &RunId::new("run_next").unwrap(),
                "again",
                false,
            )
            .unwrap_err();

        assert!(matches!(
            error,
            AppError::SessionActive {
                session_id,
                run_id
            } if session_id == "session_1" && run_id == "run_active"
        ));
    }

    #[test]
    fn jsonl_and_sqlite_reconstruct_same_record() {
        let dir = tempfile::tempdir().unwrap();
        let jsonl_path = dir.path().join("events.jsonl");
        let sqlite_path = dir.path().join("events.db");
        let run_id = RunId::new("run_1").unwrap();
        let mut jsonl = JsonlEventRecorder::create(&jsonl_path).unwrap();
        let mut sqlite = SqliteEventRecorder::create(&sqlite_path, &run_id).unwrap();
        let event = HarnessEvent::RunStarted {
            run_id,
            agent_id: AgentId::new("plato").unwrap(),
        };

        let jsonl_record = jsonl.record(event.clone()).unwrap();
        let sqlite_record = sqlite.record(event).unwrap();

        assert_eq!(jsonl_record.seq, sqlite_record.seq);
        assert_eq!(
            read_records(&jsonl_path).unwrap()[0].event,
            sqlite_record.event
        );
        assert_eq!(
            read_sqlite_records(&sqlite_path, Some("run_1")).unwrap()[0].event,
            jsonl_record.event
        );
    }

    fn started_record(run_id: &str, seq: u64, occurred_at_ms: u64) -> RecordedEvent {
        RecordedEvent {
            seq,
            occurred_at_ms,
            event: HarnessEvent::RunStarted {
                run_id: RunId::new(run_id).unwrap(),
                agent_id: AgentId::new("plato").unwrap(),
            },
        }
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

    fn session_outcome(
        ledger: &SqliteLedger,
        run_id: &str,
    ) -> (RunStateName, Option<String>, Option<String>) {
        ledger
            .connection
            .query_row(
                "SELECT status, final_answer, error
                 FROM session_runs
                 WHERE run_id = ?1",
                params![run_id],
                |row| {
                    Ok((
                        status_from_row(row, 0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .unwrap()
    }

    fn append_response_prefix(
        ledger: &mut SqliteLedger,
        run_id: &RunId,
        answer: &str,
        occurred_at_ms: u64,
    ) {
        for (seq, event) in response_run_prefix(run_id, answer, None)
            .into_iter()
            .enumerate()
        {
            ledger
                .append(
                    run_id.as_str(),
                    &RecordedEvent {
                        seq: seq as u64,
                        occurred_at_ms: occurred_at_ms + seq as u64,
                        event,
                    },
                )
                .unwrap();
        }
    }

    fn response_run_prefix(
        run_id: &RunId,
        answer: &str,
        usage: Option<ModelUsage>,
    ) -> Vec<HarnessEvent> {
        let turn_id = TurnId::new("turn_1").unwrap();
        vec![
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
                turn_id,
                step: 0,
                output: Message {
                    role: MessageRole::Assistant,
                    content: answer.into(),
                },
                proposed_calls: vec![],
                served_model: None,
                usage,
            },
        ]
    }

    fn response_run_events(run_id: &RunId, usage: Option<ModelUsage>) -> Vec<HarnessEvent> {
        let mut events = response_run_prefix(run_id, "answer", usage);
        events.push(HarnessEvent::RunFinished {
            run_id: run_id.clone(),
        });
        events
    }
}
