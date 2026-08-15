#[cfg(unix)]
use super::unix::*;
use super::{
    jsonl::{JsonlEventRecorder, append_voice_events_to_jsonl, read_records, run_jsonl_path},
    recorder::{RUN_CANCELED_REASON, next_record, now_ms},
    replay::{
        first_voice_difference, read_run_records_from, read_voice_events_from, replay_records,
        validate_voice_event_keys, validate_voice_event_stream,
    },
    types::{LEDGER_VERSION, SessionTurn, validate_ledger_identity},
};
use crate::{AppError, AppResult, paths::DefaultSqlitePath};
use platonic_core::{AgentId, HarnessEvent, MessageRole, RecordedEvent, RunId, RunPhase, RunState};
use platonic_protocol::{RunStateName, VoiceEvent, VoiceEventEnvelope};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params, types::Type};
use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const LEGACY_SQLITE_SCHEMA_VERSION: u32 = 1;
const SESSION_SQLITE_SCHEMA_VERSION: u32 = 2;
pub(super) const VOICE_EVENT_SQLITE_SCHEMA_VERSION: u32 = 3;
// Versions 4 and 5 added thread authority and thread stop tables here. Those
// tables now live in the server-wide store, because a thread must be
// enumerable from outside the workspace it runs in (D005). The numbers stay
// spent so an existing workspace ledger still opens at its recorded version.
const THREAD_STOP_SQLITE_SCHEMA_VERSION: u32 = 5;
pub(super) const SQLITE_SCHEMA_VERSION: u32 = THREAD_STOP_SQLITE_SCHEMA_VERSION;
const ORPHANED_RUN_ERROR: &str = "daemon restarted before run completed";

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

    pub(super) fn from_session(ledger: SqliteLedger, run_id: &RunId) -> Self {
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

    pub(super) fn finish_run(&mut self, final_answer: &str) -> AppResult<RecordedEvent> {
        self.record_terminal(
            HarnessEvent::RunFinished {
                run_id: self.run_id.clone(),
            },
            RunStateName::Finished,
            Some(final_answer),
            None,
        )
    }

    pub(super) fn fail_run(&mut self, error: &str, canceled: bool) -> AppResult<RecordedEvent> {
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
    pub(super) connection: Connection,
    pub(super) path: PathBuf,
    pub(super) schema_version: u32,
    #[cfg(test)]
    pub(super) terminal_fault: Option<TerminalFaultBoundary>,
    #[cfg(test)]
    pub(super) voice_fault: Option<VoiceFaultBoundary>,
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
            path: path.to_path_buf(),
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
            path: path.to_path_buf(),
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
        let jsonl_path = run_jsonl_path(&self.path, &run_id)?;
        if jsonl_path.exists() {
            let records = self.read_run(&run_id)?;
            validate_voice_event_keys(&records, &run_id, &envelopes)?;
            return append_voice_events_to_jsonl(&jsonl_path, &run_id, &envelopes);
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let records = read_run_records_from(&transaction, &run_id)?;
        validate_voice_event_keys(&records, &run_id, &envelopes)?;
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

    pub(crate) fn is_legacy_schema(&self) -> bool {
        self.schema_version == LEGACY_SQLITE_SCHEMA_VERSION
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

    pub(super) fn discard_running_session_run(
        &mut self,
        run_id: &RunId,
        created_session: bool,
    ) -> AppResult<()> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let session_id = transaction
            .query_row(
                "SELECT session_id
                 FROM session_runs
                 WHERE run_id = ?1 AND status = ?2",
                params![run_id.as_str(), RunStateName::Running.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .ok_or_else(|| {
                AppError::Config(format!(
                    "running session admission is unavailable for cleanup: {run_id}"
                ))
            })?;
        transaction.execute(
            "DELETE FROM session_runs WHERE run_id = ?1 AND status = ?2",
            params![run_id.as_str(), RunStateName::Running.as_str()],
        )?;
        if created_session {
            transaction.execute(
                "DELETE FROM sessions
                 WHERE session_id = ?1
                   AND NOT EXISTS (
                     SELECT 1 FROM session_runs WHERE session_id = ?1
                   )",
                params![session_id],
            )?;
        }
        transaction.commit()?;
        Ok(())
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
        let running_runs = {
            let mut statement = self.connection.prepare(
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
            let jsonl_path = run_jsonl_path(&self.path, run_id)?;
            if jsonl_path.exists() {
                self.recover_jsonl_session_run(&jsonl_path, run_id, error)?;
            } else {
                self.recover_sqlite_session_run(session_id, run_id, error)?;
            }
        }
        Ok(running_runs.len())
    }

    fn recover_jsonl_session_run(
        &mut self,
        path: &Path,
        run_id: &str,
        error: &str,
    ) -> AppResult<()> {
        let run_id = RunId::new(run_id.to_owned())?;
        let mut recorder = JsonlEventRecorder::open(path)?;
        let mut records = read_records(path)?;
        if records.is_empty() {
            recorder.record(HarnessEvent::RunStarted(platonic_core::RunStartedEvent {
                run_id: run_id.clone(),
                identity: platonic_core::RunIdentity::LegacyAgent {
                    agent_id: AgentId::new("plato")?,
                },
            }))?;
            records = read_records(path)?;
        }
        let state = replay_records(&records)?;
        let (status, final_answer, stored_error) = match state.phase() {
            RunPhase::Finished => (
                RunStateName::Finished,
                Some(final_answer_from_records(&run_id, &records)?.to_owned()),
                None,
            ),
            RunPhase::Failed { reason } => (failure_status(reason), None, Some(reason.to_owned())),
            _ => {
                recorder.record(HarnessEvent::RunFailed {
                    run_id: run_id.clone(),
                    reason: error.into(),
                })?;
                (RunStateName::Interrupted, None, Some(error.to_owned()))
            }
        };
        self.complete_session_terminal(
            &run_id,
            status,
            final_answer.as_deref(),
            stored_error.as_deref(),
        )
    }

    fn recover_sqlite_session_run(
        &mut self,
        session_id: &str,
        run_id: &str,
        error: &str,
    ) -> AppResult<()> {
        let now = sqlite_i64(now_ms(), "occurred_at_ms")?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
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
            RunPhase::Failed { reason } => (failure_status(reason), None, Some(reason.as_str())),
            _ => {
                let record = next_record(
                    &mut state,
                    HarnessEvent::RunFailed {
                        run_id: RunId::new(run_id.to_owned())?,
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
        transaction.commit()?;
        Ok(())
    }

    pub(super) fn session_exists(&self, session_id: &str) -> AppResult<bool> {
        session_exists_in(&self.connection, session_id)
    }

    fn replay_run_state(&self, run_id: &RunId) -> AppResult<(Vec<RecordedEvent>, RunState)> {
        let records = self.read_run(run_id.as_str())?;
        let state = replay_records(&records)?;
        Ok((records, state))
    }

    pub(super) fn commit_session_terminal(
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

    pub(super) fn complete_session_terminal(
        &mut self,
        run_id: &RunId,
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
        update_running_session_outcome(
            &transaction,
            run_id.as_str(),
            status,
            final_answer,
            error,
            now,
        )?;
        touch_session(&transaction, &session_id, now)?;
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
    pub(super) fn user_version(&self) -> AppResult<u32> {
        let version: u32 = self
            .connection
            .pragma_query_value(None, "user_version", |row| row.get(0))?;
        Ok(version)
    }
}

fn append_record_in(
    connection: &Connection,
    run_id: &str,
    record: &RecordedEvent,
) -> AppResult<()> {
    validate_ledger_identity(LEDGER_VERSION, &record.event)?;
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
pub(super) enum TerminalFaultBoundary {
    BeforeEventInsert,
    AfterEventInsert,
    BeforeOutcomeUpdate,
    AfterOutcomeUpdate,
    BeforeSessionTouch,
    AfterSessionTouch,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum VoiceFaultBoundary {
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

struct ExistingEvent {
    occurred_at_ms: u64,
    version: u32,
    event_json: String,
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

pub(super) fn session_exists_in(connection: &Connection, session_id: &str) -> AppResult<bool> {
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

pub(super) fn configure_sqlite_connection(connection: &Connection) -> AppResult<()> {
    connection.busy_timeout(SQLITE_BUSY_TIMEOUT)?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    Ok(())
}

pub(super) fn configure_sqlite_journal_mode(connection: &Connection) -> AppResult<()> {
    let journal_mode: String =
        connection.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
    if journal_mode != "wal" {
        connection.pragma_update(None, "journal_mode", "WAL")?;
    }
    Ok(())
}

pub(super) fn read_sqlite_schema_version(connection: &Connection) -> AppResult<u32> {
    let actual: u32 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if actual > SQLITE_SCHEMA_VERSION {
        return Err(AppError::SqliteSchemaVersion {
            expected: SQLITE_SCHEMA_VERSION,
            actual,
        });
    }
    Ok(actual)
}

pub(super) fn migrate_sqlite(connection: &mut Connection) -> AppResult<()> {
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

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::ledger::read_sqlite_records;
    use crate::ledger::{
        jsonl::{PersistedLedgerLine, read_ledger_lines},
        replay::status_from_row,
    };
    use platonic_core::{
        AgentId, ContextPack, HarnessEvent, Message, MessageRole, ModelName, ModelUsage, RunId,
        RunReadback, TurnId,
    };
    use serde_json::Value;
    use std::{
        sync::atomic::{AtomicBool, Ordering},
        thread,
        time::Instant,
    };

    #[cfg(unix)]
    pub(in crate::ledger) fn default_location(root: &Path) -> DefaultSqlitePath {
        DefaultSqlitePath::from_path(
            root.join("state")
                .join("platonic")
                .join("workspaces")
                .join("workspace-1")
                .join("ledger.db"),
        )
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
    fn new_run_voice_facts_share_jsonl_and_leave_sqlite_log_tables_empty() {
        let dir = tempfile::tempdir().unwrap();
        let sqlite_path = dir.path().join("ledger.db");
        let run_id = RunId::new("run_voice").unwrap();
        let jsonl_path = run_jsonl_path(&sqlite_path, run_id.as_str()).unwrap();
        fs::create_dir_all(jsonl_path.parent().unwrap()).unwrap();
        let mut recorder = JsonlEventRecorder::create(&jsonl_path).unwrap();
        for event in response_run_events(&run_id, None) {
            recorder.record(event).unwrap();
        }
        drop(recorder);
        let core_records = read_records(&jsonl_path).unwrap();
        let mut ledger = SqliteLedger::open_or_create(&sqlite_path).unwrap();
        let events = completed_voice_events(run_id.as_str());

        let committed = ledger.append_voice_events(&events).unwrap();

        assert_eq!(
            ledger.read_voice_events(run_id.as_str()).unwrap(),
            committed
        );
        assert_eq!(read_records(&jsonl_path).unwrap(), core_records);
        let lines = read_ledger_lines(&jsonl_path).unwrap();
        assert_eq!(
            lines
                .iter()
                .filter(|line| matches!(line, PersistedLedgerLine::Voice(_)))
                .count(),
            1
        );
        for table in ["ledger_events", "voice_events"] {
            let count: i64 = ledger
                .connection
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 0, "new run wrote {table}");
        }
        let replay = crate::replay::replay_file(&jsonl_path).unwrap();
        assert_eq!(replay.matches("voice_event:").count(), committed.len());
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

    pub(in crate::ledger) fn started_record(
        run_id: &str,
        seq: u64,
        occurred_at_ms: u64,
    ) -> RecordedEvent {
        RecordedEvent {
            seq,
            occurred_at_ms,
            event: HarnessEvent::RunStarted(platonic_core::RunStartedEvent {
                run_id: RunId::new(run_id).unwrap(),
                identity: platonic_core::RunIdentity::LegacyAgent {
                    agent_id: AgentId::new("plato").unwrap(),
                },
            }),
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

    pub(in crate::ledger) fn append_response_prefix(
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

    pub(in crate::ledger) fn response_run_prefix(
        run_id: &RunId,
        answer: &str,
        usage: Option<ModelUsage>,
    ) -> Vec<HarnessEvent> {
        let turn_id = TurnId::new("turn_1").unwrap();
        vec![
            HarnessEvent::RunStarted(platonic_core::RunStartedEvent {
                run_id: run_id.clone(),
                identity: platonic_core::RunIdentity::LegacyAgent {
                    agent_id: AgentId::new("plato").unwrap(),
                },
            }),
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

    pub(in crate::ledger) fn response_run_events(
        run_id: &RunId,
        usage: Option<ModelUsage>,
    ) -> Vec<HarnessEvent> {
        let mut events = response_run_prefix(run_id, "answer", usage);
        events.push(HarnessEvent::RunFinished {
            run_id: run_id.clone(),
        });
        events
    }
}
