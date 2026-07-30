use crate::{AppError, AppResult, daemon::protocol::RunStateName, paths::DefaultSqlitePath};
use platonic_core::{HarnessEvent, RecordedEvent, RunId, RunState};
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
const SQLITE_SCHEMA_VERSION: u32 = 2;
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

    pub fn record(&mut self, event: HarnessEvent) -> AppResult<RecordedEvent> {
        match self {
            Self::Jsonl(recorder) => recorder.record(event),
            Self::Sqlite(recorder) => recorder.record(event),
        }
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
    run_id: String,
    state: RunState,
}

impl SqliteEventRecorder {
    pub fn create(path: &Path, run_id: &RunId) -> AppResult<Self> {
        Ok(Self {
            ledger: SqliteLedger::open_or_create(path)?,
            run_id: run_id.to_string(),
            state: RunState::new(),
        })
    }

    pub fn create_default(path: &DefaultSqlitePath, run_id: &RunId) -> AppResult<Self> {
        Ok(Self {
            ledger: SqliteLedger::open_or_create_default(path)?,
            run_id: run_id.to_string(),
            state: RunState::new(),
        })
    }

    pub fn record(&mut self, event: HarnessEvent) -> AppResult<RecordedEvent> {
        let record = next_record(&mut self.state, event)?;
        self.ledger.append(&self.run_id, &record)?;
        Ok(record)
    }
}

pub struct SqliteLedger {
    connection: Connection,
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
        Ok(Self { connection })
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
        Ok(Self { connection })
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
        let event_json = serde_json::to_string(&record.event)?;
        let seq = sqlite_i64(record.seq, "seq")?;
        let occurred_at_ms = sqlite_i64(record.occurred_at_ms, "occurred_at_ms")?;
        let inserted = self.connection.execute(
            "INSERT OR IGNORE INTO ledger_events (run_id, seq, occurred_at_ms, v, event_json)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![run_id, seq, occurred_at_ms, LEDGER_VERSION, event_json],
        )?;
        if inserted == 1 {
            return Ok(());
        }

        let existing = self.connection.query_row(
            "SELECT occurred_at_ms, v, event_json FROM ledger_events WHERE run_id = ?1 AND seq = ?2",
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

    pub fn read_run(&self, run_id: &str) -> AppResult<Vec<RecordedEvent>> {
        read_run_from(&self.connection, run_id)
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
        self.update_session_run(run_id, RunStateName::Finished, Some(final_answer), None)
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
        self.update_session_run(run_id, status, None, Some(error))
    }

    pub fn interrupt_running_session_runs(&mut self, error: &str) -> AppResult<usize> {
        let now = sqlite_i64(now_ms(), "occurred_at_ms")?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let session_ids = {
            let mut statement = transaction.prepare(
                "SELECT DISTINCT session_id
                 FROM session_runs
                 WHERE status = ?1",
            )?;
            statement
                .query_map(params![RunStateName::Running.as_str()], |row| {
                    row.get::<_, String>(0)
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        let updated = transaction.execute(
            "UPDATE session_runs
             SET status = ?2, error = ?3, updated_at_ms = ?4
             WHERE status = ?1",
            params![
                RunStateName::Running.as_str(),
                RunStateName::Interrupted.as_str(),
                error,
                now
            ],
        )?;
        for session_id in session_ids {
            touch_session(&transaction, &session_id, now)?;
        }
        transaction.commit()?;
        Ok(updated)
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
            "SELECT s.session_id, sr.run_id, sr.status, sr.question
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
                })
            })?
            .collect::<Result<Vec<_>, _>>()?)
    }

    fn session_exists(&self, session_id: &str) -> AppResult<bool> {
        session_exists_in(&self.connection, session_id)
    }

    fn update_session_run(
        &mut self,
        run_id: &RunId,
        status: RunStateName,
        final_answer: Option<&str>,
        error: Option<&str>,
    ) -> AppResult<()> {
        let now = sqlite_i64(now_ms(), "occurred_at_ms")?;
        let transaction = self.connection.transaction()?;
        let updated = transaction.execute(
            "UPDATE session_runs
             SET status = ?2, final_answer = ?3, error = ?4, updated_at_ms = ?5
             WHERE run_id = ?1",
            params![
                run_id.to_string(),
                status.as_str(),
                final_answer,
                error,
                now
            ],
        )?;
        if updated == 0 {
            return Err(AppError::RunNotFound(run_id.to_string()));
        }
        let session_id: String = transaction.query_row(
            "SELECT session_id FROM session_runs WHERE run_id = ?1",
            params![run_id.to_string()],
            |row| row.get(0),
        )?;
        touch_session(&transaction, &session_id, now)?;
        transaction.commit()?;
        Ok(())
    }

    #[cfg(test)]
    fn user_version(&self) -> AppResult<u32> {
        let version: u32 = self
            .connection
            .pragma_query_value(None, "user_version", |row| row.get(0))?;
        Ok(version)
    }
}

fn read_run_from(connection: &Connection, run_id: &str) -> AppResult<Vec<RecordedEvent>> {
    let mut statement = connection.prepare(
        "SELECT seq, occurred_at_ms, v, event_json
             FROM ledger_events
             WHERE run_id = ?1
             ORDER BY seq ASC",
    )?;
    let records = statement
        .query_map(params![run_id], sqlite_record_from_row)?
        .collect::<Result<Vec<_>, _>>()?;
    if records.is_empty() {
        Err(AppError::RunNotFound(run_id.into()))
    } else {
        Ok(records)
    }
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

fn sqlite_record_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RecordedEvent> {
    let version: u32 = row.get(2)?;
    if !supported_ledger_version(version) {
        return Err(rusqlite::Error::InvalidQuery);
    }
    let event_json: String = row.get(3)?;
    let event = serde_json::from_str(&event_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(3, Type::Text, Box::new(error))
    })?;
    Ok(RecordedEvent {
        seq: row_u64(row, 0, "seq")?,
        occurred_at_ms: row_u64(row, 1, "occurred_at_ms")?,
        event,
    })
}

fn sqlite_i64(value: u64, field: &str) -> AppResult<i64> {
    value
        .try_into()
        .map_err(|_| AppError::Config(format!("ledger {field} exceeds sqlite integer: {value}")))
}

fn row_u64(row: &rusqlite::Row<'_>, index: usize, field: &str) -> rusqlite::Result<u64> {
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
    configure_sqlite_connection(&connection)?;
    if create {
        migrate_sqlite(&mut connection)?;
    }
    restrict_existing_sidecars(location.as_path())?;
    verify_open_file(
        location.as_path(),
        &database,
        PRIVATE_FILE_MODE,
        current_uid(),
    )?;
    Ok(SqliteLedger { connection })
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
    if location.as_path().file_name() != Some(std::ffi::OsStr::new("agent.db"))
        || workspaces_directory.file_name() != Some(std::ffi::OsStr::new("workspaces"))
        || state_root.file_name() != Some(std::ffi::OsStr::new("plato-agent"))
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
    Ok(())
}

fn migrate_sqlite(connection: &mut Connection) -> AppResult<()> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let version: u32 = transaction.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version > SQLITE_SCHEMA_VERSION {
        return Err(AppError::Config(format!(
            "unsupported sqlite schema version: {version}"
        )));
    }
    if version < 1 {
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
    if version < 2 {
        create_session_tables(&transaction)?;
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

pub fn interrupt_orphaned_sqlite_runs(path: &Path) -> AppResult<usize> {
    if !path.exists() {
        return Ok(0);
    }
    SqliteLedger::open_or_create(path)?
        .interrupt_running_session_runs("daemon restarted before run completed")
}

pub fn interrupt_orphaned_default_sqlite_runs(path: &DefaultSqlitePath) -> AppResult<usize> {
    if fs::symlink_metadata(path.as_path())
        .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
    {
        return Ok(0);
    }
    SqliteLedger::open_or_create_default(path)?
        .interrupt_running_session_runs("daemon restarted before run completed")
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
        AgentId, ContextPack, HarnessEvent, Message, MessageRole, ModelName, ModelUsage, RunId,
        TurnId,
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
                .join("plato-agent")
                .join("workspaces")
                .join("workspace-1")
                .join("agent.db"),
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

        let mut journal = location.as_path().as_os_str().to_os_string();
        journal.push("-journal");
        let journal = PathBuf::from(journal);
        assert!(journal.is_file());
        assert_eq!(mode(&journal), PRIVATE_FILE_MODE);
        ledger.connection.execute_batch("ROLLBACK").unwrap();

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
            ledger
                .append(run_id, &started_record(run_id, 0, occurred_at_ms))
                .unwrap();
            ledger
                .finish_session_run(&RunId::new(run_id).unwrap(), answer)
                .unwrap();
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
            assert_eq!(session.runs[0].records.len(), 1);
            assert_eq!(session.runs[0].records[0].event.run_id().as_str(), run_id);
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
        ledger
            .append("run_1", &started_record("run_1", 0, 10))
            .unwrap();
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
    fn sqlite_session_summaries_report_latest_session_run() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.db");
        let mut ledger = SqliteLedger::open_or_create(&path).unwrap();
        let first_run = RunId::new("run_1").unwrap();
        let second_run = RunId::new("run_2").unwrap();

        ledger
            .begin_session_run("session_1", &first_run, "first question", true)
            .unwrap();
        ledger
            .finish_session_run(&first_run, "first answer")
            .unwrap();
        ledger
            .begin_session_run("session_1", &second_run, "second question", false)
            .unwrap();

        assert_eq!(
            ledger.session_summaries().unwrap(),
            vec![PersistedSessionSummary {
                session_id: "session_1".into(),
                run_id: "run_2".into(),
                status: RunStateName::Running,
                latest_question: "second question".into(),
            }]
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
    fn sqlite_interrupts_running_session_runs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.db");
        let mut ledger = SqliteLedger::open_or_create(&path).unwrap();
        let run_id = RunId::new("run_1").unwrap();

        ledger
            .begin_session_run("session_1", &run_id, "first question", true)
            .unwrap();

        assert_eq!(
            ledger
                .interrupt_running_session_runs("daemon restarted")
                .unwrap(),
            1
        );
        assert_eq!(
            ledger.session_summaries().unwrap()[0].status,
            RunStateName::Interrupted
        );
        ledger
            .begin_session_run(
                "session_1",
                &RunId::new("run_2").unwrap(),
                "follow up",
                false,
            )
            .unwrap();
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

    fn response_run_events(run_id: &RunId, usage: Option<ModelUsage>) -> Vec<HarnessEvent> {
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
                    content: "answer".into(),
                },
                proposed_calls: vec![],
                usage,
            },
            HarnessEvent::RunFinished {
                run_id: run_id.clone(),
            },
        ]
    }
}
