#[cfg(unix)]
use super::unix::{
    PRIVATE_FILE_MODE, current_uid, prepare_private_directories, restrict_private_directory,
    verify_open_file,
};
use super::{
    recorder::next_record,
    replay::{
        first_voice_difference, replay_records, validate_record_run_ids, validate_voice_event_keys,
        validate_voice_event_stream,
    },
    sqlite::SqliteLedger,
    types::{LEDGER_VERSION, LedgerLine, supported_ledger_version},
};
use crate::{AppError, AppResult, paths::DefaultSqlitePath};
use platonic_core::{HarnessEvent, RecordedEvent, RunId, RunState};
use platonic_protocol::{RunStateName, VOICE_EVENT_VERSION, VoiceEventEnvelope};
use serde::{Deserialize, Serialize};
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct VoiceLedgerLine {
    voice_events: Vec<VoiceEventEnvelope>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(super) enum PersistedLedgerLine {
    Record(LedgerLine),
    Voice(VoiceLedgerLine),
}

pub struct JsonlEventRecorder {
    file: File,
    state: RunState,
    session: Option<JsonlSession>,
    poisoned: bool,
}

struct JsonlSession {
    ledger: SqliteLedger,
    run_id: RunId,
    created_session: bool,
    open: bool,
    terminal_attempted: bool,
}

impl JsonlEventRecorder {
    pub fn create(path: &Path) -> AppResult<Self> {
        if path.as_os_str().is_empty() {
            return Err(AppError::EmptyLedger);
        }

        let file = create_jsonl_file(path, false)?;
        Ok(Self {
            file,
            state: RunState::new(),
            session: None,
            poisoned: false,
        })
    }

    pub(super) fn create_private(path: &Path) -> AppResult<Self> {
        if path.as_os_str().is_empty() {
            return Err(AppError::EmptyLedger);
        }
        let file = create_jsonl_file(path, true)?;
        Ok(Self {
            file,
            state: RunState::new(),
            session: None,
            poisoned: false,
        })
    }

    pub(super) fn open(path: &Path) -> AppResult<Self> {
        if path.as_os_str().is_empty() {
            return Err(AppError::EmptyLedger);
        }
        let (file, state) = open_jsonl_for_append(path)?;
        Ok(Self {
            file,
            state,
            session: None,
            poisoned: false,
        })
    }

    pub(super) fn with_session(
        mut self,
        ledger: SqliteLedger,
        run_id: &RunId,
        created_session: bool,
    ) -> Self {
        self.session = Some(JsonlSession {
            ledger,
            run_id: run_id.clone(),
            created_session,
            open: true,
            terminal_attempted: false,
        });
        self
    }

    pub(super) fn discard_empty_session_admission(mut self) -> AppResult<()> {
        if self.file.metadata()?.len() != 0 {
            return Err(AppError::Config(
                "cannot discard a JSONL session admission after recording events".into(),
            ));
        }
        let mut session = self.session.take().ok_or_else(|| {
            AppError::Config("cannot discard a JSONL recorder without session admission".into())
        })?;
        if !session.open || session.terminal_attempted {
            return Err(AppError::Config(
                "cannot discard a closed JSONL session admission".into(),
            ));
        }
        let log_path = run_jsonl_path(&session.ledger.path, session.run_id.as_str())?;
        let row_cleanup = session
            .ledger
            .discard_running_session_run(&session.run_id, session.created_session);
        drop(self);
        let log_cleanup = fs::remove_file(&log_path).map_err(AppError::Io);
        match (row_cleanup, log_cleanup) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(row), Ok(())) => Err(row),
            (Ok(()), Err(log)) => Err(log),
            (Err(row), Err(log)) => Err(AppError::Config(format!(
                "{row}; failed to remove uncommitted run log {}: {log}",
                log_path.display()
            ))),
        }
    }

    pub fn record(&mut self, event: HarnessEvent) -> AppResult<RecordedEvent> {
        if self.session.as_ref().is_some_and(|session| session.open)
            && matches!(
                event,
                HarnessEvent::RunFinished { .. } | HarnessEvent::RunFailed { .. }
            )
        {
            return Err(AppError::Config(
                "JSONL session terminal events require a matching session outcome".into(),
            ));
        }
        self.record_event(event)
    }

    fn record_event(&mut self, event: HarnessEvent) -> AppResult<RecordedEvent> {
        if self.poisoned {
            return Err(AppError::Config(
                "JSONL recorder is unavailable after a prior write failure".into(),
            ));
        }
        let mut next_state = self.state.clone();
        let record = next_record(&mut next_state, event)?;
        let line = LedgerLine {
            v: LEDGER_VERSION,
            record: record.clone(),
        };
        let mut bytes = serde_json::to_vec(&line)?;
        bytes.push(b'\n');
        let write = self.file.write_all(&bytes).and_then(|()| {
            self.file.flush()?;
            // A record is acknowledged only after its newline commit marker is durable.
            self.file.sync_data()
        });
        if let Err(error) = write {
            self.poisoned = true;
            return Err(AppError::Io(error));
        }
        self.state = next_state;
        Ok(record)
    }

    pub(super) fn finish_run(
        &mut self,
        run_id: &RunId,
        final_answer: &str,
    ) -> AppResult<RecordedEvent> {
        self.record_terminal(
            HarnessEvent::RunFinished {
                run_id: run_id.clone(),
            },
            RunStateName::Finished,
            Some(final_answer),
            None,
        )
    }

    pub(super) fn fail_run(
        &mut self,
        run_id: &RunId,
        error: &str,
        canceled: bool,
    ) -> AppResult<RecordedEvent> {
        let status = if canceled {
            RunStateName::Canceled
        } else {
            RunStateName::Failed
        };
        self.record_terminal(
            HarnessEvent::RunFailed {
                run_id: run_id.clone(),
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
        let Some(session) = self.session.as_mut() else {
            return self.record_event(event);
        };
        if !session.open {
            return Err(AppError::Config(
                "JSONL session run is already closed".into(),
            ));
        }
        session.terminal_attempted = true;
        let run_id = session.run_id.clone();
        let record = self.record_event(event)?;
        let session = self
            .session
            .as_mut()
            .expect("JSONL session remains attached while recording");
        session
            .ledger
            .complete_session_terminal(&run_id, status, final_answer, error)?;
        session.open = false;
        Ok(record)
    }
}

impl Drop for JsonlEventRecorder {
    fn drop(&mut self) {
        let should_fail = self
            .session
            .as_ref()
            .is_some_and(|session| session.open && !session.terminal_attempted)
            && !self.poisoned;
        if should_fail {
            let run_id = self
                .session
                .as_ref()
                .expect("checked attached JSONL session")
                .run_id
                .clone();
            let _ = self.fail_run(&run_id, "run ended before session status was closed", false);
        }
    }
}

pub(crate) fn run_jsonl_path(sqlite_path: &Path, run_id: &str) -> AppResult<PathBuf> {
    let run_id =
        RunId::new(run_id.to_owned()).map_err(|_| AppError::RunNotFound(run_id.to_owned()))?;
    let ledger_directory = sqlite_path.parent().ok_or_else(|| {
        AppError::Config(format!(
            "workspace ledger has no directory: {}",
            sqlite_path.display()
        ))
    })?;
    Ok(ledger_directory
        .join("runs")
        .join(format!("{run_id}.jsonl")))
}

fn create_jsonl_file(path: &Path, private: bool) -> AppResult<File> {
    if private {
        prepare_private_run_directory(path)?;
    }
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options
        .mode(PRIVATE_FILE_MODE)
        .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32);
    let file = options.open(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            AppError::LedgerExists(path.into())
        } else {
            AppError::Io(error)
        }
    })?;
    #[cfg(unix)]
    if private {
        file.set_permissions(fs::Permissions::from_mode(PRIVATE_FILE_MODE))?;
        verify_open_file(path, &file, PRIVATE_FILE_MODE, current_uid())?;
    }
    Ok(file)
}

fn prepare_private_run_directory(path: &Path) -> AppResult<()> {
    let runs_directory = path
        .parent()
        .ok_or_else(|| AppError::Config(format!("run log has no directory: {}", path.display())))?;
    let workspace_directory = runs_directory.parent().ok_or_else(|| {
        AppError::Config(format!(
            "run log has no workspace directory: {}",
            path.display()
        ))
    })?;
    if runs_directory.file_name() != Some(std::ffi::OsStr::new("runs")) {
        return Err(AppError::Config(format!(
            "default run log is outside the runs directory: {}",
            path.display()
        )));
    }
    let location = DefaultSqlitePath::from_path(workspace_directory.join("ledger.db"));
    #[cfg(unix)]
    {
        prepare_private_directories(&location)?;
        restrict_private_directory(runs_directory, current_uid())?;
    }
    #[cfg(not(unix))]
    fs::create_dir_all(runs_directory)?;
    Ok(())
}

fn open_jsonl_for_append(path: &Path) -> AppResult<(File, RunState)> {
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    #[cfg(unix)]
    options.custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32);
    let mut file = options.open(path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    let committed_len = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |index| index + 1);
    let records = parse_ledger_lines(&bytes[..committed_len])?
        .into_iter()
        .filter_map(|line| match line {
            PersistedLedgerLine::Record(line) => Some(line.record),
            PersistedLedgerLine::Voice(_) => None,
        })
        .collect::<Vec<_>>();
    let state = replay_records(&records)?;
    let committed_len = u64::try_from(committed_len)
        .map_err(|_| AppError::Config("JSONL ledger length exceeds u64".into()))?;
    if file.metadata()?.len() != committed_len {
        file.set_len(committed_len)?;
        file.sync_data()?;
    }
    file.seek(SeekFrom::End(0))?;
    Ok((file, state))
}

fn parse_ledger_lines(bytes: &[u8]) -> AppResult<Vec<PersistedLedgerLine>> {
    let mut lines = Vec::new();
    for bytes in bytes.split(|byte| *byte == b'\n') {
        if bytes.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let line = serde_json::from_slice::<PersistedLedgerLine>(bytes)?;
        if let PersistedLedgerLine::Record(line) = &line
            && !supported_ledger_version(line.v)
        {
            return Err(AppError::LedgerVersion {
                expected: LEDGER_VERSION,
                actual: line.v,
            });
        }
        if let PersistedLedgerLine::Voice(line) = &line {
            validate_voice_envelopes(&line.voice_events)?;
        }
        lines.push(line);
    }
    Ok(lines)
}

pub(super) fn read_ledger_lines(path: &Path) -> AppResult<Vec<PersistedLedgerLine>> {
    let bytes = fs::read(path)?;
    let committed_len = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |index| index + 1);
    parse_ledger_lines(&bytes[..committed_len])
}

fn validate_voice_envelopes(envelopes: &[VoiceEventEnvelope]) -> AppResult<()> {
    let events = envelopes
        .iter()
        .enumerate()
        .map(|(index, envelope)| {
            let expected = u64::try_from(index).map_err(|_| {
                AppError::VoiceEventContract("voice event sequence overflowed u64".into())
            })?;
            if envelope.v != VOICE_EVENT_VERSION {
                return Err(AppError::VoiceEventVersion {
                    expected: VOICE_EVENT_VERSION,
                    actual: envelope.v,
                });
            }
            if envelope.sequence != expected {
                return Err(AppError::VoiceEventContract(format!(
                    "voice event sequence was {}, expected {expected}",
                    envelope.sequence
                )));
            }
            envelope
                .event
                .validate()
                .map_err(AppError::VoiceEventContract)?;
            Ok(envelope.event.clone())
        })
        .collect::<AppResult<Vec<_>>>()?;
    validate_voice_event_stream(&events)
}

pub(crate) fn read_voice_events_from_jsonl(
    path: &Path,
    selected_run_id: &str,
) -> AppResult<Vec<VoiceEventEnvelope>> {
    let mut records = Vec::new();
    let mut envelopes = Vec::new();
    let mut batch_seen = false;
    for line in read_ledger_lines(path)? {
        match line {
            PersistedLedgerLine::Record(line) => records.push(line.record),
            PersistedLedgerLine::Voice(line) => {
                if batch_seen {
                    return Err(AppError::VoiceEventContract(format!(
                        "run {selected_run_id} contains multiple voice event batches"
                    )));
                }
                batch_seen = true;
                envelopes = line.voice_events;
            }
        }
    }
    validate_record_run_ids(&records, selected_run_id)?;
    for envelope in &envelopes {
        if envelope.event.run_id().as_str() != selected_run_id {
            return Err(AppError::VoiceEventContract(format!(
                "voice event belongs to {}, expected {selected_run_id}",
                envelope.event.run_id()
            )));
        }
    }
    validate_voice_envelopes(&envelopes)?;
    if !envelopes.is_empty() {
        validate_voice_event_keys(&records, selected_run_id, &envelopes)?;
    }
    Ok(envelopes)
}

pub(super) fn append_voice_events_to_jsonl(
    path: &Path,
    run_id: &str,
    envelopes: &[VoiceEventEnvelope],
) -> AppResult<Vec<VoiceEventEnvelope>> {
    let (mut file, _) = open_jsonl_for_append(path)?;
    let existing = read_voice_events_from_jsonl(path, run_id)?;
    if !existing.is_empty() {
        if existing == envelopes {
            return Ok(envelopes.to_vec());
        }
        return Err(AppError::VoiceLedgerConflict {
            run_id: run_id.into(),
            sequence: first_voice_difference(&existing, envelopes),
        });
    }
    let line = VoiceLedgerLine {
        voice_events: envelopes.to_vec(),
    };
    let mut bytes = serde_json::to_vec(&line)?;
    bytes.push(b'\n');
    file.seek(SeekFrom::End(0))?;
    file.write_all(&bytes)?;
    file.flush()?;
    file.sync_data()?;
    Ok(envelopes.to_vec())
}

pub fn read_records(path: &Path) -> AppResult<Vec<RecordedEvent>> {
    Ok(read_ledger_lines(path)?
        .into_iter()
        .filter_map(|line| match line {
            PersistedLedgerLine::Record(line) => Some(line.record),
            PersistedLedgerLine::Voice(_) => None,
        })
        .collect())
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AppError,
        ledger::{
            EventRecorder, SqliteLedger,
            sqlite::tests::{append_response_prefix, default_location},
        },
    };
    use platonic_core::{AgentId, ContextPack, HarnessEvent, RunId, TurnId};
    use platonic_protocol::{RunStateName, VOICE_EVENT_VERSION, VoiceEvent, VoiceEventEnvelope};
    use rusqlite::params;
    use serde_json::Value;
    use std::{
        io::{BufRead, BufReader},
        process::Stdio,
        sync::mpsc,
        thread,
        time::Duration,
    };
    #[cfg(unix)]
    use std::{os::unix::process::ExitStatusExt, process::Command};

    #[test]
    fn writes_and_reads_versioned_jsonl_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let mut recorder = JsonlEventRecorder::create(&path).unwrap();

        let record = recorder
            .record(HarnessEvent::RunStarted {
                run_id: RunId::new("run_1").unwrap(),
                agent_id: AgentId::new("plato").unwrap(),
            })
            .unwrap();

        let records = read_records(&path).unwrap();
        assert_eq!(records, vec![record.clone()]);
        let mut expected = serde_json::to_vec(&LedgerLine {
            v: LEDGER_VERSION,
            record,
        })
        .unwrap();
        expected.push(b'\n');
        assert_eq!(fs::read(&path).unwrap(), expected);
        let raw: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(raw["v"], LEDGER_VERSION);
    }

    #[test]
    fn jsonl_readers_ignore_a_valid_unterminated_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let run_id = RunId::new("run_tail").unwrap();
        let mut recorder = JsonlEventRecorder::create(&path).unwrap();
        let committed = recorder
            .record(HarnessEvent::RunStarted {
                run_id: run_id.clone(),
                agent_id: AgentId::new("plato").unwrap(),
            })
            .unwrap();
        drop(recorder);

        let uncommitted = LedgerLine {
            v: LEDGER_VERSION,
            record: RecordedEvent {
                seq: 1,
                occurred_at_ms: 1,
                event: HarnessEvent::RunFailed {
                    run_id,
                    reason: "complete JSON without commit marker".into(),
                },
            },
        };
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&serde_json::to_vec(&uncommitted).unwrap())
            .unwrap();
        file.flush().unwrap();
        drop(file);

        assert_eq!(read_records(&path).unwrap(), vec![committed]);
    }

    #[test]
    fn an_open_tail_descriptor_reads_each_acknowledged_record() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let run_id = RunId::new("run_tail").unwrap();
        let mut recorder = JsonlEventRecorder::create(&path).unwrap();
        recorder
            .record(HarnessEvent::RunStarted {
                run_id: run_id.clone(),
                agent_id: AgentId::new("plato").unwrap(),
            })
            .unwrap();
        let mut tail = File::open(&path).unwrap();
        tail.seek(SeekFrom::End(0)).unwrap();

        let second = recorder
            .record(HarnessEvent::ContextBuilt {
                run_id,
                turn_id: TurnId::new("turn_1").unwrap(),
                context: ContextPack {
                    fragments: vec![],
                    token_budget: 4_000,
                },
            })
            .unwrap();

        let mut appended = Vec::new();
        tail.read_to_end(&mut appended).unwrap();
        let mut expected = serde_json::to_vec(&LedgerLine {
            v: LEDGER_VERSION,
            record: second,
        })
        .unwrap();
        expected.push(b'\n');
        assert_eq!(appended, expected);
    }

    #[test]
    fn a_jsonl_write_failure_neither_acks_nor_appends_through_drop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let sqlite_path = dir.path().join("ledger.db");
        let run_id = RunId::new("run_write_failure").unwrap();
        let mut ledger = SqliteLedger::open_or_create(&sqlite_path).unwrap();
        ledger
            .begin_session_run("session_1", &run_id, "question", true)
            .unwrap();
        let mut recorder = JsonlEventRecorder::create(&path)
            .unwrap()
            .with_session(ledger, &run_id, true);
        recorder
            .record(HarnessEvent::RunStarted {
                run_id: run_id.clone(),
                agent_id: AgentId::new("plato").unwrap(),
            })
            .unwrap();
        recorder.file = File::open(&path).unwrap();

        assert!(matches!(
            recorder.record(HarnessEvent::ContextBuilt {
                run_id: run_id.clone(),
                turn_id: TurnId::new("turn_1").unwrap(),
                context: ContextPack {
                    fragments: vec![],
                    token_budget: 4_000,
                },
            }),
            Err(AppError::Io(_))
        ));
        assert!(matches!(
            recorder.record(HarnessEvent::ContextBuilt {
                run_id: run_id.clone(),
                turn_id: TurnId::new("turn_1").unwrap(),
                context: ContextPack {
                    fragments: vec![],
                    token_budget: 4_000,
                },
            }),
            Err(AppError::Config(_))
        ));
        drop(recorder);
        assert_eq!(read_records(&path).unwrap().len(), 1);
        let state = SqliteLedger::open_readonly(&sqlite_path).unwrap();
        assert_eq!(
            state
                .connection
                .query_row(
                    "SELECT status FROM session_runs WHERE run_id = ?1",
                    params![run_id.as_str()],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            RunStateName::Running.as_str()
        );
        drop(state);

        let mut recovered = JsonlEventRecorder::open(&path).unwrap();
        let terminal = recovered
            .record(HarnessEvent::RunFailed {
                run_id,
                reason: "recovered".into(),
            })
            .unwrap();
        assert_eq!(terminal.seq, 1);
    }

    #[cfg(unix)]
    #[test]
    fn sigkill_torn_tail_recovery_retains_every_acknowledged_event() {
        const CHILD_PATH: &str = "PLATONIC_JSONL_SIGKILL_CHILD_PATH";
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "ledger::jsonl::tests::jsonl_sigkill_writer_child",
                "--nocapture",
            ])
            .env(CHILD_PATH, &path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (sender, receiver) = mpsc::channel();
        let reader = thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                sender.send(line.unwrap()).unwrap();
            }
        });
        loop {
            let line = receiver.recv_timeout(Duration::from_secs(5)).unwrap();
            if line == "JSONL_TORN_READY acknowledged=2" {
                break;
            }
        }

        child.kill().unwrap();
        let status = child.wait().unwrap();
        assert_eq!(status.signal(), Some(9));
        reader.join().unwrap();
        let torn_len = fs::metadata(&path).unwrap().len();

        let mut recorder = JsonlEventRecorder::open(&path).unwrap();
        let acknowledged = read_records(&path).unwrap();
        assert_eq!(acknowledged.len(), 2);
        assert_eq!(acknowledged[0].seq, 0);
        assert_eq!(acknowledged[1].seq, 1);
        assert!(fs::metadata(&path).unwrap().len() < torn_len);
        let terminal = recorder
            .record(HarnessEvent::RunFailed {
                run_id: RunId::new("run_sigkill").unwrap(),
                reason: "recovered".into(),
            })
            .unwrap();
        assert_eq!(terminal.seq, 2);
        assert_eq!(read_records(&path).unwrap().len(), 3);
    }

    #[cfg(unix)]
    #[test]
    fn jsonl_sigkill_writer_child() {
        const CHILD_PATH: &str = "PLATONIC_JSONL_SIGKILL_CHILD_PATH";
        let Some(path) = std::env::var_os(CHILD_PATH).map(PathBuf::from) else {
            return;
        };
        let run_id = RunId::new("run_sigkill").unwrap();
        let mut recorder = JsonlEventRecorder::create(&path).unwrap();
        recorder
            .record(HarnessEvent::RunStarted {
                run_id: run_id.clone(),
                agent_id: AgentId::new("plato").unwrap(),
            })
            .unwrap();
        recorder
            .record(HarnessEvent::ContextBuilt {
                run_id,
                turn_id: TurnId::new("turn_1").unwrap(),
                context: ContextPack {
                    fragments: vec![],
                    token_budget: 4_000,
                },
            })
            .unwrap();
        drop(recorder);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(br#"{"v":2,"record":{"seq":2"#).unwrap();
        file.sync_data().unwrap();
        println!("JSONL_TORN_READY acknowledged=2");
        std::io::stdout().flush().unwrap();
        let mut blocked = String::new();
        std::io::stdin().read_line(&mut blocked).unwrap();
        panic!("SIGKILL child was released instead of killed");
    }

    #[test]
    fn rejects_wrong_ledger_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        std::fs::write(&path, concat!(r#"{"v":3,"record":{"seq":0,"occurred_at_ms":0,"event":{"event":"run_started","run_id":"run_1","agent_id":"plato"}}}"#, "\n")).unwrap();

        assert!(matches!(
            read_records(&path),
            Err(AppError::LedgerVersion {
                expected: LEDGER_VERSION,
                actual: 3
            })
        ));
    }

    #[test]
    fn selected_run_jsonl_rejects_records_for_another_run() {
        let dir = tempfile::tempdir().unwrap();
        let sqlite_path = dir.path().join("ledger.db");
        let selected_run = RunId::new("run_selected").unwrap();
        let actual_run = RunId::new("run_other").unwrap();
        let jsonl_path = run_jsonl_path(&sqlite_path, selected_run.as_str()).unwrap();
        fs::create_dir_all(jsonl_path.parent().unwrap()).unwrap();
        let mut recorder = JsonlEventRecorder::create(&jsonl_path).unwrap();
        recorder
            .record(HarnessEvent::RunStarted {
                run_id: actual_run.clone(),
                agent_id: AgentId::new("plato").unwrap(),
            })
            .unwrap();
        drop(recorder);
        let ledger = SqliteLedger::open_or_create(&sqlite_path).unwrap();

        assert!(matches!(
            ledger.read_run(selected_run.as_str()),
            Err(AppError::Core(platonic_core::Error::RunIdMismatch {
                expected,
                actual
            })) if expected == selected_run.as_str() && actual == actual_run.as_str()
        ));
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
    fn jsonl_voice_read_rejects_version_unknown_fields_and_missing_core_turn() {
        let run_id = RunId::new("run_voice_corrupt").unwrap();
        let missing_turn = TurnId::new("turn_missing").unwrap();
        let event = VoiceEvent::VoiceSpoken {
            run_id: run_id.clone(),
            turn_id: missing_turn.clone(),
            ttfa_ms: 1,
            sentence_count: 1,
            interrupted_at: None,
        };
        let mut future_envelope = VoiceEventEnvelope::revision_one(0, event.clone());
        future_envelope.v = VOICE_EVENT_VERSION + 1;
        assert!(matches!(
            validate_voice_envelopes(&[future_envelope]),
            Err(AppError::VoiceEventVersion {
                expected: VOICE_EVENT_VERSION,
                actual: 2
            })
        ));

        let envelope = VoiceEventEnvelope::revision_one(0, event);
        let valid_voice = serde_json::json!({"voice_events": [envelope]});
        let mut future_version = valid_voice.clone();
        future_version["voice_events"][0]["v"] = serde_json::json!(2);
        let mut unknown_field = valid_voice.clone();
        unknown_field["unexpected"] = serde_json::json!(true);

        for (name, corruption) in [
            ("future-version", future_version),
            ("unknown-field", unknown_field),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let sqlite_path = dir.path().join(format!("{name}.db"));
            let jsonl_path = write_jsonl_core_keys(&sqlite_path, &run_id, "turn_1");
            append_jsonl_value(&jsonl_path, &corruption);

            let ledger = SqliteLedger::open_or_create(&sqlite_path).unwrap();
            assert!(matches!(
                ledger.read_voice_events(run_id.as_str()),
                Err(AppError::Json(_))
            ));
        }

        let dir = tempfile::tempdir().unwrap();
        let sqlite_path = dir.path().join("missing-turn.db");
        let jsonl_path = write_jsonl_core_keys(&sqlite_path, &run_id, "turn_1");
        append_jsonl_value(&jsonl_path, &valid_voice);

        let ledger = SqliteLedger::open_or_create(&sqlite_path).unwrap();
        assert!(matches!(
            ledger.read_voice_events(run_id.as_str()),
            Err(AppError::VoiceEventContract(message))
                if message.contains("turn_missing is absent from core run run_voice_corrupt")
        ));
    }

    #[test]
    fn discarding_empty_jsonl_admission_removes_only_its_owned_session_state() {
        let root = tempfile::tempdir().unwrap();
        let location = default_location(root.path());
        let fresh = RunId::new("run_discard_fresh").unwrap();
        let mut ledger = SqliteLedger::open_or_create_default(&location).unwrap();
        ledger
            .begin_session_run("session_fresh", &fresh, "fresh question", true)
            .unwrap();
        let fresh_log = run_jsonl_path(location.as_path(), fresh.as_str()).unwrap();
        let fresh_recorder = EventRecorder::create_default_jsonl(&location, &fresh)
            .unwrap()
            .with_session_jsonl_creation(ledger, &fresh, true);

        fresh_recorder.discard_empty_session_admission().unwrap();

        assert!(!fresh_log.exists());
        assert!(matches!(
            SqliteLedger::open_default_readonly(&location)
                .unwrap()
                .read_session("session_fresh"),
            Err(AppError::SessionNotFound(_))
        ));

        let prior = RunId::new("run_prior").unwrap();
        let continued = RunId::new("run_discard_continued").unwrap();
        let mut ledger = SqliteLedger::open_or_create_default(&location).unwrap();
        ledger
            .begin_session_run("session_existing", &prior, "prior", true)
            .unwrap();
        append_response_prefix(&mut ledger, &prior, "answer", 1);
        ledger.finish_session_run(&prior, "answer").unwrap();
        ledger
            .begin_session_run("session_existing", &continued, "discarded follow up", false)
            .unwrap();
        let continued_log = run_jsonl_path(location.as_path(), continued.as_str()).unwrap();
        let continued_recorder = EventRecorder::create_default_jsonl(&location, &continued)
            .unwrap()
            .with_session_jsonl_creation(ledger, &continued, false);

        continued_recorder
            .discard_empty_session_admission()
            .unwrap();

        assert!(!continued_log.exists());
        let session = SqliteLedger::open_default_readonly(&location)
            .unwrap()
            .read_session("session_existing")
            .unwrap();
        assert_eq!(session.runs.len(), 1);
        assert_eq!(session.runs[0].run_id, prior.as_str());
    }
    fn write_jsonl_core_keys(sqlite_path: &Path, run_id: &RunId, turn_id: &str) -> PathBuf {
        let jsonl_path = run_jsonl_path(sqlite_path, run_id.as_str()).unwrap();
        fs::create_dir_all(jsonl_path.parent().unwrap()).unwrap();
        let mut recorder = JsonlEventRecorder::create(&jsonl_path).unwrap();
        recorder
            .record(HarnessEvent::RunStarted {
                run_id: run_id.clone(),
                agent_id: AgentId::new("plato").unwrap(),
            })
            .unwrap();
        recorder
            .record(HarnessEvent::ContextBuilt {
                run_id: run_id.clone(),
                turn_id: TurnId::new(turn_id).unwrap(),
                context: ContextPack {
                    fragments: vec![],
                    token_budget: 4_000,
                },
            })
            .unwrap();
        jsonl_path
    }

    fn append_jsonl_value(path: &Path, value: &Value) {
        let mut file = OpenOptions::new().append(true).open(path).unwrap();
        serde_json::to_writer(&mut file, value).unwrap();
        file.write_all(b"\n").unwrap();
        file.flush().unwrap();
    }
}
