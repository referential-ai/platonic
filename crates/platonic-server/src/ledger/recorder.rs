use super::{
    jsonl::{JsonlEventRecorder, run_jsonl_path},
    sqlite::{SqliteEventRecorder, SqliteLedger},
};
use crate::{AppError, AppResult, paths::DefaultSqlitePath};
use platonic_core::{HarnessEvent, RecordedEvent, RunId, RunState};
use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

pub(crate) const RUN_CANCELED_REASON: &str = "run canceled";

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

    pub(crate) fn create_default_jsonl(
        path: &DefaultSqlitePath,
        run_id: &RunId,
    ) -> AppResult<Self> {
        Ok(Self::Jsonl(JsonlEventRecorder::create_private(
            &run_jsonl_path(path.as_path(), run_id.as_str())?,
        )?))
    }

    pub(crate) fn from_session_sqlite(ledger: SqliteLedger, run_id: &RunId) -> Self {
        Self::Sqlite(SqliteEventRecorder::from_session(ledger, run_id))
    }

    pub(crate) fn with_session_jsonl_creation(
        self,
        ledger: SqliteLedger,
        run_id: &RunId,
        created_session: bool,
    ) -> Self {
        match self {
            Self::Jsonl(recorder) => {
                Self::Jsonl(recorder.with_session(ledger, run_id, created_session))
            }
            Self::Sqlite(_) => unreachable!("only a JSONL recorder can attach JSONL session state"),
        }
    }

    pub(crate) fn discard_empty_session_admission(self) -> AppResult<()> {
        match self {
            Self::Jsonl(recorder) => recorder.discard_empty_session_admission(),
            Self::Sqlite(_) => Err(AppError::Config(
                "only a JSONL session admission can be discarded".into(),
            )),
        }
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
            Self::Jsonl(recorder) => recorder.finish_run(run_id, final_answer),
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
            Self::Jsonl(recorder) => recorder.fail_run(run_id, error, canceled),
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

pub(super) fn next_record(state: &mut RunState, event: HarnessEvent) -> AppResult<RecordedEvent> {
    let record = RecordedEvent {
        seq: state.next_seq(),
        occurred_at_ms: now_ms(),
        event,
    };
    state.apply(&record)?;
    Ok(record)
}

pub(super) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().try_into().unwrap_or(u64::MAX))
        .unwrap_or(0)
}
#[cfg(test)]
mod tests {
    use crate::ledger::{
        JsonlEventRecorder, SqliteEventRecorder, read_records, read_sqlite_records,
    };
    use platonic_core::{AgentId, HarnessEvent, RunId};

    #[test]
    fn jsonl_and_sqlite_reconstruct_same_record() {
        let dir = tempfile::tempdir().unwrap();
        let jsonl_path = dir.path().join("events.jsonl");
        let sqlite_path = dir.path().join("events.db");
        let run_id = RunId::new("run_1").unwrap();
        let mut jsonl = JsonlEventRecorder::create(&jsonl_path).unwrap();
        let mut sqlite = SqliteEventRecorder::create(&sqlite_path, &run_id).unwrap();
        let event = HarnessEvent::RunStarted(platonic_core::RunStartedEvent {
            run_id,
            identity: platonic_core::RunIdentity::LegacyAgent {
                agent_id: AgentId::new("plato").unwrap(),
            },
        });

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
}
