use crate::{
    AppError, AppResult, ApprovalRequest, AssistantDeltaEvent,
    app::ExternalApprovalOutcome,
    daemon::{
        DaemonPaths,
        protocol::{
            ApprovalDecision, BufferedStreamEvent, PendingApprovalSnapshot, RunStateName,
            StreamEvent,
        },
    },
    tool_catalog::SHELL_EXEC,
};
use platonic_core::{EffectClass, RecordedEvent};
#[cfg(test)]
use std::sync::Barrier;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::PathBuf,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

pub(super) const MAX_EVENT_BUFFER: usize = 256;
pub(super) const MAX_TERMINAL_RUNS: usize = 32;

#[cfg(test)]
type SessionGrantInstallBarriers = Option<(Arc<Barrier>, Arc<Barrier>)>;

#[derive(Clone, Debug)]
pub(super) struct DaemonRuntime {
    pub(super) paths: DaemonPaths,
    started_at: Instant,
    pub(super) state: Arc<Mutex<RuntimeState>>,
    session_tool_grants: Arc<Mutex<HashSet<(String, String)>>>,
    pub(super) stop_requested: Arc<AtomicBool>,
    #[cfg(test)]
    session_grant_install_barriers: Arc<Mutex<SessionGrantInstallBarriers>>,
    #[cfg(all(test, unix))]
    shutdown_flush_barrier: Arc<Mutex<Option<Arc<Barrier>>>>,
}

#[derive(Debug, Default)]
pub(super) struct RuntimeState {
    pub(super) runs: HashMap<String, Arc<RunRecord>>,
    terminal_runs: VecDeque<String>,
    shutdown_accepted: bool,
    issue_prep_active: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum RunAdmissionError {
    ShuttingDown,
    SessionActive { run_id: String },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum IssuePrepAdmissionError {
    ShuttingDown,
    Active,
}

#[must_use]
#[derive(Debug)]
pub(super) struct IssuePrepReservation {
    state: Arc<Mutex<RuntimeState>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShutdownIfIdleDecision {
    Shutdown,
    RefusedActive,
    AlreadyShuttingDown,
}

impl DaemonRuntime {
    pub(super) fn new(paths: DaemonPaths) -> Self {
        Self {
            paths,
            started_at: Instant::now(),
            state: Arc::new(Mutex::new(RuntimeState::default())),
            session_tool_grants: Arc::new(Mutex::new(HashSet::new())),
            stop_requested: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            session_grant_install_barriers: Arc::new(Mutex::new(None)),
            #[cfg(all(test, unix))]
            shutdown_flush_barrier: Arc::new(Mutex::new(None)),
        }
    }

    pub(super) fn uptime_ms(&self) -> u64 {
        self.started_at
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }

    pub(super) fn reserve_run(&self, record: Arc<RunRecord>) -> Result<(), RunAdmissionError> {
        let mut state = self.state.lock().expect("runtime state lock poisoned");
        if state.shutdown_accepted {
            return Err(RunAdmissionError::ShuttingDown);
        }
        if let Some(run_id) = state
            .runs
            .values()
            .find(|active| {
                active.session_id == record.session_id
                    && matches!(
                        active.status().state,
                        RunStateName::Running | RunStateName::CancelRequested
                    )
            })
            .map(|active| active.run_id.clone())
        {
            return Err(RunAdmissionError::SessionActive { run_id });
        }
        state.runs.insert(record.run_id.clone(), record);
        Ok(())
    }

    pub(super) fn finish_run(&self, record: &RunRecord, final_answer: String) {
        self.complete_run(
            record,
            RunStatus {
                state: RunStateName::Finished,
                final_answer: Some(final_answer),
                error: None,
            },
        );
    }

    pub(super) fn finish_run_with_error(&self, record: &RunRecord, error: &AppError) {
        self.complete_run(
            record,
            RunStatus {
                state: match error {
                    AppError::RunCanceled => RunStateName::Canceled,
                    _ => RunStateName::Failed,
                },
                final_answer: None,
                error: Some(error.to_string()),
            },
        );
    }

    fn complete_run(&self, record: &RunRecord, status: RunStatus) {
        let mut approvals = record.approvals.lock().expect("approvals lock poisoned");
        *record.status.lock().expect("run status lock poisoned") = status;
        approvals.retain(|_, pending| pending.decision.is_some());
        record.approval_changed.notify_all();
        drop(approvals);

        let mut state = self.state.lock().expect("runtime state lock poisoned");
        state.terminal_runs.push_back(record.run_id.clone());
        while state.terminal_runs.len() > MAX_TERMINAL_RUNS {
            if let Some(run_id) = state.terminal_runs.pop_front() {
                state.runs.remove(&run_id);
            }
        }
    }

    pub(super) fn reserve_issue_prep(
        &self,
    ) -> Result<IssuePrepReservation, IssuePrepAdmissionError> {
        let mut state = self.state.lock().expect("runtime state lock poisoned");
        if state.shutdown_accepted {
            return Err(IssuePrepAdmissionError::ShuttingDown);
        }
        if state.issue_prep_active {
            return Err(IssuePrepAdmissionError::Active);
        }
        state.issue_prep_active = true;
        Ok(IssuePrepReservation {
            state: self.state.clone(),
        })
    }

    pub(super) fn shutdown_if_idle(&self) -> ShutdownIfIdleDecision {
        let mut state = self.state.lock().expect("runtime state lock poisoned");
        if state.shutdown_accepted {
            return ShutdownIfIdleDecision::AlreadyShuttingDown;
        }
        if state.issue_prep_active
            || state.runs.values().any(|record| {
                matches!(
                    record.status().state,
                    RunStateName::Running | RunStateName::CancelRequested
                )
            })
        {
            return ShutdownIfIdleDecision::RefusedActive;
        }
        state.shutdown_accepted = true;
        ShutdownIfIdleDecision::Shutdown
    }

    pub(super) fn shutdown_accepted(&self) -> bool {
        self.state
            .lock()
            .expect("runtime state lock poisoned")
            .shutdown_accepted
    }

    pub(super) fn has_shell_session_grant(&self, session_id: &str) -> bool {
        self.session_tool_grants
            .lock()
            .expect("session tool grants lock poisoned")
            .contains(&(session_id.to_owned(), SHELL_EXEC.to_owned()))
    }

    #[cfg(test)]
    pub(super) fn session_tool_grant_count(&self) -> usize {
        self.session_tool_grants
            .lock()
            .expect("session tool grants lock poisoned")
            .len()
    }

    pub(super) fn install_shell_session_grant(&self, session_id: &str) -> bool {
        let installed = self
            .session_tool_grants
            .lock()
            .expect("session tool grants lock poisoned")
            .insert((session_id.to_owned(), SHELL_EXEC.to_owned()));
        #[cfg(test)]
        if installed {
            let barriers = self
                .session_grant_install_barriers
                .lock()
                .expect("session grant barrier lock poisoned")
                .take();
            if let Some((reached, release)) = barriers {
                reached.wait();
                release.wait();
            }
        }
        installed
    }

    #[cfg(test)]
    pub(super) fn set_session_grant_install_barriers(
        &self,
        reached: Arc<Barrier>,
        release: Arc<Barrier>,
    ) {
        *self
            .session_grant_install_barriers
            .lock()
            .expect("session grant barrier lock poisoned") = Some((reached, release));
    }

    #[cfg(all(test, unix))]
    pub(super) fn set_shutdown_flush_barrier(&self, barrier: Arc<Barrier>) {
        *self.shutdown_flush_barrier.lock().unwrap() = Some(barrier);
    }

    #[cfg(all(test, unix))]
    pub(super) fn wait_after_shutdown_flush(&self) {
        let barrier = self.shutdown_flush_barrier.lock().unwrap().clone();
        if let Some(barrier) = barrier {
            barrier.wait();
        }
    }
}

impl Drop for IssuePrepReservation {
    fn drop(&mut self) {
        self.state
            .lock()
            .expect("runtime state lock poisoned")
            .issue_prep_active = false;
    }
}

#[derive(Debug)]
pub(super) struct RunRecord {
    pub(super) run_id: String,
    pub(super) session_id: String,
    pub(super) ledger_path: PathBuf,
    pub(super) cancel: Arc<AtomicBool>,
    pub(super) status: Mutex<RunStatus>,
    pub(super) events: Mutex<EventBuffer>,
    pub(super) approvals: Mutex<HashMap<String, PendingApproval>>,
    pub(super) approval_changed: Condvar,
    #[cfg(test)]
    event_snapshot_barriers: Mutex<Option<(Arc<Barrier>, Arc<Barrier>)>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RunStatus {
    pub(super) state: RunStateName,
    pub(super) final_answer: Option<String>,
    pub(super) error: Option<String>,
}

#[derive(Debug)]
pub(super) struct EventBuffer {
    pub(super) first_offset: u64,
    pub(super) next_offset: u64,
    pub(super) events: VecDeque<BufferedStreamEvent>,
}

#[derive(Clone, Debug)]
pub(super) struct PendingApproval {
    pub(super) session_id: String,
    pub(super) request: ApprovalRequest,
    pub(super) decision: Option<PendingApprovalDecision>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PendingApprovalDecision {
    pub(super) decision: ApprovalDecision,
    pub(super) outcome: ExternalApprovalOutcome,
}

impl PendingApproval {
    pub(super) fn new(session_id: String, request: ApprovalRequest) -> Self {
        Self {
            session_id,
            request,
            decision: None,
        }
    }

    fn snapshot(&self) -> PendingApprovalSnapshot {
        PendingApprovalSnapshot {
            run_id: self.request.run_id.to_string(),
            tool_call_id: self.request.call_id.to_string(),
            tool_name: self.request.tool_name.clone(),
            effect: self.request.effect.clone(),
            reason: Some(self.request.reason.clone()),
            input_preview: self.request.input_preview.clone(),
            approval_preview: self.request.approval_preview.clone(),
            diff_preview: self.request.diff_preview.clone(),
        }
    }
}

impl RunRecord {
    pub(super) fn new(run_id: String, session_id: String, ledger_path: PathBuf) -> Self {
        Self {
            session_id,
            run_id,
            ledger_path,
            cancel: Arc::new(AtomicBool::new(false)),
            status: Mutex::new(RunStatus {
                state: RunStateName::Running,
                final_answer: None,
                error: None,
            }),
            events: Mutex::new(EventBuffer {
                first_offset: 0,
                next_offset: 0,
                events: VecDeque::new(),
            }),
            approvals: Mutex::new(HashMap::new()),
            approval_changed: Condvar::new(),
            #[cfg(test)]
            event_snapshot_barriers: Mutex::new(None),
        }
    }

    pub(super) fn push_event(&self, event: StreamEvent) {
        let mut buffer = self.events.lock().expect("event buffer lock poisoned");
        if buffer.events.len() == MAX_EVENT_BUFFER {
            buffer.events.pop_front();
            buffer.first_offset += 1;
        }
        let offset = buffer.next_offset;
        buffer.next_offset += 1;
        buffer
            .events
            .push_back(BufferedStreamEvent { offset, event });
    }

    pub(super) fn push_recorded_event(&self, record: RecordedEvent) {
        self.push_event(StreamEvent::Ledger { record });
    }

    pub(super) fn push_assistant_delta(&self, delta: AssistantDeltaEvent) {
        self.push_event(StreamEvent::AssistantDelta {
            run_id: delta.run_id.to_string(),
            turn_id: delta.turn_id.to_string(),
            step: delta.step,
            delta_index: delta.delta_index,
            text: delta.text,
        });
    }

    pub(super) fn status(&self) -> RunStatus {
        self.status
            .lock()
            .expect("run status lock poisoned")
            .clone()
    }

    #[cfg(test)]
    pub(super) fn set_event_snapshot_barriers(&self, reached: Arc<Barrier>, release: Arc<Barrier>) {
        *self.event_snapshot_barriers.lock().unwrap() = Some((reached, release));
    }

    #[cfg(test)]
    pub(super) fn wait_during_event_snapshot(&self) {
        let barriers = self.event_snapshot_barriers.lock().unwrap().take();
        if let Some((reached, release)) = barriers {
            reached.wait();
            release.wait();
        }
    }

    pub(super) fn pending_approval(&self) -> Option<PendingApprovalSnapshot> {
        let approvals = self.approvals.lock().expect("approvals lock poisoned");
        if self.cancel.load(Ordering::SeqCst) || self.status().state != RunStateName::Running {
            return None;
        }
        approvals
            .values()
            .find(|pending| pending.decision.is_none())
            .map(PendingApproval::snapshot)
    }
}

pub(super) fn approval_handler(
    runtime: DaemonRuntime,
    record: Arc<RunRecord>,
) -> impl Fn(ApprovalRequest) -> AppResult<ExternalApprovalOutcome> + Send + Sync + 'static {
    move |request| {
        let call_id = request.call_id.to_string();
        let mut approvals = record.approvals.lock().expect("approvals lock poisoned");
        if request.run_id.as_str() != record.run_id {
            return Err(AppError::RunFailed(
                "approval request does not match daemon run".into(),
            ));
        }
        if record.cancel.load(Ordering::SeqCst) {
            return Ok(ExternalApprovalOutcome::Denied {
                actor: "daemon",
                reason: "run canceled".into(),
            });
        }
        if record.status().state != RunStateName::Running {
            return Ok(ExternalApprovalOutcome::Denied {
                actor: "daemon",
                reason: "run is no longer active".into(),
            });
        }
        if request.tool_name == SHELL_EXEC
            && request.effect == EffectClass::ExternalSideEffect
            && runtime.has_shell_session_grant(&record.session_id)
        {
            return Ok(ExternalApprovalOutcome::Granted {
                actor: "session_grant",
            });
        }
        approvals.insert(
            call_id.clone(),
            PendingApproval::new(record.session_id.clone(), request.clone()),
        );
        record.push_event(approval_requested_event(&request));
        loop {
            if let Some(pending) = approvals.get(&call_id)
                && let Some(decision) = &pending.decision
            {
                return Ok(decision.outcome.clone());
            }
            if record.cancel.load(Ordering::SeqCst) {
                approvals.remove(&call_id);
                return Ok(ExternalApprovalOutcome::Denied {
                    actor: "daemon",
                    reason: "run canceled".into(),
                });
            }
            if record.status().state != RunStateName::Running {
                approvals.remove(&call_id);
                return Ok(ExternalApprovalOutcome::Denied {
                    actor: "daemon",
                    reason: "run is no longer active".into(),
                });
            }
            approvals = record
                .approval_changed
                .wait(approvals)
                .expect("approval condvar lock poisoned");
        }
    }
}

fn approval_requested_event(request: &ApprovalRequest) -> StreamEvent {
    StreamEvent::ApprovalRequested {
        run_id: request.run_id.to_string(),
        tool_call_id: request.call_id.to_string(),
        tool_name: request.tool_name.clone(),
        effect: request.effect.clone(),
        reason: request.reason.clone(),
        diff_preview: request.diff_preview.clone(),
        approval_preview: request.approval_preview.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use platonic_core::{EffectClass, RunId, ToolCallId};
    use std::{path::PathBuf, sync::Barrier, thread, time::Duration};

    fn runtime() -> DaemonRuntime {
        DaemonRuntime::new(DaemonPaths {
            workspace_root: PathBuf::from("/tmp/workspace"),
            workspace_id: "workspace-1".into(),
            socket_path: PathBuf::from("/tmp/agent.sock"),
            lock_path: PathBuf::from("/tmp/agent.lock"),
            ledger_path: PathBuf::from("/tmp/agent.db"),
        })
    }

    fn run_record(index: usize) -> Arc<RunRecord> {
        Arc::new(RunRecord::new(
            format!("run_{index}"),
            format!("session_{index}"),
            PathBuf::from("/tmp/agent.db"),
        ))
    }

    #[test]
    fn uptime_is_monotonic_from_one_runtime_start_instant() {
        let runtime = runtime();
        let cloned = runtime.clone();
        let first = runtime.uptime_ms();
        thread::sleep(Duration::from_millis(5));
        let second = cloned.uptime_ms();

        assert!(
            second > first,
            "uptime did not increase: {first} -> {second}"
        );
    }

    #[test]
    fn late_cancel_does_not_reclassify_failure() {
        let runtime = runtime();
        let record = run_record(1);
        runtime.reserve_run(record.clone()).unwrap();
        record.cancel.store(true, Ordering::SeqCst);

        runtime.finish_run_with_error(&record, &AppError::RunFailed("provider failed".into()));

        assert_eq!(record.status().state, RunStateName::Failed);
        assert_eq!(
            record.status().error.as_deref(),
            Some("run did not finish: provider failed")
        );
    }

    #[test]
    fn terminal_retention_uses_completion_order_and_preserves_active_runs() {
        let runtime = runtime();
        let terminal_records = (0..=MAX_TERMINAL_RUNS).map(run_record).collect::<Vec<_>>();
        for record in &terminal_records {
            runtime.reserve_run(record.clone()).unwrap();
        }
        let approval_paused = run_record(100);
        approval_paused.approvals.lock().unwrap().insert(
            "call_1".into(),
            PendingApproval::new(
                "session_100".into(),
                ApprovalRequest {
                    run_id: RunId::new("run_100").unwrap(),
                    call_id: ToolCallId::new("call_1").unwrap(),
                    tool_name: "file.write".into(),
                    effect: EffectClass::WorkspaceWrite,
                    reason: "file.write requires approval".into(),
                    input_preview: None,
                    approval_preview: None,
                    diff_preview: None,
                },
            ),
        );
        runtime.reserve_run(approval_paused.clone()).unwrap();
        let cancel_requested = run_record(101);
        cancel_requested.status.lock().unwrap().state = RunStateName::CancelRequested;
        runtime.reserve_run(cancel_requested.clone()).unwrap();

        runtime.finish_run(&terminal_records[1], "done 1".into());
        runtime.finish_run(&terminal_records[0], "done 0".into());
        for (index, record) in terminal_records.iter().enumerate().skip(2) {
            runtime.finish_run(record, format!("done {index}"));
        }

        let state = runtime.state.lock().unwrap();
        assert_eq!(state.terminal_runs.len(), MAX_TERMINAL_RUNS);
        assert_eq!(
            state.terminal_runs.front().map(String::as_str),
            Some("run_0")
        );
        assert_eq!(
            state.terminal_runs.back().map(String::as_str),
            Some("run_32")
        );
        assert!(!state.runs.contains_key("run_1"));
        assert!(state.runs.contains_key("run_0"));
        assert!(state.runs.contains_key("run_32"));
        assert!(state.runs.contains_key("run_100"));
        assert!(state.runs.contains_key("run_101"));
        assert_eq!(state.runs.len(), MAX_TERMINAL_RUNS + 2);
        drop(state);

        assert!(approval_paused.pending_approval().is_some());
        assert_eq!(
            cancel_requested.status().state,
            RunStateName::CancelRequested
        );
        assert_eq!(
            runtime.shutdown_if_idle(),
            ShutdownIfIdleDecision::RefusedActive
        );
    }

    #[test]
    fn shutdown_and_run_admission_linearize() {
        for index in 0..256 {
            let runtime = runtime();
            let barrier = Arc::new(Barrier::new(3));
            let admit_runtime = runtime.clone();
            let admit_barrier = barrier.clone();
            let admission = thread::spawn(move || {
                admit_barrier.wait();
                admit_runtime.reserve_run(run_record(index))
            });
            let shutdown_runtime = runtime.clone();
            let shutdown_barrier = barrier.clone();
            let shutdown = thread::spawn(move || {
                shutdown_barrier.wait();
                shutdown_runtime.shutdown_if_idle()
            });

            barrier.wait();
            let admission = admission.join().unwrap();
            let shutdown = shutdown.join().unwrap();
            assert!(matches!(
                (admission, shutdown),
                (Ok(()), ShutdownIfIdleDecision::RefusedActive)
                    | (
                        Err(RunAdmissionError::ShuttingDown),
                        ShutdownIfIdleDecision::Shutdown
                    )
            ));
        }
    }

    #[test]
    fn shutdown_and_issue_prep_admission_linearize() {
        for _ in 0..256 {
            let runtime = runtime();
            let barrier = Arc::new(Barrier::new(3));
            let admit_runtime = runtime.clone();
            let admit_barrier = barrier.clone();
            let admission = thread::spawn(move || {
                admit_barrier.wait();
                admit_runtime.reserve_issue_prep()
            });
            let shutdown_runtime = runtime.clone();
            let shutdown_barrier = barrier.clone();
            let shutdown = thread::spawn(move || {
                shutdown_barrier.wait();
                shutdown_runtime.shutdown_if_idle()
            });

            barrier.wait();
            let admission = admission.join().unwrap();
            let shutdown = shutdown.join().unwrap();
            match (admission, shutdown) {
                (Ok(reservation), ShutdownIfIdleDecision::RefusedActive) => drop(reservation),
                (Err(IssuePrepAdmissionError::ShuttingDown), ShutdownIfIdleDecision::Shutdown) => {}
                unexpected => panic!("issue-prep/shutdown race did not linearize: {unexpected:?}"),
            }
        }
    }

    #[test]
    fn issue_prep_reservation_rejects_duplicates_and_releases() {
        let runtime = runtime();
        let reservation = runtime.reserve_issue_prep().unwrap();

        assert!(matches!(
            runtime.reserve_issue_prep(),
            Err(IssuePrepAdmissionError::Active)
        ));
        assert_eq!(
            runtime.shutdown_if_idle(),
            ShutdownIfIdleDecision::RefusedActive
        );

        drop(reservation);
        let later = runtime.reserve_issue_prep().unwrap();
        drop(later);
        assert_eq!(runtime.shutdown_if_idle(), ShutdownIfIdleDecision::Shutdown);
        assert!(matches!(
            runtime.reserve_issue_prep(),
            Err(IssuePrepAdmissionError::ShuttingDown)
        ));
    }

    #[test]
    fn issue_prep_reservation_releases_on_success_and_error_paths() {
        let success_runtime = runtime();
        {
            let _reservation = success_runtime.reserve_issue_prep().unwrap();
        }
        assert!(success_runtime.reserve_issue_prep().is_ok());

        let failed_runtime = runtime();
        let failed: Result<(), &'static str> = {
            let _reservation = failed_runtime.reserve_issue_prep().unwrap();
            Err("provider failed")
        };
        assert_eq!(failed, Err("provider failed"));
        let later = failed_runtime.reserve_issue_prep().unwrap();
        drop(later);
        assert_eq!(
            failed_runtime.shutdown_if_idle(),
            ShutdownIfIdleDecision::Shutdown
        );
    }

    #[test]
    fn approval_paused_run_refuses_shutdown_until_terminal() {
        let runtime = runtime();
        let record = run_record(1);
        record.approvals.lock().unwrap().insert(
            "call_1".into(),
            PendingApproval::new(
                "session_1".into(),
                ApprovalRequest {
                    run_id: RunId::new("run_1").unwrap(),
                    call_id: ToolCallId::new("call_1").unwrap(),
                    tool_name: "file.write".into(),
                    effect: EffectClass::WorkspaceWrite,
                    reason: "file.write requires approval".into(),
                    input_preview: None,
                    approval_preview: None,
                    diff_preview: None,
                },
            ),
        );
        runtime.reserve_run(record.clone()).unwrap();

        assert_eq!(
            runtime.shutdown_if_idle(),
            ShutdownIfIdleDecision::RefusedActive
        );
        assert!(!runtime.shutdown_accepted());

        record.status.lock().unwrap().state = RunStateName::CancelRequested;
        assert_eq!(
            runtime.shutdown_if_idle(),
            ShutdownIfIdleDecision::RefusedActive
        );

        runtime.finish_run(&record, "done".into());
        assert_eq!(runtime.shutdown_if_idle(), ShutdownIfIdleDecision::Shutdown);
        assert_eq!(
            runtime.shutdown_if_idle(),
            ShutdownIfIdleDecision::AlreadyShuttingDown
        );
    }

    #[test]
    fn approval_requested_event_carries_diff_preview_when_present() {
        let event = serde_json::to_value(approval_requested_event(&ApprovalRequest {
            run_id: RunId::new("run_1").unwrap(),
            call_id: ToolCallId::new("call_1").unwrap(),
            tool_name: "file.edit".into(),
            effect: EffectClass::WorkspaceWrite,
            reason: "file.edit requires approval".into(),
            input_preview: None,
            approval_preview: None,
            diff_preview: Some("--- a/note.txt\n+++ b/note.txt\n".into()),
        }))
        .unwrap();

        assert_eq!(event["kind"], "approval_requested");
        assert_eq!(event["diff_preview"], "--- a/note.txt\n+++ b/note.txt\n");
    }

    #[test]
    fn approval_requested_event_omits_diff_preview_when_absent() {
        let event = serde_json::to_value(approval_requested_event(&ApprovalRequest {
            run_id: RunId::new("run_1").unwrap(),
            call_id: ToolCallId::new("call_1").unwrap(),
            tool_name: "file.write".into(),
            effect: EffectClass::WorkspaceWrite,
            reason: "file.write requires approval".into(),
            input_preview: None,
            approval_preview: None,
            diff_preview: None,
        }))
        .unwrap();

        assert!(event.get("diff_preview").is_none());
    }

    #[test]
    fn approval_requested_event_carries_approval_preview_when_present() {
        let event = serde_json::to_value(approval_requested_event(&ApprovalRequest {
            run_id: RunId::new("run_1").unwrap(),
            call_id: ToolCallId::new("call_1").unwrap(),
            tool_name: "shell.exec".into(),
            effect: EffectClass::ExternalSideEffect,
            reason: "shell.exec requires approval".into(),
            input_preview: None,
            approval_preview: Some("command: cargo test\ncwd: /tmp/work".into()),
            diff_preview: None,
        }))
        .unwrap();

        assert_eq!(
            event["approval_preview"],
            "command: cargo test\ncwd: /tmp/work"
        );
    }

    #[test]
    fn canceled_run_does_not_register_or_publish_a_late_approval() {
        let record = Arc::new(RunRecord::new(
            "run_1".into(),
            "session_1".into(),
            PathBuf::from("/tmp/agent.db"),
        ));
        record.cancel.store(true, Ordering::SeqCst);
        let decide = approval_handler(runtime(), record.clone());

        let outcome = decide(ApprovalRequest {
            run_id: RunId::new("run_1").unwrap(),
            call_id: ToolCallId::new("call_1").unwrap(),
            tool_name: "file.write".into(),
            effect: EffectClass::WorkspaceWrite,
            reason: "file.write requires approval".into(),
            input_preview: Some(r#"{"path":"out.txt"}"#.into()),
            approval_preview: None,
            diff_preview: None,
        })
        .unwrap();

        assert_eq!(
            outcome,
            ExternalApprovalOutcome::Denied {
                actor: "daemon",
                reason: "run canceled".into()
            }
        );
        assert!(record.approvals.lock().unwrap().is_empty());
        assert!(record.events.lock().unwrap().events.is_empty());
    }

    #[test]
    fn published_approval_event_has_a_complete_snapshot() {
        let record = Arc::new(RunRecord::new(
            "run_1".into(),
            "session_1".into(),
            PathBuf::from("/tmp/agent.db"),
        ));
        let decide = approval_handler(runtime(), record.clone());
        let worker = thread::spawn(move || {
            decide(ApprovalRequest {
                run_id: RunId::new("run_1").unwrap(),
                call_id: ToolCallId::new("call_1").unwrap(),
                tool_name: "file.write".into(),
                effect: EffectClass::WorkspaceWrite,
                reason: "file.write requires approval".into(),
                input_preview: Some(r#"{"path":"out.txt"}"#.into()),
                approval_preview: None,
                diff_preview: None,
            })
            .unwrap()
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while record.events.lock().unwrap().events.is_empty() {
            assert!(
                std::time::Instant::now() < deadline,
                "approval event was not published"
            );
            thread::yield_now();
        }

        let snapshot = record.pending_approval().unwrap();
        assert_eq!(snapshot.run_id, "run_1");
        assert_eq!(snapshot.tool_call_id, "call_1");
        assert_eq!(
            snapshot.input_preview.as_deref(),
            Some(r#"{"path":"out.txt"}"#)
        );
        let mut approvals = record.approvals.lock().unwrap();
        approvals.get_mut("call_1").unwrap().decision = Some(PendingApprovalDecision {
            decision: ApprovalDecision::Deny,
            outcome: ExternalApprovalOutcome::Denied {
                actor: "daemon",
                reason: "test complete".into(),
            },
        });
        record.approval_changed.notify_all();
        drop(approvals);

        assert_eq!(
            worker.join().unwrap(),
            ExternalApprovalOutcome::Denied {
                actor: "daemon",
                reason: "test complete".into()
            }
        );
    }

    #[test]
    fn push_assistant_delta_buffers_transient_event() {
        let record = RunRecord::new(
            "run_1".into(),
            "session_1".into(),
            PathBuf::from("/tmp/agent.db"),
        );

        record.push_assistant_delta(AssistantDeltaEvent {
            run_id: RunId::new("run_1").unwrap(),
            turn_id: platonic_core::TurnId::new("turn_1").unwrap(),
            step: 0,
            delta_index: 1,
            text: "hello".into(),
        });

        let buffer = record.events.lock().unwrap();
        assert_eq!(buffer.next_offset, 1);
        let event = serde_json::to_value(&buffer.events[0]).unwrap();
        assert_eq!(event["offset"], 0);
        assert_eq!(event["event"]["kind"], "assistant_delta");
        assert_eq!(event["event"]["run_id"], "run_1");
        assert_eq!(event["event"]["turn_id"], "turn_1");
        assert_eq!(event["event"]["step"], 0);
        assert_eq!(event["event"]["delta_index"], 1);
        assert_eq!(event["event"]["text"], "hello");
    }
}
