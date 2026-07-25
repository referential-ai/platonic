use crate::{
    AppError, AppResult, ApprovalRequest, AssistantDeltaEvent,
    daemon::{
        protocol::{PendingApprovalSnapshot, RunStateName},
        server::DaemonPaths,
    },
    tools::ApprovalOutcome,
};
use platonic_core::RecordedEvent;
use serde_json::{Value, json};
#[cfg(all(test, unix))]
use std::sync::Barrier;
use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

pub(super) const MAX_EVENT_BUFFER: usize = 256;

#[derive(Clone, Debug)]
pub(super) struct DaemonRuntime {
    pub(super) paths: DaemonPaths,
    pub(super) state: Arc<Mutex<RuntimeState>>,
    pub(super) stop_requested: Arc<AtomicBool>,
    #[cfg(all(test, unix))]
    shutdown_flush_barrier: Arc<Mutex<Option<Arc<Barrier>>>>,
}

#[derive(Debug, Default)]
pub(super) struct RuntimeState {
    pub(super) runs: HashMap<String, Arc<RunRecord>>,
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
            state: Arc::new(Mutex::new(RuntimeState::default())),
            stop_requested: Arc::new(AtomicBool::new(false)),
            #[cfg(all(test, unix))]
            shutdown_flush_barrier: Arc::new(Mutex::new(None)),
        }
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
    pub(super) events: VecDeque<Value>,
}

#[derive(Clone, Debug)]
pub(super) struct PendingApproval {
    pub(super) request: ApprovalRequest,
    pub(super) decision: Option<ApprovalOutcome>,
}

impl PendingApproval {
    pub(super) fn new(request: ApprovalRequest) -> Self {
        Self {
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
        }
    }

    pub(super) fn push_event(&self, event: Value) {
        let mut buffer = self.events.lock().expect("event buffer lock poisoned");
        if buffer.events.len() == MAX_EVENT_BUFFER {
            buffer.events.pop_front();
            buffer.first_offset += 1;
        }
        let offset = buffer.next_offset;
        buffer.next_offset += 1;
        buffer.events.push_back(json!({
            "offset": offset,
            "event": event,
        }));
    }

    pub(super) fn push_recorded_event(&self, record: RecordedEvent) {
        self.push_event(json!({
            "kind": "ledger",
            "record": record,
        }));
    }

    pub(super) fn push_assistant_delta(&self, delta: AssistantDeltaEvent) {
        self.push_event(json!({
            "kind": "assistant_delta",
            "run_id": delta.run_id.to_string(),
            "turn_id": delta.turn_id.to_string(),
            "step": delta.step,
            "delta_index": delta.delta_index,
            "text": delta.text,
        }));
    }

    pub(super) fn status(&self) -> RunStatus {
        self.status
            .lock()
            .expect("run status lock poisoned")
            .clone()
    }

    pub(super) fn set_finished(&self, final_answer: String) {
        let mut status = self.status.lock().expect("run status lock poisoned");
        status.state = RunStateName::Finished;
        status.final_answer = Some(final_answer);
        status.error = None;
    }

    pub(super) fn set_terminal_error(&self, error: &AppError) {
        let mut status = self.status.lock().expect("run status lock poisoned");
        status.state = match error {
            AppError::RunCanceled => RunStateName::Canceled,
            _ => RunStateName::Failed,
        };
        status.final_answer = None;
        status.error = Some(error.to_string());
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
    record: Arc<RunRecord>,
) -> impl Fn(ApprovalRequest) -> AppResult<ApprovalOutcome> + Send + Sync + 'static {
    move |request| {
        let call_id = request.call_id.to_string();
        let mut approvals = record.approvals.lock().expect("approvals lock poisoned");
        if record.cancel.load(Ordering::SeqCst) {
            return Ok(ApprovalOutcome::Denied {
                reason: "run canceled".into(),
            });
        }
        approvals.insert(call_id.clone(), PendingApproval::new(request.clone()));
        record.push_event(approval_requested_event(&request));
        loop {
            if record.cancel.load(Ordering::SeqCst) {
                approvals.remove(&call_id);
                return Ok(ApprovalOutcome::Denied {
                    reason: "run canceled".into(),
                });
            }
            if let Some(pending) = approvals.get_mut(&call_id)
                && let Some(decision) = pending.decision.take()
            {
                approvals.remove(&call_id);
                return Ok(decision);
            }
            approvals = record
                .approval_changed
                .wait(approvals)
                .expect("approval condvar lock poisoned");
        }
    }
}

fn approval_requested_event(request: &ApprovalRequest) -> Value {
    let mut event = json!({
        "kind": "approval_requested",
        "run_id": &request.run_id,
        "tool_call_id": &request.call_id,
        "tool_name": &request.tool_name,
        "effect": &request.effect,
        "reason": &request.reason,
    });
    if let Some(diff_preview) = &request.diff_preview {
        event["diff_preview"] = json!(diff_preview);
    }
    if let Some(approval_preview) = &request.approval_preview {
        event["approval_preview"] = json!(approval_preview);
    }
    event
}

#[cfg(test)]
mod tests {
    use super::*;
    use platonic_core::{EffectClass, RunId, ToolCallId};
    use std::{path::PathBuf, sync::Barrier, thread};

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
    fn late_cancel_does_not_reclassify_failure() {
        let record = run_record(1);
        record.cancel.store(true, Ordering::SeqCst);

        record.set_terminal_error(&AppError::RunFailed("provider failed".into()));

        assert_eq!(record.status().state, RunStateName::Failed);
        assert_eq!(
            record.status().error.as_deref(),
            Some("run did not finish: provider failed")
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
            PendingApproval::new(ApprovalRequest {
                run_id: RunId::new("run_1").unwrap(),
                call_id: ToolCallId::new("call_1").unwrap(),
                tool_name: "file.write".into(),
                effect: EffectClass::WorkspaceWrite,
                reason: "file.write requires approval".into(),
                input_preview: None,
                approval_preview: None,
                diff_preview: None,
            }),
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

        record.set_finished("done".into());
        assert_eq!(runtime.shutdown_if_idle(), ShutdownIfIdleDecision::Shutdown);
        assert_eq!(
            runtime.shutdown_if_idle(),
            ShutdownIfIdleDecision::AlreadyShuttingDown
        );
    }

    #[test]
    fn approval_requested_event_carries_diff_preview_when_present() {
        let event = approval_requested_event(&ApprovalRequest {
            run_id: RunId::new("run_1").unwrap(),
            call_id: ToolCallId::new("call_1").unwrap(),
            tool_name: "file.edit".into(),
            effect: EffectClass::WorkspaceWrite,
            reason: "file.edit requires approval".into(),
            input_preview: None,
            approval_preview: None,
            diff_preview: Some("--- a/note.txt\n+++ b/note.txt\n".into()),
        });

        assert_eq!(event["kind"], "approval_requested");
        assert_eq!(event["diff_preview"], "--- a/note.txt\n+++ b/note.txt\n");
    }

    #[test]
    fn approval_requested_event_omits_diff_preview_when_absent() {
        let event = approval_requested_event(&ApprovalRequest {
            run_id: RunId::new("run_1").unwrap(),
            call_id: ToolCallId::new("call_1").unwrap(),
            tool_name: "file.write".into(),
            effect: EffectClass::WorkspaceWrite,
            reason: "file.write requires approval".into(),
            input_preview: None,
            approval_preview: None,
            diff_preview: None,
        });

        assert!(event.get("diff_preview").is_none());
    }

    #[test]
    fn approval_requested_event_carries_approval_preview_when_present() {
        let event = approval_requested_event(&ApprovalRequest {
            run_id: RunId::new("run_1").unwrap(),
            call_id: ToolCallId::new("call_1").unwrap(),
            tool_name: "shell.exec".into(),
            effect: EffectClass::ExternalSideEffect,
            reason: "shell.exec requires approval".into(),
            input_preview: None,
            approval_preview: Some("command: cargo test\ncwd: /tmp/work".into()),
            diff_preview: None,
        });

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
        let decide = approval_handler(record.clone());

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
            ApprovalOutcome::Denied {
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
        let decide = approval_handler(record.clone());
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
        approvals.get_mut("call_1").unwrap().decision = Some(ApprovalOutcome::Denied {
            reason: "test complete".into(),
        });
        record.approval_changed.notify_all();
        drop(approvals);

        assert_eq!(
            worker.join().unwrap(),
            ApprovalOutcome::Denied {
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
        assert_eq!(buffer.events[0]["offset"], 0);
        assert_eq!(buffer.events[0]["event"]["kind"], "assistant_delta");
        assert_eq!(buffer.events[0]["event"]["run_id"], "run_1");
        assert_eq!(buffer.events[0]["event"]["turn_id"], "turn_1");
        assert_eq!(buffer.events[0]["event"]["step"], 0);
        assert_eq!(buffer.events[0]["event"]["delta_index"], 1);
        assert_eq!(buffer.events[0]["event"]["text"], "hello");
    }
}
