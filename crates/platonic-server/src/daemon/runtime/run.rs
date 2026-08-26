use super::{DaemonRuntime, ThreadTurnBinding};
use crate::{
    AppError, AppResult, ApprovalRequest, AssistantDeltaEvent,
    app::ExternalApprovalOutcome,
    daemon::protocol::{
        ApprovalDecision, ApprovalProfile, BufferedStreamEvent, PendingApprovalSnapshot,
        RunStateName, StreamEvent,
    },
    tool_catalog::SHELL_EXEC,
};
use platonic_core::{EffectClass, RecordedEvent};
#[cfg(test)]
use std::sync::Barrier;
use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

pub(in crate::daemon) const MAX_EVENT_BUFFER: usize = 256;
pub(in crate::daemon) const MAX_TERMINAL_RUNS: usize = 32;
#[cfg(test)]
pub(super) type RunExecutionBarriers = Option<(Arc<Barrier>, Arc<Barrier>)>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::daemon) enum RunAdmissionError {
    ShuttingDown,
    SessionActive { run_id: String },
}

impl DaemonRuntime {
    #[cfg(test)]
    pub(in crate::daemon) fn reserve_run(
        &self,
        record: Arc<RunRecord>,
    ) -> Result<(), RunAdmissionError> {
        self.reserve_run_with_profile(record, None)
    }

    pub(in crate::daemon) fn reserve_run_with_profile(
        &self,
        record: Arc<RunRecord>,
        profile: Option<ApprovalProfile>,
    ) -> Result<(), RunAdmissionError> {
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
        if let Some(profile) = profile {
            state
                .approval_profiles
                .insert(record.session_id.clone(), profile);
        }
        state.runs.insert(record.run_id.clone(), record);
        Ok(())
    }
    pub(in crate::daemon) fn release_run_reservation(&self, record: &RunRecord) {
        let mut state = self.state.lock().expect("runtime state lock poisoned");
        state
            .active_thread_runs
            .retain(|_, active| !std::ptr::eq(Arc::as_ptr(active), record));
        if state
            .runs
            .get(&record.run_id)
            .is_some_and(|reserved| std::ptr::eq(Arc::as_ptr(reserved), record))
        {
            state.runs.remove(&record.run_id);
        }
    }

    #[cfg(test)]
    pub(in crate::daemon) fn set_run_execution_barriers(
        &self,
        reached: Arc<Barrier>,
        release: Arc<Barrier>,
    ) {
        *self.run_execution_barriers.lock().unwrap() = Some((reached, release));
    }

    #[cfg(test)]
    pub(in crate::daemon) fn wait_before_run_execution(&self) {
        let barriers = self.run_execution_barriers.lock().unwrap().take();
        if let Some((reached, release)) = barriers {
            reached.wait();
            release.wait();
        }
    }

    #[cfg(test)]
    pub(in crate::daemon) fn fail_next_run_handoff(&self) {
        self.fail_next_run_handoff.store(true, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(in crate::daemon) fn has_active_thread_run(&self, thread_id: &str) -> bool {
        self.state
            .lock()
            .expect("runtime state lock poisoned")
            .active_thread_runs
            .contains_key(thread_id)
    }

    #[cfg(test)]
    pub(in crate::daemon) fn take_run_handoff_failure(&self) -> bool {
        self.fail_next_run_handoff.swap(false, Ordering::SeqCst)
    }

    pub(in crate::daemon) fn finish_run(
        &self,
        record: &RunRecord,
        final_answer: String,
        completion_claim: Option<platonic_protocol::CompletionClaim>,
    ) {
        self.complete_run(
            record,
            RunStatus {
                state: RunStateName::Finished,
                final_answer: Some(final_answer),
                error: None,
                completion_claim,
            },
        );
    }

    pub(in crate::daemon) fn finish_run_with_error(&self, record: &RunRecord, error: &AppError) {
        self.complete_run(
            record,
            RunStatus {
                state: match error {
                    AppError::RunCanceled => RunStateName::Canceled,
                    _ => RunStateName::Failed,
                },
                final_answer: None,
                error: Some(error.to_string()),
                completion_claim: None,
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
        state
            .active_thread_runs
            .retain(|_, active| !std::ptr::eq(Arc::as_ptr(active), record));
        state.terminal_runs.push_back(record.run_id.clone());
        while state.terminal_runs.len() > MAX_TERMINAL_RUNS {
            if let Some(run_id) = state.terminal_runs.pop_front() {
                state.runs.remove(&run_id);
            }
        }
    }
}

#[derive(Debug)]
pub(in crate::daemon) struct RunRecord {
    pub(in crate::daemon) run_id: String,
    pub(in crate::daemon) session_id: String,
    pub(in crate::daemon) ledger_path: PathBuf,
    pub(in crate::daemon) cancel: Arc<AtomicBool>,
    pub(in crate::daemon) status: Mutex<RunStatus>,
    pub(in crate::daemon) events: Mutex<EventBuffer>,
    pub(in crate::daemon) approvals: Mutex<HashMap<String, PendingApproval>>,
    pub(in crate::daemon) approval_changed: Condvar,
    thread_turn: Option<ThreadTurnBinding>,
    #[cfg(test)]
    event_snapshot_barriers: Mutex<Option<(Arc<Barrier>, Arc<Barrier>)>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::daemon) struct RunStatus {
    pub(in crate::daemon) state: RunStateName,
    pub(in crate::daemon) final_answer: Option<String>,
    pub(in crate::daemon) error: Option<String>,
    pub(in crate::daemon) completion_claim: Option<platonic_protocol::CompletionClaim>,
}

#[derive(Debug)]
pub(in crate::daemon) struct EventBuffer {
    pub(in crate::daemon) first_offset: u64,
    pub(in crate::daemon) next_offset: u64,
    pub(in crate::daemon) events: VecDeque<BufferedStreamEvent>,
}

#[derive(Clone, Debug)]
pub(in crate::daemon) struct PendingApproval {
    pub(in crate::daemon) session_id: String,
    pub(in crate::daemon) request: ApprovalRequest,
    pub(in crate::daemon) decision: Option<PendingApprovalDecision>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::daemon) struct PendingApprovalDecision {
    pub(in crate::daemon) decision: ApprovalDecision,
    pub(in crate::daemon) outcome: ExternalApprovalOutcome,
}

impl PendingApproval {
    pub(in crate::daemon) fn new(session_id: String, request: ApprovalRequest) -> Self {
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
    pub(in crate::daemon) fn new(run_id: String, session_id: String, ledger_path: PathBuf) -> Self {
        Self::new_inner(run_id, session_id, ledger_path, None)
    }

    pub(in crate::daemon) fn new_for_thread(
        run_id: String,
        session_id: String,
        ledger_path: PathBuf,
        turn: ThreadTurnBinding,
    ) -> Self {
        Self::new_inner(run_id, session_id, ledger_path, Some(turn))
    }

    fn new_inner(
        run_id: String,
        session_id: String,
        ledger_path: PathBuf,
        thread_turn: Option<ThreadTurnBinding>,
    ) -> Self {
        Self {
            session_id,
            run_id,
            ledger_path,
            cancel: Arc::new(AtomicBool::new(false)),
            status: Mutex::new(RunStatus {
                state: RunStateName::Running,
                final_answer: None,
                error: None,
                completion_claim: None,
            }),
            events: Mutex::new(EventBuffer {
                first_offset: 0,
                next_offset: 0,
                events: VecDeque::new(),
            }),
            approvals: Mutex::new(HashMap::new()),
            approval_changed: Condvar::new(),
            thread_turn,
            #[cfg(test)]
            event_snapshot_barriers: Mutex::new(None),
        }
    }

    pub(in crate::daemon) fn push_event(&self, event: StreamEvent) {
        let mut buffer = self.events.lock().expect("event buffer lock poisoned");
        if buffer.events.len() == MAX_EVENT_BUFFER {
            buffer.events.pop_front();
            buffer.first_offset += 1;
        }
        let offset = buffer.next_offset;
        buffer.next_offset += 1;
        buffer.events.push_back(BufferedStreamEvent {
            offset,
            event: event.clone(),
        });
        drop(buffer);
        if let Some(turn) = &self.thread_turn {
            turn.publish(event);
        }
    }

    pub(in crate::daemon) fn push_recorded_event(&self, record: RecordedEvent) {
        self.push_event(StreamEvent::Ledger { record });
    }

    pub(in crate::daemon) fn push_assistant_delta(&self, delta: AssistantDeltaEvent) {
        self.push_event(StreamEvent::AssistantDelta {
            run_id: delta.run_id.to_string(),
            turn_id: delta.turn_id.to_string(),
            step: delta.step,
            delta_index: delta.delta_index,
            text: delta.text,
        });
    }

    pub(in crate::daemon) fn status(&self) -> RunStatus {
        self.status
            .lock()
            .expect("run status lock poisoned")
            .clone()
    }

    pub(in crate::daemon) fn request_cancel(&self) -> Option<RunStateName> {
        self.request_cancel_after(|| Ok(()))
            .expect("infallible cancel")
    }

    pub(in crate::daemon) fn request_cancel_after(
        &self,
        before_first_accept: impl FnOnce() -> AppResult<()>,
    ) -> AppResult<Option<RunStateName>> {
        let mut approvals = self.approvals.lock().expect("approvals lock poisoned");
        let state = {
            let mut status = self.status.lock().expect("run status lock poisoned");
            match status.state {
                RunStateName::Running => {
                    before_first_accept()?;
                    status.state = RunStateName::CancelRequested;
                    self.cancel.store(true, Ordering::SeqCst);
                    self.push_event(StreamEvent::Canceled {
                        run_id: self.run_id.clone(),
                    });
                    approvals.retain(|_, pending| pending.decision.is_some());
                    self.approval_changed.notify_all();
                    RunStateName::CancelRequested
                }
                RunStateName::CancelRequested => RunStateName::CancelRequested,
                RunStateName::Finished
                | RunStateName::Failed
                | RunStateName::Canceled
                | RunStateName::Interrupted => return Ok(None),
            }
        };
        drop(approvals);
        Ok(Some(state))
    }

    pub(in crate::daemon) fn wait_for_terminal(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut approvals = self.approvals.lock().expect("approvals lock poisoned");
        loop {
            if !matches!(
                self.status().state,
                RunStateName::Running | RunStateName::CancelRequested
            ) {
                return true;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let (next, wait) = self
                .approval_changed
                .wait_timeout(approvals, deadline - now)
                .expect("approval condvar lock poisoned while stopping thread");
            approvals = next;
            if wait.timed_out()
                && matches!(
                    self.status().state,
                    RunStateName::Running | RunStateName::CancelRequested
                )
            {
                return false;
            }
        }
    }

    #[cfg(test)]
    pub(in crate::daemon) fn set_event_snapshot_barriers(
        &self,
        reached: Arc<Barrier>,
        release: Arc<Barrier>,
    ) {
        *self.event_snapshot_barriers.lock().unwrap() = Some((reached, release));
    }

    #[cfg(test)]
    pub(in crate::daemon) fn wait_during_event_snapshot(&self) {
        let barriers = self.event_snapshot_barriers.lock().unwrap().take();
        if let Some((reached, release)) = barriers {
            reached.wait();
            release.wait();
        }
    }

    pub(in crate::daemon) fn pending_approval(&self) -> Option<PendingApprovalSnapshot> {
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

pub(in crate::daemon) fn approval_handler(
    runtime: DaemonRuntime,
    record: Arc<RunRecord>,
    thread_yolo: bool,
) -> impl Fn(ApprovalRequest) -> AppResult<ExternalApprovalOutcome> + Send + Sync + 'static {
    move |request| {
        let call_id = request.call_id.to_string();
        let mut approvals = record.approvals.lock().expect("approvals lock poisoned");
        if request.run_id.as_str() != record.run_id {
            return Err(AppError::RunFailed(
                "approval request does not match daemon run".into(),
            ));
        }
        if request.credential_id.is_some()
            && (request.tool_name != SHELL_EXEC
                || request.effect != EffectClass::ExternalSideEffect
                || request.yolo_eligible)
        {
            return Err(AppError::RunFailed(
                "credential grant requires one explicitly approved shell.exec call".into(),
            ));
        }
        if record.cancel.load(Ordering::SeqCst) {
            return Ok(ExternalApprovalOutcome::Denied {
                actor: "daemon".into(),
                reason: "run canceled".into(),
            });
        }
        if record.status().state != RunStateName::Running {
            return Ok(ExternalApprovalOutcome::Denied {
                actor: "daemon".into(),
                reason: "run is no longer active".into(),
            });
        }
        if request.yolo_eligible && thread_yolo {
            return Ok(ExternalApprovalOutcome::Granted {
                actor: "yolo".into(),
                explicit: false,
            });
        }
        if request.yolo_eligible && runtime.session_yolo_enabled_for_decision(&record.session_id) {
            return Ok(ExternalApprovalOutcome::Granted {
                actor: "tui_yolo".into(),
                explicit: false,
            });
        }
        if request.tool_name == SHELL_EXEC
            && request.effect == EffectClass::ExternalSideEffect
            && request.credential_id.is_none()
            && runtime.has_shell_session_grant(&record.session_id)
        {
            return Ok(ExternalApprovalOutcome::Granted {
                actor: "session_grant".into(),
                explicit: false,
            });
        }
        // Record the ask before announcing it. A daemon that dies between the
        // announcement and the decision must still leave the question on disk,
        // or "approvals wait" holds only for one daemon lifetime (#435). If it
        // cannot be recorded it is denied: an unrecordable approval is one
        // nobody could answer after a restart.
        if let Err(error) = persist_approval_request(&runtime, &record, &request) {
            return Ok(ExternalApprovalOutcome::Denied {
                actor: "daemon".into(),
                reason: format!("approval could not be recorded: {error}"),
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
                let reason = "run canceled";
                record_daemon_denial(&runtime, &request, reason);
                return Ok(ExternalApprovalOutcome::Denied {
                    actor: "daemon".into(),
                    reason: reason.into(),
                });
            }
            if record.status().state != RunStateName::Running {
                approvals.remove(&call_id);
                let reason = "run is no longer active";
                record_daemon_denial(&runtime, &request, reason);
                return Ok(ExternalApprovalOutcome::Denied {
                    actor: "daemon".into(),
                    reason: reason.into(),
                });
            }
            approvals = record
                .approval_changed
                .wait(approvals)
                .expect("approval condvar lock poisoned");
        }
    }
}

/// Record that the daemon itself answered an approval nobody else could.
///
/// Best effort by design: the run's outcome is already decided in memory, and
/// a failed write leaves the approval visibly pending rather than silently
/// gone — which is the failure mode #435 exists to prevent.
fn record_daemon_denial(runtime: &DaemonRuntime, request: &ApprovalRequest, reason: &str) {
    let Ok(store) = runtime.paths.server_store() else {
        return;
    };
    let _ = store.resolve_tool_call_approval(
        request.run_id.as_str(),
        &request.call_id.to_string(),
        &crate::server_store::ToolCallApprovalDecision {
            granted: false,
            actor: "daemon".into(),
            reason: Some(reason.to_owned()),
            decided_at_ms: crate::thread_authority::now_ms(),
        },
    );
}

fn persist_approval_request(
    runtime: &DaemonRuntime,
    record: &RunRecord,
    request: &ApprovalRequest,
) -> AppResult<()> {
    runtime.paths.server_store()?.persist_tool_call_approval(
        &crate::server_store::ToolCallApprovalRecord {
            run_id: request.run_id.to_string(),
            call_id: request.call_id.to_string(),
            session_id: record.session_id.clone(),
            tool_name: request.tool_name.clone(),
            effect: request.effect.clone(),
            reason: request.reason.clone(),
            input_preview: request.input_preview.clone(),
            approval_preview: request.approval_preview.clone(),
            diff_preview: request.diff_preview.clone(),
            requested_at_ms: crate::thread_authority::now_ms(),
            decision: None,
        },
    )
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
        yolo_eligible: request.yolo_eligible,
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        ShutdownIfIdleDecision,
        tests::{run_record, runtime},
    };
    use super::*;
    use platonic_core::{EffectClass, RunId, ToolCallId};
    use std::{path::PathBuf, sync::Barrier, thread, time::Duration};

    fn yolo_request(run_id: &str, call_id: &str, eligible: bool) -> ApprovalRequest {
        ApprovalRequest {
            run_id: RunId::new(run_id).unwrap(),
            call_id: ToolCallId::new(call_id).unwrap(),
            tool_name: "file.write".into(),
            effect: EffectClass::WorkspaceWrite,
            reason: "file.write requires approval".into(),
            input_preview: Some(r#"{"path":"out.txt"}"#.into()),
            approval_preview: None,
            diff_preview: None,
            yolo_eligible: eligible,
            credential_id: None,
        }
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
                    yolo_eligible: false,
                    credential_id: None,
                },
            ),
        );
        runtime.reserve_run(approval_paused.clone()).unwrap();
        let cancel_requested = run_record(101);
        cancel_requested.status.lock().unwrap().state = RunStateName::CancelRequested;
        runtime.reserve_run(cancel_requested.clone()).unwrap();

        runtime.finish_run(&terminal_records[1], "done 1".into(), None);
        runtime.finish_run(&terminal_records[0], "done 0".into(), None);
        for (index, record) in terminal_records.iter().enumerate().skip(2) {
            runtime.finish_run(record, format!("done {index}"), None);
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
            yolo_eligible: true,
            credential_id: None,
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
            yolo_eligible: true,
            credential_id: None,
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
            yolo_eligible: true,
            credential_id: None,
        }))
        .unwrap();

        assert_eq!(
            event["approval_preview"],
            "command: cargo test\ncwd: /tmp/work"
        );
    }
    #[test]
    fn yolo_eligible_approval_is_granted_without_pending_state() {
        let runtime = runtime();
        runtime.set_approval_profile("session_1", ApprovalProfile::Yolo);
        let record = run_record(1);
        let outcome =
            approval_handler(runtime, record.clone(), false)(yolo_request("run_1", "call_1", true))
                .unwrap();

        assert_eq!(
            outcome,
            ExternalApprovalOutcome::Granted {
                actor: "tui_yolo".into(),
                explicit: false,
            }
        );
        assert!(record.approvals.lock().unwrap().is_empty());
        assert!(record.events.lock().unwrap().events.is_empty());
    }

    #[test]
    fn thread_yolo_grants_only_eligible_approval_requests() {
        let runtime = runtime();
        let granted_record = run_record(1);
        let outcome = approval_handler(runtime.clone(), granted_record.clone(), true)(
            yolo_request("run_1", "call_1", true),
        )
        .unwrap();

        assert_eq!(
            outcome,
            ExternalApprovalOutcome::Granted {
                actor: "yolo".into(),
                explicit: false,
            }
        );
        assert!(granted_record.approvals.lock().unwrap().is_empty());
        assert!(granted_record.events.lock().unwrap().events.is_empty());

        let pending_record = run_record(2);
        let decide = approval_handler(runtime, pending_record.clone(), true);
        let worker = thread::spawn(move || decide(yolo_request("run_2", "call_2", false)).unwrap());
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while pending_record.pending_approval().is_none() {
            assert!(
                std::time::Instant::now() < deadline,
                "ineligible request did not become pending"
            );
            thread::yield_now();
        }
        let mut approvals = pending_record.approvals.lock().unwrap();
        approvals.get_mut("call_2").unwrap().decision = Some(PendingApprovalDecision {
            decision: ApprovalDecision::Deny,
            outcome: ExternalApprovalOutcome::Denied {
                actor: "tester".into(),
                reason: "test complete".into(),
            },
        });
        pending_record.approval_changed.notify_all();
        drop(approvals);

        assert_eq!(
            worker.join().unwrap(),
            ExternalApprovalOutcome::Denied {
                actor: "tester".into(),
                reason: "test complete".into()
            }
        );
    }

    #[test]
    fn credential_request_bypasses_neither_yolo_nor_session_grant() {
        let runtime = runtime();
        runtime.set_approval_profile("session_1", ApprovalProfile::Yolo);
        runtime.install_shell_session_grant("session_1");
        let record = run_record(1);
        let mut request = yolo_request("run_1", "call_1", false);
        request.tool_name = SHELL_EXEC.into();
        request.effect = EffectClass::ExternalSideEffect;
        request.credential_id = Some("github".into());
        let decide = approval_handler(runtime, record.clone(), true);
        let worker = thread::spawn(move || decide(request).unwrap());
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while record.pending_approval().is_none() {
            assert!(std::time::Instant::now() < deadline);
            thread::yield_now();
        }

        assert_eq!(record.request_cancel(), Some(RunStateName::CancelRequested));
        assert!(matches!(
            worker.join().unwrap(),
            ExternalApprovalOutcome::Denied { actor, reason }
                if actor == "daemon" && reason == "run canceled"
        ));
    }

    #[test]
    fn profile_toggle_after_yolo_decision_does_not_revoke_the_grant() {
        let runtime = runtime();
        runtime.set_approval_profile("session_1", ApprovalProfile::Yolo);
        let reached = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        runtime.set_approval_profile_decision_barriers(reached.clone(), release.clone());
        let record = run_record(1);
        let decide = approval_handler(runtime.clone(), record, false);
        let worker = thread::spawn(move || decide(yolo_request("run_1", "call_1", true)).unwrap());

        reached.wait();
        let toggled_runtime = runtime.clone();
        let toggle = thread::spawn(move || {
            toggled_runtime.set_approval_profile("session_1", ApprovalProfile::Prompt);
        });
        release.wait();

        assert_eq!(
            worker.join().unwrap(),
            ExternalApprovalOutcome::Granted {
                actor: "tui_yolo".into(),
                explicit: false,
            }
        );
        toggle.join().unwrap();
        assert_eq!(
            runtime.approval_profile("session_1"),
            ApprovalProfile::Prompt
        );
    }

    #[test]
    fn profile_toggle_after_prompt_decision_does_not_grant_the_pending_ask() {
        let runtime = runtime();
        let reached = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        runtime.set_approval_profile_decision_barriers(reached.clone(), release.clone());
        let record = run_record(1);
        let decide = approval_handler(runtime.clone(), record.clone(), false);
        let worker = thread::spawn(move || decide(yolo_request("run_1", "call_1", true)).unwrap());

        reached.wait();
        let toggled_runtime = runtime.clone();
        let toggle = thread::spawn(move || {
            toggled_runtime.set_approval_profile("session_1", ApprovalProfile::Yolo);
        });
        release.wait();
        toggle.join().unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while record.events.lock().unwrap().events.is_empty() {
            assert!(
                std::time::Instant::now() < deadline,
                "approval event was not published"
            );
            thread::yield_now();
        }
        let mut approvals = record.approvals.lock().unwrap();
        approvals.get_mut("call_1").unwrap().decision = Some(PendingApprovalDecision {
            decision: ApprovalDecision::Deny,
            outcome: ExternalApprovalOutcome::Denied {
                actor: "tester".into(),
                reason: "test complete".into(),
            },
        });
        record.approval_changed.notify_all();
        drop(approvals);

        assert_eq!(
            worker.join().unwrap(),
            ExternalApprovalOutcome::Denied {
                actor: "tester".into(),
                reason: "test complete".into()
            }
        );
        assert_eq!(runtime.approval_profile("session_1"), ApprovalProfile::Yolo);
    }

    #[test]
    fn yolo_decision_linearizes_before_concurrent_cancel() {
        let runtime = runtime();
        runtime.set_approval_profile("session_1", ApprovalProfile::Yolo);
        let reached = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        runtime.set_approval_profile_decision_barriers(reached.clone(), release.clone());
        let record = run_record(1);
        let decide = approval_handler(runtime, record.clone(), false);
        let decision =
            thread::spawn(move || decide(yolo_request("run_1", "call_1", true)).unwrap());

        reached.wait();
        let canceled_record = record.clone();
        let (canceled_sender, canceled_receiver) = std::sync::mpsc::channel();
        let cancel = thread::spawn(move || {
            canceled_sender
                .send(canceled_record.request_cancel())
                .unwrap();
        });
        assert!(matches!(
            canceled_receiver.recv_timeout(Duration::from_millis(100)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));
        release.wait();

        assert_eq!(
            decision.join().unwrap(),
            ExternalApprovalOutcome::Granted {
                actor: "tui_yolo".into(),
                explicit: false,
            }
        );
        assert_eq!(
            canceled_receiver.recv_timeout(Duration::from_secs(1)),
            Ok(Some(RunStateName::CancelRequested))
        );
        cancel.join().unwrap();
    }

    #[test]
    fn yolo_decision_linearizes_before_concurrent_terminal_transition() {
        let runtime = runtime();
        runtime.set_approval_profile("session_1", ApprovalProfile::Yolo);
        let reached = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        runtime.set_approval_profile_decision_barriers(reached.clone(), release.clone());
        let record = run_record(1);
        let decide = approval_handler(runtime.clone(), record.clone(), false);
        let decision =
            thread::spawn(move || decide(yolo_request("run_1", "call_1", true)).unwrap());

        reached.wait();
        let finished_runtime = runtime.clone();
        let finished_record = record.clone();
        let (finished_sender, finished_receiver) = std::sync::mpsc::channel();
        let finish = thread::spawn(move || {
            finished_runtime.finish_run(&finished_record, "done".into(), None);
            finished_sender.send(()).unwrap();
        });
        assert!(matches!(
            finished_receiver.recv_timeout(Duration::from_millis(100)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));
        release.wait();

        assert_eq!(
            decision.join().unwrap(),
            ExternalApprovalOutcome::Granted {
                actor: "tui_yolo".into(),
                explicit: false,
            }
        );
        finished_receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        finish.join().unwrap();
        assert_eq!(record.status().state, RunStateName::Finished);
    }

    #[test]
    fn terminal_run_does_not_yolo_grant_a_late_approval() {
        let runtime = runtime();
        runtime.set_approval_profile("session_1", ApprovalProfile::Yolo);
        let record = run_record(1);
        record.status.lock().unwrap().state = RunStateName::Finished;

        let outcome =
            approval_handler(runtime, record.clone(), false)(yolo_request("run_1", "call_1", true))
                .unwrap();

        assert_eq!(
            outcome,
            ExternalApprovalOutcome::Denied {
                actor: "daemon".into(),
                reason: "run is no longer active".into()
            }
        );
        assert!(record.approvals.lock().unwrap().is_empty());
    }

    #[test]
    fn canceled_run_does_not_register_or_publish_a_late_approval() {
        let record = Arc::new(RunRecord::new(
            "run_1".into(),
            "session_1".into(),
            PathBuf::from("/tmp/agent.db"),
        ));
        record.cancel.store(true, Ordering::SeqCst);
        let decide = approval_handler(runtime(), record.clone(), false);

        let outcome = decide(ApprovalRequest {
            run_id: RunId::new("run_1").unwrap(),
            call_id: ToolCallId::new("call_1").unwrap(),
            tool_name: "file.write".into(),
            effect: EffectClass::WorkspaceWrite,
            reason: "file.write requires approval".into(),
            input_preview: Some(r#"{"path":"out.txt"}"#.into()),
            approval_preview: None,
            diff_preview: None,
            yolo_eligible: true,
            credential_id: None,
        })
        .unwrap();

        assert_eq!(
            outcome,
            ExternalApprovalOutcome::Denied {
                actor: "daemon".into(),
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
        let decide = approval_handler(runtime(), record.clone(), false);
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
                yolo_eligible: true,
                credential_id: None,
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
                actor: "daemon".into(),
                reason: "test complete".into(),
            },
        });
        record.approval_changed.notify_all();
        drop(approvals);

        assert_eq!(
            worker.join().unwrap(),
            ExternalApprovalOutcome::Denied {
                actor: "daemon".into(),
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
