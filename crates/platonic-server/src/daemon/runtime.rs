use crate::{
    AppError, AppResult, ApprovalRequest, AssistantDeltaEvent,
    app::ExternalApprovalOutcome,
    daemon::{
        DaemonPaths,
        protocol::{
            ApprovalDecision, BufferedStreamEvent, BufferedThreadEvent, PendingApprovalSnapshot,
            RunStateName, StreamEvent, ThreadEventsResult, ThreadLiveState,
            ThreadSendRejectedReason, ThreadSendResult,
        },
    },
    server_store::DurableThreadAuthority,
    thread_authority::ThreadAuthorityDraft,
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
    time::{Duration, Instant},
};

pub(super) const MAX_EVENT_BUFFER: usize = 256;
pub(super) const MAX_TERMINAL_RUNS: usize = 32;
pub(super) const MAX_THREAD_EVENT_BUFFER: usize = 256;
const MAX_PENDING_THREAD_STEERS: usize = 32;

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
    live_threads: HashMap<String, Arc<LiveThread>>,
    active_thread_runs: HashMap<String, Arc<RunRecord>>,
    stopping_threads: HashSet<String>,
    stopped_threads: HashSet<String>,
    pending_thread_spawns: HashMap<String, PendingThreadSpawn>,
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
pub(super) enum ThreadSpawnAdmissionError {
    ShuttingDown,
    Duplicate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ThreadSpawnClaimError {
    NotFound,
    WrongWorkspace,
    DecisionInProgress,
}

#[derive(Clone, Debug)]
pub(super) enum ThreadSendAdmission {
    ShuttingDown,
    Stopped,
    Started {
        receipt: ThreadSendResult,
        turn: ThreadTurnBinding,
    },
    Steered {
        receipt: ThreadSendResult,
    },
    Rejected {
        receipt: ThreadSendResult,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ThreadEventsError {
    Lagged { first_offset: u64 },
    Stopped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ThreadRunBindError {
    NotLoaded,
    Stopping,
    RunActive,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ThreadStopError {
    InProgress,
    AlreadyStopped,
}

#[derive(Clone, Debug)]
pub(super) struct ThreadStopTarget {
    pub(super) turn_id: Option<String>,
    pub(super) run: Option<Arc<RunRecord>>,
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
        Self::new_shared(
            paths,
            Instant::now(),
            Arc::new(Mutex::new(RuntimeState::default())),
            Arc::new(AtomicBool::new(false)),
        )
    }

    pub(super) fn new_shared(
        paths: DaemonPaths,
        started_at: Instant,
        state: Arc<Mutex<RuntimeState>>,
        stop_requested: Arc<AtomicBool>,
    ) -> Self {
        Self {
            paths,
            started_at,
            state,
            session_tool_grants: Arc::new(Mutex::new(HashSet::new())),
            stop_requested,
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

    pub(super) fn reserve_thread_spawn(
        &self,
        spawn_id: String,
        draft: ThreadAuthorityDraft,
    ) -> Result<(), ThreadSpawnAdmissionError> {
        let mut state = self.state.lock().expect("runtime state lock poisoned");
        if state.shutdown_accepted {
            return Err(ThreadSpawnAdmissionError::ShuttingDown);
        }
        if state.pending_thread_spawns.contains_key(&spawn_id)
            || state.live_threads.contains_key(&draft.thread_id)
            || state
                .pending_thread_spawns
                .values()
                .any(|pending| pending.draft.thread_id == draft.thread_id)
        {
            return Err(ThreadSpawnAdmissionError::Duplicate);
        }
        state.pending_thread_spawns.insert(
            spawn_id.clone(),
            PendingThreadSpawn {
                spawn_id,
                workspace_id: self.paths.workspace_id.clone(),
                draft,
                decision_in_progress: false,
            },
        );
        Ok(())
    }

    pub(super) fn claim_thread_spawn(
        &self,
        spawn_id: &str,
    ) -> Result<PendingThreadSpawn, ThreadSpawnClaimError> {
        let mut state = self.state.lock().expect("runtime state lock poisoned");
        let pending = state
            .pending_thread_spawns
            .get_mut(spawn_id)
            .ok_or(ThreadSpawnClaimError::NotFound)?;
        if pending.workspace_id != self.paths.workspace_id {
            return Err(ThreadSpawnClaimError::WrongWorkspace);
        }
        if pending.decision_in_progress {
            return Err(ThreadSpawnClaimError::DecisionInProgress);
        }
        pending.decision_in_progress = true;
        Ok(pending.clone())
    }

    pub(super) fn release_thread_spawn_claim(&self, spawn_id: &str) {
        let mut state = self.state.lock().expect("runtime state lock poisoned");
        if let Some(pending) = state.pending_thread_spawns.get_mut(spawn_id) {
            pending.decision_in_progress = false;
        }
    }

    pub(super) fn complete_thread_spawn(&self, spawn_id: &str, durable: DurableThreadAuthority) {
        let record = durable.record();
        let mut state = self.state.lock().expect("runtime state lock poisoned");
        let pending = state
            .pending_thread_spawns
            .remove(spawn_id)
            .expect("durable thread spawn retains its runtime reservation");
        debug_assert_eq!(pending.workspace_id, self.paths.workspace_id);
        debug_assert_eq!(pending.draft.thread_id, record.thread_id);
        state.live_threads.insert(
            record.thread_id.clone(),
            Arc::new(LiveThread::new(
                self.paths.workspace_id.clone(),
                record.created_at_ms,
            )),
        );
    }

    pub(super) fn complete_thread_spawn_without_authority(&self, spawn_id: &str) {
        let mut state = self.state.lock().expect("runtime state lock poisoned");
        state.pending_thread_spawns.remove(spawn_id);
    }

    pub(super) fn thread_is_loaded(&self, thread_id: &str) -> bool {
        let state = self.state.lock().expect("runtime state lock poisoned");
        !state.stopping_threads.contains(thread_id)
            && !state.stopped_threads.contains(thread_id)
            && state
                .live_threads
                .get(thread_id)
                .is_some_and(|thread| thread.workspace_id == self.paths.workspace_id)
    }

    pub(super) fn load_thread(
        &self,
        thread_id: &str,
    ) -> Result<Arc<LiveThread>, ThreadEventsError> {
        let mut state = self.state.lock().expect("runtime state lock poisoned");
        if state.stopped_threads.contains(thread_id) {
            return Err(ThreadEventsError::Stopped);
        }
        if let Some(thread) = state
            .live_threads
            .get(thread_id)
            .filter(|thread| thread.workspace_id == self.paths.workspace_id)
        {
            return Ok(Arc::clone(thread));
        }
        if state.stopping_threads.contains(thread_id) {
            return Err(ThreadEventsError::Stopped);
        }
        let thread = Arc::new(LiveThread::new(
            self.paths.workspace_id.clone(),
            crate::thread_authority::now_ms(),
        ));
        state
            .live_threads
            .insert(thread_id.to_owned(), Arc::clone(&thread));
        Ok(thread)
    }

    pub(super) fn send_thread(
        &self,
        thread_id: &str,
        controller_id: String,
        expected_turn_id: Option<&str>,
        message: String,
        new_turn_id: String,
    ) -> ThreadSendAdmission {
        let mut state = self.state.lock().expect("runtime state lock poisoned");
        if state.shutdown_accepted {
            return ThreadSendAdmission::ShuttingDown;
        }
        if state.stopping_threads.contains(thread_id) || state.stopped_threads.contains(thread_id) {
            return ThreadSendAdmission::Stopped;
        }
        let thread = state
            .live_threads
            .get(thread_id)
            .filter(|thread| thread.workspace_id == self.paths.workspace_id)
            .cloned();
        let thread = match thread {
            Some(thread) => thread,
            None if expected_turn_id.is_some() => {
                return ThreadSendAdmission::Rejected {
                    receipt: ThreadSendResult::Rejected {
                        thread_id: thread_id.into(),
                        turn_id: None,
                        reason: ThreadSendRejectedReason::TurnMismatch,
                    },
                };
            }
            None => {
                let thread = Arc::new(LiveThread::new(
                    self.paths.workspace_id.clone(),
                    crate::thread_authority::now_ms(),
                ));
                state
                    .live_threads
                    .insert(thread_id.to_owned(), Arc::clone(&thread));
                thread
            }
        };
        thread.send(
            thread_id,
            controller_id,
            expected_turn_id,
            message,
            new_turn_id,
        )
    }

    pub(super) fn thread_events(
        &self,
        thread_id: &str,
        from_offset: Option<u64>,
        limit: usize,
        wait: Duration,
    ) -> Result<ThreadEventsResult, ThreadEventsError> {
        self.load_thread(thread_id)?
            .events(thread_id, from_offset, limit, wait)
    }

    pub(super) fn bind_thread_run(
        &self,
        turn: &ThreadTurnBinding,
        record: Arc<RunRecord>,
    ) -> Result<(), ThreadRunBindError> {
        loop {
            let mut state = self.state.lock().expect("runtime state lock poisoned");
            if state.stopped_threads.contains(&turn.thread_id) {
                return Err(ThreadRunBindError::Stopping);
            }
            let thread = state
                .live_threads
                .get(&turn.thread_id)
                .filter(|thread| {
                    thread.workspace_id == self.paths.workspace_id
                        && Arc::ptr_eq(thread, &turn.thread)
                })
                .cloned()
                .ok_or(ThreadRunBindError::NotLoaded)?;
            if state.stopping_threads.contains(&turn.thread_id) {
                drop(state);
                if thread.wait_for_stop_resolution() == LiveThreadLifecycle::Running {
                    continue;
                }
                return Err(ThreadRunBindError::Stopping);
            }
            if state
                .active_thread_runs
                .get(&turn.thread_id)
                .is_some_and(|active| {
                    matches!(
                        active.status().state,
                        RunStateName::Running | RunStateName::CancelRequested
                    )
                })
            {
                return Err(ThreadRunBindError::RunActive);
            }
            thread.bind_run(&turn.turn_id)?;
            state
                .active_thread_runs
                .insert(turn.thread_id.clone(), record);
            return Ok(());
        }
    }

    pub(super) fn release_run_reservation(&self, record: &RunRecord) {
        let mut state = self.state.lock().expect("runtime state lock poisoned");
        if state
            .runs
            .get(&record.run_id)
            .is_some_and(|reserved| std::ptr::eq(Arc::as_ptr(reserved), record))
        {
            state.runs.remove(&record.run_id);
        }
    }

    pub(super) fn begin_thread_stop(
        &self,
        thread_id: &str,
    ) -> Result<ThreadStopTarget, ThreadStopError> {
        let mut state = self.state.lock().expect("runtime state lock poisoned");
        if state.stopped_threads.contains(thread_id) {
            return Err(ThreadStopError::AlreadyStopped);
        }
        if !state.stopping_threads.insert(thread_id.into()) {
            return Err(ThreadStopError::InProgress);
        }
        let turn_id = state
            .live_threads
            .get(thread_id)
            .filter(|thread| thread.workspace_id == self.paths.workspace_id)
            .and_then(|thread| thread.begin_stop());
        let run = state.active_thread_runs.get(thread_id).cloned();
        Ok(ThreadStopTarget { turn_id, run })
    }

    pub(super) fn complete_thread_stop(&self, thread_id: &str) {
        let mut state = self.state.lock().expect("runtime state lock poisoned");
        if let Some(thread) = state.live_threads.get(thread_id) {
            thread.complete_stop();
        }
        state.live_threads.remove(thread_id);
        state.active_thread_runs.remove(thread_id);
        state.stopping_threads.remove(thread_id);
        state.stopped_threads.insert(thread_id.into());
    }

    pub(super) fn abort_thread_stop(&self, thread_id: &str) {
        let mut state = self.state.lock().expect("runtime state lock poisoned");
        if let Some(thread) = state.live_threads.get(thread_id) {
            thread.abort_stop();
        }
        state.stopping_threads.remove(thread_id);
    }

    #[cfg(test)]
    pub(super) fn thread_is_stopped(&self, thread_id: &str) -> bool {
        self.state
            .lock()
            .expect("runtime state lock poisoned")
            .stopped_threads
            .contains(thread_id)
    }

    pub(super) fn next_thread_message(&self, turn: &ThreadTurnBinding) -> Option<String> {
        turn.thread.next_message_or_finish(&turn.turn_id)
    }

    pub(super) fn abort_thread_turn(&self, turn: &ThreadTurnBinding) {
        turn.thread.abort(&turn.turn_id);
    }

    pub(super) fn thread_live_state(&self, thread_id: &str) -> ThreadLiveState {
        let state = self.state.lock().expect("runtime state lock poisoned");
        match state
            .live_threads
            .get(thread_id)
            .filter(|thread| thread.workspace_id == self.paths.workspace_id)
        {
            Some(thread) => {
                let (current_turn_id, last_activity_at_ms) = thread.live_snapshot();
                ThreadLiveState {
                    loaded: true,
                    current_turn_id,
                    last_activity_at_ms: Some(last_activity_at_ms),
                }
            }
            None => ThreadLiveState {
                loaded: false,
                current_turn_id: None,
                last_activity_at_ms: None,
            },
        }
    }

    #[cfg(test)]
    pub(super) fn note_thread_activity_at(&self, thread_id: &str, activity_at_ms: u64) {
        let state = self.state.lock().expect("runtime state lock poisoned");
        if state.stopping_threads.contains(thread_id) || state.stopped_threads.contains(thread_id) {
            return;
        }
        if let Some(thread) = state
            .live_threads
            .get(thread_id)
            .filter(|thread| thread.workspace_id == self.paths.workspace_id)
        {
            thread.note_activity_at(activity_at_ms);
        }
    }

    pub(super) fn finish_run(
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
            || !state.pending_thread_spawns.is_empty()
            || !state.stopping_threads.is_empty()
            || state.live_threads.values().any(|thread| {
                thread.workspace_id == self.paths.workspace_id && thread.current_turn_id().is_some()
            })
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

#[derive(Clone, Debug)]
pub(super) struct PendingThreadSpawn {
    pub(super) spawn_id: String,
    pub(super) workspace_id: String,
    pub(super) draft: ThreadAuthorityDraft,
    decision_in_progress: bool,
}

#[derive(Debug)]
pub(super) struct LiveThread {
    workspace_id: String,
    state: Mutex<LiveThreadState>,
    changed: Condvar,
}

#[derive(Debug)]
struct LiveThreadState {
    lifecycle: LiveThreadLifecycle,
    current_turn: Option<ActiveThreadTurn>,
    last_activity_at_ms: u64,
    first_offset: u64,
    next_offset: u64,
    events: VecDeque<BufferedThreadEvent>,
    observers: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LiveThreadLifecycle {
    Running,
    Stopping,
    Stopped,
}

#[derive(Debug)]
struct ActiveThreadTurn {
    turn_id: String,
    controller_id: String,
    pending_messages: VecDeque<String>,
}

#[derive(Clone, Debug)]
pub(super) struct ThreadTurnBinding {
    pub(super) thread_id: String,
    pub(super) turn_id: String,
    thread: Arc<LiveThread>,
}

impl LiveThread {
    fn new(workspace_id: String, last_activity_at_ms: u64) -> Self {
        Self {
            workspace_id,
            state: Mutex::new(LiveThreadState {
                lifecycle: LiveThreadLifecycle::Running,
                current_turn: None,
                last_activity_at_ms,
                first_offset: 0,
                next_offset: 0,
                events: VecDeque::new(),
                observers: 0,
            }),
            changed: Condvar::new(),
        }
    }

    fn current_turn_id(&self) -> Option<String> {
        self.state
            .lock()
            .expect("live thread lock poisoned")
            .current_turn
            .as_ref()
            .map(|turn| turn.turn_id.clone())
    }

    fn live_snapshot(&self) -> (Option<String>, u64) {
        let state = self.state.lock().expect("live thread lock poisoned");
        (
            state.current_turn.as_ref().map(|turn| turn.turn_id.clone()),
            state.last_activity_at_ms,
        )
    }

    #[cfg(test)]
    fn note_activity_at(&self, activity_at_ms: u64) {
        let mut state = self.state.lock().expect("live thread lock poisoned");
        state.last_activity_at_ms = state.last_activity_at_ms.max(activity_at_ms);
    }

    fn bind_run(&self, turn_id: &str) -> Result<(), ThreadRunBindError> {
        let mut state = self.state.lock().expect("live thread lock poisoned");
        if state.lifecycle != LiveThreadLifecycle::Running {
            return Err(ThreadRunBindError::Stopping);
        }
        if state
            .current_turn
            .as_ref()
            .is_none_or(|turn| turn.turn_id != turn_id)
        {
            return Err(ThreadRunBindError::NotLoaded);
        }
        state.last_activity_at_ms = state
            .last_activity_at_ms
            .max(crate::thread_authority::now_ms());
        Ok(())
    }

    fn send(
        self: &Arc<Self>,
        thread_id: &str,
        controller_id: String,
        expected_turn_id: Option<&str>,
        message: String,
        new_turn_id: String,
    ) -> ThreadSendAdmission {
        let mut state = self.state.lock().expect("live thread lock poisoned");
        if state.lifecycle != LiveThreadLifecycle::Running {
            return ThreadSendAdmission::Stopped;
        }
        if let Some(active) = state.current_turn.as_mut() {
            if active.controller_id != controller_id {
                return ThreadSendAdmission::Rejected {
                    receipt: ThreadSendResult::Rejected {
                        thread_id: thread_id.into(),
                        turn_id: Some(active.turn_id.clone()),
                        reason: ThreadSendRejectedReason::ControllerOwned,
                    },
                };
            }
            if expected_turn_id != Some(active.turn_id.as_str()) {
                return ThreadSendAdmission::Rejected {
                    receipt: ThreadSendResult::Rejected {
                        thread_id: thread_id.into(),
                        turn_id: Some(active.turn_id.clone()),
                        reason: ThreadSendRejectedReason::TurnMismatch,
                    },
                };
            }
            if active.pending_messages.len() >= MAX_PENDING_THREAD_STEERS {
                return ThreadSendAdmission::Rejected {
                    receipt: ThreadSendResult::Rejected {
                        thread_id: thread_id.into(),
                        turn_id: Some(active.turn_id.clone()),
                        reason: ThreadSendRejectedReason::QueueFull,
                    },
                };
            }
            active.pending_messages.push_back(message);
            let turn_id = active.turn_id.clone();
            state.last_activity_at_ms = state
                .last_activity_at_ms
                .max(crate::thread_authority::now_ms());
            let receipt = ThreadSendResult::Steered {
                thread_id: thread_id.into(),
                turn_id,
            };
            self.changed.notify_all();
            return ThreadSendAdmission::Steered { receipt };
        }

        if expected_turn_id.is_some() {
            return ThreadSendAdmission::Rejected {
                receipt: ThreadSendResult::Rejected {
                    thread_id: thread_id.into(),
                    turn_id: None,
                    reason: ThreadSendRejectedReason::TurnMismatch,
                },
            };
        }
        state.current_turn = Some(ActiveThreadTurn {
            turn_id: new_turn_id.clone(),
            controller_id,
            pending_messages: VecDeque::new(),
        });
        state.last_activity_at_ms = state
            .last_activity_at_ms
            .max(crate::thread_authority::now_ms());
        self.changed.notify_all();
        ThreadSendAdmission::Started {
            receipt: ThreadSendResult::Started {
                thread_id: thread_id.into(),
                turn_id: new_turn_id.clone(),
            },
            turn: ThreadTurnBinding {
                thread_id: thread_id.into(),
                turn_id: new_turn_id,
                thread: Arc::clone(self),
            },
        }
    }

    fn publish(&self, turn_id: &str, event: StreamEvent) {
        let mut state = self.state.lock().expect("live thread lock poisoned");
        if state.lifecycle == LiveThreadLifecycle::Stopped {
            return;
        }
        if state.events.len() == MAX_THREAD_EVENT_BUFFER {
            state.events.pop_front();
            state.first_offset += 1;
        }
        let offset = state.next_offset;
        state.next_offset += 1;
        state.events.push_back(BufferedThreadEvent {
            offset,
            turn_id: turn_id.into(),
            event,
        });
        state.last_activity_at_ms = state
            .last_activity_at_ms
            .max(crate::thread_authority::now_ms());
        self.changed.notify_all();
    }

    fn events(
        &self,
        thread_id: &str,
        from_offset: Option<u64>,
        limit: usize,
        wait: Duration,
    ) -> Result<ThreadEventsResult, ThreadEventsError> {
        let mut state = self.state.lock().expect("live thread lock poisoned");
        state.observers += 1;
        let from_offset = from_offset.unwrap_or(state.next_offset);
        if from_offset < state.first_offset {
            let first_offset = state.first_offset;
            state.observers -= 1;
            return Err(ThreadEventsError::Lagged { first_offset });
        }
        if from_offset >= state.next_offset && !wait.is_zero() {
            let (next, _) = self
                .changed
                .wait_timeout(state, wait)
                .expect("live thread lock poisoned while observing");
            state = next;
        }
        if from_offset < state.first_offset {
            let first_offset = state.first_offset;
            state.observers -= 1;
            return Err(ThreadEventsError::Lagged { first_offset });
        }
        let start = (from_offset - state.first_offset) as usize;
        let events = state
            .events
            .iter()
            .skip(start)
            .take(limit)
            .cloned()
            .collect::<Vec<_>>();
        let result = ThreadEventsResult {
            thread_id: thread_id.into(),
            from_offset,
            next_offset: from_offset + events.len() as u64,
            current_turn_id: state.current_turn.as_ref().map(|turn| turn.turn_id.clone()),
            events,
        };
        state.observers -= 1;
        Ok(result)
    }

    fn next_message_or_finish(&self, turn_id: &str) -> Option<String> {
        let mut state = self.state.lock().expect("live thread lock poisoned");
        while state.lifecycle == LiveThreadLifecycle::Stopping {
            state = self
                .changed
                .wait(state)
                .expect("live thread lock poisoned while stopping");
        }
        if state.lifecycle == LiveThreadLifecycle::Stopped {
            return None;
        }
        let active = state
            .current_turn
            .as_mut()
            .filter(|active| active.turn_id == turn_id)?;
        if let Some(message) = active.pending_messages.pop_front() {
            return Some(message);
        }
        state.current_turn = None;
        self.changed.notify_all();
        None
    }

    fn begin_stop(&self) -> Option<String> {
        let mut state = self.state.lock().expect("live thread lock poisoned");
        state.lifecycle = LiveThreadLifecycle::Stopping;
        state.current_turn.as_ref().map(|turn| turn.turn_id.clone())
    }

    fn complete_stop(&self) {
        let mut state = self.state.lock().expect("live thread lock poisoned");
        state.lifecycle = LiveThreadLifecycle::Stopped;
        state.current_turn = None;
        self.changed.notify_all();
    }

    fn abort_stop(&self) {
        let mut state = self.state.lock().expect("live thread lock poisoned");
        if state.lifecycle == LiveThreadLifecycle::Stopping {
            state.lifecycle = LiveThreadLifecycle::Running;
            self.changed.notify_all();
        }
    }

    fn wait_for_stop_resolution(&self) -> LiveThreadLifecycle {
        let mut state = self.state.lock().expect("live thread lock poisoned");
        while state.lifecycle == LiveThreadLifecycle::Stopping {
            state = self
                .changed
                .wait(state)
                .expect("live thread lock poisoned while awaiting stop resolution");
        }
        state.lifecycle
    }

    fn abort(&self, turn_id: &str) {
        let mut state = self.state.lock().expect("live thread lock poisoned");
        if state.lifecycle == LiveThreadLifecycle::Running
            && state
                .current_turn
                .as_ref()
                .is_some_and(|active| active.turn_id == turn_id)
        {
            state.current_turn = None;
            self.changed.notify_all();
        }
    }

    #[cfg(test)]
    fn observer_count(&self) -> usize {
        self.state
            .lock()
            .expect("live thread lock poisoned")
            .observers
    }
}

impl ThreadTurnBinding {
    fn publish(&self, event: StreamEvent) {
        self.thread.publish(&self.turn_id, event);
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
    thread_turn: Option<ThreadTurnBinding>,
    #[cfg(test)]
    event_snapshot_barriers: Mutex<Option<(Arc<Barrier>, Arc<Barrier>)>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RunStatus {
    pub(super) state: RunStateName,
    pub(super) final_answer: Option<String>,
    pub(super) error: Option<String>,
    pub(super) completion_claim: Option<platonic_protocol::CompletionClaim>,
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
        Self::new_inner(run_id, session_id, ledger_path, None)
    }

    pub(super) fn new_for_thread(
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

    pub(super) fn push_event(&self, event: StreamEvent) {
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

    pub(super) fn request_cancel(&self) -> Option<RunStateName> {
        let mut approvals = self.approvals.lock().expect("approvals lock poisoned");
        let state = {
            let mut status = self.status.lock().expect("run status lock poisoned");
            match status.state {
                RunStateName::Running => {
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
                | RunStateName::Interrupted => return None,
            }
        };
        drop(approvals);
        Some(state)
    }

    pub(super) fn wait_for_terminal(&self, timeout: Duration) -> bool {
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
        // Record the ask before announcing it. A daemon that dies between the
        // announcement and the decision must still leave the question on disk,
        // or "approvals wait" holds only for one daemon lifetime (#435). If it
        // cannot be recorded it is denied: an unrecordable approval is one
        // nobody could answer after a restart.
        if let Err(error) = persist_approval_request(&runtime, &record, &request) {
            return Ok(ExternalApprovalOutcome::Denied {
                actor: "daemon",
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
                    actor: "daemon",
                    reason: reason.into(),
                });
            }
            if record.status().state != RunStateName::Running {
                approvals.remove(&call_id);
                let reason = "run is no longer active";
                record_daemon_denial(&runtime, &request, reason);
                return Ok(ExternalApprovalOutcome::Denied {
                    actor: "daemon",
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
            server_db_path: PathBuf::from("/tmp/platonic-server.db"),
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
    fn pending_thread_spawn_blocks_shutdown_until_resolved() {
        let root = tempfile::tempdir().unwrap();
        let runtime = runtime();
        let draft = ThreadAuthorityDraft::new(
            None,
            root.path(),
            "gpt-5.6-sol".into(),
            crate::daemon::protocol::ReasoningEffort::Xhigh,
            crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
        )
        .unwrap();
        runtime
            .reserve_thread_spawn("spawn_1".into(), draft)
            .unwrap();

        assert_eq!(
            runtime.shutdown_if_idle(),
            ShutdownIfIdleDecision::RefusedActive
        );
        runtime.complete_thread_spawn_without_authority("spawn_1");
        assert_eq!(runtime.shutdown_if_idle(), ShutdownIfIdleDecision::Shutdown);
    }

    #[test]
    fn thread_send_holds_one_controller_and_turn_across_queued_continuations() {
        let runtime = runtime();
        let (started, turn) = match runtime.send_thread(
            "thread_1",
            "controller_a".into(),
            None,
            "start".into(),
            "thread_turn_1".into(),
        ) {
            ThreadSendAdmission::Started { receipt, turn } => (receipt, turn),
            other => panic!("unexpected start admission: {other:?}"),
        };
        assert_eq!(
            started,
            ThreadSendResult::Started {
                thread_id: "thread_1".into(),
                turn_id: "thread_turn_1".into(),
            }
        );
        assert!(matches!(
            runtime.send_thread(
                "thread_1",
                "controller_b".into(),
                Some("thread_turn_1"),
                "compete".into(),
                "unused".into(),
            ),
            ThreadSendAdmission::Rejected {
                receipt: ThreadSendResult::Rejected {
                    reason: ThreadSendRejectedReason::ControllerOwned,
                    ..
                }
            }
        ));
        assert!(matches!(
            runtime.send_thread(
                "thread_1",
                "controller_a".into(),
                Some("thread_turn_1"),
                "steered text".into(),
                "unused".into(),
            ),
            ThreadSendAdmission::Steered {
                receipt: ThreadSendResult::Steered {
                    turn_id,
                    ..
                }
            } if turn_id == "thread_turn_1"
        ));

        assert_eq!(
            runtime.next_thread_message(&turn).as_deref(),
            Some("steered text")
        );
        assert_eq!(
            runtime
                .thread_live_state("thread_1")
                .current_turn_id
                .as_deref(),
            Some("thread_turn_1")
        );
        assert!(matches!(
            runtime.send_thread(
                "thread_1",
                "controller_b".into(),
                Some("thread_turn_1"),
                "boundary compete".into(),
                "unused".into(),
            ),
            ThreadSendAdmission::Rejected {
                receipt: ThreadSendResult::Rejected {
                    reason: ThreadSendRejectedReason::ControllerOwned,
                    ..
                }
            }
        ));
        assert_eq!(runtime.next_thread_message(&turn), None);
        assert_eq!(runtime.thread_live_state("thread_1").current_turn_id, None);
        assert!(matches!(
            runtime.send_thread(
                "thread_1",
                "controller_b".into(),
                None,
                "new turn".into(),
                "thread_turn_2".into(),
            ),
            ThreadSendAdmission::Started {
                receipt: ThreadSendResult::Started { turn_id, .. },
                ..
            } if turn_id == "thread_turn_2"
        ));
    }

    #[test]
    fn three_thread_observers_receive_identical_order_and_detach_cleanly() {
        let runtime = runtime();
        let turn = match runtime.send_thread(
            "thread_1",
            "controller_a".into(),
            None,
            "start".into(),
            "thread_turn_1".into(),
        ) {
            ThreadSendAdmission::Started { turn, .. } => turn,
            other => panic!("unexpected start admission: {other:?}"),
        };
        let live = runtime.load_thread("thread_1").unwrap();
        let ready = Arc::new(Barrier::new(4));
        let observers = (0..3)
            .map(|_| {
                let runtime = runtime.clone();
                let ready = ready.clone();
                thread::spawn(move || {
                    ready.wait();
                    let mut offset = 0;
                    let mut received = Vec::new();
                    while received.len() < 3 {
                        let page = runtime
                            .thread_events("thread_1", Some(offset), 3, Duration::from_secs(1))
                            .unwrap();
                        offset = page.next_offset;
                        received.extend(page.events);
                    }
                    received
                })
            })
            .collect::<Vec<_>>();
        ready.wait();
        let deadline = Instant::now() + Duration::from_secs(1);
        while live.observer_count() != 3 {
            assert!(Instant::now() < deadline, "observers did not attach");
            thread::yield_now();
        }
        for sequence in 0..3 {
            turn.publish(StreamEvent::Unknown(serde_json::json!({
                "kind": "test_event",
                "sequence": sequence,
            })));
        }

        let streams = observers
            .into_iter()
            .map(|observer| observer.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(streams[0], streams[1]);
        assert_eq!(streams[1], streams[2]);
        assert_eq!(
            streams[0]
                .iter()
                .map(|event| event.offset)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(live.observer_count(), 0);
    }

    #[test]
    fn thread_event_buffer_reports_lag_without_leaking_observers() {
        let runtime = runtime();
        let turn = match runtime.send_thread(
            "thread_1",
            "controller_a".into(),
            None,
            "start".into(),
            "thread_turn_1".into(),
        ) {
            ThreadSendAdmission::Started { turn, .. } => turn,
            other => panic!("unexpected start admission: {other:?}"),
        };
        for sequence in 0..=MAX_THREAD_EVENT_BUFFER {
            turn.publish(StreamEvent::Unknown(serde_json::json!({
                "kind": "test_event",
                "sequence": sequence,
            })));
        }
        assert_eq!(
            runtime.thread_events("thread_1", Some(0), 1, Duration::ZERO),
            Err(ThreadEventsError::Lagged { first_offset: 1 })
        );
        assert_eq!(runtime.load_thread("thread_1").unwrap().observer_count(), 0);
    }

    #[test]
    fn thread_steer_queue_is_bounded_and_preserves_fifo_order() {
        let runtime = runtime();
        let turn = match runtime.send_thread(
            "thread_1",
            "controller_a".into(),
            None,
            "start".into(),
            "thread_turn_1".into(),
        ) {
            ThreadSendAdmission::Started { turn, .. } => turn,
            other => panic!("unexpected start admission: {other:?}"),
        };
        for index in 0..MAX_PENDING_THREAD_STEERS {
            assert!(matches!(
                runtime.send_thread(
                    "thread_1",
                    "controller_a".into(),
                    Some("thread_turn_1"),
                    format!("steer {index}"),
                    "unused".into(),
                ),
                ThreadSendAdmission::Steered { .. }
            ));
        }
        assert!(matches!(
            runtime.send_thread(
                "thread_1",
                "controller_a".into(),
                Some("thread_turn_1"),
                "overflow".into(),
                "unused".into(),
            ),
            ThreadSendAdmission::Rejected {
                receipt: ThreadSendResult::Rejected {
                    reason: ThreadSendRejectedReason::QueueFull,
                    ..
                }
            }
        ));
        for index in 0..MAX_PENDING_THREAD_STEERS {
            assert_eq!(
                runtime.next_thread_message(&turn),
                Some(format!("steer {index}"))
            );
        }
        assert_eq!(runtime.next_thread_message(&turn), None);
    }

    #[test]
    fn thread_stop_wins_run_bind_without_child_admission() {
        let runtime = runtime();
        let turn = match runtime.send_thread(
            "thread_1",
            "controller_a".into(),
            None,
            "start".into(),
            "thread_turn_1".into(),
        ) {
            ThreadSendAdmission::Started { turn, .. } => turn,
            other => panic!("unexpected start admission: {other:?}"),
        };
        let record = Arc::new(RunRecord::new_for_thread(
            "run_bind_race".into(),
            "session_bind_race".into(),
            PathBuf::from("/tmp/agent.db"),
            turn.clone(),
        ));
        runtime.reserve_run(record.clone()).unwrap();

        let target = runtime.begin_thread_stop("thread_1").unwrap();
        assert_eq!(target.turn_id.as_deref(), Some("thread_turn_1"));
        assert!(target.run.is_none());
        runtime.complete_thread_stop("thread_1");

        assert_eq!(
            runtime.bind_thread_run(&turn, record.clone()),
            Err(ThreadRunBindError::Stopping)
        );
        runtime.release_run_reservation(&record);
        assert!(
            !runtime
                .state
                .lock()
                .unwrap()
                .runs
                .contains_key(&record.run_id)
        );
    }

    #[test]
    fn thread_stop_discards_queued_continuation_only_after_completion() {
        let runtime = runtime();
        let turn = match runtime.send_thread(
            "thread_1",
            "controller_a".into(),
            None,
            "start".into(),
            "thread_turn_1".into(),
        ) {
            ThreadSendAdmission::Started { turn, .. } => turn,
            other => panic!("unexpected start admission: {other:?}"),
        };
        assert!(matches!(
            runtime.send_thread(
                "thread_1",
                "controller_a".into(),
                Some("thread_turn_1"),
                "queued".into(),
                "unused".into(),
            ),
            ThreadSendAdmission::Steered { .. }
        ));

        runtime.begin_thread_stop("thread_1").unwrap();
        assert!(matches!(
            runtime.send_thread(
                "thread_1",
                "controller_a".into(),
                Some("thread_turn_1"),
                "late".into(),
                "unused".into(),
            ),
            ThreadSendAdmission::Stopped
        ));
        runtime.complete_thread_stop("thread_1");

        assert_eq!(runtime.next_thread_message(&turn), None);
    }

    #[test]
    fn aborted_thread_stop_preserves_controller_and_queued_continuation() {
        let runtime = runtime();
        let turn = match runtime.send_thread(
            "thread_1",
            "controller_a".into(),
            None,
            "start".into(),
            "thread_turn_1".into(),
        ) {
            ThreadSendAdmission::Started { turn, .. } => turn,
            other => panic!("unexpected start admission: {other:?}"),
        };
        assert!(matches!(
            runtime.send_thread(
                "thread_1",
                "controller_a".into(),
                Some("thread_turn_1"),
                "queued".into(),
                "unused".into(),
            ),
            ThreadSendAdmission::Steered { .. }
        ));

        runtime.begin_thread_stop("thread_1").unwrap();
        runtime.abort_thread_stop("thread_1");

        assert_eq!(
            runtime.next_thread_message(&turn).as_deref(),
            Some("queued")
        );
        assert_eq!(
            runtime
                .thread_live_state("thread_1")
                .current_turn_id
                .as_deref(),
            Some("thread_turn_1")
        );
        assert_eq!(runtime.next_thread_message(&turn), None);
    }

    #[test]
    fn stopped_thread_cannot_be_recreated_by_send_events_or_load() {
        let runtime = runtime();
        let turn = match runtime.send_thread(
            "thread_1",
            "controller_a".into(),
            None,
            "start".into(),
            "thread_turn_1".into(),
        ) {
            ThreadSendAdmission::Started { turn, .. } => turn,
            other => panic!("unexpected start admission: {other:?}"),
        };
        runtime.begin_thread_stop("thread_1").unwrap();
        runtime.complete_thread_stop("thread_1");

        assert!(matches!(
            runtime.send_thread(
                "thread_1",
                "controller_b".into(),
                None,
                "restart".into(),
                "thread_turn_2".into(),
            ),
            ThreadSendAdmission::Stopped
        ));
        assert_eq!(
            runtime.thread_events("thread_1", Some(0), 1, Duration::ZERO),
            Err(ThreadEventsError::Stopped)
        );
        assert!(matches!(
            runtime.load_thread("thread_1"),
            Err(ThreadEventsError::Stopped)
        ));
        assert!(!runtime.thread_live_state("thread_1").loaded);
        assert_eq!(runtime.next_thread_message(&turn), None);
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
    fn shutdown_and_thread_controller_admission_linearize() {
        for index in 0..256 {
            let runtime = runtime();
            let barrier = Arc::new(Barrier::new(3));
            let admit_runtime = runtime.clone();
            let admit_barrier = barrier.clone();
            let admission = thread::spawn(move || {
                admit_barrier.wait();
                admit_runtime.send_thread(
                    "thread_1",
                    "controller_a".into(),
                    None,
                    "start".into(),
                    format!("thread_turn_{index}"),
                )
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
                (
                    ThreadSendAdmission::Started { .. },
                    ShutdownIfIdleDecision::RefusedActive
                ) | (
                    ThreadSendAdmission::ShuttingDown,
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

        runtime.finish_run(&record, "done".into(), None);
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
