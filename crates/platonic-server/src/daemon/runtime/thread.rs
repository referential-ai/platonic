use super::{DaemonRuntime, RunRecord};
use crate::{
    daemon::protocol::{
        BufferedThreadEvent, RunStateName, StreamEvent, ThreadEventsResetReason,
        ThreadEventsResult, ThreadLiveState, ThreadSendRejectedReason, ThreadSendResult,
    },
    server_store::DurableThreadAuthority,
    thread_authority::ThreadAuthorityDraft,
};
use std::{
    collections::VecDeque,
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

pub(in crate::daemon) const MAX_THREAD_EVENT_BUFFER: usize = 256;
const MAX_PENDING_THREAD_STEERS: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::daemon) enum ThreadSpawnAdmissionError {
    ShuttingDown,
    Duplicate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::daemon) enum ThreadSpawnClaimError {
    NotFound,
    WrongWorkspace,
    DecisionInProgress,
}

#[derive(Clone, Debug)]
pub(in crate::daemon) enum ThreadSendAdmission {
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
pub(in crate::daemon) enum ThreadEventsError {
    Stopped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::daemon) enum ThreadRunBindError {
    NotLoaded,
    Stopping,
    RunActive,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::daemon) enum ThreadStopError {
    InProgress,
    AlreadyStopped,
}

#[derive(Clone, Debug)]
pub(in crate::daemon) struct ThreadStopTarget {
    pub(in crate::daemon) turn_id: Option<String>,
    pub(in crate::daemon) run: Option<Arc<RunRecord>>,
}

impl DaemonRuntime {
    pub(in crate::daemon) fn reserve_thread_spawn(
        &self,
        spawn_id: String,
        draft: ThreadAuthorityDraft,
        max_spawn_depth: u32,
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
                max_spawn_depth,
                decision_in_progress: false,
            },
        );
        Ok(())
    }

    pub(in crate::daemon) fn claim_thread_spawn(
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

    pub(in crate::daemon) fn release_thread_spawn_claim(&self, spawn_id: &str) {
        let mut state = self.state.lock().expect("runtime state lock poisoned");
        if let Some(pending) = state.pending_thread_spawns.get_mut(spawn_id) {
            pending.decision_in_progress = false;
        }
    }

    pub(in crate::daemon) fn complete_thread_spawn(
        &self,
        spawn_id: &str,
        durable: DurableThreadAuthority,
    ) {
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

    pub(in crate::daemon) fn complete_thread_spawn_without_authority(&self, spawn_id: &str) {
        let mut state = self.state.lock().expect("runtime state lock poisoned");
        state.pending_thread_spawns.remove(spawn_id);
    }

    pub(in crate::daemon) fn thread_is_loaded(&self, thread_id: &str) -> bool {
        let state = self.state.lock().expect("runtime state lock poisoned");
        !state.stopping_threads.contains(thread_id)
            && !state.stopped_threads.contains(thread_id)
            && state
                .live_threads
                .get(thread_id)
                .is_some_and(|thread| thread.workspace_id == self.paths.workspace_id)
    }

    pub(in crate::daemon) fn notify_thread_available(&self, thread_id: &str) {
        let thread = self
            .state
            .lock()
            .expect("runtime state lock poisoned")
            .live_threads
            .get(thread_id)
            .filter(|thread| thread.workspace_id == self.paths.workspace_id)
            .cloned();
        if let Some(thread) = thread {
            thread.notify_available();
        }
    }

    pub(in crate::daemon) fn load_thread(
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

    pub(in crate::daemon) fn send_thread(
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

    pub(in crate::daemon) fn thread_events(
        &self,
        thread_id: &str,
        live_epoch_id: Option<&str>,
        from_offset: Option<u64>,
        limit: usize,
        wait: Duration,
    ) -> Result<ThreadEventsResult, ThreadEventsError> {
        let epoch = self.live_epoch_id();
        Ok(self.load_thread(thread_id)?.events(
            thread_id,
            &epoch,
            live_epoch_id,
            from_offset,
            limit,
            wait,
        ))
    }

    pub(in crate::daemon) fn bind_thread_run(
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

    pub(in crate::daemon) fn begin_thread_stop(
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

    pub(in crate::daemon) fn complete_thread_stop(&self, thread_id: &str) {
        let mut state = self.state.lock().expect("runtime state lock poisoned");
        if let Some(thread) = state.live_threads.get(thread_id) {
            thread.complete_stop();
        }
        state.live_threads.remove(thread_id);
        state.active_thread_runs.remove(thread_id);
        state.stopping_threads.remove(thread_id);
        state.stopped_threads.insert(thread_id.into());
    }

    pub(in crate::daemon) fn abort_thread_stop(&self, thread_id: &str) {
        let mut state = self.state.lock().expect("runtime state lock poisoned");
        if let Some(thread) = state.live_threads.get(thread_id) {
            thread.abort_stop();
        }
        state.stopping_threads.remove(thread_id);
    }

    #[cfg(test)]
    pub(in crate::daemon) fn thread_is_stopped(&self, thread_id: &str) -> bool {
        self.state
            .lock()
            .expect("runtime state lock poisoned")
            .stopped_threads
            .contains(thread_id)
    }

    pub(in crate::daemon) fn next_thread_message(
        &self,
        turn: &ThreadTurnBinding,
    ) -> Option<String> {
        turn.thread.next_message_or_finish(&turn.turn_id)
    }

    pub(in crate::daemon) fn abort_thread_turn(&self, turn: &ThreadTurnBinding) {
        turn.thread.abort(&turn.turn_id);
    }

    pub(in crate::daemon) fn thread_live_state(&self, thread_id: &str) -> ThreadLiveState {
        let state = self.state.lock().expect("runtime state lock poisoned");
        let live_epoch_id = state.live_epoch_id.clone();
        match state
            .live_threads
            .get(thread_id)
            .filter(|thread| thread.workspace_id == self.paths.workspace_id)
        {
            Some(thread) => {
                let (current_turn_id, last_activity_at_ms) = thread.live_snapshot();
                ThreadLiveState {
                    live_epoch_id,
                    loaded: true,
                    current_turn_id,
                    last_activity_at_ms: Some(last_activity_at_ms),
                }
            }
            None => ThreadLiveState {
                live_epoch_id,
                loaded: false,
                current_turn_id: None,
                last_activity_at_ms: None,
            },
        }
    }

    #[cfg(test)]
    pub(in crate::daemon) fn note_thread_activity_at(&self, thread_id: &str, activity_at_ms: u64) {
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
}

#[derive(Clone, Debug)]
pub(in crate::daemon) struct PendingThreadSpawn {
    pub(in crate::daemon) spawn_id: String,
    pub(in crate::daemon) workspace_id: String,
    pub(in crate::daemon) draft: ThreadAuthorityDraft,
    pub(in crate::daemon) max_spawn_depth: u32,
    decision_in_progress: bool,
}

#[derive(Debug)]
pub(in crate::daemon) struct LiveThread {
    pub(super) workspace_id: String,
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
pub(in crate::daemon) struct ThreadTurnBinding {
    pub(in crate::daemon) thread_id: String,
    pub(in crate::daemon) turn_id: String,
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

    pub(super) fn current_turn_id(&self) -> Option<String> {
        self.state
            .lock()
            .expect("live thread lock poisoned")
            .current_turn
            .as_ref()
            .map(|turn| turn.turn_id.clone())
    }

    fn notify_available(&self) {
        self.changed.notify_all();
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
        live_epoch_id: &str,
        expected_live_epoch_id: Option<&str>,
        from_offset: Option<u64>,
        limit: usize,
        wait: Duration,
    ) -> ThreadEventsResult {
        let mut state = self.state.lock().expect("live thread lock poisoned");
        state.observers += 1;
        if expected_live_epoch_id.is_some_and(|expected| expected != live_epoch_id) {
            let result = ThreadEventsResult {
                thread_id: thread_id.into(),
                live_epoch_id: live_epoch_id.into(),
                reset: Some(ThreadEventsResetReason::EpochChanged),
                from_offset: state.next_offset,
                next_offset: state.next_offset,
                current_turn_id: state.current_turn.as_ref().map(|turn| turn.turn_id.clone()),
                events: Vec::new(),
            };
            state.observers -= 1;
            return result;
        }
        let from_offset = from_offset.unwrap_or(state.next_offset);
        if from_offset < state.first_offset {
            let result = ThreadEventsResult {
                thread_id: thread_id.into(),
                live_epoch_id: live_epoch_id.into(),
                reset: Some(ThreadEventsResetReason::Lagged),
                from_offset: state.first_offset,
                next_offset: state.first_offset,
                current_turn_id: state.current_turn.as_ref().map(|turn| turn.turn_id.clone()),
                events: Vec::new(),
            };
            state.observers -= 1;
            return result;
        }
        if from_offset >= state.next_offset && !wait.is_zero() {
            let (next, _) = self
                .changed
                .wait_timeout(state, wait)
                .expect("live thread lock poisoned while observing");
            state = next;
        }
        if from_offset < state.first_offset {
            let result = ThreadEventsResult {
                thread_id: thread_id.into(),
                live_epoch_id: live_epoch_id.into(),
                reset: Some(ThreadEventsResetReason::Lagged),
                from_offset: state.first_offset,
                next_offset: state.first_offset,
                current_turn_id: state.current_turn.as_ref().map(|turn| turn.turn_id.clone()),
                events: Vec::new(),
            };
            state.observers -= 1;
            return result;
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
            live_epoch_id: live_epoch_id.into(),
            reset: None,
            from_offset,
            next_offset: from_offset + events.len() as u64,
            current_turn_id: state.current_turn.as_ref().map(|turn| turn.turn_id.clone()),
            events,
        };
        state.observers -= 1;
        result
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
    pub(super) fn publish(&self, event: StreamEvent) {
        self.thread.publish(&self.turn_id, event);
    }
}

#[cfg(test)]
mod tests {
    use super::super::{ShutdownIfIdleDecision, tests::runtime};
    use super::*;
    use crate::thread_authority::ThreadAuthorityDraftParams;
    use std::{
        path::PathBuf,
        sync::Barrier,
        thread,
        time::{Duration, Instant},
    };

    #[test]
    fn pending_thread_spawn_blocks_shutdown_until_resolved() {
        let root = tempfile::tempdir().unwrap();
        let runtime = runtime();
        let draft = ThreadAuthorityDraft::new(ThreadAuthorityDraftParams {
            parent_thread_id: None,
            cwd: root.path(),
            model: "gpt-5.6-sol".into(),
            reasoning_effort: crate::daemon::protocol::ReasoningEffort::Xhigh,
            approval_policy: crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
            agent_id: None,
            profile_id: platonic_core::ProfileId::new("profile_test").unwrap(),
            profile_revision: 1,
            thread_kind: crate::daemon::protocol::ThreadKind::Home,
            toolset: vec!["file.read".into()],
            writable: false,
            network: false,
        })
        .unwrap();
        runtime
            .reserve_thread_spawn("spawn_1".into(), draft, 1)
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
                    let epoch = runtime.live_epoch_id();
                    while received.len() < 3 {
                        let page = runtime
                            .thread_events(
                                "thread_1",
                                Some(&epoch),
                                Some(offset),
                                3,
                                Duration::from_secs(1),
                            )
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
        let epoch = runtime.live_epoch_id();
        let result = runtime
            .thread_events("thread_1", Some(&epoch), Some(0), 1, Duration::ZERO)
            .unwrap();
        assert_eq!(result.reset, Some(ThreadEventsResetReason::Lagged));
        assert_eq!(result.next_offset, 1);
        assert_eq!(runtime.load_thread("thread_1").unwrap().observer_count(), 0);
    }

    #[test]
    fn stale_epoch_cursor_resets_at_current_tip_without_aliasing_offsets() {
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
        turn.publish(StreamEvent::Unknown(
            serde_json::json!({"kind": "test_event"}),
        ));

        let result = runtime
            .thread_events("thread_1", Some("prior_epoch"), Some(99), 1, Duration::ZERO)
            .unwrap();
        assert_eq!(result.live_epoch_id, runtime.live_epoch_id());
        assert_eq!(result.reset, Some(ThreadEventsResetReason::EpochChanged));
        assert_eq!(result.from_offset, 1);
        assert_eq!(result.next_offset, 1);
        assert!(result.events.is_empty());
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
            runtime.thread_events("thread_1", None, Some(0), 1, Duration::ZERO),
            Err(ThreadEventsError::Stopped)
        );
        assert!(matches!(
            runtime.load_thread("thread_1"),
            Err(ThreadEventsError::Stopped)
        ));
        assert!(!runtime.thread_live_state("thread_1").loaded);
        assert_eq!(runtime.next_thread_message(&turn), None);
    }
}
