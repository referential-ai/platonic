#[cfg(test)]
use crate::config::Config;
use crate::{
    confinement::ConfinementSupport,
    daemon::{
        DaemonPaths,
        protocol::{ApprovalProfile, RunStateName},
    },
};
use platonic_protocol::ThreadConfinement;
#[cfg(test)]
use std::sync::Barrier;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex, atomic::AtomicBool},
    time::Instant,
};

mod run;
mod session;
mod thread;

#[cfg(test)]
use run::RunExecutionBarriers;
#[allow(unused_imports)]
pub(super) use run::{
    EventBuffer, MAX_EVENT_BUFFER, MAX_TERMINAL_RUNS, PendingApproval, PendingApprovalDecision,
    RunAdmissionError, RunRecord, RunStatus, approval_handler,
};
#[cfg(test)]
use session::{ApprovalProfileDecisionBarriers, SessionGrantInstallBarriers};
#[allow(unused_imports)]
pub(super) use thread::{
    LiveThread, MAX_THREAD_EVENT_BUFFER, PendingThreadSpawn, ThreadEventsError, ThreadRunBindError,
    ThreadSendAdmission, ThreadSpawnAdmissionError, ThreadSpawnClaimError, ThreadStopError,
    ThreadStopTarget, ThreadTurnBinding,
};
#[derive(Clone, Debug)]
pub(super) struct DaemonRuntime {
    pub(super) paths: DaemonPaths,
    max_spawn_depth: u32,
    require_confinement: bool,
    confinement_support: ConfinementSupport,
    started_at: Instant,
    pub(super) state: Arc<Mutex<RuntimeState>>,
    session_tool_grants: Arc<Mutex<HashSet<(String, String)>>>,
    pub(super) stop_requested: Arc<AtomicBool>,
    #[cfg(test)]
    session_grant_install_barriers: Arc<Mutex<SessionGrantInstallBarriers>>,
    #[cfg(test)]
    approval_profile_decision_barriers: Arc<Mutex<ApprovalProfileDecisionBarriers>>,
    #[cfg(test)]
    run_execution_barriers: Arc<Mutex<RunExecutionBarriers>>,
    #[cfg(test)]
    fail_next_run_handoff: Arc<AtomicBool>,
    #[cfg(all(test, unix))]
    shutdown_flush_barrier: Arc<Mutex<Option<Arc<Barrier>>>>,
}

#[derive(Debug)]
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
    approval_profiles: HashMap<String, ApprovalProfile>,
    live_epoch_id: String,
    deciding_home_reservations: HashSet<String>,
}

impl Default for RuntimeState {
    fn default() -> Self {
        Self {
            runs: HashMap::new(),
            live_threads: HashMap::new(),
            active_thread_runs: HashMap::new(),
            stopping_threads: HashSet::new(),
            stopped_threads: HashSet::new(),
            pending_thread_spawns: HashMap::new(),
            terminal_runs: VecDeque::new(),
            shutdown_accepted: false,
            issue_prep_active: false,
            approval_profiles: HashMap::new(),
            live_epoch_id: crate::thread_authority::new_live_epoch_id(),
            deciding_home_reservations: HashSet::new(),
        }
    }
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
    #[cfg(test)]
    pub(super) fn new(paths: DaemonPaths) -> Self {
        Self::new_with_max_spawn_depth(paths, Config::default().limits.max_spawn_depth)
    }

    #[cfg(test)]
    pub(super) fn new_with_max_spawn_depth(paths: DaemonPaths, max_spawn_depth: u32) -> Self {
        Self::new_with_server_policy(
            paths,
            max_spawn_depth,
            false,
            crate::confinement::detect_support(),
        )
    }

    #[cfg(test)]
    pub(super) fn new_with_server_policy(
        paths: DaemonPaths,
        max_spawn_depth: u32,
        require_confinement: bool,
        confinement_support: ConfinementSupport,
    ) -> Self {
        Self::new_shared(
            paths,
            max_spawn_depth,
            require_confinement,
            confinement_support,
            Instant::now(),
            Arc::new(Mutex::new(RuntimeState::default())),
            Arc::new(AtomicBool::new(false)),
        )
    }

    pub(super) fn new_shared(
        paths: DaemonPaths,
        max_spawn_depth: u32,
        require_confinement: bool,
        confinement_support: ConfinementSupport,
        started_at: Instant,
        state: Arc<Mutex<RuntimeState>>,
        stop_requested: Arc<AtomicBool>,
    ) -> Self {
        Self {
            paths,
            max_spawn_depth,
            require_confinement,
            confinement_support,
            started_at,
            state,
            session_tool_grants: Arc::new(Mutex::new(HashSet::new())),
            stop_requested,
            #[cfg(test)]
            session_grant_install_barriers: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            approval_profile_decision_barriers: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            run_execution_barriers: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            fail_next_run_handoff: Arc::new(AtomicBool::new(false)),
            #[cfg(all(test, unix))]
            shutdown_flush_barrier: Arc::new(Mutex::new(None)),
        }
    }

    pub(super) fn max_spawn_depth(&self) -> u32 {
        self.max_spawn_depth
    }

    pub(super) fn live_epoch_id(&self) -> String {
        self.state
            .lock()
            .expect("runtime state lock poisoned")
            .live_epoch_id
            .clone()
    }

    pub(super) fn claim_home_reservation_decision(&self, reservation_id: &str) -> bool {
        self.state
            .lock()
            .expect("runtime state lock poisoned")
            .deciding_home_reservations
            .insert(reservation_id.into())
    }

    pub(super) fn release_home_reservation_decision(&self, reservation_id: &str) {
        self.state
            .lock()
            .expect("runtime state lock poisoned")
            .deciding_home_reservations
            .remove(reservation_id);
    }

    pub(super) fn require_confinement(&self) -> bool {
        self.require_confinement
    }

    pub(super) fn confinement_support(&self) -> ConfinementSupport {
        self.confinement_support
    }

    pub(super) fn thread_confinement(&self) -> Result<ThreadConfinement, ()> {
        match self.confinement_support {
            #[cfg(any(target_os = "linux", test))]
            ConfinementSupport::Landlock => Ok(ThreadConfinement::Landlock),
            _ if self.require_confinement => Err(()),
            _ => Ok(ThreadConfinement::None),
        }
    }

    pub(super) fn uptime_ms(&self) -> u64 {
        self.started_at
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ApprovalRequest;
    use platonic_core::{EffectClass, RunId, ToolCallId};
    use std::{path::PathBuf, sync::Barrier, thread, time::Duration};

    pub(super) fn runtime() -> DaemonRuntime {
        DaemonRuntime::new(DaemonPaths {
            workspace_root: PathBuf::from("/tmp/workspace"),
            workspace_id: "workspace-1".into(),
            socket_path: PathBuf::from("/tmp/agent.sock"),
            ledger_path: PathBuf::from("/tmp/agent.db"),
            server_db_path: PathBuf::from("/tmp/platonic-server.db"),
        })
    }

    pub(super) fn run_record(index: usize) -> Arc<RunRecord> {
        Arc::new(RunRecord::new(
            format!("run_{index}"),
            format!("session_{index}"),
            PathBuf::from("/tmp/agent.db"),
        ))
    }

    #[test]
    fn confinement_support_and_require_policy_matrix_is_typed() {
        for (support, require, expected) in [
            (
                ConfinementSupport::Landlock,
                false,
                Ok(ThreadConfinement::Landlock),
            ),
            (
                ConfinementSupport::Landlock,
                true,
                Ok(ThreadConfinement::Landlock),
            ),
            (ConfinementSupport::None, false, Ok(ThreadConfinement::None)),
            (ConfinementSupport::None, true, Err(())),
        ] {
            let runtime =
                DaemonRuntime::new_with_server_policy(runtime().paths, 1, require, support);
            assert_eq!(runtime.thread_confinement(), expected);
        }
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
                    yolo_eligible: false,
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
}
