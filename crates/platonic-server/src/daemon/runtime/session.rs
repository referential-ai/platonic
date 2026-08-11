use super::DaemonRuntime;
use crate::{daemon::protocol::ApprovalProfile, tool_catalog::SHELL_EXEC};
#[cfg(test)]
use std::sync::{Arc, Barrier};

#[cfg(test)]
pub(super) type SessionGrantInstallBarriers = Option<(Arc<Barrier>, Arc<Barrier>)>;
#[cfg(test)]
pub(super) type ApprovalProfileDecisionBarriers = Option<(Arc<Barrier>, Arc<Barrier>)>;

impl DaemonRuntime {
    pub(in crate::daemon) fn has_shell_session_grant(&self, session_id: &str) -> bool {
        self.session_tool_grants
            .lock()
            .expect("session tool grants lock poisoned")
            .contains(&(session_id.to_owned(), SHELL_EXEC.to_owned()))
    }

    pub(in crate::daemon) fn approval_profile(&self, session_id: &str) -> ApprovalProfile {
        self.state
            .lock()
            .expect("runtime state lock poisoned")
            .approval_profiles
            .get(session_id)
            .copied()
            .unwrap_or_default()
    }

    pub(in crate::daemon) fn set_approval_profile(
        &self,
        session_id: &str,
        profile: ApprovalProfile,
    ) {
        self.state
            .lock()
            .expect("runtime state lock poisoned")
            .approval_profiles
            .insert(session_id.into(), profile);
    }

    pub(in crate::daemon) fn has_runtime_session(&self, session_id: &str) -> bool {
        let state = self.state.lock().expect("runtime state lock poisoned");
        state
            .runs
            .values()
            .any(|record| record.session_id == session_id)
            || state.live_threads.keys().any(|thread_id| {
                session_id
                    .strip_prefix("session_")
                    .is_some_and(|candidate| candidate == thread_id)
            })
    }

    pub(super) fn session_yolo_enabled_for_decision(&self, session_id: &str) -> bool {
        let state = self.state.lock().expect("runtime state lock poisoned");
        let enabled = state
            .approval_profiles
            .get(session_id)
            .copied()
            .unwrap_or_default()
            == ApprovalProfile::Yolo;
        #[cfg(test)]
        {
            let barriers = self
                .approval_profile_decision_barriers
                .lock()
                .expect("approval profile barrier lock poisoned")
                .take();
            if let Some((reached, release)) = barriers {
                reached.wait();
                release.wait();
            }
        }
        enabled
    }

    #[cfg(test)]
    pub(in crate::daemon) fn set_approval_profile_decision_barriers(
        &self,
        reached: Arc<Barrier>,
        release: Arc<Barrier>,
    ) {
        *self
            .approval_profile_decision_barriers
            .lock()
            .expect("approval profile barrier lock poisoned") = Some((reached, release));
    }

    #[cfg(test)]
    pub(in crate::daemon) fn session_tool_grant_count(&self) -> usize {
        self.session_tool_grants
            .lock()
            .expect("session tool grants lock poisoned")
            .len()
    }

    pub(in crate::daemon) fn install_shell_session_grant(&self, session_id: &str) -> bool {
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
    pub(in crate::daemon) fn set_session_grant_install_barriers(
        &self,
        reached: Arc<Barrier>,
        release: Arc<Barrier>,
    ) {
        *self
            .session_grant_install_barriers
            .lock()
            .expect("session grant barrier lock poisoned") = Some((reached, release));
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        RunAdmissionError, RunRecord,
        tests::{run_record, runtime},
    };
    use super::*;
    use std::{path::PathBuf, sync::Arc};

    #[test]
    fn approval_profiles_are_daemon_lifetime_and_session_isolated() {
        let runtime = runtime();
        assert_eq!(
            runtime.approval_profile("session_1"),
            ApprovalProfile::Prompt
        );

        runtime.set_approval_profile("session_1", ApprovalProfile::Yolo);
        assert_eq!(runtime.approval_profile("session_1"), ApprovalProfile::Yolo);
        assert_eq!(
            runtime.approval_profile("session_2"),
            ApprovalProfile::Prompt
        );
        let restarted = self::runtime();
        assert_eq!(
            restarted.approval_profile("session_1"),
            ApprovalProfile::Prompt
        );
    }

    #[test]
    fn rejected_run_admission_does_not_mutate_the_session_profile() {
        let runtime = runtime();
        let active = run_record(1);
        runtime.reserve_run(active).unwrap();

        let rejected = Arc::new(RunRecord::new(
            "run_2".into(),
            "session_1".into(),
            PathBuf::from("/tmp/agent.db"),
        ));
        assert!(matches!(
            runtime.reserve_run_with_profile(rejected, Some(ApprovalProfile::Yolo)),
            Err(RunAdmissionError::SessionActive { .. })
        ));
        assert_eq!(
            runtime.approval_profile("session_1"),
            ApprovalProfile::Prompt
        );
    }

    #[test]
    fn run_admission_preserves_omitted_profiles_and_applies_explicit_fresh_defaults() {
        let runtime = runtime();
        runtime.set_approval_profile("session_1", ApprovalProfile::Yolo);
        runtime
            .reserve_run_with_profile(run_record(1), None)
            .unwrap();
        assert_eq!(runtime.approval_profile("session_1"), ApprovalProfile::Yolo);

        runtime
            .reserve_run_with_profile(run_record(2), Some(ApprovalProfile::Prompt))
            .unwrap();
        assert_eq!(
            runtime.approval_profile("session_2"),
            ApprovalProfile::Prompt
        );
        assert_eq!(runtime.approval_profile("session_1"), ApprovalProfile::Yolo);
    }
}
