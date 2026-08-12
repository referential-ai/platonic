use crate::{
    RunSession,
    confinement::ChildConfinement,
    daemon::{
        protocol::{ApprovalProfile, ThreadApprovalPolicy},
        runtime::ThreadTurnBinding,
    },
    model::RunOverrides,
};
use platonic_core::AgentId;
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub(super) struct ThreadRunContext {
    pub(super) workspace_root: PathBuf,
    pub(super) approval_policy: ThreadApprovalPolicy,
    pub(super) agent_id: AgentId,
    pub(super) toolset: Vec<String>,
    pub(super) turn: ThreadTurnBinding,
    pub(super) confinement: ChildConfinement,
}

pub(super) struct StartRunRequest {
    pub(super) request_id: Option<String>,
    pub(super) question: String,
    pub(super) session: RunSession,
    pub(super) config_path: Option<String>,
    pub(super) overrides: RunOverrides,
    pub(super) approval_profile: Option<ApprovalProfile>,
    pub(super) wait: Option<bool>,
    pub(super) thread_context: Option<ThreadRunContext>,
}
