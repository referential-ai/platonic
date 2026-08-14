//! Server-wide state, independent of any workspace.
//!
//! D005 requires every thread to be enumerable — including clientless threads
//! and orphans. Thread authority therefore cannot live in a per-workspace
//! ledger: a thread in a workspace nobody has opened would be invisible, and a
//! dead parent would hide its children. This store lives once per host, beside
//! the socket, and holds the records that must outlive any single workspace.
//!
//! Workspace ledgers keep what is workspace-scoped: the event log, sessions,
//! and voice events. Nothing here is a log; every table is current state.

mod queries;
mod rows;
mod schema;
mod types;

pub(crate) use queries::{
    ServerStore, thread_authorities, thread_authority, thread_confinement, thread_stop,
};
pub(crate) use types::{
    AgentRecord, BranchClaimConflict, BranchClaimRecord, DurableThreadAuthority,
    MAX_PROFILE_LIST_ENTRIES, ProfileRecord, ProfileRevisionContent, ProfileRevisionRecord,
    ProfileValidationError, RunCancellationRecord, ToolCallApprovalDecision,
    ToolCallApprovalRecord, WorkspaceHealth, WorkspaceRecord, mint_profile_id, mint_workspace_id,
};
