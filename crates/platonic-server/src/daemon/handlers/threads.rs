use super::{
    control::shutting_down_response,
    runs::{DEFAULT_EVENT_LIMIT, MAX_EVENT_LIMIT, start_run},
    types::{StartRunRequest, ThreadRunContext},
};
use crate::{
    AppError, AppResult, RunSession,
    config::Config,
    confinement::ChildConfinement,
    daemon::{
        protocol::{
            ERROR_DAEMON_SHUTTING_DOWN, ERROR_LAGGED, ERROR_MALFORMED_REQUEST, ERROR_NOT_FOUND,
            ERROR_OVERLOAD, ERROR_THREAD_AUTHORITY_EXCEEDED, ERROR_THREAD_AUTHORITY_FAILED,
            ERROR_THREAD_BRANCH_CLAIM_CONFLICT, ERROR_THREAD_CONFINEMENT_UNAVAILABLE,
            ERROR_THREAD_EVENTS_FAILED, ERROR_THREAD_LIST_FAILED, ERROR_THREAD_SEND_FAILED,
            ERROR_THREAD_SPAWN_FAILED, ERROR_THREAD_STATUS_FAILED, ERROR_THREAD_STOP_FAILED,
            ERROR_WORKSPACE_BROKEN, ERROR_WORKSPACE_MISMATCH, ERROR_WORKSPACE_UNREGISTERED,
            Envelope, ProtocolResponse, ThreadAuthorityParams, ThreadAuthorityResult,
            ThreadConfinement, ThreadEventsParams, ThreadListResult, ThreadRepositoryRequest,
            ThreadSendParams, ThreadSpawnDecision, ThreadSpawnParams, ThreadSpawnResult,
            ThreadStatus, ThreadStatusParams, ThreadStatusResult, ThreadStopParams,
            ThreadStopResult,
        },
        runtime::{
            DaemonRuntime, ThreadEventsError, ThreadSendAdmission, ThreadSpawnAdmissionError,
            ThreadSpawnClaimError, ThreadStopError,
        },
    },
    model::RunOverrides,
    server_store::ServerStore,
    thread_authority::{
        THREAD_SPAWN_APPROVAL_REASON, ThreadAuthorityDraft, ThreadAuthorityDraftParams,
        ThreadAuthorityError, ThreadSpawnApprovalRecord, ThreadSpawnDecisionName, ThreadStopRecord,
        authority_working_directory, legacy_status_authority, new_spawn_id, new_thread_turn_id,
        now_ms, thread_spawn_effect, validate_child_authority,
    },
    tool_catalog::{FILE_EDIT, FILE_WRITE, SHELL_EXEC, THREAD_SPAWN, effect_for_tool},
    tools::{ThreadSpawnToolHandler, ThreadSpawnToolInput, ThreadSpawnToolOutput},
};
use platonic_core::{ActorId, AgentId, EffectClass, TurnId};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

const MAX_THREAD_EVENT_WAIT_MS: u64 = 1_000;
const THREAD_STOP_WAIT: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub(super) enum ThreadSpawnFailure {
    ShuttingDown,
    Malformed(String),
    Unregistered(String),
    NotFound(String),
    WorkspaceBroken(String),
    WorkspaceMismatch(String),
    Authority(ThreadAuthorityError),
    Overload(String),
    Conflict(String),
    BranchConflict(String),
    ConfinementUnavailable,
    Persistence,
}

pub(super) fn handle_thread_spawn(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: ThreadSpawnParams,
) -> Envelope {
    match thread_spawn(runtime, params) {
        Ok(result) => Envelope::typed_response(request.id, ProtocolResponse::ThreadSpawn(result)),
        Err(ThreadSpawnFailure::ShuttingDown) => shutting_down_response(request.id, "thread.spawn"),
        Err(ThreadSpawnFailure::Malformed(message)) => Envelope::error(
            request.id,
            Some("thread.spawn".into()),
            ERROR_MALFORMED_REQUEST,
            message,
        ),
        Err(ThreadSpawnFailure::Unregistered(message)) => Envelope::error(
            request.id,
            Some("thread.spawn".into()),
            ERROR_WORKSPACE_UNREGISTERED,
            message,
        ),
        Err(ThreadSpawnFailure::NotFound(message)) => Envelope::error(
            request.id,
            Some("thread.spawn".into()),
            ERROR_NOT_FOUND,
            message,
        ),
        Err(ThreadSpawnFailure::WorkspaceBroken(message)) => Envelope::error(
            request.id,
            Some("thread.spawn".into()),
            ERROR_WORKSPACE_BROKEN,
            message,
        ),
        Err(ThreadSpawnFailure::WorkspaceMismatch(message)) => Envelope::error(
            request.id,
            Some("thread.spawn".into()),
            ERROR_WORKSPACE_MISMATCH,
            message,
        ),
        Err(ThreadSpawnFailure::Authority(error)) => Envelope::error(
            request.id,
            Some("thread.spawn".into()),
            ERROR_THREAD_AUTHORITY_EXCEEDED,
            error.to_string(),
        ),
        Err(ThreadSpawnFailure::Overload(message)) => Envelope::error(
            request.id,
            Some("thread.spawn".into()),
            ERROR_OVERLOAD,
            message,
        ),
        Err(ThreadSpawnFailure::Conflict(message)) => Envelope::error(
            request.id,
            Some("thread.spawn".into()),
            ERROR_THREAD_SPAWN_FAILED,
            message,
        ),
        Err(ThreadSpawnFailure::BranchConflict(message)) => Envelope::error(
            request.id,
            Some("thread.spawn".into()),
            ERROR_THREAD_BRANCH_CLAIM_CONFLICT,
            message,
        ),
        Err(ThreadSpawnFailure::ConfinementUnavailable) => Envelope::error(
            request.id,
            Some("thread.spawn".into()),
            ERROR_THREAD_CONFINEMENT_UNAVAILABLE,
            "server policy requires confinement, but this spawn cannot be confined",
        ),
        Err(ThreadSpawnFailure::Persistence) => Envelope::error(
            request.id,
            Some("thread.spawn".into()),
            ERROR_THREAD_SPAWN_FAILED,
            "thread spawn could not be persisted",
        ),
    }
}

fn thread_spawn(
    runtime: &DaemonRuntime,
    params: ThreadSpawnParams,
) -> Result<ThreadSpawnResult, ThreadSpawnFailure> {
    match params {
        ThreadSpawnParams::Start {
            parent_thread_id,
            cwd,
            model,
            reasoning_effort,
            approval_policy,
            repositories,
        } => start_thread_spawn(
            runtime,
            parent_thread_id,
            Path::new(&cwd),
            model,
            reasoning_effort,
            approval_policy,
            repositories,
        ),
        ThreadSpawnParams::Decide { spawn_id, approval } => {
            decide_thread_spawn(runtime, &spawn_id, approval)
        }
    }
}

fn start_thread_spawn(
    runtime: &DaemonRuntime,
    parent_thread_id: Option<String>,
    cwd: &Path,
    model: String,
    reasoning_effort: crate::model::ReasoningEffort,
    approval_policy: crate::daemon::protocol::ThreadApprovalPolicy,
    repositories: Vec<ThreadRepositoryRequest>,
) -> Result<ThreadSpawnResult, ThreadSpawnFailure> {
    if runtime.shutdown_accepted() {
        return Err(ThreadSpawnFailure::ShuttingDown);
    }
    if !cwd.is_absolute() {
        return Err(ThreadSpawnFailure::Malformed(
            "thread cwd must be an absolute path".into(),
        ));
    }
    let mut store = runtime
        .paths
        .server_store()
        .map_err(|_| ThreadSpawnFailure::Persistence)?;
    match store
        .workspace_by_root(&runtime.paths.workspace_root.to_string_lossy())
        .map_err(|_| ThreadSpawnFailure::Persistence)?
    {
        Some(workspace) if workspace.health() == crate::server_store::WorkspaceHealth::Present => {}
        Some(workspace) => {
            return Err(ThreadSpawnFailure::WorkspaceBroken(format!(
                "workspace directory is missing: {}",
                workspace.id
            )));
        }
        None => {
            return Err(ThreadSpawnFailure::Unregistered(format!(
                "workspace is not registered: {}; run platonic workspace create",
                runtime.paths.workspace_root.display()
            )));
        }
    }
    let config = Config::load(cwd, None)
        .map_err(|error| ThreadSpawnFailure::Malformed(error.to_string()))?;
    let toolset = config.tools.enabled;
    let draft = ThreadAuthorityDraft::new(ThreadAuthorityDraftParams {
        parent_thread_id,
        cwd,
        model,
        reasoning_effort,
        approval_policy,
        agent_id: AgentId::new("plato")
            .map_err(|error| ThreadSpawnFailure::Malformed(error.to_string()))?,
        writable: toolset_requires_writable_path(&toolset),
        network: toolset_has_effect(&toolset, EffectClass::Network),
        toolset,
    })
    .map_err(|error| ThreadSpawnFailure::Malformed(error.to_string()))?;
    start_thread_spawn_draft(
        runtime,
        &mut store,
        draft,
        repositories,
        runtime.max_spawn_depth(),
        "yolo",
    )
}

fn start_thread_spawn_draft(
    runtime: &DaemonRuntime,
    store: &mut ServerStore,
    mut draft: ThreadAuthorityDraft,
    repository_requests: Vec<ThreadRepositoryRequest>,
    max_spawn_depth: u32,
    auto_grant_actor: &str,
) -> Result<ThreadSpawnResult, ThreadSpawnFailure> {
    let parent = read_live_parent(runtime, store, &draft, max_spawn_depth)?;
    draft.repositories = crate::thread_repository::resolve(
        &runtime.paths.workspace_root,
        &draft.thread_id,
        Path::new(&draft.cwd),
        parent.as_ref(),
        &repository_requests,
    )
    .map_err(|error| ThreadSpawnFailure::Malformed(error.to_string()))?;
    if let Some(parent) = parent.as_ref() {
        validate_child_authority(parent, &draft).map_err(ThreadSpawnFailure::Authority)?;
    }
    let auto_grant = parent.as_ref().is_some_and(|parent| {
        parent.approval_policy == crate::daemon::protocol::ThreadApprovalPolicy::Yolo
    });
    let spawn_id = new_spawn_id();
    runtime
        .reserve_thread_spawn(spawn_id.clone(), draft.clone(), max_spawn_depth)
        .map_err(|error| match error {
            ThreadSpawnAdmissionError::ShuttingDown => ThreadSpawnFailure::ShuttingDown,
            ThreadSpawnAdmissionError::Duplicate => {
                ThreadSpawnFailure::Conflict("duplicate thread spawn reservation".into())
            }
        })?;

    if auto_grant {
        let pending = runtime
            .claim_thread_spawn(&spawn_id)
            .expect("newly reserved thread spawn can be claimed");
        return resolve_thread_spawn(
            runtime,
            store,
            pending,
            ThreadSpawnDecision::Grant {
                actor: auto_grant_actor.into(),
            },
        );
    }

    Ok(ThreadSpawnResult::ApprovalRequired {
        spawn_id,
        thread_id: draft.thread_id,
        effect: thread_spawn_effect(),
        reason: THREAD_SPAWN_APPROVAL_REASON.into(),
    })
}

fn toolset_requires_writable_path(toolset: &[String]) -> bool {
    toolset
        .iter()
        .any(|tool| matches!(tool.as_str(), FILE_WRITE | FILE_EDIT | SHELL_EXEC))
}

fn toolset_has_effect(toolset: &[String], effect: EffectClass) -> bool {
    toolset.iter().any(|tool| effect_for_tool(tool) == effect)
}

fn model_thread_spawn_handler(
    runtime: DaemonRuntime,
    parent_thread_id: String,
) -> ThreadSpawnToolHandler {
    ThreadSpawnToolHandler::new(move |input, approving_actor| {
        model_thread_spawn(&runtime, &parent_thread_id, input, approving_actor)
    })
}

pub(super) fn projected_thread_spawn_handler(
    runtime: &DaemonRuntime,
    context: &ThreadRunContext,
) -> Option<ThreadSpawnToolHandler> {
    context
        .toolset
        .iter()
        .any(|tool| tool == THREAD_SPAWN)
        .then(|| model_thread_spawn_handler(runtime.clone(), context.turn.thread_id.clone()))
}

fn model_thread_spawn(
    runtime: &DaemonRuntime,
    parent_thread_id: &str,
    input: ThreadSpawnToolInput,
    approving_actor: String,
) -> AppResult<ThreadSpawnToolOutput> {
    match model_thread_spawn_inner(runtime, parent_thread_id, input, approving_actor) {
        Ok(thread_id) => Ok(ThreadSpawnToolOutput::Spawned { thread_id }),
        Err(ThreadSpawnFailure::Persistence) => {
            Err(AppError::Tool("thread spawn could not be persisted".into()))
        }
        Err(error) => {
            let (code, reason) = thread_spawn_rejection(error);
            Ok(ThreadSpawnToolOutput::Rejected { code, reason })
        }
    }
}

fn model_thread_spawn_inner(
    runtime: &DaemonRuntime,
    parent_thread_id: &str,
    input: ThreadSpawnToolInput,
    approving_actor: String,
) -> Result<String, ThreadSpawnFailure> {
    if runtime.shutdown_accepted() {
        return Err(ThreadSpawnFailure::ShuttingDown);
    }
    if !Path::new(&input.cwd).is_absolute() {
        return Err(ThreadSpawnFailure::Malformed(
            "thread cwd must be an absolute path".into(),
        ));
    }
    let agent_id = AgentId::new(input.agent_id)
        .map_err(|error| ThreadSpawnFailure::Malformed(error.to_string()))?;
    let mut store = runtime
        .paths
        .server_store()
        .map_err(|_| ThreadSpawnFailure::Persistence)?;
    let agent = store
        .agent(&agent_id)
        .map_err(|_| ThreadSpawnFailure::Persistence)?
        .ok_or_else(|| ThreadSpawnFailure::NotFound(format!("agent not found: {agent_id}")))?;
    if agent.workspace_id != runtime.paths.workspace_id {
        return Err(ThreadSpawnFailure::WorkspaceMismatch(format!(
            "agent {agent_id} belongs to workspace {}, not {}",
            agent.workspace_id, runtime.paths.workspace_id
        )));
    }

    let toolset = input.toolset.unwrap_or_else(|| agent.toolset.clone());
    let excess = toolset
        .iter()
        .filter(|tool| !agent.toolset.contains(tool))
        .cloned()
        .collect::<Vec<_>>();
    if !excess.is_empty() {
        return Err(ThreadSpawnFailure::Authority(
            ThreadAuthorityError::Toolset { excess },
        ));
    }
    let draft = ThreadAuthorityDraft::new(ThreadAuthorityDraftParams {
        parent_thread_id: Some(parent_thread_id.into()),
        cwd: Path::new(&input.cwd),
        model: input.model.unwrap_or(agent.model),
        reasoning_effort: input.reasoning_effort.unwrap_or(agent.reasoning_effort),
        approval_policy: input.approval_policy.unwrap_or(agent.approval_policy),
        agent_id,
        writable: toolset_requires_writable_path(&toolset),
        network: toolset_has_effect(&toolset, EffectClass::Network),
        toolset,
    })
    .map_err(|error| ThreadSpawnFailure::Malformed(error.to_string()))?;
    let repositories = input.repositories.unwrap_or_default();
    let result = start_thread_spawn_draft(
        runtime,
        &mut store,
        draft,
        repositories,
        runtime.max_spawn_depth(),
        &approving_actor,
    )?;
    let result = match result {
        ThreadSpawnResult::ApprovalRequired { spawn_id, .. } => decide_thread_spawn(
            runtime,
            &spawn_id,
            ThreadSpawnDecision::Grant {
                actor: approving_actor,
            },
        )?,
        result => result,
    };
    match result {
        ThreadSpawnResult::Spawned { thread } => Ok(thread.authority.thread_id),
        ThreadSpawnResult::ApprovalRequired { .. } => Err(ThreadSpawnFailure::Conflict(
            "approved coordinator spawn remained pending".into(),
        )),
        ThreadSpawnResult::Denied { .. } | ThreadSpawnResult::Canceled { .. } => Err(
            ThreadSpawnFailure::Conflict("approved coordinator spawn was not granted".into()),
        ),
    }
}

fn thread_spawn_rejection(error: ThreadSpawnFailure) -> (String, String) {
    match error {
        ThreadSpawnFailure::ShuttingDown => (
            ERROR_DAEMON_SHUTTING_DOWN.to_string(),
            "daemon shutdown is already in progress".into(),
        ),
        ThreadSpawnFailure::Malformed(reason) => (ERROR_MALFORMED_REQUEST.to_string(), reason),
        ThreadSpawnFailure::Unregistered(reason) => {
            (ERROR_WORKSPACE_UNREGISTERED.to_string(), reason)
        }
        ThreadSpawnFailure::NotFound(reason) => (ERROR_NOT_FOUND.to_string(), reason),
        ThreadSpawnFailure::WorkspaceBroken(reason) => (ERROR_WORKSPACE_BROKEN.to_string(), reason),
        ThreadSpawnFailure::WorkspaceMismatch(reason) => {
            (ERROR_WORKSPACE_MISMATCH.to_string(), reason)
        }
        ThreadSpawnFailure::Authority(error) => (
            ERROR_THREAD_AUTHORITY_EXCEEDED.to_string(),
            error.to_string(),
        ),
        ThreadSpawnFailure::Overload(reason) => (ERROR_OVERLOAD.to_string(), reason),
        ThreadSpawnFailure::Conflict(reason) => (ERROR_THREAD_SPAWN_FAILED.to_string(), reason),
        ThreadSpawnFailure::BranchConflict(reason) => {
            (ERROR_THREAD_BRANCH_CLAIM_CONFLICT.to_string(), reason)
        }
        ThreadSpawnFailure::ConfinementUnavailable => (
            ERROR_THREAD_CONFINEMENT_UNAVAILABLE.to_string(),
            "server policy requires confinement, but this spawn cannot be confined".into(),
        ),
        ThreadSpawnFailure::Persistence => (
            ERROR_THREAD_SPAWN_FAILED.to_string(),
            "thread spawn could not be persisted".into(),
        ),
    }
}

fn decide_thread_spawn(
    runtime: &DaemonRuntime,
    spawn_id: &str,
    decision: ThreadSpawnDecision,
) -> Result<ThreadSpawnResult, ThreadSpawnFailure> {
    if runtime.shutdown_accepted() {
        return Err(ThreadSpawnFailure::ShuttingDown);
    }
    let mut store = runtime
        .paths
        .server_store()
        .map_err(|_| ThreadSpawnFailure::Persistence)?;
    if let Some(existing) = store
        .thread_spawn_approval(spawn_id)
        .map_err(|_| ThreadSpawnFailure::Persistence)?
    {
        return persisted_thread_spawn_result(runtime, &store, existing, &decision);
    }
    let pending = runtime
        .claim_thread_spawn(spawn_id)
        .map_err(|error| match error {
            ThreadSpawnClaimError::NotFound => {
                ThreadSpawnFailure::NotFound(format!("pending thread spawn not found: {spawn_id}"))
            }
            ThreadSpawnClaimError::WrongWorkspace => ThreadSpawnFailure::NotFound(format!(
                "pending thread spawn belongs to another workspace: {spawn_id}"
            )),
            ThreadSpawnClaimError::DecisionInProgress => ThreadSpawnFailure::Overload(format!(
                "thread spawn decision is already in progress: {spawn_id}"
            )),
        })?;
    resolve_thread_spawn(runtime, &mut store, pending, decision)
}

fn resolve_thread_spawn(
    runtime: &DaemonRuntime,
    store: &mut ServerStore,
    pending: crate::daemon::runtime::PendingThreadSpawn,
    decision: ThreadSpawnDecision,
) -> Result<ThreadSpawnResult, ThreadSpawnFailure> {
    let result = resolve_thread_spawn_inner(runtime, store, &pending, decision);
    if result.is_err() {
        runtime.release_thread_spawn_claim(&pending.spawn_id);
    }
    result
}

fn resolve_thread_spawn_inner(
    runtime: &DaemonRuntime,
    store: &mut ServerStore,
    pending: &crate::daemon::runtime::PendingThreadSpawn,
    decision: ThreadSpawnDecision,
) -> Result<ThreadSpawnResult, ThreadSpawnFailure> {
    let decided_at_ms = now_ms();
    let approval = ThreadSpawnApprovalRecord::from_decision(
        pending.spawn_id.clone(),
        pending.draft.thread_id.clone(),
        &decision,
        decided_at_ms,
    )
    .map_err(|error| ThreadSpawnFailure::Malformed(error.to_string()))?;
    match decision {
        ThreadSpawnDecision::Grant { actor } => {
            read_live_parent(runtime, store, &pending.draft, pending.max_spawn_depth)?;
            let confinement = runtime
                .thread_confinement()
                .map_err(|()| ThreadSpawnFailure::ConfinementUnavailable)?;
            let claims = pending
                .draft
                .repositories
                .iter()
                .map(|repository| (repository.repo.clone(), repository.branch.clone()))
                .collect::<Vec<_>>();
            if let Some(conflict) = store
                .claim_thread_branches(
                    &runtime.paths.workspace_id,
                    &pending.draft.thread_id,
                    &claims,
                    decided_at_ms,
                )
                .map_err(|_| ThreadSpawnFailure::Persistence)?
            {
                return Err(ThreadSpawnFailure::BranchConflict(format!(
                    "branch {} in repository {} is already claimed by thread {}",
                    conflict.branch, conflict.repo, conflict.thread_id
                )));
            }
            let mut draft = pending.draft.clone();
            if !draft.repositories.is_empty() {
                let prepared = match crate::thread_repository::prepare(
                    &runtime.paths.server_db_path,
                    &runtime.paths.workspace_id,
                    &draft.thread_id,
                    &draft.repositories,
                ) {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        let _ = store.release_thread_claims(&draft.thread_id);
                        return Err(ThreadSpawnFailure::Malformed(error.to_string()));
                    }
                };
                draft.worktrees = prepared.worktrees;
                draft.granted_paths = prepared.granted_paths;
            }
            let authority = draft
                .complete(actor, decided_at_ms)
                .map_err(|error| ThreadSpawnFailure::Malformed(error.to_string()))?;
            let durable =
                match store.persist_thread_spawn(&approval, Some(&authority), Some(confinement)) {
                    Ok(Some(durable)) => durable,
                    Ok(None) => unreachable!("granted spawn persistence returns durable authority"),
                    Err(_) => {
                        let _ = crate::thread_repository::discard(
                            &runtime.paths.server_db_path,
                            &runtime.paths.workspace_id,
                            &draft.thread_id,
                            &draft.repositories,
                        );
                        let _ = store.release_thread_claims(&draft.thread_id);
                        return Err(ThreadSpawnFailure::Persistence);
                    }
                };
            let authority = durable.record().clone();
            runtime.complete_thread_spawn(&pending.spawn_id, durable);
            Ok(ThreadSpawnResult::Spawned {
                thread: joined_thread_status(runtime, authority)
                    .map_err(|_| ThreadSpawnFailure::Persistence)?,
            })
        }
        ThreadSpawnDecision::Deny { actor, reason } => {
            store
                .persist_thread_spawn(&approval, None, None)
                .map_err(|_| ThreadSpawnFailure::Persistence)?;
            runtime.complete_thread_spawn_without_authority(&pending.spawn_id);
            Ok(ThreadSpawnResult::Denied {
                spawn_id: pending.spawn_id.clone(),
                thread_id: pending.draft.thread_id.clone(),
                actor,
                reason,
            })
        }
        ThreadSpawnDecision::Cancel { actor } => {
            store
                .persist_thread_spawn(&approval, None, None)
                .map_err(|_| ThreadSpawnFailure::Persistence)?;
            runtime.complete_thread_spawn_without_authority(&pending.spawn_id);
            Ok(ThreadSpawnResult::Canceled {
                spawn_id: pending.spawn_id.clone(),
                thread_id: pending.draft.thread_id.clone(),
                actor,
            })
        }
    }
}

fn persisted_thread_spawn_result(
    runtime: &DaemonRuntime,
    store: &ServerStore,
    approval: ThreadSpawnApprovalRecord,
    requested: &ThreadSpawnDecision,
) -> Result<ThreadSpawnResult, ThreadSpawnFailure> {
    if !approval.matches(requested) {
        return Err(ThreadSpawnFailure::Conflict(format!(
            "thread spawn {} already has a different durable decision",
            approval.spawn_id
        )));
    }
    match approval.decision {
        ThreadSpawnDecisionName::Granted => {
            let authority = store
                .thread_authority(&approval.thread_id)
                .map_err(|_| ThreadSpawnFailure::Persistence)?
                .ok_or(ThreadSpawnFailure::Persistence)?;
            Ok(ThreadSpawnResult::Spawned {
                thread: joined_thread_status(runtime, authority)
                    .map_err(|_| ThreadSpawnFailure::Persistence)?,
            })
        }
        ThreadSpawnDecisionName::Denied => Ok(ThreadSpawnResult::Denied {
            spawn_id: approval.spawn_id,
            thread_id: approval.thread_id,
            actor: approval.actor,
            reason: approval.reason.ok_or(ThreadSpawnFailure::Persistence)?,
        }),
        ThreadSpawnDecisionName::Canceled => Ok(ThreadSpawnResult::Canceled {
            spawn_id: approval.spawn_id,
            thread_id: approval.thread_id,
            actor: approval.actor,
        }),
    }
}

fn read_live_parent(
    runtime: &DaemonRuntime,
    store: &ServerStore,
    draft: &ThreadAuthorityDraft,
    max_spawn_depth: u32,
) -> Result<Option<crate::daemon::protocol::ThreadAuthorityRecord>, ThreadSpawnFailure> {
    let Some(parent_thread_id) = draft.parent_thread_id.as_deref() else {
        return Ok(None);
    };
    validate_spawn_depth(store, parent_thread_id, max_spawn_depth)?;
    let parent = store
        .thread_authority(parent_thread_id)
        .map_err(|_| ThreadSpawnFailure::Persistence)?
        .ok_or_else(|| {
            ThreadSpawnFailure::NotFound(format!(
                "parent thread authority not found: {parent_thread_id}"
            ))
        })?;
    if !runtime.thread_is_loaded(parent_thread_id) {
        return Err(ThreadSpawnFailure::NotFound(format!(
            "parent thread is not loaded: {parent_thread_id}"
        )));
    }
    validate_child_authority(&parent, draft).map_err(ThreadSpawnFailure::Authority)?;
    Ok(Some(parent))
}

fn validate_spawn_depth(
    store: &ServerStore,
    parent_thread_id: &str,
    maximum: u32,
) -> Result<(), ThreadSpawnFailure> {
    let mut next = Some(parent_thread_id.to_owned());
    let mut depth = 0_u32;
    while let Some(thread_id) = next {
        depth = depth.saturating_add(1);
        if depth > maximum {
            return Err(ThreadSpawnFailure::Authority(
                ThreadAuthorityError::SpawnDepth { maximum },
            ));
        }
        next = store
            .thread_authority(&thread_id)
            .map_err(|_| ThreadSpawnFailure::Persistence)?
            .ok_or_else(|| {
                ThreadSpawnFailure::NotFound(format!(
                    "parent thread authority not found: {thread_id}"
                ))
            })?
            .parent_thread_id;
    }
    Ok(())
}

pub(super) fn handle_thread_list(runtime: &DaemonRuntime, request: Envelope) -> Envelope {
    match crate::server_store::thread_authorities(&runtime.paths.server_db_path).and_then(
        |authorities| {
            authorities
                .into_iter()
                .map(|authority| joined_thread_status(runtime, authority))
                .collect::<AppResult<Vec<_>>>()
        },
    ) {
        Ok(threads) => Envelope::typed_response(
            request.id,
            ProtocolResponse::ThreadList(ThreadListResult { threads }),
        ),
        Err(_) => Envelope::error(
            request.id,
            Some("thread.list".into()),
            ERROR_THREAD_LIST_FAILED,
            "thread list readback failed",
        ),
    }
}

pub(super) fn handle_thread_status(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: ThreadStatusParams,
) -> Envelope {
    match crate::server_store::thread_authority(&runtime.paths.server_db_path, &params.thread_id)
        .and_then(|authority| {
            authority
                .map(|authority| joined_thread_status(runtime, authority))
                .transpose()
        }) {
        Ok(Some(thread)) => Envelope::typed_response(
            request.id,
            ProtocolResponse::ThreadStatus(ThreadStatusResult { thread }),
        ),
        Ok(None) => Envelope::error(
            request.id,
            Some("thread.status".into()),
            ERROR_NOT_FOUND,
            format!("thread not found: {}", params.thread_id),
        ),
        Err(_) => Envelope::error(
            request.id,
            Some("thread.status".into()),
            ERROR_THREAD_STATUS_FAILED,
            "thread status readback failed",
        ),
    }
}

pub(super) fn handle_thread_authority(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: ThreadAuthorityParams,
) -> Envelope {
    match crate::server_store::thread_authority(&runtime.paths.server_db_path, &params.thread_id)
        .and_then(|authority| {
            authority
                .map(|authority| {
                    crate::server_store::thread_confinement(
                        &runtime.paths.server_db_path,
                        &params.thread_id,
                    )
                    .map(|confinement| (authority, confinement))
                })
                .transpose()
        }) {
        Ok(Some((authority, confinement))) => Envelope::typed_response(
            request.id,
            ProtocolResponse::ThreadAuthority(ThreadAuthorityResult {
                authority,
                confinement,
            }),
        ),
        Ok(None) => Envelope::error(
            request.id,
            Some("thread.authority".into()),
            ERROR_NOT_FOUND,
            format!("thread authority not found: {}", params.thread_id),
        ),
        Err(_) => Envelope::error(
            request.id,
            Some("thread.authority".into()),
            ERROR_THREAD_AUTHORITY_FAILED,
            "thread authority readback failed",
        ),
    }
}

pub(super) fn handle_thread_send(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: ThreadSendParams,
) -> Envelope {
    if runtime.shutdown_accepted() {
        return shutting_down_response(request.id, "thread.send");
    }
    if let Err(message) = validate_thread_send(&params) {
        return Envelope::error(
            request.id,
            Some("thread.send".into()),
            ERROR_MALFORMED_REQUEST,
            message,
        );
    }
    let authority = match crate::server_store::thread_authority(
        &runtime.paths.server_db_path,
        &params.thread_id,
    ) {
        Ok(Some(authority)) => authority,
        Ok(None) => {
            return Envelope::error(
                request.id,
                Some("thread.send".into()),
                ERROR_NOT_FOUND,
                format!("thread not found: {}", params.thread_id),
            );
        }
        Err(_) => {
            return Envelope::error(
                request.id,
                Some("thread.send".into()),
                ERROR_THREAD_SEND_FAILED,
                "thread authority readback failed",
            );
        }
    };
    match crate::server_store::thread_stop(&runtime.paths.server_db_path, &params.thread_id) {
        Ok(Some(_)) => {
            return Envelope::error(
                request.id,
                Some("thread.send".into()),
                ERROR_NOT_FOUND,
                format!("thread not found: {}", params.thread_id),
            );
        }
        Ok(None) => {}
        Err(_) => {
            return Envelope::error(
                request.id,
                Some("thread.send".into()),
                ERROR_THREAD_SEND_FAILED,
                "thread stop readback failed",
            );
        }
    }
    let admission = runtime.send_thread(
        &params.thread_id,
        params.controller_id,
        params.turn_id.as_deref(),
        params.message.clone(),
        new_thread_turn_id(),
    );
    let (receipt, turn) = match admission {
        ThreadSendAdmission::ShuttingDown => {
            return shutting_down_response(request.id, "thread.send");
        }
        ThreadSendAdmission::Stopped => {
            return Envelope::error(
                request.id,
                Some("thread.send".into()),
                ERROR_NOT_FOUND,
                format!("thread not found: {}", params.thread_id),
            );
        }
        ThreadSendAdmission::Started { receipt, turn } => (receipt, turn),
        ThreadSendAdmission::Steered { receipt } | ThreadSendAdmission::Rejected { receipt } => {
            return Envelope::typed_response(request.id, ProtocolResponse::ThreadSend(receipt));
        }
    };
    let session_id = thread_session_id(&params.thread_id);
    let session = match crate::ledger::default_sqlite_session_status(
        &runtime.paths.default_ledger(),
        Some(&session_id),
    ) {
        Ok(Some(_)) => RunSession::Continue { session_id },
        Err(AppError::SessionNotFound(_)) => RunSession::Fresh { session_id },
        Ok(None) => RunSession::Fresh { session_id },
        Err(_) => {
            runtime.abort_thread_turn(&turn);
            return Envelope::error(
                request.id,
                Some("thread.send".into()),
                ERROR_THREAD_SEND_FAILED,
                "thread session readback failed",
            );
        }
    };
    let Some(workspace_root) = authority_working_directory(&authority).map(Path::to_path_buf)
    else {
        runtime.abort_thread_turn(&turn);
        return Envelope::error(
            request.id,
            Some("thread.send".into()),
            ERROR_THREAD_SEND_FAILED,
            "thread authority has no working directory",
        );
    };
    let confinement = match thread_child_confinement(runtime, &authority) {
        Ok(confinement) => confinement,
        Err(_) => {
            runtime.abort_thread_turn(&turn);
            return Envelope::error(
                request.id,
                Some("thread.send".into()),
                ERROR_THREAD_SEND_FAILED,
                "thread confinement readback failed",
            );
        }
    };
    let context = ThreadRunContext {
        workspace_root,
        approval_policy: authority.approval_policy,
        agent_id: authority
            .agent_id
            .unwrap_or_else(|| AgentId::new("plato").expect("static agent id is valid")),
        toolset: authority.toolset,
        turn: turn.clone(),
        confinement,
    };
    let response = start_run(
        runtime,
        StartRunRequest {
            request_id: request.id.clone(),
            question: params.message,
            session,
            config_path: None,
            overrides: RunOverrides {
                model: Some(authority.model),
                reasoning_effort: Some(authority.reasoning_effort),
            },
            approval_profile: None,
            prior_interrupted_run_id: params.prior_interrupted_run_id,
            wait: Some(false),
            thread_context: Some(context),
        },
    );
    if let Err(response) = response {
        runtime.abort_thread_turn(&turn);
        return *response;
    }
    Envelope::typed_response(request.id, ProtocolResponse::ThreadSend(receipt))
}

fn thread_child_confinement(
    runtime: &DaemonRuntime,
    authority: &crate::daemon::protocol::ThreadAuthorityRecord,
) -> AppResult<ChildConfinement> {
    match crate::server_store::thread_confinement(
        &runtime.paths.server_db_path,
        &authority.thread_id,
    )? {
        Some(ThreadConfinement::Landlock) => {
            let scratch = authority
                .granted_paths
                .iter()
                .find(|grant| grant.writable)
                .map(|grant| PathBuf::from(&grant.path))
                .ok_or_else(|| {
                    AppError::Config("confined thread authority has no scratch path".into())
                })?;
            let mut writable_paths = authority
                .worktrees
                .iter()
                .map(|worktree| PathBuf::from(&worktree.path))
                .collect::<Vec<_>>();
            writable_paths.extend(
                authority
                    .granted_paths
                    .iter()
                    .filter(|grant| grant.writable)
                    .map(|grant| PathBuf::from(&grant.path)),
            );
            Ok(ChildConfinement::Landlock {
                writable_paths,
                scratch,
            })
        }
        Some(ThreadConfinement::None) | None => Ok(ChildConfinement::None),
    }
}

fn validate_thread_send(params: &ThreadSendParams) -> Result<(), String> {
    ActorId::new(params.thread_id.clone()).map_err(|error| error.to_string())?;
    ActorId::new(params.controller_id.clone()).map_err(|error| error.to_string())?;
    if let Some(turn_id) = &params.turn_id {
        TurnId::new(turn_id.clone()).map_err(|error| error.to_string())?;
    }
    if params.turn_id.is_some() && params.prior_interrupted_run_id.is_some() {
        return Err("thread.send prior interruption is valid only when starting from idle".into());
    }
    if params.message.trim().is_empty() {
        return Err("thread.send message must not be empty".into());
    }
    Ok(())
}

pub(super) fn handle_thread_events(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: ThreadEventsParams,
) -> Envelope {
    let ThreadEventsParams {
        thread_id,
        from_offset,
        limit,
        wait_ms,
    } = params;
    if ActorId::new(thread_id.clone()).is_err() {
        return Envelope::error(
            request.id,
            Some("thread.events".into()),
            ERROR_MALFORMED_REQUEST,
            "thread.events thread_id is invalid",
        );
    }
    let limit = limit.unwrap_or(DEFAULT_EVENT_LIMIT);
    if limit == 0 || limit > MAX_EVENT_LIMIT {
        return Envelope::error(
            request.id,
            Some("thread.events".into()),
            ERROR_MALFORMED_REQUEST,
            format!("thread.events limit must be between 1 and {MAX_EVENT_LIMIT}"),
        );
    }
    let wait_ms = wait_ms.unwrap_or(0);
    if wait_ms > MAX_THREAD_EVENT_WAIT_MS {
        return Envelope::error(
            request.id,
            Some("thread.events".into()),
            ERROR_MALFORMED_REQUEST,
            format!("thread.events wait_ms must not exceed {MAX_THREAD_EVENT_WAIT_MS}"),
        );
    }
    match crate::server_store::thread_authority(&runtime.paths.server_db_path, &thread_id) {
        Ok(Some(_)) => {}
        Ok(None) => {
            return Envelope::error(
                request.id,
                Some("thread.events".into()),
                ERROR_NOT_FOUND,
                format!("thread not found: {thread_id}"),
            );
        }
        Err(_) => {
            return Envelope::error(
                request.id,
                Some("thread.events".into()),
                ERROR_THREAD_EVENTS_FAILED,
                "thread authority readback failed",
            );
        }
    }
    match crate::server_store::thread_stop(&runtime.paths.server_db_path, &thread_id) {
        Ok(Some(_)) => {
            return Envelope::error(
                request.id,
                Some("thread.events".into()),
                ERROR_NOT_FOUND,
                format!("thread not found: {thread_id}"),
            );
        }
        Ok(None) => {}
        Err(_) => {
            return Envelope::error(
                request.id,
                Some("thread.events".into()),
                ERROR_THREAD_EVENTS_FAILED,
                "thread stop readback failed",
            );
        }
    }
    match runtime.thread_events(
        &thread_id,
        from_offset,
        limit,
        std::time::Duration::from_millis(wait_ms),
    ) {
        Ok(result) => Envelope::typed_response(request.id, ProtocolResponse::ThreadEvents(result)),
        Err(ThreadEventsError::Lagged { first_offset }) => Envelope::error(
            request.id,
            Some("thread.events".into()),
            ERROR_LAGGED,
            format!(
                "requested thread events were evicted; first available offset is {first_offset}"
            ),
        ),
        Err(ThreadEventsError::Stopped) => Envelope::error(
            request.id,
            Some("thread.events".into()),
            ERROR_NOT_FOUND,
            format!("thread not found: {thread_id}"),
        ),
    }
}

pub(super) fn handle_thread_stop(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: ThreadStopParams,
) -> Envelope {
    let mut store = match runtime.paths.server_store() {
        Ok(store) => store,
        Err(_) => {
            return Envelope::error(
                request.id,
                Some("thread.stop".into()),
                ERROR_THREAD_STOP_FAILED,
                "thread stop ledger could not be opened",
            );
        }
    };
    let authority = match store.thread_authority(&params.thread_id) {
        Ok(Some(authority)) => authority,
        Ok(None) => {
            return Envelope::error(
                request.id,
                Some("thread.stop".into()),
                ERROR_NOT_FOUND,
                format!("thread not found: {}", params.thread_id),
            );
        }
        Err(_) => {
            return Envelope::error(
                request.id,
                Some("thread.stop".into()),
                ERROR_THREAD_STOP_FAILED,
                "thread stop authority readback failed",
            );
        }
    };
    let validated =
        match ThreadStopRecord::new(authority.thread_id.clone(), params.actor, None, now_ms()) {
            Ok(record) => record,
            Err(error) => {
                return Envelope::error(
                    request.id,
                    Some("thread.stop".into()),
                    ERROR_MALFORMED_REQUEST,
                    error.to_string(),
                );
            }
        };
    match store.thread_stop(&authority.thread_id) {
        Ok(Some(stop)) => {
            return Envelope::typed_response(
                request.id,
                ProtocolResponse::ThreadStop(thread_stop_result(stop, true)),
            );
        }
        Ok(None) => {}
        Err(_) => {
            return Envelope::error(
                request.id,
                Some("thread.stop".into()),
                ERROR_THREAD_STOP_FAILED,
                "thread stop readback failed",
            );
        }
    }

    let target = match runtime.begin_thread_stop(&authority.thread_id) {
        Ok(target) => target,
        Err(ThreadStopError::InProgress) => {
            return Envelope::error(
                request.id,
                Some("thread.stop".into()),
                ERROR_OVERLOAD,
                format!(
                    "thread stop is already in progress: {}",
                    authority.thread_id
                ),
            );
        }
        Err(ThreadStopError::AlreadyStopped) => {
            return match store.thread_stop(&authority.thread_id) {
                Ok(Some(stop)) => Envelope::typed_response(
                    request.id,
                    ProtocolResponse::ThreadStop(thread_stop_result(stop, true)),
                ),
                _ => Envelope::error(
                    request.id,
                    Some("thread.stop".into()),
                    ERROR_THREAD_STOP_FAILED,
                    "thread stopped without a durable stop record",
                ),
            };
        }
    };

    if let Some(run) = target.run.as_ref() {
        let _ = run.request_cancel();
        if !run.wait_for_terminal(THREAD_STOP_WAIT) {
            runtime.abort_thread_stop(&authority.thread_id);
            return Envelope::error(
                request.id,
                Some("thread.stop".into()),
                ERROR_THREAD_STOP_FAILED,
                format!(
                    "thread child did not reach zero residual within the stop bound: {}",
                    authority.thread_id
                ),
            );
        }
    }

    if let Err(error) = crate::thread_repository::integrate_and_discard(
        &runtime.paths.server_db_path,
        &runtime.paths.workspace_id,
        &authority,
    ) {
        runtime.abort_thread_stop(&authority.thread_id);
        return Envelope::error(
            request.id,
            Some("thread.stop".into()),
            ERROR_THREAD_STOP_FAILED,
            format!("thread repository integration failed: {error}"),
        );
    }

    let stop = ThreadStopRecord::new(
        authority.thread_id.clone(),
        validated.actor,
        target.turn_id,
        now_ms(),
    )
    .expect("thread stop inputs were validated before lifecycle execution");
    let (stop, inserted) = match store.persist_thread_stop(&stop) {
        Ok(result) => result,
        Err(_) => {
            runtime.abort_thread_stop(&authority.thread_id);
            return Envelope::error(
                request.id,
                Some("thread.stop".into()),
                ERROR_THREAD_STOP_FAILED,
                "thread stop could not be persisted",
            );
        }
    };
    runtime.complete_thread_stop(&authority.thread_id);
    Envelope::typed_response(
        request.id,
        ProtocolResponse::ThreadStop(thread_stop_result(stop, !inserted)),
    )
}

fn thread_stop_result(stop: ThreadStopRecord, already_stopped: bool) -> ThreadStopResult {
    if already_stopped {
        ThreadStopResult::AlreadyStopped {
            thread_id: stop.thread_id,
            stopped_turn_id: stop.stopped_turn_id,
            stopped_at_ms: stop.occurred_at_ms,
        }
    } else {
        ThreadStopResult::Stopped {
            thread_id: stop.thread_id,
            stopped_turn_id: stop.stopped_turn_id,
            stopped_at_ms: stop.occurred_at_ms,
        }
    }
}

pub(super) fn thread_session_id(thread_id: &str) -> String {
    format!("session_{thread_id}")
}

fn joined_thread_status(
    runtime: &DaemonRuntime,
    authority: crate::daemon::protocol::ThreadAuthorityRecord,
) -> AppResult<ThreadStatus> {
    let live = runtime.thread_live_state(&authority.thread_id);
    Ok(ThreadStatus {
        authority: legacy_status_authority(&authority)?,
        live,
    })
}

#[cfg(test)]
pub(in crate::daemon::handlers) mod tests {
    use super::*;

    use platonic_core::EffectClass;
    #[cfg(target_os = "linux")]
    use platonic_core::{AgentId, RunId};
    use serde_json::json;
    #[cfg(target_os = "linux")]
    use std::os::unix::fs::PermissionsExt;
    #[cfg(target_os = "linux")]
    use std::sync::Barrier;
    use std::{sync::Arc, time::Duration};

    #[cfg(target_os = "linux")]
    use crate::daemon::handlers::runs::{finish_run_after_event_collection, spawn_event_collector};
    use crate::daemon::handlers::{
        handle_line, handle_request, registry::tests::workspace_request,
        runs::tests::response_result,
    };
    #[cfg(target_os = "linux")]
    use crate::{ApprovalMode, RunLedger, RunOptions};
    use crate::{
        daemon::{
            protocol::{RunStateName, ThreadApprovalPolicy},
            runtime::{RunRecord, ThreadTurnBinding},
        },
        ledger::SqliteLedger,
        server_store::AgentRecord,
    };
    #[cfg(target_os = "linux")]
    use std::sync::mpsc;
    use std::{sync::atomic::Ordering, thread};

    pub(in crate::daemon::handlers) fn bare_thread_test_runtime()
    -> (tempfile::TempDir, DaemonRuntime) {
        bare_thread_test_runtime_with_max_spawn_depth(1)
    }

    pub(in crate::daemon::handlers) fn bare_thread_test_runtime_with_max_spawn_depth(
        max_spawn_depth: u32,
    ) -> (tempfile::TempDir, DaemonRuntime) {
        let root = tempfile::tempdir().unwrap();
        let workspace_root = root.path().join("workspace");
        std::fs::create_dir(&workspace_root).unwrap();
        let ledger_path = root
            .path()
            .join("state")
            .join("platonic")
            .join("workspaces")
            .join("thread-tests")
            .join("ledger.db");
        let runtime = DaemonRuntime::new_with_max_spawn_depth(
            crate::daemon::server::DaemonPaths {
                workspace_root: workspace_root.canonicalize().unwrap(),
                workspace_id: "thread-tests".into(),
                socket_path: root.path().join("agent.sock"),
                server_db_path: root.path().join("state/platonic/server.db"),
                ledger_path,
            },
            max_spawn_depth,
        );
        (root, runtime)
    }

    pub(in crate::daemon::handlers) fn thread_test_runtime() -> (tempfile::TempDir, DaemonRuntime) {
        thread_test_runtime_with_max_spawn_depth(1)
    }

    pub(in crate::daemon::handlers) fn thread_test_runtime_with_max_spawn_depth(
        max_spawn_depth: u32,
    ) -> (tempfile::TempDir, DaemonRuntime) {
        let (root, runtime) = repositoryless_thread_test_runtime(max_spawn_depth);
        init_git_repository(&runtime.paths.workspace_root);
        (root, runtime)
    }

    fn repositoryless_thread_test_runtime(
        max_spawn_depth: u32,
    ) -> (tempfile::TempDir, DaemonRuntime) {
        let (root, runtime) = bare_thread_test_runtime_with_max_spawn_depth(max_spawn_depth);
        runtime
            .paths
            .server_store()
            .unwrap()
            .register_workspace(
                "workspace-thread-tests",
                "thread-tests",
                &runtime.paths.workspace_root.to_string_lossy(),
                &runtime.paths.ledger_path.to_string_lossy(),
                1,
            )
            .unwrap();
        (root, runtime)
    }

    #[test]
    fn thread_spawn_rejects_an_unregistered_workspace_with_the_attach_error() {
        let (_root, runtime) = bare_thread_test_runtime();
        let response = handle_request(
            &runtime,
            workspace_request(
                "spawn",
                "thread.spawn",
                json!({
                    "action": "start",
                    "parent_thread_id": null,
                    "cwd": runtime.paths.workspace_root.to_string_lossy(),
                    "model": "gpt-5.6-sol",
                    "reasoning_effort": "none",
                    "approval_policy": "prompt"
                }),
            ),
        );
        let error = response.error.unwrap();
        assert_eq!(error.code, ERROR_WORKSPACE_UNREGISTERED);
        assert!(error.message.contains("platonic workspace create"));
        assert!(
            runtime
                .paths
                .server_store()
                .unwrap()
                .thread_authorities()
                .unwrap()
                .is_empty()
        );
    }

    /// After a restart the run is gone from memory, but the approval it was
    /// blocked on is not. The existing snapshot field must report it, so a
    /// client returning to the terminal sees what is waiting (#435).
    pub(in crate::daemon::handlers) fn start_thread(
        runtime: &DaemonRuntime,
        parent_thread_id: Option<String>,
        cwd: &Path,
        approval_policy: crate::daemon::protocol::ThreadApprovalPolicy,
    ) -> Result<ThreadSpawnResult, ThreadSpawnFailure> {
        thread_spawn(
            runtime,
            ThreadSpawnParams::Start {
                parent_thread_id,
                cwd: cwd.to_string_lossy().into_owned(),
                model: "gpt-5.6-sol".into(),
                reasoning_effort: crate::daemon::protocol::ReasoningEffort::Xhigh,
                approval_policy,
                repositories: Vec::new(),
            },
        )
    }

    fn start_thread_with_repositories(
        runtime: &DaemonRuntime,
        repositories: Vec<ThreadRepositoryRequest>,
    ) -> Result<ThreadSpawnResult, ThreadSpawnFailure> {
        thread_spawn(
            runtime,
            ThreadSpawnParams::Start {
                parent_thread_id: None,
                cwd: runtime.paths.workspace_root.to_string_lossy().into_owned(),
                model: "gpt-5.6-sol".into(),
                reasoning_effort: crate::daemon::protocol::ReasoningEffort::Xhigh,
                approval_policy: ThreadApprovalPolicy::Prompt,
                repositories,
            },
        )
    }

    fn git(cwd: &Path, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .current_dir(cwd)
            .args(args)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_TERMINAL_PROMPT", "0")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().into()
    }

    fn init_git_repository(path: &Path) -> String {
        if path.join(".git").is_dir() {
            return git(path, &["rev-parse", "HEAD"]);
        }
        git(path, &["init", "--quiet", "--initial-branch", "main"]);
        git(path, &["config", "user.name", "Platonic Test"]);
        git(path, &["config", "user.email", "platonic@example.invalid"]);
        std::fs::write(path.join("tracked.txt"), "user\n").unwrap();
        git(path, &["add", "tracked.txt"]);
        git(path, &["commit", "--quiet", "-m", "initial"]);
        git(path, &["rev-parse", "HEAD"])
    }

    pub(in crate::daemon::handlers) fn pending_spawn(
        result: ThreadSpawnResult,
    ) -> (String, String) {
        match result {
            ThreadSpawnResult::ApprovalRequired {
                spawn_id,
                thread_id,
                effect,
                reason,
            } => {
                assert_eq!(effect, EffectClass::WorkspaceWrite);
                assert_eq!(reason, THREAD_SPAWN_APPROVAL_REASON);
                (spawn_id, thread_id)
            }
            unexpected => panic!("expected approval-required spawn, got {unexpected:?}"),
        }
    }

    fn decide_thread(
        runtime: &DaemonRuntime,
        spawn_id: &str,
        approval: ThreadSpawnDecision,
    ) -> Result<ThreadSpawnResult, ThreadSpawnFailure> {
        thread_spawn(
            runtime,
            ThreadSpawnParams::Decide {
                spawn_id: spawn_id.into(),
                approval,
            },
        )
    }

    pub(in crate::daemon::handlers) fn grant_thread(
        runtime: &DaemonRuntime,
        spawn_id: &str,
        actor: &str,
    ) -> ThreadStatus {
        match decide_thread(
            runtime,
            spawn_id,
            ThreadSpawnDecision::Grant {
                actor: actor.into(),
            },
        )
        .unwrap()
        {
            ThreadSpawnResult::Spawned { thread } => thread,
            unexpected => panic!("expected spawned thread, got {unexpected:?}"),
        }
    }

    fn register_test_agent(
        runtime: &DaemonRuntime,
        id: &str,
        policy: ThreadApprovalPolicy,
        toolset: Vec<String>,
    ) {
        runtime
            .paths
            .server_store()
            .unwrap()
            .register_agent(&AgentRecord {
                id: platonic_core::AgentId::new(id).unwrap(),
                workspace_id: runtime.paths.workspace_id.clone(),
                model: "worker-model".into(),
                reasoning_effort: crate::daemon::protocol::ReasoningEffort::High,
                approval_policy: policy,
                toolset,
                created_at_ms: 1,
            })
            .unwrap();
    }

    fn coordinator_root(runtime: &DaemonRuntime) -> String {
        std::fs::write(
            runtime.paths.workspace_root.join("plato.toml"),
            "[tools]\nenabled = [\"file.read\", \"thread.spawn\"]\n",
        )
        .unwrap();
        let (spawn_id, thread_id) = pending_spawn(
            start_thread(
                runtime,
                None,
                &runtime.paths.workspace_root,
                ThreadApprovalPolicy::Prompt,
            )
            .unwrap(),
        );
        grant_thread(runtime, &spawn_id, "root_approver");
        thread_id
    }

    fn model_spawn_input(
        runtime: &DaemonRuntime,
        parent_thread_id: &str,
        agent_id: &str,
        toolset: Option<Vec<String>>,
        approval_policy: Option<ThreadApprovalPolicy>,
    ) -> ThreadSpawnToolInput {
        let cwd = thread_worktree(runtime, parent_thread_id)
            .to_string_lossy()
            .into_owned();
        ThreadSpawnToolInput {
            agent_id: agent_id.into(),
            cwd,
            model: None,
            reasoning_effort: None,
            approval_policy,
            toolset,
            repositories: None,
        }
    }

    fn thread_worktree(runtime: &DaemonRuntime, thread_id: &str) -> PathBuf {
        PathBuf::from(
            runtime
                .paths
                .server_store()
                .unwrap()
                .thread_authority(thread_id)
                .unwrap()
                .unwrap()
                .worktrees[0]
                .path
                .clone(),
        )
    }

    #[test]
    fn model_spawn_reuses_durable_admission_and_records_the_approving_actor() {
        let (_root, runtime) = thread_test_runtime();
        let parent_thread_id = coordinator_root(&runtime);
        register_test_agent(
            &runtime,
            "worker",
            ThreadApprovalPolicy::Prompt,
            vec!["file.read".into()],
        );

        let output = model_thread_spawn(
            &runtime,
            &parent_thread_id,
            model_spawn_input(&runtime, &parent_thread_id, "worker", None, None),
            "daemon".into(),
        )
        .unwrap();
        let child_thread_id = match output {
            ThreadSpawnToolOutput::Spawned { thread_id } => thread_id,
            output => panic!("expected spawned worker, got {output:?}"),
        };

        let store = runtime.paths.server_store().unwrap();
        let child = store.thread_authority(&child_thread_id).unwrap().unwrap();
        assert_eq!(
            child.parent_thread_id.as_deref(),
            Some(parent_thread_id.as_str())
        );
        assert_eq!(child.spawning_actor, "daemon");
        assert_eq!(
            child.agent_id,
            Some(platonic_core::AgentId::new("worker").unwrap())
        );
        assert_eq!(child.model, "worker-model");
        assert_eq!(child.toolset, ["file.read"]);
        assert_eq!(child.worktrees.len(), 1);
        assert_eq!(child.granted_paths.len(), 1);
        assert!(child.granted_paths[0].writable);
        assert!(!child.network);
        assert!(runtime.thread_is_loaded(&child_thread_id));
        drop(store);

        let connection = rusqlite::Connection::open(&runtime.paths.server_db_path).unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT decision, actor FROM thread_spawn_approvals WHERE thread_id = ?1",
                    [&child_thread_id],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .unwrap(),
            ("granted".into(), "daemon".into())
        );
    }

    #[test]
    fn model_spawn_returns_typed_rejections_for_authority_escalation() {
        let (_root, runtime) = thread_test_runtime();
        let parent_thread_id = coordinator_root(&runtime);
        register_test_agent(
            &runtime,
            "broad-worker",
            ThreadApprovalPolicy::Prompt,
            vec![
                "file.read".into(),
                "file.write".into(),
                "web.fetch".into(),
                "thread.spawn".into(),
            ],
        );
        register_test_agent(
            &runtime,
            "narrow-worker",
            ThreadApprovalPolicy::Prompt,
            vec!["file.read".into()],
        );

        let outside = tempfile::tempdir().unwrap();
        for (label, input) in [
            (
                "parent toolset",
                model_spawn_input(
                    &runtime,
                    &parent_thread_id,
                    "broad-worker",
                    Some(vec!["file.read".into(), "web.fetch".into()]),
                    None,
                ),
            ),
            (
                "parent policy",
                model_spawn_input(
                    &runtime,
                    &parent_thread_id,
                    "broad-worker",
                    Some(vec!["file.read".into()]),
                    Some(ThreadApprovalPolicy::Yolo),
                ),
            ),
            (
                "agent toolset",
                model_spawn_input(
                    &runtime,
                    &parent_thread_id,
                    "narrow-worker",
                    Some(vec!["file.write".into()]),
                    None,
                ),
            ),
            (
                "cwd",
                ThreadSpawnToolInput {
                    cwd: outside.path().to_string_lossy().into_owned(),
                    repositories: Some(vec![ThreadRepositoryRequest {
                        repo: ".".into(),
                        branch: None,
                    }]),
                    ..model_spawn_input(
                        &runtime,
                        &parent_thread_id,
                        "broad-worker",
                        Some(vec!["file.read".into()]),
                        None,
                    )
                },
            ),
        ] {
            let output =
                model_thread_spawn(&runtime, &parent_thread_id, input, "daemon".into()).unwrap();
            assert!(
                matches!(
                    output,
                    ThreadSpawnToolOutput::Rejected { ref code, .. }
                        if code == ERROR_THREAD_AUTHORITY_EXCEEDED.as_str()
                ),
                "{label} escalation was not rejected: {output:?}"
            );
        }
        assert_eq!(
            runtime
                .paths
                .server_store()
                .unwrap()
                .thread_authorities()
                .unwrap()
                .len(),
            1,
            "typed escalation rejections must not create child authority"
        );

        let first_child = match model_thread_spawn(
            &runtime,
            &parent_thread_id,
            model_spawn_input(
                &runtime,
                &parent_thread_id,
                "broad-worker",
                Some(vec!["file.read".into(), "thread.spawn".into()]),
                None,
            ),
            "daemon".into(),
        )
        .unwrap()
        {
            ThreadSpawnToolOutput::Spawned { thread_id } => thread_id,
            output => panic!("expected first bounded child, got {output:?}"),
        };
        let depth = model_thread_spawn(
            &runtime,
            &first_child,
            model_spawn_input(
                &runtime,
                &first_child,
                "broad-worker",
                Some(vec!["file.read".into()]),
                None,
            ),
            "daemon".into(),
        )
        .unwrap();
        assert!(matches!(
            depth,
            ThreadSpawnToolOutput::Rejected { code, reason }
                if code == ERROR_THREAD_AUTHORITY_EXCEEDED.as_str()
                    && reason.contains("spawn depth")
        ));
        assert_eq!(
            runtime
                .paths
                .server_store()
                .unwrap()
                .thread_authorities()
                .unwrap()
                .len(),
            2,
            "depth rejection must not create a grandchild authority"
        );
    }

    #[test]
    fn model_spawn_enforces_frozen_server_depth_after_workspace_mutation() {
        let (_root, runtime) = thread_test_runtime_with_max_spawn_depth(1);
        let parent_thread_id = coordinator_root(&runtime);
        register_test_agent(
            &runtime,
            "worker",
            ThreadApprovalPolicy::Prompt,
            vec!["file.read".into(), "thread.spawn".into()],
        );

        std::fs::write(
            runtime.paths.workspace_root.join("plato.toml"),
            "[limits]\nmax_spawn_depth = 99\n\n[tools]\nenabled = [\"file.read\", \"thread.spawn\"]\n",
        )
        .unwrap();

        let child_thread_id = match model_thread_spawn(
            &runtime,
            &parent_thread_id,
            model_spawn_input(
                &runtime,
                &parent_thread_id,
                "worker",
                Some(vec!["file.read".into(), "thread.spawn".into()]),
                None,
            ),
            "daemon".into(),
        )
        .unwrap()
        {
            ThreadSpawnToolOutput::Spawned { thread_id } => thread_id,
            output => panic!("expected first bounded child, got {output:?}"),
        };
        let rejected = model_thread_spawn(
            &runtime,
            &child_thread_id,
            model_spawn_input(
                &runtime,
                &child_thread_id,
                "worker",
                Some(vec!["file.read".into()]),
                None,
            ),
            "daemon".into(),
        )
        .unwrap();

        assert!(matches!(
            rejected,
            ThreadSpawnToolOutput::Rejected { code, reason }
                if code == ERROR_THREAD_AUTHORITY_EXCEEDED.as_str()
                    && reason == "child spawn depth exceeds server maximum 1"
        ));
        assert_eq!(runtime.max_spawn_depth(), 1);
        assert_eq!(
            runtime
                .paths
                .server_store()
                .unwrap()
                .thread_authorities()
                .unwrap()
                .len(),
            2,
            "workspace mutation must not admit a grandchild authority"
        );
    }

    #[test]
    fn model_spawn_enforces_configured_server_depth_at_admission() {
        let (_root, runtime) = thread_test_runtime_with_max_spawn_depth(2);
        let parent_thread_id = coordinator_root(&runtime);
        register_test_agent(
            &runtime,
            "worker",
            ThreadApprovalPolicy::Prompt,
            vec!["file.read".into(), "thread.spawn".into()],
        );

        let child_thread_id = match model_thread_spawn(
            &runtime,
            &parent_thread_id,
            model_spawn_input(
                &runtime,
                &parent_thread_id,
                "worker",
                Some(vec!["file.read".into(), "thread.spawn".into()]),
                None,
            ),
            "daemon".into(),
        )
        .unwrap()
        {
            ThreadSpawnToolOutput::Spawned { thread_id } => thread_id,
            output => panic!("expected depth-one child, got {output:?}"),
        };
        let grandchild_thread_id = match model_thread_spawn(
            &runtime,
            &child_thread_id,
            model_spawn_input(
                &runtime,
                &child_thread_id,
                "worker",
                Some(vec!["file.read".into(), "thread.spawn".into()]),
                None,
            ),
            "daemon".into(),
        )
        .unwrap()
        {
            ThreadSpawnToolOutput::Spawned { thread_id } => thread_id,
            output => panic!("expected depth-two grandchild, got {output:?}"),
        };
        let rejected = model_thread_spawn(
            &runtime,
            &grandchild_thread_id,
            model_spawn_input(
                &runtime,
                &grandchild_thread_id,
                "worker",
                Some(vec!["file.read".into()]),
                None,
            ),
            "daemon".into(),
        )
        .unwrap();

        assert!(matches!(
            rejected,
            ThreadSpawnToolOutput::Rejected { code, reason }
                if code == ERROR_THREAD_AUTHORITY_EXCEEDED.as_str()
                    && reason == "child spawn depth exceeds server maximum 2"
        ));
        assert_eq!(runtime.max_spawn_depth(), 2);
        assert_eq!(
            runtime
                .paths
                .server_store()
                .unwrap()
                .thread_authorities()
                .unwrap()
                .len(),
            3,
            "configured depth two must admit exactly two descendant authorities"
        );
    }

    fn stop_thread(runtime: &DaemonRuntime, thread_id: &str, actor: &str) -> Envelope {
        handle_line(
            runtime,
            &format!(
                r#"{{"v":1,"id":"stop","kind":"request","method":"thread.stop","params":{{"thread_id":"{thread_id}","actor":"{actor}"}}}}"#
            ),
        )
    }

    #[test]
    fn thread_spawn_becomes_live_only_after_complete_authority_is_durable() {
        let (_root, runtime) = thread_test_runtime();
        let (spawn_id, thread_id) = pending_spawn(
            start_thread(
                &runtime,
                None,
                &runtime.paths.workspace_root,
                crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
            )
            .unwrap(),
        );
        let store = runtime.paths.server_store().unwrap();
        assert!(store.thread_authority(&thread_id).unwrap().is_none());
        assert!(!runtime.thread_is_loaded(&thread_id));
        drop(store);

        let status = grant_thread(&runtime, &spawn_id, "stdin");
        assert_eq!(status.authority.thread_id, thread_id);
        assert_eq!(status.authority.spawning_actor, "stdin");
        assert_eq!(status.authority.parent_thread_id, None);
        let authority_response = handle_line(
            &runtime,
            &format!(
                r#"{{"v":1,"id":"authority","kind":"request","method":"thread.authority","params":{{"thread_id":"{thread_id}"}}}}"#
            ),
        );
        let authority: ThreadAuthorityResult = response_result(&authority_response);
        let expected_confinement = match runtime.confinement_support() {
            crate::confinement::ConfinementSupport::Landlock => ThreadConfinement::Landlock,
            crate::confinement::ConfinementSupport::None => ThreadConfinement::None,
        };
        assert_eq!(authority.confinement, Some(expected_confinement));
        let authority = authority.authority;
        assert_eq!(authority.agent_id, Some(AgentId::new("plato").unwrap()));
        assert_eq!(authority.worktrees.len(), 1);
        assert_eq!(authority.worktrees[0].repo, ".");
        assert_eq!(authority.worktrees[0].branch, format!("thread/{thread_id}"));
        assert_eq!(status.authority.cwd, authority.worktrees[0].path);
        assert_eq!(authority.granted_paths.len(), 1);
        assert!(authority.granted_paths[0].writable);
        assert_eq!(
            authority.toolset,
            [
                "file.read",
                "file.list",
                "file.write",
                "file.edit",
                "shell.exec",
                "web.fetch",
            ]
        );
        assert!(authority.network);
        assert_eq!(status.authority.model, "gpt-5.6-sol");
        assert_eq!(
            status.authority.reasoning_effort,
            crate::daemon::protocol::ReasoningEffort::Xhigh
        );
        assert_eq!(
            status.authority.approval_policy,
            crate::daemon::protocol::ThreadApprovalPolicy::Prompt
        );
        assert!(status.authority.created_at_ms > 0);
        assert_eq!(
            status.live,
            crate::daemon::protocol::ThreadLiveState {
                loaded: true,
                current_turn_id: None,
                last_activity_at_ms: Some(authority.created_at_ms),
            }
        );
        let store = runtime.paths.server_store().unwrap();
        assert_eq!(store.thread_authority(&thread_id).unwrap(), Some(authority));
        let approval = store.thread_spawn_approval(&spawn_id).unwrap().unwrap();
        assert_eq!(approval.decision, ThreadSpawnDecisionName::Granted);
        assert_eq!(approval.actor, status.authority.spawning_actor);
    }

    #[test]
    fn repositoryless_spawn_is_rejected_before_admission_or_server_owned_state() {
        let (_root, runtime) = repositoryless_thread_test_runtime(1);

        let error = start_thread(
            &runtime,
            None,
            &runtime.paths.workspace_root,
            ThreadApprovalPolicy::Prompt,
        )
        .unwrap_err();

        assert!(
            matches!(
                &error,
                ThreadSpawnFailure::Malformed(message)
                    if message.contains("thread spawn requires a named Git repository and claimed branch")
            ),
            "unexpected repository-less spawn failure: {error:?}"
        );
        let store = runtime.paths.server_store().unwrap();
        assert!(store.thread_authorities().unwrap().is_empty());
        assert!(store.branch_claims().unwrap().is_empty());
        assert!(
            !crate::paths::thread_repositories_root(&runtime.paths.server_db_path)
                .unwrap()
                .exists()
        );
    }

    #[test]
    fn landlock_support_is_always_recorded_for_an_admitted_spawn() {
        let (_root, base) = thread_test_runtime();
        let runtime = DaemonRuntime::new_with_server_policy(
            base.paths,
            1,
            false,
            crate::confinement::ConfinementSupport::Landlock,
        );
        let (spawn_id, thread_id) = pending_spawn(
            start_thread(
                &runtime,
                None,
                &runtime.paths.workspace_root,
                ThreadApprovalPolicy::Prompt,
            )
            .unwrap(),
        );

        grant_thread(&runtime, &spawn_id, "stdin");

        assert_eq!(
            runtime
                .paths
                .server_store()
                .unwrap()
                .thread_confinement(&thread_id)
                .unwrap(),
            Some(ThreadConfinement::Landlock)
        );
    }

    #[test]
    fn git_thread_stop_integrates_private_commit_and_cleans_only_server_owned_state() {
        let (_root, runtime) = thread_test_runtime();
        let user_commit = init_git_repository(&runtime.paths.workspace_root);
        let (spawn_id, thread_id) = pending_spawn(
            start_thread(
                &runtime,
                None,
                &runtime.paths.workspace_root,
                ThreadApprovalPolicy::Prompt,
            )
            .unwrap(),
        );
        grant_thread(&runtime, &spawn_id, "stdin");
        let store = runtime.paths.server_store().unwrap();
        let authority = store.thread_authority(&thread_id).unwrap().unwrap();
        assert_eq!(authority.worktrees.len(), 1);
        assert_eq!(authority.worktrees[0].repo, ".");
        assert_eq!(authority.worktrees[0].branch, format!("thread/{thread_id}"));
        assert_eq!(store.branch_claims().unwrap().len(), 1);
        drop(store);
        let response = handle_line(
            &runtime,
            &format!(
                r#"{{"v":1,"id":"authority","kind":"request","method":"thread.authority","params":{{"thread_id":"{thread_id}"}}}}"#
            ),
        );
        let status: ThreadAuthorityResult = response_result(&response);
        let expected_confinement = match runtime.confinement_support() {
            crate::confinement::ConfinementSupport::Landlock => ThreadConfinement::Landlock,
            crate::confinement::ConfinementSupport::None => ThreadConfinement::None,
        };
        assert_eq!(status.confinement, Some(expected_confinement));
        assert_eq!(status.authority.worktrees, authority.worktrees);

        let private = Path::new(&authority.worktrees[0].path);
        git(private, &["config", "user.name", "Platonic Test"]);
        git(
            private,
            &["config", "user.email", "platonic@example.invalid"],
        );
        std::fs::write(private.join("private.txt"), "private\n").unwrap();
        git(private, &["add", "private.txt"]);
        git(private, &["commit", "--quiet", "-m", "private"]);
        let private_commit = git(private, &["rev-parse", "HEAD"]);

        let response = stop_thread(&runtime, &thread_id, "stdin");
        let _: ThreadStopResult = response_result(&response);
        assert!(
            !crate::paths::thread_repository_root(&runtime.paths.server_db_path, &thread_id,)
                .unwrap()
                .exists()
        );
        let store = runtime.paths.server_store().unwrap();
        assert!(store.branch_claims().unwrap().is_empty());
        drop(store);
        let shared = crate::thread_repository::shared_repository_path(
            &runtime.paths.server_db_path,
            &runtime.paths.workspace_id,
            ".",
        )
        .unwrap();
        assert_eq!(
            git(
                shared.parent().unwrap(),
                &[
                    "--git-dir",
                    &shared.to_string_lossy(),
                    "rev-parse",
                    &format!("refs/heads/thread/{thread_id}")
                ]
            ),
            private_commit
        );
        assert_eq!(
            git(&runtime.paths.workspace_root, &["rev-parse", "HEAD"]),
            user_commit
        );
        assert!(!runtime.paths.workspace_root.join("private.txt").exists());
    }

    #[test]
    fn branch_conflict_and_require_confinement_fail_before_child_launch() {
        let (_root, runtime) = thread_test_runtime();
        init_git_repository(&runtime.paths.workspace_root);
        let request = ThreadRepositoryRequest {
            repo: ".".into(),
            branch: Some("main".into()),
        };
        let (first_spawn, _) =
            pending_spawn(start_thread_with_repositories(&runtime, vec![request.clone()]).unwrap());
        grant_thread(&runtime, &first_spawn, "stdin");
        let (second_spawn, second_thread) =
            pending_spawn(start_thread_with_repositories(&runtime, vec![request]).unwrap());
        let conflict = handle_line(
            &runtime,
            &format!(
                r#"{{"v":1,"id":"conflict","kind":"request","method":"thread.spawn","params":{{"action":"decide","spawn_id":"{second_spawn}","approval":{{"decision":"grant","actor":"stdin"}}}}}}"#
            ),
        );
        assert_eq!(
            conflict.error.unwrap().code,
            ERROR_THREAD_BRANCH_CLAIM_CONFLICT
        );
        assert!(
            runtime
                .paths
                .server_store()
                .unwrap()
                .thread_authority(&second_thread)
                .unwrap()
                .is_none()
        );
        assert!(
            !crate::paths::thread_repository_root(&runtime.paths.server_db_path, &second_thread,)
                .unwrap()
                .exists()
        );

        let paths = runtime.paths.clone();
        let fallback = DaemonRuntime::new_with_server_policy(
            paths.clone(),
            1,
            false,
            crate::confinement::ConfinementSupport::None,
        );
        let (fallback_spawn, fallback_thread) = pending_spawn(
            start_thread_with_repositories(
                &fallback,
                vec![ThreadRepositoryRequest {
                    repo: ".".into(),
                    branch: None,
                }],
            )
            .unwrap(),
        );
        grant_thread(&fallback, &fallback_spawn, "stdin");
        assert_eq!(
            fallback
                .paths
                .server_store()
                .unwrap()
                .thread_confinement(&fallback_thread)
                .unwrap(),
            Some(ThreadConfinement::None)
        );
        let fallback_status = handle_line(
            &fallback,
            &format!(
                r#"{{"v":1,"id":"fallback-status","kind":"request","method":"thread.authority","params":{{"thread_id":"{fallback_thread}"}}}}"#
            ),
        );
        let fallback_status: ThreadAuthorityResult = response_result(&fallback_status);
        assert_eq!(fallback_status.confinement, Some(ThreadConfinement::None));

        let required = DaemonRuntime::new_with_server_policy(
            paths,
            1,
            true,
            crate::confinement::ConfinementSupport::None,
        );
        let (spawn_id, required_thread) = pending_spawn(
            start_thread_with_repositories(
                &required,
                vec![ThreadRepositoryRequest {
                    repo: ".".into(),
                    branch: None,
                }],
            )
            .unwrap(),
        );
        let unavailable = handle_line(
            &required,
            &format!(
                r#"{{"v":1,"id":"required","kind":"request","method":"thread.spawn","params":{{"action":"decide","spawn_id":"{spawn_id}","approval":{{"decision":"grant","actor":"stdin"}}}}}}"#
            ),
        );
        assert_eq!(
            unavailable.error.unwrap().code,
            ERROR_THREAD_CONFINEMENT_UNAVAILABLE
        );
        assert!(
            !crate::paths::thread_repository_root(
                &required.paths.server_db_path,
                &required_thread,
            )
            .unwrap()
            .exists()
        );
        assert!(
            required
                .paths
                .server_store()
                .unwrap()
                .branch_claims()
                .unwrap()
                .iter()
                .all(|claim| claim.thread_id != required_thread)
        );
    }

    #[test]
    fn thread_spawn_denial_and_cancellation_leave_no_live_authority() {
        for (case, decision, expected) in [
            (
                "denied",
                ThreadSpawnDecision::Deny {
                    actor: "reviewer".into(),
                    reason: "not admitted".into(),
                },
                ThreadSpawnDecisionName::Denied,
            ),
            (
                "canceled",
                ThreadSpawnDecision::Cancel {
                    actor: "stdin".into(),
                },
                ThreadSpawnDecisionName::Canceled,
            ),
        ] {
            let (_root, runtime) = thread_test_runtime();
            let (spawn_id, thread_id) = pending_spawn(
                start_thread(
                    &runtime,
                    None,
                    &runtime.paths.workspace_root,
                    crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
                )
                .unwrap(),
            );
            let result = decide_thread(&runtime, &spawn_id, decision).unwrap();
            assert!(matches!(
                (&result, case),
                (ThreadSpawnResult::Denied { .. }, "denied")
                    | (ThreadSpawnResult::Canceled { .. }, "canceled")
            ));
            let store = runtime.paths.server_store().unwrap();
            let approval = store.thread_spawn_approval(&spawn_id).unwrap().unwrap();
            assert_eq!(approval.decision, expected);
            assert!(store.thread_authority(&thread_id).unwrap().is_none());
            assert!(!runtime.thread_is_loaded(&thread_id));
        }
    }

    fn admit_test_thread_turn(
        runtime: &DaemonRuntime,
        thread_id: &str,
        controller_id: &str,
        turn_id: &str,
    ) -> ThreadTurnBinding {
        match runtime.send_thread(
            thread_id,
            controller_id.into(),
            None,
            "test turn".into(),
            turn_id.into(),
        ) {
            ThreadSendAdmission::Started { turn, .. } => turn,
            admission => panic!("test thread turn was not admitted: {admission:?}"),
        }
    }

    #[test]
    fn idle_thread_stop_is_durable_idempotent_and_leaves_sibling_untouched() {
        let (_root, runtime) = thread_test_runtime();
        let mut threads = Vec::new();
        for _ in 0..2 {
            let (spawn_id, _) = pending_spawn(
                start_thread(
                    &runtime,
                    None,
                    &runtime.paths.workspace_root,
                    crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
                )
                .unwrap(),
            );
            threads.push(grant_thread(&runtime, &spawn_id, "stdin"));
        }
        let target_id = threads[0].authority.thread_id.clone();
        let sibling_id = threads[1].authority.thread_id.clone();
        let sibling_before = runtime.thread_live_state(&sibling_id);

        let stopped = stop_thread(&runtime, &target_id, "operator");
        let stopped: ThreadStopResult = response_result(&stopped);
        let stopped_at_ms = match stopped {
            ThreadStopResult::Stopped {
                thread_id,
                stopped_turn_id,
                stopped_at_ms,
            } => {
                assert_eq!(thread_id, target_id);
                assert_eq!(stopped_turn_id, None);
                stopped_at_ms
            }
            unexpected => panic!("expected stopped result, got {unexpected:?}"),
        };
        assert_eq!(
            runtime.thread_live_state(&target_id),
            crate::daemon::protocol::ThreadLiveState {
                loaded: false,
                current_turn_id: None,
                last_activity_at_ms: None,
            }
        );
        assert_eq!(runtime.thread_live_state(&sibling_id), sibling_before);
        let store = runtime.paths.server_store().unwrap();
        let durable = store.thread_stop(&target_id).unwrap().unwrap();
        assert_eq!(durable.actor, "operator");
        assert_eq!(durable.stopped_turn_id, None);
        assert_eq!(durable.occurred_at_ms, stopped_at_ms);
        drop(store);

        let repeated = stop_thread(&runtime, &target_id, "other_operator");
        assert_eq!(
            response_result::<ThreadStopResult>(&repeated),
            ThreadStopResult::AlreadyStopped {
                thread_id: target_id.clone(),
                stopped_turn_id: None,
                stopped_at_ms,
            }
        );
        assert!(runtime.thread_is_stopped(&target_id));
        assert_eq!(runtime.thread_live_state(&sibling_id), sibling_before);
    }

    #[test]
    fn active_thread_stop_cancels_bound_run_before_unloading_only_that_thread() {
        let (_root, runtime) = thread_test_runtime();
        let mut threads = Vec::new();
        for _ in 0..2 {
            let (spawn_id, _) = pending_spawn(
                start_thread(
                    &runtime,
                    None,
                    &runtime.paths.workspace_root,
                    crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
                )
                .unwrap(),
            );
            threads.push(grant_thread(&runtime, &spawn_id, "stdin"));
        }
        let target_id = threads[0].authority.thread_id.clone();
        let sibling_id = threads[1].authority.thread_id.clone();
        let sibling_before = runtime.thread_live_state(&sibling_id);
        let turn = admit_test_thread_turn(&runtime, &target_id, "controller", "turn_active");
        let record = Arc::new(RunRecord::new_for_thread(
            "run_thread_stop".into(),
            "session_thread_stop".into(),
            runtime.paths.ledger_path.clone(),
            turn.clone(),
        ));
        runtime.reserve_run(record.clone()).unwrap();
        runtime.bind_thread_run(&turn, record.clone()).unwrap();
        let worker_runtime = runtime.clone();
        let worker_record = record.clone();
        let worker = thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            while !worker_record.cancel.load(Ordering::SeqCst) {
                assert!(
                    std::time::Instant::now() < deadline,
                    "thread stop did not request child cancellation"
                );
                thread::yield_now();
            }
            worker_runtime.finish_run_with_error(&worker_record, &AppError::RunCanceled);
        });

        let stopped = stop_thread(&runtime, &target_id, "operator");
        worker.join().unwrap();
        assert_eq!(
            response_result::<ThreadStopResult>(&stopped),
            ThreadStopResult::Stopped {
                thread_id: target_id.clone(),
                stopped_turn_id: Some("turn_active".into()),
                stopped_at_ms: runtime
                    .paths
                    .server_store()
                    .unwrap()
                    .thread_stop(&target_id)
                    .unwrap()
                    .unwrap()
                    .occurred_at_ms,
            }
        );
        assert_eq!(record.status().state, RunStateName::Canceled);
        assert!(!runtime.thread_is_loaded(&target_id));
        assert_eq!(runtime.thread_live_state(&sibling_id), sibling_before);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn thread_stop_supervised_child_matrix_reaches_zero_residual() {
        for wedged in [false, true] {
            assert_thread_stop_supervised_child(wedged);
        }
    }

    #[cfg(target_os = "linux")]
    fn assert_thread_stop_supervised_child(wedged: bool) {
        use crate::{
            app::prepare_run,
            daemon::run_child::{
                SupervisedTestLaunch, TerminalStageBarriers, run_supervised_for_test,
            },
        };

        let (_root, runtime) = thread_test_runtime();
        let mut threads = Vec::new();
        for _ in 0..2 {
            let (spawn_id, _) = pending_spawn(
                start_thread(
                    &runtime,
                    None,
                    &runtime.paths.workspace_root,
                    crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
                )
                .unwrap(),
            );
            threads.push(grant_thread(&runtime, &spawn_id, "stdin"));
        }
        let target_id = threads[0].authority.thread_id.clone();
        let sibling_id = threads[1].authority.thread_id.clone();
        let case = if wedged { "wedged" } else { "active" };
        let run_id = RunId::new(format!("run_thread_stop_{case}")).unwrap();
        let turn =
            admit_test_thread_turn(&runtime, &target_id, "controller", &format!("turn_{case}"));
        let record = Arc::new(RunRecord::new_for_thread(
            run_id.to_string(),
            format!("session_thread_stop_{case}"),
            runtime.paths.ledger_path.clone(),
            turn.clone(),
        ));
        runtime.reserve_run(record.clone()).unwrap();
        runtime.bind_thread_run(&turn, record.clone()).unwrap();
        let sibling_turn = admit_test_thread_turn(
            &runtime,
            &sibling_id,
            "sibling_controller",
            &format!("turn_sibling_{case}"),
        );
        let sibling = Arc::new(RunRecord::new_for_thread(
            format!("run_sibling_{case}"),
            format!("session_sibling_{case}"),
            runtime.paths.ledger_path.clone(),
            sibling_turn.clone(),
        ));
        runtime.reserve_run(sibling.clone()).unwrap();
        runtime
            .bind_thread_run(&sibling_turn, sibling.clone())
            .unwrap();

        std::fs::write(
            runtime.paths.workspace_root.join("plato.toml"),
            r#"[provider]
kind = "open_ai"
model = "test-model"
api_key_env = "PATH"
base_url = "http://127.0.0.1:1"

[limits]
token_budget = 4000
max_output_tokens = 64
max_turns = 1

[tools]
enabled = ["file.read"]
"#,
        )
        .unwrap();
        let (prepared, recorder) = prepare_run(&RunOptions {
            question: "exercise thread.stop child ownership".into(),
            config_path: Some(runtime.paths.workspace_root.join("plato.toml")),
            overrides: Default::default(),
            ledger: RunLedger::DefaultSqlite(runtime.paths.default_ledger()),
            workspace_root: runtime.paths.workspace_root.clone(),
            approval_mode: ApprovalMode::Deny { actor: "test" },
            run_id: Some(run_id.clone()),
            session: Some(RunSession::Fresh {
                session_id: record.session_id.clone(),
            }),
            event_sender: None,
            stream_to_stderr: false,
            cancel: None,
            voice_interruption_context: None,
        })
        .unwrap();
        let run_started = json!({
            "kind": "record",
            "request_id": 1,
            "operation": {
                "operation": "event",
                "event": {"event": "run_started", "run_id": run_id, "agent_id": "plato"}
            }
        });
        let terminal = json!({
            "kind": "record",
            "request_id": 2,
            "operation": {
                "operation": "fail",
                "run_id": run_id,
                "error": crate::ledger::RUN_CANCELED_REASON,
                "canceled": true
            }
        });
        let canceled = json!({
            "kind": "result",
            "request_id": 3,
            "result": {"status": "canceled"}
        });
        let descendant_pid_path = runtime.paths.workspace_root.join(format!("{case}.pid"));
        let fixture = runtime.paths.workspace_root.join(format!("{case}-child"));
        let body = if wedged {
            format!(
                r#"trap '' TERM
IFS= read -r _
printf '{{"kind":"ready","request_id":0,"pid":%s}}\n' "$$"
IFS= read -r _
printf '%s\n' '{run_started}'
IFS= read -r _
/bin/sh -c 'trap "" TERM; while :; do :; done' &
printf '%s\n' "$!" > '{descendant_pid_path}.tmp'
mv '{descendant_pid_path}.tmp' '{descendant_pid_path}'
IFS= read -r _
while :; do :; done
"#,
                descendant_pid_path = descendant_pid_path.display(),
            )
        } else {
            format!(
                r#"IFS= read -r _
printf '{{"kind":"ready","request_id":0,"pid":%s}}\n' "$$"
IFS= read -r _
printf '%s\n' '{run_started}'
IFS= read -r _
IFS= read -r _
printf '%s\n' '{terminal}'
IFS= read -r _
printf '%s\n' '{canceled}'
IFS= read -r _
"#,
            )
        };
        std::fs::write(&fixture, format!("#!/bin/sh\n{body}")).unwrap();
        std::fs::set_permissions(&fixture, std::fs::Permissions::from_mode(0o700)).unwrap();

        let (event_sender, event_receiver) = mpsc::channel();
        let event_collector = spawn_event_collector(record.clone(), event_receiver);
        let (ready_sender, ready_receiver) = mpsc::channel();
        let terminal_reached = Arc::new(Barrier::new(2));
        let terminal_release = Arc::new(Barrier::new(2));
        let terminal_driver = (!wedged).then(|| {
            let reached = terminal_reached.clone();
            let release = terminal_release.clone();
            thread::spawn(move || {
                reached.wait();
                release.wait();
            })
        });
        let worker_runtime = runtime.clone();
        let worker_record = record.clone();
        let worker_cancel = record.cancel.clone();
        let (outcome_sender, outcome_receiver) = mpsc::channel();
        let worker = thread::spawn(move || {
            let completion = run_supervised_for_test(
                prepared,
                recorder,
                ApprovalMode::Deny { actor: "test" },
                event_sender,
                worker_cancel,
                None,
                SupervisedTestLaunch {
                    executable: fixture,
                    ready_child: ready_sender,
                    terminal_stage_barriers: TerminalStageBarriers {
                        reached: terminal_reached,
                        release: terminal_release,
                    },
                },
            );
            let outcome = finish_run_after_event_collection(
                &worker_runtime,
                &worker_record,
                completion,
                event_collector,
            );
            outcome_sender.send(outcome).unwrap();
        });
        let child_pid = ready_receiver.recv_timeout(Duration::from_secs(2)).unwrap();
        let descendant_pid = wedged.then(|| {
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            while !descendant_pid_path.exists() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "wedged descendant pid was not published"
                );
                thread::yield_now();
            }
            std::fs::read_to_string(&descendant_pid_path)
                .unwrap()
                .trim()
                .parse::<u32>()
                .unwrap()
        });

        let stopped = stop_thread(&runtime, &target_id, "operator");
        assert!(matches!(
            response_result::<ThreadStopResult>(&stopped),
            ThreadStopResult::Stopped {
                stopped_turn_id: Some(ref turn_id),
                ..
            } if turn_id == &format!("turn_{case}")
        ));
        assert!(matches!(
            outcome_receiver
                .recv_timeout(Duration::from_secs(5))
                .unwrap(),
            Err(AppError::RunCanceled)
        ));
        worker.join().unwrap();
        if let Some(driver) = terminal_driver {
            driver.join().unwrap();
        }
        assert!(!Path::new(&format!("/proc/{child_pid}")).exists());
        if let Some(descendant_pid) = descendant_pid {
            assert!(!Path::new(&format!("/proc/{descendant_pid}")).exists());
        }
        assert!(!runtime.thread_is_loaded(&target_id));
        assert_eq!(
            runtime
                .thread_live_state(&sibling_id)
                .current_turn_id
                .as_deref(),
            Some(format!("turn_sibling_{case}").as_str())
        );
        assert!(!sibling.cancel.load(Ordering::SeqCst));
        assert_eq!(sibling.status().state, RunStateName::Running);
        runtime.finish_run_with_error(&sibling, &AppError::RunCanceled);
    }

    #[test]
    fn thread_activity_is_live_monotone_and_absent_after_restart() {
        let (_root, runtime) = thread_test_runtime();
        let (spawn_id, _) = pending_spawn(
            start_thread(
                &runtime,
                None,
                &runtime.paths.workspace_root,
                crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
            )
            .unwrap(),
        );
        let thread = grant_thread(&runtime, &spawn_id, "stdin");
        let thread_id = thread.authority.thread_id.clone();
        let later = thread.authority.created_at_ms.saturating_add(1_000);
        runtime.note_thread_activity_at(&thread_id, later);
        runtime.note_thread_activity_at(&thread_id, later.saturating_sub(500));
        assert_eq!(
            runtime.thread_live_state(&thread_id).last_activity_at_ms,
            Some(later)
        );

        let restarted = DaemonRuntime::new(runtime.paths.clone());
        assert_eq!(
            restarted.thread_live_state(&thread_id),
            crate::daemon::protocol::ThreadLiveState {
                loaded: false,
                current_turn_id: None,
                last_activity_at_ms: None,
            }
        );
        let columns = rusqlite::Connection::open(&runtime.paths.server_db_path)
            .unwrap()
            .prepare("PRAGMA table_info(thread_authorities)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(columns.len(), 13);
        assert!(!columns.iter().any(|column| column.contains("activity")));
    }

    #[test]
    fn thread_spawn_persistence_failure_releases_claim_without_live_thread() {
        let (_root, runtime) = thread_test_runtime();
        let (spawn_id, thread_id) = pending_spawn(
            start_thread(
                &runtime,
                None,
                &runtime.paths.workspace_root,
                crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
            )
            .unwrap(),
        );
        // The authority table lives in the server-wide store now, so the
        // injected failure has to be planted there to exercise the same path.
        drop(runtime.paths.server_store().unwrap());
        let connection = rusqlite::Connection::open(&runtime.paths.server_db_path).unwrap();
        connection
            .execute_batch(
                "CREATE TRIGGER fail_thread_authority_insert
                 BEFORE INSERT ON thread_authorities
                 BEGIN SELECT RAISE(ABORT, 'injected authority failure'); END;",
            )
            .unwrap();
        drop(connection);

        assert!(matches!(
            decide_thread(
                &runtime,
                &spawn_id,
                ThreadSpawnDecision::Grant {
                    actor: "stdin".into()
                }
            ),
            Err(ThreadSpawnFailure::Persistence)
        ));
        let store = runtime.paths.server_store().unwrap();
        assert!(store.thread_authority(&thread_id).unwrap().is_none());
        assert!(store.thread_spawn_approval(&spawn_id).unwrap().is_none());
        assert!(!runtime.thread_is_loaded(&thread_id));
        drop(store);

        let connection = rusqlite::Connection::open(&runtime.paths.server_db_path).unwrap();
        connection
            .execute_batch("DROP TRIGGER fail_thread_authority_insert")
            .unwrap();
        drop(connection);
        assert_eq!(
            grant_thread(&runtime, &spawn_id, "stdin")
                .authority
                .thread_id,
            thread_id
        );
    }

    #[test]
    fn spawned_thread_never_exceeds_parent_policy_or_cwd_authority() {
        let (root, runtime) = thread_test_runtime();
        let outside_dir = root.path().join("outside");
        std::fs::create_dir(&outside_dir).unwrap();
        let (spawn_id, _) = pending_spawn(
            start_thread(
                &runtime,
                None,
                &runtime.paths.workspace_root,
                crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
            )
            .unwrap(),
        );
        let parent = grant_thread(&runtime, &spawn_id, "stdin");
        let child_dir = thread_worktree(&runtime, &parent.authority.thread_id).join("child");
        std::fs::create_dir(&child_dir).unwrap();

        assert!(matches!(
            start_thread(
                &runtime,
                Some(parent.authority.thread_id.clone()),
                &child_dir,
                crate::daemon::protocol::ThreadApprovalPolicy::Yolo,
            ),
            Err(ThreadSpawnFailure::Authority(
                ThreadAuthorityError::ApprovalPolicy { .. }
            ))
        ));
        assert!(matches!(
            thread_spawn(
                &runtime,
                ThreadSpawnParams::Start {
                    parent_thread_id: Some(parent.authority.thread_id),
                    cwd: outside_dir.to_string_lossy().into_owned(),
                    model: "gpt-5.6-sol".into(),
                    reasoning_effort: crate::daemon::protocol::ReasoningEffort::Xhigh,
                    approval_policy: crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
                    repositories: vec![ThreadRepositoryRequest {
                        repo: ".".into(),
                        branch: None,
                    }],
                },
            ),
            Err(ThreadSpawnFailure::Authority(
                ThreadAuthorityError::WorkingDirectory { .. }
            ))
        ));
        let store = runtime.paths.server_store().unwrap();
        assert_eq!(store.thread_authorities().unwrap().len(), 1);
    }

    #[test]
    fn spawned_thread_rejects_a_toolset_superset_at_the_typed_gate() {
        let (_root, runtime) = thread_test_runtime();
        std::fs::write(
            runtime.paths.workspace_root.join("plato.toml"),
            "[tools]\nenabled = [\"file.read\"]\n",
        )
        .unwrap();
        let (spawn_id, _) = pending_spawn(
            start_thread(
                &runtime,
                None,
                &runtime.paths.workspace_root,
                crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
            )
            .unwrap(),
        );
        let parent = grant_thread(&runtime, &spawn_id, "stdin");
        let child_dir = thread_worktree(&runtime, &parent.authority.thread_id).join("child");
        std::fs::create_dir(&child_dir).unwrap();
        std::fs::write(
            child_dir.join("plato.toml"),
            "[tools]\nenabled = [\"file.read\", \"file.write\"]\n",
        )
        .unwrap();

        assert!(matches!(
            start_thread(
                &runtime,
                Some(parent.authority.thread_id),
                &child_dir,
                crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
            ),
            Err(ThreadSpawnFailure::Authority(
                ThreadAuthorityError::Toolset { .. }
            ))
        ));
        assert_eq!(
            runtime
                .paths
                .server_store()
                .unwrap()
                .thread_authorities()
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn yolo_parent_auto_grants_child_with_exact_actor() {
        let (_root, runtime) = thread_test_runtime();
        let (spawn_id, _) = pending_spawn(
            start_thread(
                &runtime,
                None,
                &runtime.paths.workspace_root,
                crate::daemon::protocol::ThreadApprovalPolicy::Yolo,
            )
            .unwrap(),
        );
        let parent = grant_thread(&runtime, &spawn_id, "stdin");
        let child_dir = thread_worktree(&runtime, &parent.authority.thread_id).join("child");
        std::fs::create_dir(&child_dir).unwrap();
        let child = match start_thread(
            &runtime,
            Some(parent.authority.thread_id),
            &child_dir,
            crate::daemon::protocol::ThreadApprovalPolicy::Yolo,
        )
        .unwrap()
        {
            ThreadSpawnResult::Spawned { thread } => thread,
            unexpected => panic!("expected auto-granted child, got {unexpected:?}"),
        };
        assert_eq!(child.authority.spawning_actor, "yolo");
        let connection = rusqlite::Connection::open(&runtime.paths.server_db_path).unwrap();
        let approvals = connection
            .query_row(
                "SELECT COUNT(*) FROM thread_spawn_approvals WHERE actor = 'yolo' AND decision = 'granted'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap();
        assert_eq!(approvals, 1);
    }

    #[test]
    fn thread_list_and_status_keep_clientless_orphans_after_restart() {
        let (_root, runtime) = thread_test_runtime();
        let (spawn_id, _) = pending_spawn(
            start_thread(
                &runtime,
                None,
                &runtime.paths.workspace_root,
                crate::daemon::protocol::ThreadApprovalPolicy::Yolo,
            )
            .unwrap(),
        );
        let parent = grant_thread(&runtime, &spawn_id, "stdin");
        let child_dir = thread_worktree(&runtime, &parent.authority.thread_id).join("child");
        std::fs::create_dir(&child_dir).unwrap();
        let child = match start_thread(
            &runtime,
            Some(parent.authority.thread_id.clone()),
            &child_dir,
            crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
        )
        .unwrap()
        {
            ThreadSpawnResult::Spawned { thread } => thread,
            unexpected => panic!("expected auto-granted child, got {unexpected:?}"),
        };

        let restarted = DaemonRuntime::new(runtime.paths.clone());
        assert!(matches!(
            start_thread(
                &restarted,
                Some(parent.authority.thread_id.clone()),
                &child_dir,
                crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
            ),
            Err(ThreadSpawnFailure::NotFound(message)) if message.contains("not loaded")
        ));
        let list = handle_line(
            &restarted,
            r#"{"v":1,"id":"list","kind":"request","method":"thread.list"}"#,
        );
        let listed: ThreadListResult = response_result(&list);
        assert_eq!(listed.threads.len(), 2);
        assert!(listed.threads.iter().all(|thread| !thread.live.loaded));
        assert!(listed.threads.iter().any(|thread| {
            thread.authority.thread_id == child.authority.thread_id
                && thread.authority.parent_thread_id.as_deref()
                    == Some(parent.authority.thread_id.as_str())
        }));

        let status = handle_line(
            &restarted,
            &format!(
                r#"{{"v":1,"id":"status","kind":"request","method":"thread.status","params":{{"thread_id":"{}"}}}}"#,
                child.authority.thread_id
            ),
        );
        let status: ThreadStatusResult = response_result(&status);
        assert_eq!(status.thread.authority, child.authority);
        assert!(!status.thread.live.loaded);
    }

    #[test]
    fn duplicate_thread_decision_is_idempotent_and_conflicts_fail_closed() {
        let (_root, runtime) = thread_test_runtime();
        let (spawn_id, thread_id) = pending_spawn(
            start_thread(
                &runtime,
                None,
                &runtime.paths.workspace_root,
                crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
            )
            .unwrap(),
        );
        let first = grant_thread(&runtime, &spawn_id, "stdin");
        let duplicate = grant_thread(&runtime, &spawn_id, "stdin");
        assert_eq!(duplicate, first);
        assert!(matches!(
            decide_thread(
                &runtime,
                &spawn_id,
                ThreadSpawnDecision::Deny {
                    actor: "stdin".into(),
                    reason: "changed".into(),
                }
            ),
            Err(ThreadSpawnFailure::Conflict(message)) if message.contains("different durable decision")
        ));
        let store = runtime.paths.server_store().unwrap();
        assert_eq!(store.thread_authorities().unwrap().len(), 1);
        assert_eq!(
            legacy_status_authority(
                &store
                    .thread_authority(&thread_id)
                    .unwrap()
                    .expect("granted authority is durable")
            )
            .unwrap(),
            first.authority
        );
    }

    #[test]
    fn malformed_thread_requests_fail_before_reservation() {
        let (_root, runtime) = thread_test_runtime();
        assert!(matches!(
            start_thread(
                &runtime,
                None,
                Path::new("relative"),
                crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
            ),
            Err(ThreadSpawnFailure::Malformed(message)) if message.contains("absolute")
        ));
        let response = handle_line(
            &runtime,
            r#"{"v":1,"id":"bad","kind":"request","method":"thread.spawn","params":{"action":"start","parent_thread_id":null,"cwd":"/tmp","model":"gpt-5.6-sol","reasoning_effort":"xhigh","approval_policy":"prompt","extra":true}}"#,
        );
        assert_eq!(response.error.unwrap().code, ERROR_MALFORMED_REQUEST);
        let store = runtime.paths.server_store().unwrap();
        assert!(store.thread_authorities().unwrap().is_empty());
    }

    #[test]
    fn thread_authority_params_reject_unknown_fields_before_readback() {
        let (_root, runtime) = thread_test_runtime();
        let response = handle_line(
            &runtime,
            r#"{"v":1,"id":"bad","kind":"request","method":"thread.authority","params":{"thread_id":"thread_1","future":true}}"#,
        );
        assert_eq!(response.error.unwrap().code, ERROR_MALFORMED_REQUEST);
    }

    #[test]
    fn denied_and_stale_thread_sends_leave_authority_ledger_and_turn_unchanged() {
        let (_root, runtime) = thread_test_runtime();
        let (spawn_id, thread_id) = pending_spawn(
            start_thread(
                &runtime,
                None,
                &runtime.paths.workspace_root,
                crate::daemon::protocol::ThreadApprovalPolicy::Prompt,
            )
            .unwrap(),
        );
        let authority = grant_thread(&runtime, &spawn_id, "stdin").authority;
        let malformed = handle_line(
            &runtime,
            &format!(
                r#"{{"v":1,"id":"bad","kind":"request","method":"thread.send","params":{{"thread_id":"{thread_id}","controller_id":"","message":"no"}}}}"#
            ),
        );
        assert_eq!(malformed.error.unwrap().code, ERROR_MALFORMED_REQUEST);

        let stale = handle_line(
            &runtime,
            &format!(
                r#"{{"v":1,"id":"stale","kind":"request","method":"thread.send","params":{{"thread_id":"{thread_id}","controller_id":"controller_a","turn_id":"thread_turn_stale","message":"no"}}}}"#
            ),
        );
        assert_eq!(
            response_result::<crate::daemon::protocol::ThreadSendResult>(&stale),
            crate::daemon::protocol::ThreadSendResult::Rejected {
                thread_id: thread_id.clone(),
                turn_id: None,
                reason: crate::daemon::protocol::ThreadSendRejectedReason::TurnMismatch,
            }
        );

        let invalid_events = handle_line(
            &runtime,
            &format!(
                r#"{{"v":1,"id":"events","kind":"request","method":"thread.events","params":{{"thread_id":"{thread_id}","limit":0}}}}"#
            ),
        );
        assert_eq!(invalid_events.error.unwrap().code, ERROR_MALFORMED_REQUEST);
        assert_eq!(runtime.thread_live_state(&thread_id).current_turn_id, None);
        let store = runtime.paths.server_store().unwrap();
        assert_eq!(
            legacy_status_authority(
                &store
                    .thread_authority(&thread_id)
                    .unwrap()
                    .expect("granted authority is durable")
            )
            .unwrap(),
            authority
        );
        // The workspace ledger holds session_runs; the server store holds
        // thread state. This assertion is about the former.
        let connection = rusqlite::Connection::open(
            SqliteLedger::open_or_create_default(&runtime.paths.default_ledger())
                .map(|_| &runtime.paths.ledger_path)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM session_runs", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0
        );
    }
}
