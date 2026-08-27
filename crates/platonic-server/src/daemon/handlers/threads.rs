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
            ERROR_DAEMON_SHUTTING_DOWN, ERROR_MALFORMED_REQUEST, ERROR_NOT_FOUND, ERROR_OVERLOAD,
            ERROR_PROFILE_OPEN_CONFLICT, ERROR_PROFILE_OPEN_FAILED,
            ERROR_THREAD_AUTHORITY_EXCEEDED, ERROR_THREAD_AUTHORITY_FAILED,
            ERROR_THREAD_BRANCH_CLAIM_CONFLICT, ERROR_THREAD_CONFINEMENT_UNAVAILABLE,
            ERROR_THREAD_EVENTS_FAILED, ERROR_THREAD_LIST_FAILED, ERROR_THREAD_SEND_FAILED,
            ERROR_THREAD_SPAWN_FAILED, ERROR_THREAD_STATUS_FAILED, ERROR_THREAD_STOP_FAILED,
            ERROR_WORKSPACE_BROKEN, ERROR_WORKSPACE_MISMATCH, ERROR_WORKSPACE_UNREGISTERED,
            Envelope, ProfileOpenDecision, ProfileOpenParams, ProfileOpenResult, ProtocolResponse,
            ThreadAuthorityParams, ThreadAuthorityResult, ThreadConfinement, ThreadEventsParams,
            ThreadKind, ThreadListResult, ThreadRepositoryRequest, ThreadSendParams,
            ThreadSpawnDecision, ThreadSpawnParams, ThreadSpawnResult, ThreadStatus,
            ThreadStatusParams, ThreadStatusResult, ThreadStopParams, ThreadStopResult,
        },
        runtime::{
            DaemonRuntime, ThreadEventsError, ThreadSendAdmission, ThreadSpawnAdmissionError,
            ThreadSpawnClaimError, ThreadStopError,
        },
    },
    model::RunOverrides,
    server_store::{
        HomeReservationRecord, HomeReservationState, ProfileHomeProposal, ReserveProfileHomeResult,
        ServerStore,
    },
    thread_authority::{
        THREAD_SPAWN_APPROVAL_REASON, ThreadAuthorityDraft, ThreadAuthorityDraftParams,
        ThreadAuthorityError, ThreadSpawnApprovalRecord, ThreadSpawnDecisionName, ThreadStopRecord,
        authority_working_directory, legacy_status_authority, new_home_reservation_id,
        new_spawn_id, new_thread_turn_id, now_ms, thread_spawn_effect, validate_child_authority,
    },
    tool_catalog::{FILE_EDIT, FILE_WRITE, SHELL_EXEC, THREAD_SPAWN, effect_for_tool},
    tools::{ThreadSpawnToolHandler, ThreadSpawnToolInput, ThreadSpawnToolOutput},
};
use platonic_core::{ActorId, EffectClass, ProfileId, RunIdentity, TurnId};
use std::{
    collections::HashSet,
    path::{Component, Path, PathBuf},
    time::Duration,
};

const MAX_THREAD_EVENT_WAIT_MS: u64 = 1_000;
const THREAD_STOP_WAIT: Duration = Duration::from_secs(10);
const LOCAL_OPERATOR_ACTOR: &str = "local-operator";
const PROFILE_OPEN_APPROVAL_REASON: &str =
    "profile.open requires approval before home authority is created";

#[derive(Debug)]
enum ProfileOpenFailure {
    ShuttingDown,
    Malformed(String),
    NotFound(String),
    WorkspaceBroken(String),
    WorkspaceMismatch(String),
    Conflict(String),
    Overload(String),
    ConfinementUnavailable,
    Persistence,
}

pub(super) fn handle_profile_open(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: ProfileOpenParams,
) -> Envelope {
    match profile_open(runtime, params) {
        Ok(result) => Envelope::typed_response(request.id, ProtocolResponse::ProfileOpen(result)),
        Err(ProfileOpenFailure::ShuttingDown) => shutting_down_response(request.id, "profile.open"),
        Err(ProfileOpenFailure::Malformed(message)) => Envelope::error(
            request.id,
            Some("profile.open".into()),
            ERROR_MALFORMED_REQUEST,
            message,
        ),
        Err(ProfileOpenFailure::NotFound(message)) => Envelope::error(
            request.id,
            Some("profile.open".into()),
            ERROR_NOT_FOUND,
            message,
        ),
        Err(ProfileOpenFailure::WorkspaceBroken(message)) => Envelope::error(
            request.id,
            Some("profile.open".into()),
            ERROR_WORKSPACE_BROKEN,
            message,
        ),
        Err(ProfileOpenFailure::WorkspaceMismatch(message)) => Envelope::error(
            request.id,
            Some("profile.open".into()),
            ERROR_WORKSPACE_MISMATCH,
            message,
        ),
        Err(ProfileOpenFailure::Conflict(message)) => Envelope::error(
            request.id,
            Some("profile.open".into()),
            ERROR_PROFILE_OPEN_CONFLICT,
            message,
        ),
        Err(ProfileOpenFailure::Overload(message)) => Envelope::error(
            request.id,
            Some("profile.open".into()),
            ERROR_OVERLOAD,
            message,
        ),
        Err(ProfileOpenFailure::ConfinementUnavailable) => Envelope::error(
            request.id,
            Some("profile.open".into()),
            ERROR_THREAD_CONFINEMENT_UNAVAILABLE,
            "server policy requires confinement, but this home cannot be confined",
        ),
        Err(ProfileOpenFailure::Persistence) => Envelope::error(
            request.id,
            Some("profile.open".into()),
            ERROR_PROFILE_OPEN_FAILED,
            "profile home could not be resolved or persisted",
        ),
    }
}

fn profile_open(
    runtime: &DaemonRuntime,
    params: ProfileOpenParams,
) -> Result<ProfileOpenResult, ProfileOpenFailure> {
    if runtime.shutdown_accepted() {
        return Err(ProfileOpenFailure::ShuttingDown);
    }
    match params {
        ProfileOpenParams::Resolve { profile_id } => resolve_profile_home(runtime, &profile_id),
        ProfileOpenParams::Start {
            profile_id,
            idempotency_key,
            repositories,
            working_repository,
            working_subdir,
        } => start_profile_home(
            runtime,
            profile_id,
            idempotency_key,
            repositories,
            working_repository,
            working_subdir,
        ),
        ProfileOpenParams::Decide {
            home_reservation_id,
            decision,
        } => decide_profile_home(runtime, &home_reservation_id, decision),
    }
}

fn resolve_profile_home(
    runtime: &DaemonRuntime,
    profile_id: &ProfileId,
) -> Result<ProfileOpenResult, ProfileOpenFailure> {
    let store = runtime
        .paths
        .server_store()
        .map_err(|_| ProfileOpenFailure::Persistence)?;
    let profile = checked_profile(runtime, &store, profile_id)?;
    let Some(home_thread_id) = profile.home_thread_id else {
        return Ok(ProfileOpenResult::NoHome {
            profile_id: profile_id.clone(),
        });
    };
    opened_profile_home(runtime, &store, profile_id, &home_thread_id, false)
}

fn start_profile_home(
    runtime: &DaemonRuntime,
    profile_id: ProfileId,
    idempotency_key: String,
    repositories: Vec<ThreadRepositoryRequest>,
    working_repository: String,
    working_subdir: String,
) -> Result<ProfileOpenResult, ProfileOpenFailure> {
    validate_home_proposal(
        &idempotency_key,
        &repositories,
        &working_repository,
        &working_subdir,
    )?;
    let mut store = runtime
        .paths
        .server_store()
        .map_err(|_| ProfileOpenFailure::Persistence)?;
    let profile = checked_profile(runtime, &store, &profile_id)?;
    let proposal = ProfileHomeProposal {
        repositories,
        working_repository,
        working_subdir,
    };
    if let Some(existing) = store
        .profile_home_reservation(&profile_id, &idempotency_key)
        .map_err(|_| ProfileOpenFailure::Persistence)?
    {
        if existing.proposal != proposal {
            return Err(ProfileOpenFailure::Conflict(format!(
                "idempotency key {idempotency_key} names a different home proposal"
            )));
        }
        return profile_home_reservation_result(runtime, &store, existing);
    }
    if profile.home_thread_id.is_some() {
        return Err(ProfileOpenFailure::Conflict(format!(
            "profile {profile_id} already has a home"
        )));
    }
    let source_working_directory = runtime
        .paths
        .workspace_root
        .join(&proposal.working_repository)
        .join(&proposal.working_subdir);
    let toolset = profile.toolset.clone();
    let mut draft = ThreadAuthorityDraft::new(ThreadAuthorityDraftParams {
        parent_thread_id: None,
        cwd: &source_working_directory,
        model: profile.model,
        reasoning_effort: profile.reasoning_effort,
        approval_policy: profile.approval_policy,
        agent_id: None,
        profile_id: profile_id.clone(),
        profile_revision: profile.current_revision,
        thread_kind: ThreadKind::Home,
        writable: toolset_requires_writable_path(&toolset),
        network: toolset_has_effect(&toolset, EffectClass::Network),
        toolset,
    })
    .map_err(|error| ProfileOpenFailure::Malformed(error.to_string()))?;
    draft.repositories = crate::thread_repository::resolve(
        &runtime.paths.workspace_root,
        &draft.thread_id,
        &source_working_directory,
        None,
        &proposal.repositories,
    )
    .map_err(|error| ProfileOpenFailure::Malformed(error.to_string()))?;
    let created_at_ms = now_ms();
    let reservation = HomeReservationRecord {
        id: new_home_reservation_id(),
        workspace_id: runtime.paths.workspace_id.clone(),
        profile_id,
        idempotency_key,
        proposal,
        draft,
        state: HomeReservationState::Pending,
        decided_by: None,
        reason: None,
        created_at_ms,
        decided_at_ms: None,
    };
    let claims = reservation
        .draft
        .repositories
        .iter()
        .map(|repository| (repository.repo.clone(), repository.branch.clone()))
        .collect::<Vec<_>>();
    match store
        .reserve_profile_home(&reservation, &claims)
        .map_err(|_| ProfileOpenFailure::Persistence)?
    {
        ReserveProfileHomeResult::Reserved(reservation)
        | ReserveProfileHomeResult::Replayed(reservation) => {
            profile_home_reservation_result(runtime, &store, reservation)
        }
        ReserveProfileHomeResult::Conflict(message) => Err(ProfileOpenFailure::Conflict(message)),
    }
}

fn validate_home_proposal(
    idempotency_key: &str,
    repositories: &[ThreadRepositoryRequest],
    working_repository: &str,
    working_subdir: &str,
) -> Result<(), ProfileOpenFailure> {
    if idempotency_key.is_empty() || idempotency_key.len() > 128 {
        return Err(ProfileOpenFailure::Malformed(
            "profile.open idempotency_key must contain 1..128 UTF-8 bytes".into(),
        ));
    }
    if repositories.is_empty() || repositories.len() > 16 {
        return Err(ProfileOpenFailure::Malformed(
            "profile.open repositories must contain 1..16 entries".into(),
        ));
    }
    let mut seen = HashSet::new();
    for repository in repositories {
        if !seen.insert(repository.repo.as_str()) {
            return Err(ProfileOpenFailure::Malformed(format!(
                "profile.open names repository {} more than once",
                repository.repo
            )));
        }
    }
    if !seen.contains(working_repository) {
        return Err(ProfileOpenFailure::Malformed(
            "profile.open working_repository must name one requested repository".into(),
        ));
    }
    let subdir = Path::new(working_subdir);
    if working_subdir.is_empty()
        || subdir.is_absolute()
        || subdir.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(ProfileOpenFailure::Malformed(
            "profile.open working_subdir must stay beneath its repository".into(),
        ));
    }
    Ok(())
}

fn checked_profile(
    runtime: &DaemonRuntime,
    store: &ServerStore,
    profile_id: &ProfileId,
) -> Result<crate::server_store::ProfileRecord, ProfileOpenFailure> {
    let profile = store
        .profile(profile_id)
        .map_err(|_| ProfileOpenFailure::Persistence)?
        .ok_or_else(|| ProfileOpenFailure::NotFound(format!("profile not found: {profile_id}")))?;
    if profile.workspace_id != runtime.paths.workspace_id {
        return Err(ProfileOpenFailure::WorkspaceMismatch(format!(
            "profile {profile_id} belongs to another workspace"
        )));
    }
    let workspace = store
        .workspace(&profile.workspace_id)
        .map_err(|_| ProfileOpenFailure::Persistence)?
        .ok_or_else(|| {
            ProfileOpenFailure::NotFound(format!(
                "profile workspace not found: {}",
                profile.workspace_id
            ))
        })?;
    if workspace.health() != crate::server_store::WorkspaceHealth::Present {
        return Err(ProfileOpenFailure::WorkspaceBroken(format!(
            "workspace directory is missing: {}",
            profile.workspace_id
        )));
    }
    Ok(profile)
}

fn profile_home_reservation_result(
    runtime: &DaemonRuntime,
    store: &ServerStore,
    reservation: HomeReservationRecord,
) -> Result<ProfileOpenResult, ProfileOpenFailure> {
    match reservation.state {
        HomeReservationState::Pending => Ok(ProfileOpenResult::ApprovalRequired {
            profile_id: reservation.profile_id,
            home_reservation_id: reservation.id,
            thread_id: reservation.draft.thread_id,
            effect: thread_spawn_effect(),
            reason: PROFILE_OPEN_APPROVAL_REASON.into(),
        }),
        HomeReservationState::Granted => opened_profile_home(
            runtime,
            store,
            &reservation.profile_id,
            &reservation.draft.thread_id,
            false,
        ),
        HomeReservationState::Denied => Ok(ProfileOpenResult::Denied {
            profile_id: reservation.profile_id,
            home_reservation_id: reservation.id,
            thread_id: reservation.draft.thread_id,
            reason: reservation.reason.ok_or(ProfileOpenFailure::Persistence)?,
        }),
        HomeReservationState::Canceled => Ok(ProfileOpenResult::Canceled {
            profile_id: reservation.profile_id,
            home_reservation_id: reservation.id,
            thread_id: reservation.draft.thread_id,
        }),
    }
}

fn decide_profile_home(
    runtime: &DaemonRuntime,
    reservation_id: &str,
    decision: ProfileOpenDecision,
) -> Result<ProfileOpenResult, ProfileOpenFailure> {
    ActorId::new(reservation_id.to_owned())
        .map_err(|error| ProfileOpenFailure::Malformed(error.to_string()))?;
    if !runtime.claim_home_reservation_decision(reservation_id) {
        return Err(ProfileOpenFailure::Overload(format!(
            "profile home decision is already in progress: {reservation_id}"
        )));
    }
    let result = decide_profile_home_inner(runtime, reservation_id, decision);
    runtime.release_home_reservation_decision(reservation_id);
    result
}

fn decide_profile_home_inner(
    runtime: &DaemonRuntime,
    reservation_id: &str,
    decision: ProfileOpenDecision,
) -> Result<ProfileOpenResult, ProfileOpenFailure> {
    let mut store = runtime
        .paths
        .server_store()
        .map_err(|_| ProfileOpenFailure::Persistence)?;
    let reservation = store
        .home_reservation(reservation_id)
        .map_err(|_| ProfileOpenFailure::Persistence)?
        .ok_or_else(|| {
            ProfileOpenFailure::NotFound(format!("home reservation not found: {reservation_id}"))
        })?;
    if reservation.workspace_id != runtime.paths.workspace_id {
        return Err(ProfileOpenFailure::NotFound(format!(
            "home reservation not found: {reservation_id}"
        )));
    }
    if reservation.state != HomeReservationState::Pending {
        let matches = matches!(
            (&decision, reservation.state),
            (ProfileOpenDecision::Grant, HomeReservationState::Granted)
                | (ProfileOpenDecision::Cancel, HomeReservationState::Canceled)
        ) || matches!(
            (&decision, reservation.state),
            (ProfileOpenDecision::Deny { reason }, HomeReservationState::Denied)
                if reservation.reason.as_deref() == Some(reason)
        );
        if !matches {
            return Err(ProfileOpenFailure::Conflict(format!(
                "home reservation {reservation_id} already has a different durable decision"
            )));
        }
        return profile_home_reservation_result(runtime, &store, reservation);
    }
    match decision {
        ProfileOpenDecision::Deny { reason } => {
            if reason.trim().is_empty() {
                return Err(ProfileOpenFailure::Malformed(
                    "profile.open denial reason cannot be empty".into(),
                ));
            }
            let reservation = store
                .decide_profile_home_without_authority(
                    reservation_id,
                    HomeReservationState::Denied,
                    LOCAL_OPERATOR_ACTOR,
                    Some(&reason),
                    now_ms(),
                )
                .map_err(|_| ProfileOpenFailure::Persistence)?;
            profile_home_reservation_result(runtime, &store, reservation)
        }
        ProfileOpenDecision::Cancel => {
            let reservation = store
                .decide_profile_home_without_authority(
                    reservation_id,
                    HomeReservationState::Canceled,
                    LOCAL_OPERATOR_ACTOR,
                    None,
                    now_ms(),
                )
                .map_err(|_| ProfileOpenFailure::Persistence)?;
            profile_home_reservation_result(runtime, &store, reservation)
        }
        ProfileOpenDecision::Grant => grant_profile_home(runtime, &mut store, reservation),
    }
}

fn grant_profile_home(
    runtime: &DaemonRuntime,
    store: &mut ServerStore,
    reservation: HomeReservationRecord,
) -> Result<ProfileOpenResult, ProfileOpenFailure> {
    checked_profile(runtime, store, &reservation.profile_id)?;
    let confinement = match runtime.thread_confinement() {
        Ok(confinement) => confinement,
        Err(()) => {
            store
                .decide_profile_home_without_authority(
                    &reservation.id,
                    HomeReservationState::Canceled,
                    LOCAL_OPERATOR_ACTOR,
                    None,
                    now_ms(),
                )
                .map_err(|_| ProfileOpenFailure::Persistence)?;
            return Err(ProfileOpenFailure::ConfinementUnavailable);
        }
    };
    let mut draft = reservation.draft.clone();
    let prepared = match crate::thread_repository::prepare(
        &runtime.paths.server_db_path,
        &runtime.paths.workspace_id,
        &draft.thread_id,
        &draft.repositories,
    ) {
        Ok(prepared) => prepared,
        Err(error) => {
            let _ = store.decide_profile_home_without_authority(
                &reservation.id,
                HomeReservationState::Canceled,
                LOCAL_OPERATOR_ACTOR,
                None,
                now_ms(),
            );
            return Err(ProfileOpenFailure::Malformed(error.to_string()));
        }
    };
    draft.worktrees = prepared.worktrees;
    draft.granted_paths = prepared.granted_paths;
    let working = draft
        .worktrees
        .iter()
        .find(|worktree| worktree.repo == reservation.proposal.working_repository)
        .map(|worktree| Path::new(&worktree.path).join(&reservation.proposal.working_subdir))
        .ok_or(ProfileOpenFailure::Persistence)
        .and_then(|path| {
            let canonical = path
                .canonicalize()
                .map_err(|error| ProfileOpenFailure::Malformed(error.to_string()))?;
            let worktree = draft
                .worktrees
                .iter()
                .find(|worktree| worktree.repo == reservation.proposal.working_repository)
                .expect("working repository was selected above");
            if !canonical.starts_with(&worktree.path) || !canonical.is_dir() {
                return Err(ProfileOpenFailure::Malformed(
                    "profile.open working_subdir is not a directory beneath its repository".into(),
                ));
            }
            Ok(canonical)
        });
    let working = match working {
        Ok(working) => working,
        Err(error) => {
            let _ = crate::thread_repository::discard(
                &runtime.paths.server_db_path,
                &runtime.paths.workspace_id,
                &draft.thread_id,
                &draft.repositories,
            );
            let _ = store.decide_profile_home_without_authority(
                &reservation.id,
                HomeReservationState::Canceled,
                LOCAL_OPERATOR_ACTOR,
                None,
                now_ms(),
            );
            return Err(error);
        }
    };
    draft.cwd = working.to_string_lossy().into_owned();
    let decided_at_ms = now_ms();
    let authority = draft
        .complete(LOCAL_OPERATOR_ACTOR.into(), decided_at_ms)
        .map_err(|error| ProfileOpenFailure::Malformed(error.to_string()))?;
    let (durable, created) = match store.persist_profile_home(
        &reservation.id,
        &authority,
        confinement,
        LOCAL_OPERATOR_ACTOR,
        decided_at_ms,
    ) {
        Ok(result) => result,
        Err(_) => {
            let _ = crate::thread_repository::discard(
                &runtime.paths.server_db_path,
                &runtime.paths.workspace_id,
                &draft.thread_id,
                &draft.repositories,
            );
            return Err(ProfileOpenFailure::Persistence);
        }
    };
    let authority = durable.record().clone();
    let thread =
        joined_thread_status(runtime, authority).map_err(|_| ProfileOpenFailure::Persistence)?;
    Ok(ProfileOpenResult::Opened {
        profile_id: reservation.profile_id,
        thread: Box::new(thread),
        created,
    })
}

fn opened_profile_home(
    runtime: &DaemonRuntime,
    store: &ServerStore,
    profile_id: &ProfileId,
    thread_id: &str,
    created: bool,
) -> Result<ProfileOpenResult, ProfileOpenFailure> {
    let authority = store
        .thread_authority(thread_id)
        .map_err(|_| ProfileOpenFailure::Persistence)?
        .ok_or(ProfileOpenFailure::Persistence)?;
    if authority.thread_kind != ThreadKind::Home
        || authority.parent_thread_id.is_some()
        || authority.profile_id.as_ref() != Some(profile_id)
    {
        return Err(ProfileOpenFailure::Persistence);
    }
    Ok(ProfileOpenResult::Opened {
        profile_id: profile_id.clone(),
        thread: Box::new(
            joined_thread_status(runtime, authority)
                .map_err(|_| ProfileOpenFailure::Persistence)?,
        ),
        created,
    })
}

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
    parent_thread_id: String,
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
    let parent = store
        .thread_authority(&parent_thread_id)
        .map_err(|_| ThreadSpawnFailure::Persistence)?
        .ok_or_else(|| {
            ThreadSpawnFailure::NotFound(format!(
                "parent thread authority not found: {parent_thread_id}"
            ))
        })?;
    let profile_id = parent
        .profile_id
        .clone()
        .filter(|_| {
            matches!(
                parent.thread_kind,
                crate::daemon::protocol::ThreadKind::Home
                    | crate::daemon::protocol::ThreadKind::Child
            )
        })
        .ok_or(ThreadSpawnFailure::Authority(
            ThreadAuthorityError::SameProfileParent,
        ))?;
    if parent.profile_revision.is_none() {
        return Err(ThreadSpawnFailure::Authority(
            ThreadAuthorityError::SameProfileParent,
        ));
    }
    let profile = store
        .profile(&profile_id)
        .map_err(|_| ThreadSpawnFailure::Persistence)?
        .filter(|profile| profile.workspace_id == runtime.paths.workspace_id)
        .ok_or(ThreadSpawnFailure::Authority(
            ThreadAuthorityError::SameProfileParent,
        ))?;
    let config = Config::load(cwd, None)
        .map_err(|error| ThreadSpawnFailure::Malformed(error.to_string()))?;
    let toolset = config.tools.enabled;
    let draft = ThreadAuthorityDraft::new(ThreadAuthorityDraftParams {
        parent_thread_id: Some(parent_thread_id),
        cwd,
        model,
        reasoning_effort,
        approval_policy,
        agent_id: None,
        profile_id,
        profile_revision: profile.current_revision,
        thread_kind: crate::daemon::protocol::ThreadKind::Child,
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
        Some(&parent),
        &repository_requests,
    )
    .map_err(|error| ThreadSpawnFailure::Malformed(error.to_string()))?;
    validate_child_authority(&parent, &draft).map_err(ThreadSpawnFailure::Authority)?;
    let auto_grant = parent.approval_policy == crate::daemon::protocol::ThreadApprovalPolicy::Yolo;
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
    let mut store = runtime
        .paths
        .server_store()
        .map_err(|_| ThreadSpawnFailure::Persistence)?;
    let parent = store
        .thread_authority(parent_thread_id)
        .map_err(|_| ThreadSpawnFailure::Persistence)?
        .ok_or_else(|| {
            ThreadSpawnFailure::NotFound(format!(
                "parent thread authority not found: {parent_thread_id}"
            ))
        })?;
    let profile_id = parent
        .profile_id
        .clone()
        .ok_or(ThreadSpawnFailure::Authority(
            ThreadAuthorityError::SameProfileParent,
        ))?;
    if !matches!(
        parent.thread_kind,
        crate::daemon::protocol::ThreadKind::Home | crate::daemon::protocol::ThreadKind::Child
    ) {
        return Err(ThreadSpawnFailure::Authority(
            ThreadAuthorityError::SameProfileParent,
        ));
    }
    let profile = store
        .profile(&profile_id)
        .map_err(|_| ThreadSpawnFailure::Persistence)?
        .ok_or_else(|| ThreadSpawnFailure::NotFound(format!("profile not found: {profile_id}")))?;
    if profile.workspace_id != runtime.paths.workspace_id {
        return Err(ThreadSpawnFailure::WorkspaceMismatch(format!(
            "profile {profile_id} belongs to workspace {}, not {}",
            profile.workspace_id, runtime.paths.workspace_id
        )));
    }

    let toolset = input.toolset.unwrap_or_else(|| profile.toolset.clone());
    let excess = toolset
        .iter()
        .filter(|tool| !profile.toolset.contains(tool))
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
        model: input.model.unwrap_or(profile.model),
        reasoning_effort: input.reasoning_effort.unwrap_or(profile.reasoning_effort),
        approval_policy: input.approval_policy.unwrap_or(profile.approval_policy),
        agent_id: None,
        profile_id,
        profile_revision: profile.current_revision,
        thread_kind: crate::daemon::protocol::ThreadKind::Child,
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
                let prepared_cwd = (|| {
                    let source_cwd = Path::new(&draft.cwd);
                    let working_repository = draft
                        .repositories
                        .iter()
                        .find(|repository| source_cwd.starts_with(&repository.source_path))
                        .ok_or_else(|| {
                            ThreadSpawnFailure::Malformed(
                                "thread cwd is outside its requested repositories".into(),
                            )
                        })?;
                    let relative = source_cwd
                        .strip_prefix(&working_repository.source_path)
                        .expect("working repository containment was checked above");
                    prepared
                        .worktrees
                        .iter()
                        .find(|worktree| worktree.repo == working_repository.repo)
                        .map(|worktree| Path::new(&worktree.path).join(relative))
                        .ok_or(ThreadSpawnFailure::Persistence)?
                        .canonicalize()
                        .map_err(|error| ThreadSpawnFailure::Malformed(error.to_string()))
                })();
                let prepared_cwd = match prepared_cwd {
                    Ok(cwd) => cwd,
                    Err(error) => {
                        let _ = crate::thread_repository::discard(
                            &runtime.paths.server_db_path,
                            &runtime.paths.workspace_id,
                            &draft.thread_id,
                            &draft.repositories,
                        );
                        let _ = store.release_thread_claims(&draft.thread_id);
                        return Err(error);
                    }
                };
                draft.worktrees = prepared.worktrees;
                draft.granted_paths = prepared.granted_paths;
                draft.cwd = prepared_cwd.to_string_lossy().into_owned();
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
) -> Result<crate::daemon::protocol::ThreadAuthorityRecord, ThreadSpawnFailure> {
    let Some(parent_thread_id) = draft.parent_thread_id.as_deref() else {
        return Err(ThreadSpawnFailure::Authority(
            ThreadAuthorityError::ParentRequired,
        ));
    };
    validate_spawn_lineage(store, parent_thread_id, &draft.profile_id, max_spawn_depth)?;
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
    if parent.profile_id.as_ref() != Some(&draft.profile_id)
        || !matches!(
            parent.thread_kind,
            crate::daemon::protocol::ThreadKind::Home | crate::daemon::protocol::ThreadKind::Child
        )
    {
        return Err(ThreadSpawnFailure::Authority(
            ThreadAuthorityError::SameProfileParent,
        ));
    }
    validate_child_authority(&parent, draft).map_err(ThreadSpawnFailure::Authority)?;
    Ok(parent)
}

fn validate_spawn_lineage(
    store: &ServerStore,
    parent_thread_id: &str,
    profile_id: &platonic_core::ProfileId,
    maximum: u32,
) -> Result<(), ThreadSpawnFailure> {
    let mut next = Some(parent_thread_id.to_owned());
    let mut depth = 0_u32;
    let mut seen = HashSet::new();
    while let Some(thread_id) = next {
        if !seen.insert(thread_id.clone()) {
            return Err(ThreadSpawnFailure::Authority(
                ThreadAuthorityError::InvalidLineage,
            ));
        }
        depth = depth.saturating_add(1);
        if depth > maximum {
            return Err(ThreadSpawnFailure::Authority(
                ThreadAuthorityError::SpawnDepth { maximum },
            ));
        }
        let authority = store
            .thread_authority(&thread_id)
            .map_err(|_| ThreadSpawnFailure::Persistence)?
            .ok_or_else(|| {
                ThreadSpawnFailure::NotFound(format!(
                    "parent thread authority not found: {thread_id}"
                ))
            })?;
        if authority.profile_id.as_ref() != Some(profile_id)
            || !matches!(
                authority.thread_kind,
                crate::daemon::protocol::ThreadKind::Home
                    | crate::daemon::protocol::ThreadKind::Child
            )
        {
            return Err(ThreadSpawnFailure::Authority(
                ThreadAuthorityError::SameProfileParent,
            ));
        }
        if store
            .thread_stop(&thread_id)
            .map_err(|_| ThreadSpawnFailure::Persistence)?
            .is_some()
        {
            return Err(ThreadSpawnFailure::Authority(
                ThreadAuthorityError::StoppedParent,
            ));
        }
        next = authority.parent_thread_id;
        if next.is_none() {
            let profile = store
                .profile(profile_id)
                .map_err(|_| ThreadSpawnFailure::Persistence)?
                .ok_or_else(|| {
                    ThreadSpawnFailure::NotFound(format!("profile not found: {profile_id}"))
                })?;
            if authority.thread_kind != crate::daemon::protocol::ThreadKind::Home
                || profile.home_thread_id.as_deref() != Some(&thread_id)
            {
                return Err(ThreadSpawnFailure::Authority(
                    ThreadAuthorityError::InvalidLineage,
                ));
            }
        }
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
    let identity = match current_profile_identity(runtime, &authority) {
        Ok(identity) => identity,
        Err(message) => {
            return Envelope::error(
                request.id,
                Some("thread.send".into()),
                ERROR_THREAD_SEND_FAILED,
                message,
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
        identity,
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

fn current_profile_identity(
    runtime: &DaemonRuntime,
    authority: &crate::daemon::protocol::ThreadAuthorityRecord,
) -> Result<RunIdentity, &'static str> {
    let profile_id = match (
        authority.thread_kind,
        authority.profile_id.clone(),
        authority.profile_revision,
    ) {
        (ThreadKind::Home | ThreadKind::Child, Some(profile_id), Some(_)) => profile_id,
        _ => return Err("legacy threads are replay-only and cannot start new turns"),
    };
    let profile = runtime
        .paths
        .server_store()
        .and_then(|store| store.profile(&profile_id))
        .map_err(|_| "thread profile revision readback failed")?
        .filter(|profile| profile.workspace_id == runtime.paths.workspace_id)
        .ok_or("thread profile revision readback failed")?;
    Ok(RunIdentity::Profile {
        profile_id,
        profile_revision: profile.current_revision,
    })
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
            let mut readable_paths = authority
                .worktrees
                .iter()
                .map(|worktree| PathBuf::from(&worktree.path))
                .collect::<Vec<_>>();
            readable_paths.extend(
                authority
                    .granted_paths
                    .iter()
                    .map(|grant| PathBuf::from(&grant.path)),
            );
            for worktree in &authority.worktrees {
                readable_paths.push(crate::thread_repository::shared_repository_path(
                    &runtime.paths.server_db_path,
                    &runtime.paths.workspace_id,
                    &worktree.repo,
                )?);
            }
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
                readable_paths,
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
        live_epoch_id,
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
        live_epoch_id.as_deref(),
        from_offset,
        limit,
        std::time::Duration::from_millis(wait_ms),
    ) {
        Ok(result) => Envelope::typed_response(request.id, ProtocolResponse::ThreadEvents(result)),
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
    if authority.thread_kind == ThreadKind::Home {
        return Envelope::error(
            request.id,
            Some("thread.stop".into()),
            ERROR_THREAD_STOP_FAILED,
            "profile home threads cannot be stopped; cancel an active run instead",
        );
    }
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
    let terminal = crate::daemon::returns::persist_stopped_child(
        runtime,
        &authority.thread_id,
        stop.stopped_turn_id.clone(),
        stop.occurred_at_ms,
    );
    runtime.complete_thread_stop(&authority.thread_id);
    if terminal.is_err() {
        return Envelope::error(
            request.id,
            Some("thread.stop".into()),
            ERROR_THREAD_STOP_FAILED,
            "thread stopped, but its terminal return could not be persisted",
        );
    }
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

pub(in crate::daemon) fn thread_session_id(thread_id: &str) -> String {
    format!("session_{thread_id}")
}

fn joined_thread_status(
    runtime: &DaemonRuntime,
    authority: crate::daemon::protocol::ThreadAuthorityRecord,
) -> AppResult<ThreadStatus> {
    let live = runtime.thread_live_state(&authority.thread_id);
    let store = runtime.paths.server_store()?;
    let mut projected = legacy_status_authority(&authority)?;
    projected.home_thread_id = match (authority.thread_kind, authority.profile_id.as_ref()) {
        (ThreadKind::Home | ThreadKind::Child, Some(profile_id)) => store
            .profile(profile_id)?
            .and_then(|profile| profile.home_thread_id),
        (ThreadKind::Home | ThreadKind::Child | ThreadKind::Legacy, _) => None,
    };
    let (child_returns, parent_answers) = store.thread_return_availability(&authority.thread_id)?;
    Ok(ThreadStatus {
        authority: projected,
        live,
        return_availability: crate::daemon::protocol::ThreadReturnAvailability {
            child_returns,
            parent_answers,
        },
    })
}

#[cfg(test)]
pub(in crate::daemon::handlers) mod tests {
    use super::*;

    use platonic_core::EffectClass;
    #[cfg(target_os = "linux")]
    use platonic_core::RunId;
    #[cfg(target_os = "linux")]
    use serde_json::json;
    #[cfg(target_os = "linux")]
    use std::os::unix::fs::PermissionsExt;
    #[cfg(target_os = "linux")]
    use std::sync::Barrier;
    use std::{sync::Arc, time::Duration};

    #[cfg(target_os = "linux")]
    use crate::daemon::handlers::runs::{finish_run_after_event_collection, spawn_event_collector};
    use crate::daemon::handlers::{handle_line, runs::tests::response_result};
    #[cfg(target_os = "linux")]
    use crate::{ApprovalMode, RunLedger, RunOptions};
    use crate::{
        daemon::{
            protocol::{RunStateName, ThreadApprovalPolicy},
            runtime::{RunRecord, ThreadTurnBinding},
        },
        ledger::SqliteLedger,
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

    pub(in crate::daemon) fn thread_test_runtime() -> (tempfile::TempDir, DaemonRuntime) {
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
                &runtime.paths.workspace_id,
                "thread-tests",
                &runtime.paths.workspace_root.to_string_lossy(),
                &runtime.paths.ledger_path.to_string_lossy(),
                1,
            )
            .unwrap();
        (root, runtime)
    }

    #[test]
    fn thread_spawn_requires_a_parent_before_workspace_attachment() {
        let (_root, runtime) = bare_thread_test_runtime();
        let response = handle_line(
            &runtime,
            &format!(
                r#"{{"v":2,"id":"spawn","kind":"request","method":"thread.spawn","params":{{"action":"start","parent_thread_id":null,"cwd":"{}","model":"gpt-5.6-sol","reasoning_effort":"none","approval_policy":"prompt"}}}}"#,
                runtime.paths.workspace_root.display()
            ),
        );
        let error = response.error.unwrap();
        assert_eq!(error.code, ERROR_MALFORMED_REQUEST);
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
        if parent_thread_id.is_none() {
            if !cwd.is_absolute() {
                return Err(ThreadSpawnFailure::Malformed(
                    "thread cwd must be an absolute path".into(),
                ));
            }
            return start_test_home(runtime, cwd, approval_policy, Vec::new());
        }
        thread_spawn(
            runtime,
            ThreadSpawnParams::Start {
                parent_thread_id: parent_thread_id.expect("test child has a parent"),
                cwd: cwd.to_string_lossy().into_owned(),
                model: "gpt-5.6-sol".into(),
                reasoning_effort: crate::daemon::protocol::ReasoningEffort::Xhigh,
                approval_policy,
                repositories: Vec::new(),
            },
        )
    }

    pub(in crate::daemon) fn start_thread_for_logical_read(
        runtime: &DaemonRuntime,
        cwd: &Path,
        approval_policy: ThreadApprovalPolicy,
    ) -> ThreadSpawnResult {
        start_thread(runtime, None, cwd, approval_policy).unwrap()
    }

    fn start_thread_with_repositories(
        runtime: &DaemonRuntime,
        repositories: Vec<ThreadRepositoryRequest>,
    ) -> Result<ThreadSpawnResult, ThreadSpawnFailure> {
        start_test_home(
            runtime,
            &runtime.paths.workspace_root,
            ThreadApprovalPolicy::Prompt,
            repositories,
        )
    }

    fn start_test_home(
        runtime: &DaemonRuntime,
        cwd: &Path,
        approval_policy: ThreadApprovalPolicy,
        mut repositories: Vec<ThreadRepositoryRequest>,
    ) -> Result<ThreadSpawnResult, ThreadSpawnFailure> {
        if repositories.is_empty() {
            repositories.push(ThreadRepositoryRequest {
                repo: ".".into(),
                branch: None,
            });
        }
        let working_repository = repositories[0].repo.clone();
        let repository_root = runtime.paths.workspace_root.join(&working_repository);
        let working_subdir = cwd
            .canonicalize()
            .ok()
            .and_then(|cwd| {
                repository_root
                    .canonicalize()
                    .ok()
                    .and_then(|root| cwd.strip_prefix(root).ok().map(Path::to_path_buf))
            })
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| PathBuf::from("."))
            .to_string_lossy()
            .into_owned();
        let config = Config::load(cwd, None)
            .map_err(|error| ThreadSpawnFailure::Malformed(error.to_string()))?;
        let profile_id = create_test_profile(runtime, approval_policy, config.tools.enabled);
        let result = start_profile_home(
            runtime,
            profile_id,
            new_spawn_id(),
            repositories,
            working_repository,
            working_subdir,
        )
        .map_err(profile_open_test_failure)?;
        match result {
            ProfileOpenResult::ApprovalRequired {
                home_reservation_id,
                thread_id,
                effect,
                reason,
                ..
            } => Ok(ThreadSpawnResult::ApprovalRequired {
                spawn_id: home_reservation_id,
                thread_id,
                effect,
                reason,
            }),
            ProfileOpenResult::Opened { thread, .. } => {
                Ok(ThreadSpawnResult::Spawned { thread: *thread })
            }
            unexpected => Err(ThreadSpawnFailure::Conflict(format!(
                "unexpected test home result: {unexpected:?}"
            ))),
        }
    }

    fn create_test_profile(
        runtime: &DaemonRuntime,
        approval_policy: ThreadApprovalPolicy,
        toolset: Vec<String>,
    ) -> ProfileId {
        let created_at_ms = now_ms();
        let profile_id = crate::server_store::mint_profile_id(
            &runtime.paths.workspace_id,
            "thread test",
            created_at_ms,
        );
        let content = crate::server_store::ProfileRevisionContent::empty();
        let content_hash = content.content_hash().unwrap();
        let profile = crate::server_store::ProfileRecord {
            id: profile_id.clone(),
            workspace_id: runtime.paths.workspace_id.clone(),
            display_name: profile_id.to_string(),
            model: "gpt-5.6-sol".into(),
            reasoning_effort: crate::daemon::protocol::ReasoningEffort::Xhigh,
            approval_policy,
            toolset,
            current_revision: 1,
            home_thread_id: None,
            imported_agent_id: None,
            created_at_ms,
        };
        let revision = crate::server_store::ProfileRevisionRecord {
            profile_id: profile_id.clone(),
            revision: 1,
            parent_revision: None,
            actor: "test".into(),
            created_at_ms: profile.created_at_ms,
            content_hash,
            content,
        };
        runtime
            .paths
            .server_store()
            .unwrap()
            .create_profile(&profile, &revision)
            .unwrap();
        profile_id
    }

    fn profile_open_test_failure(error: ProfileOpenFailure) -> ThreadSpawnFailure {
        match error {
            ProfileOpenFailure::ShuttingDown => ThreadSpawnFailure::ShuttingDown,
            ProfileOpenFailure::Malformed(message) => ThreadSpawnFailure::Malformed(message),
            ProfileOpenFailure::NotFound(message) => ThreadSpawnFailure::NotFound(message),
            ProfileOpenFailure::WorkspaceBroken(message) => {
                ThreadSpawnFailure::WorkspaceBroken(message)
            }
            ProfileOpenFailure::WorkspaceMismatch(message) => {
                ThreadSpawnFailure::WorkspaceMismatch(message)
            }
            ProfileOpenFailure::Conflict(message) => ThreadSpawnFailure::Conflict(message),
            ProfileOpenFailure::Overload(message) => ThreadSpawnFailure::Overload(message),
            ProfileOpenFailure::ConfinementUnavailable => {
                ThreadSpawnFailure::ConfinementUnavailable
            }
            ProfileOpenFailure::Persistence => ThreadSpawnFailure::Persistence,
        }
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

    pub(in crate::daemon) fn pending_spawn(result: ThreadSpawnResult) -> (String, String) {
        match result {
            ThreadSpawnResult::ApprovalRequired {
                spawn_id,
                thread_id,
                effect,
                reason,
            } => {
                assert_eq!(effect, EffectClass::WorkspaceWrite);
                assert!(matches!(
                    reason.as_str(),
                    THREAD_SPAWN_APPROVAL_REASON | PROFILE_OPEN_APPROVAL_REASON
                ));
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
        if spawn_id.starts_with("home_reservation_") {
            let (decision, actor) = match approval {
                ThreadSpawnDecision::Grant { actor } => (ProfileOpenDecision::Grant, actor),
                ThreadSpawnDecision::Deny { actor, reason } => {
                    (ProfileOpenDecision::Deny { reason }, actor)
                }
                ThreadSpawnDecision::Cancel { actor } => (ProfileOpenDecision::Cancel, actor),
            };
            return match decide_profile_home(runtime, spawn_id, decision)
                .map_err(profile_open_test_failure)?
            {
                ProfileOpenResult::Opened { thread, .. } => {
                    Ok(ThreadSpawnResult::Spawned { thread: *thread })
                }
                ProfileOpenResult::Denied {
                    home_reservation_id,
                    thread_id,
                    reason,
                    ..
                } => Ok(ThreadSpawnResult::Denied {
                    spawn_id: home_reservation_id,
                    thread_id,
                    actor,
                    reason,
                }),
                ProfileOpenResult::Canceled {
                    home_reservation_id,
                    thread_id,
                    ..
                } => Ok(ThreadSpawnResult::Canceled {
                    spawn_id: home_reservation_id,
                    thread_id,
                    actor,
                }),
                unexpected => Err(ThreadSpawnFailure::Conflict(format!(
                    "unexpected test home decision: {unexpected:?}"
                ))),
            };
        }
        thread_spawn(
            runtime,
            ThreadSpawnParams::Decide {
                spawn_id: spawn_id.into(),
                approval,
            },
        )
    }

    pub(in crate::daemon) fn grant_thread(
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
            ThreadSpawnResult::Spawned { thread } => {
                if thread.authority.thread_kind == ThreadKind::Home {
                    runtime
                        .load_thread(&thread.authority.thread_id)
                        .expect("test home can be explicitly loaded");
                    joined_thread_status(
                        runtime,
                        runtime
                            .paths
                            .server_store()
                            .unwrap()
                            .thread_authority(&thread.authority.thread_id)
                            .unwrap()
                            .unwrap(),
                    )
                    .unwrap()
                } else {
                    thread
                }
            }
            unexpected => panic!("expected spawned thread, got {unexpected:?}"),
        }
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

    #[test]
    fn legacy_status_does_not_fabricate_a_home_relation() {
        let (_root, runtime) = thread_test_runtime();
        let home_thread_id = coordinator_root(&runtime);
        let mut legacy = runtime
            .paths
            .server_store()
            .unwrap()
            .thread_authority(&home_thread_id)
            .unwrap()
            .unwrap();
        legacy.thread_id = "thread_legacy".into();
        legacy.thread_kind = ThreadKind::Legacy;

        let status = joined_thread_status(&runtime, legacy).unwrap();
        assert!(status.authority.profile_id.is_some());
        assert_eq!(status.authority.home_thread_id, None);
    }

    fn model_spawn_input(
        runtime: &DaemonRuntime,
        parent_thread_id: &str,
        toolset: Option<Vec<String>>,
        approval_policy: Option<ThreadApprovalPolicy>,
    ) -> ThreadSpawnToolInput {
        let cwd = thread_worktree(runtime, parent_thread_id)
            .to_string_lossy()
            .into_owned();
        ThreadSpawnToolInput {
            cwd,
            model: None,
            reasoning_effort: None,
            approval_policy,
            toolset,
            repositories: None,
        }
    }

    fn coordinator_child(
        runtime: &DaemonRuntime,
        parent_thread_id: &str,
        actor: &str,
    ) -> ThreadStatus {
        let thread_id = match model_thread_spawn(
            runtime,
            parent_thread_id,
            model_spawn_input(runtime, parent_thread_id, None, None),
            actor.into(),
        )
        .unwrap()
        {
            ThreadSpawnToolOutput::Spawned { thread_id } => thread_id,
            output => panic!("expected spawned child, got {output:?}"),
        };
        joined_thread_status(
            runtime,
            runtime
                .paths
                .server_store()
                .unwrap()
                .thread_authority(&thread_id)
                .unwrap()
                .unwrap(),
        )
        .unwrap()
    }

    pub(in crate::daemon) fn child_return_test_runtime() -> (
        tempfile::TempDir,
        DaemonRuntime,
        String,
        String,
        String,
        platonic_core::ProfileId,
    ) {
        let (root, runtime) = thread_test_runtime();
        std::fs::write(
            runtime.paths.workspace_root.join("plato.toml"),
            "[tools]\nenabled = [\"file.read\", \"thread.spawn\", \"thread.return\", \"thread.answer\"]\n",
        )
        .unwrap();
        let (spawn_id, home_thread_id) = pending_spawn(
            start_thread(
                &runtime,
                None,
                &runtime.paths.workspace_root,
                ThreadApprovalPolicy::Prompt,
            )
            .unwrap(),
        );
        grant_thread(&runtime, &spawn_id, "return-test");
        let child_thread_id = coordinator_child(&runtime, &home_thread_id, "return-test")
            .authority
            .thread_id;
        let sibling_thread_id = coordinator_child(&runtime, &home_thread_id, "return-test")
            .authority
            .thread_id;
        let profile_id = runtime
            .paths
            .server_store()
            .unwrap()
            .thread_authority(&home_thread_id)
            .unwrap()
            .unwrap()
            .profile_id
            .unwrap();
        (
            root,
            runtime,
            home_thread_id,
            child_thread_id,
            sibling_thread_id,
            profile_id,
        )
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
    fn model_spawn_reuses_durable_same_profile_admission_and_records_the_approving_actor() {
        let (_root, runtime) = thread_test_runtime();
        let parent_thread_id = coordinator_root(&runtime);

        let output = model_thread_spawn(
            &runtime,
            &parent_thread_id,
            model_spawn_input(&runtime, &parent_thread_id, None, None),
            "daemon".into(),
        )
        .unwrap();
        let child_thread_id = match output {
            ThreadSpawnToolOutput::Spawned { thread_id } => thread_id,
            output => panic!("expected spawned worker, got {output:?}"),
        };

        let store = runtime.paths.server_store().unwrap();
        let parent = store.thread_authority(&parent_thread_id).unwrap().unwrap();
        let child = store.thread_authority(&child_thread_id).unwrap().unwrap();
        assert_eq!(
            child.parent_thread_id.as_deref(),
            Some(parent_thread_id.as_str())
        );
        assert_eq!(child.spawning_actor, "daemon");
        assert_eq!(child.agent_id, None);
        assert_eq!(child.profile_id, parent.profile_id);
        assert_eq!(child.profile_revision, parent.profile_revision);
        assert_eq!(child.thread_kind, ThreadKind::Child);
        assert_eq!(child.model, "gpt-5.6-sol");
        assert_eq!(child.toolset, ["file.read", "thread.spawn"]);
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
    fn each_new_turn_selects_the_latest_committed_profile_revision() {
        let (_root, runtime) = thread_test_runtime();
        let thread_id = coordinator_root(&runtime);
        let authority = runtime
            .paths
            .server_store()
            .unwrap()
            .thread_authority(&thread_id)
            .unwrap()
            .unwrap();
        let profile_id = authority.profile_id.clone().unwrap();
        assert_eq!(authority.profile_revision, Some(1));
        runtime
            .paths
            .server_store()
            .unwrap()
            .update_profile_content(
                &profile_id,
                "operator",
                authority.created_at_ms + 1,
                crate::server_store::ProfileRevisionContent {
                    instructions_markdown: "latest instructions".into(),
                    memory_markdown: "latest memory".into(),
                    skill_refs: vec![],
                },
            )
            .unwrap()
            .unwrap();

        assert_eq!(
            current_profile_identity(&runtime, &authority).unwrap(),
            RunIdentity::Profile {
                profile_id,
                profile_revision: 2,
            }
        );
        assert_eq!(authority.profile_revision, Some(1));
    }

    #[test]
    fn model_spawn_returns_typed_rejections_for_authority_escalation() {
        let (_root, runtime) = thread_test_runtime();
        let parent_thread_id = coordinator_root(&runtime);
        let outside = tempfile::tempdir().unwrap();
        for (label, input) in [
            (
                "parent toolset",
                model_spawn_input(
                    &runtime,
                    &parent_thread_id,
                    Some(vec!["file.read".into(), "web.fetch".into()]),
                    None,
                ),
            ),
            (
                "parent policy",
                model_spawn_input(
                    &runtime,
                    &parent_thread_id,
                    Some(vec!["file.read".into()]),
                    Some(ThreadApprovalPolicy::Yolo),
                ),
            ),
            (
                "profile toolset",
                model_spawn_input(
                    &runtime,
                    &parent_thread_id,
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
            model_spawn_input(&runtime, &first_child, Some(vec!["file.read".into()]), None),
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
    fn child_admission_rejects_a_cross_profile_parent_before_reservation() {
        let (_root, runtime) = thread_test_runtime();
        let parent_thread_id = coordinator_root(&runtime);
        let parent = runtime
            .paths
            .server_store()
            .unwrap()
            .thread_authority(&parent_thread_id)
            .unwrap()
            .unwrap();
        let other_profile = create_test_profile(
            &runtime,
            ThreadApprovalPolicy::Prompt,
            vec!["file.read".into()],
        );
        let draft = ThreadAuthorityDraft::new(ThreadAuthorityDraftParams {
            parent_thread_id: Some(parent_thread_id),
            cwd: Path::new(parent.cwd.as_deref().unwrap()),
            model: parent.model,
            reasoning_effort: parent.reasoning_effort,
            approval_policy: ThreadApprovalPolicy::Prompt,
            agent_id: None,
            profile_id: other_profile,
            profile_revision: 1,
            thread_kind: ThreadKind::Child,
            toolset: vec!["file.read".into()],
            writable: false,
            network: false,
        })
        .unwrap();
        let mut store = runtime.paths.server_store().unwrap();
        assert!(matches!(
            start_thread_spawn_draft(&runtime, &mut store, draft, Vec::new(), 1, "test"),
            Err(ThreadSpawnFailure::Authority(
                ThreadAuthorityError::SameProfileParent
            ))
        ));
        assert_eq!(store.thread_authorities().unwrap().len(), 1);
        assert_eq!(store.branch_claims().unwrap().len(), 1);
    }

    #[test]
    fn model_spawn_enforces_frozen_server_depth_after_workspace_mutation() {
        let (_root, runtime) = thread_test_runtime_with_max_spawn_depth(1);
        let parent_thread_id = coordinator_root(&runtime);
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
        let child_thread_id = match model_thread_spawn(
            &runtime,
            &parent_thread_id,
            model_spawn_input(
                &runtime,
                &parent_thread_id,
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
                r#"{{"v":2,"id":"stop","kind":"request","method":"thread.stop","params":{{"thread_id":"{thread_id}","actor":"{actor}"}}}}"#
            ),
        )
    }

    #[test]
    fn profile_home_becomes_durable_without_becoming_live_or_stoppable() {
        let (_root, runtime) = thread_test_runtime();
        let expected_toolset = Config::load(&runtime.paths.workspace_root, None)
            .unwrap()
            .tools
            .enabled;
        let expected_network = toolset_has_effect(&expected_toolset, EffectClass::Network);
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

        let status = match decide_thread(
            &runtime,
            &spawn_id,
            ThreadSpawnDecision::Grant {
                actor: "ignored-client-actor".into(),
            },
        )
        .unwrap()
        {
            ThreadSpawnResult::Spawned { thread } => thread,
            result => panic!("expected opened home, got {result:?}"),
        };
        assert_eq!(status.authority.thread_id, thread_id);
        assert_eq!(status.authority.spawning_actor, LOCAL_OPERATOR_ACTOR);
        assert_eq!(status.authority.parent_thread_id, None);
        assert_eq!(status.authority.thread_kind, ThreadKind::Home);
        let authority_response = handle_line(
            &runtime,
            &format!(
                r#"{{"v":2,"id":"authority","kind":"request","method":"thread.authority","params":{{"thread_id":"{thread_id}"}}}}"#
            ),
        );
        let authority: ThreadAuthorityResult = response_result(&authority_response);
        let expected_confinement = match runtime.confinement_support() {
            crate::confinement::ConfinementSupport::Landlock => ThreadConfinement::Landlock,
            crate::confinement::ConfinementSupport::None => ThreadConfinement::None,
        };
        assert_eq!(authority.confinement, Some(expected_confinement));
        let authority = authority.authority;
        assert_eq!(authority.agent_id, None);
        assert_eq!(authority.profile_id, status.authority.profile_id);
        assert_eq!(authority.profile_revision, Some(1));
        assert_eq!(authority.thread_kind, ThreadKind::Home);
        assert_eq!(authority.worktrees.len(), 1);
        assert_eq!(authority.worktrees[0].repo, ".");
        assert_eq!(authority.worktrees[0].branch, format!("thread/{thread_id}"));
        assert_eq!(status.authority.cwd, authority.worktrees[0].path);
        assert_eq!(authority.granted_paths.len(), 1);
        assert!(authority.granted_paths[0].writable);
        assert_eq!(authority.toolset, expected_toolset);
        assert_eq!(authority.network, expected_network);
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
                live_epoch_id: runtime.live_epoch_id(),
                loaded: false,
                current_turn_id: None,
                last_activity_at_ms: None,
            }
        );
        let store = runtime.paths.server_store().unwrap();
        assert_eq!(store.thread_authority(&thread_id).unwrap(), Some(authority));
        let reservation = store.home_reservation(&spawn_id).unwrap().unwrap();
        assert_eq!(reservation.state, HomeReservationState::Granted);
        assert_eq!(
            reservation.decided_by.as_deref(),
            Some(LOCAL_OPERATOR_ACTOR)
        );
        assert_eq!(
            store
                .profile(status.authority.profile_id.as_ref().unwrap())
                .unwrap()
                .unwrap()
                .home_thread_id
                .as_deref(),
            Some(thread_id.as_str())
        );
        drop(store);
        let stopped = stop_thread(&runtime, &thread_id, "operator");
        let error = stopped.error.unwrap();
        assert_eq!(error.code, ERROR_THREAD_STOP_FAILED);
        assert!(error.message.contains("cannot be stopped"));
    }

    #[test]
    fn profile_open_converges_concurrently_replays_and_resolves_across_restart() {
        let (_root, runtime) = thread_test_runtime();
        let profile_id = create_test_profile(
            &runtime,
            ThreadApprovalPolicy::Prompt,
            vec!["file.read".into()],
        );
        assert_eq!(
            resolve_profile_home(&runtime, &profile_id).unwrap(),
            ProfileOpenResult::NoHome {
                profile_id: profile_id.clone()
            }
        );
        assert!(
            runtime
                .paths
                .server_store()
                .unwrap()
                .thread_authorities()
                .unwrap()
                .is_empty()
        );

        let ready = Arc::new(std::sync::Barrier::new(3));
        let starts = (0..2)
            .map(|_| {
                let runtime = runtime.clone();
                let profile_id = profile_id.clone();
                let ready = ready.clone();
                thread::spawn(move || {
                    ready.wait();
                    start_profile_home(
                        &runtime,
                        profile_id,
                        "same-request".into(),
                        vec![ThreadRepositoryRequest {
                            repo: ".".into(),
                            branch: None,
                        }],
                        ".".into(),
                        ".".into(),
                    )
                })
            })
            .collect::<Vec<_>>();
        ready.wait();
        let pending = starts
            .into_iter()
            .map(|start| match start.join().unwrap().unwrap() {
                ProfileOpenResult::ApprovalRequired {
                    home_reservation_id,
                    thread_id,
                    ..
                } => (home_reservation_id, thread_id),
                result => panic!("expected pending home, got {result:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(pending[0], pending[1]);
        let (reservation_id, thread_id) = pending[0].clone();

        assert!(matches!(
            start_profile_home(
                &runtime,
                profile_id.clone(),
                "same-request".into(),
                vec![ThreadRepositoryRequest {
                    repo: ".".into(),
                    branch: None,
                }],
                ".".into(),
                ".".into(),
            )
            .unwrap(),
            ProfileOpenResult::ApprovalRequired {
                home_reservation_id,
                thread_id: replayed_thread_id,
                ..
            } if home_reservation_id == reservation_id && replayed_thread_id == thread_id
        ));
        std::fs::create_dir(runtime.paths.workspace_root.join("other")).unwrap();
        assert!(matches!(
            start_profile_home(
                &runtime,
                profile_id.clone(),
                "same-request".into(),
                vec![ThreadRepositoryRequest {
                    repo: ".".into(),
                    branch: None,
                }],
                ".".into(),
                "other".into(),
            ),
            Err(ProfileOpenFailure::Conflict(message))
                if message.contains("different home proposal")
        ));
        assert!(matches!(
            start_profile_home(
                &runtime,
                profile_id.clone(),
                "different-request".into(),
                vec![ThreadRepositoryRequest {
                    repo: ".".into(),
                    branch: None,
                }],
                ".".into(),
                ".".into(),
            ),
            Err(ProfileOpenFailure::Conflict(message))
                if message.contains("pending home reservation")
        ));

        let opened =
            decide_profile_home(&runtime, &reservation_id, ProfileOpenDecision::Grant).unwrap();
        assert!(matches!(
            opened,
            ProfileOpenResult::Opened {
                created: true,
                ref thread,
                ..
            } if thread.authority.thread_id == thread_id && !thread.live.loaded
        ));
        assert!(matches!(
            resolve_profile_home(&runtime, &profile_id).unwrap(),
            ProfileOpenResult::Opened {
                created: false,
                ref thread,
                ..
            } if thread.authority.thread_id == thread_id && !thread.live.loaded
        ));
        assert!(matches!(
            start_profile_home(
                &runtime,
                profile_id.clone(),
                "same-request".into(),
                vec![ThreadRepositoryRequest {
                    repo: ".".into(),
                    branch: None,
                }],
                ".".into(),
                ".".into(),
            )
            .unwrap(),
            ProfileOpenResult::Opened {
                created: false,
                ref thread,
                ..
            } if thread.authority.thread_id == thread_id
        ));
        assert!(matches!(
            start_profile_home(
                &runtime,
                profile_id.clone(),
                "second-home".into(),
                vec![ThreadRepositoryRequest {
                    repo: ".".into(),
                    branch: None,
                }],
                ".".into(),
                ".".into(),
            ),
            Err(ProfileOpenFailure::Conflict(message)) if message.contains("already has a home")
        ));

        let old_epoch = runtime.live_epoch_id();
        let restarted = DaemonRuntime::new(runtime.paths.clone());
        assert_ne!(restarted.live_epoch_id(), old_epoch);
        assert!(matches!(
            resolve_profile_home(&restarted, &profile_id).unwrap(),
            ProfileOpenResult::Opened {
                created: false,
                ref thread,
                ..
            } if thread.authority.thread_id == thread_id
                && !thread.live.loaded
                && thread.live.live_epoch_id == restarted.live_epoch_id()
        ));
        let store = restarted.paths.server_store().unwrap();
        assert_eq!(store.thread_authorities().unwrap().len(), 1);
        assert_eq!(store.branch_claims().unwrap().len(), 1);
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
                    if message.contains("thread repository is not a Git repository")
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
        let home_thread_id = coordinator_root(&runtime);
        let thread_id = coordinator_child(&runtime, &home_thread_id, "stdin")
            .authority
            .thread_id;
        let store = runtime.paths.server_store().unwrap();
        let authority = store.thread_authority(&thread_id).unwrap().unwrap();
        assert_eq!(authority.worktrees.len(), 1);
        assert_eq!(authority.worktrees[0].repo, ".");
        assert_eq!(authority.worktrees[0].branch, format!("thread/{thread_id}"));
        assert_eq!(store.branch_claims().unwrap().len(), 2);
        drop(store);
        let response = handle_line(
            &runtime,
            &format!(
                r#"{{"v":2,"id":"authority","kind":"request","method":"thread.authority","params":{{"thread_id":"{thread_id}"}}}}"#
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
        assert_eq!(store.branch_claims().unwrap().len(), 1);
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
        assert!(matches!(
            start_thread_with_repositories(&runtime, vec![request]),
            Err(ThreadSpawnFailure::Conflict(message))
                if message.contains("already claimed")
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
                r#"{{"v":2,"id":"fallback-status","kind":"request","method":"thread.authority","params":{{"thread_id":"{fallback_thread}"}}}}"#
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
                r#"{{"v":2,"id":"required","kind":"request","method":"profile.open","params":{{"action":"decide","home_reservation_id":"{spawn_id}","decision":{{"decision":"grant"}}}}}}"#
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
                HomeReservationState::Denied,
            ),
            (
                "canceled",
                ThreadSpawnDecision::Cancel {
                    actor: "stdin".into(),
                },
                HomeReservationState::Canceled,
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
            let reservation = store.home_reservation(&spawn_id).unwrap().unwrap();
            assert_eq!(reservation.state, expected);
            assert_eq!(
                reservation.decided_by.as_deref(),
                Some(LOCAL_OPERATOR_ACTOR)
            );
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
        let home_thread_id = coordinator_root(&runtime);
        let threads = [
            coordinator_child(&runtime, &home_thread_id, "stdin"),
            coordinator_child(&runtime, &home_thread_id, "stdin"),
        ];
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
                live_epoch_id: runtime.live_epoch_id(),
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
        let home_thread_id = coordinator_root(&runtime);
        let threads = [
            coordinator_child(&runtime, &home_thread_id, "stdin"),
            coordinator_child(&runtime, &home_thread_id, "stdin"),
        ];
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
        let home_thread_id = coordinator_root(&runtime);
        let threads = [
            coordinator_child(&runtime, &home_thread_id, "stdin"),
            coordinator_child(&runtime, &home_thread_id, "stdin"),
        ];
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
                live_epoch_id: restarted.live_epoch_id(),
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
        assert_eq!(columns.len(), 18);
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
        let child_dir = thread_worktree(&runtime, &parent.authority.thread_id);

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
                    parent_thread_id: parent.authority.thread_id,
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
        let child_dir = thread_worktree(&runtime, &parent.authority.thread_id);
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
        let child_dir = thread_worktree(&runtime, &parent.authority.thread_id);
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
        let child_dir = thread_worktree(&runtime, &parent.authority.thread_id);
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
            r#"{"v":2,"id":"list","kind":"request","method":"thread.list"}"#,
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
                r#"{{"v":2,"id":"status","kind":"request","method":"thread.status","params":{{"thread_id":"{}"}}}}"#,
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
            joined_thread_status(
                &runtime,
                store
                    .thread_authority(&thread_id)
                    .unwrap()
                    .expect("granted authority is durable")
            )
            .unwrap()
            .authority,
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
            r#"{"v":2,"id":"bad","kind":"request","method":"thread.spawn","params":{"action":"start","parent_thread_id":null,"cwd":"/tmp","model":"gpt-5.6-sol","reasoning_effort":"xhigh","approval_policy":"prompt","extra":true}}"#,
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
            r#"{"v":2,"id":"bad","kind":"request","method":"thread.authority","params":{"thread_id":"thread_1","future":true}}"#,
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
                r#"{{"v":2,"id":"bad","kind":"request","method":"thread.send","params":{{"thread_id":"{thread_id}","controller_id":"","message":"no"}}}}"#
            ),
        );
        assert_eq!(malformed.error.unwrap().code, ERROR_MALFORMED_REQUEST);

        let stale = handle_line(
            &runtime,
            &format!(
                r#"{{"v":2,"id":"stale","kind":"request","method":"thread.send","params":{{"thread_id":"{thread_id}","controller_id":"controller_a","turn_id":"thread_turn_stale","message":"no"}}}}"#
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
                r#"{{"v":2,"id":"events","kind":"request","method":"thread.events","params":{{"thread_id":"{thread_id}","limit":0}}}}"#
            ),
        );
        assert_eq!(invalid_events.error.unwrap().code, ERROR_MALFORMED_REQUEST);
        assert_eq!(runtime.thread_live_state(&thread_id).current_turn_id, None);
        let store = runtime.paths.server_store().unwrap();
        assert_eq!(
            joined_thread_status(
                &runtime,
                store
                    .thread_authority(&thread_id)
                    .unwrap()
                    .expect("granted authority is durable")
            )
            .unwrap()
            .authority,
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
