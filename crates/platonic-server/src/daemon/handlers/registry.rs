use super::control::store_error;
use crate::{
    daemon::{
        protocol::{
            AgentCreateParams, AgentCreateResult, AgentListParams, AgentListResult,
            AgentStatusParams, AgentStatusResult, AgentSummary, ERROR_MALFORMED_REQUEST,
            ERROR_NOT_FOUND, ERROR_WORKSPACE_BROKEN, Envelope, ProtocolResponse,
            WorkspaceCreateParams, WorkspaceCreateResult, WorkspaceHealthName, WorkspaceListParams,
            WorkspaceListResult, WorkspaceStatusParams, WorkspaceStatusResult, WorkspaceSummary,
        },
        runtime::DaemonRuntime,
    },
    server_store::AgentRecord,
    thread_authority::now_ms,
    tool_catalog::is_known_tool,
};

fn workspace_summary(record: &crate::server_store::WorkspaceRecord) -> WorkspaceSummary {
    WorkspaceSummary {
        id: record.id.clone(),
        name: record.name.clone(),
        root: record.root.clone(),
        ledger_path: record.ledger_path.clone(),
        created_at_ms: record.created_at_ms,
        health: match record.health() {
            crate::server_store::WorkspaceHealth::Present => WorkspaceHealthName::Present,
            crate::server_store::WorkspaceHealth::Broken => WorkspaceHealthName::Broken,
        },
    }
}

/// `workspace.create` — name a directory so the server knows it deliberately.
///
/// The directory must exist. Creating a workspace over a missing directory
/// would mint a record that is broken the moment it is written, which is worse
/// than refusing (P021).
pub(super) fn handle_workspace_create(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: WorkspaceCreateParams,
) -> Envelope {
    let name = params.name.trim();
    if name.is_empty() {
        return Envelope::error(
            request.id,
            Some("workspace.create".into()),
            ERROR_MALFORMED_REQUEST,
            "workspace name must not be empty",
        );
    }
    let root = std::path::Path::new(&params.root);
    if !root.is_dir() {
        return Envelope::error(
            request.id,
            Some("workspace.create".into()),
            ERROR_MALFORMED_REQUEST,
            format!("workspace root is not a directory: {}", params.root),
        );
    }
    let root = match root.canonicalize() {
        Ok(root) => root,
        Err(error) => {
            return Envelope::error(
                request.id,
                Some("workspace.create".into()),
                ERROR_MALFORMED_REQUEST,
                format!("workspace root could not be resolved: {error}"),
            );
        }
    };
    let mut store = match runtime.paths.server_store() {
        Ok(store) => store,
        Err(error) => return store_error(request.id, "workspace.create", error),
    };
    match store.workspace_by_name(name) {
        Ok(Some(_)) => {
            return Envelope::error(
                request.id,
                Some("workspace.create".into()),
                ERROR_MALFORMED_REQUEST,
                format!("workspace already exists: {name}"),
            );
        }
        Ok(None) => {}
        Err(error) => return store_error(request.id, "workspace.create", error),
    }
    match store.workspace_by_root(&root.to_string_lossy()) {
        Ok(Some(existing)) => {
            return Envelope::error(
                request.id,
                Some("workspace.create".into()),
                ERROR_MALFORMED_REQUEST,
                format!(
                    "workspace root is already registered as {}: {}",
                    existing.name,
                    root.display()
                ),
            );
        }
        Ok(None) => {}
        Err(error) => return store_error(request.id, "workspace.create", error),
    }
    let now_ms = crate::thread_authority::now_ms();
    let workspace_id = crate::server_store::mint_workspace_id(name, now_ms);
    let ledger_path =
        match crate::paths::workspace_sqlite_path(&runtime.paths.server_db_path, &workspace_id) {
            Ok(path) => path,
            Err(error) => return store_error(request.id, "workspace.create", error),
        };
    match store.register_workspace(
        &workspace_id,
        name,
        &root.to_string_lossy(),
        &ledger_path.to_string_lossy(),
        now_ms,
    ) {
        Ok((record, true)) => Envelope::typed_response(
            request.id,
            ProtocolResponse::WorkspaceCreate(WorkspaceCreateResult {
                workspace: workspace_summary(&record),
            }),
        ),
        Ok((existing, false)) if existing.name == name => Envelope::error(
            request.id,
            Some("workspace.create".into()),
            ERROR_MALFORMED_REQUEST,
            format!("workspace already exists: {name}"),
        ),
        Ok((existing, false)) => Envelope::error(
            request.id,
            Some("workspace.create".into()),
            ERROR_MALFORMED_REQUEST,
            format!(
                "workspace root is already registered as {}: {}",
                existing.name,
                root.display()
            ),
        ),
        Err(error) => store_error(request.id, "workspace.create", error),
    }
}

/// `workspace.list` — every registered workspace, broken ones included.
pub(super) fn handle_workspace_list(
    runtime: &DaemonRuntime,
    request: Envelope,
    _params: WorkspaceListParams,
) -> Envelope {
    let store = match runtime.paths.server_store() {
        Ok(store) => store,
        Err(error) => return store_error(request.id, "workspace.list", error),
    };
    match store.workspaces() {
        Ok(records) => Envelope::typed_response(
            request.id,
            ProtocolResponse::WorkspaceList(WorkspaceListResult {
                workspaces: records.iter().map(workspace_summary).collect(),
            }),
        ),
        Err(error) => store_error(request.id, "workspace.list", error),
    }
}

/// `workspace.status` — one workspace by minted id.
pub(super) fn handle_workspace_status(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: WorkspaceStatusParams,
) -> Envelope {
    let store = match runtime.paths.server_store() {
        Ok(store) => store,
        Err(error) => return store_error(request.id, "workspace.status", error),
    };
    match store.workspace(&params.workspace_id) {
        Ok(Some(record)) => Envelope::typed_response(
            request.id,
            ProtocolResponse::WorkspaceStatus(WorkspaceStatusResult {
                workspace: workspace_summary(&record),
            }),
        ),
        Ok(None) => Envelope::error(
            request.id,
            Some("workspace.status".into()),
            ERROR_NOT_FOUND,
            format!("workspace not found: {}", params.workspace_id),
        ),
        Err(error) => store_error(request.id, "workspace.status", error),
    }
}

fn agent_summary(record: &AgentRecord) -> AgentSummary {
    AgentSummary {
        id: record.id.clone(),
        workspace_id: record.workspace_id.clone(),
        model: record.model.clone(),
        reasoning_effort: record.reasoning_effort,
        approval_policy: record.approval_policy,
        toolset: record.toolset.clone(),
        created_at_ms: record.created_at_ms,
    }
}

pub(super) fn handle_agent_create(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: AgentCreateParams,
) -> Envelope {
    if params.model.trim().is_empty() {
        return Envelope::error(
            request.id,
            Some("agent.create".into()),
            ERROR_MALFORMED_REQUEST,
            "agent model must not be empty",
        );
    }
    if params.toolset.is_empty() {
        return Envelope::error(
            request.id,
            Some("agent.create".into()),
            ERROR_MALFORMED_REQUEST,
            "agent toolset must not be empty",
        );
    }
    if let Some(tool) = params.toolset.iter().find(|tool| !is_known_tool(tool)) {
        return Envelope::error(
            request.id,
            Some("agent.create".into()),
            ERROR_MALFORMED_REQUEST,
            format!("unknown tool in agent toolset: {tool}"),
        );
    }
    let store = match runtime.paths.server_store() {
        Ok(store) => store,
        Err(error) => return store_error(request.id, "agent.create", error),
    };
    match store.workspace(&params.workspace_id) {
        Ok(Some(workspace))
            if workspace.health() == crate::server_store::WorkspaceHealth::Present => {}
        Ok(Some(_)) => {
            return Envelope::error(
                request.id,
                Some("agent.create".into()),
                ERROR_WORKSPACE_BROKEN,
                format!("workspace directory is missing: {}", params.workspace_id),
            );
        }
        Ok(None) => {
            return Envelope::error(
                request.id,
                Some("agent.create".into()),
                ERROR_NOT_FOUND,
                format!("workspace not found: {}", params.workspace_id),
            );
        }
        Err(error) => return store_error(request.id, "agent.create", error),
    }
    match store.agent(&params.agent_id) {
        Ok(Some(_)) => {
            return Envelope::error(
                request.id,
                Some("agent.create".into()),
                ERROR_MALFORMED_REQUEST,
                format!("agent already exists: {}", params.agent_id),
            );
        }
        Ok(None) => {}
        Err(error) => return store_error(request.id, "agent.create", error),
    }
    let record = AgentRecord {
        id: params.agent_id,
        workspace_id: params.workspace_id,
        model: params.model,
        reasoning_effort: params.reasoning_effort,
        approval_policy: params.approval_policy,
        toolset: params.toolset,
        created_at_ms: now_ms(),
    };
    match store.register_agent(&record) {
        Ok(true) => Envelope::typed_response(
            request.id,
            ProtocolResponse::AgentCreate(AgentCreateResult {
                agent: agent_summary(&record),
            }),
        ),
        Ok(false) => Envelope::error(
            request.id,
            Some("agent.create".into()),
            ERROR_MALFORMED_REQUEST,
            format!("agent already exists: {}", record.id),
        ),
        Err(error) => store_error(request.id, "agent.create", error),
    }
}

pub(super) fn handle_agent_list(
    runtime: &DaemonRuntime,
    request: Envelope,
    _params: AgentListParams,
) -> Envelope {
    let store = match runtime.paths.server_store() {
        Ok(store) => store,
        Err(error) => return store_error(request.id, "agent.list", error),
    };
    match store.agents() {
        Ok(records) => Envelope::typed_response(
            request.id,
            ProtocolResponse::AgentList(AgentListResult {
                agents: records.iter().map(agent_summary).collect(),
            }),
        ),
        Err(error) => store_error(request.id, "agent.list", error),
    }
}

pub(super) fn handle_agent_status(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: AgentStatusParams,
) -> Envelope {
    let store = match runtime.paths.server_store() {
        Ok(store) => store,
        Err(error) => return store_error(request.id, "agent.status", error),
    };
    match store.agent(&params.agent_id) {
        Ok(Some(record)) => Envelope::typed_response(
            request.id,
            ProtocolResponse::AgentStatus(AgentStatusResult {
                agent: agent_summary(&record),
            }),
        ),
        Ok(None) => Envelope::error(
            request.id,
            Some("agent.status".into()),
            ERROR_NOT_FOUND,
            format!("agent not found: {}", params.agent_id),
        ),
        Err(error) => store_error(request.id, "agent.status", error),
    }
}

// Phase 1 installs daemon-owned profile operations without adding protocol-v1
// methods. The v2 routing that calls these entry points belongs to a later issue.
#[allow(dead_code)]
pub(crate) mod profiles {
    use super::*;
    use crate::{
        AppError,
        server_store::{
            MAX_PROFILE_LIST_ENTRIES, MAX_PROFILE_REVISION_ACTOR_BYTES, ProfileRecord,
            ProfileRevisionContent, ProfileRevisionRecord, ProfileValidationError, mint_profile_id,
        },
    };
    use platonic_core::{ActorId, ModelName, ProfileId, ToolName};
    use platonic_protocol::{ReasoningEffort, ThreadApprovalPolicy};
    use std::path::PathBuf;

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct ProfileCreateRequest {
        pub(crate) workspace_id: String,
        pub(crate) display_name: String,
        pub(crate) model: String,
        pub(crate) reasoning_effort: ReasoningEffort,
        pub(crate) approval_policy: ThreadApprovalPolicy,
        pub(crate) toolset: Vec<String>,
        pub(crate) content: ProfileRevisionContent,
        pub(crate) actor: String,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct ProfileListRequest {
        pub(crate) workspace_id: Option<String>,
        pub(crate) limit: usize,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct ProfileUpdateRequest {
        pub(crate) profile_id: ProfileId,
        pub(crate) content: ProfileRevisionContent,
        pub(crate) actor: String,
    }

    impl Default for ProfileListRequest {
        fn default() -> Self {
            Self {
                workspace_id: None,
                limit: 50,
            }
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct ProfileSummary {
        pub(crate) profile: ProfileRecord,
        pub(crate) workspace_health: crate::server_store::WorkspaceHealth,
        pub(crate) home_path: PathBuf,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct ProfileStatus {
        pub(crate) summary: ProfileSummary,
        pub(crate) revision: ProfileRevisionRecord,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct ProfileListResult {
        pub(crate) profiles: Vec<ProfileSummary>,
        pub(crate) truncated: bool,
    }

    #[derive(Debug, thiserror::Error)]
    pub(crate) enum ProfileRegistryError {
        #[error("workspace not found: {0}")]
        WorkspaceNotFound(String),
        #[error("workspace directory is missing: {0}")]
        WorkspaceBroken(String),
        #[error("profile already exists in workspace {workspace_id}: {display_name}")]
        DuplicateName {
            workspace_id: String,
            display_name: String,
        },
        #[error("profile not found: {0}")]
        ProfileNotFound(String),
        #[error("invalid profile: {0}")]
        Invalid(String),
        #[error(transparent)]
        Content(#[from] ProfileValidationError),
        #[error(transparent)]
        Store(#[from] AppError),
    }

    pub(crate) fn create(
        runtime: &DaemonRuntime,
        request: ProfileCreateRequest,
    ) -> Result<ProfileStatus, ProfileRegistryError> {
        let display_name = request.display_name.trim();
        if display_name.is_empty() || request.model.trim().is_empty() || request.toolset.is_empty()
        {
            return Err(ProfileRegistryError::Invalid(
                "name, model, and toolset must not be empty".into(),
            ));
        }
        ModelName::new(request.model.clone())
            .map_err(|error| ProfileRegistryError::Invalid(error.to_string()))?;
        ActorId::new(request.actor.clone())
            .map_err(|error| ProfileRegistryError::Invalid(error.to_string()))?;
        if request.actor.len() > MAX_PROFILE_REVISION_ACTOR_BYTES {
            return Err(ProfileRegistryError::Invalid(format!(
                "revision actor exceeds {MAX_PROFILE_REVISION_ACTOR_BYTES} bytes"
            )));
        }
        for tool in &request.toolset {
            ToolName::new(tool.clone())
                .map_err(|error| ProfileRegistryError::Invalid(error.to_string()))?;
            if !is_known_tool(tool) {
                return Err(ProfileRegistryError::Invalid(format!(
                    "unknown tool in profile toolset: {tool}"
                )));
            }
        }
        request.content.validate()?;

        let mut store = runtime.paths.server_store()?;
        let workspace = store
            .workspace(&request.workspace_id)?
            .ok_or_else(|| ProfileRegistryError::WorkspaceNotFound(request.workspace_id.clone()))?;
        if workspace.health() == crate::server_store::WorkspaceHealth::Broken {
            return Err(ProfileRegistryError::WorkspaceBroken(request.workspace_id));
        }
        if store
            .profile_by_name(&request.workspace_id, display_name)?
            .is_some()
        {
            return Err(ProfileRegistryError::DuplicateName {
                workspace_id: request.workspace_id,
                display_name: display_name.into(),
            });
        }

        let created_at_ms = now_ms();
        let profile_id = mint_profile_id(&request.workspace_id, display_name, created_at_ms);
        let profile = ProfileRecord {
            id: profile_id.clone(),
            workspace_id: request.workspace_id.clone(),
            display_name: display_name.into(),
            model: request.model,
            reasoning_effort: request.reasoning_effort,
            approval_policy: request.approval_policy,
            toolset: request.toolset,
            current_revision: 1,
            home_thread_id: None,
            imported_agent_id: None,
            created_at_ms,
        };
        let revision = ProfileRevisionRecord {
            profile_id,
            revision: 1,
            parent_revision: None,
            actor: request.actor,
            created_at_ms,
            content_hash: request.content.content_hash()?,
            content: request.content,
        };
        if !store.create_profile(&profile, &revision)? {
            return Err(ProfileRegistryError::DuplicateName {
                workspace_id: profile.workspace_id,
                display_name: profile.display_name,
            });
        }
        status_from_records(runtime, &workspace, profile, revision)
    }

    pub(crate) fn list(
        runtime: &DaemonRuntime,
        request: ProfileListRequest,
    ) -> Result<ProfileListResult, ProfileRegistryError> {
        if request.limit == 0 || request.limit > MAX_PROFILE_LIST_ENTRIES {
            return Err(ProfileRegistryError::Invalid(format!(
                "profile list limit must be between 1 and {MAX_PROFILE_LIST_ENTRIES}"
            )));
        }
        let store = runtime.paths.server_store()?;
        let mut records = store.profiles(
            request.workspace_id.as_deref(),
            request.limit.saturating_add(1),
        )?;
        let truncated = records.len() > request.limit;
        records.truncate(request.limit);
        let mut profiles = Vec::with_capacity(records.len());
        for profile in records {
            let workspace = store.workspace(&profile.workspace_id)?.ok_or_else(|| {
                ProfileRegistryError::WorkspaceNotFound(profile.workspace_id.clone())
            })?;
            profiles.push(summary(runtime, &workspace, profile)?);
        }
        Ok(ProfileListResult {
            profiles,
            truncated,
        })
    }

    pub(crate) fn status(
        runtime: &DaemonRuntime,
        profile_id: &ProfileId,
    ) -> Result<ProfileStatus, ProfileRegistryError> {
        let store = runtime.paths.server_store()?;
        let profile = store
            .profile(profile_id)?
            .ok_or_else(|| ProfileRegistryError::ProfileNotFound(profile_id.as_str().into()))?;
        let workspace = store
            .workspace(&profile.workspace_id)?
            .ok_or_else(|| ProfileRegistryError::WorkspaceNotFound(profile.workspace_id.clone()))?;
        let revision = store
            .profile_revision(&profile.id, profile.current_revision)?
            .ok_or_else(|| {
                ProfileRegistryError::Invalid(format!(
                    "profile {} is missing revision {}",
                    profile.id, profile.current_revision
                ))
            })?;
        status_from_records(runtime, &workspace, profile, revision)
    }

    pub(crate) fn update(
        runtime: &DaemonRuntime,
        request: ProfileUpdateRequest,
    ) -> Result<ProfileRevisionRecord, ProfileRegistryError> {
        ActorId::new(request.actor.clone())
            .map_err(|error| ProfileRegistryError::Invalid(error.to_string()))?;
        if request.actor.len() > MAX_PROFILE_REVISION_ACTOR_BYTES {
            return Err(ProfileRegistryError::Invalid(format!(
                "revision actor exceeds {MAX_PROFILE_REVISION_ACTOR_BYTES} bytes"
            )));
        }
        request.content.validate()?;
        let mut store = runtime.paths.server_store()?;
        store
            .update_profile_content(
                &request.profile_id,
                &request.actor,
                now_ms(),
                request.content,
            )?
            .ok_or_else(|| {
                ProfileRegistryError::ProfileNotFound(request.profile_id.as_str().into())
            })
    }

    fn status_from_records(
        runtime: &DaemonRuntime,
        workspace: &crate::server_store::WorkspaceRecord,
        profile: ProfileRecord,
        revision: ProfileRevisionRecord,
    ) -> Result<ProfileStatus, ProfileRegistryError> {
        Ok(ProfileStatus {
            summary: summary(runtime, workspace, profile)?,
            revision,
        })
    }

    fn summary(
        runtime: &DaemonRuntime,
        workspace: &crate::server_store::WorkspaceRecord,
        profile: ProfileRecord,
    ) -> Result<ProfileSummary, ProfileRegistryError> {
        let home_path = crate::paths::profile_home_path(
            &runtime.paths.server_db_path,
            &profile.workspace_id,
            &profile.id,
        )?;
        Ok(ProfileSummary {
            profile,
            workspace_health: workspace.health(),
            home_path,
        })
    }
}

#[cfg(test)]
pub(in crate::daemon::handlers) mod tests {
    use super::profiles::{
        ProfileCreateRequest, ProfileListRequest, ProfileRegistryError, ProfileUpdateRequest,
    };
    use super::*;

    use serde_json::json;

    use std::sync::{Arc, Barrier};

    use crate::daemon::handlers::{
        handle_line, handle_request,
        runs::tests::response_result,
        threads::tests::{bare_thread_test_runtime, thread_test_runtime},
    };
    use crate::daemon::protocol::ThreadApprovalPolicy;
    use crate::server_store::{
        MAX_PROFILE_LIST_ENTRIES, ProfileRevisionContent, ProfileValidationError,
    };
    use std::path::Path;
    pub(in crate::daemon::handlers) fn workspace_request(
        id: &str,
        method: &str,
        params: serde_json::Value,
    ) -> Envelope {
        serde_json::from_value(json!({
            "v": 1,
            "id": id,
            "kind": "request",
            "method": method,
            "params": params,
        }))
        .unwrap()
    }

    fn handle_workspace_line(
        runtime: &DaemonRuntime,
        id: &str,
        method: &str,
        params: serde_json::Value,
    ) -> Envelope {
        handle_line(
            runtime,
            &json!({
                "v": 1,
                "id": id,
                "kind": "request",
                "method": method,
                "params": params,
            })
            .to_string(),
        )
    }

    #[test]
    fn workspace_is_created_listed_inspected_and_stays_itself_when_moved_or_broken() {
        let (root, runtime) = bare_thread_test_runtime();
        let first = root.path().join("first");
        std::fs::create_dir(&first).unwrap();

        let created = handle_request(
            &runtime,
            workspace_request(
                "c1",
                "workspace.create",
                json!({"name": "alpha", "root": first.to_string_lossy()}),
            ),
        );
        assert_eq!(
            created.kind,
            crate::daemon::protocol::EnvelopeKind::Response
        );
        let created: WorkspaceCreateResult = response_result(&created);
        let id = created.workspace.id.clone();
        assert!(id.starts_with("ws-"), "id should be minted, got {id}");
        assert_eq!(created.workspace.name, "alpha");
        assert_eq!(created.workspace.health, WorkspaceHealthName::Present);
        assert_eq!(
            Path::new(&created.workspace.ledger_path),
            runtime
                .paths
                .server_db_path
                .parent()
                .unwrap()
                .join("workspaces")
                .join(&id)
                .join("ledger.db")
        );

        // The same name twice is refused rather than silently duplicated.
        let duplicate = handle_request(
            &runtime,
            workspace_request(
                "c2",
                "workspace.create",
                json!({"name": "alpha", "root": first.to_string_lossy()}),
            ),
        );
        assert_eq!(duplicate.kind, crate::daemon::protocol::EnvelopeKind::Error);

        let duplicate_root = handle_request(
            &runtime,
            workspace_request(
                "c2-root",
                "workspace.create",
                json!({"name": "beta", "root": first.to_string_lossy()}),
            ),
        );
        let duplicate_root = duplicate_root.error.unwrap();
        assert_eq!(duplicate_root.code, ERROR_MALFORMED_REQUEST);
        assert!(
            duplicate_root
                .message
                .contains("already registered as alpha")
        );

        // A root that is not a directory is refused, never registered broken.
        let missing = handle_request(
            &runtime,
            workspace_request(
                "c3",
                "workspace.create",
                json!({"name": "ghost", "root": root.path().join("nope").to_string_lossy()}),
            ),
        );
        assert_eq!(missing.kind, crate::daemon::protocol::EnvelopeKind::Error);

        let listed = handle_request(
            &runtime,
            workspace_request("l1", "workspace.list", json!({})),
        );
        let listed: WorkspaceListResult = response_result(&listed);
        assert_eq!(
            listed
                .workspaces
                .iter()
                .map(|workspace| workspace.name.as_str())
                .collect::<Vec<_>>(),
            ["alpha"]
        );

        let status = handle_request(
            &runtime,
            workspace_request("s1", "workspace.status", json!({"workspace_id": id})),
        );
        let status: WorkspaceStatusResult = response_result(&status);
        assert_eq!(status.workspace, created.workspace);

        // Moving the directory keeps the identity: a relocation, not a new
        // workspace and not a reset history.
        let second = root.path().join("second");
        std::fs::rename(&first, &second).unwrap();
        let store = runtime.paths.server_store().unwrap();
        assert!(
            store
                .relocate_workspace(&id, &second.to_string_lossy())
                .unwrap()
        );
        drop(store);
        let moved = handle_request(
            &runtime,
            workspace_request("s2", "workspace.status", json!({"workspace_id": id})),
        );
        let moved: WorkspaceStatusResult = response_result(&moved);
        assert_eq!(moved.workspace.id, id);
        assert_eq!(moved.workspace.ledger_path, created.workspace.ledger_path);
        assert_eq!(
            moved.workspace.created_at_ms,
            created.workspace.created_at_ms
        );
        assert_eq!(moved.workspace.health, WorkspaceHealthName::Present);

        // A vanished directory is reported broken, never omitted.
        std::fs::remove_dir_all(&second).unwrap();
        let broken = handle_request(
            &runtime,
            workspace_request("l2", "workspace.list", json!({})),
        );
        let broken: WorkspaceListResult = response_result(&broken);
        assert_eq!(broken.workspaces.len(), 1);
        assert_eq!(broken.workspaces[0].health, WorkspaceHealthName::Broken);
        assert_eq!(broken.workspaces[0].id, id);
    }

    #[test]
    fn concurrent_workspace_create_returns_one_typed_duplicate_without_a_second_row() {
        let (root, runtime) = bare_thread_test_runtime();
        let workspace_root = root.path().join("concurrent-workspace");
        std::fs::create_dir(&workspace_root).unwrap();
        drop(runtime.paths.server_store().unwrap());
        let barrier = Arc::new(Barrier::new(3));
        let handles =
            [("alpha", "create-alpha"), ("beta", "create-beta")].map(|(name, request_id)| {
                let runtime = runtime.clone();
                let workspace_root = workspace_root.clone();
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    handle_request(
                        &runtime,
                        workspace_request(
                            request_id,
                            "workspace.create",
                            json!({"name": name, "root": workspace_root.to_string_lossy()}),
                        ),
                    )
                })
            });
        barrier.wait();
        let responses: Vec<_> = handles
            .map(|handle| handle.join().unwrap())
            .into_iter()
            .collect();

        assert_eq!(
            responses
                .iter()
                .filter(|response| {
                    response.kind == crate::daemon::protocol::EnvelopeKind::Response
                })
                .count(),
            1
        );
        let errors: Vec<_> = responses
            .iter()
            .filter_map(|response| response.error.as_ref())
            .collect();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].code, ERROR_MALFORMED_REQUEST);
        assert!(
            errors[0]
                .message
                .contains("workspace root is already registered")
        );
        assert_eq!(
            runtime
                .paths
                .server_store()
                .unwrap()
                .workspaces()
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn agent_create_list_and_status_are_typed_and_fail_before_partial_rows() {
        let (root, runtime) = bare_thread_test_runtime();
        let workspace_root = root.path().join("agent-workspace");
        std::fs::create_dir(&workspace_root).unwrap();
        let created_workspace = handle_request(
            &runtime,
            workspace_request(
                "wc",
                "workspace.create",
                json!({"name": "alpha", "root": workspace_root.to_string_lossy()}),
            ),
        );
        let workspace: WorkspaceCreateResult = response_result(&created_workspace);
        let workspace_id = workspace.workspace.id;
        let create_params = json!({
            "agent_id": "builder",
            "workspace_id": workspace_id,
            "model": "gpt-5.6-sol",
            "reasoning_effort": "xhigh",
            "approval_policy": "prompt",
            "toolset": ["file.read", "file.write"]
        });

        let created = handle_request(
            &runtime,
            workspace_request("ac", "agent.create", create_params.clone()),
        );
        let created: AgentCreateResult = response_result(&created);
        assert_eq!(created.agent.id.as_str(), "builder");
        assert_eq!(created.agent.workspace_id, workspace_id);
        assert_eq!(created.agent.model, "gpt-5.6-sol");
        assert_eq!(
            created.agent.reasoning_effort,
            crate::daemon::protocol::ReasoningEffort::Xhigh
        );
        assert_eq!(created.agent.approval_policy, ThreadApprovalPolicy::Prompt);
        assert_eq!(created.agent.toolset, ["file.read", "file.write"]);

        let listed = handle_request(&runtime, workspace_request("al", "agent.list", json!({})));
        let listed: AgentListResult = response_result(&listed);
        assert_eq!(listed.agents, [created.agent.clone()]);
        let status = handle_request(
            &runtime,
            workspace_request("as", "agent.status", json!({"agent_id": "builder"})),
        );
        let status: AgentStatusResult = response_result(&status);
        assert_eq!(status.agent, created.agent);

        let duplicate = handle_request(
            &runtime,
            workspace_request("dup", "agent.create", create_params),
        );
        assert_eq!(duplicate.error.unwrap().code, ERROR_MALFORMED_REQUEST);
        let invalid_id = handle_workspace_line(
            &runtime,
            "bad-id",
            "agent.create",
            json!({
                "agent_id": " ", "workspace_id": workspace_id,
                "model": "gpt-5.6-sol", "reasoning_effort": "none",
                "approval_policy": "prompt", "toolset": ["file.read"]
            }),
        );
        assert_eq!(invalid_id.error.unwrap().code, ERROR_MALFORMED_REQUEST);
        let invalid_tool = handle_request(
            &runtime,
            workspace_request(
                "bad-tool",
                "agent.create",
                json!({
                    "agent_id": "bad-tool", "workspace_id": workspace_id,
                    "model": "gpt-5.6-sol", "reasoning_effort": "none",
                    "approval_policy": "prompt", "toolset": ["credential.store"]
                }),
            ),
        );
        assert_eq!(invalid_tool.error.unwrap().code, ERROR_MALFORMED_REQUEST);
        let missing_workspace = handle_request(
            &runtime,
            workspace_request(
                "missing-workspace",
                "agent.create",
                json!({
                    "agent_id": "orphan", "workspace_id": "ws-missing",
                    "model": "gpt-5.6-sol", "reasoning_effort": "none",
                    "approval_policy": "prompt", "toolset": ["file.read"]
                }),
            ),
        );
        assert_eq!(missing_workspace.error.unwrap().code, ERROR_NOT_FOUND);
        let unknown_field = handle_workspace_line(
            &runtime,
            "unknown",
            "agent.create",
            json!({
                "agent_id": "unknown", "workspace_id": workspace_id,
                "model": "gpt-5.6-sol", "reasoning_effort": "none",
                "approval_policy": "prompt", "toolset": ["file.read"],
                "credential": "must-not-land"
            }),
        );
        assert_eq!(unknown_field.error.unwrap().code, ERROR_MALFORMED_REQUEST);
        let missing_agent = handle_request(
            &runtime,
            workspace_request(
                "missing-agent",
                "agent.status",
                json!({"agent_id": "missing"}),
            ),
        );
        assert_eq!(missing_agent.error.unwrap().code, ERROR_NOT_FOUND);

        std::fs::remove_dir_all(&workspace_root).unwrap();
        let broken_workspace = handle_request(
            &runtime,
            workspace_request(
                "broken",
                "agent.create",
                json!({
                    "agent_id": "broken", "workspace_id": workspace_id,
                    "model": "gpt-5.6-sol", "reasoning_effort": "none",
                    "approval_policy": "prompt", "toolset": ["file.read"]
                }),
            ),
        );
        assert_eq!(broken_workspace.error.unwrap().code, ERROR_WORKSPACE_BROKEN);

        let listed = handle_request(&runtime, workspace_request("al2", "agent.list", json!({})));
        let listed: AgentListResult = response_result(&listed);
        assert_eq!(listed.agents.len(), 1);
    }

    fn profile_create_request(workspace_id: &str, display_name: &str) -> ProfileCreateRequest {
        ProfileCreateRequest {
            workspace_id: workspace_id.into(),
            display_name: display_name.into(),
            model: "gpt-5.6-sol".into(),
            reasoning_effort: crate::daemon::protocol::ReasoningEffort::Xhigh,
            approval_policy: ThreadApprovalPolicy::Prompt,
            toolset: vec!["file.read".into()],
            content: ProfileRevisionContent {
                instructions_markdown: "Build carefully.".into(),
                memory_markdown: "No secrets.".into(),
                skill_refs: vec!["skill:review".into()],
            },
            actor: "local-operator".into(),
        }
    }

    #[test]
    fn profile_create_list_status_survive_move_and_report_broken_workspace() {
        let (root, runtime) = bare_thread_test_runtime();
        let first = root.path().join("profile-workspace");
        std::fs::create_dir(&first).unwrap();
        let workspace = handle_request(
            &runtime,
            workspace_request(
                "profile-ws",
                "workspace.create",
                json!({"name": "profiles", "root": first.to_string_lossy()}),
            ),
        );
        let workspace: WorkspaceCreateResult = response_result(&workspace);
        let workspace_id = workspace.workspace.id;

        let request = profile_create_request(&workspace_id, "builder");
        let created = profiles::create(&runtime, request.clone()).unwrap();
        assert_eq!(created.summary.profile.display_name, "builder");
        assert_eq!(created.summary.profile.home_thread_id, None);
        assert_eq!(created.revision.revision, 1);
        assert_eq!(created.revision.content, request.content);
        assert!(created.summary.home_path.is_dir());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&created.summary.home_path)
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
        }

        assert!(matches!(
            profiles::create(&runtime, request),
            Err(ProfileRegistryError::DuplicateName { .. })
        ));
        assert!(matches!(
            profiles::create(&runtime, profile_create_request("ws-missing", "orphan")),
            Err(ProfileRegistryError::WorkspaceNotFound(_))
        ));
        let listed = profiles::list(
            &runtime,
            ProfileListRequest {
                workspace_id: Some(workspace_id.clone()),
                limit: 1,
            },
        )
        .unwrap();
        assert_eq!(listed.profiles.len(), 1);
        assert!(!listed.truncated);
        assert_eq!(
            profiles::status(&runtime, &created.summary.profile.id).unwrap(),
            created.clone()
        );

        let second = root.path().join("profile-workspace-moved");
        std::fs::rename(&first, &second).unwrap();
        assert!(
            runtime
                .paths
                .server_store()
                .unwrap()
                .relocate_workspace(&workspace_id, &second.to_string_lossy())
                .unwrap()
        );
        let moved = profiles::status(&runtime, &created.summary.profile.id).unwrap();
        assert_eq!(moved.summary.home_path, created.summary.home_path);
        assert_eq!(
            moved.summary.workspace_health,
            crate::server_store::WorkspaceHealth::Present
        );

        std::fs::remove_dir_all(second).unwrap();
        let broken = profiles::list(&runtime, ProfileListRequest::default()).unwrap();
        assert_eq!(broken.profiles.len(), 1);
        assert_eq!(
            broken.profiles[0].workspace_health,
            crate::server_store::WorkspaceHealth::Broken
        );
        assert!(matches!(
            profiles::create(&runtime, profile_create_request(&workspace_id, "blocked")),
            Err(ProfileRegistryError::WorkspaceBroken(_))
        ));
        assert!(matches!(
            profiles::list(
                &runtime,
                ProfileListRequest {
                    workspace_id: None,
                    limit: MAX_PROFILE_LIST_ENTRIES + 1,
                }
            ),
            Err(ProfileRegistryError::Invalid(_))
        ));
    }

    #[test]
    fn profile_update_appends_parented_hash_verified_revisions() {
        let (root, runtime) = bare_thread_test_runtime();
        let workspace_root = root.path().join("revision-workspace");
        std::fs::create_dir(&workspace_root).unwrap();
        let workspace = handle_request(
            &runtime,
            workspace_request(
                "revision-ws",
                "workspace.create",
                json!({"name": "revisions", "root": workspace_root.to_string_lossy()}),
            ),
        );
        let workspace: WorkspaceCreateResult = response_result(&workspace);
        let created = profiles::create(
            &runtime,
            profile_create_request(&workspace.workspace.id, "builder"),
        )
        .unwrap();
        let profile_id = created.summary.profile.id;
        let second_content = ProfileRevisionContent {
            instructions_markdown: "Second instructions.".into(),
            memory_markdown: "Second memory.".into(),
            skill_refs: vec!["skill:review-v2".into()],
        };
        let second = profiles::update(
            &runtime,
            ProfileUpdateRequest {
                profile_id: profile_id.clone(),
                content: second_content.clone(),
                actor: "operator-two".into(),
            },
        )
        .unwrap();
        assert_eq!(second.revision, 2);
        assert_eq!(second.parent_revision, Some(1));
        assert_eq!(second.actor, "operator-two");
        assert_eq!(second.content_hash, second_content.content_hash().unwrap());

        let third_content = ProfileRevisionContent {
            instructions_markdown: "Third instructions.".into(),
            memory_markdown: "Third memory.".into(),
            skill_refs: vec![],
        };
        let third = profiles::update(
            &runtime,
            ProfileUpdateRequest {
                profile_id: profile_id.clone(),
                content: third_content.clone(),
                actor: "operator-three".into(),
            },
        )
        .unwrap();
        assert_eq!(third.revision, 3);
        assert_eq!(third.parent_revision, Some(2));
        assert_eq!(third.content_hash, third_content.content_hash().unwrap());
        assert!(matches!(
            profiles::update(
                &runtime,
                ProfileUpdateRequest {
                    profile_id: profile_id.clone(),
                    content: ProfileRevisionContent::empty(),
                    actor: "a".repeat(crate::server_store::MAX_PROFILE_REVISION_ACTOR_BYTES + 1,),
                },
            ),
            Err(ProfileRegistryError::Invalid(_))
        ));

        let status = profiles::status(&runtime, &profile_id).unwrap();
        assert_eq!(status.summary.profile.current_revision, 3);
        assert_eq!(status.revision, third);
        let store = runtime.paths.server_store().unwrap();
        assert_eq!(
            store.profile_revision(&profile_id, 2).unwrap().unwrap(),
            second
        );
        assert_eq!(
            store
                .profile_revisions(&profile_id, 0, 4)
                .unwrap()
                .iter()
                .map(|revision| revision.revision)
                .collect::<Vec<_>>(),
            [1, 2, 3]
        );
        drop(store);
        let connection = rusqlite::Connection::open(&runtime.paths.server_db_path).unwrap();
        connection
            .execute_batch("DROP TRIGGER profile_revisions_no_update")
            .unwrap();
        connection
            .execute(
                "UPDATE profile_revisions SET memory_markdown = 'tampered'\n                 WHERE profile_id = ?1 AND revision = 2",
                [profile_id.as_str()],
            )
            .unwrap();
        drop(connection);
        let error = runtime
            .paths
            .server_store()
            .unwrap()
            .profile_revision(&profile_id, 2)
            .unwrap_err();
        assert!(error.to_string().contains("content hash mismatch"));
    }

    #[test]
    fn profile_home_and_database_roll_back_at_both_failure_boundaries() {
        let (root, runtime) = bare_thread_test_runtime();
        let workspace_root = root.path().join("rollback-workspace");
        std::fs::create_dir(&workspace_root).unwrap();
        let workspace = handle_request(
            &runtime,
            workspace_request(
                "rollback-ws",
                "workspace.create",
                json!({"name": "rollback", "root": workspace_root.to_string_lossy()}),
            ),
        );
        let workspace: WorkspaceCreateResult = response_result(&workspace);
        let workspace_id = workspace.workspace.id;
        let connection = rusqlite::Connection::open(&runtime.paths.server_db_path).unwrap();
        connection
            .execute_batch(
                "CREATE TRIGGER fail_profile_revision_insert
                 BEFORE INSERT ON profile_revisions
                 BEGIN
                   SELECT RAISE(FAIL, 'injected profile revision insert failure');
                 END;",
            )
            .unwrap();
        drop(connection);

        assert!(matches!(
            profiles::create(
                &runtime,
                profile_create_request(&workspace_id, "database-failure")
            ),
            Err(ProfileRegistryError::Store(_))
        ));
        let profiles_root = runtime
            .paths
            .server_db_path
            .parent()
            .unwrap()
            .join("workspaces")
            .join(&workspace_id)
            .join("profiles");
        assert_eq!(std::fs::read_dir(&profiles_root).unwrap().count(), 0);
        let connection = rusqlite::Connection::open(&runtime.paths.server_db_path).unwrap();
        connection
            .execute_batch("DROP TRIGGER fail_profile_revision_insert")
            .unwrap();
        drop(connection);
        std::fs::remove_dir(&profiles_root).unwrap();
        std::fs::write(&profiles_root, b"not a directory").unwrap();

        assert!(matches!(
            profiles::create(
                &runtime,
                profile_create_request(&workspace_id, "filesystem-failure")
            ),
            Err(ProfileRegistryError::Store(_))
        ));
        assert!(
            runtime
                .paths
                .server_store()
                .unwrap()
                .profiles(None, 2)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn profile_revision_limits_reject_each_oversized_shape() {
        let valid = ProfileRevisionContent {
            instructions_markdown: "i".repeat(128 * 1024),
            memory_markdown: "m".repeat(128 * 1024),
            skill_refs: vec!["skill".into(); 64],
        };
        valid.validate().unwrap();

        let cases = [
            (
                ProfileRevisionContent {
                    instructions_markdown: "i".repeat(128 * 1024 + 1),
                    ..valid.clone()
                },
                ProfileValidationError::InstructionsTooLarge,
            ),
            (
                ProfileRevisionContent {
                    memory_markdown: "m".repeat(128 * 1024 + 1),
                    ..valid.clone()
                },
                ProfileValidationError::MemoryTooLarge,
            ),
            (
                ProfileRevisionContent {
                    skill_refs: vec!["skill".into(); 65],
                    ..valid.clone()
                },
                ProfileValidationError::TooManySkillRefs,
            ),
            (
                ProfileRevisionContent {
                    skill_refs: vec!["x".repeat(5_000); 64],
                    ..valid
                },
                ProfileValidationError::RevisionTooLarge,
            ),
        ];
        for (content, expected) in cases {
            assert_eq!(content.validate().unwrap_err(), expected);
        }
    }

    /// Unknown fields are rejected on the way in, not ignored.
    #[test]
    fn workspace_params_reject_unknown_fields() {
        let (root, runtime) = thread_test_runtime();
        let dir = root.path().join("ws");
        std::fs::create_dir(&dir).unwrap();
        let response = handle_workspace_line(
            &runtime,
            "bad",
            "workspace.create",
            json!({"name": "a", "root": dir.to_string_lossy(), "extra": true}),
        );
        assert_eq!(response.kind, crate::daemon::protocol::EnvelopeKind::Error);
        assert_eq!(response.error.unwrap().code, ERROR_MALFORMED_REQUEST);
    }
}
