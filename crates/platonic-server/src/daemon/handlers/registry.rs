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

#[cfg(test)]
pub(in crate::daemon::handlers) mod tests {
    use super::*;

    use serde_json::json;

    use std::sync::{Arc, Barrier};

    use crate::daemon::handlers::{
        handle_line, handle_request,
        runs::tests::response_result,
        threads::tests::{bare_thread_test_runtime, thread_test_runtime},
    };
    use crate::daemon::protocol::ThreadApprovalPolicy;
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
