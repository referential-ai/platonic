use platonic_client::{ClientError, client::DaemonClient};
use platonic_core::{AgentId, EffectClass, HarnessEvent};
#[cfg(target_os = "linux")]
use platonic_protocol::RunStateName;
use platonic_protocol::{
    BufferedThreadEvent, CAPABILITY_AGENT_CREATE, CAPABILITY_AGENT_LIST, CAPABILITY_AGENT_STATUS,
    CAPABILITY_THREAD_AUTHORITY, CAPABILITY_THREAD_EVENTS, CAPABILITY_THREAD_LIST,
    CAPABILITY_THREAD_SEND, CAPABILITY_THREAD_SPAWN, CAPABILITY_THREAD_STATUS,
    CAPABILITY_THREAD_STOP, CAPABILITY_WORKSPACE_CREATE, CAPABILITY_WORKSPACE_LIST,
    CAPABILITY_WORKSPACE_STATUS, ERROR_NOT_FOUND, ERROR_WORKSPACE_UNREGISTERED, ReasoningEffort,
    ShutdownIfIdleResultName, StreamEvent, ThreadApprovalPolicy, ThreadSendRejectedReason,
    ThreadSendResult, ThreadSpawnDecision, ThreadSpawnResult, ThreadStopResult,
};
use rusqlite::{Connection, OpenFlags, params};
use serde_json::{Value, json};
#[cfg(target_os = "linux")]
use std::collections::HashSet;
use std::{
    fs,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{Arc, Mutex, mpsc},
    thread,
    time::{Duration, Instant},
};

const API_KEY_ENV: &str = "PLATO_SEMANTIC_CONFORMANCE_TEST_KEY";
const SERVED_MODEL: &str = "provider/test-model-2026-08-01";
const PROOF_TIMEOUT: Duration = Duration::from_secs(15);
const POLL_INTERVAL: Duration = Duration::from_millis(10);
static SCENARIO_SERIAL: Mutex<()> = Mutex::new(());

#[cfg(target_os = "linux")]
#[test]
fn killed_wedged_child_has_no_ledger_handle_and_other_run_stays_healthy() {
    let _serial = SCENARIO_SERIAL.lock().unwrap();
    let proof = ProofContext::new();
    let provider = KillIsolationProvider::start();
    write_provider_config(&proof.config_path, &provider.base_url);
    let daemon = ProofDaemon::start(&proof);
    let daemon_pid = daemon.pid();
    let mut first_client = daemon.connect();
    let first = first_client
        .run_start(
            "wedge the first child".into(),
            Some("plato.toml".into()),
            false,
        )
        .unwrap();
    provider
        .first_requested
        .recv_timeout(PROOF_TIMEOUT)
        .unwrap();
    let first_children = platform_direct_children(daemon_pid);
    assert_eq!(
        first_children.len(),
        1,
        "first supervised child was not unique"
    );
    let first_child = *first_children.iter().next().unwrap();

    let mut second_client = daemon.connect();
    let second = second_client
        .run_start(
            "keep the second child healthy".into(),
            Some("plato.toml".into()),
            false,
        )
        .unwrap();
    provider
        .second_requested
        .recv_timeout(PROOF_TIMEOUT)
        .unwrap();
    let all_children = platform_direct_children(daemon_pid);
    let second_child = *all_children
        .difference(&first_children)
        .next()
        .expect("second supervised child did not appear");
    assert_eq!(all_children.len(), 2);

    #[cfg(target_os = "linux")]
    {
        let ledger_path = proof.ledger_path();
        assert!(linux_process_has_fd(daemon_pid, &ledger_path));
        assert!(!linux_process_has_fd(first_child, &ledger_path));
        assert!(!linux_process_has_fd(second_child, &ledger_path));
    }

    kill_process_exact(first_child);
    provider.release_second.send(()).unwrap();

    assert_eq!(
        wait_for_terminal_status(&mut first_client, &first.run_id),
        RunStateName::Failed
    );
    assert_eq!(
        wait_for_terminal_status(&mut second_client, &second.run_id),
        RunStateName::Finished
    );
    assert_eq!(
        second_client
            .transcript_read(&second.run_id)
            .unwrap()
            .final_answer
            .as_deref(),
        Some("the second child stayed healthy")
    );
    assert_eq!(daemon.pid(), daemon_pid);
    second_client.hello(&proof.workspace).unwrap();
    wait_for_platform_process_absence(first_child);
    wait_for_platform_process_absence(second_child);

    drop(second_client);
    provider.join();
    daemon.stop(first_client);
}

#[test]
#[cfg_attr(target_os = "macos", ignore = "EINVAL on macOS; #463")]
fn one_host_daemon_serves_two_workspaces() {
    let _serial = SCENARIO_SERIAL.lock().unwrap();
    let root = Arc::new(tempfile::tempdir().unwrap());
    let first = ProofContext::in_root(Arc::clone(&root), "workspace-a");
    let second = ProofContext::in_root(root, "workspace-b");
    let host = ProofDaemon::start(&first);
    let host_pid = host.pid();
    assert_eq!(host.socket_path, first.host_socket_path());

    let mut first_client = host.connect();
    let first_hello = first_client.hello(&first.workspace).unwrap();
    assert_eq!(Path::new(&first_hello.ledger_path), first.ledger_path());

    let mut second_client = host.connect_workspace(&second.workspace);
    let second_hello = second_client.hello(&second.workspace).unwrap();
    assert_eq!(Path::new(&second_hello.ledger_path), second.ledger_path());
    let roots = second_client
        .workspace_list()
        .unwrap()
        .workspaces
        .into_iter()
        .map(|workspace| workspace.root)
        .collect::<Vec<_>>();
    assert!(roots.contains(&first.workspace.to_string_lossy().into_owned()));
    assert!(roots.contains(&second.workspace.to_string_lossy().into_owned()));
    assert_eq!(host.pid(), host_pid);

    drop(second_client);
    host.stop(first_client);
}
#[test]
fn workspace_and_agent_six_method_control_plane_is_semantically_conformant() {
    let _serial = SCENARIO_SERIAL.lock().unwrap();
    let proof = ProofContext::new();
    let host = ProofDaemon::start(&proof);
    let mut client = host.connect();
    let hello = client.hello(&proof.workspace).unwrap();
    for capability in [
        CAPABILITY_WORKSPACE_CREATE,
        CAPABILITY_WORKSPACE_LIST,
        CAPABILITY_WORKSPACE_STATUS,
        CAPABILITY_AGENT_CREATE,
        CAPABILITY_AGENT_LIST,
        CAPABILITY_AGENT_STATUS,
    ] {
        assert!(hello.capabilities.contains(&capability));
    }

    let second = proof._root.path().join("agent-control-workspace");
    fs::create_dir(&second).unwrap();
    let created = client
        .workspace_create("agent-control".into(), second.clone())
        .unwrap()
        .workspace;
    assert_eq!(Path::new(&created.root), second);
    let listed = client.workspace_list().unwrap().workspaces;
    assert!(listed.iter().any(|workspace| workspace.id == created.id));
    assert_eq!(
        client
            .workspace_status(created.id.clone())
            .unwrap()
            .workspace,
        created
    );

    let created_agent = client
        .agent_create(
            AgentId::new("builder").unwrap(),
            created.id.clone(),
            "gpt-5.6-sol".into(),
            ReasoningEffort::High,
            ThreadApprovalPolicy::Prompt,
            vec!["file.read".into(), "file.write".into()],
        )
        .unwrap()
        .agent;
    assert_eq!(created_agent.workspace_id, created.id);
    assert_eq!(created_agent.toolset, ["file.read", "file.write"]);
    assert_eq!(client.agent_list().unwrap().agents, [created_agent.clone()]);
    assert_eq!(
        client
            .agent_status(AgentId::new("builder").unwrap())
            .unwrap()
            .agent,
        created_agent
    );
    host.stop(client);
}

#[test]
fn headless_one_shot_and_remote_never_prompt_or_register() {
    let _serial = SCENARIO_SERIAL.lock().unwrap();
    let proof = ProofContext::new();
    let socket_path = proof.host_socket_path();
    let mut child = proof.daemon_command().spawn().unwrap();
    wait_for_endpoint(&socket_path, &mut child);

    for args in [vec!["hello"], vec!["--remote", "thread_missing"]] {
        let output = proof
            .plato_command()
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .unwrap();
        assert!(!output.status.success());
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(
            stderr.contains(ERROR_WORKSPACE_UNREGISTERED.as_str()),
            "{stderr}"
        );
        assert!(!stderr.contains("Workspace name ["), "{stderr}");
    }

    let mut control =
        DaemonClient::connect_with_timeout(&socket_path, Duration::from_secs(2)).unwrap();
    assert!(control.workspace_list().unwrap().workspaces.is_empty());
    drop(control);
    child.kill().unwrap();
    child.wait().unwrap();
}

#[test]
#[cfg_attr(target_os = "macos", ignore = "EINVAL on macOS; #463")]
fn thread_spawn_list_and_status_are_semantically_conformant_on_host_daemon() {
    let _serial = SCENARIO_SERIAL.lock().unwrap();
    let proof = ProofContext::new();
    fs::write(
        &proof.config_path,
        "[tools]\nenabled = [\"file.read\", \"file.list\", \"file.write\", \"file.edit\", \"shell.exec\", \"web.fetch\"]\n",
    )
    .unwrap();
    let host = ProofDaemon::start(&proof);
    let mut client = host.connect();
    let hello = client.hello(&proof.workspace).unwrap();
    for capability in [
        CAPABILITY_THREAD_SPAWN,
        CAPABILITY_THREAD_LIST,
        CAPABILITY_THREAD_STATUS,
        CAPABILITY_THREAD_AUTHORITY,
        CAPABILITY_THREAD_STOP,
    ] {
        assert!(hello.capabilities.contains(&capability));
    }

    let (spawn_id, root_thread_id) = match client
        .thread_spawn_start(
            None,
            proof
                .workspace
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .into_owned(),
            "gpt-5.6-sol".into(),
            ReasoningEffort::Xhigh,
            ThreadApprovalPolicy::Yolo,
        )
        .unwrap()
    {
        ThreadSpawnResult::ApprovalRequired {
            spawn_id,
            thread_id,
            effect,
            ..
        } => {
            assert_eq!(effect, EffectClass::WorkspaceWrite);
            (spawn_id, thread_id)
        }
        unexpected => panic!("expected root approval, got {unexpected:?}"),
    };
    let root_status = match client
        .thread_spawn_decide(
            spawn_id,
            ThreadSpawnDecision::Grant {
                actor: "semantic_fixture".into(),
            },
        )
        .unwrap()
    {
        ThreadSpawnResult::Spawned { thread } => thread,
        unexpected => panic!("expected spawned root, got {unexpected:?}"),
    };
    assert_eq!(root_status.authority.thread_id, root_thread_id);
    assert_eq!(root_status.authority.parent_thread_id, None);
    assert_eq!(root_status.authority.spawning_actor, "semantic_fixture");
    let root = client
        .thread_authority(root_thread_id.clone())
        .unwrap()
        .authority;
    assert_eq!(root.agent_id, Some(AgentId::new("plato").unwrap()));
    assert_eq!(root.worktrees.len(), 1);
    assert_eq!(
        Path::new(&root_status.authority.cwd),
        Path::new(&root.worktrees[0].path)
    );
    assert_eq!(root.granted_paths.len(), 1);
    assert!(root.granted_paths[0].writable);
    assert_eq!(
        root.toolset,
        [
            "file.read",
            "file.list",
            "file.write",
            "file.edit",
            "shell.exec",
            "web.fetch",
        ]
    );
    assert!(root.network);
    assert_eq!(root.model, "gpt-5.6-sol");
    assert_eq!(root.reasoning_effort, ReasoningEffort::Xhigh);
    assert_eq!(root.approval_policy, ThreadApprovalPolicy::Yolo);
    assert!(root.created_at_ms > 0);
    assert!(root_status.live.loaded);
    assert_eq!(root_status.live.current_turn_id, None);
    assert_eq!(
        root_status.live.last_activity_at_ms,
        Some(root.created_at_ms)
    );
    let child_cwd = PathBuf::from(&root.worktrees[0].path).join("child");
    fs::create_dir(&child_cwd).unwrap();

    let child_status = match client
        .thread_spawn_start(
            Some(root_thread_id.clone()),
            child_cwd
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .into_owned(),
            "gpt-5.6-sol".into(),
            ReasoningEffort::High,
            ThreadApprovalPolicy::Prompt,
        )
        .unwrap()
    {
        ThreadSpawnResult::Spawned { thread } => thread,
        unexpected => panic!("expected yolo auto-grant, got {unexpected:?}"),
    };
    let child_thread_id = child_status.authority.thread_id.clone();
    let child = client
        .thread_authority(child_thread_id.clone())
        .unwrap()
        .authority;
    assert_eq!(child.worktrees.len(), 1);
    assert_eq!(
        Path::new(&child_status.authority.cwd),
        Path::new(&child.worktrees[0].path)
    );
    assert_eq!(
        child.parent_thread_id.as_deref(),
        Some(root_thread_id.as_str())
    );
    assert_eq!(child.spawning_actor, "yolo");
    assert_eq!(child.agent_id, root.agent_id);
    assert_eq!(child.toolset, root.toolset);
    assert!(child_status.live.loaded);
    assert_eq!(
        child_status.live.last_activity_at_ms,
        Some(child.created_at_ms)
    );

    let listed = client.thread_list().unwrap().threads;
    assert_eq!(listed.len(), 2);
    assert!(listed.iter().all(|thread| thread.live.loaded));
    assert_eq!(
        client
            .thread_status(child_thread_id.clone())
            .unwrap()
            .thread,
        child_status
    );
    let root_stop = client
        .thread_stop(root_thread_id.clone(), "semantic_fixture".into())
        .unwrap();
    let root_stopped_at_ms = match root_stop {
        ThreadStopResult::Stopped {
            thread_id,
            stopped_turn_id,
            stopped_at_ms,
        } => {
            assert_eq!(thread_id, root_thread_id);
            assert_eq!(stopped_turn_id, None);
            stopped_at_ms
        }
        unexpected => panic!("expected stopped root, got {unexpected:?}"),
    };
    let listed = client.thread_list().unwrap().threads;
    let stopped_root = listed
        .iter()
        .find(|thread| thread.authority.thread_id == root_thread_id)
        .unwrap();
    let orphaned_child = listed
        .iter()
        .find(|thread| thread.authority.thread_id == child_thread_id)
        .unwrap();
    assert!(!stopped_root.live.loaded);
    assert_eq!(stopped_root.live.last_activity_at_ms, None);
    assert!(orphaned_child.live.loaded);
    assert_eq!(orphaned_child.authority, child_status.authority);
    let connection = rusqlite::Connection::open(&proof.server_db_path).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT actor, stopped_turn_id, occurred_at_ms FROM thread_stops WHERE thread_id = ?1",
                [&root_thread_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?, row.get::<_, i64>(2)?))
            )
            .unwrap(),
        (
            "semantic_fixture".into(),
            None,
            i64::try_from(root_stopped_at_ms).unwrap()
        )
    );
    drop(connection);
    host.stop(client);

    let restarted = ProofDaemon::start(&proof);
    let mut restarted_client = restarted.connect();
    let listed = restarted_client.thread_list().unwrap().threads;
    assert_eq!(listed.len(), 2);
    assert!(listed.iter().all(|thread| !thread.live.loaded));
    assert!(
        listed
            .iter()
            .all(|thread| thread.live.last_activity_at_ms.is_none())
    );
    let readback = restarted_client
        .thread_status(child_thread_id.clone())
        .unwrap()
        .thread;
    assert_eq!(readback.authority, child_status.authority);
    assert!(!readback.live.loaded);
    let child_stop = restarted_client
        .thread_stop(child_thread_id.clone(), "restart_fixture".into())
        .unwrap();
    let child_stopped_at_ms = match child_stop {
        ThreadStopResult::AlreadyStopped {
            stopped_at_ms,
            stopped_turn_id: None,
            ..
        } => stopped_at_ms,
        unexpected => panic!("expected reconciled stopped child, got {unexpected:?}"),
    };
    assert_eq!(
        restarted_client
            .thread_stop(child_thread_id.clone(), "retry_fixture".into())
            .unwrap(),
        ThreadStopResult::AlreadyStopped {
            thread_id: child_thread_id,
            stopped_turn_id: None,
            stopped_at_ms: child_stopped_at_ms,
        }
    );
    let stale_parent = restarted_client
        .thread_spawn_start(
            Some(root_thread_id),
            proof.workspace.to_string_lossy().into_owned(),
            "gpt-5.6-sol".into(),
            ReasoningEffort::High,
            ThreadApprovalPolicy::Prompt,
        )
        .unwrap_err();
    assert!(matches!(
        stale_parent,
        ClientError::DaemonResponse(error) if error.code == ERROR_NOT_FOUND
    ));
    restarted.stop(restarted_client);
}

#[test]
#[cfg_attr(target_os = "macos", ignore = "EINVAL on macOS; #463")]
fn coordinator_tool_dispatches_one_bounded_worker_and_reports_its_durable_id() {
    let _serial = SCENARIO_SERIAL.lock().unwrap();
    let proof = ProofContext::new();
    let worker_cwd = Arc::new(Mutex::new(proof.workspace.clone()));
    let provider = CoordinatorProvider::start(Arc::clone(&worker_cwd));
    write_coordinator_config(&proof.config_path, &provider.base_url);
    let host = ProofDaemon::start(&proof);
    let mut client = host.connect();
    client.hello(&proof.workspace).unwrap();
    let workspace_id = client
        .workspace_list()
        .unwrap()
        .workspaces
        .into_iter()
        .find(|workspace| Path::new(&workspace.root) == proof.workspace.canonicalize().unwrap())
        .unwrap()
        .id;
    client
        .agent_create(
            AgentId::new("bounded-worker").unwrap(),
            workspace_id,
            "worker-model".into(),
            ReasoningEffort::High,
            ThreadApprovalPolicy::Prompt,
            vec!["file.read".into()],
        )
        .unwrap();

    let (spawn_id, coordinator_thread_id) = match client
        .thread_spawn_start(
            None,
            proof
                .workspace
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .into_owned(),
            "test-model".into(),
            ReasoningEffort::High,
            ThreadApprovalPolicy::Prompt,
        )
        .unwrap()
    {
        ThreadSpawnResult::ApprovalRequired {
            spawn_id,
            thread_id,
            effect,
            ..
        } => {
            assert_eq!(effect, EffectClass::WorkspaceWrite);
            (spawn_id, thread_id)
        }
        result => panic!("expected coordinator approval, got {result:?}"),
    };
    assert!(matches!(
        client
            .thread_spawn_decide(
                spawn_id,
                ThreadSpawnDecision::Grant {
                    actor: "semantic_fixture".into(),
                },
            )
            .unwrap(),
        ThreadSpawnResult::Spawned { .. }
    ));
    let coordinator = client
        .thread_authority(coordinator_thread_id.clone())
        .unwrap()
        .authority;
    let private_worker_cwd = PathBuf::from(&coordinator.worktrees[0].path).join("worker");
    fs::create_dir(&private_worker_cwd).unwrap();
    *worker_cwd.lock().unwrap() = private_worker_cwd.canonicalize().unwrap();
    assert!(matches!(
        client
            .thread_send(
                coordinator_thread_id.clone(),
                "semantic_controller".into(),
                None,
                "Dispatch exactly one bounded worker and report its durable thread id.".into(),
            )
            .unwrap(),
        ThreadSendResult::Started { .. }
    ));

    let deadline = Instant::now() + PROOF_TIMEOUT;
    let mut offset = 0;
    let mut events = Vec::new();
    let mut approval = None;
    loop {
        let page = client
            .thread_events(coordinator_thread_id.clone(), Some(offset), 128, 1_000)
            .unwrap();
        offset = page.next_offset;
        for event in &page.events {
            if let StreamEvent::ApprovalRequested {
                run_id,
                tool_call_id,
                tool_name,
                effect,
                ..
            } = &event.event
            {
                assert_eq!(tool_name, "thread.spawn");
                assert_eq!(*effect, EffectClass::WorkspaceWrite);
                assert!(
                    approval.is_none(),
                    "coordinator requested duplicate approval"
                );
                approval = Some((run_id.clone(), tool_call_id.clone()));
                client
                    .approval_grant_as(run_id, tool_call_id, "jerome".into())
                    .unwrap();
            }
        }
        let empty = page.events.is_empty();
        events.extend(page.events);
        if approval.is_some() && page.current_turn_id.is_none() && empty {
            break;
        }
        assert!(Instant::now() < deadline, "coordinator turn did not finish");
    }

    let (run_id, tool_call_id) = approval.expect("coordinator did not request spawn approval");
    let spawned_thread_id = events
        .iter()
        .find_map(|event| match &event.event {
            StreamEvent::Ledger { record } => match &record.event {
                HarnessEvent::ToolFinished { result, .. } if result.data["status"] == "spawned" => {
                    result.data["thread_id"].as_str().map(str::to_owned)
                }
                _ => None,
            },
            _ => None,
        })
        .expect("coordinator did not receive a spawned thread result");
    let transcript = client.transcript_read(&run_id).unwrap();
    let expected_answer = format!("Dispatched bounded worker {spawned_thread_id}.");
    assert_eq!(
        transcript.final_answer.as_deref(),
        Some(expected_answer.as_str())
    );

    let worker = client
        .thread_authority(spawned_thread_id.clone())
        .unwrap()
        .authority;
    assert_eq!(
        worker.parent_thread_id.as_deref(),
        Some(coordinator_thread_id.as_str())
    );
    assert_eq!(worker.spawning_actor, "jerome");
    assert_eq!(
        worker.agent_id,
        Some(AgentId::new("bounded-worker").unwrap())
    );
    assert_eq!(worker.toolset, ["file.read"]);
    assert_eq!(worker.approval_policy, ThreadApprovalPolicy::Prompt);
    assert_eq!(worker.worktrees.len(), 1);
    assert_eq!(worker.granted_paths.len(), 1);
    assert!(worker.granted_paths[0].writable);
    assert!(!worker.network);

    let connection = Connection::open(&proof.server_db_path).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT tool_name, effect, decision, decided_by
                   FROM tool_call_approvals WHERE run_id = ?1 AND call_id = ?2",
                params![run_id, tool_call_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .unwrap(),
        (
            "thread.spawn".into(),
            "workspace_write".into(),
            "granted".into(),
            "jerome".into(),
        )
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT decision, actor FROM thread_spawn_approvals WHERE thread_id = ?1",
                [&spawned_thread_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .unwrap(),
        ("granted".into(), "jerome".into())
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM thread_authorities WHERE parent_thread_id = ?1",
                [&coordinator_thread_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
    drop(connection);

    let provider_proof = provider.join();
    assert_eq!(provider_proof.thread_id, spawned_thread_id);
    assert!(
        provider_proof
            .wrapped_result
            .starts_with("<tool_output name=\"thread.spawn\" trust=\"untrusted\">\n")
    );
    assert!(provider_proof.wrapped_result.ends_with("\n</tool_output>"));
    assert_eq!(provider_proof.request_count, 2);
    host.stop(client);
}

#[test]
#[cfg_attr(target_os = "macos", ignore = "EINVAL on macOS; #463")]
fn thread_send_and_three_observers_are_semantically_conformant_on_host_daemon() {
    const INITIAL_MESSAGE: &str = "begin the controlled thread proof";
    const STEERED_MESSAGE: &str = "include the exact steered phrase in the continuation";
    const CONTINUATION_ANSWER: &str = "The continuation used the exact steered phrase.";

    let _serial = SCENARIO_SERIAL.lock().unwrap();
    let proof = ProofContext::new();
    let provider = ControlledThreadProvider::start(CONTINUATION_ANSWER);
    write_provider_config(&proof.config_path, &provider.base_url);
    let host = ProofDaemon::start(&proof);
    let mut controller_a = host.connect();
    let mut controller_b = host.connect();
    let hello = controller_a.hello(&proof.workspace).unwrap();
    for capability in [
        CAPABILITY_THREAD_SEND,
        CAPABILITY_THREAD_EVENTS,
        CAPABILITY_THREAD_SPAWN,
        CAPABILITY_THREAD_LIST,
        CAPABILITY_THREAD_STATUS,
        CAPABILITY_THREAD_AUTHORITY,
    ] {
        assert!(hello.capabilities.contains(&capability));
    }
    let (spawn_id, thread_id) = match controller_a
        .thread_spawn_start(
            None,
            proof
                .workspace
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .into_owned(),
            "test-model".into(),
            ReasoningEffort::High,
            ThreadApprovalPolicy::Yolo,
        )
        .unwrap()
    {
        ThreadSpawnResult::ApprovalRequired {
            spawn_id,
            thread_id,
            ..
        } => (spawn_id, thread_id),
        unexpected => panic!("expected root approval, got {unexpected:?}"),
    };
    assert!(matches!(
        controller_a
            .thread_spawn_decide(
                spawn_id,
                ThreadSpawnDecision::Grant {
                    actor: "semantic_fixture".into(),
                },
            )
            .unwrap(),
        ThreadSpawnResult::Spawned { .. }
    ));

    let mut observers = Vec::new();
    let mut observer_ready = Vec::new();
    for _ in 0..2 {
        let (ready_sender, ready_receiver) = mpsc::channel();
        observers.push(spawn_thread_observer(
            host.socket_path.clone(),
            proof.workspace.clone(),
            thread_id.clone(),
            ready_sender,
        ));
        observer_ready.push(ready_receiver);
    }
    for ready in observer_ready {
        ready.recv_timeout(PROOF_TIMEOUT).unwrap();
    }

    let started = controller_a
        .thread_send(
            thread_id.clone(),
            "controller_a".into(),
            None,
            INITIAL_MESSAGE.into(),
        )
        .unwrap();
    let turn_id = match started {
        ThreadSendResult::Started {
            thread_id: ref receipt_thread,
            ref turn_id,
        } => {
            assert_eq!(receipt_thread, &thread_id);
            turn_id.clone()
        }
        unexpected => panic!("expected started receipt, got {unexpected:?}"),
    };
    let first_request = provider
        .first_request
        .recv_timeout(PROOF_TIMEOUT)
        .expect("first child did not reach the provider");
    assert!(first_request.to_string().contains(INITIAL_MESSAGE));

    assert_controller_owned_rejection(
        &mut controller_b,
        &thread_id,
        &turn_id,
        "controller_b",
        "competing send during child one",
    );
    let steered = controller_a
        .thread_send(
            thread_id.clone(),
            "controller_a".into(),
            Some(turn_id.clone()),
            STEERED_MESSAGE.into(),
        )
        .unwrap();
    assert_eq!(
        steered,
        ThreadSendResult::Steered {
            thread_id: thread_id.clone(),
            turn_id: turn_id.clone(),
        }
    );

    let (ready_sender, ready_receiver) = mpsc::channel();
    observers.push(spawn_thread_observer(
        host.socket_path.clone(),
        proof.workspace.clone(),
        thread_id.clone(),
        ready_sender,
    ));
    ready_receiver.recv_timeout(PROOF_TIMEOUT).unwrap();

    provider.release_first.send(()).unwrap();
    let second_request = loop {
        match provider.second_request.try_recv() {
            Ok(request) => break request,
            Err(mpsc::TryRecvError::Empty) => {
                assert_controller_owned_rejection(
                    &mut controller_b,
                    &thread_id,
                    &turn_id,
                    "controller_b",
                    "competing send across the child boundary",
                );
                assert_eq!(
                    controller_a
                        .thread_status(thread_id.clone())
                        .unwrap()
                        .thread
                        .live
                        .current_turn_id
                        .as_deref(),
                    Some(turn_id.as_str())
                );
                thread::sleep(POLL_INTERVAL);
            }
            Err(mpsc::TryRecvError::Disconnected) => panic!("controlled provider disconnected"),
        }
    };
    assert!(
        second_request.to_string().contains(STEERED_MESSAGE),
        "continuation request did not contain the accepted steer"
    );
    assert_controller_owned_rejection(
        &mut controller_b,
        &thread_id,
        &turn_id,
        "controller_b",
        "competing send during continuation child",
    );
    provider.release_second.send(()).unwrap();
    wait_for_thread_idle(&mut controller_a, &thread_id);

    let streams = observers
        .into_iter()
        .map(|observer| observer.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(streams[0], streams[1]);
    assert_eq!(streams[1], streams[2]);
    assert!(
        streams[0].iter().all(|event| event.turn_id == turn_id),
        "observer stream crossed external turn ids"
    );
    assert_eq!(
        streams[0]
            .iter()
            .filter(|event| matches!(
                &event.event,
                StreamEvent::Ledger { record }
                    if matches!(
                        record.event,
                        HarnessEvent::RunFinished { .. } | HarnessEvent::RunFailed { .. }
                    )
            ))
            .count(),
        2,
        "both child terminal records must remain observer-visible"
    );
    assert!(streams[0].iter().any(|event| matches!(
        &event.event,
        StreamEvent::Ledger { record }
            if matches!(
                &record.event,
                HarnessEvent::ModelResponded { output, .. }
                    if output.content == CONTINUATION_ANSWER
            )
    )));
    assert_eq!(
        streams[0]
            .iter()
            .map(|event| event.offset)
            .collect::<Vec<_>>(),
        (0..streams[0].len() as u64).collect::<Vec<_>>()
    );

    let next_turn = controller_b
        .thread_send(
            thread_id.clone(),
            "controller_b".into(),
            None,
            "start after final idle".into(),
        )
        .unwrap();
    assert!(matches!(
        next_turn,
        ThreadSendResult::Started {
            thread_id: receipt_thread,
            turn_id: next_turn_id,
        } if receipt_thread == thread_id && next_turn_id != turn_id
    ));
    wait_for_thread_idle(&mut controller_b, &thread_id);
    let requests = provider.join();
    assert_eq!(requests.len(), 3);

    drop(controller_b);
    host.stop(controller_a);
}

fn assert_controller_owned_rejection(
    client: &mut DaemonClient,
    thread_id: &str,
    turn_id: &str,
    controller_id: &str,
    message: &str,
) {
    assert_eq!(
        client
            .thread_send(
                thread_id.into(),
                controller_id.into(),
                Some(turn_id.into()),
                message.into(),
            )
            .unwrap(),
        ThreadSendResult::Rejected {
            thread_id: thread_id.into(),
            turn_id: Some(turn_id.into()),
            reason: ThreadSendRejectedReason::ControllerOwned,
        }
    );
}

fn spawn_thread_observer(
    socket_path: PathBuf,
    workspace: PathBuf,
    thread_id: String,
    ready: mpsc::Sender<()>,
) -> thread::JoinHandle<Vec<BufferedThreadEvent>> {
    thread::spawn(move || {
        let mut client =
            DaemonClient::connect_with_timeout(&socket_path, Duration::from_secs(2)).unwrap();
        client.hello(&workspace).unwrap();
        ready.send(()).unwrap();
        let mut offset = 0;
        let mut observed_turn = None;
        let mut events = Vec::new();
        let deadline = Instant::now() + PROOF_TIMEOUT;
        loop {
            let page = client
                .thread_events(thread_id.clone(), Some(offset), 128, 1_000)
                .unwrap();
            offset = page.next_offset;
            if observed_turn.is_none() {
                observed_turn = page
                    .current_turn_id
                    .clone()
                    .or_else(|| page.events.first().map(|event| event.turn_id.clone()));
            }
            let empty = page.events.is_empty();
            events.extend(page.events);
            if observed_turn.is_some()
                && page.current_turn_id.as_ref() != observed_turn.as_ref()
                && empty
            {
                break;
            }
            assert!(Instant::now() < deadline, "thread observer did not detach");
        }
        events
    })
}

fn wait_for_thread_idle(client: &mut DaemonClient, thread_id: &str) {
    let deadline = Instant::now() + PROOF_TIMEOUT;
    loop {
        if client
            .thread_status(thread_id.into())
            .unwrap()
            .thread
            .live
            .current_turn_id
            .is_none()
        {
            return;
        }
        assert!(Instant::now() < deadline, "thread turn did not become idle");
        thread::sleep(POLL_INTERVAL);
    }
}

#[test]
fn accepted_provider_stream_blocks_until_delayed_request_bytes_arrive() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let (accepted_sender, accepted_receiver) = mpsc::sync_channel(0);
    let server = thread::spawn(move || {
        let mut stream = accept_before(&listener, Instant::now() + PROOF_TIMEOUT);
        accepted_sender.send(()).unwrap();
        read_http_request(&mut stream)
    });

    let mut client = TcpStream::connect(address).unwrap();
    accepted_receiver.recv_timeout(PROOF_TIMEOUT).unwrap();
    client
        .write_all(b"POST / HTTP/1.1\r\ncontent-length: 16\r\n\r\n{\"delayed\":true}")
        .unwrap();

    assert_eq!(server.join().unwrap(), json!({"delayed": true}));
}

struct ProofContext {
    _root: Arc<tempfile::TempDir>,
    workspace: PathBuf,
    config_path: PathBuf,
    /// The host's server-wide store, where thread state lives.
    server_db_path: PathBuf,
    #[cfg(unix)]
    runtime_root: PathBuf,
    #[cfg(unix)]
    state_root: PathBuf,
}

#[cfg(unix)]
impl Drop for ProofContext {
    fn drop(&mut self) {
        // The runtime root lives outside the temporary directory and is
        // shared by every context on the same root, so only the last context
        // holding that root removes it.
        if Arc::strong_count(&self._root) == 1 {
            let _ = fs::remove_dir_all(&self.runtime_root);
        }
    }
}

impl ProofContext {
    fn new() -> Self {
        Self::in_root(Arc::new(tempfile::tempdir().unwrap()), "workspace")
    }

    fn in_root(root: Arc<tempfile::TempDir>, workspace_name: &str) -> Self {
        let workspace = root.path().join(workspace_name);
        fs::create_dir(&workspace).unwrap();
        init_git_repository(&workspace);

        #[cfg(unix)]
        let (runtime_root, state_root) = {
            // The runtime root holds sockets, and sockaddr_un caps sun_path at
            // 104 bytes on macOS against 108 on Linux. A runtime root inside
            // the temporary directory overflows that on macOS runners, so the
            // sockets live under a short external root instead -- the same
            // rule AGENTS.md sets for external-daemon proofs. State stays in
            // the temporary directory: only sockets carry the length limit.
            // Keyed to the shared temporary root, not to this context: two
            // contexts in one root must agree on it, because the host daemon
            // serves one socket for both.
            let root_key = {
                use std::hash::{Hash, Hasher};
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                root.path().hash(&mut hasher);
                hasher.finish() as u32
            };
            let runtime_root =
                PathBuf::from(format!("/tmp/pconf-{}-{root_key:08x}", std::process::id()));
            fs::create_dir_all(&runtime_root).unwrap();
            let state_root = root.path().join("state");
            (runtime_root, state_root)
        };
        #[cfg(unix)]
        assert!(
            runtime_root
                .join("platonic/host/agent.sock")
                .as_os_str()
                .len()
                < 100,
            "socket path must stay under the sockaddr_un limit: {}",
            runtime_root.join("platonic/host/agent.sock").display()
        );

        // The server store sits beside the workspaces directory, not inside
        // any one workspace.
        #[cfg(unix)]
        let server_db_path = state_root.join("platonic").join("server.db");

        Self {
            config_path: workspace.join("plato.toml"),
            _root: root,
            workspace,
            server_db_path,
            #[cfg(unix)]
            runtime_root,
            #[cfg(unix)]
            state_root,
        }
    }

    fn ledger_path(&self) -> PathBuf {
        let connection =
            Connection::open_with_flags(&self.server_db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        connection
            .query_row(
                "SELECT ledger_path FROM workspaces WHERE root = ?1",
                params![self.workspace.canonicalize().unwrap().to_string_lossy()],
                |row| row.get::<_, String>(0),
            )
            .map(PathBuf::from)
            .unwrap()
    }

    fn host_socket_path(&self) -> PathBuf {
        #[cfg(unix)]
        {
            self.runtime_root
                .join("platonic")
                .join("host")
                .join("agent.sock")
        }
    }

    fn apply_environment(&self, command: &mut Command) {
        #[cfg(unix)]
        command
            .env("XDG_RUNTIME_DIR", &self.runtime_root)
            .env("XDG_STATE_HOME", &self.state_root);
        command
            .env(API_KEY_ENV, "test-key")
            .env("PLATO_CONFIG", &self.config_path);
    }

    fn plato_command(&self) -> Command {
        let mut command = Command::new(workspace_binary("plato"));
        command.env("PLATONIC_BIN", workspace_binary("platonic"));
        command.current_dir(&self.workspace);
        self.apply_environment(&mut command);
        command
    }

    fn daemon_command(&self) -> Command {
        let mut command = Command::new(workspace_binary("platonic"));
        command
            .arg("serve")
            .current_dir(&self.workspace)
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        self.apply_environment(&mut command);
        command
    }
}

fn init_git_repository(path: &Path) {
    let git = |args: &[&str]| {
        let output = Command::new("git")
            .current_dir(path)
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
    };
    git(&["init", "--quiet", "--initial-branch", "main"]);
    git(&["config", "user.name", "Platonic Test"]);
    git(&["config", "user.email", "platonic@example.invalid"]);
    fs::write(path.join(".gitkeep"), "").unwrap();
    git(&["add", ".gitkeep"]);
    git(&["commit", "--quiet", "-m", "initial"]);
}

fn workspace_binary(name: &str) -> PathBuf {
    std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join(format!("{name}{}", std::env::consts::EXE_SUFFIX))
}

struct ProofDaemon {
    child: Option<Child>,
    workspace: PathBuf,
    socket_path: PathBuf,
}

impl ProofDaemon {
    fn start(proof: &ProofContext) -> Self {
        let socket_path = proof.host_socket_path();
        let mut child = proof.daemon_command().spawn().unwrap();
        wait_for_endpoint(&socket_path, &mut child);
        let daemon = Self {
            child: Some(child),
            workspace: proof.workspace.clone(),
            socket_path,
        };
        drop(daemon.connect());
        daemon
    }

    fn connect(&self) -> DaemonClient {
        self.connect_workspace(&self.workspace)
    }

    fn connect_workspace(&self, workspace: &Path) -> DaemonClient {
        connect_registered_workspace(&self.socket_path, workspace)
    }

    fn pid(&self) -> u32 {
        self.child.as_ref().unwrap().id()
    }

    fn stop(mut self, mut client: DaemonClient) {
        assert_eq!(
            client.shutdown_if_idle().unwrap().result,
            ShutdownIfIdleResultName::Shutdown
        );
        drop(client);
        let mut child = self.child.take().unwrap();
        let status = wait_bounded(&mut child, PROOF_TIMEOUT);
        if !status.success() {
            panic!(
                "daemon shutdown failed ({status}): {}",
                read_pipe(child.stderr.take())
            );
        }
    }
}

fn connect_registered_workspace(socket_path: &Path, workspace: &Path) -> DaemonClient {
    let mut client =
        DaemonClient::connect_with_timeout(socket_path, Duration::from_secs(2)).unwrap();
    match client.hello(workspace) {
        Ok(_) => client,
        Err(ClientError::DaemonResponse(error)) if error.code == ERROR_WORKSPACE_UNREGISTERED => {
            drop(client);
            let mut control =
                DaemonClient::connect_with_timeout(socket_path, Duration::from_secs(2)).unwrap();
            let name = workspace
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("workspace")
                .to_owned();
            control
                .workspace_create(name, workspace.to_path_buf())
                .unwrap();
            drop(control);
            let mut client =
                DaemonClient::connect_with_timeout(socket_path, Duration::from_secs(2)).unwrap();
            client.hello(workspace).unwrap();
            client
        }
        Err(error) => panic!("workspace attach failed: {error}"),
    }
}

fn wait_for_endpoint(socket_path: &Path, child: &mut Child) {
    let deadline = Instant::now() + PROOF_TIMEOUT;
    loop {
        if DaemonClient::connect_with_timeout(socket_path, Duration::from_millis(200)).is_ok() {
            return;
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!(
                "daemon exited before binding ({status}): {}",
                read_pipe(child.stderr.take())
            );
        }
        assert!(Instant::now() < deadline, "daemon did not bind");
        thread::sleep(POLL_INTERVAL);
    }
}

impl Drop for ProofDaemon {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child
            && child.try_wait().ok().flatten().is_none()
        {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn wait_bounded(child: &mut Child, timeout: Duration) -> ExitStatus {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let status = child.wait().unwrap();
            panic!(
                "child did not exit before timeout ({status}): {}",
                read_pipe(child.stderr.take())
            );
        }
        thread::sleep(POLL_INTERVAL);
    }
}

#[cfg(target_os = "linux")]
fn wait_for_terminal_status(client: &mut DaemonClient, run_id: &str) -> RunStateName {
    let deadline = Instant::now() + PROOF_TIMEOUT;
    loop {
        let status = client.events_stream(run_id, None, 1).unwrap().status;
        if !matches!(
            status,
            RunStateName::Running | RunStateName::CancelRequested
        ) {
            return status;
        }
        assert!(Instant::now() < deadline, "run {run_id} did not terminate");
        thread::yield_now();
    }
}

#[cfg(target_os = "linux")]
fn platform_direct_children(parent: u32) -> HashSet<u32> {
    fs::read_dir("/proc")
        .unwrap()
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().to_str()?.parse::<u32>().ok())
        .filter(|pid| {
            let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat")) else {
                return false;
            };
            stat.rsplit_once(") ")
                .and_then(|(_, tail)| tail.split_ascii_whitespace().nth(1))
                .and_then(|value| value.parse::<u32>().ok())
                == Some(parent)
        })
        .collect()
}

#[cfg(target_os = "linux")]
fn kill_process_exact(pid: u32) {
    rustix::process::kill_process(
        rustix::process::Pid::from_raw(pid as i32).unwrap(),
        rustix::process::Signal::KILL,
    )
    .unwrap();
}

#[cfg(target_os = "linux")]
fn linux_process_has_fd(pid: u32, path: &Path) -> bool {
    let expected = path.canonicalize().unwrap();
    fs::read_dir(format!("/proc/{pid}/fd"))
        .unwrap()
        .filter_map(Result::ok)
        .filter_map(|entry| fs::read_link(entry.path()).ok())
        .any(|target| target == expected)
}

#[cfg(target_os = "linux")]
fn wait_for_platform_process_absence(pid: u32) {
    let deadline = Instant::now() + PROOF_TIMEOUT;
    while Path::new(&format!("/proc/{pid}")).exists() {
        assert!(
            Instant::now() < deadline,
            "run child process {pid} remained after lifecycle cleanup"
        );
        thread::yield_now();
    }
}

fn read_pipe(pipe: Option<impl Read>) -> String {
    let mut output = String::new();
    if let Some(mut pipe) = pipe {
        pipe.read_to_string(&mut output).unwrap();
    }
    output
}

#[derive(Clone, Copy, Debug)]
enum UsageFixture {
    Known(u32, u32),
    #[cfg(target_os = "linux")]
    Unknown,
}

#[derive(Clone)]
struct ProviderReply {
    body: String,
}

impl ProviderReply {
    fn tool_call(name: &str, input: Value, usage: UsageFixture) -> Self {
        let delta = json!({
            "choices": [{
                "index": 0,
                "delta": {
                    "tool_calls": [{
                        "index": 0,
                        "id": "provider_call_1",
                        "function": {
                            "name": name,
                            "arguments": serde_json::to_string(&input).unwrap()
                        }
                    }]
                },
                "finish_reason": null
            }]
        });
        let finish = json!({
            "choices": [{
                "index": 0,
                "delta": {},
                "finish_reason": "tool_calls"
            }]
        });
        Self {
            body: event_stream([delta, finish], usage),
        }
    }

    fn answer(answer: &str, usage: UsageFixture) -> Self {
        let delta = json!({
            "choices": [{
                "index": 0,
                "delta": {"content": answer},
                "finish_reason": null
            }]
        });
        let finish = json!({
            "choices": [{
                "index": 0,
                "delta": {},
                "finish_reason": "stop"
            }]
        });
        Self {
            body: event_stream([delta, finish], usage),
        }
    }
}

fn event_stream(events: [Value; 2], usage: UsageFixture) -> String {
    let mut body = String::new();
    for mut event in events {
        event["model"] = json!(SERVED_MODEL);
        body.push_str(&format!("data: {event}\n\n"));
    }
    match usage {
        UsageFixture::Known(prompt_tokens, completion_tokens) => {
            body.push_str(&format!(
                "data: {}\n\n",
                json!({
                    "model": SERVED_MODEL,
                    "choices": [],
                    "usage": {
                        "prompt_tokens": prompt_tokens,
                        "completion_tokens": completion_tokens
                    }
                })
            ));
        }
        #[cfg(target_os = "linux")]
        UsageFixture::Unknown => {}
    }
    body.push_str("data: [DONE]\n\n");
    body
}

struct ControlledThreadProvider {
    base_url: String,
    first_request: mpsc::Receiver<Value>,
    second_request: mpsc::Receiver<Value>,
    release_first: mpsc::Sender<()>,
    release_second: mpsc::Sender<()>,
    handle: Option<thread::JoinHandle<Vec<Value>>>,
}

struct CoordinatorProvider {
    base_url: String,
    handle: Option<thread::JoinHandle<CoordinatorProviderProof>>,
}

struct CoordinatorProviderProof {
    thread_id: String,
    wrapped_result: String,
    request_count: usize,
}

impl CoordinatorProvider {
    fn start(worker_cwd: Arc<Mutex<PathBuf>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let handle = thread::spawn(move || {
            let mut first = accept_before(&listener, Instant::now() + PROOF_TIMEOUT);
            let first_request = read_http_request(&mut first);
            assert!(
                first_request["tools"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|tool| tool["function"]["name"] == "thread_spawn"),
                "coordinator authority did not project thread.spawn into provider tools"
            );
            write_http_response(
                &mut first,
                &ProviderReply::tool_call(
                    "thread_spawn",
                    json!({
                        "agent_id": "bounded-worker",
                        "cwd": worker_cwd.lock().unwrap().to_string_lossy(),
                        "toolset": ["file.read"]
                    }),
                    UsageFixture::Known(8, 3),
                )
                .body,
            );

            let mut second = accept_before(&listener, Instant::now() + PROOF_TIMEOUT);
            let second_request = read_http_request(&mut second);
            let wrapped_result = second_request["messages"]
                .as_array()
                .unwrap()
                .iter()
                .rev()
                .find(|message| message["role"] == "tool")
                .and_then(|message| message["content"].as_str())
                .expect("provider did not receive a tool result")
                .to_owned();
            let body = wrapped_result
                .strip_prefix("<tool_output name=\"thread.spawn\" trust=\"untrusted\">\n")
                .and_then(|body| body.strip_suffix("\n</tool_output>"))
                .expect("thread.spawn result was not wrapped as untrusted tool output");
            let output: Value = serde_json::from_str(body).unwrap();
            assert_eq!(output["status"], "spawned");
            let thread_id = output["thread_id"].as_str().unwrap().to_owned();
            let answer = format!("Dispatched bounded worker {thread_id}.");
            write_http_response(
                &mut second,
                &ProviderReply::answer(&answer, UsageFixture::Known(12, 4)).body,
            );

            CoordinatorProviderProof {
                thread_id,
                wrapped_result,
                request_count: 2,
            }
        });
        Self {
            base_url,
            handle: Some(handle),
        }
    }

    fn join(mut self) -> CoordinatorProviderProof {
        self.handle.take().unwrap().join().unwrap()
    }
}

impl Drop for CoordinatorProvider {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl ControlledThreadProvider {
    fn start(continuation_answer: &'static str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let (first_request_sender, first_request) = mpsc::channel();
        let (second_request_sender, second_request) = mpsc::channel();
        let (release_first, first_release) = mpsc::channel();
        let (release_second, second_release) = mpsc::channel();
        let handle = thread::spawn(move || {
            let mut requests = Vec::new();

            let mut first = accept_before(&listener, Instant::now() + PROOF_TIMEOUT);
            let first_value = read_http_request(&mut first);
            first_request_sender.send(first_value.clone()).unwrap();
            first_release.recv_timeout(PROOF_TIMEOUT).unwrap();
            write_http_response(
                &mut first,
                &ProviderReply::answer("The first child finished.", UsageFixture::Known(5, 2)).body,
            );
            requests.push(first_value);

            let mut second = accept_before(&listener, Instant::now() + PROOF_TIMEOUT);
            let second_value = read_http_request(&mut second);
            second_request_sender.send(second_value.clone()).unwrap();
            second_release.recv_timeout(PROOF_TIMEOUT).unwrap();
            write_http_response(
                &mut second,
                &ProviderReply::answer(continuation_answer, UsageFixture::Known(8, 3)).body,
            );
            requests.push(second_value);

            let mut third = accept_before(&listener, Instant::now() + PROOF_TIMEOUT);
            let third_value = read_http_request(&mut third);
            write_http_response(
                &mut third,
                &ProviderReply::answer("The later turn finished.", UsageFixture::Known(9, 3)).body,
            );
            requests.push(third_value);
            requests
        });
        Self {
            base_url,
            first_request,
            second_request,
            release_first,
            release_second,
            handle: Some(handle),
        }
    }

    fn join(mut self) -> Vec<Value> {
        self.handle.take().unwrap().join().unwrap()
    }
}

impl Drop for ControlledThreadProvider {
    fn drop(&mut self) {
        let _ = self.release_first.send(());
        let _ = self.release_second.send(());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(target_os = "linux")]
struct KillIsolationProvider {
    base_url: String,
    first_requested: mpsc::Receiver<()>,
    second_requested: mpsc::Receiver<()>,
    release_second: mpsc::Sender<()>,
    handle: thread::JoinHandle<()>,
}

#[cfg(target_os = "linux")]
impl KillIsolationProvider {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let (first_sender, first_requested) = mpsc::channel();
        let (second_sender, second_requested) = mpsc::channel();
        let (release_second, release_receiver) = mpsc::channel();
        let handle = thread::spawn(move || {
            let mut first = accept_before(&listener, Instant::now() + PROOF_TIMEOUT);
            let _ = read_http_request(&mut first);
            first_sender.send(()).unwrap();

            let mut second = accept_before(&listener, Instant::now() + PROOF_TIMEOUT);
            let _ = read_http_request(&mut second);
            second_sender.send(()).unwrap();
            release_receiver.recv_timeout(PROOF_TIMEOUT).unwrap();
            write_http_response(
                &mut second,
                &ProviderReply::answer("the second child stayed healthy", UsageFixture::Unknown)
                    .body,
            );
            drop(first);
        });
        Self {
            base_url,
            first_requested,
            second_requested,
            release_second,
            handle,
        }
    }

    fn join(self) {
        self.handle.join().unwrap();
    }
}

fn accept_before(listener: &TcpListener, deadline: Instant) -> TcpStream {
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                stream
                    .set_nonblocking(false)
                    .expect("accepted provider stream should become blocking");
                return stream;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < deadline, "provider was not called");
                thread::sleep(POLL_INTERVAL);
            }
            Err(error) => panic!("provider accept failed: {error}"),
        }
    }
}

fn read_http_request(stream: &mut TcpStream) -> Value {
    stream.set_read_timeout(Some(PROOF_TIMEOUT)).unwrap();
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 4096];
    let header_end = loop {
        let count = stream.read(&mut buffer).unwrap();
        assert_ne!(count, 0, "provider request ended before headers");
        bytes.extend_from_slice(&buffer[..count]);
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let headers = String::from_utf8_lossy(&bytes[..header_end]);
    let content_length = headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .and_then(|value| value.trim().parse::<usize>().ok())
        })
        .unwrap();
    while bytes.len() < header_end + content_length {
        let count = stream.read(&mut buffer).unwrap();
        assert_ne!(count, 0, "provider request ended before body");
        bytes.extend_from_slice(&buffer[..count]);
    }
    serde_json::from_slice(&bytes[header_end..header_end + content_length]).unwrap()
}

fn write_http_response(stream: &mut TcpStream, body: &str) {
    write!(
        stream,
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
}

fn write_provider_config(path: &Path, base_url: &str) {
    fs::write(
        path,
        format!(
            r#"[provider]
kind = "open_ai"
model = "test-model"
api_key_env = "{API_KEY_ENV}"
base_url = "{base_url}"
connect_timeout_ms = 5000
stream_idle_timeout_ms = 5000

[limits]
token_budget = 4000
max_output_tokens = 64
max_turns = 2

[tools]
enabled = ["file.read", "file.write"]
"#
        ),
    )
    .unwrap();
}

fn write_coordinator_config(path: &Path, base_url: &str) {
    fs::write(
        path,
        format!(
            r#"[provider]
kind = "open_ai"
model = "test-model"
api_key_env = "{API_KEY_ENV}"
base_url = "{base_url}"
connect_timeout_ms = 5000
stream_idle_timeout_ms = 5000

[limits]
token_budget = 4000
max_output_tokens = 64
max_turns = 2
max_spawn_depth = 1

[tools]
enabled = ["file.read", "thread.spawn"]
"#
        ),
    )
    .unwrap();
}
