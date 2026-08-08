use plato_agent::{
    ApprovalMode, RunLedger, RunOptions, RunSession,
    daemon::{
        client::{ClientError, DaemonClient},
        protocol::{
            BufferedThreadEvent, CAPABILITY_THREAD_EVENTS, CAPABILITY_THREAD_LIST,
            CAPABILITY_THREAD_SEND, CAPABILITY_THREAD_SPAWN, CAPABILITY_THREAD_STATUS,
            CAPABILITY_THREAD_STOP, ERROR_NOT_FOUND, ReasoningEffort, RunStateName,
            ShutdownIfIdleResultName, StreamEvent, ThreadApprovalPolicy, ThreadSendRejectedReason,
            ThreadSendResult, ThreadSpawnDecision, ThreadSpawnResult, ThreadStopResult,
        },
    },
    ledger::SqliteLedger,
    paths, run_question,
};
use platonic_core::{
    ActorId, AgentId, ContextPack, EffectClass, HarnessEvent, Message, MessageRole, ModelName,
    ModelUsage, PolicyDecision, ReadbackEntry, RecordedEvent, ResultVisibility, RunId, RunPhase,
    RunReadback, ToolCall, ToolCallId, ToolName, ToolProposal, ToolResult, TurnId,
};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    fs,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Output, Stdio},
    sync::{Arc, Mutex, mpsc},
    thread,
    time::{Duration, Instant},
};

const API_KEY_ENV: &str = "PLATO_SEMANTIC_CONFORMANCE_TEST_KEY";
const DENIAL_REASON: &str = "approval denied by stdin";
const FIXTURE_INITIAL: &str = "fixture baseline\n";
const FIXTURE_WRITTEN: &str = "fixture changed\n";
const SERVED_MODEL: &str = "provider/test-model-2026-08-01";
const PROOF_TIMEOUT: Duration = Duration::from_secs(15);
const POLL_INTERVAL: Duration = Duration::from_millis(10);
const REQUESTS_PER_LEG: usize = 2;
static SCENARIO_SERIAL: Mutex<()> = Mutex::new(());

#[test]
fn child_and_embedded_read_only_success_have_identical_ordered_events() {
    let _serial = SCENARIO_SERIAL.lock().unwrap();
    run_scenario(Scenario::ReadOnly);
}

#[test]
fn child_and_embedded_approved_tool_run_have_identical_ordered_events() {
    let _serial = SCENARIO_SERIAL.lock().unwrap();
    run_scenario(Scenario::ApprovedWrite);
}

#[test]
fn child_and_embedded_denied_approval_have_identical_ordered_events() {
    let _serial = SCENARIO_SERIAL.lock().unwrap();
    run_scenario(Scenario::DeniedWrite);
}

#[test]
fn child_and_embedded_provider_failure_have_identical_ordered_events() {
    temp_env::with_var(API_KEY_ENV, Some("test-key"), || {
        let _serial = SCENARIO_SERIAL.lock().unwrap();
        let root = Arc::new(tempfile::tempdir().unwrap());
        let embedded = ProofContext::in_root(Arc::clone(&root), "embedded-failure");
        let child = ProofContext::in_root(root, "child-failure");
        let provider = FailureProvider::start();
        write_provider_config(&embedded.config_path, &provider.base_url);
        write_provider_config(&child.config_path, &provider.base_url);

        let embedded_run_id = RunId::new("run_embedded_failure").unwrap();
        let result = run_question(RunOptions {
            question: "fail this run".into(),
            config_path: Some(PathBuf::from("plato.toml")),
            overrides: Default::default(),
            ledger: RunLedger::Sqlite(embedded.ledger_path.clone()),
            workspace_root: embedded.workspace.clone(),
            approval_mode: ApprovalMode::Deny { actor: "test" },
            run_id: Some(embedded_run_id.clone()),
            session: Some(RunSession::Fresh {
                session_id: "session_embedded_failure".into(),
            }),
            event_sender: None,
            stream_to_stderr: false,
            cancel: None,
            voice_interruption_context: None,
        });
        assert!(result.is_err());
        let embedded_records = SqliteLedger::open_readonly(&embedded.ledger_path)
            .unwrap()
            .read_run(embedded_run_id.as_str())
            .unwrap();

        let daemon = ProofDaemon::start(&child);
        let mut client = daemon.connect();
        let child_run = client
            .run_start("fail this run".into(), Some("plato.toml".into()), false)
            .unwrap();
        assert_eq!(
            wait_for_terminal_status(&mut client, &child_run.run_id),
            RunStateName::Failed
        );
        let child_records = SqliteLedger::open_readonly(&child.ledger_path)
            .unwrap()
            .read_run(&child_run.run_id)
            .unwrap();

        assert_eq!(
            normalize_records(&embedded_records),
            normalize_records(&child_records)
        );
        assert_eq!(
            RunReadback::from_events(&embedded_records)
                .unwrap()
                .final_phase,
            RunReadback::from_events(&child_records)
                .unwrap()
                .final_phase
        );
        assert_eq!(provider.join(), 2);
        daemon.stop(client);
    });
}

#[cfg(any(target_os = "linux", windows))]
#[test]
fn killed_wedged_child_has_no_ledger_handle_and_other_run_stays_healthy() {
    let _serial = SCENARIO_SERIAL.lock().unwrap();
    let proof = ProofContext::new();
    proof.restore_fixture();
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
        assert!(linux_process_has_fd(daemon_pid, &proof.ledger_path));
        assert!(!linux_process_has_fd(first_child, &proof.ledger_path));
        assert!(!linux_process_has_fd(second_child, &proof.ledger_path));
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
fn child_and_embedded_cancellation_have_identical_ordered_events() {
    temp_env::with_var(API_KEY_ENV, Some("test-key"), || {
        child_and_embedded_cancellation_with_key()
    });
}

fn child_and_embedded_cancellation_with_key() {
    let _serial = SCENARIO_SERIAL.lock().unwrap();
    let root = Arc::new(tempfile::tempdir().unwrap());
    let embedded = ProofContext::in_root(Arc::clone(&root), "embedded-cancel");
    let child = ProofContext::in_root(root, "child-cancel");
    let provider = CancelableProvider::start();
    write_provider_config(&embedded.config_path, &provider.base_url);
    write_provider_config(&child.config_path, &provider.base_url);

    let embedded_cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let embedded_run_id = RunId::new("run_embedded_cancel").unwrap();
    let embedded_run_id_for_worker = embedded_run_id.clone();
    let embedded_cancel_for_worker = embedded_cancel.clone();
    let embedded_workspace = embedded.workspace.clone();
    let embedded_ledger = embedded.ledger_path.clone();
    let (outcome_sender, outcome_receiver) = mpsc::channel();
    let embedded_worker = thread::spawn(move || {
        let outcome = run_question(RunOptions {
            question: "cancel this run".into(),
            config_path: Some(PathBuf::from("plato.toml")),
            overrides: Default::default(),
            ledger: RunLedger::Sqlite(embedded_ledger),
            workspace_root: embedded_workspace,
            approval_mode: ApprovalMode::Deny { actor: "test" },
            run_id: Some(embedded_run_id_for_worker),
            session: Some(RunSession::Fresh {
                session_id: "session_embedded_cancel".into(),
            }),
            event_sender: None,
            stream_to_stderr: false,
            cancel: Some(embedded_cancel_for_worker),
            voice_interruption_context: None,
        });
        outcome_sender.send(outcome).unwrap();
    });
    assert_eq!(provider.requested.recv_timeout(PROOF_TIMEOUT).unwrap(), 0);
    embedded_cancel.store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(matches!(
        outcome_receiver.recv_timeout(PROOF_TIMEOUT).unwrap(),
        Err(plato_agent::AppError::RunCanceled)
    ));
    provider.release.send(()).unwrap();
    embedded_worker.join().unwrap();
    let embedded_records = SqliteLedger::open_readonly(&embedded.ledger_path)
        .unwrap()
        .read_run(embedded_run_id.as_str())
        .unwrap();

    let daemon = ProofDaemon::start(&child);
    let mut client = daemon.connect();
    let child_run = client
        .run_start("cancel this run".into(), Some("plato.toml".into()), false)
        .unwrap();
    assert_eq!(provider.requested.recv_timeout(PROOF_TIMEOUT).unwrap(), 1);
    assert_eq!(
        client.run_cancel(&child_run.run_id).unwrap().status,
        RunStateName::CancelRequested
    );
    assert_eq!(
        wait_for_terminal_status(&mut client, &child_run.run_id),
        RunStateName::Canceled
    );
    provider.release.send(()).unwrap();
    let child_records = SqliteLedger::open_readonly(&child.ledger_path)
        .unwrap()
        .read_run(&child_run.run_id)
        .unwrap();

    assert_eq!(
        normalize_records(&embedded_records),
        normalize_records(&child_records)
    );
    assert_eq!(
        RunReadback::from_events(&embedded_records)
            .unwrap()
            .final_phase,
        RunReadback::from_events(&child_records)
            .unwrap()
            .final_phase
    );
    provider.join();
    daemon.stop(client);
}

#[test]
fn one_host_daemon_serves_two_workspaces_and_coexists_with_legacy_daemon() {
    let _serial = SCENARIO_SERIAL.lock().unwrap();
    let root = Arc::new(tempfile::tempdir().unwrap());
    let first = ProofContext::in_root(Arc::clone(&root), "workspace-a");
    let second = ProofContext::in_root(Arc::clone(&root), "workspace-b");
    let first_workspace = first.workspace.clone();
    let first_socket = first.socket_path.clone();
    let mut prepared = Vec::new();

    for (proof, scenario) in [
        (first, Scenario::ApprovedWrite),
        (second, Scenario::DeniedWrite),
    ] {
        proof.restore_fixture();
        let provider = ScriptedProvider::start(scenario.provider_replies());
        write_provider_config(&proof.config_path, &provider.base_url);
        let direct = run_direct_leg(&proof, scenario);
        proof.restore_fixture();
        prepared.push((proof, scenario, provider, direct));
    }

    let host = ProofDaemon::start_host(&prepared[0].0);
    let host_pid = host.pid();
    let host_socket = host.socket_path.clone();
    for (proof, scenario, provider, direct) in prepared {
        let mut client = host.connect_workspace(&proof.workspace);
        let hello = client.hello(&proof.workspace).unwrap();
        assert_eq!(Path::new(&hello.ledger_path), proof.ledger_path);
        let daemon = run_daemon_leg(&proof, scenario, &mut client);
        drop(client);
        assert_scenario_conformance(scenario, direct, daemon, provider.join());
        assert_eq!(
            host.pid(),
            host_pid,
            "host daemon process changed between cwds"
        );
    }

    let legacy_proof = ProofContext::in_root(root, "legacy-workspace");
    let legacy = ProofDaemon::start(&legacy_proof);
    let legacy_client = legacy.connect();
    let mut host_client = host.connect_workspace(&first_workspace);
    host_client.hello(&first_workspace).unwrap();
    assert_ne!(host_socket, first_socket);
    assert_ne!(host_socket, legacy.socket_path);
    assert!(legacy_proof.lock_path().exists());
    assert!(legacy_proof.host_lock_path().exists());
    assert_eq!(host.pid(), host_pid);
    legacy.stop(legacy_client);
    host.stop(host_client);
}

#[test]
fn thread_spawn_list_and_status_are_semantically_conformant_on_host_daemon() {
    let _serial = SCENARIO_SERIAL.lock().unwrap();
    let proof = ProofContext::new();
    let child_cwd = proof.workspace.join("child");
    fs::create_dir(&child_cwd).unwrap();
    let host = ProofDaemon::start_host(&proof);
    let mut client = host.connect();
    let hello = client.hello(&proof.workspace).unwrap();
    for capability in [
        CAPABILITY_THREAD_SPAWN,
        CAPABILITY_THREAD_LIST,
        CAPABILITY_THREAD_STATUS,
        CAPABILITY_THREAD_STOP,
    ] {
        assert!(hello.capabilities.iter().any(|served| served == capability));
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
    let root = match client
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
    assert_eq!(root.authority.thread_id, root_thread_id);
    assert_eq!(root.authority.parent_thread_id, None);
    assert_eq!(root.authority.spawning_actor, "semantic_fixture");
    assert_eq!(
        Path::new(&root.authority.cwd),
        proof.workspace.canonicalize().unwrap()
    );
    assert_eq!(root.authority.model, "gpt-5.6-sol");
    assert_eq!(root.authority.reasoning_effort, ReasoningEffort::Xhigh);
    assert_eq!(root.authority.approval_policy, ThreadApprovalPolicy::Yolo);
    assert!(root.authority.created_at_ms > 0);
    assert!(root.live.loaded);
    assert_eq!(root.live.current_turn_id, None);
    assert_eq!(
        root.live.last_activity_at_ms,
        Some(root.authority.created_at_ms)
    );

    let child = match client
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
    assert_eq!(
        child.authority.parent_thread_id.as_deref(),
        Some(root_thread_id.as_str())
    );
    assert_eq!(child.authority.spawning_actor, "yolo");
    assert!(child.live.loaded);
    assert_eq!(
        child.live.last_activity_at_ms,
        Some(child.authority.created_at_ms)
    );

    let listed = client.thread_list().unwrap().threads;
    assert_eq!(listed.len(), 2);
    assert!(listed.iter().all(|thread| thread.live.loaded));
    assert_eq!(
        client
            .thread_status(child.authority.thread_id.clone())
            .unwrap()
            .thread,
        child
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
        .find(|thread| thread.authority.thread_id == child.authority.thread_id)
        .unwrap();
    assert!(!stopped_root.live.loaded);
    assert_eq!(stopped_root.live.last_activity_at_ms, None);
    assert!(orphaned_child.live.loaded);
    assert_eq!(orphaned_child.authority, child.authority);
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

    let restarted = ProofDaemon::start_host(&proof);
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
        .thread_status(child.authority.thread_id.clone())
        .unwrap()
        .thread;
    assert_eq!(readback.authority, child.authority);
    assert!(!readback.live.loaded);
    let child_stop = restarted_client
        .thread_stop(child.authority.thread_id.clone(), "restart_fixture".into())
        .unwrap();
    let child_stopped_at_ms = match child_stop {
        ThreadStopResult::Stopped {
            stopped_at_ms,
            stopped_turn_id: None,
            ..
        } => stopped_at_ms,
        unexpected => panic!("expected stopped unloaded child, got {unexpected:?}"),
    };
    assert_eq!(
        restarted_client
            .thread_stop(child.authority.thread_id.clone(), "retry_fixture".into())
            .unwrap(),
        ThreadStopResult::AlreadyStopped {
            thread_id: child.authority.thread_id.clone(),
            stopped_turn_id: None,
            stopped_at_ms: child_stopped_at_ms,
        }
    );
    let stale_parent = restarted_client
        .thread_spawn_start(
            Some(root_thread_id),
            child_cwd.to_string_lossy().into_owned(),
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
fn thread_send_and_three_observers_are_semantically_conformant_on_host_daemon() {
    const INITIAL_MESSAGE: &str = "begin the controlled thread proof";
    const STEERED_MESSAGE: &str = "include the exact steered phrase in the continuation";
    const CONTINUATION_ANSWER: &str = "The continuation used the exact steered phrase.";

    let _serial = SCENARIO_SERIAL.lock().unwrap();
    let proof = ProofContext::new();
    let provider = ControlledThreadProvider::start(CONTINUATION_ANSWER);
    write_provider_config(&proof.config_path, &provider.base_url);
    let host = ProofDaemon::start_host(&proof);
    let mut controller_a = host.connect();
    let mut controller_b = host.connect();
    let hello = controller_a.hello(&proof.workspace).unwrap();
    for capability in [
        CAPABILITY_THREAD_SEND,
        CAPABILITY_THREAD_EVENTS,
        CAPABILITY_THREAD_SPAWN,
        CAPABILITY_THREAD_LIST,
        CAPABILITY_THREAD_STATUS,
    ] {
        assert!(hello.capabilities.iter().any(|served| served == capability));
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
fn normalizer_accepts_only_host_variance_and_keeps_ids_bijective() {
    let direct = sample_evidence(SampleApproval::Granted);
    let mut daemon = direct.clone();
    for (index, record) in daemon.records.iter_mut().enumerate() {
        record.occurred_at_ms = 9_000 + index as u64;
    }
    mutate_host_ids(&mut daemon.records, |kind, id| {
        *id = format!("daemon_{}_{}", kind.label(), id);
    });
    set_approval_actor(&mut daemon.records, "daemon");

    assert_semantically_equivalent(&direct, &daemon).unwrap();

    let mut collapsed = daemon.clone();
    mutate_host_ids(&mut collapsed.records, |kind, id| {
        if kind == IdKind::Turn && id.ends_with("turn_2") {
            *id = "daemon_turn_turn_1".into();
        }
    });
    assert_difference(&direct, &collapsed, "persisted event semantics");

    let mut non_transport_actor = daemon;
    set_approval_actor(&mut non_transport_actor.records, "automation");
    assert_difference(&direct, &non_transport_actor, "persisted event semantics");
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

#[test]
fn normalizer_rejects_changed_policy_outcome_and_event_order() {
    let direct = sample_evidence(SampleApproval::Granted);
    let mut policy_changed = direct.clone();
    let decision = policy_changed
        .records
        .iter_mut()
        .find_map(|record| match &mut record.event {
            HarnessEvent::PolicyEvaluated { decision, .. } => Some(decision),
            _ => None,
        })
        .unwrap();
    *decision = PolicyDecision::Deny {
        reason: "changed policy outcome".into(),
    };
    assert_difference(&direct, &policy_changed, "persisted event semantics");

    let mut reordered = direct.clone();
    reordered.records.swap(1, 2);
    assert_difference(&direct, &reordered, "persisted event semantics");
}

#[test]
fn semantic_comparison_rejects_changed_final_phase_and_usage_known_state() {
    let direct = sample_evidence(SampleApproval::Granted);
    let mut phase_changed = direct.clone();
    phase_changed.final_phase = RunPhase::Failed {
        reason: "changed final phase".into(),
    };
    assert_difference(&direct, &phase_changed, "final phase");

    let mut usage_changed = direct.clone();
    usage_changed.usage_known[0] = false;
    assert_difference(&direct, &usage_changed, "usage-known state");
}

#[test]
fn normalizer_rejects_changed_approval_outcome_and_denial_reason() {
    let granted = sample_evidence(SampleApproval::Granted);
    let mut outcome_changed = granted.clone();
    let approval_index = outcome_changed
        .records
        .iter()
        .position(|record| matches!(record.event, HarnessEvent::ApprovalGranted { .. }))
        .unwrap();
    let replacement = match &outcome_changed.records[approval_index].event {
        HarnessEvent::ApprovalGranted {
            run_id,
            call_id,
            actor_id,
        } => HarnessEvent::ApprovalDenied {
            run_id: run_id.clone(),
            call_id: call_id.clone(),
            actor_id: actor_id.clone(),
            reason: DENIAL_REASON.into(),
        },
        _ => unreachable!(),
    };
    outcome_changed.records[approval_index].event = replacement;
    assert_difference(&granted, &outcome_changed, "persisted event semantics");

    let denied = sample_evidence(SampleApproval::Denied);
    let mut reason_changed = denied.clone();
    let reason = reason_changed
        .records
        .iter_mut()
        .find_map(|record| match &mut record.event {
            HarnessEvent::ApprovalDenied { reason, .. } => Some(reason),
            _ => None,
        })
        .unwrap();
    *reason = "different denial reason".into();
    assert_difference(&denied, &reason_changed, "persisted event semantics");
}

#[derive(Clone, Copy, Debug)]
enum Scenario {
    ReadOnly,
    ApprovedWrite,
    DeniedWrite,
}

impl Scenario {
    fn question(self) -> &'static str {
        match self {
            Self::ReadOnly => "read the fixture",
            Self::ApprovedWrite => "write the fixture with approval",
            Self::DeniedWrite => "try to write the fixture without approval",
        }
    }

    fn answer(self) -> &'static str {
        match self {
            Self::ReadOnly => "The fixture contains its baseline text.",
            Self::ApprovedWrite => "The fixture was changed.",
            Self::DeniedWrite => "The fixture was not changed.",
        }
    }

    fn provider_tool(self) -> (&'static str, Value) {
        match self {
            Self::ReadOnly => ("file_read", json!({"path": "fixture.txt"})),
            Self::ApprovedWrite | Self::DeniedWrite => (
                "file_write",
                json!({"path": "fixture.txt", "content": FIXTURE_WRITTEN}),
            ),
        }
    }

    fn approval(self) -> ScenarioApproval {
        match self {
            Self::ReadOnly => ScenarioApproval::None,
            Self::ApprovedWrite => ScenarioApproval::Grant,
            Self::DeniedWrite => ScenarioApproval::Deny,
        }
    }

    fn usage(self) -> (UsageFixture, UsageFixture) {
        match self {
            Self::ReadOnly => (UsageFixture::Known(13, 3), UsageFixture::Known(21, 7)),
            Self::ApprovedWrite => (UsageFixture::Unknown, UsageFixture::Unknown),
            Self::DeniedWrite => (UsageFixture::Known(0, 0), UsageFixture::Known(0, 0)),
        }
    }

    fn expected_usage_known(self) -> Vec<bool> {
        let (first, second) = self.usage();
        vec![first.is_known(), second.is_known()]
    }

    fn expected_fixture(self) -> &'static str {
        match self {
            Self::ApprovedWrite => FIXTURE_WRITTEN,
            Self::ReadOnly | Self::DeniedWrite => FIXTURE_INITIAL,
        }
    }

    fn provider_replies(self) -> Vec<ProviderReply> {
        let (tool_name, input) = self.provider_tool();
        let (tool_usage, answer_usage) = self.usage();
        let tool = ProviderReply::tool_call(tool_name, input, tool_usage);
        let answer = ProviderReply::answer(self.answer(), answer_usage);
        vec![tool.clone(), answer.clone(), tool, answer]
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScenarioApproval {
    None,
    Grant,
    Deny,
}

fn run_scenario(scenario: Scenario) {
    let proof = ProofContext::new();
    proof.restore_fixture();
    let provider = ScriptedProvider::start(scenario.provider_replies());
    write_provider_config(&proof.config_path, &provider.base_url);

    let direct = run_direct_leg(&proof, scenario);
    assert_eq!(
        direct.fixture,
        scenario.expected_fixture().as_bytes(),
        "{scenario:?} direct fixture effect"
    );

    proof.restore_fixture();
    assert_eq!(
        fs::read(&proof.fixture_path).unwrap(),
        FIXTURE_INITIAL.as_bytes()
    );

    let daemon = ProofDaemon::start(&proof);
    let mut client = daemon.connect();
    let hello = client.hello(&proof.workspace).unwrap();
    assert_eq!(Path::new(&hello.ledger_path), proof.ledger_path);
    let daemon_leg = run_daemon_leg(&proof, scenario, &mut client);
    daemon.stop(client);

    assert_scenario_conformance(scenario, direct, daemon_leg, provider.join());
}

fn assert_scenario_conformance(
    scenario: Scenario,
    direct: RunEvidence,
    daemon: RunEvidence,
    requests: Vec<Value>,
) {
    assert_eq!(
        requests.len(),
        REQUESTS_PER_LEG * 2,
        "{scenario:?} provider request count"
    );
    let direct = direct.with_provider_requests(requests[..REQUESTS_PER_LEG].to_vec());
    let daemon = daemon.with_provider_requests(requests[REQUESTS_PER_LEG..].to_vec());

    assert_eq!(
        direct.usage_known,
        scenario.expected_usage_known(),
        "{scenario:?} direct usage-known state"
    );
    assert_eq!(
        daemon.usage_known,
        scenario.expected_usage_known(),
        "{scenario:?} daemon usage-known state"
    );
    assert_eq!(served_models(&direct.records), vec![SERVED_MODEL; 2]);
    assert_eq!(served_models(&daemon.records), vec![SERVED_MODEL; 2]);
    assert_approval_transport(scenario, &direct.records, &daemon.records);
    assert_policy_and_effect(scenario, &direct.records);
    assert_policy_and_effect(scenario, &daemon.records);
    assert_semantically_equivalent(&direct, &daemon)
        .unwrap_or_else(|error| panic!("{scenario:?} semantic divergence: {error}"));
}

fn run_direct_leg(proof: &ProofContext, scenario: Scenario) -> RunEvidence {
    proof.assert_no_serving_daemon("before direct CLI run");
    let mut command = proof.plato_command();
    command
        .arg("--config")
        .arg("plato.toml")
        .arg(scenario.question())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().unwrap();
    if let Some(mut stdin) = child.stdin.take() {
        match scenario.approval() {
            ScenarioApproval::Grant => stdin.write_all(b"y\n").unwrap(),
            ScenarioApproval::Deny => stdin.write_all(b"n\n").unwrap(),
            ScenarioApproval::None => {}
        }
    }
    let output = child.wait_with_output().unwrap();
    assert_success("direct CLI", &output);
    proof.assert_no_serving_daemon("after direct CLI run");

    let stderr = String::from_utf8(output.stderr).unwrap();
    let run_id = output_field(&stderr, "run_id: ");
    assert_eq!(
        Path::new(&output_field(&stderr, "ledger_path: ")),
        proof.ledger_path
    );
    let answer = String::from_utf8(output.stdout)
        .unwrap()
        .trim_end_matches(['\r', '\n'])
        .to_owned();
    assert_eq!(answer, scenario.answer());

    let records = SqliteLedger::open_readonly(&proof.ledger_path)
        .unwrap()
        .read_run(&run_id)
        .unwrap();
    assert!(
        records
            .iter()
            .all(|record| record.event.run_id().as_str() == run_id)
    );
    RunEvidence::from_leg(records, answer, fs::read(&proof.fixture_path).unwrap())
}

fn run_daemon_leg(
    proof: &ProofContext,
    scenario: Scenario,
    client: &mut DaemonClient,
) -> RunEvidence {
    let started = client
        .run_start(scenario.question().into(), Some("plato.toml".into()), false)
        .unwrap();
    assert_eq!(Path::new(&started.ledger_path), proof.ledger_path);
    let deadline = Instant::now() + PROOF_TIMEOUT;
    let mut approval_decided = false;
    let mut from_offset = Some(0);

    loop {
        let page = client
            .events_stream(&started.run_id, from_offset, 128)
            .unwrap();
        from_offset = Some(page.next_offset);
        for buffered in &page.events {
            if let StreamEvent::ApprovalRequested { tool_call_id, .. } = &buffered.event {
                assert!(!approval_decided, "received duplicate approval request");
                match scenario.approval() {
                    ScenarioApproval::Grant => {
                        client
                            .approval_grant(&started.run_id, tool_call_id)
                            .unwrap();
                    }
                    ScenarioApproval::Deny => {
                        client
                            .approval_deny(&started.run_id, tool_call_id, DENIAL_REASON.into())
                            .unwrap();
                    }
                    ScenarioApproval::None => {
                        panic!("read-only scenario unexpectedly requested approval");
                    }
                }
                approval_decided = true;
            }
        }

        match page.status {
            RunStateName::Finished => break,
            RunStateName::Running | RunStateName::CancelRequested => {}
            status => panic!("daemon run ended as {status}"),
        }
        assert!(
            Instant::now() < deadline,
            "daemon run did not finish for {scenario:?}"
        );
        thread::sleep(POLL_INTERVAL);
    }

    assert_eq!(
        approval_decided,
        scenario.approval() != ScenarioApproval::None
    );
    let transcript = client.transcript_read(&started.run_id).unwrap();
    assert_eq!(transcript.status, RunStateName::Finished);
    let answer = transcript.final_answer.unwrap();
    assert_eq!(answer, scenario.answer());
    let records = SqliteLedger::open_readonly(&proof.ledger_path)
        .unwrap()
        .read_run(&started.run_id)
        .unwrap();
    RunEvidence::from_leg(records, answer, fs::read(&proof.fixture_path).unwrap())
}

#[derive(Clone, Debug)]
struct RunEvidence {
    provider_requests: Vec<Value>,
    records: Vec<RecordedEvent>,
    final_phase: RunPhase,
    final_answer: String,
    usage_known: Vec<bool>,
    fixture: Vec<u8>,
}

impl RunEvidence {
    fn from_leg(records: Vec<RecordedEvent>, reported_answer: String, fixture: Vec<u8>) -> Self {
        let readback = RunReadback::from_events(&records).unwrap();
        let final_answer = readback_answer(&readback);
        assert_eq!(reported_answer, final_answer);
        Self {
            provider_requests: Vec::new(),
            usage_known: usage_known(&records),
            records,
            final_phase: readback.final_phase,
            final_answer,
            fixture,
        }
    }

    fn with_provider_requests(mut self, provider_requests: Vec<Value>) -> Self {
        self.provider_requests = provider_requests;
        self
    }
}

fn readback_answer(readback: &RunReadback) -> String {
    readback
        .entries
        .iter()
        .rev()
        .find_map(|entry| match entry {
            ReadbackEntry::ModelMessage { message, .. }
                if message.role == MessageRole::Assistant =>
            {
                Some(message.content.clone())
            }
            _ => None,
        })
        .expect("finished run has an assistant answer")
}

fn usage_known(records: &[RecordedEvent]) -> Vec<bool> {
    records
        .iter()
        .filter_map(|record| match &record.event {
            HarnessEvent::ModelResponded { usage, .. } => Some(usage.is_some()),
            _ => None,
        })
        .collect()
}

fn served_models(records: &[RecordedEvent]) -> Vec<&str> {
    records
        .iter()
        .filter_map(|record| match &record.event {
            HarnessEvent::ModelResponded {
                served_model: Some(model),
                ..
            } => Some(model.as_str()),
            _ => None,
        })
        .collect()
}

fn assert_semantically_equivalent(
    direct: &RunEvidence,
    daemon: &RunEvidence,
) -> Result<(), String> {
    if direct.provider_requests != daemon.provider_requests {
        return Err("provider requests differ".into());
    }
    if normalize_records(&direct.records) != normalize_records(&daemon.records) {
        return Err("persisted event semantics differ".into());
    }
    if direct.final_phase != daemon.final_phase {
        return Err("final phase differs".into());
    }
    if direct.final_answer != daemon.final_answer {
        return Err("final answer differs".into());
    }
    if direct.usage_known != daemon.usage_known {
        return Err("usage-known state differs".into());
    }
    if direct.fixture != daemon.fixture {
        return Err("workspace fixture effect differs".into());
    }
    Ok(())
}

fn assert_difference(direct: &RunEvidence, changed: &RunEvidence, expected: &str) {
    let error = assert_semantically_equivalent(direct, changed).unwrap_err();
    assert!(
        error.contains(expected),
        "expected {expected:?} difference, got {error:?}"
    );
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum IdKind {
    Run,
    Turn,
    Call,
    Artifact,
}

impl IdKind {
    fn label(self) -> &'static str {
        match self {
            Self::Run => "run",
            Self::Turn => "turn",
            Self::Call => "call",
            Self::Artifact => "artifact",
        }
    }
}

#[derive(Default)]
struct IdNormalizer {
    canonical: HashMap<(IdKind, String), String>,
    next: HashMap<IdKind, u64>,
}

impl IdNormalizer {
    fn normalize(&mut self, kind: IdKind, original: &str) -> String {
        let key = (kind, original.to_owned());
        if let Some(canonical) = self.canonical.get(&key) {
            return canonical.clone();
        }
        let next = self.next.entry(kind).or_default();
        *next += 1;
        let canonical = format!("{}_{}", kind.label(), next);
        self.canonical.insert(key, canonical.clone());
        canonical
    }
}

fn normalize_records(records: &[RecordedEvent]) -> Vec<Value> {
    let mut ids = IdNormalizer::default();
    records
        .iter()
        .map(|record| {
            let mut value = serde_json::to_value(record).unwrap();
            value["occurred_at_ms"] = json!(0);
            visit_host_ids(&mut value, |kind, id| {
                *id = ids.normalize(kind, id);
            });
            project_approval_actor(&mut value);
            value
        })
        .collect()
}

fn visit_host_ids(value: &mut Value, mut visit: impl FnMut(IdKind, &mut String)) {
    visit_string(value, "/event/run_id", IdKind::Run, &mut visit);
    visit_string(value, "/event/turn_id", IdKind::Turn, &mut visit);
    visit_string(value, "/event/call_id", IdKind::Call, &mut visit);
    visit_string(value, "/event/call/id", IdKind::Call, &mut visit);
    visit_string(value, "/event/result/call_id", IdKind::Call, &mut visit);
    if let Some(Value::Array(artifacts)) = value.pointer_mut("/event/result/artifacts") {
        for artifact in artifacts {
            if let Value::String(id) = artifact {
                visit(IdKind::Artifact, id);
            }
        }
    }
}

fn visit_string(
    value: &mut Value,
    pointer: &str,
    kind: IdKind,
    visit: &mut impl FnMut(IdKind, &mut String),
) {
    if let Some(Value::String(id)) = value.pointer_mut(pointer) {
        visit(kind, id);
    }
}

fn project_approval_actor(value: &mut Value) {
    let approval_event = matches!(
        value.pointer("/event/event").and_then(Value::as_str),
        Some("approval_granted" | "approval_denied")
    );
    if !approval_event {
        return;
    }
    if let Some(Value::String(actor)) = value.pointer_mut("/event/actor_id")
        && matches!(actor.as_str(), "stdin" | "daemon")
    {
        *actor = "human_decision".into();
    }
}

fn mutate_host_ids(records: &mut [RecordedEvent], mut mutate: impl FnMut(IdKind, &mut String)) {
    for record in records {
        let mut value = serde_json::to_value(&*record).unwrap();
        visit_host_ids(&mut value, &mut mutate);
        *record = serde_json::from_value(value).unwrap();
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ApprovalSnapshot {
    Granted { actor: String },
    Denied { actor: String, reason: String },
}

fn approval_snapshots(records: &[RecordedEvent]) -> Vec<ApprovalSnapshot> {
    records
        .iter()
        .filter_map(|record| match &record.event {
            HarnessEvent::ApprovalGranted { actor_id, .. } => Some(ApprovalSnapshot::Granted {
                actor: actor_id.to_string(),
            }),
            HarnessEvent::ApprovalDenied {
                actor_id, reason, ..
            } => Some(ApprovalSnapshot::Denied {
                actor: actor_id.to_string(),
                reason: reason.clone(),
            }),
            _ => None,
        })
        .collect()
}

fn assert_approval_transport(
    scenario: Scenario,
    direct: &[RecordedEvent],
    daemon: &[RecordedEvent],
) {
    match scenario.approval() {
        ScenarioApproval::None => {
            assert!(approval_snapshots(direct).is_empty());
            assert!(approval_snapshots(daemon).is_empty());
        }
        ScenarioApproval::Grant => {
            assert_eq!(
                approval_snapshots(direct),
                [ApprovalSnapshot::Granted {
                    actor: "stdin".into()
                }]
            );
            assert_eq!(
                approval_snapshots(daemon),
                [ApprovalSnapshot::Granted {
                    actor: "daemon".into()
                }]
            );
        }
        ScenarioApproval::Deny => {
            assert_eq!(
                approval_snapshots(direct),
                [ApprovalSnapshot::Denied {
                    actor: "stdin".into(),
                    reason: DENIAL_REASON.into()
                }]
            );
            assert_eq!(
                approval_snapshots(daemon),
                [ApprovalSnapshot::Denied {
                    actor: "daemon".into(),
                    reason: DENIAL_REASON.into()
                }]
            );
        }
    }
}

fn assert_policy_and_effect(scenario: Scenario, records: &[RecordedEvent]) {
    let (effect, decision) = records
        .iter()
        .find_map(|record| match &record.event {
            HarnessEvent::ToolCallProposed { call, .. } => Some(call.effect.clone()),
            _ => None,
        })
        .zip(records.iter().find_map(|record| match &record.event {
            HarnessEvent::PolicyEvaluated { decision, .. } => Some(decision),
            _ => None,
        }))
        .unwrap();
    match scenario {
        Scenario::ReadOnly => {
            assert_eq!(effect, EffectClass::ReadOnly);
            assert!(matches!(decision, PolicyDecision::Allow));
        }
        Scenario::ApprovedWrite | Scenario::DeniedWrite => {
            assert_eq!(effect, EffectClass::WorkspaceWrite);
            assert!(matches!(decision, PolicyDecision::RequireApproval { .. }));
        }
    }
}

fn set_approval_actor(records: &mut [RecordedEvent], actor: &str) {
    for record in records {
        match &mut record.event {
            HarnessEvent::ApprovalGranted { actor_id, .. }
            | HarnessEvent::ApprovalDenied { actor_id, .. } => {
                *actor_id = ActorId::new(actor).unwrap();
            }
            _ => {}
        }
    }
}

struct ProofContext {
    _root: Arc<tempfile::TempDir>,
    workspace: PathBuf,
    config_path: PathBuf,
    fixture_path: PathBuf,
    socket_path: PathBuf,
    ledger_path: PathBuf,
    /// The host's server-wide store, where thread state lives.
    server_db_path: PathBuf,
    #[cfg(unix)]
    runtime_root: PathBuf,
    #[cfg(unix)]
    state_root: PathBuf,
    #[cfg(windows)]
    local_app_data: PathBuf,
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
        let workspace_id = paths::workspace_id(&workspace).unwrap();

        #[cfg(unix)]
        let (socket_path, ledger_path, runtime_root, state_root) = {
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
            (
                runtime_root
                    .join("platonic")
                    .join("workspaces")
                    .join(&workspace_id)
                    .join("agent.sock"),
                state_root
                    .join("platonic")
                    .join("workspaces")
                    .join(&workspace_id)
                    .join("agent.db"),
                runtime_root,
                state_root,
            )
        };
        #[cfg(unix)]
        assert!(
            socket_path.as_os_str().len() < 100,
            "socket path must stay under the sockaddr_un limit: {}",
            socket_path.display()
        );

        #[cfg(windows)]
        let (socket_path, ledger_path, local_app_data) = {
            let local_app_data = root.path().join("local-app-data");
            (
                PathBuf::from(format!(r"\\.\pipe\plato-agent-{workspace_id}")),
                local_app_data
                    .join("platonic")
                    .join("workspaces")
                    .join(&workspace_id)
                    .join("agent.db"),
                local_app_data,
            )
        };

        // The server store sits beside the workspaces directory, not inside
        // any one workspace.
        #[cfg(unix)]
        let server_db_path = state_root.join("platonic").join("server.db");
        #[cfg(windows)]
        let server_db_path = local_app_data.join("platonic").join("server.db");

        Self {
            config_path: workspace.join("plato.toml"),
            fixture_path: workspace.join("fixture.txt"),
            _root: root,
            workspace,
            socket_path,
            ledger_path,
            server_db_path,
            #[cfg(unix)]
            runtime_root,
            #[cfg(unix)]
            state_root,
            #[cfg(windows)]
            local_app_data,
        }
    }

    fn host_socket_path(&self) -> PathBuf {
        #[cfg(unix)]
        {
            self.runtime_root
                .join("platonic")
                .join("host")
                .join("agent.sock")
        }
        #[cfg(windows)]
        {
            PathBuf::from(r"\\.\pipe\plato-agent-host")
        }
    }

    fn host_lock_path(&self) -> PathBuf {
        #[cfg(unix)]
        let runtime_root = &self.runtime_root;
        #[cfg(windows)]
        let runtime_root = &self.local_app_data;
        runtime_root
            .join("platonic")
            .join("host")
            .join("agent.lock")
    }

    fn lock_path(&self) -> PathBuf {
        let workspace_id = paths::workspace_id(&self.workspace).unwrap();
        #[cfg(unix)]
        let runtime_root = &self.runtime_root;
        #[cfg(windows)]
        let runtime_root = &self.local_app_data;
        runtime_root
            .join("platonic")
            .join("workspaces")
            .join(workspace_id)
            .join("agent.lock")
    }

    fn restore_fixture(&self) {
        fs::write(&self.fixture_path, FIXTURE_INITIAL).unwrap();
    }

    fn apply_environment(&self, command: &mut Command) {
        #[cfg(unix)]
        command
            .env("XDG_RUNTIME_DIR", &self.runtime_root)
            .env("XDG_STATE_HOME", &self.state_root);
        #[cfg(windows)]
        command.env("LOCALAPPDATA", &self.local_app_data);
        command
            .env(API_KEY_ENV, "test-key")
            .env("PLATO_CONFIG", &self.config_path);
    }

    fn plato_command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_plato"));
        command.current_dir(&self.workspace);
        self.apply_environment(&mut command);
        command
    }

    fn daemon_command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_plato-agentd"));
        command
            .arg("--workspace")
            .arg(&self.workspace)
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        self.apply_environment(&mut command);
        command
    }

    fn host_daemon_command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_plato-agentd"));
        command
            .arg("--host")
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        self.apply_environment(&mut command);
        command
    }

    fn assert_no_serving_daemon(&self, stage: &str) {
        assert!(
            DaemonClient::connect_with_timeout(&self.socket_path, Duration::from_millis(100))
                .is_err(),
            "{stage}: unique workspace unexpectedly had a serving daemon"
        );
        #[cfg(unix)]
        assert!(
            !self.socket_path.exists(),
            "{stage}: daemon socket unexpectedly exists"
        );
    }
}

struct ProofDaemon {
    child: Option<Child>,
    workspace: PathBuf,
    socket_path: PathBuf,
}

impl ProofDaemon {
    fn start(proof: &ProofContext) -> Self {
        let mut child = proof.daemon_command().spawn().unwrap();
        wait_for_daemon(&proof.socket_path, &proof.workspace, &mut child);
        Self {
            child: Some(child),
            workspace: proof.workspace.clone(),
            socket_path: proof.socket_path.clone(),
        }
    }

    fn start_host(proof: &ProofContext) -> Self {
        let socket_path = proof.host_socket_path();
        let mut child = proof.host_daemon_command().spawn().unwrap();
        wait_for_endpoint(&socket_path, &mut child);
        Self {
            child: Some(child),
            workspace: proof.workspace.clone(),
            socket_path,
        }
    }

    fn connect(&self) -> DaemonClient {
        self.connect_workspace(&self.workspace)
    }

    fn connect_workspace(&self, workspace: &Path) -> DaemonClient {
        let mut client =
            DaemonClient::connect_with_timeout(&self.socket_path, Duration::from_secs(2)).unwrap();
        client.hello(workspace).unwrap();
        client
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

fn wait_for_daemon(socket_path: &Path, workspace: &Path, child: &mut Child) {
    let deadline = Instant::now() + PROOF_TIMEOUT;
    loop {
        if let Ok(mut client) =
            DaemonClient::connect_with_timeout(socket_path, Duration::from_millis(200))
            && client.hello(workspace).is_ok()
        {
            return;
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!(
                "daemon exited before serving ({status}): {}",
                read_pipe(child.stderr.take())
            );
        }
        assert!(Instant::now() < deadline, "daemon did not start");
        thread::sleep(POLL_INTERVAL);
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

#[cfg(windows)]
fn platform_direct_children(parent: u32) -> HashSet<u32> {
    windows_processes()
        .into_iter()
        .filter_map(|(pid, process_parent)| (process_parent == parent).then_some(pid))
        .collect()
}

#[cfg(windows)]
fn kill_process_exact(pid: u32) {
    windows_process_proof::kill(pid).unwrap();
}

#[cfg(windows)]
fn wait_for_platform_process_absence(pid: u32) {
    let deadline = Instant::now() + PROOF_TIMEOUT;
    while windows_processes()
        .into_iter()
        .any(|(current, _)| current == pid)
    {
        assert!(
            Instant::now() < deadline,
            "run child process {pid} remained after lifecycle cleanup"
        );
        thread::yield_now();
    }
}

#[cfg(windows)]
fn windows_processes() -> Vec<(u32, u32)> {
    windows_process_proof::list().unwrap()
}

#[cfg(windows)]
mod windows_process_proof {
    #![allow(unsafe_code)]

    use std::{
        io, mem,
        os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle},
    };
    use windows_sys::Win32::{
        Foundation::{ERROR_NO_MORE_FILES, INVALID_HANDLE_VALUE},
        System::{
            Diagnostics::ToolHelp::{
                CreateToolhelp32Snapshot, PROCESSENTRY32W, Process32FirstW, Process32NextW,
                TH32CS_SNAPPROCESS,
            },
            Threading::{OpenProcess, PROCESS_TERMINATE, TerminateProcess},
        },
    };

    pub(super) fn list() -> io::Result<Vec<(u32, u32)>> {
        // SAFETY: the snapshot handle is checked before ownership is assumed.
        let raw = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0) };
        if raw == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: CreateToolhelp32Snapshot returned a new owned handle.
        let snapshot = unsafe { OwnedHandle::from_raw_handle(raw) };
        let mut entry = PROCESSENTRY32W {
            dwSize: mem::size_of::<PROCESSENTRY32W>()
                .try_into()
                .expect("PROCESSENTRY32W size fits u32"),
            ..Default::default()
        };
        // SAFETY: snapshot is live and entry is initialized writable storage.
        if unsafe { Process32FirstW(snapshot.as_raw_handle(), &mut entry) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let mut processes = Vec::new();
        loop {
            processes.push((entry.th32ProcessID, entry.th32ParentProcessID));
            // SAFETY: snapshot and entry remain live for enumeration.
            if unsafe { Process32NextW(snapshot.as_raw_handle(), &mut entry) } == 0 {
                let error = io::Error::last_os_error();
                return if error.raw_os_error() == Some(ERROR_NO_MORE_FILES as i32) {
                    Ok(processes)
                } else {
                    Err(error)
                };
            }
        }
    }

    pub(super) fn kill(pid: u32) -> io::Result<()> {
        // SAFETY: the returned process handle is checked before ownership is assumed.
        let raw = unsafe { OpenProcess(PROCESS_TERMINATE, 0, pid) };
        if raw.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: OpenProcess returned a new owned handle.
        let process = unsafe { OwnedHandle::from_raw_handle(raw) };
        // SAFETY: process is live and was opened with PROCESS_TERMINATE.
        if unsafe { TerminateProcess(process.as_raw_handle(), 137) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

fn read_pipe(pipe: Option<impl Read>) -> String {
    let mut output = String::new();
    if let Some(mut pipe) = pipe {
        pipe.read_to_string(&mut output).unwrap();
    }
    output
}

fn assert_success(label: &str, output: &Output) {
    assert!(
        output.status.success(),
        "{label} failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn output_field(output: &str, prefix: &str) -> String {
    output
        .lines()
        .find_map(|line| line.strip_prefix(prefix))
        .unwrap_or_else(|| panic!("missing {prefix:?} in output:\n{output}"))
        .to_owned()
}

#[derive(Clone, Copy, Debug)]
enum UsageFixture {
    Known(u32, u32),
    Unknown,
}

impl UsageFixture {
    fn is_known(self) -> bool {
        matches!(self, Self::Known(..))
    }
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
    if let UsageFixture::Known(prompt_tokens, completion_tokens) = usage {
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
    body.push_str("data: [DONE]\n\n");
    body
}

struct ScriptedProvider {
    base_url: String,
    handle: Option<thread::JoinHandle<Vec<Value>>>,
}

struct ControlledThreadProvider {
    base_url: String,
    first_request: mpsc::Receiver<Value>,
    second_request: mpsc::Receiver<Value>,
    release_first: mpsc::Sender<()>,
    release_second: mpsc::Sender<()>,
    handle: Option<thread::JoinHandle<Vec<Value>>>,
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

impl ScriptedProvider {
    fn start(replies: Vec<ProviderReply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let handle = thread::spawn(move || {
            replies
                .into_iter()
                .map(|reply| {
                    let mut stream = accept_before(&listener, Instant::now() + PROOF_TIMEOUT);
                    let request = read_http_request(&mut stream);
                    write_http_response(&mut stream, &reply.body);
                    request
                })
                .collect()
        });
        Self {
            base_url,
            handle: Some(handle),
        }
    }

    fn join(mut self) -> Vec<Value> {
        self.handle.take().unwrap().join().unwrap()
    }
}

#[cfg(any(target_os = "linux", windows))]
struct KillIsolationProvider {
    base_url: String,
    first_requested: mpsc::Receiver<()>,
    second_requested: mpsc::Receiver<()>,
    release_second: mpsc::Sender<()>,
    handle: thread::JoinHandle<()>,
}

#[cfg(any(target_os = "linux", windows))]
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

struct CancelableProvider {
    base_url: String,
    requested: mpsc::Receiver<usize>,
    release: mpsc::Sender<()>,
    handle: thread::JoinHandle<()>,
}

struct FailureProvider {
    base_url: String,
    handle: thread::JoinHandle<usize>,
}

impl FailureProvider {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let handle = thread::spawn(move || {
            for _ in 0..2 {
                let mut stream = accept_before(&listener, Instant::now() + PROOF_TIMEOUT);
                let _ = read_http_request(&mut stream);
                write!(
                    stream,
                    "HTTP/1.1 500 Internal Server Error\r\ncontent-type: text/plain\r\ncontent-length: 16\r\nconnection: close\r\n\r\nscripted failure"
                )
                .unwrap();
            }
            2
        });
        Self { base_url, handle }
    }

    fn join(self) -> usize {
        self.handle.join().unwrap()
    }
}

impl CancelableProvider {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let (requested_sender, requested) = mpsc::channel();
        let (release, release_receiver) = mpsc::channel();
        let handle = thread::spawn(move || {
            for index in 0..2 {
                let mut stream = accept_before(&listener, Instant::now() + PROOF_TIMEOUT);
                let _ = read_http_request(&mut stream);
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: 1048576\r\nconnection: close\r\n\r\n"
                )
                .unwrap();
                stream.flush().unwrap();
                requested_sender.send(index).unwrap();
                release_receiver.recv_timeout(PROOF_TIMEOUT).unwrap();
            }
        });
        Self {
            base_url,
            requested,
            release,
            handle,
        }
    }

    fn join(self) {
        self.handle.join().unwrap();
    }
}

impl Drop for ScriptedProvider {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
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

#[derive(Clone, Copy)]
enum SampleApproval {
    Granted,
    Denied,
}

fn sample_evidence(approval: SampleApproval) -> RunEvidence {
    let run_id = RunId::new("run_direct").unwrap();
    let first_turn = TurnId::new("turn_1").unwrap();
    let second_turn = TurnId::new("turn_2").unwrap();
    let call_id = ToolCallId::new("call_1").unwrap();
    let tool = ToolName::new("file.write").unwrap();
    let input = json!({"path": "fixture.txt", "content": FIXTURE_WRITTEN});
    let call = ToolCall {
        id: call_id.clone(),
        tool: tool.clone(),
        effect: EffectClass::WorkspaceWrite,
        input: input.clone(),
    };
    let mut events = vec![
        HarnessEvent::RunStarted {
            run_id: run_id.clone(),
            agent_id: AgentId::new("plato").unwrap(),
        },
        HarnessEvent::ContextBuilt {
            run_id: run_id.clone(),
            turn_id: first_turn.clone(),
            context: ContextPack {
                token_budget: 100,
                fragments: vec![],
            },
        },
        HarnessEvent::ModelRequested {
            run_id: run_id.clone(),
            turn_id: first_turn.clone(),
            step: 0,
            model: ModelName::new("test-model").unwrap(),
        },
        HarnessEvent::ModelResponded {
            run_id: run_id.clone(),
            turn_id: first_turn.clone(),
            step: 0,
            output: Message {
                role: MessageRole::Assistant,
                content: String::new(),
            },
            proposed_calls: vec![ToolProposal {
                tool: tool.clone(),
                input: input.clone(),
            }],
            served_model: Some(ModelName::new(SERVED_MODEL).unwrap()),
            usage: Some(ModelUsage {
                input_tokens: 10,
                output_tokens: 2,
            }),
        },
        HarnessEvent::ToolCallProposed {
            run_id: run_id.clone(),
            turn_id: first_turn,
            call,
        },
        HarnessEvent::PolicyEvaluated {
            run_id: run_id.clone(),
            call_id: call_id.clone(),
            decision: PolicyDecision::RequireApproval {
                reason: "mutable or networked tool call requires explicit policy allowance".into(),
            },
        },
    ];
    match approval {
        SampleApproval::Granted => {
            events.extend([
                HarnessEvent::ApprovalGranted {
                    run_id: run_id.clone(),
                    call_id: call_id.clone(),
                    actor_id: ActorId::new("stdin").unwrap(),
                },
                HarnessEvent::ToolStarted {
                    run_id: run_id.clone(),
                    call_id: call_id.clone(),
                },
                HarnessEvent::ToolFinished {
                    run_id: run_id.clone(),
                    result: ToolResult {
                        call_id,
                        summary: "wrote 16 bytes to fixture.txt".into(),
                        data: json!({"path": "fixture.txt", "bytes": 16}),
                        artifacts: vec![],
                        visibility: ResultVisibility::Both,
                    },
                },
            ]);
        }
        SampleApproval::Denied => {
            events.push(HarnessEvent::ApprovalDenied {
                run_id: run_id.clone(),
                call_id,
                actor_id: ActorId::new("stdin").unwrap(),
                reason: DENIAL_REASON.into(),
            });
        }
    }
    events.extend([
        HarnessEvent::ContextBuilt {
            run_id: run_id.clone(),
            turn_id: second_turn.clone(),
            context: ContextPack {
                token_budget: 100,
                fragments: vec![],
            },
        },
        HarnessEvent::ModelRequested {
            run_id: run_id.clone(),
            turn_id: second_turn.clone(),
            step: 1,
            model: ModelName::new("test-model").unwrap(),
        },
        HarnessEvent::ModelResponded {
            run_id: run_id.clone(),
            turn_id: second_turn,
            step: 1,
            output: Message {
                role: MessageRole::Assistant,
                content: "done".into(),
            },
            proposed_calls: vec![],
            served_model: Some(ModelName::new(SERVED_MODEL).unwrap()),
            usage: Some(ModelUsage {
                input_tokens: 20,
                output_tokens: 4,
            }),
        },
        HarnessEvent::RunFinished { run_id },
    ]);
    let records = events
        .into_iter()
        .enumerate()
        .map(|(seq, event)| RecordedEvent {
            seq: seq as u64,
            occurred_at_ms: 1_000 + seq as u64,
            event,
        })
        .collect::<Vec<_>>();
    RunEvidence::from_leg(records, "done".into(), b"fixture changed\n".to_vec())
        .with_provider_requests(vec![json!({"model": "test-model"})])
}
