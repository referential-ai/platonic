#![cfg(windows)]
#![allow(unsafe_code)]

use interprocess::{
    local_socket::{GenericFilePath, ListenerOptions, Stream, prelude::*},
    os::windows::{local_socket::ListenerOptionsExt, security_descriptor::SecurityDescriptor},
};
use plato_agent::{
    daemon::{
        client::DaemonClient,
        installer_gate::InstallerStartupGate,
        protocol::{RunStateName, ShutdownIfIdleResultName, StreamEvent},
        server::DaemonServer,
    },
    paths,
};
use serde_json::json;
use std::{
    env,
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Read, Write},
    os::windows::{ffi::OsStrExt, process::CommandExt},
    path::Path,
    process::{Child, Command, ExitStatus, Stdio},
    ptr,
    sync::{
        Arc,
        atomic::AtomicBool,
        mpsc::{self, Receiver, SyncSender},
    },
    thread,
    time::{Duration, Instant},
};
use widestring::U16CString;
use windows_sys::Win32::{
    Foundation::{CloseHandle, HANDLE, WAIT_OBJECT_0, WAIT_TIMEOUT},
    Security::{
        GetTokenInformation, ImpersonateLoggedOnUser, LOGON32_LOGON_NETWORK,
        LOGON32_PROVIDER_DEFAULT, LogonUserW, RevertToSelf, SECURITY_IMPERSONATION_LEVEL,
        SecurityIdentification, TOKEN_QUERY, TokenImpersonationLevel,
    },
    System::{
        Console::{
            ATTACH_PARENT_PROCESS, AttachConsole, CTRL_BREAK_EVENT, CTRL_C_EVENT, FreeConsole,
            GenerateConsoleCtrlEvent, SetConsoleCtrlHandler,
        },
        Threading::{
            CREATE_NEW_CONSOLE, CREATE_NEW_PROCESS_GROUP, CREATE_NO_WINDOW,
            CreateProcessWithLogonW, GetCurrentThread, GetExitCodeProcess, OpenThreadToken,
            PROCESS_INFORMATION, STARTUPINFOW, TerminateProcess, WaitForSingleObject,
        },
    },
};

const PROOF_TIMEOUT: Duration = Duration::from_secs(15);

#[test]
#[ignore = "holds the process-global installer gate; run serially"]
fn installer_gate_refuses_daemon_before_endpoint_or_lock_creation() {
    let gate = InstallerStartupGate::acquire().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let lock_path = paths::default_lock_path(workspace.path()).unwrap();
    let socket_path = paths::default_socket_path(workspace.path()).unwrap();

    let started = Instant::now();
    let output = Command::new(env!("CARGO_BIN_EXE_plato-agentd"))
        .arg("--workspace")
        .arg(workspace.path())
        .creation_flags(CREATE_NO_WINDOW)
        .output()
        .unwrap();

    assert!(started.elapsed() < Duration::from_secs(3));
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("Plato Agent installation or update is in progress")
    );
    assert!(!lock_path.exists());
    assert!(DaemonClient::connect(&socket_path).is_err());
    drop(gate);
}

#[test]
#[ignore = "requires PLATO_WINDOWS_SECOND_USER and PLATO_WINDOWS_SECOND_PASSWORD"]
fn installer_gate_is_isolated_per_current_user() {
    let username = env::var("PLATO_WINDOWS_SECOND_USER").unwrap();
    let password = env::var("PLATO_WINDOWS_SECOND_PASSWORD").unwrap();
    let public = env::var_os("PUBLIC").expect("PUBLIC is required for the cross-user proof");
    let shared = tempfile::Builder::new()
        .prefix("plato-149-")
        .tempdir_in(public)
        .unwrap();
    let grant = Command::new("icacls.exe")
        .arg(shared.path())
        .args(["/grant", &format!("{username}:(OI)(CI)M")])
        .output()
        .unwrap();
    assert!(
        grant.status.success(),
        "failed to grant helper directory access: {}",
        String::from_utf8_lossy(&grant.stderr)
    );
    let helper = shared.path().join("plato-installer-gate-helper.exe");
    fs::copy(env::current_exe().unwrap(), &helper).unwrap();

    let gate = InstallerStartupGate::acquire().unwrap();
    let mut child = LoggedOnProcess::spawn_test(
        &username,
        &password,
        &helper,
        shared.path(),
        "installer_gate_second_user_child",
    )
    .unwrap();
    assert_eq!(child.wait_bounded(PROOF_TIMEOUT).unwrap(), 0);
    drop(gate);
}

#[test]
#[ignore = "child process for the cross-user installer-gate proof"]
fn installer_gate_second_user_child() {
    drop(InstallerStartupGate::acquire().unwrap());
}

#[test]
fn daemon_round_trip_streams_and_replays_after_clean_shutdown() {
    let provider = FakeProvider::start("Windows reply");
    let workspace = tempfile::tempdir().unwrap();
    let config_path = workspace.path().join("plato.toml");
    write_provider_config(&config_path, &provider.base_url, 15_000);

    let server = DaemonServer::bind(workspace.path(), None).unwrap();
    let paths = server.paths().clone();
    let shutdown = Arc::new(AtomicBool::new(false));
    let handle = thread::spawn(move || server.serve_forever(shutdown));

    let mut client = connect_bounded(&paths.socket_path);
    let hello = client.hello(workspace.path()).unwrap();
    assert_eq!(hello.workspace_id, paths.workspace_id);
    assert_eq!(hello.ledger_path, paths.ledger_path.to_string_lossy());

    let started = client
        .run_start(
            "prove Windows transport".into(),
            Some(config_path.to_string_lossy().into_owned()),
            false,
        )
        .unwrap();
    assert_eq!(started.status, RunStateName::Running);

    let deadline = Instant::now() + PROOF_TIMEOUT;
    let mut offset = Some(0);
    let mut saw_delta = false;
    loop {
        let page = client.events_stream(&started.run_id, offset, 128).unwrap();
        assert_eq!(page.run_id, started.run_id);
        saw_delta |= page.events.iter().any(|entry| {
            matches!(
                &entry.event,
                StreamEvent::AssistantDelta { run_id, text, .. }
                    if run_id == &started.run_id && text == "Windows reply"
            )
        });
        offset = Some(page.next_offset);
        if page.status == RunStateName::Finished {
            break;
        }
        assert!(Instant::now() < deadline, "Windows run did not finish");
        thread::sleep(Duration::from_millis(20));
    }
    assert!(saw_delta, "live exact-run delta was not observed");

    let transcript = client.transcript_read(&started.run_id).unwrap();
    assert_eq!(transcript.run_id, started.run_id);
    assert_eq!(transcript.status, RunStateName::Finished);
    assert_eq!(transcript.final_answer.as_deref(), Some("Windows reply"));
    assert_eq!(transcript.typed.unwrap().runs[0].run_id, started.run_id);

    let result = client.shutdown_if_idle().unwrap();
    assert_eq!(result.result, ShutdownIfIdleResultName::Shutdown);
    drop(client);
    handle.join().unwrap().unwrap();
    provider.join();

    assert!(!paths.lock_path.exists());
    assert!(DaemonClient::connect(&paths.socket_path).is_err());

    let replay = Command::new(env!("CARGO_BIN_EXE_plato"))
        .current_dir(workspace.path())
        .arg("replay")
        .arg(format!("--db={}", paths.ledger_path.display()))
        .arg("--run")
        .arg(&started.run_id)
        .output()
        .unwrap();
    assert!(
        replay.status.success(),
        "offline replay failed: {}",
        String::from_utf8_lossy(&replay.stderr)
    );
    assert!(String::from_utf8_lossy(&replay.stdout).contains("Windows reply"));
}

#[test]
fn ctrl_break_stops_daemon_and_removes_lock() {
    let workspace = tempfile::tempdir().unwrap();
    let lock_path = paths::default_lock_path(workspace.path()).unwrap();
    let socket_path = paths::default_socket_path(workspace.path()).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_plato-agentd"))
        .arg("--workspace")
        .arg(workspace.path())
        .creation_flags(CREATE_NEW_PROCESS_GROUP)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    wait_for_path(&lock_path, &mut child);
    let mut client = connect_bounded(&socket_path);
    client.hello(workspace.path()).unwrap();
    drop(client);
    assert_ne!(
        unsafe { GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, child.id()) },
        0
    );
    let status = wait_bounded(&mut child, PROOF_TIMEOUT).unwrap();
    assert!(status.success());
    assert!(!lock_path.exists());
    assert!(DaemonClient::connect(&socket_path).is_err());
}

#[test]
fn child_kill_removes_lock_and_allows_immediate_same_workspace_restart() {
    let local_app_data = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let mut first = ProofDaemon::spawn(workspace.path(), local_app_data.path());
    let lock_path = first.lock_path.clone();

    first.child.kill().unwrap();
    first.child.wait().unwrap();

    assert!(!lock_path.exists());
    let mut restarted = ProofDaemon::spawn(workspace.path(), local_app_data.path());
    assert_eq!(restarted.lock_path, lock_path);

    let mut client = connect_bounded(&restarted.socket_path);
    client.hello(workspace.path()).unwrap();
    assert_eq!(
        client.shutdown_if_idle().unwrap().result,
        ShutdownIfIdleResultName::Shutdown
    );
    drop(client);
    assert!(restarted.wait_for_exit().success());
    assert!(!lock_path.exists());
}

#[test]
#[ignore = "temporarily reattaches the test process to the daemon console"]
fn ctrl_c_stops_daemon_and_removes_lock() {
    let workspace = tempfile::tempdir().unwrap();
    let lock_path = paths::default_lock_path(workspace.path()).unwrap();
    let socket_path = paths::default_socket_path(workspace.path()).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_plato-agentd"))
        .arg("--workspace")
        .arg(workspace.path())
        .creation_flags(CREATE_NEW_CONSOLE)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    wait_for_path(&lock_path, &mut child);
    let mut client = connect_bounded(&socket_path);
    client.hello(workspace.path()).unwrap();
    drop(client);
    {
        let _console = ChildConsole::attach(child.id()).unwrap();
        assert_ne!(unsafe { GenerateConsoleCtrlEvent(CTRL_C_EVENT, 0) }, 0);
        thread::sleep(Duration::from_millis(100));
    }
    let status = wait_bounded(&mut child, PROOF_TIMEOUT).unwrap();
    assert!(status.success());
    assert!(!lock_path.exists());
    assert!(DaemonClient::connect(&socket_path).is_err());
}

#[test]
#[ignore = "native Windows installer-control proof"]
fn installer_control_preflights_refuses_active_and_retries_after_terminal() {
    let local_app_data = tempfile::tempdir().unwrap();
    let idle_workspace = tempfile::tempdir().unwrap();
    let active_workspace = tempfile::tempdir().unwrap();
    let idle_marker = idle_workspace.path().join("user-data.txt");
    let active_marker = active_workspace.path().join("user-data.txt");
    fs::write(&idle_marker, b"idle user data").unwrap();
    fs::write(&active_marker, b"active user data").unwrap();

    let missing = shutdown_target(local_app_data.path(), idle_workspace.path());
    assert!(
        missing.status.success(),
        "missing target failed: {}",
        missing.stderr
    );
    assert_eq!(
        parse_ndjson(&missing.stdout),
        vec![json!({
            "kind": "shutdown",
            "workspace_root": idle_workspace.path().canonicalize().unwrap().to_string_lossy(),
            "result": "not_running",
        })]
    );

    let mut idle = ProofDaemon::spawn(idle_workspace.path(), local_app_data.path());
    let mut active = ProofDaemon::spawn(active_workspace.path(), local_app_data.path());
    let listed = list_workspaces(local_app_data.path());
    assert!(
        listed.status.success(),
        "list-workspaces failed: {}",
        listed.stderr
    );
    let mut actual = parse_ndjson(&listed.stdout);
    actual.sort_by_key(|record| record["workspace_id"].as_str().unwrap().to_string());
    let mut expected = vec![idle.workspace_record(), active.workspace_record()];
    expected.sort_by_key(|record| record["workspace_id"].as_str().unwrap().to_string());
    assert_eq!(actual, expected);

    let unrelated_workspace = tempfile::tempdir().unwrap();
    let unrelated_root = unrelated_workspace.path().canonicalize().unwrap();
    let unrelated_id = paths::workspace_id(&unrelated_root).unwrap();
    let unrelated_lock = local_app_data
        .path()
        .join("plato-agent/workspaces")
        .join(&unrelated_id)
        .join("agent.lock");
    fs::create_dir_all(unrelated_lock.parent().unwrap()).unwrap();
    let ping = Path::new(&env::var_os("SystemRoot").unwrap())
        .join("System32/ping.exe")
        .canonicalize()
        .unwrap();
    let mut unrelated = Command::new(&ping)
        .args(["-t", "127.0.0.1"])
        .creation_flags(CREATE_NO_WINDOW)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let unrelated_metadata = json!({
        "v": 1,
        "pid": unrelated.id(),
        "executable": ping.to_string_lossy(),
        "workspace_root": unrelated_root.to_string_lossy(),
        "workspace_id": unrelated_id,
        "socket_path": r"\\.\pipe\unrelated-process",
    });
    fs::write(
        &unrelated_lock,
        format!("{unrelated_metadata}\n").as_bytes(),
    )
    .unwrap();
    let unrelated_bytes = fs::read(&unrelated_lock).unwrap();

    let unrelated_list = list_workspaces(local_app_data.path());
    assert!(
        unrelated_list.status.success(),
        "unrelated process was not positively classified: {}",
        unrelated_list.stderr
    );
    let unrelated_records = parse_ndjson(&unrelated_list.stdout);
    assert_eq!(
        unrelated_records
            .iter()
            .filter(|record| record["kind"] == "workspace")
            .count(),
        2
    );
    let unrelated_record = unrelated_records
        .iter()
        .find(|record| record["kind"] == "unrelated")
        .unwrap_or_else(|| panic!("missing unrelated record: {unrelated_records:?}"));
    assert_eq!(unrelated_record["pid"], unrelated.id());
    let reported_lock = Path::new(unrelated_record["lock_path"].as_str().unwrap());
    assert_eq!(
        reported_lock.canonicalize().unwrap(),
        unrelated_lock.canonicalize().unwrap()
    );

    unrelated.kill().unwrap();
    unrelated.wait().unwrap();
    let stale_list = list_workspaces(local_app_data.path());
    assert!(!stale_list.status.success());
    assert_eq!(
        parse_ndjson(&stale_list.stdout)
            .iter()
            .filter(|record| record["kind"] == "workspace")
            .count(),
        2
    );
    assert!(stale_list.stderr.contains("stale pid"));
    assert_eq!(fs::read(&unrelated_lock).unwrap(), unrelated_bytes);
    fs::remove_file(&unrelated_lock).unwrap();

    let targeted = shutdown_target(local_app_data.path(), idle_workspace.path());
    assert!(
        targeted.status.success(),
        "targeted shutdown failed: {}",
        targeted.stderr
    );
    assert_eq!(
        parse_ndjson(&targeted.stdout),
        vec![idle.shutdown_record("shutdown")]
    );
    assert!(idle.wait_for_exit().success());
    assert!(!idle.lock_path.exists());
    assert!(DaemonClient::connect(&idle.socket_path).is_err());
    idle = ProofDaemon::spawn(idle_workspace.path(), local_app_data.path());
    expected = vec![idle.workspace_record(), active.workspace_record()];
    expected.sort_by_key(|record| record["workspace_id"].as_str().unwrap().to_string());

    let idle_lock_before = fs::read(&idle.lock_path).unwrap();
    let active_lock_before = fs::read(&active.lock_path).unwrap();
    let invalid_lock = local_app_data
        .path()
        .join("plato-agent/workspaces/unvalidated/agent.lock");
    let invalid_bytes = b"not valid lock metadata\r\n";
    fs::create_dir_all(invalid_lock.parent().unwrap()).unwrap();
    fs::write(&invalid_lock, invalid_bytes).unwrap();

    let invalid_list = list_workspaces(local_app_data.path());
    assert!(!invalid_list.status.success());
    let mut invalid_actual = parse_ndjson(&invalid_list.stdout);
    invalid_actual.sort_by_key(|record| record["workspace_id"].as_str().unwrap().to_string());
    assert_eq!(invalid_actual, expected);
    assert_eq!(fs::read(&invalid_lock).unwrap(), invalid_bytes);
    assert_eq!(fs::read(&idle.lock_path).unwrap(), idle_lock_before);
    assert_eq!(fs::read(&active.lock_path).unwrap(), active_lock_before);

    let invalid_shutdown = shutdown_if_idle(local_app_data.path());
    assert!(!invalid_shutdown.status.success());
    assert!(
        parse_ndjson(&invalid_shutdown.stdout)
            .iter()
            .all(|record| record["kind"] != "shutdown"),
        "aggregate preflight sent a shutdown RPC: {}",
        invalid_shutdown.stdout
    );
    idle.assert_running();
    active.assert_running();
    assert_eq!(fs::read(&invalid_lock).unwrap(), invalid_bytes);
    assert_eq!(fs::read(&idle.lock_path).unwrap(), idle_lock_before);
    assert_eq!(fs::read(&active.lock_path).unwrap(), active_lock_before);
    assert_eq!(fs::read(&idle_marker).unwrap(), b"idle user data");
    assert_eq!(fs::read(&active_marker).unwrap(), b"active user data");

    fs::remove_file(&invalid_lock).unwrap();
    let provider = BlockingProvider::start("active run finished");
    let config_path = active_workspace.path().join("plato.toml");
    write_provider_config(&config_path, &provider.base_url, 60_000);
    let mut active_client = connect_bounded(&active.socket_path);
    active_client.hello(active_workspace.path()).unwrap();
    let run = active_client
        .run_start(
            "stay active during installer control".into(),
            Some(config_path.to_string_lossy().into_owned()),
            false,
        )
        .unwrap();
    assert_eq!(run.status, RunStateName::Running);
    provider.wait_until_requested();

    let refused = shutdown_if_idle(local_app_data.path());
    assert!(!refused.status.success());
    let mut records = parse_ndjson(&refused.stdout);
    records.sort_by_key(|record| record["workspace_id"].as_str().unwrap().to_string());
    let mut expected = vec![
        idle.shutdown_record("shutdown"),
        active.shutdown_record("refused_active"),
    ];
    expected.sort_by_key(|record| record["workspace_id"].as_str().unwrap().to_string());
    assert_eq!(records, expected);
    let idle_status = idle.wait_for_exit();
    assert!(idle_status.success());
    assert!(!idle.lock_path.exists());
    assert!(DaemonClient::connect(&idle.socket_path).is_err());
    active.assert_running();
    assert_eq!(fs::read(&active.lock_path).unwrap(), active_lock_before);
    assert_eq!(fs::read(&idle_marker).unwrap(), b"idle user data");
    assert_eq!(fs::read(&active_marker).unwrap(), b"active user data");

    provider.finish();
    wait_for_terminal(&mut active_client, &run.run_id);
    drop(active_client);
    assert_eq!(fs::read(&active.lock_path).unwrap(), active_lock_before);
    let retried = shutdown_if_idle(local_app_data.path());
    assert!(
        retried.status.success(),
        "shutdown retry failed: {}",
        retried.stderr
    );
    assert_eq!(
        parse_ndjson(&retried.stdout),
        vec![active.shutdown_record("shutdown")]
    );
    let active_status = active.wait_for_exit();
    assert!(active_status.success());
    assert!(!active.lock_path.exists());
    assert!(DaemonClient::connect(&active.socket_path).is_err());
    assert_eq!(fs::read(&idle_marker).unwrap(), b"idle user data");
    assert_eq!(fs::read(&active_marker).unwrap(), b"active user data");
}

#[test]
fn long_workspace_name_binds_and_removes_its_lock() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("w".repeat(200));
    fs::create_dir(&workspace).unwrap();
    let server = DaemonServer::bind(&workspace, None).unwrap();
    let paths = server.paths().clone();
    assert!(paths.workspace_id.len() <= 81);
    let shutdown = Arc::new(AtomicBool::new(false));
    let handle = thread::spawn(move || server.serve_forever(shutdown));
    let mut client = connect_bounded(&paths.socket_path);
    client.hello(&workspace).unwrap();
    assert_eq!(
        client.shutdown_if_idle().unwrap().result,
        ShutdownIfIdleResultName::Shutdown
    );
    drop(client);
    handle.join().unwrap().unwrap();
    assert!(!paths.lock_path.exists());
}

#[test]
fn client_pipe_limits_server_impersonation_to_identification() {
    let workspace = tempfile::tempdir().unwrap();
    let endpoint = paths::default_socket_path(workspace.path()).unwrap();
    let listener = hostile_listener(&endpoint);
    let workspace_id = paths::workspace_id(workspace.path()).unwrap();
    let server = thread::spawn(move || {
        let stream = listener.accept().unwrap();
        let mut request = String::new();
        BufReader::new(&stream).read_line(&mut request).unwrap();
        let request: serde_json::Value = serde_json::from_str(request.trim()).unwrap();
        let Stream::NamedPipe(stream) = stream;
        let _impersonation = stream.inner().impersonate_client().unwrap();
        assert_eq!(current_thread_impersonation_level(), SecurityIdentification);
        let response = json!({
            "v": 1,
            "id": request["id"],
            "kind": "response",
            "method": "hello",
            "result": {
                "daemon_version": "test",
                "workspace_id": workspace_id,
                "ledger_path": "test",
                "capabilities": []
            }
        });
        writeln!(&stream, "{response}").unwrap();
    });

    let mut client = DaemonClient::connect(&endpoint).unwrap();
    client.hello(workspace.path()).unwrap();
    server.join().unwrap();
}

#[test]
#[ignore = "requires PLATO_WINDOWS_SECOND_USER and PLATO_WINDOWS_SECOND_PASSWORD"]
fn production_pipe_and_lock_reject_second_user_and_remote_clients() {
    let username = env::var("PLATO_WINDOWS_SECOND_USER").unwrap();
    let password = env::var("PLATO_WINDOWS_SECOND_PASSWORD").unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let server = DaemonServer::bind(workspace.path(), None).unwrap();
    let paths = server.paths().clone();
    let shutdown = Arc::new(AtomicBool::new(false));
    let handle = thread::spawn(move || server.serve_forever(shutdown));

    assert!(File::open(&paths.lock_path).is_ok());
    {
        let _impersonation = Impersonation::start(&username, &password).unwrap();
        assert_access_denied(Stream::connect(
            paths
                .socket_path
                .as_os_str()
                .to_fs_name::<GenericFilePath>()
                .unwrap(),
        ));
        assert_access_denied(File::open(&paths.lock_path));
        assert_access_denied(OpenOptions::new().write(true).open(&paths.lock_path));
        assert_access_denied(fs::remove_file(&paths.lock_path));
    }

    prove_remote_rejection(&paths.socket_path);

    let mut client = connect_bounded(&paths.socket_path);
    client.hello(workspace.path()).unwrap();
    assert_eq!(
        client.shutdown_if_idle().unwrap().result,
        ShutdownIfIdleResultName::Shutdown
    );
    drop(client);
    handle.join().unwrap().unwrap();
    assert!(!paths.lock_path.exists());
}

#[test]
#[ignore = "requires PLATO_WINDOWS_SECOND_USER and PLATO_WINDOWS_SECOND_PASSWORD"]
fn client_rejects_second_user_prebound_pipes() {
    let username = env::var("PLATO_WINDOWS_SECOND_USER").unwrap();
    let password = env::var("PLATO_WINDOWS_SECOND_PASSWORD").unwrap();
    let public = env::var_os("PUBLIC").expect("PUBLIC is required for the cross-user proof");
    let shared = tempfile::Builder::new()
        .prefix("plato-147-")
        .tempdir_in(public)
        .unwrap();
    let grant = Command::new("icacls.exe")
        .arg(shared.path())
        .args(["/grant", &format!("{username}:(OI)(CI)M")])
        .output()
        .unwrap();
    assert!(
        grant.status.success(),
        "failed to grant helper directory access: {}",
        String::from_utf8_lossy(&grant.stderr)
    );
    let helper = shared.path().join("plato-windows-pipe-helper.exe");
    fs::copy(env::current_exe().unwrap(), &helper).unwrap();

    let available_ready = shared.path().join("hostile-available-ready");
    let mut available = LoggedOnProcess::spawn_test(
        &username,
        &password,
        &helper,
        shared.path(),
        "hostile_available_pipe_child",
    )
    .unwrap();
    wait_for_marker(&available_ready, &mut available);

    let endpoint = paths::default_socket_path(shared.path()).unwrap();
    let lock_path = paths::default_lock_path(shared.path()).unwrap();
    assert!(
        DaemonServer::bind(shared.path(), None).is_err(),
        "the legitimate daemon replaced a hostile first pipe instance"
    );
    assert!(
        !lock_path.exists(),
        "failed bind left a workspace lock behind"
    );
    let error = match DaemonClient::connect(&endpoint) {
        Ok(_) => panic!("client accepted a second-user pipe server"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("named-pipe server is not owned by the current user"),
        "unexpected hostile-server error: {error}"
    );
    assert_eq!(available.wait_bounded(PROOF_TIMEOUT).unwrap(), 0);

    let busy_ready = shared.path().join("hostile-busy-ready");
    let mut busy = LoggedOnProcess::spawn_test(
        &username,
        &password,
        &helper,
        shared.path(),
        "hostile_busy_pipe_child",
    )
    .unwrap();
    wait_for_marker(&busy_ready, &mut busy);
    let started = Instant::now();
    let error = match DaemonClient::connect(&endpoint) {
        Ok(_) => panic!("client connected to a busy second-user pipe server"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("named-pipe connection timed out"),
        "unexpected busy hostile-server error: {error}"
    );
    assert!(started.elapsed() < Duration::from_secs(3));
    assert_eq!(busy.wait_bounded(PROOF_TIMEOUT).unwrap(), 0);
}

#[test]
#[ignore = "child process for the production remote-rejection proof"]
fn remote_pipe_probe_child() {
    let path = env::var_os("PLATO_WINDOWS_REMOTE_PIPE").unwrap();
    assert_access_denied(Stream::connect(
        path.as_os_str().to_fs_name::<GenericFilePath>().unwrap(),
    ));
}

#[test]
#[ignore = "second-user child for the hostile available-pipe proof"]
fn hostile_available_pipe_child() {
    let endpoint = paths::default_socket_path(&env::current_dir().unwrap()).unwrap();
    let listener = hostile_listener(&endpoint);
    fs::write("hostile-available-ready", b"ready").unwrap();
    let stream = listener.accept().unwrap();
    let mut byte = [0; 1];
    assert_eq!((&stream).read(&mut byte).unwrap(), 0);
}

#[test]
#[ignore = "second-user child for the hostile busy-pipe proof"]
fn hostile_busy_pipe_child() {
    let endpoint = paths::default_socket_path(&env::current_dir().unwrap()).unwrap();
    let listener = hostile_listener(&endpoint);
    let endpoint_for_client = endpoint.clone();
    let (connected_tx, connected_rx) = std::sync::mpsc::sync_channel(0);
    let client = thread::spawn(move || {
        let stream = Stream::connect(
            endpoint_for_client
                .as_os_str()
                .to_fs_name::<GenericFilePath>()
                .unwrap(),
        )
        .unwrap();
        connected_tx.send(()).unwrap();
        thread::sleep(Duration::from_secs(3));
        drop(stream);
    });
    connected_rx.recv_timeout(PROOF_TIMEOUT).unwrap();
    fs::write("hostile-busy-ready", b"ready").unwrap();
    thread::sleep(Duration::from_secs(3));
    drop(listener);
    client.join().unwrap();
}

fn prove_remote_rejection(local_path: &Path) {
    let prefix = r"\\.\pipe\";
    let local = local_path.to_string_lossy();
    let name = local
        .strip_prefix(prefix)
        .expect("production endpoint must be a local named pipe");
    let computer = env::var("COMPUTERNAME").unwrap();
    let remote = format!(r"\\{computer}\pipe\{name}");
    let mut child = Command::new(env::current_exe().unwrap())
        .args([
            "--exact",
            "remote_pipe_probe_child",
            "--ignored",
            "--nocapture",
        ])
        .env("PLATO_WINDOWS_REMOTE_PIPE", remote)
        .spawn()
        .unwrap();
    let status = wait_bounded(&mut child, Duration::from_secs(10)).unwrap();
    assert!(status.success(), "remote named-pipe probe failed closed");
}

fn assert_access_denied<T>(result: io::Result<T>) {
    match result {
        Err(error) if error.raw_os_error() == Some(5) => {}
        Err(error) => panic!("expected ERROR_ACCESS_DENIED, got {error}"),
        Ok(_) => panic!("unauthorized Windows operation succeeded"),
    }
}

fn connect_bounded(path: &Path) -> DaemonClient {
    let deadline = Instant::now() + PROOF_TIMEOUT;
    loop {
        match DaemonClient::connect(path) {
            Ok(client) => return client,
            Err(error) => {
                assert!(
                    Instant::now() < deadline,
                    "daemon did not accept clients: {error}"
                );
                thread::sleep(Duration::from_millis(20));
            }
        }
    }
}

fn wait_for_path(path: &Path, child: &mut Child) {
    let deadline = Instant::now() + PROOF_TIMEOUT;
    loop {
        if path.exists() {
            return;
        }
        if let Some(status) = child.try_wait().unwrap() {
            let mut stderr = String::new();
            child
                .stderr
                .take()
                .unwrap()
                .read_to_string(&mut stderr)
                .unwrap();
            panic!("daemon exited before creating its lock ({status}): {stderr}");
        }
        assert!(Instant::now() < deadline, "daemon did not create its lock");
        thread::sleep(Duration::from_millis(20));
    }
}

fn wait_bounded(child: &mut Child, timeout: Duration) -> io::Result<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            child.kill()?;
            let _ = child.wait();
            return Err(io::Error::new(io::ErrorKind::TimedOut, "child timed out"));
        }
        thread::sleep(Duration::from_millis(20));
    }
}

struct ProofDaemon {
    child: Child,
    workspace_root: std::path::PathBuf,
    workspace_id: String,
    socket_path: std::path::PathBuf,
    lock_path: std::path::PathBuf,
}

impl ProofDaemon {
    fn spawn(workspace_root: &Path, local_app_data: &Path) -> Self {
        let workspace_root = workspace_root.canonicalize().unwrap();
        let workspace_id = paths::workspace_id(&workspace_root).unwrap();
        let socket_path = std::path::PathBuf::from(format!(r"\\.\pipe\plato-agent-{workspace_id}"));
        let lock_path = local_app_data
            .join("plato-agent")
            .join("workspaces")
            .join(&workspace_id)
            .join("agent.lock");
        let child = Command::new(env!("CARGO_BIN_EXE_plato-agentd"))
            .arg("--workspace")
            .arg(&workspace_root)
            .env("LOCALAPPDATA", local_app_data)
            .creation_flags(CREATE_NO_WINDOW)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut daemon = Self {
            child,
            workspace_root,
            workspace_id,
            socket_path,
            lock_path,
        };
        wait_for_path(&daemon.lock_path, &mut daemon.child);
        let mut client = connect_bounded(&daemon.socket_path);
        client.hello(&daemon.workspace_root).unwrap();
        daemon
    }

    fn workspace_record(&self) -> serde_json::Value {
        json!({
            "kind": "workspace",
            "workspace_root": self.workspace_root.to_string_lossy(),
            "workspace_id": self.workspace_id,
            "socket_path": self.socket_path.to_string_lossy(),
            "pid": self.child.id(),
        })
    }

    fn shutdown_record(&self, result: &str) -> serde_json::Value {
        json!({
            "kind": "shutdown",
            "workspace_root": self.workspace_root.to_string_lossy(),
            "workspace_id": self.workspace_id,
            "socket_path": self.socket_path.to_string_lossy(),
            "pid": self.child.id(),
            "result": result,
        })
    }

    fn assert_running(&mut self) {
        if let Some(status) = self.child.try_wait().unwrap() {
            let mut stderr = String::new();
            self.child
                .stderr
                .take()
                .unwrap()
                .read_to_string(&mut stderr)
                .unwrap();
            panic!("daemon exited unexpectedly ({status}): {stderr}");
        }
    }

    fn wait_for_exit(&mut self) -> ExitStatus {
        wait_bounded(&mut self.child, PROOF_TIMEOUT).unwrap()
    }
}

impl Drop for ProofDaemon {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

struct ControlOutput {
    status: ExitStatus,
    stdout: String,
    stderr: String,
}

fn list_workspaces(local_app_data: &Path) -> ControlOutput {
    let mut command = control_command(local_app_data);
    command.arg("list-workspaces");
    command_output_bounded(command)
}

fn shutdown_if_idle(local_app_data: &Path) -> ControlOutput {
    let mut command = control_command(local_app_data);
    command.arg("shutdown-if-idle");
    command_output_bounded(command)
}

fn shutdown_target(local_app_data: &Path, workspace: &Path) -> ControlOutput {
    let mut command = control_command(local_app_data);
    command
        .arg("shutdown-if-idle")
        .arg("--workspace")
        .arg(workspace);
    command_output_bounded(command)
}

fn control_command(local_app_data: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_plato-agentd"));
    command
        .arg("control")
        .env("LOCALAPPDATA", local_app_data)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

fn command_output_bounded(mut command: Command) -> ControlOutput {
    let mut child = command.spawn().unwrap();
    let status = wait_bounded(&mut child, PROOF_TIMEOUT).unwrap();
    let mut stdout = String::new();
    let mut stderr = String::new();
    child
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut stdout)
        .unwrap();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();
    ControlOutput {
        status,
        stdout,
        stderr,
    }
}

fn parse_ndjson(raw: &str) -> Vec<serde_json::Value> {
    raw.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn wait_for_terminal(client: &mut DaemonClient, run_id: &str) {
    let deadline = Instant::now() + PROOF_TIMEOUT;
    loop {
        let transcript = client.transcript_read(run_id).unwrap();
        if transcript.status != RunStateName::Running {
            assert_eq!(transcript.status, RunStateName::Finished);
            return;
        }
        assert!(Instant::now() < deadline, "active run did not finish");
        thread::sleep(Duration::from_millis(20));
    }
}

struct FakeProvider {
    base_url: String,
    handle: thread::JoinHandle<()>,
}

impl FakeProvider {
    fn start(answer: &'static str) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            read_http_request(&mut stream);
            write_provider_answer(&mut stream, answer);
        });
        Self { base_url, handle }
    }

    fn join(self) {
        self.handle.join().unwrap();
    }
}

struct BlockingProvider {
    base_url: String,
    requested: Receiver<()>,
    release: SyncSender<()>,
    handle: thread::JoinHandle<()>,
}

impl BlockingProvider {
    fn start(answer: &'static str) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let (requested_tx, requested) = mpsc::sync_channel(0);
        let (release, release_rx) = mpsc::sync_channel(0);
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            read_http_request(&mut stream);
            requested_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            write_provider_answer(&mut stream, answer);
        });
        Self {
            base_url,
            requested,
            release,
            handle,
        }
    }

    fn wait_until_requested(&self) {
        self.requested.recv_timeout(PROOF_TIMEOUT).unwrap();
    }

    fn finish(self) {
        self.release.send(()).unwrap();
        self.handle.join().unwrap();
    }
}

fn write_provider_answer(stream: &mut std::net::TcpStream, answer: &str) {
    let content = json!({
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
    let body = format!("data: {content}\n\ndata: {finish}\n\ndata: [DONE]\n\n");
    write!(
        stream,
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
}

fn read_http_request(stream: &mut std::net::TcpStream) {
    stream.set_read_timeout(Some(PROOF_TIMEOUT)).unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut content_length = 0;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        if line == "\r\n" {
            break;
        }
        if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
            content_length = value.trim().parse().unwrap();
        }
    }
    let mut body = vec![0; content_length];
    reader.read_exact(&mut body).unwrap();
}

fn write_provider_config(path: &Path, base_url: &str, timeout_ms: u64) {
    fs::write(
        path,
        format!(
            r#"[provider]
kind = "open_ai"
model = "test-model"
api_key_env = "PATH"
base_url = "{base_url}"
timeout_ms = {timeout_ms}

[limits]
token_budget = 4000
max_output_tokens = 32
max_turns = 1

[tools]
enabled = ["file.read"]
"#
        ),
    )
    .unwrap();
}

fn hostile_listener(path: &Path) -> interprocess::local_socket::Listener {
    let descriptor = U16CString::from_str("D:P(A;;GA;;;WD)").unwrap();
    let descriptor = SecurityDescriptor::deserialize(&descriptor).unwrap();
    ListenerOptions::new()
        .name(path.as_os_str().to_fs_name::<GenericFilePath>().unwrap())
        .security_descriptor(descriptor)
        .create_sync()
        .unwrap()
}

fn wait_for_marker(path: &Path, child: &mut LoggedOnProcess) {
    let deadline = Instant::now() + PROOF_TIMEOUT;
    loop {
        if path.exists() {
            return;
        }
        if let Some(code) = child.try_wait().unwrap() {
            panic!("hostile pipe helper exited before ready with code {code}");
        }
        assert!(
            Instant::now() < deadline,
            "hostile pipe helper did not become ready"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

struct LoggedOnProcess {
    process: HANDLE,
}

impl LoggedOnProcess {
    fn spawn_test(
        username: &str,
        password: &str,
        executable: &Path,
        cwd: &Path,
        test_name: &str,
    ) -> io::Result<Self> {
        let username = wide(username);
        let domain = wide(".");
        let password = wide(password);
        let executable_wide = wide_os(executable.as_os_str());
        let cwd = wide_os(cwd.as_os_str());
        let command_line = format!(
            "\"{}\" --exact {test_name} --ignored --nocapture",
            executable.display()
        );
        let mut command_line = wide(&command_line);
        let startup = STARTUPINFOW {
            cb: std::mem::size_of::<STARTUPINFOW>()
                .try_into()
                .expect("STARTUPINFOW size fits u32"),
            ..Default::default()
        };
        let mut process = PROCESS_INFORMATION::default();
        // SAFETY: every string is NUL-terminated, command_line is writable, and both output
        // structures remain live for the call.
        if unsafe {
            CreateProcessWithLogonW(
                username.as_ptr(),
                domain.as_ptr(),
                password.as_ptr(),
                0,
                executable_wide.as_ptr(),
                command_line.as_mut_ptr(),
                CREATE_NO_WINDOW,
                ptr::null(),
                cwd.as_ptr(),
                &startup,
                &mut process,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the thread handle is independent; the process handle remains owned below.
        unsafe {
            CloseHandle(process.hThread);
        }
        Ok(Self {
            process: process.hProcess,
        })
    }

    fn try_wait(&mut self) -> io::Result<Option<u32>> {
        // SAFETY: process is a live process handle.
        match unsafe { WaitForSingleObject(self.process, 0) } {
            WAIT_OBJECT_0 => {
                let mut code = 0;
                // SAFETY: process is signaled and code is writable output storage.
                if unsafe { GetExitCodeProcess(self.process, &mut code) } == 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(Some(code))
                }
            }
            WAIT_TIMEOUT => Ok(None),
            _ => Err(io::Error::last_os_error()),
        }
    }

    fn wait_bounded(&mut self, timeout: Duration) -> io::Result<u32> {
        let timeout_ms = timeout.as_millis().clamp(1, u32::MAX as u128) as u32;
        // SAFETY: process is a live process handle.
        match unsafe { WaitForSingleObject(self.process, timeout_ms) } {
            WAIT_OBJECT_0 => self
                .try_wait()?
                .ok_or_else(|| io::Error::other("signaled process had no exit code")),
            WAIT_TIMEOUT => {
                // SAFETY: process is live and was created with termination rights.
                unsafe {
                    TerminateProcess(self.process, 1);
                    WaitForSingleObject(self.process, 5_000);
                }
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "hostile pipe helper timed out",
                ))
            }
            _ => Err(io::Error::last_os_error()),
        }
    }
}

impl Drop for LoggedOnProcess {
    fn drop(&mut self) {
        // SAFETY: process is owned here; a still-running helper is terminated before close.
        unsafe {
            if WaitForSingleObject(self.process, 0) == WAIT_TIMEOUT {
                TerminateProcess(self.process, 1);
                WaitForSingleObject(self.process, 5_000);
            }
            CloseHandle(self.process);
        }
    }
}

struct ChildConsole;

impl ChildConsole {
    fn attach(process_id: u32) -> io::Result<Self> {
        // SAFETY: detaching only changes this test process's console association.
        unsafe {
            FreeConsole();
        }
        // SAFETY: process_id names the live daemon whose dedicated console is the proof target.
        if unsafe { AttachConsole(process_id) } == 0 {
            let error = io::Error::last_os_error();
            // SAFETY: best-effort restoration of the test process's parent console.
            unsafe {
                AttachConsole(ATTACH_PARENT_PROCESS);
            }
            return Err(error);
        }
        // SAFETY: this process must ignore the broadcast Ctrl-C that it generates.
        if unsafe { SetConsoleCtrlHandler(None, 1) } == 0 {
            let error = io::Error::last_os_error();
            // SAFETY: restore the original console association before returning.
            unsafe {
                FreeConsole();
                AttachConsole(ATTACH_PARENT_PROCESS);
            }
            return Err(error);
        }
        Ok(Self)
    }
}

impl Drop for ChildConsole {
    fn drop(&mut self) {
        // SAFETY: restore this process's normal Ctrl-C handling and parent console.
        unsafe {
            FreeConsole();
            SetConsoleCtrlHandler(None, 0);
            AttachConsole(ATTACH_PARENT_PROCESS);
        }
    }
}

fn current_thread_impersonation_level() -> SECURITY_IMPERSONATION_LEVEL {
    let mut token: HANDLE = ptr::null_mut();
    // SAFETY: token is writable and GetCurrentThread returns the current thread pseudo-handle.
    if unsafe { OpenThreadToken(GetCurrentThread(), TOKEN_QUERY, 1, &mut token) } == 0 {
        panic!(
            "failed to open named-pipe impersonation token: {}",
            io::Error::last_os_error()
        );
    }
    let mut level = 0;
    let mut bytes = 0;
    // SAFETY: token is live and level is correctly sized writable output storage.
    let result = unsafe {
        GetTokenInformation(
            token,
            TokenImpersonationLevel,
            (&mut level as *mut SECURITY_IMPERSONATION_LEVEL).cast(),
            std::mem::size_of::<SECURITY_IMPERSONATION_LEVEL>() as u32,
            &mut bytes,
        )
    };
    let error = (result == 0).then(io::Error::last_os_error);
    // SAFETY: token was returned by OpenThreadToken and is owned here.
    unsafe {
        CloseHandle(token);
    }
    assert!(
        error.is_none(),
        "failed to read impersonation level: {}",
        error.unwrap()
    );
    level
}

struct Impersonation {
    token: HANDLE,
}

impl Impersonation {
    fn start(username: &str, password: &str) -> io::Result<Self> {
        let username = wide(username);
        let domain = wide(".");
        let password = wide(password);
        let mut token: HANDLE = ptr::null_mut();
        if unsafe {
            LogonUserW(
                username.as_ptr(),
                domain.as_ptr(),
                password.as_ptr(),
                LOGON32_LOGON_NETWORK,
                LOGON32_PROVIDER_DEFAULT,
                &mut token,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        if unsafe { ImpersonateLoggedOnUser(token) } == 0 {
            let error = io::Error::last_os_error();
            unsafe {
                CloseHandle(token);
            }
            return Err(error);
        }
        Ok(Self { token })
    }
}

impl Drop for Impersonation {
    fn drop(&mut self) {
        assert_ne!(unsafe { RevertToSelf() }, 0);
        unsafe {
            CloseHandle(self.token);
        }
    }
}

fn wide(value: &str) -> Vec<u16> {
    std::ffi::OsStr::new(value)
        .encode_wide()
        .chain(Some(0))
        .collect()
}

fn wide_os(value: &std::ffi::OsStr) -> Vec<u16> {
    value.encode_wide().chain(Some(0)).collect()
}
