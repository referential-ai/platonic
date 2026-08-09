use super::*;
use platonic_client::lock::LockMetadata;
use platonic_protocol::{
    ProtocolErrorCode, RunStateName, ShutdownIfIdleResultName, TypedTranscriptEntry,
};
use serde_json::json;
use std::{
    env, fs,
    io::{BufRead, BufReader, Read, Write},
    os::unix::fs::{FileTypeExt, MetadataExt},
    path::{Path, PathBuf},
    sync::{Arc, Barrier, Mutex, mpsc},
    thread,
    time::{Duration, Instant},
};

const PROOF_TIMEOUT: Duration = Duration::from_secs(15);
const PROOF_KEY_ENV: &str = "PLATO_APPIMAGE_PROOF_KEY";
const PROOF_KEY_VALUE: &str = "appimage-proof-dummy";
const PATH_COMMAND_ENV: &str = "PLATO_PACKAGED_PATH_COMMAND";
const PATH_OUTPUT_ENV: &str = "PLATO_PACKAGED_PATH_OUTPUT";

#[test]
#[ignore = "requires provisioned PLATO_APPIMAGE_TEST_DAEMON"]
fn provisioned_unix_sidecar_lifecycle() {
    let daemon = proof_executable("PLATO_APPIMAGE_TEST_DAEMON");
    let proof_key = env::var(PROOF_KEY_ENV)
        .unwrap_or_else(|_| panic!("{PROOF_KEY_ENV} must contain the scoped dummy credential"));
    assert_eq!(proof_key, PROOF_KEY_VALUE);

    shell_exit_detaches_active_daemon(&daemon);
    crash_reconnect_recovers_lock_in_place(&daemon);
    concurrent_starters_attach_to_one_winner(&daemon);
}

#[test]
#[ignore = "requires a provisioned sidecar and PATH-only command"]
fn provisioned_unix_path_only_shell_exec() {
    let daemon = proof_executable("PLATO_APPIMAGE_TEST_DAEMON");
    let command = env::var(PATH_COMMAND_ENV)
        .unwrap_or_else(|_| panic!("{PATH_COMMAND_ENV} must name the PATH-only command"));
    let executable = command
        .split_ascii_whitespace()
        .next()
        .expect("PATH-only command must not be empty");
    let expected_output = env::var(PATH_OUTPUT_ENV)
        .unwrap_or_else(|_| panic!("{PATH_OUTPUT_ENV} must name the expected command output"));
    assert!(!expected_output.is_empty());
    let executable_is_available = env::var_os("PATH").is_some_and(|path| {
        env::split_paths(&path).any(|directory| directory.join(executable).is_file())
    });
    assert!(
        !executable_is_available,
        "{executable} is already available in the desktop test process PATH"
    );

    path_only_shell_exec_uses_desktop_approval(&daemon, &command, &expected_output);
}

fn shell_exit_detaches_active_daemon(daemon: &Path) {
    let workspace = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let workspace_root = canonical_workspace(workspace.path()).unwrap();
    let workspace_file = state.path().join("workspace.json");
    persist_canonical_workspace(&workspace_file, &workspace_root).unwrap();
    let socket_path = paths::host_socket_path().unwrap();
    let lock_path = paths::host_lock_path().unwrap();
    let config_path = workspace_root.join("plato.toml");
    let provider = PausedFakeProvider::start("appimage lifecycle survived");
    write_provider_config(&config_path, &provider.base_url);
    assert!(DaemonClient::connect(&socket_path).is_err());

    let lifecycle = Mutex::new(DesktopLifecycle::default());
    let launch = test_launch(daemon.to_path_buf());
    let view =
        bootstrap_and_register_workspace(&workspace_file, &lifecycle, &launch, &workspace_root);
    assert!(matches!(view, BootstrapView::Ready { .. }));
    assert_socket(&socket_path);

    let mut run_client = connect_hello_bounded(&socket_path, &workspace_root);
    run_client.set_timeout(PROOF_TIMEOUT).unwrap();
    let started = run_client
        .run_start(
            "prove packaged Unix detach".into(),
            Some(config_path.to_string_lossy().into_owned()),
            false,
        )
        .unwrap();
    assert_eq!(started.status, RunStateName::Running);
    provider.wait_for_request(&mut run_client, &started.run_id);
    run_client.set_timeout(PROOF_TIMEOUT).unwrap();
    assert_eq!(
        run_client.transcript_read(&started.run_id).unwrap().status,
        RunStateName::Running
    );
    drop(run_client);

    drop(lifecycle);
    assert!(socket_path.exists(), "shell exit removed the daemon socket");
    assert!(lock_path.exists(), "shell exit stopped the daemon");
    let mut surviving_client = connect_hello_bounded(&socket_path, &workspace_root);
    surviving_client.set_timeout(PROOF_TIMEOUT).unwrap();
    assert_eq!(
        surviving_client
            .transcript_read(&started.run_id)
            .unwrap()
            .status,
        RunStateName::Running
    );
    drop(surviving_client);

    provider.release();
    let mut fresh_client = connect_hello_bounded(&socket_path, &workspace_root);
    let transcript = wait_for_terminal_transcript(&mut fresh_client, &started.run_id);
    assert_eq!(transcript.run_id, started.run_id);
    assert_eq!(transcript.status, RunStateName::Finished);
    assert_eq!(
        transcript.final_answer.as_deref(),
        Some("appimage lifecycle survived")
    );
    assert_eq!(
        {
            fresh_client.set_timeout(PROOF_TIMEOUT).unwrap();
            fresh_client.shutdown_if_idle().unwrap().result
        },
        ShutdownIfIdleResultName::Shutdown
    );
    drop(fresh_client);
    wait_for_socket_removal_with_persistent_lock(&socket_path, &lock_path);
}

fn path_only_shell_exec_uses_desktop_approval(daemon: &Path, command: &str, expected_output: &str) {
    let workspace = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let workspace_root = canonical_workspace(workspace.path()).unwrap();
    let workspace_file = state.path().join("workspace.json");
    persist_canonical_workspace(&workspace_file, &workspace_root).unwrap();
    let socket_path = paths::host_socket_path().unwrap();
    let lock_path = paths::host_lock_path().unwrap();
    let config_path = workspace_root.join("plato.toml");
    let provider = ShellExecProvider::start(command, expected_output);
    write_shell_provider_config(&config_path, &provider.base_url);

    let lifecycle = Mutex::new(DesktopLifecycle::default());
    let launch = test_launch(daemon.to_path_buf());
    let view =
        bootstrap_and_register_workspace(&workspace_file, &lifecycle, &launch, &workspace_root);
    assert!(matches!(view, BootstrapView::Ready { .. }));
    assert_socket(&socket_path);

    let mut client = connect_hello_bounded(&socket_path, &workspace_root);
    client.set_timeout(PROOF_TIMEOUT).unwrap();
    let started = client
        .run_start(
            "run the PATH-only packaged desktop proof".into(),
            Some(config_path.to_string_lossy().into_owned()),
            false,
        )
        .unwrap();
    let deadline = Instant::now() + PROOF_TIMEOUT;
    let pending = loop {
        client.set_timeout(PROOF_TIMEOUT).unwrap();
        let transcript = match client.transcript_read(&started.run_id) {
            Ok(transcript) => transcript,
            Err(ClientError::DaemonResponse(error))
                if error.code == ProtocolErrorCode::NotFound =>
            {
                assert!(
                    Instant::now() < deadline,
                    "shell.exec run never appeared in the ledger"
                );
                thread::sleep(Duration::from_millis(20));
                continue;
            }
            Err(error) => panic!("unable to read shell.exec proof transcript: {error}"),
        };
        if let Some(pending) = transcript.pending_approval {
            break pending;
        }
        assert!(
            Instant::now() < deadline,
            "shell.exec approval did not appear"
        );
        thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(pending.tool_name, "shell.exec");

    let decided = decide_approval_from_workspace(
        &workspace_root,
        &started.run_id,
        &pending.tool_call_id,
        DesktopApprovalDecision::Grant,
        None,
        None,
    )
    .unwrap();
    assert_eq!(decided.run_id, started.run_id);
    assert_eq!(decided.status, RunStateName::Running);

    let transcript = wait_for_terminal_transcript(&mut client, &started.run_id);
    assert_eq!(transcript.status, RunStateName::Finished);
    assert_eq!(
        transcript.final_answer.as_deref(),
        Some("PATH-only shell.exec completed")
    );
    let typed = transcript.typed.expect("typed PATH proof transcript");
    assert!(typed.runs.iter().flat_map(|run| &run.entries).any(|entry| {
        matches!(
            entry,
            TypedTranscriptEntry::ToolResult { call_id, summary }
                if call_id == &pending.tool_call_id && summary.starts_with("shell.exec exited 0")
        )
    }));
    provider.handle.join().unwrap();

    client.set_timeout(PROOF_TIMEOUT).unwrap();
    assert_eq!(
        client.shutdown_if_idle().unwrap().result,
        ShutdownIfIdleResultName::Shutdown
    );
    drop(client);
    drop(lifecycle);
    wait_for_socket_removal_with_persistent_lock(&socket_path, &lock_path);
}

fn crash_reconnect_recovers_lock_in_place(daemon: &Path) {
    let workspace = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let workspace_root = canonical_workspace(workspace.path()).unwrap();
    let workspace_file = state.path().join("workspace.json");
    persist_canonical_workspace(&workspace_file, &workspace_root).unwrap();
    let socket_path = paths::host_socket_path().unwrap();
    let lock_path = paths::host_lock_path().unwrap();
    let mut lifecycle = Mutex::new(DesktopLifecycle::default());
    let launch = test_launch(daemon.to_path_buf());

    bootstrap_and_register_workspace(&workspace_file, &lifecycle, &launch, &workspace_root);
    assert!(lifecycle.get_mut().unwrap().spawned_daemon.is_none());
    let metadata: LockMetadata = serde_json::from_slice(&fs::read(&lock_path).unwrap()).unwrap();
    let child_id = metadata.pid;
    let lock_identity = file_identity(&lock_path);
    let pid = rustix::process::Pid::from_raw(child_id as i32).unwrap();
    rustix::process::kill_process(pid, rustix::process::Signal::KILL).unwrap();
    wait_for_endpoint_close(&socket_path);
    assert!(socket_path.exists(), "abrupt crash removed the Unix socket");
    assert!(lock_path.exists(), "abrupt crash removed the daemon lock");
    let stale_lock = fs::read(&lock_path).unwrap();

    let config = resolve_desktop_connection(&workspace_root, None).unwrap();
    let attach_error =
        try_attach_workspace_until(&config, Instant::now() + Duration::from_millis(250))
            .unwrap_err();
    assert_eq!(attach_error.code, "daemon_unavailable");
    assert!(lifecycle.get_mut().unwrap().spawned_daemon.is_none());
    assert_eq!(fs::read(&lock_path).unwrap(), stale_lock);

    let view = bootstrap_with_lifecycle(&workspace_file, &lifecycle, &launch, None).unwrap();
    assert!(matches!(view, BootstrapView::Ready { .. }));
    assert!(lifecycle.get_mut().unwrap().spawned_daemon.is_none());
    let recovered: LockMetadata = serde_json::from_slice(&fs::read(&lock_path).unwrap()).unwrap();
    assert_ne!(recovered.pid, child_id);
    assert!(process_is_running(recovered.pid));
    assert_eq!(file_identity(&lock_path), lock_identity);
    let mut client = connect_hello_bounded(&socket_path, &workspace_root);
    assert_eq!(
        {
            client.set_timeout(PROOF_TIMEOUT).unwrap();
            client.shutdown_if_idle().unwrap().result
        },
        ShutdownIfIdleResultName::Shutdown
    );
    drop(client);
    drop(lifecycle);
    wait_for_socket_removal_with_persistent_lock(&socket_path, &lock_path);
    assert_eq!(file_identity(&lock_path), lock_identity);
    wait_for_process_exit(recovered.pid);
}

fn concurrent_starters_attach_to_one_winner(daemon: &Path) {
    let workspace = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let workspace_root = canonical_workspace(workspace.path()).unwrap();
    let workspace_file = state.path().join("workspace.json");
    persist_canonical_workspace(&workspace_file, &workspace_root).unwrap();
    let socket_path = paths::host_socket_path().unwrap();
    let lock_path = paths::host_lock_path().unwrap();
    let launch = test_launch(daemon.to_path_buf());
    let lifecycle = Mutex::new(DesktopLifecycle::default());
    bootstrap_and_register_workspace(&workspace_file, &lifecycle, &launch, &workspace_root);
    let mut client = connect_hello_bounded(&socket_path, &workspace_root);
    client.set_timeout(PROOF_TIMEOUT).unwrap();
    assert_eq!(
        client.shutdown_if_idle().unwrap().result,
        ShutdownIfIdleResultName::Shutdown
    );
    drop(client);
    drop(lifecycle);
    wait_for_socket_removal_with_persistent_lock(&socket_path, &lock_path);

    let barrier = Arc::new(Barrier::new(3));

    let first_barrier = Arc::clone(&barrier);
    let first_root = workspace_root.clone();
    let first_launch = launch.clone();
    let first = thread::spawn(move || {
        let mut lifecycle = DesktopLifecycle::default();
        first_barrier.wait();
        let view =
            attach_or_spawn_workspace(&first_root, None, &mut lifecycle, &first_launch).unwrap();
        (view, lifecycle)
    });
    let second_barrier = Arc::clone(&barrier);
    let second_root = workspace_root.clone();
    let second = thread::spawn(move || {
        let mut lifecycle = DesktopLifecycle::default();
        second_barrier.wait();
        let view = attach_or_spawn_workspace(&second_root, None, &mut lifecycle, &launch).unwrap();
        (view, lifecycle)
    });

    barrier.wait();
    let (first_view, first_lifecycle) = first.join().unwrap();
    let (second_view, second_lifecycle) = second.join().unwrap();
    assert!(matches!(first_view, BootstrapView::Ready { .. }));
    assert!(matches!(second_view, BootstrapView::Ready { .. }));

    assert!(first_lifecycle.spawned_daemon.is_none());
    assert!(second_lifecycle.spawned_daemon.is_none());
    let winner_lock = fs::read(&lock_path).unwrap();
    let winner: LockMetadata = serde_json::from_slice(&winner_lock).unwrap();
    assert!(process_is_running(winner.pid));
    thread::sleep(Duration::from_millis(50));
    assert_eq!(fs::read(&lock_path).unwrap(), winner_lock);

    let mut client = connect_hello_bounded(&socket_path, &workspace_root);
    assert_eq!(
        {
            client.set_timeout(PROOF_TIMEOUT).unwrap();
            client.shutdown_if_idle().unwrap().result
        },
        ShutdownIfIdleResultName::Shutdown
    );
    drop(client);
    wait_for_socket_removal_with_persistent_lock(&socket_path, &lock_path);
    wait_for_process_exit(winner.pid);
}

fn bootstrap_and_register_workspace(
    workspace_file: &Path,
    lifecycle: &Mutex<DesktopLifecycle>,
    launch: &DaemonLaunch,
    workspace_root: &Path,
) -> BootstrapView {
    let error = bootstrap_with_lifecycle(workspace_file, lifecycle, launch, None).unwrap_err();
    assert_eq!(error.code, "workspace_unregistered");
    assert!(error.message.contains("platonic workspace create"));

    let socket_path = paths::host_socket_path().unwrap();
    let mut control = DaemonClient::connect_with_timeout(&socket_path, PROOF_TIMEOUT).unwrap();
    let workspace_id = paths::workspace_id(workspace_root).unwrap();
    let created = control
        .workspace_create(
            format!("desktop-proof-{workspace_id}"),
            workspace_root.to_path_buf(),
        )
        .unwrap();
    assert_eq!(Path::new(&created.workspace.root), workspace_root);
    drop(control);

    bootstrap_with_lifecycle(workspace_file, lifecycle, launch, None).unwrap()
}

fn test_launch(executable: PathBuf) -> DaemonLaunch {
    DaemonLaunch {
        executable: Some(executable),
    }
}

fn proof_executable(variable: &str) -> PathBuf {
    let path =
        PathBuf::from(env::var_os(variable).unwrap_or_else(|| panic!("{variable} is required")));
    let path = path
        .canonicalize()
        .unwrap_or_else(|error| panic!("{variable} cannot be resolved: {error}"));
    assert!(path.is_absolute());
    assert!(path.is_file());
    path
}

fn connect_hello_bounded(socket_path: &Path, workspace_root: &Path) -> DaemonClient {
    let deadline = Instant::now() + PROOF_TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match DaemonClient::connect_with_timeout(socket_path, remaining) {
            Ok(mut client) => match client.hello(workspace_root) {
                Ok(_) => return client,
                Err(error) => assert!(
                    Instant::now() < deadline,
                    "daemon never accepted hello: {error}"
                ),
            },
            Err(error) => assert!(
                Instant::now() < deadline,
                "daemon endpoint never became available: {error}"
            ),
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn assert_socket(path: &Path) {
    assert!(
        fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_socket()),
        "{} is not a Unix socket",
        path.display()
    );
}

fn wait_for_terminal_transcript(
    client: &mut DaemonClient,
    run_id: &str,
) -> platonic_protocol::TranscriptReadResult {
    let deadline = Instant::now() + PROOF_TIMEOUT;
    loop {
        client.set_timeout(PROOF_TIMEOUT).unwrap();
        let transcript = client.transcript_read(run_id).unwrap();
        if transcript.status != RunStateName::Running {
            return transcript;
        }
        assert!(Instant::now() < deadline, "run {run_id} did not finish");
        thread::sleep(Duration::from_millis(20));
    }
}

fn wait_for_endpoint_close(socket_path: &Path) {
    let deadline = Instant::now() + PROOF_TIMEOUT;
    while DaemonClient::connect(socket_path).is_ok() {
        assert!(
            Instant::now() < deadline,
            "daemon endpoint remained live after process exit"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn wait_for_socket_removal_with_persistent_lock(socket_path: &Path, lock_path: &Path) {
    let deadline = Instant::now() + PROOF_TIMEOUT;
    while socket_path.exists() {
        assert!(lock_path.exists(), "daemon removed {}", lock_path.display());
        assert!(
            Instant::now() < deadline,
            "daemon did not remove {}",
            socket_path.display()
        );
        thread::sleep(Duration::from_millis(20));
    }
    assert!(lock_path.exists(), "daemon removed {}", lock_path.display());
}

fn file_identity(path: &Path) -> (u64, u64) {
    let metadata = fs::symlink_metadata(path).unwrap();
    (metadata.dev(), metadata.ino())
}

fn wait_for_process_exit(pid: u32) {
    let deadline = Instant::now() + PROOF_TIMEOUT;
    while process_is_running(pid) {
        assert!(Instant::now() < deadline, "daemon child did not exit");
        thread::sleep(Duration::from_millis(20));
    }
}

fn process_is_running(pid: u32) -> bool {
    let Some(pid) = rustix::process::Pid::from_raw(pid as i32) else {
        return false;
    };
    match rustix::process::test_kill_process(pid) {
        Ok(()) | Err(rustix::io::Errno::PERM) => true,
        Err(rustix::io::Errno::SRCH) => false,
        Err(error) => panic!("cannot inspect daemon process {pid}: {error}"),
    }
}

struct PausedFakeProvider {
    base_url: String,
    requested: mpsc::Receiver<()>,
    release: mpsc::Sender<()>,
    handle: thread::JoinHandle<()>,
}

impl PausedFakeProvider {
    fn start(answer: &'static str) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let (requested_tx, requested) = mpsc::channel();
        let (release, release_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let authorization = read_http_request(&mut stream).authorization;
            assert_eq!(
                authorization,
                Some(format!("Bearer {PROOF_KEY_VALUE}")),
                "provider request used the wrong scoped credential"
            );
            requested_tx.send(()).unwrap();
            release_rx.recv_timeout(PROOF_TIMEOUT).unwrap();
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
        });
        Self {
            base_url,
            requested,
            release,
            handle,
        }
    }

    fn wait_for_request(&self, client: &mut DaemonClient, run_id: &str) {
        if let Err(error) = self.requested.recv_timeout(PROOF_TIMEOUT) {
            client.set_timeout(PROOF_TIMEOUT).unwrap();
            let transcript = client.transcript_read(run_id);
            panic!("provider did not receive a request ({error}); transcript: {transcript:?}");
        }
    }

    fn release(self) {
        self.release.send(()).unwrap();
        self.handle.join().unwrap();
    }
}

struct ShellExecProvider {
    base_url: String,
    handle: thread::JoinHandle<()>,
}

impl ShellExecProvider {
    fn start(command: &str, expected_output: &str) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let command = command.to_owned();
        let expected_output = expected_output.to_owned();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            assert_eq!(
                read_http_request(&mut stream).authorization,
                Some(format!("Bearer {PROOF_KEY_VALUE}"))
            );
            let tool_delta = json!({
                "choices": [{
                    "index": 0,
                    "delta": {
                        "tool_calls": [{
                            "index": 0,
                            "id": "path_only_call",
                            "function": {
                                "name": "shell_exec",
                                "arguments": json!({"command": command}).to_string()
                            }
                        }]
                    },
                    "finish_reason": null
                }]
            });
            let tool_finish = json!({
                "choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}]
            });
            write_event_stream(&mut stream, &tool_delta, &tool_finish);

            let (mut stream, _) = listener.accept().unwrap();
            let request = read_http_request(&mut stream);
            assert_eq!(
                request.authorization,
                Some(format!("Bearer {PROOF_KEY_VALUE}"))
            );
            assert!(
                String::from_utf8(request.body)
                    .unwrap()
                    .contains(&expected_output),
                "the provider continuation did not contain the PATH-only command output"
            );
            let answer = json!({
                "choices": [{
                    "index": 0,
                    "delta": {"content": "PATH-only shell.exec completed"},
                    "finish_reason": null
                }]
            });
            let finish = json!({
                "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]
            });
            write_event_stream(&mut stream, &answer, &finish);
        });
        Self { base_url, handle }
    }
}

fn write_event_stream(
    stream: &mut std::net::TcpStream,
    first: &serde_json::Value,
    second: &serde_json::Value,
) {
    let body = format!("data: {first}\n\ndata: {second}\n\ndata: [DONE]\n\n");
    write!(
        stream,
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
}

struct HttpRequest {
    authorization: Option<String>,
    body: Vec<u8>,
}

fn read_http_request(stream: &mut std::net::TcpStream) -> HttpRequest {
    stream.set_read_timeout(Some(PROOF_TIMEOUT)).unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut content_length = 0;
    let mut authorization = None;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        if line == "\r\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.trim().parse().unwrap();
            } else if name.eq_ignore_ascii_case("authorization") {
                authorization = Some(value.trim().to_owned());
            }
        }
    }
    let mut body = vec![0; content_length];
    reader.read_exact(&mut body).unwrap();
    HttpRequest {
        authorization,
        body,
    }
}

fn write_provider_config(path: &Path, base_url: &str) {
    fs::write(
        path,
        format!(
            r#"[provider]
kind = "open_ai"
model = "test-model"
api_key_env = "{PROOF_KEY_ENV}"
base_url = "{base_url}"
timeout_ms = 15000

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

fn write_shell_provider_config(path: &Path, base_url: &str) {
    fs::write(
        path,
        format!(
            r#"[provider]
kind = "open_ai"
model = "test-model"
api_key_env = "{PROOF_KEY_ENV}"
base_url = "{base_url}"
timeout_ms = 15000

[limits]
token_budget = 4000
max_output_tokens = 32
max_turns = 2

[tools]
enabled = ["shell.exec"]
"#
        ),
    )
    .unwrap();
}
