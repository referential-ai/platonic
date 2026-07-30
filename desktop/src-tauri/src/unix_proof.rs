use super::*;
use plato_agent::daemon::{
    lock::LockMetadata,
    protocol::{RunStateName, ShutdownIfIdleResultName},
};
use serde_json::json;
use std::{
    env, fs,
    io::{BufRead, BufReader, Read, Write},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Barrier, Mutex, mpsc},
    thread,
    time::{Duration, Instant},
};

const PROOF_TIMEOUT: Duration = Duration::from_secs(15);
const PROOF_KEY_ENV: &str = "PLATO_APPIMAGE_PROOF_KEY";
const PROOF_KEY_VALUE: &str = "appimage-proof-dummy";

#[test]
#[ignore = "requires provisioned PLATO_APPIMAGE_TEST_DAEMON and PLATO_APPIMAGE_TEST_CLI"]
fn provisioned_unix_sidecar_lifecycle() {
    let daemon = proof_executable("PLATO_APPIMAGE_TEST_DAEMON");
    let cli = proof_executable("PLATO_APPIMAGE_TEST_CLI");
    let proof_key = env::var(PROOF_KEY_ENV)
        .unwrap_or_else(|_| panic!("{PROOF_KEY_ENV} must contain the scoped dummy credential"));
    assert_eq!(proof_key, PROOF_KEY_VALUE);

    shell_exit_detaches_active_daemon(&daemon, &cli);
    crash_reconnect_recovers_lock_in_place(&daemon);
    concurrent_starters_attach_to_one_winner(&daemon);
}

fn shell_exit_detaches_active_daemon(daemon: &Path, cli: &Path) {
    let workspace = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let workspace_root = canonical_workspace(workspace.path()).unwrap();
    let workspace_file = state.path().join("workspace.json");
    persist_canonical_workspace(&workspace_file, &workspace_root).unwrap();
    let socket_path = paths::default_socket_path(&workspace_root).unwrap();
    let lock_path = paths::default_lock_path(&workspace_root).unwrap();
    let config_path = workspace_root.join("plato.toml");
    let provider = PausedFakeProvider::start("appimage lifecycle survived");
    write_provider_config(&config_path, &provider.base_url);
    assert!(DaemonClient::connect(&socket_path).is_err());

    let lifecycle = Mutex::new(DesktopLifecycle::default());
    let launch = test_launch(daemon.to_path_buf());
    let view = bootstrap_with_lifecycle(&workspace_file, &lifecycle, &launch, None).unwrap();
    assert!(matches!(view, BootstrapView::Ready { .. }));

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
    provider.wait_for_request();
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

    let one_shot = Command::new(cli)
        .current_dir(&workspace_root)
        .arg(format!(
            "--db={}",
            workspace_root.join("direct-proof.db").display()
        ))
        .arg("this must fail before provider access")
        .output()
        .unwrap();
    let one_shot_error = String::from_utf8_lossy(&one_shot.stderr);
    assert!(!one_shot.status.success());
    assert!(one_shot_error.contains("daemon lock held"));
    assert!(one_shot_error.contains(lock_path.to_string_lossy().as_ref()));

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

fn crash_reconnect_recovers_lock_in_place(daemon: &Path) {
    let workspace = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let workspace_root = canonical_workspace(workspace.path()).unwrap();
    let workspace_file = state.path().join("workspace.json");
    persist_canonical_workspace(&workspace_file, &workspace_root).unwrap();
    let socket_path = paths::default_socket_path(&workspace_root).unwrap();
    let lock_path = paths::default_lock_path(&workspace_root).unwrap();
    let mut lifecycle = Mutex::new(DesktopLifecycle::default());
    let launch = test_launch(daemon.to_path_buf());

    bootstrap_with_lifecycle(&workspace_file, &lifecycle, &launch, None).unwrap();
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

    let config = DaemonConnectionConfig::resolve(&workspace_root, None).unwrap();
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
    let workspace_root = canonical_workspace(workspace.path()).unwrap();
    let socket_path = paths::default_socket_path(&workspace_root).unwrap();
    let lock_path = paths::default_lock_path(&workspace_root).unwrap();
    let launch = test_launch(daemon.to_path_buf());
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

fn wait_for_terminal_transcript(
    client: &mut DaemonClient,
    run_id: &str,
) -> plato_agent::daemon::protocol::TranscriptReadResult {
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
            let authorization = read_http_request(&mut stream);
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

    fn wait_for_request(&self) {
        self.requested.recv_timeout(PROOF_TIMEOUT).unwrap();
    }

    fn release(self) {
        self.release.send(()).unwrap();
        self.handle.join().unwrap();
    }
}

fn read_http_request(stream: &mut std::net::TcpStream) -> Option<String> {
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
    authorization
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
