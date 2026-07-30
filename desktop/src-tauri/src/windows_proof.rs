use super::*;
use plato_agent::daemon::protocol::{RunStateName, ShutdownIfIdleResultName};
use serde_json::json;
use std::{
    env, fs,
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    process::{Child, Command},
    sync::{Arc, Barrier, Mutex, mpsc},
    thread,
    time::{Duration, Instant},
};

const PROOF_TIMEOUT: Duration = Duration::from_secs(15);

#[test]
fn separate_shell_states_do_not_follow_the_shared_relaunch_seed() {
    let state = tempfile::tempdir().unwrap();
    let first_root = tempfile::tempdir().unwrap();
    let second_root = tempfile::tempdir().unwrap();
    let workspace_file = state.path().join("workspace.json");
    let first = Mutex::new(DesktopLifecycle::default());
    let second = Mutex::new(DesktopLifecycle::default());

    for (shell, root) in [(&first, first_root.path()), (&second, second_root.path())] {
        let root = canonical_workspace(root).unwrap();
        let mut shell = shell.lock().unwrap();
        let prepared = shell.prepare_workspace(&root).unwrap();
        shell.commit_workspace(prepared);
        persist_canonical_workspace(&workspace_file, &root).unwrap();
    }

    assert_eq!(
        selected_workspace(&first).unwrap(),
        first_root.path().canonicalize().unwrap()
    );
    assert_eq!(
        selected_workspace(&second).unwrap(),
        second_root.path().canonicalize().unwrap()
    );
}

#[test]
fn connected_daemon_that_never_answers_is_bounded() {
    use interprocess::local_socket::{GenericFilePath, ListenerOptions, prelude::*};

    let workspace = tempfile::tempdir().unwrap();
    let workspace_root = canonical_workspace(workspace.path()).unwrap();
    let config = DaemonConnectionConfig::resolve(&workspace_root, None).unwrap();
    let listener = ListenerOptions::new()
        .name(
            config
                .socket_path
                .as_os_str()
                .to_fs_name::<GenericFilePath>()
                .unwrap(),
        )
        .create_sync()
        .unwrap();
    let (release, released) = mpsc::channel();
    let server = thread::spawn(move || {
        let stream = listener.accept().unwrap();
        let mut request = String::new();
        BufReader::new(&stream).read_line(&mut request).unwrap();
        assert!(!request.is_empty());
        released.recv_timeout(PROOF_TIMEOUT).unwrap();
    });

    let started = Instant::now();
    let error = try_attach_workspace_until(&config, Instant::now() + Duration::from_millis(250))
        .unwrap_err();

    assert_eq!(error.code, "daemon_unavailable");
    assert!(started.elapsed() < Duration::from_secs(2));
    release.send(()).unwrap();
    server.join().unwrap();
}

#[test]
#[ignore = "requires provisioned PLATO_DESKTOP_TEST_DAEMON and PLATO_DESKTOP_TEST_CLI"]
fn provisioned_shell_exit_detaches_active_daemon_and_cli_stays_locked() {
    let daemon = proof_executable("PLATO_DESKTOP_TEST_DAEMON");
    let cli = proof_executable("PLATO_DESKTOP_TEST_CLI");
    let workspace = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let workspace_root = canonical_workspace(workspace.path()).unwrap();
    let workspace_file = state.path().join("workspace.json");
    persist_canonical_workspace(&workspace_file, &workspace_root).unwrap();
    let socket_path = paths::default_socket_path(&workspace_root).unwrap();
    let lock_path = paths::default_lock_path(&workspace_root).unwrap();
    let config_path = workspace_root.join("plato.toml");
    let provider = PausedFakeProvider::start("desktop survived");
    write_provider_config(&config_path, &provider.base_url);

    let lifecycle = Mutex::new(DesktopLifecycle::default());
    let launch = test_launch(daemon);
    let view = bootstrap_with_lifecycle(&workspace_file, &lifecycle, &launch, None).unwrap();
    assert!(matches!(view, BootstrapView::Ready { .. }));

    let mut second_client = connect_hello_bounded(&socket_path, &workspace_root);
    let started = second_client
        .run_start(
            "prove detached lifetime".into(),
            Some(config_path.to_string_lossy().into_owned()),
            false,
        )
        .unwrap();
    assert_eq!(started.status, RunStateName::Running);
    provider.wait_for_request();
    assert_eq!(
        second_client
            .transcript_read(&started.run_id)
            .unwrap()
            .status,
        RunStateName::Running
    );

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

    drop(lifecycle);
    assert!(lock_path.exists(), "shell exit stopped the daemon");
    provider.release();

    let transcript = wait_for_terminal_transcript(&mut second_client, &started.run_id);
    assert_eq!(transcript.status, RunStateName::Finished);
    assert_eq!(transcript.final_answer.as_deref(), Some("desktop survived"));

    let mut third_client = connect_hello_bounded(&socket_path, &workspace_root);
    assert_eq!(
        third_client.shutdown_if_idle().unwrap().result,
        ShutdownIfIdleResultName::Shutdown
    );
    drop(third_client);
    drop(second_client);
    wait_for_lock_removal(&lock_path);
    assert!(DaemonClient::connect(&socket_path).is_err());
}

#[test]
#[ignore = "requires provisioned PLATO_DESKTOP_TEST_DAEMON"]
fn provisioned_crash_removes_lock_and_explicit_reconnect_restarts_workspace() {
    let daemon = proof_executable("PLATO_DESKTOP_TEST_DAEMON");
    let workspace = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let workspace_root = canonical_workspace(workspace.path()).unwrap();
    let workspace_file = state.path().join("workspace.json");
    persist_canonical_workspace(&workspace_file, &workspace_root).unwrap();
    let socket_path = paths::default_socket_path(&workspace_root).unwrap();
    let lock_path = paths::default_lock_path(&workspace_root).unwrap();
    let mut lifecycle = Mutex::new(DesktopLifecycle::default());
    let launch = test_launch(daemon);

    bootstrap_with_lifecycle(&workspace_file, &lifecycle, &launch, None).unwrap();
    let child_id = {
        let spawned = lifecycle
            .get_mut()
            .unwrap()
            .spawned_daemon
            .as_mut()
            .expect("bootstrap must record its spawned daemon");
        let child_id = spawned.child.id();
        spawned.child.kill().unwrap();
        spawned.child.wait().unwrap();
        child_id
    };
    wait_for_endpoint_close(&socket_path);
    wait_for_lock_removal(&lock_path);

    let attach_error = with_saved_client(&workspace_file, None, |_| Ok(())).unwrap_err();
    assert_eq!(attach_error.code, "daemon_unavailable");
    let spawned = lifecycle
        .get_mut()
        .unwrap()
        .spawned_daemon
        .as_mut()
        .expect("attach-only operation changed lifecycle state");
    assert_eq!(spawned.child.id(), child_id);
    assert!(spawned.child.try_wait().unwrap().is_some());
    assert!(DaemonClient::connect(&socket_path).is_err());

    let view = bootstrap_with_lifecycle(&workspace_file, &lifecycle, &launch, None).unwrap();
    assert!(matches!(view, BootstrapView::Ready { .. }));
    let restarted = lifecycle
        .get_mut()
        .unwrap()
        .spawned_daemon
        .as_mut()
        .expect("explicit reconnect must record the restarted daemon");
    assert!(restarted.child.try_wait().unwrap().is_none());
    assert!(lock_path.exists());

    let mut client = connect_hello_bounded(&socket_path, &workspace_root);
    assert_eq!(
        client.shutdown_if_idle().unwrap().result,
        ShutdownIfIdleResultName::Shutdown
    );
    drop(client);
    wait_for_child_exit(&mut restarted.child);
    drop(lifecycle);
    wait_for_lock_removal(&lock_path);
}

#[test]
#[ignore = "requires provisioned PLATO_DESKTOP_TEST_DAEMON"]
fn provisioned_concurrent_starters_attach_to_one_winner() {
    let daemon = proof_executable("PLATO_DESKTOP_TEST_DAEMON");
    let workspace = tempfile::tempdir().unwrap();
    let workspace_root = canonical_workspace(workspace.path()).unwrap();
    let socket_path = paths::default_socket_path(&workspace_root).unwrap();
    let lock_path = paths::default_lock_path(&workspace_root).unwrap();
    let config = DaemonConnectionConfig::resolve(&workspace_root, None).unwrap();
    let launch = test_launch(daemon);
    let barrier = Arc::new(Barrier::new(3));

    let first_barrier = Arc::clone(&barrier);
    let first_config = config.clone();
    let first_launch = launch.clone();
    let first = thread::spawn(move || {
        let mut lifecycle = DesktopLifecycle::default();
        first_barrier.wait();
        let view = start_and_attach_workspace(
            &first_config,
            &mut lifecycle,
            &first_launch,
            DesktopError::new("daemon_unavailable", "concurrent-start miss"),
        )
        .unwrap();
        (view, lifecycle)
    });
    let second_barrier = Arc::clone(&barrier);
    let second = thread::spawn(move || {
        let mut lifecycle = DesktopLifecycle::default();
        second_barrier.wait();
        let view = start_and_attach_workspace(
            &config,
            &mut lifecycle,
            &launch,
            DesktopError::new("daemon_unavailable", "concurrent-start miss"),
        )
        .unwrap();
        (view, lifecycle)
    });

    barrier.wait();
    let (first_view, mut first_lifecycle) = first.join().unwrap();
    let (second_view, mut second_lifecycle) = second.join().unwrap();
    assert!(matches!(first_view, BootstrapView::Ready { .. }));
    assert!(matches!(second_view, BootstrapView::Ready { .. }));

    let deadline = Instant::now() + PROOF_TIMEOUT;
    loop {
        let first_running = match first_lifecycle.spawned_daemon.as_mut() {
            Some(spawned) => spawned.child.try_wait().unwrap().is_none(),
            None => false,
        };
        let second_running = match second_lifecycle.spawned_daemon.as_mut() {
            Some(spawned) => spawned.child.try_wait().unwrap().is_none(),
            None => false,
        };
        if usize::from(first_running) + usize::from(second_running) == 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "daemon race did not select one winner"
        );
        thread::sleep(Duration::from_millis(20));
    }
    let winner_lock = fs::read(&lock_path).unwrap();
    thread::sleep(Duration::from_millis(50));
    assert_eq!(fs::read(&lock_path).unwrap(), winner_lock);

    let mut client = connect_hello_bounded(&socket_path, &workspace_root);
    assert_eq!(
        client.shutdown_if_idle().unwrap().result,
        ShutdownIfIdleResultName::Shutdown
    );
    drop(client);
    wait_for_lock_removal(&lock_path);
    for lifecycle in [&mut first_lifecycle, &mut second_lifecycle] {
        if let Some(spawned) = lifecycle.spawned_daemon.as_mut() {
            wait_for_child_exit(&mut spawned.child);
        }
    }
    assert!(DaemonClient::connect(&socket_path).is_err());
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
        match DaemonClient::connect(socket_path) {
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

fn wait_for_lock_removal(lock_path: &Path) {
    let deadline = Instant::now() + PROOF_TIMEOUT;
    while lock_path.exists() {
        assert!(
            Instant::now() < deadline,
            "daemon did not remove lock {}",
            lock_path.display()
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn wait_for_child_exit(child: &mut Child) {
    let deadline = Instant::now() + PROOF_TIMEOUT;
    loop {
        if child.try_wait().unwrap().is_some() {
            return;
        }
        assert!(Instant::now() < deadline, "daemon child did not exit");
        thread::sleep(Duration::from_millis(20));
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
            read_http_request(&mut stream);
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

fn write_provider_config(path: &Path, base_url: &str) {
    fs::write(
        path,
        format!(
            r#"[provider]
kind = "open_ai"
model = "test-model"
api_key_env = "PATH"
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
