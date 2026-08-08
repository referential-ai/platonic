#[cfg(windows)]
use plato_agent::daemon::installer_gate::InstallerStartupGate;
use plato_agent::{
    VoiceEvent,
    daemon::{client::DaemonClient, lock::LockMetadata, protocol::ShutdownIfIdleResultName},
    ledger::SqliteLedger,
    paths,
};
use platonic_core::{
    AgentId, ContextPack, HarnessEvent, Message, MessageRole, ModelName, ModelUsage, RecordedEvent,
    RunId, TurnId,
};
use rusqlite::{Connection, params};
use serde_json::json;
use std::{
    ffi::OsString,
    fs,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Output, Stdio},
    sync::mpsc::{self, Receiver, SyncSender},
    thread,
    time::{Duration, Instant},
};

const PROOF_TIMEOUT: Duration = Duration::from_secs(15);
const POLL_INTERVAL: Duration = Duration::from_millis(10);
const API_KEY_ENV: &str = "PLATO_SQLITE_LOCK_TEST_KEY";
#[cfg(windows)]
const WINDOWS_REPEAT_COUNT: usize = 25;

#[cfg(windows)]
#[test]
fn independent_daemon_startups_wait_for_installer_gate_release() {
    for iteration in 1..=WINDOWS_REPEAT_COUNT {
        eprintln!("independent daemon-startup proof {iteration}/{WINDOWS_REPEAT_COUNT}");
        independent_daemon_startups_wait_for_installer_gate_release_once();
    }
}

#[cfg(windows)]
fn independent_daemon_startups_wait_for_installer_gate_release_once() {
    let gate = InstallerStartupGate::acquire_for_daemon_startup().unwrap();
    let first_proof = ProofContext::new();
    let second_proof = ProofContext::new();
    let mut first = ProofDaemon::spawn_process(&first_proof);
    let mut second = ProofDaemon::spawn_process(&second_proof);

    first.assert_waiting_for_installer_gate(&first_proof);
    second.assert_waiting_for_installer_gate(&second_proof);
    drop(gate);

    first.wait_until_ready();
    second.wait_until_ready();
    assert!(
        first_proof.lock_path.exists(),
        "first independent daemon {} did not own fixture lock {}",
        first.id(),
        first_proof.lock_path.display()
    );
    assert!(
        second_proof.lock_path.exists(),
        "second independent daemon {} did not own fixture lock {}",
        second.id(),
        second_proof.lock_path.display()
    );
    first.stop();
    second.stop();
    assert!(
        !first_proof.lock_path.exists(),
        "first independent daemon left fixture lock {}",
        first_proof.lock_path.display()
    );
    assert!(
        !second_proof.lock_path.exists(),
        "second independent daemon left fixture lock {}",
        second_proof.lock_path.display()
    );
    assert!(
        DaemonClient::connect(&first_proof.socket_path).is_err(),
        "first independent daemon left fixture endpoint {}",
        first_proof.socket_path.display()
    );
    assert!(
        DaemonClient::connect(&second_proof.socket_path).is_err(),
        "second independent daemon left fixture endpoint {}",
        second_proof.socket_path.display()
    );
}

#[test]
fn daemon_first_blocks_direct_sqlite_but_allows_jsonl_and_delegated_prompts() {
    let proof = ProofContext::new();
    let provider = FakeProvider::start(["jsonl answer", "delegated answer", "continued answer"]);
    let config_path = proof.workspace.join("plato.toml");
    write_provider_config(&config_path, &provider.base_url);
    let explicit_db = proof.workspace.join("explicit.db");
    let daemon = ProofDaemon::start(&proof);

    let direct_cases = [
        (
            "default yolo run",
            vec![
                "--yolo".into(),
                "--config".into(),
                config_path.as_os_str().into(),
                "blocked default run".into(),
            ],
        ),
        (
            "explicit run",
            vec![
                format!("--db={}", explicit_db.display()).into(),
                "--config".into(),
                config_path.as_os_str().into(),
                "blocked explicit run".into(),
            ],
        ),
        (
            "default yolo continuation",
            vec![
                "--yolo".into(),
                "-c".into(),
                "--config".into(),
                config_path.as_os_str().into(),
                "blocked default continuation".into(),
            ],
        ),
        (
            "explicit continuation",
            vec![
                format!("--db={}", explicit_db.display()).into(),
                "-c".into(),
                "--config".into(),
                config_path.as_os_str().into(),
                "blocked explicit continuation".into(),
            ],
        ),
        ("default replay", vec!["replay".into()]),
        (
            "explicit replay",
            vec![
                "replay".into(),
                format!("--db={}", explicit_db.display()).into(),
            ],
        ),
    ];

    for (label, arguments) in direct_cases {
        let output = proof.cli_output(&arguments);
        assert_lock_conflict(label, &output, daemon.id());
    }
    assert!(
        !explicit_db.exists(),
        "direct SQLite conflict opened the explicit database"
    );

    let events_path = proof.workspace.join("events.jsonl");
    let jsonl_run = proof.cli_output(&[
        "--events".into(),
        events_path.as_os_str().into(),
        "--config".into(),
        config_path.as_os_str().into(),
        "jsonl question".into(),
    ]);
    assert_success("JSONL run with live daemon", &jsonl_run);
    assert_eq!(
        String::from_utf8(jsonl_run.stdout).unwrap(),
        "jsonl answer\n"
    );

    let jsonl_replay = proof.cli_output(&["replay".into(), events_path.as_os_str().into()]);
    assert_success("JSONL replay with live daemon", &jsonl_replay);
    assert!(
        String::from_utf8(jsonl_replay.stdout)
            .unwrap()
            .contains("assistant: jsonl answer")
    );

    let delegated = proof.cli_output(&[
        "--config".into(),
        config_path.as_os_str().into(),
        "delegated question".into(),
    ]);
    assert_success("delegated fresh prompt", &delegated);
    assert_eq!(
        String::from_utf8(delegated.stdout).unwrap(),
        "delegated answer\n"
    );

    let continued = proof.cli_output(&[
        "-c".into(),
        "--config".into(),
        config_path.as_os_str().into(),
        "delegated follow up".into(),
    ]);
    assert_success("delegated continuation", &continued);
    assert_eq!(
        String::from_utf8(continued.stdout).unwrap(),
        "continued answer\n"
    );

    let requests = provider.join();
    assert_eq!(requests.len(), 3);
    assert!(requests[0].contains("jsonl question"));
    assert!(requests[1].contains("delegated question"));
    assert!(requests[2].contains("delegated question"));
    assert!(requests[2].contains("delegated answer"));
    assert!(requests[2].contains("delegated follow up"));
    daemon.stop();
}

#[test]
fn direct_default_run_blocks_daemon_then_releases_normally() {
    let proof = ProofContext::new();
    let provider = BlockingProvider::start("direct answer");
    let config_path = proof.workspace.join("plato.toml");
    write_provider_config(&config_path, &provider.base_url);
    let mut child = proof
        .plato_command()
        .arg("--config")
        .arg(&config_path)
        .arg("direct fallback question")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let request = provider.wait_for_request();
    assert!(request.contains("direct fallback question"));
    let metadata = wait_for_lock_owner(&proof.lock_path, child.id(), &mut child);
    assert_eq!(metadata.pid, child.id());
    assert_daemon_blocked_by_cli(&proof, child.id());

    provider.finish();
    let output = child.wait_with_output().unwrap();
    assert_success("direct default run", &output);
    assert_eq!(String::from_utf8(output.stdout).unwrap(), "direct answer\n");

    ProofDaemon::start(&proof).stop();
}

#[test]
fn direct_continuation_lookup_holds_lock_and_abrupt_exit_releases_it() {
    let proof = ProofContext::new();
    let explicit_db = proof.workspace.join("continuation.db");
    seed_sqlite_session(
        &explicit_db,
        "session_1",
        "run_1",
        "prior question",
        "prior answer",
    );
    let provider = BlockingProvider::start("follow-up answer");
    let config_path = proof.workspace.join("plato.toml");
    write_provider_config(&config_path, &provider.base_url);
    let mut child = proof
        .plato_command()
        .arg(format!("--db={}", explicit_db.display()))
        .arg("-c")
        .arg("--config")
        .arg(&config_path)
        .arg("follow-up question")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let request = provider.wait_for_request();
    assert!(request.contains("prior question"));
    assert!(request.contains("prior answer"));
    assert!(request.contains("follow-up question"));
    wait_for_lock_owner(&proof.lock_path, child.id(), &mut child);
    assert_daemon_blocked_by_cli(&proof, child.id());

    child.kill().unwrap();
    let status = child.wait().unwrap();
    assert!(!status.success(), "killed direct CLI exited successfully");
    provider.finish();

    ProofDaemon::start(&proof).stop();
}

#[test]
fn direct_replay_holds_lock_through_final_stdout() {
    let proof = ProofContext::new();
    let explicit_db = proof.workspace.join("replay.db");
    let answer = format!(
        "BEGIN LARGE REPLAY\n{}\nEND LARGE REPLAY",
        "x".repeat(4 * 1024 * 1024)
    );
    seed_sqlite_session(
        &explicit_db,
        "session_replay",
        "run_replay",
        "replay question",
        &answer,
    );
    let mut child = proof
        .plato_command()
        .arg("replay")
        .arg(format!("--db={}", explicit_db.display()))
        .arg("--run")
        .arg("run_replay")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    wait_for_lock_owner(&proof.lock_path, child.id(), &mut child);
    thread::sleep(Duration::from_millis(250));
    assert!(
        child.try_wait().unwrap().is_none(),
        "replay exited before its piped final output was drained"
    );
    assert_daemon_blocked_by_cli(&proof, child.id());

    let mut stdout = child.stdout.take().unwrap();
    let reader = thread::spawn(move || {
        let mut output = Vec::new();
        stdout.read_to_end(&mut output).unwrap();
        output
    });
    let status = wait_bounded(&mut child, PROOF_TIMEOUT);
    let stdout = reader.join().unwrap();
    let stderr = read_pipe(child.stderr.take());
    assert!(
        status.success(),
        "replay failed: {}",
        String::from_utf8_lossy(&stderr)
    );
    let stdout = String::from_utf8(stdout).unwrap();
    assert!(stdout.contains("BEGIN LARGE REPLAY"));
    assert!(stdout.contains("END LARGE REPLAY"));

    ProofDaemon::start(&proof).stop();
}

#[test]
fn replay_cli_reads_literal_v1_without_mutation_and_rejects_future_schema() {
    let proof = ProofContext::new();
    let workspace_id = paths::workspace_id(&proof.workspace).unwrap();
    #[cfg(unix)]
    let v1_path = proof
        .state_root
        .join("platonic")
        .join("workspaces")
        .join(&workspace_id)
        .join("agent.db");
    #[cfg(windows)]
    let v1_path = proof
        .local_app_data
        .join("platonic")
        .join("workspaces")
        .join(&workspace_id)
        .join("agent.db");
    write_literal_v1_sqlite(&v1_path);
    let bytes_before = fs::read(&v1_path).unwrap();

    let latest = proof.cli_output(&["replay".into()]);
    assert_success("literal v1 latest replay", &latest);
    let exact = proof.cli_output(&["replay".into(), "--run".into(), "run_v1".into()]);
    assert_success("literal v1 exact replay", &exact);
    assert_eq!(latest.stdout, exact.stdout);
    assert!(
        String::from_utf8(latest.stdout)
            .unwrap()
            .contains("assistant: old answer")
    );
    assert_eq!(fs::read(&v1_path).unwrap(), bytes_before);

    let v6_path = proof.workspace.join("schema-v6.db");
    let connection = Connection::open(&v6_path).unwrap();
    connection.pragma_update(None, "user_version", 6).unwrap();
    drop(connection);
    let v6_bytes_before = fs::read(&v6_path).unwrap();
    let future = proof.cli_output(&[
        "replay".into(),
        format!("--db={}", v6_path.display()).into(),
    ]);
    assert!(!future.status.success());
    assert!(future.stdout.is_empty());
    assert!(
        String::from_utf8(future.stderr)
            .unwrap()
            .contains("sqlite schema version mismatch: expected 5, actual 6")
    );
    assert_eq!(fs::read(&v6_path).unwrap(), v6_bytes_before);
}

#[test]
fn selected_run_cli_replays_typed_voice_companion_without_writes() {
    let proof = ProofContext::new();
    let path = proof.workspace.join("voice-replay.db");
    seed_sqlite_session(
        &path,
        "session_voice",
        "run_voice",
        "voice question",
        "voice answer",
    );
    let run_id = RunId::new("run_voice").unwrap();
    let turn_id = TurnId::new("turn_run_voice").unwrap();
    let events = [
        VoiceEvent::VoiceSpoken {
            run_id: run_id.clone(),
            turn_id: turn_id.clone(),
            ttfa_ms: 274,
            sentence_count: 2,
            interrupted_at: Some(1),
        },
        VoiceEvent::VoiceInterrupted {
            run_id,
            turn_id,
            spoken_prefix: "voice answer".into(),
            delta_index: 6,
        },
    ];
    let mut ledger = SqliteLedger::open_or_create(&path).unwrap();
    let envelopes = ledger.append_voice_events(&events).unwrap();
    drop(ledger);
    let bytes_before = fs::read(&path).unwrap();

    let arguments = [
        "replay".into(),
        format!("--db={}", path.display()).into(),
        "--run".into(),
        "run_voice".into(),
    ];
    let first = proof.cli_output(&arguments);
    let second = proof.cli_output(&arguments);
    assert_success("first selected voice replay", &first);
    assert_success("second selected voice replay", &second);
    assert_eq!(first.stdout, second.stdout);
    let stdout = String::from_utf8(first.stdout).unwrap();
    for envelope in envelopes {
        assert!(stdout.contains(&format!(
            "voice_event: {}",
            serde_json::to_string(&envelope).unwrap()
        )));
    }
    assert_eq!(fs::read(&path).unwrap(), bytes_before);
}

struct ProofContext {
    _root: tempfile::TempDir,
    workspace: PathBuf,
    lock_path: PathBuf,
    socket_path: PathBuf,
    #[cfg(unix)]
    runtime_root: PathBuf,
    #[cfg(unix)]
    state_root: PathBuf,
    #[cfg(windows)]
    local_app_data: PathBuf,
}

impl ProofContext {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        fs::create_dir(&workspace).unwrap();
        let workspace_id = paths::workspace_id(&workspace).unwrap();

        #[cfg(unix)]
        let (lock_path, socket_path, runtime_root, state_root) = {
            let runtime_root = root.path().join("runtime");
            let state_root = root.path().join("state");
            let workspace_runtime = runtime_root
                .join("platonic")
                .join("workspaces")
                .join(&workspace_id);
            (
                workspace_runtime.join("agent.lock"),
                workspace_runtime.join("agent.sock"),
                runtime_root,
                state_root,
            )
        };

        #[cfg(windows)]
        let (lock_path, socket_path, local_app_data) = {
            let local_app_data = root.path().join("local-app-data");
            let workspace_runtime = local_app_data
                .join("platonic")
                .join("workspaces")
                .join(&workspace_id);
            (
                workspace_runtime.join("agent.lock"),
                PathBuf::from(format!(r"\\.\pipe\plato-agent-{workspace_id}")),
                local_app_data,
            )
        };

        Self {
            _root: root,
            workspace,
            lock_path,
            socket_path,
            #[cfg(unix)]
            runtime_root,
            #[cfg(unix)]
            state_root,
            #[cfg(windows)]
            local_app_data,
        }
    }

    fn apply_environment(&self, command: &mut Command) {
        #[cfg(unix)]
        command
            .env("XDG_RUNTIME_DIR", &self.runtime_root)
            .env("XDG_STATE_HOME", &self.state_root);
        #[cfg(windows)]
        command.env("LOCALAPPDATA", &self.local_app_data);
        command.env(API_KEY_ENV, "test-key");
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

    fn cli_output(&self, arguments: &[OsString]) -> Output {
        self.plato_command().args(arguments).output().unwrap()
    }
}

struct ProofDaemon {
    child: Option<Child>,
    workspace: PathBuf,
    socket_path: PathBuf,
}

impl ProofDaemon {
    fn start(proof: &ProofContext) -> Self {
        let mut daemon = Self::spawn_process(proof);
        daemon.wait_until_ready();
        daemon
    }

    fn spawn_process(proof: &ProofContext) -> Self {
        Self {
            child: Some(proof.daemon_command().spawn().unwrap()),
            workspace: proof.workspace.clone(),
            socket_path: proof.socket_path.clone(),
        }
    }

    fn wait_until_ready(&mut self) {
        wait_for_daemon(
            &self.socket_path,
            &self.workspace,
            self.child.as_mut().unwrap(),
        );
    }

    #[cfg(windows)]
    fn assert_waiting_for_installer_gate(&mut self, proof: &ProofContext) {
        let child = self.child.as_mut().unwrap();
        let deadline = Instant::now() + Duration::from_millis(250);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                let stderr = read_pipe(child.stderr.take());
                panic!(
                    "independent daemon {} exited instead of waiting on installer gate for fixture {} ({status}): {}",
                    child.id(),
                    proof.lock_path.display(),
                    String::from_utf8_lossy(&stderr)
                );
            }
            assert!(
                !proof.lock_path.exists(),
                "independent daemon {} created fixture lock {} while installer gate was held",
                child.id(),
                proof.lock_path.display()
            );
            assert!(
                DaemonClient::connect(&proof.socket_path).is_err(),
                "independent daemon {} created fixture endpoint {} while installer gate was held",
                child.id(),
                proof.socket_path.display()
            );
            if Instant::now() >= deadline {
                return;
            }
            thread::sleep(POLL_INTERVAL);
        }
    }

    fn id(&self) -> u32 {
        self.child.as_ref().unwrap().id()
    }

    fn stop(mut self) {
        let mut client =
            DaemonClient::connect_with_timeout(&self.socket_path, Duration::from_secs(1)).unwrap();
        client.hello(&self.workspace).unwrap();
        assert_eq!(
            client.shutdown_if_idle().unwrap().result,
            ShutdownIfIdleResultName::Shutdown
        );
        drop(client);
        let mut child = self.child.take().unwrap();
        let status = wait_bounded(&mut child, PROOF_TIMEOUT);
        assert!(status.success(), "daemon shutdown failed: {status}");
    }
}

impl Drop for ProofDaemon {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

struct FakeProvider {
    base_url: String,
    handle: thread::JoinHandle<Vec<String>>,
}

impl FakeProvider {
    fn start<const N: usize>(answers: [&str; N]) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let answers = answers.map(str::to_owned);
        let handle = thread::spawn(move || {
            let deadline = Instant::now() + PROOF_TIMEOUT;
            answers
                .into_iter()
                .map(|answer| {
                    let mut stream = accept_before(&listener, deadline);
                    let request = read_http_request(&mut stream);
                    write_provider_answer(&mut stream, &answer).unwrap();
                    request
                })
                .collect()
        });
        Self { base_url, handle }
    }

    fn join(self) -> Vec<String> {
        self.handle.join().unwrap()
    }
}

struct BlockingProvider {
    base_url: String,
    request: Receiver<String>,
    release: Option<SyncSender<()>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl BlockingProvider {
    fn start(answer: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let answer = answer.to_owned();
        let (request_sender, request) = mpsc::sync_channel(1);
        let (release, release_receiver) = mpsc::sync_channel(1);
        let handle = thread::spawn(move || {
            let mut stream = accept_before(&listener, Instant::now() + PROOF_TIMEOUT);
            request_sender.send(read_http_request(&mut stream)).unwrap();
            release_receiver
                .recv_timeout(PROOF_TIMEOUT)
                .expect("provider response was not released");
            let _ = write_provider_answer(&mut stream, &answer);
        });
        Self {
            base_url,
            request,
            release: Some(release),
            handle: Some(handle),
        }
    }

    fn wait_for_request(&self) -> String {
        self.request
            .recv_timeout(PROOF_TIMEOUT)
            .expect("direct CLI did not reach the provider")
    }

    fn finish(mut self) {
        self.release_and_join();
    }

    fn release_and_join(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        if let Some(handle) = self.handle.take() {
            handle.join().unwrap();
        }
    }
}

impl Drop for BlockingProvider {
    fn drop(&mut self) {
        self.release_and_join();
    }
}

fn assert_daemon_blocked_by_cli(proof: &ProofContext, cli_pid: u32) {
    let output = proof.daemon_command().output().unwrap();
    assert_lock_conflict("daemon startup", &output, cli_pid);
}

fn assert_lock_conflict(label: &str, output: &Output, owner_pid: u32) {
    assert!(
        !output.status.success(),
        "{label} unexpectedly succeeded:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("daemon lock held"),
        "{label} did not report the workspace lock:\n{stderr}"
    );
    assert!(
        stderr.contains(&format!("pid={owner_pid}")),
        "{label} did not report owner pid {owner_pid}:\n{stderr}"
    );
    assert!(
        output.stdout.is_empty(),
        "{label} wrote stdout before failing"
    );
}

fn assert_success(label: &str, output: &Output) {
    assert!(
        output.status.success(),
        "{label} failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn wait_for_lock_owner(lock_path: &Path, expected_pid: u32, child: &mut Child) -> LockMetadata {
    let deadline = Instant::now() + PROOF_TIMEOUT;
    loop {
        if let Ok(raw) = fs::read_to_string(lock_path)
            && let Ok(metadata) = serde_json::from_str::<LockMetadata>(raw.trim())
            && metadata.pid == expected_pid
        {
            return metadata;
        }
        if let Some(status) = child.try_wait().unwrap() {
            let stderr = read_pipe(child.stderr.take());
            panic!(
                "CLI exited before owning its lock ({status}): {}",
                String::from_utf8_lossy(&stderr)
            );
        }
        assert!(
            Instant::now() < deadline,
            "CLI did not acquire {}",
            lock_path.display()
        );
        thread::sleep(POLL_INTERVAL);
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
            let stderr = read_pipe(child.stderr.take());
            panic!(
                "daemon exited before serving ({status}): {}",
                String::from_utf8_lossy(&stderr)
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
        assert!(Instant::now() < deadline, "child process did not exit");
        thread::sleep(POLL_INTERVAL);
    }
}

fn read_pipe(pipe: Option<impl Read>) -> Vec<u8> {
    let mut output = Vec::new();
    if let Some(mut pipe) = pipe {
        pipe.read_to_end(&mut output).unwrap();
    }
    output
}

fn accept_before(listener: &TcpListener, deadline: Instant) -> TcpStream {
    loop {
        match listener.accept() {
            Ok((stream, _)) => return stream,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < deadline, "provider was not called");
                thread::sleep(POLL_INTERVAL);
            }
            Err(error) => panic!("provider accept failed: {error}"),
        }
    }
}

fn read_http_request(stream: &mut TcpStream) -> String {
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
    String::from_utf8(bytes[header_end..header_end + content_length].to_vec()).unwrap()
}

fn write_provider_answer(stream: &mut TcpStream, answer: &str) -> io::Result<()> {
    let body = text_stream(answer);
    write!(
        stream,
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
}

fn text_stream(answer: &str) -> String {
    format!(
        "data: {}\n\ndata: {{\"choices\":[{{\"index\":0,\"delta\":{{}},\"finish_reason\":\"stop\"}}]}}\n\ndata: [DONE]\n\n",
        json!({
            "choices": [{
                "index": 0,
                "delta": {"content": answer},
                "finish_reason": null
            }]
        })
    )
}

fn write_provider_config(path: &Path, base_url: &str) {
    fs::write(
        path,
        format!(
            r#"
[provider]
kind = "open_ai"
model = "test-model"
api_key_env = "{API_KEY_ENV}"
base_url = "{base_url}"
connect_timeout_ms = 15000
stream_idle_timeout_ms = 15000

[limits]
token_budget = 4000
max_output_tokens = 32
max_turns = 2

[tools]
enabled = ["file.read"]
"#
        ),
    )
    .unwrap();
}

fn write_literal_v1_sqlite(path: &Path) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let connection = Connection::open(path).unwrap();
    connection
        .execute_batch(
            r#"
            CREATE TABLE ledger_events (
              run_id TEXT NOT NULL,
              seq INTEGER NOT NULL,
              occurred_at_ms INTEGER NOT NULL,
              v INTEGER NOT NULL,
              event_json TEXT NOT NULL,
              PRIMARY KEY (run_id, seq)
            );
            PRAGMA user_version = 1;
            "#,
        )
        .unwrap();
    let events = [
        r#"{"event":"run_started","run_id":"run_v1","agent_id":"plato"}"#,
        r#"{"event":"context_built","run_id":"run_v1","turn_id":"turn_1","context":{"fragments":[],"token_budget":4000}}"#,
        r#"{"event":"model_requested","run_id":"run_v1","turn_id":"turn_1","step":0,"model":"test-model"}"#,
        r#"{"event":"model_responded","run_id":"run_v1","turn_id":"turn_1","step":0,"output":{"role":"assistant","content":"old answer"},"proposed_calls":[],"usage":{"input_tokens":8,"output_tokens":3}}"#,
        r#"{"event":"run_finished","run_id":"run_v1"}"#,
    ];
    for (seq, event_json) in events.into_iter().enumerate() {
        connection
            .execute(
                "INSERT INTO ledger_events (run_id, seq, occurred_at_ms, v, event_json)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params!["run_v1", seq as i64, seq as i64, 1, event_json],
            )
            .unwrap();
    }
}

fn seed_sqlite_session(path: &Path, session_id: &str, run_id: &str, question: &str, answer: &str) {
    let run_id = RunId::new(run_id).unwrap();
    let turn_id = TurnId::new(format!("turn_{}", run_id.as_str())).unwrap();
    let mut ledger = SqliteLedger::open_or_create(path).unwrap();
    ledger
        .begin_session_run(session_id, &run_id, question, true)
        .unwrap();
    let events = [
        HarnessEvent::RunStarted {
            run_id: run_id.clone(),
            agent_id: AgentId::new("plato").unwrap(),
        },
        HarnessEvent::ContextBuilt {
            run_id: run_id.clone(),
            turn_id: turn_id.clone(),
            context: ContextPack {
                token_budget: 0,
                fragments: vec![],
            },
        },
        HarnessEvent::ModelRequested {
            run_id: run_id.clone(),
            turn_id: turn_id.clone(),
            step: 0,
            model: ModelName::new("test-model").unwrap(),
        },
        HarnessEvent::ModelResponded {
            run_id: run_id.clone(),
            turn_id,
            step: 0,
            output: Message {
                role: MessageRole::Assistant,
                content: answer.into(),
            },
            proposed_calls: vec![],
            served_model: None,
            usage: Some(ModelUsage {
                input_tokens: 0,
                output_tokens: 0,
            }),
        },
        HarnessEvent::RunFinished {
            run_id: run_id.clone(),
        },
    ];
    for (seq, event) in events.into_iter().enumerate() {
        ledger
            .append(
                run_id.as_str(),
                &RecordedEvent {
                    seq: seq as u64,
                    occurred_at_ms: seq as u64,
                    event,
                },
            )
            .unwrap();
    }
    ledger.finish_session_run(&run_id, answer).unwrap();
}
