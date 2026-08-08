#![cfg(unix)]

use plato_agent::{
    daemon::{client::DaemonClient, lock::LockMetadata},
    ledger::SqliteLedger,
    paths,
};
use platonic_core::{AgentId, HarnessEvent, RecordedEvent, RunId};
use std::{
    fs,
    io::Read,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

const PROOF_TIMEOUT: Duration = Duration::from_secs(15);

#[test]
fn live_conflict_crash_recovery_and_persistent_normal_exit() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let runtime = root.path().join("runtime");
    let state = root.path().join("state");
    fs::create_dir(&workspace).unwrap();
    let (lock_path, socket_path) = daemon_paths(&runtime, &workspace);
    let probe_db = root.path().join("probe.db");
    let run_id = RunId::new("run_lock_probe").unwrap();
    let mut ledger = SqliteLedger::open_or_create(&probe_db).unwrap();
    ledger
        .append(
            run_id.as_str(),
            &RecordedEvent {
                seq: 0,
                occurred_at_ms: 0,
                event: HarnessEvent::RunStarted {
                    run_id: run_id.clone(),
                    agent_id: AgentId::new("plato").unwrap(),
                },
            },
        )
        .unwrap();
    ledger
        .append(
            run_id.as_str(),
            &RecordedEvent {
                seq: 1,
                occurred_at_ms: 1,
                event: HarnessEvent::RunFailed {
                    run_id: run_id.clone(),
                    reason: "probe complete".into(),
                },
            },
        )
        .unwrap();
    drop(ledger);

    let mut first = DaemonChild::spawn(&runtime, &state, &workspace);
    let first_metadata = wait_for_lock_owner(&lock_path, first.id(), &mut first);
    assert_eq!(first_metadata.pid, first.id());
    let identity = file_identity(&lock_path);
    let lock_metadata = fs::symlink_metadata(&lock_path).unwrap();
    assert_eq!(lock_metadata.permissions().mode() & 0o7777, 0o600);
    assert_eq!(lock_metadata.uid(), rustix::process::geteuid().as_raw());

    let conflict = daemon_command(&runtime, &state, &workspace)
        .output()
        .unwrap();
    assert!(!conflict.status.success());
    let conflict_stderr = String::from_utf8_lossy(&conflict.stderr);
    assert!(conflict_stderr.contains("daemon lock held"));
    assert!(conflict_stderr.contains(&format!("pid={}", first.id())));

    first.kill().unwrap();
    let killed = first.wait_bounded(PROOF_TIMEOUT);
    assert!(!killed.success());
    assert!(lock_path.exists());
    assert_eq!(file_identity(&lock_path), identity);

    let replay = plato_command(&runtime, &state, &workspace)
        .arg("replay")
        .arg(format!("--db={}", probe_db.display()))
        .arg("--run")
        .arg(run_id.as_str())
        .output()
        .unwrap();
    assert!(
        replay.status.success(),
        "stale-lock CLI probe failed:\n{}",
        String::from_utf8_lossy(&replay.stderr)
    );
    assert!(String::from_utf8_lossy(&replay.stdout).contains("final_phase: Failed"));
    assert_eq!(file_identity(&lock_path), identity);

    let mut recovered = DaemonChild::spawn(&runtime, &state, &workspace);
    let recovered_metadata = wait_for_lock_owner(&lock_path, recovered.id(), &mut recovered);
    assert_eq!(recovered_metadata.pid, recovered.id());
    assert_eq!(file_identity(&lock_path), identity);
    let mut client = wait_for_client(&socket_path, &workspace, &mut recovered);
    client.shutdown_if_idle().unwrap();
    drop(client);
    assert!(recovered.wait_bounded(PROOF_TIMEOUT).success());

    assert!(lock_path.exists());
    assert_eq!(file_identity(&lock_path), identity);
    assert!(DaemonClient::connect(&socket_path).is_err());
}

#[test]
fn concurrent_daemon_acquisition_has_exactly_one_winner() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let runtime = root.path().join("runtime");
    let state = root.path().join("state");
    fs::create_dir(&workspace).unwrap();
    let (lock_path, socket_path) = daemon_paths(&runtime, &workspace);
    let mut children: Vec<_> = (0..8)
        .map(|_| DaemonChild::spawn(&runtime, &state, &workspace))
        .collect();
    let deadline = Instant::now() + PROOF_TIMEOUT;

    loop {
        let alive = children
            .iter_mut()
            .map(|child| child.try_wait().unwrap().is_none())
            .filter(|alive| *alive)
            .count();
        if alive == 1 {
            break;
        }
        assert!(Instant::now() < deadline, "{alive} race contenders remain");
        thread::sleep(Duration::from_millis(10));
    }

    let winner_index = children
        .iter_mut()
        .position(|child| child.try_wait().unwrap().is_none())
        .unwrap();
    for (index, child) in children.iter_mut().enumerate() {
        if index == winner_index {
            continue;
        }
        let status = child.try_wait().unwrap().expect("loser must exit");
        assert!(!status.success());
        assert!(child.read_stderr().contains("daemon lock held"));
    }

    let winner_id = children[winner_index].id();
    let metadata = wait_for_lock_owner(&lock_path, winner_id, &mut children[winner_index]);
    assert_eq!(metadata.pid, winner_id);
    let mut client = wait_for_client(&socket_path, &workspace, &mut children[winner_index]);
    client.shutdown_if_idle().unwrap();
    drop(client);
    assert!(children[winner_index].wait_bounded(PROOF_TIMEOUT).success());
}

fn daemon_paths(runtime: &Path, workspace: &Path) -> (PathBuf, PathBuf) {
    let workspace_id = paths::workspace_id(workspace).unwrap();
    let directory = runtime
        .join("platonic")
        .join("workspaces")
        .join(workspace_id);
    (directory.join("agent.lock"), directory.join("agent.sock"))
}

fn daemon_command(runtime: &Path, state: &Path, workspace: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_plato-agentd"));
    command
        .arg("--workspace")
        .arg(workspace)
        .env("XDG_RUNTIME_DIR", runtime)
        .env("XDG_STATE_HOME", state)
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    command
}

fn plato_command(runtime: &Path, state: &Path, workspace: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_plato"));
    command
        .current_dir(workspace)
        .env("XDG_RUNTIME_DIR", runtime)
        .env("XDG_STATE_HOME", state);
    command
}

fn wait_for_lock_owner(lock_path: &Path, pid: u32, child: &mut DaemonChild) -> LockMetadata {
    let deadline = Instant::now() + PROOF_TIMEOUT;
    loop {
        if let Ok(raw) = fs::read_to_string(lock_path)
            && let Ok(metadata) = serde_json::from_str::<LockMetadata>(raw.trim())
            && metadata.pid == pid
        {
            return metadata;
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!(
                "daemon exited before owning its lock ({status}): {}",
                child.read_stderr()
            );
        }
        assert!(
            Instant::now() < deadline,
            "daemon did not acquire {}",
            lock_path.display()
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_client(socket_path: &Path, workspace: &Path, child: &mut DaemonChild) -> DaemonClient {
    let deadline = Instant::now() + PROOF_TIMEOUT;
    loop {
        if let Ok(mut client) = DaemonClient::connect(socket_path)
            && client.hello(workspace).is_ok()
        {
            return client;
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!(
                "daemon exited before accepting clients ({status}): {}",
                child.read_stderr()
            );
        }
        assert!(Instant::now() < deadline, "daemon did not accept clients");
        thread::sleep(Duration::from_millis(10));
    }
}

fn file_identity(path: &Path) -> (u64, u64) {
    let metadata = fs::symlink_metadata(path).unwrap();
    (metadata.dev(), metadata.ino())
}

struct DaemonChild {
    child: Child,
}

impl DaemonChild {
    fn spawn(runtime: &Path, state: &Path, workspace: &Path) -> Self {
        Self {
            child: daemon_command(runtime, state, workspace).spawn().unwrap(),
        }
    }

    fn id(&self) -> u32 {
        self.child.id()
    }

    fn kill(&mut self) -> std::io::Result<()> {
        self.child.kill()
    }

    fn try_wait(&mut self) -> std::io::Result<Option<ExitStatus>> {
        self.child.try_wait()
    }

    fn wait_bounded(&mut self, timeout: Duration) -> ExitStatus {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(Instant::now() < deadline, "daemon did not exit");
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn read_stderr(&mut self) -> String {
        let mut stderr = String::new();
        if let Some(mut pipe) = self.child.stderr.take() {
            pipe.read_to_string(&mut stderr).unwrap();
        }
        stderr
    }
}

impl Drop for DaemonChild {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}
