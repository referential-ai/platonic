#![cfg(unix)]

use platonic_client::paths;
use serde_json::Value;
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    sync::Mutex,
    thread,
    time::{Duration, Instant},
};

const TIMEOUT: Duration = Duration::from_secs(10);
static SERVER_TEST: Mutex<()> = Mutex::new(());

#[test]
fn daemon_command_execs_sibling_with_argv_output_and_exit_status() {
    let output = Command::new(env!("CARGO_BIN_EXE_platonic"))
        .arg("--help")
        .output()
        .unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    for command in ["serve", "status", "shutdown", "workspace", "agent"] {
        assert!(help.contains(command), "missing {command} in:\n{help}");
    }
}

#[test]
fn daemon_command_is_replaced_by_the_signal_target() {
    let _guard = SERVER_TEST.lock().unwrap();
    let fixture = ServerFixture::start("status-shutdown");

    let status = fixture
        .command(["status", "--workspace", "."])
        .output()
        .unwrap();
    assert_success(&status);
    let value: Value = serde_json::from_slice(&status.stdout).unwrap();
    assert!(
        value["daemon"]["endpoint_path"]
            .as_str()
            .unwrap()
            .ends_with("agent.sock")
    );

    let shutdown = fixture
        .command(["shutdown", "--workspace", "."])
        .output()
        .unwrap();
    assert_success(&shutdown);
    let value: Value = serde_json::from_slice(&shutdown.stdout).unwrap();
    assert_eq!(value["result"], "shutdown");
    fixture.finish_after_shutdown();
}

#[test]
fn gateway_command_hellos_then_execs_sibling_with_environment_and_exit_status() {
    let _guard = SERVER_TEST.lock().unwrap();
    let fixture = ServerFixture::start("workspace-verbs");
    let registered = fixture.root.path().join("registered");
    fs::create_dir(&registered).unwrap();

    let created = fixture
        .command(["workspace", "create", "proof", registered.to_str().unwrap()])
        .output()
        .unwrap();
    assert_success(&created);
    let created: Value = serde_json::from_slice(&created.stdout).unwrap();
    let id = created["workspace"]["id"].as_str().unwrap();

    let listed = fixture.command(["workspace", "list"]).output().unwrap();
    assert_success(&listed);
    let listed: Value = serde_json::from_slice(&listed.stdout).unwrap();
    assert!(listed["workspaces"].as_array().unwrap().iter().any(|item| {
        item["id"] == id && item["name"] == "proof" && item["health"] == "present"
    }));

    let status = fixture
        .command(["workspace", "status", id])
        .output()
        .unwrap();
    assert_success(&status);
    let status: Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(status["workspace"]["id"], id);
}

#[test]
fn agent_commands_resolve_config_defaults_and_explicit_overrides_end_to_end() {
    let _guard = SERVER_TEST.lock().unwrap();
    let fixture = ServerFixture::start("agent-verbs");
    fs::write(
        fixture.workspace.join("agent.toml"),
        r#"
[provider]
model = "configured-model"
api_key_env = "PLATONIC_AGENT_TEST_KEY"

[tools]
enabled = ["file.read"]
"#,
    )
    .unwrap();
    let workspaces = fixture.command(["workspace", "list"]).output().unwrap();
    assert_success(&workspaces);
    let workspaces: Value = serde_json::from_slice(&workspaces.stdout).unwrap();
    let workspace_id = workspaces["workspaces"]
        .as_array()
        .unwrap()
        .iter()
        .find(|workspace| workspace["name"] == "agent-verbs")
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();

    let configured = fixture
        .command([
            "agent",
            "create",
            "builder",
            &workspace_id,
            "--reasoning-effort",
            "high",
            "--approval-policy",
            "yolo",
            "--config",
            "agent.toml",
        ])
        .env("PLATONIC_AGENT_TEST_KEY", "test-key")
        .output()
        .unwrap();
    assert_success(&configured);
    let configured: Value = serde_json::from_slice(&configured.stdout).unwrap();
    assert_eq!(configured["agent"]["id"], "builder");
    assert_eq!(configured["agent"]["workspace_id"], workspace_id);
    assert_eq!(configured["agent"]["model"], "configured-model");
    assert_eq!(configured["agent"]["reasoning_effort"], "high");
    assert_eq!(configured["agent"]["approval_policy"], "yolo");
    assert_eq!(
        configured["agent"]["toolset"],
        serde_json::json!(["file.read"])
    );

    let overridden = fixture
        .command([
            "agent",
            "create",
            "reviewer",
            &workspace_id,
            "--model",
            "override-model",
            "--tool",
            "file.list",
            "--config",
            "agent.toml",
        ])
        .env("PLATONIC_AGENT_TEST_KEY", "test-key")
        .output()
        .unwrap();
    assert_success(&overridden);
    let overridden: Value = serde_json::from_slice(&overridden.stdout).unwrap();
    assert_eq!(overridden["agent"]["model"], "override-model");
    assert_eq!(overridden["agent"]["reasoning_effort"], "none");
    assert_eq!(overridden["agent"]["approval_policy"], "prompt");
    assert_eq!(
        overridden["agent"]["toolset"],
        serde_json::json!(["file.list"])
    );

    let listed = fixture.command(["agent", "list"]).output().unwrap();
    assert_success(&listed);
    let listed: Value = serde_json::from_slice(&listed.stdout).unwrap();
    assert_eq!(listed["agents"].as_array().unwrap().len(), 2);
    let status = fixture
        .command(["agent", "status", "builder"])
        .output()
        .unwrap();
    assert_success(&status);
    let status: Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(status["agent"], configured["agent"]);
}

#[test]
fn agent_create_refuses_a_missing_configured_provider_key_without_a_row() {
    let _guard = SERVER_TEST.lock().unwrap();
    let fixture = ServerFixture::start("agent-missing-key");
    fs::write(
        fixture.workspace.join("agent.toml"),
        "[provider]\napi_key_env = \"PLATONIC_AGENT_MISSING_KEY\"\n",
    )
    .unwrap();
    let workspaces = fixture.command(["workspace", "list"]).output().unwrap();
    assert_success(&workspaces);
    let workspaces: Value = serde_json::from_slice(&workspaces.stdout).unwrap();
    let workspace_id = workspaces["workspaces"][0]["id"].as_str().unwrap();

    let refused = fixture
        .command([
            "agent",
            "create",
            "builder",
            workspace_id,
            "--config",
            "agent.toml",
        ])
        .env_remove("PLATONIC_AGENT_MISSING_KEY")
        .output()
        .unwrap();
    assert!(!refused.status.success());
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(
        stderr.contains("set PLATONIC_AGENT_MISSING_KEY"),
        "{stderr}"
    );
    assert!(stderr.contains("provider.api_key_env"), "{stderr}");
    let listed = fixture.command(["agent", "list"]).output().unwrap();
    assert_success(&listed);
    let listed: Value = serde_json::from_slice(&listed.stdout).unwrap();
    assert!(listed["agents"].as_array().unwrap().is_empty());
}

#[test]
fn gateway_probe_failures_never_launch_a_service_binary() {
    let _guard = SERVER_TEST.lock().unwrap();
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    fs::create_dir(&workspace).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_platonic"))
        .args(["status", "--workspace", "."])
        .current_dir(&workspace)
        .env("XDG_RUNTIME_DIR", root.path().join("runtime"))
        .env("XDG_STATE_HOME", root.path().join("state"))
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        !root
            .path()
            .join("runtime/platonic/host/agent.sock")
            .exists()
    );
}

#[test]
fn gateway_wrapper_rejects_workspace_gateway_before_daemon_or_service_access() {
    let _guard = SERVER_TEST.lock().unwrap();
    let fixture = ServerFixture::start("gateway-config");
    fs::write(
        fixture.workspace.join("plato.toml"),
        "[gateway.discord]\napi_key_env = \"DISCORD_BOT_TOKEN\"\n[gateway.discord.channel_threads]\n\"200\" = \"thread_news\"\n",
    )
    .unwrap();
    let output = fixture
        .command(["gateway", "discord", "--workspace", "."])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("gateway"), "unexpected stderr: {stderr}");
}

struct ServerFixture {
    root: tempfile::TempDir,
    workspace: PathBuf,
    name: String,
    child: Option<Child>,
}

impl ServerFixture {
    fn start(name: &str) -> Self {
        let root = tempfile::Builder::new()
            .prefix(&format!("p468-{name}-"))
            .tempdir()
            .unwrap();
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let workspace = root.path().join("workspace");
        let runtime = root.path().join("runtime");
        let state = root.path().join("state");
        fs::create_dir(&workspace).unwrap();
        fs::create_dir(&runtime).unwrap();
        fs::set_permissions(&runtime, fs::Permissions::from_mode(0o700)).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_platonic"))
            .arg("serve")
            .current_dir(&workspace)
            .env("XDG_RUNTIME_DIR", &runtime)
            .env("XDG_STATE_HOME", &state)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let fixture = Self {
            root,
            workspace,
            name: name.into(),
            child: Some(child),
        };
        fixture.wait_ready();
        fixture
    }

    fn command<const N: usize>(&self, args: [&str; N]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_platonic"));
        command
            .args(args)
            .current_dir(&self.workspace)
            .env("XDG_RUNTIME_DIR", self.root.path().join("runtime"))
            .env("XDG_STATE_HOME", self.root.path().join("state"));
        command
    }

    fn wait_ready(&self) {
        let socket = self.with_env(paths::host_socket_path).unwrap();
        let deadline = Instant::now() + TIMEOUT;
        while Instant::now() < deadline {
            if socket.is_socket() {
                let root = self.workspace.to_str().unwrap();
                let created = self
                    .command(["workspace", "create", &self.name, root])
                    .output()
                    .unwrap();
                let already_created =
                    String::from_utf8_lossy(&created.stderr).contains("workspace already exists");
                if !created.status.success() && !already_created {
                    thread::sleep(Duration::from_millis(25));
                    continue;
                }
                let output = self
                    .command(["status", "--workspace", "."])
                    .output()
                    .unwrap();
                if output.status.success() {
                    return;
                }
            }
            thread::sleep(Duration::from_millis(25));
        }
        panic!("server did not become ready at {}", socket.display());
    }

    fn finish_after_shutdown(mut self) {
        let mut child = self.child.take().unwrap();
        let deadline = Instant::now() + TIMEOUT;
        while Instant::now() < deadline {
            if child.try_wait().unwrap().is_some() {
                let status = child.wait().unwrap();
                assert!(status.success());
                return;
            }
            thread::sleep(Duration::from_millis(25));
        }
        let _ = child.kill();
        let _ = child.wait();
        panic!("server did not exit after shutdown");
    }

    fn with_env<T>(&self, f: impl FnOnce() -> T) -> T {
        let runtime = self.root.path().join("runtime");
        let state = self.root.path().join("state");
        temp_env::with_vars(
            [
                ("XDG_RUNTIME_DIR", Some(runtime.as_os_str())),
                ("XDG_STATE_HOME", Some(state.as_os_str())),
            ],
            f,
        )
    }
}

impl Drop for ServerFixture {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "command failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

trait IsSocket {
    fn is_socket(&self) -> bool;
}

impl IsSocket for Path {
    fn is_socket(&self) -> bool {
        std::fs::metadata(self)
            .map(|metadata| std::os::unix::fs::FileTypeExt::is_socket(&metadata.file_type()))
            .unwrap_or(false)
    }
}
