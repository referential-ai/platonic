#![cfg(unix)]

use plato_agent::{
    daemon::protocol::{EnvelopeKind, PROTOCOL_VERSION},
    paths,
};
use serde_json::{Value, json};
use std::{
    fs,
    io::{BufRead, BufReader, Write},
    os::unix::{
        fs::PermissionsExt,
        net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
    process::Command,
    thread,
    time::{Duration, Instant},
};

#[test]
fn daemon_command_execs_sibling_with_argv_output_and_exit_status() {
    let workspace = tempfile::tempdir().unwrap();
    let plato = install_plato(workspace.path());
    let args_path = workspace.path().join("daemon-args");
    let environment_path = workspace.path().join("daemon-environment");
    let socket_path = workspace.path().join("custom.sock");
    write_executable(
        &plato.with_file_name("plato-agentd"),
        r#"#!/bin/sh
printf '%s\n' "$@" > "$PLATO_TEST_ARGS"
printf '%s' "$PLATO_TEST_DAEMON_ENV" > "$PLATO_TEST_DAEMON_ENV_OUT"
printf 'daemon stdout\n'
printf 'daemon stderr\n' >&2
exit 23
"#,
    );

    let output = Command::new(&plato)
        .args(["daemon", "--socket"])
        .arg(&socket_path)
        .current_dir(workspace.path())
        .env("PLATO_TEST_ARGS", &args_path)
        .env("PLATO_TEST_DAEMON_ENV", "provider-only")
        .env("PLATO_TEST_DAEMON_ENV_OUT", &environment_path)
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(23));
    assert_eq!(output.stdout, b"daemon stdout\n");
    assert_eq!(output.stderr, b"daemon stderr\n");
    assert_eq!(
        fs::read_to_string(environment_path).unwrap(),
        "provider-only"
    );
    assert_eq!(
        read_lines(&args_path),
        vec![
            "--workspace",
            workspace.path().to_str().unwrap(),
            "--socket",
            socket_path.to_str().unwrap(),
        ]
    );
}

#[test]
fn daemon_command_is_replaced_by_the_signal_target() {
    let workspace = tempfile::tempdir().unwrap();
    let plato = install_plato(workspace.path());
    let ready_path = workspace.path().join("ready");
    let signal_path = workspace.path().join("signal");
    write_executable(
        &plato.with_file_name("plato-agentd"),
        r#"#!/bin/sh
trap 'printf term > "$PLATO_TEST_SIGNAL"; exit 42' TERM
printf ready > "$PLATO_TEST_READY"
while :; do sleep 1; done
"#,
    );
    let mut child = Command::new(&plato)
        .arg("daemon")
        .current_dir(workspace.path())
        .env("PLATO_TEST_READY", &ready_path)
        .env("PLATO_TEST_SIGNAL", &signal_path)
        .spawn()
        .unwrap();
    wait_for_path(&ready_path, &mut child);

    let pid = rustix::process::Pid::from_raw(child.id() as i32).unwrap();
    rustix::process::kill_process(pid, rustix::process::Signal::TERM).unwrap();
    let status = wait_for_exit(&mut child);

    assert_eq!(status.code(), Some(42));
    assert_eq!(fs::read_to_string(signal_path).unwrap(), "term");
}

#[test]
fn gateway_command_hellos_then_execs_sibling_with_environment_and_exit_status() {
    let workspace = tempfile::tempdir().unwrap();
    let plato = install_plato(workspace.path());
    let socket_path = workspace.path().join("agent.sock");
    let config_path = Path::new("gateway.toml");
    let args_path = workspace.path().join("gateway-args");
    let environment_path = workspace.path().join("gateway-environment");
    let daemon_launch_path = workspace.path().join("daemon-launch");
    let hello = spawn_endpoint(&socket_path, {
        let workspace_id = paths::workspace_id(workspace.path()).unwrap();
        move |request, stream| {
            write_response(
                stream,
                json!({
                    "v": PROTOCOL_VERSION,
                    "id": request["id"],
                    "kind": "response",
                    "method": "hello",
                    "result": {
                        "daemon_version": env!("CARGO_PKG_VERSION"),
                        "workspace_id": workspace_id,
                        "ledger_path": "/tmp/agent.db",
                        "capabilities": [
                            "hello",
                            "run.start",
                            "message.append",
                            "events.stream",
                            "sessions.list",
                            "transcript.read"
                        ]
                    }
                }),
            );
            request
        }
    });
    write_executable(
        &plato.with_file_name("plato-gateway-discord"),
        r#"#!/bin/sh
printf '%s\n' "$@" > "$PLATO_TEST_ARGS"
printf '%s' "$PLATO_TEST_GATEWAY_ENV" > "$PLATO_TEST_GATEWAY_ENV_OUT"
printf 'gateway stdout\n'
printf 'gateway stderr\n' >&2
exit 37
"#,
    );
    write_executable(
        &plato.with_file_name("plato-agentd"),
        r#"#!/bin/sh
printf launched > "$PLATO_TEST_DAEMON_LAUNCH"
exit 99
"#,
    );

    let output = gateway_command(&plato, workspace.path(), &socket_path)
        .args(["--config", config_path.to_str().unwrap()])
        .env("PLATO_TEST_ARGS", &args_path)
        .env("PLATO_TEST_GATEWAY_ENV", "discord-only")
        .env("PLATO_TEST_GATEWAY_ENV_OUT", &environment_path)
        .env("PLATO_TEST_DAEMON_LAUNCH", &daemon_launch_path)
        .output()
        .unwrap();
    let request = hello.join().unwrap();

    assert_eq!(output.status.code(), Some(37));
    assert_eq!(output.stdout, b"gateway stdout\n");
    assert_eq!(output.stderr, b"gateway stderr\n");
    assert_eq!(request["v"], PROTOCOL_VERSION);
    assert_eq!(request["kind"], json!(EnvelopeKind::Request));
    assert_eq!(request["method"], "hello");
    assert_eq!(
        request["params"]["workspace_root"].as_str(),
        Some(
            workspace
                .path()
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .as_ref()
        )
    );
    assert_eq!(
        request["params"]["workspace_id"],
        paths::workspace_id(workspace.path()).unwrap()
    );
    assert_eq!(
        read_lines(&args_path),
        vec![
            "--workspace",
            workspace.path().to_str().unwrap(),
            "--socket",
            socket_path.to_str().unwrap(),
            "--config",
            config_path.to_str().unwrap(),
        ]
    );
    assert_eq!(
        fs::read_to_string(environment_path).unwrap(),
        "discord-only"
    );
    assert!(!daemon_launch_path.exists());
}

#[test]
fn gateway_probe_failures_never_launch_a_service_binary() {
    let workspace = tempfile::tempdir().unwrap();
    let plato = install_plato(workspace.path());
    let gateway_launch_path = workspace.path().join("gateway-launch");
    let daemon_launch_path = workspace.path().join("daemon-launch");
    write_launch_marker(
        &plato.with_file_name("plato-gateway-discord"),
        "PLATO_TEST_GATEWAY_LAUNCH",
    );
    write_launch_marker(
        &plato.with_file_name("plato-agentd"),
        "PLATO_TEST_DAEMON_LAUNCH",
    );

    let missing = workspace.path().join("missing.sock");
    assert_probe_failure(
        &plato,
        workspace.path(),
        &missing,
        &gateway_launch_path,
        &daemon_launch_path,
        None,
    );

    let closed = workspace.path().join("closed.sock");
    let server = spawn_endpoint(&closed, |request, _stream| request);
    assert_probe_failure(
        &plato,
        workspace.path(),
        &closed,
        &gateway_launch_path,
        &daemon_launch_path,
        Some(server),
    );

    let incompatible = workspace.path().join("incompatible.sock");
    let server = spawn_endpoint(&incompatible, |request, stream| {
        write_response(
            stream,
            json!({
                "v": PROTOCOL_VERSION + 1,
                "id": request["id"],
                "kind": "response",
                "method": "hello",
                "result": {}
            }),
        );
        request
    });
    assert_probe_failure(
        &plato,
        workspace.path(),
        &incompatible,
        &gateway_launch_path,
        &daemon_launch_path,
        Some(server),
    );

    let wrong_workspace = workspace.path().join("wrong-workspace.sock");
    let server = spawn_endpoint(&wrong_workspace, |request, stream| {
        write_response(
            stream,
            json!({
                "v": PROTOCOL_VERSION,
                "id": request["id"],
                "kind": "error",
                "method": "hello",
                "error": {
                    "code": "workspace_mismatch",
                    "message": "wrong workspace"
                }
            }),
        );
        request
    });
    assert_probe_failure(
        &plato,
        workspace.path(),
        &wrong_workspace,
        &gateway_launch_path,
        &daemon_launch_path,
        Some(server),
    );

    let wrong_result = workspace.path().join("wrong-result.sock");
    let server = spawn_endpoint(&wrong_result, |request, stream| {
        write_response(
            stream,
            json!({
                "v": PROTOCOL_VERSION,
                "id": request["id"],
                "kind": "response",
                "method": "hello",
                "result": {
                    "daemon_version": "test",
                    "workspace_id": "different-workspace",
                    "ledger_path": "/tmp/agent.db",
                    "capabilities": ["hello"]
                }
            }),
        );
        request
    });
    assert_probe_failure(
        &plato,
        workspace.path(),
        &wrong_result,
        &gateway_launch_path,
        &daemon_launch_path,
        Some(server),
    );

    let missing_capability = workspace.path().join("missing-capability.sock");
    let server = spawn_endpoint(&missing_capability, {
        let workspace_id = paths::workspace_id(workspace.path()).unwrap();
        move |request, stream| {
            write_response(
                stream,
                json!({
                    "v": PROTOCOL_VERSION,
                    "id": request["id"],
                    "kind": "response",
                    "method": "hello",
                    "result": {
                        "daemon_version": "test",
                        "workspace_id": workspace_id,
                        "ledger_path": "/tmp/agent.db",
                        "capabilities": []
                    }
                }),
            );
            request
        }
    });
    assert_probe_failure(
        &plato,
        workspace.path(),
        &missing_capability,
        &gateway_launch_path,
        &daemon_launch_path,
        Some(server),
    );

    let stalled = workspace.path().join("stalled.sock");
    let server = spawn_endpoint(&stalled, |request, _stream| {
        thread::sleep(Duration::from_millis(3_100));
        request
    });
    assert_probe_failure(
        &plato,
        workspace.path(),
        &stalled,
        &gateway_launch_path,
        &daemon_launch_path,
        Some(server),
    );
}

fn assert_probe_failure(
    plato: &Path,
    workspace: &Path,
    socket_path: &Path,
    gateway_launch_path: &Path,
    daemon_launch_path: &Path,
    server: Option<thread::JoinHandle<Value>>,
) {
    let output = gateway_command(plato, workspace, socket_path)
        .env("PLATO_TEST_GATEWAY_LAUNCH", gateway_launch_path)
        .env("PLATO_TEST_DAEMON_LAUNCH", daemon_launch_path)
        .env("PLATO_TEST_GATEWAY_ENV", "discord-only")
        .output()
        .unwrap();
    if let Some(server) = server {
        server.join().unwrap();
    }

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("workspace daemon is unavailable or incompatible"));
    assert!(stderr.contains("plato daemon --socket"));
    assert!(!gateway_launch_path.exists());
    assert!(!daemon_launch_path.exists());
}

fn gateway_command(plato: &Path, workspace: &Path, socket_path: &Path) -> Command {
    let mut command = Command::new(plato);
    command
        .args(["gateway", "discord", "--socket"])
        .arg(socket_path)
        .current_dir(workspace);
    command
}

fn install_plato(workspace: &Path) -> PathBuf {
    let bin_dir = workspace.join("bin");
    fs::create_dir(&bin_dir).unwrap();
    let plato = bin_dir.join("plato");
    fs::copy(env!("CARGO_BIN_EXE_plato"), &plato).unwrap();
    let mut permissions = fs::metadata(&plato).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&plato, permissions).unwrap();
    plato
}

fn write_launch_marker(path: &Path, environment_name: &str) {
    write_executable(
        path,
        &format!("#!/bin/sh\nprintf launched > \"${{{environment_name}}}\"\nexit 99\n"),
    );
}

fn write_executable(path: &Path, body: &str) {
    fs::write(path, body).unwrap();
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).unwrap();
}

fn read_lines(path: &Path) -> Vec<String> {
    let contents = fs::read_to_string(path).unwrap();
    contents.lines().map(str::to_owned).collect()
}

fn spawn_endpoint(
    socket_path: &Path,
    respond: impl FnOnce(Value, &mut UnixStream) -> Value + Send + 'static,
) -> thread::JoinHandle<Value> {
    let listener = UnixListener::bind(socket_path).unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        let request = serde_json::from_str(line.trim()).unwrap();
        respond(request, &mut stream)
    })
}

fn write_response(stream: &mut UnixStream, response: Value) {
    serde_json::to_writer(&mut *stream, &response).unwrap();
    stream.write_all(b"\n").unwrap();
    stream.flush().unwrap();
}

fn wait_for_path(path: &Path, child: &mut std::process::Child) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while !path.exists() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("service command exited before becoming ready: {status}");
        }
        assert!(
            Instant::now() < deadline,
            "service command did not become ready"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_exit(child: &mut std::process::Child) -> std::process::ExitStatus {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            panic!("service command did not exit");
        }
        thread::sleep(Duration::from_millis(10));
    }
}
