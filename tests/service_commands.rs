#![cfg(unix)]

use plato_agent::{
    daemon::protocol::{EnvelopeKind, PROTOCOL_VERSION},
    paths,
};
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io::{self, BufRead, BufReader, Write},
    os::unix::{
        fs::PermissionsExt,
        net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    sync::{Mutex, MutexGuard},
    thread,
    time::{Duration, Instant},
};

static EXECUTABLE_FIXTURE_SETUP: Mutex<()> = Mutex::new(());

#[test]
fn daemon_command_execs_sibling_with_argv_output_and_exit_status() {
    let workspace = tempfile::tempdir().unwrap();
    let (plato, fixture_setup) = install_plato(workspace.path(), "daemon-exit-status");
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

    let mut command = Command::new(&plato);
    command
        .args(["daemon", "--socket"])
        .arg(&socket_path)
        .current_dir(workspace.path())
        .env("PLATO_TEST_ARGS", &args_path)
        .env("PLATO_TEST_DAEMON_ENV", "provider-only")
        .env("PLATO_TEST_DAEMON_ENV_OUT", &environment_path);
    let output = fixture_output(command, &plato, fixture_setup);

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
    let (plato, fixture_setup) = install_plato(workspace.path(), "daemon-signal");
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
    let mut command = Command::new(&plato);
    command
        .arg("daemon")
        .current_dir(workspace.path())
        .env("PLATO_TEST_READY", &ready_path)
        .env("PLATO_TEST_SIGNAL", &signal_path);
    let mut child = spawn_fixture(command, &plato, fixture_setup);
    wait_for_path(&ready_path, &mut child);

    let pid = rustix::process::Pid::from_raw(child.id() as i32).unwrap();
    rustix::process::kill_process(pid, rustix::process::Signal::TERM).unwrap();
    let status = wait_for_exit(&mut child, &signal_path);

    assert_eq!(status.code(), Some(42));
    assert_eq!(fs::read_to_string(signal_path).unwrap(), "term");
}

#[test]
fn gateway_command_hellos_then_execs_sibling_with_environment_and_exit_status() {
    let workspace = tempfile::tempdir().unwrap();
    let (plato, fixture_setup) = install_plato(workspace.path(), "gateway-exit-status");
    let socket_path = workspace.path().join("agent.sock");
    let config_path = Path::new("gateway.toml");
    let args_path = workspace.path().join("gateway-args");
    let environment_path = workspace.path().join("gateway-environment");
    let daemon_launch_path = workspace.path().join("daemon-launch");
    fs::write(workspace.path().join("mapped.toml"), "").unwrap();
    fs::write(
        workspace.path().join(config_path),
        r#"
[gateway.discord]
api_key_env = "DISCORD_BOT_TOKEN"
owner_user_ids = [42]

[gateway.discord.channel_configs]
"200" = "mapped.toml"
"#,
    )
    .unwrap();
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

    let mut command = gateway_command(&plato, workspace.path(), &socket_path);
    command
        .args(["--config", config_path.to_str().unwrap()])
        .env("PLATO_TEST_ARGS", &args_path)
        .env("PLATO_TEST_GATEWAY_ENV", "discord-only")
        .env("PLATO_TEST_GATEWAY_ENV_OUT", &environment_path)
        .env("PLATO_TEST_DAEMON_LAUNCH", &daemon_launch_path);
    let output = fixture_output(command, &plato, fixture_setup);
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
fn gateway_wrapper_rejects_workspace_gateway_before_daemon_or_service_access() {
    let workspace = tempfile::tempdir().unwrap();
    let (plato, fixture_setup) = install_plato(workspace.path(), "gateway-workspace-config");
    let socket_path = workspace.path().join("agent.sock");
    let daemon = UnixListener::bind(&socket_path).unwrap();
    daemon.set_nonblocking(true).unwrap();
    let gateway_launch_path = plato.with_file_name("gateway-launch");
    let daemon_launch_path = plato.with_file_name("daemon-launch");
    write_launch_marker(
        &plato.with_file_name("plato-gateway-discord"),
        "PLATO_TEST_GATEWAY_LAUNCH",
    );
    write_launch_marker(
        &plato.with_file_name("plato-agentd"),
        "PLATO_TEST_DAEMON_LAUNCH",
    );
    fs::write(
        workspace.path().join("plato.toml"),
        r#"
[gateway.discord]
api_key_env = "DISCORD_BOT_TOKEN"
owner_user_ids = [42]

[gateway.discord.channel_configs]
"200" = "mapped.toml"
"#,
    )
    .unwrap();

    let mut command = gateway_command(&plato, workspace.path(), &socket_path);
    command
        .env("PLATO_TEST_GATEWAY_LAUNCH", &gateway_launch_path)
        .env("PLATO_TEST_DAEMON_LAUNCH", &daemon_launch_path);
    let output = fixture_output(command, &plato, fixture_setup);

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("workspace plato.toml cannot set [gateway]")
    );
    assert!(matches!(
        daemon.accept(),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
    ));
    assert!(!gateway_launch_path.exists());
    assert!(!daemon_launch_path.exists());
}

#[test]
fn gateway_probe_failures_never_launch_a_service_binary() {
    let workspace = tempfile::tempdir().unwrap();

    let missing = workspace.path().join("missing.sock");
    assert_probe_failure(workspace.path(), &missing, "missing-endpoint", None);

    let closed = workspace.path().join("closed.sock");
    let server = spawn_endpoint(&closed, |request, _stream| request);
    assert_probe_failure(workspace.path(), &closed, "closed-endpoint", Some(server));

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
        workspace.path(),
        &incompatible,
        "incompatible-protocol",
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
        workspace.path(),
        &wrong_workspace,
        "workspace-error",
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
        workspace.path(),
        &wrong_result,
        "wrong-workspace-result",
        Some(server),
    );

    let required_capabilities = [
        "hello",
        "run.start",
        "message.append",
        "events.stream",
        "sessions.list",
        "transcript.read",
    ];
    for missing in required_capabilities {
        let socket = workspace
            .path()
            .join(format!("missing-{}.sock", missing.replace('.', "-")));
        let capabilities = required_capabilities
            .iter()
            .filter(|capability| **capability != missing)
            .copied()
            .collect::<Vec<_>>();
        let server = spawn_endpoint(&socket, {
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
                            "capabilities": capabilities
                        }
                    }),
                );
                request
            }
        });
        let fixture_name = format!("missing-{}", missing.replace('.', "-"));
        let stderr = assert_probe_failure(workspace.path(), &socket, &fixture_name, Some(server));
        assert!(
            stderr.contains(&format!("required capability {missing}")),
            "{fixture_name} fixture did not name the missing capability: {stderr}"
        );
    }

    let stalled = workspace.path().join("stalled.sock");
    let server = spawn_endpoint(&stalled, |request, _stream| {
        thread::sleep(Duration::from_millis(3_100));
        request
    });
    assert_probe_failure(workspace.path(), &stalled, "stalled-hello", Some(server));
}

fn assert_probe_failure(
    workspace: &Path,
    socket_path: &Path,
    fixture_name: &str,
    server: Option<thread::JoinHandle<Value>>,
) -> String {
    let (plato, fixture_setup) = install_plato(workspace, fixture_name);
    let gateway_launch_path = plato.with_file_name("gateway-launch");
    let daemon_launch_path = plato.with_file_name("daemon-launch");
    write_launch_marker(
        &plato.with_file_name("plato-gateway-discord"),
        "PLATO_TEST_GATEWAY_LAUNCH",
    );
    write_launch_marker(
        &plato.with_file_name("plato-agentd"),
        "PLATO_TEST_DAEMON_LAUNCH",
    );
    let mut command = gateway_command(&plato, workspace, socket_path);
    command
        .env("PLATO_TEST_GATEWAY_LAUNCH", &gateway_launch_path)
        .env("PLATO_TEST_DAEMON_LAUNCH", &daemon_launch_path)
        .env("PLATO_TEST_GATEWAY_ENV", "discord-only");
    let output = fixture_output(command, &plato, fixture_setup);
    if let Some(server) = server {
        server.join().unwrap();
    }

    assert!(
        !output.status.success(),
        "{fixture_name} fixture unexpectedly succeeded at {}",
        plato.display()
    );
    assert!(
        output.stdout.is_empty(),
        "{fixture_name} fixture wrote stdout at {}",
        plato.display()
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("workspace daemon is unavailable or incompatible"),
        "{fixture_name} fixture stderr did not name daemon incompatibility: {stderr}"
    );
    assert!(
        stderr.contains("plato daemon --socket"),
        "{fixture_name} fixture stderr did not contain the socket hint: {stderr}"
    );
    assert!(
        !gateway_launch_path.exists(),
        "{fixture_name} fixture launched gateway marker {}",
        gateway_launch_path.display()
    );
    assert!(
        !daemon_launch_path.exists(),
        "{fixture_name} fixture launched daemon marker {}",
        daemon_launch_path.display()
    );
    stderr
}

fn gateway_command(plato: &Path, workspace: &Path, socket_path: &Path) -> Command {
    let mut command = Command::new(plato);
    command
        .args(["gateway", "discord", "--socket"])
        .arg(socket_path)
        .current_dir(workspace)
        .env_remove("PLATO_CONFIG")
        .env("HOME", workspace);
    command
}

fn install_plato(workspace: &Path, fixture_name: &str) -> (PathBuf, MutexGuard<'static, ()>) {
    // Forked test children inherit writers from every thread. Hold this only
    // through fixture writes and the initial exec, not the child lifetime.
    let fixture_setup = EXECUTABLE_FIXTURE_SETUP
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let bin_dir = tempfile::Builder::new()
        .prefix(&format!("{fixture_name}-"))
        .tempdir_in(workspace)
        .unwrap_or_else(|error| {
            panic!(
                "failed to create {fixture_name} executable fixture under {}: {error}",
                workspace.display()
            )
        })
        .keep();
    let plato = bin_dir.join("plato");
    let source_path = Path::new(env!("CARGO_BIN_EXE_plato"));
    let mut source = File::open(source_path).unwrap_or_else(|error| {
        panic!(
            "failed to open {fixture_name} source executable {}: {error}",
            source_path.display()
        )
    });
    let mut destination = File::create(&plato).unwrap_or_else(|error| {
        panic!(
            "failed to create {fixture_name} executable fixture {}: {error}",
            plato.display()
        )
    });
    io::copy(&mut source, &mut destination).unwrap_or_else(|error| {
        panic!(
            "failed to copy {fixture_name} executable fixture {}: {error}",
            plato.display()
        )
    });
    drop(destination);
    drop(source);
    let mut permissions = fs::metadata(&plato)
        .unwrap_or_else(|error| {
            panic!(
                "failed to read {fixture_name} executable fixture {}: {error}",
                plato.display()
            )
        })
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&plato, permissions).unwrap_or_else(|error| {
        panic!(
            "failed to set {fixture_name} executable permissions on {}: {error}",
            plato.display()
        )
    });
    (plato, fixture_setup)
}

fn fixture_output(
    mut command: Command,
    fixture: &Path,
    fixture_setup: MutexGuard<'static, ()>,
) -> Output {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    spawn_fixture(command, fixture, fixture_setup)
        .wait_with_output()
        .unwrap_or_else(|error| {
            panic!(
                "failed to collect service fixture output from {}: {error}",
                fixture.display()
            )
        })
}

fn spawn_fixture(
    mut command: Command,
    fixture: &Path,
    fixture_setup: MutexGuard<'static, ()>,
) -> Child {
    let child = command.spawn().unwrap_or_else(|error| {
        panic!(
            "failed to execute service fixture {}: {error}",
            fixture.display()
        )
    });
    drop(fixture_setup);
    child
}

fn write_launch_marker(path: &Path, environment_name: &str) {
    write_executable(
        path,
        &format!("#!/bin/sh\nprintf launched > \"${{{environment_name}}}\"\nexit 99\n"),
    );
}

fn write_executable(path: &Path, body: &str) {
    fs::write(path, body).unwrap_or_else(|error| {
        panic!(
            "failed to write service executable fixture {}: {error}",
            path.display()
        )
    });
    let mut permissions = fs::metadata(path)
        .unwrap_or_else(|error| {
            panic!(
                "failed to read service executable fixture {}: {error}",
                path.display()
            )
        })
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).unwrap_or_else(|error| {
        panic!(
            "failed to set service executable permissions on {}: {error}",
            path.display()
        )
    });
}

fn read_lines(path: &Path) -> Vec<String> {
    let contents = fs::read_to_string(path).unwrap_or_else(|error| {
        panic!(
            "failed to read service fixture output {}: {error}",
            path.display()
        )
    });
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
            panic!(
                "service command exited before fixture {} became ready: {status}",
                path.display()
            );
        }
        assert!(
            Instant::now() < deadline,
            "service command fixture {} did not become ready",
            path.display()
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_exit(child: &mut std::process::Child, resource: &Path) -> std::process::ExitStatus {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            panic!(
                "service command fixture {} did not exit",
                resource.display()
            );
        }
        thread::sleep(Duration::from_millis(10));
    }
}
