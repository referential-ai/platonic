#![cfg(unix)]

use platonic_client::paths;
use platonic_client::{
    ClientError,
    client::{DaemonClient, DaemonConnectionConfig},
};
use platonic_protocol::{
    ERROR_LAGGED, ERROR_WORKSPACE_UNREGISTERED, Envelope, EnvelopeKind, PROTOCOL_VERSION,
    RunStateName, ShutdownIfIdleResultName,
};
use pty_process::{
    Size,
    blocking::{Command, Pty, open},
};
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io::{BufRead, BufReader, ErrorKind, Read, Write},
    net::TcpListener,
    os::{
        fd::AsFd,
        unix::net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
    process::{Child, ExitStatus},
    sync::{
        Arc, Mutex,
        mpsc::{self, Receiver, Sender, TryRecvError},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

const INITIAL_ROWS: u16 = 24;
const INITIAL_COLS: u16 = 80;
const RESIZED_ROWS: u16 = 30;
const RESIZED_COLS: u16 = 100;
const PROOF_TIMEOUT: Duration = Duration::from_secs(15);
const MARKER: &str = "__PLATO_TUI_PTY_237__";
const EXPECTED_DRAFT: &str = "ask hello café pasted text";
const PENDING_RUN_ID: &str = "run_pty_pending";
const PENDING_CALL_ID: &str = "call_pty_pending";
const CONVERSATION_RUN_ID: &str = "run_pty_conversation_full_identifier";
const STREAMING_RUN_ID: &str = "run_pty_streaming_384";
const STREAMING_SOURCE: &str = concat!(
    "Burst line one.\n",
    "quiet partial\n",
    "| Name | Value |\n",
    "| --- | --- |\n",
    "| alpha | one |\n",
    "final mid-tok",
);
const SCROLLBACK_SENTINEL: &str = "PLATO_NATIVE_SCROLLBACK_SENTINEL_377";
const CONVERSATION_USER_TEXT: &str = concat!(
    "PLATO_NATIVE_SCROLLBACK_SENTINEL_377\n",
    "history row 01\nhistory row 02\nhistory row 03\nhistory row 04\n",
    "history row 05\nhistory row 06\nhistory row 07\nhistory row 08\n",
    "history row 09\nhistory row 10\nhistory row 11\nhistory row 12\n",
    "history row 13\nhistory row 14\nhistory row 15\nhistory row 16\n",
    "history row 17\nhistory row 18\nhistory row 19\nhistory row 20\n",
    "history row 21\nhistory row 22\nhistory row 23\nhistory row 24\n",
    "history row 25\nhistory row 26\nhistory row 27\nhistory row 28\n",
    "history row 29\nhistory row 30\n",
    "**Conversation-first PTY question**",
);
const ENTER_ALTERNATE_SCREEN: &[u8] = b"\x1b[?1049h";
const LEAVE_ALTERNATE_SCREEN: &[u8] = b"\x1b[?1049l";
const ENABLE_ALTERNATE_SCROLL: &[u8] = b"\x1b[?1007h";
const DISABLE_ALTERNATE_SCROLL: &[u8] = b"\x1b[?1007l";
const CURSOR_POSITION_QUERY: &[u8] = b"\x1b[6n";

#[test]
fn plato_tui_cold_starts_host_thread_and_remote_reuses_it() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let runtime = root.path().join("runtime");
    let state = root.path().join("state");
    let home = root.path().join("home");
    for directory in [&workspace, &runtime, &state, &home] {
        fs::create_dir(directory).unwrap();
    }
    let endpoint = runtime.join("platonic").join("host").join("agent.sock");
    let config = DaemonConnectionConfig::resolve(&workspace, Some(endpoint.clone())).unwrap();
    let _daemon_cleanup = HostDaemonCleanup {
        config: config.clone(),
        endpoint: endpoint.clone(),
    };
    let mut local = PtyShell::spawn(&workspace, &runtime, &state, &home);

    local.write(
        br#""$PLATO_ROOT_BIN" --tui; printf '\n%sLOCAL_STATUS:%s\n' "$PTY_MARK" "$?"
"#,
    );
    local.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "Workspace name [workspace]");
    local.write(b"\r");
    local.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "Approve thread.spawn?");
    local.write(b"y\r");
    local.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "Plato Agent");

    let mut client = connect_pty_daemon(&config);
    client.hello(&workspace).unwrap();
    let threads = client.thread_list().unwrap().threads;
    assert_eq!(threads.len(), 1);
    let thread = &threads[0];
    assert!(thread.live.loaded);
    assert_eq!(thread.authority.spawning_actor, "local_tui");
    assert_eq!(thread.authority.cwd, workspace.to_string_lossy());
    let thread_id = thread.authority.thread_id.clone();
    let authority = client
        .thread_authority(thread_id.clone())
        .unwrap()
        .authority;
    assert_eq!(authority.spawning_actor, "local_tui");
    assert_eq!(authority.granted_paths[0].path, workspace.to_string_lossy());
    assert_eq!(
        client
            .thread_status(thread_id.clone())
            .unwrap()
            .thread
            .authority,
        thread.authority
    );

    let mut remote = PtyShell::spawn(&workspace, &runtime, &state, &home);
    remote.write(
        format!(
            "\"$PLATO_ROOT_BIN\" --remote \"{thread_id}\"; printf '\\n%sREMOTE_STATUS:%s\\n' \"$PTY_MARK\" \"$?\"\n"
        )
        .as_bytes(),
    );
    remote.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "Plato Agent");
    assert_eq!(client.thread_list().unwrap().threads.len(), 1);

    remote.write(b"q");
    assert_eq!(remote.wait_for_marker("REMOTE_STATUS"), "0");
    remote.write(b"exit\r");
    assert!(remote.wait_bounded(PROOF_TIMEOUT).success());
    local.write(b"q");
    assert_eq!(local.wait_for_marker("LOCAL_STATUS"), "0");
    local.write(b"exit\r");
    assert!(local.wait_bounded(PROOF_TIMEOUT).success());

    assert_eq!(
        client.shutdown_if_idle().unwrap().result,
        ShutdownIfIdleResultName::Shutdown
    );
    wait_for_endpoint_removal(&endpoint);
}

#[test]
fn local_interactive_one_shot_asks_once_and_enter_registers_the_directory() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let runtime = root.path().join("runtime");
    let state = root.path().join("state");
    let home = root.path().join("home");
    for directory in [&workspace, &runtime, &state, &home] {
        fs::create_dir(directory).unwrap();
    }
    fs::write(workspace.join("missing-key.toml"), "[provider\n").unwrap();
    let endpoint = runtime.join("platonic").join("host").join("agent.sock");
    let config = DaemonConnectionConfig::resolve(&workspace, Some(endpoint.clone())).unwrap();
    let mut daemon = std::process::Command::new(workspace_binary("platonic"))
        .arg("serve")
        .current_dir(&workspace)
        .env("HOME", &home)
        .env("XDG_RUNTIME_DIR", &runtime)
        .env("XDG_STATE_HOME", &state)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + PROOF_TIMEOUT;
    loop {
        if DaemonClient::connect(&config.socket_path).is_ok() {
            break;
        }
        assert!(daemon.try_wait().unwrap().is_none(), "host daemon exited");
        assert!(Instant::now() < deadline, "host daemon did not bind");
        thread::sleep(Duration::from_millis(10));
    }
    let mut shell = PtyShell::spawn(&workspace, &runtime, &state, &home);

    shell.write(
        br#""$PLATO_ROOT_BIN" --config missing-key.toml hello; printf '\n%sSTATUS:%s\n' "$PTY_MARK" "$?"
"#,
    );
    shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "Workspace name [workspace]");
    shell.write(b"\r");
    let deadline = Instant::now() + PROOF_TIMEOUT;
    let mut client = loop {
        if let Ok(mut client) = DaemonClient::connect(&config.socket_path)
            && client
                .workspace_list()
                .is_ok_and(|listed| !listed.workspaces.is_empty())
        {
            break client;
        }
        assert!(
            Instant::now() < deadline,
            "Enter did not create the workspace"
        );
        thread::sleep(Duration::from_millis(10));
    };
    client.hello(&workspace).unwrap();
    let workspaces = client.workspace_list().unwrap().workspaces;
    assert_eq!(workspaces.len(), 1);
    assert_eq!(workspaces[0].name, "workspace");
    assert_eq!(Path::new(&workspaces[0].root), workspace);
    daemon.kill().unwrap();
    daemon.wait().unwrap();
    assert_ne!(shell.wait_for_marker("STATUS"), "0");
    fs::remove_file(&endpoint).unwrap();

    shell.write(b"exit\r");
    assert!(shell.wait_bounded(PROOF_TIMEOUT).success());
}

#[test]
fn standalone_tui_default_local_endpoint_asks_once_and_registers() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let runtime = root.path().join("runtime");
    let state = root.path().join("state");
    let home = root.path().join("home");
    for directory in [&workspace, &runtime, &state, &home] {
        fs::create_dir(directory).unwrap();
    }
    let workspace_id = paths::workspace_id(&workspace).unwrap();
    let endpoint = runtime
        .join("platonic")
        .join("workspaces")
        .join(&workspace_id)
        .join("agent.sock");
    let config = DaemonConnectionConfig::resolve(&workspace, Some(endpoint.clone())).unwrap();
    let mut daemon = std::process::Command::new(workspace_binary("platonic"))
        .arg("serve")
        .arg("--workspace")
        .arg(&workspace)
        .current_dir(&workspace)
        .env("HOME", &home)
        .env("XDG_RUNTIME_DIR", &runtime)
        .env("XDG_STATE_HOME", &state)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    wait_for_unregistered_daemon(&config, &mut daemon);
    let _daemon_cleanup = HostDaemonCleanup {
        config: config.clone(),
        endpoint: endpoint.clone(),
    };
    let mut shell = PtyShell::spawn(&workspace, &runtime, &state, &home);

    shell.write(
        br#""$PLATO_BIN" --run run_missing; printf '\n%sSTATUS:%s\n' "$PTY_MARK" "$?"
"#,
    );
    shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "Workspace name [workspace]");
    shell.write(b"\r");
    shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "Transcript unavailable");

    let mut client = connect_pty_daemon(&config);
    client.hello(&workspace).unwrap();
    let workspaces = client.workspace_list().unwrap().workspaces;
    assert_eq!(workspaces.len(), 1);
    assert_eq!(workspaces[0].name, "workspace");
    assert_eq!(Path::new(&workspaces[0].root), workspace);

    shell.write(b"q");
    assert_eq!(shell.wait_for_marker("STATUS"), "0");
    shell.write(b"exit\r");
    assert!(shell.wait_bounded(PROOF_TIMEOUT).success());
    assert_eq!(
        client.shutdown_if_idle().unwrap().result,
        ShutdownIfIdleResultName::Shutdown
    );
    drop(client);
    wait_for_endpoint_removal(&endpoint);
    assert!(wait_for_daemon_exit(&mut daemon).success());
}

#[test]
fn standalone_tui_custom_socket_never_prompts_or_registers() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let runtime = root.path().join("runtime");
    let state = root.path().join("state");
    let home = root.path().join("home");
    for directory in [&workspace, &runtime, &state, &home] {
        fs::create_dir(directory).unwrap();
    }
    let endpoint = runtime.join("custom.sock");
    let config = DaemonConnectionConfig::resolve(&workspace, Some(endpoint.clone())).unwrap();
    let mut daemon = std::process::Command::new(workspace_binary("platonic"))
        .arg("serve")
        .arg("--workspace")
        .arg(&workspace)
        .arg("--socket")
        .arg(&endpoint)
        .current_dir(&workspace)
        .env("HOME", &home)
        .env("XDG_RUNTIME_DIR", &runtime)
        .env("XDG_STATE_HOME", &state)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    wait_for_unregistered_daemon(&config, &mut daemon);
    let _daemon_cleanup = HostDaemonCleanup {
        config: config.clone(),
        endpoint: endpoint.clone(),
    };
    let mut shell = PtyShell::spawn(&workspace, &runtime, &state, &home);

    shell.write(
        format!(
            "\"$PLATO_BIN\" --socket \"{}\"; printf '\\n%sSTATUS:%s\\n' \"$PTY_MARK\" \"$?\"\n",
            endpoint.display()
        )
        .as_bytes(),
    );
    assert_ne!(shell.wait_for_marker("STATUS"), "0");
    assert!(
        !String::from_utf8_lossy(&shell.output.lock().unwrap())
            .contains("Workspace name [workspace]")
    );
    assert!(
        String::from_utf8_lossy(&shell.output.lock().unwrap()).contains("workspace_unregistered")
    );

    let mut control = DaemonClient::connect(&endpoint).unwrap();
    assert!(control.workspace_list().unwrap().workspaces.is_empty());
    shell.write(b"exit\r");
    assert!(shell.wait_bounded(PROOF_TIMEOUT).success());
    assert_eq!(
        control.shutdown_if_idle().unwrap().result,
        ShutdownIfIdleResultName::Shutdown
    );
    drop(control);
    wait_for_endpoint_removal(&endpoint);
    assert!(wait_for_daemon_exit(&mut daemon).success());
}

#[test]
fn standalone_tui_snapshot_returns_typed_unregistered_without_prompting() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let runtime = root.path().join("runtime");
    let state = root.path().join("state");
    let home = root.path().join("home");
    for directory in [&workspace, &runtime, &state, &home] {
        fs::create_dir(directory).unwrap();
    }
    let workspace_id = paths::workspace_id(&workspace).unwrap();
    let endpoint = runtime
        .join("platonic")
        .join("workspaces")
        .join(&workspace_id)
        .join("agent.sock");
    let config = DaemonConnectionConfig::resolve(&workspace, Some(endpoint.clone())).unwrap();
    let mut daemon = std::process::Command::new(workspace_binary("platonic"))
        .arg("serve")
        .arg("--workspace")
        .arg(&workspace)
        .current_dir(&workspace)
        .env("HOME", &home)
        .env("XDG_RUNTIME_DIR", &runtime)
        .env("XDG_STATE_HOME", &state)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    wait_for_unregistered_daemon(&config, &mut daemon);
    let _daemon_cleanup = HostDaemonCleanup {
        config: config.clone(),
        endpoint: endpoint.clone(),
    };

    let output = std::process::Command::new(workspace_binary("plato-tui"))
        .arg("--workspace")
        .arg(&workspace)
        .arg("--snapshot")
        .current_dir(&workspace)
        .env("HOME", &home)
        .env("XDG_RUNTIME_DIR", &runtime)
        .env("XDG_STATE_HOME", &state)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("workspace_unregistered"), "{stderr}");
    assert!(!stderr.contains("Workspace name"), "{stderr}");

    let mut control = DaemonClient::connect(&endpoint).unwrap();
    assert!(control.workspace_list().unwrap().workspaces.is_empty());
    assert_eq!(
        control.shutdown_if_idle().unwrap().result,
        ShutdownIfIdleResultName::Shutdown
    );
    drop(control);
    wait_for_endpoint_removal(&endpoint);
    assert!(wait_for_daemon_exit(&mut daemon).success());
}

#[test]
fn standalone_tui_absent_default_endpoint_keeps_the_offline_view_without_prompting() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let runtime = root.path().join("runtime");
    let state = root.path().join("state");
    let home = root.path().join("home");
    for directory in [&workspace, &runtime, &state, &home] {
        fs::create_dir(directory).unwrap();
    }
    let mut shell = PtyShell::spawn(&workspace, &runtime, &state, &home);

    shell.write(
        br#""$PLATO_BIN"; printf '\n%sSTATUS:%s\n' "$PTY_MARK" "$?"
"#,
    );
    shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "daemon unavailable");
    assert!(
        !String::from_utf8_lossy(&shell.output.lock().unwrap())
            .contains("Workspace name [workspace]")
    );
    shell.write(b"q");
    assert_eq!(shell.wait_for_marker("STATUS"), "0");
    shell.write(b"exit\r");
    assert!(shell.wait_bounded(PROOF_TIMEOUT).success());
}

#[test]
fn standalone_tui_surfaces_registration_io_failure_after_prompt() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let runtime = root.path().join("runtime");
    let state = root.path().join("state");
    let home = root.path().join("home");
    for directory in [&workspace, &runtime, &state, &home] {
        fs::create_dir(directory).unwrap();
    }
    let workspace_id = paths::workspace_id(&workspace).unwrap();
    let endpoint = runtime
        .join("platonic")
        .join("workspaces")
        .join(&workspace_id)
        .join("agent.sock");
    let config = DaemonConnectionConfig::resolve(&workspace, Some(endpoint.clone())).unwrap();
    let mut daemon = std::process::Command::new(workspace_binary("platonic"))
        .arg("serve")
        .arg("--workspace")
        .arg(&workspace)
        .current_dir(&workspace)
        .env("HOME", &home)
        .env("XDG_RUNTIME_DIR", &runtime)
        .env("XDG_STATE_HOME", &state)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    wait_for_unregistered_daemon(&config, &mut daemon);
    let mut shell = PtyShell::spawn(&workspace, &runtime, &state, &home);

    shell.write(
        br#""$PLATO_BIN"; printf '\n%sSTATUS:%s\n' "$PTY_MARK" "$?"
"#,
    );
    shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "Workspace name [workspace]");
    daemon.kill().unwrap();
    daemon.wait().unwrap();
    shell.write(b"\r");
    assert_ne!(shell.wait_for_marker("STATUS"), "0");
    let output = shell.output.lock().unwrap().clone();
    let output = String::from_utf8_lossy(&output);
    assert!(
        output.contains("error:"),
        "{}",
        output_tail(output.as_bytes())
    );
    assert!(!output.contains("daemon unavailable"));
    if endpoint.exists() {
        fs::remove_file(&endpoint).unwrap();
    }
    shell.write(b"exit\r");
    assert!(shell.wait_bounded(PROOF_TIMEOUT).success());
}

#[test]
#[cfg_attr(target_os = "macos", ignore = "pty semantics diverge on macOS; #464")]
fn bare_plato_preserves_draft_and_restores_parent_terminal() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let runtime = root.path().join("runtime");
    let state = root.path().join("state");
    let home = root.path().join("home");
    for directory in [&workspace, &runtime, &state, &home] {
        fs::create_dir(directory).unwrap();
    }

    let workspace_id = paths::workspace_id(&workspace).unwrap();
    let endpoint = runtime
        .join("platonic")
        .join("workspaces")
        .join(&workspace_id)
        .join("agent.sock");
    let ledger = state.join("fake-agent.db");
    let fake = FakeDaemon::bind(&endpoint, &workspace, &workspace_id, &ledger);
    let mut shell = PtyShell::spawn(&workspace, &runtime, &state, &home);

    shell.write(
        br#"pre=$(stty -g); printf '\n%sPRE:%s\n' "$PTY_MARK" "$pre"; "$PLATO_BIN"; status=$?; post=$(stty -g); printf '\n%sPOST:%s\n%sSTATUS:%s\n' "$PTY_MARK" "$post" "$PTY_MARK" "$status"
"#,
    );
    let before_termios = shell.wait_for_marker("PRE");
    shell.wait_for_screen_row(
        INITIAL_ROWS,
        INITIAL_COLS,
        None,
        INITIAL_ROWS - 2,
        "Try \"read README.md and summarize it\"",
    );
    let footer = shell.wait_for_screen_row(
        INITIAL_ROWS,
        INITIAL_COLS,
        None,
        INITIAL_ROWS - 1,
        "? shortcuts",
    );
    assert!(footer.contains("Tab queue 0"));
    assert!(!footer.contains("workspace"));

    let idle_output_len = shell.output_len();
    thread::sleep(Duration::from_secs(5));
    assert_eq!(
        shell.output_len(),
        idle_output_len,
        "an unchanged idle TUI must not redraw during the five-second observation"
    );
    let keypress_at = Instant::now();
    shell.write(b"?");
    let shortcuts = shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "Shortcuts");
    assert!(shortcuts.contains("alt + enter"));
    assert!(shortcuts.contains("? shortcuts · Esc close"));
    assert!(
        keypress_at.elapsed() < Duration::from_secs(1),
        "terminal input did not trigger a prompt redraw"
    );
    shell.write(b"\x1b");
    shell.wait_for_screen_without_text(INITIAL_ROWS, INITIAL_COLS, "Shortcuts");

    shell.write(b"ask hllo");
    shell.write(b"\x1b[D\x1b[D\x1b[D");
    shell.write(b"e");
    shell.write(b"\x1b[C\x1b[C\x1b[C");
    shell.write(" café x".as_bytes());
    shell.write(b"\x7f");
    shell.write(b"\x1b[200~pasted text\x1b[201~");
    shell.wait_for_screen_row(
        INITIAL_ROWS,
        INITIAL_COLS,
        None,
        INITIAL_ROWS - 2,
        EXPECTED_DRAFT,
    );

    let resize_at = shell.output_len();
    shell.resize(RESIZED_ROWS, RESIZED_COLS);
    let resized_row = shell.wait_for_screen_row(
        RESIZED_ROWS,
        RESIZED_COLS,
        Some(resize_at),
        RESIZED_ROWS - 2,
        EXPECTED_DRAFT,
    );
    let visible_draft = resized_row
        .strip_prefix("> ")
        .expect("resized composer row should contain the visible draft");
    assert_eq!(visible_draft, EXPECTED_DRAFT);
    assert!(!resized_row.contains('|'));

    let daemon_event_at = Instant::now();
    let daemon_output_at = shell.output_len();
    shell.write(b"\r");
    let run_start = fake.wait_for_request("run.start");
    let question = run_start
        .params
        .as_ref()
        .and_then(|params| params.get("question"))
        .and_then(Value::as_str)
        .expect("run.start.question should be a string");
    assert_eq!(question, visible_draft);
    assert!(!question.contains("\x1b[200~"));
    assert!(!question.contains("\x1b[201~"));

    let working = shell.wait_for_screen_text_after(
        RESIZED_ROWS,
        RESIZED_COLS,
        daemon_output_at,
        "Esc to interrupt",
    );
    assert!(working.contains("Working"));
    assert!(
        daemon_event_at.elapsed() < Duration::from_secs(1),
        "the daemon run-start event did not trigger a prompt redraw"
    );

    let stream_resize_at = shell.output_len();
    shell.resize(28, 90);
    let streamed = shell.wait_for_screen_text_after(28, 90, stream_resize_at, "Working");
    assert!(streamed.contains("Esc to interrupt"));
    assert!(streamed.contains("Esc interrupt"));
    assert_synchronized_frames(&shell.output_since(stream_resize_at));

    shell.write(b"\x03");
    let cancel = fake.wait_for_request("run.cancel");
    assert_eq!(cancel.params.as_ref().unwrap()["run_id"], "run_tui_pty");
    shell.wait_for_screen_text_after(28, 90, stream_resize_at, "press Ctrl+C again to quit");
    shell.write(b"\x03");
    let after_termios = shell.wait_for_marker("POST");
    assert_eq!(after_termios, before_termios);
    assert_eq!(shell.wait_for_marker("STATUS"), "0");

    shell.write(
        br#"printf '%sUSABLE:yes\n' "$PTY_MARK"
"#,
    );
    assert_eq!(shell.wait_for_marker("USABLE"), "yes");
    shell.write(b"exit\r");
    assert!(shell.wait_bounded(PROOF_TIMEOUT).success());

    let requests = fake.finish();
    let run_starts: Vec<_> = requests
        .iter()
        .filter(|request| request.method.as_deref() == Some("run.start"))
        .collect();
    assert_eq!(run_starts.len(), 1);
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method.as_deref() == Some("run.cancel"))
            .count(),
        1
    );
    assert!(
        !requests
            .iter()
            .any(|request| request.method.as_deref() == Some("message.append"))
    );
    assert!(
        !endpoint.with_file_name("agent.lock").exists(),
        "the pre-bound fake must prevent a real daemon from starting"
    );
}

#[test]
#[cfg_attr(target_os = "macos", ignore = "pty semantics diverge on macOS; #464")]
fn composer_cursor_stays_real_at_placeholder_origin_and_narrow_wrap() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let runtime = root.path().join("runtime");
    let state = root.path().join("state");
    let home = root.path().join("home");
    for directory in [&workspace, &runtime, &state, &home] {
        fs::create_dir(directory).unwrap();
    }

    let workspace_id = paths::workspace_id(&workspace).unwrap();
    let endpoint = runtime
        .join("platonic")
        .join("workspaces")
        .join(&workspace_id)
        .join("agent.sock");
    let ledger = state.join("fake-agent.db");
    let fake = FakeDaemon::bind(&endpoint, &workspace, &workspace_id, &ledger);
    let mut shell = PtyShell::spawn(&workspace, &runtime, &state, &home);

    shell.write(
        br#""$PLATO_BIN"; printf '\n%sSTATUS:%s\n' "$PTY_MARK" "$?"
"#,
    );
    shell.wait_for_screen_text(
        INITIAL_ROWS,
        INITIAL_COLS,
        "Try \"read README.md and summarize it\"",
    );

    let resize_at = shell.output_len();
    shell.resize(12, 10);
    shell.wait_for_screen_row(12, 10, Some(resize_at), 10, "Try");
    shell.wait_for_cursor_position(12, 10, Some(resize_at), (10, 2));

    shell.write(b"abcdefgh");
    shell.wait_for_screen_row(12, 10, Some(resize_at), 9, "> abcdefgh");
    shell.wait_for_cursor_position(12, 10, Some(resize_at), (10, 0));

    shell.write(b"\x15");
    shell.wait_for_screen_row(12, 10, Some(resize_at), 10, "Try");
    shell.wait_for_cursor_position(12, 10, Some(resize_at), (10, 2));
    assert!(fake.requests_for("run.start").is_empty());

    shell.write(b"q");
    assert_eq!(shell.wait_for_marker("STATUS"), "0");
    shell.write(b"exit\r");
    assert!(shell.wait_bounded(PROOF_TIMEOUT).success());
    fake.finish();
}

#[test]
#[cfg_attr(target_os = "macos", ignore = "pty semantics diverge on macOS; #464")]
fn composer_textarea_features_preserve_submit_queue_slash_and_history_contracts() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let runtime = root.path().join("runtime");
    let state = root.path().join("state");
    let home = root.path().join("home");
    for directory in [&workspace, &runtime, &state, &home] {
        fs::create_dir(directory).unwrap();
    }

    let workspace_id = paths::workspace_id(&workspace).unwrap();
    let endpoint = runtime
        .join("platonic")
        .join("workspaces")
        .join(&workspace_id)
        .join("agent.sock");
    let ledger = state.join("fake-agent.db");
    let fake = FakeDaemon::bind(&endpoint, &workspace, &workspace_id, &ledger);
    let mut shell = PtyShell::spawn(&workspace, &runtime, &state, &home);

    shell.write(
        br#""$PLATO_BIN"; printf '\n%sSTATUS:%s\n' "$PTY_MARK" "$?"
"#,
    );
    shell.wait_for_screen_text(
        INITIAL_ROWS,
        INITIAL_COLS,
        "Try \"read README.md and summarize it\"",
    );

    shell.write(b"\x1b[200~alpha\r\nbeta\x1b[201~");
    let pasted = shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "beta");
    assert!(pasted.contains("> alpha"));
    assert!(pasted.contains("| beta"));
    assert!(fake.requests_for("run.start").is_empty());

    shell.write(b"\x1a");
    shell.wait_for_screen_text(
        INITIAL_ROWS,
        INITIAL_COLS,
        "Try \"read README.md and summarize it\"",
    );
    shell.write(b"\x12");
    shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "| beta");

    shell.write(b"\x1bbX");
    shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "| Xbeta");
    let selection_at = shell.output_len();
    shell.write(b"\x1b[1;2D");
    shell.wait_for_output_after(selection_at);
    assert!(contains_sgr_parameter(&shell.output_since(selection_at), 7));
    shell.write(b"Y");
    shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "| Ybeta");
    assert!(fake.requests_for("run.start").is_empty());

    shell.write(b"\r");
    let run_start = fake.wait_for_request("run.start");
    assert_eq!(
        run_start.params.as_ref().unwrap()["question"],
        "alpha\nYbeta"
    );
    shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "Working");

    shell.write(b"next by tab\t");
    shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "1 next by tab");
    assert!(fake.requests_for("message.append").is_empty());

    shell.write(b"\x1b[A");
    shell.wait_for_screen_row(
        INITIAL_ROWS,
        INITIAL_COLS,
        None,
        INITIAL_ROWS - 2,
        "next by tab",
    );
    shell.write(b"\x15/");
    let popup = shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "show this help");
    assert!(popup.contains("clear the visible transcript"));
    shell.write(b"\x1b[B\t");
    shell.wait_for_screen_row(
        INITIAL_ROWS,
        INITIAL_COLS,
        None,
        INITIAL_ROWS - 2,
        "/clear ",
    );

    shell.write(b"\x15/c");
    let filtered = shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "clear the visible");
    assert!(!filtered.contains("show this help"));
    shell.write(b"\t\r");
    shell.wait_for_screen_text(
        INITIAL_ROWS,
        INITIAL_COLS,
        "Try \"read README.md and summarize it\"",
    );
    assert_eq!(fake.requests_for("run.start").len(), 1);

    shell.write(b"q");
    assert_eq!(shell.wait_for_marker("STATUS"), "0");
    shell.write(b"exit\r");
    assert!(shell.wait_bounded(PROOF_TIMEOUT).success());

    let requests = fake.finish();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method.as_deref() == Some("run.start"))
            .count(),
        1
    );
    assert!(
        !requests
            .iter()
            .any(|request| request.method.as_deref() == Some("message.append"))
    );
}

#[test]
#[cfg_attr(target_os = "macos", ignore = "pty semantics diverge on macOS; #464")]
fn nonempty_no_color_suppresses_only_color_sgr_in_the_pty() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let runtime = root.path().join("runtime");
    let state = root.path().join("state");
    let home = root.path().join("home");
    for directory in [&workspace, &runtime, &state, &home] {
        fs::create_dir(directory).unwrap();
    }

    let colored = capture_initial_frame(&workspace, &runtime, &state, &home, None);
    let no_color = capture_initial_frame(&workspace, &runtime, &state, &home, Some("1"));

    assert!(contains_color_sgr(&colored.output));
    assert!(!contains_color_sgr(&no_color.output));
    assert!(contains_sgr_parameter(&no_color.output, 1));
    assert!(contains_sgr_parameter(&no_color.output, 2));
    assert_eq!(colored.screen.contents(), no_color.screen.contents());
    assert_eq!(
        colored.screen.cursor_position(),
        no_color.screen.cursor_position()
    );
}

#[test]
#[cfg_attr(target_os = "macos", ignore = "pty semantics diverge on macOS; #464")]
fn bare_plato_status_modal_sends_one_read_only_request_and_escape_closes() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let runtime = root.path().join("runtime");
    let state = root.path().join("state");
    let home = root.path().join("home");
    for directory in [&workspace, &runtime, &state, &home] {
        fs::create_dir(directory).unwrap();
    }

    let workspace_id = paths::workspace_id(&workspace).unwrap();
    let endpoint = runtime
        .join("platonic")
        .join("workspaces")
        .join(&workspace_id)
        .join("agent.sock");
    let ledger = state.join("fake-agent.db");
    let fake = FakeDaemon::bind(&endpoint, &workspace, &workspace_id, &ledger);
    let mut shell = PtyShell::spawn(&workspace, &runtime, &state, &home);

    shell.write(
        br#"pre=$(stty -g); printf '\n%sPRE:%s\n' "$PTY_MARK" "$pre"; "$PLATO_BIN"; status=$?; post=$(stty -g); printf '\n%sPOST:%s\n%sSTATUS:%s\n' "$PTY_MARK" "$post" "$PTY_MARK" "$status"
"#,
    );
    let before_termios = shell.wait_for_marker("PRE");
    shell.wait_for_screen_text(
        INITIAL_ROWS,
        INITIAL_COLS,
        "Try \"read README.md and summarize it\"",
    );

    shell.write(b"/status\r");
    let request = fake.wait_for_request("daemon.status");
    let params = request.params.as_ref().unwrap();
    assert!(params["session_id"].is_null());
    assert!(params["config_path"].is_null());
    let modal = shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "TRUST");
    for heading in ["MODEL", "DAEMON", "SESSION", "USAGE", "TRUST"] {
        assert!(modal.contains(heading), "missing {heading}: {modal}");
    }
    assert!(modal.contains("~openai/gpt-latest"));
    assert!(modal.contains("served model    unknown"));
    assert!(modal.contains("selected        none"));
    assert!(modal.contains("Esc close"));

    shell.write(b"g");
    thread::sleep(Duration::from_millis(50));
    assert!(fake.requests_for("approval.decide").is_empty());
    assert_eq!(fake.requests_for("daemon.status").len(), 1);

    shell.write(b"\x1b");
    let closed = shell.wait_for_screen_without_text(INITIAL_ROWS, INITIAL_COLS, "MODEL");
    assert!(closed.contains("Plato Agent"));
    shell.write(b"q");

    let after_termios = shell.wait_for_marker("POST");
    assert_eq!(after_termios, before_termios);
    assert_eq!(shell.wait_for_marker("STATUS"), "0");
    shell.write(b"exit\r");
    assert!(shell.wait_bounded(PROOF_TIMEOUT).success());

    let requests = fake.finish();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method.as_deref() == Some("daemon.status"))
            .count(),
        1
    );
    assert!(!requests.iter().any(|request| matches!(
        request.method.as_deref(),
        Some("run.start" | "message.append" | "approval.decide" | "run.cancel")
    )));
}

#[test]
#[cfg_attr(target_os = "macos", ignore = "pty semantics diverge on macOS; #464")]
fn bare_plato_restores_pending_approval_after_lag_and_sends_exact_deny() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let runtime = root.path().join("runtime");
    let state = root.path().join("state");
    let home = root.path().join("home");
    for directory in [&workspace, &runtime, &state, &home] {
        fs::create_dir(directory).unwrap();
    }

    let workspace_id = paths::workspace_id(&workspace).unwrap();
    let endpoint = runtime
        .join("platonic")
        .join("workspaces")
        .join(&workspace_id)
        .join("agent.sock");
    let ledger = state.join("fake-agent.db");
    let fake = FakeDaemon::bind_pending_approval(&endpoint, &workspace, &workspace_id, &ledger);
    let mut shell = PtyShell::spawn(&workspace, &runtime, &state, &home);

    shell.write(
        br#"pre=$(stty -g); printf '\n%sPRE:%s\n' "$PTY_MARK" "$pre"; "$PLATO_BIN"; status=$?; post=$(stty -g); printf '\n%sPOST:%s\n%sSTATUS:%s\n' "$PTY_MARK" "$post" "$PTY_MARK" "$status"
"#,
    );
    let before_termios = shell.wait_for_marker("PRE");
    let approval_screen = shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, PENDING_CALL_ID);
    assert!(approval_screen.contains("Approval"));
    assert!(approval_screen.contains("file.edit (workspace_write)"));
    assert!(approval_screen.contains("review the PTY edit"));
    assert!(approval_screen.contains("-old PTY"));
    shell.write(b"\x1b[6~");
    let scrolled_approval = shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "+new PTY");
    assert!(scrolled_approval.contains("+new PTY"));

    let resize_at = shell.output_len();
    shell.resize(18, 70);
    let resized_approval = shell.wait_for_current_screen_text_after(resize_at, PENDING_CALL_ID);
    assert!(resized_approval.contains("+new PTY"));
    let resize_output = shell.output_since(resize_at);
    assert_eq!(
        resize_output
            .windows(CURSOR_POSITION_QUERY.len())
            .filter(|bytes| *bytes == CURSOR_POSITION_QUERY)
            .count(),
        1,
        "overlay resize must refresh inline geometry while the event reader is paused"
    );

    fake.wait_for_request_count("events.stream", 2);
    let stream_requests = fake.requests_for("events.stream");
    assert_eq!(
        stream_requests[0].params.as_ref().unwrap()["from_offset"],
        0
    );
    assert!(
        stream_requests[1]
            .params
            .as_ref()
            .unwrap()
            .get("from_offset")
            .is_none(),
        "lag recovery must request the current tip"
    );

    let deny_at = shell.output_len();
    shell.write(b"d");
    let decision = fake.wait_for_request("approval.decide");
    let params = decision.params.as_ref().unwrap();
    assert_eq!(params["run_id"], PENDING_RUN_ID);
    assert_eq!(params["tool_call_id"], PENDING_CALL_ID);
    assert_eq!(params["decision"], "deny");
    assert_eq!(params["reason"], "denied by plato-tui");

    let restored_output =
        shell.wait_for_ordered_output_after(deny_at, LEAVE_ALTERNATE_SCREEN, b"\x1b[?2026l");
    let decided = shell.wait_for_current_screen_text("Trace  approval | running");
    assert!(
        restored_output
            .windows(b"You".len())
            .any(|bytes| bytes == b"You")
    );
    assert!(
        restored_output
            .windows(LEAVE_ALTERNATE_SCREEN.len())
            .any(|bytes| bytes == LEAVE_ALTERNATE_SCREEN)
    );
    assert!(
        !restored_output
            .windows(CURSOR_POSITION_QUERY.len())
            .any(|bytes| bytes == CURSOR_POSITION_QUERY),
        "daemon-driven overlay closure must not query the cursor after the reader resumes"
    );
    assert!(decided.contains("Trace  approval | running"));
    assert!(!decided.contains("Trace  warning"));
    assert!(!decided.contains(PENDING_RUN_ID));
    assert!(!decided.contains(PENDING_CALL_ID));

    shell.write(b"v");
    let audit = shell.wait_for_current_screen_text("approval denied");
    assert!(audit.contains(PENDING_CALL_ID));
    assert!(audit.contains("? shortcuts"));
    shell.write(b"q");

    let after_termios = shell.wait_for_marker("POST");
    assert_eq!(after_termios, before_termios);
    assert_eq!(shell.wait_for_marker("STATUS"), "0");
    shell.write(b"exit\r");
    assert!(shell.wait_bounded(PROOF_TIMEOUT).success());

    let requests = fake.finish();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method.as_deref() == Some("approval.decide"))
            .count(),
        1
    );
}

#[test]
fn bare_plato_shell_session_grant_flow_is_scoped_and_expires_on_daemon_restart() {
    let root = tempfile::tempdir().unwrap();
    let root_path = root.path().to_path_buf();
    eprintln!("session-grant fixture root={}", root_path.display());
    let workspace = root.path().join("workspace");
    let runtime = root.path().join("runtime");
    let state = root.path().join("state");
    let home = root.path().join("home");
    for directory in [&workspace, &runtime, &state, &home] {
        fs::create_dir(directory).unwrap();
    }
    let provider = spawn_pty_shell_sequence_provider(&[
        ("printf once > pty-session.txt", "done-once"),
        ("printf session >> pty-session.txt", "done-session"),
        ("printf repeat >> pty-session.txt", "done-repeat"),
        ("printf other > pty-other.txt", "done-other"),
        ("printf restart > pty-restart.txt", "done-restart"),
    ]);
    let config_path = root.path().join("test-plato.toml");
    fs::write(
        &config_path,
        format!(
            r#"
[provider]
kind = "open_ai"
model = "test-model"
api_key_env = "PATH"
base_url = "{}"
timeout_ms = 5000

[limits]
token_budget = 100000
max_output_tokens = 32
max_turns = 2

[tools]
enabled = ["shell.exec"]
"#,
            provider.base_url
        ),
    )
    .unwrap();

    let workspace_id = paths::workspace_id(&workspace).unwrap();
    let endpoint = runtime
        .join("platonic")
        .join("workspaces")
        .join(&workspace_id)
        .join("agent.sock");
    let config = DaemonConnectionConfig::resolve(&workspace, Some(endpoint.clone())).unwrap();
    let lock_path = endpoint.with_file_name("agent.lock");
    let mut daemon = SessionGrantWorkspaceDaemon::start(
        &workspace,
        &runtime,
        &state,
        &home,
        &config,
        root.path().join("workspace-daemon-1.stderr"),
    );
    let first_daemon_pid = daemon.pid();
    let mut shell = PtyShell::spawn(&workspace, &runtime, &state, &home);

    shell.write(
        format!(
            "\"$PLATO_BIN\" --config \"{}\"; printf '\\n%sSTATUS1:%s\\n' \"$PTY_MARK\" \"$?\"\n",
            config_path.display()
        )
        .as_bytes(),
    );
    shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "Plato Agent");

    shell.write(b"allow once\r");
    let allow_once = shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "printf once");
    assert!(allow_once.contains("g allow once"));
    assert!(allow_once.contains("s allow shell.exec for session"));
    assert!(allow_once.contains("d deny"));
    shell.write(b"g");
    shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "done-once");

    shell.write(b"grant session\r");
    let session = shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "printf session");
    assert!(session.contains("s allow shell.exec for session"));
    shell.write(b"s");
    shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "done-session");

    shell.write(b"repeat shell\r");
    let repeated = shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "done-repeat");
    assert!(!repeated.contains("Approval"));
    let mut client = connect_pty_daemon(&config);
    let session_id = wait_for_finished_session(&mut client, "allow once");
    let ready = shell.wait_for_screen_without_text(INITIAL_ROWS, INITIAL_COLS, "Esc interrupt");
    assert!(ready.contains("? shortcuts · Tab queue 0"));

    shell.write(b"/new");
    shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "> /new");
    shell.write(b"\r");
    shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "Plato Agent");
    shell.write(b"different session\r");
    let different = shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "printf other");
    assert!(different.contains("s allow shell.exec for session"));
    shell.write(b"d");
    shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "done-other");

    let other_session_id = wait_for_finished_session(&mut client, "different session");
    assert_ne!(session_id, other_session_id);
    assert!(
        client
            .daemon_status(
                Some(session_id.clone()),
                Some(config_path.to_string_lossy().into_owned()),
            )
            .unwrap()
            .trust
            .shell_session_grant
    );
    assert!(
        !client
            .daemon_status(
                Some(other_session_id),
                Some(config_path.to_string_lossy().into_owned()),
            )
            .unwrap()
            .trust
            .shell_session_grant
    );

    shell.write(b"q");
    assert_eq!(shell.wait_for_marker("STATUS1"), "0");
    assert_eq!(
        client.shutdown_if_idle().unwrap().result,
        ShutdownIfIdleResultName::Shutdown
    );
    wait_for_endpoint_removal(&endpoint);
    let first_daemon_exit = daemon.join_after_shutdown();
    assert_eq!(first_daemon_exit.pid, first_daemon_pid);
    if !first_daemon_exit.forced_cleanup {
        assert!(
            first_daemon_exit.status.success(),
            "workspace daemon {} failed ({})\n{}",
            first_daemon_exit.pid,
            first_daemon_exit.status,
            first_daemon_exit.stderr
        );
    }
    remove_session_grant_owned_file(&endpoint).unwrap();
    remove_session_grant_owned_file(&lock_path).unwrap();
    assert_session_grant_lifecycle_absent(first_daemon_pid, &endpoint, &lock_path);
    drop(daemon);

    let mut daemon = SessionGrantWorkspaceDaemon::start(
        &workspace,
        &runtime,
        &state,
        &home,
        &config,
        root.path().join("workspace-daemon-2.stderr"),
    );
    let second_daemon_pid = daemon.pid();

    let restart_at = shell.output_len();
    shell.write(
        format!(
            "\"$PLATO_BIN\" --config \"{}\"; printf '\\n%sSTATUS2:%s\\n' \"$PTY_MARK\" \"$?\"\n",
            config_path.display()
        )
        .as_bytes(),
    );
    shell.wait_for_ordered_output_after(restart_at, b"\x1b[6n", b"\x1b[?2026l");
    shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "different session");
    shell.write(b"/sessions\r");
    let picker = shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "Sessions");
    assert!(picker.contains("allow once"));
    assert!(picker.contains("different session"));
    shell.write(b"\x1b[B\r");
    shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "repeat shell");

    shell.write(b"restart expires grant\r");
    let restarted = shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "printf restart");
    assert!(restarted.contains("s allow shell.exec for session"));
    let mut restarted_client = connect_pty_daemon(&config);
    assert!(
        !restarted_client
            .daemon_status(
                Some(session_id),
                Some(config_path.to_string_lossy().into_owned()),
            )
            .unwrap()
            .trust
            .shell_session_grant
    );
    shell.write(b"d");
    shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "done-restart");

    shell.write(b"q");
    assert_eq!(shell.wait_for_marker("STATUS2"), "0");
    assert_eq!(
        shutdown_when_idle(&mut restarted_client),
        ShutdownIfIdleResultName::Shutdown
    );
    wait_for_endpoint_removal(&endpoint);
    let second_daemon_exit = daemon.join_after_shutdown();
    assert_eq!(second_daemon_exit.pid, second_daemon_pid);
    if !second_daemon_exit.forced_cleanup {
        assert!(
            second_daemon_exit.status.success(),
            "workspace daemon {} failed ({})\n{}",
            second_daemon_exit.pid,
            second_daemon_exit.status,
            second_daemon_exit.stderr
        );
    }
    remove_session_grant_owned_file(&endpoint).unwrap();
    remove_session_grant_owned_file(&lock_path).unwrap();
    assert_session_grant_lifecycle_absent(second_daemon_pid, &endpoint, &lock_path);
    shell.write(b"exit\r");
    assert!(shell.wait_bounded(PROOF_TIMEOUT).success());

    assert_eq!(
        fs::read_to_string(workspace.join("pty-session.txt")).unwrap(),
        "oncesessionrepeat"
    );
    assert!(!workspace.join("pty-other.txt").exists());
    assert!(!workspace.join("pty-restart.txt").exists());
    assert_eq!(provider.handle.join().unwrap(), 10);
    drop(client);
    drop(restarted_client);
    drop(shell);
    drop(daemon);
    root.close().unwrap();
    assert!(!root_path.exists());
}

#[test]
#[cfg_attr(target_os = "macos", ignore = "pty semantics diverge on macOS; #464")]
fn bare_plato_round_trips_conversation_and_audit_without_refetch() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let runtime = root.path().join("runtime");
    let state = root.path().join("state");
    let home = root.path().join("home");
    for directory in [&workspace, &runtime, &state, &home] {
        fs::create_dir(directory).unwrap();
    }

    let workspace_id = paths::workspace_id(&workspace).unwrap();
    let endpoint = runtime
        .join("platonic")
        .join("workspaces")
        .join(&workspace_id)
        .join("agent.sock");
    let ledger = state.join("fake-agent.db");
    let fake = FakeDaemon::bind_conversation_audit(&endpoint, &workspace, &workspace_id, &ledger);
    let mut shell = PtyShell::spawn(&workspace, &runtime, &state, &home);

    shell.write(
        br#"pre=$(stty -g); printf '\n%sPRE:%s\n' "$PTY_MARK" "$pre"; "$PLATO_BIN"; status=$?; post=$(stty -g); printf '\n%sPOST:%s\n%sSTATUS:%s\n' "$PTY_MARK" "$post" "$PTY_MARK" "$status"
"#,
    );
    let before_termios = shell.wait_for_marker("PRE");
    fake.wait_for_request_count("events.stream", 1);
    let default = shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "Trace");
    assert!(default.contains("Plato"));
    assert!(default.contains("**Conversation-first PTY question**"));
    assert!(default.contains("Conversation-first PTY answer"));
    assert!(default.contains("rendered Markdown"));
    assert!(default.contains("fn pty_rendered() {}"));
    assert!(!default.contains("## Conversation-first PTY answer"));
    assert!(!default.contains("**rendered Markdown**"));
    assert!(!default.contains("```rust"));
    assert_eq!(
        default
            .lines()
            .filter(|line| line.trim_end() == "Plato")
            .count(),
        1
    );
    assert!(!default.contains(CONVERSATION_RUN_ID));
    assert!(!default.contains("#7"));
    assert!(!default.contains(SCROLLBACK_SENTINEL));
    let inline_output = shell.output_since(0);
    assert_inline_scrollback_sequence(&inline_output);
    assert!(
        !inline_output
            .windows(ENTER_ALTERNATE_SCREEN.len())
            .any(|bytes| bytes == ENTER_ALTERNATE_SCREEN)
    );
    assert_eq!(fake.requests_for("transcript.read").len(), 1);

    let committed_answer_copies = inline_output
        .windows(b"Conversation-first PTY answer".len())
        .filter(|bytes| *bytes == b"Conversation-first PTY answer")
        .count();
    assert!(committed_answer_copies > 0);
    for width in [40, 80, 120] {
        let resize_at = shell.output_len();
        shell.resize(INITIAL_ROWS, width);
        let resized = shell.wait_for_current_screen_text_after(resize_at, "? shortcuts");
        let rows = resized.lines().collect::<Vec<_>>();
        assert!(rows[usize::from(INITIAL_ROWS - 2)].starts_with("> "));
        assert!(rows[usize::from(INITIAL_ROWS - 1)].contains("? shortcuts"));
        let answer_copies = shell
            .output_since(0)
            .windows(b"Conversation-first PTY answer".len())
            .filter(|bytes| *bytes == b"Conversation-first PTY answer")
            .count();
        assert_eq!(answer_copies, committed_answer_copies, "width {width}");
    }

    let overlay_at = shell.output_len();
    shell.write(b"v");
    let audit = shell.wait_for_current_screen_text(CONVERSATION_RUN_ID);
    let overlay_output = shell.output_since(overlay_at);
    assert!(
        overlay_output
            .windows(ENTER_ALTERNATE_SCREEN.len())
            .any(|bytes| bytes == ENTER_ALTERNATE_SCREEN)
    );
    assert!(
        overlay_output
            .windows(ENABLE_ALTERNATE_SCROLL.len())
            .any(|bytes| bytes == ENABLE_ALTERNATE_SCROLL)
    );
    assert!(audit.contains("#7 model_stage"));
    assert!(audit.contains("? shortcuts · Tab queue 0"));
    assert!(audit.contains("## Conversation-first PTY answer"));
    assert!(audit.contains("**rendered Markdown**"));
    assert!(audit.contains("```rust"));
    assert!(!audit.contains(SCROLLBACK_SENTINEL));

    shell.write(b"\x1b[5~\x1b[5~\x1b[5~\x1b[5~");
    shell.wait_for_current_screen_text(SCROLLBACK_SENTINEL);
    shell.write(b"\x1b[6~\x1b[6~\x1b[6~\x1b[6~");
    shell.wait_for_current_screen_text("## Conversation-first PTY answer");

    let restore_at = shell.output_len();
    shell.write(b"v");
    let conversation = shell.wait_for_current_screen_text("Plato");
    let restore_output = shell.output_since(restore_at);
    assert!(
        restore_output
            .windows(DISABLE_ALTERNATE_SCROLL.len())
            .any(|bytes| bytes == DISABLE_ALTERNATE_SCROLL)
    );
    assert!(
        restore_output
            .windows(LEAVE_ALTERNATE_SCREEN.len())
            .any(|bytes| bytes == LEAVE_ALTERNATE_SCREEN)
    );
    assert!(!conversation.contains(CONVERSATION_RUN_ID));
    assert!(!conversation.contains("#7"));
    assert!(conversation.contains("? shortcuts · Tab queue 0"));
    assert_inline_scrollback_sequence(&shell.output_since(0));

    let exit_overlay_at = shell.output_len();
    shell.write(b"v");
    shell.wait_for_current_screen_text(CONVERSATION_RUN_ID);
    shell.write(b"q");
    let after_termios = shell.wait_for_marker("POST");
    let exit_output = shell.output_since(exit_overlay_at);
    assert!(
        exit_output
            .windows(DISABLE_ALTERNATE_SCROLL.len())
            .any(|bytes| bytes == DISABLE_ALTERNATE_SCROLL)
    );
    assert!(
        exit_output
            .windows(LEAVE_ALTERNATE_SCREEN.len())
            .any(|bytes| bytes == LEAVE_ALTERNATE_SCREEN)
    );
    assert_eq!(after_termios, before_termios);
    assert_eq!(shell.wait_for_marker("STATUS"), "0");
    let retained_output = shell.output_since(0);
    assert_inline_scrollback_sequence(&retained_output);
    assert!(
        retained_output
            .windows(b"Conversation-first PTY answer".len())
            .any(|bytes| bytes == b"Conversation-first PTY answer")
    );
    assert_eq!(fake.requests_for("transcript.read").len(), 1);

    shell.write(b"exit\r");
    assert!(shell.wait_bounded(PROOF_TIMEOUT).success());

    let requests = fake.finish();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method.as_deref() == Some("transcript.read"))
            .count(),
        1
    );
}

#[test]
#[cfg_attr(target_os = "macos", ignore = "pty semantics diverge on macOS; #464")]
fn streamed_markdown_smooths_holds_tables_and_survives_reload_and_resize() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let runtime = root.path().join("runtime");
    let state = root.path().join("state");
    let home = root.path().join("home");
    for directory in [&workspace, &runtime, &state, &home] {
        fs::create_dir(directory).unwrap();
    }

    let workspace_id = paths::workspace_id(&workspace).unwrap();
    let endpoint = runtime
        .join("platonic")
        .join("workspaces")
        .join(&workspace_id)
        .join("agent.sock");
    let ledger = state.join("fake-agent.db");
    let fake = FakeDaemon::bind_streaming(&endpoint, &workspace, &workspace_id, &ledger);
    let mut shell = PtyShell::spawn(&workspace, &runtime, &state, &home);

    shell.write(
        br#""$PLATO_BIN"; printf '\n%sSTATUS:%s\n' "$PTY_MARK" "$?"
"#,
    );
    shell.wait_for_screen_text(
        INITIAL_ROWS,
        INITIAL_COLS,
        "Try \"read README.md and summarize it\"",
    );
    let stream_at = shell.output_len();
    shell.write(b"stream the answer\r");
    fake.wait_for_request("run.start");

    shell.wait_for_current_screen_text_after(stream_at, "Burst line one.");
    shell.wait_for_current_screen_text_after(stream_at, "quiet partial");
    fake.wait_for_request_count("events.stream", 2);
    thread::sleep(Duration::from_millis(50));
    let output = shell.output.lock().unwrap().clone();
    let resizes = shell.resizes.lock().unwrap().clone();
    let during_table = parsed_terminal(&output, &resizes, 0).screen().contents();
    assert!(!during_table.contains("Name"), "{during_table}");
    assert!(!during_table.contains("alpha"), "{during_table}");

    let finalized = shell.wait_for_current_screen_text_after(stream_at, "final mid-tok");
    assert!(finalized.contains("Name"));
    assert!(finalized.contains("alpha"));
    fake.wait_for_request_count("transcript.read", 1);
    assert_eq!(fake.requests_for("transcript.read").len(), 1);

    shell.write(b"v");
    let loaded_run = format!("run {STREAMING_RUN_ID}");
    let audit = shell.wait_for_current_screen_text(&loaded_run);
    assert!(audit.contains("| Name | Value |"));
    assert!(audit.contains("| alpha | one |"));
    assert!(audit.contains("final mid-tok"));
    shell.write(b"v");
    shell.wait_for_current_screen_text("final mid-tok");

    shell.write(b"/sessions\r");
    shell.wait_for_current_screen_text("Sessions");
    shell.write(b"\r");
    fake.wait_for_request_count("transcript.read", 2);
    let reloaded = shell.wait_for_current_screen_text("final mid-tok");
    assert!(reloaded.contains("Name"));
    assert!(reloaded.contains("alpha"));
    assert_eq!(reloaded.matches("final mid-tok").count(), 1);

    let commits_before_resize = inline_scrollback_count(&shell.output_since(0));
    assert!(commits_before_resize > 0);
    for width in [120, 40, 120] {
        let resize_at = shell.output_len();
        shell.resize(INITIAL_ROWS, width);
        let resized = shell.wait_for_current_screen_text_after(resize_at, "? shortcuts");
        assert_eq!(resized.matches("final mid-tok").count(), 1, "width {width}");
        assert_eq!(
            inline_scrollback_count(&shell.output_since(0)),
            commits_before_resize,
            "width {width} recommitted the durable transcript"
        );
    }

    shell.write(b"q");
    assert_eq!(shell.wait_for_marker("STATUS"), "0");
    shell.write(b"exit\r");
    assert!(shell.wait_bounded(PROOF_TIMEOUT).success());

    let requests = fake.finish();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method.as_deref() == Some("transcript.read"))
            .count(),
        2
    );
}

#[test]
#[cfg_attr(target_os = "macos", ignore = "pty semantics diverge on macOS; #464")]
fn bare_plato_session_picker_resumes_exact_hidden_session_id() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let runtime = root.path().join("runtime");
    let state = root.path().join("state");
    let home = root.path().join("home");
    for directory in [&workspace, &runtime, &state, &home] {
        fs::create_dir(directory).unwrap();
    }

    let workspace_id = paths::workspace_id(&workspace).unwrap();
    let endpoint = runtime
        .join("platonic")
        .join("workspaces")
        .join(&workspace_id)
        .join("agent.sock");
    let ledger = state.join("fake-agent.db");
    let fake = FakeDaemon::bind_conversation_audit(&endpoint, &workspace, &workspace_id, &ledger);
    let mut shell = PtyShell::spawn(&workspace, &runtime, &state, &home);

    shell.write(
        br#""$PLATO_BIN"; printf '\n%sSTATUS:%s\n' "$PTY_MARK" "$?"
"#,
    );
    shell.wait_for_screen_text(
        INITIAL_ROWS,
        INITIAL_COLS,
        "Conversation-first PTY question",
    );

    shell.write(b"/sessions\r");
    let picker = shell.wait_for_screen_text(INITIAL_ROWS, INITIAL_COLS, "Sessions");
    assert!(picker.contains("running"));
    assert!(picker.contains("Conversation-first PTY question"));
    assert!(!picker.contains("approved, go ahead"));
    assert!(!picker.contains("session_pty_conversation"));

    shell.write(b"\r");
    fake.wait_for_request_count("transcript.read", 2);
    let transcript_requests = fake.requests_for("transcript.read");
    assert_eq!(
        transcript_requests[1].params.as_ref().unwrap()["session_id"],
        "session_pty_conversation"
    );

    shell.write(b"q");
    assert_eq!(shell.wait_for_marker("STATUS"), "0");
    shell.write(b"exit\r");
    assert!(shell.wait_bounded(PROOF_TIMEOUT).success());

    let requests = fake.finish();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method.as_deref() == Some("transcript.read"))
            .count(),
        2
    );
}

struct CapturedFrame {
    output: Vec<u8>,
    screen: vt100::Screen,
}

fn capture_initial_frame(
    workspace: &Path,
    runtime: &Path,
    state: &Path,
    home: &Path,
    no_color: Option<&str>,
) -> CapturedFrame {
    let workspace_id = paths::workspace_id(workspace).unwrap();
    let endpoint = runtime
        .join("platonic")
        .join("workspaces")
        .join(&workspace_id)
        .join("agent.sock");
    let ledger = state.join("fake-agent.db");
    let fake = FakeDaemon::bind(&endpoint, workspace, &workspace_id, &ledger);
    let mut shell = PtyShell::spawn_with_no_color(workspace, runtime, state, home, no_color);

    shell.write(
        br#""$PLATO_BIN"; printf '\n%sSTATUS:%s\n' "$PTY_MARK" "$?"
"#,
    );
    shell.wait_for_screen_text(
        INITIAL_ROWS,
        INITIAL_COLS,
        "Try \"read README.md and summarize it\"",
    );
    let output = shell.output.lock().unwrap().clone();
    let screen = parsed_screen(&output, INITIAL_ROWS, INITIAL_COLS, None);

    shell.write(b"q");
    assert_eq!(shell.wait_for_marker("STATUS"), "0");
    shell.write(b"exit\r");
    assert!(shell.wait_bounded(PROOF_TIMEOUT).success());
    fake.finish();
    fs::remove_file(endpoint).unwrap();

    CapturedFrame { output, screen }
}

fn contains_color_sgr(output: &[u8]) -> bool {
    sgr_parameters(output).any(|parameter| matches!(parameter, 30..=49 | 58 | 59 | 90..=107))
}

fn contains_sgr_parameter(output: &[u8], expected: u16) -> bool {
    sgr_parameters(output).any(|parameter| parameter == expected)
}

fn sgr_parameters(output: &[u8]) -> impl Iterator<Item = u16> + '_ {
    output
        .split(|byte| *byte == b'\x1b')
        .filter_map(|sequence| sequence.strip_prefix(b"["))
        .filter_map(|sequence| {
            let end = sequence.iter().position(|byte| *byte == b'm')?;
            std::str::from_utf8(&sequence[..end]).ok()
        })
        .flat_map(|parameters| parameters.split([';', ':']))
        .filter_map(|parameter| parameter.parse().ok())
}

struct PtyShell {
    pty: Pty,
    child: Child,
    output: Arc<Mutex<Vec<u8>>>,
    size: Arc<Mutex<(u16, u16)>>,
    resizes: Mutex<Vec<(usize, u16, u16)>>,
    reader: Option<JoinHandle<()>>,
}

struct HostDaemonCleanup {
    config: DaemonConnectionConfig,
    endpoint: PathBuf,
}

struct SessionGrantWorkspaceDaemon {
    child: Option<Child>,
    pid: u32,
    stderr_path: PathBuf,
    endpoint: PathBuf,
    lock_path: PathBuf,
}

struct SessionGrantDaemonExit {
    pid: u32,
    status: ExitStatus,
    stderr: String,
    forced_cleanup: bool,
}

impl SessionGrantWorkspaceDaemon {
    fn start(
        workspace: &Path,
        runtime: &Path,
        state: &Path,
        home: &Path,
        config: &DaemonConnectionConfig,
        stderr_path: PathBuf,
    ) -> Self {
        let stderr = File::create(&stderr_path).unwrap();
        let child = std::process::Command::new(workspace_binary("platonic"))
            .arg("serve")
            .arg("--workspace")
            .arg(workspace)
            .current_dir(workspace)
            .env("HOME", home)
            .env("XDG_RUNTIME_DIR", runtime)
            .env("XDG_STATE_HOME", state)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(stderr)
            .spawn()
            .unwrap();
        let pid = child.id();
        let endpoint = config.socket_path.clone();
        let lock_path = endpoint.with_file_name("agent.lock");
        eprintln!(
            "session-grant fixture daemon pid={pid} endpoint={} lock={}",
            endpoint.display(),
            lock_path.display()
        );
        let mut daemon = Self {
            child: Some(child),
            pid,
            stderr_path,
            endpoint,
            lock_path,
        };
        daemon.wait_until_ready(config);
        daemon
    }

    fn pid(&self) -> u32 {
        self.pid
    }

    fn wait_until_ready(&mut self, config: &DaemonConnectionConfig) {
        let deadline = Instant::now() + PROOF_TIMEOUT;
        loop {
            if let Ok(mut client) = DaemonClient::connect(&config.socket_path) {
                match client.hello(&config.workspace_root) {
                    Ok(_) => return,
                    Err(ClientError::DaemonResponse(error))
                        if error.code == ERROR_WORKSPACE_UNREGISTERED =>
                    {
                        drop(client);
                        let mut control = DaemonClient::connect(&config.socket_path).unwrap();
                        control
                            .workspace_create(
                                paths::workspace_id(&config.workspace_root).unwrap(),
                                config.workspace_root.clone(),
                            )
                            .unwrap();
                    }
                    Err(error) => panic!("workspace daemon hello failed: {error}"),
                }
            }
            if let Some(status) = self.child.as_mut().unwrap().try_wait().unwrap() {
                let stderr = fs::read_to_string(&self.stderr_path).unwrap_or_default();
                panic!(
                    "workspace daemon {} exited before readiness ({status})\n{stderr}",
                    self.pid
                );
            }
            assert!(
                Instant::now() < deadline,
                "workspace daemon {} did not create ready endpoint {}",
                self.pid,
                self.endpoint.display()
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn join_after_shutdown(&mut self) -> SessionGrantDaemonExit {
        let deadline = Instant::now() + PROOF_TIMEOUT;
        let (status, forced_cleanup) = loop {
            if let Some(status) = self.child.as_mut().unwrap().try_wait().unwrap() {
                break (status, false);
            }
            if Instant::now() >= deadline {
                break self
                    .terminate_and_join()
                    .expect("workspace daemon could not be joined after shutdown");
            }
            thread::sleep(Duration::from_millis(10));
        };
        drop(self.child.take());
        SessionGrantDaemonExit {
            pid: self.pid,
            status,
            stderr: fs::read_to_string(&self.stderr_path).unwrap(),
            forced_cleanup,
        }
    }

    fn terminate_and_join(&mut self) -> Option<(ExitStatus, bool)> {
        let child = self.child.as_mut()?;
        if let Ok(Some(status)) = child.try_wait() {
            return Some((status, false));
        }
        let mut forced_cleanup = false;
        if let Some(pid) = rustix::process::Pid::from_raw(self.pid as i32) {
            forced_cleanup = true;
            let _ = rustix::process::kill_process(pid, rustix::process::Signal::TERM);
        }
        let deadline = Instant::now() + PROOF_TIMEOUT;
        loop {
            match child.try_wait() {
                Ok(Some(status)) => return Some((status, forced_cleanup)),
                Ok(None) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(10));
                }
                _ => break,
            }
        }
        forced_cleanup = true;
        let _ = child.kill();
        child.wait().ok().map(|status| (status, forced_cleanup))
    }
}

fn workspace_binary(name: &str) -> PathBuf {
    std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join(format!("{name}{}", std::env::consts::EXE_SUFFIX))
}

fn wait_for_unregistered_daemon(config: &DaemonConnectionConfig, child: &mut Child) {
    let deadline = Instant::now() + PROOF_TIMEOUT;
    loop {
        if let Ok(mut client) = DaemonClient::connect(&config.socket_path) {
            let error = client.hello(&config.workspace_root).unwrap_err();
            assert!(matches!(
                error,
                ClientError::DaemonResponse(ref error)
                    if error.code == ERROR_WORKSPACE_UNREGISTERED
            ));
            return;
        }
        assert!(child.try_wait().unwrap().is_none(), "daemon exited");
        assert!(Instant::now() < deadline, "daemon did not bind");
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_daemon_exit(child: &mut Child) -> ExitStatus {
    let deadline = Instant::now() + PROOF_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            let status = child.wait().unwrap();
            panic!("daemon did not exit after shutdown ({status})");
        }
        thread::sleep(Duration::from_millis(10));
    }
}

impl Drop for SessionGrantWorkspaceDaemon {
    fn drop(&mut self) {
        let _ = self.terminate_and_join();
        drop(self.child.take());
        let _ = remove_session_grant_owned_file(&self.endpoint);
        let _ = remove_session_grant_owned_file(&self.lock_path);
    }
}

fn remove_session_grant_owned_file(path: &Path) -> std::io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg_attr(not(target_os = "linux"), allow(unused_variables))]
fn assert_session_grant_lifecycle_absent(pid: u32, endpoint: &Path, lock_path: &Path) {
    #[cfg(target_os = "linux")]
    assert!(
        !Path::new(&format!("/proc/{pid}")).exists(),
        "workspace daemon {pid} remained after bounded join"
    );
    assert!(
        !endpoint.exists(),
        "workspace daemon socket remained: {}",
        endpoint.display()
    );
    assert!(
        !lock_path.exists(),
        "workspace daemon lock remained: {}",
        lock_path.display()
    );
}

impl Drop for HostDaemonCleanup {
    fn drop(&mut self) {
        if let Ok(mut client) = DaemonClient::connect(&self.config.socket_path) {
            let _ = client.hello(&self.config.workspace_root);
            let _ = client.shutdown_if_idle();
        }
        let deadline = Instant::now() + PROOF_TIMEOUT;
        while self.endpoint.exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
    }
}

/// Polls `shutdown_if_idle` until the daemon is idle enough to accept it.
///
/// A run outlives the client that started it: clients attach, detach, and
/// reattach, and detaching must not stop an in-flight turn. So a daemon can
/// legitimately refuse shutdown for a moment after a client process exits,
/// while the run it left behind reaches a terminal state. Asserting on a
/// single call races that window.
fn shutdown_when_idle(client: &mut DaemonClient) -> ShutdownIfIdleResultName {
    let deadline = Instant::now() + PROOF_TIMEOUT;
    loop {
        let result = client.shutdown_if_idle().unwrap().result;
        if result != ShutdownIfIdleResultName::RefusedActive || Instant::now() >= deadline {
            return result;
        }
        thread::sleep(Duration::from_millis(25));
    }
}

impl PtyShell {
    fn spawn(workspace: &Path, runtime: &Path, state: &Path, home: &Path) -> Self {
        Self::spawn_with_no_color(workspace, runtime, state, home, None)
    }

    fn spawn_with_no_color(
        workspace: &Path,
        runtime: &Path,
        state: &Path,
        home: &Path,
        no_color: Option<&str>,
    ) -> Self {
        let (pty, pts) = open().unwrap();
        pty.resize(Size::new(INITIAL_ROWS, INITIAL_COLS)).unwrap();
        let reader_file = File::from(pty.as_fd().try_clone_to_owned().unwrap());
        let responder_file = File::from(pty.as_fd().try_clone_to_owned().unwrap());
        let output = Arc::new(Mutex::new(Vec::new()));
        let reader_output = Arc::clone(&output);
        let size = Arc::new(Mutex::new((INITIAL_ROWS, INITIAL_COLS)));
        let reader_size = Arc::clone(&size);
        let reader = thread::spawn(move || {
            read_pty(reader_file, responder_file, reader_output, reader_size)
        });
        let command = Command::new("/bin/sh")
            .arg("-i")
            .current_dir(workspace)
            .env("TERM", "xterm-256color")
            .env("LANG", "C.UTF-8")
            .env("HOME", home)
            .env("XDG_RUNTIME_DIR", runtime)
            .env("XDG_STATE_HOME", state)
            .env("PLATO_BIN", env!("CARGO_BIN_EXE_plato-tui"))
            .env("PLATO_ROOT_BIN", env!("CARGO_BIN_EXE_plato"))
            .env("PTY_MARK", MARKER)
            .env("PS1", "")
            .env("PS2", "")
            .env_remove("ENV")
            .env_remove("PLATO_CONFIG")
            .env_remove("NO_COLOR");
        let command = match no_color {
            Some(value) => command.env("NO_COLOR", value),
            None => command,
        };
        let child = command.spawn(pts).unwrap();
        Self {
            pty,
            child,
            output,
            size,
            resizes: Mutex::new(Vec::new()),
            reader: Some(reader),
        }
    }

    fn write(&mut self, bytes: &[u8]) {
        self.pty.write_all(bytes).unwrap();
        self.pty.flush().unwrap();
    }

    fn resize(&self, rows: u16, cols: u16) {
        self.resizes
            .lock()
            .unwrap()
            .push((self.output_len(), rows, cols));
        *self.size.lock().unwrap() = (rows, cols);
        self.pty.resize(Size::new(rows, cols)).unwrap();
    }

    fn output_len(&self) -> usize {
        self.output.lock().unwrap().len()
    }

    fn output_since(&self, offset: usize) -> Vec<u8> {
        let output = self.output.lock().unwrap();
        output[offset.min(output.len())..].to_vec()
    }

    fn wait_for_current_screen_text(&mut self, expected: &str) -> String {
        self.wait_for_current_screen_text_after(0, expected)
    }

    fn wait_for_current_screen_text_after(&mut self, offset: usize, expected: &str) -> String {
        let deadline = Instant::now() + PROOF_TIMEOUT;
        loop {
            let output = self.output.lock().unwrap().clone();
            let resizes = self.resizes.lock().unwrap().clone();
            let parser = parsed_terminal(&output, &resizes, 0);
            let screen = parser.screen();
            let contents = screen.contents();
            if output.len() > offset && contents.contains(expected) {
                assert_eq!(screen.size(), *self.size.lock().unwrap());
                return contents;
            }
            self.assert_running(expected);
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {expected:?} on the current rendered screen\nrendered:\n{}\nraw:\n{}",
                contents,
                output_tail(&output)
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn wait_for_marker(&mut self, name: &str) -> String {
        let deadline = Instant::now() + PROOF_TIMEOUT;
        loop {
            let output = self.output.lock().unwrap().clone();
            if let Some(value) = marker_value(&output, name) {
                return value;
            }
            self.assert_running(name);
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {name} marker\n{}",
                output_tail(&output)
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn wait_for_screen_row(
        &mut self,
        rows: u16,
        cols: u16,
        resize_at: Option<usize>,
        row: u16,
        expected: &str,
    ) -> String {
        let deadline = Instant::now() + PROOF_TIMEOUT;
        loop {
            let output = self.output.lock().unwrap().clone();
            let screen = parsed_screen(&output, rows, cols, resize_at);
            let rendered_row = screen.rows(0, cols).nth(usize::from(row)).unwrap();
            let has_post_resize_output = resize_at.is_none_or(|offset| output.len() > offset);
            if has_post_resize_output && rendered_row.contains(expected) {
                assert_eq!(screen.size(), (rows, cols));
                return rendered_row;
            }
            self.assert_running(expected);
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {expected:?} on rendered row {row}\nrendered:\n{}\nraw:\n{}",
                screen.contents(),
                output_tail(&output)
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn wait_for_screen_text(&mut self, rows: u16, cols: u16, expected: &str) -> String {
        let deadline = Instant::now() + PROOF_TIMEOUT;
        loop {
            let output = self.output.lock().unwrap().clone();
            let screen = parsed_screen(&output, rows, cols, None);
            let contents = screen.contents();
            if contents.contains(expected) {
                assert_eq!(screen.size(), (rows, cols));
                return contents;
            }
            self.assert_running(expected);
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {expected:?} on rendered screen\nrendered:\n{}\nraw:\n{}",
                contents,
                output_tail(&output)
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn wait_for_screen_text_after(
        &mut self,
        rows: u16,
        cols: u16,
        offset: usize,
        expected: &str,
    ) -> String {
        let deadline = Instant::now() + PROOF_TIMEOUT;
        loop {
            let output = self.output_since(offset);
            let mut parser = vt100::Parser::new(rows, cols, 0);
            parser.process(&output);
            let screen = parser.screen();
            let contents = screen.contents();
            if contents.contains(expected) {
                assert_eq!(screen.size(), (rows, cols));
                return contents;
            }
            self.assert_running(expected);
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {expected:?} after output offset {offset}\nrendered:\n{}\nraw:\n{}",
                contents,
                output_tail(&output)
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn wait_for_screen_without_text(&mut self, rows: u16, cols: u16, unexpected: &str) -> String {
        let deadline = Instant::now() + PROOF_TIMEOUT;
        loop {
            let output = self.output.lock().unwrap().clone();
            let screen = parsed_screen(&output, rows, cols, None);
            let contents = screen.contents();
            if !contents.contains(unexpected) {
                assert_eq!(screen.size(), (rows, cols));
                return contents;
            }
            self.assert_running(unexpected);
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {unexpected:?} to leave rendered screen\nrendered:\n{}\nraw:\n{}",
                contents,
                output_tail(&output)
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn wait_for_cursor_position(
        &mut self,
        rows: u16,
        cols: u16,
        resize_at: Option<usize>,
        expected: (u16, u16),
    ) {
        let deadline = Instant::now() + PROOF_TIMEOUT;
        loop {
            let output = self.output.lock().unwrap().clone();
            let screen = parsed_screen(&output, rows, cols, resize_at);
            if screen.cursor_position() == expected {
                return;
            }
            self.assert_running("cursor position");
            assert!(
                Instant::now() < deadline,
                "timed out waiting for cursor {expected:?}; got {:?}\nrendered:\n{}\nraw:\n{}",
                screen.cursor_position(),
                screen.contents(),
                output_tail(&output)
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn wait_for_output_after(&mut self, offset: usize) {
        let deadline = Instant::now() + PROOF_TIMEOUT;
        loop {
            if self.output_len() > offset {
                return;
            }
            self.assert_running("redraw after input");
            assert!(
                Instant::now() < deadline,
                "timed out waiting for output after byte {offset}"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn wait_for_ordered_output_after(
        &mut self,
        offset: usize,
        first: &[u8],
        second: &[u8],
    ) -> Vec<u8> {
        let deadline = Instant::now() + PROOF_TIMEOUT;
        loop {
            let output = self.output_since(offset);
            if let Some(first_at) = output.windows(first.len()).position(|bytes| bytes == first)
                && output[first_at + first.len()..]
                    .windows(second.len())
                    .any(|bytes| bytes == second)
            {
                return output;
            }
            self.assert_running("ordered terminal output");
            assert!(
                Instant::now() < deadline,
                "timed out waiting for ordered terminal output after byte {offset}\n{}",
                output_tail(&output)
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn assert_running(&mut self, context: &str) {
        if let Some(status) = self.child.try_wait().unwrap() {
            let output = self.output.lock().unwrap();
            panic!(
                "PTY shell exited while waiting for {context:?} ({status})\n{}",
                output_tail(&output)
            );
        }
    }

    fn wait_bounded(&mut self, timeout: Duration) -> ExitStatus {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            if Instant::now() >= deadline {
                self.child.kill().unwrap();
                let status = self.child.wait().unwrap();
                panic!(
                    "PTY shell did not exit within {timeout:?} ({status})\n{}",
                    output_tail(&self.output.lock().unwrap())
                );
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for PtyShell {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

fn read_pty(
    mut reader: File,
    mut responder: File,
    output: Arc<Mutex<Vec<u8>>>,
    size: Arc<Mutex<(u16, u16)>>,
) {
    let mut buffer = [0; 4096];
    let mut parser = vt100::Parser::new(INITIAL_ROWS, INITIAL_COLS, 0);
    let mut query_tail = Vec::new();
    loop {
        match reader.read(&mut buffer) {
            Ok(0) | Err(_) => break,
            Ok(read) => {
                let (rows, cols) = *size.lock().unwrap();
                if parser.screen().size() != (rows, cols) {
                    parser.set_size(rows, cols);
                }
                let bytes = &buffer[..read];
                parser.process(bytes);
                output.lock().unwrap().extend_from_slice(bytes);
                query_tail.extend_from_slice(bytes);
                let cursor_queries = query_tail
                    .windows(4)
                    .filter(|window| *window == b"\x1b[6n")
                    .count();
                if cursor_queries > 0 {
                    let (row, col) = parser.screen().cursor_position();
                    for _ in 0..cursor_queries {
                        write!(responder, "\x1b[{};{}R", row + 1, col + 1).unwrap();
                    }
                    responder.flush().unwrap();
                }
                let keep_from = query_tail.len().saturating_sub(3);
                query_tail.drain(..keep_from);
            }
        }
    }
}

fn parsed_screen(output: &[u8], rows: u16, cols: u16, resize_at: Option<usize>) -> vt100::Screen {
    let mut parser = vt100::Parser::new(INITIAL_ROWS, INITIAL_COLS, 0);
    if let Some(offset) = resize_at {
        let offset = offset.min(output.len());
        parser.process(&output[..offset]);
        parser.set_size(rows, cols);
        parser.process(&output[offset..]);
    } else {
        parser.process(output);
    }
    parser.screen().clone()
}

fn parsed_terminal(
    output: &[u8],
    resizes: &[(usize, u16, u16)],
    scrollback_len: usize,
) -> vt100::Parser {
    let mut parser = vt100::Parser::new(INITIAL_ROWS, INITIAL_COLS, scrollback_len);
    let mut processed = 0;
    for &(offset, rows, cols) in resizes {
        let offset = offset.min(output.len()).max(processed);
        parser.process(&output[processed..offset]);
        parser.set_size(rows, cols);
        processed = offset;
    }
    parser.process(&output[processed..]);
    parser
}

fn marker_value(output: &[u8], name: &str) -> Option<String> {
    let output = String::from_utf8_lossy(output);
    let prefix = format!("{MARKER}{name}:");
    let value = output.split(&prefix).nth(1)?;
    Some(value.trim_start().split(['\r', '\n']).next()?.to_owned())
}

fn output_tail(output: &[u8]) -> String {
    let start = output.len().saturating_sub(8_000);
    String::from_utf8_lossy(&output[start..]).into_owned()
}

fn assert_synchronized_frames(output: &[u8]) {
    const BEGIN: &[u8] = b"\x1b[?2026h";
    const END: &[u8] = b"\x1b[?2026l";
    let begins = output
        .windows(BEGIN.len())
        .filter(|bytes| *bytes == BEGIN)
        .count();
    let ends = output
        .windows(END.len())
        .filter(|bytes| *bytes == END)
        .count();
    assert!(
        begins > 0,
        "resize plus stream emitted no synchronized frame"
    );
    assert_eq!(
        begins, ends,
        "resize plus stream left a synchronized frame open"
    );

    let mut depth = 0_u8;
    let mut index = 0;
    while index < output.len() {
        if output[index..].starts_with(BEGIN) {
            assert_eq!(depth, 0, "synchronized frames must not nest");
            depth = 1;
            index += BEGIN.len();
        } else if output[index..].starts_with(END) {
            assert_eq!(depth, 1, "synchronized frame ended without a begin");
            depth = 0;
            index += END.len();
        } else {
            index += 1;
        }
    }
    assert_eq!(depth, 0, "synchronized frame was not closed");
}

fn assert_inline_scrollback_sequence(output: &[u8]) {
    const SCROLL_INLINE_REGION: &[u8] = b"\x1b[1;12r\x1b[12S\x1b[r";
    assert!(
        output
            .windows(SCROLLBACK_SENTINEL.len())
            .any(|bytes| bytes == SCROLLBACK_SENTINEL.as_bytes()),
        "committed sentinel was not written to the terminal"
    );
    assert!(
        output
            .windows(SCROLL_INLINE_REGION.len())
            .filter(|bytes| *bytes == SCROLL_INLINE_REGION)
            .count()
            >= 3,
        "long committed content did not exercise stock scrolling regions"
    );
}

fn inline_scrollback_count(output: &[u8]) -> usize {
    const SCROLL_INLINE_REGION_START: &[u8] = b"\x1b[1;12r";
    output
        .windows(SCROLL_INLINE_REGION_START.len())
        .filter(|bytes| *bytes == SCROLL_INLINE_REGION_START)
        .count()
}

struct PtyShellSequenceProvider {
    base_url: String,
    handle: JoinHandle<usize>,
}

fn spawn_pty_shell_sequence_provider(runs: &[(&str, &str)]) -> PtyShellSequenceProvider {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let runs = runs
        .iter()
        .map(|(command, answer)| (command.to_string(), answer.to_string()))
        .collect::<Vec<_>>();
    let handle = thread::spawn(move || {
        let mut request_count = 0;
        for (index, (command, answer)) in runs.into_iter().enumerate() {
            let (mut stream, _) = listener.accept().unwrap();
            read_pty_provider_request(&mut stream);
            request_count += 1;
            let tool_delta = json!({
                "choices": [{
                    "index": 0,
                    "delta": {
                        "tool_calls": [{
                            "index": 0,
                            "id": format!("provider_pty_shell_{index}"),
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
                "choices": [{
                    "index": 0,
                    "delta": {},
                    "finish_reason": "tool_calls"
                }]
            });
            write_pty_provider_response(
                &mut stream,
                &format!("data: {tool_delta}\n\ndata: {tool_finish}\n\ndata: [DONE]\n\n"),
            );

            let (mut stream, _) = listener.accept().unwrap();
            read_pty_provider_request(&mut stream);
            request_count += 1;
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
            write_pty_provider_response(
                &mut stream,
                &format!("data: {content}\n\ndata: {finish}\n\ndata: [DONE]\n\n"),
            );
        }
        request_count
    });
    PtyShellSequenceProvider { base_url, handle }
}

fn read_pty_provider_request(stream: &mut std::net::TcpStream) {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 1024];
    let header_end = loop {
        let read = stream.read(&mut buffer).unwrap();
        assert_ne!(read, 0, "provider client closed before headers");
        bytes.extend_from_slice(&buffer[..read]);
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let headers = String::from_utf8_lossy(&bytes[..header_end]);
    let content_length = headers
        .lines()
        .find_map(|line| {
            line.strip_prefix("Content-Length:")
                .or_else(|| line.strip_prefix("content-length:"))
                .and_then(|value| value.trim().parse::<usize>().ok())
        })
        .unwrap_or(0);
    while bytes.len() < header_end + content_length {
        let read = stream.read(&mut buffer).unwrap();
        assert_ne!(read, 0, "provider client closed before body");
        bytes.extend_from_slice(&buffer[..read]);
    }
}

fn write_pty_provider_response(stream: &mut std::net::TcpStream, body: &str) {
    write!(
        stream,
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    )
    .unwrap();
    stream.flush().unwrap();
}

fn connect_pty_daemon(config: &DaemonConnectionConfig) -> DaemonClient {
    let deadline = Instant::now() + PROOF_TIMEOUT;
    loop {
        match DaemonClient::connect(&config.socket_path) {
            Ok(client) => return client,
            Err(error) => {
                assert!(
                    Instant::now() < deadline,
                    "could not connect to PTY daemon: {error}"
                );
                thread::sleep(Duration::from_millis(10));
            }
        }
    }
}

fn wait_for_finished_session(client: &mut DaemonClient, first_question: &str) -> String {
    let deadline = Instant::now() + PROOF_TIMEOUT;
    loop {
        if let Some(session) = client
            .sessions_list()
            .unwrap()
            .into_iter()
            .find(|session| session.first_question == first_question)
            && session.status == RunStateName::Finished
        {
            return session.session_id;
        }
        assert!(
            Instant::now() < deadline,
            "session {first_question:?} did not finish"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_endpoint_removal(endpoint: &Path) {
    let deadline = Instant::now() + PROOF_TIMEOUT;
    while endpoint.exists() {
        assert!(
            Instant::now() < deadline,
            "daemon endpoint remained after shutdown: {}",
            endpoint.display()
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum FakeScenario {
    FreshRun,
    PendingApproval,
    ConversationAudit,
    Streaming,
}

struct FakeDaemon {
    requests: Arc<Mutex<Vec<Envelope>>>,
    stop: Sender<()>,
    server: Option<JoinHandle<Result<(), String>>>,
}

impl FakeDaemon {
    fn bind(endpoint: &Path, workspace: &Path, workspace_id: &str, ledger: &Path) -> Self {
        Self::bind_scenario(
            endpoint,
            workspace,
            workspace_id,
            ledger,
            FakeScenario::FreshRun,
        )
    }

    fn bind_pending_approval(
        endpoint: &Path,
        workspace: &Path,
        workspace_id: &str,
        ledger: &Path,
    ) -> Self {
        Self::bind_scenario(
            endpoint,
            workspace,
            workspace_id,
            ledger,
            FakeScenario::PendingApproval,
        )
    }

    fn bind_conversation_audit(
        endpoint: &Path,
        workspace: &Path,
        workspace_id: &str,
        ledger: &Path,
    ) -> Self {
        Self::bind_scenario(
            endpoint,
            workspace,
            workspace_id,
            ledger,
            FakeScenario::ConversationAudit,
        )
    }

    fn bind_streaming(
        endpoint: &Path,
        workspace: &Path,
        workspace_id: &str,
        ledger: &Path,
    ) -> Self {
        Self::bind_scenario(
            endpoint,
            workspace,
            workspace_id,
            ledger,
            FakeScenario::Streaming,
        )
    }

    fn bind_scenario(
        endpoint: &Path,
        workspace: &Path,
        workspace_id: &str,
        ledger: &Path,
        scenario: FakeScenario,
    ) -> Self {
        fs::create_dir_all(endpoint.parent().unwrap()).unwrap();
        let listener = UnixListener::bind(endpoint).unwrap();
        listener.set_nonblocking(true).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let server_requests = Arc::clone(&requests);
        let workspace_root = workspace.canonicalize().unwrap();
        let workspace_id = workspace_id.to_owned();
        let ledger = ledger.to_path_buf();
        let (stop, stopped) = mpsc::channel();
        let server = thread::spawn(move || {
            serve_fake_daemon(
                listener,
                stopped,
                server_requests,
                workspace_root,
                workspace_id,
                ledger,
                scenario,
            )
        });
        Self {
            requests,
            stop,
            server: Some(server),
        }
    }

    fn wait_for_request_count(&self, method: &str, count: usize) {
        let deadline = Instant::now() + PROOF_TIMEOUT;
        loop {
            let actual = self
                .requests
                .lock()
                .unwrap()
                .iter()
                .filter(|request| request.method.as_deref() == Some(method))
                .count();
            if actual >= count {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "fake daemon received {actual} {method} requests, expected {count}; received {:?}",
                self.request_methods()
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn requests_for(&self, method: &str) -> Vec<Envelope> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.method.as_deref() == Some(method))
            .cloned()
            .collect()
    }

    fn wait_for_request(&self, method: &str) -> Envelope {
        let deadline = Instant::now() + PROOF_TIMEOUT;
        loop {
            if let Some(request) = self
                .requests
                .lock()
                .unwrap()
                .iter()
                .find(|request| request.method.as_deref() == Some(method))
                .cloned()
            {
                return request;
            }
            assert!(
                Instant::now() < deadline,
                "fake daemon did not receive {method}; received {:?}",
                self.request_methods()
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn request_methods(&self) -> Vec<Option<String>> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .map(|request| request.method.clone())
            .collect()
    }

    fn finish(mut self) -> Vec<Envelope> {
        let _ = self.stop.send(());
        if let Some(server) = self.server.take() {
            server.join().unwrap().unwrap();
        }
        self.requests.lock().unwrap().clone()
    }
}

impl Drop for FakeDaemon {
    fn drop(&mut self) {
        let _ = self.stop.send(());
        if let Some(server) = self.server.take() {
            let _ = server.join();
        }
    }
}

fn serve_fake_daemon(
    listener: UnixListener,
    stopped: Receiver<()>,
    requests: Arc<Mutex<Vec<Envelope>>>,
    workspace_root: PathBuf,
    workspace_id: String,
    ledger: PathBuf,
    scenario: FakeScenario,
) -> Result<(), String> {
    loop {
        match stopped.try_recv() {
            Ok(()) | Err(TryRecvError::Disconnected) => return Ok(()),
            Err(TryRecvError::Empty) => {}
        }
        match listener.accept() {
            Ok((stream, _)) => handle_connection(
                stream,
                &requests,
                &workspace_root,
                &workspace_id,
                &ledger,
                scenario,
            )?,
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(error) => return Err(format!("fake daemon accept failed: {error}")),
        }
    }
}

fn handle_connection(
    mut stream: UnixStream,
    requests: &Mutex<Vec<Envelope>>,
    workspace_root: &Path,
    workspace_id: &str,
    ledger: &Path,
    scenario: FakeScenario,
) -> Result<(), String> {
    let reader = stream
        .try_clone()
        .map_err(|error| format!("fake daemon clone failed: {error}"))?;
    let mut reader = BufReader::new(reader);
    loop {
        let mut line = String::new();
        let read = match reader.read_line(&mut line) {
            Ok(read) => read,
            Err(error) if error.kind() == ErrorKind::ConnectionReset => return Ok(()),
            Err(error) => return Err(format!("fake daemon read failed: {error}")),
        };
        if read == 0 {
            return Ok(());
        }
        let request: Envelope = serde_json::from_str(line.trim())
            .map_err(|error| format!("invalid daemon request: {error}: {line:?}"))?;
        if request.v != PROTOCOL_VERSION || request.kind != EnvelopeKind::Request {
            return Err(format!("invalid daemon envelope: {request:?}"));
        }
        let request_index = requests
            .lock()
            .unwrap()
            .iter()
            .filter(|recorded| recorded.method == request.method)
            .count();
        let response = fake_response(
            &request,
            workspace_root,
            workspace_id,
            ledger,
            scenario,
            request_index,
        )?;
        let mut response = serde_json::to_vec(&response)
            .map_err(|error| format!("fake daemon response failed: {error}"))?;
        response.push(b'\n');
        if let Err(error) = stream.write_all(&response).and_then(|()| stream.flush()) {
            if matches!(
                error.kind(),
                ErrorKind::BrokenPipe | ErrorKind::ConnectionReset
            ) {
                return Ok(());
            }
            return Err(format!("fake daemon response write failed: {error}"));
        }
        requests.lock().unwrap().push(request);
    }
}

fn fake_response(
    request: &Envelope,
    workspace_root: &Path,
    workspace_id: &str,
    ledger: &Path,
    scenario: FakeScenario,
    request_index: usize,
) -> Result<Envelope, String> {
    let method = request
        .method
        .as_deref()
        .ok_or_else(|| "daemon request omitted method".to_owned())?;
    if scenario == FakeScenario::PendingApproval && method == "events.stream" && request_index == 0
    {
        return Ok(Envelope::error(
            request.id.clone(),
            Some(method.into()),
            ERROR_LAGGED,
            "offset is no longer buffered",
        ));
    }
    let result = match method {
        "hello" => {
            let params = request
                .params
                .as_ref()
                .ok_or_else(|| "hello omitted params".to_owned())?;
            let expected_root = workspace_root.to_string_lossy();
            if params.get("workspace_root").and_then(Value::as_str) != Some(expected_root.as_ref())
                || params.get("workspace_id").and_then(Value::as_str) != Some(workspace_id)
            {
                return Err(format!(
                    "hello did not identify the test workspace: {params}"
                ));
            }
            json!({
                "daemon_version": "test",
                "workspace_id": workspace_id,
                "ledger_path": ledger.to_string_lossy(),
                "capabilities": [
                    "hello",
                    "run.start",
                    "run.cancel",
                    "events.stream",
                    "sessions.list",
                    "transcript.read",
                    "transcript.read.typed",
                    "transcript.read.pending_approval",
                    "daemon.status",
                    "approval.decide"
                ]
            })
        }
        "sessions.list" => match scenario {
            FakeScenario::FreshRun => json!({"sessions": []}),
            FakeScenario::PendingApproval => json!({
                "sessions": [
                    {
                        "session_id": "session_pty_pending",
                        "run_id": PENDING_RUN_ID,
                        "status": "running",
                        "latest_question": "review the PTY edit",
                        "first_question": "review the PTY edit",
                        "updated_at_ms": 1_785_638_400_000_u64,
                        "ledger_path": ledger.to_string_lossy()
                    },
                    {
                        "session_id": "session_pty_other",
                        "run_id": "run_pty_other",
                        "status": "running",
                        "latest_question": "other simultaneous run",
                        "first_question": "other simultaneous run",
                        "updated_at_ms": 1_785_638_300_000_u64,
                        "ledger_path": ledger.to_string_lossy()
                    }
                ]
            }),
            FakeScenario::ConversationAudit => json!({
                "sessions": [{
                    "session_id": "session_pty_conversation",
                    "run_id": CONVERSATION_RUN_ID,
                    "status": "running",
                    "latest_question": "approved, go ahead",
                    "first_question": "Conversation-first PTY question",
                    "updated_at_ms": 1_785_638_400_000_u64,
                    "ledger_path": ledger.to_string_lossy()
                }]
            }),
            FakeScenario::Streaming if request_index == 0 => json!({"sessions": []}),
            FakeScenario::Streaming => json!({
                "sessions": [{
                    "session_id": "session_pty_streaming_384",
                    "run_id": STREAMING_RUN_ID,
                    "status": "finished",
                    "latest_question": "stream the answer",
                    "first_question": "stream the answer",
                    "updated_at_ms": 1_785_638_400_000_u64,
                    "ledger_path": ledger.to_string_lossy()
                }]
            }),
        },
        "transcript.read" if scenario == FakeScenario::PendingApproval => json!({
            "run_id": PENDING_RUN_ID,
            "status": "running",
            "final_answer": null,
            "transcript": "[turn_pty] user: review the PTY edit\n",
            "pending_approval": {
                "run_id": PENDING_RUN_ID,
                "tool_call_id": PENDING_CALL_ID,
                "tool_name": "file.edit",
                "effect": "workspace_write",
                "reason": "review the PTY edit",
                "input_preview": "{\"path\":\"pty.txt\"}",
                "diff_preview": "-old PTY\n+new PTY\n"
            }
        }),
        "transcript.read" if scenario == FakeScenario::ConversationAudit => json!({
            "run_id": CONVERSATION_RUN_ID,
            "status": "running",
            "final_answer": null,
            "transcript": format!(
                "run_id: {CONVERSATION_RUN_ID}\n[turn_pty] user: {CONVERSATION_USER_TEXT}\n[turn_pty] assistant: \n[turn_pty] tool_call call_pty file.read {{\"path\":\"README.md\"}}\ntool_result call_pty README loaded\n[turn_pty] assistant: ## Conversation-first PTY answer\n\nUse **rendered Markdown**.\n\n```rust\nfn pty_rendered() {{}}\n```\n"
            ),
            "typed": {
                "runs": [{
                    "run_id": CONVERSATION_RUN_ID,
                    "session_index": 0,
                    "status": "running",
                    "entries": [
                        {"kind": "user", "text": CONVERSATION_USER_TEXT},
                        {"kind": "assistant", "text": ""},
                        {
                            "kind": "tool_call",
                            "call_id": "call_pty",
                            "tool": "file.read",
                            "input": {"path": "README.md"}
                        },
                        {
                            "kind": "tool_result",
                            "call_id": "call_pty",
                            "summary": "README loaded"
                        },
                        {"kind": "assistant", "text": "## Conversation-first PTY answer\n\nUse **rendered Markdown**.\n\n```rust\nfn pty_rendered() {}\n```"}
                    ]
                }]
            }
        }),
        "transcript.read" if scenario == FakeScenario::Streaming => json!({
            "run_id": STREAMING_RUN_ID,
            "status": "finished",
            "final_answer": STREAMING_SOURCE,
            "transcript": format!(
                "run_id: {STREAMING_RUN_ID}\n[turn_384] user: stream the answer\n[turn_384] assistant: {STREAMING_SOURCE}\n"
            ),
            "typed": {
                "runs": [{
                    "run_id": STREAMING_RUN_ID,
                    "session_index": 0,
                    "status": "finished",
                    "entries": [
                        {"kind": "user", "text": "stream the answer"},
                        {"kind": "assistant", "text": STREAMING_SOURCE}
                    ]
                }]
            }
        }),
        "run.start" => json!({
            "run_id": if scenario == FakeScenario::Streaming {
                STREAMING_RUN_ID
            } else {
                "run_tui_pty"
            },
            "session_id": "session_tui_pty",
            "ledger_path": ledger.to_string_lossy(),
            "status": "running",
            "final_answer": null
        }),
        "run.cancel" => json!({
            "run_id": "run_tui_pty",
            "status": "cancel_requested"
        }),
        "daemon.status" => json!({
            "model": {
                "requested_alias": "~openai/gpt-latest",
                "served_model": null,
                "provider_kind": "open_router",
                "key_present": false
            },
            "daemon": {
                "package_version": "0.1.0",
                "build_commit": null,
                "build_date_utc": null,
                "uptime_ms": 42,
                "endpoint_path": "/tmp/fake-agent.sock",
                "workspace_id": workspace_id
            },
            "session": {
                "session_id": null,
                "latest_run_id": null,
                "human_turn_count": 0,
                "ledger_path": ledger.to_string_lossy(),
                "core_event_count": 0
            },
            "usage": {
                "last_run": {
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "unknown_response_count": 0
                },
                "session": {
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "unknown_response_count": 0
                }
            },
            "trust": {
                "approval_granted_count": 0,
                "approval_denied_count": 0
            }
        }),
        "events.stream" => {
            let from_offset = request
                .params
                .as_ref()
                .and_then(|params| params.get("from_offset"))
                .and_then(Value::as_u64)
                .unwrap_or(9);
            let run_id = match scenario {
                FakeScenario::PendingApproval => PENDING_RUN_ID,
                FakeScenario::ConversationAudit => CONVERSATION_RUN_ID,
                FakeScenario::FreshRun => "run_tui_pty",
                FakeScenario::Streaming => STREAMING_RUN_ID,
            };
            let (next_offset, status, events) = match (scenario, request_index) {
                (FakeScenario::ConversationAudit, 0) => (
                    8,
                    "running",
                    json!([{"offset": 7, "event": {"kind": "model_stage"}}]),
                ),
                (FakeScenario::Streaming, 0) => (
                    2,
                    "running",
                    json!([
                        {
                            "offset": 0,
                            "event": {
                                "kind": "assistant_delta",
                                "run_id": STREAMING_RUN_ID,
                                "turn_id": "turn_384",
                                "step": 0,
                                "delta_index": 0,
                                "text": "Burst line one.\n"
                            }
                        },
                        {
                            "offset": 1,
                            "event": {
                                "kind": "assistant_delta",
                                "run_id": STREAMING_RUN_ID,
                                "turn_id": "turn_384",
                                "step": 0,
                                "delta_index": 1,
                                "text": "quiet partial"
                            }
                        }
                    ]),
                ),
                (FakeScenario::Streaming, 1) => (
                    5,
                    "running",
                    json!([
                        {
                            "offset": 2,
                            "event": {
                                "kind": "assistant_delta",
                                "run_id": STREAMING_RUN_ID,
                                "turn_id": "turn_384",
                                "step": 0,
                                "delta_index": 2,
                                "text": "\n| Name | Value |\n"
                            }
                        },
                        {
                            "offset": 3,
                            "event": {
                                "kind": "assistant_delta",
                                "run_id": STREAMING_RUN_ID,
                                "turn_id": "turn_384",
                                "step": 0,
                                "delta_index": 3,
                                "text": "| --- | --- |\n"
                            }
                        },
                        {
                            "offset": 4,
                            "event": {
                                "kind": "assistant_delta",
                                "run_id": STREAMING_RUN_ID,
                                "turn_id": "turn_384",
                                "step": 0,
                                "delta_index": 4,
                                "text": "| alpha | one"
                            }
                        }
                    ]),
                ),
                (FakeScenario::Streaming, _) => (
                    8,
                    "finished",
                    json!([
                        {
                            "offset": 5,
                            "event": {
                                "kind": "assistant_delta",
                                "run_id": STREAMING_RUN_ID,
                                "turn_id": "turn_384",
                                "step": 0,
                                "delta_index": 5,
                                "text": " |\nfinal mid-tok"
                            }
                        },
                        {
                            "offset": 6,
                            "event": {
                                "kind": "ledger",
                                "record": {
                                    "seq": 6,
                                    "occurred_at_ms": 6,
                                    "event": {
                                        "event": "model_responded",
                                        "run_id": STREAMING_RUN_ID,
                                        "turn_id": "turn_384",
                                        "step": 0,
                                        "output": {"role": "assistant", "content": STREAMING_SOURCE},
                                        "proposed_calls": [],
                                        "served_model": "test/streaming",
                                        "usage": null
                                    }
                                }
                            }
                        },
                        {
                            "offset": 7,
                            "event": {
                                "kind": "ledger",
                                "record": {
                                    "seq": 7,
                                    "occurred_at_ms": 7,
                                    "event": {"event": "run_finished", "run_id": STREAMING_RUN_ID}
                                }
                            }
                        }
                    ]),
                ),
                _ => (from_offset, "running", json!([])),
            };
            json!({
                "run_id": run_id,
                "from_offset": from_offset,
                "next_offset": next_offset,
                "status": status,
                "events": events
            })
        }
        "approval.decide" if scenario == FakeScenario::PendingApproval => json!({
            "run_id": PENDING_RUN_ID,
            "status": "running"
        }),
        _ => {
            return Ok(Envelope::error(
                request.id.clone(),
                Some(method.to_owned()),
                "unsupported_method",
                format!("fake daemon does not support {method}"),
            ));
        }
    };
    Ok(Envelope::response(
        request.id.clone(),
        Some(method.to_owned()),
        result,
    ))
}
