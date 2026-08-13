use super::*;
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Read, Write},
    os::unix::net::UnixListener,
    process::{Command, Stdio},
    sync::atomic::AtomicBool,
};

#[test]
fn all_http_routes_map_only_to_bounded_native_methods_and_complete_via_tls_proxy() {
    let root = tempfile::tempdir().unwrap();
    let workspace_root = root.path().join("workspace");
    std::fs::create_dir(&workspace_root).unwrap();
    let socket = root.path().join("daemon.sock");
    let native_listener = UnixListener::bind(&socket).unwrap();
    native_listener.set_nonblocking(true).unwrap();
    let native_shutdown = Arc::new(AtomicBool::new(false));
    let calls = Arc::new(Mutex::new(Vec::<(String, Value)>::new()));
    let native = spawn_native(
        native_listener,
        native_shutdown.clone(),
        calls.clone(),
        workspace_root,
    );

    let generated = generate_http_token().unwrap();
    let rotated = generate_http_token().unwrap();
    let gateway = Gateway::new(
        socket,
        vec![HttpGatewayPrincipal {
            name: "remote_laptop".into(),
            token_sha256: vec![
                Sha256::digest(generated.token.as_bytes()).into(),
                Sha256::digest(rotated.token.as_bytes()).into(),
            ],
            workspace_ids: vec!["workspace-1".into()],
        }],
        root.path().join("idempotency.db"),
    )
    .unwrap();
    let tcp = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = tcp.local_addr().unwrap();
    let gateway_shutdown = Arc::new(AtomicBool::new(false));
    let server = {
        let shutdown = gateway_shutdown.clone();
        thread::spawn(move || gateway.serve(tcp, shutdown).unwrap())
    };

    let cases = [
        ("GET", "/v1/status", None, &STATUS_METHODS[..]),
        ("GET", "/v1/workspaces", None, &WORKSPACE_METHODS[..]),
        (
            "GET",
            "/v1/workspaces/workspace-1/threads",
            None,
            &THREAD_LIST_METHODS[..],
        ),
        (
            "GET",
            "/v1/workspaces/workspace-1/threads/thread-1",
            None,
            &THREAD_STATUS_METHODS[..],
        ),
        (
            "GET",
            "/v1/workspaces/workspace-1/threads/thread-1/authority",
            None,
            &THREAD_AUTHORITY_METHODS[..],
        ),
        (
            "POST",
            "/v1/workspaces/workspace-1/threads/thread-1/messages",
            Some(r#"{"message":"mapped"}"#),
            &THREAD_SEND_METHODS[..],
        ),
        (
            "GET",
            "/v1/workspaces/workspace-1/threads/thread-1/events",
            None,
            &THREAD_EVENT_METHODS[..],
        ),
        (
            "POST",
            "/v1/workspaces/workspace-1/threads/thread-1/stop",
            Some("{}"),
            &THREAD_STOP_METHODS[..],
        ),
        (
            "GET",
            "/v1/workspaces/workspace-1/runs/run-1/transcript",
            None,
            &TRANSCRIPT_METHODS[..],
        ),
        (
            "GET",
            "/v1/workspaces/workspace-1/runs/run-1/events",
            None,
            &RUN_EVENT_METHODS[..],
        ),
        (
            "POST",
            "/v1/workspaces/workspace-1/runs/run-1/cancel",
            Some("{}"),
            &RUN_CANCEL_METHODS[..],
        ),
        (
            "POST",
            "/v1/workspaces/workspace-1/runs/run-1/approvals/call-1",
            Some(r#"{"decision":"deny","reason":"bounded"}"#),
            &APPROVAL_METHODS[..],
        ),
    ];
    let mut first_message_response = Vec::new();
    for (index, (method, path, body, expected)) in cases.into_iter().enumerate() {
        let before = calls.lock().unwrap().len();
        let response = http_request(
            address,
            &generated.token,
            method,
            path,
            body,
            Some(&format!("mapping-{index}")),
        );
        let methods = calls.lock().unwrap()[before..]
            .iter()
            .map(|(method, _)| method.clone())
            .collect::<Vec<_>>();
        assert_eq!(methods, expected, "{method} {path}");
        if path == "/v1/workspaces" {
            let response = String::from_utf8_lossy(&response);
            assert!(response.contains("workspace-1"));
            assert!(!response.contains("workspace-other"));
        }
        if path == "/v1/workspaces/workspace-1/threads" {
            let response = String::from_utf8_lossy(&response);
            assert!(response.contains("thread-1"));
            assert!(!response.contains("thread-cross"));
        }
        if path.ends_with("/messages") {
            first_message_response = response;
        }
    }

    let calls_after_mapping = calls.lock().unwrap().len();
    let replay = http_request(
        address,
        &rotated.token,
        "POST",
        "/v1/workspaces/workspace-1/threads/thread-1/messages",
        Some(r#"{"message":"mapped"}"#),
        Some("mapping-5"),
    );
    assert_eq!(calls.lock().unwrap().len(), calls_after_mapping);
    assert!(String::from_utf8_lossy(&replay).contains("Idempotency-Replayed: true"));
    assert_eq!(http_body(&replay), http_body(&first_message_response));

    let conflict = http_request(
        address,
        &generated.token,
        "POST",
        "/v1/workspaces/workspace-1/threads/thread-1/messages",
        Some(r#"{"message":"different"}"#),
        Some("mapping-5"),
    );
    assert_eq!(calls.lock().unwrap().len(), calls_after_mapping);
    assert!(String::from_utf8_lossy(&conflict).contains("idempotency_key_conflict"));

    let success = http_request(
        address,
        &generated.token,
        "POST",
        "/v1/workspaces/workspace-1/threads/thread-1/messages",
        Some(r#"{"message":"success"}"#),
        Some("success"),
    );
    let calls_after_success = calls.lock().unwrap().len();
    let success_replay = http_request(
        address,
        &generated.token,
        "POST",
        "/v1/workspaces/workspace-1/threads/thread-1/messages",
        Some(r#"{"message":"success"}"#),
        Some("success"),
    );
    assert_eq!(calls.lock().unwrap().len(), calls_after_success);
    assert_eq!(http_body(&success), http_body(&success_replay));

    let disconnected = http_request(
        address,
        &generated.token,
        "POST",
        "/v1/workspaces/workspace-1/threads/thread-1/messages",
        Some(r#"{"message":"disconnect"}"#),
        Some("disconnect"),
    );
    assert!(String::from_utf8_lossy(&disconnected).contains("native_unavailable"));
    let calls_after_disconnect = calls.lock().unwrap().len();
    let ambiguous = http_request(
        address,
        &generated.token,
        "POST",
        "/v1/workspaces/workspace-1/threads/thread-1/messages",
        Some(r#"{"message":"disconnect"}"#),
        Some("disconnect"),
    );
    assert_eq!(calls.lock().unwrap().len(), calls_after_disconnect);
    assert!(String::from_utf8_lossy(&ambiguous).contains("idempotency_outcome_unknown"));

    let timed_out = http_request(
        address,
        &generated.token,
        "POST",
        "/v1/workspaces/workspace-1/threads/thread-1/messages",
        Some(r#"{"message":"timeout"}"#),
        Some("timeout"),
    );
    assert!(String::from_utf8_lossy(&timed_out).contains("native_unavailable"));
    let calls_after_timeout = calls.lock().unwrap().len();
    let timeout_retry = http_request(
        address,
        &generated.token,
        "POST",
        "/v1/workspaces/workspace-1/threads/thread-1/messages",
        Some(r#"{"message":"timeout"}"#),
        Some("timeout"),
    );
    assert_eq!(calls.lock().unwrap().len(), calls_after_timeout);
    assert!(String::from_utf8_lossy(&timeout_retry).contains("idempotency_outcome_unknown"));

    let before_slow = calls.lock().unwrap().len();
    let slow = {
        let token = generated.token.clone();
        thread::spawn(move || {
            http_request(
                address,
                &token,
                "POST",
                "/v1/workspaces/workspace-1/threads/thread-1/messages",
                Some(r#"{"message":"slow"}"#),
                Some("slow"),
            )
        })
    };
    wait_for_call(&calls, before_slow + 5);
    let in_progress = http_request(
        address,
        &generated.token,
        "POST",
        "/v1/workspaces/workspace-1/threads/thread-1/messages",
        Some(r#"{"message":"slow"}"#),
        Some("slow"),
    );
    assert!(String::from_utf8_lossy(&in_progress).contains("idempotency_in_progress"));
    slow.join().unwrap();
    assert_eq!(calls.lock().unwrap().len(), before_slow + 5);

    let before_drop = calls.lock().unwrap().len();
    http_disconnect(
        address,
        &generated.token,
        "/v1/workspaces/workspace-1/threads/thread-1/messages",
        r#"{"message":"fire-and-forget"}"#,
        "fire-and-forget",
    );
    wait_for_call(&calls, before_drop + 5);
    assert_eq!(calls.lock().unwrap()[before_drop + 4].0, "thread.send");

    for (path, token) in [
        (
            "/v1/workspaces/workspace-other/threads",
            generated.token.as_str(),
        ),
        ("/v1/native/arbitrary", generated.token.as_str()),
        ("/v1/status", "not-a-token"),
        ("/v1/native/arbitrary", "not-a-token"),
    ] {
        let before = calls.lock().unwrap().len();
        let _ = http_request(address, token, "GET", path, None, None);
        assert_eq!(calls.lock().unwrap().len(), before, "{path}");
    }
    let before_forbidden_method = calls.lock().unwrap().len();
    let forbidden_method = http_request(
        address,
        &generated.token,
        "DELETE",
        "/v1/status",
        None,
        None,
    );
    assert!(String::from_utf8_lossy(&forbidden_method).contains("method_not_allowed"));
    assert_eq!(calls.lock().unwrap().len(), before_forbidden_method);

    let before_crossed_thread = calls.lock().unwrap().len();
    let crossed_thread = http_request(
        address,
        &generated.token,
        "POST",
        "/v1/workspaces/workspace-1/threads/thread-cross/messages",
        Some(r#"{"message":"do not dispatch"}"#),
        Some("crossed-thread"),
    );
    assert!(String::from_utf8_lossy(&crossed_thread).contains("forbidden_scope"));
    assert_eq!(
        calls.lock().unwrap()[before_crossed_thread..]
            .iter()
            .map(|(method, _)| method.as_str())
            .collect::<Vec<_>>(),
        [
            "workspace.status",
            "hello",
            "thread.authority",
            "agent.status"
        ]
    );
    let after_crossed_thread = calls.lock().unwrap().len();
    let replay = http_request(
        address,
        &generated.token,
        "POST",
        "/v1/workspaces/workspace-1/threads/thread-cross/messages",
        Some(r#"{"message":"do not dispatch"}"#),
        Some("crossed-thread"),
    );
    assert_eq!(calls.lock().unwrap().len(), after_crossed_thread);
    assert!(String::from_utf8_lossy(&replay).contains("Idempotency-Replayed: true"));

    let before_crossed_run = calls.lock().unwrap().len();
    let crossed_run = http_request(
        address,
        &generated.token,
        "GET",
        "/v1/workspaces/workspace-1/runs/run-cross/events",
        None,
        None,
    );
    assert!(String::from_utf8_lossy(&crossed_run).contains("not_found"));
    assert_eq!(
        calls.lock().unwrap()[before_crossed_run..]
            .iter()
            .map(|(method, _)| method.as_str())
            .collect::<Vec<_>>(),
        ["workspace.status", "hello", "transcript.read"]
    );
    for (method, params) in calls.lock().unwrap().iter() {
        match method.as_str() {
            "thread.send" => assert_eq!(params["controller_id"], "remote_laptop"),
            "thread.stop" | "run.cancel" | "approval.decide" => {
                assert_eq!(params["actor"], "remote_laptop")
            }
            _ => {}
        }
    }
    https_journey_via_test_proxy(address, &generated.token, root.path());
    gateway_shutdown.store(true, Ordering::SeqCst);
    server.join().unwrap();
    native_shutdown.store(true, Ordering::SeqCst);
    native.join().unwrap();
}

static STATUS_METHODS: [&str; 3] = ["workspace.status", "hello", "daemon.status"];
static WORKSPACE_METHODS: [&str; 1] = ["workspace.list"];
static THREAD_LIST_METHODS: [&str; 7] = [
    "workspace.status",
    "hello",
    "thread.list",
    "thread.authority",
    "agent.status",
    "thread.authority",
    "agent.status",
];
static THREAD_STATUS_METHODS: [&str; 5] = [
    "workspace.status",
    "hello",
    "thread.authority",
    "agent.status",
    "thread.status",
];
static THREAD_AUTHORITY_METHODS: [&str; 4] = [
    "workspace.status",
    "hello",
    "thread.authority",
    "agent.status",
];
static THREAD_SEND_METHODS: [&str; 5] = [
    "workspace.status",
    "hello",
    "thread.authority",
    "agent.status",
    "thread.send",
];
static THREAD_EVENT_METHODS: [&str; 5] = [
    "workspace.status",
    "hello",
    "thread.authority",
    "agent.status",
    "thread.events",
];
static THREAD_STOP_METHODS: [&str; 5] = [
    "workspace.status",
    "hello",
    "thread.authority",
    "agent.status",
    "thread.stop",
];
static TRANSCRIPT_METHODS: [&str; 3] = ["workspace.status", "hello", "transcript.read"];
static RUN_EVENT_METHODS: [&str; 4] = [
    "workspace.status",
    "hello",
    "transcript.read",
    "events.stream",
];
static RUN_CANCEL_METHODS: [&str; 4] =
    ["workspace.status", "hello", "transcript.read", "run.cancel"];
static APPROVAL_METHODS: [&str; 4] = [
    "workspace.status",
    "hello",
    "transcript.read",
    "approval.decide",
];

fn spawn_native(
    listener: UnixListener,
    shutdown: Arc<AtomicBool>,
    calls: Arc<Mutex<Vec<(String, Value)>>>,
    workspace_root: PathBuf,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        while !shutdown.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((stream, _)) => {
                    let calls = calls.clone();
                    let root = workspace_root.clone();
                    thread::spawn(move || native_connection(stream, &calls, &root));
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5))
                }
                Err(error) => panic!("fake native accept failed: {error}"),
            }
        }
    })
}

fn native_connection(
    stream: std::os::unix::net::UnixStream,
    calls: &Mutex<Vec<(String, Value)>>,
    root: &std::path::Path,
) {
    let mut writer = stream.try_clone().unwrap();
    let mut reader = BufReader::new(stream);
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => return,
            Ok(_) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                ) =>
            {
                return;
            }
            Err(error) => panic!("fake native read failed: {error}"),
        }
        let request: Value = serde_json::from_str(line.trim()).unwrap();
        let method = request["method"].as_str().unwrap().to_owned();
        let params = request.get("params").cloned().unwrap_or(Value::Null);
        calls.lock().unwrap().push((method.clone(), params.clone()));
        if method == "thread.send" && params["message"] == "disconnect" {
            return;
        }
        if method == "thread.send" && params["message"] == "slow" {
            thread::sleep(Duration::from_millis(250));
        }
        if method == "thread.send" && params["message"] == "timeout" {
            thread::sleep(DAEMON_TIMEOUT + Duration::from_millis(100));
        }
        let result = match method.as_str() {
            "workspace.status" => Some(json!({"workspace": workspace("workspace-1", root)})),
            "workspace.list" => Some(
                json!({"workspaces": [workspace("workspace-1", root), workspace("workspace-other", root)]}),
            ),
            "hello" => Some(
                json!({"daemon_version": "test", "workspace_id": "workspace-1", "ledger_path": root.join("ledger.db"), "capabilities": REQUIRED_CAPABILITIES, "daemon_scope": "host"}),
            ),
            "daemon.status" => Some(daemon_status(root)),
            "thread.list" => Some(json!({
                "threads": [thread_status("thread-1"), thread_status("thread-cross")]
            })),
            "thread.authority" => {
                let thread_id = params["thread_id"].as_str().unwrap();
                let agent_id = if thread_id == "thread-cross" {
                    "other-agent"
                } else {
                    "remote-agent"
                };
                Some(json!({
                    "authority": {
                        "thread_id": thread_id,
                        "parent_thread_id": null,
                        "spawning_actor": "local",
                        "agent_id": agent_id,
                        "model": "test",
                        "reasoning_effort": "medium",
                        "approval_policy": "prompt",
                        "toolset": [],
                        "worktrees": [],
                        "granted_paths": [],
                        "network": false,
                        "created_at_ms": 1
                    },
                    "confinement": "none"
                }))
            }
            "agent.status" => {
                let agent_id = params["agent_id"].as_str().unwrap();
                let workspace_id = if agent_id == "other-agent" {
                    "workspace-other"
                } else {
                    "workspace-1"
                };
                Some(json!({
                    "agent": {
                        "id": agent_id,
                        "workspace_id": workspace_id,
                        "model": "test",
                        "reasoning_effort": "medium",
                        "approval_policy": "prompt",
                        "toolset": [],
                        "created_at_ms": 1
                    }
                }))
            }
            "thread.status" => Some(json!({
                "thread": thread_status(params["thread_id"].as_str().unwrap())
            })),
            "thread.events" if params["thread_id"] == "thread-tls" => {
                let from_offset = params["from_offset"].as_u64().unwrap_or(0);
                let events = if from_offset == 0 {
                    vec![json!({
                        "offset": 0,
                        "turn_id": "turn-tls",
                        "event": {"kind": "canceled", "run_id": "run-tls"}
                    })]
                } else {
                    Vec::new()
                };
                Some(json!({
                    "thread_id": "thread-tls",
                    "from_offset": from_offset,
                    "next_offset": 1,
                    "current_turn_id": "turn-tls",
                    "events": events
                }))
            }
            "thread.stop" => Some(json!({
                "status": "stopped",
                "thread_id": params["thread_id"],
                "stopped_turn_id": null,
                "stopped_at_ms": 1
            })),
            "transcript.read" if params["run_id"] != "run-cross" => Some(json!({
                "run_id": params["run_id"],
                "status": "running",
                "final_answer": null,
                "transcript": "",
                "typed": null,
                "pending_approval": null,
                "completion_claim": null
            })),
            "events.stream" if params["run_id"] == "run-tls" => {
                let from_offset = params["from_offset"].as_u64().unwrap_or(0);
                let events = if from_offset == 0 {
                    vec![json!({
                        "offset": 0,
                        "event": {"kind": "canceled", "run_id": "run-tls"}
                    })]
                } else {
                    Vec::new()
                };
                Some(json!({
                    "run_id": "run-tls",
                    "from_offset": from_offset,
                    "next_offset": 1,
                    "status": "running",
                    "events": events
                }))
            }
            "run.cancel" => Some(json!({
                "run_id": params["run_id"],
                "status": "cancel_requested"
            })),
            "approval.decide" => Some(json!({
                "run_id": params["run_id"],
                "status": "running"
            })),
            "thread.send"
                if matches!(
                    params["message"].as_str(),
                    Some("success" | "slow" | "timeout" | "fire-and-forget" | "tls-success")
                ) =>
            {
                Some(
                    json!({"status": "started", "thread_id": params["thread_id"], "turn_id": "turn-1"}),
                )
            }
            _ => None,
        };
        let response = result.map_or_else(
            || json!({"v": 1, "id": request["id"], "kind": "error", "method": method, "error": {"code": "not_found", "message": "deterministic native rejection"}}),
            |result| json!({"v": 1, "id": request["id"], "kind": "response", "method": method, "result": result}),
        );
        if serde_json::to_writer(&mut writer, &response).is_err()
            || writer.write_all(b"\n").is_err()
            || writer.flush().is_err()
        {
            return;
        }
    }
}

fn workspace(id: &str, root: &std::path::Path) -> Value {
    json!({"id": id, "name": id, "root": root, "ledger_path": root.join(format!("{id}.db")), "created_at_ms": 1, "health": "present"})
}

fn daemon_status(root: &std::path::Path) -> Value {
    json!({
        "model": {
            "requested_alias": "test",
            "served_model": null,
            "provider_kind": "open_router",
            "key_present": false
        },
        "daemon": {
            "package_version": "test",
            "build_commit": null,
            "build_date_utc": null,
            "uptime_ms": 1,
            "endpoint_path": root.join("daemon.sock"),
            "workspace_id": "workspace-1"
        },
        "session": {
            "session_id": null,
            "latest_run_id": null,
            "human_turn_count": 0,
            "ledger_path": root.join("ledger.db"),
            "core_event_count": 0
        },
        "usage": {
            "last_run": {"input_tokens": 0, "output_tokens": 0, "unknown_response_count": 0},
            "session": {"input_tokens": 0, "output_tokens": 0, "unknown_response_count": 0}
        },
        "trust": {
            "approval_granted_count": 0,
            "approval_denied_count": 0,
            "shell_session_grant": false
        }
    })
}

fn thread_status(thread_id: &str) -> Value {
    json!({
        "authority": {
            "thread_id": thread_id,
            "parent_thread_id": null,
            "spawning_actor": "local",
            "cwd": "/tmp/workspace",
            "model": "test",
            "reasoning_effort": "medium",
            "approval_policy": "prompt",
            "created_at_ms": 1
        },
        "live": {"loaded": false, "current_turn_id": null}
    })
}

fn http_request(
    address: SocketAddr,
    token: &str,
    method: &str,
    path: &str,
    body: Option<&str>,
    key: Option<&str>,
) -> Vec<u8> {
    let body = body.unwrap_or("");
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token}\r\nContent-Length: {}\r\n",
        body.len()
    );
    if method == "POST" {
        request.push_str("Content-Type: application/json\r\n");
    }
    if let Some(key) = key {
        request.push_str(&format!("Idempotency-Key: {key}\r\n"));
    }
    request.push_str("\r\n");
    request.push_str(body);
    let mut stream = TcpStream::connect(address).unwrap();
    stream.write_all(request.as_bytes()).unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();
    response
}

fn http_body(response: &[u8]) -> &[u8] {
    let start = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .unwrap()
        + 4;
    &response[start..]
}

fn http_disconnect(address: SocketAddr, token: &str, path: &str, body: &str, key: &str) {
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nIdempotency-Key: {key}\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    let mut stream = TcpStream::connect(address).unwrap();
    stream.write_all(request.as_bytes()).unwrap();
}

fn https_journey_via_test_proxy(address: SocketAddr, token: &str, root: &std::path::Path) {
    let cert = root.join("test-cert.pem");
    let key = root.join("test-key.pem");
    let generated = Command::new("openssl")
        .args([
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            key.to_str().unwrap(),
            "-out",
            cert.to_str().unwrap(),
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("openssl is required for the Linux/macOS TLS proxy proof");
    assert!(
        generated.success(),
        "test TLS certificate generation failed"
    );

    let mut child = Command::new("python3")
        .args([
            "-c",
            TLS_PROXY_JOURNEY,
            &address.port().to_string(),
            cert.to_str().unwrap(),
            key.to_str().unwrap(),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("python3 is required for the Linux/macOS TLS proxy proof");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(token.as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "TLS proxy journey failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

const TLS_PROXY_JOURNEY: &str = r#"
import http.client
import json
import socket
import ssl
import sys
import threading

gateway_port = int(sys.argv[1])
cert_path = sys.argv[2]
key_path = sys.argv[3]
token = sys.stdin.read().strip()

cases = [
    ("GET", "/v1/status", None, False),
    ("GET", "/v1/workspaces", None, False),
    ("GET", "/v1/workspaces/workspace-1/threads", None, False),
    ("GET", "/v1/workspaces/workspace-1/threads/thread-tls", None, False),
    ("GET", "/v1/workspaces/workspace-1/threads/thread-tls/authority", None, False),
    ("POST", "/v1/workspaces/workspace-1/threads/thread-tls/messages", '{"message":"tls-success"}', False),
    ("GET", "/v1/workspaces/workspace-1/threads/thread-tls/events", None, True),
    ("GET", "/v1/workspaces/workspace-1/runs/run-tls/transcript", None, False),
    ("GET", "/v1/workspaces/workspace-1/runs/run-tls/events", None, True),
    ("POST", "/v1/workspaces/workspace-1/runs/run-tls/approvals/call-tls", '{"decision":"deny","reason":"bounded"}', False),
    ("POST", "/v1/workspaces/workspace-1/runs/run-tls/cancel", '{}', False),
    ("POST", "/v1/workspaces/workspace-1/threads/thread-tls/stop", '{}', False),
]

server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
server_context.load_cert_chain(cert_path, key_path)
listener = socket.socket()
listener.bind(("127.0.0.1", 0))
listener.listen(16)
proxy_port = listener.getsockname()[1]

def pump(source, destination):
    try:
        while True:
            data = source.recv(65536)
            if not data:
                break
            destination.sendall(data)
    except (OSError, ssl.SSLError):
        pass
    try:
        destination.shutdown(socket.SHUT_WR)
    except OSError:
        pass

def handle(raw):
    incoming = server_context.wrap_socket(raw, server_side=True)
    upstream = socket.create_connection(("127.0.0.1", gateway_port), timeout=5)
    try:
        incoming.settimeout(6)
        upstream.settimeout(6)
        threads = [
            threading.Thread(target=pump, args=(incoming, upstream)),
            threading.Thread(target=pump, args=(upstream, incoming)),
        ]
        for worker in threads:
            worker.start()
        for worker in threads:
            worker.join()
    finally:
        incoming.close()
        upstream.close()

def proxy():
    workers = []
    for _ in cases:
        raw, _ = listener.accept()
        worker = threading.Thread(target=handle, args=(raw,))
        worker.start()
        workers.append(worker)
    for worker in workers:
        worker.join()
    listener.close()

proxy_thread = threading.Thread(target=proxy, daemon=True)
proxy_thread.start()
client_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
client_context.check_hostname = False
client_context.verify_mode = ssl.CERT_NONE

for index, (method, path, body, is_stream) in enumerate(cases):
    connection = http.client.HTTPSConnection(
        "localhost", proxy_port, context=client_context, timeout=5
    )
    headers = {"Authorization": "Bearer " + token}
    if method == "POST":
        headers["Content-Type"] = "application/json"
        headers["Idempotency-Key"] = "tls-journey-" + str(index)
    connection.request(method, path, body=body, headers=headers)
    response = connection.getresponse()
    if response.status != 200:
        raise AssertionError((path, response.status, response.read().decode()))
    if is_stream:
        saw_data = False
        while True:
            line = response.fp.readline()
            if not line:
                break
            saw_data = saw_data or line.startswith(b"data: ")
            if saw_data and line == b"\n":
                break
        if not saw_data:
            raise AssertionError((path, "missing SSE data"))
    else:
        json.loads(response.read())
    connection.close()

proxy_thread.join(10)
if proxy_thread.is_alive():
    raise AssertionError("TLS test proxy did not stop")
"#;

fn wait_for_call(calls: &Mutex<Vec<(String, Value)>>, count: usize) {
    let started = Instant::now();
    while calls.lock().unwrap().len() < count {
        assert!(started.elapsed() < Duration::from_secs(2));
        thread::sleep(Duration::from_millis(5));
    }
}
