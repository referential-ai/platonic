use platonic_server::inference_proxy::InferenceProxyStatus;
use serde_json::{Value, json};
use std::{
    fs,
    io::Write,
    net::TcpStream,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Output},
    thread,
    time::{Duration, Instant},
};

const SECRET: &str = "integration-openrouter-key-sentinel";

#[test]
fn lifecycle_is_idempotent_private_and_refuses_active_down() {
    let root = tempfile::tempdir().unwrap();
    let runtime = root.path().join("runtime");
    let state = root.path().join("state");
    let capture = root.path().join("capture");

    let first = command(&runtime, &state)
        .args([
            "inference-proxy",
            "up",
            "--capture-dir",
            capture.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    let first_status = successful_status(&first);
    let _guard = ProxyGuard::new(runtime.clone(), state.clone());
    assert!(first_status.running);
    assert_eq!(first_status.capture_dir.as_deref(), Some(capture.as_path()));
    assert!(
        first_status
            .base_url
            .as_deref()
            .unwrap()
            .ends_with("/api/v1")
    );

    let second = command(&runtime, &state)
        .args(["inference-proxy", "up"])
        .output()
        .unwrap();
    assert_eq!(successful_status(&second), first_status);
    let status = command(&runtime, &state)
        .args(["inference-proxy", "status"])
        .output()
        .unwrap();
    assert_eq!(successful_status(&status), first_status);

    let control = runtime.join("platonic/inference-proxy/control.sock");
    assert!(control.as_os_str().len() < 100);
    assert_eq!(
        fs::symlink_metadata(control.parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        fs::symlink_metadata(&control).unwrap().permissions().mode() & 0o777,
        0o600
    );

    let address = first_status
        .base_url
        .as_deref()
        .unwrap()
        .strip_prefix("http://")
        .unwrap()
        .strip_suffix("/api/v1")
        .unwrap();
    let mut flow = TcpStream::connect(address).unwrap();
    flow.write_all(
        b"POST /api/v1/responses HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: 50\r\n\r\n{",
    )
    .unwrap();
    wait_for_active(&runtime, &state, 1);
    let refused = command(&runtime, &state)
        .args(["inference-proxy", "down"])
        .output()
        .unwrap();
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("1 active flow(s); down refused"));
    drop(flow);
    wait_for_active(&runtime, &state, 0);

    let stopped = command(&runtime, &state)
        .args(["inference-proxy", "down"])
        .output()
        .unwrap();
    assert!(!successful_status(&stopped).running);
    let stopped_again = command(&runtime, &state)
        .args(["inference-proxy", "down"])
        .output()
        .unwrap();
    assert!(!successful_status(&stopped_again).running);
    assert!(capture.join("traffic.jsonl").is_file());
    assert!(
        fs::read_to_string(capture.join("traffic.jsonl"))
            .unwrap()
            .contains("downstream_disconnect")
    );

    for output in [&first, &second, &status, &refused, &stopped, &stopped_again] {
        assert!(
            !output
                .stdout
                .windows(SECRET.len())
                .any(|bytes| bytes == SECRET.as_bytes())
        );
        assert!(
            !output
                .stderr
                .windows(SECRET.len())
                .any(|bytes| bytes == SECRET.as_bytes())
        );
    }
    assert_root_lacks_secret(root.path());
}

#[test]
fn compare_command_emits_one_responses_and_one_chat_capture() {
    let root = tempfile::tempdir().unwrap();
    let capture = root.path().join("capture");
    fs::create_dir(&capture).unwrap();
    let fixtures = [
        (
            "flow-00000001",
            "/api/v1/responses",
            br#"{"input":"marker","model":"m","stream":false}"#.as_slice(),
            "090391f2caf47e6f05fb5eb90381cdbf50ac79dd94a3b40bb39f6f5ed6dae7b7",
        ),
        (
            "flow-00000002",
            "/api/v1/chat/completions",
            br#"{"messages":[{"content":"marker","role":"user"}],"model":"m","stream":false}"#
                .as_slice(),
            "09f11cfc5ea2281a74ea03232fac46b59df618777726b88227b0bf5591ee39e0",
        ),
    ];
    let mut lines = Vec::new();
    let mut seq = 0u64;
    for (flow_id, path, body, hash) in fixtures {
        for (event, fields) in [
            ("flow_start", json!({})),
            (
                "request_head",
                json!({"path":path,"protocol":"HTTP/1.1","headers":[]}),
            ),
            (
                "request_body_chunk",
                json!({"offset":0,"bytes":body.len(),"hex":hex(body)}),
            ),
            ("request_end", json!({"bytes":body.len(),"sha256":hash})),
        ] {
            seq += 1;
            let mut value = fields;
            value["v"] = 1.into();
            value["seq"] = seq.into();
            value["flow_id"] = flow_id.into();
            value["wall_ms"] = 1.into();
            value["delta_us"] = seq.into();
            value["event"] = event.into();
            lines.push(serde_json::to_string(&value).unwrap());
        }
    }
    fs::write(capture.join("traffic.jsonl"), lines.join("\n") + "\n").unwrap();

    let output = command(&root.path().join("runtime"), &root.path().join("state"))
        .args(["inference-proxy", "compare", capture.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let compared: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(compared["flows"][0]["protocol"], "responses");
    assert_eq!(compared["flows"][0]["user_content"][0], "marker");
    assert_eq!(compared["flows"][1]["protocol"], "chat_completions");
    assert_eq!(compared["flows"][1]["user_content"][0], "marker");
}

fn command(runtime: &Path, state: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_platonic"));
    command
        .env("XDG_RUNTIME_DIR", runtime)
        .env("XDG_STATE_HOME", state)
        .env("OPENROUTER_API_KEY", SECRET);
    command
}

fn successful_status(output: &Output) -> InferenceProxyStatus {
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn wait_for_active(runtime: &Path, state: &Path, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let output = command(runtime, state)
            .args(["inference-proxy", "status"])
            .output()
            .unwrap();
        let status = successful_status(&output);
        if status.active_flows == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "active flow count stayed at {}",
            status.active_flows
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn assert_root_lacks_secret(path: &Path) {
    for entry in fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() {
            assert_root_lacks_secret(&entry.path());
        } else {
            let bytes = fs::read(entry.path()).unwrap();
            assert!(
                !bytes
                    .windows(SECRET.len())
                    .any(|part| part == SECRET.as_bytes())
            );
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut output = String::new();
    for byte in bytes {
        write!(&mut output, "{byte:02x}").unwrap();
    }
    output
}

struct ProxyGuard {
    runtime: PathBuf,
    state: PathBuf,
}

impl ProxyGuard {
    fn new(runtime: PathBuf, state: PathBuf) -> Self {
        Self { runtime, state }
    }
}

impl Drop for ProxyGuard {
    fn drop(&mut self) {
        let _ = command(&self.runtime, &self.state)
            .args(["inference-proxy", "down"])
            .output();
    }
}
