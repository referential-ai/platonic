use serde_json::{Value, json};
use std::{
    fs,
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

const PROVIDER_TIMEOUT: Duration = Duration::from_secs(10);

#[test]
fn cli_fake_provider_writes_candidate_artifacts_in_order_and_rejects_reuse() {
    let provider = FakeProvider::start(candidate_responses());
    let workspace = tempfile::tempdir().unwrap();
    let environment = CliEnvironment::new(workspace.path());
    let config_path = workspace.path().join("test-plato.toml");
    write_config(&config_path, &provider.base_url);
    let run_dir = workspace.path().join("run");
    let input = "Turn this rough request into a bounded implementation issue.";

    let output = run_cli(&environment, &config_path, &run_dir, input);

    assert!(
        output.status.success(),
        "issue prep failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("## Acceptance Criteria"));
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains(&format!("run_dir: {}", run_dir.display()))
    );
    let mut names = fs::read_dir(&run_dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(
        names,
        [
            "00-manifest.json",
            "01-input.md",
            "10-prepare.prompt.md",
            "11-prepare.result.json",
            "12-prepare.validation.json",
            "20-refine.prompt.md",
            "21-refine.result.json",
            "22-refine.validation.json",
            "30-review.prompt.md",
            "31-review.result.json",
            "32-review.validation.json",
            "40-candidate.md",
        ]
    );
    assert_eq!(
        validation_kind(&run_dir.join("12-prepare.validation.json")),
        "structural"
    );
    assert_eq!(
        validation_kind(&run_dir.join("22-refine.validation.json")),
        "structural"
    );
    assert_eq!(
        validation_kind(&run_dir.join("32-review.validation.json")),
        "model_review"
    );

    let requests = provider.join();
    assert_eq!(requests.len(), 3);
    for request in &requests {
        assert_eq!(request["tools"], json!([]));
    }
    assert!(request_prompt(&requests[0]).starts_with("# Stage: Prepare"));
    assert!(request_prompt(&requests[1]).starts_with("# Stage: Refine"));
    assert!(request_prompt(&requests[2]).starts_with("# Stage: Review"));

    let before = fs::read(run_dir.join("40-candidate.md")).unwrap();
    let repeated = run_cli(&environment, &config_path, &run_dir, input);
    assert!(!repeated.status.success());
    assert!(String::from_utf8_lossy(&repeated.stderr).contains("requires a new run directory"));
    assert_eq!(fs::read(run_dir.join("40-candidate.md")).unwrap(), before);
}

#[test]
fn cli_fake_provider_structural_failure_blocks_before_refinement() {
    let provider = FakeProvider::start(vec!["not json".into()]);
    let workspace = tempfile::tempdir().unwrap();
    let environment = CliEnvironment::new(workspace.path());
    let config_path = workspace.path().join("test-plato.toml");
    write_config(&config_path, &provider.base_url);
    let run_dir = workspace.path().join("run");

    let output = run_cli(&environment, &config_path, &run_dir, "Prepare this issue.");

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("blocked at prepare"));
    assert!(run_dir.join("12-prepare.validation.json").is_file());
    assert!(!run_dir.join("20-refine.prompt.md").exists());
    assert_eq!(provider.join().len(), 1);
}

#[test]
fn cli_help_contains_only_the_start_execution_path() {
    let output = Command::new(env!("CARGO_BIN_EXE_plato"))
        .args(["issue-prep", "--help"])
        .output()
        .unwrap();
    let help = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    assert!(help.contains("start"));
    assert!(!help.contains("resume"));
    assert!(!help.contains("--issue"));
}

fn run_cli(
    environment: &CliEnvironment,
    config_path: &Path,
    run_dir: &Path,
    input: &str,
) -> std::process::Output {
    let mut command = environment.command(env!("CARGO_BIN_EXE_plato"));
    let mut child = command
        .arg("--config")
        .arg(config_path)
        .args(["issue-prep", "start"])
        .arg(run_dir)
        .env("PLATO_ISSUE_PREP_TEST_KEY", "test-key")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

struct CliEnvironment {
    workspace: PathBuf,
    runtime: PathBuf,
    state: PathBuf,
    home: PathBuf,
    daemon: Option<Child>,
}

impl CliEnvironment {
    fn new(workspace: &Path) -> Self {
        let mut environment = Self {
            workspace: workspace.to_path_buf(),
            runtime: workspace.join(".runtime"),
            state: workspace.join(".state"),
            home: workspace.join(".home"),
            daemon: None,
        };
        for directory in [&environment.runtime, &environment.state, &environment.home] {
            fs::create_dir(directory).unwrap();
            #[cfg(unix)]
            fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let child = environment
            .command(workspace_binary("platonic"))
            .arg("serve")
            .env("PLATO_ISSUE_PREP_TEST_KEY", "test-key")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        environment.daemon = Some(child);
        let deadline = Instant::now() + PROVIDER_TIMEOUT;
        loop {
            let created = environment
                .command(workspace_binary("platonic"))
                .args(["workspace", "create", "issue-prep"])
                .arg(workspace)
                .output()
                .unwrap();
            if created.status.success() {
                break;
            }
            assert!(
                environment
                    .daemon
                    .as_mut()
                    .unwrap()
                    .try_wait()
                    .unwrap()
                    .is_none(),
                "issue-prep daemon exited before workspace.create"
            );
            assert!(Instant::now() < deadline, "issue-prep daemon did not bind");
            thread::sleep(Duration::from_millis(10));
        }
        environment
    }

    fn command(&self, binary: impl AsRef<Path>) -> Command {
        let mut command = Command::new(binary.as_ref());
        command
            .current_dir(&self.workspace)
            .env("PLATONIC_BIN", workspace_binary("platonic"))
            .env("HOME", &self.home);
        #[cfg(unix)]
        command
            .env("XDG_RUNTIME_DIR", &self.runtime)
            .env("XDG_STATE_HOME", &self.state);
        command
    }
}

impl Drop for CliEnvironment {
    fn drop(&mut self) {
        let _ = self
            .command(workspace_binary("platonic"))
            .args(["shutdown", "--workspace", "."])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        if let Some(child) = self.daemon.as_mut() {
            let deadline = Instant::now() + PROVIDER_TIMEOUT;
            while child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(10));
            }
            if child.try_wait().ok().flatten().is_none() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
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

fn validation_kind(path: &Path) -> String {
    let validation: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    validation["validation_kind"].as_str().unwrap().into()
}

fn request_prompt(request: &Value) -> &str {
    request["messages"][1]["content"].as_str().unwrap()
}

fn candidate_responses() -> Vec<String> {
    let issue = json!({
        "title": "Add fixed issue preparation",
        "problem": "Rough requests are not normalized.",
        "current_behavior": "The request remains free-form.",
        "expected_behavior": "One bounded candidate is emitted.",
        "target_repo_surface": "plato-agent issue-prep CLI",
        "scope": ["Run the fixed pipeline."],
        "non_goals": ["No configurable workflow."],
        "acceptance_criteria": ["The candidate contains every required section."],
        "proof": ["The CLI fake-provider test passes."],
        "open_questions": []
    })
    .to_string();
    let review = json!({
        "verdict": "candidate",
        "findings": []
    })
    .to_string();
    vec![issue.clone(), issue, review]
}

fn write_config(path: &Path, base_url: &str) {
    fs::write(
        path,
        format!(
            r#"
[provider]
kind = "open_ai"
model = "test-model"
api_key_env = "PLATO_ISSUE_PREP_TEST_KEY"
base_url = "{base_url}"
timeout_ms = 10000

[limits]
token_budget = 4000
max_output_tokens = 1024
max_turns = 2

[tools]
enabled = ["file.read"]
"#
        ),
    )
    .unwrap();
}

struct FakeProvider {
    base_url: String,
    handle: thread::JoinHandle<Vec<Value>>,
}

impl FakeProvider {
    fn start(responses: Vec<String>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let handle = thread::spawn(move || {
            let deadline = Instant::now() + PROVIDER_TIMEOUT;
            let mut requests = Vec::new();
            for content in responses {
                let mut stream = loop {
                    match listener.accept() {
                        Ok((stream, _)) => break stream,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            assert!(
                                Instant::now() < deadline,
                                "timed out waiting for issue-prep provider request"
                            );
                            thread::sleep(Duration::from_millis(10));
                        }
                        Err(error) => panic!("fake provider accept failed: {error}"),
                    }
                };
                let request = read_request(&stream);
                requests.push(serde_json::from_str(&request).unwrap());
                write_response(&mut stream, &content);
            }
            requests
        });
        Self { base_url, handle }
    }

    fn join(self) -> Vec<Value> {
        self.handle.join().unwrap()
    }
}

fn read_request(stream: &TcpStream) -> String {
    stream.set_read_timeout(Some(PROVIDER_TIMEOUT)).unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut content_length = None;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert!(!line.is_empty(), "provider request ended before headers");
        if line == "\r\n" {
            break;
        }
        if let Some(value) = line
            .to_ascii_lowercase()
            .strip_prefix("content-length:")
            .map(str::trim)
        {
            content_length = Some(value.parse::<usize>().unwrap());
        }
    }
    let mut body = vec![0; content_length.expect("content-length header")];
    reader.read_exact(&mut body).unwrap();
    String::from_utf8(body).unwrap()
}

fn write_response(stream: &mut TcpStream, content: &str) {
    let body = json!({
        "choices": [{
            "finish_reason": "stop",
            "message": {
                "content": content
            }
        }],
        "usage": {
            "prompt_tokens": 10,
            "completion_tokens": 20
        }
    })
    .to_string();
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).unwrap();
}
