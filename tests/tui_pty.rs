#![cfg(unix)]

use plato_agent::{
    daemon::protocol::{Envelope, EnvelopeKind, PROTOCOL_VERSION},
    paths,
};
use pty_process::{
    Size,
    blocking::{Command, Pty, open},
};
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io::{BufRead, BufReader, ErrorKind, Read, Write},
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

#[test]
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
        .join("plato-agent")
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
        .and_then(|line| line.strip_suffix('|'))
        .expect("resized composer row should contain the visible draft and cursor");
    assert_eq!(visible_draft, EXPECTED_DRAFT);

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

    shell.write(b"q");
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

struct PtyShell {
    pty: Pty,
    child: Child,
    output: Arc<Mutex<Vec<u8>>>,
    reader: Option<JoinHandle<()>>,
}

impl PtyShell {
    fn spawn(workspace: &Path, runtime: &Path, state: &Path, home: &Path) -> Self {
        let (pty, pts) = open().unwrap();
        pty.resize(Size::new(INITIAL_ROWS, INITIAL_COLS)).unwrap();
        let reader_file = File::from(pty.as_fd().try_clone_to_owned().unwrap());
        let output = Arc::new(Mutex::new(Vec::new()));
        let reader_output = Arc::clone(&output);
        let reader = thread::spawn(move || read_pty(reader_file, reader_output));
        let child = Command::new("/bin/sh")
            .arg("-i")
            .current_dir(workspace)
            .env("TERM", "xterm-256color")
            .env("LANG", "C.UTF-8")
            .env("HOME", home)
            .env("XDG_RUNTIME_DIR", runtime)
            .env("XDG_STATE_HOME", state)
            .env("PLATO_BIN", env!("CARGO_BIN_EXE_plato"))
            .env("PTY_MARK", MARKER)
            .env("PS1", "")
            .env("PS2", "")
            .env_remove("ENV")
            .env_remove("PLATO_CONFIG")
            .spawn(pts)
            .unwrap();
        Self {
            pty,
            child,
            output,
            reader: Some(reader),
        }
    }

    fn write(&mut self, bytes: &[u8]) {
        self.pty.write_all(bytes).unwrap();
        self.pty.flush().unwrap();
    }

    fn resize(&self, rows: u16, cols: u16) {
        self.pty.resize(Size::new(rows, cols)).unwrap();
    }

    fn output_len(&self) -> usize {
        self.output.lock().unwrap().len()
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

fn read_pty(mut reader: File, output: Arc<Mutex<Vec<u8>>>) {
    let mut buffer = [0; 4096];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) | Err(_) => break,
            Ok(read) => output.lock().unwrap().extend_from_slice(&buffer[..read]),
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

struct FakeDaemon {
    requests: Arc<Mutex<Vec<Envelope>>>,
    stop: Sender<()>,
    server: Option<JoinHandle<Result<(), String>>>,
}

impl FakeDaemon {
    fn bind(endpoint: &Path, workspace: &Path, workspace_id: &str, ledger: &Path) -> Self {
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
            )
        });
        Self {
            requests,
            stop,
            server: Some(server),
        }
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
) -> Result<(), String> {
    loop {
        match stopped.try_recv() {
            Ok(()) | Err(TryRecvError::Disconnected) => return Ok(()),
            Err(TryRecvError::Empty) => {}
        }
        match listener.accept() {
            Ok((stream, _)) => {
                handle_connection(stream, &requests, &workspace_root, &workspace_id, &ledger)?
            }
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
        let response = fake_response(&request, workspace_root, workspace_id, ledger)?;
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
) -> Result<Envelope, String> {
    let method = request
        .method
        .as_deref()
        .ok_or_else(|| "daemon request omitted method".to_owned())?;
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
                    "events.stream",
                    "sessions.list",
                    "transcript.read"
                ]
            })
        }
        "sessions.list" => json!({"sessions": []}),
        "run.start" => json!({
            "run_id": "run_tui_pty",
            "session_id": "session_tui_pty",
            "ledger_path": ledger.to_string_lossy(),
            "status": "running",
            "final_answer": null
        }),
        "events.stream" => {
            let from_offset = request
                .params
                .as_ref()
                .and_then(|params| params.get("from_offset"))
                .and_then(Value::as_u64)
                .unwrap_or(0);
            json!({
                "run_id": "run_tui_pty",
                "from_offset": from_offset,
                "next_offset": from_offset,
                "status": "running",
                "events": []
            })
        }
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
