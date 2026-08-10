#![allow(unsafe_code)]

use platonic_client::{client::DaemonClient, installer_gate::InstallerStartupGate, paths};
use platonic_protocol::RunStateName;
use serde_json::json;
use std::{
    env, fs,
    io::{BufRead, BufReader, Read, Write},
    os::windows::process::CommandExt,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};
use windows_sys::Win32::{
    Foundation::{ERROR_FILE_NOT_FOUND, ERROR_SUCCESS},
    System::{
        Registry::{HKEY_CURRENT_USER, RRF_RT_REG_SZ, RegGetValueW},
        Threading::CREATE_NO_WINDOW,
    },
};

const INSTALL_TIMEOUT: Duration = Duration::from_secs(120);
const PROOF_TIMEOUT: Duration = Duration::from_secs(30);
const UNINSTALL_KEY: &str = r"Software\Microsoft\Windows\CurrentVersion\Uninstall\Plato";
const PROOF_KEY: &str = "installer-proof-key";

#[test]
#[ignore = "requires unsigned base and upgrade NSIS installers"]
fn unsigned_installer_cold_launch_upgrade_and_uninstall_matrix() {
    let base_installer = proof_file("PLATO_DESKTOP_TEST_BASE_INSTALLER");
    let upgrade_installer = proof_file("PLATO_DESKTOP_TEST_UPGRADE_INSTALLER");
    assert!(
        registry_string("InstallLocation").unwrap().is_none(),
        "installer proof requires a clean per-user installation using the Plato technical identity"
    );

    let first_workspace = tempfile::tempdir().unwrap();
    let second_workspace = tempfile::tempdir().unwrap();
    let first_marker = first_workspace.path().join("user-data.txt");
    let second_marker = second_workspace.path().join("user-data.txt");
    fs::write(&first_marker, b"first workspace user data").unwrap();
    fs::write(&second_marker, b"second workspace user data").unwrap();
    let workspace_file = saved_workspace_path();
    assert!(
        !workspace_file.exists(),
        "installer proof would overwrite {}",
        workspace_file.display()
    );
    write_saved_workspace(&workspace_file, first_workspace.path());

    assert!(run_installer(&base_installer).success());
    let base = InstalledSnapshot::capture("0.1.0");
    let gate = InstallerStartupGate::acquire().unwrap();
    let mut blocked_app = spawn_app(&base.main, None);
    let blocked_status = wait_for_status(&mut blocked_app, PROOF_TIMEOUT, &base.main);
    assert!(
        !blocked_status.success(),
        "desktop relaunched while the installer gate was held"
    );
    drop(gate);

    let first_provider = PausedFakeProvider::start("installed cold launch reply");
    write_provider_config(first_workspace.path(), &first_provider.base_url);
    let mut app = spawn_app(&base.main, Some(&first_workspace.path().join("plato.toml")));
    let mut first_client = connect_workspace(first_workspace.path());
    let cold_run = first_client
        .run_start("prove installed cold launch".into(), None, false)
        .unwrap();
    assert_eq!(cold_run.status, RunStateName::Running);
    first_provider.assert_request();
    first_provider.release();
    assert_eq!(
        wait_for_terminal(&mut first_client, &cold_run.run_id),
        RunStateName::Finished
    );
    drop(first_client);

    let first_lock = paths::default_lock_path(first_workspace.path()).unwrap();
    let second_lock = paths::default_lock_path(second_workspace.path()).unwrap();
    let active_provider = PausedFakeProvider::start("upgrade active reply");
    write_provider_config(second_workspace.path(), &active_provider.base_url);
    let mut second_daemon = spawn_daemon(&base.sidecar, second_workspace.path());
    let mut second_client = connect_workspace(second_workspace.path());
    let active_run = second_client
        .run_start("hold upgrade open".into(), None, false)
        .unwrap();
    active_provider.assert_request();
    assert!(app.try_wait().unwrap().is_none());

    let failed_upgrade = run_installer_with_gate_probe(&upgrade_installer);
    assert!(
        !failed_upgrade.success(),
        "active upgrade unexpectedly succeeded"
    );
    wait_for_child_exit(&mut app);
    wait_for_path_absent(&first_lock);
    assert!(second_lock.exists());
    assert!(second_daemon.try_wait().unwrap().is_none());
    base.assert_unchanged();
    assert_user_state(&workspace_file, &first_marker, &second_marker);

    active_provider.release();
    assert_eq!(
        wait_for_terminal(&mut second_client, &active_run.run_id),
        RunStateName::Finished
    );
    drop(second_client);
    assert!(run_installer(&upgrade_installer).success());
    wait_for_child_exit(&mut second_daemon);
    wait_for_path_absent(&second_lock);

    let upgraded = InstalledSnapshot::capture("0.1.1");
    assert!(
        base.main_bytes != upgraded.main_bytes,
        "upgrade did not replace the versioned desktop executable"
    );
    assert_user_state(&workspace_file, &first_marker, &second_marker);

    let mut upgraded_app = spawn_app(
        &upgraded.main,
        Some(&first_workspace.path().join("plato.toml")),
    );
    let first_client = connect_workspace(first_workspace.path());
    drop(first_client);

    let uninstall_provider = PausedFakeProvider::start("uninstall active reply");
    write_provider_config(second_workspace.path(), &uninstall_provider.base_url);
    let mut uninstall_daemon = spawn_daemon(&upgraded.sidecar, second_workspace.path());
    let mut uninstall_client = connect_workspace(second_workspace.path());
    let uninstall_run = uninstall_client
        .run_start("hold uninstall open".into(), None, false)
        .unwrap();
    uninstall_provider.assert_request();
    assert!(upgraded_app.try_wait().unwrap().is_none());

    let failed_uninstall = run_uninstaller(&upgraded.uninstaller);
    assert!(
        !failed_uninstall.success(),
        "active uninstall unexpectedly succeeded"
    );
    wait_for_child_exit(&mut upgraded_app);
    wait_for_path_absent(&first_lock);
    assert!(second_lock.exists());
    assert!(uninstall_daemon.try_wait().unwrap().is_none());
    upgraded.assert_unchanged();
    assert_user_state(&workspace_file, &first_marker, &second_marker);

    uninstall_provider.release();
    assert_eq!(
        wait_for_terminal(&mut uninstall_client, &uninstall_run.run_id),
        RunStateName::Finished
    );
    drop(uninstall_client);
    assert!(run_uninstaller(&upgraded.uninstaller).success());
    wait_for_child_exit(&mut uninstall_daemon);
    wait_for_path_absent(&second_lock);

    assert!(registry_string("InstallLocation").unwrap().is_none());
    for path in [&upgraded.main, &upgraded.sidecar, &upgraded.uninstaller] {
        assert!(!path.exists(), "installer left {}", path.display());
    }
    assert_user_state(&workspace_file, &first_marker, &second_marker);
    fs::remove_file(workspace_file).unwrap();
}

struct InstalledSnapshot {
    main: PathBuf,
    sidecar: PathBuf,
    uninstaller: PathBuf,
    main_bytes: Vec<u8>,
    sidecar_bytes: Vec<u8>,
    uninstaller_bytes: Vec<u8>,
    version: String,
}

impl InstalledSnapshot {
    fn capture(expected_version: &str) -> Self {
        let install_location = registry_string("InstallLocation")
            .unwrap()
            .expect("installer did not register InstallLocation");
        let install_dir = PathBuf::from(install_location.trim_matches('"'));
        let main = install_dir.join("plato-desktop.exe");
        let sidecar = install_dir.join("platonic.exe");
        let uninstaller = install_dir.join("uninstall.exe");
        let version = registry_string("DisplayVersion")
            .unwrap()
            .expect("installer did not register DisplayVersion");
        assert_eq!(version, expected_version);
        Self {
            main_bytes: fs::read(&main).unwrap(),
            sidecar_bytes: fs::read(&sidecar).unwrap(),
            uninstaller_bytes: fs::read(&uninstaller).unwrap(),
            main,
            sidecar,
            uninstaller,
            version,
        }
    }

    fn assert_unchanged(&self) {
        assert_eq!(
            registry_string("DisplayVersion").unwrap().as_deref(),
            Some(self.version.as_str())
        );
        assert!(fs::read(&self.main).unwrap() == self.main_bytes);
        assert!(fs::read(&self.sidecar).unwrap() == self.sidecar_bytes);
        assert!(fs::read(&self.uninstaller).unwrap() == self.uninstaller_bytes);
    }
}

fn proof_file(variable: &str) -> PathBuf {
    let path =
        PathBuf::from(env::var_os(variable).unwrap_or_else(|| panic!("{variable} is required")));
    path.canonicalize()
        .unwrap_or_else(|error| panic!("{variable} cannot be resolved: {error}"))
}

fn run_installer(installer: &Path) -> ExitStatus {
    run_bounded(installer, &["/S", "/NS"])
}

fn run_installer_with_gate_probe(installer: &Path) -> ExitStatus {
    let mut child = spawn_bounded(installer, &["/S", "/NS"]);
    let deadline = Instant::now() + PROOF_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("installer exited before acquiring its gate: {status}");
        }
        match InstallerStartupGate::acquire() {
            Ok(gate) => drop(gate),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(error) => panic!("installer gate probe failed: {error}"),
        }
        assert!(
            Instant::now() < deadline,
            "installer did not acquire its current-user gate"
        );
        thread::sleep(Duration::from_millis(10));
    }

    wait_for_status(&mut child, INSTALL_TIMEOUT, installer)
}

fn run_uninstaller(uninstaller: &Path) -> ExitStatus {
    let copy_dir = tempfile::tempdir().unwrap();
    let copy = copy_dir.path().join("uninstall.exe");
    fs::copy(uninstaller, &copy).unwrap();
    let install_dir = uninstaller.parent().unwrap();
    let mut child = Command::new(&copy)
        .arg("/S")
        .raw_arg(format!("_?={}", install_dir.display()))
        .creation_flags(CREATE_NO_WINDOW)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_for_status(&mut child, INSTALL_TIMEOUT, &copy)
}

fn run_bounded(program: &Path, args: &[&str]) -> ExitStatus {
    let mut child = spawn_bounded(program, args);
    wait_for_status(&mut child, INSTALL_TIMEOUT, program)
}

fn spawn_bounded(program: &Path, args: &[&str]) -> Child {
    Command::new(program)
        .args(args)
        .creation_flags(CREATE_NO_WINDOW)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap_or_else(|error| panic!("failed to start {}: {error}", program.display()))
}

fn wait_for_status(child: &mut Child, timeout: Duration, program: &Path) -> ExitStatus {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("{} exceeded {timeout:?}", program.display());
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn spawn_app(executable: &Path, config_path: Option<&Path>) -> Child {
    let mut command = Command::new(executable);
    if let Some(config_path) = config_path {
        command.env("PLATO_CONFIG", config_path);
    }
    command
        .env("PLATO_INSTALLER_TEST_KEY", PROOF_KEY)
        .creation_flags(CREATE_NO_WINDOW)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap()
}

fn spawn_daemon(executable: &Path, workspace: &Path) -> Child {
    Command::new(executable)
        .arg("--workspace")
        .arg(workspace)
        .env("PLATO_CONFIG", workspace.join("plato.toml"))
        .env("PLATO_INSTALLER_TEST_KEY", PROOF_KEY)
        .creation_flags(CREATE_NO_WINDOW)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap()
}

fn connect_workspace(workspace: &Path) -> DaemonClient {
    let socket = paths::default_socket_path(workspace).unwrap();
    let deadline = Instant::now() + PROOF_TIMEOUT;
    loop {
        if let Ok(mut client) = DaemonClient::connect(&socket)
            && client.hello(workspace).is_ok()
        {
            return client;
        }
        assert!(
            Instant::now() < deadline,
            "daemon did not start for {}",
            workspace.display()
        );
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_terminal(client: &mut DaemonClient, run_id: &str) -> RunStateName {
    let deadline = Instant::now() + PROOF_TIMEOUT;
    loop {
        let status = client.transcript_read(run_id).unwrap().status;
        if !matches!(
            status,
            RunStateName::Running | RunStateName::CancelRequested
        ) {
            return status;
        }
        assert!(Instant::now() < deadline, "run {run_id} did not finish");
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_child_exit(child: &mut Child) {
    let label = PathBuf::from(format!("process {}", child.id()));
    let _ = wait_for_status(child, PROOF_TIMEOUT, &label);
}

fn wait_for_path_absent(path: &Path) {
    let deadline = Instant::now() + PROOF_TIMEOUT;
    while path.exists() {
        assert!(
            Instant::now() < deadline,
            "path remained after shutdown: {}",
            path.display()
        );
        thread::sleep(Duration::from_millis(50));
    }
}

fn write_provider_config(workspace: &Path, base_url: &str) {
    fs::write(
        workspace.join("plato.toml"),
        format!(
            r#"[provider]
kind = "open_ai"
model = "installer-proof"
api_key_env = "PLATO_INSTALLER_TEST_KEY"
base_url = "{base_url}"
timeout_ms = 30000

[limits]
token_budget = 4000
max_output_tokens = 32
max_turns = 1

[tools]
enabled = ["file.read"]
"#
        ),
    )
    .unwrap();
}

fn saved_workspace_path() -> PathBuf {
    PathBuf::from(env::var_os("APPDATA").expect("APPDATA is required"))
        .join("ai.referential.plato")
        .join("workspace.json")
}

fn write_saved_workspace(path: &Path, workspace: &Path) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let canonical = workspace.canonicalize().unwrap();
    fs::write(
        path,
        serde_json::to_vec(&json!({
            "workspace_root": canonical.to_string_lossy(),
        }))
        .unwrap(),
    )
    .unwrap();
}

fn assert_user_state(workspace_file: &Path, first_marker: &Path, second_marker: &Path) {
    assert!(workspace_file.exists());
    assert_eq!(
        fs::read(first_marker).unwrap(),
        b"first workspace user data"
    );
    assert_eq!(
        fs::read(second_marker).unwrap(),
        b"second workspace user data"
    );
}

fn registry_string(value: &str) -> std::io::Result<Option<String>> {
    let subkey = wide(UNINSTALL_KEY);
    let value = wide(value);
    let mut bytes = 0;
    // SAFETY: both strings are NUL-terminated and the size pointer is writable.
    let result = unsafe {
        RegGetValueW(
            HKEY_CURRENT_USER,
            subkey.as_ptr(),
            value.as_ptr(),
            RRF_RT_REG_SZ,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &mut bytes,
        )
    };
    if result == ERROR_FILE_NOT_FOUND {
        return Ok(None);
    }
    if result != ERROR_SUCCESS {
        return Err(std::io::Error::from_raw_os_error(result as i32));
    }
    let mut buffer = vec![0_u16; (bytes as usize).div_ceil(2)];
    // SAFETY: buffer has the byte count returned by the size query.
    let result = unsafe {
        RegGetValueW(
            HKEY_CURRENT_USER,
            subkey.as_ptr(),
            value.as_ptr(),
            RRF_RT_REG_SZ,
            std::ptr::null_mut(),
            buffer.as_mut_ptr().cast(),
            &mut bytes,
        )
    };
    if result != ERROR_SUCCESS {
        return Err(std::io::Error::from_raw_os_error(result as i32));
    }
    let len = buffer
        .iter()
        .position(|word| *word == 0)
        .unwrap_or(buffer.len());
    String::from_utf16(&buffer[..len])
        .map(Some)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
}

fn wide(value: &str) -> Vec<u16> {
    value.encode_utf16().chain(std::iter::once(0)).collect()
}

struct PausedFakeProvider {
    base_url: String,
    requested: mpsc::Receiver<String>,
    release: mpsc::Sender<()>,
    handle: Option<thread::JoinHandle<()>>,
}

impl PausedFakeProvider {
    fn start(answer: &'static str) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let (requested_tx, requested) = mpsc::channel();
        let (release, release_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            let deadline = Instant::now() + PROOF_TIMEOUT;
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        match release_rx.try_recv() {
                            Ok(()) | Err(mpsc::TryRecvError::Disconnected) => return,
                            Err(mpsc::TryRecvError::Empty) => {}
                        }
                        assert!(
                            Instant::now() < deadline,
                            "provider did not receive a connection"
                        );
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("provider accept failed: {error}"),
                }
            };
            stream.set_nonblocking(false).unwrap();
            let request = read_http_request(&mut stream);
            requested_tx.send(request).unwrap();
            release_rx.recv_timeout(PROOF_TIMEOUT).unwrap();
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
            let body = format!("data: {content}\n\ndata: {finish}\n\ndata: [DONE]\n\n");
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        });
        Self {
            base_url,
            requested,
            release,
            handle: Some(handle),
        }
    }

    fn assert_request(&self) {
        let request = self.requested.recv_timeout(PROOF_TIMEOUT).unwrap();
        assert!(
            request
                .to_ascii_lowercase()
                .contains(&format!("authorization: bearer {PROOF_KEY}"))
        );
    }

    fn release(mut self) {
        self.release.send(()).unwrap();
        self.handle.take().unwrap().join().unwrap();
    }
}

impl Drop for PausedFakeProvider {
    fn drop(&mut self) {
        let _ = self.release.send(());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn read_http_request(stream: &mut std::net::TcpStream) -> String {
    stream.set_read_timeout(Some(PROOF_TIMEOUT)).unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut request = String::new();
    let mut content_length = 0;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        request.push_str(&line);
        if line == "\r\n" {
            break;
        }
        if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
            content_length = value.trim().parse().unwrap();
        }
    }
    let mut body = vec![0; content_length];
    reader.read_exact(&mut body).unwrap();
    request.push_str(&String::from_utf8_lossy(&body));
    request
}
