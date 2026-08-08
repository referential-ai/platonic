use crate::{
    AppError, AppResult,
    daemon::{
        client::{ClientError, DaemonClient},
        lock::{LOCK_VERSION, LockMetadata},
        protocol::{CAPABILITY_DAEMON_SHUTDOWN_IF_IDLE, ShutdownIfIdleResultName},
    },
    paths,
    windows_security::{self, CurrentUserProcess},
};
use serde::Serialize;
use std::{
    fs::{self, File},
    io::{ErrorKind, Write},
    os::windows::fs::{FileExt, MetadataExt},
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};
use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;

const LOCK_FILE_NAME: &str = "agent.lock";
const MAX_LOCK_BYTES: u64 = 16 * 1024;
const METADATA_RETRY: Duration = Duration::from_millis(500);
const METADATA_RETRY_INTERVAL: Duration = Duration::from_millis(25);
const PROCESS_EXIT_TIMEOUT: Duration = Duration::from_secs(5);

struct ValidatedWorkspace {
    lock_path: PathBuf,
    lock_file: Option<File>,
    raw_lock: Vec<u8>,
    metadata: LockMetadata,
}

impl ValidatedWorkspace {
    fn lock_file(&self) -> &File {
        self.lock_file
            .as_ref()
            .expect("validated workspace keeps its lock pinned")
    }
}

struct LiveWorkspace {
    workspace: ValidatedWorkspace,
    process: CurrentUserProcess,
    metadata_executable: PinnedFile,
    process_executable: PinnedFile,
    control_executable: PinnedFile,
    client: DaemonClient,
}

struct LiveUnrelatedProcess {
    lock_path: PathBuf,
    lock_file: File,
    raw_lock: Vec<u8>,
    pid: u32,
    metadata_executable: PinnedFile,
    process_executable: PinnedFile,
    control_executable: PinnedFile,
    process: CurrentUserProcess,
}

struct PinnedFile {
    path: PathBuf,
    file: File,
}

impl PinnedFile {
    fn open(path: PathBuf) -> AppResult<Self> {
        let file = windows_security::open_file_for_identity(&path)?;
        Ok(Self { path, file })
    }

    fn is_same_file(&self, other: &Self) -> AppResult<bool> {
        Ok(windows_security::same_file_handles(
            &self.file,
            &other.file,
        )?)
    }

    fn is_unchanged(&self) -> AppResult<bool> {
        Ok(windows_security::same_file_handle_path(
            &self.file, &self.path,
        )?)
    }
}

enum LiveObservation {
    Exact(Box<LiveWorkspace>),
    Unrelated(LiveUnrelatedProcess),
    Gone,
}

struct Preflight {
    exact: Vec<LiveWorkspace>,
    unrelated: Vec<LiveUnrelatedProcess>,
    errors: Vec<String>,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum ControlShutdownResult {
    Shutdown,
    RefusedActive,
    NotRunning,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ControlRecord<'a> {
    Workspace {
        workspace_root: &'a str,
        workspace_id: &'a str,
        socket_path: &'a str,
        pid: u32,
    },
    Unrelated {
        lock_path: String,
        pid: u32,
        executable: String,
    },
    Shutdown {
        workspace_root: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        workspace_id: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        socket_path: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pid: Option<u32>,
        result: ControlShutdownResult,
    },
}

#[derive(Clone, Copy)]
enum ShutdownOutcome {
    Shutdown,
    RefusedActive,
}

pub fn list_workspaces(output: &mut dyn Write) -> AppResult<()> {
    let preflight = preflight_all()?;
    for workspace in &preflight.exact {
        write_record(
            output,
            &ControlRecord::Workspace {
                workspace_root: &workspace.workspace.metadata.workspace_root,
                workspace_id: &workspace.workspace.metadata.workspace_id,
                socket_path: &workspace.workspace.metadata.socket_path,
                pid: workspace.workspace.metadata.pid,
            },
        )?;
    }
    write_unrelated(output, &preflight.unrelated)?;
    finish_preflight(preflight.errors)
}

pub fn shutdown_if_idle(workspace_root: Option<&Path>, output: &mut dyn Write) -> AppResult<()> {
    match workspace_root {
        Some(workspace_root) => shutdown_target(workspace_root, output),
        None => shutdown_all(output),
    }
}

fn shutdown_target(workspace_root: &Path, output: &mut dyn Write) -> AppResult<()> {
    let canonical_root = workspace_root.canonicalize()?;
    let lock_path = paths::default_lock_path(&canonical_root)?;
    let live = match inspect_lock(&lock_path)? {
        LiveObservation::Exact(live) => *live,
        LiveObservation::Unrelated(unrelated) => {
            write_unrelated(output, std::slice::from_ref(&unrelated))?;
            return write_record(
                output,
                &ControlRecord::Shutdown {
                    workspace_root: &canonical_root.to_string_lossy(),
                    workspace_id: None,
                    socket_path: None,
                    pid: None,
                    result: ControlShutdownResult::NotRunning,
                },
            );
        }
        LiveObservation::Gone => {
            return write_record(
                output,
                &ControlRecord::Shutdown {
                    workspace_root: &canonical_root.to_string_lossy(),
                    workspace_id: None,
                    socket_path: None,
                    pid: None,
                    result: ControlShutdownResult::NotRunning,
                },
            );
        }
    };
    if Path::new(&live.workspace.metadata.workspace_root) != canonical_root {
        return Err(control_error(format!(
            "{} belongs to workspace {}, not {}",
            lock_path.display(),
            live.workspace.metadata.workspace_root,
            canonical_root.display()
        )));
    }

    let (workspace, outcome) = shutdown_live(live)?;
    write_shutdown_record(output, &workspace, outcome)?;
    if matches!(outcome, ShutdownOutcome::RefusedActive) {
        return Err(control_error(format!(
            "workspace {} refused shutdown because it is active",
            workspace.metadata.workspace_root
        )));
    }
    Ok(())
}

fn shutdown_all(output: &mut dyn Write) -> AppResult<()> {
    let Preflight {
        exact,
        unrelated,
        errors,
    } = preflight_all()?;
    write_unrelated(output, &unrelated)?;
    finish_preflight(errors)?;
    recheck_preflight(&exact, &unrelated)?;

    let mut failures = Vec::new();
    let mut outcomes = Vec::new();
    for live in exact {
        let workspace_root = live.workspace.metadata.workspace_root.clone();
        match shutdown_live(live) {
            Ok((workspace, outcome)) => {
                if matches!(outcome, ShutdownOutcome::RefusedActive) {
                    failures.push(format!(
                        "workspace {} refused shutdown because it is active",
                        workspace.metadata.workspace_root
                    ));
                }
                outcomes.push((workspace, outcome));
            }
            Err(error) => failures.push(format!("workspace {workspace_root}: {error}")),
        }
    }

    if failures.is_empty() {
        let remaining = preflight_all()?;
        if !remaining.errors.is_empty() {
            failures.extend(remaining.errors);
        }
        if !remaining.exact.is_empty() {
            failures.push(format!(
                "{} exact-sidecar daemon(s) appeared or remained after shutdown",
                remaining.exact.len()
            ));
        }
    }

    for (workspace, outcome) in &outcomes {
        write_shutdown_record(output, workspace, *outcome)?;
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(control_errors("shutdown failed", failures))
    }
}

fn preflight_all() -> AppResult<Preflight> {
    let mut exact = Vec::new();
    let mut unrelated = Vec::new();
    let (candidates, mut errors) = observed_lock_paths()?;
    for candidate in candidates {
        match inspect_lock(&candidate) {
            Ok(LiveObservation::Exact(workspace)) => exact.push(*workspace),
            Ok(LiveObservation::Unrelated(process)) => unrelated.push(process),
            Ok(LiveObservation::Gone) => {}
            Err(error) => errors.push(format!("{}: {error}", candidate.display())),
        }
    }
    exact.sort_by(|left, right| {
        left.workspace
            .metadata
            .workspace_id
            .cmp(&right.workspace.metadata.workspace_id)
    });
    unrelated.sort_by(|left, right| left.lock_path.cmp(&right.lock_path));
    Ok(Preflight {
        exact,
        unrelated,
        errors,
    })
}

fn recheck_preflight(exact: &[LiveWorkspace], unrelated: &[LiveUnrelatedProcess]) -> AppResult<()> {
    let (mut observed, errors) = observed_lock_paths()?;
    finish_preflight(errors)?;
    let mut expected: Vec<PathBuf> = exact
        .iter()
        .map(|live| live.workspace.lock_path.clone())
        .chain(unrelated.iter().map(|live| live.lock_path.clone()))
        .collect();
    observed.sort();
    expected.sort();
    if observed != expected {
        return Err(control_error(
            "daemon lock namespace changed after preflight",
        ));
    }
    for live in exact {
        if !windows_security::same_file_handle_path(
            live.workspace.lock_file(),
            &live.workspace.lock_path,
        )? || read_lock_handle(live.workspace.lock_file())? != live.workspace.raw_lock
            || !live.process.is_running()?
            || !live.metadata_executable.is_unchanged()?
            || !live.process_executable.is_unchanged()?
            || !live.control_executable.is_unchanged()?
        {
            return Err(control_error(format!(
                "daemon {} changed after preflight",
                live.workspace.metadata.workspace_id
            )));
        }
    }
    for live in unrelated {
        if !windows_security::same_file_handle_path(&live.lock_file, &live.lock_path)?
            || read_lock_handle(&live.lock_file)? != live.raw_lock
            || !live.process.is_running()?
            || !live.metadata_executable.is_unchanged()?
            || !live.process_executable.is_unchanged()?
            || !live.control_executable.is_unchanged()?
        {
            return Err(control_error(format!(
                "unrelated process {} changed after preflight",
                live.pid
            )));
        }
    }
    Ok(())
}

fn observed_lock_paths() -> AppResult<(Vec<PathBuf>, Vec<String>)> {
    let root = paths::runtime_home()?.join("platonic").join("workspaces");
    let entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok((Vec::new(), Vec::new()));
        }
        Err(error) => return Err(error.into()),
    };
    let mut paths = Vec::new();
    let mut errors = Vec::new();
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                errors.push(format!("failed to enumerate daemon lock: {error}"));
                continue;
            }
        };
        let entry_path = entry.path();
        let metadata = match fs::symlink_metadata(&entry_path) {
            Ok(metadata) => metadata,
            Err(error) => {
                errors.push(format!("{}: {error}", entry_path.display()));
                continue;
            }
        };
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            errors.push(format!(
                "workspace runtime entry is a reparse point: {}",
                entry_path.display()
            ));
            continue;
        }
        if !metadata.is_dir() {
            continue;
        }
        let lock_path = entry_path.join(LOCK_FILE_NAME);
        match fs::symlink_metadata(&lock_path) {
            Ok(metadata) if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 => {
                errors.push(format!(
                    "daemon lock is a reparse point: {}",
                    lock_path.display()
                ));
            }
            Ok(metadata) if metadata.is_file() => paths.push(lock_path),
            Ok(_) => {
                errors.push(format!(
                    "daemon lock is not a file: {}",
                    lock_path.display()
                ));
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => errors.push(format!("{}: {error}", lock_path.display())),
        }
    }
    paths.sort();
    Ok((paths, errors))
}

fn inspect_lock(lock_path: &Path) -> AppResult<LiveObservation> {
    let Some((lock_file, raw_lock, metadata)) =
        read_lock_metadata(lock_path, Instant::now() + METADATA_RETRY)?
    else {
        return Ok(LiveObservation::Gone);
    };
    let workspace_root = validate_metadata(lock_path, &lock_file, &metadata)?;
    let executable = metadata
        .executable
        .as_deref()
        .ok_or_else(|| control_error("daemon lock omits executable"))?;
    let metadata_executable_path = PathBuf::from(executable);
    if !metadata_executable_path.is_absolute() {
        return Err(control_error("daemon lock executable is not absolute"));
    }
    if !windows_security::is_local_disk_path(&metadata_executable_path)? {
        return Err(control_error(format!(
            "daemon executable is not on a local disk: {}",
            metadata_executable_path.display()
        )));
    }
    let Some(process) = CurrentUserProcess::open(metadata.pid)? else {
        return Err(control_error(format!(
            "daemon lock has stale pid {}",
            metadata.pid
        )));
    };
    let process_executable_path = process.executable()?;
    if !windows_security::is_local_disk_path(&process_executable_path)? {
        return Err(control_error(format!(
            "daemon pid {} executable is not on a local disk",
            metadata.pid
        )));
    }
    let metadata_executable = PinnedFile::open(metadata_executable_path)?;
    let process_executable = PinnedFile::open(process_executable_path)?;
    if !metadata_executable.is_same_file(&process_executable)? {
        return Err(control_error(format!(
            "daemon lock executable does not match pid {}",
            metadata.pid
        )));
    }
    let current_executable_path = std::env::current_exe()?;
    if !windows_security::is_local_disk_path(&current_executable_path)? {
        return Err(control_error(
            "installer control executable is not on a local disk",
        ));
    }
    let control_executable = PinnedFile::open(current_executable_path)?;
    if !process_executable.is_same_file(&control_executable)? {
        let current_raw = read_lock_handle(&lock_file)?;
        if current_raw != raw_lock || !process.is_running()? {
            return Err(control_error(format!(
                "unrelated daemon identity changed during validation: {}",
                lock_path.display()
            )));
        }
        return Ok(LiveObservation::Unrelated(LiveUnrelatedProcess {
            lock_path: lock_path.to_path_buf(),
            lock_file,
            raw_lock,
            pid: metadata.pid,
            metadata_executable,
            process_executable,
            control_executable,
            process,
        }));
    }
    let socket_path = PathBuf::from(&metadata.socket_path);
    if !is_local_pipe(&socket_path) {
        return Err(control_error(format!(
            "daemon lock socket is not a local named pipe: {}",
            metadata.socket_path
        )));
    }

    let mut client = DaemonClient::connect_expected_server(&socket_path, metadata.pid)?;
    let hello = client.hello(&workspace_root)?;
    if hello.workspace_id != metadata.workspace_id {
        return Err(control_error(format!(
            "daemon hello workspace mismatch: expected {}, got {}",
            metadata.workspace_id, hello.workspace_id
        )));
    }
    if !hello
        .capabilities
        .iter()
        .any(|capability| capability == CAPABILITY_DAEMON_SHUTDOWN_IF_IDLE)
    {
        return Err(control_error(format!(
            "daemon {} lacks {CAPABILITY_DAEMON_SHUTDOWN_IF_IDLE}",
            metadata.workspace_id
        )));
    }
    let current_raw = read_lock_handle(&lock_file)?;
    if current_raw != raw_lock {
        return Err(control_error(format!(
            "daemon lock changed during validation: {}",
            lock_path.display()
        )));
    }
    if !process.is_running()? {
        return Err(control_error(format!(
            "daemon pid {} exited during validation",
            metadata.pid
        )));
    }

    Ok(LiveObservation::Exact(Box::new(LiveWorkspace {
        workspace: ValidatedWorkspace {
            lock_path: lock_path.to_path_buf(),
            lock_file: Some(lock_file),
            raw_lock,
            metadata,
        },
        process,
        metadata_executable,
        process_executable,
        control_executable,
        client,
    })))
}

fn validate_metadata(
    lock_path: &Path,
    lock_file: &File,
    metadata: &LockMetadata,
) -> AppResult<PathBuf> {
    if metadata.v != LOCK_VERSION {
        return Err(control_error(format!(
            "unsupported daemon lock version {}",
            metadata.v
        )));
    }
    if metadata.pid == 0 {
        return Err(control_error("daemon lock pid is zero"));
    }
    let workspace_root = PathBuf::from(&metadata.workspace_root);
    if !workspace_root.is_absolute() {
        return Err(control_error("daemon lock workspace_root is not absolute"));
    }
    let canonical_root = workspace_root.canonicalize()?;
    if canonical_root != workspace_root {
        return Err(control_error(format!(
            "daemon lock workspace_root is not canonical: {}",
            workspace_root.display()
        )));
    }
    let expected_id = paths::workspace_id(&canonical_root)?;
    if metadata.workspace_id != expected_id {
        return Err(control_error(format!(
            "daemon lock workspace_id mismatch: expected {expected_id}, got {}",
            metadata.workspace_id
        )));
    }
    if lock_path
        .parent()
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        != Some(metadata.workspace_id.as_str())
    {
        return Err(control_error(format!(
            "daemon lock directory does not match workspace_id {}",
            metadata.workspace_id
        )));
    }
    let expected_lock = paths::default_lock_path(&canonical_root)?;
    if !windows_security::same_file_handle_path(lock_file, &expected_lock)? {
        return Err(control_error(format!(
            "daemon lock is not the workspace default: {}",
            lock_path.display()
        )));
    }
    Ok(canonical_root)
}

fn shutdown_live(live: LiveWorkspace) -> AppResult<(ValidatedWorkspace, ShutdownOutcome)> {
    let LiveWorkspace {
        mut workspace,
        process,
        metadata_executable,
        process_executable,
        control_executable,
        mut client,
    } = live;
    if read_lock_handle(workspace.lock_file())? != workspace.raw_lock
        || !process.is_running()?
        || !metadata_executable.is_unchanged()?
        || !process_executable.is_unchanged()?
        || !control_executable.is_unchanged()?
    {
        return Err(control_error(format!(
            "daemon {} changed before shutdown",
            workspace.metadata.workspace_id
        )));
    }
    let response = client.shutdown_if_idle()?;
    drop(client);
    match response.result {
        ShutdownIfIdleResultName::RefusedActive => {
            if !process.is_running()?
                || read_lock_handle(workspace.lock_file())? != workspace.raw_lock
            {
                return Err(control_error(
                    "refusing daemon changed process or lock state unexpectedly",
                ));
            }
            drop(workspace.lock_file.take());
            Ok((workspace, ShutdownOutcome::RefusedActive))
        }
        ShutdownIfIdleResultName::Shutdown => {
            if !process.wait_until(Instant::now() + PROCESS_EXIT_TIMEOUT)? {
                return Err(control_error(format!(
                    "daemon pid {} did not exit after shutdown acknowledgement",
                    workspace.metadata.pid
                )));
            }
            drop(workspace.lock_file.take());
            match fs::symlink_metadata(&workspace.lock_path) {
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    Ok((workspace, ShutdownOutcome::Shutdown))
                }
                Ok(_) => Err(control_error(format!(
                    "daemon lock remained after process exit: {}",
                    workspace.lock_path.display()
                ))),
                Err(error) => Err(error.into()),
            }
        }
    }
}

fn read_lock_metadata(
    path: &Path,
    deadline: Instant,
) -> AppResult<Option<(File, Vec<u8>, LockMetadata)>> {
    loop {
        match windows_security::open_lock_file_for_read(path) {
            Ok(file) => match read_lock_handle(&file) {
                Ok(raw) => match serde_json::from_slice::<LockMetadata>(&raw) {
                    Ok(metadata) => return Ok(Some((file, raw, metadata))),
                    Err(error) if Instant::now() < deadline => {
                        thread::sleep(METADATA_RETRY_INTERVAL);
                        let _ = error;
                    }
                    Err(error) => {
                        return Err(control_error(format!(
                            "daemon lock metadata remained malformed: {error}"
                        )));
                    }
                },
                Err(error) if Instant::now() < deadline => {
                    thread::sleep(METADATA_RETRY_INTERVAL);
                    let _ = error;
                }
                Err(error) => return Err(error),
            },
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) if Instant::now() < deadline => {
                thread::sleep(METADATA_RETRY_INTERVAL);
                let _ = error;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn read_lock_handle(file: &File) -> AppResult<Vec<u8>> {
    if file.metadata()?.len() > MAX_LOCK_BYTES {
        return Err(control_error(format!(
            "daemon lock exceeds {MAX_LOCK_BYTES} bytes"
        )));
    }
    let mut raw = Vec::new();
    let mut offset = 0;
    let mut buffer = [0; 4096];
    loop {
        let read = file.seek_read(&mut buffer, offset)?;
        if read == 0 {
            break;
        }
        raw.extend_from_slice(&buffer[..read]);
        if raw.len() as u64 > MAX_LOCK_BYTES {
            return Err(control_error(format!(
                "daemon lock exceeds {MAX_LOCK_BYTES} bytes"
            )));
        }
        offset += read as u64;
    }
    Ok(raw)
}

fn is_local_pipe(path: &Path) -> bool {
    let value = path.as_os_str().to_string_lossy();
    let lower = value.to_ascii_lowercase();
    lower
        .strip_prefix(r"\\.\pipe\")
        .is_some_and(|name| !name.is_empty() && !name.contains('\\'))
}

fn write_shutdown_record(
    output: &mut dyn Write,
    workspace: &ValidatedWorkspace,
    outcome: ShutdownOutcome,
) -> AppResult<()> {
    let result = match outcome {
        ShutdownOutcome::Shutdown => ControlShutdownResult::Shutdown,
        ShutdownOutcome::RefusedActive => ControlShutdownResult::RefusedActive,
    };
    write_record(
        output,
        &ControlRecord::Shutdown {
            workspace_root: &workspace.metadata.workspace_root,
            workspace_id: Some(&workspace.metadata.workspace_id),
            socket_path: Some(&workspace.metadata.socket_path),
            pid: Some(workspace.metadata.pid),
            result,
        },
    )
}

fn write_unrelated(output: &mut dyn Write, processes: &[LiveUnrelatedProcess]) -> AppResult<()> {
    for process in processes {
        write_record(
            output,
            &ControlRecord::Unrelated {
                lock_path: process.lock_path.to_string_lossy().into_owned(),
                pid: process.pid,
                executable: process
                    .process_executable
                    .path
                    .to_string_lossy()
                    .into_owned(),
            },
        )?;
    }
    Ok(())
}

fn write_record(output: &mut dyn Write, record: &ControlRecord<'_>) -> AppResult<()> {
    serde_json::to_writer(&mut *output, record)?;
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(())
}

fn finish_preflight(errors: Vec<String>) -> AppResult<()> {
    if errors.is_empty() {
        Ok(())
    } else {
        Err(control_errors("daemon lock preflight failed", errors))
    }
}

fn control_errors(context: &str, errors: Vec<String>) -> AppError {
    control_error(format!("{context}: {}", errors.join("; ")))
}

fn control_error(message: impl Into<String>) -> AppError {
    ClientError::DaemonControl(message.into()).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_pipe_rejects_remote_and_empty_names() {
        assert!(is_local_pipe(Path::new(r"\\.\pipe\plato-agent-test")));
        assert!(!is_local_pipe(Path::new(r"\\server\pipe\plato-agent-test")));
        assert!(!is_local_pipe(Path::new(r"\\.\pipe\")));
        assert!(!is_local_pipe(Path::new(r"C:\tmp\agent.sock")));
    }

    #[test]
    fn metadata_read_retries_a_partial_write() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join(LOCK_FILE_NAME);
        fs::write(&path, b"{").unwrap();
        let writer_path = path.clone();
        let writer = thread::spawn(move || {
            thread::sleep(Duration::from_millis(40));
            let metadata = LockMetadata {
                v: LOCK_VERSION,
                pid: 1,
                executable: Some(r"C:\platonic.exe".into()),
                workspace_root: r"C:\workspace".into(),
                workspace_id: "workspace-1".into(),
                socket_path: r"\\.\pipe\plato-agent-workspace-1".into(),
            };
            fs::write(writer_path, serde_json::to_vec(&metadata).unwrap()).unwrap();
        });

        let (_, _, metadata) = read_lock_metadata(&path, Instant::now() + Duration::from_secs(1))
            .unwrap()
            .unwrap();
        writer.join().unwrap();

        assert_eq!(metadata.workspace_id, "workspace-1");
    }

    #[test]
    fn malformed_metadata_is_never_modified() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join(LOCK_FILE_NAME);
        let original = b"not json";
        fs::write(&path, original).unwrap();

        let error =
            read_lock_metadata(&path, Instant::now() + Duration::from_millis(30)).unwrap_err();

        assert!(error.to_string().contains("remained malformed"));
        assert_eq!(fs::read(path).unwrap(), original);
    }

    #[test]
    fn pinned_file_detects_path_replacement() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("sidecar.exe");
        let moved = directory.path().join("old-sidecar.exe");
        fs::write(&path, b"old").unwrap();
        let pinned = PinnedFile::open(path.clone()).unwrap();

        fs::rename(&path, moved).unwrap();
        fs::write(&path, b"new").unwrap();

        assert!(!pinned.is_unchanged().unwrap());
    }
}
