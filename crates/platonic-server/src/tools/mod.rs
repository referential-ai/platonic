pub mod github;
pub mod web;

use crate::tool_catalog::{
    FILE_EDIT, FILE_LIST, FILE_READ, FILE_WRITE, SHELL_EXEC, THREAD_SPAWN, WEB_FETCH,
};
use crate::{AppError, AppResult};
use platonic_core::{ResultVisibility, ToolResult};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    env,
    ffi::OsString,
    fs,
    io::{self, ErrorKind, Read, Write},
    path::{Component, Path, PathBuf},
    process::{ChildStderr, ChildStdout, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
#[cfg(unix)]
use std::{os::unix::process::CommandExt, process::Child};

pub(crate) const PLATONIC_MEMORY_FILENAME: &str = "PLATONIC.md";
pub(crate) const PLATONIC_MEMORY_MAX_BYTES: usize = 8_192;
const MAX_READ_BYTES: usize = 64 * 1024;
const READ_UTF8_LOOKAHEAD_BYTES: usize = 4;
const MAX_LIST_ENTRIES: usize = 200;
const MAX_LIST_DATA_BYTES: usize = 32 * 1024;
const SHELL_OUTPUT_BYTES: usize = 32 * 1024;
const SHELL_OUTPUT_TRUNCATED_MARKER: &str = "\n... output truncated";
const SHELL_DEFAULT_TIMEOUT_SECONDS: u64 = 120;
const SHELL_MAX_TIMEOUT_SECONDS: u64 = 600;
const APPROVAL_PREVIEW_CHARS: usize = 1_000;
const DIFF_PREVIEW_CHARS: usize = 16 * 1024;
const DIFF_TRUNCATED_MARKER: &str = "... diff truncated";
const SHELL_ENV_ALLOWLIST: &[&str] = &[
    "PATH",
    "HOME",
    "USER",
    "LOGNAME",
    "SHELL",
    "TERM",
    "COLORTERM",
    "NO_COLOR",
    "LANG",
    "LC_ALL",
    "TMPDIR",
    "TEMP",
    "TMP",
    "CARGO_HOME",
    "RUSTUP_HOME",
];
#[cfg(unix)]
type ShellChild = Child;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileReadInput {
    path: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileListInput {
    path: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileContentInput {
    path: String,
    content: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ShellExecInput {
    command: String,
    timeout_seconds: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ThreadSpawnToolInput {
    pub(crate) agent_id: String,
    pub(crate) cwd: String,
    pub(crate) model: Option<String>,
    pub(crate) reasoning_effort: Option<platonic_protocol::ReasoningEffort>,
    pub(crate) approval_policy: Option<platonic_protocol::ThreadApprovalPolicy>,
    pub(crate) toolset: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) repositories: Option<Vec<platonic_protocol::ThreadRepositoryRequest>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub(crate) enum ThreadSpawnToolOutput {
    Spawned { thread_id: String },
    Rejected { code: String, reason: String },
}

impl ThreadSpawnToolOutput {
    pub(crate) fn is_rejected(&self) -> bool {
        matches!(self, Self::Rejected { .. })
    }
}

#[derive(Clone)]
pub(crate) struct ThreadSpawnToolHandler {
    execute:
        Arc<dyn Fn(ThreadSpawnToolInput, String) -> AppResult<ThreadSpawnToolOutput> + Send + Sync>,
}

impl std::fmt::Debug for ThreadSpawnToolHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ThreadSpawnToolHandler")
            .finish_non_exhaustive()
    }
}

impl ThreadSpawnToolHandler {
    pub(crate) fn new(
        execute: impl Fn(ThreadSpawnToolInput, String) -> AppResult<ThreadSpawnToolOutput>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            execute: Arc::new(execute),
        }
    }

    pub(crate) fn execute(
        &self,
        input: ThreadSpawnToolInput,
        approving_actor: String,
    ) -> AppResult<ThreadSpawnToolOutput> {
        (self.execute)(input, approving_actor)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApprovalOutcome {
    Granted,
    Denied { reason: String },
}

#[derive(Clone, Copy, Debug)]
pub struct ToolExecutionContext<'a> {
    pub workspace_root: &'a Path,
    pub provider_api_key_env: Option<&'a str>,
    pub cancel: Option<&'a AtomicBool>,
    pub(crate) thread_spawn: Option<&'a ThreadSpawnToolHandler>,
    pub(crate) approving_actor: Option<&'a str>,
}

impl<'a> ToolExecutionContext<'a> {
    pub fn new(workspace_root: &'a Path) -> Self {
        Self {
            workspace_root,
            provider_api_key_env: None,
            cancel: None,
            thread_spawn: None,
            approving_actor: None,
        }
    }
}

pub(crate) fn targets_platonic_memory(
    workspace_root: &Path,
    tool_name: &str,
    input: &Value,
) -> bool {
    matches!(tool_name, FILE_WRITE | FILE_EDIT)
        && input
            .get("path")
            .and_then(Value::as_str)
            .and_then(|path| platonic_memory_target_path(workspace_root, path))
            .is_some()
}

pub fn execute_tool(
    workspace_root: &Path,
    call_id: platonic_core::ToolCallId,
    tool_name: &str,
    input: Value,
) -> AppResult<ToolResult> {
    execute_tool_with_context(
        ToolExecutionContext::new(workspace_root),
        call_id,
        tool_name,
        input,
    )
}

pub fn execute_tool_with_context(
    context: ToolExecutionContext<'_>,
    call_id: platonic_core::ToolCallId,
    tool_name: &str,
    input: Value,
) -> AppResult<ToolResult> {
    match tool_name {
        FILE_READ => read_file(context.workspace_root, call_id, input),
        FILE_LIST => list_directory(context.workspace_root, call_id, input),
        FILE_WRITE => write_file(context.workspace_root, call_id, input, "wrote", "to"),
        FILE_EDIT => write_file(context.workspace_root, call_id, input, "edited", "at"),
        SHELL_EXEC => shell_exec(context, call_id, input),
        WEB_FETCH => web::fetch(call_id, input, context.cancel),
        THREAD_SPAWN => spawn_thread(context, call_id, input),
        _ => Err(AppError::Tool(format!("unknown tool: {tool_name}"))),
    }
}

fn spawn_thread(
    context: ToolExecutionContext<'_>,
    call_id: platonic_core::ToolCallId,
    input: Value,
) -> AppResult<ToolResult> {
    let input: ThreadSpawnToolInput = serde_json::from_value(input)?;
    let handler = context
        .thread_spawn
        .ok_or_else(|| AppError::Tool("thread.spawn requires a coordinator thread".into()))?;
    let actor = context
        .approving_actor
        .ok_or_else(|| AppError::Tool("thread.spawn requires an approving actor".into()))?;
    let output = handler.execute(input, actor.into())?;
    let rejected = output.is_rejected();
    Ok(ToolResult {
        call_id,
        summary: if rejected {
            "thread spawn rejected".into()
        } else {
            "spawned worker thread".into()
        },
        data: serde_json::to_value(output)?,
        artifacts: vec![],
        visibility: ResultVisibility::Both,
    })
}

pub fn ask_for_approval(
    tool_name: &str,
    input: &Value,
    approval_preview: Option<&str>,
) -> AppResult<ApprovalOutcome> {
    eprint!("{}", approval_prompt(tool_name, input, approval_preview));
    io::stderr().flush()?;

    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    let normalized = line.trim().to_ascii_lowercase();
    if normalized == "y" || normalized == "yes" {
        Ok(ApprovalOutcome::Granted)
    } else {
        Ok(ApprovalOutcome::Denied {
            reason: "approval denied by stdin".into(),
        })
    }
}

pub fn approval_diff_preview(
    workspace_root: &Path,
    tool_name: &str,
    input: &Value,
) -> Option<String> {
    if tool_name != FILE_EDIT {
        return None;
    }

    let input: FileContentInput = serde_json::from_value(input.clone()).ok()?;
    let path = resolve_write_path(workspace_root, &input.path).ok()?;
    let current = match fs::read_to_string(path) {
        Ok(content) => content,
        Err(error) if error.kind() == ErrorKind::NotFound => String::new(),
        Err(_) => return None,
    };

    Some(unified_diff(
        &input.path,
        &current,
        &input.content,
        DIFF_PREVIEW_CHARS,
    ))
}

pub fn approval_command_preview(
    workspace_root: &Path,
    tool_name: &str,
    input: &Value,
    provider_api_key_env: Option<&str>,
) -> AppResult<Option<String>> {
    if tool_name == WEB_FETCH {
        return web::approval_preview(input)
            .map(Some)
            .map_err(|error| AppError::Tool(error.to_string()));
    }
    if tool_name != SHELL_EXEC {
        return Ok(None);
    }

    let input: ShellExecInput = match serde_json::from_value(input.clone()) {
        Ok(input) => input,
        Err(_) => return Ok(None),
    };
    let timeout_seconds = normalize_timeout_seconds(input.timeout_seconds);
    let cwd = workspace_root
        .canonicalize()
        .unwrap_or_else(|_| workspace_root.to_path_buf());
    let provider = provider_api_key_env.unwrap_or("configured provider key");
    Ok(Some(format!(
        "command: {}\ncwd: {}\ntimeout: {}s\neffect: ExternalSideEffect\nenv: scrubbed allowlist; credential-like names and {provider} removed",
        input.command,
        cwd.display(),
        timeout_seconds
    )))
}

fn read_file(
    workspace_root: &Path,
    call_id: platonic_core::ToolCallId,
    input: Value,
) -> AppResult<ToolResult> {
    let input: FileReadInput = serde_json::from_value(input)?;
    let path = resolve_existing_path(workspace_root, &input.path)?;
    let mut file = fs::File::open(&path)?;
    let bytes = file.metadata()?.len();
    let content = read_utf8_prefix(&mut file, bytes)?;
    let truncated = bytes > MAX_READ_BYTES as u64;
    let visible = truncate_utf8(&content, MAX_READ_BYTES);

    Ok(ToolResult {
        call_id,
        summary: format!("read {bytes} bytes from {}", input.path),
        data: json!({
            "path": input.path,
            "content": visible,
            "truncated": truncated,
            "bytes": bytes
        }),
        artifacts: vec![],
        visibility: ResultVisibility::Both,
    })
}

fn list_directory(
    workspace_root: &Path,
    call_id: platonic_core::ToolCallId,
    input: Value,
) -> AppResult<ToolResult> {
    let input: FileListInput = serde_json::from_value(input)?;
    let path = resolve_existing_path(workspace_root, &input.path)?;
    if !path.metadata()?.is_dir() {
        return Err(AppError::Tool(format!("not a directory: {}", input.path)));
    }

    let mut entries = Vec::with_capacity(MAX_LIST_ENTRIES);
    let mut entry_count = 0usize;
    for entry in fs::read_dir(&path)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        entry_count += 1;
        retain_list_candidate(
            &mut entries,
            ListEntry {
                name: entry.file_name().to_string_lossy().into_owned(),
                kind: file_kind(&file_type),
            },
        );
    }
    let mut returned = Vec::new();
    let mut data_bytes = 0usize;
    let mut truncated = false;
    for entry in entries {
        let entry_bytes = estimated_list_entry_bytes(&entry);
        if returned.len() >= MAX_LIST_ENTRIES
            || data_bytes.saturating_add(entry_bytes) > MAX_LIST_DATA_BYTES
        {
            truncated = true;
            break;
        }
        data_bytes += entry_bytes;
        returned.push(entry);
    }
    truncated |= returned.len() < entry_count;
    let returned_count = returned.len();

    Ok(ToolResult {
        call_id,
        summary: format!(
            "listed {} of {} entries in {}",
            returned_count, entry_count, input.path
        ),
        data: json!({
            "path": input.path,
            "entries": returned,
            "truncated": truncated,
            "entry_count": entry_count,
            "returned_count": returned_count
        }),
        artifacts: vec![],
        visibility: ResultVisibility::Both,
    })
}

fn read_utf8_prefix(reader: &mut impl Read, source_bytes: u64) -> io::Result<String> {
    let buffer_bytes = MAX_READ_BYTES + READ_UTF8_LOOKAHEAD_BYTES;
    let mut bytes = Vec::with_capacity(buffer_bytes);
    reader.take(buffer_bytes as u64).read_to_end(&mut bytes)?;

    let valid_bytes = match std::str::from_utf8(&bytes) {
        Ok(_) => bytes.len(),
        Err(error)
            if source_bytes > buffer_bytes as u64
                && error.error_len().is_none()
                && error.valid_up_to() >= MAX_READ_BYTES =>
        {
            error.valid_up_to()
        }
        Err(error) => return Err(io::Error::new(ErrorKind::InvalidData, error)),
    };
    bytes.truncate(valid_bytes);
    String::from_utf8(bytes).map_err(|error| io::Error::new(ErrorKind::InvalidData, error))
}

fn retain_list_candidate(entries: &mut Vec<ListEntry>, candidate: ListEntry) {
    let index = entries.partition_point(|entry| entry.name <= candidate.name);
    if index >= MAX_LIST_ENTRIES {
        return;
    }
    if entries.len() == MAX_LIST_ENTRIES {
        entries.pop();
    }
    entries.insert(index, candidate);
}

fn write_file(
    workspace_root: &Path,
    call_id: platonic_core::ToolCallId,
    input: Value,
    summary_verb: &str,
    summary_preposition: &str,
) -> AppResult<ToolResult> {
    let input: FileContentInput = serde_json::from_value(input)?;
    if let Some(path) = platonic_memory_target_path(workspace_root, &input.path) {
        validate_platonic_memory_content(&path, input.content.as_bytes())?;
        validate_platonic_memory_target(&path)?;
    }
    let path = resolve_write_path(workspace_root, &input.path)?;
    fs::write(&path, &input.content)?;

    Ok(ToolResult {
        call_id,
        summary: format!(
            "{summary_verb} {} bytes {summary_preposition} {}",
            input.content.len(),
            input.path
        ),
        data: json!({
            "path": input.path,
            "bytes": input.content.len()
        }),
        artifacts: vec![],
        visibility: ResultVisibility::Both,
    })
}

fn platonic_memory_target_path(workspace_root: &Path, raw_path: &str) -> Option<PathBuf> {
    let mut normalized = workspace_root.to_path_buf();
    for component in Path::new(raw_path).components() {
        match component {
            Component::CurDir => {}
            Component::Normal(component) => normalized.push(component),
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    (normalized == workspace_root.join(PLATONIC_MEMORY_FILENAME)).then_some(normalized)
}

fn validate_platonic_memory_content(path: &Path, content: &[u8]) -> AppResult<()> {
    if content.len() > PLATONIC_MEMORY_MAX_BYTES {
        return Err(AppError::PlatonicMemoryTooLarge {
            path: path.to_path_buf(),
            max_bytes: PLATONIC_MEMORY_MAX_BYTES,
        });
    }
    std::str::from_utf8(content)
        .map(|_| ())
        .map_err(|_| AppError::PlatonicMemoryInvalidUtf8(path.to_path_buf()))
}

fn validate_platonic_memory_target(path: &Path) -> AppResult<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(()),
        Ok(_) => Err(AppError::PlatonicMemoryNotRegular(path.to_path_buf())),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn shell_exec(
    context: ToolExecutionContext<'_>,
    call_id: platonic_core::ToolCallId,
    input: Value,
) -> AppResult<ToolResult> {
    let input: ShellExecInput = serde_json::from_value(input)?;
    if input.command.trim().is_empty() {
        return Err(AppError::Tool("shell.exec command is empty".into()));
    }
    let timeout_seconds = normalize_timeout_seconds(input.timeout_seconds);
    let cwd = context.workspace_root.canonicalize()?;
    let env = shell_child_env(context.provider_api_key_env);
    let started = Instant::now();
    let mut child = spawn_shell(&input.command, &cwd, env)?;

    let stdout = take_shell_stdout(&mut child)
        .ok_or_else(|| AppError::Tool("shell.exec stdout pipe unavailable".into()))?;
    let stderr = take_shell_stderr(&mut child)
        .ok_or_else(|| AppError::Tool("shell.exec stderr pipe unavailable".into()))?;
    let stdout_reader = thread::spawn(move || read_capped_output(stdout, SHELL_OUTPUT_BYTES));
    let stderr_reader = thread::spawn(move || read_capped_output(stderr, SHELL_OUTPUT_BYTES));
    let deadline = Instant::now() + Duration::from_secs(timeout_seconds);
    let mut timed_out = false;
    let mut canceled = false;
    let status = loop {
        if let Some(status) = try_wait_shell(&mut child)? {
            break Some(status);
        }
        if context
            .cancel
            .is_some_and(|cancel| cancel.load(Ordering::SeqCst))
        {
            canceled = true;
            break terminate_shell(&mut child)?;
        }
        if Instant::now() >= deadline {
            timed_out = true;
            break terminate_shell(&mut child)?;
        }
        thread::sleep(Duration::from_millis(20));
    };
    let stdout = join_output_reader(stdout_reader)?;
    let stderr = join_output_reader(stderr_reader)?;
    let duration_ms = started.elapsed().as_millis() as u64;

    if timed_out {
        return Err(AppError::Tool(format!(
            "shell.exec timed out after {timeout_seconds}s"
        )));
    }
    if canceled {
        return Err(AppError::Tool("shell.exec canceled".into()));
    }

    let status = status.expect("completed shell has an exit status");
    let exit_code = status.code();
    let exit_label = exit_code
        .map(|code| code.to_string())
        .unwrap_or_else(|| "signal".into());
    Ok(ToolResult {
        call_id,
        summary: format!("shell.exec exited {exit_label} in {duration_ms}ms"),
        data: json!({
            "command": input.command,
            "cwd": cwd.to_string_lossy(),
            "exit_code": exit_code,
            "duration_ms": duration_ms,
            "stdout": stdout.text,
            "stderr": stderr.text,
            "stdout_truncated": stdout.truncated,
            "stderr_truncated": stderr.truncated
        }),
        artifacts: vec![],
        visibility: ResultVisibility::Both,
    })
}

#[cfg(unix)]
fn spawn_shell(command: &str, cwd: &Path, env: Vec<(String, String)>) -> io::Result<ShellChild> {
    Command::new("/bin/sh")
        .arg("-c")
        .arg(command)
        .current_dir(cwd)
        .env_clear()
        .envs(env)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0)
        .spawn()
}

#[cfg(unix)]
fn take_shell_stdout(child: &mut ShellChild) -> Option<ChildStdout> {
    child.stdout.take()
}

#[cfg(unix)]
fn take_shell_stderr(child: &mut ShellChild) -> Option<ChildStderr> {
    child.stderr.take()
}

#[cfg(unix)]
fn try_wait_shell(child: &mut ShellChild) -> io::Result<Option<std::process::ExitStatus>> {
    child.try_wait()
}

#[cfg(unix)]
fn terminate_shell(child: &mut ShellChild) -> io::Result<Option<std::process::ExitStatus>> {
    if let Some(pid) = rustix::process::Pid::from_raw(child.id() as i32) {
        let _ = rustix::process::kill_process_group(pid, rustix::process::Signal::KILL);
    }
    let _ = child.kill();
    child.wait().map(Some)
}

fn normalize_timeout_seconds(timeout_seconds: Option<u64>) -> u64 {
    timeout_seconds
        .unwrap_or(SHELL_DEFAULT_TIMEOUT_SECONDS)
        .clamp(1, SHELL_MAX_TIMEOUT_SECONDS)
}

fn shell_child_env(provider_api_key_env: Option<&str>) -> Vec<(String, String)> {
    shell_child_env_from(env::vars(), provider_api_key_env)
}

pub(crate) fn supervised_run_child_env(
    provider_api_key_env: &str,
) -> AppResult<Vec<(OsString, OsString)>> {
    supervised_run_child_env_from(env::vars_os(), provider_api_key_env)
}

fn supervised_run_child_env_from(
    vars: impl IntoIterator<Item = (OsString, OsString)>,
    provider_api_key_env: &str,
) -> AppResult<Vec<(OsString, OsString)>> {
    let mut child_env = Vec::new();
    let mut provider_api_key = None;
    for (name, value) in vars {
        let Some(name_text) = name.to_str() else {
            continue;
        };
        if shell_env_names_equal(provider_api_key_env, name_text) {
            provider_api_key = Some(value);
        } else if shell_child_env_name_is_allowed(name_text, Some(provider_api_key_env)) {
            child_env.push((name, value));
        }
    }
    let provider_api_key =
        provider_api_key.ok_or_else(|| AppError::MissingApiKey(provider_api_key_env.into()))?;
    child_env.push((provider_api_key_env.into(), provider_api_key));
    Ok(child_env)
}

fn shell_child_env_from(
    vars: impl IntoIterator<Item = (String, String)>,
    provider_api_key_env: Option<&str>,
) -> Vec<(String, String)> {
    vars.into_iter()
        .filter(|(name, _)| shell_child_env_name_is_allowed(name, provider_api_key_env))
        .collect()
}

fn shell_child_env_name_is_allowed(name: &str, provider_api_key_env: Option<&str>) -> bool {
    shell_env_name_is_allowlisted(name)
        && !is_credential_env_name(name)
        && provider_api_key_env.is_none_or(|provider| !shell_env_names_equal(provider, name))
}

fn shell_env_name_is_allowlisted(name: &str) -> bool {
    #[cfg(unix)]
    {
        SHELL_ENV_ALLOWLIST.contains(&name)
    }
}

fn shell_env_names_equal(left: &str, right: &str) -> bool {
    #[cfg(unix)]
    {
        left == right
    }
}

fn is_credential_env_name(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    ["KEY", "TOKEN", "SECRET", "PASSWORD", "CREDENTIAL", "AUTH"]
        .iter()
        .any(|needle| upper.contains(needle))
}

#[derive(Debug, Eq, PartialEq)]
struct CappedOutput {
    text: String,
    truncated: bool,
}

fn read_capped_output(mut reader: impl Read, max_bytes: usize) -> io::Result<CappedOutput> {
    let mut bytes = Vec::new();
    let mut truncated = false;
    let mut buffer = [0u8; 8192];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        let remaining = max_bytes.saturating_sub(bytes.len());
        if remaining == 0 {
            truncated = true;
            continue;
        }
        let take = remaining.min(read);
        bytes.extend_from_slice(&buffer[..take]);
        if take < read {
            truncated = true;
        }
    }

    let mut text = String::from_utf8_lossy(&bytes).into_owned();
    if truncated {
        text.push_str(SHELL_OUTPUT_TRUNCATED_MARKER);
    }
    Ok(CappedOutput { text, truncated })
}

fn join_output_reader(
    reader: thread::JoinHandle<io::Result<CappedOutput>>,
) -> AppResult<CappedOutput> {
    reader
        .join()
        .map_err(|_| AppError::Tool("shell.exec output reader panicked".into()))?
        .map_err(AppError::from)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct ListEntry {
    name: String,
    kind: &'static str,
}

fn file_kind(file_type: &fs::FileType) -> &'static str {
    if file_type.is_symlink() {
        "symlink"
    } else if file_type.is_dir() {
        "directory"
    } else if file_type.is_file() {
        "file"
    } else {
        "other"
    }
}

fn estimated_list_entry_bytes(entry: &ListEntry) -> usize {
    entry.name.len() + entry.kind.len() + 32
}

fn resolve_existing_path(workspace_root: &Path, raw_path: &str) -> AppResult<PathBuf> {
    let raw = Path::new(raw_path);
    if path_escapes(raw) {
        return Err(AppError::PathEscapesWorkspace(raw.into()));
    }

    let root = workspace_root.canonicalize()?;
    let candidate = root.join(raw).canonicalize()?;
    if !candidate.starts_with(&root) {
        return Err(AppError::PathEscapesWorkspace(candidate));
    }
    Ok(candidate)
}

fn resolve_write_path(workspace_root: &Path, raw_path: &str) -> AppResult<PathBuf> {
    let raw = Path::new(raw_path);
    if path_escapes(raw) {
        return Err(AppError::PathEscapesWorkspace(raw.into()));
    }

    let root = workspace_root.canonicalize()?;
    let candidate = root.join(raw);
    if let Ok(metadata) = fs::symlink_metadata(&candidate) {
        if metadata.file_type().is_symlink() {
            return Err(AppError::PathEscapesWorkspace(candidate));
        }
        let canonical = candidate.canonicalize()?;
        if !canonical.starts_with(&root) {
            return Err(AppError::PathEscapesWorkspace(canonical));
        }
    }
    let parent = candidate
        .parent()
        .ok_or_else(|| AppError::PathEscapesWorkspace(candidate.clone()))?
        .canonicalize()?;
    if !parent.starts_with(&root) {
        return Err(AppError::PathEscapesWorkspace(parent));
    }
    Ok(candidate)
}

fn path_escapes(path: &Path) -> bool {
    path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::Prefix(_)))
}

fn truncate_utf8(content: &str, max_bytes: usize) -> &str {
    if content.len() <= max_bytes {
        return content;
    }

    let boundary = content
        .char_indices()
        .map(|(index, _)| index)
        .take_while(|index| *index <= max_bytes)
        .last()
        .unwrap_or(0);
    &content[..boundary]
}

pub fn approval_input_preview(input: &Value) -> String {
    let input = input.to_string();
    if input.chars().count() <= APPROVAL_PREVIEW_CHARS {
        return input;
    }

    let truncated = input
        .chars()
        .take(APPROVAL_PREVIEW_CHARS)
        .collect::<String>();
    format!("{truncated}...(truncated)")
}

fn approval_prompt(tool_name: &str, input: &Value, approval_preview: Option<&str>) -> String {
    if let Some(approval_preview) = approval_preview {
        return format!("Approve {tool_name}?\n{approval_preview}\n[y/N] ");
    }

    let preview = approval_input_preview(input);
    format!("Approve {tool_name} {preview}? [y/N] ")
}

fn unified_diff(path: &str, current: &str, proposed: &str, max_chars: usize) -> String {
    if current == proposed {
        return String::new();
    }

    let current_lines = diff_lines(current);
    let proposed_lines = diff_lines(proposed);
    let prefix = common_prefix(&current_lines, &proposed_lines);
    let suffix = common_suffix(&current_lines[prefix..], &proposed_lines[prefix..]);
    let context = 3usize;
    let current_changed_end = current_lines.len() - suffix;
    let proposed_changed_end = proposed_lines.len() - suffix;
    let current_start = prefix.saturating_sub(context);
    let proposed_start = prefix.saturating_sub(context);
    let current_end = current_lines.len().min(current_changed_end + context);
    let proposed_end = proposed_lines.len().min(proposed_changed_end + context);
    let current_count = current_end - current_start;
    let proposed_count = proposed_end - proposed_start;

    let mut diff = DiffPreview::new(max_chars);
    diff.push(&format!("--- a/{path}\n"));
    diff.push(&format!("+++ b/{path}\n"));
    diff.push(&format!(
        "@@ -{},{} +{},{} @@\n",
        hunk_start(current_start, current_count),
        current_count,
        hunk_start(proposed_start, proposed_count),
        proposed_count
    ));

    for line in &current_lines[current_start..prefix] {
        diff.push_line(' ', line);
    }
    push_changed_lines(
        &mut diff,
        &current_lines[prefix..current_changed_end],
        &proposed_lines[prefix..proposed_changed_end],
    );
    for line in &current_lines[current_changed_end..current_end] {
        diff.push_line(' ', line);
    }

    diff.finish()
}

fn diff_lines(content: &str) -> Vec<&str> {
    if content.is_empty() {
        Vec::new()
    } else {
        content.split_inclusive('\n').collect()
    }
}

fn common_prefix(left: &[&str], right: &[&str]) -> usize {
    left.iter()
        .zip(right.iter())
        .take_while(|(left, right)| left == right)
        .count()
}

fn common_suffix(left: &[&str], right: &[&str]) -> usize {
    let max = left.len().min(right.len());
    let mut count = 0usize;
    while count < max && left[left.len() - count - 1] == right[right.len() - count - 1] {
        count += 1;
    }
    count
}

fn push_changed_lines(diff: &mut DiffPreview, current: &[&str], proposed: &[&str]) {
    let mut current_index = 0usize;
    let mut proposed_index = 0usize;

    while current_index < current.len() || proposed_index < proposed.len() {
        match (current.get(current_index), proposed.get(proposed_index)) {
            (Some(current_line), Some(proposed_line)) if current_line == proposed_line => {
                diff.push_line(' ', current_line);
                current_index += 1;
                proposed_index += 1;
            }
            (Some(current_line), Some(proposed_line)) => {
                diff.push_line('-', current_line);
                diff.push_line('+', proposed_line);
                current_index += 1;
                proposed_index += 1;
            }
            (Some(current_line), None) => {
                diff.push_line('-', current_line);
                current_index += 1;
            }
            (None, Some(proposed_line)) => {
                diff.push_line('+', proposed_line);
                proposed_index += 1;
            }
            (None, None) => break,
        }
    }
}

fn hunk_start(start: usize, count: usize) -> usize {
    if count == 0 { start } else { start + 1 }
}

struct DiffPreview {
    value: String,
    max_chars: usize,
    chars: usize,
    truncated: bool,
}

impl DiffPreview {
    fn new(max_chars: usize) -> Self {
        Self {
            value: String::new(),
            max_chars,
            chars: 0,
            truncated: false,
        }
    }

    fn push_line(&mut self, prefix: char, line: &str) {
        self.push(&prefix.to_string());
        self.push(line);
        if !line.ends_with('\n') {
            self.push("\n");
        }
    }

    fn push(&mut self, content: &str) {
        if self.truncated {
            return;
        }

        let remaining = self.max_chars.saturating_sub(self.chars);
        let content_chars = content.chars().count();
        if content_chars <= remaining {
            self.value.push_str(content);
            self.chars += content_chars;
            return;
        }

        self.value.extend(content.chars().take(remaining));
        self.chars = self.max_chars;
        self.mark_truncated();
    }

    fn mark_truncated(&mut self) {
        if self.truncated {
            return;
        }
        if !self.value.ends_with('\n') {
            self.value.push('\n');
        }
        self.value.push_str(DIFF_TRUNCATED_MARKER);
        self.value.push('\n');
        self.truncated = true;
    }

    fn finish(self) -> String {
        self.value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use platonic_core::ToolCallId;
    #[cfg(unix)]
    use std::os::unix::ffi::OsStringExt;
    use std::sync::Mutex;

    #[test]
    fn thread_spawn_executes_only_with_host_handler_and_approving_actor() {
        let root = tempfile::tempdir().unwrap();
        let observed = Arc::new(Mutex::new(None));
        let captured = Arc::clone(&observed);
        let handler = ThreadSpawnToolHandler::new(move |input, actor| {
            *captured.lock().unwrap() = Some((input, actor));
            Ok(ThreadSpawnToolOutput::Spawned {
                thread_id: "thread_worker".into(),
            })
        });
        let result = execute_tool_with_context(
            ToolExecutionContext {
                workspace_root: root.path(),
                provider_api_key_env: None,
                cancel: None,
                thread_spawn: Some(&handler),
                approving_actor: Some("reviewer"),
            },
            ToolCallId::new("call_spawn").unwrap(),
            THREAD_SPAWN,
            json!({"agent_id": "worker", "cwd": root.path()}),
        )
        .unwrap();

        assert_eq!(
            result.data,
            json!({"status": "spawned", "thread_id": "thread_worker"})
        );
        let (input, actor) = observed.lock().unwrap().clone().unwrap();
        assert_eq!(input.agent_id, "worker");
        assert_eq!(input.cwd, root.path().to_string_lossy());
        assert_eq!(actor, "reviewer");

        let error = execute_tool_with_context(
            ToolExecutionContext {
                workspace_root: root.path(),
                provider_api_key_env: None,
                cancel: None,
                thread_spawn: Some(&handler),
                approving_actor: None,
            },
            ToolCallId::new("call_without_actor").unwrap(),
            THREAD_SPAWN,
            json!({"agent_id": "worker", "cwd": root.path()}),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            AppError::Tool(message) if message.contains("approving actor")
        ));
    }

    struct InstrumentedReader {
        bytes: Vec<u8>,
        position: usize,
    }

    impl Read for InstrumentedReader {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            let remaining = &self.bytes[self.position..];
            let count = remaining.len().min(buffer.len());
            buffer[..count].copy_from_slice(&remaining[..count]);
            self.position += count;
            Ok(count)
        }
    }

    #[test]
    fn read_file_rejects_paths_outside_workspace() {
        let dir = tempfile::tempdir().unwrap();
        let err = execute_tool(
            dir.path(),
            ToolCallId::new("call_1").unwrap(),
            "file.read",
            json!({"path": "../outside.txt"}),
        )
        .unwrap_err();

        assert!(matches!(err, AppError::PathEscapesWorkspace(_)));
    }

    #[test]
    fn write_file_requires_parent_inside_workspace() {
        let dir = tempfile::tempdir().unwrap();
        let result = execute_tool(
            dir.path(),
            ToolCallId::new("call_1").unwrap(),
            "file.write",
            json!({"path": "note.txt", "content": "hello"}),
        )
        .unwrap();

        assert_eq!(result.summary, "wrote 5 bytes to note.txt");
        assert_eq!(
            fs::read_to_string(dir.path().join("note.txt")).unwrap(),
            "hello"
        );
    }

    #[test]
    fn edit_file_writes_full_proposed_content() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("note.txt"), "old").unwrap();

        let result = execute_tool(
            dir.path(),
            ToolCallId::new("call_1").unwrap(),
            "file.edit",
            json!({"path": "note.txt", "content": "new"}),
        )
        .unwrap();

        assert_eq!(result.summary, "edited 3 bytes at note.txt");
        assert_eq!(
            fs::read_to_string(dir.path().join("note.txt")).unwrap(),
            "new"
        );
    }

    #[test]
    fn edit_file_rejects_paths_outside_workspace() {
        let dir = tempfile::tempdir().unwrap();
        let err = execute_tool(
            dir.path(),
            ToolCallId::new("call_1").unwrap(),
            "file.edit",
            json!({"path": "../outside.txt", "content": "hello"}),
        )
        .unwrap_err();

        assert!(matches!(err, AppError::PathEscapesWorkspace(_)));
    }

    #[test]
    fn edit_file_rejects_unknown_input_fields() {
        let dir = tempfile::tempdir().unwrap();
        let err = execute_tool(
            dir.path(),
            ToolCallId::new("call_1").unwrap(),
            "file.edit",
            json!({"path": "note.txt", "content": "hello", "anchor": "old"}),
        )
        .unwrap_err();

        assert!(matches!(err, AppError::Json(_)));
    }

    #[test]
    fn platonic_memory_target_recognition_normalizes_absent_root_aliases() {
        let workspace = tempfile::tempdir().unwrap();
        assert!(!workspace.path().join(PLATONIC_MEMORY_FILENAME).exists());

        for tool in [FILE_WRITE, FILE_EDIT] {
            for path in [
                "PLATONIC.md",
                "./PLATONIC.md",
                "././PLATONIC.md",
                ".//PLATONIC.md",
            ] {
                assert!(
                    targets_platonic_memory(
                        workspace.path(),
                        tool,
                        &json!({"path": path, "content": "hello"})
                    ),
                    "{tool} {path} was not recognized"
                );
            }
        }
    }

    #[test]
    fn platonic_memory_target_recognition_is_exact_and_workspace_relative() {
        let workspace = tempfile::tempdir().unwrap();
        let absolute = workspace
            .path()
            .join(PLATONIC_MEMORY_FILENAME)
            .to_string_lossy()
            .into_owned();

        for path in [
            "PLATO.md",
            "platonic.md",
            "PLATONIC.md.bak",
            "nested/PLATONIC.md",
            "../PLATONIC.md",
            &absolute,
        ] {
            assert!(!targets_platonic_memory(
                workspace.path(),
                FILE_WRITE,
                &json!({"path": path, "content": "hello"})
            ));
        }
        assert!(!targets_platonic_memory(
            workspace.path(),
            FILE_READ,
            &json!({"path": "PLATONIC.md"})
        ));
        assert!(!targets_platonic_memory(
            workspace.path(),
            FILE_WRITE,
            &json!({"content": "hello"})
        ));
    }

    #[test]
    fn platonic_memory_write_and_edit_accept_exact_multibyte_byte_cap() {
        let content = "é".repeat(PLATONIC_MEMORY_MAX_BYTES / "é".len());
        assert_eq!(content.len(), PLATONIC_MEMORY_MAX_BYTES);
        assert!(content.chars().count() < content.len());

        for (tool, requested_path) in [(FILE_WRITE, "PLATONIC.md"), (FILE_EDIT, "./PLATONIC.md")] {
            let workspace = tempfile::tempdir().unwrap();
            let path = workspace.path().join(PLATONIC_MEMORY_FILENAME);
            if tool == FILE_EDIT {
                fs::write(&path, "prior").unwrap();
            }

            execute_tool(
                workspace.path(),
                ToolCallId::new("call_1").unwrap(),
                tool,
                json!({"path": requested_path, "content": content}),
            )
            .unwrap();

            assert_eq!(fs::read(&path).unwrap(), content.as_bytes());
        }
    }

    #[test]
    fn platonic_memory_cap_plus_one_leaves_prior_and_absent_targets_unchanged() {
        let content = "a".repeat(PLATONIC_MEMORY_MAX_BYTES + 1);

        for (tool, requested_path) in [(FILE_WRITE, "PLATONIC.md"), (FILE_EDIT, "./PLATONIC.md")] {
            for prior in [None, Some(b"prior".as_slice())] {
                let workspace = tempfile::tempdir().unwrap();
                let path = workspace.path().join(PLATONIC_MEMORY_FILENAME);
                if let Some(prior) = prior {
                    fs::write(&path, prior).unwrap();
                }

                let error = execute_tool(
                    workspace.path(),
                    ToolCallId::new("call_1").unwrap(),
                    tool,
                    json!({"path": requested_path, "content": content}),
                )
                .unwrap_err();

                assert!(matches!(
                    error,
                    AppError::PlatonicMemoryTooLarge {
                        path: error_path,
                        max_bytes: PLATONIC_MEMORY_MAX_BYTES
                    } if error_path == path
                ));
                match prior {
                    Some(prior) => assert_eq!(fs::read(&path).unwrap(), prior),
                    None => assert!(!path.exists()),
                }
            }
        }
    }

    #[test]
    fn platonic_memory_multibyte_cap_plus_one_is_measured_in_bytes() {
        let workspace = tempfile::tempdir().unwrap();
        let path = workspace.path().join(PLATONIC_MEMORY_FILENAME);
        fs::write(&path, "prior").unwrap();
        let mut content = "é".repeat(PLATONIC_MEMORY_MAX_BYTES / "é".len());
        content.push('x');
        assert_eq!(content.len(), PLATONIC_MEMORY_MAX_BYTES + 1);
        assert!(content.chars().count() < content.len());

        let error = execute_tool(
            workspace.path(),
            ToolCallId::new("call_1").unwrap(),
            FILE_EDIT,
            json!({"path": "PLATONIC.md", "content": content}),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            AppError::PlatonicMemoryTooLarge {
                path: error_path,
                max_bytes: PLATONIC_MEMORY_MAX_BYTES
            } if error_path == path
        ));
        assert_eq!(fs::read_to_string(path).unwrap(), "prior");
    }

    #[test]
    fn platonic_memory_invalid_utf8_validation_is_typed_and_non_mutating() {
        let workspace = tempfile::tempdir().unwrap();
        let path = workspace.path().join(PLATONIC_MEMORY_FILENAME);
        fs::write(&path, "prior").unwrap();

        let error = validate_platonic_memory_content(&path, &[0xff]).unwrap_err();

        assert!(matches!(
            error,
            AppError::PlatonicMemoryInvalidUtf8(error_path) if error_path == path
        ));
        assert_eq!(fs::read_to_string(path).unwrap(), "prior");
    }

    #[test]
    fn platonic_memory_cap_does_not_apply_to_other_paths() {
        let workspace = tempfile::tempdir().unwrap();
        let content = "a".repeat(PLATONIC_MEMORY_MAX_BYTES + 1);

        execute_tool(
            workspace.path(),
            ToolCallId::new("call_1").unwrap(),
            FILE_WRITE,
            json!({"path": "PLATO.md", "content": content}),
        )
        .unwrap();

        assert_eq!(
            fs::read(workspace.path().join("PLATO.md")).unwrap().len(),
            PLATONIC_MEMORY_MAX_BYTES + 1
        );
    }

    #[test]
    fn platonic_memory_write_and_edit_reject_directory_target_without_mutation() {
        for tool in [FILE_WRITE, FILE_EDIT] {
            let workspace = tempfile::tempdir().unwrap();
            let path = workspace.path().join(PLATONIC_MEMORY_FILENAME);
            fs::create_dir(&path).unwrap();

            let error = execute_tool(
                workspace.path(),
                ToolCallId::new("call_1").unwrap(),
                tool,
                json!({"path": "./PLATONIC.md", "content": "hello"}),
            )
            .unwrap_err();

            assert!(matches!(
                error,
                AppError::PlatonicMemoryNotRegular(error_path) if error_path == path
            ));
            assert!(path.is_dir());
        }
    }

    #[cfg(unix)]
    #[test]
    fn platonic_memory_write_and_edit_reject_symlink_target_without_mutation() {
        for tool in [FILE_WRITE, FILE_EDIT] {
            let workspace = tempfile::tempdir().unwrap();
            let path = workspace.path().join(PLATONIC_MEMORY_FILENAME);
            let outside = tempfile::NamedTempFile::new().unwrap();
            fs::write(outside.path(), "outside").unwrap();
            std::os::unix::fs::symlink(outside.path(), &path).unwrap();

            let error = execute_tool(
                workspace.path(),
                ToolCallId::new("call_1").unwrap(),
                tool,
                json!({"path": "PLATONIC.md", "content": "hello"}),
            )
            .unwrap_err();

            assert!(matches!(
                error,
                AppError::PlatonicMemoryNotRegular(error_path) if error_path == path
            ));
            assert_eq!(fs::read_to_string(outside.path()).unwrap(), "outside");
            assert!(fs::symlink_metadata(path).unwrap().file_type().is_symlink());
        }
    }

    #[test]
    fn read_file_preserves_exact_cap_and_cap_plus_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("note.txt");
        fs::write(&path, "a".repeat(MAX_READ_BYTES)).unwrap();

        let exact = execute_tool(
            dir.path(),
            ToolCallId::new("call_1").unwrap(),
            "file.read",
            json!({"path": "note.txt"}),
        )
        .unwrap();

        assert_eq!(
            exact.summary,
            format!("read {MAX_READ_BYTES} bytes from note.txt")
        );
        assert_eq!(exact.data["bytes"], MAX_READ_BYTES);
        assert_eq!(exact.data["truncated"], false);
        assert_eq!(
            exact.data["content"].as_str().unwrap().len(),
            MAX_READ_BYTES
        );

        fs::write(&path, "a".repeat(MAX_READ_BYTES + 1)).unwrap();
        let over = execute_tool(
            dir.path(),
            ToolCallId::new("call_2").unwrap(),
            "file.read",
            json!({"path": "note.txt"}),
        )
        .unwrap();

        assert_eq!(
            over.summary,
            format!("read {} bytes from note.txt", MAX_READ_BYTES + 1)
        );
        assert_eq!(over.data["bytes"], MAX_READ_BYTES + 1);
        assert_eq!(over.data["truncated"], true);
        assert_eq!(over.data["content"].as_str().unwrap().len(), MAX_READ_BYTES);
    }

    #[test]
    fn read_file_truncates_on_utf8_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let content = format!("{}éz", "a".repeat(MAX_READ_BYTES - 1));
        fs::write(dir.path().join("note.txt"), &content).unwrap();

        let result = execute_tool(
            dir.path(),
            ToolCallId::new("call_1").unwrap(),
            "file.read",
            json!({"path": "note.txt"}),
        )
        .unwrap();

        let content = result.data["content"].as_str().unwrap();
        assert!(content.is_char_boundary(content.len()));
        assert_eq!(content.len(), MAX_READ_BYTES - 1);
        assert_eq!(result.data["bytes"], MAX_READ_BYTES + 2);
        assert_eq!(result.data["truncated"], true);
    }

    #[test]
    fn read_file_does_not_read_or_validate_past_lookahead() {
        let buffer_bytes = MAX_READ_BYTES + READ_UTF8_LOOKAHEAD_BYTES;
        let mut bytes = vec![b'a'; buffer_bytes + 1];
        bytes[buffer_bytes] = 0xff;
        let mut reader = InstrumentedReader { bytes, position: 0 };

        let content = read_utf8_prefix(&mut reader, (buffer_bytes + 1) as u64).unwrap();

        assert_eq!(reader.position, buffer_bytes);
        assert_eq!(content.len(), buffer_bytes);
    }

    #[test]
    fn read_file_rejects_invalid_utf8_in_bounded_prefix() {
        let buffer_bytes = MAX_READ_BYTES + READ_UTF8_LOOKAHEAD_BYTES;
        let mut bytes = vec![b'a'; buffer_bytes + 1];
        bytes[MAX_READ_BYTES - 1] = 0xff;
        let mut reader = InstrumentedReader { bytes, position: 0 };

        let error = read_utf8_prefix(&mut reader, (buffer_bytes + 1) as u64).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn list_directory_lists_single_level_entries_in_sorted_order() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("b.txt"), "b").unwrap();
        fs::write(dir.path().join("a.txt"), "a").unwrap();
        fs::create_dir(dir.path().join("nested")).unwrap();
        fs::write(dir.path().join("nested").join("c.txt"), "c").unwrap();

        let result = execute_tool(
            dir.path(),
            ToolCallId::new("call_1").unwrap(),
            "file.list",
            json!({"path": "."}),
        )
        .unwrap();

        assert_eq!(result.summary, "listed 3 of 3 entries in .");
        assert_eq!(result.data["truncated"], false);
        assert_eq!(result.data["entry_count"], 3);
        assert_eq!(result.data["returned_count"], 3);
        assert_eq!(
            result.data["entries"],
            json!([
                {"name": "a.txt", "kind": "file"},
                {"name": "b.txt", "kind": "file"},
                {"name": "nested", "kind": "directory"}
            ])
        );
    }

    #[test]
    fn list_directory_rejects_paths_outside_workspace() {
        let dir = tempfile::tempdir().unwrap();
        let err = execute_tool(
            dir.path(),
            ToolCallId::new("call_1").unwrap(),
            "file.list",
            json!({"path": "../outside"}),
        )
        .unwrap_err();

        assert!(matches!(err, AppError::PathEscapesWorkspace(_)));
    }

    #[test]
    fn list_directory_rejects_file_paths() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("note.txt"), "hello").unwrap();

        let err = execute_tool(
            dir.path(),
            ToolCallId::new("call_1").unwrap(),
            "file.list",
            json!({"path": "note.txt"}),
        )
        .unwrap_err();

        assert!(matches!(
            err,
            AppError::Tool(message) if message == "not a directory: note.txt"
        ));
    }

    #[test]
    fn list_directory_truncates_after_max_entries() {
        let dir = tempfile::tempdir().unwrap();
        for index in 0..=MAX_LIST_ENTRIES {
            fs::write(dir.path().join(format!("file_{index:03}.txt")), "x").unwrap();
        }

        let result = execute_tool(
            dir.path(),
            ToolCallId::new("call_1").unwrap(),
            "file.list",
            json!({"path": "."}),
        )
        .unwrap();

        assert_eq!(result.data["truncated"], true);
        assert_eq!(result.data["entry_count"], MAX_LIST_ENTRIES + 1);
        assert_eq!(result.data["returned_count"], MAX_LIST_ENTRIES);
        assert_eq!(
            result.data["entries"].as_array().unwrap().len(),
            MAX_LIST_ENTRIES
        );
    }

    #[test]
    fn list_candidates_stay_bounded_in_adverse_iteration_order() {
        let total = MAX_LIST_ENTRIES * 10;
        let mut entries = Vec::with_capacity(MAX_LIST_ENTRIES);
        let capacity = entries.capacity();

        for index in (0..total).rev() {
            retain_list_candidate(
                &mut entries,
                ListEntry {
                    name: format!("file_{index:04}.txt"),
                    kind: "file",
                },
            );
            assert!(entries.len() <= MAX_LIST_ENTRIES);
            assert_eq!(entries.capacity(), capacity);
        }

        assert_eq!(entries.len(), MAX_LIST_ENTRIES);
        for (index, entry) in entries.iter().enumerate() {
            assert_eq!(entry.name, format!("file_{index:04}.txt"));
        }
    }

    #[cfg(unix)]
    #[test]
    fn list_directory_rejects_symlink_escape() {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), dir.path().join("outside")).unwrap();

        let err = execute_tool(
            dir.path(),
            ToolCallId::new("call_1").unwrap(),
            "file.list",
            json!({"path": "outside"}),
        )
        .unwrap_err();

        assert!(matches!(err, AppError::PathEscapesWorkspace(_)));
    }

    #[cfg(unix)]
    #[test]
    fn write_file_rejects_existing_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::NamedTempFile::new().unwrap();
        std::os::unix::fs::symlink(outside.path(), dir.path().join("link.txt")).unwrap();

        let err = execute_tool(
            dir.path(),
            ToolCallId::new("call_1").unwrap(),
            "file.write",
            json!({"path": "link.txt", "content": "hello"}),
        )
        .unwrap_err();

        assert!(matches!(err, AppError::PathEscapesWorkspace(_)));
    }

    #[cfg(unix)]
    #[test]
    fn edit_file_rejects_existing_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::NamedTempFile::new().unwrap();
        std::os::unix::fs::symlink(outside.path(), dir.path().join("link.txt")).unwrap();

        let err = execute_tool(
            dir.path(),
            ToolCallId::new("call_1").unwrap(),
            "file.edit",
            json!({"path": "link.txt", "content": "hello"}),
        )
        .unwrap_err();

        assert!(matches!(err, AppError::PathEscapesWorkspace(_)));
    }

    #[test]
    fn approval_input_preview_is_bounded() {
        let preview = approval_input_preview(&json!({"content": "x".repeat(2_000)}));

        assert!(preview.ends_with("...(truncated)"));
        assert_eq!(
            preview
                .strip_suffix("...(truncated)")
                .unwrap()
                .chars()
                .count(),
            APPROVAL_PREVIEW_CHARS
        );
    }

    #[test]
    fn file_edit_diff_preview_shows_current_vs_proposed() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("note.txt"), "old\nsame\n").unwrap();

        let diff = approval_diff_preview(
            dir.path(),
            FILE_EDIT,
            &json!({"path": "note.txt", "content": "new\nsame\n"}),
        )
        .unwrap();

        assert!(diff.contains("--- a/note.txt"));
        assert!(diff.contains("+++ b/note.txt"));
        assert!(diff.contains("-old\n"));
        assert!(diff.contains("+new\n"));
        assert!(diff.contains(" same\n"));
    }

    #[test]
    fn file_edit_diff_preview_for_missing_file_is_whole_file_add() {
        let dir = tempfile::tempdir().unwrap();

        let diff = approval_diff_preview(
            dir.path(),
            FILE_EDIT,
            &json!({"path": "created.txt", "content": "hello\n"}),
        )
        .unwrap();

        assert!(diff.contains("@@ -0,0 +1,1 @@"));
        assert!(diff.contains("+hello\n"));
    }

    #[test]
    fn file_edit_diff_preview_skips_unreadable_current_file() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join("note.txt")).unwrap();

        let diff = approval_diff_preview(
            dir.path(),
            FILE_EDIT,
            &json!({"path": "note.txt", "content": "hello\n"}),
        );

        assert_eq!(diff, None);
    }

    #[test]
    fn file_edit_diff_preview_truncates_huge_diff_with_marker() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("note.txt"), "old\n").unwrap();

        let diff = approval_diff_preview(
            dir.path(),
            FILE_EDIT,
            &json!({"path": "note.txt", "content": "new\n".repeat(DIFF_PREVIEW_CHARS)}),
        )
        .unwrap();

        assert!(diff.contains(DIFF_TRUNCATED_MARKER));
        assert!(diff.chars().count() < DIFF_PREVIEW_CHARS + DIFF_TRUNCATED_MARKER.len() + 4);
    }

    #[test]
    fn file_edit_diff_preview_truncation_keeps_proposed_content() {
        let dir = tempfile::tempdir().unwrap();
        let middle = (0..2000)
            .map(|line| format!("same-{line:04}\n"))
            .collect::<String>();
        fs::write(
            dir.path().join("note.txt"),
            format!("old top\n{middle}old bottom\n"),
        )
        .unwrap();

        let diff = approval_diff_preview(
            dir.path(),
            FILE_EDIT,
            &json!({"path": "note.txt", "content": format!("new top\n{middle}new bottom\n")}),
        )
        .unwrap();

        assert!(diff.contains("-old top\n"));
        assert!(diff.contains("+new top\n"));
        assert!(diff.contains(" same-0000\n"));
        assert!(diff.contains(DIFF_TRUNCATED_MARKER));
        assert!(diff.find("+new top").unwrap() < diff.find(DIFF_TRUNCATED_MARKER).unwrap());
    }

    #[test]
    fn stdin_approval_prompt_keeps_json_preview_for_file_edit() {
        let prompt = approval_prompt(
            FILE_EDIT,
            &json!({"path": "note.txt", "content": "new\n"}),
            None,
        );

        assert!(prompt.contains(r#""path":"note.txt""#));
        assert!(prompt.contains(r#""content":"new\n""#));
        assert!(!prompt.contains("--- a/note.txt"));
    }

    #[test]
    fn shell_approval_preview_includes_command_cwd_timeout_effect_and_env_posture() {
        let dir = tempfile::tempdir().unwrap();
        let preview = approval_command_preview(
            dir.path(),
            SHELL_EXEC,
            &json!({"command": "cargo test", "timeout_seconds": 700}),
            Some("OPENROUTER_API_KEY"),
        )
        .unwrap()
        .unwrap();

        assert!(preview.contains("command: cargo test"));
        assert!(preview.contains(&format!(
            "cwd: {}",
            dir.path().canonicalize().unwrap().display()
        )));
        assert!(preview.contains("timeout: 600s"));
        assert!(preview.contains("effect: ExternalSideEffect"));
        assert!(preview.contains("env: scrubbed allowlist"));
        assert!(preview.contains("OPENROUTER_API_KEY removed"));
    }

    #[test]
    fn shell_timeout_defaults_and_clamps() {
        assert_eq!(normalize_timeout_seconds(None), 120);
        assert_eq!(normalize_timeout_seconds(Some(0)), 1);
        assert_eq!(normalize_timeout_seconds(Some(10)), 10);
        assert_eq!(normalize_timeout_seconds(Some(700)), 600);
    }

    #[test]
    fn shell_env_keeps_only_allowlisted_non_credentials() {
        let env = shell_child_env_from(
            vec![
                ("PATH".into(), "/bin".into()),
                ("HOME".into(), "/home/user".into()),
                ("OPENROUTER_API_KEY".into(), "secret".into()),
                ("CARGO_AUTH_TOKEN".into(), "secret".into()),
                ("HTTP_PROXY".into(), "http://proxy".into()),
                ("RUSTUP_HOME".into(), "/rustup".into()),
            ],
            Some("OPENROUTER_API_KEY"),
        );

        assert_eq!(
            env,
            vec![
                ("PATH".into(), "/bin".into()),
                ("HOME".into(), "/home/user".into()),
                ("RUSTUP_HOME".into(), "/rustup".into())
            ]
        );
    }

    #[test]
    fn supervised_run_child_env_keeps_baseline_and_exact_configured_provider() {
        let mut vars = SHELL_ENV_ALLOWLIST
            .iter()
            .map(|name| ((*name).into(), format!("baseline-{name}").into()))
            .collect::<Vec<_>>();
        vars.extend([
            (
                "PLATONIC_CUSTOM_PROVIDER".into(),
                "provider-sentinel".into(),
            ),
            ("OPENAI_API_KEY".into(), "openai-sentinel".into()),
            ("GITHUB_TOKEN".into(), "github-sentinel".into()),
            ("AWS_ACCESS_KEY_ID".into(), "aws-id-sentinel".into()),
            ("AWS_SECRET_ACCESS_KEY".into(), "aws-secret-sentinel".into()),
            (
                "GOOGLE_APPLICATION_CREDENTIALS".into(),
                "google-sentinel".into(),
            ),
            ("AZURE_CLIENT_SECRET".into(), "azure-sentinel".into()),
            ("NPM_TOKEN".into(), "npm-sentinel".into()),
            (
                "CARGO_REGISTRIES_CRATES_IO_TOKEN".into(),
                "cargo-sentinel".into(),
            ),
            ("SSH_AUTH_SOCK".into(), "/tmp/agent-sentinel".into()),
            ("UNKNOWN_PARENT_SETTING".into(), "unknown-sentinel".into()),
        ]);

        let env = supervised_run_child_env_from(vars, "PLATONIC_CUSTOM_PROVIDER").unwrap();
        let mut expected = SHELL_ENV_ALLOWLIST
            .iter()
            .map(|name| ((*name).into(), format!("baseline-{name}").into()))
            .collect::<Vec<_>>();
        expected.push((
            "PLATONIC_CUSTOM_PROVIDER".into(),
            "provider-sentinel".into(),
        ));

        assert_eq!(env, expected);
    }

    #[test]
    fn supervised_run_child_env_fails_typed_when_configured_provider_is_missing() {
        let error = supervised_run_child_env_from(
            [("PATH".into(), "/bin".into())],
            "PLATONIC_MISSING_PROVIDER",
        )
        .unwrap_err();

        assert!(matches!(
            error,
            AppError::MissingApiKey(name) if name == "PLATONIC_MISSING_PROVIDER"
        ));
    }

    #[test]
    fn supervised_run_child_env_injects_allowlisted_provider_name_once() {
        let env = supervised_run_child_env_from(
            [
                ("PATH".into(), "/runtime-and-provider".into()),
                ("HOME".into(), "/home/user".into()),
            ],
            "PATH",
        )
        .unwrap();

        assert_eq!(
            env,
            [
                ("HOME".into(), "/home/user".into()),
                ("PATH".into(), "/runtime-and-provider".into())
            ]
        );
    }

    #[cfg(unix)]
    #[test]
    fn supervised_run_child_env_matches_unix_provider_names_case_sensitively() {
        let error = supervised_run_child_env_from(
            [(
                "Platonic_Custom_Provider".into(),
                "provider-sentinel".into(),
            )],
            "PLATONIC_CUSTOM_PROVIDER",
        )
        .unwrap_err();

        assert!(matches!(error, AppError::MissingApiKey(_)));
    }

    #[cfg(unix)]
    #[test]
    fn supervised_run_child_env_ignores_unrelated_non_utf8_names_and_values() {
        let env = supervised_run_child_env_from(
            [
                (OsString::from("PATH"), OsString::from("/bin")),
                (
                    OsString::from("PLATONIC_CUSTOM_PROVIDER"),
                    OsString::from("provider-sentinel"),
                ),
                (
                    OsString::from("UNRELATED_NON_UTF8_VALUE"),
                    OsString::from_vec(b"value-\xff".to_vec()),
                ),
                (
                    OsString::from_vec(b"UNRELATED_NON_UTF8_NAME_\xff".to_vec()),
                    OsString::from("name-sentinel"),
                ),
            ],
            "PLATONIC_CUSTOM_PROVIDER",
        )
        .unwrap();

        assert_eq!(
            env,
            [
                (OsString::from("PATH"), OsString::from("/bin")),
                (
                    OsString::from("PLATONIC_CUSTOM_PROVIDER"),
                    OsString::from("provider-sentinel"),
                )
            ]
        );
    }

    #[test]
    fn capped_output_marks_truncation() {
        let output = read_capped_output(io::Cursor::new(b"abcdef".to_vec()), 3).unwrap();

        assert_eq!(output.text, format!("abc{SHELL_OUTPUT_TRUNCATED_MARKER}"));
        assert!(output.truncated);
    }

    #[cfg(unix)]
    #[test]
    fn shell_exec_runs_from_workspace_root() {
        let dir = tempfile::tempdir().unwrap();
        let result = execute_tool(
            dir.path(),
            ToolCallId::new("call_1").unwrap(),
            SHELL_EXEC,
            json!({"command": "pwd"}),
        )
        .unwrap();

        assert_eq!(result.data["exit_code"], 0);
        let cwd = dir.path().canonicalize().unwrap();
        assert_eq!(result.data["cwd"].as_str().unwrap(), cwd.to_string_lossy());
        assert_eq!(
            result.data["stdout"].as_str().unwrap().trim(),
            cwd.to_string_lossy()
        );
    }

    #[cfg(unix)]
    #[test]
    fn shell_exec_uses_a_trusted_shell_and_resolves_commands_on_user_path() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("user-bin");
        fs::create_dir(&bin).unwrap();
        let fake_shell = bin.join("sh");
        let user_command = bin.join("user-path-proof");
        fs::write(&fake_shell, "#!/bin/sh\nprintf compromised\n").unwrap();
        fs::write(&user_command, "#!/bin/sh\nprintf user-path-ok\n").unwrap();
        for executable in [&fake_shell, &user_command] {
            let mut permissions = fs::metadata(executable).unwrap().permissions();
            permissions.set_mode(0o700);
            fs::set_permissions(executable, permissions).unwrap();
        }

        let output = spawn_shell(
            "user-path-proof",
            dir.path(),
            vec![("PATH".into(), bin.to_string_lossy().into_owned())],
        )
        .unwrap()
        .wait_with_output()
        .unwrap();

        assert!(output.status.success());
        assert_eq!(output.stdout, b"user-path-ok");
    }

    #[cfg(unix)]
    #[test]
    fn shell_exec_records_nonzero_exit_as_finished_result() {
        let dir = tempfile::tempdir().unwrap();
        let result = execute_tool(
            dir.path(),
            ToolCallId::new("call_1").unwrap(),
            SHELL_EXEC,
            json!({"command": "printf fail >&2; exit 7"}),
        )
        .unwrap();

        assert_eq!(result.data["exit_code"], 7);
        assert_eq!(result.data["stderr"], "fail");
        assert!(result.data.get("timed_out").is_none());
    }

    #[cfg(unix)]
    #[test]
    fn shell_exec_caps_stdout_and_stderr_independently() {
        let dir = tempfile::tempdir().unwrap();
        let result = execute_tool(
            dir.path(),
            ToolCallId::new("call_1").unwrap(),
            SHELL_EXEC,
            json!({"command": "yes out | head -c 33000; yes err | head -c 33000 >&2"}),
        )
        .unwrap();

        assert!(result.data["stdout"].as_str().unwrap().len() > SHELL_OUTPUT_BYTES);
        assert!(result.data["stderr"].as_str().unwrap().len() > SHELL_OUTPUT_BYTES);
        assert!(
            result.data["stdout"]
                .as_str()
                .unwrap()
                .contains(SHELL_OUTPUT_TRUNCATED_MARKER)
        );
        assert!(
            result.data["stderr"]
                .as_str()
                .unwrap()
                .contains(SHELL_OUTPUT_TRUNCATED_MARKER)
        );
        assert_eq!(result.data["stdout_truncated"], true);
        assert_eq!(result.data["stderr_truncated"], true);
    }

    #[cfg(unix)]
    #[test]
    fn shell_exec_times_out() {
        let dir = tempfile::tempdir().unwrap();
        let err = execute_tool(
            dir.path(),
            ToolCallId::new("call_1").unwrap(),
            SHELL_EXEC,
            json!({"command": "sleep 2", "timeout_seconds": 1}),
        )
        .unwrap_err();

        assert!(matches!(
            err,
            AppError::Tool(message) if message == "shell.exec timed out after 1s"
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn shell_exec_timeout_kills_grandchildren() {
        let dir = tempfile::tempdir().unwrap();
        let pid_file = dir.path().join("grandchild.pid");
        let command = format!(
            "sleep 30 >/dev/null 2>&1 & echo $! > {}; wait",
            pid_file.display()
        );
        let started = Instant::now();
        let err = execute_tool(
            dir.path(),
            ToolCallId::new("call_1").unwrap(),
            SHELL_EXEC,
            json!({"command": command, "timeout_seconds": 1}),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AppError::Tool(message) if message == "shell.exec timed out after 1s"
        ));
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "timeout return blocked on surviving grandchild"
        );

        let pid: i32 = fs::read_to_string(&pid_file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match fs::read_to_string(format!("/proc/{pid}/stat")) {
                Err(_) => break,
                Ok(stat) if stat.split_whitespace().nth(2) == Some("Z") => break,
                Ok(_) => {}
            }
            assert!(
                Instant::now() < deadline,
                "grandchild {pid} survived group kill"
            );
            thread::sleep(Duration::from_millis(50));
        }
    }

    #[cfg(unix)]
    #[test]
    fn shell_exec_observes_cancel_flag() {
        let dir = tempfile::tempdir().unwrap();
        let cancel = std::sync::atomic::AtomicBool::new(true);
        let err = execute_tool_with_context(
            ToolExecutionContext {
                workspace_root: dir.path(),
                provider_api_key_env: None,
                cancel: Some(&cancel),
                thread_spawn: None,
                approving_actor: None,
            },
            ToolCallId::new("call_1").unwrap(),
            SHELL_EXEC,
            json!({"command": "sleep 5"}),
        )
        .unwrap_err();

        assert!(matches!(
            err,
            AppError::Tool(message) if message == "shell.exec canceled"
        ));
    }
}
