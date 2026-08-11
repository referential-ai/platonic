use super::ToolExecutionContext;
use crate::{AppError, AppResult};
use platonic_core::{ResultVisibility, ToolResult};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    env,
    ffi::OsString,
    io::{self, Read},
    path::Path,
    process::{ChildStderr, ChildStdout, Command, Stdio},
    sync::atomic::Ordering,
    thread,
    time::{Duration, Instant},
};
#[cfg(unix)]
use std::{os::unix::process::CommandExt, process::Child};

const SHELL_OUTPUT_BYTES: usize = 32 * 1024;
const SHELL_OUTPUT_TRUNCATED_MARKER: &str = "\n... output truncated";
const SHELL_DEFAULT_TIMEOUT_SECONDS: u64 = 120;
const SHELL_MAX_TIMEOUT_SECONDS: u64 = 600;
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
pub(super) struct ShellExecInput {
    pub(super) command: String,
    pub(super) timeout_seconds: Option<u64>,
}
pub(super) fn shell_exec(
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

pub(super) fn normalize_timeout_seconds(timeout_seconds: Option<u64>) -> u64 {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        tool_catalog::SHELL_EXEC,
        tools::{ToolExecutionContext, execute_tool, execute_tool_with_context},
    };
    use platonic_core::ToolCallId;
    use serde_json::json;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::ffi::OsStringExt;
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
