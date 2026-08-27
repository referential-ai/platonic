use super::{
    files::{FileContentInput, resolve_write_path},
    shell::{ShellExecInput, normalize_timeout_seconds},
    web,
};
use crate::tool_catalog::{FILE_EDIT, SHELL_EXEC, WEB_FETCH};
use crate::{AppError, AppResult};
use serde_json::Value;
use std::{
    fs,
    io::{self, ErrorKind, Write},
    path::Path,
};

const APPROVAL_PREVIEW_CHARS: usize = 1_000;
const DIFF_PREVIEW_CHARS: usize = 16 * 1024;
const DIFF_TRUNCATED_MARKER: &str = "... diff truncated";
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApprovalOutcome {
    Granted,
    Denied { reason: String },
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
    if input
        .credential
        .as_deref()
        .is_some_and(|credential_id| !crate::config::valid_credential_id(credential_id))
    {
        return Err(AppError::Tool("shell.exec credential id is invalid".into()));
    }
    let timeout_seconds = normalize_timeout_seconds(input.timeout_seconds);
    let cwd = workspace_root
        .canonicalize()
        .unwrap_or_else(|_| workspace_root.to_path_buf());
    let provider = provider_api_key_env.unwrap_or("configured provider key");
    let credential = input.credential.as_deref().map_or_else(String::new, |id| {
        format!("\ncredential: {id}\ncredential path: $TMPDIR/credentials/{id}\ncredential lifetime: this approved call only")
    });
    Ok(Some(format!(
        "command: {}\ncwd: {}\ntimeout: {}s{credential}\neffect: ExternalSideEffect\nenv: scrubbed allowlist; credential-like names and {provider} removed",
        input.command,
        cwd.display(),
        timeout_seconds
    )))
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
    use serde_json::json;
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
}
