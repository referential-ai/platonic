use super::memory::{
    platonic_memory_target_path, validate_platonic_memory_content, validate_platonic_memory_target,
};
use crate::{AppError, AppResult};
use platonic_core::{ResultVisibility, ToolResult};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fs,
    io::{self, ErrorKind, Read},
    path::{Component, Path, PathBuf},
};

const MAX_READ_BYTES: usize = 64 * 1024;
const READ_UTF8_LOOKAHEAD_BYTES: usize = 4;
const MAX_LIST_ENTRIES: usize = 200;
const MAX_LIST_DATA_BYTES: usize = 32 * 1024;
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
pub(super) struct FileContentInput {
    pub(super) path: String,
    pub(super) content: String,
}

pub(super) fn read_file(
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

pub(super) fn list_directory(
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

pub(super) fn write_file(
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

pub(super) fn resolve_write_path(workspace_root: &Path, raw_path: &str) -> AppResult<PathBuf> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::execute_tool;
    use platonic_core::ToolCallId;
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
}
