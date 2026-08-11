use crate::tool_catalog::{FILE_EDIT, FILE_WRITE};
use crate::{AppError, AppResult};
use serde_json::Value;
use std::{
    fs,
    io::ErrorKind,
    path::{Component, Path, PathBuf},
};

pub(crate) const PLATONIC_MEMORY_FILENAME: &str = "PLATONIC.md";
pub(crate) const PLATONIC_MEMORY_MAX_BYTES: usize = 8_192;
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
pub(super) fn platonic_memory_target_path(
    workspace_root: &Path,
    raw_path: &str,
) -> Option<PathBuf> {
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

pub(super) fn validate_platonic_memory_content(path: &Path, content: &[u8]) -> AppResult<()> {
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

pub(super) fn validate_platonic_memory_target(path: &Path) -> AppResult<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(()),
        Ok(_) => Err(AppError::PlatonicMemoryNotRegular(path.to_path_buf())),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{tool_catalog::FILE_READ, tools::execute_tool};
    use platonic_core::ToolCallId;
    use serde_json::json;
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
}
