use super::context::estimate_tokens;
use crate::{
    AppError, AppResult,
    config::Config,
    ledger::SessionTurn,
    model::{ModelBlock, ModelMessage, system_prompt},
    tool_catalog::ToolSpec,
    tools::{PLATONIC_MEMORY_FILENAME, PLATONIC_MEMORY_MAX_BYTES},
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io::{self, Read},
    path::Path,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunSession {
    Fresh { session_id: String },
    Continue { session_id: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tool_catalog::tool_specs;

    #[test]
    fn platonic_memory_accepts_exact_byte_cap_without_trimming() {
        let workspace = tempfile::tempdir().unwrap();
        let path = workspace.path().join(PLATONIC_MEMORY_FILENAME);
        let content = format!(" \n{} \n", "a".repeat(PLATONIC_MEMORY_MAX_BYTES - 4));
        assert_eq!(content.len(), PLATONIC_MEMORY_MAX_BYTES);
        std::fs::write(&path, &content).unwrap();

        let loaded = load_platonic_memory(workspace.path()).unwrap();

        assert_eq!(loaded.as_deref(), Some(content.as_str()));
    }

    #[test]
    fn platonic_memory_rejects_cap_plus_one_and_counts_multibyte_utf8_bytes() {
        let workspace = tempfile::tempdir().unwrap();
        let path = workspace.path().join(PLATONIC_MEMORY_FILENAME);
        for content in [
            vec![b'a'; PLATONIC_MEMORY_MAX_BYTES + 1],
            "\u{754c}".repeat(2_731).into_bytes(),
        ] {
            assert_eq!(content.len(), PLATONIC_MEMORY_MAX_BYTES + 1);
            std::fs::write(&path, content).unwrap();

            assert!(matches!(
                load_platonic_memory(workspace.path()),
                Err(AppError::PlatonicMemoryTooLarge {
                    path: error_path,
                    max_bytes: PLATONIC_MEMORY_MAX_BYTES,
                }) if error_path == path
            ));
        }
    }

    #[test]
    fn platonic_memory_rejects_invalid_utf8() {
        let workspace = tempfile::tempdir().unwrap();
        let path = workspace.path().join(PLATONIC_MEMORY_FILENAME);
        std::fs::write(&path, [b'v', 0xff]).unwrap();

        assert!(matches!(
            load_platonic_memory(workspace.path()),
            Err(AppError::PlatonicMemoryInvalidUtf8(error_path)) if error_path == path
        ));
    }

    #[test]
    fn platonic_memory_rejects_directory_targets() {
        let workspace = tempfile::tempdir().unwrap();
        let path = workspace.path().join(PLATONIC_MEMORY_FILENAME);
        std::fs::create_dir(&path).unwrap();

        assert!(matches!(
            load_platonic_memory(workspace.path()),
            Err(AppError::PlatonicMemoryNotRegular(error_path)) if error_path == path
        ));
    }

    #[cfg(unix)]
    #[test]
    fn platonic_memory_rejects_symlink_and_other_non_regular_targets() {
        let workspace = tempfile::tempdir().unwrap();
        let path = workspace.path().join(PLATONIC_MEMORY_FILENAME);
        let target = workspace.path().join("memory-target.md");
        std::fs::write(&target, "must not be followed").unwrap();
        std::os::unix::fs::symlink(&target, &path).unwrap();

        assert!(matches!(
            load_platonic_memory(workspace.path()),
            Err(AppError::PlatonicMemoryNotRegular(error_path)) if error_path == path
        ));

        std::fs::remove_file(&path).unwrap();
        let _socket = std::os::unix::net::UnixListener::bind(&path).unwrap();
        assert!(matches!(
            load_platonic_memory(workspace.path()),
            Err(AppError::PlatonicMemoryNotRegular(error_path)) if error_path == path
        ));
    }

    #[test]
    fn platonic_memory_loads_only_the_exact_workspace_root_file() {
        let workspace = tempfile::tempdir().unwrap();
        std::fs::write(workspace.path().join("PLATO.md"), "alias").unwrap();
        std::fs::create_dir(workspace.path().join("nested")).unwrap();
        std::fs::write(
            workspace
                .path()
                .join("nested")
                .join(PLATONIC_MEMORY_FILENAME),
            "nested",
        )
        .unwrap();

        assert_eq!(load_platonic_memory(workspace.path()).unwrap(), None);

        std::fs::write(
            workspace.path().join(PLATONIC_MEMORY_FILENAME),
            "exact root",
        )
        .unwrap();
        assert_eq!(
            load_platonic_memory(workspace.path()).unwrap().as_deref(),
            Some("exact root")
        );
    }

    #[test]
    fn session_hydration_includes_prior_turns_and_current_question() {
        let config = Config::default();
        let tools = tool_specs(&config.tools.enabled);
        let turns = vec![SessionTurn {
            question: "first question".into(),
            final_answer: "first answer".into(),
        }];

        let hydration =
            hydrated_messages(&turns, "second question", &config, &tools, system_prompt()).unwrap();
        let messages = &hydration.retained_messages;

        assert_eq!(messages.len(), 3);
        assert_eq!(text(&messages[0]), "first question");
        assert_eq!(text(&messages[1]), "first answer");
        assert_eq!(text(&messages[2]), "second question");
        assert_eq!(hydration.dropped_turns, 0);
        assert_eq!(
            hydration.estimated_tokens_before,
            hydration.estimated_tokens_after
        );
        assert_eq!(
            hydration.estimated_tokens_before,
            estimated_context_tokens(system_prompt(), messages, &tools).unwrap()
        );
    }

    #[test]
    fn session_hydration_drops_oldest_turns_with_marker() {
        let mut config = Config::default();
        let tools = tool_specs(&config.tools.enabled);
        let turns = vec![
            SessionTurn {
                question: "old question ".repeat(400),
                final_answer: "old answer ".repeat(400),
            },
            SessionTurn {
                question: "middle question ".repeat(400),
                final_answer: "middle answer ".repeat(400),
            },
            SessionTurn {
                question: "recent question".into(),
                final_answer: "recent answer".into(),
            },
        ];
        let expected_before_messages = session_messages_from(&turns, "current question", false);
        let one_drop_messages = session_messages_from(&turns[1..], "current question", true);
        let expected_after_messages = session_messages_from(&turns[2..], "current question", true);
        let expected_before =
            estimated_context_tokens(system_prompt(), &expected_before_messages, &tools).unwrap();
        let one_drop =
            estimated_context_tokens(system_prompt(), &one_drop_messages, &tools).unwrap();
        let expected_after =
            estimated_context_tokens(system_prompt(), &expected_after_messages, &tools).unwrap();
        assert!(one_drop > expected_after);
        config.limits.token_budget = expected_after;

        let hydration =
            hydrated_messages(&turns, "current question", &config, &tools, system_prompt())
                .unwrap();
        let serialized = serde_json::to_string(&hydration.retained_messages).unwrap();

        assert!(serialized.contains(SESSION_TRUNCATION_MARKER));
        assert!(!serialized.contains("old question"));
        assert!(!serialized.contains("middle question"));
        assert!(serialized.contains("recent question"));
        assert!(serialized.contains("current question"));
        assert_eq!(hydration.dropped_turns, 2);
        assert_eq!(hydration.estimated_tokens_before, expected_before);
        assert_eq!(hydration.estimated_tokens_after, expected_after);
        assert_eq!(hydration.retained_messages, expected_after_messages);
    }

    fn text(message: &ModelMessage) -> &str {
        match &message.content[0] {
            ModelBlock::Text { text } => text,
            block => panic!("expected text block, got {block:?}"),
        }
    }
}

impl RunSession {
    pub fn session_id(&self) -> &str {
        match self {
            Self::Fresh { session_id } | Self::Continue { session_id } => session_id,
        }
    }

    pub(super) fn create_session(&self) -> bool {
        matches!(self, Self::Fresh { .. })
    }
}

pub(super) const SESSION_TRUNCATION_MARKER: &str =
    "[older session turns omitted to fit the context budget]";
pub(super) const PLATONIC_MEMORY_SEPARATOR: &str = "\n\n";
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct SessionHydration {
    pub(super) retained_messages: Vec<ModelMessage>,
    pub(super) dropped_turns: u64,
    pub(super) estimated_tokens_before: u32,
    pub(super) estimated_tokens_after: u32,
}
pub(super) fn hydrated_messages(
    turns: &[SessionTurn],
    question: &str,
    config: &Config,
    tools: &[ToolSpec],
    system_context: &str,
) -> AppResult<SessionHydration> {
    let mut first_retained_turn = 0;
    let mut retained_messages = session_messages_from(turns, question, false);
    let estimated_tokens_before =
        estimated_context_tokens(system_context, &retained_messages, tools)?;
    let mut estimated_tokens_after = estimated_tokens_before;

    while estimated_tokens_after > config.limits.token_budget && first_retained_turn < turns.len() {
        first_retained_turn += 1;
        retained_messages = session_messages_from(&turns[first_retained_turn..], question, true);
        estimated_tokens_after =
            estimated_context_tokens(system_context, &retained_messages, tools)?;
    }

    let dropped_turns = u64::try_from(first_retained_turn)
        .map_err(|_| AppError::Config("session history exceeds compaction range".into()))?;
    Ok(SessionHydration {
        retained_messages,
        dropped_turns,
        estimated_tokens_before,
        estimated_tokens_after,
    })
}

pub(super) fn session_messages_from(
    turns: &[SessionTurn],
    question: &str,
    truncated: bool,
) -> Vec<ModelMessage> {
    let mut messages = Vec::new();
    if truncated {
        messages.push(ModelMessage::user_text(SESSION_TRUNCATION_MARKER));
    }
    for turn in turns {
        messages.push(ModelMessage::user_text(turn.question.clone()));
        messages.push(ModelMessage::assistant_blocks(vec![ModelBlock::Text {
            text: turn.final_answer.clone(),
        }]));
    }
    messages.push(ModelMessage::user_text(question.to_string()));
    messages
}

pub(super) fn load_platonic_memory(workspace_root: &Path) -> AppResult<Option<String>> {
    let path = workspace_root.join(PLATONIC_MEMORY_FILENAME);
    let Some(mut file) = open_platonic_memory(&path)? else {
        return Ok(None);
    };
    let mut bytes = Vec::with_capacity(PLATONIC_MEMORY_MAX_BYTES + 1);
    Read::by_ref(&mut file)
        .take((PLATONIC_MEMORY_MAX_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > PLATONIC_MEMORY_MAX_BYTES {
        return Err(AppError::PlatonicMemoryTooLarge {
            path,
            max_bytes: PLATONIC_MEMORY_MAX_BYTES,
        });
    }
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|_| AppError::PlatonicMemoryInvalidUtf8(path))
}

fn open_platonic_memory(path: &Path) -> AppResult<Option<File>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if !metadata.file_type().is_file() => {
            return Err(AppError::PlatonicMemoryNotRegular(path.to_path_buf()));
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }

    let file = match open_final_component_no_follow(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return match fs::symlink_metadata(path) {
                Ok(metadata) if !metadata.file_type().is_file() => {
                    Err(AppError::PlatonicMemoryNotRegular(path.to_path_buf()))
                }
                Err(current) if current.kind() == io::ErrorKind::NotFound => Ok(None),
                _ => Err(error.into()),
            };
        }
    };
    if !file.metadata()?.file_type().is_file() {
        return Err(AppError::PlatonicMemoryNotRegular(path.to_path_buf()));
    }
    Ok(Some(file))
}

#[cfg(unix)]
fn open_final_component_no_follow(path: &Path) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;

    fs::OpenOptions::new()
        .read(true)
        .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
        .open(path)
}

pub(super) fn provider_system_context(platonic_memory: Option<&str>) -> String {
    provider_system_context_with_interruption(platonic_memory, None)
}

pub(super) fn provider_system_context_with_interruption(
    platonic_memory: Option<&str>,
    voice_interruption: Option<&str>,
) -> String {
    let mut context = system_prompt().to_string();
    if let Some(content) = platonic_memory {
        context.push_str(PLATONIC_MEMORY_SEPARATOR);
        context.push_str(content);
    }
    if let Some(content) = voice_interruption {
        context.push_str(PLATONIC_MEMORY_SEPARATOR);
        context.push_str(content);
    }
    context
}

pub(super) fn estimated_context_tokens(
    system_context: &str,
    messages: &[ModelMessage],
    tools: &[ToolSpec],
) -> AppResult<u32> {
    let messages = serde_json::to_string(messages)?;
    let tools = serde_json::to_string(tools)?;
    Ok(estimate_tokens(system_context)
        .saturating_add(estimate_tokens(&messages))
        .saturating_add(estimate_tokens(&tools)))
}
