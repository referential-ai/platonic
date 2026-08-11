use crate::{
    AppError, AppResult,
    model::{ModelBlock, ModelResponse, ModelStop},
    tool_catalog::internal_name_for_provider,
};
use platonic_core::{ModelName, ModelUsage};
use serde::{Deserialize, Serialize};
#[cfg(test)]
use serde_json::{Value, json};

pub(super) const MAX_ASSISTANT_TEXT_BYTES: usize = 4 * 1024 * 1024;
pub(super) const MAX_TOOL_CALLS: usize = 64;
pub(super) const MAX_TOOL_CALL_INDEX: usize = MAX_TOOL_CALLS - 1;
pub(super) const MAX_TOOL_NAME_BYTES: usize = 256;
pub(super) const MAX_TOOL_ARGUMENT_BYTES: usize = 1024 * 1024;
pub(super) const MAX_TOOL_ARGUMENTS_BYTES: usize = 4 * 1024 * 1024;

pub(super) const ASSISTANT_TEXT_LIMIT_ERROR: &str =
    "provider response exceeded the 4 MiB assistant text limit";
pub(super) const TOOL_CALL_LIMIT_ERROR: &str = "provider response exceeded the 64 tool call limit";
pub(super) const TOOL_NAME_LIMIT_ERROR: &str =
    "provider response exceeded the 256-byte tool name limit";
pub(super) const TOOL_ARGUMENT_LIMIT_ERROR: &str =
    "provider response exceeded the 1 MiB per-call tool arguments limit";
pub(super) const TOOL_ARGUMENTS_LIMIT_ERROR: &str =
    "provider response exceeded the 4 MiB aggregate tool arguments limit";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ChatToolType {
    Function,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct ChatToolCall {
    pub(super) id: String,
    #[serde(rename = "type")]
    pub(super) tool_type: ChatToolType,
    pub(super) function: ChatFunctionCall,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct ChatFunctionCall {
    pub(super) name: String,
    pub(super) arguments: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ChatFinishReason {
    Stop,
    Length,
    ToolCalls,
    ContentFilter,
    FunctionCall,
}

#[derive(Clone, Debug, Deserialize)]
pub(super) struct ChatUsage {
    pub(super) prompt_tokens: Option<u32>,
    pub(super) completion_tokens: Option<u32>,
}

pub(super) fn tool_use_from_provider(
    id: String,
    name: String,
    arguments: String,
) -> AppResult<ModelBlock> {
    if id.trim().is_empty() {
        return Err(AppError::Provider(
            "provider returned tool call without id".into(),
        ));
    }
    validate_tool_call(&name, &arguments)?;
    let tool_name = internal_name_for_provider(&name)
        .ok_or_else(|| AppError::Provider(format!("provider returned unknown tool {name}")))?;
    let input = serde_json::from_str(&arguments).map_err(|error| {
        AppError::Provider(format!(
            "provider returned invalid JSON for {name}: {error}"
        ))
    })?;
    Ok(ModelBlock::ToolUse {
        id,
        name: tool_name.into(),
        input,
    })
}

pub(super) fn validate_tool_call(name: &str, arguments: &str) -> AppResult<()> {
    if name.len() > MAX_TOOL_NAME_BYTES {
        return Err(AppError::Provider(TOOL_NAME_LIMIT_ERROR.into()));
    }
    if arguments.len() > MAX_TOOL_ARGUMENT_BYTES {
        return Err(AppError::Provider(TOOL_ARGUMENT_LIMIT_ERROR.into()));
    }
    Ok(())
}

fn stop_from_finish(finish_reason: ChatFinishReason) -> ModelStop {
    match finish_reason {
        ChatFinishReason::Stop => ModelStop::EndTurn,
        ChatFinishReason::ToolCalls | ChatFinishReason::FunctionCall => ModelStop::ToolUse,
        ChatFinishReason::Length => ModelStop::MaxOutput,
        ChatFinishReason::ContentFilter => ModelStop::ContentFilter,
    }
}

fn usage_from(usage: Option<ChatUsage>) -> Option<ModelUsage> {
    let usage = usage?;
    Some(ModelUsage {
        input_tokens: usage.prompt_tokens?,
        output_tokens: usage.completion_tokens?,
    })
}

pub(super) fn model_response(
    content: Vec<ModelBlock>,
    finish_reason: ChatFinishReason,
    served_model: Option<ModelName>,
    usage: Option<ChatUsage>,
) -> ModelResponse {
    ModelResponse {
        content,
        stop: stop_from_finish(finish_reason),
        served_model,
        usage: usage_from(usage),
    }
}

#[cfg(test)]
pub(super) fn utf8_string_with_bytes(bytes: usize) -> String {
    let mut value = "\u{754c}".repeat(bytes / "\u{754c}".len());
    value.push_str(&"x".repeat(bytes % "\u{754c}".len()));
    assert_eq!(value.len(), bytes);
    value
}

#[cfg(test)]
pub(super) fn fragmented_unicode_json_arguments(bytes: usize) -> [String; 2] {
    const PREFIX: &str = "{\"value\":\"";
    const SUFFIX: &str = "\"}";

    let content_bytes = bytes
        .checked_sub(PREFIX.len() + SUFFIX.len())
        .expect("argument fixture must fit JSON framing");
    let first_bytes = content_bytes / 2;
    let second_bytes = content_bytes - first_bytes;
    let fragments = [
        format!("{PREFIX}{}", utf8_string_with_bytes(first_bytes)),
        format!("{}{SUFFIX}", utf8_string_with_bytes(second_bytes)),
    ];
    assert_eq!(fragments.iter().map(String::len).sum::<usize>(), bytes);
    fragments
}

#[cfg(test)]
pub(super) fn json_arguments_with_bytes(bytes: usize) -> String {
    assert!(bytes >= 2);
    let arguments = format!("{{{}}}", " ".repeat(bytes - 2));
    assert_eq!(arguments.len(), bytes);
    arguments
}

#[cfg(test)]
pub(super) fn usage_fixtures() -> Vec<(&'static str, Option<Value>, Option<ModelUsage>)> {
    vec![
        (
            "reported",
            Some(json!({
                "prompt_tokens": 10,
                "completion_tokens": 5
            })),
            Some(ModelUsage {
                input_tokens: 10,
                output_tokens: 5,
            }),
        ),
        (
            "reported_zero",
            Some(json!({
                "prompt_tokens": 0,
                "completion_tokens": 0
            })),
            Some(ModelUsage {
                input_tokens: 0,
                output_tokens: 0,
            }),
        ),
        ("omitted", None, None),
        (
            "partial_prompt_only",
            Some(json!({"prompt_tokens": 10})),
            None,
        ),
        (
            "partial_completion_only",
            Some(json!({"completion_tokens": 5})),
            None,
        ),
    ]
}
