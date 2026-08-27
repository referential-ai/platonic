use super::{
    response::read_non_stream_body,
    stream::parse_sse_stream,
    types::{
        ASSISTANT_TEXT_LIMIT_ERROR, MAX_ASSISTANT_TEXT_BYTES, MAX_TOOL_ARGUMENT_BYTES,
        MAX_TOOL_ARGUMENTS_BYTES, MAX_TOOL_CALLS, TOOL_ARGUMENT_LIMIT_ERROR,
        TOOL_ARGUMENTS_LIMIT_ERROR, TOOL_CALL_LIMIT_ERROR, tool_use_from_provider,
    },
};
use crate::{
    AppError, AppResult,
    model::{ModelBlock, ModelMessage, ModelRequest, ModelResponse, ModelRole, ModelStop},
    tool_catalog::{ToolSpec, provider_name_for_internal},
};
use platonic_core::{ModelName, ModelUsage};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, io::BufRead, io::Read};

const INVALID_RESPONSE_ERROR: &str = "provider returned an invalid Responses response";
const INVALID_STREAM_EVENT_ERROR: &str = "provider returned an invalid Responses stream event";
const UNKNOWN_STREAM_EVENT_ERROR: &str = "provider returned an unknown Responses stream event";
const STREAM_ERROR_EVENT_ERROR: &str = "provider Responses stream returned an error event";
const STREAM_ARGUMENT_MISMATCH_ERROR: &str =
    "provider Responses stream returned conflicting function arguments";

#[derive(Debug, Serialize)]
pub(super) struct ResponsesRequest {
    model: String,
    instructions: String,
    input: Vec<ResponsesInputItem>,
    tools: Vec<ResponsesTool>,
    tool_choice: &'static str,
    parallel_tool_calls: bool,
    max_output_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning: Option<ResponsesReasoning>,
    store: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) stream: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponsesInputItem {
    Message {
        role: ResponsesMessageRole,
        content: Vec<ResponsesInputContent>,
        #[serde(skip_serializing_if = "Option::is_none")]
        status: Option<&'static str>,
    },
    FunctionCall {
        call_id: String,
        name: String,
        arguments: String,
    },
    FunctionCallOutput {
        call_id: String,
        output: String,
    },
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum ResponsesMessageRole {
    User,
    Assistant,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponsesInputContent {
    InputText { text: String },
    OutputText { text: String, annotations: Vec<()> },
}

#[derive(Debug, Serialize)]
struct ResponsesTool {
    #[serde(rename = "type")]
    tool_type: &'static str,
    name: String,
    description: String,
    parameters: Value,
}

#[derive(Debug, Serialize)]
struct ResponsesReasoning {
    effort: crate::model::ReasoningEffort,
}

impl ResponsesRequest {
    pub(super) fn from_model_request(request: &ModelRequest) -> AppResult<Self> {
        let mut input = Vec::new();
        for message in &request.messages {
            input.extend(ResponsesInputItem::from_model_message(message)?);
        }
        Ok(Self {
            model: request.model.clone(),
            instructions: request.system.clone(),
            input,
            tools: request.tools.iter().map(ResponsesTool::from).collect(),
            tool_choice: "auto",
            parallel_tool_calls: false,
            max_output_tokens: request.max_output_tokens,
            reasoning: request
                .reasoning_effort
                .map(|effort| ResponsesReasoning { effort }),
            store: false,
            stream: None,
        })
    }
}

impl ResponsesInputItem {
    fn from_model_message(message: &ModelMessage) -> AppResult<Vec<Self>> {
        match message.role {
            ModelRole::User => {
                let content = message
                    .content
                    .iter()
                    .map(|block| match block {
                        ModelBlock::Text { text } => {
                            Ok(ResponsesInputContent::InputText { text: text.clone() })
                        }
                        ModelBlock::ToolUse { .. } | ModelBlock::ToolResult { .. } => Err(
                            AppError::Provider("user message contained a non-text block".into()),
                        ),
                    })
                    .collect::<AppResult<Vec<_>>>()?;
                Ok(vec![Self::Message {
                    role: ResponsesMessageRole::User,
                    content,
                    status: None,
                }])
            }
            ModelRole::Assistant => message
                .content
                .iter()
                .map(|block| match block {
                    ModelBlock::Text { text } => Ok(Self::Message {
                        role: ResponsesMessageRole::Assistant,
                        content: vec![ResponsesInputContent::OutputText {
                            text: text.clone(),
                            annotations: Vec::new(),
                        }],
                        status: Some("completed"),
                    }),
                    ModelBlock::ToolUse { id, name, input } => {
                        let name = provider_name_for_internal(name).ok_or_else(|| {
                            AppError::Provider(format!(
                                "model message contained unknown tool {name}"
                            ))
                        })?;
                        Ok(Self::FunctionCall {
                            call_id: id.clone(),
                            name: name.into(),
                            arguments: serde_json::to_string(input).map_err(|_| {
                                AppError::Provider(
                                    "model message contained invalid tool arguments".into(),
                                )
                            })?,
                        })
                    }
                    ModelBlock::ToolResult { .. } => Err(AppError::Provider(
                        "assistant message contained a tool result".into(),
                    )),
                })
                .collect(),
            ModelRole::Tool => {
                let items = message
                    .content
                    .iter()
                    .map(|block| match block {
                        ModelBlock::ToolResult {
                            tool_call_id,
                            content,
                            ..
                        } => Ok(Self::FunctionCallOutput {
                            call_id: tool_call_id.clone(),
                            output: content.clone(),
                        }),
                        ModelBlock::Text { .. } | ModelBlock::ToolUse { .. } => Err(
                            AppError::Provider("tool message contained a non-result block".into()),
                        ),
                    })
                    .collect::<AppResult<Vec<_>>>()?;
                if items.is_empty() {
                    return Err(AppError::Provider(
                        "tool message did not contain a tool result".into(),
                    ));
                }
                Ok(items)
            }
        }
    }
}

impl From<&ToolSpec> for ResponsesTool {
    fn from(spec: &ToolSpec) -> Self {
        Self {
            tool_type: "function",
            name: spec.name.clone(),
            description: spec.description.clone(),
            parameters: spec.input_schema.clone(),
        }
    }
}

pub(super) fn parse_responses_response(reader: impl Read) -> AppResult<ModelResponse> {
    let body = read_non_stream_body(reader)?;
    serde_json::from_slice::<ResponsesResponse>(&body)
        .map_err(|_| AppError::Provider(INVALID_RESPONSE_ERROR.into()))?
        .into_model_response()
}

#[derive(Debug, Deserialize)]
struct ResponsesResponse {
    status: ResponsesStatus,
    #[serde(default)]
    output: Vec<ResponsesOutputItem>,
    model: Option<ModelName>,
    usage: Option<ResponsesUsage>,
    incomplete_details: Option<ResponsesIncompleteDetails>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum ResponsesStatus {
    Completed,
    Incomplete,
    Failed,
    Cancelled,
    InProgress,
    Queued,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponsesOutputItem {
    Message {
        role: ResponsesOutputRole,
        status: Option<ResponsesItemStatus>,
        #[serde(default)]
        content: Vec<ResponsesOutputContent>,
    },
    FunctionCall {
        call_id: String,
        name: String,
        arguments: String,
        status: Option<ResponsesItemStatus>,
    },
    Reasoning {},
    #[serde(other)]
    Unsupported,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum ResponsesOutputRole {
    Assistant,
    #[serde(other)]
    Unsupported,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum ResponsesItemStatus {
    Completed,
    InProgress,
    Incomplete,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponsesOutputContent {
    OutputText {
        text: String,
    },
    Refusal {
        refusal: String,
    },
    #[serde(other)]
    Unsupported,
}

#[derive(Debug, Deserialize)]
struct ResponsesUsage {
    input_tokens: Option<u32>,
    output_tokens: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct ResponsesIncompleteDetails {
    reason: ResponsesIncompleteReason,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ResponsesIncompleteReason {
    MaxOutputTokens,
    ContentFilter,
    #[serde(other)]
    Unknown,
}

impl ResponsesResponse {
    fn into_model_response(self) -> AppResult<ModelResponse> {
        let response_incomplete = matches!(self.status, ResponsesStatus::Incomplete);
        let stop = match self.status {
            ResponsesStatus::Completed => None,
            ResponsesStatus::Incomplete => match self
                .incomplete_details
                .map(|details| details.reason)
            {
                Some(ResponsesIncompleteReason::MaxOutputTokens) => Some(ModelStop::MaxOutput),
                Some(ResponsesIncompleteReason::ContentFilter) => Some(ModelStop::ContentFilter),
                Some(ResponsesIncompleteReason::Unknown) | None => {
                    return Err(AppError::Provider(INVALID_RESPONSE_ERROR.into()));
                }
            },
            ResponsesStatus::Failed | ResponsesStatus::Cancelled => {
                return Err(AppError::Provider(
                    "provider Responses request did not complete".into(),
                ));
            }
            ResponsesStatus::InProgress | ResponsesStatus::Queued | ResponsesStatus::Unknown => {
                return Err(AppError::Provider(INVALID_RESPONSE_ERROR.into()));
            }
        };

        let mut content = Vec::new();
        let mut text_bytes = 0_usize;
        let mut tool_arguments_bytes = 0_usize;
        let mut saw_text = false;
        let mut saw_tool = false;
        let mut saw_refusal = false;
        for item in self.output {
            match item {
                ResponsesOutputItem::Message {
                    role,
                    status,
                    content: parts,
                } => {
                    let status_allowed = status == Some(ResponsesItemStatus::Completed)
                        || (response_incomplete && status == Some(ResponsesItemStatus::Incomplete));
                    if role != ResponsesOutputRole::Assistant || !status_allowed {
                        return Err(AppError::Provider(INVALID_RESPONSE_ERROR.into()));
                    }
                    for part in parts {
                        match part {
                            ResponsesOutputContent::OutputText { text } => {
                                text_bytes =
                                    text_bytes.checked_add(text.len()).ok_or_else(|| {
                                        AppError::Provider(ASSISTANT_TEXT_LIMIT_ERROR.into())
                                    })?;
                                if text_bytes > MAX_ASSISTANT_TEXT_BYTES {
                                    return Err(AppError::Provider(
                                        ASSISTANT_TEXT_LIMIT_ERROR.into(),
                                    ));
                                }
                                if !text.is_empty() {
                                    saw_text = true;
                                    content.push(ModelBlock::Text { text });
                                }
                            }
                            ResponsesOutputContent::Refusal { refusal } => {
                                let _ = refusal;
                                saw_refusal = true;
                            }
                            ResponsesOutputContent::Unsupported => {
                                return Err(AppError::Provider(INVALID_RESPONSE_ERROR.into()));
                            }
                        }
                    }
                }
                ResponsesOutputItem::FunctionCall {
                    call_id,
                    name,
                    arguments,
                    status,
                } => {
                    if !matches!(status, None | Some(ResponsesItemStatus::Completed)) {
                        return Err(AppError::Provider(INVALID_RESPONSE_ERROR.into()));
                    }
                    if content
                        .iter()
                        .filter(|block| matches!(block, ModelBlock::ToolUse { .. }))
                        .count()
                        == MAX_TOOL_CALLS
                    {
                        return Err(AppError::Provider(TOOL_CALL_LIMIT_ERROR.into()));
                    }
                    tool_arguments_bytes = tool_arguments_bytes
                        .checked_add(arguments.len())
                        .ok_or_else(|| AppError::Provider(TOOL_ARGUMENTS_LIMIT_ERROR.into()))?;
                    if tool_arguments_bytes > MAX_TOOL_ARGUMENTS_BYTES {
                        return Err(AppError::Provider(TOOL_ARGUMENTS_LIMIT_ERROR.into()));
                    }
                    content.push(tool_use_from_provider(call_id, name, arguments)?);
                    saw_tool = true;
                }
                ResponsesOutputItem::Reasoning {} => {}
                ResponsesOutputItem::Unsupported => {
                    return Err(AppError::Provider(INVALID_RESPONSE_ERROR.into()));
                }
            }
        }

        let stop = if saw_refusal {
            ModelStop::ContentFilter
        } else if let Some(stop) = stop {
            stop
        } else if saw_tool {
            ModelStop::ToolUse
        } else if saw_text {
            ModelStop::EndTurn
        } else {
            return Err(AppError::Provider(INVALID_RESPONSE_ERROR.into()));
        };
        Ok(ModelResponse {
            content,
            stop,
            served_model: self.model,
            usage: self.usage.and_then(ResponsesUsage::into_model_usage),
        })
    }
}

impl ResponsesUsage {
    fn into_model_usage(self) -> Option<ModelUsage> {
        Some(ModelUsage {
            input_tokens: self.input_tokens?,
            output_tokens: self.output_tokens?,
        })
    }
}

pub(super) fn parse_responses_stream(
    reader: impl BufRead,
    on_delta: &mut impl FnMut(&str) -> AppResult<()>,
) -> AppResult<ModelResponse> {
    let mut state = ResponsesStreamState::default();
    parse_sse_stream(reader, |data| state.apply_event(data, on_delta))?;
    state.terminal.ok_or_else(|| {
        AppError::Provider("provider Responses stream ended before a terminal response".into())
    })
}

#[derive(Default)]
struct ResponsesStreamState {
    text_bytes: usize,
    argument_bytes: usize,
    arguments: BTreeMap<usize, String>,
    terminal: Option<ModelResponse>,
}

impl ResponsesStreamState {
    fn apply_event(
        &mut self,
        data: &str,
        on_delta: &mut impl FnMut(&str) -> AppResult<()>,
    ) -> AppResult<bool> {
        if data.trim() == "[DONE]" {
            return Ok(true);
        }
        let value: Value = serde_json::from_str(data)
            .map_err(|_| AppError::Provider(INVALID_STREAM_EVENT_ERROR.into()))?;
        let event_type = value
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| AppError::Provider(INVALID_STREAM_EVENT_ERROR.into()))?
            .to_owned();
        match event_type.as_str() {
            "response.output_text.delta" => {
                let event: ResponsesTextDelta = serde_json::from_value(value)
                    .map_err(|_| AppError::Provider(INVALID_STREAM_EVENT_ERROR.into()))?;
                self.text_bytes = self
                    .text_bytes
                    .checked_add(event.delta.len())
                    .ok_or_else(|| AppError::Provider(ASSISTANT_TEXT_LIMIT_ERROR.into()))?;
                if self.text_bytes > MAX_ASSISTANT_TEXT_BYTES {
                    return Err(AppError::Provider(ASSISTANT_TEXT_LIMIT_ERROR.into()));
                }
                if !event.delta.is_empty() {
                    on_delta(&event.delta)?;
                }
            }
            "response.function_call_arguments.delta" => {
                let event: ResponsesArgumentsDelta = serde_json::from_value(value)
                    .map_err(|_| AppError::Provider(INVALID_STREAM_EVENT_ERROR.into()))?;
                self.push_arguments(event.output_index, &event.delta)?;
            }
            "response.function_call_arguments.done" => {
                let event: ResponsesArgumentsDone = serde_json::from_value(value)
                    .map_err(|_| AppError::Provider(INVALID_STREAM_EVENT_ERROR.into()))?;
                match self.arguments.get(&event.output_index) {
                    Some(arguments) if arguments != &event.arguments => {
                        return Err(AppError::Provider(STREAM_ARGUMENT_MISMATCH_ERROR.into()));
                    }
                    Some(_) => {}
                    None => self.push_arguments(event.output_index, &event.arguments)?,
                }
            }
            "response.completed"
            | "response.incomplete"
            | "response.failed"
            | "response.cancelled"
            | "response.done" => {
                let event: ResponsesTerminalEvent = serde_json::from_value(value)
                    .map_err(|_| AppError::Provider(INVALID_STREAM_EVENT_ERROR.into()))?;
                let status_matches = matches!(
                    (event_type.as_str(), &event.response.status),
                    (
                        "response.completed" | "response.done",
                        ResponsesStatus::Completed
                    ) | ("response.incomplete", ResponsesStatus::Incomplete)
                        | ("response.failed", ResponsesStatus::Failed)
                        | ("response.cancelled", ResponsesStatus::Cancelled)
                );
                if !status_matches {
                    return Err(AppError::Provider(INVALID_STREAM_EVENT_ERROR.into()));
                }
                self.terminal = Some(event.response.into_model_response()?);
                return Ok(true);
            }
            "error" | "response.error" => {
                return Err(AppError::Provider(STREAM_ERROR_EVENT_ERROR.into()));
            }
            "response.created"
            | "response.queued"
            | "response.in_progress"
            | "response.output_item.added"
            | "response.output_item.done"
            | "response.content_part.added"
            | "response.content_part.done"
            | "response.output_text.done"
            | "response.refusal.delta"
            | "response.refusal.done"
            | "response.reasoning_summary_part.added"
            | "response.reasoning_summary_part.done"
            | "response.reasoning_summary_text.delta"
            | "response.reasoning_summary_text.done"
            | "response.reasoning_text.delta"
            | "response.reasoning_text.done" => {}
            _ => return Err(AppError::Provider(UNKNOWN_STREAM_EVENT_ERROR.into())),
        }
        Ok(false)
    }

    fn push_arguments(&mut self, output_index: usize, delta: &str) -> AppResult<()> {
        if !self.arguments.contains_key(&output_index) && self.arguments.len() == MAX_TOOL_CALLS {
            return Err(AppError::Provider(TOOL_CALL_LIMIT_ERROR.into()));
        }
        let current = self.arguments.get(&output_index).map_or(0, String::len);
        if current
            .checked_add(delta.len())
            .is_none_or(|bytes| bytes > MAX_TOOL_ARGUMENT_BYTES)
        {
            return Err(AppError::Provider(TOOL_ARGUMENT_LIMIT_ERROR.into()));
        }
        self.argument_bytes = self
            .argument_bytes
            .checked_add(delta.len())
            .ok_or_else(|| AppError::Provider(TOOL_ARGUMENTS_LIMIT_ERROR.into()))?;
        if self.argument_bytes > MAX_TOOL_ARGUMENTS_BYTES {
            return Err(AppError::Provider(TOOL_ARGUMENTS_LIMIT_ERROR.into()));
        }
        self.arguments
            .entry(output_index)
            .or_default()
            .push_str(delta);
        Ok(())
    }
}

#[derive(Deserialize)]
struct ResponsesTextDelta {
    delta: String,
}

#[derive(Deserialize)]
struct ResponsesArgumentsDelta {
    output_index: usize,
    delta: String,
}

#[derive(Deserialize)]
struct ResponsesArgumentsDone {
    output_index: usize,
    arguments: String,
}

#[derive(Deserialize)]
struct ResponsesTerminalEvent {
    response: ResponsesResponse,
}

#[cfg(test)]
mod tests {
    use super::super::client::tests::test_provider_config;
    use super::*;
    use crate::{
        config::ProviderProtocol, model::ReasoningEffort,
        provider::openai_compat::OpenAiCompatibleClient,
    };
    use serde_json::json;
    use std::{
        io::{Cursor, ErrorKind, Read, Write},
        net::{TcpListener, TcpStream},
        sync::atomic::AtomicBool,
        thread,
        time::Duration,
    };

    #[test]
    fn request_fixture_maps_self_contained_history_tools_and_reasoning() {
        let request = ModelRequest {
            model: "test-model".into(),
            system: "system instructions".into(),
            max_output_tokens: 42,
            reasoning_effort: Some(ReasoningEffort::High),
            messages: vec![
                ModelMessage::user_text("read the file"),
                ModelMessage::assistant_blocks(vec![
                    ModelBlock::Text {
                        text: "I will read it.".into(),
                    },
                    ModelBlock::ToolUse {
                        id: "call_1".into(),
                        name: "file.read".into(),
                        input: json!({"path": "README.md"}),
                    },
                ]),
                ModelMessage::tool_result("call_1".into(), "contents".into(), false),
            ],
            tools: vec![ToolSpec {
                name: "file_read".into(),
                description: "Read a file".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {"path": {"type": "string"}},
                    "required": ["path"]
                }),
            }],
        };

        let value =
            serde_json::to_value(ResponsesRequest::from_model_request(&request).unwrap()).unwrap();

        assert_eq!(
            value,
            json!({
                "model": "test-model",
                "instructions": "system instructions",
                "input": [
                    {
                        "type": "message",
                        "role": "user",
                        "content": [{"type": "input_text", "text": "read the file"}]
                    },
                    {
                        "type": "message",
                        "role": "assistant",
                        "status": "completed",
                        "content": [{
                            "type": "output_text",
                            "text": "I will read it.",
                            "annotations": []
                        }]
                    },
                    {
                        "type": "function_call",
                        "call_id": "call_1",
                        "name": "file_read",
                        "arguments": "{\"path\":\"README.md\"}"
                    },
                    {
                        "type": "function_call_output",
                        "call_id": "call_1",
                        "output": "contents"
                    }
                ],
                "tools": [{
                    "type": "function",
                    "name": "file_read",
                    "description": "Read a file",
                    "parameters": {
                        "type": "object",
                        "properties": {"path": {"type": "string"}},
                        "required": ["path"]
                    }
                }],
                "tool_choice": "auto",
                "parallel_tool_calls": false,
                "max_output_tokens": 42,
                "reasoning": {"effort": "high"},
                "store": false
            })
        );
        assert!(value.get("previous_response_id").is_none());
        assert!(value.get("stream").is_none());
    }

    #[test]
    fn non_stream_fixture_maps_text_tool_usage_and_served_model() {
        let text = parse_responses_response(Cursor::new(
            json!({
                "status": "completed",
                "model": "provider/served-model",
                "output": [
                    {"type": "reasoning", "summary": []},
                    {
                        "type": "message",
                        "role": "assistant",
                        "status": "completed",
                        "content": [{"type": "output_text", "text": "done", "annotations": []}]
                    }
                ],
                "usage": {"input_tokens": 10, "output_tokens": 5, "total_tokens": 15}
            })
            .to_string(),
        ))
        .unwrap();
        assert_eq!(text.text(), "done");
        assert_eq!(text.stop, ModelStop::EndTurn);
        assert_eq!(
            text.served_model,
            Some(ModelName::new("provider/served-model").unwrap())
        );
        assert_eq!(
            text.usage,
            Some(ModelUsage {
                input_tokens: 10,
                output_tokens: 5,
            })
        );

        let tool = parse_responses_response(Cursor::new(
            json!({
                "status": "completed",
                "model": "provider/served-model",
                "output": [{
                    "type": "function_call",
                    "status": "completed",
                    "call_id": "call_1",
                    "name": "file_read",
                    "arguments": "{\"path\":\"README.md\"}"
                }]
            })
            .to_string(),
        ))
        .unwrap();
        assert_eq!(tool.stop, ModelStop::ToolUse);
        assert_eq!(
            tool.tool_uses(),
            vec![(
                "call_1".into(),
                "file.read".into(),
                json!({"path": "README.md"})
            )]
        );
    }

    #[test]
    fn terminal_fixtures_map_limits_and_refusals_and_fail_closed() {
        for (fixture, expected) in [
            (
                json!({
                    "status": "incomplete",
                    "incomplete_details": {"reason": "max_output_tokens"},
                    "output": [{
                        "type": "message",
                        "role": "assistant",
                        "status": "incomplete",
                        "content": [{"type": "output_text", "text": "partial"}]
                    }]
                }),
                ModelStop::MaxOutput,
            ),
            (
                json!({
                    "status": "incomplete",
                    "incomplete_details": {"reason": "content_filter"},
                    "output": []
                }),
                ModelStop::ContentFilter,
            ),
            (
                json!({
                    "status": "completed",
                    "output": [{
                        "type": "message",
                        "role": "assistant",
                        "status": "completed",
                        "content": [{"type": "refusal", "refusal": "cannot comply"}]
                    }]
                }),
                ModelStop::ContentFilter,
            ),
        ] {
            let response = parse_responses_response(Cursor::new(fixture.to_string())).unwrap();
            assert_eq!(response.stop, expected);
        }

        for fixture in [
            json!({"status": "failed", "error": {"message": "secret failure"}}),
            json!({"status": "cancelled", "error": {"message": "secret cancellation"}}),
            json!({"status": "future_terminal", "output": []}),
            json!({
                "status": "completed",
                "output": [{"type": "web_search_call", "id": "secret output"}]
            }),
            json!({
                "status": "completed",
                "output": [{
                    "type": "message",
                    "role": "assistant",
                    "status": "in_progress",
                    "content": [{"type": "output_text", "text": "secret partial"}]
                }]
            }),
        ] {
            let error = parse_responses_response(Cursor::new(fixture.to_string())).unwrap_err();
            assert!(!error.to_string().contains("secret"));
        }
        let error = parse_responses_response(Cursor::new("{not json secret}")).unwrap_err();
        assert_eq!(
            error.to_string(),
            format!("provider error: {INVALID_RESPONSE_ERROR}")
        );
    }

    #[test]
    fn streaming_fixture_uses_deltas_but_terminal_response_for_final_facts() {
        let terminal = json!({
            "type": "response.completed",
            "response": {
                "status": "completed",
                "model": "provider/final-model",
                "output": [
                    {
                        "type": "message",
                        "role": "assistant",
                        "status": "completed",
                        "content": [{
                            "type": "output_text",
                            "text": "authoritative final",
                            "annotations": []
                        }]
                    },
                    {
                        "type": "function_call",
                        "status": "completed",
                        "call_id": "call_1",
                        "name": "file_read",
                        "arguments": "{\"path\":\"README.md\"}"
                    }
                ],
                "usage": {"input_tokens": 8, "output_tokens": 3}
            }
        });
        let raw = [
            sse(json!({"type": "response.created", "response": {"status": "in_progress"}})),
            sse(json!({"type": "response.output_text.delta", "delta": "Hel"})),
            sse(json!({"type": "response.output_text.delta", "delta": "lo"})),
            sse(json!({
                "type": "response.function_call_arguments.delta",
                "output_index": 1,
                "delta": "{\"path\":"
            })),
            sse(json!({
                "type": "response.function_call_arguments.delta",
                "output_index": 1,
                "delta": "\"README.md\"}"
            })),
            sse(json!({
                "type": "response.function_call_arguments.done",
                "output_index": 1,
                "arguments": "{\"path\":\"README.md\"}"
            })),
            sse(terminal),
        ]
        .concat();
        let mut deltas = Vec::new();

        let response = parse_responses_stream(Cursor::new(raw), &mut |delta| {
            deltas.push(delta.to_owned());
            Ok(())
        })
        .unwrap();

        assert_eq!(deltas, ["Hel", "lo"]);
        assert_eq!(response.text(), "authoritative final");
        assert_eq!(response.stop, ModelStop::ToolUse);
        assert_eq!(
            response.served_model,
            Some(ModelName::new("provider/final-model").unwrap())
        );
        assert_eq!(
            response.usage,
            Some(ModelUsage {
                input_tokens: 8,
                output_tokens: 3,
            })
        );
        assert_eq!(response.tool_uses()[0].0, "call_1");
    }

    #[test]
    fn streaming_unknown_error_and_malformed_events_fail_without_body_details() {
        for raw in [
            sse(json!({"type": "response.future_event", "secret": "future-secret"})),
            sse(json!({"type": "error", "error": {"message": "error-secret"}})),
            sse(json!({
                "type": "response.completed",
                "response": {"status": "failed", "error": {"message": "status-secret"}}
            })),
            "data: {not-json-secret}\n\n".into(),
        ] {
            let error = parse_responses_stream(Cursor::new(raw), &mut |_| Ok(())).unwrap_err();
            assert!(!error.to_string().contains("secret"));
        }

        let raw = [
            sse(json!({
                "type": "response.function_call_arguments.delta",
                "output_index": 0,
                "delta": "{}"
            })),
            sse(json!({
                "type": "response.function_call_arguments.done",
                "output_index": 0,
                "arguments": "{\"different\":true}"
            })),
        ]
        .concat();
        let error = parse_responses_stream(Cursor::new(raw), &mut |_| Ok(())).unwrap_err();
        assert_eq!(
            error.to_string(),
            format!("provider error: {STREAM_ARGUMENT_MISMATCH_ERROR}")
        );
    }

    #[test]
    fn selected_protocol_posts_only_its_exact_endpoint() {
        for (protocol, endpoint, response_body) in [
            (
                ProviderProtocol::default(),
                "/chat/completions",
                json!({
                    "choices": [{
                        "finish_reason": "stop",
                        "message": {"content": "chat"}
                    }]
                })
                .to_string(),
            ),
            (
                ProviderProtocol::Responses,
                "/responses",
                json!({
                    "status": "completed",
                    "output": [{
                        "type": "message",
                        "role": "assistant",
                        "status": "completed",
                        "content": [{"type": "output_text", "text": "responses"}]
                    }]
                })
                .to_string(),
            ),
        ] {
            let (base_url, server) = spawn_provider(move |mut stream| {
                let (request_line, body) = read_request(&mut stream);
                assert_eq!(request_line, format!("POST {endpoint} HTTP/1.1"));
                if protocol == ProviderProtocol::Responses {
                    assert_eq!(body["store"], false);
                    assert!(body.get("previous_response_id").is_none());
                }
                write_json_response(&mut stream, 200, "OK", &response_body);
            });

            let response = provider_client(base_url, protocol)
                .send(&simple_request())
                .unwrap();
            server.join().unwrap();
            assert!(!response.text().is_empty());
        }
    }

    #[test]
    fn responses_http_error_is_secret_safe_and_does_not_fallback() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let (request_line, _) = read_request(&mut stream);
            assert_eq!(request_line, "POST /responses HTTP/1.1");
            write_json_response(
                &mut stream,
                500,
                "Internal Server Error",
                "provider-secret-body",
            );
            listener.set_nonblocking(true).unwrap();
            assert!(
                matches!(listener.accept(), Err(error) if error.kind() == ErrorKind::WouldBlock)
            );
        });

        let error = provider_client(base_url, ProviderProtocol::Responses)
            .send(&simple_request())
            .unwrap_err();
        server.join().unwrap();

        assert_eq!(
            error.to_string(),
            "provider error: provider returned http 500"
        );
        assert!(!error.to_string().contains("secret"));
    }

    #[test]
    fn responses_stream_honors_existing_caller_cancellation() {
        let terminal = sse(json!({
            "type": "response.completed",
            "response": {
                "status": "completed",
                "output": [{
                    "type": "message",
                    "role": "assistant",
                    "status": "completed",
                    "content": [{"type": "output_text", "text": "done"}]
                }]
            }
        }));
        let body = format!(
            "{}{terminal}",
            sse(json!({"type": "response.output_text.delta", "delta": "ignored"}))
        );
        let (base_url, server) = spawn_provider(move |mut stream| {
            read_request(&mut stream);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        });
        let cancel = AtomicBool::new(true);
        let mut deltas = Vec::new();

        let result = provider_client(base_url, ProviderProtocol::Responses)
            .send_streaming_with_cancel(&simple_request(), Some(&cancel), |delta| {
                deltas.push(delta.to_owned());
                Ok(())
            });
        server.join().unwrap();

        assert!(matches!(result, Err(AppError::RunCanceled)));
        assert!(deltas.is_empty());
    }

    fn sse(value: Value) -> String {
        format!("data: {value}\n\n")
    }

    fn simple_request() -> ModelRequest {
        ModelRequest {
            model: "test-model".into(),
            system: "system".into(),
            max_output_tokens: 32,
            reasoning_effort: None,
            messages: vec![ModelMessage::user_text("hello")],
            tools: Vec::new(),
        }
    }

    fn provider_client(base_url: String, protocol: ProviderProtocol) -> OpenAiCompatibleClient {
        let config = test_provider_config(base_url, protocol);
        temp_env::with_var(&config.api_key_env, Some("test-key"), || {
            OpenAiCompatibleClient::from_config(&config).unwrap()
        })
    }

    fn spawn_provider(
        handler: impl FnOnce(TcpStream) + Send + 'static,
    ) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let handle = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            handler(stream);
        });
        (base_url, handle)
    }

    fn read_request(stream: &mut TcpStream) -> (String, Value) {
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut received = Vec::new();
        let mut buffer = [0_u8; 4096];
        let header_end = loop {
            let count = stream.read(&mut buffer).unwrap();
            assert_ne!(count, 0, "provider request ended before headers");
            received.extend_from_slice(&buffer[..count]);
            if let Some(index) = received.windows(4).position(|window| window == b"\r\n\r\n") {
                break index + 4;
            }
        };
        let headers = String::from_utf8_lossy(&received[..header_end]).into_owned();
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .and_then(|value| value.trim().parse::<usize>().ok())
            })
            .unwrap();
        while received.len() - header_end < content_length {
            let count = stream.read(&mut buffer).unwrap();
            assert_ne!(count, 0, "provider request ended before body");
            received.extend_from_slice(&buffer[..count]);
        }
        let request_line = headers.lines().next().unwrap().to_owned();
        let body =
            serde_json::from_slice(&received[header_end..header_end + content_length]).unwrap();
        (request_line, body)
    }

    fn write_json_response(stream: &mut TcpStream, status: u16, reason: &str, body: &str) {
        write!(
            stream,
            "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
    }
}
