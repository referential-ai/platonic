use super::types::{
    ASSISTANT_TEXT_LIMIT_ERROR, ChatFinishReason, ChatUsage, MAX_ASSISTANT_TEXT_BYTES,
    MAX_TOOL_ARGUMENT_BYTES, MAX_TOOL_ARGUMENTS_BYTES, MAX_TOOL_CALL_INDEX, MAX_TOOL_CALLS,
    MAX_TOOL_NAME_BYTES, TOOL_ARGUMENT_LIMIT_ERROR, TOOL_ARGUMENTS_LIMIT_ERROR,
    TOOL_CALL_LIMIT_ERROR, TOOL_NAME_LIMIT_ERROR, model_response, tool_use_from_provider,
};
use crate::{
    AppError, AppResult,
    model::{ModelBlock, ModelResponse},
};
use platonic_core::ModelName;
use serde::Deserialize;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    io::{self, BufRead, Read},
};

const MAX_DECODED_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MAX_SSE_EVENT_BYTES: usize = 1024 * 1024;
const DECODED_RESPONSE_LIMIT_ERROR: &str =
    "provider response exceeded the 8 MiB decoded data limit";
const SSE_EVENT_LIMIT_ERROR: &str = "provider response exceeded the 1 MiB SSE event limit";
const CONFLICTING_SERVED_MODEL_ERROR: &str =
    "provider stream returned conflicting served model values";
const STREAM_ERROR_EVENT_ERROR: &str = "provider stream returned an error event";

#[derive(Debug, Deserialize)]
struct ChatCompletionChunk {
    model: Option<ModelName>,
    #[serde(default)]
    choices: Vec<ChatChunkChoice>,
    usage: Option<ChatUsage>,
}

#[derive(Debug, Deserialize)]
struct ChatChunkChoice {
    #[serde(default)]
    index: usize,
    #[serde(default)]
    delta: ChatDelta,
    finish_reason: Option<ChatFinishReason>,
}

#[derive(Debug, Default, Deserialize)]
struct ChatDelta {
    content: Option<String>,
    #[serde(default)]
    tool_calls: Vec<ChatToolCallDelta>,
}

#[derive(Debug, Deserialize)]
struct ChatToolCallDelta {
    index: usize,
    id: Option<String>,
    function: Option<ChatFunctionCallDelta>,
}

#[derive(Debug, Deserialize)]
struct ChatFunctionCallDelta {
    name: Option<String>,
    arguments: Option<String>,
}

#[derive(Default)]
struct StreamingAssembler {
    text: String,
    tool_calls: BTreeMap<usize, StreamingToolCall>,
    tool_arguments_bytes: usize,
    finish_reason: Option<ChatFinishReason>,
    served_model: Option<ModelName>,
    usage: Option<ChatUsage>,
}

#[derive(Default)]
struct StreamingToolCall {
    id: Option<String>,
    name: String,
    arguments: String,
}

impl StreamingAssembler {
    fn apply_chunk(
        &mut self,
        chunk: ChatCompletionChunk,
        on_delta: &mut impl FnMut(&str) -> AppResult<()>,
    ) -> AppResult<()> {
        if let Some(served_model) = chunk.model {
            match &self.served_model {
                Some(first) if first != &served_model => {
                    return Err(AppError::Provider(CONFLICTING_SERVED_MODEL_ERROR.into()));
                }
                Some(_) => {}
                None => self.served_model = Some(served_model),
            }
        }
        if let Some(usage) = chunk.usage {
            self.usage = Some(usage);
        }
        for choice in chunk.choices {
            if choice.index != 0 {
                continue;
            }
            if let Some(text) = choice.delta.content.filter(|text| !text.is_empty()) {
                if self
                    .text
                    .len()
                    .checked_add(text.len())
                    .is_none_or(|bytes| bytes > MAX_ASSISTANT_TEXT_BYTES)
                {
                    return Err(AppError::Provider(ASSISTANT_TEXT_LIMIT_ERROR.into()));
                }
                on_delta(&text)?;
                self.text.push_str(&text);
            }
            for tool_call in choice.delta.tool_calls {
                if tool_call.index > MAX_TOOL_CALL_INDEX {
                    return Err(AppError::Provider(TOOL_CALL_LIMIT_ERROR.into()));
                }
                if !self.tool_calls.contains_key(&tool_call.index)
                    && self.tool_calls.len() == MAX_TOOL_CALLS
                {
                    return Err(AppError::Provider(TOOL_CALL_LIMIT_ERROR.into()));
                }

                let current = self.tool_calls.get(&tool_call.index);
                let added_arguments_bytes = tool_call
                    .function
                    .as_ref()
                    .and_then(|function| function.arguments.as_ref())
                    .map_or(0, String::len);
                if let Some(function) = &tool_call.function {
                    if let Some(name) = &function.name
                        && current
                            .map_or(0, |call| call.name.len())
                            .checked_add(name.len())
                            .is_none_or(|bytes| bytes > MAX_TOOL_NAME_BYTES)
                    {
                        return Err(AppError::Provider(TOOL_NAME_LIMIT_ERROR.into()));
                    }
                    if current
                        .map_or(0, |call| call.arguments.len())
                        .checked_add(added_arguments_bytes)
                        .is_none_or(|bytes| bytes > MAX_TOOL_ARGUMENT_BYTES)
                    {
                        return Err(AppError::Provider(TOOL_ARGUMENT_LIMIT_ERROR.into()));
                    }
                    if self
                        .tool_arguments_bytes
                        .checked_add(added_arguments_bytes)
                        .is_none_or(|bytes| bytes > MAX_TOOL_ARGUMENTS_BYTES)
                    {
                        return Err(AppError::Provider(TOOL_ARGUMENTS_LIMIT_ERROR.into()));
                    }
                }

                let entry = self.tool_calls.entry(tool_call.index).or_default();
                if let Some(id) = tool_call.id.filter(|id| !id.is_empty()) {
                    entry.id = Some(id);
                }
                if let Some(function) = tool_call.function {
                    if let Some(name) = function.name {
                        entry.name.push_str(&name);
                    }
                    if let Some(arguments) = function.arguments {
                        entry.arguments.push_str(&arguments);
                    }
                }
                self.tool_arguments_bytes += added_arguments_bytes;
            }
            if let Some(reason) = choice.finish_reason {
                self.finish_reason = Some(reason);
            }
        }
        Ok(())
    }

    fn into_model_response(self) -> AppResult<ModelResponse> {
        let mut content = Vec::new();
        if !self.text.is_empty() {
            content.push(ModelBlock::Text { text: self.text });
        }
        for (_, call) in self.tool_calls {
            let id = call.id.ok_or_else(|| {
                AppError::Provider("provider stream returned tool call without id".into())
            })?;
            content.push(tool_use_from_provider(id, call.name, call.arguments)?);
        }
        let finish_reason = self.finish_reason.ok_or_else(|| {
            AppError::Provider("provider stream ended without finish_reason".into())
        })?;
        Ok(model_response(
            content,
            finish_reason,
            self.served_model,
            self.usage,
        ))
    }
}

pub(super) fn parse_chat_completion_stream(
    mut reader: impl BufRead,
    on_delta: &mut impl FnMut(&str) -> AppResult<()>,
) -> AppResult<ModelResponse> {
    let mut assembler = StreamingAssembler::default();
    let mut event_data = String::new();
    let mut line = Vec::new();
    let mut decoded_bytes = 0;
    let mut event_bytes = 0;
    let mut saw_done = false;

    loop {
        let remaining =
            (MAX_DECODED_RESPONSE_BYTES - decoded_bytes).min(MAX_SSE_EVENT_BYTES - event_bytes);
        let read = Read::by_ref(&mut reader)
            .take((remaining + 1) as u64)
            .read_until(b'\n', &mut line)?;
        if read == 0 {
            break;
        }
        decoded_bytes += read;
        if decoded_bytes > MAX_DECODED_RESPONSE_BYTES {
            return Err(AppError::Provider(DECODED_RESPONSE_LIMIT_ERROR.into()));
        }
        event_bytes += read;
        if event_bytes > MAX_SSE_EVENT_BYTES {
            return Err(AppError::Provider(SSE_EVENT_LIMIT_ERROR.into()));
        }

        if line.last() == Some(&b'\n') {
            line.pop();
            if append_stream_line(&line, &mut event_data)? {
                if !event_data.is_empty() {
                    if process_stream_data(&event_data, &mut assembler, on_delta)? {
                        saw_done = true;
                        break;
                    }
                    event_data.clear();
                }
                event_bytes = 0;
            }
            line.clear();
        }
    }

    if !line.is_empty() && !saw_done && append_stream_line(&line, &mut event_data)? {
        saw_done =
            !event_data.is_empty() && process_stream_data(&event_data, &mut assembler, on_delta)?;
        event_data.clear();
    }
    if !event_data.is_empty() && !saw_done {
        saw_done = process_stream_data(&event_data, &mut assembler, on_delta)?;
    }
    if !saw_done {
        return Err(AppError::Provider(
            "provider stream ended before [DONE]".into(),
        ));
    }
    assembler.into_model_response()
}

fn append_stream_line(line: &[u8], event_data: &mut String) -> AppResult<bool> {
    let line = std::str::from_utf8(line)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let line = line.trim_end_matches('\r');
    if line.is_empty() {
        return Ok(true);
    }
    if let Some(data) = line.strip_prefix("data:") {
        if !event_data.is_empty() {
            event_data.push('\n');
        }
        event_data.push_str(data.trim_start());
    }
    Ok(false)
}

fn process_stream_data(
    data: &str,
    assembler: &mut StreamingAssembler,
    on_delta: &mut impl FnMut(&str) -> AppResult<()>,
) -> AppResult<bool> {
    if data.trim() == "[DONE]" {
        return Ok(true);
    }
    let value: Value = serde_json::from_str(data).map_err(|error| {
        AppError::Provider(format!("provider returned invalid SSE JSON: {error}"))
    })?;
    if value.get("error").is_some() {
        return Err(AppError::Provider(STREAM_ERROR_EVENT_ERROR.into()));
    }
    let chunk = serde_json::from_value::<ChatCompletionChunk>(value).map_err(|error| {
        AppError::Provider(format!("provider returned invalid SSE chunk: {error}"))
    })?;
    assembler.apply_chunk(chunk, on_delta)?;
    Ok(false)
}

#[cfg(test)]
pub(super) mod tests {
    use super::super::types::{
        fragmented_unicode_json_arguments, json_arguments_with_bytes, usage_fixtures,
        utf8_string_with_bytes,
    };
    use super::*;
    use crate::model::ModelStop;
    use platonic_core::ModelUsage;
    use serde_json::json;
    use std::io::{BufReader, Cursor, ErrorKind};

    pub(in super::super) fn sse_delta(text: &str) -> String {
        format!(
            "data: {}\n\n",
            json!({
                "choices": [{
                    "index": 0,
                    "delta": {"content": text},
                    "finish_reason": null
                }]
            })
        )
    }

    pub(in super::super) fn sse_finish() -> String {
        concat!(
            "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
            "data: [DONE]\n\n",
        )
        .into()
    }
    #[test]
    fn streaming_text_assembles_final_response_and_emits_deltas() {
        let raw = concat!(
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hel\"},\"finish_reason\":null}]}\n\n",
            "data: {\"model\":\"provider/concrete-model-2026-08-01\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"lo\"},\"finish_reason\":null}]}\n\n",
            "data: {\"model\":\"provider/concrete-model-2026-08-01\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":2}}\n\n",
            "data: [DONE]\n\n",
        );
        let mut deltas = Vec::new();

        let response = parse_chat_completion_stream(Cursor::new(raw), &mut |delta| {
            deltas.push(delta.to_string());
            Ok(())
        })
        .unwrap();

        assert_eq!(deltas, vec!["Hel", "lo"]);
        assert_eq!(response.text(), "Hello");
        assert_eq!(response.stop, ModelStop::EndTurn);
        assert_eq!(
            response.served_model,
            Some(ModelName::new("provider/concrete-model-2026-08-01").unwrap())
        );
        assert_eq!(
            response.usage,
            Some(ModelUsage {
                input_tokens: 4,
                output_tokens: 2,
            })
        );
    }

    #[test]
    fn streaming_conflicting_served_models_fail_before_later_delta_or_success() {
        let raw = concat!(
            "data: {\"model\":\"provider/model-a\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"first\"},\"finish_reason\":null}]}\n\n",
            "data: {\"model\":\"provider/model-b\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"second\"},\"finish_reason\":null}]}\n\n",
            "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
            "data: [DONE]\n\n",
        );
        let mut deltas = Vec::new();

        let error = parse_chat_completion_stream(Cursor::new(raw), &mut |delta| {
            deltas.push(delta.to_owned());
            Ok(())
        })
        .unwrap_err();

        assert_eq!(deltas, ["first"]);
        assert!(matches!(
            error,
            AppError::Provider(message) if message == CONFLICTING_SERVED_MODEL_ERROR
        ));
    }

    #[test]
    fn streaming_error_event_body_is_not_exposed() {
        let secret = "provider-secret-error-body";
        let raw = format!(
            "data: {}\n\ndata: [DONE]\n\n",
            json!({"error": {"message": secret}})
        );

        let error = parse_chat_completion_stream(Cursor::new(raw), &mut |_| Ok(())).unwrap_err();

        assert_eq!(
            error.to_string(),
            format!("provider error: {STREAM_ERROR_EVENT_ERROR}")
        );
        assert!(!error.to_string().contains(secret));
    }

    #[test]
    fn streaming_usage_is_known_only_when_both_counts_are_reported() {
        for (fixture, raw_usage, expected) in usage_fixtures() {
            let mut raw = concat!(
                "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"done\"},\"finish_reason\":null}]}\n\n",
                "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
            )
            .to_string();
            if let Some(raw_usage) = raw_usage {
                raw.push_str(&format!(
                    "data: {}\n\n",
                    json!({"choices": [], "usage": raw_usage})
                ));
            }
            raw.push_str("data: [DONE]\n\n");

            let response = parse_chat_completion_stream(Cursor::new(raw), &mut |_| Ok(())).unwrap();

            assert_eq!(response.usage, expected, "fixture: {fixture}");
            assert_eq!(response.served_model, None, "fixture: {fixture}");
        }
    }

    #[test]
    fn streaming_tool_calls_assemble_without_text_deltas() {
        let raw = concat!(
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"file_read\",\"arguments\":\"{\\\"path\\\":\\\"README\"}}]},\"finish_reason\":null}]}\n\n",
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\".md\\\"}\"}}]},\"finish_reason\":null}]}\n\n",
            "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
            "data: [DONE]\n\n",
        );
        let mut deltas = Vec::new();

        let response = parse_chat_completion_stream(Cursor::new(raw), &mut |delta| {
            deltas.push(delta.to_string());
            Ok(())
        })
        .unwrap();

        assert!(deltas.is_empty());
        assert_eq!(response.stop, ModelStop::ToolUse);
        assert_eq!(response.usage, None);
        assert_eq!(
            response.tool_uses(),
            vec![(
                "call_1".into(),
                "file.read".into(),
                json!({"path": "README.md"})
            )]
        );
    }

    #[test]
    fn streaming_tool_calls_require_an_id() {
        let raw = concat!(
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"name\":\"file_read\",\"arguments\":\"{\\\"path\\\":\\\"README.md\\\"}\"}}]},\"finish_reason\":null}]}\n\n",
            "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
            "data: [DONE]\n\n",
        );

        let error = parse_chat_completion_stream(Cursor::new(raw), &mut |_| Ok(())).unwrap_err();

        assert!(matches!(
            error,
            AppError::Provider(message) if message == "provider stream returned tool call without id"
        ));
    }

    #[test]
    fn streaming_parser_rejects_adversarial_inputs() {
        let huge_event = format!(
            "data: {}\n\ndata: [DONE]\n\n",
            json!({"garbage": "x".repeat(1024 * 1024)})
        )
        .into_bytes();
        let cases = [
            b"data: not-json\n\n".to_vec(),
            b"data: {\"choices\":\"wrong shape\"}\n\n".to_vec(),
            b"data: {\"model\":\" \",\"choices\":[]}\n\n".to_vec(),
            b"data: {\"model\":7,\"choices\":[]}\n\n".to_vec(),
            b"data: {\"choices\":[\n\n".to_vec(),
            b"data: \xff\n\n".to_vec(),
            huge_event,
        ];

        for raw in cases {
            for capacity in [1, 2, 7, 64] {
                let reader = BufReader::with_capacity(capacity, Cursor::new(&raw));
                let mut deltas = Vec::new();
                let error = parse_chat_completion_stream(reader, &mut |delta| {
                    deltas.push(delta.to_string());
                    Ok(())
                })
                .unwrap_err();

                assert!(deltas.is_empty());
                match error {
                    AppError::Provider(_) => {}
                    AppError::Io(error) => assert_eq!(error.kind(), ErrorKind::InvalidData),
                    other => panic!("unexpected stream error: {other}"),
                }
            }
        }
    }

    #[test]
    fn streaming_parser_handles_split_utf8_and_ignores_non_data_fields() {
        let chunk = json!({
            "choices": [{
                "index": 0,
                "delta": {"content": "h\u{e9}\u{754c}"},
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
        let raw = format!(
            ": keepalive\nevent: message\nid: 1\nretry: 100\ngarbage\ndata: {chunk}\n\ndata: {finish}\n\ndata: [DONE]\n\n"
        );

        for capacity in 1..=4 {
            let reader = BufReader::with_capacity(capacity, Cursor::new(raw.as_bytes()));
            let mut deltas = Vec::new();
            let response = parse_chat_completion_stream(reader, &mut |delta| {
                deltas.push(delta.to_string());
                Ok(())
            })
            .unwrap();

            assert_eq!(deltas, ["h\u{e9}\u{754c}"]);
            assert_eq!(response.text(), "h\u{e9}\u{754c}");
            assert_eq!(response.stop, ModelStop::EndTurn);
        }
    }

    #[test]
    fn streaming_aggregate_decoded_limit_is_exact() {
        let exact = streaming_body_with_size(MAX_DECODED_RESPONSE_BYTES);
        let response = parse_chat_completion_stream(Cursor::new(exact), &mut |_| Ok(())).unwrap();
        assert_eq!(response.stop, ModelStop::EndTurn);

        let mut deltas = Vec::new();
        let error = parse_chat_completion_stream(
            Cursor::new(streaming_body_with_size(MAX_DECODED_RESPONSE_BYTES + 1)),
            &mut |delta| {
                deltas.push(delta.to_string());
                Ok(())
            },
        )
        .unwrap_err();
        assert!(deltas.is_empty());
        assert!(matches!(
            error,
            AppError::Provider(message) if message == DECODED_RESPONSE_LIMIT_ERROR
        ));
    }

    #[test]
    fn individual_sse_event_limit_is_exact_before_delta_emission() {
        let chunk = json!({
            "choices": [{
                "index": 0,
                "delta": {"content": "within-limit"},
                "finish_reason": null
            }]
        });
        let mut exact = sse_event_with_size(&chunk, MAX_SSE_EVENT_BYTES);
        exact.push_str(&sse_finish());
        let mut deltas = Vec::new();
        let response = parse_chat_completion_stream(Cursor::new(exact), &mut |delta| {
            deltas.push(delta.to_string());
            Ok(())
        })
        .unwrap();
        assert_eq!(deltas, ["within-limit"]);
        assert_eq!(response.text(), "within-limit");

        let mut over = sse_event_with_size(&chunk, MAX_SSE_EVENT_BYTES + 1);
        over.push_str(&sse_finish());
        let mut deltas = Vec::new();
        let error = parse_chat_completion_stream(Cursor::new(over), &mut |delta| {
            deltas.push(delta.to_string());
            Ok(())
        })
        .unwrap_err();
        assert!(deltas.is_empty());
        assert!(matches!(
            error,
            AppError::Provider(message) if message == SSE_EVENT_LIMIT_ERROR
        ));
    }

    #[test]
    fn fragmented_unicode_streaming_text_limit_is_exact_before_delta_emission() {
        let fragments = (0..8)
            .map(|_| utf8_string_with_bytes(MAX_ASSISTANT_TEXT_BYTES / 8))
            .collect::<Vec<_>>();
        let mut exact = fragments
            .iter()
            .map(|text| sse_delta(text))
            .collect::<String>();
        exact.push_str(&sse_finish());
        let mut deltas = Vec::new();
        let response = parse_chat_completion_stream(Cursor::new(exact), &mut |delta| {
            deltas.push(delta.to_string());
            Ok(())
        })
        .unwrap();
        assert_eq!(response.text().len(), MAX_ASSISTANT_TEXT_BYTES);
        assert_eq!(
            deltas.iter().map(String::len).sum::<usize>(),
            MAX_ASSISTANT_TEXT_BYTES
        );

        let mut over = fragments
            .iter()
            .map(|text| sse_delta(text))
            .collect::<String>();
        over.push_str(&sse_delta("x"));
        over.push_str(&sse_finish());
        let mut deltas = Vec::new();
        let error = parse_chat_completion_stream(Cursor::new(over), &mut |delta| {
            deltas.push(delta.to_string());
            Ok(())
        })
        .unwrap_err();
        assert_eq!(
            deltas.iter().map(String::len).sum::<usize>(),
            MAX_ASSISTANT_TEXT_BYTES
        );
        assert!(matches!(
            error,
            AppError::Provider(message) if message == ASSISTANT_TEXT_LIMIT_ERROR
        ));
    }
    #[test]
    fn streaming_tool_name_and_argument_limits_count_assembled_utf8_bytes() {
        let mut name_assembler = StreamingAssembler::default();
        for (index, fragment) in [
            utf8_string_with_bytes(MAX_TOOL_NAME_BYTES / 2),
            utf8_string_with_bytes(MAX_TOOL_NAME_BYTES / 2),
        ]
        .into_iter()
        .enumerate()
        {
            name_assembler
                .apply_chunk(
                    tool_delta_chunk(
                        0,
                        (index == 0).then_some("call_0"),
                        Some(fragment),
                        (index == 0).then_some("{}".into()),
                    ),
                    &mut |_| Ok(()),
                )
                .unwrap();
        }
        assert_eq!(
            name_assembler.tool_calls.get(&0).unwrap().name.len(),
            MAX_TOOL_NAME_BYTES
        );
        let error = name_assembler
            .apply_chunk(
                tool_delta_chunk(0, None, Some("x".into()), None),
                &mut |_| Ok(()),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            AppError::Provider(message) if message == TOOL_NAME_LIMIT_ERROR
        ));
        assert_eq!(
            name_assembler.tool_calls.get(&0).unwrap().name.len(),
            MAX_TOOL_NAME_BYTES
        );

        let argument_fragments = fragmented_unicode_json_arguments(MAX_TOOL_ARGUMENT_BYTES);
        let mut argument_assembler = StreamingAssembler::default();
        for (index, fragment) in argument_fragments.into_iter().enumerate() {
            argument_assembler
                .apply_chunk(
                    tool_delta_chunk(
                        0,
                        (index == 0).then_some("call_0"),
                        (index == 0).then_some("file_read".into()),
                        Some(fragment),
                    ),
                    &mut |_| Ok(()),
                )
                .unwrap();
        }
        argument_assembler.finish_reason = Some(ChatFinishReason::ToolCalls);
        let response = argument_assembler.into_model_response().unwrap();
        assert_eq!(response.tool_uses().len(), 1);

        let mut over_assembler = StreamingAssembler::default();
        for (index, fragment) in fragmented_unicode_json_arguments(MAX_TOOL_ARGUMENT_BYTES)
            .into_iter()
            .enumerate()
        {
            over_assembler
                .apply_chunk(
                    tool_delta_chunk(
                        0,
                        (index == 0).then_some("call_0"),
                        (index == 0).then_some("file_read".into()),
                        Some(fragment),
                    ),
                    &mut |_| Ok(()),
                )
                .unwrap();
        }
        let error = over_assembler
            .apply_chunk(
                tool_delta_chunk(0, None, None, Some("x".into())),
                &mut |_| Ok(()),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            AppError::Provider(message) if message == TOOL_ARGUMENT_LIMIT_ERROR
        ));
        assert_eq!(
            over_assembler.tool_calls.get(&0).unwrap().arguments.len(),
            MAX_TOOL_ARGUMENT_BYTES
        );
    }

    #[test]
    fn streaming_tool_count_indices_and_aggregate_arguments_are_bounded() {
        let exact_calls = (0..MAX_TOOL_CALLS)
            .map(|index| {
                streaming_tool_delta(
                    index,
                    Some(format!("call_{index}")),
                    Some("file_read".into()),
                    Some("{}".into()),
                )
            })
            .collect();
        let mut assembler = StreamingAssembler::default();
        assembler
            .apply_chunk(
                ChatCompletionChunk {
                    model: None,
                    choices: vec![ChatChunkChoice {
                        index: 0,
                        delta: ChatDelta {
                            content: None,
                            tool_calls: exact_calls,
                        },
                        finish_reason: Some(ChatFinishReason::ToolCalls),
                    }],
                    usage: None,
                },
                &mut |_| Ok(()),
            )
            .unwrap();
        assert!(assembler.tool_calls.contains_key(&MAX_TOOL_CALL_INDEX));
        let response = assembler.into_model_response().unwrap();
        assert_eq!(response.tool_uses().len(), MAX_TOOL_CALLS);

        for index in [MAX_TOOL_CALLS, usize::MAX] {
            let mut assembler = StreamingAssembler::default();
            let error = assembler
                .apply_chunk(
                    tool_delta_chunk(
                        index,
                        Some("call_sparse"),
                        Some("file_read".into()),
                        Some("{}".into()),
                    ),
                    &mut |_| Ok(()),
                )
                .unwrap_err();
            assert!(matches!(
                error,
                AppError::Provider(message) if message == TOOL_CALL_LIMIT_ERROR
            ));
            assert!(assembler.tool_calls.is_empty());
        }

        let over_calls = (0..=MAX_TOOL_CALLS)
            .map(|index| {
                streaming_tool_delta(
                    index,
                    Some(format!("call_{index}")),
                    Some("file_read".into()),
                    Some("{}".into()),
                )
            })
            .collect();
        let mut assembler = StreamingAssembler::default();
        let error = assembler
            .apply_chunk(
                ChatCompletionChunk {
                    model: None,
                    choices: vec![ChatChunkChoice {
                        index: 0,
                        delta: ChatDelta {
                            content: None,
                            tool_calls: over_calls,
                        },
                        finish_reason: None,
                    }],
                    usage: None,
                },
                &mut |_| Ok(()),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            AppError::Provider(message) if message == TOOL_CALL_LIMIT_ERROR
        ));
        assert_eq!(assembler.tool_calls.len(), MAX_TOOL_CALLS);

        let mut assembler = StreamingAssembler::default();
        for index in 0..4 {
            assembler
                .apply_chunk(
                    ChatCompletionChunk {
                        model: None,
                        choices: vec![ChatChunkChoice {
                            index: 0,
                            delta: ChatDelta {
                                content: None,
                                tool_calls: vec![streaming_tool_delta(
                                    index,
                                    Some(format!("call_{index}")),
                                    Some("file_read".into()),
                                    Some(json_arguments_with_bytes(MAX_TOOL_ARGUMENT_BYTES)),
                                )],
                            },
                            finish_reason: None,
                        }],
                        usage: None,
                    },
                    &mut |_| Ok(()),
                )
                .unwrap();
        }
        assert_eq!(assembler.tool_arguments_bytes, MAX_TOOL_ARGUMENTS_BYTES);
        let error = assembler
            .apply_chunk(
                tool_delta_chunk(
                    4,
                    Some("call_4"),
                    Some("file_read".into()),
                    Some("0".into()),
                ),
                &mut |_| Ok(()),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            AppError::Provider(message) if message == TOOL_ARGUMENTS_LIMIT_ERROR
        ));
        assert!(!assembler.tool_calls.contains_key(&4));
        assembler.finish_reason = Some(ChatFinishReason::ToolCalls);
        let response = assembler.into_model_response().unwrap();
        assert_eq!(response.tool_uses().len(), 4);
    }
    fn streaming_body_with_size(bytes: usize) -> String {
        let finish = sse_finish();
        assert!(finish.len() + 3 <= bytes);
        let mut remaining = bytes - finish.len();
        let mut body = String::with_capacity(bytes);
        while remaining > MAX_SSE_EVENT_BYTES {
            let leftover = remaining - MAX_SSE_EVENT_BYTES;
            let event_bytes = if leftover < 3 {
                MAX_SSE_EVENT_BYTES - (3 - leftover)
            } else {
                MAX_SSE_EVENT_BYTES
            };
            body.push(':');
            body.push_str(&"x".repeat(event_bytes - 3));
            body.push_str("\n\n");
            remaining -= event_bytes;
        }
        assert!(remaining >= 3);
        body.push(':');
        body.push_str(&"x".repeat(remaining - 3));
        body.push_str("\n\n");
        body.push_str(&finish);
        assert_eq!(body.len(), bytes);
        body
    }

    fn sse_event_with_size(chunk: &Value, bytes: usize) -> String {
        let data_line = format!("data: {chunk}\n");
        let fixed_bytes = data_line.len() + 3;
        assert!(fixed_bytes <= bytes);
        let event = format!(":{}\n{data_line}\n", "x".repeat(bytes - fixed_bytes));
        assert_eq!(event.len(), bytes);
        event
    }
    fn streaming_tool_delta(
        index: usize,
        id: Option<String>,
        name: Option<String>,
        arguments: Option<String>,
    ) -> ChatToolCallDelta {
        ChatToolCallDelta {
            index,
            id,
            function: (name.is_some() || arguments.is_some())
                .then_some(ChatFunctionCallDelta { name, arguments }),
        }
    }

    fn tool_delta_chunk(
        index: usize,
        id: Option<&str>,
        name: Option<String>,
        arguments: Option<String>,
    ) -> ChatCompletionChunk {
        ChatCompletionChunk {
            model: None,
            choices: vec![ChatChunkChoice {
                index: 0,
                delta: ChatDelta {
                    content: None,
                    tool_calls: vec![streaming_tool_delta(
                        index,
                        id.map(str::to_string),
                        name,
                        arguments,
                    )],
                },
                finish_reason: None,
            }],
            usage: None,
        }
    }
}
