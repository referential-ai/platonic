use crate::{
    AppError, AppResult,
    model::{
        ModelBlock, ModelMessage, ModelRequest, ModelResponse, ModelRole, ModelStop,
        ReasoningEffort,
    },
    tool_catalog::{ToolSpec, internal_name_for_provider, provider_name_for_internal},
};
use platonic_core::{ModelName, ModelUsage};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    io::{self, BufRead, BufReader, Read},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

const RESPONSE_READ_CANCEL_POLL_INTERVAL: Duration = Duration::from_millis(25);
const MAX_DECODED_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MAX_NON_STREAM_BODY_BYTES: usize = 1024 * 1024;
const MAX_SSE_EVENT_BYTES: usize = 1024 * 1024;
const MAX_ASSISTANT_TEXT_BYTES: usize = 4 * 1024 * 1024;
const MAX_TOOL_CALLS: usize = 64;
const MAX_TOOL_CALL_INDEX: usize = MAX_TOOL_CALLS - 1;
const MAX_TOOL_NAME_BYTES: usize = 256;
const MAX_TOOL_ARGUMENT_BYTES: usize = 1024 * 1024;
const MAX_TOOL_ARGUMENTS_BYTES: usize = 4 * 1024 * 1024;

const DECODED_RESPONSE_LIMIT_ERROR: &str =
    "provider response exceeded the 8 MiB decoded data limit";
const NON_STREAM_BODY_LIMIT_ERROR: &str =
    "provider response exceeded the 1 MiB non-stream body limit";
const SSE_EVENT_LIMIT_ERROR: &str = "provider response exceeded the 1 MiB SSE event limit";
const ASSISTANT_TEXT_LIMIT_ERROR: &str =
    "provider response exceeded the 4 MiB assistant text limit";
const TOOL_CALL_LIMIT_ERROR: &str = "provider response exceeded the 64 tool call limit";
const TOOL_NAME_LIMIT_ERROR: &str = "provider response exceeded the 256-byte tool name limit";
const TOOL_ARGUMENT_LIMIT_ERROR: &str =
    "provider response exceeded the 1 MiB per-call tool arguments limit";
const TOOL_ARGUMENTS_LIMIT_ERROR: &str =
    "provider response exceeded the 4 MiB aggregate tool arguments limit";
const CONFLICTING_SERVED_MODEL_ERROR: &str =
    "provider stream returned conflicting served model values";
const STREAM_ERROR_EVENT_ERROR: &str = "provider stream returned an error event";

pub struct OpenAiCompatibleClient {
    api_key: String,
    base_url: String,
    connect_timeout: Duration,
    stream_idle_timeout: Duration,
    http_referer: Option<String>,
    app_title: Option<String>,
    token_limit_field: TokenLimitField,
}

impl OpenAiCompatibleClient {
    pub fn from_config(
        api_key_env: &str,
        base_url: String,
        connect_timeout_ms: u64,
        stream_idle_timeout_ms: u64,
        http_referer: Option<String>,
        app_title: Option<String>,
        token_limit_field: TokenLimitField,
    ) -> AppResult<Self> {
        let api_key =
            std::env::var(api_key_env).map_err(|_| AppError::MissingApiKey(api_key_env.into()))?;
        if base_url.trim().is_empty() {
            return Err(AppError::Config(
                "provider.base_url must not be empty".into(),
            ));
        }
        if connect_timeout_ms == 0 {
            return Err(AppError::Config(
                "provider.connect_timeout_ms must be positive".into(),
            ));
        }
        if stream_idle_timeout_ms == 0 {
            return Err(AppError::Config(
                "provider.stream_idle_timeout_ms must be positive".into(),
            ));
        }
        Ok(Self {
            api_key,
            base_url,
            connect_timeout: Duration::from_millis(connect_timeout_ms),
            stream_idle_timeout: Duration::from_millis(stream_idle_timeout_ms),
            http_referer,
            app_title,
            token_limit_field,
        })
    }

    pub fn send(&self, request: &ModelRequest) -> AppResult<ModelResponse> {
        self.send_with_cancel(request, None)
    }

    pub(crate) fn send_with_cancel(
        &self,
        request: &ModelRequest,
        cancel: Option<&AtomicBool>,
    ) -> AppResult<ModelResponse> {
        let body = ChatCompletionRequest::from_model_request(request, self.token_limit_field)?;
        self.send_body(body, cancel)
    }

    pub fn send_streaming(
        &self,
        request: &ModelRequest,
        on_delta: impl FnMut(&str) -> AppResult<()>,
    ) -> AppResult<ModelResponse> {
        self.send_streaming_with_cancel(request, None, on_delta)
    }

    pub(crate) fn send_streaming_with_cancel(
        &self,
        request: &ModelRequest,
        cancel: Option<&AtomicBool>,
        mut on_delta: impl FnMut(&str) -> AppResult<()>,
    ) -> AppResult<ModelResponse> {
        let mut body = ChatCompletionRequest::from_model_request(request, self.token_limit_field)?;
        body.stream = Some(true);
        body.stream_options = Some(ChatStreamOptions {
            include_usage: true,
        });
        let response = self.post_completion(body)?;
        match cancel {
            Some(cancel) => read_streaming_response_with_cancel(response, cancel, &mut on_delta),
            None => {
                parse_chat_completion_stream(BufReader::new(response.into_reader()), &mut on_delta)
            }
        }
    }

    fn send_body(
        &self,
        body: ChatCompletionRequest,
        cancel: Option<&AtomicBool>,
    ) -> AppResult<ModelResponse> {
        let response = self.post_completion(body)?;
        match cancel {
            Some(cancel) => read_non_stream_response_with_cancel(response, cancel),
            None => parse_non_stream_response(response.into_reader()),
        }
    }

    fn post_completion(&self, body: ChatCompletionRequest) -> AppResult<ureq::Response> {
        let url = format!("{}/chat/completions", self.base_url.trim_end_matches('/'));
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(self.connect_timeout)
            .timeout_write(self.connect_timeout)
            .timeout_read(self.stream_idle_timeout)
            .build();
        self.authorized_post(&agent, &url)
            .send_json(body)
            .map_err(provider_send_error)
    }

    fn authorized_post(&self, agent: &ureq::Agent, url: &str) -> ureq::Request {
        let mut call = agent
            .post(url)
            .set("authorization", &format!("Bearer {}", self.api_key))
            .set("content-type", "application/json");
        if let Some(http_referer) = &self.http_referer {
            call = call.set("HTTP-Referer", http_referer);
        }
        if let Some(app_title) = &self.app_title {
            call = call.set("X-OpenRouter-Title", app_title);
        }
        call
    }
}

fn provider_send_error(error: ureq::Error) -> AppError {
    match error {
        ureq::Error::Status(429, response) => AppError::ProviderCompletionRateLimited {
            retry_after_seconds: response
                .header("Retry-After")
                .and_then(parse_retry_after_seconds),
        },
        ureq::Error::Status(status, _) => {
            AppError::Provider(format!("provider returned http {status}"))
        }
        error => AppError::Provider(error.to_string()),
    }
}

// Keep ureq's blocking body reads off the run thread; rendezvous delivery leaves
// every result and user-visible delta under the run thread's cancel check.
enum StreamingBodyMessage {
    Delta(String),
    Finished(AppResult<ModelResponse>),
}

fn parse_non_stream_response(reader: impl Read) -> AppResult<ModelResponse> {
    let body = read_non_stream_body(reader)?;
    serde_json::from_slice::<ChatCompletionResponse>(&body)
        .map_err(|error| AppError::Provider(error.to_string()))?
        .into_model_response()
}

fn read_non_stream_response_with_cancel(
    response: ureq::Response,
    cancel: &AtomicBool,
) -> AppResult<ModelResponse> {
    let (result_sender, result_receiver) = mpsc::sync_channel(0);
    let worker = thread::Builder::new()
        .name("plato-provider-body".into())
        .spawn(move || {
            let result = parse_non_stream_response(response.into_reader());
            let _ = result_sender.send(result);
        })?;

    receive_body_result(result_receiver, cancel, worker)
}

fn read_streaming_response_with_cancel(
    response: ureq::Response,
    cancel: &AtomicBool,
    on_delta: &mut impl FnMut(&str) -> AppResult<()>,
) -> AppResult<ModelResponse> {
    let (message_sender, message_receiver) = mpsc::sync_channel(0);
    let worker = thread::Builder::new()
        .name("plato-provider-stream".into())
        .spawn(move || {
            let result = parse_chat_completion_stream(
                BufReader::new(response.into_reader()),
                &mut |delta| {
                    message_sender
                        .send(StreamingBodyMessage::Delta(delta.to_owned()))
                        .map_err(|_| AppError::RunCanceled)
                },
            );
            let _ = message_sender.send(StreamingBodyMessage::Finished(result));
        })?;

    loop {
        check_response_read_cancel(cancel)?;
        match message_receiver.recv_timeout(RESPONSE_READ_CANCEL_POLL_INTERVAL) {
            Ok(StreamingBodyMessage::Delta(delta)) => {
                check_response_read_cancel(cancel)?;
                on_delta(&delta)?;
            }
            Ok(StreamingBodyMessage::Finished(result)) => {
                check_response_read_cancel(cancel)?;
                join_response_body_worker(worker)?;
                return result;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(response_body_worker_error(worker));
            }
        }
    }
}

fn receive_body_result<T>(
    result_receiver: mpsc::Receiver<AppResult<T>>,
    cancel: &AtomicBool,
    worker: thread::JoinHandle<()>,
) -> AppResult<T> {
    loop {
        check_response_read_cancel(cancel)?;
        match result_receiver.recv_timeout(RESPONSE_READ_CANCEL_POLL_INTERVAL) {
            Ok(result) => {
                check_response_read_cancel(cancel)?;
                join_response_body_worker(worker)?;
                return result;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(response_body_worker_error(worker));
            }
        }
    }
}

fn check_response_read_cancel(cancel: &AtomicBool) -> AppResult<()> {
    if cancel.load(Ordering::SeqCst) {
        record_response_read_cancel_observation();
        return Err(AppError::RunCanceled);
    }
    Ok(())
}

fn join_response_body_worker(worker: thread::JoinHandle<()>) -> AppResult<()> {
    worker.join().map_err(|_| {
        AppError::Provider("provider response body worker panicked after returning a result".into())
    })
}

fn response_body_worker_error(worker: thread::JoinHandle<()>) -> AppError {
    match worker.join() {
        Ok(()) => AppError::Provider("provider response body worker ended without a result".into()),
        Err(_) => AppError::Provider("provider response body worker panicked".into()),
    }
}

#[cfg(test)]
std::thread_local! {
    static RESPONSE_READ_CANCEL_OBSERVER: std::cell::RefCell<Option<ResponseReadCancelObserver>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
struct ResponseReadCancelObserver {
    sender: mpsc::Sender<std::time::Instant>,
    delay: std::time::Duration,
}

#[cfg(test)]
pub(crate) fn with_response_read_cancel_observer<T>(
    observer: mpsc::Sender<std::time::Instant>,
    delay: std::time::Duration,
    run: impl FnOnce() -> T,
) -> T {
    struct ObserverGuard(Option<ResponseReadCancelObserver>);

    impl Drop for ObserverGuard {
        fn drop(&mut self) {
            RESPONSE_READ_CANCEL_OBSERVER.with(|slot| {
                slot.replace(self.0.take());
            });
        }
    }

    let previous = RESPONSE_READ_CANCEL_OBSERVER.with(|slot| {
        slot.replace(Some(ResponseReadCancelObserver {
            sender: observer,
            delay,
        }))
    });
    let _guard = ObserverGuard(previous);
    run()
}

#[cfg(test)]
fn record_response_read_cancel_observation() {
    RESPONSE_READ_CANCEL_OBSERVER.with(|slot| {
        if let Some(observer) = slot.borrow_mut().take() {
            std::thread::sleep(observer.delay);
            let _ = observer.sender.send(std::time::Instant::now());
        }
    });
}

#[cfg(not(test))]
fn record_response_read_cancel_observation() {}

fn read_non_stream_body(reader: impl Read) -> AppResult<Vec<u8>> {
    let mut body = Vec::new();
    reader
        .take((MAX_NON_STREAM_BODY_BYTES + 1) as u64)
        .read_to_end(&mut body)?;
    if body.len() > MAX_NON_STREAM_BODY_BYTES {
        return Err(AppError::Provider(NON_STREAM_BODY_LIMIT_ERROR.into()));
    }
    Ok(body)
}

fn parse_retry_after_seconds(value: &str) -> Option<f64> {
    let seconds = value.trim().parse::<f64>().ok()?;
    (seconds.is_finite() && seconds >= 0.0).then_some(seconds)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TokenLimitField {
    MaxTokens,
    MaxCompletionTokens,
}

#[derive(Debug, Serialize)]
struct ChatCompletionRequest {
    model: String,
    messages: Vec<ChatMessage>,
    tools: Vec<ChatTool>,
    tool_choice: &'static str,
    parallel_tool_calls: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning_effort: Option<ReasoningEffort>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning: Option<OpenRouterReasoning>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_options: Option<ChatStreamOptions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_completion_tokens: Option<u32>,
}

#[derive(Debug, Serialize)]
struct OpenRouterReasoning {
    effort: ReasoningEffort,
}

#[derive(Debug, Serialize)]
struct ChatStreamOptions {
    include_usage: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChatMessage {
    role: ChatRole,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<Vec<ChatToolCall>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ChatRole {
    System,
    User,
    Assistant,
    Tool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChatTool {
    #[serde(rename = "type")]
    tool_type: ChatToolType,
    function: ChatFunctionDefinition,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ChatToolType {
    Function,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChatFunctionDefinition {
    name: String,
    description: String,
    parameters: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ChatToolCall {
    id: String,
    #[serde(rename = "type")]
    tool_type: ChatToolType,
    function: ChatFunctionCall,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ChatFunctionCall {
    name: String,
    arguments: String,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionResponse {
    model: Option<ModelName>,
    choices: Vec<ChatChoice>,
    usage: Option<ChatUsage>,
}

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

#[derive(Debug, Deserialize)]
struct ChatChoice {
    finish_reason: ChatFinishReason,
    message: ChatResponseMessage,
}

#[derive(Debug, Deserialize)]
struct ChatResponseMessage {
    content: Option<String>,
    tool_calls: Option<Vec<ChatToolCall>>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ChatFinishReason {
    Stop,
    Length,
    ToolCalls,
    ContentFilter,
    FunctionCall,
}

#[derive(Clone, Debug, Deserialize)]
struct ChatUsage {
    prompt_tokens: Option<u32>,
    completion_tokens: Option<u32>,
}

impl ChatCompletionRequest {
    fn from_model_request(
        request: &ModelRequest,
        token_limit_field: TokenLimitField,
    ) -> AppResult<Self> {
        let mut messages = Vec::with_capacity(request.messages.len() + 1);
        messages.push(ChatMessage {
            role: ChatRole::System,
            content: Some(request.system.clone()),
            tool_calls: None,
            tool_call_id: None,
        });
        for message in &request.messages {
            messages.push(ChatMessage::from_model_message(message)?);
        }

        let (reasoning_effort, reasoning) = match token_limit_field {
            TokenLimitField::MaxTokens => (
                None,
                request
                    .reasoning_effort
                    .map(|effort| OpenRouterReasoning { effort }),
            ),
            TokenLimitField::MaxCompletionTokens => (request.reasoning_effort, None),
        };

        Ok(Self {
            model: request.model.clone(),
            messages,
            tools: request.tools.iter().map(ChatTool::from_tool_spec).collect(),
            tool_choice: "auto",
            parallel_tool_calls: false,
            reasoning_effort,
            reasoning,
            stream: None,
            stream_options: None,
            max_tokens: matches!(token_limit_field, TokenLimitField::MaxTokens)
                .then_some(request.max_output_tokens),
            max_completion_tokens: matches!(
                token_limit_field,
                TokenLimitField::MaxCompletionTokens
            )
            .then_some(request.max_output_tokens),
        })
    }
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

fn parse_chat_completion_stream(
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

impl ChatMessage {
    fn from_model_message(message: &ModelMessage) -> AppResult<Self> {
        match message.role {
            ModelRole::User => Ok(Self {
                role: ChatRole::User,
                content: Some(text_from_blocks(&message.content)),
                tool_calls: None,
                tool_call_id: None,
            }),
            ModelRole::Assistant => {
                let text = text_from_blocks(&message.content);
                let tool_calls = message
                    .content
                    .iter()
                    .filter_map(|block| match block {
                        ModelBlock::ToolUse { id, name, input } => Some((id, name, input)),
                        ModelBlock::Text { .. } | ModelBlock::ToolResult { .. } => None,
                    })
                    .map(|(id, name, input)| {
                        let provider_name = provider_name_for_internal(name).ok_or_else(|| {
                            AppError::Provider(format!(
                                "model message contained unknown tool {name}"
                            ))
                        })?;
                        Ok(ChatToolCall {
                            id: id.clone(),
                            tool_type: ChatToolType::Function,
                            function: ChatFunctionCall {
                                name: provider_name.into(),
                                arguments: serde_json::to_string(input).unwrap_or_default(),
                            },
                        })
                    })
                    .collect::<AppResult<Vec<_>>>()?;
                Ok(Self {
                    role: ChatRole::Assistant,
                    content: (!text.is_empty()).then_some(text),
                    tool_calls: (!tool_calls.is_empty()).then_some(tool_calls),
                    tool_call_id: None,
                })
            }
            ModelRole::Tool => {
                let result = message.content.iter().find_map(|block| match block {
                    ModelBlock::ToolResult {
                        tool_call_id,
                        content,
                        ..
                    } => Some((tool_call_id, content)),
                    ModelBlock::Text { .. } | ModelBlock::ToolUse { .. } => None,
                });
                let (tool_call_id, content) = result.ok_or_else(|| {
                    AppError::Provider("tool message did not contain a tool result".into())
                })?;
                Ok(Self {
                    role: ChatRole::Tool,
                    content: Some(content.clone()),
                    tool_calls: None,
                    tool_call_id: Some(tool_call_id.clone()),
                })
            }
        }
    }
}

fn tool_use_from_provider(id: String, name: String, arguments: String) -> AppResult<ModelBlock> {
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

fn validate_tool_call(name: &str, arguments: &str) -> AppResult<()> {
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

fn model_response(
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

impl ChatTool {
    fn from_tool_spec(spec: &ToolSpec) -> Self {
        Self {
            tool_type: ChatToolType::Function,
            function: ChatFunctionDefinition {
                name: spec.name.clone(),
                description: spec.description.clone(),
                parameters: spec.input_schema.clone(),
            },
        }
    }
}

impl ChatCompletionResponse {
    fn into_model_response(self) -> AppResult<ModelResponse> {
        let choice = self
            .choices
            .into_iter()
            .next()
            .ok_or_else(|| AppError::Provider("provider returned no choices".into()))?;
        let text = choice.message.content.filter(|text| !text.is_empty());
        if text
            .as_ref()
            .is_some_and(|text| text.len() > MAX_ASSISTANT_TEXT_BYTES)
        {
            return Err(AppError::Provider(ASSISTANT_TEXT_LIMIT_ERROR.into()));
        }
        let tool_calls = choice.message.tool_calls.unwrap_or_default();
        if tool_calls.len() > MAX_TOOL_CALLS {
            return Err(AppError::Provider(TOOL_CALL_LIMIT_ERROR.into()));
        }
        let mut tool_arguments_bytes = 0_usize;
        for call in &tool_calls {
            validate_tool_call(&call.function.name, &call.function.arguments)?;
            tool_arguments_bytes = tool_arguments_bytes
                .checked_add(call.function.arguments.len())
                .ok_or_else(|| AppError::Provider(TOOL_ARGUMENTS_LIMIT_ERROR.into()))?;
            if tool_arguments_bytes > MAX_TOOL_ARGUMENTS_BYTES {
                return Err(AppError::Provider(TOOL_ARGUMENTS_LIMIT_ERROR.into()));
            }
        }

        let mut content = Vec::new();
        if let Some(text) = text {
            content.push(ModelBlock::Text { text });
        }
        for call in tool_calls {
            content.push(tool_use_from_provider(
                call.id,
                call.function.name,
                call.function.arguments,
            )?);
        }
        Ok(model_response(
            content,
            choice.finish_reason,
            self.model,
            self.usage,
        ))
    }
}

fn text_from_blocks(blocks: &[ModelBlock]) -> String {
    blocks
        .iter()
        .filter_map(|block| match block {
            ModelBlock::Text { text } => Some(text.as_str()),
            ModelBlock::ToolUse { .. } | ModelBlock::ToolResult { .. } => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::{
        io::{BufReader, Cursor, ErrorKind, Read, Write},
        net::{TcpListener, TcpStream},
        sync::{Arc, mpsc},
        thread,
        time::Instant,
    };

    #[test]
    fn maps_openai_tool_calls_to_internal_tool_names() {
        let response: ChatCompletionResponse = serde_json::from_value(json!({
            "model": "provider/test-model-2026-08-01",
            "choices": [{
                "finish_reason": "tool_calls",
                "message": {
                    "content": "I will read it.",
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": {
                            "name": "file_read",
                            "arguments": "{\"path\":\"README.md\"}"
                        }
                    }]
                }
            }],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 5
            }
        }))
        .unwrap();

        let response = response.into_model_response().unwrap();

        assert_eq!(response.stop, ModelStop::ToolUse);
        assert_eq!(
            response.served_model,
            Some(ModelName::new("provider/test-model-2026-08-01").unwrap())
        );
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
    fn non_streaming_served_model_is_validated_and_omission_stays_unknown() {
        let response: ChatCompletionResponse = serde_json::from_value(json!({
            "model": "provider/concrete-model-2026-08-01",
            "choices": [{
                "finish_reason": "stop",
                "message": {"content": "done"}
            }]
        }))
        .unwrap();
        assert_eq!(
            response.into_model_response().unwrap().served_model,
            Some(ModelName::new("provider/concrete-model-2026-08-01").unwrap())
        );

        let omitted: ChatCompletionResponse = serde_json::from_value(json!({
            "choices": [{
                "finish_reason": "stop",
                "message": {"content": "done"}
            }]
        }))
        .unwrap();
        assert_eq!(omitted.into_model_response().unwrap().served_model, None);

        for malformed in [json!(""), json!(" "), json!(7)] {
            let error = serde_json::from_value::<ChatCompletionResponse>(json!({
                "model": malformed,
                "choices": [{
                    "finish_reason": "stop",
                    "message": {"content": "done"}
                }]
            }))
            .unwrap_err();
            assert!(!error.to_string().contains("done"));
        }
    }

    #[test]
    fn non_streaming_usage_is_known_only_when_both_counts_are_reported() {
        for (fixture, raw_usage, expected) in usage_fixtures() {
            let mut raw_response = json!({
                "choices": [{
                    "finish_reason": "stop",
                    "message": {
                        "content": "done"
                    }
                }]
            });
            if let Some(raw_usage) = raw_usage {
                raw_response["usage"] = raw_usage;
            }

            let response: ChatCompletionResponse = serde_json::from_value(raw_response).unwrap();
            let response = response.into_model_response().unwrap();

            assert_eq!(response.usage, expected, "fixture: {fixture}");
        }
    }

    #[test]
    fn provider_unknown_tool_names_fail_response_parse() {
        let response: ChatCompletionResponse = serde_json::from_value(json!({
            "choices": [{
                "finish_reason": "tool_calls",
                "message": {
                    "content": null,
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": {
                            "name": "shell_delete",
                            "arguments": "{\"command\":\"pwd\"}"
                        }
                    }]
                }
            }]
        }))
        .unwrap();

        let err = response.into_model_response().unwrap_err();

        assert!(matches!(
            err,
            AppError::Provider(message) if message == "provider returned unknown tool shell_delete"
        ));
    }

    #[test]
    fn provider_tool_calls_require_an_id() {
        let response: ChatCompletionResponse = serde_json::from_value(json!({
            "choices": [{
                "finish_reason": "tool_calls",
                "message": {
                    "content": null,
                    "tool_calls": [{
                        "id": "  ",
                        "type": "function",
                        "function": {
                            "name": "file_read",
                            "arguments": "{\"path\":\"README.md\"}"
                        }
                    }]
                }
            }]
        }))
        .unwrap();

        let error = response.into_model_response().unwrap_err();

        assert!(matches!(
            error,
            AppError::Provider(message) if message == "provider returned tool call without id"
        ));
    }

    #[test]
    fn ignores_extra_fields_on_provider_tool_calls() {
        let response: ChatCompletionResponse = serde_json::from_value(json!({
            "choices": [{
                "finish_reason": "tool_calls",
                "message": {
                    "content": null,
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "index": 0,
                        "function": {
                            "name": "file_read",
                            "arguments": "{\"path\":\"README.md\"}",
                            "parsed_arguments": {"path": "README.md"}
                        }
                    }]
                }
            }]
        }))
        .unwrap();

        let response = response.into_model_response().unwrap();

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
    fn maps_model_messages_to_chat_completion_messages() {
        let message = ModelMessage::assistant_blocks(vec![
            ModelBlock::Text {
                text: "Reading".into(),
            },
            ModelBlock::ToolUse {
                id: "call_1".into(),
                name: "file.read".into(),
                input: json!({"path": "README.md"}),
            },
        ]);

        let chat = ChatMessage::from_model_message(&message).unwrap();

        assert!(matches!(chat.role, ChatRole::Assistant));
        assert_eq!(chat.content, Some("Reading".into()));
        assert_eq!(chat.tool_calls.unwrap()[0].function.name, "file_read");
    }

    #[test]
    fn openrouter_serializes_reasoning_as_nested_effort() {
        let request = reasoning_request(Some(ReasoningEffort::High));

        let body = ChatCompletionRequest::from_model_request(&request, TokenLimitField::MaxTokens)
            .unwrap();
        let value = serde_json::to_value(body).unwrap();

        assert_eq!(value["reasoning"], json!({"effort": "high"}));
        assert!(value.get("reasoning_effort").is_none());
    }

    #[test]
    fn openai_serializes_reasoning_as_top_level_effort() {
        let request = reasoning_request(Some(ReasoningEffort::Medium));

        let body = ChatCompletionRequest::from_model_request(
            &request,
            TokenLimitField::MaxCompletionTokens,
        )
        .unwrap();
        let value = serde_json::to_value(body).unwrap();

        assert_eq!(value["reasoning_effort"], "medium");
        assert!(value.get("reasoning").is_none());
    }

    #[test]
    fn provider_request_omits_unspecified_reasoning() {
        for token_limit_field in [
            TokenLimitField::MaxTokens,
            TokenLimitField::MaxCompletionTokens,
        ] {
            let request = reasoning_request(None);
            let body =
                ChatCompletionRequest::from_model_request(&request, token_limit_field).unwrap();
            let value = serde_json::to_value(body).unwrap();

            assert!(value.get("reasoning").is_none());
            assert!(value.get("reasoning_effort").is_none());
        }
    }

    #[test]
    fn streaming_request_includes_usage_stream_options() {
        let request = reasoning_request(None);
        let mut body =
            ChatCompletionRequest::from_model_request(&request, TokenLimitField::MaxTokens)
                .unwrap();

        body.stream = Some(true);
        body.stream_options = Some(ChatStreamOptions {
            include_usage: true,
        });

        let value = serde_json::to_value(body).unwrap();
        assert_eq!(value["stream"], true);
        assert_eq!(value["stream_options"]["include_usage"], true);
    }

    fn reasoning_request(reasoning_effort: Option<ReasoningEffort>) -> ModelRequest {
        ModelRequest {
            model: "test-model".into(),
            system: "system".into(),
            max_output_tokens: 32,
            reasoning_effort,
            messages: vec![ModelMessage::user_text("hello")],
            tools: Vec::new(),
        }
    }

    #[test]
    fn retry_after_parser_accepts_only_finite_nonnegative_numbers() {
        assert_eq!(parse_retry_after_seconds("0"), Some(0.0));
        assert_eq!(parse_retry_after_seconds("0.25"), Some(0.25));
        assert_eq!(parse_retry_after_seconds("30"), Some(30.0));
        assert_eq!(
            parse_retry_after_seconds("30.000000000000004"),
            Some(f64::from_bits(30.0_f64.to_bits() + 1))
        );
        assert_eq!(parse_retry_after_seconds("1e300"), Some(1e300));
        assert_eq!(parse_retry_after_seconds("-1"), None);
        assert_eq!(parse_retry_after_seconds("NaN"), None);
        assert_eq!(parse_retry_after_seconds("inf"), None);
        assert_eq!(parse_retry_after_seconds("tomorrow"), None);
    }

    #[test]
    fn completion_429_is_typed_without_exposing_response_body() {
        let body = "provider-secret-body";
        let (base_url, server) = spawn_loopback_provider(move |mut stream| {
            read_provider_request(&mut stream);
            write!(
                stream,
                "HTTP/1.1 429 Too Many Requests\r\nretry-after: 0.25\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        });
        let client = timeout_test_client(base_url, 2_000, 2_000);

        let error = client.send(&reasoning_request(None)).unwrap_err();
        server.join().unwrap();

        assert_eq!(
            error.to_string(),
            "provider completion POST returned http 429 before response body"
        );
        assert!(matches!(
            error,
            AppError::ProviderCompletionRateLimited {
                retry_after_seconds: Some(seconds)
            } if seconds == 0.25
        ));
    }

    #[test]
    fn non_429_status_errors_expose_only_status() {
        for (status, reason) in [
            (400, "Bad Request"),
            (401, "Unauthorized"),
            (403, "Forbidden"),
            (500, "Internal Server Error"),
        ] {
            let body = format!("secret-for-{status}");
            let response_body = body.clone();
            let (base_url, server) = spawn_loopback_provider(move |mut stream| {
                read_provider_request(&mut stream);
                write!(
                    stream,
                    "HTTP/1.1 {status} {reason}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{response_body}",
                    response_body.len()
                )
                .unwrap();
            });
            let client = timeout_test_client(base_url, 2_000, 2_000);

            let error = client.send(&reasoning_request(None)).unwrap_err();
            server.join().unwrap();

            assert_eq!(
                error.to_string(),
                format!("provider error: provider returned http {status}")
            );
            assert!(!error.to_string().contains(&body));
        }
    }

    #[test]
    fn request_write_stall_uses_connect_budget() {
        let (stop_sender, stop_receiver) = mpsc::channel();
        let (base_url, server) = spawn_loopback_provider(move |_stream| {
            let _ = stop_receiver.recv_timeout(Duration::from_secs(5));
        });
        let client = timeout_test_client(base_url, 100, 2_000);
        let mut request = reasoning_request(None);
        request.system = "x".repeat(16 * 1024 * 1024);

        let started = Instant::now();
        let result = client.send(&request);
        let elapsed = started.elapsed();
        let _ = stop_sender.send(());
        server.join().unwrap();

        assert_timeout(result.unwrap_err());
        assert!(
            elapsed < Duration::from_secs(3),
            "request write took {elapsed:?}"
        );
    }

    #[test]
    fn cancellable_429_wait_keeps_the_configured_response_header_deadline() {
        assert!(RESPONSE_READ_CANCEL_POLL_INTERVAL <= Duration::from_millis(100));
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let (request_sender, request_receiver) = mpsc::channel();
        let (response_sender, response_receiver) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            read_provider_request(&mut stream);
            request_sender.send(()).unwrap();
            response_receiver
                .recv_timeout(Duration::from_secs(2))
                .unwrap();
            write!(
                stream,
                "HTTP/1.1 429 Too Many Requests\r\nretry-after: 0.1\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
            )
            .unwrap();
            stream.flush().unwrap();
        });
        let client = timeout_test_client(base_url, 2_000, 500);
        let cancel = AtomicBool::new(false);
        let (result_sender, result_receiver) = mpsc::channel();
        let request = thread::spawn(move || {
            result_sender
                .send(client.send_with_cancel(&reasoning_request(None), Some(&cancel)))
                .unwrap();
        });

        request_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        assert!(matches!(
            result_receiver.recv_timeout(Duration::from_millis(150)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        response_sender.send(()).unwrap();
        let error = result_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap_err();
        request.join().unwrap();
        server.join().unwrap();

        assert!(matches!(
            error,
            AppError::ProviderCompletionRateLimited {
                retry_after_seconds: Some(seconds)
            } if seconds == 0.1
        ));
    }

    #[test]
    fn streaming_response_stall_uses_idle_budget() {
        let (stop_sender, stop_receiver) = mpsc::channel();
        let first = sse_delta("start");
        let tail = sse_finish();
        let response_length = first.len() + tail.len();
        let (base_url, server) = spawn_loopback_provider(move |mut stream| {
            read_provider_request(&mut stream);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {response_length}\r\nconnection: close\r\n\r\n{first}"
            )
            .unwrap();
            stream.flush().unwrap();
            let _ = stop_receiver.recv_timeout(Duration::from_secs(5));
        });
        let idle_budget = Duration::from_millis(350);
        let client = timeout_test_client(base_url, 2_000, idle_budget.as_millis() as u64);
        let cancel = AtomicBool::new(false);
        let mut deltas = Vec::new();

        let started = Instant::now();
        let result =
            client.send_streaming_with_cancel(&reasoning_request(None), Some(&cancel), |delta| {
                deltas.push(delta.to_string());
                Ok(())
            });
        let elapsed = started.elapsed();
        let _ = stop_sender.send(());
        server.join().unwrap();

        assert_timeout(result.unwrap_err());
        assert_eq!(deltas, ["start"]);
        assert!(
            elapsed >= idle_budget.saturating_sub(Duration::from_millis(25)),
            "streaming response timed out before its idle budget: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "streaming response read took {elapsed:?}"
        );
    }

    #[test]
    fn streaming_progress_receives_fresh_idle_windows() {
        let chunks = ["a", "b", "c", "d", "e", "f"];
        let events = chunks.map(sse_delta);
        let finish = sse_finish();
        let response_length = events.iter().map(String::len).sum::<usize>() + finish.len();
        let (base_url, server) = spawn_loopback_provider(move |mut stream| {
            read_provider_request(&mut stream);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {response_length}\r\nconnection: close\r\n\r\n"
            )
            .unwrap();
            for event in events {
                stream.write_all(event.as_bytes()).unwrap();
                stream.flush().unwrap();
                thread::sleep(Duration::from_millis(150));
            }
            stream.write_all(finish.as_bytes()).unwrap();
            stream.flush().unwrap();
        });
        let idle_budget = Duration::from_millis(400);
        let client = timeout_test_client(base_url, 2_000, idle_budget.as_millis() as u64);
        let cancel = AtomicBool::new(false);

        let started = Instant::now();
        let response = client
            .send_streaming_with_cancel(&reasoning_request(None), Some(&cancel), |_| Ok(()))
            .unwrap();
        let elapsed = started.elapsed();
        server.join().unwrap();

        assert_eq!(response.text(), "abcdef");
        assert!(
            elapsed > idle_budget,
            "stream finished before exceeding the former total budget: {elapsed:?}"
        );
    }

    #[test]
    fn non_streaming_response_stall_uses_idle_budget() {
        let (stop_sender, stop_receiver) = mpsc::channel();
        let body = r#"{"choices":[{"finish_reason":"stop","message":{"content":"ok"}}]}"#;
        let partial = &body[..body.len() / 2];
        let response_length = body.len();
        let (base_url, server) = spawn_loopback_provider(move |mut stream| {
            read_provider_request(&mut stream);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {response_length}\r\nconnection: close\r\n\r\n{partial}"
            )
            .unwrap();
            stream.flush().unwrap();
            let _ = stop_receiver.recv_timeout(Duration::from_secs(5));
        });
        let idle_budget = Duration::from_millis(350);
        let client = timeout_test_client(base_url, 2_000, idle_budget.as_millis() as u64);
        let cancel = AtomicBool::new(false);

        let started = Instant::now();
        let result = client.send_with_cancel(&reasoning_request(None), Some(&cancel));
        let elapsed = started.elapsed();
        let _ = stop_sender.send(());
        server.join().unwrap();

        assert_timeout(result.unwrap_err());
        assert!(
            elapsed >= idle_budget.saturating_sub(Duration::from_millis(25)),
            "non-streaming response timed out before its idle budget: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "non-streaming response read took {elapsed:?}"
        );
    }

    #[test]
    fn stalled_stream_reads_cancel_at_every_partial_boundary_without_more_bytes() {
        let first = sse_delta("start");
        let partial_event = format!("{first}data: {{\"choices\":[");
        let unicode_event = concat!(
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"\u{754c}\"},",
            "\"finish_reason\":null}]}\n\n",
        );
        let unicode_split = unicode_event
            .as_bytes()
            .iter()
            .position(|byte| *byte >= 0x80)
            .expect("fixture contains UTF-8");
        let mut partial_utf8 = first.as_bytes().to_vec();
        partial_utf8.extend_from_slice(&unicode_event.as_bytes()[..=unicode_split]);

        let cases = [
            ("before_first_data", Vec::new(), false),
            ("between_chunks", first.as_bytes().to_vec(), true),
            ("partial_event", partial_event.into_bytes(), true),
            ("partial_utf8", partial_utf8, true),
        ];

        for (name, prefix, expect_first_delta) in cases {
            let provider = spawn_stalled_body_provider("text/event-stream", prefix);
            let client = timeout_test_client(provider.base_url.clone(), 2_000, 2_000);
            let cancel = Arc::new(AtomicBool::new(false));
            let run_cancel = Arc::clone(&cancel);
            let (delta_sender, delta_receiver) = mpsc::channel();
            let handle = thread::spawn(move || {
                client.send_streaming_with_cancel(
                    &reasoning_request(None),
                    Some(run_cancel.as_ref()),
                    |delta| {
                        delta_sender.send(delta.to_owned()).unwrap();
                        Ok(())
                    },
                )
            });

            provider
                .ready_receiver
                .recv_timeout(Duration::from_secs(2))
                .unwrap();
            if expect_first_delta {
                assert_eq!(
                    delta_receiver.recv_timeout(Duration::from_secs(2)).unwrap(),
                    "start",
                    "boundary: {name}"
                );
            } else {
                thread::sleep(Duration::from_millis(25));
            }

            let canceled_at = Instant::now();
            cancel.store(true, Ordering::SeqCst);
            let result = handle.join().unwrap();
            let elapsed = canceled_at.elapsed();

            assert!(
                matches!(result, Err(AppError::RunCanceled)),
                "boundary {name} returned {result:?}"
            );
            assert!(
                elapsed < Duration::from_millis(500),
                "boundary {name} cancellation took {elapsed:?}"
            );
            assert!(delta_receiver.try_recv().is_err(), "boundary: {name}");

            provider.release_sender.send(()).unwrap();
            provider.handle.join().unwrap();
        }
    }

    #[test]
    fn stalled_non_stream_body_read_returns_typed_cancellation_without_more_bytes() {
        let body = br#"{"choices":[{"finish_reason":"stop","message":{"content":"ok"}}]}"#;
        let provider =
            spawn_stalled_body_provider("application/json", body[..body.len() / 2].to_vec());
        let client = timeout_test_client(provider.base_url.clone(), 2_000, 2_000);
        let cancel = Arc::new(AtomicBool::new(false));
        let run_cancel = Arc::clone(&cancel);
        let handle = thread::spawn(move || {
            client.send_with_cancel(&reasoning_request(None), Some(run_cancel.as_ref()))
        });

        provider
            .ready_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        thread::sleep(Duration::from_millis(25));
        let canceled_at = Instant::now();
        cancel.store(true, Ordering::SeqCst);
        let result = handle.join().unwrap();
        let elapsed = canceled_at.elapsed();

        assert!(matches!(result, Err(AppError::RunCanceled)));
        assert!(
            elapsed < Duration::from_millis(500),
            "non-stream cancellation took {elapsed:?}"
        );
        provider.release_sender.send(()).unwrap();
        provider.handle.join().unwrap();
    }

    #[test]
    fn response_reader_preserves_eof_and_non_timeout_io_failures() {
        let mut deltas = Vec::new();
        let error = parse_chat_completion_stream(Cursor::new(sse_delta("orphan")), &mut |delta| {
            deltas.push(delta.to_owned());
            Ok(())
        })
        .unwrap_err();
        assert_eq!(deltas, ["orphan"]);
        assert!(matches!(
            error,
            AppError::Provider(message) if message == "provider stream ended before [DONE]"
        ));

        let error = read_non_stream_body(FixedReadError(ErrorKind::ConnectionReset)).unwrap_err();
        assert!(matches!(
            error,
            AppError::Io(error) if error.kind() == ErrorKind::ConnectionReset
        ));
    }

    #[test]
    fn non_stream_body_limit_is_exact() {
        let exact = padded_non_stream_response(MAX_NON_STREAM_BODY_BYTES);
        let body = read_non_stream_body(Cursor::new(exact)).unwrap();
        let response = serde_json::from_slice::<ChatCompletionResponse>(&body)
            .unwrap()
            .into_model_response()
            .unwrap();
        assert_eq!(response.text(), "ok");

        let error = read_non_stream_body(Cursor::new(padded_non_stream_response(
            MAX_NON_STREAM_BODY_BYTES + 1,
        )))
        .unwrap_err();
        assert!(matches!(
            error,
            AppError::Provider(message) if message == NON_STREAM_BODY_LIMIT_ERROR
        ));
    }

    fn timeout_test_client(
        base_url: String,
        connect_timeout_ms: u64,
        stream_idle_timeout_ms: u64,
    ) -> OpenAiCompatibleClient {
        temp_env::with_var("PLATO_PROVIDER_TIMEOUT_TEST_KEY", Some("test-key"), || {
            OpenAiCompatibleClient::from_config(
                "PLATO_PROVIDER_TIMEOUT_TEST_KEY",
                base_url,
                connect_timeout_ms,
                stream_idle_timeout_ms,
                None,
                None,
                TokenLimitField::MaxTokens,
            )
            .unwrap()
        })
    }

    struct StalledBodyProvider {
        base_url: String,
        ready_receiver: mpsc::Receiver<()>,
        release_sender: mpsc::Sender<()>,
        handle: thread::JoinHandle<()>,
    }

    fn spawn_stalled_body_provider(
        content_type: &'static str,
        prefix: Vec<u8>,
    ) -> StalledBodyProvider {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let (ready_sender, ready_receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            read_provider_request(&mut stream);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                prefix.len() + 4_096
            )
            .unwrap();
            stream.write_all(&prefix).unwrap();
            stream.flush().unwrap();
            ready_sender.send(()).unwrap();
            release_receiver
                .recv_timeout(Duration::from_secs(5))
                .unwrap();

            listener.set_nonblocking(true).unwrap();
            match listener.accept() {
                Err(error) if error.kind() == ErrorKind::WouldBlock => {}
                Ok(_) => panic!("canceled provider read issued an extra request"),
                Err(error) => panic!("extra-request probe failed: {error}"),
            }
        });
        StalledBodyProvider {
            base_url,
            ready_receiver,
            release_sender,
            handle,
        }
    }

    struct FixedReadError(ErrorKind);

    impl Read for FixedReadError {
        fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::new(self.0, "fixed provider read failure"))
        }
    }

    fn spawn_loopback_provider(
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

    fn read_provider_request(stream: &mut TcpStream) {
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
        let headers = String::from_utf8_lossy(&received[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .and_then(|value| value.trim().parse::<usize>().ok())
            })
            .unwrap_or(0);
        let mut body_length = received.len() - header_end;
        while body_length < content_length {
            let count = stream.read(&mut buffer).unwrap();
            assert_ne!(count, 0, "provider request ended before body");
            body_length += count;
        }
    }

    fn sse_delta(text: &str) -> String {
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

    fn sse_finish() -> String {
        concat!(
            "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
            "data: [DONE]\n\n",
        )
        .into()
    }

    fn assert_timeout(error: AppError) {
        match error {
            AppError::Io(error) => assert!(
                matches!(error.kind(), ErrorKind::TimedOut | ErrorKind::WouldBlock),
                "unexpected I/O error: {error}"
            ),
            AppError::Provider(message) => {
                let message = message.to_ascii_lowercase();
                assert!(
                    message.contains("timed out")
                        || message.contains("would block")
                        || message.contains("temporarily unavailable"),
                    "unexpected provider error: {message}"
                );
            }
            error => panic!("unexpected timeout error: {error}"),
        }
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
    fn non_streaming_normalization_limits_are_exact() {
        let exact_text = utf8_string_with_bytes(MAX_ASSISTANT_TEXT_BYTES);
        let response = non_stream_response(Some(exact_text), Vec::new())
            .into_model_response()
            .unwrap();
        assert_eq!(response.text().len(), MAX_ASSISTANT_TEXT_BYTES);
        let error = non_stream_response(
            Some(utf8_string_with_bytes(MAX_ASSISTANT_TEXT_BYTES + 1)),
            Vec::new(),
        )
        .into_model_response()
        .unwrap_err();
        assert!(matches!(
            error,
            AppError::Provider(message) if message == ASSISTANT_TEXT_LIMIT_ERROR
        ));

        let exact_name = utf8_string_with_bytes(MAX_TOOL_NAME_BYTES);
        validate_tool_call(&exact_name, "{}").unwrap();
        let error =
            validate_tool_call(&utf8_string_with_bytes(MAX_TOOL_NAME_BYTES + 1), "{}").unwrap_err();
        assert!(matches!(
            error,
            AppError::Provider(message) if message == TOOL_NAME_LIMIT_ERROR
        ));

        let exact_arguments = fragmented_unicode_json_arguments(MAX_TOOL_ARGUMENT_BYTES).concat();
        let response = non_stream_response(
            None,
            vec![provider_tool_call("call_0", "file_read", exact_arguments)],
        )
        .into_model_response()
        .unwrap();
        assert_eq!(response.tool_uses().len(), 1);
        let error = non_stream_response(
            None,
            vec![provider_tool_call(
                "call_0",
                "file_read",
                fragmented_unicode_json_arguments(MAX_TOOL_ARGUMENT_BYTES + 1).concat(),
            )],
        )
        .into_model_response()
        .unwrap_err();
        assert!(matches!(
            error,
            AppError::Provider(message) if message == TOOL_ARGUMENT_LIMIT_ERROR
        ));

        let exact_calls = (0..MAX_TOOL_CALLS)
            .map(|index| provider_tool_call(&format!("call_{index}"), "file_read", "{}".into()))
            .collect();
        let response = non_stream_response(None, exact_calls)
            .into_model_response()
            .unwrap();
        assert_eq!(response.tool_uses().len(), MAX_TOOL_CALLS);
        let over_calls = (0..=MAX_TOOL_CALLS)
            .map(|index| provider_tool_call(&format!("call_{index}"), "file_read", "{}".into()))
            .collect();
        let error = non_stream_response(None, over_calls)
            .into_model_response()
            .unwrap_err();
        assert!(matches!(
            error,
            AppError::Provider(message) if message == TOOL_CALL_LIMIT_ERROR
        ));

        let exact_calls = (0..4)
            .map(|index| {
                provider_tool_call(
                    &format!("call_{index}"),
                    "file_read",
                    json_arguments_with_bytes(MAX_TOOL_ARGUMENT_BYTES),
                )
            })
            .collect();
        let response = non_stream_response(None, exact_calls)
            .into_model_response()
            .unwrap();
        assert_eq!(response.tool_uses().len(), 4);
        let mut over_calls = (0..4)
            .map(|index| {
                provider_tool_call(
                    &format!("call_{index}"),
                    "file_read",
                    json_arguments_with_bytes(MAX_TOOL_ARGUMENT_BYTES),
                )
            })
            .collect::<Vec<_>>();
        over_calls.push(provider_tool_call("call_4", "file_read", "0".into()));
        let error = non_stream_response(None, over_calls)
            .into_model_response()
            .unwrap_err();
        assert!(matches!(
            error,
            AppError::Provider(message) if message == TOOL_ARGUMENTS_LIMIT_ERROR
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

    fn padded_non_stream_response(bytes: usize) -> Vec<u8> {
        let mut body = json!({
            "choices": [{
                "finish_reason": "stop",
                "message": {"content": "ok"}
            }]
        })
        .to_string()
        .into_bytes();
        assert!(body.len() <= bytes);
        body.resize(bytes, b' ');
        body
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

    fn utf8_string_with_bytes(bytes: usize) -> String {
        let mut value = "\u{754c}".repeat(bytes / "\u{754c}".len());
        value.push_str(&"x".repeat(bytes % "\u{754c}".len()));
        assert_eq!(value.len(), bytes);
        value
    }

    fn fragmented_unicode_json_arguments(bytes: usize) -> [String; 2] {
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

    fn json_arguments_with_bytes(bytes: usize) -> String {
        assert!(bytes >= 2);
        let arguments = format!("{{{}}}", " ".repeat(bytes - 2));
        assert_eq!(arguments.len(), bytes);
        arguments
    }

    fn non_stream_response(
        text: Option<String>,
        tool_calls: Vec<ChatToolCall>,
    ) -> ChatCompletionResponse {
        ChatCompletionResponse {
            model: None,
            choices: vec![ChatChoice {
                finish_reason: if tool_calls.is_empty() {
                    ChatFinishReason::Stop
                } else {
                    ChatFinishReason::ToolCalls
                },
                message: ChatResponseMessage {
                    content: text,
                    tool_calls: (!tool_calls.is_empty()).then_some(tool_calls),
                },
            }],
            usage: None,
        }
    }

    fn provider_tool_call(id: &str, name: &str, arguments: String) -> ChatToolCall {
        ChatToolCall {
            id: id.into(),
            tool_type: ChatToolType::Function,
            function: ChatFunctionCall {
                name: name.into(),
                arguments,
            },
        }
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

    fn usage_fixtures() -> Vec<(&'static str, Option<Value>, Option<ModelUsage>)> {
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
}
