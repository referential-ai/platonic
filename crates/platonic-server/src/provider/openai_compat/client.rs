use super::{
    response::{
        parse_non_stream_response, read_non_stream_response_with_cancel,
        read_streaming_response_with_cancel,
    },
    stream::parse_chat_completion_stream,
    types::{ChatFunctionCall, ChatToolCall, ChatToolType},
};
use crate::{
    AppError, AppResult,
    model::{ModelBlock, ModelMessage, ModelRequest, ModelResponse, ModelRole, ReasoningEffort},
    tool_catalog::{ToolSpec, provider_name_for_internal},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{io::BufReader, sync::atomic::AtomicBool, time::Duration};

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
#[serde(deny_unknown_fields)]
struct ChatFunctionDefinition {
    name: String,
    description: String,
    parameters: Value,
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
pub(super) mod tests {
    use super::super::response::RESPONSE_READ_CANCEL_POLL_INTERVAL;
    use super::super::stream::tests::{sse_delta, sse_finish};
    use super::*;
    use serde_json::json;
    use std::{
        io::{ErrorKind, Read, Write},
        net::{TcpListener, TcpStream},
        sync::mpsc,
        thread,
        time::Instant,
    };

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

    pub(in super::super) fn reasoning_request(
        reasoning_effort: Option<ReasoningEffort>,
    ) -> ModelRequest {
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

    pub(in super::super) fn timeout_test_client(
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

    pub(in super::super) fn read_provider_request(stream: &mut TcpStream) {
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
}
