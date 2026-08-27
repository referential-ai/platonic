use super::{
    responses::{parse_responses_response, parse_responses_stream},
    stream::parse_chat_completion_stream,
    types::{
        ASSISTANT_TEXT_LIMIT_ERROR, ChatFinishReason, ChatToolCall, ChatUsage,
        MAX_ASSISTANT_TEXT_BYTES, MAX_TOOL_ARGUMENTS_BYTES, MAX_TOOL_CALLS,
        TOOL_ARGUMENTS_LIMIT_ERROR, TOOL_CALL_LIMIT_ERROR, model_response, tool_use_from_provider,
        validate_tool_call,
    },
};
use crate::{
    AppError, AppResult,
    config::ProviderProtocol,
    model::{ModelBlock, ModelResponse},
};
use platonic_core::ModelName;
use serde::Deserialize;
use std::{
    io::{BufReader, Read},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

pub(super) const RESPONSE_READ_CANCEL_POLL_INTERVAL: Duration = Duration::from_millis(25);
const MAX_NON_STREAM_BODY_BYTES: usize = 1024 * 1024;
const NON_STREAM_BODY_LIMIT_ERROR: &str =
    "provider response exceeded the 1 MiB non-stream body limit";

// Keep ureq's blocking body reads off the run thread; rendezvous delivery leaves
// every result and user-visible delta under the run thread's cancel check.
enum StreamingBodyMessage {
    Delta(String),
    Finished(AppResult<ModelResponse>),
}

pub(super) fn parse_non_stream_response(reader: impl Read) -> AppResult<ModelResponse> {
    let body = read_non_stream_body(reader)?;
    serde_json::from_slice::<ChatCompletionResponse>(&body)
        .map_err(|error| AppError::Provider(error.to_string()))?
        .into_model_response()
}

pub(super) fn read_non_stream_response_with_cancel(
    response: ureq::Response,
    cancel: &AtomicBool,
    protocol: ProviderProtocol,
) -> AppResult<ModelResponse> {
    let (result_sender, result_receiver) = mpsc::sync_channel(0);
    let worker = thread::Builder::new()
        .name("plato-provider-body".into())
        .spawn(move || {
            let result = match protocol {
                ProviderProtocol::ChatCompletions => {
                    parse_non_stream_response(response.into_reader())
                }
                ProviderProtocol::Responses => parse_responses_response(response.into_reader()),
            };
            let _ = result_sender.send(result);
        })?;

    receive_body_result(result_receiver, cancel, worker)
}

pub(super) fn read_streaming_response_with_cancel(
    response: ureq::Response,
    cancel: &AtomicBool,
    protocol: ProviderProtocol,
    on_delta: &mut impl FnMut(&str) -> AppResult<()>,
) -> AppResult<ModelResponse> {
    let (message_sender, message_receiver) = mpsc::sync_channel(0);
    let worker = thread::Builder::new()
        .name("plato-provider-stream".into())
        .spawn(move || {
            let mut send_delta = |delta: &str| {
                message_sender
                    .send(StreamingBodyMessage::Delta(delta.to_owned()))
                    .map_err(|_| AppError::RunCanceled)
            };
            let result = match protocol {
                ProviderProtocol::ChatCompletions => parse_chat_completion_stream(
                    BufReader::new(response.into_reader()),
                    &mut send_delta,
                ),
                ProviderProtocol::Responses => {
                    parse_responses_stream(BufReader::new(response.into_reader()), &mut send_delta)
                }
            };
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

pub(super) fn read_non_stream_body(reader: impl Read) -> AppResult<Vec<u8>> {
    let mut body = Vec::new();
    reader
        .take((MAX_NON_STREAM_BODY_BYTES + 1) as u64)
        .read_to_end(&mut body)?;
    if body.len() > MAX_NON_STREAM_BODY_BYTES {
        return Err(AppError::Provider(NON_STREAM_BODY_LIMIT_ERROR.into()));
    }
    Ok(body)
}

#[derive(Debug, Deserialize)]
struct ChatCompletionResponse {
    model: Option<ModelName>,
    choices: Vec<ChatChoice>,
    usage: Option<ChatUsage>,
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

#[cfg(test)]
mod tests {
    use super::super::{
        client::tests::{read_provider_request, reasoning_request, timeout_test_client},
        stream::tests::sse_delta,
        types::{
            ChatFunctionCall, ChatToolType, MAX_TOOL_ARGUMENT_BYTES, MAX_TOOL_NAME_BYTES,
            TOOL_ARGUMENT_LIMIT_ERROR, TOOL_NAME_LIMIT_ERROR, fragmented_unicode_json_arguments,
            json_arguments_with_bytes, usage_fixtures, utf8_string_with_bytes,
        },
    };
    use super::*;
    use crate::model::ModelStop;
    use serde_json::json;
    use std::{
        io::{self, Cursor, ErrorKind, Read, Write},
        net::TcpListener,
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
}
