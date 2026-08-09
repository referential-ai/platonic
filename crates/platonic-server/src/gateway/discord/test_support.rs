#[cfg(unix)]
use super::daemon_bridge::{EVENT_PAGE_LIMIT, REQUIRED_CAPABILITIES};
use super::{
    DiscordGateway, DiscordPlatform,
    commands::{
        DISCORD_APPLICATION_COMMAND, DISCORD_CHAT_INPUT_COMMAND, DISCORD_MODEL_COMMAND,
        DISCORD_REASONING_COMMAND, DISCORD_STATUS_COMMAND, DISCORD_STRING_OPTION, DiscordAuthor,
        DiscordCommandHandler, InteractionCreateEvent, InteractionData, InteractionMember,
        InteractionOption,
    },
    daemon_bridge::{EYES_EMOJI, FAILURE_EMOJI, SUCCESS_EMOJI},
    rest::DiscordRestClient,
    websocket::{DISCORD_INTENTS, DiscordGatewayReceiver, DiscordMessage},
};
use super::{DiscordGatewayTimings, GatewayResult};
use platonic_client::client::DaemonConnectionConfig;
use platonic_protocol::{BufferedStreamEvent, RunOverrides};
#[cfg(unix)]
use platonic_protocol::{ERROR_LAGGED, Envelope, ProtocolErrorCode, RunStateName};
use serde_json::{Value, json};
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
#[cfg(unix)]
use std::path::Path;
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, Sender},
    },
    thread,
    time::{Duration, Instant},
};
use tungstenite::{
    Message, WebSocket, accept,
    error::ProtocolError,
    protocol::{CloseFrame, frame::coding::CloseCode},
};

pub(super) fn buffered_event(offset: u64, event: Value) -> BufferedStreamEvent {
    serde_json::from_value(json!({"offset": offset, "event": event})).unwrap()
}

pub(super) fn ledger_event(offset: u64, event: Value) -> BufferedStreamEvent {
    buffered_event(
        offset,
        json!({
            "kind": "ledger",
            "record": {
                "seq": offset,
                "occurred_at_ms": offset,
                "event": event
            }
        }),
    )
}

#[cfg(unix)]
pub(super) fn buffered_event_json(offset: u64, event: Value) -> Value {
    serde_json::to_value(buffered_event(offset, event)).unwrap()
}

#[cfg(unix)]
pub(super) fn ledger_event_json(offset: u64, event: Value) -> Value {
    serde_json::to_value(ledger_event(offset, event)).unwrap()
}

#[cfg(unix)]
fn request_params_value(request: &Envelope) -> Value {
    let request = serde_json::to_value(request.params.as_ref().unwrap()).unwrap();
    request.get("params").cloned().unwrap()
}

#[cfg(unix)]
pub(super) fn spawn_preflight_daemon(
    socket_path: &Path,
    workspace_id: String,
    capabilities: Vec<String>,
) -> thread::JoinHandle<Envelope> {
    let listener = UnixListener::bind(socket_path).unwrap();
    thread::spawn(move || {
        let (mut writer, _) = listener.accept().unwrap();
        let mut reader = BufReader::new(writer.try_clone().unwrap());
        let hello = read_daemon_request(&mut reader);
        write_daemon_response(
            &mut writer,
            hello.id.clone(),
            "hello",
            json!({
                "daemon_version": "test",
                "workspace_id": workspace_id,
                "ledger_path": "/tmp/agent.db",
                "capabilities": capabilities
            }),
        );
        hello
    })
}

pub(super) fn discord_message(author_id: u64, channel_id: u64, content: &str) -> DiscordMessage {
    DiscordMessage {
        id: 300,
        channel_id,
        author_id,
        content: content.into(),
    }
}

pub(super) fn discord_status_interaction(author_id: u64) -> InteractionCreateEvent {
    discord_command_interaction(author_id, 200, DISCORD_STATUS_COMMAND, None)
}

pub(super) fn discord_command_interaction(
    author_id: u64,
    channel_id: u64,
    command: &str,
    option: Option<(&str, &str)>,
) -> InteractionCreateEvent {
    InteractionCreateEvent {
        id: "400".into(),
        application_id: "100".into(),
        channel_id: channel_id.to_string(),
        kind: DISCORD_APPLICATION_COMMAND,
        token: "interaction-token".into(),
        data: Some(InteractionData {
            kind: DISCORD_CHAT_INPUT_COMMAND,
            name: command.into(),
            options: option
                .map(|(name, value)| InteractionOption {
                    kind: DISCORD_STRING_OPTION,
                    name: name.into(),
                    value: json!(value),
                })
                .into_iter()
                .collect(),
        }),
        member: Some(InteractionMember {
            user: DiscordAuthor {
                id: author_id.to_string(),
                bot: Some(false),
            },
        }),
        user: None,
    }
}

pub(super) fn test_command_handler(
    api_base: &str,
    workspace: &tempfile::TempDir,
    socket_path: PathBuf,
) -> DiscordCommandHandler {
    DiscordCommandHandler {
        api_base: api_base.into(),
        application_id: 100,
        daemon: DaemonConnectionConfig::resolve(workspace.path(), Some(socket_path)).unwrap(),
        owner_user_ids: std::collections::HashSet::from([42]),
        allowed_channel_ids: std::collections::HashSet::from([200]),
        base_model: "base-model".into(),
        overrides: Arc::new(Mutex::new(std::collections::HashMap::new())),
        daemon_client_timeout: DiscordGatewayTimings::default().daemon_client_timeout,
        presentation_timeout: DiscordGatewayTimings::default().presentation_timeout,
    }
}

pub(super) fn assert_reaction(request: &HttpRequest, method: &str, emoji: &str) {
    let emoji = match emoji {
        EYES_EMOJI => "%F0%9F%91%80",
        SUCCESS_EMOJI => "%E2%9C%85",
        FAILURE_EMOJI => "%E2%9D%8C",
        _ => panic!("unexpected test emoji"),
    };
    assert_eq!(request.method, method);
    assert_eq!(
        request.path,
        format!("/channels/200/messages/300/reactions/{emoji}/@me")
    );
    assert_eq!(request.authorization, "Bot test-token");
}

pub(super) fn test_platform(api_base: &str, message: DiscordMessage) -> DiscordPlatform {
    test_platform_messages(api_base, [message])
}

pub(super) fn test_platform_messages(
    api_base: &str,
    messages_to_send: impl IntoIterator<Item = DiscordMessage>,
) -> DiscordPlatform {
    let (sender, messages) = mpsc::channel();
    for message in messages_to_send {
        sender.send(Ok(message)).unwrap();
    }
    DiscordPlatform {
        rest: DiscordRestClient::new(api_base, "test-token".into()),
        messages,
        stop: Arc::new(AtomicBool::new(false)),
        worker: None,
    }
}

pub(super) fn test_gateway(
    workspace: &tempfile::TempDir,
    socket_path: PathBuf,
    platform: DiscordPlatform,
) -> DiscordGateway {
    test_gateway_with_overrides(
        workspace,
        socket_path,
        platform,
        Arc::new(Mutex::new(std::collections::HashMap::new())),
    )
}

pub(super) fn test_gateway_with_overrides(
    workspace: &tempfile::TempDir,
    socket_path: PathBuf,
    platform: DiscordPlatform,
    overrides: Arc<Mutex<std::collections::HashMap<u64, RunOverrides>>>,
) -> DiscordGateway {
    let daemon = DaemonConnectionConfig::resolve(workspace.path(), Some(socket_path)).unwrap();
    let timings = DiscordGatewayTimings::default();
    let mut gateway = DiscordGateway {
        platform,
        daemon,
        channel_config_paths: std::collections::HashMap::from([(200, "mapped.toml".into())]),
        owner_user_ids: std::collections::HashSet::from([42]),
        sessions: std::collections::HashMap::new(),
        overrides,
        daemon_client_timeout: timings.daemon_client_timeout,
        event_poll_delay: timings.event_poll_delay,
        reconnect_delay: timings.daemon_reconnect_delay,
    };
    gateway.event_poll_delay = Duration::ZERO;
    gateway.reconnect_delay = Duration::from_millis(5);
    gateway
}

pub(super) struct FakeRest {
    pub(super) base_url: String,
    pub(super) handle: thread::JoinHandle<Vec<HttpRequest>>,
}

pub(super) struct ObservedRest {
    pub(super) base_url: String,
    stop: Sender<()>,
    handle: thread::JoinHandle<Vec<HttpRequest>>,
}

impl ObservedRest {
    pub(super) fn finish(self) -> Vec<HttpRequest> {
        self.stop.send(()).unwrap();
        self.handle.join().unwrap()
    }
}

pub(super) struct FakeResponse {
    pub(super) status: u16,
    pub(super) body: Value,
    pub(super) headers: Vec<(&'static str, &'static str)>,
}

pub(super) enum FakeRestAction {
    Respond(FakeResponse),
    Disconnect,
}

pub(super) struct HttpRequest {
    pub(super) method: String,
    pub(super) path: String,
    pub(super) authorization: String,
    pub(super) body: Value,
    pub(super) received_at: Instant,
}

pub(super) fn spawn_fake_rest(
    expected_requests: usize,
    status: u16,
    gateway_url: Option<String>,
) -> FakeRest {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let handle = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut requests = Vec::new();
        while requests.len() < expected_requests && Instant::now() < deadline {
            let (mut stream, _) = match listener.accept() {
                Ok(connection) => connection,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                    continue;
                }
                Err(error) => panic!("discord REST accept failed: {error}"),
            };
            let request = read_http_request(&mut stream);
            let body = match request.path.as_str() {
                "/oauth2/applications/@me" => json!({"id": "100"}),
                "/applications/100/commands" => {
                    json!([
                        {"name": DISCORD_STATUS_COMMAND, "type": 1},
                        {"name": DISCORD_MODEL_COMMAND, "type": 1},
                        {"name": DISCORD_REASONING_COMMAND, "type": 1}
                    ])
                }
                "/gateway/bot" => json!({"url": gateway_url}),
                _ => json!({"id": "message_1"}),
            };
            write_http_response(&mut stream, status, &body);
            requests.push(request);
        }
        assert_eq!(requests.len(), expected_requests);
        requests
    });
    FakeRest { base_url, handle }
}

pub(super) fn spawn_scripted_rest(responses: Vec<FakeResponse>) -> FakeRest {
    spawn_scripted_rest_actions(responses.into_iter().map(FakeRestAction::Respond).collect())
}

pub(super) fn spawn_scripted_rest_actions(actions: Vec<FakeRestAction>) -> FakeRest {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let handle = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut requests = Vec::new();
        for action in actions {
            let (mut stream, _) = loop {
                match listener.accept() {
                    Ok(connection) => break connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(Instant::now() < deadline, "discord REST request timed out");
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("discord REST accept failed: {error}"),
                }
            };
            requests.push(read_http_request(&mut stream));
            if let FakeRestAction::Respond(response) = action {
                write_http_response_with_headers(
                    &mut stream,
                    response.status,
                    &response.body,
                    &response.headers,
                );
            }
        }
        requests
    });
    FakeRest { base_url, handle }
}

pub(super) fn spawn_observed_rest(actions: Vec<FakeRestAction>) -> ObservedRest {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let (stop, stopped) = mpsc::channel();
    let handle = thread::spawn(move || {
        let mut actions = actions.into_iter();
        let mut requests = Vec::new();
        loop {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    requests.push(read_http_request(&mut stream));
                    match actions.next() {
                        Some(FakeRestAction::Respond(response)) => {
                            write_http_response_with_headers(
                                &mut stream,
                                response.status,
                                &response.body,
                                &response.headers,
                            );
                        }
                        Some(FakeRestAction::Disconnect) => {}
                        None => {
                            write_http_response(&mut stream, 200, &json!({"id": "unexpected"}));
                        }
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    match stopped.try_recv() {
                        Ok(()) | Err(mpsc::TryRecvError::Disconnected) => break,
                        Err(mpsc::TryRecvError::Empty) => {
                            thread::sleep(Duration::from_millis(1));
                        }
                    }
                }
                Err(error) => panic!("discord REST accept failed: {error}"),
            }
        }
        requests
    });
    ObservedRest {
        base_url,
        stop,
        handle,
    }
}

pub(super) fn spawn_stalled_rest(delay: Duration) -> FakeRest {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let request = read_http_request(&mut stream);
        thread::sleep(delay);
        vec![request]
    });
    FakeRest { base_url, handle }
}

fn read_http_request(stream: &mut TcpStream) -> HttpRequest {
    let read_timeout = Duration::from_secs(2);
    stream.set_read_timeout(Some(read_timeout)).unwrap();
    let read_deadline = Instant::now() + read_timeout;
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut request_line = String::new();
    loop {
        match reader.read_line(&mut request_line) {
            Ok(_) => break,
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    && Instant::now() < read_deadline =>
            {
                thread::sleep(
                    read_deadline
                        .saturating_duration_since(Instant::now())
                        .min(Duration::from_millis(1)),
                );
            }
            Err(error) => panic!("discord REST request line read failed: {error}"),
        }
    }
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().unwrap().to_owned();
    let path = request_parts.next().unwrap().to_owned();
    let mut content_length = 0;
    let mut authorization = String::new();
    loop {
        let mut line = String::new();
        loop {
            match reader.read_line(&mut line) {
                Ok(_) => break,
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        && Instant::now() < read_deadline =>
                {
                    thread::sleep(
                        read_deadline
                            .saturating_duration_since(Instant::now())
                            .min(Duration::from_millis(1)),
                    );
                }
                Err(error) => panic!("discord REST header read failed: {error}"),
            }
        }
        if line == "\r\n" {
            break;
        }
        if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
            content_length = value.trim().parse().unwrap();
        }
        if line.to_ascii_lowercase().starts_with("authorization:") {
            authorization = line.split_once(':').unwrap().1.trim().to_owned();
        }
    }
    let mut body = vec![0; content_length];
    let mut body_read = 0;
    while body_read < body.len() {
        match reader.read(&mut body[body_read..]) {
            Ok(0) => panic!("discord REST request ended before body"),
            Ok(count) => body_read += count,
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    && Instant::now() < read_deadline =>
            {
                thread::sleep(
                    read_deadline
                        .saturating_duration_since(Instant::now())
                        .min(Duration::from_millis(1)),
                );
            }
            Err(error) => panic!("discord REST body read failed: {error}"),
        }
    }
    HttpRequest {
        method,
        path,
        authorization,
        body: if body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&body).unwrap()
        },
        received_at: Instant::now(),
    }
}

fn write_http_response(stream: &mut TcpStream, status: u16, body: &Value) {
    write_http_response_with_headers(stream, status, body, &[]);
}

fn write_http_response_with_headers(
    stream: &mut TcpStream,
    status: u16,
    body: &Value,
    headers: &[(&str, &str)],
) {
    let body = if status == 204 {
        Vec::new()
    } else {
        serde_json::to_vec(body).unwrap()
    };
    let reason = match status {
        200 => "OK",
        204 => "No Content",
        429 => "Too Many Requests",
        _ => "Error",
    };
    let headers = headers
        .iter()
        .map(|(name, value)| format!("{name}: {value}\r\n"))
        .collect::<String>();
    write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\n{headers}content-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    )
    .unwrap();
    stream.write_all(&body).unwrap();
}

pub(super) const TEST_GATEWAY_BOUND: Duration = Duration::from_secs(2);
const TEST_GATEWAY_READ_TIMEOUT: Duration = Duration::from_millis(10);
pub(super) const TEST_SESSION_ID: &str = "discord_session";
pub(super) const TEST_READY_SEQUENCE: u64 = 17;
const TEST_LATEST_SEQUENCE: u64 = 23;

pub(super) fn test_gateway_listener() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let websocket_url = format!("ws://{}", listener.local_addr().unwrap());
    (listener, websocket_url)
}

pub(super) fn accept_test_websocket(listener: &TcpListener) -> WebSocket<TcpStream> {
    listener.set_nonblocking(true).unwrap();
    let deadline = Instant::now() + TEST_GATEWAY_BOUND;
    let stream = loop {
        match listener.accept() {
            Ok((stream, _)) => break stream,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    Instant::now() < deadline,
                    "gateway connection exceeded the local test bound"
                );
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => panic!("gateway websocket accept failed: {error}"),
        }
    };
    stream.set_nonblocking(false).unwrap();
    stream.set_read_timeout(Some(TEST_GATEWAY_BOUND)).unwrap();
    accept(stream).unwrap()
}

pub(super) fn spawn_test_gateway_receiver(
    websocket_url: &str,
) -> (
    Arc<AtomicBool>,
    Receiver<GatewayResult<DiscordMessage>>,
    thread::JoinHandle<()>,
) {
    let (sender, messages) = mpsc::channel();
    let stop = Arc::new(AtomicBool::new(false));
    let receiver = DiscordGatewayReceiver {
        token: "test-token".into(),
        initial_url: websocket_url.into(),
        read_timeout: TEST_GATEWAY_READ_TIMEOUT,
        hello_timeout: DiscordGatewayTimings::default().gateway_hello_timeout,
        reconnect_delay: Duration::from_millis(1),
        commands: DiscordCommandHandler {
            api_base: "http://127.0.0.1".into(),
            application_id: 100,
            daemon: DaemonConnectionConfig {
                workspace_root: PathBuf::from("/"),
                socket_path: PathBuf::from("/tmp/missing-plato-agent.sock"),
            },
            owner_user_ids: std::collections::HashSet::from([42]),
            allowed_channel_ids: std::collections::HashSet::from([200]),
            base_model: "base-model".into(),
            overrides: Arc::new(Mutex::new(std::collections::HashMap::new())),
            daemon_client_timeout: DiscordGatewayTimings::default().daemon_client_timeout,
            presentation_timeout: DiscordGatewayTimings::default().presentation_timeout,
        },
    };
    let worker_stop = Arc::clone(&stop);
    let worker = thread::spawn(move || receiver.run(sender, worker_stop));
    (stop, messages, worker)
}

pub(super) fn finish_recoverable_gateway_receiver(
    stop: Arc<AtomicBool>,
    messages: Receiver<GatewayResult<DiscordMessage>>,
    worker: thread::JoinHandle<()>,
) {
    stop.store(true, Ordering::Relaxed);
    worker.join().unwrap();
    let results = messages.into_iter().collect::<Vec<_>>();
    assert!(
        results.is_empty(),
        "recoverable gateway path emitted {} receiver results",
        results.len()
    );
}

pub(super) fn hello_and_read_gateway_auth(
    socket: &mut WebSocket<TcpStream>,
    heartbeat_interval: u64,
) -> Value {
    send_websocket_json(
        socket,
        json!({"op": 10, "d": {"heartbeat_interval": heartbeat_interval}}),
    );
    read_websocket_json(socket).expect("gateway disconnected before authenticating")
}

pub(super) fn establish_test_gateway_session(
    socket: &mut WebSocket<TcpStream>,
    websocket_url: &str,
) {
    let identify = hello_and_read_gateway_auth(socket, 60_000);
    assert_gateway_identify(&identify);
    send_websocket_json(
        socket,
        json!({
            "op": 0,
            "s": TEST_READY_SEQUENCE,
            "t": "READY",
            "d": {
                "session_id": TEST_SESSION_ID,
                "resume_gateway_url": websocket_url
            }
        }),
    );
    send_websocket_json(
        socket,
        json!({
            "op": 0,
            "s": TEST_LATEST_SEQUENCE,
            "t": "CHANNEL_UPDATE",
            "d": {}
        }),
    );
}

pub(super) fn assert_gateway_identify(payload: &Value) {
    assert_eq!(payload["op"], 2);
    assert_eq!(payload["d"]["token"], "test-token");
    assert_eq!(payload["d"]["intents"], DISCORD_INTENTS);
    assert!(payload["d"].get("session_id").is_none());
    assert!(payload["d"].get("seq").is_none());
}

pub(super) fn assert_gateway_resume(payload: &Value) {
    assert_eq!(payload["op"], 6);
    assert_eq!(payload["d"]["token"], "test-token");
    assert_eq!(payload["d"]["session_id"], TEST_SESSION_ID);
    assert_eq!(payload["d"]["seq"], TEST_LATEST_SEQUENCE);
}

pub(super) fn assert_recoverable_gateway_close(code: Option<u16>, reidentify: bool) {
    let (listener, websocket_url) = test_gateway_listener();
    let (stop, messages, worker) = spawn_test_gateway_receiver(&websocket_url);
    let mut socket = accept_test_websocket(&listener);
    establish_test_gateway_session(&mut socket, &websocket_url);

    send_websocket_close(&mut socket, code);

    let mut reconnected = accept_test_websocket(&listener);
    let auth = hello_and_read_gateway_auth(&mut reconnected, 60_000);
    finish_recoverable_gateway_receiver(stop, messages, worker);
    if reidentify {
        assert_gateway_identify(&auth);
    } else {
        assert_gateway_resume(&auth);
    }
}

pub(super) fn assert_fatal_gateway_close(code: u16) {
    let (listener, websocket_url) = test_gateway_listener();
    let (stop, messages, worker) = spawn_test_gateway_receiver(&websocket_url);
    let mut socket = accept_test_websocket(&listener);
    let identify = hello_and_read_gateway_auth(&mut socket, 60_000);
    assert_gateway_identify(&identify);

    send_websocket_close(&mut socket, Some(code));

    let result = match messages.recv_timeout(TEST_GATEWAY_BOUND) {
        Ok(result) => result,
        Err(error) => {
            stop.store(true, Ordering::Relaxed);
            panic!("fatal close code {code} did not stop the receiver: {error}");
        }
    };
    let error = result.expect_err("fatal close emitted a Discord message");
    worker.join().unwrap();
    assert!(messages.into_iter().next().is_none());
    assert_eq!(
        error.to_string(),
        format!("provider error: discord gateway closed with fatal code {code}")
    );
}

pub(super) fn send_websocket_close(socket: &mut WebSocket<TcpStream>, code: Option<u16>) {
    let frame = code.map(|code| CloseFrame {
        code: CloseCode::from(code),
        reason: "test close".into(),
    });
    socket.send(Message::Close(frame)).unwrap();
}

pub(super) fn read_websocket_json(socket: &mut WebSocket<TcpStream>) -> Option<Value> {
    loop {
        match socket.read() {
            Ok(Message::Text(text)) => {
                return Some(serde_json::from_str(&text).unwrap());
            }
            Ok(Message::Ping(payload)) => socket.send(Message::Pong(payload)).unwrap(),
            Ok(Message::Close(_))
            | Err(tungstenite::Error::ConnectionClosed)
            | Err(tungstenite::Error::AlreadyClosed)
            | Err(tungstenite::Error::Protocol(ProtocolError::ResetWithoutClosingHandshake)) => {
                return None;
            }
            Ok(_) => {}
            Err(error) => panic!("fake websocket read failed: {error}"),
        }
    }
}

pub(super) fn send_websocket_json(socket: &mut WebSocket<TcpStream>, payload: Value) {
    socket
        .send(Message::Text(payload.to_string().into()))
        .unwrap();
}

#[cfg(unix)]
pub(super) fn spawn_finished_daemon(
    socket_path: &Path,
    method: &str,
    session_id: &str,
    answer: &str,
) -> thread::JoinHandle<Value> {
    let listener = UnixListener::bind(socket_path).unwrap();
    let method = method.to_owned();
    let session_id = session_id.to_owned();
    let answer = answer.to_owned();
    thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut writer = stream.try_clone().unwrap();
        let mut reader = BufReader::new(stream);
        respond_hello(&mut reader, &mut writer);
        let request = read_daemon_request(&mut reader);
        assert_eq!(request.method.as_deref(), Some(method.as_str()));
        let request_params = request_params_value(&request);
        write_daemon_response(
            &mut writer,
            request.id,
            &method,
            json!({
                "run_id": "run_1",
                "session_id": session_id,
                "ledger_path": "/tmp/agent.db",
                "status": "running",
                "final_answer": null
            }),
        );
        let events = read_daemon_request(&mut reader);
        assert_eq!(events.method.as_deref(), Some("events.stream"));
        write_daemon_response(
            &mut writer,
            events.id,
            "events.stream",
            json!({
                "run_id": "run_1",
                "from_offset": 0,
                "next_offset": 1,
                "status": "finished",
                "events": []
            }),
        );
        let transcript = read_daemon_request(&mut reader);
        assert_eq!(transcript.method.as_deref(), Some("transcript.read"));
        let transcript_params = request_params_value(&transcript);
        assert_eq!(transcript_params["run_id"], "run_1");
        assert!(transcript_params["session_id"].is_null());
        write_daemon_response(
            &mut writer,
            transcript.id,
            "transcript.read",
            json!({
                "run_id": "run_1",
                "status": "finished",
                "final_answer": answer,
                "transcript": "rendered text must not be parsed"
            }),
        );
        request_params
    })
}

#[cfg(unix)]
pub(super) fn spawn_catch_up_daemon(socket_path: &Path) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket_path).unwrap();
    thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut writer = stream.try_clone().unwrap();
        let mut reader = BufReader::new(stream);
        respond_hello(&mut reader, &mut writer);
        let start = read_daemon_request(&mut reader);
        write_daemon_response(
            &mut writer,
            start.id,
            "run.start",
            json!({
                "run_id": "run_1",
                "session_id": "session_1",
                "ledger_path": "/tmp/agent.db",
                "status": "running",
                "final_answer": null
            }),
        );

        let catch_up = read_daemon_request(&mut reader);
        let mut events = vec![buffered_event_json(
            0,
            json!({
                "kind": "approval_requested",
                "run_id": "run_1",
                "tool_call_id": "call_1",
                "tool_name": "file.write",
                "effect": "workspace_write",
                "reason": "approval required",
                "approval_preview": "write note.txt"
            }),
        )];
        events.extend(
            (1..EVENT_PAGE_LIMIT)
                .map(|offset| json!({"offset": offset, "event": {"kind": "delta"}})),
        );
        write_daemon_response(
            &mut writer,
            catch_up.id,
            "events.stream",
            json!({
                "run_id": "run_1",
                "from_offset": 0,
                "next_offset": EVENT_PAGE_LIMIT,
                "status": "running",
                "events": events
            }),
        );

        let resolution = read_daemon_request(&mut reader);
        let mut events = vec![ledger_event_json(
            EVENT_PAGE_LIMIT as u64,
            json!({
                "event": "approval_granted",
                "run_id": "run_1",
                "call_id": "call_1",
                "actor_id": "human_1"
            }),
        )];
        events.extend((1..EVENT_PAGE_LIMIT).map(|offset| {
            json!({
                "offset": EVENT_PAGE_LIMIT + offset,
                "event": {"kind": "delta"}
            })
        }));
        write_daemon_response(
            &mut writer,
            resolution.id,
            "events.stream",
            json!({
                "run_id": "run_1",
                "from_offset": EVENT_PAGE_LIMIT,
                "next_offset": EVENT_PAGE_LIMIT * 2,
                "status": "running",
                "events": events
            }),
        );

        let running = read_daemon_request(&mut reader);
        write_daemon_response(
            &mut writer,
            running.id,
            "events.stream",
            json!({
                "run_id": "run_1",
                "from_offset": EVENT_PAGE_LIMIT * 2,
                "next_offset": EVENT_PAGE_LIMIT * 2,
                "status": "running",
                "events": []
            }),
        );

        let finished = read_daemon_request(&mut reader);
        write_daemon_response(
            &mut writer,
            finished.id,
            "events.stream",
            json!({
                "run_id": "run_1",
                "from_offset": EVENT_PAGE_LIMIT * 2,
                "next_offset": EVENT_PAGE_LIMIT * 2,
                "status": "finished",
                "events": []
            }),
        );

        let transcript = read_daemon_request(&mut reader);
        write_daemon_response(
            &mut writer,
            transcript.id,
            "transcript.read",
            json!({
                "run_id": "run_1",
                "status": "finished",
                "final_answer": "caught up",
                "transcript": "not parsed"
            }),
        );
    })
}

#[cfg(unix)]
pub(super) fn spawn_approval_daemon(socket_path: &Path) -> thread::JoinHandle<Vec<&'static str>> {
    let listener = UnixListener::bind(socket_path).unwrap();
    thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut writer = stream.try_clone().unwrap();
        let mut reader = BufReader::new(stream);
        let mut methods = Vec::new();

        respond_hello(&mut reader, &mut writer);
        methods.push("hello");

        let start = read_daemon_request(&mut reader);
        assert_eq!(start.method.as_deref(), Some("run.start"));
        methods.push("run.start");
        write_daemon_response(
            &mut writer,
            start.id,
            "run.start",
            json!({
                "run_id": "run_1",
                "session_id": "session_1",
                "ledger_path": "/tmp/agent.db",
                "status": "running",
                "final_answer": null
            }),
        );

        let pending = read_daemon_request(&mut reader);
        assert_eq!(pending.method.as_deref(), Some("events.stream"));
        methods.push("events.stream");
        write_daemon_response(
            &mut writer,
            pending.id,
            "events.stream",
            json!({
                "run_id": "run_1",
                "from_offset": 0,
                "next_offset": 2,
                "status": "running",
                "events": [
                    buffered_event_json(0, json!({
                        "kind": "approval_requested",
                        "run_id": "run_1",
                        "tool_call_id": "call_1",
                        "tool_name": "file.write",
                        "effect": "workspace_write",
                        "reason": "approval required"
                    })),
                    ledger_event_json(1, json!({
                        "event": "tool_call_proposed",
                        "run_id": "run_1",
                        "turn_id": "turn_1",
                        "call": {
                            "id": "call_1",
                            "tool": "file.write",
                            "effect": "workspace_write",
                            "input": {
                                "path": "note.txt",
                                "content": "hello"
                            }
                        }
                    }))
                ]
            }),
        );

        let resolved = read_daemon_request(&mut reader);
        assert_eq!(resolved.method.as_deref(), Some("events.stream"));
        methods.push("events.stream");
        write_daemon_response(
            &mut writer,
            resolved.id,
            "events.stream",
            json!({
                "run_id": "run_1",
                "from_offset": 2,
                "next_offset": 3,
                "status": "running",
                "events": [ledger_event_json(2, json!({
                    "event": "approval_granted",
                    "run_id": "run_1",
                    "call_id": "call_1",
                    "actor_id": "human_1"
                }))]
            }),
        );

        let finished = read_daemon_request(&mut reader);
        assert_eq!(finished.method.as_deref(), Some("events.stream"));
        methods.push("events.stream");
        write_daemon_response(
            &mut writer,
            finished.id,
            "events.stream",
            json!({
                "run_id": "run_1",
                "from_offset": 3,
                "next_offset": 3,
                "status": "finished",
                "events": []
            }),
        );

        let transcript = read_daemon_request(&mut reader);
        assert_eq!(transcript.method.as_deref(), Some("transcript.read"));
        methods.push("transcript.read");
        assert_eq!(request_params_value(&transcript)["run_id"], "run_1");
        write_daemon_response(
            &mut writer,
            transcript.id,
            "transcript.read",
            json!({
                "run_id": "run_1",
                "status": "finished",
                "final_answer": "saved note",
                "transcript": "not parsed"
            }),
        );
        methods
    })
}

#[cfg(unix)]
pub(super) fn spawn_folded_terminal_daemon(socket_path: &Path) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket_path).unwrap();
    thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut writer = stream.try_clone().unwrap();
        let mut reader = BufReader::new(stream);
        respond_hello(&mut reader, &mut writer);
        let start = read_daemon_request(&mut reader);
        write_daemon_response(
            &mut writer,
            start.id,
            "run.start",
            json!({
                "run_id": "run_1",
                "session_id": "session_1",
                "ledger_path": "/tmp/agent.db",
                "status": "running",
                "final_answer": null
            }),
        );
        let events = read_daemon_request(&mut reader);
        write_daemon_response(
            &mut writer,
            events.id,
            "events.stream",
            json!({
                "run_id": "run_1",
                "from_offset": 0,
                "next_offset": 2,
                "status": "finished",
                "events": [
                    buffered_event_json(0, json!({
                        "kind": "approval_requested",
                        "run_id": "run_1",
                        "tool_call_id": "call_1",
                        "tool_name": "file.write",
                        "effect": "workspace_write",
                        "reason": "approval required",
                        "approval_preview": "write note.txt"
                    })),
                    ledger_event_json(1, json!({
                        "event": "approval_granted",
                        "run_id": "run_1",
                        "call_id": "call_1",
                        "actor_id": "human_1"
                    }))
                ]
            }),
        );
        let transcript = read_daemon_request(&mut reader);
        write_daemon_response(
            &mut writer,
            transcript.id,
            "transcript.read",
            json!({
                "run_id": "run_1",
                "status": "finished",
                "final_answer": "saved without stale effects",
                "transcript": "not parsed"
            }),
        );
    })
}

#[cfg(unix)]
pub(super) fn spawn_failed_daemon(socket_path: &Path) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket_path).unwrap();
    thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut writer = stream.try_clone().unwrap();
        let mut reader = BufReader::new(stream);
        respond_hello(&mut reader, &mut writer);
        let start = read_daemon_request(&mut reader);
        write_daemon_response(
            &mut writer,
            start.id,
            "run.start",
            json!({
                "run_id": "run_1",
                "session_id": "session_1",
                "ledger_path": "/tmp/agent.db",
                "status": "running",
                "final_answer": null
            }),
        );
        let events = read_daemon_request(&mut reader);
        write_daemon_response(
            &mut writer,
            events.id,
            "events.stream",
            json!({
                "run_id": "run_1",
                "from_offset": 0,
                "next_offset": 0,
                "status": "failed",
                "events": []
            }),
        );
        let transcript = read_daemon_request(&mut reader);
        assert_eq!(request_params_value(&transcript)["run_id"], "run_1");
        write_daemon_response(
            &mut writer,
            transcript.id,
            "transcript.read",
            json!({
                "run_id": "run_1",
                "status": "failed",
                "final_answer": null,
                "transcript": "run_failed"
            }),
        );
    })
}

#[cfg(unix)]
pub(super) fn spawn_status_query_daemon(socket_path: &Path) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket_path).unwrap();
    listener.set_nonblocking(true).unwrap();
    thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(3);
        let (stream, _) = loop {
            match listener.accept() {
                Ok(connection) => break connection,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < deadline, "daemon query timed out");
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("daemon query accept failed: {error}"),
            }
        };
        let mut writer = stream.try_clone().unwrap();
        let mut reader = BufReader::new(stream);
        respond_hello(&mut reader, &mut writer);
        let sessions = read_daemon_request(&mut reader);
        assert_eq!(sessions.method.as_deref(), Some("sessions.list"));
        let sessions_result = ["running", "cancel_requested", "finished"]
            .into_iter()
            .enumerate()
            .map(|(index, status)| {
                json!({
                    "session_id": format!("session_{index}"),
                    "run_id": format!("run_{index}"),
                    "status": status,
                    "latest_question": format!("question {index}"),
                    "ledger_path": "/tmp/agent.db"
                })
            })
            .collect::<Vec<_>>();
        write_daemon_response(
            &mut writer,
            sessions.id,
            "sessions.list",
            json!({"sessions": sessions_result}),
        );
    })
}

#[cfg(unix)]
pub(super) fn spawn_status_daemon(
    socket_path: &Path,
    statuses: Vec<RunStateName>,
) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket_path).unwrap();
    thread::spawn(move || {
        let terminal_status = *statuses.last().unwrap();
        let (stream, _) = listener.accept().unwrap();
        let mut writer = stream.try_clone().unwrap();
        let mut reader = BufReader::new(stream);
        respond_hello(&mut reader, &mut writer);
        let start = read_daemon_request(&mut reader);
        write_daemon_response(
            &mut writer,
            start.id,
            "run.start",
            json!({
                "run_id": "run_1",
                "session_id": "session_1",
                "ledger_path": "/tmp/agent.db",
                "status": "running",
                "final_answer": null
            }),
        );
        for status in statuses {
            let events = read_daemon_request(&mut reader);
            write_daemon_response(
                &mut writer,
                events.id,
                "events.stream",
                json!({
                    "run_id": "run_1",
                    "from_offset": 0,
                    "next_offset": 0,
                    "status": status.to_string(),
                    "events": []
                }),
            );
        }
        let transcript = read_daemon_request(&mut reader);
        write_daemon_response(
            &mut writer,
            transcript.id,
            "transcript.read",
            json!({
                "run_id": "run_1",
                "status": terminal_status.to_string(),
                "final_answer": null,
                "transcript": "not parsed"
            }),
        );
    })
}

#[cfg(unix)]
pub(super) fn spawn_canceled_event_daemon(socket_path: &Path) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket_path).unwrap();
    thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut writer = stream.try_clone().unwrap();
        let mut reader = BufReader::new(stream);
        respond_hello(&mut reader, &mut writer);
        let start = read_daemon_request(&mut reader);
        write_daemon_response(
            &mut writer,
            start.id,
            "run.start",
            json!({
                "run_id": "run_1",
                "session_id": "session_1",
                "ledger_path": "/tmp/agent.db",
                "status": "running",
                "final_answer": null
            }),
        );
        let canceled = read_daemon_request(&mut reader);
        write_daemon_response(
            &mut writer,
            canceled.id,
            "events.stream",
            json!({
                "run_id": "run_1",
                "from_offset": 0,
                "next_offset": 2,
                "status": "running",
                "events": [
                    buffered_event_json(0, json!({
                        "kind": "approval_requested",
                        "run_id": "run_1",
                        "tool_call_id": "call_1",
                        "tool_name": "file.write",
                        "effect": "workspace_write",
                        "reason": "approval required",
                        "approval_preview": "write note.txt"
                    })),
                    buffered_event_json(
                        1,
                        json!({"kind": "canceled", "run_id": "run_1"})
                    )
                ]
            }),
        );
        let terminal = read_daemon_request(&mut reader);
        write_daemon_response(
            &mut writer,
            terminal.id,
            "events.stream",
            json!({
                "run_id": "run_1",
                "from_offset": 2,
                "next_offset": 2,
                "status": "canceled",
                "events": []
            }),
        );
        let transcript = read_daemon_request(&mut reader);
        write_daemon_response(
            &mut writer,
            transcript.id,
            "transcript.read",
            json!({
                "run_id": "run_1",
                "status": "canceled",
                "final_answer": null,
                "transcript": "not parsed"
            }),
        );
    })
}

#[cfg(unix)]
pub(super) fn spawn_advanced_session_daemon(
    socket_path: &Path,
    answer: &str,
) -> thread::JoinHandle<()> {
    let first_listener = UnixListener::bind(socket_path).unwrap();
    let socket_path = socket_path.to_path_buf();
    let answer = answer.to_owned();
    thread::spawn(move || {
        {
            let (stream, _) = first_listener.accept().unwrap();
            let mut writer = stream.try_clone().unwrap();
            let mut reader = BufReader::new(stream);
            respond_hello(&mut reader, &mut writer);
            let start = read_daemon_request(&mut reader);
            assert_eq!(start.method.as_deref(), Some("run.start"));
            write_daemon_response(
                &mut writer,
                start.id,
                "run.start",
                json!({
                    "run_id": "run_1",
                    "session_id": "session_1",
                    "ledger_path": "/tmp/agent.db",
                    "status": "running",
                    "final_answer": null
                }),
            );
            let events = read_daemon_request(&mut reader);
            assert_eq!(events.method.as_deref(), Some("events.stream"));
        }
        drop(first_listener);
        std::fs::remove_file(&socket_path).unwrap();
        let second_listener = UnixListener::bind(&socket_path).unwrap();
        let (stream, _) = second_listener.accept().unwrap();
        let mut writer = stream.try_clone().unwrap();
        let mut reader = BufReader::new(stream);
        respond_hello(&mut reader, &mut writer);
        let sessions = read_daemon_request(&mut reader);
        assert_eq!(sessions.method.as_deref(), Some("sessions.list"));
        write_daemon_response(
            &mut writer,
            sessions.id,
            "sessions.list",
            json!({
                "sessions": [{
                    "session_id": "session_1",
                    "run_id": "run_2",
                    "status": "running",
                    "latest_question": "newer local run",
                    "ledger_path": "/tmp/agent.db"
                }]
            }),
        );
        let transcript = read_daemon_request(&mut reader);
        assert_eq!(transcript.method.as_deref(), Some("transcript.read"));
        write_daemon_response(
            &mut writer,
            transcript.id,
            "transcript.read",
            json!({
                "run_id": "run_1",
                "status": "finished",
                "final_answer": answer,
                "transcript": "not the answer"
            }),
        );
    })
}

#[cfg(unix)]
pub(super) fn spawn_reconnecting_pending_daemon(socket_path: &Path) -> thread::JoinHandle<()> {
    let first_listener = UnixListener::bind(socket_path).unwrap();
    let socket_path = socket_path.to_path_buf();
    thread::spawn(move || {
        {
            let (stream, _) = first_listener.accept().unwrap();
            let mut writer = stream.try_clone().unwrap();
            let mut reader = BufReader::new(stream);
            respond_hello(&mut reader, &mut writer);
            let start = read_daemon_request(&mut reader);
            write_daemon_response(
                &mut writer,
                start.id,
                "run.start",
                json!({
                    "run_id": "run_1",
                    "session_id": "session_1",
                    "ledger_path": "/tmp/agent.db",
                    "status": "running",
                    "final_answer": null
                }),
            );
            let pending = read_daemon_request(&mut reader);
            write_daemon_response(
                &mut writer,
                pending.id,
                "events.stream",
                json!({
                    "run_id": "run_1",
                    "from_offset": 0,
                    "next_offset": 1,
                    "status": "running",
                    "events": [buffered_event_json(0, json!({
                        "kind": "approval_requested",
                        "run_id": "run_1",
                        "tool_call_id": "call_1",
                        "tool_name": "file.write",
                        "effect": "workspace_write",
                        "reason": "approval required",
                        "approval_preview": "write note.txt"
                    }))]
                }),
            );
            let reconnecting = read_daemon_request(&mut reader);
            assert_eq!(reconnecting.method.as_deref(), Some("events.stream"));
        }
        drop(first_listener);
        std::fs::remove_file(&socket_path).unwrap();
        let second_listener = UnixListener::bind(&socket_path).unwrap();
        let (stream, _) = second_listener.accept().unwrap();
        let mut writer = stream.try_clone().unwrap();
        let mut reader = BufReader::new(stream);
        respond_hello(&mut reader, &mut writer);
        let sessions = read_daemon_request(&mut reader);
        write_daemon_response(
            &mut writer,
            sessions.id,
            "sessions.list",
            json!({
                "sessions": [{
                    "session_id": "session_1",
                    "run_id": "run_1",
                    "status": "running",
                    "latest_question": "hello",
                    "ledger_path": "/tmp/agent.db"
                }]
            }),
        );
        let running = read_daemon_request(&mut reader);
        assert!(request_params_value(&running).get("from_offset").is_none());
        write_daemon_response(
            &mut writer,
            running.id,
            "events.stream",
            json!({
                "run_id": "run_1",
                "from_offset": 2,
                "next_offset": 2,
                "status": "running",
                "events": []
            }),
        );
        let finished = read_daemon_request(&mut reader);
        write_daemon_response(
            &mut writer,
            finished.id,
            "events.stream",
            json!({
                "run_id": "run_1",
                "from_offset": 2,
                "next_offset": 2,
                "status": "finished",
                "events": []
            }),
        );
        let transcript = read_daemon_request(&mut reader);
        write_daemon_response(
            &mut writer,
            transcript.id,
            "transcript.read",
            json!({
                "run_id": "run_1",
                "status": "finished",
                "final_answer": "answer after reconnect",
                "transcript": "not parsed"
            }),
        );
    })
}

#[cfg(unix)]
pub(super) fn spawn_lagged_daemon(socket_path: &Path, answer: &str) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket_path).unwrap();
    let answer = answer.to_owned();
    thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut writer = stream.try_clone().unwrap();
        let mut reader = BufReader::new(stream);
        respond_hello(&mut reader, &mut writer);
        let start = read_daemon_request(&mut reader);
        assert_eq!(start.method.as_deref(), Some("run.start"));
        write_daemon_response(
            &mut writer,
            start.id,
            "run.start",
            json!({
                "run_id": "run_1",
                "session_id": "session_1",
                "ledger_path": "/tmp/agent.db",
                "status": "running",
                "final_answer": null
            }),
        );
        let pending = read_daemon_request(&mut reader);
        assert_eq!(request_params_value(&pending)["from_offset"], 0);
        write_daemon_response(
            &mut writer,
            pending.id,
            "events.stream",
            json!({
                "run_id": "run_1",
                "from_offset": 0,
                "next_offset": 1,
                "status": "running",
                "events": [buffered_event_json(0, json!({
                    "kind": "approval_requested",
                    "run_id": "run_1",
                    "tool_call_id": "call_1",
                    "tool_name": "file.write",
                    "effect": "workspace_write",
                    "reason": "approval required",
                    "approval_preview": "write note.txt"
                }))]
            }),
        );
        let lagged = read_daemon_request(&mut reader);
        assert_eq!(request_params_value(&lagged)["from_offset"], 1);
        write_daemon_error(&mut writer, lagged.id, "events.stream", ERROR_LAGGED);
        let resumed = read_daemon_request(&mut reader);
        assert!(request_params_value(&resumed).get("from_offset").is_none());
        write_daemon_response(
            &mut writer,
            resumed.id,
            "events.stream",
            json!({
                "run_id": "run_1",
                "from_offset": 3,
                "next_offset": 3,
                "status": "running",
                "events": []
            }),
        );
        let finished = read_daemon_request(&mut reader);
        write_daemon_response(
            &mut writer,
            finished.id,
            "events.stream",
            json!({
                "run_id": "run_1",
                "from_offset": 3,
                "next_offset": 3,
                "status": "finished",
                "events": []
            }),
        );
        let transcript = read_daemon_request(&mut reader);
        write_daemon_response(
            &mut writer,
            transcript.id,
            "transcript.read",
            json!({
                "run_id": "run_1",
                "status": "finished",
                "final_answer": answer,
                "transcript": "not the answer"
            }),
        );
    })
}

#[cfg(unix)]
fn respond_hello(reader: &mut BufReader<UnixStream>, writer: &mut UnixStream) {
    let hello = read_daemon_request(reader);
    assert_eq!(hello.method.as_deref(), Some("hello"));
    let workspace_id = request_params_value(&hello)["workspace_id"].clone();
    write_daemon_response(
        writer,
        hello.id,
        "hello",
        json!({
            "daemon_version": "test",
            "workspace_id": workspace_id,
            "ledger_path": "/tmp/agent.db",
            "capabilities": REQUIRED_CAPABILITIES
        }),
    );
}

#[cfg(unix)]
fn read_daemon_request(reader: &mut BufReader<UnixStream>) -> Envelope {
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    serde_json::from_str(line.trim()).unwrap()
}

#[cfg(unix)]
fn write_daemon_response(writer: &mut UnixStream, id: Option<String>, method: &str, result: Value) {
    serde_json::to_writer(
        &mut *writer,
        &Envelope::response(id, Some(method.into()), result),
    )
    .unwrap();
    writer.write_all(b"\n").unwrap();
    writer.flush().unwrap();
}

#[cfg(unix)]
fn write_daemon_error(
    writer: &mut UnixStream,
    id: Option<String>,
    method: &str,
    code: ProtocolErrorCode,
) {
    serde_json::to_writer(
        &mut *writer,
        &Envelope::error(id, Some(method.into()), code, "test error"),
    )
    .unwrap();
    writer.write_all(b"\n").unwrap();
    writer.flush().unwrap();
}
