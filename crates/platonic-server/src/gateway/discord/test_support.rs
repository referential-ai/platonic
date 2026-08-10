#[cfg(unix)]
use super::daemon_bridge::{EVENT_PAGE_LIMIT, REQUIRED_CAPABILITIES};
use super::{
    DiscordGateway, DiscordPlatform,
    commands::{
        DISCORD_APPLICATION_COMMAND, DISCORD_APPROVE_COMMAND, DISCORD_CHAT_INPUT_COMMAND,
        DISCORD_DENY_COMMAND, DISCORD_STATUS_COMMAND, DiscordAuthor, DiscordCommandHandler,
        InteractionCreateEvent, InteractionData, InteractionMember,
    },
    daemon_bridge::{EYES_EMOJI, FAILURE_EMOJI, SUCCESS_EMOJI},
    rest::DiscordRestClient,
    websocket::{DISCORD_INTENTS, DiscordGatewayReceiver, DiscordMessage},
};
use super::{DiscordGatewayTimings, GatewayResult};
use crate::config::DiscordGatewayPrincipal;
use platonic_client::client::DaemonConnectionConfig;
#[cfg(unix)]
use platonic_protocol::Envelope;
use platonic_protocol::ThreadApprovalPolicy;
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

#[cfg(unix)]
pub(super) fn spawn_gateway_startup_daemon(
    socket_path: &Path,
    workspace_id: String,
) -> thread::JoinHandle<Vec<Envelope>> {
    let listener = UnixListener::bind(socket_path).unwrap();
    thread::spawn(move || {
        let mut requests = Vec::new();
        for connection in 0..2 {
            let (stream, _) = listener.accept().unwrap();
            let mut writer = stream.try_clone().unwrap();
            let mut reader = BufReader::new(stream);
            let hello = read_daemon_request(&mut reader);
            write_daemon_response(
                &mut writer,
                hello.id.clone(),
                "hello",
                json!({
                    "daemon_version": "test",
                    "workspace_id": workspace_id,
                    "ledger_path": "/tmp/agent.db",
                    "capabilities": REQUIRED_CAPABILITIES
                }),
            );
            requests.push(hello);
            if connection == 1 {
                let authority = read_daemon_request(&mut reader);
                write_daemon_response(
                    &mut writer,
                    authority.id.clone(),
                    "thread.authority",
                    json!({
                        "authority": {
                            "thread_id": "thread_news",
                            "parent_thread_id": null,
                            "spawning_actor": "local",
                            "agent_id": "plato",
                            "model": "test-model",
                            "reasoning_effort": "medium",
                            "approval_policy": "prompt",
                            "toolset": ["file.read", "file.write"],
                            "worktrees": [],
                            "granted_paths": [],
                            "network": false,
                            "created_at_ms": 1
                        }
                    }),
                );
                requests.push(authority);
            }
        }
        requests
    })
}

#[cfg(unix)]
pub(super) fn spawn_gateway_roundtrip_daemon(
    socket_path: &Path,
) -> thread::JoinHandle<Vec<Envelope>> {
    let listener = UnixListener::bind(socket_path).unwrap();
    thread::spawn(move || {
        let (gateway_stream, _) = listener.accept().unwrap();
        let mut gateway_writer = gateway_stream.try_clone().unwrap();
        let mut gateway_reader = BufReader::new(gateway_stream);
        let mut requests = Vec::new();
        respond_hello(&mut gateway_reader, &mut gateway_writer);

        let status = read_daemon_request(&mut gateway_reader);
        write_daemon_response(
            &mut gateway_writer,
            status.id.clone(),
            "thread.status",
            json!({
                "thread": {
                    "authority": {
                        "thread_id": "thread_news",
                        "parent_thread_id": null,
                        "spawning_actor": "local",
                        "cwd": "/tmp/workspace",
                        "model": "test-model",
                        "reasoning_effort": "medium",
                        "approval_policy": "prompt",
                        "created_at_ms": 1
                    },
                    "live": {
                        "loaded": true,
                        "current_turn_id": "turn_gateway"
                    }
                }
            }),
        );
        requests.push(status);

        let send = read_daemon_request(&mut gateway_reader);
        write_daemon_response(
            &mut gateway_writer,
            send.id.clone(),
            "thread.send",
            json!({
                "status": "steered",
                "thread_id": "thread_news",
                "turn_id": "turn_gateway"
            }),
        );
        requests.push(send);

        let pending_events = read_daemon_request(&mut gateway_reader);
        write_daemon_response(
            &mut gateway_writer,
            pending_events.id.clone(),
            "thread.events",
            json!({
                "thread_id": "thread_news",
                "from_offset": 0,
                "next_offset": 1,
                "current_turn_id": "turn_gateway",
                "events": [{
                    "offset": 0,
                    "turn_id": "turn_gateway",
                    "event": {
                        "kind": "approval_requested",
                        "run_id": "run_gateway",
                        "tool_call_id": "call_exact",
                        "tool_name": "file.write",
                        "effect": "workspace_write",
                        "reason": "approval required",
                        "approval_preview": "write note.txt"
                    }
                }]
            }),
        );
        requests.push(pending_events);

        let (approval_stream, _) = listener.accept().unwrap();
        let mut approval_writer = approval_stream.try_clone().unwrap();
        let mut approval_reader = BufReader::new(approval_stream);
        respond_hello(&mut approval_reader, &mut approval_writer);
        let approval_authority = read_daemon_request(&mut approval_reader);
        write_thread_authority(&mut approval_writer, approval_authority.id.clone());
        requests.push(approval_authority);
        let decision = read_daemon_request(&mut approval_reader);
        write_daemon_response(
            &mut approval_writer,
            decision.id.clone(),
            "approval.decide",
            json!({"run_id": "run_gateway", "status": "running"}),
        );
        requests.push(decision);

        let terminal_events = read_daemon_request(&mut gateway_reader);
        write_daemon_response(
            &mut gateway_writer,
            terminal_events.id.clone(),
            "thread.events",
            json!({
                "thread_id": "thread_news",
                "from_offset": 1,
                "next_offset": 3,
                "current_turn_id": null,
                "events": [
                    {
                        "offset": 1,
                        "turn_id": "turn_gateway",
                        "event": {
                            "kind": "ledger",
                            "record": {
                                "seq": 1,
                                "occurred_at_ms": 1,
                                "event": {
                                    "event": "model_responded",
                                    "run_id": "run_gateway",
                                    "turn_id": "model_turn",
                                    "step": 1,
                                    "output": {"role": "assistant", "content": "approved and complete"},
                                    "proposed_calls": [],
                                    "usage": null
                                }
                            }
                        }
                    },
                    {
                        "offset": 2,
                        "turn_id": "turn_gateway",
                        "event": {
                            "kind": "ledger",
                            "record": {
                                "seq": 2,
                                "occurred_at_ms": 2,
                                "event": {"event": "run_finished", "run_id": "run_gateway"}
                            }
                        }
                    }
                ]
            }),
        );
        requests.push(terminal_events);
        requests
    })
}

#[cfg(unix)]
pub(super) fn spawn_reconnecting_completed_thread_daemon(
    socket_path: &Path,
) -> thread::JoinHandle<Vec<Envelope>> {
    let listener = UnixListener::bind(socket_path).unwrap();
    thread::spawn(move || {
        let mut requests = Vec::new();
        {
            let (gateway_stream, _) = listener.accept().unwrap();
            let mut gateway_writer = gateway_stream.try_clone().unwrap();
            let mut gateway_reader = BufReader::new(gateway_stream);
            respond_hello(&mut gateway_reader, &mut gateway_writer);

            let status = read_daemon_request(&mut gateway_reader);
            write_daemon_response(
                &mut gateway_writer,
                status.id.clone(),
                "thread.status",
                json!({
                    "thread": {
                        "authority": {
                            "thread_id": "thread_news",
                            "parent_thread_id": null,
                            "spawning_actor": "local",
                            "cwd": "/tmp/workspace",
                            "model": "test-model",
                            "reasoning_effort": "medium",
                            "approval_policy": "prompt",
                            "created_at_ms": 1
                        },
                        "live": {
                            "loaded": true,
                            "current_turn_id": "turn_gateway"
                        }
                    }
                }),
            );
            requests.push(status);

            let send = read_daemon_request(&mut gateway_reader);
            write_daemon_response(
                &mut gateway_writer,
                send.id.clone(),
                "thread.send",
                json!({
                    "status": "steered",
                    "thread_id": "thread_news",
                    "turn_id": "turn_gateway"
                }),
            );
            requests.push(send);

            let pending = read_daemon_request(&mut gateway_reader);
            write_daemon_response(
                &mut gateway_writer,
                pending.id.clone(),
                "thread.events",
                json!({
                    "thread_id": "thread_news",
                    "from_offset": 0,
                    "next_offset": 1,
                    "current_turn_id": "turn_gateway",
                    "events": [{
                        "offset": 0,
                        "turn_id": "turn_gateway",
                        "event": {
                            "kind": "approval_requested",
                            "run_id": "run_gateway",
                            "tool_call_id": "call_exact",
                            "tool_name": "file.write",
                            "effect": "workspace_write",
                            "reason": "approval required",
                            "approval_preview": "write note.txt"
                        }
                    }]
                }),
            );
            requests.push(pending);

            let disconnected = read_daemon_request(&mut gateway_reader);
            assert_eq!(disconnected.method.as_deref(), Some("thread.events"));
            assert_eq!(request_params_value(&disconnected)["from_offset"], 1);
            requests.push(disconnected);
        }

        let (recovered_stream, _) = listener.accept().unwrap();
        let mut recovered_writer = recovered_stream.try_clone().unwrap();
        let mut recovered_reader = BufReader::new(recovered_stream);
        respond_hello(&mut recovered_reader, &mut recovered_writer);
        let replay = read_daemon_request(&mut recovered_reader);
        assert_eq!(replay.method.as_deref(), Some("thread.events"));
        assert_eq!(request_params_value(&replay)["from_offset"], 0);
        let mut replay_events = vec![
            json!({
                "offset": 0,
                "turn_id": "turn_gateway",
                "event": {
                    "kind": "approval_requested",
                    "run_id": "run_gateway",
                    "tool_call_id": "call_exact",
                    "tool_name": "file.write",
                    "effect": "workspace_write",
                    "reason": "approval required",
                    "approval_preview": "write note.txt"
                }
            }),
            json!({
                "offset": 1,
                "turn_id": "turn_gateway",
                "event": {
                    "kind": "ledger",
                    "record": {
                        "seq": 1,
                        "occurred_at_ms": 1,
                        "event": {
                            "event": "approval_granted",
                            "run_id": "run_gateway",
                            "call_id": "call_exact",
                            "actor_id": "jerome"
                        }
                    }
                }
            }),
        ];
        replay_events.extend((2..EVENT_PAGE_LIMIT as u64).map(|offset| {
            json!({
                "offset": offset,
                "turn_id": "turn_gateway",
                "event": {
                    "kind": "assistant_delta",
                    "run_id": "run_gateway",
                    "turn_id": "model_turn",
                    "step": 1,
                    "delta_index": offset,
                    "text": "partial"
                }
            })
        }));
        write_daemon_response(
            &mut recovered_writer,
            replay.id.clone(),
            "thread.events",
            json!({
                "thread_id": "thread_news",
                "from_offset": 0,
                "next_offset": EVENT_PAGE_LIMIT,
                "current_turn_id": null,
                "events": replay_events
            }),
        );
        requests.push(replay);

        let terminal = read_daemon_request(&mut recovered_reader);
        assert_eq!(terminal.method.as_deref(), Some("thread.events"));
        assert_eq!(
            request_params_value(&terminal)["from_offset"],
            EVENT_PAGE_LIMIT
        );
        write_daemon_response(
            &mut recovered_writer,
            terminal.id.clone(),
            "thread.events",
            json!({
                "thread_id": "thread_news",
                "from_offset": EVENT_PAGE_LIMIT,
                "next_offset": EVENT_PAGE_LIMIT + 3,
                "current_turn_id": "turn_later",
                "events": [
                    {
                        "offset": EVENT_PAGE_LIMIT,
                        "turn_id": "turn_gateway",
                        "event": {
                            "kind": "ledger",
                            "record": {
                                "seq": EVENT_PAGE_LIMIT,
                                "occurred_at_ms": EVENT_PAGE_LIMIT,
                                "event": {
                                    "event": "model_responded",
                                    "run_id": "run_gateway",
                                    "turn_id": "model_turn",
                                    "step": 1,
                                    "output": {
                                        "role": "assistant",
                                        "content": "recovered exact answer"
                                    },
                                    "proposed_calls": [],
                                    "usage": null
                                }
                            }
                        }
                    },
                    {
                        "offset": EVENT_PAGE_LIMIT + 1,
                        "turn_id": "turn_gateway",
                        "event": {
                            "kind": "ledger",
                            "record": {
                                "seq": EVENT_PAGE_LIMIT + 1,
                                "occurred_at_ms": EVENT_PAGE_LIMIT + 1,
                                "event": {
                                    "event": "run_finished",
                                    "run_id": "run_gateway"
                                }
                            }
                        }
                    },
                    {
                        "offset": EVENT_PAGE_LIMIT + 2,
                        "turn_id": "turn_later",
                        "event": {
                            "kind": "ledger",
                            "record": {
                                "seq": EVENT_PAGE_LIMIT + 2,
                                "occurred_at_ms": EVENT_PAGE_LIMIT + 2,
                                "event": {
                                    "event": "model_responded",
                                    "run_id": "run_later",
                                    "turn_id": "later_model_turn",
                                    "step": 1,
                                    "output": {
                                        "role": "assistant",
                                        "content": "wrong later answer"
                                    },
                                    "proposed_calls": [],
                                    "usage": null
                                }
                            }
                        }
                    }
                ]
            }),
        );
        requests.push(terminal);

        let transcript = read_daemon_request(&mut recovered_reader);
        assert_eq!(transcript.method.as_deref(), Some("transcript.read"));
        assert_eq!(request_params_value(&transcript)["run_id"], "run_gateway");
        write_daemon_response(
            &mut recovered_writer,
            transcript.id.clone(),
            "transcript.read",
            json!({
                "run_id": "run_gateway",
                "status": "finished",
                "final_answer": "recovered exact answer",
                "transcript": "not parsed"
            }),
        );
        requests.push(transcript);
        requests
    })
}

#[cfg(unix)]
pub(super) fn spawn_lagged_completed_thread_daemon(
    socket_path: &Path,
) -> thread::JoinHandle<Vec<Envelope>> {
    let listener = UnixListener::bind(socket_path).unwrap();
    thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut writer = stream.try_clone().unwrap();
        let mut reader = BufReader::new(stream);
        let mut requests = Vec::new();
        respond_hello(&mut reader, &mut writer);

        let status = read_daemon_request(&mut reader);
        write_daemon_response(
            &mut writer,
            status.id.clone(),
            "thread.status",
            json!({
                "thread": {
                    "authority": {
                        "thread_id": "thread_news",
                        "parent_thread_id": null,
                        "spawning_actor": "local",
                        "cwd": "/tmp/workspace",
                        "model": "test-model",
                        "reasoning_effort": "medium",
                        "approval_policy": "prompt",
                        "created_at_ms": 1
                    },
                    "live": {
                        "loaded": true,
                        "current_turn_id": "turn_gateway"
                    }
                }
            }),
        );
        requests.push(status);

        let send = read_daemon_request(&mut reader);
        write_daemon_response(
            &mut writer,
            send.id.clone(),
            "thread.send",
            json!({
                "status": "steered",
                "thread_id": "thread_news",
                "turn_id": "turn_gateway"
            }),
        );
        requests.push(send);

        let started = read_daemon_request(&mut reader);
        write_daemon_response(
            &mut writer,
            started.id.clone(),
            "thread.events",
            json!({
                "thread_id": "thread_news",
                "from_offset": 0,
                "next_offset": 1,
                "current_turn_id": "turn_gateway",
                "events": [{
                    "offset": 0,
                    "turn_id": "turn_gateway",
                    "event": {
                        "kind": "ledger",
                        "record": {
                            "seq": 0,
                            "occurred_at_ms": 0,
                            "event": {
                                "event": "run_started",
                                "run_id": "run_gateway",
                                "agent_id": "plato"
                            }
                        }
                    }
                }]
            }),
        );
        requests.push(started);

        let lagged = read_daemon_request(&mut reader);
        serde_json::to_writer(
            &mut writer,
            &Envelope::error(
                lagged.id.clone(),
                Some("thread.events".into()),
                platonic_protocol::ERROR_LAGGED,
                "requested thread events were evicted; first available offset is 8",
            ),
        )
        .unwrap();
        writer.write_all(b"\n").unwrap();
        writer.flush().unwrap();
        requests.push(lagged);

        let tip = read_daemon_request(&mut reader);
        assert!(request_params_value(&tip).get("from_offset").is_none());
        write_daemon_response(
            &mut writer,
            tip.id.clone(),
            "thread.events",
            json!({
                "thread_id": "thread_news",
                "from_offset": 8,
                "next_offset": 8,
                "current_turn_id": null,
                "events": []
            }),
        );
        requests.push(tip);

        let transcript = read_daemon_request(&mut reader);
        assert_eq!(transcript.method.as_deref(), Some("transcript.read"));
        assert_eq!(request_params_value(&transcript)["run_id"], "run_gateway");
        write_daemon_response(
            &mut writer,
            transcript.id.clone(),
            "transcript.read",
            json!({
                "run_id": "run_gateway",
                "status": "finished",
                "final_answer": "durable answer after lag",
                "transcript": "not parsed"
            }),
        );
        requests.push(transcript);
        requests
    })
}

#[cfg(unix)]
fn write_thread_authority(writer: &mut UnixStream, id: Option<String>) {
    write_daemon_response(
        writer,
        id,
        "thread.authority",
        json!({
            "authority": {
                "thread_id": "thread_news",
                "parent_thread_id": null,
                "spawning_actor": "local",
                "agent_id": "plato",
                "model": "test-model",
                "reasoning_effort": "medium",
                "approval_policy": "prompt",
                "toolset": ["file.read", "file.write"],
                "worktrees": [],
                "granted_paths": [],
                "network": false,
                "created_at_ms": 1
            }
        }),
    );
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
                .map(|(name, value)| json!({"type": 3, "name": name, "value": value}))
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
        principals: std::collections::HashMap::from([(
            42,
            DiscordGatewayPrincipal {
                name: "jerome".into(),
                remote_ceiling: ThreadApprovalPolicy::Prompt,
            },
        )]),
        channel_thread_ids: std::collections::HashMap::from([(200, "thread_news".into())]),
        pending_approvals: Arc::new(Mutex::new(std::collections::HashMap::new())),
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
    let daemon = DaemonConnectionConfig::resolve(workspace.path(), Some(socket_path)).unwrap();
    let timings = DiscordGatewayTimings::default();
    let mut gateway = DiscordGateway {
        platform,
        daemon,
        channel_thread_ids: std::collections::HashMap::from([(200, "thread_news".into())]),
        principals: std::collections::HashMap::from([(
            42,
            DiscordGatewayPrincipal {
                name: "jerome".into(),
                remote_ceiling: ThreadApprovalPolicy::Prompt,
            },
        )]),
        pending_approvals: Arc::new(Mutex::new(std::collections::HashMap::new())),
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
                        {"name": DISCORD_APPROVE_COMMAND, "type": 1},
                        {"name": DISCORD_DENY_COMMAND, "type": 1}
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

pub(super) fn spawn_approval_signaling_rest(expected_requests: usize) -> (FakeRest, Receiver<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let (approval_seen, approval_notification) = mpsc::channel();
    let handle = thread::spawn(move || {
        let mut requests = Vec::new();
        let mut approval_seen = Some(approval_seen);
        for _ in 0..expected_requests {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_http_request(&mut stream);
            if request.body["content"]
                .as_str()
                .is_some_and(|content| content.starts_with("Approval required:"))
                && let Some(approval_seen) = approval_seen.take()
            {
                approval_seen.send(()).unwrap();
            }
            write_http_response(&mut stream, 200, &json!({"id": "message_1"}));
            requests.push(request);
        }
        requests
    });
    (FakeRest { base_url, handle }, approval_notification)
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
            principals: std::collections::HashMap::from([(
                42,
                DiscordGatewayPrincipal {
                    name: "jerome".into(),
                    remote_ceiling: ThreadApprovalPolicy::Prompt,
                },
            )]),
            channel_thread_ids: std::collections::HashMap::from([(200, "thread_news".into())]),
            pending_approvals: Arc::new(Mutex::new(std::collections::HashMap::new())),
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
        let status = read_daemon_request(&mut reader);
        assert_eq!(status.method.as_deref(), Some("thread.status"));
        write_daemon_response(
            &mut writer,
            status.id,
            "thread.status",
            json!({
                "thread": {
                    "authority": {
                        "thread_id": "thread_news",
                        "parent_thread_id": null,
                        "spawning_actor": "local",
                        "cwd": "/tmp/workspace",
                        "model": "test-model",
                        "reasoning_effort": "medium",
                        "approval_policy": "prompt",
                        "created_at_ms": 1
                    },
                    "live": {
                        "loaded": true,
                        "current_turn_id": null
                    }
                }
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
