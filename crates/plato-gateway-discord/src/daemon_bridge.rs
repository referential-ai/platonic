use super::{DiscordGateway, DiscordPlatform, rest::DISCORD_MESSAGE_LIMIT};
use crate::{GatewayError, GatewayResult};
use platonic_client::{
    ClientError,
    client::{DaemonClient, DaemonConnectionConfig},
    paths,
};
use platonic_protocol::{
    BufferedStreamEvent, HarnessEvent, HelloResult, RunStateName, StreamEvent, TranscriptReadResult,
};
use std::{
    collections::HashMap,
    path::Path,
    thread,
    time::{Duration, Instant},
};

pub(super) const EVENT_PAGE_LIMIT: usize = 64;
const APPROVAL_FIELD_LIMIT: usize = 80;
pub(super) const RUN_FAILED_MESSAGE: &str = "Run failed. Inspect it locally with: plato replay";
pub(super) const EYES_EMOJI: &str = "👀";
pub(super) const SUCCESS_EMOJI: &str = "✅";
pub(super) const FAILURE_EMOJI: &str = "❌";
pub(super) const TYPING_INTERVAL: Duration = Duration::from_secs(8);
const RECONNECT_ATTEMPTS: usize = 40;
pub(super) const REQUIRED_CAPABILITIES: [&str; 6] = [
    "hello",
    "run.start",
    "message.append",
    "events.stream",
    "sessions.list",
    "transcript.read",
];

impl DiscordGateway {
    pub(super) fn handle_message(
        &mut self,
        message: super::websocket::DiscordMessage,
    ) -> GatewayResult<()> {
        let mut presentation = MessagePresentation::new(message.channel_id, message.id);
        presentation.add_eyes(&self.platform);
        let result = self.handle_allowed_message(message, &mut presentation);
        if result.is_err() {
            presentation.abnormal_exit(&self.platform);
        }
        result
    }

    fn handle_allowed_message(
        &mut self,
        message: super::websocket::DiscordMessage,
        presentation: &mut MessagePresentation,
    ) -> GatewayResult<()> {
        let channel_id = message.channel_id;
        let overrides = self
            .overrides
            .lock()
            .map_err(|_| GatewayError::Discord("discord run settings lock poisoned".into()))?
            .get(&channel_id)
            .cloned()
            .unwrap_or_default();
        let config_path = self.channel_config_paths.get(&channel_id).cloned();
        let mut daemon = self.connect_daemon(self.daemon_client_timeout)?;
        let run = match self.sessions.get(&channel_id).cloned() {
            Some(session_id) => daemon.message_append_to_session_with_overrides(
                message.content,
                Some(session_id),
                config_path,
                overrides,
                false,
            ),
            None => daemon.run_start_with_overrides(message.content, config_path, overrides, false),
        }?;
        self.sessions.insert(channel_id, run.session_id.clone());
        let terminal = self.wait_for_run(&mut daemon, channel_id, &run.run_id, presentation)?;
        let terminal_status = terminal.status;
        if let Some(message) = terminal_message(terminal)?
            && let Err(error) = self.platform.send_message(channel_id, &message)
        {
            presentation.abnormal_exit(&self.platform);
            report_response_delivery_failure(&error);
            return Ok(());
        }
        presentation.finish(&self.platform, terminal_status);
        Ok(())
    }

    fn wait_for_run(
        &self,
        daemon: &mut DaemonClient,
        channel_id: u64,
        run_id: &str,
        presentation: &mut MessagePresentation,
    ) -> GatewayResult<TranscriptReadResult> {
        let mut next_offset = Some(0);
        let mut approvals = ApprovalNotifications::default();
        let mut canceling = false;
        loop {
            match daemon
                .events_stream(run_id, next_offset, EVENT_PAGE_LIMIT)
                .map_err(GatewayError::from)
            {
                Ok(events) => {
                    next_offset = Some(events.next_offset);
                    let needs_catch_up = events.events.len() == EVENT_PAGE_LIMIT
                        && events.next_offset > events.from_offset;
                    let was_pending = approvals.pending.is_some();
                    canceling |= approvals.fold(&events.events);
                    if was_pending != approvals.pending.is_some() {
                        presentation.stop_typing();
                    }
                    if needs_catch_up {
                        continue;
                    }
                    match events.status {
                        RunStateName::Running => {
                            if let Some(message) = approvals.take_notification()
                                && let Err(error) = self.platform.send_message(channel_id, &message)
                            {
                                report_response_delivery_failure(&error);
                            }
                            presentation.observe_running(
                                &self.platform,
                                approvals.pending.is_some() || canceling,
                                Instant::now(),
                            );
                            if events.events.is_empty() {
                                thread::sleep(self.event_poll_delay);
                            }
                        }
                        RunStateName::CancelRequested => {
                            canceling = true;
                            presentation.stop_typing();
                            if let Some(message) = approvals.take_notification()
                                && let Err(error) = self.platform.send_message(channel_id, &message)
                            {
                                report_response_delivery_failure(&error);
                            }
                            if events.events.is_empty() {
                                thread::sleep(self.event_poll_delay);
                            }
                        }
                        RunStateName::Finished
                        | RunStateName::Failed
                        | RunStateName::Canceled
                        | RunStateName::Interrupted => {
                            presentation.stop_typing();
                            approvals.clear();
                            return self.read_terminal_run(daemon, run_id);
                        }
                    }
                }
                Err(GatewayError::Client(ClientError::DaemonResponse(error)))
                    if error.code == "lagged" =>
                {
                    next_offset = None;
                    approvals.clear();
                    canceling = false;
                    presentation.stop_typing();
                }
                Err(error) if reconnectable(&error) => {
                    *daemon = self.reconnect_daemon()?;
                    approvals.clear();
                    canceling = false;
                    presentation.stop_typing();
                    let status = daemon
                        .sessions_list()?
                        .into_iter()
                        .find(|session| session.run_id == run_id)
                        .map(|session| session.status);
                    match status {
                        Some(RunStateName::Running | RunStateName::CancelRequested) => {
                            next_offset = None;
                        }
                        Some(
                            RunStateName::Finished
                            | RunStateName::Failed
                            | RunStateName::Canceled
                            | RunStateName::Interrupted,
                        )
                        | None => return self.read_terminal_run(daemon, run_id),
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn reconnect_daemon(&self) -> GatewayResult<DaemonClient> {
        for _ in 0..RECONNECT_ATTEMPTS {
            match self.connect_daemon(self.daemon_client_timeout) {
                Ok(client) => return Ok(client),
                Err(error) if reconnectable(&error) => thread::sleep(self.reconnect_delay),
                Err(error) => return Err(error),
            }
        }
        Err(GatewayError::DaemonProtocol(
            "daemon unavailable during gateway recovery".into(),
        ))
    }

    fn connect_daemon(&self, timeout: Duration) -> GatewayResult<DaemonClient> {
        let mut client = DaemonClient::connect_with_timeout(&self.daemon.socket_path, timeout)?;
        let hello = client.hello(&self.daemon.workspace_root)?;
        require_gateway_daemon_contract(&self.daemon.workspace_root, &hello)?;
        Ok(client)
    }

    fn read_terminal_run(
        &self,
        daemon: &mut DaemonClient,
        run_id: &str,
    ) -> GatewayResult<TranscriptReadResult> {
        match daemon.transcript_read(run_id).map_err(GatewayError::from) {
            Ok(transcript) => Ok(transcript),
            Err(error) if reconnectable(&error) => {
                *daemon = self.reconnect_daemon()?;
                Ok(daemon.transcript_read(run_id)?)
            }
            Err(error) => Err(error),
        }
    }
}

pub(super) fn report_response_delivery_failure(error: &GatewayError) {
    eprintln!("discord response delivery failed; gateway continues: {error}");
}

/// Verifies daemon identity and every capability required by the gateway.
pub fn preflight_discord_gateway_daemon(
    config: &DaemonConnectionConfig,
    timeout: Duration,
) -> GatewayResult<()> {
    let mut client = DaemonClient::connect_with_timeout(&config.socket_path, timeout)?;
    let hello = client.hello(&config.workspace_root)?;
    require_gateway_daemon_contract(&config.workspace_root, &hello)
}

pub(super) fn require_gateway_daemon_contract(
    workspace_root: &Path,
    hello: &HelloResult,
) -> GatewayResult<()> {
    let expected_workspace_id = paths::workspace_id(workspace_root)?;
    if hello.workspace_id != expected_workspace_id {
        return Err(GatewayError::DaemonProtocol(format!(
            "hello workspace_id mismatch: expected {expected_workspace_id}, got {}",
            hello.workspace_id
        )));
    }
    if let Some(capability) = REQUIRED_CAPABILITIES.iter().find(|capability| {
        !hello
            .capabilities
            .iter()
            .any(|actual| actual == **capability)
    }) {
        return Err(GatewayError::DaemonProtocol(format!(
            "daemon does not advertise required capability {capability}"
        )));
    }
    Ok(())
}

fn terminal_message(transcript: TranscriptReadResult) -> GatewayResult<Option<String>> {
    match transcript.status {
        RunStateName::Finished => transcript.final_answer.map(Some).ok_or_else(|| {
            GatewayError::RunFailed(format!(
                "run {} ended with status {} without a final answer",
                transcript.run_id, transcript.status
            ))
        }),
        RunStateName::Failed => Ok(Some(RUN_FAILED_MESSAGE.into())),
        RunStateName::Canceled | RunStateName::Interrupted => Ok(None),
        RunStateName::Running | RunStateName::CancelRequested => {
            Err(GatewayError::DaemonProtocol(format!(
                "run {} read back with nonterminal status {}",
                transcript.run_id, transcript.status
            )))
        }
    }
}

#[derive(Default)]
struct ApprovalNotifications {
    pending: Option<PendingApprovalNotification>,
    input_previews: HashMap<String, String>,
}

struct PendingApprovalNotification {
    call_id: String,
    tool_name: String,
    effect: String,
    preview: Option<String>,
    notified: bool,
}

impl ApprovalNotifications {
    fn fold(&mut self, entries: &[BufferedStreamEvent]) -> bool {
        let mut canceled = false;
        for entry in entries {
            let event = &entry.event;
            if matches!(event, StreamEvent::Canceled { .. }) {
                self.clear();
                canceled = true;
                continue;
            }
            if let Some((call_id, preview)) = tool_input_preview(event) {
                self.input_previews.insert(call_id, preview);
            }
            if let StreamEvent::ApprovalRequested {
                tool_call_id,
                tool_name,
                effect,
                diff_preview,
                approval_preview,
                ..
            } = event
            {
                self.pending = Some(PendingApprovalNotification {
                    call_id: tool_call_id.clone(),
                    tool_name: tool_name.clone(),
                    effect: serde_json::to_value(effect)
                        .ok()
                        .and_then(|value| value.as_str().map(str::to_owned))
                        .unwrap_or_else(|| "unknown effect".into()),
                    preview: diff_preview
                        .as_deref()
                        .filter(|preview| !preview.is_empty())
                        .or_else(|| {
                            approval_preview
                                .as_deref()
                                .filter(|preview| !preview.is_empty())
                        })
                        .map(str::to_owned),
                    notified: false,
                });
            }
            if let Some(call_id) = approval_resolution_call_id(event) {
                self.input_previews.remove(call_id);
                if self
                    .pending
                    .as_ref()
                    .map(|pending| pending.call_id.as_str())
                    == Some(call_id)
                {
                    self.pending = None;
                }
            }
        }
        canceled
    }

    fn take_notification(&mut self) -> Option<String> {
        let pending = self.pending.as_mut()?;
        if pending.notified {
            return None;
        }
        let preview = pending.preview.as_deref().or_else(|| {
            self.input_previews
                .get(&pending.call_id)
                .map(String::as_str)
        })?;
        pending.notified = true;
        Some(approval_notification(
            &pending.tool_name,
            &pending.effect,
            preview,
        ))
    }

    fn clear(&mut self) {
        self.pending = None;
        self.input_previews.clear();
    }
}

fn tool_input_preview(event: &StreamEvent) -> Option<(String, String)> {
    let StreamEvent::Ledger { record } = event else {
        return None;
    };
    let HarnessEvent::ToolCallProposed { call, .. } = &record.event else {
        return None;
    };
    let preview = serde_json::to_string_pretty(&call.input).ok()?;
    Some((call.id.to_string(), preview))
}

fn approval_resolution_call_id(event: &StreamEvent) -> Option<&str> {
    let StreamEvent::Ledger { record } = event else {
        return None;
    };
    match &record.event {
        HarnessEvent::ApprovalGranted { call_id, .. }
        | HarnessEvent::ApprovalDenied { call_id, .. } => Some(call_id.as_str()),
        _ => None,
    }
}

fn approval_notification(tool_name: &str, effect: &str, preview: &str) -> String {
    let tool_name = truncate_chars(tool_name, APPROVAL_FIELD_LIMIT);
    let effect = truncate_chars(effect, APPROVAL_FIELD_LIMIT);
    let prefix = format!("Approval required: `{tool_name}` ({effect})\nPreview:\n");
    let suffix = "\nGrant or deny it locally in `plato-tui`.";
    let preview_limit =
        DISCORD_MESSAGE_LIMIT.saturating_sub(prefix.chars().count() + suffix.chars().count());
    format!("{prefix}{}{suffix}", truncate_chars(preview, preview_limit))
}

fn truncate_chars(value: &str, limit: usize) -> String {
    if value.chars().count() <= limit {
        return value.into();
    }
    if limit <= 3 {
        return ".".repeat(limit);
    }
    let mut truncated = value.chars().take(limit - 3).collect::<String>();
    truncated.push_str("...");
    truncated
}

fn reconnectable(error: &GatewayError) -> bool {
    matches!(
        error,
        GatewayError::Io(_)
            | GatewayError::Json(_)
            | GatewayError::DaemonProtocol(_)
            | GatewayError::Client(
                ClientError::Io(_) | ClientError::Json(_) | ClientError::DaemonProtocol(_)
            )
    ) || matches!(
        error,
        GatewayError::Client(ClientError::DaemonResponse(error)) if error.code == "not_found"
    )
}

struct MessagePresentation {
    channel_id: u64,
    message_id: u64,
    next_typing_at: Option<Instant>,
}

impl MessagePresentation {
    fn new(channel_id: u64, message_id: u64) -> Self {
        Self {
            channel_id,
            message_id,
            next_typing_at: None,
        }
    }

    fn add_eyes(&self, platform: &DiscordPlatform) {
        self.ignore(platform.add_reaction(self.channel_id, self.message_id, EYES_EMOJI));
    }

    fn observe_running(&mut self, platform: &DiscordPlatform, paused: bool, now: Instant) {
        if !self.typing_due(paused, now) {
            return;
        }
        self.ignore(platform.trigger_typing(self.channel_id));
    }

    fn typing_due(&mut self, paused: bool, now: Instant) -> bool {
        if paused {
            self.stop_typing();
            return false;
        }
        if self.next_typing_at.is_some_and(|deadline| now < deadline) {
            return false;
        }
        self.next_typing_at = now.checked_add(TYPING_INTERVAL);
        true
    }

    fn stop_typing(&mut self) {
        self.next_typing_at = None;
    }

    fn finish(&mut self, platform: &DiscordPlatform, status: RunStateName) {
        self.stop_typing();
        match status {
            RunStateName::Finished => {
                self.remove_eyes(platform);
                self.ignore(platform.add_terminal_reaction(
                    self.channel_id,
                    self.message_id,
                    SUCCESS_EMOJI,
                ));
            }
            RunStateName::Failed => {
                self.remove_eyes(platform);
                self.ignore(platform.add_terminal_reaction(
                    self.channel_id,
                    self.message_id,
                    FAILURE_EMOJI,
                ));
            }
            RunStateName::Canceled | RunStateName::Interrupted => self.remove_eyes(platform),
            RunStateName::Running | RunStateName::CancelRequested => {}
        }
    }

    fn abnormal_exit(&mut self, platform: &DiscordPlatform) {
        self.stop_typing();
        self.remove_eyes(platform);
        self.ignore(platform.add_reaction(self.channel_id, self.message_id, FAILURE_EMOJI));
    }

    fn remove_eyes(&self, platform: &DiscordPlatform) {
        self.ignore(platform.remove_reaction(self.channel_id, self.message_id, EYES_EMOJI));
    }

    fn ignore(&self, result: GatewayResult<()>) {
        if let Err(error) = result {
            eprintln!("discord presentation effect failed: {error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;
    use platonic_protocol::TranscriptReadResult;
    #[cfg(unix)]
    use platonic_protocol::{ReasoningEffort, RunOverrides};
    #[cfg(unix)]
    use serde_json::Value;
    use serde_json::json;
    #[cfg(unix)]
    use std::os::unix::net::UnixListener;
    use std::time::{Duration, Instant};
    #[cfg(unix)]
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    #[cfg(unix)]
    #[test]
    fn discord_client_bounds_a_stalled_hello() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let server = std::thread::spawn(move || {
            let _stream = listener.accept().unwrap();
            std::thread::sleep(Duration::from_millis(150));
        });
        let platform = test_platform("http://127.0.0.1", discord_message(42, 200, "hello"));
        let gateway = test_gateway(&workspace, socket_path, platform);

        let started = Instant::now();
        let error = match gateway.connect_daemon(Duration::from_millis(50)) {
            Ok(_) => panic!("stalled daemon unexpectedly answered"),
            Err(error) => error,
        };
        let elapsed = started.elapsed();
        server.join().unwrap();

        assert!(matches!(
            error,
            GatewayError::Client(ClientError::Io(error))
                if error.kind() == std::io::ErrorKind::TimedOut
        ));
        assert!(elapsed < Duration::from_secs(1), "request took {elapsed:?}");
        assert_eq!(
            crate::DiscordGatewayTimings::default().daemon_client_timeout,
            Duration::from_secs(3)
        );
    }

    #[cfg(unix)]
    #[test]
    fn owner_message_replies_with_typed_final_answer() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("daemon.sock");
        let daemon = spawn_finished_daemon(&socket_path, "run.start", "session_1", "final answer");
        let rest = spawn_fake_rest(4, 200, None);
        let content = "keep\u{2003}this\u{7} byte-for-byte";
        let platform = test_platform(&rest.base_url, discord_message(42, 200, content));
        let overrides = Arc::new(Mutex::new(HashMap::from([(
            200,
            RunOverrides {
                model: Some("openai/gpt-5".into()),
                reasoning_effort: Some(ReasoningEffort::High),
            },
        )])));
        let mut gateway = test_gateway_with_overrides(&workspace, socket_path, platform, overrides);

        gateway.poll_once().unwrap();

        let start_params = daemon.join().unwrap();
        assert_eq!(start_params["question"], content);
        assert!(start_params.get("session_id").is_none());
        assert_eq!(
            start_params["overrides"],
            json!({
                "model": "openai/gpt-5",
                "reasoning_effort": "high"
            })
        );
        assert_eq!(start_params["wait"], false);
        let requests = rest.handle.join().unwrap();
        assert_reaction(&requests[0], "PUT", EYES_EMOJI);
        assert_eq!(requests[1].method, "POST");
        assert_eq!(requests[1].path, "/channels/200/messages");
        assert_eq!(requests[1].authorization, "Bot test-token");
        assert_eq!(requests[1].body["content"], "final answer");
        assert_eq!(requests[1].body["allowed_mentions"]["parse"], json!([]));
        assert_reaction(&requests[2], "DELETE", EYES_EMOJI);
        assert_reaction(&requests[3], "PUT", SUCCESS_EMOJI);
        assert_eq!(gateway.sessions[&200], "session_1");
    }

    #[cfg(unix)]
    #[test]
    fn response_send_failure_does_not_stop_next_owner_message() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("daemon.sock");
        let first_daemon =
            spawn_finished_daemon(&socket_path, "run.start", "session_1", "first answer");
        let respond = |status, body| {
            FakeRestAction::Respond(FakeResponse {
                status,
                body,
                headers: Vec::new(),
            })
        };
        let rest = spawn_scripted_rest_actions(vec![
            respond(204, Value::Null),
            respond(429, json!({"retry_after": 0.0, "global": false})),
            respond(429, json!({"retry_after": 0.0, "global": false})),
            respond(204, Value::Null),
            respond(204, Value::Null),
            respond(204, Value::Null),
            respond(200, json!({"id": "reply_2"})),
            respond(204, Value::Null),
            respond(204, Value::Null),
        ]);
        let platform = test_platform_messages(
            &rest.base_url,
            [
                discord_message(42, 200, "first"),
                discord_message(42, 200, "second"),
            ],
        );
        let mut gateway = test_gateway(&workspace, socket_path.clone(), platform);

        gateway.poll_once().unwrap();
        let first = first_daemon.join().unwrap();
        assert_eq!(first["question"], "first");
        assert_eq!(gateway.sessions[&200], "session_1");

        std::fs::remove_file(&socket_path).unwrap();
        let second_daemon =
            spawn_finished_daemon(&socket_path, "message.append", "session_1", "second answer");
        gateway.poll_once().unwrap();
        let second = second_daemon.join().unwrap();
        assert_eq!(second["message"], "second");
        assert_eq!(second["session_id"], "session_1");

        let receiver_error = gateway.poll_once().unwrap_err();
        assert!(
            receiver_error
                .to_string()
                .contains("discord gateway receiver stopped")
        );

        let requests = rest.handle.join().unwrap();
        assert_eq!(requests.len(), 9);
        assert_reaction(&requests[0], "PUT", EYES_EMOJI);
        assert_eq!(requests[1].method, "POST");
        assert_eq!(requests[1].body["content"], "first answer");
        assert_eq!(requests[2].body["content"], "first answer");
        assert_reaction(&requests[3], "DELETE", EYES_EMOJI);
        assert_reaction(&requests[4], "PUT", FAILURE_EMOJI);
        assert_reaction(&requests[5], "PUT", EYES_EMOJI);
        assert_eq!(requests[6].method, "POST");
        assert_eq!(requests[6].body["content"], "second answer");
        assert_reaction(&requests[7], "DELETE", EYES_EMOJI);
        assert_reaction(&requests[8], "PUT", SUCCESS_EMOJI);
    }

    #[cfg(unix)]
    #[test]
    fn mapped_channel_config_is_forwarded_for_fresh_and_continued_runs() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("daemon.sock");
        let first_daemon =
            spawn_finished_daemon(&socket_path, "run.start", "session_1", "first answer");
        let rest = spawn_fake_rest(8, 200, None);
        let platform = test_platform_messages(
            &rest.base_url,
            [
                discord_message(42, 200, "first"),
                discord_message(42, 200, "second"),
            ],
        );
        let mapped_path = workspace.path().join("mapped.toml");
        let mut gateway = test_gateway(&workspace, socket_path.clone(), platform);
        gateway
            .channel_config_paths
            .insert(200, mapped_path.to_string_lossy().into_owned());

        gateway.poll_once().unwrap();
        let first = first_daemon.join().unwrap();
        assert_eq!(first["config_path"], mapped_path.to_string_lossy().as_ref());
        assert!(first.get("session_id").is_none());

        std::fs::remove_file(&socket_path).unwrap();
        let second_daemon =
            spawn_finished_daemon(&socket_path, "message.append", "session_1", "second answer");
        gateway.poll_once().unwrap();
        let second = second_daemon.join().unwrap();
        assert_eq!(
            second["config_path"],
            mapped_path.to_string_lossy().as_ref()
        );
        assert_eq!(second["session_id"], "session_1");

        rest.handle.join().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn terminal_reaction_waits_once_after_shared_bucket_429() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("daemon.sock");
        let daemon = spawn_finished_daemon(&socket_path, "run.start", "session_1", "final answer");
        let rest = spawn_scripted_rest(vec![
            FakeResponse {
                status: 204,
                body: Value::Null,
                headers: Vec::new(),
            },
            FakeResponse {
                status: 200,
                body: json!({"id": "reply_1"}),
                headers: Vec::new(),
            },
            FakeResponse {
                status: 204,
                body: Value::Null,
                headers: Vec::new(),
            },
            FakeResponse {
                status: 429,
                body: json!({"retry_after": 0.05, "global": false}),
                headers: vec![("Retry-After", "0.05")],
            },
            FakeResponse {
                status: 204,
                body: Value::Null,
                headers: Vec::new(),
            },
        ]);
        let platform = test_platform(&rest.base_url, discord_message(42, 200, "hello"));
        let mut gateway = test_gateway(&workspace, socket_path, platform);

        gateway.poll_once().unwrap();

        daemon.join().unwrap();
        let requests = rest.handle.join().unwrap();
        assert_reaction(&requests[0], "PUT", EYES_EMOJI);
        assert_eq!(requests[1].path, "/channels/200/messages");
        assert_reaction(&requests[2], "DELETE", EYES_EMOJI);
        assert_reaction(&requests[3], "PUT", SUCCESS_EMOJI);
        assert_reaction(&requests[4], "PUT", SUCCESS_EMOJI);
        let retry_delay = requests[4]
            .received_at
            .duration_since(requests[3].received_at);
        assert!(retry_delay >= Duration::from_millis(40));
        assert!(retry_delay < Duration::from_secs(1));
    }

    #[cfg(unix)]
    #[test]
    fn owner_followup_appends_to_channel_session() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("daemon.sock");
        let daemon = spawn_finished_daemon(
            &socket_path,
            "message.append",
            "session_existing",
            "next answer",
        );
        let rest = spawn_fake_rest(4, 200, None);
        let platform = test_platform(&rest.base_url, discord_message(42, 200, "follow up"));
        let overrides = Arc::new(Mutex::new(HashMap::from([(
            200,
            RunOverrides {
                model: Some("openai/gpt-5-mini".into()),
                reasoning_effort: Some(ReasoningEffort::Low),
            },
        )])));
        let mut gateway = test_gateway_with_overrides(&workspace, socket_path, platform, overrides);
        gateway.sessions.insert(200, "session_existing".into());

        gateway.poll_once().unwrap();

        let append_params = daemon.join().unwrap();
        assert_eq!(append_params["message"], "follow up");
        assert_eq!(append_params["session_id"], "session_existing");
        assert_eq!(
            append_params["overrides"],
            json!({
                "model": "openai/gpt-5-mini",
                "reasoning_effort": "low"
            })
        );
        assert_eq!(append_params["wait"], false);
        let requests = rest.handle.join().unwrap();
        assert_eq!(requests[1].body["content"], "next answer");
        assert_eq!(gateway.sessions[&200], "session_existing");
    }

    #[cfg(unix)]
    #[test]
    fn catch_up_pages_do_not_burst_typing() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("daemon.sock");
        let daemon = spawn_catch_up_daemon(&socket_path);
        let rest = spawn_fake_rest(5, 200, None);
        let platform = test_platform(&rest.base_url, discord_message(42, 200, "hello"));
        let mut gateway = test_gateway(&workspace, socket_path, platform);

        gateway.poll_once().unwrap();

        daemon.join().unwrap();
        let requests = rest.handle.join().unwrap();
        assert_reaction(&requests[0], "PUT", EYES_EMOJI);
        assert_eq!(requests[1].method, "POST");
        assert_eq!(requests[1].path, "/channels/200/typing");
        assert_eq!(requests[2].body["content"], "caught up");
        assert_reaction(&requests[3], "DELETE", EYES_EMOJI);
        assert_reaction(&requests[4], "PUT", SUCCESS_EMOJI);
    }

    #[cfg(unix)]
    #[test]
    fn approval_required_run_notifies_once_then_replies_without_deciding() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("daemon.sock");
        let daemon = spawn_approval_daemon(&socket_path);
        let rest = spawn_fake_rest(6, 200, None);
        let platform = test_platform(&rest.base_url, discord_message(42, 200, "edit note"));
        let mut gateway = test_gateway(&workspace, socket_path, platform);

        gateway.poll_once().unwrap();

        let methods = daemon.join().unwrap();
        assert_eq!(
            methods,
            [
                "hello",
                "run.start",
                "events.stream",
                "events.stream",
                "events.stream",
                "transcript.read"
            ]
        );
        assert!(!methods.contains(&"approval.decide"));
        let requests = rest.handle.join().unwrap();
        assert_eq!(requests.len(), 6);
        assert_reaction(&requests[0], "PUT", EYES_EMOJI);
        assert_eq!(
            requests[1].body["content"],
            "Approval required: `file.write` (workspace_write)\nPreview:\n{\n  \"content\": \"hello\",\n  \"path\": \"note.txt\"\n}\nGrant or deny it locally in `plato-tui`."
        );
        assert_eq!(requests[2].method, "POST");
        assert_eq!(requests[2].path, "/channels/200/typing");
        assert_eq!(requests[3].body["content"], "saved note");
        assert_reaction(&requests[4], "DELETE", EYES_EMOJI);
        assert_reaction(&requests[5], "PUT", SUCCESS_EMOJI);
    }

    #[cfg(unix)]
    #[test]
    fn request_decision_and_terminal_in_one_page_do_not_emit_stale_effects() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("daemon.sock");
        let daemon = spawn_folded_terminal_daemon(&socket_path);
        let rest = spawn_fake_rest(4, 200, None);
        let platform = test_platform(&rest.base_url, discord_message(42, 200, "edit note"));
        let mut gateway = test_gateway(&workspace, socket_path, platform);

        gateway.poll_once().unwrap();

        daemon.join().unwrap();
        let requests = rest.handle.join().unwrap();
        assert_reaction(&requests[0], "PUT", EYES_EMOJI);
        assert_eq!(requests[1].body["content"], "saved without stale effects");
        assert_reaction(&requests[2], "DELETE", EYES_EMOJI);
        assert_reaction(&requests[3], "PUT", SUCCESS_EMOJI);
    }

    #[cfg(unix)]
    #[test]
    fn failed_run_sends_canonical_terminal_notification() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("daemon.sock");
        let daemon = spawn_failed_daemon(&socket_path);
        let rest = spawn_fake_rest(4, 200, None);
        let platform = test_platform(&rest.base_url, discord_message(42, 200, "fail"));
        let mut gateway = test_gateway(&workspace, socket_path, platform);

        gateway.poll_once().unwrap();

        daemon.join().unwrap();
        let requests = rest.handle.join().unwrap();
        assert_reaction(&requests[0], "PUT", EYES_EMOJI);
        assert_eq!(requests[1].body["content"], RUN_FAILED_MESSAGE);
        assert_reaction(&requests[2], "DELETE", EYES_EMOJI);
        assert_reaction(&requests[3], "PUT", FAILURE_EMOJI);
    }

    #[test]
    fn approval_fold_suppresses_resolved_requests_and_bounds_unicode_preview() {
        let mut approvals = ApprovalNotifications::default();
        let long_preview = "界".repeat(DISCORD_MESSAGE_LIMIT);
        let _ = approvals.fold(&[
            buffered_event(
                0,
                json!({
                    "kind": "approval_requested",
                    "run_id": "run_1",
                    "tool_call_id": "call_1",
                    "tool_name": "file.edit",
                    "effect": "workspace_write",
                    "reason": "approval required",
                    "diff_preview": long_preview
                }),
            ),
            ledger_event(
                1,
                json!({
                    "event": "approval_granted",
                    "run_id": "run_1",
                    "call_id": "call_1",
                    "actor_id": "human_1"
                }),
            ),
        ]);

        assert_eq!(approvals.take_notification(), None);

        let _ = approvals.fold(&[buffered_event(
            2,
            json!({
                "kind": "approval_requested",
                "run_id": "run_1",
                "tool_call_id": "call_2",
                "tool_name": "file.edit",
                "effect": "workspace_write",
                "reason": "approval required",
                "diff_preview": "界".repeat(DISCORD_MESSAGE_LIMIT)
            }),
        )]);
        let message = approvals.take_notification().unwrap();
        assert!(message.chars().count() <= DISCORD_MESSAGE_LIMIT);
        assert!(message.ends_with("Grant or deny it locally in `plato-tui`."));
        assert_eq!(approvals.take_notification(), None);
    }

    #[test]
    fn approval_fold_suppresses_a_request_canceled_while_status_is_running() {
        let mut approvals = ApprovalNotifications::default();
        let canceled = approvals.fold(&[
            buffered_event(
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
            ),
            buffered_event(
                1,
                json!({
                    "kind": "canceled",
                    "run_id": "run_1"
                }),
            ),
        ]);

        assert_eq!(approvals.take_notification(), None);
        assert!(canceled);
    }

    #[test]
    fn typing_deadline_is_immediate_bounded_and_resumes_immediately() {
        let now = Instant::now();
        let mut presentation = MessagePresentation::new(200, 300);

        assert!(presentation.typing_due(false, now));
        assert!(!presentation.typing_due(false, now + TYPING_INTERVAL - Duration::from_millis(1)));
        assert!(presentation.typing_due(false, now + TYPING_INTERVAL));
        assert!(!presentation.typing_due(true, now + TYPING_INTERVAL));
        assert!(presentation.typing_due(false, now + TYPING_INTERVAL));
    }

    #[test]
    fn approval_fold_normalizes_transient_and_durable_call_ids() {
        let mut approvals = ApprovalNotifications::default();
        let _ = approvals.fold(&[buffered_event(
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
        )]);
        let _ = approvals.fold(&[ledger_event(
            1,
            json!({
                "event": "approval_denied",
                "run_id": "run_1",
                "call_id": "call_2",
                "actor_id": "human_1",
                "reason": "denied"
            }),
        )]);
        assert!(approvals.pending.is_some());

        let _ = approvals.fold(&[ledger_event(
            2,
            json!({
                "event": "approval_granted",
                "run_id": "run_1",
                "call_id": "call_1",
                "actor_id": "human_1"
            }),
        )]);
        assert!(approvals.pending.is_none());
    }

    #[test]
    fn canceled_and_interrupted_runs_are_silent() {
        for status in [RunStateName::Canceled, RunStateName::Interrupted] {
            assert_eq!(
                terminal_message(TranscriptReadResult {
                    run_id: "run_1".into(),
                    status,
                    final_answer: None,
                    transcript: String::new(),
                    typed: None,
                    pending_approval: None,
                    completion_claim: None,
                })
                .unwrap(),
                None
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn canceled_and_interrupted_runs_only_remove_eyes() {
        for status in [RunStateName::Canceled, RunStateName::Interrupted] {
            let workspace = tempfile::tempdir().unwrap();
            let socket_dir = tempfile::tempdir().unwrap();
            let socket_path = socket_dir.path().join("daemon.sock");
            let daemon = spawn_status_daemon(&socket_path, vec![status]);
            let rest = spawn_fake_rest(2, 200, None);
            let platform = test_platform(&rest.base_url, discord_message(42, 200, "stop"));
            let mut gateway = test_gateway(&workspace, socket_path, platform);

            gateway.poll_once().unwrap();

            daemon.join().unwrap();
            let requests = rest.handle.join().unwrap();
            assert_reaction(&requests[0], "PUT", EYES_EMOJI);
            assert_reaction(&requests[1], "DELETE", EYES_EMOJI);
        }
    }

    #[cfg(unix)]
    #[test]
    fn cancel_requested_stops_typing_without_changing_reactions() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("daemon.sock");
        let daemon = spawn_status_daemon(
            &socket_path,
            vec![RunStateName::CancelRequested, RunStateName::Canceled],
        );
        let rest = spawn_fake_rest(2, 200, None);
        let platform = test_platform(&rest.base_url, discord_message(42, 200, "stop"));
        let mut gateway = test_gateway(&workspace, socket_path, platform);

        gateway.poll_once().unwrap();

        daemon.join().unwrap();
        let requests = rest.handle.join().unwrap();
        assert_reaction(&requests[0], "PUT", EYES_EMOJI);
        assert_reaction(&requests[1], "DELETE", EYES_EMOJI);
    }

    #[cfg(unix)]
    #[test]
    fn canceled_event_keeps_running_status_quiet_until_terminal() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("daemon.sock");
        let daemon = spawn_canceled_event_daemon(&socket_path);
        let rest = spawn_fake_rest(2, 200, None);
        let platform = test_platform(&rest.base_url, discord_message(42, 200, "stop"));
        let mut gateway = test_gateway(&workspace, socket_path, platform);

        gateway.poll_once().unwrap();

        daemon.join().unwrap();
        let requests = rest.handle.join().unwrap();
        assert_reaction(&requests[0], "PUT", EYES_EMOJI);
        assert_reaction(&requests[1], "DELETE", EYES_EMOJI);
    }

    #[test]
    fn outer_daemon_failure_attempts_reaction_cleanup_then_propagates() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let rest = spawn_fake_rest(3, 200, None);
        let platform = test_platform(&rest.base_url, discord_message(42, 200, "hello"));
        let mut gateway =
            test_gateway(&workspace, socket_dir.path().join("missing.sock"), platform);

        let error = gateway.poll_once().unwrap_err();

        assert!(matches!(error, GatewayError::Client(ClientError::Io(_))));
        let requests = rest.handle.join().unwrap();
        assert_reaction(&requests[0], "PUT", EYES_EMOJI);
        assert_reaction(&requests[1], "DELETE", EYES_EMOJI);
        assert_reaction(&requests[2], "PUT", FAILURE_EMOJI);
    }

    #[cfg(unix)]
    #[test]
    fn product_message_failure_attempts_cleanup_then_is_contained() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("daemon.sock");
        let daemon = spawn_finished_daemon(&socket_path, "run.start", "session_1", "answer");
        let rest = spawn_scripted_rest(vec![
            FakeResponse {
                status: 200,
                body: json!({}),
                headers: Vec::new(),
            },
            FakeResponse {
                status: 500,
                body: json!({}),
                headers: Vec::new(),
            },
            FakeResponse {
                status: 200,
                body: json!({}),
                headers: Vec::new(),
            },
            FakeResponse {
                status: 200,
                body: json!({}),
                headers: Vec::new(),
            },
        ]);
        let platform = test_platform(&rest.base_url, discord_message(42, 200, "hello"));
        let mut gateway = test_gateway(&workspace, socket_path, platform);

        gateway.poll_once().unwrap();

        daemon.join().unwrap();
        let requests = rest.handle.join().unwrap();
        assert_reaction(&requests[0], "PUT", EYES_EMOJI);
        assert_eq!(requests[1].path, "/channels/200/messages");
        assert_reaction(&requests[2], "DELETE", EYES_EMOJI);
        assert_reaction(&requests[3], "PUT", FAILURE_EMOJI);
    }

    #[cfg(unix)]
    #[test]
    fn reconnect_reads_exact_run_when_the_session_has_advanced() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("daemon.sock");
        let daemon = spawn_advanced_session_daemon(&socket_path, "recovered answer");
        let rest = spawn_fake_rest(4, 200, None);
        let platform = test_platform(&rest.base_url, discord_message(42, 200, "hello"));
        let mut gateway = test_gateway(&workspace, socket_path, platform);

        gateway.poll_once().unwrap();

        daemon.join().unwrap();
        let requests = rest.handle.join().unwrap();
        assert_eq!(requests[1].body["content"], "recovered answer");
    }

    #[cfg(unix)]
    #[test]
    fn reconnect_clears_pending_pause_and_resumes_typing() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("daemon.sock");
        let daemon = spawn_reconnecting_pending_daemon(&socket_path);
        let rest = spawn_fake_rest(6, 200, None);
        let platform = test_platform(&rest.base_url, discord_message(42, 200, "hello"));
        let mut gateway = test_gateway(&workspace, socket_path, platform);

        gateway.poll_once().unwrap();

        daemon.join().unwrap();
        let requests = rest.handle.join().unwrap();
        assert_reaction(&requests[0], "PUT", EYES_EMOJI);
        assert!(
            requests[1].body["content"]
                .as_str()
                .unwrap()
                .starts_with("Approval required:")
        );
        assert_eq!(requests[2].path, "/channels/200/typing");
        assert_eq!(requests[3].body["content"], "answer after reconnect");
        assert_reaction(&requests[4], "DELETE", EYES_EMOJI);
        assert_reaction(&requests[5], "PUT", SUCCESS_EMOJI);
    }

    #[cfg(unix)]
    #[test]
    fn lag_resumes_at_tip_and_reads_typed_final_answer() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("daemon.sock");
        let daemon = spawn_lagged_daemon(&socket_path, "answer after lag");
        let rest = spawn_fake_rest(6, 200, None);
        let platform = test_platform(&rest.base_url, discord_message(42, 200, "hello"));
        let mut gateway = test_gateway(&workspace, socket_path, platform);

        gateway.poll_once().unwrap();

        daemon.join().unwrap();
        let requests = rest.handle.join().unwrap();
        assert_eq!(
            requests[1].body["content"],
            "Approval required: `file.write` (workspace_write)\nPreview:\nwrite note.txt\nGrant or deny it locally in `plato-tui`."
        );
        assert_eq!(requests[2].path, "/channels/200/typing");
        assert_eq!(requests[3].body["content"], "answer after lag");
        assert_reaction(&requests[4], "DELETE", EYES_EMOJI);
        assert_reaction(&requests[5], "PUT", SUCCESS_EMOJI);
    }
}
