use super::{DiscordGateway, DiscordPlatform, PendingGatewayApproval, rest::DISCORD_MESSAGE_LIMIT};
use super::{GatewayError, GatewayResult};
use crate::config::DiscordGatewayPrincipal;
use platonic_client::{
    ClientError,
    client::{DaemonClient, DaemonConnectionConfig},
    paths,
};
#[cfg(test)]
use platonic_protocol::BufferedStreamEvent;
use platonic_protocol::{
    BufferedThreadEvent, CAPABILITY_APPROVAL_DECIDE, CAPABILITY_HELLO, CAPABILITY_THREAD_AUTHORITY,
    CAPABILITY_THREAD_EVENTS, CAPABILITY_THREAD_SEND, CAPABILITY_THREAD_STATUS,
    CAPABILITY_TRANSCRIPT_READ, Capability, ERROR_LAGGED, ERROR_NOT_FOUND, HarnessEvent,
    HelloResult, RunStateName, StreamEvent, ThreadApprovalPolicy, ThreadSendResult,
    TranscriptReadResult,
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
pub(super) const REQUIRED_CAPABILITIES: [Capability; 7] = [
    CAPABILITY_HELLO,
    CAPABILITY_THREAD_AUTHORITY,
    CAPABILITY_THREAD_STATUS,
    CAPABILITY_THREAD_SEND,
    CAPABILITY_THREAD_EVENTS,
    CAPABILITY_APPROVAL_DECIDE,
    CAPABILITY_TRANSCRIPT_READ,
];

impl DiscordGateway {
    pub(super) fn handle_message(
        &mut self,
        message: super::websocket::DiscordMessage,
        principal: DiscordGatewayPrincipal,
        thread_id: String,
    ) -> GatewayResult<()> {
        let mut presentation = MessagePresentation::new(message.channel_id, message.id);
        presentation.add_eyes(&self.platform);
        let result = self.handle_allowed_message(message, principal, thread_id, &mut presentation);
        if result.is_err() {
            presentation.abnormal_exit(&self.platform);
        }
        result
    }

    fn handle_allowed_message(
        &mut self,
        message: super::websocket::DiscordMessage,
        principal: DiscordGatewayPrincipal,
        thread_id: String,
        presentation: &mut MessagePresentation,
    ) -> GatewayResult<()> {
        let channel_id = message.channel_id;
        let mut daemon = self.connect_daemon(self.daemon_client_timeout)?;
        let status = daemon.thread_status(thread_id.clone())?.thread;
        require_remote_ceiling(&principal, status.authority.approval_policy)?;
        let controller_id = principal.name.clone();
        let turn_id = match daemon.thread_send(
            thread_id.clone(),
            controller_id,
            status.live.current_turn_id,
            message.content,
        )? {
            ThreadSendResult::Started { turn_id, .. }
            | ThreadSendResult::Steered { turn_id, .. } => turn_id,
            ThreadSendResult::Rejected { reason, .. } => {
                return Err(GatewayError::DaemonProtocol(format!(
                    "thread.send rejected the Discord message: {reason:?}"
                )));
            }
        };
        let terminal =
            self.wait_for_thread(&mut daemon, channel_id, &thread_id, &turn_id, presentation)?;
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

    fn wait_for_thread(
        &self,
        daemon: &mut DaemonClient,
        channel_id: u64,
        thread_id: &str,
        turn_id: &str,
        presentation: &mut MessagePresentation,
    ) -> GatewayResult<FinishedThreadTurn> {
        let mut next_offset = Some(0);
        let mut approvals = ApprovalNotifications::default();
        let mut readback = ThreadTurnReadback::default();
        let mut canceling = false;
        let mut needs_durable_terminal = false;
        loop {
            match daemon
                .thread_events(
                    thread_id.into(),
                    next_offset,
                    EVENT_PAGE_LIMIT,
                    u64::try_from(self.event_poll_delay.as_millis()).unwrap_or(u64::MAX),
                )
                .map_err(GatewayError::from)
            {
                Ok(events) => {
                    next_offset = Some(events.next_offset);
                    let needs_catch_up = events.events.len() == EVENT_PAGE_LIMIT
                        && events.next_offset > events.from_offset;
                    let was_pending = approvals.pending.is_some();
                    canceling |= approvals.fold_thread(&events.events, turn_id);
                    readback.fold(&events.events, turn_id);
                    self.sync_pending_approval(channel_id, approvals.pending.as_ref())?;
                    if was_pending != approvals.pending.is_some() {
                        presentation.stop_typing();
                    }
                    if needs_catch_up {
                        continue;
                    }
                    if let Some(message) = approvals.take_notification()
                        && let Err(error) = self.platform.send_message(channel_id, &message)
                    {
                        report_response_delivery_failure(&error);
                    }
                    if events.current_turn_id.as_deref() != Some(turn_id) && !needs_catch_up {
                        presentation.stop_typing();
                        approvals.clear();
                        self.sync_pending_approval(channel_id, None)?;
                        return if needs_durable_terminal {
                            self.read_durable_thread_turn(daemon, thread_id, turn_id, &readback)
                        } else {
                            readback.finish(thread_id, turn_id)
                        };
                    }
                    presentation.observe_running(
                        &self.platform,
                        approvals.pending.is_some() || canceling,
                        Instant::now(),
                    );
                }
                Err(GatewayError::Client(ClientError::DaemonResponse(error)))
                    if error.code == ERROR_LAGGED =>
                {
                    approvals.clear();
                    self.sync_pending_approval(channel_id, None)?;
                    canceling = false;
                    presentation.stop_typing();
                    needs_durable_terminal = true;
                    next_offset = None;
                }
                Err(error) if reconnectable(&error) => {
                    *daemon = self.reconnect_daemon()?;
                    approvals.clear();
                    self.sync_pending_approval(channel_id, None)?;
                    canceling = false;
                    presentation.stop_typing();
                    readback.clear_terminal_facts();
                    next_offset = Some(0);
                    needs_durable_terminal = true;
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

    fn read_durable_thread_turn(
        &self,
        daemon: &mut DaemonClient,
        thread_id: &str,
        turn_id: &str,
        readback: &ThreadTurnReadback,
    ) -> GatewayResult<FinishedThreadTurn> {
        let run_id = readback.run_id.as_deref().ok_or_else(|| {
            GatewayError::DaemonProtocol(format!(
                "cannot safely recover thread {thread_id} turn {turn_id}: no exact run identifier was retained"
            ))
        })?;
        let transcript = self.read_exact_run(daemon, run_id)?;
        if transcript.run_id != run_id {
            return Err(GatewayError::DaemonProtocol(format!(
                "transcript.read returned run {} while recovering exact run {run_id}",
                transcript.run_id
            )));
        }
        match transcript.status {
            RunStateName::Finished
            | RunStateName::Failed
            | RunStateName::Canceled
            | RunStateName::Interrupted => Ok(FinishedThreadTurn {
                status: transcript.status,
                final_answer: transcript.final_answer,
            }),
            RunStateName::Running | RunStateName::CancelRequested => {
                Err(GatewayError::DaemonProtocol(format!(
                    "thread {thread_id} turn {turn_id} became idle while exact run {run_id} remained {}",
                    transcript.status
                )))
            }
        }
    }

    fn read_exact_run(
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

    fn connect_daemon(&self, timeout: Duration) -> GatewayResult<DaemonClient> {
        let mut client = DaemonClient::connect_with_timeout(&self.daemon.socket_path, timeout)?;
        let hello = client.hello(&self.daemon.workspace_root)?;
        require_gateway_daemon_contract(&self.daemon.workspace_root, &hello)?;
        Ok(client)
    }

    fn sync_pending_approval(
        &self,
        channel_id: u64,
        pending: Option<&PendingApprovalNotification>,
    ) -> GatewayResult<()> {
        let mut shared = self
            .pending_approvals
            .lock()
            .map_err(|_| GatewayError::Discord("discord pending approval lock poisoned".into()))?;
        match pending {
            Some(pending) => {
                shared.insert(
                    channel_id,
                    PendingGatewayApproval {
                        run_id: pending.run_id.clone(),
                        tool_call_id: pending.call_id.clone(),
                    },
                );
            }
            None => {
                shared.remove(&channel_id);
            }
        }
        Ok(())
    }
}

pub(super) fn require_remote_ceiling(
    principal: &DiscordGatewayPrincipal,
    policy: ThreadApprovalPolicy,
) -> GatewayResult<()> {
    if principal.remote_ceiling == ThreadApprovalPolicy::Prompt
        && policy == ThreadApprovalPolicy::Yolo
    {
        return Err(GatewayError::DaemonProtocol(format!(
            "principal {} has prompt remote ceiling and cannot control a yolo thread",
            principal.name
        )));
    }
    Ok(())
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

pub(super) fn validate_gateway_threads(
    config: &DaemonConnectionConfig,
    channel_thread_ids: &HashMap<u64, String>,
    timeout: Duration,
) -> GatewayResult<()> {
    let mut client = DaemonClient::connect_with_timeout(&config.socket_path, timeout)?;
    let hello = client.hello(&config.workspace_root)?;
    require_gateway_daemon_contract(&config.workspace_root, &hello)?;
    for (channel_id, thread_id) in channel_thread_ids {
        client
            .thread_authority(thread_id.clone())
            .map_err(|error| {
                GatewayError::DaemonProtocol(format!(
                    "gateway channel {channel_id} maps to unavailable thread {thread_id}: {error}"
                ))
            })?;
    }
    Ok(())
}

pub(super) fn require_gateway_daemon_contract(
    workspace_root: &Path,
    hello: &HelloResult,
) -> GatewayResult<()> {
    let legacy_workspace_id = paths::workspace_id(workspace_root)?;
    let minted_workspace_id = hello
        .workspace_id
        .strip_prefix("ws-")
        .is_some_and(|suffix| {
            suffix.len() == 16 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
        });
    if hello.workspace_id != legacy_workspace_id && !minted_workspace_id {
        return Err(GatewayError::DaemonProtocol(format!(
            "hello workspace_id mismatch: expected {legacy_workspace_id} or a server-minted id, got {}",
            hello.workspace_id
        )));
    }
    if let Some(capability) = REQUIRED_CAPABILITIES.iter().find(|capability| {
        !hello
            .capabilities
            .iter()
            .any(|actual| actual == *capability)
    }) {
        return Err(GatewayError::DaemonProtocol(format!(
            "daemon does not advertise required capability {capability}"
        )));
    }
    Ok(())
}

fn terminal_message(readback: FinishedThreadTurn) -> GatewayResult<Option<String>> {
    match readback.status {
        RunStateName::Finished => readback.final_answer.map(Some).ok_or_else(|| {
            GatewayError::RunFailed(format!(
                "thread turn ended with status {} without a final answer",
                readback.status
            ))
        }),
        RunStateName::Failed => Ok(Some(RUN_FAILED_MESSAGE.into())),
        RunStateName::Canceled | RunStateName::Interrupted => Ok(None),
        RunStateName::Running | RunStateName::CancelRequested => {
            Err(GatewayError::DaemonProtocol(format!(
                "thread turn read back with nonterminal status {}",
                readback.status
            )))
        }
    }
}

#[derive(Default)]
struct ThreadTurnReadback {
    run_id: Option<String>,
    status: Option<RunStateName>,
    final_answer: Option<String>,
}

struct FinishedThreadTurn {
    status: RunStateName,
    final_answer: Option<String>,
}

impl ThreadTurnReadback {
    fn fold(&mut self, entries: &[BufferedThreadEvent], turn_id: &str) {
        for entry in entries.iter().filter(|entry| entry.turn_id == turn_id) {
            if let Some(run_id) = stream_event_run_id(&entry.event) {
                self.run_id = Some(run_id.into());
            }
            match &entry.event {
                StreamEvent::Ledger { record } => match &record.event {
                    HarnessEvent::ModelResponded { output, .. } => {
                        self.final_answer = Some(output.content.clone());
                    }
                    HarnessEvent::RunFinished { .. } => self.status = Some(RunStateName::Finished),
                    HarnessEvent::RunFailed { .. } => self.status = Some(RunStateName::Failed),
                    _ => {}
                },
                StreamEvent::Canceled { .. } => self.status = Some(RunStateName::Canceled),
                _ => {}
            }
        }
    }

    fn clear_terminal_facts(&mut self) {
        self.status = None;
        self.final_answer = None;
    }

    fn finish(self, thread_id: &str, turn_id: &str) -> GatewayResult<FinishedThreadTurn> {
        let Some(status) = self.status else {
            return Err(GatewayError::DaemonProtocol(format!(
                "thread {thread_id} turn {turn_id} became idle without a terminal event"
            )));
        };
        Ok(FinishedThreadTurn {
            status,
            final_answer: self.final_answer,
        })
    }
}

fn stream_event_run_id(event: &StreamEvent) -> Option<&str> {
    match event {
        StreamEvent::Ledger { record } => Some(record.event.run_id().as_str()),
        StreamEvent::AssistantDelta { run_id, .. }
        | StreamEvent::ApprovalRequested { run_id, .. }
        | StreamEvent::Canceled { run_id }
        | StreamEvent::CompletionClaimed { run_id, .. } => Some(run_id),
        StreamEvent::Unknown(_) => None,
    }
}

#[derive(Default)]
struct ApprovalNotifications {
    pending: Option<PendingApprovalNotification>,
    input_previews: HashMap<String, String>,
}

struct PendingApprovalNotification {
    run_id: String,
    call_id: String,
    tool_name: String,
    effect: String,
    preview: Option<String>,
    notified: bool,
}

impl ApprovalNotifications {
    #[cfg(test)]
    fn fold(&mut self, entries: &[BufferedStreamEvent]) -> bool {
        self.fold_events(entries.iter().map(|entry| &entry.event))
    }

    fn fold_thread(&mut self, entries: &[BufferedThreadEvent], turn_id: &str) -> bool {
        self.fold_events(
            entries
                .iter()
                .filter(|entry| entry.turn_id == turn_id)
                .map(|entry| &entry.event),
        )
    }

    fn fold_events<'a>(&mut self, events: impl IntoIterator<Item = &'a StreamEvent>) -> bool {
        let mut canceled = false;
        for event in events {
            if matches!(event, StreamEvent::Canceled { .. }) {
                self.clear();
                canceled = true;
                continue;
            }
            if let Some((call_id, preview)) = tool_input_preview(event) {
                self.input_previews.insert(call_id, preview);
            }
            if let StreamEvent::ApprovalRequested {
                run_id,
                tool_call_id,
                tool_name,
                effect,
                diff_preview,
                approval_preview,
                ..
            } = event
            {
                self.pending = Some(PendingApprovalNotification {
                    run_id: run_id.clone(),
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
            if let Some((run_id, call_id)) = approval_resolution_key(event) {
                self.input_previews.remove(call_id);
                if self
                    .pending
                    .as_ref()
                    .map(|pending| (pending.run_id.as_str(), pending.call_id.as_str()))
                    == Some((run_id, call_id))
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

fn approval_resolution_key(event: &StreamEvent) -> Option<(&str, &str)> {
    let StreamEvent::Ledger { record } = event else {
        return None;
    };
    match &record.event {
        HarnessEvent::ApprovalGranted {
            run_id, call_id, ..
        }
        | HarnessEvent::ApprovalDenied {
            run_id, call_id, ..
        } => Some((run_id.as_str(), call_id.as_str())),
        _ => None,
    }
}

fn approval_notification(tool_name: &str, effect: &str, preview: &str) -> String {
    let tool_name = truncate_chars(tool_name, APPROVAL_FIELD_LIMIT);
    let effect = truncate_chars(effect, APPROVAL_FIELD_LIMIT);
    let prefix = format!("Approval required: `{tool_name}` ({effect})\nPreview:\n");
    let suffix = "\nUse `/approve` or `/deny` in this channel.";
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
        GatewayError::Client(ClientError::DaemonResponse(error)) if error.code == ERROR_NOT_FOUND
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
    #[cfg(unix)]
    use super::super::{commands::DISCORD_APPROVE_COMMAND, test_support::*};
    use super::*;
    use crate::config::DiscordGatewayPrincipal;
    use platonic_protocol::ThreadApprovalPolicy;
    use serde_json::json;
    #[cfg(unix)]
    use std::{sync::Arc, time::Duration};

    fn principal(remote_ceiling: ThreadApprovalPolicy) -> DiscordGatewayPrincipal {
        DiscordGatewayPrincipal {
            name: "jerome".into(),
            remote_ceiling,
        }
    }

    #[test]
    fn remote_ceiling_defaults_to_prompt_and_only_explicit_yolo_admits_yolo() {
        require_remote_ceiling(
            &principal(ThreadApprovalPolicy::Prompt),
            ThreadApprovalPolicy::Prompt,
        )
        .unwrap();
        require_remote_ceiling(
            &principal(ThreadApprovalPolicy::Yolo),
            ThreadApprovalPolicy::Prompt,
        )
        .unwrap();
        require_remote_ceiling(
            &principal(ThreadApprovalPolicy::Yolo),
            ThreadApprovalPolicy::Yolo,
        )
        .unwrap();

        let error = require_remote_ceiling(
            &principal(ThreadApprovalPolicy::Prompt),
            ThreadApprovalPolicy::Yolo,
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "daemon protocol error: principal jerome has prompt remote ceiling and cannot control a yolo thread"
        );
    }

    #[test]
    fn pending_approval_is_bound_to_the_exact_run_and_call() {
        let mut approvals = ApprovalNotifications::default();
        let requested = BufferedStreamEvent {
            offset: 0,
            event: serde_json::from_value(json!({
                "kind": "approval_requested",
                "run_id": "run_1",
                "tool_call_id": "call_1",
                "tool_name": "file.write",
                "effect": "workspace_write",
                "reason": "approval required",
                "approval_preview": "write note.txt"
            }))
            .unwrap(),
        };
        let wrong_run = BufferedStreamEvent {
            offset: 1,
            event: serde_json::from_value(json!({
                "kind": "ledger",
                "record": {
                    "seq": 1,
                    "occurred_at_ms": 1,
                    "event": {
                        "event": "approval_granted",
                        "run_id": "run_2",
                        "call_id": "call_1",
                        "actor_id": "jerome"
                    }
                }
            }))
            .unwrap(),
        };
        let right_call = BufferedStreamEvent {
            offset: 3,
            event: serde_json::from_value(json!({
                "kind": "ledger",
                "record": {
                    "seq": 3,
                    "occurred_at_ms": 3,
                    "event": {
                        "event": "approval_granted",
                        "run_id": "run_1",
                        "call_id": "call_1",
                        "actor_id": "jerome"
                    }
                }
            }))
            .unwrap(),
        };
        let wrong_call = BufferedStreamEvent {
            offset: 2,
            event: serde_json::from_value(json!({
                "kind": "ledger",
                "record": {
                    "seq": 2,
                    "occurred_at_ms": 2,
                    "event": {
                        "event": "approval_granted",
                        "run_id": "run_1",
                        "call_id": "call_2",
                        "actor_id": "jerome"
                    }
                }
            }))
            .unwrap(),
        };

        approvals.fold(&[requested]);
        approvals.fold(&[wrong_run]);
        assert!(approvals.pending.is_some());
        approvals.fold(&[wrong_call]);
        assert!(approvals.pending.is_some());
        approvals.fold(&[right_call]);
        assert!(approvals.pending.is_none());
    }

    #[test]
    fn canceled_and_interrupted_thread_turns_are_silent() {
        for status in [RunStateName::Canceled, RunStateName::Interrupted] {
            assert_eq!(
                terminal_message(FinishedThreadTurn {
                    status,
                    final_answer: None,
                })
                .unwrap(),
                None
            );
        }
    }

    #[test]
    fn thread_turn_readback_rejects_a_later_turn_terminal() {
        let later = serde_json::from_value(json!({
            "offset": 9,
            "turn_id": "turn_later",
            "event": {
                "kind": "ledger",
                "record": {
                    "seq": 9,
                    "occurred_at_ms": 9,
                    "event": {
                        "event": "run_finished",
                        "run_id": "run_later"
                    }
                }
            }
        }))
        .unwrap();
        let mut readback = ThreadTurnReadback::default();

        readback.fold(&[later], "turn_gateway");

        assert_eq!(
            readback
                .finish("thread_news", "turn_gateway")
                .err()
                .unwrap()
                .to_string(),
            "daemon protocol error: thread thread_news turn turn_gateway became idle without a terminal event"
        );
    }

    #[cfg(unix)]
    #[test]
    fn reconnect_replays_exact_completed_turn_and_clears_pending_approval() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("daemon.sock");
        let daemon = spawn_reconnecting_completed_thread_daemon(&socket_path);
        let rest = spawn_fake_rest(5, 200, None);
        let platform = test_platform(&rest.base_url, discord_message(42, 200, "finish it"));
        let mut gateway = test_gateway(&workspace, socket_path, platform);
        let pending_approvals = Arc::clone(&gateway.pending_approvals);

        gateway.poll_once().unwrap();

        let daemon_requests = daemon.join().unwrap();
        let rest_requests = rest.handle.join().unwrap();
        assert_eq!(
            daemon_requests
                .iter()
                .map(|request| request.method.as_deref().unwrap())
                .collect::<Vec<_>>(),
            [
                "thread.status",
                "thread.send",
                "thread.events",
                "thread.events",
                "thread.events",
                "thread.events",
                "transcript.read"
            ]
        );
        assert!(pending_approvals.lock().unwrap().is_empty());
        assert_reaction(&rest_requests[0], "PUT", EYES_EMOJI);
        assert!(
            rest_requests[1].body["content"]
                .as_str()
                .unwrap()
                .starts_with("Approval required:")
        );
        assert_eq!(rest_requests[2].body["content"], "recovered exact answer");
        assert_reaction(&rest_requests[3], "DELETE", EYES_EMOJI);
        assert_reaction(&rest_requests[4], "PUT", SUCCESS_EMOJI);
    }

    #[cfg(unix)]
    #[test]
    fn lag_recovers_the_exact_run_from_durable_terminal_readback() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("daemon.sock");
        let daemon = spawn_lagged_completed_thread_daemon(&socket_path);
        let rest = spawn_fake_rest(5, 200, None);
        let platform = test_platform(&rest.base_url, discord_message(42, 200, "finish it"));
        let mut gateway = test_gateway(&workspace, socket_path, platform);

        gateway.poll_once().unwrap();

        let daemon_requests = daemon.join().unwrap();
        let rest_requests = rest.handle.join().unwrap();
        assert_eq!(
            daemon_requests
                .iter()
                .map(|request| request.method.as_deref().unwrap())
                .collect::<Vec<_>>(),
            [
                "thread.status",
                "thread.send",
                "thread.events",
                "thread.events",
                "thread.events",
                "transcript.read"
            ]
        );
        assert_reaction(&rest_requests[0], "PUT", EYES_EMOJI);
        assert_eq!(rest_requests[1].path, "/channels/200/typing");
        assert_eq!(rest_requests[2].body["content"], "durable answer after lag");
        assert_reaction(&rest_requests[3], "DELETE", EYES_EMOJI);
        assert_reaction(&rest_requests[4], "PUT", SUCCESS_EMOJI);
    }

    #[cfg(unix)]
    #[test]
    fn discord_round_trip_steers_approves_exact_effect_and_attributes_principal() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("daemon.sock");
        let daemon = spawn_gateway_roundtrip_daemon(&socket_path);
        let (rest, approval_notification) = spawn_approval_signaling_rest(7);
        let platform = test_platform(&rest.base_url, discord_message(42, 200, "write the note"));
        let gateway = test_gateway(&workspace, socket_path.clone(), platform);
        let pending_approvals = Arc::clone(&gateway.pending_approvals);
        let gateway_worker = std::thread::spawn(move || {
            let mut gateway = gateway;
            gateway.poll_once()
        });

        approval_notification
            .recv_timeout(Duration::from_secs(2))
            .expect("gateway did not publish the pending approval within the proof bound");
        let mut commands = test_command_handler(&rest.base_url, &workspace, socket_path);
        commands.pending_approvals = pending_approvals;
        commands
            .handle(discord_command_interaction(
                42,
                200,
                DISCORD_APPROVE_COMMAND,
                None,
            ))
            .unwrap();

        gateway_worker.join().unwrap().unwrap();
        let daemon_requests = daemon.join().unwrap();
        let rest_requests = rest.handle.join().unwrap();
        let params = |request: &platonic_protocol::Envelope| {
            let value = serde_json::to_value(request.params.as_ref().unwrap()).unwrap();
            value["params"].clone()
        };

        assert_eq!(
            daemon_requests
                .iter()
                .map(|request| request.method.as_deref().unwrap())
                .collect::<Vec<_>>(),
            [
                "thread.status",
                "thread.send",
                "thread.events",
                "thread.authority",
                "approval.decide",
                "thread.events"
            ]
        );
        assert_eq!(
            params(&daemon_requests[1]),
            json!({
                "thread_id": "thread_news",
                "controller_id": "jerome",
                "turn_id": "turn_gateway",
                "message": "write the note"
            })
        );
        assert_eq!(
            params(&daemon_requests[4]),
            json!({
                "run_id": "run_gateway",
                "tool_call_id": "call_exact",
                "decision": "grant",
                "reason": null,
                "actor": "jerome"
            })
        );
        let delivered = rest_requests
            .iter()
            .filter_map(|request| request.body["content"].as_str())
            .collect::<Vec<_>>();
        assert!(delivered.iter().any(|message| {
            message.contains("Approval required:") && message.contains("`/approve`")
        }));
        assert!(delivered.contains(&"Approved operation `call_exact` as `jerome`."));
        assert!(delivered.contains(&"approved and complete"));
    }
}
