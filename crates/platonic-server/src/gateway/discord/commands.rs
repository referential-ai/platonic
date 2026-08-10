use super::{
    GatewayError, GatewayResult, PendingGatewayApproval,
    daemon_bridge::{require_gateway_daemon_contract, require_remote_ceiling},
    rest::{AllowedMentions, CreateMessage, DiscordRestClient, discord_http_error},
};
use crate::config::DiscordGatewayPrincipal;
use platonic_client::client::{DaemonClient, DaemonConnectionConfig};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    thread,
};

pub(super) const DISCORD_STATUS_COMMAND: &str = "status";
pub(super) const DISCORD_STATUS_DESCRIPTION: &str = "Show this gateway thread status";
pub(super) const DISCORD_APPROVE_COMMAND: &str = "approve";
pub(super) const DISCORD_APPROVE_DESCRIPTION: &str = "Approve this channel's pending effect";
pub(super) const DISCORD_DENY_COMMAND: &str = "deny";
pub(super) const DISCORD_DENY_DESCRIPTION: &str = "Deny this channel's pending effect";
pub(super) const DISCORD_APPLICATION_COMMAND: u8 = 2;
pub(super) const DISCORD_CHAT_INPUT_COMMAND: u8 = 1;
pub(super) const DISCORD_DEFERRED_CHANNEL_MESSAGE: u8 = 5;
pub(super) const DISCORD_EPHEMERAL_FLAG: u64 = 64;

impl DiscordRestClient {
    pub(super) fn replace_application_commands(&self, application_id: u64) -> GatewayResult<()> {
        let commands = [
            DiscordApplicationCommand {
                kind: DISCORD_CHAT_INPUT_COMMAND,
                name: DISCORD_STATUS_COMMAND,
                description: DISCORD_STATUS_DESCRIPTION,
            },
            DiscordApplicationCommand {
                kind: DISCORD_CHAT_INPUT_COMMAND,
                name: DISCORD_APPROVE_COMMAND,
                description: DISCORD_APPROVE_DESCRIPTION,
            },
            DiscordApplicationCommand {
                kind: DISCORD_CHAT_INPUT_COMMAND,
                name: DISCORD_DENY_COMMAND,
                description: DISCORD_DENY_DESCRIPTION,
            },
        ];
        let response = self
            .request(self.agent.put(&format!(
                "{}/applications/{application_id}/commands",
                self.api_base
            )))
            .send_json(commands)
            .map_err(|error| discord_http_error("command synchronization", error))?;
        let registered: Vec<RegisteredApplicationCommand> = response.into_json().map_err(|_| {
            GatewayError::Discord("discord command synchronization returned invalid JSON".into())
        })?;
        let expected = [
            DISCORD_STATUS_COMMAND,
            DISCORD_APPROVE_COMMAND,
            DISCORD_DENY_COMMAND,
        ];
        if registered.len() != expected.len()
            || expected.iter().any(|expected| {
                !registered.iter().any(|registered| {
                    registered.kind == DISCORD_CHAT_INPUT_COMMAND && registered.name == *expected
                })
            })
        {
            return Err(GatewayError::Discord(
                "discord command synchronization returned an unexpected registry".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(super) struct DiscordCommandHandler {
    pub(super) api_base: String,
    pub(super) application_id: u64,
    pub(super) daemon: DaemonConnectionConfig,
    pub(super) principals: HashMap<u64, DiscordGatewayPrincipal>,
    pub(super) channel_thread_ids: HashMap<u64, String>,
    pub(super) pending_approvals: Arc<Mutex<HashMap<u64, PendingGatewayApproval>>>,
    pub(super) daemon_client_timeout: std::time::Duration,
    pub(super) presentation_timeout: std::time::Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DiscordCommand {
    Status,
    Approve,
    Deny,
}

impl DiscordCommandHandler {
    pub(super) fn handle(&self, interaction: InteractionCreateEvent) -> GatewayResult<()> {
        if interaction.kind != DISCORD_APPLICATION_COMMAND {
            return Ok(());
        }
        let author_id = interaction
            .member
            .as_ref()
            .map(|member| &member.user)
            .or(interaction.user.as_ref())
            .map(|user| parse_snowflake(&user.id))
            .transpose()?
            .ok_or_else(|| {
                GatewayError::Discord("discord interaction omitted its author".into())
            })?;
        let Some(principal) = self.principals.get(&author_id).cloned() else {
            return Ok(());
        };
        let channel_id = parse_snowflake(&interaction.channel_id)?;
        if !self.channel_thread_ids.contains_key(&channel_id) {
            return Ok(());
        }
        let application_id = parse_snowflake(&interaction.application_id)?;
        if application_id != self.application_id {
            return Err(GatewayError::Discord(
                "discord interaction application id does not match the authenticated bot".into(),
            ));
        }
        let Some(data) = interaction.data.as_ref() else {
            return Err(GatewayError::Discord(
                "discord interaction omitted command data".into(),
            ));
        };
        if data.kind != DISCORD_CHAT_INPUT_COMMAND {
            return Ok(());
        }
        let Some(command) = discord_command(data)? else {
            return Ok(());
        };
        let interaction_id = parse_snowflake(&interaction.id)?;
        if interaction.token.is_empty() {
            return Err(GatewayError::Discord(
                "discord interaction omitted its authentication token".into(),
            ));
        }
        let handler = self.clone();
        thread::Builder::new()
            .name("discord-command".into())
            .spawn(move || {
                if let Err(error) = handler.respond(
                    interaction_id,
                    &interaction.token,
                    channel_id,
                    principal,
                    command,
                ) {
                    eprintln!("discord command interaction failed: {error}");
                }
            })?;
        Ok(())
    }

    fn respond(
        &self,
        interaction_id: u64,
        interaction_token: &str,
        channel_id: u64,
        principal: DiscordGatewayPrincipal,
        command: DiscordCommand,
    ) -> GatewayResult<()> {
        let agent = ureq::AgentBuilder::new()
            .timeout(self.presentation_timeout)
            .build();
        agent
            .post(&format!(
                "{}/interactions/{interaction_id}/{interaction_token}/callback",
                self.api_base
            ))
            .send_json(DiscordInteractionResponse {
                kind: DISCORD_DEFERRED_CHANNEL_MESSAGE,
                data: DiscordInteractionResponseData {
                    flags: DISCORD_EPHEMERAL_FLAG,
                },
            })
            .map_err(|error| discord_http_error("command defer", error))?;

        let content = match command {
            DiscordCommand::Status => self.status_content(channel_id, &principal)?,
            DiscordCommand::Approve => self.decide_pending(channel_id, &principal, true)?,
            DiscordCommand::Deny => self.decide_pending(channel_id, &principal, false)?,
        };
        agent
            .patch(&format!(
                "{}/webhooks/{}/{interaction_token}/messages/@original",
                self.api_base, self.application_id
            ))
            .send_json(CreateMessage {
                content,
                allowed_mentions: AllowedMentions { parse: Vec::new() },
            })
            .map_err(|error| discord_http_error("command response edit", error))?;
        Ok(())
    }

    fn status_content(
        &self,
        channel_id: u64,
        principal: &DiscordGatewayPrincipal,
    ) -> GatewayResult<String> {
        let thread_id = &self.channel_thread_ids[&channel_id];
        let status = (|| {
            let mut daemon = self.connect_daemon()?;
            let thread = daemon.thread_status(thread_id.clone())?.thread;
            require_remote_ceiling(principal, thread.authority.approval_policy)?;
            Ok::<_, GatewayError>(thread)
        })();
        match status {
            Ok(thread) => Ok(format!(
                "Platonic gateway status\nDaemon: connected\nPrincipal: {}\nThread: {}\nModel: {}\nReasoning effort: {}\nApproval policy: {}\nState: {}",
                principal.name,
                thread.authority.thread_id,
                thread.authority.model,
                thread.authority.reasoning_effort,
                thread.authority.approval_policy,
                if thread.live.current_turn_id.is_some() {
                    "running"
                } else {
                    "idle"
                },
            )),
            Err(_) => Ok(format!(
                "Platonic gateway status\nDaemon: unavailable\nPrincipal: {}\nThread: {thread_id}",
                principal.name
            )),
        }
    }

    fn decide_pending(
        &self,
        channel_id: u64,
        principal: &DiscordGatewayPrincipal,
        grant: bool,
    ) -> GatewayResult<String> {
        let pending = self
            .pending_approvals
            .lock()
            .map_err(|_| GatewayError::Discord("discord pending approval lock poisoned".into()))?
            .get(&channel_id)
            .cloned();
        let Some(pending) = pending else {
            return Ok("No pending effect for this channel.".into());
        };
        let thread_id = &self.channel_thread_ids[&channel_id];
        let mut daemon = self.connect_daemon()?;
        let authority = daemon.thread_authority(thread_id.clone())?.authority;
        require_remote_ceiling(principal, authority.approval_policy)?;
        if grant {
            daemon.approval_grant_as(
                &pending.run_id,
                &pending.tool_call_id,
                principal.name.clone(),
            )?;
        } else {
            daemon.approval_deny_as(
                &pending.run_id,
                &pending.tool_call_id,
                principal.name.clone(),
                "denied by Discord principal".into(),
            )?;
        }
        let mut shared = self
            .pending_approvals
            .lock()
            .map_err(|_| GatewayError::Discord("discord pending approval lock poisoned".into()))?;
        if shared.get(&channel_id) == Some(&pending) {
            shared.remove(&channel_id);
        }
        Ok(format!(
            "{} operation `{}` as `{}`.",
            if grant { "Approved" } else { "Denied" },
            pending.tool_call_id,
            principal.name
        ))
    }

    fn connect_daemon(&self) -> GatewayResult<DaemonClient> {
        let mut daemon = DaemonClient::connect_with_timeout(
            &self.daemon.socket_path,
            self.daemon_client_timeout,
        )?;
        let hello = daemon.hello(&self.daemon.workspace_root)?;
        require_gateway_daemon_contract(&self.daemon.workspace_root, &hello)?;
        Ok(daemon)
    }
}

fn discord_command(data: &InteractionData) -> GatewayResult<Option<DiscordCommand>> {
    if !data.options.is_empty() {
        return Err(GatewayError::Discord(
            "discord command interaction included unexpected options".into(),
        ));
    }
    Ok(match data.name.as_str() {
        DISCORD_STATUS_COMMAND => Some(DiscordCommand::Status),
        DISCORD_APPROVE_COMMAND => Some(DiscordCommand::Approve),
        DISCORD_DENY_COMMAND => Some(DiscordCommand::Deny),
        _ => None,
    })
}

#[derive(Serialize)]
struct DiscordApplicationCommand {
    #[serde(rename = "type")]
    kind: u8,
    name: &'static str,
    description: &'static str,
}

#[derive(Deserialize)]
struct RegisteredApplicationCommand {
    #[serde(rename = "type")]
    kind: u8,
    name: String,
}

#[derive(Deserialize)]
pub(super) struct InteractionCreateEvent {
    pub(super) id: String,
    pub(super) application_id: String,
    pub(super) channel_id: String,
    #[serde(rename = "type")]
    pub(super) kind: u8,
    pub(super) token: String,
    pub(super) data: Option<InteractionData>,
    pub(super) member: Option<InteractionMember>,
    pub(super) user: Option<DiscordAuthor>,
}

#[derive(Deserialize)]
pub(super) struct InteractionData {
    #[serde(rename = "type")]
    pub(super) kind: u8,
    pub(super) name: String,
    #[serde(default)]
    pub(super) options: Vec<Value>,
}

#[derive(Deserialize)]
pub(super) struct InteractionMember {
    pub(super) user: DiscordAuthor,
}

#[derive(Serialize)]
struct DiscordInteractionResponse {
    #[serde(rename = "type")]
    kind: u8,
    data: DiscordInteractionResponseData,
}

#[derive(Serialize)]
struct DiscordInteractionResponseData {
    flags: u64,
}

#[derive(Deserialize)]
pub(super) struct DiscordAuthor {
    pub(super) id: String,
    pub(super) bot: Option<bool>,
}

pub(super) fn parse_snowflake(value: &str) -> GatewayResult<u64> {
    value
        .parse()
        .map_err(|_| GatewayError::Discord("discord gateway returned an invalid snowflake".into()))
}

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::*;
    #[cfg(unix)]
    use std::os::unix::net::UnixListener;

    #[test]
    fn unknown_principal_command_is_ignored_before_channel_rest_or_daemon_access() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let rest = spawn_fake_rest(0, 200, None);
        let handler = test_command_handler(
            &rest.base_url,
            &workspace,
            socket_dir.path().join("missing.sock"),
        );

        handler
            .handle(discord_command_interaction(
                99,
                201,
                DISCORD_APPROVE_COMMAND,
                None,
            ))
            .unwrap();

        assert!(rest.handle.join().unwrap().is_empty());
        assert!(handler.pending_approvals.lock().unwrap().is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn unmapped_principal_interaction_is_ignored_before_rest_or_daemon_access() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("daemon.sock");
        let daemon = UnixListener::bind(&socket_path).unwrap();
        daemon.set_nonblocking(true).unwrap();
        let rest = spawn_observed_rest(Vec::new());
        let handler = test_command_handler(&rest.base_url, &workspace, socket_path);
        let mut interaction = discord_status_interaction(42);
        interaction.channel_id = "201".into();

        handler.handle(interaction).unwrap();

        assert!(rest.finish().is_empty());
        assert!(matches!(
            daemon.accept(),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
        ));
    }

    #[test]
    fn mismatched_application_and_empty_interaction_token_fail_closed() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let handler = test_command_handler(
            "http://127.0.0.1",
            &workspace,
            socket_dir.path().join("missing.sock"),
        );
        let mut mismatched = discord_status_interaction(42);
        mismatched.application_id = "101".into();
        assert!(
            handler
                .handle(mismatched)
                .unwrap_err()
                .to_string()
                .contains("does not match the authenticated bot")
        );
        let mut empty = discord_status_interaction(42);
        empty.token.clear();
        assert!(
            handler
                .handle(empty)
                .unwrap_err()
                .to_string()
                .contains("omitted its authentication token")
        );
    }
}
