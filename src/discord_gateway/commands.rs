use super::{
    daemon_bridge::{DAEMON_CLIENT_TIMEOUT, require_gateway_daemon_contract},
    rest::{
        AllowedMentions, CreateMessage, DiscordRestClient, PRESENTATION_TIMEOUT, discord_http_error,
    },
};
use crate::{
    AppError, AppResult,
    daemon::{
        client::{DaemonClient, DaemonConnectionConfig},
        protocol::RunStateName,
    },
    model::{ReasoningEffort, RunOverrides},
};
use platonic_core::ModelName;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    thread,
};

pub(super) const DISCORD_STATUS_COMMAND: &str = "status";
pub(super) const DISCORD_STATUS_DESCRIPTION: &str = "Show Plato Agent gateway and daemon status";
pub(super) const DISCORD_MODEL_COMMAND: &str = "model";
pub(super) const DISCORD_MODEL_DESCRIPTION: &str = "Show or set this channel's model";
pub(super) const DISCORD_REASONING_COMMAND: &str = "reasoning";
pub(super) const DISCORD_REASONING_DESCRIPTION: &str =
    "Show or set this channel's reasoning effort";
pub(super) const DISCORD_MODEL_OPTION: &str = "name";
pub(super) const DISCORD_REASONING_OPTION: &str = "effort";
const DISCORD_DEFAULT_SETTING: &str = "default";
const DISCORD_MODEL_LIMIT: usize = 256;
pub(super) const DISCORD_APPLICATION_COMMAND: u8 = 2;
pub(super) const DISCORD_CHAT_INPUT_COMMAND: u8 = 1;
pub(super) const DISCORD_STRING_OPTION: u8 = 3;
pub(super) const DISCORD_DEFERRED_CHANNEL_MESSAGE: u8 = 5;
pub(super) const DISCORD_EPHEMERAL_FLAG: u64 = 64;

impl DiscordRestClient {
    pub(super) fn replace_application_commands(&self, application_id: u64) -> AppResult<()> {
        let commands = [
            DiscordApplicationCommand {
                kind: DISCORD_CHAT_INPUT_COMMAND,
                name: DISCORD_STATUS_COMMAND,
                description: DISCORD_STATUS_DESCRIPTION,
                options: Vec::new(),
            },
            DiscordApplicationCommand {
                kind: DISCORD_CHAT_INPUT_COMMAND,
                name: DISCORD_MODEL_COMMAND,
                description: DISCORD_MODEL_DESCRIPTION,
                options: vec![DiscordApplicationCommandOption {
                    kind: DISCORD_STRING_OPTION,
                    name: DISCORD_MODEL_OPTION,
                    description: "Model name or default",
                    required: false,
                    choices: Vec::new(),
                }],
            },
            DiscordApplicationCommand {
                kind: DISCORD_CHAT_INPUT_COMMAND,
                name: DISCORD_REASONING_COMMAND,
                description: DISCORD_REASONING_DESCRIPTION,
                options: vec![DiscordApplicationCommandOption {
                    kind: DISCORD_STRING_OPTION,
                    name: DISCORD_REASONING_OPTION,
                    description: "Reasoning effort or default",
                    required: false,
                    choices: reasoning_choices(),
                }],
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
            AppError::Provider("discord command synchronization returned invalid JSON".into())
        })?;
        let expected = [
            DISCORD_STATUS_COMMAND,
            DISCORD_MODEL_COMMAND,
            DISCORD_REASONING_COMMAND,
        ];
        if registered.len() != expected.len()
            || expected.iter().any(|expected| {
                !registered.iter().any(|registered| {
                    registered.kind == DISCORD_CHAT_INPUT_COMMAND && registered.name == *expected
                })
            })
        {
            return Err(AppError::Provider(
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
    pub(super) owner_user_ids: HashSet<u64>,
    pub(super) allowed_channel_ids: HashSet<u64>,
    pub(super) base_model: String,
    pub(super) overrides: Arc<Mutex<HashMap<u64, RunOverrides>>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DiscordCommand {
    Status,
    Model(Option<String>),
    Reasoning(Option<String>),
}

impl DiscordCommandHandler {
    pub(super) fn handle(&self, interaction: InteractionCreateEvent) -> AppResult<()> {
        if interaction.kind != DISCORD_APPLICATION_COMMAND {
            return Ok(());
        }
        let channel_id = parse_snowflake(&interaction.channel_id)?;
        if !self.allowed_channel_ids.contains(&channel_id) {
            return Ok(());
        }
        let application_id = parse_snowflake(&interaction.application_id)?;
        if application_id != self.application_id {
            return Err(AppError::Provider(
                "discord interaction used an unexpected application id".into(),
            ));
        }
        let author_id = interaction
            .member
            .as_ref()
            .map(|member| &member.user)
            .or(interaction.user.as_ref())
            .map(|user| parse_snowflake(&user.id))
            .transpose()?
            .ok_or_else(|| AppError::Provider("discord interaction omitted its author".into()))?;
        if !self.owner_user_ids.contains(&author_id) {
            return Ok(());
        }
        let Some(data) = interaction.data.as_ref() else {
            return Err(AppError::Provider(
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
            return Err(AppError::Provider(
                "discord interaction omitted its token".into(),
            ));
        }
        let handler = self.clone();
        thread::Builder::new()
            .name("discord-command".into())
            .spawn(move || {
                if let Err(error) =
                    handler.respond(interaction_id, &interaction.token, channel_id, command)
                {
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
        command: DiscordCommand,
    ) -> AppResult<()> {
        let agent = ureq::AgentBuilder::new()
            .timeout(PRESENTATION_TIMEOUT)
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
            DiscordCommand::Status => self.status_content(channel_id)?,
            DiscordCommand::Model(value) => self.model_content(channel_id, value)?,
            DiscordCommand::Reasoning(value) => self.reasoning_content(channel_id, value)?,
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

    fn status_content(&self, channel_id: u64) -> AppResult<String> {
        let (model, reasoning) = self.effective_settings(channel_id)?;
        match self.daemon_status() {
            Ok((version, sessions, active_runs)) => Ok(format!(
                "Plato Agent status\nGateway: connected\nDaemon: connected\nDaemon version: {version}\nModel: {model}\nReasoning effort: {reasoning}\nWorkspace sessions: {sessions}\nActive runs: {active_runs}"
            )),
            Err(_) => Ok(format!(
                "Plato Agent status\nGateway: connected\nDaemon: unavailable\nModel: {model}\nReasoning effort: {reasoning}"
            )),
        }
    }

    fn model_content(&self, channel_id: u64, value: Option<String>) -> AppResult<String> {
        if let Some(value) = value {
            let value = value.trim();
            if value.eq_ignore_ascii_case(DISCORD_DEFAULT_SETTING) {
                self.update_overrides(channel_id, |overrides| overrides.model = None)?;
            } else {
                if value.len() > DISCORD_MODEL_LIMIT || value.chars().any(char::is_whitespace) {
                    return Ok(format!(
                        "Model names must be one non-whitespace value of at most {DISCORD_MODEL_LIMIT} bytes."
                    ));
                }
                let model = ModelName::new(value)?.to_string();
                self.update_overrides(channel_id, |overrides| overrides.model = Some(model))?;
            }
        }
        let (model, _) = self.effective_settings(channel_id)?;
        Ok(format!(
            "Model: {model}\nScope: this Discord channel\nApplies to later messages."
        ))
    }

    fn reasoning_content(&self, channel_id: u64, value: Option<String>) -> AppResult<String> {
        if let Some(value) = value {
            if value.eq_ignore_ascii_case(DISCORD_DEFAULT_SETTING) {
                self.update_overrides(channel_id, |overrides| {
                    overrides.reasoning_effort = None;
                })?;
            } else {
                let normalized = value.to_ascii_lowercase();
                let Some(effort) = ReasoningEffort::parse(&normalized) else {
                    return Ok(
                        "Reasoning effort must be default, none, minimal, low, medium, high, xhigh, or max."
                            .into(),
                    );
                };
                self.update_overrides(channel_id, |overrides| {
                    overrides.reasoning_effort = Some(effort);
                })?;
            }
        }
        let (_, reasoning) = self.effective_settings(channel_id)?;
        Ok(format!(
            "Reasoning effort: {reasoning}\nScope: this Discord channel\nApplies to later messages."
        ))
    }

    fn update_overrides(
        &self,
        channel_id: u64,
        update: impl FnOnce(&mut RunOverrides),
    ) -> AppResult<()> {
        let mut settings = self
            .overrides
            .lock()
            .map_err(|_| AppError::Provider("discord run settings lock poisoned".into()))?;
        let overrides = settings.entry(channel_id).or_default();
        update(overrides);
        if overrides.is_empty() {
            settings.remove(&channel_id);
        }
        Ok(())
    }

    fn effective_settings(&self, channel_id: u64) -> AppResult<(String, String)> {
        let settings = self
            .overrides
            .lock()
            .map_err(|_| AppError::Provider("discord run settings lock poisoned".into()))?;
        let overrides = settings.get(&channel_id);
        let model = overrides
            .and_then(|overrides| overrides.model.clone())
            .unwrap_or_else(|| self.base_model.clone());
        let reasoning = overrides
            .and_then(|overrides| overrides.reasoning_effort)
            .map_or_else(|| "provider default".into(), |effort| effort.to_string());
        Ok((model, reasoning))
    }

    fn daemon_status(&self) -> AppResult<(String, usize, usize)> {
        let mut daemon =
            DaemonClient::connect_with_timeout(&self.daemon.socket_path, DAEMON_CLIENT_TIMEOUT)?;
        let hello = daemon.hello(&self.daemon.workspace_root)?;
        require_gateway_daemon_contract(&self.daemon.workspace_root, &hello)?;
        let sessions = daemon.sessions_list()?;
        let active_runs = sessions
            .iter()
            .filter(|session| {
                matches!(
                    session.status,
                    RunStateName::Running | RunStateName::CancelRequested
                )
            })
            .count();
        Ok((hello.daemon_version, sessions.len(), active_runs))
    }
}

fn discord_command(data: &InteractionData) -> AppResult<Option<DiscordCommand>> {
    match data.name.as_str() {
        DISCORD_STATUS_COMMAND => {
            if !data.options.is_empty() {
                return Err(AppError::Provider(
                    "discord status interaction included unexpected options".into(),
                ));
            }
            Ok(Some(DiscordCommand::Status))
        }
        DISCORD_MODEL_COMMAND => optional_string_option(data, DISCORD_MODEL_OPTION)
            .map(|value| Some(DiscordCommand::Model(value))),
        DISCORD_REASONING_COMMAND => optional_string_option(data, DISCORD_REASONING_OPTION)
            .map(|value| Some(DiscordCommand::Reasoning(value))),
        _ => Ok(None),
    }
}

fn optional_string_option(data: &InteractionData, expected: &str) -> AppResult<Option<String>> {
    if data.options.is_empty() {
        return Ok(None);
    }
    if data.options.len() != 1 {
        return Err(AppError::Provider(
            "discord command interaction included unexpected options".into(),
        ));
    }
    let option = &data.options[0];
    if option.kind != DISCORD_STRING_OPTION || option.name != expected {
        return Err(AppError::Provider(
            "discord command interaction included an unexpected option".into(),
        ));
    }
    option
        .value
        .as_str()
        .map(|value| Some(value.to_string()))
        .ok_or_else(|| AppError::Provider("discord command option was not a string".into()))
}

#[derive(Serialize)]
struct DiscordApplicationCommand {
    #[serde(rename = "type")]
    kind: u8,
    name: &'static str,
    description: &'static str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    options: Vec<DiscordApplicationCommandOption>,
}

#[derive(Serialize)]
struct DiscordApplicationCommandOption {
    #[serde(rename = "type")]
    kind: u8,
    name: &'static str,
    description: &'static str,
    required: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    choices: Vec<DiscordApplicationCommandChoice>,
}

#[derive(Serialize)]
pub(super) struct DiscordApplicationCommandChoice {
    name: &'static str,
    value: &'static str,
}

pub(super) fn reasoning_choices() -> Vec<DiscordApplicationCommandChoice> {
    [
        DISCORD_DEFAULT_SETTING,
        "none",
        "minimal",
        "low",
        "medium",
        "high",
        "xhigh",
        "max",
    ]
    .into_iter()
    .map(|value| DiscordApplicationCommandChoice { name: value, value })
    .collect()
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
    pub(super) options: Vec<InteractionOption>,
}

#[derive(Deserialize)]
pub(super) struct InteractionOption {
    #[serde(rename = "type")]
    pub(super) kind: u8,
    pub(super) name: String,
    pub(super) value: Value,
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

pub(super) fn parse_snowflake(value: &str) -> AppResult<u64> {
    value
        .parse()
        .map_err(|_| AppError::Provider("discord gateway returned an invalid snowflake".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discord_gateway::test_support::*;
    #[cfg(unix)]
    use std::os::unix::net::UnixListener;

    #[test]
    fn status_interaction_reports_unavailable_daemon_after_defer() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let rest = spawn_fake_rest(2, 200, None);
        let handler = test_command_handler(
            &rest.base_url,
            &workspace,
            socket_dir.path().join("missing.sock"),
        );

        handler.handle(discord_status_interaction(42)).unwrap();

        let requests = rest.handle.join().unwrap();
        assert_eq!(requests[0].body["type"], DISCORD_DEFERRED_CHANNEL_MESSAGE);
        assert_eq!(
            requests[1].body["content"],
            "Plato Agent status\nGateway: connected\nDaemon: unavailable\nModel: base-model\nReasoning effort: provider default"
        );
    }

    #[test]
    fn non_owner_command_interaction_is_ignored_before_rest_or_daemon_access() {
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
                200,
                DISCORD_MODEL_COMMAND,
                Some((DISCORD_MODEL_OPTION, "openai/gpt-5")),
            ))
            .unwrap();

        assert!(rest.handle.join().unwrap().is_empty());
        assert!(handler.overrides.lock().unwrap().is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn unmapped_owner_interaction_is_ignored_before_scanning_rest_daemon_or_dispatch() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("daemon.sock");
        let daemon = UnixListener::bind(&socket_path).unwrap();
        daemon.set_nonblocking(true).unwrap();
        let rest = spawn_observed_rest(Vec::new());
        let handler = test_command_handler(&rest.base_url, &workspace, socket_path);
        handler.overrides.lock().unwrap().insert(
            201,
            RunOverrides {
                model: Some("unchanged-model".into()),
                reasoning_effort: None,
            },
        );
        let mut interaction = discord_status_interaction(42);
        interaction.channel_id = "201".into();
        interaction.data = None;

        handler.handle(interaction).unwrap();

        assert!(rest.finish().is_empty());
        assert!(matches!(
            daemon.accept(),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
        ));
        assert_eq!(
            handler.overrides.lock().unwrap()[&201].model.as_deref(),
            Some("unchanged-model")
        );
    }

    #[test]
    fn unsupported_and_malformed_commands_do_not_access_daemon_or_settings() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let rest = spawn_fake_rest(0, 200, None);
        let handler = test_command_handler(
            &rest.base_url,
            &workspace,
            socket_dir.path().join("missing.sock"),
        );

        handler
            .handle(discord_command_interaction(42, 200, "unsupported", None))
            .unwrap();
        assert!(
            handler
                .handle(discord_command_interaction(
                    42,
                    200,
                    DISCORD_MODEL_COMMAND,
                    Some(("unexpected", "openai/gpt-5")),
                ))
                .is_err()
        );

        assert!(rest.handle.join().unwrap().is_empty());
        assert!(handler.overrides.lock().unwrap().is_empty());
    }

    #[test]
    fn model_and_reasoning_settings_are_channel_scoped_and_resettable() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let handler = test_command_handler(
            "http://127.0.0.1",
            &workspace,
            socket_dir.path().join("missing.sock"),
        );

        assert_eq!(
            handler
                .model_content(200, Some("openai/gpt-5".into()))
                .unwrap(),
            "Model: openai/gpt-5\nScope: this Discord channel\nApplies to later messages."
        );
        assert_eq!(
            handler
                .reasoning_content(200, Some("XHIGH".into()))
                .unwrap(),
            "Reasoning effort: xhigh\nScope: this Discord channel\nApplies to later messages."
        );
        assert_eq!(
            handler.effective_settings(200).unwrap(),
            ("openai/gpt-5".into(), "xhigh".into())
        );
        assert_eq!(
            handler.effective_settings(201).unwrap(),
            ("base-model".into(), "provider default".into())
        );
        assert!(
            handler
                .status_content(200)
                .unwrap()
                .contains("Model: openai/gpt-5\nReasoning effort: xhigh")
        );

        handler.model_content(200, Some("DEFAULT".into())).unwrap();
        handler
            .reasoning_content(200, Some("default".into()))
            .unwrap();

        assert_eq!(
            handler.effective_settings(200).unwrap(),
            ("base-model".into(), "provider default".into())
        );
        assert!(handler.overrides.lock().unwrap().is_empty());
    }

    #[test]
    fn invalid_run_settings_do_not_mutate_channel_state() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let handler = test_command_handler(
            "http://127.0.0.1",
            &workspace,
            socket_dir.path().join("missing.sock"),
        );

        assert!(
            handler
                .model_content(200, Some("two models".into()))
                .unwrap()
                .starts_with("Model names must be")
        );
        assert!(
            handler
                .reasoning_content(200, Some("turbo".into()))
                .unwrap()
                .starts_with("Reasoning effort must be")
        );
        assert!(handler.overrides.lock().unwrap().is_empty());
    }

    #[test]
    fn owner_model_interaction_is_ephemeral_and_updates_its_channel() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let rest = spawn_fake_rest(2, 200, None);
        let handler = test_command_handler(
            &rest.base_url,
            &workspace,
            socket_dir.path().join("missing.sock"),
        );

        handler
            .handle(discord_command_interaction(
                42,
                200,
                DISCORD_MODEL_COMMAND,
                Some((DISCORD_MODEL_OPTION, "openai/gpt-5-mini")),
            ))
            .unwrap();

        let requests = rest.handle.join().unwrap();
        assert_eq!(requests[0].body["type"], DISCORD_DEFERRED_CHANNEL_MESSAGE);
        assert_eq!(requests[0].body["data"]["flags"], DISCORD_EPHEMERAL_FLAG);
        assert_eq!(
            requests[1].body["content"],
            "Model: openai/gpt-5-mini\nScope: this Discord channel\nApplies to later messages."
        );
        assert_eq!(
            handler.effective_settings(200).unwrap().0,
            "openai/gpt-5-mini"
        );
    }
}
