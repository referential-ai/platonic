use crate::{
    AppError, AppResult,
    config::{Config, DiscordGatewayConfig, ResolvedConfigPath, resolve_config},
    daemon::client::DaemonConnectionConfig,
};
use plato_gateway_discord::DiscordGatewayRuntimeConfig;
use std::{
    collections::HashMap,
    ffi::OsString,
    path::{Path, PathBuf},
    time::Duration,
};

pub struct DiscordGatewayOptions {
    pub workspace_root: PathBuf,
    pub socket_path: Option<PathBuf>,
    pub config_path: Option<PathBuf>,
}

pub fn run_discord_gateway(options: DiscordGatewayOptions) -> AppResult<()> {
    let runtime = resolve_discord_gateway_runtime(options)?;
    plato_gateway_discord::run_discord_gateway(runtime).map_err(Into::into)
}

pub fn preflight_discord_gateway_daemon(
    config: &DaemonConnectionConfig,
    timeout: Duration,
) -> AppResult<()> {
    plato_gateway_discord::preflight_discord_gateway_daemon(config, timeout).map_err(Into::into)
}

fn resolve_discord_gateway_runtime(
    options: DiscordGatewayOptions,
) -> AppResult<DiscordGatewayRuntimeConfig> {
    let resolved = resolve_config(&options.workspace_root, options.config_path.as_deref())?;
    let config = Config::load_resolved(resolved.as_ref())?;
    let discord = config
        .gateway
        .clone()
        .map(|gateway| gateway.discord)
        .ok_or_else(|| AppError::Config("gateway.discord configuration is required".into()))?;
    let (channel_config_paths, mapped_provider_envs) =
        resolve_channel_configs(&options.workspace_root, &discord.channel_configs)?;
    let token = gateway_token(&config, &discord, &mapped_provider_envs, |name| {
        std::env::var_os(name)
    })?;
    let daemon = DaemonConnectionConfig::resolve(&options.workspace_root, options.socket_path)?;
    Ok(DiscordGatewayRuntimeConfig::new(
        token,
        discord.owner_user_ids,
        channel_config_paths,
        config.provider.model,
        daemon,
    ))
}

fn resolve_channel_configs(
    workspace_root: &Path,
    channel_configs: &HashMap<u64, PathBuf>,
) -> AppResult<(HashMap<u64, String>, Vec<String>)> {
    let mut resolved_paths = HashMap::new();
    let mut provider_envs = Vec::new();
    for (channel_id, path) in channel_configs {
        let resolved = resolve_config(workspace_root, Some(path)).map_err(|_| {
            AppError::Config(
                "gateway.discord.channel_configs contains an invalid mapped path".into(),
            )
        })?;
        let Some(resolved) = resolved.as_ref() else {
            return Err(AppError::Config(
                "gateway.discord.channel_configs contains an invalid mapped path".into(),
            ));
        };
        let ResolvedConfigPath::Authorized(resolved_path) = resolved else {
            return Err(AppError::Config(
                "gateway.discord.channel_configs contains an unauthorized mapped path".into(),
            ));
        };
        let mapped_config = Config::load_resolved(Some(resolved))
            .map_err(|error| mapped_config_error(*channel_id, error))?;
        resolved_paths.insert(*channel_id, resolved_path.to_string_lossy().into_owned());
        provider_envs.push(mapped_config.provider.api_key_env);
    }
    provider_envs.sort();
    provider_envs.dedup();
    Ok((resolved_paths, provider_envs))
}

fn mapped_config_error(channel_id: u64, error: AppError) -> AppError {
    let reason = match error {
        AppError::Io(error) if error.kind() == std::io::ErrorKind::NotFound => "does not exist",
        AppError::Io(_) => "could not be read",
        _ => "is invalid",
    };
    AppError::Config(format!(
        "gateway.discord.channel_configs entry for channel {channel_id} {reason}"
    ))
}

fn gateway_token(
    config: &Config,
    discord: &DiscordGatewayConfig,
    mapped_provider_envs: &[String],
    env: impl Fn(&str) -> Option<OsString>,
) -> AppResult<String> {
    let mut provider_envs = vec![
        config.provider.api_key_env.as_str(),
        "OPENAI_API_KEY",
        "OPENROUTER_API_KEY",
    ];
    provider_envs.extend(mapped_provider_envs.iter().map(String::as_str));
    if provider_envs
        .iter()
        .any(|name| *name == discord.api_key_env)
    {
        return Err(AppError::Config(
            "gateway token env var must differ from provider credential env vars".into(),
        ));
    }
    for name in provider_envs {
        if env(name).is_some() {
            return Err(AppError::Config(format!(
                "gateway refuses provider credential env var {name}"
            )));
        }
    }
    let token = env(&discord.api_key_env).ok_or_else(|| {
        AppError::Config(format!(
            "gateway token env var {} is not set",
            discord.api_key_env
        ))
    })?;
    let token = token
        .into_string()
        .map_err(|_| AppError::Config("gateway token is not valid UTF-8".into()))?;
    if token.is_empty() {
        return Err(AppError::Config("gateway token must not be empty".into()));
    }
    Ok(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn discord_config() -> DiscordGatewayConfig {
        DiscordGatewayConfig {
            api_key_env: "DISCORD_BOT_TOKEN".into(),
            owner_user_ids: vec![42],
            channel_configs: HashMap::from([(200, PathBuf::from("mapped.toml"))]),
        }
    }

    #[test]
    fn gateway_environment_rejects_provider_credentials() {
        let config = Config::default();
        let discord = discord_config();

        let error = gateway_token(&config, &discord, &[], |name| match name {
            "DISCORD_BOT_TOKEN" => Some(OsString::from("discord-secret")),
            "OPENROUTER_API_KEY" => Some(OsString::from("provider-secret")),
            _ => None,
        })
        .unwrap_err();

        assert!(error.to_string().contains("OPENROUTER_API_KEY"));
        assert!(!error.to_string().contains("provider-secret"));
        assert!(!error.to_string().contains("discord-secret"));
    }

    #[test]
    fn gateway_environment_rejects_mapped_provider_credentials() {
        let config = Config::default();
        let discord = discord_config();
        let mapped_provider_envs = vec!["CHANNEL_PROVIDER_KEY".into()];

        let error = gateway_token(
            &config,
            &discord,
            &mapped_provider_envs,
            |name| match name {
                "DISCORD_BOT_TOKEN" => Some(OsString::from("discord-secret")),
                "CHANNEL_PROVIDER_KEY" => Some(OsString::from("provider-secret")),
                _ => None,
            },
        )
        .unwrap_err();

        assert!(error.to_string().contains("CHANNEL_PROVIDER_KEY"));
        assert!(!error.to_string().contains("provider-secret"));
        assert!(!error.to_string().contains("discord-secret"));
    }

    #[test]
    fn workspace_gateway_table_fails_before_discord_or_daemon_access() {
        let workspace = tempfile::tempdir().unwrap();
        let mapped_path = workspace.path().join("mapped.toml");
        std::fs::write(
            &mapped_path,
            r#"
[provider]
api_key_env = "CHANNEL_PROVIDER_KEY"
base_url = "https://provider.example/v1"
"#,
        )
        .unwrap();
        std::fs::write(
            workspace.path().join("plato.toml"),
            r#"
[gateway.discord]
api_key_env = "DISCORD_BOT_TOKEN"
owner_user_ids = [42]

[gateway.discord.channel_configs]
"200" = "mapped.toml"
"#,
        )
        .unwrap();

        temp_env::with_var("PLATO_CONFIG", None::<&str>, || {
            let error = match resolve_discord_gateway_runtime(DiscordGatewayOptions {
                workspace_root: workspace.path().to_path_buf(),
                socket_path: None,
                config_path: None,
            }) {
                Ok(_) => panic!("workspace gateway config unexpectedly reached runtime input"),
                Err(error) => error,
            };

            assert!(matches!(
                error,
                AppError::Config(message)
                    if message
                        == "workspace plato.toml cannot set [gateway]; use --config, PLATO_CONFIG, or user config"
            ));
        });
    }

    #[test]
    fn authorized_channel_configs_resolve_relative_paths_and_validate_contents() {
        let workspace = tempfile::tempdir().unwrap();
        let mapped_path = workspace.path().join("configs").join("mapped.toml");
        std::fs::create_dir_all(mapped_path.parent().unwrap()).unwrap();
        std::fs::write(
            &mapped_path,
            r#"
[provider]
api_key_env = "CHANNEL_PROVIDER_KEY"
base_url = "https://provider.example/v1"
"#,
        )
        .unwrap();
        let channel_configs = HashMap::from([(200, PathBuf::from("configs/mapped.toml"))]);

        let (paths, provider_envs) =
            resolve_channel_configs(workspace.path(), &channel_configs).unwrap();

        assert_eq!(Path::new(&paths[&200]), mapped_path.as_path());
        assert_eq!(provider_envs, vec!["CHANNEL_PROVIDER_KEY"]);
    }

    #[test]
    fn authorized_channel_configs_expand_leading_tilde() {
        let workspace = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let mapped_path = home.path().join("mapped.toml");
        std::fs::write(&mapped_path, "").unwrap();
        let channel_configs = HashMap::from([(200, PathBuf::from("~/mapped.toml"))]);

        let home_env = if cfg!(windows) { "USERPROFILE" } else { "HOME" };
        temp_env::with_var(home_env, Some(home.path().as_os_str()), || {
            let (paths, _) = resolve_channel_configs(workspace.path(), &channel_configs).unwrap();

            assert_eq!(paths[&200], mapped_path.to_string_lossy());
        });
    }

    #[test]
    fn missing_and_invalid_channel_configs_return_bounded_config_errors() {
        let workspace = tempfile::tempdir().unwrap();
        let invalid_path = workspace.path().join("invalid.toml");
        std::fs::write(&invalid_path, "[provider\n").unwrap();
        for (name, reason) in [
            ("missing.toml", "does not exist"),
            ("invalid.toml", "is invalid"),
        ] {
            let channel_configs = HashMap::from([(200, PathBuf::from(name))]);
            let error = resolve_channel_configs(workspace.path(), &channel_configs).unwrap_err();
            let AppError::Config(message) = error else {
                panic!("mapped config error was not bounded as a config error");
            };

            assert!(message.contains(reason));
            assert_eq!(
                message,
                format!("gateway.discord.channel_configs entry for channel 200 {reason}")
            );
        }
    }
}
