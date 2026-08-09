use super::{DiscordGatewayRuntimeConfig, run_runtime};
use crate::{
    AppError, AppResult,
    config::{Config, DiscordGatewayConfig, resolve_config, server_discord_principals},
};
use platonic_client::{client::DaemonConnectionConfig, paths};
use std::{ffi::OsString, path::PathBuf};

/// Server-owned inputs for one Discord gateway process.
pub struct DiscordGatewayOptions {
    /// Workspace served by the gateway.
    pub workspace_root: PathBuf,
    /// Optional host endpoint override for testing or operations.
    pub socket_path: Option<PathBuf>,
    /// Optional authorized server configuration path.
    pub config_path: Option<PathBuf>,
}

/// Resolves server configuration and runs the Discord gateway.
pub fn run_discord_gateway(options: DiscordGatewayOptions) -> AppResult<()> {
    let runtime = resolve_discord_gateway_runtime(options)?;
    run_runtime(runtime).map_err(Into::into)
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
    let principals = server_discord_principals()?;
    let token = gateway_token(&config, &discord, |name| std::env::var_os(name))?;
    let socket_path = match options.socket_path {
        Some(socket_path) => socket_path,
        None => paths::host_socket_path()?,
    };
    let daemon = DaemonConnectionConfig::resolve(&options.workspace_root, Some(socket_path))?;
    Ok(DiscordGatewayRuntimeConfig::new(
        token,
        principals,
        discord.channel_threads,
        daemon,
    ))
}

fn gateway_token(
    config: &Config,
    discord: &DiscordGatewayConfig,
    env: impl Fn(&str) -> Option<OsString>,
) -> AppResult<String> {
    let provider_envs = vec![
        config.provider.api_key_env.as_str(),
        "OPENAI_API_KEY",
        "OPENROUTER_API_KEY",
    ];
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
    #[cfg(unix)]
    use crate::{
        daemon::server::HostDaemonServer,
        gateway::discord::{GatewayError, test_support::spawn_observed_rest},
    };
    #[cfg(unix)]
    use platonic_client::ClientError;
    #[cfg(unix)]
    use platonic_protocol::{ERROR_WORKSPACE_UNREGISTERED, ProtocolError};
    use std::collections::HashMap;
    #[cfg(unix)]
    use std::{sync::Arc, thread};

    fn discord_config() -> DiscordGatewayConfig {
        DiscordGatewayConfig {
            api_key_env: "DISCORD_BOT_TOKEN".into(),
            channel_threads: HashMap::from([(200, "thread_news".into())]),
        }
    }

    #[test]
    fn gateway_environment_rejects_provider_credentials() {
        let config = Config::default();
        let discord = discord_config();

        let error = gateway_token(&config, &discord, |name| match name {
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
    fn gateway_environment_rejects_missing_and_empty_tokens() {
        let config = Config::default();
        let discord = discord_config();

        let missing = gateway_token(&config, &discord, |_| None).unwrap_err();
        let empty = gateway_token(&config, &discord, |name| {
            (name == "DISCORD_BOT_TOKEN").then(OsString::new)
        })
        .unwrap_err();

        assert_eq!(
            missing.to_string(),
            "config error: gateway token env var DISCORD_BOT_TOKEN is not set"
        );
        assert_eq!(
            empty.to_string(),
            "config error: gateway token must not be empty"
        );
    }

    #[test]
    fn workspace_gateway_table_fails_before_discord_or_daemon_access() {
        let workspace = tempfile::tempdir().unwrap();
        std::fs::write(
            workspace.path().join("plato.toml"),
            r#"
[gateway.discord]
api_key_env = "DISCORD_BOT_TOKEN"

[gateway.discord.channel_threads]
"200" = "thread_news"
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
    #[cfg(unix)]
    fn default_host_endpoint_rejects_unregistered_workspace_before_discord_access() {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("unregistered");
        let home = root.path().join("home");
        let explicit = root.path().join("gateway.toml");
        let home_config = home.join(".config/plato/config.toml");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(home_config.parent().unwrap()).unwrap();
        std::fs::write(
            &explicit,
            r#"
[gateway.discord]
api_key_env = "DISCORD_BOT_TOKEN"

[gateway.discord.channel_threads]
"200" = "thread_news"
"#,
        )
        .unwrap();
        std::fs::write(
            &home_config,
            r#"
[principals.discord."42"]
name = "jerome"
"#,
        )
        .unwrap();

        crate::paths::with_test_xdg(root.path(), || {
            temp_env::with_vars(
                [
                    ("HOME", Some(home.as_os_str())),
                    ("PLATO_CONFIG", None::<&std::ffi::OsStr>),
                    ("OPENAI_API_KEY", None::<&std::ffi::OsStr>),
                    ("OPENROUTER_API_KEY", None::<&std::ffi::OsStr>),
                    (
                        "DISCORD_BOT_TOKEN",
                        Some(std::ffi::OsStr::new("discord-secret")),
                    ),
                ],
                || {
                    let server = Arc::new(HostDaemonServer::bind().unwrap());
                    let expected_socket = paths::host_socket_path().unwrap();
                    let legacy_socket = paths::default_socket_path(&workspace).unwrap();
                    let runner = Arc::clone(&server);
                    let daemon = thread::spawn(move || runner.serve_next().unwrap());
                    let rest = spawn_observed_rest(Vec::new());
                    let mut runtime = resolve_discord_gateway_runtime(DiscordGatewayOptions {
                        workspace_root: workspace.clone(),
                        socket_path: None,
                        config_path: Some(explicit.clone()),
                    })
                    .unwrap();

                    assert_eq!(runtime.daemon.socket_path, expected_socket);
                    assert_ne!(runtime.daemon.socket_path, legacy_socket);
                    runtime.discord_api_base = rest.base_url.clone();

                    let error = run_runtime(runtime).unwrap_err();

                    daemon.join().unwrap();
                    assert!(matches!(
                        error,
                        GatewayError::Client(ClientError::DaemonResponse(ProtocolError {
                            code,
                            ref message,
                        })) if code == ERROR_WORKSPACE_UNREGISTERED
                            && message.contains("platonic workspace create")
                    ));
                    assert!(rest.finish().is_empty());
                },
            );
        });
    }

    #[test]
    #[cfg(unix)]
    fn explicit_and_environment_configs_cannot_supply_gateway_principal_authority() {
        let workspace = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let explicit = workspace.path().join("gateway.toml");
        std::fs::write(
            &explicit,
            r#"
[gateway.discord]
api_key_env = "DISCORD_BOT_TOKEN"

[gateway.discord.channel_threads]
"200" = "thread_news"

[principals.discord."42"]
name = "workspace_attacker"
remote_ceiling = "yolo"
"#,
        )
        .unwrap();

        let home_env = if cfg!(windows) { "USERPROFILE" } else { "HOME" };
        for through_environment in [false, true] {
            temp_env::with_vars(
                [
                    (home_env, Some(home.path().as_os_str())),
                    ("DISCORD_BOT_TOKEN", Some(std::ffi::OsStr::new("secret"))),
                    (
                        "PLATO_CONFIG",
                        through_environment.then_some(explicit.as_os_str()),
                    ),
                ],
                || {
                    let error = match resolve_discord_gateway_runtime(DiscordGatewayOptions {
                        workspace_root: workspace.path().to_path_buf(),
                        socket_path: Some(workspace.path().join("never-contact.sock")),
                        config_path: (!through_environment).then(|| explicit.clone()),
                    }) {
                        Ok(_) => {
                            panic!("non-home config unexpectedly supplied principal authority")
                        }
                        Err(error) => error,
                    };

                    assert_eq!(
                        error.to_string(),
                        "config error: Discord gateway principals require [principals.discord] in the user config"
                    );
                },
            );
        }
    }

    #[test]
    #[cfg(unix)]
    fn runtime_uses_only_canonical_home_principals() {
        let workspace = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let explicit = workspace.path().join("gateway.toml");
        let home_config = home.path().join(".config/plato/config.toml");
        std::fs::create_dir_all(home_config.parent().unwrap()).unwrap();
        std::fs::write(
            &explicit,
            r#"
[gateway.discord]
api_key_env = "DISCORD_BOT_TOKEN"

[gateway.discord.channel_threads]
"200" = "thread_news"

[principals.discord."42"]
name = "ignored_explicit_actor"
remote_ceiling = "yolo"
"#,
        )
        .unwrap();
        std::fs::write(
            &home_config,
            r#"
[principals.discord."42"]
name = "jerome"
"#,
        )
        .unwrap();

        temp_env::with_vars(
            [
                ("HOME", Some(home.path().as_os_str())),
                ("DISCORD_BOT_TOKEN", Some(std::ffi::OsStr::new("secret"))),
            ],
            || {
                let runtime = resolve_discord_gateway_runtime(DiscordGatewayOptions {
                    workspace_root: workspace.path().to_path_buf(),
                    socket_path: Some(workspace.path().join("daemon.sock")),
                    config_path: Some(explicit.clone()),
                })
                .unwrap();

                assert_eq!(runtime.principals[&42].name, "jerome");
                assert_eq!(
                    runtime.principals[&42].remote_ceiling,
                    platonic_protocol::ThreadApprovalPolicy::Prompt
                );
                assert_eq!(runtime.channel_thread_ids[&200], "thread_news");
                assert_eq!(
                    runtime.daemon.socket_path,
                    workspace.path().join("daemon.sock")
                );
            },
        );
    }
}
