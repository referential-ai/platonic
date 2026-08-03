use crate::{
    AppError, AppResult,
    tool_catalog::{default_enabled_tools, is_known_tool},
};
use serde::Deserialize;
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

const DEFAULT_OPENAI_MODEL: &str = "gpt-5.5";
const DEFAULT_OPENROUTER_MODEL: &str = "~openai/gpt-latest";
const DEFAULT_TOKEN_BUDGET: u32 = 4_000;
const DEFAULT_MAX_OUTPUT_TOKENS: u32 = 1_024;
const DEFAULT_MAX_TURNS: u32 = 8;
const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_STREAM_IDLE_TIMEOUT_MS: u64 = 120_000;
const OPENAI_BASE_URL: &str = "https://api.openai.com/v1";
const OPENROUTER_BASE_URL: &str = "https://openrouter.ai/api/v1";
const PLATO_CONFIG_ENV: &str = "PLATO_CONFIG";
const WORKSPACE_PROVIDER_OVERRIDE_ERROR: &str = "workspace plato.toml cannot set provider.api_key_env or provider.base_url; use --config, PLATO_CONFIG, or user config";
const WORKSPACE_GATEWAY_ERROR: &str =
    "workspace plato.toml cannot set [gateway]; use --config, PLATO_CONFIG, or user config";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ResolvedConfigPath {
    Authorized(PathBuf),
    Workspace(PathBuf),
}

impl ResolvedConfigPath {
    fn path(&self) -> &Path {
        match self {
            Self::Authorized(path) | Self::Workspace(path) => path,
        }
    }

    fn into_path(self) -> PathBuf {
        match self {
            Self::Authorized(path) | Self::Workspace(path) => path,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Config {
    pub provider: ProviderConfig,
    pub limits: LimitsConfig,
    pub tools: ToolsConfig,
    pub gateway: Option<GatewayConfig>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatewayConfig {
    pub discord: DiscordGatewayConfig,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscordGatewayConfig {
    pub api_key_env: String,
    pub owner_user_ids: Vec<u64>,
    pub channel_configs: HashMap<u64, PathBuf>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderConfig {
    pub kind: ProviderKind,
    pub model: String,
    pub api_key_env: String,
    pub base_url: String,
    pub connect_timeout_ms: u64,
    pub stream_idle_timeout_ms: u64,
    pub http_referer: Option<String>,
    pub app_title: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    OpenAi,
    OpenRouter,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LimitsConfig {
    pub token_budget: u32,
    pub max_output_tokens: u32,
    pub max_turns: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolsConfig {
    pub enabled: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    provider: Option<RawProviderConfig>,
    limits: Option<RawLimitsConfig>,
    tools: Option<RawToolsConfig>,
    gateway: Option<RawGatewayConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawGatewayConfig {
    discord: RawDiscordGatewayConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDiscordGatewayConfig {
    api_key_env: String,
    owner_user_ids: Vec<u64>,
    #[serde(default)]
    channel_configs: HashMap<String, PathBuf>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawProviderConfig {
    kind: Option<ProviderKind>,
    model: Option<String>,
    api_key_env: Option<String>,
    base_url: Option<String>,
    connect_timeout_ms: Option<u64>,
    stream_idle_timeout_ms: Option<u64>,
    timeout_ms: Option<u64>,
    http_referer: Option<String>,
    app_title: Option<String>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawLimitsConfig {
    token_budget: Option<u32>,
    max_output_tokens: Option<u32>,
    max_turns: Option<u32>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawToolsConfig {
    enabled: Option<Vec<String>>,
}

impl Config {
    pub fn load(workspace_root: &Path, explicit_path: Option<&Path>) -> AppResult<Self> {
        let resolved = resolve_config(workspace_root, explicit_path)?;
        Self::load_resolved(resolved.as_ref())
    }

    pub(crate) fn load_resolved(resolved: Option<&ResolvedConfigPath>) -> AppResult<Self> {
        let Some(resolved) = resolved else {
            return Ok(Self::default());
        };
        let raw = Self::read_raw(resolved.path())?;
        if matches!(resolved, ResolvedConfigPath::Workspace(_)) && raw.gateway.is_some() {
            return Err(AppError::Config(WORKSPACE_GATEWAY_ERROR.into()));
        }
        if matches!(resolved, ResolvedConfigPath::Workspace(_))
            && raw.provider.as_ref().is_some_and(|provider| {
                provider.api_key_env.is_some() || provider.base_url.is_some()
            })
        {
            return Err(AppError::Config(WORKSPACE_PROVIDER_OVERRIDE_ERROR.into()));
        }
        Self::from_raw(raw)
    }

    fn read_raw(path: &Path) -> AppResult<RawConfig> {
        let raw = fs::read_to_string(path)?;
        Ok(toml::from_str(&raw)?)
    }

    fn from_raw(raw: RawConfig) -> AppResult<Self> {
        let provider = raw.provider.unwrap_or_default();
        let limits = raw.limits.unwrap_or_default();
        let tools = raw.tools.unwrap_or_default();
        let gateway = raw.gateway.map(GatewayConfig::from_raw).transpose()?;
        let token_budget = positive(
            limits.token_budget.unwrap_or(DEFAULT_TOKEN_BUDGET),
            "limits.token_budget",
        )?;
        let max_output_tokens = positive(
            limits
                .max_output_tokens
                .unwrap_or(DEFAULT_MAX_OUTPUT_TOKENS),
            "limits.max_output_tokens",
        )?;
        let max_turns = positive(
            limits.max_turns.unwrap_or(DEFAULT_MAX_TURNS),
            "limits.max_turns",
        )?;
        let connect_timeout_ms = positive(
            provider
                .connect_timeout_ms
                .unwrap_or(DEFAULT_CONNECT_TIMEOUT_MS),
            "provider.connect_timeout_ms",
        )?;
        let stream_idle_timeout_ms = match (provider.stream_idle_timeout_ms, provider.timeout_ms) {
            (Some(_), Some(_)) => {
                return Err(AppError::Config(
                    "provider.timeout_ms and provider.stream_idle_timeout_ms cannot both be set"
                        .into(),
                ));
            }
            (Some(value), None) => positive(value, "provider.stream_idle_timeout_ms")?,
            (None, Some(value)) => positive(value, "provider.timeout_ms")?,
            (None, None) => DEFAULT_STREAM_IDLE_TIMEOUT_MS,
        };
        let kind = provider.kind.unwrap_or(ProviderKind::OpenRouter);

        let enabled = tools.enabled.unwrap_or_else(default_enabled_tools);
        if enabled.is_empty() {
            return Err(AppError::Config("tools.enabled must not be empty".into()));
        }
        if let Some(tool) = enabled.iter().find(|tool| !is_known_tool(tool)) {
            return Err(AppError::Config(format!(
                "unknown tool in tools.enabled: {tool}"
            )));
        }

        Ok(Self {
            provider: ProviderConfig {
                model: provider
                    .model
                    .unwrap_or_else(|| default_model(&kind).into()),
                api_key_env: provider
                    .api_key_env
                    .unwrap_or_else(|| default_api_key_env(&kind).into()),
                base_url: provider
                    .base_url
                    .unwrap_or_else(|| default_base_url(&kind).into()),
                connect_timeout_ms,
                stream_idle_timeout_ms,
                http_referer: provider.http_referer,
                app_title: provider.app_title,
                kind,
            },
            limits: LimitsConfig {
                token_budget,
                max_output_tokens,
                max_turns,
            },
            tools: ToolsConfig { enabled },
            gateway,
        })
    }
}

impl Default for Config {
    fn default() -> Self {
        let kind = ProviderKind::OpenRouter;
        Self {
            provider: ProviderConfig {
                model: default_model(&kind).into(),
                api_key_env: default_api_key_env(&kind).into(),
                base_url: default_base_url(&kind).into(),
                connect_timeout_ms: DEFAULT_CONNECT_TIMEOUT_MS,
                stream_idle_timeout_ms: DEFAULT_STREAM_IDLE_TIMEOUT_MS,
                http_referer: None,
                app_title: None,
                kind,
            },
            limits: LimitsConfig {
                token_budget: DEFAULT_TOKEN_BUDGET,
                max_output_tokens: DEFAULT_MAX_OUTPUT_TOKENS,
                max_turns: DEFAULT_MAX_TURNS,
            },
            tools: ToolsConfig {
                enabled: default_enabled_tools(),
            },
            gateway: None,
        }
    }
}

fn positive<T: From<u8> + PartialEq>(value: T, field: &str) -> AppResult<T> {
    if value == T::from(0) {
        return Err(AppError::Config(format!("{field} must be positive")));
    }
    Ok(value)
}

impl GatewayConfig {
    fn from_raw(raw: RawGatewayConfig) -> AppResult<Self> {
        if raw.discord.api_key_env.trim().is_empty() {
            return Err(AppError::Config(
                "gateway.discord.api_key_env must not be empty".into(),
            ));
        }
        if raw.discord.owner_user_ids.is_empty() {
            return Err(AppError::Config(
                "gateway.discord.owner_user_ids must not be empty".into(),
            ));
        }
        if raw.discord.owner_user_ids.contains(&0) {
            return Err(AppError::Config(
                "gateway.discord.owner_user_ids must contain positive integers".into(),
            ));
        }
        if raw.discord.channel_configs.is_empty() {
            return Err(AppError::Config(
                "gateway.discord.channel_configs must not be empty".into(),
            ));
        }
        let mut channel_configs = HashMap::new();
        for (channel_id, path) in raw.discord.channel_configs {
            if !channel_id.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(AppError::Config(
                    "gateway.discord.channel_configs keys must be positive numeric Discord channel IDs"
                        .into(),
                ));
            }
            let channel_id = channel_id.parse::<u64>().map_err(|_| {
                AppError::Config(
                    "gateway.discord.channel_configs keys must be positive numeric Discord channel IDs"
                        .into(),
                )
            })?;
            if channel_id == 0 {
                return Err(AppError::Config(
                    "gateway.discord.channel_configs keys must be positive numeric Discord channel IDs"
                        .into(),
                ));
            }
            if channel_configs.insert(channel_id, path).is_some() {
                return Err(AppError::Config(
                    "gateway.discord.channel_configs contains a duplicate numeric Discord channel ID"
                        .into(),
                ));
            }
        }
        Ok(Self {
            discord: DiscordGatewayConfig {
                api_key_env: raw.discord.api_key_env,
                owner_user_ids: raw.discord.owner_user_ids,
                channel_configs,
            },
        })
    }
}

fn default_model(kind: &ProviderKind) -> &'static str {
    match kind {
        ProviderKind::OpenAi => DEFAULT_OPENAI_MODEL,
        ProviderKind::OpenRouter => DEFAULT_OPENROUTER_MODEL,
    }
}

fn default_api_key_env(kind: &ProviderKind) -> &'static str {
    match kind {
        ProviderKind::OpenAi => "OPENAI_API_KEY",
        ProviderKind::OpenRouter => "OPENROUTER_API_KEY",
    }
}

fn default_base_url(kind: &ProviderKind) -> &'static str {
    match kind {
        ProviderKind::OpenAi => OPENAI_BASE_URL,
        ProviderKind::OpenRouter => OPENROUTER_BASE_URL,
    }
}

pub fn resolve_config_path(
    workspace_root: &Path,
    explicit_path: Option<&Path>,
) -> AppResult<Option<PathBuf>> {
    Ok(resolve_config(workspace_root, explicit_path)?.map(ResolvedConfigPath::into_path))
}

pub(crate) fn resolve_config(
    workspace_root: &Path,
    explicit_path: Option<&Path>,
) -> AppResult<Option<ResolvedConfigPath>> {
    let home = user_home();
    resolve_config_with(
        workspace_root,
        explicit_path.map(Path::to_path_buf),
        std::env::var_os(PLATO_CONFIG_ENV).map(PathBuf::from),
        home.clone(),
        user_config_path(home.as_deref()),
    )
}

#[cfg(test)]
fn resolve_config_path_with(
    workspace_root: &Path,
    explicit_path: Option<PathBuf>,
    env_path: Option<PathBuf>,
    home: Option<PathBuf>,
    user_config: Option<PathBuf>,
) -> AppResult<Option<PathBuf>> {
    Ok(
        resolve_config_with(workspace_root, explicit_path, env_path, home, user_config)?
            .map(ResolvedConfigPath::into_path),
    )
}

fn resolve_config_with(
    workspace_root: &Path,
    explicit_path: Option<PathBuf>,
    env_path: Option<PathBuf>,
    home: Option<PathBuf>,
    user_config: Option<PathBuf>,
) -> AppResult<Option<ResolvedConfigPath>> {
    if let Some(path) = explicit_path {
        return resolve_explicit_config_path(workspace_root, path, home.as_deref())
            .map(|path| Some(ResolvedConfigPath::Authorized(path)));
    }
    if let Some(path) = env_path {
        return resolve_explicit_config_path(workspace_root, path, home.as_deref())
            .map(|path| Some(ResolvedConfigPath::Authorized(path)));
    }

    let workspace_config = workspace_root.join("plato.toml");
    if workspace_config.exists() {
        return Ok(Some(ResolvedConfigPath::Workspace(workspace_config)));
    }

    if let Some(user_config) = user_config
        && user_config.exists()
    {
        return Ok(Some(ResolvedConfigPath::Authorized(user_config)));
    }

    Ok(None)
}

#[cfg(unix)]
fn user_home() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

#[cfg(windows)]
fn user_home() -> Option<PathBuf> {
    std::env::var_os("USERPROFILE")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

#[cfg(unix)]
fn user_config_path(home: Option<&Path>) -> Option<PathBuf> {
    home.map(|home| home.join(".config").join("plato").join("config.toml"))
}

#[cfg(windows)]
fn user_config_path(_home: Option<&Path>) -> Option<PathBuf> {
    std::env::var_os("APPDATA")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .map(|root| root.join("plato").join("config.toml"))
}

fn resolve_explicit_config_path(
    workspace_root: &Path,
    path: PathBuf,
    home: Option<&Path>,
) -> AppResult<PathBuf> {
    let path = expand_leading_tilde(path, home)?;
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(workspace_root.join(path))
    }
}

fn expand_leading_tilde(path: PathBuf, home: Option<&Path>) -> AppResult<PathBuf> {
    let Some(raw) = path.to_str() else {
        return Ok(path);
    };
    if raw == "~" {
        return home
            .map(Path::to_path_buf)
            .ok_or_else(|| AppError::Config("user home is required for ~ expansion".into()));
    }
    if let Some(rest) = leading_tilde_rest(raw) {
        let home =
            home.ok_or_else(|| AppError::Config("user home is required for ~ expansion".into()))?;
        return Ok(home.join(rest));
    }
    Ok(path)
}

#[cfg(unix)]
fn leading_tilde_rest(path: &str) -> Option<&str> {
    path.strip_prefix("~/")
}

#[cfg(windows)]
fn leading_tilde_rest(path: &str) -> Option<&str> {
    path.strip_prefix("~/").or(path.strip_prefix(r"~\"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_has_the_bootstrap_tools() {
        let config = Config::default();

        assert_eq!(config.provider.kind, ProviderKind::OpenRouter);
        assert_eq!(config.provider.model, "~openai/gpt-latest");
        assert_eq!(config.provider.api_key_env, "OPENROUTER_API_KEY");
        assert_eq!(config.provider.base_url, "https://openrouter.ai/api/v1");
        assert_eq!(
            config.provider.connect_timeout_ms,
            DEFAULT_CONNECT_TIMEOUT_MS
        );
        assert_eq!(
            config.provider.stream_idle_timeout_ms,
            DEFAULT_STREAM_IDLE_TIMEOUT_MS
        );
        assert_eq!(config.limits.max_turns, 8);
        assert!(config.gateway.is_none());
        assert_eq!(
            config.tools.enabled,
            vec![
                "file.read",
                "file.list",
                "file.write",
                "file.edit",
                "shell.exec",
                "web.fetch"
            ]
        );
    }

    #[test]
    fn parses_discord_gateway_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("plato.toml");
        std::fs::write(
            &path,
            r#"
[gateway.discord]
api_key_env = "DISCORD_BOT_TOKEN"
owner_user_ids = [123456789]

[gateway.discord.channel_configs]
"111111111111111111" = "~/.config/plato/channels/news.toml"
"#,
        )
        .unwrap();

        let resolved = ResolvedConfigPath::Authorized(path);
        let config = Config::load_resolved(Some(&resolved)).unwrap();
        let discord = config.gateway.unwrap().discord;

        assert_eq!(discord.api_key_env, "DISCORD_BOT_TOKEN");
        assert_eq!(discord.owner_user_ids, vec![123456789]);
        assert_eq!(
            discord.channel_configs,
            HashMap::from([(
                111111111111111111,
                PathBuf::from("~/.config/plato/channels/news.toml")
            )])
        );
    }

    #[test]
    fn rejects_empty_discord_channel_configs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gateway.toml");
        std::fs::write(
            &path,
            r#"
[gateway.discord]
api_key_env = "DISCORD_BOT_TOKEN"
owner_user_ids = [123456789]
"#,
        )
        .unwrap();

        let resolved = ResolvedConfigPath::Authorized(path);
        let error = Config::load_resolved(Some(&resolved)).unwrap_err();

        assert!(matches!(
            error,
            AppError::Config(message)
                if message == "gateway.discord.channel_configs must not be empty"
        ));
    }

    #[test]
    fn rejects_zero_and_nonnumeric_discord_channel_config_keys() {
        for channel_id in ["0", "+1", "not-a-channel"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("plato.toml");
            std::fs::write(
                &path,
                format!(
                    r#"
[gateway.discord]
api_key_env = "DISCORD_BOT_TOKEN"
owner_user_ids = [123456789]

[gateway.discord.channel_configs]
"{channel_id}" = "channel.toml"
"#
                ),
            )
            .unwrap();

            let resolved = ResolvedConfigPath::Authorized(path);
            let error = Config::load_resolved(Some(&resolved)).unwrap_err();

            assert!(matches!(
                error,
                AppError::Config(message)
                    if message
                        == "gateway.discord.channel_configs keys must be positive numeric Discord channel IDs"
            ));
        }
    }

    #[test]
    fn rejects_duplicate_discord_channel_config_keys() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("plato.toml");
        std::fs::write(
            &path,
            r#"
[gateway.discord]
api_key_env = "DISCORD_BOT_TOKEN"
owner_user_ids = [123456789]

[gateway.discord.channel_configs]
"111111111111111111" = "news.toml"
"111111111111111111" = "dev.toml"
"#,
        )
        .unwrap();

        let resolved = ResolvedConfigPath::Authorized(path);
        let error = Config::load_resolved(Some(&resolved)).unwrap_err();

        assert!(matches!(error, AppError::Toml(_)));
    }

    #[test]
    fn rejects_discord_channel_config_keys_with_duplicate_numeric_values() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("plato.toml");
        std::fs::write(
            &path,
            r#"
[gateway.discord]
api_key_env = "DISCORD_BOT_TOKEN"
owner_user_ids = [123456789]

[gateway.discord.channel_configs]
"1" = "news.toml"
"01" = "dev.toml"
"#,
        )
        .unwrap();

        let resolved = ResolvedConfigPath::Authorized(path);
        let error = Config::load_resolved(Some(&resolved)).unwrap_err();

        assert!(matches!(
            error,
            AppError::Config(message)
                if message
                    == "gateway.discord.channel_configs contains a duplicate numeric Discord channel ID"
        ));
    }

    #[test]
    fn auto_workspace_config_rejects_sensitive_provider_fields() {
        for field in [
            r#"api_key_env = "STOLEN_SECRET""#,
            r#"base_url = "https://attacker.invalid/v1""#,
        ] {
            let workspace = tempfile::tempdir().unwrap();
            std::fs::write(
                workspace.path().join("plato.toml"),
                format!("[provider]\n{field}\n"),
            )
            .unwrap();
            let resolved = resolve_config_with(workspace.path(), None, None, None, None)
                .unwrap()
                .unwrap();

            assert!(matches!(&resolved, ResolvedConfigPath::Workspace(_)));
            let error = Config::load_resolved(Some(&resolved)).unwrap_err();
            assert!(matches!(
                error,
                AppError::Config(message) if message == WORKSPACE_PROVIDER_OVERRIDE_ERROR
            ));
        }
    }

    #[test]
    fn auto_workspace_config_rejects_the_gateway_table() {
        let workspace = tempfile::tempdir().unwrap();
        std::fs::write(
            workspace.path().join("plato.toml"),
            r#"
[gateway.discord]
api_key_env = "DISCORD_BOT_TOKEN"
owner_user_ids = [42]

[gateway.discord.channel_configs]
"200" = "channel.toml"
"#,
        )
        .unwrap();
        let resolved = resolve_config_with(workspace.path(), None, None, None, None)
            .unwrap()
            .unwrap();

        assert!(matches!(&resolved, ResolvedConfigPath::Workspace(_)));
        let error = Config::load_resolved(Some(&resolved)).unwrap_err();
        assert!(matches!(
            error,
            AppError::Config(message) if message == WORKSPACE_GATEWAY_ERROR
        ));
    }

    #[test]
    fn auto_workspace_config_allows_other_fields() {
        let workspace = tempfile::tempdir().unwrap();
        std::fs::write(
            workspace.path().join("plato.toml"),
            r#"
[provider]
kind = "open_ai"
model = "gpt-test"
timeout_ms = 3000

[limits]
max_turns = 2

[tools]
enabled = ["file.read"]
"#,
        )
        .unwrap();
        let resolved = resolve_config_with(workspace.path(), None, None, None, None)
            .unwrap()
            .unwrap();

        let config = Config::load_resolved(Some(&resolved)).unwrap();

        assert_eq!(config.provider.kind, ProviderKind::OpenAi);
        assert_eq!(config.provider.model, "gpt-test");
        assert_eq!(config.provider.api_key_env, "OPENAI_API_KEY");
        assert_eq!(config.provider.base_url, OPENAI_BASE_URL);
        assert_eq!(
            config.provider.connect_timeout_ms,
            DEFAULT_CONNECT_TIMEOUT_MS
        );
        assert_eq!(config.provider.stream_idle_timeout_ms, 3000);
        assert_eq!(config.limits.max_turns, 2);
        assert_eq!(config.tools.enabled, vec!["file.read"]);
        assert!(config.gateway.is_none());
    }

    #[test]
    fn explicit_environment_and_user_configs_allow_trusted_fields() {
        for source in ["explicit", "environment", "user"] {
            let workspace = tempfile::tempdir().unwrap();
            let name = if source == "explicit" {
                "plato.toml".into()
            } else {
                format!("{source}.toml")
            };
            let path = workspace.path().join(name);
            std::fs::write(
                &path,
                r#"
[provider]
api_key_env = "AUTHORIZED_SECRET"
base_url = "https://provider.example/v1"

[gateway.discord]
api_key_env = "DISCORD_BOT_TOKEN"
owner_user_ids = [42]

[gateway.discord.channel_configs]
"200" = "channel.toml"
"#,
            )
            .unwrap();
            let resolved = match source {
                "explicit" => resolve_config_with(
                    workspace.path(),
                    Some(PathBuf::from("plato.toml")),
                    None,
                    None,
                    None,
                ),
                "environment" => {
                    resolve_config_with(workspace.path(), None, Some(path.clone()), None, None)
                }
                "user" => {
                    resolve_config_with(workspace.path(), None, None, None, Some(path.clone()))
                }
                _ => unreachable!(),
            };
            let resolved = resolved.unwrap().unwrap();

            let config = Config::load_resolved(Some(&resolved)).unwrap();

            assert!(matches!(
                &resolved,
                ResolvedConfigPath::Authorized(resolved_path) if resolved_path == &path
            ));
            assert_eq!(config.provider.api_key_env, "AUTHORIZED_SECRET");
            assert_eq!(config.provider.base_url, "https://provider.example/v1");
            let discord = config.gateway.unwrap().discord;
            assert_eq!(discord.owner_user_ids, vec![42]);
            assert_eq!(
                discord.channel_configs,
                HashMap::from([(200, PathBuf::from("channel.toml"))])
            );
        }
    }

    #[test]
    fn rejects_zero_token_budget() {
        let raw = RawConfig {
            provider: None,
            limits: Some(RawLimitsConfig {
                token_budget: Some(0),
                max_output_tokens: None,
                max_turns: None,
            }),
            tools: None,
            gateway: None,
        };

        assert!(matches!(Config::from_raw(raw), Err(AppError::Config(_))));
    }

    #[test]
    fn rejects_zero_max_output_tokens() {
        let raw = RawConfig {
            provider: None,
            limits: Some(RawLimitsConfig {
                token_budget: None,
                max_output_tokens: Some(0),
                max_turns: None,
            }),
            tools: None,
            gateway: None,
        };

        assert!(matches!(Config::from_raw(raw), Err(AppError::Config(_))));
    }

    #[test]
    fn rejects_unknown_enabled_tools() {
        let raw = RawConfig {
            provider: None,
            limits: None,
            tools: Some(RawToolsConfig {
                enabled: Some(vec!["shell.delete".into()]),
            }),
            gateway: None,
        };

        let err = Config::from_raw(raw).unwrap_err();

        assert!(matches!(
            err,
            AppError::Config(message) if message == "unknown tool in tools.enabled: shell.delete"
        ));
    }

    #[test]
    fn rejects_zero_max_turns() {
        let raw = RawConfig {
            provider: None,
            limits: Some(RawLimitsConfig {
                token_budget: None,
                max_output_tokens: None,
                max_turns: Some(0),
            }),
            tools: None,
            gateway: None,
        };

        assert!(matches!(Config::from_raw(raw), Err(AppError::Config(_))));
    }

    #[test]
    fn parses_configured_max_turns() {
        let raw = RawConfig {
            provider: None,
            limits: Some(RawLimitsConfig {
                token_budget: None,
                max_output_tokens: None,
                max_turns: Some(3),
            }),
            tools: None,
            gateway: None,
        };

        assert_eq!(Config::from_raw(raw).unwrap().limits.max_turns, 3);
    }

    #[test]
    fn parses_explicit_provider_timeouts() {
        let raw = toml::from_str(
            r#"
[provider]
connect_timeout_ms = 2500
stream_idle_timeout_ms = 9000
"#,
        )
        .unwrap();

        let config = Config::from_raw(raw).unwrap();

        assert_eq!(config.provider.connect_timeout_ms, 2500);
        assert_eq!(config.provider.stream_idle_timeout_ms, 9000);
    }

    #[test]
    fn legacy_provider_timeout_maps_to_stream_idle_budget() {
        let raw = toml::from_str(
            r#"
[provider]
connect_timeout_ms = 2500
timeout_ms = 9000
"#,
        )
        .unwrap();

        let config = Config::from_raw(raw).unwrap();

        assert_eq!(config.provider.connect_timeout_ms, 2500);
        assert_eq!(config.provider.stream_idle_timeout_ms, 9000);
    }

    #[test]
    fn rejects_legacy_and_explicit_stream_idle_timeouts_together() {
        let raw = toml::from_str(
            r#"
[provider]
timeout_ms = 9000
stream_idle_timeout_ms = 9000
"#,
        )
        .unwrap();

        let error = Config::from_raw(raw).unwrap_err();

        assert!(matches!(
            error,
            AppError::Config(message)
                if message
                    == "provider.timeout_ms and provider.stream_idle_timeout_ms cannot both be set"
        ));
    }

    #[test]
    fn rejects_zero_provider_timeouts() {
        for (field, source) in [
            (
                "provider.connect_timeout_ms",
                "[provider]\nconnect_timeout_ms = 0\n",
            ),
            (
                "provider.stream_idle_timeout_ms",
                "[provider]\nstream_idle_timeout_ms = 0\n",
            ),
            ("provider.timeout_ms", "[provider]\ntimeout_ms = 0\n"),
        ] {
            let raw = toml::from_str(source).unwrap();
            let error = Config::from_raw(raw).unwrap_err();

            assert!(matches!(
                error,
                AppError::Config(message) if message == format!("{field} must be positive")
            ));
        }
    }

    #[test]
    fn openrouter_defaults_to_openrouter_endpoint_and_key() {
        let raw = RawConfig {
            provider: Some(RawProviderConfig {
                kind: Some(ProviderKind::OpenRouter),
                model: None,
                api_key_env: None,
                base_url: None,
                connect_timeout_ms: None,
                stream_idle_timeout_ms: None,
                timeout_ms: None,
                http_referer: None,
                app_title: None,
            }),
            limits: None,
            tools: None,
            gateway: None,
        };

        let config = Config::from_raw(raw).unwrap();

        assert_eq!(config.provider.model, "~openai/gpt-latest");
        assert_eq!(config.provider.api_key_env, "OPENROUTER_API_KEY");
        assert_eq!(config.provider.base_url, "https://openrouter.ai/api/v1");
    }

    #[test]
    fn explicit_config_path_wins_resolution() {
        let dir = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let explicit = dir.path().join("explicit.toml");
        std::fs::write(dir.path().join("plato.toml"), "").unwrap();

        let path = resolve_config_path_with(
            dir.path(),
            Some(explicit.clone()),
            Some(PathBuf::from("env.toml")),
            Some(home.path().to_path_buf()),
            None,
        )
        .unwrap();

        assert_eq!(path, Some(explicit));
    }

    #[test]
    fn plato_config_env_is_second_resolution_step() {
        let dir = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let env_path = dir.path().join("env.toml");
        std::fs::write(dir.path().join("plato.toml"), "").unwrap();

        let path = resolve_config_path_with(
            dir.path(),
            None,
            Some(env_path.clone()),
            Some(home.path().to_path_buf()),
            None,
        )
        .unwrap();

        assert_eq!(path, Some(env_path));
    }

    #[test]
    fn workspace_plato_toml_is_third_resolution_step() {
        let dir = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let workspace_config = dir.path().join("plato.toml");
        std::fs::write(&workspace_config, "").unwrap();

        let path = resolve_config_path_with(
            dir.path(),
            None,
            None,
            Some(home.path().to_path_buf()),
            None,
        )
        .unwrap();

        assert_eq!(path, Some(workspace_config));
    }

    #[test]
    fn user_config_is_fourth_resolution_step() {
        let dir = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let user_config = home
            .path()
            .join(".config")
            .join("plato")
            .join("config.toml");
        std::fs::create_dir_all(user_config.parent().unwrap()).unwrap();
        std::fs::write(&user_config, "").unwrap();

        let path = resolve_config_path_with(
            dir.path(),
            None,
            None,
            Some(home.path().to_path_buf()),
            Some(user_config.clone()),
        )
        .unwrap();

        assert_eq!(path, Some(user_config));
    }

    #[test]
    fn missing_config_paths_resolve_to_built_in_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();

        let path = resolve_config_path_with(
            dir.path(),
            None,
            None,
            Some(home.path().to_path_buf()),
            Some(
                home.path()
                    .join(".config")
                    .join("plato")
                    .join("config.toml"),
            ),
        )
        .unwrap();

        assert_eq!(path, None);
    }

    #[test]
    fn expands_leading_tilde_for_explicit_config_paths() {
        let workspace = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();

        let path = resolve_config_path_with(
            workspace.path(),
            Some(PathBuf::from("~/plato.toml")),
            None,
            Some(home.path().to_path_buf()),
            None,
        )
        .unwrap();

        assert_eq!(path, Some(home.path().join("plato.toml")));
    }

    #[test]
    fn relative_explicit_config_paths_resolve_against_workspace_root() {
        let workspace = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();

        let path = resolve_config_path_with(
            workspace.path(),
            Some(PathBuf::from("config/plato.toml")),
            None,
            Some(home.path().to_path_buf()),
            None,
        )
        .unwrap();

        assert_eq!(
            path,
            Some(workspace.path().join("config").join("plato.toml"))
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_config_uses_roaming_app_data_and_user_profile() {
        let workspace = tempfile::tempdir().unwrap();
        let roaming = tempfile::tempdir().unwrap();
        let profile = tempfile::tempdir().unwrap();
        let user_config = roaming.path().join("plato").join("config.toml");
        std::fs::create_dir_all(user_config.parent().unwrap()).unwrap();
        std::fs::write(&user_config, "").unwrap();

        temp_env::with_vars(
            [
                (PLATO_CONFIG_ENV, None),
                ("APPDATA", Some(roaming.path().as_os_str())),
                ("USERPROFILE", Some(profile.path().as_os_str())),
            ],
            || {
                assert_eq!(
                    resolve_config_path(workspace.path(), None).unwrap(),
                    Some(user_config)
                );
                assert_eq!(
                    resolve_config_path(workspace.path(), Some(Path::new("~/plato.toml"))).unwrap(),
                    Some(profile.path().join("plato.toml"))
                );
                assert_eq!(
                    resolve_config_path(workspace.path(), Some(Path::new(r"~\plato.toml")))
                        .unwrap(),
                    Some(profile.path().join("plato.toml"))
                );
            },
        );
    }
}
