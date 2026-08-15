use clap::{Parser, Subcommand};
use platonic_client::{
    client::{DaemonClient, DaemonConnectionConfig},
    paths,
};
use platonic_protocol::{
    ProfileContent, ProfileCreateParams, ProfileId, ProfileOpenDecision, ProfileOpenResult,
    ProfileUpdateParams, ReasoningEffort, ThreadApprovalPolicy, ThreadRepositoryRequest,
};
use platonic_server::{
    AppError, AppResult,
    config::Config,
    daemon::{run_stdio_child, server::HostDaemonServer, wake_listener},
};
#[cfg(unix)]
use signal_hook::{
    consts::{SIGINT, SIGTERM},
    iterator::Signals,
};
#[cfg(unix)]
use std::thread;
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

mod discord;
mod http;

const CLIENT_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, Parser)]
#[command(name = "platonic")]
#[command(about = "Platonic agent server")]
#[command(version = platonic_protocol::PLATONIC_BUILD_IDENTITY)]
struct Cli {
    #[arg(long, hide = true)]
    run_child: bool,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the server in the foreground.
    Serve,
    /// Read server status for a workspace.
    Status {
        #[arg(long, value_name = "DIR", default_value = ".")]
        workspace: PathBuf,
        #[arg(long, value_name = "PATH")]
        socket: Option<PathBuf>,
        #[arg(long, value_name = "SESSION_ID")]
        session: Option<String>,
        #[arg(long, value_name = "FILE")]
        config: Option<PathBuf>,
    },
    /// Shut down an idle server.
    Shutdown {
        #[arg(long, value_name = "DIR", default_value = ".")]
        workspace: PathBuf,
        #[arg(long, value_name = "PATH")]
        socket: Option<PathBuf>,
    },
    /// Manage registered workspaces.
    Workspace {
        #[command(subcommand)]
        command: WorkspaceCommand,
        #[arg(long, value_name = "PATH", global = true)]
        socket: Option<PathBuf>,
    },
    /// Manage workspace-bound profiles.
    Profile {
        #[command(subcommand)]
        command: ProfileCommand,
        #[arg(long, value_name = "PATH", global = true)]
        socket: Option<PathBuf>,
    },
    /// Run a server-owned gateway connector.
    Gateway {
        #[command(subcommand)]
        command: GatewayCommand,
    },
}

#[derive(Debug, Subcommand)]
enum WorkspaceCommand {
    /// Register a named workspace directory.
    Create {
        #[arg(value_name = "NAME")]
        name: String,
        #[arg(value_name = "DIR")]
        root: PathBuf,
    },
    /// List every registered workspace.
    List,
    /// Read one workspace by its server-minted id.
    Status {
        #[arg(value_name = "WORKSPACE_ID")]
        workspace_id: String,
    },
}

#[derive(Debug, Subcommand)]
enum ProfileCommand {
    /// Create a workspace-bound profile.
    Create {
        #[arg(value_name = "NAME")]
        display_name: String,
        #[arg(value_name = "WORKSPACE_ID")]
        workspace_id: String,
        #[arg(long, value_name = "FILE")]
        config: Option<PathBuf>,
        #[arg(long, value_name = "MODEL")]
        model: Option<String>,
        #[arg(
            long,
            value_name = "EFFORT",
            default_value = "none",
            value_parser = parse_reasoning_effort
        )]
        reasoning_effort: ReasoningEffort,
        #[arg(
            long,
            value_name = "POLICY",
            default_value = "prompt",
            value_parser = parse_approval_policy
        )]
        approval_policy: ThreadApprovalPolicy,
        #[arg(long = "tool", value_name = "TOOL")]
        toolset: Vec<String>,
        #[arg(long, value_name = "FILE")]
        instructions: Option<PathBuf>,
        #[arg(long, value_name = "FILE")]
        memory: Option<PathBuf>,
        #[arg(long = "skill", value_name = "REF")]
        skills: Vec<String>,
    },
    /// List profiles, optionally within one workspace.
    List {
        #[arg(long, value_name = "WORKSPACE_ID")]
        workspace: Option<String>,
        #[arg(long, value_name = "COUNT")]
        limit: Option<usize>,
    },
    /// Read one profile and its current content revision.
    Status {
        #[arg(value_name = "PROFILE_ID")]
        profile_id: String,
    },
    /// Update future-thread defaults and append one content revision.
    Update {
        #[arg(value_name = "PROFILE_ID")]
        profile_id: String,
        #[arg(long, value_name = "FILE")]
        config: Option<PathBuf>,
        #[arg(long, value_name = "MODEL")]
        model: Option<String>,
        #[arg(long, value_name = "EFFORT", value_parser = parse_reasoning_effort)]
        reasoning_effort: Option<ReasoningEffort>,
        #[arg(long, value_name = "POLICY", value_parser = parse_approval_policy)]
        approval_policy: Option<ThreadApprovalPolicy>,
        #[arg(long = "tool", value_name = "TOOL")]
        toolset: Vec<String>,
        #[arg(long, value_name = "FILE")]
        instructions: Option<PathBuf>,
        #[arg(long, value_name = "FILE")]
        memory: Option<PathBuf>,
        #[arg(long = "skill", value_name = "REF", conflicts_with = "clear_skills")]
        skills: Vec<String>,
        #[arg(long)]
        clear_skills: bool,
    },
    /// Resolve or create a profile's durable home thread.
    Open {
        #[arg(value_name = "PROFILE_ID")]
        profile_id: String,
        #[arg(long = "repo", value_name = "PATH")]
        repositories: Vec<String>,
        #[arg(long, value_name = "PATH", default_value = ".")]
        working_repository: String,
        #[arg(long, value_name = "PATH", default_value = ".")]
        working_subdir: String,
        #[arg(long, value_name = "KEY")]
        idempotency_key: Option<String>,
        #[arg(long)]
        approve: bool,
    },
}

#[derive(Debug, Subcommand)]
enum GatewayCommand {
    /// Run the Discord gateway for one workspace.
    Discord {
        #[arg(long, value_name = "DIR", default_value = ".")]
        workspace: PathBuf,
        /// Override the host endpoint for testing or operations.
        #[arg(long, value_name = "PATH")]
        socket: Option<PathBuf>,
        #[arg(long, value_name = "FILE")]
        config: Option<PathBuf>,
    },
    /// Run the authenticated plaintext HTTP/SSE gateway.
    Http {
        /// Override the host endpoint for testing or operations.
        #[arg(long, value_name = "PATH")]
        socket: Option<PathBuf>,
        /// Read gateway settings from an authorized configuration file.
        #[arg(long, value_name = "FILE")]
        config: Option<PathBuf>,
        /// Override the plaintext listener address.
        #[arg(long, value_name = "ADDRESS")]
        bind: Option<std::net::SocketAddr>,
        /// Explicitly authorize a non-loopback plaintext listener.
        #[arg(long)]
        allow_non_loopback: bool,
        /// Generate one bearer token and its configuration hash without persistence.
        #[arg(long)]
        generate_token: bool,
    },
}

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

fn run() -> AppResult<()> {
    let cli = Cli::parse();
    if cli.run_child {
        if cli.command.is_some() {
            return Err(AppError::Config(
                "--run-child cannot be combined with a command".into(),
            ));
        }
        return run_stdio_child();
    }

    match cli.command {
        Some(Command::Serve) => serve(),
        Some(Command::Status {
            workspace,
            socket,
            session,
            config,
        }) => {
            let mut client = connect(&workspace, socket)?;
            let status = client.daemon_status(
                session,
                config.map(|path| path.to_string_lossy().into_owned()),
            )?;
            println!("{}", serde_json::to_string(&status)?);
            Ok(())
        }
        Some(Command::Shutdown { workspace, socket }) => {
            let mut client = connect(&workspace, socket)?;
            println!("{}", serde_json::to_string(&client.shutdown_if_idle()?)?);
            Ok(())
        }
        Some(Command::Workspace { command, socket }) => run_workspace(command, socket),
        Some(Command::Profile { command, socket }) => run_profile(command, socket),
        Some(Command::Gateway { command }) => match command {
            GatewayCommand::Discord {
                workspace,
                socket,
                config,
            } => discord::run(workspace, socket, config),
            GatewayCommand::Http {
                socket,
                config,
                bind,
                allow_non_loopback,
                generate_token,
            } => http::run(socket, config, bind, allow_non_loopback, generate_token),
        },
        None => Err(AppError::Config("a command is required".into())),
    }
}

fn serve() -> AppResult<()> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let server = HostDaemonServer::bind()?;
    let socket_path = server.socket_path().to_path_buf();
    eprintln!("daemon_scope: host");
    eprintln!("socket_path: {}", socket_path.display());
    install_shutdown_handler(Arc::clone(&shutdown), socket_path)?;
    server.serve_forever(shutdown)
}

fn connect(workspace: &std::path::Path, socket: Option<PathBuf>) -> AppResult<DaemonClient> {
    let endpoint = match socket {
        Some(socket) => socket,
        None => paths::host_socket_path()?,
    };
    let config = DaemonConnectionConfig::resolve(workspace, Some(endpoint))?;
    let mut client = DaemonClient::connect_with_timeout(&config.socket_path, CLIENT_TIMEOUT)?;
    client.hello(&config.workspace_root)?;
    Ok(client)
}

fn run_workspace(command: WorkspaceCommand, socket: Option<PathBuf>) -> AppResult<()> {
    let mut client = connect_control(socket)?;
    let output = match command {
        WorkspaceCommand::Create { name, root } => {
            serde_json::to_string(&client.workspace_create(name, root)?)?
        }
        WorkspaceCommand::List => serde_json::to_string(&client.workspace_list()?)?,
        WorkspaceCommand::Status { workspace_id } => {
            serde_json::to_string(&client.workspace_status(workspace_id)?)?
        }
    };
    println!("{output}");
    Ok(())
}

fn connect_control(socket: Option<PathBuf>) -> AppResult<DaemonClient> {
    let endpoint = match socket {
        Some(socket) => socket,
        None => paths::host_socket_path()?,
    };
    Ok(DaemonClient::connect_with_timeout(
        &endpoint,
        CLIENT_TIMEOUT,
    )?)
}

fn run_profile(command: ProfileCommand, socket: Option<PathBuf>) -> AppResult<()> {
    let mut client = connect_control(socket)?;
    let output = match command {
        ProfileCommand::Create {
            display_name,
            workspace_id,
            config,
            model,
            reasoning_effort,
            approval_policy,
            toolset,
            instructions,
            memory,
            skills,
        } => {
            let workspace = client.workspace_status(workspace_id.clone())?.workspace;
            let config = Config::load(Path::new(&workspace.root), config.as_deref())?;
            if std::env::var_os(&config.provider.api_key_env).is_none() {
                return Err(AppError::Config(format!(
                    "provider key is unavailable: set {} (for example, export {}=<provider-key>), or set provider.api_key_env in --config, PLATO_CONFIG, or the user config before running platonic profile create",
                    config.provider.api_key_env, config.provider.api_key_env
                )));
            }
            let model = model.unwrap_or(config.provider.model);
            let toolset = if toolset.is_empty() {
                config.tools.enabled
            } else {
                toolset
            };
            let result = client.profile_create(ProfileCreateParams {
                workspace_id,
                display_name,
                model: Some(model),
                reasoning_effort,
                approval_policy,
                toolset: Some(toolset),
                content: ProfileContent {
                    instructions_markdown: read_content(instructions.as_deref())?,
                    memory_markdown: read_content(memory.as_deref())?,
                    skill_refs: skills,
                },
                config_path: None,
            })?;
            serde_json::to_string(&result)?
        }
        ProfileCommand::List { workspace, limit } => {
            serde_json::to_string(&client.profile_list(workspace, limit)?)?
        }
        ProfileCommand::Status { profile_id } => {
            serde_json::to_string(&client.profile_status(ProfileId::new(profile_id)?)?)?
        }
        ProfileCommand::Update {
            profile_id,
            config,
            model,
            reasoning_effort,
            approval_policy,
            toolset,
            instructions,
            memory,
            skills,
            clear_skills,
        } => {
            let profile_id = ProfileId::new(profile_id)?;
            let current = client.profile_status(profile_id.clone())?.status;
            let configured = match config.as_deref() {
                Some(path) => {
                    let workspace = client
                        .workspace_status(current.profile.workspace_id.clone())?
                        .workspace;
                    let config = Config::load(Path::new(&workspace.root), Some(path))?;
                    if std::env::var_os(&config.provider.api_key_env).is_none() {
                        return Err(AppError::Config(format!(
                            "provider key is unavailable: set {} before updating the profile",
                            config.provider.api_key_env
                        )));
                    }
                    Some(config)
                }
                None => None,
            };
            let content = ProfileContent {
                instructions_markdown: match instructions.as_deref() {
                    Some(path) => read_content(Some(path))?,
                    None => current.revision.content.instructions_markdown,
                },
                memory_markdown: match memory.as_deref() {
                    Some(path) => read_content(Some(path))?,
                    None => current.revision.content.memory_markdown,
                },
                skill_refs: if clear_skills || !skills.is_empty() {
                    skills
                } else {
                    current.revision.content.skill_refs
                },
            };
            serde_json::to_string(
                &client.profile_update(ProfileUpdateParams {
                    profile_id,
                    model: model
                        .or_else(|| {
                            configured
                                .as_ref()
                                .map(|config| config.provider.model.clone())
                        })
                        .unwrap_or(current.profile.model),
                    reasoning_effort: reasoning_effort.unwrap_or(current.profile.reasoning_effort),
                    approval_policy: approval_policy.unwrap_or(current.profile.approval_policy),
                    toolset: if toolset.is_empty() {
                        configured
                            .map(|config| config.tools.enabled)
                            .unwrap_or(current.profile.toolset)
                    } else {
                        toolset
                    },
                    content,
                })?,
            )?
        }
        ProfileCommand::Open {
            profile_id,
            repositories,
            working_repository,
            working_subdir,
            idempotency_key,
            approve,
        } => {
            let profile_id = ProfileId::new(profile_id)?;
            let workspace_id = client
                .profile_status(profile_id.clone())?
                .status
                .profile
                .workspace_id;
            let workspace = client.workspace_status(workspace_id)?.workspace;
            client.hello(Path::new(&workspace.root))?;
            let mut result = client.profile_open_resolve(profile_id.clone())?;
            if matches!(result, ProfileOpenResult::NoHome { .. }) {
                let repositories = if repositories.is_empty() {
                    vec![".".into()]
                } else {
                    repositories
                };
                result = client.profile_open_start(
                    profile_id.clone(),
                    idempotency_key
                        .unwrap_or_else(|| format!("platonic-profile-open-{profile_id}")),
                    repositories
                        .into_iter()
                        .map(|repo| ThreadRepositoryRequest { repo, branch: None })
                        .collect(),
                    working_repository,
                    working_subdir,
                )?;
            }
            if approve
                && let ProfileOpenResult::ApprovalRequired {
                    home_reservation_id,
                    ..
                } = result
            {
                result =
                    client.profile_open_decide(home_reservation_id, ProfileOpenDecision::Grant)?;
            }
            serde_json::to_string(&result)?
        }
    };
    println!("{output}");
    Ok(())
}

fn read_content(path: Option<&Path>) -> AppResult<String> {
    match path {
        Some(path) => Ok(std::fs::read_to_string(path)?),
        None => Ok(String::new()),
    }
}

fn parse_reasoning_effort(value: &str) -> Result<ReasoningEffort, String> {
    ReasoningEffort::parse(value).ok_or_else(|| {
        format!(
            "unknown reasoning effort {value}; expected none, minimal, low, medium, high, xhigh, or max"
        )
    })
}

fn parse_approval_policy(value: &str) -> Result<ThreadApprovalPolicy, String> {
    ThreadApprovalPolicy::parse(value)
        .ok_or_else(|| format!("unknown approval policy {value}; expected prompt or yolo"))
}

#[cfg(unix)]
fn install_shutdown_handler(shutdown: Arc<AtomicBool>, socket_path: PathBuf) -> AppResult<()> {
    let mut signals = Signals::new([SIGINT, SIGTERM])?;
    thread::spawn(move || {
        if signals.forever().next().is_some() {
            request_shutdown(&shutdown, &socket_path);
        }
    });
    Ok(())
}

fn request_shutdown(shutdown: &AtomicBool, socket_path: &std::path::Path) {
    shutdown.store(true, Ordering::SeqCst);
    wake_listener(socket_path);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serve_rejects_retired_workspace_and_socket_options() {
        assert!(Cli::try_parse_from(["platonic"]).is_ok());
        assert!(Cli::try_parse_from(["platonic", "serve"]).is_ok());
        assert!(Cli::try_parse_from(["platonic", "serve", "--socket", "agent.sock"]).is_err());
        assert!(Cli::try_parse_from(["platonic", "serve", "--workspace", "."]).is_err());
    }

    #[test]
    fn shutdown_request_sets_flag_when_listener_is_missing() {
        let workspace = tempfile::tempdir().unwrap();
        let shutdown = AtomicBool::new(false);

        request_shutdown(&shutdown, &workspace.path().join("missing.sock"));

        assert!(shutdown.load(Ordering::SeqCst));
    }
}
