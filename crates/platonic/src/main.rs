use clap::{Parser, Subcommand};
use platonic_client::{
    client::{DaemonClient, DaemonConnectionConfig},
    paths,
};
use platonic_server::{
    AppError, AppResult,
    daemon::{
        run_stdio_child,
        server::{DaemonServer, HostDaemonServer},
        wake_listener,
    },
};
#[cfg(unix)]
use signal_hook::{
    consts::{SIGINT, SIGTERM},
    iterator::Signals,
};
#[cfg(unix)]
use std::thread;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

mod discord;

const CLIENT_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, Parser)]
#[command(name = "platonic")]
#[command(about = "Platonic agent server")]
#[command(version = platonic_protocol::BUILD_IDENTITY)]
struct Cli {
    #[arg(long, hide = true)]
    run_child: bool,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the server in the foreground.
    Serve {
        /// Serve one legacy workspace instead of the host endpoint.
        #[arg(long, value_name = "DIR")]
        workspace: Option<PathBuf>,
        /// Override the endpoint for a legacy workspace server.
        #[arg(long, value_name = "PATH", requires = "workspace")]
        socket: Option<PathBuf>,
    },
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
        #[arg(long, value_name = "DIR", default_value = ".", global = true)]
        workspace: PathBuf,
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
enum GatewayCommand {
    /// Run the Discord gateway for one workspace.
    Discord {
        #[arg(long, value_name = "DIR", default_value = ".")]
        workspace: PathBuf,
        #[arg(long, value_name = "PATH")]
        socket: Option<PathBuf>,
        #[arg(long, value_name = "FILE")]
        config: Option<PathBuf>,
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
        Some(Command::Serve { workspace, socket }) => serve(workspace, socket),
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
        Some(Command::Workspace {
            command,
            workspace,
            socket,
        }) => run_workspace(command, workspace, socket),
        Some(Command::Gateway { command }) => match command {
            GatewayCommand::Discord {
                workspace,
                socket,
                config,
            } => discord::run(workspace, socket, config),
        },
        None => Err(AppError::Config("a command is required".into())),
    }
}

fn serve(workspace: Option<PathBuf>, socket: Option<PathBuf>) -> AppResult<()> {
    #[cfg(windows)]
    let installer_gate =
        platonic_client::installer_gate::InstallerStartupGate::acquire_for_daemon_startup()?;
    let shutdown = Arc::new(AtomicBool::new(false));
    if let Some(workspace) = workspace {
        let server = DaemonServer::bind(&workspace, socket)?;
        #[cfg(windows)]
        drop(installer_gate);
        let socket_path = server.paths().socket_path.clone();
        eprintln!("workspace_id: {}", server.paths().workspace_id);
        eprintln!("socket_path: {}", socket_path.display());
        eprintln!("ledger_path: {}", server.paths().ledger_path.display());
        install_shutdown_handler(Arc::clone(&shutdown), socket_path)?;
        return server.serve_forever(shutdown);
    }

    let server = HostDaemonServer::bind()?;
    #[cfg(windows)]
    drop(installer_gate);
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

fn run_workspace(
    command: WorkspaceCommand,
    workspace: PathBuf,
    socket: Option<PathBuf>,
) -> AppResult<()> {
    let mut client = connect(&workspace, socket)?;
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

#[cfg(windows)]
fn install_shutdown_handler(shutdown: Arc<AtomicBool>, socket_path: PathBuf) -> AppResult<()> {
    ctrlc::set_handler(move || request_shutdown(&shutdown, &socket_path)).map_err(|error| {
        std::io::Error::other(format!(
            "failed to install console control handler: {error}"
        ))
    })?;
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
    fn host_mode_conflicts_with_workspace_and_custom_socket() {
        assert!(Cli::try_parse_from(["platonic"]).is_ok());
        assert!(Cli::try_parse_from(["platonic", "serve"]).is_ok());
        assert!(Cli::try_parse_from(["platonic", "serve", "--socket", "agent.sock"]).is_err());
    }

    #[test]
    fn shutdown_request_sets_flag_when_listener_is_missing() {
        let workspace = tempfile::tempdir().unwrap();
        let shutdown = AtomicBool::new(false);

        request_shutdown(&shutdown, &workspace.path().join("missing.sock"));

        assert!(shutdown.load(Ordering::SeqCst));
    }
}
