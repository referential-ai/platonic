use clap::Parser;
#[cfg(windows)]
use clap::Subcommand;
use plato_agent::daemon::{
    server::{DaemonServer, HostDaemonServer},
    wake_listener,
};
#[cfg(unix)]
use signal_hook::{
    consts::{SIGINT, SIGTERM},
    iterator::Signals,
};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(unix)]
use std::thread;

#[derive(Debug, Parser)]
#[command(name = "plato-agentd")]
#[command(about = "Plato Agent local daemon")]
#[command(version = platonic_protocol::BUILD_IDENTITY)]
#[command(args_conflicts_with_subcommands = true)]
struct Cli {
    #[cfg(windows)]
    #[command(subcommand)]
    command: Option<Command>,

    #[arg(long, hide = true)]
    run_child: bool,

    #[arg(
        long,
        help = "Serve multiple workspaces on the stable host endpoint",
        conflicts_with = "workspace",
        conflicts_with = "socket"
    )]
    host: bool,

    #[arg(long, default_value = ".", conflicts_with = "host")]
    workspace: PathBuf,

    #[arg(long, value_name = "PATH", conflicts_with = "host")]
    socket: Option<PathBuf>,
}

#[cfg(windows)]
#[derive(Debug, Subcommand)]
enum Command {
    Control {
        #[command(subcommand)]
        command: ControlCommand,
    },
}

#[cfg(windows)]
#[derive(Debug, Subcommand)]
enum ControlCommand {
    ListWorkspaces,
    ShutdownIfIdle {
        #[arg(long, value_name = "ROOT")]
        workspace: Option<PathBuf>,
        #[arg(long)]
        quiet: bool,
    },
}

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

fn run() -> plato_agent::AppResult<()> {
    let cli = Cli::parse();
    #[cfg(windows)]
    if let Some(Command::Control { command }) = cli.command {
        let stdout = std::io::stdout();
        let mut output = stdout.lock();
        return match command {
            ControlCommand::ListWorkspaces => {
                plato_agent::daemon::control::list_workspaces(&mut output)
            }
            ControlCommand::ShutdownIfIdle { workspace, quiet } => {
                if quiet {
                    plato_agent::daemon::control::shutdown_if_idle(
                        workspace.as_deref(),
                        &mut std::io::sink(),
                    )
                } else {
                    plato_agent::daemon::control::shutdown_if_idle(
                        workspace.as_deref(),
                        &mut output,
                    )
                }
            }
        };
    }

    if cli.run_child {
        return plato_agent::daemon::run_stdio_child();
    }

    #[cfg(windows)]
    let installer_gate =
        plato_agent::daemon::installer_gate::InstallerStartupGate::acquire_for_daemon_startup()?;
    if cli.host {
        let server = HostDaemonServer::bind()?;
        #[cfg(windows)]
        drop(installer_gate);
        let socket_path = server.socket_path().to_path_buf();
        eprintln!("daemon_scope: host");
        eprintln!("socket_path: {}", socket_path.display());

        let shutdown = Arc::new(AtomicBool::new(false));
        install_shutdown_handler(shutdown.clone(), socket_path)?;
        return server.serve_forever(shutdown);
    }

    let server = DaemonServer::bind(&cli.workspace, cli.socket)?;
    #[cfg(windows)]
    drop(installer_gate);
    let socket_path = server.paths().socket_path.clone();
    eprintln!("workspace_id: {}", server.paths().workspace_id);
    eprintln!("socket_path: {}", socket_path.display());
    eprintln!("ledger_path: {}", server.paths().ledger_path.display());

    let shutdown = Arc::new(AtomicBool::new(false));
    install_shutdown_handler(shutdown.clone(), socket_path)?;

    server.serve_forever(shutdown)
}

#[cfg(unix)]
fn install_shutdown_handler(
    shutdown: Arc<AtomicBool>,
    socket_path: PathBuf,
) -> plato_agent::AppResult<()> {
    let mut signals = Signals::new([SIGINT, SIGTERM])?;
    thread::spawn(move || {
        if signals.forever().next().is_some() {
            request_shutdown(&shutdown, &socket_path);
        }
    });
    Ok(())
}

#[cfg(windows)]
fn install_shutdown_handler(
    shutdown: Arc<AtomicBool>,
    socket_path: PathBuf,
) -> plato_agent::AppResult<()> {
    ctrlc::set_handler(move || {
        request_shutdown(&shutdown, &socket_path);
    })
    .map_err(|error| {
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

    #[cfg(windows)]
    #[test]
    fn control_cli_parses_aggregate_and_targeted_shutdown() {
        let aggregate =
            Cli::try_parse_from(["plato-agentd", "control", "shutdown-if-idle"]).unwrap();
        assert!(matches!(
            aggregate.command,
            Some(Command::Control {
                command: ControlCommand::ShutdownIfIdle {
                    workspace: None,
                    quiet: false
                }
            })
        ));

        let targeted = Cli::try_parse_from([
            "plato-agentd",
            "control",
            "shutdown-if-idle",
            "--workspace",
            r"C:\work",
        ])
        .unwrap();
        assert!(matches!(
            targeted.command,
            Some(Command::Control {
                command: ControlCommand::ShutdownIfIdle {
                    workspace: Some(_),
                    quiet: false
                }
            })
        ));

        let quiet = Cli::try_parse_from(["plato-agentd", "control", "shutdown-if-idle", "--quiet"])
            .unwrap();
        assert!(matches!(
            quiet.command,
            Some(Command::Control {
                command: ControlCommand::ShutdownIfIdle { quiet: true, .. }
            })
        ));
    }

    #[cfg(windows)]
    #[test]
    fn serve_arguments_conflict_with_control() {
        assert!(
            Cli::try_parse_from([
                "plato-agentd",
                "--socket",
                r"\\.\pipe\custom",
                "control",
                "list-workspaces",
            ])
            .is_err()
        );
    }

    #[test]
    fn host_mode_conflicts_with_workspace_and_custom_socket() {
        let host = Cli::try_parse_from(["plato-agentd", "--host"]).unwrap();
        assert!(host.host);
        assert!(Cli::try_parse_from(["plato-agentd", "--host", "--workspace", "."]).is_err());
        assert!(Cli::try_parse_from(["plato-agentd", "--host", "--socket", "agent.sock"]).is_err());
    }

    #[test]
    fn shutdown_request_sets_flag_when_listener_is_missing() {
        let workspace = tempfile::tempdir().unwrap();
        let shutdown = AtomicBool::new(false);

        request_shutdown(&shutdown, &workspace.path().join("missing.sock"));

        assert!(shutdown.load(Ordering::SeqCst));
    }
}
