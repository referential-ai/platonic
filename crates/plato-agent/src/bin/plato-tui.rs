use clap::Parser;
use plato_agent::{
    attach_server_interactive,
    tui::{TuiOptions, run_tui},
};
use platonic_client::{
    ClientError,
    client::{DaemonClient, DaemonConnectionConfig},
};
use std::{
    io::{self, IsTerminal},
    path::{Path, PathBuf},
    time::Duration,
};

const LOCAL_ENDPOINT_PROBE_TIMEOUT: Duration = Duration::from_millis(200);

#[derive(Debug, Parser)]
#[command(name = "plato-tui")]
#[command(about = "Plato Agent terminal client")]
#[command(version = platonic_protocol::BUILD_IDENTITY)]
struct Cli {
    #[arg(long, default_value = ".", help = "Workspace served by platonic")]
    workspace: PathBuf,

    #[arg(
        long,
        value_name = "PATH",
        help = "Server endpoint printed by platonic serve"
    )]
    socket: Option<PathBuf>,

    #[arg(
        long,
        value_name = "RUN_ID",
        help = "Initial transcript run to display"
    )]
    run: Option<String>,

    #[arg(long, value_name = "PATH", help = "Config path passed to daemon runs")]
    config: Option<PathBuf>,

    #[arg(long, help = "Render the current TUI state once and exit")]
    snapshot: bool,

    #[arg(long, help = "Use a static working indicator")]
    reduced_motion: bool,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

fn run() -> plato_agent::AppResult<()> {
    let cli = Cli::parse();
    let local_interactive = local_registration_prompt(
        cli.socket.as_deref(),
        cli.snapshot,
        io::stdin().is_terminal(),
        io::stdout().is_terminal(),
        io::stderr().is_terminal(),
    );
    let config = DaemonConnectionConfig::resolve(&cli.workspace, cli.socket.clone())?;
    match DaemonClient::connect_with_timeout(&config.socket_path, LOCAL_ENDPOINT_PROBE_TIMEOUT) {
        Ok(mut client) => {
            if local_interactive {
                drop(client);
                attach_server_interactive(
                    &config.workspace_root,
                    &config.socket_path,
                    &mut io::stdin().lock(),
                    &mut io::stderr(),
                )?;
            } else {
                client.hello(&config.workspace_root)?;
            }
        }
        Err(ClientError::Io(error)) if endpoint_is_unavailable(&error) => {}
        Err(error) => return Err(error.into()),
    }
    run_tui(TuiOptions {
        workspace: cli.workspace,
        socket: cli.socket,
        run: cli.run,
        config: cli.config,
        snapshot: cli.snapshot,
        reduced_motion: cli.reduced_motion,
        thread: None,
    })
}

fn endpoint_is_unavailable(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused | io::ErrorKind::TimedOut
    )
}

fn local_registration_prompt(
    socket: Option<&Path>,
    snapshot: bool,
    stdin_is_terminal: bool,
    stdout_is_terminal: bool,
    stderr_is_terminal: bool,
) -> bool {
    socket.is_none() && !snapshot && stdin_is_terminal && stdout_is_terminal && stderr_is_terminal
}

#[cfg(test)]
mod tests {
    use super::{endpoint_is_unavailable, local_registration_prompt};
    use std::{
        io::{Error, ErrorKind},
        path::Path,
    };

    #[test]
    fn offline_fallback_accepts_only_endpoint_unavailability_io() {
        for kind in [
            ErrorKind::NotFound,
            ErrorKind::ConnectionRefused,
            ErrorKind::TimedOut,
        ] {
            assert!(endpoint_is_unavailable(&Error::from(kind)));
        }
        for kind in [
            ErrorKind::PermissionDenied,
            ErrorKind::InvalidInput,
            ErrorKind::OutOfMemory,
        ] {
            assert!(!endpoint_is_unavailable(&Error::from(kind)));
        }
    }

    #[test]
    fn only_default_local_interactive_tui_may_prompt_for_registration() {
        assert!(local_registration_prompt(None, false, true, true, true));
        assert!(!local_registration_prompt(
            Some(Path::new("remote.sock")),
            false,
            true,
            true,
            true
        ));
        assert!(!local_registration_prompt(None, true, true, true, true));
        assert!(!local_registration_prompt(None, false, false, true, true));
        assert!(!local_registration_prompt(None, false, true, false, true));
        assert!(!local_registration_prompt(None, false, true, true, false));
    }
}
