use clap::Parser;
use plato_agent::tui::{TuiOptions, run_tui};
use std::path::PathBuf;

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
