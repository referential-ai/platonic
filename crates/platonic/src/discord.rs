use clap::Parser;
use plato_agent::discord_gateway::{DiscordGatewayOptions, run_discord_gateway};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "plato-gateway-discord")]
#[command(about = "Discord gateway for a local Plato Agent daemon")]
struct Cli {
    #[arg(long, default_value = ".")]
    workspace: PathBuf,

    #[arg(long, value_name = "PATH")]
    socket: Option<PathBuf>,

    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,
}

fn main() {
    let cli = Cli::parse();
    if let Err(error) = run_discord_gateway(DiscordGatewayOptions {
        workspace_root: cli.workspace,
        socket_path: cli.socket,
        config_path: cli.config,
    }) {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}
