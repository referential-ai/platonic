use platonic_server::gateway::discord::{DiscordGatewayOptions, run_discord_gateway};
use std::path::PathBuf;

pub fn run(
    workspace_root: PathBuf,
    socket_path: Option<PathBuf>,
    config_path: Option<PathBuf>,
) -> platonic_server::AppResult<()> {
    run_discord_gateway(DiscordGatewayOptions {
        workspace_root,
        socket_path,
        config_path,
    })
}
