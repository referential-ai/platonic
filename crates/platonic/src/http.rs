use platonic_server::gateway::http::{HttpGatewayOptions, generate_http_token, run_http_gateway};
use std::{net::SocketAddr, path::PathBuf};

pub fn run(
    socket_path: Option<PathBuf>,
    config_path: Option<PathBuf>,
    bind: Option<SocketAddr>,
    allow_non_loopback: bool,
    generate_token: bool,
) -> platonic_server::AppResult<()> {
    if generate_token {
        if socket_path.is_some() || config_path.is_some() || bind.is_some() || allow_non_loopback {
            return Err(platonic_server::AppError::Config(
                "--generate-token cannot be combined with listener options".into(),
            ));
        }
        println!("{}", serde_json::to_string(&generate_http_token()?)?);
        return Ok(());
    }
    run_http_gateway(HttpGatewayOptions {
        socket_path,
        config_path,
        bind,
        allow_non_loopback,
    })
}
