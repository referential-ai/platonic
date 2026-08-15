//! Authenticated HTTP/SSE adapter over the native Platonic v2 client.

#![deny(unsafe_code)]
#![warn(missing_docs)]

mod idempotency;
#[cfg(all(test, unix))]
#[path = "tests.rs"]
mod integration_tests;
mod openapi;
mod routes;
mod wire;

pub use openapi::gateway_openapi;

use crate::{
    AppError, AppResult,
    config::{HttpGatewayPrincipal, server_http_gateway_config, server_http_principals},
    paths,
};
use idempotency::IdempotencyStore;
#[cfg(unix)]
use platonic_client::transport;
use platonic_client::{ClientError, client::DaemonClient, paths as client_paths};
use platonic_protocol::{
    CAPABILITY_APPROVAL_DECIDE, CAPABILITY_DAEMON_STATUS, CAPABILITY_EVENTS_STREAM,
    CAPABILITY_HELLO, CAPABILITY_PROFILE_LIST, CAPABILITY_PROFILE_STATUS, CAPABILITY_RUN_CANCEL,
    CAPABILITY_THREAD_AUTHORITY, CAPABILITY_THREAD_EVENTS, CAPABILITY_THREAD_LIST,
    CAPABILITY_THREAD_SEND, CAPABILITY_THREAD_STATUS, CAPABILITY_THREAD_STOP,
    CAPABILITY_TRANSCRIPT_READ, CAPABILITY_WORKSPACE_LIST, CAPABILITY_WORKSPACE_STATUS, Capability,
    HelloResult, ProfileId, ProfileStatusResult, StreamEvent, ThreadAuthorityResult,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fmt::Write as _,
    fs::File,
    io::Read,
    net::{SocketAddr, TcpListener, TcpStream},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use wire::{HttpRequest, HttpResponse};

const DAEMON_TIMEOUT: Duration = Duration::from_secs(3);
const TOTAL_ACTIVE_LIMIT: usize = 64;
const PRINCIPAL_ACTIVE_LIMIT: usize = 8;
const TOTAL_STREAM_LIMIT: usize = 16;
const PRINCIPAL_STREAM_LIMIT: usize = 4;
const MUTATIONS_PER_SECOND: f64 = 10.0;
const MUTATION_BURST: f64 = 20.0;
const MAX_STREAM_CURSORS: usize = 100_000;

const REQUIRED_CAPABILITIES: &[Capability] = &[
    CAPABILITY_HELLO,
    CAPABILITY_WORKSPACE_LIST,
    CAPABILITY_WORKSPACE_STATUS,
    CAPABILITY_PROFILE_LIST,
    CAPABILITY_PROFILE_STATUS,
    CAPABILITY_DAEMON_STATUS,
    CAPABILITY_THREAD_LIST,
    CAPABILITY_THREAD_STATUS,
    CAPABILITY_THREAD_AUTHORITY,
    CAPABILITY_THREAD_SEND,
    CAPABILITY_THREAD_EVENTS,
    CAPABILITY_THREAD_STOP,
    CAPABILITY_TRANSCRIPT_READ,
    CAPABILITY_EVENTS_STREAM,
    CAPABILITY_APPROVAL_DECIDE,
    CAPABILITY_RUN_CANCEL,
];

/// Operator inputs for one HTTP gateway process.
#[derive(Clone, Debug, Default)]
pub struct HttpGatewayOptions {
    /// Optional host socket override.
    pub socket_path: Option<PathBuf>,
    /// Optional authorized configuration path.
    pub config_path: Option<PathBuf>,
    /// Optional listener override.
    pub bind: Option<SocketAddr>,
    /// Explicitly permits a non-loopback plaintext listener.
    pub allow_non_loopback: bool,
}

/// One generated bearer token and the hash stored in trusted configuration.
#[derive(Eq, PartialEq, Serialize)]
pub struct GeneratedHttpToken {
    /// Base64url-without-padding bearer material. This value is emitted once.
    pub token: String,
    /// Lowercase SHA-256 configuration value.
    pub token_sha256: String,
}

/// Generates one non-persisting 256-bit HTTP bearer token.
pub fn generate_http_token() -> AppResult<GeneratedHttpToken> {
    let mut bytes = [0; 32];
    #[cfg(unix)]
    File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    #[cfg(not(unix))]
    return Err(AppError::Config(
        "HTTP gateway token generation is supported on Linux and macOS".into(),
    ));
    let token = base64url_no_pad(&bytes);
    let token_sha256 = hex_sha256(token.as_bytes());
    Ok(GeneratedHttpToken {
        token,
        token_sha256,
    })
}

/// Resolves trusted configuration and runs the plaintext HTTP gateway.
pub fn run_http_gateway(options: HttpGatewayOptions) -> AppResult<()> {
    let mut config = server_http_gateway_config(options.config_path.as_deref())?;
    if let Some(bind) = options.bind {
        config.bind = bind;
    }
    validate_bind(
        config.bind,
        config.allow_non_loopback,
        options.allow_non_loopback,
    )?;
    if !config.bind.ip().is_loopback() {
        eprintln!(
            "warning: Platonic HTTP gateway is plaintext on {}; terminate TLS in the authorized operator proxy",
            config.bind
        );
    }
    let socket_path = options
        .socket_path
        .unwrap_or(client_paths::host_socket_path()?);
    let state_path = paths::server_state_root()?
        .join("gateway")
        .join("http-idempotency.db");
    let gateway = Gateway::new(socket_path, server_http_principals()?, state_path)?;
    let listener = TcpListener::bind(config.bind)?;
    eprintln!("http_gateway_bind: {}", listener.local_addr()?);
    gateway.serve(listener, Arc::new(AtomicBool::new(false)))
}

fn validate_bind(
    bind: SocketAddr,
    configured_non_loopback: bool,
    cli_non_loopback: bool,
) -> AppResult<()> {
    if bind.ip().is_loopback() || configured_non_loopback || cli_non_loopback {
        return Ok(());
    }
    Err(AppError::Config(
        "a non-loopback HTTP gateway bind requires allow_non_loopback = true or --allow-non-loopback"
            .into(),
    ))
}

#[derive(Clone)]
struct Gateway {
    socket_path: PathBuf,
    principals: Arc<Vec<HttpGatewayPrincipal>>,
    idempotency: Arc<Mutex<IdempotencyStore>>,
    limits: Arc<Limits>,
    active_connections: Arc<AtomicUsize>,
    stream_cursors: Arc<Mutex<HashMap<String, StreamCursor>>>,
    #[cfg(unix)]
    daemon_generations: Arc<Mutex<DaemonGenerationState>>,
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SocketIdentity {
    device: u64,
    inode: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DaemonGeneration(u64);

#[cfg(unix)]
struct DaemonConnection {
    identity: SocketIdentity,
    stream: transport::Stream,
    generation: DaemonGeneration,
}

#[cfg(unix)]
#[derive(Default)]
struct DaemonGenerationState {
    current: Option<DaemonConnection>,
    next: u64,
}

#[derive(Clone, Copy, Debug)]
struct StreamCursor {
    generation: DaemonGeneration,
    next_offset: u64,
}

impl Gateway {
    fn new(
        socket_path: PathBuf,
        principals: Vec<HttpGatewayPrincipal>,
        state_path: PathBuf,
    ) -> AppResult<Self> {
        let idempotency = IdempotencyStore::open(&state_path, now_ms())
            .map_err(|error| AppError::Config(error.to_string()))?;
        Ok(Self {
            socket_path,
            principals: Arc::new(principals),
            idempotency: Arc::new(Mutex::new(idempotency)),
            limits: Arc::new(Limits::default()),
            active_connections: Arc::new(AtomicUsize::new(0)),
            stream_cursors: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(unix)]
            daemon_generations: Arc::new(Mutex::new(DaemonGenerationState::default())),
        })
    }

    fn serve(&self, listener: TcpListener, shutdown: Arc<AtomicBool>) -> AppResult<()> {
        listener.set_nonblocking(true)?;
        while !shutdown.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    if !try_increment(&self.active_connections, TOTAL_ACTIVE_LIMIT) {
                        let response = error_response(
                            503,
                            "overloaded",
                            "the gateway active request limit is reached",
                        );
                        let _ = wire::write_response(&mut stream, &response);
                        continue;
                    }
                    let gateway = self.clone();
                    thread::spawn(move || {
                        let _guard = CounterGuard::new(gateway.active_connections.clone());
                        gateway.handle_connection(&mut stream);
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    fn handle_connection(&self, stream: &mut TcpStream) {
        let request = match wire::read_request(stream) {
            Ok(request) => request,
            Err(error) => {
                let response = wire_error_response(&error);
                let _ = wire::write_response(stream, &response);
                return;
            }
        };
        if let Some(response) = routes::handle(self, request, stream) {
            let _ = wire::write_response(stream, &response);
        }
    }

    fn authenticate(&self, request: &HttpRequest) -> Result<HttpGatewayPrincipal, HttpResponse> {
        let values = request.header_values("authorization").collect::<Vec<_>>();
        if values.len() != 1 {
            return Err(unauthorized());
        }
        let value = std::str::from_utf8(values[0]).map_err(|_| unauthorized())?;
        let (scheme, token) = value.split_once(' ').ok_or_else(unauthorized)?;
        if !scheme.eq_ignore_ascii_case("bearer")
            || token.len() != 43
            || !token
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
        {
            return Err(unauthorized());
        }
        let presented: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let mut matched = None;
        for principal in self.principals.iter() {
            let mut principal_match = 0u8;
            for expected in &principal.token_sha256 {
                principal_match |= constant_time_eq(&presented, expected);
            }
            if principal_match == 1 {
                matched = Some(principal.clone());
            }
        }
        matched.ok_or_else(unauthorized)
    }

    fn control_client(&self) -> Result<DaemonClient, ClientError> {
        DaemonClient::connect_with_timeout(&self.socket_path, DAEMON_TIMEOUT)
    }

    fn workspace_client(
        &self,
        principal: &HttpGatewayPrincipal,
        workspace_id: &str,
    ) -> Result<DaemonClient, HttpResponse> {
        if !principal
            .workspace_ids
            .iter()
            .any(|allowed| allowed == workspace_id)
        {
            return Err(error_response(
                403,
                "forbidden_scope",
                "the requested workspace is not admitted",
            ));
        }
        let mut control = self
            .control_client()
            .map_err(|error| native_transport_error(&error))?;
        let workspace = control
            .workspace_status(workspace_id.into())
            .map_err(|error| target_error(&error))?
            .workspace;
        let mut client = DaemonClient::connect_with_timeout(&self.socket_path, DAEMON_TIMEOUT)
            .map_err(|error| native_transport_error(&error))?;
        let hello = client
            .hello(std::path::Path::new(&workspace.root))
            .map_err(|error| native_transport_error(&error))?;
        validate_native_contract(workspace_id, &hello)?;
        Ok(client)
    }

    fn authorize_workspace(
        &self,
        principal: &HttpGatewayPrincipal,
        workspace_id: &str,
    ) -> Result<(), HttpResponse> {
        principal
            .workspace_ids
            .iter()
            .any(|allowed| allowed == workspace_id)
            .then_some(())
            .ok_or_else(|| {
                error_response(
                    403,
                    "forbidden_scope",
                    "the requested workspace is not admitted",
                )
            })
    }

    fn authorize_thread(
        &self,
        principal: &HttpGatewayPrincipal,
        client: &mut DaemonClient,
        workspace_id: &str,
        profile_id: &str,
        thread_id: &str,
    ) -> Result<ThreadAuthorityResult, HttpResponse> {
        self.authorize_profile(principal, workspace_id, profile_id)?;
        let authority = client
            .thread_authority(thread_id.into())
            .map_err(|error| scope_error(&error))?;
        if authority
            .authority
            .profile_id
            .as_ref()
            .map(ProfileId::as_str)
            != Some(profile_id)
        {
            return Err(forbidden_target());
        }
        Ok(authority)
    }

    fn authorize_profile(
        &self,
        principal: &HttpGatewayPrincipal,
        workspace_id: &str,
        profile_id: &str,
    ) -> Result<ProfileStatusResult, HttpResponse> {
        self.authorize_profile_scope(principal, workspace_id, profile_id)?;
        let mut control = self
            .control_client()
            .map_err(|error| native_transport_error(&error))?;
        let status = control
            .profile_status(ProfileId::new(profile_id.to_owned()).map_err(|_| forbidden_target())?)
            .map_err(|error| scope_error(&error))?;
        if status.status.profile.workspace_id != workspace_id {
            return Err(forbidden_target());
        }
        Ok(status)
    }

    fn authorize_profile_scope(
        &self,
        principal: &HttpGatewayPrincipal,
        workspace_id: &str,
        profile_id: &str,
    ) -> Result<(), HttpResponse> {
        self.authorize_workspace(principal, workspace_id)?;
        if !principal.profile_ids.is_empty()
            && !principal
                .profile_ids
                .iter()
                .any(|allowed| allowed == profile_id)
        {
            return Err(forbidden_target());
        }
        Ok(())
    }

    fn authorize_run(
        &self,
        principal: &HttpGatewayPrincipal,
        client: &mut DaemonClient,
        workspace_id: &str,
        profile_id: &str,
        run_id: &str,
    ) -> Result<(), HttpResponse> {
        self.authorize_profile(principal, workspace_id, profile_id)?;
        let page = client
            .events_stream(run_id, Some(0), 1)
            .map_err(|error| scope_error(&error))?;
        let matches_profile = page.events.first().is_some_and(|buffered| {
            matches!(
                &buffered.event,
                StreamEvent::Ledger { record }
                    if matches!(
                        &record.event,
                        platonic_core::HarnessEvent::RunStarted(platonic_core::RunStartedEvent {
                            identity: platonic_core::RunIdentity::Profile {
                                profile_id: run_profile_id,
                                ..
                            },
                            ..
                        }) if run_profile_id.as_str() == profile_id
                    )
            )
        });
        matches_profile.then_some(()).ok_or_else(forbidden_target)
    }

    #[cfg(unix)]
    fn daemon_generation(&self) -> Result<DaemonGeneration, HttpResponse> {
        let identity = self.daemon_socket_identity()?;
        let mut state = self
            .daemon_generations
            .lock()
            .expect("HTTP daemon generation lock poisoned");
        if let Some(current) = state.current.as_mut()
            && current.identity == identity
            && daemon_connection_alive(&mut current.stream)
        {
            return Ok(current.generation);
        }

        let stream = transport::connect_with_timeout(&self.socket_path, DAEMON_TIMEOUT)
            .map_err(|_| daemon_unavailable())?;
        stream
            .set_nonblocking(true)
            .map_err(|_| daemon_unavailable())?;
        if self.daemon_socket_identity()? != identity {
            return Err(daemon_unavailable());
        }
        let generation =
            DaemonGeneration(state.next.checked_add(1).ok_or_else(daemon_unavailable)?);
        state.next = generation.0;
        state.current = Some(DaemonConnection {
            identity,
            stream,
            generation,
        });
        Ok(generation)
    }

    #[cfg(unix)]
    fn daemon_socket_identity(&self) -> Result<SocketIdentity, HttpResponse> {
        use std::os::unix::fs::MetadataExt;

        let metadata = std::fs::metadata(&self.socket_path).map_err(|_| daemon_unavailable())?;
        Ok(SocketIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }

    #[cfg(not(unix))]
    fn daemon_generation(&self) -> Result<DaemonGeneration, HttpResponse> {
        Err(error_response(
            503,
            "native_unavailable",
            "the HTTP gateway requires a host-local Unix socket",
        ))
    }

    fn admit_stream_cursor(
        &self,
        target: &str,
        generation: DaemonGeneration,
        requested: Option<u64>,
    ) -> Result<(), HttpResponse> {
        let cursors = self
            .stream_cursors
            .lock()
            .expect("HTTP stream cursor lock poisoned");
        match (requested, cursors.get(target)) {
            (None, _) => Ok(()),
            (Some(_), None) => Ok(()),
            (Some(offset), Some(cursor))
                if cursor.generation == generation && offset <= cursor.next_offset =>
            {
                Ok(())
            }
            (Some(_), _) => Err(cursor_unavailable()),
        }
    }

    fn known_stream_tip(&self, target: &str, generation: DaemonGeneration) -> Option<u64> {
        self.stream_cursors
            .lock()
            .expect("HTTP stream cursor lock poisoned")
            .get(target)
            .filter(|cursor| cursor.generation == generation)
            .map(|cursor| cursor.next_offset)
    }

    fn record_stream_cursor(
        &self,
        target: String,
        generation: DaemonGeneration,
        next_offset: u64,
    ) -> Result<(), HttpResponse> {
        let mut cursors = self
            .stream_cursors
            .lock()
            .expect("HTTP stream cursor lock poisoned");
        if !cursors.contains_key(&target) && cursors.len() >= MAX_STREAM_CURSORS {
            return Err(error_response(
                503,
                "overloaded",
                "the gateway stream cursor limit is reached",
            ));
        }
        cursors
            .entry(target)
            .and_modify(|cursor| {
                if cursor.generation == generation {
                    cursor.next_offset = cursor.next_offset.max(next_offset);
                } else {
                    *cursor = StreamCursor {
                        generation,
                        next_offset,
                    };
                }
            })
            .or_insert(StreamCursor {
                generation,
                next_offset,
            });
        Ok(())
    }
}

#[cfg(unix)]
fn daemon_connection_alive(stream: &mut transport::Stream) -> bool {
    let mut byte = [0];
    loop {
        match stream.read(&mut byte) {
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return true,
            _ => return false,
        }
    }
}

#[derive(Default)]
struct Limits {
    principal_active: Mutex<HashMap<String, usize>>,
    streams: Mutex<StreamCounts>,
    rates: Mutex<HashMap<String, TokenBucket>>,
}

#[derive(Default)]
struct StreamCounts {
    total: usize,
    principals: HashMap<String, usize>,
}

struct PrincipalGuard {
    limits: Arc<Limits>,
    principal: String,
}

impl Drop for PrincipalGuard {
    fn drop(&mut self) {
        let mut active = self
            .limits
            .principal_active
            .lock()
            .expect("HTTP principal limit lock poisoned");
        if let Some(count) = active.get_mut(&self.principal) {
            *count -= 1;
            if *count == 0 {
                active.remove(&self.principal);
            }
        }
    }
}

struct StreamGuard {
    limits: Arc<Limits>,
    principal: String,
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        let mut streams = self
            .limits
            .streams
            .lock()
            .expect("HTTP stream limit lock poisoned");
        streams.total -= 1;
        if let Some(count) = streams.principals.get_mut(&self.principal) {
            *count -= 1;
            if *count == 0 {
                streams.principals.remove(&self.principal);
            }
        }
    }
}

impl Limits {
    fn acquire_principal(
        self: &Arc<Self>,
        principal: &str,
    ) -> Result<PrincipalGuard, HttpResponse> {
        let mut active = self
            .principal_active
            .lock()
            .expect("HTTP principal limit lock poisoned");
        let count = active.entry(principal.into()).or_default();
        if *count >= PRINCIPAL_ACTIVE_LIMIT {
            return Err(error_response(
                429,
                "rate_limited",
                "the principal active request limit is reached",
            ));
        }
        *count += 1;
        Ok(PrincipalGuard {
            limits: self.clone(),
            principal: principal.into(),
        })
    }

    fn acquire_stream(self: &Arc<Self>, principal: &str) -> Result<StreamGuard, HttpResponse> {
        let mut streams = self
            .streams
            .lock()
            .expect("HTTP stream limit lock poisoned");
        let principal_count = streams.principals.get(principal).copied().unwrap_or(0);
        if streams.total >= TOTAL_STREAM_LIMIT || principal_count >= PRINCIPAL_STREAM_LIMIT {
            return Err(error_response(
                503,
                "overloaded",
                "the gateway SSE stream limit is reached",
            ));
        }
        streams.total += 1;
        *streams.principals.entry(principal.into()).or_default() += 1;
        Ok(StreamGuard {
            limits: self.clone(),
            principal: principal.into(),
        })
    }

    fn admit_mutation(&self, principal: &str, now: Instant) -> bool {
        let mut rates = self.rates.lock().expect("HTTP rate limit lock poisoned");
        rates
            .entry(principal.into())
            .or_insert_with(|| TokenBucket::new(now))
            .take(now)
    }
}

struct TokenBucket {
    tokens: f64,
    updated: Instant,
}

impl TokenBucket {
    fn new(now: Instant) -> Self {
        Self {
            tokens: MUTATION_BURST,
            updated: now,
        }
    }

    fn take(&mut self, now: Instant) -> bool {
        self.tokens = (self.tokens
            + now.saturating_duration_since(self.updated).as_secs_f64() * MUTATIONS_PER_SECOND)
            .min(MUTATION_BURST);
        self.updated = self.updated.max(now);
        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;
        true
    }
}

struct CounterGuard {
    counter: Arc<AtomicUsize>,
}

impl CounterGuard {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        Self { counter }
    }
}

impl Drop for CounterGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::SeqCst);
    }
}

fn try_increment(counter: &AtomicUsize, limit: usize) -> bool {
    counter
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
            (value < limit).then_some(value + 1)
        })
        .is_ok()
}

fn validate_native_contract(workspace_id: &str, hello: &HelloResult) -> Result<(), HttpResponse> {
    if hello.workspace_id != workspace_id
        || hello.daemon_scope.as_deref() != Some("host")
        || REQUIRED_CAPABILITIES
            .iter()
            .any(|required| !hello.capabilities.contains(required))
    {
        return Err(error_response(
            503,
            "native_version_skew",
            "the native daemon does not satisfy the HTTP v2 contract",
        ));
    }
    Ok(())
}

fn unauthorized() -> HttpResponse {
    error_response(401, "unauthorized", "bearer authentication failed")
        .with_header("WWW-Authenticate", "Bearer")
}

fn target_error(error: &ClientError) -> HttpResponse {
    match error {
        ClientError::DaemonResponse(error) => routes::native_protocol_error_with_message(
            error,
            "the requested target is unavailable in the admitted workspace",
        ),
        _ => native_transport_error(error),
    }
}

fn scope_error(error: &ClientError) -> HttpResponse {
    match error {
        ClientError::DaemonResponse(_) => forbidden_target(),
        _ => native_transport_error(error),
    }
}

fn cursor_unavailable() -> HttpResponse {
    error_response(
        409,
        "event_cursor_unavailable",
        "the native event cursor is unavailable; inspect status or transcript",
    )
}

#[cfg(unix)]
fn daemon_unavailable() -> HttpResponse {
    error_response(
        503,
        "native_unavailable",
        "the native daemon is unavailable",
    )
}

fn forbidden_target() -> HttpResponse {
    error_response(
        404,
        "forbidden_scope",
        "the requested target is unavailable in the admitted workspace",
    )
}

fn native_transport_error(error: &ClientError) -> HttpResponse {
    match error {
        ClientError::DaemonProtocol(_) => error_response(
            503,
            "native_version_skew",
            "the native daemon does not satisfy the HTTP v2 contract",
        ),
        _ => error_response(
            503,
            "native_unavailable",
            "the native daemon is unavailable",
        ),
    }
}

#[derive(Serialize)]
struct ErrorEnvelope<'a> {
    error: ErrorBody<'a>,
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    code: &'a str,
    message: &'a str,
}

fn error_response(status: u16, code: &str, message: &str) -> HttpResponse {
    let body = serde_json::to_vec(&ErrorEnvelope {
        error: ErrorBody { code, message },
    })
    .expect("HTTP error envelope is serializable");
    HttpResponse::json(status, body)
}

fn wire_error_response(error: &wire::WireError) -> HttpResponse {
    match error {
        wire::WireError::BodyTooLarge => {
            error_response(413, "request_too_large", "the request body is too large")
        }
        wire::WireError::HeadersTooLarge | wire::WireError::TooManyHeaders => error_response(
            413,
            "request_headers_too_large",
            "the request headers are too large",
        ),
        _ => error_response(400, "malformed_request", "the HTTP request is malformed"),
    }
}

fn constant_time_eq(left: &[u8; 32], right: &[u8; 32]) -> u8 {
    let mut difference = 0u8;
    for index in 0..32 {
        difference |= std::hint::black_box(left[index] ^ right[index]);
    }
    u8::from(std::hint::black_box(difference) == 0)
}

fn base64url_no_pad(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let value = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        output.push(ALPHABET[((value >> 18) & 0x3f) as usize] as char);
        output.push(ALPHABET[((value >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            output.push(ALPHABET[((value >> 6) & 0x3f) as usize] as char);
        }
        if chunk.len() > 2 {
            output.push(ALPHABET[(value & 0x3f) as usize] as char);
        }
    }
    output
}

fn hex_sha256(value: &[u8]) -> String {
    let digest = Sha256::digest(value);
    let mut output = String::with_capacity(64);
    for byte in digest {
        write!(&mut output, "{byte:02x}").expect("writing to a string cannot fail");
    }
    output
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(token: &str, extra_headers: &[(&str, &str)]) -> HttpRequest {
        let mut headers = vec![(
            "Authorization".into(),
            format!("Bearer {token}").into_bytes(),
        )];
        headers.extend(
            extra_headers
                .iter()
                .map(|(name, value)| ((*name).into(), value.as_bytes().to_vec())),
        );
        HttpRequest {
            method: "GET".into(),
            target: "/v2/status".into(),
            headers,
            body: Vec::new(),
        }
    }

    #[test]
    fn generated_token_is_256_bit_base64url_and_hash_matches() {
        let generated = generate_http_token().unwrap();
        assert_eq!(generated.token.len(), 43);
        assert!(
            generated
                .token
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
        );
        assert_eq!(
            generated.token_sha256,
            hex_sha256(generated.token.as_bytes())
        );
    }

    #[test]
    fn constant_time_hash_comparison_checks_all_bytes() {
        assert_eq!(constant_time_eq(&[7; 32], &[7; 32]), 1);
        let mut first = [7; 32];
        first[0] = 6;
        assert_eq!(constant_time_eq(&first, &[7; 32]), 0);
        let mut last = [7; 32];
        last[31] = 6;
        assert_eq!(constant_time_eq(&last, &[7; 32]), 0);
    }

    #[test]
    fn token_bucket_has_burst_and_ten_per_second_refill() {
        let start = Instant::now();
        let mut bucket = TokenBucket::new(start);
        for _ in 0..20 {
            assert!(bucket.take(start));
        }
        assert!(!bucket.take(start));
        assert!(bucket.take(start + Duration::from_millis(100)));
        assert!(!bucket.take(start + Duration::from_millis(100)));
        assert!(!bucket.take(start));
        assert!(bucket.take(start + Duration::from_millis(200)));
    }

    #[test]
    fn non_loopback_bind_requires_one_explicit_gate() {
        let external: SocketAddr = "0.0.0.0:8787".parse().unwrap();
        let loopback: SocketAddr = "127.0.0.1:8787".parse().unwrap();

        assert!(validate_bind(loopback, false, false).is_ok());
        assert!(validate_bind(external, false, false).is_err());
        assert!(validate_bind(external, true, false).is_ok());
        assert!(validate_bind(external, false, true).is_ok());
    }

    #[test]
    fn bearer_auth_fails_closed_and_admits_configured_rotation_only() {
        let root = tempfile::tempdir().unwrap();
        let first = generate_http_token().unwrap();
        let rotated = generate_http_token().unwrap();
        let expired = generate_http_token().unwrap();
        let principal = HttpGatewayPrincipal {
            name: "remote_laptop".into(),
            token_sha256: vec![
                Sha256::digest(first.token.as_bytes()).into(),
                Sha256::digest(rotated.token.as_bytes()).into(),
            ],
            workspace_ids: vec!["workspace-1".into()],
            profile_ids: Vec::new(),
        };
        let gateway = Gateway::new(
            root.path().join("daemon.sock"),
            vec![principal],
            root.path().join("idempotency.db"),
        )
        .unwrap();

        for token in [&first.token, &rotated.token] {
            let authenticated = gateway
                .authenticate(&request(token, &[("X-Forwarded-User", "forged")]))
                .unwrap();
            assert_eq!(authenticated.name, "remote_laptop");
        }
        assert!(gateway.authenticate(&request(&expired.token, &[])).is_err());
        assert!(gateway.authenticate(&request("short", &[])).is_err());

        let mut duplicate = request(&first.token, &[]);
        duplicate.headers.push((
            "Authorization".into(),
            format!("Bearer {}", rotated.token).into_bytes(),
        ));
        assert!(gateway.authenticate(&duplicate).is_err());
    }

    #[test]
    fn principal_and_stream_concurrency_limits_recover_after_release() {
        let active = AtomicUsize::new(0);
        for _ in 0..TOTAL_ACTIVE_LIMIT {
            assert!(try_increment(&active, TOTAL_ACTIVE_LIMIT));
        }
        assert!(!try_increment(&active, TOTAL_ACTIVE_LIMIT));

        let limits = Arc::new(Limits::default());
        let principal = (0..PRINCIPAL_ACTIVE_LIMIT)
            .map(|_| limits.acquire_principal("remote").unwrap())
            .collect::<Vec<_>>();
        assert!(limits.acquire_principal("remote").is_err());
        drop(principal);
        assert!(limits.acquire_principal("remote").is_ok());

        let streams = (0..PRINCIPAL_STREAM_LIMIT)
            .map(|_| limits.acquire_stream("remote").unwrap())
            .collect::<Vec<_>>();
        assert!(limits.acquire_stream("remote").is_err());
        drop(streams);
        assert!(limits.acquire_stream("remote").is_ok());

        let limits = Arc::new(Limits::default());
        let total_streams = (0..TOTAL_STREAM_LIMIT)
            .map(|index| {
                let principal = format!("remote-{}", index / PRINCIPAL_STREAM_LIMIT);
                limits.acquire_stream(&principal).unwrap()
            })
            .collect::<Vec<_>>();
        assert!(limits.acquire_stream("one-more").is_err());
        drop(total_streams);
        assert!(limits.acquire_stream("one-more").is_ok());
    }

    #[test]
    fn mutations_require_the_exact_workspace_and_every_native_capability() {
        let mut hello = HelloResult {
            daemon_version: "test".into(),
            workspace_id: "workspace-1".into(),
            ledger_path: "/tmp/ledger.db".into(),
            capabilities: REQUIRED_CAPABILITIES.to_vec(),
            daemon_scope: Some("host".into()),
        };
        assert!(validate_native_contract("workspace-1", &hello).is_ok());
        assert!(validate_native_contract("workspace-other", &hello).is_err());

        hello.daemon_scope = None;
        assert!(validate_native_contract("workspace-1", &hello).is_err());
        hello.daemon_scope = Some("host".into());
        hello.capabilities.pop();
        let response = validate_native_contract("workspace-1", &hello).unwrap_err();
        assert_eq!(response.status, 503);
        assert!(
            String::from_utf8(response.body)
                .unwrap()
                .contains("native_version_skew")
        );
    }

    #[cfg(unix)]
    #[test]
    fn stream_cursors_resume_exactly_and_fail_closed_after_daemon_restart() {
        use std::os::unix::net::UnixListener;

        let root = tempfile::tempdir().unwrap();
        let socket = root.path().join("daemon.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let gateway = Gateway::new(
            socket.clone(),
            Vec::new(),
            root.path().join("idempotency.db"),
        )
        .unwrap();
        let first = gateway.daemon_generation().unwrap();
        assert!(
            gateway
                .admit_stream_cursor("thread\0ws\0unseen", first, Some(0))
                .is_ok()
        );
        gateway
            .record_stream_cursor("thread\0ws\0t".into(), first, 8)
            .unwrap();
        assert!(
            gateway
                .admit_stream_cursor("thread\0ws\0t", first, Some(8))
                .is_ok()
        );
        assert!(
            gateway
                .admit_stream_cursor("thread\0ws\0t", first, Some(9))
                .is_err()
        );

        drop(listener);
        std::fs::remove_file(&socket).unwrap();
        let _restarted = UnixListener::bind(&socket).unwrap();
        let restarted_identity = gateway.daemon_socket_identity().unwrap();
        gateway
            .daemon_generations
            .lock()
            .unwrap()
            .current
            .as_mut()
            .unwrap()
            .identity = restarted_identity;
        let second = gateway.daemon_generation().unwrap();
        assert_ne!(first, second);
        assert!(
            gateway
                .admit_stream_cursor("thread\0ws\0t", second, Some(8))
                .is_err()
        );
    }
}
