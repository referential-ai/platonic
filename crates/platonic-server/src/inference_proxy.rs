//! Fixed-loopback OpenRouter reverse proxy and opt-in traffic comparison.

use crate::{
    AppError, AppResult,
    daemon::lock::{HostProcessLock, LockMetadata},
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashSet},
    fmt::Write as _,
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    os::unix::{
        fs::{DirBuilderExt, FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt},
        net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const OPENROUTER_ORIGIN: &str = "https://openrouter.ai";
const MAX_HEADER_BYTES: usize = 32 * 1024;
const MAX_HEADER_COUNT: usize = 128;
const MAX_REQUEST_BYTES: usize = 32 * 1024 * 1024;
const IO_CHUNK_BYTES: usize = 16 * 1024;
const START_TIMEOUT: Duration = Duration::from_secs(5);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(120);
const CONTROL_TIMEOUT: Duration = Duration::from_secs(2);
const PRIVATE_DIRECTORY_MODE: u32 = 0o700;
const PRIVATE_FILE_MODE: u32 = 0o600;
const CAPTURE_VERSION: u64 = 1;
const CAPTURE_FILE: &str = "traffic.jsonl";

/// Inspectable state returned by `inference-proxy up`, `status`, and `down`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InferenceProxyStatus {
    pub running: bool,
    pub pid: Option<u32>,
    pub base_url: Option<String>,
    pub capture_dir: Option<PathBuf>,
    pub capture_file: Option<PathBuf>,
    pub active_flows: usize,
}

impl InferenceProxyStatus {
    fn stopped() -> Self {
        Self {
            running: false,
            pid: None,
            base_url: None,
            capture_dir: None,
            capture_file: None,
            active_flows: 0,
        }
    }
}

/// Starts the one host-scoped proxy, or returns the already-running instance.
pub fn up(bind: SocketAddr, capture_dir: Option<PathBuf>) -> AppResult<InferenceProxyStatus> {
    validate_loopback(bind)?;
    if let Some(status) = query_control("status")? {
        return Ok(status);
    }

    let mut child = Command::new(std::env::current_exe()?)
        .args(["inference-proxy", "__serve", "--bind", &bind.to_string()])
        .args(
            capture_dir
                .as_ref()
                .map(|path| ["--capture-dir".into(), path.as_os_str().to_owned()])
                .into_iter()
                .flatten(),
        )
        .env_remove("OPENROUTER_API_KEY")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        if let Some(status) = query_control("status")? {
            return Ok(status);
        }
        if let Some(status) = child.try_wait()? {
            return Err(AppError::Config(format!(
                "inference proxy failed to start (child status {status})"
            )));
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(AppError::Config(
                "inference proxy did not become ready within 5 seconds".into(),
            ));
        }
        thread::sleep(Duration::from_millis(20));
    }
}

/// Returns host-scoped proxy state without starting it.
pub fn status() -> AppResult<InferenceProxyStatus> {
    Ok(query_control("status")?.unwrap_or_else(InferenceProxyStatus::stopped))
}

/// Stops an idle proxy. An active proxy flow is never interrupted by this command.
pub fn down() -> AppResult<InferenceProxyStatus> {
    let Some(status) = query_control("down")? else {
        return Ok(InferenceProxyStatus::stopped());
    };
    if status.running {
        return Err(AppError::Config(format!(
            "inference proxy has {} active flow(s); down refused",
            status.active_flows
        )));
    }
    Ok(status)
}

/// Runs the hidden foreground child used by [`up`].
pub fn serve(bind: SocketAddr, capture_dir: Option<PathBuf>) -> AppResult<()> {
    validate_loopback(bind)?;
    let paths = RuntimePaths::resolve()?;
    let _lock =
        HostProcessLock::acquire(paths.lock.clone(), LockMetadata::for_host(&paths.control))
            .map_err(|conflict| {
                AppError::Config(format!(
                    "inference proxy is already running: {}",
                    conflict.owner_summary()
                ))
            })?;
    let control = BoundControl::bind(paths.control)?;
    let listener = TcpListener::bind(bind)?;
    let capture = capture_dir.map(Capture::create).transpose()?.map(Arc::new);
    let proxy = Proxy::new(
        OPENROUTER_ORIGIN.into(),
        capture,
        CONNECT_TIMEOUT,
        STREAM_IDLE_TIMEOUT,
    );
    proxy.serve(listener, control)
}

fn validate_loopback(bind: SocketAddr) -> AppResult<()> {
    if bind.ip().is_loopback() {
        Ok(())
    } else {
        Err(AppError::Config(
            "inference proxy bind must be loopback".into(),
        ))
    }
}

#[derive(Debug)]
struct RuntimePaths {
    control: PathBuf,
    lock: PathBuf,
}

impl RuntimePaths {
    fn resolve() -> AppResult<Self> {
        let runtime_home = platonic_client::paths::runtime_home()?;
        ensure_private_directory(&runtime_home)?;
        let product = runtime_home.join("platonic");
        ensure_private_directory(&product)?;
        let root = product.join("inference-proxy");
        ensure_private_directory(&root)?;
        Ok(Self {
            control: root.join("control.sock"),
            lock: root.join("proxy.lock"),
        })
    }
}

fn ensure_private_directory(path: &Path) -> AppResult<()> {
    match fs::DirBuilder::new()
        .mode(PRIVATE_DIRECTORY_MODE)
        .create(path)
    {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir()
        || metadata.file_type().is_symlink()
        || metadata.uid() != rustix::process::geteuid().as_raw()
    {
        return Err(AppError::Config(format!(
            "inference proxy directory is not a current-user real directory: {}",
            path.display()
        )));
    }
    fs::set_permissions(path, fs::Permissions::from_mode(PRIVATE_DIRECTORY_MODE))?;
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir()
        || metadata.file_type().is_symlink()
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.mode() & 0o777 != PRIVATE_DIRECTORY_MODE
    {
        return Err(AppError::Config(format!(
            "inference proxy directory permissions could not be secured: {}",
            path.display()
        )));
    }
    Ok(())
}

struct BoundControl {
    listener: UnixListener,
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl BoundControl {
    fn bind(path: PathBuf) -> AppResult<Self> {
        match fs::symlink_metadata(&path) {
            Ok(metadata)
                if metadata.file_type().is_socket()
                    && metadata.uid() == rustix::process::geteuid().as_raw() =>
            {
                fs::remove_file(&path)?;
            }
            Ok(_) => {
                return Err(AppError::Config(format!(
                    "inference proxy control path is not a current-user socket: {}",
                    path.display()
                )));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let listener = UnixListener::bind(&path)?;
        fs::set_permissions(&path, fs::Permissions::from_mode(PRIVATE_FILE_MODE))?;
        let metadata = fs::symlink_metadata(&path)?;
        Ok(Self {
            listener,
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
}

impl Drop for BoundControl {
    fn drop(&mut self) {
        if let Ok(metadata) = fs::symlink_metadata(&self.path)
            && metadata.dev() == self.device
            && metadata.ino() == self.inode
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn query_control(command: &str) -> AppResult<Option<InferenceProxyStatus>> {
    let path = RuntimePaths::resolve()?.control;
    let mut stream = match UnixStream::connect(path) {
        Ok(stream) => stream,
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
            ) =>
        {
            return Ok(None);
        }
        Err(error) => return Err(error.into()),
    };
    stream.set_read_timeout(Some(CONTROL_TIMEOUT))?;
    stream.set_write_timeout(Some(CONTROL_TIMEOUT))?;
    stream.write_all(command.as_bytes())?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    let mut line = String::new();
    BufReader::new(stream)
        .take(64 * 1024)
        .read_line(&mut line)?;
    if line.is_empty() {
        return Ok(None);
    }
    Ok(Some(serde_json::from_str(line.trim())?))
}

#[derive(Clone)]
struct Proxy {
    upstream: String,
    capture: Option<Arc<Capture>>,
    active: Arc<AtomicUsize>,
    next_flow: Arc<AtomicU64>,
    connect_timeout: Duration,
    stream_idle_timeout: Duration,
}

impl Proxy {
    fn new(
        upstream: String,
        capture: Option<Arc<Capture>>,
        connect_timeout: Duration,
        stream_idle_timeout: Duration,
    ) -> Self {
        Self {
            upstream,
            capture,
            active: Arc::new(AtomicUsize::new(0)),
            next_flow: Arc::new(AtomicU64::new(1)),
            connect_timeout,
            stream_idle_timeout,
        }
    }

    fn serve(&self, listener: TcpListener, control: BoundControl) -> AppResult<()> {
        listener.set_nonblocking(true)?;
        control.listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let base_url = format!("http://{address}/api/v1");
        loop {
            match control.listener.accept() {
                Ok((stream, _)) => {
                    if self.handle_control(stream, &base_url)? {
                        return Ok(());
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(error.into()),
            }
            match listener.accept() {
                Ok((stream, _)) => {
                    self.active.fetch_add(1, Ordering::AcqRel);
                    let proxy = self.clone();
                    thread::spawn(move || {
                        let _guard = ActiveFlow::new(Arc::clone(&proxy.active));
                        proxy.handle_connection(stream);
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(error.into()),
            }
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn handle_control(&self, mut stream: UnixStream, base_url: &str) -> AppResult<bool> {
        stream.set_read_timeout(Some(CONTROL_TIMEOUT))?;
        stream.set_write_timeout(Some(CONTROL_TIMEOUT))?;
        let mut line = String::new();
        BufReader::new(stream.try_clone()?)
            .take(64)
            .read_line(&mut line)?;
        let active_flows = self.active.load(Ordering::Acquire);
        let stop = line.trim() == "down" && active_flows == 0;
        let capture_dir = self.capture.as_ref().map(|capture| capture.dir.clone());
        let status = InferenceProxyStatus {
            running: !stop,
            pid: (!stop).then(std::process::id),
            base_url: (!stop).then(|| base_url.to_owned()),
            capture_file: capture_dir.as_ref().map(|dir| dir.join(CAPTURE_FILE)),
            capture_dir,
            active_flows,
        };
        serde_json::to_writer(&mut stream, &status)?;
        stream.write_all(b"\n")?;
        stream.flush()?;
        Ok(stop)
    }

    fn handle_connection(&self, mut stream: TcpStream) {
        let _ = stream.set_read_timeout(Some(self.stream_idle_timeout));
        let _ = stream.set_write_timeout(Some(self.stream_idle_timeout));
        let (request, prefix) = match read_request_head(&mut stream) {
            Ok(request) => request,
            Err(error) => {
                let _ = write_error_response(&mut stream, error.status, error.message);
                return;
            }
        };
        let flow_id = format!("flow-{:08}", self.next_flow.fetch_add(1, Ordering::Relaxed));
        if !self.record(&flow_id, "flow_start", json!({}))
            || !self.record(
                &flow_id,
                "request_head",
                json!({
                    "method": request.method,
                    "path": request.path,
                    "protocol": request.protocol,
                    "headers": safe_request_headers(&request.headers),
                }),
            )
        {
            let _ = write_error_response(&mut stream, 500, "capture write failed");
            return;
        }

        let mut body = Vec::with_capacity(request.content_length.min(IO_CHUNK_BYTES));
        let mut request_hash = Sha256::new();
        if !self.accept_request_chunk(&flow_id, &mut body, &mut request_hash, &prefix) {
            let _ = write_error_response(&mut stream, 500, "capture write failed");
            return;
        }
        let mut buffer = [0; IO_CHUNK_BYTES];
        while body.len() < request.content_length {
            let remaining = request.content_length - body.len();
            let read_limit = remaining.min(buffer.len());
            match stream.read(&mut buffer[..read_limit]) {
                Ok(0) => {
                    self.record_terminal(
                        &flow_id,
                        "downstream_disconnect",
                        json!({"stage": "request_body", "bytes": body.len(), "sha256": digest_hex(&request_hash)}),
                    );
                    return;
                }
                Ok(read) => {
                    if !self.accept_request_chunk(
                        &flow_id,
                        &mut body,
                        &mut request_hash,
                        &buffer[..read],
                    ) {
                        let _ = write_error_response(&mut stream, 500, "capture write failed");
                        return;
                    }
                }
                Err(_) => {
                    self.record_terminal(
                        &flow_id,
                        "downstream_disconnect",
                        json!({"stage": "request_body", "bytes": body.len(), "sha256": digest_hex(&request_hash)}),
                    );
                    return;
                }
            }
        }
        if !self.record(
            &flow_id,
            "request_end",
            json!({"bytes": body.len(), "sha256": digest_hex(&request_hash)}),
        ) {
            let _ = write_error_response(&mut stream, 500, "capture write failed");
            return;
        }
        self.forward(request, body, flow_id, stream);
    }

    fn accept_request_chunk(
        &self,
        flow_id: &str,
        body: &mut Vec<u8>,
        hash: &mut Sha256,
        chunk: &[u8],
    ) -> bool {
        if chunk.is_empty() {
            return true;
        }
        let offset = body.len();
        body.extend_from_slice(chunk);
        hash.update(chunk);
        self.record(
            flow_id,
            "request_body_chunk",
            json!({"offset": offset, "bytes": chunk.len(), "hex": hex(chunk)}),
        )
    }

    fn forward(
        &self,
        request: ProxyRequest,
        body: Vec<u8>,
        flow_id: String,
        mut client: TcpStream,
    ) {
        let url = format!("{}{}", self.upstream, request.path);
        let agent = ureq::AgentBuilder::new()
            .try_proxy_from_env(false)
            .redirects(0)
            .max_idle_connections(0)
            .max_idle_connections_per_host(0)
            .timeout_connect(self.connect_timeout)
            .timeout_write(self.connect_timeout)
            .timeout_read(self.stream_idle_timeout)
            .build();
        let mut call = agent.post(&url);
        for (name, value) in &request.headers {
            if !request_header_is_hop_by_hop(name, &request.connection_tokens) {
                call = call.set(name, value);
            }
        }
        call = call.set("Accept-Encoding", "identity");
        let response = match call.send_bytes(&body) {
            Ok(response) => response,
            Err(ureq::Error::Status(_, response)) => response,
            Err(ureq::Error::Transport(error)) => {
                let timeout = transport_is_timeout(&error);
                self.record_terminal(
                    &flow_id,
                    if timeout { "timeout" } else { "upstream_error" },
                    json!({"stage": "response_head"}),
                );
                let _ = write_error_response(
                    &mut client,
                    if timeout { 504 } else { 502 },
                    if timeout {
                        "upstream timed out"
                    } else {
                        "upstream request failed"
                    },
                );
                return;
            }
        };

        let response_headers = response_headers(&response);
        if !self.record(
            &flow_id,
            "response_head",
            json!({
                "status": response.status(),
                "headers": safe_response_headers(&response_headers),
            }),
        ) {
            let _ = write_error_response(&mut client, 500, "capture write failed");
            return;
        }
        if write_response_head(
            &mut client,
            response.status(),
            response.status_text(),
            &response_headers,
        )
        .is_err()
        {
            self.record_terminal(
                &flow_id,
                "downstream_disconnect",
                json!({"stage": "response_head", "bytes": 0}),
            );
            return;
        }

        let mut reader = response.into_reader();
        let mut buffer = [0; IO_CHUNK_BYTES];
        let mut response_bytes = 0usize;
        let mut response_hash = Sha256::new();
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => {
                    let chunk = &buffer[..read];
                    if !self.record(
                        &flow_id,
                        "response_body_chunk",
                        json!({
                            "offset": response_bytes,
                            "bytes": read,
                            "hex": hex(chunk),
                        }),
                    ) {
                        return;
                    }
                    response_bytes += read;
                    response_hash.update(chunk);
                    if write_chunk(&mut client, chunk).is_err() {
                        self.record_terminal(
                            &flow_id,
                            "downstream_disconnect",
                            json!({"stage": "response_body", "bytes": response_bytes, "sha256": digest_hex(&response_hash)}),
                        );
                        return;
                    }
                }
                Err(error) => {
                    self.record_terminal(
                        &flow_id,
                        if matches!(
                            error.kind(),
                            io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                        ) {
                            "timeout"
                        } else {
                            "upstream_error"
                        },
                        json!({"stage": "response_body", "bytes": response_bytes, "sha256": digest_hex(&response_hash)}),
                    );
                    return;
                }
            }
        }
        if !self.record(
            &flow_id,
            "response_end",
            json!({"bytes": response_bytes, "sha256": digest_hex(&response_hash)}),
        ) {
            return;
        }
        let _ = client.write_all(b"0\r\n\r\n");
        let _ = client.flush();
    }

    fn record(&self, flow_id: &str, event: &str, fields: Value) -> bool {
        let Some(capture) = &self.capture else {
            return true;
        };
        if capture.record(flow_id, event, fields).is_ok() {
            true
        } else {
            let _ = capture.record(flow_id, "capture_write_failure", json!({"stage": event}));
            false
        }
    }

    fn record_terminal(&self, flow_id: &str, event: &str, fields: Value) {
        let _ = self.record(flow_id, event, fields);
    }
}

struct ActiveFlow {
    active: Arc<AtomicUsize>,
}

impl ActiveFlow {
    fn new(active: Arc<AtomicUsize>) -> Self {
        Self { active }
    }
}

impl Drop for ActiveFlow {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
    }
}

struct ClientRequestError {
    status: u16,
    message: &'static str,
}

struct ProxyRequest {
    method: String,
    path: String,
    protocol: String,
    headers: Vec<(String, String)>,
    connection_tokens: HashSet<String>,
    content_length: usize,
}

fn read_request_head(
    stream: &mut TcpStream,
) -> Result<(ProxyRequest, Vec<u8>), ClientRequestError> {
    let mut raw = Vec::with_capacity(4096);
    let end = loop {
        if let Some(end) = header_end(&raw) {
            break end;
        }
        if raw.len() >= MAX_HEADER_BYTES {
            return Err(ClientRequestError {
                status: 431,
                message: "request headers too large",
            });
        }
        let mut buffer = [0; 4096];
        let read_limit = buffer.len().min(MAX_HEADER_BYTES - raw.len());
        let read = stream
            .read(&mut buffer[..read_limit])
            .map_err(|_| ClientRequestError {
                status: 400,
                message: "request read failed",
            })?;
        if read == 0 {
            return Err(ClientRequestError {
                status: 400,
                message: "incomplete request head",
            });
        }
        raw.extend_from_slice(&buffer[..read]);
    };
    let mut parsed_headers = [httparse::EMPTY_HEADER; MAX_HEADER_COUNT];
    let mut parsed = httparse::Request::new(&mut parsed_headers);
    let parsed_end = match parsed.parse(&raw[..end]) {
        Ok(httparse::Status::Complete(end)) => end,
        _ => {
            return Err(ClientRequestError {
                status: 400,
                message: "malformed request head",
            });
        }
    };
    if parsed_end != end {
        return Err(ClientRequestError {
            status: 400,
            message: "malformed request head",
        });
    }
    let method = parsed.method.unwrap_or_default();
    if method != "POST" {
        return Err(ClientRequestError {
            status: 405,
            message: "only POST is admitted",
        });
    }
    let path = parsed.path.unwrap_or_default();
    if !matches!(path, "/api/v1/responses" | "/api/v1/chat/completions") {
        return Err(ClientRequestError {
            status: 404,
            message: "route is not admitted",
        });
    }
    let protocol = match parsed.version {
        Some(0) => "HTTP/1.0",
        Some(1) => "HTTP/1.1",
        _ => {
            return Err(ClientRequestError {
                status: 400,
                message: "unsupported HTTP version",
            });
        }
    };
    let mut headers = Vec::with_capacity(parsed.headers.len());
    for header in parsed.headers.iter() {
        let value = std::str::from_utf8(header.value).map_err(|_| ClientRequestError {
            status: 400,
            message: "request header is not UTF-8",
        })?;
        headers.push((header.name.to_owned(), value.to_owned()));
    }
    let lengths = header_values(&headers, "content-length").collect::<Vec<_>>();
    if lengths.len() != 1
        || !lengths[0].bytes().all(|byte| byte.is_ascii_digit())
        || lengths[0].is_empty()
    {
        return Err(ClientRequestError {
            status: 411,
            message: "one decimal Content-Length is required",
        });
    }
    let content_length = lengths[0]
        .parse::<usize>()
        .map_err(|_| ClientRequestError {
            status: 413,
            message: "request body too large",
        })?;
    if content_length > MAX_REQUEST_BYTES {
        return Err(ClientRequestError {
            status: 413,
            message: "request body too large",
        });
    }
    if header_values(&headers, "transfer-encoding")
        .next()
        .is_some()
    {
        return Err(ClientRequestError {
            status: 400,
            message: "Transfer-Encoding is not admitted",
        });
    }
    let content_types = header_values(&headers, "content-type").collect::<Vec<_>>();
    if content_types.len() != 1
        || !content_types[0]
            .split(';')
            .next()
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
    {
        return Err(ClientRequestError {
            status: 415,
            message: "Content-Type must be application/json",
        });
    }
    let connection_tokens = header_values(&headers, "connection")
        .flat_map(|value| value.split(','))
        .map(|value| value.trim().to_ascii_lowercase())
        .collect();
    let prefix = raw[end..].to_vec();
    if prefix.len() > content_length {
        return Err(ClientRequestError {
            status: 400,
            message: "request contains bytes after its body",
        });
    }
    Ok((
        ProxyRequest {
            method: method.into(),
            path: path.into(),
            protocol: protocol.into(),
            headers,
            connection_tokens,
            content_length,
        },
        prefix,
    ))
}

fn header_values<'a>(
    headers: &'a [(String, String)],
    name: &'a str,
) -> impl Iterator<Item = &'a str> {
    headers
        .iter()
        .filter(move |(candidate, _)| candidate.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn header_end(bytes: &[u8]) -> Option<usize> {
    bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| position + 4)
}

fn request_header_is_hop_by_hop(name: &str, connection_tokens: &HashSet<String>) -> bool {
    let lower = name.to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "host"
            | "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "content-length"
            | "accept-encoding"
    ) || connection_tokens.contains(&lower)
}

fn response_headers(response: &ureq::Response) -> Vec<(String, String)> {
    let connection_tokens = response
        .all("connection")
        .into_iter()
        .flat_map(|value| value.split(','))
        .map(|value| value.trim().to_ascii_lowercase())
        .collect::<HashSet<_>>();
    let mut seen = HashSet::new();
    let mut headers = Vec::new();
    for name in response.headers_names() {
        if !seen.insert(name.clone()) || response_header_is_hop_by_hop(&name, &connection_tokens) {
            continue;
        }
        for value in response.all(&name) {
            headers.push((name.clone(), value.to_owned()));
        }
    }
    headers
}

fn response_header_is_hop_by_hop(name: &str, connection_tokens: &HashSet<String>) -> bool {
    let lower = name.to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "content-length"
    ) || connection_tokens.contains(&lower)
}

fn safe_request_headers(headers: &[(String, String)]) -> Vec<Value> {
    safe_headers(headers, &["accept", "content-type", "user-agent"])
}

fn safe_response_headers(headers: &[(String, String)]) -> Vec<Value> {
    safe_headers(headers, &["content-type", "retry-after", "x-request-id"])
}

fn safe_headers(headers: &[(String, String)], allowed: &[&str]) -> Vec<Value> {
    headers
        .iter()
        .filter(|(name, _)| {
            allowed
                .iter()
                .any(|allowed| name.eq_ignore_ascii_case(allowed))
        })
        .map(|(name, value)| json!({"name": name.to_ascii_lowercase(), "value": value}))
        .collect()
}

fn write_response_head(
    stream: &mut TcpStream,
    status: u16,
    status_text: &str,
    headers: &[(String, String)],
) -> io::Result<()> {
    write!(stream, "HTTP/1.1 {status} {status_text}\r\n")?;
    for (name, value) in headers {
        write!(stream, "{name}: {value}\r\n")?;
    }
    stream.write_all(b"Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n")?;
    stream.flush()
}

fn write_chunk(stream: &mut TcpStream, chunk: &[u8]) -> io::Result<()> {
    write!(stream, "{:x}\r\n", chunk.len())?;
    stream.write_all(chunk)?;
    stream.write_all(b"\r\n")?;
    stream.flush()
}

fn write_error_response(stream: &mut TcpStream, status: u16, message: &str) -> io::Result<()> {
    let body = serde_json::to_vec(&json!({"error": message})).map_err(io::Error::other)?;
    write!(
        stream,
        "HTTP/1.1 {status} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        status_text(status),
        body.len()
    )?;
    stream.write_all(&body)
}

fn status_text(status: u16) -> &'static str {
    match status {
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        411 => "Length Required",
        413 => "Payload Too Large",
        415 => "Unsupported Media Type",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        504 => "Gateway Timeout",
        _ => "Error",
    }
}

fn transport_is_timeout(transport: &ureq::Transport) -> bool {
    let mut source = std::error::Error::source(transport);
    while let Some(error) = source {
        if let Some(error) = error.downcast_ref::<io::Error>()
            && matches!(
                error.kind(),
                io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
            )
        {
            return true;
        }
        source = error.source();
    }
    false
}

struct Capture {
    dir: PathBuf,
    started: Instant,
    writer: Mutex<CaptureWriter>,
}

struct CaptureWriter {
    file: File,
    seq: u64,
}

impl Capture {
    fn create(dir: PathBuf) -> AppResult<Self> {
        ensure_private_directory(&dir)?;
        if fs::read_dir(&dir)?.next().transpose()?.is_some() {
            return Err(AppError::Config(format!(
                "capture directory is not empty: {}",
                dir.display()
            )));
        }
        let file = OpenOptions::new()
            .append(true)
            .create_new(true)
            .mode(PRIVATE_FILE_MODE)
            .open(dir.join(CAPTURE_FILE))?;
        file.set_permissions(fs::Permissions::from_mode(PRIVATE_FILE_MODE))?;
        let metadata = file.metadata()?;
        if !metadata.file_type().is_file()
            || metadata.uid() != rustix::process::geteuid().as_raw()
            || metadata.mode() & 0o777 != PRIVATE_FILE_MODE
        {
            return Err(AppError::Config(
                "capture file permissions could not be secured".into(),
            ));
        }
        Ok(Self {
            dir,
            started: Instant::now(),
            writer: Mutex::new(CaptureWriter { file, seq: 0 }),
        })
    }

    fn record(&self, flow_id: &str, event: &str, fields: Value) -> io::Result<()> {
        let mut writer = self
            .writer
            .lock()
            .map_err(|_| io::Error::other("capture writer lock poisoned"))?;
        writer.seq += 1;
        let mut object = match fields {
            Value::Object(object) => object,
            _ => Map::new(),
        };
        object.insert("v".into(), CAPTURE_VERSION.into());
        object.insert("seq".into(), writer.seq.into());
        object.insert("flow_id".into(), flow_id.into());
        object.insert("wall_ms".into(), now_ms().into());
        object.insert(
            "delta_us".into(),
            u64::try_from(self.started.elapsed().as_micros())
                .unwrap_or(u64::MAX)
                .into(),
        );
        object.insert("event".into(), event.into());
        serde_json::to_writer(&mut writer.file, &object).map_err(io::Error::other)?;
        writer.file.write_all(b"\n")?;
        writer.file.flush()
    }
}

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

fn hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing to a string cannot fail");
    }
    output
}

fn digest_hex(hash: &Sha256) -> String {
    hex(&hash.clone().finalize())
}

/// Reconstructs captured requests and emits only the comparison fields owned by #641.
pub fn compare(dir: &Path) -> AppResult<Value> {
    let file = File::open(dir.join(CAPTURE_FILE))?;
    let mut flows = BTreeMap::<String, CapturedFlow>::new();
    let mut last_seq = 0;
    for line in BufReader::new(file).lines() {
        let event: Value = serde_json::from_str(&line?)?;
        let seq = event_u64(&event, "seq")?;
        if seq <= last_seq {
            return Err(AppError::Config(
                "capture sequence is not strictly increasing".into(),
            ));
        }
        last_seq = seq;
        let flow_id = event_string(&event, "flow_id")?.to_owned();
        flows.entry(flow_id).or_default().accept(&event, seq)?;
    }
    let mut compared = flows
        .into_iter()
        .filter_map(|(flow_id, flow)| flow.compare(flow_id).transpose())
        .collect::<AppResult<Vec<_>>>()?;
    compared.sort_by_key(|value| value["first_seq"].as_u64().unwrap_or(u64::MAX));
    Ok(json!({"v": CAPTURE_VERSION, "flows": compared}))
}

#[derive(Default)]
struct CapturedFlow {
    first_seq: u64,
    started_us: Option<u64>,
    path: Option<String>,
    protocol: Option<String>,
    user_agent: Option<String>,
    request: Vec<u8>,
    request_end: Option<(usize, String)>,
    response: Vec<u8>,
    response_head_us: Option<u64>,
    first_response_byte_us: Option<u64>,
    terminal_us: Option<u64>,
}

impl CapturedFlow {
    fn accept(&mut self, event: &Value, seq: u64) -> AppResult<()> {
        if self.first_seq == 0 {
            self.first_seq = seq;
        }
        match event_string(event, "event")? {
            "flow_start" => self.started_us = Some(event_u64(event, "delta_us")?),
            "request_head" => {
                self.path = Some(event_string(event, "path")?.into());
                self.protocol = Some(event_string(event, "protocol")?.into());
                self.user_agent = event["headers"].as_array().and_then(|headers| {
                    headers.iter().find_map(|header| {
                        (header["name"].as_str() == Some("user-agent"))
                            .then(|| header["value"].as_str().map(str::to_owned))
                            .flatten()
                    })
                });
            }
            "request_body_chunk" => append_captured_chunk(&mut self.request, event)?,
            "request_end" => {
                self.request_end = Some((
                    usize::try_from(event_u64(event, "bytes")?).map_err(|_| {
                        AppError::Config("captured request byte count overflows usize".into())
                    })?,
                    event_string(event, "sha256")?.into(),
                ));
            }
            "response_head" => self.response_head_us = Some(event_u64(event, "delta_us")?),
            "response_body_chunk" => {
                if self.first_response_byte_us.is_none() {
                    self.first_response_byte_us = Some(event_u64(event, "delta_us")?);
                }
                append_captured_chunk(&mut self.response, event)?;
            }
            "response_end"
            | "downstream_disconnect"
            | "upstream_error"
            | "timeout"
            | "capture_write_failure" => self.terminal_us = Some(event_u64(event, "delta_us")?),
            _ => {}
        }
        Ok(())
    }

    fn compare(self, flow_id: String) -> AppResult<Option<Value>> {
        let Some((bytes, expected_hash)) = self.request_end else {
            return Ok(None);
        };
        let actual_hash = hex(&Sha256::digest(&self.request));
        if bytes != self.request.len() || expected_hash != actual_hash {
            return Err(AppError::Config(format!(
                "captured request summary does not match chunks for {flow_id}"
            )));
        }
        let request: Value = serde_json::from_slice(&self.request)?;
        let path = self.path.unwrap_or_default();
        let protocol = match path.as_str() {
            "/api/v1/responses" => "responses",
            "/api/v1/chat/completions" => "chat_completions",
            _ => return Ok(None),
        };
        let semantics = request_semantics(&request, protocol);
        let usage = response_usage(&self.response);
        let started_us = self
            .started_us
            .ok_or_else(|| AppError::Config(format!("captured flow has no start: {flow_id}")))?;
        Ok(Some(json!({
            "first_seq": self.first_seq,
            "flow_id": flow_id,
            "protocol": protocol,
            "http_protocol": self.protocol,
            "user_agent": self.user_agent,
            "model": request.get("model").cloned().unwrap_or(Value::Null),
            "stream": request.get("stream").cloned().unwrap_or(Value::Bool(false)),
            "system_content": semantics.system,
            "developer_content": semantics.developer,
            "user_content": semantics.user,
            "prior_turn_count": semantics.prior_turn_count,
            "tools": semantics.tools,
            "model_settings": model_settings(&request),
            "request_bytes": bytes,
            "request_sha256": actual_hash,
            "response_usage": usage,
            "timing": {
                "response_head_us": self.response_head_us.map(|value| value.saturating_sub(started_us)),
                "first_response_byte_us": self.first_response_byte_us.map(|value| value.saturating_sub(started_us)),
                "terminal_us": self.terminal_us.map(|value| value.saturating_sub(started_us)),
            },
        })))
    }
}

fn append_captured_chunk(output: &mut Vec<u8>, event: &Value) -> AppResult<()> {
    let offset = usize::try_from(event_u64(event, "offset")?)
        .map_err(|_| AppError::Config("captured chunk offset overflows usize".into()))?;
    if offset != output.len() {
        return Err(AppError::Config(
            "captured body chunks are not contiguous".into(),
        ));
    }
    let bytes = decode_hex(event_string(event, "hex")?)?;
    if bytes.len() as u64 != event_u64(event, "bytes")? {
        return Err(AppError::Config(
            "captured chunk byte count does not match hex".into(),
        ));
    }
    output.extend_from_slice(&bytes);
    Ok(())
}

fn event_u64(event: &Value, field: &str) -> AppResult<u64> {
    event[field]
        .as_u64()
        .ok_or_else(|| AppError::Config(format!("capture event is missing integer {field}")))
}

fn event_string<'a>(event: &'a Value, field: &str) -> AppResult<&'a str> {
    event[field]
        .as_str()
        .ok_or_else(|| AppError::Config(format!("capture event is missing string {field}")))
}

fn decode_hex(value: &str) -> AppResult<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return Err(AppError::Config("captured hex has odd length".into()));
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).expect("hex source is UTF-8");
            u8::from_str_radix(text, 16)
                .map_err(|_| AppError::Config("captured body contains invalid hex".into()))
        })
        .collect()
}

#[derive(Default)]
struct RequestSemantics {
    system: Vec<String>,
    developer: Vec<String>,
    user: Vec<String>,
    user_message_count: usize,
    prior_turn_count: usize,
    tools: Vec<Value>,
}

fn request_semantics(request: &Value, protocol: &str) -> RequestSemantics {
    let mut result = RequestSemantics::default();
    if protocol == "responses" {
        collect_content(request.get("instructions"), &mut result.developer);
        match request.get("input") {
            Some(Value::Array(items)) => collect_messages(items, &mut result),
            input => {
                if input.is_some_and(|value| !value.is_null()) {
                    result.user_message_count = 1;
                }
                collect_content(input, &mut result.user);
            }
        }
    } else if let Some(messages) = request.get("messages").and_then(Value::as_array) {
        collect_messages(messages, &mut result);
    }
    result.prior_turn_count = result.user_message_count.saturating_sub(1);
    result.tools = request
        .get("tools")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(tool_semantics)
        .collect();
    result
}

fn collect_messages(messages: &[Value], result: &mut RequestSemantics) {
    for message in messages {
        let role = message.get("role").and_then(Value::as_str);
        if role == Some("user") {
            result.user_message_count += 1;
        }
        let destination = match role {
            Some("system") => &mut result.system,
            Some("developer") => &mut result.developer,
            Some("user") => &mut result.user,
            _ => continue,
        };
        collect_content(message.get("content"), destination);
    }
}

fn collect_content(content: Option<&Value>, output: &mut Vec<String>) {
    match content {
        Some(Value::String(text)) => output.push(text.clone()),
        Some(Value::Array(parts)) => {
            for part in parts {
                if let Some(text) = part.as_str().or_else(|| part.get("text")?.as_str()) {
                    output.push(text.to_owned());
                }
            }
        }
        Some(Value::Object(object)) => {
            if let Some(text) = object.get("text").and_then(Value::as_str) {
                output.push(text.to_owned());
            }
        }
        _ => {}
    }
}

fn tool_semantics(tool: &Value) -> Option<Value> {
    let function = tool.get("function").unwrap_or(tool);
    let name = function.get("name")?.clone();
    let schema = function
        .get("parameters")
        .or_else(|| function.get("input_schema"))
        .cloned()
        .unwrap_or(Value::Null);
    Some(json!({"name": name, "schema": schema}))
}

fn model_settings(request: &Value) -> Value {
    let mut settings = Map::new();
    for field in [
        "temperature",
        "top_p",
        "max_tokens",
        "max_completion_tokens",
        "max_output_tokens",
        "reasoning_effort",
        "reasoning",
        "tool_choice",
        "parallel_tool_calls",
        "truncation",
        "text",
    ] {
        if let Some(value) = request.get(field) {
            settings.insert(field.into(), value.clone());
        }
    }
    Value::Object(settings)
}

fn response_usage(body: &[u8]) -> Value {
    if let Ok(value) = serde_json::from_slice::<Value>(body)
        && let Some(usage) = value
            .get("usage")
            .or_else(|| value.pointer("/response/usage"))
    {
        return usage.clone();
    }
    let mut usage = Value::Null;
    for line in body.split(|byte| *byte == b'\n') {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let Some(data) = line.strip_prefix(b"data:") else {
            continue;
        };
        let data = data.strip_prefix(b" ").unwrap_or(data);
        if data == b"[DONE]" {
            continue;
        }
        if let Ok(value) = serde_json::from_slice::<Value>(data)
            && let Some(found) = value
                .get("usage")
                .or_else(|| value.pointer("/response/usage"))
        {
            usage = found.clone();
        }
    }
    usage
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{net::Shutdown, time::Duration};

    #[derive(Clone)]
    struct ObservedRequest {
        head: String,
        body: Vec<u8>,
    }

    enum UpstreamScript {
        Response { status: u16, chunks: Vec<Vec<u8>> },
        Malformed,
        Partial(Vec<u8>),
        Stall(Duration),
    }

    #[test]
    fn inference_proxy_forwards_both_protocols_streaming_and_non_streaming() {
        let root = tempfile::tempdir().unwrap();
        let capture_dir = root.path().join("capture");
        let capture = Arc::new(Capture::create(capture_dir.clone()).unwrap());
        let replies = [
            (201, br#"{"id":"response-a","usage":{"input_tokens":3}}"#.to_vec()),
            (200, br#"{"id":"chat-a","usage":{"prompt_tokens":4}}"#.to_vec()),
            (
                200,
                b"data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":5}}}\n\ndata: [DONE]\n\n".to_vec(),
            ),
            (
                503,
                b"data: {\"usage\":{\"prompt_tokens\":6}}\n\ndata: [DONE]\n\n".to_vec(),
            ),
        ];
        let scripts = replies
            .iter()
            .map(|(status, body)| UpstreamScript::Response {
                status: *status,
                chunks: body.chunks(11).map(<[u8]>::to_vec).collect(),
            })
            .collect();
        let (upstream, observed, upstream_thread) = spawn_upstream(scripts);
        let (proxy, proxy_thread) = spawn_proxy(
            upstream,
            Some(Arc::clone(&capture)),
            Duration::from_secs(2),
            replies.len(),
        );
        let requests = [
            (
                "/api/v1/responses",
                json!({"model":"a","stream":false,"input":"one"}),
            ),
            (
                "/api/v1/chat/completions",
                json!({"model":"b","stream":false,"messages":[{"role":"user","content":"two"}]}),
            ),
            (
                "/api/v1/responses",
                json!({"model":"c","stream":true,"input":"three"}),
            ),
            (
                "/api/v1/chat/completions",
                json!({"model":"d","stream":true,"messages":[{"role":"user","content":"four"}]}),
            ),
        ];
        let authorization = "Bearer credential-header-sentinel";
        let api_key = "api-key-header-sentinel";
        for (index, (path, request)) in requests.iter().enumerate() {
            let request_body = serde_json::to_vec(request).unwrap();
            let result = ureq::post(&format!("http://{proxy}{path}"))
                .set("Authorization", authorization)
                .set("X-Api-Key", api_key)
                .set("Content-Type", "application/json")
                .set("User-Agent", "proxy-contract-test")
                .send_bytes(&request_body);
            let response = match result {
                Ok(response) => response,
                Err(ureq::Error::Status(_, response)) => response,
                Err(error) => panic!("proxy request failed: {error}"),
            };
            assert_eq!(response.status(), replies[index].0);
            assert_eq!(
                response.header("Set-Cookie"),
                Some("response-secret-sentinel")
            );
            let mut response_body = Vec::new();
            response
                .into_reader()
                .read_to_end(&mut response_body)
                .unwrap();
            assert_eq!(response_body, replies[index].1);
        }
        proxy_thread.join().unwrap();
        upstream_thread.join().unwrap();

        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), requests.len(), "the proxy added a retry");
        for (index, request) in observed.iter().enumerate() {
            assert!(
                request
                    .head
                    .starts_with(&format!("POST {} ", requests[index].0))
            );
            assert!(request.head.contains(authorization));
            assert!(request.head.contains(api_key));
            assert!(
                request
                    .head
                    .to_ascii_lowercase()
                    .contains("accept-encoding: identity")
            );
            assert_eq!(
                request.body,
                serde_json::to_vec(&requests[index].1).unwrap()
            );
        }
        drop(observed);

        let raw_capture = fs::read(capture_dir.join(CAPTURE_FILE)).unwrap();
        assert!(
            !raw_capture
                .windows(authorization.len())
                .any(|part| part == authorization.as_bytes())
        );
        assert!(
            !raw_capture
                .windows(api_key.len())
                .any(|part| part == api_key.as_bytes())
        );
        assert!(
            !raw_capture
                .windows("response-secret-sentinel".len())
                .any(|part| part == b"response-secret-sentinel")
        );
        let events = capture_events(&capture_dir);
        assert_eq!(
            events
                .iter()
                .filter(|event| event["event"] == "flow_start")
                .count(),
            requests.len()
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event["event"] == "response_end")
                .count(),
            requests.len()
        );
        for (event, (_, body)) in events
            .iter()
            .filter(|event| event["event"] == "response_end")
            .zip(&replies)
        {
            assert_eq!(event["bytes"], body.len());
            assert_eq!(event["sha256"], hex(&Sha256::digest(body)));
        }
        for (index, event) in events.iter().enumerate() {
            assert_eq!(event["seq"], u64::try_from(index + 1).unwrap());
            assert_eq!(event["v"], CAPTURE_VERSION);
        }
        let compared = compare(&capture_dir).unwrap();
        assert_eq!(compared["flows"].as_array().unwrap().len(), requests.len());
        for (index, flow) in compared["flows"].as_array().unwrap().iter().enumerate() {
            let body = serde_json::to_vec(&requests[index].1).unwrap();
            assert_eq!(flow["request_bytes"], body.len());
            assert_eq!(flow["request_sha256"], hex(&Sha256::digest(body)));
        }
        assert_eq!(
            fs::symlink_metadata(&capture_dir)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            PRIVATE_DIRECTORY_MODE
        );
        assert_eq!(
            fs::symlink_metadata(capture_dir.join(CAPTURE_FILE))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            PRIVATE_FILE_MODE
        );
    }

    #[test]
    fn inference_proxy_records_errors_timeouts_partial_sse_and_disconnects() {
        let cases = [
            (UpstreamScript::Malformed, "upstream_error", 502),
            (
                UpstreamScript::Stall(Duration::from_millis(250)),
                "timeout",
                504,
            ),
        ];
        for (index, (script, terminal, status)) in cases.into_iter().enumerate() {
            let root = tempfile::tempdir().unwrap();
            let capture_dir = root.path().join(format!("capture-{index}"));
            let capture = Arc::new(Capture::create(capture_dir.clone()).unwrap());
            let (upstream, _, upstream_thread) = spawn_upstream(vec![script]);
            let (proxy, proxy_thread) =
                spawn_proxy(upstream, Some(capture), Duration::from_millis(75), 1);
            let response = ureq::post(&format!("http://{proxy}/api/v1/responses"))
                .set("Content-Type", "application/json")
                .send_bytes(br#"{"model":"test","input":"marker"}"#)
                .unwrap_err();
            assert_eq!(response.into_response().unwrap().status(), status);
            proxy_thread.join().unwrap();
            upstream_thread.join().unwrap();
            assert!(
                capture_events(&capture_dir)
                    .iter()
                    .any(|event| event["event"] == terminal)
            );
        }

        let root = tempfile::tempdir().unwrap();
        let capture_dir = root.path().join("partial");
        let capture = Arc::new(Capture::create(capture_dir.clone()).unwrap());
        let partial = b"data: {\"delta\":\"partial\"}\n\n".to_vec();
        let (upstream, _, upstream_thread) =
            spawn_upstream(vec![UpstreamScript::Partial(partial.clone())]);
        let (proxy, proxy_thread) = spawn_proxy(upstream, Some(capture), Duration::from_secs(1), 1);
        let response = ureq::post(&format!("http://{proxy}/api/v1/responses"))
            .set("Content-Type", "application/json")
            .send_bytes(br#"{"model":"test","stream":true,"input":"marker"}"#)
            .unwrap();
        let mut received = Vec::new();
        assert!(response.into_reader().read_to_end(&mut received).is_err());
        assert_eq!(received, partial);
        proxy_thread.join().unwrap();
        upstream_thread.join().unwrap();
        let events = capture_events(&capture_dir);
        assert!(
            events
                .iter()
                .any(|event| event["event"] == "response_body_chunk")
        );
        assert!(
            events
                .iter()
                .any(|event| event["event"] == "upstream_error")
        );

        let root = tempfile::tempdir().unwrap();
        let capture_dir = root.path().join("disconnect");
        let capture = Arc::new(Capture::create(capture_dir.clone()).unwrap());
        let (upstream, _, upstream_thread) = spawn_upstream(vec![UpstreamScript::Response {
            status: 200,
            chunks: vec![vec![b'x'; 512 * 1024]],
        }]);
        let (proxy, proxy_thread) = spawn_proxy(upstream, Some(capture), Duration::from_secs(1), 1);
        let mut client = TcpStream::connect(proxy).unwrap();
        client
            .write_all(
                b"POST /api/v1/responses HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}",
            )
            .unwrap();
        rustix::net::sockopt::set_socket_linger(&client, Some(Duration::ZERO)).unwrap();
        client.shutdown(Shutdown::Both).unwrap();
        drop(client);
        proxy_thread.join().unwrap();
        upstream_thread.join().unwrap();
        assert!(
            capture_events(&capture_dir)
                .iter()
                .any(|event| event["event"] == "downstream_disconnect")
        );
    }

    #[test]
    fn inference_proxy_capture_off_and_capture_failure_are_closed() {
        let root = tempfile::tempdir().unwrap();
        let off_path = root.path().join("capture-off");
        let (upstream, _, upstream_thread) = spawn_upstream(vec![UpstreamScript::Response {
            status: 200,
            chunks: vec![b"{}".to_vec()],
        }]);
        let (proxy, proxy_thread) = spawn_proxy(upstream, None, Duration::from_secs(1), 1);
        let response = ureq::post(&format!("http://{proxy}/api/v1/chat/completions"))
            .set("Content-Type", "application/json")
            .send_bytes(b"{}")
            .unwrap();
        assert_eq!(response.status(), 200);
        proxy_thread.join().unwrap();
        upstream_thread.join().unwrap();
        assert!(!off_path.exists());

        let upstream = TcpListener::bind("127.0.0.1:0").unwrap();
        upstream.set_nonblocking(true).unwrap();
        let dir = root.path().join("capture-failure");
        fs::create_dir(&dir).unwrap();
        let capture = Arc::new(Capture {
            dir,
            started: Instant::now(),
            writer: Mutex::new(CaptureWriter {
                file: OpenOptions::new().write(true).open("/dev/full").unwrap(),
                seq: 0,
            }),
        });
        let (proxy, proxy_thread) = spawn_proxy(
            format!("http://{}", upstream.local_addr().unwrap()),
            Some(capture),
            Duration::from_secs(1),
            1,
        );
        let error = ureq::post(&format!("http://{proxy}/api/v1/responses"))
            .set("Content-Type", "application/json")
            .send_bytes(b"{}")
            .unwrap_err();
        assert_eq!(error.into_response().unwrap().status(), 500);
        proxy_thread.join().unwrap();
        assert!(matches!(
            upstream.accept().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        ));
    }

    #[test]
    fn inference_proxy_compare_extracts_responses_and_chat_semantics() {
        let root = tempfile::tempdir().unwrap();
        let capture = Capture::create(root.path().join("capture")).unwrap();
        let fixtures = [
            (
                "flow-00000001",
                "/api/v1/responses",
                json!({
                    "model": "model-a",
                    "stream": true,
                    "instructions": "developer-a",
                    "input": [{"role":"user","content":[{"type":"input_text","text":"user-a"}]}],
                    "tools": [{"type":"function","name":"read","parameters":{"type":"object"}}],
                    "reasoning": {"effort":"medium"}
                }),
            ),
            (
                "flow-00000002",
                "/api/v1/chat/completions",
                json!({
                    "model": "model-b",
                    "stream": false,
                    "messages": [
                        {"role":"system","content":"system-b"},
                        {"role":"user","content":"prior-b"},
                        {"role":"assistant","content":"answer-b"},
                        {"role":"user","content":"user-b"}
                    ],
                    "tools": [{"type":"function","function":{"name":"write","parameters":{"type":"object"}}}],
                    "temperature": 0
                }),
            ),
        ];
        for (flow, path, request) in fixtures {
            let body = serde_json::to_vec(&request).unwrap();
            capture.record(flow, "flow_start", json!({})).unwrap();
            capture
                .record(
                    flow,
                    "request_head",
                    json!({"path":path,"protocol":"HTTP/1.1","headers":[]}),
                )
                .unwrap();
            capture
                .record(
                    flow,
                    "request_body_chunk",
                    json!({"offset":0,"bytes":body.len(),"hex":hex(&body)}),
                )
                .unwrap();
            capture
                .record(
                    flow,
                    "request_end",
                    json!({"bytes":body.len(),"sha256":hex(&Sha256::digest(&body))}),
                )
                .unwrap();
        }

        let compared = compare(&capture.dir).unwrap();
        assert_eq!(compared["flows"][0]["protocol"], "responses");
        assert_eq!(compared["flows"][0]["developer_content"][0], "developer-a");
        assert_eq!(compared["flows"][0]["tools"][0]["name"], "read");
        assert_eq!(compared["flows"][1]["protocol"], "chat_completions");
        assert_eq!(compared["flows"][1]["system_content"][0], "system-b");
        assert_eq!(compared["flows"][1]["prior_turn_count"], 1);
        assert_eq!(compared["flows"][1]["model_settings"]["temperature"], 0);
    }

    #[test]
    fn inference_proxy_rejects_non_loopback_and_unsafe_capture_paths() {
        assert!(validate_loopback("0.0.0.0:0".parse().unwrap()).is_err());
        let root = tempfile::tempdir().unwrap();
        let capture = root.path().join("capture");
        fs::create_dir(&capture).unwrap();
        fs::write(capture.join("existing"), b"keep").unwrap();
        assert!(Capture::create(capture).is_err());
    }

    fn spawn_proxy(
        upstream: String,
        capture: Option<Arc<Capture>>,
        timeout: Duration,
        connections: usize,
    ) -> (SocketAddr, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let proxy = Proxy::new(upstream, capture, timeout, timeout);
        let handle = thread::spawn(move || {
            for _ in 0..connections {
                let (stream, _) = listener.accept().unwrap();
                proxy.handle_connection(stream);
            }
        });
        (address, handle)
    }

    fn spawn_upstream(
        scripts: Vec<UpstreamScript>,
    ) -> (
        String,
        Arc<Mutex<Vec<ObservedRequest>>>,
        thread::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let observed = Arc::new(Mutex::new(Vec::new()));
        let output = Arc::clone(&observed);
        let handle = thread::spawn(move || {
            for script in scripts {
                let (mut stream, _) = listener.accept().unwrap();
                let request = read_upstream_request(&mut stream);
                output.lock().unwrap().push(request);
                match script {
                    UpstreamScript::Response { status, chunks } => {
                        let length = chunks.iter().map(Vec::len).sum::<usize>();
                        write!(
                            stream,
                            "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nSet-Cookie: response-secret-sentinel\r\nContent-Length: {length}\r\n\r\n"
                        )
                        .unwrap();
                        for chunk in chunks {
                            if stream.write_all(&chunk).is_err() {
                                break;
                            }
                            let _ = stream.flush();
                            thread::sleep(Duration::from_millis(2));
                        }
                    }
                    UpstreamScript::Malformed => {
                        stream.write_all(b"not an HTTP response\r\n\r\n").unwrap();
                    }
                    UpstreamScript::Partial(body) => {
                        write!(
                            stream,
                            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\n\r\n",
                            body.len() + 20
                        )
                        .unwrap();
                        stream.write_all(&body).unwrap();
                    }
                    UpstreamScript::Stall(duration) => thread::sleep(duration),
                }
            }
        });
        (url, observed, handle)
    }

    fn read_upstream_request(stream: &mut TcpStream) -> ObservedRequest {
        let mut raw = Vec::new();
        let end = loop {
            if let Some(end) = header_end(&raw) {
                break end;
            }
            let mut buffer = [0; 4096];
            let read = stream.read(&mut buffer).unwrap();
            assert!(read > 0);
            raw.extend_from_slice(&buffer[..read]);
        };
        let head = String::from_utf8(raw[..end].to_vec()).unwrap();
        let content_length = head
            .to_ascii_lowercase()
            .lines()
            .find_map(|line| line.strip_prefix("content-length:"))
            .unwrap()
            .trim()
            .parse::<usize>()
            .unwrap();
        let mut body = raw[end..].to_vec();
        body.resize(content_length, 0);
        stream.read_exact(&mut body[raw.len() - end..]).unwrap();
        ObservedRequest { head, body }
    }

    fn capture_events(dir: &Path) -> Vec<Value> {
        BufReader::new(File::open(dir.join(CAPTURE_FILE)).unwrap())
            .lines()
            .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
            .collect()
    }
}
