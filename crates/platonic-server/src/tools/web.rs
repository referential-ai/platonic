use crate::{AppError, AppResult};
use html2text::render::{PlainDecorator, TextDecorator};
use platonic_core::{ResultVisibility, ToolCallId, ToolResult};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    io::{self, Read},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs},
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};
use thiserror::Error;
use url::{Host, Url};

const MAX_URL_BYTES: usize = 2_048;
const MAX_BODY_BYTES: usize = 1024 * 1024;
const MAX_RETURNED_BYTES: usize = 48 * 1024;
const MAX_REDIRECTS: u8 = 3;
const HTML_WIDTH: usize = 120;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const OPERATION_TIMEOUT: Duration = Duration::from_secs(20);
const READ_POLL_TIMEOUT: Duration = Duration::from_secs(1);
const CLOUD_METADATA_NAMES: &[&str] = &[
    "instance-data",
    "instance-data.ec2.internal",
    "metadata",
    "metadata.goog",
    "metadata.google.internal",
];

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WebFetchInput {
    url: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ValidatedUrl {
    url: Url,
    normalized: String,
    scheme: String,
    host: String,
    port: u16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ResolvedAddress {
    validated_ip: IpAddr,
    connect_addr: SocketAddr,
}

#[derive(Clone, Copy, Debug)]
struct FetchBounds {
    connect: Duration,
    operation: Duration,
    read_poll: Duration,
}

impl Default for FetchBounds {
    fn default() -> Self {
        Self {
            connect: CONNECT_TIMEOUT,
            operation: OPERATION_TIMEOUT,
            read_poll: READ_POLL_TIMEOUT,
        }
    }
}

trait HostResolver {
    fn resolve(&self, host: &str, port: u16) -> io::Result<Vec<ResolvedAddress>>;
}

struct SystemResolver;

impl HostResolver for SystemResolver {
    fn resolve(&self, host: &str, port: u16) -> io::Result<Vec<ResolvedAddress>> {
        let socket_addrs = if let Ok(ip) = host.parse::<IpAddr>() {
            vec![SocketAddr::new(ip, port)]
        } else {
            (host, port).to_socket_addrs()?.collect()
        };
        Ok(socket_addrs
            .into_iter()
            .map(|connect_addr| ResolvedAddress {
                validated_ip: connect_addr.ip(),
                connect_addr,
            })
            .collect())
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub(crate) enum WebFetchError {
    #[error("web.fetch input must contain only a URL string")]
    InvalidInput,
    #[error("web.fetch URL exceeds the 2048-byte limit")]
    UrlTooLong,
    #[error("web.fetch URL must be an absolute HTTP(S) URL")]
    InvalidUrl,
    #[error("web.fetch URL authority is not canonical")]
    NonCanonicalAuthority,
    #[error("web.fetch URL userinfo is forbidden")]
    UserinfoForbidden,
    #[error("web.fetch destination name is forbidden")]
    MetadataNameForbidden,
    #[error("web.fetch destination resolution failed")]
    ResolutionFailed,
    #[error("web.fetch destination resolved to no addresses")]
    NoAddresses,
    #[error("web.fetch destination address is not globally routable: {0}")]
    DeniedAddress(IpAddr),
    #[error("web.fetch connection timed out")]
    ConnectTimeout,
    #[error("web.fetch operation timed out")]
    OperationTimeout,
    #[error("web.fetch was canceled")]
    Canceled,
    #[error("web.fetch transport failed")]
    Transport,
    #[error("web.fetch returned HTTP status {0}")]
    HttpStatus(u16),
    #[error("web.fetch redirect is missing a valid Location")]
    InvalidRedirect,
    #[error("web.fetch redirect changed the approved origin")]
    CrossOriginRedirect,
    #[error("web.fetch exceeded the three-redirect limit")]
    TooManyRedirects,
    #[error("web.fetch response is missing a supported Content-Type")]
    MissingMediaType,
    #[error("web.fetch response media type is unsupported")]
    UnsupportedMediaType,
    #[error("web.fetch response charset must be UTF-8")]
    UnsupportedCharset,
    #[error("web.fetch response Content-Length is invalid")]
    InvalidContentLength,
    #[error("web.fetch response body exceeds the 1048576-byte limit")]
    BodyTooLarge,
    #[error("web.fetch response body is not valid UTF-8")]
    InvalidUtf8,
    #[error("web.fetch HTML conversion failed")]
    HtmlConversion,
}

pub(super) fn approval_preview(input: &Value) -> Result<String, WebFetchError> {
    let input = parse_input(input)?;
    let target = validate_url(&input.url)?;
    let started = Instant::now();
    let addresses = resolve_and_validate(
        &SystemResolver,
        &target,
        started,
        FetchBounds::default(),
        None,
    )?;
    Ok(format_approval_preview(&target, &addresses))
}

pub(super) fn fetch(
    call_id: ToolCallId,
    input: Value,
    cancel: Option<&AtomicBool>,
) -> AppResult<ToolResult> {
    let input = parse_input(&input).map_err(tool_error)?;
    let target = validate_url(&input.url).map_err(tool_error)?;
    fetch_with(
        call_id,
        target,
        &SystemResolver,
        cancel,
        FetchBounds::default(),
    )
    .map_err(tool_error)
}

fn tool_error(error: WebFetchError) -> AppError {
    AppError::Tool(error.to_string())
}

fn parse_input(input: &Value) -> Result<WebFetchInput, WebFetchError> {
    serde_json::from_value(input.clone()).map_err(|_| WebFetchError::InvalidInput)
}

fn validate_url(raw: &str) -> Result<ValidatedUrl, WebFetchError> {
    if raw.len() > MAX_URL_BYTES {
        return Err(WebFetchError::UrlTooLong);
    }
    if raw.is_empty()
        || raw
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
    {
        return Err(WebFetchError::InvalidUrl);
    }
    if raw.contains('\\') {
        return Err(WebFetchError::NonCanonicalAuthority);
    }

    let raw_authority = raw_authority(raw)?;
    if raw_authority.contains('@') {
        return Err(WebFetchError::UserinfoForbidden);
    }
    let (raw_host, raw_port) = split_raw_authority(raw_authority)?;

    let mut url = Url::parse(raw).map_err(|_| WebFetchError::InvalidUrl)?;
    if !matches!(url.scheme(), "http" | "https") || !url.has_host() || url.cannot_be_a_base() {
        return Err(WebFetchError::InvalidUrl);
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(WebFetchError::UserinfoForbidden);
    }

    let host = match url.host().ok_or(WebFetchError::InvalidUrl)? {
        Host::Domain(host) => {
            validate_domain(host)?;
            if raw_host.ends_with('.') || raw_host.contains('%') {
                return Err(WebFetchError::NonCanonicalAuthority);
            }
            host.to_ascii_lowercase()
        }
        Host::Ipv4(address) => {
            if raw_host.parse::<Ipv4Addr>().ok() != Some(address) || raw_host != address.to_string()
            {
                return Err(WebFetchError::NonCanonicalAuthority);
            }
            address.to_string()
        }
        Host::Ipv6(address) => {
            if raw_host.parse::<Ipv6Addr>().ok() != Some(address)
                || raw_host.to_ascii_lowercase() != address.to_string()
            {
                return Err(WebFetchError::NonCanonicalAuthority);
            }
            address.to_string()
        }
    };

    if CLOUD_METADATA_NAMES.contains(&host.as_str()) {
        return Err(WebFetchError::MetadataNameForbidden);
    }
    validate_raw_port(raw_port)?;
    let port = url
        .port_or_known_default()
        .ok_or(WebFetchError::InvalidUrl)?;
    if port == 0 {
        return Err(WebFetchError::NonCanonicalAuthority);
    }

    url.set_fragment(None);
    let normalized = url.to_string();
    if normalized.len() > MAX_URL_BYTES {
        return Err(WebFetchError::UrlTooLong);
    }

    Ok(ValidatedUrl {
        scheme: url.scheme().to_owned(),
        url,
        normalized,
        host,
        port,
    })
}

fn raw_authority(raw: &str) -> Result<&str, WebFetchError> {
    let (_, after_scheme) = raw.split_once("://").ok_or(WebFetchError::InvalidUrl)?;
    let end = after_scheme
        .find(['/', '?', '#'])
        .unwrap_or(after_scheme.len());
    let authority = &after_scheme[..end];
    if authority.is_empty() {
        return Err(WebFetchError::InvalidUrl);
    }
    Ok(authority)
}

fn split_raw_authority(authority: &str) -> Result<(&str, Option<&str>), WebFetchError> {
    if let Some(after_open) = authority.strip_prefix('[') {
        let close = after_open
            .find(']')
            .ok_or(WebFetchError::NonCanonicalAuthority)?;
        let host = &after_open[..close];
        let remainder = &after_open[close + 1..];
        let port = if remainder.is_empty() {
            None
        } else {
            Some(
                remainder
                    .strip_prefix(':')
                    .ok_or(WebFetchError::NonCanonicalAuthority)?,
            )
        };
        return Ok((host, port));
    }

    if authority.matches(':').count() > 1 {
        return Err(WebFetchError::NonCanonicalAuthority);
    }
    Ok(match authority.rsplit_once(':') {
        Some((host, port)) => (host, Some(port)),
        None => (authority, None),
    })
}

fn validate_domain(host: &str) -> Result<(), WebFetchError> {
    if host.is_empty() || host.len() > 253 {
        return Err(WebFetchError::NonCanonicalAuthority);
    }
    for label in host.split('.') {
        if label.is_empty()
            || label.len() > 63
            || label.starts_with('-')
            || label.ends_with('-')
            || !label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(WebFetchError::NonCanonicalAuthority);
        }
    }
    Ok(())
}

fn validate_raw_port(port: Option<&str>) -> Result<(), WebFetchError> {
    let Some(port) = port else {
        return Ok(());
    };
    let parsed = port
        .parse::<u16>()
        .map_err(|_| WebFetchError::NonCanonicalAuthority)?;
    if parsed == 0 || parsed.to_string() != port {
        return Err(WebFetchError::NonCanonicalAuthority);
    }
    Ok(())
}

fn format_approval_preview(target: &ValidatedUrl, addresses: &[ResolvedAddress]) -> String {
    let mut ips = addresses
        .iter()
        .map(|address| address.validated_ip.to_string())
        .collect::<Vec<_>>();
    ips.sort();
    ips.dedup();
    format!(
        "url: {}\norigin: {}\naddresses: {}\neffect: Network",
        target.normalized,
        normalized_origin(target),
        ips.join(", ")
    )
}

fn normalized_origin(target: &ValidatedUrl) -> String {
    let default_port = match target.scheme.as_str() {
        "http" => 80,
        "https" => 443,
        _ => target.port,
    };
    let host = if target.host.parse::<Ipv6Addr>().is_ok() {
        format!("[{}]", target.host)
    } else {
        target.host.clone()
    };
    if target.port == default_port {
        format!("{}://{host}", target.scheme)
    } else {
        format!("{}://{host}:{}", target.scheme, target.port)
    }
}

fn resolve_and_validate<R: HostResolver>(
    resolver: &R,
    target: &ValidatedUrl,
    started: Instant,
    bounds: FetchBounds,
    cancel: Option<&AtomicBool>,
) -> Result<Vec<ResolvedAddress>, WebFetchError> {
    check_active(started, bounds, cancel)?;
    let connect_started = Instant::now();
    let mut addresses = resolver
        .resolve(&target.host, target.port)
        .map_err(|_| WebFetchError::ResolutionFailed)?;
    check_active(started, bounds, cancel)?;
    if connect_started.elapsed() >= bounds.connect {
        return Err(WebFetchError::ConnectTimeout);
    }
    if addresses.is_empty() {
        return Err(WebFetchError::NoAddresses);
    }
    for address in &addresses {
        if !is_globally_routable(address.validated_ip) {
            return Err(WebFetchError::DeniedAddress(address.validated_ip));
        }
    }
    addresses.sort_by_key(|address| (address.validated_ip, address.connect_addr));
    addresses.dedup_by_key(|address| (address.validated_ip, address.connect_addr));
    Ok(addresses)
}

fn is_globally_routable(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => is_global_ipv4(address),
        IpAddr::V6(address) => is_global_ipv6(address),
    }
}

fn is_global_ipv4(address: Ipv4Addr) -> bool {
    let [a, b, c, _] = address.octets();
    !matches!(
        (a, b, c),
        (0, _, _)
            | (10, _, _)
            | (100, 64..=127, _)
            | (127, _, _)
            | (169, 254, _)
            | (172, 16..=31, _)
            | (192, 0, 0)
            | (192, 0, 2)
            | (192, 88, 99)
            | (192, 168, _)
            | (198, 18..=19, _)
            | (198, 51, 100)
            | (203, 0, 113)
            | (224..=255, _, _)
    )
}

fn is_global_ipv6(address: Ipv6Addr) -> bool {
    if let Some(mapped) = address.to_ipv4_mapped() {
        return is_global_ipv4(mapped);
    }
    if address.is_unspecified() || address.is_loopback() || address.is_multicast() {
        return false;
    }
    let segments = address.segments();
    if segments[0] & 0xfe00 == 0xfc00
        || segments[0] & 0xffc0 == 0xfe80
        || segments[0] & 0xffc0 == 0xfec0
        || segments[0] & 0xe000 != 0x2000
    {
        return false;
    }
    if segments[0] == 0x2001
        && (segments[1] == 0x0000
            || segments[1] == 0x0002
            || segments[1] == 0x0db8
            || (0x0010..=0x002f).contains(&segments[1]))
    {
        return false;
    }
    segments[0] != 0x2002 && !(segments[0] == 0x3fff && segments[1] & 0xfff0 == 0)
}

fn fetch_with<R: HostResolver>(
    call_id: ToolCallId,
    approved: ValidatedUrl,
    resolver: &R,
    cancel: Option<&AtomicBool>,
    bounds: FetchBounds,
) -> Result<ToolResult, WebFetchError> {
    let started = Instant::now();
    let mut current = approved.clone();
    let mut redirect_count = 0u8;

    loop {
        let addresses = resolve_and_validate(resolver, &current, started, bounds, cancel)?;
        let response = send_get(&current, addresses, started, bounds, cancel)?;
        let status = response.status();

        if is_redirect_status(status) {
            if redirect_count == MAX_REDIRECTS {
                return Err(WebFetchError::TooManyRedirects);
            }
            let location = response
                .header("Location")
                .ok_or(WebFetchError::InvalidRedirect)?;
            if location.len() > MAX_URL_BYTES {
                return Err(WebFetchError::InvalidRedirect);
            }
            let joined = current
                .url
                .join(location)
                .map_err(|_| WebFetchError::InvalidRedirect)?;
            let next = validate_url(joined.as_str()).map_err(|_| WebFetchError::InvalidRedirect)?;
            if next.scheme != approved.scheme
                || next.host != approved.host
                || next.port != approved.port
            {
                return Err(WebFetchError::CrossOriginRedirect);
            }
            redirect_count += 1;
            current = next;
            continue;
        }
        if !(200..300).contains(&status) {
            return Err(WebFetchError::HttpStatus(status));
        }

        let media_type = validate_media_type(response.header("Content-Type"))?;
        validate_response_framing(&response)?;
        validate_content_length(&response)?;
        let body = read_body(response.into_reader(), started, bounds, cancel)?;
        let source_bytes = body.len();
        let source = std::str::from_utf8(&body).map_err(|_| WebFetchError::InvalidUtf8)?;
        let transformed = if media_type == "text/html" {
            html_to_text(source.as_bytes())?
        } else {
            source.to_owned()
        };
        check_active(started, bounds, cancel)?;
        let (content, truncated) = truncate_utf8(&transformed, MAX_RETURNED_BYTES);
        let returned_bytes = content.len();

        return Ok(ToolResult {
            call_id,
            summary: format!("fetched {source_bytes} bytes from {}", current.normalized),
            data: json!({
                "url": current.normalized,
                "status": status,
                "media_type": media_type,
                "source_bytes": source_bytes,
                "returned_bytes": returned_bytes,
                "redirects": redirect_count,
                "truncated": truncated,
                "content": content,
            }),
            artifacts: vec![],
            visibility: ResultVisibility::Both,
        });
    }
}

fn send_get(
    target: &ValidatedUrl,
    addresses: Vec<ResolvedAddress>,
    started: Instant,
    bounds: FetchBounds,
    cancel: Option<&AtomicBool>,
) -> Result<ureq::Response, WebFetchError> {
    check_active(started, bounds, cancel)?;
    let connect_timeout = bounds
        .connect
        .min(remaining_time(started, bounds.operation)?);
    let read_timeout = bounds
        .read_poll
        .min(remaining_time(started, bounds.operation)?);
    let expected_netloc = format!(
        "{}:{}",
        target.url.host_str().ok_or(WebFetchError::InvalidUrl)?,
        target.port
    );
    let pinned = addresses
        .into_iter()
        .map(|address| address.connect_addr)
        .collect::<Vec<_>>();
    let agent = ureq::AgentBuilder::new()
        .try_proxy_from_env(false)
        .redirects(0)
        .max_idle_connections(0)
        .max_idle_connections_per_host(0)
        .timeout_connect(connect_timeout)
        .timeout_read(read_timeout)
        .timeout_write(read_timeout)
        .resolver(move |netloc: &str| {
            if netloc == expected_netloc {
                Ok(pinned.clone())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "resolver target did not match validated origin",
                ))
            }
        })
        .build();

    let result = agent
        .get(target.url.as_str())
        .set("Accept-Encoding", "identity")
        .set("Connection", "close")
        .call();
    match result {
        Ok(response) => Ok(response),
        Err(ureq::Error::Status(_, response)) => Ok(response),
        Err(ureq::Error::Transport(transport)) => {
            if cancel_requested(cancel) {
                Err(WebFetchError::Canceled)
            } else if started.elapsed() >= bounds.operation || transport_is_timeout(&transport) {
                Err(WebFetchError::OperationTimeout)
            } else {
                Err(WebFetchError::Transport)
            }
        }
    }
}

fn transport_is_timeout(transport: &ureq::Transport) -> bool {
    let mut source = std::error::Error::source(transport);
    while let Some(error) = source {
        if let Some(io_error) = error.downcast_ref::<io::Error>() {
            return matches!(
                io_error.kind(),
                io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
            );
        }
        source = error.source();
    }
    false
}

fn is_redirect_status(status: u16) -> bool {
    matches!(status, 301 | 302 | 303 | 307 | 308)
}

fn validate_media_type(header: Option<&str>) -> Result<String, WebFetchError> {
    let header = header.ok_or(WebFetchError::MissingMediaType)?;
    let mut parts = header.split(';');
    let media_type = parts
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(WebFetchError::MissingMediaType)?
        .to_ascii_lowercase();
    let accepted = matches!(
        media_type.as_str(),
        "text/plain" | "text/markdown" | "text/html" | "application/json"
    ) || (media_type.starts_with("application/") && media_type.ends_with("+json"));
    if !accepted {
        return Err(WebFetchError::UnsupportedMediaType);
    }
    for parameter in parts {
        let Some((name, value)) = parameter.split_once('=') else {
            continue;
        };
        if name.trim().eq_ignore_ascii_case("charset") {
            let charset = value.trim().trim_matches('"');
            if !charset.eq_ignore_ascii_case("utf-8") && !charset.eq_ignore_ascii_case("utf8") {
                return Err(WebFetchError::UnsupportedCharset);
            }
        }
    }
    Ok(media_type)
}

fn validate_response_framing(response: &ureq::Response) -> Result<(), WebFetchError> {
    if response.header("Transfer-Encoding").is_some() && response.header("Content-Length").is_some()
    {
        return Err(WebFetchError::InvalidContentLength);
    }
    Ok(())
}

fn validate_content_length(response: &ureq::Response) -> Result<(), WebFetchError> {
    let headers = response.all("Content-Length");
    if headers.len() > 1 {
        return Err(WebFetchError::InvalidContentLength);
    }
    let Some(header) = headers.first() else {
        return Ok(());
    };
    let length = header
        .trim()
        .parse::<u64>()
        .map_err(|_| WebFetchError::InvalidContentLength)?;
    if length > MAX_BODY_BYTES as u64 {
        return Err(WebFetchError::BodyTooLarge);
    }
    Ok(())
}

fn read_body(
    mut reader: impl Read,
    started: Instant,
    bounds: FetchBounds,
    cancel: Option<&AtomicBool>,
) -> Result<Vec<u8>, WebFetchError> {
    let mut body = Vec::with_capacity(16 * 1024);
    let mut buffer = [0u8; 16 * 1024];
    loop {
        check_active(started, bounds, cancel)?;
        let remaining = MAX_BODY_BYTES + 1 - body.len();
        if remaining == 0 {
            return Err(WebFetchError::BodyTooLarge);
        }
        let read_limit = buffer.len().min(remaining);
        match reader.read(&mut buffer[..read_limit]) {
            Ok(0) => break,
            Ok(read) => body.extend_from_slice(&buffer[..read]),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                ) =>
            {
                continue;
            }
            Err(_) => return Err(WebFetchError::Transport),
        }
    }
    if body.len() > MAX_BODY_BYTES {
        return Err(WebFetchError::BodyTooLarge);
    }
    Ok(body)
}

#[derive(Clone)]
struct NoImageDecorator {
    plain: PlainDecorator,
}

impl NoImageDecorator {
    fn new() -> Self {
        Self {
            plain: PlainDecorator::new(),
        }
    }
}

impl TextDecorator for NoImageDecorator {
    type Annotation = ();

    fn decorate_link_start(&mut self, url: &str) -> (String, Self::Annotation) {
        self.plain.decorate_link_start(url)
    }

    fn decorate_link_end(&mut self) -> String {
        self.plain.decorate_link_end()
    }

    fn decorate_em_start(&self) -> (String, Self::Annotation) {
        self.plain.decorate_em_start()
    }

    fn decorate_em_end(&self) -> String {
        self.plain.decorate_em_end()
    }

    fn decorate_strong_start(&self) -> (String, Self::Annotation) {
        self.plain.decorate_strong_start()
    }

    fn decorate_strong_end(&self) -> String {
        self.plain.decorate_strong_end()
    }

    fn decorate_strikeout_start(&self) -> (String, Self::Annotation) {
        self.plain.decorate_strikeout_start()
    }

    fn decorate_strikeout_end(&self) -> String {
        self.plain.decorate_strikeout_end()
    }

    fn decorate_code_start(&self) -> (String, Self::Annotation) {
        self.plain.decorate_code_start()
    }

    fn decorate_code_end(&self) -> String {
        self.plain.decorate_code_end()
    }

    fn decorate_preformat_first(&self) -> Self::Annotation {
        self.plain.decorate_preformat_first()
    }

    fn decorate_preformat_cont(&self) -> Self::Annotation {
        self.plain.decorate_preformat_cont()
    }

    fn decorate_image(&mut self, _src: &str, _title: &str) -> (String, Self::Annotation) {
        (String::new(), ())
    }

    fn header_prefix(&self, level: usize) -> String {
        self.plain.header_prefix(level)
    }

    fn quote_prefix(&self) -> String {
        self.plain.quote_prefix()
    }

    fn unordered_item_prefix(&self) -> String {
        self.plain.unordered_item_prefix()
    }

    fn ordered_item_prefix(&self, index: i64) -> String {
        self.plain.ordered_item_prefix(index)
    }

    fn make_subblock_decorator(&self) -> Self {
        self.clone()
    }
}

fn html_to_text(body: &[u8]) -> Result<String, WebFetchError> {
    html2text::config::with_decorator(NoImageDecorator::new())
        .string_from_read(body, HTML_WIDTH)
        .map_err(|_| WebFetchError::HtmlConversion)
}

fn truncate_utf8(content: &str, max_bytes: usize) -> (String, bool) {
    if content.len() <= max_bytes {
        return (content.to_owned(), false);
    }
    let mut end = max_bytes;
    while !content.is_char_boundary(end) {
        end -= 1;
    }
    (content[..end].to_owned(), true)
}

fn remaining_time(started: Instant, limit: Duration) -> Result<Duration, WebFetchError> {
    limit
        .checked_sub(started.elapsed())
        .filter(|remaining| !remaining.is_zero())
        .ok_or(WebFetchError::OperationTimeout)
}

fn check_active(
    started: Instant,
    bounds: FetchBounds,
    cancel: Option<&AtomicBool>,
) -> Result<(), WebFetchError> {
    if cancel_requested(cancel) {
        return Err(WebFetchError::Canceled);
    }
    remaining_time(started, bounds.operation).map(|_| ())
}

fn cancel_requested(cancel: Option<&AtomicBool>) -> bool {
    cancel.is_some_and(|cancel| cancel.load(Ordering::SeqCst))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::VecDeque,
        io::Write,
        net::{Shutdown, TcpListener, TcpStream},
        sync::{Arc, Mutex},
        thread,
    };

    const PUBLIC_V4_A: IpAddr = IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34));
    const PUBLIC_V4_B: IpAddr = IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8));

    #[derive(Clone)]
    struct ResolutionStep {
        delay: Duration,
        addresses: Vec<ResolvedAddress>,
    }

    struct SequenceResolver {
        steps: Mutex<VecDeque<ResolutionStep>>,
        calls: Mutex<Vec<(String, u16)>>,
    }

    impl SequenceResolver {
        fn new(steps: Vec<ResolutionStep>) -> Self {
            Self {
                steps: Mutex::new(steps.into()),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn call_count(&self) -> usize {
            self.calls.lock().unwrap().len()
        }
    }

    impl HostResolver for SequenceResolver {
        fn resolve(&self, host: &str, port: u16) -> io::Result<Vec<ResolvedAddress>> {
            self.calls.lock().unwrap().push((host.to_owned(), port));
            let step = self
                .steps
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| io::Error::other("resolution plan exhausted"))?;
            if !step.delay.is_zero() {
                thread::sleep(step.delay);
            }
            Ok(step.addresses)
        }
    }

    #[derive(Clone)]
    struct ResponseScript {
        status: u16,
        reason: &'static str,
        headers: Vec<(String, String)>,
        chunks: Vec<(Duration, Vec<u8>)>,
    }

    impl ResponseScript {
        fn ok(media_type: Option<&str>, body: impl Into<Vec<u8>>) -> Self {
            let body = body.into();
            let mut headers = Vec::new();
            if let Some(media_type) = media_type {
                headers.push(("Content-Type".into(), media_type.into()));
            }
            headers.push(("Content-Length".into(), body.len().to_string()));
            Self {
                status: 200,
                reason: "OK",
                headers,
                chunks: vec![(Duration::ZERO, body)],
            }
        }

        fn redirect(location: &str) -> Self {
            Self {
                status: 302,
                reason: "Found",
                headers: vec![
                    ("Location".into(), location.into()),
                    ("Content-Length".into(), "0".into()),
                ],
                chunks: Vec::new(),
            }
        }

        fn with_header(mut self, name: &str, value: &str) -> Self {
            self.headers
                .retain(|(candidate, _)| !candidate.eq_ignore_ascii_case(name));
            self.headers.push((name.into(), value.into()));
            self
        }

        fn without_header(mut self, name: &str) -> Self {
            self.headers
                .retain(|(candidate, _)| !candidate.eq_ignore_ascii_case(name));
            self
        }
    }

    struct FixtureServer {
        address: SocketAddr,
        running: Arc<AtomicBool>,
        requests: Arc<Mutex<Vec<String>>>,
        closed_connections: Arc<Mutex<usize>>,
        worker: Option<thread::JoinHandle<()>>,
    }

    impl FixtureServer {
        fn new(scripts: Vec<ResponseScript>) -> Self {
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
            listener.set_nonblocking(true).unwrap();
            let address = listener.local_addr().unwrap();
            let running = Arc::new(AtomicBool::new(true));
            let requests = Arc::new(Mutex::new(Vec::new()));
            let closed_connections = Arc::new(Mutex::new(0));
            let worker_running = running.clone();
            let worker_requests = requests.clone();
            let worker_closed = closed_connections.clone();
            let worker = thread::spawn(move || {
                let mut scripts = VecDeque::from(scripts);
                while worker_running.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            stream
                                .set_read_timeout(Some(Duration::from_millis(500)))
                                .unwrap();
                            let request = read_request(&mut stream);
                            worker_requests.lock().unwrap().push(request);
                            let Some(script) = scripts.pop_front() else {
                                break;
                            };
                            write_response(&mut stream, &script);
                            let _ = stream.shutdown(Shutdown::Write);
                            if wait_for_peer_close(&mut stream) {
                                *worker_closed.lock().unwrap() += 1;
                            }
                        }
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(2));
                        }
                        Err(_) => break,
                    }
                }
            });
            Self {
                address,
                running,
                requests,
                closed_connections,
                worker: Some(worker),
            }
        }

        fn url(&self, path: &str) -> String {
            format!("http://fixture.example:{}{path}", self.address.port())
        }

        fn request_count(&self) -> usize {
            self.requests.lock().unwrap().len()
        }

        fn requests(&self) -> Vec<String> {
            self.requests.lock().unwrap().clone()
        }

        fn wait_for_requests(&self, expected: usize) {
            let deadline = Instant::now() + Duration::from_secs(2);
            while self.request_count() < expected && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(2));
            }
            assert_eq!(self.request_count(), expected);
        }

        fn wait_for_closed(&self, expected: usize) {
            let deadline = Instant::now() + Duration::from_secs(2);
            while *self.closed_connections.lock().unwrap() < expected && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(2));
            }
            assert_eq!(*self.closed_connections.lock().unwrap(), expected);
        }
    }

    impl Drop for FixtureServer {
        fn drop(&mut self) {
            self.running.store(false, Ordering::SeqCst);
            if let Some(worker) = self.worker.take() {
                worker.join().unwrap();
            }
        }
    }

    fn read_request(stream: &mut TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut buffer = [0u8; 1024];
        while bytes.len() < 64 * 1024 {
            match stream.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => {
                    bytes.extend_from_slice(&buffer[..read]);
                    if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        String::from_utf8(bytes).unwrap()
    }

    fn wait_for_peer_close(stream: &mut TcpStream) -> bool {
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut byte = [0u8; 1];
        loop {
            match stream.read(&mut byte) {
                Ok(0) => return true,
                Ok(_) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::ConnectionReset
                            | io::ErrorKind::ConnectionAborted
                            | io::ErrorKind::BrokenPipe
                            | io::ErrorKind::NotConnected
                            | io::ErrorKind::UnexpectedEof
                    ) =>
                {
                    return true;
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::TimedOut
                            | io::ErrorKind::WouldBlock
                            | io::ErrorKind::Interrupted
                    ) => {}
                Err(_) => return false,
            }
            if Instant::now() >= deadline {
                return false;
            }
        }
    }

    fn write_response(stream: &mut TcpStream, script: &ResponseScript) {
        let mut head = format!("HTTP/1.1 {} {}\r\n", script.status, script.reason);
        for (name, value) in &script.headers {
            head.push_str(&format!("{name}: {value}\r\n"));
        }
        head.push_str("\r\n");
        if stream.write_all(head.as_bytes()).is_err() || stream.flush().is_err() {
            return;
        }
        for (delay, chunk) in &script.chunks {
            if !delay.is_zero() {
                thread::sleep(*delay);
            }
            if stream.write_all(chunk).is_err() || stream.flush().is_err() {
                break;
            }
        }
    }

    fn resolution(ip: IpAddr, connect_addr: SocketAddr) -> ResolutionStep {
        ResolutionStep {
            delay: Duration::ZERO,
            addresses: vec![ResolvedAddress {
                validated_ip: ip,
                connect_addr,
            }],
        }
    }

    fn fetch_fixture(
        server: &FixtureServer,
        path: &str,
        resolver: &SequenceResolver,
    ) -> Result<ToolResult, WebFetchError> {
        let target = validate_url(&server.url(path)).unwrap();
        fetch_with(
            ToolCallId::new("call_web").unwrap(),
            target,
            resolver,
            None,
            FetchBounds::default(),
        )
    }

    fn approved_fetch(
        server: &FixtureServer,
        path: &str,
        resolver: &SequenceResolver,
        bounds: FetchBounds,
        cancel: Option<&AtomicBool>,
    ) -> Result<ToolResult, WebFetchError> {
        let target = validate_url(&server.url(path)).unwrap();
        let approval_addresses =
            resolve_and_validate(resolver, &target, Instant::now(), bounds, cancel)?;
        let preview = format_approval_preview(&target, &approval_addresses);
        assert!(preview.contains(&target.normalized));
        assert!(preview.contains(&normalized_origin(&target)));
        fetch_with(
            ToolCallId::new("call_web").unwrap(),
            target,
            resolver,
            cancel,
            bounds,
        )
    }

    #[test]
    fn url_canonicalization_strips_fragments_and_preview_lists_origin_and_addresses() {
        let target = validate_url("HTTP://BÜCHER.Example:80/a?q=1#not-sent").unwrap();
        assert_eq!(target.normalized, "http://xn--bcher-kva.example/a?q=1");
        assert_eq!(normalized_origin(&target), "http://xn--bcher-kva.example");

        let resolver = SequenceResolver::new(vec![ResolutionStep {
            delay: Duration::ZERO,
            addresses: vec![
                ResolvedAddress {
                    validated_ip: PUBLIC_V4_A,
                    connect_addr: SocketAddr::new(PUBLIC_V4_A, 80),
                },
                ResolvedAddress {
                    validated_ip: PUBLIC_V4_B,
                    connect_addr: SocketAddr::new(PUBLIC_V4_B, 80),
                },
            ],
        }]);
        let addresses = resolve_and_validate(
            &resolver,
            &target,
            Instant::now(),
            FetchBounds::default(),
            None,
        )
        .unwrap();
        assert_eq!(
            format_approval_preview(&target, &addresses),
            concat!(
                "url: http://xn--bcher-kva.example/a?q=1\n",
                "origin: http://xn--bcher-kva.example\n",
                "addresses: 8.8.8.8, 93.184.216.34\n",
                "effect: Network"
            )
        );
    }

    #[test]
    fn url_input_bounds_and_authority_fixtures_fail_closed() {
        assert!(validate_url(&format!("https://example.com/{}", "a".repeat(2028))).is_ok());
        assert_eq!(
            validate_url(&format!("https://example.com/{}", "a".repeat(2029))),
            Err(WebFetchError::UrlTooLong)
        );
        assert_eq!(
            parse_input(&json!({"url": "https://example.com", "method": "POST"})).unwrap_err(),
            WebFetchError::InvalidInput
        );

        let invalid = [
            "example.com/path",
            "ftp://example.com/path",
            "https://user@example.com/path",
            "https://user:pass@example.com/path",
            "https://example.com:0/path",
            "https://example.com:0443/path",
            "https://example.com:99999/path",
            "https://example.com./path",
            "https://bad_name.example/path",
            "https://example.com\\@127.0.0.1/path",
            " https://example.com/path",
            "https://example.com/path\n",
            "http://127.1/path",
            "http://2130706433/path",
            "http://0177.0.0.1/path",
            "http://0x7f.0.0.1/path",
            "http://[2001:0DB8::1]/path",
        ];
        for candidate in invalid {
            assert!(validate_url(candidate).is_err(), "accepted {candidate}");
        }
        for name in CLOUD_METADATA_NAMES {
            assert_eq!(
                validate_url(&format!("http://{name}/latest")),
                Err(WebFetchError::MetadataNameForbidden)
            );
        }
    }

    #[test]
    fn ip_policy_denies_every_non_public_class_and_mapped_bypass() {
        let denied = [
            "0.0.0.0",
            "10.0.0.1",
            "100.64.0.1",
            "127.0.0.1",
            "169.254.169.254",
            "172.16.0.1",
            "192.0.0.1",
            "192.0.2.1",
            "192.88.99.1",
            "192.168.0.1",
            "198.18.0.1",
            "198.51.100.1",
            "203.0.113.1",
            "224.0.0.1",
            "240.0.0.1",
            "255.255.255.255",
            "::",
            "::1",
            "::ffff:127.0.0.1",
            "100::1",
            "2001:2::1",
            "2001:db8::1",
            "2001:10::1",
            "2001:20::1",
            "2002:7f00:1::1",
            "3fff::1",
            "fc00::1",
            "fd00:ec2::254",
            "fe80::1",
            "fec0::1",
            "ff02::1",
        ];
        for address in denied {
            let address = address.parse::<IpAddr>().unwrap();
            assert!(!is_globally_routable(address), "allowed {address}");
        }
        for address in ["1.1.1.1", "8.8.8.8", "2001:4860:4860::8888"] {
            let address = address.parse::<IpAddr>().unwrap();
            assert!(is_globally_routable(address), "denied {address}");
        }
    }

    #[test]
    fn mixed_public_private_dns_answers_are_rejected_before_connection() {
        let target = validate_url("https://example.com/").unwrap();
        let resolver = SequenceResolver::new(vec![ResolutionStep {
            delay: Duration::ZERO,
            addresses: vec![
                ResolvedAddress {
                    validated_ip: PUBLIC_V4_A,
                    connect_addr: SocketAddr::new(PUBLIC_V4_A, 443),
                },
                ResolvedAddress {
                    validated_ip: "10.0.0.1".parse().unwrap(),
                    connect_addr: "10.0.0.1:443".parse().unwrap(),
                },
            ],
        }]);

        assert_eq!(
            resolve_and_validate(
                &resolver,
                &target,
                Instant::now(),
                FetchBounds::default(),
                None,
            ),
            Err(WebFetchError::DeniedAddress("10.0.0.1".parse().unwrap()))
        );
    }

    #[test]
    fn plain_markdown_json_and_html_fixtures_return_exact_bounded_metadata() {
        let cases = [
            ("text/plain; charset=utf-8", "plain text", "plain text"),
            ("text/markdown", "# Heading\n", "# Heading\n"),
            ("application/json", r#"{"ok":true}"#, r#"{"ok":true}"#),
            (
                "application/problem+json",
                r#"{"detail":"safe"}"#,
                r#"{"detail":"safe"}"#,
            ),
        ];
        for (media_type, body, expected) in cases {
            let server = FixtureServer::new(vec![ResponseScript::ok(Some(media_type), body)]);
            let resolver = SequenceResolver::new(vec![resolution(PUBLIC_V4_A, server.address)]);
            let result = fetch_fixture(&server, "/data", &resolver).unwrap();
            assert_eq!(result.data["content"], expected);
            assert_eq!(result.data["source_bytes"], body.len());
            assert_eq!(result.data["returned_bytes"], expected.len());
            assert_eq!(result.data["status"], 200);
            assert_eq!(result.data["redirects"], 0);
            assert_eq!(result.data["truncated"], false);
            server.wait_for_closed(1);
        }

        let html = concat!(
            "<html><head><style>.secret{display:block}</style></head><body>",
            "<h1>Hello</h1><p>Safe text.</p>",
            "<script>steal()</script><img src='http://private/' alt='hidden image'>",
            "</body></html>"
        );
        let server = FixtureServer::new(vec![ResponseScript::ok(Some("text/html"), html)]);
        let resolver = SequenceResolver::new(vec![resolution(PUBLIC_V4_A, server.address)]);
        let result = fetch_fixture(&server, "/page", &resolver).unwrap();
        assert_eq!(result.data["content"], "# Hello\n\nSafe text.\n");
        assert!(!result.data["content"].as_str().unwrap().contains("steal"));
        assert!(!result.data["content"].as_str().unwrap().contains("private"));
        assert!(
            !result.data["content"]
                .as_str()
                .unwrap()
                .contains("hidden image")
        );
        assert_eq!(server.request_count(), 1);
    }

    #[test]
    fn request_is_get_only_credential_free_bodyless_and_proxy_independent() {
        let server = FixtureServer::new(vec![ResponseScript::ok(Some("text/plain"), "ok")]);
        let resolver = SequenceResolver::new(vec![resolution(PUBLIC_V4_A, server.address)]);
        temp_env::with_vars(
            [
                ("HTTP_PROXY", Some("http://127.0.0.1:9")),
                ("HTTPS_PROXY", Some("http://127.0.0.1:9")),
                ("OPENROUTER_API_KEY", Some("provider-secret")),
            ],
            || fetch_fixture(&server, "/headers", &resolver).unwrap(),
        );
        server.wait_for_requests(1);
        let request = &server.requests()[0];
        let lower = request.to_ascii_lowercase();
        assert!(request.starts_with("GET /headers HTTP/1.1\r\n"));
        assert!(lower.contains("accept-encoding: identity\r\n"));
        assert!(lower.contains("connection: close\r\n"));
        assert!(!lower.contains("authorization:"));
        assert!(!lower.contains("cookie:"));
        assert!(!request.contains("provider-secret"));
        assert!(request.ends_with("\r\n\r\n"));
        server.wait_for_closed(1);
    }

    #[test]
    fn media_length_and_utf8_failures_are_typed_and_bounded() {
        let cases = vec![
            (
                ResponseScript::ok(None, "body"),
                WebFetchError::MissingMediaType,
            ),
            (
                ResponseScript::ok(Some("application/octet-stream"), "body"),
                WebFetchError::UnsupportedMediaType,
            ),
            (
                ResponseScript::ok(Some("text/plain; charset=iso-8859-1"), "body"),
                WebFetchError::UnsupportedCharset,
            ),
            (
                ResponseScript::ok(Some("text/plain"), vec![0xff]),
                WebFetchError::InvalidUtf8,
            ),
            (
                ResponseScript::ok(Some("text/plain"), "body")
                    .with_header("Content-Length", "false"),
                WebFetchError::InvalidContentLength,
            ),
            (
                ResponseScript::ok(Some("text/plain"), "secret error body")
                    .with_header("Content-Length", &(MAX_BODY_BYTES + 1).to_string()),
                WebFetchError::BodyTooLarge,
            ),
        ];
        for (script, expected) in cases {
            let server = FixtureServer::new(vec![script]);
            let resolver = SequenceResolver::new(vec![resolution(PUBLIC_V4_A, server.address)]);
            assert_eq!(fetch_fixture(&server, "/invalid", &resolver), Err(expected));
        }
    }

    #[test]
    fn body_and_returned_text_boundaries_are_exact_and_utf8_safe() {
        let exact_body = "a".repeat(MAX_BODY_BYTES);
        let server = FixtureServer::new(vec![ResponseScript::ok(
            Some("text/plain"),
            exact_body.clone(),
        )]);
        let resolver = SequenceResolver::new(vec![resolution(PUBLIC_V4_A, server.address)]);
        let result = fetch_fixture(&server, "/exact-body", &resolver).unwrap();
        assert_eq!(result.data["source_bytes"], MAX_BODY_BYTES);
        assert_eq!(result.data["returned_bytes"], MAX_RETURNED_BYTES);
        assert_eq!(result.data["truncated"], true);

        let over_body = "a".repeat(MAX_BODY_BYTES + 1);
        let server = FixtureServer::new(vec![
            ResponseScript::ok(Some("text/plain"), over_body).without_header("Content-Length"),
        ]);
        let resolver = SequenceResolver::new(vec![resolution(PUBLIC_V4_A, server.address)]);
        assert_eq!(
            fetch_fixture(&server, "/over-body", &resolver),
            Err(WebFetchError::BodyTooLarge)
        );

        for (body, returned, truncated) in [
            ("a".repeat(MAX_RETURNED_BYTES), MAX_RETURNED_BYTES, false),
            (
                format!("{}界", "a".repeat(MAX_RETURNED_BYTES - 1)),
                MAX_RETURNED_BYTES - 1,
                true,
            ),
        ] {
            let server = FixtureServer::new(vec![ResponseScript::ok(Some("text/plain"), body)]);
            let resolver = SequenceResolver::new(vec![resolution(PUBLIC_V4_A, server.address)]);
            let result = fetch_fixture(&server, "/return-bound", &resolver).unwrap();
            assert_eq!(result.data["returned_bytes"], returned);
            assert_eq!(result.data["truncated"], truncated);
            assert!(
                std::str::from_utf8(result.data["content"].as_str().unwrap().as_bytes()).is_ok()
            );
        }
    }

    #[test]
    fn same_origin_redirects_reresolve_and_stop_before_a_fourth_target() {
        let server = FixtureServer::new(vec![
            ResponseScript::redirect("/one"),
            ResponseScript::redirect("/two"),
            ResponseScript::redirect("/three"),
            ResponseScript::ok(Some("text/plain"), "done"),
        ]);
        let resolver = SequenceResolver::new(vec![
            resolution(PUBLIC_V4_A, server.address),
            resolution(PUBLIC_V4_B, server.address),
            resolution(PUBLIC_V4_A, server.address),
            resolution(PUBLIC_V4_B, server.address),
            resolution(PUBLIC_V4_A, server.address),
        ]);
        let result =
            approved_fetch(&server, "/start", &resolver, FetchBounds::default(), None).unwrap();
        assert_eq!(result.data["url"], server.url("/three"));
        assert_eq!(result.data["redirects"], 3);
        assert_eq!(result.data["content"], "done");
        assert_eq!(resolver.call_count(), 5);
        server.wait_for_requests(4);

        let server = FixtureServer::new(vec![
            ResponseScript::redirect("/one"),
            ResponseScript::redirect("/two"),
            ResponseScript::redirect("/three"),
            ResponseScript::redirect("/must-not-fetch"),
        ]);
        let resolver = SequenceResolver::new(vec![
            resolution(PUBLIC_V4_A, server.address),
            resolution(PUBLIC_V4_A, server.address),
            resolution(PUBLIC_V4_A, server.address),
            resolution(PUBLIC_V4_A, server.address),
            resolution(PUBLIC_V4_A, server.address),
        ]);
        assert_eq!(
            approved_fetch(&server, "/start", &resolver, FetchBounds::default(), None,),
            Err(WebFetchError::TooManyRedirects)
        );
        server.wait_for_requests(4);
        assert_eq!(resolver.call_count(), 5);
    }

    #[test]
    fn redirects_reject_cross_origin_downgrade_port_and_invalid_locations() {
        let redirects = [
            "http://other.example/path".to_owned(),
            "https://fixture.example/path".to_owned(),
            "http://fixture.example:9/path".to_owned(),
        ];
        for location in redirects {
            let server = FixtureServer::new(vec![ResponseScript::redirect(&location)]);
            let resolver = SequenceResolver::new(vec![resolution(PUBLIC_V4_A, server.address)]);
            assert_eq!(
                fetch_fixture(&server, "/start", &resolver),
                Err(WebFetchError::CrossOriginRedirect)
            );
            server.wait_for_requests(1);
        }

        let server = FixtureServer::new(vec![ResponseScript::redirect("http://[")]);
        let resolver = SequenceResolver::new(vec![resolution(PUBLIC_V4_A, server.address)]);
        assert_eq!(
            fetch_fixture(&server, "/start", &resolver),
            Err(WebFetchError::InvalidRedirect)
        );
    }

    #[test]
    fn rebinding_to_denied_address_is_rejected_before_any_socket_is_opened() {
        let server = FixtureServer::new(vec![ResponseScript::ok(Some("text/plain"), "secret")]);
        let resolver = SequenceResolver::new(vec![
            resolution(PUBLIC_V4_A, server.address),
            ResolutionStep {
                delay: Duration::ZERO,
                addresses: vec![ResolvedAddress {
                    validated_ip: "127.0.0.1".parse().unwrap(),
                    connect_addr: server.address,
                }],
            },
        ]);

        assert_eq!(
            approved_fetch(
                &server,
                "/metadata",
                &resolver,
                FetchBounds::default(),
                None,
            ),
            Err(WebFetchError::DeniedAddress("127.0.0.1".parse().unwrap()))
        );
        thread::sleep(Duration::from_millis(20));
        assert_eq!(resolver.call_count(), 2);
        assert_eq!(server.request_count(), 0);
    }

    #[test]
    #[cfg_attr(
        target_os = "macos",
        ignore = "timing-sensitive on macOS runners; #465"
    )]
    fn connect_read_overall_and_cancellation_bounds_close_the_request() {
        let server = FixtureServer::new(vec![]);
        let resolver = SequenceResolver::new(vec![ResolutionStep {
            delay: Duration::from_millis(35),
            addresses: resolution(PUBLIC_V4_A, server.address).addresses,
        }]);
        let bounds = FetchBounds {
            connect: Duration::from_millis(20),
            operation: Duration::from_millis(100),
            read_poll: Duration::from_millis(10),
        };
        let target = validate_url(&server.url("/slow-connect")).unwrap();
        assert_eq!(
            resolve_and_validate(&resolver, &target, Instant::now(), bounds, None),
            Err(WebFetchError::ConnectTimeout)
        );

        let slow = ResponseScript::ok(Some("text/plain"), "late");
        let slow = ResponseScript {
            chunks: vec![(Duration::from_millis(120), b"late".to_vec())],
            ..slow
        };
        let server = FixtureServer::new(vec![slow]);
        let resolver = SequenceResolver::new(vec![resolution(PUBLIC_V4_A, server.address)]);
        let bounds = FetchBounds {
            connect: Duration::from_millis(50),
            operation: Duration::from_millis(70),
            read_poll: Duration::from_millis(10),
        };
        let target = validate_url(&server.url("/slow-read")).unwrap();
        assert_eq!(
            fetch_with(
                ToolCallId::new("call_slow").unwrap(),
                target,
                &resolver,
                None,
                bounds,
            ),
            Err(WebFetchError::OperationTimeout)
        );
        server.wait_for_closed(1);

        let drip = ResponseScript::ok(Some("text/plain"), "abcd");
        let drip = ResponseScript {
            chunks: vec![
                (Duration::from_millis(15), b"a".to_vec()),
                (Duration::from_millis(15), b"b".to_vec()),
                (Duration::from_millis(15), b"c".to_vec()),
                (Duration::from_millis(15), b"d".to_vec()),
            ],
            ..drip
        };
        let server = FixtureServer::new(vec![drip]);
        let resolver = SequenceResolver::new(vec![resolution(PUBLIC_V4_A, server.address)]);
        let bounds = FetchBounds {
            connect: Duration::from_millis(50),
            operation: Duration::from_millis(35),
            read_poll: Duration::from_millis(20),
        };
        let target = validate_url(&server.url("/overall")).unwrap();
        assert_eq!(
            fetch_with(
                ToolCallId::new("call_overall").unwrap(),
                target,
                &resolver,
                None,
                bounds,
            ),
            Err(WebFetchError::OperationTimeout)
        );
        server.wait_for_closed(1);

        let delayed = ResponseScript::ok(Some("text/plain"), "late");
        let delayed = ResponseScript {
            chunks: vec![(Duration::from_millis(100), b"late".to_vec())],
            ..delayed
        };
        let server = FixtureServer::new(vec![delayed]);
        let resolver = SequenceResolver::new(vec![resolution(PUBLIC_V4_A, server.address)]);
        let cancel = Arc::new(AtomicBool::new(false));
        let signal = cancel.clone();
        let canceler = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            signal.store(true, Ordering::SeqCst);
        });
        let target = validate_url(&server.url("/cancel")).unwrap();
        let result = fetch_with(
            ToolCallId::new("call_cancel").unwrap(),
            target,
            &resolver,
            Some(cancel.as_ref()),
            FetchBounds {
                connect: Duration::from_millis(50),
                operation: Duration::from_millis(200),
                read_poll: Duration::from_millis(10),
            },
        );
        canceler.join().unwrap();
        assert_eq!(result, Err(WebFetchError::Canceled));
        server.wait_for_closed(1);
    }

    #[test]
    fn non_success_and_transport_errors_redact_bodies_urls_and_secrets() {
        let secret = "TOP_SECRET_RESPONSE_BODY";
        let server = FixtureServer::new(vec![ResponseScript {
            status: 500,
            reason: "Internal Server Error",
            headers: vec![
                ("Content-Type".into(), "text/plain".into()),
                ("Content-Length".into(), secret.len().to_string()),
            ],
            chunks: vec![(Duration::ZERO, secret.as_bytes().to_vec())],
        }]);
        let resolver = SequenceResolver::new(vec![resolution(PUBLIC_V4_A, server.address)]);
        let error = fetch_fixture(&server, "/failure?token=query-secret", &resolver).unwrap_err();
        assert_eq!(error, WebFetchError::HttpStatus(500));
        let display = error.to_string();
        assert!(!display.contains(secret));
        assert!(!display.contains("query-secret"));

        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let unused = listener.local_addr().unwrap();
        drop(listener);
        let target = validate_url(&format!(
            "http://fixture.example:{}/private?token=query-secret",
            unused.port()
        ))
        .unwrap();
        let resolver = SequenceResolver::new(vec![resolution(PUBLIC_V4_A, unused)]);
        let error = fetch_with(
            ToolCallId::new("call_transport").unwrap(),
            target,
            &resolver,
            None,
            FetchBounds::default(),
        )
        .unwrap_err();
        assert_eq!(error, WebFetchError::Transport);
        assert!(!error.to_string().contains("query-secret"));
        assert!(!error.to_string().contains("private"));
    }
}
