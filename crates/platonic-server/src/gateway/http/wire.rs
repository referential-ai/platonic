use std::{
    io::{Read, Write},
    net::TcpStream,
    time::{Duration, Instant},
};

pub(super) const MAX_BODY_BYTES: usize = 768 * 1024;
pub(super) const MAX_HEADER_BYTES: usize = 32 * 1024;
pub(super) const MAX_HEADER_COUNT: usize = 64;
const MAX_TARGET_BYTES: usize = 4 * 1024;
const HEADER_TIMEOUT: Duration = Duration::from_secs(5);
const BODY_TIMEOUT: Duration = Duration::from_secs(10);
const WRITE_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, thiserror::Error)]
pub(super) enum WireError {
    #[error("request headers exceed {MAX_HEADER_BYTES} bytes")]
    HeadersTooLarge,
    #[error("request has more than {MAX_HEADER_COUNT} headers")]
    TooManyHeaders,
    #[error("request body exceeds {MAX_BODY_BYTES} bytes")]
    BodyTooLarge,
    #[error("malformed HTTP request")]
    Malformed,
    #[error("unsupported HTTP transfer encoding")]
    TransferEncoding,
    #[error("HTTP I/O failed: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Clone, Eq, PartialEq)]
pub(super) struct HttpRequest {
    pub(super) method: String,
    pub(super) target: String,
    pub(super) headers: Vec<(String, Vec<u8>)>,
    pub(super) body: Vec<u8>,
}

impl HttpRequest {
    pub(super) fn header_values<'a>(
        &'a self,
        name: &'a str,
    ) -> impl Iterator<Item = &'a [u8]> + 'a {
        self.headers
            .iter()
            .filter(move |(field, _)| field.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_slice())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct HttpResponse {
    pub(super) status: u16,
    pub(super) headers: Vec<(String, String)>,
    pub(super) body: Vec<u8>,
}

impl HttpResponse {
    pub(super) fn json(status: u16, body: Vec<u8>) -> Self {
        Self {
            status,
            headers: vec![("Content-Type".into(), "application/json".into())],
            body,
        }
    }

    pub(super) fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

pub(super) fn read_request(stream: &mut TcpStream) -> Result<HttpRequest, WireError> {
    stream.set_write_timeout(Some(WRITE_TIMEOUT))?;
    stream.set_nodelay(true)?;

    let mut bytes = Vec::with_capacity(4096);
    let header_deadline = Instant::now() + HEADER_TIMEOUT;
    let header_end = loop {
        if let Some(end) = find_header_end(&bytes) {
            break end;
        }
        if bytes.len() >= MAX_HEADER_BYTES {
            return Err(WireError::HeadersTooLarge);
        }
        let remaining = MAX_HEADER_BYTES + 1 - bytes.len();
        let mut chunk = [0; 4096];
        let length = remaining.min(chunk.len());
        set_remaining_timeout(stream, header_deadline)?;
        let count = stream.read(&mut chunk[..length])?;
        if count == 0 {
            return Err(WireError::Malformed);
        }
        bytes.extend_from_slice(&chunk[..count]);
    };
    if header_end > MAX_HEADER_BYTES {
        return Err(WireError::HeadersTooLarge);
    }

    let mut parsed_headers = [httparse::EMPTY_HEADER; MAX_HEADER_COUNT];
    let mut parsed = httparse::Request::new(&mut parsed_headers);
    let parsed_end = match parsed.parse(&bytes[..header_end]) {
        Ok(httparse::Status::Complete(end)) => end,
        Ok(httparse::Status::Partial) => return Err(WireError::Malformed),
        Err(httparse::Error::TooManyHeaders) => return Err(WireError::TooManyHeaders),
        Err(_) => return Err(WireError::Malformed),
    };
    if parsed_end != header_end || parsed.version != Some(1) {
        return Err(WireError::Malformed);
    }
    let method = parsed.method.ok_or(WireError::Malformed)?;
    let target = parsed.path.ok_or(WireError::Malformed)?;
    if target.len() > MAX_TARGET_BYTES || !target.starts_with('/') || target.contains('#') {
        return Err(WireError::Malformed);
    }

    let headers = parsed
        .headers
        .iter()
        .map(|header| (header.name.to_string(), header.value.to_vec()))
        .collect::<Vec<_>>();
    if headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("transfer-encoding"))
    {
        return Err(WireError::TransferEncoding);
    }
    if headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("host"))
        .count()
        != 1
    {
        return Err(WireError::Malformed);
    }
    let content_lengths = headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .map(|(_, value)| value)
        .collect::<Vec<_>>();
    if content_lengths.len() > 1 {
        return Err(WireError::Malformed);
    }
    let content_length = match content_lengths.first() {
        Some(value) if !value.is_empty() && value.iter().all(u8::is_ascii_digit) => {
            std::str::from_utf8(value)
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .ok_or(WireError::Malformed)?
        }
        Some(_) => return Err(WireError::Malformed),
        None => 0,
    };
    if content_length > MAX_BODY_BYTES {
        return Err(WireError::BodyTooLarge);
    }

    let body_deadline = Instant::now() + BODY_TIMEOUT;
    let mut body = bytes[header_end..].to_vec();
    body.truncate(content_length);
    while body.len() < content_length {
        let remaining = content_length - body.len();
        let mut chunk = [0; 8192];
        let length = remaining.min(chunk.len());
        set_remaining_timeout(stream, body_deadline)?;
        let count = stream.read(&mut chunk[..length])?;
        if count == 0 {
            return Err(WireError::Malformed);
        }
        body.extend_from_slice(&chunk[..count]);
    }
    Ok(HttpRequest {
        method: method.into(),
        target: target.into(),
        headers,
        body,
    })
}

fn set_remaining_timeout(stream: &TcpStream, deadline: Instant) -> std::io::Result<()> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "HTTP request deadline elapsed",
        ));
    }
    stream.set_read_timeout(Some(remaining))
}

pub(super) fn write_response(
    stream: &mut TcpStream,
    response: &HttpResponse,
) -> std::io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 {}\r\nContent-Length: {}\r\nConnection: close\r\nX-Content-Type-Options: nosniff\r\nCache-Control: no-store\r\n",
        status_line(response.status),
        response.body.len(),
    )?;
    for (name, value) in &response.headers {
        write!(stream, "{name}: {value}\r\n")?;
    }
    stream.write_all(b"\r\n")?;
    stream.write_all(&response.body)?;
    stream.flush()
}

pub(super) fn write_sse_headers(stream: &mut TcpStream) -> std::io::Result<()> {
    stream.write_all(
        b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: close\r\nX-Content-Type-Options: nosniff\r\n\r\n",
    )?;
    stream.flush()
}

pub(super) fn write_sse_event(stream: &mut TcpStream, id: u64, data: &[u8]) -> std::io::Result<()> {
    write!(stream, "id: {id}\ndata: ")?;
    stream.write_all(data)?;
    stream.write_all(b"\n\n")?;
    stream.flush()
}

pub(super) fn write_sse_error(stream: &mut TcpStream, data: &[u8]) -> std::io::Result<()> {
    stream.write_all(b"event: error\ndata: ")?;
    stream.write_all(data)?;
    stream.write_all(b"\n\n")?;
    stream.flush()
}

pub(super) fn write_sse_keepalive(stream: &mut TcpStream) -> std::io::Result<()> {
    stream.write_all(b": keepalive\n\n")?;
    stream.flush()
}

fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| position + 4)
}

fn status_line(status: u16) -> &'static str {
    match status {
        200 => "200 OK",
        400 => "400 Bad Request",
        401 => "401 Unauthorized",
        403 => "403 Forbidden",
        404 => "404 Not Found",
        405 => "405 Method Not Allowed",
        409 => "409 Conflict",
        413 => "413 Payload Too Large",
        415 => "415 Unsupported Media Type",
        429 => "429 Too Many Requests",
        500 => "500 Internal Server Error",
        502 => "502 Bad Gateway",
        503 => "503 Service Unavailable",
        _ => "500 Internal Server Error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    fn parse(raw: Vec<u8>) -> Result<HttpRequest, WireError> {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let writer = std::thread::spawn(move || {
            let mut stream = TcpStream::connect(address).unwrap();
            stream.write_all(&raw).unwrap();
        });
        let (mut stream, _) = listener.accept().unwrap();
        let result = read_request(&mut stream);
        writer.join().unwrap();
        result
    }

    #[test]
    fn parses_one_bounded_http_11_request() {
        let request = parse(
            b"POST /v1/workspaces/ws/threads/t/messages HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\n\r\n{}"
                .to_vec(),
        )
        .unwrap();
        assert_eq!(request.method, "POST");
        assert_eq!(request.body, b"{}");
    }

    #[test]
    fn rejects_oversized_bodies_and_transfer_encoding() {
        let oversized = format!(
            "POST /v1/status HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n",
            MAX_BODY_BYTES + 1
        )
        .into_bytes();
        assert!(matches!(parse(oversized), Err(WireError::BodyTooLarge)));
        assert!(matches!(
            parse(
                b"POST /v1/status HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n"
                    .to_vec()
            ),
            Err(WireError::TransferEncoding)
        ));
        assert!(matches!(
            parse(
                b"POST /v1/status HTTP/1.1\r\nHost: localhost\r\nContent-Length: +2\r\n\r\n{}"
                    .to_vec()
            ),
            Err(WireError::Malformed)
        ));
    }

    #[test]
    fn rejects_ambiguous_or_unbounded_request_metadata() {
        assert!(matches!(
            parse(b"GET /v1/status HTTP/1.0\r\nHost: localhost\r\n\r\n".to_vec()),
            Err(WireError::Malformed)
        ));
        assert!(matches!(
            parse(b"GET /v1/status HTTP/1.1\r\nHost: one\r\nHost: two\r\n\r\n".to_vec()),
            Err(WireError::Malformed)
        ));

        let mut too_many = b"GET /v1/status HTTP/1.1\r\nHost: localhost\r\n".to_vec();
        for index in 0..MAX_HEADER_COUNT {
            too_many.extend_from_slice(format!("X-{index}: value\r\n").as_bytes());
        }
        too_many.extend_from_slice(b"\r\n");
        assert!(matches!(parse(too_many), Err(WireError::TooManyHeaders)));
    }

    #[test]
    fn responses_never_add_browser_or_tls_authority() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let reader = std::thread::spawn(move || {
            let mut stream = TcpStream::connect(address).unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).unwrap();
            response
        });
        let (mut stream, _) = listener.accept().unwrap();
        write_response(&mut stream, &HttpResponse::json(200, b"{}".to_vec())).unwrap();
        drop(stream);
        let response = reader.join().unwrap().to_ascii_lowercase();

        assert!(!response.contains("access-control-"));
        assert!(!response.contains("set-cookie"));
        assert!(!response.contains("strict-transport-security"));
    }
}
