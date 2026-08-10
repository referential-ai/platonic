//! Cross-platform synchronous local IPC primitives.

use std::{io, path::Path, time::Duration};

/// Platform listener used by the daemon server.
#[cfg(unix)]
pub use std::os::unix::net::UnixListener as Listener;
/// Platform stream used by daemon clients and servers.
#[cfg(unix)]
pub use std::os::unix::net::UnixStream as Stream;

const ACCEPT_RETRY_BACKOFF: Duration = Duration::from_millis(50);

#[cfg(unix)]
/// Binds a listener at `endpoint`.
pub fn bind(endpoint: &Path) -> io::Result<Listener> {
    Listener::bind(endpoint)
}

#[cfg(unix)]
/// Connects to `endpoint` using the platform's ordinary connect behavior.
pub fn connect(endpoint: &Path) -> io::Result<Stream> {
    Stream::connect(endpoint)
}

#[cfg(unix)]
/// Connects to `endpoint` within `timeout`.
pub fn connect_with_timeout(endpoint: &Path, timeout: std::time::Duration) -> io::Result<Stream> {
    use rustix::{
        event::{PollFd, PollFlags, Timespec, poll},
        fs::{OFlags, fcntl_getfl, fcntl_setfl},
        net::{AddressFamily, SocketAddrUnix, SocketType, connect},
    };

    #[cfg(target_os = "linux")]
    let socket = rustix::net::socket_with(
        AddressFamily::UNIX,
        SocketType::STREAM,
        rustix::net::SocketFlags::CLOEXEC | rustix::net::SocketFlags::NONBLOCK,
        None,
    )?;
    #[cfg(not(target_os = "linux"))]
    let socket = {
        let socket = rustix::net::socket(AddressFamily::UNIX, SocketType::STREAM, None)?;
        rustix::io::fcntl_setfd(
            &socket,
            rustix::io::fcntl_getfd(&socket)? | rustix::io::FdFlags::CLOEXEC,
        )?;
        fcntl_setfl(&socket, fcntl_getfl(&socket)? | OFlags::NONBLOCK)?;
        socket
    };
    let address = SocketAddrUnix::new(endpoint)?;
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if std::time::Instant::now() >= deadline {
            return Err(connect_timeout());
        }
        match connect(&socket, &address) {
            Ok(()) => break,
            Err(rustix::io::Errno::AGAIN) => {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    return Err(connect_timeout());
                }
                std::thread::sleep(remaining.min(std::time::Duration::from_millis(10)));
            }
            Err(rustix::io::Errno::INTR) => {}
            Err(rustix::io::Errno::INPROGRESS) => {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    return Err(connect_timeout());
                }
                let timeout = Timespec::try_from(remaining).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "daemon connect timeout is too large",
                    )
                })?;
                let mut poll_fd = [PollFd::new(&socket, PollFlags::OUT)];
                if poll(&mut poll_fd, Some(&timeout))? == 0 {
                    return Err(connect_timeout());
                }
                if let Err(error) = rustix::net::sockopt::socket_error(&socket)? {
                    return Err(error.into());
                }
                break;
            }
            Err(error) => return Err(error.into()),
        }
    }
    fcntl_setfl(&socket, fcntl_getfl(&socket)? - OFlags::NONBLOCK)?;
    Ok(socket.into())
}

#[cfg(unix)]
fn connect_timeout() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "daemon connect timed out")
}

#[cfg(unix)]
/// Applies one absolute read/write deadline to `stream`.
pub fn set_deadline(stream: &mut Stream, deadline: std::time::Instant) -> io::Result<()> {
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    if remaining.is_zero() {
        return Err(request_io_timeout());
    }
    stream.set_read_timeout(Some(remaining))?;
    stream.set_write_timeout(Some(remaining))
}

#[cfg(unix)]
/// Clears read/write deadlines from `stream`.
pub fn clear_deadline(stream: &mut Stream) -> io::Result<()> {
    stream.set_read_timeout(None)?;
    stream.set_write_timeout(None)
}

#[cfg(unix)]
/// Accepts one stream from `listener`.
pub fn accept(listener: &Listener) -> io::Result<Stream> {
    listener.accept().map(|(stream, _)| stream)
}

/// Returns the existing bounded retry delay for a transient accept failure.
pub fn accept_retry_delay(error: &io::Error) -> Option<Duration> {
    match error.kind() {
        io::ErrorKind::Interrupted => Some(Duration::ZERO),
        io::ErrorKind::WouldBlock | io::ErrorKind::ConnectionAborted => Some(ACCEPT_RETRY_BACKOFF),
        _ if is_platform_retryable_accept_error(error) => Some(ACCEPT_RETRY_BACKOFF),
        _ => None,
    }
}

#[cfg(unix)]
fn is_platform_retryable_accept_error(error: &io::Error) -> bool {
    error.raw_os_error().is_some_and(|code| {
        [
            rustix::io::Errno::MFILE,
            rustix::io::Errno::NFILE,
            rustix::io::Errno::NOBUFS,
            rustix::io::Errno::NOMEM,
        ]
        .into_iter()
        .any(|errno| errno.raw_os_error() == code)
    })
}

#[cfg(unix)]
/// Clones a stream handle for independent buffered reading and writing.
pub fn try_clone(stream: &Stream) -> io::Result<Stream> {
    stream.try_clone()
}

/// Wakes a blocking daemon accept call by making a best-effort connection.
pub fn wake(endpoint: &Path) {
    let _ = connect(endpoint);
}

#[cfg(unix)]
fn request_io_timeout() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "daemon request timed out")
}

#[cfg(all(test, target_os = "linux"))]
mod unix_tests {
    use super::*;
    use rustix::{
        fs::{OFlags, fcntl_getfl, fcntl_setfl},
        net::{AddressFamily, SocketAddrUnix, SocketType},
    };
    use std::time::{Duration, Instant};

    #[test]
    fn timed_connect_stops_when_the_unix_backlog_is_full() {
        let directory = tempfile::tempdir().unwrap();
        let endpoint = directory.path().join("agent.sock");
        let address = SocketAddrUnix::new(&endpoint).unwrap();
        let listener = rustix::net::socket(AddressFamily::UNIX, SocketType::STREAM, None).unwrap();
        rustix::net::bind(&listener, &address).unwrap();
        rustix::net::listen(&listener, 1).unwrap();

        let mut queued = Vec::new();
        let mut saturated = false;
        for _ in 0..32 {
            let socket =
                rustix::net::socket(AddressFamily::UNIX, SocketType::STREAM, None).unwrap();
            fcntl_setfl(&socket, fcntl_getfl(&socket).unwrap() | OFlags::NONBLOCK).unwrap();
            match rustix::net::connect(&socket, &address) {
                Ok(()) => queued.push(socket),
                Err(rustix::io::Errno::INPROGRESS | rustix::io::Errno::AGAIN) => {
                    queued.push(socket);
                    saturated = true;
                    break;
                }
                Err(error) => panic!("unable to saturate listener: {error}"),
            }
        }
        assert!(saturated, "test never filled the Unix listen backlog");

        let started = Instant::now();
        let error = connect_with_timeout(&endpoint, Duration::from_millis(100)).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(started.elapsed() < Duration::from_secs(1));
        drop(queued);
    }
}
