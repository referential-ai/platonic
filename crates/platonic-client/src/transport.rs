//! Cross-platform synchronous local IPC primitives.

#[cfg(windows)]
use std::io::{Read as _, Write as _};
use std::{io, path::Path, time::Duration};

/// Platform listener used by the daemon server.
#[cfg(unix)]
pub use std::os::unix::net::UnixListener as Listener;
/// Platform stream used by daemon clients and servers.
#[cfg(unix)]
pub use std::os::unix::net::UnixStream as Stream;

/// Platform listener used by the daemon server.
#[cfg(windows)]
pub use interprocess::local_socket::Listener;

/// Platform stream used by daemon clients and servers.
#[cfg(windows)]
#[derive(Debug)]
pub struct Stream {
    inner: WindowsStream,
    deadline: Option<std::time::Instant>,
}

#[cfg(windows)]
#[derive(Debug)]
enum WindowsStream {
    Client(std::fs::File),
    Server(interprocess::local_socket::Stream),
}

#[cfg(all(windows, not(test)))]
const CONTROL_IO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
#[cfg(all(windows, test))]
const CONTROL_IO_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(250);
const ACCEPT_RETRY_BACKOFF: Duration = Duration::from_millis(50);

#[cfg(unix)]
/// Binds a listener at `endpoint`.
pub fn bind(endpoint: &Path) -> io::Result<Listener> {
    Listener::bind(endpoint)
}

#[cfg(windows)]
/// Binds a current-user-only named-pipe listener at `endpoint`.
pub fn bind(endpoint: &Path) -> io::Result<Listener> {
    use interprocess::{
        local_socket::{GenericFilePath, ListenerOptions, prelude::*},
        os::windows::local_socket::ListenerOptionsExt,
    };

    let descriptor = crate::windows_security::current_user_pipe_descriptor()?;
    ListenerOptions::new()
        .name(endpoint.to_fs_name::<GenericFilePath>()?)
        .security_descriptor(descriptor)
        .create_sync()
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

#[cfg(windows)]
/// Connects to a current-user named-pipe server.
pub fn connect(endpoint: &Path) -> io::Result<Stream> {
    Ok(Stream {
        inner: WindowsStream::Client(crate::windows_security::connect_current_user_pipe(
            endpoint,
        )?),
        deadline: None,
    })
}

#[cfg(windows)]
/// Connects to a current-user named-pipe server within `timeout`.
pub fn connect_with_timeout(endpoint: &Path, timeout: std::time::Duration) -> io::Result<Stream> {
    Ok(Stream {
        inner: WindowsStream::Client(
            crate::windows_security::connect_current_user_pipe_with_timeout(endpoint, timeout)?,
        ),
        deadline: None,
    })
}

#[cfg(windows)]
/// Connects to the current-user named-pipe server with `expected_pid`.
pub fn connect_expected_server(endpoint: &Path, expected_pid: u32) -> io::Result<Stream> {
    Ok(Stream {
        inner: WindowsStream::Client(crate::windows_security::connect_current_user_pipe_for_pid(
            endpoint,
            expected_pid,
        )?),
        deadline: Some(std::time::Instant::now() + CONTROL_IO_TIMEOUT),
    })
}

#[cfg(windows)]
/// Applies one absolute I/O deadline to `stream`.
pub fn set_deadline(stream: &mut Stream, deadline: std::time::Instant) -> io::Result<()> {
    stream.deadline = Some(deadline);
    Ok(())
}

#[cfg(windows)]
/// Clears the client-pipe deadline and restores blocking pipe mode.
pub fn clear_deadline(stream: &mut Stream) -> io::Result<()> {
    if let WindowsStream::Client(pipe) = &stream.inner {
        crate::windows_security::set_pipe_wait(pipe)?;
    }
    stream.deadline = None;
    Ok(())
}

#[cfg(unix)]
/// Accepts one stream from `listener`.
pub fn accept(listener: &Listener) -> io::Result<Stream> {
    listener.accept().map(|(stream, _)| stream)
}

#[cfg(windows)]
/// Accepts one named-pipe stream from `listener`.
pub fn accept(listener: &Listener) -> io::Result<Stream> {
    use interprocess::local_socket::prelude::*;

    listener.accept().map(|stream| Stream {
        inner: WindowsStream::Server(stream),
        deadline: None,
    })
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

#[cfg(windows)]
fn is_platform_retryable_accept_error(error: &io::Error) -> bool {
    use windows_sys::Win32::Foundation::{
        ERROR_COMMITMENT_LIMIT, ERROR_NO_SYSTEM_RESOURCES, ERROR_NONPAGED_SYSTEM_RESOURCES,
        ERROR_NOT_ENOUGH_MEMORY, ERROR_NOT_ENOUGH_QUOTA, ERROR_OUTOFMEMORY,
        ERROR_PAGED_SYSTEM_RESOURCES, ERROR_TOO_MANY_OPEN_FILES, ERROR_WORKING_SET_QUOTA,
    };

    error.raw_os_error().is_some_and(|code| {
        [
            ERROR_TOO_MANY_OPEN_FILES,
            ERROR_NOT_ENOUGH_MEMORY,
            ERROR_OUTOFMEMORY,
            ERROR_NO_SYSTEM_RESOURCES,
            ERROR_NONPAGED_SYSTEM_RESOURCES,
            ERROR_PAGED_SYSTEM_RESOURCES,
            ERROR_WORKING_SET_QUOTA,
            ERROR_COMMITMENT_LIMIT,
            ERROR_NOT_ENOUGH_QUOTA,
        ]
        .into_iter()
        .any(|retryable| retryable as i32 == code)
    })
}

#[cfg(unix)]
/// Clones a stream handle for independent buffered reading and writing.
pub fn try_clone(stream: &Stream) -> io::Result<Stream> {
    stream.try_clone()
}

#[cfg(windows)]
/// Refreshes the fixed control-client deadline when one is active.
pub fn reset_deadline(stream: &mut Stream) {
    if stream.deadline.is_some() {
        stream.deadline = Some(std::time::Instant::now() + CONTROL_IO_TIMEOUT);
    }
}

#[cfg(windows)]
/// Clones a stream handle for independent buffered reading and writing.
pub fn try_clone(stream: &Stream) -> io::Result<Stream> {
    let inner = match &stream.inner {
        WindowsStream::Client(stream) => WindowsStream::Client(stream.try_clone()?),
        WindowsStream::Server(stream) => {
            WindowsStream::Server(interprocess::TryClone::try_clone(stream)?)
        }
    };
    Ok(Stream {
        inner,
        deadline: stream.deadline,
    })
}

/// Wakes a blocking daemon accept call by making a best-effort connection.
pub fn wake(endpoint: &Path) {
    let _ = connect(endpoint);
}

#[cfg(windows)]
impl io::Read for Stream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match &mut self.inner {
            WindowsStream::Client(stream) => read_client(stream, self.deadline, buffer),
            WindowsStream::Server(stream) => stream.read(buffer),
        }
    }
}

#[cfg(windows)]
impl io::Read for &Stream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match &self.inner {
            WindowsStream::Client(stream) => read_client(stream, self.deadline, buffer),
            WindowsStream::Server(stream) => {
                let mut server = stream;
                server.read(buffer)
            }
        }
    }
}

#[cfg(windows)]
impl io::Write for Stream {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        match &mut self.inner {
            WindowsStream::Client(stream) => write_client(stream, self.deadline, buffer),
            WindowsStream::Server(stream) => stream.write(buffer),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match &mut self.inner {
            WindowsStream::Client(_) if self.deadline.is_some() => Ok(()),
            WindowsStream::Client(stream) => stream.flush(),
            WindowsStream::Server(stream) => stream.flush(),
        }
    }
}

#[cfg(windows)]
fn read_client(
    mut stream: &std::fs::File,
    deadline: Option<std::time::Instant>,
    buffer: &mut [u8],
) -> io::Result<usize> {
    loop {
        if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
            return Err(pipe_io_timeout());
        }
        // PIPE_NOWAIT reports an empty connected pipe as a zero-byte read.
        match stream.read(buffer) {
            Ok(0) if wait_for_pipe(deadline) => continue,
            Ok(0) if deadline.is_some() => return Err(pipe_io_timeout()),
            Err(error)
                if deadline.is_some() && pipe_would_block(&error) && wait_for_pipe(deadline) =>
            {
                continue;
            }
            Err(error) if deadline.is_some() && pipe_would_block(&error) => {
                return Err(pipe_io_timeout());
            }
            result => return result,
        }
    }
}

#[cfg(windows)]
fn write_client(
    mut stream: &std::fs::File,
    deadline: Option<std::time::Instant>,
    buffer: &[u8],
) -> io::Result<usize> {
    loop {
        if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
            return Err(pipe_io_timeout());
        }
        match stream.write(buffer) {
            Ok(0) if wait_for_pipe(deadline) => continue,
            Ok(0) if deadline.is_some() => return Err(pipe_io_timeout()),
            Err(error)
                if deadline.is_some() && pipe_would_block(&error) && wait_for_pipe(deadline) =>
            {
                continue;
            }
            Err(error) if deadline.is_some() && pipe_would_block(&error) => {
                return Err(pipe_io_timeout());
            }
            result => return result,
        }
    }
}

#[cfg(windows)]
fn pipe_would_block(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::WouldBlock
        || error.raw_os_error() == Some(windows_sys::Win32::Foundation::ERROR_NO_DATA as i32)
}

#[cfg(windows)]
fn wait_for_pipe(deadline: Option<std::time::Instant>) -> bool {
    let Some(deadline) = deadline else {
        return false;
    };
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    if remaining.is_zero() {
        return false;
    }
    std::thread::sleep(remaining.min(std::time::Duration::from_millis(10)));
    true
}

#[cfg(windows)]
fn pipe_io_timeout() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "named-pipe request timed out")
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

#[cfg(all(test, windows))]
mod tests {
    use super::*;
    use interprocess::os::windows::named_pipe::{PipeListenerOptions, pipe_mode};
    use std::{
        io::{Read, Write},
        num::NonZeroU8,
        path::PathBuf,
        thread,
    };

    #[test]
    fn named_pipe_round_trip_supports_cloned_streams() {
        let endpoint = PathBuf::from(format!(
            r"\\.\pipe\plato-agent-transport-test-{}",
            std::process::id()
        ));
        let listener = bind(&endpoint).unwrap();
        let client_endpoint = endpoint.clone();
        let client = thread::spawn(move || {
            let stream = connect_expected_server(&client_endpoint, std::process::id()).unwrap();
            let mut writer = try_clone(&stream).unwrap();
            writer.write_all(b"ping").unwrap();
            let mut response = [0; 4];
            let mut reader = &stream;
            reader.read_exact(&mut response).unwrap();
            response
        });

        let stream = accept(&listener).unwrap();
        let mut writer = try_clone(&stream).unwrap();
        let mut request = [0; 4];
        let mut reader = &stream;
        reader.read_exact(&mut request).unwrap();
        writer.write_all(b"pong").unwrap();

        assert_eq!(request, *b"ping");
        assert_eq!(client.join().unwrap(), *b"pong");
    }

    #[test]
    fn named_pipe_rejects_an_unexpected_server_pid() {
        let endpoint = PathBuf::from(format!(
            r"\\.\pipe\plato-agent-transport-pid-test-{}",
            std::process::id()
        ));
        let listener = bind(&endpoint).unwrap();
        let client_endpoint = endpoint.clone();
        let client = thread::spawn(move || {
            connect_expected_server(&client_endpoint, std::process::id().wrapping_add(1))
        });

        let stream = accept(&listener).unwrap();
        drop(stream);
        let error = client.join().unwrap().unwrap_err();

        assert!(
            error
                .to_string()
                .contains("named-pipe server process does not match lock metadata")
        );
    }

    #[test]
    fn expected_server_reads_time_out_without_a_response() {
        let endpoint = PathBuf::from(format!(
            r"\\.\pipe\plato-agent-transport-timeout-test-{}",
            std::process::id()
        ));
        let listener = bind(&endpoint).unwrap();
        let client_endpoint = endpoint.clone();
        let client = thread::spawn(move || {
            let stream = connect_expected_server(&client_endpoint, std::process::id()).unwrap();
            let started = std::time::Instant::now();
            let mut reader = &stream;
            let mut byte = [0; 1];
            let error = reader.read_exact(&mut byte).unwrap_err();
            (started.elapsed(), error)
        });

        let stream = accept(&listener).unwrap();
        thread::sleep(CONTROL_IO_TIMEOUT + std::time::Duration::from_millis(100));
        drop(stream);
        let (elapsed, error) = client.join().unwrap();

        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(elapsed < CONTROL_IO_TIMEOUT + std::time::Duration::from_millis(100));
    }

    #[test]
    fn timed_connect_stops_when_the_named_pipe_is_busy() {
        let endpoint = PathBuf::from(format!(
            r"\\.\pipe\plato-agent-transport-connect-timeout-test-{}",
            std::process::id()
        ));
        let listener = PipeListenerOptions::new()
            .path(endpoint.as_path())
            .instance_limit(NonZeroU8::new(1))
            .security_descriptor(Some(
                crate::windows_security::current_user_pipe_descriptor().unwrap(),
            ))
            .create_duplex::<pipe_mode::Bytes>()
            .unwrap();
        let client_endpoint = endpoint.clone();
        let client = thread::spawn(move || connect(&client_endpoint).unwrap());
        let client_stream = client.join().unwrap();

        let started = std::time::Instant::now();
        let error =
            connect_with_timeout(&endpoint, std::time::Duration::from_millis(100)).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
        drop(client_stream);
        drop(listener);
    }
}
