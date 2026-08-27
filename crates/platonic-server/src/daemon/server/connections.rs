#[cfg(unix)]
use super::socket::{prepare_runtime_path, prepare_socket_parent, prepare_temp_runtime_home};
#[cfg(all(test, unix))]
use super::{
    DaemonPaths,
    reconcile::reconcile_thread_repositories,
    socket::{PRIVATE_DIRECTORY_MODE, SOCKET_MODE, mode},
};
use super::{
    host::{HostRuntime, handle_host_line},
    socket::BoundEndpoint,
};
#[cfg(all(test, unix))]
use crate::daemon::{
    handlers::handle_line,
    protocol::{ERROR_INTERNAL, decode_request},
    runtime::DaemonRuntime,
};
#[cfg(unix)]
use crate::paths;
use crate::{
    AppResult,
    daemon::{
        lock::HostProcessLock,
        protocol::{
            ERROR_MALFORMED_REQUEST, Envelope, MAX_PROTOCOL_LINE_BYTES, ProtocolResponse,
            ShutdownIfIdleResultName,
        },
        transport,
    },
};
use std::{
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

#[cfg(all(test, unix))]
use std::{
    fs::{self, Permissions},
    io::ErrorKind,
    os::unix::fs::MetadataExt,
};

const MAX_CONNECTION_HANDLERS: usize = 64;

#[derive(Debug, Default)]
struct HandlerCapacity {
    live: AtomicUsize,
}

impl HandlerCapacity {
    fn try_acquire(self: &Arc<Self>) -> Option<HandlerPermit> {
        self.live
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |live| {
                (live < MAX_CONNECTION_HANDLERS).then_some(live + 1)
            })
            .ok()?;
        Some(HandlerPermit {
            capacity: Arc::clone(self),
        })
    }
}

struct HandlerPermit {
    capacity: Arc<HandlerCapacity>,
}

impl Drop for HandlerPermit {
    fn drop(&mut self) {
        let previous = self.capacity.live.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0);
    }
}

#[derive(Debug)]
pub struct HostDaemonServer {
    endpoint: BoundEndpoint,
    runtime: HostRuntime,
    handlers: Arc<HandlerCapacity>,
    _lock: HostProcessLock,
}

impl HostDaemonServer {
    pub fn bind() -> AppResult<Self> {
        let socket_path = crate::paths::host_socket_path()?;
        let lock_path = crate::paths::host_lock_path()?;
        #[cfg(unix)]
        {
            let (runtime_home, is_fallback) = paths::runtime_home_and_fallback();
            if is_fallback {
                prepare_temp_runtime_home(&runtime_home)?;
            }
            prepare_runtime_path(&runtime_home, &lock_path)?;
            prepare_socket_parent(&runtime_home, &socket_path)?;
        }
        let lock = HostProcessLock::acquire_for_host(lock_path, &socket_path)?;
        let endpoint = BoundEndpoint::bind(socket_path.clone(), true)?;
        Ok(Self {
            endpoint,
            runtime: HostRuntime::new(socket_path)?,
            handlers: Arc::new(HandlerCapacity::default()),
            _lock: lock,
        })
    }

    pub fn socket_path(&self) -> &Path {
        &self.endpoint.socket_path
    }

    pub fn serve_forever(&self, shutdown: Arc<AtomicBool>) -> AppResult<()> {
        let runtime = self.runtime.clone();
        serve_connections(
            &shutdown,
            &self.runtime.control_runtime.stop_requested,
            Arc::clone(&self.handlers),
            || transport::accept(&self.endpoint.listener),
            move |stream| handle_host_stream(runtime.clone(), stream),
            thread::sleep,
        )
    }

    pub fn serve_next(&self) -> AppResult<()> {
        let stream = transport::accept(&self.endpoint.listener)?;
        handle_host_stream(self.runtime.clone(), stream)
    }
}

fn serve_connections<S, A, H, B>(
    shutdown: &AtomicBool,
    stop_requested: &AtomicBool,
    handlers: Arc<HandlerCapacity>,
    mut accept: A,
    handle: H,
    mut backoff: B,
) -> AppResult<()>
where
    S: Send + 'static,
    A: FnMut() -> std::io::Result<S>,
    H: Fn(S) -> AppResult<()> + Send + Sync + 'static,
    B: FnMut(Duration),
{
    let handle = Arc::new(handle);
    loop {
        let stream = match accept() {
            Ok(stream) => stream,
            Err(error) => match transport::accept_retry_delay(&error) {
                Some(delay) => {
                    if !delay.is_zero() {
                        backoff(delay);
                    }
                    continue;
                }
                None => return Err(error.into()),
            },
        };
        if shutdown.load(Ordering::SeqCst) || stop_requested.load(Ordering::SeqCst) {
            return Ok(());
        }
        let Some(permit) = handlers.try_acquire() else {
            drop(stream);
            continue;
        };
        drop(spawn_connection_handler(
            permit,
            stream,
            Arc::clone(&handle),
        ));
    }
}

fn spawn_connection_handler<S, H>(
    permit: HandlerPermit,
    stream: S,
    handle: Arc<H>,
) -> thread::JoinHandle<()>
where
    S: Send + 'static,
    H: Fn(S) -> AppResult<()> + Send + Sync + 'static,
{
    thread::spawn(move || {
        let _permit = permit;
        if let Err(error) = handle(stream) {
            eprintln!("daemon connection error: {error}");
        }
    })
}

#[cfg(all(test, unix))]
fn handle_stream(runtime: DaemonRuntime, stream: transport::Stream) -> AppResult<()> {
    let stop_requested = Arc::clone(&runtime.stop_requested);
    let socket_path = runtime.paths.socket_path.clone();
    #[cfg(all(test, unix))]
    let shutdown_runtime = runtime.clone();
    handle_stream_lines(
        stream,
        stop_requested,
        socket_path,
        move |line| handle_registered_line(&runtime, line),
        move || {
            #[cfg(all(test, unix))]
            shutdown_runtime.wait_after_shutdown_flush();
        },
    )
}

#[cfg(all(test, unix))]
fn handle_registered_line(runtime: &DaemonRuntime, line: &str) -> Envelope {
    match runtime_with_registered_paths(runtime) {
        Ok(runtime) => handle_line(&runtime, line),
        Err(error) => match decode_request(line) {
            Ok(request) => Envelope::error(
                request.id,
                request.method.map(|method| method.to_string()),
                ERROR_INTERNAL,
                error.to_string(),
            ),
            Err(error) => *error,
        },
    }
}

#[cfg(all(test, unix))]
fn runtime_with_registered_paths(runtime: &DaemonRuntime) -> AppResult<DaemonRuntime> {
    let store = runtime.paths.server_store()?;
    let Some(record) = store.workspace_by_root(&runtime.paths.workspace_root.to_string_lossy())?
    else {
        return Ok(runtime.clone());
    };
    if runtime.paths.workspace_id == record.id
        && runtime.paths.ledger_path == PathBuf::from(&record.ledger_path)
    {
        return Ok(runtime.clone());
    }
    let mut registered = runtime.clone();
    registered.paths = runtime.paths.with_workspace_record(&record);
    Ok(registered)
}

fn handle_host_stream(runtime: HostRuntime, stream: transport::Stream) -> AppResult<()> {
    let stop_requested = Arc::clone(&runtime.control_runtime.stop_requested);
    let socket_path = runtime.socket_path.clone();
    let mut workspace_runtime = None;
    handle_stream_lines(
        stream,
        stop_requested,
        socket_path,
        move |line| handle_host_line(&runtime, &mut workspace_runtime, line),
        || {},
    )
}

fn handle_stream_lines<H, F>(
    stream: transport::Stream,
    stop_requested: Arc<AtomicBool>,
    socket_path: PathBuf,
    mut handle: H,
    after_shutdown_flush: F,
) -> AppResult<()>
where
    H: FnMut(&str) -> Envelope,
    F: FnOnce(),
{
    let mut writer = transport::try_clone(&stream)?;
    let mut reader = BufReader::new(stream);
    loop {
        let mut line = Vec::new();
        let read = reader
            .by_ref()
            .take((MAX_PROTOCOL_LINE_BYTES + 1) as u64)
            .read_until(b'\n', &mut line)?;
        if read == 0 {
            break;
        }
        let terminated = line.last() == Some(&b'\n');
        if terminated {
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
        }
        if line.len() > MAX_PROTOCOL_LINE_BYTES {
            let response = Envelope::error(
                None,
                None,
                ERROR_MALFORMED_REQUEST,
                format!("protocol line exceeds {MAX_PROTOCOL_LINE_BYTES} bytes"),
            );
            serde_json::to_writer(&mut writer, &response)?;
            writer.write_all(b"\n")?;
            writer.flush()?;
            return Ok(());
        }
        let line = std::str::from_utf8(&line)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        if line.trim().is_empty() {
            continue;
        }
        let response = handle(line);
        let stop_after_response = matches!(
            response.result,
            Some(ProtocolResponse::DaemonShutdownIfIdle(ref result))
                if result.result == ShutdownIfIdleResultName::Shutdown
        );
        let write_result = (|| -> AppResult<()> {
            serde_json::to_writer(&mut writer, &response)?;
            writer.write_all(b"\n")?;
            writer.flush()?;
            Ok(())
        })();
        if stop_after_response {
            after_shutdown_flush();
            stop_requested.store(true, Ordering::SeqCst);
            transport::wake(&socket_path);
            return write_result;
        }
        write_result?;
    }
    Ok(())
}

#[cfg(test)]
mod connection_tests {
    use super::*;
    use crate::AppError;
    use std::{
        collections::VecDeque,
        io,
        sync::{Barrier, Mutex, mpsc},
    };

    const FATAL_ACCEPT_CODE: i32 = 12_345;

    struct InjectedStream {
        id: usize,
        rejected: Arc<AtomicBool>,
    }

    impl Drop for InjectedStream {
        fn drop(&mut self) {
            if self.id == MAX_CONNECTION_HANDLERS {
                self.rejected.store(true, Ordering::SeqCst);
            }
        }
    }

    #[test]
    fn sixty_fifth_connection_closes_without_a_handler() {
        let shutdown = AtomicBool::new(false);
        let stop_requested = AtomicBool::new(false);
        let handlers = Arc::new(HandlerCapacity::default());
        let rejected = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(MAX_CONNECTION_HANDLERS + 1));
        let handled = Arc::new(Mutex::new(Vec::new()));
        let (done_tx, done_rx) = mpsc::channel();
        let mut next_id = 0;

        let handler_barrier = Arc::clone(&barrier);
        let handled_streams = Arc::clone(&handled);
        let result = serve_connections(
            &shutdown,
            &stop_requested,
            Arc::clone(&handlers),
            || {
                if next_id <= MAX_CONNECTION_HANDLERS {
                    let stream = InjectedStream {
                        id: next_id,
                        rejected: Arc::clone(&rejected),
                    };
                    next_id += 1;
                    Ok(stream)
                } else {
                    Err(io::Error::from_raw_os_error(FATAL_ACCEPT_CODE))
                }
            },
            move |stream: InjectedStream| {
                handled_streams.lock().unwrap().push(stream.id);
                handler_barrier.wait();
                done_tx.send(()).unwrap();
                Ok(())
            },
            |_| panic!("fatal accept errors must not back off"),
        );

        assert!(matches!(
            result,
            Err(AppError::Io(error)) if error.raw_os_error() == Some(FATAL_ACCEPT_CODE)
        ));
        assert!(rejected.load(Ordering::SeqCst));
        assert_eq!(
            handlers.live.load(Ordering::SeqCst),
            MAX_CONNECTION_HANDLERS
        );

        barrier.wait();
        for _ in 0..MAX_CONNECTION_HANDLERS {
            done_rx.recv().unwrap();
        }
        let mut handled = handled.lock().unwrap().clone();
        handled.sort_unstable();
        assert_eq!(handled, (0..MAX_CONNECTION_HANDLERS).collect::<Vec<_>>());
    }

    #[test]
    fn handler_error_and_panic_release_capacity() {
        let handlers = Arc::new(HandlerCapacity::default());
        let permit = handlers.try_acquire().unwrap();
        let error_handler = spawn_connection_handler(
            permit,
            (),
            Arc::new(|()| -> AppResult<()> {
                Err(io::Error::new(io::ErrorKind::BrokenPipe, "injected handler error").into())
            }),
        );
        error_handler.join().unwrap();
        assert_eq!(handlers.live.load(Ordering::SeqCst), 0);

        let permit = handlers.try_acquire().unwrap();
        let panic_handler = spawn_connection_handler(
            permit,
            (),
            Arc::new(|()| -> AppResult<()> {
                panic!("injected handler panic");
            }),
        );
        assert!(panic_handler.join().is_err());
        assert_eq!(handlers.live.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn listed_accept_errors_retry_and_serve_a_later_connection() {
        let errors = retryable_accept_errors();
        let retry_count = errors.len();
        let mut outcomes = errors.into_iter().map(Err).collect::<VecDeque<_>>();
        outcomes.push_back(Ok(7_u8));

        let shutdown = AtomicBool::new(false);
        let stop_requested = Arc::new(AtomicBool::new(false));
        let handler_stop = Arc::clone(&stop_requested);
        let served = Arc::new(AtomicBool::new(false));
        let handler_served = Arc::clone(&served);
        let (served_tx, served_rx) = mpsc::channel();
        let mut backoffs = Vec::new();

        serve_connections(
            &shutdown,
            &stop_requested,
            Arc::new(HandlerCapacity::default()),
            move || match outcomes.pop_front() {
                Some(outcome) => outcome,
                None => {
                    served_rx.recv().unwrap();
                    Ok(8)
                }
            },
            move |stream| {
                assert_eq!(stream, 7);
                handler_served.store(true, Ordering::SeqCst);
                handler_stop.store(true, Ordering::SeqCst);
                served_tx.send(()).unwrap();
                Ok(())
            },
            |delay| backoffs.push(delay),
        )
        .unwrap();

        assert!(served.load(Ordering::SeqCst));
        assert_eq!(backoffs, vec![Duration::from_millis(50); retry_count - 1]);
    }

    #[test]
    fn unlisted_accept_error_is_returned_unchanged() {
        let shutdown = AtomicBool::new(false);
        let stop_requested = AtomicBool::new(false);
        let mut error = Some(io::Error::from_raw_os_error(FATAL_ACCEPT_CODE));

        let result = serve_connections(
            &shutdown,
            &stop_requested,
            Arc::new(HandlerCapacity::default()),
            || Err::<(), _>(error.take().unwrap()),
            |()| panic!("fatal accept errors must not spawn handlers"),
            |_| panic!("fatal accept errors must not back off"),
        );

        assert!(matches!(
            result,
            Err(AppError::Io(error)) if error.raw_os_error() == Some(FATAL_ACCEPT_CODE)
        ));
    }

    #[test]
    fn shutdown_flags_close_the_accepted_wake_connection() {
        for (shutdown_set, stop_set) in [(true, false), (false, true)] {
            let shutdown = AtomicBool::new(shutdown_set);
            let stop_requested = AtomicBool::new(stop_set);
            let accepted = Arc::new(AtomicUsize::new(0));
            let dropped = Arc::new(AtomicUsize::new(0));
            let handled = Arc::new(AtomicUsize::new(0));
            let accepted_count = Arc::clone(&accepted);
            let dropped_count = Arc::clone(&dropped);
            let handled_count = Arc::clone(&handled);

            serve_connections(
                &shutdown,
                &stop_requested,
                Arc::new(HandlerCapacity::default()),
                move || {
                    assert_eq!(accepted_count.fetch_add(1, Ordering::SeqCst), 0);
                    Ok(DroppedStream(Arc::clone(&dropped_count)))
                },
                move |_stream| {
                    handled_count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
                |_| panic!("shutdown must not back off"),
            )
            .unwrap();

            assert_eq!(accepted.load(Ordering::SeqCst), 1);
            assert_eq!(dropped.load(Ordering::SeqCst), 1);
            assert_eq!(handled.load(Ordering::SeqCst), 0);
        }
    }

    struct DroppedStream(Arc<AtomicUsize>);

    impl Drop for DroppedStream {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn retryable_accept_errors() -> Vec<io::Error> {
        let mut errors = vec![
            io::Error::new(io::ErrorKind::Interrupted, "interrupted"),
            io::Error::new(io::ErrorKind::WouldBlock, "would block"),
            io::Error::new(io::ErrorKind::ConnectionAborted, "connection aborted"),
        ];

        #[cfg(unix)]
        errors.extend(
            [
                rustix::io::Errno::MFILE,
                rustix::io::Errno::NFILE,
                rustix::io::Errno::NOBUFS,
                rustix::io::Errno::NOMEM,
            ]
            .into_iter()
            .map(|errno| io::Error::from_raw_os_error(errno.raw_os_error())),
        );

        errors
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::{
        AppError, ApprovalRequest,
        app::ExternalApprovalOutcome,
        daemon::{
            client::DaemonClient,
            protocol::{
                DaemonStatusProviderKind, DaemonStatusResult, ERROR_DAEMON_SHUTTING_DOWN,
                ERROR_INTERNAL, ERROR_ISSUE_PREP_FAILED, ERROR_LAGGED, ERROR_MALFORMED_REQUEST,
                ERROR_NOT_FOUND, ERROR_OVERLOAD, ERROR_RUN_FAILED, ERROR_SESSIONS_LIST_FAILED,
                ERROR_VOICE_EVENTS_CONFLICT, ERROR_WORKSPACE_MISMATCH,
                ERROR_WORKSPACE_UNREGISTERED, Envelope, EnvelopeKind, ProtocolError,
                RunStartResult, RunStateName, StreamEvent, TypedTranscript, TypedTranscriptEntry,
                VoiceEvent, VoiceEventsResult,
            },
            runtime::{MAX_EVENT_BUFFER, MAX_TERMINAL_RUNS, PendingApproval, RunRecord},
        },
        ledger::{EventRecorder, SqliteLedger},
        tool_catalog::SHELL_EXEC,
    };
    use platonic_core::{
        AgentId, ContextPack, EffectClass, HarnessEvent, Message, MessageRole, ModelName,
        ModelUsage, RecordedEvent, RunId, ToolCallId, TurnId,
    };
    use rusqlite::params;
    use serde_json::json;
    use sha2::{Digest, Sha256};
    use std::{
        io::{BufRead, Read},
        net::TcpListener,
        os::unix::{
            fs::{FileTypeExt, PermissionsExt, symlink},
            net::{UnixListener, UnixStream},
            process::ExitStatusExt,
        },
        process::{Child, Command, Stdio},
        sync::{Arc, Barrier, mpsc},
        thread,
        time::{Duration, Instant},
    };

    const FAKE_PROVIDER_TIMEOUT: Duration = Duration::from_secs(15);

    fn response_value(response: &Envelope) -> serde_json::Value {
        let response = serde_json::to_value(response.result.as_ref().unwrap()).unwrap();
        response["result"].clone()
    }

    fn response_result<T: serde::de::DeserializeOwned>(response: &Envelope) -> T {
        serde_json::from_value(response_value(response)).unwrap()
    }

    fn pending_request(run_id: &str, call_id: &str) -> ApprovalRequest {
        ApprovalRequest {
            run_id: RunId::new(run_id).unwrap(),
            call_id: ToolCallId::new(call_id).unwrap(),
            tool_name: "file.write".into(),
            effect: EffectClass::WorkspaceWrite,
            reason: "file.write requires approval".into(),
            input_preview: Some(r#"{"path":"out.txt"}"#.into()),
            approval_preview: None,
            diff_preview: None,
            yolo_eligible: false,
            credential_id: None,
        }
    }

    #[derive(Debug)]
    struct TestDaemonServer {
        endpoint: BoundEndpoint,
        runtime: DaemonRuntime,
        handlers: Arc<HandlerCapacity>,
    }

    impl TestDaemonServer {
        fn bind(workspace_root: &Path, socket_path: Option<PathBuf>) -> AppResult<Self> {
            paths::with_test_xdg(workspace_root, || {
                let max_spawn_depth = crate::config::server_max_spawn_depth()?;
                let require_confinement = crate::config::server_require_confinement()?;
                let confinement_support = crate::confinement::detect_support();
                let paths = DaemonPaths::provisional(workspace_root, socket_path)?;
                let reclaim_host_socket = paths.socket_path == crate::paths::host_socket_path()?;
                prepare_socket_parent(&paths::runtime_home_and_fallback().0, &paths.socket_path)?;
                let endpoint = BoundEndpoint::bind(paths.socket_path.clone(), reclaim_host_socket)?;
                let paths = paths.resolve_workspace_record()?;
                if paths.is_registered() {
                    crate::ledger::interrupt_orphaned_default_sqlite_runs(&paths.default_ledger())?;
                }
                reconcile_thread_repositories(&paths.server_db_path)?;
                let runtime = DaemonRuntime::new_with_server_policy(
                    paths,
                    max_spawn_depth,
                    require_confinement,
                    confinement_support,
                );
                if runtime.paths.is_registered() {
                    crate::daemon::returns::reconcile_workspace(&runtime)?;
                }
                Ok(Self {
                    endpoint,
                    runtime,
                    handlers: Arc::new(HandlerCapacity::default()),
                })
            })
        }

        fn paths(&self) -> &DaemonPaths {
            &self.runtime.paths
        }

        fn serve_forever(&self, shutdown: Arc<AtomicBool>) -> AppResult<()> {
            let runtime = self.runtime.clone();
            serve_connections(
                &shutdown,
                &self.runtime.stop_requested,
                Arc::clone(&self.handlers),
                || transport::accept(&self.endpoint.listener),
                move |stream| handle_stream(runtime.clone(), stream),
                thread::sleep,
            )
        }

        fn serve_next(&self) -> AppResult<()> {
            let stream = transport::accept(&self.endpoint.listener)?;
            handle_stream(self.runtime.clone(), stream)
        }

        fn handle_line(&self, line: &str) -> Envelope {
            handle_registered_line(&self.runtime, line)
        }
    }

    fn bind_test(
        workspace_root: &Path,
        socket_path: Option<PathBuf>,
    ) -> AppResult<TestDaemonServer> {
        let mut server = TestDaemonServer::bind(workspace_root, socket_path)?;
        register_test_workspace(&mut server);
        Ok(server)
    }

    const P501_HELPER_MODE: &str = "PLATONIC_P501_HELPER_MODE";
    const P501_HELPER_ROOT: &str = "PLATONIC_P501_HELPER_ROOT";

    #[test]
    fn p501_external_daemon_helper() {
        let Ok(mode) = std::env::var(P501_HELPER_MODE) else {
            return;
        };
        let root = PathBuf::from(std::env::var_os(P501_HELPER_ROOT).unwrap());
        let workspace = root.join("workspace");
        let socket_path = root.join("d.sock");
        let config_path = workspace.join("platonic.toml");
        fs::create_dir_all(&workspace).unwrap();
        write_provider_config(&config_path, "http://127.0.0.1:1", SHELL_EXEC);

        let server = bind_test(&workspace, Some(socket_path.clone())).unwrap();
        if mode == "serve" {
            let reached = Arc::new(Barrier::new(2));
            let release = Arc::new(Barrier::new(2));
            server
                .runtime
                .set_run_execution_barriers(reached.clone(), release);
            thread::spawn(move || {
                reached.wait();
                println!("P501_EXECUTION_BLOCKED");
                std::io::stdout().flush().unwrap();
            });
            println!(
                "P501_READY socket={} socket_bytes={} config={} ledger={}",
                socket_path.display(),
                socket_path.as_os_str().as_encoded_bytes().len(),
                config_path.display(),
                server.paths().ledger_path.display()
            );
            std::io::stdout().flush().unwrap();
            server
                .serve_forever(Arc::new(AtomicBool::new(false)))
                .unwrap();
        } else if mode == "recover" {
            println!(
                "P501_RECOVERED ledger={}",
                server.paths().ledger_path.display()
            );
            std::io::stdout().flush().unwrap();
        } else {
            panic!("unknown {P501_HELPER_MODE}: {mode}");
        }
    }

    fn spawn_p501_helper(root: &Path, mode: &str) -> (Child, mpsc::Receiver<String>) {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "daemon::server::connections::tests::p501_external_daemon_helper",
                "--nocapture",
            ])
            .env(P501_HELPER_MODE, mode)
            .env(P501_HELPER_ROOT, root)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (sender, receiver) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                if sender.send(line.unwrap()).is_err() {
                    break;
                }
            }
        });
        (child, receiver)
    }

    fn wait_for_p501_marker(receiver: &mpsc::Receiver<String>, marker: &str) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let line = receiver
                .recv_timeout(remaining)
                .unwrap_or_else(|error| panic!("helper did not print {marker}: {error}"));
            if line.starts_with(marker) {
                return;
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn post_receipt_sigkill_recovers_same_admitted_run_and_question() {
        let root = tempfile::tempdir().unwrap();
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let workspace = root.path().join("workspace");
        let socket_path = root.path().join("d.sock");
        let config_path = workspace.join("platonic.toml");
        assert!(socket_path.as_os_str().as_encoded_bytes().len() < 100);

        let (mut child, output) = spawn_p501_helper(root.path(), "serve");
        wait_for_p501_marker(&output, "P501_READY");
        assert!(
            fs::symlink_metadata(&socket_path)
                .unwrap()
                .file_type()
                .is_socket()
        );

        let mut client =
            DaemonClient::connect_with_timeout(&socket_path, Duration::from_secs(5)).unwrap();
        let hello = client.hello(&workspace).unwrap();
        let status = client
            .daemon_status(None, Some(config_path.to_string_lossy().into_owned()))
            .unwrap();
        assert_eq!(hello.ledger_path, status.session.ledger_path);
        let question = "receipt survived process death";
        let admitted = client
            .run_start(
                question.into(),
                Some(config_path.to_string_lossy().into_owned()),
                false,
            )
            .unwrap();
        assert_eq!(admitted.status, RunStateName::Running);
        wait_for_p501_marker(&output, "P501_EXECUTION_BLOCKED");

        child.kill().unwrap();
        let exit = child.wait().unwrap();
        assert_eq!(exit.signal(), Some(9));
        fs::remove_file(&socket_path).unwrap();

        let (mut recovery, output) = spawn_p501_helper(root.path(), "recover");
        wait_for_p501_marker(&output, "P501_RECOVERED");
        assert!(recovery.wait().unwrap().success());

        let location = crate::paths::DefaultSqlitePath::from_path(admitted.ledger_path.into());
        let ledger = SqliteLedger::open_default_readonly(&location).unwrap();
        let recovered = ledger.read_session_run(&admitted.run_id).unwrap();
        assert_eq!(recovered.run_id, admitted.run_id);
        assert_eq!(recovered.question, question);
        assert_eq!(recovered.status, RunStateName::Interrupted);
        assert_eq!(recovered.records.len(), 2);
        assert!(matches!(
            &recovered.records[0].event,
            HarnessEvent::RunStarted(platonic_core::RunStartedEvent { run_id, .. })
                if run_id.as_str() == admitted.run_id
        ));
        assert!(matches!(
            &recovered.records[1].event,
            HarnessEvent::RunFailed { run_id, reason }
                if run_id.as_str() == admitted.run_id
                    && reason == "daemon restarted before run completed"
        ));
    }

    fn register_test_workspace(server: &mut TestDaemonServer) -> DaemonPaths {
        if server.runtime.paths.is_registered() {
            return server.runtime.paths.clone();
        }
        let response = server.handle_line(
            &json!({
                "v": 2,
                "id": "workspace_create",
                "kind": "request",
                "method": "workspace.create",
                "params": {
                    "name": server.paths().workspace_id,
                    "root": server.paths().workspace_root,
                }
            })
            .to_string(),
        );
        assert_eq!(response.kind, EnvelopeKind::Response);
        let paths = runtime_with_registered_paths(&server.runtime)
            .unwrap()
            .paths;
        server.runtime.paths = paths.clone();
        paths
    }

    #[test]
    fn bind_sets_private_socket_permissions() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_root = tempfile::tempdir().unwrap();
        let parent = socket_root.path().join("private").join("nested");
        let socket_path = parent.join("agent.sock");

        let server = bind_test(workspace.path(), Some(socket_path.clone())).unwrap();

        assert_eq!(mode(&parent), PRIVATE_DIRECTORY_MODE);
        assert_eq!(mode(&socket_path), SOCKET_MODE);
        assert!(
            server
                .paths()
                .ledger_path
                .starts_with(workspace.path().join("xdg-state"))
        );
        drop(server);
    }

    #[test]
    fn overlong_protocol_lines_are_rejected_before_dispatch() {
        for terminated in [false, true] {
            let (server_stream, mut client_stream) = UnixStream::pair().unwrap();
            let dispatched = Arc::new(AtomicUsize::new(0));
            let handler_dispatched = Arc::clone(&dispatched);
            let handler = thread::spawn(move || {
                handle_stream_lines(
                    server_stream,
                    Arc::new(AtomicBool::new(false)),
                    PathBuf::from("unused.sock"),
                    move |_| {
                        handler_dispatched.fetch_add(1, Ordering::SeqCst);
                        Envelope::error(None, None, ERROR_INTERNAL, "unexpected dispatch")
                    },
                    || {},
                )
                .unwrap();
            });

            client_stream
                .write_all(&vec![b'x'; MAX_PROTOCOL_LINE_BYTES + 1])
                .unwrap();
            if terminated {
                client_stream.write_all(b"\n").unwrap();
            }
            client_stream.shutdown(std::net::Shutdown::Write).unwrap();
            let mut response = String::new();
            BufReader::new(&client_stream)
                .read_line(&mut response)
                .unwrap();
            let response: Envelope = serde_json::from_str(&response).unwrap();

            handler.join().unwrap();
            assert_eq!(dispatched.load(Ordering::SeqCst), 0);
            assert_eq!(response.kind, EnvelopeKind::Error);
            assert_eq!(response.error.unwrap().code, ERROR_MALFORMED_REQUEST);
        }
    }

    #[test]
    fn voice_event_handlers_validate_bounds_and_durable_run_authority_without_mutation() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let server = bind_test(
            workspace.path(),
            Some(socket_dir.path().join("voice-validation.sock")),
        )
        .unwrap();
        let run_id = "run_voice_validation";
        let turn_id = "turn_run_voice_validation";

        let missing = server.handle_line(
            r#"{"v":2,"id":"read_missing","kind":"request","method":"voice.events.read","params":{"run_id":"run_absent"}}"#,
        );
        assert_eq!(missing.error.unwrap().code, ERROR_NOT_FOUND);

        seed_finished_session_run(
            &server.paths().ledger_path,
            run_id,
            "session_voice_validation",
            "durable question",
            "answer",
            true,
        );
        let empty = server.handle_line(
            &json!({
                "v": 2,
                "id": "read_empty",
                "kind": "request",
                "method": "voice.events.read",
                "params": {"run_id": run_id}
            })
            .to_string(),
        );
        assert!(
            response_result::<VoiceEventsResult>(&empty)
                .events
                .is_empty()
        );
        let missing = server.handle_line(
            r#"{"v":2,"id":"read_missing","kind":"request","method":"voice.events.read","params":{"run_id":"run_absent"}}"#,
        );
        assert_eq!(missing.error.unwrap().code, ERROR_NOT_FOUND);

        seed_running_session(
            &server.paths().ledger_path,
            "run_voice_running",
            "session_voice_running",
            "still running",
        );
        let nonterminal = commit_voice_line(
            &server,
            "run_voice_running",
            vec![spoken_voice_event(
                "run_voice_running",
                "turn_run_voice_running",
                1,
            )],
        );
        assert_eq!(nonterminal.error.unwrap().code, ERROR_MALFORMED_REQUEST);

        let mut capture_mismatch = captured_voice_event(run_id, turn_id, "durable question");
        if let VoiceEvent::VoiceCaptured {
            transcript_sha256, ..
        } = &mut capture_mismatch
        {
            *transcript_sha256 = "0".repeat(64);
        }
        let malformed = vec![
            vec![],
            vec![spoken_voice_event("run_other", turn_id, 1)],
            vec![spoken_voice_event(run_id, "turn_absent", 1)],
            vec![capture_mismatch],
            vec![VoiceEvent::VoiceInterrupted {
                run_id: RunId::new(run_id).unwrap(),
                turn_id: TurnId::new(turn_id).unwrap(),
                spoken_prefix: "heard".into(),
                delta_index: 0,
            }],
            vec![spoken_voice_event(run_id, turn_id, 1); 129],
            (0..100)
                .map(|_| spoken_voice_event(run_id, &"t".repeat(3_000), 1))
                .collect(),
            vec![VoiceEvent::VoiceInterrupted {
                run_id: RunId::new(run_id).unwrap(),
                turn_id: TurnId::new(turn_id).unwrap(),
                spoken_prefix: "x".repeat(16 * 1024 + 1),
                delta_index: 0,
            }],
        ];
        for events in malformed {
            let response = commit_voice_line(&server, run_id, events);
            assert_eq!(response.error.unwrap().code, ERROR_MALFORMED_REQUEST);
        }

        let ledger = SqliteLedger::open_default_readonly(&server.paths().default_ledger()).unwrap();
        assert!(ledger.read_voice_events(run_id).unwrap().is_empty());
        assert!(
            ledger
                .read_voice_events("run_voice_running")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn capture_only_commit_is_valid_for_a_failed_run_with_a_first_turn() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let server = bind_test(
            workspace.path(),
            Some(socket_dir.path().join("voice-failed.sock")),
        )
        .unwrap();
        let run_id = "run_voice_failed";
        let turn_id = "turn_run_voice_failed";
        let question = "capture survived failure";
        seed_failed_session_run(
            &server.paths().ledger_path,
            run_id,
            "session_voice_failed",
            question,
        );

        let response = commit_voice_line(
            &server,
            run_id,
            vec![captured_voice_event(run_id, turn_id, question)],
        );
        let committed = response_result::<VoiceEventsResult>(&response);

        assert_eq!(committed.run_id, run_id);
        assert_eq!(committed.events.len(), 1);
        assert_eq!(committed.events[0].sequence, 0);
    }

    #[test]
    fn concurrent_voice_commits_are_serialized_and_survive_restart_for_both_ledgers() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("voice-concurrency.sock");
        let server = bind_test(workspace.path(), Some(socket_path.clone())).unwrap();
        let ledger_path = server.paths().ledger_path.clone();
        let same_run = "run_voice_same";
        let conflict_run = "run_voice_conflict";
        seed_finished_session_run(
            &ledger_path,
            same_run,
            "session_voice_same",
            "question",
            "answer",
            true,
        );
        seed_finished_jsonl_session_run(
            &ledger_path,
            conflict_run,
            "session_voice_conflict",
            "question",
            "answer",
        );
        let shutdown = Arc::new(AtomicBool::new(false));
        let server_shutdown = Arc::clone(&shutdown);
        let handle = thread::spawn(move || server.serve_forever(server_shutdown).unwrap());

        let same_batch = vec![spoken_voice_event(same_run, "turn_run_voice_same", 287)];
        let [first, second] =
            concurrent_voice_commits(&socket_path, same_run, same_batch.clone(), same_batch);
        let first = first.unwrap();
        let second = second.unwrap();
        assert_eq!(first, second);
        assert_eq!(first.events.len(), 1);
        assert_eq!(first.events[0].sequence, 0);

        let [left, right] = concurrent_voice_commits(
            &socket_path,
            conflict_run,
            vec![spoken_voice_event(
                conflict_run,
                "turn_run_voice_conflict",
                10,
            )],
            vec![spoken_voice_event(
                conflict_run,
                "turn_run_voice_conflict",
                20,
            )],
        );
        let winner = match (left, right) {
            (Ok(winner), Err(error)) | (Err(error), Ok(winner)) => {
                let crate::daemon::client::ClientError::DaemonResponse(ProtocolError {
                    code,
                    message,
                }) = error
                else {
                    panic!("expected typed voice conflict");
                };
                assert_eq!(code, ERROR_VOICE_EVENTS_CONFLICT);
                assert!(message.contains("sequence 0"));
                winner
            }
            outcomes => panic!("expected one voice commit winner and one conflict: {outcomes:?}"),
        };
        assert_eq!(winner.events.len(), 1);
        assert_eq!(winner.events[0].sequence, 0);

        shutdown.store(true, Ordering::SeqCst);
        transport::wake(&socket_path);
        handle.join().unwrap();

        let restarted = bind_test(workspace.path(), Some(socket_path.clone())).unwrap();
        let restarted_shutdown = Arc::new(AtomicBool::new(false));
        let server_shutdown = Arc::clone(&restarted_shutdown);
        let restarted_handle =
            thread::spawn(move || restarted.serve_forever(server_shutdown).unwrap());
        let mut client = DaemonClient::connect(&socket_path).unwrap();
        assert_eq!(client.voice_events_read(same_run).unwrap(), first);
        assert_eq!(client.voice_events_read(conflict_run).unwrap(), winner);
        drop(client);
        restarted_shutdown.store(true, Ordering::SeqCst);
        transport::wake(&socket_path);
        restarted_handle.join().unwrap();
    }

    #[test]
    fn bind_restricts_preexisting_wide_custom_socket_parent() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_root = tempfile::tempdir().unwrap();
        let parent = socket_root.path().join("shared");
        fs::create_dir(&parent).unwrap();
        fs::set_permissions(&parent, Permissions::from_mode(0o755)).unwrap();
        let socket_path = parent.join("agent.sock");

        let server = bind_test(workspace.path(), Some(socket_path.clone())).unwrap();

        assert_eq!(mode(&parent), PRIVATE_DIRECTORY_MODE);
        assert_eq!(mode(&socket_path), SOCKET_MODE);
        drop(server);
    }

    fn test_host_socket_path(workspace: &Path) -> PathBuf {
        crate::paths::with_test_xdg(workspace, || crate::paths::host_socket_path().unwrap())
    }

    fn socket_identity(path: &Path) -> (u64, u64) {
        let metadata = fs::symlink_metadata(path).unwrap();
        (metadata.dev(), metadata.ino())
    }

    fn assert_addr_in_use(error: AppError) {
        match error {
            AppError::Io(error) => assert_eq!(error.kind(), ErrorKind::AddrInUse),
            error => panic!("expected AddrInUse, got {error}"),
        }
    }

    #[test]
    fn regular_files_survive_failed_default_and_custom_bind_attempts() {
        for custom_socket in [false, true] {
            let workspace = tempfile::tempdir().unwrap();
            let socket_root = tempfile::tempdir().unwrap();
            let socket_path = if custom_socket {
                socket_root.path().join("agent.sock")
            } else {
                test_host_socket_path(workspace.path())
            };
            fs::create_dir_all(socket_path.parent().unwrap()).unwrap();
            fs::write(&socket_path, b"rightful owner").unwrap();

            let error = bind_test(workspace.path(), custom_socket.then(|| socket_path.clone()))
                .unwrap_err();

            assert_addr_in_use(error);
            assert_eq!(fs::read(&socket_path).unwrap(), b"rightful owner");
            assert!(fs::symlink_metadata(&socket_path).unwrap().is_file());
        }
    }

    #[test]
    fn symlinks_survive_failed_default_and_custom_bind_attempts() {
        for custom_socket in [false, true] {
            let workspace = tempfile::tempdir().unwrap();
            let socket_root = tempfile::tempdir().unwrap();
            let socket_path = if custom_socket {
                socket_root.path().join("agent.sock")
            } else {
                test_host_socket_path(workspace.path())
            };
            let target = workspace.path().join("rightful-owner");
            fs::write(&target, b"rightful owner").unwrap();
            fs::create_dir_all(socket_path.parent().unwrap()).unwrap();
            symlink(&target, &socket_path).unwrap();

            let error = bind_test(workspace.path(), custom_socket.then(|| socket_path.clone()))
                .unwrap_err();

            assert_addr_in_use(error);
            assert_eq!(fs::read_link(&socket_path).unwrap(), target);
            assert_eq!(fs::read(&target).unwrap(), b"rightful owner");
        }
    }

    #[test]
    fn custom_bind_preserves_a_stale_socket() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_root = tempfile::tempdir().unwrap();
        let socket_path = socket_root.path().join("agent.sock");
        let stale_listener = UnixListener::bind(&socket_path).unwrap();
        let stale_identity = socket_identity(&socket_path);
        drop(stale_listener);

        let error = bind_test(workspace.path(), Some(socket_path.clone())).unwrap_err();

        assert_addr_in_use(error);
        assert_eq!(socket_identity(&socket_path), stale_identity);
    }

    #[test]
    fn custom_socket_collision_preserves_the_first_server() {
        let first_workspace = tempfile::tempdir().unwrap();
        let second_workspace = tempfile::tempdir().unwrap();
        let socket_root = tempfile::tempdir().unwrap();
        let socket_path = socket_root.path().join("agent.sock");
        let first_server = bind_test(first_workspace.path(), Some(socket_path.clone())).unwrap();
        let first_paths = first_server.paths().clone();
        let first_identity = socket_identity(&socket_path);

        let error = bind_test(second_workspace.path(), Some(socket_path.clone())).unwrap_err();

        assert_addr_in_use(error);
        assert_eq!(socket_identity(&socket_path), first_identity);

        let first_workspace_id = first_paths.workspace_id;
        let handle = thread::spawn(move || first_server.serve_next().unwrap());
        let mut client = DaemonClient::connect(&socket_path).unwrap();
        let hello = client.hello(first_workspace.path()).unwrap();
        assert_eq!(hello.workspace_id, first_workspace_id);
        drop(client);
        handle.join().unwrap();
    }

    #[test]
    fn default_bind_recovers_a_stale_current_user_socket() {
        assert_stale_default_socket_recovers(false);
    }

    #[test]
    fn explicit_default_bind_recovers_a_stale_current_user_socket() {
        assert_stale_default_socket_recovers(true);
    }

    fn assert_stale_default_socket_recovers(explicit_default: bool) {
        // Exercise stale recovery at the host endpoint rather than a custom
        // test socket.
        let workspace = tempfile::Builder::new().tempdir_in("/tmp").unwrap();
        let socket_path = test_host_socket_path(workspace.path());
        fs::create_dir_all(socket_path.parent().unwrap()).unwrap();
        let stale_listener = UnixListener::bind(&socket_path).unwrap();
        drop(stale_listener);

        let server = bind_test(
            workspace.path(),
            explicit_default.then(|| socket_path.clone()),
        )
        .unwrap();
        assert_eq!(server.paths().socket_path, socket_path);
        assert_eq!(mode(&socket_path), SOCKET_MODE);
        let handle = thread::spawn(move || server.serve_next().unwrap());
        let mut client = DaemonClient::connect(&socket_path).unwrap();
        client.hello(workspace.path()).unwrap();
        drop(client);
        handle.join().unwrap();
        assert!(matches!(
            fs::symlink_metadata(&socket_path),
            Err(error) if error.kind() == ErrorKind::NotFound
        ));
    }

    #[test]
    fn drop_preserves_a_replacement_socket() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_root = tempfile::tempdir().unwrap();
        let socket_path = socket_root.path().join("agent.sock");
        let server = bind_test(workspace.path(), Some(socket_path.clone())).unwrap();
        let bound_identity = socket_identity(&socket_path);
        fs::remove_file(&socket_path).unwrap();
        let replacement = UnixListener::bind(&socket_path).unwrap();
        let replacement_identity = socket_identity(&socket_path);
        assert_ne!(replacement_identity, bound_identity);

        drop(server);

        assert_eq!(socket_identity(&socket_path), replacement_identity);
        let stream = UnixStream::connect(&socket_path).unwrap();
        drop(stream);
        drop(replacement);
    }

    #[test]
    fn drop_removes_the_unchanged_bound_socket() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_root = tempfile::tempdir().unwrap();
        let socket_path = socket_root.path().join("agent.sock");
        let server = bind_test(workspace.path(), Some(socket_path.clone())).unwrap();
        assert_eq!(
            (server.endpoint.socket_device, server.endpoint.socket_inode),
            socket_identity(&socket_path)
        );

        drop(server);

        assert!(matches!(
            fs::symlink_metadata(&socket_path),
            Err(error) if error.kind() == ErrorKind::NotFound
        ));
    }
    #[test]
    fn host_hello_reports_scope_and_existing_build_provenance() {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        fs::create_dir(&workspace).unwrap();
        paths::with_test_xdg(root.path(), || {
            let server = Arc::new(HostDaemonServer::bind().unwrap());
            let socket_path = server.socket_path().to_path_buf();
            let requested_workspace_id = paths::workspace_id(&workspace).unwrap();
            assert_eq!(socket_path, paths::host_socket_path().unwrap());

            let runner = Arc::clone(&server);
            let handle = thread::spawn(move || {
                for _ in 0..3 {
                    runner.serve_next().unwrap();
                }
            });

            let mut unregistered = DaemonClient::connect(&socket_path).unwrap();
            let error = unregistered.hello(&workspace).unwrap_err();
            assert!(matches!(
                error,
                platonic_client::ClientError::DaemonResponse(ProtocolError { ref code, ref message })
                    if *code == ERROR_WORKSPACE_UNREGISTERED
                        && message.contains("platonic workspace create")
            ));
            drop(unregistered);

            let mut control = DaemonClient::connect(&socket_path).unwrap();
            assert!(control.workspace_list().unwrap().workspaces.is_empty());
            let created = control
                .workspace_create("workspace".into(), workspace.clone())
                .unwrap();
            assert_eq!(Path::new(&created.workspace.root), workspace);
            drop(control);

            let mut attached = UnixStream::connect(&socket_path).unwrap();
            writeln!(
                attached,
                r#"{{"v":2,"id":"host_hello","kind":"request","method":"hello","params":{{"workspace_root":"{}","workspace_id":"{}"}}}}"#,
                workspace.display(),
                requested_workspace_id
            )
            .unwrap();
            attached.shutdown(std::net::Shutdown::Write).unwrap();
            let mut raw = String::new();
            attached.read_to_string(&mut raw).unwrap();
            let response: Envelope = serde_json::from_str(raw.trim()).unwrap();
            let result = response_value(&response);
            handle.join().unwrap();

            assert_eq!(response.kind, EnvelopeKind::Response);
            assert_eq!(result["daemon_scope"], "host");
            assert_eq!(
                result["daemon_version"],
                platonic_protocol::PLATONIC_DIAGNOSTIC_IDENTITY
            );
            assert_eq!(result["workspace_id"], created.workspace.id);
            assert_eq!(
                Path::new(result["ledger_path"].as_str().unwrap()),
                Path::new(&created.workspace.ledger_path)
            );
        });
    }

    #[test]
    fn host_first_attach_migrates_registered_legacy_history_and_relocation_preserves_replay() {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        fs::create_dir(&workspace).unwrap();
        paths::with_test_xdg(root.path(), || {
            let workspace_id = "ws-0123456789abcdef";
            let legacy_ledger = paths::legacy_sqlite_path(&workspace).unwrap();
            fs::create_dir_all(legacy_ledger.parent().unwrap()).unwrap();
            seed_finished_session(
                &legacy_ledger,
                "run_legacy",
                "session_legacy",
                "preserved answer",
            );
            let server_db = paths::server_db_path().unwrap();
            drop(crate::server_store::ServerStore::open_or_create(&server_db).unwrap());
            let registry = rusqlite::Connection::open(&server_db).unwrap();
            registry
                .execute(
                    "INSERT INTO workspaces (id, name, root, ledger_path, created_at_ms)
                     VALUES (?1, 'adopted', ?2, ?3, 10)",
                    params![
                        workspace_id,
                        workspace.canonicalize().unwrap().to_string_lossy(),
                        legacy_ledger.to_string_lossy()
                    ],
                )
                .unwrap();
            drop(registry);

            let server = Arc::new(HostDaemonServer::bind().unwrap());
            let socket_path = server.socket_path().to_path_buf();
            let runner = Arc::clone(&server);
            let handle = thread::spawn(move || {
                for _ in 0..2 {
                    runner.serve_next().unwrap();
                }
            });

            let mut first = DaemonClient::connect(&socket_path).unwrap();
            let first_hello = first.hello(&workspace).unwrap();
            drop(first);
            let ledger_path = paths::default_sqlite_path(workspace_id).unwrap();
            assert_eq!(ledger_path, PathBuf::from(&first_hello.ledger_path));
            assert_eq!(ledger_path.file_name().unwrap(), "ledger.db");
            assert!(ledger_path.is_file());
            assert!(!legacy_ledger.exists());
            assert_eq!(first_hello.workspace_id, workspace_id);
            assert_eq!(Path::new(&first_hello.ledger_path), ledger_path);
            let migrated = crate::server_store::ServerStore::open_or_create(&server_db)
                .unwrap()
                .workspace(workspace_id)
                .unwrap()
                .unwrap();
            assert_eq!(migrated.id, workspace_id);
            assert_eq!(Path::new(&migrated.ledger_path), ledger_path);
            assert!(
                crate::replay::replay_sqlite(&ledger_path, Some("run_legacy"))
                    .unwrap()
                    .contains("preserved answer")
            );

            let relocated = root.path().join("relocated");
            fs::rename(&workspace, &relocated).unwrap();
            let store =
                crate::server_store::ServerStore::open_or_create(&paths::server_db_path().unwrap())
                    .unwrap();
            assert!(
                store
                    .relocate_workspace(workspace_id, &relocated.to_string_lossy())
                    .unwrap()
            );
            drop(store);

            let mut moved = DaemonClient::connect(&socket_path).unwrap();
            let moved_hello = moved.hello(&relocated).unwrap();
            drop(moved);
            handle.join().unwrap();
            assert_eq!(moved_hello.workspace_id, workspace_id);
            assert_eq!(Path::new(&moved_hello.ledger_path), ledger_path);
            assert!(
                crate::replay::replay_sqlite(&ledger_path, Some("run_legacy"))
                    .unwrap()
                    .contains("preserved answer")
            );
        });
    }

    #[test]
    fn daemon_status_uses_authorized_config_and_keeps_secrets_out_of_every_response() {
        const KEY_ENV: &str = "PLATO_STATUS_TEST_SECRET_NAME";
        const SECRET: &str = "plato-status-secret-sentinel-355";

        temp_env::with_var(KEY_ENV, Some(SECRET), || {
            let workspace = tempfile::tempdir().unwrap();
            let socket_dir = tempfile::tempdir().unwrap();
            let config_path = workspace.path().join("status.toml");
            fs::write(
                &config_path,
                format!(
                    r#"
[provider]
kind = "open_ai"
protocol = "responses"
model = "requested-status-alias"
api_key_env = "{KEY_ENV}"
base_url = "https://example.invalid/v1"
"#,
                ),
            )
            .unwrap();
            let server = bind_test(
                workspace.path(),
                Some(socket_dir.path().join("status.sock")),
            )
            .unwrap();
            let paths = server.paths().clone();
            let request = |id: &str, session_id: Option<&str>, config_path: &Path| {
                json!({
                    "v": 2,
                    "id": id,
                    "kind": "request",
                    "method": "daemon.status",
                    "params": {
                        "session_id": session_id,
                        "config_path": config_path
                    }
                })
                .to_string()
            };

            let first = server.handle_line(&request("status_1", None, &config_path));
            let first_wire = serde_json::to_string(&first).unwrap();
            let first: DaemonStatusResult = response_result(&first);
            thread::sleep(Duration::from_millis(5));
            let second = server.handle_line(&request("status_2", None, &config_path));
            let second_wire = serde_json::to_string(&second).unwrap();
            let second: DaemonStatusResult = response_result(&second);

            assert_eq!(first.model.requested_alias, "requested-status-alias");
            assert_eq!(first.model.served_model, None);
            assert_eq!(first.model.provider_kind, DaemonStatusProviderKind::OpenAi);
            assert_eq!(
                first.model.provider_protocol,
                platonic_protocol::DaemonStatusProviderProtocol::Responses
            );
            assert!(first.model.key_present);
            assert_eq!(
                first.daemon.endpoint_path,
                paths.socket_path.to_string_lossy()
            );
            assert_eq!(first.daemon.workspace_id, paths.workspace_id);
            assert_eq!(
                first.daemon.package_version,
                platonic_protocol::PLATONIC_PRODUCT_VERSION
            );
            assert_eq!(
                first.daemon.build_commit.as_deref(),
                (platonic_protocol::PLATONIC_BUILD_COMMIT != "unknown")
                    .then_some(platonic_protocol::PLATONIC_BUILD_COMMIT)
            );
            assert_eq!(
                first.daemon.build_date_utc.as_deref(),
                (platonic_protocol::PLATONIC_BUILD_DATE != "unknown")
                    .then_some(platonic_protocol::PLATONIC_BUILD_DATE)
            );
            assert!(second.daemon.uptime_ms > first.daemon.uptime_ms);
            assert_eq!(first.session.session_id, None);
            assert_eq!(first.session.latest_run_id, None);
            assert_eq!(first.session.human_turn_count, 0);
            assert_eq!(first.session.core_event_count, 0);
            assert_eq!(first.usage.last_run.unknown_response_count, 0);
            assert_eq!(first.usage.session.unknown_response_count, 0);
            assert_eq!(first.trust.approval_granted_count, 0);
            assert_eq!(first.trust.approval_denied_count, 0);
            assert!(!paths.ledger_path.exists());
            for wire in [&first_wire, &second_wire] {
                assert!(!wire.contains(KEY_ENV));
                assert!(!wire.contains(SECRET));
            }

            let missing = server.handle_line(&request(
                "status_missing",
                Some("missing-session"),
                &config_path,
            ));
            assert_eq!(missing.kind, EnvelopeKind::Error);
            assert_eq!(missing.error.as_ref().unwrap().code, ERROR_NOT_FOUND);
            let missing_wire = serde_json::to_string(&missing).unwrap();
            assert!(!missing_wire.contains(KEY_ENV));
            assert!(!missing_wire.contains(SECRET));

            let invalid_config = workspace.path().join("invalid-status.toml");
            fs::write(
                &invalid_config,
                format!(
                    "[provider]\nkind = \"open_ai\"\napi_key_env = \"{KEY_ENV}\"\nfuture = \"{SECRET}\"\n"
                ),
            )
            .unwrap();
            let invalid = server.handle_line(&request("status_invalid", None, &invalid_config));
            assert_eq!(invalid.kind, EnvelopeKind::Error);
            assert_eq!(invalid.error.as_ref().unwrap().code, ERROR_INTERNAL);
            let invalid_wire = serde_json::to_string(&invalid).unwrap();
            assert!(!invalid_wire.contains(KEY_ENV));
            assert!(!invalid_wire.contains(SECRET));
        });
    }

    #[test]
    fn shutdown_if_idle_keeps_the_exact_wire_contract() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let server = bind_test(
            workspace.path(),
            Some(socket_dir.path().join("omitted.sock")),
        )
        .unwrap();

        let response = server.handle_line(
            r#"{"v":2,"id":"shutdown_1","kind":"request","method":"daemon.shutdown_if_idle"}"#,
        );
        assert_eq!(
            serde_json::to_value(response).unwrap(),
            json!({
                "v": 2,
                "id": "shutdown_1",
                "kind": "response",
                "method": "daemon.shutdown_if_idle",
                "result": {"result": "shutdown"}
            })
        );

        let duplicate = server.handle_line(
            r#"{"v":2,"id":"shutdown_2","kind":"request","method":"daemon.shutdown_if_idle","params":{}}"#,
        );
        assert_eq!(duplicate.kind, EnvelopeKind::Error);
        assert_eq!(duplicate.error.unwrap().code, ERROR_DAEMON_SHUTTING_DOWN);
        for request in [
            r#"{"v":2,"id":"run_1","kind":"request","method":"run.start","params":{"question":"hello"}}"#,
            r#"{"v":2,"id":"append_1","kind":"request","method":"message.append","params":{"message":"hello"}}"#,
            r#"{"v":2,"id":"append_2","kind":"request","method":"message.append","params":{"session_id":"session_1","message":"hello"}}"#,
            r#"{"v":2,"id":"issue_prep_1","kind":"request","method":"issue-prep.start","params":{"input":"rough issue"}}"#,
        ] {
            let response = server.handle_line(request);
            assert_eq!(response.kind, EnvelopeKind::Error);
            assert_eq!(response.error.unwrap().code, ERROR_DAEMON_SHUTTING_DOWN);
        }
        assert!(server.runtime.state.lock().unwrap().runs.is_empty());

        drop(server);
        let empty_server =
            bind_test(workspace.path(), Some(socket_dir.path().join("empty.sock"))).unwrap();
        let invalid = empty_server.handle_line(
            r#"{"v":2,"id":"invalid","kind":"request","method":"daemon.shutdown_if_idle","params":{"force":true}}"#,
        );
        assert_eq!(invalid.kind, EnvelopeKind::Error);
        assert_eq!(invalid.error.unwrap().code, ERROR_MALFORMED_REQUEST);
        let invalid = empty_server.handle_line(
            r#"{"v":2,"id":"invalid_array","kind":"request","method":"daemon.shutdown_if_idle","params":[]}"#,
        );
        assert_eq!(invalid.kind, EnvelopeKind::Error);
        assert_eq!(invalid.error.unwrap().code, ERROR_MALFORMED_REQUEST);
        let response = empty_server.handle_line(
            r#"{"v":2,"id":"shutdown_3","kind":"request","method":"daemon.shutdown_if_idle","params":{}}"#,
        );
        assert_eq!(response.kind, EnvelopeKind::Response);
        assert_eq!(response_value(&response), json!({"result": "shutdown"}));
    }

    #[test]
    fn admission_closed_window_returns_typed_errors_before_teardown() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let server = bind_test(workspace.path(), Some(socket_path.clone())).unwrap();
        let barrier = Arc::new(Barrier::new(2));
        server.runtime.set_shutdown_flush_barrier(barrier.clone());
        let handle = thread::spawn(move || {
            server
                .serve_forever(Arc::new(AtomicBool::new(false)))
                .unwrap()
        });
        let mut shutdown_stream = UnixStream::connect(&socket_path).unwrap();
        let mut shutdown_reader = BufReader::new(shutdown_stream.try_clone().unwrap());

        writeln!(
            shutdown_stream,
            r#"{{"v":2,"id":"shutdown_1","kind":"request","method":"daemon.shutdown_if_idle"}}"#
        )
        .unwrap();
        let response = read_envelope(&mut shutdown_reader);
        assert_eq!(response_value(&response), json!({"result": "shutdown"}));
        assert!(socket_path.exists());

        for request in [
            r#"{"v":2,"id":"shutdown_2","kind":"request","method":"daemon.shutdown_if_idle"}"#,
            r#"{"v":2,"id":"run_1","kind":"request","method":"run.start","params":{"question":"hello"}}"#,
            r#"{"v":2,"id":"append_1","kind":"request","method":"message.append","params":{"session_id":"session_1","message":"hello"}}"#,
        ] {
            let mut stream = UnixStream::connect(&socket_path).unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            writeln!(stream, "{request}").unwrap();
            let response = read_envelope(&mut reader);
            assert_eq!(response.kind, EnvelopeKind::Error);
            assert_eq!(response.error.unwrap().code, ERROR_DAEMON_SHUTTING_DOWN);
        }

        shutdown_reader
            .get_mut()
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        barrier.wait();
        handle.join().unwrap();
        let mut trailing = String::new();
        match shutdown_reader.read_line(&mut trailing) {
            Ok(0) => {}
            Err(error) if error.kind() == ErrorKind::ConnectionReset => {}
            outcome => panic!("post-ack connection did not close: {outcome:?}"),
        }
        assert!(!socket_path.exists());
    }

    #[test]
    fn approval_paused_refusal_keeps_daemon_usable_until_retry() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let server = bind_test(workspace.path(), Some(socket_path.clone())).unwrap();
        let paths = server.paths().clone();
        let runtime = server.runtime.clone();
        let record = Arc::new(RunRecord::new(
            "run_1".into(),
            "session_1".into(),
            paths.ledger_path.clone(),
        ));
        record.approvals.lock().unwrap().insert(
            "call_1".into(),
            PendingApproval::new("session_1".into(), pending_request("run_1", "call_1")),
        );
        server.runtime.reserve_run(record.clone()).unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let handle = thread::spawn(move || server.serve_forever(shutdown).unwrap());
        let mut stream = UnixStream::connect(&socket_path).unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());

        writeln!(
            stream,
            r#"{{"v":2,"id":"shutdown_1","kind":"request","method":"daemon.shutdown_if_idle"}}"#
        )
        .unwrap();
        let refused = read_envelope(&mut reader);
        assert_eq!(refused.kind, EnvelopeKind::Response);
        assert_eq!(
            response_value(&refused),
            json!({"result": "refused_active"})
        );
        assert!(socket_path.exists());

        writeln!(
            stream,
            r#"{{"v":2,"id":"hello_1","kind":"request","method":"hello","params":{{"workspace_root":"{}","workspace_id":"{}"}}}}"#,
            paths.workspace_root.display(),
            paths.workspace_id
        )
        .unwrap();
        assert_eq!(read_envelope(&mut reader).kind, EnvelopeKind::Response);

        writeln!(
            stream,
            r#"{{"v":2,"id":"deny_1","kind":"request","method":"approval.decide","params":{{"run_id":"run_1","tool_call_id":"call_1","decision":"deny"}}}}"#
        )
        .unwrap();
        assert_eq!(read_envelope(&mut reader).kind, EnvelopeKind::Response);
        record.approvals.lock().unwrap().clear();
        runtime.finish_run(&record, "done".into(), None);
        writeln!(
            stream,
            r#"{{"v":2,"id":"shutdown_2","kind":"request","method":"daemon.shutdown_if_idle","params":{{}}}}"#
        )
        .unwrap();
        let accepted = read_envelope(&mut reader);
        assert_eq!(accepted.kind, EnvelopeKind::Response);
        assert_eq!(response_value(&accepted), json!({"result": "shutdown"}));

        handle.join().unwrap();
        assert!(!socket_path.exists());
    }

    #[test]
    fn hello_rejects_workspace_mismatch() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let server = bind_test(workspace.path(), Some(socket_path.clone())).unwrap();

        let handle = thread::spawn(move || server.serve_next().unwrap());

        let mut stream = UnixStream::connect(&socket_path).unwrap();
        writeln!(
            stream,
            r#"{{"v":2,"id":"req_1","kind":"request","method":"hello","params":{{"workspace_root":"{}","workspace_id":"wrong"}}}}"#,
            workspace.path().display()
        )
        .unwrap();
        stream.shutdown(std::net::Shutdown::Write).unwrap();

        let mut raw = String::new();
        stream.read_to_string(&mut raw).unwrap();
        handle.join().unwrap();
        let response: Envelope = serde_json::from_str(raw.trim()).unwrap();
        let error: ProtocolError = response.error.unwrap();

        assert_eq!(response.kind, EnvelopeKind::Error);
        assert_eq!(error.code, ERROR_WORKSPACE_MISMATCH);
    }

    #[test]
    fn run_start_reports_shared_driver_error() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let config_path = workspace.path().join("plato.toml");
        std::fs::write(
            &config_path,
            r#"
[provider]
kind = "open_ai"
model = "gpt-5.5"
api_key_env = "PLATO_AGENT_TEST_MISSING_KEY"
"#,
        )
        .unwrap();
        let server = bind_test(workspace.path(), Some(socket_path)).unwrap();

        let response = server.handle_line(&format!(
            r#"{{"v":2,"id":"run_1","kind":"request","method":"run.start","params":{{"question":"hello","config_path":"{}","wait":true}}}}"#,
            config_path.display()
        ));
        let error = response.error.unwrap();

        assert_eq!(response.kind, EnvelopeKind::Error);
        assert_eq!(response.method.as_deref(), Some("run.start"));
        assert_eq!(error.code, ERROR_RUN_FAILED);
        assert!(error.message.contains("PLATO_AGENT_TEST_MISSING_KEY"));
    }

    #[test]
    fn daemon_run_start_uses_shared_platonic_memory_context() {
        let provider = spawn_text_provider("done");
        let workspace = tempfile::tempdir().unwrap();
        let memory = "daemon workspace memory";
        std::fs::write(workspace.path().join("PLATONIC.md"), memory).unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let config_path = workspace.path().join("plato.toml");
        write_provider_config(&config_path, &provider.base_url, "file.read");
        let server = bind_test(workspace.path(), Some(socket_path)).unwrap();

        let response = server.handle_line(&format!(
            r#"{{"v":2,"id":"run_1","kind":"request","method":"run.start","params":{{"question":"hello","config_path":"{}","wait":true}}}}"#,
            config_path.display()
        ));
        assert_eq!(
            response.kind,
            EnvelopeKind::Response,
            "{:?}",
            response.error
        );
        let result = response_value(&response);
        let run_root = crate::paths::one_shot_run_root(
            &server.paths().server_db_path,
            result["run_id"].as_str().unwrap(),
        )
        .unwrap();
        assert!(!run_root.exists());
        let request = http_request_json(&provider.handle.join().unwrap());
        let ledger = SqliteLedger::open_readonly(&server.paths().ledger_path).unwrap();
        let records = ledger
            .read_latest_session()
            .unwrap()
            .runs
            .pop()
            .unwrap()
            .records;
        let context = records
            .iter()
            .find_map(|record| match &record.event {
                HarnessEvent::ContextBuilt { context, .. } => Some(context),
                _ => None,
            })
            .unwrap();
        let retrieved = context
            .fragments
            .iter()
            .filter(|fragment| fragment.lane == platonic_core::ContextLane::RetrievedContext)
            .collect::<Vec<_>>();
        let run_id = result["run_id"].as_str().unwrap();
        let run_log = server
            .paths()
            .ledger_path
            .parent()
            .unwrap()
            .join("runs")
            .join(format!("{run_id}.jsonl"));
        let state = rusqlite::Connection::open(&server.paths().ledger_path).unwrap();

        assert_eq!(response.kind, EnvelopeKind::Response);
        assert_eq!(result["status"], "finished");
        assert!(run_log.is_file());
        for table in ["ledger_events", "voice_events"] {
            let count: i64 = state
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 0, "new daemon run wrote {table}");
        }
        assert_eq!(
            request["messages"][0]["content"],
            format!("{}\n\n{memory}", crate::model::system_prompt())
        );
        assert_eq!(
            request["messages"][0]["content"]
                .as_str()
                .unwrap()
                .matches(memory)
                .count(),
            1
        );
        assert_eq!(retrieved.len(), 1);
        assert_eq!(retrieved[0].source, "PLATONIC.md");
        assert_eq!(retrieved[0].content, memory);
        assert!(
            context
                .fragments
                .iter()
                .find(|fragment| { fragment.lane == platonic_core::ContextLane::RecentTurns })
                .is_some_and(|fragment| !fragment.content.contains(memory))
        );
    }

    #[test]
    fn run_start_without_wait_exposes_and_clears_approval_on_same_connection() {
        let provider = spawn_tool_call_provider();
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let config_path = workspace.path().join("plato.toml");
        write_provider_config(&config_path, &provider.base_url, "file.write");
        let server = bind_test(workspace.path(), Some(socket_path.clone())).unwrap();
        let handle = thread::spawn(move || server.serve_next().unwrap());
        let mut stream = UnixStream::connect(&socket_path).unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());

        writeln!(
            stream,
            r#"{{"v":2,"id":"run_1","kind":"request","method":"run.start","params":{{"question":"write a file","config_path":"{}"}}}}"#,
            config_path.display()
        )
        .unwrap();
        let response = read_envelope(&mut reader);
        assert_eq!(response.kind, EnvelopeKind::Response);
        let result = response_value(&response);
        assert_eq!(result["status"], "running");
        assert!(result["final_answer"].is_null());
        let run_id = result["run_id"].as_str().unwrap().to_string();

        let mut approval_seen = false;
        let mut last_events = serde_json::Value::Null;
        for attempt in 0..100 {
            writeln!(
                stream,
                r#"{{"v":2,"id":"events_{attempt}","kind":"request","method":"events.stream","params":{{"run_id":"{run_id}","from_offset":0,"limit":32}}}}"#
            )
            .unwrap();
            let response = read_envelope(&mut reader);
            assert_eq!(response.kind, EnvelopeKind::Response);
            let events = response_value(&response)["events"].clone();
            last_events = events.clone();
            approval_seen = events_contain_approval_request(&events);
            if approval_seen {
                break;
            }
            thread::sleep(std::time::Duration::from_millis(25));
        }
        assert!(
            approval_seen,
            "single connection should keep serving lines; last events: {last_events}"
        );

        writeln!(
            stream,
            r#"{{"v":2,"id":"transcript_pending","kind":"request","method":"transcript.read","params":{{"run_id":"{run_id}"}}}}"#
        )
        .unwrap();
        let response = read_envelope(&mut reader);
        assert_eq!(response.kind, EnvelopeKind::Response);
        let pending = response_value(&response)["pending_approval"].clone();
        assert_eq!(pending["run_id"], run_id);
        assert_eq!(pending["tool_call_id"], "call_1");
        assert_eq!(pending["tool_name"], "file.write");
        assert_eq!(pending["effect"], "workspace_write");
        assert!(
            pending["input_preview"]
                .as_str()
                .unwrap()
                .contains("out.txt")
        );

        writeln!(
            stream,
            r#"{{"v":2,"id":"grant_1","kind":"request","method":"approval.decide","params":{{"run_id":"{run_id}","tool_call_id":"call_1","decision":"grant"}}}}"#
        )
        .unwrap();
        let response = read_envelope(&mut reader);
        assert_eq!(response.kind, EnvelopeKind::Response);
        assert_eq!(response_value(&response)["status"], "running");

        writeln!(
            stream,
            r#"{{"v":2,"id":"transcript_resolved","kind":"request","method":"transcript.read","params":{{"run_id":"{run_id}"}}}}"#
        )
        .unwrap();
        let response = read_envelope(&mut reader);
        assert_eq!(response.kind, EnvelopeKind::Response);
        assert!(response_value(&response).get("pending_approval").is_none());

        stream.shutdown(std::net::Shutdown::Write).unwrap();
        handle.join().unwrap();
        let _provider_request = provider.handle.join().unwrap();
    }

    #[test]
    fn shell_session_grant_is_session_scoped_daemon_lifetime_and_records_exact_actors() {
        let provider = spawn_shell_run_sequence_provider(&[
            "printf once > session-shell.txt",
            "printf session >> session-shell.txt",
            "printf repeat >> session-shell.txt",
            "printf other >> other-shell.txt",
            "printf restart >> restart-shell.txt",
        ]);
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let config_path = workspace.path().join("plato.toml");
        write_provider_config(&config_path, &provider.base_url, SHELL_EXEC);
        let server = bind_test(workspace.path(), Some(socket_path.clone())).unwrap();

        let first = start_test_run(&server, &config_path, "allow once");
        let session_id = first.session_id.clone();
        wait_for_pending_call(&server, &first.run_id, "call_1");
        let allowed_once = server.handle_line(&format!(
            r#"{{"v":2,"id":"allow_once","kind":"request","method":"approval.decide","params":{{"run_id":"{}","tool_call_id":"call_1","decision":"grant"}}}}"#,
            first.run_id
        ));
        assert_eq!(allowed_once.kind, EnvelopeKind::Response);
        wait_for_finished_run(&server, &first.run_id);

        let establishing = append_test_run(&server, &config_path, &session_id, "grant session");
        wait_for_pending_call(&server, &establishing.run_id, "call_1");
        let granted_session = server.handle_line(&format!(
            r#"{{"v":2,"id":"grant_session","kind":"request","method":"approval.decide","params":{{"run_id":"{}","tool_call_id":"call_1","decision":"grant_session"}}}}"#,
            establishing.run_id
        ));
        assert_eq!(granted_session.kind, EnvelopeKind::Response);
        wait_for_finished_run(&server, &establishing.run_id);

        let status = server.handle_line(&format!(
            r#"{{"v":2,"id":"status_granted","kind":"request","method":"daemon.status","params":{{"session_id":"{session_id}","config_path":"{}"}}}}"#,
            config_path.display()
        ));
        assert_eq!(status.kind, EnvelopeKind::Response);
        assert_eq!(
            response_value(&status)["trust"]["shell_session_grant"],
            true
        );

        let repeated = append_test_run(&server, &config_path, &session_id, "repeat shell");
        assert_run_finishes_without_pending_approval(&server, &repeated.run_id);

        let different = start_test_run(&server, &config_path, "different session");
        assert_ne!(different.session_id, session_id);
        wait_for_pending_call(&server, &different.run_id, "call_1");
        let other_status = server.handle_line(&format!(
            r#"{{"v":2,"id":"status_other","kind":"request","method":"daemon.status","params":{{"session_id":"{}","config_path":"{}"}}}}"#,
            different.session_id,
            config_path.display()
        ));
        assert_eq!(other_status.kind, EnvelopeKind::Response);
        assert_eq!(
            response_value(&other_status)["trust"]["shell_session_grant"],
            false
        );
        let denied = server.handle_line(&format!(
            r#"{{"v":2,"id":"deny_other","kind":"request","method":"approval.decide","params":{{"run_id":"{}","tool_call_id":"call_1","decision":"deny","reason":"not this session"}}}}"#,
            different.run_id
        ));
        assert_eq!(denied.kind, EnvelopeKind::Response);
        wait_for_finished_run(&server, &different.run_id);
        assert_run_denial_facts(
            &server.paths().ledger_path,
            &different.run_id,
            "daemon",
            "not this session",
        );

        let transcript = server.handle_line(&format!(
            r#"{{"v":2,"id":"transcript_actors","kind":"request","method":"transcript.read","params":{{"session_id":"{session_id}"}}}}"#
        ));
        let typed: TypedTranscript =
            serde_json::from_value(response_value(&transcript)["typed"].clone()).unwrap();
        let actors = typed
            .runs
            .iter()
            .flat_map(|run| &run.entries)
            .filter_map(|entry| match entry {
                TypedTranscriptEntry::Approval { actor_id, .. } => Some(actor_id.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(actors, vec!["daemon", "tui_session_grant", "session_grant"]);

        let ledger_path = server.paths().ledger_path.clone();
        assert_run_approval_facts(&ledger_path, &first.run_id, "daemon");
        assert_run_approval_facts(&ledger_path, &establishing.run_id, "tui_session_grant");
        assert_run_approval_facts(&ledger_path, &repeated.run_id, "session_grant");
        let replay = crate::replay::replay_sqlite(&ledger_path, Some(&repeated.run_id)).unwrap();
        assert!(replay.contains("approval_granted call_1 by session_grant"));

        drop(server);
        let restarted = bind_test(workspace.path(), Some(socket_path)).unwrap();
        let after_restart = append_test_run(
            &restarted,
            &config_path,
            &session_id,
            "restart expires grant",
        );
        wait_for_pending_call(&restarted, &after_restart.run_id, "call_1");
        let restarted_status = restarted.handle_line(&format!(
            r#"{{"v":2,"id":"status_restarted","kind":"request","method":"daemon.status","params":{{"session_id":"{session_id}","config_path":"{}"}}}}"#,
            config_path.display()
        ));
        assert_eq!(restarted_status.kind, EnvelopeKind::Response);
        assert_eq!(
            response_value(&restarted_status)["trust"]["shell_session_grant"],
            false
        );
        let denied = restarted.handle_line(&format!(
            r#"{{"v":2,"id":"deny_restarted","kind":"request","method":"approval.decide","params":{{"run_id":"{}","tool_call_id":"call_1","decision":"deny"}}}}"#,
            after_restart.run_id
        ));
        assert_eq!(denied.kind, EnvelopeKind::Response);
        wait_for_finished_run(&restarted, &after_restart.run_id);
        assert_run_denial_facts(
            &restarted.paths().ledger_path,
            &after_restart.run_id,
            "daemon",
            "approval denied by daemon client",
        );

        assert_eq!(
            fs::read_to_string(workspace.path().join("session-shell.txt")).unwrap(),
            "oncesessionrepeat"
        );
        assert!(!workspace.path().join("other-shell.txt").exists());
        assert!(!workspace.path().join("restart-shell.txt").exists());
        assert_eq!(provider.handle.join().unwrap().len(), 10);
    }

    #[test]
    fn run_cancel_without_wait_records_canceled_in_memory_and_sqlite() {
        let provider = spawn_tool_call_provider();
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let config_path = workspace.path().join("plato.toml");
        write_provider_config(&config_path, &provider.base_url, "file.write");
        let server = bind_test(workspace.path(), Some(socket_path)).unwrap();

        let response = server.handle_line(&format!(
            r#"{{"v":2,"id":"run_1","kind":"request","method":"run.start","params":{{"question":"write a file","config_path":"{}"}}}}"#,
            config_path.display()
        ));
        let run_id = response_value(&response)["run_id"]
            .as_str()
            .unwrap()
            .to_string();
        let record = server.runtime.state.lock().unwrap().runs[&run_id].clone();
        let deadline = Instant::now() + Duration::from_secs(2);
        while !record.approvals.lock().unwrap().contains_key("call_1") {
            assert!(Instant::now() < deadline, "approval did not become pending");
            thread::sleep(Duration::from_millis(5));
        }

        let response = server.handle_line(&format!(
            r#"{{"v":2,"id":"cancel_1","kind":"request","method":"run.cancel","params":{{"run_id":"{run_id}"}}}}"#
        ));
        assert_eq!(response.kind, EnvelopeKind::Response);
        assert_eq!(response_value(&response)["status"], "cancel_requested");
        assert!(record.approvals.lock().unwrap().is_empty());
        assert_eq!(record.pending_approval(), None);
        let transcript = server.handle_line(&format!(
            r#"{{"v":2,"id":"transcript_1","kind":"request","method":"transcript.read","params":{{"run_id":"{run_id}"}}}}"#
        ));
        assert_eq!(transcript.kind, EnvelopeKind::Response);
        assert!(
            response_value(&transcript)
                .get("pending_approval")
                .is_none()
        );
        let stale = server.handle_line(&format!(
            r#"{{"v":2,"id":"approval_1","kind":"request","method":"approval.decide","params":{{"run_id":"{run_id}","tool_call_id":"call_1","decision":"grant"}}}}"#
        ));
        assert_eq!(stale.kind, EnvelopeKind::Error);
        assert_eq!(stale.error.unwrap().code, ERROR_NOT_FOUND);
        let deadline = Instant::now() + Duration::from_secs(2);
        while matches!(
            record.status().state,
            RunStateName::Running | RunStateName::CancelRequested
        ) {
            assert!(
                Instant::now() < deadline,
                "canceled approval worker did not exit"
            );
            thread::sleep(Duration::from_millis(5));
        }

        assert_canceled_terminal(&server, &record);
        assert_eq!(record.pending_approval(), None);
        provider.handle.join().unwrap();
    }

    #[test]
    fn run_cancel_with_wait_records_canceled_in_memory_and_sqlite() {
        let provider = spawn_tool_call_provider();
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let config_path = workspace.path().join("plato.toml");
        write_provider_config(&config_path, &provider.base_url, "file.write");
        let server = bind_test(workspace.path(), Some(socket_path)).unwrap();
        let runtime = server.runtime.clone();
        let request = format!(
            r#"{{"v":2,"id":"run_1","kind":"request","method":"run.start","params":{{"question":"write a file","config_path":"{}","wait":true}}}}"#,
            config_path.display()
        );
        let run = thread::spawn(move || handle_line(&runtime, &request));

        let deadline = Instant::now() + Duration::from_secs(2);
        let record = loop {
            let record = server
                .runtime
                .state
                .lock()
                .unwrap()
                .runs
                .values()
                .next()
                .cloned();
            if let Some(record) = record
                && record.approvals.lock().unwrap().contains_key("call_1")
            {
                break record;
            }
            assert!(Instant::now() < deadline, "approval did not become pending");
            thread::sleep(Duration::from_millis(5));
        };

        let cancel = server.handle_line(&format!(
            r#"{{"v":2,"id":"cancel_1","kind":"request","method":"run.cancel","params":{{"run_id":"{}"}}}}"#,
            record.run_id
        ));
        assert_eq!(cancel.kind, EnvelopeKind::Response);
        let response = run.join().unwrap();
        assert_eq!(response.kind, EnvelopeKind::Error);
        let error = response.error.unwrap();
        assert_eq!(error.code, ERROR_RUN_FAILED);
        assert_eq!(error.message, "run did not finish: run canceled");
        assert_canceled_terminal(&server, &record);
        provider.handle.join().unwrap();
    }

    #[test]
    fn stalled_provider_cancel_reaches_terminal_daemon_readback_within_500_ms() {
        let provider = spawn_stalled_text_provider();
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let config_path = workspace.path().join("plato.toml");
        write_provider_config(&config_path, &provider.base_url, "file.read");
        let server = bind_test(workspace.path(), Some(socket_path)).unwrap();

        let started = server.handle_line(&format!(
            r#"{{"v":2,"id":"run_1","kind":"request","method":"run.start","params":{{"question":"wait for an answer","config_path":"{}"}}}}"#,
            config_path.display()
        ));
        assert_eq!(started.kind, EnvelopeKind::Response);
        let started = response_value(&started);
        let run_id = started["run_id"].as_str().unwrap().to_owned();
        let session_id = started["session_id"].as_str().unwrap().to_owned();
        let record = server.runtime.state.lock().unwrap().runs[&run_id].clone();

        provider
            .ready_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        let cancel = server.handle_line(&format!(
            r#"{{"v":2,"id":"cancel_1","kind":"request","method":"run.cancel","params":{{"run_id":"{run_id}"}}}}"#
        ));
        assert_eq!(cancel.kind, EnvelopeKind::Response);
        assert_eq!(response_value(&cancel)["status"], "cancel_requested");

        let accepted_at = Instant::now();
        while matches!(
            record.status().state,
            RunStateName::Running | RunStateName::CancelRequested
        ) {
            assert!(
                accepted_at.elapsed() < Duration::from_millis(500),
                "stalled provider remained cancel_requested"
            );
            thread::sleep(Duration::from_millis(5));
        }
        assert_canceled_terminal(&server, &record);

        let events = server.handle_line(&format!(
            r#"{{"v":2,"id":"events_1","kind":"request","method":"events.stream","params":{{"run_id":"{run_id}","from_offset":0}}}}"#
        ));
        assert_eq!(events.kind, EnvelopeKind::Response);
        assert_eq!(response_value(&events)["status"], "canceled");
        let sessions = server
            .handle_line(r#"{"v":2,"id":"sessions_1","kind":"request","method":"sessions.list"}"#);
        assert_eq!(sessions.kind, EnvelopeKind::Response);
        let sessions = response_value(&sessions);
        assert_eq!(sessions["sessions"][0]["session_id"], session_id);
        assert_eq!(sessions["sessions"][0]["status"], "canceled");
        let transcript = server.handle_line(&format!(
            r#"{{"v":2,"id":"transcript_1","kind":"request","method":"transcript.read","params":{{"run_id":"{run_id}"}}}}"#
        ));
        assert_eq!(transcript.kind, EnvelopeKind::Response);
        assert_eq!(response_value(&transcript)["status"], "canceled");
        assert!(
            response_value(&transcript)["transcript"]
                .as_str()
                .unwrap()
                .contains("run canceled")
        );
        let replay =
            crate::replay::replay_sqlite_session(&server.paths().ledger_path, &session_id).unwrap();
        assert!(replay.contains("final_phase: Failed"));
        assert!(replay.contains("run canceled"));
        let elapsed = accepted_at.elapsed();
        assert!(
            elapsed < Duration::from_millis(500),
            "daemon terminal/session readback took {elapsed:?}"
        );

        provider.release_sender.send(()).unwrap();
        let request = provider.handle.join().unwrap();
        assert!(request.starts_with("POST /chat/completions "));
        let ledger = SqliteLedger::open_readonly(&server.paths().ledger_path).unwrap();
        let run = ledger
            .read_session(&session_id)
            .unwrap()
            .runs
            .into_iter()
            .find(|run| run.run_id == run_id)
            .unwrap();
        assert_eq!(
            run.records
                .iter()
                .filter(|record| matches!(record.event, HarnessEvent::ModelRequested { .. }))
                .count(),
            1
        );
        assert_eq!(
            run.records
                .iter()
                .filter(|record| matches!(record.event, HarnessEvent::RunFailed { .. }))
                .count(),
            1
        );
        assert!(
            !run.records
                .iter()
                .any(|record| matches!(record.event, HarnessEvent::ModelResponded { .. }))
        );
    }

    #[test]
    fn different_sessions_run_concurrently_with_separate_ledgers() {
        let provider = spawn_concurrent_text_provider();
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let config_path = workspace.path().join("plato.toml");
        write_provider_config(&config_path, &provider.base_url, "file.read");
        let server = bind_test(workspace.path(), Some(socket_path)).unwrap();

        let first = server.handle_line(&format!(
            r#"{{"v":2,"id":"run_1","kind":"request","method":"run.start","params":{{"question":"question one","config_path":"{}"}}}}"#,
            config_path.display()
        ));
        assert_eq!(first.kind, EnvelopeKind::Response, "{:?}", first.error);
        let first = response_value(&first);
        assert_eq!(first["status"], "running");

        let second = server.handle_line(&format!(
            r#"{{"v":2,"id":"run_2","kind":"request","method":"run.start","params":{{"question":"question two","config_path":"{}"}}}}"#,
            config_path.display()
        ));
        assert_eq!(second.kind, EnvelopeKind::Response, "{:?}", second.error);
        let second = response_value(&second);

        let first_run = first["run_id"].as_str().unwrap();
        let first_session = first["session_id"].as_str().unwrap();
        let second_run = second["run_id"].as_str().unwrap();
        let second_session = second["session_id"].as_str().unwrap();
        assert_ne!(first_run, second_run);
        assert_ne!(first_session, second_session);

        wait_for_finished_run(&server, first_run);
        wait_for_finished_run(&server, second_run);
        let requests = provider.handle.join().unwrap();
        assert_eq!(requests.len(), 2);

        let ledger = SqliteLedger::open_readonly(&server.paths().ledger_path).unwrap();
        for (session_id, run_id, question, answer) in [
            (first_session, first_run, "question one", "answer one"),
            (second_session, second_run, "question two", "answer two"),
        ] {
            let session = ledger.read_session(session_id).unwrap();
            assert_eq!(session.runs.len(), 1);
            assert_eq!(session.runs[0].run_id, run_id);
            assert!(
                session.runs[0]
                    .records
                    .iter()
                    .all(|record| record.event.run_id().to_string() == run_id)
            );
            assert!(matches!(
                session.runs[0].records.last().map(|record| &record.event),
                Some(HarnessEvent::RunFinished { .. })
            ));

            let turns = ledger.session_turns(session_id).unwrap();
            assert_eq!(turns.len(), 1);
            assert_eq!(turns[0].question, question);
            assert_eq!(turns[0].final_answer, answer);
        }
    }

    #[test]
    fn concurrent_message_append_reserves_only_one_session_run() {
        const CLIENTS: usize = 64;

        let provider = spawn_tool_call_provider();
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let config_path = workspace.path().join("plato.toml");
        write_provider_config(&config_path, &provider.base_url, "file.write");
        let server = Arc::new(bind_test(workspace.path(), Some(socket_path)).unwrap());
        seed_finished_session(
            &server.paths().ledger_path,
            "seed_run",
            "shared_session",
            "seed answer",
        );
        let barrier = Arc::new(Barrier::new(CLIENTS + 1));
        let mut clients = Vec::new();
        for index in 0..CLIENTS {
            let server = Arc::clone(&server);
            let barrier = Arc::clone(&barrier);
            let config_path = config_path.clone();
            clients.push(thread::spawn(move || {
                barrier.wait();
                server.handle_line(&format!(
                    r#"{{"v":2,"id":"append_{index}","kind":"request","method":"message.append","params":{{"message":"write a file","session_id":"shared_session","config_path":"{}"}}}}"#,
                    config_path.display()
                ))
            }));
        }
        barrier.wait();
        let responses = clients
            .into_iter()
            .map(|client| client.join().unwrap())
            .collect::<Vec<_>>();
        let admitted_run_ids = responses
            .iter()
            .filter(|response| response.kind == EnvelopeKind::Response)
            .filter_map(|response| {
                response_value(response)["run_id"]
                    .as_str()
                    .map(str::to_string)
            })
            .collect::<Vec<_>>();

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let pending = admitted_run_ids.iter().any(|run_id| {
                server.runtime.state.lock().unwrap().runs[run_id]
                    .approvals
                    .lock()
                    .unwrap()
                    .contains_key("call_1")
            });
            if pending {
                break;
            }
            assert!(Instant::now() < deadline, "approval did not become pending");
            thread::sleep(Duration::from_millis(5));
        }
        for run_id in &admitted_run_ids {
            server.handle_line(&format!(
                r#"{{"v":2,"id":"cancel","kind":"request","method":"run.cancel","params":{{"run_id":"{run_id}"}}}}"#
            ));
        }
        provider.handle.join().unwrap();

        assert_eq!(
            admitted_run_ids.len(),
            1,
            "one session admitted multiple concurrent runs: {admitted_run_ids:?}"
        );
        assert_eq!(
            responses
                .iter()
                .filter(|response| response.kind == EnvelopeKind::Error)
                .filter(|response| {
                    response.error.as_ref().map(|error| error.code) == Some(ERROR_OVERLOAD)
                })
                .count(),
            CLIENTS - 1
        );
    }

    #[test]
    fn run_start_rejects_invalid_params_before_driver() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let server = bind_test(workspace.path(), Some(socket_path)).unwrap();

        let response = server.handle_line(
            r#"{"v":2,"id":"run_1","kind":"request","method":"run.start","params":{}}"#,
        );
        let error = response.error.unwrap();

        assert_eq!(response.kind, EnvelopeKind::Error);
        assert_eq!(error.code, ERROR_MALFORMED_REQUEST);
    }

    #[test]
    fn issue_prep_reports_validation_failure_and_run_directory() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let config_path = workspace.path().join("test-plato.toml");
        std::fs::write(&config_path, "").unwrap();
        let server = bind_test(workspace.path(), Some(socket_path)).unwrap();
        let request = serde_json::to_string(&json!({
            "v": 2,
            "id": "issue_prep_1",
            "kind": "request",
            "method": "issue-prep.start",
            "params": {
                "input": "",
                "config_path": config_path
            }
        }))
        .unwrap();

        let response = server.handle_line(&request);
        let error = response.error.unwrap();

        assert_eq!(response.kind, EnvelopeKind::Error);
        assert_eq!(error.code, ERROR_ISSUE_PREP_FAILED);
        assert!(error.message.contains("input must not be empty"));
        assert!(error.message.contains(".plato/issue-prep/run_"));
        assert!(!workspace.path().join(".plato").exists());

        let shutdown = server.handle_line(
            r#"{"v":2,"id":"shutdown","kind":"request","method":"daemon.shutdown_if_idle"}"#,
        );
        assert_eq!(shutdown.kind, EnvelopeKind::Response);
        assert_eq!(response_value(&shutdown), json!({"result": "shutdown"}));
    }

    #[test]
    fn events_stream_returns_buffered_events() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let server = bind_test(workspace.path(), Some(socket_path)).unwrap();
        let record = Arc::new(RunRecord::new(
            "run_1".into(),
            "session_1".into(),
            server.paths().ledger_path.clone(),
        ));
        record.push_event(StreamEvent::Unknown(json!({"kind": "test"})));
        server
            .runtime
            .state
            .lock()
            .unwrap()
            .runs
            .insert("run_1".into(), record);

        let response = server.handle_line(
            r#"{"v":2,"id":"events_1","kind":"request","method":"events.stream","params":{"run_id":"run_1","from_offset":0,"limit":1}}"#,
        );
        let result = response_value(&response);

        assert_eq!(response.kind, EnvelopeKind::Response);
        assert_eq!(result["run_id"], "run_1");
        assert_eq!(result["events"].as_array().unwrap().len(), 1);
        assert_eq!(result["next_offset"], 1);
    }

    #[test]
    fn events_stream_next_offset_advances_by_returned_page() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let server = bind_test(workspace.path(), Some(socket_path)).unwrap();
        let record = Arc::new(RunRecord::new(
            "run_1".into(),
            "session_1".into(),
            server.paths().ledger_path.clone(),
        ));
        record.push_event(StreamEvent::Unknown(json!({"kind": "first"})));
        record.push_event(StreamEvent::Unknown(json!({"kind": "second"})));
        server
            .runtime
            .state
            .lock()
            .unwrap()
            .runs
            .insert("run_1".into(), record);

        let first = server.handle_line(
            r#"{"v":2,"id":"events_1","kind":"request","method":"events.stream","params":{"run_id":"run_1","from_offset":0,"limit":1}}"#,
        );
        let second = server.handle_line(
            r#"{"v":2,"id":"events_2","kind":"request","method":"events.stream","params":{"run_id":"run_1","from_offset":1,"limit":1}}"#,
        );

        let first = response_value(&first);
        let second = response_value(&second);
        assert_eq!(first["next_offset"], 1);
        assert_eq!(first["events"][0]["event"]["kind"], "first");
        assert_eq!(second["next_offset"], 2);
        assert_eq!(second["events"][0]["event"]["kind"], "second");
    }

    #[test]
    fn events_stream_reports_lagged_offsets() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let server = bind_test(workspace.path(), Some(socket_path)).unwrap();
        let record = Arc::new(RunRecord::new(
            "run_1".into(),
            "session_1".into(),
            server.paths().ledger_path.clone(),
        ));
        for index in 0..(MAX_EVENT_BUFFER + 1) {
            record.push_event(StreamEvent::Unknown(json!({
                "kind": "fixture",
                "index": index
            })));
        }
        server
            .runtime
            .state
            .lock()
            .unwrap()
            .runs
            .insert("run_1".into(), record);

        let response = server.handle_line(
            r#"{"v":2,"id":"events_1","kind":"request","method":"events.stream","params":{"run_id":"run_1","from_offset":0}}"#,
        );
        let error = response.error.unwrap();

        assert_eq!(response.kind, EnvelopeKind::Error);
        assert_eq!(error.code, ERROR_LAGGED);
    }

    #[test]
    fn client_recovers_from_lag_at_tip_with_typed_final_state() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let server = bind_test(workspace.path(), Some(socket_path.clone())).unwrap();
        seed_finished_session(&server.paths().ledger_path, "run_1", "session_1", "done");
        let record = Arc::new(RunRecord::new(
            "run_1".into(),
            "session_1".into(),
            server.paths().ledger_path.clone(),
        ));
        for index in 0..(MAX_EVENT_BUFFER + 1) {
            record.push_event(StreamEvent::Unknown(json!({
                "kind": "fixture",
                "index": index
            })));
        }
        server.runtime.reserve_run(record.clone()).unwrap();
        server.runtime.finish_run(&record, "done".into(), None);
        let handle = thread::spawn(move || server.serve_next().unwrap());
        let mut client = DaemonClient::connect(&socket_path).unwrap();

        let error = client.events_stream("run_1", Some(0), 16).unwrap_err();
        assert!(matches!(
            error,
            crate::daemon::client::ClientError::DaemonResponse(ProtocolError { code, .. })
                if code == ERROR_LAGGED
        ));

        let tail = client.events_stream("run_1", None, 16).unwrap();
        assert_eq!(tail.from_offset, (MAX_EVENT_BUFFER + 1) as u64);
        assert_eq!(tail.next_offset, tail.from_offset);
        assert!(tail.events.is_empty());
        assert_eq!(tail.status, RunStateName::Finished);

        let transcript = client.transcript_read("run_1").unwrap();
        assert_eq!(transcript.status, RunStateName::Finished);
        assert_eq!(transcript.final_answer.as_deref(), Some("done"));

        drop(client);
        handle.join().unwrap();
    }

    #[test]
    fn evicted_terminal_run_streams_durable_events_without_transient_events() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let server =
            bind_test(workspace.path(), Some(socket_dir.path().join("agent.sock"))).unwrap();
        seed_finished_session(
            &server.paths().ledger_path,
            "run_0",
            "session_0",
            "persisted answer",
        );
        for index in 0..=MAX_TERMINAL_RUNS {
            let record = Arc::new(RunRecord::new(
                format!("run_{index}"),
                format!("session_{index}"),
                server.paths().ledger_path.clone(),
            ));
            record.push_event(StreamEvent::Unknown(json!({
                "kind": "fixture",
                "index": index
            })));
            server.runtime.reserve_run(record.clone()).unwrap();
            server
                .runtime
                .finish_run(&record, format!("answer {index}"), None);
        }

        let evicted = server.handle_line(
            r#"{"v":2,"id":"events_old","kind":"request","method":"events.stream","params":{"run_id":"run_0","from_offset":0}}"#,
        );
        assert_eq!(evicted.kind, EnvelopeKind::Response);
        let evicted = response_value(&evicted);
        assert_eq!(evicted["from_offset"], 0);
        assert_eq!(evicted["next_offset"], 5);
        assert_eq!(evicted["status"], "finished");
        let events = evicted["events"].as_array().unwrap();
        assert_eq!(events.len(), 5);
        assert!(
            events
                .iter()
                .all(|event| event["event"]["kind"] == "ledger")
        );

        let retained = server.handle_line(&format!(
            r#"{{"v":2,"id":"events_new","kind":"request","method":"events.stream","params":{{"run_id":"run_{MAX_TERMINAL_RUNS}"}}}}"#
        ));
        assert_eq!(retained.kind, EnvelopeKind::Response);
        let retained = response_value(&retained);
        assert_eq!(retained["from_offset"], 1);
        assert_eq!(retained["next_offset"], 1);
        assert_eq!(retained["events"], json!([]));
        assert_eq!(retained["status"], "finished");

        let transcript = server.handle_line(
            r#"{"v":2,"id":"transcript","kind":"request","method":"transcript.read","params":{"run_id":"run_0"}}"#,
        );
        assert_eq!(transcript.kind, EnvelopeKind::Response);
        let transcript = response_value(&transcript);
        assert_eq!(transcript["status"], "finished");
        assert_eq!(transcript["final_answer"], "persisted answer");

        let sessions = server
            .handle_line(r#"{"v":2,"id":"sessions","kind":"request","method":"sessions.list"}"#);
        assert_eq!(sessions.kind, EnvelopeKind::Response);
        assert_eq!(response_value(&sessions)["sessions"][0]["run_id"], "run_0");
    }

    #[test]
    fn client_recovers_pending_approval_after_lag_and_reconnect() {
        let provider = spawn_tool_call_provider();
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let config_path = workspace.path().join("plato.toml");
        write_provider_config(&config_path, &provider.base_url, "file.write");
        let server = bind_test(workspace.path(), Some(socket_path.clone())).unwrap();
        let runtime = server.runtime.clone();
        let shutdown = Arc::new(AtomicBool::new(false));
        let server_shutdown = Arc::clone(&shutdown);
        let handle = thread::spawn(move || server.serve_forever(server_shutdown).unwrap());
        let mut client = DaemonClient::connect(&socket_path).unwrap();

        let started = client
            .run_start(
                "write a file".into(),
                Some(config_path.to_string_lossy().into_owned()),
                false,
            )
            .unwrap();
        let deadline = Instant::now() + FAKE_PROVIDER_TIMEOUT;
        let pending = loop {
            match client.transcript_read(&started.run_id) {
                Ok(transcript) if transcript.pending_approval.is_some() => {
                    break transcript.pending_approval.unwrap();
                }
                Ok(_) | Err(_) => {
                    assert!(
                        Instant::now() < deadline,
                        "approval snapshot did not appear"
                    );
                    thread::sleep(Duration::from_millis(10));
                }
            }
        };
        assert_eq!(pending.run_id, started.run_id);
        assert_eq!(pending.tool_call_id, "call_1");
        assert_eq!(pending.tool_name, "file.write");
        assert_eq!(pending.effect, EffectClass::WorkspaceWrite);
        assert!(
            pending
                .reason
                .as_deref()
                .is_some_and(|reason| !reason.is_empty())
        );
        let input_preview = pending.input_preview.as_deref().unwrap();
        assert!(input_preview.contains("out.txt"));
        assert!(input_preview.contains("hello"));
        assert_eq!(pending.approval_preview, None);
        assert_eq!(pending.diff_preview, None);

        let record = runtime.state.lock().unwrap().runs[&started.run_id].clone();
        for index in 0..(MAX_EVENT_BUFFER + 1) {
            record.push_event(StreamEvent::Unknown(
                json!({"kind": "filler", "index": index}),
            ));
        }
        let error = client
            .events_stream(&started.run_id, Some(0), 16)
            .unwrap_err();
        assert!(matches!(
            error,
            crate::daemon::client::ClientError::DaemonResponse(ProtocolError { code, .. })
                if code == ERROR_LAGGED
        ));
        drop(client);

        let mut client = DaemonClient::connect(&socket_path).unwrap();
        let anchor = client.events_stream(&started.run_id, None, 16).unwrap();
        assert!(anchor.events.is_empty());
        let run = client.transcript_read(&started.run_id).unwrap();
        assert_eq!(run.pending_approval.as_ref(), Some(&pending));
        let session = client.transcript_read_session(&started.session_id).unwrap();
        assert_eq!(session.run_id, started.run_id);
        assert_eq!(session.pending_approval.as_ref(), Some(&pending));

        client
            .approval_deny(&started.run_id, "call_1", "proof complete".into())
            .unwrap();
        assert_eq!(
            client
                .transcript_read(&started.run_id)
                .unwrap()
                .pending_approval,
            None
        );
        let stale = client
            .approval_grant(&started.run_id, "call_1")
            .unwrap_err();
        assert!(matches!(
            stale,
            crate::daemon::client::ClientError::DaemonResponse(ProtocolError { code, .. })
                if code == ERROR_NOT_FOUND
        ));
        assert_eq!(
            client
                .transcript_read(&started.run_id)
                .unwrap()
                .pending_approval,
            None
        );

        drop(client);
        shutdown.store(true, Ordering::SeqCst);
        UnixStream::connect(&socket_path).unwrap();
        handle.join().unwrap();
        provider.handle.join().unwrap();
    }

    #[test]
    fn client_recovers_typed_final_answer_after_daemon_restart() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let first_socket = socket_dir.path().join("agent-1.sock");
        let first_server = bind_test(workspace.path(), Some(first_socket)).unwrap();
        seed_finished_session(
            &first_server.paths().ledger_path,
            "run_1",
            "session_1",
            "persisted answer",
        );
        drop(first_server);

        let second_socket = socket_dir.path().join("agent-2.sock");
        let second_server = bind_test(workspace.path(), Some(second_socket.clone())).unwrap();
        let handle = thread::spawn(move || second_server.serve_next().unwrap());
        let mut client = DaemonClient::connect(&second_socket).unwrap();

        let sessions = client.sessions_list().unwrap();
        assert_eq!(sessions[0].session_id, "session_1");
        assert_eq!(sessions[0].status, RunStateName::Finished);

        let transcript = client.transcript_read_session("session_1").unwrap();
        assert_eq!(transcript.run_id, "run_1");
        assert_eq!(transcript.status, RunStateName::Finished);
        assert_eq!(transcript.final_answer.as_deref(), Some("persisted answer"));

        drop(client);
        handle.join().unwrap();
    }

    #[test]
    fn transcript_read_returns_one_typed_run_or_all_session_runs_in_order() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let server =
            bind_test(workspace.path(), Some(socket_dir.path().join("agent.sock"))).unwrap();
        seed_finished_session_run(
            &server.paths().ledger_path,
            "run_1",
            "session_1",
            "first question",
            "first answer",
            true,
        );
        seed_finished_session_run(
            &server.paths().ledger_path,
            "run_2",
            "session_1",
            "second question",
            "second answer",
            false,
        );

        let run = server.handle_line(
            r#"{"v":2,"id":"run_read","kind":"request","method":"transcript.read","params":{"run_id":"run_2"}}"#,
        );
        assert_eq!(run.kind, EnvelopeKind::Response);
        assert_eq!(
            response_value(&run),
            json!({
                "run_id": "run_2",
                "status": "finished",
                "final_answer": "second answer",
                "transcript": "final_phase: Finished\nnext_seq: 5\nlegacy_agent_id: agent_1\n[turn_run_2] assistant: second answer",
                "typed": {
                    "runs": [{
                        "run_id": "run_2",
                        "session_index": 1,
                        "status": "finished",
                        "model_status": {"state": "responded"},
                        "entries": [
                            {"kind": "user", "text": "second question"},
                            {"kind": "assistant", "text": "second answer"}
                        ]
                    }]
                }
            })
        );

        let session = server.handle_line(
            r#"{"v":2,"id":"session_read","kind":"request","method":"transcript.read","params":{"session_id":"session_1"}}"#,
        );
        assert_eq!(session.kind, EnvelopeKind::Response);
        assert_eq!(
            response_value(&session),
            json!({
                "run_id": "run_2",
                "status": "finished",
                "final_answer": "second answer",
                "transcript": concat!(
                    "session_id: session_1\n",
                    "run_id: run_1\n",
                    "final_phase: Finished\n",
                    "next_seq: 5\n",
                    "legacy_agent_id: agent_1\n",
                    "[turn_run_1] assistant: first answer\n",
                    "run_id: run_2\n",
                    "final_phase: Finished\n",
                    "next_seq: 5\n",
                    "legacy_agent_id: agent_1\n",
                    "[turn_run_2] assistant: second answer"
                ),
                "typed": {
                    "runs": [
                        {
                            "run_id": "run_1",
                            "session_index": 0,
                            "status": "finished",
                            "model_status": {"state": "responded"},
                            "entries": [
                                {"kind": "user", "text": "first question"},
                                {"kind": "assistant", "text": "first answer"}
                            ]
                        },
                        {
                            "run_id": "run_2",
                            "session_index": 1,
                            "status": "finished",
                            "model_status": {"state": "responded"},
                            "entries": [
                                {"kind": "user", "text": "second question"},
                                {"kind": "assistant", "text": "second answer"}
                            ]
                        }
                    ]
                }
            })
        );
    }

    #[test]
    fn transcript_read_distinguishes_missing_from_internal_failures() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let server =
            bind_test(workspace.path(), Some(socket_dir.path().join("agent.sock"))).unwrap();

        let missing = server.handle_line(
            r#"{"v":2,"id":"transcript_1","kind":"request","method":"transcript.read","params":{"run_id":"run_missing"}}"#,
        );
        assert_eq!(missing.kind, EnvelopeKind::Error);
        assert_eq!(missing.error.unwrap().code, ERROR_NOT_FOUND);

        std::fs::create_dir_all(server.paths().ledger_path.parent().unwrap()).unwrap();
        std::fs::write(&server.paths().ledger_path, b"not a sqlite database").unwrap();
        let corrupt = server.handle_line(
            r#"{"v":2,"id":"transcript_2","kind":"request","method":"transcript.read","params":{"run_id":"run_missing"}}"#,
        );
        assert_eq!(corrupt.kind, EnvelopeKind::Error);
        assert_eq!(corrupt.error.unwrap().code, ERROR_INTERNAL);
    }

    #[test]
    fn approval_decide_updates_pending_request() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let server = bind_test(workspace.path(), Some(socket_path)).unwrap();
        let record = Arc::new(RunRecord::new(
            "run_1".into(),
            "session_1".into(),
            server.paths().ledger_path.clone(),
        ));
        record.approvals.lock().unwrap().insert(
            "call_1".into(),
            PendingApproval::new("session_1".into(), pending_request("run_1", "call_1")),
        );
        server
            .runtime
            .state
            .lock()
            .unwrap()
            .runs
            .insert("run_1".into(), record.clone());
        assert!(record.pending_approval().is_some());

        for invalid_params in [
            r#"{"run_id":"run_1","tool_call_id":"call_1","decision":"granted"}"#,
            r#"{"run_id":"run_1","tool_call_id":"call_1","decision":"grant","extra":true}"#,
            r#"{"run_id":"run_1","tool_call_id":"call_1","decision":"grant","actor":""}"#,
        ] {
            let response = server.handle_line(&format!(
                r#"{{"v":2,"id":"invalid","kind":"request","method":"approval.decide","params":{invalid_params}}}"#
            ));

            assert_eq!(response.kind, EnvelopeKind::Error);
            assert_eq!(response.error.unwrap().code, ERROR_MALFORMED_REQUEST);
            assert_eq!(record.approvals.lock().unwrap()["call_1"].decision, None);
            assert!(record.pending_approval().is_some());
        }

        for unauthorized in [
            r#"{"run_id":"run_missing","tool_call_id":"call_1","decision":"grant","actor":"jerome"}"#,
            r#"{"run_id":"run_1","tool_call_id":"call_missing","decision":"grant","actor":"jerome"}"#,
        ] {
            let response = server.handle_line(&format!(
                r#"{{"v":2,"id":"unauthorized","kind":"request","method":"approval.decide","params":{unauthorized}}}"#
            ));
            assert_eq!(response.kind, EnvelopeKind::Error);
            assert_eq!(response.error.unwrap().code, ERROR_NOT_FOUND);
            assert!(record.pending_approval().is_some());
        }

        let response = server.handle_line(
            r#"{"v":2,"id":"approval_1","kind":"request","method":"approval.decide","params":{"run_id":"run_1","tool_call_id":"call_1","decision":"grant","actor":"jerome"}}"#,
        );

        assert_eq!(response.kind, EnvelopeKind::Response);
        assert_eq!(
            record.approvals.lock().unwrap()["call_1"]
                .decision
                .as_ref()
                .map(|decision| decision.decision),
            Some(crate::daemon::protocol::ApprovalDecision::Grant)
        );
        assert_eq!(record.pending_approval(), None);
        assert!(matches!(
            &record.approvals.lock().unwrap()["call_1"]
                .decision
                .as_ref()
                .unwrap()
                .outcome,
            ExternalApprovalOutcome::Granted { actor, .. } if actor == "jerome"
        ));

        let duplicate = server.handle_line(
            r#"{"v":2,"id":"approval_duplicate","kind":"request","method":"approval.decide","params":{"run_id":"run_1","tool_call_id":"call_1","decision":"grant","actor":"jerome"}}"#,
        );
        assert_eq!(duplicate.kind, EnvelopeKind::Response);
        assert_eq!(server.runtime.session_tool_grant_count(), 0);

        let substituted_actor = server.handle_line(
            r#"{"v":2,"id":"approval_substitution","kind":"request","method":"approval.decide","params":{"run_id":"run_1","tool_call_id":"call_1","decision":"grant","actor":"mallory"}}"#,
        );
        assert_eq!(substituted_actor.kind, EnvelopeKind::Error);
        assert_eq!(substituted_actor.error.unwrap().code, ERROR_NOT_FOUND);

        let stale = server.handle_line(
            r#"{"v":2,"id":"approval_2","kind":"request","method":"approval.decide","params":{"run_id":"run_1","tool_call_id":"call_1","decision":"deny","reason":"too late"}}"#,
        );
        assert_eq!(stale.kind, EnvelopeKind::Error);
        assert_eq!(stale.error.unwrap().code, ERROR_NOT_FOUND);
        assert_eq!(
            record.approvals.lock().unwrap()["call_1"]
                .decision
                .as_ref()
                .map(|decision| decision.decision),
            Some(crate::daemon::protocol::ApprovalDecision::Grant)
        );
        assert_eq!(record.pending_approval(), None);

        record.approvals.lock().unwrap().insert(
            "call_2".into(),
            PendingApproval::new("session_1".into(), pending_request("run_1", "call_2")),
        );
        let denied = server.handle_line(
            r#"{"v":2,"id":"approval_3","kind":"request","method":"approval.decide","params":{"run_id":"run_1","tool_call_id":"call_2","decision":"deny"}}"#,
        );
        assert_eq!(denied.kind, EnvelopeKind::Response);
        assert_eq!(
            record.approvals.lock().unwrap()["call_2"]
                .decision
                .as_ref()
                .map(|decision| decision.decision),
            Some(crate::daemon::protocol::ApprovalDecision::Deny)
        );
        assert_eq!(record.pending_approval(), None);

        let duplicate = server.handle_line(
            r#"{"v":2,"id":"approval_4","kind":"request","method":"approval.decide","params":{"run_id":"run_1","tool_call_id":"call_2","decision":"deny"}}"#,
        );
        assert_eq!(duplicate.kind, EnvelopeKind::Response);
        assert_eq!(server.runtime.session_tool_grant_count(), 0);
    }

    #[test]
    fn run_cancel_synchronizes_with_pending_approval() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let server = Arc::new(bind_test(workspace.path(), Some(socket_path)).unwrap());
        let record = Arc::new(RunRecord::new(
            "run_1".into(),
            "session_1".into(),
            server.paths().ledger_path.clone(),
        ));
        server
            .runtime
            .state
            .lock()
            .unwrap()
            .runs
            .insert("run_1".into(), record.clone());
        let mut approvals = record.approvals.lock().unwrap();
        approvals.insert(
            "call_1".into(),
            PendingApproval::new("session_1".into(), pending_request("run_1", "call_1")),
        );
        let (sender, receiver) = mpsc::channel();
        let (started_sender, started_receiver) = mpsc::channel();
        let cancel_server = Arc::clone(&server);
        let cancel = thread::spawn(move || {
            started_sender.send(()).unwrap();
            sender
                .send(cancel_server.handle_line(
                    r#"{"v":2,"id":"cancel_1","kind":"request","method":"run.cancel","params":{"run_id":"run_1"}}"#,
                ))
                .unwrap();
        });

        started_receiver.recv().unwrap();
        assert!(
            receiver.recv_timeout(Duration::from_millis(100)).is_err(),
            "run.cancel must synchronize with the approval waiter mutex"
        );
        drop(approvals);
        let response = receiver.recv_timeout(Duration::from_secs(1)).unwrap();
        cancel.join().unwrap();

        assert_eq!(response.kind, EnvelopeKind::Response);
        assert!(record.cancel.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn sessions_list_reports_run_projection() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let server = bind_test(workspace.path(), Some(socket_path)).unwrap();
        let record = Arc::new(RunRecord::new(
            "run_1".into(),
            "session_1".into(),
            server.paths().ledger_path.clone(),
        ));
        server
            .runtime
            .state
            .lock()
            .unwrap()
            .runs
            .insert("run_1".into(), record);

        let response = server
            .handle_line(r#"{"v":2,"id":"sessions_1","kind":"request","method":"sessions.list"}"#);
        let result = response_value(&response);

        assert_eq!(response.kind, EnvelopeKind::Response);
        assert_eq!(result["sessions"][0]["session_id"], "session_1");
        assert_eq!(result["sessions"][0]["run_id"], "run_1");
        assert_eq!(result["sessions"][0]["status"], "running");
        assert_eq!(result["sessions"][0]["first_question"], "");
        assert_eq!(result["sessions"][0]["updated_at_ms"], 0);

        let cancel = server.handle_line(
            r#"{"v":2,"id":"cancel_1","kind":"request","method":"run.cancel","params":{"run_id":"run_1"}}"#,
        );
        assert_eq!(cancel.kind, EnvelopeKind::Response);
        assert_eq!(response_value(&cancel)["status"], "cancel_requested");

        let response = server
            .handle_line(r#"{"v":2,"id":"sessions_2","kind":"request","method":"sessions.list"}"#);
        let result = response_value(&response);
        assert_eq!(response.kind, EnvelopeKind::Response);
        assert_eq!(result["sessions"][0]["run_id"], "run_1");
        assert_eq!(result["sessions"][0]["status"], "cancel_requested");
    }

    #[test]
    fn sessions_list_reports_persisted_sessions_after_restart() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let first_socket = socket_dir.path().join("agent-1.sock");
        let first_server = bind_test(workspace.path(), Some(first_socket)).unwrap();
        let ledger_path = first_server.paths().ledger_path.clone();
        seed_finished_session_run(
            &ledger_path,
            "run_1",
            "session_1",
            "first question",
            "first answer",
            true,
        );
        seed_finished_session_run(
            &ledger_path,
            "run_2",
            "session_1",
            "approved, go ahead",
            "second answer",
            false,
        );
        drop(first_server);

        let second_socket = socket_dir.path().join("agent-2.sock");
        let second_server = bind_test(workspace.path(), Some(second_socket)).unwrap();
        let response = second_server
            .handle_line(r#"{"v":2,"id":"sessions_1","kind":"request","method":"sessions.list"}"#);
        let result = response_value(&response);

        assert_eq!(response.kind, EnvelopeKind::Response);
        assert_eq!(result["sessions"][0]["session_id"], "session_1");
        assert_eq!(result["sessions"][0]["run_id"], "run_2");
        assert_eq!(result["sessions"][0]["status"], "finished");
        assert_eq!(
            result["sessions"][0]["latest_question"],
            "approved, go ahead"
        );
        assert_eq!(result["sessions"][0]["first_question"], "first question");
        assert!(result["sessions"][0]["updated_at_ms"].as_u64().unwrap() > 0);
    }

    #[test]
    fn sessions_list_empty_fresh_workspace_does_not_create_ledger() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let server = bind_test(workspace.path(), Some(socket_path)).unwrap();
        let ledger_path = server.paths().ledger_path.clone();
        assert!(!ledger_path.exists());

        let response = server
            .handle_line(r#"{"v":2,"id":"sessions_1","kind":"request","method":"sessions.list"}"#);
        let result = response_value(&response);

        assert_eq!(response.kind, EnvelopeKind::Response);
        assert_eq!(result["sessions"].as_array().unwrap().len(), 0);
        assert!(!ledger_path.exists());
    }

    #[test]
    fn sessions_list_failure_uses_dedicated_error_code() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let server = bind_test(workspace.path(), Some(socket_path)).unwrap();
        let ledger_path = &server.paths().ledger_path;
        std::fs::create_dir_all(ledger_path.parent().unwrap()).unwrap();
        std::fs::write(ledger_path, "not a sqlite database").unwrap();

        let response = server
            .handle_line(r#"{"v":2,"id":"sessions_1","kind":"request","method":"sessions.list"}"#);
        let error = response.error.unwrap();

        assert_eq!(response.kind, EnvelopeKind::Error);
        assert_eq!(response.method.as_deref(), Some("sessions.list"));
        assert_eq!(error.code, ERROR_SESSIONS_LIST_FAILED);
    }

    #[test]
    fn sessions_list_marks_orphaned_running_session_interrupted_after_restart() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let first_socket = socket_dir.path().join("agent-1.sock");
        let first_server = bind_test(workspace.path(), Some(first_socket)).unwrap();
        let ledger_path = first_server.paths().ledger_path.clone();
        seed_running_session(&ledger_path, "run_1", "session_1", "first question");
        drop(first_server);

        let second_socket = socket_dir.path().join("agent-2.sock");
        let second_server = bind_test(workspace.path(), Some(second_socket)).unwrap();
        let response = second_server
            .handle_line(r#"{"v":2,"id":"sessions_1","kind":"request","method":"sessions.list"}"#);
        let result = response_value(&response);

        assert_eq!(response.kind, EnvelopeKind::Response);
        assert_eq!(result["sessions"][0]["session_id"], "session_1");
        assert_eq!(result["sessions"][0]["run_id"], "run_1");
        assert_eq!(result["sessions"][0]["status"], "interrupted");
    }

    #[test]
    fn daemon_startup_reconciles_orphaned_running_session_for_resume() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let first_socket = socket_dir.path().join("agent-1.sock");
        let first_server = bind_test(workspace.path(), Some(first_socket)).unwrap();
        let ledger_path = first_server.paths().ledger_path.clone();
        seed_running_session(&ledger_path, "run_1", "session_1", "first question");
        drop(first_server);

        let second_socket = socket_dir.path().join("agent-2.sock");
        let _second_server = bind_test(workspace.path(), Some(second_socket)).unwrap();
        let mut ledger = SqliteLedger::open_or_create(&ledger_path).unwrap();

        assert_eq!(
            ledger.session_summaries().unwrap()[0].status,
            RunStateName::Interrupted
        );
        ledger
            .begin_session_run(
                "session_1",
                &RunId::new("run_2").unwrap(),
                "follow up",
                false,
            )
            .unwrap();
    }

    #[test]
    fn sessions_list_reports_latest_question_preview() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let server = bind_test(workspace.path(), Some(socket_path)).unwrap();
        let long_question = format!("{}\nsecond line", "x".repeat(130));
        seed_finished_session_run(
            &server.paths().ledger_path,
            "run_1",
            "session_1",
            &long_question,
            "first answer",
            true,
        );

        let response = server
            .handle_line(r#"{"v":2,"id":"sessions_1","kind":"request","method":"sessions.list"}"#);
        let result = response_value(&response);

        assert_eq!(response.kind, EnvelopeKind::Response);
        assert_eq!(
            result["sessions"][0]["latest_question"],
            format!("{}...", "x".repeat(120))
        );
        assert_eq!(result["sessions"][0]["first_question"], long_question);
    }

    #[test]
    fn message_append_rejects_active_session_run() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let server = bind_test(workspace.path(), Some(socket_path)).unwrap();
        let record = Arc::new(RunRecord::new(
            "run_1".into(),
            "session_1".into(),
            server.paths().ledger_path.clone(),
        ));
        server
            .runtime
            .state
            .lock()
            .unwrap()
            .runs
            .insert("run_1".into(), record);

        let response = server.handle_line(
            r#"{"v":2,"id":"append_1","kind":"request","method":"message.append","params":{"session_id":"session_1","message":"again"}}"#,
        );
        let error = response.error.unwrap();

        assert_eq!(response.kind, EnvelopeKind::Error);
        assert_eq!(error.code, ERROR_OVERLOAD);
        assert!(error.message.contains("session already has an active run"));
    }

    #[test]
    fn message_append_without_wait_returns_running_by_default() {
        let provider = spawn_tool_call_provider();
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let config_path = workspace.path().join("plato.toml");
        write_provider_config(&config_path, &provider.base_url, "file.write");
        let server = bind_test(workspace.path(), Some(socket_path)).unwrap();
        seed_finished_session_run(
            &server.paths().ledger_path,
            "run_prior",
            "session_1",
            "first question",
            "first answer",
            true,
        );

        let response = server.handle_line(&format!(
            r#"{{"v":2,"id":"append_1","kind":"request","method":"message.append","params":{{"session_id":"session_1","message":"follow up","config_path":"{}"}}}}"#,
            config_path.display()
        ));
        assert_eq!(response.kind, EnvelopeKind::Response);
        let result = response_value(&response);
        assert_eq!(result["status"], "running");
        let run_id = result["run_id"].as_str().unwrap().to_string();

        let mut approval_seen = false;
        for attempt in 0..100 {
            let response = server.handle_line(&format!(
                r#"{{"v":2,"id":"events_{attempt}","kind":"request","method":"events.stream","params":{{"run_id":"{run_id}","from_offset":0,"limit":32}}}}"#
            ));
            assert_eq!(response.kind, EnvelopeKind::Response);
            let events = response_value(&response)["events"].clone();
            approval_seen = events_contain_approval_request(&events);
            if approval_seen {
                break;
            }
            thread::sleep(std::time::Duration::from_millis(25));
        }
        assert!(approval_seen);

        let response = server.handle_line(&format!(
            r#"{{"v":2,"id":"deny_1","kind":"request","method":"approval.decide","params":{{"run_id":"{run_id}","tool_call_id":"call_1","decision":"deny","reason":"test done"}}}}"#
        ));
        assert_eq!(response.kind, EnvelopeKind::Response);
        let _provider_request = provider.handle.join().unwrap();
    }

    #[test]
    fn message_append_hydrates_persisted_session_turns() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let config_path = workspace.path().join("plato.toml");
        std::fs::write(
            &config_path,
            r#"
[provider]
api_key_env = "PATH"
base_url = "http://127.0.0.1:9"
timeout_ms = 1

[limits]
token_budget = 4000
max_output_tokens = 1

[tools]
enabled = ["file.read"]
"#,
        )
        .unwrap();
        let server = bind_test(workspace.path(), Some(socket_path)).unwrap();
        seed_finished_session_run(
            &server.paths().ledger_path,
            "run_prior",
            "session_1",
            "first question",
            "first answer",
            true,
        );

        let response = server.handle_line(&format!(
            r#"{{"v":2,"id":"append_1","kind":"request","method":"message.append","params":{{"session_id":"session_1","message":"follow up","config_path":"{}","wait":true}}}}"#,
            config_path.display()
        ));
        assert_eq!(response.kind, EnvelopeKind::Error);

        let ledger = SqliteLedger::open_readonly(&server.paths().ledger_path).unwrap();
        let records = ledger
            .read_latest_session()
            .unwrap()
            .runs
            .pop()
            .unwrap()
            .records;
        let recent_turns = records
            .iter()
            .find_map(|record| match &record.event {
                HarnessEvent::ContextBuilt { context, .. } => context
                    .fragments
                    .iter()
                    .find(|fragment| fragment.source == "model.messages")
                    .map(|fragment| fragment.content.as_str()),
                _ => None,
            })
            .expect("continued run should record model messages context");

        assert!(recent_turns.contains("first question"));
        assert!(recent_turns.contains("first answer"));
        assert!(recent_turns.contains("follow up"));
    }

    struct ToolCallProvider {
        base_url: String,
        handle: thread::JoinHandle<String>,
    }

    struct TextProvider {
        base_url: String,
        handle: thread::JoinHandle<String>,
    }

    struct ConcurrentTextProvider {
        base_url: String,
        handle: thread::JoinHandle<Vec<String>>,
    }

    struct ShellRunSequenceProvider {
        base_url: String,
        handle: thread::JoinHandle<Vec<String>>,
    }

    struct StalledTextProvider {
        base_url: String,
        ready_receiver: mpsc::Receiver<()>,
        release_sender: mpsc::Sender<()>,
        handle: thread::JoinHandle<String>,
    }

    fn write_provider_config(path: &Path, base_url: &str, enabled_tool: &str) {
        let timeout_ms = FAKE_PROVIDER_TIMEOUT.as_millis();
        std::fs::write(
            path,
            format!(
                r#"
[provider]
kind = "open_ai"
model = "test-model"
api_key_env = "PATH"
base_url = "{base_url}"
timeout_ms = {timeout_ms}

[limits]
token_budget = 4000
max_output_tokens = 32
max_turns = 2

[tools]
enabled = ["{enabled_tool}"]
"#
            ),
        )
        .unwrap();
    }

    fn spawn_tool_call_provider() -> ToolCallProvider {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_http_request(&mut stream);
            let body = concat!(
                "data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"file_write\",\"arguments\":\"{\\\"path\\\":\\\"out.txt\\\",\\\"content\\\":\\\"hello\\\"}\"}}]},\"finish_reason\":null}]}\n\n",
                "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
                "data: [DONE]\n\n",
            );
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).unwrap();
            request
        });
        ToolCallProvider { base_url, handle }
    }

    fn spawn_shell_run_sequence_provider(commands: &[&str]) -> ShellRunSequenceProvider {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let commands = commands
            .iter()
            .map(|command| command.to_string())
            .collect::<Vec<_>>();
        let handle = thread::spawn(move || {
            let mut requests = Vec::new();
            for (index, command) in commands.into_iter().enumerate() {
                let (mut stream, _) = listener.accept().unwrap();
                requests.push(read_http_request(&mut stream));
                let tool_delta = json!({
                    "choices": [{
                        "index": 0,
                        "delta": {
                            "tool_calls": [{
                                "index": 0,
                                "id": format!("provider_shell_{index}"),
                                "function": {
                                    "name": "shell_exec",
                                    "arguments": json!({"command": command}).to_string()
                                }
                            }]
                        },
                        "finish_reason": null
                    }]
                });
                let tool_finish = json!({
                    "choices": [{
                        "index": 0,
                        "delta": {},
                        "finish_reason": "tool_calls"
                    }]
                });
                let body = format!("data: {tool_delta}\n\ndata: {tool_finish}\n\ndata: [DONE]\n\n");
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                )
                .unwrap();

                let (mut stream, _) = listener.accept().unwrap();
                requests.push(read_http_request(&mut stream));
                let content = json!({
                    "choices": [{
                        "index": 0,
                        "delta": {"content": "done"},
                        "finish_reason": null
                    }]
                });
                let finish = json!({
                    "choices": [{
                        "index": 0,
                        "delta": {},
                        "finish_reason": "stop"
                    }]
                });
                let body = format!("data: {content}\n\ndata: {finish}\n\ndata: [DONE]\n\n");
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                )
                .unwrap();
            }
            requests
        });
        ShellRunSequenceProvider { base_url, handle }
    }

    fn spawn_text_provider(answer: &str) -> TextProvider {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let answer = answer.to_string();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_http_request(&mut stream);
            let content = json!({
                "choices": [{
                    "index": 0,
                    "delta": {"content": answer},
                    "finish_reason": null
                }]
            });
            let finish = json!({
                "choices": [{
                    "index": 0,
                    "delta": {},
                    "finish_reason": "stop"
                }]
            });
            let body = format!("data: {content}\n\ndata: {finish}\n\ndata: [DONE]\n\n");
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
            request
        });
        TextProvider { base_url, handle }
    }

    fn spawn_stalled_text_provider() -> StalledTextProvider {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let (ready_sender, ready_receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_http_request(&mut stream);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: 4096\r\nconnection: close\r\n\r\n"
            )
            .unwrap();
            stream.flush().unwrap();
            ready_sender.send(()).unwrap();
            release_receiver
                .recv_timeout(Duration::from_secs(2))
                .unwrap();

            listener.set_nonblocking(true).unwrap();
            match listener.accept() {
                Err(error) if error.kind() == ErrorKind::WouldBlock => {}
                Ok(_) => panic!("canceled daemon run issued an extra provider request"),
                Err(error) => panic!("extra-request probe failed: {error}"),
            }
            request
        });
        StalledTextProvider {
            base_url,
            ready_receiver,
            release_sender,
            handle,
        }
    }

    fn spawn_concurrent_text_provider() -> ConcurrentTextProvider {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let handle = thread::spawn(move || {
            let deadline = Instant::now() + FAKE_PROVIDER_TIMEOUT;
            let mut clients = Vec::new();
            while clients.len() < 2 && Instant::now() < deadline {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream.set_nonblocking(false).unwrap();
                        stream
                            .set_read_timeout(Some(Duration::from_secs(2)))
                            .unwrap();
                        let request = read_http_request(&mut stream);
                        clients.push((stream, request));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("provider accept failed: {error}"),
                }
            }
            assert_eq!(
                clients.len(),
                2,
                "both daemon runs must reach the provider before either response"
            );

            let mut requests = Vec::new();
            for (mut stream, request) in clients {
                let answer = if request.contains("question one") {
                    "answer one"
                } else if request.contains("question two") {
                    "answer two"
                } else {
                    panic!("provider received an unexpected request")
                };
                let content = json!({
                    "choices": [{
                        "index": 0,
                        "delta": {"content": answer},
                        "finish_reason": null
                    }]
                });
                let finish = json!({
                    "choices": [{
                        "index": 0,
                        "delta": {},
                        "finish_reason": "stop"
                    }]
                });
                let body = format!("data: {content}\n\ndata: {finish}\n\ndata: [DONE]\n\n");
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream.write_all(response.as_bytes()).unwrap();
                requests.push(request);
            }
            requests
        });
        ConcurrentTextProvider { base_url, handle }
    }

    fn wait_for_finished_run(server: &TestDaemonServer, run_id: &str) {
        let deadline = Instant::now() + FAKE_PROVIDER_TIMEOUT;
        loop {
            let response = server.handle_line(&format!(
                r#"{{"v":2,"id":"events","kind":"request","method":"events.stream","params":{{"run_id":"{run_id}","from_offset":0,"limit":1}}}}"#
            ));
            assert_eq!(response.kind, EnvelopeKind::Response);
            let result = response_value(&response);
            match result["status"].as_str().unwrap() {
                "finished" => return,
                "running" => {}
                status => {
                    let record = server.runtime.state.lock().unwrap().runs[run_id].clone();
                    panic!(
                        "run {run_id} ended as {status}: {:?}",
                        record.status().error
                    )
                }
            }
            assert!(Instant::now() < deadline, "run {run_id} did not finish");
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn start_test_run(
        server: &TestDaemonServer,
        config_path: &Path,
        question: &str,
    ) -> RunStartResult {
        let response = server.handle_line(&format!(
            r#"{{"v":2,"id":"start","kind":"request","method":"run.start","params":{{"question":"{question}","config_path":"{}"}}}}"#,
            config_path.display()
        ));
        assert_eq!(response.kind, EnvelopeKind::Response);
        response_result(&response)
    }

    fn append_test_run(
        server: &TestDaemonServer,
        config_path: &Path,
        session_id: &str,
        message: &str,
    ) -> RunStartResult {
        let response = server.handle_line(&format!(
            r#"{{"v":2,"id":"append","kind":"request","method":"message.append","params":{{"session_id":"{session_id}","message":"{message}","config_path":"{}"}}}}"#,
            config_path.display()
        ));
        assert_eq!(response.kind, EnvelopeKind::Response);
        response_result(&response)
    }

    fn wait_for_pending_call(server: &TestDaemonServer, run_id: &str, call_id: &str) {
        let record = server.runtime.state.lock().unwrap().runs[run_id].clone();
        let deadline = Instant::now() + FAKE_PROVIDER_TIMEOUT;
        loop {
            if record
                .pending_approval()
                .is_some_and(|pending| pending.tool_call_id == call_id)
            {
                return;
            }
            assert_eq!(record.status().state, RunStateName::Running);
            assert!(
                Instant::now() < deadline,
                "run {run_id} did not publish pending approval {call_id}"
            );
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn assert_run_finishes_without_pending_approval(server: &TestDaemonServer, run_id: &str) {
        let record = server.runtime.state.lock().unwrap().runs[run_id].clone();
        let deadline = Instant::now() + FAKE_PROVIDER_TIMEOUT;
        loop {
            assert_eq!(record.pending_approval(), None, "run {run_id} prompted");
            match record.status().state {
                RunStateName::Finished => return,
                RunStateName::Running => {}
                status => panic!("run {run_id} ended as {status}"),
            }
            assert!(Instant::now() < deadline, "run {run_id} did not finish");
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn assert_run_approval_facts(ledger_path: &Path, run_id: &str, actor: &str) {
        let records = crate::ledger::read_sqlite_records(ledger_path, Some(run_id)).unwrap();
        assert_eq!(
            records
                .iter()
                .filter_map(|record| match &record.event {
                    HarnessEvent::PolicyEvaluated {
                        call_id,
                        decision: platonic_core::PolicyDecision::RequireApproval { reason },
                        ..
                    } => Some((call_id.as_str(), reason.as_str())),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![("call_1", "shell.exec requires explicit local approval")]
        );
        assert_eq!(
            records
                .iter()
                .filter_map(|record| match &record.event {
                    HarnessEvent::ApprovalGranted {
                        call_id, actor_id, ..
                    } => Some((call_id.as_str(), actor_id.as_str())),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![("call_1", actor)]
        );
    }

    fn assert_run_denial_facts(ledger_path: &Path, run_id: &str, actor: &str, reason: &str) {
        let records = crate::ledger::read_sqlite_records(ledger_path, Some(run_id)).unwrap();
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(
                    record.event,
                    HarnessEvent::PolicyEvaluated {
                        decision: platonic_core::PolicyDecision::RequireApproval { .. },
                        ..
                    }
                ))
                .count(),
            1
        );
        assert_eq!(
            records
                .iter()
                .filter_map(|record| match &record.event {
                    HarnessEvent::ApprovalDenied {
                        call_id,
                        actor_id,
                        reason,
                        ..
                    } => Some((call_id.as_str(), actor_id.as_str(), reason.as_str())),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![("call_1", actor, reason)]
        );
    }

    fn assert_canceled_terminal(server: &TestDaemonServer, record: &RunRecord) {
        let status = record.status();
        assert_eq!(status.state, RunStateName::Canceled);
        assert_eq!(
            status.error.as_deref(),
            Some("run did not finish: run canceled")
        );

        let ledger = SqliteLedger::open_readonly(&server.paths().ledger_path).unwrap();
        let session = ledger.read_session(&record.session_id).unwrap();
        let run = session
            .runs
            .iter()
            .find(|run| run.run_id == record.run_id)
            .unwrap();
        assert_eq!(run.status, RunStateName::Canceled);
        assert!(matches!(
            run.records.last().map(|record| &record.event),
            Some(HarnessEvent::RunFailed { reason, .. }) if reason == "run canceled"
        ));
    }

    fn seed_finished_session(path: &Path, run_id: &str, session_id: &str, answer: &str) {
        seed_finished_session_run(path, run_id, session_id, "question", answer, true);
    }

    fn seed_running_session(path: &Path, run_id: &str, session_id: &str, question: &str) {
        let run_id = RunId::new(run_id).unwrap();
        let mut ledger = SqliteLedger::open_or_create(path).unwrap();
        ledger
            .begin_session_run(session_id, &run_id, question, true)
            .unwrap();
        ledger
            .append(
                run_id.as_str(),
                &RecordedEvent {
                    seq: 0,
                    occurred_at_ms: 0,
                    event: HarnessEvent::RunStarted(platonic_core::RunStartedEvent {
                        run_id: run_id.clone(),
                        identity: platonic_core::RunIdentity::LegacyAgent {
                            agent_id: AgentId::new("agent_1").unwrap(),
                        },
                    }),
                },
            )
            .unwrap();
    }

    fn seed_failed_session_run(path: &Path, run_id: &str, session_id: &str, question: &str) {
        let run_id = RunId::new(run_id).unwrap();
        let turn_id = TurnId::new(format!("turn_{}", run_id.as_str())).unwrap();
        let mut ledger = SqliteLedger::open_or_create(path).unwrap();
        ledger
            .begin_session_run(session_id, &run_id, question, true)
            .unwrap();
        for (seq, event) in [
            HarnessEvent::RunStarted(platonic_core::RunStartedEvent {
                run_id: run_id.clone(),
                identity: platonic_core::RunIdentity::LegacyAgent {
                    agent_id: AgentId::new("agent_1").unwrap(),
                },
            }),
            HarnessEvent::ContextBuilt {
                run_id: run_id.clone(),
                turn_id,
                context: ContextPack {
                    token_budget: 0,
                    fragments: vec![],
                },
            },
        ]
        .into_iter()
        .enumerate()
        {
            ledger
                .append(
                    run_id.as_str(),
                    &RecordedEvent {
                        seq: seq as u64,
                        occurred_at_ms: seq as u64,
                        event,
                    },
                )
                .unwrap();
        }
        ledger
            .fail_session_run(&run_id, "synthetic failure", false)
            .unwrap();
    }

    fn seed_finished_jsonl_session_run(
        path: &Path,
        run_id: &str,
        session_id: &str,
        question: &str,
        answer: &str,
    ) {
        let location = crate::paths::DefaultSqlitePath::from_path(path.to_owned());
        let run_id = RunId::new(run_id).unwrap();
        let turn_id = TurnId::new(format!("turn_{}", run_id.as_str())).unwrap();
        let mut ledger = SqliteLedger::open_or_create_default(&location).unwrap();
        ledger
            .begin_session_run(session_id, &run_id, question, true)
            .unwrap();
        let mut recorder = EventRecorder::create_default_jsonl(&location, &run_id)
            .unwrap()
            .with_session_jsonl_creation(ledger, &run_id, true);
        for event in [
            HarnessEvent::RunStarted(platonic_core::RunStartedEvent {
                run_id: run_id.clone(),
                identity: platonic_core::RunIdentity::LegacyAgent {
                    agent_id: AgentId::new("agent_1").unwrap(),
                },
            }),
            HarnessEvent::ContextBuilt {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                context: ContextPack {
                    token_budget: 0,
                    fragments: vec![],
                },
            },
            HarnessEvent::ModelRequested {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                step: 0,
                model: ModelName::new("model_1").unwrap(),
            },
            HarnessEvent::ModelResponded {
                run_id: run_id.clone(),
                turn_id,
                step: 0,
                output: Message {
                    role: MessageRole::Assistant,
                    content: answer.into(),
                },
                proposed_calls: vec![],
                served_model: None,
                usage: Some(ModelUsage {
                    input_tokens: 0,
                    output_tokens: 0,
                }),
            },
        ] {
            recorder.record(event).unwrap();
        }
        recorder.finish_run(&run_id, answer).unwrap();
    }

    fn spoken_voice_event(run_id: &str, turn_id: &str, ttfa_ms: u64) -> VoiceEvent {
        VoiceEvent::VoiceSpoken {
            run_id: RunId::new(run_id).unwrap(),
            turn_id: TurnId::new(turn_id).unwrap(),
            ttfa_ms,
            sentence_count: 1,
            interrupted_at: None,
        }
    }

    fn captured_voice_event(run_id: &str, turn_id: &str, question: &str) -> VoiceEvent {
        VoiceEvent::VoiceCaptured {
            run_id: RunId::new(run_id).unwrap(),
            turn_id: TurnId::new(turn_id).unwrap(),
            transcript_sha256: format!("{:x}", Sha256::digest(question.as_bytes())),
            transcript_bytes: question.len().try_into().unwrap(),
            transcript_span_ms: 800,
            input_frames: 38_400,
            output_frames: 12_800,
            vad_start_sample: 320,
            vad_speech_end_sample: 11_200,
            vad_close_sample: 12_800,
            vad_close_to_final_us: 105_000,
            normalization_resampling_us: 900,
        }
    }

    fn commit_voice_line(
        server: &TestDaemonServer,
        run_id: &str,
        events: Vec<VoiceEvent>,
    ) -> Envelope {
        server.handle_line(
            &json!({
                "v": 2,
                "id": "voice_commit",
                "kind": "request",
                "method": "voice.events.commit",
                "params": {"run_id": run_id, "events": events}
            })
            .to_string(),
        )
    }

    fn concurrent_voice_commits(
        socket_path: &Path,
        run_id: &str,
        first_events: Vec<VoiceEvent>,
        second_events: Vec<VoiceEvent>,
    ) -> [platonic_client::ClientResult<VoiceEventsResult>; 2] {
        let barrier = Arc::new(Barrier::new(3));
        let first_barrier = Arc::clone(&barrier);
        let first_socket = socket_path.to_owned();
        let first_run = run_id.to_owned();
        let first = thread::spawn(move || {
            let mut client = DaemonClient::connect(&first_socket).unwrap();
            first_barrier.wait();
            client.voice_events_commit(&first_run, first_events)
        });
        let second_barrier = Arc::clone(&barrier);
        let second_socket = socket_path.to_owned();
        let second_run = run_id.to_owned();
        let second = thread::spawn(move || {
            let mut client = DaemonClient::connect(&second_socket).unwrap();
            second_barrier.wait();
            client.voice_events_commit(&second_run, second_events)
        });
        barrier.wait();
        [first.join().unwrap(), second.join().unwrap()]
    }

    fn seed_finished_session_run(
        path: &Path,
        run_id: &str,
        session_id: &str,
        question: &str,
        answer: &str,
        create_session: bool,
    ) {
        let run_id = RunId::new(run_id).unwrap();
        let turn_id = TurnId::new(format!("turn_{}", run_id.as_str())).unwrap();
        let mut ledger = SqliteLedger::open_or_create(path).unwrap();
        ledger
            .begin_session_run(session_id, &run_id, question, create_session)
            .unwrap();
        let events = vec![
            HarnessEvent::RunStarted(platonic_core::RunStartedEvent {
                run_id: run_id.clone(),
                identity: platonic_core::RunIdentity::LegacyAgent {
                    agent_id: AgentId::new("agent_1").unwrap(),
                },
            }),
            HarnessEvent::ContextBuilt {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                context: ContextPack {
                    token_budget: 0,
                    fragments: vec![],
                },
            },
            HarnessEvent::ModelRequested {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                step: 0,
                model: ModelName::new("model_1").unwrap(),
            },
            HarnessEvent::ModelResponded {
                run_id: run_id.clone(),
                turn_id,
                step: 0,
                output: Message {
                    role: MessageRole::Assistant,
                    content: answer.into(),
                },
                proposed_calls: vec![],
                served_model: None,
                usage: Some(ModelUsage {
                    input_tokens: 0,
                    output_tokens: 0,
                }),
            },
            HarnessEvent::RunFinished {
                run_id: run_id.clone(),
            },
        ];
        for (seq, event) in events.into_iter().enumerate() {
            ledger
                .append(
                    run_id.as_str(),
                    &RecordedEvent {
                        seq: seq as u64,
                        occurred_at_ms: seq as u64,
                        event,
                    },
                )
                .unwrap();
        }
        ledger.finish_session_run(&run_id, answer).unwrap();
    }

    fn read_envelope(reader: &mut BufReader<UnixStream>) -> Envelope {
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        serde_json::from_str(line.trim()).unwrap()
    }

    fn events_contain_approval_request(events: &serde_json::Value) -> bool {
        events.as_array().unwrap().iter().any(|entry| {
            entry["event"]["kind"] == "approval_requested"
                && entry["event"]["tool_call_id"] == "call_1"
        })
    }

    fn read_http_request(stream: &mut std::net::TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 1024];
        let header_end = loop {
            let read = stream.read(&mut buffer).unwrap();
            assert_ne!(read, 0, "client closed before headers");
            bytes.extend_from_slice(&buffer[..read]);
            if let Some(header_end) = find_header_end(&bytes) {
                break header_end;
            }
        };
        let headers = String::from_utf8_lossy(&bytes[..header_end]).into_owned();
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.strip_prefix("Content-Length:")
                    .or_else(|| line.strip_prefix("content-length:"))
                    .and_then(|value| value.trim().parse::<usize>().ok())
            })
            .unwrap_or(0);
        while bytes.len() < header_end + content_length {
            let read = stream.read(&mut buffer).unwrap();
            assert_ne!(read, 0, "client closed before body");
            bytes.extend_from_slice(&buffer[..read]);
        }
        String::from_utf8(bytes).unwrap()
    }

    fn http_request_json(request: &str) -> serde_json::Value {
        serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap()
    }

    fn find_header_end(bytes: &[u8]) -> Option<usize> {
        bytes
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|index| index + 4)
    }
}
