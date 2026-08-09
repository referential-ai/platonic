use std::{
    ffi::{OsStr, OsString},
    io::{self, Read},
    os::{
        fd::AsFd,
        unix::{ffi::OsStringExt, process::CommandExt},
    },
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

const DAEMON_EXECUTABLE: &str = "platonic";
const PATH_BEGIN: &[u8] = b"\x1ePLATO_USER_PATH_BEGIN_7E2F3C91\x1f";
const PATH_END: &[u8] = b"\x1ePLATO_USER_PATH_END_7E2F3C91\x1f";
const PATH_PROBE: &str = r#"command printf '\036PLATO_USER_PATH_BEGIN_7E2F3C91\037'; command printf '%s' "$PATH"; command printf '\036PLATO_USER_PATH_END_7E2F3C91\037'"#;
const PATH_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const PATH_PROBE_POLL: Duration = Duration::from_millis(20);
const STDOUT_LIMIT: usize = 256 * 1024;
const STDERR_LIMIT: usize = 512;
const READ_DRAIN_LIMIT: usize = 64 * 1024;

pub(crate) fn sibling_daemon_executable() -> io::Result<PathBuf> {
    let executable = std::env::current_exe()?;
    let parent = executable
        .parent()
        .ok_or_else(|| io::Error::other("desktop executable has no parent directory"))?;
    Ok(parent.join(DAEMON_EXECUTABLE))
}

pub(crate) fn user_launch_path() -> io::Result<OsString> {
    let shell = std::env::var_os("SHELL")
        .filter(|shell| !shell.is_empty())
        .unwrap_or_else(|| OsString::from("/bin/sh"));
    user_launch_path_from_shell(&shell, PATH_PROBE_TIMEOUT)
}

pub(crate) fn spawn_detached_daemon(
    executable: &Path,
    canonical_workspace_root: &Path,
    user_path: &OsStr,
) -> io::Result<Child> {
    if !executable.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "daemon executable path must be absolute",
        ));
    }
    if !canonical_workspace_root.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "workspace root must be absolute",
        ));
    }
    if user_path.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "user launch PATH must not be empty",
        ));
    }

    let mut command = Command::new(executable);
    command
        .arg("serve")
        .current_dir(canonical_workspace_root)
        .env("PATH", user_path)
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    command.spawn()
}

fn user_launch_path_from_shell(shell: &OsStr, timeout: Duration) -> io::Result<OsString> {
    if !Path::new(shell).is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "login shell path must be absolute",
        ));
    }

    let mut command = Command::new(shell);
    command
        .args(["-ilc", PATH_PROBE])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // SAFETY: `setsid` is the only child-side operation, and rustix implements it as
    // the async-signal-safe system call required between fork and exec.
    unsafe {
        command.pre_exec(|| {
            rustix::process::setsid()
                .map(|_| ())
                .map_err(io::Error::from)
        });
    }
    if let Some(home) = std::env::var_os("HOME").filter(|home| !home.is_empty()) {
        if !Path::new(&home).is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "HOME must be absolute for login shell PATH discovery",
            ));
        }
        command.current_dir(home);
    }
    let mut child = command.spawn()?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| io::Error::other("login shell stdout is unavailable"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| io::Error::other("login shell stderr is unavailable"))?;
    let (status, stdout, _stderr) = capture_shell_output(&mut child, stdout, stderr, timeout)?;
    let Some(status) = status else {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "login shell PATH discovery timed out after {} seconds",
                timeout.as_secs_f64()
            ),
        ));
    };

    if !status.success() {
        return Err(io::Error::other(format!(
            "login shell exited with {status}"
        )));
    }

    parse_user_launch_path(&stdout)
}

fn capture_shell_output(
    child: &mut Child,
    mut stdout: impl Read + AsFd,
    mut stderr: impl Read + AsFd,
    timeout: Duration,
) -> io::Result<(Option<ExitStatus>, Vec<u8>, Vec<u8>)> {
    set_nonblocking(&stdout)?;
    set_nonblocking(&stderr)?;
    let deadline = Instant::now() + timeout;
    let mut status = None;
    let mut stdout_output = Vec::new();
    let mut stderr_output = Vec::new();
    let mut stdout_closed = false;
    let mut stderr_closed = false;
    loop {
        if !stdout_closed {
            stdout_closed = read_available(&mut stdout, &mut stdout_output, STDOUT_LIMIT)?;
        }
        if !stderr_closed {
            stderr_closed = read_available(&mut stderr, &mut stderr_output, STDERR_LIMIT)?;
        }
        if status.is_none() {
            status = child.try_wait()?;
        }
        if status.is_some() {
            let _ = read_available(&mut stdout, &mut stdout_output, STDOUT_LIMIT)?;
            let _ = read_available(&mut stderr, &mut stderr_output, STDERR_LIMIT)?;
            return Ok((status, stdout_output, stderr_output));
        }
        let now = Instant::now();
        if now >= deadline {
            if status.is_none() {
                terminate_process_group(child)?;
            }
            let _ = read_available(&mut stdout, &mut stdout_output, STDOUT_LIMIT);
            let _ = read_available(&mut stderr, &mut stderr_output, STDERR_LIMIT);
            return Ok((status, stdout_output, stderr_output));
        }
        thread::sleep(PATH_PROBE_POLL.min(deadline - now));
    }
}

fn set_nonblocking(stream: &impl AsFd) -> io::Result<()> {
    let flags = rustix::fs::fcntl_getfl(stream)?;
    rustix::fs::fcntl_setfl(stream, flags | rustix::fs::OFlags::NONBLOCK).map_err(io::Error::from)
}

fn kill_process_group(child: &Child) {
    if let Some(group) = rustix::process::Pid::from_raw(child.id() as i32) {
        let _ = rustix::process::kill_process_group(group, rustix::process::Signal::KILL);
    }
}

fn terminate_process_group(child: &mut Child) -> io::Result<()> {
    kill_process_group(child);
    let _ = child.kill();
    child.wait().map(|_| ())
}

fn read_available(stream: &mut impl Read, output: &mut Vec<u8>, limit: usize) -> io::Result<bool> {
    let mut buffer = [0_u8; 4096];
    let mut drained = 0;
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => return Ok(true),
            Ok(read) => {
                append_capped_tail(output, &buffer[..read], limit);
                drained += read;
                if drained >= READ_DRAIN_LIMIT {
                    return Ok(false);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(false),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

fn append_capped_tail(output: &mut Vec<u8>, bytes: &[u8], limit: usize) {
    if bytes.len() >= limit {
        output.clear();
        output.extend_from_slice(&bytes[bytes.len() - limit..]);
        return;
    }
    let overflow = output
        .len()
        .saturating_add(bytes.len())
        .saturating_sub(limit);
    if overflow > 0 {
        output.drain(..overflow);
    }
    output.extend_from_slice(bytes);
}

fn parse_user_launch_path(output: &[u8]) -> io::Result<OsString> {
    let begin = output
        .windows(PATH_BEGIN.len())
        .rposition(|window| window == PATH_BEGIN)
        .map(|position| position + PATH_BEGIN.len())
        .ok_or_else(|| io::Error::other("login shell did not return a delimited PATH"))?;
    let end = output[begin..]
        .windows(PATH_END.len())
        .position(|window| window == PATH_END)
        .map(|position| begin + position)
        .ok_or_else(|| io::Error::other("login shell did not finish PATH discovery"))?;
    if begin == end {
        return Err(io::Error::other("login shell returned an empty PATH"));
    }
    let path = &output[begin..end];
    std::str::from_utf8(path).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "login shell returned a non-UTF-8 PATH",
        )
    })?;
    Ok(OsString::from_vec(path.to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        os::unix::{ffi::OsStrExt, fs::PermissionsExt},
        sync::{Arc, Barrier},
    };
    use tempfile::{tempdir, tempdir_in};

    #[test]
    fn parses_delimited_path_around_shell_noise() {
        let output = [
            b"startup noise\n".as_slice(),
            PATH_BEGIN,
            b"/launch/bin:/usr/bin",
            PATH_END,
            b"\nlogout noise",
        ]
        .concat();

        let path = parse_user_launch_path(&output).expect("parse PATH");

        assert_eq!(path.as_bytes(), b"/launch/bin:/usr/bin");
    }

    #[test]
    fn rejects_empty_delimited_path() {
        let output = [PATH_BEGIN, PATH_END].concat();

        let error = parse_user_launch_path(&output).expect_err("reject empty PATH");

        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert!(error.to_string().contains("empty PATH"));
    }

    #[test]
    fn login_shell_path_discovery_is_bounded() {
        let directory = tempdir().expect("create temp directory");
        let shell = directory.path().join("stalling-shell");
        write_executable(&shell, "#!/bin/sh\nsleep 30\n");
        let started = Instant::now();

        let error = user_launch_path_from_shell(&shell.into_os_string(), Duration::from_millis(50))
            .expect_err("time out stalled shell");

        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn login_shell_path_discovery_does_not_wait_for_background_children() {
        let directory = tempdir().expect("create temp directory");
        let shell = directory.path().join("background-shell");
        let pid_file = directory.path().join("background.pid");
        write_executable(
            &shell,
            &format!(
                "#!/bin/sh\nsleep 30 &\nprintf '%s' $! > '{}'\nprintf '\\036PLATO_USER_PATH_BEGIN_7E2F3C91\\037/launch/bin:/usr/bin\\036PLATO_USER_PATH_END_7E2F3C91\\037'\n",
                pid_file.display()
            ),
        );
        let started = Instant::now();

        let path = user_launch_path_from_shell(&shell.into_os_string(), Duration::from_secs(5))
            .expect("read PATH without waiting for background child");

        assert_eq!(path.as_bytes(), b"/launch/bin:/usr/bin");
        assert!(started.elapsed() < Duration::from_secs(2));
        let pid: i32 = fs::read_to_string(pid_file).unwrap().parse().unwrap();
        rustix::process::kill_process(
            rustix::process::Pid::from_raw(pid).unwrap(),
            rustix::process::Signal::KILL,
        )
        .unwrap();
    }

    #[test]
    fn login_shell_path_discovery_times_out_continuous_output() {
        let directory = tempdir().expect("create temp directory");
        let shell = directory.path().join("noisy-shell");
        write_executable(&shell, "#!/bin/sh\nwhile :; do printf x; done\n");
        let started = Instant::now();

        let error = user_launch_path_from_shell(&shell.into_os_string(), Duration::from_millis(50))
            .expect_err("time out noisy shell");

        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn executable_fixture_publication_survives_parallel_replacement() {
        const PUBLISHERS: usize = 2;
        const EXECUTORS: usize = 2;
        const ITERATIONS: usize = 4;

        let directory = tempdir().expect("create temp directory");
        let executable = directory.path().join("parallel-shell");
        let contents = "#!/bin/sh\nexit 0\n";
        write_executable(&executable, contents);
        let busy_inode = fs::OpenOptions::new()
            .write(true)
            .open(&executable)
            .expect("hold published executable open for writing");
        write_executable(&executable, contents);
        let start = Arc::new(Barrier::new(PUBLISHERS + EXECUTORS));

        thread::scope(|scope| {
            for _ in 0..PUBLISHERS {
                let executable = &executable;
                let start = Arc::clone(&start);
                scope.spawn(move || {
                    start.wait();
                    for _ in 0..ITERATIONS {
                        write_executable(executable, contents);
                    }
                });
            }
            for _ in 0..EXECUTORS {
                let executable = &executable;
                let start = Arc::clone(&start);
                scope.spawn(move || {
                    start.wait();
                    for _ in 0..ITERATIONS {
                        let status = Command::new(executable)
                            .status()
                            .expect("execute fixture during parallel publication");
                        assert!(status.success());
                    }
                });
            }
        });

        drop(busy_inode);
    }

    #[test]
    fn rejects_non_utf8_launch_path_before_daemon_spawn() {
        let output = [PATH_BEGIN, b"/launch/\xff/bin:/usr/bin", PATH_END].concat();

        let error = parse_user_launch_path(&output).expect_err("reject non-UTF-8 PATH");

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("non-UTF-8 PATH"));
    }

    #[test]
    fn detached_daemon_receives_explicit_user_path() {
        let directory = tempdir().expect("create temp directory");
        let workspace_root = directory.path().canonicalize().expect("canonical root");
        let daemon = workspace_root.join("fake-daemon");
        let observed = workspace_root.join("observed-path");
        write_executable(
            &daemon,
            "#!/bin/sh\nprintf '%s' \"$PATH\" > observed-path\n",
        );

        let mut child = spawn_detached_daemon(
            &daemon,
            &workspace_root,
            OsStr::new("/launch-only/bin:/usr/bin"),
        )
        .expect("spawn fake daemon");
        let status = child.wait().expect("wait for fake daemon");

        assert!(status.success());
        assert_eq!(
            fs::read(observed).expect("read observed PATH"),
            b"/launch-only/bin:/usr/bin"
        );
    }

    #[test]
    fn detached_daemon_rejects_invalid_paths() {
        let directory = tempdir().expect("create temp directory");
        let workspace_root = directory.path().canonicalize().expect("canonical root");
        let executable = workspace_root.join("platonic");

        let executable_error = spawn_detached_daemon(
            Path::new("platonic"),
            &workspace_root,
            OsStr::new("/usr/bin"),
        )
        .expect_err("reject relative executable");
        let workspace_error =
            spawn_detached_daemon(&executable, Path::new("workspace"), OsStr::new("/usr/bin"))
                .expect_err("reject relative workspace");
        let path_error = spawn_detached_daemon(&executable, &workspace_root, OsStr::new(""))
            .expect_err("reject empty PATH");

        assert_eq!(executable_error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(workspace_error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(path_error.kind(), io::ErrorKind::InvalidInput);
    }

    fn write_executable(path: &Path, contents: &str) {
        let parent = path.parent().expect("executable fixture parent");
        let publication = tempdir_in(parent).expect("create executable fixture directory");
        let temporary = publication.path().join("executable");
        let status = Command::new("/bin/sh")
            .args(["-c", "printf '%s' \"$2\" > \"$1\"", "fixture-writer"])
            .arg(&temporary)
            .arg(contents)
            .status()
            .expect("write executable fixture");
        assert!(status.success());
        let mut permissions = fs::metadata(&temporary)
            .expect("read executable fixture metadata")
            .permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&temporary, permissions).expect("set executable mode");
        fs::rename(temporary, path).expect("publish executable fixture");
    }
}
