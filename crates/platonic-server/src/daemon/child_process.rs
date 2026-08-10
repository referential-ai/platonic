use std::{
    io,
    process::{ChildStderr, ChildStdin, ChildStdout, Command, ExitStatus},
    time::{Duration, Instant},
};

const PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(10);

pub(super) struct ProcessTreeChild {
    child: platform::PlatformChild,
}

impl ProcessTreeChild {
    pub(super) fn spawn(command: &mut Command) -> io::Result<Self> {
        Ok(Self {
            child: platform::PlatformChild::spawn(command)?,
        })
    }

    pub(super) fn id(&self) -> u32 {
        self.child.id()
    }

    pub(super) fn take_stdin(&mut self) -> Option<ChildStdin> {
        self.child.take_stdin()
    }

    pub(super) fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.child.take_stdout()
    }

    pub(super) fn take_stderr(&mut self) -> Option<ChildStderr> {
        self.child.take_stderr()
    }

    pub(super) fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.child.try_wait()
    }

    pub(super) fn observe_descendants(&mut self) -> io::Result<()> {
        self.child.observe_descendants()
    }

    pub(super) fn wait_for_exit(&mut self, timeout: Duration) -> io::Result<Option<ExitStatus>> {
        let deadline = Instant::now() + timeout;
        loop {
            self.observe_descendants()?;
            if let Some(status) = self.try_wait()? {
                return Ok(Some(status));
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            std::thread::park_timeout(PROCESS_POLL_INTERVAL.min(deadline - now));
        }
    }

    pub(super) fn terminate_tree(
        &mut self,
        grace: Duration,
        kill_wait: Duration,
    ) -> io::Result<ExitStatus> {
        self.child.terminate_tree(grace, kill_wait)
    }

    pub(super) fn assert_zero_residual(&mut self, timeout: Duration) -> io::Result<()> {
        self.child.assert_zero_residual(timeout)
    }
}

impl Drop for ProcessTreeChild {
    fn drop(&mut self) {
        self.child.kill_on_drop();
    }
}

#[cfg(unix)]
mod platform {
    use super::PROCESS_POLL_INTERVAL;
    use rustix::io::Errno;
    // The process-tree walk reads /proc, so these are Linux-only in practice;
    // macOS compiles this module without them and its clippy said so.
    #[cfg(target_os = "linux")]
    use std::collections::{HashSet, VecDeque};
    #[cfg(target_os = "linux")]
    use std::fs;
    use std::{
        collections::HashMap,
        io,
        os::unix::process::CommandExt,
        process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, ExitStatus},
        time::{Duration, Instant},
    };

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct ObservedProcess {
        pid: i32,
        start_time: u64,
    }

    pub(super) struct PlatformChild {
        child: Child,
        process_group: rustix::process::Pid,
        observed: HashMap<i32, ObservedProcess>,
        status: Option<ExitStatus>,
    }

    impl PlatformChild {
        pub(super) fn spawn(command: &mut Command) -> io::Result<Self> {
            let child = command.process_group(0).spawn()?;
            let process_group = rustix::process::Pid::from_raw(child.id() as i32)
                .ok_or_else(|| io::Error::other("run child had an invalid process id"))?;
            Ok(Self {
                child,
                process_group,
                observed: HashMap::new(),
                status: None,
            })
        }

        pub(super) fn id(&self) -> u32 {
            self.child.id()
        }

        pub(super) fn take_stdin(&mut self) -> Option<ChildStdin> {
            self.child.stdin.take()
        }

        pub(super) fn take_stdout(&mut self) -> Option<ChildStdout> {
            self.child.stdout.take()
        }

        pub(super) fn take_stderr(&mut self) -> Option<ChildStderr> {
            self.child.stderr.take()
        }

        pub(super) fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
            if self.status.is_none() {
                self.status = self.child.try_wait()?;
            }
            Ok(self.status)
        }

        pub(super) fn observe_descendants(&mut self) -> io::Result<()> {
            #[cfg(target_os = "linux")]
            {
                let processes = linux_processes()?;
                let mut parents = HashSet::from([self.child.id() as i32]);
                parents.extend(self.observed.keys().copied());
                let mut queue = VecDeque::from_iter(parents.iter().copied());
                while let Some(parent) = queue.pop_front() {
                    for process in processes
                        .values()
                        .filter(|process| process.parent == parent)
                    {
                        if process.pid != self.child.id() as i32
                            && self
                                .observed
                                .insert(
                                    process.pid,
                                    ObservedProcess {
                                        pid: process.pid,
                                        start_time: process.start_time,
                                    },
                                )
                                .is_none()
                        {
                            queue.push_back(process.pid);
                        }
                    }
                }
            }
            Ok(())
        }

        pub(super) fn terminate_tree(
            &mut self,
            grace: Duration,
            kill_wait: Duration,
        ) -> io::Result<ExitStatus> {
            self.observe_descendants()?;
            self.signal_observed(rustix::process::Signal::TERM);
            let _ = rustix::process::kill_process_group(
                self.process_group,
                rustix::process::Signal::TERM,
            );

            let grace_deadline = Instant::now() + grace;
            loop {
                self.observe_descendants()?;
                self.signal_observed(rustix::process::Signal::TERM);
                if self.try_wait()?.is_some() && !self.any_observed_alive() {
                    break;
                }
                let now = Instant::now();
                if now >= grace_deadline {
                    break;
                }
                std::thread::park_timeout(PROCESS_POLL_INTERVAL.min(grace_deadline - now));
            }

            if self.try_wait()?.is_none() || self.any_observed_alive() || self.group_alive()? {
                self.freeze_descendant_tree()?;
                self.signal_observed(rustix::process::Signal::KILL);
                let _ = rustix::process::kill_process_group(
                    self.process_group,
                    rustix::process::Signal::KILL,
                );
                let _ = self.child.kill();
            }

            let kill_deadline = Instant::now() + kill_wait;
            while self.try_wait()?.is_none() {
                let now = Instant::now();
                if now >= kill_deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("run child {} did not exit after kill", self.child.id()),
                    ));
                }
                std::thread::park_timeout(PROCESS_POLL_INTERVAL.min(kill_deadline - now));
            }
            self.assert_zero_residual(kill_wait)?;
            Ok(self.status.expect("run child exit status was observed"))
        }

        pub(super) fn assert_zero_residual(&mut self, timeout: Duration) -> io::Result<()> {
            let deadline = Instant::now() + timeout;
            loop {
                self.observe_descendants()?;
                let group_alive = self.group_alive()?;
                let descendants_alive = self.any_observed_alive();
                if !group_alive && !descendants_alive {
                    return Ok(());
                }
                let now = Instant::now();
                if now >= deadline {
                    let residual = self
                        .observed
                        .values()
                        .filter(|process| process_alive(**process))
                        .map(|process| process.pid.to_string())
                        .collect::<Vec<_>>()
                        .join(",");
                    return Err(io::Error::other(format!(
                        "run child {} left residual process group or descendants [{residual}]",
                        self.child.id()
                    )));
                }
                std::thread::park_timeout(PROCESS_POLL_INTERVAL.min(deadline - now));
            }
        }

        pub(super) fn kill_on_drop(&mut self) {
            if self.try_wait().ok().flatten().is_none() || self.any_observed_alive() {
                let _ = self.terminate_tree(Duration::ZERO, Duration::from_secs(1));
            }
        }

        fn freeze_descendant_tree(&mut self) -> io::Result<()> {
            loop {
                let before = self.observed.len();
                self.observe_descendants()?;
                self.signal_observed(rustix::process::Signal::STOP);
                let _ = rustix::process::kill_process(
                    self.process_group,
                    rustix::process::Signal::STOP,
                );
                self.observe_descendants()?;
                if self.observed.len() == before {
                    return Ok(());
                }
            }
        }

        fn signal_observed(&self, signal: rustix::process::Signal) {
            for process in self.observed.values().copied() {
                if process_alive(process)
                    && let Some(pid) = rustix::process::Pid::from_raw(process.pid)
                {
                    let _ = rustix::process::kill_process(pid, signal);
                }
            }
        }

        fn any_observed_alive(&self) -> bool {
            self.observed.values().copied().any(process_alive)
        }

        fn group_alive(&self) -> io::Result<bool> {
            match rustix::process::test_kill_process_group(self.process_group) {
                Ok(()) => Ok(true),
                Err(Errno::SRCH) => Ok(false),
                Err(error) => Err(io::Error::from_raw_os_error(error.raw_os_error())),
            }
        }
    }

    #[cfg(target_os = "linux")]
    #[derive(Clone, Copy)]
    struct LinuxProcess {
        pid: i32,
        parent: i32,
        start_time: u64,
    }

    #[cfg(target_os = "linux")]
    fn linux_processes() -> io::Result<HashMap<i32, LinuxProcess>> {
        let mut processes = HashMap::new();
        for entry in fs::read_dir("/proc")? {
            let entry = entry?;
            let Some(pid) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse::<i32>().ok())
            else {
                continue;
            };
            let Ok(stat) = fs::read_to_string(entry.path().join("stat")) else {
                continue;
            };
            let Some(process) = parse_linux_stat(pid, &stat) else {
                continue;
            };
            processes.insert(pid, process);
        }
        Ok(processes)
    }

    #[cfg(target_os = "linux")]
    fn parse_linux_stat(pid: i32, stat: &str) -> Option<LinuxProcess> {
        let tail = stat.rsplit_once(") ")?.1;
        let fields = tail.split_ascii_whitespace().collect::<Vec<_>>();
        Some(LinuxProcess {
            pid,
            parent: fields.get(1)?.parse().ok()?,
            start_time: fields.get(19)?.parse().ok()?,
        })
    }

    fn process_alive(process: ObservedProcess) -> bool {
        #[cfg(target_os = "linux")]
        {
            let Ok(stat) = fs::read_to_string(format!("/proc/{}/stat", process.pid)) else {
                return false;
            };
            parse_linux_stat(process.pid, &stat)
                .is_some_and(|current| current.start_time == process.start_time)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let Some(pid) = rustix::process::Pid::from_raw(process.pid) else {
                return false;
            };
            match rustix::process::test_kill_process(pid) {
                Ok(()) => true,
                Err(Errno::SRCH) => false,
                Err(_) => true,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader};

    #[test]
    fn natural_success_and_failure_leave_zero_residual_processes() {
        for exit_code in [0, 7] {
            let mut command = fixture_command(&format!("exit {exit_code}"));
            command
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null());
            let mut child = ProcessTreeChild::spawn(&mut command).unwrap();
            let status = child
                .wait_for_exit(Duration::from_secs(2))
                .unwrap()
                .expect("fixture child did not exit");
            assert_eq!(status.success(), exit_code == 0);
            child.assert_zero_residual(Duration::from_secs(2)).unwrap();
        }
    }

    #[test]
    fn grace_then_kill_terminates_the_full_descendant_tree() {
        let mut command = descendant_fixture_command();
        command
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null());
        let mut child = ProcessTreeChild::spawn(&mut command).unwrap();
        let stdout = child.take_stdout().unwrap();
        let mut reader = BufReader::new(stdout);
        let mut ready = String::new();
        reader.read_line(&mut ready).unwrap();
        assert_eq!(ready.trim(), "ready");

        let status = child
            .terminate_tree(Duration::from_millis(50), Duration::from_secs(2))
            .unwrap();
        assert!(!status.success());
        child.assert_zero_residual(Duration::from_secs(2)).unwrap();
    }

    #[cfg(unix)]
    fn fixture_command(script: &str) -> Command {
        let mut command = Command::new("/bin/sh");
        command.arg("-c").arg(script);
        command
    }

    #[cfg(windows)]
    fn fixture_command(script: &str) -> Command {
        let mut command = Command::new(
            std::env::var_os("COMSPEC").unwrap_or_else(|| r"C:\Windows\System32\cmd.exe".into()),
        );
        command.arg("/D").arg("/S").arg("/C").arg(script);
        command
    }

    #[cfg(unix)]
    fn descendant_fixture_command() -> Command {
        fixture_command(
            "echo ready; trap '' TERM; /bin/sh -c \"trap '' TERM; sleep 60 & wait\" & wait",
        )
    }

    #[cfg(windows)]
    fn descendant_fixture_command() -> Command {
        fixture_command("echo ready & ping -t 127.0.0.1 >NUL")
    }
}

#[cfg(windows)]
mod platform {
    #![allow(unsafe_code)]

    use super::PROCESS_POLL_INTERVAL;
    use std::{
        io, mem,
        os::windows::{
            io::{AsRawHandle, FromRawHandle, OwnedHandle},
            process::CommandExt,
        },
        process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, ExitStatus},
        ptr,
        time::{Duration, Instant},
    };
    use windows_sys::Win32::{
        Foundation::{ERROR_NO_MORE_FILES, INVALID_HANDLE_VALUE},
        System::{
            Diagnostics::ToolHelp::{
                CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First,
                Thread32Next,
            },
            JobObjects::{
                AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
                JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
                JobObjectBasicAccountingInformation, JobObjectExtendedLimitInformation,
                QueryInformationJobObject, SetInformationJobObject, TerminateJobObject,
            },
            Threading::{CREATE_SUSPENDED, OpenThread, ResumeThread, THREAD_SUSPEND_RESUME},
        },
    };

    pub(super) struct PlatformChild {
        child: Child,
        job: Option<OwnedHandle>,
        status: Option<ExitStatus>,
    }

    impl PlatformChild {
        pub(super) fn spawn(command: &mut Command) -> io::Result<Self> {
            command.creation_flags(CREATE_SUSPENDED);
            let job = create_kill_on_close_job()?;
            let mut child = command.spawn()?;
            if let Err(error) = assign_to_job(&child, &job) {
                let _ = child.kill();
                let _ = child.wait();
                return Err(error);
            }
            if let Err(error) = resume_process(child.id()) {
                drop(job);
                let _ = child.wait();
                return Err(error);
            }
            Ok(Self {
                child,
                job: Some(job),
                status: None,
            })
        }

        pub(super) fn id(&self) -> u32 {
            self.child.id()
        }

        pub(super) fn take_stdin(&mut self) -> Option<ChildStdin> {
            self.child.stdin.take()
        }

        pub(super) fn take_stdout(&mut self) -> Option<ChildStdout> {
            self.child.stdout.take()
        }

        pub(super) fn take_stderr(&mut self) -> Option<ChildStderr> {
            self.child.stderr.take()
        }

        pub(super) fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
            if self.status.is_none() {
                self.status = self.child.try_wait()?;
            }
            Ok(self.status)
        }

        pub(super) fn observe_descendants(&mut self) -> io::Result<()> {
            Ok(())
        }

        pub(super) fn terminate_tree(
            &mut self,
            _grace: Duration,
            kill_wait: Duration,
        ) -> io::Result<ExitStatus> {
            let job = self
                .job
                .as_ref()
                .ok_or_else(|| io::Error::other("run child job was already closed"))?;
            // SAFETY: the job handle is live and owned for the complete run-child lifetime.
            if unsafe { TerminateJobObject(job.as_raw_handle(), 1) } == 0 {
                return Err(io::Error::last_os_error());
            }
            let deadline = Instant::now() + kill_wait;
            while self.try_wait()?.is_none() {
                let now = Instant::now();
                if now >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("run child {} did not exit after job kill", self.child.id()),
                    ));
                }
                std::thread::park_timeout(PROCESS_POLL_INTERVAL.min(deadline - now));
            }
            self.assert_zero_residual(kill_wait)?;
            Ok(self.status.expect("run child exit status was observed"))
        }

        pub(super) fn assert_zero_residual(&mut self, timeout: Duration) -> io::Result<()> {
            let deadline = Instant::now() + timeout;
            loop {
                let active = self.active_processes()?;
                if active == 0 {
                    return Ok(());
                }
                let now = Instant::now();
                if now >= deadline {
                    return Err(io::Error::other(format!(
                        "run child {} job retained {active} processes",
                        self.child.id()
                    )));
                }
                std::thread::park_timeout(PROCESS_POLL_INTERVAL.min(deadline - now));
            }
        }

        pub(super) fn kill_on_drop(&mut self) {
            if self.active_processes().unwrap_or(1) != 0 {
                if let Some(job) = self.job.as_ref() {
                    // SAFETY: the owned job remains live until the handle is dropped below.
                    unsafe {
                        TerminateJobObject(job.as_raw_handle(), 1);
                    }
                }
                let _ = self.child.wait();
            }
            drop(self.job.take());
        }

        fn active_processes(&self) -> io::Result<u32> {
            let job = self
                .job
                .as_ref()
                .ok_or_else(|| io::Error::other("run child job was already closed"))?;
            let mut accounting = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
            // SAFETY: the job is live and accounting is valid writable storage of the given size.
            if unsafe {
                QueryInformationJobObject(
                    job.as_raw_handle(),
                    JobObjectBasicAccountingInformation,
                    (&raw mut accounting).cast(),
                    mem::size_of_val(&accounting)
                        .try_into()
                        .expect("job accounting size fits u32"),
                    ptr::null_mut(),
                )
            } == 0
            {
                return Err(io::Error::last_os_error());
            }
            Ok(accounting.ActiveProcesses)
        }
    }

    fn create_kill_on_close_job() -> io::Result<OwnedHandle> {
        // SAFETY: null attributes/name request an unnamed, non-inheritable job.
        let raw = unsafe { CreateJobObjectW(ptr::null(), ptr::null()) };
        if raw.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: CreateJobObjectW returned a new owned handle.
        let job = unsafe { OwnedHandle::from_raw_handle(raw) };
        let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        // SAFETY: job is live and limits points to the documented fixed-size structure.
        if unsafe {
            SetInformationJobObject(
                job.as_raw_handle(),
                JobObjectExtendedLimitInformation,
                (&raw const limits).cast(),
                mem::size_of_val(&limits)
                    .try_into()
                    .expect("job limits size fits u32"),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(job)
    }

    fn assign_to_job(child: &Child, job: &OwnedHandle) -> io::Result<()> {
        // SAFETY: both handles stay live through the assignment.
        if unsafe { AssignProcessToJobObject(job.as_raw_handle(), child.as_raw_handle()) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn resume_process(process_id: u32) -> io::Result<()> {
        // SAFETY: the snapshot handle is checked before ownership is assumed.
        let raw = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
        if raw == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: CreateToolhelp32Snapshot returned a new owned handle.
        let snapshot = unsafe { OwnedHandle::from_raw_handle(raw) };
        let mut entry = THREADENTRY32 {
            dwSize: mem::size_of::<THREADENTRY32>()
                .try_into()
                .expect("THREADENTRY32 size fits u32"),
            ..Default::default()
        };
        // SAFETY: snapshot is live and entry points to initialized writable storage.
        if unsafe { Thread32First(snapshot.as_raw_handle(), &mut entry) } == 0 {
            return Err(io::Error::last_os_error());
        }

        loop {
            if entry.th32OwnerProcessID == process_id {
                // SAFETY: the returned thread handle is checked before ownership is assumed.
                let raw = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, entry.th32ThreadID) };
                if raw.is_null() {
                    return Err(io::Error::last_os_error());
                }
                // SAFETY: OpenThread returned a new owned handle.
                let thread = unsafe { OwnedHandle::from_raw_handle(raw) };
                // SAFETY: thread is live and was opened with THREAD_SUSPEND_RESUME.
                if unsafe { ResumeThread(thread.as_raw_handle()) } == u32::MAX {
                    return Err(io::Error::last_os_error());
                }
                return Ok(());
            }
            // SAFETY: snapshot and entry remain live for enumeration.
            if unsafe { Thread32Next(snapshot.as_raw_handle(), &mut entry) } == 0 {
                let error = io::Error::last_os_error();
                return if error.raw_os_error() == Some(ERROR_NO_MORE_FILES as i32) {
                    Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("run child {process_id} had no suspended primary thread"),
                    ))
                } else {
                    Err(error)
                };
            }
        }
    }
}
