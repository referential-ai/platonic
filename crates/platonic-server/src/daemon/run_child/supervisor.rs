use super::messages::{
    ApprovalReply, ChildMessage, ChildRunResult, ParentMessage, RecordOperation,
};
use crate::tool_catalog::{COMPUTER_OBSERVE, COMPUTER_WINDOWS};
use crate::{
    AppError, AppResult, ApprovalMode, RunEvent, RunOutcome,
    app::{ExternalApprovalOutcome, PreparedRun},
    daemon::child_process::ProcessTreeChild,
    ledger::{EventRecorder, RUN_CANCELED_REASON},
    tools::ThreadSpawnToolHandler,
};
use platonic_core::{RecordedEvent, RunId};
use std::{
    ffi::OsStr,
    fs,
    io::{self, BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

const CHILD_MODE_ARG: &str = "--run-child";
const RUN_CHILD_DEADLINE: Duration = Duration::from_secs(30 * 60);
const CANCEL_TOKEN_GRACE: Duration = Duration::from_millis(500);
const TERMINATION_GRACE: Duration = Duration::from_millis(250);
const KILL_WAIT: Duration = Duration::from_secs(2);
pub(super) const OUTPUT_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);
const SUPERVISOR_POLL_INTERVAL: Duration = Duration::from_millis(25);
const STDERR_LIMIT: usize = 32 * 1024;

#[derive(Clone, Copy, Debug)]
pub(in crate::daemon) struct ChildLifecycleLimits {
    deadline: Duration,
    cancel_grace: Duration,
    termination_grace: Duration,
    kill_wait: Duration,
    output_drain: Duration,
}

impl Default for ChildLifecycleLimits {
    fn default() -> Self {
        Self {
            deadline: RUN_CHILD_DEADLINE,
            cancel_grace: CANCEL_TOKEN_GRACE,
            termination_grace: TERMINATION_GRACE,
            kill_wait: KILL_WAIT,
            output_drain: OUTPUT_DRAIN_TIMEOUT,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StopCause {
    Canceled,
    TimedOut,
    ChildFailed,
}

enum ChildReaderEvent {
    Message(ChildMessage),
    Invalid(String),
    Closed,
}

struct ParentRunRecorder {
    recorder: EventRecorder,
    event_sender: mpsc::Sender<RunEvent>,
    terminal: Option<RecordOperation>,
}

impl ParentRunRecorder {
    fn apply(&mut self, operation: RecordOperation) -> AppResult<Option<RecordedEvent>> {
        match operation {
            RecordOperation::Event { event } => {
                if self.terminal.is_some() {
                    return Err(AppError::SupervisedRun(
                        "run child emitted a nonterminal event after terminal intent".into(),
                    ));
                }
                let record = self.recorder.record(event)?;
                self.event_sender
                    .send(RunEvent::Ledger(record.clone()))
                    .map_err(|_| AppError::SupervisedRun("daemon event collector closed".into()))?;
                Ok(Some(record))
            }
            terminal => {
                if self.terminal.replace(terminal).is_some() {
                    return Err(AppError::SupervisedRun(
                        "run child emitted more than one terminal intent".into(),
                    ));
                }
                Ok(None)
            }
        }
    }

    fn complete(
        self,
        run_id: &RunId,
        outcome: AppResult<RunOutcome>,
        preserve_child_terminal: bool,
    ) -> SupervisedRunCompletion {
        let terminal = if preserve_child_terminal {
            match validate_terminal_intent(self.terminal, run_id, &outcome) {
                Ok(terminal) => terminal,
                Err(error) => {
                    return SupervisedRunCompletion::new(
                        self.recorder,
                        self.event_sender,
                        run_id,
                        Err(error),
                    );
                }
            }
        } else {
            terminal_for_outcome(run_id, &outcome)
        };
        drop(self.event_sender);
        SupervisedRunCompletion {
            outcome,
            terminal: TerminalPublication {
                recorder: self.recorder,
                operation: terminal,
            },
        }
    }
}

pub(in crate::daemon) struct SupervisedRunCompletion {
    outcome: AppResult<RunOutcome>,
    terminal: TerminalPublication,
}

impl SupervisedRunCompletion {
    fn new(
        recorder: EventRecorder,
        event_sender: mpsc::Sender<RunEvent>,
        run_id: &RunId,
        outcome: AppResult<RunOutcome>,
    ) -> Self {
        drop(event_sender);
        Self {
            terminal: TerminalPublication {
                operation: terminal_for_outcome(run_id, &outcome),
                recorder,
            },
            outcome,
        }
    }

    pub(in crate::daemon) fn override_failure(self, error: AppError) -> Self {
        let run_id = self.terminal.operation.run_id().clone();
        let reason = terminal_error(&error);
        Self {
            outcome: Err(error),
            terminal: TerminalPublication {
                recorder: self.terminal.recorder,
                operation: terminal_failure(&run_id, reason, false),
            },
        }
    }

    pub(in crate::daemon) fn publish(self) -> (AppResult<RunOutcome>, AppResult<RecordedEvent>) {
        (self.outcome, self.terminal.publish())
    }
}

struct TerminalPublication {
    recorder: EventRecorder,
    operation: RecordOperation,
}

impl TerminalPublication {
    fn publish(mut self) -> AppResult<RecordedEvent> {
        match self.operation {
            RecordOperation::Finish {
                run_id,
                final_answer,
            } => self.recorder.finish_run(&run_id, &final_answer),
            RecordOperation::Fail {
                run_id,
                error,
                canceled,
            } => self.recorder.fail_run(&run_id, &error, canceled),
            RecordOperation::Event { .. } => unreachable!("terminal publication stores intent"),
        }
    }
}

fn validate_terminal_intent(
    terminal: Option<RecordOperation>,
    run_id: &RunId,
    outcome: &AppResult<RunOutcome>,
) -> AppResult<RecordOperation> {
    let terminal = terminal.ok_or_else(|| {
        AppError::SupervisedRun("run child returned without terminal intent".into())
    })?;
    let matches = match (&terminal, outcome) {
        (
            RecordOperation::Finish {
                run_id: terminal_run_id,
                final_answer,
            },
            Ok(outcome),
        ) => {
            terminal_run_id == run_id
                && &outcome.run_id == run_id
                && final_answer == &outcome.final_answer
        }
        (
            RecordOperation::Fail {
                run_id: terminal_run_id,
                error,
                canceled: true,
            },
            Err(AppError::RunCanceled),
        ) => terminal_run_id == run_id && error == RUN_CANCELED_REASON,
        (
            RecordOperation::Fail {
                run_id: terminal_run_id,
                error,
                canceled: false,
            },
            Err(AppError::SupervisedRun(result_error)),
        ) => {
            terminal_run_id == run_id
                && (result_error == error
                    || result_error == &format!("run did not finish: {error}"))
        }
        _ => false,
    };
    if matches {
        Ok(terminal)
    } else {
        Err(AppError::SupervisedRun(
            "run child result did not match terminal intent".into(),
        ))
    }
}

fn terminal_for_outcome(run_id: &RunId, outcome: &AppResult<RunOutcome>) -> RecordOperation {
    match outcome {
        Ok(outcome) => RecordOperation::Finish {
            run_id: run_id.clone(),
            final_answer: outcome.final_answer.clone(),
        },
        Err(AppError::RunCanceled) => terminal_failure(run_id, RUN_CANCELED_REASON.into(), true),
        Err(AppError::RunChildTimedOut(_)) => {
            terminal_failure(run_id, "run child deadline exceeded".into(), false)
        }
        Err(error) => terminal_failure(run_id, terminal_error(error), false),
    }
}

fn terminal_error(error: &AppError) -> String {
    match error {
        AppError::RunFailed(reason) | AppError::SupervisedRun(reason) => reason.clone(),
        _ => error.to_string(),
    }
}

fn terminal_failure(run_id: &RunId, error: String, canceled: bool) -> RecordOperation {
    RecordOperation::Fail {
        run_id: run_id.clone(),
        error,
        canceled,
    }
}

struct SupervisedChild {
    child: ProcessTreeChild,
    writer: Option<BufWriter<std::process::ChildStdin>>,
    reader_receiver: Option<mpsc::Receiver<ChildReaderEvent>>,
    stderr_receiver: Option<mpsc::Receiver<io::Result<String>>>,
    stdout_reader: Option<thread::JoinHandle<()>>,
    stderr_reader: Option<thread::JoinHandle<()>>,
    reader_closed: bool,
    finalized: bool,
    limits: ChildLifecycleLimits,
}

impl SupervisedChild {
    fn new(child: ProcessTreeChild, limits: ChildLifecycleLimits) -> Self {
        Self {
            child,
            writer: None,
            reader_receiver: None,
            stderr_receiver: None,
            stdout_reader: None,
            stderr_reader: None,
            reader_closed: false,
            finalized: false,
            limits,
        }
    }

    fn connect(&mut self) -> AppResult<()> {
        let stdin = self
            .child
            .take_stdin()
            .ok_or_else(|| AppError::SupervisedRun("run child stdin was not piped".into()))?;
        self.writer = Some(BufWriter::new(stdin));
        let stdout = self
            .child
            .take_stdout()
            .ok_or_else(|| AppError::SupervisedRun("run child stdout was not piped".into()))?;
        let stderr = self
            .child
            .take_stderr()
            .ok_or_else(|| AppError::SupervisedRun("run child stderr was not piped".into()))?;

        let (reader_sender, reader_receiver) = mpsc::channel();
        self.reader_receiver = Some(reader_receiver);
        self.stdout_reader = Some(thread::spawn(move || {
            read_child_messages(stdout, reader_sender);
        }));
        let (stderr_sender, stderr_receiver) = mpsc::channel();
        self.stderr_receiver = Some(stderr_receiver);
        self.stderr_reader = Some(thread::spawn(move || {
            let result = read_bounded(stderr, STDERR_LIMIT);
            let _ = stderr_sender.send(result);
        }));
        Ok(())
    }

    fn id(&self) -> u32 {
        self.child.id()
    }

    fn write(&mut self, message: &ParentMessage) -> AppResult<()> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| AppError::SupervisedRun("run child stdin was already closed".into()))?;
        write_parent_message(writer, message)
    }

    fn receive(&self, timeout: Duration) -> Result<ChildReaderEvent, mpsc::RecvTimeoutError> {
        self.reader_receiver
            .as_ref()
            .expect("run child reader is connected before supervision")
            .recv_timeout(timeout)
    }

    fn finalize_success(&mut self) -> AppResult<ExitStatus> {
        self.writer.take();
        let mut errors = Vec::new();
        let mut status = match self.child.wait_for_exit(self.limits.output_drain) {
            Ok(status) => status,
            Err(error) => {
                errors.push(format!("wait for run child exit failed: {error}"));
                None
            }
        };

        let zero_residual = status
            .is_some()
            .then(|| self.child.assert_zero_residual(Duration::ZERO))
            .transpose()
            .is_ok();
        if status.is_none() || !zero_residual {
            match self
                .child
                .terminate_tree(self.limits.termination_grace, self.limits.kill_wait)
            {
                Ok(terminated_status) => status = Some(terminated_status),
                Err(error) => errors.push(format!("terminate run child tree failed: {error}")),
            }
        }
        if let Err(error) = self.child.assert_zero_residual(self.limits.kill_wait) {
            errors.push(format!("run child zero-residual assertion failed: {error}"));
        }
        if let Err(error) = self.drain_readers(false) {
            errors.push(error.to_string());
        }
        self.finalized = true;

        if errors.is_empty() {
            status.ok_or_else(|| {
                AppError::SupervisedRun("run child exit status was not observed".into())
            })
        } else {
            Err(cleanup_errors(errors))
        }
    }

    fn cleanup_failure(&mut self, primary: AppError) -> AppError {
        if self.finalized {
            return primary;
        }

        self.writer.take();
        let mut errors = Vec::new();
        if let Err(error) = self
            .child
            .terminate_tree(self.limits.termination_grace, self.limits.kill_wait)
        {
            errors.push(format!("terminate run child tree failed: {error}"));
        }
        if let Err(error) = self.child.assert_zero_residual(self.limits.kill_wait) {
            errors.push(format!("run child zero-residual assertion failed: {error}"));
        }
        if let Err(error) = self.drain_readers(true) {
            errors.push(error.to_string());
        }
        self.finalized = true;

        if errors.is_empty() {
            primary
        } else {
            AppError::SupervisedRun(format!(
                "{primary}; supervised child cleanup also failed: {}",
                errors.join("; ")
            ))
        }
    }

    fn drain_readers(&mut self, discard_pending_messages: bool) -> AppResult<()> {
        let Some(reader_receiver) = self.reader_receiver.take() else {
            return Ok(());
        };
        let stderr_receiver = self
            .stderr_receiver
            .take()
            .expect("stderr receiver accompanies event receiver");
        let stdout_reader = self
            .stdout_reader
            .take()
            .expect("stdout reader accompanies event receiver");
        let stderr_reader = self
            .stderr_reader
            .take()
            .expect("stderr reader accompanies event receiver");
        drain_after_exit(
            &reader_receiver,
            &mut self.reader_closed,
            &stderr_receiver,
            stdout_reader,
            stderr_reader,
            self.limits.output_drain,
            discard_pending_messages,
        )
    }
}

fn cleanup_errors(errors: Vec<String>) -> AppError {
    AppError::SupervisedRun(format!(
        "supervised child cleanup failed: {}",
        errors.join("; ")
    ))
}

struct ChildLaunch {
    limits: ChildLifecycleLimits,
    executable: PathBuf,
    ready_child: Option<mpsc::Sender<u32>>,
    confinement: crate::confinement::ChildConfinement,
    #[cfg(test)]
    terminal_stage_barriers: Option<TerminalStageBarriers>,
}

#[cfg(test)]
pub(in crate::daemon) struct TerminalStageBarriers {
    pub(in crate::daemon) reached: Arc<std::sync::Barrier>,
    pub(in crate::daemon) release: Arc<std::sync::Barrier>,
}

#[cfg(all(test, target_os = "linux"))]
pub(in crate::daemon) struct SupervisedTestLaunch {
    pub(in crate::daemon) executable: PathBuf,
    pub(in crate::daemon) ready_child: mpsc::Sender<u32>,
    pub(in crate::daemon) terminal_stage_barriers: TerminalStageBarriers,
}

pub(in crate::daemon) fn run_supervised(
    prepared: PreparedRun,
    recorder: EventRecorder,
    approval_mode: ApprovalMode,
    event_sender: mpsc::Sender<RunEvent>,
    cancel: Arc<AtomicBool>,
    thread_spawn: Option<ThreadSpawnToolHandler>,
    confinement: crate::confinement::ChildConfinement,
) -> SupervisedRunCompletion {
    let executable = match resolve_run_child_executable() {
        Ok(executable) => executable,
        Err(error) => {
            let run_id = prepared.run_id().clone();
            return SupervisedRunCompletion::new(recorder, event_sender, &run_id, Err(error));
        }
    };
    run_supervised_with_limits(
        prepared,
        recorder,
        approval_mode,
        event_sender,
        cancel,
        thread_spawn,
        ChildLaunch {
            limits: ChildLifecycleLimits::default(),
            executable,
            ready_child: None,
            confinement,
            #[cfg(test)]
            terminal_stage_barriers: None,
        },
    )
}

#[cfg(all(test, target_os = "linux"))]
pub(in crate::daemon) fn run_supervised_for_test(
    prepared: PreparedRun,
    recorder: EventRecorder,
    approval_mode: ApprovalMode,
    event_sender: mpsc::Sender<RunEvent>,
    cancel: Arc<AtomicBool>,
    thread_spawn: Option<ThreadSpawnToolHandler>,
    launch: SupervisedTestLaunch,
) -> SupervisedRunCompletion {
    run_supervised_with_limits(
        prepared,
        recorder,
        approval_mode,
        event_sender,
        cancel,
        thread_spawn,
        ChildLaunch {
            limits: ChildLifecycleLimits::default(),
            executable: launch.executable,
            ready_child: Some(launch.ready_child),
            confinement: crate::confinement::ChildConfinement::None,
            terminal_stage_barriers: Some(launch.terminal_stage_barriers),
        },
    )
}

fn resolve_run_child_executable() -> AppResult<PathBuf> {
    resolve_run_child_executable_from(&std::env::current_exe()?, std::env::consts::EXE_SUFFIX)
}

fn resolve_run_child_executable_from(
    current: &Path,
    executable_suffix: &str,
) -> AppResult<PathBuf> {
    let binary_name = format!("platonic{executable_suffix}");
    let candidate = if current.file_name() == Some(OsStr::new(&binary_name)) {
        current.to_path_buf()
    } else {
        let parent = current.parent().ok_or_else(|| {
            AppError::SupervisedRun(format!(
                "cannot resolve run child beside host image {}",
                current.display()
            ))
        })?;
        if parent.file_name() == Some(OsStr::new("deps")) {
            parent
                .parent()
                .ok_or_else(|| {
                    AppError::SupervisedRun(format!(
                        "cannot resolve run child above Cargo deps host {}",
                        current.display()
                    ))
                })?
                .join(binary_name)
        } else {
            parent.join(binary_name)
        }
    };
    let metadata = fs::symlink_metadata(&candidate).map_err(|error| {
        AppError::SupervisedRun(format!(
            "run child executable {} is unavailable: {error}",
            candidate.display()
        ))
    })?;
    if !metadata.file_type().is_file() {
        return Err(AppError::SupervisedRun(format!(
            "run child executable {} is not a regular file",
            candidate.display()
        )));
    }
    Ok(candidate)
}

fn run_supervised_with_limits(
    prepared: PreparedRun,
    recorder: EventRecorder,
    approval_mode: ApprovalMode,
    event_sender: mpsc::Sender<RunEvent>,
    cancel: Arc<AtomicBool>,
    thread_spawn: Option<ThreadSpawnToolHandler>,
    launch: ChildLaunch,
) -> SupervisedRunCompletion {
    let ChildLaunch {
        limits,
        executable,
        ready_child,
        confinement,
        #[cfg(test)]
        terminal_stage_barriers,
    } = launch;
    let run_id = prepared.run_id().clone();
    let mut recorder = ParentRunRecorder {
        recorder,
        event_sender,
        terminal: None,
    };
    let computer_enabled =
        prepared.has_tool(COMPUTER_WINDOWS) || prepared.has_tool(COMPUTER_OBSERVE);
    let child_env = match crate::tools::supervised_run_child_env(
        prepared.provider_api_key_env(),
        computer_enabled,
    ) {
        Ok(child_env) => child_env,
        Err(error) => return recorder.complete(&run_id, Err(error), false),
    };
    let mut command = Command::new(executable);
    command
        .arg(CHILD_MODE_ARG)
        .env_clear()
        .envs(child_env)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Err(error) = crate::confinement::configure_child(&mut command, &confinement) {
        return recorder.complete(&run_id, Err(error), false);
    }
    let child = match ProcessTreeChild::spawn(&mut command) {
        Ok(child) => child,
        Err(error) => {
            return recorder.complete(&run_id, Err(error.into()), false);
        }
    };
    let mut child = SupervisedChild::new(child, limits);
    let child_pid = child.id();
    let mut ready_child = ready_child;
    #[cfg(test)]
    let mut terminal_stage_barriers = terminal_stage_barriers;
    let result = (|| -> AppResult<(ChildRunResult, Option<StopCause>)> {
        child.connect()?;
        child.write(&ParentMessage::Start {
            prepared: Box::new(prepared),
        })?;
        let deadline = Instant::now() + limits.deadline;
        let mut ready = false;
        let mut stop_cause = None;
        let mut cancel_grace_deadline = None;
        let mut result = None;

        loop {
            child.child.observe_descendants()?;
            let now = Instant::now();
            if stop_cause.is_none() && cancel.load(Ordering::SeqCst) {
                stop_cause = Some(StopCause::Canceled);
                cancel_grace_deadline = Some(now + limits.cancel_grace);
                child.write(&ParentMessage::Cancel)?;
            } else if stop_cause.is_none() && recorder.terminal.is_none() && now >= deadline {
                stop_cause = Some(StopCause::TimedOut);
                cancel_grace_deadline = Some(now + limits.cancel_grace);
                child.write(&ParentMessage::Cancel)?;
            }

            if cancel_grace_deadline.is_some_and(|grace_deadline| now >= grace_deadline) {
                let cause = stop_cause.expect("stop cause accompanies cancel grace");
                let reason = stop_reason(cause, child_pid, None);
                return stop_error(cause, limits.deadline, reason);
            }

            if let Some(status) = child.child.try_wait()? {
                if result.is_some() {
                    break;
                }
                let reason = stop_reason(StopCause::ChildFailed, child_pid, Some(status));
                return Err(AppError::SupervisedRun(reason));
            }

            match child.receive(SUPERVISOR_POLL_INTERVAL) {
                Ok(ChildReaderEvent::Message(message)) => match message {
                    ChildMessage::Ready { request_id, pid } => {
                        if ready || pid != child_pid {
                            let reason = format!(
                                "run child ready identity mismatch: expected {child_pid}, got {pid}"
                            );
                            return Err(AppError::SupervisedRun(reason));
                        }
                        ready = true;
                        child.write(&ParentMessage::Ack {
                            request_id,
                            record: None,
                        })?;
                        if let Some(ready_child) = ready_child.take() {
                            let _ = ready_child.send(child_pid);
                        }
                    }
                    ChildMessage::Record {
                        request_id,
                        operation,
                    } => {
                        #[cfg(test)]
                        let terminal = !matches!(&operation, RecordOperation::Event { .. });
                        let record = recorder.apply(operation)?;
                        #[cfg(test)]
                        if terminal && let Some(barriers) = terminal_stage_barriers.take() {
                            barriers.reached.wait();
                            barriers.release.wait();
                        }
                        child.write(&ParentMessage::Ack { request_id, record })?;
                    }
                    ChildMessage::AssistantDelta { request_id, delta } => {
                        recorder
                            .event_sender
                            .send(RunEvent::AssistantDelta(delta))
                            .map_err(|_| {
                                AppError::SupervisedRun("daemon event collector closed".into())
                            })?;
                        child.write(&ParentMessage::Ack {
                            request_id,
                            record: None,
                        })?;
                    }
                    ChildMessage::Approval {
                        request_id,
                        request,
                    } => {
                        let outcome = approval_mode.decide_external(request)?;
                        let outcome = match outcome {
                            ExternalApprovalOutcome::Granted { actor } => {
                                ApprovalReply::Granted { actor }
                            }
                            ExternalApprovalOutcome::Denied { actor, reason } => {
                                ApprovalReply::Denied { actor, reason }
                            }
                        };
                        child.write(&ParentMessage::Approval {
                            request_id,
                            outcome,
                        })?;
                    }
                    ChildMessage::ThreadSpawn {
                        request_id,
                        input,
                        approving_actor,
                    } => match thread_spawn.as_ref() {
                        Some(handler) => match handler.execute(input, approving_actor) {
                            Ok(output) => {
                                child.write(&ParentMessage::ThreadSpawn { request_id, output })?
                            }
                            Err(error) => child.write(&ParentMessage::Reject {
                                request_id,
                                error: error.to_string(),
                            })?,
                        },
                        None => child.write(&ParentMessage::Reject {
                            request_id,
                            error: "thread.spawn requires a coordinator thread".into(),
                        })?,
                    },
                    ChildMessage::Result {
                        request_id,
                        result: child_result,
                    } => {
                        child.write(&ParentMessage::Ack {
                            request_id,
                            record: None,
                        })?;
                        result = Some(child_result);
                    }
                },
                Ok(ChildReaderEvent::Invalid(error)) => {
                    let reason = format!("run child transport failed: {error}");
                    return Err(AppError::SupervisedRun(reason));
                }
                Ok(ChildReaderEvent::Closed) => child.reader_closed = true,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => child.reader_closed = true,
            }

            if result.is_some() {
                break;
            }
            if child.reader_closed {
                let reason =
                    format!("run child {child_pid} closed its event stream without a result");
                return Err(AppError::SupervisedRun(reason));
            }
        }

        let status = child.finalize_success()?;
        if !status.success() {
            let reason = stop_reason(StopCause::ChildFailed, child_pid, Some(status));
            return Err(AppError::SupervisedRun(reason));
        }

        let result = result.expect("result was checked before child exit");
        Ok((result, stop_cause))
    })();

    match result {
        Ok((result, stop_cause)) => {
            let outcome = match stop_cause {
                Some(StopCause::TimedOut) => Err(AppError::RunChildTimedOut(
                    limits.deadline.as_millis().try_into().unwrap_or(u64::MAX),
                )),
                Some(StopCause::Canceled) => Err(AppError::RunCanceled),
                Some(StopCause::ChildFailed) => {
                    unreachable!("child failure returns before result mapping")
                }
                None => match result {
                    ChildRunResult::Finished { outcome } => Ok(outcome),
                    ChildRunResult::Canceled => Err(AppError::RunCanceled),
                    ChildRunResult::Failed { error } => Err(AppError::SupervisedRun(error)),
                },
            };
            recorder.complete(&run_id, outcome, stop_cause.is_none())
        }
        Err(error) => {
            let error = child.cleanup_failure(error);
            recorder.complete(&run_id, Err(error), false)
        }
    }
}

fn stop_reason(cause: StopCause, pid: u32, status: Option<ExitStatus>) -> String {
    match cause {
        StopCause::Canceled => RUN_CANCELED_REASON.into(),
        StopCause::TimedOut => "run child deadline exceeded".into(),
        StopCause::ChildFailed => match status {
            Some(status) => format!("run child {pid} exited unexpectedly with {status}"),
            None => format!("run child {pid} exited unexpectedly"),
        },
    }
}

fn stop_error<T>(cause: StopCause, deadline: Duration, reason: String) -> AppResult<T> {
    match cause {
        StopCause::Canceled => Err(AppError::RunCanceled),
        StopCause::TimedOut => Err(AppError::RunChildTimedOut(
            deadline.as_millis().try_into().unwrap_or(u64::MAX),
        )),
        StopCause::ChildFailed => Err(AppError::SupervisedRun(reason)),
    }
}

fn write_parent_message(
    writer: &mut BufWriter<std::process::ChildStdin>,
    message: &ParentMessage,
) -> AppResult<()> {
    serde_json::to_writer(&mut *writer, message)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

fn read_child_messages(stdout: std::process::ChildStdout, sender: mpsc::Sender<ChildReaderEvent>) {
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => {
                let _ = sender.send(ChildReaderEvent::Closed);
                return;
            }
            Ok(_) => match serde_json::from_str(line.trim_end()) {
                Ok(message) => {
                    if sender.send(ChildReaderEvent::Message(message)).is_err() {
                        return;
                    }
                }
                Err(error) => {
                    let _ = sender.send(ChildReaderEvent::Invalid(error.to_string()));
                    return;
                }
            },
            Err(error) => {
                let _ = sender.send(ChildReaderEvent::Invalid(error.to_string()));
                return;
            }
        }
    }
}

fn read_bounded(mut reader: impl Read, limit: usize) -> io::Result<String> {
    let mut bytes = Vec::with_capacity(limit.min(4096));
    reader
        .by_ref()
        .take((limit + 1) as u64)
        .read_to_end(&mut bytes)?;
    bytes.truncate(limit);
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

fn drain_after_exit(
    reader_receiver: &mpsc::Receiver<ChildReaderEvent>,
    reader_closed: &mut bool,
    stderr_receiver: &mpsc::Receiver<io::Result<String>>,
    stdout_reader: thread::JoinHandle<()>,
    stderr_reader: thread::JoinHandle<()>,
    timeout: Duration,
    discard_pending_messages: bool,
) -> AppResult<()> {
    let deadline = Instant::now() + timeout;
    let mut errors = Vec::new();
    let mut stdout_complete = *reader_closed;
    while !*reader_closed {
        let now = Instant::now();
        if now >= deadline {
            errors.push("run child event drain exceeded its deadline".into());
            break;
        }
        match reader_receiver.recv_timeout(deadline - now) {
            Ok(ChildReaderEvent::Closed) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                *reader_closed = true;
                stdout_complete = true;
            }
            Ok(ChildReaderEvent::Message(_)) if !discard_pending_messages => {
                errors.push("run child emitted an event after its result".into());
            }
            Ok(ChildReaderEvent::Message(_)) => {}
            Ok(ChildReaderEvent::Invalid(error)) => {
                errors.push(format!("run child event drain failed: {error}"));
                *reader_closed = true;
                stdout_complete = true;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                errors.push("run child event drain exceeded its deadline".into());
                break;
            }
        }
    }

    if stdout_complete {
        if stdout_reader.join().is_err() {
            errors.push("run child event reader panicked".into());
        }
    } else {
        errors.push("run child event reader was not joined before its deadline".into());
    }

    let remaining = deadline.saturating_duration_since(Instant::now());
    let stderr_complete = match stderr_receiver.recv_timeout(remaining) {
        Ok(Ok(stderr)) => {
            if !stderr.is_empty() {
                errors.push(format!(
                    "run child wrote unexpected stderr: {}",
                    stderr.trim()
                ));
            }
            true
        }
        Ok(Err(error)) => {
            errors.push(format!("run child stderr drain failed: {error}"));
            true
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => true,
        Err(mpsc::RecvTimeoutError::Timeout) => {
            errors.push("run child stderr drain exceeded its deadline".into());
            false
        }
    };
    if stderr_complete {
        if stderr_reader.join().is_err() {
            errors.push("run child stderr reader panicked".into());
        }
    } else {
        errors.push("run child stderr reader was not joined before its deadline".into());
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(AppError::SupervisedRun(errors.join("; ")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(target_os = "linux")]
    use crate::ApprovalRequest;
    use platonic_core::HarnessEvent;
    use std::fs;

    #[cfg(unix)]
    use crate::{RunLedger, RunOptions, RunSession, app::prepare_run, ledger::SqliteLedger};
    #[cfg(target_os = "linux")]
    use platonic_core::{EffectClass, ToolCallId};
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    #[cfg(unix)]
    use std::sync::mpsc;
    #[cfg(target_os = "linux")]
    use std::{
        collections::BTreeMap,
        ffi::OsString,
        net::{TcpListener, TcpStream},
        os::unix::ffi::{OsStrExt, OsStringExt},
    };

    #[test]
    fn resolver_keeps_exact_agentd_image() {
        let root = tempfile::tempdir().unwrap();
        let image = root
            .path()
            .join(format!("platonic{}", std::env::consts::EXE_SUFFIX));
        fs::write(&image, []).unwrap();

        assert_eq!(
            resolve_run_child_executable_from(&image, std::env::consts::EXE_SUFFIX).unwrap(),
            image
        );
    }

    #[test]
    fn resolver_uses_parent_sibling_for_literal_cargo_deps() {
        let root = tempfile::tempdir().unwrap();
        let profile = root.path().join("debug");
        let deps = profile.join("deps");
        fs::create_dir_all(&deps).unwrap();
        let host = deps.join(format!("plato-test{}", std::env::consts::EXE_SUFFIX));
        fs::write(&host, []).unwrap();
        let image = profile.join(format!("platonic{}", std::env::consts::EXE_SUFFIX));
        fs::write(&image, []).unwrap();

        assert_eq!(
            resolve_run_child_executable_from(&host, std::env::consts::EXE_SUFFIX).unwrap(),
            image
        );
    }

    #[test]
    fn resolver_uses_same_directory_sidecar() {
        let root = tempfile::tempdir().unwrap();
        let host = root
            .path()
            .join(format!("desktop-host{}", std::env::consts::EXE_SUFFIX));
        fs::write(&host, []).unwrap();
        let image = root
            .path()
            .join(format!("platonic{}", std::env::consts::EXE_SUFFIX));
        fs::write(&image, []).unwrap();

        assert_eq!(
            resolve_run_child_executable_from(&host, std::env::consts::EXE_SUFFIX).unwrap(),
            image
        );
    }

    #[test]
    fn resolver_fails_closed_for_missing_image() {
        let root = tempfile::tempdir().unwrap();
        let host = root
            .path()
            .join(format!("host{}", std::env::consts::EXE_SUFFIX));
        fs::write(&host, []).unwrap();

        let error =
            resolve_run_child_executable_from(&host, std::env::consts::EXE_SUFFIX).unwrap_err();
        assert!(
            matches!(error, AppError::SupervisedRun(reason) if reason.contains("is unavailable"))
        );
    }

    #[test]
    fn resolver_fails_closed_for_non_file_image() {
        let root = tempfile::tempdir().unwrap();
        let host = root
            .path()
            .join(format!("host{}", std::env::consts::EXE_SUFFIX));
        fs::write(&host, []).unwrap();
        let image = root
            .path()
            .join(format!("platonic{}", std::env::consts::EXE_SUFFIX));
        fs::create_dir(&image).unwrap();

        let error =
            resolve_run_child_executable_from(&host, std::env::consts::EXE_SUFFIX).unwrap_err();
        assert!(
            matches!(error, AppError::SupervisedRun(reason) if reason.contains("is not a regular file"))
        );
    }

    #[test]
    fn resolver_uses_the_platform_executable_name() {
        let name = format!("platonic{}", std::env::consts::EXE_SUFFIX);
        assert_eq!(name, "platonic");
    }

    #[test]
    fn production_supervisor_uses_explicit_bounded_lifecycle_limits() {
        let limits = ChildLifecycleLimits::default();
        assert_eq!(limits.deadline, Duration::from_secs(30 * 60));
        assert_eq!(limits.cancel_grace, Duration::from_millis(500));
        assert_eq!(limits.termination_grace, Duration::from_millis(250));
        assert_eq!(limits.kill_wait, Duration::from_secs(2));
        assert_eq!(limits.output_drain, Duration::from_secs(2));
        std::hint::black_box(run_supervised);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn post_ready_approval_error_cleans_tree_and_leaves_next_run_healthy() {
        temp_env::with_var(
            "PLATO_RUN_CHILD_TRANSPORT_TEST_KEY",
            Some("test-key"),
            || {
                let root = tempfile::tempdir().unwrap();
                let root_path = root.path().to_path_buf();
                let workspace = root.path().join("workspace");
                fs::create_dir(&workspace).unwrap();
                fs::write(
                    workspace.join("plato.toml"),
                    r#"[provider]
kind = "open_ai"
model = "test-model"
api_key_env = "PLATO_RUN_CHILD_TRANSPORT_TEST_KEY"
base_url = "http://127.0.0.1:1"

[limits]
token_budget = 4000
max_output_tokens = 64
max_turns = 2

[tools]
enabled = ["file.read"]
"#,
                )
                .unwrap();
                let ledger_path = root.path().join("transport.db");
                let run_id = RunId::new("run_parent_transport_failure").unwrap();
                let (prepared, recorder) = prepare_run(&RunOptions {
                    question: "exercise parent transport cleanup".into(),
                    config_path: Some("plato.toml".into()),
                    overrides: Default::default(),
                    ledger: RunLedger::Sqlite(ledger_path.clone()),
                    workspace_root: workspace.clone(),
                    approval_mode: ApprovalMode::Deny { actor: "test" },
                    run_id: Some(run_id.clone()),
                    session: Some(RunSession::Fresh {
                        session_id: "session_parent_transport_failure".into(),
                    }),
                    event_sender: None,
                    stream_to_stderr: false,
                    cancel: None,
                    voice_interruption_context: None,
                })
                .unwrap();
                let run_started = serde_json::to_string(&ChildMessage::Record {
                    request_id: 1,
                    operation: RecordOperation::Event {
                        event: HarnessEvent::RunStarted(platonic_core::RunStartedEvent {
                            run_id: run_id.clone(),
                            identity: platonic_core::RunIdentity::LegacyAgent {
                                agent_id: platonic_core::AgentId::new("plato").unwrap(),
                            },
                        }),
                    },
                })
                .unwrap();
                let approval = serde_json::to_string(&ChildMessage::Approval {
                    request_id: 2,
                    request: ApprovalRequest {
                        run_id: run_id.clone(),
                        call_id: ToolCallId::new("call_parent_transport_failure").unwrap(),
                        tool_name: "shell.exec".into(),
                        effect: EffectClass::ExternalSideEffect,
                        reason: "test approval required".into(),
                        input_preview: Some("{}".into()),
                        approval_preview: None,
                        diff_preview: None,
                        yolo_eligible: false,
                    },
                })
                .unwrap();
                let descendant_pid_path = root.path().join("descendant.pid");
                let descendant_pid_path_display = descendant_pid_path.display();
                let fixture = root.path().join("parent-transport-run-child");
                fs::write(
                    &fixture,
                    format!(
                        r#"#!/bin/sh
trap '' TERM
IFS= read -r _
printf '{{"kind":"ready","request_id":0,"pid":%s}}\n' "$$"
IFS= read -r _
/bin/sh -c 'trap "" TERM; while :; do :; done' &
printf '%s\n' "$!" > '{descendant_pid_path_display}'
printf '%s\n' '{run_started}'
IFS= read -r _
printf '%s\n' '{approval}'
while :; do :; done
"#
                    ),
                )
                .unwrap();
                fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();

                let (event_sender, event_receiver) = mpsc::channel();
                let (ready_sender, ready_receiver) = mpsc::channel();
                let result = run_supervised_with_limits(
                    prepared,
                    recorder,
                    ApprovalMode::external_with_actor("test", |_| {
                        Err(AppError::SupervisedRun(
                            "test approval authority rejected the request".into(),
                        ))
                    }),
                    event_sender,
                    Arc::new(AtomicBool::new(false)),
                    None,
                    ChildLaunch {
                        limits: ChildLifecycleLimits {
                            deadline: Duration::from_secs(5),
                            cancel_grace: Duration::from_millis(50),
                            termination_grace: Duration::from_millis(50),
                            kill_wait: Duration::from_secs(2),
                            output_drain: Duration::from_secs(2),
                        },
                        executable: fixture,
                        ready_child: Some(ready_sender),
                        confinement: crate::confinement::ChildConfinement::None,
                        terminal_stage_barriers: None,
                    },
                );
                let result = publish_for_test(result);
                let child_pid = ready_receiver.recv_timeout(Duration::from_secs(2)).unwrap();
                let descendant_pid = fs::read_to_string(&descendant_pid_path)
                    .unwrap()
                    .trim()
                    .parse::<u32>()
                    .unwrap();
                assert!(matches!(
                    result,
                    Err(AppError::SupervisedRun(reason))
                        if reason == "test approval authority rejected the request"
                ));
                assert!(!Path::new(&format!("/proc/{child_pid}")).exists());
                assert!(!Path::new(&format!("/proc/{descendant_pid}")).exists());

                let records = SqliteLedger::open_readonly(&ledger_path)
                    .unwrap()
                    .read_run(run_id.as_str())
                    .unwrap();
                assert!(matches!(
                    records.last().map(|record| &record.event),
                    Some(HarnessEvent::RunFailed { reason, .. })
                        if reason == "test approval authority rejected the request"
                ));
                assert_eq!(
                    event_receiver
                        .try_iter()
                        .filter(|event| matches!(event, RunEvent::Ledger(_)))
                        .count()
                        + 1,
                    records.len()
                );

                let healthy_run_id = RunId::new("run_after_parent_transport_failure").unwrap();
                let (healthy_prepared, healthy_recorder) = prepare_run(&RunOptions {
                    question: "prove the next run is healthy".into(),
                    config_path: Some("plato.toml".into()),
                    overrides: Default::default(),
                    ledger: RunLedger::Sqlite(ledger_path.clone()),
                    workspace_root: workspace,
                    approval_mode: ApprovalMode::Deny { actor: "test" },
                    run_id: Some(healthy_run_id.clone()),
                    session: Some(RunSession::Fresh {
                        session_id: "session_after_parent_transport_failure".into(),
                    }),
                    event_sender: None,
                    stream_to_stderr: false,
                    cancel: None,
                    voice_interruption_context: None,
                })
                .unwrap();
                let healthy_started = serde_json::to_string(&ChildMessage::Record {
                    request_id: 1,
                    operation: RecordOperation::Event {
                        event: HarnessEvent::RunStarted(platonic_core::RunStartedEvent {
                            run_id: healthy_run_id.clone(),
                            identity: platonic_core::RunIdentity::LegacyAgent {
                                agent_id: platonic_core::AgentId::new("plato").unwrap(),
                            },
                        }),
                    },
                })
                .unwrap();
                let healthy_terminal = serde_json::to_string(&ChildMessage::Record {
                    request_id: 2,
                    operation: RecordOperation::Fail {
                        run_id: healthy_run_id.clone(),
                        error: "scripted healthy-run failure".into(),
                        canceled: false,
                    },
                })
                .unwrap();
                let healthy_result = serde_json::to_string(&ChildMessage::Result {
                    request_id: 3,
                    result: ChildRunResult::Failed {
                        error: "scripted healthy-run failure".into(),
                    },
                })
                .unwrap();
                let healthy_fixture = root.path().join("healthy-run-child");
                fs::write(
                    &healthy_fixture,
                    format!(
                        r#"#!/bin/sh
IFS= read -r _
printf '{{"kind":"ready","request_id":0,"pid":%s}}\n' "$$"
IFS= read -r _
printf '%s\n' '{healthy_started}'
IFS= read -r _
printf '%s\n' '{healthy_terminal}'
IFS= read -r _
printf '%s\n' '{healthy_result}'
IFS= read -r _
"#
                    ),
                )
                .unwrap();
                fs::set_permissions(&healthy_fixture, fs::Permissions::from_mode(0o700)).unwrap();
                let (healthy_event_sender, _healthy_event_receiver) = mpsc::channel();
                let healthy_result = run_supervised_with_limits(
                    healthy_prepared,
                    healthy_recorder,
                    ApprovalMode::Deny { actor: "test" },
                    healthy_event_sender,
                    Arc::new(AtomicBool::new(false)),
                    None,
                    ChildLaunch {
                        limits: ChildLifecycleLimits::default(),
                        executable: healthy_fixture,
                        ready_child: None,
                        confinement: crate::confinement::ChildConfinement::None,
                        terminal_stage_barriers: None,
                    },
                );
                let healthy_result = publish_for_test(healthy_result);
                assert!(matches!(
                    healthy_result,
                    Err(AppError::SupervisedRun(reason))
                        if reason == "scripted healthy-run failure"
                ));
                let healthy_records = SqliteLedger::open_readonly(&ledger_path)
                    .unwrap()
                    .read_run(healthy_run_id.as_str())
                    .unwrap();
                assert!(matches!(
                    healthy_records.last().map(|record| &record.event),
                    Some(HarnessEvent::RunFailed { reason, .. })
                        if reason == "scripted healthy-run failure"
                ));

                drop(root);
                assert!(!root_path.exists());
            },
        );
    }

    #[cfg(unix)]
    #[test]
    fn deadline_kills_wedged_descendant_tree_and_drains_output() {
        let proof = run_wedged_child(false);
        assert!(matches!(proof.result, Err(AppError::RunChildTimedOut(100))));
        assert!(matches!(
            proof.records.last().map(|record| &record.event),
            Some(HarnessEvent::RunFailed { reason, .. })
                if reason == "run child deadline exceeded"
        ));
    }

    #[cfg(unix)]
    #[test]
    fn cancellation_grace_kills_wedged_descendant_tree_and_drains_output() {
        let proof = run_wedged_child(true);
        assert!(matches!(proof.result, Err(AppError::RunCanceled)));
        assert!(matches!(
            proof.records.last().map(|record| &record.event),
            Some(HarnessEvent::RunFailed { reason, .. }) if reason == RUN_CANCELED_REASON
        ));
    }

    #[cfg(unix)]
    struct WedgedChildProof {
        result: AppResult<RunOutcome>,
        records: Vec<RecordedEvent>,
    }

    #[cfg(unix)]
    fn run_wedged_child(cancel_on_ready: bool) -> WedgedChildProof {
        temp_env::with_var(
            "PLATO_RUN_CHILD_LIFECYCLE_TEST_KEY",
            Some("test-key"),
            || {
                let root = tempfile::tempdir().unwrap();
                let workspace = root.path().join("workspace");
                fs::create_dir(&workspace).unwrap();
                fs::write(
                    workspace.join("plato.toml"),
                    r#"[provider]
kind = "open_ai"
model = "test-model"
api_key_env = "PLATO_RUN_CHILD_LIFECYCLE_TEST_KEY"
base_url = "http://127.0.0.1:1"

[limits]
token_budget = 4000
max_output_tokens = 64
max_turns = 2

[tools]
enabled = ["file.read"]
"#,
                )
                .unwrap();
                let ledger_path = root.path().join("lifecycle.db");
                let run_id = RunId::new("run_timeout_fixture").unwrap();
                let (prepared, recorder) = prepare_run(&RunOptions {
                    question: "exercise the child deadline".into(),
                    config_path: Some("plato.toml".into()),
                    overrides: Default::default(),
                    ledger: RunLedger::Sqlite(ledger_path.clone()),
                    workspace_root: workspace,
                    approval_mode: ApprovalMode::Deny { actor: "test" },
                    run_id: Some(run_id.clone()),
                    session: Some(RunSession::Fresh {
                        session_id: "session_timeout_fixture".into(),
                    }),
                    event_sender: None,
                    stream_to_stderr: false,
                    cancel: None,
                    voice_interruption_context: None,
                })
                .unwrap();
                let start_message = serde_json::to_string(&ParentMessage::Start {
                    prepared: Box::new(prepared.clone()),
                })
                .unwrap();
                assert!(!start_message.contains(&*ledger_path.to_string_lossy()));
                let fixture = root.path().join("wedged-run-child");
                fs::write(
                &fixture,
                r#"#!/bin/sh
trap '' TERM
IFS= read -r _
printf '{"kind":"ready","request_id":0,"pid":%s}\n' "$$"
IFS= read -r _
printf '%s\n' '{"kind":"record","request_id":1,"operation":{"operation":"event","event":{"event":"run_started","run_id":"run_timeout_fixture","agent_id":"plato"}}}'
IFS= read -r _
/bin/sh -c 'trap "" TERM; while :; do :; done' &
while :; do :; done
"#,
            )
            .unwrap();
                fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();
                let (event_sender, event_receiver) = mpsc::channel();
                let (ready_sender, ready_receiver) = mpsc::channel();
                let cancel = Arc::new(AtomicBool::new(false));
                let (cancel_trigger, deadline_ready) = if cancel_on_ready {
                    let cancel = cancel.clone();
                    let trigger = thread::spawn(move || {
                        let child_pid = ready_receiver.recv().unwrap();
                        cancel.store(true, Ordering::SeqCst);
                        child_pid
                    });
                    (Some(trigger), None)
                } else {
                    (None, Some(ready_receiver))
                };
                let deadline = if cancel_on_ready {
                    Duration::from_secs(5)
                } else {
                    Duration::from_millis(100)
                };
                let result = run_supervised_with_limits(
                    prepared,
                    recorder,
                    ApprovalMode::Deny { actor: "test" },
                    event_sender,
                    cancel,
                    None,
                    ChildLaunch {
                        limits: ChildLifecycleLimits {
                            deadline,
                            cancel_grace: Duration::from_millis(50),
                            termination_grace: Duration::from_millis(50),
                            kill_wait: Duration::from_secs(2),
                            output_drain: Duration::from_secs(2),
                        },
                        executable: fixture,
                        ready_child: Some(ready_sender),
                        confinement: crate::confinement::ChildConfinement::None,
                        terminal_stage_barriers: None,
                    },
                );
                let result = publish_for_test(result);
                #[cfg_attr(not(target_os = "linux"), allow(unused_variables))]
                let child_pid = match cancel_trigger {
                    Some(trigger) => trigger.join().unwrap(),
                    None => deadline_ready
                        .unwrap()
                        .recv_timeout(Duration::from_secs(2))
                        .unwrap(),
                };
                #[cfg(target_os = "linux")]
                assert!(!std::path::Path::new(&format!("/proc/{child_pid}")).exists());

                let records = SqliteLedger::open_readonly(&ledger_path)
                    .unwrap()
                    .read_run(run_id.as_str())
                    .unwrap();
                assert_eq!(
                    event_receiver
                        .try_iter()
                        .filter(|event| matches!(event, RunEvent::Ledger(_)))
                        .count()
                        + 1,
                    records.len()
                );
                WedgedChildProof { result, records }
            },
        )
    }

    #[cfg(target_os = "linux")]
    const RUN_CHILD_PROVIDER_ENV: &str = "PLATONIC_RUN_CHILD_CUSTOM_PROVIDER";
    #[cfg(target_os = "linux")]
    const RUN_CHILD_PROVIDER_SENTINEL: &str = "non-secret-provider-sentinel";
    #[cfg(target_os = "linux")]
    const RUN_CHILD_ENV_DRIVER_FIXTURE: &str =
        "daemon::run_child::supervisor::tests::supervised_environment_driver_fixture";
    #[cfg(target_os = "linux")]
    const RUN_CHILD_BASELINE_NAMES: &[&str] = &[
        "PATH",
        "HOME",
        "USER",
        "LOGNAME",
        "SHELL",
        "TERM",
        "COLORTERM",
        "NO_COLOR",
        "LANG",
        "LC_ALL",
        "TMPDIR",
        "TEMP",
        "TMP",
        "CARGO_HOME",
        "RUSTUP_HOME",
    ];

    #[cfg(target_os = "linux")]
    #[test]
    fn supervised_child_runs_with_minimal_env_and_shell_grandchild_stays_scrubbed() {
        let root = tempfile::tempdir().unwrap();
        let home = root.path().join("home");
        let temp = root.path().join("tmp");
        let cargo_home = root.path().join("cargo-home");
        let rustup_home = root.path().join("rustup-home");
        for path in [&home, &temp, &cargo_home, &rustup_home] {
            fs::create_dir(path).unwrap();
        }
        let mut parent_env: Vec<(OsString, OsString)> = vec![
            ("PATH".into(), "/usr/bin:/bin".into()),
            ("HOME".into(), home.into_os_string()),
            ("USER".into(), "platonic-test".into()),
            ("LOGNAME".into(), "platonic-test".into()),
            ("SHELL".into(), "/bin/sh".into()),
            ("TERM".into(), "dumb".into()),
            ("COLORTERM".into(), "none".into()),
            ("NO_COLOR".into(), "1".into()),
            ("LANG".into(), "C".into()),
            ("LC_ALL".into(), "C".into()),
            ("TMPDIR".into(), temp.into_os_string()),
            ("TEMP".into(), "/tmp".into()),
            ("TMP".into(), "/tmp".into()),
            ("CARGO_HOME".into(), cargo_home.into_os_string()),
            ("RUSTUP_HOME".into(), rustup_home.into_os_string()),
        ];
        parent_env.extend([
            (
                RUN_CHILD_PROVIDER_ENV.into(),
                RUN_CHILD_PROVIDER_SENTINEL.into(),
            ),
            ("OPENAI_API_KEY".into(), "openai-sentinel".into()),
            ("GITHUB_TOKEN".into(), "github-sentinel".into()),
            ("AWS_SECRET_ACCESS_KEY".into(), "aws-secret-sentinel".into()),
            ("NPM_TOKEN".into(), "npm-sentinel".into()),
            (
                "CARGO_REGISTRIES_CRATES_IO_TOKEN".into(),
                "cargo-token-sentinel".into(),
            ),
            ("SSH_AUTH_SOCK".into(), "/tmp/agent-socket-sentinel".into()),
            ("UNKNOWN_PARENT_SETTING".into(), "unknown-sentinel".into()),
            (
                "UNRELATED_NON_UTF8_VALUE".into(),
                OsString::from_vec(b"value-\xff".to_vec()),
            ),
            (
                OsString::from_vec(b"UNRELATED_NON_UTF8_NAME_\xff".to_vec()),
                "name-sentinel".into(),
            ),
        ]);

        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", RUN_CHILD_ENV_DRIVER_FIXTURE])
            .env_clear()
            .envs(parent_env.iter().map(|(name, value)| (name, value)))
            .output()
            .unwrap();

        assert!(output.status.success(), "environment driver fixture failed");
        for captured in [&output.stdout, &output.stderr] {
            assert!(
                !captured
                    .windows(RUN_CHILD_PROVIDER_SENTINEL.len())
                    .any(|window| window == RUN_CHILD_PROVIDER_SENTINEL.as_bytes()),
                "provider credential reached captured child output"
            );
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "launched in an isolated sentinel-rich parent environment"]
    fn supervised_environment_driver_fixture() {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        fs::create_dir(&workspace).unwrap();

        let (provider, base_url) = spawn_run_child_provider(RUN_CHILD_PROVIDER_SENTINEL);
        fs::write(
            workspace.join("plato.toml"),
            format!(
                r#"[provider]
kind = "open_ai"
model = "test-model"
api_key_env = "{RUN_CHILD_PROVIDER_ENV}"
base_url = "{base_url}"

[limits]
token_budget = 4000
max_output_tokens = 64
max_turns = 2

[tools]
enabled = ["shell.exec"]
"#
            ),
        )
        .unwrap();
        let ledger_path = root.path().join("minimal-environment.db");
        let run_id = RunId::new("run_minimal_child_environment").unwrap();
        let test_binary = std::env::current_exe().unwrap();
        let test_binary = test_binary.to_string_lossy().replace('\'', "'\\''");
        let fixture = root.path().join("stdio-run-child");
        fs::write(
            &fixture,
            format!(
                r#"#!/bin/sh
wrapper_pid=$$
'{test_binary}' --ignored --exact daemon::run_child::child::tests::supervised_stdio_child_fixture --nocapture |
while IFS= read -r line; do
    case "$line" in
        '{{"kind":"ready","request_id":'*)
            printf '{{"kind":"ready","request_id":0,"pid":%s}}\n' "$wrapper_pid"
            ;;
        '{{'*)
            printf '%s\n' "$line"
            ;;
    esac
done
"#
            ),
        )
        .unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();

        let baseline = RUN_CHILD_BASELINE_NAMES
            .iter()
            .map(|name| (*name, std::env::var_os(name).unwrap()))
            .collect::<Vec<_>>();

        let (prepared, recorder) = prepare_run(&RunOptions {
            question: "exercise the local provider and scrubbed shell".into(),
            config_path: Some("plato.toml".into()),
            overrides: Default::default(),
            ledger: RunLedger::Sqlite(ledger_path.clone()),
            workspace_root: workspace,
            approval_mode: ApprovalMode::Deny { actor: "test" },
            run_id: Some(run_id.clone()),
            session: Some(RunSession::Fresh {
                session_id: "session_minimal_child_environment".into(),
            }),
            event_sender: None,
            stream_to_stderr: false,
            cancel: None,
            voice_interruption_context: None,
        })
        .unwrap();
        let start_message = serde_json::to_string(&ParentMessage::Start {
            prepared: Box::new(prepared.clone()),
        })
        .unwrap();
        assert!(!start_message.contains(RUN_CHILD_PROVIDER_SENTINEL));

        let (event_sender, event_receiver) = mpsc::channel();
        let (ready_sender, ready_receiver) = mpsc::channel();
        let supervisor = thread::spawn(move || {
            publish_for_test(run_supervised_with_limits(
                prepared,
                recorder,
                ApprovalMode::external_with_actor("test", |_| {
                    Ok(ExternalApprovalOutcome::Granted {
                        actor: "test".into(),
                    })
                }),
                event_sender,
                Arc::new(AtomicBool::new(false)),
                None,
                ChildLaunch {
                    limits: ChildLifecycleLimits {
                        deadline: Duration::from_secs(10),
                        ..ChildLifecycleLimits::default()
                    },
                    executable: fixture,
                    ready_child: Some(ready_sender),
                    confinement: crate::confinement::ChildConfinement::None,
                    terminal_stage_barriers: None,
                },
            ))
        });
        let child_pid = ready_receiver.recv_timeout(Duration::from_secs(5)).unwrap();
        let actual_env = read_proc_environment(child_pid);
        provider.release_first_response.send(()).unwrap();

        let outcome = supervisor.join().unwrap().unwrap();
        let event_records = event_receiver
            .try_iter()
            .filter_map(|event| match event {
                RunEvent::Ledger(record) => Some(record),
                RunEvent::AssistantDelta(_) => None,
            })
            .collect::<Vec<_>>();
        let records = SqliteLedger::open_readonly(&ledger_path)
            .unwrap()
            .read_run(run_id.as_str())
            .unwrap();

        let mut expected_env = baseline
            .iter()
            .map(|(name, value)| {
                (
                    name.as_bytes().to_vec(),
                    value.as_os_str().as_bytes().to_vec(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        expected_env.insert(
            RUN_CHILD_PROVIDER_ENV.as_bytes().to_vec(),
            RUN_CHILD_PROVIDER_SENTINEL.as_bytes().to_vec(),
        );
        assert_eq!(
            actual_env.keys().collect::<Vec<_>>(),
            expected_env.keys().collect::<Vec<_>>()
        );
        for (name, expected_value) in expected_env {
            assert!(
                actual_env.get(&name) == Some(&expected_value),
                "supervised child value differed for {}",
                String::from_utf8_lossy(&name)
            );
        }
        assert_eq!(outcome.final_answer, "done");
        assert_eq!(event_records.len() + 1, records.len());
        let live = serde_json::to_string(&event_records).unwrap();
        assert!(!live.contains(RUN_CHILD_PROVIDER_SENTINEL));
        let durable = serde_json::to_string(&records).unwrap();
        assert!(durable.contains("runtime-and-scrub-ok"));
        assert!(!durable.contains(RUN_CHILD_PROVIDER_SENTINEL));
        let outcome = serde_json::to_string(&outcome).unwrap();
        assert!(!outcome.contains(RUN_CHILD_PROVIDER_SENTINEL));

        let provider_proof = provider.handle.join().unwrap();
        assert_eq!(provider_proof.request_count, 2);
        assert!(provider_proof.authorization_was_exact);
        assert!(provider_proof.shell_reported_scrubbed);
    }

    #[cfg(target_os = "linux")]
    struct RunChildProvider {
        release_first_response: mpsc::Sender<()>,
        handle: thread::JoinHandle<RunChildProviderProof>,
    }

    #[cfg(target_os = "linux")]
    struct RunChildProviderProof {
        request_count: usize,
        authorization_was_exact: bool,
        shell_reported_scrubbed: bool,
    }

    #[cfg(target_os = "linux")]
    fn spawn_run_child_provider(provider_sentinel: &'static str) -> (RunChildProvider, String) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let (release_sender, release_receiver) = mpsc::channel();
        let handle = thread::spawn(move || {
            let shell_command = concat!(
                "test -n \"$PATH\" && test -n \"$HOME\" && ",
                "test -n \"$TMPDIR\" && test -n \"$CARGO_HOME\" && ",
                "test -n \"$RUSTUP_HOME\" && ",
                "test \"${PLATONIC_RUN_CHILD_CUSTOM_PROVIDER+x}\" != x && ",
                "test \"${OPENAI_API_KEY+x}\" != x && ",
                "test \"${GITHUB_TOKEN+x}\" != x && ",
                "test \"${AWS_SECRET_ACCESS_KEY+x}\" != x && ",
                "test \"${NPM_TOKEN+x}\" != x && ",
                "test \"${CARGO_REGISTRIES_CRATES_IO_TOKEN+x}\" != x && ",
                "test \"${SSH_AUTH_SOCK+x}\" != x && ",
                "test \"${UNKNOWN_PARENT_SETTING+x}\" != x && ",
                "printf runtime-and-scrub-ok"
            );
            let arguments = serde_json::to_string(&serde_json::json!({
                "command": shell_command
            }))
            .unwrap();
            let responses = [
                format!(
                    "data: {}\n\ndata: {{\"choices\":[{{\"index\":0,\"delta\":{{}},\"finish_reason\":\"tool_calls\"}}]}}\n\ndata: [DONE]\n\n",
                    serde_json::json!({
                        "choices": [{
                            "index": 0,
                            "delta": {
                                "tool_calls": [{
                                    "index": 0,
                                    "id": "provider_shell",
                                    "function": {
                                        "name": "shell_exec",
                                        "arguments": arguments
                                    }
                                }]
                            },
                            "finish_reason": null
                        }]
                    })
                ),
                concat!(
                    "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"done\"},\"finish_reason\":null}]}\n\n",
                    "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                    "data: [DONE]\n\n"
                )
                .into(),
            ];
            let mut authorization_was_exact = true;
            let mut shell_reported_scrubbed = false;
            for (index, response) in responses.into_iter().enumerate() {
                let (mut stream, _) = listener.accept().unwrap();
                let request = read_run_child_provider_request(&mut stream);
                authorization_was_exact &= request.lines().any(|line| {
                    line.split_once(':').is_some_and(|(name, value)| {
                        name.eq_ignore_ascii_case("authorization")
                            && value.trim() == format!("Bearer {provider_sentinel}")
                    })
                });
                if index == 0 {
                    release_receiver
                        .recv_timeout(Duration::from_secs(5))
                        .unwrap();
                } else {
                    shell_reported_scrubbed = request.contains("runtime-and-scrub-ok");
                }
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    response.len(),
                    response
                )
                .unwrap();
            }
            RunChildProviderProof {
                request_count: 2,
                authorization_was_exact,
                shell_reported_scrubbed,
            }
        });
        (
            RunChildProvider {
                release_first_response: release_sender,
                handle,
            },
            base_url,
        )
    }

    #[cfg(target_os = "linux")]
    fn read_run_child_provider_request(stream: &mut TcpStream) -> String {
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 1024];
        let header_end = loop {
            let read = stream.read(&mut buffer).unwrap();
            assert_ne!(read, 0, "provider client closed before headers");
            bytes.extend_from_slice(&buffer[..read]);
            if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break index + 4;
            }
        };
        let headers = String::from_utf8_lossy(&bytes[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.split_once(':').and_then(|(name, value)| {
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
            })
            .unwrap_or(0);
        while bytes.len() < header_end + content_length {
            let read = stream.read(&mut buffer).unwrap();
            assert_ne!(read, 0, "provider client closed before body");
            bytes.extend_from_slice(&buffer[..read]);
        }
        String::from_utf8(bytes).unwrap()
    }

    #[cfg(target_os = "linux")]
    fn read_proc_environment(pid: u32) -> BTreeMap<Vec<u8>, Vec<u8>> {
        fs::read(format!("/proc/{pid}/environ"))
            .unwrap()
            .split(|byte| *byte == 0)
            .filter(|entry| !entry.is_empty())
            .map(|entry| {
                let separator = entry
                    .iter()
                    .position(|byte| *byte == b'=')
                    .expect("environment entry has a name and value");
                let (name, value) = entry.split_at(separator);
                (name.to_vec(), value[1..].to_vec())
            })
            .collect()
    }

    #[cfg(unix)]
    fn publish_for_test(completion: SupervisedRunCompletion) -> AppResult<RunOutcome> {
        let (outcome, terminal) = completion.publish();
        terminal.unwrap();
        outcome
    }
}
