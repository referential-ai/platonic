use crate::{
    AppError, AppResult, ApprovalMode, ApprovalRequest, AssistantDeltaEvent, RunEvent, RunOutcome,
    app::{ExternalApprovalOutcome, PreparedRun, run_prepared_question},
    daemon::child_process::ProcessTreeChild,
    ledger::{EventRecorder, RUN_CANCELED_REASON, RunEventRecorder},
    tool_catalog::THREAD_SPAWN,
    tools::{ThreadSpawnToolHandler, ThreadSpawnToolInput, ThreadSpawnToolOutput},
};
use platonic_core::{HarnessEvent, RecordedEvent, RunId};
use serde::{Deserialize, Serialize};
use std::{
    ffi::OsStr,
    fs,
    io::{self, BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
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
const OUTPUT_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);
const SUPERVISOR_POLL_INTERVAL: Duration = Duration::from_millis(25);
const STDERR_LIMIT: usize = 32 * 1024;

#[derive(Clone, Copy, Debug)]
pub(super) struct ChildLifecycleLimits {
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

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
enum ParentMessage {
    Start {
        prepared: Box<PreparedRun>,
    },
    Ack {
        request_id: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        record: Option<RecordedEvent>,
    },
    Reject {
        request_id: u64,
        error: String,
    },
    Approval {
        request_id: u64,
        outcome: ApprovalReply,
    },
    ThreadSpawn {
        request_id: u64,
        output: ThreadSpawnToolOutput,
    },
    Cancel,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "decision")]
enum ApprovalReply {
    Granted { actor: String },
    Denied { actor: String, reason: String },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
enum ChildMessage {
    Ready {
        request_id: u64,
        pid: u32,
    },
    Record {
        request_id: u64,
        operation: RecordOperation,
    },
    AssistantDelta {
        request_id: u64,
        delta: AssistantDeltaEvent,
    },
    Approval {
        request_id: u64,
        request: ApprovalRequest,
    },
    ThreadSpawn {
        request_id: u64,
        input: ThreadSpawnToolInput,
        approving_actor: String,
    },
    Result {
        request_id: u64,
        result: ChildRunResult,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "operation")]
enum RecordOperation {
    Event {
        event: HarnessEvent,
    },
    Finish {
        run_id: RunId,
        final_answer: String,
    },
    Fail {
        run_id: RunId,
        error: String,
        canceled: bool,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
enum ChildRunResult {
    Finished { outcome: RunOutcome },
    Canceled,
    Failed { error: String },
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

pub(super) struct SupervisedRunCompletion {
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

    pub(super) fn override_failure(self, error: AppError) -> Self {
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

    pub(super) fn publish(self) -> (AppResult<RunOutcome>, AppResult<RecordedEvent>) {
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

impl RecordOperation {
    fn run_id(&self) -> &RunId {
        match self {
            Self::Event { event } => event.run_id(),
            Self::Finish { run_id, .. } | Self::Fail { run_id, .. } => run_id,
        }
    }

    fn event(&self) -> HarnessEvent {
        match self {
            Self::Event { event } => event.clone(),
            Self::Finish { run_id, .. } => HarnessEvent::RunFinished {
                run_id: run_id.clone(),
            },
            Self::Fail { run_id, error, .. } => HarnessEvent::RunFailed {
                run_id: run_id.clone(),
                reason: error.clone(),
            },
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
    #[cfg(test)]
    terminal_stage_barriers: Option<TerminalStageBarriers>,
}

#[cfg(test)]
pub(super) struct TerminalStageBarriers {
    pub(super) reached: Arc<std::sync::Barrier>,
    pub(super) release: Arc<std::sync::Barrier>,
}

#[cfg(all(test, target_os = "linux"))]
pub(super) struct SupervisedTestLaunch {
    pub(super) executable: PathBuf,
    pub(super) ready_child: mpsc::Sender<u32>,
    pub(super) terminal_stage_barriers: TerminalStageBarriers,
}

pub(super) fn run_supervised(
    prepared: PreparedRun,
    recorder: EventRecorder,
    approval_mode: ApprovalMode,
    event_sender: mpsc::Sender<RunEvent>,
    cancel: Arc<AtomicBool>,
    thread_spawn: Option<ThreadSpawnToolHandler>,
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
            #[cfg(test)]
            terminal_stage_barriers: None,
        },
    )
}

#[cfg(all(test, target_os = "linux"))]
pub(super) fn run_supervised_for_test(
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
        #[cfg(test)]
        terminal_stage_barriers,
    } = launch;
    let run_id = prepared.run_id().clone();
    let mut recorder = ParentRunRecorder {
        recorder,
        event_sender,
        terminal: None,
    };
    let mut command = Command::new(executable);
    command
        .arg(CHILD_MODE_ARG)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
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

pub fn run_stdio_child() -> AppResult<()> {
    let cancel = Arc::new(AtomicBool::new(false));
    let (parent_sender, parent_receiver) = mpsc::channel();
    let reader_cancel = cancel.clone();
    let input_reader = thread::spawn(move || read_parent_messages(parent_sender, reader_cancel));
    let prepared = match parent_receiver.recv() {
        Ok(Ok(ParentMessage::Start { prepared })) => *prepared,
        Ok(Ok(_)) => {
            return Err(AppError::SupervisedRun(
                "run child expected start as its first parent message".into(),
            ));
        }
        Ok(Err(error)) => return Err(AppError::SupervisedRun(error)),
        Err(_) => {
            return Err(AppError::SupervisedRun(
                "run child parent stream closed before start".into(),
            ));
        }
    };
    let rpc = Arc::new(ChildRpc {
        writer: Mutex::new(BufWriter::new(io::stdout())),
        replies: Mutex::new(parent_receiver),
        transaction: Mutex::new(()),
        next_request_id: AtomicU64::new(0),
    });
    rpc.ready()?;
    let (event_sender, event_receiver) = mpsc::channel();
    let (delta_ack_sender, delta_ack_receiver) = mpsc::channel();
    let mut recorder = ChildTransportRecorder {
        rpc: rpc.clone(),
        delta_drain: AssistantDeltaDrain::new(delta_ack_receiver),
        next_seq: 0,
    };
    let event_rpc = rpc.clone();
    let event_forwarder = thread::spawn(move || {
        forward_child_events(
            event_receiver,
            |delta| event_rpc.assistant_delta(delta),
            delta_ack_sender,
        )
    });
    let approval_rpc = rpc.clone();
    let thread_spawn = prepared.has_tool(THREAD_SPAWN).then(|| {
        let thread_spawn_rpc = rpc.clone();
        ThreadSpawnToolHandler::new(move |input, approving_actor| {
            thread_spawn_rpc.thread_spawn(input, approving_actor)
        })
    });
    let outcome = run_prepared_question(
        prepared,
        &mut recorder,
        ApprovalMode::external_with_actor("daemon", move |request| approval_rpc.approval(request)),
        Some(event_sender),
        false,
        Some(cancel),
        thread_spawn,
    );
    event_forwarder
        .join()
        .map_err(|_| AppError::SupervisedRun("run child event forwarder panicked".into()))??;
    let result = match outcome {
        Ok(outcome) => ChildRunResult::Finished { outcome },
        Err(AppError::RunCanceled) => ChildRunResult::Canceled,
        Err(error) => ChildRunResult::Failed {
            error: error.to_string(),
        },
    };
    rpc.result(result)?;
    drop(rpc);
    drop(input_reader);
    Ok(())
}

fn read_parent_messages(
    sender: mpsc::Sender<Result<ParentMessage, String>>,
    cancel: Arc<AtomicBool>,
) {
    let stdin = io::stdin();
    let mut reader = BufReader::new(stdin.lock());
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => return,
            Ok(_) => match serde_json::from_str::<ParentMessage>(line.trim_end()) {
                Ok(ParentMessage::Cancel) => cancel.store(true, Ordering::SeqCst),
                Ok(message) => {
                    if sender.send(Ok(message)).is_err() {
                        return;
                    }
                }
                Err(error) => {
                    let _ = sender.send(Err(error.to_string()));
                    return;
                }
            },
            Err(error) => {
                let _ = sender.send(Err(error.to_string()));
                return;
            }
        }
    }
}

struct ChildRpc {
    writer: Mutex<BufWriter<io::Stdout>>,
    replies: Mutex<mpsc::Receiver<Result<ParentMessage, String>>>,
    transaction: Mutex<()>,
    next_request_id: AtomicU64,
}

impl ChildRpc {
    fn ready(&self) -> AppResult<()> {
        let _transaction = self.transaction.lock().expect("child RPC lock poisoned");
        let request_id = self.next_request_id();
        self.send(&ChildMessage::Ready {
            request_id,
            pid: std::process::id(),
        })?;
        self.expect_ack(request_id).map(|_| ())
    }

    fn record(&self, operation: RecordOperation) -> AppResult<RecordedEvent> {
        let _transaction = self.transaction.lock().expect("child RPC lock poisoned");
        let request_id = self.next_request_id();
        self.send(&ChildMessage::Record {
            request_id,
            operation,
        })?;
        self.expect_ack(request_id)?.ok_or_else(|| {
            AppError::SupervisedRun("parent acknowledged a record without ledger data".into())
        })
    }

    fn stage_terminal(&self, operation: RecordOperation) -> AppResult<()> {
        let _transaction = self.transaction.lock().expect("child RPC lock poisoned");
        let request_id = self.next_request_id();
        self.send(&ChildMessage::Record {
            request_id,
            operation,
        })?;
        match self.expect_ack(request_id)? {
            None => Ok(()),
            Some(_) => Err(AppError::SupervisedRun(
                "parent published terminal intent before child cleanup".into(),
            )),
        }
    }

    fn assistant_delta(&self, delta: AssistantDeltaEvent) -> AppResult<()> {
        let _transaction = self.transaction.lock().expect("child RPC lock poisoned");
        let request_id = self.next_request_id();
        self.send(&ChildMessage::AssistantDelta { request_id, delta })?;
        self.expect_ack(request_id).map(|_| ())
    }

    fn approval(&self, request: ApprovalRequest) -> AppResult<ExternalApprovalOutcome> {
        let _transaction = self.transaction.lock().expect("child RPC lock poisoned");
        let request_id = self.next_request_id();
        self.send(&ChildMessage::Approval {
            request_id,
            request,
        })?;
        match self.next_reply(request_id)? {
            ParentMessage::Approval { outcome, .. } => match outcome {
                ApprovalReply::Granted { actor } => Ok(ExternalApprovalOutcome::Granted { actor }),
                ApprovalReply::Denied { actor, reason } => {
                    Ok(ExternalApprovalOutcome::Denied { actor, reason })
                }
            },
            _ => Err(AppError::SupervisedRun(
                "parent sent a non-approval reply to an approval request".into(),
            )),
        }
    }

    fn thread_spawn(
        &self,
        input: ThreadSpawnToolInput,
        approving_actor: String,
    ) -> AppResult<ThreadSpawnToolOutput> {
        let _transaction = self.transaction.lock().expect("child RPC lock poisoned");
        let request_id = self.next_request_id();
        self.send(&ChildMessage::ThreadSpawn {
            request_id,
            input,
            approving_actor,
        })?;
        match self.next_reply(request_id)? {
            ParentMessage::ThreadSpawn { output, .. } => Ok(output),
            _ => Err(AppError::SupervisedRun(
                "parent sent a non-spawn reply to a thread.spawn request".into(),
            )),
        }
    }

    fn result(&self, result: ChildRunResult) -> AppResult<()> {
        let _transaction = self.transaction.lock().expect("child RPC lock poisoned");
        let request_id = self.next_request_id();
        self.send(&ChildMessage::Result { request_id, result })?;
        self.expect_ack(request_id).map(|_| ())
    }

    fn send(&self, message: &ChildMessage) -> AppResult<()> {
        let mut writer = self.writer.lock().expect("child stdout lock poisoned");
        serde_json::to_writer(&mut *writer, message)?;
        writer.write_all(b"\n")?;
        writer.flush()?;
        Ok(())
    }

    fn expect_ack(&self, request_id: u64) -> AppResult<Option<RecordedEvent>> {
        match self.next_reply(request_id)? {
            ParentMessage::Ack { record, .. } => Ok(record),
            _ => Err(AppError::SupervisedRun(
                "parent sent a non-acknowledgment transport reply".into(),
            )),
        }
    }

    fn next_reply(&self, request_id: u64) -> AppResult<ParentMessage> {
        let reply = self
            .replies
            .lock()
            .expect("child reply lock poisoned")
            .recv()
            .map_err(|_| AppError::SupervisedRun("parent reply stream closed".into()))?
            .map_err(AppError::SupervisedRun)?;
        match &reply {
            ParentMessage::Ack {
                request_id: reply_id,
                ..
            }
            | ParentMessage::Reject {
                request_id: reply_id,
                ..
            }
            | ParentMessage::Approval {
                request_id: reply_id,
                ..
            }
            | ParentMessage::ThreadSpawn {
                request_id: reply_id,
                ..
            } if *reply_id == request_id => {}
            ParentMessage::Reject { error, .. } => {
                return Err(AppError::SupervisedRun(error.clone()));
            }
            _ => {
                return Err(AppError::SupervisedRun(format!(
                    "parent reply did not match child request {request_id}"
                )));
            }
        }
        if let ParentMessage::Reject { error, .. } = &reply {
            return Err(AppError::SupervisedRun(error.clone()));
        }
        Ok(reply)
    }

    fn next_request_id(&self) -> u64 {
        self.next_request_id.fetch_add(1, Ordering::SeqCst)
    }
}

struct ChildTransportRecorder {
    rpc: Arc<ChildRpc>,
    delta_drain: AssistantDeltaDrain,
    next_seq: u64,
}

impl RunEventRecorder for ChildTransportRecorder {
    fn record(&mut self, event: HarnessEvent) -> AppResult<RecordedEvent> {
        let record = send_record_after_delta_drain(&mut self.delta_drain, event, |operation| {
            self.rpc.record(operation)
        })?;
        if record.seq != self.next_seq {
            return Err(AppError::SupervisedRun(format!(
                "parent record sequence mismatch: expected {}, got {}",
                self.next_seq, record.seq
            )));
        }
        self.next_seq += 1;
        Ok(record)
    }

    fn finish_run(&mut self, run_id: &RunId, final_answer: &str) -> AppResult<RecordedEvent> {
        self.stage_terminal(RecordOperation::Finish {
            run_id: run_id.clone(),
            final_answer: final_answer.into(),
        })
    }

    fn fail_run(
        &mut self,
        run_id: &RunId,
        error: &str,
        canceled: bool,
    ) -> AppResult<RecordedEvent> {
        self.stage_terminal(RecordOperation::Fail {
            run_id: run_id.clone(),
            error: error.into(),
            canceled,
        })
    }
}

struct AssistantDeltaDrain {
    receiver: mpsc::Receiver<AssistantDeltaEvent>,
}

impl AssistantDeltaDrain {
    fn new(receiver: mpsc::Receiver<AssistantDeltaEvent>) -> Self {
        Self { receiver }
    }

    fn before_record(&mut self, event: &HarnessEvent) -> AppResult<()> {
        let HarnessEvent::ModelResponded {
            run_id,
            turn_id,
            step,
            output,
            ..
        } = event
        else {
            return Ok(());
        };
        if output.content.is_empty() {
            return Ok(());
        }

        let deadline = Instant::now() + OUTPUT_DRAIN_TIMEOUT;
        let mut next_delta_index = 0;
        let mut acknowledged_text = String::new();
        while acknowledged_text != output.content {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(AppError::SupervisedRun(
                    "run child assistant delta drain exceeded its deadline".into(),
                ));
            }
            let delta = match self.receiver.recv_timeout(remaining) {
                Ok(delta) => delta,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    return Err(AppError::SupervisedRun(
                        "run child assistant delta drain exceeded its deadline".into(),
                    ));
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(AppError::SupervisedRun(
                        "run child assistant delta forwarder closed before model response".into(),
                    ));
                }
            };
            if delta.run_id != *run_id
                || delta.turn_id != *turn_id
                || delta.step != *step
                || delta.delta_index != next_delta_index
            {
                return Err(AppError::SupervisedRun(
                    "run child assistant delta sequence did not match model response".into(),
                ));
            }
            acknowledged_text.push_str(&delta.text);
            if !output.content.starts_with(&acknowledged_text) {
                return Err(AppError::SupervisedRun(
                    "run child assistant delta text did not match model response".into(),
                ));
            }
            next_delta_index += 1;
        }
        Ok(())
    }
}

fn send_record_after_delta_drain<T>(
    delta_drain: &mut AssistantDeltaDrain,
    event: HarnessEvent,
    send_record: impl FnOnce(RecordOperation) -> AppResult<T>,
) -> AppResult<T> {
    delta_drain.before_record(&event)?;
    send_record(RecordOperation::Event { event })
}

fn forward_child_events(
    event_receiver: mpsc::Receiver<RunEvent>,
    mut forward_delta: impl FnMut(AssistantDeltaEvent) -> AppResult<()>,
    delta_ack_sender: mpsc::Sender<AssistantDeltaEvent>,
) -> AppResult<()> {
    for event in event_receiver {
        match event {
            RunEvent::Ledger(_) => {}
            RunEvent::AssistantDelta(delta) => {
                forward_delta(delta.clone())?;
                delta_ack_sender.send(delta).map_err(|_| {
                    AppError::SupervisedRun("run child assistant delta drain closed".into())
                })?;
            }
        }
    }
    Ok(())
}

impl ChildTransportRecorder {
    fn stage_terminal(&mut self, operation: RecordOperation) -> AppResult<RecordedEvent> {
        self.rpc.stage_terminal(operation.clone())?;
        // The run driver requires a return value, but child-side ledger events are discarded;
        // the parent creates the durable record only after supervised cleanup.
        let record = RecordedEvent {
            seq: self.next_seq,
            occurred_at_ms: 0,
            event: operation.event(),
        };
        self.next_seq += 1;
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[cfg(unix)]
    use crate::{RunLedger, RunOptions, RunSession, app::prepare_run, ledger::SqliteLedger};
    #[cfg(target_os = "linux")]
    use platonic_core::{EffectClass, ToolCallId};
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    #[cfg(unix)]
    use std::sync::mpsc;

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
    fn approval_actor_transport_preserves_named_principals() {
        let reply = ApprovalReply::Granted {
            actor: "jerome".into(),
        };
        let encoded = serde_json::to_string(&reply).unwrap();
        let decoded: ApprovalReply = serde_json::from_str(&encoded).unwrap();
        assert!(matches!(
            decoded,
            ApprovalReply::Granted { actor } if actor == "jerome"
        ));
    }

    #[test]
    fn thread_spawn_rpc_preserves_input_actor_and_typed_result() {
        let child = ChildMessage::ThreadSpawn {
            request_id: 7,
            input: ThreadSpawnToolInput {
                agent_id: "worker".into(),
                cwd: "/tmp/workspace".into(),
                model: None,
                reasoning_effort: None,
                approval_policy: None,
                toolset: Some(vec!["file.read".into()]),
            },
            approving_actor: "daemon".into(),
        };
        let encoded = serde_json::to_string(&child).unwrap();
        match serde_json::from_str::<ChildMessage>(&encoded).unwrap() {
            ChildMessage::ThreadSpawn {
                request_id,
                input,
                approving_actor,
            } => {
                assert_eq!(request_id, 7);
                assert_eq!(input.agent_id, "worker");
                assert_eq!(input.toolset.unwrap(), ["file.read"]);
                assert_eq!(approving_actor, "daemon");
            }
            message => panic!("unexpected child RPC message: {message:?}"),
        }

        let parent = ParentMessage::ThreadSpawn {
            request_id: 7,
            output: ThreadSpawnToolOutput::Spawned {
                thread_id: "thread_worker".into(),
            },
        };
        let encoded = serde_json::to_string(&parent).unwrap();
        match serde_json::from_str::<ParentMessage>(&encoded).unwrap() {
            ParentMessage::ThreadSpawn { request_id, output } => {
                assert_eq!(request_id, 7);
                assert_eq!(
                    output,
                    ThreadSpawnToolOutput::Spawned {
                        thread_id: "thread_worker".into()
                    }
                );
            }
            message => panic!("unexpected parent RPC message: {message:?}"),
        }
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
        #[cfg(windows)]
        assert_eq!(name, "platonic.exe");
        #[cfg(not(windows))]
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

    #[test]
    fn assistant_deltas_are_parent_acknowledged_before_model_responded() {
        #[derive(Debug, Eq, PartialEq)]
        enum ParentVisible {
            Delta(u64),
            ModelResponded,
        }

        let run_id = RunId::new("run_delta_drain").unwrap();
        let turn_id = platonic_core::TurnId::new("turn_delta_drain").unwrap();
        let delta = |delta_index, text: &str| {
            RunEvent::AssistantDelta(AssistantDeltaEvent {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                step: 0,
                delta_index,
                text: text.into(),
            })
        };
        let (event_sender, event_receiver) = std::sync::mpsc::channel();
        event_sender.send(delta(0, "first ")).unwrap();
        event_sender.send(delta(1, "answer")).unwrap();
        drop(event_sender);

        let (delta_ack_sender, delta_ack_receiver) = std::sync::mpsc::channel();
        let (parent_visible_sender, parent_visible_receiver) = std::sync::mpsc::channel();
        let (parent_ack_sender, parent_ack_receiver) = std::sync::mpsc::channel();
        let forwarder_visible_sender = parent_visible_sender.clone();
        let forwarder = thread::spawn(move || {
            forward_child_events(
                event_receiver,
                |delta| {
                    forwarder_visible_sender
                        .send(ParentVisible::Delta(delta.delta_index))
                        .unwrap();
                    parent_ack_receiver.recv().map_err(|_| {
                        AppError::SupervisedRun("test parent acknowledgment closed".into())
                    })
                },
                delta_ack_sender,
            )
        });

        assert_eq!(
            parent_visible_receiver
                .recv_timeout(OUTPUT_DRAIN_TIMEOUT)
                .unwrap(),
            ParentVisible::Delta(0)
        );
        let model_responded = HarnessEvent::ModelResponded {
            run_id,
            turn_id,
            step: 0,
            output: platonic_core::Message {
                role: platonic_core::MessageRole::Assistant,
                content: "first answer".into(),
            },
            proposed_calls: vec![],
            served_model: None,
            usage: None,
        };
        let (record_started_sender, record_started_receiver) = std::sync::mpsc::channel();
        let recorder = thread::spawn(move || {
            let mut delta_drain = AssistantDeltaDrain::new(delta_ack_receiver);
            record_started_sender.send(()).unwrap();
            send_record_after_delta_drain(&mut delta_drain, model_responded, |operation| {
                assert!(matches!(
                    operation,
                    RecordOperation::Event {
                        event: HarnessEvent::ModelResponded { .. }
                    }
                ));
                parent_visible_sender
                    .send(ParentVisible::ModelResponded)
                    .unwrap();
                Ok(())
            })
        });

        record_started_receiver
            .recv_timeout(OUTPUT_DRAIN_TIMEOUT)
            .unwrap();
        parent_ack_sender.send(()).unwrap();
        assert_eq!(
            parent_visible_receiver
                .recv_timeout(OUTPUT_DRAIN_TIMEOUT)
                .unwrap(),
            ParentVisible::Delta(1)
        );
        parent_ack_sender.send(()).unwrap();

        assert_eq!(
            parent_visible_receiver
                .recv_timeout(OUTPUT_DRAIN_TIMEOUT)
                .unwrap(),
            ParentVisible::ModelResponded
        );
        recorder.join().unwrap().unwrap();
        forwarder.join().unwrap().unwrap();
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
                        event: HarnessEvent::RunStarted {
                            run_id: run_id.clone(),
                            agent_id: platonic_core::AgentId::new("plato").unwrap(),
                        },
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
                        event: HarnessEvent::RunStarted {
                            run_id: healthy_run_id.clone(),
                            agent_id: platonic_core::AgentId::new("plato").unwrap(),
                        },
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

    #[cfg(unix)]
    fn publish_for_test(completion: SupervisedRunCompletion) -> AppResult<RunOutcome> {
        let (outcome, terminal) = completion.publish();
        terminal.unwrap();
        outcome
    }
}
