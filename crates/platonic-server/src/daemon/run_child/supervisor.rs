use super::messages::{
    ApprovalReply, ChildMessage, ChildRunResult, ParentMessage, RecordOperation,
};
use crate::tool_catalog::{COMPUTER_OBSERVE, COMPUTER_WINDOWS};
use crate::{
    AppError, AppResult, ApprovalMode, RunEvent, RunOutcome,
    app::{ExternalApprovalOutcome, PreparedRun},
    daemon::child_process::ProcessTreeChild,
    ledger::{EventRecorder, RUN_CANCELED_REASON},
    tools::{
        LogicalReadRequest, LogicalReadToolHandler, ParentAnswerToolHandler, ParentAnswerToolInput,
        RunToolHandlers, ThreadReturnToolHandler, ThreadReturnToolInput,
    },
};
use platonic_core::{ActorId, HarnessEvent, RecordedEvent, RunId, ToolCallId};
use std::{
    collections::HashMap,
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

struct ActiveCredentialGrant {
    call_id: ToolCallId,
    credential_id: String,
    actor_id: ActorId,
    path: PathBuf,
    approval_recorded: bool,
    grant_recorded: bool,
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

fn materialize_credential_grant(
    request: &crate::ApprovalRequest,
    actor: &str,
    sources: &HashMap<String, PathBuf>,
    confinement: &crate::confinement::ChildConfinement,
) -> AppResult<ActiveCredentialGrant> {
    let credential_id = request
        .credential_id
        .as_deref()
        .ok_or_else(|| AppError::SupervisedRun("credential grant omitted its identity".into()))?;
    if request.tool_name != crate::tool_catalog::SHELL_EXEC
        || request.effect != platonic_core::EffectClass::ExternalSideEffect
        || request.yolo_eligible
        || !crate::config::valid_credential_id(credential_id)
    {
        return Err(AppError::SupervisedRun(
            "credential grant requires one explicitly approved shell.exec call".into(),
        ));
    }
    let source = sources.get(credential_id).ok_or_else(|| {
        AppError::SupervisedRun(format!("credential {credential_id} is not configured"))
    })?;
    let actor_id = ActorId::new(actor.to_owned())?;
    let scratch = match confinement {
        crate::confinement::ChildConfinement::Landlock {
            writable_paths,
            scratch,
            ..
        } if writable_paths.iter().any(|path| path == scratch) => scratch,
        _ => {
            return Err(AppError::SupervisedRun(format!(
                "credential {credential_id} requires confined server scratch"
            )));
        }
    };
    let root = scratch.join(crate::tools::CREDENTIAL_GRANTS_DIR);
    match fs::symlink_metadata(&root) {
        Ok(metadata) if metadata.file_type().is_dir() => set_private_directory(&root)?,
        Ok(_) => {
            return Err(AppError::SupervisedRun(format!(
                "credential {credential_id} scratch is unavailable"
            )));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            crate::thread_repository::create_private_directory(&root)?;
        }
        Err(_) => {
            return Err(AppError::SupervisedRun(format!(
                "credential {credential_id} scratch is unavailable"
            )));
        }
    }
    let path = crate::tools::credential_grant_path(scratch, credential_id);
    if fs::symlink_metadata(&path).is_ok() {
        return Err(AppError::SupervisedRun(format!(
            "credential {credential_id} scratch path is occupied"
        )));
    }
    if copy_credential_tree(source, &path).is_err() {
        remove_credential_path(&path).map_err(|_| {
            AppError::SupervisedRun(format!(
                "credential {credential_id} materialization cleanup failed"
            ))
        })?;
        return Err(AppError::SupervisedRun(format!(
            "credential {credential_id} materialization failed"
        )));
    }
    Ok(ActiveCredentialGrant {
        call_id: request.call_id.clone(),
        credential_id: credential_id.into(),
        actor_id,
        path,
        approval_recorded: false,
        grant_recorded: false,
    })
}

fn copy_credential_tree(source: &Path, destination: &Path) -> io::Result<()> {
    let metadata = fs::symlink_metadata(source)?;
    if metadata.file_type().is_symlink() {
        return Err(io::Error::other(
            "credential source contains a symbolic link",
        ));
    }
    if metadata.file_type().is_file() {
        fs::copy(source, destination)?;
        return set_private_file(destination);
    }
    if !metadata.file_type().is_dir() {
        return Err(io::Error::other("credential source is not file-backed"));
    }
    fs::create_dir(destination)?;
    set_private_directory(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        copy_credential_tree(&entry.path(), &destination.join(entry.file_name()))?;
    }
    Ok(())
}

fn set_private_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn set_private_file(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn remove_credential_path(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => fs::remove_dir_all(path),
        Ok(_) => fs::remove_file(path),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn clear_stale_credential_grants(
    confinement: &crate::confinement::ChildConfinement,
) -> AppResult<()> {
    let crate::confinement::ChildConfinement::Landlock { scratch, .. } = confinement else {
        return Ok(());
    };
    remove_credential_path(&scratch.join(crate::tools::CREDENTIAL_GRANTS_DIR))
        .map_err(|_| AppError::SupervisedRun("stale credential grant cleanup failed".into()))
}

fn apply_child_record(
    recorder: &mut ParentRunRecorder,
    active: &mut Option<ActiveCredentialGrant>,
    operation: RecordOperation,
) -> AppResult<Option<RecordedEvent>> {
    let mut recorded_approval = false;
    let mut recorded_grant = false;
    let mut recorded_revocation = false;
    match (&operation, active.as_ref()) {
        (RecordOperation::Event { event }, Some(grant)) if !grant.approval_recorded => {
            match event {
                HarnessEvent::ApprovalGranted {
                    call_id, actor_id, ..
                } if call_id == &grant.call_id && actor_id == &grant.actor_id => {
                    recorded_approval = true;
                }
                _ => {
                    return Err(AppError::SupervisedRun(
                        "credential materialization was not followed by its approval record".into(),
                    ));
                }
            }
        }
        (RecordOperation::Event { event }, Some(grant)) if !grant.grant_recorded => match event {
            HarnessEvent::CredentialGranted {
                call_id,
                credential_id,
                ..
            } if call_id == &grant.call_id && credential_id == &grant.credential_id => {
                recorded_grant = true;
            }
            _ => {
                return Err(AppError::SupervisedRun(
                    "approved credential materialization was not followed by its grant record"
                        .into(),
                ));
            }
        },
        (
            RecordOperation::Event {
                event:
                    HarnessEvent::CredentialRevoked {
                        call_id,
                        credential_id,
                        ..
                    },
            },
            Some(grant),
        ) if call_id == &grant.call_id && credential_id == &grant.credential_id => {
            remove_credential_path(&grant.path).map_err(|_| {
                AppError::SupervisedRun(format!("credential {credential_id} revocation failed"))
            })?;
            recorded_revocation = true;
        }
        (
            RecordOperation::Event {
                event:
                    HarnessEvent::CredentialGranted { .. } | HarnessEvent::CredentialRevoked { .. },
            },
            _,
        ) => {
            return Err(AppError::SupervisedRun(
                "run child emitted an unmaterialized credential lifecycle record".into(),
            ));
        }
        (RecordOperation::Finish { .. } | RecordOperation::Fail { .. }, Some(_)) => {
            return Err(AppError::SupervisedRun(
                "run child emitted terminal intent before credential revocation".into(),
            ));
        }
        _ => {}
    }

    let record = recorder.apply(operation)?;
    if let Some(grant) = active.as_mut() {
        grant.approval_recorded |= recorded_approval;
        grant.grant_recorded |= recorded_grant;
    }
    if recorded_revocation {
        *active = None;
    }
    Ok(record)
}

fn revoke_after_child_exit(
    recorder: &mut ParentRunRecorder,
    run_id: &RunId,
    active: &mut Option<ActiveCredentialGrant>,
) -> AppResult<()> {
    let Some(grant) = active.as_mut() else {
        return Ok(());
    };
    remove_credential_path(&grant.path).map_err(|_| {
        AppError::SupervisedRun(format!(
            "credential {} revocation failed",
            grant.credential_id
        ))
    })?;
    if !grant.approval_recorded {
        recorder.apply(RecordOperation::Event {
            event: HarnessEvent::ApprovalGranted {
                run_id: run_id.clone(),
                call_id: grant.call_id.clone(),
                actor_id: grant.actor_id.clone(),
            },
        })?;
        grant.approval_recorded = true;
    }
    if !grant.grant_recorded {
        recorder.apply(RecordOperation::Event {
            event: HarnessEvent::CredentialGranted {
                run_id: run_id.clone(),
                call_id: grant.call_id.clone(),
                credential_id: grant.credential_id.clone(),
            },
        })?;
        grant.grant_recorded = true;
    }
    recorder.apply(RecordOperation::Event {
        event: HarnessEvent::CredentialRevoked {
            run_id: run_id.clone(),
            call_id: grant.call_id.clone(),
            credential_id: grant.credential_id.clone(),
        },
    })?;
    *active = None;
    Ok(())
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

#[allow(clippy::too_many_arguments)]
pub(in crate::daemon) fn run_supervised(
    prepared: PreparedRun,
    recorder: EventRecorder,
    approval_mode: ApprovalMode,
    event_sender: mpsc::Sender<RunEvent>,
    cancel: Arc<AtomicBool>,
    handlers: RunToolHandlers,
    confinement: crate::confinement::ChildConfinement,
    credential_sources: Arc<HashMap<String, PathBuf>>,
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
        handlers,
        credential_sources,
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
    thread_spawn: Option<crate::tools::ThreadSpawnToolHandler>,
    launch: SupervisedTestLaunch,
) -> SupervisedRunCompletion {
    run_supervised_with_limits(
        prepared,
        recorder,
        approval_mode,
        event_sender,
        cancel,
        RunToolHandlers {
            thread_spawn,
            logical_read: None,
            thread_return: None,
            parent_answer: None,
        },
        Arc::new(HashMap::new()),
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

#[allow(clippy::too_many_arguments)]
fn run_supervised_with_limits(
    prepared: PreparedRun,
    recorder: EventRecorder,
    approval_mode: ApprovalMode,
    event_sender: mpsc::Sender<RunEvent>,
    cancel: Arc<AtomicBool>,
    handlers: RunToolHandlers,
    credential_sources: Arc<HashMap<String, PathBuf>>,
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
    let RunToolHandlers {
        thread_spawn,
        logical_read,
        thread_return,
        parent_answer,
    } = handlers;
    let run_id = prepared.run_id().clone();
    let mut recorder = ParentRunRecorder {
        recorder,
        event_sender,
        terminal: None,
    };
    let mut active_credential_grant = None;
    if let Err(error) = clear_stale_credential_grants(&confinement) {
        return recorder.complete(&run_id, Err(error), false);
    }
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
                        let record = apply_child_record(
                            &mut recorder,
                            &mut active_credential_grant,
                            operation,
                        )?;
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
                        if active_credential_grant.is_some() {
                            return Err(AppError::SupervisedRun(
                                "run child requested approval while a credential grant was active"
                                    .into(),
                            ));
                        }
                        let materialization_request = request.clone();
                        let outcome = approval_mode.decide_external(request)?;
                        let outcome = match outcome {
                            ExternalApprovalOutcome::Granted { actor, explicit } => {
                                if materialization_request.credential_id.is_some() {
                                    if !explicit {
                                        return Err(AppError::SupervisedRun(
                                            "credential grant requires one explicit allow-once decision"
                                                .into(),
                                        ));
                                    }
                                    active_credential_grant = Some(materialize_credential_grant(
                                        &materialization_request,
                                        &actor,
                                        &credential_sources,
                                        &confinement,
                                    )?);
                                }
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
                    ChildMessage::LogicalRead {
                        request_id,
                        request,
                    } => reply_to_logical_read(
                        &mut child,
                        logical_read.as_ref(),
                        request_id,
                        request,
                    )?,
                    ChildMessage::ThreadReturn {
                        request_id,
                        call_id,
                        input,
                    } => reply_to_thread_return(
                        &mut child,
                        thread_return.as_ref(),
                        request_id,
                        call_id,
                        input,
                    )?,
                    ChildMessage::ParentAnswer {
                        request_id,
                        call_id,
                        input,
                    } => reply_to_parent_answer(
                        &mut child,
                        parent_answer.as_ref(),
                        request_id,
                        call_id,
                        input,
                    )?,
                    ChildMessage::Result {
                        request_id,
                        result: child_result,
                    } => {
                        if active_credential_grant.is_some() {
                            return Err(AppError::SupervisedRun(
                                "run child returned before credential revocation".into(),
                            ));
                        }
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

    let result = match result {
        Ok(result) => Ok(result),
        Err(error) => Err(child.cleanup_failure(error)),
    };
    let credential_was_active = active_credential_grant.is_some();
    if let Err(error) =
        revoke_after_child_exit(&mut recorder, &run_id, &mut active_credential_grant)
    {
        return recorder.complete(&run_id, Err(error), false);
    }
    let result = if credential_was_active && result.is_ok() {
        Err(AppError::SupervisedRun(
            "run child exited before credential revocation".into(),
        ))
    } else {
        result
    };

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
        Err(error) => recorder.complete(&run_id, Err(error), false),
    }
}

fn reply_to_logical_read(
    child: &mut SupervisedChild,
    handler: Option<&LogicalReadToolHandler>,
    request_id: u64,
    request: LogicalReadRequest,
) -> AppResult<()> {
    let reply = match handler {
        Some(handler) => match handler.execute(request) {
            Ok(output) => ParentMessage::LogicalRead { request_id, output },
            Err(error) => ParentMessage::Reject {
                request_id,
                error: error.to_string(),
            },
        },
        None => ParentMessage::Reject {
            request_id,
            error: "profile reads require a profile thread".into(),
        },
    };
    child.write(&reply)
}

fn reply_to_thread_return(
    child: &mut SupervisedChild,
    handler: Option<&ThreadReturnToolHandler>,
    request_id: u64,
    call_id: platonic_core::ToolCallId,
    input: ThreadReturnToolInput,
) -> AppResult<()> {
    let reply = match handler {
        Some(handler) => match handler.execute(input, call_id) {
            Ok(output) => ParentMessage::ThreadReturn { request_id, output },
            Err(error) => ParentMessage::Reject {
                request_id,
                error: error.to_string(),
            },
        },
        None => ParentMessage::Reject {
            request_id,
            error: "thread.return requires an admitted child thread".into(),
        },
    };
    child.write(&reply)
}

fn reply_to_parent_answer(
    child: &mut SupervisedChild,
    handler: Option<&ParentAnswerToolHandler>,
    request_id: u64,
    call_id: platonic_core::ToolCallId,
    input: ParentAnswerToolInput,
) -> AppResult<()> {
    let reply = match handler {
        Some(handler) => match handler.execute(input, call_id) {
            Ok(output) => ParentMessage::ParentAnswer { request_id, output },
            Err(error) => ParentMessage::Reject {
                request_id,
                error: error.to_string(),
            },
        },
        None => ParentMessage::Reject {
            request_id,
            error: "thread.answer requires an admitted profile thread".into(),
        },
    };
    child.write(&reply)
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

    #[cfg(target_os = "linux")]
    fn credential_request(run_id: &RunId, call_id: &ToolCallId) -> ApprovalRequest {
        ApprovalRequest {
            run_id: run_id.clone(),
            call_id: call_id.clone(),
            tool_name: crate::tool_catalog::SHELL_EXEC.into(),
            effect: EffectClass::ExternalSideEffect,
            reason: "shell.exec requires explicit local approval".into(),
            input_preview: Some(
                r#"{"command":"test -r $TMPDIR/credentials/github/token","credential":"github"}"#
                    .into(),
            ),
            approval_preview: Some(
                "credential: github\ncredential path: $TMPDIR/credentials/github".into(),
            ),
            diff_preview: None,
            yolo_eligible: false,
            credential_id: Some("github".into()),
        }
    }

    #[cfg(target_os = "linux")]
    fn record_until_approval(recorder: &mut EventRecorder, run_id: &RunId, call_id: &ToolCallId) {
        let turn_id = platonic_core::TurnId::new("turn_credential").unwrap();
        let tool = platonic_core::ToolName::new(crate::tool_catalog::SHELL_EXEC).unwrap();
        let input = serde_json::json!({
            "command": "test -r $TMPDIR/credentials/github/token",
            "credential": "github"
        });
        let proposal = platonic_core::ToolProposal {
            tool: tool.clone(),
            input: input.clone(),
        };
        for event in [
            HarnessEvent::RunStarted(platonic_core::RunStartedEvent {
                run_id: run_id.clone(),
                identity: platonic_core::RunIdentity::LegacyAgent {
                    agent_id: platonic_core::AgentId::new("plato").unwrap(),
                },
            }),
            HarnessEvent::ContextBuilt {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                context: platonic_core::ContextPack {
                    token_budget: 1,
                    fragments: vec![],
                },
            },
            HarnessEvent::ModelRequested {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                step: 0,
                model: platonic_core::ModelName::new("test-model").unwrap(),
            },
            HarnessEvent::ModelResponded {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                step: 0,
                output: platonic_core::Message {
                    role: platonic_core::MessageRole::Assistant,
                    content: String::new(),
                },
                proposed_calls: vec![proposal],
                served_model: None,
                usage: None,
            },
            HarnessEvent::ToolCallProposed {
                run_id: run_id.clone(),
                turn_id,
                call: platonic_core::ToolCall {
                    id: call_id.clone(),
                    tool,
                    effect: EffectClass::ExternalSideEffect,
                    input,
                },
            },
            HarnessEvent::PolicyEvaluated {
                run_id: run_id.clone(),
                call_id: call_id.clone(),
                decision: platonic_core::PolicyDecision::RequireApproval {
                    reason: "shell.exec requires explicit local approval".into(),
                },
            },
        ] {
            recorder.record(event).unwrap();
        }
    }

    #[cfg(target_os = "linux")]
    fn materialized_grant(
        root: &Path,
        run_id: &RunId,
        call_id: &ToolCallId,
    ) -> (ActiveCredentialGrant, PathBuf, PathBuf) {
        let source = root.join("host-source");
        let scratch = root.join("scratch");
        fs::create_dir(&source).unwrap();
        fs::write(source.join("token"), "credential-value-sentinel").unwrap();
        crate::thread_repository::create_private_directory(&scratch).unwrap();
        let sources = HashMap::from([("github".into(), source.clone())]);
        let confinement = crate::confinement::ChildConfinement::Landlock {
            readable_paths: vec![scratch.clone()],
            writable_paths: vec![scratch.clone()],
            scratch: scratch.clone(),
        };
        let grant = materialize_credential_grant(
            &credential_request(run_id, call_id),
            "operator",
            &sources,
            &confinement,
        )
        .unwrap();
        (grant, source, scratch)
    }

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

    #[cfg(target_os = "linux")]
    #[test]
    fn credential_materialization_is_private_atomic_on_failure_and_non_exposing() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let root = tempfile::tempdir().unwrap();
        let run_id = RunId::new("run_credential_materialize").unwrap();
        let call_id = ToolCallId::new("call_credential_materialize").unwrap();
        let source = root.path().join("host-source");
        let scratch = root.path().join("scratch");
        fs::create_dir(&source).unwrap();
        fs::write(source.join("token"), "credential-value-sentinel").unwrap();
        crate::thread_repository::create_private_directory(&scratch).unwrap();
        let sources = HashMap::from([("github".into(), source.clone())]);
        let confinement = crate::confinement::ChildConfinement::Landlock {
            readable_paths: vec![scratch.clone()],
            writable_paths: vec![scratch.clone()],
            scratch: scratch.clone(),
        };
        let request = credential_request(&run_id, &call_id);
        let serialized = serde_json::to_string(&request).unwrap();
        assert!(!serialized.contains("credential-value-sentinel"));
        assert!(!serialized.contains(source.to_string_lossy().as_ref()));

        assert!(materialize_credential_grant(&request, "  ", &sources, &confinement).is_err());
        assert!(!scratch.join(crate::tools::CREDENTIAL_GRANTS_DIR).exists());

        let grant =
            materialize_credential_grant(&request, "operator", &sources, &confinement).unwrap();
        assert_eq!(
            fs::read_to_string(grant.path.join("token")).unwrap(),
            "credential-value-sentinel"
        );
        assert_eq!(
            fs::metadata(&grant.path).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(grant.path.join("token"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        remove_credential_path(&grant.path).unwrap();

        symlink(source.join("token"), source.join("linked-token")).unwrap();
        let error = materialize_credential_grant(&request, "operator", &sources, &confinement)
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("materialization failed"));
        assert!(!error.contains("credential-value-sentinel"));
        assert!(!crate::tools::credential_grant_path(&scratch, "github").exists());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn stale_grant_root_is_removed_before_a_noncredential_child_spawns() {
        let root = tempfile::tempdir().unwrap();
        let scratch = root.path().join("scratch");
        let stale = crate::tools::credential_grant_path(&scratch, "github");
        crate::thread_repository::create_private_directory(&stale).unwrap();
        fs::write(stale.join("token"), "prior-run-credential-sentinel").unwrap();
        let confinement = crate::confinement::ChildConfinement::Landlock {
            readable_paths: vec![scratch.clone()],
            writable_paths: vec![scratch.clone()],
            scratch: scratch.clone(),
        };

        clear_stale_credential_grants(&confinement).unwrap();
        let status = Command::new("/bin/sh")
            .arg("-c")
            .arg("test ! -r \"$TMPDIR/credentials/github/token\"")
            .env_clear()
            .env("TMPDIR", scratch)
            .status()
            .unwrap();

        assert!(status.success());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn credential_revocation_precedes_success_failure_and_cancellation_terminals() {
        for terminal in ["success", "failure", "cancellation"] {
            let root = tempfile::tempdir().unwrap();
            let run_id = RunId::new(format!("run_credential_{terminal}")).unwrap();
            let call_id = ToolCallId::new(format!("call_credential_{terminal}")).unwrap();
            let ledger_path = root.path().join("events.jsonl");
            let mut event_recorder = EventRecorder::create_jsonl(&ledger_path).unwrap();
            record_until_approval(&mut event_recorder, &run_id, &call_id);
            let (grant, _, _) = materialized_grant(root.path(), &run_id, &call_id);
            let grant_path = grant.path.clone();
            let (event_sender, _event_receiver) = mpsc::channel();
            let mut recorder = ParentRunRecorder {
                recorder: event_recorder,
                event_sender,
                terminal: None,
            };
            let mut active = Some(grant);
            for event in [
                HarnessEvent::ApprovalGranted {
                    run_id: run_id.clone(),
                    call_id: call_id.clone(),
                    actor_id: ActorId::new("operator").unwrap(),
                },
                HarnessEvent::CredentialGranted {
                    run_id: run_id.clone(),
                    call_id: call_id.clone(),
                    credential_id: "github".into(),
                },
                HarnessEvent::ToolStarted {
                    run_id: run_id.clone(),
                    call_id: call_id.clone(),
                },
            ] {
                apply_child_record(&mut recorder, &mut active, RecordOperation::Event { event })
                    .unwrap();
            }
            let tool_event = if terminal == "success" {
                HarnessEvent::ToolFinished {
                    run_id: run_id.clone(),
                    result: platonic_core::ToolResult {
                        call_id: call_id.clone(),
                        summary: "done".into(),
                        data: serde_json::json!({"exit_code": 0}),
                        artifacts: vec![],
                        visibility: platonic_core::ResultVisibility::Both,
                    },
                }
            } else {
                HarnessEvent::ToolFailed {
                    run_id: run_id.clone(),
                    call_id: call_id.clone(),
                    reason: terminal.into(),
                }
            };
            apply_child_record(
                &mut recorder,
                &mut active,
                RecordOperation::Event { event: tool_event },
            )
            .unwrap();
            apply_child_record(
                &mut recorder,
                &mut active,
                RecordOperation::Event {
                    event: HarnessEvent::CredentialRevoked {
                        run_id: run_id.clone(),
                        call_id: call_id.clone(),
                        credential_id: "github".into(),
                    },
                },
            )
            .unwrap();
            assert!(!grant_path.exists(), "{terminal} left credential bytes");

            let (operation, outcome) = match terminal {
                "success" => (
                    RecordOperation::Finish {
                        run_id: run_id.clone(),
                        final_answer: "done".into(),
                    },
                    Ok(RunOutcome {
                        run_id: run_id.clone(),
                        final_answer: "done".into(),
                        completion_claim: None,
                    }),
                ),
                "failure" => (
                    RecordOperation::Fail {
                        run_id: run_id.clone(),
                        error: "failure".into(),
                        canceled: false,
                    },
                    Err(AppError::SupervisedRun("failure".into())),
                ),
                _ => (
                    RecordOperation::Fail {
                        run_id: run_id.clone(),
                        error: RUN_CANCELED_REASON.into(),
                        canceled: true,
                    },
                    Err(AppError::RunCanceled),
                ),
            };
            apply_child_record(&mut recorder, &mut active, operation).unwrap();
            let (_, terminal_record) = recorder.complete(&run_id, outcome, true).publish();
            terminal_record.unwrap();
            let records = crate::ledger::read_records(&ledger_path).unwrap();
            assert!(matches!(
                records[records.len() - 2].event,
                HarnessEvent::CredentialRevoked { .. }
            ));
            assert!(matches!(
                records.last().unwrap().event,
                HarnessEvent::RunFinished { .. } | HarnessEvent::RunFailed { .. }
            ));
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn process_exit_removes_credential_before_synthetic_audit_and_terminal() {
        let root = tempfile::tempdir().unwrap();
        let run_id = RunId::new("run_credential_process_exit").unwrap();
        let call_id = ToolCallId::new("call_credential_process_exit").unwrap();
        let ledger_path = root.path().join("events.jsonl");
        let mut event_recorder = EventRecorder::create_jsonl(&ledger_path).unwrap();
        record_until_approval(&mut event_recorder, &run_id, &call_id);
        let (grant, _, _) = materialized_grant(root.path(), &run_id, &call_id);
        let grant_path = grant.path.clone();
        let (event_sender, _event_receiver) = mpsc::channel();
        let mut recorder = ParentRunRecorder {
            recorder: event_recorder,
            event_sender,
            terminal: None,
        };
        let mut active = Some(grant);

        revoke_after_child_exit(&mut recorder, &run_id, &mut active).unwrap();
        assert!(!grant_path.exists());
        let (_, terminal_record) = recorder
            .complete(
                &run_id,
                Err(AppError::SupervisedRun("child exited".into())),
                false,
            )
            .publish();
        terminal_record.unwrap();

        let records = crate::ledger::read_records(&ledger_path).unwrap();
        let names = records
            .iter()
            .rev()
            .take(4)
            .map(|record| record.event.name())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            [
                "run_failed",
                "credential_revoked",
                "credential_granted",
                "approval_granted"
            ]
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn audit_failure_cannot_skip_physical_process_exit_cleanup() {
        let root = tempfile::tempdir().unwrap();
        let run_id = RunId::new("run_credential_audit_failure").unwrap();
        let call_id = ToolCallId::new("call_credential_audit_failure").unwrap();
        let mut event_recorder =
            EventRecorder::create_jsonl(&root.path().join("events.jsonl")).unwrap();
        record_until_approval(&mut event_recorder, &run_id, &call_id);
        let (grant, _, _) = materialized_grant(root.path(), &run_id, &call_id);
        let grant_path = grant.path.clone();
        let (event_sender, event_receiver) = mpsc::channel();
        drop(event_receiver);
        let mut recorder = ParentRunRecorder {
            recorder: event_recorder,
            event_sender,
            terminal: None,
        };
        let mut active = Some(grant);

        assert!(revoke_after_child_exit(&mut recorder, &run_id, &mut active).is_err());
        assert!(!grant_path.exists());
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
                        credential_id: None,
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
                    RunToolHandlers::default(),
                    Arc::new(HashMap::new()),
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
                    RunToolHandlers::default(),
                    Arc::new(HashMap::new()),
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
                    RunToolHandlers::default(),
                    Arc::new(HashMap::new()),
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
            assert!(
                !captured
                    .windows("credential-value-sentinel".len())
                    .any(|window| window == b"credential-value-sentinel"),
                "file credential reached captured child output"
            );
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "launched in an isolated sentinel-rich parent environment"]
    fn supervised_environment_driver_fixture() {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        let scratch = root.path().join("scratch");
        let credential_source = root.path().join("host-credential-source");
        fs::create_dir(&workspace).unwrap();
        crate::thread_repository::create_private_directory(&scratch).unwrap();
        fs::create_dir(&credential_source).unwrap();
        fs::write(credential_source.join("token"), "credential-value-sentinel").unwrap();

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
            workspace_root: workspace.clone(),
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
        assert!(!start_message.contains("credential-value-sentinel"));
        assert!(!start_message.contains(credential_source.to_string_lossy().as_ref()));

        let (event_sender, event_receiver) = mpsc::channel();
        let (ready_sender, ready_receiver) = mpsc::channel();
        let child_workspace = workspace.clone();
        let child_scratch = scratch.clone();
        let child_credential_source = credential_source.clone();
        let supervisor = thread::spawn(move || {
            publish_for_test(run_supervised_with_limits(
                prepared,
                recorder,
                ApprovalMode::external_with_actor("test", |_| {
                    Ok(ExternalApprovalOutcome::Granted {
                        actor: "test".into(),
                        explicit: true,
                    })
                }),
                event_sender,
                Arc::new(AtomicBool::new(false)),
                RunToolHandlers::default(),
                Arc::new(HashMap::from([("github".into(), child_credential_source)])),
                ChildLaunch {
                    limits: ChildLifecycleLimits {
                        deadline: Duration::from_secs(10),
                        ..ChildLifecycleLimits::default()
                    },
                    executable: fixture,
                    ready_child: Some(ready_sender),
                    confinement: crate::confinement::ChildConfinement::Landlock {
                        readable_paths: vec![child_workspace.clone(), child_scratch.clone()],
                        writable_paths: vec![child_workspace, child_scratch.clone()],
                        scratch: child_scratch,
                    },
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
        expected_env.insert(b"TMPDIR".to_vec(), scratch.as_os_str().as_bytes().to_vec());
        expected_env.insert(b"PLATONIC_CHILD_CONFINEMENT".to_vec(), b"landlock".to_vec());
        expected_env.insert(
            b"PLATONIC_CHILD_READABLE_PATHS".to_vec(),
            serde_json::to_vec(&vec![workspace.clone(), scratch.clone()]).unwrap(),
        );
        expected_env.insert(
            b"PLATONIC_CHILD_WRITABLE_PATHS".to_vec(),
            serde_json::to_vec(&vec![workspace, scratch.clone()]).unwrap(),
        );
        expected_env.insert(b"GIT_CONFIG_GLOBAL".to_vec(), b"/dev/null".to_vec());
        expected_env.insert(b"GIT_CONFIG_NOSYSTEM".to_vec(), b"1".to_vec());
        expected_env.insert(
            b"XDG_CONFIG_HOME".to_vec(),
            scratch.join("xdg-config").as_os_str().as_bytes().to_vec(),
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
        assert!(!live.contains("credential-value-sentinel"));
        assert!(!live.contains(credential_source.to_string_lossy().as_ref()));
        let durable = serde_json::to_string(&records).unwrap();
        assert!(durable.contains("runtime-and-scrub-ok"));
        assert!(!durable.contains(RUN_CHILD_PROVIDER_SENTINEL));
        assert!(!durable.contains("credential-value-sentinel"));
        assert!(!durable.contains(credential_source.to_string_lossy().as_ref()));
        assert!(!crate::tools::credential_grant_path(&scratch, "github").exists());
        let outcome = serde_json::to_string(&outcome).unwrap();
        assert!(!outcome.contains(RUN_CHILD_PROVIDER_SENTINEL));
        assert!(!outcome.contains("credential-value-sentinel"));
        assert!(!outcome.contains(credential_source.to_string_lossy().as_ref()));

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
                "test -s \"$TMPDIR/credentials/github/token\" && ",
                "test \"${PLATONIC_CHILD_CONFINEMENT+x}\" != x && ",
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
                "command": shell_command,
                "credential": "github"
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
