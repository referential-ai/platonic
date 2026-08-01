use crate::{
    AppError, AppResult, ApprovalMode, RunEvent, RunLedger, RunOptions, RunOutcome, RunSession,
    daemon::{
        protocol::{
            ApprovalDecideParams, ApprovalDecision, ApprovalDecisionName, CAPABILITIES,
            CommandAcceptedResult, ERROR_DAEMON_SHUTTING_DOWN, ERROR_INTERNAL,
            ERROR_ISSUE_PREP_FAILED, ERROR_LAGGED, ERROR_MALFORMED_REQUEST, ERROR_NOT_FOUND,
            ERROR_OVERLOAD, ERROR_RUN_FAILED, ERROR_SESSIONS_LIST_FAILED, ERROR_UNSUPPORTED_METHOD,
            ERROR_WORKSPACE_MISMATCH, Envelope, EventsStreamParams, EventsStreamResult,
            HelloParams, HelloResult, IssuePrepResult, IssuePrepStartParams, IssuePrepStartResult,
            MessageAppendParams, RunCancelParams, RunStartParams, RunStartResult, RunStateName,
            SessionSummary, SessionsListResult, ShutdownIfIdleResult, ShutdownIfIdleResultName,
            StreamEvent, TranscriptReadParams, TranscriptReadResult, TypedRun, TypedTranscript,
            TypedTranscriptEntry, decode_request,
        },
        runtime::{
            DaemonRuntime, IssuePrepAdmissionError, RunAdmissionError, RunRecord,
            ShutdownIfIdleDecision, approval_handler,
        },
    },
    issue_prep::{IssuePrepOptions, IssuePrepOutcome, run_issue_prep},
    ledger::{SessionRunRecords, SqliteLedger},
    model::RunOverrides,
    new_run_id, new_session_id,
    paths::DefaultSqlitePath,
    replay::{format_readback, format_session_readback},
    run_question,
    tools::ApprovalOutcome,
};
use platonic_core::{ReadbackEntry, RunReadback};
use std::{
    path::PathBuf,
    sync::{Arc, atomic::Ordering, mpsc},
    thread,
};

const DEFAULT_EVENT_LIMIT: usize = 64;
const MAX_EVENT_LIMIT: usize = 128;
const LATEST_QUESTION_MAX_CHARS: usize = 120;
const EVENT_COLLECTOR_PANIC: &str = "daemon event collector panicked";

pub(super) fn handle_line(runtime: &DaemonRuntime, line: &str) -> Envelope {
    match decode_request(line) {
        Ok(request) => handle_request(runtime, request),
        Err(error) => *error,
    }
}

fn handle_request(runtime: &DaemonRuntime, request: Envelope) -> Envelope {
    match request.method.as_deref() {
        Some("hello") => handle_with_params(runtime, request, "hello", handle_hello),
        Some("run.start") => handle_with_params(runtime, request, "run.start", handle_run_start),
        Some("message.append") => {
            handle_with_params(runtime, request, "message.append", handle_message_append)
        }
        Some("issue-prep.start") => handle_with_params(
            runtime,
            request,
            "issue-prep.start",
            handle_issue_prep_start,
        ),
        Some("events.stream") => {
            handle_with_params(runtime, request, "events.stream", handle_events_stream)
        }
        Some("approval.decide") => {
            handle_with_params(runtime, request, "approval.decide", handle_approval_decide)
        }
        Some("run.cancel") => handle_with_params(runtime, request, "run.cancel", handle_run_cancel),
        Some("sessions.list") => handle_sessions_list(runtime, request),
        Some("daemon.shutdown_if_idle") => handle_shutdown_if_idle(runtime, request),
        Some("transcript.read") => {
            handle_with_params(runtime, request, "transcript.read", handle_transcript_read)
        }
        Some(method) => Envelope::error(
            request.id,
            Some(method.into()),
            ERROR_UNSUPPORTED_METHOD,
            format!("unsupported method: {method}"),
        ),
        None => Envelope::error(
            request.id,
            None,
            ERROR_MALFORMED_REQUEST,
            "request method is required",
        ),
    }
}

fn handle_hello(runtime: &DaemonRuntime, request: Envelope, params: HelloParams) -> Envelope {
    if params.workspace_id != runtime.paths.workspace_id {
        return Envelope::error(
            request.id,
            Some("hello".into()),
            ERROR_WORKSPACE_MISMATCH,
            format!(
                "workspace_id mismatch: expected {}, got {}",
                runtime.paths.workspace_id, params.workspace_id
            ),
        );
    }

    match PathBuf::from(&params.workspace_root).canonicalize() {
        Ok(root) if root == runtime.paths.workspace_root => {}
        Ok(root) => {
            return Envelope::error(
                request.id,
                Some("hello".into()),
                ERROR_WORKSPACE_MISMATCH,
                format!(
                    "workspace_root mismatch: expected {}, got {}",
                    runtime.paths.workspace_root.display(),
                    root.display()
                ),
            );
        }
        Err(error) => {
            return Envelope::error(
                request.id,
                Some("hello".into()),
                ERROR_WORKSPACE_MISMATCH,
                format!("workspace_root cannot be resolved: {error}"),
            );
        }
    }

    Envelope::response_from(
        request.id,
        Some("hello".into()),
        HelloResult {
            daemon_version: env!("CARGO_PKG_VERSION").into(),
            workspace_id: runtime.paths.workspace_id.clone(),
            ledger_path: runtime.paths.ledger_path.to_string_lossy().into_owned(),
            capabilities: CAPABILITIES.into_iter().map(str::to_owned).collect(),
        },
    )
}

fn handle_run_start(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: RunStartParams,
) -> Envelope {
    start_run(
        runtime,
        request.id,
        params.question,
        RunSession::Fresh {
            session_id: new_session_id(),
        },
        params.config_path,
        params.overrides,
        params.wait,
    )
}

fn handle_message_append(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: MessageAppendParams,
) -> Envelope {
    if runtime.shutdown_accepted() {
        return shutting_down_response(request.id, "message.append");
    }
    let session_id = match params.session_id {
        Some(session_id) => session_id,
        None => match latest_session_id(runtime) {
            Ok(session_id) => session_id,
            Err(error) => {
                if runtime.shutdown_accepted() {
                    return shutting_down_response(request.id, "message.append");
                }
                return Envelope::error(
                    request.id,
                    Some("message.append".into()),
                    ERROR_NOT_FOUND,
                    error,
                );
            }
        },
    };
    start_run(
        runtime,
        request.id,
        params.message,
        RunSession::Continue { session_id },
        params.config_path,
        params.overrides,
        params.wait,
    )
}

fn handle_issue_prep_start(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: IssuePrepStartParams,
) -> Envelope {
    let _reservation = match runtime.reserve_issue_prep() {
        Ok(reservation) => reservation,
        Err(IssuePrepAdmissionError::ShuttingDown) => {
            return shutting_down_response(request.id, "issue-prep.start");
        }
        Err(IssuePrepAdmissionError::Active) => {
            return Envelope::error(
                request.id,
                Some("issue-prep.start".into()),
                ERROR_OVERLOAD,
                "issue prep is already active",
            );
        }
    };
    let run_id = match new_run_id() {
        Ok(run_id) => run_id,
        Err(error) => {
            return Envelope::error(
                request.id,
                Some("issue-prep.start".into()),
                ERROR_ISSUE_PREP_FAILED,
                error.to_string(),
            );
        }
    };
    let run_dir = runtime
        .paths
        .workspace_root
        .join(".plato")
        .join("issue-prep")
        .join(run_id.to_string());
    let options = IssuePrepOptions {
        workspace_root: runtime.paths.workspace_root.clone(),
        config_path: params.config_path.map(PathBuf::from),
        run_dir: run_dir.clone(),
        input: params.input,
    };
    match run_issue_prep(options) {
        Ok(outcome) => Envelope::response_from(
            request.id,
            Some("issue-prep.start".into()),
            IssuePrepStartResult {
                run_dir: run_dir.to_string_lossy().into_owned(),
                outcome: match outcome {
                    IssuePrepOutcome::Candidate { markdown } => {
                        IssuePrepResult::Candidate { markdown }
                    }
                    IssuePrepOutcome::Blocked { stage, reasons } => {
                        IssuePrepResult::Blocked { stage, reasons }
                    }
                },
            },
        ),
        Err(error) => Envelope::error(
            request.id,
            Some("issue-prep.start".into()),
            ERROR_ISSUE_PREP_FAILED,
            format!("{error}; run directory: {}", run_dir.display()),
        ),
    }
}

fn start_run(
    runtime: &DaemonRuntime,
    request_id: Option<String>,
    question: String,
    session: RunSession,
    config_path: Option<String>,
    overrides: RunOverrides,
    wait: Option<bool>,
) -> Envelope {
    let method = match &session {
        RunSession::Fresh { .. } => "run.start",
        RunSession::Continue { .. } => "message.append",
    };
    let session_id = session.session_id().to_string();
    let run_id = match new_run_id() {
        Ok(run_id) => run_id,
        Err(error) => {
            return Envelope::error(
                request_id,
                Some(method.into()),
                ERROR_RUN_FAILED,
                error.to_string(),
            );
        }
    };
    let run_id_string = run_id.to_string();
    let record = Arc::new(RunRecord::new(
        run_id_string.clone(),
        session_id,
        runtime.paths.ledger_path.clone(),
    ));
    match runtime.reserve_run(record.clone()) {
        Ok(()) => {}
        Err(RunAdmissionError::ShuttingDown) => {
            return shutting_down_response(request_id, method);
        }
        Err(RunAdmissionError::SessionActive { run_id }) => {
            return Envelope::error(
                request_id,
                Some(method.into()),
                ERROR_OVERLOAD,
                format!(
                    "session already has an active run: {} ({run_id})",
                    record.session_id
                ),
            );
        }
    }

    let (event_sender, event_receiver) = mpsc::channel::<RunEvent>();
    let event_collector = spawn_event_collector(record.clone(), event_receiver);
    let options = RunOptions {
        question,
        config_path: config_path.map(PathBuf::from),
        overrides,
        ledger: RunLedger::DefaultSqlite(runtime.paths.default_ledger()),
        workspace_root: runtime.paths.workspace_root.clone(),
        approval_mode: ApprovalMode::external("daemon", approval_handler(record.clone())),
        run_id: Some(run_id),
        session: Some(session),
        event_sender: Some(event_sender),
        stream_to_stderr: false,
        cancel: Some(record.cancel.clone()),
        voice_interruption_context: None,
    };

    if wait.unwrap_or(false) {
        match run_to_completion(runtime, &record, options, event_collector) {
            Ok(_) => run_start_response(request_id, method, &record),
            Err(error) => Envelope::error(
                request_id,
                Some(method.into()),
                ERROR_RUN_FAILED,
                error.to_string(),
            ),
        }
    } else {
        let worker_runtime = runtime.clone();
        let worker_record = record.clone();
        thread::spawn(move || {
            let _ = run_to_completion(&worker_runtime, &worker_record, options, event_collector);
        });
        run_start_response(request_id, method, &record)
    }
}

fn run_to_completion(
    runtime: &DaemonRuntime,
    record: &RunRecord,
    options: RunOptions,
    event_collector: thread::JoinHandle<()>,
) -> AppResult<RunOutcome> {
    let outcome = run_question(options);
    finish_run_after_event_collection(runtime, record, outcome, event_collector)
}

fn finish_run_after_event_collection(
    runtime: &DaemonRuntime,
    record: &RunRecord,
    outcome: AppResult<RunOutcome>,
    event_collector: thread::JoinHandle<()>,
) -> AppResult<RunOutcome> {
    let outcome = match event_collector.join() {
        Ok(()) => outcome,
        Err(_) => Err(AppError::RunFailed(EVENT_COLLECTOR_PANIC.into())),
    };
    match &outcome {
        Ok(outcome) => runtime.finish_run(record, outcome.final_answer.clone()),
        Err(error) => runtime.finish_run_with_error(record, error),
    }
    outcome
}

fn handle_shutdown_if_idle(runtime: &DaemonRuntime, request: Envelope) -> Envelope {
    let valid_params = match request.params.as_ref() {
        None => true,
        Some(serde_json::Value::Object(params)) => params.is_empty(),
        Some(_) => false,
    };
    if !valid_params {
        return Envelope::error(
            request.id,
            request.method,
            ERROR_MALFORMED_REQUEST,
            "daemon.shutdown_if_idle params must be omitted or an empty object",
        );
    }
    match runtime.shutdown_if_idle() {
        ShutdownIfIdleDecision::Shutdown => Envelope::response_from(
            request.id,
            Some("daemon.shutdown_if_idle".into()),
            ShutdownIfIdleResult {
                result: ShutdownIfIdleResultName::Shutdown,
            },
        ),
        ShutdownIfIdleDecision::RefusedActive => Envelope::response_from(
            request.id,
            Some("daemon.shutdown_if_idle".into()),
            ShutdownIfIdleResult {
                result: ShutdownIfIdleResultName::RefusedActive,
            },
        ),
        ShutdownIfIdleDecision::AlreadyShuttingDown => {
            shutting_down_response(request.id, "daemon.shutdown_if_idle")
        }
    }
}

fn shutting_down_response(request_id: Option<String>, method: &'static str) -> Envelope {
    Envelope::error(
        request_id,
        Some(method.into()),
        ERROR_DAEMON_SHUTTING_DOWN,
        "daemon shutdown is already in progress",
    )
}

fn run_start_response(request_id: Option<String>, method: &str, record: &RunRecord) -> Envelope {
    let status = record.status();
    Envelope::response_from(
        request_id,
        Some(method.into()),
        RunStartResult {
            run_id: record.run_id.clone(),
            session_id: record.session_id.clone(),
            ledger_path: record.ledger_path.to_string_lossy().into_owned(),
            status: status.state,
            final_answer: status.final_answer,
        },
    )
}

fn handle_events_stream(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: EventsStreamParams,
) -> Envelope {
    let record = match find_run(runtime, &params.run_id) {
        Ok(record) => record,
        Err(error) => return error_response(request.id, "events.stream", error),
    };
    let limit = params.limit.unwrap_or(DEFAULT_EVENT_LIMIT);
    if limit > MAX_EVENT_LIMIT {
        return Envelope::error(
            request.id,
            Some("events.stream".into()),
            ERROR_OVERLOAD,
            format!("event stream limit exceeds maximum {MAX_EVENT_LIMIT}: {limit}"),
        );
    }
    let result = {
        // Terminal status is published only after collection, so snapshot status before events.
        let status = record.status.lock().expect("run status lock poisoned");
        let buffer = record.events.lock().expect("event buffer lock poisoned");
        #[cfg(test)]
        record.wait_during_event_snapshot();
        let from_offset = params.from_offset.unwrap_or(buffer.next_offset);
        if from_offset < buffer.first_offset {
            return Envelope::error(
                request.id,
                Some("events.stream".into()),
                ERROR_LAGGED,
                format!(
                    "requested offset {from_offset} is no longer buffered; first available is {}",
                    buffer.first_offset
                ),
            );
        }
        let start = (from_offset - buffer.first_offset) as usize;
        let events = buffer
            .events
            .iter()
            .skip(start)
            .take(limit)
            .cloned()
            .collect::<Vec<_>>();
        let next_offset = from_offset + events.len() as u64;
        EventsStreamResult {
            run_id: record.run_id.clone(),
            from_offset,
            next_offset,
            status: status.state,
            events,
        }
    };
    Envelope::response_from(request.id, Some("events.stream".into()), result)
}

fn handle_approval_decide(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: ApprovalDecideParams,
) -> Envelope {
    let record = match find_run(runtime, &params.run_id) {
        Ok(record) => record,
        Err(error) => return error_response(request.id, "approval.decide", error),
    };
    let mut approvals = record.approvals.lock().expect("approvals lock poisoned");
    if record.cancel.load(Ordering::SeqCst) {
        return Envelope::error(
            request.id,
            Some("approval.decide".into()),
            ERROR_NOT_FOUND,
            format!("pending approval not found: {}", params.tool_call_id),
        );
    }
    let pending = match approvals.get_mut(&params.tool_call_id) {
        Some(pending) => pending,
        None => {
            return Envelope::error(
                request.id,
                Some("approval.decide".into()),
                ERROR_NOT_FOUND,
                format!("pending approval not found: {}", params.tool_call_id),
            );
        }
    };
    if pending.decision.is_some() {
        return Envelope::error(
            request.id,
            Some("approval.decide".into()),
            ERROR_NOT_FOUND,
            format!("pending approval not found: {}", params.tool_call_id),
        );
    }
    pending.decision = Some(match params.decision {
        ApprovalDecision::Grant => ApprovalOutcome::Granted,
        ApprovalDecision::Deny => ApprovalOutcome::Denied {
            reason: params
                .reason
                .unwrap_or_else(|| "approval denied by daemon client".into()),
        },
    });
    record.approval_changed.notify_all();
    drop(approvals);
    Envelope::response_from(
        request.id,
        Some("approval.decide".into()),
        CommandAcceptedResult {
            run_id: record.run_id.clone(),
            status: record.status().state,
        },
    )
}

fn handle_run_cancel(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: RunCancelParams,
) -> Envelope {
    let record = match find_run(runtime, &params.run_id) {
        Ok(record) => record,
        Err(error) => return error_response(request.id, "run.cancel", error),
    };
    let mut approvals = record.approvals.lock().expect("approvals lock poisoned");
    let status = {
        let mut status = record.status.lock().expect("run status lock poisoned");
        match status.state {
            RunStateName::Running => {
                status.state = RunStateName::CancelRequested;
                record.cancel.store(true, Ordering::SeqCst);
                record.push_event(StreamEvent::Canceled {
                    run_id: record.run_id.clone(),
                });
                approvals.clear();
                record.approval_changed.notify_all();
            }
            RunStateName::CancelRequested => {}
            RunStateName::Finished
            | RunStateName::Failed
            | RunStateName::Canceled
            | RunStateName::Interrupted => {
                return error_response(
                    request.id,
                    "run.cancel",
                    format!("run is not active: {}", record.run_id),
                );
            }
        }
        status.clone()
    };
    drop(approvals);
    Envelope::response_from(
        request.id,
        Some("run.cancel".into()),
        CommandAcceptedResult {
            run_id: record.run_id.clone(),
            status: status.state,
        },
    )
}

fn handle_sessions_list(runtime: &DaemonRuntime, request: Envelope) -> Envelope {
    match session_summaries(runtime) {
        Ok(sessions) => Envelope::response_from(
            request.id,
            Some("sessions.list".into()),
            SessionsListResult { sessions },
        ),
        Err(error) => Envelope::error(
            request.id,
            Some("sessions.list".into()),
            ERROR_SESSIONS_LIST_FAILED,
            error.to_string(),
        ),
    }
}

fn session_summaries(runtime: &DaemonRuntime) -> crate::AppResult<Vec<SessionSummary>> {
    let ledger_path = runtime.paths.ledger_path.clone();
    let mut sessions =
        crate::ledger::default_sqlite_session_summaries(&runtime.paths.default_ledger())?
            .into_iter()
            .map(|session| SessionSummary {
                session_id: session.session_id,
                run_id: session.run_id,
                status: session.status,
                latest_question: latest_question_preview(&session.latest_question),
                ledger_path: ledger_path.to_string_lossy().into_owned(),
            })
            .collect::<Vec<_>>();

    let state = runtime.state.lock().expect("runtime state lock poisoned");
    let active_sessions = state
        .runs
        .values()
        .filter_map(|record| {
            let status = record.status();
            if !matches!(
                status.state,
                RunStateName::Running | RunStateName::CancelRequested
            ) {
                return None;
            }
            Some(SessionSummary {
                session_id: record.session_id.clone(),
                run_id: record.run_id.clone(),
                status: status.state,
                latest_question: String::new(),
                ledger_path: record.ledger_path.to_string_lossy().into_owned(),
            })
        })
        .collect::<Vec<_>>();

    for session in &mut sessions {
        if session.status == RunStateName::Running
            && !active_sessions
                .iter()
                .any(|active| active.session_id == session.session_id)
        {
            session.status = RunStateName::Interrupted;
        }
    }

    for summary in active_sessions {
        if let Some(existing) = sessions
            .iter_mut()
            .find(|session| session.session_id == summary.session_id)
        {
            existing.run_id = summary.run_id;
            existing.status = summary.status;
            existing.ledger_path = summary.ledger_path;
        } else {
            // wait=false runs can be visible before begin_session_run persists the question.
            sessions.insert(0, summary);
        }
    }

    Ok(sessions)
}

fn latest_question_preview(question: &str) -> String {
    let line = question.lines().next().unwrap_or_default();
    if line.chars().count() <= LATEST_QUESTION_MAX_CHARS {
        return line.to_owned();
    }
    format!(
        "{}...",
        line.chars()
            .take(LATEST_QUESTION_MAX_CHARS)
            .collect::<String>()
    )
}

fn handle_transcript_read(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: TranscriptReadParams,
) -> Envelope {
    let transcript = if let Some(run_id) = params.run_id {
        read_run_transcript(&runtime.paths.default_ledger(), &run_id)
    } else if let Some(session_id) = params.session_id {
        read_session_transcript(&runtime.paths.default_ledger(), &session_id)
    } else {
        return Envelope::error(
            request.id,
            Some("transcript.read".into()),
            ERROR_MALFORMED_REQUEST,
            "run_id or session_id is required",
        );
    };
    match transcript {
        Ok(mut transcript) => {
            transcript.pending_approval = runtime_pending_approval(runtime, &transcript.run_id);
            Envelope::response_from(request.id, Some("transcript.read".into()), transcript)
        }
        Err(error) => Envelope::error(
            request.id,
            Some("transcript.read".into()),
            transcript_error_code(&error),
            error.to_string(),
        ),
    }
}

fn runtime_pending_approval(
    runtime: &DaemonRuntime,
    run_id: &str,
) -> Option<crate::daemon::protocol::PendingApprovalSnapshot> {
    let record = runtime
        .state
        .lock()
        .expect("runtime state lock poisoned")
        .runs
        .get(run_id)
        .cloned()?;
    record.pending_approval()
}

fn read_run_transcript(path: &DefaultSqlitePath, run_id: &str) -> AppResult<TranscriptReadResult> {
    if std::fs::symlink_metadata(path.as_path())
        .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
    {
        return Err(AppError::RunNotFound(run_id.into()));
    }
    let run = SqliteLedger::open_default_readonly(path)?.read_session_run(run_id)?;
    let readback = RunReadback::from_events(&run.records)?;
    let transcript = format_readback(&readback);
    Ok(TranscriptReadResult {
        run_id: run.run_id.clone(),
        status: run.status,
        final_answer: run.final_answer.clone(),
        transcript,
        typed: Some(TypedTranscript {
            runs: vec![typed_run(&run, readback.entries)],
        }),
        pending_approval: None,
    })
}

fn read_session_transcript(
    path: &DefaultSqlitePath,
    session_id: &str,
) -> AppResult<TranscriptReadResult> {
    if std::fs::symlink_metadata(path.as_path())
        .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
    {
        return Err(AppError::SessionNotFound(session_id.into()));
    }
    let session = SqliteLedger::open_default_readonly(path)?.read_session(session_id)?;
    let latest = session
        .runs
        .last()
        .ok_or_else(|| AppError::SessionNotFound(session_id.into()))?;
    let transcript = format_session_readback(&session)?;
    let typed_runs = session
        .runs
        .iter()
        .map(|run| {
            let readback = RunReadback::from_events(&run.records)?;
            Ok(typed_run(run, readback.entries))
        })
        .collect::<AppResult<Vec<_>>>()?;
    Ok(TranscriptReadResult {
        run_id: latest.run_id.clone(),
        status: latest.status,
        final_answer: latest.final_answer.clone(),
        transcript,
        typed: Some(TypedTranscript { runs: typed_runs }),
        pending_approval: None,
    })
}

fn typed_run(run: &SessionRunRecords, readback_entries: Vec<ReadbackEntry>) -> TypedRun {
    TypedRun {
        run_id: run.run_id.clone(),
        session_index: run.session_index,
        status: run.status,
        entries: typed_entries(&run.question, readback_entries),
    }
}

fn typed_entries(
    question: &str,
    readback_entries: Vec<ReadbackEntry>,
) -> Vec<TypedTranscriptEntry> {
    let mut entries = Vec::with_capacity(readback_entries.len() + 1);
    entries.push(TypedTranscriptEntry::User {
        text: question.into(),
    });
    for entry in readback_entries {
        let entry = match entry {
            ReadbackEntry::ContextFragment { .. }
            | ReadbackEntry::ContextCompacted { .. }
            | ReadbackEntry::ModelFailed { .. }
            | ReadbackEntry::ToolProposalsRejected { .. } => continue,
            ReadbackEntry::ModelMessage { message, .. } => TypedTranscriptEntry::Assistant {
                text: message.content,
            },
            ReadbackEntry::ToolCall { call, .. } => TypedTranscriptEntry::ToolCall {
                call_id: call.id.to_string(),
                tool: call.tool.to_string(),
                input: call.input,
            },
            ReadbackEntry::ToolResult { result } => TypedTranscriptEntry::ToolResult {
                call_id: result.call_id.to_string(),
                summary: result.summary,
            },
            ReadbackEntry::PolicyDenied { call_id, reason } => TypedTranscriptEntry::PolicyDenied {
                call_id: call_id.to_string(),
                reason,
            },
            ReadbackEntry::ApprovalGranted { call_id, actor_id } => {
                TypedTranscriptEntry::Approval {
                    call_id: call_id.to_string(),
                    decision: ApprovalDecisionName::Granted,
                    actor_id: actor_id.to_string(),
                    reason: None,
                }
            }
            ReadbackEntry::ApprovalDenied {
                call_id,
                actor_id,
                reason,
            } => TypedTranscriptEntry::Approval {
                call_id: call_id.to_string(),
                decision: ApprovalDecisionName::Denied,
                actor_id: actor_id.to_string(),
                reason: Some(reason),
            },
            ReadbackEntry::ToolFailed { call_id, reason } => TypedTranscriptEntry::ToolFailed {
                call_id: call_id.to_string(),
                error: reason,
            },
        };
        entries.push(entry);
    }
    entries
}

fn transcript_error_code(error: &AppError) -> &'static str {
    match error {
        AppError::RunNotFound(_)
        | AppError::SessionNotFound(_)
        | AppError::NoSqliteRuns
        | AppError::NoSqliteSessions => ERROR_NOT_FOUND,
        _ => ERROR_INTERNAL,
    }
}

fn latest_session_id(runtime: &DaemonRuntime) -> Result<String, String> {
    crate::ledger::latest_default_sqlite_session_id(&runtime.paths.default_ledger()).map_err(
        |error| match error {
            crate::AppError::NoSqliteSessions | crate::AppError::NoSqliteRuns => {
                "no previous session exists".into()
            }
            error => error.to_string(),
        },
    )
}

fn handle_with_params<T: serde::de::DeserializeOwned>(
    runtime: &DaemonRuntime,
    request: Envelope,
    method: &'static str,
    handler: fn(&DaemonRuntime, Envelope, T) -> Envelope,
) -> Envelope {
    let params = match &request.params {
        Some(params) => match serde_json::from_value::<T>(params.clone()) {
            Ok(params) => params,
            Err(error) => {
                return Envelope::error(
                    request.id,
                    Some(method.into()),
                    ERROR_MALFORMED_REQUEST,
                    format!("{method} params are invalid: {error}"),
                );
            }
        },
        None => {
            return Envelope::error(
                request.id,
                Some(method.into()),
                ERROR_MALFORMED_REQUEST,
                format!("{method} params are required"),
            );
        }
    };
    handler(runtime, request, params)
}

fn spawn_event_collector(
    record: Arc<RunRecord>,
    receiver: mpsc::Receiver<RunEvent>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        for event in receiver {
            collect_run_event(&record, event);
        }
    })
}

fn collect_run_event(record: &RunRecord, event: RunEvent) {
    match event {
        RunEvent::Ledger(recorded) => record.push_recorded_event(recorded),
        RunEvent::AssistantDelta(delta) => record.push_assistant_delta(delta),
    }
}

fn find_run(runtime: &DaemonRuntime, run_id: &str) -> Result<Arc<RunRecord>, String> {
    runtime
        .state
        .lock()
        .expect("runtime state lock poisoned")
        .runs
        .get(run_id)
        .cloned()
        .ok_or_else(|| format!("run not found: {run_id}"))
}

fn error_response(request_id: Option<String>, method: &'static str, message: String) -> Envelope {
    Envelope::error(request_id, Some(method.into()), ERROR_NOT_FOUND, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AssistantDeltaEvent;
    use platonic_core::{
        ActorId, ContextFragment, ContextLane, EffectClass, HarnessEvent, Message, MessageRole,
        RecordedEvent, ResultVisibility, RunId, ToolCall, ToolCallId, ToolName, ToolResult, TurnId,
    };
    use serde_json::json;
    use std::{sync::Barrier, time::Duration};

    #[test]
    fn terminal_status_waits_for_collected_events_on_success_failure_and_cancellation() {
        assert_terminal_waits_for_collector(
            "success",
            Ok(RunOutcome {
                run_id: RunId::new("run_success").unwrap(),
                final_answer: "done".into(),
            }),
            RunStateName::Finished,
        );
        assert_terminal_waits_for_collector(
            "failure",
            Err(AppError::RunFailed("provider failed".into())),
            RunStateName::Failed,
        );
        assert_terminal_waits_for_collector(
            "cancellation",
            Err(AppError::RunCanceled),
            RunStateName::Canceled,
        );
    }

    #[test]
    fn collector_handle_drains_in_order_without_changing_retention() {
        let record = test_run_record("retention");
        let (event_sender, event_receiver) = mpsc::channel();
        let event_collector = spawn_event_collector(record.clone(), event_receiver);
        let sent = crate::daemon::runtime::MAX_EVENT_BUFFER as u64 + 2;
        for delta_index in 0..sent {
            event_sender
                .send(test_delta("retention", delta_index))
                .unwrap();
        }
        drop(event_sender);

        event_collector.join().unwrap();

        let buffer = record.events.lock().unwrap();
        assert_eq!(buffer.first_offset, 2);
        assert_eq!(buffer.next_offset, sent);
        assert_eq!(
            buffer
                .events
                .iter()
                .map(|event| event.offset)
                .collect::<Vec<_>>(),
            (2..sent).collect::<Vec<_>>()
        );
    }

    #[test]
    fn collector_panic_becomes_typed_run_failure() {
        let runtime = test_runtime();
        let record = test_run_record("collector_panic");
        runtime.reserve_run(record.clone()).unwrap();
        let event_collector = thread::spawn(|| panic!("injected collector panic"));

        let error = finish_run_after_event_collection(
            &runtime,
            &record,
            Ok(RunOutcome {
                run_id: RunId::new("run_collector_panic").unwrap(),
                final_answer: "must not publish".into(),
            }),
            event_collector,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            AppError::RunFailed(ref reason) if reason == EVENT_COLLECTOR_PANIC
        ));
        assert_eq!(
            record.status(),
            crate::daemon::runtime::RunStatus {
                state: RunStateName::Failed,
                final_answer: None,
                error: Some(format!("run did not finish: {EVENT_COLLECTOR_PANIC}")),
            }
        );
    }

    #[test]
    fn event_snapshot_before_collector_cannot_publish_a_stale_terminal_page() {
        let runtime = test_runtime();
        let record = test_run_record("snapshot_before_collector");
        runtime.reserve_run(record.clone()).unwrap();
        let snapshot_reached = Arc::new(Barrier::new(2));
        let snapshot_release = Arc::new(Barrier::new(2));
        record.set_event_snapshot_barriers(snapshot_reached.clone(), snapshot_release.clone());

        let stream_runtime = runtime.clone();
        let stream_record = record.clone();
        let (stream_sender, stream_receiver) = mpsc::channel();
        let stream = thread::spawn(move || {
            stream_sender
                .send(stream_run(
                    &stream_runtime,
                    "stream_before_collector",
                    &stream_record.run_id,
                ))
                .unwrap();
        });
        snapshot_reached.wait();

        let (event_sender, event_receiver) = mpsc::channel();
        event_sender
            .send(test_delta("snapshot_before_collector", 0))
            .unwrap();
        drop(event_sender);
        let event_collector = spawn_event_collector(record.clone(), event_receiver);
        let finisher_runtime = runtime.clone();
        let finisher_record = record.clone();
        let finish_started = Arc::new(Barrier::new(2));
        let started = finish_started.clone();
        let (finished_sender, finished_receiver) = mpsc::channel();
        let finisher = thread::spawn(move || {
            started.wait();
            event_collector.join().unwrap();
            finisher_runtime.finish_run(&finisher_record, "done".into());
            finished_sender.send(()).unwrap();
        });
        finish_started.wait();
        assert!(matches!(
            finished_receiver.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));

        snapshot_release.wait();
        let before_collector = stream_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        stream.join().unwrap();
        assert_eq!(before_collector.status, RunStateName::Running);
        assert_eq!(before_collector.from_offset, 0);
        assert_eq!(before_collector.next_offset, 0);
        assert!(before_collector.events.is_empty());

        finished_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        finisher.join().unwrap();
        let terminal = stream_run(&runtime, "stream_terminal", &record.run_id);
        assert_eq!(terminal.status, RunStateName::Finished);
        assert_eq!(terminal.from_offset, 0);
        assert_eq!(terminal.next_offset, 1);
        assert!(matches!(
            terminal.events.as_slice(),
            [crate::daemon::protocol::BufferedStreamEvent {
                offset: 0,
                event: StreamEvent::AssistantDelta { delta_index: 0, .. },
            }]
        ));
    }

    #[test]
    fn run_cancel_stores_one_idempotent_transition_and_immediate_readback() {
        let runtime = test_runtime();
        let record = test_run_record("cancel_idempotent");
        runtime.reserve_run(record.clone()).unwrap();

        let first = cancel_run(&runtime, "cancel_1", &record.run_id);
        let first_result: CommandAcceptedResult =
            serde_json::from_value(first.result.clone().unwrap()).unwrap();

        assert_eq!(first.kind, crate::daemon::protocol::EnvelopeKind::Response);
        assert_eq!(first_result.run_id, record.run_id);
        assert_eq!(first_result.status, RunStateName::CancelRequested);
        assert_eq!(record.status().state, RunStateName::CancelRequested);
        assert!(record.cancel.load(Ordering::SeqCst));

        let readback = stream_run(&runtime, "stream_1", &record.run_id);
        assert_eq!(readback.status, RunStateName::CancelRequested);
        assert_eq!(readback.from_offset, 0);
        assert_eq!(readback.next_offset, 1);
        assert!(matches!(
            readback.events.as_slice(),
            [crate::daemon::protocol::BufferedStreamEvent {
                offset: 0,
                event: StreamEvent::Canceled { run_id },
            }] if run_id == &record.run_id
        ));

        let duplicate = cancel_run(&runtime, "cancel_2", &record.run_id);
        let duplicate_result: CommandAcceptedResult =
            serde_json::from_value(duplicate.result.clone().unwrap()).unwrap();
        assert_eq!(
            duplicate.kind,
            crate::daemon::protocol::EnvelopeKind::Response
        );
        assert_eq!(duplicate_result, first_result);

        let duplicate_readback = stream_run(&runtime, "stream_2", &record.run_id);
        assert_eq!(duplicate_readback.status, RunStateName::CancelRequested);
        assert_eq!(duplicate_readback.next_offset, 1);
        assert_eq!(duplicate_readback.events, readback.events);
    }

    #[test]
    fn run_cancel_rejects_every_terminal_status_without_side_effects() {
        let runtime = test_runtime();

        for terminal in [
            RunStateName::Finished,
            RunStateName::Failed,
            RunStateName::Canceled,
        ] {
            let record = test_run_record(terminal.as_str());
            record.status.lock().unwrap().state = terminal;
            runtime.reserve_run(record.clone()).unwrap();

            let response = cancel_run(&runtime, "cancel_terminal", &record.run_id);

            assert_eq!(response.kind, crate::daemon::protocol::EnvelopeKind::Error);
            let error = response.error.unwrap();
            assert_eq!(error.code, ERROR_NOT_FOUND);
            assert_eq!(
                error.message,
                format!("run is not active: {}", record.run_id)
            );
            assert_eq!(record.status().state, terminal);
            assert!(!record.cancel.load(Ordering::SeqCst));
            assert!(record.events.lock().unwrap().events.is_empty());
        }
    }

    #[test]
    fn run_cancel_and_finish_linearize_in_both_barrier_schedules() {
        let cancel_first_runtime = test_runtime();
        let cancel_first_record = test_run_record("cancel_first");
        cancel_first_runtime
            .reserve_run(cancel_first_record.clone())
            .unwrap();
        let collector_waiting = Arc::new(Barrier::new(2));
        let collector_release = Arc::new(Barrier::new(2));
        let waiting = collector_waiting.clone();
        let release = collector_release.clone();
        let event_collector = thread::spawn(move || {
            waiting.wait();
            release.wait();
        });
        collector_waiting.wait();
        let finish_started = Arc::new(Barrier::new(2));
        let started = finish_started.clone();
        let finisher_runtime = cancel_first_runtime.clone();
        let finisher_record = cancel_first_record.clone();
        let (finished_sender, finished_receiver) = mpsc::channel();
        let finisher = thread::spawn(move || {
            started.wait();
            let result = finish_run_after_event_collection(
                &finisher_runtime,
                &finisher_record,
                Ok(RunOutcome {
                    run_id: RunId::new("run_cancel_first").unwrap(),
                    final_answer: "done".into(),
                }),
                event_collector,
            );
            finished_sender.send(result).unwrap();
        });
        finish_started.wait();
        assert!(matches!(
            finished_receiver.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));

        let cancel = cancel_run(
            &cancel_first_runtime,
            "cancel_first",
            &cancel_first_record.run_id,
        );
        assert_eq!(cancel.kind, crate::daemon::protocol::EnvelopeKind::Response);
        assert_eq!(
            cancel_first_record.status().state,
            RunStateName::CancelRequested
        );
        collector_release.wait();
        finished_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();
        finisher.join().unwrap();

        let status = cancel_first_record.status();
        assert_eq!(status.state, RunStateName::Finished);
        assert_eq!(status.final_answer.as_deref(), Some("done"));
        assert_eq!(status.error, None);
        assert!(cancel_first_record.cancel.load(Ordering::SeqCst));
        assert_eq!(cancel_first_record.events.lock().unwrap().events.len(), 1);

        let finish_first_runtime = test_runtime();
        let finish_first_record = test_run_record("finish_first");
        finish_first_runtime
            .reserve_run(finish_first_record.clone())
            .unwrap();
        let finish_started = Arc::new(Barrier::new(2));
        let started = finish_started.clone();
        let finisher_runtime = finish_first_runtime.clone();
        let finisher_record = finish_first_record.clone();
        let (finished_sender, finished_receiver) = mpsc::channel();
        let finisher = thread::spawn(move || {
            started.wait();
            finisher_runtime.finish_run(&finisher_record, "done".into());
            finished_sender.send(()).unwrap();
        });
        finish_started.wait();
        finished_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap();

        let cancel = cancel_run(
            &finish_first_runtime,
            "finish_first",
            &finish_first_record.run_id,
        );
        finisher.join().unwrap();
        assert_eq!(cancel.kind, crate::daemon::protocol::EnvelopeKind::Error);
        assert_eq!(cancel.error.unwrap().code, ERROR_NOT_FOUND);
        let status = finish_first_record.status();
        assert_eq!(status.state, RunStateName::Finished);
        assert_eq!(status.final_answer.as_deref(), Some("done"));
        assert_eq!(status.error, None);
        assert!(!finish_first_record.cancel.load(Ordering::SeqCst));
        assert!(finish_first_record.events.lock().unwrap().events.is_empty());
    }

    fn assert_terminal_waits_for_collector(
        case: &'static str,
        outcome: AppResult<RunOutcome>,
        expected_status: RunStateName,
    ) {
        let runtime = test_runtime();
        let record = test_run_record(case);
        runtime.reserve_run(record.clone()).unwrap();
        let (event_sender, event_receiver) = mpsc::channel();
        for delta_index in 0..2 {
            event_sender.send(test_delta(case, delta_index)).unwrap();
        }
        drop(event_sender);

        let collected = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let collector_record = record.clone();
        let collector_collected = collected.clone();
        let collector_release = release.clone();
        let event_collector = thread::spawn(move || {
            let pending = event_receiver.into_iter().collect::<Vec<_>>();
            collector_collected.wait();
            collector_release.wait();
            for event in pending {
                collect_run_event(&collector_record, event);
            }
        });
        collected.wait();

        let finish_started = Arc::new(Barrier::new(2));
        let finisher_started = finish_started.clone();
        let finisher_runtime = runtime.clone();
        let finisher_record = record.clone();
        let (finished_sender, finished_receiver) = mpsc::channel();
        let finisher = thread::spawn(move || {
            finisher_started.wait();
            let result = finish_run_after_event_collection(
                &finisher_runtime,
                &finisher_record,
                outcome,
                event_collector,
            );
            finished_sender.send(result).unwrap();
        });
        finish_started.wait();

        assert_eq!(record.status().state, RunStateName::Running);
        let buffer = record.events.lock().unwrap();
        assert_eq!(buffer.first_offset, 0);
        assert_eq!(buffer.next_offset, 0);
        assert!(buffer.events.is_empty());
        drop(buffer);
        assert!(
            matches!(
                finished_receiver.recv_timeout(Duration::from_millis(100)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ),
            "{case} terminal status published before collector release"
        );

        release.wait();
        let result = finished_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        finisher.join().unwrap();
        let status = record.status();
        assert_eq!(status.state, expected_status);
        match expected_status {
            RunStateName::Finished => {
                assert_eq!(result.unwrap().final_answer, "done");
                assert_eq!(status.final_answer.as_deref(), Some("done"));
                assert_eq!(status.error, None);
            }
            RunStateName::Failed => {
                assert!(matches!(
                    result,
                    Err(AppError::RunFailed(ref reason)) if reason == "provider failed"
                ));
                assert_eq!(status.final_answer, None);
                assert_eq!(
                    status.error.as_deref(),
                    Some("run did not finish: provider failed")
                );
            }
            RunStateName::Canceled => {
                assert!(matches!(result, Err(AppError::RunCanceled)));
                assert_eq!(status.final_answer, None);
                assert_eq!(
                    status.error.as_deref(),
                    Some("run did not finish: run canceled")
                );
            }
            unexpected => panic!("unexpected terminal test status: {unexpected}"),
        }

        let buffer = record.events.lock().unwrap();
        assert_eq!(buffer.first_offset, 0);
        assert_eq!(buffer.next_offset, 2);
        assert_eq!(
            buffer
                .events
                .iter()
                .map(|event| event.offset)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
    }

    fn cancel_run(runtime: &DaemonRuntime, request_id: &str, run_id: &str) -> Envelope {
        handle_line(
            runtime,
            &format!(
                r#"{{"v":1,"id":"{request_id}","kind":"request","method":"run.cancel","params":{{"run_id":"{run_id}"}}}}"#
            ),
        )
    }

    fn stream_run(runtime: &DaemonRuntime, request_id: &str, run_id: &str) -> EventsStreamResult {
        let response = handle_line(
            runtime,
            &format!(
                r#"{{"v":1,"id":"{request_id}","kind":"request","method":"events.stream","params":{{"run_id":"{run_id}","from_offset":0,"limit":128}}}}"#
            ),
        );
        assert_eq!(
            response.kind,
            crate::daemon::protocol::EnvelopeKind::Response
        );
        serde_json::from_value(response.result.unwrap()).unwrap()
    }

    fn test_runtime() -> DaemonRuntime {
        DaemonRuntime::new(crate::daemon::server::DaemonPaths {
            workspace_root: PathBuf::from("/tmp/workspace"),
            workspace_id: "workspace-1".into(),
            socket_path: PathBuf::from("/tmp/agent.sock"),
            lock_path: PathBuf::from("/tmp/agent.lock"),
            ledger_path: PathBuf::from("/tmp/agent.db"),
        })
    }

    fn test_run_record(case: &str) -> Arc<RunRecord> {
        Arc::new(RunRecord::new(
            format!("run_{case}"),
            format!("session_{case}"),
            PathBuf::from("/tmp/agent.db"),
        ))
    }

    fn test_delta(case: &str, delta_index: u64) -> RunEvent {
        RunEvent::AssistantDelta(AssistantDeltaEvent {
            run_id: RunId::new(format!("run_{case}")).unwrap(),
            turn_id: TurnId::new("turn_1").unwrap(),
            step: 0,
            delta_index,
            text: format!("delta {delta_index}"),
        })
    }

    #[test]
    fn typed_entries_omit_context_compaction() {
        let entries = typed_entries(
            "current question",
            vec![ReadbackEntry::ContextCompacted {
                turn_id: TurnId::new("turn_1").unwrap(),
                estimated_tokens_before: 321,
                estimated_tokens_after: 123,
                dropped_turn_start: 0,
                dropped_turn_end_exclusive: 2,
            }],
        );

        assert_eq!(
            entries,
            vec![TypedTranscriptEntry::User {
                text: "current question".into()
            }]
        );
    }

    #[test]
    fn context_compaction_uses_v1_ledger_stream_envelope() {
        let runtime = DaemonRuntime::new(crate::daemon::server::DaemonPaths {
            workspace_root: PathBuf::from("/tmp/workspace"),
            workspace_id: "workspace-1".into(),
            socket_path: PathBuf::from("/tmp/agent.sock"),
            lock_path: PathBuf::from("/tmp/agent.lock"),
            ledger_path: PathBuf::from("/tmp/agent.db"),
        });
        let record = Arc::new(RunRecord::new(
            "run_1".into(),
            "session_1".into(),
            PathBuf::from("/tmp/agent.db"),
        ));
        runtime.reserve_run(record.clone()).unwrap();
        record.push_recorded_event(RecordedEvent {
            seq: 1,
            occurred_at_ms: 42,
            event: HarnessEvent::ContextCompacted {
                run_id: RunId::new("run_1").unwrap(),
                turn_id: TurnId::new("turn_1").unwrap(),
                estimated_tokens_before: 321,
                estimated_tokens_after: 123,
                dropped_turn_start: 0,
                dropped_turn_end_exclusive: 2,
            },
        });

        let response = handle_events_stream(
            &runtime,
            Envelope {
                v: 1,
                id: Some("stream_1".into()),
                kind: crate::daemon::protocol::EnvelopeKind::Request,
                method: Some("events.stream".into()),
                params: None,
                result: None,
                error: None,
            },
            EventsStreamParams {
                run_id: "run_1".into(),
                from_offset: Some(0),
                limit: Some(1),
            },
        );

        assert_eq!(response.v, 1);
        let result: EventsStreamResult = serde_json::from_value(response.result.unwrap()).unwrap();
        assert_eq!(result.events.len(), 1);
        let wire = serde_json::to_value(&result.events[0]).unwrap();
        assert_eq!(wire["event"]["kind"], "ledger");
        assert_eq!(
            wire["event"]["record"]["event"],
            json!({
                "event": "context_compacted",
                "run_id": "run_1",
                "turn_id": "turn_1",
                "estimated_tokens_before": 321,
                "estimated_tokens_after": 123,
                "dropped_turn_start": 0,
                "dropped_turn_end_exclusive": 2
            })
        );
    }

    #[test]
    fn typed_entries_map_all_human_readback_facts_in_order() {
        let turn_id = TurnId::new("turn_1").unwrap();
        let call_id = ToolCallId::new("call_1").unwrap();
        let entries = typed_entries(
            "do work",
            vec![
                ReadbackEntry::ContextFragment {
                    turn_id: turn_id.clone(),
                    fragment: ContextFragment {
                        lane: ContextLane::CurrentTask,
                        source: "user".into(),
                        content: "diagnostic context".into(),
                        estimated_tokens: 2,
                    },
                },
                ReadbackEntry::ModelMessage {
                    turn_id: turn_id.clone(),
                    message: Message {
                        role: MessageRole::Assistant,
                        content: "working".into(),
                    },
                },
                ReadbackEntry::ToolCall {
                    turn_id,
                    call: ToolCall {
                        id: call_id.clone(),
                        tool: ToolName::new("file.write").unwrap(),
                        effect: EffectClass::WorkspaceWrite,
                        input: json!({"path": "out.txt", "content": "done"}),
                    },
                },
                ReadbackEntry::ApprovalGranted {
                    call_id: call_id.clone(),
                    actor_id: ActorId::new("human_1").unwrap(),
                },
                ReadbackEntry::ToolResult {
                    result: ToolResult {
                        call_id: call_id.clone(),
                        summary: "wrote out.txt".into(),
                        data: json!({"bytes": 4}),
                        artifacts: vec![],
                        visibility: ResultVisibility::Both,
                    },
                },
                ReadbackEntry::ApprovalDenied {
                    call_id: ToolCallId::new("call_2").unwrap(),
                    actor_id: ActorId::new("human_2").unwrap(),
                    reason: "not now".into(),
                },
                ReadbackEntry::PolicyDenied {
                    call_id: ToolCallId::new("call_3").unwrap(),
                    reason: "secret access denied".into(),
                },
                ReadbackEntry::ToolFailed {
                    call_id: ToolCallId::new("call_4").unwrap(),
                    reason: "tool crashed".into(),
                },
            ],
        );

        assert_eq!(
            entries,
            vec![
                TypedTranscriptEntry::User {
                    text: "do work".into()
                },
                TypedTranscriptEntry::Assistant {
                    text: "working".into()
                },
                TypedTranscriptEntry::ToolCall {
                    call_id: "call_1".into(),
                    tool: "file.write".into(),
                    input: json!({"path": "out.txt", "content": "done"}),
                },
                TypedTranscriptEntry::Approval {
                    call_id: "call_1".into(),
                    decision: ApprovalDecisionName::Granted,
                    actor_id: "human_1".into(),
                    reason: None,
                },
                TypedTranscriptEntry::ToolResult {
                    call_id: "call_1".into(),
                    summary: "wrote out.txt".into(),
                },
                TypedTranscriptEntry::Approval {
                    call_id: "call_2".into(),
                    decision: ApprovalDecisionName::Denied,
                    actor_id: "human_2".into(),
                    reason: Some("not now".into()),
                },
                TypedTranscriptEntry::PolicyDenied {
                    call_id: "call_3".into(),
                    reason: "secret access denied".into(),
                },
                TypedTranscriptEntry::ToolFailed {
                    call_id: "call_4".into(),
                    error: "tool crashed".into(),
                },
            ]
        );
    }
}
