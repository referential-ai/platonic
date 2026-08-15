use super::{
    control::shutting_down_response,
    sessions::latest_session_id,
    threads::projected_thread_spawn_handler,
    types::{StartRunRequest, ThreadRunContext},
};
use crate::{
    AppError, AppResult, ApprovalMode, RunEvent, RunLedger, RunOptions, RunOutcome, RunSession,
    app::{ExternalApprovalOutcome, PreparedRun},
    confinement::ChildConfinement,
    daemon::{
        protocol::{
            ApprovalDecideParams, ApprovalDecision, BufferedStreamEvent, CommandAcceptedResult,
            ERROR_INTERNAL, ERROR_ISSUE_PREP_FAILED, ERROR_LAGGED, ERROR_MALFORMED_REQUEST,
            ERROR_NOT_FOUND, ERROR_OVERLOAD, ERROR_RUN_FAILED, ERROR_THREAD_SEND_FAILED,
            ERROR_VOICE_EVENTS_CONFLICT, Envelope, EventsStreamParams, EventsStreamResult,
            IssuePrepResult, IssuePrepStartParams, IssuePrepStartResult, MessageAppendParams,
            ProtocolResponse, RunCancelParams, RunStartParams, RunStartResult, RunStateName,
            StreamEvent, ThreadApprovalPolicy, ThreadConfinement, VoiceEvent,
            VoiceEventsCommitParams, VoiceEventsReadParams, VoiceEventsResult,
        },
        runtime::{
            DaemonRuntime, IssuePrepAdmissionError, RunAdmissionError, RunRecord,
            ThreadRunBindError, approval_handler,
        },
    },
    issue_prep::{IssuePrepOptions, IssuePrepOutcome, run_issue_prep},
    ledger::{EventRecorder, SqliteLedger},
    model::RunOverrides,
    new_run_id, new_session_id,
    tool_catalog::{SHELL_EXEC, THREAD_ANSWER, THREAD_RETURN},
    tools::{
        LogicalReadToolHandler, ParentAnswerToolHandler, ThreadReturnToolHandler,
        ThreadSpawnToolHandler,
    },
};
use platonic_core::{ActorId, EffectClass, HarnessEvent, RunId, RunIdentity};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
};

pub(super) const DEFAULT_EVENT_LIMIT: usize = 64;
pub(super) const MAX_EVENT_LIMIT: usize = 128;
const EVENT_COLLECTOR_PANIC: &str = "daemon event collector panicked";
const MAX_VOICE_EVENTS: usize = 128;
const MAX_VOICE_EVENT_PAYLOAD_BYTES: usize = 256 * 1024;
const MAX_SPOKEN_PREFIX_BYTES: usize = 16 * 1024;

pub(super) fn handle_run_start(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: RunStartParams,
) -> Envelope {
    let request_id = request.id;
    match start_run(
        runtime,
        StartRunRequest {
            request_id: request_id.clone(),
            question: params.question,
            session: RunSession::Fresh {
                session_id: new_session_id(),
            },
            config_path: params.config_path,
            overrides: params.overrides,
            approval_profile: Some(params.approval_profile.unwrap_or_default()),
            prior_interrupted_run_id: None,
            wait: params.wait,
            thread_context: None,
        },
    ) {
        Ok(result) => Envelope::typed_response(request_id, ProtocolResponse::RunStart(result)),
        Err(response) => *response,
    }
}

pub(super) fn handle_message_append(
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
    let request_id = request.id;
    match start_run(
        runtime,
        StartRunRequest {
            request_id: request_id.clone(),
            question: params.message,
            session: RunSession::Continue { session_id },
            config_path: params.config_path,
            overrides: params.overrides,
            approval_profile: params.approval_profile,
            prior_interrupted_run_id: params.prior_interrupted_run_id,
            wait: params.wait,
            thread_context: None,
        },
    ) {
        Ok(result) => Envelope::typed_response(request_id, ProtocolResponse::MessageAppend(result)),
        Err(response) => *response,
    }
}

pub(super) fn handle_issue_prep_start(
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
        Ok(outcome) => Envelope::typed_response(
            request.id,
            ProtocolResponse::IssuePrepStart(IssuePrepStartResult {
                run_dir: run_dir.to_string_lossy().into_owned(),
                outcome: match outcome {
                    IssuePrepOutcome::Candidate { markdown } => {
                        IssuePrepResult::Candidate { markdown }
                    }
                    IssuePrepOutcome::Blocked { stage, reasons } => {
                        IssuePrepResult::Blocked { stage, reasons }
                    }
                },
            }),
        ),
        Err(error) => Envelope::error(
            request.id,
            Some("issue-prep.start".into()),
            ERROR_ISSUE_PREP_FAILED,
            format!("{error}; run directory: {}", run_dir.display()),
        ),
    }
}

struct ThreadTurnDriver {
    context: ThreadRunContext,
    session_id: String,
    config_path: Option<String>,
    overrides: RunOverrides,
}

struct RunAuthorityProjection {
    identity: Option<platonic_core::RunIdentity>,
    toolset: Option<Vec<String>>,
    thread_spawn: Option<ThreadSpawnToolHandler>,
    logical_read: Option<LogicalReadToolHandler>,
    thread_return: Option<ThreadReturnToolHandler>,
    parent_answer: Option<ParentAnswerToolHandler>,
    confinement: ChildConfinement,
}

struct AdmittedRun {
    prepared: PreparedRun,
    recorder: EventRecorder,
    approval_mode: ApprovalMode,
    event_sender: mpsc::Sender<RunEvent>,
    cancel: Arc<AtomicBool>,
    event_collector: thread::JoinHandle<()>,
    authority: RunAuthorityProjection,
    owns_one_shot_scratch: bool,
}

pub(super) fn start_run(
    runtime: &DaemonRuntime,
    request: StartRunRequest,
) -> Result<RunStartResult, Box<Envelope>> {
    let StartRunRequest {
        request_id,
        question,
        session,
        config_path,
        overrides,
        approval_profile,
        prior_interrupted_run_id,
        wait,
        thread_context,
    } = request;
    let method = match (&thread_context, &session) {
        (Some(_), _) => "thread.send",
        (None, RunSession::Fresh { .. }) => "run.start",
        (None, RunSession::Continue { .. }) => "message.append",
    };
    let error_code = if thread_context.is_some() {
        ERROR_THREAD_SEND_FAILED
    } else {
        ERROR_RUN_FAILED
    };
    let one_shot_confinement = if thread_context.is_none() {
        match runtime.thread_confinement() {
            Ok(confinement) => Some(confinement),
            Err(()) => {
                return Err(Box::new(Envelope::error(
                    request_id,
                    Some(method.into()),
                    error_code,
                    "server policy requires confinement, but this run cannot be confined",
                )));
            }
        }
    } else {
        None
    };
    let session_id = session.session_id().to_string();
    let voice_interruption_context = match prior_interrupted_run_id.as_deref() {
        Some(prior_run_id) => {
            let context = match thread_context.as_ref() {
                Some(thread) => prior_thread_interruption_context(
                    runtime,
                    &session_id,
                    prior_run_id,
                    &thread.identity,
                ),
                None => prior_voice_interruption_context(runtime, &session_id, prior_run_id),
            };
            match context {
                Ok(context) => Some(context),
                Err(error) => {
                    return Err(Box::new(Envelope::error(
                        request_id,
                        Some(method.into()),
                        error_code,
                        error.to_string(),
                    )));
                }
            }
        }
        None => None,
    };
    let run_id = match new_run_id() {
        Ok(run_id) => run_id,
        Err(error) => {
            return Err(Box::new(Envelope::error(
                request_id,
                Some(method.into()),
                error_code,
                error.to_string(),
            )));
        }
    };
    let run_id_string = run_id.to_string();
    let record = Arc::new(match &thread_context {
        Some(context) => RunRecord::new_for_thread(
            run_id_string.clone(),
            session_id.clone(),
            runtime.paths.ledger_path.clone(),
            context.turn.clone(),
        ),
        None => RunRecord::new(
            run_id_string.clone(),
            session_id.clone(),
            runtime.paths.ledger_path.clone(),
        ),
    });
    match runtime.reserve_run_with_profile(record.clone(), approval_profile) {
        Ok(()) => {}
        Err(RunAdmissionError::ShuttingDown) => {
            return Err(Box::new(shutting_down_response(request_id, method)));
        }
        Err(RunAdmissionError::SessionActive { run_id }) => {
            return Err(Box::new(Envelope::error(
                request_id,
                Some(method.into()),
                ERROR_OVERLOAD,
                format!(
                    "session already has an active run: {} ({run_id})",
                    record.session_id
                ),
            )));
        }
    }
    let (one_shot_confinement, owns_one_shot_scratch) =
        match prepare_one_shot_confinement(runtime, &run_id_string, one_shot_confinement) {
            Ok(prepared) => prepared,
            Err(error) => {
                runtime.release_run_reservation(&record);
                return Err(Box::new(Envelope::error(
                    request_id,
                    Some(method.into()),
                    error_code,
                    error.to_string(),
                )));
            }
        };
    if let Some(context) = thread_context.as_ref()
        && let Err(error) = runtime.bind_thread_run(&context.turn, record.clone())
    {
        runtime.release_run_reservation(&record);
        runtime.abort_thread_turn(&context.turn);
        return Err(Box::new(match error {
            ThreadRunBindError::Stopping | ThreadRunBindError::NotLoaded => Envelope::error(
                request_id,
                Some(method.into()),
                ERROR_NOT_FOUND,
                format!("thread not found: {}", context.turn.thread_id),
            ),
            ThreadRunBindError::RunActive => Envelope::error(
                request_id,
                Some(method.into()),
                ERROR_OVERLOAD,
                format!(
                    "thread already has an active run: {}",
                    context.turn.thread_id
                ),
            ),
        }));
    }

    let continuation_config_path = config_path.clone();
    let continuation_overrides = overrides.clone();
    let thread_yolo = matches!(
        thread_context
            .as_ref()
            .map(|context| context.approval_policy),
        Some(ThreadApprovalPolicy::Yolo)
    );
    let options = RunOptions {
        question,
        config_path: config_path.map(PathBuf::from),
        overrides,
        ledger: RunLedger::DefaultSqlite(runtime.paths.default_ledger()),
        workspace_root: thread_context.as_ref().map_or_else(
            || runtime.paths.workspace_root.clone(),
            |context| context.workspace_root.clone(),
        ),
        approval_mode: ApprovalMode::external_with_actor(
            "daemon",
            approval_handler(runtime.clone(), record.clone(), thread_yolo),
        ),
        run_id: Some(run_id),
        session: Some(session),
        event_sender: None,
        stream_to_stderr: false,
        cancel: None,
        voice_interruption_context,
    };
    let run_identity = thread_context
        .as_ref()
        .map(|context| context.identity.clone());
    let run_toolset = thread_context
        .as_ref()
        .map(|context| context.toolset.clone());
    let thread_spawn = thread_context
        .as_ref()
        .and_then(|context| projected_thread_spawn_handler(runtime, context));
    let logical_read = thread_context.as_ref().and_then(|context| {
        crate::daemon::logical_reads::projected_handler(
            runtime,
            &context.turn.thread_id,
            &context.identity,
            &context.toolset,
        )
    });
    let thread_return = thread_context.as_ref().and_then(|context| {
        crate::daemon::returns::projected_thread_return_handler(
            runtime,
            &context.turn.thread_id,
            &context.turn.turn_id,
            &run_id_string,
            context.toolset.iter().any(|tool| tool == THREAD_RETURN),
        )
    });
    let parent_answer = thread_context.as_ref().and_then(|context| {
        crate::daemon::returns::projected_parent_answer_handler(
            runtime,
            &context.turn.thread_id,
            &context.turn.turn_id,
            &run_id_string,
            context.toolset.iter().any(|tool| tool == THREAD_ANSWER),
        )
    });
    let child_confinement = thread_context
        .as_ref()
        .map_or(one_shot_confinement, |context| context.confinement.clone());
    let authority = RunAuthorityProjection {
        identity: run_identity,
        toolset: run_toolset,
        thread_spawn,
        logical_read,
        thread_return,
        parent_answer,
        confinement: child_confinement,
    };

    let admission =
        selected_profile_revision(runtime, authority.identity.as_ref()).and_then(|revision| {
            let (mut prepared, recorder) = crate::app::prepare_run_for_thread(
                &options,
                authority.identity.clone(),
                authority.toolset.as_deref(),
                revision.as_ref(),
            )?;
            if let (Some(context), Some(identity)) =
                (thread_context.as_ref(), authority.identity.as_ref())
                && let Err(error) = crate::daemon::returns::admit_spawn_edge_context(
                    runtime,
                    &mut prepared,
                    identity,
                    &context.turn.thread_id,
                    &context.turn.turn_id,
                    &run_id_string,
                )
            {
                let release =
                    crate::daemon::returns::discard_run_admission(runtime, &run_id_string);
                let ledger = recorder.discard_empty_session_admission();
                return Err(combine_run_admission_errors(error, release, ledger));
            }
            Ok((prepared, recorder))
        });
    let (prepared, recorder) = match admission {
        Ok(admission) => admission,
        Err(error) => {
            runtime.release_run_reservation(&record);
            if let Some(context) = thread_context.as_ref() {
                runtime.abort_thread_turn(&context.turn);
            }
            let cleanup = remove_one_shot_run_root(
                &runtime.paths.server_db_path,
                &run_id_string,
                owns_one_shot_scratch,
            );
            let message = match cleanup {
                Ok(()) => error.to_string(),
                Err(cleanup) => {
                    format!("{error}; one-shot scratch cleanup failed: {cleanup}")
                }
            };
            return Err(Box::new(Envelope::error(
                request_id,
                Some(method.into()),
                error_code,
                message,
            )));
        }
    };
    let (event_sender, event_receiver) = mpsc::channel::<RunEvent>();
    let event_collector = spawn_event_collector(record.clone(), event_receiver);
    let admitted = AdmittedRun {
        prepared,
        recorder,
        approval_mode: options.approval_mode,
        event_sender,
        cancel: record.cancel.clone(),
        event_collector,
        authority,
        owns_one_shot_scratch,
    };

    if wait.unwrap_or(false) {
        match run_to_completion(runtime, &record, admitted) {
            Ok(_) => Ok(run_start_result(&record)),
            Err(error) => Err(Box::new(Envelope::error(
                request_id,
                Some(method.into()),
                error_code,
                error.to_string(),
            ))),
        }
    } else {
        let worker_runtime = runtime.clone();
        let worker_record = record.clone();
        let worker_context = thread_context.clone();
        let driver = thread_context.map(|context| ThreadTurnDriver {
            context,
            session_id,
            config_path: continuation_config_path,
            overrides: continuation_overrides,
        });
        let (handoff_sender, handoff_receiver) = mpsc::sync_channel(0);
        #[cfg(test)]
        let fail_handoff = runtime.take_run_handoff_failure();
        let worker = thread::Builder::new().spawn(move || {
            #[cfg(test)]
            if fail_handoff {
                return;
            }
            let Ok(admitted) = handoff_receiver.recv() else {
                return;
            };
            #[cfg(test)]
            worker_runtime.wait_before_run_execution();
            match driver {
                Some(driver) => {
                    drive_thread_turn(worker_runtime, worker_record, admitted, driver);
                }
                None => {
                    let _ = run_to_completion(&worker_runtime, &worker_record, admitted);
                }
            }
        });
        let worker = match worker {
            Ok(worker) => worker,
            Err(error) => {
                let cleanup =
                    discard_unstarted_run(runtime, &record, worker_context.as_ref(), admitted);
                return Err(Box::new(Envelope::error(
                    request_id,
                    Some(method.into()),
                    error_code,
                    handoff_error(error.to_string(), cleanup),
                )));
            }
        };
        if let Err(error) = handoff_sender.send(admitted) {
            let _ = worker.join();
            let cleanup = discard_unstarted_run(runtime, &record, worker_context.as_ref(), error.0);
            return Err(Box::new(Envelope::error(
                request_id,
                Some(method.into()),
                error_code,
                handoff_error("run execution handoff failed".into(), cleanup),
            )));
        }
        drop(worker);
        Ok(run_start_result(&record))
    }
}

fn selected_profile_revision(
    runtime: &DaemonRuntime,
    identity: Option<&RunIdentity>,
) -> AppResult<Option<crate::server_store::ProfileRevisionRecord>> {
    let Some(RunIdentity::Profile {
        profile_id,
        profile_revision,
    }) = identity
    else {
        return Ok(None);
    };
    let store = runtime.paths.server_store()?;
    let profile = store
        .profile(profile_id)?
        .filter(|profile| profile.workspace_id == runtime.paths.workspace_id)
        .ok_or_else(|| AppError::Config("run profile is not a member of its workspace".into()))?;
    if *profile_revision > profile.current_revision {
        return Err(AppError::Config(format!(
            "run selected future profile revision {profile_revision} for {profile_id}"
        )));
    }
    store
        .profile_revision(profile_id, *profile_revision)?
        .map(Some)
        .ok_or_else(|| {
            AppError::Config(format!(
                "profile {profile_id} is missing selected revision {profile_revision}"
            ))
        })
}

fn discard_unstarted_run(
    runtime: &DaemonRuntime,
    record: &RunRecord,
    thread_context: Option<&ThreadRunContext>,
    admitted: AdmittedRun,
) -> AppResult<()> {
    runtime.release_run_reservation(record);
    if let Some(context) = thread_context {
        runtime.abort_thread_turn(&context.turn);
    }
    let AdmittedRun {
        recorder,
        event_sender,
        event_collector,
        owns_one_shot_scratch,
        ..
    } = admitted;
    drop(event_sender);
    let collector = event_collector
        .join()
        .map_err(|_| AppError::RunFailed(EVENT_COLLECTOR_PANIC.into()));
    let returns = crate::daemon::returns::discard_run_admission(runtime, &record.run_id);
    let admission = recorder.discard_empty_session_admission();
    let admission = match (returns, admission) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(returns), Err(admission)) => Err(AppError::Config(format!(
            "{returns}; failed to discard ledger run admission: {admission}"
        ))),
    };
    let admission = match (collector, admission) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(collector), Err(admission)) => Err(AppError::Config(format!(
            "{collector}; failed to discard run admission: {admission}"
        ))),
    };
    let scratch = remove_one_shot_run_root(
        &runtime.paths.server_db_path,
        &record.run_id,
        owns_one_shot_scratch,
    );
    match (admission, scratch) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(admission), Err(scratch)) => Err(AppError::Config(format!(
            "{admission}; one-shot scratch cleanup failed: {scratch}"
        ))),
    }
}

fn handoff_error(error: String, cleanup: AppResult<()>) -> String {
    match cleanup {
        Ok(()) => error,
        Err(cleanup) => format!("{error}; run admission cleanup failed: {cleanup}"),
    }
}

fn combine_run_admission_errors(
    error: AppError,
    return_cleanup: AppResult<()>,
    ledger_cleanup: AppResult<()>,
) -> AppError {
    let mut message = error.to_string();
    if let Err(cleanup) = return_cleanup {
        message.push_str(&format!("; return reservation cleanup failed: {cleanup}"));
    }
    if let Err(cleanup) = ledger_cleanup {
        message.push_str(&format!("; ledger admission cleanup failed: {cleanup}"));
    }
    AppError::Config(message)
}

fn drive_thread_turn(
    runtime: DaemonRuntime,
    record: Arc<RunRecord>,
    admitted: AdmittedRun,
    driver: ThreadTurnDriver,
) {
    let _ = run_to_completion(&runtime, &record, admitted);
    while let Some(message) = runtime.next_thread_message(&driver.context.turn) {
        let _ = start_run(
            &runtime,
            StartRunRequest {
                request_id: None,
                question: message,
                session: RunSession::Continue {
                    session_id: driver.session_id.clone(),
                },
                config_path: driver.config_path.clone(),
                overrides: driver.overrides.clone(),
                approval_profile: None,
                prior_interrupted_run_id: None,
                wait: Some(true),
                thread_context: Some(driver.context.clone()),
            },
        );
    }
    runtime.abort_thread_turn(&driver.context.turn);
}

fn run_to_completion(
    runtime: &DaemonRuntime,
    record: &RunRecord,
    admitted: AdmittedRun,
) -> AppResult<RunOutcome> {
    let AdmittedRun {
        prepared,
        recorder,
        approval_mode,
        event_sender,
        cancel,
        event_collector,
        authority,
        owns_one_shot_scratch,
    } = admitted;
    let RunAuthorityProjection {
        thread_spawn,
        logical_read,
        thread_return,
        parent_answer,
        confinement,
        ..
    } = authority;
    #[cfg(test)]
    let _ = &confinement;
    #[cfg(test)]
    let mut recorder = recorder;
    #[cfg(test)]
    let completion = RunCompletion::Published(crate::app::run_prepared_question(
        prepared,
        &mut recorder,
        approval_mode,
        Some(event_sender),
        false,
        Some(cancel),
        crate::tools::RunToolHandlers {
            thread_spawn,
            logical_read,
            thread_return,
            parent_answer,
        },
    ));
    #[cfg(not(test))]
    let completion = RunCompletion::Supervised(Box::new(crate::daemon::run_child::run_supervised(
        prepared,
        recorder,
        approval_mode,
        event_sender,
        cancel,
        crate::tools::RunToolHandlers {
            thread_spawn,
            logical_read,
            thread_return,
            parent_answer,
        },
        confinement,
    )));
    let completion = match remove_one_shot_run_root(
        &runtime.paths.server_db_path,
        &record.run_id,
        owns_one_shot_scratch,
    ) {
        Ok(()) => completion,
        Err(error) => match completion {
            RunCompletion::Published(_) => RunCompletion::Published(Err(error)),
            RunCompletion::Supervised(completion) => {
                RunCompletion::Supervised(Box::new((*completion).override_failure(error)))
            }
        },
    };
    finish_run_after_event_collection(runtime, record, completion, event_collector)
}

fn prepare_one_shot_confinement(
    runtime: &DaemonRuntime,
    run_id: &str,
    confinement: Option<ThreadConfinement>,
) -> AppResult<(ChildConfinement, bool)> {
    match confinement {
        Some(ThreadConfinement::Landlock) => {
            let run_root = crate::paths::one_shot_run_root(&runtime.paths.server_db_path, run_id)?;
            if run_root.exists() {
                return Err(AppError::Config(format!(
                    "one-shot run root already exists: {}",
                    run_root.display()
                )));
            }
            if let Err(error) = crate::thread_repository::create_private_directory(&run_root) {
                let _ = remove_one_shot_run_root(&runtime.paths.server_db_path, run_id, true);
                return Err(error);
            }
            let scratch = run_root.join("scratch");
            if let Err(error) = crate::thread_repository::create_private_directory(&scratch) {
                let _ = remove_one_shot_run_root(&runtime.paths.server_db_path, run_id, true);
                return Err(error);
            }
            let scratch = match scratch.canonicalize() {
                Ok(scratch) => scratch,
                Err(error) => {
                    let _ = remove_one_shot_run_root(&runtime.paths.server_db_path, run_id, true);
                    return Err(error.into());
                }
            };
            Ok((
                ChildConfinement::Landlock {
                    readable_paths: vec![runtime.paths.workspace_root.clone(), scratch.clone()],
                    writable_paths: vec![runtime.paths.workspace_root.clone(), scratch.clone()],
                    scratch,
                },
                true,
            ))
        }
        Some(ThreadConfinement::None) | None => Ok((ChildConfinement::None, false)),
    }
}

fn remove_one_shot_run_root(server_db_path: &Path, run_id: &str, owned: bool) -> AppResult<()> {
    if !owned {
        return Ok(());
    }
    let root = crate::paths::one_shot_run_root(server_db_path, run_id)?;
    match std::fs::symlink_metadata(&root) {
        Ok(metadata) if metadata.file_type().is_dir() => {
            std::fs::remove_dir_all(root).map_err(Into::into)
        }
        Ok(_) => Err(AppError::Config(format!(
            "one-shot run root is not a directory: {}",
            root.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub(in crate::daemon) fn reconcile_one_shot_run_roots(server_db_path: &Path) -> AppResult<()> {
    let root = crate::paths::one_shot_runs_root(server_db_path)?;
    let entries = match std::fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    for entry in entries {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            std::fs::remove_dir_all(entry.path())?;
        } else {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

pub(super) enum RunCompletion {
    Published(AppResult<RunOutcome>),
    Supervised(Box<crate::daemon::run_child::SupervisedRunCompletion>),
}

impl From<AppResult<RunOutcome>> for RunCompletion {
    fn from(outcome: AppResult<RunOutcome>) -> Self {
        Self::Published(outcome)
    }
}

impl From<crate::daemon::run_child::SupervisedRunCompletion> for RunCompletion {
    fn from(completion: crate::daemon::run_child::SupervisedRunCompletion) -> Self {
        Self::Supervised(Box::new(completion))
    }
}

pub(super) fn finish_run_after_event_collection(
    runtime: &DaemonRuntime,
    record: &RunRecord,
    completion: impl Into<RunCompletion>,
    event_collector: thread::JoinHandle<()>,
) -> AppResult<RunOutcome> {
    let completion = match (completion.into(), event_collector.join()) {
        (completion, Ok(())) => completion,
        (RunCompletion::Published(_), Err(_)) => {
            RunCompletion::Published(Err(AppError::RunFailed(EVENT_COLLECTOR_PANIC.into())))
        }
        (RunCompletion::Supervised(completion), Err(_)) => RunCompletion::Supervised(Box::new(
            (*completion).override_failure(AppError::RunFailed(EVENT_COLLECTOR_PANIC.into())),
        )),
    };
    let outcome = match completion {
        RunCompletion::Published(outcome) => outcome,
        RunCompletion::Supervised(completion) => {
            let (outcome, terminal) = (*completion).publish();
            match terminal {
                Ok(recorded) => {
                    collect_run_event(record, RunEvent::Ledger(recorded));
                    outcome
                }
                Err(error) => Err(error),
            }
        }
    };
    let outcome = match crate::daemon::returns::reconcile_run(runtime, &record.run_id) {
        Ok(()) => outcome,
        Err(error) => Err(error),
    };
    match &outcome {
        Ok(outcome) => runtime.finish_run(
            record,
            outcome.final_answer.clone(),
            outcome.completion_claim.clone(),
        ),
        Err(error) => runtime.finish_run_with_error(record, error),
    }
    outcome
}

fn run_start_result(record: &RunRecord) -> RunStartResult {
    let status = record.status();
    RunStartResult {
        run_id: record.run_id.clone(),
        session_id: record.session_id.clone(),
        ledger_path: record.ledger_path.to_string_lossy().into_owned(),
        status: status.state,
        final_answer: status.final_answer,
        completion_claim: status.completion_claim,
    }
}

pub(super) fn handle_events_stream(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: EventsStreamParams,
) -> Envelope {
    let limit = params.limit.unwrap_or(DEFAULT_EVENT_LIMIT);
    if limit > MAX_EVENT_LIMIT {
        return Envelope::error(
            request.id,
            Some("events.stream".into()),
            ERROR_OVERLOAD,
            format!("event stream limit exceeds maximum {MAX_EVENT_LIMIT}: {limit}"),
        );
    }
    let record = match find_run(runtime, &params.run_id) {
        Ok(record) => record,
        Err(_) => {
            return match durable_events_stream(runtime, &params, limit) {
                Ok(result) => {
                    Envelope::typed_response(request.id, ProtocolResponse::EventsStream(result))
                }
                Err(AppError::RunNotFound(_)) => error_response(
                    request.id,
                    "events.stream",
                    format!("run not found: {}", params.run_id),
                ),
                Err(error) => Envelope::error(
                    request.id,
                    Some("events.stream".into()),
                    ERROR_INTERNAL,
                    error.to_string(),
                ),
            };
        }
    };
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
    Envelope::typed_response(request.id, ProtocolResponse::EventsStream(result))
}

pub(super) fn handle_voice_events_commit(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: VoiceEventsCommitParams,
) -> Envelope {
    let request_id = request.id;
    match commit_voice_events(runtime, params) {
        Ok(result) => {
            Envelope::typed_response(request_id, ProtocolResponse::VoiceEventsCommit(result))
        }
        Err(error) => voice_events_error(request_id, "voice.events.commit", error),
    }
}

fn commit_voice_events(
    runtime: &DaemonRuntime,
    params: VoiceEventsCommitParams,
) -> AppResult<VoiceEventsResult> {
    validate_voice_commit_request(&params)?;

    // ponytail: one global lock gives both backends one writer; split only if throughput needs it.
    let _state = runtime.state.lock().expect("daemon state lock poisoned");
    require_voice_ledger(runtime, &params.run_id)?;
    let mut ledger = SqliteLedger::open_or_create_default(&runtime.paths.default_ledger())?;
    let run = ledger.read_session_run(&params.run_id)?;
    if matches!(
        run.status,
        RunStateName::Running | RunStateName::CancelRequested
    ) {
        return Err(AppError::VoiceEventContract(format!(
            "run {} is not terminal",
            params.run_id
        )));
    }
    validate_voice_capture(&run, &params.events)?;
    let events = ledger.append_voice_events(&params.events)?;
    Ok(VoiceEventsResult {
        run_id: params.run_id,
        events,
    })
}

pub(super) fn handle_voice_events_read(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: VoiceEventsReadParams,
) -> Envelope {
    let request_id = request.id;
    match read_voice_events(runtime, params) {
        Ok(result) => {
            Envelope::typed_response(request_id, ProtocolResponse::VoiceEventsRead(result))
        }
        Err(error) => voice_events_error(request_id, "voice.events.read", error),
    }
}

fn read_voice_events(
    runtime: &DaemonRuntime,
    params: VoiceEventsReadParams,
) -> AppResult<VoiceEventsResult> {
    validate_voice_run_id(&params.run_id)?;
    let _state = runtime.state.lock().expect("daemon state lock poisoned");
    require_voice_ledger(runtime, &params.run_id)?;
    let ledger = SqliteLedger::open_default_readonly(&runtime.paths.default_ledger())?;
    ledger.read_session_run(&params.run_id)?;
    let events = ledger.read_voice_events(&params.run_id)?;
    Ok(VoiceEventsResult {
        run_id: params.run_id,
        events,
    })
}

fn validate_voice_commit_request(params: &VoiceEventsCommitParams) -> AppResult<()> {
    validate_voice_run_id(&params.run_id)?;
    if params.events.is_empty() {
        return Err(AppError::VoiceEventContract(
            "voice event commit must not be empty".into(),
        ));
    }
    if params.events.len() > MAX_VOICE_EVENTS {
        return Err(AppError::VoiceEventContract(format!(
            "voice event count exceeds {MAX_VOICE_EVENTS}"
        )));
    }
    let encoded_bytes = serde_json::to_vec(&params.events)?.len();
    if encoded_bytes > MAX_VOICE_EVENT_PAYLOAD_BYTES {
        return Err(AppError::VoiceEventContract(format!(
            "voice event payload exceeds {MAX_VOICE_EVENT_PAYLOAD_BYTES} bytes"
        )));
    }
    for event in &params.events {
        if event.run_id().as_str() != params.run_id {
            return Err(AppError::VoiceEventContract(format!(
                "voice event run {} does not match request run {}",
                event.run_id(),
                params.run_id
            )));
        }
        if let VoiceEvent::VoiceInterrupted { spoken_prefix, .. } = event
            && spoken_prefix.len() > MAX_SPOKEN_PREFIX_BYTES
        {
            return Err(AppError::VoiceEventContract(format!(
                "spoken prefix exceeds {MAX_SPOKEN_PREFIX_BYTES} bytes"
            )));
        }
    }
    Ok(())
}

fn validate_voice_run_id(run_id: &str) -> AppResult<()> {
    RunId::new(run_id.to_owned())
        .map(|_| ())
        .map_err(|error| AppError::VoiceEventContract(error.to_string()))
}

fn require_voice_ledger(runtime: &DaemonRuntime, run_id: &str) -> AppResult<()> {
    if std::fs::symlink_metadata(&runtime.paths.ledger_path)
        .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
    {
        return Err(AppError::RunNotFound(run_id.into()));
    }
    Ok(())
}

fn prior_thread_interruption_context(
    runtime: &DaemonRuntime,
    session_id: &str,
    prior_run_id: &str,
    expected_identity: &RunIdentity,
) -> AppResult<String> {
    RunId::new(prior_run_id.to_owned())?;
    let _state = runtime.state.lock().expect("daemon state lock poisoned");
    require_voice_ledger(runtime, prior_run_id)?;
    let ledger = SqliteLedger::open_default_readonly(&runtime.paths.default_ledger())?;
    let session = ledger.read_session(session_id)?;
    let prior = session
        .runs
        .iter()
        .find(|run| run.run_id == prior_run_id)
        .ok_or_else(|| {
            AppError::Config(format!(
                "prior interrupted run {prior_run_id} does not belong to thread session {session_id}"
            ))
        })?;
    if prior.status != RunStateName::Interrupted {
        return Err(AppError::Config(format!(
            "prior interrupted run {prior_run_id} is not interrupted"
        )));
    }
    let identity = prior.records.iter().find_map(|record| match &record.event {
        HarnessEvent::RunStarted(started) => Some(&started.identity),
        _ => None,
    });
    if identity != Some(expected_identity) {
        return Err(AppError::Config(format!(
            "prior interrupted run {prior_run_id} belongs to another profile"
        )));
    }

    let mut uncertain = BTreeSet::new();
    let mut committed = Vec::new();
    for record in &prior.records {
        match &record.event {
            HarnessEvent::ToolStarted { call_id, .. } => {
                uncertain.insert(call_id.to_string());
            }
            HarnessEvent::ToolFinished { result, .. } => {
                uncertain.remove(result.call_id.as_str());
                committed.push(format!(
                    "completed tool {}: {}",
                    result.call_id, result.summary
                ));
            }
            HarnessEvent::ToolFailed {
                call_id, reason, ..
            } => {
                uncertain.remove(call_id.as_str());
                committed.push(format!("failed tool {call_id}: {reason}"));
            }
            _ => {}
        }
    }
    let mut context = format!(
        "Prior run {prior_run_id} was interrupted. Only durable committed facts are carried forward; no provider request, worker, effect, or live turn is resumed. Prior question: {}.",
        prior.question
    );
    if !committed.is_empty() {
        context.push_str(" Committed tool facts: ");
        context.push_str(&committed.join("; "));
        context.push('.');
    }
    if !uncertain.is_empty() {
        context.push_str(" Uncertain prior effects started without a terminal fact: ");
        context.push_str(&uncertain.into_iter().collect::<Vec<_>>().join(", "));
        context.push_str(". Do not retry them automatically; require a new explicit proposal.");
    }
    Ok(context)
}

fn prior_voice_interruption_context(
    runtime: &DaemonRuntime,
    session_id: &str,
    prior_run_id: &str,
) -> AppResult<String> {
    validate_voice_run_id(prior_run_id)?;
    let _state = runtime.state.lock().expect("daemon state lock poisoned");
    require_voice_ledger(runtime, prior_run_id)?;
    let ledger = SqliteLedger::open_default_readonly(&runtime.paths.default_ledger())?;
    let session = ledger.read_session(session_id)?;
    let prior = session.runs.last().ok_or_else(|| {
        AppError::VoiceEventContract(format!("session {session_id} has no prior run"))
    })?;
    if prior.run_id != prior_run_id {
        return Err(AppError::VoiceEventContract(format!(
            "prior interrupted run {prior_run_id} is not the latest run in session {session_id}"
        )));
    }
    if matches!(
        prior.status,
        RunStateName::Running | RunStateName::CancelRequested
    ) {
        return Err(AppError::VoiceEventContract(format!(
            "prior interrupted run {prior_run_id} is not terminal"
        )));
    }
    let events = ledger.read_voice_events(prior_run_id)?;
    let pair = events
        .len()
        .checked_sub(2)
        .and_then(|index| events.get(index).zip(events.get(index.saturating_add(1))));
    let Some((spoken, interrupted)) = pair else {
        return Err(AppError::VoiceEventContract(format!(
            "prior run {prior_run_id} has no committed terminal voice interruption"
        )));
    };
    let (
        VoiceEvent::VoiceSpoken {
            run_id: spoken_run,
            turn_id: spoken_turn,
            interrupted_at: Some(sentence_index),
            ..
        },
        VoiceEvent::VoiceInterrupted {
            run_id: interrupted_run,
            turn_id: interrupted_turn,
            spoken_prefix,
            delta_index,
        },
    ) = (&spoken.event, &interrupted.event)
    else {
        return Err(AppError::VoiceEventContract(format!(
            "prior run {prior_run_id} has no committed terminal voice interruption"
        )));
    };
    if spoken_run.as_str() != prior_run_id
        || interrupted_run != spoken_run
        || interrupted_turn != spoken_turn
    {
        return Err(AppError::VoiceEventContract(format!(
            "prior run {prior_run_id} has a mismatched terminal voice interruption"
        )));
    }
    let prefix = serde_json::to_string(spoken_prefix)?;
    Ok(format!(
        "The user interrupted your spoken reply after {prefix} (assistant sentence index {sentence_index}, assistant delta index {delta_index})."
    ))
}

fn validate_voice_capture(
    run: &crate::ledger::SessionRunRecords,
    events: &[VoiceEvent],
) -> AppResult<()> {
    let first_turn_id = run.records.iter().find_map(|record| match &record.event {
        HarnessEvent::ContextBuilt { turn_id, .. } => Some(turn_id),
        _ => None,
    });
    for event in events {
        let VoiceEvent::VoiceCaptured {
            turn_id,
            transcript_sha256,
            transcript_bytes,
            ..
        } = event
        else {
            continue;
        };
        if first_turn_id != Some(turn_id) {
            return Err(AppError::VoiceEventContract(format!(
                "voice capture turn {turn_id} is not the durable first turn for run {}",
                run.run_id
            )));
        }
        let expected_hash = format!("{:x}", Sha256::digest(run.question.as_bytes()));
        let expected_bytes = u64::try_from(run.question.len()).map_err(|_| {
            AppError::VoiceEventContract("durable question length exceeds u64".into())
        })?;
        if transcript_sha256 != &expected_hash || *transcript_bytes != expected_bytes {
            return Err(AppError::VoiceEventContract(format!(
                "voice capture transcript does not match durable question for run {}",
                run.run_id
            )));
        }
    }
    Ok(())
}

fn voice_events_error(request_id: Option<String>, method: &str, error: AppError) -> Envelope {
    let code = match &error {
        AppError::RunNotFound(_) => ERROR_NOT_FOUND,
        AppError::VoiceEventContract(_) => ERROR_MALFORMED_REQUEST,
        AppError::VoiceLedgerConflict { .. } => ERROR_VOICE_EVENTS_CONFLICT,
        _ => ERROR_INTERNAL,
    };
    Envelope::error(request_id, Some(method.into()), code, error.to_string())
}

pub(super) fn durable_events_stream(
    runtime: &DaemonRuntime,
    params: &EventsStreamParams,
    limit: usize,
) -> AppResult<EventsStreamResult> {
    if std::fs::symlink_metadata(&runtime.paths.ledger_path)
        .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
    {
        return Err(AppError::RunNotFound(params.run_id.clone()));
    }
    let run = SqliteLedger::open_default_readonly(&runtime.paths.default_ledger())?
        .read_session_run(&params.run_id)?;
    let tip = u64::try_from(run.records.len())
        .map_err(|_| AppError::Config("durable event count exceeds u64".into()))?;
    let from_offset = params.from_offset.unwrap_or(tip);
    let events = run
        .records
        .into_iter()
        .skip(usize::try_from(from_offset).unwrap_or(usize::MAX))
        .take(limit)
        .enumerate()
        .map(|(index, record)| BufferedStreamEvent {
            offset: from_offset + u64::try_from(index).unwrap_or(u64::MAX),
            event: StreamEvent::Ledger { record },
        })
        .collect::<Vec<_>>();
    let next_offset = from_offset
        + u64::try_from(events.len())
            .map_err(|_| AppError::Config("durable event page exceeds u64".into()))?;
    Ok(EventsStreamResult {
        run_id: run.run_id,
        from_offset,
        next_offset,
        status: run.status,
        events,
    })
}

pub(super) fn handle_approval_decide(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: ApprovalDecideParams,
) -> Envelope {
    let attributed_actor = match params.actor.as_deref() {
        Some(actor) => match ActorId::new(actor.to_owned()) {
            Ok(actor) => Some(actor.to_string()),
            Err(error) => {
                return Envelope::error(
                    request.id,
                    Some("approval.decide".into()),
                    ERROR_MALFORMED_REQUEST,
                    error.to_string(),
                );
            }
        },
        None => None,
    };
    // This field is attribution from an already-trusted local client. The
    // run and call lookups below remain the complete approval authority gate.
    let decision_actor = match params.decision {
        ApprovalDecision::GrantSession => "tui_session_grant",
        ApprovalDecision::Grant | ApprovalDecision::Deny => {
            attributed_actor.as_deref().unwrap_or("daemon")
        }
    };
    let record = match find_run(runtime, &params.run_id) {
        Ok(record) => record,
        Err(error) => return error_response(request.id, "approval.decide", error),
    };
    let mut approvals = record.approvals.lock().expect("approvals lock poisoned");
    let status = record.status.lock().expect("run status lock poisoned");
    if record.cancel.load(Ordering::SeqCst) || status.state != RunStateName::Running {
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
    if pending.request.run_id.as_str() != record.run_id
        || pending.session_id != record.session_id
        || pending.request.call_id.as_str() != params.tool_call_id
    {
        return Envelope::error(
            request.id,
            Some("approval.decide".into()),
            ERROR_NOT_FOUND,
            format!("pending approval not found: {}", params.tool_call_id),
        );
    }
    if let Some(existing) = &pending.decision {
        let existing_actor = match &existing.outcome {
            ExternalApprovalOutcome::Granted { actor }
            | ExternalApprovalOutcome::Denied { actor, .. } => actor,
        };
        if existing.decision == params.decision && existing_actor == decision_actor {
            return Envelope::typed_response(
                request.id,
                ProtocolResponse::ApprovalDecide(CommandAcceptedResult {
                    run_id: record.run_id.clone(),
                    status: status.state,
                }),
            );
        }
        return Envelope::error(
            request.id,
            Some("approval.decide".into()),
            ERROR_NOT_FOUND,
            format!("pending approval not found: {}", params.tool_call_id),
        );
    }
    let outcome = match params.decision {
        ApprovalDecision::Grant => ExternalApprovalOutcome::Granted {
            actor: decision_actor.into(),
        },
        ApprovalDecision::GrantSession => {
            if pending.request.tool_name != SHELL_EXEC
                || pending.request.effect != EffectClass::ExternalSideEffect
            {
                return Envelope::error(
                    request.id,
                    Some("approval.decide".into()),
                    ERROR_NOT_FOUND,
                    format!("pending approval not found: {}", params.tool_call_id),
                );
            }
            runtime.install_shell_session_grant(&record.session_id);
            ExternalApprovalOutcome::Granted {
                actor: "tui_session_grant".into(),
            }
        }
        ApprovalDecision::Deny => ExternalApprovalOutcome::Denied {
            actor: decision_actor.into(),
            reason: params
                .reason
                .unwrap_or_else(|| "approval denied by daemon client".into()),
        },
    };
    // Record the answer beside the question, so the decision survives the
    // daemon exactly as the request does (#435). A failure here is reported
    // rather than swallowed: an unrecorded decision would leave the approval
    // looking unanswered forever.
    if let Err(error) =
        record_approval_decision(runtime, &record.run_id, &params.tool_call_id, &outcome)
    {
        return Envelope::error(
            request.id,
            Some("approval.decide".into()),
            ERROR_INTERNAL,
            format!("approval decision could not be recorded: {error}"),
        );
    }
    pending.decision = Some(crate::daemon::runtime::PendingApprovalDecision {
        decision: params.decision,
        outcome,
    });
    record.approval_changed.notify_all();
    drop(status);
    drop(approvals);
    Envelope::typed_response(
        request.id,
        ProtocolResponse::ApprovalDecide(CommandAcceptedResult {
            run_id: record.run_id.clone(),
            status: record.status().state,
        }),
    )
}

fn record_approval_decision(
    runtime: &DaemonRuntime,
    run_id: &str,
    call_id: &str,
    outcome: &ExternalApprovalOutcome,
) -> AppResult<()> {
    let (granted, actor, reason) = match outcome {
        ExternalApprovalOutcome::Granted { actor } => (true, actor.clone(), None),
        ExternalApprovalOutcome::Denied { actor, reason } => {
            (false, actor.clone(), Some(reason.clone()))
        }
    };
    runtime.paths.server_store()?.resolve_tool_call_approval(
        run_id,
        call_id,
        &crate::server_store::ToolCallApprovalDecision {
            granted,
            actor,
            reason,
            decided_at_ms: crate::thread_authority::now_ms(),
        },
    )?;
    Ok(())
}

pub(super) fn handle_run_cancel(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: RunCancelParams,
) -> Envelope {
    let actor = match params.actor {
        Some(actor) => match platonic_core::ActorId::new(actor) {
            Ok(actor) => actor.to_string(),
            Err(error) => {
                return Envelope::error(
                    request.id,
                    Some("run.cancel".into()),
                    ERROR_MALFORMED_REQUEST,
                    format!("invalid cancellation actor: {error}"),
                );
            }
        },
        None => "daemon".into(),
    };
    let record = match find_run(runtime, &params.run_id) {
        Ok(record) => record,
        Err(error) => return error_response(request.id, "run.cancel", error),
    };
    let cancellation = crate::server_store::RunCancellationRecord {
        run_id: record.run_id.clone(),
        actor,
        requested_at_ms: crate::thread_authority::now_ms(),
    };
    let accepted = record.request_cancel_after(|| {
        let mut store = runtime.paths.server_store()?;
        store.persist_run_cancellation(&cancellation)?;
        Ok(())
    });
    let Some(status) = (match accepted {
        Ok(status) => status,
        Err(error) => {
            return Envelope::error(
                request.id,
                Some("run.cancel".into()),
                ERROR_INTERNAL,
                error.to_string(),
            );
        }
    }) else {
        return error_response(
            request.id,
            "run.cancel",
            format!("run is not active: {}", record.run_id),
        );
    };
    Envelope::typed_response(
        request.id,
        ProtocolResponse::RunCancel(CommandAcceptedResult {
            run_id: record.run_id.clone(),
            status,
        }),
    )
}

pub(super) fn spawn_event_collector(
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

/// A failure to reach the server-wide store is internal, not a missing record.
fn error_response(request_id: Option<String>, method: &'static str, message: String) -> Envelope {
    Envelope::error(request_id, Some(method.into()), ERROR_NOT_FOUND, message)
}

#[cfg(test)]
pub(in crate::daemon::handlers) mod tests {
    use super::*;
    use crate::{
        ApprovalRequest, AssistantDeltaEvent,
        daemon::protocol::{EnvelopeKind, StreamEvent, ThreadSendResult},
        daemon::runtime::{PendingApproval, PendingApprovalDecision},
    };
    use platonic_core::{
        AgentId, ContextPack, EffectClass, HarnessEvent, Message, MessageRole, ModelName,
        PolicyDecision, ProfileId, RecordedEvent, ResultVisibility, RunId, RunIdentity, ToolCall,
        ToolCallId, ToolName, ToolProposal, ToolResult, TurnId,
    };
    use serde_json::json;
    #[cfg(target_os = "linux")]
    use std::os::unix::fs::PermissionsExt;
    use std::{
        sync::{Arc, Barrier},
        time::Duration,
    };

    #[cfg(target_os = "linux")]
    use crate::daemon::handlers::sessions::read_run_transcript;
    use crate::daemon::handlers::{
        handle_line,
        threads::{tests::*, thread_session_id},
    };
    #[cfg(target_os = "linux")]
    use crate::daemon::runtime::ShutdownIfIdleDecision;
    pub(in crate::daemon::handlers) fn response_result<T: serde::de::DeserializeOwned>(
        response: &Envelope,
    ) -> T {
        let response = serde_json::to_value(response.result.as_ref().unwrap()).unwrap();
        serde_json::from_value(response["result"].clone()).unwrap()
    }

    fn write_admission_test_config(workspace_root: &Path) -> PathBuf {
        let path = workspace_root.join("admission-test.toml");
        std::fs::write(
            &path,
            r#"[provider]
kind = "open_ai"
model = "test-model"
api_key_env = "PATH"
base_url = "http://127.0.0.1:1"
connect_timeout_ms = 50
stream_idle_timeout_ms = 50

[limits]
token_budget = 4000
max_output_tokens = 32
max_turns = 1

[tools]
enabled = ["file.read"]
"#,
        )
        .unwrap();
        path
    }

    fn seed_terminal_voice_run(
        runtime: &DaemonRuntime,
        session_id: &str,
        run_id: &str,
        create_session: bool,
        interrupted: bool,
    ) {
        let run_id = RunId::new(run_id).unwrap();
        let turn_id = TurnId::new(format!("turn_{}", run_id.as_str())).unwrap();
        let mut ledger =
            SqliteLedger::open_or_create_default(&runtime.paths.default_ledger()).unwrap();
        ledger
            .begin_session_run(session_id, &run_id, "question", create_session)
            .unwrap();
        for (seq, event) in [
            HarnessEvent::RunStarted(platonic_core::RunStartedEvent {
                run_id: run_id.clone(),
                identity: platonic_core::RunIdentity::LegacyAgent {
                    agent_id: AgentId::new("plato").unwrap(),
                },
            }),
            HarnessEvent::ContextBuilt {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                context: ContextPack {
                    token_budget: 1,
                    fragments: Vec::new(),
                },
            },
            HarnessEvent::ModelRequested {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                step: 0,
                model: ModelName::new("model").unwrap(),
            },
            HarnessEvent::ModelResponded {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                step: 0,
                output: Message {
                    role: MessageRole::Assistant,
                    content: "answer".into(),
                },
                proposed_calls: Vec::new(),
                served_model: None,
                usage: None,
            },
            HarnessEvent::RunFinished {
                run_id: run_id.clone(),
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
        ledger.finish_session_run(&run_id, "answer").unwrap();
        let spoken = VoiceEvent::VoiceSpoken {
            run_id: run_id.clone(),
            turn_id: turn_id.clone(),
            ttfa_ms: 10,
            sentence_count: 1,
            interrupted_at: interrupted.then_some(3),
        };
        let events = if interrupted {
            vec![
                spoken,
                VoiceEvent::VoiceInterrupted {
                    run_id,
                    turn_id,
                    spoken_prefix: "one \"two\"".into(),
                    delta_index: 8,
                },
            ]
        } else {
            vec![spoken]
        };
        ledger.append_voice_events(&events).unwrap();
    }

    fn seed_interrupted_profile_run(
        runtime: &DaemonRuntime,
        session_id: &str,
        run_id: &str,
        identity: RunIdentity,
    ) {
        let run_id = RunId::new(run_id).unwrap();
        let completed_call = ToolCallId::new("call_completed").unwrap();
        let uncertain_call = ToolCallId::new("call_uncertain").unwrap();
        let completed_turn = TurnId::new("turn_completed").unwrap();
        let uncertain_turn = TurnId::new("turn_uncertain").unwrap();
        let tool = ToolName::new("file.read").unwrap();
        let input = json!({"path": "committed.txt"});
        let mut ledger =
            SqliteLedger::open_or_create_default(&runtime.paths.default_ledger()).unwrap();
        ledger
            .begin_session_run(session_id, &run_id, "interrupted question", true)
            .unwrap();
        for (seq, event) in [
            HarnessEvent::RunStarted(platonic_core::RunStartedEvent {
                run_id: run_id.clone(),
                identity,
            }),
            HarnessEvent::ContextBuilt {
                run_id: run_id.clone(),
                turn_id: completed_turn.clone(),
                context: ContextPack {
                    token_budget: 1,
                    fragments: Vec::new(),
                },
            },
            HarnessEvent::ModelRequested {
                run_id: run_id.clone(),
                turn_id: completed_turn.clone(),
                step: 0,
                model: ModelName::new("model").unwrap(),
            },
            HarnessEvent::ModelResponded {
                run_id: run_id.clone(),
                turn_id: completed_turn.clone(),
                step: 0,
                output: Message {
                    role: MessageRole::Assistant,
                    content: "read the committed file".into(),
                },
                proposed_calls: vec![ToolProposal {
                    tool: tool.clone(),
                    input: input.clone(),
                }],
                served_model: None,
                usage: None,
            },
            HarnessEvent::ToolCallProposed {
                run_id: run_id.clone(),
                turn_id: completed_turn,
                call: ToolCall {
                    id: completed_call.clone(),
                    tool: tool.clone(),
                    effect: EffectClass::ReadOnly,
                    input: input.clone(),
                },
            },
            HarnessEvent::PolicyEvaluated {
                run_id: run_id.clone(),
                call_id: completed_call.clone(),
                decision: PolicyDecision::Allow,
            },
            HarnessEvent::ToolStarted {
                run_id: run_id.clone(),
                call_id: completed_call.clone(),
            },
            HarnessEvent::ToolFinished {
                run_id: run_id.clone(),
                result: ToolResult {
                    call_id: completed_call,
                    summary: "committed result".into(),
                    data: json!({"private": "not carried"}),
                    artifacts: Vec::new(),
                    visibility: ResultVisibility::Both,
                },
            },
            HarnessEvent::ContextBuilt {
                run_id: run_id.clone(),
                turn_id: uncertain_turn.clone(),
                context: ContextPack {
                    token_budget: 1,
                    fragments: Vec::new(),
                },
            },
            HarnessEvent::ModelRequested {
                run_id: run_id.clone(),
                turn_id: uncertain_turn.clone(),
                step: 1,
                model: ModelName::new("model").unwrap(),
            },
            HarnessEvent::ModelResponded {
                run_id: run_id.clone(),
                turn_id: uncertain_turn.clone(),
                step: 1,
                output: Message {
                    role: MessageRole::Assistant,
                    content: "read another file".into(),
                },
                proposed_calls: vec![ToolProposal {
                    tool: tool.clone(),
                    input: input.clone(),
                }],
                served_model: None,
                usage: None,
            },
            HarnessEvent::ToolCallProposed {
                run_id: run_id.clone(),
                turn_id: uncertain_turn,
                call: ToolCall {
                    id: uncertain_call.clone(),
                    tool,
                    effect: EffectClass::ReadOnly,
                    input,
                },
            },
            HarnessEvent::PolicyEvaluated {
                run_id: run_id.clone(),
                call_id: uncertain_call.clone(),
                decision: PolicyDecision::Allow,
            },
            HarnessEvent::ToolStarted {
                run_id: run_id.clone(),
                call_id: uncertain_call,
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
        assert_eq!(
            ledger
                .interrupt_running_session_runs("daemon restarted")
                .unwrap(),
            1
        );
    }

    #[test]
    fn prior_voice_interruption_context_is_server_derived_and_bound_to_the_latest_session_run() {
        let (_root, runtime) = bare_thread_test_runtime();
        seed_terminal_voice_run(&runtime, "session_voice", "run_prior", true, true);

        assert_eq!(
            prior_voice_interruption_context(&runtime, "session_voice", "run_prior").unwrap(),
            "The user interrupted your spoken reply after \"one \\\"two\\\"\" (assistant sentence index 3, assistant delta index 8)."
        );
        assert!(prior_voice_interruption_context(&runtime, "session_other", "run_prior").is_err());
        assert!(
            prior_voice_interruption_context(&runtime, "session_voice", "run_missing").is_err()
        );

        seed_terminal_voice_run(&runtime, "session_plain", "run_plain", true, false);
        assert!(prior_voice_interruption_context(&runtime, "session_plain", "run_plain").is_err());

        seed_terminal_voice_run(&runtime, "session_voice", "run_latest", false, false);
        assert!(prior_voice_interruption_context(&runtime, "session_voice", "run_prior").is_err());
    }

    #[test]
    fn prior_thread_interruption_is_profile_bound_and_marks_uncertain_effects() {
        let (_root, runtime) = bare_thread_test_runtime();
        let identity = RunIdentity::Profile {
            profile_id: ProfileId::new("profile_a").unwrap(),
            profile_revision: 3,
        };
        seed_interrupted_profile_run(
            &runtime,
            "session_thread_a",
            "run_interrupted",
            identity.clone(),
        );

        let context = prior_thread_interruption_context(
            &runtime,
            "session_thread_a",
            "run_interrupted",
            &identity,
        )
        .unwrap();
        assert!(context.contains("completed tool call_completed: committed result"));
        assert!(context.contains("call_uncertain"));
        assert!(context.contains("Do not retry them automatically"));
        assert!(!context.contains("not carried"));
        assert!(
            prior_thread_interruption_context(
                &runtime,
                "session_thread_a",
                "run_interrupted",
                &RunIdentity::Profile {
                    profile_id: ProfileId::new("profile_b").unwrap(),
                    profile_revision: 3,
                },
            )
            .is_err()
        );
        assert!(
            prior_thread_interruption_context(
                &runtime,
                "session_other",
                "run_interrupted",
                &identity,
            )
            .is_err()
        );
    }

    #[test]
    fn profile_home_restart_resolves_unloaded_and_send_starts_a_new_turn() {
        let (_root, runtime) = thread_test_runtime();
        let (reservation_id, thread_id) = pending_spawn(
            start_thread(
                &runtime,
                None,
                &runtime.paths.workspace_root,
                ThreadApprovalPolicy::Prompt,
            )
            .unwrap(),
        );
        grant_thread(&runtime, &reservation_id, "test");
        let authority = runtime
            .paths
            .server_store()
            .unwrap()
            .thread_authority(&thread_id)
            .unwrap()
            .unwrap();
        let profile_id = authority.profile_id.clone().unwrap();
        let identity = RunIdentity::Profile {
            profile_id: profile_id.clone(),
            profile_revision: authority.profile_revision.unwrap(),
        };
        let session_id = thread_session_id(&thread_id);
        seed_interrupted_profile_run(&runtime, &session_id, "run_before_restart", identity);

        let old_epoch = runtime.live_epoch_id();
        let restarted = DaemonRuntime::new(runtime.paths.clone());
        assert_ne!(restarted.live_epoch_id(), old_epoch);
        let resolved = handle_line(
            &restarted,
            &format!(
                r#"{{"v":2,"id":"resolve","kind":"request","method":"profile.open","params":{{"action":"resolve","profile_id":"{profile_id}"}}}}"#
            ),
        );
        let resolved: crate::daemon::protocol::ProfileOpenResult = response_result(&resolved);
        match resolved {
            crate::daemon::protocol::ProfileOpenResult::Opened {
                thread, created, ..
            } => {
                assert_eq!(thread.authority.thread_id, thread_id);
                assert!(!created);
                assert!(!thread.live.loaded);
                assert_eq!(thread.live.live_epoch_id, restarted.live_epoch_id());
            }
            result => panic!("expected resolved home, got {result:?}"),
        }

        let reached = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        restarted.set_run_execution_barriers(reached.clone(), release.clone());
        let response = temp_env::with_var("OPENROUTER_API_KEY", Some("test-key"), || {
            handle_line(
                &restarted,
                &format!(
                    r#"{{"v":2,"id":"send","kind":"request","method":"thread.send","params":{{"thread_id":"{thread_id}","controller_id":"test","prior_interrupted_run_id":"run_before_restart","message":"continue explicitly"}}}}"#
                ),
            )
        });
        assert!(matches!(
            response_result::<ThreadSendResult>(&response),
            ThreadSendResult::Started { .. }
        ));
        reached.wait();
        assert!(restarted.thread_is_loaded(&thread_id));
        let new_run = restarted
            .state
            .lock()
            .unwrap()
            .runs
            .values()
            .find(|run| run.session_id == session_id)
            .cloned()
            .unwrap();
        assert_ne!(new_run.run_id, "run_before_restart");
        assert_eq!(new_run.status().state, RunStateName::Running);
        assert_eq!(
            std::fs::metadata(
                crate::ledger::run_jsonl_path(&restarted.paths.ledger_path, &new_run.run_id)
                    .unwrap()
            )
            .unwrap()
            .len(),
            0,
            "the admitted replacement turn must not replay a prior effect before execution"
        );
        let connection = rusqlite::Connection::open(&restarted.paths.ledger_path).unwrap();
        let statuses = connection
            .prepare(
                "SELECT run_id, status FROM session_runs WHERE session_id = ?1 ORDER BY session_index",
            )
            .unwrap()
            .query_map([&session_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(statuses.len(), 2);
        assert_eq!(
            statuses[0],
            ("run_before_restart".into(), "interrupted".into())
        );
        assert_eq!(statuses[1].0, new_run.run_id);
        assert_eq!(statuses[1].1, "running");
        let canceled = cancel_run(&restarted, "cancel-replacement", &new_run.run_id);
        assert_eq!(
            response_result::<CommandAcceptedResult>(&canceled),
            CommandAcceptedResult {
                run_id: new_run.run_id.clone(),
                status: RunStateName::CancelRequested,
            }
        );
        release.wait();
        wait_for_run_terminal(&restarted, &new_run.run_id);
        assert!(restarted.thread_is_loaded(&thread_id));
        assert!(
            restarted
                .paths
                .server_store()
                .unwrap()
                .thread_stop(&thread_id)
                .unwrap()
                .is_none()
        );
    }

    fn assert_running_admission(
        runtime: &DaemonRuntime,
        run_id: &str,
        session_id: &str,
        question: &str,
    ) {
        let log_path = crate::ledger::run_jsonl_path(&runtime.paths.ledger_path, run_id).unwrap();
        assert!(log_path.is_file());
        assert_eq!(std::fs::metadata(log_path).unwrap().len(), 0);
        let connection = rusqlite::Connection::open(&runtime.paths.ledger_path).unwrap();
        let (stored_session, stored_question, stored_status) = connection
            .query_row(
                "SELECT session_id, question, status FROM session_runs WHERE run_id = ?1",
                rusqlite::params![run_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(stored_session, session_id);
        assert_eq!(stored_question, question);
        assert_eq!(stored_status, RunStateName::Running.as_str());
        let session_runs = connection
            .query_row(
                "SELECT COUNT(*) FROM session_runs WHERE session_id = ?1",
                rusqlite::params![session_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap();
        assert_eq!(session_runs, 1);
    }

    fn wait_for_run_terminal(runtime: &DaemonRuntime, run_id: &str) {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            let record = runtime
                .state
                .lock()
                .unwrap()
                .runs
                .get(run_id)
                .cloned()
                .expect("admitted run remains visible until terminal retention");
            if !matches!(
                record.status().state,
                RunStateName::Running | RunStateName::CancelRequested
            ) {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "run {run_id} did not become terminal"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn cancel_admitted_run(runtime: &DaemonRuntime, run_id: &str) {
        runtime
            .state
            .lock()
            .unwrap()
            .runs
            .get(run_id)
            .unwrap()
            .cancel
            .store(true, Ordering::SeqCst);
    }

    fn session_run_count(runtime: &DaemonRuntime) -> i64 {
        let connection = rusqlite::Connection::open(&runtime.paths.ledger_path).unwrap();
        connection
            .query_row("SELECT COUNT(*) FROM session_runs", [], |row| row.get(0))
            .unwrap()
    }

    #[test]
    fn all_async_entry_methods_admit_jsonl_and_session_row_before_receipt() {
        let (_root, base) = thread_test_runtime();
        let runtime = DaemonRuntime::new_with_server_policy(
            base.paths,
            1,
            false,
            crate::confinement::ConfinementSupport::Landlock,
        );
        let config_path = write_admission_test_config(&runtime.paths.workspace_root);

        let reached = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        runtime.set_run_execution_barriers(reached.clone(), release.clone());
        let response = handle_line(
            &runtime,
            &format!(
                r#"{{"v":2,"id":"start","kind":"request","method":"run.start","params":{{"question":"first question","config_path":"{}"}}}}"#,
                config_path.display()
            ),
        );
        assert_eq!(response.kind, EnvelopeKind::Response);
        let started: RunStartResult = response_result(&response);
        reached.wait();
        assert_running_admission(
            &runtime,
            &started.run_id,
            &started.session_id,
            "first question",
        );
        let started_root =
            crate::paths::one_shot_run_root(&runtime.paths.server_db_path, &started.run_id)
                .unwrap();
        assert!(started_root.join("scratch").is_dir());
        cancel_admitted_run(&runtime, &started.run_id);
        release.wait();
        wait_for_run_terminal(&runtime, &started.run_id);
        assert!(!started_root.exists());

        let reached = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        runtime.set_run_execution_barriers(reached.clone(), release.clone());
        let response = handle_line(
            &runtime,
            &format!(
                r#"{{"v":2,"id":"append","kind":"request","method":"message.append","params":{{"session_id":"{}","message":"follow up","config_path":"{}"}}}}"#,
                started.session_id,
                config_path.display()
            ),
        );
        assert_eq!(response.kind, EnvelopeKind::Response);
        let appended: RunStartResult = response_result(&response);
        reached.wait();
        let connection = rusqlite::Connection::open(&runtime.paths.ledger_path).unwrap();
        let (question, status) = connection
            .query_row(
                "SELECT question, status FROM session_runs WHERE run_id = ?1",
                rusqlite::params![appended.run_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .unwrap();
        assert_eq!(question, "follow up");
        assert_eq!(status, RunStateName::Running.as_str());
        assert!(
            crate::ledger::run_jsonl_path(&runtime.paths.ledger_path, &appended.run_id)
                .unwrap()
                .is_file()
        );
        let appended_root =
            crate::paths::one_shot_run_root(&runtime.paths.server_db_path, &appended.run_id)
                .unwrap();
        assert!(appended_root.join("scratch").is_dir());
        cancel_admitted_run(&runtime, &appended.run_id);
        release.wait();
        wait_for_run_terminal(&runtime, &appended.run_id);
        assert!(!appended_root.exists());

        let (spawn_id, thread_id) = pending_spawn(
            start_thread(
                &runtime,
                None,
                &runtime.paths.workspace_root,
                ThreadApprovalPolicy::Prompt,
            )
            .unwrap(),
        );
        grant_thread(&runtime, &spawn_id, "test");
        let reached = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        runtime.set_run_execution_barriers(reached.clone(), release.clone());
        let response = temp_env::with_var("OPENROUTER_API_KEY", Some("test-key"), || {
            handle_line(
                &runtime,
                &format!(
                    r#"{{"v":2,"id":"send","kind":"request","method":"thread.send","params":{{"thread_id":"{thread_id}","controller_id":"test","message":"thread question"}}}}"#
                ),
            )
        });
        assert_eq!(response.kind, EnvelopeKind::Response);
        assert!(matches!(
            response_result::<ThreadSendResult>(&response),
            ThreadSendResult::Started { .. }
        ));
        reached.wait();
        let thread_session = thread_session_id(&thread_id);
        let thread_run = runtime
            .state
            .lock()
            .unwrap()
            .runs
            .values()
            .find(|record| record.session_id == thread_session)
            .cloned()
            .expect("initial thread.send has one admitted run");
        assert_running_admission(
            &runtime,
            &thread_run.run_id,
            &thread_session,
            "thread question",
        );
        let authority = runtime
            .paths
            .server_store()
            .unwrap()
            .thread_authority(&thread_id)
            .unwrap()
            .unwrap();
        let thread_scratch = PathBuf::from(&authority.granted_paths[0].path);
        assert!(thread_scratch.is_dir());
        assert!(
            !crate::paths::one_shot_run_root(&runtime.paths.server_db_path, &thread_run.run_id,)
                .unwrap()
                .exists()
        );
        cancel_admitted_run(&runtime, &thread_run.run_id);
        release.wait();
        wait_for_run_terminal(&runtime, &thread_run.run_id);
        assert!(thread_scratch.is_dir());
    }

    #[test]
    fn admission_failures_remove_runtime_thread_and_empty_storage_reservations() {
        let (_root, base) = bare_thread_test_runtime();
        let jsonl_runtime = DaemonRuntime::new_with_server_policy(
            base.paths,
            1,
            false,
            crate::confinement::ConfinementSupport::Landlock,
        );
        let jsonl_config = write_admission_test_config(&jsonl_runtime.paths.workspace_root);
        drop(SqliteLedger::open_or_create_default(&jsonl_runtime.paths.default_ledger()).unwrap());
        let runs_path = jsonl_runtime
            .paths
            .ledger_path
            .parent()
            .unwrap()
            .join("runs");
        std::fs::write(&runs_path, "not a directory").unwrap();
        let response = handle_line(
            &jsonl_runtime,
            &format!(
                r#"{{"v":2,"id":"start","kind":"request","method":"run.start","params":{{"question":"cannot create log","config_path":"{}"}}}}"#,
                jsonl_config.display()
            ),
        );
        assert_eq!(response.error.unwrap().code, ERROR_RUN_FAILED);
        assert!(jsonl_runtime.state.lock().unwrap().runs.is_empty());
        assert_eq!(session_run_count(&jsonl_runtime), 0);
        assert_eq!(
            std::fs::read_dir(
                crate::paths::one_shot_runs_root(&jsonl_runtime.paths.server_db_path).unwrap()
            )
            .unwrap()
            .count(),
            0
        );

        let (_root, sqlite_runtime) = bare_thread_test_runtime();
        let sqlite_config = write_admission_test_config(&sqlite_runtime.paths.workspace_root);
        let active_run = RunId::new("run_already_active").unwrap();
        let mut ledger =
            SqliteLedger::open_or_create_default(&sqlite_runtime.paths.default_ledger()).unwrap();
        ledger
            .begin_session_run("session_active", &active_run, "active", true)
            .unwrap();
        drop(ledger);
        let response = handle_line(
            &sqlite_runtime,
            &format!(
                r#"{{"v":2,"id":"append","kind":"request","method":"message.append","params":{{"session_id":"session_active","message":"rejected","config_path":"{}"}}}}"#,
                sqlite_config.display()
            ),
        );
        assert_eq!(response.error.unwrap().code, ERROR_RUN_FAILED);
        assert!(sqlite_runtime.state.lock().unwrap().runs.is_empty());
        assert_eq!(session_run_count(&sqlite_runtime), 1);
        assert_eq!(
            std::fs::read_dir(
                sqlite_runtime
                    .paths
                    .ledger_path
                    .parent()
                    .unwrap()
                    .join("runs")
            )
            .unwrap()
            .count(),
            0
        );

        let (_root, base) = bare_thread_test_runtime();
        let one_shot_handoff = DaemonRuntime::new_with_server_policy(
            base.paths,
            1,
            false,
            crate::confinement::ConfinementSupport::Landlock,
        );
        let config_path = write_admission_test_config(&one_shot_handoff.paths.workspace_root);
        one_shot_handoff.fail_next_run_handoff();
        let response = handle_line(
            &one_shot_handoff,
            &format!(
                r#"{{"v":2,"id":"start","kind":"request","method":"run.start","params":{{"question":"handoff fails","config_path":"{}"}}}}"#,
                config_path.display()
            ),
        );
        assert_eq!(response.error.unwrap().code, ERROR_RUN_FAILED);
        assert!(one_shot_handoff.state.lock().unwrap().runs.is_empty());
        assert_eq!(session_run_count(&one_shot_handoff), 0);
        assert_eq!(
            std::fs::read_dir(
                crate::paths::one_shot_runs_root(&one_shot_handoff.paths.server_db_path).unwrap()
            )
            .unwrap()
            .count(),
            0
        );

        let (_root, handoff_runtime) = thread_test_runtime();
        let (spawn_id, thread_id) = pending_spawn(
            start_thread(
                &handoff_runtime,
                None,
                &handoff_runtime.paths.workspace_root,
                ThreadApprovalPolicy::Prompt,
            )
            .unwrap(),
        );
        grant_thread(&handoff_runtime, &spawn_id, "test");
        handoff_runtime.fail_next_run_handoff();
        let response = temp_env::with_var("OPENROUTER_API_KEY", Some("test-key"), || {
            handle_line(
                &handoff_runtime,
                &format!(
                    r#"{{"v":2,"id":"send","kind":"request","method":"thread.send","params":{{"thread_id":"{thread_id}","controller_id":"test","message":"handoff fails"}}}}"#
                ),
            )
        });
        assert_eq!(response.error.unwrap().code, ERROR_THREAD_SEND_FAILED);
        assert!(handoff_runtime.state.lock().unwrap().runs.is_empty());
        assert!(!handoff_runtime.has_active_thread_run(&thread_id));
        assert_eq!(
            handoff_runtime
                .thread_live_state(&thread_id)
                .current_turn_id,
            None
        );
        assert_eq!(session_run_count(&handoff_runtime), 0);
        assert!(
            handoff_runtime
                .paths
                .server_store()
                .unwrap()
                .thread_run_admissions(&handoff_runtime.paths.workspace_id)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            std::fs::read_dir(
                handoff_runtime
                    .paths
                    .ledger_path
                    .parent()
                    .unwrap()
                    .join("runs")
            )
            .unwrap()
            .count(),
            0
        );
        assert!(matches!(
            SqliteLedger::open_default_readonly(&handoff_runtime.paths.default_ledger())
                .unwrap()
                .read_session(&thread_session_id(&thread_id)),
            Err(AppError::SessionNotFound(_))
        ));
    }

    #[test]
    fn wait_true_new_and_continued_runs_clean_owned_scratch_after_failure() {
        let (_root, base) = bare_thread_test_runtime();
        let runtime = DaemonRuntime::new_with_server_policy(
            base.paths,
            1,
            false,
            crate::confinement::ConfinementSupport::Landlock,
        );
        let config_path = write_admission_test_config(&runtime.paths.workspace_root);
        let response = handle_line(
            &runtime,
            &format!(
                r#"{{"v":2,"id":"start","kind":"request","method":"run.start","params":{{"question":"synchronous question","config_path":"{}","wait":true}}}}"#,
                config_path.display()
            ),
        );
        assert_eq!(response.kind, EnvelopeKind::Error);
        assert_eq!(response.error.unwrap().code, ERROR_RUN_FAILED);
        assert_eq!(session_run_count(&runtime), 1);
        let session = SqliteLedger::open_default_readonly(&runtime.paths.default_ledger())
            .unwrap()
            .read_latest_session()
            .unwrap();
        assert_eq!(session.runs.len(), 1);
        assert_eq!(session.runs[0].question, "synchronous question");
        assert_eq!(session.runs[0].status, RunStateName::Failed);
        assert_eq!(
            session.runs[0]
                .records
                .iter()
                .filter(|record| matches!(record.event, HarnessEvent::RunStarted(_)))
                .count(),
            1
        );
        assert!(
            !crate::paths::one_shot_run_root(
                &runtime.paths.server_db_path,
                &session.runs[0].run_id,
            )
            .unwrap()
            .exists()
        );

        let response = handle_line(
            &runtime,
            &format!(
                r#"{{"v":2,"id":"append","kind":"request","method":"message.append","params":{{"session_id":"{}","message":"continued question","config_path":"{}","wait":true}}}}"#,
                session.session_id,
                config_path.display()
            ),
        );
        assert_eq!(response.kind, EnvelopeKind::Error);
        assert_eq!(response.error.unwrap().code, ERROR_RUN_FAILED);
        assert_eq!(session_run_count(&runtime), 2);
        let session = SqliteLedger::open_default_readonly(&runtime.paths.default_ledger())
            .unwrap()
            .read_latest_session()
            .unwrap();
        assert_eq!(session.runs.len(), 2);
        assert_eq!(session.runs[1].question, "continued question");
        assert_eq!(session.runs[1].status, RunStateName::Failed);
        for run in &session.runs {
            assert!(
                !crate::paths::one_shot_run_root(&runtime.paths.server_db_path, &run.run_id,)
                    .unwrap()
                    .exists()
            );
        }
        assert_eq!(
            std::fs::read_dir(runtime.paths.ledger_path.parent().unwrap().join("runs"))
                .unwrap()
                .count(),
            2
        );
    }

    #[test]
    fn one_shot_fallback_runs_unconfined_and_require_refuses_before_admission() {
        let (_root, base) = bare_thread_test_runtime();
        let fallback = DaemonRuntime::new_with_server_policy(
            base.paths,
            1,
            false,
            crate::confinement::ConfinementSupport::None,
        );
        assert_eq!(
            prepare_one_shot_confinement(
                &fallback,
                "run_fallback",
                Some(fallback.thread_confinement().unwrap()),
            )
            .unwrap(),
            (ChildConfinement::None, false)
        );
        let config_path = write_admission_test_config(&fallback.paths.workspace_root);
        let response = handle_line(
            &fallback,
            &format!(
                r#"{{"v":2,"id":"fallback","kind":"request","method":"run.start","params":{{"question":"fallback","config_path":"{}","wait":true}}}}"#,
                config_path.display()
            ),
        );
        assert_eq!(response.error.unwrap().code, ERROR_RUN_FAILED);
        assert_eq!(session_run_count(&fallback), 1);
        assert!(
            !crate::paths::one_shot_runs_root(&fallback.paths.server_db_path)
                .unwrap()
                .exists()
        );

        let (_root, base) = bare_thread_test_runtime();
        let required = DaemonRuntime::new_with_server_policy(
            base.paths,
            1,
            true,
            crate::confinement::ConfinementSupport::None,
        );
        let config_path = write_admission_test_config(&required.paths.workspace_root);
        let response = handle_line(
            &required,
            &format!(
                r#"{{"v":2,"id":"required","kind":"request","method":"run.start","params":{{"question":"required","config_path":"{}"}}}}"#,
                config_path.display()
            ),
        );
        let error = response.error.unwrap();
        assert_eq!(error.code, ERROR_RUN_FAILED);
        assert_eq!(
            error.message,
            "server policy requires confinement, but this run cannot be confined"
        );
        assert!(required.state.lock().unwrap().runs.is_empty());
        assert!(!required.paths.ledger_path.exists());
        assert!(
            !crate::paths::one_shot_runs_root(&required.paths.server_db_path)
                .unwrap()
                .exists()
        );
    }

    #[test]
    fn one_shot_landlock_selection_uses_canonical_workspace_and_owned_tmpdir() {
        let (_root, base) = bare_thread_test_runtime();
        let runtime = DaemonRuntime::new_with_server_policy(
            base.paths,
            1,
            false,
            crate::confinement::ConfinementSupport::Landlock,
        );
        let run_id = "run_landlock_selection";
        let scratch = crate::paths::one_shot_run_root(&runtime.paths.server_db_path, run_id)
            .unwrap()
            .join("scratch");

        let (confinement, owned) = prepare_one_shot_confinement(
            &runtime,
            run_id,
            Some(runtime.thread_confinement().unwrap()),
        )
        .unwrap();

        assert!(owned);
        assert_eq!(
            confinement,
            ChildConfinement::Landlock {
                readable_paths: vec![
                    runtime.paths.workspace_root.clone(),
                    scratch.canonicalize().unwrap(),
                ],
                writable_paths: vec![
                    runtime.paths.workspace_root.clone(),
                    scratch.canonicalize().unwrap(),
                ],
                scratch: scratch.canonicalize().unwrap(),
            }
        );
        remove_one_shot_run_root(&runtime.paths.server_db_path, run_id, owned).unwrap();
        assert!(!scratch.exists());
    }

    #[test]
    fn terminal_status_waits_for_collected_events_on_success_failure_and_cancellation() {
        assert_terminal_waits_for_collector(
            "success",
            Ok(RunOutcome {
                run_id: RunId::new("run_success").unwrap(),
                final_answer: "done".into(),
                completion_claim: None,
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
                completion_claim: None,
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
                completion_claim: None,
            }
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn supervised_terminal_intent_stays_nonterminal_until_cleanup_completes() {
        assert_supervised_terminal_publication(false);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn supervised_cleanup_failure_replaces_staged_success_before_publication() {
        assert_supervised_terminal_publication(true);
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
            finisher_runtime.finish_run(&finisher_record, "done".into(), None);
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
        let first_result: CommandAcceptedResult = response_result(&first);

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
        let duplicate_result: CommandAcceptedResult = response_result(&duplicate);
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
    fn attributed_run_cancel_records_only_the_first_actor() {
        let root = tempfile::tempdir().unwrap();
        let runtime = DaemonRuntime::new(crate::daemon::server::DaemonPaths {
            workspace_root: root.path().to_path_buf(),
            workspace_id: "workspace-1".into(),
            socket_path: root.path().join("agent.sock"),
            ledger_path: root.path().join("ledger.db"),
            server_db_path: root.path().join("server.db"),
        });
        let record = test_run_record("attributed_cancel");
        runtime.reserve_run(record.clone()).unwrap();

        let first = cancel_run_as(&runtime, "cancel_actor_1", &record.run_id, "remote_laptop");
        let duplicate = cancel_run_as(&runtime, "cancel_actor_2", &record.run_id, "other_actor");

        assert_eq!(first.kind, EnvelopeKind::Response);
        assert_eq!(duplicate.kind, EnvelopeKind::Response);
        let stored = runtime
            .paths
            .server_store()
            .unwrap()
            .run_cancellation(&record.run_id)
            .unwrap()
            .unwrap();
        assert_eq!(stored.run_id, record.run_id);
        assert_eq!(stored.actor, "remote_laptop");
        assert!(stored.requested_at_ms > 0);
    }

    #[test]
    fn attributed_run_cancel_fails_before_control_when_recording_fails() {
        let root = tempfile::tempdir().unwrap();
        let unusable = root.path().join("server.db");
        std::fs::create_dir(&unusable).unwrap();
        let runtime = DaemonRuntime::new(crate::daemon::server::DaemonPaths {
            workspace_root: root.path().to_path_buf(),
            workspace_id: "workspace-1".into(),
            socket_path: root.path().join("agent.sock"),
            ledger_path: root.path().join("ledger.db"),
            server_db_path: unusable,
        });
        let record = test_run_record("cancel_store_failure");
        runtime.reserve_run(record.clone()).unwrap();

        let response = cancel_run_as(
            &runtime,
            "cancel_store_failure",
            &record.run_id,
            "remote_laptop",
        );

        assert_eq!(response.kind, EnvelopeKind::Error);
        assert_eq!(response.error.unwrap().code, ERROR_INTERNAL);
        assert_eq!(record.status().state, RunStateName::Running);
        assert!(!record.cancel.load(Ordering::SeqCst));
        assert!(record.events.lock().unwrap().events.is_empty());
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
    fn grant_session_is_exact_idempotent_and_fail_closed_before_installation() {
        for (case, tool_name, effect) in [
            ("file_write", "file.write", EffectClass::WorkspaceWrite),
            ("shell_network", SHELL_EXEC, EffectClass::Network),
            ("shell_secret", SHELL_EXEC, EffectClass::SecretAccess),
            ("shell_write", SHELL_EXEC, EffectClass::WorkspaceWrite),
            (
                "computer",
                "computer.click",
                EffectClass::ExternalSideEffect,
            ),
            ("unknown", "unknown.tool", EffectClass::ExternalSideEffect),
        ] {
            let runtime = test_runtime();
            let record = test_run_record(case);
            record.approvals.lock().unwrap().insert(
                "call_1".into(),
                PendingApproval::new(
                    record.session_id.clone(),
                    test_approval_request(&record.run_id, "call_1", tool_name, effect),
                ),
            );
            runtime.reserve_run(record.clone()).unwrap();

            let response = decide_session_grant(&runtime, case, &record.run_id, "call_1");

            assert_eq!(response.kind, crate::daemon::protocol::EnvelopeKind::Error);
            assert_eq!(response.error.unwrap().code, ERROR_NOT_FOUND);
            assert_eq!(runtime.session_tool_grant_count(), 0);
            assert_eq!(record.approvals.lock().unwrap()["call_1"].decision, None);
        }

        let runtime = test_runtime();
        let record = test_run_record("exact");
        record.approvals.lock().unwrap().insert(
            "call_1".into(),
            PendingApproval::new(
                record.session_id.clone(),
                test_approval_request(
                    &record.run_id,
                    "call_1",
                    SHELL_EXEC,
                    EffectClass::ExternalSideEffect,
                ),
            ),
        );
        runtime.reserve_run(record.clone()).unwrap();

        record
            .approvals
            .lock()
            .unwrap()
            .get_mut("call_1")
            .unwrap()
            .session_id = "session_other".into();
        let mismatched_session =
            decide_session_grant(&runtime, "wrong_session", &record.run_id, "call_1");
        assert_eq!(
            mismatched_session.kind,
            crate::daemon::protocol::EnvelopeKind::Error
        );
        assert_eq!(runtime.session_tool_grant_count(), 0);
        assert_eq!(record.approvals.lock().unwrap()["call_1"].decision, None);
        record
            .approvals
            .lock()
            .unwrap()
            .get_mut("call_1")
            .unwrap()
            .session_id = record.session_id.clone();

        let mismatched_run = decide_session_grant(&runtime, "wrong_run", "run_missing", "call_1");
        let mismatched_call =
            decide_session_grant(&runtime, "wrong_call", &record.run_id, "call_missing");
        assert_eq!(
            mismatched_run.kind,
            crate::daemon::protocol::EnvelopeKind::Error
        );
        assert_eq!(
            mismatched_call.kind,
            crate::daemon::protocol::EnvelopeKind::Error
        );
        assert_eq!(runtime.session_tool_grant_count(), 0);
        assert_eq!(record.approvals.lock().unwrap()["call_1"].decision, None);

        let first = decide_session_grant(&runtime, "first", &record.run_id, "call_1");
        let duplicate = decide_session_grant(&runtime, "duplicate", &record.run_id, "call_1");
        assert_eq!(first.kind, crate::daemon::protocol::EnvelopeKind::Response);
        assert_eq!(
            duplicate.kind,
            crate::daemon::protocol::EnvelopeKind::Response
        );
        assert_eq!(runtime.session_tool_grant_count(), 1);
        assert!(runtime.has_shell_session_grant(&record.session_id));
        assert!(!runtime.has_shell_session_grant("session_other"));
        assert_eq!(
            record.approvals.lock().unwrap()["call_1"].decision,
            Some(PendingApprovalDecision {
                decision: ApprovalDecision::GrantSession,
                outcome: ExternalApprovalOutcome::Granted {
                    actor: "tui_session_grant".into()
                },
            })
        );

        let conflicting = handle_line(
            &runtime,
            &format!(
                r#"{{"v":2,"id":"conflicting","kind":"request","method":"approval.decide","params":{{"run_id":"{}","tool_call_id":"call_1","decision":"deny"}}}}"#,
                record.run_id
            ),
        );
        assert_eq!(
            conflicting.kind,
            crate::daemon::protocol::EnvelopeKind::Error
        );
        assert_eq!(runtime.session_tool_grant_count(), 1);
        assert_eq!(
            record.approvals.lock().unwrap()["call_1"]
                .decision
                .as_ref()
                .map(|decision| decision.decision),
            Some(ApprovalDecision::GrantSession)
        );
    }

    #[test]
    fn canceled_and_terminal_session_grants_reject_before_mutation() {
        for (case, state, canceled) in [
            ("cancel_requested", RunStateName::CancelRequested, true),
            ("finished", RunStateName::Finished, false),
            ("failed", RunStateName::Failed, false),
            ("canceled", RunStateName::Canceled, true),
            ("interrupted", RunStateName::Interrupted, false),
        ] {
            let runtime = test_runtime();
            let record = test_run_record(case);
            record.approvals.lock().unwrap().insert(
                "call_1".into(),
                PendingApproval::new(
                    record.session_id.clone(),
                    test_approval_request(
                        &record.run_id,
                        "call_1",
                        SHELL_EXEC,
                        EffectClass::ExternalSideEffect,
                    ),
                ),
            );
            record.status.lock().unwrap().state = state;
            record.cancel.store(canceled, Ordering::SeqCst);
            runtime
                .state
                .lock()
                .unwrap()
                .runs
                .insert(record.run_id.clone(), record.clone());

            let response = decide_session_grant(&runtime, case, &record.run_id, "call_1");

            assert_eq!(response.kind, crate::daemon::protocol::EnvelopeKind::Error);
            assert_eq!(response.error.unwrap().code, ERROR_NOT_FOUND);
            assert_eq!(runtime.session_tool_grant_count(), 0);
            assert_eq!(record.approvals.lock().unwrap()["call_1"].decision, None);
        }
    }

    #[test]
    fn session_grant_decision_linearizes_before_concurrent_cancel_without_lost_wakeup() {
        let runtime = test_runtime();
        let record = test_run_record("grant_cancel");
        runtime.reserve_run(record.clone()).unwrap();
        let waiter = spawn_shell_approval_waiter(&runtime, &record);
        wait_for_pending_approval(&record);

        let install_reached = Arc::new(Barrier::new(2));
        let install_release = Arc::new(Barrier::new(2));
        runtime
            .set_session_grant_install_barriers(install_reached.clone(), install_release.clone());
        let decision_runtime = runtime.clone();
        let decision_run_id = record.run_id.clone();
        let (decision_sender, decision_receiver) = mpsc::channel();
        let decision = thread::spawn(move || {
            decision_sender
                .send(decide_session_grant(
                    &decision_runtime,
                    "grant_cancel",
                    &decision_run_id,
                    "call_1",
                ))
                .unwrap();
        });
        install_reached.wait();

        let cancel_runtime = runtime.clone();
        let cancel_run_id = record.run_id.clone();
        let (cancel_sender, cancel_receiver) = mpsc::channel();
        let cancel = thread::spawn(move || {
            cancel_sender
                .send(cancel_run(&cancel_runtime, "cancel_race", &cancel_run_id))
                .unwrap();
        });
        assert!(matches!(
            cancel_receiver.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        install_release.wait();

        let decision_response = decision_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        let cancel_response = cancel_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        decision.join().unwrap();
        cancel.join().unwrap();
        assert_eq!(
            decision_response.kind,
            crate::daemon::protocol::EnvelopeKind::Response
        );
        assert_eq!(
            cancel_response.kind,
            crate::daemon::protocol::EnvelopeKind::Response
        );
        assert_eq!(
            waiter.join().unwrap(),
            ExternalApprovalOutcome::Granted {
                actor: "tui_session_grant".into()
            }
        );
        assert_eq!(runtime.session_tool_grant_count(), 1);
        assert_eq!(record.status().state, RunStateName::CancelRequested);
        assert_eq!(
            record.approvals.lock().unwrap()["call_1"]
                .decision
                .as_ref()
                .map(|decision| decision.decision),
            Some(ApprovalDecision::GrantSession)
        );
    }

    #[test]
    fn session_grant_decision_linearizes_before_concurrent_terminal_without_lost_wakeup() {
        let runtime = test_runtime();
        let record = test_run_record("grant_terminal");
        runtime.reserve_run(record.clone()).unwrap();
        let waiter = spawn_shell_approval_waiter(&runtime, &record);
        wait_for_pending_approval(&record);

        let install_reached = Arc::new(Barrier::new(2));
        let install_release = Arc::new(Barrier::new(2));
        runtime
            .set_session_grant_install_barriers(install_reached.clone(), install_release.clone());
        let decision_runtime = runtime.clone();
        let decision_run_id = record.run_id.clone();
        let (decision_sender, decision_receiver) = mpsc::channel();
        let decision = thread::spawn(move || {
            decision_sender
                .send(decide_session_grant(
                    &decision_runtime,
                    "grant_terminal",
                    &decision_run_id,
                    "call_1",
                ))
                .unwrap();
        });
        install_reached.wait();

        let finish_runtime = runtime.clone();
        let finish_record = record.clone();
        let (finish_sender, finish_receiver) = mpsc::channel();
        let finisher = thread::spawn(move || {
            finish_runtime.finish_run(&finish_record, "done".into(), None);
            finish_sender.send(()).unwrap();
        });
        assert!(matches!(
            finish_receiver.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        install_release.wait();

        let decision_response = decision_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        finish_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        decision.join().unwrap();
        finisher.join().unwrap();
        assert_eq!(
            decision_response.kind,
            crate::daemon::protocol::EnvelopeKind::Response
        );
        assert_eq!(
            waiter.join().unwrap(),
            ExternalApprovalOutcome::Granted {
                actor: "tui_session_grant".into()
            }
        );
        assert_eq!(runtime.session_tool_grant_count(), 1);
        assert_eq!(record.status().state, RunStateName::Finished);
        assert_eq!(
            record.approvals.lock().unwrap()["call_1"]
                .decision
                .as_ref()
                .map(|decision| decision.decision),
            Some(ApprovalDecision::GrantSession)
        );
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
                    completion_claim: None,
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
            finisher_runtime.finish_run(&finisher_record, "done".into(), None);
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
                r#"{{"v":2,"id":"{request_id}","kind":"request","method":"run.cancel","params":{{"run_id":"{run_id}"}}}}"#
            ),
        )
    }

    fn cancel_run_as(
        runtime: &DaemonRuntime,
        request_id: &str,
        run_id: &str,
        actor: &str,
    ) -> Envelope {
        handle_line(
            runtime,
            &format!(
                r#"{{"v":2,"id":"{request_id}","kind":"request","method":"run.cancel","params":{{"run_id":"{run_id}","actor":"{actor}"}}}}"#
            ),
        )
    }

    fn decide_session_grant(
        runtime: &DaemonRuntime,
        request_id: &str,
        run_id: &str,
        call_id: &str,
    ) -> Envelope {
        handle_line(
            runtime,
            &format!(
                r#"{{"v":2,"id":"{request_id}","kind":"request","method":"approval.decide","params":{{"run_id":"{run_id}","tool_call_id":"{call_id}","decision":"grant_session"}}}}"#
            ),
        )
    }

    fn test_approval_request(
        run_id: &str,
        call_id: &str,
        tool_name: &str,
        effect: EffectClass,
    ) -> ApprovalRequest {
        ApprovalRequest {
            run_id: RunId::new(run_id).unwrap(),
            call_id: ToolCallId::new(call_id).unwrap(),
            tool_name: tool_name.into(),
            effect,
            reason: "test approval required".into(),
            input_preview: Some("{}".into()),
            approval_preview: None,
            diff_preview: None,
            yolo_eligible: false,
        }
    }

    fn spawn_shell_approval_waiter(
        runtime: &DaemonRuntime,
        record: &Arc<RunRecord>,
    ) -> thread::JoinHandle<ExternalApprovalOutcome> {
        let decide = approval_handler(runtime.clone(), record.clone(), false);
        let request = test_approval_request(
            &record.run_id,
            "call_1",
            SHELL_EXEC,
            EffectClass::ExternalSideEffect,
        );
        thread::spawn(move || decide(request).unwrap())
    }

    fn wait_for_pending_approval(record: &RunRecord) {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while record.pending_approval().is_none() {
            assert!(
                std::time::Instant::now() < deadline,
                "approval did not become pending"
            );
            thread::yield_now();
        }
    }

    #[cfg(target_os = "linux")]
    fn assert_supervised_terminal_publication(cleanup_failure: bool) {
        use crate::{
            app::prepare_run,
            daemon::run_child::{
                SupervisedTestLaunch, TerminalStageBarriers, run_supervised_for_test,
            },
        };

        let root = tempfile::tempdir().unwrap();
        let root_path = root.path().to_path_buf();
        let workspace = root.path().join("w");
        std::fs::create_dir(&workspace).unwrap();
        let config_path = workspace.join("plato.toml");
        std::fs::write(
            &config_path,
            r#"[provider]
kind = "open_ai"
model = "test-model"
api_key_env = "PATH"
base_url = "http://127.0.0.1:1"

[limits]
token_budget = 4000
max_output_tokens = 64
max_turns = 1

[tools]
enabled = ["file.read"]
"#,
        )
        .unwrap();
        let runtime = DaemonRuntime::new(crate::daemon::server::DaemonPaths {
            workspace_root: workspace.canonicalize().unwrap(),
            workspace_id: "terminal-order".into(),
            socket_path: root.path().join("a.sock"),
            ledger_path: root
                .path()
                .join("state/platonic/workspaces/terminal-order/ledger.db"),
            server_db_path: root.path().join("state/platonic/server.db"),
        });
        let case = if cleanup_failure {
            "cleanup"
        } else {
            "success"
        };
        let run_id = RunId::new(format!("run_terminal_{case}")).unwrap();
        let record = Arc::new(RunRecord::new(
            run_id.to_string(),
            format!("session_terminal_{case}"),
            runtime.paths.ledger_path.clone(),
        ));
        runtime.reserve_run(record.clone()).unwrap();
        let (prepared, recorder) = prepare_run(&RunOptions {
            question: "prove terminal publication ordering".into(),
            config_path: Some(config_path),
            overrides: Default::default(),
            ledger: RunLedger::DefaultSqlite(runtime.paths.default_ledger()),
            workspace_root: runtime.paths.workspace_root.clone(),
            approval_mode: ApprovalMode::Deny { actor: "test" },
            run_id: Some(run_id.clone()),
            session: Some(RunSession::Fresh {
                session_id: record.session_id.clone(),
            }),
            event_sender: None,
            stream_to_stderr: false,
            cancel: None,
            voice_interruption_context: None,
        })
        .unwrap();

        let turn_id = TurnId::new("turn_terminal").unwrap();
        let nonterminal_events = vec![
            HarnessEvent::RunStarted(platonic_core::RunStartedEvent {
                run_id: run_id.clone(),
                identity: platonic_core::RunIdentity::LegacyAgent {
                    agent_id: AgentId::new("plato").unwrap(),
                },
            }),
            HarnessEvent::ContextBuilt {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                context: ContextPack {
                    token_budget: 4000,
                    fragments: vec![],
                },
            },
            HarnessEvent::ModelRequested {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                step: 0,
                model: ModelName::new("test-model").unwrap(),
            },
            HarnessEvent::ModelResponded {
                run_id: run_id.clone(),
                turn_id,
                step: 0,
                output: Message {
                    role: MessageRole::Assistant,
                    content: "done".into(),
                },
                proposed_calls: vec![],
                served_model: None,
                usage: None,
            },
        ];
        let nonterminal = nonterminal_events
            .into_iter()
            .enumerate()
            .map(|(index, event)| {
                json!({
                    "kind": "record",
                    "request_id": index + 1,
                    "operation": {"operation": "event", "event": event}
                })
            })
            .map(|message| format!("printf '%s\\n' '{message}'\nIFS= read -r _\n"))
            .collect::<String>();
        let terminal = json!({
            "kind": "record",
            "request_id": 5,
            "operation": {
                "operation": "finish",
                "run_id": run_id,
                "final_answer": "done"
            }
        });
        let result = json!({
            "kind": "result",
            "request_id": 6,
            "result": {
                "status": "finished",
                "outcome": {"run_id": run_id, "final_answer": "done"}
            }
        });
        let fifo = root.path().join("r");
        let descendant_pid_path = root.path().join("d.pid");
        let fixture = root.path().join("child");
        let stderr = if cleanup_failure {
            "printf 'cleanup drain failure\\n' >&2"
        } else {
            ":"
        };
        std::fs::write(
            &fixture,
            format!(
                r#"#!/bin/sh
mkfifo '{fifo}'
/bin/sh -c 'IFS= read -r _ < "$1"' sh '{fifo}' &
printf '%s\n' "$!" > '{descendant_pid_path}'
IFS= read -r _
printf '{{"kind":"ready","request_id":0,"pid":%s}}\n' "$$"
IFS= read -r _
{nonterminal}
printf '%s\n' '{terminal}'
IFS= read -r _
printf 'release\n' > '{fifo}'
wait
rm -f '{fifo}'
printf '%s\n' '{result}'
IFS= read -r _
{stderr}
"#,
                fifo = fifo.display(),
                descendant_pid_path = descendant_pid_path.display(),
            ),
        )
        .unwrap();
        std::fs::set_permissions(&fixture, std::fs::Permissions::from_mode(0o700)).unwrap();

        let (event_sender, event_receiver) = mpsc::channel();
        let event_collector = spawn_event_collector(record.clone(), event_receiver);
        let terminal_reached = Arc::new(Barrier::new(2));
        let terminal_release = Arc::new(Barrier::new(2));
        let (ready_sender, ready_receiver) = mpsc::channel();
        let (finished_sender, finished_receiver) = mpsc::channel();
        let worker_runtime = runtime.clone();
        let worker_record = record.clone();
        let reached = terminal_reached.clone();
        let release = terminal_release.clone();
        let worker = thread::spawn(move || {
            let completion = run_supervised_for_test(
                prepared,
                recorder,
                ApprovalMode::Deny { actor: "test" },
                event_sender,
                Arc::new(std::sync::atomic::AtomicBool::new(false)),
                None,
                SupervisedTestLaunch {
                    executable: fixture,
                    ready_child: ready_sender,
                    terminal_stage_barriers: TerminalStageBarriers { reached, release },
                },
            );
            let outcome = finish_run_after_event_collection(
                &worker_runtime,
                &worker_record,
                completion,
                event_collector,
            );
            finished_sender.send(outcome).unwrap();
        });

        let child_pid = ready_receiver.recv_timeout(Duration::from_secs(2)).unwrap();
        terminal_reached.wait();
        let descendant_pid = std::fs::read_to_string(&descendant_pid_path)
            .unwrap()
            .trim()
            .parse::<u32>()
            .unwrap();
        let before = read_run_transcript(&runtime.paths.default_ledger(), run_id.as_str()).unwrap();
        assert_eq!(before.status, RunStateName::Running);
        assert_eq!(before.final_answer, None);
        assert_eq!(record.status().state, RunStateName::Running);
        assert_eq!(
            runtime.shutdown_if_idle(),
            ShutdownIfIdleDecision::RefusedActive
        );
        let before_records = SqliteLedger::open_default_readonly(&runtime.paths.default_ledger())
            .unwrap()
            .read_session_run(run_id.as_str())
            .unwrap();
        assert_eq!(terminal_count(&before_records.records), 0);

        terminal_release.wait();
        let outcome = finished_receiver
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        worker.join().unwrap();
        assert!(!Path::new(&format!("/proc/{child_pid}")).exists());
        assert!(!Path::new(&format!("/proc/{descendant_pid}")).exists());
        assert!(!fifo.exists());

        let after = read_run_transcript(&runtime.paths.default_ledger(), run_id.as_str()).unwrap();
        let after_records = SqliteLedger::open_default_readonly(&runtime.paths.default_ledger())
            .unwrap()
            .read_session_run(run_id.as_str())
            .unwrap();
        assert_eq!(
            terminal_count(&after_records.records),
            1,
            "outcome={outcome:?} transcript={after:?} records={:?}",
            after_records.records
        );
        let buffered_terminals = record
            .events
            .lock()
            .unwrap()
            .events
            .iter()
            .filter(|event| {
                matches!(
                    &event.event,
                    StreamEvent::Ledger {
                        record: RecordedEvent {
                            event: HarnessEvent::RunFinished { .. }
                                | HarnessEvent::RunFailed { .. },
                            ..
                        }
                    }
                )
            })
            .count();
        assert_eq!(buffered_terminals, 1);

        if cleanup_failure {
            let reason = match outcome {
                Err(AppError::SupervisedRun(reason)) => reason,
                unexpected => panic!("unexpected cleanup outcome: {unexpected:?}"),
            };
            assert!(reason.contains("cleanup drain failure"));
            assert_eq!(after.status, RunStateName::Failed);
            assert_eq!(after.final_answer, None);
            assert!(matches!(
                after_records.records.last().map(|record| &record.event),
                Some(HarnessEvent::RunFailed { reason: recorded, .. }) if recorded == &reason
            ));
        } else {
            assert_eq!(outcome.unwrap().final_answer, "done");
            assert_eq!(after.status, RunStateName::Finished);
            assert_eq!(after.final_answer.as_deref(), Some("done"));
            assert!(matches!(
                after_records.records.last().map(|record| &record.event),
                Some(HarnessEvent::RunFinished { .. })
            ));
        }
        assert_eq!(runtime.shutdown_if_idle(), ShutdownIfIdleDecision::Shutdown);

        drop(before_records);
        drop(after_records);
        drop(record);
        drop(runtime);
        drop(root);
        assert!(!root_path.exists());
    }

    #[cfg(target_os = "linux")]
    fn terminal_count(records: &[RecordedEvent]) -> usize {
        records
            .iter()
            .filter(|record| {
                matches!(
                    record.event,
                    HarnessEvent::RunFinished { .. } | HarnessEvent::RunFailed { .. }
                )
            })
            .count()
    }

    fn stream_run(runtime: &DaemonRuntime, request_id: &str, run_id: &str) -> EventsStreamResult {
        let response = handle_line(
            runtime,
            &format!(
                r#"{{"v":2,"id":"{request_id}","kind":"request","method":"events.stream","params":{{"run_id":"{run_id}","from_offset":0,"limit":128}}}}"#
            ),
        );
        assert_eq!(
            response.kind,
            crate::daemon::protocol::EnvelopeKind::Response
        );
        response_result(&response)
    }

    fn test_runtime() -> DaemonRuntime {
        DaemonRuntime::new(crate::daemon::server::DaemonPaths {
            workspace_root: PathBuf::from("/tmp/workspace"),
            workspace_id: "workspace-1".into(),
            socket_path: PathBuf::from("/tmp/agent.sock"),
            ledger_path: PathBuf::from("/tmp/agent.db"),
            server_db_path: PathBuf::from("/tmp/platonic-server.db"),
        })
    }

    pub(in crate::daemon::handlers) fn test_run_record(case: &str) -> Arc<RunRecord> {
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
    fn context_compaction_uses_v2_protocol_and_preserves_the_typed_ledger_event() {
        let runtime = DaemonRuntime::new(crate::daemon::server::DaemonPaths {
            workspace_root: PathBuf::from("/tmp/workspace"),
            workspace_id: "workspace-1".into(),
            socket_path: PathBuf::from("/tmp/agent.sock"),
            ledger_path: PathBuf::from("/tmp/agent.db"),
            server_db_path: PathBuf::from("/tmp/platonic-server.db"),
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
                v: 2,
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

        assert_eq!(response.v, 2);
        let result: EventsStreamResult = response_result(&response);
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
}
