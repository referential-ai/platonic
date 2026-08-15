use super::control::shutting_down_response;
use crate::{
    AppError, AppResult,
    daemon::{
        protocol::{
            ApprovalDecisionName, ERROR_INTERNAL, ERROR_MALFORMED_REQUEST, ERROR_NOT_FOUND,
            ERROR_SESSIONS_LIST_FAILED, Envelope, ModelIdentityStatus, ProtocolErrorCode,
            ProtocolResponse, RunStateName, SessionApprovalProfileSetParams,
            SessionApprovalProfileSetResult, SessionSummary, SessionsListResult,
            TranscriptReadParams, TranscriptReadResult, TypedRun, TypedTranscript,
            TypedTranscriptEntry,
        },
        runtime::DaemonRuntime,
    },
    ledger::{SessionRunRecords, SqliteLedger},
    paths::DefaultSqlitePath,
    replay::{format_readback, format_session_readback},
};
use platonic_core::{HarnessEvent, ReadbackEntry, RecordedEvent, RunReadback};

const LATEST_QUESTION_MAX_CHARS: usize = 120;

pub(super) fn handle_session_approval_profile_set(
    runtime: &DaemonRuntime,
    request: Envelope,
    params: SessionApprovalProfileSetParams,
) -> Envelope {
    if runtime.shutdown_accepted() {
        return shutting_down_response(request.id, "session.approval_profile.set");
    }
    let exists = if runtime.has_runtime_session(&params.session_id) {
        Ok(true)
    } else {
        crate::ledger::default_sqlite_session_status(
            &runtime.paths.default_ledger(),
            Some(&params.session_id),
        )
        .map(|status| status.is_some())
        .or_else(|error| match error {
            AppError::SessionNotFound(_) | AppError::NoSqliteRuns | AppError::NoSqliteSessions => {
                Ok(false)
            }
            error => Err(error),
        })
    };
    match exists {
        Ok(true) => {}
        Ok(false) => {
            return Envelope::error(
                request.id,
                Some("session.approval_profile.set".into()),
                ERROR_NOT_FOUND,
                format!("session not found: {}", params.session_id),
            );
        }
        Err(_) => {
            return Envelope::error(
                request.id,
                Some("session.approval_profile.set".into()),
                ERROR_INTERNAL,
                "session approval profile could not be updated",
            );
        }
    }
    runtime.set_approval_profile(&params.session_id, params.profile);
    Envelope::typed_response(
        request.id,
        ProtocolResponse::SessionApprovalProfileSet(SessionApprovalProfileSetResult {
            session_id: params.session_id,
            profile: params.profile,
        }),
    )
}

pub(super) fn handle_sessions_list(runtime: &DaemonRuntime, request: Envelope) -> Envelope {
    match session_summaries(runtime) {
        Ok(sessions) => Envelope::typed_response(
            request.id,
            ProtocolResponse::SessionsList(SessionsListResult { sessions }),
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
                first_question: session.first_question,
                updated_at_ms: session.updated_at_ms,
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
                first_question: String::new(),
                updated_at_ms: 0,
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

pub(super) fn handle_transcript_read(
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
            Envelope::typed_response(request.id, ProtocolResponse::TranscriptRead(transcript))
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
        .cloned();
    match record.and_then(|record| record.pending_approval()) {
        Some(snapshot) => Some(snapshot),
        // The run is not loaded, which is the state after a restart. The
        // approval outlived its daemon, so read it from disk (#435).
        None => restored_pending_approval(runtime, run_id),
    }
}

fn restored_pending_approval(
    runtime: &DaemonRuntime,
    run_id: &str,
) -> Option<crate::daemon::protocol::PendingApprovalSnapshot> {
    let store = runtime.paths.server_store().ok()?;
    let approval = store
        .pending_tool_call_approvals()
        .ok()?
        .into_iter()
        .find(|approval| approval.run_id == run_id)?;
    Some(crate::daemon::protocol::PendingApprovalSnapshot {
        run_id: approval.run_id,
        tool_call_id: approval.call_id,
        tool_name: approval.tool_name,
        effect: approval.effect,
        reason: Some(approval.reason),
        input_preview: approval.input_preview,
        approval_preview: approval.approval_preview,
        diff_preview: approval.diff_preview,
    })
}

pub(super) fn read_run_transcript(
    path: &DefaultSqlitePath,
    run_id: &str,
) -> AppResult<TranscriptReadResult> {
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
        completion_claim: None,
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
        completion_claim: None,
    })
}

fn typed_run(run: &SessionRunRecords, readback_entries: Vec<ReadbackEntry>) -> TypedRun {
    TypedRun {
        run_id: run.run_id.clone(),
        session_index: run.session_index,
        status: run.status,
        model_status: model_status(&run.records),
        entries: typed_entries(&run.question, readback_entries),
    }
}

pub(in crate::daemon) fn typed_entries_for_run(
    run: &SessionRunRecords,
) -> AppResult<Vec<TypedTranscriptEntry>> {
    let readback = RunReadback::from_events(&run.records)?;
    Ok(typed_entries(&run.question, readback.entries))
}

fn model_status(records: &[RecordedEvent]) -> Option<ModelIdentityStatus> {
    records.iter().rev().find_map(|record| match &record.event {
        HarnessEvent::ModelRequested { model, .. } => Some(ModelIdentityStatus::Requested {
            model: model.to_string(),
        }),
        HarnessEvent::ModelResponded { served_model, .. } => Some(ModelIdentityStatus::Responded {
            served_model: served_model.as_ref().map(ToString::to_string),
        }),
        _ => None,
    })
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

fn transcript_error_code(error: &AppError) -> ProtocolErrorCode {
    match error {
        AppError::RunNotFound(_)
        | AppError::SessionNotFound(_)
        | AppError::NoSqliteRuns
        | AppError::NoSqliteSessions => ERROR_NOT_FOUND,
        _ => ERROR_INTERNAL,
    }
}

pub(super) fn latest_session_id(runtime: &DaemonRuntime) -> Result<String, String> {
    crate::ledger::latest_default_sqlite_session_id(&runtime.paths.default_ledger()).map_err(
        |error| match error {
            crate::AppError::NoSqliteSessions | crate::AppError::NoSqliteRuns => {
                "no previous session exists".into()
            }
            error => error.to_string(),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daemon::protocol::StreamEvent;
    use platonic_core::AgentId;
    use platonic_core::{
        ActorId, ContextFragment, ContextLane, EffectClass, HarnessEvent, Message, MessageRole,
        ModelName, RecordedEvent, ResultVisibility, RunId, ToolCall, ToolCallId, ToolName,
        ToolResult, TurnId,
    };
    use serde_json::json;

    use crate::daemon::handlers::{
        handle_request,
        registry::tests::workspace_request,
        runs::tests::{response_result, test_run_record},
        runs::{MAX_EVENT_LIMIT, durable_events_stream},
        threads::tests::{bare_thread_test_runtime, thread_test_runtime},
    };
    use crate::daemon::protocol::{ApprovalProfile, DaemonStatusResult, EventsStreamParams};
    #[test]
    fn transcript_read_is_identical_for_jsonl_and_legacy_sqlite_events() {
        let (_root, runtime) = bare_thread_test_runtime();
        let location = runtime.paths.default_ledger();
        let run_id = RunId::new("run_parity").unwrap();
        let turn_id = TurnId::new("turn_parity").unwrap();
        let events = vec![
            HarnessEvent::RunStarted(platonic_core::RunStartedEvent {
                run_id: run_id.clone(),
                identity: platonic_core::RunIdentity::LegacyAgent {
                    agent_id: AgentId::new("plato").unwrap(),
                },
            }),
            HarnessEvent::ContextBuilt {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                context: platonic_core::ContextPack {
                    fragments: vec![],
                    token_budget: 4_000,
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
                    content: "byte-faithful answer".into(),
                },
                proposed_calls: vec![],
                served_model: None,
                usage: None,
            },
            HarnessEvent::RunFinished {
                run_id: run_id.clone(),
            },
        ];
        let mut legacy = SqliteLedger::open_or_create_default(&location).unwrap();
        legacy
            .begin_session_run("session_parity", &run_id, "parity question", true)
            .unwrap();
        for (seq, event) in events.iter().cloned().enumerate() {
            legacy
                .append(
                    run_id.as_str(),
                    &RecordedEvent {
                        seq: u64::try_from(seq).unwrap(),
                        occurred_at_ms: u64::try_from(seq).unwrap(),
                        event,
                    },
                )
                .unwrap();
        }
        legacy
            .finish_session_run(&run_id, "byte-faithful answer")
            .unwrap();
        drop(legacy);
        let sqlite_transcript = read_run_transcript(&location, run_id.as_str()).unwrap();

        let mut jsonl =
            crate::ledger::EventRecorder::create_default_jsonl(&location, &run_id).unwrap();
        for event in events.iter().cloned() {
            jsonl.record(event).unwrap();
        }
        drop(jsonl);

        assert_eq!(
            read_run_transcript(&location, run_id.as_str()).unwrap(),
            sqlite_transcript
        );
        let durable = durable_events_stream(
            &runtime,
            &EventsStreamParams {
                run_id: run_id.to_string(),
                from_offset: Some(0),
                limit: Some(MAX_EVENT_LIMIT),
            },
            MAX_EVENT_LIMIT,
        )
        .unwrap();
        assert_eq!(durable.next_offset, 5);
        assert_eq!(durable.status, RunStateName::Finished);
        assert_eq!(
            durable
                .events
                .into_iter()
                .map(|buffered| match buffered.event {
                    StreamEvent::Ledger { record } => record.event,
                    event => panic!("durable stream returned transient event: {event:?}"),
                })
                .collect::<Vec<_>>(),
            events
        );
    }

    #[test]
    fn session_profile_mutation_and_status_use_the_exact_live_session() {
        let (_root, runtime) = thread_test_runtime();
        let record = test_run_record("profile");
        runtime.reserve_run(record.clone()).unwrap();

        let updated = handle_request(
            &runtime,
            workspace_request(
                "profile-on",
                "session.approval_profile.set",
                json!({"session_id": record.session_id, "profile": "yolo"}),
            ),
        );
        let updated: SessionApprovalProfileSetResult = response_result(&updated);
        assert_eq!(updated.session_id, record.session_id);
        assert_eq!(updated.profile, ApprovalProfile::Yolo);

        let status = handle_request(
            &runtime,
            workspace_request(
                "profile-status",
                "daemon.status",
                json!({"session_id": record.session_id, "config_path": null}),
            ),
        );
        let status: DaemonStatusResult = response_result(&status);
        assert_eq!(
            status.session.session_id.as_deref(),
            Some(record.session_id.as_str())
        );
        assert_eq!(status.trust.approval_profile, ApprovalProfile::Yolo);
        assert_eq!(
            runtime.approval_profile("session_other"),
            ApprovalProfile::Prompt
        );

        let missing = handle_request(
            &runtime,
            workspace_request(
                "profile-missing",
                "session.approval_profile.set",
                json!({"session_id": "session_missing", "profile": "yolo"}),
            ),
        );
        assert_eq!(missing.error.unwrap().code, ERROR_NOT_FOUND);
    }

    /// A workspace can be created, listed and inspected end to end, keeps a
    /// stable identity when it moves, and is reported broken rather than
    /// omitted when its directory vanishes (P021).
    #[test]
    fn restored_pending_approval_is_reported_when_the_run_is_not_loaded() {
        let (_root, runtime) = thread_test_runtime();
        let store = runtime.paths.server_store().unwrap();
        store
            .persist_tool_call_approval(&crate::server_store::ToolCallApprovalRecord {
                run_id: "run_restored".into(),
                call_id: "call_restored".into(),
                session_id: "session_restored".into(),
                tool_name: "shell_exec".into(),
                effect: platonic_core::EffectClass::ExternalSideEffect,
                reason: "writes outside the workspace".into(),
                input_preview: Some("git push".into()),
                approval_preview: Some("shell_exec: git push".into()),
                diff_preview: None,
                requested_at_ms: 4_200,
                decision: None,
            })
            .unwrap();
        drop(store);

        // Nothing is loaded: this is exactly the post-restart state.
        assert!(runtime.state.lock().unwrap().runs.is_empty());
        let snapshot = runtime_pending_approval(&runtime, "run_restored")
            .expect("restored approval should be reported");
        assert_eq!(snapshot.run_id, "run_restored");
        assert_eq!(snapshot.tool_call_id, "call_restored");
        assert_eq!(snapshot.tool_name, "shell_exec");
        assert_eq!(
            snapshot.effect,
            platonic_core::EffectClass::ExternalSideEffect
        );
        assert_eq!(
            snapshot.reason.as_deref(),
            Some("writes outside the workspace")
        );
        assert_eq!(snapshot.input_preview.as_deref(), Some("git push"));

        // A decided approval is no longer waiting on anyone.
        runtime
            .paths
            .server_store()
            .unwrap()
            .resolve_tool_call_approval(
                "run_restored",
                "call_restored",
                &crate::server_store::ToolCallApprovalDecision {
                    granted: false,
                    actor: "stdin".into(),
                    reason: Some("no".into()),
                    decided_at_ms: 4_300,
                },
            )
            .unwrap();
        assert!(runtime_pending_approval(&runtime, "run_restored").is_none());
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
    fn model_status_follows_latest_durable_request_or_response() {
        let run_id = RunId::new("run_1").unwrap();
        let turn_id = TurnId::new("turn_1").unwrap();
        let record = |seq, event| RecordedEvent {
            seq,
            occurred_at_ms: seq,
            event,
        };
        let mut records = vec![record(
            0,
            HarnessEvent::ModelRequested {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                step: 0,
                model: ModelName::new("~openai/gpt-latest").unwrap(),
            },
        )];
        assert_eq!(
            model_status(&records),
            Some(ModelIdentityStatus::Requested {
                model: "~openai/gpt-latest".into()
            })
        );

        records.push(record(
            1,
            HarnessEvent::ModelResponded {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                step: 0,
                output: Message {
                    role: MessageRole::Assistant,
                    content: "tool call".into(),
                },
                proposed_calls: vec![],
                served_model: Some(ModelName::new("openai/gpt-5.2-2026-08-01").unwrap()),
                usage: None,
            },
        ));
        assert_eq!(
            model_status(&records),
            Some(ModelIdentityStatus::Responded {
                served_model: Some("openai/gpt-5.2-2026-08-01".into())
            })
        );

        records.push(record(
            2,
            HarnessEvent::ModelRequested {
                run_id: run_id.clone(),
                turn_id: TurnId::new("turn_2").unwrap(),
                step: 1,
                model: ModelName::new("~openai/gpt-latest").unwrap(),
            },
        ));
        assert!(matches!(
            model_status(&records),
            Some(ModelIdentityStatus::Requested { .. })
        ));

        records.push(record(
            3,
            HarnessEvent::ModelResponded {
                run_id,
                turn_id: TurnId::new("turn_2").unwrap(),
                step: 1,
                output: Message {
                    role: MessageRole::Assistant,
                    content: "done".into(),
                },
                proposed_calls: vec![],
                served_model: None,
                usage: None,
            },
        ));
        assert_eq!(
            model_status(&records),
            Some(ModelIdentityStatus::Responded { served_model: None })
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
                    served_model: None,
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
