use platonic_core::EffectClass;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

pub const PROTOCOL_VERSION: u32 = 1;

pub const ERROR_DAEMON_SHUTTING_DOWN: &str = "daemon_shutting_down";
pub const ERROR_MALFORMED_REQUEST: &str = "malformed_request";
pub const ERROR_LAGGED: &str = "lagged";
pub const ERROR_INTERNAL: &str = "internal_error";
pub const ERROR_ISSUE_PREP_FAILED: &str = "issue_prep_failed";
pub const ERROR_NOT_FOUND: &str = "not_found";
pub const ERROR_OVERLOAD: &str = "overload";
pub const ERROR_RUN_FAILED: &str = "run_failed";
pub const ERROR_SESSIONS_LIST_FAILED: &str = "sessions_list_failed";
pub const ERROR_UNSUPPORTED_METHOD: &str = "unsupported_method";
pub const ERROR_UNSUPPORTED_VERSION: &str = "unsupported_version";
pub const ERROR_WORKSPACE_MISMATCH: &str = "workspace_mismatch";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStateName {
    Running,
    Finished,
    Failed,
    Canceled,
    CancelRequested,
    Interrupted,
}

impl RunStateName {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Finished => "finished",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
            Self::CancelRequested => "cancel_requested",
            Self::Interrupted => "interrupted",
        }
    }
}

impl fmt::Display for RunStateName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.pad(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvelopeKind {
    Request,
    Response,
    Event,
    Error,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Envelope {
    pub v: u32,
    pub id: Option<String>,
    pub kind: EnvelopeKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ProtocolError>,
}

impl Envelope {
    pub fn response(id: Option<String>, method: Option<String>, result: Value) -> Self {
        Self {
            v: PROTOCOL_VERSION,
            id,
            kind: EnvelopeKind::Response,
            method,
            params: None,
            result: Some(result),
            error: None,
        }
    }

    pub fn response_from<T: Serialize>(
        id: Option<String>,
        method: Option<String>,
        result: T,
    ) -> Self {
        Self::response(
            id,
            method,
            serde_json::to_value(result).expect("protocol result serializes"),
        )
    }

    pub fn error(
        id: Option<String>,
        method: Option<String>,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            v: PROTOCOL_VERSION,
            id,
            kind: EnvelopeKind::Error,
            method,
            params: None,
            result: None,
            error: Some(ProtocolError {
                code: code.into(),
                message: message.into(),
            }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolError {
    pub code: String,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HelloParams {
    pub workspace_root: String,
    pub workspace_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HelloResult {
    pub daemon_version: String,
    pub workspace_id: String,
    pub ledger_path: String,
    pub capabilities: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunStartParams {
    pub question: String,
    #[serde(default)]
    pub config_path: Option<String>,
    #[serde(default)]
    pub wait: Option<bool>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RunStartResult {
    pub run_id: String,
    pub session_id: String,
    pub ledger_path: String,
    pub status: RunStateName,
    pub final_answer: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageAppendParams {
    pub message: String,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub config_path: Option<String>,
    #[serde(default)]
    pub wait: Option<bool>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IssuePrepStartParams {
    pub input: String,
    #[serde(default)]
    pub config_path: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IssuePrepStartResult {
    pub run_dir: String,
    pub outcome: IssuePrepResult,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum IssuePrepResult {
    Candidate { markdown: String },
    Blocked { stage: String, reasons: Vec<String> },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventsStreamParams {
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_offset: Option<u64>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventsStreamResult {
    pub run_id: String,
    pub from_offset: u64,
    pub next_offset: u64,
    pub status: RunStateName,
    pub events: Vec<Value>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalDecideParams {
    pub run_id: String,
    pub tool_call_id: String,
    pub decision: String,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunCancelParams {
    pub run_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommandAcceptedResult {
    pub run_id: String,
    pub status: RunStateName,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShutdownIfIdleResultName {
    Shutdown,
    RefusedActive,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ShutdownIfIdleResult {
    pub result: ShutdownIfIdleResultName,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SessionsListResult {
    pub sessions: Vec<SessionSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SessionSummary {
    pub session_id: String,
    pub run_id: String,
    pub status: RunStateName,
    pub latest_question: String,
    pub ledger_path: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TranscriptReadParams {
    #[serde(default)]
    pub run_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TranscriptReadResult {
    pub run_id: String,
    pub status: RunStateName,
    pub final_answer: Option<String>,
    pub transcript: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub typed: Option<TypedTranscript>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_approval: Option<PendingApprovalSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PendingApprovalSnapshot {
    pub run_id: String,
    pub tool_call_id: String,
    pub tool_name: String,
    pub effect: EffectClass,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_preview: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_preview: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diff_preview: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TypedTranscript {
    pub runs: Vec<TypedRun>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TypedRun {
    pub run_id: String,
    pub session_index: u64,
    pub status: RunStateName,
    pub entries: Vec<TypedTranscriptEntry>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecisionName {
    Granted,
    Denied,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum TypedTranscriptEntry {
    User {
        text: String,
    },
    Assistant {
        text: String,
    },
    ToolCall {
        call_id: String,
        tool: String,
        input: Value,
    },
    ToolResult {
        call_id: String,
        summary: String,
    },
    Approval {
        call_id: String,
        decision: ApprovalDecisionName,
        actor_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    PolicyDenied {
        call_id: String,
        reason: String,
    },
    ToolFailed {
        call_id: String,
        error: String,
    },
}

pub fn decode_request(line: &str) -> Result<Envelope, Box<Envelope>> {
    let envelope = serde_json::from_str::<Envelope>(line).map_err(|error| {
        Box::new(Envelope::error(
            None,
            None,
            ERROR_MALFORMED_REQUEST,
            format!("request is not a valid protocol envelope: {error}"),
        ))
    })?;

    if envelope.v != PROTOCOL_VERSION {
        return Err(Box::new(Envelope::error(
            envelope.id,
            envelope.method,
            ERROR_UNSUPPORTED_VERSION,
            format!("unsupported protocol version: {}", envelope.v),
        )));
    }
    if envelope.kind != EnvelopeKind::Request {
        return Err(Box::new(Envelope::error(
            envelope.id,
            envelope.method,
            ERROR_MALFORMED_REQUEST,
            "envelope kind must be request",
        )));
    }
    if envelope.method.is_none() {
        return Err(Box::new(Envelope::error(
            envelope.id,
            None,
            ERROR_MALFORMED_REQUEST,
            "request method is required",
        )));
    }

    Ok(envelope)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn run_state_names_keep_wire_values() {
        let cases = [
            (RunStateName::Running, "running"),
            (RunStateName::Finished, "finished"),
            (RunStateName::Failed, "failed"),
            (RunStateName::Canceled, "canceled"),
            (RunStateName::CancelRequested, "cancel_requested"),
            (RunStateName::Interrupted, "interrupted"),
        ];

        for (state, wire_value) in cases {
            assert_eq!(state.as_str(), wire_value);
            assert_eq!(serde_json::to_value(state).unwrap(), wire_value);
            assert_eq!(
                serde_json::from_value::<RunStateName>(wire_value.into()).unwrap(),
                state
            );
        }
    }

    #[test]
    fn issue_prep_result_keeps_typed_wire_shape() {
        let result = IssuePrepStartResult {
            run_dir: "/work/.plato/issue-prep/run_1".into(),
            outcome: IssuePrepResult::Blocked {
                stage: "review".into(),
                reasons: vec!["acceptance is not testable".into()],
            },
        };

        let wire = serde_json::to_value(&result).unwrap();

        assert_eq!(
            wire,
            json!({
                "run_dir": "/work/.plato/issue-prep/run_1",
                "outcome": {
                    "status": "blocked",
                    "stage": "review",
                    "reasons": ["acceptance is not testable"]
                }
            })
        );
        assert_eq!(
            serde_json::from_value::<IssuePrepStartResult>(wire).unwrap(),
            result
        );
    }

    #[test]
    fn decodes_request_envelope() {
        let envelope = decode_request(
            r#"{"v":1,"id":"req_1","kind":"request","method":"hello","params":{"workspace_root":"/tmp/work","workspace_id":"work-1234"}}"#,
        )
        .unwrap();

        assert_eq!(envelope.id.as_deref(), Some("req_1"));
        assert_eq!(envelope.method.as_deref(), Some("hello"));
    }

    #[test]
    fn rejects_unsupported_version_with_typed_error() {
        let error =
            decode_request(r#"{"v":2,"id":"req_1","kind":"request","method":"hello","params":{}}"#)
                .unwrap_err();

        assert_eq!(error.kind, EnvelopeKind::Error);
        assert_eq!(
            error.error.unwrap().code,
            ERROR_UNSUPPORTED_VERSION.to_string()
        );
    }

    #[test]
    fn response_serializes_without_request_params() {
        let response = Envelope::response(
            Some("req_1".into()),
            Some("hello".into()),
            serde_json::json!({"workspace_id":"work-1234"}),
        );

        let raw = serde_json::to_string(&response).unwrap();

        assert!(raw.contains(r#""kind":"response""#));
        assert!(!raw.contains("params"));
    }

    #[test]
    fn typed_transcript_keeps_exact_wire_shape_and_both_compat_directions() {
        let current = TranscriptReadResult {
            run_id: "run_1".into(),
            status: RunStateName::Finished,
            final_answer: Some("done".into()),
            transcript: "legacy replay".into(),
            typed: Some(TypedTranscript {
                runs: vec![TypedRun {
                    run_id: "run_1".into(),
                    session_index: 0,
                    status: RunStateName::Finished,
                    entries: vec![
                        TypedTranscriptEntry::User {
                            text: "do work".into(),
                        },
                        TypedTranscriptEntry::Assistant {
                            text: "working".into(),
                        },
                        TypedTranscriptEntry::ToolCall {
                            call_id: "call_1".into(),
                            tool: "file.write".into(),
                            input: json!({"path": "out.txt", "content": "done"}),
                        },
                        TypedTranscriptEntry::ToolResult {
                            call_id: "call_1".into(),
                            summary: "wrote out.txt".into(),
                        },
                        TypedTranscriptEntry::Approval {
                            call_id: "call_1".into(),
                            decision: ApprovalDecisionName::Granted,
                            actor_id: "human_1".into(),
                            reason: None,
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
                    ],
                }],
            }),
            pending_approval: None,
        };

        let wire = serde_json::to_value(&current).unwrap();
        assert_eq!(
            wire,
            json!({
                "run_id": "run_1",
                "status": "finished",
                "final_answer": "done",
                "transcript": "legacy replay",
                "typed": {
                    "runs": [{
                        "run_id": "run_1",
                        "session_index": 0,
                        "status": "finished",
                        "entries": [
                            {"kind": "user", "text": "do work"},
                            {"kind": "assistant", "text": "working"},
                            {
                                "kind": "tool_call",
                                "call_id": "call_1",
                                "tool": "file.write",
                                "input": {"path": "out.txt", "content": "done"}
                            },
                            {
                                "kind": "tool_result",
                                "call_id": "call_1",
                                "summary": "wrote out.txt"
                            },
                            {
                                "kind": "approval",
                                "call_id": "call_1",
                                "decision": "granted",
                                "actor_id": "human_1"
                            },
                            {
                                "kind": "approval",
                                "call_id": "call_2",
                                "decision": "denied",
                                "actor_id": "human_2",
                                "reason": "not now"
                            },
                            {
                                "kind": "policy_denied",
                                "call_id": "call_3",
                                "reason": "secret access denied"
                            },
                            {
                                "kind": "tool_failed",
                                "call_id": "call_4",
                                "error": "tool crashed"
                            }
                        ]
                    }]
                }
            })
        );

        #[derive(Deserialize)]
        struct LegacyTranscriptReadResult {
            run_id: String,
            status: RunStateName,
            final_answer: Option<String>,
            transcript: String,
        }

        let legacy_client: LegacyTranscriptReadResult =
            serde_json::from_value(wire).expect("legacy clients ignore typed");
        assert_eq!(legacy_client.run_id, "run_1");
        assert_eq!(legacy_client.status, RunStateName::Finished);
        assert_eq!(legacy_client.final_answer.as_deref(), Some("done"));
        assert_eq!(legacy_client.transcript, "legacy replay");

        let current_client: TranscriptReadResult = serde_json::from_value(json!({
            "run_id": "run_1",
            "status": "finished",
            "final_answer": "done",
            "transcript": "legacy replay"
        }))
        .expect("current clients decode typed-less daemon responses");
        assert_eq!(current_client.typed, None);
        assert_eq!(current_client.pending_approval, None);
    }

    #[test]
    fn pending_approval_snapshot_keeps_exact_additive_wire_shape() {
        let current = TranscriptReadResult {
            run_id: "run_1".into(),
            status: RunStateName::Running,
            final_answer: None,
            transcript: "partial replay".into(),
            typed: None,
            pending_approval: Some(PendingApprovalSnapshot {
                run_id: "run_1".into(),
                tool_call_id: "call_1".into(),
                tool_name: "file.write".into(),
                effect: EffectClass::WorkspaceWrite,
                reason: Some("file.write requires approval".into()),
                input_preview: Some(r#"{"path":"out.txt"}"#.into()),
                approval_preview: Some("write out.txt".into()),
                diff_preview: Some("--- a/out.txt\n+++ b/out.txt\n".into()),
            }),
        };

        let wire = serde_json::to_value(&current).unwrap();
        assert_eq!(
            wire,
            json!({
                "run_id": "run_1",
                "status": "running",
                "final_answer": null,
                "transcript": "partial replay",
                "pending_approval": {
                    "run_id": "run_1",
                    "tool_call_id": "call_1",
                    "tool_name": "file.write",
                    "effect": "workspace_write",
                    "reason": "file.write requires approval",
                    "input_preview": "{\"path\":\"out.txt\"}",
                    "approval_preview": "write out.txt",
                    "diff_preview": "--- a/out.txt\n+++ b/out.txt\n"
                }
            })
        );

        #[derive(Deserialize)]
        struct LegacyTranscriptReadResult {
            run_id: String,
            status: RunStateName,
            transcript: String,
        }

        let decoded: TranscriptReadResult = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(decoded, current);

        let legacy: LegacyTranscriptReadResult = serde_json::from_value(wire).unwrap();
        assert_eq!(legacy.run_id, "run_1");
        assert_eq!(legacy.status, RunStateName::Running);
        assert_eq!(legacy.transcript, "partial replay");

        let minimal = serde_json::to_value(PendingApprovalSnapshot {
            run_id: "run_2".into(),
            tool_call_id: "call_2".into(),
            tool_name: "shell.exec".into(),
            effect: EffectClass::ExternalSideEffect,
            reason: None,
            input_preview: None,
            approval_preview: None,
            diff_preview: None,
        })
        .unwrap();
        assert_eq!(
            minimal,
            json!({
                "run_id": "run_2",
                "tool_call_id": "call_2",
                "tool_name": "shell.exec",
                "effect": "external_side_effect"
            })
        );
    }
}
