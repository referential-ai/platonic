use crate::{
    ApprovalRequest, AssistantDeltaEvent, RunOutcome,
    app::PreparedRun,
    tools::{
        LogicalReadRequest, LogicalReadToolOutput, ParentAnswerToolInput, ParentAnswerToolOutput,
        ThreadReturnToolInput, ThreadReturnToolOutput, ThreadSpawnToolInput, ThreadSpawnToolOutput,
    },
};
use platonic_core::{HarnessEvent, RecordedEvent, RunId, ToolCallId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub(super) enum ParentMessage {
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
    LogicalRead {
        request_id: u64,
        output: LogicalReadToolOutput,
    },
    ThreadReturn {
        request_id: u64,
        output: ThreadReturnToolOutput,
    },
    ParentAnswer {
        request_id: u64,
        output: ParentAnswerToolOutput,
    },
    Cancel,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "decision")]
pub(super) enum ApprovalReply {
    Granted { actor: String },
    Denied { actor: String, reason: String },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub(super) enum ChildMessage {
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
    LogicalRead {
        request_id: u64,
        request: LogicalReadRequest,
    },
    ThreadReturn {
        request_id: u64,
        call_id: ToolCallId,
        input: ThreadReturnToolInput,
    },
    ParentAnswer {
        request_id: u64,
        call_id: ToolCallId,
        input: ParentAnswerToolInput,
    },
    Result {
        request_id: u64,
        result: ChildRunResult,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "operation")]
pub(super) enum RecordOperation {
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
pub(super) enum ChildRunResult {
    Finished { outcome: RunOutcome },
    Canceled,
    Failed { error: String },
}
impl RecordOperation {
    pub(super) fn run_id(&self) -> &RunId {
        match self {
            Self::Event { event } => event.run_id(),
            Self::Finish { run_id, .. } | Self::Fail { run_id, .. } => run_id,
        }
    }

    pub(super) fn event(&self) -> HarnessEvent {
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

#[cfg(test)]
mod tests {
    use super::*;

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
                cwd: "/tmp/workspace".into(),
                model: None,
                reasoning_effort: None,
                approval_policy: None,
                toolset: Some(vec!["file.read".into()]),
                repositories: None,
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
                assert_eq!(input.cwd, "/tmp/workspace");
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
    fn spawn_edge_rpc_preserves_host_call_ids_and_typed_results() {
        let returned = ChildMessage::ThreadReturn {
            request_id: 8,
            call_id: ToolCallId::new("call-return").unwrap(),
            input: ThreadReturnToolInput {
                kind: crate::tools::ThreadReturnToolKind::Question,
                payload: "which format?".into(),
                artifact_refs: vec!["artifact_1".into()],
            },
        };
        let encoded = serde_json::to_string(&returned).unwrap();
        assert!(matches!(
            serde_json::from_str::<ChildMessage>(&encoded).unwrap(),
            ChildMessage::ThreadReturn { request_id: 8, call_id, input }
                if call_id.as_str() == "call-return"
                    && input.kind == crate::tools::ThreadReturnToolKind::Question
                    && input.artifact_refs == ["artifact_1"]
        ));

        let answered = ChildMessage::ParentAnswer {
            request_id: 9,
            call_id: ToolCallId::new("call-answer").unwrap(),
            input: ParentAnswerToolInput {
                child_thread_id: "thread-child".into(),
                kind: crate::tools::ParentAnswerToolKind::FollowUp,
                payload: "use JSON".into(),
            },
        };
        let encoded = serde_json::to_string(&answered).unwrap();
        assert!(matches!(
            serde_json::from_str::<ChildMessage>(&encoded).unwrap(),
            ChildMessage::ParentAnswer { request_id: 9, call_id, input }
                if call_id.as_str() == "call-answer"
                    && input.child_thread_id == "thread-child"
                    && input.kind == crate::tools::ParentAnswerToolKind::FollowUp
        ));

        for reply in [
            ParentMessage::ThreadReturn {
                request_id: 8,
                output: ThreadReturnToolOutput::Delivered {
                    message_id: "return-1".into(),
                    replayed: true,
                },
            },
            ParentMessage::ParentAnswer {
                request_id: 9,
                output: ParentAnswerToolOutput::Rejected {
                    code: "target_denied".into(),
                    reason: "not an immediate child".into(),
                },
            },
        ] {
            let encoded = serde_json::to_string(&reply).unwrap();
            assert!(matches!(
                serde_json::from_str::<ParentMessage>(&encoded).unwrap(),
                ParentMessage::ThreadReturn { request_id: 8, .. }
                    | ParentMessage::ParentAnswer { request_id: 9, .. }
            ));
        }
    }

    #[test]
    fn logical_read_rpc_preserves_typed_request_and_denial() {
        let child = ChildMessage::LogicalRead {
            request_id: 9,
            request: LogicalReadRequest::Profile(crate::tools::ProfileReadInput {
                profile_id: None,
                revision: Some(3),
                cursor: Some("2".into()),
                limit: Some(1),
            }),
        };
        let encoded = serde_json::to_string(&child).unwrap();
        assert!(matches!(
            serde_json::from_str::<ChildMessage>(&encoded).unwrap(),
            ChildMessage::LogicalRead {
                request_id: 9,
                request: LogicalReadRequest::Profile(crate::tools::ProfileReadInput {
                    revision: Some(3),
                    limit: Some(1),
                    ..
                }),
            }
        ));

        let parent = ParentMessage::LogicalRead {
            request_id: 9,
            output: LogicalReadToolOutput::error(
                crate::tools::LogicalReadErrorCode::MembershipDenied,
                "denied",
            ),
        };
        let encoded = serde_json::to_string(&parent).unwrap();
        assert!(matches!(
            serde_json::from_str::<ParentMessage>(&encoded).unwrap(),
            ParentMessage::LogicalRead {
                request_id: 9,
                output: LogicalReadToolOutput::Error {
                    code: crate::tools::LogicalReadErrorCode::MembershipDenied,
                    ..
                },
            }
        ));
    }
}
