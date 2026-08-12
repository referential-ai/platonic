use crate::{
    ApprovalRequest, AssistantDeltaEvent, RunOutcome,
    app::PreparedRun,
    tools::{ThreadSpawnToolInput, ThreadSpawnToolOutput},
};
use platonic_core::{HarnessEvent, RecordedEvent, RunId};
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
                agent_id: "worker".into(),
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
}
