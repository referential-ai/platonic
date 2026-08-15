use super::{
    messages::{ApprovalReply, ChildMessage, ChildRunResult, ParentMessage, RecordOperation},
    supervisor::OUTPUT_DRAIN_TIMEOUT,
};
use crate::{
    AppError, AppResult, ApprovalMode, ApprovalRequest, AssistantDeltaEvent, RunEvent,
    app::{ExternalApprovalOutcome, run_prepared_question},
    ledger::RunEventRecorder,
    tool_catalog::{THREAD_ANSWER, THREAD_RETURN, THREAD_SPAWN},
    tools::{
        LogicalReadRequest, LogicalReadToolHandler, LogicalReadToolOutput, ParentAnswerToolHandler,
        ParentAnswerToolInput, ParentAnswerToolOutput, ThreadReturnToolHandler,
        ThreadReturnToolInput, ThreadReturnToolOutput, ThreadSpawnToolHandler,
        ThreadSpawnToolInput, ThreadSpawnToolOutput,
    },
};
use platonic_core::{HarnessEvent, RecordedEvent, RunId};
use std::{
    io::{self, BufRead, BufReader, BufWriter, Write},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::Instant,
};

pub fn run_stdio_child() -> AppResult<()> {
    crate::confinement::apply_child()?;
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
    let logical_read = prepared.has_logical_read_tool().then(|| {
        let logical_read_rpc = rpc.clone();
        LogicalReadToolHandler::new(move |request| logical_read_rpc.logical_read(request))
    });
    let thread_return = prepared.has_tool(THREAD_RETURN).then(|| {
        let thread_return_rpc = rpc.clone();
        ThreadReturnToolHandler::new(move |input, call_id| {
            thread_return_rpc.thread_return(call_id, input)
        })
    });
    let parent_answer = prepared.has_tool(THREAD_ANSWER).then(|| {
        let parent_answer_rpc = rpc.clone();
        ParentAnswerToolHandler::new(move |input, call_id| {
            parent_answer_rpc.parent_answer(call_id, input)
        })
    });
    let outcome = run_prepared_question(
        prepared,
        &mut recorder,
        ApprovalMode::external_with_actor("daemon", move |request| approval_rpc.approval(request)),
        Some(event_sender),
        false,
        Some(cancel),
        crate::tools::RunToolHandlers {
            thread_spawn,
            logical_read,
            thread_return,
            parent_answer,
        },
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

    fn logical_read(&self, request: LogicalReadRequest) -> AppResult<LogicalReadToolOutput> {
        let _transaction = self.transaction.lock().expect("child RPC lock poisoned");
        let request_id = self.next_request_id();
        self.send(&ChildMessage::LogicalRead {
            request_id,
            request,
        })?;
        match self.next_reply(request_id)? {
            ParentMessage::LogicalRead { output, .. } => Ok(output),
            _ => Err(AppError::SupervisedRun(
                "parent sent a non-read reply to a profile read".into(),
            )),
        }
    }

    fn thread_return(
        &self,
        call_id: platonic_core::ToolCallId,
        input: ThreadReturnToolInput,
    ) -> AppResult<ThreadReturnToolOutput> {
        let _transaction = self.transaction.lock().expect("child RPC lock poisoned");
        let request_id = self.next_request_id();
        self.send(&ChildMessage::ThreadReturn {
            request_id,
            call_id,
            input,
        })?;
        match self.next_reply(request_id)? {
            ParentMessage::ThreadReturn { output, .. } => Ok(output),
            _ => Err(AppError::SupervisedRun(
                "parent sent a non-return reply to thread.return".into(),
            )),
        }
    }

    fn parent_answer(
        &self,
        call_id: platonic_core::ToolCallId,
        input: ParentAnswerToolInput,
    ) -> AppResult<ParentAnswerToolOutput> {
        let _transaction = self.transaction.lock().expect("child RPC lock poisoned");
        let request_id = self.next_request_id();
        self.send(&ChildMessage::ParentAnswer {
            request_id,
            call_id,
            input,
        })?;
        match self.next_reply(request_id)? {
            ParentMessage::ParentAnswer { output, .. } => Ok(output),
            _ => Err(AppError::SupervisedRun(
                "parent sent a non-answer reply to thread.answer".into(),
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
            }
            | ParentMessage::LogicalRead {
                request_id: reply_id,
                ..
            }
            | ParentMessage::ThreadReturn {
                request_id: reply_id,
                ..
            }
            | ParentMessage::ParentAnswer {
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
    #[ignore = "launched through the supervised child transport fixture"]
    fn supervised_stdio_child_fixture() {
        run_stdio_child().unwrap();
    }
}
