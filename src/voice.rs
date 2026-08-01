//! Root-owned composition from app run events to the audio IO leaf.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::Duration,
};

use plato_audio::{
    CaptureConfig, CaptureDeviceInfo, CaptureError, CaptureMetrics, CaptureReport, CaptureWorker,
    CaptureWorkerShutdown, KokoroConfig, KokoroMetrics, KokoroMetricsReader, KokoroProvenance,
    KokoroSynthesizer, PlaybackConfig, PlaybackDeviceInfo, PlaybackMetrics, PlaybackReport,
    Sentence, SentenceCutter, SttError, SynthError, SynthWorker, SynthWorkerError,
    SynthWorkerShutdown, SynthWorkerStartError, Transcript, WhisperConfig, WhisperMetrics,
    WhisperMetricsReader, WhisperProvenance, WhisperRecognizer,
};
use platonic_core::{HarnessEvent, RunId, TurnId};
use serde::Serialize;
use thiserror::Error;

use crate::{AppError, AssistantDeltaEvent, RunEvent, RunOptions, RunOutcome};

/// Root composition failures while interpreting existing run events.
#[derive(Debug, Error)]
pub enum VoiceError {
    /// Assistant deltas and their durable response boundary disagreed.
    #[error("assistant narration event contract failed: {reason}")]
    EventContract {
        /// Bounded contract diagnostic.
        reason: String,
    },
    /// Warm model synthesis failed.
    #[error(transparent)]
    Synthesis(#[from] SynthError),
    /// The persistent stream, resampling plan, or synth thread could not start.
    #[error(transparent)]
    WorkerStart(#[from] SynthWorkerStartError),
    /// Sentence admission or the running worker failed.
    #[error(transparent)]
    Worker(#[from] SynthWorkerError),
    /// Resident Whisper setup failed.
    #[error(transparent)]
    Recognition(#[from] SttError),
    /// Persistent input, endpointing, or recognition failed.
    #[error(transparent)]
    Capture(#[from] CaptureError),
    /// This session was opened without the explicit capture path.
    #[error("voice session has no capture worker")]
    CaptureUnavailable,
    /// The voice session was explicitly shut down.
    #[error("voice session is closed")]
    SessionClosed,
}

/// Failures from the app run or its root-owned narration composition.
#[derive(Debug, Error)]
pub enum VoiceRunError {
    /// A caller had already reserved the run-event channel for another owner.
    #[error("narrated runs require exclusive ownership of RunOptions.event_sender")]
    EventSenderAlreadySet,
    /// The app run failed independently of audio composition.
    #[error(transparent)]
    Run(#[from] AppError),
    /// Audio composition failed and canceled the app run.
    #[error(transparent)]
    Voice(#[from] VoiceError),
    /// The synchronous app run panicked in its scoped thread.
    #[error("run_question panicked while narration was consuming events")]
    RunThreadPanicked,
}

/// One sentence and its measured device handoff.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct NarratedSentenceReport {
    /// Exact trimmed sentence passed to Kokoro.
    pub sentence: String,
    /// Sentence-acceptance through first audible device frame timing.
    pub playback: PlaybackReport,
}

/// Bounded proof of sentence order and warm resource reuse for one app run.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct NarrationReport {
    /// Sentences in exact accepted and callback playback order.
    pub sentences: Vec<NarratedSentenceReport>,
    /// Resident model reuse counters after the run.
    pub kokoro_metrics: KokoroMetrics,
    /// Persistent output reuse counters after the run.
    pub playback_metrics: PlaybackMetrics,
}

/// Existing app outcome paired with root-owned narration proof.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NarratedRunOutcome {
    /// Unmodified app run result.
    pub run: RunOutcome,
    /// Audio-only observation outside the durable harness ledger.
    pub narration: NarrationReport,
}

/// One final voice question paired with the existing narrated run outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapturedRunOutcome {
    /// Input device, endpoint, transcript, and latency observation.
    pub capture: CaptureReport,
    /// Unmodified existing run result and AU2 spoken-answer observation.
    pub narrated: NarratedRunOutcome,
}

/// Joined input and output ownership for a root voice session.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct VoiceSessionShutdown {
    /// Present only when the session was opened with explicit input capture.
    pub capture: Option<CaptureWorkerShutdown>,
    /// AU2 synthesis worker and persistent playback teardown.
    pub synthesis: SynthWorkerShutdown,
}

/// Warm Kokoro engine and persistent cpal stream reused across narrated runs.
pub struct VoiceSession {
    provenance: KokoroProvenance,
    kokoro_metrics: KokoroMetricsReader,
    worker: Option<SynthWorker>,
    whisper_provenance: Option<WhisperProvenance>,
    whisper_metrics: Option<WhisperMetricsReader>,
    capture: Option<CaptureWorker>,
}

impl VoiceSession {
    /// Loads the pinned model and opens the output device before any app run.
    pub fn open(kokoro: KokoroConfig, playback: PlaybackConfig) -> Result<Self, VoiceError> {
        let synthesizer = KokoroSynthesizer::load(kokoro)?;
        let provenance = synthesizer.provenance().clone();
        let kokoro_metrics = synthesizer.metrics_reader();
        let worker = SynthWorker::spawn(synthesizer, playback)?;
        Ok(Self {
            provenance,
            kokoro_metrics,
            worker: Some(worker),
            whisper_provenance: None,
            whisper_metrics: None,
            capture: None,
        })
    }

    /// Opens AU2 output plus one resident CUDA recognizer and persistent input stream.
    pub fn open_with_capture(
        kokoro: KokoroConfig,
        playback: PlaybackConfig,
        whisper: WhisperConfig,
        capture_config: CaptureConfig,
    ) -> Result<Self, VoiceError> {
        let mut session = Self::open(kokoro, playback)?;
        let recognizer = WhisperRecognizer::load(whisper)?;
        let whisper_provenance = recognizer.provenance().clone();
        let whisper_metrics = recognizer.metrics_reader();
        let capture = CaptureWorker::open(capture_config, recognizer)?;
        session.whisper_provenance = Some(whisper_provenance);
        session.whisper_metrics = Some(whisper_metrics);
        session.capture = Some(capture);
        Ok(session)
    }

    /// Returns exact model and runtime provenance captured at warm load.
    pub fn provenance(&self) -> &KokoroProvenance {
        &self.provenance
    }

    /// Returns the live host, output device, format, and buffer request.
    pub fn device_info(&self) -> &PlaybackDeviceInfo {
        self.worker
            .as_ref()
            .expect("open voice session retains its worker")
            .device_info()
    }

    /// Returns resident Whisper artifact and CUDA runtime identity when enabled.
    pub fn recognizer_provenance(&self) -> Option<&WhisperProvenance> {
        self.whisper_provenance.as_ref()
    }

    /// Returns the live input device and negotiated native format when enabled.
    pub fn capture_device_info(&self) -> Option<&CaptureDeviceInfo> {
        self.capture.as_ref().map(CaptureWorker::device_info)
    }

    /// Reads resident recognizer reuse counters when capture is enabled.
    pub fn recognizer_metrics(&self) -> Option<WhisperMetrics> {
        self.whisper_metrics
            .as_ref()
            .map(WhisperMetricsReader::snapshot)
    }

    /// Reads persistent input, VAD, conversion, and overflow counters when enabled.
    pub fn capture_metrics(&self) -> Option<CaptureMetrics> {
        self.capture.as_ref().map(CaptureWorker::metrics)
    }

    /// Closes admission, drains accepted audio, and joins the synth worker.
    pub fn shutdown(mut self) -> Result<VoiceSessionShutdown, VoiceError> {
        let capture = self.capture.take().map(CaptureWorker::shutdown);
        let synthesis = self
            .worker
            .take()
            .ok_or(VoiceError::SessionClosed)?
            .shutdown()
            .map_err(|failure| VoiceError::Worker(SynthWorkerError::Failed(failure)))?;
        Ok(VoiceSessionShutdown { capture, synthesis })
    }

    /// Captures one final question, then reuses the existing run and AU2 answer path.
    pub fn capture_question(
        &mut self,
        options: RunOptions,
        timeout: Duration,
    ) -> Result<CapturedRunOutcome, VoiceRunError> {
        if options.event_sender.is_some() {
            return Err(VoiceRunError::EventSenderAlreadySet);
        }
        let capture = self
            .capture
            .as_ref()
            .ok_or(VoiceError::CaptureUnavailable)?
            .capture(timeout)
            .map_err(VoiceError::from)?;
        let options = options_for_transcript(options, &capture.transcript)?;
        let narrated = self.run_question(options)?;
        Ok(CapturedRunOutcome { capture, narrated })
    }

    /// Drives the existing synchronous app run while narrating its event stream.
    pub fn run_question(
        &mut self,
        mut options: RunOptions,
    ) -> Result<NarratedRunOutcome, VoiceRunError> {
        if options.event_sender.is_some() {
            return Err(VoiceRunError::EventSenderAlreadySet);
        }
        let cancel = options
            .cancel
            .get_or_insert_with(|| Arc::new(AtomicBool::new(false)))
            .clone();
        let (sender, receiver) = mpsc::channel();
        options.event_sender = Some(sender);
        let worker = self.worker.as_ref().ok_or(VoiceError::SessionClosed)?;

        std::thread::scope(|scope| {
            let run = scope.spawn(move || crate::app::run_question(options));
            let mut stream = AssistantTextStream::default();
            let mut sentences = Vec::new();
            let mut voice_error = None;

            loop {
                match receiver.recv_timeout(Duration::from_millis(1)) {
                    Ok(event) => {
                        let accepted = match stream.accept(event) {
                            Ok(accepted) => accepted,
                            Err(error) => {
                                voice_error = Some(error);
                                cancel.store(true, Ordering::Release);
                                break;
                            }
                        };
                        for sentence in accepted {
                            match worker.accept(sentence, Arc::clone(&cancel)) {
                                Ok(admission) => {
                                    sentences.extend(admission.completed.into_iter().map(
                                        |report| NarratedSentenceReport {
                                            sentence: report.sentence,
                                            playback: report.playback,
                                        },
                                    ));
                                }
                                Err(error) => {
                                    voice_error = Some(VoiceError::Worker(error));
                                    cancel.store(true, Ordering::Release);
                                    break;
                                }
                            }
                        }
                        if voice_error.is_some() {
                            break;
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        if let Err(failure) = worker.check_health() {
                            voice_error =
                                Some(VoiceError::Worker(SynthWorkerError::Failed(failure)));
                            cancel.store(true, Ordering::Release);
                            break;
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }

            let run_result = run.join().map_err(|_| VoiceRunError::RunThreadPanicked)?;
            let audio_result = worker.wait_until_idle();
            if let Some(error) = voice_error {
                return Err(VoiceRunError::Voice(error));
            }
            let run = run_result?;
            stream.finish()?;
            let reports = audio_result.map_err(|failure| {
                VoiceRunError::Voice(VoiceError::Worker(SynthWorkerError::Failed(failure)))
            })?;
            sentences.extend(reports.into_iter().map(|report| NarratedSentenceReport {
                sentence: report.sentence,
                playback: report.playback,
            }));
            Ok(NarratedRunOutcome {
                run,
                narration: NarrationReport {
                    sentences,
                    kokoro_metrics: self.kokoro_metrics.snapshot(),
                    playback_metrics: worker.playback_metrics(),
                },
            })
        })
    }
}

pub(crate) fn options_for_transcript(
    mut options: RunOptions,
    transcript: &Transcript,
) -> Result<RunOptions, VoiceError> {
    if !transcript.is_final {
        return Err(contract_error(
            "voice input bridge requires one final transcript",
        ));
    }
    options.question.clone_from(&transcript.text);
    Ok(options)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ResponseKey {
    run_id: RunId,
    turn_id: TurnId,
    step: u32,
}

struct PendingResponse {
    key: ResponseKey,
    next_delta_index: u64,
    text: String,
    cutter: SentenceCutter,
}

#[derive(Default)]
struct AssistantTextStream {
    pending: Option<PendingResponse>,
    last_committed: Option<ResponseKey>,
}

impl AssistantTextStream {
    fn accept(&mut self, event: RunEvent) -> Result<Vec<Sentence>, VoiceError> {
        match event {
            RunEvent::AssistantDelta(delta) => self.accept_delta(delta),
            RunEvent::Ledger(record) => match record.event {
                HarnessEvent::ModelResponded {
                    run_id,
                    turn_id,
                    step,
                    output,
                    ..
                } => self.commit_response(
                    ResponseKey {
                        run_id,
                        turn_id,
                        step,
                    },
                    output.content,
                ),
                _ => Ok(Vec::new()),
            },
        }
    }

    fn accept_delta(&mut self, delta: AssistantDeltaEvent) -> Result<Vec<Sentence>, VoiceError> {
        let key = ResponseKey {
            run_id: delta.run_id,
            turn_id: delta.turn_id,
            step: delta.step,
        };
        if self.last_committed.as_ref() == Some(&key) {
            return Err(contract_error("received a delta after its response commit"));
        }
        if self.pending.is_none() {
            if delta.delta_index != 0 {
                return Err(contract_error(format!(
                    "first delta index was {}, expected 0",
                    delta.delta_index
                )));
            }
            self.pending = Some(PendingResponse {
                key: key.clone(),
                next_delta_index: 0,
                text: String::new(),
                cutter: SentenceCutter::new(),
            });
        }
        let pending = self.pending.as_mut().expect("pending response initialized");
        if pending.key != key {
            return Err(contract_error(
                "received a new assistant response before the prior response committed",
            ));
        }
        if delta.delta_index != pending.next_delta_index {
            return Err(contract_error(format!(
                "delta index was {}, expected {}",
                delta.delta_index, pending.next_delta_index
            )));
        }
        pending.next_delta_index += 1;
        pending.text.push_str(&delta.text);
        Ok(pending.cutter.push(&delta.text))
    }

    fn commit_response(
        &mut self,
        key: ResponseKey,
        committed_text: String,
    ) -> Result<Vec<Sentence>, VoiceError> {
        if self.last_committed.as_ref() == Some(&key) {
            return Err(contract_error("received a duplicate model response commit"));
        }
        let mut sentences = Vec::new();
        match self.pending.take() {
            Some(mut pending) => {
                if pending.key != key {
                    self.pending = Some(pending);
                    return Err(contract_error(
                        "model response commit did not match the pending assistant deltas",
                    ));
                }
                if pending.text != committed_text {
                    return Err(contract_error(format!(
                        "streamed assistant text differed from committed text ({} streamed bytes, {} committed bytes)",
                        pending.text.len(),
                        committed_text.len()
                    )));
                }
                if let Some(tail) = pending.cutter.finish() {
                    sentences.push(tail);
                }
            }
            None => {
                let mut cutter = SentenceCutter::new();
                sentences.extend(cutter.push(&committed_text));
                if let Some(tail) = cutter.finish() {
                    sentences.push(tail);
                }
            }
        }
        self.last_committed = Some(key);
        Ok(sentences)
    }

    fn finish(&self) -> Result<(), VoiceRunError> {
        if self.pending.is_some() {
            return Err(VoiceRunError::Voice(contract_error(
                "run event channel closed with uncommitted assistant deltas",
            )));
        }
        Ok(())
    }
}

fn contract_error(reason: impl Into<String>) -> VoiceError {
    VoiceError::EventContract {
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use platonic_core::{Message, MessageRole, RecordedEvent};

    fn run_options(question: &str) -> RunOptions {
        RunOptions {
            question: question.to_owned(),
            config_path: None,
            overrides: crate::RunOverrides::default(),
            ledger: crate::RunLedger::Jsonl(std::path::PathBuf::from("unused.jsonl")),
            workspace_root: std::path::PathBuf::from("."),
            approval_mode: crate::ApprovalMode::Deny { actor: "test" },
            run_id: None,
            session: None,
            event_sender: None,
            stream_to_stderr: false,
            cancel: None,
        }
    }

    fn delta(index: u64, text: &str) -> RunEvent {
        RunEvent::AssistantDelta(AssistantDeltaEvent {
            run_id: RunId::new("run_1").unwrap(),
            turn_id: TurnId::new("turn_1").unwrap(),
            step: 0,
            delta_index: index,
            text: text.to_owned(),
        })
    }

    fn commit(text: &str) -> RunEvent {
        RunEvent::Ledger(RecordedEvent {
            seq: 3,
            occurred_at_ms: 1,
            event: HarnessEvent::ModelResponded {
                run_id: RunId::new("run_1").unwrap(),
                turn_id: TurnId::new("turn_1").unwrap(),
                step: 0,
                output: Message {
                    role: MessageRole::Assistant,
                    content: text.to_owned(),
                },
                proposed_calls: Vec::new(),
                usage: None,
            },
        })
    }

    fn strings(sentences: Vec<Sentence>) -> Vec<String> {
        sentences.into_iter().map(Sentence::into_string).collect()
    }

    #[test]
    fn fragmented_deltas_produce_one_exact_sentence_sequence() {
        let mut stream = AssistantTextStream::default();
        let mut accepted = Vec::new();
        accepted.extend(stream.accept(delta(0, "This first sentence ")).unwrap());
        accepted.extend(stream.accept(delta(1, "is complete. A second ")).unwrap());
        accepted.extend(
            stream
                .accept(delta(2, "sentence is complete! Tail"))
                .unwrap(),
        );
        accepted.extend(
            stream
                .accept(commit(
                    "This first sentence is complete. A second sentence is complete! Tail",
                ))
                .unwrap(),
        );
        assert_eq!(
            strings(accepted),
            [
                "This first sentence is complete.",
                "A second sentence is complete!",
                "Tail"
            ]
        );
        stream.finish().unwrap();
    }

    #[test]
    fn committed_fallback_without_deltas_is_narrated_once() {
        let mut stream = AssistantTextStream::default();
        assert_eq!(
            strings(
                stream
                    .accept(commit("A committed response without streaming."))
                    .unwrap()
            ),
            ["A committed response without streaming."]
        );
        stream.finish().unwrap();
    }

    #[test]
    fn mismatched_indexes_and_committed_text_fail_closed() {
        let mut stream = AssistantTextStream::default();
        assert!(stream.accept(delta(1, "out of order")).is_err());

        let mut stream = AssistantTextStream::default();
        stream.accept(delta(0, "streamed text")).unwrap();
        assert!(stream.accept(commit("different text")).is_err());
    }

    #[test]
    fn event_channel_must_not_close_with_an_uncommitted_response() {
        let mut stream = AssistantTextStream::default();
        stream.accept(delta(0, "pending")).unwrap();
        assert!(stream.finish().is_err());
    }

    #[test]
    fn only_a_final_transcript_can_replace_the_typed_question() {
        let transcript = Transcript::new("spoken question", true, 500).unwrap();
        let options =
            options_for_transcript(run_options("typed placeholder"), &transcript).unwrap();
        assert_eq!(options.question, "spoken question");

        let rolling = Transcript::new("unfinished", false, 250).unwrap();
        assert!(matches!(
            options_for_transcript(run_options("typed placeholder"), &rolling),
            Err(VoiceError::EventContract { .. })
        ));
    }
}
