//! Client-owned composition from protocol run events to the audio I/O leaf.

use std::{
    collections::{BTreeMap, BTreeSet},
    io::{self, Write},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::Duration,
};

use plato_audio::{
    BargeInMetrics, CaptureConfig, CaptureDeviceInfo, CaptureError, CaptureMetrics, CapturePartial,
    CaptureReport, CaptureWorker, CaptureWorkerShutdown, KokoroConfig, KokoroMetrics,
    KokoroMetricsReader, KokoroProvenance, KokoroSynthesizer, OrtRuntime, OrtRuntimeError,
    OrtRuntimeMetrics, OrtRuntimeMetricsReader, PlaybackConfig, PlaybackDeviceInfo,
    PlaybackMetrics, PlaybackReport, Sentence, SentenceCutter, SileroConfig, SileroMetrics,
    SileroMetricsReader, SileroProvenance, SileroVad, SpeechSource, SpokenInterruption, SttError,
    SynthError, SynthWorker, SynthWorkerError, SynthWorkerShutdown, SynthWorkerStartError,
    Transcript, VadError, WhisperConfig, WhisperMetrics, WhisperMetricsReader, WhisperProvenance,
    WhisperRecognizer,
};
use platonic_core::{HarnessEvent, RunId, TurnId};
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    AppError, AssistantDeltaEvent, RunEvent, RunOptions, RunOutcome, VoiceEvent, VoiceEventEnvelope,
};

/// Root composition failures while interpreting existing run events.
#[derive(Debug, Error)]
pub enum VoiceError {
    /// Shared process-global ONNX Runtime acquisition failed.
    #[error(transparent)]
    Runtime(#[from] OrtRuntimeError),
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
    /// Resident Silero setup or inference failed.
    #[error(transparent)]
    Vad(#[from] VadError),
    /// Persistent input, endpointing, or recognition failed.
    #[error(transparent)]
    Capture(#[from] CaptureError),
    /// Live root transcript presentation failed before a run began.
    #[error("cannot present live voice transcript: {0}")]
    Presentation(#[source] io::Error),
    /// This session was opened without the explicit capture path.
    #[error("voice session has no capture worker")]
    CaptureUnavailable,
    /// The voice session was explicitly shut down.
    #[error("voice session is closed")]
    SessionClosed,
    /// Voice facts require the server-owned SQLite stream returned by the run.
    #[error("narrated runs require a server-owned SQLite ledger")]
    SqliteRequired,
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
    /// Exact revision-one companion facts produced by client-side observation.
    pub voice_events: Vec<VoiceEventEnvelope>,
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

/// Root-owned warm voice engines and persistent cpal streams reused across runs.
pub struct VoiceSession {
    ort_runtime: OrtRuntime,
    ort_metrics: OrtRuntimeMetricsReader,
    provenance: KokoroProvenance,
    kokoro_metrics: KokoroMetricsReader,
    worker: Option<SynthWorker>,
    whisper_provenance: Option<WhisperProvenance>,
    whisper_metrics: Option<WhisperMetricsReader>,
    silero_provenance: Option<SileroProvenance>,
    silero_metrics: Option<SileroMetricsReader>,
    capture: Option<CaptureWorker>,
    cancel: Arc<AtomicBool>,
}

impl VoiceSession {
    /// Loads the pinned model and opens the output device before any app run.
    pub fn open(kokoro: KokoroConfig, playback: PlaybackConfig) -> Result<Self, VoiceError> {
        let ort_runtime = OrtRuntime::acquire()?;
        let ort_metrics = ort_runtime.metrics_reader();
        let synthesizer = KokoroSynthesizer::load_with_runtime(kokoro, ort_runtime.clone())?;
        let provenance = synthesizer.provenance().clone();
        let kokoro_metrics = synthesizer.metrics_reader();
        let cancel = Arc::new(AtomicBool::new(false));
        let worker = SynthWorker::spawn(synthesizer, playback, Arc::clone(&cancel))?;
        Ok(Self {
            ort_runtime,
            ort_metrics,
            provenance,
            kokoro_metrics,
            worker: Some(worker),
            whisper_provenance: None,
            whisper_metrics: None,
            silero_provenance: None,
            silero_metrics: None,
            capture: None,
            cancel,
        })
    }

    /// Opens AU2 output plus resident Whisper/Silero input through one shared ONNX owner.
    pub fn open_with_capture(
        kokoro: KokoroConfig,
        playback: PlaybackConfig,
        whisper: WhisperConfig,
        silero: SileroConfig,
        capture_config: CaptureConfig,
    ) -> Result<Self, VoiceError> {
        let mut session = Self::open(kokoro, playback)?;
        let recognizer = WhisperRecognizer::load(whisper)?;
        let whisper_provenance = recognizer.provenance().clone();
        let whisper_metrics = recognizer.metrics_reader();
        let detector = SileroVad::load_with_runtime(silero, session.ort_runtime.clone())?;
        let silero_provenance = detector.provenance().clone();
        let silero_metrics = detector.metrics_reader();
        let barge_in = session
            .worker
            .as_ref()
            .expect("open voice session retains its worker")
            .barge_in_handle();
        let capture =
            CaptureWorker::open_with_barge_in(capture_config, detector, recognizer, barge_in)?;
        session.whisper_provenance = Some(whisper_provenance);
        session.whisper_metrics = Some(whisper_metrics);
        session.silero_provenance = Some(silero_provenance);
        session.silero_metrics = Some(silero_metrics);
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

    /// Returns resident Silero artifact and shared-runtime identity when enabled.
    pub fn vad_provenance(&self) -> Option<&SileroProvenance> {
        self.silero_provenance.as_ref()
    }

    /// Reads the one-environment ONNX session residency counters.
    pub fn ort_runtime_metrics(&self) -> OrtRuntimeMetrics {
        self.ort_metrics.snapshot()
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

    /// Reads resident Silero session and recurrent-state counters when enabled.
    pub fn vad_metrics(&self) -> Option<SileroMetrics> {
        self.silero_metrics
            .as_ref()
            .map(SileroMetricsReader::snapshot)
    }

    /// Reads persistent input, VAD, conversion, and overflow counters when enabled.
    pub fn capture_metrics(&self) -> Option<CaptureMetrics> {
        self.capture.as_ref().map(CaptureWorker::metrics)
    }

    /// Returns the one cancel allocation shared by app, synth, capture, and callback.
    pub fn cancel_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.cancel)
    }

    /// Reads playback-time speech-onset and first-silent-callback evidence.
    pub fn barge_in_metrics(&self) -> BargeInMetrics {
        self.worker
            .as_ref()
            .expect("open voice session retains its worker")
            .barge_in_metrics()
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

    /// Presents replaceable partials, commits one final question, then starts one run.
    pub fn capture_question(
        &mut self,
        options: RunOptions,
        timeout: Duration,
    ) -> Result<CapturedRunOutcome, VoiceRunError> {
        if options.event_sender.is_some() {
            return Err(VoiceRunError::EventSenderAlreadySet);
        }
        let display_live = options.stream_to_stderr;
        let mut input = ActiveVoiceInput::default();
        let mut input_error = None;
        let mut presentation_error = None;
        let mut visible_partial_us = Vec::new();
        let stderr = io::stderr();
        let mut presentation = display_live.then(|| TerminalVoiceInput::new(stderr.lock()));
        let capture = self
            .capture
            .as_ref()
            .ok_or(VoiceError::CaptureUnavailable)?
            .capture_with_partials(timeout, |partial| {
                if input_error.is_none()
                    && let Err(error) = input.replace_partial(partial)
                {
                    input_error = Some(error);
                }
                if let Some(presentation) = presentation.as_mut()
                    && presentation_error.is_none()
                {
                    match presentation.replace(partial) {
                        Ok(()) => visible_partial_us.push(partial.observed_latency_us()),
                        Err(error) => presentation_error = Some(error),
                    }
                }
            })
            .map_err(VoiceError::from)?;
        if let Some(error) = input_error {
            return Err(error.into());
        }
        input.finalize(&capture.transcript)?;
        if let Some(error) = presentation_error {
            return Err(VoiceError::Presentation(error).into());
        }
        if display_live && visible_partial_us.len() != capture.partials.len() {
            return Err(
                contract_error("live presentation did not observe every capture partial").into(),
            );
        }
        let mut capture = capture;
        for (partial, visible_us) in capture.partials.iter_mut().zip(visible_partial_us) {
            partial.audio_available_to_visible_us = Some(visible_us);
        }
        if let Some(presentation) = presentation.as_mut() {
            presentation
                .commit(&capture.transcript)
                .map_err(VoiceError::Presentation)?;
        }
        drop(presentation);
        let options = options_for_transcript(options, &capture.transcript)?;
        let narrated = self.run_question_with_capture(options, Some(&capture))?;
        Ok(CapturedRunOutcome { capture, narrated })
    }

    /// Drives the existing synchronous app run while narrating its event stream.
    pub fn run_question(
        &mut self,
        options: RunOptions,
    ) -> Result<NarratedRunOutcome, VoiceRunError> {
        self.run_question_with_capture(options, None)
    }

    fn run_question_with_capture(
        &mut self,
        mut options: RunOptions,
        capture_report: Option<&CaptureReport>,
    ) -> Result<NarratedRunOutcome, VoiceRunError> {
        if options.event_sender.is_some() {
            return Err(VoiceRunError::EventSenderAlreadySet);
        }
        bind_voice_cancel(&mut options, &self.cancel)?;
        let cancel = Arc::clone(&self.cancel);
        let (sender, receiver) = mpsc::channel();
        options.event_sender = Some(sender);
        let worker = self.worker.as_ref().ok_or(VoiceError::SessionClosed)?;
        debug_assert!(worker.uses_cancel(&cancel));
        worker.begin_run().map_err(VoiceError::from)?;
        let capture = self.capture.as_ref();
        let mut next_interruption = None;
        let mut accepted_sources = BTreeMap::new();
        let mut first_response_key = None;

        let result = std::thread::scope(|scope| {
            let run = scope.spawn(move || crate::run_question(options));
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
                        for narrated in accepted {
                            let source = narrated.source;
                            let key = narrated.key;
                            match worker.accept(narrated.sentence, source) {
                                Ok(admission) => {
                                    if accepted_sources
                                        .insert(
                                            admission.sequence,
                                            AcceptedNarrationSource { key, source },
                                        )
                                        .is_some()
                                    {
                                        voice_error = Some(contract_error(
                                            "synthesis reused a worker sequence within one run",
                                        ));
                                        cancel.store(true, Ordering::Release);
                                        break;
                                    }
                                    sentences.extend(admission.completed.into_iter().map(
                                        |report| NarratedSentenceReport {
                                            sentence: report.sentence,
                                            playback: report.playback,
                                        },
                                    ));
                                }
                                Err(SynthWorkerError::Canceled) => break,
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
                        if let Some(capture) = capture
                            && let Err(error) = capture.check_health()
                        {
                            voice_error = Some(VoiceError::Capture(error));
                            cancel.store(true, Ordering::Release);
                            break;
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }

            let run_result = run.join().map_err(|_| VoiceRunError::RunThreadPanicked);
            let audio_result = worker.wait_until_idle();
            let capture_health = capture.map(CaptureWorker::check_health).transpose();
            let finish_result = if audio_result.is_ok() {
                worker.finish_run().map_err(VoiceError::from)
            } else {
                Ok(None)
            };
            next_interruption = finish_result?;
            if let Some(error) = voice_error {
                return Err(VoiceRunError::Voice(error));
            }
            capture_health.map_err(VoiceError::from)?;
            let run = run_result??;
            stream.finish()?;
            first_response_key = stream.first_committed().cloned();
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
                voice_events: Vec::new(),
            })
        });

        let mut outcome = result?;
        let events = voice_events_for_run(
            &outcome,
            capture_report,
            first_response_key.as_ref(),
            &accepted_sources,
            next_interruption.as_ref(),
            worker,
        )?;
        require_voice_sqlite(&outcome.run.ledger_path)?;
        outcome.voice_events = events
            .into_iter()
            .enumerate()
            .map(|(sequence, event)| {
                VoiceEventEnvelope::revision_one(u64::try_from(sequence).unwrap_or(u64::MAX), event)
            })
            .collect();
        Ok(outcome)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AcceptedNarrationSource {
    key: ResponseKey,
    source: SpeechSource,
}

#[derive(Clone, Debug)]
struct VoiceTurnEvidence {
    key: ResponseKey,
    audible_sequences: BTreeSet<u64>,
    interruption: Option<SpokenInterruption>,
}

fn require_voice_sqlite(ledger_path: &std::path::Path) -> Result<(), VoiceError> {
    if ledger_path
        .extension()
        .is_some_and(|extension| extension == "db")
    {
        Ok(())
    } else {
        Err(VoiceError::SqliteRequired)
    }
}

fn voice_events_for_run(
    outcome: &NarratedRunOutcome,
    capture: Option<&CaptureReport>,
    first_response_key: Option<&ResponseKey>,
    accepted: &BTreeMap<u64, AcceptedNarrationSource>,
    interruption: Option<&SpokenInterruption>,
    worker: &SynthWorker,
) -> Result<Vec<VoiceEvent>, VoiceError> {
    voice_events_for_observations(
        &outcome.run.run_id,
        &outcome.narration.sentences,
        capture,
        first_response_key,
        accepted,
        interruption,
        |sequence| worker.accepted_to_first_non_silent_us(sequence),
    )
}

fn voice_events_for_observations(
    run_id: &RunId,
    sentences: &[NarratedSentenceReport],
    capture: Option<&CaptureReport>,
    first_response_key: Option<&ResponseKey>,
    accepted: &BTreeMap<u64, AcceptedNarrationSource>,
    interruption: Option<&SpokenInterruption>,
    ttfa_us_for_sequence: impl Fn(u64) -> Option<u64>,
) -> Result<Vec<VoiceEvent>, VoiceError> {
    let mut events = Vec::new();
    if let Some(capture) = capture {
        if !capture.transcript.is_final {
            return Err(contract_error(
                "durable capture mapping received a non-final transcript",
            ));
        }
        let response = first_response_key.ok_or_else(|| {
            contract_error("captured run completed without a committed response turn")
        })?;
        if &response.run_id != run_id {
            return Err(contract_error(
                "captured response run ID differed from the completed run",
            ));
        }
        events.push(captured_event(
            run_id.clone(),
            response.turn_id.clone(),
            capture,
        ));
    }

    let mut turns = Vec::<VoiceTurnEvidence>::new();
    for report in sentences {
        let source = accepted.get(&report.playback.sequence).ok_or_else(|| {
            contract_error(format!(
                "completed sentence {} had no root source mapping",
                report.playback.sequence
            ))
        })?;
        if &source.key.run_id != run_id {
            return Err(contract_error(
                "completed narration run ID differed from the app outcome",
            ));
        }
        turn_evidence(&mut turns, &source.key)
            .audible_sequences
            .insert(report.playback.sequence);
    }

    if let Some(interruption) = interruption {
        let (sequence, source) = accepted
            .iter()
            .find(|(_, source)| {
                source.source.sentence_index == interruption.sentence_index
                    && source.source.assistant_delta_index == interruption.assistant_delta_index
            })
            .ok_or_else(|| {
                contract_error("AU5 interruption latch had no matching root sentence source")
            })?;
        let turn = turn_evidence(&mut turns, &source.key);
        if turn.interruption.is_some() {
            return Err(contract_error(
                "one narrated run produced more than one interruption latch",
            ));
        }
        turn.audible_sequences.insert(*sequence);
        turn.interruption = Some(interruption.clone());
    }

    turns.sort_by_key(|turn| turn.audible_sequences.first().copied().unwrap_or(u64::MAX));
    for turn in turns {
        if &turn.key.run_id != run_id {
            return Err(contract_error(
                "voice turn run ID differed from the app outcome",
            ));
        }
        let first_sequence = turn
            .audible_sequences
            .first()
            .copied()
            .ok_or_else(|| contract_error("voice turn had no audible sentence"))?;
        let ttfa_us = sentences
            .iter()
            .find(|report| report.playback.sequence == first_sequence)
            .map(|report| report.playback.accepted_to_first_non_silent_us)
            .or_else(|| ttfa_us_for_sequence(first_sequence))
            .ok_or_else(|| {
                contract_error(format!(
                    "voice turn first sentence {first_sequence} had no first non-silent timing"
                ))
            })?;
        let sentence_count = u64::try_from(turn.audible_sequences.len())
            .map_err(|_| contract_error("voice turn sentence count overflowed u64"))?;
        let interrupted_at = turn
            .interruption
            .as_ref()
            .map(|interruption| interruption.sentence_index);
        events.push(VoiceEvent::VoiceSpoken {
            run_id: run_id.clone(),
            turn_id: turn.key.turn_id.clone(),
            ttfa_ms: ttfa_us / 1_000,
            sentence_count,
            interrupted_at,
        });
        if let Some(interruption) = turn.interruption {
            events.push(VoiceEvent::VoiceInterrupted {
                run_id: run_id.clone(),
                turn_id: turn.key.turn_id,
                spoken_prefix: interruption.spoken_prefix,
                delta_index: interruption.assistant_delta_index,
            });
        }
    }
    Ok(events)
}

fn turn_evidence<'a>(
    turns: &'a mut Vec<VoiceTurnEvidence>,
    key: &ResponseKey,
) -> &'a mut VoiceTurnEvidence {
    if let Some(index) = turns.iter().position(|turn| turn.key == *key) {
        return &mut turns[index];
    }
    turns.push(VoiceTurnEvidence {
        key: key.clone(),
        audible_sequences: BTreeSet::new(),
        interruption: None,
    });
    turns.last_mut().expect("voice turn was just inserted")
}

fn captured_event(run_id: RunId, turn_id: TurnId, report: &CaptureReport) -> VoiceEvent {
    let transcript = report.transcript.text.as_bytes();
    VoiceEvent::VoiceCaptured {
        run_id,
        turn_id,
        transcript_sha256: format!("{:x}", Sha256::digest(transcript)),
        transcript_bytes: u64::try_from(transcript.len()).unwrap_or(u64::MAX),
        transcript_span_ms: report.transcript.span_ms,
        input_frames: report.input_frames,
        output_frames: report.output_frames,
        vad_start_sample: report.endpoint.start_sample,
        vad_speech_end_sample: report.endpoint.speech_end_sample,
        vad_close_sample: report.endpoint.close_sample,
        vad_close_to_final_us: report.vad_close_to_final_us,
        normalization_resampling_us: report.normalization_resampling_us,
    }
}

#[derive(Default)]
struct ActiveVoiceInput {
    partial: Option<Transcript>,
    finalized: bool,
}

impl ActiveVoiceInput {
    fn replace_partial(&mut self, partial: &CapturePartial) -> Result<(), VoiceError> {
        if partial.transcript.is_final {
            return Err(contract_error(
                "voice input partial callback received a final transcript",
            ));
        }
        if self.finalized {
            return Err(contract_error(
                "voice input partial arrived after finalization",
            ));
        }
        self.partial = Some(partial.transcript.clone());
        Ok(())
    }

    fn finalize(&mut self, transcript: &Transcript) -> Result<(), VoiceError> {
        if !transcript.is_final {
            return Err(contract_error(
                "voice input finalization received a non-final transcript",
            ));
        }
        if self.finalized {
            return Err(contract_error("voice input finalized more than once"));
        }
        self.finalized = true;
        Ok(())
    }
}

struct TerminalVoiceInput<W> {
    writer: W,
}

impl<W: Write> TerminalVoiceInput<W> {
    fn new(writer: W) -> Self {
        Self { writer }
    }

    fn replace(&mut self, partial: &CapturePartial) -> io::Result<()> {
        write!(
            self.writer,
            "\r\x1b[2KYou: {}",
            single_line(&partial.transcript.text)
        )?;
        self.writer.flush()
    }

    fn commit(&mut self, transcript: &Transcript) -> io::Result<()> {
        writeln!(
            self.writer,
            "\r\x1b[2KYou: {}",
            single_line(&transcript.text)
        )?;
        self.writer.flush()
    }
}

fn single_line(text: &str) -> String {
    text.chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect()
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

#[cfg(test)]
fn interruption_context(interruption: &SpokenInterruption) -> String {
    let prefix = serde_json::to_string(&interruption.spoken_prefix)
        .expect("serializing a Rust string cannot fail");
    format!(
        "The user interrupted your spoken reply after {prefix} (assistant sentence index {}, assistant delta index {}).",
        interruption.sentence_index, interruption.assistant_delta_index
    )
}

fn bind_voice_cancel(
    options: &mut RunOptions,
    session_cancel: &Arc<AtomicBool>,
) -> Result<(), VoiceError> {
    if options
        .cancel
        .as_ref()
        .is_some_and(|cancel| !Arc::ptr_eq(cancel, session_cancel))
    {
        return Err(contract_error(
            "narrated run cancel flag does not match the voice session authority",
        ));
    }
    options.cancel = Some(Arc::clone(session_cancel));
    Ok(())
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct NarratedSentence {
    sentence: Sentence,
    source: SpeechSource,
    key: ResponseKey,
}

#[derive(Default)]
struct AssistantTextStream {
    pending: Option<PendingResponse>,
    first_committed: Option<ResponseKey>,
    last_committed: Option<ResponseKey>,
    next_sentence_index: u64,
}

impl AssistantTextStream {
    fn accept(&mut self, event: RunEvent) -> Result<Vec<NarratedSentence>, VoiceError> {
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

    fn accept_delta(
        &mut self,
        delta: AssistantDeltaEvent,
    ) -> Result<Vec<NarratedSentence>, VoiceError> {
        let delta_index = delta.delta_index;
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
        let sentences = pending.cutter.push(&delta.text);
        Ok(self.tag_sentences(sentences, delta_index, &key))
    }

    fn commit_response(
        &mut self,
        key: ResponseKey,
        committed_text: String,
    ) -> Result<Vec<NarratedSentence>, VoiceError> {
        if self.last_committed.as_ref() == Some(&key) {
            return Err(contract_error("received a duplicate model response commit"));
        }
        let mut sentences = Vec::new();
        let assistant_delta_index = match self.pending.take() {
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
                pending.next_delta_index.saturating_sub(1)
            }
            None => {
                let mut cutter = SentenceCutter::new();
                sentences.extend(cutter.push(&committed_text));
                if let Some(tail) = cutter.finish() {
                    sentences.push(tail);
                }
                0
            }
        };
        if self.first_committed.is_none() {
            self.first_committed = Some(key.clone());
        }
        self.last_committed = Some(key.clone());
        Ok(self.tag_sentences(sentences, assistant_delta_index, &key))
    }

    fn tag_sentences(
        &mut self,
        sentences: Vec<Sentence>,
        assistant_delta_index: u64,
        key: &ResponseKey,
    ) -> Vec<NarratedSentence> {
        sentences
            .into_iter()
            .map(|sentence| {
                let source = SpeechSource::new(self.next_sentence_index, assistant_delta_index);
                self.next_sentence_index = self.next_sentence_index.saturating_add(1);
                NarratedSentence {
                    sentence,
                    source,
                    key: key.clone(),
                }
            })
            .collect()
    }

    fn finish(&self) -> Result<(), VoiceRunError> {
        if self.pending.is_some() {
            return Err(VoiceRunError::Voice(contract_error(
                "run event channel closed with uncommitted assistant deltas",
            )));
        }
        Ok(())
    }

    fn first_committed(&self) -> Option<&ResponseKey> {
        self.first_committed.as_ref()
    }
}

fn contract_error(reason: impl Into<String>) -> VoiceError {
    VoiceError::EventContract {
        reason: reason.into(),
    }
}

#[cfg(all(test, feature = "whisper-cuda"))]
mod timing_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use platonic_core::{Message, MessageRole, RecordedEvent};

    #[test]
    fn options_for_transcript_replaces_only_the_question_and_requires_a_final_transcript() {
        use crate::{ApprovalMode, RunOptions, RunOverrides};
        use std::path::PathBuf;

        let base = RunOptions {
            question: "typed placeholder".to_owned(),
            config_path: None,
            overrides: RunOverrides::default(),
            workspace_root: PathBuf::from("/tmp"),
            approval_mode: ApprovalMode::Deny,
            session_id: None,
            continue_latest: false,
            event_sender: None,
            stream_to_stderr: false,
            cancel: None,
        };

        let final_transcript = Transcript::new("spoken parity question", true, 700).unwrap();
        let voiced = options_for_transcript(base.clone(), &final_transcript).unwrap();

        assert_eq!(voiced.question, "spoken parity question");
        assert_eq!(voiced.workspace_root, base.workspace_root);
        assert_eq!(voiced.stream_to_stderr, base.stream_to_stderr);

        let partial = Transcript::new("still speaking", false, 700).unwrap();
        assert!(matches!(
            options_for_transcript(base, &partial),
            Err(VoiceError::EventContract { .. })
        ));
    }

    fn playback_report(sequence: u64, ttfa_us: u64) -> PlaybackReport {
        PlaybackReport {
            sequence,
            accepted_ns: 1_000,
            synth_started_ns: 2_000,
            synth_finished_ns: 3_000,
            first_pcm_ns: 4_000,
            first_non_silent_ns: 1_000 + ttfa_us * 1_000,
            pcm_end_ns: 9_000_000,
            accepted_to_first_non_silent_us: ttfa_us,
            synthesis_us: 1,
            gap_before_us: None,
            first_callback_frames: 256,
            callback_count: 3,
            source_frames: 1_000,
            device_frames: 3_000,
            underrun: plato_audio::PlaybackUnderrun::default(),
        }
    }

    fn run_options(question: &str) -> RunOptions {
        RunOptions {
            question: question.to_owned(),
            config_path: None,
            overrides: crate::RunOverrides::default(),
            workspace_root: std::path::PathBuf::from("."),
            approval_mode: crate::ApprovalMode::Deny,
            session_id: None,
            continue_latest: false,
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
                served_model: None,
                usage: None,
            },
        })
    }

    fn strings(sentences: Vec<NarratedSentence>) -> Vec<String> {
        sentences
            .into_iter()
            .map(|sentence| sentence.sentence.into_string())
            .collect()
    }

    #[test]
    fn interrupted_voice_facts_use_exact_au2_and_au5_boundaries() {
        let run_id = RunId::new("run_voice").unwrap();
        let turn_id = TurnId::new("turn_1").unwrap();
        let key = ResponseKey {
            run_id: run_id.clone(),
            turn_id: turn_id.clone(),
            step: 0,
        };
        let accepted = BTreeMap::from([
            (
                40,
                AcceptedNarrationSource {
                    key: key.clone(),
                    source: SpeechSource::new(0, 3),
                },
            ),
            (
                41,
                AcceptedNarrationSource {
                    key,
                    source: SpeechSource::new(1, 4),
                },
            ),
        ]);
        let sentences = vec![NarratedSentenceReport {
            sentence: "First sentence.".into(),
            playback: playback_report(40, 321_999),
        }];
        let interruption = SpokenInterruption {
            played_samples: 8_192,
            sentence_index: 1,
            assistant_delta_index: 4,
            spoken_prefix: "Second sentence was".into(),
        };

        let events = voice_events_for_observations(
            &run_id,
            &sentences,
            None,
            None,
            &accepted,
            Some(&interruption),
            |sequence| match sequence {
                40 => Some(321_999),
                41 => Some(280_000),
                _ => None,
            },
        )
        .unwrap();

        assert_eq!(
            events,
            [
                VoiceEvent::VoiceSpoken {
                    run_id: run_id.clone(),
                    turn_id: turn_id.clone(),
                    ttfa_ms: 321,
                    sentence_count: 2,
                    interrupted_at: Some(1),
                },
                VoiceEvent::VoiceInterrupted {
                    run_id,
                    turn_id,
                    spoken_prefix: "Second sentence was".into(),
                    delta_index: 4,
                },
            ]
        );
    }

    #[test]
    fn interrupted_first_sentence_without_first_audio_timing_emits_no_success_facts() {
        let run_id = RunId::new("run_voice").unwrap();
        let key = ResponseKey {
            run_id: run_id.clone(),
            turn_id: TurnId::new("turn_1").unwrap(),
            step: 0,
        };
        let accepted = BTreeMap::from([(
            9,
            AcceptedNarrationSource {
                key,
                source: SpeechSource::new(0, 0),
            },
        )]);
        let interruption = SpokenInterruption {
            played_samples: 64,
            sentence_index: 0,
            assistant_delta_index: 0,
            spoken_prefix: "Audible".into(),
        };

        assert!(matches!(
            voice_events_for_observations(
                &run_id,
                &[],
                None,
                None,
                &accepted,
                Some(&interruption),
                |_| None,
            ),
            Err(VoiceError::EventContract { .. })
        ));
    }

    #[test]
    fn voice_fact_emission_rejects_jsonl_instead_of_claiming_durability() {
        assert!(matches!(
            require_voice_sqlite(std::path::Path::new("events.jsonl")),
            Err(VoiceError::SqliteRequired)
        ));
        assert!(require_voice_sqlite(std::path::Path::new("events.db")).is_ok());
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
            accepted
                .iter()
                .map(|sentence| sentence.source)
                .collect::<Vec<_>>(),
            [
                SpeechSource::new(0, 1),
                SpeechSource::new(1, 2),
                SpeechSource::new(2, 2),
            ]
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

    #[test]
    fn pre_canceled_same_arc_binding_preserves_generic_cancel_without_voice_context() {
        let cancel = Arc::new(AtomicBool::new(true));
        let mut options = run_options("already canceled");
        options.cancel = Some(Arc::clone(&cancel));

        bind_voice_cancel(&mut options, &cancel).unwrap();

        let bound = options.cancel.as_ref().unwrap();
        assert!(Arc::ptr_eq(bound, &cancel));
        assert!(bound.load(Ordering::Acquire));
    }

    #[test]
    fn spoken_interruption_maps_exact_sample_latch_coordinates_into_context() {
        let interruption = SpokenInterruption {
            played_samples: 7_424,
            sentence_index: 2,
            assistant_delta_index: 5,
            spoken_prefix: "quoted \"prefix\"".to_owned(),
        };

        assert_eq!(
            interruption_context(&interruption),
            "The user interrupted your spoken reply after \"quoted \\\"prefix\\\"\" (assistant sentence index 2, assistant delta index 5)."
        );
    }

    #[test]
    fn rolling_voice_input_replaces_state_and_commits_exactly_one_final() {
        let mut input = ActiveVoiceInput::default();
        let first = CapturePartial::new(Transcript::new("what is", false, 320).unwrap(), 40_000);
        let revised = CapturePartial::new(
            Transcript::new("what is the capital", false, 480).unwrap(),
            45_000,
        );
        input.replace_partial(&first).unwrap();
        input.replace_partial(&revised).unwrap();
        assert_eq!(
            input.partial.as_ref().map(|partial| partial.text.as_str()),
            Some("what is the capital")
        );

        let final_transcript =
            Transcript::new("What is the capital of France?", true, 1_500).unwrap();
        input.finalize(&final_transcript).unwrap();
        assert!(input.finalize(&final_transcript).is_err());
        assert!(input.replace_partial(&revised).is_err());
    }

    #[test]
    fn terminal_partials_clear_and_replace_one_active_line() {
        let mut output = Vec::new();
        let first = CapturePartial::new(Transcript::new("what is", false, 320).unwrap(), 10);
        let revised = CapturePartial::new(
            Transcript::new("what\nis the capital", false, 480).unwrap(),
            11,
        );
        let final_transcript =
            Transcript::new("What is the capital of France?", true, 1_500).unwrap();
        {
            let mut presentation = TerminalVoiceInput::new(&mut output);
            presentation.replace(&first).unwrap();
            presentation.replace(&revised).unwrap();
            presentation.commit(&final_transcript).unwrap();
        }
        assert_eq!(
            String::from_utf8(output).unwrap(),
            "\r\x1b[2KYou: what is\r\x1b[2KYou: what is the capital\r\x1b[2KYou: What is the capital of France?\n"
        );
    }
}
