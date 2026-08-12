//! Client-owned composition from protocol run events to the audio I/O leaf.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::Duration,
};

use plato_audio::{
    BargeInMetrics, CaptureConfig, CaptureDeviceInfo, CaptureError, CaptureMetrics, CapturePartial,
    CaptureReport, CaptureRequest, CaptureWorker, CaptureWorkerShutdown, InputDeviceSelection,
    KokoroConfig, KokoroMetrics, KokoroMetricsReader, KokoroProvenance, KokoroSynthesizer,
    OrtRuntime, OrtRuntimeError, OrtRuntimeMetrics, OrtRuntimeMetricsReader, OutputDeviceSelection,
    PlaybackConfig, PlaybackDeviceInfo, PlaybackMetrics, PlaybackReport, Sentence, SentenceCutter,
    SileroConfig, SileroMetrics, SileroMetricsReader, SileroProvenance, SileroVad, SpeechSource,
    SpokenInterruption, SttError, SynthError, SynthWorker, SynthWorkerError, SynthWorkerShutdown,
    SynthWorkerStartError, SynthesizedSentenceReport, Transcript, VadError, WhisperConfig,
    WhisperMetrics, WhisperMetricsReader, WhisperProvenance, WhisperRecognizer,
};
use platonic_core::{HarnessEvent, RunId, TurnId};
use platonic_protocol::{RunStateName, StreamEvent};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    AppError, AssistantDeltaEvent, RunEvent, RunOptions, RunOutcome, VoiceEvent, VoiceEventEnvelope,
};

/// Fail-closed diagnostics for the one client-owned voice configuration.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum VoiceConfigError {
    /// The exact client-selected configuration file could not be read.
    #[error("voice configuration could not be read from {}: {reason}", path.display())]
    Read {
        /// Exact path selected by the client.
        path: PathBuf,
        /// Bounded filesystem diagnostic.
        reason: String,
    },
    /// The trusted document did not define voice at all.
    #[error("voice configuration is unavailable: missing [voice]")]
    MissingTable,
    /// One required explicit artifact path was absent.
    #[error("voice configuration is incomplete: missing voice.{field}")]
    MissingField {
        /// Exact field name from the voice table.
        field: &'static str,
    },
    /// An explicit path or device identifier was empty.
    #[error("voice configuration field voice.{field} must not be empty")]
    EmptyField {
        /// Exact field name from the voice table.
        field: &'static str,
    },
    /// The trusted TOML document or voice table had the wrong shape.
    #[error("voice configuration is invalid: {reason}")]
    InvalidDocument {
        /// TOML diagnostic from the exact selected document.
        reason: String,
    },
}

/// Explicit local model paths and device choices for one client voice session.
#[derive(Clone, Debug, PartialEq)]
pub struct VoiceConfig {
    kokoro: KokoroConfig,
    whisper: WhisperConfig,
    silero: SileroConfig,
    capture: CaptureConfig,
    playback: PlaybackConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct VoiceConfigDocument {
    voice: Option<RawVoiceConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawVoiceConfig {
    kokoro_model: Option<String>,
    whisper_model: Option<String>,
    silero_model: Option<String>,
    capture_device: Option<String>,
    playback_device: Option<String>,
}

impl VoiceConfig {
    /// Parses only caller-vetted TOML text; this performs no discovery or environment lookup.
    pub fn from_trusted_toml(document: &str, config_dir: &Path) -> Result<Self, VoiceConfigError> {
        let document = toml::from_str::<VoiceConfigDocument>(document).map_err(|error| {
            VoiceConfigError::InvalidDocument {
                reason: error.to_string(),
            }
        })?;
        let raw = document.voice.ok_or(VoiceConfigError::MissingTable)?;
        let kokoro_model = required_path(raw.kokoro_model, "kokoro_model", config_dir)?;
        let whisper_model = required_path(raw.whisper_model, "whisper_model", config_dir)?;
        let silero_model = required_path(raw.silero_model, "silero_model", config_dir)?;
        let capture =
            CaptureConfig::for_device(match optional_id(raw.capture_device, "capture_device")? {
                Some(device) => InputDeviceSelection::Id(device),
                None => InputDeviceSelection::Default,
            });
        let playback = PlaybackConfig::for_device(
            match optional_id(raw.playback_device, "playback_device")? {
                Some(device) => OutputDeviceSelection::Id(device),
                None => OutputDeviceSelection::Default,
            },
        );
        Ok(Self {
            kokoro: KokoroConfig::from_model_dir(kokoro_model),
            whisper: WhisperConfig::new(whisper_model),
            silero: SileroConfig::new(silero_model),
            capture,
            playback,
        })
    }

    /// Returns the exact local Kokoro artifact selection.
    pub fn kokoro(&self) -> &KokoroConfig {
        &self.kokoro
    }

    /// Returns the exact local Whisper artifact selection.
    pub fn whisper(&self) -> &WhisperConfig {
        &self.whisper
    }

    /// Returns the exact local Silero artifact selection.
    pub fn silero(&self) -> &SileroConfig {
        &self.silero
    }

    /// Returns the explicit or host-default capture-device selection.
    pub fn capture(&self) -> &CaptureConfig {
        &self.capture
    }

    /// Returns the explicit or host-default playback-device selection.
    pub fn playback(&self) -> &PlaybackConfig {
        &self.playback
    }
}

fn required_path(
    value: Option<String>,
    field: &'static str,
    config_dir: &Path,
) -> Result<PathBuf, VoiceConfigError> {
    let value = value.ok_or(VoiceConfigError::MissingField { field })?;
    let value = nonempty(value, field)?;
    let path = PathBuf::from(value);
    Ok(if path.is_absolute() {
        path
    } else {
        config_dir.join(path)
    })
}

fn optional_id(
    value: Option<String>,
    field: &'static str,
) -> Result<Option<String>, VoiceConfigError> {
    value.map(|value| nonempty(value, field)).transpose()
}

fn nonempty(value: String, field: &'static str) -> Result<String, VoiceConfigError> {
    if value.trim().is_empty() {
        Err(VoiceConfigError::EmptyField { field })
    } else {
        Ok(value)
    }
}

/// The one local user decision accepted before a client opens audio devices.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VoiceGrant {
    /// The user explicitly allowed voice for this client session.
    Granted,
    /// The user declined voice; no model or device may be opened.
    Denied,
}

/// Observable result of an idempotent client voice transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VoiceActivationChange {
    /// The explicit grant opened one new voice session.
    Enabled,
    /// Voice was already enabled and no second session was opened.
    AlreadyEnabled,
    /// The explicit grant was denied and voice remained off.
    Denied,
    /// One live voice session was stopped and closed.
    Disabled,
    /// Voice was already off and no teardown ran.
    AlreadyDisabled,
}

/// Failures while validating configuration or opening/stopping client audio.
#[derive(Debug, Error)]
pub enum VoiceActivationError {
    /// The explicit client configuration was absent, incomplete, or malformed.
    #[error(transparent)]
    Config(#[from] VoiceConfigError),
    /// The existing concrete voice session could not open or stop cleanly.
    #[error(transparent)]
    Session(#[from] VoiceError),
}

/// Client-session-local voice activation and device ownership.
pub struct VoiceActivation {
    config: Result<VoiceConfig, VoiceConfigError>,
    session: Option<VoiceSession>,
}

impl VoiceActivation {
    /// Creates an off session from one exact client-selected file, or unavailable if absent.
    pub fn from_explicit_config(path: Option<&Path>) -> Self {
        let config = match path {
            Some(path) => fs::read_to_string(path)
                .map_err(|error| VoiceConfigError::Read {
                    path: path.to_path_buf(),
                    reason: error.to_string(),
                })
                .and_then(|document| {
                    VoiceConfig::from_trusted_toml(
                        &document,
                        path.parent().unwrap_or_else(|| Path::new(".")),
                    )
                }),
            None => Err(VoiceConfigError::MissingTable),
        };
        Self {
            config,
            session: None,
        }
    }

    /// Creates an off session from caller-vetted TOML without discovering any config or model.
    pub fn from_trusted_toml(document: &str, config_dir: &Path) -> Self {
        Self {
            config: VoiceConfig::from_trusted_toml(document, config_dir),
            session: None,
        }
    }

    /// Reports whether this client currently owns open voice devices.
    pub fn is_enabled(&self) -> bool {
        self.session.is_some()
    }

    pub(crate) fn session_mut(&mut self) -> Option<&mut VoiceSession> {
        self.session.as_mut()
    }

    /// Opens one concrete voice session only after the explicit local grant.
    pub fn enable(
        &mut self,
        grant: VoiceGrant,
    ) -> Result<VoiceActivationChange, VoiceActivationError> {
        if self.session.is_some() {
            return Ok(VoiceActivationChange::AlreadyEnabled);
        }
        if grant == VoiceGrant::Denied {
            return Ok(VoiceActivationChange::Denied);
        }
        let config = self.config.as_ref().map_err(Clone::clone)?;
        let session = VoiceSession::open_with_capture(
            config.kokoro.clone(),
            config.playback.clone(),
            config.whisper.clone(),
            config.silero.clone(),
            config.capture.clone(),
        )?;
        self.session = Some(session);
        Ok(VoiceActivationChange::Enabled)
    }

    /// Revokes the grant and stops, drains, joins, and closes at most one session.
    pub fn disable(&mut self) -> Result<VoiceActivationChange, VoiceActivationError> {
        let Some(mut session) = self.session.take() else {
            return Ok(VoiceActivationChange::AlreadyDisabled);
        };
        let _ = session.close()?;
        Ok(VoiceActivationChange::Disabled)
    }
}

impl Drop for VoiceActivation {
    fn drop(&mut self) {
        if let Some(session) = self.session.as_mut() {
            let _ = session.close();
        }
    }
}

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
    capture_request: Option<CaptureRequest>,
    submission_pending: bool,
    pending_capture: Option<CaptureReport>,
    streamed_run: Option<StreamedVoiceRun>,
    pending_commit: Option<PendingVoiceCommit>,
}

/// One asynchronous outcome from the TUI voice bridge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VoiceSessionEvent {
    /// One final transcript is ready for the ordinary TUI submission route.
    Captured {
        /// Exact final recognizer text.
        transcript: String,
        /// Prior interrupted run, present only after its commit acknowledgement.
        prior_interrupted_run_id: Option<String>,
    },
    /// Local playback is silent and the matching daemon run should be canceled.
    CancelRun {
        /// Exact run whose playback triggered barge-in.
        run_id: String,
    },
    /// One exact in-memory raw batch is ready for the existing daemon commit method.
    Commit {
        /// Terminal run receiving the batch.
        run_id: String,
        /// Complete raw batch, retained unchanged until acknowledgement.
        events: Vec<VoiceEvent>,
    },
}

struct StreamedVoiceRun {
    run_id: RunId,
    capture: Option<CaptureReport>,
    capture_turn_id: Option<TurnId>,
    stream: AssistantTextStream,
    accepted_sources: BTreeMap<u64, AcceptedNarrationSource>,
    sentences: Vec<NarratedSentenceReport>,
    interruption: Option<SpokenInterruption>,
    next_capture: Option<CaptureReport>,
    terminal: Option<RunStateName>,
    audio_finished: bool,
    barge_cancel_sent: bool,
    narration_abandoned: bool,
}

struct PendingVoiceCommit {
    run_id: String,
    events: Vec<VoiceEvent>,
    next_capture: Option<CaptureReport>,
    retried: bool,
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
            capture_request: None,
            submission_pending: false,
            pending_capture: None,
            streamed_run: None,
            pending_commit: None,
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

    /// Arms one bounded hands-free capture when no submission or commit is active.
    pub fn arm_capture(&mut self) -> Result<(), VoiceError> {
        if self.capture_request.is_some()
            || self.submission_pending
            || self.pending_capture.is_some()
            || self.streamed_run.is_some()
            || self.pending_commit.is_some()
        {
            return Ok(());
        }
        let capture = self
            .capture
            .as_ref()
            .ok_or(VoiceError::CaptureUnavailable)?;
        self.capture_request = Some(capture.arm_capture(Duration::from_secs(30))?);
        Ok(())
    }

    /// Disarms idle capture before the ordinary daemon submission begins.
    pub fn submission_started(&mut self) -> Result<(), VoiceError> {
        self.submission_pending = true;
        if self.capture_request.is_some() {
            self.capture
                .as_ref()
                .ok_or(VoiceError::CaptureUnavailable)?
                .cancel_capture()?;
            self.capture_request = None;
        }
        Ok(())
    }

    /// Drops an unbound captured question after daemon admission failed.
    pub fn submission_failed(&mut self) -> Result<(), VoiceError> {
        self.submission_pending = false;
        self.pending_capture = None;
        self.arm_capture()
    }

    /// Binds the current captured question, when any, to one daemon-minted run.
    pub fn observe_run(&mut self, run_id: &str) -> Result<(), VoiceError> {
        let run_id =
            RunId::new(run_id.to_owned()).map_err(|error| contract_error(error.to_string()))?;
        if let Some(active) = self.streamed_run.as_ref() {
            if active.run_id == run_id {
                return Ok(());
            }
            return Err(contract_error("voice bridge observed a second active run"));
        }
        if self.pending_commit.is_some() {
            return Err(contract_error(
                "voice bridge observed a run before the prior voice commit was acknowledged",
            ));
        }
        self.submission_started()?;
        self.submission_pending = false;
        self.worker
            .as_ref()
            .ok_or(VoiceError::SessionClosed)?
            .begin_run()?;
        self.streamed_run = Some(StreamedVoiceRun {
            run_id,
            capture: self.pending_capture.take(),
            capture_turn_id: None,
            stream: AssistantTextStream::default(),
            accepted_sources: BTreeMap::new(),
            sentences: Vec::new(),
            interruption: None,
            next_capture: None,
            terminal: None,
            audio_finished: false,
            barge_cancel_sent: false,
            narration_abandoned: false,
        });
        Ok(())
    }

    /// Feeds one existing daemon stream event into the authoritative text/synthesis owners.
    pub fn accept_stream_event(&mut self, event: StreamEvent) -> Result<(), VoiceError> {
        let Some(run_id) = stream_event_run_id(&event) else {
            return Ok(());
        };
        self.observe_run(&run_id)?;
        let active = self
            .streamed_run
            .as_mut()
            .expect("observing a run creates streamed voice state");
        if active.run_id.as_str() != run_id {
            return Err(contract_error("voice stream event run ID changed"));
        }
        if let StreamEvent::Ledger { record } = &event
            && let HarnessEvent::ContextBuilt { turn_id, .. } = &record.event
        {
            active
                .capture_turn_id
                .get_or_insert_with(|| turn_id.clone());
        }
        let event = match event {
            StreamEvent::Ledger { record } => RunEvent::Ledger(record),
            StreamEvent::AssistantDelta {
                run_id,
                turn_id,
                step,
                delta_index,
                text,
            } => RunEvent::AssistantDelta(AssistantDeltaEvent {
                run_id: RunId::new(run_id).map_err(|error| contract_error(error.to_string()))?,
                turn_id: TurnId::new(turn_id).map_err(|error| contract_error(error.to_string()))?,
                step,
                delta_index,
                text,
            }),
            StreamEvent::ApprovalRequested { .. }
            | StreamEvent::Canceled { .. }
            | StreamEvent::CompletionClaimed { .. }
            | StreamEvent::Unknown(_) => return Ok(()),
        };
        if active.narration_abandoned {
            return Ok(());
        }
        let accepted = active.stream.accept(event)?;
        let worker = self.worker.as_ref().ok_or(VoiceError::SessionClosed)?;
        for narrated in accepted {
            match worker.try_accept(narrated.sentence, narrated.source) {
                Ok(admission) => {
                    if active
                        .accepted_sources
                        .insert(
                            admission.sequence,
                            AcceptedNarrationSource {
                                key: narrated.key,
                                source: narrated.source,
                            },
                        )
                        .is_some()
                    {
                        return Err(contract_error(
                            "synthesis reused a worker sequence within one streamed run",
                        ));
                    }
                    append_completed_sentences(active, admission.completed);
                }
                Err(SynthWorkerError::Canceled)
                    if worker.barge_in_metrics().speech_onset_decision_ns.is_some() => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    /// Records the daemon's terminal status; polling finishes audio and emits any batch.
    pub fn observe_terminal(
        &mut self,
        run_id: &str,
        status: RunStateName,
    ) -> Result<(), VoiceError> {
        let active = self
            .streamed_run
            .as_mut()
            .ok_or_else(|| contract_error("voice terminal arrived without an active run"))?;
        if active.run_id.as_str() != run_id {
            return Err(contract_error("voice terminal run ID changed"));
        }
        if matches!(
            status,
            RunStateName::Running | RunStateName::CancelRequested
        ) {
            return Err(contract_error(
                "voice terminal carried a nonterminal status",
            ));
        }
        active.terminal = Some(status);
        Ok(())
    }

    /// Silences local playback for plain Ctrl-C without producing interruption facts.
    pub fn cancel_run(&mut self, run_id: &str) -> Result<(), VoiceError> {
        let mut active = self
            .streamed_run
            .take()
            .ok_or_else(|| contract_error("voice cancel arrived without an active run"))?;
        if active.run_id.as_str() != run_id {
            self.streamed_run = Some(active);
            return Err(contract_error("voice cancel run ID changed"));
        }
        self.cancel.store(true, Ordering::Release);
        finish_stream_audio(self.worker.as_ref(), &mut active)?;
        active.interruption = None;
        active.next_capture = None;
        active.narration_abandoned = true;
        active.barge_cancel_sent = true;
        if let Some(capture) = self.capture.as_ref() {
            capture.cancel_capture()?;
        }
        self.streamed_run = Some(active);
        Ok(())
    }

    /// Silences and abandons narration while leaving the daemon text run untouched.
    pub fn abandon_run(&mut self) -> Result<(), VoiceError> {
        self.submission_pending = true;
        self.pending_capture = None;
        if self.capture_request.is_some() {
            self.capture
                .as_ref()
                .ok_or(VoiceError::CaptureUnavailable)?
                .cancel_capture()?;
            self.capture_request = None;
        }
        let Some(mut active) = self.streamed_run.take() else {
            return Ok(());
        };
        self.cancel.store(true, Ordering::Release);
        finish_stream_audio(self.worker.as_ref(), &mut active)?;
        active.interruption = None;
        active.next_capture = None;
        active.narration_abandoned = true;
        if let Some(capture) = self.capture.as_ref() {
            capture.cancel_capture()?;
        }
        self.streamed_run = Some(active);
        Ok(())
    }

    /// Reconciles an abandoned run after the normal TUI reload path recovers.
    pub fn observe_loaded_run(&mut self, active_run_id: Option<&str>) -> Result<(), VoiceError> {
        let Some(mut active) = self.streamed_run.take() else {
            return match active_run_id {
                Some(run_id) => self.observe_run(run_id),
                None => {
                    self.submission_pending = false;
                    self.arm_capture()
                }
            };
        };
        if active_run_id == Some(active.run_id.as_str()) || active.terminal.is_some() {
            self.streamed_run = Some(active);
            return Ok(());
        }
        if !active.narration_abandoned {
            self.streamed_run = Some(active);
            return Err(contract_error(
                "voice reload dropped an unabandoned active run",
            ));
        }
        finish_stream_audio(self.worker.as_ref(), &mut active)?;
        self.submission_pending = false;
        self.arm_capture()
    }

    /// Polls capture, barge-in, terminal audio, and commit readiness without blocking daemon I/O.
    pub fn poll_bridge(&mut self) -> Result<Vec<VoiceSessionEvent>, VoiceError> {
        let mut events = Vec::new();
        if let Some(request) = self.capture_request.as_ref() {
            match request.try_complete() {
                Ok(Some(report)) => {
                    self.capture_request = None;
                    let transcript = report.transcript.text.clone();
                    self.pending_capture = Some(report);
                    events.push(VoiceSessionEvent::Captured {
                        transcript,
                        prior_interrupted_run_id: None,
                    });
                }
                Ok(None) => {}
                Err(CaptureError::Timeout { .. } | CaptureError::Canceled) => {
                    self.capture_request = None;
                }
                Err(error) => {
                    self.capture_request = None;
                    return Err(error.into());
                }
            }
        }

        if let Some(report) = self
            .capture
            .as_ref()
            .ok_or(VoiceError::CaptureUnavailable)?
            .poll_barge_in_capture()?
        {
            let active = self.streamed_run.as_mut().ok_or_else(|| {
                contract_error("barge-in capture completed without an active run")
            })?;
            if !active.narration_abandoned && active.next_capture.replace(report).is_some() {
                return Err(contract_error(
                    "one streamed run produced more than one barge-in capture",
                ));
            }
        }

        let barge_started = self
            .worker
            .as_ref()
            .ok_or(VoiceError::SessionClosed)?
            .barge_in_metrics()
            .speech_onset_decision_ns
            .is_some();
        if self
            .streamed_run
            .as_ref()
            .is_some_and(|active| barge_started && !active.barge_cancel_sent)
        {
            let mut active = self.streamed_run.take().expect("checked active run");
            finish_stream_audio(self.worker.as_ref(), &mut active)?;
            active.barge_cancel_sent = true;
            events.push(VoiceSessionEvent::CancelRun {
                run_id: active.run_id.to_string(),
            });
            self.streamed_run = Some(active);
        }

        let ready = self.streamed_run.as_ref().is_some_and(|active| {
            active.terminal.is_some()
                && (active.narration_abandoned
                    || !active.barge_cancel_sent
                    || active.next_capture.is_some())
        });
        if ready {
            let mut active = self.streamed_run.take().expect("checked active run");
            finish_stream_audio(self.worker.as_ref(), &mut active)?;
            if let Err(error) = active.stream.finish() {
                if active.narration_abandoned || active.terminal != Some(RunStateName::Finished) {
                    active.narration_abandoned = true;
                    active.sentences.clear();
                    active.accepted_sources.clear();
                    active.interruption = None;
                } else {
                    return Err(match error {
                        VoiceRunError::Voice(error) => error,
                        _ => contract_error(error.to_string()),
                    });
                }
            }
            if active.capture_turn_id.is_none()
                && (active.narration_abandoned || active.terminal != Some(RunStateName::Finished))
            {
                active.capture = None;
            }
            let raw = voice_events_for_observations(
                &active.run_id,
                &active.sentences,
                active.capture.as_ref(),
                active.capture_turn_id.as_ref(),
                &active.accepted_sources,
                active.interruption.as_ref(),
                |sequence| {
                    self.worker
                        .as_ref()
                        .and_then(|worker| worker.accepted_to_first_non_silent_us(sequence))
                },
            )?;
            if active.next_capture.is_some()
                && !raw.iter().any(|event| {
                    matches!(
                        event,
                        VoiceEvent::VoiceInterrupted { run_id, .. }
                            if run_id == &active.run_id
                    )
                })
            {
                return Err(contract_error(
                    "barge-in utterance completed without an interrupted voice fact",
                ));
            }
            if !raw.is_empty() {
                let run_id = active.run_id.to_string();
                self.pending_commit = Some(PendingVoiceCommit {
                    run_id: run_id.clone(),
                    events: raw.clone(),
                    next_capture: active.next_capture,
                    retried: false,
                });
                events.push(VoiceSessionEvent::Commit {
                    run_id,
                    events: raw,
                });
            }
            self.submission_pending = false;
        }
        self.worker
            .as_ref()
            .ok_or(VoiceError::SessionClosed)?
            .check_health()
            .map_err(|failure| VoiceError::Worker(SynthWorkerError::Failed(failure)))?;
        self.capture
            .as_ref()
            .ok_or(VoiceError::CaptureUnavailable)?
            .check_health()?;
        Ok(events)
    }

    /// Releases a retained barge-in utterance only after exact commit acknowledgement.
    pub fn acknowledge_commit(
        &mut self,
        run_id: &str,
    ) -> Result<Option<VoiceSessionEvent>, VoiceError> {
        let pending = self
            .pending_commit
            .take()
            .ok_or_else(|| contract_error("voice commit acknowledgement had no pending batch"))?;
        if pending.run_id != run_id {
            self.pending_commit = Some(pending);
            return Err(contract_error(
                "voice commit acknowledgement run ID changed",
            ));
        }
        let Some(report) = pending.next_capture else {
            return Ok(None);
        };
        let transcript = report.transcript.text.clone();
        self.pending_capture = Some(report);
        Ok(Some(VoiceSessionEvent::Captured {
            transcript,
            prior_interrupted_run_id: Some(run_id.to_owned()),
        }))
    }

    /// Returns the unchanged live batch once after a failed acknowledgement attempt.
    pub fn retry_commit(&mut self, run_id: &str) -> Result<Option<VoiceSessionEvent>, VoiceError> {
        let pending = self
            .pending_commit
            .as_mut()
            .ok_or_else(|| contract_error("voice commit retry had no pending batch"))?;
        if pending.run_id != run_id {
            return Err(contract_error("voice commit retry run ID changed"));
        }
        if pending.retried {
            return Ok(None);
        }
        pending.retried = true;
        Ok(Some(VoiceSessionEvent::Commit {
            run_id: pending.run_id.clone(),
            events: pending.events.clone(),
        }))
    }

    fn close(&mut self) -> Result<Option<VoiceSessionShutdown>, VoiceError> {
        let Some(worker) = self.worker.take() else {
            return Ok(None);
        };
        let capture = self.capture.take().map(CaptureWorker::shutdown);
        let synthesis = worker
            .shutdown()
            .map_err(|failure| VoiceError::Worker(SynthWorkerError::Failed(failure)))?;
        Ok(Some(VoiceSessionShutdown { capture, synthesis }))
    }

    /// Closes admission, drains accepted audio, and joins the synth worker.
    pub fn shutdown(mut self) -> Result<VoiceSessionShutdown, VoiceError> {
        self.close()?.ok_or(VoiceError::SessionClosed)
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

impl Drop for VoiceSession {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

fn stream_event_run_id(event: &StreamEvent) -> Option<String> {
    match event {
        StreamEvent::Ledger { record } => Some(record.event.run_id().to_string()),
        StreamEvent::AssistantDelta { run_id, .. }
        | StreamEvent::ApprovalRequested { run_id, .. }
        | StreamEvent::Canceled { run_id }
        | StreamEvent::CompletionClaimed { run_id, .. } => Some(run_id.clone()),
        StreamEvent::Unknown(_) => None,
    }
}

fn append_completed_sentences(
    active: &mut StreamedVoiceRun,
    reports: Vec<SynthesizedSentenceReport>,
) {
    active
        .sentences
        .extend(reports.into_iter().map(|report| NarratedSentenceReport {
            sentence: report.sentence,
            playback: report.playback,
        }));
}

fn finish_stream_audio(
    worker: Option<&SynthWorker>,
    active: &mut StreamedVoiceRun,
) -> Result<(), VoiceError> {
    if active.audio_finished {
        return Ok(());
    }
    let worker = worker.ok_or(VoiceError::SessionClosed)?;
    let completed = worker
        .wait_until_idle()
        .map_err(|failure| VoiceError::Worker(SynthWorkerError::Failed(failure)))?;
    append_completed_sentences(active, completed);
    active.interruption = worker.finish_run()?;
    active.audio_finished = true;
    Ok(())
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

fn voice_events_for_run(
    outcome: &NarratedRunOutcome,
    capture: Option<&CaptureReport>,
    first_response_key: Option<&ResponseKey>,
    accepted: &BTreeMap<u64, AcceptedNarrationSource>,
    interruption: Option<&SpokenInterruption>,
    worker: &SynthWorker,
) -> Result<Vec<VoiceEvent>, VoiceError> {
    let capture_turn_id = first_response_key.map(|key| &key.turn_id);
    voice_events_for_observations(
        &outcome.run.run_id,
        &outcome.narration.sentences,
        capture,
        capture_turn_id,
        accepted,
        interruption,
        |sequence| worker.accepted_to_first_non_silent_us(sequence),
    )
}

fn voice_events_for_observations(
    run_id: &RunId,
    sentences: &[NarratedSentenceReport],
    capture: Option<&CaptureReport>,
    capture_turn_id: Option<&TurnId>,
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
        let turn_id = capture_turn_id
            .ok_or_else(|| contract_error("captured run completed without a durable first turn"))?;
        events.push(captured_event(run_id.clone(), turn_id.clone(), capture));
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
    fn trusted_voice_config_resolves_exact_artifacts_and_devices() {
        let config = VoiceConfig::from_trusted_toml(
            r#"
[voice]
kokoro_model = "models/kokoro"
whisper_model = "models/whisper.bin"
silero_model = "/opt/models/silero.onnx"
capture_device = "cpal:input-7"
playback_device = "cpal:output-9"
"#,
            Path::new("/trusted/config"),
        )
        .unwrap();

        assert_eq!(
            config.kokoro().model_path(),
            Path::new("/trusted/config/models/kokoro/model.onnx")
        );
        assert_eq!(
            config.whisper().model_path(),
            Path::new("/trusted/config/models/whisper.bin")
        );
        assert_eq!(
            config.silero().model_path(),
            Path::new("/opt/models/silero.onnx")
        );
        assert_eq!(
            config.capture().device(),
            &InputDeviceSelection::Id("cpal:input-7".to_owned())
        );
        assert_eq!(
            config.playback().device(),
            &OutputDeviceSelection::Id("cpal:output-9".to_owned())
        );
    }

    #[test]
    fn voice_config_is_typed_and_fail_closed_for_absent_or_incomplete_input() {
        assert_eq!(
            VoiceConfig::from_trusted_toml("", Path::new("/tmp")).unwrap_err(),
            VoiceConfigError::MissingTable
        );
        assert!(matches!(
            VoiceConfig::from_trusted_toml("[provider]\nmodel = 'gpt-test'", Path::new("/tmp")),
            Err(VoiceConfigError::InvalidDocument { .. })
        ));

        let cases = [
            (
                "kokoro_model",
                "[voice]\nwhisper_model='w'\nsilero_model='s'\n",
            ),
            (
                "whisper_model",
                "[voice]\nkokoro_model='k'\nsilero_model='s'\n",
            ),
            (
                "silero_model",
                "[voice]\nkokoro_model='k'\nwhisper_model='w'\n",
            ),
        ];
        for (field, document) in cases {
            assert_eq!(
                VoiceConfig::from_trusted_toml(document, Path::new("/tmp")).unwrap_err(),
                VoiceConfigError::MissingField { field }
            );
        }

        assert_eq!(
            VoiceConfig::from_trusted_toml(
                "[voice]\nkokoro_model='k'\nwhisper_model='w'\nsilero_model='  '\n",
                Path::new("/tmp"),
            )
            .unwrap_err(),
            VoiceConfigError::EmptyField {
                field: "silero_model"
            }
        );
        assert!(matches!(
            VoiceConfig::from_trusted_toml(
                "[voice]\nkokoro_model='k'\nwhisper_model='w'\nsilero_model='s'\nauto_download=true\n",
                Path::new("/tmp"),
            ),
            Err(VoiceConfigError::InvalidDocument { .. })
        ));
    }

    #[test]
    fn omitted_devices_use_host_defaults_without_discovery() {
        let config = VoiceConfig::from_trusted_toml(
            "[voice]\nkokoro_model='k'\nwhisper_model='w'\nsilero_model='s'\n",
            Path::new("/trusted"),
        )
        .unwrap();

        assert_eq!(config.capture().device(), &InputDeviceSelection::Default);
        assert_eq!(config.playback().device(), &OutputDeviceSelection::Default);
    }

    #[test]
    fn activation_starts_off_and_denial_or_missing_config_never_opens_devices() {
        let mut activation = VoiceActivation::from_explicit_config(None);

        assert!(!activation.is_enabled());
        assert_eq!(
            activation.enable(VoiceGrant::Denied).unwrap(),
            VoiceActivationChange::Denied
        );
        assert!(!activation.is_enabled());
        assert!(matches!(
            activation.enable(VoiceGrant::Granted),
            Err(VoiceActivationError::Config(VoiceConfigError::MissingTable))
        ));
        assert_eq!(
            activation.disable().unwrap(),
            VoiceActivationChange::AlreadyDisabled
        );
        assert_eq!(
            activation.disable().unwrap(),
            VoiceActivationChange::AlreadyDisabled
        );
    }

    #[test]
    fn explicit_voice_config_reads_only_the_selected_file_and_uses_its_directory() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("voice.toml");
        fs::write(
            &path,
            "[voice]\nkokoro_model='k'\nwhisper_model='w'\nsilero_model='s'\n",
        )
        .unwrap();

        let activation = VoiceActivation::from_explicit_config(Some(&path));
        let config = activation.config.as_ref().unwrap();
        assert_eq!(
            config.kokoro().model_path(),
            directory.path().join("k/model.onnx")
        );
        assert_eq!(config.whisper().model_path(), directory.path().join("w"));
        assert_eq!(config.silero().model_path(), directory.path().join("s"));

        let missing = directory.path().join("missing.toml");
        let activation = VoiceActivation::from_explicit_config(Some(&missing));
        assert!(matches!(
            &activation.config,
            Err(VoiceConfigError::Read { path, .. }) if path == &missing
        ));
    }

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
