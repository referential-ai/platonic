use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use serde::Serialize;
#[cfg(feature = "whisper-cuda")]
use sha2::{Digest, Sha256};
#[cfg(feature = "whisper-cuda")]
use std::io::Read;
#[cfg(feature = "whisper-cuda")]
use std::sync::Mutex;
#[cfg(feature = "whisper-cuda")]
use tracing::{
    Event, Metadata, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
};
#[cfg(feature = "whisper-cuda")]
use whisper_rs::{
    FullParams, SamplingStrategy, WhisperContext, WhisperContextParameters, WhisperState,
};

#[cfg(feature = "whisper-cuda")]
use crate::SampleFormat;
use crate::{AudioFormat, InferenceBackend, PcmFrame, SttError};

/// Immutable model repository containing the admitted ggml artifact.
pub const WHISPER_MODEL_SOURCE: &str = "ggerganov/whisper.cpp";
/// Verified repository commit that introduced large-v3-turbo.
pub const WHISPER_MODEL_REVISION: &str = "6034871ec87c84e342efab769d4c5c06cd126db3";
/// SHA-256 of the admitted unquantized `ggml-large-v3-turbo.bin` artifact.
pub const WHISPER_MODEL_SHA256: &str =
    "1fc70f774d38eb169993ac391eea357ef47c88757ef72ee5943879b7e8e2bc69";
/// Exact Rust wrapper used to embed whisper.cpp.
pub const WHISPER_RS_RUNTIME_VERSION: &str = "whisper-rs 0.16.0";
/// Maximum PCM retained in each rolling partial re-decode.
pub const WHISPER_PARTIAL_WINDOW_MS: u64 = 5_000;
/// Exact PCM cadence between eligible rolling partial re-decodes.
pub const WHISPER_PARTIAL_CADENCE_MS: u64 = 160;
/// Minimum active speech span before the first partial re-decode.
pub const WHISPER_PARTIAL_MINIMUM_MS: u64 = 320;

#[cfg(feature = "whisper-cuda")]
const WHISPER_SAMPLE_RATE: u32 = 16_000;
#[cfg(feature = "whisper-cuda")]
const MAX_DIAGNOSTIC_CHARS: usize = 2_048;
#[cfg(feature = "whisper-cuda")]
const MAX_BACKEND_LOG_MESSAGES: usize = 128;

/// Rolling or final recognized text with its exact accepted PCM span.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Transcript {
    /// Recognized UTF-8 text, trimmed at the model boundary.
    pub text: String,
    /// Whether this text commits the VAD-closed utterance.
    pub is_final: bool,
    /// Accepted PCM duration in whole milliseconds.
    pub span_ms: u64,
}

impl Transcript {
    /// Constructs nonempty rolling or final recognized text.
    pub fn new(text: impl Into<String>, is_final: bool, span_ms: u64) -> Result<Self, SttError> {
        let text = text.into().trim().to_owned();
        if text.is_empty() {
            return Err(SttError::EmptyTranscript);
        }
        Ok(Self {
            text,
            is_final,
            span_ms,
        })
    }
}

/// One warm-resident PCM-to-text engine.
pub trait SpeechRecognizer: Send {
    /// Returns the exact PCM format accepted by [`Self::accept`].
    fn input_format(&self) -> AudioFormat;

    /// Accepts one normalized PCM frame and may return rolling transcripts.
    fn accept(&mut self, frame: &PcmFrame) -> Result<Vec<Transcript>, SttError>;

    /// Discards an unfinished utterance without reconstructing resident model state.
    fn reset(&mut self);

    /// Closes the current VAD endpoint and returns one committed transcript.
    fn finalize(&mut self) -> Result<Transcript, SttError>;
}

/// Path to the pinned large-v3-turbo artifact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WhisperConfig {
    model_path: PathBuf,
}

impl WhisperConfig {
    /// Selects one local model file. Its pinned checksum is always verified.
    pub fn new(model_path: impl Into<PathBuf>) -> Self {
        Self {
            model_path: model_path.into(),
        }
    }

    /// Returns the selected local artifact path.
    pub fn model_path(&self) -> &Path {
        &self.model_path
    }
}

/// Exact model, wrapper, whisper.cpp, and CUDA identity captured at load.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WhisperProvenance {
    /// Immutable model repository.
    pub model_source: String,
    /// Immutable model commit.
    pub model_revision: String,
    /// Verified model digest.
    pub model_sha256: String,
    /// Pinned Rust wrapper line.
    pub whisper_rs_runtime: String,
    /// Vendored whisper.cpp version reported by the wrapper.
    pub whisper_cpp_version: String,
    /// Bounded compile-capability report from whisper.cpp.
    pub system_info: String,
    /// Admitted resident backend.
    pub backend: InferenceBackend,
    /// Runtime CUDA device selected by whisper.cpp.
    pub cuda_device: u32,
    /// Bounded in-process runtime evidence for the selected backend.
    pub backend_evidence: String,
    /// Fixed rolling partial window bound.
    pub partial_window_ms: u64,
    /// Fixed rolling partial re-decode cadence.
    pub partial_cadence_ms: u64,
}

/// Observable resident-recognizer reuse counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct WhisperMetrics {
    /// Successful model/context constructions. This remains one across utterances.
    pub model_loads: u64,
    /// Successful final utterance decodes.
    pub finalizations: u64,
    /// Successful bounded rolling decode calls.
    pub partial_decodes: u64,
    /// Nonempty changed partial hypotheses returned to capture.
    pub partial_updates: u64,
}

#[derive(Default)]
struct WhisperMetricCounters {
    model_loads: AtomicU64,
    finalizations: AtomicU64,
    partial_decodes: AtomicU64,
    partial_updates: AtomicU64,
}

/// Cloneable read-only access to resident recognizer counters.
#[derive(Clone)]
pub struct WhisperMetricsReader {
    counters: Arc<WhisperMetricCounters>,
}

impl WhisperMetricsReader {
    /// Reads monotonic load and finalization counters.
    pub fn snapshot(&self) -> WhisperMetrics {
        WhisperMetrics {
            model_loads: self.counters.model_loads.load(Ordering::Relaxed),
            finalizations: self.counters.finalizations.load(Ordering::Relaxed),
            partial_decodes: self.counters.partial_decodes.load(Ordering::Relaxed),
            partial_updates: self.counters.partial_updates.load(Ordering::Relaxed),
        }
    }
}

#[cfg(feature = "whisper-cuda")]
#[derive(Clone, Default)]
struct BackendLogCapture {
    messages: Arc<Mutex<Vec<String>>>,
}

#[cfg(feature = "whisper-cuda")]
impl BackendLogCapture {
    fn snapshot(&self) -> Vec<String> {
        self.messages
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

#[cfg(feature = "whisper-cuda")]
struct BackendLogSubscriber {
    capture: BackendLogCapture,
}

#[cfg(feature = "whisper-cuda")]
impl Subscriber for BackendLogSubscriber {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target().starts_with("whisper_rs")
    }

    fn new_span(&self, _span: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut visitor = BackendLogVisitor::default();
        event.record(&mut visitor);
        let mut messages = self
            .capture
            .messages
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for line in visitor
            .message
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
        {
            if messages.len() >= MAX_BACKEND_LOG_MESSAGES {
                break;
            }
            messages.push(bounded(line));
        }
    }

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
}

#[cfg(feature = "whisper-cuda")]
#[derive(Default)]
struct BackendLogVisitor {
    message: String,
}

#[cfg(feature = "whisper-cuda")]
impl Visit for BackendLogVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message.clear();
            self.message.push_str(value);
        }
    }
}

#[cfg(feature = "whisper-cuda")]
struct CudaBackendAdmission {
    device: u32,
    evidence: String,
}

/// Resident whisper.cpp large-v3-turbo CUDA recognizer.
pub struct WhisperRecognizer {
    #[cfg(feature = "whisper-cuda")]
    state: WhisperState,
    samples: Vec<f32>,
    samples_since_partial: usize,
    last_partial: String,
    input_format: AudioFormat,
    provenance: WhisperProvenance,
    metrics: Arc<WhisperMetricCounters>,
}

impl WhisperRecognizer {
    /// Verifies and loads the admitted CUDA model exactly once.
    pub fn load(config: WhisperConfig) -> Result<Self, SttError> {
        #[cfg(not(feature = "whisper-cuda"))]
        {
            let _ = config;
            Err(SttError::CudaUnavailable {
                platform: std::env::consts::OS,
            })
        }

        #[cfg(feature = "whisper-cuda")]
        {
            whisper_rs::install_logging_hooks();
            let actual = sha256_file(&config.model_path)?;
            if actual != WHISPER_MODEL_SHA256 {
                return Err(SttError::ArtifactChecksum {
                    path: config.model_path,
                    expected: WHISPER_MODEL_SHA256,
                    actual,
                });
            }

            let system_info = bounded(whisper_rs::print_system_info());
            if !system_info.contains("CUDA :") {
                return Err(SttError::CudaUnavailable {
                    platform: std::env::consts::OS,
                });
            }
            let mut parameters = WhisperContextParameters::default();
            parameters.use_gpu(true).flash_attn(true).gpu_device(0);
            let backend_logs = BackendLogCapture::default();
            let subscriber = BackendLogSubscriber {
                capture: backend_logs.clone(),
            };
            let state = tracing::subscriber::with_default(subscriber, || {
                let context = WhisperContext::new_with_params(&config.model_path, parameters)
                    .map_err(|error| SttError::ModelLoad {
                        reason: bounded(&error.to_string()),
                    })?;
                context
                    .create_state()
                    .map_err(|error| SttError::StateCreation {
                        reason: bounded(&error.to_string()),
                    })
            })?;
            let messages = backend_logs.snapshot();
            let admission = admit_cuda_backend(&messages)?;
            let metrics = Arc::new(WhisperMetricCounters::default());
            metrics.model_loads.store(1, Ordering::Relaxed);
            Ok(Self {
                state,
                samples: Vec::new(),
                samples_since_partial: 0,
                last_partial: String::new(),
                input_format: AudioFormat::new(WHISPER_SAMPLE_RATE, 1, SampleFormat::F32)
                    .expect("literal Whisper format is valid"),
                provenance: WhisperProvenance {
                    model_source: WHISPER_MODEL_SOURCE.to_owned(),
                    model_revision: WHISPER_MODEL_REVISION.to_owned(),
                    model_sha256: actual,
                    whisper_rs_runtime: WHISPER_RS_RUNTIME_VERSION.to_owned(),
                    whisper_cpp_version: whisper_rs::WHISPER_CPP_VERSION.to_owned(),
                    system_info,
                    backend: InferenceBackend::Cuda,
                    cuda_device: admission.device,
                    backend_evidence: admission.evidence,
                    partial_window_ms: WHISPER_PARTIAL_WINDOW_MS,
                    partial_cadence_ms: WHISPER_PARTIAL_CADENCE_MS,
                },
                metrics,
            })
        }
    }

    /// Returns exact artifact and runtime identity captured at warm load.
    pub fn provenance(&self) -> &WhisperProvenance {
        &self.provenance
    }

    /// Returns a reader that remains valid after the recognizer enters its worker.
    pub fn metrics_reader(&self) -> WhisperMetricsReader {
        WhisperMetricsReader {
            counters: Arc::clone(&self.metrics),
        }
    }
}

impl SpeechRecognizer for WhisperRecognizer {
    fn input_format(&self) -> AudioFormat {
        self.input_format
    }

    fn accept(&mut self, frame: &PcmFrame) -> Result<Vec<Transcript>, SttError> {
        if frame.format() != self.input_format {
            return Err(SttError::FormatMismatch {
                expected: self.input_format,
                actual: frame.format(),
            });
        }
        let sample = frame
            .samples()
            .as_f32()
            .expect("Whisper format contract is f32")[0];
        self.samples.push(sample);
        self.samples_since_partial = self.samples_since_partial.saturating_add(1);

        #[cfg(not(feature = "whisper-cuda"))]
        {
            Ok(Vec::new())
        }

        #[cfg(feature = "whisper-cuda")]
        {
            let minimum_samples = milliseconds_to_samples(WHISPER_PARTIAL_MINIMUM_MS);
            let cadence_samples = milliseconds_to_samples(WHISPER_PARTIAL_CADENCE_MS);
            if self.samples.len() < minimum_samples || self.samples_since_partial < cadence_samples
            {
                return Ok(Vec::new());
            }
            self.samples_since_partial = 0;
            let window = partial_window(&self.samples).to_vec();
            let text = self.decode_text(&window)?;
            self.metrics.partial_decodes.fetch_add(1, Ordering::Relaxed);
            let text = text.trim();
            if text.is_empty() || text == self.last_partial {
                return Ok(Vec::new());
            }
            self.last_partial.clear();
            self.last_partial.push_str(text);
            let span_ms = samples_to_milliseconds(self.samples.len());
            let transcript = Transcript::new(text, false, span_ms)?;
            self.metrics.partial_updates.fetch_add(1, Ordering::Relaxed);
            Ok(vec![transcript])
        }
    }

    fn reset(&mut self) {
        self.samples.clear();
        self.samples_since_partial = 0;
        self.last_partial.clear();
    }

    fn finalize(&mut self) -> Result<Transcript, SttError> {
        #[cfg(not(feature = "whisper-cuda"))]
        {
            Err(SttError::CudaUnavailable {
                platform: std::env::consts::OS,
            })
        }

        #[cfg(feature = "whisper-cuda")]
        {
            if self.samples.is_empty() {
                return Err(SttError::NoAudio);
            }
            let audio = std::mem::take(&mut self.samples);
            let span_ms = samples_to_milliseconds(audio.len());
            let text = self.decode_text(&audio);
            self.samples_since_partial = 0;
            self.last_partial.clear();
            let transcript = Transcript::new(text?, true, span_ms)?;
            self.metrics.finalizations.fetch_add(1, Ordering::Relaxed);
            Ok(transcript)
        }
    }
}

#[cfg(feature = "whisper-cuda")]
impl WhisperRecognizer {
    fn decode_text(&mut self, audio: &[f32]) -> Result<String, SttError> {
        let mut params = FullParams::new(SamplingStrategy::Greedy { best_of: 1 });
        params.set_language(Some("en"));
        params.set_translate(false);
        params.set_no_context(true);
        params.set_single_segment(false);
        params.set_print_special(false);
        params.set_print_progress(false);
        params.set_print_realtime(false);
        params.set_print_timestamps(false);
        params.set_no_timestamps(true);
        self.state
            .full(params, audio)
            .map_err(|error| SttError::Inference {
                reason: bounded(&error.to_string()),
            })?;
        self.state
            .as_iter()
            .try_fold(String::new(), |mut text, segment| {
                let segment = segment.to_str().map_err(|error| SttError::Inference {
                    reason: bounded(&error.to_string()),
                })?;
                text.push_str(segment);
                Ok::<_, SttError>(text)
            })
    }
}

#[cfg(feature = "whisper-cuda")]
fn milliseconds_to_samples(milliseconds: u64) -> usize {
    usize::try_from(
        u64::from(WHISPER_SAMPLE_RATE)
            .saturating_mul(milliseconds)
            .div_ceil(1_000),
    )
    .unwrap_or(usize::MAX)
}

#[cfg(feature = "whisper-cuda")]
fn samples_to_milliseconds(samples: usize) -> u64 {
    (samples as u64).saturating_mul(1_000) / u64::from(WHISPER_SAMPLE_RATE)
}

#[cfg(feature = "whisper-cuda")]
fn partial_window(samples: &[f32]) -> &[f32] {
    let maximum = milliseconds_to_samples(WHISPER_PARTIAL_WINDOW_MS);
    &samples[samples.len().saturating_sub(maximum)..]
}

#[cfg(feature = "whisper-cuda")]
fn admit_cuda_backend(messages: &[String]) -> Result<CudaBackendAdmission, SttError> {
    const SELECTED_PREFIX: &str = "whisper_backend_init_gpu: using CUDA";
    for message in messages {
        let Some((_, selected)) = message.split_once(SELECTED_PREFIX) else {
            continue;
        };
        let Some((device, suffix)) = selected.trim().split_once(' ') else {
            continue;
        };
        if suffix != "backend" {
            continue;
        }
        let Ok(device) = device.parse::<u32>() else {
            continue;
        };
        if device != 0 {
            return Err(SttError::CudaBackendUnavailable {
                reason: format!("whisper.cpp selected CUDA device {device}, expected device 0"),
            });
        }
        let evidence = bounded(
            &messages
                .iter()
                .filter(|candidate| {
                    [
                        "use gpu    = 1",
                        "flash attn = 1",
                        "gpu_device = 0",
                        "CUDA0 total size",
                        "found GPU device 0: CUDA0",
                        SELECTED_PREFIX,
                    ]
                    .iter()
                    .any(|marker| candidate.contains(marker))
                })
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(" | "),
        );
        return Ok(CudaBackendAdmission { device, evidence });
    }

    let fallback = messages
        .iter()
        .find(|message| message.contains("CPU") && message.contains("backend"))
        .map(|message| format!("; observed {}", bounded(message)))
        .unwrap_or_default();
    Err(SttError::CudaBackendUnavailable {
        reason: format!("whisper.cpp did not select CUDA device 0{fallback}"),
    })
}

#[cfg(feature = "whisper-cuda")]
fn sha256_file(path: &Path) -> Result<String, SttError> {
    let mut file = std::fs::File::open(path).map_err(|source| SttError::ArtifactRead {
        path: path.to_path_buf(),
        source,
    })?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|source| SttError::ArtifactRead {
                path: path.to_path_buf(),
                source,
            })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

#[cfg(feature = "whisper-cuda")]
fn bounded(value: &str) -> String {
    value.chars().take(MAX_DIAGNOSTIC_CHARS).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transcript_trims_text_and_rejects_empty_results() {
        assert_eq!(
            Transcript::new("  spoken question  ", true, 750).unwrap(),
            Transcript {
                text: "spoken question".to_owned(),
                is_final: true,
                span_ms: 750,
            }
        );
        assert!(matches!(
            Transcript::new(" \n ", true, 10),
            Err(SttError::EmptyTranscript)
        ));
    }

    #[cfg(not(feature = "whisper-cuda"))]
    #[test]
    fn default_build_returns_typed_cuda_unavailable_without_reading_a_model() {
        let error = match WhisperRecognizer::load(WhisperConfig::new("missing-model.bin")) {
            Err(error) => error,
            Ok(_) => panic!("default build must not silently use CPU"),
        };
        assert!(matches!(error, SttError::CudaUnavailable { .. }));
    }

    #[cfg(feature = "whisper-cuda")]
    #[test]
    fn runtime_cuda_admission_rejects_compiled_capability_and_cpu_fallback() {
        let compiled_only = vec!["WHISPER : CUDA : ARCHS = 890".to_owned()];
        assert!(matches!(
            admit_cuda_backend(&compiled_only),
            Err(SttError::CudaBackendUnavailable { .. })
        ));

        let cpu_fallback = vec![
            "whisper_backend_init_gpu: no GPU device available".to_owned(),
            "whisper_backend_init: using CPU backend".to_owned(),
        ];
        assert!(matches!(
            admit_cuda_backend(&cpu_fallback),
            Err(SttError::CudaBackendUnavailable { reason })
                if reason.contains("CPU backend")
        ));
    }

    #[cfg(feature = "whisper-cuda")]
    #[test]
    fn runtime_cuda_admission_records_exact_device_zero_evidence() {
        let messages = [
            "whisper_init_with_params_no_state: use gpu    = 1",
            "whisper_init_with_params_no_state: flash attn = 1",
            "whisper_init_with_params_no_state: gpu_device = 0",
            "whisper_model_load: CUDA0 total size = 1623.92 MB",
            "whisper_backend_init_gpu: found GPU device 0: CUDA0 (type: 1, cnt: 0)",
            "whisper_backend_init_gpu: using CUDA0 backend",
        ];
        let admission = admit_cuda_backend(&messages.map(str::to_owned)).unwrap();
        assert_eq!(admission.device, 0);
        assert_eq!(admission.evidence, messages.join(" | "));
    }

    #[cfg(feature = "whisper-cuda")]
    #[test]
    fn rolling_partial_window_keeps_only_the_newest_five_seconds() {
        let maximum = milliseconds_to_samples(WHISPER_PARTIAL_WINDOW_MS);
        let samples = (0..maximum + 2_560)
            .map(|sample| sample as f32)
            .collect::<Vec<_>>();
        let window = partial_window(&samples);
        assert_eq!(window.len(), maximum);
        assert_eq!(window.first(), Some(&2_560.0));
        assert_eq!(window.last(), samples.last());
    }

    #[cfg(feature = "whisper-cuda")]
    #[test]
    #[ignore = "requires the pinned model in a process with CUDA_VISIBLE_DEVICES=-1"]
    fn runtime_without_visible_cuda_device_fails_closed() {
        assert_eq!(
            std::env::var("CUDA_VISIBLE_DEVICES").as_deref(),
            Ok("-1"),
            "run this proof in a process with CUDA hidden"
        );
        let model_path = std::env::var_os("PLATO_AUDIO_WHISPER_MODEL")
            .expect("PLATO_AUDIO_WHISPER_MODEL must name the pinned model");
        let error = match WhisperRecognizer::load(WhisperConfig::new(model_path)) {
            Err(error) => error,
            Ok(_) => panic!("a CUDA recognizer must not admit whisper.cpp's CPU fallback"),
        };
        assert!(matches!(
            error,
            SttError::CudaBackendUnavailable { reason }
                if reason.contains("did not select CUDA device 0")
        ));
    }
}
