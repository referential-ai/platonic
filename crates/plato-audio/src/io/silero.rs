use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use ort::{session::Session, value::Tensor};
use serde::Serialize;
use sha2::{Digest, Sha256};

use super::runtime::{OrtRuntime, SessionLoadError};
use crate::{
    InferenceBackend, ORT_RUNTIME_VERSION, SILERO_SPEECH_THRESHOLD, SILERO_WINDOW_SAMPLES,
    VadError, VoiceActivityDetector,
};

/// Immutable upstream repository containing the admitted Silero model.
pub const SILERO_MODEL_SOURCE: &str = "snakers4/silero-vad";
/// Immutable upstream commit tagged `v6.2.1`.
pub const SILERO_MODEL_REVISION: &str = "7e30209a3e901f9842f81b225f3e93d8199902b1";
/// Upstream repository license covering the admitted ONNX model artifact.
pub const SILERO_MODEL_LICENSE: &str = "MIT";
/// SHA-256 of `src/silero_vad/data/silero_vad.onnx` at the admitted revision.
pub const SILERO_MODEL_SHA256: &str =
    "1a153a22f4509e292a94e67d6f9b85e8deb25b4988682b7e174c65279d8788e3";

const SILERO_MODEL_VERSION: &str = "v6.2.1";
const SAMPLE_RATE: i64 = 16_000;
const CONTEXT_SAMPLES: usize = 64;
const STATE_VALUES: usize = 2 * 128;
const MAX_DIAGNOSTIC_CHARS: usize = 2_048;

/// Path to the pinned Silero VAD ONNX artifact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SileroConfig {
    model_path: PathBuf,
}

impl SileroConfig {
    /// Selects one local model file whose checksum is always verified.
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

/// Exact artifact, endpoint, and shared-runtime identity captured at load.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SileroProvenance {
    /// Immutable model repository.
    pub model_source: String,
    /// Immutable model commit.
    pub model_revision: String,
    /// Upstream release tag at that commit.
    pub model_version: String,
    /// Upstream repository license for the model artifact.
    pub model_license: String,
    /// Verified ONNX model digest.
    pub model_sha256: String,
    /// Pinned Rust wrapper and runtime line.
    pub ort_runtime: String,
    /// Loaded ONNX Runtime build identity.
    pub ort_build_info: String,
    /// Shared process-local runtime owner used for this session.
    pub ort_runtime_owner: u64,
    /// Resident inference provider.
    pub backend: InferenceBackend,
    /// CUDA diagnostic retained when CPU fallback was required.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fallback_reason: Option<String>,
    /// Fixed 16 kHz model frame size.
    pub frame_samples: usize,
    /// Probability threshold used by pure endpoint state.
    pub speech_threshold: f32,
}

/// Observable warm Silero session and recurrent-state reuse counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct SileroMetrics {
    /// Successful resident session constructions. This remains one.
    pub session_loads: u64,
    /// Successful fixed-frame inference calls.
    pub inference_frames: u64,
    /// Recurrent-state resets between explicit capture requests.
    pub state_resets: u64,
}

#[derive(Default)]
struct SileroMetricCounters {
    session_loads: AtomicU64,
    inference_frames: AtomicU64,
    state_resets: AtomicU64,
}

/// Cloneable read-only access to counters owned by the capture worker's VAD.
#[derive(Clone)]
pub struct SileroMetricsReader {
    counters: Arc<SileroMetricCounters>,
}

impl SileroMetricsReader {
    /// Reads monotonic warm-session and recurrent-state counters.
    pub fn snapshot(&self) -> SileroMetrics {
        SileroMetrics {
            session_loads: self.counters.session_loads.load(Ordering::Relaxed),
            inference_frames: self.counters.inference_frames.load(Ordering::Relaxed),
            state_resets: self.counters.state_resets.load(Ordering::Relaxed),
        }
    }
}

/// Warm Silero VAD v6.2.1 session with recurrent state retained across frames.
pub struct SileroVad {
    session: Session,
    backend: InferenceBackend,
    state: [f32; STATE_VALUES],
    context: [f32; CONTEXT_SAMPLES],
    provenance: SileroProvenance,
    metrics: Arc<SileroMetricCounters>,
}

impl SileroVad {
    /// Verifies and loads one resident session through a new runtime owner.
    pub fn load(config: SileroConfig) -> Result<Self, VadError> {
        let runtime = OrtRuntime::acquire()?;
        Self::load_with_runtime(config, runtime)
    }

    /// Verifies and loads one resident session through an explicitly shared owner.
    pub fn load_with_runtime(config: SileroConfig, runtime: OrtRuntime) -> Result<Self, VadError> {
        let bytes = std::fs::read(&config.model_path).map_err(|source| VadError::ArtifactRead {
            path: config.model_path.clone(),
            source,
        })?;
        let actual = format!("{:x}", Sha256::digest(&bytes));
        if actual != SILERO_MODEL_SHA256 {
            return Err(VadError::ArtifactChecksum {
                path: config.model_path,
                expected: SILERO_MODEL_SHA256,
                actual,
            });
        }
        drop(bytes);

        let resident = runtime
            .load_session(&config.model_path)
            .map_err(map_session_error)?;
        let metrics = Arc::new(SileroMetricCounters::default());
        metrics.session_loads.store(1, Ordering::Relaxed);
        Ok(Self {
            session: resident.session,
            backend: resident.backend,
            state: [0.0; STATE_VALUES],
            context: [0.0; CONTEXT_SAMPLES],
            provenance: SileroProvenance {
                model_source: SILERO_MODEL_SOURCE.to_owned(),
                model_revision: SILERO_MODEL_REVISION.to_owned(),
                model_version: SILERO_MODEL_VERSION.to_owned(),
                model_license: SILERO_MODEL_LICENSE.to_owned(),
                model_sha256: SILERO_MODEL_SHA256.to_owned(),
                ort_runtime: ORT_RUNTIME_VERSION.to_owned(),
                ort_build_info: bounded(ort::info()),
                ort_runtime_owner: runtime.owner_id(),
                backend: resident.backend,
                fallback_reason: resident.fallback_reason,
                frame_samples: SILERO_WINDOW_SAMPLES,
                speech_threshold: SILERO_SPEECH_THRESHOLD,
            },
            metrics,
        })
    }

    /// Returns immutable artifact, endpoint, and runtime identity.
    pub fn provenance(&self) -> &SileroProvenance {
        &self.provenance
    }

    /// Returns a reader that remains valid after the detector enters its worker.
    pub fn metrics_reader(&self) -> SileroMetricsReader {
        SileroMetricsReader {
            counters: Arc::clone(&self.metrics),
        }
    }
}

impl VoiceActivityDetector for SileroVad {
    fn frame_samples(&self) -> usize {
        SILERO_WINDOW_SAMPLES
    }

    fn reset(&mut self) {
        self.state.fill(0.0);
        self.context.fill(0.0);
        self.metrics.state_resets.fetch_add(1, Ordering::Relaxed);
    }

    fn speech_probability(&mut self, samples: &[f32]) -> Result<f32, VadError> {
        if samples.len() != SILERO_WINDOW_SAMPLES {
            return Err(VadError::FrameLength {
                expected: SILERO_WINDOW_SAMPLES,
                actual: samples.len(),
            });
        }
        let mut input = Vec::with_capacity(CONTEXT_SAMPLES + SILERO_WINDOW_SAMPLES);
        input.extend_from_slice(&self.context);
        input.extend_from_slice(samples);
        let backend = self.backend;
        let input = Tensor::from_array((
            [1_usize, CONTEXT_SAMPLES + SILERO_WINDOW_SAMPLES],
            input.into_boxed_slice(),
        ))
        .map_err(|error| inference_error(backend, error))?;
        let state = Tensor::from_array(([2_usize, 1, 128], self.state.to_vec().into_boxed_slice()))
            .map_err(|error| inference_error(backend, error))?;
        let sample_rate = Tensor::from_array(((), vec![SAMPLE_RATE].into_boxed_slice()))
            .map_err(|error| inference_error(backend, error))?;

        let (probability, next_state) = {
            let outputs = self
                .session
                .run(ort::inputs![
                    "input" => input,
                    "state" => state,
                    "sr" => sample_rate,
                ])
                .map_err(|error| inference_error(backend, error))?;
            let (_, probabilities) = outputs[0]
                .try_extract_tensor::<f32>()
                .map_err(|error| inference_error(backend, error))?;
            let probability =
                probabilities
                    .first()
                    .copied()
                    .ok_or_else(|| VadError::OutputContract {
                        reason: "probability tensor was empty".to_owned(),
                    })?;
            let (_, state) = outputs[1]
                .try_extract_tensor::<f32>()
                .map_err(|error| inference_error(backend, error))?;
            (probability, state.to_vec())
        };
        if next_state.len() != STATE_VALUES {
            return Err(VadError::OutputContract {
                reason: format!(
                    "recurrent state contained {} values, expected {STATE_VALUES}",
                    next_state.len()
                ),
            });
        }
        if !probability.is_finite() || !(0.0..=1.0).contains(&probability) {
            return Err(VadError::InvalidProbability { probability });
        }
        self.state.copy_from_slice(&next_state);
        self.context
            .copy_from_slice(&samples[SILERO_WINDOW_SAMPLES - CONTEXT_SAMPLES..]);
        self.metrics
            .inference_frames
            .fetch_add(1, Ordering::Relaxed);
        Ok(probability)
    }
}

fn map_session_error(error: SessionLoadError) -> VadError {
    match error {
        SessionLoadError::Fallback { cuda, cpu } => VadError::ModelLoadFallback { cuda, cpu },
        #[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
        SessionLoadError::Backend { backend, reason } => VadError::ModelLoad { backend, reason },
    }
}

fn inference_error(backend: InferenceBackend, error: ort::Error) -> VadError {
    VadError::Inference {
        backend,
        reason: bounded(&error.to_string()),
    }
}

fn bounded(value: &str) -> String {
    value.chars().take(MAX_DIAGNOSTIC_CHARS).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "requires the pinned Silero ONNX artifact"]
    fn pinned_model_scores_speech_above_noise_with_one_resident_session() {
        let model = std::env::var_os("PLATO_AUDIO_SILERO_MODEL")
            .expect("PLATO_AUDIO_SILERO_MODEL must name the pinned artifact");
        let mut vad = SileroVad::load(SileroConfig::new(model)).unwrap();
        let metrics = vad.metrics_reader();
        let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/au3");
        let speech = probabilities(&mut vad, &read_wav(&fixtures.join("spoken-question.wav")));
        vad.reset();
        let noise = probabilities(&mut vad, &read_wav(&fixtures.join("steady-noise.wav")));
        assert!(speech.iter().any(|probability| *probability >= 0.5));
        assert!(noise.iter().all(|probability| *probability < 0.5));
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.session_loads, 1);
        assert_eq!(
            snapshot.inference_frames,
            (speech.len() + noise.len()) as u64
        );
        assert_eq!(snapshot.state_resets, 1);
    }

    fn probabilities(vad: &mut SileroVad, samples: &[f32]) -> Vec<f32> {
        samples
            .chunks_exact(SILERO_WINDOW_SAMPLES)
            .map(|frame| vad.speech_probability(frame).unwrap())
            .collect()
    }

    fn read_wav(path: &Path) -> Vec<f32> {
        let mut reader = hound::WavReader::open(path).unwrap();
        reader
            .samples::<i16>()
            .map(|sample| f32::from(sample.unwrap()) / 32_768.0)
            .collect()
    }
}
