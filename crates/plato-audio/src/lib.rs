//! Typed local audio primitives with persistent capture and playback.
//!
//! This crate is an IO leaf. It deliberately has no dependency on any Platonic
//! crate and assigns no run, session, policy, approval, ledger, or protocol
//! meaning to text or audio.

#![deny(unsafe_code)]
#![warn(missing_docs)]

mod core;
mod error;
mod io;

pub use core::{
    AudioFormat, BargeInHandle, BargeInMetrics, CAPTURE_SAMPLE_RATE, CaptureResampleReport,
    CaptureSample, MAX_UTTERANCE_MS, NeuralVadEvent, NeuralVadState, PcmChunk, PcmData, PcmFrame,
    RUBATO_RUNTIME_VERSION, ResampleReport, ResamplingPlan, SELF_PLAYBACK_GATE_MS,
    SENTENCE_PREFETCH_CAPACITY, SILERO_HANGOVER_FRAMES, SILERO_MINIMUM_SPEECH_FRAMES,
    SILERO_ONSET_FRAMES, SILERO_SPEECH_THRESHOLD, SILERO_WINDOW_SAMPLES, SampleFormat, Sentence,
    SentenceCutter, SentenceQueueError, SpeechSource, SpokenInterruption, VAD_HANGOVER_WINDOWS,
    VAD_MINIMUM_SPEECH_WINDOWS, VAD_ONSET_WINDOWS, VAD_RMS_THRESHOLD, VAD_WINDOW_SAMPLES,
    VadEndpoint, VoiceActivityDetector, VoiceSegment,
};
pub use error::{
    CaptureError, DeviceError, OrtRuntimeError, PcmError, PcmSinkError, ResampleError,
    SentenceError, SttError, SynthError, VadError,
};
pub use io::{
    CPAL_RUNTIME_VERSION, CaptureConfig, CaptureDeviceDescriptor, CaptureDeviceInfo,
    CaptureMetrics, CaptureOverflow, CapturePartial, CaptureReport, CaptureRequest, CaptureWorker,
    CaptureWorkerShutdown, DeviceBufferSize, InferenceBackend, InputDeviceSelection,
    KOKORO_MODEL_REVISION, KOKORO_MODEL_SHA256, KOKORO_MODEL_SOURCE, KOKORO_SAMPLE_RATE,
    KOKORO_TOKENIZER_SHA256, KOKORO_VOICE_SHA256, KokoroConfig, KokoroMetrics, KokoroMetricsReader,
    KokoroProvenance, KokoroSynthesizer, ORT_RUNTIME_VERSION, OrtRuntime, OrtRuntimeMetrics,
    OrtRuntimeMetricsReader, OutputDeviceSelection, PcmSink, PlaybackConfig, PlaybackDeviceInfo,
    PlaybackMetrics, PlaybackReport, PlaybackUnderrun, RTRB_RUNTIME_VERSION, SILERO_MODEL_LICENSE,
    SILERO_MODEL_REVISION, SILERO_MODEL_SHA256, SILERO_MODEL_SOURCE, SentenceAdmission,
    SileroConfig, SileroMetrics, SileroMetricsReader, SileroProvenance, SileroVad,
    SpeechRecognizer, SpeechSynthesizer, SynthWorker, SynthWorkerError, SynthWorkerFailure,
    SynthWorkerShutdown, SynthWorkerStartError, SynthesizedSentenceReport, Transcript,
    WHISPER_MODEL_REVISION, WHISPER_MODEL_SHA256, WHISPER_MODEL_SOURCE, WHISPER_PARTIAL_CADENCE_MS,
    WHISPER_PARTIAL_MINIMUM_MS, WHISPER_PARTIAL_WINDOW_MS, WHISPER_RS_RUNTIME_VERSION,
    WHISPER_STABLE_OVERLAP_MS, WhisperConfig, WhisperMetrics, WhisperMetricsReader,
    WhisperProvenance, WhisperRecognizer, capture_devices,
};
