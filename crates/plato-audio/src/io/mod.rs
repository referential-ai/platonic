mod capture;
#[cfg(all(test, feature = "whisper-cuda"))]
mod corpus_tests;
mod kokoro;
mod playback;
mod recognize;
mod runtime;
mod silero;
mod synth;
#[cfg(test)]
mod vad_corpus_tests;

pub use capture::{
    CaptureConfig, CaptureDeviceDescriptor, CaptureDeviceInfo, CaptureMetrics, CaptureOverflow,
    CapturePartial, CaptureReport, CaptureWorker, CaptureWorkerShutdown, InputDeviceSelection,
    capture_devices,
};
pub use kokoro::{
    KOKORO_MODEL_REVISION, KOKORO_MODEL_SHA256, KOKORO_MODEL_SOURCE, KOKORO_SAMPLE_RATE,
    KOKORO_TOKENIZER_SHA256, KOKORO_VOICE_SHA256, KokoroConfig, KokoroMetrics, KokoroMetricsReader,
    KokoroProvenance, KokoroSynthesizer,
};
pub use playback::{
    CPAL_RUNTIME_VERSION, DeviceBufferSize, PlaybackConfig, PlaybackDeviceInfo, PlaybackMetrics,
    PlaybackReport, PlaybackUnderrun, RTRB_RUNTIME_VERSION,
};
pub use recognize::{
    SpeechRecognizer, Transcript, WHISPER_MODEL_REVISION, WHISPER_MODEL_SHA256,
    WHISPER_MODEL_SOURCE, WHISPER_PARTIAL_CADENCE_MS, WHISPER_PARTIAL_MINIMUM_MS,
    WHISPER_PARTIAL_WINDOW_MS, WHISPER_RS_RUNTIME_VERSION, WhisperConfig, WhisperMetrics,
    WhisperMetricsReader, WhisperProvenance, WhisperRecognizer,
};
pub use runtime::{
    InferenceBackend, ORT_RUNTIME_VERSION, OrtRuntime, OrtRuntimeMetrics, OrtRuntimeMetricsReader,
};
pub use silero::{
    SILERO_MODEL_LICENSE, SILERO_MODEL_REVISION, SILERO_MODEL_SHA256, SILERO_MODEL_SOURCE,
    SileroConfig, SileroMetrics, SileroMetricsReader, SileroProvenance, SileroVad,
};
pub use synth::{
    PcmSink, SentenceAdmission, SpeechSynthesizer, SynthWorker, SynthWorkerError,
    SynthWorkerFailure, SynthWorkerShutdown, SynthWorkerStartError, SynthesizedSentenceReport,
};
