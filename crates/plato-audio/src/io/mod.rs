mod capture;
#[cfg(all(test, feature = "whisper-cuda"))]
mod corpus_tests;
mod kokoro;
mod playback;
mod recognize;
mod synth;

pub use capture::{
    CaptureConfig, CaptureDeviceDescriptor, CaptureDeviceInfo, CaptureMetrics, CaptureOverflow,
    CaptureReport, CaptureWorker, CaptureWorkerShutdown, InputDeviceSelection, capture_devices,
};
pub use kokoro::{
    InferenceBackend, KOKORO_MODEL_REVISION, KOKORO_MODEL_SHA256, KOKORO_MODEL_SOURCE,
    KOKORO_SAMPLE_RATE, KOKORO_TOKENIZER_SHA256, KOKORO_VOICE_SHA256, KokoroConfig, KokoroMetrics,
    KokoroMetricsReader, KokoroProvenance, KokoroSynthesizer, ORT_RUNTIME_VERSION,
};
pub use playback::{
    CPAL_RUNTIME_VERSION, DeviceBufferSize, PlaybackConfig, PlaybackDeviceInfo, PlaybackMetrics,
    PlaybackReport, PlaybackUnderrun, RTRB_RUNTIME_VERSION,
};
pub use recognize::{
    SpeechRecognizer, Transcript, WHISPER_MODEL_REVISION, WHISPER_MODEL_SHA256,
    WHISPER_MODEL_SOURCE, WHISPER_RS_RUNTIME_VERSION, WhisperConfig, WhisperMetrics,
    WhisperMetricsReader, WhisperProvenance, WhisperRecognizer,
};
pub use synth::{
    PcmSink, SentenceAdmission, SpeechSynthesizer, SynthWorker, SynthWorkerError,
    SynthWorkerFailure, SynthWorkerShutdown, SynthWorkerStartError, SynthesizedSentenceReport,
};
