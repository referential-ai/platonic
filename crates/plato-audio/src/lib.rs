//! Typed local audio primitives, warm Kokoro synthesis, and persistent playback.
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
    AudioFormat, PcmChunk, PcmData, PcmFrame, RUBATO_RUNTIME_VERSION, ResampleReport,
    ResamplingPlan, SENTENCE_PREFETCH_CAPACITY, SampleFormat, Sentence, SentenceCutter,
    SentenceQueueError,
};
pub use error::{DeviceError, PcmError, PcmSinkError, ResampleError, SentenceError, SynthError};
pub use io::{
    CPAL_RUNTIME_VERSION, DeviceBufferSize, InferenceBackend, KOKORO_MODEL_REVISION,
    KOKORO_MODEL_SHA256, KOKORO_MODEL_SOURCE, KOKORO_SAMPLE_RATE, KOKORO_TOKENIZER_SHA256,
    KOKORO_VOICE_SHA256, KokoroConfig, KokoroMetrics, KokoroMetricsReader, KokoroProvenance,
    KokoroSynthesizer, ORT_RUNTIME_VERSION, PcmSink, PlaybackConfig, PlaybackDeviceInfo,
    PlaybackMetrics, PlaybackReport, PlaybackUnderrun, RTRB_RUNTIME_VERSION, SentenceAdmission,
    SpeechSynthesizer, SynthWorker, SynthWorkerError, SynthWorkerFailure, SynthWorkerShutdown,
    SynthWorkerStartError, SynthesizedSentenceReport,
};
