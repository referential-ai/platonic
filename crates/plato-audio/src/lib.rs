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

pub use core::{AudioFormat, PcmChunk, PcmData, PcmFrame, SampleFormat, Sentence, SentenceCutter};
pub use error::{DeviceError, PcmError, PcmSinkError, SentenceError, SynthError};
pub use io::{
    CPAL_RUNTIME_VERSION, DeviceBufferSize, InferenceBackend, KOKORO_MODEL_REVISION,
    KOKORO_MODEL_SHA256, KOKORO_MODEL_SOURCE, KOKORO_SAMPLE_RATE, KOKORO_TOKENIZER_SHA256,
    KOKORO_VOICE_SHA256, KokoroConfig, KokoroMetrics, KokoroProvenance, KokoroSynthesizer,
    ORT_RUNTIME_VERSION, PcmSink, PersistentPlayback, PlaybackConfig, PlaybackDeviceInfo,
    PlaybackMetrics, PlaybackReport, SpeechSynthesizer,
};
