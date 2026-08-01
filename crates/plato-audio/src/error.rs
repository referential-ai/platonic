use std::path::PathBuf;

use thiserror::Error;

use crate::{AudioFormat, InferenceBackend, SampleFormat};

/// Validation failures for typed PCM values.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum PcmError {
    /// A sample rate of zero cannot describe PCM.
    #[error("audio sample rate must be greater than zero")]
    ZeroSampleRate,
    /// A channel count of zero cannot describe PCM.
    #[error("audio channel count must be greater than zero")]
    ZeroChannels,
    /// The declared sample format does not match the sample storage.
    #[error("declared sample format {declared:?} does not match {actual:?} sample storage")]
    SampleFormatMismatch {
        /// Format carried by the audio descriptor.
        declared: SampleFormat,
        /// Format carried by the sample storage.
        actual: SampleFormat,
    },
    /// Interleaved samples do not contain a whole number of frames.
    #[error("{samples} samples do not form complete {channels}-channel frames")]
    IncompleteFrame {
        /// Number of interleaved samples.
        samples: usize,
        /// Declared channel count.
        channels: u16,
    },
    /// A frame must contain exactly one sample for each channel.
    #[error("frame contains {samples} samples but format declares {channels} channels")]
    FrameChannelMismatch {
        /// Number of samples in the frame.
        samples: usize,
        /// Declared channel count.
        channels: u16,
    },
    /// Floating-point PCM must not contain NaN or infinity.
    #[error("floating-point PCM sample at index {index} is not finite")]
    NonFiniteSample {
        /// Zero-based sample index.
        index: usize,
    },
}

/// Validation failures for sentence values.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SentenceError {
    /// Whitespace-only text is not a speakable sentence.
    #[error("sentence must contain non-whitespace text")]
    Empty,
}

/// A PCM consumer rejected synthesized output.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum PcmSinkError {
    /// The sink rejected a chunk without changing model state.
    #[error("PCM sink rejected synthesized audio: {reason}")]
    Rejected {
        /// Bounded sink-specific reason.
        reason: String,
    },
}

/// Warm synthesis setup and inference failures.
#[derive(Debug, Error)]
pub enum SynthError {
    /// A caller supplied an invalid engine setting.
    #[error("invalid Kokoro configuration: {reason}")]
    InvalidConfig {
        /// Invalid field and bound.
        reason: String,
    },
    /// A pinned model support artifact could not be read.
    #[error("cannot read Kokoro {artifact} artifact at {path}: {source}")]
    ArtifactRead {
        /// Literal artifact role.
        artifact: &'static str,
        /// Requested artifact path.
        path: PathBuf,
        /// Filesystem failure.
        #[source]
        source: std::io::Error,
    },
    /// An artifact did not match its pinned digest.
    #[error("Kokoro {artifact} checksum mismatch at {path}: expected {expected}, got {actual}")]
    ArtifactChecksum {
        /// Literal artifact role.
        artifact: &'static str,
        /// Checked artifact path.
        path: PathBuf,
        /// Pinned SHA-256 digest.
        expected: &'static str,
        /// Observed SHA-256 digest.
        actual: String,
    },
    /// The pinned tokenizer file is malformed or incompatible.
    #[error("invalid Kokoro tokenizer at {path}: {reason}")]
    Tokenizer {
        /// Tokenizer artifact path.
        path: PathBuf,
        /// Parse or compatibility failure.
        reason: String,
    },
    /// The pinned voice tensor is malformed or incompatible.
    #[error("invalid Kokoro voice at {path}: {reason}")]
    Voice {
        /// Voice artifact path.
        path: PathBuf,
        /// Shape or encoding failure.
        reason: String,
    },
    /// The espeak-ng executable could not be started.
    #[error("cannot start espeak-ng phonemizer at {program}: {source}")]
    PhonemizerStart {
        /// Executable path or program name.
        program: PathBuf,
        /// Process creation failure.
        #[source]
        source: std::io::Error,
    },
    /// espeak-ng rejected the sentence.
    #[error("espeak-ng phonemizer failed with status {status:?}: {stderr}")]
    PhonemizerFailed {
        /// Process exit code when available.
        status: Option<i32>,
        /// Bounded diagnostic output.
        stderr: String,
    },
    /// espeak-ng returned non-UTF-8 phoneme output.
    #[error("espeak-ng returned non-UTF-8 phoneme output")]
    InvalidPhonemeEncoding,
    /// espeak-ng produced symbols absent from the pinned tokenizer.
    #[error("Kokoro tokenizer does not contain phoneme symbols: {symbols}")]
    UnknownPhonemes {
        /// Unique unsupported symbols.
        symbols: String,
    },
    /// The sentence exceeds the model's token or voice-style bound.
    #[error("Kokoro sentence has {tokens} tokens; maximum supported is {maximum}")]
    SentenceTooLong {
        /// Produced phoneme token count.
        tokens: usize,
        /// Maximum supported token count.
        maximum: usize,
    },
    /// Both the accelerated and fallback runtimes failed to load the model.
    #[error("cannot load Kokoro model with CUDA ({cuda}) or CPU ({cpu})")]
    ModelLoadFallback {
        /// CUDA setup or session-load failure.
        cuda: String,
        /// CPU session-load failure.
        cpu: String,
    },
    /// The selected runtime failed to load the model.
    #[error("cannot load Kokoro model with {backend:?}: {reason}")]
    ModelLoad {
        /// Attempted inference backend.
        backend: InferenceBackend,
        /// ONNX Runtime diagnostic.
        reason: String,
    },
    /// Warm ONNX inference failed.
    #[error("Kokoro inference failed on {backend:?}: {reason}")]
    Inference {
        /// Resident inference backend.
        backend: InferenceBackend,
        /// ONNX Runtime diagnostic.
        reason: String,
    },
    /// The model emitted an invalid PCM value.
    #[error(transparent)]
    Pcm(#[from] PcmError),
    /// The caller canceled synthesis at a supported boundary.
    #[error("Kokoro synthesis canceled")]
    Canceled,
    /// The caller-provided PCM sink rejected output.
    #[error(transparent)]
    Sink(#[from] PcmSinkError),
    /// A model support process received more text than its bounded input.
    #[error("sentence contains {bytes} bytes; maximum supported is {maximum}")]
    SentenceTextTooLong {
        /// UTF-8 input length.
        bytes: usize,
        /// Maximum supported UTF-8 length.
        maximum: usize,
    },
}

/// Output-device setup and serial playback failures.
#[derive(Debug, Error)]
pub enum DeviceError {
    /// The host has no default output device.
    #[error("no default output device is available")]
    NoOutputDevice,
    /// Device capabilities could not be queried.
    #[error("cannot query output device capabilities: {reason}")]
    DeviceQuery {
        /// cpal diagnostic.
        reason: String,
    },
    /// No cpal configuration can play the model's sample rate.
    #[error("output device does not support {sample_rate} Hz in f32, i16, or u16")]
    UnsupportedSampleRate {
        /// Required model sample rate.
        sample_rate: u32,
    },
    /// The persistent output stream could not be built.
    #[error("cannot build persistent output stream: {reason}")]
    StreamBuild {
        /// cpal diagnostic.
        reason: String,
    },
    /// The persistent output stream could not be started.
    #[error("cannot start persistent output stream: {reason}")]
    StreamStart {
        /// cpal diagnostic.
        reason: String,
    },
    /// The device invalidated the live stream.
    #[error("persistent output stream failed")]
    StreamFailed,
    /// A chunk does not match the serial playback input contract.
    #[error("playback requires {expected:?}, got {actual:?}")]
    FormatMismatch {
        /// Required model PCM format.
        expected: AudioFormat,
        /// Supplied chunk format.
        actual: AudioFormat,
    },
    /// The fixed callback buffer cannot hold the synthesized sentence.
    #[error("PCM chunk has {frames} frames; persistent buffer capacity is {capacity}")]
    ChunkTooLarge {
        /// Submitted mono frames.
        frames: usize,
        /// Preallocated mono-frame capacity.
        capacity: usize,
    },
    /// Serial playback was asked to load while a prior chunk remained active.
    #[error("persistent playback is still draining the prior chunk")]
    PlaybackBusy,
    /// The callback did not drain a submitted chunk within its bounded deadline.
    #[error("persistent playback timed out after {milliseconds} ms")]
    PlaybackTimeout {
        /// Waited duration.
        milliseconds: u128,
    },
    /// The callback drained a chunk containing no audible sample.
    #[error("synthesized PCM contains no non-silent sample")]
    SilentChunk,
    /// The supplied PCM value itself is invalid.
    #[error(transparent)]
    Pcm(#[from] PcmError),
}
