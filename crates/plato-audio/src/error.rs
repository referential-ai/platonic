use std::path::PathBuf;

use thiserror::Error;

use crate::{AudioFormat, InferenceBackend, SampleFormat};

/// Process-global ONNX Runtime acquisition failures.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum OrtRuntimeError {
    /// The linked ONNX Runtime could not provide its process environment.
    #[error("cannot acquire shared ONNX Runtime environment: {reason}")]
    Environment {
        /// Bounded ONNX Runtime diagnostic.
        reason: String,
    },
}

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

/// Construction and processing failures for a fixed sample-rate plan.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ResampleError {
    /// AU2 accepts mono f32 synthesis PCM only.
    #[error("resampling requires mono f32 source PCM, got {actual:?}")]
    UnsupportedSource {
        /// Supplied synthesis format.
        actual: AudioFormat,
    },
    /// A chunk did not match the pair used to build the plan.
    #[error("resampling plan requires {expected:?}, got {actual:?}")]
    FormatMismatch {
        /// Source format captured when the plan was built.
        expected: AudioFormat,
        /// Supplied chunk format.
        actual: AudioFormat,
    },
    /// Rubato rejected the fixed source/device rate pair.
    #[error("cannot build resampling plan from {source_format:?} to {device_format:?}: {reason}")]
    PlanConstruction {
        /// Synthesis format.
        source_format: AudioFormat,
        /// Live output-device format.
        device_format: AudioFormat,
        /// Bounded rubato diagnostic.
        reason: String,
    },
    /// Rubato rejected validated source PCM while using the resident plan.
    #[error("resampling failed: {reason}")]
    Processing {
        /// Bounded adapter or rubato diagnostic.
        reason: String,
    },
    /// Resampled output violated the typed PCM contract.
    #[error(transparent)]
    Pcm(#[from] PcmError),
}

/// Warm speech-recognition setup and inference failures.
#[derive(Debug, Error)]
pub enum SttError {
    /// The model artifact could not be read.
    #[error("cannot read Whisper model at {path}: {source}")]
    ArtifactRead {
        /// Requested model path.
        path: PathBuf,
        /// Filesystem failure.
        #[source]
        source: std::io::Error,
    },
    /// The model artifact did not match the admitted large-v3-turbo digest.
    #[error("Whisper model checksum mismatch at {path}: expected {expected}, got {actual}")]
    ArtifactChecksum {
        /// Checked model path.
        path: PathBuf,
        /// Pinned SHA-256 digest.
        expected: &'static str,
        /// Observed SHA-256 digest.
        actual: String,
    },
    /// This build cannot construct the admitted CUDA recognizer.
    #[error("Whisper CUDA support is unavailable in this build for {platform}")]
    CudaUnavailable {
        /// Compile-time target identity.
        platform: &'static str,
    },
    /// A CUDA-capable build did not select the required runtime device/backend.
    #[error("Whisper CUDA runtime backend is unavailable: {reason}")]
    CudaBackendUnavailable {
        /// Bounded backend-selection evidence.
        reason: String,
    },
    /// whisper.cpp could not load the verified model on CUDA.
    #[error("cannot load resident Whisper CUDA model: {reason}")]
    ModelLoad {
        /// Bounded whisper.cpp diagnostic.
        reason: String,
    },
    /// whisper.cpp could not create the one resident decode state.
    #[error("cannot create resident Whisper decode state: {reason}")]
    StateCreation {
        /// Bounded whisper.cpp diagnostic.
        reason: String,
    },
    /// An input frame did not match 16 kHz mono f32.
    #[error("speech recognition requires {expected:?}, got {actual:?}")]
    FormatMismatch {
        /// Required Whisper PCM format.
        expected: AudioFormat,
        /// Supplied PCM format.
        actual: AudioFormat,
    },
    /// The recognizer was finalized without accepted PCM.
    #[error("cannot finalize speech recognition without PCM")]
    NoAudio,
    /// Warm whisper.cpp inference failed.
    #[error("Whisper inference failed: {reason}")]
    Inference {
        /// Bounded whisper.cpp diagnostic.
        reason: String,
    },
    /// Whisper returned no usable text for a VAD-confirmed segment.
    #[error("Whisper returned an empty final transcript")]
    EmptyTranscript,
    /// A recognizer implementation violated the rolling/final transcript contract.
    #[error("speech recognizer contract failed: {reason}")]
    Contract {
        /// Bounded contract diagnostic.
        reason: String,
    },
    /// The supplied PCM value itself is invalid.
    #[error(transparent)]
    Pcm(#[from] PcmError),
}

/// Neural voice-activity model, state, and artifact failures.
#[derive(Debug, Error)]
pub enum VadError {
    /// The process-global ONNX Runtime could not be acquired.
    #[error(transparent)]
    Runtime(#[from] OrtRuntimeError),
    /// The model artifact could not be read.
    #[error("cannot read Silero VAD model at {path}: {source}")]
    ArtifactRead {
        /// Requested model path.
        path: PathBuf,
        /// Filesystem failure.
        #[source]
        source: std::io::Error,
    },
    /// The model artifact did not match the admitted digest.
    #[error("Silero VAD model checksum mismatch at {path}: expected {expected}, got {actual}")]
    ArtifactChecksum {
        /// Checked model path.
        path: PathBuf,
        /// Pinned SHA-256 digest.
        expected: &'static str,
        /// Observed SHA-256 digest.
        actual: String,
    },
    /// Both accelerated and CPU session construction failed.
    #[error("cannot load Silero VAD with CUDA ({cuda}) or CPU ({cpu})")]
    ModelLoadFallback {
        /// CUDA setup or session-load failure.
        cuda: String,
        /// CPU session-load failure.
        cpu: String,
    },
    /// The selected backend could not construct a resident session.
    #[error("cannot load Silero VAD with {backend:?}: {reason}")]
    ModelLoad {
        /// Attempted inference backend.
        backend: InferenceBackend,
        /// Bounded ONNX Runtime diagnostic.
        reason: String,
    },
    /// A detector did not accept the fixed 16 kHz frame size.
    #[error("voice activity detector requires {expected} samples, got {actual}")]
    FrameLength {
        /// Model frame size.
        expected: usize,
        /// Supplied sample count.
        actual: usize,
    },
    /// Warm Silero inference failed.
    #[error("Silero VAD inference failed on {backend:?}: {reason}")]
    Inference {
        /// Resident inference backend.
        backend: InferenceBackend,
        /// Bounded ONNX Runtime diagnostic.
        reason: String,
    },
    /// Silero returned malformed recurrent state or probability output.
    #[error("Silero VAD output contract failed: {reason}")]
    OutputContract {
        /// Bounded contract diagnostic.
        reason: String,
    },
    /// A VAD implementation returned a non-finite or out-of-range probability.
    #[error("voice activity probability must be finite and within 0..=1, got {probability}")]
    InvalidProbability {
        /// Invalid model value.
        probability: f32,
    },
    /// A VAD-confirmed utterance exceeded the fixed memory bound.
    #[error("captured utterance exceeded the fixed {maximum_ms} ms bound")]
    UtteranceTooLong {
        /// Literal maximum accepted utterance duration.
        maximum_ms: u64,
    },
}

/// Capture-worker, endpointing, and recognizer outcomes.
#[derive(Debug, Error)]
pub enum CaptureError {
    /// Input-device construction or runtime failure.
    #[error(transparent)]
    Device(#[from] DeviceError),
    /// Worker-side normalization or resampling failure.
    #[error(transparent)]
    Resample(#[from] ResampleError),
    /// Resident speech-recognition failure.
    #[error(transparent)]
    Recognition(#[from] SttError),
    /// Neural voice activity evaluation or endpoint state failed.
    #[error(transparent)]
    Vad(#[from] VadError),
    /// A raw ring sample did not match the negotiated device format.
    #[error("capture sample format changed from {expected:?} to {actual:?}")]
    SampleFormatMismatch {
        /// Native representation fixed when the stream opened.
        expected: SampleFormat,
        /// Representation observed by the worker.
        actual: SampleFormat,
    },
    /// A native floating-point input sample was NaN or infinite.
    #[error("capture input contained a non-finite f32 sample")]
    NonFiniteInput,
    /// The callback ring filled while an explicit capture request was armed.
    #[error(
        "capture ring overflowed in {callbacks} callback(s), dropping {dropped_samples} native samples"
    )]
    RingOverflow {
        /// Callback invocations that overflowed during this request.
        callbacks: u64,
        /// Native interleaved samples dropped during this request.
        dropped_samples: u64,
    },
    /// A VAD-confirmed utterance exceeded the fixed memory bound.
    #[error("captured utterance exceeded the fixed {maximum_ms} ms bound")]
    UtteranceTooLong {
        /// Literal maximum accepted utterance duration.
        maximum_ms: u64,
    },
    /// The caller's bounded wait elapsed without a final transcript.
    #[error("capture timed out after {milliseconds} ms without a final transcript")]
    Timeout {
        /// Caller-selected wait bound.
        milliseconds: u128,
    },
    /// The owned capture worker panicked.
    #[error("capture worker panicked")]
    WorkerPanicked,
    /// The operating system rejected construction of the sole capture worker.
    #[error("cannot start capture worker thread: {reason}")]
    WorkerThreadStart {
        /// Bounded operating-system thread diagnostic.
        reason: String,
    },
    /// The capture result channel closed without a typed terminal outcome.
    #[error("capture worker stopped without a terminal outcome")]
    WorkerStopped,
    /// The capture session was explicitly closed.
    #[error("capture session is closed")]
    Closed,
}

/// Warm synthesis setup and inference failures.
#[derive(Debug, Error)]
pub enum SynthError {
    /// The process-global ONNX Runtime could not be acquired.
    #[error(transparent)]
    Runtime(#[from] OrtRuntimeError),
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

/// Output-device setup and persistent playback failures.
#[derive(Clone, Debug, Error)]
pub enum DeviceError {
    /// Persistent playback was configured with a zero capacity or period.
    #[error(
        "playback ring capacity ({capacity_frames}) and preferred buffer frames ({preferred_buffer_frames}) must be nonzero"
    )]
    InvalidPlaybackConfig {
        /// Requested mono f32 ring capacity.
        capacity_frames: usize,
        /// Requested callback period before device-range clamping.
        preferred_buffer_frames: u32,
    },
    /// Persistent capture was configured with a zero capacity or period.
    #[error(
        "capture ring capacity ({capacity_samples}) and preferred buffer frames ({preferred_buffer_frames}) must be nonzero"
    )]
    InvalidCaptureConfig {
        /// Requested raw-sample ring capacity.
        capacity_samples: usize,
        /// Requested callback period before device-range clamping.
        preferred_buffer_frames: u32,
    },
    /// The raw capture ring cannot hold one complete native frame.
    #[error(
        "capture ring capacity ({capacity_samples} samples) is smaller than one {channels}-channel frame"
    )]
    CaptureRingTooSmall {
        /// Fixed ring capacity.
        capacity_samples: usize,
        /// Negotiated native input channels.
        channels: u16,
    },
    /// The host has no default output device.
    #[error("no default output device is available")]
    NoOutputDevice,
    /// The host has no default input device.
    #[error("no default input device is available")]
    NoInputDevice,
    /// An explicitly selected input device is not present.
    #[error("input device is not available: {device_id}")]
    InputDeviceNotFound {
        /// Backend-qualified cpal device identifier.
        device_id: String,
    },
    /// Device capabilities could not be queried.
    #[error("cannot query output device capabilities: {reason}")]
    DeviceQuery {
        /// cpal diagnostic.
        reason: String,
    },
    /// Input-device capabilities could not be queried.
    #[error("cannot query input device capabilities: {reason}")]
    InputDeviceQuery {
        /// Bounded cpal diagnostic.
        reason: String,
    },
    /// The live device's native sample representation is unsupported.
    #[error("output device offers no f32, i16, or u16 stream format")]
    UnsupportedOutputFormat,
    /// The live input device has no supported native representation.
    #[error("input device offers no f32, i16, or u16 stream format")]
    UnsupportedInputFormat,
    /// The persistent output stream could not be built.
    #[error("cannot build persistent output stream: {reason}")]
    StreamBuild {
        /// cpal diagnostic.
        reason: String,
    },
    /// The persistent input stream could not be built.
    #[error("cannot build persistent input stream: {reason}")]
    InputStreamBuild {
        /// Bounded cpal diagnostic.
        reason: String,
    },
    /// The persistent output stream could not be started.
    #[error("cannot start persistent output stream: {reason}")]
    StreamStart {
        /// cpal diagnostic.
        reason: String,
    },
    /// The persistent input stream could not be started.
    #[error("cannot start persistent input stream: {reason}")]
    InputStreamStart {
        /// Bounded cpal diagnostic.
        reason: String,
    },
    /// The device invalidated the live stream.
    #[error("persistent output stream failed")]
    StreamFailed,
    /// The input device invalidated the live stream.
    #[error("persistent input stream failed")]
    InputStreamFailed,
    /// The callback observed PCM without matching published sentence metadata.
    #[error("persistent output callback observed an invalid PCM boundary")]
    CallbackContract,
    /// The stream owner closed playback while the producer still had PCM.
    #[error("persistent output stream is closed")]
    PlaybackClosed,
    /// A chunk does not match the playback input contract.
    #[error("playback requires {expected:?}, got {actual:?}")]
    FormatMismatch {
        /// Required model PCM format.
        expected: AudioFormat,
        /// Supplied chunk format.
        actual: AudioFormat,
    },
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
