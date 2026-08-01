use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use serde::Serialize;

use crate::{AudioFormat, DeviceBufferSize, DeviceError, Transcript, VadEndpoint};

mod device;
mod worker;

pub use device::capture_devices;
pub use worker::CaptureWorker;
#[cfg(all(test, feature = "whisper-cuda"))]
pub(crate) use worker::recognize_segment;

const DEFAULT_CAPACITY_SAMPLES: usize = 192_000;
const DEFAULT_PREFERRED_BUFFER_FRAMES: u32 = 256;
const MAX_DIAGNOSTIC_CHARS: usize = 2_048;

/// Input-device choice that never changes the host's default audio policy.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum InputDeviceSelection {
    /// Use the host's current default input device.
    #[default]
    Default,
    /// Open exactly one backend-qualified cpal input device.
    Id(String),
}

/// Bounded construction settings for one persistent input stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaptureConfig {
    capacity_samples: usize,
    preferred_buffer_frames: u32,
    device: InputDeviceSelection,
}

impl Default for CaptureConfig {
    fn default() -> Self {
        Self {
            capacity_samples: DEFAULT_CAPACITY_SAMPLES,
            preferred_buffer_frames: DEFAULT_PREFERRED_BUFFER_FRAMES,
            device: InputDeviceSelection::Default,
        }
    }
}

impl CaptureConfig {
    /// Uses the default bounded ring and period with one explicit device choice.
    pub fn for_device(device: InputDeviceSelection) -> Self {
        Self {
            device,
            ..Self::default()
        }
    }

    /// Constructs a nonzero raw-sample ring and callback-period request.
    pub fn new(
        capacity_samples: usize,
        preferred_buffer_frames: u32,
        device: InputDeviceSelection,
    ) -> Result<Self, DeviceError> {
        if capacity_samples == 0 || preferred_buffer_frames == 0 {
            return Err(DeviceError::InvalidCaptureConfig {
                capacity_samples,
                preferred_buffer_frames,
            });
        }
        Ok(Self {
            capacity_samples,
            preferred_buffer_frames,
            device,
        })
    }

    /// Returns the exact raw native-sample ring capacity.
    pub fn capacity_samples(&self) -> usize {
        self.capacity_samples
    }

    /// Returns the desired callback period before device-range clamping.
    pub fn preferred_buffer_frames(&self) -> u32 {
        self.preferred_buffer_frames
    }

    /// Returns the explicit or default device choice.
    pub fn device(&self) -> &InputDeviceSelection {
        &self.device
    }
}

/// One discoverable input device without any policy mutation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CaptureDeviceDescriptor {
    /// cpal host backend name.
    pub backend: String,
    /// Backend-qualified cpal device identifier.
    pub device_id: String,
    /// Backend-provided display name.
    pub device: String,
    /// Whether this device was the host default at enumeration time.
    pub is_default: bool,
}

/// Exact host, device, format, and ring identity for a live input stream.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CaptureDeviceInfo {
    /// cpal host backend name.
    pub backend: String,
    /// Backend-qualified cpal device identifier.
    pub device_id: String,
    /// Backend-provided display name.
    pub device: String,
    /// Actual interleaved native input stream format.
    pub format: AudioFormat,
    /// Fixed mono f32 worker/VAD/recognizer format.
    pub worker_format: AudioFormat,
    /// Requested device buffer mode and advertised range.
    pub buffer_size: DeviceBufferSize,
}

/// Samples lost because the bounded callback ring had no complete-frame room.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct CaptureOverflow {
    /// Callback invocations that dropped at least one complete input frame.
    pub callbacks: u64,
    /// Native interleaved samples dropped, always whole-frame aligned.
    pub samples: u64,
}

/// Observable persistent-stream, worker, VAD, and conversion counters.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct CaptureMetrics {
    /// Successful stream constructions. This remains one for the worker lifetime.
    pub stream_opens: u64,
    /// Capture worker threads constructed. This remains one.
    pub worker_threads: u64,
    /// Exact fixed native-sample ring capacity.
    pub ring_capacity_samples: usize,
    /// Complete native frames normalized on the worker.
    pub input_frames: u64,
    /// Mono 16 kHz samples emitted to VAD.
    pub output_frames: u64,
    /// Onset-qualified segments rejected by minimum-speech hysteresis.
    pub rejected_transients: u64,
    /// Final transcripts returned to explicit capture requests.
    pub transcripts: u64,
    /// Nonempty changed partial hypotheses delivered before finalization.
    pub partial_updates: u64,
    /// Aggregate worker-side normalization and resampling time.
    pub normalization_resampling_us: u64,
    /// Bounded callback overflow accounting.
    pub overflow: CaptureOverflow,
}

/// One ephemeral rolling hypothesis and its visible-presentation timing input.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CapturePartial {
    /// Typed non-final recognizer hypothesis.
    pub transcript: Transcript,
    /// Audio-frame availability through capture-worker delivery.
    pub audio_available_to_partial_us: u64,
    /// Audio-frame availability through root presentation, when rendered live.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio_available_to_visible_us: Option<u64>,
    #[serde(skip)]
    available_at: Instant,
}

impl CapturePartial {
    /// Records worker delivery while retaining a process-local availability clock.
    pub fn new(transcript: Transcript, audio_available_to_partial_us: u64) -> Self {
        let delivered_at = Instant::now();
        let available_at = delivered_at
            .checked_sub(Duration::from_micros(audio_available_to_partial_us))
            .unwrap_or(delivered_at);
        Self {
            transcript,
            audio_available_to_partial_us,
            audio_available_to_visible_us: None,
            available_at,
        }
    }

    /// Measures availability through the caller's just-completed observation.
    pub fn observed_latency_us(&self) -> u64 {
        u64::try_from(self.available_at.elapsed().as_micros()).unwrap_or(u64::MAX)
    }
}

/// One explicit VAD-closed recognition outcome.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CaptureReport {
    /// Monotonic explicit-capture request sequence.
    pub sequence: u64,
    /// The only committed recognizer outcome for this request.
    pub transcript: Transcript,
    /// Ordered ephemeral partials delivered before this final transcript.
    pub partials: Vec<CapturePartial>,
    /// Exact Silero VAD boundaries on the 16 kHz worker clock.
    pub endpoint: VadEndpoint,
    /// VAD close through final transcript construction.
    pub vad_close_to_final_us: u64,
    /// Worker-side normalization and resampling time for the request.
    pub normalization_resampling_us: u64,
    /// Complete native frames consumed for the request.
    pub input_frames: u64,
    /// Mono 16 kHz samples passed through VAD for the request.
    pub output_frames: u64,
    /// Callback overflow snapshot when the transcript completed.
    pub overflow: CaptureOverflow,
}

/// Deterministic ownership proof returned after capture teardown.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct CaptureWorkerShutdown {
    /// Whether the sole worker joined without panic.
    pub worker_joined: bool,
    /// Whether the input stream owner dropped its stream.
    pub input_closed: bool,
    /// Successful stream constructions during this worker lifetime.
    pub stream_opens: u64,
    /// Capture worker threads constructed during this worker lifetime.
    pub worker_threads: u64,
    /// Whether an unwind was caught inside the owned worker.
    pub worker_panicked: bool,
}

#[derive(Default)]
struct CaptureCounters {
    overflow_callbacks: AtomicU64,
    overflow_samples: AtomicU64,
    input_frames: AtomicU64,
    output_frames: AtomicU64,
    rejected_transients: AtomicU64,
    transcripts: AtomicU64,
    partial_updates: AtomicU64,
    normalization_resampling_us: AtomicU64,
}

impl CaptureCounters {
    fn overflow(&self) -> CaptureOverflow {
        CaptureOverflow {
            callbacks: self.overflow_callbacks.load(Ordering::Relaxed),
            samples: self.overflow_samples.load(Ordering::Relaxed),
        }
    }
}

fn bounded(value: &str) -> String {
    value.chars().take(MAX_DIAGNOSTIC_CHARS).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_serialization_separates_worker_delivery_from_optional_visibility() {
        let mut partial = CapturePartial::new(
            Transcript::new("rolling words", false, 320).unwrap(),
            12_000,
        );
        assert!(partial.observed_latency_us() >= 12_000);
        let delivered = serde_json::to_value(&partial).unwrap();
        assert_eq!(delivered["audio_available_to_partial_us"], 12_000);
        assert!(delivered.get("audio_available_to_visible_us").is_none());
        assert!(delivered.get("available_at").is_none());

        partial.audio_available_to_visible_us = Some(13_000);
        let visible = serde_json::to_value(partial).unwrap();
        assert_eq!(visible["audio_available_to_visible_us"], 13_000);
    }
}
