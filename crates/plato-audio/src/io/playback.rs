use std::{
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use cpal::{
    BufferSize, SampleFormat as CpalSampleFormat, Stream, StreamConfig, SupportedBufferSize,
    SupportedStreamConfig,
    traits::{DeviceTrait, HostTrait, StreamTrait},
};
use serde::Serialize;

use crate::{AudioFormat, DeviceError, PcmChunk, SampleFormat, core::playback::CallbackBuffer};

/// Exact cpal runtime crate version used by the playback implementation.
pub const CPAL_RUNTIME_VERSION: &str = "cpal 0.18.1";

const DEFAULT_CAPACITY_FRAMES: usize = 24_000 * 120;
const DEFAULT_PREFERRED_BUFFER_FRAMES: u32 = 256;
const PLAYBACK_TIMEOUT_SLACK: Duration = Duration::from_secs(5);
const POLL_INTERVAL: Duration = Duration::from_millis(1);
const AUDIBLE_EPSILON: f32 = 1.0e-6;

/// Buffer request selected from cpal's advertised device range.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum DeviceBufferSize {
    /// A fixed low-latency period was requested.
    Fixed {
        /// Requested callback period in frames.
        requested_frames: u32,
        /// Device-advertised minimum frames.
        supported_min_frames: u32,
        /// Device-advertised maximum frames.
        supported_max_frames: u32,
    },
    /// The backend does not expose a controllable period.
    DefaultUnknown,
}

/// Bounded construction settings for the persistent output stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlaybackConfig {
    capacity_frames: usize,
    preferred_buffer_frames: u32,
}

impl Default for PlaybackConfig {
    fn default() -> Self {
        Self {
            capacity_frames: DEFAULT_CAPACITY_FRAMES,
            preferred_buffer_frames: DEFAULT_PREFERRED_BUFFER_FRAMES,
        }
    }
}

impl PlaybackConfig {
    /// Constructs a configuration with a nonzero mono-frame capacity.
    pub fn new(capacity_frames: usize, preferred_buffer_frames: u32) -> Result<Self, DeviceError> {
        if capacity_frames == 0 || preferred_buffer_frames == 0 {
            return Err(DeviceError::DeviceQuery {
                reason: "playback capacity and preferred buffer size must be nonzero".to_owned(),
            });
        }
        Ok(Self {
            capacity_frames,
            preferred_buffer_frames,
        })
    }

    /// Returns the maximum mono sentence frames held by the callback buffer.
    pub fn capacity_frames(self) -> usize {
        self.capacity_frames
    }

    /// Returns the desired callback period before device-range clamping.
    pub fn preferred_buffer_frames(self) -> u32 {
        self.preferred_buffer_frames
    }
}

/// Exact host, device, format, and buffer request for a live output stream.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PlaybackDeviceInfo {
    /// cpal host backend name.
    pub backend: String,
    /// Backend-qualified cpal device identifier.
    pub device_id: String,
    /// Default output device name.
    pub device: String,
    /// Actual device stream format.
    pub format: AudioFormat,
    /// Requested device buffer mode and advertised range.
    pub buffer_size: DeviceBufferSize,
}

/// Observable persistent-device reuse counters.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct PlaybackMetrics {
    /// Successful stream constructions. This remains one for a reused device.
    pub stream_opens: u64,
    /// Completely drained sentence chunks.
    pub chunks_played: u64,
}

/// Timing observed for one serial sentence playback.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct PlaybackReport {
    /// Sentence-acceptance to first non-silent callback frame in microseconds.
    pub accepted_to_first_non_silent_us: u64,
    /// Actual frames supplied in the callback that emitted the first audible sample.
    pub first_callback_frames: usize,
    /// Callback invocations while this sentence was active.
    pub callback_count: u64,
    /// Complete mono PCM frames drained.
    pub frames_played: usize,
}

/// One persistent cpal output stream that drains synthesized sentences serially.
pub struct PersistentPlayback {
    callback: Arc<CallbackBuffer>,
    device_info: PlaybackDeviceInfo,
    metrics: PlaybackMetrics,
    _stream: Stream,
}

impl PersistentPlayback {
    /// Opens and starts the default output device exactly once.
    pub fn open(config: PlaybackConfig) -> Result<Self, DeviceError> {
        let host = cpal::default_host();
        let backend = host.id().name().to_owned();
        let device = host
            .default_output_device()
            .ok_or(DeviceError::NoOutputDevice)?;
        let device_id = device
            .id()
            .map_err(|error| DeviceError::DeviceQuery {
                reason: error.to_string(),
            })?
            .to_string();
        let device_name = device.to_string();
        let supported = device
            .supported_output_configs()
            .map_err(|error| DeviceError::DeviceQuery {
                reason: error.to_string(),
            })?
            .collect::<Vec<_>>();
        let selected =
            choose_supported_config(&supported).ok_or(DeviceError::UnsupportedSampleRate {
                sample_rate: super::KOKORO_SAMPLE_RATE,
            })?;
        let (buffer_size, requested_buffer) =
            select_buffer_size(selected.buffer_size(), config.preferred_buffer_frames);
        let mut stream_config: StreamConfig = selected.into();
        stream_config.buffer_size = requested_buffer;
        let channels = usize::from(stream_config.channels);
        let callback = Arc::new(CallbackBuffer::new(config.capacity_frames));
        let stream = build_stream(
            &device,
            stream_config,
            selected.sample_format(),
            Arc::clone(&callback),
            channels,
        )?;
        stream.play().map_err(|error| DeviceError::StreamStart {
            reason: error.to_string(),
        })?;
        let format = AudioFormat::new(
            selected.sample_rate(),
            selected.channels(),
            sample_format(selected.sample_format()),
        )?;

        Ok(Self {
            callback,
            device_info: PlaybackDeviceInfo {
                backend,
                device_id,
                device: device_name,
                format,
                buffer_size,
            },
            metrics: PlaybackMetrics {
                stream_opens: 1,
                chunks_played: 0,
            },
            _stream: stream,
        })
    }

    /// Returns the exact live stream identity.
    pub fn device_info(&self) -> &PlaybackDeviceInfo {
        &self.device_info
    }

    /// Returns counters proving stream reuse.
    pub fn metrics(&self) -> PlaybackMetrics {
        self.metrics
    }

    /// Plays one complete 24 kHz mono f32 chunk and waits for its final frame.
    ///
    /// `accepted_at` is the sentence-acceptance boundary and may precede
    /// synthesis. The report measures from that instant to the first audible
    /// device callback frame.
    pub fn play_blocking(
        &mut self,
        chunk: &PcmChunk,
        accepted_at: Instant,
    ) -> Result<PlaybackReport, DeviceError> {
        let expected = AudioFormat::new(super::KOKORO_SAMPLE_RATE, 1, SampleFormat::F32)?;
        if chunk.format() != expected {
            return Err(DeviceError::FormatMismatch {
                expected,
                actual: chunk.format(),
            });
        }
        let samples = chunk.samples().as_f32().expect("validated f32 chunk");
        if !samples.iter().any(|sample| sample.abs() > AUDIBLE_EPSILON) {
            return Err(DeviceError::SilentChunk);
        }
        let accepted_ns = self.callback.timestamp(accepted_at);
        self.callback.load(samples, accepted_ns)?;
        let timeout = chunk.duration().saturating_add(PLAYBACK_TIMEOUT_SLACK);
        let deadline = Instant::now() + timeout;
        while !self.callback.is_idle() {
            if self.callback.stream_failed() {
                return Err(DeviceError::StreamFailed);
            }
            if Instant::now() >= deadline {
                return Err(DeviceError::PlaybackTimeout {
                    milliseconds: timeout.as_millis(),
                });
            }
            thread::sleep(POLL_INTERVAL);
        }
        if self.callback.stream_failed() {
            return Err(DeviceError::StreamFailed);
        }
        let observation = self.callback.observation();
        let first_non_silent_ns = observation
            .first_non_silent_ns
            .ok_or(DeviceError::SilentChunk)?;
        let first_callback_frames = observation
            .first_callback_frames
            .ok_or(DeviceError::SilentChunk)?;
        self.metrics.chunks_played += 1;
        Ok(PlaybackReport {
            accepted_to_first_non_silent_us: first_non_silent_ns
                .saturating_sub(observation.accepted_ns)
                / 1_000,
            first_callback_frames,
            callback_count: observation.callback_count,
            frames_played: chunk.frame_count(),
        })
    }
}

fn choose_supported_config(
    supported: &[cpal::SupportedStreamConfigRange],
) -> Option<SupportedStreamConfig> {
    [
        CpalSampleFormat::F32,
        CpalSampleFormat::I16,
        CpalSampleFormat::U16,
    ]
    .into_iter()
    .find_map(|sample_format| {
        supported
            .iter()
            .filter(|range| range.sample_format() == sample_format)
            .filter_map(|range| range.try_with_sample_rate(super::KOKORO_SAMPLE_RATE))
            .min_by_key(SupportedStreamConfig::channels)
    })
}

fn select_buffer_size(
    supported: &SupportedBufferSize,
    preferred: u32,
) -> (DeviceBufferSize, BufferSize) {
    match *supported {
        SupportedBufferSize::Range { min, max } => {
            let requested = preferred.clamp(min, max);
            (
                DeviceBufferSize::Fixed {
                    requested_frames: requested,
                    supported_min_frames: min,
                    supported_max_frames: max,
                },
                BufferSize::Fixed(requested),
            )
        }
        SupportedBufferSize::Unknown => (DeviceBufferSize::DefaultUnknown, BufferSize::Default),
    }
}

fn build_stream(
    device: &cpal::Device,
    config: StreamConfig,
    sample_format: CpalSampleFormat,
    callback: Arc<CallbackBuffer>,
    channels: usize,
) -> Result<Stream, DeviceError> {
    match sample_format {
        CpalSampleFormat::F32 => build_typed_stream(
            device,
            config,
            callback,
            channels,
            CallbackBuffer::write_f32,
        ),
        CpalSampleFormat::I16 => build_typed_stream(
            device,
            config,
            callback,
            channels,
            CallbackBuffer::write_i16,
        ),
        CpalSampleFormat::U16 => build_typed_stream(
            device,
            config,
            callback,
            channels,
            CallbackBuffer::write_u16,
        ),
        _ => Err(DeviceError::StreamBuild {
            reason: format!("unsupported selected cpal sample format {sample_format}"),
        }),
    }
}

fn build_typed_stream<T: cpal::SizedSample + 'static>(
    device: &cpal::Device,
    config: StreamConfig,
    callback: Arc<CallbackBuffer>,
    channels: usize,
    write: fn(&CallbackBuffer, &mut [T], usize),
) -> Result<Stream, DeviceError> {
    let error_state = Arc::clone(&callback);
    device
        .build_output_stream(
            config,
            move |output: &mut [T], _| write(&callback, output, channels),
            move |_| error_state.mark_stream_failed(),
            None,
        )
        .map_err(|error| DeviceError::StreamBuild {
            reason: error.to_string(),
        })
}

fn sample_format(sample_format: CpalSampleFormat) -> SampleFormat {
    match sample_format {
        CpalSampleFormat::F32 => SampleFormat::F32,
        CpalSampleFormat::I16 => SampleFormat::I16,
        CpalSampleFormat::U16 => SampleFormat::U16,
        _ => unreachable!("selection filters cpal sample formats"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selection_prefers_f32_and_fewer_channels_at_model_rate() {
        let supported = [
            cpal::SupportedStreamConfigRange::new(
                2,
                8_000,
                48_000,
                SupportedBufferSize::Unknown,
                CpalSampleFormat::I16,
            ),
            cpal::SupportedStreamConfigRange::new(
                6,
                24_000,
                96_000,
                SupportedBufferSize::Unknown,
                CpalSampleFormat::F32,
            ),
            cpal::SupportedStreamConfigRange::new(
                2,
                24_000,
                48_000,
                SupportedBufferSize::Unknown,
                CpalSampleFormat::F32,
            ),
        ];
        let selected = choose_supported_config(&supported).unwrap();
        assert_eq!(selected.sample_format(), CpalSampleFormat::F32);
        assert_eq!(selected.channels(), 2);
        assert_eq!(selected.sample_rate(), super::super::KOKORO_SAMPLE_RATE);
    }

    #[test]
    fn selection_rejects_ranges_without_model_rate() {
        let supported = [cpal::SupportedStreamConfigRange::new(
            2,
            44_100,
            48_000,
            SupportedBufferSize::Unknown,
            CpalSampleFormat::F32,
        )];
        assert!(choose_supported_config(&supported).is_none());
    }

    #[test]
    fn fixed_period_is_clamped_to_advertised_range() {
        assert_eq!(
            select_buffer_size(
                &SupportedBufferSize::Range {
                    min: 512,
                    max: 1024
                },
                256
            ),
            (
                DeviceBufferSize::Fixed {
                    requested_frames: 512,
                    supported_min_frames: 512,
                    supported_max_frames: 1024,
                },
                BufferSize::Fixed(512),
            )
        );
        assert_eq!(
            select_buffer_size(&SupportedBufferSize::Unknown, 256),
            (DeviceBufferSize::DefaultUnknown, BufferSize::Default)
        );
    }

    #[test]
    fn playback_config_rejects_zero_bounds_without_a_device() {
        assert!(PlaybackConfig::new(0, 256).is_err());
        assert!(PlaybackConfig::new(1024, 0).is_err());
    }
}
