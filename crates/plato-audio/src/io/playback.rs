use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use cpal::{
    BufferSize, SampleFormat as CpalSampleFormat, Stream, StreamConfig, SupportedBufferSize,
    SupportedStreamConfig,
    traits::{DeviceTrait, HostTrait, StreamTrait},
};
use rtrb::{Producer, PushError, RingBuffer};
use serde::Serialize;

use crate::{
    AudioFormat, BargeInHandle, DeviceError, PcmChunk, SampleFormat,
    core::playback::{CallbackDrain, PlaybackConsumerReplacement, PlaybackTimeline},
};

/// Exact cpal runtime crate version used by the playback implementation.
pub const CPAL_RUNTIME_VERSION: &str = "cpal 0.18.1";
/// Exact wait-free PCM ring implementation used by the callback edge.
pub const RTRB_RUNTIME_VERSION: &str = "rtrb 0.3.4";

const DEFAULT_CAPACITY_FRAMES: usize = 24_000 * 120;
const DEFAULT_PREFERRED_BUFFER_FRAMES: u32 = 256;
const RING_STALL_TIMEOUT: Duration = Duration::from_secs(5);
const RING_POLL_INTERVAL: Duration = Duration::from_millis(1);
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
    /// Constructs a configuration with a nonzero mono-frame ring capacity.
    pub fn new(capacity_frames: usize, preferred_buffer_frames: u32) -> Result<Self, DeviceError> {
        if capacity_frames == 0 || preferred_buffer_frames == 0 {
            return Err(DeviceError::InvalidPlaybackConfig {
                capacity_frames,
                preferred_buffer_frames,
            });
        }
        Ok(Self {
            capacity_frames,
            preferred_buffer_frames,
        })
    }

    /// Returns the exact mono f32 sample capacity of the PCM ring.
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
    /// Mono f32 format accepted by the callback ring.
    pub ring_format: AudioFormat,
    /// Requested device buffer mode and advertised range.
    pub buffer_size: DeviceBufferSize,
}

/// Explicit silence emitted while an accepted sentence lacked ring PCM.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct PlaybackUnderrun {
    /// Callback invocations that emitted at least one underrun frame.
    pub callbacks: u64,
    /// Device frames filled with silence during those callbacks.
    pub frames: u64,
}

/// Observable persistent-device and bounded-ring counters.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct PlaybackMetrics {
    /// Successful stream constructions. This remains one for a reused device.
    pub stream_opens: u64,
    /// Completely drained sentence regions.
    pub chunks_played: u64,
    /// Output callback invocations since stream construction.
    pub callback_count: u64,
    /// Exact fixed PCM ring capacity.
    pub ring_capacity_frames: usize,
    /// Highest accepted-but-not-finished job count observed by the owner.
    pub max_accepted_unfinished: usize,
    /// Aggregate underrun silence while jobs were accepted.
    pub underrun: PlaybackUnderrun,
}

/// Callback and synthesis timing for one sentence region.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct PlaybackReport {
    /// Worker-global sentence sequence.
    pub sequence: u64,
    /// Sentence acceptance timestamp relative to stream construction.
    pub accepted_ns: u64,
    /// Synth worker start timestamp relative to stream construction.
    pub synth_started_ns: u64,
    /// Synth plus resampling completion timestamp.
    pub synth_finished_ns: u64,
    /// First sentence PCM frame copied by the callback.
    pub first_pcm_ns: u64,
    /// First non-silent sentence PCM frame copied by the callback.
    pub first_non_silent_ns: u64,
    /// Exclusive end timestamp of the final sentence PCM frame.
    pub pcm_end_ns: u64,
    /// Sentence-acceptance to first non-silent callback frame.
    pub accepted_to_first_non_silent_us: u64,
    /// Worker synthesis plus resampling duration.
    pub synthesis_us: u64,
    /// Silence after the previous sentence PCM region, if one exists.
    pub gap_before_us: Option<u64>,
    /// Device frames in the first callback touching this sentence.
    pub first_callback_frames: usize,
    /// Callback invocations from first touch through final PCM.
    pub callback_count: u64,
    /// Complete source PCM frames before resampling.
    pub source_frames: usize,
    /// Complete mono device-rate frames drained from rtrb.
    pub device_frames: usize,
    /// Typed silence emitted while this sentence was accepted but unavailable.
    pub underrun: PlaybackUnderrun,
}

/// One persistent cpal output stream whose callback owns the rtrb consumer.
pub(crate) struct PersistentPlayback {
    timeline: Arc<PlaybackTimeline>,
    device_info: PlaybackDeviceInfo,
    ring_capacity_frames: usize,
    stream: Option<Stream>,
}

pub(crate) struct PlaybackProducer {
    producer: Producer<f32>,
    replacements: Producer<PlaybackConsumerReplacement>,
    timeline: Arc<PlaybackTimeline>,
    barge_in: BargeInHandle,
    ring_format: AudioFormat,
    ring_capacity_frames: usize,
    produced_samples: u64,
    replacement_generation: u64,
}

pub(crate) struct PreparedSentence<'a> {
    pub(crate) start_sample: u64,
    pub(crate) end_sample: u64,
    samples: &'a [f32],
}

pub(crate) enum PlaybackWriteError {
    Canceled,
    Device(DeviceError),
}

impl PersistentPlayback {
    pub(crate) fn open(
        config: PlaybackConfig,
        barge_in: BargeInHandle,
    ) -> Result<(Self, PlaybackProducer), DeviceError> {
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
        let default = device
            .default_output_config()
            .map_err(|error| DeviceError::DeviceQuery {
                reason: error.to_string(),
            })?;
        let selected = if supported_sample_format(default.sample_format()) {
            default
        } else {
            let supported = device
                .supported_output_configs()
                .map_err(|error| DeviceError::DeviceQuery {
                    reason: error.to_string(),
                })?
                .collect::<Vec<_>>();
            choose_supported_config(&supported).ok_or(DeviceError::UnsupportedOutputFormat)?
        };
        let (buffer_size, requested_buffer) =
            select_buffer_size(selected.buffer_size(), config.preferred_buffer_frames);
        let mut stream_config: StreamConfig = selected.into();
        stream_config.buffer_size = requested_buffer;
        let device_format = AudioFormat::new(
            selected.sample_rate(),
            selected.channels(),
            sample_format(selected.sample_format()),
        )?;
        let ring_format = AudioFormat::new(device_format.sample_rate(), 1, SampleFormat::F32)?;
        barge_in
            .configure_output(device_format.sample_rate())
            .map_err(|_| DeviceError::CallbackContract)?;
        let timeline = Arc::new(PlaybackTimeline::new());
        let (producer, consumer) = RingBuffer::new(config.capacity_frames);
        let (replacement_producer, replacement_consumer) = RingBuffer::new(1);
        let drain = CallbackDrain::new(
            consumer,
            replacement_consumer,
            Arc::clone(&timeline),
            barge_in.clone(),
            device_format.sample_rate(),
        );
        let stream = build_stream(
            &device,
            stream_config,
            selected.sample_format(),
            drain,
            Arc::clone(&timeline),
            usize::from(device_format.channels()),
        )?;
        stream.play().map_err(|error| DeviceError::StreamStart {
            reason: error.to_string(),
        })?;
        let playback = Self {
            timeline: Arc::clone(&timeline),
            device_info: PlaybackDeviceInfo {
                backend,
                device_id,
                device: device_name,
                format: device_format,
                ring_format,
                buffer_size,
            },
            ring_capacity_frames: config.capacity_frames,
            stream: Some(stream),
        };
        let producer = PlaybackProducer {
            producer,
            replacements: replacement_producer,
            timeline,
            barge_in,
            ring_format,
            ring_capacity_frames: config.capacity_frames,
            produced_samples: 0,
            replacement_generation: 0,
        };
        Ok((playback, producer))
    }

    /// Returns the exact live stream identity.
    pub fn device_info(&self) -> &PlaybackDeviceInfo {
        &self.device_info
    }

    pub(crate) fn timeline(&self) -> &Arc<PlaybackTimeline> {
        &self.timeline
    }

    pub(crate) fn metrics(&self, max_accepted_unfinished: usize) -> PlaybackMetrics {
        PlaybackMetrics {
            stream_opens: 1,
            chunks_played: self.timeline.finished_sentences(),
            callback_count: self.timeline.callback_count(),
            ring_capacity_frames: self.ring_capacity_frames,
            max_accepted_unfinished,
            underrun: PlaybackUnderrun {
                callbacks: self.timeline.underrun_callbacks(),
                frames: self.timeline.underrun_frames(),
            },
        }
    }

    pub(crate) fn close(&mut self) {
        self.timeline.mark_shutdown();
        drop(self.stream.take());
    }

    #[cfg(test)]
    pub(crate) fn test_pair(
        device_format: AudioFormat,
        ring_capacity_frames: usize,
        barge_in: BargeInHandle,
    ) -> (Self, PlaybackProducer, CallbackDrain) {
        let ring_format =
            AudioFormat::new(device_format.sample_rate(), 1, SampleFormat::F32).unwrap();
        let timeline = Arc::new(PlaybackTimeline::new());
        let (producer, consumer) = RingBuffer::new(ring_capacity_frames);
        let (replacement_producer, replacement_consumer) = RingBuffer::new(1);
        barge_in
            .configure_output(device_format.sample_rate())
            .unwrap();
        let callback = CallbackDrain::new(
            consumer,
            replacement_consumer,
            Arc::clone(&timeline),
            barge_in.clone(),
            device_format.sample_rate(),
        );
        (
            Self {
                timeline: Arc::clone(&timeline),
                device_info: PlaybackDeviceInfo {
                    backend: "test".to_owned(),
                    device_id: "test:output".to_owned(),
                    device: "Synthetic output".to_owned(),
                    format: device_format,
                    ring_format,
                    buffer_size: DeviceBufferSize::Fixed {
                        requested_frames: 4,
                        supported_min_frames: 1,
                        supported_max_frames: 64,
                    },
                },
                ring_capacity_frames,
                stream: None,
            },
            PlaybackProducer {
                producer,
                replacements: replacement_producer,
                timeline,
                barge_in,
                ring_format,
                ring_capacity_frames,
                produced_samples: 0,
                replacement_generation: 0,
            },
            callback,
        )
    }
}

impl Drop for PersistentPlayback {
    fn drop(&mut self) {
        self.close();
    }
}

impl PlaybackProducer {
    #[cfg(test)]
    pub(crate) fn ring_format(&self) -> AudioFormat {
        self.ring_format
    }

    pub(crate) fn prepare_sentence<'a>(
        &self,
        sequence: u64,
        source_frames: usize,
        chunk: &'a PcmChunk,
    ) -> Result<PreparedSentence<'a>, DeviceError> {
        if chunk.format() != self.ring_format {
            return Err(DeviceError::FormatMismatch {
                expected: self.ring_format,
                actual: chunk.format(),
            });
        }
        let samples = chunk.samples().as_f32().expect("ring contract is f32");
        if samples.is_empty() || !samples.iter().any(|sample| sample.abs() > AUDIBLE_EPSILON) {
            return Err(DeviceError::SilentChunk);
        }
        self.timeline
            .publish_pcm(
                sequence,
                self.produced_samples,
                source_frames,
                samples.len(),
            )
            .map_err(|_| DeviceError::CallbackContract)?;
        Ok(PreparedSentence {
            start_sample: self.produced_samples,
            end_sample: self
                .produced_samples
                .saturating_add(u64::try_from(samples.len()).unwrap_or(u64::MAX)),
            samples,
        })
    }

    pub(crate) fn write_prepared(
        &mut self,
        prepared: PreparedSentence<'_>,
        cancel: &AtomicBool,
    ) -> Result<(), PlaybackWriteError> {
        let mut remaining = prepared.samples;
        let mut stall_deadline = Instant::now() + RING_STALL_TIMEOUT;
        while !remaining.is_empty() {
            if cancel.load(Ordering::Acquire) {
                return Err(PlaybackWriteError::Canceled);
            }
            self.check_health().map_err(PlaybackWriteError::Device)?;
            let pushed = self.producer.slots().min(remaining.len());
            if pushed > 0 {
                self.barge_in.record_queued_frames(pushed);
                let (pushed_samples, remainder) =
                    self.producer.push_partial_slice(&remaining[..pushed]);
                debug_assert_eq!(pushed_samples.len(), pushed);
                debug_assert!(remainder.is_empty());
                remaining = &remaining[pushed..];
            }
            self.produced_samples = self
                .produced_samples
                .saturating_add(u64::try_from(pushed).unwrap_or(u64::MAX));
            if pushed > 0 {
                stall_deadline = Instant::now() + RING_STALL_TIMEOUT;
            } else if Instant::now() >= stall_deadline {
                return Err(PlaybackWriteError::Device(DeviceError::PlaybackTimeout {
                    milliseconds: RING_STALL_TIMEOUT.as_millis(),
                }));
            } else {
                thread::sleep(RING_POLL_INTERVAL);
            }
        }
        Ok(())
    }

    pub(crate) fn flush(&mut self, next_sequence: u64) -> Result<usize, DeviceError> {
        self.replacement_generation = self.replacement_generation.saturating_add(1);
        let generation = self.replacement_generation;
        let (replacement_producer, replacement_consumer) =
            RingBuffer::new(self.ring_capacity_frames);
        let retired_producer = std::mem::replace(&mut self.producer, replacement_producer);
        let mut replacement = PlaybackConsumerReplacement {
            generation,
            consumer: replacement_consumer,
            next_sequence,
        };
        let deadline = Instant::now() + RING_STALL_TIMEOUT;
        loop {
            match self.replacements.push(replacement) {
                Ok(()) => break,
                Err(PushError::Full(returned)) => replacement = returned,
            }
            self.check_health()?;
            if Instant::now() >= deadline {
                return Err(DeviceError::PlaybackTimeout {
                    milliseconds: RING_STALL_TIMEOUT.as_millis(),
                });
            }
            thread::sleep(RING_POLL_INTERVAL);
        }
        while self.timeline.consumer_generation() != generation {
            self.check_health()?;
            if Instant::now() >= deadline {
                return Err(DeviceError::PlaybackTimeout {
                    milliseconds: RING_STALL_TIMEOUT.as_millis(),
                });
            }
            thread::sleep(RING_POLL_INTERVAL);
        }
        if self.barge_in.playback_started() {
            while !self.barge_in.silent_callback_observed() {
                self.check_health()?;
                if Instant::now() >= deadline {
                    return Err(DeviceError::PlaybackTimeout {
                        milliseconds: RING_STALL_TIMEOUT.as_millis(),
                    });
                }
                thread::sleep(RING_POLL_INTERVAL);
            }
        }
        drop(retired_producer);
        let sample_cursor = self.timeline.played_samples();
        self.produced_samples = sample_cursor;
        Ok(self.barge_in.flush_pcm_queue())
    }

    fn check_health(&self) -> Result<(), DeviceError> {
        if self.timeline.stream_failed() {
            return Err(DeviceError::StreamFailed);
        }
        if self.timeline.callback_contract_failed() {
            return Err(DeviceError::CallbackContract);
        }
        if self.timeline.is_shutdown() {
            return Err(DeviceError::PlaybackClosed);
        }
        Ok(())
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
            .cloned()
            .map(cpal::SupportedStreamConfigRange::with_max_sample_rate)
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
    drain: CallbackDrain,
    timeline: Arc<PlaybackTimeline>,
    channels: usize,
) -> Result<Stream, DeviceError> {
    match sample_format {
        CpalSampleFormat::F32 => build_typed_stream(
            device,
            config,
            drain,
            timeline,
            channels,
            CallbackDrain::write_f32,
        ),
        CpalSampleFormat::I16 => build_typed_stream(
            device,
            config,
            drain,
            timeline,
            channels,
            CallbackDrain::write_i16,
        ),
        CpalSampleFormat::U16 => build_typed_stream(
            device,
            config,
            drain,
            timeline,
            channels,
            CallbackDrain::write_u16,
        ),
        _ => Err(DeviceError::UnsupportedOutputFormat),
    }
}

fn build_typed_stream<T: cpal::SizedSample + 'static>(
    device: &cpal::Device,
    config: StreamConfig,
    mut drain: CallbackDrain,
    timeline: Arc<PlaybackTimeline>,
    channels: usize,
    write: fn(&mut CallbackDrain, &mut [T], usize),
) -> Result<Stream, DeviceError> {
    device
        .build_output_stream(
            config,
            move |output: &mut [T], _| write(&mut drain, output, channels),
            move |_| timeline.mark_stream_failed(),
            None,
        )
        .map_err(|error| DeviceError::StreamBuild {
            reason: error.to_string(),
        })
}

fn supported_sample_format(sample_format: CpalSampleFormat) -> bool {
    matches!(
        sample_format,
        CpalSampleFormat::F32 | CpalSampleFormat::I16 | CpalSampleFormat::U16
    )
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
    fn fallback_selection_prefers_f32_and_fewer_channels() {
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
        assert_eq!(selected.sample_rate(), 48_000);
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
        assert!(matches!(
            PlaybackConfig::new(0, 256),
            Err(DeviceError::InvalidPlaybackConfig {
                capacity_frames: 0,
                preferred_buffer_frames: 256
            })
        ));
        assert!(matches!(
            PlaybackConfig::new(1024, 0),
            Err(DeviceError::InvalidPlaybackConfig {
                capacity_frames: 1024,
                preferred_buffer_frames: 0
            })
        ));
    }

    #[test]
    fn synthetic_pair_has_exact_bounded_ring_and_native_format() {
        let format = AudioFormat::new(48_000, 2, SampleFormat::I16).unwrap();
        let cancel = Arc::new(AtomicBool::new(false));
        let barge_in = BargeInHandle::new(cancel);
        let (playback, producer, _callback) = PersistentPlayback::test_pair(format, 17, barge_in);
        assert_eq!(playback.device_info().format, format);
        assert_eq!(producer.ring_format().sample_rate(), 48_000);
        assert_eq!(playback.metrics(0).ring_capacity_frames, 17);
    }
}
