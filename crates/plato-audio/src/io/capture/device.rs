use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Instant;

use cpal::{
    BufferSize, SampleFormat as CpalSampleFormat, Stream, StreamConfig, SupportedBufferSize,
    SupportedStreamConfig,
    traits::{DeviceTrait, HostTrait},
};
use rtrb::Producer;

use crate::{CaptureSample, DeviceBufferSize, DeviceError, SampleFormat};

use super::{
    CaptureCounters, CaptureDeviceDescriptor, InputDeviceSelection, TimedCaptureSample, bounded,
};

/// Lists real cpal input devices without selecting one or changing host policy.
pub fn capture_devices() -> Result<Vec<CaptureDeviceDescriptor>, DeviceError> {
    let host = cpal::default_host();
    let backend = host.id().name().to_owned();
    let default_id = host
        .default_input_device()
        .and_then(|device| device.id().ok())
        .map(|id| id.to_string());
    host.input_devices()
        .map_err(|error| DeviceError::InputDeviceQuery {
            reason: bounded(&error.to_string()),
        })?
        .map(|device| {
            let device_id = device
                .id()
                .map_err(|error| DeviceError::InputDeviceQuery {
                    reason: bounded(&error.to_string()),
                })?
                .to_string();
            Ok(CaptureDeviceDescriptor {
                backend: backend.clone(),
                is_default: default_id.as_deref() == Some(device_id.as_str()),
                device_id,
                device: device.to_string(),
            })
        })
        .collect()
}

pub(super) struct CallbackWriter {
    producer: Producer<TimedCaptureSample>,
    channels: usize,
    counters: Arc<CaptureCounters>,
}

impl CallbackWriter {
    pub(super) fn new(
        producer: Producer<TimedCaptureSample>,
        channels: u16,
        counters: Arc<CaptureCounters>,
    ) -> Self {
        Self {
            producer,
            channels: usize::from(channels),
            counters,
        }
    }

    pub(super) fn write<T: Copy>(&mut self, input: &[T], wrap: fn(T) -> CaptureSample) {
        let available_at = Instant::now();
        let mut dropped = 0_u64;
        for frame in input.chunks_exact(self.channels) {
            if self.producer.slots() < self.channels {
                dropped = dropped.saturating_add(self.channels as u64);
                continue;
            }
            for &sample in frame {
                self.producer
                    .push(TimedCaptureSample {
                        sample: wrap(sample),
                        available_at,
                    })
                    .expect("complete-frame capacity was checked");
            }
        }
        if dropped > 0 {
            self.counters
                .overflow_callbacks
                .fetch_add(1, Ordering::Relaxed);
            self.counters
                .overflow_samples
                .fetch_add(dropped, Ordering::Relaxed);
        }
    }
}

pub(super) fn select_device(
    host: &cpal::Host,
    selection: &InputDeviceSelection,
) -> Result<cpal::Device, DeviceError> {
    match selection {
        InputDeviceSelection::Default => host
            .default_input_device()
            .ok_or(DeviceError::NoInputDevice),
        InputDeviceSelection::Id(requested) => {
            let devices = host
                .input_devices()
                .map_err(|error| DeviceError::InputDeviceQuery {
                    reason: bounded(&error.to_string()),
                })?;
            for device in devices {
                let id = device
                    .id()
                    .map_err(|error| DeviceError::InputDeviceQuery {
                        reason: bounded(&error.to_string()),
                    })?
                    .to_string();
                if id == *requested {
                    return Ok(device);
                }
            }
            Err(DeviceError::InputDeviceNotFound {
                device_id: requested.clone(),
            })
        }
    }
}

pub(super) fn select_input_config(
    device: &cpal::Device,
) -> Result<SupportedStreamConfig, DeviceError> {
    let default = device
        .default_input_config()
        .map_err(|error| DeviceError::InputDeviceQuery {
            reason: bounded(&error.to_string()),
        })?;
    if supported_sample_format(default.sample_format()) {
        return Ok(default);
    }
    let supported = device
        .supported_input_configs()
        .map_err(|error| DeviceError::InputDeviceQuery {
            reason: bounded(&error.to_string()),
        })?
        .collect::<Vec<_>>();
    choose_supported_config(&supported).ok_or(DeviceError::UnsupportedInputFormat)
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

pub(super) fn select_buffer_size(
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

pub(super) fn build_stream(
    device: &cpal::Device,
    config: StreamConfig,
    sample_format: SampleFormat,
    callback: CallbackWriter,
    stream_failed: Arc<AtomicBool>,
) -> Result<Stream, DeviceError> {
    match sample_format {
        SampleFormat::F32 => {
            build_typed_stream(device, config, callback, stream_failed, CaptureSample::F32)
        }
        SampleFormat::I16 => {
            build_typed_stream(device, config, callback, stream_failed, CaptureSample::I16)
        }
        SampleFormat::U16 => {
            build_typed_stream(device, config, callback, stream_failed, CaptureSample::U16)
        }
    }
}

fn build_typed_stream<T: cpal::SizedSample + Copy + 'static>(
    device: &cpal::Device,
    config: StreamConfig,
    mut callback: CallbackWriter,
    stream_failed: Arc<AtomicBool>,
    wrap: fn(T) -> CaptureSample,
) -> Result<Stream, DeviceError> {
    device
        .build_input_stream(
            config,
            move |input: &[T], _| callback.write(input, wrap),
            move |_| stream_failed.store(true, Ordering::Release),
            None,
        )
        .map_err(|error| DeviceError::InputStreamBuild {
            reason: bounded(&error.to_string()),
        })
}

fn supported_sample_format(sample_format: CpalSampleFormat) -> bool {
    matches!(
        sample_format,
        CpalSampleFormat::F32 | CpalSampleFormat::I16 | CpalSampleFormat::U16
    )
}

pub(super) fn sample_format(sample_format: CpalSampleFormat) -> SampleFormat {
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
    use rtrb::RingBuffer;

    use crate::CaptureOverflow;

    #[test]
    fn callback_copies_native_samples_and_drops_only_complete_frames() {
        let counters = Arc::new(CaptureCounters::default());
        let (producer, mut consumer) = RingBuffer::new(4);
        let mut callback = CallbackWriter::new(producer, 2, Arc::clone(&counters));
        let before = Instant::now();
        callback.write(&[1_i16, 2, 3, 4, 5, 6], CaptureSample::I16);
        let after = Instant::now();
        let copied = (0..4).map(|_| consumer.pop().unwrap()).collect::<Vec<_>>();
        assert_eq!(
            copied
                .iter()
                .map(|sample| sample.sample)
                .collect::<Vec<_>>(),
            [
                CaptureSample::I16(1),
                CaptureSample::I16(2),
                CaptureSample::I16(3),
                CaptureSample::I16(4),
            ]
        );
        assert!(
            copied
                .iter()
                .all(|sample| sample.available_at >= before && sample.available_at <= after)
        );
        assert!(
            copied
                .iter()
                .all(|sample| sample.available_at == copied[0].available_at)
        );
        assert!(consumer.pop().is_err());
        assert_eq!(
            counters.overflow(),
            CaptureOverflow {
                callbacks: 1,
                samples: 2,
            }
        );
    }

    #[test]
    fn input_format_fallback_prefers_f32_and_fewer_channels() {
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
    fn input_period_is_clamped_to_the_device_range() {
        assert_eq!(
            select_buffer_size(
                &SupportedBufferSize::Range {
                    min: 512,
                    max: 1_024,
                },
                256,
            ),
            (
                DeviceBufferSize::Fixed {
                    requested_frames: 512,
                    supported_min_frames: 512,
                    supported_max_frames: 1_024,
                },
                BufferSize::Fixed(512),
            )
        );
        assert_eq!(
            select_buffer_size(&SupportedBufferSize::Unknown, 256),
            (DeviceBufferSize::DefaultUnknown, BufferSize::Default)
        );
    }
}
