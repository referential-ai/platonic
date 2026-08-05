use rubato::{Fft, FixedSync, Resampler, audioadapter_buffers::direct::InterleavedSlice};
use serde::Serialize;

use crate::{AudioFormat, CaptureError, ResampleError, SampleFormat};

use super::vad::CAPTURE_SAMPLE_RATE;

const CAPTURE_RESAMPLE_CHUNK_FRAMES: usize = 480;
const MAX_DIAGNOSTIC_CHARS: usize = 2_048;

/// One native input sample copied verbatim through the real-time ring.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum CaptureSample {
    /// Signed 16-bit device sample.
    I16(i16),
    /// Unsigned 16-bit device sample.
    U16(u16),
    /// Native 32-bit floating-point device sample.
    F32(f32),
}

impl CaptureSample {
    pub(crate) fn sample_format(self) -> SampleFormat {
        match self {
            Self::I16(_) => SampleFormat::I16,
            Self::U16(_) => SampleFormat::U16,
            Self::F32(_) => SampleFormat::F32,
        }
    }
}

/// Exact worker-side conversion accounting for one drained ring batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct CaptureResampleReport {
    /// Complete native device frames consumed.
    pub input_frames: usize,
    /// Mono 16 kHz frames emitted to VAD.
    pub output_frames: usize,
    /// Negotiated native input sample rate.
    pub input_rate: u32,
    /// Fixed Whisper/VAD sample rate.
    pub output_rate: u32,
}

pub(crate) struct CaptureNormalizer {
    input_format: AudioFormat,
    channel_sum: f64,
    channel_index: u16,
    resampler: StreamingResampler,
}

impl CaptureNormalizer {
    pub(crate) fn new(input_format: AudioFormat) -> Result<Self, CaptureError> {
        Ok(Self {
            input_format,
            channel_sum: 0.0,
            channel_index: 0,
            resampler: StreamingResampler::new(input_format.sample_rate())?,
        })
    }

    pub(crate) fn push(
        &mut self,
        samples: &[CaptureSample],
    ) -> Result<(Vec<f32>, CaptureResampleReport), CaptureError> {
        let channels = self.input_format.channels();
        let mut mono = Vec::with_capacity(samples.len() / usize::from(channels) + 1);
        for &sample in samples {
            let actual = sample.sample_format();
            if actual != self.input_format.sample_format() {
                return Err(CaptureError::SampleFormatMismatch {
                    expected: self.input_format.sample_format(),
                    actual,
                });
            }
            self.channel_sum += f64::from(normalize(sample)?);
            self.channel_index += 1;
            if self.channel_index == channels {
                mono.push((self.channel_sum / f64::from(channels)).clamp(-1.0, 1.0) as f32);
                self.channel_sum = 0.0;
                self.channel_index = 0;
            }
        }

        let input_frames = mono.len();
        let output = self.resampler.push(&mono)?;
        let output_frames = output.len();
        Ok((
            output,
            CaptureResampleReport {
                input_frames,
                output_frames,
                input_rate: self.input_format.sample_rate(),
                output_rate: CAPTURE_SAMPLE_RATE,
            },
        ))
    }
}

enum StreamingResampler {
    Identity,
    Rubato {
        resampler: Box<Fft<f32>>,
        pending: Vec<f32>,
    },
}

impl StreamingResampler {
    fn new(input_rate: u32) -> Result<Self, ResampleError> {
        if input_rate == CAPTURE_SAMPLE_RATE {
            return Ok(Self::Identity);
        }
        let source_format = AudioFormat::new(input_rate, 1, SampleFormat::F32)?;
        let device_format = AudioFormat::new(CAPTURE_SAMPLE_RATE, 1, SampleFormat::F32)?;
        let resampler = Fft::<f32>::new(
            input_rate as usize,
            CAPTURE_SAMPLE_RATE as usize,
            CAPTURE_RESAMPLE_CHUNK_FRAMES,
            1,
            FixedSync::Input,
        )
        .map_err(|error| ResampleError::PlanConstruction {
            source_format,
            device_format,
            reason: bounded(&error.to_string()),
        })?;
        Ok(Self::Rubato {
            resampler: Box::new(resampler),
            pending: Vec::with_capacity(CAPTURE_RESAMPLE_CHUNK_FRAMES * 2),
        })
    }

    fn push(&mut self, mono: &[f32]) -> Result<Vec<f32>, ResampleError> {
        match self {
            Self::Identity => Ok(mono.to_vec()),
            Self::Rubato { resampler, pending } => {
                pending.extend_from_slice(mono);
                let mut output = Vec::new();
                loop {
                    let required = resampler.input_frames_next();
                    if pending.len() < required {
                        break;
                    }
                    let chunk = {
                        let adapter = InterleavedSlice::new(&pending[..required], 1, required)
                            .map_err(|error| ResampleError::Processing {
                                reason: bounded(&error.to_string()),
                            })?;
                        resampler
                            .process(&adapter, None)
                            .map_err(|error| ResampleError::Processing {
                                reason: bounded(&error.to_string()),
                            })?
                            .take_data()
                    };
                    pending.drain(..required);
                    output.extend(chunk);
                }
                Ok(output)
            }
        }
    }
}

fn normalize(sample: CaptureSample) -> Result<f32, CaptureError> {
    match sample {
        CaptureSample::I16(sample) => Ok(f32::from(sample) / 32_768.0),
        CaptureSample::U16(sample) => Ok((f32::from(sample) - 32_768.0) / 32_768.0),
        CaptureSample::F32(sample) if sample.is_finite() => Ok(sample.clamp(-1.0, 1.0)),
        CaptureSample::F32(_) => Err(CaptureError::NonFiniteInput),
    }
}

fn bounded(value: &str) -> String {
    value.chars().take(MAX_DIAGNOSTIC_CHARS).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn format(rate: u32, channels: u16, sample: SampleFormat) -> AudioFormat {
        AudioFormat::new(rate, channels, sample).unwrap()
    }

    #[test]
    fn native_sample_normalization_uses_exact_full_scale_mapping() {
        assert_eq!(normalize(CaptureSample::I16(i16::MIN)).unwrap(), -1.0);
        assert_eq!(normalize(CaptureSample::I16(0)).unwrap(), 0.0);
        assert_eq!(normalize(CaptureSample::U16(0)).unwrap(), -1.0);
        assert_eq!(normalize(CaptureSample::U16(32_768)).unwrap(), 0.0);
        assert_eq!(normalize(CaptureSample::F32(1.5)).unwrap(), 1.0);
        assert!(matches!(
            normalize(CaptureSample::F32(f32::NAN)),
            Err(CaptureError::NonFiniteInput)
        ));
    }

    #[test]
    fn stereo_downmix_carries_complete_frames_across_batches() {
        let mut normalizer = CaptureNormalizer::new(format(16_000, 2, SampleFormat::I16)).unwrap();
        let (first, first_report) = normalizer.push(&[CaptureSample::I16(i16::MAX)]).unwrap();
        assert!(first.is_empty());
        assert_eq!(first_report.input_frames, 0);

        let (second, second_report) = normalizer
            .push(&[
                CaptureSample::I16(i16::MIN),
                CaptureSample::I16(16_384),
                CaptureSample::I16(16_384),
            ])
            .unwrap();
        assert_eq!(second.len(), 2);
        assert!((second[0] - (-1.0 / 65_536.0)).abs() < 1.0e-6);
        assert_eq!(second[1], 0.5);
        assert_eq!(second_report.input_frames, 2);
        assert_eq!(second_report.output_frames, 2);
    }

    #[test]
    fn forty_eight_khz_input_is_streamed_to_sixteen_khz() {
        let mut normalizer = CaptureNormalizer::new(format(48_000, 1, SampleFormat::F32)).unwrap();
        let mut output = Vec::new();
        let mut input_frames = 0;
        for offset in (0..48_000).step_by(257) {
            let end = (offset + 257).min(48_000);
            let samples = (offset..end)
                .map(|frame| {
                    CaptureSample::F32(
                        (2.0 * std::f32::consts::PI * 1_000.0 * frame as f32 / 48_000.0).sin()
                            * 0.2,
                    )
                })
                .collect::<Vec<_>>();
            let (chunk, report) = normalizer.push(&samples).unwrap();
            input_frames += report.input_frames;
            output.extend(chunk);
        }

        assert_eq!(input_frames, 48_000);
        assert!(output.len() >= 15_500 && output.len() <= 16_500);
        assert!(output.iter().all(|sample| sample.is_finite()));
        assert!(output.iter().any(|sample| sample.abs() > 0.1));
    }

    #[test]
    fn negotiated_sample_format_cannot_drift_in_the_ring() {
        let mut normalizer = CaptureNormalizer::new(format(16_000, 1, SampleFormat::I16)).unwrap();
        assert!(matches!(
            normalizer.push(&[CaptureSample::F32(0.0)]),
            Err(CaptureError::SampleFormatMismatch {
                expected: SampleFormat::I16,
                actual: SampleFormat::F32,
            })
        ));
    }
}
