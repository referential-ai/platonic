use rubato::{Fft, FixedSync, Resampler, audioadapter_buffers::direct::InterleavedSlice};
use serde::Serialize;

use crate::{AudioFormat, PcmChunk, PcmData, ResampleError, SampleFormat};

const RESAMPLE_CHUNK_FRAMES: usize = 1_024;
const MAX_DIAGNOSTIC_CHARS: usize = 2_048;

/// Exact fixed-rate conversion implementation used by AU2.
pub const RUBATO_RUNTIME_VERSION: &str = "rubato 4.0.0";

/// Exact frame accounting for one use of a resident sample-rate plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct ResampleReport {
    /// Mono source frames supplied by the synthesizer.
    pub source_frames: usize,
    /// Mono device-rate frames produced before ring handoff.
    pub device_frames: usize,
    /// Source sample rate captured by the plan.
    pub source_rate: u32,
    /// Device sample rate captured by the plan.
    pub device_rate: u32,
}

enum PlanKind {
    Identity,
    Rubato(Box<Fft<f32>>),
}

/// One reusable conversion plan for an exact synthesis/device format pair.
pub struct ResamplingPlan {
    source_format: AudioFormat,
    device_format: AudioFormat,
    ring_format: AudioFormat,
    kind: PlanKind,
}

impl ResamplingPlan {
    /// Builds the only plan used for this source and live output device.
    pub fn new(
        source_format: AudioFormat,
        device_format: AudioFormat,
    ) -> Result<Self, ResampleError> {
        if source_format.channels() != 1 || source_format.sample_format() != SampleFormat::F32 {
            return Err(ResampleError::UnsupportedSource {
                actual: source_format,
            });
        }
        let ring_format = AudioFormat::new(device_format.sample_rate(), 1, SampleFormat::F32)?;
        let kind = if source_format.sample_rate() == device_format.sample_rate() {
            PlanKind::Identity
        } else {
            let resampler = Fft::<f32>::new(
                source_format.sample_rate() as usize,
                device_format.sample_rate() as usize,
                RESAMPLE_CHUNK_FRAMES,
                1,
                FixedSync::Input,
            )
            .map_err(|error| ResampleError::PlanConstruction {
                source_format,
                device_format,
                reason: bounded(&error.to_string()),
            })?;
            PlanKind::Rubato(Box::new(resampler))
        };
        Ok(Self {
            source_format,
            device_format,
            ring_format,
            kind,
        })
    }

    /// Returns the synthesis format captured during construction.
    pub fn source_format(&self) -> AudioFormat {
        self.source_format
    }

    /// Returns the exact live-device format captured during construction.
    pub fn device_format(&self) -> AudioFormat {
        self.device_format
    }

    /// Returns mono f32 at the device sample rate, the PCM ring contract.
    pub fn ring_format(&self) -> AudioFormat {
        self.ring_format
    }

    /// Converts one complete synthesis chunk with the resident plan.
    pub fn process(
        &mut self,
        chunk: &PcmChunk,
    ) -> Result<(PcmChunk, ResampleReport), ResampleError> {
        if chunk.format() != self.source_format {
            return Err(ResampleError::FormatMismatch {
                expected: self.source_format,
                actual: chunk.format(),
            });
        }
        let source_frames = chunk.frame_count();
        let input = chunk.samples().as_f32().expect("source contract is f32");
        let output = match &mut self.kind {
            PlanKind::Identity => input.to_vec(),
            PlanKind::Rubato(resampler) => {
                let adapter = InterleavedSlice::new(input, 1, source_frames).map_err(|error| {
                    ResampleError::Processing {
                        reason: bounded(&error.to_string()),
                    }
                })?;
                resampler
                    .process_all(&adapter, source_frames, None)
                    .map_err(|error| ResampleError::Processing {
                        reason: bounded(&error.to_string()),
                    })?
                    .take_data()
            }
        };
        let device_frames = output.len();
        let output = PcmChunk::new(self.ring_format, PcmData::F32(output.into_boxed_slice()))?;
        Ok((
            output,
            ResampleReport {
                source_frames,
                device_frames,
                source_rate: self.source_format.sample_rate(),
                device_rate: self.device_format.sample_rate(),
            },
        ))
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
    fn identity_plan_preserves_exact_samples_and_accounting() {
        let source = format(24_000, 1, SampleFormat::F32);
        let device = format(24_000, 2, SampleFormat::I16);
        let mut plan = ResamplingPlan::new(source, device).unwrap();
        let input = PcmChunk::from_f32(source, vec![0.0, 0.25, -0.5, 1.0]).unwrap();
        let (output, report) = plan.process(&input).unwrap();
        assert_eq!(output.format(), format(24_000, 1, SampleFormat::F32));
        assert_eq!(output.samples().as_f32().unwrap(), [0.0, 0.25, -0.5, 1.0]);
        assert_eq!(
            report,
            ResampleReport {
                source_frames: 4,
                device_frames: 4,
                source_rate: 24_000,
                device_rate: 24_000,
            }
        );
    }

    #[test]
    fn resident_plan_reuses_exact_rate_and_length_accounting() {
        let source = format(24_000, 1, SampleFormat::F32);
        let device = format(48_000, 2, SampleFormat::F32);
        let mut plan = ResamplingPlan::new(source, device).unwrap();

        for frames in [1_000, 2_401] {
            let samples = (0..frames)
                .map(|frame| (frame as f32 * 0.01).sin())
                .collect::<Vec<_>>();
            let input = PcmChunk::from_f32(source, samples).unwrap();
            let (output, report) = plan.process(&input).unwrap();
            assert_eq!(report.source_frames, frames);
            assert_eq!(report.device_frames, frames * 2);
            assert_eq!(output.frame_count(), frames * 2);
            assert!(
                output
                    .samples()
                    .as_f32()
                    .unwrap()
                    .iter()
                    .all(|sample| sample.is_finite())
            );
            assert!(
                output
                    .samples()
                    .as_f32()
                    .unwrap()
                    .iter()
                    .any(|sample| sample.abs() > 1.0e-6)
            );
        }
    }

    #[test]
    fn plan_rejects_wrong_storage_and_later_format_drift() {
        let invalid = format(24_000, 2, SampleFormat::F32);
        let device = format(48_000, 2, SampleFormat::F32);
        assert!(matches!(
            ResamplingPlan::new(invalid, device),
            Err(ResampleError::UnsupportedSource { actual }) if actual == invalid
        ));

        let source = format(24_000, 1, SampleFormat::F32);
        let mut plan = ResamplingPlan::new(source, device).unwrap();
        let drifted = PcmChunk::from_f32(format(44_100, 1, SampleFormat::F32), vec![0.5]).unwrap();
        assert!(matches!(
            plan.process(&drifted),
            Err(ResampleError::FormatMismatch { expected, actual })
                if expected == source && actual == drifted.format()
        ));
    }
}
