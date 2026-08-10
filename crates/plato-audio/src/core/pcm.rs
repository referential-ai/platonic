use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize, de::Error as _};

use crate::PcmError;

/// The in-memory representation of one PCM sample.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SampleFormat {
    /// Signed 16-bit integer PCM.
    I16,
    /// Unsigned 16-bit integer PCM.
    U16,
    /// Normalized 32-bit floating-point PCM.
    F32,
}

/// A validated PCM stream format.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct AudioFormat {
    sample_rate: u32,
    channels: u16,
    sample: SampleFormat,
}

#[derive(Deserialize)]
struct AudioFormatFields {
    sample_rate: u32,
    channels: u16,
    sample: SampleFormat,
}

impl AudioFormat {
    /// Constructs a nonzero PCM format.
    pub fn new(sample_rate: u32, channels: u16, sample: SampleFormat) -> Result<Self, PcmError> {
        if sample_rate == 0 {
            return Err(PcmError::ZeroSampleRate);
        }
        if channels == 0 {
            return Err(PcmError::ZeroChannels);
        }
        Ok(Self {
            sample_rate,
            channels,
            sample,
        })
    }

    /// Returns samples per second for each channel.
    pub fn sample_rate(self) -> u32 {
        self.sample_rate
    }

    /// Returns the number of interleaved channels.
    pub fn channels(self) -> u16 {
        self.channels
    }

    /// Returns the sample representation.
    pub fn sample_format(self) -> SampleFormat {
        self.sample
    }
}

impl<'de> Deserialize<'de> for AudioFormat {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields = AudioFormatFields::deserialize(deserializer)?;
        Self::new(fields.sample_rate, fields.channels, fields.sample).map_err(D::Error::custom)
    }
}

/// Owned PCM samples whose Rust storage agrees with a [`SampleFormat`].
#[derive(Clone, Debug, PartialEq)]
pub enum PcmData {
    /// Signed 16-bit integer samples.
    I16(Box<[i16]>),
    /// Unsigned 16-bit integer samples.
    U16(Box<[u16]>),
    /// Normalized 32-bit floating-point samples.
    F32(Box<[f32]>),
}

impl PcmData {
    /// Returns the storage format.
    pub fn sample_format(&self) -> SampleFormat {
        match self {
            Self::I16(_) => SampleFormat::I16,
            Self::U16(_) => SampleFormat::U16,
            Self::F32(_) => SampleFormat::F32,
        }
    }

    /// Returns the number of interleaved samples.
    pub fn len(&self) -> usize {
        match self {
            Self::I16(samples) => samples.len(),
            Self::U16(samples) => samples.len(),
            Self::F32(samples) => samples.len(),
        }
    }

    /// Returns whether the sample storage is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Borrows floating-point samples when this storage uses f32.
    pub fn as_f32(&self) -> Option<&[f32]> {
        match self {
            Self::F32(samples) => Some(samples),
            Self::I16(_) | Self::U16(_) => None,
        }
    }

    fn validate(&self) -> Result<(), PcmError> {
        if let Self::F32(samples) = self
            && let Some(index) = samples.iter().position(|sample| !sample.is_finite())
        {
            return Err(PcmError::NonFiniteSample { index });
        }
        Ok(())
    }
}

/// Exactly one sample for every channel at one point in time.
#[derive(Clone, Debug, PartialEq)]
pub struct PcmFrame {
    format: AudioFormat,
    samples: PcmData,
}

impl PcmFrame {
    /// Constructs a frame and validates its storage and channel count.
    pub fn new(format: AudioFormat, samples: PcmData) -> Result<Self, PcmError> {
        validate_storage(format, &samples)?;
        if samples.len() != usize::from(format.channels()) {
            return Err(PcmError::FrameChannelMismatch {
                samples: samples.len(),
                channels: format.channels(),
            });
        }
        Ok(Self { format, samples })
    }

    /// Returns the frame format.
    pub fn format(&self) -> AudioFormat {
        self.format
    }

    /// Borrows the channel samples.
    pub fn samples(&self) -> &PcmData {
        &self.samples
    }
}

/// Owned interleaved PCM containing zero or more complete frames.
#[derive(Clone, Debug, PartialEq)]
pub struct PcmChunk {
    format: AudioFormat,
    samples: PcmData,
}

impl PcmChunk {
    /// Constructs a chunk and validates its storage and frame alignment.
    pub fn new(format: AudioFormat, samples: PcmData) -> Result<Self, PcmError> {
        validate_storage(format, &samples)?;
        if !samples.len().is_multiple_of(usize::from(format.channels())) {
            return Err(PcmError::IncompleteFrame {
                samples: samples.len(),
                channels: format.channels(),
            });
        }
        Ok(Self { format, samples })
    }

    /// Constructs normalized f32 PCM.
    pub fn from_f32(format: AudioFormat, samples: Vec<f32>) -> Result<Self, PcmError> {
        Self::new(format, PcmData::F32(samples.into_boxed_slice()))
    }

    /// Returns the chunk format.
    pub fn format(&self) -> AudioFormat {
        self.format
    }

    /// Borrows interleaved samples.
    pub fn samples(&self) -> &PcmData {
        &self.samples
    }

    /// Returns the number of complete frames.
    pub fn frame_count(&self) -> usize {
        self.samples.len() / usize::from(self.format.channels())
    }

    /// Returns the nominal PCM duration.
    pub fn duration(&self) -> Duration {
        Duration::from_secs_f64(self.frame_count() as f64 / f64::from(self.format.sample_rate()))
    }

    /// Returns whether this chunk contains no frames.
    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }
}

fn validate_storage(format: AudioFormat, samples: &PcmData) -> Result<(), PcmError> {
    let actual = samples.sample_format();
    if format.sample_format() != actual {
        return Err(PcmError::SampleFormatMismatch {
            declared: format.sample_format(),
            actual,
        });
    }
    samples.validate()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_rejects_zero_dimensions_in_code_and_serialized_input() {
        assert_eq!(
            AudioFormat::new(0, 1, SampleFormat::F32),
            Err(PcmError::ZeroSampleRate)
        );
        assert_eq!(
            AudioFormat::new(24_000, 0, SampleFormat::F32),
            Err(PcmError::ZeroChannels)
        );
        let error = serde_json::from_str::<AudioFormat>(
            r#"{"sample_rate":24000,"channels":0,"sample":"f32"}"#,
        )
        .expect_err("invalid serialized format must fail");
        assert!(error.to_string().contains("channel count"));
    }

    #[test]
    fn frame_requires_one_sample_per_channel() {
        let format = AudioFormat::new(48_000, 2, SampleFormat::I16).unwrap();
        let error = PcmFrame::new(format, PcmData::I16(vec![1].into_boxed_slice())).unwrap_err();
        assert_eq!(
            error,
            PcmError::FrameChannelMismatch {
                samples: 1,
                channels: 2,
            }
        );
    }

    #[test]
    fn chunk_validates_storage_alignment_and_finite_samples() {
        let stereo = AudioFormat::new(48_000, 2, SampleFormat::F32).unwrap();
        assert_eq!(
            PcmChunk::from_f32(stereo, vec![0.0]).unwrap_err(),
            PcmError::IncompleteFrame {
                samples: 1,
                channels: 2,
            }
        );
        assert_eq!(
            PcmChunk::from_f32(stereo, vec![0.0, f32::NAN]).unwrap_err(),
            PcmError::NonFiniteSample { index: 1 }
        );

        let mono_i16 = AudioFormat::new(24_000, 1, SampleFormat::I16).unwrap();
        assert_eq!(
            PcmChunk::new(mono_i16, PcmData::F32(vec![0.0].into_boxed_slice())).unwrap_err(),
            PcmError::SampleFormatMismatch {
                declared: SampleFormat::I16,
                actual: SampleFormat::F32,
            }
        );
    }

    #[test]
    fn chunk_reports_frames_and_duration_from_interleaved_samples() {
        let format = AudioFormat::new(24_000, 2, SampleFormat::I16).unwrap();
        let chunk =
            PcmChunk::new(format, PcmData::I16(vec![0; 48_000].into_boxed_slice())).unwrap();
        assert_eq!(chunk.frame_count(), 24_000);
        assert_eq!(chunk.duration(), Duration::from_secs(1));
    }
}
