use serde::Serialize;

#[cfg(test)]
use crate::CaptureError;
use crate::VadError;

mod neural;

pub use neural::{
    NeuralVadEvent, NeuralVadState, SILERO_HANGOVER_FRAMES, SILERO_MINIMUM_SPEECH_FRAMES,
    SILERO_ONSET_FRAMES, SILERO_SPEECH_THRESHOLD, SILERO_WINDOW_SAMPLES,
};

/// Whisper input rate and the shared endpoint sample clock.
pub const CAPTURE_SAMPLE_RATE: u32 = 16_000;
/// Samples in each literal 10 ms RMS window.
pub const VAD_WINDOW_SAMPLES: usize = 160;
/// Literal full-scale RMS threshold selected for AU3 fixtures.
pub const VAD_RMS_THRESHOLD: f32 = 0.015;
/// Consecutive above-threshold windows required for onset.
pub const VAD_ONSET_WINDOWS: u16 = 3;
/// Above-threshold windows required to retain an utterance.
pub const VAD_MINIMUM_SPEECH_WINDOWS: u16 = 20;
/// Consecutive quiet windows required to close an utterance.
pub const VAD_HANGOVER_WINDOWS: u16 = 25;
/// Fixed utterance memory bound shared by threshold fixtures and neural endpointing.
pub const MAX_UTTERANCE_MS: u64 = 30_000;

#[cfg(test)]
const MAX_UTTERANCE_SAMPLES: usize =
    (CAPTURE_SAMPLE_RATE as usize * MAX_UTTERANCE_MS as usize) / 1_000;

/// One warm frame-level voice activity inference engine.
pub trait VoiceActivityDetector: Send {
    /// Returns the fixed number of 16 kHz mono samples accepted per inference.
    fn frame_samples(&self) -> usize;

    /// Clears recurrent utterance state without reloading the model session.
    fn reset(&mut self);

    /// Returns a speech probability for one exact-size normalized frame.
    fn speech_probability(&mut self, samples: &[f32]) -> Result<f32, VadError>;
}

/// Exact sample boundaries produced by an endpoint detector.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct VadEndpoint {
    /// First sample of the onset candidate, in the worker's 16 kHz clock.
    pub start_sample: u64,
    /// Exclusive end of the last above-threshold RMS window.
    pub speech_end_sample: u64,
    /// Exclusive end of the hangover window that closed the utterance.
    pub close_sample: u64,
}

/// One bounded 16 kHz mono segment retained after VAD hysteresis.
#[derive(Clone, Debug, PartialEq)]
pub struct VoiceSegment {
    samples: Vec<f32>,
    endpoint: VadEndpoint,
}

impl VoiceSegment {
    /// Borrows normalized mono samples, including the fixed hangover tail.
    pub fn samples(&self) -> &[f32] {
        &self.samples
    }

    /// Returns exact onset, last-speech, and close sample boundaries.
    pub fn endpoint(&self) -> VadEndpoint {
        self.endpoint
    }

    /// Returns onset-to-close duration in whole milliseconds.
    pub fn span_ms(&self) -> u64 {
        self.endpoint
            .close_sample
            .saturating_sub(self.endpoint.start_sample)
            .saturating_mul(1_000)
            / u64::from(CAPTURE_SAMPLE_RATE)
    }
}

#[cfg(test)]
pub(crate) enum VadEvent {
    Segment(VoiceSegment),
    RejectedTransient(VadEndpoint),
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum VadState {
    #[default]
    Silence,
    Candidate {
        start_sample: u64,
        above_windows: u16,
    },
    Speech {
        start_sample: u64,
        voiced_windows: u16,
        quiet_windows: u16,
        speech_end_sample: u64,
    },
}

#[cfg(test)]
pub(crate) struct ThresholdVad {
    state: VadState,
    window: [f32; VAD_WINDOW_SAMPLES],
    window_len: usize,
    segment: Vec<f32>,
    processed_samples: u64,
}

#[cfg(test)]
impl ThresholdVad {
    pub(crate) fn new() -> Self {
        Self {
            state: VadState::Silence,
            window: [0.0; VAD_WINDOW_SAMPLES],
            window_len: 0,
            segment: Vec::with_capacity(MAX_UTTERANCE_SAMPLES),
            processed_samples: 0,
        }
    }

    pub(crate) fn push(&mut self, samples: &[f32]) -> Result<Vec<VadEvent>, CaptureError> {
        let mut events = Vec::new();
        for &sample in samples {
            self.window[self.window_len] = sample;
            self.window_len += 1;
            if self.window_len == VAD_WINDOW_SAMPLES {
                if let Some(event) = self.process_window()? {
                    events.push(event);
                }
                self.window_len = 0;
            }
        }
        Ok(events)
    }

    fn process_window(&mut self) -> Result<Option<VadEvent>, CaptureError> {
        let start = self.processed_samples;
        let end = start.saturating_add(VAD_WINDOW_SAMPLES as u64);
        self.processed_samples = end;
        let above_threshold = rms(&self.window) >= VAD_RMS_THRESHOLD;

        match self.state {
            VadState::Silence if above_threshold => {
                self.segment.clear();
                self.segment.extend_from_slice(&self.window);
                self.state = VadState::Candidate {
                    start_sample: start,
                    above_windows: 1,
                };
                Ok(None)
            }
            VadState::Silence => Ok(None),
            VadState::Candidate {
                start_sample,
                above_windows,
            } if above_threshold => {
                self.segment.extend_from_slice(&self.window);
                let above_windows = above_windows.saturating_add(1);
                if above_windows >= VAD_ONSET_WINDOWS {
                    self.state = VadState::Speech {
                        start_sample,
                        voiced_windows: above_windows,
                        quiet_windows: 0,
                        speech_end_sample: end,
                    };
                } else {
                    self.state = VadState::Candidate {
                        start_sample,
                        above_windows,
                    };
                }
                Ok(None)
            }
            VadState::Candidate { .. } => {
                self.segment.clear();
                self.state = VadState::Silence;
                Ok(None)
            }
            VadState::Speech {
                start_sample,
                voiced_windows,
                quiet_windows: _,
                speech_end_sample: _,
            } if above_threshold => {
                self.append_speech_window()?;
                self.state = VadState::Speech {
                    start_sample,
                    voiced_windows: voiced_windows.saturating_add(1),
                    quiet_windows: 0,
                    speech_end_sample: end,
                };
                Ok(None)
            }
            VadState::Speech {
                start_sample,
                voiced_windows,
                quiet_windows,
                speech_end_sample,
            } => {
                self.append_speech_window()?;
                let quiet_windows = quiet_windows.saturating_add(1);
                if quiet_windows < VAD_HANGOVER_WINDOWS {
                    self.state = VadState::Speech {
                        start_sample,
                        voiced_windows,
                        quiet_windows,
                        speech_end_sample,
                    };
                    return Ok(None);
                }

                let endpoint = VadEndpoint {
                    start_sample,
                    speech_end_sample,
                    close_sample: end,
                };
                self.state = VadState::Silence;
                if voiced_windows < VAD_MINIMUM_SPEECH_WINDOWS {
                    self.segment.clear();
                    return Ok(Some(VadEvent::RejectedTransient(endpoint)));
                }
                let samples = std::mem::take(&mut self.segment);
                Ok(Some(VadEvent::Segment(VoiceSegment { samples, endpoint })))
            }
        }
    }

    fn append_speech_window(&mut self) -> Result<(), CaptureError> {
        if self.segment.len().saturating_add(VAD_WINDOW_SAMPLES) > MAX_UTTERANCE_SAMPLES {
            self.segment.clear();
            self.state = VadState::Silence;
            return Err(CaptureError::UtteranceTooLong {
                maximum_ms: MAX_UTTERANCE_MS,
            });
        }
        self.segment.extend_from_slice(&self.window);
        Ok(())
    }
}

#[cfg(test)]
fn rms(samples: &[f32]) -> f32 {
    let mean_square = samples
        .iter()
        .map(|sample| f64::from(*sample) * f64::from(*sample))
        .sum::<f64>()
        / samples.len() as f64;
    mean_square.sqrt() as f32
}

#[cfg(test)]
mod tests {
    use super::*;

    fn windows(count: usize, amplitude: f32) -> Vec<f32> {
        vec![amplitude; count * VAD_WINDOW_SAMPLES]
    }

    fn segment(events: Vec<VadEvent>) -> VoiceSegment {
        assert_eq!(events.len(), 1);
        match events.into_iter().next().unwrap() {
            VadEvent::Segment(segment) => segment,
            VadEvent::RejectedTransient(_) => panic!("expected retained segment"),
        }
    }

    #[test]
    fn silence_and_steady_room_tone_never_open_vad() {
        let mut vad = ThresholdVad::new();
        assert!(vad.push(&windows(400, 0.0)).unwrap().is_empty());
        assert!(vad.push(&windows(400, 0.005)).unwrap().is_empty());
    }

    #[test]
    fn onset_requires_three_consecutive_windows() {
        let mut vad = ThresholdVad::new();
        assert!(vad.push(&windows(2, 0.05)).unwrap().is_empty());
        assert!(vad.push(&windows(1, 0.0)).unwrap().is_empty());
        assert!(vad.push(&windows(2, 0.05)).unwrap().is_empty());
        assert!(vad.push(&windows(25, 0.0)).unwrap().is_empty());
    }

    #[test]
    fn minimum_speech_rejects_an_onset_qualified_transient() {
        let mut vad = ThresholdVad::new();
        assert!(vad.push(&windows(19, 0.05)).unwrap().is_empty());
        let events = vad.push(&windows(25, 0.0)).unwrap();
        assert!(matches!(
            events.as_slice(),
            [VadEvent::RejectedTransient(VadEndpoint {
                start_sample: 0,
                speech_end_sample: 3_040,
                close_sample: 7_040,
            })]
        ));
    }

    #[test]
    fn minimum_speech_and_hangover_emit_exact_boundaries() {
        let mut vad = ThresholdVad::new();
        assert!(vad.push(&windows(20, 0.05)).unwrap().is_empty());
        assert!(vad.push(&windows(24, 0.0)).unwrap().is_empty());
        let retained = segment(vad.push(&windows(1, 0.0)).unwrap());
        assert_eq!(
            retained.endpoint(),
            VadEndpoint {
                start_sample: 0,
                speech_end_sample: 3_200,
                close_sample: 7_200,
            }
        );
        assert_eq!(retained.samples().len(), 7_200);
        assert_eq!(retained.span_ms(), 450);
    }

    #[test]
    fn sub_hangover_pause_does_not_split_speech() {
        let mut vad = ThresholdVad::new();
        vad.push(&windows(20, 0.05)).unwrap();
        vad.push(&windows(24, 0.0)).unwrap();
        vad.push(&windows(4, 0.05)).unwrap();
        let retained = segment(vad.push(&windows(25, 0.0)).unwrap());
        assert_eq!(retained.endpoint().start_sample, 0);
        assert_eq!(retained.endpoint().speech_end_sample, 7_680);
        assert_eq!(retained.endpoint().close_sample, 11_680);
    }
}
