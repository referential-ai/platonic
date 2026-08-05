use crate::{VadError, VoiceActivityDetector};

use super::{CAPTURE_SAMPLE_RATE, MAX_UTTERANCE_MS, VadEndpoint, VoiceSegment};

/// Fixed 32 ms input required by Silero VAD at 16 kHz.
pub const SILERO_WINDOW_SAMPLES: usize = 512;
/// Admitted probability threshold for one speech-positive Silero frame.
pub const SILERO_SPEECH_THRESHOLD: f32 = 0.5;
/// Consecutive speech-positive frames required to open an onset candidate.
pub const SILERO_ONSET_FRAMES: u16 = 1;
/// Speech-positive frames required before PCM reaches Whisper.
pub const SILERO_MINIMUM_SPEECH_FRAMES: u16 = 4;
/// Consecutive speech-negative frames required to close an utterance.
pub const SILERO_HANGOVER_FRAMES: u16 = 8;

const MAX_UTTERANCE_SAMPLES: usize =
    (CAPTURE_SAMPLE_RATE as usize * MAX_UTTERANCE_MS as usize) / 1_000;

/// Typed streaming and terminal events from the pure Silero endpoint state.
pub enum NeuralVadEvent {
    /// Minimum-speech-qualified onset decision before any recognition work.
    SpeechOnset {
        /// First sample of the retained onset candidate.
        start_sample: u64,
        /// Exclusive sample position of the frame that qualified speech.
        decision_sample: u64,
    },
    /// Newly gated PCM that must reach rolling recognition exactly once.
    SpeechSamples(Box<[f32]>),
    /// One minimum-speech-qualified utterance closed by the fixed hangover.
    Segment(VoiceSegment),
    /// An onset candidate that closed before the fixed minimum speech span.
    RejectedTransient(VadEndpoint),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum EndpointState {
    #[default]
    Silence,
    Candidate {
        start_sample: u64,
        onset_frames: u16,
    },
    Speech {
        start_sample: u64,
        voiced_frames: u16,
        quiet_frames: u16,
        speech_end_sample: u64,
        announced: bool,
    },
}

/// Bounded pure endpoint state driven by one warm probability detector.
pub struct NeuralVadState {
    state: EndpointState,
    frame: [f32; SILERO_WINDOW_SAMPLES],
    frame_len: usize,
    segment: Vec<f32>,
    processed_samples: u64,
}

impl NeuralVadState {
    /// Constructs state for the admitted fixed Silero frame length.
    pub fn new(detector_frame_samples: usize) -> Result<Self, VadError> {
        if detector_frame_samples != SILERO_WINDOW_SAMPLES {
            return Err(VadError::FrameLength {
                expected: SILERO_WINDOW_SAMPLES,
                actual: detector_frame_samples,
            });
        }
        Ok(Self {
            state: EndpointState::Silence,
            frame: [0.0; SILERO_WINDOW_SAMPLES],
            frame_len: 0,
            segment: Vec::with_capacity(MAX_UTTERANCE_SAMPLES),
            processed_samples: 0,
        })
    }

    /// Evaluates complete frames and returns ordered rolling or endpoint events.
    pub fn push(
        &mut self,
        samples: &[f32],
        detector: &mut dyn VoiceActivityDetector,
    ) -> Result<Vec<NeuralVadEvent>, VadError> {
        let mut events = Vec::new();
        for &sample in samples {
            self.frame[self.frame_len] = sample;
            self.frame_len += 1;
            if self.frame_len == SILERO_WINDOW_SAMPLES {
                let probability = detector.speech_probability(&self.frame)?;
                if !probability.is_finite() || !(0.0..=1.0).contains(&probability) {
                    return Err(VadError::InvalidProbability { probability });
                }
                self.process_frame(probability >= SILERO_SPEECH_THRESHOLD, &mut events)?;
                self.frame_len = 0;
            }
        }
        Ok(events)
    }

    fn process_frame(
        &mut self,
        speech_positive: bool,
        events: &mut Vec<NeuralVadEvent>,
    ) -> Result<(), VadError> {
        let start = self.processed_samples;
        let end = start.saturating_add(SILERO_WINDOW_SAMPLES as u64);
        self.processed_samples = end;

        match self.state {
            EndpointState::Silence if speech_positive => {
                self.segment.clear();
                self.append_frame()?;
                if SILERO_ONSET_FRAMES == 1 {
                    self.state = EndpointState::Speech {
                        start_sample: start,
                        voiced_frames: 1,
                        quiet_frames: 0,
                        speech_end_sample: end,
                        announced: false,
                    };
                } else {
                    self.state = EndpointState::Candidate {
                        start_sample: start,
                        onset_frames: 1,
                    };
                }
            }
            EndpointState::Silence => {}
            EndpointState::Candidate {
                start_sample,
                onset_frames,
            } if speech_positive => {
                self.append_frame()?;
                let onset_frames = onset_frames.saturating_add(1);
                if onset_frames >= SILERO_ONSET_FRAMES {
                    self.state = EndpointState::Speech {
                        start_sample,
                        voiced_frames: onset_frames,
                        quiet_frames: 0,
                        speech_end_sample: end,
                        announced: false,
                    };
                } else {
                    self.state = EndpointState::Candidate {
                        start_sample,
                        onset_frames,
                    };
                }
            }
            EndpointState::Candidate { .. } => {
                self.segment.clear();
                self.state = EndpointState::Silence;
            }
            EndpointState::Speech {
                start_sample,
                voiced_frames,
                quiet_frames: _,
                speech_end_sample: _,
                announced,
            } if speech_positive => {
                self.append_frame()?;
                let voiced_frames = voiced_frames.saturating_add(1);
                let now_announced = announced || voiced_frames >= SILERO_MINIMUM_SPEECH_FRAMES;
                self.emit_progress(start_sample, end, announced, now_announced, events);
                self.state = EndpointState::Speech {
                    start_sample,
                    voiced_frames,
                    quiet_frames: 0,
                    speech_end_sample: end,
                    announced: now_announced,
                };
            }
            EndpointState::Speech {
                start_sample,
                voiced_frames,
                quiet_frames,
                speech_end_sample,
                announced,
            } => {
                self.append_frame()?;
                let quiet_frames = quiet_frames.saturating_add(1);
                if announced {
                    events.push(NeuralVadEvent::SpeechSamples(
                        self.frame.to_vec().into_boxed_slice(),
                    ));
                }
                if quiet_frames < SILERO_HANGOVER_FRAMES {
                    self.state = EndpointState::Speech {
                        start_sample,
                        voiced_frames,
                        quiet_frames,
                        speech_end_sample,
                        announced,
                    };
                    return Ok(());
                }

                let endpoint = VadEndpoint {
                    start_sample,
                    speech_end_sample,
                    close_sample: end,
                };
                self.state = EndpointState::Silence;
                if !announced {
                    self.segment.clear();
                    events.push(NeuralVadEvent::RejectedTransient(endpoint));
                } else {
                    let samples = std::mem::take(&mut self.segment);
                    events.push(NeuralVadEvent::Segment(VoiceSegment { samples, endpoint }));
                }
            }
        }
        Ok(())
    }

    fn emit_progress(
        &self,
        start_sample: u64,
        decision_sample: u64,
        was_announced: bool,
        now_announced: bool,
        events: &mut Vec<NeuralVadEvent>,
    ) {
        if !was_announced && now_announced {
            events.push(NeuralVadEvent::SpeechOnset {
                start_sample,
                decision_sample,
            });
            events.push(NeuralVadEvent::SpeechSamples(
                self.segment.clone().into_boxed_slice(),
            ));
        } else if was_announced {
            events.push(NeuralVadEvent::SpeechSamples(
                self.frame.to_vec().into_boxed_slice(),
            ));
        }
    }

    fn append_frame(&mut self) -> Result<(), VadError> {
        if self.segment.len().saturating_add(SILERO_WINDOW_SAMPLES) > MAX_UTTERANCE_SAMPLES {
            self.segment.clear();
            self.state = EndpointState::Silence;
            return Err(VadError::UtteranceTooLong {
                maximum_ms: MAX_UTTERANCE_MS,
            });
        }
        self.segment.extend_from_slice(&self.frame);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct ProbabilitySequence {
        values: std::vec::IntoIter<f32>,
    }

    impl VoiceActivityDetector for ProbabilitySequence {
        fn frame_samples(&self) -> usize {
            SILERO_WINDOW_SAMPLES
        }

        fn reset(&mut self) {}

        fn speech_probability(&mut self, samples: &[f32]) -> Result<f32, VadError> {
            assert_eq!(samples.len(), SILERO_WINDOW_SAMPLES);
            Ok(self.values.next().expect("one probability per input frame"))
        }
    }

    fn evaluate(probabilities: Vec<f32>) -> Vec<NeuralVadEvent> {
        let mut detector = ProbabilitySequence {
            values: probabilities.clone().into_iter(),
        };
        let mut state = NeuralVadState::new(detector.frame_samples()).unwrap();
        state
            .push(
                &vec![0.1; probabilities.len() * SILERO_WINDOW_SAMPLES],
                &mut detector,
            )
            .unwrap()
    }

    #[test]
    fn confirmed_speech_streams_each_sample_once_and_closes_once() {
        let mut probabilities = vec![0.9; usize::from(SILERO_MINIMUM_SPEECH_FRAMES)];
        probabilities.extend(vec![0.1; usize::from(SILERO_HANGOVER_FRAMES)]);
        let events = evaluate(probabilities);
        let streamed = events
            .iter()
            .filter_map(|event| match event {
                NeuralVadEvent::SpeechSamples(samples) => Some(samples.len()),
                NeuralVadEvent::SpeechOnset { .. }
                | NeuralVadEvent::Segment(_)
                | NeuralVadEvent::RejectedTransient(_) => None,
            })
            .sum::<usize>();
        assert_eq!(
            streamed,
            usize::from(SILERO_MINIMUM_SPEECH_FRAMES + SILERO_HANGOVER_FRAMES)
                * SILERO_WINDOW_SAMPLES
        );
        let segments = events
            .iter()
            .filter_map(|event| match event {
                NeuralVadEvent::Segment(segment) => Some(segment),
                NeuralVadEvent::SpeechOnset { .. }
                | NeuralVadEvent::SpeechSamples(_)
                | NeuralVadEvent::RejectedTransient(_) => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].samples().len(), streamed);
        assert_eq!(segments[0].endpoint().start_sample, 0);
        assert_eq!(segments[0].endpoint().speech_end_sample, 2_048);
        assert_eq!(segments[0].endpoint().close_sample, 6_144);
        assert!(matches!(
            events.first(),
            Some(NeuralVadEvent::SpeechOnset {
                start_sample: 0,
                decision_sample: 2_048,
            })
        ));
    }

    #[test]
    fn transient_never_streams_pcm_to_recognition() {
        let mut probabilities = vec![0.9; 2];
        probabilities.extend(vec![0.1; usize::from(SILERO_HANGOVER_FRAMES)]);
        let events = evaluate(probabilities);
        assert!(matches!(
            events.as_slice(),
            [NeuralVadEvent::RejectedTransient(_)]
        ));
    }

    #[test]
    fn invalid_detector_contract_fails_before_endpoint_state() {
        assert!(matches!(
            NeuralVadState::new(160),
            Err(VadError::FrameLength {
                expected: SILERO_WINDOW_SAMPLES,
                actual: 160,
            })
        ));
    }
}
