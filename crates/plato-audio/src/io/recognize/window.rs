const TIMESTAMP_SAMPLES: usize = 160;
pub(super) const MAX_END_PADDING_SAMPLES: usize = 2 * TIMESTAMP_SAMPLES;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct DecodedSegment {
    pub(super) text: String,
    pub(super) start_sample: usize,
    pub(super) end_sample: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct DecodedWindow {
    pub(super) segments: Vec<DecodedSegment>,
}

impl DecodedWindow {
    pub(super) fn text(&self) -> String {
        self.segments
            .iter()
            .map(|segment| segment.text.as_str())
            .collect()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct StablePrefix {
    pub(super) text: String,
    pub(super) drain_samples: usize,
}

pub(super) fn timestamp_samples(timestamp: i64) -> Result<usize, String> {
    usize::try_from(timestamp)
        .ok()
        .and_then(|value| value.checked_mul(TIMESTAMP_SAMPLES))
        .ok_or_else(|| format!("invalid Whisper timestamp {timestamp}"))
}

pub(super) fn clamp_end_padding(
    end_sample: usize,
    pending_samples: usize,
) -> Result<usize, String> {
    let padding = end_sample.saturating_sub(pending_samples);
    if end_sample <= pending_samples {
        Ok(end_sample)
    } else if padding <= MAX_END_PADDING_SAMPLES {
        Ok(pending_samples)
    } else {
        Err(format!(
            "Whisper segment end exceeds pending PCM by {padding} samples; maximum timestamp \
             padding is {MAX_END_PADDING_SAMPLES}"
        ))
    }
}

pub(super) fn validate_decode_window(samples: usize, maximum: usize) -> Result<(), String> {
    if samples > maximum {
        return Err(format!(
            "Whisper decode window contains {samples} samples, maximum is {maximum}"
        ));
    }
    Ok(())
}

pub(super) fn validate_decoded_window(
    window: &DecodedWindow,
    pending_samples: usize,
) -> Result<(), String> {
    let mut previous_end = 0;
    for (index, segment) in window.segments.iter().enumerate() {
        if segment.start_sample < previous_end
            || segment.end_sample <= segment.start_sample
            || segment.end_sample > pending_samples
        {
            return Err(format!(
                "Whisper segment {index} has invalid bounds {}..{} for {pending_samples} pending \
                 samples",
                segment.start_sample, segment.end_sample,
            ));
        }
        previous_end = segment.end_sample;
    }
    Ok(())
}

pub(super) fn stable_prefix(
    previous: &DecodedWindow,
    current: &DecodedWindow,
    pending_samples: usize,
    overlap_samples: usize,
) -> Result<Option<StablePrefix>, String> {
    validate_decoded_window(previous, pending_samples)?;
    validate_decoded_window(current, pending_samples)?;
    let stable_limit = pending_samples.saturating_sub(overlap_samples);
    let current_text = current.text();
    let mut text = String::new();
    let mut drain_samples = 0;
    for previous in &previous.segments {
        if previous.end_sample > stable_limit {
            break;
        }
        let mut candidate = text.clone();
        candidate.push_str(&previous.text);
        if candidate.is_empty() || !current_text.starts_with(&candidate) {
            break;
        }
        text = candidate;
        drain_samples = previous.end_sample;
    }
    if drain_samples == 0 {
        Ok(None)
    } else {
        Ok(Some(StablePrefix {
            text,
            drain_samples,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn segment(text: &str, start_sample: usize, end_sample: usize) -> DecodedSegment {
        DecodedSegment {
            text: text.to_owned(),
            start_sample,
            end_sample,
        }
    }

    #[test]
    fn commits_only_identical_leading_segments_outside_overlap() {
        let previous = DecodedWindow {
            segments: vec![
                segment(" Hello", 0, 16_000),
                segment(" word", 16_000, 30_000),
            ],
        };
        let current = DecodedWindow {
            segments: vec![segment(" Hello world", 0, 30_000)],
        };

        assert_eq!(
            stable_prefix(&previous, &current, 48_000, 16_000).unwrap(),
            Some(StablePrefix {
                text: " Hello".to_owned(),
                drain_samples: 16_000,
            })
        );
    }

    #[test]
    fn current_timestamp_drift_does_not_override_a_valid_prior_boundary() {
        let previous = DecodedWindow {
            segments: vec![segment(" Hello", 0, 16_000)],
        };
        let current = DecodedWindow {
            segments: vec![segment(" Hello", 0, 16_160)],
        };

        assert_eq!(
            stable_prefix(&previous, &current, 48_000, 16_000).unwrap(),
            Some(StablePrefix {
                text: " Hello".to_owned(),
                drain_samples: 16_000,
            })
        );
    }

    #[test]
    fn segment_text_and_post_commit_whitespace_are_byte_exact() {
        let current = DecodedWindow {
            segments: vec![
                segment(" First.  ", 0, 16_000),
                segment("Second.", 16_000, 32_000),
            ],
        };
        let prefix = stable_prefix(&current, &current, 64_000, 16_000)
            .unwrap()
            .unwrap();
        assert_eq!(prefix.text, " First.  Second.");
    }

    #[test]
    fn repeated_rollover_preserves_every_stable_segment_and_bounded_tail() {
        const MAXIMUM: usize = 80_000;
        const DRAIN: usize = 64_000;
        let mut committed = String::new();
        let mut accepted = 0;
        for index in 0..6 {
            accepted += MAXIMUM - accepted.min(MAXIMUM - DRAIN);
            let stable = segment(&format!(" part-{index}"), 0, DRAIN);
            let tail = segment(" pending", DRAIN, MAXIMUM);
            let previous = DecodedWindow {
                segments: vec![stable.clone(), tail.clone()],
            };
            let current = DecodedWindow {
                segments: vec![stable, tail],
            };
            validate_decode_window(MAXIMUM, MAXIMUM).unwrap();
            let prefix = stable_prefix(&previous, &current, MAXIMUM, 16_000)
                .unwrap()
                .unwrap();
            assert_eq!(prefix.drain_samples, DRAIN);
            committed.push_str(&prefix.text);
            assert_eq!(prefix.text, format!(" part-{index}"));
        }

        committed.push_str(" pending");
        assert_eq!(
            committed,
            " part-0 part-1 part-2 part-3 part-4 part-5 pending"
        );
        assert_eq!(accepted, 400_000);
        assert!(validate_decode_window(MAXIMUM + 1, MAXIMUM).is_err());
    }

    #[test]
    fn invalid_or_overlapping_timestamps_fail_closed() {
        assert!(timestamp_samples(-1).is_err());
        let invalid = DecodedWindow {
            segments: vec![segment(" a", 20_000, 10_000)],
        };
        assert!(stable_prefix(&invalid, &invalid, 48_000, 16_000).is_err());

        let valid = DecodedWindow {
            segments: vec![segment(" a", 0, 10_000)],
        };
        let out_of_bounds = DecodedWindow {
            segments: vec![segment(" a", 0, 48_321)],
        };
        assert!(stable_prefix(&valid, &out_of_bounds, 48_000, 16_000).is_err());
    }

    #[test]
    fn terminal_end_padding_clamps_two_ticks_and_rejects_one_sample_more() {
        assert_eq!(clamp_end_padding(48_320, 48_000).unwrap(), 48_000);
        assert!(clamp_end_padding(48_321, 48_000).is_err());
    }
}
