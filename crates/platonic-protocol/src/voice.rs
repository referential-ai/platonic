//! Root-owned durable voice facts, separate from the core harness event schema.

use platonic_core::{RunId, TurnId};
use serde::{Deserialize, Deserializer, Serialize, de};

/// Revision of the root-owned voice event envelope and payload schema.
pub const VOICE_EVENT_VERSION: u32 = 1;

/// One immutable, per-run ordered companion fact.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VoiceEventEnvelope {
    /// Voice payload schema revision.
    #[serde(deserialize_with = "deserialize_revision_one")]
    pub v: u32,
    /// Zero-based, contiguous sequence within one run.
    pub sequence: u64,
    /// Typed root-owned voice fact.
    pub event: VoiceEvent,
}

impl VoiceEventEnvelope {
    /// Wraps one validated voice event in the current durable envelope.
    pub fn revision_one(sequence: u64, event: VoiceEvent) -> Self {
        Self {
            v: VOICE_EVENT_VERSION,
            sequence,
            event,
        }
    }
}

/// Bounded voice observations persisted beside, never inside, the core ledger.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case", deny_unknown_fields)]
pub enum VoiceEvent {
    /// One final capture identity and its existing bounded capture measurements.
    VoiceCaptured {
        /// Core run receiving the final transcript as its task input.
        run_id: RunId,
        /// First run turn receiving that task input.
        turn_id: TurnId,
        /// Lowercase SHA-256 of the final transcript's exact UTF-8 bytes.
        transcript_sha256: String,
        /// Length of the hashed final transcript in UTF-8 bytes.
        transcript_bytes: u64,
        /// Recognizer-accepted PCM duration in whole milliseconds.
        transcript_span_ms: u64,
        /// Complete native input frames consumed for the capture request.
        input_frames: u64,
        /// Mono 16 kHz samples passed through VAD for the request.
        output_frames: u64,
        /// First onset-candidate sample on the 16 kHz worker clock.
        vad_start_sample: u64,
        /// Exclusive end of the final speech frame on that clock.
        vad_speech_end_sample: u64,
        /// Exclusive end of the hangover frame that closed capture.
        vad_close_sample: u64,
        /// Closing VAD evaluation entry through final transcript construction.
        vad_close_to_final_us: u64,
        /// Worker normalization and resampling time for the request.
        normalization_resampling_us: u64,
    },
    /// One narrated turn after its first non-silent callback established TTFA.
    VoiceSpoken {
        /// Core run whose assistant text was narrated.
        run_id: RunId,
        /// Core turn whose assistant text was narrated.
        turn_id: TurnId,
        /// Sentence acceptance through first non-silent callback, in whole milliseconds.
        ttfa_ms: u64,
        /// Sentences that completed, or began audibly before interruption, in this turn.
        sentence_count: u64,
        /// Zero-based run sentence index where the AU5 latch interrupted, when present.
        interrupted_at: Option<u64>,
    },
    /// Exact sample-derived AU5 interruption latch projected into the root stream.
    VoiceInterrupted {
        /// Core run whose narration was interrupted.
        run_id: RunId,
        /// Core turn containing the interrupted sentence.
        turn_id: TurnId,
        /// Whitespace-normalized prefix whose proportional PCM buckets completed.
        spoken_prefix: String,
        /// Assistant delta that completed the interrupted sentence.
        delta_index: u64,
    },
}

impl VoiceEvent {
    /// Returns the stable run key duplicated by companion SQLite.
    pub fn run_id(&self) -> &RunId {
        match self {
            Self::VoiceCaptured { run_id, .. }
            | Self::VoiceSpoken { run_id, .. }
            | Self::VoiceInterrupted { run_id, .. } => run_id,
        }
    }

    /// Returns the stable turn key duplicated by companion SQLite.
    pub fn turn_id(&self) -> &TurnId {
        match self {
            Self::VoiceCaptured { turn_id, .. }
            | Self::VoiceSpoken { turn_id, .. }
            | Self::VoiceInterrupted { turn_id, .. } => turn_id,
        }
    }

    /// Validates the bounded durable fields carried by this event.
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::VoiceCaptured {
                transcript_sha256,
                transcript_bytes,
                vad_start_sample,
                vad_speech_end_sample,
                vad_close_sample,
                ..
            } => {
                if *transcript_bytes == 0 {
                    return Err("captured transcript length must be nonzero".into());
                }
                if transcript_sha256.len() != 64
                    || !transcript_sha256
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                {
                    return Err("captured transcript SHA-256 must be 64 lowercase hex bytes".into());
                }
                if vad_start_sample > vad_speech_end_sample
                    || vad_speech_end_sample > vad_close_sample
                {
                    return Err("captured VAD sample boundaries are not ordered".into());
                }
            }
            Self::VoiceSpoken { sentence_count, .. } if *sentence_count == 0 => {
                return Err("spoken sentence count must be nonzero".into());
            }
            Self::VoiceSpoken { .. } | Self::VoiceInterrupted { .. } => {}
        }
        Ok(())
    }
}

fn deserialize_revision_one<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let version = u32::deserialize(deserializer)?;
    if version != VOICE_EVENT_VERSION {
        return Err(de::Error::custom(format_args!(
            "voice event version must be {VOICE_EVENT_VERSION}, got {version}"
        )));
    }
    Ok(version)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    fn transcript_identity(transcript: &str) -> (String, u64) {
        let bytes = transcript.as_bytes();
        (
            format!("{:x}", Sha256::digest(bytes)),
            u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        )
    }

    #[test]
    fn revision_one_serialization_is_literal_and_fail_closed() {
        let run_id = RunId::new("run_voice").unwrap();
        let turn_id = TurnId::new("turn_2").unwrap();
        let cases = [
            (
                VoiceEvent::VoiceCaptured {
                    run_id: run_id.clone(),
                    turn_id: turn_id.clone(),
                    transcript_sha256: "a".repeat(64),
                    transcript_bytes: 12,
                    transcript_span_ms: 740,
                    input_frames: 35_520,
                    output_frames: 11_840,
                    vad_start_sample: 160,
                    vad_speech_end_sample: 10_240,
                    vad_close_sample: 11_840,
                    vad_close_to_final_us: 91_000,
                    normalization_resampling_us: 870,
                },
                concat!(
                    r#"{"v":1,"sequence":0,"event":{"event":"voice_captured","run_id":"run_voice","turn_id":"turn_2","transcript_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","transcript_bytes":12,"transcript_span_ms":740,"input_frames":35520,"output_frames":11840,"vad_start_sample":160,"vad_speech_end_sample":10240,"vad_close_sample":11840,"vad_close_to_final_us":91000,"normalization_resampling_us":870}}"#,
                ),
            ),
            (
                VoiceEvent::VoiceSpoken {
                    run_id: run_id.clone(),
                    turn_id: turn_id.clone(),
                    ttfa_ms: 287,
                    sentence_count: 3,
                    interrupted_at: Some(2),
                },
                r#"{"v":1,"sequence":0,"event":{"event":"voice_spoken","run_id":"run_voice","turn_id":"turn_2","ttfa_ms":287,"sentence_count":3,"interrupted_at":2}}"#,
            ),
            (
                VoiceEvent::VoiceInterrupted {
                    run_id,
                    turn_id,
                    spoken_prefix: "The exact spoken prefix".into(),
                    delta_index: 17,
                },
                r#"{"v":1,"sequence":0,"event":{"event":"voice_interrupted","run_id":"run_voice","turn_id":"turn_2","spoken_prefix":"The exact spoken prefix","delta_index":17}}"#,
            ),
        ];

        for (event, expected) in cases {
            let envelope = VoiceEventEnvelope::revision_one(0, event);
            let encoded = serde_json::to_string(&envelope).unwrap();
            assert_eq!(encoded, expected);
            assert_eq!(
                serde_json::from_str::<VoiceEventEnvelope>(&encoded).unwrap(),
                envelope
            );
        }

        let unknown_variant = r#"{"v":1,"sequence":0,"event":{"event":"voice_guaranteed","run_id":"run_voice","turn_id":"turn_2"}}"#;
        assert!(serde_json::from_str::<VoiceEventEnvelope>(unknown_variant).is_err());
        let unknown_field = r#"{"v":1,"sequence":0,"event":{"event":"voice_spoken","run_id":"run_voice","turn_id":"turn_2","ttfa_ms":1,"sentence_count":1,"interrupted_at":null,"success":true}}"#;
        assert!(serde_json::from_str::<VoiceEventEnvelope>(unknown_field).is_err());
        let future_revision = r#"{"v":2,"sequence":0,"event":{"event":"voice_spoken","run_id":"run_voice","turn_id":"turn_2","ttfa_ms":1,"sentence_count":1,"interrupted_at":null}}"#;
        assert!(serde_json::from_str::<VoiceEventEnvelope>(future_revision).is_err());
    }

    #[test]
    fn transcript_identity_hashes_exact_utf8_bytes_without_retaining_content() {
        assert_eq!(
            transcript_identity("hello"),
            (
                "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824".into(),
                5,
            )
        );
        assert_eq!(transcript_identity("\u{00e9}").1, 2);
    }
}
