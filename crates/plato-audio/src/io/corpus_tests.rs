use std::path::Path;

use super::{WhisperConfig, WhisperRecognizer, capture::recognize_segment};
use crate::{
    Transcript, VadEndpoint,
    core::vad::{ThresholdVad, VadEvent},
};

const SPEECH_SHA256: &str = "5020c50762851fb3182a7f9690adb3882e4cc2083b5610edb505f046c15b3dbc";
const NOISE_SHA256: &str = "3edf82ef3f40c9aa88174c5cf1e5ae15a5e88ce4bdf11d9cec6278747f13e6c3";
const EXPECTED_TRANSCRIPT: &str = "What is the capital of France?";

#[test]
#[ignore = "requires the pinned large-v3-turbo model and CUDA device"]
fn au3_threshold_corpus_final_and_silence_regression_remains_exact() {
    let fixture_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/au3");
    let speech_path = fixture_dir.join("spoken-question.wav");
    let noise_path = fixture_dir.join("steady-noise.wav");
    assert_eq!(sha256_file(&speech_path), SPEECH_SHA256);
    assert_eq!(sha256_file(&noise_path), NOISE_SHA256);

    let speech = read_wav(&speech_path);
    let mut vad = ThresholdVad::new();
    let segment = vad
        .push(&speech)
        .unwrap()
        .into_iter()
        .find_map(|event| match event {
            VadEvent::Segment(segment) => Some(segment),
            VadEvent::RejectedTransient(_) => None,
        })
        .expect("AU3 speech fixture retains one segment");
    assert_eq!(
        segment.endpoint(),
        VadEndpoint {
            start_sample: 8_000,
            speech_end_sample: 36_320,
            close_sample: 40_320,
        }
    );

    let model = std::env::var_os("PLATO_AUDIO_WHISPER_MODEL")
        .expect("PLATO_AUDIO_WHISPER_MODEL must name the pinned model");
    let mut recognizer = WhisperRecognizer::load(WhisperConfig::new(model)).unwrap();
    let transcript = recognize_segment(&mut recognizer, &segment).unwrap();
    assert_eq!(
        transcript,
        Transcript {
            text: EXPECTED_TRANSCRIPT.to_owned(),
            is_final: true,
            span_ms: 2_020,
        }
    );
    let before_noise = recognizer.metrics_reader().snapshot();
    let mut noise_vad = ThresholdVad::new();
    assert!(noise_vad.push(&read_wav(&noise_path)).unwrap().is_empty());
    assert_eq!(recognizer.metrics_reader().snapshot(), before_noise);
}

fn read_wav(path: &Path) -> Vec<f32> {
    let mut reader = hound::WavReader::open(path).expect("fixture WAV must be readable");
    let spec = reader.spec();
    assert_eq!(spec.sample_rate, 16_000);
    assert_eq!(spec.channels, 1);
    assert_eq!(spec.bits_per_sample, 16);
    assert_eq!(spec.sample_format, hound::SampleFormat::Int);
    reader
        .samples::<i16>()
        .map(|sample| f32::from(sample.expect("fixture sample must decode")) / 32_768.0)
        .collect()
}

fn sha256_file(path: &Path) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(std::fs::read(path).unwrap()))
}
