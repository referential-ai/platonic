use std::{path::Path, time::Instant};

use super::{WhisperConfig, WhisperRecognizer, capture::recognize_segment};
use crate::{
    PcmData, PcmFrame, SpeechRecognizer, Transcript, VadEndpoint,
    core::vad::{ThresholdVad, VadEvent},
};

const SPEECH_SHA256: &str = "5020c50762851fb3182a7f9690adb3882e4cc2083b5610edb505f046c15b3dbc";
const NOISE_SHA256: &str = "3edf82ef3f40c9aa88174c5cf1e5ae15a5e88ce4bdf11d9cec6278747f13e6c3";
const EXPECTED_TRANSCRIPT: &str = "What is the capital of France?";
const LONG_WAV_SHA256: &str = "71129abe7edb62301ab3c7bd035d999cb1b43d0ab4d92665e387555f3b5ec1d0";
const LONG_MANIFEST_SHA256: &str =
    "7cd6da0efa9db84c6cbbbade052fa9d088e3114c6bc3a85ce113aedca7a4deee";
const LONG_EXPECTED_TRANSCRIPT: &str = concat!(
    "What is the capital of France? Which city is the capital of of Germany. ",
    "Name the capital of Italy. Tell me the capital of Spain. What is the capital of Portugal? ",
    "which city is the capital of Belgium. the capital of Austria. Tell me the capital of Greece. ",
    "What is the capital of Ireland? which city is the capital of Denmark. ",
    "Name the capital of Sweden. Tell me the capital of Norway."
);
const LONG_TRIALS: usize = 20;
const MAX_FINAL_P95_US: u64 = 120_000;

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

#[test]
#[ignore = "requires the pinned large-v3-turbo model and CUDA device"]
fn long_utterance_final_is_bounded_and_preserves_exact_stable_text() {
    let long_audio = long_fixture();
    let model = std::env::var_os("PLATO_AUDIO_WHISPER_MODEL")
        .expect("PLATO_AUDIO_WHISPER_MODEL must name the pinned model");
    let mut recognizer = WhisperRecognizer::load(WhisperConfig::new(model)).unwrap();
    let metrics = recognizer.metrics_reader();

    let (transcript, final_us) = recognize_long_utterance(&mut recognizer, &long_audio);
    assert_long_transcript(&transcript);
    let metrics = metrics.snapshot();
    assert_eq!(metrics.finalizations, 1);
    assert!(metrics.window_commits > 0);
    assert!(metrics.maximum_decode_samples <= 80_000);
    assert!(metrics.last_final_window_samples <= 80_000);
    assert!(final_us <= MAX_FINAL_P95_US);
}

#[test]
#[ignore = "requires the pinned large-v3-turbo model and CUDA device"]
fn twenty_long_utterance_finals_are_bounded_and_preserve_exact_stable_text() {
    let long_audio = long_fixture();

    let model = std::env::var_os("PLATO_AUDIO_WHISPER_MODEL")
        .expect("PLATO_AUDIO_WHISPER_MODEL must name the pinned model");
    let mut recognizer = WhisperRecognizer::load(WhisperConfig::new(model)).unwrap();
    let provenance = recognizer.provenance().clone();
    let metrics = recognizer.metrics_reader();
    let warmup = recognize_long_utterance(&mut recognizer, &long_audio);
    assert_long_transcript(&warmup.0);
    let mut final_us = Vec::with_capacity(LONG_TRIALS);
    for _ in 0..LONG_TRIALS {
        let (transcript, elapsed_us) = recognize_long_utterance(&mut recognizer, &long_audio);
        assert_long_transcript(&transcript);
        final_us.push(elapsed_us);
    }
    let mut sorted = final_us.clone();
    sorted.sort_unstable();
    let p50_us = percentile(&sorted, 50);
    let p95_us = percentile(&sorted, 95);
    assert!(p95_us <= MAX_FINAL_P95_US);

    let metrics = metrics.snapshot();
    assert_eq!(metrics.finalizations, (LONG_TRIALS + 1) as u64);
    assert!(metrics.window_commits > 0);
    assert!(metrics.maximum_decode_samples <= 80_000);
    assert!(metrics.last_final_window_samples <= 80_000);
    println!(
        "AU4_LONG_WHISPER_FINAL_PROOF={}",
        serde_json::json!({
            "schema": "plato_audio.au4_long_whisper_final.v1",
            "admitted_base": "aca8304a768c519f379ff14f8ca1d515dde231a4",
            "timing_boundary": "WhisperRecognizer::finalize entry through exact final Transcript construction; model/session warm and rolling decode work excluded",
            "warmup_excluded": true,
            "trial_count": LONG_TRIALS,
            "wav_sha256": LONG_WAV_SHA256,
            "sample_count": long_audio.len(),
            "duration_ms": warmup.0.span_ms,
            "expected_transcript": LONG_EXPECTED_TRANSCRIPT,
            "threshold_us": MAX_FINAL_P95_US,
            "p50_us": p50_us,
            "p95_us": p95_us,
            "max_us": sorted.last().unwrap(),
            "samples_us": final_us,
            "provenance": provenance,
            "metrics": metrics,
        })
    );
}

fn long_fixture() -> Vec<f32> {
    let fixture_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/au4");
    assert_eq!(
        sha256_file(&fixture_dir.join("long-utterance.json")),
        LONG_MANIFEST_SHA256
    );
    let fixture = fixture_dir.join("long-utterance.wav");
    assert_eq!(sha256_file(&fixture), LONG_WAV_SHA256);
    let audio = read_wav(&fixture);
    assert_eq!(audio.len(), 397_752);
    audio
}

fn recognize_long_utterance(
    recognizer: &mut WhisperRecognizer,
    audio: &[f32],
) -> (Transcript, u64) {
    recognizer.reset();
    let format = recognizer.input_format();
    for &sample in audio {
        let frame = PcmFrame::new(format, PcmData::F32(Box::new([sample]))).unwrap();
        let _ = recognizer.accept(&frame).unwrap();
    }
    let started = Instant::now();
    let transcript = recognizer.finalize().unwrap();
    (
        transcript,
        u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
    )
}

fn assert_long_transcript(transcript: &Transcript) {
    assert_eq!(transcript.text, LONG_EXPECTED_TRANSCRIPT);
    assert!(transcript.is_final);
    assert_eq!(transcript.span_ms, 24_859);
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    let index = (sorted.len() * percentile).div_ceil(100).saturating_sub(1);
    sorted[index]
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
