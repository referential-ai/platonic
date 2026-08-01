use std::{fs, path::Path, time::Instant};

use serde::Serialize;
use sha2::{Digest, Sha256};

use super::{
    capture::recognize_segment,
    recognize::{WhisperConfig, WhisperMetrics, WhisperProvenance, WhisperRecognizer},
};
use crate::{
    Transcript, VAD_WINDOW_SAMPLES, VadEndpoint,
    core::vad::{ThresholdVad, VadEvent},
};

const TRIALS: usize = 20;
const MAX_P95_US: u64 = 300_000;
const SPEECH_SHA256: &str = "5020c50762851fb3182a7f9690adb3882e4cc2083b5610edb505f046c15b3dbc";
const NOISE_SHA256: &str = "3edf82ef3f40c9aa88174c5cf1e5ae15a5e88ce4bdf11d9cec6278747f13e6c3";
const EXPECTED_TRANSCRIPT: &str = "What is the capital of France?";
const TIMING_BOUNDARY: &str = "fixed-threshold VAD close through one final Transcript; resident model load and warmup excluded";

#[derive(Serialize)]
struct CorpusProof {
    schema: &'static str,
    timing_boundary: &'static str,
    trial_count: usize,
    threshold_us: u64,
    p50_us: u64,
    p95_us: u64,
    max_us: u64,
    trials_us: Vec<u64>,
    endpoint: VadEndpoint,
    transcript: Transcript,
    no_silence_hallucination: bool,
    manifest: serde_json::Value,
    provenance: WhisperProvenance,
    metrics: WhisperMetrics,
}

#[test]
#[ignore = "requires the pinned large-v3-turbo model and CUDA device"]
fn recorded_corpus_has_exact_endpoint_transcript_and_warm_latency() {
    let fixture_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/au3");
    let speech_path = fixture_dir.join("spoken-question.wav");
    let noise_path = fixture_dir.join("steady-noise.wav");
    assert_eq!(sha256(&speech_path), SPEECH_SHA256);
    assert_eq!(sha256(&noise_path), NOISE_SHA256);
    let manifest: serde_json::Value = serde_json::from_slice(
        &fs::read(fixture_dir.join("manifest.json")).expect("fixture manifest must be readable"),
    )
    .expect("fixture manifest must be valid JSON");
    assert_eq!(manifest["license"], "CC0-1.0");
    assert_eq!(manifest["human_recording"], false);

    let speech = read_wav(&speech_path);
    let mut vad = ThresholdVad::new();
    let events = vad.push(&speech).unwrap();
    let segment = match events.as_slice() {
        [VadEvent::Segment(segment)] => segment,
        [VadEvent::RejectedTransient(endpoint)] => {
            panic!("speech fixture was rejected as a transient at {endpoint:?}")
        }
        _ => panic!(
            "speech fixture must produce exactly one retained segment; observed {} VAD events",
            events.len()
        ),
    };
    assert_eq!(segment.endpoint(), expected_endpoint());

    let model_path = std::env::var_os("PLATO_AUDIO_WHISPER_MODEL")
        .expect("PLATO_AUDIO_WHISPER_MODEL must name the pinned model");
    let mut recognizer = WhisperRecognizer::load(WhisperConfig::new(model_path)).unwrap();
    let provenance = recognizer.provenance().clone();
    let metrics = recognizer.metrics_reader();

    let warmup = recognize_segment(&mut recognizer, segment).unwrap();
    assert_eq!(warmup.text, EXPECTED_TRANSCRIPT);
    let mut trials_us = Vec::with_capacity(TRIALS);
    let mut transcript = warmup;
    for _ in 0..TRIALS {
        let started = Instant::now();
        transcript = recognize_segment(&mut recognizer, segment).unwrap();
        trials_us.push(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX));
        assert_eq!(transcript.text, EXPECTED_TRANSCRIPT);
    }
    let mut sorted = trials_us.clone();
    sorted.sort_unstable();
    let p50_us = percentile(&sorted, 50);
    let p95_us = percentile(&sorted, 95);
    assert!(
        p95_us <= MAX_P95_US,
        "warm VAD-close latency was {p95_us} us"
    );

    let before_noise = metrics.snapshot();
    let noise = read_wav(&noise_path);
    let mut noise_vad = ThresholdVad::new();
    assert!(noise_vad.push(&noise).unwrap().is_empty());
    let after_noise = metrics.snapshot();
    assert_eq!(before_noise.finalizations, after_noise.finalizations);
    assert_eq!(after_noise.model_loads, 1);
    assert_eq!(after_noise.finalizations, (TRIALS + 1) as u64);

    let proof = CorpusProof {
        schema: "plato_audio.au3_corpus_proof.v1",
        timing_boundary: TIMING_BOUNDARY,
        trial_count: TRIALS,
        threshold_us: MAX_P95_US,
        p50_us,
        p95_us,
        max_us: *sorted.last().expect("twenty trials are nonempty"),
        trials_us,
        endpoint: segment.endpoint(),
        transcript,
        no_silence_hallucination: true,
        manifest,
        provenance,
        metrics: after_noise,
    };
    println!(
        "AU3_CORPUS_PROOF={}",
        serde_json::to_string(&proof).unwrap()
    );
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

fn sha256(path: &Path) -> String {
    format!(
        "{:x}",
        Sha256::digest(fs::read(path).expect("fixture must be readable"))
    )
}

fn expected_endpoint() -> VadEndpoint {
    VadEndpoint {
        start_sample: 8_000,
        speech_end_sample: 36_320,
        close_sample: 40_320,
    }
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    let index = (sorted.len() * percentile).div_ceil(100).saturating_sub(1);
    sorted[index]
}

#[test]
fn corpus_lengths_are_complete_vad_windows() {
    let fixture_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/au3");
    assert_eq!(
        read_wav(&fixture_dir.join("steady-noise.wav")).len() % VAD_WINDOW_SAMPLES,
        0
    );
}
