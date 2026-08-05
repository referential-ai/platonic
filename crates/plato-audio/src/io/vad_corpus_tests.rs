use std::{fs, ops::Range, path::Path};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{SileroConfig, SileroMetrics, SileroProvenance, SileroVad};
use crate::{
    SILERO_WINDOW_SAMPLES, VadEndpoint, VoiceActivityDetector,
    core::vad::{NeuralVadEvent, NeuralVadState, ThresholdVad, VadEvent},
};

const WAV_SHA256: &str = "ce0775c71a2bb748234a92a2c446997d17c299a56a04d38cfa43975fa6245ff3";
const MANIFEST_SHA256: &str = "228cecd260153155a4c6ec7b8f7a25a519c6c934e4451aad2820465b548d6658";
const CORPUS_SHA256: &str = "b70723e810ea53c39dff05d0bb746eb89e7dbeb76648c555e1330fbffbebe8f4";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CorpusManifest {
    schema: String,
    license: String,
    human_recording: bool,
    format: CorpusFormat,
    fixture: CorpusFixture,
    annotations: CorpusAnnotations,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CorpusFormat {
    codec: String,
    sample_rate: u32,
    channels: u16,
    samples: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CorpusFixture {
    path: String,
    source: String,
    source_text: String,
    sha256: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CorpusAnnotations {
    speech: Vec<SpeechAnnotation>,
    noise: Vec<NoiseAnnotation>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SpeechAnnotation {
    start_sample: u64,
    end_sample: u64,
    text: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NoiseAnnotation {
    start_sample: u64,
    end_sample: u64,
    kind: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
struct ConfusionMatrix {
    true_positive_samples: u64,
    true_negative_samples: u64,
    false_positive_samples: u64,
    false_negative_samples: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
struct EndpointDelta {
    annotation_start_sample: u64,
    annotation_end_sample: u64,
    predicted_start_delta_samples: i64,
    predicted_speech_end_delta_samples: i64,
    predicted_close_delta_samples: i64,
}

#[derive(Debug, Serialize)]
struct DetectorScore {
    detector: &'static str,
    confusion: ConfusionMatrix,
    predicted_endpoints: Vec<VadEndpoint>,
    endpoint_deltas: Vec<EndpointDelta>,
    false_cuts: usize,
    false_cut_rate: f64,
    missed_speech_segments: usize,
}

#[derive(Serialize)]
struct VadCorpusProof {
    schema: &'static str,
    admitted_base: &'static str,
    corpus_license: &'static str,
    corpus_checksum_composition: &'static str,
    corpus_sha256: &'static str,
    manifest_sha256: &'static str,
    wav_sha256: &'static str,
    sample_count: usize,
    annotations: usize,
    threshold: DetectorScore,
    silero: DetectorScore,
    silero_provenance: SileroProvenance,
    silero_metrics: SileroMetrics,
}

#[test]
#[ignore = "requires the pinned Silero ONNX artifact"]
fn silero_strictly_reduces_au3_false_cuts_without_missing_speech() {
    let fixture_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/au4");
    let manifest_bytes = fs::read(fixture_dir.join("manifest.json")).unwrap();
    let wav_bytes = fs::read(fixture_dir.join("speech-plus-noise.wav")).unwrap();
    assert_eq!(sha256(&manifest_bytes), MANIFEST_SHA256);
    assert_eq!(sha256(&wav_bytes), WAV_SHA256);
    let mut corpus_hasher = Sha256::new();
    corpus_hasher.update(&manifest_bytes);
    corpus_hasher.update(&wav_bytes);
    assert_eq!(format!("{:x}", corpus_hasher.finalize()), CORPUS_SHA256);

    let manifest: CorpusManifest = serde_json::from_slice(&manifest_bytes).unwrap();
    assert_eq!(manifest.schema, "plato_audio.au4_corpus.v1");
    assert_eq!(manifest.license, "CC0-1.0");
    assert!(!manifest.human_recording);
    assert_eq!(manifest.format.codec, "pcm_s16le");
    assert_eq!(manifest.format.sample_rate, 16_000);
    assert_eq!(manifest.format.channels, 1);
    assert_eq!(manifest.fixture.path, "speech-plus-noise.wav");
    assert!(manifest.fixture.source.contains("FFmpeg n8.1.2"));
    assert_eq!(
        manifest.fixture.source_text,
        "What is the capital of France?"
    );
    assert_eq!(manifest.fixture.sha256, WAV_SHA256);
    assert_eq!(manifest.annotations.noise.len(), 1);
    assert_eq!(manifest.annotations.noise[0].start_sample, 8_000);
    assert_eq!(manifest.annotations.noise[0].end_sample, 15_200);
    assert_eq!(manifest.annotations.noise[0].kind, "white_noise_burst");

    let samples = read_wav(&fixture_dir.join(&manifest.fixture.path));
    assert_eq!(samples.len(), manifest.format.samples);
    assert_eq!(samples.len() % SILERO_WINDOW_SAMPLES, 0);
    let annotations = manifest
        .annotations
        .speech
        .iter()
        .map(|annotation| {
            assert_eq!(annotation.text, manifest.fixture.source_text);
            annotation.start_sample..annotation.end_sample
        })
        .collect::<Vec<_>>();

    let threshold_endpoints = threshold_endpoints(&samples);
    let threshold = score(
        "au3_threshold",
        samples.len(),
        &annotations,
        threshold_endpoints,
    );

    let model = std::env::var_os("PLATO_AUDIO_SILERO_MODEL")
        .expect("PLATO_AUDIO_SILERO_MODEL must name the pinned artifact");
    let mut detector = SileroVad::load(SileroConfig::new(model)).unwrap();
    let provenance = detector.provenance().clone();
    let metrics = detector.metrics_reader();
    detector.reset();
    let silero_endpoints = silero_endpoints(&samples, &mut detector);
    let silero = score(
        "silero_v6_2_1",
        samples.len(),
        &annotations,
        silero_endpoints,
    );
    let silero_metrics = metrics.snapshot();

    assert!(silero.false_cuts < threshold.false_cuts);
    assert!(silero.false_cut_rate < threshold.false_cut_rate);
    assert!(silero.missed_speech_segments <= threshold.missed_speech_segments);
    assert_eq!(silero_metrics.session_loads, 1);
    assert_eq!(
        silero_metrics.inference_frames,
        (samples.len() / SILERO_WINDOW_SAMPLES) as u64
    );
    assert_eq!(silero_metrics.state_resets, 1);

    let proof = VadCorpusProof {
        schema: "plato_audio.au4_vad_corpus_proof.v1",
        admitted_base: "aca8304a768c519f379ff14f8ca1d515dde231a4",
        corpus_license: "CC0-1.0",
        corpus_checksum_composition: "exact manifest.json bytes followed by exact speech-plus-noise.wav bytes",
        corpus_sha256: CORPUS_SHA256,
        manifest_sha256: MANIFEST_SHA256,
        wav_sha256: WAV_SHA256,
        sample_count: samples.len(),
        annotations: annotations.len(),
        threshold,
        silero,
        silero_provenance: provenance,
        silero_metrics,
    };
    println!(
        "AU4_VAD_CORPUS_PROOF={}",
        serde_json::to_string(&proof).unwrap()
    );
}

fn threshold_endpoints(samples: &[f32]) -> Vec<VadEndpoint> {
    ThresholdVad::new()
        .push(samples)
        .unwrap()
        .into_iter()
        .filter_map(|event| match event {
            VadEvent::Segment(segment) => Some(segment.endpoint()),
            VadEvent::RejectedTransient(_) => None,
        })
        .collect()
}

fn silero_endpoints(samples: &[f32], detector: &mut dyn VoiceActivityDetector) -> Vec<VadEndpoint> {
    let mut state = NeuralVadState::new(detector.frame_samples()).unwrap();
    state
        .push(samples, detector)
        .unwrap()
        .into_iter()
        .filter_map(|event| match event {
            NeuralVadEvent::Segment(segment) => Some(segment.endpoint()),
            NeuralVadEvent::SpeechOnset { .. }
            | NeuralVadEvent::SpeechSamples(_)
            | NeuralVadEvent::RejectedTransient(_) => None,
        })
        .collect()
}

fn score(
    detector: &'static str,
    sample_count: usize,
    annotations: &[Range<u64>],
    predictions: Vec<VadEndpoint>,
) -> DetectorScore {
    let mut confusion = ConfusionMatrix {
        true_positive_samples: 0,
        true_negative_samples: 0,
        false_positive_samples: 0,
        false_negative_samples: 0,
    };
    for sample in 0..sample_count as u64 {
        let expected = annotations.iter().any(|range| range.contains(&sample));
        let predicted = predictions
            .iter()
            .any(|endpoint| (endpoint.start_sample..endpoint.speech_end_sample).contains(&sample));
        match (expected, predicted) {
            (true, true) => confusion.true_positive_samples += 1,
            (false, false) => confusion.true_negative_samples += 1,
            (false, true) => confusion.false_positive_samples += 1,
            (true, false) => confusion.false_negative_samples += 1,
        }
    }

    let mut used_predictions = vec![false; predictions.len()];
    let mut endpoint_deltas = Vec::new();
    for annotation in annotations {
        let matched = predictions
            .iter()
            .enumerate()
            .filter(|(index, _)| !used_predictions[*index])
            .max_by_key(|(_, endpoint)| overlap(annotation, endpoint));
        if let Some((index, endpoint)) =
            matched.filter(|(_, endpoint)| overlap(annotation, endpoint) > 0)
        {
            used_predictions[index] = true;
            endpoint_deltas.push(EndpointDelta {
                annotation_start_sample: annotation.start,
                annotation_end_sample: annotation.end,
                predicted_start_delta_samples: signed_delta(
                    endpoint.start_sample,
                    annotation.start,
                ),
                predicted_speech_end_delta_samples: signed_delta(
                    endpoint.speech_end_sample,
                    annotation.end,
                ),
                predicted_close_delta_samples: signed_delta(endpoint.close_sample, annotation.end),
            });
        }
    }
    let matched = used_predictions.iter().filter(|matched| **matched).count();
    let false_cuts = predictions.len().saturating_sub(matched);
    DetectorScore {
        detector,
        confusion,
        predicted_endpoints: predictions,
        endpoint_deltas,
        false_cuts,
        false_cut_rate: false_cuts as f64 / annotations.len() as f64,
        missed_speech_segments: annotations.len().saturating_sub(matched),
    }
}

fn overlap(annotation: &Range<u64>, endpoint: &VadEndpoint) -> u64 {
    annotation
        .end
        .min(endpoint.speech_end_sample)
        .saturating_sub(annotation.start.max(endpoint.start_sample))
}

fn signed_delta(actual: u64, expected: u64) -> i64 {
    i64::try_from(actual).unwrap_or(i64::MAX) - i64::try_from(expected).unwrap_or(i64::MAX)
}

fn read_wav(path: &Path) -> Vec<f32> {
    let mut reader = hound::WavReader::open(path).unwrap();
    let spec = reader.spec();
    assert_eq!(spec.sample_rate, 16_000);
    assert_eq!(spec.channels, 1);
    assert_eq!(spec.bits_per_sample, 16);
    assert_eq!(spec.sample_format, hound::SampleFormat::Int);
    reader
        .samples::<i16>()
        .map(|sample| f32::from(sample.unwrap()) / 32_768.0)
        .collect()
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[test]
fn scorer_counts_extra_endpoints_as_false_cuts_and_unmatched_annotations_as_misses() {
    let annotations = vec![100..200, 400..500];
    let predictions = vec![
        VadEndpoint {
            start_sample: 90,
            speech_end_sample: 190,
            close_sample: 220,
        },
        VadEndpoint {
            start_sample: 150,
            speech_end_sample: 210,
            close_sample: 240,
        },
    ];
    let score = score("fixture", 600, &annotations, predictions);
    assert_eq!(score.false_cuts, 1);
    assert_eq!(score.missed_speech_segments, 1);
    assert_eq!(score.endpoint_deltas.len(), 1);
}
