use std::{
    io::{self, IsTerminal},
    path::Path,
    process::Command,
    time::Instant,
};

use plato_audio::{
    AudioFormat, CapturePartial, KokoroConfig, KokoroMetrics, KokoroProvenance, KokoroSynthesizer,
    NeuralVadEvent, NeuralVadState, OrtRuntime, OrtRuntimeMetrics, PcmData, PcmFrame,
    SILERO_WINDOW_SAMPLES, SampleFormat, SileroConfig, SileroMetrics, SileroProvenance, SileroVad,
    SpeechRecognizer, Transcript, VadEndpoint, VoiceActivityDetector, WHISPER_MODEL_SHA256,
    WhisperConfig, WhisperMetrics, WhisperProvenance, WhisperRecognizer,
};
use serde::Serialize;

use super::TerminalVoiceInput;

const TRIALS: usize = 20;
const MAX_PARTIAL_P95_US: u64 = 200_000;
const MAX_FINAL_P95_US: u64 = 120_000;
const CORPUS_SHA256: &str = "b70723e810ea53c39dff05d0bb746eb89e7dbeb76648c555e1330fbffbebe8f4";
const WAV_SHA256: &str = "ce0775c71a2bb748234a92a2c446997d17c299a56a04d38cfa43975fa6245ff3";
const EXPECTED_TRANSCRIPT: &str = "What is the capital of France?";
const PARTIAL_TIMING_BOUNDARY: &str = "16 kHz audio frame available before Silero inference through root TerminalVoiceInput write and flush to a live stderr TTY; model/session warmup excluded";
const FINAL_TIMING_BOUNDARY: &str = "Silero VAD close through exactly one final Transcript and root TerminalVoiceInput write and flush to a live stderr TTY; model/session warmup excluded";

#[derive(Serialize)]
struct TimingDistribution {
    sample_count: usize,
    threshold_us: u64,
    p50_us: u64,
    p95_us: u64,
    max_us: u64,
    samples_us: Vec<u64>,
}

#[derive(Serialize)]
struct NativePackages {
    cuda: String,
    cudnn: String,
    alsa_lib: String,
    pipewire: String,
}

#[derive(Serialize)]
struct HardwareEnvironment {
    gpu: String,
    driver: String,
    memory_mib: u64,
    os: &'static str,
    arch: &'static str,
    kernel: String,
    rustc: String,
    native_packages: NativePackages,
    input_backend: &'static str,
    input_device_id: &'static str,
    input_format: AudioFormat,
    source_samples: usize,
    source_duration_ms: u64,
    resampling_us: u64,
    presentation: &'static str,
    stderr_is_terminal: bool,
}

#[derive(Serialize)]
struct SttTimingProof {
    schema: &'static str,
    admitted_base: &'static str,
    partial_timing_boundary: &'static str,
    final_timing_boundary: &'static str,
    trial_count: usize,
    warmup_excluded: bool,
    corpus_sha256: &'static str,
    wav_sha256: &'static str,
    environment: HardwareEnvironment,
    endpoint: VadEndpoint,
    transcript: Transcript,
    utterance_duration_ms: u64,
    warmup_partial_hypotheses: Vec<String>,
    partial: TimingDistribution,
    final_flush: TimingDistribution,
    whisper_provenance: WhisperProvenance,
    whisper_metrics: WhisperMetrics,
    silero_provenance: SileroProvenance,
    silero_metrics: SileroMetrics,
    kokoro_provenance: KokoroProvenance,
    kokoro_metrics: KokoroMetrics,
    ort_runtime: OrtRuntimeMetrics,
}

struct TrialResult {
    partial_us: Vec<u64>,
    partial_hypotheses: Vec<String>,
    final_us: u64,
    endpoint: VadEndpoint,
    transcript: Transcript,
}

#[test]
#[ignore = "requires pinned Kokoro, Silero, and large-v3-turbo artifacts on an RTX 4090 stderr TTY"]
fn twenty_warm_rtx4090_live_partial_and_final_trials_meet_au4_bounds() {
    assert!(
        io::stderr().is_terminal(),
        "run live presentation proof in the named tmux TTY"
    );
    let fixture_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("crates/plato-audio/fixtures/au4");
    let samples = read_wav(&fixture_dir.join("speech-plus-noise.wav"));
    assert_eq!(samples.len(), 82_944);
    assert_eq!(samples.len() % SILERO_WINDOW_SAMPLES, 0);
    assert_eq!(
        sha256_file(&fixture_dir.join("speech-plus-noise.wav")),
        WAV_SHA256
    );

    let runtime = OrtRuntime::acquire().unwrap();
    let runtime_metrics = runtime.metrics_reader();
    let kokoro_dir = std::env::var_os("PLATO_AUDIO_KOKORO_DIR")
        .expect("PLATO_AUDIO_KOKORO_DIR must name the pinned artifact directory");
    let kokoro = KokoroSynthesizer::load_with_runtime(
        KokoroConfig::from_model_dir(kokoro_dir),
        runtime.clone(),
    )
    .unwrap();
    let kokoro_provenance = kokoro.provenance().clone();
    let kokoro_metrics = kokoro.metrics();

    let silero_path = std::env::var_os("PLATO_AUDIO_SILERO_MODEL")
        .expect("PLATO_AUDIO_SILERO_MODEL must name the pinned artifact");
    let mut detector =
        SileroVad::load_with_runtime(SileroConfig::new(silero_path), runtime).unwrap();
    let silero_provenance = detector.provenance().clone();
    let silero_metrics = detector.metrics_reader();
    assert_eq!(
        silero_provenance.ort_runtime_owner,
        kokoro_provenance.ort_runtime_owner
    );
    assert_eq!(runtime_metrics.snapshot().session_loads, 2);

    let whisper_path = std::env::var_os("PLATO_AUDIO_WHISPER_MODEL")
        .expect("PLATO_AUDIO_WHISPER_MODEL must name the pinned model");
    let mut recognizer = WhisperRecognizer::load(WhisperConfig::new(whisper_path)).unwrap();
    let whisper_provenance = recognizer.provenance().clone();
    let whisper_metrics = recognizer.metrics_reader();
    assert_eq!(whisper_provenance.model_sha256, WHISPER_MODEL_SHA256);

    let (warmup, partial_us, final_us, last) = {
        let stderr = io::stderr();
        let mut stderr = stderr.lock();
        let mut presentation = TerminalVoiceInput::new(&mut stderr);
        let warmup = run_trial(&samples, &mut detector, &mut recognizer, &mut presentation);
        assert_trial(&warmup);
        let mut partial_us = Vec::new();
        let mut final_us = Vec::with_capacity(TRIALS);
        let mut last = None;
        for _ in 0..TRIALS {
            let trial = run_trial(&samples, &mut detector, &mut recognizer, &mut presentation);
            assert_trial(&trial);
            assert_eq!(trial.endpoint, warmup.endpoint);
            assert_eq!(trial.partial_hypotheses, warmup.partial_hypotheses);
            partial_us.extend_from_slice(&trial.partial_us);
            final_us.push(trial.final_us);
            last = Some(trial);
        }
        (warmup, partial_us, final_us, last)
    };

    let partial = distribution(partial_us, MAX_PARTIAL_P95_US);
    let final_flush = distribution(final_us, MAX_FINAL_P95_US);
    assert!(partial.p95_us <= MAX_PARTIAL_P95_US);
    assert!(final_flush.p95_us <= MAX_FINAL_P95_US);

    let whisper_metrics = whisper_metrics.snapshot();
    let silero_metrics = silero_metrics.snapshot();
    assert_eq!(whisper_metrics.model_loads, 1);
    assert_eq!(whisper_metrics.finalizations, (TRIALS + 1) as u64);
    assert!(whisper_metrics.partial_decodes >= partial.sample_count as u64);
    assert_eq!(
        whisper_metrics.partial_updates,
        partial.sample_count as u64 + warmup.partial_us.len() as u64
    );
    assert_eq!(silero_metrics.session_loads, 1);
    assert_eq!(silero_metrics.state_resets, (TRIALS + 1) as u64);
    assert_eq!(
        silero_metrics.inference_frames,
        ((TRIALS + 1) * samples.len() / SILERO_WINDOW_SAMPLES) as u64
    );
    assert_eq!(kokoro_metrics.session_loads, 1);
    assert_eq!(kokoro_metrics.syntheses, 0);
    let ort_runtime = runtime_metrics.snapshot();
    assert_eq!(ort_runtime.environment_instances, 1);
    assert_eq!(ort_runtime.session_loads, 2);
    assert_eq!(ort_runtime.cuda_sessions, 2);
    assert_eq!(ort_runtime.cpu_sessions, 0);

    let last = last.expect("twenty trials produce a final result");
    let utterance_duration_ms = last.transcript.span_ms;
    let proof = SttTimingProof {
        schema: "plato_agent.au4_live_stt_timing.v1",
        admitted_base: "aca8304a768c519f379ff14f8ca1d515dde231a4",
        partial_timing_boundary: PARTIAL_TIMING_BOUNDARY,
        final_timing_boundary: FINAL_TIMING_BOUNDARY,
        trial_count: TRIALS,
        warmup_excluded: true,
        corpus_sha256: CORPUS_SHA256,
        wav_sha256: WAV_SHA256,
        environment: hardware_environment(samples.len()),
        endpoint: last.endpoint,
        transcript: last.transcript,
        utterance_duration_ms,
        warmup_partial_hypotheses: warmup.partial_hypotheses,
        partial,
        final_flush,
        whisper_provenance,
        whisper_metrics,
        silero_provenance,
        silero_metrics,
        kokoro_provenance,
        kokoro_metrics,
        ort_runtime,
    };
    println!(
        "AU4_STT_TIMING_PROOF={}",
        serde_json::to_string(&proof).unwrap()
    );
}

fn run_trial<W: io::Write>(
    samples: &[f32],
    detector: &mut dyn VoiceActivityDetector,
    recognizer: &mut WhisperRecognizer,
    presentation: &mut TerminalVoiceInput<W>,
) -> TrialResult {
    detector.reset();
    recognizer.reset();
    let mut vad = NeuralVadState::new(detector.frame_samples()).unwrap();
    let mut partial_us = Vec::new();
    let mut partial_hypotheses = Vec::new();
    let mut final_result = None;
    for frame in samples.chunks_exact(SILERO_WINDOW_SAMPLES) {
        let audio_available = Instant::now();
        for event in vad.push(frame, detector).unwrap() {
            match event {
                NeuralVadEvent::SpeechSamples(samples) => {
                    for transcript in recognize_samples(recognizer, &samples) {
                        let partial =
                            CapturePartial::new(transcript, duration_us(audio_available.elapsed()));
                        presentation.replace(&partial).unwrap();
                        partial_us.push(duration_us(audio_available.elapsed()));
                        partial_hypotheses.push(partial.transcript.text);
                    }
                }
                NeuralVadEvent::Segment(segment) => {
                    assert!(final_result.is_none(), "one endpoint must finalize once");
                    let close = Instant::now();
                    let transcript = recognizer.finalize().unwrap();
                    assert!(transcript.is_final);
                    assert_eq!(transcript.span_ms, segment.span_ms());
                    presentation.commit(&transcript).unwrap();
                    final_result =
                        Some((duration_us(close.elapsed()), segment.endpoint(), transcript));
                }
                NeuralVadEvent::RejectedTransient(endpoint) => {
                    panic!("annotated corpus produced a rejected transient at {endpoint:?}");
                }
            }
        }
    }
    let (final_us, endpoint, transcript) =
        final_result.expect("annotated corpus must produce one final endpoint");
    TrialResult {
        partial_us,
        partial_hypotheses,
        final_us,
        endpoint,
        transcript,
    }
}

fn recognize_samples(recognizer: &mut WhisperRecognizer, samples: &[f32]) -> Vec<Transcript> {
    let format = recognizer.input_format();
    let mut partials = Vec::new();
    for &sample in samples {
        let frame = PcmFrame::new(format, PcmData::F32(Box::new([sample]))).unwrap();
        for partial in recognizer.accept(&frame).unwrap() {
            assert!(!partial.is_final);
            partials.push(partial);
        }
    }
    partials
}

fn assert_trial(trial: &TrialResult) {
    assert!(!trial.partial_us.is_empty());
    assert_eq!(trial.transcript.text, EXPECTED_TRANSCRIPT);
    assert!(trial.transcript.is_final);
    assert_eq!(
        trial.endpoint,
        VadEndpoint {
            start_sample: 32_256,
            speech_end_sample: 60_416,
            close_sample: 64_512,
        }
    );
}

fn distribution(samples_us: Vec<u64>, threshold_us: u64) -> TimingDistribution {
    assert!(!samples_us.is_empty());
    let mut sorted = samples_us.clone();
    sorted.sort_unstable();
    TimingDistribution {
        sample_count: sorted.len(),
        threshold_us,
        p50_us: percentile(&sorted, 50),
        p95_us: percentile(&sorted, 95),
        max_us: *sorted.last().unwrap(),
        samples_us,
    }
}

fn hardware_environment(sample_count: usize) -> HardwareEnvironment {
    let gpu = command_output(
        "nvidia-smi",
        &[
            "--query-gpu=name,driver_version,memory.total",
            "--format=csv,noheader,nounits",
        ],
    );
    let fields = gpu.split(',').map(str::trim).collect::<Vec<_>>();
    assert_eq!(fields.len(), 3);
    assert_eq!(fields[0], "NVIDIA GeForce RTX 4090");
    HardwareEnvironment {
        gpu: fields[0].to_owned(),
        driver: fields[1].to_owned(),
        memory_mib: fields[2].parse().unwrap(),
        os: std::env::consts::OS,
        arch: std::env::consts::ARCH,
        kernel: command_output("uname", &["-srmo"]),
        rustc: command_output("rustc", &["--version"]),
        native_packages: NativePackages {
            cuda: package("cuda"),
            cudnn: package("cudnn"),
            alsa_lib: package("alsa-lib"),
            pipewire: package("pipewire"),
        },
        input_backend: "recorded WAV; no cpal device",
        input_device_id: "tracked:crates/plato-audio/fixtures/au4/speech-plus-noise.wav",
        input_format: AudioFormat::new(16_000, 1, SampleFormat::F32).unwrap(),
        source_samples: sample_count,
        source_duration_ms: sample_count as u64 * 1_000 / 16_000,
        resampling_us: 0,
        presentation: "root TerminalVoiceInput -> locked stderr in named tmux PTY",
        stderr_is_terminal: io::stderr().is_terminal(),
    }
}

fn package(name: &str) -> String {
    command_output("pacman", &["-Q", name])
}

fn command_output(program: &str, arguments: &[&str]) -> String {
    let output = Command::new(program)
        .args(arguments)
        .output()
        .unwrap_or_else(|error| panic!("cannot run {program}: {error}"));
    assert!(output.status.success(), "{program} returned {output:?}");
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
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

fn duration_us(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    let index = (sorted.len() * percentile).div_ceil(100).saturating_sub(1);
    sorted[index]
}
