use std::{
    fs,
    io::{self, IsTerminal},
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use plato_audio::{
    AudioFormat, CaptureConfig, CaptureDeviceInfo, CaptureMetrics, CaptureWorker,
    CaptureWorkerShutdown, InputDeviceSelection, KokoroConfig, KokoroMetrics, KokoroProvenance,
    KokoroSynthesizer, OrtRuntime, OrtRuntimeMetrics, SileroConfig, SileroMetrics,
    SileroProvenance, SileroVad, Transcript, VadEndpoint, WHISPER_MODEL_SHA256, WhisperConfig,
    WhisperMetrics, WhisperProvenance, WhisperRecognizer,
};
use serde::Serialize;

use super::TerminalVoiceInput;

const TRIALS: usize = 20;
const SOURCE_REPETITIONS: usize = 24;
const MAX_PARTIAL_P95_US: u64 = 200_000;
const MAX_FINAL_P95_US: u64 = 120_000;
const SILERO_FRAME_SAMPLES: u64 = 512;
const SILERO_HANGOVER_SAMPLES: u64 = 4_096;
const MAX_UTTERANCE_SAMPLES: u64 = 30 * 16_000;
const CORPUS_SHA256: &str = "b70723e810ea53c39dff05d0bb746eb89e7dbeb76648c555e1330fbffbebe8f4";
const WAV_SHA256: &str = "ce0775c71a2bb748234a92a2c446997d17c299a56a04d38cfa43975fa6245ff3";
const RAW_SHA256: &str = "b55089e93fce31bdb40af141a22f9d8b3380a81ad28426244d68f73cc6d26fa6";
const EXPECTED_TRANSCRIPT: &str = "What is the capital of France?";
const INPUT_DEVICE_ID: &str = "alsa:pulse";
const PULSE_SINK: &str = "plato_au4_timing";
const PULSE_SOURCE: &str = "plato_au4_timing.monitor";
const PARTIAL_TIMING_BOUNDARY: &str = "entry to the cpal input callback for the earliest drained native frame through rtrb enqueue and wait, CaptureWorker normalization/resampling, Silero and bounded Whisper processing, worker delivery, and root TerminalVoiceInput write and flush to a real stderr TTY; model/session warmup and all ALSA/hardware time before callback entry excluded";
const FINAL_TIMING_BOUNDARY: &str = "entry to the closing CaptureWorker vad.push evaluation through the Silero close decision, every ordered close-batch speech/partial event, bounded final-window Whisper decode, worker delivery, and root TerminalVoiceInput final write and flush to a real stderr TTY; a conservative upper bound rather than the exact internal close instant; model/session warmup excluded";
const RECORDED_INPUT_BOUNDARY: &str = "24 repeated copies of the tracked CC0 WAV are paced by pacat into a named PipeWire/Pulse null-sink monitor and captured through cpal alsa:pulse plus the real root TTY; this is virtual recorded input, not a physical microphone or live human speech";

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
struct PhysicalInputInventory {
    alsa_capture_paths: [&'static str; 2],
    idle_levels: [&'static str; 2],
    pipewire_sources: &'static str,
    pactl_sources: &'static str,
    configured_usb_volt: &'static str,
    physical_spoken_signal_proven: bool,
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
    input: CaptureDeviceInfo,
    input_device_id: &'static str,
    pulse_sink: &'static str,
    pulse_source: &'static str,
    pulse_module_id: u32,
    pulse_feeder_pid: u32,
    recorded_payload_path: PathBuf,
    recorded_payload_sha256: &'static str,
    recorded_payload_repetitions: usize,
    source_format: AudioFormat,
    source_samples_per_repetition: usize,
    source_duration_ms_per_repetition: u64,
    presentation: &'static str,
    stderr_is_terminal: bool,
    physical_input_inventory: PhysicalInputInventory,
}

#[derive(Serialize)]
struct SttTimingProof {
    schema: &'static str,
    admitted_base: &'static str,
    recorded_input_boundary: &'static str,
    partial_timing_boundary: &'static str,
    final_timing_boundary: &'static str,
    trial_count: usize,
    warmup_excluded: bool,
    corpus_sha256: &'static str,
    wav_sha256: &'static str,
    environment: HardwareEnvironment,
    endpoint: VadEndpoint,
    endpoint_duration_ms: u64,
    transcript: Transcript,
    warmup_partial_hypotheses: Vec<String>,
    partial_visible: TimingDistribution,
    final_visible: TimingDistribution,
    worker_final_construction: TimingDistribution,
    capture_metrics: CaptureMetrics,
    capture_shutdown: CaptureWorkerShutdown,
    whisper_provenance: WhisperProvenance,
    whisper_metrics: WhisperMetrics,
    silero_provenance: SileroProvenance,
    silero_metrics: SileroMetrics,
    kokoro_provenance: KokoroProvenance,
    kokoro_metrics: KokoroMetrics,
    ort_runtime: OrtRuntimeMetrics,
}

struct TrialResult {
    partial_visible_us: Vec<u64>,
    partial_hypotheses: Vec<String>,
    final_visible_us: u64,
    worker_final_us: u64,
    endpoint: VadEndpoint,
    transcript: Transcript,
}

#[test]
#[ignore = "requires pinned models, named PipeWire/Pulse virtual source and feeder, RTX 4090, and stderr TTY"]
fn twenty_warm_rtx4090_live_partial_and_final_trials_meet_au4_bounds() {
    assert!(
        io::stderr().is_terminal(),
        "run live presentation proof in the named tmux TTY"
    );
    let fixture_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("crates/plato-audio/fixtures/au4");
    let wav_path = fixture_dir.join("speech-plus-noise.wav");
    assert_eq!(sha256_file(&wav_path), WAV_SHA256);
    let source_pcm = wav_pcm_bytes(&wav_path);
    assert_eq!(source_pcm.len(), 82_944 * 2);
    let raw_path = PathBuf::from(
        std::env::var_os("PLATO_AUDIO_RECORDED_FIXTURE_RAW")
            .expect("PLATO_AUDIO_RECORDED_FIXTURE_RAW must name the repeated raw fixture"),
    );
    assert_repeated_raw(&raw_path, &source_pcm);
    assert_eq!(std::env::var("PULSE_SOURCE").as_deref(), Ok(PULSE_SOURCE));
    let pulse_module_id = environment_u32("PLATO_AUDIO_PULSE_MODULE_ID");
    let pulse_feeder_pid = environment_u32("PLATO_AUDIO_PULSE_FEEDER_PID");
    assert!(
        command_output("pactl", &["list", "short", "sources"]).contains(PULSE_SOURCE),
        "named virtual source must remain live throughout proof"
    );
    assert!(
        Command::new("ps")
            .args(["-p", &pulse_feeder_pid.to_string()])
            .status()
            .unwrap()
            .success(),
        "paced pacat feeder must remain live throughout proof"
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
    let detector = SileroVad::load_with_runtime(SileroConfig::new(silero_path), runtime).unwrap();
    let silero_provenance = detector.provenance().clone();
    let silero_metrics = detector.metrics_reader();
    assert_eq!(
        silero_provenance.ort_runtime_owner,
        kokoro_provenance.ort_runtime_owner
    );
    assert_eq!(runtime_metrics.snapshot().session_loads, 2);

    let whisper_path = std::env::var_os("PLATO_AUDIO_WHISPER_MODEL")
        .expect("PLATO_AUDIO_WHISPER_MODEL must name the pinned model");
    let recognizer = WhisperRecognizer::load(WhisperConfig::new(whisper_path)).unwrap();
    let whisper_provenance = recognizer.provenance().clone();
    let whisper_metrics = recognizer.metrics_reader();
    assert_eq!(whisper_provenance.model_sha256, WHISPER_MODEL_SHA256);

    let capture = CaptureWorker::open(
        CaptureConfig::for_device(InputDeviceSelection::Id(INPUT_DEVICE_ID.to_owned())),
        detector,
        recognizer,
    )
    .unwrap();
    let input = capture.device_info().clone();
    assert_eq!(input.backend, "ALSA");
    assert_eq!(input.device_id, INPUT_DEVICE_ID);

    let (warmup, partial_us, final_us, worker_final_us, last) = {
        let stderr = io::stderr();
        let mut stderr = stderr.lock();
        let mut presentation = TerminalVoiceInput::new(&mut stderr);
        let warmup = run_trial(&capture, &mut presentation);
        assert_trial(&warmup);
        let mut partial_us = Vec::new();
        let mut final_us = Vec::with_capacity(TRIALS);
        let mut worker_final_us = Vec::with_capacity(TRIALS);
        let mut last = None;
        for _ in 0..TRIALS {
            let trial = run_trial(&capture, &mut presentation);
            assert_trial(&trial);
            partial_us.extend_from_slice(&trial.partial_visible_us);
            final_us.push(trial.final_visible_us);
            worker_final_us.push(trial.worker_final_us);
            last = Some(trial);
        }
        (warmup, partial_us, final_us, worker_final_us, last)
    };

    let partial_visible = distribution(partial_us, MAX_PARTIAL_P95_US);
    let final_visible = distribution(final_us, MAX_FINAL_P95_US);
    let worker_final_construction = distribution(worker_final_us, MAX_FINAL_P95_US);
    assert!(partial_visible.p95_us <= MAX_PARTIAL_P95_US);
    assert!(final_visible.p95_us <= MAX_FINAL_P95_US);

    let capture_metrics = capture.metrics();
    assert_eq!(capture_metrics.stream_opens, 1);
    assert_eq!(capture_metrics.worker_threads, 1);
    assert_eq!(capture_metrics.transcripts, (TRIALS + 1) as u64);
    assert_eq!(capture_metrics.overflow.samples, 0);
    assert!(capture_metrics.normalization_resampling_us > 0);
    let capture_shutdown = capture.shutdown();
    assert!(capture_shutdown.worker_joined);
    assert!(capture_shutdown.input_closed);
    assert!(!capture_shutdown.worker_panicked);

    let whisper_metrics = whisper_metrics.snapshot();
    let silero_metrics = silero_metrics.snapshot();
    assert_eq!(whisper_metrics.model_loads, 1);
    assert_eq!(whisper_metrics.finalizations, (TRIALS + 1) as u64);
    assert!(whisper_metrics.partial_decodes >= partial_visible.sample_count as u64);
    assert_eq!(
        whisper_metrics.partial_updates,
        partial_visible.sample_count as u64 + warmup.partial_visible_us.len() as u64
    );
    assert_eq!(silero_metrics.session_loads, 1);
    assert_eq!(silero_metrics.state_resets, (TRIALS + 1) as u64);
    assert!(silero_metrics.inference_frames > 0);
    assert_eq!(kokoro_metrics.session_loads, 1);
    assert_eq!(kokoro_metrics.syntheses, 0);
    let ort_runtime = runtime_metrics.snapshot();
    assert_eq!(ort_runtime.environment_instances, 1);
    assert_eq!(ort_runtime.session_loads, 2);
    assert_eq!(ort_runtime.cuda_sessions, 2);
    assert_eq!(ort_runtime.cpu_sessions, 0);

    let last = last.expect("twenty trials produce a final result");
    let endpoint_duration_ms = endpoint_duration_ms(last.endpoint);
    let proof = SttTimingProof {
        schema: "plato_agent.au4_live_stt_timing.v2",
        admitted_base: "aca8304a768c519f379ff14f8ca1d515dde231a4",
        recorded_input_boundary: RECORDED_INPUT_BOUNDARY,
        partial_timing_boundary: PARTIAL_TIMING_BOUNDARY,
        final_timing_boundary: FINAL_TIMING_BOUNDARY,
        trial_count: TRIALS,
        warmup_excluded: true,
        corpus_sha256: CORPUS_SHA256,
        wav_sha256: WAV_SHA256,
        environment: hardware_environment(input, raw_path, pulse_module_id, pulse_feeder_pid),
        endpoint: last.endpoint,
        endpoint_duration_ms,
        transcript: last.transcript,
        warmup_partial_hypotheses: warmup.partial_hypotheses,
        partial_visible,
        final_visible,
        worker_final_construction,
        capture_metrics,
        capture_shutdown,
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
    capture: &CaptureWorker,
    presentation: &mut TerminalVoiceInput<W>,
) -> TrialResult {
    let mut partial_visible_us = Vec::new();
    let mut partial_hypotheses = Vec::new();
    let report = capture
        .capture_with_partials(Duration::from_secs(8), |partial| {
            presentation.replace(partial).unwrap();
            partial_visible_us.push(partial.observed_latency_us());
            partial_hypotheses.push(partial.transcript.text.clone());
        })
        .expect("production virtual capture must complete");
    presentation.commit(&report.transcript).unwrap();
    let final_visible_us = report.observed_final_latency_us();
    TrialResult {
        partial_visible_us,
        partial_hypotheses,
        final_visible_us,
        worker_final_us: report.vad_close_to_final_us,
        endpoint: report.endpoint,
        transcript: report.transcript,
    }
}

fn assert_trial(trial: &TrialResult) {
    assert!(!trial.partial_visible_us.is_empty());
    assert_eq!(trial.transcript.text, EXPECTED_TRANSCRIPT);
    assert!(trial.transcript.is_final);
    let speech_span = trial
        .endpoint
        .speech_end_sample
        .saturating_sub(trial.endpoint.start_sample);
    let close_span = trial
        .endpoint
        .close_sample
        .saturating_sub(trial.endpoint.start_sample);
    assert!(trial.endpoint.start_sample < trial.endpoint.speech_end_sample);
    assert!(trial.endpoint.speech_end_sample < trial.endpoint.close_sample);
    assert_eq!(trial.endpoint.start_sample % SILERO_FRAME_SAMPLES, 0);
    assert_eq!(trial.endpoint.speech_end_sample % SILERO_FRAME_SAMPLES, 0);
    assert_eq!(trial.endpoint.close_sample % SILERO_FRAME_SAMPLES, 0);
    assert_eq!(speech_span % SILERO_FRAME_SAMPLES, 0);
    assert_eq!(
        trial
            .endpoint
            .close_sample
            .saturating_sub(trial.endpoint.speech_end_sample),
        SILERO_HANGOVER_SAMPLES
    );
    assert_eq!(close_span, speech_span + SILERO_HANGOVER_SAMPLES);
    assert!(close_span < MAX_UTTERANCE_SAMPLES);
    assert_eq!(
        trial.transcript.span_ms,
        endpoint_duration_ms(trial.endpoint)
    );
    assert!(trial.final_visible_us >= trial.worker_final_us);
}

fn endpoint_duration_ms(endpoint: VadEndpoint) -> u64 {
    endpoint
        .close_sample
        .saturating_sub(endpoint.start_sample)
        .saturating_mul(1_000)
        / 16_000
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

fn hardware_environment(
    input: CaptureDeviceInfo,
    raw_path: PathBuf,
    pulse_module_id: u32,
    pulse_feeder_pid: u32,
) -> HardwareEnvironment {
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
        input,
        input_device_id: INPUT_DEVICE_ID,
        pulse_sink: PULSE_SINK,
        pulse_source: PULSE_SOURCE,
        pulse_module_id,
        pulse_feeder_pid,
        recorded_payload_path: raw_path,
        recorded_payload_sha256: RAW_SHA256,
        recorded_payload_repetitions: SOURCE_REPETITIONS,
        source_format: AudioFormat::new(16_000, 1, plato_audio::SampleFormat::I16).unwrap(),
        source_samples_per_repetition: 82_944,
        source_duration_ms_per_repetition: 5_184,
        presentation: "root TerminalVoiceInput -> locked stderr in named tmux PTY",
        stderr_is_terminal: io::stderr().is_terminal(),
        physical_input_inventory: PhysicalInputInventory {
            alsa_capture_paths: [
                "PCH hw:1,0 ALC1220 Analog (48 kHz stereo S16_LE opens)",
                "PCH hw:1,2 ALC1220 Alt Analog (48 kHz stereo S16_LE opens)",
            ],
            idle_levels: [
                "hw:1,0 two-second idle mean -78.8 dB, max -65.7 dB",
                "hw:1,2 two-second idle mean/max -91 dB",
            ],
            pipewire_sources: "before virtual proof: no physical Source nodes",
            pactl_sources: "before virtual proof: HDMI monitor only; not used as a microphone",
            configured_usb_volt: "absent",
            physical_spoken_signal_proven: false,
        },
    }
}

fn environment_u32(name: &str) -> u32 {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("{name} must be set for the virtual-source proof"))
        .parse()
        .unwrap_or_else(|error| panic!("{name} must be an integer: {error}"))
}

fn assert_repeated_raw(path: &Path, source_pcm: &[u8]) {
    let raw = fs::read(path).expect("repeated raw fixture must be readable");
    assert_eq!(sha256_bytes(&raw), RAW_SHA256);
    assert_eq!(raw.len(), source_pcm.len() * SOURCE_REPETITIONS);
    assert!(
        raw.chunks_exact(source_pcm.len())
            .all(|copy| copy == source_pcm)
    );
}

fn wav_pcm_bytes(path: &Path) -> Vec<u8> {
    let mut reader = hound::WavReader::open(path).expect("fixture WAV must be readable");
    let spec = reader.spec();
    assert_eq!(spec.sample_rate, 16_000);
    assert_eq!(spec.channels, 1);
    assert_eq!(spec.bits_per_sample, 16);
    assert_eq!(spec.sample_format, hound::SampleFormat::Int);
    reader
        .samples::<i16>()
        .flat_map(|sample| sample.expect("fixture sample must decode").to_le_bytes())
        .collect()
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

fn sha256_file(path: &Path) -> String {
    sha256_bytes(&fs::read(path).unwrap())
}

fn sha256_bytes(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(bytes))
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    let index = (sorted.len() * percentile).div_ceil(100).saturating_sub(1);
    sorted[index]
}
