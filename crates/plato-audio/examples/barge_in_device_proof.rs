use std::{
    error::Error,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use plato_audio::{
    AudioFormat, BargeInMetrics, CPAL_RUNTIME_VERSION, NeuralVadEvent, NeuralVadState, PcmChunk,
    PcmSink, PlaybackConfig, PlaybackDeviceInfo, PlaybackMetrics, RTRB_RUNTIME_VERSION,
    SILERO_WINDOW_SAMPLES, SampleFormat, Sentence, SileroConfig, SileroMetrics, SileroProvenance,
    SileroVad, SpeechSource, SpeechSynthesizer, SpokenInterruption, SynthError, SynthWorker,
    SynthWorkerShutdown, VoiceActivityDetector,
};
use serde::Serialize;

const TRIALS: usize = 25;
const STOP_LIMIT_US: u64 = 30_000;
const ADMITTED_BASE: &str = "efa9b0791e832438941dfcf932e4bd697269ec49";
const SOURCE_RATE: u32 = 24_000;
const SENTENCE_SECONDS: usize = 4;
const POLL_INTERVAL: Duration = Duration::from_millis(1);
const WAIT_LIMIT: Duration = Duration::from_secs(5);
const INPUT_KIND: &str = "recorded CC0 synthetic speech-plus-noise WAV fed directly to the resident Silero state after the playback gate; not a physical microphone, live speech, or cpal input-latency claim";
const TIMING_BOUNDARY: &str = "resident Silero minimum-speech decision completion through entry to the first actual-device cpal output callback that emits an entirely silent quantum";

struct ProofSynth {
    format: AudioFormat,
    samples: Vec<f32>,
}

impl SpeechSynthesizer for ProofSynth {
    fn output_format(&self) -> AudioFormat {
        self.format
    }

    fn synthesize(
        &mut self,
        _sentence: &Sentence,
        sink: &mut dyn PcmSink,
        cancel: &AtomicBool,
    ) -> Result<(), SynthError> {
        if cancel.load(Ordering::Acquire) {
            return Err(SynthError::Canceled);
        }
        sink.push(PcmChunk::from_f32(self.format, self.samples.clone())?)?;
        Ok(())
    }
}

#[derive(Serialize)]
struct ProofReport {
    schema: &'static str,
    admitted_base: &'static str,
    timing_boundary: &'static str,
    input_kind: &'static str,
    trial_count: usize,
    threshold_us: u64,
    p50_us: u64,
    p95_us: u64,
    max_us: u64,
    every_trial_within_threshold: bool,
    callback_quantum_frames: Vec<usize>,
    callback_quantum_us: Vec<u64>,
    self_playback_gate_ms: u64,
    output: PlaybackDeviceInfo,
    cpal_runtime: &'static str,
    rtrb_runtime: &'static str,
    silero: SileroProvenance,
    silero_metrics: SileroMetrics,
    shared_cancel_authority: bool,
    trials: Vec<TrialReport>,
    playback_before_shutdown: PlaybackMetrics,
    shutdown: SynthWorkerShutdown,
}

#[derive(Serialize)]
struct TrialReport {
    trial: usize,
    silero_decision_sample: u64,
    interruption: SpokenInterruption,
    metrics: BargeInMetrics,
}

fn main() -> Result<(), Box<dyn Error>> {
    let Some((model_path, fixture_path)) = arguments()? else {
        return Ok(());
    };
    let fixture = read_fixture(&fixture_path)?;
    let mut silero = SileroVad::load(SileroConfig::new(model_path))?;
    let silero_provenance = silero.provenance().clone();
    let silero_metrics = silero.metrics_reader();
    let format = AudioFormat::new(SOURCE_RATE, 1, SampleFormat::F32)?;
    let synth = ProofSynth {
        format,
        samples: tone_samples(),
    };
    let cancel = Arc::new(AtomicBool::new(false));
    let worker = SynthWorker::spawn(synth, PlaybackConfig::default(), Arc::clone(&cancel))?;
    let output = worker.device_info().clone();
    let shared_cancel_authority = worker.uses_cancel(&cancel);
    let barge_in = worker.barge_in_handle();
    let mut trials = Vec::with_capacity(TRIALS);

    for trial in 1..=TRIALS {
        worker.begin_run()?;
        for sentence_index in 0..plato_audio::SENTENCE_PREFETCH_CAPACITY {
            worker.try_accept(
                Sentence::new(
                    "one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen seventeen eighteen nineteen twenty twenty-one twenty-two twenty-three twenty-four twenty-five twenty-six twenty-seven twenty-eight twenty-nine thirty thirty-one thirty-two.",
                )?,
                SpeechSource::new(sentence_index as u64, sentence_index as u64),
            )?;
        }
        wait_for_gate(&barge_in)?;
        silero.reset();
        let decision_sample = decide_recorded_speech(&mut silero, &fixture, &barge_in)?;
        let reports = worker.wait_until_idle()?;
        if !reports.is_empty() {
            return Err(format!(
                "trial {trial} completed {} sentence(s) before barge-in",
                reports.len()
            )
            .into());
        }
        let interruption = worker
            .finish_run()?
            .ok_or_else(|| format!("trial {trial} produced no interruption latch"))?;
        let metrics = worker.barge_in_metrics();
        validate_trial(trial, &metrics, &interruption)?;
        trials.push(TrialReport {
            trial,
            silero_decision_sample: decision_sample,
            interruption,
            metrics,
        });
    }

    let mut latencies = trials
        .iter()
        .map(|trial| {
            trial
                .metrics
                .decision_to_silence_us
                .expect("validated trial has stop latency")
        })
        .collect::<Vec<_>>();
    latencies.sort_unstable();
    let callback_quantum_frames = unique_sorted(
        trials
            .iter()
            .map(|trial| {
                trial
                    .metrics
                    .silent_callback_frames
                    .expect("validated trial has callback quantum")
            })
            .collect(),
    );
    let callback_quantum_us = callback_quantum_frames
        .iter()
        .map(|frames| {
            u64::try_from(*frames)
                .unwrap_or(u64::MAX)
                .saturating_mul(1_000_000)
                / u64::from(output.format.sample_rate())
        })
        .collect();
    let playback_before_shutdown = worker.playback_metrics();
    let shutdown = worker.shutdown()?;
    let report = ProofReport {
        schema: "plato_audio.au5_barge_in_device.v1",
        admitted_base: ADMITTED_BASE,
        timing_boundary: TIMING_BOUNDARY,
        input_kind: INPUT_KIND,
        trial_count: trials.len(),
        threshold_us: STOP_LIMIT_US,
        p50_us: percentile(&latencies, 50),
        p95_us: percentile(&latencies, 95),
        max_us: *latencies.last().expect("twenty-five trials are present"),
        every_trial_within_threshold: latencies.iter().all(|latency| *latency <= STOP_LIMIT_US),
        callback_quantum_frames,
        callback_quantum_us,
        self_playback_gate_ms: plato_audio::SELF_PLAYBACK_GATE_MS,
        output,
        cpal_runtime: CPAL_RUNTIME_VERSION,
        rtrb_runtime: RTRB_RUNTIME_VERSION,
        silero: silero_provenance,
        silero_metrics: silero_metrics.snapshot(),
        shared_cancel_authority,
        trials,
        playback_before_shutdown,
        shutdown,
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn wait_for_gate(barge_in: &plato_audio::BargeInHandle) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + WAIT_LIMIT;
    while !barge_in.gate_open() {
        if Instant::now() >= deadline {
            return Err("actual output did not reach the 150 ms playback gate".into());
        }
        thread::sleep(POLL_INTERVAL);
    }
    Ok(())
}

fn decide_recorded_speech(
    silero: &mut SileroVad,
    fixture: &[f32],
    barge_in: &plato_audio::BargeInHandle,
) -> Result<u64, Box<dyn Error>> {
    let mut vad = NeuralVadState::new(silero.frame_samples())?;
    for samples in fixture.chunks(SILERO_WINDOW_SAMPLES) {
        for event in vad.push(samples, silero)? {
            if let NeuralVadEvent::SpeechOnset {
                decision_sample, ..
            } = event
            {
                if !barge_in.trigger_speech_onset() {
                    return Err("Silero decided speech while the playback gate was closed".into());
                }
                return Ok(decision_sample);
            }
        }
    }
    Err("recorded fixture produced no minimum-speech Silero decision".into())
}

fn validate_trial(
    trial: usize,
    metrics: &BargeInMetrics,
    interruption: &SpokenInterruption,
) -> Result<(), Box<dyn Error>> {
    let latency = metrics
        .decision_to_silence_us
        .ok_or_else(|| format!("trial {trial} observed no all-silent callback"))?;
    if latency > STOP_LIMIT_US {
        return Err(format!(
            "trial {trial} stop latency was {latency} us, above {STOP_LIMIT_US} us"
        )
        .into());
    }
    if !metrics.gate_open_at_decision
        || metrics.queued_pcm_frames_at_decision == 0
        || metrics.queued_sentences_at_decision == 0
        || metrics.silent_callback_frames.is_none()
        || metrics.sentence_queue_flushes != 1
        || metrics.pcm_queue_flushes != 1
    {
        return Err(format!("trial {trial} failed queue/gate/callback proof: {metrics:?}").into());
    }
    let callback_frames = metrics
        .silent_callback_frames
        .expect("validated callback quantum is present") as u64;
    if interruption.spoken_prefix.is_empty()
        || interruption.sentence_index != 0
        || interruption.assistant_delta_index != 0
        || interruption.played_samples < metrics.played_frames_at_decision
        || interruption.played_samples
            > metrics
                .played_frames_at_decision
                .saturating_add(callback_frames)
    {
        return Err(format!(
            "trial {trial} failed sample-derived interruption proof: {interruption:?}"
        )
        .into());
    }
    Ok(())
}

fn read_fixture(path: &PathBuf) -> Result<Vec<f32>, Box<dyn Error>> {
    let mut reader = hound::WavReader::open(path)?;
    let spec = reader.spec();
    if spec.sample_rate != 16_000 || spec.channels != 1 || spec.bits_per_sample != 16 {
        return Err(format!("fixture must be 16 kHz mono signed 16-bit PCM, got {spec:?}").into());
    }
    reader
        .samples::<i16>()
        .map(|sample| sample.map(|sample| f32::from(sample) / 32_768.0))
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn tone_samples() -> Vec<f32> {
    let frames = SOURCE_RATE as usize * SENTENCE_SECONDS;
    (0..frames)
        .map(|frame| {
            let phase = frame as f32 * 440.0 * std::f32::consts::TAU / SOURCE_RATE as f32;
            phase.sin() * 0.08
        })
        .collect()
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    let rank = (percentile * sorted.len()).div_ceil(100);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn unique_sorted<T: Ord>(mut values: Vec<T>) -> Vec<T> {
    values.sort_unstable();
    values.dedup();
    values
}

fn arguments() -> Result<Option<(PathBuf, PathBuf)>, Box<dyn Error>> {
    let mut model_path = std::env::var_os("PLATO_AUDIO_SILERO_MODEL").map(PathBuf::from);
    let mut fixture_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/au4/speech-plus-noise.wav");
    let mut arguments = std::env::args().skip(1);
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--silero-model" => {
                model_path = Some(PathBuf::from(
                    arguments.next().ok_or("--silero-model requires a path")?,
                ));
            }
            "--fixture" => {
                fixture_path = PathBuf::from(arguments.next().ok_or("--fixture requires a path")?);
            }
            "-h" | "--help" => {
                println!(
                    "Usage: barge_in_device_proof --silero-model PATH [--fixture WAV]\n\
                     \n\
                     PLATO_AUDIO_SILERO_MODEL may provide PATH. Runs 25 recorded-input Silero\n\
                     decisions against one real persistent output device; no microphone is claimed."
                );
                return Ok(None);
            }
            unknown => return Err(format!("unknown argument {unknown:?}").into()),
        }
    }
    let model_path = model_path.ok_or(
        "provide --silero-model PATH or set PLATO_AUDIO_SILERO_MODEL to the pinned artifact",
    )?;
    Ok(Some((model_path, fixture_path)))
}
