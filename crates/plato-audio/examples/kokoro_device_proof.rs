use std::{
    error::Error,
    path::PathBuf,
    process::Command,
    sync::{Arc, atomic::AtomicBool},
};

use plato_audio::{
    AudioFormat, CPAL_RUNTIME_VERSION, InferenceBackend, KokoroConfig, KokoroMetrics,
    KokoroProvenance, KokoroSynthesizer, PlaybackConfig, PlaybackDeviceInfo, PlaybackMetrics,
    PlaybackReport, PlaybackUnderrun, RTRB_RUNTIME_VERSION, RUBATO_RUNTIME_VERSION,
    SENTENCE_PREFETCH_CAPACITY, Sentence, SynthWorker, SynthWorkerShutdown,
};
use serde::Serialize;

const TRIALS: usize = 20;
const MAX_TTFA_P95_US: u64 = 350_000;
const MAX_GAP_US: u64 = 20_000;
const DEFAULT_SENTENCE: &str = "Plato speaks this complete warm sentence.";
const TIMING_BOUNDARY: &str = "sentence accepted into the fixed four-job window through the first non-silent sample copied by the persistent cpal output callback; model load and stream open excluded";
const MULTI_SENTENCE_CORPUS: [&str; SENTENCE_PREFETCH_CAPACITY] = [
    "Prefetch keeps this first sentence playing for the listener.",
    "The second sentence synthesizes while the first one is still audible.",
    "A third sentence follows through the same persistent device stream.",
    "The final sentence proves exact order and a clean bounded drain.",
];

#[derive(Serialize)]
struct ProofReport {
    schema: &'static str,
    timing_boundary: &'static str,
    ttfa: LatencyMetric,
    multi_sentence: MultiSentenceMetric,
    model_format: AudioFormat,
    provenance: KokoroProvenance,
    cpal_runtime: &'static str,
    rtrb_runtime: &'static str,
    rubato_runtime: &'static str,
    output: PlaybackDeviceInfo,
    observed_device_period_us: Vec<u64>,
    accelerator: AcceleratorInfo,
    kokoro_metrics: KokoroMetrics,
    shutdown: SynthWorkerShutdown,
    warmup_excluded: bool,
}

#[derive(Serialize)]
struct LatencyMetric {
    trial_count: usize,
    threshold_us: u64,
    p50_us: u64,
    p95_us: u64,
    max_us: u64,
    trials: Vec<PlaybackReport>,
}

#[derive(Serialize)]
struct MultiSentenceMetric {
    capacity: usize,
    corpus: Vec<String>,
    callback_sample_timestamps: Vec<PlaybackReport>,
    gaps_us: Vec<u64>,
    p50_us: u64,
    p95_us: u64,
    max_us: u64,
    threshold_us: u64,
    overlaps: Vec<OverlapProof>,
    every_boundary_within_threshold: bool,
    demonstrated_synthesis_playback_overlap: bool,
    underrun: PlaybackUnderrun,
    metrics_before_shutdown: PlaybackMetrics,
}

#[derive(Serialize)]
struct OverlapProof {
    playing_sequence: u64,
    following_sequence: u64,
    playing_first_pcm_ns: u64,
    playing_pcm_end_ns: u64,
    following_synth_started_ns: u64,
    following_synth_finished_ns: u64,
    overlap_us: u64,
}

#[derive(Serialize)]
struct AcceleratorInfo {
    name: String,
    driver_version: String,
    memory_mib: u64,
}

fn main() -> Result<(), Box<dyn Error>> {
    let Some((model_dir, sentence)) = arguments()? else {
        return Ok(());
    };
    let sentence = Sentence::new(sentence)?;
    let cancel = Arc::new(AtomicBool::new(false));
    let synthesizer = KokoroSynthesizer::load(KokoroConfig::from_model_dir(model_dir))?;
    if synthesizer.provenance().backend != InferenceBackend::Cuda {
        return Err(format!(
            "device proof requires the admitted CUDA backend; CPU fallback was selected: {}",
            synthesizer
                .provenance()
                .fallback_reason
                .as_deref()
                .unwrap_or("CUDA fallback reason unavailable")
        )
        .into());
    }
    let provenance = synthesizer.provenance().clone();
    let model_format = plato_audio::SpeechSynthesizer::output_format(&synthesizer);
    let metrics_reader = synthesizer.metrics_reader();
    let worker = SynthWorker::spawn(synthesizer, PlaybackConfig::default(), Arc::clone(&cancel))?;
    worker.begin_run()?;
    let output = worker.device_info().clone();
    let accelerator = accelerator_info()?;

    let mut source_index = 0_u64;
    let warmup_admission = worker.accept(
        sentence.clone(),
        plato_audio::SpeechSource::new(source_index, source_index),
    )?;
    source_index += 1;
    let mut warmup = warmup_admission.completed;
    warmup.extend(worker.wait_until_idle()?);
    if warmup.len() != 1 {
        return Err(format!("warmup returned {} reports; expected one", warmup.len()).into());
    }

    let mut trials = Vec::with_capacity(TRIALS);
    for _ in 0..TRIALS {
        let admission = worker.accept(
            sentence.clone(),
            plato_audio::SpeechSource::new(source_index, source_index),
        )?;
        source_index += 1;
        let mut report = admission.completed;
        report.extend(worker.wait_until_idle()?);
        if report.len() != 1 {
            return Err(
                format!("TTFA trial returned {} reports; expected one", report.len()).into(),
            );
        }
        trials.push(report.pop().expect("one report was checked").playback);
    }
    let ttfa_values = trials
        .iter()
        .map(|trial| trial.accepted_to_first_non_silent_us)
        .collect::<Vec<_>>();
    let ttfa = metric(ttfa_values, MAX_TTFA_P95_US, trials);

    let mut multi_reports = Vec::with_capacity(SENTENCE_PREFETCH_CAPACITY);
    for text in MULTI_SENTENCE_CORPUS {
        let admission = worker.try_accept(
            Sentence::new(text)?,
            plato_audio::SpeechSource::new(source_index, source_index),
        )?;
        source_index += 1;
        multi_reports.extend(admission.completed);
    }
    multi_reports.extend(worker.wait_until_idle()?);
    let corpus = multi_reports
        .iter()
        .map(|report| report.sentence.clone())
        .collect::<Vec<_>>();
    if corpus != MULTI_SENTENCE_CORPUS {
        return Err(format!(
            "multi-sentence order mismatch: expected {MULTI_SENTENCE_CORPUS:?}, got {corpus:?}"
        )
        .into());
    }
    let callback_sample_timestamps = multi_reports
        .iter()
        .map(|report| report.playback)
        .collect::<Vec<_>>();
    let gaps_us = callback_sample_timestamps
        .iter()
        .skip(1)
        .map(|report| {
            report
                .gap_before_us
                .expect("adjacent sentence has a prior boundary")
        })
        .collect::<Vec<_>>();
    let overlaps = callback_sample_timestamps
        .windows(2)
        .map(|pair| {
            let overlap_start = pair[0].first_pcm_ns.max(pair[1].synth_started_ns);
            let overlap_end = pair[0].pcm_end_ns.min(pair[1].synth_finished_ns);
            OverlapProof {
                playing_sequence: pair[0].sequence,
                following_sequence: pair[1].sequence,
                playing_first_pcm_ns: pair[0].first_pcm_ns,
                playing_pcm_end_ns: pair[0].pcm_end_ns,
                following_synth_started_ns: pair[1].synth_started_ns,
                following_synth_finished_ns: pair[1].synth_finished_ns,
                overlap_us: overlap_end.saturating_sub(overlap_start) / 1_000,
            }
        })
        .collect::<Vec<_>>();
    let every_boundary_within_threshold = gaps_us.iter().all(|gap| *gap <= MAX_GAP_US);
    let demonstrated_synthesis_playback_overlap = overlaps.iter().any(|proof| proof.overlap_us > 0);
    let multi_sentence_underrun =
        callback_sample_timestamps
            .iter()
            .fold(PlaybackUnderrun::default(), |mut total, report| {
                total.callbacks = total.callbacks.saturating_add(report.underrun.callbacks);
                total.frames = total.frames.saturating_add(report.underrun.frames);
                total
            });
    let mut sorted_gaps = gaps_us.clone();
    sorted_gaps.sort_unstable();
    let multi_sentence = MultiSentenceMetric {
        capacity: SENTENCE_PREFETCH_CAPACITY,
        corpus,
        callback_sample_timestamps,
        gaps_us,
        p50_us: percentile(&sorted_gaps, 50),
        p95_us: percentile(&sorted_gaps, 95),
        max_us: *sorted_gaps.last().expect("four sentences have three gaps"),
        threshold_us: MAX_GAP_US,
        overlaps,
        every_boundary_within_threshold,
        demonstrated_synthesis_playback_overlap,
        underrun: multi_sentence_underrun,
        metrics_before_shutdown: worker.playback_metrics(),
    };
    let observed_device_period_us = ttfa
        .trials
        .iter()
        .chain(multi_sentence.callback_sample_timestamps.iter())
        .map(|report| {
            u64::try_from(report.first_callback_frames)
                .unwrap_or(u64::MAX)
                .saturating_mul(1_000_000)
                / u64::from(output.format.sample_rate())
        })
        .fold(Vec::new(), |mut periods, period| {
            if !periods.contains(&period) {
                periods.push(period);
            }
            periods
        });
    let kokoro_metrics = metrics_reader.snapshot();
    let shutdown = worker.shutdown()?;

    if kokoro_metrics.session_loads != 1
        || kokoro_metrics.syntheses != (1 + TRIALS + SENTENCE_PREFETCH_CAPACITY) as u64
    {
        return Err(format!("warm model reuse assertion failed: {kokoro_metrics:?}").into());
    }
    if shutdown.playback.stream_opens != 1
        || shutdown.playback.chunks_played != (1 + TRIALS + SENTENCE_PREFETCH_CAPACITY) as u64
        || shutdown.playback.max_accepted_unfinished != SENTENCE_PREFETCH_CAPACITY
        || shutdown.synth_worker_threads != 1
        || shutdown.resampling_plan_builds != 1
        || !shutdown.worker_joined
        || !shutdown.playback_closed
    {
        return Err(format!(
            "persistent bounded playback assertion failed: {:?}",
            shutdown.playback
        )
        .into());
    }
    if ttfa.p95_us > MAX_TTFA_P95_US {
        return Err(format!(
            "warm TTFA p95 was {} us, above the admitted {} us limit",
            ttfa.p95_us, MAX_TTFA_P95_US
        )
        .into());
    }
    if !multi_sentence.every_boundary_within_threshold {
        return Err(format!(
            "inter-sentence gaps {:?} exceeded the admitted {} us limit",
            multi_sentence.gaps_us, MAX_GAP_US
        )
        .into());
    }
    if !multi_sentence.demonstrated_synthesis_playback_overlap {
        return Err("no synthesis N+1 interval overlapped playback N".into());
    }

    let report = ProofReport {
        schema: "plato_audio.audio_prefetch_device.v2",
        timing_boundary: TIMING_BOUNDARY,
        ttfa,
        multi_sentence,
        model_format,
        provenance,
        cpal_runtime: CPAL_RUNTIME_VERSION,
        rtrb_runtime: RTRB_RUNTIME_VERSION,
        rubato_runtime: RUBATO_RUNTIME_VERSION,
        output,
        observed_device_period_us,
        accelerator,
        kokoro_metrics,
        shutdown,
        warmup_excluded: true,
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn metric(mut values: Vec<u64>, threshold_us: u64, trials: Vec<PlaybackReport>) -> LatencyMetric {
    values.sort_unstable();
    LatencyMetric {
        trial_count: values.len(),
        threshold_us,
        p50_us: percentile(&values, 50),
        p95_us: percentile(&values, 95),
        max_us: *values.last().expect("twenty trials are present"),
        trials,
    }
}

fn arguments() -> Result<Option<(PathBuf, String)>, Box<dyn Error>> {
    let mut model_dir = std::env::var_os("PLATO_AUDIO_KOKORO_DIR").map(PathBuf::from);
    let mut sentence = DEFAULT_SENTENCE.to_owned();
    let mut arguments = std::env::args().skip(1);
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--model-dir" => {
                model_dir = Some(PathBuf::from(
                    arguments.next().ok_or("--model-dir requires a path")?,
                ));
            }
            "--sentence" => {
                sentence = arguments
                    .next()
                    .ok_or("--sentence requires nonempty text")?;
            }
            "-h" | "--help" => {
                println!(
                    "Usage: kokoro_device_proof --model-dir PATH [--sentence TEXT]\n\
                     \n\
                     PLATO_AUDIO_KOKORO_DIR may provide PATH. The proof excludes one warmup,\n\
                     measures 20 warm TTFA trials, then proves four-sentence gap and overlap\n\
                     behavior through one persistent device stream. CUDA is required."
                );
                return Ok(None);
            }
            unknown => return Err(format!("unknown argument {unknown:?}").into()),
        }
    }
    let model_dir = model_dir.ok_or(
        "provide --model-dir PATH or set PLATO_AUDIO_KOKORO_DIR to the pinned artifact directory",
    )?;
    Ok(Some((model_dir, sentence)))
}

fn accelerator_info() -> Result<AcceleratorInfo, Box<dyn Error>> {
    let output = Command::new("nvidia-smi")
        .args([
            "--query-gpu=name,driver_version,memory.total",
            "--format=csv,noheader,nounits",
            "--id=0",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "nvidia-smi failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    let stdout = String::from_utf8(output.stdout)?;
    let fields = stdout
        .lines()
        .next()
        .ok_or("nvidia-smi returned no device")?
        .split(',')
        .map(str::trim)
        .collect::<Vec<_>>();
    if fields.len() != 3 {
        return Err(format!("unexpected nvidia-smi inventory: {stdout:?}").into());
    }
    Ok(AcceleratorInfo {
        name: fields[0].to_owned(),
        driver_version: fields[1].to_owned(),
        memory_mib: fields[2].parse()?,
    })
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    let rank = (percentile * sorted.len()).div_ceil(100);
    sorted[rank.saturating_sub(1)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_rank_percentiles_are_deterministic() {
        let values = (1..=20).collect::<Vec<_>>();
        assert_eq!(percentile(&values, 50), 10);
        assert_eq!(percentile(&values, 95), 19);
        assert_eq!(percentile(&values, 100), 20);
    }
}
