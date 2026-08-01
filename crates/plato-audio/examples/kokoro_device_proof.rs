use std::{error::Error, path::PathBuf, process::Command, sync::atomic::AtomicBool, time::Instant};

use plato_audio::{
    AudioFormat, CPAL_RUNTIME_VERSION, InferenceBackend, KokoroConfig, KokoroMetrics,
    KokoroProvenance, KokoroSynthesizer, PcmChunk, PersistentPlayback, PlaybackConfig,
    PlaybackDeviceInfo, PlaybackMetrics, PlaybackReport, Sentence, SpeechSynthesizer,
};
use serde::Serialize;

const TRIALS: usize = 20;
const MAX_P95_US: u64 = 500_000;
const DEFAULT_SENTENCE: &str = "Plato speaks this complete warm sentence.";
const TIMING_BOUNDARY: &str = "Instant immediately before warm synthesize(sentence) returns through the first non-silent sample copied by the persistent cpal output callback";

#[derive(Serialize)]
struct ProofReport {
    schema: &'static str,
    trial_count: usize,
    sentence: String,
    timing_boundary: &'static str,
    threshold_us: u64,
    p50_us: u64,
    p95_us: u64,
    max_us: u64,
    trials: Vec<PlaybackReport>,
    model_format: AudioFormat,
    provenance: KokoroProvenance,
    cpal_runtime: &'static str,
    output: PlaybackDeviceInfo,
    accelerator: AcceleratorInfo,
    kokoro_metrics: KokoroMetrics,
    playback_metrics: PlaybackMetrics,
    warmup_excluded: bool,
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
    let cancel = AtomicBool::new(false);

    let mut synthesizer = KokoroSynthesizer::load(KokoroConfig::from_model_dir(model_dir))?;
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
    let mut playback = PersistentPlayback::open(PlaybackConfig::default())?;
    let accelerator = accelerator_info()?;

    let warmup_accepted = Instant::now();
    let warmup = synthesize_one(&mut synthesizer, &sentence, &cancel)?;
    playback.play_blocking(&warmup, warmup_accepted)?;

    let mut trials = Vec::with_capacity(TRIALS);
    for _ in 0..TRIALS {
        let accepted_at = Instant::now();
        let chunk = synthesize_one(&mut synthesizer, &sentence, &cancel)?;
        trials.push(playback.play_blocking(&chunk, accepted_at)?);
    }

    let mut latencies = trials
        .iter()
        .map(|trial| trial.accepted_to_first_non_silent_us)
        .collect::<Vec<_>>();
    latencies.sort_unstable();
    let kokoro_metrics = synthesizer.metrics();
    let playback_metrics = playback.metrics();
    if kokoro_metrics.session_loads != 1 || kokoro_metrics.syntheses != (TRIALS + 1) as u64 {
        return Err(format!("warm model reuse assertion failed: {kokoro_metrics:?}").into());
    }
    if playback_metrics.stream_opens != 1 || playback_metrics.chunks_played != (TRIALS + 1) as u64 {
        return Err(
            format!("persistent device reuse assertion failed: {playback_metrics:?}").into(),
        );
    }

    let p50_us = percentile(&latencies, 50);
    let p95_us = percentile(&latencies, 95);
    let max_us = *latencies.last().expect("twenty trials are present");
    let report = ProofReport {
        schema: "plato_audio.kokoro_device_ttfa.v1",
        trial_count: TRIALS,
        sentence: sentence.into_string(),
        timing_boundary: TIMING_BOUNDARY,
        threshold_us: MAX_P95_US,
        p50_us,
        p95_us,
        max_us,
        trials,
        model_format: synthesizer.output_format(),
        provenance: synthesizer.provenance().clone(),
        cpal_runtime: CPAL_RUNTIME_VERSION,
        output: playback.device_info().clone(),
        accelerator,
        kokoro_metrics,
        playback_metrics,
        warmup_excluded: true,
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    if p95_us > MAX_P95_US {
        return Err(format!(
            "warm TTFA p95 was {p95_us} us, above the admitted {MAX_P95_US} us limit"
        )
        .into());
    }
    Ok(())
}

fn synthesize_one(
    synthesizer: &mut KokoroSynthesizer,
    sentence: &Sentence,
    cancel: &AtomicBool,
) -> Result<PcmChunk, Box<dyn Error>> {
    let mut chunks = Vec::new();
    synthesizer.synthesize(sentence, &mut chunks, cancel)?;
    if chunks.len() != 1 {
        return Err(format!("Kokoro emitted {} chunks; expected one", chunks.len()).into());
    }
    Ok(chunks.pop().expect("one chunk was checked"))
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
                     PLATO_AUDIO_KOKORO_DIR may provide PATH. The proof performs one excluded\n\
                     warmup followed by 20 serial synth/playback TTFA trials and requires CUDA."
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
