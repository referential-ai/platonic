use std::{
    collections::{BTreeSet, HashMap},
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use ort::{session::Session, value::Tensor};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    PcmSink, SpeechSynthesizer,
    runtime::{OrtRuntime, SessionLoadError},
};
use crate::{AudioFormat, InferenceBackend, PcmChunk, SampleFormat, Sentence, SynthError};

/// Immutable Hugging Face repository used for the admitted Kokoro model.
pub const KOKORO_MODEL_SOURCE: &str = "onnx-community/Kokoro-82M-v1.0-ONNX";
/// Immutable Hugging Face commit containing all admitted model artifacts.
pub const KOKORO_MODEL_REVISION: &str = "1939ad2a8e416c0acfeecc08a694d14ef25f2231";
/// SHA-256 of `onnx/model.onnx` at [`KOKORO_MODEL_REVISION`].
pub const KOKORO_MODEL_SHA256: &str =
    "8fbea51ea711f2af382e88c833d9e288c6dc82ce5e98421ea61c058ce21a34cb";
/// SHA-256 of `tokenizer.json` at [`KOKORO_MODEL_REVISION`].
pub const KOKORO_TOKENIZER_SHA256: &str =
    "77a02c8e164413299b4b4c403b14f8e0e1c1b727db4d46a09d6327b861060a34";
/// SHA-256 of `voices/af_sky.bin` at [`KOKORO_MODEL_REVISION`].
pub const KOKORO_VOICE_SHA256: &str =
    "4435255c9744f3f31659e0d714ab7689bf65d9e77ec1cce060f083912614f0b9";
/// Exact model output sample rate in hertz.
pub const KOKORO_SAMPLE_RATE: u32 = 24_000;
const MODEL_FILENAME: &str = "model.onnx";
const TOKENIZER_FILENAME: &str = "tokenizer.json";
const VOICE_FILENAME: &str = "af_sky.bin";
const VOICE_WIDTH: usize = 256;
const MAX_SENTENCE_BYTES: usize = 16 * 1024;
const MAX_DIAGNOSTIC_BYTES: usize = 2 * 1024;

/// Explicit paths and bounded settings for a pinned Kokoro engine.
#[derive(Clone, Debug, PartialEq)]
pub struct KokoroConfig {
    model_path: PathBuf,
    tokenizer_path: PathBuf,
    voice_path: PathBuf,
    phonemizer_program: PathBuf,
    language: String,
    speed: f32,
}

impl KokoroConfig {
    /// Resolves the three pinned artifacts from one untracked model directory.
    pub fn from_model_dir(model_dir: impl Into<PathBuf>) -> Self {
        let model_dir = model_dir.into();
        Self {
            model_path: model_dir.join(MODEL_FILENAME),
            tokenizer_path: model_dir.join(TOKENIZER_FILENAME),
            voice_path: model_dir.join(VOICE_FILENAME),
            phonemizer_program: PathBuf::from("espeak-ng"),
            language: "en-us".to_owned(),
            speed: 1.0,
        }
    }

    /// Overrides the fixed external espeak-ng executable.
    pub fn with_phonemizer_program(mut self, program: impl Into<PathBuf>) -> Self {
        self.phonemizer_program = program.into();
        self
    }

    /// Selects an espeak-ng voice or language identifier.
    pub fn with_language(mut self, language: impl Into<String>) -> Result<Self, SynthError> {
        let language = language.into();
        if language.trim().is_empty() {
            return Err(SynthError::InvalidConfig {
                reason: "phonemizer language must not be empty".to_owned(),
            });
        }
        self.language = language;
        Ok(self)
    }

    /// Sets the positive finite model speed multiplier.
    pub fn with_speed(mut self, speed: f32) -> Result<Self, SynthError> {
        if !speed.is_finite() || speed <= 0.0 {
            return Err(SynthError::InvalidConfig {
                reason: "speed must be positive and finite".to_owned(),
            });
        }
        self.speed = speed;
        Ok(self)
    }

    /// Returns the ONNX model path.
    pub fn model_path(&self) -> &Path {
        &self.model_path
    }

    /// Returns the tokenizer path.
    pub fn tokenizer_path(&self) -> &Path {
        &self.tokenizer_path
    }

    /// Returns the selected voice tensor path.
    pub fn voice_path(&self) -> &Path {
        &self.voice_path
    }
}

/// Exact artifact and runtime identity captured by a loaded engine.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct KokoroProvenance {
    /// Immutable model repository.
    pub model_source: String,
    /// Immutable model commit.
    pub model_revision: String,
    /// Verified ONNX model digest.
    pub model_sha256: String,
    /// Verified tokenizer digest.
    pub tokenizer_sha256: String,
    /// Verified voice digest.
    pub voice_sha256: String,
    /// Pinned Rust wrapper and runtime line.
    pub ort_runtime: String,
    /// Loaded ONNX Runtime build identity.
    pub ort_build_info: String,
    /// Shared process-local runtime owner used to construct this session.
    pub ort_runtime_owner: u64,
    /// Exact espeak-ng version output.
    pub phonemizer_version: String,
    /// Resident inference provider.
    pub backend: InferenceBackend,
    /// CUDA diagnostic retained when CPU fallback was required.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fallback_reason: Option<String>,
}

/// Observable resident-engine reuse counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct KokoroMetrics {
    /// Successful model session constructions. This remains one for a reused engine.
    pub session_loads: u64,
    /// Phonemizer process invocations after construction.
    pub phonemizer_invocations: u64,
    /// Successful inference calls.
    pub syntheses: u64,
}

#[derive(Default)]
struct KokoroMetricCounters {
    session_loads: AtomicU64,
    phonemizer_invocations: AtomicU64,
    syntheses: AtomicU64,
}

/// Cloneable read-only access to counters owned by the synth worker's engine.
#[derive(Clone)]
pub struct KokoroMetricsReader {
    counters: Arc<KokoroMetricCounters>,
}

impl KokoroMetricsReader {
    /// Reads one internally consistent-enough monotonic metrics snapshot.
    pub fn snapshot(&self) -> KokoroMetrics {
        KokoroMetrics {
            session_loads: self.counters.session_loads.load(Ordering::Relaxed),
            phonemizer_invocations: self.counters.phonemizer_invocations.load(Ordering::Relaxed),
            syntheses: self.counters.syntheses.load(Ordering::Relaxed),
        }
    }
}

/// Warm Kokoro-82M inference state backed by one resident ONNX session.
pub struct KokoroSynthesizer {
    session: Session,
    backend: InferenceBackend,
    tokenizer: HashMap<char, i64>,
    voice: Box<[f32]>,
    voice_rows: usize,
    phonemizer: EspeakPhonemizer,
    speed: f32,
    provenance: KokoroProvenance,
    metrics: Arc<KokoroMetricCounters>,
}

impl KokoroSynthesizer {
    /// Verifies all artifacts, starts the phonemizer, and loads one resident session.
    pub fn load(config: KokoroConfig) -> Result<Self, SynthError> {
        let runtime = OrtRuntime::acquire()?;
        Self::load_with_runtime(config, runtime)
    }

    /// Loads one resident session through an explicitly shared ONNX runtime owner.
    pub fn load_with_runtime(
        config: KokoroConfig,
        runtime: OrtRuntime,
    ) -> Result<Self, SynthError> {
        let model = read_verified("model", &config.model_path, KOKORO_MODEL_SHA256)?;
        drop(model);
        let tokenizer_bytes =
            read_verified("tokenizer", &config.tokenizer_path, KOKORO_TOKENIZER_SHA256)?;
        let voice_bytes = read_verified("voice", &config.voice_path, KOKORO_VOICE_SHA256)?;
        let tokenizer = parse_tokenizer(&config.tokenizer_path, &tokenizer_bytes)?;
        let (voice, voice_rows) = parse_voice(&config.voice_path, &voice_bytes)?;
        let phonemizer = EspeakPhonemizer::start(config.phonemizer_program, config.language)?;
        let resident = runtime
            .load_session(&config.model_path)
            .map_err(map_session_error)?;
        let session = resident.session;
        let backend = resident.backend;
        let fallback_reason = resident.fallback_reason;

        let provenance = KokoroProvenance {
            model_source: KOKORO_MODEL_SOURCE.to_owned(),
            model_revision: KOKORO_MODEL_REVISION.to_owned(),
            model_sha256: KOKORO_MODEL_SHA256.to_owned(),
            tokenizer_sha256: KOKORO_TOKENIZER_SHA256.to_owned(),
            voice_sha256: KOKORO_VOICE_SHA256.to_owned(),
            ort_runtime: super::ORT_RUNTIME_VERSION.to_owned(),
            ort_build_info: bounded(ort::info()),
            ort_runtime_owner: runtime.owner_id(),
            phonemizer_version: phonemizer.version.clone(),
            backend,
            fallback_reason,
        };

        let metrics = Arc::new(KokoroMetricCounters::default());
        metrics.session_loads.store(1, Ordering::Relaxed);
        Ok(Self {
            session,
            backend,
            tokenizer,
            voice,
            voice_rows,
            phonemizer,
            speed: config.speed,
            provenance,
            metrics,
        })
    }

    /// Returns immutable runtime and artifact identity.
    pub fn provenance(&self) -> &KokoroProvenance {
        &self.provenance
    }

    /// Returns counters proving resident session reuse.
    pub fn metrics(&self) -> KokoroMetrics {
        self.metrics_reader().snapshot()
    }

    /// Returns a read-only counter handle that remains valid after worker ownership transfer.
    pub fn metrics_reader(&self) -> KokoroMetricsReader {
        KokoroMetricsReader {
            counters: Arc::clone(&self.metrics),
        }
    }

    fn token_ids(&mut self, sentence: &Sentence) -> Result<Vec<i64>, SynthError> {
        self.metrics
            .phonemizer_invocations
            .fetch_add(1, Ordering::Relaxed);
        let phonemes = self.phonemizer.phonemize(sentence)?;
        tokenize(&self.tokenizer, &phonemes, self.voice_rows)
    }
}

impl SpeechSynthesizer for KokoroSynthesizer {
    fn output_format(&self) -> AudioFormat {
        AudioFormat::new(KOKORO_SAMPLE_RATE, 1, SampleFormat::F32)
            .expect("constant Kokoro format is valid")
    }

    fn synthesize(
        &mut self,
        sentence: &Sentence,
        sink: &mut dyn PcmSink,
        cancel: &AtomicBool,
    ) -> Result<(), SynthError> {
        if cancel.load(Ordering::Acquire) {
            return Err(SynthError::Canceled);
        }
        let token_ids = self.token_ids(sentence)?;
        if cancel.load(Ordering::Acquire) {
            return Err(SynthError::Canceled);
        }
        let style_offset = token_ids.len() * VOICE_WIDTH;
        let style = self.voice[style_offset..style_offset + VOICE_WIDTH]
            .to_vec()
            .into_boxed_slice();
        let input_ids =
            Tensor::from_array(([1_usize, token_ids.len()], token_ids.into_boxed_slice()))
                .map_err(|error| self.inference_error(error))?;
        let style = Tensor::from_array(([1_usize, VOICE_WIDTH], style))
            .map_err(|error| self.inference_error(error))?;
        let speed = Tensor::from_array(([1_usize], vec![self.speed].into_boxed_slice()))
            .map_err(|error| self.inference_error(error))?;

        let samples = {
            let output = self
                .session
                .run(ort::inputs![
                    "input_ids" => input_ids,
                    "style" => style,
                    "speed" => speed,
                ])
                .map_err(|error| SynthError::Inference {
                    backend: self.backend,
                    reason: bounded(&error.to_string()),
                })?;
            let (_, samples) =
                output[0]
                    .try_extract_tensor::<f32>()
                    .map_err(|error| SynthError::Inference {
                        backend: self.backend,
                        reason: bounded(&error.to_string()),
                    })?;
            samples.to_vec()
        };
        let chunk = PcmChunk::from_f32(self.output_format(), samples)?;
        if cancel.load(Ordering::Acquire) {
            return Err(SynthError::Canceled);
        }
        sink.push(chunk)?;
        self.metrics.syntheses.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

impl KokoroSynthesizer {
    fn inference_error(&self, error: ort::Error) -> SynthError {
        SynthError::Inference {
            backend: self.backend,
            reason: bounded(&error.to_string()),
        }
    }
}

#[derive(Deserialize)]
struct TokenizerDocument {
    model: TokenizerModel,
}

#[derive(Deserialize)]
struct TokenizerModel {
    vocab: HashMap<String, i64>,
}

fn parse_tokenizer(path: &Path, bytes: &[u8]) -> Result<HashMap<char, i64>, SynthError> {
    let document: TokenizerDocument =
        serde_json::from_slice(bytes).map_err(|error| SynthError::Tokenizer {
            path: path.to_owned(),
            reason: bounded(&error.to_string()),
        })?;
    let mut tokenizer = HashMap::with_capacity(document.model.vocab.len());
    for (symbol, id) in document.model.vocab {
        let mut characters = symbol.chars();
        let Some(character) = characters.next() else {
            return Err(SynthError::Tokenizer {
                path: path.to_owned(),
                reason: "vocabulary contains an empty symbol".to_owned(),
            });
        };
        if characters.next().is_some() || id < 0 {
            return Err(SynthError::Tokenizer {
                path: path.to_owned(),
                reason: format!("unsupported vocabulary entry {symbol:?} => {id}"),
            });
        }
        tokenizer.insert(character, id);
    }
    if tokenizer.get(&'$') != Some(&0) {
        return Err(SynthError::Tokenizer {
            path: path.to_owned(),
            reason: "padding symbol `$` must map to token zero".to_owned(),
        });
    }
    Ok(tokenizer)
}

fn parse_voice(path: &Path, bytes: &[u8]) -> Result<(Box<[f32]>, usize), SynthError> {
    let row_bytes = VOICE_WIDTH * size_of::<f32>();
    if bytes.is_empty() || bytes.len() % row_bytes != 0 {
        return Err(SynthError::Voice {
            path: path.to_owned(),
            reason: format!(
                "{} bytes do not form rows of {VOICE_WIDTH} little-endian f32 values",
                bytes.len()
            ),
        });
    }
    let voice = bytes
        .chunks_exact(size_of::<f32>())
        .map(|bytes| f32::from_le_bytes(bytes.try_into().expect("four-byte chunk")))
        .collect::<Vec<_>>();
    if let Some(index) = voice.iter().position(|sample| !sample.is_finite()) {
        return Err(SynthError::Voice {
            path: path.to_owned(),
            reason: format!("voice value at index {index} is not finite"),
        });
    }
    let rows = voice.len() / VOICE_WIDTH;
    Ok((voice.into_boxed_slice(), rows))
}

fn tokenize(
    tokenizer: &HashMap<char, i64>,
    phonemes: &str,
    voice_rows: usize,
) -> Result<Vec<i64>, SynthError> {
    let mut unknown = BTreeSet::new();
    let mut ids = Vec::with_capacity(phonemes.chars().count() + 2);
    ids.push(0);
    for symbol in phonemes.chars() {
        match tokenizer.get(&symbol) {
            Some(id) => ids.push(*id),
            None => {
                unknown.insert(symbol);
            }
        }
    }
    ids.push(0);
    if !unknown.is_empty() {
        return Err(SynthError::UnknownPhonemes {
            symbols: unknown.into_iter().collect(),
        });
    }
    if ids.len() >= voice_rows {
        return Err(SynthError::SentenceTooLong {
            tokens: ids.len(),
            maximum: voice_rows.saturating_sub(1),
        });
    }
    Ok(ids)
}

struct EspeakPhonemizer {
    program: PathBuf,
    language: String,
    version: String,
}

impl EspeakPhonemizer {
    fn start(program: PathBuf, language: String) -> Result<Self, SynthError> {
        let output = Command::new(&program)
            .arg("--version")
            .output()
            .map_err(|source| SynthError::PhonemizerStart {
                program: program.clone(),
                source,
            })?;
        if !output.status.success() {
            return Err(SynthError::PhonemizerFailed {
                status: output.status.code(),
                stderr: bounded(&String::from_utf8_lossy(&output.stderr)),
            });
        }
        let version = String::from_utf8_lossy(&output.stdout)
            .lines()
            .chain(String::from_utf8_lossy(&output.stderr).lines())
            .find(|line| !line.trim().is_empty())
            .map(str::trim)
            .unwrap_or("espeak-ng version unavailable")
            .to_owned();
        Ok(Self {
            program,
            language,
            version,
        })
    }

    fn phonemize(&self, sentence: &Sentence) -> Result<String, SynthError> {
        if sentence.as_str().len() > MAX_SENTENCE_BYTES {
            return Err(SynthError::SentenceTextTooLong {
                bytes: sentence.as_str().len(),
                maximum: MAX_SENTENCE_BYTES,
            });
        }
        let mut child = Command::new(&self.program)
            .args(["-q", "--ipa=3", "-v"])
            .arg(&self.language)
            .arg("--stdin")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|source| SynthError::PhonemizerStart {
                program: self.program.clone(),
                source,
            })?;
        child
            .stdin
            .take()
            .expect("piped stdin is present")
            .write_all(sentence.as_str().as_bytes())
            .map_err(|source| SynthError::PhonemizerStart {
                program: self.program.clone(),
                source,
            })?;
        let output = child
            .wait_with_output()
            .map_err(|source| SynthError::PhonemizerStart {
                program: self.program.clone(),
                source,
            })?;
        if !output.status.success() {
            return Err(SynthError::PhonemizerFailed {
                status: output.status.code(),
                stderr: bounded(&String::from_utf8_lossy(&output.stderr)),
            });
        }
        let output =
            String::from_utf8(output.stdout).map_err(|_| SynthError::InvalidPhonemeEncoding)?;
        normalize_phonemes(&output, sentence)
    }
}

fn normalize_phonemes(output: &str, sentence: &Sentence) -> Result<String, SynthError> {
    let mut clauses = Vec::new();
    for line in output.lines() {
        let normalized: String = line
            .chars()
            .filter(|character| !matches!(character, '\u{200c}' | '\u{200d}' | '\u{feff}'))
            .collect();
        let normalized = normalized.split_whitespace().collect::<Vec<_>>().join(" ");
        if !normalized.is_empty() {
            clauses.push(normalized);
        }
    }
    let mut phonemes = clauses.join(", ");
    if phonemes.is_empty() {
        return Err(SynthError::PhonemizerFailed {
            status: Some(0),
            stderr: "espeak-ng produced no phonemes".to_owned(),
        });
    }
    let punctuation = sentence
        .as_str()
        .trim_end()
        .chars()
        .next_back()
        .filter(|character| matches!(character, '.' | '!' | '?'))
        .unwrap_or('.');
    phonemes.push(punctuation);
    Ok(phonemes)
}

fn read_verified(
    artifact: &'static str,
    path: &Path,
    expected: &'static str,
) -> Result<Vec<u8>, SynthError> {
    let bytes = std::fs::read(path).map_err(|source| SynthError::ArtifactRead {
        artifact,
        path: path.to_owned(),
        source,
    })?;
    let actual = format!("{:x}", Sha256::digest(&bytes));
    if actual != expected {
        return Err(SynthError::ArtifactChecksum {
            artifact,
            path: path.to_owned(),
            expected,
            actual,
        });
    }
    Ok(bytes)
}

fn map_session_error(error: SessionLoadError) -> SynthError {
    match error {
        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        SessionLoadError::Fallback { cuda, cpu } => SynthError::ModelLoadFallback { cuda, cpu },
        #[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
        SessionLoadError::Backend { backend, reason } => SynthError::ModelLoad { backend, reason },
    }
}

fn bounded(value: &str) -> String {
    value.chars().take(MAX_DIAGNOSTIC_BYTES).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_rejects_invalid_language_and_speed() {
        assert!(
            KokoroConfig::from_model_dir("model")
                .with_language("  ")
                .is_err()
        );
        assert!(
            KokoroConfig::from_model_dir("model")
                .with_speed(0.0)
                .is_err()
        );
    }

    #[test]
    fn tokenizer_requires_padding_and_single_character_symbols() {
        let path = Path::new("tokenizer.json");
        let valid = br#"{"model":{"vocab":{"$":0,"a":1}}}"#;
        assert_eq!(parse_tokenizer(path, valid).unwrap().get(&'a'), Some(&1));

        let missing_padding = br#"{"model":{"vocab":{"a":1}}}"#;
        assert!(parse_tokenizer(path, missing_padding).is_err());
        let compound = br#"{"model":{"vocab":{"$":0,"ab":1}}}"#;
        assert!(parse_tokenizer(path, compound).is_err());
    }

    #[test]
    fn tokenization_reports_unknown_symbols_and_voice_bound() {
        let tokenizer = HashMap::from([('$', 0), ('a', 1), ('.', 2)]);
        assert_eq!(tokenize(&tokenizer, "a.", 8).unwrap(), [0, 1, 2, 0]);
        assert!(matches!(
            tokenize(&tokenizer, "b.", 8),
            Err(SynthError::UnknownPhonemes { symbols }) if symbols == "b"
        ));
        assert!(matches!(
            tokenize(&tokenizer, "a.", 4),
            Err(SynthError::SentenceTooLong {
                tokens: 4,
                maximum: 3
            })
        ));
    }

    #[test]
    fn phoneme_normalization_removes_joiners_and_restores_prosody() {
        let sentence = Sentence::new("Hello there!").unwrap();
        assert_eq!(
            normalize_phonemes(" h\u{200d}əloʊ  \n ðɛɹ \n", &sentence).unwrap(),
            "həloʊ, ðɛɹ!"
        );
        let sentence = Sentence::new("No punctuation").unwrap();
        assert_eq!(normalize_phonemes("noʊ", &sentence).unwrap(), "noʊ.");
    }

    #[test]
    fn voice_parser_requires_complete_finite_rows() {
        let path = Path::new("voice.bin");
        assert!(parse_voice(path, &[0; 4]).is_err());
        let mut row = vec![0_u8; VOICE_WIDTH * size_of::<f32>()];
        row[..4].copy_from_slice(&f32::NAN.to_le_bytes());
        assert!(parse_voice(path, &row).is_err());
    }
}
