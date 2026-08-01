use std::{
    error::Error,
    fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    thread,
    time::Duration,
};

use plato_agent::{ApprovalMode, RunLedger, RunOptions, RunOverrides, VoiceSession};
use plato_audio::{KokoroConfig, PlaybackConfig, SentenceCutter};
use serde::Serialize;

const DEFAULT_QUESTION: &str =
    "Reply with exactly two complete sentences about a warm local voice. Do not use tools.";

#[derive(Serialize)]
struct ProofOutput {
    schema: &'static str,
    run_id: String,
    final_answer: String,
    ledger: String,
    provider: &'static str,
    model: plato_audio::KokoroProvenance,
    output: plato_audio::PlaybackDeviceInfo,
    narration: plato_agent::NarrationReport,
    shutdown: plato_audio::SynthWorkerShutdown,
    synthesis_overlapped_playback: bool,
}

fn main() -> Result<(), Box<dyn Error>> {
    let Some(arguments) = Arguments::read()? else {
        return Ok(());
    };
    let mut voice = VoiceSession::open(
        KokoroConfig::from_model_dir(arguments.model_dir),
        PlaybackConfig::default(),
    )?;
    let ledger = arguments.events.unwrap_or_else(|| {
        std::env::temp_dir().join(format!("plato-narrated-run-{}.jsonl", std::process::id()))
    });
    let fixture = arguments.fixture.then(FixtureProvider::start).transpose()?;
    let config_path = fixture
        .as_ref()
        .map(|fixture| fixture.config_path.clone())
        .or(arguments.config);
    let outcome = voice.run_question(RunOptions {
        question: arguments.question,
        config_path,
        overrides: RunOverrides::default(),
        ledger: RunLedger::Jsonl(ledger.clone()),
        workspace_root: arguments.workspace_root,
        approval_mode: ApprovalMode::Deny {
            actor: "narrated_run_example",
        },
        run_id: None,
        session: None,
        event_sender: None,
        stream_to_stderr: true,
        cancel: None,
    })?;
    if let Some(fixture) = fixture {
        fixture.finish()?;
    }
    let expected = cut_sentences(&outcome.run.final_answer);
    let narrated = outcome
        .narration
        .sentences
        .iter()
        .map(|report| report.sentence.clone())
        .collect::<Vec<_>>();
    if narrated != expected {
        return Err(format!(
            "narrated sentence sequence differed from final response: expected {expected:?}, got {narrated:?}"
        )
        .into());
    }
    let synthesis_overlapped_playback = outcome.narration.sentences.windows(2).all(|pair| {
        pair[1].playback.synth_started_ns < pair[0].playback.pcm_end_ns
            && pair[1].playback.synth_finished_ns > pair[0].playback.first_pcm_ns
    });
    if !synthesis_overlapped_playback {
        return Err("narrated fixture did not overlap synthesis N+1 with playback N".into());
    }
    if outcome
        .narration
        .sentences
        .iter()
        .skip(1)
        .any(|report| report.playback.gap_before_us.is_none_or(|gap| gap > 20_000))
    {
        return Err("narrated fixture exceeded the 20 ms inter-sentence gap bound".into());
    }
    let model = voice.provenance().clone();
    let output = voice.device_info().clone();
    let shutdown = voice.shutdown()?;
    let proof = ProofOutput {
        schema: "plato_agent.narrated_run.v2",
        run_id: outcome.run.run_id.to_string(),
        final_answer: outcome.run.final_answer,
        ledger: ledger.display().to_string(),
        provider: if arguments.fixture {
            "credential-free-loopback-fixture"
        } else {
            "configured-provider"
        },
        model,
        output,
        narration: outcome.narration,
        shutdown,
        synthesis_overlapped_playback,
    };
    println!("{}", serde_json::to_string_pretty(&proof)?);
    Ok(())
}

fn cut_sentences(text: &str) -> Vec<String> {
    let mut cutter = SentenceCutter::new();
    let mut sentences = cutter
        .push(text)
        .into_iter()
        .map(|sentence| sentence.into_string())
        .collect::<Vec<_>>();
    if let Some(tail) = cutter.finish() {
        sentences.push(tail.into_string());
    }
    sentences
}

struct Arguments {
    model_dir: PathBuf,
    config: Option<PathBuf>,
    events: Option<PathBuf>,
    workspace_root: PathBuf,
    question: String,
    fixture: bool,
}

impl Arguments {
    fn read() -> Result<Option<Self>, Box<dyn Error>> {
        let mut model_dir = std::env::var_os("PLATO_AUDIO_KOKORO_DIR").map(PathBuf::from);
        let mut config = None;
        let mut events = None;
        let mut workspace_root = std::env::current_dir()?;
        let mut question = Vec::new();
        let mut fixture = false;
        let mut arguments = std::env::args().skip(1);
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--model-dir" => {
                    model_dir = Some(PathBuf::from(
                        arguments.next().ok_or("--model-dir requires a path")?,
                    ));
                }
                "--config" => {
                    config = Some(PathBuf::from(
                        arguments.next().ok_or("--config requires a path")?,
                    ));
                }
                "--events" => {
                    events = Some(PathBuf::from(
                        arguments.next().ok_or("--events requires a path")?,
                    ));
                }
                "--workspace-root" => {
                    workspace_root =
                        PathBuf::from(arguments.next().ok_or("--workspace-root requires a path")?);
                }
                "--fixture" => fixture = true,
                "-h" | "--help" => {
                    println!(
                        "Usage: narrated_run --model-dir PATH [--config PATH] [--events PATH]\n\
                         \x20      [--workspace-root PATH] [--fixture] [QUESTION ...]\n\
                         \n\
                         PLATO_AUDIO_KOKORO_DIR may provide the model directory. With no QUESTION,\n\
                         a fixed two-sentence no-tools prompt is used. --fixture uses a local SSE\n\
                         provider and requires PLATO_AUDIO_FIXTURE_KEY=local-proof."
                    );
                    return Ok(None);
                }
                value => question.push(value.to_owned()),
            }
        }
        let model_dir = model_dir.ok_or(
            "provide --model-dir PATH or set PLATO_AUDIO_KOKORO_DIR to the pinned artifact directory",
        )?;
        Ok(Some(Self {
            model_dir,
            config,
            events,
            workspace_root,
            question: if question.is_empty() {
                DEFAULT_QUESTION.to_owned()
            } else {
                question.join(" ")
            },
            fixture,
        }))
    }
}

struct FixtureProvider {
    config_path: PathBuf,
    server: Option<thread::JoinHandle<Result<(), String>>>,
}

impl FixtureProvider {
    fn start() -> Result<Self, Box<dyn Error>> {
        if std::env::var_os("PLATO_AUDIO_FIXTURE_KEY").is_none() {
            return Err("--fixture requires PLATO_AUDIO_FIXTURE_KEY=local-proof".into());
        }
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        let config_path = std::env::temp_dir().join(format!(
            "plato-narrated-fixture-{}.toml",
            std::process::id()
        ));
        fs::write(
            &config_path,
            format!(
                "[provider]\n\
                 kind = \"open_ai\"\n\
                 model = \"narrated-run-fixture\"\n\
                 api_key_env = \"PLATO_AUDIO_FIXTURE_KEY\"\n\
                 base_url = \"http://{address}\"\n\
                 \n\
                 [limits]\n\
                 token_budget = 4096\n\
                 max_output_tokens = 256\n\
                 max_turns = 1\n\
                 \n\
                 [tools]\n\
                 enabled = [\"file.read\"]\n"
            ),
        )?;
        let server = thread::spawn(move || serve_fixture(listener));
        Ok(Self {
            config_path,
            server: Some(server),
        })
    }

    fn finish(mut self) -> Result<(), Box<dyn Error>> {
        let result = self
            .server
            .take()
            .expect("fixture server is joined once")
            .join()
            .map_err(|_| "loopback fixture provider panicked")?;
        fs::remove_file(&self.config_path)?;
        result.map_err(Into::into)
    }
}

impl Drop for FixtureProvider {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.config_path);
    }
}

fn serve_fixture(listener: TcpListener) -> Result<(), String> {
    let (mut stream, _) = listener.accept().map_err(|error| error.to_string())?;
    read_request(&mut stream)?;
    let fragments = [
        "Warm local narration speaks ",
        "this first sentence. It keeps ",
        "one engine and one device stream resident.",
    ];
    let mut body = fragments
        .into_iter()
        .map(sse_delta)
        .collect::<Vec<_>>()
        .join("");
    body.push_str(
        "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
    );
    body.push_str(
        "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":8,\"completion_tokens\":14}}\n\n",
    );
    body.push_str("data: [DONE]\n\n");
    write!(
        stream,
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
    .map_err(|error| error.to_string())?;
    stream.flush().map_err(|error| error.to_string())
}

fn read_request(stream: &mut TcpStream) -> Result<(), String> {
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|error| error.to_string())?;
    let mut received = Vec::new();
    let mut buffer = [0_u8; 4096];
    let header_end = loop {
        let count = stream
            .read(&mut buffer)
            .map_err(|error| error.to_string())?;
        if count == 0 {
            return Err("provider request ended before its headers".to_owned());
        }
        received.extend_from_slice(&buffer[..count]);
        if let Some(index) = received.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let headers = String::from_utf8_lossy(&received[..header_end]);
    let content_length = headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .and_then(|value| value.trim().parse::<usize>().ok())
        })
        .unwrap_or(0);
    let mut body_length = received.len() - header_end;
    while body_length < content_length {
        let count = stream
            .read(&mut buffer)
            .map_err(|error| error.to_string())?;
        if count == 0 {
            return Err("provider request ended before its body".to_owned());
        }
        body_length += count;
    }
    Ok(())
}

fn sse_delta(text: &str) -> String {
    format!(
        "data: {{\"choices\":[{{\"index\":0,\"delta\":{{\"content\":{}}},\"finish_reason\":null}}]}}\n\n",
        serde_json::to_string(text).expect("fixture text serializes")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn final_answer_uses_the_same_sentence_contract_as_runtime_composition() {
        assert_eq!(
            cut_sentences("A complete first sentence. A final tail"),
            ["A complete first sentence.", "A final tail"]
        );
    }
}
