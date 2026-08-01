# Quickstart — run and test Plato Agent

Everything below is copy-pasteable. Companion docs: [`../README.md`](../README.md) (full reference), [`ARCHITECTURE.md`](ARCHITECTURE.md) (topology and law).

## 0. One-time setup

```bash
cd ~/projects/platonic-workspace/plato-agent
cargo build --locked                      # builds all binaries
export OPENROUTER_API_KEY="$(cat /path/to/your/openrouter-key)"
export PATH="$PWD/target/debug:$PATH"     # so the binaries just work in this shell
```

`plato` works without a local config when `OPENROUTER_API_KEY` is exported.
Config is discovered in this order: `--config`, `PLATO_CONFIG`, `./plato.toml`,
`~/.config/plato/config.toml`, built-in defaults. Optional local config:

```toml
[provider]
kind = "open_router"
model = "~openai/gpt-latest"
api_key_env = "OPENROUTER_API_KEY"

[limits]
token_budget = 4000
max_output_tokens = 1024
max_turns = 8

[tools]
enabled = ["file.read", "file.list", "file.write", "file.edit", "shell.exec"]
```

## 1. First run (60-second smoke test)

```bash
plato "list the files here and summarize what this project is"
plato -c "name the most important file from that summary"
plato replay        # audit the latest default SQLite session
```

Live assistant text prints to stderr; the final answer prints to stdout. The
complete run ledger lands in the default platform SQLite store for the workspace.
`-c` continues the latest workspace session. Use `--events <file>` when you
want JSONL.

## 2. Test the approval boundary

```bash
plato --events w1.jsonl "write hello.txt containing: hi from plato"
# -> Approve file.write {...}? [y/N]   press Enter -> denied (default no)

plato --yolo --events w2.jsonl "write hello.txt containing: hi from plato"
# -> auto-approved; the ledger records actor "yolo"

plato --events w3.jsonl "run cargo test --locked and summarize the result"
# -> Approve shell.exec?   press y to run the command
```

Reads and listings never prompt. Workspace writes prompt unless `--yolo`.
Yolo does not approve network tools or `shell.exec`. `shell.exec` always
prompts and runs with a scrubbed environment that does not inherit provider
credentials.
Nothing escapes the workspace: `../`, absolute paths, and symlinks out are refused.

## 3. Durable runs (SQLite)

```bash
plato "read Cargo.toml and name the package"
# stderr prints: run_id / ledger_path / the exact replay command
plato -c "what did I ask you to inspect?"
plato replay                # replays the latest session
```

Explicit SQLite paths need the equals form: `--db=/tmp/run.db`. If the
workspace daemon is live, default-ledger prompts delegate to it. Replay,
explicit `--db=<path>`, and direct `--yolo` SQLite paths remain direct and fail
closed if they conflict with the daemon-owned store. Replay shows final
assistant messages, not partial live deltas.

## 4. The full experience: TUI

One terminal, same workspace:

```bash
plato
```

This attaches to the workspace daemon if one is already running. Otherwise it
starts the sibling `plato-agentd` detached. Quitting the TUI leaves that daemon
running. `plato --tui --config plato.toml` is the explicit form when selecting
a config.
The screen is a chat-first transcript with a bottom status rule and composer.

Manual two-terminal mode still works:

```bash
plato daemon                                          # terminal A
plato-tui --workspace "$PWD" --config plato.toml      # terminal B
```

`plato daemon` stays in the foreground and delegates to the supported sibling
`plato-agentd`. Ctrl-C shuts it down cleanly (socket and lock removed).
Quitting either TUI form never stops the daemon.

Start the optional Discord connector from a separate environment:

```bash
unset OPENAI_API_KEY OPENROUTER_API_KEY
export DISCORD_BOT_TOKEN="$(cat /path/to/discord-bot-token)"
plato gateway discord --config ~/.config/plato/gateway.toml
```

The gateway entry requires a successful workspace daemon `hello`. Probe
failures start no connector and point to `plato daemon`; the gateway never
starts a daemon with its Discord environment. The direct
`plato-gateway-discord --workspace "$PWD"` technical command remains supported.

| Key | Does |
| --- | --- |
| type + Enter | start a run when idle |
| `g` / `d` | grant / deny in the approval modal |
| Ctrl-C | first press cancels the active run; second quits the TUI |
| `r` | reconnect (only when the screen shows daemon unavailable) |
| `q` / Esc | quit (`q` only with an empty composer, so it is typeable in words) |
| Ctrl-U | clear the composer |

When `plato-gateway-discord` reaches an approval-required tool, Discord gets one
bounded notification with the tool, effect, and preview. Grant or deny it
locally in `plato-tui`; the gateway never sends approval decisions. Failed runs
post `Run failed. Inspect it locally with: plato replay`. Canceled and
interrupted runs do not post terminal messages. Allowed messages show 👀 and a
typing indicator while active, then ✅ or ❌; canceled and interrupted runs
remove 👀 without a terminal reaction. The bot needs Add Reactions and Read
Message History, plus Send Messages in Threads when threads are used.

## 5. Local voice proof (developer MVP)

AU2 voice-out and AU4 explicit voice-in are exposed through focused examples,
not a general CLI or ambient listener. Install espeak-ng, CUDA, and the native
cpal backend headers, then place the pinned Kokoro, Silero v6.2.1, and
large-v3-turbo artifacts described in
[`../crates/plato-audio/README.md`](../crates/plato-audio/README.md) outside the
repository.

```bash
export PLATO_AUDIO_KOKORO_DIR="$HOME/.cache/plato-audio/kokoro-82m-v1.0-onnx-1939ad2a8e416c0acfeecc08a694d14ef25f2231"
export PLATO_AUDIO_SILERO_MODEL="$HOME/.cache/plato-audio/silero-vad-7e30209a3e901f9842f81b225f3e93d8199902b1/silero_vad.onnx"
export PLATO_AUDIO_WHISPER_MODEL="$HOME/.cache/plato-audio/ggml-large-v3-turbo-6034871ec87c84e342efab769d4c5c06cd126db3.bin"

# Credential-free real run_question delta narration through the live speaker:
PLATO_AUDIO_FIXTURE_KEY=local-proof \
  cargo run --release --locked --example narrated_run -- --fixture

# One excluded warmup, 20 TTFA trials, and four-sentence gap/overlap proof:
cargo run --release --locked -p plato-audio --example kokoro_device_proof

# Twenty-five actual-output barge-in trials; every all-silent callback <=30 ms:
cargo run --release --locked -p plato-audio --example barge_in_device_proof \
  > docs/proofs/issue-330-barge-in-device.json

# Public non-human corpus: AU3 threshold versus Silero confusion/endpoints:
cargo test --release --locked -p plato-audio \
  silero_strictly_reduces_au3_false_cuts_without_missing_speech -- --ignored --nocapture

# Twenty warm RTX 4090 trials: live partial p95 <=200 ms, final p95 <=120 ms:
ffmpeg -hide_banner -loglevel error -y -stream_loop 23 \
  -i crates/plato-audio/fixtures/au4/speech-plus-noise.wav \
  -f s16le -acodec pcm_s16le -ac 1 -ar 16000 \
  /tmp/plato-329-au4-cpal-24x.raw
(
  module_id=$(pactl load-module module-null-sink \
    sink_name=plato_au4_timing rate=48000 channels=2)
  pacat --playback --raw --device=plato_au4_timing --rate=16000 \
    --channels=1 --format=s16le </tmp/plato-329-au4-cpal-24x.raw &
  feeder_pid=$!
  trap 'kill "$feeder_pid" 2>/dev/null || true; wait "$feeder_pid" 2>/dev/null || true; pactl unload-module "$module_id"' EXIT
  PULSE_SOURCE=plato_au4_timing.monitor \
  PLATO_AUDIO_PULSE_MODULE_ID="$module_id" \
  PLATO_AUDIO_PULSE_FEEDER_PID="$feeder_pid" \
  PLATO_AUDIO_RECORDED_FIXTURE_RAW=/tmp/plato-329-au4-cpal-24x.raw \
    cargo test --release --locked --features whisper-cuda \
    twenty_warm_rtx4090_live_partial_and_final_trials_meet_au4_bounds -- --ignored --nocapture
)

# Exact 24.859-second transcript plus 20 warm bounded final-window decodes:
cargo test --release --locked -p plato-audio --features whisper-cuda \
  long_utterance_final_is_bounded_and_preserves_exact_stable_text -- --ignored
cargo test --release --locked -p plato-audio --features whisper-cuda \
  twenty_long_utterance_finals_are_bounded_and_preserve_exact_stable_text \
  -- --ignored --nocapture

# Inspect input IDs without changing the host default audio policy:
cargo run --locked --example narrated_run -- --list-input-devices

# One microphone question -> one existing run -> spoken AU2 answer:
PLATO_AUDIO_FIXTURE_KEY=local-proof \
  cargo run --release --locked --features whisper-cuda --example narrated_run -- \
  --fixture --whisper-model "$PLATO_AUDIO_WHISPER_MODEL" \
  --silero-model "$PLATO_AUDIO_SILERO_MODEL" --input-device CPAL_ID
```

The model engine, native-rate resampling plan, and output stream open before
timing. Both examples fail closed on artifact checksum, phonemizer, backend,
device, PCM, worker, callback, sentence-order, gap, overlap, or teardown errors.
AU4 opens one persistent input stream and one worker, normalizes/resamples on
the worker, and runs a warm Silero session through the ONNX Runtime owner shared
with Kokoro. The resident CUDA recognizer re-decodes only a bounded five-second
pending window, commits only timestamp-bounded byte-stable prefixes outside a
one-second overlap, and finalizes only the retained tail. Its non-final text
replaces one live stderr line; only the single final transcript enters the
existing run path. Ring overflow, device loss, worker panic, VAD failure, and
recognition failure are typed terminal outcomes.

During narrated playback, the resident Silero session also evaluates input
continuously after a fixed 150 ms self-playback gate. Qualified speech uses the
run's existing cancel atomic, silences the next complete output callback
quantum, and flushes synthesis/prefetch outside the real-time callback. The next
run receives exactly one sample-derived spoken prefix and sentence/delta
position in its recorded `ContextBuilt` current-task context. A generic cancel
does not create that context, and AU5 does not start another run automatically.

The deterministic timing command selects a named PipeWire/Pulse null-sink
monitor through the real production cpal/callback/rtrb/normalization path. The
spoken payload is the recorded CC0 WAV, not a physical microphone or live human
voice. Its partial clock starts at cpal callback entry and therefore includes
ring wait and worker normalization; it does not claim analog, driver, device,
or virtual-source pacing latency before that callback. The interactive
microphone form retains no raw audio. Proof JSON contains transcripts, bounded
metrics, and provenance. Model files and provider credentials are never written
into the repository or proof JSON.

The separate AU5 output proof also uses the recorded CC0 synthetic WAV, fed
directly to Silero after the playback gate. Its clock starts when resident
Silero qualifies speech and ends at the first entirely silent callback on the
actual output device. It therefore makes no physical-microphone, cpal-input, or
acoustic-loop latency claim.

## 6. Run the test suite (no API key needed)

```bash
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
cargo fmt --check
```

## 7. Troubleshooting

| Symptom | Fix |
| --- | --- |
| daemon lock held | on Unix, a live kernel lock owner or failed lock-file safety validation blocks startup; inspect the reported details. Crashed owners recover automatically. Never delete a live lock |
| `--db /path` ignored | use the equals form: `--db=/path` |
| provider api key env is not set | re-export `OPENROUTER_API_KEY` in this shell |
| ledger already exists | JSONL ledgers never overwrite — pass a fresh `--events` name |
| run stops after 8 turns | runs are bounded by `limits.max_turns`; ask tighter or configure a different limit |
| `plato -c` says no previous session | run `plato "..."` once in this workspace first |
