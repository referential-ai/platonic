# Quickstart — run and test Plato Agent

Everything below is copy-pasteable. Companion docs: [`../README.md`](../README.md) (full reference), [`ARCHITECTURE.md`](ARCHITECTURE.md) (topology and law).

## 0. One-time setup

```bash
cd ~/projects/platonic-workspace/plato-agent

head=$(git rev-parse --verify 'HEAD^{commit}')
proof_root=$(mktemp -d)
artifact="$proof_root/plato-agent-$head.tar.gz"
source_root="$proof_root/source"
prefix="$proof_root/install"

git archive --format=tar.gz --prefix="plato-agent-$head/" \
  --output "$artifact" "$head"
sha256sum "$artifact"

mkdir "$source_root"
tar -xzf "$artifact" -C "$source_root"
test ! -e "$prefix"
PLATO_BUILD_IDENTITY="0.2.0 $head $(date -u +%Y-%m-%d)" \
  CARGO_TARGET_DIR="$proof_root/target" \
  cargo install --locked --root "$prefix" \
    --path "$source_root/plato-agent-$head/crates/plato-agent"
PLATO_BUILD_IDENTITY="0.2.0 $head $(date -u +%Y-%m-%d)" \
  CARGO_TARGET_DIR="$proof_root/target" \
  cargo install --locked --root "$prefix" \
    --path "$source_root/plato-agent-$head/crates/platonic"

export PATH="$prefix/bin:$PATH"
for binary in plato platonic plato-tui; do
  "$binary" --version
done
for binary in plato platonic plato-tui; do
  "$binary" --help >/dev/null
done

export OPENROUTER_API_KEY="$(cat /path/to/your/openrouter-key)"
```

The tarball is a complete source snapshot of the exact commit printed in its
name. Installation builds only from its extracted workspace and writes the
three binaries to the fresh prefix under `$proof_root`; it does not use
`cargo run`, publish crates, or change an existing installation.

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
enabled = ["file.read", "file.list", "file.write", "file.edit", "shell.exec", "web.fetch"]
```

## 1. First run (60-second smoke test)

```bash
plato "list the files here and summarize what this project is"
plato -c "name the most important file from that summary"
plato replay        # audit the latest default SQLite session
```

Live assistant text prints to stderr; the final answer prints to stdout. The
complete run ledger lands in the default platform SQLite store for the workspace.
`-c` continues the latest workspace session through the same host server.

## 2. Test the approval boundary

```bash
plato "write hello.txt containing: hi from plato"
# -> Approve file.write {...}? [y/N]   press Enter -> denied (default no)

plato --yolo "write hello.txt containing: hi from plato"
# -> auto-approved; the ledger records actor "yolo"

plato "run cargo test --locked and summarize the result"
# -> Approve shell.exec?   press y to run the command
```

Reads and listings never prompt. Workspace writes prompt unless `--yolo`.
Yolo does not approve network tools or `shell.exec`. `shell.exec` always
prompts and runs with a scrubbed environment that does not inherit provider
credentials.
`web.fetch` always prompts with its normalized public origin and validated
addresses, revalidates immediately before each pinned connection, and returns
only bounded UTF-8 text from the approved origin.
Nothing escapes the workspace: `../`, absolute paths, and symlinks out are refused.

## 3. Durable runs (SQLite)

```bash
plato "read Cargo.toml and name the package"
# stderr prints: run_id / ledger_path / the exact replay command
plato -c "what did I ask you to inspect?"
plato replay                # replays the latest session
```

Explicit replay paths use the equals form: `--db=/tmp/run.db`. Prompts always
use the server-owned workspace ledger. Replay is read-only and fully offline;
it shows final assistant messages, not partial live deltas.

## 4. The full experience: TUI

One terminal, same workspace:

```bash
plato
```

This ensures the host-scoped `platonic serve`, asks for the root thread spawn
decision, and attaches the TUI to that durable thread. Quitting the TUI leaves
the daemon and thread authority available. `plato --tui --config plato.toml`
is the explicit form when selecting a config. During a run, one working row
shows elapsed active time and the interrupt key. Use `plato --reduced-motion`
or set `PLATO_REDUCED_MOTION=1` to replace its animated braille marker with a
static bullet.
The screen is a chat-first transcript with a bottom status rule and composer.

From another terminal in the same workspace, list the durable thread and
attach another interactive client:

```bash
plato thread list
plato --remote <thread-id>
```

Both clients observe the same live output. Exactly one controller owns an
active turn; another client is refused until that turn is idle, then can drive
the next turn. Remote attachment does not create a duplicate thread.

The explicit legacy workspace-daemon mode still works:

```bash
platonic serve --workspace "$PWD"                    # terminal A
plato-tui --workspace "$PWD" --config plato.toml      # terminal B
```

`platonic serve --workspace` stays in the foreground. Ctrl-C shuts it down
cleanly (socket and lock removed).
Quitting either TUI form never stops the daemon.

Start the optional Discord connector from a separate environment:

```bash
unset OPENAI_API_KEY OPENROUTER_API_KEY
export DISCORD_BOT_TOKEN="$(cat /path/to/discord-bot-token)"
platonic gateway discord --config ~/.config/plato/gateway.toml
```

The gateway entry requires a successful workspace daemon `hello`. Probe
failures start no connector; the gateway never starts a server with its
Discord environment.

The TUI footer is contextual by default and moves model and workspace context
out of the transcript. Press `?` for the shared shortcut overlay. The footer
switches to a second-press quit hint after cancel and to
`daemon unavailable — r to reconnect` while offline. At 120 columns it includes
model and workspace context; below 120 columns that right-side context drops,
below 80 the queue hint drops, and below 40 the remaining `?` hint truncates
without wrapping.

| Key | Does |
| --- | --- |
| type + Enter | start a run when idle |
| `?` | open shortcuts; `?`, Esc, or `q` closes the overlay |
| Tab | complete a slash command, submit, or queue behind an active run |
| Alt-Enter | insert a newline (`⌥ Enter` on macOS, `alt + enter` elsewhere) |
| `g` / `d` | grant / deny in the approval modal |
| Esc | interrupt an active run; otherwise close or quit |
| Ctrl-C | first press cancels the active run; the footer prompts for a second press to quit |
| `r` | reconnect (only when the screen shows daemon unavailable) |
| `q` | quit when the composer is empty |
| Ctrl-U | clear the composer |

When the Discord gateway reaches an approval-required tool, Discord gets one
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
`narrated_run` runs through the host server and reports its server-owned SQLite
ledger path. Its proof JSON includes the exact revision-one `VoiceEvent`
envelopes observed by the client. Offline `plato replay --db=/path/to/run.db
--run RUN_ID` reads the server-owned core ledger without starting or contacting
the server.

`VoiceCaptured` stores only the final transcript's SHA-256 and UTF-8 byte
length, transcript span, native and 16 kHz frame counts, VAD sample boundaries,
and capture timing. `VoiceSpoken` stores the AU2 sentence-acceptance to first
non-silent callback TTFA in whole milliseconds plus sentence/interruption
coordinates; an AU5 latch adds one exact `VoiceInterrupted` prefix and delta
index. These companion facts are committed atomically beside the core ledger,
never as `HarnessEvent` variants.

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
cargo test --workspace --locked
cargo clippy --workspace --locked --all-targets -- -D warnings
cargo fmt --check
```

## 7. Troubleshooting

| Symptom | Fix |
| --- | --- |
| daemon lock held | on Unix, a live kernel lock owner or failed lock-file safety validation blocks startup; inspect the reported details. Crashed owners recover automatically. Never delete a live lock |
| `--db /path` ignored | use the equals form: `--db=/path` |
| provider api key env is not set | re-export `OPENROUTER_API_KEY` in this shell |
| server unavailable | run `platonic serve`; ordinary `plato` prompts auto-ensure it when the sibling binary is installed |
| run stops after 8 turns | runs are bounded by `limits.max_turns`; ask tighter or configure a different limit |
| `plato -c` says no previous session | run `plato "..."` once in this workspace first |
