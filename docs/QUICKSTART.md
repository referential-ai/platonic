# Quickstart - run Platonic with Plato Agent

Companion docs: [`../README.md`](../README.md) (full reference),
[`RELEASE.md`](RELEASE.md) (release artifacts), and the
[platform decision map](https://github.com/referential-ai/platonic-workspace/issues/83)
(architecture authority).

## 0. One-time setup

```bash
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) target=linux-x86_64 ;;
  Darwin-arm64) target=macos-arm64 ;;
  *) echo "unsupported platform" >&2; exit 1 ;;
esac

bundle="platonic-0.1.0-$target"
archive="$bundle.tar.gz"
release="https://github.com/referential-ai/platonic/releases/download/platonic-v0.1.0"
curl -fLO "$release/$archive"
curl -fLO "$release/$bundle.sha256"

if command -v sha256sum >/dev/null 2>&1; then
  sha256sum --check "$bundle.sha256"
else
  shasum -a 256 --check "$bundle.sha256"
fi

tar -xzf "$archive"
install -d "$HOME/.local/bin"
install -m 0755 "$bundle/bin/platonic" "$HOME/.local/bin/platonic"
install -m 0755 "$bundle/bin/plato" "$HOME/.local/bin/plato"
install -m 0755 "$bundle/bin/plato-tui" "$HOME/.local/bin/plato-tui"
export PATH="$HOME/.local/bin:$PATH"

platonic --version
plato --version
plato-tui --version

export OPENROUTER_API_KEY="$(cat /path/to/your/openrouter-key)"
```

The release tag is exactly `platonic-v0.1.0`. The bundle contains the Platonic
server command `platonic` and the Plato Agent client commands `plato` and
`plato-tui`. Linux x86-64 and macOS Apple silicon are the only launch targets;
Windows server and client support is withdrawn. The
[release contract](RELEASE.md) lists the exact archive contents and explains
the independent `platonic-core` semver.

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

In terminal 1, start the one host server:

```bash
platonic serve
```

In terminal 2, register the workspace deliberately and run Plato Agent as a
short-lived client:

```bash
mkdir -p "$HOME/platonic-quickstart"
cd "$HOME/platonic-quickstart"
git init
git -c user.name='Platonic Quickstart' \
  -c user.email='quickstart@invalid' commit --allow-empty -m 'Initial workspace'
platonic workspace create quickstart "$PWD"
platonic status --workspace "$PWD"
plato "list the files here and summarize what this project is"
plato -c "name the most important file from that summary"
plato replay        # audit the latest default workspace session
```

When no server is running, an interactive local `plato` one-shot or TUI starts
the installed sibling `platonic` server. In an unknown directory it asks once
for a workspace name and defaults to the directory basename. Piped/headless
commands, `plato --remote`, gateways, desktop clients, explicit `--socket`
attachments, and `plato-tui --snapshot` never register a workspace; use
`platonic workspace create <name> <directory>` first.

Live assistant text prints to stderr; the final answer prints to stdout. The
complete run event log lands in one JSONL file under the workspace ledger
directory. SQLite retains the session index and other queryable state.
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

Reads and listings never prompt. Workspace writes and exact `shell.exec` calls
prompt unless `--yolo`; direct root `PLATONIC.md` changes still prompt. Yolo
never approves network, secret-access, unknown, disabled, or other
external-side-effect tools. `shell.exec` runs with a scrubbed environment that
does not inherit provider credentials.
`web.fetch` always prompts with its normalized public origin and validated
addresses, revalidates immediately before each pinned connection, and returns
only bounded UTF-8 text from the approved origin.
File tools refuse `../`, absolute paths, and symlinks that escape their granted
roots. Server-created thread children use Landlock write confinement when the
Linux host supports it; macOS and Linux hosts without Landlock record
`confinement: "none"`. Set `[confinement] require = true` in the user config to
refuse an unconfined spawn. `plato thread status <thread-id>` reads the durable
protocol-v1 authority projection and live state; the typed `thread.authority`
protocol readback carries the complete immutable record and confinement fact.

## 3. Durable runs

```bash
plato "read Cargo.toml and name the package"
# stderr prints: run_id / ledger_path / the exact replay command
plato -c "what did I ask you to inspect?"
plato replay                # replays the latest session
```

Explicit replay paths use the equals form: `--db=/tmp/run.db`. Prompts always
use the server-owned workspace ledger. Replay is read-only and fully offline;
it finds the selected per-run JSONL through the state database and shows final
assistant messages, not partial live deltas. Runs created before the JSONL
transition still replay from their SQLite event rows.

## 4. The full experience: TUI

One terminal, same workspace:

```bash
plato
# Start the TUI's local session in yolo mode instead:
plato --tui --yolo
```

This ensures the host-scoped `platonic serve`, asks once to register an unknown
directory (Enter accepts the basename), asks for the root thread spawn
decision, and attaches the TUI to that durable thread. Quitting the TUI leaves
the server and thread authority available. `plato --tui --config plato.toml`
is the explicit form when selecting a config. During a run, one working row
shows elapsed active time and the interrupt key. Use `plato --reduced-motion`
or set `PLATO_REDUCED_MOTION=1` to replace its animated braille marker with a
static bullet.
The screen is a chat-first transcript with a bottom status rule and composer.

From another terminal in the same workspace, list the durable thread and
attach another interactive client:

```bash
plato thread list
plato thread status <thread-id>
plato --remote <thread-id>
```

Both clients observe the same live output. Exactly one controller owns an
active turn; another client is refused until that turn is idle, then can drive
the next turn. Remote attachment does not create a duplicate thread.

Quitting either TUI form never stops the server.

Start the optional Discord connector from a separate environment:

```bash
unset OPENAI_API_KEY OPENROUTER_API_KEY
platonic gateway discord --workspace "$PWD" \
  --config "$HOME/.config/plato/gateway.toml"
```

Run that command only from a private environment that loaded the token from
`$HOME/.config/plato/discord-bot-token` outside terminal or pane input. Never
put its literal value in `argv`, pane text, logs, GitHub, or chat. The [gateway
guide](GATEWAY.md) owns the file setup and `gateway-live` channel walkthrough.

The gateway attaches to the host endpoint and requires a successful `hello` for
the selected workspace. Probe failures start no connector; the gateway never
starts a server with its Discord environment. An explicit `--socket` remains a
test/operator override.

The TUI footer is contextual by default and moves model and workspace context
out of the transcript. Press `?` for the shared shortcut overlay. The footer
switches to a second-press quit hint after cancel and to
`daemon unavailable — r to reconnect` while offline. At 120 columns it includes
model and workspace context; below 120 columns that right-side context drops,
below 80 the queue hint drops, and below 40 the remaining `?` hint truncates
without wrapping.
When yolo is active, the footer shows `yolo` for the selected session or `yolo
next` before a fresh session exists. Use `/yolo on|off` to change that
daemon-lifetime local profile; `/status` reads it back authoritatively.

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
bounded notification with the tool, effect, and preview. An admitted home-config
principal can use `/approve` or `/deny` in the mapped channel; the gateway binds
the decision to that exact pending operation and records the principal name as
attribution. Failed runs post `Run failed. Inspect it locally with: plato replay`. Canceled and
interrupted runs do not post terminal messages. Allowed messages show 👀 and a
typing indicator while active, then ✅ or ❌; canceled and interrupted runs
remove 👀 without a terminal reaction. The bot needs Add Reactions and Read
Message History, plus Send Messages in Threads when threads are used.

After closing the TUI and gateway, stop the idle host server explicitly:

```bash
platonic shutdown --workspace "$PWD"
```

## 5. Local voice activation and device proof

TUI voice is opt-in through a dedicated client file that the server never reads.
Choose every local model explicitly; Plato Agent does not search for or download
artifacts. Relative model paths resolve from the voice file's directory.

```toml
[voice]
kokoro_model = "/models/kokoro-82m"
whisper_model = "/models/ggml-large-v3-turbo.bin"
silero_model = "/models/silero_vad.onnx"
# capture_device = "exact cpal input device ID"
# playback_device = "exact cpal output device ID"
```

```bash
plato --tui --voice-config /path/to/voice.toml
# In the TUI, /voice on is the one session-local device grant.
# /voice off stops capture, drains accepted speech, and closes both devices.
```

Missing, unreadable, incomplete, or unknown configuration fails closed in the
TUI status line. Voice starts off after every client restart and `/new` turns it
off before selecting a fresh session. Install espeak-ng, CUDA, and the native
cpal backend headers, then place the pinned Kokoro, Silero v6.2.1, and
large-v3-turbo artifacts described in
[`../crates/plato-audio/README.md`](../crates/plato-audio/README.md) outside the
repository for the focused device proofs below.

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
`narrated_run` runs through the host server and reports its server-owned
workspace ledger path. Its proof JSON includes the exact revision-one `VoiceEvent`
envelopes observed by the client. Offline `plato replay --db=/path/to/run.db
--run RUN_ID` reads the server-owned per-run log without starting or contacting
the server. New voice companion streams use that same JSONL file; legacy runs
remain readable from SQLite.

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
