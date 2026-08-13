# Quickstart entry point

This file selects the guide that matches the command bundle. It is not a
second copy of the user manual.

<a id="0-one-time-setup"></a>

## Current public release: 0.1.0

Install the released 0.1.0 bundle for Linux x86-64 or macOS Apple silicon:

```bash
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) target=linux-x86_64 ;;
  Darwin-arm64) target=macos-arm64 ;;
  *) echo "unsupported platform" >&2; exit 1 ;;
esac

bundle="platonic-0.1.0-$target"
release="https://github.com/referential-ai/platonic/releases/download/platonic-v0.1.0"
curl -fLO "$release/$bundle.tar.gz"
curl -fLO "$release/$bundle.sha256"

if command -v sha256sum >/dev/null 2>&1; then
  sha256sum --check "$bundle.sha256"
else
  shasum -a 256 --check "$bundle.sha256"
fi

tar -xzf "$bundle.tar.gz"
install -d "$HOME/.local/bin"
install -m 0755 "$bundle/bin/platonic" "$HOME/.local/bin/platonic"
install -m 0755 "$bundle/bin/plato" "$HOME/.local/bin/plato"
install -m 0755 "$bundle/bin/plato-tui" "$HOME/.local/bin/plato-tui"
export PATH="$HOME/.local/bin:$PATH"

platonic --version
plato --version
plato-tui --version
```

The tag is exactly `platonic-v0.1.0`. See the
[release contract](https://github.com/referential-ai/platonic/blob/develop/docs/RELEASE.md)
for archive contents, supported targets, and verification details.

## Unreleased develop / 0.2.0

The Starlight [user overview](https://docs.referential.ai/user/) and
[first productive journey](https://docs.referential.ai/user/first-run/)
document current `develop` behavior. They use one OpenRouter route and exact
what-you-see checkpoints with binaries built from the exact source commit. No
released 0.2.0 bundle or bundle-install proof exists, so they are review
documentation, not the current public release guide, until
`platonic-v0.2.0` exists.

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
TUI status line. Voice starts off after every client restart. Once enabled, the
TUI captures whenever no submission is active and sends each final transcript
through the ordinary composer route: `thread.send` for an attached thread,
`message.append` for a selected session, or `run.start` otherwise. Exact daemon
deltas are narrated in order, with the durable response as the final equality
check. `/voice off`, `/new`, and TUI exit close local capture and playback but do
not cancel a continuing text run. Install espeak-ng, CUDA, and the native cpal
backend headers, then place the pinned Kokoro, Silero v6.2.1, and large-v3-turbo
artifacts described in
[`crates/plato-audio/README.md`](https://github.com/referential-ai/platonic/blob/develop/crates/plato-audio/README.md)
outside the repository for the focused device proofs below.

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
envelopes observed by the client, but the example does not commit them. The
native protocol accepts one complete batch through `voice.events.commit` and
returns server-minted envelopes through `voice.events.read`. Offline `plato replay --db=/path/to/run.db
--run RUN_ID` reads the server-owned per-run log without starting or contacting
the server. New voice companion streams use that same JSONL file; legacy runs
remain readable from SQLite. A live client retries only the unchanged in-memory
batch. There is no voice outbox: a client crash before commit acknowledgement
can lose voice observations, but never the durable question or text run.

`VoiceCaptured` stores only the final transcript's SHA-256 and UTF-8 byte
length, transcript span, native and 16 kHz frame counts, VAD sample boundaries,
and capture timing. `VoiceSpoken` stores the AU2 sentence-acceptance to first
non-silent callback TTFA in whole milliseconds plus sentence/interruption
coordinates; an AU5 latch adds one exact `VoiceInterrupted` prefix and delta
index. When submitted through `voice.events.commit`, these companion facts are
committed atomically beside the core ledger, never as `HarnessEvent` variants.

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
quantum, and flushes synthesis/prefetch outside the real-time callback. The same
utterance continues through final recognition while the TUI cancels the daemon
run. Only after the terminal result and acknowledged interrupted VoiceEvent
batch does the TUI submit that utterance with the prior run ID; the server then
derives the one-turn interruption context from committed facts. Plain Ctrl-C
silences before remote cancellation but creates no interruption fact or next
prompt.

The client-to-voice queue holds exactly 128 events and never blocks daemon
polling. Audio retains one capture command, eight capture updates, a 30-second
utterance limit, four unfinished synthesis sentences, and fixed PCM rings.
Queue overflow, lag, disconnect, malformed deltas, or audio worker failure
silences and abandons current narration without replaying possibly audible
text. The text run continues unless Ctrl-C or barge-in explicitly cancels it,
and voice may re-arm for a later run after recovery. A captured question whose
admitted run fails commits only its capture fact.

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

## Next guides

Continue with the Starlight
[User operations guide](https://docs.referential.ai/user/operations/) for daily
operation, approvals, and providers, or the
[Developer guide](https://docs.referential.ai/developer/) for server and protocol
internals.
