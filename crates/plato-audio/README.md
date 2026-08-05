# plato-audio

`plato-audio` is Plato Agent's synchronous local audio IO leaf. It owns typed
PCM, sentence, partial-transcript, and final-transcript values; neural endpoint
state; the sans-IO sentence cutter and prefetch state; resident Kokoro, Silero,
and Whisper engines; and persistent cpal input/output streams. It does not
depend on a Platonic crate and owns no run, ledger, policy, approval, session,
daemon, display protocol, or configuration-registry behavior.

AU2 moves synthesis onto one owned `std::thread`. A fixed four-sentence
accepted-but-not-finished window feeds one bounded `rtrb` SPSC PCM ring. One
`rubato` plan converts Kokoro's 24 kHz mono f32 output to the live device rate
before the callback; the callback only drains, converts samples, records atomic
timing, and emits silence on underrun.

AU3 adds one persistent input callback that only copies native samples into a
bounded `rtrb` ring and records overflow. One owned worker normalizes and
resamples complete device frames to 16 kHz mono. AU4 replaces its production
RMS endpoint with one warm Silero v6.2.1 session: 512-sample frames, probability
threshold `0.5`, four-frame minimum speech, and eight-frame hangover. The AU3
threshold detector remains test-only as the pinned comparison baseline.

While Silero holds an utterance open, the same worker feeds each gated frame to
one resident Whisper large-v3-turbo state. Whisper re-decodes at a fixed 160 ms
cadence after 320 ms of speech. It commits only byte-stable leading model text
at validated timestamp boundaries outside a fixed one-second overlap, retains
at most five seconds of pending PCM, and forces rollover before sample 80,001.
If no stable boundary exists at that cap, recognition fails closed. Endpoint
finalization decodes only the pending window and concatenates its exact text
with the stable prefix; total span remains the full accepted PCM duration.
Empty or unchanged hypotheses are suppressed and all rolling updates remain
typed `Transcript { is_final: false }` values. Root replaces the active display
line and starts no run until the one final transcript arrives at the Silero
endpoint.

AU5 keeps that same resident Silero state running while narrated PCM is active.
It discards input through a fixed 150 ms self-playback gate, then a qualified
speech onset sets the run's existing `Arc<AtomicBool>`. The output callback
checks that atomic at callback entry and fills the complete quantum with silence;
the synthesis worker, never the callback, replaces the PCM ring and clears the
four-sentence prefetch window. A sans-IO latch maps actual emitted samples back
to one normalized spoken prefix and assistant sentence/delta position. Root
adds that latch once as `voice.interruption` in the next run's `ContextBuilt`
current-task lane. Generic cancellation uses the same path without fabricating
an interruption latch. There is no AEC, wake word, cloud fallback, second
recognizer, autonomous follow-up run, or post-playback ambient recognition.

## Pinned artifacts

The supported artifacts come from
`onnx-community/Kokoro-82M-v1.0-ONNX` at immutable commit
`1939ad2a8e416c0acfeecc08a694d14ef25f2231` (Apache-2.0). Keep them outside
the repository:

```bash
revision=1939ad2a8e416c0acfeecc08a694d14ef25f2231
model_dir="$HOME/.cache/plato-audio/kokoro-82m-v1.0-onnx-$revision"
mkdir -p "$model_dir"
curl -fL "https://huggingface.co/onnx-community/Kokoro-82M-v1.0-ONNX/resolve/$revision/onnx/model.onnx?download=true" -o "$model_dir/model.onnx"
curl -fL "https://huggingface.co/onnx-community/Kokoro-82M-v1.0-ONNX/resolve/$revision/tokenizer.json?download=true" -o "$model_dir/tokenizer.json"
curl -fL "https://huggingface.co/onnx-community/Kokoro-82M-v1.0-ONNX/resolve/$revision/voices/af_sky.bin?download=true" -o "$model_dir/af_sky.bin"
sha256sum "$model_dir/model.onnx" "$model_dir/tokenizer.json" "$model_dir/af_sky.bin"
```

Expected SHA-256 values:

```text
8fbea51ea711f2af382e88c833d9e288c6dc82ce5e98421ea61c058ce21a34cb  model.onnx
77a02c8e164413299b4b4c403b14f8e0e1c1b727db4d46a09d6327b861060a34  tokenizer.json
4435255c9744f3f31659e0d714ab7689bf65d9e77ec1cce060f083912614f0b9  af_sky.bin
```

`KokoroSynthesizer::load` verifies all three digests before constructing the
session. Downloaded model files are never packaged or committed.

AU3 admits `ggml-large-v3-turbo.bin` from `ggerganov/whisper.cpp` commit
`6034871ec87c84e342efab769d4c5c06cd126db3`. Keep it outside the repository:

```bash
whisper_revision=6034871ec87c84e342efab769d4c5c06cd126db3
whisper_model="$HOME/.cache/plato-audio/ggml-large-v3-turbo-$whisper_revision.bin"
curl -fL "https://huggingface.co/ggerganov/whisper.cpp/resolve/$whisper_revision/ggml-large-v3-turbo.bin?download=true" -o "$whisper_model"
sha256sum "$whisper_model"
```

The required SHA-256 is
`1fc70f774d38eb169993ac391eea357ef47c88757ef72ee5943879b7e8e2bc69`.
`WhisperRecognizer::load` verifies it before constructing one resident state.

AU4 admits `silero_vad.onnx` from `snakers4/silero-vad` tag `v6.2.1`, immutable
commit `7e30209a3e901f9842f81b225f3e93d8199902b1` (MIT). Keep it outside the
repository:

```bash
silero_revision=7e30209a3e901f9842f81b225f3e93d8199902b1
silero_dir="$HOME/.cache/plato-audio/silero-vad-$silero_revision"
mkdir -p "$silero_dir"
curl -fL "https://raw.githubusercontent.com/snakers4/silero-vad/$silero_revision/src/silero_vad/data/silero_vad.onnx" -o "$silero_dir/silero_vad.onnx"
sha256sum "$silero_dir/silero_vad.onnx"
```

The required SHA-256 is
`1a153a22f4509e292a94e67d6f9b85e8deb25b4988682b7e174c65279d8788e3`.
`SileroVad::load_with_runtime` verifies it before constructing one resident
session.

## Native runtime

The crate pins `ort 2.0.0-rc.13` (ONNX Runtime 1.28, CUDA 13 build) and
`cpal 0.18.1`, `rtrb 0.3.4`, and `rubato 4.0.0`. Root creates one explicit
`OrtRuntime` owner and passes clones to Kokoro and Silero. Each model constructs
one warm session through that owner; no frame or utterance constructs a runtime
or session. On Linux x86_64 each ONNX model attempts CUDA device zero with
registration errors enabled, then constructs a CPU session if CUDA cannot
load. Other targets construct CPU sessions directly.

espeak-ng is invoked as a fixed external executable, not linked into this
dual-licensed crate. The admitted proof host used these signed Arch packages:

```text
cuda 13.3.1-1                    LicenseRef-NVIDIA-CUDA
cudnn 9.25.0.15-1               LicenseRef-NVIDIA-cuDNN
espeak-ng 1.52.0-1              GPL-3.0-or-later external executable
```

Linux compilation also requires ALSA development headers. For example,
install `libasound2-dev` on Ubuntu. The hardware proof deliberately requires
CUDA; the Kokoro path retains its typed CPU fallback.

Whisper uses pinned `whisper-rs 0.16.0` and is compiled only by the explicit
`whisper-cuda` feature. That feature requires CUDA device zero and flash
attention; it returns a typed error rather than falling back to CPU. Default
builds retain the public types and fail closed if the CUDA recognizer is
requested, keeping ordinary hosted builds platform-neutral.

## Proof commands

```bash
export PLATO_AUDIO_KOKORO_DIR="$model_dir"
export PLATO_AUDIO_SILERO_MODEL="$silero_dir/silero_vad.onnx"
cargo test --locked -p plato-audio
cargo run --release --locked -p plato-audio --example kokoro_device_proof

PLATO_AUDIO_SILERO_MODEL="$PLATO_AUDIO_SILERO_MODEL" \
  cargo run --release --locked -p plato-audio --example barge_in_device_proof \
  > docs/proofs/issue-330-barge-in-device.json

PLATO_AUDIO_SILERO_MODEL="$PLATO_AUDIO_SILERO_MODEL" \
  cargo test --release --locked -p plato-audio \
  silero_strictly_reduces_au3_false_cuts_without_missing_speech -- --ignored --nocapture

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
  PLATO_AUDIO_KOKORO_DIR="$PLATO_AUDIO_KOKORO_DIR" \
  PLATO_AUDIO_SILERO_MODEL="$PLATO_AUDIO_SILERO_MODEL" \
  PLATO_AUDIO_WHISPER_MODEL="$whisper_model" \
    cargo test --release --locked --features whisper-cuda \
    twenty_warm_rtx4090_live_partial_and_final_trials_meet_au4_bounds -- --ignored --nocapture
)

PLATO_AUDIO_WHISPER_MODEL="$whisper_model" \
  cargo test --release --locked -p plato-audio --features whisper-cuda \
  au3_threshold_corpus_final_and_silence_regression_remains_exact -- --ignored

PLATO_AUDIO_WHISPER_MODEL="$whisper_model" \
  cargo test --release --locked -p plato-audio --features whisper-cuda \
  long_utterance_final_is_bounded_and_preserves_exact_stable_text -- --ignored

PLATO_AUDIO_WHISPER_MODEL="$whisper_model" \
  cargo test --release --locked -p plato-audio --features whisper-cuda \
  twenty_long_utterance_finals_are_bounded_and_preserve_exact_stable_text \
  -- --ignored --nocapture

CUDA_VISIBLE_DEVICES=-1 PLATO_AUDIO_WHISPER_MODEL="$whisper_model" \
  cargo test --locked -p plato-audio --features whisper-cuda \
  runtime_without_visible_cuda_device_fails_closed -- --ignored

PLATO_AUDIO_FIXTURE_KEY=local-proof \
  cargo run --release --locked --example narrated_run -- --fixture

cargo run --locked --example narrated_run -- --list-input-devices
PLATO_AUDIO_FIXTURE_KEY=local-proof \
  cargo run --release --locked --features whisper-cuda --example narrated_run -- \
  --fixture --whisper-model "$whisper_model" \
  --silero-model "$PLATO_AUDIO_SILERO_MODEL" --input-device CPAL_ID
```

The device proof opens the model and stream before timing, excludes one warmup,
then runs exactly 20 warm sentence-acceptance trials. It also admits a fixed
four-sentence corpus at once and reports callback/sample timestamps, every
inter-sentence gap, device period, underruns, exact order, and synthesis/playback
overlap. It exits unsuccessfully unless CUDA is active, TTFA p95 is at most
350 ms, every gap is at most 20 ms, a measured synthesis N+1 interval overlaps
playback N, the maximum unfinished count is four, and shutdown joins the single
worker and closes the stream. Later sentences may finish synthesis even earlier
as the prefetch fills.

The AU5 device proof runs 25 Silero decisions against one actual persistent
output stream and exits unsuccessfully unless every decision-to-first-all-silent
callback interval is at most 30 ms. It records p50/p95/max latency, callback
quantum, gate state, sentence and PCM queue depths, flush counts, backend, and
device format. Its CC0 synthetic speech-plus-noise WAV is fed directly to the
resident Silero state after the gate; this proves the output callback boundary,
not physical-microphone, live-speech, cpal-input, or acoustic-loop latency. The
committed report is `../../docs/proofs/issue-330-barge-in-device.json`.

The narrated-run fixture uses the real root `run_question` driver and existing
assistant-delta event channel with a credential-free loopback SSE provider. It
checks the spoken sentence sequence against the committed final response and
does not add assistant deltas to the durable harness ledger.

The tracked `fixtures/au4` corpus is non-human CC0 audio with exact source,
annotations, and checksums. Its scorer compares the AU3 threshold baseline and
Silero sample by sample, reports both confusion matrices and endpoint deltas,
and requires fewer false cuts without more missed speech. Its RTX 4090 proof
paces repeated recorded WAV bytes into a named PipeWire/Pulse null-sink monitor
and opens that virtual source through the production cpal capture worker. It
excludes one warmup, runs 20 utterances through the same resident Silero and
Whisper sessions, and requires callback-entry-to-visible-partial p95 at most
200 ms plus closing-VAD-evaluation-entry-to-visible-final p95 at most 120 ms.
Those boundaries include rtrb, worker normalization, inference, channel
delivery, and real root stderr TTY writes, but not audio time before cpal
callback entry. This is recorded virtual input, not a physical-microphone or
live-human-speech claim. The separate 24.859-second fixture proves 20 warm
final-window decodes stay bounded while preserving the exact committed
transcript. The committed proof artifacts are
`../../docs/proofs/issue-329-vad-corpus.json` and
`../../docs/proofs/issue-329-whisper-partials.json`, plus
`../../docs/proofs/issue-329-whisper-long-final.json`.

The AU3 corpus remains an exact final-text and silence regression. A separate
hidden-device test proves that compiled CUDA capability cannot admit
whisper.cpp's CPU fallback. The captured-run form arms exactly one microphone
question, replaces partials on stderr, passes only its final `Transcript` into
the same typed `RunOptions.question` path, and speaks the answer through AU2.
It retains transcripts, metrics, and device/model provenance, never raw live
audio.

## Package boundary

The direct dependency graph is deliberately one-way:

```text
plato-agent -> plato-audio -> cpal / rtrb / rubato
                           -> ort
                           -> whisper-rs (only with whisper-cuda)
                           -> serde / serde_json / sha2 / thiserror
```

Run `cargo tree -p plato-audio --edges normal` to inspect the complete external
closure. No `plato-*` or `platonic-core` package appears beneath this leaf.
