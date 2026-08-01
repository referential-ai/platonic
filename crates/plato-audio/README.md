# plato-audio

`plato-audio` is Plato Agent's synchronous local audio IO leaf. It owns typed
PCM and sentence values, fixed threshold endpointing, the sans-IO sentence
cutter and prefetch state, resident Kokoro and Whisper engines, and persistent
cpal input/output streams. It does not depend on a Platonic crate and owns no
run, ledger, policy, approval, session, daemon, protocol, or
configuration-registry behavior.

AU2 moves synthesis onto one owned `std::thread`. A fixed four-sentence
accepted-but-not-finished window feeds one bounded `rtrb` SPSC PCM ring. One
`rubato` plan converts Kokoro's 24 kHz mono f32 output to the live device rate
before the callback; the callback only drains, converts samples, records atomic
timing, and emits silence on underrun.

AU3 adds one persistent input callback that only copies native samples into a
bounded `rtrb` ring and records overflow. One owned worker normalizes and
resamples complete device frames to 16 kHz mono, applies a literal 10 ms RMS
threshold (`0.015`) with three-window onset, 200 ms minimum speech, and 250 ms
hangover, then returns one final transcript for an explicit capture request.
Overflow is a typed terminal capture result. There is no ambient recognition,
partial transcript UI, barge-in, AEC, wake word, cloud fallback, or second
recognizer.

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

## Native runtime

The crate pins `ort 2.0.0-rc.13` (ONNX Runtime 1.28, CUDA 13 build) and
`cpal 0.18.1`, `rtrb 0.3.4`, and `rubato 4.0.0`. On Linux x86_64 it attempts
CUDA device zero with registration errors enabled, then constructs a CPU
session if CUDA cannot load. Other targets construct the CPU session directly.

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
cargo test --locked -p plato-audio
cargo run --release --locked -p plato-audio --example kokoro_device_proof

PLATO_AUDIO_WHISPER_MODEL="$whisper_model" \
  cargo test --release --locked -p plato-audio --features whisper-cuda \
  recorded_corpus_has_exact_endpoint_transcript_and_warm_latency -- --ignored --nocapture

CUDA_VISIBLE_DEVICES=-1 PLATO_AUDIO_WHISPER_MODEL="$whisper_model" \
  cargo test --locked -p plato-audio --features whisper-cuda \
  runtime_without_visible_cuda_device_fails_closed -- --ignored

PLATO_AUDIO_FIXTURE_KEY=local-proof \
  cargo run --release --locked --example narrated_run -- --fixture

cargo run --locked --example narrated_run -- --list-input-devices
PLATO_AUDIO_FIXTURE_KEY=local-proof \
  cargo run --release --locked --features whisper-cuda --example narrated_run -- \
  --fixture --whisper-model "$whisper_model" --input-device CPAL_ID
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

The narrated-run fixture uses the real root `run_question` driver and existing
assistant-delta event channel with a credential-free loopback SSE provider. It
checks the spoken sentence sequence against the committed final response and
does not add assistant deltas to the durable harness ledger.

The tracked `fixtures/au3` corpus is non-human CC0 audio with explicit source,
license, and checksums. Its CUDA test requires exactly one retained VAD segment
with no rejected or additional event, then asserts the exact endpoint and
transcript, no finalization for below-threshold noise, one model load, and 20
warm VAD-close-to-final trials with p95 at most 300 ms. A separate hidden-device
test proves that compiled CUDA capability cannot admit whisper.cpp's CPU
fallback. The captured-run form arms exactly one microphone question, passes
its final `Transcript` into the same typed `RunOptions.question` path, and
speaks the answer through AU2. It retains only transcript, metrics, and
device/model provenance, never raw live audio.

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
