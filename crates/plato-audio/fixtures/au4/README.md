# AU4 Speech-Plus-Noise Corpus

This non-human corpus is purpose-built from the AU3 CC0 eSpeak question and
deterministic FFmpeg noise. Referential AI releases the combined fixture under
CC0-1.0. No microphone or personal audio was used.

`speech-plus-noise.wav` is 82,944 samples of 16 kHz mono signed 16-bit PCM. It
contains a 450 ms high-energy white-noise event that the AU3 RMS threshold
admits as a false utterance, followed by the original synthesized question over
steady low room noise. The only annotated speech interval is samples
32,000..60,320. The noise event is samples 8,000..15,200.

It was generated with FFmpeg n8.1.2 from the committed AU3 fixture and these
literal lavfi sources:

```text
silence 0.50000 s
white noise amplitude=0.04 seed=329 duration=0.45000 s
silence 0.55000 s
AU3 spoken-question.wav
silence 0.52875 s
white background amplitude=0.003 seed=3294 duration=5.18400 s
```

The event track and background are mixed without normalization, then encoded
as `pcm_s16le`. `manifest.json` pins source identity, annotations, format, and
the resulting WAV checksum. Live or recorded human audio must never replace
this tracked fixture.

```text
228cecd260153155a4c6ec7b8f7a25a519c6c934e4451aad2820465b548d6658  manifest.json
ce0775c71a2bb748234a92a2c446997d17c299a56a04d38cfa43975fa6245ff3  speech-plus-noise.wav
```

The aggregate corpus SHA-256 is
`b70723e810ea53c39dff05d0bb746eb89e7dbeb76648c555e1330fbffbebe8f4`.
It is computed over the exact `manifest.json` bytes followed immediately by the
exact `speech-plus-noise.wav` bytes.

## Long final-window fixture

`long-utterance.wav` is a separate 397,752-sample (24.859 second) CC0 fixture
for bounded incremental Whisper proof. It was synthesized from the literal text
in `long-utterance.json` with eSpeak NG 1.52.0 (`en-us`, 165 words/minute), then
resampled to 16 kHz mono signed 16-bit PCM and padded with 250 ms of leading and
trailing silence by FFmpeg n8.1.2. The manifest pins both the source text and the
exact large-v3-turbo CUDA transcript used to prove that stable committed text is
not mechanically truncated when retained PCM rolls over.

```text
7cd6da0efa9db84c6cbbbade052fa9d088e3114c6bc3a85ce113aedca7a4deee  long-utterance.json
71129abe7edb62301ab3c7bd035d999cb1b43d0ab4d92665e387555f3b5ec1d0  long-utterance.wav
```

## Production capture-path timing

The production-path proof feeds a named PipeWire/Pulse null sink, captures its
monitor through cpal device `alsa:pulse`, and renders every rolling hypothesis
through the real root TTY. Generate the 24-copy headerless S16 payload without
changing the tracked WAV:

```bash
ffmpeg -hide_banner -loglevel error -y -stream_loop 23 \
  -i speech-plus-noise.wav -f s16le -acodec pcm_s16le -ac 1 -ar 16000 \
  /tmp/plato-329-au4-cpal-24x.raw
sha256sum /tmp/plato-329-au4-cpal-24x.raw
```

The expected hashes are:

```text
b55089e93fce31bdb40af141a22f9d8b3380a81ad28426244d68f73cc6d26fa6  /tmp/plato-329-au4-cpal-24x.raw
```

The feeder uses `pacat` so Pulse paces these samples on its audio clock. A shell
trap must stop the feeder and unload the named null-sink module on either test
result. The payload is virtual recorded input, not a physical microphone or
live human voice. This arrangement exercises production cpal selection and
callback, rtrb, worker normalization, Silero/Whisper, and root TTY presentation
without claiming audio latency before callback entry.
