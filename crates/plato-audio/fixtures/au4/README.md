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
