use super::*;
use std::sync::atomic::AtomicUsize;

use crate::{
    InferenceBackend, InputDeviceSelection, SILERO_HANGOVER_FRAMES, SILERO_MINIMUM_SPEECH_FRAMES,
    SILERO_WINDOW_SAMPLES, VadError,
};

struct FakeVad;

struct FailingVad;

impl VoiceActivityDetector for FakeVad {
    fn frame_samples(&self) -> usize {
        SILERO_WINDOW_SAMPLES
    }

    fn reset(&mut self) {}

    fn speech_probability(&mut self, samples: &[f32]) -> Result<f32, VadError> {
        assert_eq!(samples.len(), SILERO_WINDOW_SAMPLES);
        Ok(if samples.iter().any(|sample| sample.abs() >= 0.02) {
            0.9
        } else {
            0.1
        })
    }
}

impl VoiceActivityDetector for FailingVad {
    fn frame_samples(&self) -> usize {
        SILERO_WINDOW_SAMPLES
    }

    fn reset(&mut self) {}

    fn speech_probability(&mut self, _samples: &[f32]) -> Result<f32, VadError> {
        Err(VadError::Inference {
            backend: InferenceBackend::Cpu,
            reason: "synthetic neural failure".to_owned(),
        })
    }
}

struct FakeRecognizer {
    samples: usize,
    finalizations: Arc<AtomicUsize>,
    failure: bool,
}

struct PanickingRecognizer {
    samples: usize,
}

struct DropRecognizer {
    dropped: Arc<AtomicUsize>,
}

impl Drop for DropRecognizer {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }
}

impl SpeechRecognizer for DropRecognizer {
    fn input_format(&self) -> AudioFormat {
        whisper_format()
    }

    fn accept(&mut self, _frame: &PcmFrame) -> Result<Vec<Transcript>, SttError> {
        unreachable!("failed startup cannot accept PCM")
    }

    fn reset(&mut self) {}

    fn finalize(&mut self) -> Result<Transcript, SttError> {
        unreachable!("failed startup cannot finalize")
    }
}

impl SpeechRecognizer for PanickingRecognizer {
    fn input_format(&self) -> AudioFormat {
        whisper_format()
    }

    fn accept(&mut self, _frame: &PcmFrame) -> Result<Vec<Transcript>, SttError> {
        self.samples += 1;
        Ok(Vec::new())
    }

    fn reset(&mut self) {
        self.samples = 0;
    }

    fn finalize(&mut self) -> Result<Transcript, SttError> {
        assert!(self.samples > 0);
        panic!("synthetic recognizer panic")
    }
}

impl SpeechRecognizer for FakeRecognizer {
    fn input_format(&self) -> AudioFormat {
        whisper_format()
    }

    fn accept(&mut self, _frame: &PcmFrame) -> Result<Vec<Transcript>, SttError> {
        self.samples += 1;
        match self.samples {
            2_048 => Ok(vec![Transcript::new("synthetic", false, 128)?]),
            4_096 => Ok(vec![Transcript::new("synthetic question", false, 256)?]),
            _ => Ok(Vec::new()),
        }
    }

    fn reset(&mut self) {
        self.samples = 0;
    }

    fn finalize(&mut self) -> Result<Transcript, SttError> {
        self.finalizations.fetch_add(1, Ordering::Relaxed);
        if self.failure {
            return Err(SttError::Inference {
                reason: "synthetic failure".to_owned(),
            });
        }
        let span_ms = self.samples as u64 * 1_000 / 16_000;
        self.samples = 0;
        Transcript::new("synthetic question", true, span_ms)
    }
}

fn fake_recognizer(failure: bool) -> (FakeRecognizer, Arc<AtomicUsize>) {
    let finalizations = Arc::new(AtomicUsize::new(0));
    (
        FakeRecognizer {
            samples: 0,
            finalizations: Arc::clone(&finalizations),
            failure,
        },
        finalizations,
    )
}

fn format(rate: u32, channels: u16, sample: SampleFormat) -> AudioFormat {
    AudioFormat::new(rate, channels, sample).unwrap()
}

fn utterance() -> Vec<f32> {
    let mut samples = vec![0.05; usize::from(SILERO_MINIMUM_SPEECH_FRAMES) * SILERO_WINDOW_SAMPLES];
    samples.extend(vec![
        0.0;
        usize::from(SILERO_HANGOVER_FRAMES)
            * SILERO_WINDOW_SAMPLES
    ]);
    samples
}

fn feed_after_arm(mut callback: CallbackWriter, samples: Vec<f32>) -> JoinHandle<()> {
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        callback.write(&samples, CaptureSample::F32);
    })
}

#[test]
fn armed_ring_overflow_is_a_typed_terminal_outcome() {
    let (recognizer, finalizations) = fake_recognizer(false);
    let (worker, callback, _) =
        CaptureWorker::test_worker(format(16_000, 1, SampleFormat::F32), 4, FakeVad, recognizer);
    let feeder = feed_after_arm(callback, vec![0.05; 64 * SILERO_WINDOW_SAMPLES]);
    assert!(matches!(
        worker.capture(Duration::from_secs(2)),
        Err(CaptureError::RingOverflow {
            callbacks: 1,
            dropped_samples: 1..
        })
    ));
    feeder.join().unwrap();
    assert_eq!(finalizations.load(Ordering::Relaxed), 0);
}

#[test]
fn capture_setup_preserves_the_original_typed_error() {
    let (reply, result) = mpsc::sync_channel(1);
    let active = start_capture(
        1,
        Duration::from_secs(1),
        reply,
        Err(CaptureError::NonFiniteInput),
        NeuralVadState::new(SILERO_WINDOW_SAMPLES),
        CaptureOverflow::default(),
    );
    assert!(active.is_none());
    assert!(matches!(
        result.recv().unwrap(),
        CaptureMessage::Complete(Err(CaptureError::NonFiniteInput))
    ));
}

#[test]
fn worker_panic_reaches_the_request_and_teardown_joins_the_thread() {
    let (worker, callback, _) = CaptureWorker::test_worker(
        format(16_000, 1, SampleFormat::F32),
        16_384,
        FakeVad,
        PanickingRecognizer { samples: 0 },
    );
    let feeder = feed_after_arm(callback, utterance());
    assert!(matches!(
        worker.capture(Duration::from_secs(2)),
        Err(CaptureError::WorkerPanicked)
    ));
    feeder.join().unwrap();
    assert!(matches!(
        worker.capture(Duration::from_secs(1)),
        Err(CaptureError::WorkerPanicked)
    ));
    let shutdown = worker.shutdown();
    assert!(shutdown.worker_joined);
    assert!(shutdown.input_closed);
    assert!(shutdown.worker_panicked);
}

#[test]
fn worker_thread_start_failure_is_typed_and_bounded() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let (producer, consumer) = RingBuffer::new(4);
    drop(producer);
    let (commands, requests) = mpsc::sync_channel(1);
    drop(commands);
    let result = spawn_worker_with(
        consumer,
        format(16_000, 1, SampleFormat::F32),
        CaptureEngines {
            detector: Box::new(FakeVad),
            recognizer: Box::new(DropRecognizer {
                dropped: Arc::clone(&dropped),
            }),
        },
        requests,
        Arc::new(AtomicBool::new(false)),
        Arc::new(CaptureCounters::default()),
        |_task| Err(std::io::Error::other("synthetic thread refusal")),
    );
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("synthetic thread launcher must fail"),
    };
    assert!(matches!(
        error,
        CaptureError::WorkerThreadStart { reason }
            if reason == "synthetic thread refusal"
    ));
    assert_eq!(dropped.load(Ordering::Relaxed), 1);
}

#[test]
fn one_explicit_capture_returns_one_final_endpoint() {
    let (recognizer, finalizations) = fake_recognizer(false);
    let (worker, callback, _) = CaptureWorker::test_worker(
        format(16_000, 1, SampleFormat::F32),
        16_384,
        FakeVad,
        recognizer,
    );
    let feeder = feed_after_arm(callback, utterance());
    let mut delivered = Vec::new();
    let report = worker
        .capture_with_partials(Duration::from_secs(2), |partial| {
            delivered.push(partial.transcript.text.clone());
        })
        .unwrap();
    feeder.join().unwrap();
    assert!(report.transcript.is_final);
    assert_eq!(report.transcript.text, "synthetic question");
    assert_eq!(report.endpoint.start_sample, 0);
    assert_eq!(report.endpoint.speech_end_sample, 2_048);
    assert_eq!(report.endpoint.close_sample, 6_144);
    assert_eq!(
        report
            .partials
            .iter()
            .map(|partial| partial.transcript.text.as_str())
            .collect::<Vec<_>>(),
        ["synthetic", "synthetic question"]
    );
    assert_eq!(delivered, ["synthetic", "synthetic question"]);
    assert_eq!(finalizations.load(Ordering::Relaxed), 1);
    assert_eq!(worker.metrics().transcripts, 1);
    let shutdown = worker.shutdown();
    assert!(shutdown.worker_joined);
    assert!(shutdown.input_closed);
}

#[test]
fn neural_inference_failure_is_typed_and_never_finalizes() {
    let (recognizer, finalizations) = fake_recognizer(false);
    let (worker, callback, _) = CaptureWorker::test_worker(
        format(16_000, 1, SampleFormat::F32),
        16_384,
        FailingVad,
        recognizer,
    );
    let feeder = feed_after_arm(callback, vec![0.05; SILERO_WINDOW_SAMPLES]);
    assert!(matches!(
        worker.capture(Duration::from_secs(2)),
        Err(CaptureError::Vad(VadError::Inference {
            backend: InferenceBackend::Cpu,
            reason,
        })) if reason == "synthetic neural failure"
    ));
    feeder.join().unwrap();
    assert_eq!(finalizations.load(Ordering::Relaxed), 0);
    assert!(worker.shutdown().worker_joined);
}

#[test]
fn shutdown_closes_an_active_capture_and_joins_the_worker() {
    let (recognizer, finalizations) = fake_recognizer(false);
    let (worker, _callback, _) = CaptureWorker::test_worker(
        format(16_000, 1, SampleFormat::F32),
        16_384,
        FakeVad,
        recognizer,
    );
    let (reply, result) = mpsc::sync_channel(1);
    worker
        .commands
        .send(WorkerCommand::Capture {
            timeout: Duration::from_secs(30),
            reply,
        })
        .unwrap();
    worker.commands.send(WorkerCommand::Shutdown).unwrap();
    assert!(matches!(
        result.recv_timeout(Duration::from_secs(1)).unwrap(),
        CaptureMessage::Complete(Err(CaptureError::Closed))
    ));
    assert_eq!(finalizations.load(Ordering::Relaxed), 0);
    let shutdown = worker.shutdown();
    assert!(shutdown.worker_joined);
    assert!(shutdown.input_closed);
}

#[test]
fn silence_and_transient_never_invoke_recognizer() {
    let (recognizer, finalizations) = fake_recognizer(false);
    let (worker, callback, _) = CaptureWorker::test_worker(
        format(16_000, 1, SampleFormat::F32),
        16_384,
        FakeVad,
        recognizer,
    );
    let mut input = vec![0.005; 10 * SILERO_WINDOW_SAMPLES];
    input.extend(vec![0.05; 2 * SILERO_WINDOW_SAMPLES]);
    input.extend(vec![
        0.0;
        usize::from(SILERO_HANGOVER_FRAMES)
            * SILERO_WINDOW_SAMPLES
    ]);
    let feeder = feed_after_arm(callback, input);
    assert!(matches!(
        worker.capture(Duration::from_millis(150)),
        Err(CaptureError::Timeout { .. })
    ));
    feeder.join().unwrap();
    assert_eq!(finalizations.load(Ordering::Relaxed), 0);
    assert_eq!(worker.metrics().rejected_transients, 1);
}

#[test]
fn recognizer_and_device_failures_are_typed() {
    let (recognizer, _) = fake_recognizer(true);
    let (worker, callback, _) = CaptureWorker::test_worker(
        format(16_000, 1, SampleFormat::F32),
        16_384,
        FakeVad,
        recognizer,
    );
    let feeder = feed_after_arm(callback, utterance());
    assert!(matches!(
        worker.capture(Duration::from_secs(2)),
        Err(CaptureError::Recognition(SttError::Inference { .. }))
    ));
    feeder.join().unwrap();

    let (recognizer, _) = fake_recognizer(false);
    let (worker, _callback, stream_failed) = CaptureWorker::test_worker(
        format(16_000, 1, SampleFormat::F32),
        16_384,
        FakeVad,
        recognizer,
    );
    stream_failed.store(true, Ordering::Release);
    assert!(matches!(
        worker.capture(Duration::from_secs(1)),
        Err(CaptureError::Device(DeviceError::InputStreamFailed))
    ));
}

#[test]
fn configuration_and_recognizer_formats_are_rejected_before_io() {
    assert!(matches!(
        CaptureConfig::new(0, 256, InputDeviceSelection::Default),
        Err(DeviceError::InvalidCaptureConfig { .. })
    ));
    assert!(matches!(
        validate_recognizer_format(format(24_000, 1, SampleFormat::F32)),
        Err(CaptureError::Recognition(SttError::FormatMismatch { .. }))
    ));
    assert!(matches!(
        validate_detector_frame(160),
        Err(CaptureError::Vad(VadError::FrameLength { .. }))
    ));
}
