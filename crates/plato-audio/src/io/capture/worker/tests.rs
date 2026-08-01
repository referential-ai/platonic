use super::*;
use std::sync::atomic::AtomicUsize;

use crate::{
    InputDeviceSelection, VAD_HANGOVER_WINDOWS, VAD_MINIMUM_SPEECH_WINDOWS, VAD_WINDOW_SAMPLES,
};

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
        Ok(Vec::new())
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
    let mut samples = vec![0.05; usize::from(VAD_MINIMUM_SPEECH_WINDOWS) * VAD_WINDOW_SAMPLES];
    samples.extend(vec![
        0.0;
        usize::from(VAD_HANGOVER_WINDOWS) * VAD_WINDOW_SAMPLES
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
        CaptureWorker::test_worker(format(16_000, 1, SampleFormat::F32), 4, recognizer);
    let feeder = feed_after_arm(callback, vec![0.05; 64 * VAD_WINDOW_SAMPLES]);
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
        CaptureOverflow::default(),
    );
    assert!(active.is_none());
    assert!(matches!(
        result.recv().unwrap(),
        Err(CaptureError::NonFiniteInput)
    ));
}

#[test]
fn worker_panic_reaches_the_request_and_teardown_joins_the_thread() {
    let (worker, callback, _) = CaptureWorker::test_worker(
        format(16_000, 1, SampleFormat::F32),
        16_384,
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
        Box::new(DropRecognizer {
            dropped: Arc::clone(&dropped),
        }),
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
    let (worker, callback, _) =
        CaptureWorker::test_worker(format(16_000, 1, SampleFormat::F32), 16_384, recognizer);
    let feeder = feed_after_arm(callback, utterance());
    let report = worker.capture(Duration::from_secs(2)).unwrap();
    feeder.join().unwrap();
    assert!(report.transcript.is_final);
    assert_eq!(report.transcript.text, "synthetic question");
    assert_eq!(report.endpoint.start_sample, 0);
    assert_eq!(report.endpoint.speech_end_sample, 3_200);
    assert_eq!(report.endpoint.close_sample, 7_200);
    assert_eq!(finalizations.load(Ordering::Relaxed), 1);
    assert_eq!(worker.metrics().transcripts, 1);
    let shutdown = worker.shutdown();
    assert!(shutdown.worker_joined);
    assert!(shutdown.input_closed);
}

#[test]
fn silence_and_transient_never_invoke_recognizer() {
    let (recognizer, finalizations) = fake_recognizer(false);
    let (worker, callback, _) =
        CaptureWorker::test_worker(format(16_000, 1, SampleFormat::F32), 16_384, recognizer);
    let mut input = vec![0.005; 50 * VAD_WINDOW_SAMPLES];
    input.extend(vec![0.05; 10 * VAD_WINDOW_SAMPLES]);
    input.extend(vec![
        0.0;
        usize::from(VAD_HANGOVER_WINDOWS) * VAD_WINDOW_SAMPLES
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
    let (worker, callback, _) =
        CaptureWorker::test_worker(format(16_000, 1, SampleFormat::F32), 16_384, recognizer);
    let feeder = feed_after_arm(callback, utterance());
    assert!(matches!(
        worker.capture(Duration::from_secs(2)),
        Err(CaptureError::Recognition(SttError::Inference { .. }))
    ));
    feeder.join().unwrap();

    let (recognizer, _) = fake_recognizer(false);
    let (worker, _callback, stream_failed) =
        CaptureWorker::test_worker(format(16_000, 1, SampleFormat::F32), 16_384, recognizer);
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
}
