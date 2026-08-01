use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender, TryRecvError},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use cpal::{
    Stream, StreamConfig,
    traits::{DeviceTrait, StreamTrait},
};
use rtrb::{Consumer, RingBuffer};

#[cfg(test)]
use crate::DeviceBufferSize;
use crate::{
    AudioFormat, CaptureError, CaptureResampleReport, CaptureSample, DeviceError, PcmData,
    PcmFrame, SampleFormat, SpeechRecognizer, SttError, Transcript, VoiceActivityDetector,
    core::{
        capture::CaptureNormalizer,
        vad::{NeuralVadEvent, NeuralVadState},
    },
};

use super::{
    CaptureConfig, CaptureCounters, CaptureDeviceInfo, CaptureMetrics, CaptureOverflow,
    CapturePartial, CaptureReport, CaptureWorkerShutdown, bounded,
    device::{
        CallbackWriter, build_stream, sample_format, select_buffer_size, select_device,
        select_input_config,
    },
};

const MAX_DRAIN_SAMPLES: usize = 8_192;
const WORKER_POLL_INTERVAL: Duration = Duration::from_millis(1);
const CAPTURE_EVENT_CAPACITY: usize = 8;

/// One persistent input stream and exactly one normalization/VAD/STT worker.
pub struct CaptureWorker {
    stream: Option<Stream>,
    device_info: CaptureDeviceInfo,
    ring_capacity_samples: usize,
    commands: SyncSender<WorkerCommand>,
    worker: Option<JoinHandle<()>>,
    stream_failed: Arc<AtomicBool>,
    counters: Arc<CaptureCounters>,
    worker_status: Arc<WorkerStatus>,
    closed: bool,
}

#[derive(Default)]
struct WorkerStatus {
    active_reply: Mutex<Option<SyncSender<CaptureMessage>>>,
    panicked: AtomicBool,
    exited: AtomicBool,
}

impl WorkerStatus {
    fn arm(&self, reply: SyncSender<CaptureMessage>) {
        *self
            .active_reply
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(reply);
    }

    fn clear(&self) {
        self.active_reply
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
    }

    fn mark_panicked(&self) {
        self.panicked.store(true, Ordering::Release);
        if let Some(reply) = self
            .active_reply
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            let _ = reply.try_send(CaptureMessage::Complete(Err(CaptureError::WorkerPanicked)));
        }
    }
}

impl CaptureWorker {
    /// Opens the selected device once and moves the resident recognizer into one worker.
    pub fn open<V, R>(
        config: CaptureConfig,
        detector: V,
        recognizer: R,
    ) -> Result<Self, CaptureError>
    where
        V: VoiceActivityDetector + 'static,
        R: SpeechRecognizer + 'static,
    {
        validate_recognizer_format(recognizer.input_format())?;
        validate_detector_frame(detector.frame_samples())?;
        let host = cpal::default_host();
        let backend = host.id().name().to_owned();
        let device = select_device(&host, &config.device)?;
        let device_id = device
            .id()
            .map_err(|error| DeviceError::InputDeviceQuery {
                reason: bounded(&error.to_string()),
            })?
            .to_string();
        let device_name = device.to_string();
        let selected = select_input_config(&device)?;
        let channels = selected.channels();
        if config.capacity_samples < usize::from(channels) {
            return Err(DeviceError::CaptureRingTooSmall {
                capacity_samples: config.capacity_samples,
                channels,
            }
            .into());
        }
        let (buffer_size, requested_buffer) =
            select_buffer_size(selected.buffer_size(), config.preferred_buffer_frames);
        let format = AudioFormat::new(
            selected.sample_rate(),
            channels,
            sample_format(selected.sample_format()),
        )
        .map_err(DeviceError::from)?;
        let worker_format = recognizer.input_format();
        let mut stream_config: StreamConfig = selected.into();
        stream_config.buffer_size = requested_buffer;
        let (producer, consumer) = RingBuffer::new(config.capacity_samples);
        let counters = Arc::new(CaptureCounters::default());
        let stream_failed = Arc::new(AtomicBool::new(false));
        let callback = CallbackWriter::new(producer, channels, Arc::clone(&counters));
        let stream = build_stream(
            &device,
            stream_config,
            format.sample_format(),
            callback,
            Arc::clone(&stream_failed),
        )?;
        stream
            .play()
            .map_err(|error| DeviceError::InputStreamStart {
                reason: bounded(&error.to_string()),
            })?;
        let (commands, requests) = mpsc::sync_channel(1);
        let (worker, worker_status) = spawn_worker(
            consumer,
            format,
            Box::new(detector),
            Box::new(recognizer),
            requests,
            Arc::clone(&stream_failed),
            Arc::clone(&counters),
        )?;
        Ok(Self {
            stream: Some(stream),
            device_info: CaptureDeviceInfo {
                backend,
                device_id,
                device: device_name,
                format,
                worker_format,
                buffer_size,
            },
            ring_capacity_samples: config.capacity_samples,
            commands,
            worker: Some(worker),
            stream_failed,
            counters,
            worker_status,
            closed: false,
        })
    }

    /// Returns exact live input stream identity.
    pub fn device_info(&self) -> &CaptureDeviceInfo {
        &self.device_info
    }

    /// Arms capture for exactly one VAD-closed final transcript.
    pub fn capture(&self, timeout: Duration) -> Result<CaptureReport, CaptureError> {
        self.capture_with_partials(timeout, |_| {})
    }

    /// Delivers typed non-final updates while waiting for one final transcript.
    pub fn capture_with_partials(
        &self,
        timeout: Duration,
        mut on_partial: impl FnMut(&CapturePartial),
    ) -> Result<CaptureReport, CaptureError> {
        if self.closed {
            return Err(CaptureError::Closed);
        }
        if self.stream_failed.load(Ordering::Acquire) {
            return Err(DeviceError::InputStreamFailed.into());
        }
        if self.worker_status.panicked.load(Ordering::Acquire) {
            return Err(CaptureError::WorkerPanicked);
        }
        let (reply, result) = mpsc::sync_channel(CAPTURE_EVENT_CAPACITY);
        self.commands
            .send(WorkerCommand::Capture { timeout, reply })
            .map_err(|_| {
                if self.worker_status.panicked.load(Ordering::Acquire) {
                    CaptureError::WorkerPanicked
                } else {
                    CaptureError::WorkerStopped
                }
            })?;
        let wait_deadline = Instant::now() + timeout.saturating_add(Duration::from_secs(1));
        loop {
            let remaining = wait_deadline.saturating_duration_since(Instant::now());
            let message = result
                .recv_timeout(remaining)
                .map_err(|error| match error {
                    mpsc::RecvTimeoutError::Timeout => CaptureError::Timeout {
                        milliseconds: timeout.as_millis(),
                    },
                    mpsc::RecvTimeoutError::Disconnected => CaptureError::WorkerStopped,
                })?;
            match message {
                CaptureMessage::Partial(partial) => on_partial(&partial),
                CaptureMessage::Complete(result) => return result,
            }
        }
    }

    /// Reads monotonic capture, endpointing, and callback-overflow counters.
    pub fn metrics(&self) -> CaptureMetrics {
        CaptureMetrics {
            stream_opens: 1,
            worker_threads: 1,
            ring_capacity_samples: self.ring_capacity_samples,
            input_frames: self.counters.input_frames.load(Ordering::Relaxed),
            output_frames: self.counters.output_frames.load(Ordering::Relaxed),
            rejected_transients: self.counters.rejected_transients.load(Ordering::Relaxed),
            transcripts: self.counters.transcripts.load(Ordering::Relaxed),
            partial_updates: self.counters.partial_updates.load(Ordering::Relaxed),
            normalization_resampling_us: self
                .counters
                .normalization_resampling_us
                .load(Ordering::Relaxed),
            overflow: self.counters.overflow(),
        }
    }

    /// Stops the worker, drops the persistent input stream, and joins ownership.
    pub fn shutdown(mut self) -> CaptureWorkerShutdown {
        self.close()
    }

    fn close(&mut self) -> CaptureWorkerShutdown {
        if self.closed {
            return CaptureWorkerShutdown {
                worker_joined: self.worker.is_none(),
                input_closed: self.stream.is_none(),
                stream_opens: 1,
                worker_threads: 1,
                worker_panicked: self.worker_status.panicked.load(Ordering::Acquire),
            };
        }
        self.closed = true;
        drop(self.stream.take());
        let _ = self.commands.send(WorkerCommand::Shutdown);
        let worker_joined = self
            .worker
            .take()
            .is_none_or(|worker| worker.join().is_ok())
            && self.worker_status.exited.load(Ordering::Acquire);
        CaptureWorkerShutdown {
            worker_joined,
            input_closed: self.stream.is_none(),
            stream_opens: 1,
            worker_threads: 1,
            worker_panicked: self.worker_status.panicked.load(Ordering::Acquire),
        }
    }

    #[cfg(test)]
    fn test_worker<V, R>(
        format: AudioFormat,
        capacity_samples: usize,
        detector: V,
        recognizer: R,
    ) -> (Self, CallbackWriter, Arc<AtomicBool>)
    where
        V: VoiceActivityDetector + 'static,
        R: SpeechRecognizer + 'static,
    {
        validate_recognizer_format(recognizer.input_format()).unwrap();
        validate_detector_frame(detector.frame_samples()).unwrap();
        let (producer, consumer) = RingBuffer::new(capacity_samples);
        let counters = Arc::new(CaptureCounters::default());
        let stream_failed = Arc::new(AtomicBool::new(false));
        let callback = CallbackWriter::new(producer, format.channels(), Arc::clone(&counters));
        let (commands, requests) = mpsc::sync_channel(1);
        let (worker, worker_status) = spawn_worker(
            consumer,
            format,
            Box::new(detector),
            Box::new(recognizer),
            requests,
            Arc::clone(&stream_failed),
            Arc::clone(&counters),
        )
        .unwrap();
        (
            Self {
                stream: None,
                device_info: CaptureDeviceInfo {
                    backend: "test".to_owned(),
                    device_id: "test:input".to_owned(),
                    device: "Synthetic input".to_owned(),
                    format,
                    worker_format: whisper_format(),
                    buffer_size: DeviceBufferSize::DefaultUnknown,
                },
                ring_capacity_samples: capacity_samples,
                commands,
                worker: Some(worker),
                stream_failed: Arc::clone(&stream_failed),
                counters,
                worker_status,
                closed: false,
            },
            callback,
            stream_failed,
        )
    }
}

impl Drop for CaptureWorker {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

enum WorkerCommand {
    Capture {
        timeout: Duration,
        reply: SyncSender<CaptureMessage>,
    },
    Shutdown,
}

enum CaptureMessage {
    Partial(CapturePartial),
    Complete(Result<CaptureReport, CaptureError>),
}

struct CaptureEngines {
    detector: Box<dyn VoiceActivityDetector>,
    recognizer: Box<dyn SpeechRecognizer>,
}

struct ActiveCapture {
    sequence: u64,
    deadline: Instant,
    timeout: Duration,
    reply: SyncSender<CaptureMessage>,
    normalizer: CaptureNormalizer,
    vad: NeuralVadState,
    partials: Vec<CapturePartial>,
    normalization_resampling_us: u64,
    input_frames: u64,
    output_frames: u64,
    overflow_at_arm: CaptureOverflow,
}

impl ActiveCapture {
    fn new(
        sequence: u64,
        timeout: Duration,
        reply: SyncSender<CaptureMessage>,
        normalizer: CaptureNormalizer,
        vad: NeuralVadState,
        overflow_at_arm: CaptureOverflow,
    ) -> Self {
        Self {
            sequence,
            deadline: Instant::now() + timeout,
            timeout,
            reply,
            normalizer,
            vad,
            partials: Vec::new(),
            normalization_resampling_us: 0,
            input_frames: 0,
            output_frames: 0,
            overflow_at_arm,
        }
    }
}

fn spawn_worker(
    consumer: Consumer<CaptureSample>,
    format: AudioFormat,
    detector: Box<dyn VoiceActivityDetector>,
    recognizer: Box<dyn SpeechRecognizer>,
    commands: Receiver<WorkerCommand>,
    stream_failed: Arc<AtomicBool>,
    counters: Arc<CaptureCounters>,
) -> Result<(JoinHandle<()>, Arc<WorkerStatus>), CaptureError> {
    spawn_worker_with(
        consumer,
        format,
        CaptureEngines {
            detector,
            recognizer,
        },
        commands,
        stream_failed,
        counters,
        |task| {
            thread::Builder::new()
                .name("plato-audio-capture".to_owned())
                .spawn(task)
        },
    )
}

fn spawn_worker_with<F>(
    consumer: Consumer<CaptureSample>,
    format: AudioFormat,
    engines: CaptureEngines,
    commands: Receiver<WorkerCommand>,
    stream_failed: Arc<AtomicBool>,
    counters: Arc<CaptureCounters>,
    spawn: F,
) -> Result<(JoinHandle<()>, Arc<WorkerStatus>), CaptureError>
where
    F: FnOnce(Box<dyn FnOnce() + Send>) -> std::io::Result<JoinHandle<()>>,
{
    let worker_status = Arc::new(WorkerStatus::default());
    let thread_status = Arc::clone(&worker_status);
    let task = Box::new(move || {
        let result = catch_unwind(AssertUnwindSafe(|| {
            run_worker(
                consumer,
                format,
                engines,
                commands,
                stream_failed,
                counters,
                thread_status.as_ref(),
            );
        }));
        if result.is_err() {
            thread_status.mark_panicked();
        }
        thread_status.exited.store(true, Ordering::Release);
    });
    let worker = spawn(task).map_err(thread_start_error)?;
    Ok((worker, worker_status))
}

fn thread_start_error(error: std::io::Error) -> CaptureError {
    CaptureError::WorkerThreadStart {
        reason: bounded(&error.to_string()),
    }
}

fn run_worker(
    mut consumer: Consumer<CaptureSample>,
    format: AudioFormat,
    engines: CaptureEngines,
    commands: Receiver<WorkerCommand>,
    stream_failed: Arc<AtomicBool>,
    counters: Arc<CaptureCounters>,
    worker_status: &WorkerStatus,
) {
    let CaptureEngines {
        mut detector,
        mut recognizer,
    } = engines;
    let mut active: Option<ActiveCapture> = None;
    let mut sequence = 0_u64;
    let mut drained = Vec::with_capacity(MAX_DRAIN_SAMPLES);
    loop {
        match commands.try_recv() {
            Ok(WorkerCommand::Capture { timeout, reply }) if active.is_none() => {
                let overflow_at_arm = counters.overflow();
                drain_discard(&mut consumer);
                detector.reset();
                recognizer.reset();
                sequence = sequence.saturating_add(1);
                worker_status.arm(reply.clone());
                active = start_capture(
                    sequence,
                    timeout,
                    reply,
                    CaptureNormalizer::new(format),
                    NeuralVadState::new(detector.frame_samples()),
                    overflow_at_arm,
                );
                if active.is_none() {
                    worker_status.clear();
                }
            }
            Ok(WorkerCommand::Capture { reply, .. }) => {
                let _ = reply.send(CaptureMessage::Complete(Err(CaptureError::WorkerStopped)));
            }
            Ok(WorkerCommand::Shutdown) => {
                if let Some(capture) = active.take() {
                    complete_capture(worker_status, capture, Err(CaptureError::Closed));
                }
                return;
            }
            Err(TryRecvError::Disconnected) => {
                if let Some(capture) = active.take() {
                    complete_capture(worker_status, capture, Err(CaptureError::WorkerStopped));
                }
                return;
            }
            Err(TryRecvError::Empty) => {}
        }

        if stream_failed.load(Ordering::Acquire) {
            if let Some(capture) = active.take() {
                complete_capture(
                    worker_status,
                    capture,
                    Err(DeviceError::InputStreamFailed.into()),
                );
            }
            return;
        }

        if let Some(capture) = active.as_ref()
            && Instant::now() >= capture.deadline
        {
            let capture = active.take().expect("active capture exists");
            let timeout = capture.timeout;
            complete_capture(
                worker_status,
                capture,
                Err(CaptureError::Timeout {
                    milliseconds: timeout.as_millis(),
                }),
            );
            continue;
        }

        if let Some(capture) = active.as_ref()
            && let Some(error) = overflow_error(capture.overflow_at_arm, counters.overflow())
        {
            let capture = active.take().expect("active capture exists");
            complete_capture(worker_status, capture, Err(error));
            drain_discard(&mut consumer);
            continue;
        }

        drained.clear();
        while drained.len() < MAX_DRAIN_SAMPLES {
            match consumer.pop() {
                Ok(sample) => drained.push(sample),
                Err(_) => break,
            }
        }
        if drained.is_empty() {
            thread::sleep(WORKER_POLL_INTERVAL);
            continue;
        }
        let Some(capture) = active.as_mut() else {
            continue;
        };
        let started = Instant::now();
        let normalized = capture.normalizer.push(&drained);
        let elapsed_us = duration_us(started.elapsed());
        capture.normalization_resampling_us = capture
            .normalization_resampling_us
            .saturating_add(elapsed_us);
        counters
            .normalization_resampling_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
        let (samples, report) = match normalized {
            Ok(result) => result,
            Err(error) => {
                let capture = active.take().expect("active capture exists");
                complete_capture(worker_status, capture, Err(error));
                continue;
            }
        };
        update_conversion_counters(capture, &counters, report);
        let audio_available = Instant::now();
        let events = match capture.vad.push(&samples, detector.as_mut()) {
            Ok(events) => events,
            Err(error) => {
                let capture = active.take().expect("active capture exists");
                complete_capture(worker_status, capture, Err(error.into()));
                continue;
            }
        };
        for event in events {
            match event {
                NeuralVadEvent::RejectedTransient(endpoint) => {
                    debug_assert!(endpoint.close_sample >= endpoint.speech_end_sample);
                    counters.rejected_transients.fetch_add(1, Ordering::Relaxed);
                }
                NeuralVadEvent::SpeechSamples(samples) => {
                    let Some(capture) = active.as_mut() else {
                        break;
                    };
                    let updates = match recognize_samples(recognizer.as_mut(), &samples) {
                        Ok(updates) => updates,
                        Err(error) => {
                            let capture = active.take().expect("active capture exists");
                            complete_capture(worker_status, capture, Err(error));
                            return;
                        }
                    };
                    for transcript in updates {
                        let partial =
                            CapturePartial::new(transcript, duration_us(audio_available.elapsed()));
                        capture.partials.push(partial.clone());
                        counters.partial_updates.fetch_add(1, Ordering::Relaxed);
                        if capture
                            .reply
                            .send(CaptureMessage::Partial(partial))
                            .is_err()
                        {
                            worker_status.clear();
                            return;
                        }
                    }
                }
                NeuralVadEvent::Segment(segment) => {
                    let close = Instant::now();
                    let overflow_at_close = counters.overflow();
                    let capture = active.take().expect("active capture exists");
                    let recognition = overflow_error(capture.overflow_at_arm, overflow_at_close)
                        .map_or_else(|| finalize_segment(recognizer.as_mut(), &segment), Err);
                    let result = recognition.map(|transcript| {
                        counters.transcripts.fetch_add(1, Ordering::Relaxed);
                        CaptureReport {
                            sequence: capture.sequence,
                            transcript,
                            partials: capture.partials.clone(),
                            endpoint: segment.endpoint(),
                            vad_close_to_final_us: duration_us(close.elapsed()),
                            normalization_resampling_us: capture.normalization_resampling_us,
                            input_frames: capture.input_frames,
                            output_frames: capture.output_frames,
                            overflow: overflow_at_close,
                        }
                    });
                    let recognition_failed = result.is_err();
                    complete_capture(worker_status, capture, result);
                    if recognition_failed {
                        return;
                    }
                    break;
                }
            }
        }
    }
}

fn complete_capture(
    worker_status: &WorkerStatus,
    capture: ActiveCapture,
    result: Result<CaptureReport, CaptureError>,
) {
    worker_status.clear();
    let _ = capture.reply.send(CaptureMessage::Complete(result));
}

fn start_capture(
    sequence: u64,
    timeout: Duration,
    reply: SyncSender<CaptureMessage>,
    normalizer: Result<CaptureNormalizer, CaptureError>,
    vad: Result<NeuralVadState, crate::VadError>,
    overflow_at_arm: CaptureOverflow,
) -> Option<ActiveCapture> {
    match (normalizer, vad) {
        (Ok(normalizer), Ok(vad)) => Some(ActiveCapture::new(
            sequence,
            timeout,
            reply,
            normalizer,
            vad,
            overflow_at_arm,
        )),
        (Err(error), _) => {
            let _ = reply.send(CaptureMessage::Complete(Err(error)));
            None
        }
        (_, Err(error)) => {
            let _ = reply.send(CaptureMessage::Complete(Err(error.into())));
            None
        }
    }
}

fn overflow_error(start: CaptureOverflow, current: CaptureOverflow) -> Option<CaptureError> {
    let callbacks = current.callbacks.saturating_sub(start.callbacks);
    let dropped_samples = current.samples.saturating_sub(start.samples);
    (callbacks > 0 || dropped_samples > 0).then_some(CaptureError::RingOverflow {
        callbacks,
        dropped_samples,
    })
}

fn update_conversion_counters(
    capture: &mut ActiveCapture,
    counters: &CaptureCounters,
    report: CaptureResampleReport,
) {
    let input = u64::try_from(report.input_frames).unwrap_or(u64::MAX);
    let output = u64::try_from(report.output_frames).unwrap_or(u64::MAX);
    capture.input_frames = capture.input_frames.saturating_add(input);
    capture.output_frames = capture.output_frames.saturating_add(output);
    counters.input_frames.fetch_add(input, Ordering::Relaxed);
    counters.output_frames.fetch_add(output, Ordering::Relaxed);
}

#[cfg(all(test, feature = "whisper-cuda"))]
pub(crate) fn recognize_segment(
    recognizer: &mut dyn SpeechRecognizer,
    segment: &crate::VoiceSegment,
) -> Result<Transcript, CaptureError> {
    recognizer.reset();
    let _ = recognize_samples(recognizer, segment.samples())?;
    finalize_segment(recognizer, segment)
}

pub(crate) fn recognize_samples(
    recognizer: &mut dyn SpeechRecognizer,
    samples: &[f32],
) -> Result<Vec<Transcript>, CaptureError> {
    let format = recognizer.input_format();
    let mut partials = Vec::new();
    for &sample in samples {
        let frame =
            PcmFrame::new(format, PcmData::F32(Box::new([sample]))).map_err(SttError::from)?;
        for rolling in recognizer.accept(&frame)? {
            if rolling.is_final {
                return Err(SttError::Contract {
                    reason: "accept returned a final transcript before endpoint close".to_owned(),
                }
                .into());
            }
            partials.push(rolling);
        }
    }
    Ok(partials)
}

pub(crate) fn finalize_segment(
    recognizer: &mut dyn SpeechRecognizer,
    segment: &crate::VoiceSegment,
) -> Result<Transcript, CaptureError> {
    let transcript = recognizer.finalize()?;
    if !transcript.is_final {
        return Err(SttError::Contract {
            reason: "finalize returned a non-final transcript".to_owned(),
        }
        .into());
    }
    if transcript.span_ms != segment.span_ms() {
        return Err(SttError::Contract {
            reason: format!(
                "final transcript span {} ms does not match VAD segment {} ms",
                transcript.span_ms,
                segment.span_ms()
            ),
        }
        .into());
    }
    Ok(transcript)
}

fn validate_recognizer_format(actual: AudioFormat) -> Result<(), CaptureError> {
    let expected = whisper_format();
    if actual != expected {
        return Err(SttError::FormatMismatch { expected, actual }.into());
    }
    Ok(())
}

fn validate_detector_frame(actual: usize) -> Result<(), CaptureError> {
    NeuralVadState::new(actual).map(|_| ()).map_err(Into::into)
}

fn whisper_format() -> AudioFormat {
    AudioFormat::new(16_000, 1, SampleFormat::F32).expect("literal worker format is valid")
}

fn drain_discard(consumer: &mut Consumer<CaptureSample>) {
    while consumer.pop().is_ok() {}
}

fn duration_us(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests;
