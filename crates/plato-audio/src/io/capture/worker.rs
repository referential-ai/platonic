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
    AudioFormat, BargeInHandle, CaptureError, CaptureResampleReport, DeviceError, MAX_UTTERANCE_MS,
    PcmData, PcmFrame, SampleFormat, SpeechRecognizer, SttError, Transcript, VoiceActivityDetector,
    core::{
        capture::CaptureNormalizer,
        vad::{NeuralVadEvent, NeuralVadState},
    },
};
#[cfg(test)]
use std::sync::atomic::AtomicU64;

use super::{
    CaptureConfig, CaptureCounters, CaptureDeviceInfo, CaptureMetrics, CaptureOverflow,
    CapturePartial, CaptureReport, CaptureWorkerShutdown, TimedCaptureSample, bounded,
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
    barge_results: Receiver<CaptureMessage>,
    closed: bool,
}

/// One armed capture whose bounded updates are drained without blocking the owner.
pub struct CaptureRequest {
    results: Receiver<CaptureMessage>,
}

impl CaptureRequest {
    /// Returns the final report when ready while consuming ephemeral partial hypotheses.
    pub fn try_complete(&self) -> Result<Option<CaptureReport>, CaptureError> {
        loop {
            match self.results.try_recv() {
                Ok(CaptureMessage::Partial(_)) => {}
                Ok(CaptureMessage::Complete(result)) => return result.map(Some),
                Err(TryRecvError::Empty) => return Ok(None),
                Err(TryRecvError::Disconnected) => return Err(CaptureError::WorkerStopped),
            }
        }
    }
}

#[derive(Default)]
struct WorkerStatus {
    active_reply: Mutex<Option<SyncSender<CaptureMessage>>>,
    barge_failure: Mutex<Option<String>>,
    #[cfg(test)]
    barge_in_generation: AtomicU64,
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

    fn mark_barge_failure(&self, reason: String) {
        self.barge_failure
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get_or_insert(reason);
    }

    fn barge_failure(&self) -> Option<String> {
        self.barge_failure
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
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
        Self::open_inner(config, detector, recognizer, None)
    }

    /// Opens continuous playback-time VAD bound to one existing cancel authority.
    pub fn open_with_barge_in<V, R>(
        config: CaptureConfig,
        detector: V,
        recognizer: R,
        barge_in: BargeInHandle,
    ) -> Result<Self, CaptureError>
    where
        V: VoiceActivityDetector + 'static,
        R: SpeechRecognizer + 'static,
    {
        Self::open_inner(config, detector, recognizer, Some(barge_in))
    }

    fn open_inner<V, R>(
        config: CaptureConfig,
        detector: V,
        recognizer: R,
        barge_in: Option<BargeInHandle>,
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
        let (barge_reply, barge_results) = mpsc::sync_channel(CAPTURE_EVENT_CAPACITY);
        let (worker, worker_status) = spawn_worker(
            CaptureWorkerRuntime {
                consumer,
                format,
                commands: requests,
                stream_failed: Arc::clone(&stream_failed),
                counters: Arc::clone(&counters),
                barge_in,
                barge_reply,
            },
            Box::new(detector),
            Box::new(recognizer),
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
            barge_results,
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
        self.check_health()?;
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

    /// Arms one capture and returns immediately so another owner can poll its result.
    pub fn arm_capture(&self, timeout: Duration) -> Result<CaptureRequest, CaptureError> {
        if self.closed {
            return Err(CaptureError::Closed);
        }
        self.check_health()?;
        let (reply, results) = mpsc::sync_channel(CAPTURE_EVENT_CAPACITY);
        self.commands
            .send(WorkerCommand::Capture { timeout, reply })
            .map_err(|_| {
                if self.worker_status.panicked.load(Ordering::Acquire) {
                    CaptureError::WorkerPanicked
                } else {
                    CaptureError::WorkerStopped
                }
            })?;
        Ok(CaptureRequest { results })
    }

    /// Disarms the current explicit capture without closing the persistent input stream.
    pub fn cancel_capture(&self) -> Result<(), CaptureError> {
        if self.closed {
            return Err(CaptureError::Closed);
        }
        self.commands
            .send(WorkerCommand::CancelCapture)
            .map_err(|_| CaptureError::WorkerStopped)
    }

    /// Polls the final transcript retained from playback-time barge-in onset.
    pub fn poll_barge_in_capture(&self) -> Result<Option<CaptureReport>, CaptureError> {
        loop {
            match self.barge_results.try_recv() {
                Ok(CaptureMessage::Partial(_)) => {}
                Ok(CaptureMessage::Complete(Err(CaptureError::Canceled))) => return Ok(None),
                Ok(CaptureMessage::Complete(result)) => return result.map(Some),
                Err(TryRecvError::Empty) => return Ok(None),
                Err(TryRecvError::Disconnected) => return Err(CaptureError::WorkerStopped),
            }
        }
    }

    #[cfg(test)]
    fn barge_in_monitor_ready(&self, generation: u64) -> bool {
        self.worker_status
            .barge_in_generation
            .load(Ordering::Acquire)
            == generation
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

    /// Returns a typed persistent-stream or continuous-VAD failure.
    pub fn check_health(&self) -> Result<(), CaptureError> {
        if self.stream_failed.load(Ordering::Acquire) {
            return Err(DeviceError::InputStreamFailed.into());
        }
        if self.worker_status.panicked.load(Ordering::Acquire) {
            return Err(CaptureError::WorkerPanicked);
        }
        if let Some(reason) = self.worker_status.barge_failure() {
            return Err(CaptureError::BargeIn { reason });
        }
        if self.worker_status.exited.load(Ordering::Acquire) && !self.closed {
            return Err(CaptureError::WorkerStopped);
        }
        Ok(())
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
        Self::test_worker_inner(format, capacity_samples, detector, recognizer, None)
    }

    #[cfg(test)]
    fn test_worker_with_barge_in<V, R>(
        format: AudioFormat,
        capacity_samples: usize,
        detector: V,
        recognizer: R,
        barge_in: BargeInHandle,
    ) -> (Self, CallbackWriter, Arc<AtomicBool>)
    where
        V: VoiceActivityDetector + 'static,
        R: SpeechRecognizer + 'static,
    {
        Self::test_worker_inner(
            format,
            capacity_samples,
            detector,
            recognizer,
            Some(barge_in),
        )
    }

    #[cfg(test)]
    fn test_worker_inner<V, R>(
        format: AudioFormat,
        capacity_samples: usize,
        detector: V,
        recognizer: R,
        barge_in: Option<BargeInHandle>,
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
        let (barge_reply, barge_results) = mpsc::sync_channel(CAPTURE_EVENT_CAPACITY);
        let (worker, worker_status) = spawn_worker(
            CaptureWorkerRuntime {
                consumer,
                format,
                commands: requests,
                stream_failed: Arc::clone(&stream_failed),
                counters: Arc::clone(&counters),
                barge_in,
                barge_reply,
            },
            Box::new(detector),
            Box::new(recognizer),
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
                barge_results,
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
    CancelCapture,
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

struct CaptureWorkerRuntime {
    consumer: Consumer<TimedCaptureSample>,
    format: AudioFormat,
    commands: Receiver<WorkerCommand>,
    stream_failed: Arc<AtomicBool>,
    counters: Arc<CaptureCounters>,
    barge_in: Option<BargeInHandle>,
    barge_reply: SyncSender<CaptureMessage>,
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

struct ActiveBargeIn {
    generation: u64,
    normalizer: CaptureNormalizer,
    vad: NeuralVadState,
    overflow_at_gate: CaptureOverflow,
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
    runtime: CaptureWorkerRuntime,
    detector: Box<dyn VoiceActivityDetector>,
    recognizer: Box<dyn SpeechRecognizer>,
) -> Result<(JoinHandle<()>, Arc<WorkerStatus>), CaptureError> {
    spawn_worker_with(
        runtime,
        CaptureEngines {
            detector,
            recognizer,
        },
        |task| {
            thread::Builder::new()
                .name("plato-audio-capture".to_owned())
                .spawn(task)
        },
    )
}

fn spawn_worker_with<F>(
    runtime: CaptureWorkerRuntime,
    engines: CaptureEngines,
    spawn: F,
) -> Result<(JoinHandle<()>, Arc<WorkerStatus>), CaptureError>
where
    F: FnOnce(Box<dyn FnOnce() + Send>) -> std::io::Result<JoinHandle<()>>,
{
    let worker_status = Arc::new(WorkerStatus::default());
    let thread_status = Arc::clone(&worker_status);
    let task = Box::new(move || {
        let result = catch_unwind(AssertUnwindSafe(|| {
            run_worker(runtime, engines, thread_status.as_ref());
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
    runtime: CaptureWorkerRuntime,
    engines: CaptureEngines,
    worker_status: &WorkerStatus,
) {
    let CaptureWorkerRuntime {
        mut consumer,
        format,
        commands,
        stream_failed,
        counters,
        barge_in,
        barge_reply,
    } = runtime;
    let CaptureEngines {
        mut detector,
        mut recognizer,
    } = engines;
    let mut active: Option<ActiveCapture> = None;
    let mut active_barge_in: Option<ActiveBargeIn> = None;
    let mut sequence = 0_u64;
    let mut drained = Vec::with_capacity(MAX_DRAIN_SAMPLES);
    let mut native_samples = Vec::with_capacity(MAX_DRAIN_SAMPLES);
    loop {
        match commands.try_recv() {
            Ok(WorkerCommand::Capture { timeout, reply }) if active.is_none() => {
                active_barge_in = None;
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
            Ok(WorkerCommand::CancelCapture) => {
                if let Some(capture) = active.take() {
                    complete_capture(worker_status, capture, Err(CaptureError::Canceled));
                }
                active_barge_in = None;
                drain_discard(&mut consumer);
                continue;
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
        let audio_available = drained
            .first()
            .expect("nonempty drain has one callback timestamp")
            .available_at;
        let Some(capture) = active.as_mut() else {
            let Some(handle) = barge_in.as_ref() else {
                continue;
            };
            if !handle.is_active() || handle.cancel_requested() || !handle.playback_active() {
                active_barge_in = None;
                continue;
            }
            if !handle.gate_open() {
                active_barge_in = None;
                continue;
            }

            let generation = handle.generation();
            if active_barge_in
                .as_ref()
                .is_none_or(|monitor| monitor.generation != generation)
            {
                detector.reset();
                let normalizer = match CaptureNormalizer::new(format) {
                    Ok(normalizer) => normalizer,
                    Err(error) => {
                        fail_barge_in(worker_status, handle, error);
                        return;
                    }
                };
                let vad = match NeuralVadState::new(detector.frame_samples()) {
                    Ok(vad) => vad,
                    Err(error) => {
                        fail_barge_in(worker_status, handle, error);
                        return;
                    }
                };
                active_barge_in = Some(ActiveBargeIn {
                    generation,
                    normalizer,
                    vad,
                    overflow_at_gate: counters.overflow(),
                });
                #[cfg(test)]
                worker_status
                    .barge_in_generation
                    .store(generation, Ordering::Release);
                // The gate can open during a native callback. Start on the next
                // drained batch so pre-gate self-playback never enters Silero.
                continue;
            }

            let monitor = active_barge_in
                .as_mut()
                .expect("barge-in monitor initialized for this generation");
            if let Some(error) = overflow_error(monitor.overflow_at_gate, counters.overflow()) {
                fail_barge_in(worker_status, handle, error);
                return;
            }
            native_samples.clear();
            native_samples.extend(drained.iter().map(|sample| sample.sample));
            let started = Instant::now();
            let (samples, report) = match monitor.normalizer.push(&native_samples) {
                Ok(result) => result,
                Err(error) => {
                    fail_barge_in(worker_status, handle, error);
                    return;
                }
            };
            let elapsed_us = duration_us(started.elapsed());
            counters
                .normalization_resampling_us
                .fetch_add(elapsed_us, Ordering::Relaxed);
            update_global_conversion_counters(&counters, report);
            let vad_evaluation_started_at = Instant::now();
            let events = match monitor.vad.push(&samples, detector.as_mut()) {
                Ok(events) => events,
                Err(error) => {
                    fail_barge_in(worker_status, handle, error);
                    return;
                }
            };
            if events
                .iter()
                .any(|event| matches!(event, NeuralVadEvent::SpeechOnset { .. }))
                && handle.trigger_speech_onset()
            {
                let monitor = active_barge_in
                    .take()
                    .expect("barge-in monitor produced the onset");
                recognizer.reset();
                sequence = sequence.saturating_add(1);
                let timeout = Duration::from_millis(MAX_UTTERANCE_MS);
                let mut capture = ActiveCapture::new(
                    sequence,
                    timeout,
                    barge_reply.clone(),
                    monitor.normalizer,
                    monitor.vad,
                    monitor.overflow_at_gate,
                );
                capture.normalization_resampling_us = elapsed_us;
                capture.input_frames = u64::try_from(report.input_frames).unwrap_or(u64::MAX);
                capture.output_frames = u64::try_from(report.output_frames).unwrap_or(u64::MAX);
                worker_status.arm(barge_reply.clone());
                active = Some(capture);
                if !handle_capture_events(
                    &mut active,
                    events,
                    audio_available,
                    vad_evaluation_started_at,
                    recognizer.as_mut(),
                    &counters,
                    worker_status,
                ) {
                    return;
                }
            }
            continue;
        };
        native_samples.clear();
        native_samples.extend(drained.iter().map(|sample| sample.sample));
        let started = Instant::now();
        let normalized = capture.normalizer.push(&native_samples);
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
        let vad_evaluation_started_at = Instant::now();
        let events = match capture.vad.push(&samples, detector.as_mut()) {
            Ok(events) => events,
            Err(error) => {
                let capture = active.take().expect("active capture exists");
                complete_capture(worker_status, capture, Err(error.into()));
                continue;
            }
        };
        if !handle_capture_events(
            &mut active,
            events,
            audio_available,
            vad_evaluation_started_at,
            recognizer.as_mut(),
            &counters,
            worker_status,
        ) {
            return;
        }
    }
}

fn handle_capture_events(
    active: &mut Option<ActiveCapture>,
    events: Vec<NeuralVadEvent>,
    audio_available: Instant,
    vad_evaluation_started_at: Instant,
    recognizer: &mut dyn SpeechRecognizer,
    counters: &CaptureCounters,
    worker_status: &WorkerStatus,
) -> bool {
    for event in events {
        match event {
            NeuralVadEvent::SpeechOnset { .. } => {}
            NeuralVadEvent::RejectedTransient(endpoint) => {
                debug_assert!(endpoint.close_sample >= endpoint.speech_end_sample);
                counters.rejected_transients.fetch_add(1, Ordering::Relaxed);
            }
            NeuralVadEvent::SpeechSamples(samples) => {
                let Some(capture) = active.as_mut() else {
                    break;
                };
                let updates = match recognize_samples(recognizer, &samples) {
                    Ok(updates) => updates,
                    Err(error) => {
                        let capture = active.take().expect("active capture exists");
                        complete_capture(worker_status, capture, Err(error));
                        return false;
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
                        active.take();
                        return true;
                    }
                }
            }
            NeuralVadEvent::Segment(segment) => {
                let overflow_at_close = counters.overflow();
                let capture = active.take().expect("active capture exists");
                let recognition = overflow_error(capture.overflow_at_arm, overflow_at_close)
                    .map_or_else(|| finalize_segment(recognizer, &segment), Err);
                let result = recognition.map(|transcript| {
                    counters.transcripts.fetch_add(1, Ordering::Relaxed);
                    CaptureReport {
                        sequence: capture.sequence,
                        transcript,
                        partials: capture.partials.clone(),
                        endpoint: segment.endpoint(),
                        vad_close_to_final_us: duration_us(vad_evaluation_started_at.elapsed()),
                        vad_evaluation_started_at,
                        normalization_resampling_us: capture.normalization_resampling_us,
                        input_frames: capture.input_frames,
                        output_frames: capture.output_frames,
                        overflow: overflow_at_close,
                    }
                });
                let recognition_failed = result.is_err();
                complete_capture(worker_status, capture, result);
                return !recognition_failed;
            }
        }
    }
    true
}

fn fail_barge_in(
    worker_status: &WorkerStatus,
    handle: &BargeInHandle,
    error: impl std::fmt::Display,
) {
    worker_status.mark_barge_failure(bounded(&error.to_string()));
    handle.cancel_for_failure();
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

fn update_global_conversion_counters(counters: &CaptureCounters, report: CaptureResampleReport) {
    counters.input_frames.fetch_add(
        u64::try_from(report.input_frames).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    counters.output_frames.fetch_add(
        u64::try_from(report.output_frames).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
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

fn drain_discard(consumer: &mut Consumer<TimedCaptureSample>) {
    while consumer.pop().is_ok() {}
}

fn duration_us(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests;
