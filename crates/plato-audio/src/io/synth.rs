use std::{
    collections::VecDeque,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Condvar, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use serde::Serialize;
use thiserror::Error;

use super::playback::{
    PersistentPlayback, PlaybackConfig, PlaybackDeviceInfo, PlaybackMetrics, PlaybackProducer,
    PlaybackReport, PlaybackUnderrun, PlaybackWriteError,
};
use crate::{
    AudioFormat, BargeInHandle, BargeInMetrics, DeviceError, PcmChunk, PcmSinkError, ResampleError,
    ResamplingPlan, Sentence, SentenceQueueError, SpeechSource, SpokenInterruption, SynthError,
    core::{
        latch::InterruptionLatch,
        playback::{PlaybackObservation, PlaybackTimeline},
        prefetch::{PrefetchWindow, SentenceJobStage},
    },
};

const WORKER_POLL_INTERVAL: Duration = Duration::from_millis(1);
const CALLBACK_STALL_TIMEOUT: Duration = Duration::from_secs(5);

/// A synchronous consumer for model-emitted PCM chunks.
pub trait PcmSink {
    /// Accepts one validated chunk without assigning run or session meaning.
    fn push(&mut self, chunk: PcmChunk) -> Result<(), PcmSinkError>;
}

/// A synchronous speech engine whose resident state is reused across calls.
pub trait SpeechSynthesizer: Send {
    /// Returns the exact PCM format produced by this engine.
    fn output_format(&self) -> AudioFormat;

    /// Synthesizes one sentence and pushes its PCM synchronously into `sink`.
    fn synthesize(
        &mut self,
        sentence: &Sentence,
        sink: &mut dyn PcmSink,
        cancel: &AtomicBool,
    ) -> Result<(), SynthError>;
}

impl PcmSink for Vec<PcmChunk> {
    fn push(&mut self, chunk: PcmChunk) -> Result<(), PcmSinkError> {
        self.push(chunk);
        Ok(())
    }
}

/// Failures before the owned synth thread begins accepting work.
#[derive(Clone, Debug, Error)]
pub enum SynthWorkerStartError {
    /// The persistent output stream could not be opened.
    #[error(transparent)]
    Playback(#[from] DeviceError),
    /// The fixed source/device conversion plan could not be built.
    #[error(transparent)]
    Resampling(#[from] ResampleError),
    /// The one owned worker thread could not be created.
    #[error("cannot start synth worker thread: {reason}")]
    ThreadStart {
        /// Operating-system thread diagnostic.
        reason: String,
    },
}

/// Terminal outcome owned by the single synth worker.
#[derive(Clone, Debug, Error)]
pub enum SynthWorkerFailure {
    /// The resident engine failed one accepted sentence.
    #[error("sentence {sequence} synthesis failed: {error}")]
    Synthesis {
        /// Worker-global accepted order.
        sequence: u64,
        /// Original typed synthesis error.
        #[source]
        error: Arc<SynthError>,
    },
    /// The admitted Kokoro path emitted an unexpected chunk count.
    #[error("sentence {sequence} emitted {chunks} PCM chunks; expected exactly one")]
    OutputContract {
        /// Worker-global accepted order.
        sequence: u64,
        /// Chunks supplied to the worker sink.
        chunks: usize,
    },
    /// The resident rubato plan failed one accepted sentence.
    #[error("sentence {sequence} resampling failed: {error}")]
    Resampling {
        /// Worker-global accepted order.
        sequence: u64,
        /// Typed conversion error.
        #[source]
        error: ResampleError,
    },
    /// The persistent device or PCM ring failed.
    #[error("sentence {sequence} playback failed: {error}")]
    Playback {
        /// Oldest accepted sentence affected by the failure.
        sequence: u64,
        /// Typed playback error.
        #[source]
        error: DeviceError,
    },
    /// The worker panicked; the owner caught the unwind and closed admission.
    #[error("synth worker panicked")]
    Panicked,
}

/// Admission or terminal failure returned to a sentence producer.
#[derive(Clone, Debug, Error)]
pub enum SynthWorkerError {
    /// The fixed sentence window rejected admission.
    #[error(transparent)]
    Queue(#[from] SentenceQueueError),
    /// The worker entered a terminal failure state.
    #[error(transparent)]
    Failed(#[from] SynthWorkerFailure),
    /// The one worker cancel authority stopped admission for this run.
    #[error("synthesis run canceled")]
    Canceled,
    /// A sentence was submitted outside an active synthesis run.
    #[error("synthesis run is not active")]
    RunInactive,
    /// A new synthesis run was requested before the prior run became idle.
    #[error("cannot begin synthesis run while accepted work remains")]
    RunActive,
}

/// One exact sentence paired with callback/sample timing.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SynthesizedSentenceReport {
    /// Exact trimmed text supplied to the resident engine.
    pub sentence: String,
    /// Synthesis, rtrb, callback, and underrun observation.
    pub playback: PlaybackReport,
}

/// One accepted sequence plus any earlier bounded reports freed by admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SentenceAdmission {
    /// Worker-global sequence assigned to the newly accepted sentence.
    pub sequence: u64,
    /// Earlier completed reports drained before the new slot was admitted.
    pub completed: Vec<SynthesizedSentenceReport>,
}

/// Clean close-and-join outcome for the one worker and persistent stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct SynthWorkerShutdown {
    /// True only after the owned `std::thread` joined.
    pub worker_joined: bool,
    /// True only after the persistent cpal stream was closed.
    pub playback_closed: bool,
    /// Exact number of synth worker threads constructed for this owner.
    pub synth_worker_threads: u64,
    /// Exact number of source/device rubato plans constructed for this owner.
    pub resampling_plan_builds: u64,
    /// Sentences completely drained before join.
    pub completed_sentences: u64,
    /// Final persistent callback and ring counters.
    pub playback: PlaybackMetrics,
}

struct QueuedSentence {
    sequence: u64,
    sentence: Option<Sentence>,
    text: String,
    source: SpeechSource,
}

struct WorkerState {
    window: PrefetchWindow,
    jobs: VecDeque<QueuedSentence>,
    completed: VecDeque<SynthesizedSentenceReport>,
    terminal: Option<SynthWorkerFailure>,
    worker_exited: bool,
    max_accepted_unfinished: usize,
    last_pcm_end_ns: Option<u64>,
    latch: InterruptionLatch,
    interruption: Option<SpokenInterruption>,
    run_active: bool,
    cancel_handled: bool,
    cancel_flush_in_progress: bool,
}

impl WorkerState {
    fn new() -> Self {
        Self {
            window: PrefetchWindow::new(),
            jobs: VecDeque::with_capacity(crate::SENTENCE_PREFETCH_CAPACITY),
            completed: VecDeque::with_capacity(crate::SENTENCE_PREFETCH_CAPACITY),
            terminal: None,
            worker_exited: false,
            max_accepted_unfinished: 0,
            last_pcm_end_ns: None,
            latch: InterruptionLatch::default(),
            interruption: None,
            run_active: false,
            cancel_handled: false,
            cancel_flush_in_progress: false,
        }
    }
}

struct SharedWorker {
    state: Mutex<WorkerState>,
    changed: Condvar,
    timeline: Arc<PlaybackTimeline>,
    cancel: Arc<AtomicBool>,
    barge_in: BargeInHandle,
}

impl SharedWorker {
    fn lock(&self) -> MutexGuard<'_, WorkerState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn fail(&self, sequence: u64, failure: SynthWorkerFailure) {
        let mut state = self.lock();
        let _ = state.window.fail(sequence);
        state.terminal.get_or_insert(failure);
        self.timeline.mark_failed(sequence);
        self.changed.notify_all();
    }

    fn mark_panicked(&self) {
        let mut state = self.lock();
        state.window.close();
        state.terminal.get_or_insert(SynthWorkerFailure::Panicked);
        state.worker_exited = true;
        self.changed.notify_all();
    }
}

/// One owned synthesis thread feeding one persistent callback through rtrb.
pub struct SynthWorker {
    shared: Arc<SharedWorker>,
    join: Option<JoinHandle<()>>,
    playback: PersistentPlayback,
    cancel: Arc<AtomicBool>,
    barge_in: BargeInHandle,
}

impl SynthWorker {
    /// Opens the device, builds one rubato plan, then starts one owned worker.
    pub fn spawn<S>(
        synthesizer: S,
        playback_config: PlaybackConfig,
        cancel: Arc<AtomicBool>,
    ) -> Result<Self, SynthWorkerStartError>
    where
        S: SpeechSynthesizer + 'static,
    {
        let source_format = synthesizer.output_format();
        let barge_in = BargeInHandle::new(Arc::clone(&cancel));
        let (playback, producer) = PersistentPlayback::open(playback_config, barge_in.clone())?;
        let plan = ResamplingPlan::new(source_format, playback.device_info().format)?;
        Self::spawn_with_parts(
            Box::new(synthesizer),
            playback,
            producer,
            plan,
            cancel,
            barge_in,
        )
    }

    fn spawn_with_parts(
        synthesizer: Box<dyn SpeechSynthesizer>,
        playback: PersistentPlayback,
        producer: PlaybackProducer,
        plan: ResamplingPlan,
        cancel: Arc<AtomicBool>,
        barge_in: BargeInHandle,
    ) -> Result<Self, SynthWorkerStartError> {
        let shared = Arc::new(SharedWorker {
            state: Mutex::new(WorkerState::new()),
            changed: Condvar::new(),
            timeline: Arc::clone(playback.timeline()),
            cancel: Arc::clone(&cancel),
            barge_in: barge_in.clone(),
        });
        let worker_shared = Arc::clone(&shared);
        let join = thread::Builder::new()
            .name("plato-audio-synth".to_owned())
            .spawn(move || {
                let result = catch_unwind(AssertUnwindSafe(|| {
                    worker_loop(worker_shared.as_ref(), synthesizer, producer, plan)
                }));
                if result.is_err() {
                    worker_shared.mark_panicked();
                }
            })
            .map_err(|error| SynthWorkerStartError::ThreadStart {
                reason: error.to_string(),
            })?;
        Ok(Self {
            shared,
            join: Some(join),
            playback,
            cancel,
            barge_in,
        })
    }

    /// Begins one run after the prior run is complete without erasing a caller pre-cancel.
    pub fn begin_run(&self) -> Result<(), SynthWorkerError> {
        let mut state = self.shared.lock();
        if let Some(failure) = state.terminal.clone() {
            return Err(failure.into());
        }
        if state.run_active || !state.window.is_empty() {
            return Err(SynthWorkerError::RunActive);
        }
        state.run_active = true;
        state.cancel_handled = false;
        state.cancel_flush_in_progress = false;
        state.interruption = None;
        state.latch.begin(self.shared.timeline.played_samples());
        self.barge_in.begin_run();
        self.shared.changed.notify_all();
        Ok(())
    }

    /// Blocks until the fixed window accepts this sentence or closes/fails.
    pub fn accept(
        &self,
        sentence: Sentence,
        source: SpeechSource,
    ) -> Result<SentenceAdmission, SynthWorkerError> {
        self.accept_inner(sentence, source, true)
    }

    /// Attempts admission without waiting when four jobs remain unfinished.
    pub fn try_accept(
        &self,
        sentence: Sentence,
        source: SpeechSource,
    ) -> Result<SentenceAdmission, SynthWorkerError> {
        self.accept_inner(sentence, source, false)
    }

    /// Returns a terminal worker failure without waiting for a sentence event.
    pub fn check_health(&self) -> Result<(), SynthWorkerFailure> {
        let state = self.shared.lock();
        state.terminal.clone().map_or(Ok(()), Err)
    }

    /// Waits for every accepted sentence and drains its ordered reports.
    pub fn wait_until_idle(&self) -> Result<Vec<SynthesizedSentenceReport>, SynthWorkerFailure> {
        let mut state = self.shared.lock();
        loop {
            if let Some(failure) = state.terminal.clone() {
                return Err(failure);
            }
            let cancellation_pending =
                state.run_active && self.cancel.load(Ordering::Acquire) && !state.cancel_handled;
            if state.window.is_empty() && !state.cancel_flush_in_progress && !cancellation_pending {
                state.last_pcm_end_ns = None;
                return Ok(state.completed.drain(..).collect());
            }
            state = self
                .shared
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    /// Returns current persistent stream and bounded-window counters.
    pub fn playback_metrics(&self) -> PlaybackMetrics {
        let max = self.shared.lock().max_accepted_unfinished;
        self.playback.metrics(max)
    }

    /// Returns stop timing, gate, and queue snapshots for the active or last run.
    pub fn barge_in_metrics(&self) -> BargeInMetrics {
        self.barge_in.metrics()
    }

    /// Returns a cloneable capture-side handle bound to this worker's cancel atomic.
    pub fn barge_in_handle(&self) -> BargeInHandle {
        self.barge_in.clone()
    }

    /// Confirms that playback, synthesis, and the caller share one cancel allocation.
    pub fn uses_cancel(&self, cancel: &Arc<AtomicBool>) -> bool {
        Arc::ptr_eq(&self.cancel, cancel) && self.barge_in.uses_cancel(cancel)
    }

    /// Finishes an idle run and consumes its interruption latch at most once.
    pub fn finish_run(&self) -> Result<Option<SpokenInterruption>, SynthWorkerError> {
        let mut state = self.shared.lock();
        if !state.run_active {
            return Err(SynthWorkerError::RunInactive);
        }
        if !state.window.is_empty() || state.cancel_flush_in_progress {
            return Err(SynthWorkerError::RunActive);
        }
        state.run_active = false;
        self.barge_in.finish_run();
        Ok(state.interruption.take())
    }

    /// Returns exact live device and ring formats.
    pub fn device_info(&self) -> &PlaybackDeviceInfo {
        self.playback.device_info()
    }

    /// Idempotently closes new sentence admission while accepted work drains.
    pub fn close_admission(&self) {
        let mut state = self.shared.lock();
        state.window.close();
        self.shared.changed.notify_all();
    }

    /// Closes admission, drains accepted work, and joins the owned worker.
    pub fn shutdown(mut self) -> Result<SynthWorkerShutdown, SynthWorkerFailure> {
        self.close_and_join()
    }

    fn accept_inner(
        &self,
        sentence: Sentence,
        source: SpeechSource,
        wait_when_full: bool,
    ) -> Result<SentenceAdmission, SynthWorkerError> {
        let mut sentence = Some(sentence);
        let mut state = self.shared.lock();
        loop {
            if let Some(failure) = state.terminal.clone() {
                return Err(failure.into());
            }
            if !state.run_active {
                return Err(SynthWorkerError::RunInactive);
            }
            if self.cancel.load(Ordering::Acquire) {
                return Err(SynthWorkerError::Canceled);
            }
            match state.window.try_accept() {
                Ok(sequence) => {
                    let completed = state.completed.drain(..).collect();
                    if self
                        .shared
                        .timeline
                        .accept(sequence, Instant::now())
                        .is_err()
                    {
                        let failure = SynthWorkerFailure::Playback {
                            sequence,
                            error: DeviceError::CallbackContract,
                        };
                        let _ = state.window.fail(sequence);
                        state.terminal = Some(failure.clone());
                        self.shared.changed.notify_all();
                        return Err(failure.into());
                    }
                    let sentence = sentence.take().expect("accepted once");
                    let text = sentence.as_str().to_owned();
                    state.jobs.push_back(QueuedSentence {
                        sequence,
                        sentence: Some(sentence),
                        text,
                        source,
                    });
                    state.max_accepted_unfinished =
                        state.max_accepted_unfinished.max(state.window.len());
                    self.barge_in.set_queued_sentences(state.window.len());
                    self.shared.changed.notify_all();
                    return Ok(SentenceAdmission {
                        sequence,
                        completed,
                    });
                }
                Err(SentenceQueueError::Full { capacity }) if wait_when_full => {
                    debug_assert_eq!(capacity, crate::SENTENCE_PREFETCH_CAPACITY);
                    let (next, _) = self
                        .shared
                        .changed
                        .wait_timeout(state, WORKER_POLL_INTERVAL)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    state = next;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    fn close_and_join(&mut self) -> Result<SynthWorkerShutdown, SynthWorkerFailure> {
        self.close_admission();
        if let Some(join) = self.join.take()
            && join.join().is_err()
        {
            self.shared.mark_panicked();
        }
        self.playback.close();
        let state = self.shared.lock();
        if let Some(failure) = state.terminal.clone() {
            return Err(failure);
        }
        Ok(SynthWorkerShutdown {
            worker_joined: state.worker_exited,
            playback_closed: true,
            synth_worker_threads: 1,
            resampling_plan_builds: 1,
            completed_sentences: self.shared.timeline.finished_sentences(),
            playback: self.playback.metrics(state.max_accepted_unfinished),
        })
    }
}

impl Drop for SynthWorker {
    fn drop(&mut self) {
        let _ = self.close_and_join();
    }
}

struct SynthesisAction {
    sequence: u64,
    sentence: Sentence,
    text: String,
    source: SpeechSource,
}

#[derive(Default)]
struct SingleChunkSink {
    chunk: Option<PcmChunk>,
    pushed_chunks: usize,
}

impl PcmSink for SingleChunkSink {
    fn push(&mut self, chunk: PcmChunk) -> Result<(), PcmSinkError> {
        self.pushed_chunks = self.pushed_chunks.saturating_add(1);
        if self.chunk.is_some() {
            return Err(PcmSinkError::Rejected {
                reason: "fixed synth worker sink accepts one PCM chunk per sentence".to_owned(),
            });
        }
        self.chunk = Some(chunk);
        Ok(())
    }
}

fn worker_loop(
    shared: &SharedWorker,
    mut synthesizer: Box<dyn SpeechSynthesizer>,
    mut producer: PlaybackProducer,
    mut plan: ResamplingPlan,
) {
    let mut last_callback_count = shared.timeline.callback_count();
    let mut last_callback_progress = Instant::now();
    loop {
        let action = {
            let mut state = shared.lock();
            if shared.cancel.load(Ordering::Acquire)
                && state.run_active
                && !state.cancel_handled
                && !state.cancel_flush_in_progress
            {
                state.cancel_flush_in_progress = true;
                drop(state);
                if !flush_canceled_run(shared, &mut producer) {
                    return;
                }
                continue;
            }
            if !reap_finished(&mut state, shared) {
                return;
            }
            if state.terminal.is_some() {
                return;
            }
            if let Some(failure) = playback_health_failure(&state, shared) {
                let sequence = state.window.front().map_or(0, |(sequence, _)| sequence);
                drop(state);
                shared.fail(sequence, failure);
                return;
            }

            if state.window.is_empty() {
                last_callback_count = shared.timeline.callback_count();
                last_callback_progress = Instant::now();
            } else {
                let callbacks = shared.timeline.callback_count();
                if callbacks != last_callback_count {
                    last_callback_count = callbacks;
                    last_callback_progress = Instant::now();
                } else if last_callback_progress.elapsed() >= CALLBACK_STALL_TIMEOUT
                    && state.window.front().is_some_and(|(_, stage)| {
                        matches!(stage, SentenceJobStage::Buffered { .. })
                    })
                {
                    let sequence = state.window.front().expect("checked front").0;
                    let failure = SynthWorkerFailure::Playback {
                        sequence,
                        error: DeviceError::PlaybackTimeout {
                            milliseconds: CALLBACK_STALL_TIMEOUT.as_millis(),
                        },
                    };
                    drop(state);
                    shared.fail(sequence, failure);
                    return;
                }
            }

            if let Some(sequence) = state.window.next_accepted() {
                let job = state
                    .jobs
                    .iter_mut()
                    .find(|job| job.sequence == sequence)
                    .expect("window and payload queue agree");
                let sentence = job.sentence.take().expect("sentence starts once");
                let text = job.text.clone();
                let source = job.source;
                Some(SynthesisAction {
                    sequence,
                    sentence,
                    text,
                    source,
                })
            } else if state.window.is_closed() && state.window.is_empty() {
                state.worker_exited = true;
                shared.changed.notify_all();
                return;
            } else if !state.run_active && state.window.is_empty() {
                drop(
                    shared
                        .changed
                        .wait(state)
                        .unwrap_or_else(|poisoned| poisoned.into_inner()),
                );
                None
            } else {
                let (_state, _) = shared
                    .changed
                    .wait_timeout(state, WORKER_POLL_INTERVAL)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                None
            }
        };

        let Some(action) = action else {
            continue;
        };
        if shared.timeline.begin_synthesis(action.sequence).is_err() {
            shared.fail(
                action.sequence,
                SynthWorkerFailure::Playback {
                    sequence: action.sequence,
                    error: DeviceError::CallbackContract,
                },
            );
            return;
        }
        let mut sink = SingleChunkSink::default();
        let synthesis = synthesizer.synthesize(&action.sentence, &mut sink, shared.cancel.as_ref());
        if sink.pushed_chunks > 1 {
            shared.fail(
                action.sequence,
                SynthWorkerFailure::OutputContract {
                    sequence: action.sequence,
                    chunks: sink.pushed_chunks,
                },
            );
            return;
        }
        if matches!(synthesis, Err(SynthError::Canceled)) || shared.cancel.load(Ordering::Acquire) {
            let mut state = shared.lock();
            if !state.cancel_flush_in_progress {
                state.cancel_flush_in_progress = true;
            }
            drop(state);
            if !flush_canceled_run(shared, &mut producer) {
                return;
            }
            continue;
        }
        if let Err(error) = synthesis {
            shared.fail(
                action.sequence,
                SynthWorkerFailure::Synthesis {
                    sequence: action.sequence,
                    error: Arc::new(error),
                },
            );
            return;
        }
        let Some(chunk) = sink.chunk else {
            shared.fail(
                action.sequence,
                SynthWorkerFailure::OutputContract {
                    sequence: action.sequence,
                    chunks: 0,
                },
            );
            return;
        };
        let (chunk, resampling) = match plan.process(&chunk) {
            Ok(output) => output,
            Err(error) => {
                shared.fail(
                    action.sequence,
                    SynthWorkerFailure::Resampling {
                        sequence: action.sequence,
                        error,
                    },
                );
                return;
            }
        };
        if shared.cancel.load(Ordering::Acquire) {
            let mut state = shared.lock();
            state.cancel_flush_in_progress = true;
            drop(state);
            if !flush_canceled_run(shared, &mut producer) {
                return;
            }
            continue;
        }
        if shared.timeline.finish_synthesis(action.sequence).is_err() {
            shared.fail(
                action.sequence,
                SynthWorkerFailure::Playback {
                    sequence: action.sequence,
                    error: DeviceError::CallbackContract,
                },
            );
            return;
        }
        let prepared =
            match producer.prepare_sentence(action.sequence, resampling.source_frames, &chunk) {
                Ok(prepared) => prepared,
                Err(error) => {
                    shared.fail(
                        action.sequence,
                        SynthWorkerFailure::Playback {
                            sequence: action.sequence,
                            error,
                        },
                    );
                    return;
                }
            };
        {
            let mut state = shared.lock();
            if state
                .latch
                .record_sentence(
                    action.source,
                    &action.text,
                    prepared.start_sample,
                    prepared.end_sample,
                )
                .is_err()
            {
                drop(state);
                shared.fail(
                    action.sequence,
                    SynthWorkerFailure::Playback {
                        sequence: action.sequence,
                        error: DeviceError::CallbackContract,
                    },
                );
                return;
            }
        }
        match producer.write_prepared(prepared, shared.cancel.as_ref()) {
            Ok(()) => {}
            Err(PlaybackWriteError::Canceled) => {
                let mut state = shared.lock();
                state.cancel_flush_in_progress = true;
                drop(state);
                if !flush_canceled_run(shared, &mut producer) {
                    return;
                }
                continue;
            }
            Err(PlaybackWriteError::Device(error)) => {
                shared.fail(
                    action.sequence,
                    SynthWorkerFailure::Playback {
                        sequence: action.sequence,
                        error,
                    },
                );
                return;
            }
        }
        let mut state = shared.lock();
        if state
            .window
            .mark_buffered(action.sequence, resampling.device_frames)
            .is_err()
        {
            drop(state);
            shared.fail(
                action.sequence,
                SynthWorkerFailure::Playback {
                    sequence: action.sequence,
                    error: DeviceError::CallbackContract,
                },
            );
            return;
        }
        shared.changed.notify_all();
    }
}

fn flush_canceled_run(shared: &SharedWorker, producer: &mut PlaybackProducer) -> bool {
    let (sequences, next_sequence) = {
        let mut state = shared.lock();
        if state.cancel_handled {
            state.cancel_flush_in_progress = false;
            shared.changed.notify_all();
            return true;
        }
        let sequences = state.window.interrupt();
        state.jobs.clear();
        let next_sequence = state.window.next_sequence();
        shared.barge_in.flush_sentence_queue(sequences.len());
        shared.changed.notify_all();
        (sequences, next_sequence)
    };

    if (!sequences.is_empty() || shared.barge_in.playback_started())
        && let Err(error) = producer.flush(next_sequence)
    {
        let sequence = sequences.first().copied().unwrap_or(next_sequence);
        shared.fail(sequence, SynthWorkerFailure::Playback { sequence, error });
        return false;
    }

    for &sequence in &sequences {
        shared.timeline.mark_canceled(sequence);
    }

    let mut state = shared.lock();
    if shared.barge_in.metrics().speech_onset_decision_ns.is_some() {
        state.latch.interrupt(shared.timeline.played_samples());
        state.interruption = state.latch.take();
    }
    state.cancel_handled = true;
    state.cancel_flush_in_progress = false;
    state.last_pcm_end_ns = None;
    shared.changed.notify_all();
    true
}

fn playback_health_failure(
    state: &WorkerState,
    shared: &SharedWorker,
) -> Option<SynthWorkerFailure> {
    let sequence = state.window.front()?.0;
    if shared.timeline.stream_failed() {
        return Some(SynthWorkerFailure::Playback {
            sequence,
            error: DeviceError::StreamFailed,
        });
    }
    if shared.timeline.callback_contract_failed() {
        return Some(SynthWorkerFailure::Playback {
            sequence,
            error: DeviceError::CallbackContract,
        });
    }
    None
}

fn reap_finished(state: &mut WorkerState, shared: &SharedWorker) -> bool {
    while let Some((sequence, stage)) = state.window.front() {
        if !matches!(stage, SentenceJobStage::Buffered { .. })
            || !shared.timeline.is_finished(sequence)
        {
            break;
        }
        let Some(observation) = shared.timeline.observation(sequence) else {
            record_contract_failure(state, shared, sequence);
            return false;
        };
        if observation.first_non_silent_ns == u64::MAX {
            let failure = SynthWorkerFailure::Playback {
                sequence,
                error: DeviceError::SilentChunk,
            };
            let _ = state.window.fail(sequence);
            state.terminal = Some(failure);
            shared.changed.notify_all();
            return false;
        }
        let report = playback_report(sequence, observation, state.last_pcm_end_ns);
        state.last_pcm_end_ns = Some(observation.pcm_end_ns);
        if state.window.finish_front(sequence).is_err() {
            record_contract_failure(state, shared, sequence);
            return false;
        }
        let Some(job) = state.jobs.pop_front() else {
            record_contract_failure(state, shared, sequence);
            return false;
        };
        if job.sequence != sequence {
            record_contract_failure(state, shared, sequence);
            return false;
        }
        if state.completed.len() == crate::SENTENCE_PREFETCH_CAPACITY {
            record_contract_failure(state, shared, sequence);
            return false;
        }
        state.completed.push_back(SynthesizedSentenceReport {
            sentence: job.text,
            playback: report,
        });
        shared.barge_in.set_queued_sentences(state.window.len());
        shared.changed.notify_all();
    }
    true
}

fn record_contract_failure(state: &mut WorkerState, shared: &SharedWorker, sequence: u64) {
    let failure = SynthWorkerFailure::Playback {
        sequence,
        error: DeviceError::CallbackContract,
    };
    let _ = state.window.fail(sequence);
    state.terminal.get_or_insert(failure);
    shared.timeline.mark_failed(sequence);
    shared.changed.notify_all();
}

fn playback_report(
    sequence: u64,
    observation: PlaybackObservation,
    prior_pcm_end_ns: Option<u64>,
) -> PlaybackReport {
    PlaybackReport {
        sequence,
        accepted_ns: observation.accepted_ns,
        synth_started_ns: observation.synth_started_ns,
        synth_finished_ns: observation.synth_finished_ns,
        first_pcm_ns: observation.first_pcm_ns,
        first_non_silent_ns: observation.first_non_silent_ns,
        pcm_end_ns: observation.pcm_end_ns,
        accepted_to_first_non_silent_us: observation
            .first_non_silent_ns
            .saturating_sub(observation.accepted_ns)
            / 1_000,
        synthesis_us: observation
            .synth_finished_ns
            .saturating_sub(observation.synth_started_ns)
            / 1_000,
        gap_before_us: prior_pcm_end_ns
            .map(|prior| observation.first_pcm_ns.saturating_sub(prior) / 1_000),
        first_callback_frames: observation.first_callback_frames,
        callback_count: observation.callback_count,
        source_frames: observation.source_frames,
        device_frames: observation.device_frames,
        underrun: PlaybackUnderrun {
            callbacks: observation.underrun_callbacks,
            frames: observation.underrun_frames,
        },
    }
}

#[cfg(test)]
#[path = "synth_tests.rs"]
mod tests;
