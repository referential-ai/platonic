use std::{
    sync::{Barrier, mpsc},
    thread,
};

use crate::{PcmData, SampleFormat};

use super::*;
use crate::core::playback::CallbackDrain;

struct FixedSynth {
    format: AudioFormat,
    samples: Vec<f32>,
}

impl SpeechSynthesizer for FixedSynth {
    fn output_format(&self) -> AudioFormat {
        self.format
    }

    fn synthesize(
        &mut self,
        _sentence: &Sentence,
        sink: &mut dyn PcmSink,
        _cancel: &AtomicBool,
    ) -> Result<(), SynthError> {
        sink.push(PcmChunk::new(
            self.format,
            PcmData::F32(self.samples.clone().into_boxed_slice()),
        )?)?;
        Ok(())
    }
}

struct FailingSynth {
    format: AudioFormat,
}

impl SpeechSynthesizer for FailingSynth {
    fn output_format(&self) -> AudioFormat {
        self.format
    }

    fn synthesize(
        &mut self,
        _sentence: &Sentence,
        _sink: &mut dyn PcmSink,
        _cancel: &AtomicBool,
    ) -> Result<(), SynthError> {
        Err(SynthError::InvalidConfig {
            reason: "synthetic failure".to_owned(),
        })
    }
}

struct MultipleChunkSynth {
    format: AudioFormat,
}

impl SpeechSynthesizer for MultipleChunkSynth {
    fn output_format(&self) -> AudioFormat {
        self.format
    }

    fn synthesize(
        &mut self,
        _sentence: &Sentence,
        sink: &mut dyn PcmSink,
        _cancel: &AtomicBool,
    ) -> Result<(), SynthError> {
        sink.push(PcmChunk::from_f32(self.format, vec![0.5; 8])?)?;
        sink.push(PcmChunk::from_f32(self.format, vec![0.25; 8])?)?;
        Ok(())
    }
}

struct BarrierSynth {
    format: AudioFormat,
    entered: mpsc::Sender<()>,
    release: Arc<Barrier>,
    block_once: bool,
}

struct CancelAwareSecondBarrierSynth {
    format: AudioFormat,
    samples: Vec<f32>,
    calls: usize,
    entered: mpsc::Sender<()>,
    release: Arc<Barrier>,
    observed_cancel: Arc<AtomicBool>,
}

impl SpeechSynthesizer for CancelAwareSecondBarrierSynth {
    fn output_format(&self) -> AudioFormat {
        self.format
    }

    fn synthesize(
        &mut self,
        _sentence: &Sentence,
        sink: &mut dyn PcmSink,
        cancel: &AtomicBool,
    ) -> Result<(), SynthError> {
        self.calls = self.calls.saturating_add(1);
        if self.calls == 2 {
            self.entered.send(()).unwrap();
            self.release.wait();
            if cancel.load(Ordering::Acquire) {
                self.observed_cancel.store(true, Ordering::Release);
                return Err(SynthError::Canceled);
            }
        }
        sink.push(PcmChunk::from_f32(self.format, self.samples.clone())?)?;
        Ok(())
    }
}

impl SpeechSynthesizer for BarrierSynth {
    fn output_format(&self) -> AudioFormat {
        self.format
    }

    fn synthesize(
        &mut self,
        _sentence: &Sentence,
        sink: &mut dyn PcmSink,
        _cancel: &AtomicBool,
    ) -> Result<(), SynthError> {
        if self.block_once {
            self.block_once = false;
            self.entered.send(()).unwrap();
            self.release.wait();
        }
        sink.push(PcmChunk::from_f32(self.format, vec![0.5; 1_024])?)?;
        Ok(())
    }
}

fn formats() -> (AudioFormat, AudioFormat) {
    (
        AudioFormat::new(24_000, 1, SampleFormat::F32).unwrap(),
        AudioFormat::new(48_000, 2, SampleFormat::F32).unwrap(),
    )
}

fn synthetic_worker<S: SpeechSynthesizer + 'static>(
    synthesizer: S,
    ring_capacity: usize,
) -> (SynthWorker, CallbackDrain, Arc<AtomicBool>) {
    synthetic_worker_with_initial_cancel(synthesizer, ring_capacity, false)
}

fn synthetic_worker_with_initial_cancel<S: SpeechSynthesizer + 'static>(
    synthesizer: S,
    ring_capacity: usize,
    canceled: bool,
) -> (SynthWorker, CallbackDrain, Arc<AtomicBool>) {
    let (_, device) = formats();
    let cancel = Arc::new(AtomicBool::new(canceled));
    let barge_in = BargeInHandle::new(Arc::clone(&cancel));
    let (playback, producer, callback) =
        PersistentPlayback::test_pair(device, ring_capacity, barge_in.clone());
    let plan = ResamplingPlan::new(synthesizer.output_format(), device).unwrap();
    let worker = SynthWorker::spawn_with_parts(
        Box::new(synthesizer),
        playback,
        producer,
        plan,
        Arc::clone(&cancel),
        barge_in,
    )
    .unwrap();
    worker.begin_run().unwrap();
    (worker, callback, cancel)
}

fn sentence(index: usize) -> Sentence {
    Sentence::new(format!("Synthetic sentence number {index}.")).unwrap()
}

fn drain_until(callback: &mut CallbackDrain, timeline: &PlaybackTimeline, count: u64) {
    for _ in 0..1_000_000 {
        if timeline.finished_sentences() >= count {
            return;
        }
        let mut output = [0.0_f32; 32];
        callback.write_f32(&mut output, 2);
        thread::yield_now();
    }
    panic!("synthetic callback did not drain {count} sentences");
}

fn drive_until_gate(worker: &SynthWorker, callback: &mut CallbackDrain) {
    let barge_in = worker.barge_in_handle();
    for _ in 0..1_000_000 {
        let mut output = [0.0_f32; 32];
        callback.write_f32(&mut output, 2);
        if barge_in.gate_open() {
            return;
        }
        thread::yield_now();
    }
    panic!("synthetic callback did not reach the self-playback gate");
}

fn wait_for_idle_with_callbacks(
    worker: &SynthWorker,
    callback: &mut CallbackDrain,
) -> Vec<SynthesizedSentenceReport> {
    let (result_tx, result_rx) = mpsc::channel();
    thread::scope(|scope| {
        scope.spawn(|| result_tx.send(worker.wait_until_idle()).unwrap());
        for _ in 0..1_000_000 {
            match result_rx.try_recv() {
                Ok(result) => return result.unwrap(),
                Err(mpsc::TryRecvError::Empty) => {
                    let mut output = [1.0_f32; 32];
                    callback.write_f32(&mut output, 2);
                    thread::yield_now();
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    panic!("synth wait disconnected before reaching idle")
                }
            }
        }
        panic!("synth worker did not reach idle while callbacks advanced")
    })
}

#[test]
fn cancel_before_chunk_rejects_admission_and_flushes_once_without_a_latch() {
    let (source, _) = formats();
    let synth = FixedSynth {
        format: source,
        samples: vec![0.5; 1_024],
    };
    let (worker, mut callback, cancel) = synthetic_worker_with_initial_cancel(synth, 4_096, true);

    assert!(cancel.load(Ordering::Acquire));
    assert!(matches!(
        worker.accept(sentence(0), SpeechSource::new(0, 0)),
        Err(SynthWorkerError::Canceled)
    ));
    assert!(wait_for_idle_with_callbacks(&worker, &mut callback).is_empty());
    cancel.store(true, Ordering::Release);
    assert!(worker.wait_until_idle().unwrap().is_empty());
    let metrics = worker.barge_in_metrics();
    assert_eq!(metrics.sentence_queue_flushes, 1);
    assert_eq!(metrics.pcm_queue_flushes, 0);
    assert_eq!(worker.finish_run().unwrap(), None);
    worker.shutdown().unwrap();
}

#[test]
fn cancellation_during_synthesis_abandons_the_chunk_and_silences_the_callback() {
    let (source, _) = formats();
    let (entered_tx, entered_rx) = mpsc::channel();
    let release = Arc::new(Barrier::new(2));
    let observed_cancel = Arc::new(AtomicBool::new(false));
    let synth = CancelAwareSecondBarrierSynth {
        format: source,
        samples: vec![0.5; 12_000],
        calls: 0,
        entered: entered_tx,
        release: Arc::clone(&release),
        observed_cancel: Arc::clone(&observed_cancel),
    };
    let (worker, mut callback, cancel) = synthetic_worker(synth, 48_000);
    worker.accept(sentence(0), SpeechSource::new(0, 2)).unwrap();
    worker.accept(sentence(1), SpeechSource::new(1, 4)).unwrap();
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    drive_until_gate(&worker, &mut callback);
    let barge_in = worker.barge_in_handle();
    assert!(barge_in.trigger_speech_onset());
    assert!(Arc::ptr_eq(&cancel, &worker.cancel));
    let played_at_decision = worker.playback.timeline().played_samples();
    let mut output = [1.0_f32; 32];
    callback.write_f32(&mut output, 2);
    assert!(output.iter().all(|sample| *sample == 0.0));
    assert_eq!(
        worker.playback.timeline().played_samples(),
        played_at_decision
    );
    release.wait();

    assert!(wait_for_idle_with_callbacks(&worker, &mut callback).is_empty());
    assert!(observed_cancel.load(Ordering::Acquire));
    let interruption = worker.finish_run().unwrap().unwrap();
    assert_eq!(interruption.sentence_index, 0);
    let metrics = worker.barge_in_metrics();
    assert_eq!(metrics.sentence_queue_flushes, 1);
    assert_eq!(metrics.pcm_queue_flushes, 1);
    worker.shutdown().unwrap();
}

#[test]
fn queue_full_waiter_wakes_canceled_without_deadlock_or_double_flush() {
    let (source, _) = formats();
    let (entered_tx, entered_rx) = mpsc::channel();
    let release = Arc::new(Barrier::new(2));
    let synth = BarrierSynth {
        format: source,
        entered: entered_tx,
        release: Arc::clone(&release),
        block_once: true,
    };
    let (worker, mut callback, cancel) = synthetic_worker(synth, 4_096);
    for index in 0..crate::SENTENCE_PREFETCH_CAPACITY {
        worker
            .try_accept(
                sentence(index),
                SpeechSource::new(index as u64, index as u64),
            )
            .unwrap();
    }
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    thread::scope(|scope| {
        let (result_tx, result_rx) = mpsc::channel();
        let worker_ref = &worker;
        scope.spawn(move || {
            result_tx
                .send(worker_ref.accept(sentence(4), SpeechSource::new(4, 4)))
                .unwrap()
        });
        thread::sleep(Duration::from_millis(5));
        cancel.store(true, Ordering::Release);
        assert!(matches!(
            result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Err(SynthWorkerError::Canceled)
        ));
    });
    release.wait();
    assert!(wait_for_idle_with_callbacks(&worker, &mut callback).is_empty());
    assert!(worker.wait_until_idle().unwrap().is_empty());
    let metrics = worker.barge_in_metrics();
    assert_eq!(metrics.sentence_queue_flushes, 1);
    assert_eq!(metrics.discarded_sentences, 4);
    assert_eq!(metrics.pcm_queue_flushes, 1);
    assert_eq!(worker.finish_run().unwrap(), None);
    worker.shutdown().unwrap();
}

#[test]
fn callback_drain_stops_at_one_quantum_and_latch_matches_emitted_samples() {
    let (source, _) = formats();
    let synth = FixedSynth {
        format: source,
        samples: vec![0.5; 12_000],
    };
    let (worker, mut callback, _cancel) = synthetic_worker(synth, 48_000);
    worker
        .accept(
            Sentence::new("one two three four").unwrap(),
            SpeechSource::new(7, 11),
        )
        .unwrap();
    drive_until_gate(&worker, &mut callback);
    let barge_in = worker.barge_in_handle();
    assert!(barge_in.trigger_speech_onset());
    assert!(!barge_in.trigger_speech_onset());
    let played_at_decision = worker.playback.timeline().played_samples();
    let mut output = [1.0_f32; 32];
    callback.write_f32(&mut output, 2);
    assert!(output.iter().all(|sample| *sample == 0.0));
    assert_eq!(
        worker.playback.timeline().played_samples(),
        played_at_decision
    );

    assert!(wait_for_idle_with_callbacks(&worker, &mut callback).is_empty());
    let interruption = worker.finish_run().unwrap().unwrap();
    assert_eq!(interruption.played_samples, played_at_decision);
    assert_eq!(interruption.sentence_index, 7);
    assert_eq!(interruption.assistant_delta_index, 11);
    assert_eq!(interruption.spoken_prefix, "one");
    let metrics = worker.barge_in_metrics();
    assert_eq!(metrics.sentence_queue_flushes, 1);
    assert_eq!(metrics.pcm_queue_flushes, 1);
    assert_eq!(metrics.silent_callback_frames, Some(16));
    assert_eq!(
        metrics.discarded_pcm_frames,
        metrics.queued_pcm_frames_at_decision as u64
    );
    worker.shutdown().unwrap();
}

#[test]
fn terminal_run_cancel_race_is_idempotent_and_never_fabricates_interruption() {
    let (source, _) = formats();
    let synth = FixedSynth {
        format: source,
        samples: vec![0.5; 1_024],
    };
    let (worker, mut callback, cancel) = synthetic_worker(synth, 4_096);
    worker.accept(sentence(0), SpeechSource::new(0, 0)).unwrap();
    drain_until(&mut callback, worker.playback.timeline().as_ref(), 1);
    cancel.store(true, Ordering::Release);
    let reports = wait_for_idle_with_callbacks(&worker, &mut callback);
    assert!(reports.len() <= 1);
    assert!(worker.wait_until_idle().unwrap().is_empty());
    let metrics = worker.barge_in_metrics();
    assert_eq!(metrics.sentence_queue_flushes, 1);
    assert_eq!(metrics.pcm_queue_flushes, 1);
    assert_eq!(worker.finish_run().unwrap(), None);
    worker.shutdown().unwrap();
}

#[test]
fn full_window_blocks_admission_until_callback_drain_frees_the_oldest_job() {
    let (source, _) = formats();
    let (entered_tx, entered_rx) = mpsc::channel();
    let release = Arc::new(Barrier::new(2));
    let synth = BarrierSynth {
        format: source,
        entered: entered_tx,
        release: Arc::clone(&release),
        block_once: true,
    };
    let (worker, mut callback, _cancel) = synthetic_worker(synth, 128);
    for index in 0..crate::SENTENCE_PREFETCH_CAPACITY {
        worker
            .try_accept(
                sentence(index),
                SpeechSource::new(index as u64, index as u64),
            )
            .unwrap();
    }
    entered_rx.recv().unwrap();
    assert!(matches!(
        worker.try_accept(sentence(5), SpeechSource::new(5, 5)),
        Err(SynthWorkerError::Queue(SentenceQueueError::Full {
            capacity: 4
        }))
    ));

    let timeline = Arc::clone(worker.playback.timeline());
    let (started_tx, started_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    thread::scope(|scope| {
        scope.spawn(|| {
            started_tx.send(()).unwrap();
            result_tx
                .send(worker.accept(sentence(4), SpeechSource::new(4, 4)))
                .unwrap();
        });
        started_rx.recv().unwrap();
        assert!(matches!(
            result_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        release.wait();
        let admission = (0..1_000_000)
            .find_map(|_| match result_rx.try_recv() {
                Ok(result) => Some(result),
                Err(mpsc::TryRecvError::Empty) => {
                    let mut output = [0.0_f32; 32];
                    callback.write_f32(&mut output, 2);
                    thread::yield_now();
                    None
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    panic!("blocking producer disconnected before admission")
                }
            })
            .expect("blocking producer must resume after a bounded callback drain")
            .unwrap();
        assert_eq!(admission.sequence, 4);
        assert!(timeline.finished_sentences() >= 1);
        drain_until(&mut callback, timeline.as_ref(), 5);
        let mut reports = admission.completed;
        reports.extend(worker.wait_until_idle().unwrap());
        assert_eq!(reports.len(), 5);
        assert_eq!(
            reports
                .iter()
                .map(|report| report.playback.sequence)
                .collect::<Vec<_>>(),
            [0, 1, 2, 3, 4]
        );
    });
    assert_eq!(worker.playback_metrics().max_accepted_unfinished, 4);
    worker.shutdown().unwrap();
}

#[test]
fn synthetic_resampling_and_callback_drain_preserve_sentence_order() {
    let (source, _) = formats();
    let synth = FixedSynth {
        format: source,
        samples: vec![0.5; 1_024],
    };
    let (worker, mut callback, _cancel) = synthetic_worker(synth, 64);
    for index in 0..3 {
        worker
            .accept(
                sentence(index),
                SpeechSource::new(index as u64, index as u64),
            )
            .unwrap();
    }
    drain_until(&mut callback, worker.playback.timeline().as_ref(), 3);
    let reports = worker.wait_until_idle().unwrap();
    assert_eq!(
        reports
            .iter()
            .map(|report| report.sentence.as_str())
            .collect::<Vec<_>>(),
        [
            "Synthetic sentence number 0.",
            "Synthetic sentence number 1.",
            "Synthetic sentence number 2."
        ]
    );
    assert!(
        reports
            .iter()
            .all(|report| report.playback.source_frames == 1_024)
    );
    assert!(
        reports
            .iter()
            .all(|report| report.playback.device_frames == 2_048)
    );
    assert!(reports[1].playback.gap_before_us.is_some());
    worker.shutdown().unwrap();
}

#[test]
fn synth_failure_closes_waiters_with_original_typed_error() {
    let (source, _) = formats();
    let (worker, _callback, _cancel) = synthetic_worker(FailingSynth { format: source }, 8);
    worker.accept(sentence(0), SpeechSource::new(0, 0)).unwrap();
    let error = worker.wait_until_idle().unwrap_err();
    assert!(matches!(
        error,
        SynthWorkerFailure::Synthesis { sequence: 0, error }
            if matches!(error.as_ref(), SynthError::InvalidConfig { reason } if reason == "synthetic failure")
    ));
    assert!(matches!(
        worker.try_accept(sentence(1), SpeechSource::new(1, 1)),
        Err(SynthWorkerError::Failed(
            SynthWorkerFailure::Synthesis { .. }
        ))
    ));
    assert!(worker.shutdown().is_err());
}

#[test]
fn multiple_chunks_fail_as_a_typed_output_contract_violation() {
    let (source, _) = formats();
    let (worker, _callback, _cancel) = synthetic_worker(MultipleChunkSynth { format: source }, 16);
    worker.accept(sentence(0), SpeechSource::new(0, 0)).unwrap();
    assert!(matches!(
        worker.wait_until_idle(),
        Err(SynthWorkerFailure::OutputContract {
            sequence: 0,
            chunks: 2
        })
    ));
    assert!(worker.shutdown().is_err());
}

#[test]
fn playback_failure_is_typed_and_never_waits_for_teardown() {
    let (source, _) = formats();
    let synth = FixedSynth {
        format: source,
        samples: vec![0.5; 1_024],
    };
    let (worker, _callback, _cancel) = synthetic_worker(synth, 64);
    worker.accept(sentence(0), SpeechSource::new(0, 0)).unwrap();
    worker.playback.timeline().mark_stream_failed();
    let error = worker.wait_until_idle().unwrap_err();
    assert!(matches!(
        error,
        SynthWorkerFailure::Playback {
            error: DeviceError::StreamFailed,
            ..
        }
    ));
    assert!(worker.shutdown().is_err());
}

#[test]
fn close_drains_all_accepted_pcm_then_joins_exactly_once() {
    let (source, _) = formats();
    let synth = FixedSynth {
        format: source,
        samples: vec![0.5; 1_024],
    };
    let (worker, mut callback, _cancel) = synthetic_worker(synth, 128);
    let timeline = Arc::clone(worker.playback.timeline());
    for index in 0..4 {
        worker
            .accept(
                sentence(index),
                SpeechSource::new(index as u64, index as u64),
            )
            .unwrap();
    }
    worker.close_admission();
    assert!(matches!(
        worker.try_accept(sentence(5), SpeechSource::new(5, 5)),
        Err(SynthWorkerError::Queue(SentenceQueueError::Closed))
    ));
    let drain = thread::spawn(move || drain_until(&mut callback, timeline.as_ref(), 4));
    let shutdown = worker.shutdown().unwrap();
    drain.join().unwrap();
    assert!(shutdown.worker_joined);
    assert!(shutdown.playback_closed);
    assert_eq!(shutdown.completed_sentences, 4);
    assert_eq!(shutdown.playback.max_accepted_unfinished, 4);
}
