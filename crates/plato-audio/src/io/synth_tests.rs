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
) -> (SynthWorker, CallbackDrain) {
    let (_, device) = formats();
    let (playback, producer, callback) = PersistentPlayback::test_pair(device, ring_capacity);
    let plan = ResamplingPlan::new(synthesizer.output_format(), device).unwrap();
    (
        SynthWorker::spawn_with_parts(Box::new(synthesizer), playback, producer, plan).unwrap(),
        callback,
    )
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
    let (worker, mut callback) = synthetic_worker(synth, 128);
    let cancel = Arc::new(AtomicBool::new(false));
    for index in 0..crate::SENTENCE_PREFETCH_CAPACITY {
        worker
            .try_accept(sentence(index), Arc::clone(&cancel))
            .unwrap();
    }
    entered_rx.recv().unwrap();
    assert!(matches!(
        worker.try_accept(sentence(5), Arc::clone(&cancel)),
        Err(SynthWorkerError::Queue(SentenceQueueError::Full {
            capacity: 4
        }))
    ));

    let timeline = Arc::clone(worker.playback.timeline());
    let (started_tx, started_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    thread::scope(|scope| {
        let cancel = Arc::clone(&cancel);
        scope.spawn(|| {
            started_tx.send(()).unwrap();
            result_tx.send(worker.accept(sentence(4), cancel)).unwrap();
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
    let (worker, mut callback) = synthetic_worker(synth, 64);
    let cancel = Arc::new(AtomicBool::new(false));
    for index in 0..3 {
        worker.accept(sentence(index), Arc::clone(&cancel)).unwrap();
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
    let (worker, _callback) = synthetic_worker(FailingSynth { format: source }, 8);
    worker
        .accept(sentence(0), Arc::new(AtomicBool::new(false)))
        .unwrap();
    let error = worker.wait_until_idle().unwrap_err();
    assert!(matches!(
        error,
        SynthWorkerFailure::Synthesis { sequence: 0, error }
            if matches!(error.as_ref(), SynthError::InvalidConfig { reason } if reason == "synthetic failure")
    ));
    assert!(matches!(
        worker.try_accept(sentence(1), Arc::new(AtomicBool::new(false))),
        Err(SynthWorkerError::Failed(
            SynthWorkerFailure::Synthesis { .. }
        ))
    ));
    assert!(worker.shutdown().is_err());
}

#[test]
fn multiple_chunks_fail_as_a_typed_output_contract_violation() {
    let (source, _) = formats();
    let (worker, _callback) = synthetic_worker(MultipleChunkSynth { format: source }, 16);
    worker
        .accept(sentence(0), Arc::new(AtomicBool::new(false)))
        .unwrap();
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
    let (worker, _callback) = synthetic_worker(synth, 64);
    worker
        .accept(sentence(0), Arc::new(AtomicBool::new(false)))
        .unwrap();
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
    let (worker, mut callback) = synthetic_worker(synth, 128);
    let timeline = Arc::clone(worker.playback.timeline());
    for index in 0..4 {
        worker
            .accept(sentence(index), Arc::new(AtomicBool::new(false)))
            .unwrap();
    }
    worker.close_admission();
    assert!(matches!(
        worker.try_accept(sentence(5), Arc::new(AtomicBool::new(false))),
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
