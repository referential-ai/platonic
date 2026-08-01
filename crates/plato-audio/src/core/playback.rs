use std::{
    array,
    sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering},
    time::{Duration, Instant},
};

use rtrb::Consumer;

use super::prefetch::SENTENCE_PREFETCH_CAPACITY;

const VACANT: u8 = 0;
const ACCEPTED: u8 = 1;
const SYNTHESIZING: u8 = 2;
const BUFFERED: u8 = 3;
const PLAYING: u8 = 4;
const FINISHED: u8 = 5;
const FAILED: u8 = 6;
const UNSET: u64 = u64::MAX;
const AUDIBLE_EPSILON: f32 = 1.0e-6;

struct PlaybackSlot {
    state: AtomicU8,
    sequence: AtomicU64,
    accepted_ns: AtomicU64,
    synth_started_ns: AtomicU64,
    synth_finished_ns: AtomicU64,
    first_pcm_ns: AtomicU64,
    first_non_silent_ns: AtomicU64,
    pcm_end_ns: AtomicU64,
    start_sample: AtomicU64,
    end_sample: AtomicU64,
    source_frames: AtomicUsize,
    device_frames: AtomicUsize,
    first_callback_frames: AtomicUsize,
    first_callback_index: AtomicU64,
    last_callback_index: AtomicU64,
    underrun_callbacks: AtomicU64,
    underrun_frames: AtomicU64,
}

impl PlaybackSlot {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(VACANT),
            sequence: AtomicU64::new(UNSET),
            accepted_ns: AtomicU64::new(UNSET),
            synth_started_ns: AtomicU64::new(UNSET),
            synth_finished_ns: AtomicU64::new(UNSET),
            first_pcm_ns: AtomicU64::new(UNSET),
            first_non_silent_ns: AtomicU64::new(UNSET),
            pcm_end_ns: AtomicU64::new(UNSET),
            start_sample: AtomicU64::new(UNSET),
            end_sample: AtomicU64::new(UNSET),
            source_frames: AtomicUsize::new(0),
            device_frames: AtomicUsize::new(0),
            first_callback_frames: AtomicUsize::new(0),
            first_callback_index: AtomicU64::new(UNSET),
            last_callback_index: AtomicU64::new(UNSET),
            underrun_callbacks: AtomicU64::new(0),
            underrun_frames: AtomicU64::new(0),
        }
    }

    fn reset(&self, sequence: u64, accepted_ns: u64) -> Result<(), ()> {
        if !matches!(
            self.state.load(Ordering::Acquire),
            VACANT | FINISHED | FAILED
        ) {
            return Err(());
        }
        self.sequence.store(sequence, Ordering::Relaxed);
        self.accepted_ns.store(accepted_ns, Ordering::Relaxed);
        self.synth_started_ns.store(UNSET, Ordering::Relaxed);
        self.synth_finished_ns.store(UNSET, Ordering::Relaxed);
        self.first_pcm_ns.store(UNSET, Ordering::Relaxed);
        self.first_non_silent_ns.store(UNSET, Ordering::Relaxed);
        self.pcm_end_ns.store(UNSET, Ordering::Relaxed);
        self.start_sample.store(UNSET, Ordering::Relaxed);
        self.end_sample.store(UNSET, Ordering::Relaxed);
        self.source_frames.store(0, Ordering::Relaxed);
        self.device_frames.store(0, Ordering::Relaxed);
        self.first_callback_frames.store(0, Ordering::Relaxed);
        self.first_callback_index.store(UNSET, Ordering::Relaxed);
        self.last_callback_index.store(UNSET, Ordering::Relaxed);
        self.underrun_callbacks.store(0, Ordering::Relaxed);
        self.underrun_frames.store(0, Ordering::Relaxed);
        self.state.store(ACCEPTED, Ordering::Release);
        Ok(())
    }

    fn matches(&self, sequence: u64) -> bool {
        self.sequence.load(Ordering::Acquire) == sequence
    }

    fn observe_callback(&self, callback_index: u64, callback_frames: usize) {
        if self
            .first_callback_index
            .compare_exchange(UNSET, callback_index, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.first_callback_frames
                .store(callback_frames, Ordering::Release);
        }
        self.last_callback_index
            .store(callback_index, Ordering::Release);
    }
}

pub(crate) struct PlaybackTimeline {
    origin: Instant,
    slots: [PlaybackSlot; SENTENCE_PREFETCH_CAPACITY],
    callback_count: AtomicU64,
    finished_sentences: AtomicU64,
    underrun_callbacks: AtomicU64,
    underrun_frames: AtomicU64,
    stream_failed: AtomicBool,
    callback_contract_failed: AtomicBool,
    shutdown: AtomicBool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PlaybackObservation {
    pub(crate) accepted_ns: u64,
    pub(crate) synth_started_ns: u64,
    pub(crate) synth_finished_ns: u64,
    pub(crate) first_pcm_ns: u64,
    pub(crate) first_non_silent_ns: u64,
    pub(crate) pcm_end_ns: u64,
    pub(crate) source_frames: usize,
    pub(crate) device_frames: usize,
    pub(crate) first_callback_frames: usize,
    pub(crate) callback_count: u64,
    pub(crate) underrun_callbacks: u64,
    pub(crate) underrun_frames: u64,
}

impl PlaybackTimeline {
    pub(crate) fn new() -> Self {
        Self {
            origin: Instant::now(),
            slots: array::from_fn(|_| PlaybackSlot::new()),
            callback_count: AtomicU64::new(0),
            finished_sentences: AtomicU64::new(0),
            underrun_callbacks: AtomicU64::new(0),
            underrun_frames: AtomicU64::new(0),
            stream_failed: AtomicBool::new(false),
            callback_contract_failed: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
        }
    }

    pub(crate) fn timestamp(&self, instant: Instant) -> u64 {
        let now = Instant::now();
        let elapsed_at_now = self.origin.elapsed();
        let elapsed_since_instant = now.saturating_duration_since(instant);
        duration_ns(elapsed_at_now.saturating_sub(elapsed_since_instant))
    }

    pub(crate) fn accept(&self, sequence: u64, accepted_at: Instant) -> Result<(), ()> {
        self.slot(sequence)
            .reset(sequence, self.timestamp(accepted_at))
    }

    pub(crate) fn begin_synthesis(&self, sequence: u64) -> Result<(), ()> {
        let slot = self.checked_slot(sequence)?;
        slot.state
            .compare_exchange(ACCEPTED, SYNTHESIZING, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| ())?;
        slot.synth_started_ns
            .store(duration_ns(self.origin.elapsed()), Ordering::Release);
        Ok(())
    }

    pub(crate) fn finish_synthesis(&self, sequence: u64) -> Result<(), ()> {
        let slot = self.checked_slot(sequence)?;
        if slot.state.load(Ordering::Acquire) != SYNTHESIZING {
            return Err(());
        }
        slot.synth_finished_ns
            .store(duration_ns(self.origin.elapsed()), Ordering::Release);
        Ok(())
    }

    pub(crate) fn publish_pcm(
        &self,
        sequence: u64,
        start_sample: u64,
        source_frames: usize,
        device_frames: usize,
    ) -> Result<(), ()> {
        let slot = self.checked_slot(sequence)?;
        if slot.state.load(Ordering::Acquire) != SYNTHESIZING || device_frames == 0 {
            return Err(());
        }
        let device_frames_u64 = u64::try_from(device_frames).map_err(|_| ())?;
        let end_sample = start_sample.checked_add(device_frames_u64).ok_or(())?;
        slot.start_sample.store(start_sample, Ordering::Relaxed);
        slot.end_sample.store(end_sample, Ordering::Relaxed);
        slot.source_frames.store(source_frames, Ordering::Relaxed);
        slot.device_frames.store(device_frames, Ordering::Relaxed);
        slot.state.store(BUFFERED, Ordering::Release);
        Ok(())
    }

    pub(crate) fn mark_failed(&self, sequence: u64) {
        if let Ok(slot) = self.checked_slot(sequence) {
            slot.state.store(FAILED, Ordering::Release);
        }
    }

    pub(crate) fn is_finished(&self, sequence: u64) -> bool {
        self.checked_slot(sequence)
            .is_ok_and(|slot| slot.state.load(Ordering::Acquire) == FINISHED)
    }

    pub(crate) fn observation(&self, sequence: u64) -> Option<PlaybackObservation> {
        let slot = self.checked_slot(sequence).ok()?;
        if slot.state.load(Ordering::Acquire) != FINISHED {
            return None;
        }
        let first_callback = slot.first_callback_index.load(Ordering::Acquire);
        let last_callback = slot.last_callback_index.load(Ordering::Acquire);
        Some(PlaybackObservation {
            accepted_ns: slot.accepted_ns.load(Ordering::Acquire),
            synth_started_ns: slot.synth_started_ns.load(Ordering::Acquire),
            synth_finished_ns: slot.synth_finished_ns.load(Ordering::Acquire),
            first_pcm_ns: slot.first_pcm_ns.load(Ordering::Acquire),
            first_non_silent_ns: slot.first_non_silent_ns.load(Ordering::Acquire),
            pcm_end_ns: slot.pcm_end_ns.load(Ordering::Acquire),
            source_frames: slot.source_frames.load(Ordering::Acquire),
            device_frames: slot.device_frames.load(Ordering::Acquire),
            first_callback_frames: slot.first_callback_frames.load(Ordering::Acquire),
            callback_count: last_callback
                .saturating_sub(first_callback)
                .saturating_add(1),
            underrun_callbacks: slot.underrun_callbacks.load(Ordering::Acquire),
            underrun_frames: slot.underrun_frames.load(Ordering::Acquire),
        })
    }

    pub(crate) fn stream_failed(&self) -> bool {
        self.stream_failed.load(Ordering::Acquire)
    }

    pub(crate) fn mark_stream_failed(&self) {
        self.stream_failed.store(true, Ordering::Release);
    }

    pub(crate) fn callback_contract_failed(&self) -> bool {
        self.callback_contract_failed.load(Ordering::Acquire)
    }

    pub(crate) fn mark_shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    pub(crate) fn callback_count(&self) -> u64 {
        self.callback_count.load(Ordering::Acquire)
    }

    pub(crate) fn finished_sentences(&self) -> u64 {
        self.finished_sentences.load(Ordering::Acquire)
    }

    pub(crate) fn underrun_callbacks(&self) -> u64 {
        self.underrun_callbacks.load(Ordering::Acquire)
    }

    pub(crate) fn underrun_frames(&self) -> u64 {
        self.underrun_frames.load(Ordering::Acquire)
    }

    fn pending(&self, sequence: u64) -> Option<&PlaybackSlot> {
        let slot = self.checked_slot(sequence).ok()?;
        matches!(
            slot.state.load(Ordering::Acquire),
            ACCEPTED | SYNTHESIZING | BUFFERED | PLAYING
        )
        .then_some(slot)
    }

    fn slot(&self, sequence: u64) -> &PlaybackSlot {
        &self.slots[sequence as usize % SENTENCE_PREFETCH_CAPACITY]
    }

    fn checked_slot(&self, sequence: u64) -> Result<&PlaybackSlot, ()> {
        let slot = self.slot(sequence);
        slot.matches(sequence).then_some(slot).ok_or(())
    }
}

/// Callback-owned rtrb consumer and sample cursor.
pub(crate) struct CallbackDrain {
    consumer: Consumer<f32>,
    timeline: std::sync::Arc<PlaybackTimeline>,
    sample_rate: u32,
    next_sequence: u64,
    consumed_samples: u64,
}

impl CallbackDrain {
    pub(crate) fn new(
        consumer: Consumer<f32>,
        timeline: std::sync::Arc<PlaybackTimeline>,
        sample_rate: u32,
    ) -> Self {
        Self {
            consumer,
            timeline,
            sample_rate,
            next_sequence: 0,
            consumed_samples: 0,
        }
    }

    pub(crate) fn write_f32(&mut self, output: &mut [f32], channels: usize) {
        self.write(output, channels, 0.0, |sample| sample.clamp(-1.0, 1.0));
    }

    pub(crate) fn write_i16(&mut self, output: &mut [i16], channels: usize) {
        self.write(output, channels, 0, |sample| {
            (sample.clamp(-1.0, 1.0) * f32::from(i16::MAX)).round() as i16
        });
    }

    pub(crate) fn write_u16(&mut self, output: &mut [u16], channels: usize) {
        self.write(output, channels, u16::MAX / 2 + 1, |sample| {
            ((sample.clamp(-1.0, 1.0) * 0.5 + 0.5) * f32::from(u16::MAX)).round() as u16
        });
    }

    fn write<T: Copy>(
        &mut self,
        output: &mut [T],
        channels: usize,
        silence: T,
        convert: impl Fn(f32) -> T,
    ) {
        output.fill(silence);
        if channels == 0 || self.sample_rate == 0 {
            return;
        }
        let callback_frames = output.len() / channels;
        let callback_index = self.timeline.callback_count.fetch_add(1, Ordering::Relaxed);
        let callback_ns = duration_ns(self.timeline.origin.elapsed());
        let frame_ns = 1_000_000_000_u64 / u64::from(self.sample_rate);
        let mut underrun_sequences = [UNSET; SENTENCE_PREFETCH_CAPACITY];

        for (frame_index, frame) in output.chunks_exact_mut(channels).enumerate() {
            let frame_timestamp = callback_ns.saturating_add(
                u64::try_from(frame_index)
                    .unwrap_or(u64::MAX)
                    .saturating_mul(frame_ns),
            );
            match self.consumer.pop() {
                Ok(sample) => {
                    let Some(slot) = self.timeline.pending(self.next_sequence) else {
                        self.timeline
                            .callback_contract_failed
                            .store(true, Ordering::Release);
                        continue;
                    };
                    let start = slot.start_sample.load(Ordering::Acquire);
                    let end = slot.end_sample.load(Ordering::Acquire);
                    if start == UNSET
                        || end == UNSET
                        || self.consumed_samples < start
                        || self.consumed_samples >= end
                    {
                        self.timeline
                            .callback_contract_failed
                            .store(true, Ordering::Release);
                        continue;
                    }
                    slot.observe_callback(callback_index, callback_frames);
                    if self.consumed_samples == start {
                        slot.first_pcm_ns.store(frame_timestamp, Ordering::Release);
                        slot.state.store(PLAYING, Ordering::Release);
                    }
                    if sample.abs() > AUDIBLE_EPSILON {
                        let _ = slot.first_non_silent_ns.compare_exchange(
                            UNSET,
                            frame_timestamp,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        );
                    }
                    frame.fill(convert(sample));
                    self.consumed_samples = self.consumed_samples.saturating_add(1);
                    if self.consumed_samples == end {
                        slot.pcm_end_ns
                            .store(frame_timestamp.saturating_add(frame_ns), Ordering::Release);
                        slot.state.store(FINISHED, Ordering::Release);
                        self.timeline
                            .finished_sentences
                            .fetch_add(1, Ordering::Relaxed);
                        self.next_sequence = self.next_sequence.saturating_add(1);
                    }
                }
                Err(_) => {
                    if self.timeline.is_shutdown() {
                        continue;
                    }
                    if let Some(slot) = self.timeline.pending(self.next_sequence) {
                        slot.observe_callback(callback_index, callback_frames);
                        slot.underrun_frames.fetch_add(1, Ordering::Relaxed);
                        self.timeline
                            .underrun_frames
                            .fetch_add(1, Ordering::Relaxed);
                        if !underrun_sequences.contains(&self.next_sequence)
                            && let Some(entry) =
                                underrun_sequences.iter_mut().find(|entry| **entry == UNSET)
                        {
                            *entry = self.next_sequence;
                            slot.underrun_callbacks.fetch_add(1, Ordering::Relaxed);
                            self.timeline
                                .underrun_callbacks
                                .fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        }
    }
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX - 1)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rtrb::RingBuffer;

    use super::*;

    #[test]
    fn callback_concatenates_sentence_pcm_in_exact_order() {
        let timeline = Arc::new(PlaybackTimeline::new());
        let (mut producer, consumer) = RingBuffer::new(8);
        let now = Instant::now();
        timeline.accept(0, now).unwrap();
        timeline.begin_synthesis(0).unwrap();
        timeline.finish_synthesis(0).unwrap();
        timeline.publish_pcm(0, 0, 2, 2).unwrap();
        timeline.accept(1, now).unwrap();
        timeline.begin_synthesis(1).unwrap();
        timeline.finish_synthesis(1).unwrap();
        timeline.publish_pcm(1, 2, 3, 3).unwrap();
        producer
            .push_entire_slice(&[0.25, -0.5, 0.75, 1.0, -1.0])
            .unwrap();

        let mut callback = CallbackDrain::new(consumer, Arc::clone(&timeline), 48_000);
        let mut output = [9.0_f32; 12];
        callback.write_f32(&mut output, 2);
        assert_eq!(
            output,
            [
                0.25, 0.25, -0.5, -0.5, 0.75, 0.75, 1.0, 1.0, -1.0, -1.0, 0.0, 0.0
            ]
        );
        assert!(timeline.is_finished(0));
        assert!(timeline.is_finished(1));
        assert_eq!(timeline.finished_sentences(), 2);
        assert_eq!(timeline.observation(0).unwrap().device_frames, 2);
        assert_eq!(timeline.observation(1).unwrap().device_frames, 3);
        assert!(!timeline.callback_contract_failed());
    }

    #[test]
    fn callback_emits_typed_underrun_silence_without_advancing_pcm() {
        let timeline = Arc::new(PlaybackTimeline::new());
        let (_producer, consumer) = RingBuffer::new(4);
        timeline.accept(0, Instant::now()).unwrap();
        let mut callback = CallbackDrain::new(consumer, Arc::clone(&timeline), 48_000);
        let mut output = [1.0_f32; 6];
        callback.write_f32(&mut output, 2);
        assert_eq!(output, [0.0; 6]);
        let slot = timeline.checked_slot(0).unwrap();
        assert_eq!(slot.underrun_callbacks.load(Ordering::Acquire), 1);
        assert_eq!(slot.underrun_frames.load(Ordering::Acquire), 3);
        assert_eq!(callback.consumed_samples, 0);
    }

    #[test]
    fn callback_converts_f32_ring_samples_for_integer_devices() {
        let timeline = Arc::new(PlaybackTimeline::new());
        let (mut producer, consumer) = RingBuffer::new(3);
        timeline.accept(0, Instant::now()).unwrap();
        timeline.begin_synthesis(0).unwrap();
        timeline.finish_synthesis(0).unwrap();
        timeline.publish_pcm(0, 0, 3, 3).unwrap();
        producer.push_entire_slice(&[-1.0, 0.0, 1.0]).unwrap();
        let mut callback = CallbackDrain::new(consumer, Arc::clone(&timeline), 24_000);
        let mut signed = [0_i16; 3];
        callback.write_i16(&mut signed, 1);
        assert_eq!(signed, [-32_767, 0, 32_767]);

        let timeline = Arc::new(PlaybackTimeline::new());
        let (mut producer, consumer) = RingBuffer::new(3);
        timeline.accept(0, Instant::now()).unwrap();
        timeline.begin_synthesis(0).unwrap();
        timeline.finish_synthesis(0).unwrap();
        timeline.publish_pcm(0, 0, 3, 3).unwrap();
        producer.push_entire_slice(&[-1.0, 0.0, 1.0]).unwrap();
        let mut callback = CallbackDrain::new(consumer, timeline, 24_000);
        let mut unsigned = [0_u16; 3];
        callback.write_u16(&mut unsigned, 1);
        assert_eq!(unsigned, [0, 32_768, 65_535]);
    }

    #[test]
    fn callback_failure_and_shutdown_are_atomic_outcomes() {
        let timeline = PlaybackTimeline::new();
        assert!(!timeline.stream_failed());
        timeline.mark_stream_failed();
        assert!(timeline.stream_failed());
        assert!(!timeline.is_shutdown());
        timeline.mark_shutdown();
        assert!(timeline.is_shutdown());
    }
}
