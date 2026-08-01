use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use serde::Serialize;

const UNSET: u64 = u64::MAX;

/// Fixed self-playback interval ignored before speech may trigger barge-in.
pub const SELF_PLAYBACK_GATE_MS: u64 = 150;

/// Plain sentence position carried from assistant text into spoken PCM.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct SpeechSource {
    /// Zero-based sentence position in the narrated run.
    pub sentence_index: u64,
    /// Source assistant-delta index that completed this sentence.
    pub assistant_delta_index: u64,
}

impl SpeechSource {
    /// Constructs one literal text-to-audio source position.
    pub fn new(sentence_index: u64, assistant_delta_index: u64) -> Self {
        Self {
            sentence_index,
            assistant_delta_index,
        }
    }
}

/// Sample-derived record of the assistant audio emitted before barge-in silence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SpokenInterruption {
    /// Device-rate mono frames emitted during this run before silence.
    pub played_samples: u64,
    /// Sentence containing the final emitted sample.
    pub sentence_index: u64,
    /// Assistant delta that completed that sentence.
    pub assistant_delta_index: u64,
    /// Whitespace-normalized words whose proportional PCM buckets completed.
    pub spoken_prefix: String,
}

/// Atomic timing and queue observation for one active playback run.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct BargeInMetrics {
    /// Fixed self-playback gate excluded from stop latency.
    pub self_playback_gate_ms: u64,
    /// Gate length at the actual output sample rate.
    pub self_playback_gate_frames: u64,
    /// Whether the gate was open at the admitted Silero decision.
    pub gate_open_at_decision: bool,
    /// Device frames emitted before the Silero decision.
    pub played_frames_at_decision: u64,
    /// PCM ring depth observed at the Silero decision.
    pub queued_pcm_frames_at_decision: usize,
    /// Accepted sentence depth observed at the Silero decision.
    pub queued_sentences_at_decision: usize,
    /// Silero speech-onset decision timestamp relative to handle construction.
    pub speech_onset_decision_ns: Option<u64>,
    /// First callback entry that emitted an entirely silent quantum.
    pub first_silent_callback_ns: Option<u64>,
    /// Silero decision through first all-silent callback entry.
    pub decision_to_silence_us: Option<u64>,
    /// Frames in that first all-silent output callback.
    pub silent_callback_frames: Option<usize>,
    /// Idempotent non-real-time sentence-window flush count.
    pub sentence_queue_flushes: u64,
    /// Sentences discarded by those flushes.
    pub discarded_sentences: u64,
    /// Idempotent non-real-time PCM-ring replacement count.
    pub pcm_queue_flushes: u64,
    /// Device frames discarded by those replacements.
    pub discarded_pcm_frames: u64,
}

struct BargeInState {
    origin: Instant,
    cancel: Arc<AtomicBool>,
    active: AtomicBool,
    generation: AtomicU64,
    output_sample_rate: AtomicU32,
    playback_started_ns: AtomicU64,
    played_frames: AtomicU64,
    queued_pcm_frames: AtomicUsize,
    queued_sentences: AtomicUsize,
    speech_onset_decision_ns: AtomicU64,
    first_silent_callback_ns: AtomicU64,
    played_frames_at_decision: AtomicU64,
    queued_pcm_frames_at_decision: AtomicUsize,
    queued_sentences_at_decision: AtomicUsize,
    silent_callback_frames: AtomicUsize,
    sentence_queue_flushes: AtomicU64,
    discarded_sentences: AtomicU64,
    pcm_queue_flushes: AtomicU64,
    discarded_pcm_frames: AtomicU64,
}

/// Cloneable observation handle around the one caller-owned cancel atomic.
#[derive(Clone)]
pub struct BargeInHandle {
    state: Arc<BargeInState>,
}

impl BargeInHandle {
    /// Binds playback and capture observation to exactly one cancel authority.
    pub fn new(cancel: Arc<AtomicBool>) -> Self {
        Self {
            state: Arc::new(BargeInState {
                origin: Instant::now(),
                cancel,
                active: AtomicBool::new(false),
                generation: AtomicU64::new(0),
                output_sample_rate: AtomicU32::new(0),
                playback_started_ns: AtomicU64::new(UNSET),
                played_frames: AtomicU64::new(0),
                queued_pcm_frames: AtomicUsize::new(0),
                queued_sentences: AtomicUsize::new(0),
                speech_onset_decision_ns: AtomicU64::new(UNSET),
                first_silent_callback_ns: AtomicU64::new(UNSET),
                played_frames_at_decision: AtomicU64::new(0),
                queued_pcm_frames_at_decision: AtomicUsize::new(0),
                queued_sentences_at_decision: AtomicUsize::new(0),
                silent_callback_frames: AtomicUsize::new(0),
                sentence_queue_flushes: AtomicU64::new(0),
                discarded_sentences: AtomicU64::new(0),
                pcm_queue_flushes: AtomicU64::new(0),
                discarded_pcm_frames: AtomicU64::new(0),
            }),
        }
    }

    /// Returns a snapshot after a run or during actual-device proof.
    pub fn metrics(&self) -> BargeInMetrics {
        let decision = optional_ns(self.state.speech_onset_decision_ns.load(Ordering::Acquire));
        let silent = optional_ns(self.state.first_silent_callback_ns.load(Ordering::Acquire));
        BargeInMetrics {
            self_playback_gate_ms: SELF_PLAYBACK_GATE_MS,
            self_playback_gate_frames: self.gate_frames(),
            gate_open_at_decision: decision.is_some()
                && self.state.played_frames_at_decision.load(Ordering::Acquire)
                    >= self.gate_frames(),
            played_frames_at_decision: self.state.played_frames_at_decision.load(Ordering::Acquire),
            queued_pcm_frames_at_decision: self
                .state
                .queued_pcm_frames_at_decision
                .load(Ordering::Acquire),
            queued_sentences_at_decision: self
                .state
                .queued_sentences_at_decision
                .load(Ordering::Acquire),
            speech_onset_decision_ns: decision,
            first_silent_callback_ns: silent,
            decision_to_silence_us: decision
                .zip(silent)
                .map(|(decision, silent)| silent.saturating_sub(decision) / 1_000),
            silent_callback_frames: silent
                .map(|_| self.state.silent_callback_frames.load(Ordering::Acquire)),
            sentence_queue_flushes: self.state.sentence_queue_flushes.load(Ordering::Acquire),
            discarded_sentences: self.state.discarded_sentences.load(Ordering::Acquire),
            pcm_queue_flushes: self.state.pcm_queue_flushes.load(Ordering::Acquire),
            discarded_pcm_frames: self.state.discarded_pcm_frames.load(Ordering::Acquire),
        }
    }

    pub(crate) fn uses_cancel(&self, cancel: &Arc<AtomicBool>) -> bool {
        Arc::ptr_eq(&self.state.cancel, cancel)
    }

    pub(crate) fn configure_output(&self, sample_rate: u32) -> Result<(), ()> {
        match self.state.output_sample_rate.compare_exchange(
            0,
            sample_rate,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(()),
            Err(current) if current == sample_rate => Ok(()),
            Err(_) => Err(()),
        }
    }

    pub(crate) fn begin_run(&self) -> u64 {
        self.state.active.store(false, Ordering::Release);
        self.state
            .playback_started_ns
            .store(UNSET, Ordering::Relaxed);
        self.state.played_frames.store(0, Ordering::Relaxed);
        self.state.queued_pcm_frames.store(0, Ordering::Relaxed);
        self.state.queued_sentences.store(0, Ordering::Relaxed);
        self.state
            .speech_onset_decision_ns
            .store(UNSET, Ordering::Relaxed);
        self.state
            .first_silent_callback_ns
            .store(UNSET, Ordering::Relaxed);
        self.state
            .played_frames_at_decision
            .store(0, Ordering::Relaxed);
        self.state
            .queued_pcm_frames_at_decision
            .store(0, Ordering::Relaxed);
        self.state
            .queued_sentences_at_decision
            .store(0, Ordering::Relaxed);
        self.state
            .silent_callback_frames
            .store(0, Ordering::Relaxed);
        self.state
            .sentence_queue_flushes
            .store(0, Ordering::Relaxed);
        self.state.discarded_sentences.store(0, Ordering::Relaxed);
        self.state.pcm_queue_flushes.store(0, Ordering::Relaxed);
        self.state.discarded_pcm_frames.store(0, Ordering::Relaxed);
        let generation = self.state.generation.fetch_add(1, Ordering::AcqRel) + 1;
        self.state.active.store(true, Ordering::Release);
        generation
    }

    pub(crate) fn finish_run(&self) {
        self.state.active.store(false, Ordering::Release);
        self.state.cancel.store(false, Ordering::Release);
    }

    pub(crate) fn generation(&self) -> u64 {
        self.state.generation.load(Ordering::Acquire)
    }

    pub(crate) fn is_active(&self) -> bool {
        self.state.active.load(Ordering::Acquire)
    }

    pub(crate) fn cancel_requested(&self) -> bool {
        self.state.cancel.load(Ordering::Acquire)
    }

    pub(crate) fn cancel_for_failure(&self) {
        self.state.cancel.store(true, Ordering::Release);
    }

    pub(crate) fn playback_started(&self) -> bool {
        self.state.playback_started_ns.load(Ordering::Acquire) != UNSET
    }

    pub(crate) fn playback_active(&self) -> bool {
        self.playback_started() && self.state.queued_pcm_frames.load(Ordering::Acquire) > 0
    }

    /// Returns whether active output has crossed the fixed self-playback gate.
    pub fn gate_open(&self) -> bool {
        self.is_active()
            && self.playback_started()
            && self.state.played_frames.load(Ordering::Acquire) >= self.gate_frames()
    }

    /// Records one minimum-speech-qualified VAD decision and sets the shared cancel flag.
    pub fn trigger_speech_onset(&self) -> bool {
        if !self.gate_open() || !self.playback_active() {
            return false;
        }
        let decision_ns = self.timestamp();
        if self
            .state
            .speech_onset_decision_ns
            .compare_exchange(UNSET, decision_ns, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        self.state.played_frames_at_decision.store(
            self.state.played_frames.load(Ordering::Acquire),
            Ordering::Release,
        );
        self.state.queued_pcm_frames_at_decision.store(
            self.state.queued_pcm_frames.load(Ordering::Acquire),
            Ordering::Release,
        );
        self.state.queued_sentences_at_decision.store(
            self.state.queued_sentences.load(Ordering::Acquire),
            Ordering::Release,
        );
        self.state.cancel.store(true, Ordering::Release);
        true
    }

    pub(crate) fn record_playback_started(&self) {
        let _ = self.state.playback_started_ns.compare_exchange(
            UNSET,
            self.timestamp(),
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    pub(crate) fn record_played_frames(&self, frames: usize) {
        if frames == 0 {
            return;
        }
        self.state
            .played_frames
            .fetch_add(u64::try_from(frames).unwrap_or(u64::MAX), Ordering::AcqRel);
        subtract_saturating(&self.state.queued_pcm_frames, frames);
    }

    pub(crate) fn record_queued_frames(&self, frames: usize) {
        self.state
            .queued_pcm_frames
            .fetch_add(frames, Ordering::AcqRel);
    }

    pub(crate) fn set_queued_sentences(&self, sentences: usize) {
        self.state
            .queued_sentences
            .store(sentences, Ordering::Release);
    }

    pub(crate) fn record_silent_callback(&self, callback_frames: usize) {
        if self
            .state
            .first_silent_callback_ns
            .compare_exchange(UNSET, self.timestamp(), Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.state
                .silent_callback_frames
                .store(callback_frames, Ordering::Release);
        }
    }

    pub(crate) fn silent_callback_observed(&self) -> bool {
        self.state.first_silent_callback_ns.load(Ordering::Acquire) != UNSET
    }

    pub(crate) fn flush_sentence_queue(&self, discarded: usize) {
        self.state
            .sentence_queue_flushes
            .fetch_add(1, Ordering::Relaxed);
        self.state.discarded_sentences.fetch_add(
            u64::try_from(discarded).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.set_queued_sentences(0);
    }

    pub(crate) fn flush_pcm_queue(&self) -> usize {
        let discarded = self.state.queued_pcm_frames.swap(0, Ordering::AcqRel);
        self.state.pcm_queue_flushes.fetch_add(1, Ordering::Relaxed);
        self.state.discarded_pcm_frames.fetch_add(
            u64::try_from(discarded).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        discarded
    }

    fn gate_frames(&self) -> u64 {
        u64::from(self.state.output_sample_rate.load(Ordering::Acquire))
            .saturating_mul(SELF_PLAYBACK_GATE_MS)
            / 1_000
    }

    fn timestamp(&self) -> u64 {
        duration_ns(self.state.origin.elapsed())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SentenceSpan {
    source: SpeechSource,
    text: String,
    start_sample: u64,
    end_sample: u64,
}

#[derive(Debug, Default)]
pub(crate) struct InterruptionLatch {
    run_start_sample: u64,
    spans: Vec<SentenceSpan>,
    interruption: Option<SpokenInterruption>,
}

impl InterruptionLatch {
    pub(crate) fn begin(&mut self, run_start_sample: u64) {
        self.run_start_sample = run_start_sample;
        self.spans.clear();
        self.interruption = None;
    }

    pub(crate) fn record_sentence(
        &mut self,
        source: SpeechSource,
        text: &str,
        start_sample: u64,
        end_sample: u64,
    ) -> Result<(), ()> {
        let text = normalize_whitespace(text);
        if text.is_empty()
            || start_sample >= end_sample
            || start_sample < self.run_start_sample
            || self
                .spans
                .last()
                .is_some_and(|span| start_sample < span.end_sample)
        {
            return Err(());
        }
        self.spans.push(SentenceSpan {
            source,
            text,
            start_sample,
            end_sample,
        });
        Ok(())
    }

    pub(crate) fn interrupt(&mut self, played_sample: u64) {
        if self.interruption.is_some() || played_sample <= self.run_start_sample {
            return;
        }
        let mut words = Vec::new();
        let mut final_source = None;
        for span in &self.spans {
            if played_sample <= span.start_sample {
                break;
            }
            final_source = Some(span.source);
            let span_frames = span.end_sample.saturating_sub(span.start_sample);
            let emitted_frames = played_sample
                .min(span.end_sample)
                .saturating_sub(span.start_sample);
            let span_words = span.text.split_whitespace().collect::<Vec<_>>();
            for (index, word) in span_words.iter().enumerate() {
                let word_end = proportional_end(
                    span.start_sample,
                    span_frames,
                    index.saturating_add(1),
                    span_words.len(),
                );
                if emitted_frames == span_frames || played_sample >= word_end {
                    words.push(*word);
                } else {
                    break;
                }
            }
            if played_sample < span.end_sample {
                break;
            }
        }
        let Some(source) = final_source else {
            return;
        };
        self.interruption = Some(SpokenInterruption {
            played_samples: played_sample.saturating_sub(self.run_start_sample),
            sentence_index: source.sentence_index,
            assistant_delta_index: source.assistant_delta_index,
            spoken_prefix: words.join(" "),
        });
    }

    pub(crate) fn take(&mut self) -> Option<SpokenInterruption> {
        self.interruption.take()
    }
}

fn normalize_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn proportional_end(start: u64, frames: u64, completed: usize, total: usize) -> u64 {
    let completed = u64::try_from(completed).unwrap_or(u64::MAX);
    let total = u64::try_from(total).unwrap_or(u64::MAX).max(1);
    let numerator = frames.saturating_mul(completed);
    start.saturating_add(numerator.saturating_add(total - 1) / total)
}

fn subtract_saturating(value: &AtomicUsize, amount: usize) {
    let _ = value.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
        Some(current.saturating_sub(amount))
    });
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX - 1)
}

fn optional_ns(value: u64) -> Option<u64> {
    (value != UNSET).then_some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latch_uses_emitted_samples_for_one_normalized_prefix_and_position() {
        let mut latch = InterruptionLatch::default();
        latch.begin(1_000);
        latch
            .record_sentence(
                SpeechSource::new(0, 3),
                "  one   two three four  ",
                1_000,
                1_400,
            )
            .unwrap();
        latch
            .record_sentence(SpeechSource::new(1, 7), "five six", 1_400, 1_600)
            .unwrap();

        latch.interrupt(1_450);
        assert_eq!(
            latch.take(),
            Some(SpokenInterruption {
                played_samples: 450,
                sentence_index: 1,
                assistant_delta_index: 7,
                spoken_prefix: "one two three four".to_owned(),
            })
        );
        assert_eq!(latch.take(), None);
    }

    #[test]
    fn repeated_interrupt_is_idempotent_and_never_claims_an_unplayed_word() {
        let mut latch = InterruptionLatch::default();
        latch.begin(0);
        latch
            .record_sentence(SpeechSource::new(2, 4), "first second", 0, 100)
            .unwrap();
        latch.interrupt(49);
        latch.interrupt(100);
        assert_eq!(
            latch.take().unwrap(),
            SpokenInterruption {
                played_samples: 49,
                sentence_index: 2,
                assistant_delta_index: 4,
                spoken_prefix: String::new(),
            }
        );
    }

    #[test]
    fn barge_in_sets_the_exact_cancel_atomic_only_after_the_gate() {
        let cancel = Arc::new(AtomicBool::new(false));
        let handle = BargeInHandle::new(Arc::clone(&cancel));
        handle.configure_output(48_000).unwrap();
        handle.begin_run();
        handle.record_playback_started();
        handle.record_played_frames(7_199);
        assert!(!handle.trigger_speech_onset());
        assert!(!cancel.load(Ordering::Acquire));

        handle.record_played_frames(1);
        handle.record_queued_frames(512);
        handle.set_queued_sentences(3);
        assert!(handle.trigger_speech_onset());
        assert!(cancel.load(Ordering::Acquire));
        assert!(!handle.trigger_speech_onset());
        let metrics = handle.metrics();
        assert!(metrics.gate_open_at_decision);
        assert_eq!(metrics.played_frames_at_decision, 7_200);
        assert_eq!(metrics.queued_pcm_frames_at_decision, 512);
        assert_eq!(metrics.queued_sentences_at_decision, 3);
    }

    #[test]
    fn begin_preserves_a_caller_pre_cancel_and_finish_clears_only_stale_run_state() {
        let cancel = Arc::new(AtomicBool::new(true));
        let handle = BargeInHandle::new(Arc::clone(&cancel));
        handle.configure_output(48_000).unwrap();

        handle.begin_run();
        assert!(cancel.load(Ordering::Acquire));
        assert!(handle.cancel_requested());
        assert!(handle.metrics().speech_onset_decision_ns.is_none());

        handle.finish_run();
        assert!(!cancel.load(Ordering::Acquire));
    }
}
