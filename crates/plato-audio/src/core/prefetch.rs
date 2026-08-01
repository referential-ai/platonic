use std::collections::VecDeque;

use thiserror::Error;

/// Fixed count of accepted sentence jobs that may remain unfinished.
pub const SENTENCE_PREFETCH_CAPACITY: usize = 4;

/// Typed admission outcomes for the fixed sentence window.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum SentenceQueueError {
    /// Four accepted jobs already remain unfinished.
    #[error("sentence prefetch window is full at its fixed capacity of {capacity}")]
    Full {
        /// Compile-time sentence-window capacity.
        capacity: usize,
    },
    /// The owner closed admission for shutdown.
    #[error("sentence prefetch window is closed")]
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SentenceJobStage {
    Accepted,
    Synthesizing,
    Buffered { device_frames: usize },
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SentenceJob {
    sequence: u64,
    stage: SentenceJobStage,
}

/// Pure state for the four accepted-but-not-finished sentence slots.
#[derive(Debug)]
pub(crate) struct PrefetchWindow {
    jobs: VecDeque<SentenceJob>,
    next_sequence: u64,
    closed: bool,
}

impl PrefetchWindow {
    pub(crate) fn new() -> Self {
        Self {
            jobs: VecDeque::with_capacity(SENTENCE_PREFETCH_CAPACITY),
            next_sequence: 0,
            closed: false,
        }
    }

    pub(crate) fn try_accept(&mut self) -> Result<u64, SentenceQueueError> {
        if self.closed {
            return Err(SentenceQueueError::Closed);
        }
        if self.jobs.len() == SENTENCE_PREFETCH_CAPACITY {
            return Err(SentenceQueueError::Full {
                capacity: SENTENCE_PREFETCH_CAPACITY,
            });
        }
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.jobs.push_back(SentenceJob {
            sequence,
            stage: SentenceJobStage::Accepted,
        });
        Ok(sequence)
    }

    pub(crate) fn next_accepted(&mut self) -> Option<u64> {
        let job = self
            .jobs
            .iter_mut()
            .find(|job| job.stage == SentenceJobStage::Accepted)?;
        job.stage = SentenceJobStage::Synthesizing;
        Some(job.sequence)
    }

    pub(crate) fn mark_buffered(
        &mut self,
        sequence: u64,
        device_frames: usize,
    ) -> Result<(), &'static str> {
        let job = self.job_mut(sequence)?;
        if job.stage != SentenceJobStage::Synthesizing {
            return Err("only a synthesizing sentence can become buffered");
        }
        job.stage = SentenceJobStage::Buffered { device_frames };
        Ok(())
    }

    pub(crate) fn finish_front(&mut self, sequence: u64) -> Result<(), &'static str> {
        let Some(job) = self.jobs.front() else {
            return Err("cannot finish a sentence from an empty window");
        };
        if job.sequence != sequence {
            return Err("sentences must finish in accepted order");
        }
        if !matches!(job.stage, SentenceJobStage::Buffered { .. }) {
            return Err("only a buffered sentence can finish playback");
        }
        self.jobs.pop_front();
        Ok(())
    }

    pub(crate) fn fail(&mut self, sequence: u64) -> Result<(), &'static str> {
        let job = self.job_mut(sequence)?;
        if job.stage == SentenceJobStage::Failed {
            return Err("sentence already failed");
        }
        job.stage = SentenceJobStage::Failed;
        self.closed = true;
        Ok(())
    }

    pub(crate) fn close(&mut self) {
        self.closed = true;
    }

    pub(crate) fn interrupt(&mut self) -> Vec<u64> {
        self.jobs.drain(..).map(|job| job.sequence).collect()
    }

    pub(crate) fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    pub(crate) fn len(&self) -> usize {
        self.jobs.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed
    }

    pub(crate) fn front(&self) -> Option<(u64, SentenceJobStage)> {
        self.jobs.front().map(|job| (job.sequence, job.stage))
    }

    fn job_mut(&mut self, sequence: u64) -> Result<&mut SentenceJob, &'static str> {
        self.jobs
            .iter_mut()
            .find(|job| job.sequence == sequence)
            .ok_or("sentence sequence is not in the accepted window")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_window_counts_synthesizing_and_buffered_jobs_until_playback_finishes() {
        let mut window = PrefetchWindow::new();
        let sequences = (0..SENTENCE_PREFETCH_CAPACITY)
            .map(|_| window.try_accept().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(sequences, [0, 1, 2, 3]);
        assert_eq!(window.next_accepted(), Some(0));
        window.mark_buffered(0, 480).unwrap();
        assert_eq!(window.len(), SENTENCE_PREFETCH_CAPACITY);
        assert_eq!(
            window.try_accept(),
            Err(SentenceQueueError::Full {
                capacity: SENTENCE_PREFETCH_CAPACITY
            })
        );

        window.finish_front(0).unwrap();
        assert_eq!(window.try_accept().unwrap(), 4);
        assert_eq!(window.len(), SENTENCE_PREFETCH_CAPACITY);
    }

    #[test]
    fn drain_and_close_transitions_are_ordered_and_deterministic() {
        let mut window = PrefetchWindow::new();
        let first = window.try_accept().unwrap();
        let second = window.try_accept().unwrap();
        assert_eq!(window.next_accepted(), Some(first));
        window.mark_buffered(first, 12).unwrap();
        assert!(window.finish_front(second).is_err());
        window.finish_front(first).unwrap();
        assert_eq!(window.next_accepted(), Some(second));
        window.mark_buffered(second, 8).unwrap();
        window.close();
        assert_eq!(window.try_accept(), Err(SentenceQueueError::Closed));
        window.finish_front(second).unwrap();
        assert!(window.is_empty());
        assert!(window.is_closed());
    }

    #[test]
    fn failure_closes_admission_without_erasing_unfinished_work() {
        let mut window = PrefetchWindow::new();
        let first = window.try_accept().unwrap();
        window.try_accept().unwrap();
        assert_eq!(window.next_accepted(), Some(first));
        window.fail(first).unwrap();
        assert_eq!(window.front(), Some((first, SentenceJobStage::Failed)));
        assert_eq!(window.len(), 2);
        assert_eq!(window.try_accept(), Err(SentenceQueueError::Closed));
    }

    #[test]
    fn interruption_discards_every_stage_without_reusing_sequences() {
        let mut window = PrefetchWindow::new();
        let first = window.try_accept().unwrap();
        let second = window.try_accept().unwrap();
        assert_eq!(window.next_accepted(), Some(first));
        assert_eq!(window.interrupt(), [first, second]);
        assert!(window.is_empty());
        assert_eq!(window.next_sequence(), 2);
        assert_eq!(window.try_accept().unwrap(), 2);
    }
}
