use std::{
    sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering},
    time::{Duration, Instant},
};

use crate::DeviceError;

const IDLE: u8 = 0;
const WRITING: u8 = 1;
const READY: u8 = 2;
const PLAYING: u8 = 3;
const UNSET_TIMESTAMP: u64 = u64::MAX;
const AUDIBLE_EPSILON: f32 = 1.0e-6;

/// Preallocated single-producer/single-callback storage for one mono sentence.
///
/// The device callback performs only bounded atomic reads and writes. It never
/// allocates, waits on a lock, or calls into the synthesizer.
pub(crate) struct CallbackBuffer {
    origin: Instant,
    samples: Box<[AtomicU32]>,
    state: AtomicU8,
    length: AtomicUsize,
    cursor: AtomicUsize,
    accepted_ns: AtomicU64,
    first_non_silent_ns: AtomicU64,
    first_callback_frames: AtomicUsize,
    callback_count: AtomicU64,
    stream_failed: AtomicBool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CallbackObservation {
    pub(crate) accepted_ns: u64,
    pub(crate) first_non_silent_ns: Option<u64>,
    pub(crate) first_callback_frames: Option<usize>,
    pub(crate) callback_count: u64,
}

impl CallbackBuffer {
    pub(crate) fn new(capacity_frames: usize) -> Self {
        let samples = (0..capacity_frames)
            .map(|_| AtomicU32::new(0.0_f32.to_bits()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            origin: Instant::now(),
            samples,
            state: AtomicU8::new(IDLE),
            length: AtomicUsize::new(0),
            cursor: AtomicUsize::new(0),
            accepted_ns: AtomicU64::new(0),
            first_non_silent_ns: AtomicU64::new(UNSET_TIMESTAMP),
            first_callback_frames: AtomicUsize::new(0),
            callback_count: AtomicU64::new(0),
            stream_failed: AtomicBool::new(false),
        }
    }

    pub(crate) fn capacity(&self) -> usize {
        self.samples.len()
    }

    pub(crate) fn timestamp(&self, instant: Instant) -> u64 {
        let now = Instant::now();
        let elapsed_at_now = self.origin.elapsed();
        let elapsed_since_instant = now.saturating_duration_since(instant);
        duration_ns(elapsed_at_now.saturating_sub(elapsed_since_instant))
    }

    pub(crate) fn load(&self, samples: &[f32], accepted_ns: u64) -> Result<(), DeviceError> {
        if samples.len() > self.capacity() {
            return Err(DeviceError::ChunkTooLarge {
                frames: samples.len(),
                capacity: self.capacity(),
            });
        }
        self.state
            .compare_exchange(IDLE, WRITING, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| DeviceError::PlaybackBusy)?;

        for (slot, sample) in self.samples.iter().zip(samples) {
            slot.store(sample.to_bits(), Ordering::Relaxed);
        }
        self.length.store(samples.len(), Ordering::Relaxed);
        self.cursor.store(0, Ordering::Relaxed);
        self.accepted_ns.store(accepted_ns, Ordering::Relaxed);
        self.first_non_silent_ns
            .store(UNSET_TIMESTAMP, Ordering::Relaxed);
        self.first_callback_frames.store(0, Ordering::Relaxed);
        self.callback_count.store(0, Ordering::Relaxed);
        self.state.store(READY, Ordering::Release);
        Ok(())
    }

    pub(crate) fn is_idle(&self) -> bool {
        self.state.load(Ordering::Acquire) == IDLE
    }

    pub(crate) fn stream_failed(&self) -> bool {
        self.stream_failed.load(Ordering::Acquire)
    }

    pub(crate) fn mark_stream_failed(&self) {
        self.stream_failed.store(true, Ordering::Release);
    }

    pub(crate) fn observation(&self) -> CallbackObservation {
        let first_non_silent_ns = self.first_non_silent_ns.load(Ordering::Acquire);
        let first_callback_frames = self.first_callback_frames.load(Ordering::Acquire);
        CallbackObservation {
            accepted_ns: self.accepted_ns.load(Ordering::Acquire),
            first_non_silent_ns: (first_non_silent_ns != UNSET_TIMESTAMP)
                .then_some(first_non_silent_ns),
            first_callback_frames: (first_callback_frames != 0).then_some(first_callback_frames),
            callback_count: self.callback_count.load(Ordering::Acquire),
        }
    }

    pub(crate) fn write_f32(&self, output: &mut [f32], channels: usize) {
        self.write(output, channels, 0.0, |sample| sample.clamp(-1.0, 1.0));
    }

    pub(crate) fn write_i16(&self, output: &mut [i16], channels: usize) {
        self.write(output, channels, 0, |sample| {
            (sample.clamp(-1.0, 1.0) * f32::from(i16::MAX)).round() as i16
        });
    }

    pub(crate) fn write_u16(&self, output: &mut [u16], channels: usize) {
        self.write(output, channels, u16::MAX / 2 + 1, |sample| {
            ((sample.clamp(-1.0, 1.0) * 0.5 + 0.5) * f32::from(u16::MAX)).round() as u16
        });
    }

    fn write<T: Copy>(
        &self,
        output: &mut [T],
        channels: usize,
        silence: T,
        convert: impl Fn(f32) -> T,
    ) {
        output.fill(silence);
        if channels == 0 {
            return;
        }
        self.callback_count.fetch_add(1, Ordering::Relaxed);

        let state = self.state.load(Ordering::Acquire);
        if state == READY {
            let _ =
                self.state
                    .compare_exchange(READY, PLAYING, Ordering::AcqRel, Ordering::Acquire);
        }
        if self.state.load(Ordering::Acquire) != PLAYING {
            return;
        }

        let length = self.length.load(Ordering::Relaxed);
        let mut cursor = self.cursor.load(Ordering::Relaxed);
        let callback_frames = output.len() / channels;
        for frame in output.chunks_exact_mut(channels) {
            if cursor >= length {
                break;
            }
            let sample = f32::from_bits(self.samples[cursor].load(Ordering::Relaxed));
            let converted = convert(sample);
            frame.fill(converted);
            if sample.abs() > AUDIBLE_EPSILON
                && self
                    .first_non_silent_ns
                    .compare_exchange(
                        UNSET_TIMESTAMP,
                        duration_ns(self.origin.elapsed()),
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
            {
                self.first_callback_frames
                    .store(callback_frames, Ordering::Release);
            }
            cursor += 1;
        }
        self.cursor.store(cursor, Ordering::Relaxed);
        if cursor >= length {
            self.state.store(IDLE, Ordering::Release);
        }
    }
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_callback_writes_silence_without_changing_shape() {
        let buffer = CallbackBuffer::new(8);
        let mut output = [9.0_f32; 6];
        buffer.write_f32(&mut output, 2);
        assert_eq!(output, [0.0; 6]);
        assert!(buffer.is_idle());
    }

    #[test]
    fn callback_upmixes_mono_and_drains_serially() {
        let buffer = CallbackBuffer::new(4);
        buffer.load(&[0.25, -0.5, 1.0], 7).unwrap();

        let mut first = [0.0_f32; 4];
        buffer.write_f32(&mut first, 2);
        assert_eq!(first, [0.25, 0.25, -0.5, -0.5]);
        assert!(!buffer.is_idle());

        let mut second = [9.0_f32; 4];
        buffer.write_f32(&mut second, 2);
        assert_eq!(second, [1.0, 1.0, 0.0, 0.0]);
        assert!(buffer.is_idle());
        let observation = buffer.observation();
        assert_eq!(observation.accepted_ns, 7);
        assert_eq!(observation.first_callback_frames, Some(2));
        assert_eq!(observation.callback_count, 2);
        assert!(observation.first_non_silent_ns.is_some());
    }

    #[test]
    fn callbacks_convert_to_integer_device_formats() {
        let buffer = CallbackBuffer::new(3);
        buffer.load(&[-1.0, 0.0, 1.0], 0).unwrap();
        let mut signed = [0_i16; 3];
        buffer.write_i16(&mut signed, 1);
        assert_eq!(signed, [-32_767, 0, 32_767]);

        buffer.load(&[-1.0, 0.0, 1.0], 0).unwrap();
        let mut unsigned = [0_u16; 3];
        buffer.write_u16(&mut unsigned, 1);
        assert_eq!(unsigned, [0, 32_768, 65_535]);
    }

    #[test]
    fn producer_rejects_oversize_and_concurrent_chunks() {
        let buffer = CallbackBuffer::new(2);
        assert!(matches!(
            buffer.load(&[0.0, 0.0, 0.0], 0),
            Err(DeviceError::ChunkTooLarge {
                frames: 3,
                capacity: 2
            })
        ));
        buffer.load(&[0.5], 0).unwrap();
        assert!(matches!(
            buffer.load(&[0.5], 0),
            Err(DeviceError::PlaybackBusy)
        ));
    }

    #[test]
    fn callback_failure_is_reported_without_a_device() {
        let buffer = CallbackBuffer::new(1);
        assert!(!buffer.stream_failed());
        buffer.mark_stream_failed();
        assert!(buffer.stream_failed());
    }
}
