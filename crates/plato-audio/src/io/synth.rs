use std::sync::atomic::AtomicBool;

use crate::{AudioFormat, PcmChunk, PcmSinkError, Sentence, SynthError};

/// A synchronous consumer for model-emitted PCM chunks.
pub trait PcmSink {
    /// Accepts one validated chunk without assigning run or session meaning.
    fn push(&mut self, chunk: PcmChunk) -> Result<(), PcmSinkError>;
}

/// A synchronous speech engine whose resident state is reused across calls.
pub trait SpeechSynthesizer: Send {
    /// Returns the exact PCM format produced by this engine.
    fn output_format(&self) -> AudioFormat;

    /// Synthesizes one sentence and pushes its PCM serially into `sink`.
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
