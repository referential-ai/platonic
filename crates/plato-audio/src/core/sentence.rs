use std::{fmt, mem};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

use crate::SentenceError;

const MIN_SENTENCE_CHARS: usize = 20;

/// Nonempty, trimmed text accepted for speech synthesis.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Sentence(String);

impl Sentence {
    /// Constructs a speakable sentence from nonempty text.
    pub fn new(text: impl Into<String>) -> Result<Self, SentenceError> {
        let text = text.into();
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return Err(SentenceError::Empty);
        }
        Ok(Self(trimmed.to_owned()))
    }

    /// Borrows the sentence text.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the value and returns its text.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for Sentence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for Sentence {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Sentence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

/// Incrementally cuts streamed text using the admitted Hermes sentence rules.
///
/// A boundary is recognized after `.`, `!`, or `?` followed by whitespace, or
/// at a blank line. Heads shorter than 20 Unicode characters remain buffered
/// and merge forward. [`SentenceCutter::finish`] drains a final nonempty tail
/// once.
#[derive(Debug, Default)]
pub struct SentenceCutter {
    buffer: String,
}

impl SentenceCutter {
    /// Constructs an empty sentence cutter.
    pub fn new() -> Self {
        Self::default()
    }

    /// Absorbs one streaming fragment and returns every newly complete sentence.
    pub fn push(&mut self, fragment: &str) -> Vec<Sentence> {
        self.buffer.push_str(fragment);
        self.cut_ready()
    }

    /// Drains the remaining nonempty text exactly once.
    pub fn finish(&mut self) -> Option<Sentence> {
        let tail = mem::take(&mut self.buffer);
        Sentence::new(tail).ok()
    }

    /// Returns whether uncommitted non-whitespace text remains buffered.
    pub fn has_pending_text(&self) -> bool {
        !self.buffer.trim().is_empty()
    }

    fn cut_ready(&mut self) -> Vec<Sentence> {
        let mut sentences = Vec::new();
        let mut search_from = 0;

        while let Some(boundary_end) = next_boundary(&self.buffer, search_from) {
            let head = &self.buffer[..boundary_end];
            if head.trim().chars().count() < MIN_SENTENCE_CHARS {
                search_from = boundary_end;
                continue;
            }

            let head = self.buffer[..boundary_end].to_owned();
            self.buffer.drain(..boundary_end);
            if let Ok(sentence) = Sentence::new(head) {
                sentences.push(sentence);
            }
            search_from = 0;
        }

        sentences
    }
}

fn next_boundary(text: &str, search_from: usize) -> Option<usize> {
    let mut previous = None;
    for (offset, current) in text.char_indices() {
        if offset < search_from {
            previous = Some(current);
            continue;
        }

        let punctuation_boundary = current.is_whitespace()
            && previous.is_some_and(|character| matches!(character, '.' | '!' | '?'));
        let blank_line =
            current == '\n' && (text[..offset].ends_with('\n') || text[..offset].ends_with("\n\r"));
        if punctuation_boundary || blank_line {
            return Some(offset + current.len_utf8());
        }
        previous = Some(current);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(fragments: &[&str]) -> Vec<String> {
        let mut cutter = SentenceCutter::new();
        let mut sentences: Vec<String> = fragments
            .iter()
            .flat_map(|fragment| cutter.push(fragment))
            .map(Sentence::into_string)
            .collect();
        if let Some(tail) = cutter.finish() {
            sentences.push(tail.into_string());
        }
        sentences
    }

    #[test]
    fn waits_for_whitespace_after_punctuation_across_fragments() {
        let mut cutter = SentenceCutter::new();
        assert!(cutter.push("This sentence is ready now.").is_empty());
        assert_eq!(
            cutter.push(" Next")[0].as_str(),
            "This sentence is ready now."
        );
        assert_eq!(cutter.finish().unwrap().as_str(), "Next");
    }

    #[test]
    fn punctuation_without_following_whitespace_is_not_a_boundary() {
        assert_eq!(
            collect(&["Version 1.2 is available today.", " Great."]),
            vec!["Version 1.2 is available today.", "Great."]
        );
    }

    #[test]
    fn blank_lines_cut_without_terminal_punctuation() {
        assert_eq!(
            collect(&["A paragraph long enough", " to speak\n", "\nTail"]),
            vec!["A paragraph long enough to speak", "Tail"]
        );
        assert_eq!(
            collect(&["A Windows paragraph long enough\r\n", "\r\nTail"]),
            vec!["A Windows paragraph long enough", "Tail"]
        );
    }

    #[test]
    fn short_heads_merge_forward_until_the_accumulated_head_is_long_enough() {
        assert_eq!(
            collect(&["Ha! Tiny. This following sentence is long enough. Tail"]),
            vec!["Ha! Tiny. This following sentence is long enough.", "Tail"]
        );
    }

    #[test]
    fn final_tail_is_flushed_once() {
        let mut cutter = SentenceCutter::new();
        assert!(cutter.push("Unpunctuated final fragment").is_empty());
        assert_eq!(
            cutter.finish().unwrap().as_str(),
            "Unpunctuated final fragment"
        );
        assert_eq!(cutter.finish(), None);
        assert!(!cutter.has_pending_text());
    }

    #[test]
    fn every_fragmentation_produces_the_same_sentence_sequence() {
        let text = "A complete first sentence. Ha! A complete second sentence follows! Tail";
        let expected = collect(&[text]);

        for split in text
            .char_indices()
            .map(|(index, _)| index)
            .chain(std::iter::once(text.len()))
        {
            assert_eq!(collect(&[&text[..split], &text[split..]]), expected);
        }

        let char_boundaries: Vec<usize> = text
            .char_indices()
            .map(|(index, _)| index)
            .chain(std::iter::once(text.len()))
            .collect();
        for window in char_boundaries.windows(3) {
            assert_eq!(
                collect(&[
                    &text[..window[1]],
                    &text[window[1]..window[2]],
                    &text[window[2]..]
                ]),
                expected
            );
        }
    }

    #[test]
    fn unicode_character_count_controls_short_fragment_merging() {
        let short = format!("{}! ", "é".repeat(18));
        let long = format!("{}! ", "é".repeat(20));
        let mut cutter = SentenceCutter::new();
        assert!(cutter.push(&short).is_empty());
        assert_eq!(
            cutter.push(&long)[0].as_str(),
            format!("{short}{long}").trim()
        );
    }
}
