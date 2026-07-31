use crate::daemon::protocol::{
    HelloResult, PendingApprovalSnapshot, RunStateName, SessionSummary, TranscriptReadResult,
};
use platonic_core::EffectClass;
use ratatui::text::Line;
use std::{fmt, sync::RwLock, time::Instant};

use super::{
    ApprovalModalView,
    commands::{SlashCommandSpec, has_slash_command_prefix, matching_slash_commands},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TuiState {
    pub workspace_root: String,
    pub socket_path: String,
    pub connection: ConnectionState,
    pub sessions: Vec<SessionSummary>,
    pub selected_session_id: Option<String>,
    pub transcript: TranscriptState,
    pub active_run: Option<ActiveRunView>,
    pub live_events: Vec<LiveEventLine>,
    pub(super) history_rows: HistoryRowsCache,
    pub scroll_offset: usize,
    pub active_model: Option<String>,
    pub active_run_elapsed_secs: Option<u64>,
    pub composer: String,
    pub composer_cursor: usize,
    pub composer_kill_buffer: String,
    pub slash_popup: Option<SlashPopupView>,
    pub session_picker: Option<SessionPickerView>,
    pub queued_messages: Vec<String>,
    pub issue_prep_started_at: Option<Instant>,
    pub input_history: Vec<String>,
    pub history_index: Option<usize>,
    pub status_message: Option<String>,
    pub stream_warning: Option<String>,
    pub approval: Option<ApprovalModalView>,
    pub help_visible: bool,
    pub cancel_requested: bool,
}

impl TuiState {
    pub fn connected(
        workspace_root: String,
        socket_path: String,
        hello: HelloResult,
        sessions: Vec<SessionSummary>,
        transcript: TranscriptState,
    ) -> Self {
        let selected_session_id = sessions.first().map(|session| session.session_id.clone());
        let mut state = Self::new(
            workspace_root,
            socket_path,
            ConnectionState::Connected {
                workspace_id: hello.workspace_id,
                daemon_version: hello.daemon_version,
                ledger_path: hello.ledger_path,
            },
        );
        state.sessions = sessions;
        state.selected_session_id = selected_session_id;
        state.replace_transcript(transcript);
        state
    }

    pub fn disconnected(workspace_root: String, socket_path: String, error: String) -> Self {
        Self::new(
            workspace_root,
            socket_path,
            ConnectionState::Disconnected { error },
        )
    }

    fn new(workspace_root: String, socket_path: String, connection: ConnectionState) -> Self {
        Self {
            workspace_root,
            socket_path,
            connection,
            sessions: Vec::new(),
            selected_session_id: None,
            transcript: TranscriptState::None,
            active_run: None,
            live_events: Vec::new(),
            history_rows: HistoryRowsCache::default(),
            scroll_offset: 0,
            active_model: None,
            active_run_elapsed_secs: None,
            composer: String::new(),
            composer_cursor: 0,
            composer_kill_buffer: String::new(),
            slash_popup: None,
            session_picker: None,
            queued_messages: Vec::new(),
            issue_prep_started_at: None,
            input_history: Vec::new(),
            history_index: None,
            status_message: None,
            stream_warning: None,
            approval: None,
            help_visible: false,
            cancel_requested: false,
        }
    }

    pub(super) fn move_slash_popup_selection(&mut self, delta: isize) {
        let Some(popup) = self.slash_popup.as_mut() else {
            return;
        };
        let count = matching_slash_commands(&popup.filter).len().min(5);
        popup.selected = Self::wrapped_selection(popup.selected, count, delta);
    }

    pub(super) fn wrapped_selection(selected: usize, count: usize, delta: isize) -> usize {
        if count == 0 {
            return 0;
        }
        let current = selected.min(count - 1);
        if delta < 0 {
            current.checked_sub(1).unwrap_or(count - 1)
        } else {
            (current + 1) % count
        }
    }

    pub(super) fn selected_slash_command(&self) -> Option<&'static SlashCommandSpec> {
        let popup = self.slash_popup.as_ref()?;
        matching_slash_commands(&popup.filter)
            .into_iter()
            .take(5)
            .nth(popup.selected)
    }

    pub(super) fn complete_selected_slash_command(&mut self) {
        let Some(command) = self.selected_slash_command() else {
            return;
        };
        self.composer = format!("/{} ", command.name);
        self.composer_cursor = self.composer.len();
        self.history_index = None;
        self.slash_popup = None;
    }

    fn sync_slash_popup(&mut self) {
        let Some(filter) = slash_filter_at_cursor(&self.composer, self.composer_cursor) else {
            self.slash_popup = None;
            return;
        };
        let selected = self.slash_popup.as_ref().map_or(0, |popup| popup.selected);
        let count = matching_slash_commands(&filter).len().min(5);
        self.slash_popup = Some(SlashPopupView {
            filter,
            selected: selected.min(count.saturating_sub(1)),
        });
    }

    pub(super) fn handle_paste_text(&mut self, text: &str) {
        if self.help_visible || self.approval.is_some() {
            return;
        }
        self.insert_composer_text(&text.replace('\r', "\n"));
    }

    pub(super) fn insert_composer_char(&mut self, ch: char) {
        let mut buffer = [0; 4];
        self.insert_composer_text(ch.encode_utf8(&mut buffer));
    }

    pub(super) fn insert_composer_text(&mut self, text: &str) {
        self.clamp_composer_cursor();
        self.composer.insert_str(self.composer_cursor, text);
        self.composer_cursor += text.len();
        self.history_index = None;
        self.sync_slash_popup();
    }

    pub(super) fn delete_composer_before_cursor(&mut self) {
        self.clamp_composer_cursor();
        if self.composer_cursor == 0 {
            return;
        }
        let start = previous_boundary(&self.composer, self.composer_cursor);
        self.composer.replace_range(start..self.composer_cursor, "");
        self.composer_cursor = start;
        self.history_index = None;
        self.sync_slash_popup();
    }

    pub(super) fn delete_composer_after_cursor(&mut self) {
        self.clamp_composer_cursor();
        if self.composer_cursor >= self.composer.len() {
            return;
        }
        let end = next_boundary(&self.composer, self.composer_cursor);
        self.composer.replace_range(self.composer_cursor..end, "");
        self.history_index = None;
        self.sync_slash_popup();
    }

    pub(super) fn delete_composer_to_line_end(&mut self) {
        self.clamp_composer_cursor();
        let end = line_end_at(&self.composer, self.composer_cursor);
        self.composer_kill_buffer = self.composer[self.composer_cursor..end].to_owned();
        self.composer.replace_range(self.composer_cursor..end, "");
        self.history_index = None;
        self.sync_slash_popup();
    }

    pub(super) fn delete_previous_word(&mut self) {
        self.clamp_composer_cursor();
        let mut start = self.composer_cursor;
        while start > 0 && char_before(&self.composer, start).is_some_and(char::is_whitespace) {
            start = previous_boundary(&self.composer, start);
        }
        while start > 0 && char_before(&self.composer, start).is_some_and(|ch| !ch.is_whitespace())
        {
            start = previous_boundary(&self.composer, start);
        }
        self.composer_kill_buffer = self.composer[start..self.composer_cursor].to_owned();
        self.composer.replace_range(start..self.composer_cursor, "");
        self.composer_cursor = start;
        self.history_index = None;
        self.sync_slash_popup();
    }

    pub(super) fn kill_composer_to_start(&mut self) {
        self.clamp_composer_cursor();
        self.composer_kill_buffer = self.composer[..self.composer_cursor].to_owned();
        self.composer.replace_range(..self.composer_cursor, "");
        self.composer_cursor = 0;
        self.history_index = None;
        self.sync_slash_popup();
    }

    pub(super) fn yank_composer_kill_buffer(&mut self) {
        if self.composer_kill_buffer.is_empty() {
            return;
        }
        let text = self.composer_kill_buffer.clone();
        self.insert_composer_text(&text);
    }

    pub(super) fn clear_composer(&mut self) {
        self.composer.clear();
        self.composer_cursor = 0;
        self.history_index = None;
        self.slash_popup = None;
    }

    pub(super) fn move_composer_left(&mut self) {
        self.clamp_composer_cursor();
        self.composer_cursor = previous_boundary(&self.composer, self.composer_cursor);
        self.sync_slash_popup();
    }

    pub(super) fn move_composer_right(&mut self) {
        self.clamp_composer_cursor();
        self.composer_cursor = next_boundary(&self.composer, self.composer_cursor);
        self.sync_slash_popup();
    }

    pub(super) fn move_composer_line_start(&mut self) {
        self.clamp_composer_cursor();
        self.composer_cursor = line_start_at(&self.composer, self.composer_cursor);
        self.sync_slash_popup();
    }

    pub(super) fn move_composer_line_end(&mut self) {
        self.clamp_composer_cursor();
        self.composer_cursor = line_end_at(&self.composer, self.composer_cursor);
        self.sync_slash_popup();
    }

    pub(super) fn move_composer_word_left(&mut self) {
        self.clamp_composer_cursor();
        let mut start = self.composer_cursor;
        while start > 0 && char_before(&self.composer, start).is_some_and(char::is_whitespace) {
            start = previous_boundary(&self.composer, start);
        }
        while start > 0 && char_before(&self.composer, start).is_some_and(|ch| !ch.is_whitespace())
        {
            start = previous_boundary(&self.composer, start);
        }
        self.composer_cursor = start;
        self.sync_slash_popup();
    }

    pub(super) fn move_composer_word_right(&mut self) {
        self.clamp_composer_cursor();
        let mut end = self.composer_cursor;
        while end < self.composer.len()
            && char_at(&self.composer, end).is_some_and(|ch| !ch.is_whitespace())
        {
            end = next_boundary(&self.composer, end);
        }
        while end < self.composer.len()
            && char_at(&self.composer, end).is_some_and(char::is_whitespace)
        {
            end = next_boundary(&self.composer, end);
        }
        self.composer_cursor = end;
        self.sync_slash_popup();
    }

    pub(super) fn move_composer_up(&mut self) -> bool {
        self.clamp_composer_cursor();
        let start = line_start_at(&self.composer, self.composer_cursor);
        if start == 0 {
            return false;
        }
        let column = self.composer[start..self.composer_cursor].chars().count();
        let previous_end = previous_boundary(&self.composer, start);
        let previous_start = line_start_at(&self.composer, previous_end);
        self.composer_cursor =
            nth_char_boundary(&self.composer, previous_start, previous_end, column);
        self.sync_slash_popup();
        true
    }

    pub(super) fn move_composer_down(&mut self) -> bool {
        self.clamp_composer_cursor();
        let start = line_start_at(&self.composer, self.composer_cursor);
        let end = line_end_at(&self.composer, self.composer_cursor);
        if end >= self.composer.len() {
            return false;
        }
        let column = self.composer[start..self.composer_cursor].chars().count();
        let next_start = next_boundary(&self.composer, end);
        let next_end = line_end_at(&self.composer, next_start);
        self.composer_cursor = nth_char_boundary(&self.composer, next_start, next_end, column);
        self.sync_slash_popup();
        true
    }

    pub(super) fn consume_line_continuation(&mut self) -> bool {
        self.clamp_composer_cursor();
        if self.composer_cursor == 0 {
            return false;
        }
        let start = previous_boundary(&self.composer, self.composer_cursor);
        if &self.composer[start..self.composer_cursor] != "\\" {
            return false;
        }
        self.composer
            .replace_range(start..self.composer_cursor, "\n");
        self.composer_cursor = start + 1;
        self.history_index = None;
        self.sync_slash_popup();
        true
    }

    pub(super) fn recall_history_previous(&mut self) {
        if self.input_history.is_empty() {
            return;
        }
        let index = self
            .history_index
            .map(|index| index.saturating_sub(1))
            .unwrap_or_else(|| self.input_history.len() - 1);
        self.history_index = Some(index);
        self.composer = self.input_history[index].clone();
        self.composer_cursor = self.composer.len();
        self.sync_slash_popup();
    }

    pub(super) fn recall_history_next(&mut self) {
        let Some(index) = self.history_index else {
            return;
        };
        if index + 1 >= self.input_history.len() {
            self.clear_composer();
        } else {
            let next = index + 1;
            self.history_index = Some(next);
            self.composer = self.input_history[next].clone();
            self.composer_cursor = self.composer.len();
            self.sync_slash_popup();
        }
    }

    pub(super) fn record_input_history(&mut self, message: &str) {
        if self.input_history.last().is_none_or(|last| last != message) {
            self.input_history.push(message.to_owned());
        }
        self.history_index = None;
    }

    fn clamp_composer_cursor(&mut self) {
        self.composer_cursor = self.composer_cursor.min(self.composer.len());
        while !self.composer.is_char_boundary(self.composer_cursor) {
            self.composer_cursor -= 1;
        }
    }

    pub(super) fn replace_transcript(&mut self, transcript: TranscriptState) {
        self.transcript = transcript;
        let _ = self
            .history_rows
            .transcript
            .get_mut()
            .expect("transcript row cache lock poisoned")
            .take();
    }

    pub(super) fn clear_live_events(&mut self) {
        self.live_events.clear();
        self.invalidate_live_event_rows();
    }

    pub(super) fn invalidate_live_event_rows(&mut self) {
        let _ = self
            .history_rows
            .live_events
            .get_mut()
            .expect("live event row cache lock poisoned")
            .take();
    }
}

fn slash_filter_at_cursor(text: &str, cursor: usize) -> Option<String> {
    if !text.starts_with('/') {
        return None;
    }
    let first_line_end = text.find('\n').unwrap_or(text.len());
    if cursor > first_line_end {
        return None;
    }
    let after_slash = &text[1..first_line_end];
    let name_len = after_slash
        .find(char::is_whitespace)
        .unwrap_or(after_slash.len());
    let name_end = 1 + name_len;
    if cursor > name_end {
        return None;
    }
    let name = &after_slash[..name_len];
    let rest = &text[name_end..first_line_end];
    if name.is_empty() && !rest.is_empty() {
        return None;
    }
    if name.is_empty() || has_slash_command_prefix(name) {
        Some(name.to_owned())
    } else {
        None
    }
}

fn previous_boundary(value: &str, position: usize) -> usize {
    if position == 0 {
        return 0;
    }
    value[..position]
        .char_indices()
        .last()
        .map_or(0, |(index, _)| index)
}

fn next_boundary(value: &str, position: usize) -> usize {
    if position >= value.len() {
        return value.len();
    }
    position + value[position..].chars().next().map_or(0, char::len_utf8)
}

fn char_before(value: &str, position: usize) -> Option<char> {
    if position == 0 {
        None
    } else {
        value[..position].chars().next_back()
    }
}

fn char_at(value: &str, position: usize) -> Option<char> {
    if position >= value.len() {
        None
    } else {
        value[position..].chars().next()
    }
}

fn line_start_at(value: &str, position: usize) -> usize {
    value[..position].rfind('\n').map_or(0, |index| index + 1)
}

fn line_end_at(value: &str, position: usize) -> usize {
    value[position..]
        .find('\n')
        .map_or(value.len(), |index| position + index)
}

fn nth_char_boundary(value: &str, start: usize, end: usize, column: usize) -> usize {
    value[start..end]
        .char_indices()
        .map(|(index, _)| start + index)
        .chain(std::iter::once(end))
        .nth(column)
        .unwrap_or(end)
}

#[derive(Default)]
pub(super) struct HistoryRowsCache {
    pub(super) transcript: RwLock<Option<(String, Vec<Line<'static>>)>>,
    pub(super) live_events: RwLock<Option<(Vec<LiveEventLine>, Vec<Line<'static>>)>>,
}

// These rows are derived from public source fields and are not semantic TUI state.
impl Clone for HistoryRowsCache {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl fmt::Debug for HistoryRowsCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HistoryRowsCache")
    }
}

impl PartialEq for HistoryRowsCache {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for HistoryRowsCache {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SlashPopupView {
    pub filter: String,
    pub selected: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionPickerView {
    pub filter: String,
    pub selected: usize,
}

impl SessionPickerView {
    pub fn matching_sessions<'a>(&self, sessions: &'a [SessionSummary]) -> Vec<&'a SessionSummary> {
        let filter = self.filter.to_lowercase();
        sessions
            .iter()
            .filter(|session| session.latest_question.to_lowercase().contains(&filter))
            .collect()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectionState {
    Connected {
        workspace_id: String,
        daemon_version: String,
        ledger_path: String,
    },
    Disconnected {
        error: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TranscriptView {
    pub run_id: String,
    pub content: String,
}

impl From<TranscriptReadResult> for TranscriptView {
    fn from(transcript: TranscriptReadResult) -> Self {
        Self {
            run_id: transcript.run_id,
            content: transcript.transcript,
        }
    }
}

pub(super) fn approval_from_snapshot(snapshot: PendingApprovalSnapshot) -> ApprovalModalView {
    let effect = match snapshot.effect {
        EffectClass::ReadOnly => "read_only",
        EffectClass::WorkspaceWrite => "workspace_write",
        EffectClass::Network => "network",
        EffectClass::ExternalSideEffect => "external_side_effect",
        EffectClass::SecretAccess => "secret_access",
    };
    ApprovalModalView {
        run_id: snapshot.run_id,
        tool_call_id: snapshot.tool_call_id,
        tool_name: snapshot.tool_name,
        effect: effect.into(),
        reason: snapshot
            .reason
            .unwrap_or_else(|| "approval required".into()),
        input_preview: snapshot
            .input_preview
            .unwrap_or_else(|| "input preview unavailable".into()),
        approval_preview: snapshot.approval_preview,
        diff_preview: snapshot.diff_preview,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TranscriptState {
    None,
    Loaded(TranscriptView),
    Unavailable { run_id: String, error: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActiveRunView {
    pub run_id: String,
    pub status: RunStateName,
}

impl ActiveRunView {
    pub fn new(run_id: String, status: RunStateName) -> Self {
        Self { run_id, status }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveEventLine {
    pub offset: Option<u64>,
    pub kind: LiveEventKind,
    pub text: String,
}

impl LiveEventLine {
    pub fn user(text: impl Into<String>) -> Self {
        Self {
            offset: None,
            kind: LiveEventKind::User,
            text: text.into(),
        }
    }

    pub fn assistant(offset: Option<u64>, text: impl Into<String>) -> Self {
        Self {
            offset,
            kind: LiveEventKind::Assistant,
            text: text.into(),
        }
    }

    pub fn assistant_delta(offset: Option<u64>, text: impl Into<String>) -> Self {
        Self {
            offset,
            kind: LiveEventKind::AssistantDelta,
            text: text.into(),
        }
    }

    pub fn tool(offset: Option<u64>, text: impl Into<String>) -> Self {
        Self {
            offset,
            kind: LiveEventKind::Tool,
            text: text.into(),
        }
    }

    pub fn status(offset: Option<u64>, text: impl Into<String>) -> Self {
        Self {
            offset,
            kind: LiveEventKind::Status,
            text: text.into(),
        }
    }

    pub fn warning(offset: Option<u64>, text: impl Into<String>) -> Self {
        Self {
            offset,
            kind: LiveEventKind::Warning,
            text: text.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveEventKind {
    User,
    Assistant,
    AssistantDelta,
    Tool,
    Status,
    Warning,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_picker_matches_unicode_lowercase_substrings_in_source_order() {
        let sessions = vec![
            session("session_1", "First STRAẞE task"),
            session("session_2", "unrelated"),
            session("session_3", "Third Straße follow-up"),
        ];
        let mut picker = SessionPickerView {
            filter: String::new(),
            selected: 0,
        };

        assert_eq!(
            session_ids(picker.matching_sessions(&sessions)),
            vec!["session_1", "session_2", "session_3"]
        );

        picker.filter = "straße".into();
        assert_eq!(
            session_ids(picker.matching_sessions(&sessions)),
            vec!["session_1", "session_3"]
        );
    }

    #[test]
    fn session_picker_does_not_normalize_unicode_or_apply_locale_rules() {
        let sessions = vec![
            session("session_1", "Review Café"),
            session("session_2", "Visit İSTANBUL"),
        ];
        let mut picker = SessionPickerView {
            filter: "cafe\u{301}".into(),
            selected: 0,
        };

        assert!(picker.matching_sessions(&sessions).is_empty());

        picker.filter = "istanbul".into();
        assert!(picker.matching_sessions(&sessions).is_empty());

        picker.filter = "i\u{307}stanbul".into();
        assert_eq!(
            session_ids(picker.matching_sessions(&sessions)),
            vec!["session_2"]
        );
    }

    #[test]
    fn pending_approval_snapshot_maps_exact_modal_fields() {
        let modal = approval_from_snapshot(PendingApprovalSnapshot {
            run_id: "run_selected".into(),
            tool_call_id: "call_selected".into(),
            tool_name: "file.edit".into(),
            effect: EffectClass::WorkspaceWrite,
            reason: Some("review the selected edit".into()),
            input_preview: Some(r#"{"path":"selected.txt"}"#.into()),
            approval_preview: Some("edit selected.txt".into()),
            diff_preview: Some("-old selected\n+new selected\n".into()),
        });

        assert_eq!(modal.run_id, "run_selected");
        assert_eq!(modal.tool_call_id, "call_selected");
        assert_eq!(modal.tool_name, "file.edit");
        assert_eq!(modal.effect, "workspace_write");
        assert_eq!(modal.reason, "review the selected edit");
        assert_eq!(modal.input_preview, r#"{"path":"selected.txt"}"#);
        assert_eq!(modal.approval_preview.as_deref(), Some("edit selected.txt"));
        assert_eq!(
            modal.diff_preview.as_deref(),
            Some("-old selected\n+new selected\n")
        );
    }

    fn session(session_id: &str, latest_question: &str) -> SessionSummary {
        SessionSummary {
            session_id: session_id.into(),
            run_id: format!("run_{session_id}"),
            status: RunStateName::Finished,
            latest_question: latest_question.into(),
            ledger_path: "/tmp/agent.db".into(),
        }
    }

    fn session_ids(sessions: Vec<&SessionSummary>) -> Vec<&str> {
        sessions
            .into_iter()
            .map(|session| session.session_id.as_str())
            .collect()
    }
}
