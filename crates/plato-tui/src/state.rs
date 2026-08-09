use nucleo::{
    Config, Matcher, Utf32Str,
    pattern::{Atom, AtomKind, CaseMatching, Normalization},
};
use platonic_core::EffectClass;
use platonic_protocol::{
    DaemonStatusResult, HelloResult, ModelIdentityStatus, PendingApprovalSnapshot, RunStateName,
    SessionSummary, TranscriptReadResult, TypedTranscript,
};
use ratatui::text::Line;
use std::{
    collections::VecDeque,
    fmt,
    sync::RwLock,
    time::{Duration, Instant},
};
use tui_textarea::{CursorMove, TextArea};

use super::{
    ApprovalModalView,
    commands::{SlashCommandSpec, has_slash_command_match, matching_slash_commands},
    markdown::{MarkdownRenderer, SyntaxTheme},
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) enum DisplayMode {
    #[default]
    Conversation,
    Audit,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) enum MotionMode {
    #[default]
    Animated,
    Reduced,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) enum FooterMode {
    #[default]
    Contextual,
    Shortcuts,
    QuitConfirm,
    Offline,
}

#[derive(Clone, Debug)]
/// Complete render and interaction state for the terminal client.
pub struct TuiState {
    /// Canonical workspace root displayed by the client.
    pub workspace_root: String,
    /// Daemon endpoint displayed by the client.
    pub socket_path: String,
    /// Current daemon connection state.
    pub connection: ConnectionState,
    /// Sessions returned by the daemon, newest first.
    pub sessions: Vec<SessionSummary>,
    /// Session selected for display and continuation.
    pub selected_session_id: Option<String>,
    /// Selected transcript state.
    pub transcript: TranscriptState,
    /// Run currently active in the selected session.
    pub active_run: Option<ActiveRunView>,
    /// Transient events received since transcript readback.
    pub live_events: Vec<LiveEventLine>,
    pub(super) streaming: StreamingBuffer,
    pub(super) history_rows: HistoryRowsCache,
    pub(super) display_mode: DisplayMode,
    /// Latest requested-or-responded model identity state for the selected run.
    pub active_model: Option<ModelIdentityStatus>,
    /// Elapsed active-run time, in seconds.
    pub active_run_elapsed_secs: Option<u64>,
    pub(super) working_elapsed_millis: u64,
    /// Composer editing state.
    pub composer: TextArea<'static>,
    /// Open slash-command popup state.
    pub slash_popup: Option<SlashPopupView>,
    /// Open session-picker state.
    pub session_picker: Option<SessionPickerView>,
    /// Messages queued behind the active operation.
    pub queued_messages: Vec<String>,
    /// Start time of an active issue-preparation request.
    pub issue_prep_started_at: Option<Instant>,
    pub(super) issue_prep_elapsed_secs: Option<u64>,
    pub(super) motion_mode: MotionMode,
    /// Submitted composer history.
    pub input_history: Vec<String>,
    /// Selected composer-history index.
    pub history_index: Option<usize>,
    /// Current status-row message.
    pub status_message: Option<String>,
    /// Current event-stream warning.
    pub stream_warning: Option<String>,
    /// Approval request currently awaiting a decision.
    pub approval: Option<ApprovalModalView>,
    /// Top-relative scroll offset for the bounded approval pane.
    pub approval_scroll_offset: usize,
    /// Whether the help overlay is open.
    pub help_visible: bool,
    /// Authoritative daemon status currently shown in its read-only modal.
    pub status_modal: Option<DaemonStatusResult>,
    /// Whether cancellation has already been requested.
    pub cancel_requested: bool,
}

impl PartialEq for TuiState {
    fn eq(&self, other: &Self) -> bool {
        self.workspace_root == other.workspace_root
            && self.socket_path == other.socket_path
            && self.connection == other.connection
            && self.sessions == other.sessions
            && self.selected_session_id == other.selected_session_id
            && self.transcript == other.transcript
            && self.active_run == other.active_run
            && self.live_events == other.live_events
            && self.streaming == other.streaming
            && self.history_rows == other.history_rows
            && self.display_mode == other.display_mode
            && self.active_model == other.active_model
            && self.active_run_elapsed_secs == other.active_run_elapsed_secs
            && self.working_elapsed_millis == other.working_elapsed_millis
            && composer_eq(&self.composer, &other.composer)
            && self.slash_popup == other.slash_popup
            && self.session_picker == other.session_picker
            && self.queued_messages == other.queued_messages
            && self.issue_prep_started_at == other.issue_prep_started_at
            && self.issue_prep_elapsed_secs == other.issue_prep_elapsed_secs
            && self.motion_mode == other.motion_mode
            && self.input_history == other.input_history
            && self.history_index == other.history_index
            && self.status_message == other.status_message
            && self.stream_warning == other.stream_warning
            && self.approval == other.approval
            && self.approval_scroll_offset == other.approval_scroll_offset
            && self.help_visible == other.help_visible
            && self.status_modal == other.status_modal
            && self.cancel_requested == other.cancel_requested
    }
}

impl Eq for TuiState {}

fn composer_eq(left: &TextArea<'static>, right: &TextArea<'static>) -> bool {
    if left.tab_length() != right.tab_length()
        || left.hard_tab_indent() != right.hard_tab_indent()
        || left.max_histories() != right.max_histories()
        || !composer_surface_eq(left, right)
    {
        return false;
    }

    let (mut left_undo, mut right_undo) = (left.clone(), right.clone());
    loop {
        match (left_undo.undo(), right_undo.undo()) {
            (false, false) => break,
            (true, true) if composer_surface_eq(&left_undo, &right_undo) => {}
            _ => return false,
        }
    }

    let (mut left_redo, mut right_redo) = (left.clone(), right.clone());
    loop {
        match (left_redo.redo(), right_redo.redo()) {
            (false, false) => break,
            (true, true) if composer_surface_eq(&left_redo, &right_redo) => {}
            _ => return false,
        }
    }
    true
}

fn composer_surface_eq(left: &TextArea<'static>, right: &TextArea<'static>) -> bool {
    left.lines() == right.lines()
        && left.cursor() == right.cursor()
        && left.selection_range() == right.selection_range()
        && left.yank_text() == right.yank_text()
}

impl TuiState {
    /// Creates state from a successful daemon hello and session readback.
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
        state.active_model = model_status_from_transcript(&transcript);
        state.replace_transcript(transcript);
        state
    }

    /// Creates disconnected state with its rendered connection error.
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
            streaming: StreamingBuffer::default(),
            history_rows: HistoryRowsCache::default(),
            display_mode: DisplayMode::Conversation,
            active_model: None,
            active_run_elapsed_secs: None,
            working_elapsed_millis: 0,
            composer: TextArea::default(),
            slash_popup: None,
            session_picker: None,
            queued_messages: Vec::new(),
            issue_prep_started_at: None,
            issue_prep_elapsed_secs: None,
            motion_mode: MotionMode::Animated,
            input_history: Vec::new(),
            history_index: None,
            status_message: None,
            stream_warning: None,
            approval: None,
            approval_scroll_offset: 0,
            help_visible: false,
            status_modal: None,
            cancel_requested: false,
        }
    }

    pub(super) fn set_reduced_motion(&mut self, reduced: bool) {
        self.motion_mode = if reduced {
            MotionMode::Reduced
        } else {
            MotionMode::Animated
        };
    }

    pub(super) fn footer_mode(&self) -> FooterMode {
        if self.help_visible {
            FooterMode::Shortcuts
        } else if matches!(self.connection, ConnectionState::Disconnected { .. }) {
            FooterMode::Offline
        } else if self.cancel_requested {
            FooterMode::QuitConfirm
        } else {
            FooterMode::Contextual
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
        self.set_composer_text(format!("/{} ", command.name));
        self.history_index = None;
        self.slash_popup = None;
    }

    fn sync_slash_popup(&mut self) {
        let (row, column) = self.composer.cursor();
        let Some(filter) = (row == 0)
            .then(|| slash_filter_at_cursor(&self.composer.lines()[0], column))
            .flatten()
        else {
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
        if self.help_visible || self.status_modal.is_some() || self.approval.is_some() {
            return;
        }
        let text = text.replace("\r\n", "\n").replace('\r', "\n");
        let modified = self.composer.insert_str(text);
        self.finish_composer_edit(modified);
    }

    pub(super) fn scroll_approval_up(&mut self, lines: usize) {
        self.approval_scroll_offset = self.approval_scroll_offset.saturating_sub(lines);
    }

    pub(super) fn scroll_approval_down(&mut self, lines: usize) {
        self.approval_scroll_offset = self.approval_scroll_offset.saturating_add(lines);
    }

    pub(super) fn handle_composer_key(&mut self, key: crossterm::event::KeyEvent) {
        let modified = self.composer.input(key);
        self.finish_composer_edit(modified);
    }

    pub(super) fn insert_composer_text(&mut self, text: &str) {
        let modified = self.composer.insert_str(text);
        self.finish_composer_edit(modified);
    }

    pub(super) fn composer_text(&self) -> String {
        self.composer.lines().join("\n")
    }

    pub(super) fn composer_is_empty(&self) -> bool {
        self.composer.is_empty()
    }

    pub(super) fn set_composer_text(&mut self, text: impl AsRef<str>) {
        self.composer = TextArea::from(text.as_ref().split('\n'));
        self.composer.move_cursor(CursorMove::Bottom);
        self.composer.move_cursor(CursorMove::End);
        self.sync_slash_popup();
    }

    pub(super) fn move_composer_cursor(&mut self, motion: CursorMove, selecting: bool) {
        if selecting {
            if !self.composer.is_selecting() {
                self.composer.start_selection();
            }
        } else {
            self.composer.cancel_selection();
        }
        self.composer.move_cursor(motion);
        self.sync_slash_popup();
    }

    pub(super) fn delete_composer_to_start(&mut self) {
        self.composer.start_selection();
        self.composer.move_cursor(CursorMove::Jump(0, 0));
        let modified = self.composer.cut();
        self.finish_composer_edit(modified);
    }

    pub(super) fn undo_composer(&mut self) {
        let modified = self.composer.undo();
        self.finish_composer_edit(modified);
    }

    pub(super) fn redo_composer(&mut self) {
        let modified = self.composer.redo();
        self.finish_composer_edit(modified);
    }

    pub(super) fn clear_composer(&mut self) {
        self.composer = TextArea::default();
        self.history_index = None;
        self.slash_popup = None;
    }

    pub(super) fn consume_line_continuation(&mut self) -> bool {
        let (row, column) = self.composer.cursor();
        if column == 0 || self.composer.lines()[row].chars().nth(column - 1) != Some('\\') {
            return false;
        }
        self.composer.delete_char();
        self.composer.insert_newline();
        self.finish_composer_edit(true);
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
        let text = self.input_history[index].clone();
        self.set_composer_text(text);
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
            let text = self.input_history[next].clone();
            self.set_composer_text(text);
        }
    }

    pub(super) fn record_input_history(&mut self, message: &str) {
        if self.input_history.last().is_none_or(|last| last != message) {
            self.input_history.push(message.to_owned());
        }
        self.history_index = None;
    }

    fn finish_composer_edit(&mut self, modified: bool) {
        if modified {
            self.history_index = None;
        }
        self.sync_slash_popup();
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

    pub(super) fn toggle_display_mode(&mut self) {
        self.display_mode = match self.display_mode {
            DisplayMode::Conversation => DisplayMode::Audit,
            DisplayMode::Audit => DisplayMode::Conversation,
        };
        self.invalidate_history_rows();
    }

    fn invalidate_history_rows(&mut self) {
        let _ = self
            .history_rows
            .transcript
            .get_mut()
            .expect("transcript row cache lock poisoned")
            .take();
        self.invalidate_live_event_rows();
    }

    pub(super) fn clear_live_events(&mut self) {
        self.live_events.clear();
        self.streaming = StreamingBuffer::default();
        self.invalidate_live_event_rows();
    }

    pub(super) fn queue_assistant_delta(&mut self, line: LiveEventLine, now: Instant) {
        if self.streaming.is_active() && !self.streaming.matches_run(line.run_id.as_deref()) {
            self.finalize_streaming(None);
        }
        if !self.streaming.is_active() {
            self.streaming
                .start(line.run_id.clone(), self.live_events.len());
        }
        self.streaming.push(line.offset, &line.text, now);
    }

    pub(super) fn drain_streaming_at(&mut self, now: Instant) -> bool {
        let Some(drained) = self.streaming.drain_at(now) else {
            return false;
        };
        let run_id = self.streaming.run_id.clone();
        let index = match self.streaming.event_index {
            Some(index) => index,
            None => {
                let index = self.streaming.insertion_index.min(self.live_events.len());
                self.live_events.insert(
                    index,
                    LiveEventLine {
                        run_id,
                        offset: drained.offset,
                        kind: LiveEventKind::Assistant,
                        text: String::new(),
                    },
                );
                self.streaming.event_index = Some(index);
                index
            }
        };
        let event = &mut self.live_events[index];
        event.text.push_str(&drained.text);
        event.offset = drained.offset.or(event.offset);
        self.invalidate_live_event_rows();
        true
    }

    pub(super) fn streaming_deadline(&self) -> Option<Instant> {
        self.streaming.deadline()
    }

    pub(super) fn finalize_streaming(&mut self, run_id: Option<&str>) -> bool {
        if !self.streaming.is_active() || run_id.is_some() && !self.streaming.matches_run(run_id) {
            return false;
        }
        let mut streaming = std::mem::take(&mut self.streaming);
        let source = std::mem::take(&mut streaming.source);
        let offset = streaming.last_offset;
        self.install_stream_source(streaming, source, offset)
    }

    pub(super) fn consolidate_assistant(&mut self, line: LiveEventLine) {
        if self.streaming.is_active() && self.streaming.matches_run(line.run_id.as_deref()) {
            let streaming = std::mem::take(&mut self.streaming);
            self.install_stream_source(streaming, line.text, line.offset);
            return;
        }
        if let Some(last) = self.live_events.last_mut()
            && last.kind == LiveEventKind::Assistant
            && last.run_id == line.run_id
        {
            *last = line;
        } else {
            self.live_events.push(line);
        }
        self.invalidate_live_event_rows();
    }

    fn install_stream_source(
        &mut self,
        streaming: StreamingBuffer,
        source: String,
        offset: Option<u64>,
    ) -> bool {
        if source.is_empty() {
            return false;
        }
        let line = LiveEventLine {
            run_id: streaming.run_id,
            offset,
            kind: LiveEventKind::Assistant,
            text: source,
        };
        if let Some(index) = streaming.event_index {
            self.live_events[index] = line;
        } else {
            let index = streaming.insertion_index.min(self.live_events.len());
            self.live_events.insert(index, line);
        }
        self.invalidate_live_event_rows();
        true
    }

    pub(super) fn bind_latest_user_to_run(&mut self, run_id: &str) {
        if let Some(event) = self
            .live_events
            .iter_mut()
            .rev()
            .find(|event| event.kind == LiveEventKind::User && event.run_id.is_none())
        {
            event.run_id = Some(run_id.to_owned());
            self.invalidate_live_event_rows();
        }
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

pub(super) const STREAM_QUIET_FLUSH: Duration = Duration::from_millis(80);

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct StreamingBuffer {
    run_id: Option<String>,
    source: String,
    pending: String,
    pending_offset: Option<u64>,
    last_offset: Option<u64>,
    last_arrival: Option<Instant>,
    queued: VecDeque<StreamChunk>,
    table_holdback: TableHoldback,
    next_drain_at: Option<Instant>,
    insertion_index: usize,
    event_index: Option<usize>,
}

impl StreamingBuffer {
    fn start(&mut self, run_id: Option<String>, insertion_index: usize) {
        self.run_id = run_id;
        self.insertion_index = insertion_index;
    }

    fn is_active(&self) -> bool {
        self.run_id.is_some() || !self.source.is_empty()
    }

    fn matches_run(&self, run_id: Option<&str>) -> bool {
        self.run_id.as_deref() == run_id
    }

    fn push(&mut self, offset: Option<u64>, text: &str, arrived_at: Instant) {
        self.source.push_str(text);
        self.pending.push_str(text);
        self.pending_offset = offset.or(self.pending_offset);
        self.last_offset = offset.or(self.last_offset);
        self.last_arrival = Some(arrived_at);

        while let Some(newline) = self.pending.find('\n') {
            let remaining = self.pending.split_off(newline + 1);
            let line = std::mem::replace(&mut self.pending, remaining);
            self.collect_line(StreamChunk {
                text: line,
                offset: self.pending_offset,
                arrived_at,
            });
        }
        if self.pending.is_empty() {
            self.pending_offset = None;
        }
    }

    fn collect_line(&mut self, line: StreamChunk) {
        match std::mem::take(&mut self.table_holdback) {
            TableHoldback::None => {
                if is_table_row(&line.text) {
                    self.table_holdback = TableHoldback::Candidate(line);
                } else {
                    self.queue(line);
                }
            }
            TableHoldback::Candidate(header) => {
                if is_table_delimiter(&line.text) {
                    self.table_holdback = TableHoldback::Table(vec![header, line]);
                } else {
                    self.queue(header);
                    self.collect_line(line);
                }
            }
            TableHoldback::Table(mut table) => {
                if is_table_row(&line.text) {
                    table.push(line);
                    self.table_holdback = TableHoldback::Table(table);
                } else {
                    self.queue(combine_chunks(table));
                    self.collect_line(line);
                }
            }
        }
    }

    fn queue(&mut self, chunk: StreamChunk) {
        let arrived_at = chunk.arrived_at;
        self.queued.push_back(chunk);
        let (delay, _) = drain_plan(self.queued.len());
        let deadline = arrived_at + delay;
        self.next_drain_at = Some(
            self.next_drain_at
                .map(|current| current.min(deadline))
                .unwrap_or(deadline),
        );
    }

    fn deadline(&self) -> Option<Instant> {
        let quiet = self.last_arrival.and_then(|arrived| {
            (!self.pending.is_empty()
                && matches!(self.table_holdback, TableHoldback::None)
                && !is_table_row(&self.pending))
            .then_some(arrived + STREAM_QUIET_FLUSH)
        });
        match (self.next_drain_at, quiet) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
            (None, None) => None,
        }
    }

    fn drain_at(&mut self, now: Instant) -> Option<StreamDrain> {
        if self
            .last_arrival
            .is_some_and(|arrived| now.saturating_duration_since(arrived) >= STREAM_QUIET_FLUSH)
            && !self.pending.is_empty()
            && matches!(self.table_holdback, TableHoldback::None)
            && !is_table_row(&self.pending)
        {
            let chunk = StreamChunk {
                text: std::mem::take(&mut self.pending),
                offset: self.pending_offset,
                arrived_at: self.last_arrival.expect("quiet stream has an arrival"),
            };
            self.pending_offset = None;
            self.queue(chunk);
            self.next_drain_at = Some(now);
        }

        if self.next_drain_at.is_none_or(|deadline| deadline > now) {
            return None;
        }
        let (_, count) = drain_plan(self.queued.len());
        let mut text = String::new();
        let mut offset = None;
        for _ in 0..count {
            let Some(chunk) = self.queued.pop_front() else {
                break;
            };
            text.push_str(&chunk.text);
            offset = chunk.offset.or(offset);
        }
        if self.queued.is_empty() {
            self.next_drain_at = None;
        } else {
            let (delay, _) = drain_plan(self.queued.len());
            self.next_drain_at = Some(now + delay);
        }
        (!text.is_empty()).then_some(StreamDrain { text, offset })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StreamChunk {
    text: String,
    offset: Option<u64>,
    arrived_at: Instant,
}

struct StreamDrain {
    text: String,
    offset: Option<u64>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
enum TableHoldback {
    #[default]
    None,
    Candidate(StreamChunk),
    Table(Vec<StreamChunk>),
}

fn combine_chunks(chunks: Vec<StreamChunk>) -> StreamChunk {
    let last = chunks.last().expect("table holdback is nonempty");
    let offset = last.offset;
    let arrived_at = last.arrived_at;
    let mut text = String::new();
    for chunk in chunks {
        text.push_str(&chunk.text);
    }
    StreamChunk {
        text,
        offset,
        arrived_at,
    }
}

fn drain_plan(pressure: usize) -> (Duration, usize) {
    match pressure {
        0 => (Duration::ZERO, 0),
        1..=3 => (Duration::from_millis(40), 1),
        4..=15 => (Duration::from_millis(24), 2),
        16..=63 => (Duration::from_millis(12), 4),
        _ => (Duration::from_millis(8), 8),
    }
}

fn is_table_row(line: &str) -> bool {
    table_cells(line).is_some_and(|cells| cells.len() >= 2)
}

fn is_table_delimiter(line: &str) -> bool {
    table_cells(line).is_some_and(|cells| {
        cells.len() >= 2
            && cells.into_iter().all(|cell| {
                let marker = cell.trim().trim_start_matches(':').trim_end_matches(':');
                marker.len() >= 3 && marker.bytes().all(|byte| byte == b'-')
            })
    })
}

fn table_cells(line: &str) -> Option<Vec<&str>> {
    let line = line.trim_end_matches(['\r', '\n']).trim();
    if line.is_empty() || !line.contains('|') {
        return None;
    }
    let line = line.strip_prefix('|').unwrap_or(line);
    let line = line.strip_suffix('|').unwrap_or(line);
    Some(line.split('|').collect())
}

fn model_status_from_transcript(transcript: &TranscriptState) -> Option<ModelIdentityStatus> {
    let TranscriptState::Loaded(transcript) = transcript else {
        return None;
    };
    transcript
        .typed
        .as_ref()?
        .runs
        .iter()
        .find(|run| run.run_id == transcript.run_id)?
        .model_status
        .clone()
}

fn slash_filter_at_cursor(text: &str, cursor: usize) -> Option<String> {
    if !text.starts_with('/') {
        return None;
    }
    let after_slash = &text[1..];
    let name_len = after_slash
        .find(char::is_whitespace)
        .unwrap_or(after_slash.len());
    if cursor > 1 + after_slash[..name_len].chars().count() {
        return None;
    }
    let name = &after_slash[..name_len];
    let rest = &after_slash[name_len..];
    if name.is_empty() && !rest.is_empty() {
        return None;
    }
    if name.is_empty() || has_slash_command_match(name) {
        Some(name.to_owned())
    } else {
        None
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct TranscriptRowsKey {
    pub(super) source: TranscriptView,
    pub(super) width: u16,
    pub(super) display_mode: DisplayMode,
    pub(super) syntax_theme: SyntaxTheme,
}

pub(super) struct CachedTranscriptRows {
    pub(super) key: TranscriptRowsKey,
    pub(super) rows: Vec<Line<'static>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct LiveEventRowsKey {
    pub(super) source: Vec<LiveEventLine>,
    pub(super) committed: Vec<(String, LiveEventKind, String)>,
    pub(super) width: u16,
    pub(super) display_mode: DisplayMode,
    pub(super) syntax_theme: SyntaxTheme,
}

pub(super) struct CachedLiveEventRows {
    pub(super) key: LiveEventRowsKey,
    pub(super) rows: Vec<Line<'static>>,
}

#[derive(Default)]
pub(super) struct HistoryRowsCache {
    pub(super) transcript: RwLock<Option<CachedTranscriptRows>>,
    pub(super) live_events: RwLock<Option<CachedLiveEventRows>>,
    pub(super) markdown: MarkdownRenderer,
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
/// Filter and focus state for the session picker.
pub struct SessionPickerView {
    /// Case-insensitive session-question filter.
    pub filter: String,
    /// Focused index within the filtered results.
    pub selected: usize,
}

impl SessionPickerView {
    /// Returns sessions whose visible first-question label or recovery ID matches the filter.
    pub fn matching_sessions<'a>(&self, sessions: &'a [SessionSummary]) -> Vec<&'a SessionSummary> {
        if self.filter.is_empty() {
            return sessions.iter().collect();
        }

        let fuzzy = Atom::new(
            &self.filter,
            CaseMatching::Ignore,
            Normalization::Never,
            AtomKind::Fuzzy,
            false,
        );
        let prefix = Atom::new(
            &self.filter,
            CaseMatching::Ignore,
            Normalization::Never,
            AtomKind::Prefix,
            false,
        );
        let mut config = Config::DEFAULT;
        config.prefer_prefix = true;
        let mut matcher = Matcher::new(config);
        let mut chars = Vec::new();
        let recovery_filter = self.filter.to_lowercase();
        let mut matches: Vec<_> = sessions
            .iter()
            .enumerate()
            .filter_map(|(source_index, session)| {
                let label = session_question_label(session);
                if let Some(score) = fuzzy.score(Utf32Str::new(label, &mut chars), &mut matcher) {
                    let is_prefix = prefix
                        .score(Utf32Str::new(label, &mut chars), &mut matcher)
                        .is_some();
                    Some((source_index, true, is_prefix, score, session))
                } else if session.session_id.to_lowercase().contains(&recovery_filter) {
                    Some((source_index, false, false, 0, session))
                } else {
                    None
                }
            })
            .collect();
        matches.sort_by(|left, right| {
            right
                .1
                .cmp(&left.1)
                .then_with(|| right.2.cmp(&left.2))
                .then_with(|| right.3.cmp(&left.3))
                .then_with(|| left.0.cmp(&right.0))
        });
        matches
            .into_iter()
            .map(|(_, _, _, _, session)| session)
            .collect()
    }
}

pub(super) fn session_question_label(session: &SessionSummary) -> &str {
    if !session.first_question.trim().is_empty() {
        &session.first_question
    } else if !session.latest_question.trim().is_empty() {
        &session.latest_question
    } else {
        "(no question)"
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Current connection state shown by the terminal client.
pub enum ConnectionState {
    /// The daemon hello completed successfully.
    Connected {
        /// Workspace identifier reported by the daemon.
        workspace_id: String,
        /// Daemon version reported by hello.
        daemon_version: String,
        /// Ledger path reported by hello.
        ledger_path: String,
    },
    /// The daemon is unavailable or incompatible.
    Disconnected {
        /// Rendered connection failure.
        error: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Transcript readback selected for display.
pub struct TranscriptView {
    /// Run represented by the readback.
    pub run_id: String,
    /// Run lifecycle status.
    pub status: RunStateName,
    /// Legacy plain-text transcript.
    pub content: String,
    /// Typed transcript projection when advertised by the daemon.
    pub typed: Option<TypedTranscript>,
}

impl From<TranscriptReadResult> for TranscriptView {
    fn from(transcript: TranscriptReadResult) -> Self {
        Self {
            run_id: transcript.run_id,
            status: transcript.status,
            content: transcript.transcript,
            typed: transcript.typed,
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
/// Availability state for the selected transcript.
pub enum TranscriptState {
    /// No transcript is selected.
    None,
    /// Transcript readback loaded successfully.
    Loaded(TranscriptView),
    /// Transcript readback failed.
    Unavailable {
        /// Run or session identifier requested from the daemon.
        run_id: String,
        /// Rendered readback failure.
        error: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Active run shown in the selected session.
pub struct ActiveRunView {
    /// Active run identifier.
    pub run_id: String,
    /// Current run lifecycle status.
    pub status: RunStateName,
}

impl ActiveRunView {
    /// Creates an active-run view.
    pub fn new(run_id: String, status: RunStateName) -> Self {
        Self { run_id, status }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// One transient event row in the live transcript.
pub struct LiveEventLine {
    /// Run that owns the row, when known.
    pub run_id: Option<String>,
    /// Stream offset that produced the row, when known.
    pub offset: Option<u64>,
    /// Semantic row kind.
    pub kind: LiveEventKind,
    /// Text rendered for the row.
    pub text: String,
}

impl LiveEventLine {
    /// Creates a user-message row.
    pub fn user(text: impl Into<String>) -> Self {
        Self {
            run_id: None,
            offset: None,
            kind: LiveEventKind::User,
            text: text.into(),
        }
    }

    /// Creates a complete assistant-message row.
    pub fn assistant(offset: Option<u64>, text: impl Into<String>) -> Self {
        Self {
            run_id: None,
            offset,
            kind: LiveEventKind::Assistant,
            text: text.into(),
        }
    }

    /// Creates a streaming assistant-delta row.
    pub fn assistant_delta(offset: Option<u64>, text: impl Into<String>) -> Self {
        Self {
            run_id: None,
            offset,
            kind: LiveEventKind::AssistantDelta,
            text: text.into(),
        }
    }

    /// Creates a tool-event row.
    pub fn tool(offset: Option<u64>, text: impl Into<String>) -> Self {
        Self {
            run_id: None,
            offset,
            kind: LiveEventKind::Tool,
            text: text.into(),
        }
    }

    /// Creates an approval-decision row.
    pub fn approval(offset: Option<u64>, text: impl Into<String>) -> Self {
        Self {
            run_id: None,
            offset,
            kind: LiveEventKind::Approval,
            text: text.into(),
        }
    }

    /// Creates a status row.
    pub fn status(offset: Option<u64>, text: impl Into<String>) -> Self {
        Self {
            run_id: None,
            offset,
            kind: LiveEventKind::Status,
            text: text.into(),
        }
    }

    /// Creates a warning row.
    pub fn warning(offset: Option<u64>, text: impl Into<String>) -> Self {
        Self {
            run_id: None,
            offset,
            kind: LiveEventKind::Warning,
            text: text.into(),
        }
    }

    pub(super) fn with_run_id(mut self, run_id: impl Into<String>) -> Self {
        self.run_id = Some(run_id.into());
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Semantic kind of a live event row.
pub enum LiveEventKind {
    /// User message.
    User,
    /// Complete assistant message.
    Assistant,
    /// Streaming assistant delta.
    AssistantDelta,
    /// Tool activity.
    Tool,
    /// Accepted approval decision.
    Approval,
    /// Run or client status.
    Status,
    /// Recoverable warning or failure.
    Warning,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_equality_tracks_observable_composer_state_and_edit_history() {
        let mut state = TuiState::disconnected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            "offline".into(),
        );
        state.set_composer_text("alpha beta");
        state.composer.move_cursor(CursorMove::WordBack);
        state.composer.start_selection();
        state.composer.move_cursor(CursorMove::End);
        state.composer.set_yank_text("saved");

        assert_eq!(state, state.clone());

        let mut different_text = state.clone();
        different_text.composer.insert_char('!');
        assert_ne!(state, different_text);

        let mut different_cursor = state.clone();
        different_cursor.composer.cancel_selection();
        different_cursor.composer.move_cursor(CursorMove::Back);
        assert_ne!(state, different_cursor);

        let mut different_selection = state.clone();
        different_selection.composer.cancel_selection();
        assert_ne!(state, different_selection);

        let mut different_yank = state.clone();
        different_yank.composer.set_yank_text("other");
        assert_ne!(state, different_yank);

        let mut different_history = state.clone();
        different_history.composer.cancel_selection();
        different_history.composer.insert_char('!');
        assert!(different_history.composer.undo());
        let mut same_surface = state.clone();
        same_surface.composer.cancel_selection();
        assert_eq!(
            different_history.composer.lines(),
            same_surface.composer.lines()
        );
        assert_eq!(
            different_history.composer.cursor(),
            same_surface.composer.cursor()
        );
        assert_ne!(different_history, same_surface);
    }

    #[test]
    fn connected_state_restores_known_and_unknown_served_model_status() {
        for status in [
            ModelIdentityStatus::Responded {
                served_model: Some("openai/gpt-5.2-2026-08-01".into()),
            },
            ModelIdentityStatus::Responded { served_model: None },
        ] {
            let transcript = TranscriptState::Loaded(
                TranscriptReadResult {
                    run_id: "run_1".into(),
                    status: RunStateName::Finished,
                    final_answer: Some("done".into()),
                    transcript: "[turn_1] assistant: done\n".into(),
                    typed: Some(TypedTranscript {
                        runs: vec![platonic_protocol::TypedRun {
                            run_id: "run_1".into(),
                            session_index: 0,
                            status: RunStateName::Finished,
                            model_status: Some(status.clone()),
                            entries: vec![],
                        }],
                    }),
                    pending_approval: None,
                    completion_claim: None,
                }
                .into(),
            );

            let state = TuiState::connected(
                "/tmp/workspace".into(),
                "/tmp/agent.sock".into(),
                HelloResult {
                    daemon_version: "0.1.0".into(),
                    workspace_id: "workspace-1".into(),
                    ledger_path: "/tmp/agent.db".into(),
                    capabilities: vec![],
                    daemon_scope: None,
                },
                vec![],
                transcript,
            );

            assert_eq!(state.active_model, Some(status));
        }
    }

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
    fn session_picker_ranks_prefixes_before_mid_question_subsequences() {
        let sessions = vec![
            session("session_mid", "Plan the deploy checklist"),
            session("session_prefix", "Deploy the release"),
            session("session_other", "Review documentation"),
        ];
        let mut picker = SessionPickerView {
            filter: "DEPLOY".into(),
            selected: 0,
        };

        assert_eq!(
            session_ids(picker.matching_sessions(&sessions)),
            vec!["session_prefix", "session_mid"]
        );

        picker.filter = "checklist".into();
        assert_eq!(
            session_ids(picker.matching_sessions(&sessions)),
            vec!["session_mid"]
        );
    }

    #[test]
    fn session_picker_preserves_source_order_for_equal_scores_across_repeated_runs() {
        let sessions = vec![
            session("session_1", "Review deterministic matching"),
            session("session_2", "Review deterministic matching"),
            session("session_3", "Review deterministic matching"),
        ];
        let picker = SessionPickerView {
            filter: "rdm".into(),
            selected: 0,
        };
        let expected = vec!["session_1", "session_2", "session_3"];

        for _ in 0..32 {
            assert_eq!(session_ids(picker.matching_sessions(&sessions)), expected);
        }
    }

    #[test]
    fn session_picker_uses_nucleo_case_folding_without_unicode_normalization() {
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
        assert_eq!(
            session_ids(picker.matching_sessions(&sessions)),
            vec!["session_2"]
        );

        picker.filter = "i\u{307}stanbul".into();
        assert_eq!(
            session_ids(picker.matching_sessions(&sessions)),
            vec!["session_2"]
        );
    }

    #[test]
    fn session_picker_filters_first_question_with_legacy_and_id_recovery_fallbacks() {
        let sessions = vec![
            session_with_questions("session_first", "Plan the release", "approved, go ahead"),
            session_with_questions("session_legacy", "", "Legacy latest question"),
        ];
        let mut picker = SessionPickerView {
            filter: "release".into(),
            selected: 0,
        };

        assert_eq!(
            session_ids(picker.matching_sessions(&sessions)),
            vec!["session_first"]
        );
        picker.filter = "approved".into();
        assert!(picker.matching_sessions(&sessions).is_empty());
        picker.filter = "legacy latest".into();
        assert_eq!(
            session_ids(picker.matching_sessions(&sessions)),
            vec!["session_legacy"]
        );
        picker.filter = "SESSION_FIRST".into();
        assert_eq!(
            session_ids(picker.matching_sessions(&sessions)),
            vec!["session_first"]
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
        session_with_questions(session_id, latest_question, latest_question)
    }

    fn session_with_questions(
        session_id: &str,
        first_question: &str,
        latest_question: &str,
    ) -> SessionSummary {
        SessionSummary {
            session_id: session_id.into(),
            run_id: format!("run_{session_id}"),
            status: RunStateName::Finished,
            latest_question: latest_question.into(),
            first_question: first_question.into(),
            updated_at_ms: 1,
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
