use ratatui::{
    Frame, Terminal,
    backend::TestBackend,
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Modifier, Style},
    symbols,
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph, Widget, Wrap},
};

use super::{
    ApprovalModalView, ConnectionState, LiveEventKind, TranscriptState, TuiState,
    markdown::{DEFAULT_SYNTAX_THEME, MarkdownRenderer, SyntaxTheme},
    state::{
        CachedLiveEventRows, CachedTranscriptRows, DisplayMode, FooterMode, LiveEventRowsKey,
        MotionMode, TranscriptRowsKey, thread_live_label,
    },
};
use crate::{
    color::{self, SemanticRole},
    commands::{
        FooterHintPriority, FooterHintWhen, KEY_MAP, KeyAction, KeyBinding, KeyLabelPlatform,
        KeyMap, SLASH_COMMANDS, SlashCommandAction, matching_slash_commands,
    },
};
use platonic_protocol::{
    ApprovalProfile, DaemonStatusResult, DaemonStatusTokenUsage, ModelIdentityStatus, RunStateName,
    TypedRun, TypedTranscriptEntry,
};
use std::time::{SystemTime, UNIX_EPOCH};
use unicode_width::UnicodeWidthChar;

#[cfg(test)]
mod doc_capture;

const THREAD_STATE_WIDTH: usize = 8;
const WORKING_FRAMES: [&str; 8] = ["⣾", "⣽", "⣻", "⢿", "⡿", "⣟", "⣯", "⣷"];
const WORKING_FRAME_MILLIS: u64 = 80;
const CURSOR_PROBE: Modifier = Modifier::RAPID_BLINK;
const FOOTER_HELP_WIDTH: u16 = 40;
const FOOTER_QUEUE_WIDTH: u16 = 80;
const FOOTER_CONTEXT_WIDTH: u16 = 120;

/// Renders the current client state into a terminal frame.
pub fn render(frame: &mut Frame<'_>, state: &TuiState) {
    render_overlay_at(frame, state, 0, unix_now_ms());
}

pub(crate) fn render_main(frame: &mut Frame<'_>, state: &TuiState) {
    let [history, _, composer, footer] = vertical(frame.area(), state);
    render_live_history(frame, history, state);
    render_composer(frame, composer, state);
    render_footer(frame, footer, state);
}

pub(crate) fn render_overlay(
    frame: &mut Frame<'_>,
    state: &TuiState,
    history_scroll_offset: usize,
) {
    render_overlay_at(frame, state, history_scroll_offset, unix_now_ms());
}

fn render_overlay_at(
    frame: &mut Frame<'_>,
    state: &TuiState,
    history_scroll_offset: usize,
    _now_ms: u64,
) {
    let [history, approval, composer, footer] = vertical(frame.area(), state);
    render_history(frame, history, state, history_scroll_offset);
    if let Some(approval_view) = &state.approval {
        render_approval_pane(frame, approval, approval_view, state.approval_scroll_offset);
    }
    render_composer(frame, composer, state);
    render_footer(frame, footer, state);
    if state.footer_mode() == FooterMode::Shortcuts {
        render_shortcuts_overlay(frame, history);
    }
    if state.session_picker.is_some() {
        render_session_picker(frame, frame.area(), state);
    }
    if let Some(status) = &state.status_modal {
        render_status_modal(frame, frame.area(), status);
    }
}

fn chrome_style() -> Style {
    Style::default().add_modifier(Modifier::DIM)
}

fn semantic_style(role: SemanticRole) -> Style {
    color::active().semantic_style(role)
}

fn accent_style() -> Style {
    color::active().accent_style()
}

fn selected_row_style() -> Style {
    accent_style().add_modifier(Modifier::REVERSED)
}

fn user_message_style() -> Style {
    color::active().user_message_style()
}

/// Renders client state into a plain-text test snapshot of the requested size.
pub fn render_snapshot(state: &TuiState, width: u16, height: u16) -> std::io::Result<String> {
    render_snapshot_at(state, width, height, unix_now_ms())
}

fn render_snapshot_at(
    state: &TuiState,
    width: u16,
    height: u16,
    now_ms: u64,
) -> std::io::Result<String> {
    let backend = TestBackend::new(width, height);
    let mut terminal = Terminal::new(backend)?;
    terminal.draw(|frame| render_overlay_at(frame, state, 0, now_ms))?;
    let buffer = terminal.backend().buffer();
    let area = buffer.area;
    let mut output = String::new();
    for y in area.top()..area.bottom() {
        for x in area.left()..area.right() {
            output.push_str(buffer[(x, y)].symbol());
        }
        output.push('\n');
    }
    Ok(output)
}

fn render_history(frame: &mut Frame<'_>, area: Rect, state: &TuiState, scroll_offset: usize) {
    let mut lines = history_lines(state, area.width);
    if lines.is_empty() {
        lines.push(Line::from(""));
    }
    let paragraph = Paragraph::new(lines).wrap(Wrap { trim: false });
    let bottom = paragraph
        .line_count(area.width.max(1))
        .saturating_sub(area.height as usize);
    let scroll = bottom.saturating_sub(scroll_offset);
    frame.render_widget(
        paragraph.scroll((u16::try_from(scroll).unwrap_or(u16::MAX), 0)),
        area,
    );
}

fn render_live_history(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    let mut lines = main_history_lines(state, area.width);
    if lines.is_empty() {
        lines.push(Line::from(""));
    }
    let paragraph = Paragraph::new(lines).wrap(Wrap { trim: false });
    let scroll = paragraph
        .line_count(area.width.max(1))
        .saturating_sub(area.height as usize);
    frame.render_widget(
        paragraph.scroll((u16::try_from(scroll).unwrap_or(u16::MAX), 0)),
        area,
    );
}

fn main_history_lines(state: &TuiState, width: u16) -> Vec<Line<'static>> {
    let TranscriptState::Loaded(transcript) = &state.transcript else {
        return conversation_history_lines(state, width, DEFAULT_SYNTAX_THEME);
    };
    let mut lines = Vec::new();
    append_conversation_activity(
        &mut lines,
        state,
        Some(transcript),
        width,
        DEFAULT_SYNTAX_THEME,
    );
    append_queue_preview(&mut lines, state);
    lines
}

pub(crate) fn committed_transcript_lines(
    state: &TuiState,
    transcript: &super::TranscriptView,
    width: u16,
) -> Vec<Line<'static>> {
    conversation_transcript_lines(
        transcript,
        width,
        DEFAULT_SYNTAX_THEME,
        &state.history_rows.markdown,
    )
}

fn history_lines(state: &TuiState, width: u16) -> Vec<Line<'static>> {
    history_lines_with_theme(state, width, DEFAULT_SYNTAX_THEME)
}

fn history_lines_with_theme(
    state: &TuiState,
    width: u16,
    syntax_theme: SyntaxTheme,
) -> Vec<Line<'static>> {
    match state.display_mode {
        DisplayMode::Conversation => conversation_history_lines(state, width, syntax_theme),
        DisplayMode::Audit => audit_history_lines(state, width, syntax_theme),
    }
}

fn audit_history_lines(
    state: &TuiState,
    width: u16,
    syntax_theme: SyntaxTheme,
) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    match &state.transcript {
        TranscriptState::Loaded(transcript) => {
            lines.push(status_row(format!("run {}", transcript.run_id)));
            lines.push(Line::from(""));
            append_transcript_rows(
                &mut lines,
                state,
                transcript,
                width,
                DisplayMode::Audit,
                syntax_theme,
            );
        }
        TranscriptState::Unavailable { run_id, error } => {
            clear_transcript_rows(state);
            lines.push(Line::from(vec![
                Span::styled(
                    "transcript unavailable ",
                    semantic_style(SemanticRole::Warning),
                ),
                Span::raw(run_id.clone()),
            ]));
            lines.push(Line::from(error.clone()));
        }
        TranscriptState::None if matches!(state.connection, ConnectionState::Connected { .. }) => {
            clear_transcript_rows(state);
            lines.extend(intro_lines(state));
        }
        TranscriptState::None => {
            clear_transcript_rows(state);
            lines.push(Line::from(vec![Span::styled(
                "daemon unavailable",
                semantic_style(SemanticRole::Error).add_modifier(Modifier::BOLD),
            )]));
            if let ConnectionState::Disconnected { error } = &state.connection {
                lines.push(Line::from(error.clone()));
            }
            lines.push(Line::from(""));
            lines.push(Line::from(
                "Quit and run plato --tui to ensure the host daemon.",
            ));
            lines.push(Line::from(
                "Or start platonic serve, then press r to reconnect.",
            ));
        }
    }

    append_audit_live_transcript(&mut lines, state, width, syntax_theme);
    append_queue_preview(&mut lines, state);
    lines
}

fn conversation_history_lines(
    state: &TuiState,
    width: u16,
    syntax_theme: SyntaxTheme,
) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    let mut latest_transcript = None;
    match &state.transcript {
        TranscriptState::Loaded(transcript) => {
            append_transcript_rows(
                &mut lines,
                state,
                transcript,
                width,
                DisplayMode::Conversation,
                syntax_theme,
            );
            latest_transcript = Some(transcript);
        }
        TranscriptState::Unavailable { run_id, error } => {
            clear_transcript_rows(state);
            lines.push(Line::from(vec![Span::styled(
                "Transcript unavailable",
                semantic_style(SemanticRole::Warning),
            )]));
            lines.push(Line::from(error.replace(run_id, "selected run")));
        }
        TranscriptState::None if matches!(state.connection, ConnectionState::Connected { .. }) => {
            clear_transcript_rows(state);
            lines.extend(intro_lines(state));
        }
        TranscriptState::None => {
            clear_transcript_rows(state);
            lines.push(Line::from(vec![Span::styled(
                "daemon unavailable",
                semantic_style(SemanticRole::Error).add_modifier(Modifier::BOLD),
            )]));
            if let ConnectionState::Disconnected { error } = &state.connection {
                lines.push(Line::from(error.clone()));
            }
            lines.push(Line::from(""));
            lines.push(Line::from(
                "Quit and run plato --tui to ensure the host daemon.",
            ));
            lines.push(Line::from(
                "Or start platonic serve, then press r to reconnect.",
            ));
        }
    }

    append_conversation_activity(&mut lines, state, latest_transcript, width, syntax_theme);
    append_queue_preview(&mut lines, state);
    lines
}

fn clear_transcript_rows(state: &TuiState) {
    if state
        .history_rows
        .transcript
        .read()
        .expect("transcript row cache lock poisoned")
        .is_some()
    {
        let _ = state
            .history_rows
            .transcript
            .write()
            .expect("transcript row cache lock poisoned")
            .take();
    }
}

fn append_transcript_rows(
    lines: &mut Vec<Line<'static>>,
    state: &TuiState,
    transcript: &super::TranscriptView,
    width: u16,
    mode: DisplayMode,
    syntax_theme: SyntaxTheme,
) {
    let changed = state
        .history_rows
        .transcript
        .read()
        .expect("transcript row cache lock poisoned")
        .as_ref()
        .is_none_or(|cached| {
            &cached.key.source != transcript
                || cached.key.width != width
                || cached.key.display_mode != mode
                || cached.key.syntax_theme != syntax_theme
        });
    if changed {
        let key = TranscriptRowsKey {
            source: transcript.clone(),
            width,
            display_mode: mode,
            syntax_theme,
        };
        let rows = match mode {
            DisplayMode::Conversation => conversation_transcript_lines(
                transcript,
                width,
                syntax_theme,
                &state.history_rows.markdown,
            ),
            DisplayMode::Audit => readback_lines(&transcript.content),
        };
        let cached = CachedTranscriptRows { key, rows };
        *state
            .history_rows
            .transcript
            .write()
            .expect("transcript row cache lock poisoned") = Some(cached);
    }
    let cached = state
        .history_rows
        .transcript
        .read()
        .expect("transcript row cache lock poisoned");
    let rows = &cached
        .as_ref()
        .expect("transcript rows were initialized")
        .rows;
    if mode == DisplayMode::Conversation {
        append_spaced_rows(lines, rows.iter().cloned());
    } else {
        lines.extend(rows.iter().cloned());
    }
}

fn intro_lines(state: &TuiState) -> Vec<Line<'static>> {
    let mut lines = vec![
        Line::from(vec![Span::styled("Plato Agent", accent_style())]),
        Line::from("Local Rust agent runtime"),
        Line::from(""),
    ];

    if let ConnectionState::Connected {
        workspace_id,
        daemon_version,
        ledger_path,
    } = &state.connection
    {
        lines.extend([
            Line::from(vec![
                Span::styled("workspace ", chrome_style()),
                Span::raw(workspace_id.clone()),
            ]),
            Line::from(vec![
                Span::styled("daemon    ", chrome_style()),
                Span::raw(daemon_identity_label(daemon_version)),
            ]),
            Line::from(vec![
                Span::styled("ledger    ", chrome_style()),
                Span::raw(ledger_path.clone()),
            ]),
            Line::from(vec![
                Span::styled("cwd       ", chrome_style()),
                Span::raw(state.workspace_root.clone()),
            ]),
            Line::from(""),
            Line::from(format!(
                "{} session{}",
                state.sessions.len(),
                plural(state.sessions.len())
            )),
        ]);
    }

    lines
}

fn append_audit_live_transcript(
    lines: &mut Vec<Line<'static>>,
    state: &TuiState,
    width: u16,
    syntax_theme: SyntaxTheme,
) {
    let has_activity = state.active_run.is_some()
        || state.status_message.is_some()
        || state.stream_warning.is_some()
        || !state.live_events.is_empty();
    if !has_activity {
        clear_live_event_rows(state);
        return;
    }

    lines.push(Line::from(""));
    lines.push(Line::from(vec![Span::styled(
        "transcript",
        semantic_style(SemanticRole::Primary),
    )]));

    if let Some(active) = &state.active_run {
        lines.push(status_row(format!("{} {}", active.status, active.run_id)));
    }
    if let Some(elapsed) = issue_prep_activity(state) {
        lines.push(status_row(format!(
            "issue prep {}",
            format_elapsed(elapsed)
        )));
    } else if let Some(message) = &state.status_message {
        lines.push(status_row(message.clone()));
    }
    if let Some(warning) = &state.stream_warning {
        lines.push(warning_row(format!("stream warning {warning}")));
    }
    append_audit_live_event_rows(lines, state, width, syntax_theme);
    append_working_row(lines, state);
}

fn clear_live_event_rows(state: &TuiState) {
    if state
        .history_rows
        .live_events
        .read()
        .expect("live event row cache lock poisoned")
        .is_some()
    {
        let _ = state
            .history_rows
            .live_events
            .write()
            .expect("live event row cache lock poisoned")
            .take();
    }
}

fn append_audit_live_event_rows(
    lines: &mut Vec<Line<'static>>,
    state: &TuiState,
    width: u16,
    syntax_theme: SyntaxTheme,
) {
    let changed = state
        .history_rows
        .live_events
        .read()
        .expect("live event row cache lock poisoned")
        .as_ref()
        .is_none_or(|cached| {
            cached.key.source != state.live_events
                || !cached.key.committed.is_empty()
                || cached.key.width != width
                || cached.key.display_mode != DisplayMode::Audit
                || cached.key.syntax_theme != syntax_theme
        });
    if changed {
        let key = LiveEventRowsKey {
            source: state.live_events.clone(),
            committed: Vec::new(),
            width,
            display_mode: DisplayMode::Audit,
            syntax_theme,
        };
        let rows = state.live_events.iter().flat_map(event_rows).collect();
        let cached = CachedLiveEventRows { key, rows };
        *state
            .history_rows
            .live_events
            .write()
            .expect("live event row cache lock poisoned") = Some(cached);
    }
    let cached = state
        .history_rows
        .live_events
        .read()
        .expect("live event row cache lock poisoned");
    let rows = &cached
        .as_ref()
        .expect("live event rows were initialized")
        .rows;
    lines.extend(rows.iter().cloned());
}

fn conversation_transcript_lines(
    transcript: &super::TranscriptView,
    width: u16,
    syntax_theme: SyntaxTheme,
    markdown: &MarkdownRenderer,
) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    if let Some(typed) = &transcript.typed {
        for run in &typed.runs {
            for entry in &run.entries {
                match entry {
                    TypedTranscriptEntry::User { text } => {
                        push_message_rows(
                            &mut lines,
                            LiveEventKind::User,
                            text,
                            width,
                            syntax_theme,
                            markdown,
                            true,
                        );
                    }
                    TypedTranscriptEntry::Assistant { text } => {
                        push_message_rows(
                            &mut lines,
                            LiveEventKind::Assistant,
                            text,
                            width,
                            syntax_theme,
                            markdown,
                            true,
                        );
                    }
                    TypedTranscriptEntry::ToolCall { .. }
                    | TypedTranscriptEntry::ToolResult { .. }
                    | TypedTranscriptEntry::Approval { .. }
                    | TypedTranscriptEntry::PolicyDenied { .. }
                    | TypedTranscriptEntry::ToolFailed { .. } => {}
                }
            }
            if run.run_id != transcript.run_id
                && let Some(summary) =
                    trace_summary(Some(run), &[], None, Some(run.status), false, false)
            {
                push_trace_row(&mut lines, summary);
            }
        }
    } else {
        for line in transcript.content.lines() {
            if let Some(text) = turn_text(line, "user: ") {
                push_message_rows(
                    &mut lines,
                    LiveEventKind::User,
                    text,
                    width,
                    syntax_theme,
                    markdown,
                    false,
                );
            } else if let Some(text) = turn_text(line, "assistant: ") {
                push_message_rows(
                    &mut lines,
                    LiveEventKind::Assistant,
                    text,
                    width,
                    syntax_theme,
                    markdown,
                    false,
                );
            }
        }
    }
    lines
}

fn append_conversation_activity(
    lines: &mut Vec<Line<'static>>,
    state: &TuiState,
    transcript: Option<&super::TranscriptView>,
    width: u16,
    syntax_theme: SyntaxTheme,
) {
    let latest_run = transcript.and_then(latest_typed_run);
    let live_run_id = state
        .active_run
        .as_ref()
        .map(|run| run.run_id.as_str())
        .or_else(|| {
            state
                .live_events
                .iter()
                .rev()
                .find_map(|event| event.run_id.as_deref())
        });
    let live_matches_latest = transcript
        .is_some_and(|transcript| live_run_id.is_some_and(|run_id| run_id == transcript.run_id));

    if !live_matches_latest
        && let Some(transcript) = transcript
        && let Some(summary) =
            trace_summary(latest_run, &[], None, Some(transcript.status), false, false)
    {
        push_trace_row(lines, summary);
    }

    if state.live_events.is_empty() {
        clear_live_event_rows(state);
    } else {
        let committed = committed_message_keys(transcript);
        append_conversation_live_event_rows(lines, state, committed, width, syntax_theme);
    }

    let live_status = state
        .active_run
        .as_ref()
        .filter(|run| live_run_id == Some(run.run_id.as_str()))
        .map(|run| run.status)
        .or_else(|| {
            live_matches_latest
                .then(|| transcript.map(|transcript| transcript.status))
                .flatten()
        });
    let typed = live_matches_latest.then_some(latest_run).flatten();
    if let Some(summary) = trace_summary(
        typed,
        &state.live_events,
        live_run_id,
        live_status,
        state.stream_warning.is_some(),
        state.approval.is_some(),
    ) {
        push_trace_row(lines, summary);
    }
    append_working_row(lines, state);
}

fn latest_typed_run(transcript: &super::TranscriptView) -> Option<&TypedRun> {
    transcript
        .typed
        .as_ref()?
        .runs
        .iter()
        .find(|run| run.run_id == transcript.run_id)
}

fn committed_message_keys(
    transcript: Option<&super::TranscriptView>,
) -> Vec<(String, LiveEventKind, String)> {
    let Some(transcript) = transcript else {
        return Vec::new();
    };
    if let Some(typed) = &transcript.typed {
        return typed
            .runs
            .iter()
            .flat_map(|run| {
                run.entries.iter().filter_map(|entry| match entry {
                    TypedTranscriptEntry::User { text } => {
                        Some((run.run_id.clone(), LiveEventKind::User, text.clone()))
                    }
                    TypedTranscriptEntry::Assistant { text } => {
                        Some((run.run_id.clone(), LiveEventKind::Assistant, text.clone()))
                    }
                    TypedTranscriptEntry::ToolCall { .. }
                    | TypedTranscriptEntry::ToolResult { .. }
                    | TypedTranscriptEntry::Approval { .. }
                    | TypedTranscriptEntry::PolicyDenied { .. }
                    | TypedTranscriptEntry::ToolFailed { .. } => None,
                })
            })
            .collect();
    }
    transcript
        .content
        .lines()
        .filter_map(|line| {
            turn_text(line, "user: ")
                .map(|text| {
                    (
                        transcript.run_id.clone(),
                        LiveEventKind::User,
                        text.to_owned(),
                    )
                })
                .or_else(|| {
                    turn_text(line, "assistant: ").map(|text| {
                        (
                            transcript.run_id.clone(),
                            LiveEventKind::Assistant,
                            text.to_owned(),
                        )
                    })
                })
        })
        .collect()
}

fn append_conversation_live_event_rows(
    lines: &mut Vec<Line<'static>>,
    state: &TuiState,
    committed: Vec<(String, LiveEventKind, String)>,
    width: u16,
    syntax_theme: SyntaxTheme,
) {
    let changed = state
        .history_rows
        .live_events
        .read()
        .expect("live event row cache lock poisoned")
        .as_ref()
        .is_none_or(|cached| {
            cached.key.source != state.live_events
                || cached.key.committed != committed
                || cached.key.width != width
                || cached.key.display_mode != DisplayMode::Conversation
                || cached.key.syntax_theme != syntax_theme
        });
    if changed {
        let key = LiveEventRowsKey {
            source: state.live_events.clone(),
            committed,
            width,
            display_mode: DisplayMode::Conversation,
            syntax_theme,
        };
        let rows = conversation_live_event_lines(
            &state.live_events,
            &key.committed,
            width,
            syntax_theme,
            &state.history_rows.markdown,
        );
        let cached = CachedLiveEventRows { key, rows };
        *state
            .history_rows
            .live_events
            .write()
            .expect("live event row cache lock poisoned") = Some(cached);
    }
    let cached = state
        .history_rows
        .live_events
        .read()
        .expect("live event row cache lock poisoned");
    let rows = &cached
        .as_ref()
        .expect("live event rows were initialized")
        .rows;
    append_spaced_rows(lines, rows.iter().cloned());
}

fn conversation_live_event_lines(
    events: &[super::LiveEventLine],
    committed: &[(String, LiveEventKind, String)],
    width: u16,
    syntax_theme: SyntaxTheme,
    markdown: &MarkdownRenderer,
) -> Vec<Line<'static>> {
    let mut committed = committed.to_vec();
    let mut lines = Vec::new();
    for event in events {
        let kind = match event.kind {
            LiveEventKind::User => LiveEventKind::User,
            LiveEventKind::Assistant | LiveEventKind::AssistantDelta => LiveEventKind::Assistant,
            LiveEventKind::Status | LiveEventKind::Warning
                if event.offset.is_none()
                    && ((event.kind == LiveEventKind::Warning
                        && (event.text.starts_with("issue prep")
                            || event.text.starts_with("issue-prep")
                            || event.text.starts_with("thread send rejected:")
                            || event.text.starts_with("voice ")))
                        || (event.kind == LiveEventKind::Status
                            && (event.text.starts_with("issue-prep artifacts:")
                                || event.text.starts_with("voice ")))) =>
            {
                let role = if event.kind == LiveEventKind::Warning {
                    SemanticRole::Error
                } else {
                    SemanticRole::Muted
                };
                push_notice_rows(&mut lines, &event.text, role);
                continue;
            }
            LiveEventKind::Tool
            | LiveEventKind::Approval
            | LiveEventKind::Status
            | LiveEventKind::Warning => continue,
        };
        if let Some(run_id) = event.run_id.as_deref()
            && let Some(index) =
                committed
                    .iter()
                    .position(|(committed_run_id, committed_kind, text)| {
                        committed_run_id == run_id && *committed_kind == kind && text == &event.text
                    })
        {
            committed.remove(index);
            continue;
        }
        push_message_rows(
            &mut lines,
            kind,
            &event.text,
            width,
            syntax_theme,
            markdown,
            true,
        );
    }
    lines
}

fn push_notice_rows(lines: &mut Vec<Line<'static>>, text: &str, role: SemanticRole) {
    if !lines.is_empty() {
        lines.push(Line::from(""));
    }
    let style = semantic_style(role);
    lines.push(Line::from(Span::styled(
        "Notice",
        style.add_modifier(Modifier::BOLD),
    )));
    lines.extend(
        text.lines()
            .map(|line| Line::from(vec![Span::raw("  "), Span::styled(line.to_owned(), style)])),
    );
}

fn push_message_rows(
    lines: &mut Vec<Line<'static>>,
    kind: LiveEventKind,
    text: &str,
    width: u16,
    syntax_theme: SyntaxTheme,
    markdown: &MarkdownRenderer,
    assistant_markdown: bool,
) {
    if text.is_empty()
        && matches!(
            kind,
            LiveEventKind::Assistant | LiveEventKind::AssistantDelta
        )
    {
        return;
    }
    if !lines.is_empty() {
        lines.push(Line::from(""));
    }
    let (label, label_style) = match kind {
        LiveEventKind::User => ("You", accent_style()),
        LiveEventKind::Assistant | LiveEventKind::AssistantDelta => (
            "Plato",
            semantic_style(SemanticRole::Success).add_modifier(Modifier::BOLD),
        ),
        LiveEventKind::Tool
        | LiveEventKind::Approval
        | LiveEventKind::Status
        | LiveEventKind::Warning => return,
    };
    lines.push(Line::from(Span::styled(label, label_style)));
    match kind {
        LiveEventKind::User => {
            let mut text_lines = text.lines().peekable();
            if text_lines.peek().is_none() {
                lines.push(Line::from("  ").style(user_message_style()));
            } else {
                lines.extend(text_lines.map(|line| {
                    Line::from(vec![Span::raw("  "), Span::raw(line.to_owned())])
                        .style(user_message_style())
                }));
            }
        }
        LiveEventKind::Assistant | LiveEventKind::AssistantDelta => {
            if assistant_markdown {
                lines.extend(markdown.render(text, width, syntax_theme));
            } else {
                lines.extend(
                    text.lines()
                        .map(|line| Line::from(vec![Span::raw("  "), Span::raw(line.to_owned())])),
                );
            }
        }
        LiveEventKind::Tool
        | LiveEventKind::Approval
        | LiveEventKind::Status
        | LiveEventKind::Warning => {}
    }
}

fn trace_summary(
    typed: Option<&TypedRun>,
    live_events: &[super::LiveEventLine],
    live_run_id: Option<&str>,
    status: Option<RunStateName>,
    stream_warning: bool,
    approval_pending: bool,
) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(run) = typed {
        for entry in &run.entries {
            match entry {
                TypedTranscriptEntry::ToolCall { .. } | TypedTranscriptEntry::ToolResult { .. } => {
                    push_trace_part(&mut parts, "tools");
                }
                TypedTranscriptEntry::Approval { .. } => {
                    push_trace_part(&mut parts, "approval");
                }
                TypedTranscriptEntry::PolicyDenied { .. } => {
                    push_trace_part(&mut parts, "policy denied");
                }
                TypedTranscriptEntry::ToolFailed { .. } => {
                    push_trace_part(&mut parts, "tool failed");
                }
                TypedTranscriptEntry::User { .. } | TypedTranscriptEntry::Assistant { .. } => {}
            }
        }
    }
    let mut has_live_detail = false;
    let mut approval_warning = false;
    for event in live_events {
        if event.run_id.as_deref() != live_run_id {
            continue;
        }
        if event.offset.is_none()
            && (event.text.starts_with("issue prep") || event.text.starts_with("issue-prep"))
        {
            continue;
        }
        match event.kind {
            LiveEventKind::Tool => {
                has_live_detail = true;
                push_trace_part(&mut parts, "tools");
            }
            LiveEventKind::Approval => {
                has_live_detail = true;
                approval_warning = false;
                push_trace_part(&mut parts, "approval");
            }
            LiveEventKind::Warning => {
                has_live_detail = true;
                if event.text.starts_with("approval pending ") {
                    approval_warning = true;
                } else {
                    push_trace_part(&mut parts, "warning");
                }
            }
            LiveEventKind::Status => has_live_detail = true,
            LiveEventKind::User | LiveEventKind::Assistant | LiveEventKind::AssistantDelta => {}
        }
    }
    if stream_warning {
        has_live_detail = true;
        push_trace_part(&mut parts, "stream warning");
    }
    if approval_pending {
        has_live_detail = true;
        approval_warning = true;
    }
    if approval_warning {
        push_trace_part(&mut parts, "warning");
    }
    if approval_pending {
        push_trace_part(&mut parts, "approval pending");
    }
    match status {
        Some(RunStateName::Finished) if !parts.is_empty() || has_live_detail => {
            push_trace_part(&mut parts, "finished");
        }
        Some(RunStateName::Failed) => push_trace_part(&mut parts, "failed"),
        Some(RunStateName::Canceled) => push_trace_part(&mut parts, "canceled"),
        Some(RunStateName::CancelRequested) => {
            push_trace_part(&mut parts, "cancel requested");
        }
        Some(RunStateName::Interrupted) => push_trace_part(&mut parts, "interrupted"),
        Some(RunStateName::Running) if !parts.is_empty() || has_live_detail => {
            push_trace_part(&mut parts, "running");
        }
        Some(RunStateName::Finished | RunStateName::Running) | None => {}
    }
    if parts.is_empty() && has_live_detail {
        parts.push("activity");
    }
    (!parts.is_empty()).then(|| parts.join(" | "))
}

fn push_trace_part(parts: &mut Vec<&'static str>, part: &'static str) {
    if !parts.contains(&part) {
        parts.push(part);
    }
}

fn push_trace_row(lines: &mut Vec<Line<'static>>, summary: String) {
    let row = Line::from(vec![
        Span::styled("Trace  ", chrome_style()),
        Span::styled(summary, chrome_style()),
    ]);
    append_spaced_rows(lines, std::iter::once(row));
}

fn append_spaced_rows(
    lines: &mut Vec<Line<'static>>,
    rows: impl IntoIterator<Item = Line<'static>>,
) {
    let rows = rows.into_iter().collect::<Vec<_>>();
    if rows.is_empty() {
        return;
    }
    if lines.last().is_some_and(|line| line.width() > 0) {
        lines.push(Line::from(""));
    }
    lines.extend(rows);
}

fn append_queue_preview(lines: &mut Vec<Line<'static>>, state: &TuiState) {
    if state.queued_messages.is_empty() {
        return;
    }

    lines.push(Line::from(""));
    lines.push(Line::from(vec![Span::styled(
        "queued",
        semantic_style(SemanticRole::Primary),
    )]));
    lines.extend(
        state
            .queued_messages
            .iter()
            .enumerate()
            .map(|(index, message)| Line::from(format!("{} {}", index + 1, message))),
    );
}

fn append_working_row(lines: &mut Vec<Line<'static>>, state: &TuiState) {
    let Some((task, elapsed, interruptible)) = working_task(state) else {
        return;
    };
    let marker = match state.motion_mode {
        MotionMode::Animated => {
            let index = (state.working_elapsed_millis / WORKING_FRAME_MILLIS) as usize;
            WORKING_FRAMES[index % WORKING_FRAMES.len()]
        }
        MotionMode::Reduced => "•",
    };
    let mut spans = vec![
        Span::styled(format!("{marker} "), semantic_style(SemanticRole::Primary)),
        Span::styled(task, Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(format!("  {}", format_elapsed(elapsed))),
    ];
    if interruptible {
        spans.push(Span::styled("  Esc to interrupt", chrome_style()));
    }
    append_spaced_rows(lines, std::iter::once(Line::from(spans)));
}

fn working_task(state: &TuiState) -> Option<(&'static str, u64, bool)> {
    if let Some(elapsed) = issue_prep_activity(state) {
        return Some(("Preparing", elapsed, false));
    }
    state.active_run.as_ref().and_then(|run| {
        matches!(
            run.status,
            RunStateName::Running | RunStateName::CancelRequested
        )
        .then_some(("Working", state.active_run_elapsed_secs.unwrap_or(0), true))
    })
}

fn render_footer(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    frame.render_widget(Paragraph::new(footer_line(state, area.width)), area);
}

fn footer_line(state: &TuiState, width: u16) -> Line<'static> {
    footer_line_with_keymap(state, width, KEY_MAP, KeyLabelPlatform::current())
}

fn footer_line_with_keymap(
    state: &TuiState,
    width: u16,
    key_map: KeyMap<'_>,
    platform: KeyLabelPlatform,
) -> Line<'static> {
    let line = match state.footer_mode() {
        FooterMode::Contextual => contextual_footer(state, width, key_map, platform),
        FooterMode::Shortcuts => shortcut_footer(key_map, platform),
        FooterMode::QuitConfirm => quit_confirm_footer(key_map, platform),
        FooterMode::Offline => offline_footer(key_map, platform),
    };
    truncate_line(line, width)
}

fn contextual_footer(
    state: &TuiState,
    width: u16,
    key_map: KeyMap<'_>,
    platform: KeyLabelPlatform,
) -> Line<'static> {
    let mut spans = Vec::new();
    if state.approval_profile == ApprovalProfile::Yolo {
        spans.push(Span::styled(
            if state.selected_session_id.is_some() {
                "yolo"
            } else {
                "yolo next"
            },
            semantic_style(SemanticRole::Warning).add_modifier(Modifier::BOLD),
        ));
    }
    let hints = footer_hint_spans(
        key_map.bindings().iter().filter(|binding| {
            binding.footer.is_some_and(|hint| {
                hint.priority == FooterHintPriority::Essential
                    && hint.when == FooterHintWhen::Always
            })
        }),
        state,
        platform,
    );
    if !spans.is_empty() && !hints.is_empty() {
        push_footer_separator(&mut spans);
    }
    spans.extend(hints);
    if width < FOOTER_HELP_WIDTH {
        return Line::from(spans);
    }
    if width >= FOOTER_QUEUE_WIDTH {
        append_footer_hints(
            &mut spans,
            key_map.bindings().iter().filter(|binding| {
                binding.footer.is_some_and(|hint| {
                    hint.priority == FooterHintPriority::Queue
                        && (hint.when == FooterHintWhen::Always
                            || active_run_is_interruptible(state))
                })
            }),
            state,
            platform,
        );
    }
    if width >= FOOTER_CONTEXT_WIDTH {
        push_footer_separator(&mut spans);
        spans.push(Span::styled(
            model_status_label(state.active_model.as_ref()),
            chrome_style(),
        ));
        push_footer_separator(&mut spans);
        spans.push(Span::styled(
            format!("workspace {}", state.workspace_root),
            chrome_style(),
        ));
    }
    Line::from(spans)
}

fn shortcut_footer(key_map: KeyMap<'_>, platform: KeyLabelPlatform) -> Line<'static> {
    let mut spans = key_hint_spans(key_map.binding(KeyAction::Shortcuts), "shortcuts", platform);
    push_footer_separator(&mut spans);
    spans.extend(key_hint_spans(
        key_map.binding(KeyAction::Interrupt),
        "close",
        platform,
    ));
    Line::from(spans)
}

fn quit_confirm_footer(key_map: KeyMap<'_>, platform: KeyLabelPlatform) -> Line<'static> {
    let mut spans = vec![Span::styled("press ", chrome_style())];
    spans.extend(key_label_spans(key_map.binding(KeyAction::Quit), platform));
    spans.push(Span::styled(" again to quit", chrome_style()));
    Line::from(spans)
}

fn offline_footer(key_map: KeyMap<'_>, platform: KeyLabelPlatform) -> Line<'static> {
    let mut spans = vec![Span::styled("daemon unavailable — ", chrome_style())];
    spans.extend(key_label_spans(
        key_map.binding(KeyAction::Reconnect),
        platform,
    ));
    spans.push(Span::styled(" to reconnect", chrome_style()));
    Line::from(spans)
}

fn footer_hint_spans<'a>(
    bindings: impl IntoIterator<Item = &'a KeyBinding>,
    state: &TuiState,
    platform: KeyLabelPlatform,
) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    append_footer_hints(&mut spans, bindings, state, platform);
    spans
}

fn append_footer_hints<'a>(
    spans: &mut Vec<Span<'static>>,
    bindings: impl IntoIterator<Item = &'a KeyBinding>,
    state: &TuiState,
    platform: KeyLabelPlatform,
) {
    for binding in bindings {
        if !spans.is_empty() {
            push_footer_separator(spans);
        }
        let description = match binding.action {
            KeyAction::Queue => format!("queue {}", state.queued_messages.len()),
            KeyAction::Interrupt => "interrupt".into(),
            _ => binding.description.into(),
        };
        spans.extend(key_hint_spans(binding, &description, platform));
    }
}

fn key_hint_spans(
    binding: &KeyBinding,
    description: &str,
    platform: KeyLabelPlatform,
) -> Vec<Span<'static>> {
    let mut spans = key_label_spans(binding, platform);
    spans.push(Span::styled(format!(" {description}"), chrome_style()));
    spans
}

fn key_label_spans(binding: &KeyBinding, platform: KeyLabelPlatform) -> Vec<Span<'static>> {
    vec![Span::styled(
        binding.label.text(platform),
        composer_prefix_style(),
    )]
}

fn push_footer_separator(spans: &mut Vec<Span<'static>>) {
    spans.push(Span::styled(" · ", chrome_style()));
}

fn active_run_is_interruptible(state: &TuiState) -> bool {
    state.active_run.as_ref().is_some_and(|run| {
        matches!(
            run.status,
            RunStateName::Running | RunStateName::CancelRequested
        )
    })
}

fn model_status_label(status: Option<&ModelIdentityStatus>) -> String {
    match status {
        Some(ModelIdentityStatus::Requested { model }) => format!("selected {model}"),
        Some(ModelIdentityStatus::Responded {
            served_model: Some(model),
        }) => format!("served {model}"),
        Some(ModelIdentityStatus::Responded { served_model: None }) => "served unknown".into(),
        None => "model pending".into(),
    }
}

fn daemon_identity_label(identity: &str) -> String {
    let mut parts = identity.split_whitespace();
    let Some(version) = parts.next() else {
        return "unknown unknown unknown".into();
    };
    let Some(commit) = parts.next() else {
        return format!("{version} unknown unknown");
    };
    let Some(date) = parts.next() else {
        return format!("{version} {commit} unknown");
    };
    if parts.next().is_some() {
        return identity.into();
    }
    let commit = if commit.len() == 40 && commit.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        &commit[..7]
    } else {
        commit
    };
    format!("{version} {commit} {date}")
}

fn truncate_line(line: Line<'static>, width: u16) -> Line<'static> {
    let width = usize::from(width);
    if line.width() <= width {
        return line;
    }
    if width == 0 {
        return Line::from("");
    }

    let content_width = width - 1;
    let mut used = 0;
    let mut spans = Vec::new();
    'spans: for span in line.spans {
        let mut content = String::new();
        for character in span.content.chars() {
            let character_width = UnicodeWidthChar::width(character).unwrap_or(0);
            if used + character_width > content_width {
                if !content.is_empty() {
                    spans.push(Span::styled(content, span.style));
                }
                break 'spans;
            }
            content.push(character);
            used += character_width;
        }
        if !content.is_empty() {
            spans.push(Span::styled(content, span.style));
        }
        if used >= content_width {
            break;
        }
    }
    spans.push(Span::styled("~", chrome_style()));
    Line::from(spans)
}

fn issue_prep_activity(state: &TuiState) -> Option<u64> {
    state.issue_prep_started_at?;
    Some(state.issue_prep_elapsed_secs.unwrap_or(0))
}

fn render_composer(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    let mut lines = slash_popup_lines(state);
    lines.extend(composer_lines(state));
    frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), area);
    if composer_has_focus(state)
        && let Some(position) = composer_cursor_position(area, state, true)
        && position.0 < area.right()
        && position.1 < area.bottom()
    {
        frame.set_cursor_position(position);
    }
}

fn composer_has_focus(state: &TuiState) -> bool {
    !state.help_visible
        && state.session_picker.is_none()
        && state.approval.is_none()
        && state.status_modal.is_none()
}

fn composer_lines(state: &TuiState) -> Vec<Line<'static>> {
    if state.composer_is_empty() {
        return vec![Line::from(vec![
            Span::styled(">", composer_prefix_style()),
            Span::raw("   "),
            Span::styled("Try \"read README.md and summarize it\"", chrome_style()),
        ])];
    }
    let selection = state.composer.selection_range();
    state
        .composer
        .lines()
        .iter()
        .enumerate()
        .map(|(index, line)| {
            let mut spans = vec![
                Span::styled(composer_prefix(index), composer_prefix_style()),
                Span::raw(" "),
            ];
            spans.extend(composer_text_spans(index, line, selection));
            Line::from(spans)
        })
        .collect()
}

fn composer_text_spans(
    row: usize,
    line: &str,
    selection: Option<((usize, usize), (usize, usize))>,
) -> Vec<Span<'static>> {
    let Some((start, end)) = selection.filter(|(start, end)| start != end) else {
        return vec![Span::raw(line.to_owned())];
    };
    if row < start.0 || row > end.0 {
        return vec![Span::raw(line.to_owned())];
    }
    let start_column = if row == start.0 { start.1 } else { 0 };
    let end_column = if row == end.0 {
        end.1
    } else {
        line.chars().count()
    };
    let start = char_byte_index(line, start_column);
    let end = char_byte_index(line, end_column);
    if start == end {
        return vec![Span::raw(line.to_owned())];
    }
    vec![
        Span::raw(line[..start].to_owned()),
        Span::styled(
            line[start..end].to_owned(),
            Style::default().add_modifier(Modifier::REVERSED),
        ),
        Span::raw(line[end..].to_owned()),
    ]
}

fn char_byte_index(value: &str, column: usize) -> usize {
    value
        .char_indices()
        .map(|(index, _)| index)
        .chain(std::iter::once(value.len()))
        .nth(column)
        .unwrap_or(value.len())
}

fn composer_prefix(index: usize) -> &'static str {
    if index == 0 { ">" } else { "|" }
}

fn composer_prefix_style() -> Style {
    accent_style()
}

fn composer_cursor_position(
    area: Rect,
    state: &TuiState,
    include_popup: bool,
) -> Option<(u16, u16)> {
    if area.is_empty() {
        return None;
    }
    let (lines, after_last_probe) = composer_cursor_probe_lines(state, include_popup);
    let mut buffer = Buffer::empty(area);
    Paragraph::new(lines)
        .wrap(Wrap { trim: false })
        .render(area, &mut buffer);

    let mut probes = area.rows().flat_map(|row| {
        row.columns()
            .filter(|position| buffer[*position].modifier.contains(CURSOR_PROBE))
    });
    if after_last_probe {
        let position = probes.last()?;
        let width = Line::from(buffer[position].symbol()).width().max(1) as u16;
        let next_x = position.x.saturating_add(width);
        if next_x >= area.right() {
            Some((area.left(), position.y.saturating_add(1)))
        } else {
            Some((next_x, position.y))
        }
    } else {
        probes.next().map(|position| (position.x, position.y))
    }
}

fn composer_cursor_probe_lines(
    state: &TuiState,
    include_popup: bool,
) -> (Vec<Line<'static>>, bool) {
    let mut lines = if include_popup {
        slash_popup_lines(state)
    } else {
        Vec::new()
    };
    if state.composer_is_empty() {
        lines.push(Line::from(vec![
            Span::styled(">", composer_prefix_style()),
            Span::raw(" "),
            Span::styled(" ", Style::default().add_modifier(CURSOR_PROBE)),
            Span::raw(" "),
            Span::styled("Try \"read README.md and summarize it\"", chrome_style()),
        ]));
        return (lines, false);
    }

    let (cursor_line, cursor_column) = state.composer.cursor();
    let mut after_last_probe = false;
    lines.extend(
        state
            .composer
            .lines()
            .iter()
            .enumerate()
            .map(|(index, line)| {
                let prefix = Span::styled(composer_prefix(index), composer_prefix_style());
                if index != cursor_line {
                    return Line::from(vec![prefix, Span::raw(format!(" {line}"))]);
                }

                let cursor_in_line = char_byte_index(line, cursor_column);
                let before = &line[..cursor_in_line];
                let after = &line[cursor_in_line..];
                if Line::from(after).width() > 0 {
                    Line::from(vec![
                        prefix,
                        Span::raw(format!(" {before}")),
                        Span::styled(
                            after.to_owned(),
                            Style::default().add_modifier(CURSOR_PROBE),
                        ),
                    ])
                } else if Line::from(line.as_str()).width() > 0 {
                    after_last_probe = true;
                    Line::from(vec![
                        prefix,
                        Span::raw(" "),
                        Span::styled(line.to_owned(), Style::default().add_modifier(CURSOR_PROBE)),
                    ])
                } else {
                    after_last_probe = true;
                    Line::from(vec![
                        prefix,
                        Span::styled(" ", Style::default().add_modifier(CURSOR_PROBE)),
                        Span::raw(line.to_owned()),
                    ])
                }
            }),
    );
    (lines, after_last_probe)
}

fn slash_popup_lines(state: &TuiState) -> Vec<Line<'static>> {
    let Some(popup) = &state.slash_popup else {
        return Vec::new();
    };
    let matches = matching_slash_commands(&popup.filter);
    if matches.is_empty() {
        return vec![Line::from(Span::styled(
            "  no commands match",
            chrome_style(),
        ))];
    }
    matches
        .into_iter()
        .take(5)
        .enumerate()
        .map(|(index, command)| {
            let selected = index == popup.selected;
            let style = if selected {
                selected_row_style()
            } else {
                chrome_style()
            };
            Line::from(vec![
                Span::styled(if selected { "> " } else { "  " }, style),
                Span::styled(format!("/{}", command.name), style),
                Span::styled("  ", style),
                Span::styled(command.description.to_owned(), style),
            ])
        })
        .collect()
}

fn plural(count: usize) -> &'static str {
    if count == 1 { "" } else { "s" }
}

fn event_rows(event: &super::LiveEventLine) -> Vec<Line<'static>> {
    let (role, semantic_role) = match event.kind {
        LiveEventKind::User => ("user", SemanticRole::Primary),
        LiveEventKind::Assistant | LiveEventKind::AssistantDelta => {
            ("assistant", SemanticRole::Success)
        }
        LiveEventKind::Tool => ("tool", SemanticRole::Primary),
        LiveEventKind::Approval | LiveEventKind::Status => ("status", SemanticRole::Muted),
        LiveEventKind::Warning => ("warning", SemanticRole::Warning),
    };
    let mut text_lines = event.text.lines();
    let first = text_lines.next().unwrap_or_default();
    let first = match event.offset {
        Some(offset) => format!("#{offset} {first}"),
        None => first.to_owned(),
    };
    let mut rows = vec![role_row(role, semantic_role, &first)];
    rows.extend(text_lines.map(|line| role_row("", semantic_role, line)));
    rows
}

fn readback_lines(content: &str) -> Vec<Line<'static>> {
    let mut lines = content
        .lines()
        .filter_map(readback_line)
        .collect::<Vec<_>>();
    if lines.is_empty() {
        lines.push(status_row("no chat messages in readback"));
    }
    lines
}

fn readback_line(line: &str) -> Option<Line<'static>> {
    if line.starts_with("final_phase:")
        || line.starts_with("next_seq:")
        || line.starts_with("session_id:")
        || line.contains("] context ")
    {
        return None;
    }
    if let Some(run_id) = line.strip_prefix("run_id: ") {
        return Some(status_row(format!("run {run_id}")));
    }
    if let Some(text) = turn_text(line, "user: ") {
        return Some(role_row("user", SemanticRole::Primary, text));
    }
    if let Some(text) = turn_text(line, "assistant: ") {
        return Some(role_row("assistant", SemanticRole::Success, text));
    }
    if let Some(text) = turn_text(line, "tool: ") {
        return Some(role_row("tool", SemanticRole::Primary, text));
    }
    if let Some(text) = turn_text(line, "tool_call ") {
        return Some(role_row("tool", SemanticRole::Primary, text));
    }
    if let Some(text) = line.strip_prefix("tool_result ") {
        return Some(role_row("tool", SemanticRole::Primary, text));
    }
    if line.starts_with("policy_denied ")
        || line.starts_with("approval_denied ")
        || line.starts_with("tool_failed ")
    {
        return Some(warning_row(line.to_owned()));
    }
    if line.starts_with("approval_granted ") {
        return Some(status_row(line.to_owned()));
    }
    Some(status_row(line.to_owned()))
}

fn turn_text<'a>(line: &'a str, marker: &str) -> Option<&'a str> {
    let start = line.find("] ")? + 2;
    line[start..].strip_prefix(marker)
}

fn role_row(role: &'static str, semantic_role: SemanticRole, text: &str) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{role:<9} "), semantic_style(semantic_role)),
        Span::raw(text.to_owned()),
    ])
}

fn status_row(text: impl Into<String>) -> Line<'static> {
    role_row("status", SemanticRole::Muted, &text.into())
}

fn warning_row(text: impl Into<String>) -> Line<'static> {
    role_row("warning", SemanticRole::Warning, &text.into())
}

fn format_elapsed(seconds: u64) -> String {
    let hours = seconds / 3_600;
    let minutes = (seconds % 3_600) / 60;
    let seconds = seconds % 60;
    if hours > 0 {
        format!("{hours}h {minutes:02}m {seconds:02}s")
    } else if minutes > 0 {
        format!("{minutes}m {seconds:02}s")
    } else {
        format!("{seconds}s")
    }
}

fn render_shortcuts_overlay(frame: &mut Frame<'_>, area: Rect) {
    let mut lines = shortcut_lines(KEY_MAP, KeyLabelPlatform::current());
    lines.push(Line::from(""));
    lines.extend(
        SLASH_COMMANDS
            .iter()
            .filter(|command| command.action == SlashCommandAction::Threads)
            .map(|command| {
                Line::from(vec![
                    Span::styled(format!("/{:<10}", command.name), composer_prefix_style()),
                    Span::styled(command.description, chrome_style()),
                ])
            }),
    );
    let area = centered_overlay_rect(area, 68, lines.len().saturating_add(2));
    if area.is_empty() {
        return;
    }
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(lines).block(
            Block::default()
                .borders(Borders::ALL)
                .border_set(symbols::border::ROUNDED)
                .title("Shortcuts"),
        ),
        area,
    );
}

fn shortcut_lines(key_map: KeyMap<'_>, platform: KeyLabelPlatform) -> Vec<Line<'static>> {
    let labels = key_map
        .bindings()
        .iter()
        .map(|binding| binding.label.text(platform))
        .collect::<Vec<_>>();
    let label_width = labels
        .iter()
        .map(|label| Line::from(label.as_str()).width())
        .max()
        .unwrap_or(0);
    key_map
        .bindings()
        .iter()
        .zip(labels)
        .map(|(binding, label)| {
            let padding = label_width.saturating_sub(Line::from(label.as_str()).width()) + 2;
            Line::from(vec![
                Span::styled(label, composer_prefix_style()),
                Span::styled(" ".repeat(padding), chrome_style()),
                Span::styled(binding.description, chrome_style()),
            ])
        })
        .collect()
}

fn centered_overlay_rect(area: Rect, percent_x: u16, content_height: usize) -> Rect {
    if area.is_empty() {
        return Rect::default();
    }
    let width = area.width.saturating_mul(percent_x) / 100;
    let width = width.clamp(1, area.width);
    let height = u16::try_from(content_height)
        .unwrap_or(u16::MAX)
        .clamp(1, area.height);
    Rect::new(
        area.x + area.width.saturating_sub(width) / 2,
        area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    )
}

fn render_status_modal(frame: &mut Frame<'_>, area: Rect, status: &DaemonStatusResult) {
    let area = status_modal_rect(area);
    let lines = vec![
        modal_heading("MODEL"),
        Line::from(format!("requested alias  {}", status.model.requested_alias)),
        Line::from(format!(
            "served model    {}",
            known_or_unknown(status.model.served_model.as_deref())
        )),
        Line::from(format!(
            "provider        {}    key present: {}",
            status.model.provider_kind, status.model.key_present
        )),
        modal_heading("DAEMON"),
        Line::from(format!(
            "package         {}    commit {}",
            status.daemon.package_version,
            known_or_unknown(status.daemon.build_commit.as_deref())
        )),
        Line::from(format!(
            "build UTC       {}    uptime {} ms",
            known_or_unknown(status.daemon.build_date_utc.as_deref()),
            status.daemon.uptime_ms
        )),
        Line::from(format!("endpoint        {}", status.daemon.endpoint_path)),
        Line::from(format!("workspace       {}", status.daemon.workspace_id)),
        modal_heading("SESSION"),
        Line::from(format!(
            "selected        {}",
            selected_or_none(status.session.session_id.as_deref())
        )),
        Line::from(format!(
            "latest run      {}",
            selected_or_none(status.session.latest_run_id.as_deref())
        )),
        Line::from(format!(
            "human turns     {}    core events {}",
            status.session.human_turn_count, status.session.core_event_count
        )),
        Line::from(format!("ledger          {}", status.session.ledger_path)),
        modal_heading("USAGE"),
        usage_line("last run", &status.usage.last_run),
        usage_line("session", &status.usage.session),
        modal_heading("TRUST"),
        Line::from(format!(
            "granted         {}    denied {}",
            status.trust.approval_granted_count, status.trust.approval_denied_count
        )),
        Line::from(format!(
            "shell session   {}",
            if status.trust.shell_session_grant {
                "granted"
            } else {
                "not granted"
            }
        )),
        Line::from(format!("profile         {}", status.trust.approval_profile)),
        Line::from("Esc close"),
    ];
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title("Status")),
        area,
    );
}

fn status_modal_rect(area: Rect) -> Rect {
    let width = (area.width.saturating_mul(92) / 100).max(1);
    let height = area.height.clamp(1, 24);
    Rect::new(
        area.x + area.width.saturating_sub(width) / 2,
        area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    )
}

fn modal_heading(heading: &'static str) -> Line<'static> {
    Line::from(Span::styled(
        heading,
        semantic_style(SemanticRole::Primary).add_modifier(Modifier::BOLD),
    ))
}

fn usage_line(label: &str, usage: &DaemonStatusTokenUsage) -> Line<'static> {
    Line::from(format!(
        "{label:<9} input {}    output {}    unknown {}",
        usage.input_tokens, usage.output_tokens, usage.unknown_response_count
    ))
}

fn known_or_unknown(value: Option<&str>) -> &str {
    value.unwrap_or("unknown")
}

fn selected_or_none(value: Option<&str>) -> &str {
    value.unwrap_or("none")
}

fn render_session_picker(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    let area = centered_rect(78, 64, area);
    let row_width = area.width.saturating_sub(2);
    let picker = state
        .session_picker
        .as_ref()
        .expect("session picker is open");
    let threads = picker.matching_threads(&state.threads);
    let mut lines = vec![
        Line::from(vec![Span::styled(
            "Threads",
            Style::default().add_modifier(Modifier::BOLD),
        )]),
        Line::from("Type to filter    Backspace edit    Esc close"),
        Line::from("Up/Down or Ctrl-P/Ctrl-N move    Enter attach"),
        Line::from(format!("Filter: {}|", picker.filter)),
        Line::from(""),
    ];
    if threads.is_empty() && state.threads.is_empty() && picker.filter.is_empty() {
        lines.push(Line::from("No threads"));
    } else if threads.is_empty() {
        lines.push(Line::from("No matching threads"));
    } else {
        lines.extend(threads.iter().enumerate().map(|(index, thread)| {
            session_picker_row(state, thread, index == picker.selected, row_width)
        }));
    }
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(lines)
            .block(Block::default().borders(Borders::ALL).title("Threads"))
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn session_picker_row(
    state: &TuiState,
    thread: &platonic_protocol::ThreadStatus,
    focused: bool,
    row_width: u16,
) -> Line<'static> {
    let focus = if focused { ">" } else { " " };
    let current =
        if state.selected_thread_id.as_deref() == Some(thread.authority.thread_id.as_str()) {
            "*"
        } else {
            " "
        };
    let focus_style = if focused {
        selected_row_style()
    } else {
        Style::default()
    };
    let live_style = if focused {
        selected_row_style()
    } else {
        chrome_style()
    };
    let id_width = usize::from(row_width).saturating_sub(3 + THREAD_STATE_WIDTH + 1);
    let thread_id = bounded_preview(&thread.authority.thread_id, id_width);
    Line::from(vec![
        Span::styled(format!("{focus}{current} "), focus_style),
        Span::styled(
            format!("{:<THREAD_STATE_WIDTH$}", thread_live_label(thread)),
            live_style,
        ),
        Span::styled(" ", focus_style),
        Span::styled(thread_id, focus_style),
    ])
}

fn bounded_preview(value: &str, max_chars: usize) -> String {
    let line = value.lines().next().unwrap_or_default();
    if line.chars().count() <= max_chars {
        return line.to_owned();
    }
    if max_chars <= 3 {
        return line.chars().take(max_chars).collect();
    }
    format!(
        "{}...",
        line.chars().take(max_chars - 3).collect::<String>()
    )
}

fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().try_into().unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn render_approval_pane(
    frame: &mut Frame<'_>,
    area: Rect,
    approval: &ApprovalModalView,
    scroll_offset: usize,
) {
    let controls = if approval.can_grant_shell_session() {
        "g allow once    s allow shell.exec for session    d deny    Ctrl-C cancel    q quit"
    } else {
        "g allow once    d deny    Ctrl-C cancel run    q quit TUI"
    };
    let mut lines = vec![
        Line::from(controls),
        Line::from(""),
        Line::from(vec![
            Span::styled("run ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(approval.run_id.clone()),
        ]),
        Line::from(vec![
            Span::styled("call ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(approval.tool_call_id.clone()),
        ]),
        Line::from(vec![
            Span::styled("tool ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(format!("{} ({})", approval.tool_name, approval.effect)),
        ]),
        Line::from(vec![
            Span::styled("reason ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(approval.reason.clone()),
        ]),
        Line::from(""),
        Line::from("input preview:"),
    ];
    lines.extend(
        approval
            .input_preview
            .lines()
            .map(|line| Line::from(line.to_owned())),
    );
    if let Some(preview) = approval.approval_preview.as_deref() {
        lines.push(Line::from(""));
        lines.push(Line::from("approval preview:"));
        lines.extend(preview.lines().map(|line| Line::from(line.to_owned())));
    }
    if let Some(preview) = approval.diff_preview.as_deref() {
        lines.push(Line::from(""));
        lines.push(Line::from("diff preview:"));
        lines.extend(preview.lines().map(|line| Line::from(line.to_owned())));
    }
    let paragraph = Paragraph::new(lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(semantic_style(SemanticRole::Border))
                .title("Approval"),
        )
        .wrap(Wrap { trim: false });
    let content_width = area.width.saturating_sub(2).max(1);
    let content_height = usize::from(area.height.saturating_sub(2));
    let max_scroll = paragraph
        .line_count(content_width)
        .saturating_sub(content_height);
    let scroll = u16::try_from(scroll_offset.min(max_scroll)).unwrap_or(u16::MAX);
    frame.render_widget(paragraph.scroll((scroll, 0)), area);
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(vertical[1])[1]
}

fn vertical(area: Rect, state: &TuiState) -> [Rect; 4] {
    let footer_height = u16::from(area.height >= 2);
    let composer_height =
        composer_height(state, area.width).min(area.height.saturating_sub(footer_height));
    let reserved_history_height =
        u16::from(area.height > composer_height.saturating_add(footer_height));
    let approval_height = state.approval.as_ref().map_or(0, |_| {
        area.height
            .saturating_sub(
                composer_height
                    .saturating_add(footer_height)
                    .saturating_add(reserved_history_height),
            )
            .min(14)
    });
    let history_height = area.height.saturating_sub(
        approval_height
            .saturating_add(composer_height)
            .saturating_add(footer_height),
    );
    let history = Rect::new(area.x, area.y, area.width, history_height);
    let approval = Rect::new(area.x, history.bottom(), area.width, approval_height);
    let composer = Rect::new(area.x, approval.bottom(), area.width, composer_height);
    let footer = Rect::new(area.x, composer.bottom(), area.width, footer_height);
    [history, approval, composer, footer]
}

fn composer_height(state: &TuiState, width: u16) -> u16 {
    let popup_lines = state
        .slash_popup
        .as_ref()
        .map(|popup| matching_slash_commands(&popup.filter).len().clamp(1, 5))
        .unwrap_or(0);
    let rendered_lines = if state.composer_is_empty() {
        1
    } else {
        Paragraph::new(composer_lines(state))
            .wrap(Wrap { trim: false })
            .line_count(width.max(1))
    };
    let cursor_lines = composer_cursor_position(Rect::new(0, 0, width, 9), state, false)
        .map_or(0, |(_, y)| usize::from(y) + 1);
    (popup_lines + rendered_lines.max(cursor_lines)).clamp(1, 9) as u16
}

#[cfg(test)]
mod tests {
    use super::*;
    use platonic_core::EffectClass;
    use platonic_protocol::{
        ApprovalDecisionName, HelloResult, PendingApprovalSnapshot, SessionSummary,
        TranscriptReadResult, TypedRun, TypedTranscript, TypedTranscriptEntry,
    };
    use ratatui::backend::Backend;

    use super::super::state::approval_from_snapshot;
    use super::super::{ActiveRunView, LiveEventLine};

    #[test]
    fn renders_intro_as_chat_surface() {
        let state = TuiState::connected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            HelloResult {
                daemon_version: "0.1.0 0123456789abcdef0123456789abcdef01234567 2026-08-01".into(),
                workspace_id: "work-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![],
                daemon_scope: None,
            },
            Vec::new(),
            TranscriptState::None,
        );

        let output = render_to_text(&state);

        assert!(output.contains("Plato Agent"));
        assert!(output.contains("Local Rust agent runtime"));
        assert!(output.contains("0.1.0 0123456 2026-08-01"));
        assert!(!output.contains("0123456789abcdef"));
        assert!(output.contains("work-1234"));
        assert!(output.contains("? shortcuts · Tab queue 0"));
        assert!(output.contains("Try \"read README.md and summarize it\""));
        assert!(!output.contains("? help"));
        assert!(!output.contains("v toggle"));
        assert!(!output.contains("Status"));
        assert!(!output.contains("Sessions"));
        assert!(!output.contains("Live Events"));
        assert!(!output.contains("Composer"));
    }

    #[test]
    fn daemon_identity_preserves_release_and_unknown_provenance() {
        let release = "platonic 0.1.0 (0123456789abcdef0123456789abcdef01234567, 2026-08-01)";
        assert_eq!(daemon_identity_label(release), release);
        assert_eq!(
            daemon_identity_label("0.1.0 unknown unknown"),
            "0.1.0 unknown unknown"
        );
        assert_eq!(daemon_identity_label("legacy"), "legacy unknown unknown");
    }

    #[test]
    fn model_status_labels_distinguish_selected_known_served_and_unknown_served() {
        assert_eq!(
            model_status_label(Some(&ModelIdentityStatus::Requested {
                model: "~openai/gpt-latest".into(),
            })),
            "selected ~openai/gpt-latest"
        );
        assert_eq!(
            model_status_label(Some(&ModelIdentityStatus::Responded {
                served_model: Some("openai/gpt-5.2-2026-08-01".into()),
            })),
            "served openai/gpt-5.2-2026-08-01"
        );
        assert_eq!(
            model_status_label(Some(&ModelIdentityStatus::Responded { served_model: None })),
            "served unknown"
        );
        assert_eq!(model_status_label(None), "model pending");
    }

    #[test]
    fn footer_uses_literal_40_80_120_collapse_order_without_wrapping() {
        let mut state = conversation_fixture();
        state.active_run = Some(ActiveRunView {
            run_id: "run_hidden_identifier".into(),
            status: RunStateName::Running,
        });
        state.active_model = Some(ModelIdentityStatus::Requested {
            model: "model-with-a-very-long-display-name".into(),
        });
        state.queued_messages = vec!["one".into(), "two".into()];

        assert_eq!(
            [FOOTER_HELP_WIDTH, FOOTER_QUEUE_WIDTH, FOOTER_CONTEXT_WIDTH],
            [40, 80, 120]
        );
        assert_eq!(footer_line(&state, 40).to_string(), "? shortcuts");
        assert_eq!(
            footer_line(&state, 80).to_string(),
            "? shortcuts · Tab queue 2 · Esc interrupt"
        );
        assert_eq!(
            footer_line(&state, 120).to_string(),
            "? shortcuts · Tab queue 2 · Esc interrupt · selected model-with-a-very-long-display-name · workspace /tmp/work"
        );

        for width in [0, 8, 24, 40, 79, 80, 119, 120] {
            let line = footer_line(&state, width);
            assert!(line.width() <= usize::from(width));
            assert!(!line.to_string().contains("run_hidden_identifier"));
            assert!(!line.to_string().contains("v toggle"));
        }

        let [history, approval, composer, footer] = vertical(Rect::new(0, 0, 48, 12), &state);
        assert_eq!(history.height, 10);
        assert_eq!(approval.height, 0);
        assert_eq!(composer.height, 1);
        assert_eq!(footer.height, 1);
    }

    #[test]
    fn yolo_footer_signal_is_stable_for_current_and_next_sessions() {
        let mut state = conversation_fixture();
        state.approval_profile = ApprovalProfile::Yolo;
        state.selected_session_id = Some("session_1".into());
        assert!(footer_line(&state, 40).to_string().starts_with("yolo · "));

        state.selected_session_id = None;
        assert!(
            footer_line(&state, 40)
                .to_string()
                .starts_with("yolo next · ")
        );
        for width in [0, 4, 8, 16, 24, 40, 80, 120] {
            assert!(footer_line(&state, width).width() <= usize::from(width));
        }
    }

    #[test]
    fn renders_connected_sessions_and_transcript() {
        let state = TuiState::connected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            HelloResult {
                daemon_version: "0.1.0".into(),
                workspace_id: "work-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![],
                daemon_scope: None,
            },
            vec![SessionSummary {
                session_id: "run_1".into(),
                run_id: "run_1".into(),
                status: RunStateName::Finished,
                latest_question: "read README".into(),
                first_question: "read README".into(),
                updated_at_ms: 1,
                ledger_path: "/tmp/agent.db".into(),
            }],
            TranscriptState::Loaded(
                TranscriptReadResult {
                    run_id: "run_1".into(),
                    status: RunStateName::Finished,
                    final_answer: Some("README summary".into()),
                    transcript:
                        "final_phase: Finished\nnext_seq: 5\n[turn_1] context ToolSchemas model.tools: [{\"name\":\"file_read\"}]\n[turn_1] user: read README\n[turn_1] assistant: README summary\n"
                            .into(),
                    typed: None,
                    pending_approval: None,
                completion_claim: None,
                }
                .into(),
            ),
        );

        let output = render_to_text(&state);

        assert!(!output.contains("run_1"));
        assert!(output.contains("You"));
        assert!(output.contains("read README"));
        assert!(output.contains("Plato"));
        assert!(output.contains("README summary"));
        assert!(!output.contains("final_phase"));
        assert!(!output.contains("next_seq"));
        assert!(!output.contains("ToolSchemas"));
        assert!(!output.contains("file_read"));
    }

    #[test]
    fn conversation_and_audit_snapshots_at_normal_and_narrow_widths() {
        let mut state = conversation_fixture();
        let normal_conversation = focused_snapshot(&state, 96, 24);
        let narrow_conversation = focused_snapshot(&state, 48, 24);
        assert_eq!(
            render_to_text(&state).lines().next().map(str::trim_end),
            Some("You")
        );
        assert_eq!(
            normal_conversation,
            "You\n  First question asks for a concise summary.\n\nPlato\n  First answer is short and clear.\n\nTrace  tools | finished\n\nYou\n  Second question remains readable at narrow widths.\n\nPlato\n  Second answer stays readable.\n\nTrace  tool failed | warning | failed\n\n>   Try \"read README.md and summarize it\"\n? shortcuts · Tab queue 0"
        );
        assert_eq!(
            narrow_conversation,
            "You\n  First question asks for a concise summary.\n\nPlato\n  First answer is short and clear.\n\nTrace  tools | finished\n\nYou\n  Second question remains readable at narrow\nwidths.\n\nPlato\n  Second answer stays readable.\n\nTrace  tool failed | warning | failed\n\n>   Try \"read README.md and summarize it\"\n? shortcuts"
        );
        for snapshot in [&normal_conversation, &narrow_conversation] {
            assert_eq!(snapshot.lines().filter(|line| *line == "You").count(), 2);
            assert_eq!(snapshot.lines().filter(|line| *line == "Plato").count(), 2);
            assert_eq!(
                snapshot
                    .lines()
                    .filter(|line| line.starts_with("Trace  "))
                    .count(),
                2
            );
            assert!(!snapshot.contains("run_alpha_full_identifier"));
            assert!(!snapshot.contains("run_beta_full_identifier"));
            assert!(!snapshot.contains("#41"));
            assert!(!snapshot.contains("#42"));
        }

        state.toggle_display_mode();
        let normal_audit = focused_snapshot(&state, 96, 24);
        let narrow_audit = focused_snapshot(&state, 48, 24);
        assert_eq!(
            normal_audit,
            "status    run run_beta_full_identifier\n\nstatus    run run_alpha_full_identifier\nuser      First question asks for a concise summary.\nassistant\ntool      call_alpha file.read {\\\"path\\\":\\\"README.md\\\"}\ntool      call_alpha README loaded\nassistant First answer is short and clear.\nstatus    run run_beta_full_identifier\nuser      Second question remains readable at narrow widths.\nassistant Second answer stays readable.\nwarning   tool_failed call_beta: permission denied\n\ntranscript\nassistant #41 Second answer stays readable.\nwarning   #42 permission denied for call_beta\n\n>   Try \"read README.md and summarize it\"\n? shortcuts · Tab queue 0"
        );
        assert_eq!(
            narrow_audit,
            "status    run run_beta_full_identifier\n\nstatus    run run_alpha_full_identifier\nuser      First question asks for a concise\nsummary.\nassistant\ntool      call_alpha file.read\n{\\\"path\\\":\\\"README.md\\\"}\ntool      call_alpha README loaded\nassistant First answer is short and clear.\nstatus    run run_beta_full_identifier\nuser      Second question remains readable at\nnarrow widths.\nassistant Second answer stays readable.\nwarning   tool_failed call_beta: permission\ndenied\n\ntranscript\nassistant #41 Second answer stays readable.\nwarning   #42 permission denied for call_beta\n\n>   Try \"read README.md and summarize it\"\n? shortcuts"
        );
        for snapshot in [&normal_audit, &narrow_audit] {
            assert!(snapshot.contains("run_alpha_full_identifier"));
            assert!(snapshot.contains("run_beta_full_identifier"));
            assert!(snapshot.contains("#41"));
            assert!(snapshot.contains("#42"));
            assert!(snapshot.contains("call_alpha"));
        }
    }

    #[test]
    fn chrome_is_dim_while_status_roles_use_the_terminal_palette() {
        assert_eq!(chrome_style(), Style::default().add_modifier(Modifier::DIM));

        color::with_test_colors(
            color::TerminalColors::forced(color::ColorCapability::TrueColor, Some((0, 0, 0))),
            || {
                let state = conversation_fixture();
                let footer = footer_line(&state, 100);
                assert_eq!(footer.spans[0].style, composer_prefix_style());
                assert_eq!(footer.spans[1].style, chrome_style());
                let intro = intro_lines(&state);
                for line in &intro[3..7] {
                    assert_eq!(line.spans[0].style, chrome_style());
                }
                let mut trace = Vec::new();
                push_trace_row(&mut trace, "tools | finished".into());
                assert!(
                    trace[0]
                        .spans
                        .iter()
                        .all(|span| span.style == chrome_style())
                );

                assert_eq!(
                    status_row("ready").spans[0].style,
                    semantic_style(SemanticRole::Muted)
                );
            },
        );
    }

    #[test]
    fn typed_and_live_markdown_match_on_reload_and_skip_empty_assistant_cells() {
        let TranscriptState::Loaded(mut transcript) = conversation_fixture().transcript else {
            panic!("expected loaded transcript");
        };
        transcript.run_id = "run_alpha_full_identifier".into();
        transcript.typed.as_mut().unwrap().runs.truncate(1);
        let entries = &mut transcript.typed.as_mut().unwrap().runs[0].entries;
        entries[0] = TypedTranscriptEntry::User {
            text: "**literal user**".into(),
        };
        entries[4] = TypedTranscriptEntry::Assistant {
            text: "# Reloaded\n\nA **bold** answer.".into(),
        };
        let markdown = MarkdownRenderer::default();
        let typed =
            conversation_transcript_lines(&transcript, 100, DEFAULT_SYNTAX_THEME, &markdown);
        let live = conversation_live_event_lines(
            &[
                LiveEventLine::user("**literal user**").with_run_id("run_alpha_full_identifier"),
                LiveEventLine::assistant(Some(1), "").with_run_id("run_alpha_full_identifier"),
                LiveEventLine::tool(Some(2), "call_alpha proposed")
                    .with_run_id("run_alpha_full_identifier"),
                LiveEventLine::assistant(Some(3), "# Reloaded\n\nA **bold** answer.")
                    .with_run_id("run_alpha_full_identifier"),
            ],
            &[],
            100,
            DEFAULT_SYNTAX_THEME,
            &markdown,
        );

        assert_eq!(live, typed);
        assert_eq!(
            typed.iter().map(ToString::to_string).collect::<Vec<_>>(),
            vec![
                "You",
                "  **literal user**",
                "",
                "Plato",
                "  Reloaded",
                "",
                "  A bold answer."
            ]
        );
    }

    #[test]
    fn finalized_raw_markdown_rerenders_from_120_to_40_and_back() {
        let source = concat!(
            "## Resize-safe answer\n\n",
            "This deliberately long assistant sentence must wrap at forty columns while ",
            "remaining one raw Markdown source when the terminal returns to one hundred twenty.\n\n",
            "| Name | Value |\n| --- | --- |\n| alpha | one |"
        );
        let events = [LiveEventLine::assistant(Some(9), source).with_run_id("run_resize")];
        let markdown = MarkdownRenderer::default();
        let wide =
            conversation_live_event_lines(&events, &[], 120, DEFAULT_SYNTAX_THEME, &markdown);
        let narrow =
            conversation_live_event_lines(&events, &[], 40, DEFAULT_SYNTAX_THEME, &markdown);
        let wide_again =
            conversation_live_event_lines(&events, &[], 120, DEFAULT_SYNTAX_THEME, &markdown);

        assert_eq!(wide_again, wide);
        assert!(narrow.len() > wide.len());
        assert_eq!(events[0].text.as_bytes(), source.as_bytes());
    }

    #[test]
    fn conversation_preserves_whitespace_bearing_assistant_content() {
        let rows = conversation_live_event_lines(
            &[LiveEventLine::assistant(Some(1), " \t")],
            &[],
            100,
            DEFAULT_SYNTAX_THEME,
            &MarkdownRenderer::default(),
        );

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].to_string(), "Plato");
        assert_eq!(rows[1].spans[1].content.as_ref(), " \t");
    }

    #[test]
    fn conversation_labels_have_distinct_literal_styles() {
        color::with_test_colors(
            color::TerminalColors::forced(color::ColorCapability::TrueColor, Some((0, 0, 0))),
            || {
                let state = conversation_fixture();
                let TranscriptState::Loaded(transcript) = &state.transcript else {
                    panic!("expected loaded transcript");
                };
                let rows = conversation_transcript_lines(
                    transcript,
                    100,
                    DEFAULT_SYNTAX_THEME,
                    &MarkdownRenderer::default(),
                );
                let you = rows
                    .iter()
                    .find(|line| line.spans.first().is_some_and(|span| span.content == "You"))
                    .unwrap();
                let plato = rows
                    .iter()
                    .find(|line| {
                        line.spans
                            .first()
                            .is_some_and(|span| span.content == "Plato")
                    })
                    .unwrap();

                assert_eq!(you.spans[0].style, accent_style());
                assert_eq!(
                    plato.spans[0].style,
                    semantic_style(SemanticRole::Success).add_modifier(Modifier::BOLD)
                );
                assert_ne!(you.spans[0].style, plato.spans[0].style);
            },
        );
    }

    #[test]
    fn focused_dark_and_light_message_style_snapshots() {
        fn snapshot(colors: color::TerminalColors) -> (Style, Style) {
            color::with_test_colors(colors, || {
                let state = conversation_fixture();
                let TranscriptState::Loaded(transcript) = &state.transcript else {
                    panic!("expected loaded transcript");
                };
                let rows = conversation_transcript_lines(
                    transcript,
                    100,
                    DEFAULT_SYNTAX_THEME,
                    &MarkdownRenderer::default(),
                );
                let label_index = rows
                    .iter()
                    .position(|line| line.spans.first().is_some_and(|span| span.content == "You"))
                    .unwrap();
                (
                    rows[label_index].spans[0].style,
                    rows[label_index + 1].style,
                )
            })
        }

        assert_eq!(
            snapshot(color::TerminalColors::forced(
                color::ColorCapability::TrueColor,
                Some((0, 0, 0)),
            )),
            (
                Style::default()
                    .fg(ratatui::style::Color::Rgb(0, 255, 255))
                    .add_modifier(Modifier::BOLD),
                Style::default().bg(ratatui::style::Color::Rgb(30, 30, 30)),
            )
        );
        assert_eq!(
            snapshot(color::TerminalColors::forced(
                color::ColorCapability::TrueColor,
                Some((255, 255, 255)),
            )),
            (
                Style::default()
                    .fg(ratatui::style::Color::Rgb(0, 95, 135))
                    .add_modifier(Modifier::BOLD),
                Style::default().bg(ratatui::style::Color::Rgb(244, 244, 244)),
            )
        );
    }

    #[test]
    fn no_color_preserves_byte_identical_layout() {
        let colored = color::with_test_colors(
            color::TerminalColors::forced(color::ColorCapability::TrueColor, Some((0, 0, 0))),
            || render_snapshot(&conversation_fixture(), 96, 24).unwrap(),
        );
        let no_color = color::with_test_colors(
            color::TerminalColors::forced_no_color(
                color::ColorCapability::TrueColor,
                Some((0, 0, 0)),
            ),
            || render_snapshot(&conversation_fixture(), 96, 24).unwrap(),
        );

        assert_eq!(no_color, colored);
    }

    #[test]
    fn conversation_deduplicates_committed_live_messages_across_runs() {
        let mut state = conversation_fixture();
        state.live_events.insert(
            0,
            LiveEventLine::assistant(Some(40), "First answer is short and clear.")
                .with_run_id("run_alpha_full_identifier"),
        );

        let output = render_to_text(&state);

        assert_eq!(
            output.matches("First answer is short and clear.").count(),
            1
        );
        assert_eq!(output.matches("Second answer stays readable.").count(), 1);
        assert_eq!(output.matches("Trace").count(), 2);
        assert!(!output.contains("#40"));
        assert!(!output.contains("#41"));
    }

    #[test]
    fn reuses_cached_history_rows_while_dynamic_rows_change() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<TuiState>();

        let mut state = history_cache_state(
            "[turn_1] user: cached question\n[turn_1] assistant: cached answer\n",
            LiveEventLine::tool(Some(1), "cached tool event"),
        );

        let equal_before_render = state.clone();
        let first = render_to_text(&state);
        assert_eq!(state, equal_before_render);
        let cloned = state.clone();
        assert_eq!(cloned, state);
        assert!(cloned.history_rows.transcript.read().unwrap().is_none());
        assert!(cloned.history_rows.live_events.read().unwrap().is_none());
        assert_eq!(render_to_text(&cloned), first);
        let cached_ptrs = cached_row_ptrs(&state);

        state.status_message = Some("dynamic status".into());
        state.stream_warning = Some("dynamic warning".into());
        state.queued_messages.push("dynamic queue".into());
        let second = render_to_text(&state);

        assert_ne!(first, second);
        assert!(!second.contains("dynamic status"));
        assert!(second.contains("stream warning"));
        assert!(!second.contains("dynamic warning"));
        assert!(second.contains("dynamic queue"));
        assert_eq!(cached_row_ptrs(&state), cached_ptrs);
    }

    #[test]
    fn cache_key_covers_exact_source_width_mode_and_theme_revision() {
        let mut state = conversation_fixture();
        history_lines_with_theme(&state, 80, DEFAULT_SYNTAX_THEME);
        let calls = state.history_rows.markdown.render_calls();
        history_lines_with_theme(&state, 80, DEFAULT_SYNTAX_THEME);
        assert_eq!(state.history_rows.markdown.render_calls(), calls);

        history_lines_with_theme(&state, 81, DEFAULT_SYNTAX_THEME);
        assert!(state.history_rows.markdown.render_calls() > calls);
        assert_eq!(transcript_cache_key(&state).width, 81);

        let calls = state.history_rows.markdown.render_calls();
        let TranscriptState::Loaded(transcript) = &mut state.transcript else {
            panic!("expected loaded transcript");
        };
        transcript.content.push_str("raw source changed\n");
        history_lines_with_theme(&state, 81, DEFAULT_SYNTAX_THEME);
        assert!(state.history_rows.markdown.render_calls() > calls);

        let calls = state.history_rows.markdown.render_calls();
        state.toggle_display_mode();
        history_lines_with_theme(&state, 81, DEFAULT_SYNTAX_THEME);
        assert_eq!(state.history_rows.markdown.render_calls(), calls);
        assert_eq!(
            transcript_cache_key(&state).display_mode,
            DisplayMode::Audit
        );

        state.toggle_display_mode();
        let revised = DEFAULT_SYNTAX_THEME.with_revision(1);
        history_lines_with_theme(&state, 81, revised);
        assert!(state.history_rows.markdown.render_calls() > calls);
        assert_eq!(transcript_cache_key(&state).syntax_theme, revised);
    }

    #[test]
    fn refreshes_cached_rows_after_direct_public_source_mutation() {
        let mut state = history_cache_state(
            "[turn_1] assistant: old transcript\n",
            LiveEventLine::assistant(Some(1), "old live event"),
        );
        let first = render_to_text(&state);
        assert!(first.contains("old transcript"));
        assert!(first.contains("old live event"));

        let TranscriptState::Loaded(transcript) = &mut state.transcript else {
            panic!("expected loaded transcript");
        };
        transcript.content = "[turn_1] assistant: new transcript\n".into();
        state.live_events[0].text = "new live event".into();
        state
            .live_events
            .push(LiveEventLine::assistant(Some(2), "directly pushed event"));

        let second = render_to_text(&state);

        assert!(second.contains("new transcript"));
        assert!(!second.contains("old transcript"));
        assert!(second.contains("new live event"));
        assert!(!second.contains("old live event"));
        assert!(second.contains("directly pushed event"));

        let TranscriptState::Loaded(transcript) = &mut state.transcript else {
            panic!("expected loaded transcript");
        };
        transcript.content = "[turn_1] assistant: \n".into();
        state.live_events = vec![LiveEventLine::assistant(Some(3), "")];
        let empty = render_to_text(&state);
        assert_eq!(
            empty
                .lines()
                .filter(|line| line.trim_end() == "Plato")
                .count(),
            0
        );

        state.transcript = TranscriptState::None;
        state.live_events.clear();
        let cleared = render_to_text(&state);

        assert!(!cleared.contains("new transcript"));
        assert!(!cleared.contains("new live event"));
        assert!(state.history_rows.transcript.read().unwrap().is_none());
        assert!(state.history_rows.live_events.read().unwrap().is_none());
    }

    #[test]
    fn renders_transcript_error_for_selected_run() {
        let state = TuiState::connected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            HelloResult {
                daemon_version: "0.1.0".into(),
                workspace_id: "work-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![],
                daemon_scope: None,
            },
            vec![SessionSummary {
                session_id: "run_1".into(),
                run_id: "run_1".into(),
                status: RunStateName::Failed,
                latest_question: "read README".into(),
                first_question: "read README".into(),
                updated_at_ms: 1,
                ledger_path: "/tmp/agent.db".into(),
            }],
            TranscriptState::Unavailable {
                run_id: "run_1".into(),
                error: "run not found: run_1".into(),
            },
        );

        let output = render_to_text(&state);

        assert!(output.contains("Transcript unavailable"));
        assert!(!output.contains("run_1"));
        assert!(output.contains("selected run"));
    }

    #[test]
    fn renders_daemon_unavailable_guidance() {
        let state = TuiState::disconnected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            "connection refused".into(),
        );

        let output = render_to_text(&state);

        assert!(output.contains("daemon unavailable"));
        assert!(output.contains("plato --tui to ensure the host daemon"));
        assert!(output.contains("start platonic serve"));
        assert!(!output.contains("serve --workspace"));
        assert!(!output.contains("cargo run"));
        assert!(output.contains("press r to reconnect"));
        assert!(output.contains("daemon unavailable — r to reconnect"));
    }

    #[test]
    fn renders_active_run_composer_and_live_events() {
        let mut state = TuiState::connected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            HelloResult {
                daemon_version: "0.1.0".into(),
                workspace_id: "work-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![],
                daemon_scope: None,
            },
            Vec::new(),
            TranscriptState::None,
        );
        state.active_run = Some(ActiveRunView {
            run_id: "run_1".into(),
            status: RunStateName::Running,
        });
        state.set_composer_text("summarize this file");
        state
            .composer
            .move_cursor(tui_textarea::CursorMove::Jump(0, 9));
        state
            .live_events
            .push(LiveEventLine::assistant(Some(2), "assistant response"));

        let output = render_to_text(&state);

        assert!(output.contains("Working"));
        assert!(output.contains("Esc interrupt"));
        assert!(!output.contains("run_1"));
        assert!(output.contains("assistant response"));
        assert!(output.contains("> summarize this file"));
        assert!(!output.contains("summarize|"));
    }

    #[test]
    fn composer_cursor_tracks_unicode_newlines_and_soft_wrap_without_a_caret_glyph() {
        let mut state = conversation_fixture();

        state.set_composer_text("ab界café");
        state
            .composer
            .move_cursor(tui_textarea::CursorMove::Jump(0, 3));
        assert_eq!(render_cursor_position(&state, 20, 12), (6, 10));
        let unicode = render_snapshot_at(&state, 20, 12, 0).unwrap();
        assert!(unicode.contains("> ab界"));
        assert!(unicode.contains("café"));
        assert!(!unicode.contains("界|"));

        state.set_composer_text("first\n界second");
        state
            .composer
            .move_cursor(tui_textarea::CursorMove::Jump(1, 1));
        assert_eq!(render_cursor_position(&state, 20, 12), (4, 10));
        let multiline = render_snapshot_at(&state, 20, 12, 0).unwrap();
        assert!(multiline.contains("> first"));
        assert!(multiline.contains("| 界"));
        assert!(multiline.contains("second"));
        assert!(!multiline.contains("界|"));

        state.set_composer_text("abcdefgh");
        assert_eq!(render_cursor_position(&state, 10, 8), (0, 6));
        let wrapped = render_snapshot_at(&state, 10, 8, 0).unwrap();
        assert!(wrapped.contains("> abcdefgh"));
        assert!(!wrapped.contains("abcdefgh|"));
    }

    #[test]
    fn composer_renders_textarea_selection_without_moving_the_real_cursor() {
        let mut state = conversation_fixture();
        state.set_composer_text("select this");
        state
            .composer
            .move_cursor(tui_textarea::CursorMove::Jump(0, 7));
        state.composer.start_selection();
        state.composer.move_cursor(tui_textarea::CursorMove::End);

        let lines = composer_lines(&state);
        let selected = lines[0]
            .spans
            .iter()
            .find(|span| span.style.add_modifier.contains(Modifier::REVERSED))
            .expect("selected composer span");
        assert_eq!(selected.content, "this");
        assert_eq!(render_cursor_position(&state, 20, 12), (13, 10));
    }

    #[test]
    fn empty_composer_keeps_placeholder_geometry_and_uses_the_terminal_cursor() {
        let state = conversation_fixture();
        let output = render_snapshot_at(&state, 48, 12, 0).unwrap();

        assert!(output.contains(">   Try \"read README.md and summarize it\""));
        assert_eq!(render_cursor_position(&state, 48, 12), (2, 10));
    }

    #[test]
    fn renders_animated_issue_prep_activity() {
        let mut state = TuiState::connected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            HelloResult {
                daemon_version: "0.1.0".into(),
                workspace_id: "work-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![],
                daemon_scope: None,
            },
            Vec::new(),
            TranscriptState::None,
        );
        state.issue_prep_started_at = Some(std::time::Instant::now());
        state.issue_prep_elapsed_secs = Some(2);
        state.working_elapsed_millis = 2_000;
        state.status_message = Some("issue prep running".into());

        let output = render_to_text(&state);

        assert!(output.contains("2s"));
        assert!(output.contains("Preparing"));
    }

    #[test]
    fn working_row_uses_braille_cadence_and_reduced_motion_fallback() {
        let mut state = TuiState::connected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            HelloResult {
                daemon_version: "0.1.0".into(),
                workspace_id: "work-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![],
                daemon_scope: None,
            },
            Vec::new(),
            TranscriptState::None,
        );
        state.active_run = Some(ActiveRunView::new(
            "run_working".into(),
            RunStateName::Running,
        ));
        state.active_run_elapsed_secs = Some(60);

        for (index, frame) in WORKING_FRAMES.iter().enumerate() {
            state.working_elapsed_millis = index as u64 * WORKING_FRAME_MILLIS;
            let output = render_to_text(&state);
            assert!(output.contains(&format!("{frame} Working  1m 00s  Esc to interrupt")));
        }

        state.set_reduced_motion(true);
        let output = render_to_text(&state);
        assert!(output.contains("• Working  1m 00s  Esc to interrupt"));
        assert!(!WORKING_FRAMES.iter().any(|frame| output.contains(frame)));
    }

    #[test]
    fn compact_elapsed_formats_literal_boundary_forms() {
        assert_eq!(format_elapsed(59), "59s");
        assert_eq!(format_elapsed(60), "1m 00s");
        assert_eq!(format_elapsed(7_389), "2h 03m 09s");
    }

    #[test]
    fn renders_multiline_live_event_as_separate_rows() {
        let mut state = TuiState::connected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            HelloResult {
                daemon_version: "0.1.0".into(),
                workspace_id: "work-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![],
                daemon_scope: None,
            },
            Vec::new(),
            TranscriptState::None,
        );
        state.live_events.push(LiveEventLine::assistant(
            None,
            "# Prepared issue\n\n## Problem\nThe issue is unclear.",
        ));

        let output = render_to_text(&state);

        assert!(output.lines().any(|line| line.contains("Prepared issue")));
        assert!(output.lines().any(|line| line.contains("Problem")));
        assert!(!output.contains("# Prepared issue"));
        assert!(!output.contains("## Problem"));
        assert!(!output.contains("Prepared issueProblem"));
    }

    #[test]
    fn renders_bottom_of_wrapped_multiline_event() {
        let mut state = TuiState::connected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            HelloResult {
                daemon_version: "0.1.0".into(),
                workspace_id: "work-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![],
                daemon_scope: None,
            },
            Vec::new(),
            TranscriptState::None,
        );
        let candidate = (0..30)
            .map(|index| {
                format!(
                    "Acceptance criterion {index} has enough text to wrap across terminal rows."
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        state
            .live_events
            .push(LiveEventLine::assistant(None, candidate));
        state.live_events.push(LiveEventLine::status(
            None,
            "issue-prep artifacts: /tmp/work/.plato/issue-prep/run_1",
        ));

        let output = render_to_text(&state);

        assert!(output.contains("issue-prep artifacts"));
        assert!(output.contains(".plato/issue-prep/run_1"));
    }

    #[test]
    fn renders_queue_preview_and_multiline_composer() {
        let mut state = TuiState::connected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            HelloResult {
                daemon_version: "0.1.0".into(),
                workspace_id: "work-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![],
                daemon_scope: None,
            },
            Vec::new(),
            TranscriptState::None,
        );
        state.queued_messages = vec!["queued next".into()];
        state.set_composer_text("first line\nsecond line");

        let output = render_to_text(&state);

        assert!(output.contains("queued"));
        assert!(output.contains("Tab queue 1"));
        assert!(output.contains("1 queued next"));
        assert!(output.contains("> first line"));
        assert!(output.contains("| second line"));
        assert!(!output.contains("second line|"));
    }

    #[test]
    fn renders_typed_tool_and_status_rows() {
        let mut state = TuiState::connected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            HelloResult {
                daemon_version: "0.1.0".into(),
                workspace_id: "work-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![],
                daemon_scope: None,
            },
            Vec::new(),
            TranscriptState::None,
        );
        state.active_model = Some(ModelIdentityStatus::Requested {
            model: "openrouter/auto".into(),
        });
        state.active_run_elapsed_secs = Some(65);
        state.live_events = vec![
            LiveEventLine::user("read README"),
            LiveEventLine::tool(Some(3), "file.read finished"),
            LiveEventLine::warning(Some(4), "approval pending shell.exec"),
        ];

        let output = render_snapshot_at(&state, 120, 24, 0).unwrap();

        assert!(output.contains("selected openrouter/auto"));
        assert!(output.contains("You"));
        assert!(output.contains("read README"));
        assert!(output.contains("Trace"));
        assert!(output.contains("tools"));
        assert!(!output.contains("file.read finished"));
        assert!(output.contains("warning"));
        assert!(!output.contains("approval pending shell.exec"));
    }

    #[test]
    fn conversation_renders_only_admitted_unoffset_client_notices() {
        let mut state = TuiState::connected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            HelloResult {
                daemon_version: "0.1.0".into(),
                workspace_id: "work-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![],
                daemon_scope: None,
            },
            Vec::new(),
            TranscriptState::None,
        );
        state.live_events = vec![
            LiveEventLine::warning(None, "thread send rejected: controller_owned"),
            LiveEventLine::warning(None, "voice configuration is unavailable: missing [voice]"),
            LiveEventLine::status(None, "voice enabled"),
            LiveEventLine::warning(None, "generic warning remains hidden"),
            LiveEventLine::warning(
                Some(7),
                "thread send rejected: offset warning remains hidden",
            ),
        ];

        let output = render_to_text(&state);

        assert!(output.contains("Notice"));
        assert!(output.contains("thread send rejected: controller_owned"));
        assert!(output.contains("voice configuration is unavailable: missing [voice]"));
        assert!(output.contains("voice enabled"));
        assert!(!output.contains("generic warning remains hidden"));
        assert!(!output.contains("offset warning remains hidden"));
    }

    #[test]
    fn approval_trace_reduces_pending_and_decisions_in_order() {
        for (decision, text) in [
            (ApprovalDecisionName::Granted, "approval granted call_1"),
            (ApprovalDecisionName::Denied, "approval denied call_1"),
        ] {
            let pending = vec![
                LiveEventLine::warning(Some(4), "approval pending file.write (workspace_write)")
                    .with_run_id("run_1"),
            ];
            let pending_summary = trace_summary(
                None,
                &pending,
                Some("run_1"),
                Some(RunStateName::Running),
                false,
                true,
            );
            assert_eq!(
                pending_summary.as_deref(),
                Some("warning | approval pending | running")
            );
            let pending_readback = TypedRun {
                run_id: "run_1".into(),
                session_index: 0,
                status: RunStateName::Running,
                model_status: None,
                entries: Vec::new(),
            };
            assert_eq!(
                trace_summary(
                    Some(&pending_readback),
                    &[],
                    None,
                    Some(RunStateName::Running),
                    false,
                    true,
                )
                .as_deref(),
                pending_summary.as_deref()
            );

            let mut resolved = pending;
            resolved.push(LiveEventLine::approval(Some(5), text).with_run_id("run_1"));
            assert_eq!(
                trace_summary(
                    None,
                    &resolved,
                    Some("run_1"),
                    Some(RunStateName::Running),
                    false,
                    false,
                )
                .as_deref(),
                Some("approval | running")
            );

            let typed = TypedRun {
                run_id: "run_1".into(),
                session_index: 0,
                status: RunStateName::Running,
                model_status: None,
                entries: vec![TypedTranscriptEntry::Approval {
                    call_id: "call_1".into(),
                    decision,
                    actor_id: "human".into(),
                    reason: None,
                }],
            };
            assert_eq!(
                trace_summary(
                    Some(&typed),
                    &[],
                    None,
                    Some(RunStateName::Running),
                    false,
                    false,
                )
                .as_deref(),
                Some("approval | running")
            );
        }
    }

    #[test]
    fn resolved_approval_keeps_unrelated_warning_and_terminal_priority() {
        let events = vec![
            LiveEventLine::warning(Some(4), "approval pending file.write (workspace_write)")
                .with_run_id("run_1"),
            LiveEventLine::approval(Some(5), "approval granted call_1").with_run_id("run_1"),
            LiveEventLine::warning(Some(6), "provider failed").with_run_id("run_1"),
        ];

        assert_eq!(
            trace_summary(
                None,
                &events,
                Some("run_1"),
                Some(RunStateName::Failed),
                false,
                false,
            )
            .as_deref(),
            Some("approval | warning | failed")
        );
    }

    #[test]
    fn approval_conversation_and_audit_snapshots_keep_current_and_historical_facts() {
        let mut state = approval_trace_fixture();
        assert_eq!(
            focused_snapshot(&state, 96, 24),
            "You\n  Review the proposed edit.\n\nTrace  approval | running\n\n⣾ Working  0s  Esc to interrupt\n\n>   Try \"read README.md and summarize it\"\n? shortcuts · Tab queue 0 · Esc interrupt"
        );

        state.toggle_display_mode();
        assert_eq!(
            focused_snapshot(&state, 96, 24),
            "status    run run_approval\n\nuser      Review the proposed edit.\n\ntranscript\nstatus    running run_approval\nwarning   #4 approval pending file.write (workspace_write)\nstatus    #5 approval granted call_approval\n\n⣾ Working  0s  Esc to interrupt\n\n>   Try \"read README.md and summarize it\"\n? shortcuts · Tab queue 0 · Esc interrupt"
        );
    }

    #[test]
    fn audit_overlay_renders_a_scrolled_transcript_window() {
        let mut state = TuiState::connected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            HelloResult {
                daemon_version: "0.1.0".into(),
                workspace_id: "work-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![],
                daemon_scope: None,
            },
            Vec::new(),
            TranscriptState::None,
        );
        state.live_events = (0..30)
            .map(|index| LiveEventLine::status(Some(index), format!("line {index}")))
            .collect();
        state.toggle_display_mode();

        let output = render_overlay_snapshot(&state, 100, 12, 10);

        assert!(output.contains("line 15"));
        assert!(!output.contains("line 29"));
    }

    #[test]
    fn inline_main_screen_keeps_committed_rows_out_of_the_live_viewport() {
        let state = conversation_fixture();
        let TranscriptState::Loaded(transcript) = &state.transcript else {
            panic!("conversation fixture must have a transcript");
        };

        for width in [40, 80, 120] {
            let committed = committed_transcript_lines(&state, transcript, width);
            assert!(
                committed
                    .iter()
                    .any(|line| line.to_string().contains("First question"))
            );

            let main = render_main_snapshot(&state, width, 12);
            assert!(!main.contains("First question"), "width {width}: {main}");
            assert!(!main.contains("Second answer"), "width {width}: {main}");
            assert!(main.contains("Trace"), "width {width}: {main}");
            assert!(main.contains("> "), "width {width}: {main}");
            assert!(main.contains("? shortcuts"), "width {width}: {main}");
        }
    }

    #[test]
    fn renders_stream_warning() {
        let mut state = TuiState::connected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            HelloResult {
                daemon_version: "0.1.0".into(),
                workspace_id: "work-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![],
                daemon_scope: None,
            },
            Vec::new(),
            TranscriptState::None,
        );
        state.stream_warning = Some("lagged; transcript recovered".into());

        let output = render_to_text(&state);

        assert!(output.contains("stream warning"));
        assert!(!output.contains("lagged"));
    }

    #[test]
    fn renders_shortcuts_overlay_from_styled_platform_keymap() {
        let mut state = TuiState::connected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            HelloResult {
                daemon_version: "0.1.0".into(),
                workspace_id: "work-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![],
                daemon_scope: None,
            },
            Vec::new(),
            TranscriptState::None,
        );
        state.help_visible = true;

        let output = render_to_text(&state);

        assert!(output.contains("╭Shortcuts"));
        assert!(output.contains(if cfg!(target_os = "macos") {
            "⌥ enter"
        } else {
            "alt + enter"
        }));
        assert!(output.contains("PgUp/PgDown"));
        assert!(output.contains("toggle conversation / audit"));
        assert!(output.contains("Ctrl+C"));
        assert!(output.contains("close overlay"));
        assert!(output.contains("/threads"));
        assert!(output.contains("open the thread picker"));
        assert!(output.contains("/sessions"));
        assert!(output.contains("compatibility alias for /threads"));
        assert!(output.contains("? shortcuts · Esc close"));

        let lines = shortcut_lines(KEY_MAP, KeyLabelPlatform::Other);
        assert!(lines.iter().all(|line| {
            line.spans
                .first()
                .is_some_and(|span| span.style == composer_prefix_style())
                && line.spans[1..]
                    .iter()
                    .all(|span| span.style == chrome_style())
        }));
    }

    #[test]
    fn four_footer_modes_render_their_documented_transient_hints() {
        let mut state = conversation_fixture();
        assert_eq!(state.footer_mode(), FooterMode::Contextual);
        assert_eq!(
            footer_line(&state, 80).to_string(),
            "? shortcuts · Tab queue 0"
        );

        state.help_visible = true;
        assert_eq!(state.footer_mode(), FooterMode::Shortcuts);
        assert_eq!(
            footer_line(&state, 80).to_string(),
            "? shortcuts · Esc close"
        );

        state.help_visible = false;
        state.cancel_requested = true;
        assert_eq!(state.footer_mode(), FooterMode::QuitConfirm);
        assert_eq!(
            footer_line(&state, 80).to_string(),
            "press Ctrl+C again to quit"
        );

        let offline = TuiState::disconnected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            "connection closed".into(),
        );
        assert_eq!(offline.footer_mode(), FooterMode::Offline);
        assert_eq!(
            footer_line(&offline, 80).to_string(),
            "daemon unavailable — r to reconnect"
        );
    }

    #[test]
    fn one_keymap_binding_updates_compact_footer_and_shortcuts_overlay() {
        let extra = KeyBinding {
            action: KeyAction::ToggleView,
            label: crate::commands::KeyLabel::Literal("x"),
            description: "inspect",
            footer: Some(crate::commands::FooterHint {
                priority: FooterHintPriority::Essential,
                when: FooterHintWhen::Always,
            }),
        };
        let mut bindings = KEY_MAP.bindings().to_vec();
        bindings.push(extra);
        let key_map = KeyMap::new(&bindings);
        let state = conversation_fixture();

        let footer = footer_line_with_keymap(&state, 40, key_map, KeyLabelPlatform::Other);
        let overlay = shortcut_lines(key_map, KeyLabelPlatform::Other)
            .into_iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(footer.to_string().contains("x inspect"));
        assert!(
            overlay
                .lines()
                .any(|line| line.starts_with('x') && line.ends_with("inspect"))
        );
    }

    #[test]
    fn short_terminal_layout_keeps_composer_and_footer_separate_without_panics() {
        let mut state = conversation_fixture();
        state.help_visible = true;

        for height in 0..=6 {
            let [history, approval, composer, footer] =
                vertical(Rect::new(0, 0, 40, height), &state);
            assert!(history.bottom() <= approval.top());
            assert!(approval.bottom() <= composer.top());
            assert!(composer.bottom() <= footer.top());
            assert!(footer.bottom() <= height);
            render_snapshot_at(&state, 40, height, 0).unwrap();
        }

        let [history, approval, composer, footer] = vertical(Rect::new(0, 0, 40, 2), &state);
        assert_eq!(history.height, 0);
        assert_eq!(approval.height, 0);
        assert_eq!(composer.height, 1);
        assert_eq!(footer.height, 1);
        let two_rows = render_snapshot_at(&state, 40, 2, 0).unwrap();
        let rows = two_rows.lines().collect::<Vec<_>>();
        assert!(rows[0].starts_with(">"));
        assert!(rows[1].starts_with("? shortcuts"));
    }

    #[test]
    fn status_modal_has_stable_normal_and_narrow_read_only_snapshots() {
        const SECRET: &str = "plato-status-secret-sentinel-355";
        let mut state = conversation_fixture();
        state.status_modal = Some(status_fixture());

        let normal = focused_snapshot(&state, 100, 24);
        let narrow = focused_snapshot(&state, 48, 24);

        for snapshot in [&normal, &narrow] {
            let mut previous = 0;
            for heading in ["MODEL", "DAEMON", "SESSION", "USAGE", "TRUST"] {
                assert_eq!(snapshot.matches(heading).count(), 1, "{snapshot}");
                let position = snapshot.find(heading).unwrap();
                assert!(
                    position >= previous,
                    "headings overlapped or reordered: {snapshot}"
                );
                previous = position;
            }
            assert!(snapshot.contains("Esc close"));
            assert!(!snapshot.contains(SECRET));
            assert!(snapshot.lines().count() <= 24);
        }
        assert!(normal.contains("~openai/gpt-latest"));
        assert!(normal.contains("openai/gpt-5.5-2026-08-01"));
        assert!(normal.contains("0123456789abcdef0123456789abcdef01234567"));
        assert!(normal.contains("human turns     2    core events 17"));
        assert!(normal.contains("last run  input 7    output 3    unknown 1"));
        assert!(normal.contains("session   input 17    output 8    unknown 2"));
        assert!(normal.contains("granted         2    denied 1"));
        assert!(normal.contains("shell session   granted"));
        assert!(normal.contains("profile         yolo"));
    }

    #[test]
    fn renders_slash_command_popup_from_registry() {
        let mut state = TuiState::connected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            HelloResult {
                daemon_version: "0.1.0".into(),
                workspace_id: "work-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![],
                daemon_scope: None,
            },
            Vec::new(),
            TranscriptState::None,
        );
        state.set_composer_text("/c");
        state.slash_popup = Some(super::super::state::SlashPopupView {
            filter: "c".into(),
            selected: 0,
        });

        let output = render_to_text(&state);

        assert!(output.contains("/clear"));
        assert!(output.contains("clear the visible transcript"));
        assert!(output.contains("? shortcuts · Tab queue 0"));
    }

    #[test]
    fn renders_thread_picker_overlay_with_durable_ids_and_live_states() {
        let mut state = TuiState::connected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            HelloResult {
                daemon_version: "0.1.0".into(),
                workspace_id: "work-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![],
                daemon_scope: None,
            },
            vec![],
            TranscriptState::None,
        );
        state.threads = vec![
            test_thread("thread_loaded", true, false),
            test_thread("thread_unloaded", false, false),
            test_thread("thread_active", true, true),
        ];
        state.selected_thread_id = Some("thread_loaded".into());
        state.session_picker = Some(super::super::state::SessionPickerView {
            filter: String::new(),
            selected: 1,
        });

        let output = render_to_text(&state);

        assert!(output.contains("Threads"));
        assert!(output.contains("Type to filter"));
        assert!(output.contains("Ctrl-P/Ctrl-N"));
        assert!(output.contains("Enter attach"));
        assert!(output.contains("Filter: |"));
        assert!(output.contains("loaded   thread_loaded"));
        assert!(output.contains("unloaded thread_unloaded"));
        assert!(output.contains("active   thread_active"));
    }

    #[test]
    fn selected_picker_rows_have_full_row_accents_at_40_and_80_columns() {
        let mut slash_state = TuiState::disconnected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            "offline".into(),
        );
        slash_state.set_composer_text("/sp");
        slash_state.slash_popup = Some(super::super::state::SlashPopupView {
            filter: "sp".into(),
            selected: 0,
        });

        let mut thread_state = TuiState::disconnected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            "offline".into(),
        );
        thread_state.threads = vec![test_thread("thread_1", true, false)];
        thread_state.selected_thread_id = Some("thread_1".into());
        thread_state.session_picker = Some(super::super::state::SessionPickerView {
            filter: String::new(),
            selected: 0,
        });

        for width in [40, 80] {
            assert_selected_row_accent(&slash_state, width, "> /issue-prep");
            assert_selected_row_accent(&thread_state, width, ">* loaded");
        }
    }

    #[test]
    fn thread_picker_renders_filtered_threads_and_explicit_no_match() {
        let mut state = TuiState::connected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            HelloResult {
                daemon_version: "0.1.0".into(),
                workspace_id: "work-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![],
                daemon_scope: None,
            },
            vec![],
            TranscriptState::None,
        );
        state.threads = vec![
            test_thread("thread_release", true, false),
            test_thread("thread_continue", false, false),
        ];
        state.session_picker = Some(super::super::state::SessionPickerView {
            filter: "CONT".into(),
            selected: 0,
        });

        let output = render_to_text_at(&state, 172_800_000);

        assert!(output.contains("Filter: CONT|"));
        assert!(output.contains("thread_continue"));
        assert!(!output.contains("thread_release"));
        assert!(!output.contains("No matching threads"));

        state.session_picker.as_mut().unwrap().filter = "missing".into();
        let output = render_to_text_at(&state, 172_800_000);

        assert!(output.contains("Filter: missing|"));
        assert!(output.contains("No matching threads"));
        assert!(!output.contains("thread_release"));
        assert!(!output.contains("thread_continue"));
    }

    #[test]
    fn thread_picker_row_bounds_long_stable_id() {
        let thread = test_thread(
            "thread_this_identifier_is_deliberately_much_longer_than_the_picker_row",
            false,
            false,
        );

        let row = session_picker_row(
            &TuiState::disconnected("w".into(), "s".into(), "e".into()),
            &thread,
            true,
            48,
        );
        let rendered = row.to_string();

        assert!(row.width() <= 48);
        assert!(rendered.ends_with("..."));
        assert!(rendered.contains("unloaded"));
    }

    #[test]
    fn renders_approval_modal() {
        let mut state = TuiState::connected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            HelloResult {
                daemon_version: "0.1.0".into(),
                workspace_id: "work-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![],
                daemon_scope: None,
            },
            Vec::new(),
            TranscriptState::None,
        );
        state.approval = Some(approval_from_snapshot(PendingApprovalSnapshot {
            run_id: "run_1".into(),
            tool_call_id: "call_1".into(),
            tool_name: "file.write".into(),
            effect: EffectClass::WorkspaceWrite,
            reason: Some("file.write requires approval".into()),
            input_preview: Some(r#"{"path":"scratch.txt"}"#.into()),
            approval_preview: None,
            diff_preview: None,
        }));

        let output = render_to_text(&state);

        assert!(output.contains("Approval"));
        assert!(output.contains("file.write"));
        assert!(output.contains("workspace_write"));
        assert!(output.contains("scratch.txt"));
        assert!(output.contains("g allow once"));
        assert!(output.contains("d deny"));
        assert!(!output.contains("s allow shell.exec for session"));
    }

    #[test]
    fn renders_approval_modal_diff_preview_when_present() {
        let mut state = TuiState::connected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            HelloResult {
                daemon_version: "0.1.0".into(),
                workspace_id: "work-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![],
                daemon_scope: None,
            },
            Vec::new(),
            TranscriptState::None,
        );
        state.approval = Some(ApprovalModalView {
            run_id: "run_1".into(),
            tool_call_id: "call_1".into(),
            tool_name: "file.edit".into(),
            effect: "WorkspaceWrite".into(),
            reason: "file.edit requires approval".into(),
            input_preview: r#"{"path":"scratch.txt"}"#.into(),
            approval_preview: None,
            diff_preview: Some("--- a/scratch.txt\n+++ b/scratch.txt\n-old\n+new\n".into()),
        });

        let initial = render_to_text(&state);
        state.approval_scroll_offset = usize::MAX;
        let scrolled = render_to_text(&state);
        let output = format!("{initial}\n{scrolled}");

        assert!(output.contains("input preview:"));
        assert!(output.contains("scratch.txt"));
        assert!(output.contains("diff preview"));
        assert!(output.contains("--- a/scratch.txt"));
        assert!(output.contains("-old"));
        assert!(output.contains("+new"));
    }

    #[test]
    fn renders_approval_modal_controls_with_long_diff_preview() {
        let mut state = TuiState::connected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            HelloResult {
                daemon_version: "0.1.0".into(),
                workspace_id: "work-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![],
                daemon_scope: None,
            },
            Vec::new(),
            TranscriptState::None,
        );
        let body = (0..40)
            .map(|line| format!("-old-{line}\n+new-{line}\n"))
            .collect::<String>();
        state.approval = Some(ApprovalModalView {
            run_id: "run_1".into(),
            tool_call_id: "call_1".into(),
            tool_name: "file.edit".into(),
            effect: "WorkspaceWrite".into(),
            reason: "file.edit requires approval".into(),
            input_preview: r#"{"path":"scratch.txt"}"#.into(),
            approval_preview: None,
            diff_preview: Some(format!("--- a/scratch.txt\n+++ b/scratch.txt\n{body}")),
        });

        let initial = render_to_text(&state);
        state.approval_scroll_offset = usize::MAX;
        let scrolled = render_to_text(&state);
        let output = format!("{initial}\n{scrolled}");

        assert!(output.contains("g allow once"));
        assert!(output.contains("d deny"));
        assert!(output.contains("input preview:"));
        assert!(output.contains("scratch.txt"));
        assert!(output.contains("diff preview"));
        assert!(output.contains("--- a/scratch.txt"));
        assert!(output.contains("-old-39"));
        assert!(output.contains("+new-39"));
    }

    #[test]
    fn renders_approval_modal_approval_preview_when_present() {
        let mut state = TuiState::connected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            HelloResult {
                daemon_version: "0.1.0".into(),
                workspace_id: "work-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![],
                daemon_scope: None,
            },
            Vec::new(),
            TranscriptState::None,
        );
        state.approval = Some(ApprovalModalView {
            run_id: "run_1".into(),
            tool_call_id: "call_1".into(),
            tool_name: "shell.exec".into(),
            effect: "external_side_effect".into(),
            reason: "shell.exec requires approval".into(),
            input_preview: r#"{"command":"cargo test"}"#.into(),
            approval_preview: Some("command: cargo test\ncwd: /tmp/work".into()),
            diff_preview: None,
        });

        let initial = render_to_text(&state);
        state.approval_scroll_offset = usize::MAX;
        let scrolled = render_to_text(&state);
        let output = format!("{initial}\n{scrolled}");

        assert!(output.contains("g allow once"));
        assert!(output.contains("s allow shell.exec for session"));
        assert!(output.contains("d deny"));
        assert!(output.contains("input preview:"));
        assert!(output.contains(r#"{"command":"cargo test"}"#));
        assert!(output.contains("approval preview"));
        assert!(output.contains("command: cargo test"));
        assert!(output.contains("cwd: /tmp/work"));
    }

    #[test]
    fn approval_pane_scroll_keeps_transcript_and_composer_visible_at_normal_and_narrow_sizes() {
        let mut state = TuiState::connected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            HelloResult {
                daemon_version: "0.1.0".into(),
                workspace_id: "work-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![],
                daemon_scope: None,
            },
            Vec::new(),
            TranscriptState::Loaded(
                TranscriptReadResult {
                    run_id: "run_1".into(),
                    status: RunStateName::Running,
                    final_answer: None,
                    transcript: "[turn_1] user: TRANSCRIPT_VISIBLE\n".into(),
                    typed: Some(TypedTranscript {
                        runs: vec![TypedRun {
                            run_id: "run_1".into(),
                            session_index: 0,
                            status: RunStateName::Running,
                            model_status: None,
                            entries: vec![TypedTranscriptEntry::User {
                                text: "TRANSCRIPT_VISIBLE".into(),
                            }],
                        }],
                    }),
                    pending_approval: None,
                    completion_claim: None,
                }
                .into(),
            ),
        );
        state.set_composer_text("COMPOSER_VISIBLE");
        state.approval = Some(ApprovalModalView {
            run_id: "run_scroll".into(),
            tool_call_id: "call_scroll".into(),
            tool_name: "shell.exec".into(),
            effect: "external_side_effect".into(),
            reason: "shell.exec requires approval".into(),
            input_preview: "{\n  \"command\": \"printf proof\",\n  \"cwd\": \"/tmp/work\",\n  \"timeout_seconds\": 600,\n  \"env\": {\"VISIBLE\": \"yes\"}\n}"
                .into(),
            approval_preview: Some(
                "command: printf proof\ncwd: /tmp/work\ntimeout: 600s\neffect: ExternalSideEffect\nenv: scrubbed allowlist"
                    .into(),
            ),
            diff_preview: Some(
                "--- a/scratch.txt\n+++ b/scratch.txt\n-old value\n+new value\n".into(),
            ),
        });

        for (width, height) in [(100, 24), (48, 24)] {
            let mut all_scroll_positions = String::new();
            for offset in 0..=64 {
                state.approval_scroll_offset = offset;
                let snapshot = render_snapshot(&state, width, height).unwrap();
                assert!(snapshot.contains("TRANSCRIPT_VISIBLE"), "{snapshot}");
                assert!(snapshot.contains("COMPOSER_VISIBLE"), "{snapshot}");
                all_scroll_positions.push_str(&snapshot);
            }

            for expected in [
                "g allow once",
                "s allow shell.exec for session",
                "d deny",
                "run_scroll",
                "call_scroll",
                "shell.exec (external_side_effect)",
                "shell.exec requires approval",
                "input preview:",
                "\"command\": \"printf proof\"",
                "\"cwd\": \"/tmp/work\"",
                "\"timeout_seconds\": 600",
                "\"env\": {\"VISIBLE\": \"yes\"}",
                "approval preview:",
                "command: printf proof",
                "cwd: /tmp/work",
                "timeout: 600s",
                "effect: ExternalSideEffect",
                "env: scrubbed allowlist",
                "diff preview:",
                "--- a/scratch.txt",
                "+++ b/scratch.txt",
                "-old value",
                "+new value",
            ] {
                assert!(
                    all_scroll_positions.contains(expected),
                    "missing {expected}"
                );
            }
        }
    }

    fn history_cache_state(transcript: &str, live_event: LiveEventLine) -> TuiState {
        let mut state = TuiState::connected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            HelloResult {
                daemon_version: "0.1.0".into(),
                workspace_id: "work-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![],
                daemon_scope: None,
            },
            Vec::new(),
            TranscriptState::Loaded(
                TranscriptReadResult {
                    run_id: "run_1".into(),
                    status: RunStateName::Finished,
                    final_answer: None,
                    transcript: transcript.into(),
                    typed: None,
                    pending_approval: None,
                    completion_claim: None,
                }
                .into(),
            ),
        );
        state.live_events.push(live_event);
        state
    }

    fn conversation_fixture() -> TuiState {
        let mut state = TuiState::connected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            HelloResult {
                daemon_version: "0.1.0".into(),
                workspace_id: "work-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![],
                daemon_scope: None,
            },
            Vec::new(),
            TranscriptState::Loaded(
                TranscriptReadResult {
                    run_id: "run_beta_full_identifier".into(),
                    status: RunStateName::Failed,
                    final_answer: Some("Second answer stays readable.".into()),
                    transcript: "run_id: run_alpha_full_identifier\n[turn_alpha] user: First question asks for a concise summary.\n[turn_alpha] assistant: \n[turn_alpha] tool_call call_alpha file.read {\\\"path\\\":\\\"README.md\\\"}\ntool_result call_alpha README loaded\n[turn_alpha] assistant: First answer is short and clear.\nrun_id: run_beta_full_identifier\n[turn_beta] user: Second question remains readable at narrow widths.\n[turn_beta] assistant: Second answer stays readable.\ntool_failed call_beta: permission denied\n".into(),
                    typed: Some(TypedTranscript {
                        runs: vec![
                            TypedRun {
                                run_id: "run_alpha_full_identifier".into(),
                                session_index: 0,
                                status: RunStateName::Finished,
                                model_status: None,
                                entries: vec![
                                    TypedTranscriptEntry::User {
                                        text: "First question asks for a concise summary.".into(),
                                    },
                                    TypedTranscriptEntry::Assistant {
                                        text: String::new(),
                                    },
                                    TypedTranscriptEntry::ToolCall {
                                        call_id: "call_alpha".into(),
                                        tool: "file.read".into(),
                                        input: serde_json::json!({"path": "README.md"}),
                                    },
                                    TypedTranscriptEntry::ToolResult {
                                        call_id: "call_alpha".into(),
                                        summary: "README loaded".into(),
                                    },
                                    TypedTranscriptEntry::Assistant {
                                        text: "First answer is short and clear.".into(),
                                    },
                                ],
                            },
                            TypedRun {
                                run_id: "run_beta_full_identifier".into(),
                                session_index: 1,
                                status: RunStateName::Failed,
                                model_status: None,
                                entries: vec![
                                    TypedTranscriptEntry::User {
                                        text: "Second question remains readable at narrow widths."
                                            .into(),
                                    },
                                    TypedTranscriptEntry::Assistant {
                                        text: "Second answer stays readable.".into(),
                                    },
                                    TypedTranscriptEntry::ToolFailed {
                                        call_id: "call_beta".into(),
                                        error: "permission denied".into(),
                                    },
                                ],
                            },
                        ],
                    }),
                    pending_approval: None,
                completion_claim: None,
                }
                .into(),
            ),
        );
        state.active_model = Some(ModelIdentityStatus::Requested {
            model: "openrouter/auto".into(),
        });
        state.live_events = vec![
            LiveEventLine::assistant(Some(41), "Second answer stays readable.")
                .with_run_id("run_beta_full_identifier"),
            LiveEventLine::warning(Some(42), "permission denied for call_beta")
                .with_run_id("run_beta_full_identifier"),
        ];
        state
    }

    fn status_fixture() -> DaemonStatusResult {
        serde_json::from_value(serde_json::json!({
            "model": {
                "requested_alias": "~openai/gpt-latest",
                "served_model": "openai/gpt-5.5-2026-08-01",
                "provider_kind": "open_router",
                "key_present": true
            },
            "daemon": {
                "package_version": "0.1.0",
                "build_commit": "0123456789abcdef0123456789abcdef01234567",
                "build_date_utc": "2026-08-01",
                "uptime_ms": 42,
                "endpoint_path": "/tmp/agent.sock",
                "workspace_id": "work-1234"
            },
            "session": {
                "session_id": "session_1",
                "latest_run_id": "run_2",
                "human_turn_count": 2,
                "ledger_path": "/tmp/agent.db",
                "core_event_count": 17
            },
            "usage": {
                "last_run": {
                    "input_tokens": 7,
                    "output_tokens": 3,
                    "unknown_response_count": 1
                },
                "session": {
                    "input_tokens": 17,
                    "output_tokens": 8,
                    "unknown_response_count": 2
                }
            },
            "trust": {
                "approval_granted_count": 2,
                "approval_denied_count": 1,
                "shell_session_grant": true,
                "approval_profile": "yolo"
            }
        }))
        .unwrap()
    }

    fn approval_trace_fixture() -> TuiState {
        let mut state = TuiState::connected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            HelloResult {
                daemon_version: "0.1.0".into(),
                workspace_id: "work-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![],
                daemon_scope: None,
            },
            Vec::new(),
            TranscriptState::Loaded(
                TranscriptReadResult {
                    run_id: "run_approval".into(),
                    status: RunStateName::Running,
                    final_answer: None,
                    transcript: "[turn_approval] user: Review the proposed edit.\n".into(),
                    typed: Some(TypedTranscript {
                        runs: vec![TypedRun {
                            run_id: "run_approval".into(),
                            session_index: 0,
                            status: RunStateName::Running,
                            model_status: None,
                            entries: vec![TypedTranscriptEntry::User {
                                text: "Review the proposed edit.".into(),
                            }],
                        }],
                    }),
                    pending_approval: None,
                    completion_claim: None,
                }
                .into(),
            ),
        );
        state.active_run = Some(ActiveRunView::new(
            "run_approval".into(),
            RunStateName::Running,
        ));
        state.live_events = vec![
            LiveEventLine::warning(Some(4), "approval pending file.write (workspace_write)")
                .with_run_id("run_approval"),
            LiveEventLine::approval(Some(5), "approval granted call_approval")
                .with_run_id("run_approval"),
        ];
        state
    }

    fn focused_snapshot(state: &TuiState, width: u16, height: u16) -> String {
        let output = render_snapshot(state, width, height).unwrap();
        let mut lines = Vec::new();
        for line in output.lines().map(str::trim_end) {
            if line.is_empty()
                && lines
                    .last()
                    .is_some_and(|previous: &&str| previous.is_empty())
            {
                continue;
            }
            lines.push(line);
        }
        while lines.first().is_some_and(|line| line.is_empty()) {
            lines.remove(0);
        }
        while lines.last().is_some_and(|line| line.is_empty()) {
            lines.pop();
        }
        lines.join("\n")
    }

    fn render_main_snapshot(state: &TuiState, width: u16, height: u16) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|frame| render_main(frame, state)).unwrap();
        terminal_buffer_text(&terminal)
    }

    fn render_overlay_snapshot(
        state: &TuiState,
        width: u16,
        height: u16,
        scroll_offset: usize,
    ) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| render_overlay_at(frame, state, scroll_offset, 0))
            .unwrap();
        terminal_buffer_text(&terminal)
    }

    fn terminal_buffer_text(terminal: &Terminal<TestBackend>) -> String {
        let buffer = terminal.backend().buffer();
        let area = buffer.area;
        let mut output = String::new();
        for y in area.top()..area.bottom() {
            for x in area.left()..area.right() {
                output.push_str(buffer[(x, y)].symbol());
            }
            output.push('\n');
        }
        output
    }

    fn assert_selected_row_accent(state: &TuiState, width: u16, needle: &str) {
        let backend = TestBackend::new(width, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| render_overlay_at(frame, state, 0, 0))
            .unwrap();
        let buffer = terminal.backend().buffer();
        let area = buffer.area;
        let row = (area.top()..area.bottom())
            .find(|y| {
                let text = (area.left()..area.right())
                    .map(|x| buffer[(x, *y)].symbol())
                    .collect::<String>();
                text.contains(needle)
            })
            .unwrap_or_else(|| panic!("missing selected row {needle:?} at width {width}"));
        let start = (area.left()..area.right())
            .find(|x| buffer[(*x, row)].symbol() == ">")
            .expect("selected row marker");
        let styled_cells: Vec<_> = (start..area.right())
            .filter(|x| {
                let symbol = buffer[(*x, row)].symbol();
                !symbol.trim().is_empty() && symbol != "│"
            })
            .collect();

        assert!(
            styled_cells.len() >= 8,
            "selected row was truncated: {needle}"
        );
        for x in styled_cells {
            let modifiers = buffer[(x, row)].modifier;
            assert!(
                modifiers.contains(Modifier::BOLD | Modifier::REVERSED),
                "selected row cell at ({x}, {row}) lacked its full accent at width {width}"
            );
        }
    }

    fn cached_row_ptrs(state: &TuiState) -> (*const Line<'static>, *const Line<'static>) {
        let transcript = state.history_rows.transcript.read().unwrap();
        let live_events = state.history_rows.live_events.read().unwrap();
        (
            transcript.as_ref().unwrap().rows.as_ptr(),
            live_events.as_ref().unwrap().rows.as_ptr(),
        )
    }

    fn transcript_cache_key(state: &TuiState) -> TranscriptRowsKey {
        state
            .history_rows
            .transcript
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .key
            .clone()
    }

    fn render_to_text(state: &TuiState) -> String {
        render_snapshot(state, 100, 24).unwrap()
    }

    fn render_to_text_at(state: &TuiState, now_ms: u64) -> String {
        render_snapshot_at(state, 100, 24, now_ms).unwrap()
    }

    fn test_thread(thread_id: &str, loaded: bool, active: bool) -> platonic_protocol::ThreadStatus {
        serde_json::from_value(serde_json::json!({
            "authority": {
                "thread_id": thread_id,
                "parent_thread_id": null,
                "spawning_actor": "test",
                "cwd": "/tmp/work",
                "model": "test-model",
                "reasoning_effort": "none",
                "approval_policy": "prompt",
                "created_at_ms": 1
            },
            "live": {
                "loaded": loaded,
                "current_turn_id": active.then_some("turn_active")
            }
        }))
        .unwrap()
    }

    fn render_cursor_position(state: &TuiState, width: u16, height: u16) -> (u16, u16) {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| render_overlay_at(frame, state, 0, 0))
            .unwrap();
        let position = terminal.backend_mut().get_cursor_position().unwrap();
        (position.x, position.y)
    }
}
