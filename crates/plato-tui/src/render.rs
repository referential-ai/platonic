use ratatui::{
    Frame, Terminal,
    backend::TestBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph, Wrap},
};

use super::{
    ApprovalModalView, ConnectionState, LiveEventKind, TranscriptState, TuiState,
    markdown::{DEFAULT_SYNTAX_THEME, MarkdownRenderer, SyntaxTheme},
    state::{
        CachedLiveEventRows, CachedTranscriptRows, DisplayMode, LiveEventRowsKey,
        TranscriptRowsKey, session_question_label,
    },
};
use crate::commands::{SLASH_COMMANDS, matching_slash_commands};
use plato_protocol::{
    DaemonStatusResult, DaemonStatusTokenUsage, ModelIdentityStatus, RunStateName, TypedRun,
    TypedTranscriptEntry,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const SESSION_STATUS_WIDTH: usize = 16;
const SESSION_AGE_WIDTH: usize = 5;
const SESSION_QUESTION_MAX_CHARS: usize = 72;

/// Renders the current client state into a terminal frame.
pub fn render(frame: &mut Frame<'_>, state: &TuiState) {
    render_at(frame, state, unix_now_ms());
}

fn render_at(frame: &mut Frame<'_>, state: &TuiState, now_ms: u64) {
    let [history, composer, status] = vertical(frame.area(), state);
    render_history(frame, history, state);
    render_composer(frame, composer, state);
    render_status_line(frame, status, state);
    if state.help_visible {
        render_help_modal(frame, frame.area());
    }
    if state.session_picker.is_some() {
        render_session_picker(frame, frame.area(), state, now_ms);
    }
    if let Some(approval) = &state.approval {
        render_approval_modal(frame, frame.area(), approval);
    }
    if let Some(status) = &state.status_modal {
        render_status_modal(frame, frame.area(), status);
    }
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
    terminal.draw(|frame| render_at(frame, state, now_ms))?;
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

fn render_history(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    let mut lines = history_lines(state, area.width);
    if lines.is_empty() {
        lines.push(Line::from(""));
    }
    let paragraph = Paragraph::new(lines).wrap(Wrap { trim: false });
    let bottom = paragraph
        .line_count(area.width.max(1))
        .saturating_sub(area.height as usize);
    let scroll = bottom.saturating_sub(state.scroll_offset);
    frame.render_widget(
        paragraph.scroll((u16::try_from(scroll).unwrap_or(u16::MAX), 0)),
        area,
    );
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
                    Style::default().fg(Color::Yellow),
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
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            )]));
            if let ConnectionState::Disconnected { error } = &state.connection {
                lines.push(Line::from(error.clone()));
            }
            lines.push(Line::from(""));
            lines.push(Line::from(
                "Start plato-agentd manually, then press r to reconnect.",
            ));
            lines.push(Line::from(format!(
                "cargo run --bin plato-agentd -- --workspace {}",
                state.workspace_root
            )));
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
                Style::default().fg(Color::Yellow),
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
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            )]));
            if let ConnectionState::Disconnected { error } = &state.connection {
                lines.push(Line::from(error.clone()));
            }
            lines.push(Line::from(""));
            lines.push(Line::from(
                "Start plato-agentd manually, then press r to reconnect.",
            ));
            lines.push(Line::from(format!(
                "cargo run --bin plato-agentd -- --workspace {}",
                state.workspace_root
            )));
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
        Line::from(vec![Span::styled(
            "Plato Agent",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )]),
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
                Span::styled("workspace ", Style::default().fg(Color::DarkGray)),
                Span::raw(workspace_id.clone()),
            ]),
            Line::from(vec![
                Span::styled("daemon    ", Style::default().fg(Color::DarkGray)),
                Span::raw(daemon_identity_label(daemon_version)),
            ]),
            Line::from(vec![
                Span::styled("ledger    ", Style::default().fg(Color::DarkGray)),
                Span::raw(ledger_path.clone()),
            ]),
            Line::from(vec![
                Span::styled("cwd       ", Style::default().fg(Color::DarkGray)),
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
        Style::default().fg(Color::Yellow),
    )]));

    if let Some(active) = &state.active_run {
        lines.push(status_row(format!("{} {}", active.status, active.run_id)));
    }
    if let Some((marker, elapsed)) = issue_prep_activity(state) {
        lines.push(status_row(format!(
            "issue prep {marker} {}",
            format_elapsed(elapsed)
        )));
    } else if let Some(message) = &state.status_message {
        lines.push(status_row(message.clone()));
    }
    if let Some(warning) = &state.stream_warning {
        lines.push(warning_row(format!("stream warning {warning}")));
    }
    append_audit_live_event_rows(lines, state, width, syntax_theme);
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
                            || event.text.starts_with("issue-prep")))
                        || (event.kind == LiveEventKind::Status
                            && event.text.starts_with("issue-prep artifacts:"))) =>
            {
                let color = if event.kind == LiveEventKind::Warning {
                    Color::Red
                } else {
                    Color::DarkGray
                };
                push_notice_rows(&mut lines, &event.text, color);
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

fn push_notice_rows(lines: &mut Vec<Line<'static>>, text: &str, color: Color) {
    if !lines.is_empty() {
        lines.push(Line::from(""));
    }
    lines.push(Line::from(Span::styled(
        "Notice",
        Style::default().fg(color).add_modifier(Modifier::BOLD),
    )));
    lines.extend(text.lines().map(|line| {
        Line::from(vec![
            Span::raw("  "),
            Span::styled(line.to_owned(), Style::default().fg(color)),
        ])
    }));
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
        LiveEventKind::User => (
            "You",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        LiveEventKind::Assistant | LiveEventKind::AssistantDelta => (
            "Plato",
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
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
                lines.push(Line::from("  "));
            } else {
                lines.extend(text_lines.map(|line| {
                    Line::from(vec![
                        Span::raw("  "),
                        Span::styled(line.to_owned(), Style::default().fg(Color::Cyan)),
                    ])
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
        Span::styled("Trace  ", Style::default().fg(Color::DarkGray)),
        Span::styled(summary, Style::default().fg(Color::DarkGray)),
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
        Style::default().fg(Color::Yellow),
    )]));
    lines.extend(
        state
            .queued_messages
            .iter()
            .enumerate()
            .map(|(index, message)| Line::from(format!("{} {}", index + 1, message))),
    );
}

fn render_status_line(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    frame.render_widget(Paragraph::new(status_line(state, area.width)), area);
}

fn status_line(state: &TuiState, width: u16) -> Line<'static> {
    let (run_status, elapsed) = if let Some((marker, elapsed)) = issue_prep_activity(state) {
        (format!("issue prep {marker}"), format_elapsed(elapsed))
    } else {
        (
            state
                .active_run
                .as_ref()
                .map(|run| run.status.as_str())
                .unwrap_or("ready")
                .into(),
            state
                .active_run_elapsed_secs
                .map(format_elapsed)
                .unwrap_or_else(|| "0s".into()),
        )
    };
    let model = model_status_label(state.active_model.as_ref());
    let connection = match &state.connection {
        ConnectionState::Connected { .. } => "online",
        ConnectionState::Disconnected { .. } => "offline",
    };
    let identity = match &state.connection {
        ConnectionState::Connected { daemon_version, .. } => daemon_identity_label(daemon_version),
        ConnectionState::Disconnected { .. } => "provenance unknown".into(),
    };
    let queued = state.queued_messages.len();
    let mode = match state.display_mode {
        DisplayMode::Conversation => "conversation",
        DisplayMode::Audit => "audit",
    };
    let full = format!(
        "{connection} {identity} | {run_status} {elapsed} | {model} | queued {queued} | {mode}"
    );
    let medium = format!("{connection} | {run_status} | {model} | q {queued} | {mode}");
    let short_connection = if connection == "online" { "on" } else { "off" };
    let short_mode = if state.display_mode == DisplayMode::Conversation {
        "chat"
    } else {
        "audit"
    };
    let short = format!("{short_connection} | {run_status} | {model} | q{queued} | {short_mode}");
    let compact = format!("{short_connection} | {run_status} | {model}");
    let text = [full, medium, short, compact]
        .into_iter()
        .find(|candidate| candidate.chars().count() <= usize::from(width))
        .unwrap_or(model);
    Line::from(Span::styled(
        bounded_status_text(text, width),
        Style::default().fg(Color::DarkGray),
    ))
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

fn bounded_status_text(mut text: String, width: u16) -> String {
    let width = usize::from(width);
    if Line::from(text.as_str()).width() <= width {
        return text;
    }
    if width == 0 {
        return String::new();
    }
    while Line::from(text.as_str()).width() >= width {
        if text.pop().is_none() {
            break;
        }
    }
    text.push('~');
    text
}

fn issue_prep_activity(state: &TuiState) -> Option<(&'static str, u64)> {
    let elapsed = state.issue_prep_started_at?.elapsed();
    Some((activity_marker(elapsed), elapsed.as_secs()))
}

fn activity_marker(elapsed: Duration) -> &'static str {
    match (elapsed.as_millis() / 200) % 4 {
        0 => ".",
        1 => ":",
        2 => "*",
        _ => "+",
    }
}

fn render_composer(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    let mut lines = slash_popup_lines(state);
    let mut composer_lines = if state.composer.is_empty() {
        vec![Line::from(vec![
            Span::styled(
                ">",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(" "),
            Span::styled("|", Style::default().fg(Color::Yellow)),
            Span::raw(" "),
            Span::styled(
                "Try \"read README.md and summarize it\"",
                Style::default().fg(Color::DarkGray),
            ),
        ])]
    } else {
        composer_with_cursor(state)
            .lines()
            .enumerate()
            .map(|(index, line)| {
                let prefix = if index == 0 { ">" } else { "|" };
                Line::from(vec![
                    Span::styled(
                        prefix,
                        Style::default()
                            .fg(Color::Yellow)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::raw(format!(" {line}")),
                ])
            })
            .collect()
    };
    lines.append(&mut composer_lines);
    frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), area);
}

fn slash_popup_lines(state: &TuiState) -> Vec<Line<'static>> {
    let Some(popup) = &state.slash_popup else {
        return Vec::new();
    };
    let matches = matching_slash_commands(&popup.filter);
    if matches.is_empty() {
        return vec![Line::from(Span::styled(
            "  no commands match",
            Style::default().fg(Color::DarkGray),
        ))];
    }
    matches
        .into_iter()
        .take(5)
        .enumerate()
        .map(|(index, command)| {
            let style = if index == popup.selected {
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::DarkGray)
            };
            Line::from(vec![
                Span::styled(if index == popup.selected { "> " } else { "  " }, style),
                Span::styled(format!("/{}", command.name), style),
                Span::raw("  "),
                Span::styled(
                    command.description.to_owned(),
                    Style::default().fg(Color::DarkGray),
                ),
            ])
        })
        .collect()
}

fn plural(count: usize) -> &'static str {
    if count == 1 { "" } else { "s" }
}

fn event_rows(event: &super::LiveEventLine) -> Vec<Line<'static>> {
    let (role, color) = match event.kind {
        LiveEventKind::User => ("user", Color::Cyan),
        LiveEventKind::Assistant | LiveEventKind::AssistantDelta => ("assistant", Color::Green),
        LiveEventKind::Tool => ("tool", Color::Magenta),
        LiveEventKind::Approval | LiveEventKind::Status => ("status", Color::DarkGray),
        LiveEventKind::Warning => ("warning", Color::Red),
    };
    let mut text_lines = event.text.lines();
    let first = text_lines.next().unwrap_or_default();
    let first = match event.offset {
        Some(offset) => format!("#{offset} {first}"),
        None => first.to_owned(),
    };
    let mut rows = vec![role_row(role, color, &first)];
    rows.extend(text_lines.map(|line| role_row("", color, line)));
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
        return Some(role_row("user", Color::Cyan, text));
    }
    if let Some(text) = turn_text(line, "assistant: ") {
        return Some(role_row("assistant", Color::Green, text));
    }
    if let Some(text) = turn_text(line, "tool: ") {
        return Some(role_row("tool", Color::Magenta, text));
    }
    if let Some(text) = turn_text(line, "tool_call ") {
        return Some(role_row("tool", Color::Magenta, text));
    }
    if let Some(text) = line.strip_prefix("tool_result ") {
        return Some(role_row("tool", Color::Magenta, text));
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

fn role_row(role: &'static str, color: Color, text: &str) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{role:<9} "), Style::default().fg(color)),
        Span::raw(text.to_owned()),
    ])
}

fn status_row(text: impl Into<String>) -> Line<'static> {
    role_row("status", Color::DarkGray, &text.into())
}

fn warning_row(text: impl Into<String>) -> Line<'static> {
    role_row("warning", Color::Red, &text.into())
}

fn composer_with_cursor(state: &TuiState) -> String {
    let mut draft = state.composer.clone();
    let mut cursor = state.composer_cursor.min(draft.len());
    while !draft.is_char_boundary(cursor) {
        cursor -= 1;
    }
    draft.insert(cursor, '|');
    draft
}

fn format_elapsed(seconds: u64) -> String {
    let minutes = seconds / 60;
    let seconds = seconds % 60;
    if minutes == 0 {
        format!("{seconds}s")
    } else {
        format!("{minutes}m{seconds:02}s")
    }
}

fn render_help_modal(frame: &mut Frame<'_>, area: Rect) {
    let area = centered_rect(68, 100, area);
    let mut lines = vec![Line::from(vec![Span::styled(
        "Commands",
        Style::default().add_modifier(Modifier::BOLD),
    )])];
    for command in SLASH_COMMANDS
        .iter()
        .filter(|command| command.name != "exit")
    {
        lines.push(Line::from(format!(
            "/{:<10} {}",
            command.name, command.description
        )));
    }
    lines.extend([
        Line::from(""),
        Line::from(vec![Span::styled(
            "Keys",
            Style::default().add_modifier(Modifier::BOLD),
        )]),
        Line::from("Enter        submit"),
        Line::from("Shift-Enter  newline"),
        Line::from("Alt-Enter    newline"),
        Line::from("Ctrl-J/M     newline"),
        Line::from("Tab          complete command or submit/queue"),
        Line::from("v            toggle conversation/audit"),
        Line::from("PgUp/PgDown  scroll"),
        Line::from("Up/Down      input history"),
        Line::from("Ctrl-C       cancel active run"),
        Line::from("Esc or q     close"),
    ]);
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(lines)
            .block(Block::default().borders(Borders::ALL).title("Help"))
            .wrap(Wrap { trim: false }),
        area,
    );
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
    let height = area.height.clamp(1, 22);
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
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
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

fn render_session_picker(frame: &mut Frame<'_>, area: Rect, state: &TuiState, now_ms: u64) {
    let area = centered_rect(78, 64, area);
    let row_width = area.width.saturating_sub(2);
    let picker = state
        .session_picker
        .as_ref()
        .expect("session picker is open");
    let sessions = picker.matching_sessions(&state.sessions);
    let mut lines = vec![
        Line::from(vec![Span::styled(
            "Sessions",
            Style::default().add_modifier(Modifier::BOLD),
        )]),
        Line::from("Type to filter    Backspace edit    Esc close"),
        Line::from("Up/Down or Ctrl-P/Ctrl-N move    Enter resume"),
        Line::from(format!("Filter: {}|", picker.filter)),
        Line::from(""),
    ];
    if sessions.is_empty() && state.sessions.is_empty() && picker.filter.is_empty() {
        lines.push(Line::from("No sessions"));
    } else if sessions.is_empty() {
        lines.push(Line::from("No matching sessions"));
    } else {
        lines.extend(sessions.iter().enumerate().map(|(index, session)| {
            session_picker_row(state, session, index == picker.selected, now_ms, row_width)
        }));
    }
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(lines)
            .block(Block::default().borders(Borders::ALL).title("Sessions"))
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn session_picker_row(
    state: &TuiState,
    session: &plato_protocol::SessionSummary,
    focused: bool,
    now_ms: u64,
    row_width: u16,
) -> Line<'static> {
    let focus = if focused { ">" } else { " " };
    let current = if state.selected_session_id.as_deref() == Some(session.session_id.as_str()) {
        "*"
    } else {
        " "
    };
    let style = if focused {
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    };
    let age = relative_age(session.updated_at_ms, now_ms);
    let prefix_width = 3 + SESSION_STATUS_WIDTH + 1 + SESSION_AGE_WIDTH + 1;
    let question_width = usize::from(row_width)
        .saturating_sub(prefix_width)
        .min(SESSION_QUESTION_MAX_CHARS);
    let question = bounded_question_preview(session_question_label(session), question_width);
    Line::from(vec![
        Span::styled(format!("{focus}{current} "), style),
        Span::styled(
            format!("{:<SESSION_STATUS_WIDTH$}", session.status),
            status_style(&session.status),
        ),
        Span::raw(" "),
        Span::styled(
            format!("{age:>SESSION_AGE_WIDTH$}"),
            Style::default().fg(Color::DarkGray),
        ),
        Span::raw(" "),
        Span::raw(question),
    ])
}

fn relative_age(updated_at_ms: u64, now_ms: u64) -> String {
    if updated_at_ms == 0 {
        return "--".into();
    }
    let elapsed_ms = now_ms.saturating_sub(updated_at_ms);
    if elapsed_ms < 60_000 {
        format!("{}s", elapsed_ms / 1_000)
    } else if elapsed_ms < 3_600_000 {
        format!("{}m", elapsed_ms / 60_000)
    } else if elapsed_ms < 86_400_000 {
        format!("{}h", elapsed_ms / 3_600_000)
    } else {
        let days = elapsed_ms / 86_400_000;
        if days > 999 {
            "999d+".into()
        } else {
            format!("{days}d")
        }
    }
}

fn bounded_question_preview(question: &str, max_chars: usize) -> String {
    let line = question.lines().next().unwrap_or_default();
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

fn status_style(status: &RunStateName) -> Style {
    match status {
        RunStateName::Running => Style::default().fg(Color::Green),
        RunStateName::Interrupted => Style::default().fg(Color::Yellow),
        RunStateName::Failed | RunStateName::Canceled => Style::default().fg(Color::Red),
        _ => Style::default().fg(Color::DarkGray),
    }
}

fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().try_into().unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn render_approval_modal(frame: &mut Frame<'_>, area: Rect, approval: &ApprovalModalView) {
    let area = centered_rect(74, 64, area);
    let mut lines = vec![
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
        Line::from("g grant    d deny    Ctrl-C cancel run    q quit TUI"),
        Line::from(""),
    ];
    let preview = approval
        .diff_preview
        .as_deref()
        .map(|preview| ("diff preview:", preview))
        .or_else(|| {
            approval
                .approval_preview
                .as_deref()
                .map(|preview| ("approval preview:", preview))
        });
    if let Some((title, preview)) = preview {
        lines.push(Line::from(title));
        lines.extend(preview.lines().map(|line| Line::from(line.to_owned())));
    } else {
        lines.push(Line::from("input preview:"));
        lines.push(Line::from(approval.input_preview.clone()));
    }
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(lines)
            .block(Block::default().borders(Borders::ALL).title("Approval"))
            .wrap(Wrap { trim: false }),
        area,
    );
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

fn vertical(area: Rect, state: &TuiState) -> [Rect; 3] {
    let composer_height = composer_height(state);
    Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(1),
            Constraint::Length(composer_height),
            Constraint::Length(1),
        ])
        .areas(area)
}

fn composer_height(state: &TuiState) -> u16 {
    let draft_lines = if state.composer.is_empty() {
        1
    } else {
        state.composer.lines().count().max(1)
    };
    let popup_lines = state
        .slash_popup
        .as_ref()
        .map(|popup| matching_slash_commands(&popup.filter).len().clamp(1, 5))
        .unwrap_or(0);
    (draft_lines + popup_lines).clamp(1, 9) as u16
}

#[cfg(test)]
mod tests {
    use super::*;
    use plato_protocol::{
        ApprovalDecisionName, HelloResult, PendingApprovalSnapshot, SessionSummary,
        TranscriptReadResult, TypedRun, TypedTranscript, TypedTranscriptEntry,
    };
    use platonic_core::EffectClass;

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
        assert!(output.contains("model pending"));
        assert!(output.contains("online 0.1.0 0123456 2026-08-01 | ready"));
        assert!(output.contains("Try \"read README.md and summarize it\""));
        assert!(!output.contains("? help"));
        assert!(!output.contains("v toggle"));
        assert!(!output.contains("Status"));
        assert!(!output.contains("Sessions"));
        assert!(!output.contains("Live Events"));
        assert!(!output.contains("Composer"));
    }

    #[test]
    fn daemon_identity_keeps_unknown_provenance_explicit() {
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
    fn status_chrome_stays_one_bounded_row() {
        let mut state = conversation_fixture();
        state.active_run = Some(ActiveRunView {
            run_id: "run_hidden_identifier".into(),
            status: RunStateName::Running,
        });
        state.active_model = Some(ModelIdentityStatus::Requested {
            model: "model-with-a-very-long-display-name".into(),
        });
        state.queued_messages = vec!["one".into(), "two".into()];

        for width in [0, 8, 24, 48, 96] {
            let line = status_line(&state, width);
            assert!(line.width() <= usize::from(width));
            assert!(!line.to_string().contains("run_hidden_identifier"));
            assert!(!line.to_string().contains("? help"));
            assert!(!line.to_string().contains("v toggle"));
        }

        let [history, composer, status] = vertical(Rect::new(0, 0, 48, 12), &state);
        assert_eq!(history.height, 10);
        assert_eq!(composer.height, 1);
        assert_eq!(status.height, 1);
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
                }
                .into(),
            ),
        );

        let output = render_to_text(&state);

        assert!(output.contains("ready"));
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
            normal_conversation,
            "You\n  First question asks for a concise summary.\n\nPlato\n  First answer is short and clear.\n\nTrace  tools | finished\n\nYou\n  Second question remains readable at narrow widths.\n\nPlato\n  Second answer stays readable.\n\nTrace  tool failed | warning | failed\n\n> | Try \"read README.md and summarize it\"\nonline 0.1.0 unknown unknown | ready 0s | selected openrouter/auto | queued 0 | conversation"
        );
        assert_eq!(
            narrow_conversation,
            "You\n  First question asks for a concise summary.\n\nPlato\n  First answer is short and clear.\n\nTrace  tools | finished\n\nYou\n  Second question remains readable at narrow\nwidths.\n\nPlato\n  Second answer stays readable.\n\nTrace  tool failed | warning | failed\n\n> | Try \"read README.md and summarize it\"\non | ready | selected openrouter/auto"
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
            "status    run run_beta_full_identifier\n\nstatus    run run_alpha_full_identifier\nuser      First question asks for a concise summary.\nassistant\ntool      call_alpha file.read {\\\"path\\\":\\\"README.md\\\"}\ntool      call_alpha README loaded\nassistant First answer is short and clear.\nstatus    run run_beta_full_identifier\nuser      Second question remains readable at narrow widths.\nassistant Second answer stays readable.\nwarning   tool_failed call_beta: permission denied\n\ntranscript\nassistant #41 Second answer stays readable.\nwarning   #42 permission denied for call_beta\n\n> | Try \"read README.md and summarize it\"\nonline 0.1.0 unknown unknown | ready 0s | selected openrouter/auto | queued 0 | audit"
        );
        assert_eq!(
            narrow_audit,
            "status    run run_beta_full_identifier\n\nstatus    run run_alpha_full_identifier\nuser      First question asks for a concise\nsummary.\nassistant\ntool      call_alpha file.read\n{\\\"path\\\":\\\"README.md\\\"}\ntool      call_alpha README loaded\nassistant First answer is short and clear.\nstatus    run run_beta_full_identifier\nuser      Second question remains readable at\nnarrow widths.\nassistant Second answer stays readable.\nwarning   tool_failed call_beta: permission\ndenied\n\ntranscript\nassistant #41 Second answer stays readable.\nwarning   #42 permission denied for call_beta\n\n> | Try \"read README.md and summarize it\"\non | ready | selected openrouter/auto"
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

        assert_eq!(
            you.spans[0].style,
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD)
        );
        assert_eq!(
            plato.spans[0].style,
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD)
        );
        assert_ne!(you.spans[0].style, plato.spans[0].style);
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
        assert!(output.contains("cargo run --bin plato-agentd"));
        assert!(output.contains("press r to reconnect"));
        assert!(output.contains("offline provenance unknown | ready"));
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
            },
            Vec::new(),
            TranscriptState::None,
        );
        state.active_run = Some(ActiveRunView {
            run_id: "run_1".into(),
            status: RunStateName::Running,
        });
        state.composer = "summarize this file".into();
        state.composer_cursor = "summarize".len();
        state
            .live_events
            .push(LiveEventLine::assistant(Some(2), "assistant response"));

        let output = render_to_text(&state);

        assert!(output.contains("running"));
        assert!(!output.contains("run_1"));
        assert!(output.contains("assistant response"));
        assert!(output.contains("> summarize| this file"));
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
            },
            Vec::new(),
            TranscriptState::None,
        );
        state.issue_prep_started_at = Some(std::time::Instant::now() - Duration::from_secs(2));
        state.status_message = Some("issue prep running".into());

        let output = render_to_text(&state);

        assert!(output.contains("issue prep"));
        assert!(output.contains("2s"));
        assert_eq!(activity_marker(Duration::ZERO), ".");
        assert_eq!(activity_marker(Duration::from_millis(200)), ":");
        assert_eq!(activity_marker(Duration::from_millis(400)), "*");
        assert_eq!(activity_marker(Duration::from_millis(600)), "+");
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
            },
            Vec::new(),
            TranscriptState::None,
        );
        state.queued_messages = vec!["queued next".into()];
        state.composer = "first line\nsecond line".into();
        state.composer_cursor = state.composer.len();

        let output = render_to_text(&state);

        assert!(output.contains("queued"));
        assert!(output.contains("queued 1"));
        assert!(output.contains("1 queued next"));
        assert!(output.contains("> first line"));
        assert!(output.contains("| second line|"));
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

        let output = render_to_text(&state);

        assert!(output.contains("1m05s"));
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
            "You\n  Review the proposed edit.\n\nTrace  approval | running\n\n> | Try \"read README.md and summarize it\"\nonline 0.1.0 unknown unknown | running 0s | model pending | queued 0 | conversation"
        );

        state.toggle_display_mode();
        assert_eq!(
            focused_snapshot(&state, 96, 24),
            "status    run run_approval\n\nuser      Review the proposed edit.\n\ntranscript\nstatus    running run_approval\nwarning   #4 approval pending file.write (workspace_write)\nstatus    #5 approval granted call_approval\n\n> | Try \"read README.md and summarize it\"\nonline 0.1.0 unknown unknown | running 0s | model pending | queued 0 | audit"
        );
    }

    #[test]
    fn renders_scrolled_transcript_window() {
        let mut state = TuiState::connected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            HelloResult {
                daemon_version: "0.1.0".into(),
                workspace_id: "work-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![],
            },
            Vec::new(),
            TranscriptState::None,
        );
        state.live_events = (0..30)
            .map(|index| LiveEventLine::status(Some(index), format!("line {index}")))
            .collect();
        state.toggle_display_mode();
        state.scroll_history_up(10);

        let output = render_snapshot(&state, 100, 12).unwrap();

        assert!(output.contains("line 15"));
        assert!(!output.contains("line 29"));
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
    fn renders_help_modal() {
        let mut state = TuiState::connected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            HelloResult {
                daemon_version: "0.1.0".into(),
                workspace_id: "work-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![],
            },
            Vec::new(),
            TranscriptState::None,
        );
        state.help_visible = true;

        let output = render_to_text(&state);

        assert!(output.contains("Help"));
        assert!(output.contains("/help"));
        assert!(output.contains("/status"));
        assert!(output.contains("/clear"));
        assert!(output.contains("/issue-prep"));
        assert!(output.contains("/reconnect"));
        assert!(output.contains("/quit"));
        assert!(output.contains("PgUp/PgDown"));
        assert!(output.contains("toggle conversation/audit"));
        assert!(output.contains("Ctrl-C"));
        assert!(output.contains("Esc or q     close"));
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
            },
            Vec::new(),
            TranscriptState::None,
        );
        state.composer = "/c".into();
        state.composer_cursor = state.composer.len();
        state.slash_popup = Some(super::super::state::SlashPopupView {
            filter: "c".into(),
            selected: 0,
        });

        let output = render_to_text(&state);

        assert!(output.contains("/clear"));
        assert!(output.contains("clear the visible transcript"));
        assert!(output.contains("conversation"));
    }

    #[test]
    fn renders_session_picker_overlay() {
        const NOW_MS: u64 = 172_800_000;
        let mut state = TuiState::connected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            HelloResult {
                daemon_version: "0.1.0".into(),
                workspace_id: "work-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![],
            },
            vec![
                SessionSummary {
                    session_id: "session_1".into(),
                    run_id: "run_1".into(),
                    status: RunStateName::Finished,
                    latest_question: "approved, go ahead".into(),
                    first_question: "read README".into(),
                    updated_at_ms: 172_680_000,
                    ledger_path: "/tmp/agent.db".into(),
                },
                SessionSummary {
                    session_id: "session_2".into(),
                    run_id: "run_2".into(),
                    status: RunStateName::Interrupted,
                    latest_question: "continue docs".into(),
                    first_question: "continue docs".into(),
                    updated_at_ms: 169_200_000,
                    ledger_path: "/tmp/agent.db".into(),
                },
            ],
            TranscriptState::None,
        );
        state.selected_session_id = Some("session_1".into());
        state.session_picker = Some(super::super::state::SessionPickerView {
            filter: String::new(),
            selected: 1,
        });

        let output = render_to_text_at(&state, NOW_MS);

        assert!(output.contains("Sessions"));
        assert!(output.contains("Type to filter"));
        assert!(output.contains("Ctrl-P/Ctrl-N"));
        assert!(output.contains("Enter resume"));
        assert!(output.contains("Filter: |"));
        assert!(output.contains("read README"));
        assert!(output.contains("2m read README"));
        assert!(output.contains("interrupted"));
        assert!(output.contains("1h continue docs"));
        assert!(!output.contains("approved, go ahead"));
        assert!(!output.contains("session_1"));
        assert!(!output.contains("session_2"));
    }

    #[test]
    fn session_picker_renders_filtered_sessions_and_explicit_no_match() {
        let mut state = TuiState::connected(
            "/tmp/work".into(),
            "/tmp/agent.sock".into(),
            HelloResult {
                daemon_version: "0.1.0".into(),
                workspace_id: "work-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![],
            },
            vec![
                SessionSummary {
                    session_id: "session_1".into(),
                    run_id: "run_1".into(),
                    status: RunStateName::Finished,
                    latest_question: "prepare release notes".into(),
                    first_question: "prepare release notes".into(),
                    updated_at_ms: 1,
                    ledger_path: "/tmp/agent.db".into(),
                },
                SessionSummary {
                    session_id: "session_2".into(),
                    run_id: "run_2".into(),
                    status: RunStateName::Interrupted,
                    latest_question: "continue docs".into(),
                    first_question: "continue docs".into(),
                    updated_at_ms: 1,
                    ledger_path: "/tmp/agent.db".into(),
                },
            ],
            TranscriptState::None,
        );
        state.session_picker = Some(super::super::state::SessionPickerView {
            filter: "CONT".into(),
            selected: 0,
        });

        let output = render_to_text_at(&state, 172_800_000);

        assert!(output.contains("Filter: CONT|"));
        assert!(output.contains("continue docs"));
        assert!(!output.contains("prepare release notes"));
        assert!(!output.contains("No matching sessions"));

        state.session_picker.as_mut().unwrap().filter = "missing".into();
        let output = render_to_text_at(&state, 172_800_000);

        assert!(output.contains("Filter: missing|"));
        assert!(output.contains("No matching sessions"));
        assert!(!output.contains("prepare release notes"));
        assert!(!output.contains("continue docs"));
    }

    #[test]
    fn session_picker_relative_age_uses_deterministic_unit_boundaries() {
        const NOW_MS: u64 = 1_000_000_000;
        for (elapsed_ms, expected) in [
            (0, "0s"),
            (999, "0s"),
            (1_000, "1s"),
            (59_999, "59s"),
            (60_000, "1m"),
            (3_599_999, "59m"),
            (3_600_000, "1h"),
            (86_399_999, "23h"),
            (86_400_000, "1d"),
        ] {
            assert_eq!(relative_age(NOW_MS - elapsed_ms, NOW_MS), expected);
        }
        assert_eq!(relative_age(NOW_MS + 1, NOW_MS), "0s");
        assert_eq!(relative_age(0, NOW_MS), "--");
    }

    #[test]
    fn session_picker_row_bounds_first_question_and_keeps_legacy_fallback() {
        let session = SessionSummary {
            session_id: "session_full_raw_identifier".into(),
            run_id: "run_full_raw_identifier".into(),
            status: RunStateName::Finished,
            latest_question: "approved, go ahead".into(),
            first_question:
                "This first question is deliberately much longer than the picker row can display"
                    .into(),
            updated_at_ms: 99_000,
            ledger_path: "/tmp/agent.db".into(),
        };

        let row = session_picker_row(
            &TuiState::disconnected("w".into(), "s".into(), "e".into()),
            &session,
            true,
            100_000,
            48,
        );
        let rendered = row.to_string();

        assert!(row.width() <= 48);
        assert!(rendered.ends_with("..."));
        assert!(!rendered.contains("session_full_raw_identifier"));
        assert!(!rendered.contains("approved, go ahead"));

        let legacy = SessionSummary {
            first_question: String::new(),
            latest_question: "legacy latest question".into(),
            updated_at_ms: 0,
            ..session
        };
        let rendered = session_picker_row(
            &TuiState::disconnected("w".into(), "s".into(), "e".into()),
            &legacy,
            false,
            100_000,
            80,
        )
        .to_string();
        assert!(rendered.contains("-- legacy latest question"));

        let empty = SessionSummary {
            latest_question: String::new(),
            ..legacy
        };
        let rendered = session_picker_row(
            &TuiState::disconnected("w".into(), "s".into(), "e".into()),
            &empty,
            false,
            100_000,
            80,
        )
        .to_string();
        assert!(rendered.contains("-- (no question)"));
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
        assert!(output.contains("g grant"));
        assert!(output.contains("d deny"));
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

        let output = render_to_text(&state);

        assert!(output.contains("diff preview"));
        assert!(output.contains("--- a/scratch.txt"));
        assert!(output.contains("-old"));
        assert!(output.contains("+new"));
        assert!(!output.contains("input preview:"));
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

        let output = render_to_text(&state);

        assert!(output.contains("g grant"));
        assert!(output.contains("d deny"));
        assert!(output.contains("diff preview"));
        assert!(output.contains("--- a/scratch.txt"));
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
            },
            Vec::new(),
            TranscriptState::None,
        );
        state.approval = Some(ApprovalModalView {
            run_id: "run_1".into(),
            tool_call_id: "call_1".into(),
            tool_name: "shell.exec".into(),
            effect: "ExternalSideEffect".into(),
            reason: "shell.exec requires approval".into(),
            input_preview: r#"{"command":"cargo test"}"#.into(),
            approval_preview: Some("command: cargo test\ncwd: /tmp/work".into()),
            diff_preview: None,
        });

        let output = render_to_text(&state);

        assert!(output.contains("approval preview"));
        assert!(output.contains("command: cargo test"));
        assert!(output.contains("cwd: /tmp/work"));
        assert!(!output.contains("input preview:"));
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
                "approval_denied_count": 1
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
}
