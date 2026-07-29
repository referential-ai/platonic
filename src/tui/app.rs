use crate::{
    AppError, AppResult,
    daemon::client::{DaemonClient, DaemonConnectionConfig},
    daemon::protocol::{
        CommandAcceptedResult, ERROR_LAGGED, ERROR_OVERLOAD, ERROR_UNSUPPORTED_VERSION,
        ERROR_WORKSPACE_MISMATCH, EventsStreamResult, IssuePrepResult, IssuePrepStartResult,
        RunStartResult, RunStateName, StreamEvent,
    },
    tui::{ActiveRunView, TranscriptState, TranscriptView, TuiState, render, render_snapshot},
};
use crossterm::{
    event::{
        self, DisableBracketedPaste, EnableBracketedPaste, Event, KeyCode, KeyEvent, KeyEventKind,
        KeyModifiers,
    },
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{Terminal, backend::CrosstermBackend};
use std::{
    collections::HashMap,
    io::{self, Stdout},
    path::PathBuf,
    sync::mpsc::{self, Receiver, Sender},
    thread,
    time::{Duration, Instant},
};

use super::{
    commands::{
        SlashCommandAction, find_slash_command, has_slash_command_prefix, matching_slash_commands,
    },
    state::{SessionPickerView, SlashPopupView},
};

const ACTIVE_POLL_INTERVAL: Duration = Duration::from_millis(200);
const DAEMON_CLIENT_TIMEOUT: Duration = Duration::from_secs(3);
const EVENT_LIMIT: usize = 128;
const SCROLL_PAGE_LINES: usize = 10;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TuiOptions {
    pub workspace: PathBuf,
    pub socket: Option<PathBuf>,
    pub run: Option<String>,
    pub config: Option<PathBuf>,
    pub snapshot: bool,
}

impl TuiOptions {
    pub fn new(workspace: PathBuf) -> Self {
        Self {
            workspace,
            socket: None,
            run: None,
            config: None,
            snapshot: false,
        }
    }
}

pub fn run_tui(options: TuiOptions) -> AppResult<()> {
    let config = DaemonConnectionConfig::resolve(&options.workspace, options.socket)?;
    let mut state = load_state(&config, options.run.as_deref());
    if options.snapshot {
        print!("{}", render_snapshot(&state, 100, 24)?);
        return Ok(());
    }
    let config_path = options
        .config
        .as_ref()
        .map(|path| path.to_string_lossy().into_owned());
    let (commands, events) = spawn_client_worker(config.clone());
    let mut runtime = UiRuntime::from_state(&state, config_path.clone());
    let mut terminal = TerminalSession::enter()?;

    loop {
        drain_client_events(&mut state, &mut runtime, &events, &commands);
        maybe_poll_events(&mut runtime, &commands);
        update_elapsed(&mut state, &runtime);
        terminal.draw(&state)?;
        if event::poll(Duration::from_millis(50))? {
            match event::read()? {
                Event::Key(key)
                    if matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) =>
                {
                    if !handle_key_press(
                        key,
                        &mut state,
                        &runtime,
                        &commands,
                        options.run.clone(),
                        config_path.clone(),
                    ) {
                        break;
                    }
                }
                Event::Paste(text) => handle_paste_text(&mut state, &text),
                _ => {}
            }
        }
    }
    Ok(())
}

fn handle_key_press(
    key: KeyEvent,
    state: &mut TuiState,
    runtime: &UiRuntime,
    commands: &Sender<ClientCommand>,
    initial_run_id: Option<String>,
    config_path: Option<String>,
) -> bool {
    if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
        if state.issue_prep_started_at.is_some() {
            state.status_message = Some("issue prep is still running".into());
            return true;
        }
        return request_cancel(commands, state);
    }

    if state.help_visible {
        match key.code {
            KeyCode::Char('?') | KeyCode::Char('q') | KeyCode::Esc => {
                state.help_visible = false;
            }
            _ => {}
        }
        return true;
    }

    if state.approval.is_some() {
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => return false,
            KeyCode::Char('g') => decide_approval(commands, state, ApprovalAction::Grant),
            KeyCode::Char('d') => decide_approval(commands, state, ApprovalAction::Deny),
            _ => {}
        }
        return true;
    }

    if state.session_picker.is_some() {
        return handle_session_picker_key(key, state, commands);
    }

    if state.slash_popup.is_some()
        && let Some(keep_running) = handle_slash_popup_key(
            key,
            state,
            commands,
            initial_run_id.clone(),
            runtime,
            config_path.clone(),
        )
    {
        return keep_running;
    }

    if is_newline_key(key) {
        insert_composer_text(state, "\n");
        return true;
    }

    if key.modifiers.contains(KeyModifiers::CONTROL) {
        match key.code {
            KeyCode::Char('a') => {
                move_composer_line_start(state);
                return true;
            }
            KeyCode::Char('b') => {
                move_composer_left(state);
                return true;
            }
            KeyCode::Char('e') => {
                move_composer_line_end(state);
                return true;
            }
            KeyCode::Char('f') => {
                move_composer_right(state);
                return true;
            }
            KeyCode::Char('k') => {
                delete_composer_to_line_end(state);
                return true;
            }
            KeyCode::Char('u') => {
                kill_composer_to_start(state);
                return true;
            }
            KeyCode::Char('w') => {
                delete_previous_word(state);
                return true;
            }
            KeyCode::Char('y') => {
                yank_composer_kill_buffer(state);
                return true;
            }
            KeyCode::Char('p') => {
                recall_history_previous(state);
                return true;
            }
            KeyCode::Char('n') => {
                recall_history_next(state);
                return true;
            }
            _ => {}
        }
    }

    match key.code {
        KeyCode::Esc => handle_exit_request(state),
        KeyCode::Char('?') if state.composer.is_empty() => {
            state.help_visible = true;
            true
        }
        KeyCode::Char('q') if state.composer.is_empty() => handle_exit_request(state),
        KeyCode::Char('r') if is_disconnected(state) => {
            reconnect(commands, state, initial_run_id);
            true
        }
        KeyCode::Enter => {
            if !consume_line_continuation(state) {
                return submit_composer(commands, state, runtime, initial_run_id, config_path);
            }
            true
        }
        KeyCode::Tab => submit_composer(commands, state, runtime, initial_run_id, config_path),
        KeyCode::Char('b') if key.modifiers == KeyModifiers::ALT => {
            move_composer_word_left(state);
            true
        }
        KeyCode::Char('f') if key.modifiers == KeyModifiers::ALT => {
            move_composer_word_right(state);
            true
        }
        KeyCode::Backspace => {
            delete_composer_before_cursor(state);
            true
        }
        KeyCode::Delete => {
            delete_composer_after_cursor(state);
            true
        }
        KeyCode::Left => {
            if key.modifiers.contains(KeyModifiers::ALT) {
                move_composer_word_left(state);
            } else {
                move_composer_left(state);
            }
            true
        }
        KeyCode::Right => {
            if key.modifiers.contains(KeyModifiers::ALT) {
                move_composer_word_right(state);
            } else {
                move_composer_right(state);
            }
            true
        }
        KeyCode::Home => {
            move_composer_line_start(state);
            true
        }
        KeyCode::End => {
            move_composer_line_end(state);
            true
        }
        KeyCode::Up => {
            if !move_composer_up(state) {
                recall_history_previous(state);
            }
            true
        }
        KeyCode::Down => {
            if !move_composer_down(state) {
                recall_history_next(state);
            }
            true
        }
        KeyCode::PageUp => {
            scroll_history_up(state);
            true
        }
        KeyCode::PageDown => {
            scroll_history_down(state);
            true
        }
        KeyCode::Char(ch)
            if !key.modifiers.contains(KeyModifiers::CONTROL)
                && !key.modifiers.contains(KeyModifiers::ALT) =>
        {
            insert_composer_char(state, ch);
            true
        }
        _ => true,
    }
}

fn reconnect(commands: &Sender<ClientCommand>, state: &mut TuiState, run_id: Option<String>) {
    state.status_message = Some("reconnecting".into());
    send_command(commands, ClientCommand::Load { run_id }, state);
}

fn is_disconnected(state: &TuiState) -> bool {
    matches!(
        state.connection,
        crate::tui::ConnectionState::Disconnected { .. }
    )
}

fn is_newline_key(key: KeyEvent) -> bool {
    matches!(key.code, KeyCode::Enter)
        && key
            .modifiers
            .intersects(KeyModifiers::SHIFT | KeyModifiers::ALT | KeyModifiers::CONTROL)
        || matches!(key.code, KeyCode::Char('j' | 'm')) && key.modifiers == KeyModifiers::CONTROL
}

fn handle_slash_popup_key(
    key: KeyEvent,
    state: &mut TuiState,
    commands: &Sender<ClientCommand>,
    initial_run_id: Option<String>,
    runtime: &UiRuntime,
    config_path: Option<String>,
) -> Option<bool> {
    match key {
        KeyEvent {
            code: KeyCode::Up, ..
        }
        | KeyEvent {
            code: KeyCode::Char('p'),
            modifiers: KeyModifiers::CONTROL,
            ..
        } => {
            move_slash_popup_selection(state, -1);
            Some(true)
        }
        KeyEvent {
            code: KeyCode::Down,
            ..
        }
        | KeyEvent {
            code: KeyCode::Char('n'),
            modifiers: KeyModifiers::CONTROL,
            ..
        } => {
            move_slash_popup_selection(state, 1);
            Some(true)
        }
        KeyEvent {
            code: KeyCode::Esc, ..
        } => {
            state.slash_popup = None;
            Some(true)
        }
        KeyEvent {
            code: KeyCode::Tab, ..
        } => {
            complete_selected_slash_command(state);
            Some(true)
        }
        KeyEvent {
            code: KeyCode::Enter,
            modifiers: KeyModifiers::NONE,
            ..
        } => Some(dispatch_selected_slash_command(
            commands,
            state,
            initial_run_id,
            runtime,
            config_path,
        )),
        _ => None,
    }
}

fn handle_session_picker_key(
    key: KeyEvent,
    state: &mut TuiState,
    commands: &Sender<ClientCommand>,
) -> bool {
    match key {
        KeyEvent {
            code: KeyCode::Up, ..
        }
        | KeyEvent {
            code: KeyCode::Char('p'),
            modifiers: KeyModifiers::CONTROL,
            ..
        } => {
            move_session_picker_selection(state, -1);
            true
        }
        KeyEvent {
            code: KeyCode::Down,
            ..
        }
        | KeyEvent {
            code: KeyCode::Char('n'),
            modifiers: KeyModifiers::CONTROL,
            ..
        } => {
            move_session_picker_selection(state, 1);
            true
        }
        KeyEvent {
            code: KeyCode::Enter,
            modifiers: KeyModifiers::NONE,
            ..
        } => {
            select_picker_session(commands, state);
            true
        }
        KeyEvent {
            code: KeyCode::Esc, ..
        }
        | KeyEvent {
            code: KeyCode::Char('q'),
            ..
        } => {
            state.session_picker = None;
            true
        }
        _ => true,
    }
}

fn open_session_picker(state: &mut TuiState) {
    let selected = state
        .selected_session_id
        .as_deref()
        .and_then(|session_id| {
            state
                .sessions
                .iter()
                .position(|session| session.session_id == session_id)
        })
        .unwrap_or(0);
    state.session_picker = Some(SessionPickerView {
        selected: selected.min(state.sessions.len().saturating_sub(1)),
    });
    state.status_message = Some("session picker opened".into());
}

fn move_session_picker_selection(state: &mut TuiState, delta: isize) {
    let Some(picker) = state.session_picker.as_mut() else {
        return;
    };
    let count = state.sessions.len();
    picker.selected = wrapped_selection(picker.selected, count, delta);
}

fn select_picker_session(commands: &Sender<ClientCommand>, state: &mut TuiState) {
    let Some(session) = state
        .session_picker
        .as_ref()
        .and_then(|picker| state.sessions.get(picker.selected))
        .cloned()
    else {
        state.session_picker = None;
        state.status_message = Some("no sessions".into());
        return;
    };
    state.session_picker = None;
    state.selected_session_id = Some(session.session_id.clone());
    state.status_message = Some(format!("loading session {}", session.session_id));
    send_command(
        commands,
        ClientCommand::LoadSession {
            session_id: session.session_id,
        },
        state,
    );
}

fn move_slash_popup_selection(state: &mut TuiState, delta: isize) {
    let Some(popup) = state.slash_popup.as_mut() else {
        return;
    };
    let count = matching_slash_commands(&popup.filter).len().min(5);
    popup.selected = wrapped_selection(popup.selected, count, delta);
}

fn wrapped_selection(selected: usize, count: usize, delta: isize) -> usize {
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

fn selected_slash_command(state: &TuiState) -> Option<&'static super::commands::SlashCommandSpec> {
    let popup = state.slash_popup.as_ref()?;
    matching_slash_commands(&popup.filter)
        .into_iter()
        .take(5)
        .nth(popup.selected)
}

fn complete_selected_slash_command(state: &mut TuiState) {
    let Some(command) = selected_slash_command(state) else {
        return;
    };
    state.composer = format!("/{} ", command.name);
    state.composer_cursor = state.composer.len();
    state.history_index = None;
    state.slash_popup = None;
}

fn dispatch_selected_slash_command(
    commands: &Sender<ClientCommand>,
    state: &mut TuiState,
    initial_run_id: Option<String>,
    runtime: &UiRuntime,
    config_path: Option<String>,
) -> bool {
    let Some(command) = selected_slash_command(state) else {
        return submit_composer(commands, state, runtime, initial_run_id, config_path);
    };
    let message = format!("/{}", command.name);
    record_input_history(state, &message);
    clear_composer(state);
    dispatch_slash_command(
        commands,
        state,
        command.action,
        &message,
        initial_run_id,
        runtime,
        config_path,
    )
}

fn sync_slash_popup(state: &mut TuiState) {
    let Some(filter) = slash_filter_at_cursor(&state.composer, state.composer_cursor) else {
        state.slash_popup = None;
        return;
    };
    let selected = state.slash_popup.as_ref().map_or(0, |popup| popup.selected);
    let count = matching_slash_commands(&filter).len().min(5);
    state.slash_popup = Some(SlashPopupView {
        filter,
        selected: selected.min(count.saturating_sub(1)),
    });
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

fn handle_paste_text(state: &mut TuiState, text: &str) {
    if state.help_visible || state.approval.is_some() {
        return;
    }
    insert_composer_text(state, &text.replace('\r', "\n"));
}

fn insert_composer_char(state: &mut TuiState, ch: char) {
    let mut buffer = [0; 4];
    insert_composer_text(state, ch.encode_utf8(&mut buffer));
}

fn insert_composer_text(state: &mut TuiState, text: &str) {
    clamp_composer_cursor(state);
    state.composer.insert_str(state.composer_cursor, text);
    state.composer_cursor += text.len();
    state.history_index = None;
    sync_slash_popup(state);
}

fn delete_composer_before_cursor(state: &mut TuiState) {
    clamp_composer_cursor(state);
    if state.composer_cursor == 0 {
        return;
    }
    let start = previous_boundary(&state.composer, state.composer_cursor);
    state
        .composer
        .replace_range(start..state.composer_cursor, "");
    state.composer_cursor = start;
    state.history_index = None;
    sync_slash_popup(state);
}

fn delete_composer_after_cursor(state: &mut TuiState) {
    clamp_composer_cursor(state);
    if state.composer_cursor >= state.composer.len() {
        return;
    }
    let end = next_boundary(&state.composer, state.composer_cursor);
    state.composer.replace_range(state.composer_cursor..end, "");
    state.history_index = None;
    sync_slash_popup(state);
}

fn delete_composer_to_line_end(state: &mut TuiState) {
    clamp_composer_cursor(state);
    let end = line_end_at(&state.composer, state.composer_cursor);
    state.composer_kill_buffer = state.composer[state.composer_cursor..end].to_owned();
    state.composer.replace_range(state.composer_cursor..end, "");
    state.history_index = None;
    sync_slash_popup(state);
}

fn delete_previous_word(state: &mut TuiState) {
    clamp_composer_cursor(state);
    let mut start = state.composer_cursor;
    while start > 0 && char_before(&state.composer, start).is_some_and(char::is_whitespace) {
        start = previous_boundary(&state.composer, start);
    }
    while start > 0 && char_before(&state.composer, start).is_some_and(|ch| !ch.is_whitespace()) {
        start = previous_boundary(&state.composer, start);
    }
    state.composer_kill_buffer = state.composer[start..state.composer_cursor].to_owned();
    state
        .composer
        .replace_range(start..state.composer_cursor, "");
    state.composer_cursor = start;
    state.history_index = None;
    sync_slash_popup(state);
}

fn kill_composer_to_start(state: &mut TuiState) {
    clamp_composer_cursor(state);
    state.composer_kill_buffer = state.composer[..state.composer_cursor].to_owned();
    state.composer.replace_range(..state.composer_cursor, "");
    state.composer_cursor = 0;
    state.history_index = None;
    sync_slash_popup(state);
}

fn yank_composer_kill_buffer(state: &mut TuiState) {
    if state.composer_kill_buffer.is_empty() {
        return;
    }
    let text = state.composer_kill_buffer.clone();
    insert_composer_text(state, &text);
}

fn clear_composer(state: &mut TuiState) {
    state.composer.clear();
    state.composer_cursor = 0;
    state.history_index = None;
    state.slash_popup = None;
}

fn scroll_history_up(state: &mut TuiState) {
    state.scroll_offset = state.scroll_offset.saturating_add(SCROLL_PAGE_LINES);
}

fn scroll_history_down(state: &mut TuiState) {
    state.scroll_offset = state.scroll_offset.saturating_sub(SCROLL_PAGE_LINES);
}

fn move_composer_left(state: &mut TuiState) {
    clamp_composer_cursor(state);
    state.composer_cursor = previous_boundary(&state.composer, state.composer_cursor);
    sync_slash_popup(state);
}

fn move_composer_right(state: &mut TuiState) {
    clamp_composer_cursor(state);
    state.composer_cursor = next_boundary(&state.composer, state.composer_cursor);
    sync_slash_popup(state);
}

fn move_composer_line_start(state: &mut TuiState) {
    clamp_composer_cursor(state);
    state.composer_cursor = line_start_at(&state.composer, state.composer_cursor);
    sync_slash_popup(state);
}

fn move_composer_line_end(state: &mut TuiState) {
    clamp_composer_cursor(state);
    state.composer_cursor = line_end_at(&state.composer, state.composer_cursor);
    sync_slash_popup(state);
}

fn move_composer_word_left(state: &mut TuiState) {
    clamp_composer_cursor(state);
    let mut start = state.composer_cursor;
    while start > 0 && char_before(&state.composer, start).is_some_and(char::is_whitespace) {
        start = previous_boundary(&state.composer, start);
    }
    while start > 0 && char_before(&state.composer, start).is_some_and(|ch| !ch.is_whitespace()) {
        start = previous_boundary(&state.composer, start);
    }
    state.composer_cursor = start;
    sync_slash_popup(state);
}

fn move_composer_word_right(state: &mut TuiState) {
    clamp_composer_cursor(state);
    let mut end = state.composer_cursor;
    while end < state.composer.len()
        && char_at(&state.composer, end).is_some_and(|ch| !ch.is_whitespace())
    {
        end = next_boundary(&state.composer, end);
    }
    while end < state.composer.len()
        && char_at(&state.composer, end).is_some_and(char::is_whitespace)
    {
        end = next_boundary(&state.composer, end);
    }
    state.composer_cursor = end;
    sync_slash_popup(state);
}

fn move_composer_up(state: &mut TuiState) -> bool {
    clamp_composer_cursor(state);
    let start = line_start_at(&state.composer, state.composer_cursor);
    if start == 0 {
        return false;
    }
    let column = state.composer[start..state.composer_cursor].chars().count();
    let previous_end = previous_boundary(&state.composer, start);
    let previous_start = line_start_at(&state.composer, previous_end);
    state.composer_cursor =
        nth_char_boundary(&state.composer, previous_start, previous_end, column);
    sync_slash_popup(state);
    true
}

fn move_composer_down(state: &mut TuiState) -> bool {
    clamp_composer_cursor(state);
    let start = line_start_at(&state.composer, state.composer_cursor);
    let end = line_end_at(&state.composer, state.composer_cursor);
    if end >= state.composer.len() {
        return false;
    }
    let column = state.composer[start..state.composer_cursor].chars().count();
    let next_start = next_boundary(&state.composer, end);
    let next_end = line_end_at(&state.composer, next_start);
    state.composer_cursor = nth_char_boundary(&state.composer, next_start, next_end, column);
    sync_slash_popup(state);
    true
}

fn consume_line_continuation(state: &mut TuiState) -> bool {
    clamp_composer_cursor(state);
    if state.composer_cursor == 0 {
        return false;
    }
    let start = previous_boundary(&state.composer, state.composer_cursor);
    if &state.composer[start..state.composer_cursor] != "\\" {
        return false;
    }
    state
        .composer
        .replace_range(start..state.composer_cursor, "\n");
    state.composer_cursor = start + 1;
    state.history_index = None;
    sync_slash_popup(state);
    true
}

fn recall_history_previous(state: &mut TuiState) {
    if state.input_history.is_empty() {
        return;
    }
    let index = state
        .history_index
        .map(|index| index.saturating_sub(1))
        .unwrap_or_else(|| state.input_history.len() - 1);
    state.history_index = Some(index);
    state.composer = state.input_history[index].clone();
    state.composer_cursor = state.composer.len();
    sync_slash_popup(state);
}

fn recall_history_next(state: &mut TuiState) {
    let Some(index) = state.history_index else {
        return;
    };
    if index + 1 >= state.input_history.len() {
        clear_composer(state);
    } else {
        let next = index + 1;
        state.history_index = Some(next);
        state.composer = state.input_history[next].clone();
        state.composer_cursor = state.composer.len();
        sync_slash_popup(state);
    }
}

fn record_input_history(state: &mut TuiState, message: &str) {
    if state
        .input_history
        .last()
        .is_none_or(|last| last != message)
    {
        state.input_history.push(message.to_owned());
    }
    state.history_index = None;
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

fn clamp_composer_cursor(state: &mut TuiState) {
    state.composer_cursor = state.composer_cursor.min(state.composer.len());
    while !state.composer.is_char_boundary(state.composer_cursor) {
        state.composer_cursor -= 1;
    }
}

fn load_state(config: &DaemonConnectionConfig, run_id: Option<&str>) -> TuiState {
    match load_connected_state(config, run_id, None) {
        Ok(state) => state,
        Err(error) => TuiState::disconnected(
            config.workspace_root.to_string_lossy().into_owned(),
            config.socket_path.to_string_lossy().into_owned(),
            error.to_string(),
        ),
    }
}

fn load_connected_state(
    config: &DaemonConnectionConfig,
    run_id: Option<&str>,
    session_id: Option<&str>,
) -> AppResult<TuiState> {
    let mut client = connect_daemon(config, DAEMON_CLIENT_TIMEOUT)?;
    let hello = client.hello(&config.workspace_root)?;
    let sessions = client.sessions_list()?;
    let selected_session_id = session_id
        .map(str::to_owned)
        .or_else(|| {
            run_id.and_then(|run_id| {
                sessions
                    .iter()
                    .find(|session| session.run_id == run_id)
                    .map(|session| session.session_id.clone())
            })
        })
        .or_else(|| sessions.first().map(|session| session.session_id.clone()));
    let transcript = if let Some(session_id) = session_id.or(selected_session_id.as_deref()) {
        match client.transcript_read_session(session_id) {
            Ok(transcript) => TranscriptState::Loaded(TranscriptView::from(transcript)),
            Err(error) => TranscriptState::Unavailable {
                run_id: session_id.to_owned(),
                error: error.to_string(),
            },
        }
    } else {
        match run_id {
            Some(run_id) => match client.transcript_read(run_id) {
                Ok(transcript) => TranscriptState::Loaded(TranscriptView::from(transcript)),
                Err(error) => TranscriptState::Unavailable {
                    run_id: run_id.to_owned(),
                    error: error.to_string(),
                },
            },
            None => TranscriptState::None,
        }
    };
    let mut state = TuiState::connected(
        config.workspace_root.to_string_lossy().into_owned(),
        config.socket_path.to_string_lossy().into_owned(),
        hello,
        sessions,
        transcript,
    );
    state.selected_session_id = selected_session_id;
    let active_session = state
        .selected_session_id
        .as_deref()
        .and_then(|session_id| {
            state.sessions.iter().find(|session| {
                session.session_id == session_id && session.status == RunStateName::Running
            })
        })
        .or_else(|| {
            state
                .sessions
                .iter()
                .find(|session| session.status == RunStateName::Running)
        });
    if let Some(session) = active_session {
        state.active_run = Some(ActiveRunView::new(session.run_id.clone(), session.status));
    }
    Ok(state)
}

fn load_selected_session_state(config: &DaemonConnectionConfig, session_id: &str) -> TuiState {
    match load_connected_state(config, None, Some(session_id)) {
        Ok(state) => state,
        Err(error) => TuiState::disconnected(
            config.workspace_root.to_string_lossy().into_owned(),
            config.socket_path.to_string_lossy().into_owned(),
            error.to_string(),
        ),
    }
}

#[derive(Debug)]
struct UiRuntime {
    active_run_id: Option<String>,
    config_path: Option<String>,
    next_offset: u64,
    poll_in_flight: bool,
    polling: bool,
    last_poll: Instant,
    tool_inputs: HashMap<String, String>,
    active_since: Option<Instant>,
}

impl UiRuntime {
    fn from_state(state: &TuiState, config_path: Option<String>) -> Self {
        Self {
            active_run_id: state.active_run.as_ref().map(|run| run.run_id.clone()),
            config_path,
            next_offset: 0,
            poll_in_flight: false,
            polling: state
                .active_run
                .as_ref()
                .is_some_and(|run| run.status == RunStateName::Running),
            last_poll: Instant::now(),
            tool_inputs: HashMap::new(),
            active_since: state.active_run.as_ref().map(|_| Instant::now()),
        }
    }

    fn sync_from_state(&mut self, state: &TuiState) {
        self.active_run_id = state.active_run.as_ref().map(|run| run.run_id.clone());
        self.polling = state
            .active_run
            .as_ref()
            .is_some_and(|run| run.status == RunStateName::Running);
        self.next_offset = 0;
        self.poll_in_flight = false;
        self.last_poll = Instant::now();
        self.tool_inputs.clear();
        self.active_since = state.active_run.as_ref().map(|_| Instant::now());
    }
}

#[derive(Debug)]
enum ClientCommand {
    Load {
        run_id: Option<String>,
    },
    LoadSession {
        session_id: String,
    },
    RunStart {
        question: String,
        config_path: Option<String>,
    },
    MessageAppend {
        message: String,
        session_id: String,
        config_path: Option<String>,
    },
    IssuePrepStart {
        input: String,
        config_path: Option<String>,
    },
    PollEvents {
        run_id: String,
        from_offset: Option<u64>,
    },
    ApprovalGrant {
        run_id: String,
        tool_call_id: String,
    },
    ApprovalDeny {
        run_id: String,
        tool_call_id: String,
        reason: String,
    },
    RunCancel {
        run_id: String,
    },
}

#[derive(Debug)]
enum ClientEvent {
    Loaded(Box<TuiState>),
    RunStarted(RunStartResult),
    IssuePrepFinished(IssuePrepStartResult),
    EventsPolled(EventsStreamResult),
    ApprovalDecided(CommandAcceptedResult),
    RunCanceled(CommandAcceptedResult),
    Failed {
        context: &'static str,
        error: crate::AppError,
    },
}

fn spawn_client_worker(
    config: DaemonConnectionConfig,
) -> (Sender<ClientCommand>, Receiver<ClientEvent>) {
    let (command_sender, command_receiver) = mpsc::channel();
    let (event_sender, event_receiver) = mpsc::channel();
    thread::spawn(move || {
        for command in command_receiver {
            let event = handle_client_command(&config, command);
            if event_sender.send(event).is_err() {
                break;
            }
        }
    });
    (command_sender, event_receiver)
}

fn handle_client_command(config: &DaemonConnectionConfig, command: ClientCommand) -> ClientEvent {
    match command {
        ClientCommand::Load { run_id } => {
            ClientEvent::Loaded(Box::new(load_state(config, run_id.as_deref())))
        }
        ClientCommand::LoadSession { session_id } => {
            ClientEvent::Loaded(Box::new(load_selected_session_state(config, &session_id)))
        }
        ClientCommand::RunStart {
            question,
            config_path,
        } => with_client(config, |client| {
            client.run_start(question, config_path, false)
        })
        .map_or_else(failed_event("run.start"), ClientEvent::RunStarted),
        ClientCommand::MessageAppend {
            message,
            session_id,
            config_path,
        } => with_client(config, |client| {
            client.message_append_to_session(message, Some(session_id), config_path, false)
        })
        .map_or_else(failed_event("message.append"), ClientEvent::RunStarted),
        ClientCommand::IssuePrepStart { input, config_path } => {
            with_client(config, |client| client.issue_prep_start(input, config_path)).map_or_else(
                failed_event("issue-prep.start"),
                ClientEvent::IssuePrepFinished,
            )
        }
        ClientCommand::PollEvents {
            run_id,
            from_offset,
        } => with_client(config, |client| {
            client.events_stream(&run_id, from_offset, EVENT_LIMIT)
        })
        .map_or_else(failed_event("events.stream"), ClientEvent::EventsPolled),
        ClientCommand::ApprovalGrant {
            run_id,
            tool_call_id,
        } => with_client(config, |client| {
            client.approval_grant(&run_id, &tool_call_id)
        })
        .map_or_else(
            failed_event("approval.decide"),
            ClientEvent::ApprovalDecided,
        ),
        ClientCommand::ApprovalDeny {
            run_id,
            tool_call_id,
            reason,
        } => with_client(config, |client| {
            client.approval_deny(&run_id, &tool_call_id, reason)
        })
        .map_or_else(
            failed_event("approval.decide"),
            ClientEvent::ApprovalDecided,
        ),
        ClientCommand::RunCancel { run_id } => {
            with_client(config, |client| client.run_cancel(&run_id))
                .map_or_else(failed_event("run.cancel"), ClientEvent::RunCanceled)
        }
    }
}

fn with_client<T>(
    config: &DaemonConnectionConfig,
    run: impl FnOnce(&mut DaemonClient) -> AppResult<T>,
) -> AppResult<T> {
    let mut client = connect_daemon(config, DAEMON_CLIENT_TIMEOUT)?;
    client.hello(&config.workspace_root)?;
    run(&mut client)
}

fn connect_daemon(config: &DaemonConnectionConfig, timeout: Duration) -> AppResult<DaemonClient> {
    DaemonClient::connect_with_timeout(&config.socket_path, timeout)
}

fn failed_event(context: &'static str) -> impl FnOnce(crate::AppError) -> ClientEvent {
    move |error| ClientEvent::Failed { context, error }
}

fn drain_client_events(
    state: &mut TuiState,
    runtime: &mut UiRuntime,
    events: &Receiver<ClientEvent>,
    commands: &Sender<ClientCommand>,
) {
    while let Ok(event) = events.try_recv() {
        match event {
            ClientEvent::Loaded(loaded) => {
                apply_loaded_state(state, *loaded);
                runtime.sync_from_state(state);
            }
            ClientEvent::RunStarted(result) => {
                apply_run_response(state, runtime, result, "run started")
            }
            ClientEvent::IssuePrepFinished(result) => {
                state.issue_prep_started_at = None;
                match result.outcome {
                    IssuePrepResult::Candidate { markdown } => {
                        push_live_event(
                            state,
                            crate::tui::LiveEventLine::assistant(None, markdown),
                        );
                        push_live_event(
                            state,
                            crate::tui::LiveEventLine::status(
                                None,
                                format!("issue-prep artifacts: {}", result.run_dir),
                            ),
                        );
                        state.status_message =
                            Some(format!("issue ready; artifacts: {}", result.run_dir));
                    }
                    IssuePrepResult::Blocked { stage, reasons } => {
                        let reason_text = if reasons.is_empty() {
                            String::new()
                        } else {
                            format!(":\n- {}", reasons.join("\n- "))
                        };
                        push_live_event(
                            state,
                            crate::tui::LiveEventLine::warning(
                                None,
                                format!(
                                    "issue prep blocked at {stage}{reason_text}\nartifacts: {}",
                                    result.run_dir
                                ),
                            ),
                        );
                        state.status_message = Some(format!(
                            "issue prep blocked at {stage}; artifacts: {}",
                            result.run_dir
                        ));
                    }
                }
                state.scroll_offset = 0;
                start_next_queued(commands, state, runtime);
            }
            ClientEvent::EventsPolled(result) => {
                apply_events_result(state, runtime, commands, result)
            }
            ClientEvent::ApprovalDecided(result) => {
                state.status_message =
                    Some(format!("approval decision sent for {}", result.run_id));
                state.approval = None;
                state.active_run = Some(ActiveRunView::new(result.run_id, result.status));
            }
            ClientEvent::RunCanceled(result) => {
                state.status_message = Some(format!("cancel requested for {}", result.run_id));
                state.cancel_requested = true;
                state.approval = None;
                state.active_run = Some(ActiveRunView::new(result.run_id.clone(), result.status));
                push_live_event(
                    state,
                    crate::tui::LiveEventLine::status(
                        None,
                        format!("cancel requested: {}", result.run_id),
                    ),
                );
            }
            ClientEvent::Failed { context, error } => {
                runtime.poll_in_flight = false;
                let connection_error = is_connection_error(&error);
                let lagged = matches!(
                    &error,
                    AppError::DaemonResponse(error) if error.code == ERROR_LAGGED
                );
                let overloaded = matches!(
                    &error,
                    AppError::DaemonResponse(error) if error.code == ERROR_OVERLOAD
                );
                let message = error.to_string();
                if context == "events.stream" && lagged {
                    state.stream_warning = Some(format!("{message}; resuming at current tip"));
                    if let Some(run_id) = runtime.active_run_id.clone() {
                        poll_events_from(runtime, commands, run_id, None);
                    }
                } else if context == "events.stream" && overloaded {
                    state.stream_warning = Some(message);
                } else {
                    if connection_error {
                        runtime.polling = false;
                        state.connection = crate::tui::ConnectionState::Disconnected {
                            error: message.clone(),
                        };
                    }
                    if context == "run.cancel" {
                        state.cancel_requested = false;
                    }
                    let failure = format!("{context} failed: {message}");
                    state.status_message = Some(failure.clone());
                    if context == "issue-prep.start" {
                        state.issue_prep_started_at = None;
                        push_live_event(state, crate::tui::LiveEventLine::warning(None, failure));
                        if !connection_error {
                            start_next_queued(commands, state, runtime);
                        }
                    }
                }
            }
        }
    }
}

fn apply_loaded_state(state: &mut TuiState, mut loaded: TuiState) {
    loaded.composer = std::mem::take(&mut state.composer);
    loaded.composer_cursor = state.composer_cursor;
    loaded.composer_kill_buffer = state.composer_kill_buffer.clone();
    loaded.slash_popup = state.slash_popup.clone();
    loaded.queued_messages = std::mem::take(&mut state.queued_messages);
    loaded.issue_prep_started_at = state.issue_prep_started_at;
    loaded.input_history = std::mem::take(&mut state.input_history);
    loaded.history_index = state.history_index;
    loaded.help_visible = state.help_visible;
    if loaded.status_message.is_none() {
        loaded.status_message = state.status_message.clone();
    }
    if loaded.stream_warning.is_none() {
        loaded.stream_warning = state.stream_warning.clone();
    }
    if loaded.live_events.is_empty() {
        loaded.live_events = state.live_events.clone();
    }
    loaded.scroll_offset = state.scroll_offset;
    if loaded.active_model.is_none() {
        loaded.active_model = state.active_model.clone();
    }
    if loaded.active_run_elapsed_secs.is_none() {
        loaded.active_run_elapsed_secs = state.active_run_elapsed_secs;
    }
    if loaded.active_run.as_ref().map(|run| &run.run_id)
        == state.active_run.as_ref().map(|run| &run.run_id)
    {
        loaded.cancel_requested = state.cancel_requested;
    }
    *state = loaded;
}

fn apply_run_response(
    state: &mut TuiState,
    runtime: &mut UiRuntime,
    result: RunStartResult,
    message: &'static str,
) {
    let run_id = result.run_id.clone();
    let status = result.status;
    state.selected_session_id = Some(result.session_id.clone());
    state.status_message = Some(format!("{message}: {run_id}"));
    state.stream_warning = None;
    state.cancel_requested = false;
    state.approval = None;
    state.active_run = Some(ActiveRunView::new(run_id.clone(), status));
    push_live_event(
        state,
        crate::tui::LiveEventLine::status(None, format!("{message}: {run_id}")),
    );
    state.scroll_offset = 0;
    runtime.active_run_id = Some(run_id);
    runtime.next_offset = 0;
    runtime.poll_in_flight = false;
    runtime.polling = status == RunStateName::Running;
    runtime.last_poll = Instant::now() - ACTIVE_POLL_INTERVAL;
    runtime.tool_inputs.clear();
    runtime.active_since = Some(Instant::now());
}

fn apply_events_result(
    state: &mut TuiState,
    runtime: &mut UiRuntime,
    commands: &Sender<ClientCommand>,
    result: EventsStreamResult,
) {
    runtime.poll_in_flight = false;
    runtime.next_offset = result.next_offset;
    let needs_catch_up =
        result.events.len() == EVENT_LIMIT && result.next_offset > result.from_offset;
    runtime.polling = result.status == RunStateName::Running || needs_catch_up;
    state.stream_warning = None;
    state.active_run = Some(ActiveRunView::new(result.run_id.clone(), result.status));
    for buffered in result.events {
        let event = &buffered.event;
        if let Some(model) = crate::tui::model_from_event(event) {
            state.active_model = Some(model);
        }
        if let Some((call_id, input_preview)) = crate::tui::tool_input_preview_from_event(event) {
            runtime
                .tool_inputs
                .insert(call_id.clone(), input_preview.clone());
            if let Some(approval) = state.approval.as_mut()
                && approval.tool_call_id == call_id
            {
                approval.input_preview = input_preview;
            }
        }
        if let Some(approval) = crate::tui::approval_from_event(
            event,
            match event {
                StreamEvent::ApprovalRequested { tool_call_id, .. } => {
                    runtime.tool_inputs.get(tool_call_id).cloned()
                }
                _ => None,
            },
        ) {
            state.approval = Some(approval);
        }
        let line = crate::tui::live_event_line(&buffered);
        push_live_event(state, line);
    }
    if needs_catch_up {
        maybe_poll_events_now(runtime, commands);
    } else if result.status != RunStateName::Running {
        runtime.active_since = None;
        send_command(
            commands,
            ClientCommand::Load {
                run_id: Some(result.run_id),
            },
            state,
        );
        start_next_queued(commands, state, runtime);
    }
}

fn maybe_poll_events(runtime: &mut UiRuntime, commands: &Sender<ClientCommand>) {
    if !runtime.polling || runtime.poll_in_flight {
        return;
    }
    if runtime.last_poll.elapsed() < ACTIVE_POLL_INTERVAL {
        return;
    }
    maybe_poll_events_now(runtime, commands);
}

fn maybe_poll_events_now(runtime: &mut UiRuntime, commands: &Sender<ClientCommand>) {
    let Some(run_id) = runtime.active_run_id.clone() else {
        return;
    };
    poll_events_from(runtime, commands, run_id, Some(runtime.next_offset));
}

fn poll_events_from(
    runtime: &mut UiRuntime,
    commands: &Sender<ClientCommand>,
    run_id: String,
    from_offset: Option<u64>,
) {
    if commands
        .send(ClientCommand::PollEvents {
            run_id,
            from_offset,
        })
        .is_ok()
    {
        runtime.poll_in_flight = true;
        runtime.last_poll = Instant::now();
    } else {
        runtime.polling = false;
    }
}

enum ApprovalAction {
    Grant,
    Deny,
}

fn decide_approval(commands: &Sender<ClientCommand>, state: &mut TuiState, action: ApprovalAction) {
    let Some(approval) = state.approval.clone() else {
        return;
    };
    let command = match action {
        ApprovalAction::Grant => ClientCommand::ApprovalGrant {
            run_id: approval.run_id.clone(),
            tool_call_id: approval.tool_call_id.clone(),
        },
        ApprovalAction::Deny => ClientCommand::ApprovalDeny {
            run_id: approval.run_id.clone(),
            tool_call_id: approval.tool_call_id.clone(),
            reason: "denied by plato-tui".into(),
        },
    };
    state.status_message = Some(match action {
        ApprovalAction::Grant => format!("grant sent for {}", approval.tool_call_id),
        ApprovalAction::Deny => format!("deny sent for {}", approval.tool_call_id),
    });
    state.approval = None;
    send_command(commands, command, state);
}

fn request_cancel(commands: &Sender<ClientCommand>, state: &mut TuiState) -> bool {
    let Some(active) = state.active_run.clone() else {
        return false;
    };
    if active.status != RunStateName::Running || state.cancel_requested {
        return false;
    }
    state.cancel_requested = true;
    state.status_message = Some(format!("cancel requested for {}", active.run_id));
    send_command(
        commands,
        ClientCommand::RunCancel {
            run_id: active.run_id,
        },
        state,
    );
    true
}

fn handle_exit_request(state: &mut TuiState) -> bool {
    if state.issue_prep_started_at.is_some() {
        state.status_message = Some("issue prep is still running".into());
        true
    } else {
        false
    }
}

fn is_connection_error(error: &AppError) -> bool {
    match error {
        AppError::Io(_) | AppError::DaemonLockHeld { .. } | AppError::DaemonProtocol(_) => true,
        AppError::DaemonResponse(error) => matches!(
            error.code.as_str(),
            ERROR_UNSUPPORTED_VERSION | ERROR_WORKSPACE_MISMATCH
        ),
        _ => false,
    }
}

fn submit_composer(
    commands: &Sender<ClientCommand>,
    state: &mut TuiState,
    runtime: &UiRuntime,
    initial_run_id: Option<String>,
    config_path: Option<String>,
) -> bool {
    let message = state.composer.trim().to_string();
    if message.is_empty() {
        return true;
    }
    record_input_history(state, &message);
    clear_composer(state);
    if let Some(keep_running) = handle_composer_command(
        commands,
        state,
        &message,
        initial_run_id,
        runtime,
        config_path.clone(),
    ) {
        return keep_running;
    }
    if runtime_is_busy(runtime, state) {
        state.queued_messages.push(message);
        state.status_message = Some("queued for next turn".into());
        return true;
    }
    push_live_event(state, crate::tui::LiveEventLine::user(message.clone()));
    let command = submit_message_command(message, state.selected_session_id.clone(), config_path);
    state.status_message = Some("submitted to daemon".into());
    send_command(commands, command, state);
    true
}

fn handle_composer_command(
    commands: &Sender<ClientCommand>,
    state: &mut TuiState,
    message: &str,
    initial_run_id: Option<String>,
    runtime: &UiRuntime,
    config_path: Option<String>,
) -> Option<bool> {
    if !message.starts_with('/') {
        return None;
    }
    let name = message
        .strip_prefix('/')
        .unwrap_or(message)
        .split_whitespace()
        .next()
        .unwrap_or_default();
    let Some(command) = find_slash_command(name) else {
        state.status_message = Some(format!("unknown command: {message}; try /help"));
        return Some(true);
    };
    Some(dispatch_slash_command(
        commands,
        state,
        command.action,
        message,
        initial_run_id,
        runtime,
        config_path,
    ))
}

fn dispatch_slash_command(
    commands: &Sender<ClientCommand>,
    state: &mut TuiState,
    action: SlashCommandAction,
    message: &str,
    initial_run_id: Option<String>,
    runtime: &UiRuntime,
    config_path: Option<String>,
) -> bool {
    match action {
        SlashCommandAction::Help => {
            state.help_visible = true;
            state.status_message = Some("help opened".into());
            true
        }
        SlashCommandAction::Clear => {
            clear_visible_transcript(state);
            state.status_message = Some("visible transcript cleared".into());
            true
        }
        SlashCommandAction::Sessions => {
            open_session_picker(state);
            true
        }
        SlashCommandAction::NewSession => {
            start_fresh_session(state);
            true
        }
        SlashCommandAction::IssuePrep => {
            start_issue_prep(commands, state, runtime, message, config_path);
            true
        }
        SlashCommandAction::Reconnect => {
            if is_disconnected(state) {
                reconnect(commands, state, initial_run_id);
            } else {
                state.status_message = Some("already connected".into());
            }
            true
        }
        SlashCommandAction::Quit => handle_exit_request(state),
    }
}

fn clear_visible_transcript(state: &mut TuiState) {
    state.transcript = TranscriptState::None;
    state.live_events.clear();
    state.stream_warning = None;
    state.scroll_offset = 0;
}

fn start_fresh_session(state: &mut TuiState) {
    state.selected_session_id = None;
    state.transcript = TranscriptState::None;
    state.live_events.clear();
    state.stream_warning = None;
    state.session_picker = None;
    state.scroll_offset = 0;
    state.status_message = Some("new session selected".into());
}

fn start_issue_prep(
    commands: &Sender<ClientCommand>,
    state: &mut TuiState,
    runtime: &UiRuntime,
    message: &str,
    config_path: Option<String>,
) {
    if state.issue_prep_started_at.is_some() {
        state.status_message = Some("issue prep already running".into());
        return;
    }
    if runtime.polling || runtime.poll_in_flight {
        state.status_message = Some("issue prep is unavailable while a run is active".into());
        return;
    }
    let input = message
        .char_indices()
        .find(|(_, character)| character.is_whitespace())
        .map_or("", |(index, _)| message[index..].trim());
    if input.is_empty() {
        state.status_message = Some("usage: /issue-prep <rough issue>".into());
        return;
    }

    state.issue_prep_started_at = Some(Instant::now());
    state.status_message = Some("issue prep running".into());
    push_live_event(
        state,
        crate::tui::LiveEventLine::user(format!("/issue-prep {input}")),
    );
    if commands
        .send(ClientCommand::IssuePrepStart {
            input: input.into(),
            config_path,
        })
        .is_err()
    {
        state.issue_prep_started_at = None;
        state.status_message = Some("daemon client worker stopped".into());
    }
}

fn runtime_is_busy(runtime: &UiRuntime, state: &TuiState) -> bool {
    state.issue_prep_started_at.is_some() || runtime.polling || runtime.poll_in_flight
}

fn start_next_queued(
    commands: &Sender<ClientCommand>,
    state: &mut TuiState,
    runtime: &mut UiRuntime,
) {
    if runtime_is_busy(runtime, state) || state.queued_messages.is_empty() {
        return;
    }
    let message = state.queued_messages.remove(0);
    push_live_event(state, crate::tui::LiveEventLine::user(message.clone()));
    let command = submit_message_command(
        message,
        state.selected_session_id.clone(),
        runtime.config_path.clone(),
    );
    runtime.polling = true;
    runtime.poll_in_flight = false;
    runtime.active_run_id = None;
    runtime.active_since = Some(Instant::now());
    state.status_message = Some("submitted queued message".into());
    send_command(commands, command, state);
}

fn submit_message_command(
    message: String,
    selected_session_id: Option<String>,
    config_path: Option<String>,
) -> ClientCommand {
    match selected_session_id {
        Some(session_id) => ClientCommand::MessageAppend {
            message,
            session_id,
            config_path,
        },
        None => ClientCommand::RunStart {
            question: message,
            config_path,
        },
    }
}

fn send_command(commands: &Sender<ClientCommand>, command: ClientCommand, state: &mut TuiState) {
    if commands.send(command).is_err() {
        state.status_message = Some("daemon client worker stopped".into());
    }
}

fn update_elapsed(state: &mut TuiState, runtime: &UiRuntime) {
    state.active_run_elapsed_secs = runtime
        .active_since
        .map(|started| started.elapsed().as_secs());
}

fn push_live_event(state: &mut TuiState, mut line: crate::tui::LiveEventLine) {
    use crate::tui::LiveEventKind;

    if line.kind == LiveEventKind::AssistantDelta {
        if let Some(last) = state.live_events.last_mut()
            && last.kind == LiveEventKind::Assistant
        {
            last.text.push_str(&line.text);
            last.offset = line.offset;
            state.scroll_offset = 0;
            return;
        }
        line.kind = LiveEventKind::Assistant;
    } else if line.kind == LiveEventKind::Assistant
        && let Some(last) = state.live_events.last_mut()
        && last.kind == LiveEventKind::Assistant
    {
        last.text = line.text;
        last.offset = line.offset;
        state.scroll_offset = 0;
        return;
    }
    state.live_events.push(line);
    state.scroll_offset = 0;
}

struct TerminalSession {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl TerminalSession {
    fn enter() -> AppResult<Self> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen, EnableBracketedPaste)?;
        let terminal = Terminal::new(CrosstermBackend::new(stdout))?;
        Ok(Self { terminal })
    }

    fn draw(&mut self, state: &TuiState) -> AppResult<()> {
        self.terminal.draw(|frame| render(frame, state))?;
        Ok(())
    }
}

impl Drop for TerminalSession {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(
            self.terminal.backend_mut(),
            DisableBracketedPaste,
            LeaveAlternateScreen
        );
        let _ = self.terminal.show_cursor();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        daemon::protocol::{BufferedStreamEvent, HelloResult, ProtocolError, SessionSummary},
        tui::TranscriptState,
    };
    use serde_json::json;

    fn buffered_event(offset: u64, event: serde_json::Value) -> BufferedStreamEvent {
        serde_json::from_value(json!({"offset": offset, "event": event})).unwrap()
    }

    fn ledger_event(offset: u64, event: serde_json::Value) -> BufferedStreamEvent {
        buffered_event(
            offset,
            json!({
                "kind": "ledger",
                "record": {
                    "seq": offset,
                    "occurred_at_ms": offset,
                    "event": event
                }
            }),
        )
    }

    fn press_key(
        key: KeyEvent,
        state: &mut TuiState,
        runtime: &UiRuntime,
        sender: &Sender<ClientCommand>,
    ) -> bool {
        handle_key_press(key, state, runtime, sender, None, None)
    }

    #[cfg(unix)]
    #[test]
    fn tui_client_bounds_a_stalled_hello() {
        use std::os::unix::net::UnixListener;

        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let config = DaemonConnectionConfig::resolve(workspace.path(), Some(socket_path)).unwrap();
        let server = thread::spawn(move || {
            let _stream = listener.accept().unwrap();
            thread::sleep(Duration::from_millis(150));
        });
        let mut client = connect_daemon(&config, Duration::from_millis(50)).unwrap();

        let started = Instant::now();
        let error = client.hello(&config.workspace_root).unwrap_err();
        let elapsed = started.elapsed();
        server.join().unwrap();

        assert!(matches!(
            error,
            AppError::Io(error) if error.kind() == io::ErrorKind::TimedOut
        ));
        assert!(elapsed < Duration::from_secs(1), "request took {elapsed:?}");
        assert_eq!(DAEMON_CLIENT_TIMEOUT, Duration::from_secs(3));
    }

    #[test]
    fn submit_composer_uses_run_start_when_idle() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        state.composer = "start work".into();
        let runtime = UiRuntime::from_state(&state, None);

        assert!(submit_composer(
            &sender,
            &mut state,
            &runtime,
            None,
            Some("plato.toml".into())
        ));

        match receiver.try_recv().unwrap() {
            ClientCommand::RunStart {
                question,
                config_path,
            } => {
                assert_eq!(question, "start work");
                assert_eq!(config_path.as_deref(), Some("plato.toml"));
            }
            other => panic!("unexpected command: {other:?}"),
        }
        assert!(state.composer.is_empty());
    }

    #[test]
    fn submit_composer_uses_message_append_when_session_selected() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        state.selected_session_id = Some("session_1".into());
        state.composer = "continue work".into();
        let runtime = UiRuntime::from_state(&state, None);

        assert!(submit_composer(
            &sender,
            &mut state,
            &runtime,
            None,
            Some("plato.toml".into())
        ));

        match receiver.try_recv().unwrap() {
            ClientCommand::MessageAppend {
                message,
                session_id,
                config_path,
            } => {
                assert_eq!(message, "continue work");
                assert_eq!(session_id, "session_1");
                assert_eq!(config_path.as_deref(), Some("plato.toml"));
            }
            other => panic!("unexpected command: {other:?}"),
        }
        assert!(state.composer.is_empty());
    }

    #[test]
    fn submit_composer_queues_follow_up_while_run_is_polling() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        state.composer = "next turn".into();
        state.composer_cursor = state.composer.len();
        let runtime = UiRuntime {
            active_run_id: Some("run_1".into()),
            config_path: Some("plato.toml".into()),
            next_offset: 0,
            poll_in_flight: false,
            polling: true,
            last_poll: Instant::now(),
            tool_inputs: HashMap::new(),
            active_since: Some(Instant::now()),
        };

        assert!(submit_composer(&sender, &mut state, &runtime, None, None));

        assert!(receiver.try_recv().is_err());
        assert!(state.composer.is_empty());
        assert_eq!(state.composer_cursor, 0);
        assert_eq!(state.queued_messages, vec!["next turn"]);
        assert_eq!(state.input_history, vec!["next turn"]);
        assert_eq!(
            state.status_message.as_deref(),
            Some("queued for next turn")
        );
    }

    #[test]
    fn submit_selected_session_queues_without_second_active_run() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        state.selected_session_id = Some("session_1".into());
        state.composer = "next turn".into();
        state.composer_cursor = state.composer.len();
        let runtime = UiRuntime {
            active_run_id: Some("run_1".into()),
            config_path: Some("plato.toml".into()),
            next_offset: 0,
            poll_in_flight: false,
            polling: true,
            last_poll: Instant::now(),
            tool_inputs: HashMap::new(),
            active_since: Some(Instant::now()),
        };

        assert!(submit_composer(&sender, &mut state, &runtime, None, None));

        assert!(receiver.try_recv().is_err());
        assert_eq!(state.queued_messages, vec!["next turn"]);
        assert_eq!(
            state.status_message.as_deref(),
            Some("queued for next turn")
        );
    }

    #[test]
    fn question_mark_opens_and_esc_closes_help() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        let runtime = UiRuntime::from_state(&state, None);

        assert!(handle_key_press(
            KeyEvent::new(KeyCode::Char('?'), KeyModifiers::empty()),
            &mut state,
            &runtime,
            &sender,
            None,
            None,
        ));
        assert!(state.help_visible);

        assert!(handle_key_press(
            KeyEvent::new(KeyCode::Esc, KeyModifiers::empty()),
            &mut state,
            &runtime,
            &sender,
            None,
            None,
        ));
        assert!(!state.help_visible);
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn help_command_opens_help_without_daemon_command() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        state.composer = "/help".into();
        state.composer_cursor = state.composer.len();
        let runtime = UiRuntime::from_state(&state, None);

        assert!(submit_composer(&sender, &mut state, &runtime, None, None));

        assert!(state.help_visible);
        assert_eq!(state.status_message.as_deref(), Some("help opened"));
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn issue_prep_command_sends_typed_daemon_request() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        state.composer = "/issue-prep make retries bounded and testable".into();
        state.composer_cursor = state.composer.len();
        let runtime = UiRuntime::from_state(&state, None);

        assert!(submit_composer(
            &sender,
            &mut state,
            &runtime,
            None,
            Some("plato.toml".into())
        ));

        match receiver.try_recv().unwrap() {
            ClientCommand::IssuePrepStart { input, config_path } => {
                assert_eq!(input, "make retries bounded and testable");
                assert_eq!(config_path.as_deref(), Some("plato.toml"));
            }
            other => panic!("unexpected command: {other:?}"),
        }
        assert!(state.issue_prep_started_at.is_some());
        assert_eq!(state.status_message.as_deref(), Some("issue prep running"));
        assert_eq!(
            state.live_events.last().map(|event| event.text.as_str()),
            Some("/issue-prep make retries bounded and testable")
        );
    }

    #[test]
    fn issue_prep_command_channel_failure_clears_activity() {
        let (sender, receiver) = mpsc::channel();
        drop(receiver);
        let mut state = test_state();
        let runtime = UiRuntime::from_state(&state, None);

        start_issue_prep(
            &sender,
            &mut state,
            &runtime,
            "/issue-prep make the proof deterministic",
            None,
        );

        assert!(state.issue_prep_started_at.is_none());
        assert_eq!(
            state.status_message.as_deref(),
            Some("daemon client worker stopped")
        );
    }

    #[test]
    fn issue_prep_command_requires_input() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        state.composer = "/issue-prep".into();
        state.composer_cursor = state.composer.len();
        let runtime = UiRuntime::from_state(&state, None);

        assert!(submit_composer(&sender, &mut state, &runtime, None, None));

        assert!(state.issue_prep_started_at.is_none());
        assert_eq!(
            state.status_message.as_deref(),
            Some("usage: /issue-prep <rough issue>")
        );
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn issue_prep_command_rejects_concurrent_work() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        state.issue_prep_started_at = Some(Instant::now());
        state.composer = "/issue-prep another issue".into();
        state.composer_cursor = state.composer.len();
        let runtime = UiRuntime::from_state(&state, None);

        assert!(submit_composer(&sender, &mut state, &runtime, None, None));
        assert_eq!(
            state.status_message.as_deref(),
            Some("issue prep already running")
        );
        assert!(receiver.try_recv().is_err());

        state.issue_prep_started_at = None;
        state.composer = "/issue-prep another issue".into();
        state.composer_cursor = state.composer.len();
        let mut runtime = UiRuntime::from_state(&state, None);
        runtime.polling = true;

        assert!(submit_composer(&sender, &mut state, &runtime, None, None));
        assert_eq!(
            state.status_message.as_deref(),
            Some("issue prep is unavailable while a run is active")
        );
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn normal_message_queues_while_issue_prep_runs() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        state.issue_prep_started_at = Some(Instant::now());
        state.composer = "follow up".into();
        state.composer_cursor = state.composer.len();
        let runtime = UiRuntime::from_state(&state, None);

        assert!(submit_composer(&sender, &mut state, &runtime, None, None));

        assert_eq!(state.queued_messages, vec!["follow up"]);
        assert_eq!(
            state.status_message.as_deref(),
            Some("queued for next turn")
        );
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn clear_command_clears_visible_transcript_only() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        state.transcript = TranscriptState::Unavailable {
            run_id: "run_1".into(),
            error: "boom".into(),
        };
        state.live_events = vec![crate::tui::LiveEventLine::assistant(Some(1), "hello")];
        state.stream_warning = Some("lagged".into());
        state.scroll_offset = 10;
        state.composer = "/clear".into();
        state.composer_cursor = state.composer.len();
        let runtime = UiRuntime::from_state(&state, None);

        assert!(submit_composer(&sender, &mut state, &runtime, None, None));

        assert_eq!(state.transcript, TranscriptState::None);
        assert!(state.live_events.is_empty());
        assert!(state.stream_warning.is_none());
        assert_eq!(state.scroll_offset, 0);
        assert_eq!(
            state.status_message.as_deref(),
            Some("visible transcript cleared")
        );
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn quit_command_exits_without_daemon_command() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        state.composer = "/quit".into();
        state.composer_cursor = state.composer.len();
        let runtime = UiRuntime::from_state(&state, None);

        assert!(!submit_composer(&sender, &mut state, &runtime, None, None));

        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn reconnect_command_only_sends_load_when_offline() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        state.composer = "/reconnect".into();
        state.composer_cursor = state.composer.len();
        let runtime = UiRuntime::from_state(&state, None);

        assert!(submit_composer(&sender, &mut state, &runtime, None, None));
        assert_eq!(state.status_message.as_deref(), Some("already connected"));
        assert!(receiver.try_recv().is_err());

        state.connection = crate::tui::ConnectionState::Disconnected {
            error: "connection closed".into(),
        };
        state.composer = "/reconnect".into();
        state.composer_cursor = state.composer.len();
        assert!(submit_composer(
            &sender,
            &mut state,
            &runtime,
            Some("run_1".into()),
            None
        ));

        assert_eq!(state.status_message.as_deref(), Some("reconnecting"));
        match receiver.try_recv().unwrap() {
            ClientCommand::Load { run_id } => assert_eq!(run_id.as_deref(), Some("run_1")),
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn unknown_slash_command_does_not_hit_daemon() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        state.composer = "/wat".into();
        state.composer_cursor = state.composer.len();
        let runtime = UiRuntime::from_state(&state, None);

        assert!(submit_composer(&sender, &mut state, &runtime, None, None));

        assert_eq!(
            state.status_message.as_deref(),
            Some("unknown command: /wat; try /help")
        );
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn sessions_command_opens_picker_without_daemon_command() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        state.sessions = vec![test_session(
            "session_1",
            "run_1",
            RunStateName::Finished,
            "first",
        )];
        state.composer = "/sessions".into();
        state.composer_cursor = state.composer.len();
        let runtime = UiRuntime::from_state(&state, None);

        assert!(submit_composer(&sender, &mut state, &runtime, None, None));

        assert_eq!(
            state.session_picker,
            Some(SessionPickerView { selected: 0 })
        );
        assert_eq!(
            state.status_message.as_deref(),
            Some("session picker opened")
        );
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn session_picker_enter_loads_selected_session() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        state.sessions = vec![
            test_session("session_1", "run_1", RunStateName::Finished, "first"),
            test_session("session_2", "run_2", RunStateName::Interrupted, "second"),
        ];
        state.session_picker = Some(SessionPickerView { selected: 0 });
        let runtime = UiRuntime::from_state(&state, None);

        assert!(press_key(
            KeyEvent::new(KeyCode::Down, KeyModifiers::empty()),
            &mut state,
            &runtime,
            &sender,
        ));
        assert!(press_key(
            KeyEvent::new(KeyCode::Enter, KeyModifiers::empty()),
            &mut state,
            &runtime,
            &sender,
        ));

        assert!(state.session_picker.is_none());
        assert_eq!(state.selected_session_id.as_deref(), Some("session_2"));
        match receiver.try_recv().unwrap() {
            ClientCommand::LoadSession { session_id } => assert_eq!(session_id, "session_2"),
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn new_command_clears_selected_session_for_fresh_submit() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        state.selected_session_id = Some("session_1".into());
        state.live_events = vec![crate::tui::LiveEventLine::assistant(None, "old")];
        state.composer = "/new".into();
        state.composer_cursor = state.composer.len();
        let runtime = UiRuntime::from_state(&state, None);

        assert!(submit_composer(&sender, &mut state, &runtime, None, None));

        assert!(state.selected_session_id.is_none());
        assert!(state.live_events.is_empty());
        assert_eq!(
            state.status_message.as_deref(),
            Some("new session selected")
        );
        assert!(receiver.try_recv().is_err());

        state.composer = "fresh work".into();
        state.composer_cursor = state.composer.len();
        assert!(submit_composer(&sender, &mut state, &runtime, None, None));
        match receiver.try_recv().unwrap() {
            ClientCommand::RunStart { question, .. } => assert_eq!(question, "fresh work"),
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn slash_popup_filters_and_tab_completes_selected_command() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        let runtime = UiRuntime::from_state(&state, None);

        assert!(press_key(
            KeyEvent::new(KeyCode::Char('/'), KeyModifiers::empty()),
            &mut state,
            &runtime,
            &sender,
        ));
        assert_eq!(
            state
                .slash_popup
                .as_ref()
                .map(|popup| popup.filter.as_str()),
            Some("")
        );

        assert!(press_key(
            KeyEvent::new(KeyCode::Char('c'), KeyModifiers::empty()),
            &mut state,
            &runtime,
            &sender,
        ));
        assert_eq!(state.composer, "/c");
        assert_eq!(
            state
                .slash_popup
                .as_ref()
                .map(|popup| popup.filter.as_str()),
            Some("c")
        );

        assert!(press_key(
            KeyEvent::new(KeyCode::Tab, KeyModifiers::empty()),
            &mut state,
            &runtime,
            &sender,
        ));
        assert_eq!(state.composer, "/clear ");
        assert_eq!(state.composer_cursor, state.composer.len());
        assert!(state.slash_popup.is_none());
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn slash_popup_enter_dispatches_selected_command() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        let runtime = UiRuntime::from_state(&state, None);

        assert!(press_key(
            KeyEvent::new(KeyCode::Char('/'), KeyModifiers::empty()),
            &mut state,
            &runtime,
            &sender,
        ));
        assert!(press_key(
            KeyEvent::new(KeyCode::Down, KeyModifiers::empty()),
            &mut state,
            &runtime,
            &sender,
        ));
        assert!(press_key(
            KeyEvent::new(KeyCode::Enter, KeyModifiers::empty()),
            &mut state,
            &runtime,
            &sender,
        ));

        assert_eq!(
            state.status_message.as_deref(),
            Some("visible transcript cleared")
        );
        assert_eq!(state.input_history, vec!["/clear"]);
        assert!(state.composer.is_empty());
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn slash_popup_ctrl_navigation_matches_codex_keys() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        let runtime = UiRuntime::from_state(&state, None);

        assert!(press_key(
            KeyEvent::new(KeyCode::Char('/'), KeyModifiers::empty()),
            &mut state,
            &runtime,
            &sender,
        ));
        assert!(press_key(
            KeyEvent::new(KeyCode::Char('n'), KeyModifiers::CONTROL),
            &mut state,
            &runtime,
            &sender,
        ));
        assert_eq!(
            state.slash_popup.as_ref().map(|popup| popup.selected),
            Some(1)
        );
        assert!(press_key(
            KeyEvent::new(KeyCode::Char('p'), KeyModifiers::CONTROL),
            &mut state,
            &runtime,
            &sender,
        ));
        assert_eq!(
            state.slash_popup.as_ref().map(|popup| popup.selected),
            Some(0)
        );
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn codex_newline_keys_insert_newlines_without_submitting() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        let runtime = UiRuntime::from_state(&state, None);

        state.composer = "a".into();
        state.composer_cursor = state.composer.len();
        for key in [
            KeyEvent::new(KeyCode::Enter, KeyModifiers::SHIFT),
            KeyEvent::new(KeyCode::Enter, KeyModifiers::ALT),
            KeyEvent::new(KeyCode::Char('j'), KeyModifiers::CONTROL),
            KeyEvent::new(KeyCode::Char('m'), KeyModifiers::CONTROL),
        ] {
            assert!(press_key(key, &mut state, &runtime, &sender));
            state.composer.push('x');
            state.composer_cursor = state.composer.len();
        }

        assert_eq!(state.composer, "a\nx\nx\nx\nx");
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn paste_normalizes_carriage_returns_and_updates_popup() {
        let (_sender, _receiver) = mpsc::channel::<ClientCommand>();
        let mut state = test_state();

        handle_paste_text(&mut state, "/c\rnext");

        assert_eq!(state.composer, "/c\nnext");
        assert_eq!(state.composer_cursor, state.composer.len());
        assert!(state.slash_popup.is_none());
    }

    #[test]
    fn kill_and_yank_follow_codex_composer_basics() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        let runtime = UiRuntime::from_state(&state, None);
        state.composer = "hello world".into();
        state.composer_cursor = "hello ".len();

        assert!(press_key(
            KeyEvent::new(KeyCode::Char('k'), KeyModifiers::CONTROL),
            &mut state,
            &runtime,
            &sender,
        ));
        assert_eq!(state.composer, "hello ");
        assert_eq!(state.composer_kill_buffer, "world");

        assert!(press_key(
            KeyEvent::new(KeyCode::Char('y'), KeyModifiers::CONTROL),
            &mut state,
            &runtime,
            &sender,
        ));
        assert_eq!(state.composer, "hello world");
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn printable_r_is_composer_text_when_connected() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        let runtime = UiRuntime::from_state(&state, None);

        for ch in "read write current target/current".chars() {
            assert!(handle_key_press(
                KeyEvent::new(KeyCode::Char(ch), KeyModifiers::empty()),
                &mut state,
                &runtime,
                &sender,
                None,
                None,
            ));
        }

        assert_eq!(state.composer, "read write current target/current");
        assert_eq!(state.composer_cursor, state.composer.len());
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn composer_edits_at_cursor_and_supports_multiline() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        let runtime = UiRuntime::from_state(&state, None);

        for ch in "helo".chars() {
            assert!(handle_key_press(
                KeyEvent::new(KeyCode::Char(ch), KeyModifiers::empty()),
                &mut state,
                &runtime,
                &sender,
                None,
                None,
            ));
        }
        assert!(handle_key_press(
            KeyEvent::new(KeyCode::Left, KeyModifiers::empty()),
            &mut state,
            &runtime,
            &sender,
            None,
            None,
        ));
        assert!(handle_key_press(
            KeyEvent::new(KeyCode::Char('l'), KeyModifiers::empty()),
            &mut state,
            &runtime,
            &sender,
            None,
            None,
        ));

        assert_eq!(state.composer, "hello");
        assert_eq!(state.composer_cursor, 4);

        assert!(handle_key_press(
            KeyEvent::new(KeyCode::End, KeyModifiers::empty()),
            &mut state,
            &runtime,
            &sender,
            None,
            None,
        ));
        assert!(handle_key_press(
            KeyEvent::new(KeyCode::Enter, KeyModifiers::ALT),
            &mut state,
            &runtime,
            &sender,
            None,
            None,
        ));
        for ch in "world".chars() {
            assert!(handle_key_press(
                KeyEvent::new(KeyCode::Char(ch), KeyModifiers::empty()),
                &mut state,
                &runtime,
                &sender,
                None,
                None,
            ));
        }

        assert_eq!(state.composer, "hello\nworld");
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn history_navigation_recalls_submitted_inputs() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        let runtime = UiRuntime::from_state(&state, None);
        state.input_history = vec!["first".into(), "second".into()];

        assert!(handle_key_press(
            KeyEvent::new(KeyCode::Up, KeyModifiers::empty()),
            &mut state,
            &runtime,
            &sender,
            None,
            None,
        ));
        assert_eq!(state.composer, "second");
        assert!(handle_key_press(
            KeyEvent::new(KeyCode::Up, KeyModifiers::empty()),
            &mut state,
            &runtime,
            &sender,
            None,
            None,
        ));
        assert_eq!(state.composer, "first");
        assert!(handle_key_press(
            KeyEvent::new(KeyCode::Down, KeyModifiers::empty()),
            &mut state,
            &runtime,
            &sender,
            None,
            None,
        ));
        assert_eq!(state.composer, "second");
        assert!(handle_key_press(
            KeyEvent::new(KeyCode::Down, KeyModifiers::empty()),
            &mut state,
            &runtime,
            &sender,
            None,
            None,
        ));
        assert!(state.composer.is_empty());
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn r_reconnects_from_disconnected_state() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        state.connection = crate::tui::ConnectionState::Disconnected {
            error: "connection closed".into(),
        };
        let runtime = UiRuntime::from_state(&state, None);

        assert!(handle_key_press(
            KeyEvent::new(KeyCode::Char('r'), KeyModifiers::empty()),
            &mut state,
            &runtime,
            &sender,
            Some("run_1".into()),
            None,
        ));

        assert_eq!(state.status_message.as_deref(), Some("reconnecting"));
        match receiver.try_recv().unwrap() {
            ClientCommand::Load { run_id } => assert_eq!(run_id.as_deref(), Some("run_1")),
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn events_result_updates_live_state_and_requests_reload_on_finish() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        let mut runtime = UiRuntime {
            active_run_id: Some("run_1".into()),
            config_path: None,
            next_offset: 0,
            poll_in_flight: true,
            polling: true,
            last_poll: Instant::now(),
            tool_inputs: HashMap::new(),
            active_since: Some(Instant::now()),
        };
        let result = EventsStreamResult {
            run_id: "run_1".into(),
            from_offset: 0,
            next_offset: 2,
            status: RunStateName::Finished,
            events: vec![ledger_event(
                1,
                json!({"event": "run_finished", "run_id": "run_1"}),
            )],
        };

        apply_events_result(&mut state, &mut runtime, &sender, result);

        assert_eq!(runtime.next_offset, 2);
        assert!(!runtime.polling);
        assert_eq!(state.live_events[0].text, "run finished");
        match receiver.try_recv().unwrap() {
            ClientCommand::Load { run_id } => assert_eq!(run_id.as_deref(), Some("run_1")),
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn assistant_delta_flood_accumulates_into_one_message() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        let mut runtime = UiRuntime {
            active_run_id: Some("run_1".into()),
            config_path: None,
            next_offset: 0,
            poll_in_flight: true,
            polling: true,
            last_poll: Instant::now(),
            tool_inputs: HashMap::new(),
            active_since: Some(Instant::now()),
        };
        let events = (0..500)
            .map(|index| {
                buffered_event(
                    index,
                    json!({
                        "kind": "assistant_delta",
                        "run_id": "run_1",
                        "turn_id": "turn_1",
                        "step": 0,
                        "delta_index": index,
                        "text": "x"
                    }),
                )
            })
            .collect::<Vec<_>>();

        apply_events_result(
            &mut state,
            &mut runtime,
            &sender,
            EventsStreamResult {
                run_id: "run_1".into(),
                from_offset: 0,
                next_offset: 500,
                status: RunStateName::Running,
                events,
            },
        );

        assert_eq!(state.live_events.len(), 1);
        assert_eq!(
            state.live_events[0].kind,
            crate::tui::LiveEventKind::Assistant
        );
        assert_eq!(state.live_events[0].text.len(), 500);
        assert!(state.stream_warning.is_none());
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn full_event_page_immediately_requests_catch_up_poll() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        let mut runtime = UiRuntime {
            active_run_id: Some("run_1".into()),
            config_path: None,
            next_offset: 0,
            poll_in_flight: true,
            polling: true,
            last_poll: Instant::now(),
            tool_inputs: HashMap::new(),
            active_since: Some(Instant::now()),
        };
        let events = (0..EVENT_LIMIT)
            .map(|index| {
                buffered_event(
                    index as u64,
                    json!({
                        "kind": "assistant_delta",
                        "run_id": "run_1",
                        "turn_id": "turn_1",
                        "step": 0,
                        "delta_index": index,
                        "text": "x"
                    }),
                )
            })
            .collect::<Vec<_>>();

        apply_events_result(
            &mut state,
            &mut runtime,
            &sender,
            EventsStreamResult {
                run_id: "run_1".into(),
                from_offset: 0,
                next_offset: EVENT_LIMIT as u64,
                status: RunStateName::Running,
                events,
            },
        );

        match receiver.try_recv().unwrap() {
            ClientCommand::PollEvents {
                run_id,
                from_offset,
            } => {
                assert_eq!(run_id, "run_1");
                assert_eq!(from_offset, Some(EVENT_LIMIT as u64));
            }
            other => panic!("unexpected command: {other:?}"),
        }
        assert!(runtime.poll_in_flight);
        assert!(runtime.polling);
    }

    #[test]
    fn model_requested_event_updates_status_model() {
        let (sender, _receiver) = mpsc::channel();
        let mut state = test_state();
        let mut runtime = UiRuntime {
            active_run_id: Some("run_1".into()),
            config_path: None,
            next_offset: 0,
            poll_in_flight: true,
            polling: true,
            last_poll: Instant::now(),
            tool_inputs: HashMap::new(),
            active_since: Some(Instant::now()),
        };

        apply_events_result(
            &mut state,
            &mut runtime,
            &sender,
            EventsStreamResult {
                run_id: "run_1".into(),
                from_offset: 0,
                next_offset: 1,
                status: RunStateName::Running,
                events: vec![ledger_event(
                    0,
                    json!({
                        "event": "model_requested",
                        "run_id": "run_1",
                        "turn_id": "turn_1",
                        "step": 0,
                        "model": "openrouter/auto"
                    }),
                )],
            },
        );

        assert_eq!(state.active_model.as_deref(), Some("openrouter/auto"));
    }

    #[test]
    fn run_response_selects_returned_session_for_continuation() {
        let mut state = test_state();
        let mut runtime = UiRuntime::from_state(&state, None);

        apply_run_response(
            &mut state,
            &mut runtime,
            RunStartResult {
                run_id: "run_1".into(),
                session_id: "session_1".into(),
                ledger_path: "/tmp/agent.db".into(),
                status: RunStateName::Running,
                final_answer: None,
            },
            "run started",
        );

        assert_eq!(state.selected_session_id.as_deref(), Some("session_1"));
        assert_eq!(runtime.active_run_id.as_deref(), Some("run_1"));
    }

    #[test]
    fn page_keys_adjust_scroll_offset() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        let runtime = UiRuntime::from_state(&state, None);

        assert!(handle_key_press(
            KeyEvent::new(KeyCode::PageUp, KeyModifiers::empty()),
            &mut state,
            &runtime,
            &sender,
            None,
            None,
        ));
        assert_eq!(state.scroll_offset, SCROLL_PAGE_LINES);

        assert!(handle_key_press(
            KeyEvent::new(KeyCode::PageDown, KeyModifiers::empty()),
            &mut state,
            &runtime,
            &sender,
            None,
            None,
        ));
        assert_eq!(state.scroll_offset, 0);
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn events_result_drains_queued_message_after_finish() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        state.queued_messages = vec!["next turn".into()];
        let mut runtime = UiRuntime {
            active_run_id: Some("run_1".into()),
            config_path: Some("plato.toml".into()),
            next_offset: 0,
            poll_in_flight: true,
            polling: true,
            last_poll: Instant::now(),
            tool_inputs: HashMap::new(),
            active_since: Some(Instant::now()),
        };
        let result = EventsStreamResult {
            run_id: "run_1".into(),
            from_offset: 0,
            next_offset: 1,
            status: RunStateName::Finished,
            events: Vec::new(),
        };

        apply_events_result(&mut state, &mut runtime, &sender, result);

        match receiver.try_recv().unwrap() {
            ClientCommand::Load { run_id } => assert_eq!(run_id.as_deref(), Some("run_1")),
            other => panic!("unexpected command: {other:?}"),
        }
        match receiver.try_recv().unwrap() {
            ClientCommand::RunStart {
                question,
                config_path,
            } => {
                assert_eq!(question, "next turn");
                assert_eq!(config_path.as_deref(), Some("plato.toml"));
            }
            other => panic!("unexpected command: {other:?}"),
        }
        assert!(state.queued_messages.is_empty());
        assert_eq!(
            state.status_message.as_deref(),
            Some("submitted queued message")
        );
    }

    #[test]
    fn events_result_drains_queued_selected_session_message_after_finish() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        state.selected_session_id = Some("session_1".into());
        state.queued_messages = vec!["next turn".into()];
        let mut runtime = UiRuntime {
            active_run_id: Some("run_1".into()),
            config_path: Some("plato.toml".into()),
            next_offset: 0,
            poll_in_flight: true,
            polling: true,
            last_poll: Instant::now(),
            tool_inputs: HashMap::new(),
            active_since: Some(Instant::now()),
        };
        let result = EventsStreamResult {
            run_id: "run_1".into(),
            from_offset: 0,
            next_offset: 1,
            status: RunStateName::Finished,
            events: Vec::new(),
        };

        apply_events_result(&mut state, &mut runtime, &sender, result);
        let _load = receiver.try_recv().unwrap();

        match receiver.try_recv().unwrap() {
            ClientCommand::MessageAppend {
                message,
                session_id,
                config_path,
            } => {
                assert_eq!(message, "next turn");
                assert_eq!(session_id, "session_1");
                assert_eq!(config_path.as_deref(), Some("plato.toml"));
            }
            other => panic!("unexpected command: {other:?}"),
        }
        assert!(state.queued_messages.is_empty());
    }

    #[test]
    fn issue_prep_candidate_is_rendered_with_artifact_path() {
        let (command_sender, command_receiver) = mpsc::channel();
        let (event_sender, event_receiver) = mpsc::channel();
        let mut state = test_state();
        state.issue_prep_started_at = Some(Instant::now());
        let mut runtime = UiRuntime::from_state(&state, None);
        event_sender
            .send(ClientEvent::IssuePrepFinished(IssuePrepStartResult {
                run_dir: "/tmp/workspace/.plato/issue-prep/run_1".into(),
                outcome: IssuePrepResult::Candidate {
                    markdown: "# Prepared issue".into(),
                },
            }))
            .unwrap();

        drain_client_events(&mut state, &mut runtime, &event_receiver, &command_sender);

        assert!(state.issue_prep_started_at.is_none());
        assert_eq!(
            state.status_message.as_deref(),
            Some("issue ready; artifacts: /tmp/workspace/.plato/issue-prep/run_1")
        );
        assert!(state.live_events.iter().any(|event| {
            event.kind == crate::tui::LiveEventKind::Assistant && event.text == "# Prepared issue"
        }));
        assert!(state.live_events.iter().any(|event| {
            event.kind == crate::tui::LiveEventKind::Status
                && event.text.contains(".plato/issue-prep/run_1")
        }));
        assert!(command_receiver.try_recv().is_err());
    }

    #[test]
    fn issue_prep_daemon_error_clears_activity() {
        let (command_sender, command_receiver) = mpsc::channel();
        let (event_sender, event_receiver) = mpsc::channel();
        let mut state = test_state();
        state.issue_prep_started_at = Some(Instant::now());
        let mut runtime = UiRuntime::from_state(&state, None);
        event_sender
            .send(ClientEvent::Failed {
                context: "issue-prep.start",
                error: crate::AppError::DaemonResponse(ProtocolError {
                    code: "issue_prep_failed".into(),
                    message: "provider failed".into(),
                }),
            })
            .unwrap();

        drain_client_events(&mut state, &mut runtime, &event_receiver, &command_sender);

        assert!(state.issue_prep_started_at.is_none());
        assert_eq!(
            state.status_message.as_deref(),
            Some(
                "issue-prep.start failed: daemon protocol error issue_prep_failed: provider failed"
            )
        );
        assert!(state.live_events.iter().any(|event| {
            event.kind == crate::tui::LiveEventKind::Warning
                && event.text.contains("provider failed")
        }));
        assert!(command_receiver.try_recv().is_err());
    }

    #[test]
    fn issue_prep_block_is_rendered_with_reasons() {
        let (command_sender, command_receiver) = mpsc::channel();
        let (event_sender, event_receiver) = mpsc::channel();
        let mut state = test_state();
        state.issue_prep_started_at = Some(Instant::now());
        let mut runtime = UiRuntime::from_state(&state, None);
        event_sender
            .send(ClientEvent::IssuePrepFinished(IssuePrepStartResult {
                run_dir: "/tmp/workspace/.plato/issue-prep/run_2".into(),
                outcome: IssuePrepResult::Blocked {
                    stage: "review".into(),
                    reasons: vec!["acceptance is not testable".into()],
                },
            }))
            .unwrap();

        drain_client_events(&mut state, &mut runtime, &event_receiver, &command_sender);

        assert!(state.issue_prep_started_at.is_none());
        assert_eq!(
            state.status_message.as_deref(),
            Some(
                "issue prep blocked at review; artifacts: \
                 /tmp/workspace/.plato/issue-prep/run_2"
            )
        );
        assert!(state.live_events.iter().any(|event| {
            event.kind == crate::tui::LiveEventKind::Warning
                && event.text.contains("acceptance is not testable")
                && event.text.contains(".plato/issue-prep/run_2")
        }));
        assert!(command_receiver.try_recv().is_err());
    }

    #[test]
    fn lagged_stream_resumes_at_current_tip() {
        let (command_sender, command_receiver) = mpsc::channel();
        let (event_sender, event_receiver) = mpsc::channel();
        let mut state = test_state();
        state.active_run = Some(crate::tui::ActiveRunView {
            run_id: "run_1".into(),
            status: RunStateName::Running,
        });
        let mut runtime = UiRuntime {
            active_run_id: Some("run_1".into()),
            config_path: None,
            next_offset: 7,
            poll_in_flight: true,
            polling: true,
            last_poll: Instant::now(),
            tool_inputs: HashMap::new(),
            active_since: Some(Instant::now()),
        };
        event_sender
            .send(ClientEvent::Failed {
                context: "events.stream",
                error: crate::AppError::DaemonResponse(ProtocolError {
                    code: "lagged".into(),
                    message: "offset is no longer buffered".into(),
                }),
            })
            .unwrap();

        drain_client_events(&mut state, &mut runtime, &event_receiver, &command_sender);

        assert!(
            state
                .stream_warning
                .as_deref()
                .unwrap()
                .contains("current tip")
        );
        assert!(runtime.poll_in_flight);
        assert!(matches!(
            command_receiver.try_recv().unwrap(),
            ClientCommand::PollEvents {
                run_id,
                from_offset: None,
            } if run_id == "run_1"
        ));
    }

    #[test]
    fn stream_connection_failure_enters_disconnected_and_stops_polling() {
        let (command_sender, command_receiver) = mpsc::channel();
        let (event_sender, event_receiver) = mpsc::channel();
        let mut state = test_state();
        state.active_run = Some(crate::tui::ActiveRunView {
            run_id: "run_1".into(),
            status: RunStateName::Running,
        });
        let mut runtime = UiRuntime {
            active_run_id: Some("run_1".into()),
            config_path: None,
            next_offset: 7,
            poll_in_flight: true,
            polling: true,
            last_poll: Instant::now() - ACTIVE_POLL_INTERVAL,
            tool_inputs: HashMap::new(),
            active_since: Some(Instant::now()),
        };
        event_sender
            .send(ClientEvent::Failed {
                context: "events.stream",
                error: crate::AppError::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "Connection refused",
                )),
            })
            .unwrap();

        drain_client_events(&mut state, &mut runtime, &event_receiver, &command_sender);
        maybe_poll_events(&mut runtime, &command_sender);

        assert!(!runtime.polling);
        assert!(!runtime.poll_in_flight);
        assert!(is_disconnected(&state));
        assert!(command_receiver.try_recv().is_err());
    }

    #[test]
    fn connection_error_classification_uses_typed_errors() {
        assert!(is_connection_error(&AppError::DaemonLockHeld {
            path: PathBuf::from("/tmp/agent.lock"),
            owner: "pid 1".into(),
        }));
        assert!(is_connection_error(&AppError::DaemonProtocol(
            "response id mismatch".into()
        )));
        assert!(is_connection_error(&AppError::Io(std::io::Error::other(
            "socket failed"
        ))));
        assert!(is_connection_error(&AppError::DaemonResponse(
            ProtocolError {
                code: ERROR_UNSUPPORTED_VERSION.into(),
                message: "unsupported".into(),
            }
        )));
        assert!(is_connection_error(&AppError::DaemonResponse(
            ProtocolError {
                code: ERROR_WORKSPACE_MISMATCH.into(),
                message: "wrong workspace".into(),
            }
        )));
        assert!(!is_connection_error(&AppError::DaemonResponse(
            ProtocolError {
                code: ERROR_OVERLOAD.into(),
                message: "busy".into(),
            }
        )));
    }

    #[test]
    fn approval_preview_updates_when_tool_input_arrives_after_request() {
        let (sender, _receiver) = mpsc::channel();
        let mut state = test_state();
        let mut runtime = UiRuntime {
            active_run_id: Some("run_1".into()),
            config_path: None,
            next_offset: 0,
            poll_in_flight: true,
            polling: true,
            last_poll: Instant::now(),
            tool_inputs: HashMap::new(),
            active_since: Some(Instant::now()),
        };
        let result = EventsStreamResult {
            run_id: "run_1".into(),
            from_offset: 0,
            next_offset: 2,
            status: RunStateName::Running,
            events: vec![
                buffered_event(
                    1,
                    json!({
                        "kind": "approval_requested",
                        "run_id": "run_1",
                        "tool_call_id": "call_1",
                        "tool_name": "file.write",
                        "effect": "workspace_write",
                        "reason": "file.write requires approval"
                    }),
                ),
                ledger_event(
                    2,
                    json!({
                        "event": "tool_call_proposed",
                        "run_id": "run_1",
                        "turn_id": "turn_1",
                        "call": {
                            "id": "call_1",
                            "tool": "file.write",
                            "effect": "workspace_write",
                            "input": {
                                "path": "scratch/tui-preview.txt",
                                "content": "preview body"
                            }
                        }
                    }),
                ),
            ],
        };

        apply_events_result(&mut state, &mut runtime, &sender, result);

        let approval = state.approval.as_ref().expect("approval modal");
        assert_eq!(approval.tool_call_id, "call_1");
        assert!(approval.input_preview.contains("scratch/tui-preview.txt"));
        assert!(approval.input_preview.contains("preview body"));
    }

    #[test]
    fn approval_decisions_send_daemon_commands() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        state.approval = Some(crate::tui::ApprovalModalView {
            run_id: "run_1".into(),
            tool_call_id: "call_1".into(),
            tool_name: "file.write".into(),
            effect: "WorkspaceWrite".into(),
            reason: "requires approval".into(),
            input_preview: "{}".into(),
            approval_preview: None,
            diff_preview: None,
        });

        decide_approval(&sender, &mut state, ApprovalAction::Grant);

        assert!(state.approval.is_none());
        match receiver.try_recv().unwrap() {
            ClientCommand::ApprovalGrant {
                run_id,
                tool_call_id,
            } => {
                assert_eq!(run_id, "run_1");
                assert_eq!(tool_call_id, "call_1");
            }
            other => panic!("unexpected command: {other:?}"),
        }

        state.approval = Some(crate::tui::ApprovalModalView {
            run_id: "run_2".into(),
            tool_call_id: "call_2".into(),
            tool_name: "file.write".into(),
            effect: "WorkspaceWrite".into(),
            reason: "requires approval".into(),
            input_preview: "{}".into(),
            approval_preview: None,
            diff_preview: None,
        });

        decide_approval(&sender, &mut state, ApprovalAction::Deny);

        match receiver.try_recv().unwrap() {
            ClientCommand::ApprovalDeny {
                run_id,
                tool_call_id,
                reason,
            } => {
                assert_eq!(run_id, "run_2");
                assert_eq!(tool_call_id, "call_2");
                assert_eq!(reason, "denied by plato-tui");
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn first_cancel_requests_daemon_and_second_cancel_quits() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        state.active_run = Some(crate::tui::ActiveRunView {
            run_id: "run_1".into(),
            status: RunStateName::Running,
        });

        assert!(request_cancel(&sender, &mut state));
        assert!(state.cancel_requested);
        match receiver.try_recv().unwrap() {
            ClientCommand::RunCancel { run_id } => assert_eq!(run_id, "run_1"),
            other => panic!("unexpected command: {other:?}"),
        }

        assert!(!request_cancel(&sender, &mut state));
    }

    #[test]
    fn issue_prep_prevents_graceful_exit_until_finished() {
        let mut state = test_state();
        state.issue_prep_started_at = Some(Instant::now());

        assert!(handle_exit_request(&mut state));
        assert_eq!(
            state.status_message.as_deref(),
            Some("issue prep is still running")
        );

        state.issue_prep_started_at = None;
        assert!(!handle_exit_request(&mut state));
    }

    #[test]
    fn state_reload_preserves_issue_prep_start_time() {
        let mut state = test_state();
        let started_at = Instant::now();
        state.issue_prep_started_at = Some(started_at);
        let loaded = test_state();

        apply_loaded_state(&mut state, loaded);

        assert_eq!(state.issue_prep_started_at, Some(started_at));
    }

    fn test_state() -> TuiState {
        TuiState::connected(
            "/tmp/workspace".into(),
            "/tmp/agent.sock".into(),
            HelloResult {
                daemon_version: "0.1.0".into(),
                workspace_id: "workspace-1234".into(),
                ledger_path: "/tmp/agent.db".into(),
                capabilities: vec![],
            },
            Vec::new(),
            TranscriptState::None,
        )
    }

    fn test_session(
        session_id: &str,
        run_id: &str,
        status: RunStateName,
        latest_question: &str,
    ) -> SessionSummary {
        SessionSummary {
            session_id: session_id.into(),
            run_id: run_id.into(),
            status,
            latest_question: latest_question.into(),
            ledger_path: "/tmp/agent.db".into(),
        }
    }
}
