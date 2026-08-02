use crate::{TranscriptState, TuiState, render, render_snapshot};
use crossterm::{
    event::{
        self, DisableBracketedPaste, EnableBracketedPaste, Event, KeyCode, KeyEvent, KeyEventKind,
        KeyModifiers,
    },
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use plato_daemon_client::{ClientResult, client::DaemonConnectionConfig};
use plato_protocol::RunStateName;
use ratatui::{Terminal, backend::CrosstermBackend};
use std::{
    io::{self, Stdout},
    path::PathBuf,
    sync::mpsc::Sender,
    time::{Duration, Instant},
};

use super::{
    client::{
        ClientCommand, UiRuntime, drain_client_events, load_state, maybe_poll_events,
        spawn_client_worker,
    },
    commands::{SlashCommandAction, find_slash_command},
    state::SessionPickerView,
};

const SCROLL_PAGE_LINES: usize = 10;

#[derive(Clone, Debug, Eq, PartialEq)]
/// Options for connecting and starting the terminal client.
pub struct TuiOptions {
    /// Workspace root served by the daemon.
    pub workspace: PathBuf,
    /// Optional explicit daemon endpoint.
    pub socket: Option<PathBuf>,
    /// Optional run to select on startup.
    pub run: Option<String>,
    /// Optional config path forwarded with new run requests.
    pub config: Option<PathBuf>,
    /// Whether to render one frame and exit without entering raw mode.
    pub snapshot: bool,
}

impl TuiOptions {
    /// Creates options for a workspace using the default daemon endpoint.
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

/// Connects to the workspace daemon and runs the terminal client.
pub fn run_tui(options: TuiOptions) -> ClientResult<()> {
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
                Event::Paste(text) => state.handle_paste_text(&text),
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
        state.insert_composer_text("\n");
        return true;
    }

    if key.modifiers.contains(KeyModifiers::CONTROL) {
        match key.code {
            KeyCode::Char('a') => {
                state.move_composer_line_start();
                return true;
            }
            KeyCode::Char('b') => {
                state.move_composer_left();
                return true;
            }
            KeyCode::Char('e') => {
                state.move_composer_line_end();
                return true;
            }
            KeyCode::Char('f') => {
                state.move_composer_right();
                return true;
            }
            KeyCode::Char('k') => {
                state.delete_composer_to_line_end();
                return true;
            }
            KeyCode::Char('u') => {
                state.kill_composer_to_start();
                return true;
            }
            KeyCode::Char('w') => {
                state.delete_previous_word();
                return true;
            }
            KeyCode::Char('y') => {
                state.yank_composer_kill_buffer();
                return true;
            }
            KeyCode::Char('p') => {
                state.recall_history_previous();
                return true;
            }
            KeyCode::Char('n') => {
                state.recall_history_next();
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
        KeyCode::Char('v') if state.composer.is_empty() && key.modifiers == KeyModifiers::NONE => {
            state.toggle_display_mode();
            true
        }
        KeyCode::Char('q') if state.composer.is_empty() => handle_exit_request(state),
        KeyCode::Char('r') if is_disconnected(state) => {
            reconnect(commands, state, initial_run_id);
            true
        }
        KeyCode::Enter => {
            if !state.consume_line_continuation() {
                return submit_composer(commands, state, runtime, initial_run_id, config_path);
            }
            true
        }
        KeyCode::Tab => submit_composer(commands, state, runtime, initial_run_id, config_path),
        KeyCode::Char('b') if key.modifiers == KeyModifiers::ALT => {
            state.move_composer_word_left();
            true
        }
        KeyCode::Char('f') if key.modifiers == KeyModifiers::ALT => {
            state.move_composer_word_right();
            true
        }
        KeyCode::Backspace => {
            state.delete_composer_before_cursor();
            true
        }
        KeyCode::Delete => {
            state.delete_composer_after_cursor();
            true
        }
        KeyCode::Left => {
            if key.modifiers.contains(KeyModifiers::ALT) {
                state.move_composer_word_left();
            } else {
                state.move_composer_left();
            }
            true
        }
        KeyCode::Right => {
            if key.modifiers.contains(KeyModifiers::ALT) {
                state.move_composer_word_right();
            } else {
                state.move_composer_right();
            }
            true
        }
        KeyCode::Home => {
            state.move_composer_line_start();
            true
        }
        KeyCode::End => {
            state.move_composer_line_end();
            true
        }
        KeyCode::Up => {
            if !state.move_composer_up() {
                state.recall_history_previous();
            }
            true
        }
        KeyCode::Down => {
            if !state.move_composer_down() {
                state.recall_history_next();
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
            state.insert_composer_char(ch);
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
        crate::ConnectionState::Disconnected { .. }
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
            state.move_slash_popup_selection(-1);
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
            state.move_slash_popup_selection(1);
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
            state.complete_selected_slash_command();
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
        } => {
            state.session_picker = None;
            true
        }
        KeyEvent {
            code: KeyCode::Backspace,
            ..
        } => {
            let picker = state.session_picker.as_mut().expect("picker is open");
            if picker.filter.pop().is_some() {
                picker.selected = 0;
            }
            true
        }
        KeyEvent {
            code: KeyCode::Char(character),
            modifiers,
            ..
        } if !character.is_control()
            && !modifiers.intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
        {
            let picker = state.session_picker.as_mut().expect("picker is open");
            picker.filter.push(character);
            picker.selected = 0;
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
        filter: String::new(),
        selected: selected.min(state.sessions.len().saturating_sub(1)),
    });
    state.status_message = Some("session picker opened".into());
}

fn move_session_picker_selection(state: &mut TuiState, delta: isize) {
    let count = state
        .session_picker
        .as_ref()
        .map(|picker| picker.matching_sessions(&state.sessions).len())
        .unwrap_or(0);
    let Some(picker) = state.session_picker.as_mut() else {
        return;
    };
    picker.selected = TuiState::wrapped_selection(picker.selected, count, delta);
}

fn select_picker_session(commands: &Sender<ClientCommand>, state: &mut TuiState) {
    let Some(session) = state
        .session_picker
        .as_ref()
        .and_then(|picker| {
            picker
                .matching_sessions(&state.sessions)
                .into_iter()
                .nth(picker.selected)
        })
        .cloned()
    else {
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

fn dispatch_selected_slash_command(
    commands: &Sender<ClientCommand>,
    state: &mut TuiState,
    initial_run_id: Option<String>,
    runtime: &UiRuntime,
    config_path: Option<String>,
) -> bool {
    let Some(command) = state.selected_slash_command() else {
        return submit_composer(commands, state, runtime, initial_run_id, config_path);
    };
    let message = format!("/{}", command.name);
    state.record_input_history(&message);
    state.clear_composer();
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

fn scroll_history_up(state: &mut TuiState) {
    state.scroll_history_up(SCROLL_PAGE_LINES);
}

fn scroll_history_down(state: &mut TuiState) {
    state.scroll_history_down(SCROLL_PAGE_LINES);
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
    state.record_input_history(&message);
    state.clear_composer();
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
    push_live_event(state, crate::LiveEventLine::user(message.clone()));
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
    state.replace_transcript(TranscriptState::None);
    state.clear_live_events();
    state.stream_warning = None;
    state.reset_all_scroll();
}

fn start_fresh_session(state: &mut TuiState) {
    state.selected_session_id = None;
    state.replace_transcript(TranscriptState::None);
    state.clear_live_events();
    state.stream_warning = None;
    state.session_picker = None;
    state.reset_all_scroll();
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
        crate::LiveEventLine::user(format!("/issue-prep {input}")),
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

pub(super) fn start_next_queued(
    commands: &Sender<ClientCommand>,
    state: &mut TuiState,
    runtime: &mut UiRuntime,
) {
    if runtime_is_busy(runtime, state) || state.queued_messages.is_empty() {
        return;
    }
    let message = state.queued_messages.remove(0);
    push_live_event(state, crate::LiveEventLine::user(message.clone()));
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

pub(super) fn send_command(
    commands: &Sender<ClientCommand>,
    command: ClientCommand,
    state: &mut TuiState,
) {
    if commands.send(command).is_err() {
        state.status_message = Some("daemon client worker stopped".into());
    }
}

fn update_elapsed(state: &mut TuiState, runtime: &UiRuntime) {
    state.active_run_elapsed_secs = runtime
        .active_since
        .map(|started| started.elapsed().as_secs());
}

pub(super) fn push_live_event(state: &mut TuiState, mut line: crate::LiveEventLine) {
    use crate::LiveEventKind;

    state.invalidate_live_event_rows();
    if line.kind == LiveEventKind::Approval
        && line.offset.is_some()
        && let Some(immediate) = state.live_events.iter_mut().rev().find(|event| {
            event.kind == LiveEventKind::Approval
                && event.run_id == line.run_id
                && event.text == line.text
                && event.offset.is_none()
        })
    {
        *immediate = line;
        state.reset_scroll();
        return;
    }
    if line.kind == LiveEventKind::AssistantDelta {
        if let Some(last) = state.live_events.last_mut()
            && last.kind == LiveEventKind::Assistant
            && last.run_id == line.run_id
        {
            last.text.push_str(&line.text);
            last.offset = line.offset;
            state.reset_scroll();
            return;
        }
        line.kind = LiveEventKind::Assistant;
    } else if line.kind == LiveEventKind::Assistant
        && let Some(last) = state.live_events.last_mut()
        && last.kind == LiveEventKind::Assistant
        && last.run_id == line.run_id
    {
        last.text = line.text;
        last.offset = line.offset;
        state.reset_scroll();
        return;
    }
    state.live_events.push(line);
    state.reset_scroll();
}

struct TerminalSession {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl TerminalSession {
    fn enter() -> ClientResult<Self> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen, EnableBracketedPaste)?;
        let terminal = Terminal::new(CrosstermBackend::new(stdout))?;
        Ok(Self { terminal })
    }

    fn draw(&mut self, state: &TuiState) -> ClientResult<()> {
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
    use super::super::client::{
        ACTIVE_POLL_INTERVAL, ClientEvent, ClientOperation, EVENT_LIMIT, apply_events_result,
        apply_loaded_state, apply_run_response, is_connection_error,
    };
    #[cfg(unix)]
    use super::super::client::{DAEMON_CLIENT_TIMEOUT, connect_daemon};
    use super::super::state::DisplayMode;
    use super::*;
    use crate::TranscriptState;
    use plato_daemon_client::ClientError;
    use plato_protocol::{
        BufferedStreamEvent, ERROR_OVERLOAD, ERROR_UNSUPPORTED_VERSION, ERROR_WORKSPACE_MISMATCH,
        EventsStreamResult, HelloResult, IssuePrepResult, IssuePrepStartResult,
        ModelIdentityStatus, ProtocolError, RunStartResult, SessionSummary, TranscriptReadResult,
    };
    use serde_json::json;
    #[cfg(unix)]
    use std::thread;
    use std::{collections::HashMap, sync::mpsc};

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
            ClientError::Io(error)
                if error.kind() == io::ErrorKind::TimedOut
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
    fn empty_composer_v_toggles_local_projection_and_invalidates_rows() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        state.replace_transcript(loaded_transcript(
            "run_1",
            "[turn_1] user: question\n[turn_1] assistant: answer\n",
        ));
        state.live_events = vec![crate::LiveEventLine::status(Some(7), "run finished")];
        state.scroll_history_up(20);
        let transcript = state.transcript.clone();
        let live_events = state.live_events.clone();
        let runtime = UiRuntime::from_state(&state, None);
        render_snapshot(&state, 100, 24).unwrap();
        assert_cached_rows(&state, true, true);

        assert!(press_key(
            KeyEvent::new(KeyCode::Char('v'), KeyModifiers::NONE),
            &mut state,
            &runtime,
            &sender,
        ));

        assert_eq!(state.display_mode, DisplayMode::Audit);
        assert_eq!(state.scroll_offset, 0);
        assert_cached_rows(&state, false, false);
        assert_eq!(state.transcript, transcript);
        assert_eq!(state.live_events, live_events);
        assert!(receiver.try_recv().is_err());

        state.scroll_history_up(10);
        render_snapshot(&state, 100, 24).unwrap();
        assert_cached_rows(&state, true, true);
        assert!(press_key(
            KeyEvent::new(KeyCode::Char('v'), KeyModifiers::NONE),
            &mut state,
            &runtime,
            &sender,
        ));

        assert_eq!(state.display_mode, DisplayMode::Conversation);
        assert_eq!(state.scroll_offset, 20);
        assert_cached_rows(&state, false, false);
        assert_eq!(state.audit_scroll_offset, 10);
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn v_in_nonempty_composer_remains_text_input() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        state.composer = "sa".into();
        state.composer_cursor = state.composer.len();
        let runtime = UiRuntime::from_state(&state, None);

        assert!(press_key(
            KeyEvent::new(KeyCode::Char('v'), KeyModifiers::NONE),
            &mut state,
            &runtime,
            &sender,
        ));

        assert_eq!(state.composer, "sav");
        assert_eq!(state.composer_cursor, 3);
        assert_eq!(state.display_mode, DisplayMode::Conversation);
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
        state.replace_transcript(loaded_transcript("run_1", "[turn_1] user: hello\n"));
        state.live_events = vec![crate::LiveEventLine::assistant(Some(1), "hello")];
        state.stream_warning = Some("lagged".into());
        state.scroll_offset = 10;
        state.composer = "/clear".into();
        state.composer_cursor = state.composer.len();
        let runtime = UiRuntime::from_state(&state, None);
        render_snapshot(&state, 100, 24).unwrap();
        assert_cached_rows(&state, true, true);
        state.transcript = TranscriptState::Unavailable {
            run_id: "run_1".into(),
            error: "boom".into(),
        };

        assert!(submit_composer(&sender, &mut state, &runtime, None, None));

        assert_eq!(state.transcript, TranscriptState::None);
        assert!(state.live_events.is_empty());
        assert_cached_rows(&state, false, false);
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

        state.connection = crate::ConnectionState::Disconnected {
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
            Some(SessionPickerView {
                filter: String::new(),
                selected: 0,
            })
        );
        assert_eq!(
            state.status_message.as_deref(),
            Some("session picker opened")
        );
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn session_picker_enter_loads_focused_filtered_session() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        state.sessions = vec![
            test_session("session_1", "run_1", RunStateName::Finished, "first"),
            test_session("session_2", "run_2", RunStateName::Interrupted, "second"),
        ];
        state.session_picker = Some(SessionPickerView {
            filter: "sec".into(),
            selected: 0,
        });
        let runtime = UiRuntime::from_state(&state, None);

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
    fn session_picker_filter_edit_is_local_and_resets_selection() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        state.sessions = vec![
            test_session("session_1", "run_1", RunStateName::Finished, "question one"),
            test_session("session_2", "run_2", RunStateName::Finished, "question two"),
        ];
        state.session_picker = Some(SessionPickerView {
            filter: String::new(),
            selected: 1,
        });
        let runtime = UiRuntime::from_state(&state, None);

        assert!(press_key(
            KeyEvent::new(KeyCode::Char('q'), KeyModifiers::empty()),
            &mut state,
            &runtime,
            &sender,
        ));
        assert_eq!(
            state.session_picker,
            Some(SessionPickerView {
                filter: "q".into(),
                selected: 0,
            })
        );
        assert!(receiver.try_recv().is_err());

        state.session_picker.as_mut().unwrap().selected = 1;
        assert!(press_key(
            KeyEvent::new(KeyCode::Backspace, KeyModifiers::empty()),
            &mut state,
            &runtime,
            &sender,
        ));
        assert_eq!(
            state.session_picker,
            Some(SessionPickerView {
                filter: String::new(),
                selected: 0,
            })
        );
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn session_picker_navigation_wraps_filtered_results() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        state.sessions = vec![
            test_session("session_1", "run_1", RunStateName::Finished, "alpha"),
            test_session("session_2", "run_2", RunStateName::Finished, "unrelated"),
            test_session("session_3", "run_3", RunStateName::Finished, "ALPINE"),
        ];
        state.session_picker = Some(SessionPickerView {
            filter: "alp".into(),
            selected: 0,
        });
        let runtime = UiRuntime::from_state(&state, None);

        for (key, expected) in [
            (KeyEvent::new(KeyCode::Up, KeyModifiers::empty()), 1),
            (KeyEvent::new(KeyCode::Char('n'), KeyModifiers::CONTROL), 0),
            (KeyEvent::new(KeyCode::Char('p'), KeyModifiers::CONTROL), 1),
            (KeyEvent::new(KeyCode::Down, KeyModifiers::empty()), 0),
        ] {
            assert!(press_key(key, &mut state, &runtime, &sender));
            assert_eq!(
                state.session_picker.as_ref().map(|picker| picker.selected),
                Some(expected)
            );
        }
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn session_picker_no_match_enter_stays_open_and_escape_closes() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        state.sessions = vec![test_session(
            "session_1",
            "run_1",
            RunStateName::Finished,
            "first",
        )];
        state.session_picker = Some(SessionPickerView {
            filter: "missing".into(),
            selected: 0,
        });
        let runtime = UiRuntime::from_state(&state, None);

        assert!(press_key(
            KeyEvent::new(KeyCode::Enter, KeyModifiers::empty()),
            &mut state,
            &runtime,
            &sender,
        ));
        assert!(state.session_picker.is_some());
        assert!(receiver.try_recv().is_err());

        assert!(press_key(
            KeyEvent::new(KeyCode::Esc, KeyModifiers::empty()),
            &mut state,
            &runtime,
            &sender,
        ));
        assert!(state.session_picker.is_none());
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn new_command_clears_selected_session_for_fresh_submit() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        state.selected_session_id = Some("session_1".into());
        state.replace_transcript(loaded_transcript("run_1", "[turn_1] user: old\n"));
        state.live_events = vec![crate::LiveEventLine::assistant(None, "old")];
        state.composer = "/new".into();
        state.composer_cursor = state.composer.len();
        let runtime = UiRuntime::from_state(&state, None);
        render_snapshot(&state, 100, 24).unwrap();
        assert_cached_rows(&state, true, true);

        assert!(submit_composer(&sender, &mut state, &runtime, None, None));

        assert!(state.selected_session_id.is_none());
        assert!(state.live_events.is_empty());
        assert_cached_rows(&state, false, false);
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

        state.handle_paste_text("/c\rnext");

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
        state.connection = crate::ConnectionState::Disconnected {
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
    fn new_live_event_invalidates_only_live_event_rows() {
        let mut state = test_state();
        state.replace_transcript(loaded_transcript(
            "run_1",
            "[turn_1] assistant: cached answer\n",
        ));
        push_live_event(&mut state, crate::LiveEventLine::user("cached question"));
        render_snapshot(&state, 100, 24).unwrap();
        let transcript_rows_ptr = cached_transcript_rows_ptr(&state);
        assert_cached_rows(&state, true, true);

        push_live_event(
            &mut state,
            crate::LiveEventLine::tool(Some(1), "new tool event"),
        );

        assert_eq!(cached_transcript_rows_ptr(&state), transcript_rows_ptr);
        assert_cached_rows(&state, true, false);
        let output = render_snapshot(&state, 100, 24).unwrap();
        assert!(output.contains("cached question"));
        assert!(output.contains("Trace"));
        assert!(output.contains("tools"));
        assert!(!output.contains("new tool event"));
        assert_cached_rows(&state, true, true);
    }

    #[test]
    fn durable_approval_replaces_the_immediate_client_fact() {
        let mut state = test_state();
        push_live_event(
            &mut state,
            crate::LiveEventLine::approval(None, "approval granted call_1").with_run_id("run_1"),
        );

        push_live_event(
            &mut state,
            crate::LiveEventLine::approval(Some(7), "approval granted call_1").with_run_id("run_1"),
        );

        assert_eq!(state.live_events.len(), 1);
        assert_eq!(state.live_events[0].offset, Some(7));
        assert_eq!(state.live_events[0].text, "approval granted call_1");
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
        assert_eq!(state.live_events[0].kind, crate::LiveEventKind::Assistant);
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

        assert_eq!(
            state.active_model,
            Some(ModelIdentityStatus::Requested {
                model: "openrouter/auto".into()
            })
        );
    }

    #[test]
    fn model_responded_events_update_status_to_known_or_unknown_served_identity() {
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

        for (offset, served_model, expected) in [
            (
                0,
                json!("openai/gpt-5.2-2026-08-01"),
                Some("openai/gpt-5.2-2026-08-01".into()),
            ),
            (1, json!(null), None),
        ] {
            apply_events_result(
                &mut state,
                &mut runtime,
                &sender,
                EventsStreamResult {
                    run_id: "run_1".into(),
                    from_offset: offset,
                    next_offset: offset + 1,
                    status: RunStateName::Running,
                    events: vec![ledger_event(
                        offset,
                        json!({
                            "event": "model_responded",
                            "run_id": "run_1",
                            "turn_id": "turn_1",
                            "step": 0,
                            "output": {"role": "assistant", "content": "done"},
                            "proposed_calls": [],
                            "served_model": served_model,
                            "usage": null
                        }),
                    )],
                },
            );

            assert_eq!(
                state.active_model,
                Some(ModelIdentityStatus::Responded {
                    served_model: expected
                })
            );
        }
    }

    #[test]
    fn run_response_selects_returned_session_for_continuation() {
        let mut state = test_state();
        let mut runtime = UiRuntime::from_state(&state, None);
        push_live_event(&mut state, crate::LiveEventLine::user("question"));

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
        assert_eq!(state.live_events.len(), 2);
        assert!(
            state
                .live_events
                .iter()
                .all(|event| event.run_id.as_deref() == Some("run_1"))
        );
    }

    #[test]
    fn assistant_events_from_different_runs_do_not_merge() {
        let mut state = test_state();
        push_live_event(
            &mut state,
            crate::LiveEventLine::assistant(Some(1), "first").with_run_id("run_1"),
        );

        push_live_event(
            &mut state,
            crate::LiveEventLine::assistant_delta(Some(2), "second").with_run_id("run_2"),
        );

        assert_eq!(state.live_events.len(), 2);
        assert_eq!(state.live_events[0].text, "first");
        assert_eq!(state.live_events[1].text, "second");
        assert_eq!(state.live_events[0].run_id.as_deref(), Some("run_1"));
        assert_eq!(state.live_events[1].run_id.as_deref(), Some("run_2"));
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
            event.kind == crate::LiveEventKind::Assistant && event.text == "# Prepared issue"
        }));
        assert!(state.live_events.iter().any(|event| {
            event.kind == crate::LiveEventKind::Status
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
                operation: ClientOperation::IssuePrepStart,
                error: ClientError::DaemonResponse(ProtocolError {
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
            event.kind == crate::LiveEventKind::Warning && event.text.contains("provider failed")
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
            event.kind == crate::LiveEventKind::Warning
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
        state.active_run = Some(crate::ActiveRunView {
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
                operation: ClientOperation::EventsStream,
                error: ClientError::DaemonResponse(ProtocolError {
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
        state.active_run = Some(crate::ActiveRunView {
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
                operation: ClientOperation::EventsStream,
                error: ClientError::Io(std::io::Error::new(
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
        assert!(is_connection_error(&ClientError::DaemonProtocol(
            "response id mismatch".into()
        )));
        assert!(is_connection_error(&ClientError::Io(
            std::io::Error::other("socket failed")
        )));
        assert!(is_connection_error(&ClientError::DaemonResponse(
            ProtocolError {
                code: ERROR_UNSUPPORTED_VERSION.into(),
                message: "unsupported".into(),
            }
        )));
        assert!(is_connection_error(&ClientError::DaemonResponse(
            ProtocolError {
                code: ERROR_WORKSPACE_MISMATCH.into(),
                message: "wrong workspace".into(),
            }
        )));
        assert!(!is_connection_error(&ClientError::DaemonResponse(
            ProtocolError {
                code: ERROR_OVERLOAD.into(),
                message: "busy".into(),
            }
        )));
        assert!(!is_connection_error(&ClientError::Config(
            "missing runtime".into()
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
        state.approval = Some(crate::ApprovalModalView {
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

        assert_eq!(
            state
                .approval
                .as_ref()
                .map(|approval| approval.tool_call_id.as_str()),
            Some("call_1")
        );
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

        state.approval = Some(crate::ApprovalModalView {
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

        assert_eq!(
            state
                .approval
                .as_ref()
                .map(|approval| approval.tool_call_id.as_str()),
            Some("call_2")
        );
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
    fn failed_grant_keeps_same_approval_retryable_until_success() {
        assert_failed_approval_retry(true);
    }

    #[test]
    fn failed_deny_keeps_same_approval_retryable_until_success() {
        assert_failed_approval_retry(false);
    }

    fn assert_failed_approval_retry(grant: bool) {
        let (command_sender, command_receiver) = mpsc::channel();
        let (event_sender, event_receiver) = mpsc::channel();
        let mut state = test_state();
        let approval = crate::ApprovalModalView {
            run_id: "run_retry".into(),
            tool_call_id: "call_retry".into(),
            tool_name: "file.write".into(),
            effect: "workspace_write".into(),
            reason: "requires approval".into(),
            input_preview: r#"{"path":"retry.txt"}"#.into(),
            approval_preview: None,
            diff_preview: None,
        };
        state.approval = Some(approval.clone());
        state.active_run = Some(crate::ActiveRunView {
            run_id: approval.run_id.clone(),
            status: RunStateName::Running,
        });
        let mut runtime = UiRuntime::from_state(&state, None);
        let assert_command = |command| match (grant, command) {
            (
                true,
                ClientCommand::ApprovalGrant {
                    run_id,
                    tool_call_id,
                },
            ) => {
                assert_eq!(run_id, "run_retry");
                assert_eq!(tool_call_id, "call_retry");
            }
            (
                false,
                ClientCommand::ApprovalDeny {
                    run_id,
                    tool_call_id,
                    reason,
                },
            ) => {
                assert_eq!(run_id, "run_retry");
                assert_eq!(tool_call_id, "call_retry");
                assert_eq!(reason, "denied by plato-tui");
            }
            (_, other) => panic!("unexpected approval command: {other:?}"),
        };

        decide_approval(
            &command_sender,
            &mut state,
            if grant {
                ApprovalAction::Grant
            } else {
                ApprovalAction::Deny
            },
        );
        assert_command(command_receiver.try_recv().unwrap());

        event_sender
            .send(ClientEvent::Failed {
                operation: ClientOperation::ApprovalDecide,
                error: ClientError::DaemonResponse(ProtocolError {
                    code: "temporarily_unavailable".into(),
                    message: "try the same decision again".into(),
                }),
            })
            .unwrap();
        drain_client_events(&mut state, &mut runtime, &event_receiver, &command_sender);

        assert_eq!(state.approval.as_ref(), Some(&approval));
        let failed = render_snapshot(&state, 100, 24).unwrap();
        assert!(failed.contains("call_retry"));
        assert!(failed.contains("g grant"));
        assert!(failed.contains("d deny"));

        decide_approval(
            &command_sender,
            &mut state,
            if grant {
                ApprovalAction::Grant
            } else {
                ApprovalAction::Deny
            },
        );
        assert_command(command_receiver.try_recv().unwrap());
        assert!(command_receiver.try_recv().is_err());
        assert_eq!(state.approval.as_ref(), Some(&approval));

        event_sender
            .send(ClientEvent::ApprovalDecided {
                result: plato_protocol::CommandAcceptedResult {
                    run_id: "run_retry".into(),
                    status: RunStateName::Running,
                },
                tool_call_id: approval.tool_call_id.clone(),
                decision: if grant {
                    plato_protocol::ApprovalDecisionName::Granted
                } else {
                    plato_protocol::ApprovalDecisionName::Denied
                },
            })
            .unwrap();
        drain_client_events(&mut state, &mut runtime, &event_receiver, &command_sender);
        assert!(state.approval.is_none());
        assert!(state.live_events.iter().any(|event| {
            event.kind == crate::LiveEventKind::Approval
                && event.run_id.as_deref() == Some("run_retry")
                && event.text
                    == format!(
                        "approval {} call_retry",
                        if grant { "granted" } else { "denied" }
                    )
        }));
    }

    #[test]
    fn first_cancel_requests_daemon_and_second_cancel_quits() {
        let (sender, receiver) = mpsc::channel();
        let mut state = test_state();
        state.active_run = Some(crate::ActiveRunView {
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

    #[test]
    fn matching_selected_run_reload_preserves_live_state_and_cache() {
        let mut state = test_state();
        state.sessions = vec![test_session(
            "session_1",
            "run_1",
            RunStateName::Running,
            "matching run",
        )];
        state.selected_session_id = Some("session_1".into());
        state.active_run = Some(crate::ActiveRunView {
            run_id: "run_1".into(),
            status: RunStateName::Running,
        });
        state.replace_transcript(loaded_transcript(
            "run_1",
            "[turn_1] assistant: old answer\n",
        ));
        push_live_event(
            &mut state,
            crate::LiveEventLine::status(Some(1), "live status"),
        );
        state.stream_warning = Some("matching warning".into());
        state.active_model = Some(ModelIdentityStatus::Requested {
            model: "matching-model".into(),
        });
        state.active_run_elapsed_secs = Some(17);
        state.toggle_display_mode();
        state.scroll_history_up(10);
        state.cancel_requested = true;
        state.approval = Some(test_approval("run_1", "call_1"));
        render_snapshot(&state, 100, 24).unwrap();
        let live_event_rows_ptr = cached_live_event_rows_ptr(&state);

        let mut loaded = test_state();
        loaded.sessions = state.sessions.clone();
        loaded.selected_session_id = Some("session_1".into());
        loaded.active_run = Some(crate::ActiveRunView {
            run_id: "run_1".into(),
            status: RunStateName::Running,
        });
        loaded.replace_transcript(loaded_transcript(
            "run_1",
            "[turn_2] assistant: refreshed answer\n",
        ));
        apply_loaded_state(&mut state, loaded);

        assert_cached_rows(&state, false, true);
        assert_eq!(cached_live_event_rows_ptr(&state), live_event_rows_ptr);
        assert_eq!(state.stream_warning.as_deref(), Some("matching warning"));
        assert_eq!(
            state.active_model,
            Some(ModelIdentityStatus::Requested {
                model: "matching-model".into()
            })
        );
        assert_eq!(state.active_run_elapsed_secs, Some(17));
        assert_eq!(state.display_mode, DisplayMode::Audit);
        assert_eq!(state.scroll_offset, 10);
        assert_eq!(state.audit_scroll_offset, 10);
        assert!(state.cancel_requested);
        assert_eq!(
            state
                .approval
                .as_ref()
                .map(|approval| approval.tool_call_id.as_str()),
            Some("call_1")
        );
        assert_eq!(
            state.live_events.first().map(|event| event.text.as_str()),
            Some("live status")
        );
        state.approval = None;
        let output = render_snapshot(&state, 100, 24).unwrap();
        assert!(output.contains("refreshed answer"));
        assert!(!output.contains("old answer"));
        assert!(output.contains("live status"));
    }

    #[test]
    fn repeated_session_switches_clear_transcript_live_and_approval_state() {
        let mut state = selected_state("session_a", "run_a", "[turn_a] assistant: transcript-a\n");
        state.toggle_display_mode();

        for (next_session, next_run, next_transcript) in [
            ("session_b", "run_b", "[turn_b] assistant: transcript-b\n"),
            ("session_a", "run_a", "[turn_a] assistant: transcript-a\n"),
            ("session_b", "run_b", "[turn_b] assistant: transcript-b\n"),
        ] {
            let previous_session = state.selected_session_id.clone().unwrap();
            let previous_run = state.active_run.as_ref().unwrap().run_id.clone();
            let old_marker = format!("old-live-{previous_session}");
            state.live_events = vec![crate::LiveEventLine::assistant(Some(7), old_marker.clone())];
            state.stream_warning = Some(format!("old-warning-{previous_session}"));
            state.active_model = Some(ModelIdentityStatus::Requested {
                model: format!("old-model-{previous_session}"),
            });
            state.active_run_elapsed_secs = Some(91);
            state.approval = Some(test_approval(&previous_run, "old-call"));
            state.scroll_history_up(10);
            render_snapshot(&state, 100, 24).unwrap();
            assert_cached_rows(&state, true, true);

            apply_loaded_state(
                &mut state,
                selected_state(next_session, next_run, next_transcript),
            );

            assert!(state.live_events.is_empty());
            assert!(state.stream_warning.is_none());
            assert!(state.active_model.is_none());
            assert!(state.active_run_elapsed_secs.is_none());
            assert!(state.approval.is_none());
            assert_eq!(state.display_mode, DisplayMode::Audit);
            assert_eq!(state.scroll_offset, 0);
            assert_eq!(state.conversation_scroll_offset, 0);
            assert_eq!(state.audit_scroll_offset, 0);
            assert_cached_rows(&state, false, false);
            let output = render_snapshot(&state, 100, 24).unwrap();
            assert!(output.contains(next_transcript.split(": ").last().unwrap().trim()));
            assert!(!output.contains(&old_marker));
            assert!(!output.contains(&format!("old-warning-{previous_session}")));
            assert!(!output.contains(&format!("old-model-{previous_session}")));
            assert!(!output.contains("old-call"));
        }
    }

    #[test]
    fn reload_without_selected_identity_clears_live_state() {
        let mut state = test_state();
        state.live_events = vec![crate::LiveEventLine::status(None, "unowned live state")];
        state.stream_warning = Some("unowned warning".into());
        state.active_model = Some(ModelIdentityStatus::Requested {
            model: "unowned-model".into(),
        });
        state.active_run_elapsed_secs = Some(12);
        state.approval = Some(test_approval("unowned-run", "unowned-call"));
        render_snapshot(&state, 100, 24).unwrap();

        apply_loaded_state(&mut state, test_state());

        assert!(state.live_events.is_empty());
        assert!(state.stream_warning.is_none());
        assert!(state.active_model.is_none());
        assert!(state.active_run_elapsed_secs.is_none());
        assert!(state.approval.is_none());
        assert_cached_rows(&state, false, false);
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

    fn selected_state(session_id: &str, run_id: &str, transcript: &str) -> TuiState {
        let mut state = test_state();
        state.sessions = vec![test_session(
            session_id,
            run_id,
            RunStateName::Running,
            transcript,
        )];
        state.selected_session_id = Some(session_id.into());
        state.active_run = Some(crate::ActiveRunView {
            run_id: run_id.into(),
            status: RunStateName::Running,
        });
        state.replace_transcript(loaded_transcript(run_id, transcript));
        state
    }

    fn test_approval(run_id: &str, tool_call_id: &str) -> crate::ApprovalModalView {
        crate::ApprovalModalView {
            run_id: run_id.into(),
            tool_call_id: tool_call_id.into(),
            tool_name: "file.write".into(),
            effect: "workspace_write".into(),
            reason: "requires approval".into(),
            input_preview: "{}".into(),
            approval_preview: None,
            diff_preview: None,
        }
    }

    fn assert_cached_rows(state: &TuiState, transcript: bool, live_events: bool) {
        assert_eq!(
            state.history_rows.transcript.read().unwrap().is_some(),
            transcript
        );
        assert_eq!(
            state.history_rows.live_events.read().unwrap().is_some(),
            live_events
        );
    }

    fn cached_transcript_rows_ptr(state: &TuiState) -> *const ratatui::text::Line<'static> {
        state
            .history_rows
            .transcript
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .1
            .as_ptr()
    }

    fn cached_live_event_rows_ptr(state: &TuiState) -> *const ratatui::text::Line<'static> {
        state
            .history_rows
            .live_events
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .2
            .as_ptr()
    }

    fn loaded_transcript(run_id: &str, transcript: &str) -> TranscriptState {
        TranscriptState::Loaded(
            TranscriptReadResult {
                run_id: run_id.into(),
                status: RunStateName::Finished,
                final_answer: None,
                transcript: transcript.into(),
                typed: None,
                pending_approval: None,
            }
            .into(),
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
            first_question: latest_question.into(),
            updated_at_ms: 1,
            ledger_path: "/tmp/agent.db".into(),
        }
    }
}
