use crate::daemon::protocol::{HelloResult, RunStateName, SessionSummary, TranscriptReadResult};
use std::time::Instant;

use super::ApprovalModalView;

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
        state.transcript = transcript;
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
}

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
