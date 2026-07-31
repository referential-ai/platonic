#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SlashCommandAction {
    Help,
    Clear,
    Sessions,
    NewSession,
    IssuePrep,
    Reconnect,
    Quit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SlashCommandSpec {
    pub(crate) name: &'static str,
    pub(crate) description: &'static str,
    pub(crate) action: SlashCommandAction,
}

pub(crate) const SLASH_COMMANDS: &[SlashCommandSpec] = &[
    SlashCommandSpec {
        name: "help",
        description: "show this help",
        action: SlashCommandAction::Help,
    },
    SlashCommandSpec {
        name: "clear",
        description: "clear the visible transcript",
        action: SlashCommandAction::Clear,
    },
    SlashCommandSpec {
        name: "sessions",
        description: "open the session picker",
        action: SlashCommandAction::Sessions,
    },
    SlashCommandSpec {
        name: "new",
        description: "start a fresh session",
        action: SlashCommandAction::NewSession,
    },
    SlashCommandSpec {
        name: "issue-prep",
        description: "prepare and review an issue",
        action: SlashCommandAction::IssuePrep,
    },
    SlashCommandSpec {
        name: "reconnect",
        description: "reconnect when offline",
        action: SlashCommandAction::Reconnect,
    },
    SlashCommandSpec {
        name: "quit",
        description: "close the TUI",
        action: SlashCommandAction::Quit,
    },
    SlashCommandSpec {
        name: "exit",
        description: "close the TUI",
        action: SlashCommandAction::Quit,
    },
];

pub(crate) fn find_slash_command(name: &str) -> Option<&'static SlashCommandSpec> {
    SLASH_COMMANDS
        .iter()
        .find(|command| command.name.eq_ignore_ascii_case(name))
}

pub(crate) fn matching_slash_commands(filter: &str) -> Vec<&'static SlashCommandSpec> {
    let filter = filter.trim().to_ascii_lowercase();
    SLASH_COMMANDS
        .iter()
        .filter(|command| filter.is_empty() || command.name.starts_with(&filter))
        .collect()
}

pub(crate) fn has_slash_command_prefix(filter: &str) -> bool {
    !filter.contains('/') && matching_slash_commands(filter).into_iter().next().is_some()
}
