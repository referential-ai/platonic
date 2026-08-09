use nucleo::{
    Config, Matcher, Utf32Str,
    pattern::{Atom, AtomKind, CaseMatching, Normalization},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SlashCommandAction {
    Help,
    Status,
    Yolo,
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
        name: "status",
        description: "show authoritative runtime status",
        action: SlashCommandAction::Status,
    },
    SlashCommandSpec {
        name: "yolo",
        description: "set session yolo on or off",
        action: SlashCommandAction::Yolo,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum KeyAction {
    Submit,
    Newline,
    Queue,
    ToggleView,
    Scroll,
    InputHistory,
    Interrupt,
    Quit,
    Reconnect,
    Shortcuts,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum KeyLabel {
    Literal(&'static str),
    Alt(&'static str),
}

impl KeyLabel {
    pub(crate) fn text(self, platform: KeyLabelPlatform) -> String {
        match (self, platform) {
            (Self::Literal(label), _) => label.into(),
            (Self::Alt(key), KeyLabelPlatform::MacOs) => format!("⌥ {key}"),
            (Self::Alt(key), KeyLabelPlatform::Other) => format!("alt + {key}"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum KeyLabelPlatform {
    MacOs,
    Other,
}

impl KeyLabelPlatform {
    pub(crate) fn current() -> Self {
        if cfg!(target_os = "macos") {
            Self::MacOs
        } else {
            Self::Other
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FooterHintPriority {
    Essential,
    Queue,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FooterHintWhen {
    Always,
    ActiveRun,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FooterHint {
    pub(crate) priority: FooterHintPriority,
    pub(crate) when: FooterHintWhen,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct KeyBinding {
    pub(crate) action: KeyAction,
    pub(crate) label: KeyLabel,
    pub(crate) description: &'static str,
    pub(crate) footer: Option<FooterHint>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct KeyMap<'a> {
    bindings: &'a [KeyBinding],
}

impl<'a> KeyMap<'a> {
    pub(crate) const fn new(bindings: &'a [KeyBinding]) -> Self {
        Self { bindings }
    }

    pub(crate) fn bindings(self) -> &'a [KeyBinding] {
        self.bindings
    }

    pub(crate) fn binding(self, action: KeyAction) -> &'a KeyBinding {
        self.bindings
            .iter()
            .find(|binding| binding.action == action)
            .expect("key map is missing a required action")
    }
}

const KEY_BINDINGS: &[KeyBinding] = &[
    KeyBinding {
        action: KeyAction::Submit,
        label: KeyLabel::Literal("Enter"),
        description: "submit",
        footer: None,
    },
    KeyBinding {
        action: KeyAction::Newline,
        label: KeyLabel::Literal("Shift+Enter"),
        description: "newline",
        footer: None,
    },
    KeyBinding {
        action: KeyAction::Newline,
        label: KeyLabel::Alt("enter"),
        description: "newline",
        footer: None,
    },
    KeyBinding {
        action: KeyAction::Newline,
        label: KeyLabel::Literal("Ctrl+J/M"),
        description: "newline",
        footer: None,
    },
    KeyBinding {
        action: KeyAction::Queue,
        label: KeyLabel::Literal("Tab"),
        description: "complete command or submit / queue",
        footer: Some(FooterHint {
            priority: FooterHintPriority::Queue,
            when: FooterHintWhen::Always,
        }),
    },
    KeyBinding {
        action: KeyAction::ToggleView,
        label: KeyLabel::Literal("v"),
        description: "toggle conversation / audit",
        footer: None,
    },
    KeyBinding {
        action: KeyAction::Scroll,
        label: KeyLabel::Literal("PgUp/PgDown"),
        description: "scroll",
        footer: None,
    },
    KeyBinding {
        action: KeyAction::InputHistory,
        label: KeyLabel::Literal("Up/Down"),
        description: "input history",
        footer: None,
    },
    KeyBinding {
        action: KeyAction::Interrupt,
        label: KeyLabel::Literal("Esc"),
        description: "interrupt active run / close overlay",
        footer: Some(FooterHint {
            priority: FooterHintPriority::Queue,
            when: FooterHintWhen::ActiveRun,
        }),
    },
    KeyBinding {
        action: KeyAction::Quit,
        label: KeyLabel::Literal("Ctrl+C"),
        description: "cancel active run; press again to quit",
        footer: None,
    },
    KeyBinding {
        action: KeyAction::Quit,
        label: KeyLabel::Literal("q"),
        description: "quit when composer is empty / close overlay",
        footer: None,
    },
    KeyBinding {
        action: KeyAction::Reconnect,
        label: KeyLabel::Literal("r"),
        description: "reconnect when offline",
        footer: None,
    },
    KeyBinding {
        action: KeyAction::Shortcuts,
        label: KeyLabel::Literal("?"),
        description: "shortcuts",
        footer: Some(FooterHint {
            priority: FooterHintPriority::Essential,
            when: FooterHintWhen::Always,
        }),
    },
];

pub(crate) const KEY_MAP: KeyMap<'static> = KeyMap::new(KEY_BINDINGS);

pub(crate) fn find_slash_command(name: &str) -> Option<&'static SlashCommandSpec> {
    SLASH_COMMANDS
        .iter()
        .find(|command| command.name.eq_ignore_ascii_case(name))
}

pub(crate) fn matching_slash_commands(filter: &str) -> Vec<&'static SlashCommandSpec> {
    let filter = filter.trim();
    if filter.is_empty() {
        return SLASH_COMMANDS.iter().collect();
    }

    let fuzzy = Atom::new(
        filter,
        CaseMatching::Ignore,
        Normalization::Never,
        AtomKind::Fuzzy,
        false,
    );
    let prefix = Atom::new(
        filter,
        CaseMatching::Ignore,
        Normalization::Never,
        AtomKind::Prefix,
        false,
    );
    let mut config = Config::DEFAULT;
    config.prefer_prefix = true;
    let mut matcher = Matcher::new(config);
    let mut chars = Vec::new();
    let mut matches: Vec<_> = SLASH_COMMANDS
        .iter()
        .enumerate()
        .filter_map(|(source_index, command)| {
            let score = fuzzy.score(Utf32Str::new(command.name, &mut chars), &mut matcher)?;
            let is_prefix = prefix
                .score(Utf32Str::new(command.name, &mut chars), &mut matcher)
                .is_some();
            Some((source_index, is_prefix, score, command))
        })
        .collect();
    matches.sort_by(|left, right| {
        right
            .1
            .cmp(&left.1)
            .then_with(|| right.2.cmp(&left.2))
            .then_with(|| left.0.cmp(&right.0))
    });
    matches
        .into_iter()
        .map(|(_, _, _, command)| command)
        .collect()
}

pub(crate) fn has_slash_command_match(filter: &str) -> bool {
    !filter.contains('/') && matching_slash_commands(filter).into_iter().next().is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alt_key_labels_are_platform_aware_without_platform_hardware() {
        let label = KeyLabel::Alt("enter");

        assert_eq!(label.text(KeyLabelPlatform::MacOs), "⌥ enter");
        assert_eq!(label.text(KeyLabelPlatform::Other), "alt + enter");
    }

    #[test]
    fn slash_commands_match_case_insensitive_subsequences_with_prefix_strength() {
        let names = |filter| {
            matching_slash_commands(filter)
                .into_iter()
                .map(|command| command.name)
                .collect::<Vec<_>>()
        };

        assert!(has_slash_command_match("SP"));
        assert!(names("SP").contains(&"issue-prep"));

        let matches = names("s");
        let subsequence = matches
            .iter()
            .position(|name| *name == "issue-prep")
            .unwrap();
        for prefix in ["sessions", "status"] {
            assert!(matches.iter().position(|name| *name == prefix).unwrap() <= subsequence);
        }

        for _ in 0..32 {
            assert_eq!(names("s"), matches);
        }
    }
}
