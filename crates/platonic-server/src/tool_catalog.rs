use platonic_core::EffectClass;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const FILE_READ: &str = "file.read";
pub const FILE_LIST: &str = "file.list";
pub const FILE_WRITE: &str = "file.write";
pub const FILE_EDIT: &str = "file.edit";
pub const SHELL_EXEC: &str = "shell.exec";
pub const WEB_FETCH: &str = "web.fetch";
pub const PROFILE_READ: &str = "profile.read";
pub const THREAD_TREE_READ: &str = "thread.tree";
pub const THREAD_EVENTS_READ: &str = "thread.events";
pub const THREAD_TRANSCRIPT_READ: &str = "thread.transcript";
pub const THREAD_SPAWN: &str = "thread.spawn";
pub const THREAD_RETURN: &str = "thread.return";
pub const THREAD_ANSWER: &str = "thread.answer";
pub const COMPUTER_WINDOWS: &str = "computer.windows";
pub const COMPUTER_OBSERVE: &str = "computer.observe";

const PROVIDER_FILE_READ: &str = "file_read";
const PROVIDER_FILE_LIST: &str = "file_list";
const PROVIDER_FILE_WRITE: &str = "file_write";
const PROVIDER_FILE_EDIT: &str = "file_edit";
const PROVIDER_SHELL_EXEC: &str = "shell_exec";
const PROVIDER_WEB_FETCH: &str = "web_fetch";
const PROVIDER_PROFILE_READ: &str = "profile_read";
const PROVIDER_THREAD_TREE_READ: &str = "thread_tree";
const PROVIDER_THREAD_EVENTS_READ: &str = "thread_events";
const PROVIDER_THREAD_TRANSCRIPT_READ: &str = "thread_transcript";
const PROVIDER_THREAD_SPAWN: &str = "thread_spawn";
const PROVIDER_THREAD_RETURN: &str = "thread_return";
const PROVIDER_THREAD_ANSWER: &str = "thread_answer";
const PROVIDER_COMPUTER_WINDOWS: &str = "computer_windows";
const PROVIDER_COMPUTER_OBSERVE: &str = "computer_observe";

const BOOTSTRAP_TOOLS: &[ToolDefinition] = &[
    ToolDefinition {
        internal_name: FILE_READ,
        provider_name: PROVIDER_FILE_READ,
        effect: EffectClass::ReadOnly,
        description: "Read a UTF-8 text file inside the current workspace.",
        input_schema: ToolInputSchema::Read,
    },
    ToolDefinition {
        internal_name: FILE_LIST,
        provider_name: PROVIDER_FILE_LIST,
        effect: EffectClass::ReadOnly,
        description: "List direct entries in one directory inside the current workspace.",
        input_schema: ToolInputSchema::List,
    },
    ToolDefinition {
        internal_name: FILE_WRITE,
        provider_name: PROVIDER_FILE_WRITE,
        effect: EffectClass::WorkspaceWrite,
        description: "Write UTF-8 text to a relative path inside the current workspace after approval.",
        input_schema: ToolInputSchema::Write,
    },
    ToolDefinition {
        internal_name: FILE_EDIT,
        provider_name: PROVIDER_FILE_EDIT,
        effect: EffectClass::WorkspaceWrite,
        description: "Replace a workspace file with full proposed UTF-8 content after approval.",
        input_schema: ToolInputSchema::Write,
    },
    ToolDefinition {
        internal_name: SHELL_EXEC,
        provider_name: PROVIDER_SHELL_EXEC,
        effect: EffectClass::ExternalSideEffect,
        description: "Run one approved shell command from the workspace root with a scrubbed environment.",
        input_schema: ToolInputSchema::ShellExec,
    },
    ToolDefinition {
        internal_name: WEB_FETCH,
        provider_name: PROVIDER_WEB_FETCH,
        effect: EffectClass::Network,
        description: "Fetch bounded UTF-8 text from one approved public HTTP(S) URL.",
        input_schema: ToolInputSchema::WebFetch,
    },
    ToolDefinition {
        internal_name: PROFILE_READ,
        provider_name: PROVIDER_PROFILE_READ,
        effect: EffectClass::ReadOnly,
        description: "Read one own-profile content revision and bounded revision history.",
        input_schema: ToolInputSchema::ProfileRead,
    },
    ToolDefinition {
        internal_name: THREAD_TREE_READ,
        provider_name: PROVIDER_THREAD_TREE_READ,
        effect: EffectClass::ReadOnly,
        description: "List bounded metadata for threads in the current profile tree.",
        input_schema: ToolInputSchema::ThreadTree,
    },
    ToolDefinition {
        internal_name: THREAD_EVENTS_READ,
        provider_name: PROVIDER_THREAD_EVENTS_READ,
        effect: EffectClass::ReadOnly,
        description: "Read bounded committed events from one current-profile thread.",
        input_schema: ToolInputSchema::ThreadHistory,
    },
    ToolDefinition {
        internal_name: THREAD_TRANSCRIPT_READ,
        provider_name: PROVIDER_THREAD_TRANSCRIPT_READ,
        effect: EffectClass::ReadOnly,
        description: "Read a bounded committed transcript from one current-profile thread.",
        input_schema: ToolInputSchema::ThreadHistory,
    },
    ToolDefinition {
        internal_name: THREAD_SPAWN,
        provider_name: PROVIDER_THREAD_SPAWN,
        effect: EffectClass::WorkspaceWrite,
        description: "Spawn one bounded same-profile child thread after approval.",
        input_schema: ToolInputSchema::ThreadSpawn,
    },
    ToolDefinition {
        internal_name: THREAD_RETURN,
        provider_name: PROVIDER_THREAD_RETURN,
        effect: EffectClass::WorkspaceWrite,
        description: "Return typed progress or a question to this thread's immediate parent.",
        input_schema: ToolInputSchema::ThreadReturn,
    },
    ToolDefinition {
        internal_name: THREAD_ANSWER,
        provider_name: PROVIDER_THREAD_ANSWER,
        effect: EffectClass::WorkspaceWrite,
        description: "Send an attributed answer or follow-up to one immediate child thread.",
        input_schema: ToolInputSchema::ThreadAnswer,
    },
    ToolDefinition {
        internal_name: COMPUTER_WINDOWS,
        provider_name: PROVIDER_COMPUTER_WINDOWS,
        effect: EffectClass::SecretAccess,
        description: "List visible Linux desktop windows as run-local opaque references.",
        input_schema: ToolInputSchema::ComputerWindows,
    },
    ToolDefinition {
        internal_name: COMPUTER_OBSERVE,
        provider_name: PROVIDER_COMPUTER_OBSERVE,
        effect: EffectClass::SecretAccess,
        description: "Read bounded semantic accessibility data for one approved window reference.",
        input_schema: ToolInputSchema::ComputerObserve,
    },
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolDefinition {
    pub internal_name: &'static str,
    pub provider_name: &'static str,
    pub effect: EffectClass,
    pub description: &'static str,
    input_schema: ToolInputSchema,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ToolInputSchema {
    Read,
    List,
    Write,
    ShellExec,
    WebFetch,
    ProfileRead,
    ThreadTree,
    ThreadHistory,
    ThreadSpawn,
    ThreadReturn,
    ThreadAnswer,
    ComputerWindows,
    ComputerObserve,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

pub fn bootstrap_tools() -> &'static [ToolDefinition] {
    BOOTSTRAP_TOOLS
}

pub fn default_enabled_tools() -> Vec<String> {
    BOOTSTRAP_TOOLS
        .iter()
        .filter(|tool| {
            !matches!(
                tool.internal_name,
                PROFILE_READ
                    | THREAD_TREE_READ
                    | THREAD_EVENTS_READ
                    | THREAD_TRANSCRIPT_READ
                    | THREAD_SPAWN
                    | THREAD_RETURN
                    | THREAD_ANSWER
                    | COMPUTER_WINDOWS
                    | COMPUTER_OBSERVE
            )
        })
        .map(|tool| tool.internal_name.into())
        .collect()
}

pub fn is_known_tool(name: &str) -> bool {
    lookup_internal(name).is_some()
}

pub fn effect_for_tool(name: &str) -> EffectClass {
    lookup_internal(name)
        .map(|tool| tool.effect.clone())
        .unwrap_or(EffectClass::ExternalSideEffect)
}

pub fn provider_name_for_internal(name: &str) -> Option<&'static str> {
    lookup_internal(name).map(|tool| tool.provider_name)
}

pub fn internal_name_for_provider(name: &str) -> Option<&'static str> {
    BOOTSTRAP_TOOLS
        .iter()
        .find(|tool| tool.provider_name == name || tool.internal_name == name)
        .map(|tool| tool.internal_name)
}

pub(crate) fn is_logical_read_tool(name: &str) -> bool {
    matches!(
        name,
        PROFILE_READ | THREAD_TREE_READ | THREAD_EVENTS_READ | THREAD_TRANSCRIPT_READ
    )
}

pub fn tool_specs(enabled_tools: &[String]) -> Vec<ToolSpec> {
    enabled_tools
        .iter()
        .filter_map(|name| lookup_internal(name))
        .map(ToolSpec::from_definition)
        .collect()
}

fn lookup_internal(name: &str) -> Option<&'static ToolDefinition> {
    BOOTSTRAP_TOOLS
        .iter()
        .find(|tool| tool.internal_name == name)
}

impl ToolSpec {
    fn from_definition(definition: &ToolDefinition) -> Self {
        Self {
            name: definition.provider_name.into(),
            description: definition.description.into(),
            input_schema: definition.input_schema.to_json(),
        }
    }
}

impl ToolInputSchema {
    fn to_json(self) -> Value {
        match self {
            Self::Read => json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Relative path inside the current workspace."
                    }
                },
                "required": ["path"],
                "additionalProperties": false
            }),
            Self::List => json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Directory path inside the current workspace."
                    }
                },
                "required": ["path"],
                "additionalProperties": false
            }),
            Self::Write => json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Relative path inside the current workspace."
                    },
                    "content": {
                        "type": "string",
                        "description": "UTF-8 content to write."
                    }
                },
                "required": ["path", "content"],
                "additionalProperties": false
            }),
            Self::ShellExec => json!({
                "type": "object",
                "properties": {
                    "command": {
                        "type": "string",
                        "description": "Shell command to run from the workspace root."
                    },
                    "timeout_seconds": {
                        "type": "integer",
                        "description": "Optional timeout in seconds. Defaults to 120 and is capped at 600.",
                        "minimum": 1,
                        "maximum": 600
                    },
                    "credential": {
                        "type": "string",
                        "description": "Optional operator-configured file credential. It is available only for this approved call at $TMPDIR/credentials/<credential>.",
                        "pattern": "^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$",
                        "maxLength": 64
                    }
                },
                "required": ["command"],
                "additionalProperties": false
            }),
            Self::WebFetch => json!({
                "type": "object",
                "properties": {
                    "url": {
                        "type": "string",
                        "description": "Absolute public HTTP(S) URL to fetch.",
                        "maxLength": 2048
                    }
                },
                "required": ["url"],
                "additionalProperties": false
            }),
            Self::ProfileRead => json!({
                "type": "object",
                "properties": {
                    "profile_id": {
                        "type": "string",
                        "description": "Optional target profile; only the current profile is permitted."
                    },
                    "revision": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "Exact revision to read; defaults to current."
                    },
                    "cursor": {
                        "type": "string",
                        "description": "Revision-history cursor returned by the previous page."
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 100,
                        "default": 50
                    }
                },
                "additionalProperties": false
            }),
            Self::ThreadTree => json!({
                "type": "object",
                "properties": {
                    "profile_id": {
                        "type": "string",
                        "description": "Optional target profile; only the current profile is permitted."
                    },
                    "cursor": {
                        "type": "string",
                        "description": "Thread cursor returned by the previous page."
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 100,
                        "default": 50
                    }
                },
                "additionalProperties": false
            }),
            Self::ThreadHistory => json!({
                "type": "object",
                "properties": {
                    "thread_id": {
                        "type": "string",
                        "description": "Target thread; defaults to the current thread."
                    },
                    "run_id": {
                        "type": "string",
                        "description": "Optional exact run within the target thread."
                    },
                    "cursor": {
                        "type": "string",
                        "description": "Committed-history cursor returned by the previous page."
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 256,
                        "default": 50
                    }
                },
                "additionalProperties": false
            }),
            Self::ThreadSpawn => json!({
                "type": "object",
                "properties": {
                    "cwd": {
                        "type": "string",
                        "description": "Absolute worker directory within the coordinator's granted paths."
                    },
                    "model": {
                        "type": "string",
                        "description": "Optional model override."
                    },
                    "reasoning_effort": {
                        "type": "string",
                        "enum": ["none", "minimal", "low", "medium", "high", "xhigh", "max"],
                        "description": "Optional reasoning-effort override."
                    },
                    "approval_policy": {
                        "type": "string",
                        "enum": ["prompt", "yolo"],
                        "description": "Optional worker approval-policy override."
                    },
                    "toolset": {
                        "type": "array",
                        "items": {"type": "string"},
                        "uniqueItems": true,
                        "description": "Optional narrowing override of the profile's default toolset."
                    },
                    "repositories": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "repo": {
                                    "type": "string",
                                    "description": "Workspace-relative repository name."
                                },
                                "branch": {
                                    "type": "string",
                                    "description": "Existing branch to claim; omitted for a fresh thread branch."
                                }
                            },
                            "required": ["repo"],
                            "additionalProperties": false
                        },
                        "uniqueItems": true,
                        "description": "Optional repository and branch claims."
                    }
                },
                "required": ["cwd"],
                "additionalProperties": false
            }),
            Self::ThreadReturn => json!({
                "type": "object",
                "properties": {
                    "kind": {
                        "type": "string",
                        "enum": ["progress", "question"]
                    },
                    "payload": {
                        "type": "string",
                        "description": "Untrusted UTF-8 child data for the immediate parent."
                    },
                    "artifact_refs": {
                        "type": "array",
                        "items": {"type": "string"},
                        "uniqueItems": true,
                        "description": "Optional artifacts already produced by this run."
                    }
                },
                "required": ["kind", "payload"],
                "additionalProperties": false
            }),
            Self::ThreadAnswer => json!({
                "type": "object",
                "properties": {
                    "child_thread_id": {
                        "type": "string",
                        "description": "One immediate child of the executing parent thread."
                    },
                    "kind": {
                        "type": "string",
                        "enum": ["answer", "follow_up"]
                    },
                    "payload": {
                        "type": "string",
                        "description": "Untrusted UTF-8 parent data for the immediate child."
                    }
                },
                "required": ["child_thread_id", "kind", "payload"],
                "additionalProperties": false
            }),
            Self::ComputerWindows => json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
            Self::ComputerObserve => json!({
                "type": "object",
                "properties": {
                    "window_ref": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 96,
                        "pattern": "^[A-Za-z0-9_-]+$"
                    },
                    "max_elements": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 200,
                        "default": 100
                    }
                },
                "required": ["window_ref"],
                "additionalProperties": false
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_catalog_has_exact_names_and_effects() {
        let actual = bootstrap_tools()
            .iter()
            .map(|tool| (tool.internal_name, tool.effect.clone()))
            .collect::<Vec<_>>();

        assert_eq!(
            actual,
            vec![
                (FILE_READ, EffectClass::ReadOnly),
                (FILE_LIST, EffectClass::ReadOnly),
                (FILE_WRITE, EffectClass::WorkspaceWrite),
                (FILE_EDIT, EffectClass::WorkspaceWrite),
                (SHELL_EXEC, EffectClass::ExternalSideEffect),
                (WEB_FETCH, EffectClass::Network),
                (PROFILE_READ, EffectClass::ReadOnly),
                (THREAD_TREE_READ, EffectClass::ReadOnly),
                (THREAD_EVENTS_READ, EffectClass::ReadOnly),
                (THREAD_TRANSCRIPT_READ, EffectClass::ReadOnly),
                (THREAD_SPAWN, EffectClass::WorkspaceWrite),
                (THREAD_RETURN, EffectClass::WorkspaceWrite),
                (THREAD_ANSWER, EffectClass::WorkspaceWrite),
                (COMPUTER_WINDOWS, EffectClass::SecretAccess),
                (COMPUTER_OBSERVE, EffectClass::SecretAccess),
            ]
        );

        let provider_names = bootstrap_tools()
            .iter()
            .map(|tool| tool.provider_name)
            .collect::<Vec<_>>();
        assert_eq!(
            provider_names,
            vec![
                PROVIDER_FILE_READ,
                PROVIDER_FILE_LIST,
                PROVIDER_FILE_WRITE,
                PROVIDER_FILE_EDIT,
                PROVIDER_SHELL_EXEC,
                PROVIDER_WEB_FETCH,
                PROVIDER_PROFILE_READ,
                PROVIDER_THREAD_TREE_READ,
                PROVIDER_THREAD_EVENTS_READ,
                PROVIDER_THREAD_TRANSCRIPT_READ,
                PROVIDER_THREAD_SPAWN,
                PROVIDER_THREAD_RETURN,
                PROVIDER_THREAD_ANSWER,
                PROVIDER_COMPUTER_WINDOWS,
                PROVIDER_COMPUTER_OBSERVE,
            ]
        );
    }

    #[test]
    fn unknown_tool_effect_fails_closed() {
        assert_eq!(
            effect_for_tool("shell.delete"),
            EffectClass::ExternalSideEffect
        );
    }

    #[test]
    fn emits_provider_tool_specs_from_catalog() {
        let specs = tool_specs(&[
            FILE_READ.into(),
            FILE_LIST.into(),
            FILE_WRITE.into(),
            FILE_EDIT.into(),
            SHELL_EXEC.into(),
            WEB_FETCH.into(),
            THREAD_SPAWN.into(),
            COMPUTER_WINDOWS.into(),
            COMPUTER_OBSERVE.into(),
        ]);

        assert_eq!(specs.len(), 9);
        assert_eq!(specs[0].name, PROVIDER_FILE_READ);
        assert_eq!(specs[1].name, PROVIDER_FILE_LIST);
        assert_eq!(specs[2].name, PROVIDER_FILE_WRITE);
        assert_eq!(specs[3].name, PROVIDER_FILE_EDIT);
        assert_eq!(specs[4].name, PROVIDER_SHELL_EXEC);
        assert_eq!(specs[5].name, PROVIDER_WEB_FETCH);
        assert_eq!(specs[6].name, PROVIDER_THREAD_SPAWN);
        assert_eq!(specs[7].name, PROVIDER_COMPUTER_WINDOWS);
        assert_eq!(specs[8].name, PROVIDER_COMPUTER_OBSERVE);
        assert_eq!(
            specs[4].input_schema,
            json!({
                "type": "object",
                "properties": {
                    "command": {
                        "type": "string",
                        "description": "Shell command to run from the workspace root."
                    },
                    "timeout_seconds": {
                        "type": "integer",
                        "description": "Optional timeout in seconds. Defaults to 120 and is capped at 600.",
                        "minimum": 1,
                        "maximum": 600
                    },
                    "credential": {
                        "type": "string",
                        "description": "Optional operator-configured file credential. It is available only for this approved call at $TMPDIR/credentials/<credential>.",
                        "pattern": "^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$",
                        "maxLength": 64
                    }
                },
                "required": ["command"],
                "additionalProperties": false
            })
        );
        assert_eq!(
            specs[5].input_schema,
            json!({
                "type": "object",
                "properties": {
                    "url": {
                        "type": "string",
                        "description": "Absolute public HTTP(S) URL to fetch.",
                        "maxLength": 2048
                    }
                },
                "required": ["url"],
                "additionalProperties": false
            })
        );
        assert_eq!(
            specs[6].input_schema,
            json!({
                "type": "object",
                "properties": {
                    "cwd": {
                        "type": "string",
                        "description": "Absolute worker directory within the coordinator's granted paths."
                    },
                    "model": {
                        "type": "string",
                        "description": "Optional model override."
                    },
                    "reasoning_effort": {
                        "type": "string",
                        "enum": ["none", "minimal", "low", "medium", "high", "xhigh", "max"],
                        "description": "Optional reasoning-effort override."
                    },
                    "approval_policy": {
                        "type": "string",
                        "enum": ["prompt", "yolo"],
                        "description": "Optional worker approval-policy override."
                    },
                    "toolset": {
                        "type": "array",
                        "items": {"type": "string"},
                        "uniqueItems": true,
                        "description": "Optional narrowing override of the profile's default toolset."
                    },
                    "repositories": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "repo": {
                                    "type": "string",
                                    "description": "Workspace-relative repository name."
                                },
                                "branch": {
                                    "type": "string",
                                    "description": "Existing branch to claim; omitted for a fresh thread branch."
                                }
                            },
                            "required": ["repo"],
                            "additionalProperties": false
                        },
                        "uniqueItems": true,
                        "description": "Optional repository and branch claims."
                    }
                },
                "required": ["cwd"],
                "additionalProperties": false
            })
        );
        assert_eq!(
            specs[7].input_schema,
            json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            })
        );
        assert_eq!(
            specs[8].input_schema,
            json!({
                "type": "object",
                "properties": {
                    "window_ref": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 96,
                        "pattern": "^[A-Za-z0-9_-]+$"
                    },
                    "max_elements": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 200,
                        "default": 100
                    }
                },
                "required": ["window_ref"],
                "additionalProperties": false
            })
        );
        assert!(!default_enabled_tools().contains(&THREAD_SPAWN.to_owned()));
        assert!(!default_enabled_tools().contains(&THREAD_RETURN.to_owned()));
        assert!(!default_enabled_tools().contains(&THREAD_ANSWER.to_owned()));
        assert!(!default_enabled_tools().contains(&COMPUTER_WINDOWS.to_owned()));
        assert!(!default_enabled_tools().contains(&COMPUTER_OBSERVE.to_owned()));
    }
}
