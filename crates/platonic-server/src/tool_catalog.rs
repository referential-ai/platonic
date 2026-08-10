use platonic_core::EffectClass;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const FILE_READ: &str = "file.read";
pub const FILE_LIST: &str = "file.list";
pub const FILE_WRITE: &str = "file.write";
pub const FILE_EDIT: &str = "file.edit";
pub const SHELL_EXEC: &str = "shell.exec";
pub const WEB_FETCH: &str = "web.fetch";
pub const THREAD_SPAWN: &str = "thread.spawn";

const PROVIDER_FILE_READ: &str = "file_read";
const PROVIDER_FILE_LIST: &str = "file_list";
const PROVIDER_FILE_WRITE: &str = "file_write";
const PROVIDER_FILE_EDIT: &str = "file_edit";
const PROVIDER_SHELL_EXEC: &str = "shell_exec";
const PROVIDER_WEB_FETCH: &str = "web_fetch";
const PROVIDER_THREAD_SPAWN: &str = "thread_spawn";

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
        internal_name: THREAD_SPAWN,
        provider_name: PROVIDER_THREAD_SPAWN,
        effect: EffectClass::WorkspaceWrite,
        description: "Spawn one bounded worker thread from a configured agent after approval.",
        input_schema: ToolInputSchema::ThreadSpawn,
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
    ThreadSpawn,
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
        .filter(|tool| tool.internal_name != THREAD_SPAWN)
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
            Self::ThreadSpawn => json!({
                "type": "object",
                "properties": {
                    "agent_id": {
                        "type": "string",
                        "description": "Configured target agent in the coordinator's workspace."
                    },
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
                        "description": "Optional narrowing override of the target agent's default toolset."
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
                "required": ["agent_id", "cwd"],
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
                (THREAD_SPAWN, EffectClass::WorkspaceWrite),
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
                PROVIDER_THREAD_SPAWN,
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
        ]);

        assert_eq!(specs.len(), 7);
        assert_eq!(specs[0].name, PROVIDER_FILE_READ);
        assert_eq!(specs[1].name, PROVIDER_FILE_LIST);
        assert_eq!(specs[2].name, PROVIDER_FILE_WRITE);
        assert_eq!(specs[3].name, PROVIDER_FILE_EDIT);
        assert_eq!(specs[4].name, PROVIDER_SHELL_EXEC);
        assert_eq!(specs[5].name, PROVIDER_WEB_FETCH);
        assert_eq!(specs[6].name, PROVIDER_THREAD_SPAWN);
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
                    "agent_id": {
                        "type": "string",
                        "description": "Configured target agent in the coordinator's workspace."
                    },
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
                        "description": "Optional narrowing override of the target agent's default toolset."
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
                "required": ["agent_id", "cwd"],
                "additionalProperties": false
            })
        );
        assert!(!default_enabled_tools().contains(&THREAD_SPAWN.to_owned()));
    }
}
