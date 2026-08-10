//! Tool-call and tool-result boundary types.

use crate::{ArtifactId, EffectClass, ToolCallId, ToolName};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A model-authored tool proposal before host validation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolProposal {
    /// Registered tool name requested by the model.
    pub tool: ToolName,
    /// JSON input to validate against the registered schema.
    pub input: Value,
}

/// A host-validated tool invocation evaluated by policy.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolCall {
    /// Stable call id assigned by the harness.
    pub id: ToolCallId,
    /// Registered tool name.
    pub tool: ToolName,
    /// Host/registry-declared effect class.
    pub effect: EffectClass,
    /// JSON input validated against the registered tool schema.
    pub input: Value,
}

/// Controls which audience sees a tool result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultVisibility {
    /// Only model receives it.
    Model,
    /// Only user-facing transcript receives it.
    User,
    /// Model and user-facing transcript both receive it.
    Both,
}

/// Structured tool result; raw output should be kept as an artifact when large.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolResult {
    /// Tool call this result answers.
    pub call_id: ToolCallId,
    /// Short model-usable summary.
    pub summary: String,
    /// Structured data result.
    pub data: Value,
    /// Artifacts created or referenced by this result.
    pub artifacts: Vec<ArtifactId>,
    /// Visibility boundary for this result.
    pub visibility: ResultVisibility,
}
