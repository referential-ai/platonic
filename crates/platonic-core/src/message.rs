//! Model-facing message primitives.

use serde::{Deserialize, Serialize};

/// Role for a message visible to a model adapter.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    /// Stable operating contract and runtime rules.
    System,
    /// Human or upstream caller input.
    User,
    /// Model-authored message.
    Assistant,
    /// Tool result observation.
    Tool,
}

/// Message passed through the model boundary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Message {
    /// Message role.
    pub role: MessageRole,
    /// Plain text content. Richer content blocks belong above this core layer.
    pub content: String,
}
