//! Policy primitives for evaluating proposed side effects.

use serde::{Deserialize, Serialize};

/// High-level class of effect a tool may produce.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectClass {
    /// Reads local or remote state without mutation.
    ReadOnly,
    /// Mutates files in an explicit workspace.
    WorkspaceWrite,
    /// Performs network IO without an external irreversible side effect.
    Network,
    /// Sends, publishes, charges, deploys, deletes, or otherwise affects the world.
    ExternalSideEffect,
    /// Requests access to credentials, secrets, or protected material.
    SecretAccess,
}

impl EffectClass {
    /// Returns the fail-closed baseline decision for this effect class.
    pub fn default_policy(&self) -> PolicyDecision {
        match self {
            Self::ReadOnly => PolicyDecision::Allow,
            Self::WorkspaceWrite | Self::Network => PolicyDecision::RequireApproval {
                reason: "mutable or networked tool call requires explicit policy allowance".into(),
            },
            Self::ExternalSideEffect | Self::SecretAccess => PolicyDecision::Deny {
                reason: "external side effects and secret access fail closed by default".into(),
            },
        }
    }
}

/// Policy decision for a proposed model or tool action.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "decision")]
pub enum PolicyDecision {
    /// Action may proceed.
    Allow,
    /// Action may proceed only after approval.
    RequireApproval {
        /// Explanation presented to the approver and retained in run state.
        reason: String,
    },
    /// Action must not proceed.
    Deny {
        /// Durable explanation for rejecting the action.
        reason: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn external_side_effects_fail_closed_by_default() {
        assert!(matches!(
            EffectClass::ExternalSideEffect.default_policy(),
            PolicyDecision::Deny { .. }
        ));
    }

    #[test]
    fn read_only_is_allowed_by_default() {
        assert!(matches!(
            EffectClass::ReadOnly.default_policy(),
            PolicyDecision::Allow
        ));
    }

    #[test]
    fn workspace_writes_require_approval_by_default() {
        assert!(matches!(
            EffectClass::WorkspaceWrite.default_policy(),
            PolicyDecision::RequireApproval { .. }
        ));
    }
}
