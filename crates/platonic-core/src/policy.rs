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
///
/// The tagged JSON schema rejects unknown fields rather than discarding policy data.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "decision")]
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

    const ACCEPTED_V0_2_0_POLICY_DECISION_JSON: &str =
        r#"{"decision":"require_approval","reason":"operator confirmation required"}"#;
    const REJECTED_UNKNOWN_FIELD_POLICY_DECISION_JSON: &str = r#"{"decision":"require_approval","reason":"operator confirmation required","future_field":true}"#;

    #[test]
    fn policy_decision_json_schema_is_fail_closed_and_v0_2_0_compatible() {
        let accepted: PolicyDecision =
            serde_json::from_str(ACCEPTED_V0_2_0_POLICY_DECISION_JSON).unwrap();
        assert_eq!(
            accepted,
            PolicyDecision::RequireApproval {
                reason: "operator confirmation required".into(),
            }
        );

        let error =
            serde_json::from_str::<PolicyDecision>(REJECTED_UNKNOWN_FIELD_POLICY_DECISION_JSON)
                .unwrap_err();
        assert!(error.to_string().contains("unknown field `future_field`"));
    }

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
