//! Context assembly primitives with lane labels and budget validation.

use crate::Error;
use serde::{Deserialize, Serialize};

/// Lane accounting for context assembly.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextLane {
    /// Stable system contract.
    SystemContract,
    /// Current user task.
    CurrentTask,
    /// Selected tool schemas only.
    ToolSchemas,
    /// Recent turns preserved verbatim.
    RecentTurns,
    /// Retrieved memories or project facts.
    RetrievedContext,
    /// Artifact summaries instead of raw large blobs.
    ArtifactSummary,
    /// Runtime policy and approval constraints.
    Policy,
}

/// One accountable context fragment.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextFragment {
    /// Context lane this fragment belongs to.
    pub lane: ContextLane,
    /// Human-readable source path, URL, event id, or synthetic label.
    pub source: String,
    /// Text injected into the model prompt.
    pub content: String,
    /// Estimated token count used for budget checks.
    pub estimated_tokens: u32,
}

/// A bounded prompt/context bundle.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextPack {
    /// Maximum allowed prompt tokens for this pack.
    pub token_budget: u32,
    /// Context fragments selected for the next model call.
    pub fragments: Vec<ContextFragment>,
}

impl ContextPack {
    /// Sums fragment estimates, saturating at [`u32::MAX`].
    pub fn estimated_tokens(&self) -> u32 {
        self.estimated_tokens_u64().min(u64::from(u32::MAX)) as u32
    }

    fn estimated_tokens_u64(&self) -> u64 {
        self.fragments
            .iter()
            .map(|fragment| u64::from(fragment.estimated_tokens))
            .sum()
    }

    /// Rejects a fragment sum that exceeds the declared token budget.
    pub fn validate_budget(&self) -> Result<(), Error> {
        let used = self.estimated_tokens_u64();
        if used > u64::from(self.token_budget) {
            return Err(Error::ContextBudgetExceeded {
                used: used.min(u64::from(u32::MAX)) as u32,
                budget: self.token_budget,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_budget_is_enforced() {
        let pack = ContextPack {
            token_budget: 10,
            fragments: vec![ContextFragment {
                lane: ContextLane::CurrentTask,
                source: "user".into(),
                content: "test".into(),
                estimated_tokens: 11,
            }],
        };

        assert!(matches!(
            pack.validate_budget(),
            Err(Error::ContextBudgetExceeded {
                used: 11,
                budget: 10
            })
        ));
    }

    #[test]
    fn context_budget_rejects_overflowing_fragment_sum() {
        let large_fragment = u32::MAX / 2 + 1;
        let pack = ContextPack {
            token_budget: u32::MAX,
            fragments: vec![
                ContextFragment {
                    lane: ContextLane::CurrentTask,
                    source: "first".into(),
                    content: "test".into(),
                    estimated_tokens: large_fragment,
                },
                ContextFragment {
                    lane: ContextLane::RetrievedContext,
                    source: "second".into(),
                    content: "test".into(),
                    estimated_tokens: large_fragment,
                },
            ],
        };

        assert_eq!(pack.estimated_tokens(), u32::MAX);
        assert!(matches!(
            pack.validate_budget(),
            Err(Error::ContextBudgetExceeded {
                used: u32::MAX,
                budget: u32::MAX
            })
        ));
    }
}
