//! Compact identifier newtypes used across the harness event ledger.

use crate::Error;
use serde::{Deserialize, Deserializer, Serialize, de};
use std::fmt;

/// Defines a compact string-backed identifier newtype.
macro_rules! id_type {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize)]
        pub struct $name(String);

        impl $name {
            /// Rejects empty or all-whitespace input and otherwise stores it verbatim.
            pub fn new(value: impl Into<String>) -> Result<Self, Error> {
                let value = value.into();
                if value.trim().is_empty() {
                    return Err(Error::EmptyIdentifier(stringify!($name)));
                }
                Ok(Self(value))
            }

            /// Borrows the exact stored identifier value.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::new(value).map_err(de::Error::custom)
            }
        }
    };
}

id_type!(RunId, "Identifier for one durable harness run.");
id_type!(TurnId, "Identifier for one model/tool turn inside a run.");
id_type!(AgentId, "Identifier for one bounded agent unit.");
id_type!(
    ToolCallId,
    "Identifier for one host-validated tool invocation."
);
id_type!(
    ArtifactId,
    "Identifier for a durable artifact emitted by a run."
);
id_type!(ToolName, "Stable registered tool name.");
id_type!(
    ModelName,
    "Stable model identifier selected for a request or reported for a response."
);
id_type!(ActorId, "Identifier for a human or host approval actor.");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_reject_empty_values() {
        assert!(matches!(
            RunId::new("  "),
            Err(Error::EmptyIdentifier("RunId"))
        ));
    }

    #[test]
    fn identifiers_display_their_inner_value() {
        let id = AgentId::new("agent_alpha").unwrap();
        assert_eq!(id.to_string(), "agent_alpha");
        assert_eq!(id.as_str(), "agent_alpha");
    }

    #[test]
    fn identifiers_reject_empty_json_values() {
        let err = serde_json::from_str::<RunId>("\"  \"").unwrap_err();
        assert!(err.to_string().contains("RunId cannot be empty"));
    }
}
