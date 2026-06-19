//! The scope model: what a session should load and ignore, and why.

use serde::{Deserialize, Serialize};

use crate::message::{ConversationId, MessageId};
use crate::projection::Confidence;

/// A repository area or component a session may load or ignore.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Component(pub String);

/// What a session should load and ignore, with a human-readable reason.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Scope {
    pub conversation: ConversationId,
    pub load: Vec<Component>,
    pub ignore: Vec<Component>,
    pub reason: String,
    pub confidence: Confidence,
}

/// A request to build a scope for a session: the conversation and the messages
/// that form it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeRequest {
    pub conversation: ConversationId,
    pub messages: Vec<MessageId>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Scope {
        Scope {
            conversation: ConversationId("c-1".into()),
            load: vec![
                Component("auth-sdk".into()),
                Component("token-refresh".into()),
            ],
            ignore: vec![Component("web-ui".into()), Component("billing".into())],
            reason: "Session focuses on auth-sdk, token-refresh.".into(),
            confidence: Confidence::new(0.8),
        }
    }

    #[test]
    fn scope_round_trips_and_has_documented_fields() {
        let scope = sample();
        let value: serde_json::Value = serde_json::to_value(&scope).expect("to value");
        assert!(value.get("load").is_some());
        assert!(value.get("ignore").is_some());
        assert!(value.get("reason").is_some());

        let json = serde_json::to_string(&scope).expect("serialize");
        let back: Scope = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(scope, back);
    }
}
