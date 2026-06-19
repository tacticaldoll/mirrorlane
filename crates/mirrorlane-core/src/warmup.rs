//! The warm-up model: what a session needs to start, and to resume.

use serde::{Deserialize, Serialize};

use crate::message::{ConversationId, MessageId};
use crate::scope::Component;

/// A session warm-up: what to focus on, the recent decisions, open questions,
/// and tasks, plus a rendered human-/agent-readable summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WarmupDocument {
    pub conversation: ConversationId,
    pub focus: Vec<Component>,
    pub decisions: Vec<MessageId>,
    pub open_questions: Vec<MessageId>,
    pub tasks: Vec<MessageId>,
    pub summary: String,
}

/// A request to build a warm-up for a session: the conversation and its
/// messages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WarmupRequest {
    pub conversation: ConversationId,
    pub messages: Vec<MessageId>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_round_trips_through_json() {
        let document = WarmupDocument {
            conversation: ConversationId("c-1".into()),
            focus: vec![Component("auth-sdk".into())],
            decisions: vec![MessageId("m-1".into())],
            open_questions: vec![MessageId("m-2".into())],
            tasks: vec![MessageId("m-3".into())],
            summary: "You are joining c-1.".into(),
        };
        let json = serde_json::to_string(&document).expect("serialize");
        let back: WarmupDocument = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(document, back);
    }
}
