//! The message model: a normalized envelope for one unit of human
//! communication, sourced from Discord, Slack, GitHub, or a manual entry.

use serde::{Deserialize, Serialize};

/// Stable identifier for a message.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MessageId(pub String);

/// Where a message originated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    Discord,
    Slack,
    GitHub,
    Manual,
}

/// Stable identifier for a message author.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AuthorId(pub String);

/// The author of a message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Author {
    pub id: AuthorId,
    pub display_name: String,
}

/// Identifier for a conversation (channel, DM, issue, etc.).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConversationId(pub String);

/// Identifier for a thread within a conversation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ThreadId(pub String);

/// The conversation a message belongs to, with an optional thread.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Conversation {
    pub id: ConversationId,
    pub thread: Option<ThreadId>,
}

/// A normalized message: the unit of input to the projection pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageEnvelope {
    pub id: MessageId,
    pub source: Source,
    pub author: Author,
    pub conversation: Conversation,
    pub body: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> MessageEnvelope {
        MessageEnvelope {
            id: MessageId("m-1".into()),
            source: Source::Discord,
            author: Author {
                id: AuthorId("u-alice".into()),
                display_name: "Alice".into(),
            },
            conversation: Conversation {
                id: ConversationId("c-1".into()),
                thread: Some(ThreadId("t-1".into())),
            },
            body: "Should we use sqlite for the auth sdk?".into(),
        }
    }

    #[test]
    fn envelope_round_trips_through_json() {
        let message = sample();
        let json = serde_json::to_string(&message).expect("serialize");
        let back: MessageEnvelope = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(message, back);
    }
}
