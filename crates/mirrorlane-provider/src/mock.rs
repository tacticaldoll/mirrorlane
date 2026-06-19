//! A deterministic, keyword-based projector.
//!
//! This is a placeholder for real SLM inference: it exists to validate the
//! pipeline and the model, not to understand language. Given the same message
//! it always returns the same projection, so replay stays deterministic.

use mirrorlane_core::message::MessageEnvelope;
use mirrorlane_core::projection::{Confidence, Entity, Intent, Projection, Topic};
use mirrorlane_runtime::{Step, StepVersion};

/// Known topic keywords, matched as case-insensitive substrings of the body.
const TOPIC_KEYWORDS: &[&str] = &[
    "rust", "backend", "sdk", "infra", "ai", "auth", "oauth", "ci",
];

/// Known entity keywords, matched as case-insensitive substrings of the body.
const ENTITY_KEYWORDS: &[&str] = &[
    "postgres",
    "sqlite",
    "refresh-token",
    "authservice",
    "opentelemetry",
];

/// A deterministic mock [`Projector`].
#[derive(Debug, Default, Clone)]
pub struct MockProjector;

impl MockProjector {
    /// Create a mock projector.
    pub fn new() -> Self {
        Self
    }
}

impl Step for MockProjector {
    type In = MessageEnvelope;
    type Out = Projection;

    fn kind(&self) -> &'static str {
        "mirrorlane.projection.mock"
    }

    fn version(&self) -> StepVersion {
        StepVersion::new("mock:v1")
    }

    fn run(&self, message: &MessageEnvelope) -> Projection {
        let body = message.body.to_lowercase();

        let topics: Vec<Topic> = TOPIC_KEYWORDS
            .iter()
            .filter(|kw| body.contains(*kw))
            .map(|kw| Topic((*kw).to_string()))
            .collect();

        let entities: Vec<Entity> = ENTITY_KEYWORDS
            .iter()
            .filter(|kw| body.contains(*kw))
            .map(|kw| Entity((*kw).to_string()))
            .collect();

        // More recognized signal -> higher confidence, deterministically.
        let signal = topics.len() + entities.len();
        let confidence = Confidence::new(0.5 + 0.1 * signal as f64);

        Projection {
            message_id: message.id.clone(),
            intent: detect_intent(&body),
            topics,
            entities,
            confidence,
        }
    }
}

fn detect_intent(body: &str) -> Intent {
    if body.contains('?') {
        Intent::Question
    } else if body.contains("propose") || body.contains("proposal") || body.contains("suggest") {
        Intent::Proposal
    } else if body.contains("decided") || body.contains("let's") || body.contains("we will") {
        Intent::Decision
    } else if body.contains("bug") || body.contains("error") || body.contains("broken") {
        Intent::Issue
    } else if body.contains("todo") || body.contains("task") || body.contains("need to") {
        Intent::Task
    } else {
        Intent::Social
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mirrorlane_core::Projector;
    use mirrorlane_core::message::{
        Author, AuthorId, Conversation, ConversationId, MessageId, Source,
    };

    fn message(body: &str) -> MessageEnvelope {
        MessageEnvelope {
            id: MessageId("m-1".into()),
            source: Source::Manual,
            author: Author {
                id: AuthorId("u-1".into()),
                display_name: "Dev".into(),
            },
            conversation: Conversation {
                id: ConversationId("c-1".into()),
                thread: None,
            },
            body: body.into(),
        }
    }

    #[test]
    fn same_message_projects_identically() {
        let projector = MockProjector::new();
        let msg = message("Should we use sqlite for the auth sdk refresh-token store?");
        assert_eq!(projector.project(&msg), projector.project(&msg));
    }

    #[test]
    fn recognizes_intent_topics_and_entities() {
        let projector = MockProjector::new();
        let projection = projector.project(&message(
            "Should we use sqlite for the auth sdk refresh-token?",
        ));
        assert_eq!(projection.intent, Intent::Question);
        assert!(projection.topics.contains(&Topic("auth".into())));
        assert!(projection.topics.contains(&Topic("sdk".into())));
        assert!(projection.entities.contains(&Entity("sqlite".into())));
        assert!(
            projection
                .entities
                .contains(&Entity("refresh-token".into()))
        );
    }
}
