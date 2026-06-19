//! The GitHub draft consumer: turn a routed projection into a GitHub draft.
//!
//! [`GitHubConsumer`] is the first concrete routing [`Consumer`], behind
//! `ConsumerKind::GitHub`. On a routed projection it drafts an issue or PR
//! description via [`draft_for`] and records it, keyed by message id — it does
//! **not** post to GitHub. Drafting is a pure function of the projection, so it
//! is deterministic and replay-safe; a real publish step is a later, opt-in
//! change.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use mirrorlane_core::Consumer;
use mirrorlane_core::message::MessageId;
use mirrorlane_core::projection::{Intent, Projection};
use mirrorlane_core::routing::{ConsumerError, RoutingDecision};

/// Which GitHub artifact a [`GitHubDraft`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DraftKind {
    Issue,
    PullRequest,
}

/// A drafted GitHub artifact, derived from a projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitHubDraft {
    pub kind: DraftKind,
    pub message_id: MessageId,
    pub title: String,
    pub body: String,
}

fn intent_label(intent: Intent) -> &'static str {
    match intent {
        Intent::Question => "question",
        Intent::Decision => "decision",
        Intent::Proposal => "proposal",
        Intent::Task => "task",
        Intent::Issue => "issue",
        Intent::Social => "social",
    }
}

/// Draft a GitHub artifact from a projection, deterministically.
///
/// A `Proposal` drafts a `PullRequest`; any other intent drafts an `Issue`. The
/// title is derived from the primary topic and intent; the body is a stable
/// summary of the projection.
pub fn draft_for(projection: &Projection) -> GitHubDraft {
    let kind = match projection.intent {
        Intent::Proposal => DraftKind::PullRequest,
        _ => DraftKind::Issue,
    };
    let primary = projection
        .topics
        .first()
        .map(|t| t.0.as_str())
        .unwrap_or("general");
    let title = match kind {
        DraftKind::Issue => format!("[{primary}] {}", intent_label(projection.intent)),
        DraftKind::PullRequest => format!("Proposal: {primary}"),
    };
    let topics = projection
        .topics
        .iter()
        .map(|t| t.0.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let entities = projection
        .entities
        .iter()
        .map(|e| e.0.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let body = format!(
        "Auto-drafted from message {} (confidence {:.2}).\n\nTopics: {}\nEntities: {}",
        projection.message_id.0,
        projection.confidence.get(),
        if topics.is_empty() { "—" } else { &topics },
        if entities.is_empty() {
            "—"
        } else {
            &entities
        },
    );
    GitHubDraft {
        kind,
        message_id: projection.message_id.clone(),
        title,
        body,
    }
}

/// A routing [`Consumer`] that records GitHub drafts, keyed by message id.
///
/// Idempotent: re-consuming the same message overwrites its draft with an
/// identical one, so the consumer holds exactly one draft per message under
/// at-least-once dispatch.
#[derive(Debug, Default)]
pub struct GitHubConsumer {
    drafts: Mutex<HashMap<MessageId, GitHubDraft>>,
}

impl GitHubConsumer {
    /// Create an empty consumer.
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of distinct drafts recorded.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether nothing has been drafted.
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    /// The draft recorded for a message, if any.
    pub fn get(&self, message_id: &MessageId) -> Option<GitHubDraft> {
        self.lock().get(message_id).cloned()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<MessageId, GitHubDraft>> {
        self.drafts.lock().expect("github consumer mutex poisoned")
    }
}

#[async_trait]
impl Consumer for GitHubConsumer {
    async fn consume(
        &self,
        _decision: &RoutingDecision,
        projection: &Projection,
    ) -> Result<(), ConsumerError> {
        let draft = draft_for(projection);
        self.lock().insert(projection.message_id.clone(), draft);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mirrorlane_core::projection::{Confidence, Entity, Topic};
    use mirrorlane_core::routing::ConsumerKind;

    fn projection(intent: Intent) -> Projection {
        Projection {
            message_id: MessageId("m-1".into()),
            intent,
            topics: vec![Topic("auth".into())],
            entities: vec![Entity("refresh-token".into())],
            confidence: Confidence::new(0.8),
        }
    }

    fn decision() -> RoutingDecision {
        RoutingDecision {
            message_id: MessageId("m-1".into()),
            target: ConsumerKind::GitHub,
            reason: "test".into(),
            escalated: false,
        }
    }

    #[test]
    fn draft_round_trips_through_json() {
        let draft = draft_for(&projection(Intent::Issue));
        let json = serde_json::to_string(&draft).expect("serialize");
        let back: GitHubDraft = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(draft, back);
    }

    #[test]
    fn proposal_drafts_a_pull_request() {
        assert_eq!(
            draft_for(&projection(Intent::Proposal)).kind,
            DraftKind::PullRequest
        );
    }

    #[test]
    fn other_intents_draft_an_issue() {
        assert_eq!(draft_for(&projection(Intent::Issue)).kind, DraftKind::Issue);
        assert_eq!(draft_for(&projection(Intent::Task)).kind, DraftKind::Issue);
    }

    #[test]
    fn drafting_is_stable() {
        let p = projection(Intent::Issue);
        assert_eq!(draft_for(&p), draft_for(&p));
    }

    #[tokio::test]
    async fn consuming_records_one_draft_idempotently() {
        let consumer = GitHubConsumer::new();
        consumer
            .consume(&decision(), &projection(Intent::Issue))
            .await
            .expect("consume");
        consumer
            .consume(&decision(), &projection(Intent::Issue))
            .await
            .expect("consume");
        assert_eq!(consumer.len(), 1, "re-delivery records one draft");
        assert!(consumer.get(&MessageId("m-1".into())).is_some());
    }
}
