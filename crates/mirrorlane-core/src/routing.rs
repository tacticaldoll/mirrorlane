//! The routing model: the orchestrator's output edge.
//!
//! Routing decides, per projection, which [`ConsumerKind`] should receive it —
//! a human, an agent, or a Worklane job — and a [`ConsumerRegistry`] dispatches
//! the [`RoutingDecision`] to the registered [`Consumer`].
//!
//! Routing is a **separate dispatch path, not a replay phase**: the decision is
//! deterministic derived state, but dispatch is an external side effect that
//! `Replay` must never re-run, so the no-duplication contract holds once
//! consumers are real.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::message::MessageId;
use crate::projection::Projection;
use crate::skill::ExpertCandidate;

/// Who a projection's context is routed to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerKind {
    Human,
    Agent,
    WorklaneJob,
    GitHub,
}

/// A rule mapping a projection [`Intent`](crate::projection::Intent) to a target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutingRule {
    pub intent: crate::projection::Intent,
    pub target: ConsumerKind,
}

/// Where a projection was routed, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutingDecision {
    pub message_id: MessageId,
    pub target: ConsumerKind,
    pub reason: String,
    pub escalated: bool,
}

/// A single step in the routing rule evaluation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvaluationStep {
    pub rule_name: String,
    pub matched: bool,
    pub resulting_target: Option<ConsumerKind>,
}

/// The trace of how a routing decision was reached.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutingTrace {
    pub message_id: MessageId,
    pub steps: Vec<EvaluationStep>,
}

/// Skill-derived guidance attached to a routed message: who should review it
/// (`reviewers`, ranked best-first across the message's projected topics) and the
/// single best person to route to (`human_hint`, the top reviewer or `None` when
/// no one qualifies).
///
/// Derived from the skill index, it is **replayable derived state**, not a
/// dispatch side effect: replay re-computes it identically and it triggers no
/// delivery.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RoutingHint {
    pub message_id: MessageId,
    pub reviewers: Vec<ExpertCandidate>,
    pub human_hint: Option<ExpertCandidate>,
}

/// A record that a consumer received a routed message.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConsumerReceipt {
    pub kind: ConsumerKind,
    pub message_id: MessageId,
}

/// A request to route a set of messages by their ids.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutingRequest {
    pub messages: Vec<MessageId>,
}

/// A request to build routing hints for a set of messages by their ids.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutingHintRequest {
    pub messages: Vec<MessageId>,
}

/// An error from a [`Consumer`] while consuming a routed decision. Real
/// consumers do I/O (a Worklane enqueue, a GitHub call) that can fail; this lets
/// them report it instead of swallowing it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerError(pub String);

impl ConsumerError {
    /// Build an error from any displayable cause.
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for ConsumerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "consumer error: {}", self.0)
    }
}

impl std::error::Error for ConsumerError {}

/// A sink for routed context. Real consumers (Worklane enqueue, GitHub) do I/O
/// and must be idempotent, since dispatch is at-least-once. `consume` is async
/// and fallible so an I/O consumer can report a failure rather than hide it; a
/// recording consumer simply returns `Ok(())`.
#[async_trait]
pub trait Consumer: Send + Sync {
    async fn consume(
        &self,
        decision: &RoutingDecision,
        projection: &Projection,
    ) -> Result<(), ConsumerError>;
}

/// Dispatches a [`RoutingDecision`] to the [`Consumer`] registered for its
/// target. Dispatch to an unregistered target is a successful no-op, so a missing
/// real consumer never crashes routing; a registered consumer's error propagates
/// to the caller.
#[derive(Default)]
pub struct ConsumerRegistry {
    consumers: HashMap<ConsumerKind, Arc<dyn Consumer>>,
}

impl ConsumerRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register the consumer for a kind, replacing any prior one.
    pub fn register(&mut self, kind: ConsumerKind, consumer: Arc<dyn Consumer>) {
        self.consumers.insert(kind, consumer);
    }

    /// Dispatch a decision to its target consumer, if one is registered. An
    /// unregistered target is a successful no-op; a registered consumer's error
    /// propagates to the caller.
    pub async fn dispatch(
        &self,
        decision: &RoutingDecision,
        projection: &Projection,
    ) -> Result<(), ConsumerError> {
        if let Some(consumer) = self.consumers.get(&decision.target) {
            consumer.consume(decision, projection).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projection::{Confidence, Intent};
    use std::sync::Mutex;

    fn decision(target: ConsumerKind) -> RoutingDecision {
        RoutingDecision {
            message_id: MessageId("m-1".into()),
            target,
            reason: "test".into(),
            escalated: false,
        }
    }

    fn projection() -> Projection {
        Projection {
            message_id: MessageId("m-1".into()),
            intent: Intent::Task,
            topics: Vec::new(),
            entities: Vec::new(),
            confidence: Confidence::new(0.9),
        }
    }

    #[test]
    fn decision_round_trips_through_json() {
        let d = decision(ConsumerKind::WorklaneJob);
        let json = serde_json::to_string(&d).expect("serialize");
        let back: RoutingDecision = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(d, back);
    }

    #[test]
    fn hint_round_trips_through_json() {
        use crate::message::AuthorId;
        use crate::skill::SkillScore;

        let alice = ExpertCandidate {
            author: AuthorId("u-alice".into()),
            display_name: "alice".into(),
            score: SkillScore::new(1.0),
        };
        let hint = RoutingHint {
            message_id: MessageId("m-1".into()),
            reviewers: vec![alice.clone()],
            human_hint: Some(alice),
        };
        let json = serde_json::to_string(&hint).expect("serialize");
        let back: RoutingHint = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(hint, back);
    }

    #[derive(Default)]
    struct Spy {
        seen: Mutex<Vec<MessageId>>,
    }
    #[async_trait]
    impl Consumer for Spy {
        async fn consume(
            &self,
            decision: &RoutingDecision,
            _projection: &Projection,
        ) -> Result<(), ConsumerError> {
            self.seen.lock().unwrap().push(decision.message_id.clone());
            Ok(())
        }
    }

    #[tokio::test]
    async fn dispatch_reaches_the_target_consumer() {
        let spy = Arc::new(Spy::default());
        let mut registry = ConsumerRegistry::new();
        registry.register(ConsumerKind::Agent, spy.clone());

        registry
            .dispatch(&decision(ConsumerKind::Agent), &projection())
            .await
            .expect("dispatch succeeds");
        assert_eq!(spy.seen.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn dispatch_to_an_unregistered_target_is_a_noop() {
        let spy = Arc::new(Spy::default());
        let mut registry = ConsumerRegistry::new();
        registry.register(ConsumerKind::Agent, spy.clone());

        // A decision for an unregistered target succeeds and consumes nothing.
        registry
            .dispatch(&decision(ConsumerKind::Human), &projection())
            .await
            .expect("unregistered target is a successful no-op");
        assert!(spy.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_consumer_error_propagates_from_dispatch() {
        struct Failing;
        #[async_trait]
        impl Consumer for Failing {
            async fn consume(
                &self,
                _decision: &RoutingDecision,
                _projection: &Projection,
            ) -> Result<(), ConsumerError> {
                Err(ConsumerError::new("boom"))
            }
        }

        let mut registry = ConsumerRegistry::new();
        registry.register(ConsumerKind::Agent, Arc::new(Failing));
        let err = registry
            .dispatch(&decision(ConsumerKind::Agent), &projection())
            .await
            .expect_err("a consumer error propagates");
        assert_eq!(err, ConsumerError::new("boom"));
    }
}
