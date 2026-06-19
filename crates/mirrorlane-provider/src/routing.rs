//! A deterministic, rule-based router and a recording consumer.
//!
//! [`RuleRouter`] maps a projection's intent to a target consumer, escalating to
//! a human when confidence is below a threshold. [`RecordingConsumer`] is a
//! sink: it records a receipt per `(kind, message id)`, idempotently, since
//! dispatch is at-least-once. Real per-kind delivery (Worklane enqueue, GitHub)
//! replaces the recorder without touching the router.

use std::collections::HashSet;
use std::sync::Mutex;

use async_trait::async_trait;

use mirrorlane_core::Router;
use mirrorlane_core::projection::{Intent, Projection};
use mirrorlane_core::routing::{
    Consumer, ConsumerError, ConsumerKind, ConsumerReceipt, EvaluationStep, RoutingDecision,
    RoutingRule, RoutingTrace,
};

/// Default confidence threshold: below this, routing escalates to a human
/// regardless of intent.
const DEFAULT_ESCALATION_THRESHOLD: f64 = 0.6;

/// A deterministic [`Router`] driven by a **rule set**: the first rule whose
/// `intent` matches selects the target (else the default target), and confidence
/// below the threshold escalates to a human. Destinations are data, not code —
/// `new()` supplies the default rule set, [`with_rules`](RuleRouter::with_rules)
/// supplies a custom one.
#[derive(Debug, Clone)]
pub struct RuleRouter {
    rules: Vec<RoutingRule>,
    escalation_threshold: f64,
    default_target: ConsumerKind,
}

impl Default for RuleRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl RuleRouter {
    /// A router with the default rule set, threshold, and default target — the
    /// reference routing behavior.
    pub fn new() -> Self {
        Self::with_rules(
            default_rules(),
            DEFAULT_ESCALATION_THRESHOLD,
            ConsumerKind::Human,
        )
    }

    /// A router over a supplied rule set, escalation threshold, and default target
    /// (used for an intent with no matching rule).
    pub fn with_rules(
        rules: Vec<RoutingRule>,
        escalation_threshold: f64,
        default_target: ConsumerKind,
    ) -> Self {
        Self {
            rules,
            escalation_threshold,
            default_target,
        }
    }

    /// The target for an intent: the first matching rule, else the default target.
    fn target_for(&self, intent: Intent) -> ConsumerKind {
        self.rules
            .iter()
            .find(|rule| rule.intent == intent)
            .map(|rule| rule.target)
            .unwrap_or(self.default_target)
    }
}

/// The default rule set, reproducing the reference mapping. The single place a
/// default retarget would live; custom mappings come as data via `with_rules`.
fn default_rules() -> Vec<RoutingRule> {
    vec![
        RoutingRule {
            intent: Intent::Question,
            target: ConsumerKind::Human,
        },
        RoutingRule {
            intent: Intent::Decision,
            target: ConsumerKind::Agent,
        },
        RoutingRule {
            intent: Intent::Proposal,
            target: ConsumerKind::GitHub,
        },
        RoutingRule {
            intent: Intent::Issue,
            target: ConsumerKind::GitHub,
        },
        RoutingRule {
            intent: Intent::Task,
            target: ConsumerKind::WorklaneJob,
        },
        RoutingRule {
            intent: Intent::Social,
            target: ConsumerKind::Human,
        },
    ]
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

fn target_label(target: ConsumerKind) -> &'static str {
    match target {
        ConsumerKind::Human => "human",
        ConsumerKind::Agent => "agent",
        ConsumerKind::WorklaneJob => "worklane-job",
        ConsumerKind::GitHub => "github",
    }
}

impl Router for RuleRouter {
    fn route(&self, projection: &Projection) -> (RoutingDecision, RoutingTrace) {
        let mut steps = Vec::new();
        let confidence = projection.confidence.get();

        let threshold = self.escalation_threshold;
        steps.push(EvaluationStep {
            rule_name: "confidence_threshold".to_string(),
            matched: confidence < threshold,
            resulting_target: if confidence < threshold {
                Some(ConsumerKind::Human)
            } else {
                None
            },
        });

        if confidence < threshold {
            return (
                RoutingDecision {
                    message_id: projection.message_id.clone(),
                    target: ConsumerKind::Human,
                    reason: format!(
                        "confidence {confidence:.2} below {threshold:.2} threshold; escalated to human"
                    ),
                    escalated: true,
                },
                RoutingTrace {
                    message_id: projection.message_id.clone(),
                    steps,
                },
            );
        }

        let target = self.target_for(projection.intent);
        steps.push(EvaluationStep {
            rule_name: "intent_routing".to_string(),
            matched: true,
            resulting_target: Some(target),
        });

        (
            RoutingDecision {
                message_id: projection.message_id.clone(),
                target,
                reason: format!(
                    "intent={} routed to {}",
                    intent_label(projection.intent),
                    target_label(target)
                ),
                escalated: false,
            },
            RoutingTrace {
                message_id: projection.message_id.clone(),
                steps,
            },
        )
    }
}

/// A [`Consumer`] that records what it received. Receipts are deduped by
/// `(kind, message id)`, so re-delivery leaves exactly one — a well-behaved,
/// idempotent consumer.
#[derive(Debug, Default)]
pub struct RecordingConsumer {
    receipts: Mutex<HashSet<ConsumerReceipt>>,
}

impl RecordingConsumer {
    /// Create an empty recording consumer.
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of distinct receipts recorded.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether nothing has been recorded.
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    /// Whether a message was received under a given kind.
    pub fn received(
        &self,
        kind: ConsumerKind,
        message_id: &mirrorlane_core::message::MessageId,
    ) -> bool {
        self.lock().contains(&ConsumerReceipt {
            kind,
            message_id: message_id.clone(),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashSet<ConsumerReceipt>> {
        self.receipts
            .lock()
            .expect("recording consumer mutex poisoned")
    }
}

#[async_trait]
impl Consumer for RecordingConsumer {
    async fn consume(
        &self,
        decision: &RoutingDecision,
        _projection: &Projection,
    ) -> Result<(), ConsumerError> {
        self.lock().insert(ConsumerReceipt {
            kind: decision.target,
            message_id: decision.message_id.clone(),
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mirrorlane_core::message::MessageId;
    use mirrorlane_core::projection::{Confidence, Entity, Topic};

    fn projection(intent: Intent, confidence: f64) -> Projection {
        Projection {
            message_id: MessageId("m-1".into()),
            intent,
            topics: vec![Topic("auth".into())],
            entities: vec![Entity("sqlite".into())],
            confidence: Confidence::new(confidence),
        }
    }

    #[test]
    fn intent_selects_target_without_escalation() {
        let router = RuleRouter::new();
        let (decision, _trace) = router.route(&projection(Intent::Task, 0.9));
        assert_eq!(decision.target, ConsumerKind::WorklaneJob);
        assert!(!decision.escalated);
    }

    #[test]
    fn github_bound_intents_route_to_github() {
        let router = RuleRouter::new();
        for intent in [Intent::Issue, Intent::Proposal] {
            let (decision, _trace) = router.route(&projection(intent, 0.9));
            assert_eq!(decision.target, ConsumerKind::GitHub);
            assert!(!decision.escalated);
        }
    }

    #[test]
    fn a_custom_rule_set_retargets_an_intent() {
        // Route Issue to Agent instead of the default GitHub.
        let router = RuleRouter::with_rules(
            vec![RoutingRule {
                intent: Intent::Issue,
                target: ConsumerKind::Agent,
            }],
            0.6,
            ConsumerKind::Human,
        );
        let (decision, _) = router.route(&projection(Intent::Issue, 0.9));
        assert_eq!(decision.target, ConsumerKind::Agent);
    }

    #[test]
    fn an_unmatched_intent_routes_to_the_default_target() {
        // A rule set with no rule for Task falls back to the default target.
        let router = RuleRouter::with_rules(
            vec![RoutingRule {
                intent: Intent::Issue,
                target: ConsumerKind::GitHub,
            }],
            0.6,
            ConsumerKind::Agent,
        );
        let (decision, _) = router.route(&projection(Intent::Task, 0.9));
        assert_eq!(
            decision.target,
            ConsumerKind::Agent,
            "no rule → default target"
        );
        assert!(!decision.escalated);
    }

    #[test]
    fn low_confidence_escalates_to_human() {
        let router = RuleRouter::new();
        // Decision intent would route to Agent, but low confidence escalates.
        let (decision, _trace) = router.route(&projection(Intent::Decision, 0.3));
        assert_eq!(decision.target, ConsumerKind::Human);
        assert!(decision.escalated);
    }

    #[test]
    fn same_projection_routes_identically() {
        let router = RuleRouter::new();
        let p = projection(Intent::Proposal, 0.8);
        assert_eq!(router.route(&p).0, router.route(&p).0);
        assert_eq!(router.route(&p).1, router.route(&p).1);
    }

    #[tokio::test]
    async fn recorder_dedups_redelivery() {
        let consumer = RecordingConsumer::new();
        let decision = RoutingDecision {
            message_id: MessageId("m-1".into()),
            target: ConsumerKind::Agent,
            reason: "test".into(),
            escalated: false,
        };
        consumer
            .consume(&decision, &projection(Intent::Proposal, 0.8))
            .await
            .expect("consume");
        consumer
            .consume(&decision, &projection(Intent::Proposal, 0.8))
            .await
            .expect("consume");
        assert_eq!(consumer.len(), 1, "re-delivery records one receipt");
        assert!(consumer.received(ConsumerKind::Agent, &MessageId("m-1".into())));
    }
}
