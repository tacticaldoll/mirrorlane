//! A deterministic, catalog-based scope projector.
//!
//! A placeholder for real, repository-aware scoping: it maps a session's
//! projection topics and entities to components from a static catalog, loads the
//! matched components, and ignores the rest. Given the same projections it always
//! returns the same scope.

use std::collections::BTreeSet;

use mirrorlane_core::ScopeProjector;
use mirrorlane_core::message::ConversationId;
use mirrorlane_core::projection::{Confidence, Projection};
use mirrorlane_core::scope::{Component, Scope};

/// The components this scoper knows about. Ignored components are drawn from here.
const CATALOG: &[&str] = &[
    "auth-sdk",
    "oauth2",
    "token-refresh",
    "web-ui",
    "analytics",
    "billing",
];

/// Map a topic/entity keyword to a catalog component, if any.
fn component_for(keyword: &str) -> Option<&'static str> {
    match keyword.to_lowercase().as_str() {
        "auth" | "sdk" => Some("auth-sdk"),
        "oauth" | "oauth2" => Some("oauth2"),
        "refresh-token" => Some("token-refresh"),
        "web" | "ui" | "web-ui" => Some("web-ui"),
        "analytics" => Some("analytics"),
        "billing" => Some("billing"),
        _ => None,
    }
}

/// A deterministic mock [`ScopeProjector`].
#[derive(Debug, Default, Clone)]
pub struct MockScopeProjector;

impl MockScopeProjector {
    /// Create a mock scope projector.
    pub fn new() -> Self {
        Self
    }
}

impl ScopeProjector for MockScopeProjector {
    fn scope(&self, conversation: &ConversationId, projections: &[Projection]) -> Scope {
        let mut loaded: BTreeSet<Component> = BTreeSet::new();
        let mut confidence_sum = 0.0;

        for projection in projections {
            confidence_sum += projection.confidence.get();
            let keywords = projection
                .topics
                .iter()
                .map(|t| t.0.as_str())
                .chain(projection.entities.iter().map(|e| e.0.as_str()));
            for keyword in keywords {
                if let Some(component) = component_for(keyword) {
                    loaded.insert(Component(component.to_string()));
                }
            }
        }

        let load: Vec<Component> = loaded.iter().cloned().collect();
        let ignore: Vec<Component> = CATALOG
            .iter()
            .map(|c| Component((*c).to_string()))
            .filter(|c| !loaded.contains(c))
            .collect();

        let confidence = if projections.is_empty() {
            Confidence::new(0.0)
        } else {
            Confidence::new(confidence_sum / projections.len() as f64)
        };

        let reason = if load.is_empty() {
            "No known components detected for this session.".to_string()
        } else {
            let names: Vec<&str> = load.iter().map(|c| c.0.as_str()).collect();
            format!("Session focuses on {}.", names.join(", "))
        };

        Scope {
            conversation: conversation.clone(),
            load,
            ignore,
            reason,
            confidence,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mirrorlane_core::message::MessageId;
    use mirrorlane_core::projection::{Entity, Intent, Topic};

    fn projection(id: &str, topics: &[&str], entities: &[&str], confidence: f64) -> Projection {
        Projection {
            message_id: MessageId(id.into()),
            intent: Intent::Question,
            topics: topics.iter().map(|t| Topic((*t).into())).collect(),
            entities: entities.iter().map(|e| Entity((*e).into())).collect(),
            confidence: Confidence::new(confidence),
        }
    }

    fn conversation() -> ConversationId {
        ConversationId("c-1".into())
    }

    #[test]
    fn same_projections_scope_identically() {
        let scoper = MockScopeProjector::new();
        let projections = vec![projection("m-1", &["auth", "sdk"], &["refresh-token"], 0.9)];
        assert_eq!(
            scoper.scope(&conversation(), &projections),
            scoper.scope(&conversation(), &projections)
        );
    }

    #[test]
    fn detected_components_load_others_ignore_and_are_disjoint() {
        let scoper = MockScopeProjector::new();
        let projections = vec![projection("m-1", &["auth", "sdk"], &["refresh-token"], 0.8)];
        let scope = scoper.scope(&conversation(), &projections);

        assert!(scope.load.contains(&Component("auth-sdk".into())));
        assert!(scope.load.contains(&Component("token-refresh".into())));
        assert!(scope.ignore.contains(&Component("web-ui".into())));
        for component in &scope.load {
            assert!(
                !scope.ignore.contains(component),
                "load and ignore disjoint"
            );
        }
    }

    #[test]
    fn empty_session_yields_empty_load_and_zero_confidence() {
        let scoper = MockScopeProjector::new();
        let scope = scoper.scope(&conversation(), &[]);
        assert!(scope.load.is_empty());
        assert_eq!(scope.ignore.len(), CATALOG.len());
        assert_eq!(scope.confidence.get(), 0.0);
    }
}
