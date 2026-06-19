//! The per-conversation derived-output unit and the durable cache port for it.
//!
//! [`ConversationDerivation`] is the reproducible, consumable derivation for one
//! conversation — the part of a session's context that is a deterministic function
//! of the log. It is built only from core types, so the spine and the storage crate
//! can hold it; the read-time routing decisions and GitHub drafts a full session
//! context also carries are re-derived from its `projections` at read time (they
//! pull in non-core types and dispatch nothing).
//!
//! [`DerivedOutputCache`] is the port for caching it. Per the
//! [design rationale](../../../docs/architecture.md), a persisted
//! derivation is a *cache* of a deterministic computation, never a store of record:
//! a lookup under a different derivation version or a different content hash misses,
//! so the output is recomputed by replay.

use std::collections::HashMap;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::StepVersion;
use crate::message::ConversationId;
use crate::projection::Projection;
use crate::routing::RoutingHint;
use crate::scope::Scope;
use crate::skill::SessionDevelopers;
use crate::warmup::WarmupDocument;

/// The version of mirrorlane's derivation code. Bump it whenever any derivation
/// step's behavior or the pipeline composition changes, so cached output the old
/// code produced misses and is recomputed. The five non-projector builder ports
/// carry no runtime version of their own, so this single knob covers them; the
/// projector contributes its own version on top (see [`derivation_version`]).
pub const DERIVATION_SCHEMA_VERSION: &str = "1";

/// Compose the version that keys the [`DerivedOutputCache`]: the schema version,
/// the strategy id (so different strategies never collide on one entry), and the
/// projector's runtime [`StepVersion`] (so a model/prompt change misses).
pub fn derivation_version(strategy: &str, projector: &StepVersion) -> StepVersion {
    StepVersion::new(format!(
        "{DERIVATION_SCHEMA_VERSION}:{strategy}:{}",
        projector.as_str()
    ))
}

/// The reproducible derived output for one conversation: its projections plus the
/// scope, warm-up, developers, and routing hints derived from them. Excludes the
/// read-time routing decisions and GitHub drafts, which are re-derived from
/// `projections` when assembling a full session context.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConversationDerivation {
    pub conversation: ConversationId,
    pub projections: Vec<Projection>,
    pub scope: Option<Scope>,
    pub warmup: WarmupDocument,
    pub developers: Option<SessionDevelopers>,
    pub hints: Vec<RoutingHint>,
}

/// A durable cache of [`ConversationDerivation`], keyed by a derivation version, a
/// conversation, and a hash of the conversation's message content. A change to the
/// version (e.g. the projector) or the content misses, so the cache never serves
/// output for a stale message set — it is a delivery optimization over replay, not a
/// source of truth.
pub trait DerivedOutputCache: Send + Sync {
    /// Fetch the cached derivation for `(version, conversation, content)`.
    fn get(
        &self,
        version: &StepVersion,
        conversation: &ConversationId,
        content: &str,
    ) -> Option<ConversationDerivation>;

    /// Cache `value` under `(version, conversation, content)`.
    fn put(
        &self,
        version: &StepVersion,
        conversation: &ConversationId,
        content: &str,
        value: ConversationDerivation,
    );

    /// Reclaim any cached output for `conversation` superseded by the current
    /// `(keep_version, keep_content)` — best-effort delivery-cycle cleanup of
    /// stale-by-key rows. The default is a no-op; only a durable cache that grows on
    /// disk (the SQLite adapter) overrides it. Pruning never removes a row a later
    /// read could hit, so a failed reclamation only leaves dead rows behind — it
    /// never affects correctness.
    fn reclaim_superseded(
        &self,
        conversation: &ConversationId,
        keep_version: &StepVersion,
        keep_content: &str,
    ) {
        let _ = (conversation, keep_version, keep_content);
    }
}

/// An in-memory [`DerivedOutputCache`] backed by a `HashMap`, for tests and
/// ephemeral use. The durable counterpart is `SqliteDerivedOutputCache`.
#[derive(Default)]
pub struct InMemoryDerivedOutputCache {
    inner: Mutex<HashMap<(String, String, String), ConversationDerivation>>,
}

impl InMemoryDerivedOutputCache {
    /// Create an empty cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// A tuple key — not a `:`-joined string — so components that contain `:` (the
    /// derivation version is `{schema}:{strategy}:{projector}`; a conversation id
    /// can be `github:owner/repo`) cannot alias the way a concatenation would.
    fn key(
        version: &StepVersion,
        conversation: &ConversationId,
        content: &str,
    ) -> (String, String, String) {
        (
            version.as_str().to_string(),
            conversation.0.clone(),
            content.to_string(),
        )
    }
}

impl DerivedOutputCache for InMemoryDerivedOutputCache {
    fn get(
        &self,
        version: &StepVersion,
        conversation: &ConversationId,
        content: &str,
    ) -> Option<ConversationDerivation> {
        self.inner
            .lock()
            .expect("derived-output cache mutex poisoned")
            .get(&Self::key(version, conversation, content))
            .cloned()
    }

    fn put(
        &self,
        version: &StepVersion,
        conversation: &ConversationId,
        content: &str,
        value: ConversationDerivation,
    ) {
        self.inner
            .lock()
            .expect("derived-output cache mutex poisoned")
            .insert(Self::key(version, conversation, content), value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_varies_with_strategy_and_projector_and_embeds_schema() {
        let p1 = StepVersion::new("p1");
        let p2 = StepVersion::new("p2");
        assert_ne!(
            derivation_version("projection", &p1).as_str(),
            derivation_version("custom", &p1).as_str(),
            "different strategies must not collide"
        );
        assert_ne!(
            derivation_version("projection", &p1).as_str(),
            derivation_version("projection", &p2).as_str(),
            "a projector version change must miss"
        );
        assert!(
            derivation_version("projection", &p1)
                .as_str()
                .starts_with(DERIVATION_SCHEMA_VERSION),
            "the schema version is part of the key"
        );
    }

    fn derivation(conversation: &str) -> ConversationDerivation {
        ConversationDerivation {
            conversation: ConversationId(conversation.into()),
            projections: Vec::new(),
            scope: None,
            warmup: WarmupDocument {
                conversation: ConversationId(conversation.into()),
                focus: Vec::new(),
                decisions: Vec::new(),
                open_questions: Vec::new(),
                tasks: Vec::new(),
                summary: "s".into(),
            },
            developers: None,
            hints: Vec::new(),
        }
    }

    #[test]
    fn in_memory_cache_does_not_alias_on_delimiters() {
        // Components contain `:` — the version is `{schema}:{strategy}:{projector}`
        // and a conversation id can be `github:owner/repo`. A tuple key keeps tuples
        // that a `:`-joined string would have aliased distinct.
        let cache = InMemoryDerivedOutputCache::new();
        let version = StepVersion::new("1:projection:v1");
        let conv = ConversationId("github:acme/widgets".into());
        cache.put(&version, &conv, "hash", derivation("github:acme/widgets"));

        assert!(
            cache
                .get(
                    &StepVersion::new("1:projection"),
                    &ConversationId("v1:github:acme/widgets".into()),
                    "hash",
                )
                .is_none(),
            "a different version/conversation split must not alias"
        );
        assert_eq!(
            cache.get(&version, &conv, "hash"),
            Some(derivation("github:acme/widgets"))
        );
    }
}
