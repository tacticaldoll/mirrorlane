//! Projection caching as an instance of the generic runtime cache.
//!
//! The semantic cache itself is generic and lives in `mirrorlane-runtime`
//! ([`Cache`], [`CacheKey`], [`Cached`]). Here it is specialized to the projection
//! workload: a [`MessageEnvelope`] supplies its cache key (a hash of its id **and
//! body**, so editing a message in place — same id, new body — misses rather than
//! serving a stale projection), and [`InMemoryProjectionCache`] is the in-memory
//! cache for `Projection`s.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use mirrorlane_runtime::{CacheKey, InMemoryCache};

use crate::message::MessageEnvelope;
use crate::projection::Projection;

impl CacheKey for MessageEnvelope {
    fn cache_key(&self) -> String {
        // Key on id *and* body: the message store replaces in place by id, so keying
        // on id alone would serve a stale projection after an in-place body edit.
        let mut hasher = DefaultHasher::new();
        self.id.0.hash(&mut hasher);
        self.body.hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    }
}

/// An in-memory projection cache: the generic in-memory cache, specialized to
/// [`Projection`]. Memoizes projector output keyed by a projector version and a
/// message id.
pub type InMemoryProjectionCache = InMemoryCache<Projection>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{Author, AuthorId, Conversation, ConversationId, MessageId, Source};
    use crate::projection::{Confidence, Intent};
    use mirrorlane_runtime::{Cache, StepVersion};

    fn message(id: &str) -> MessageEnvelope {
        MessageEnvelope {
            id: MessageId(id.into()),
            source: Source::Manual,
            author: Author {
                id: AuthorId("u-1".into()),
                display_name: "Dev".into(),
            },
            conversation: Conversation {
                id: ConversationId("c-1".into()),
                thread: None,
            },
            body: "hello".into(),
        }
    }

    fn projection(id: &str) -> Projection {
        Projection {
            message_id: MessageId(id.into()),
            intent: Intent::Social,
            topics: Vec::new(),
            entities: Vec::new(),
            confidence: Confidence::new(0.5),
        }
    }

    #[test]
    fn message_cache_key_covers_id_and_body() {
        // Stable for the same id+body, so a re-run hits.
        assert_eq!(message("m-1").cache_key(), message("m-1").cache_key());
        // Same id, different body → different key, so an in-place edit misses rather
        // than serving a stale projection.
        let mut edited = message("m-1");
        edited.body = "hello, edited".into();
        assert_ne!(message("m-1").cache_key(), edited.cache_key());
    }

    #[test]
    fn projection_cache_round_trips_and_misses_on_version() {
        let cache = InMemoryProjectionCache::new();
        let v1 = StepVersion::new("v1");
        let key = message("m-1").cache_key();
        cache.put("proj", &v1, &key, projection("m-1"));
        assert_eq!(cache.get("proj", &v1, &key), Some(projection("m-1")));
        assert!(cache.get("proj", &StepVersion::new("v2"), &key).is_none());
    }
}
