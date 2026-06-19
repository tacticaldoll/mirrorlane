//! A caching projector decorator.
//!
//! `CachingProjector` is a [`Cached`] instance over the projection workload: it
//! wraps an inner projector with a [`Cache`] and a version tag, so the inner
//! projector is invoked at most once per `(version, message id)`. This is what
//! makes a non-deterministic inner projector (e.g. an SLM) replay-safe: the first
//! projection is frozen in the cache and every later projection — and every
//! replay — reads it back.
//!
//! The **version tag must encode the inner projector's identity and
//! configuration** (model, prompt). Changing it invalidates prior entries.

use std::sync::Arc;

use mirrorlane_core::message::MessageEnvelope;
use mirrorlane_core::projection::Projection;
use mirrorlane_runtime::{Cache, Cached, Step, StepVersion};

/// The inner projector as a cacheable [`Step`] held behind a trait object. A
/// concrete projector coerces straight into this; the runtime's blanket impl
/// makes the trait object itself a `Step`.
type InnerProjector = Arc<dyn Step<In = MessageEnvelope, Out = Projection>>;

/// A `Projector` that memoizes an inner projector's output in a [`Cache`], as a
/// [`Cached`] instance specialized to `Projection`.
pub struct CachingProjector {
    inner: Cached<InnerProjector>,
}

impl CachingProjector {
    /// Wrap `inner` with `cache`, tagging entries with `version`.
    ///
    /// `version` must change whenever the inner projector's output could change
    /// (e.g. a different model or prompt), so stale entries are invalidated.
    pub fn new(
        inner: InnerProjector,
        cache: Arc<dyn Cache<Projection>>,
        version: impl Into<String>,
    ) -> Self {
        Self {
            inner: Cached::new(inner, cache, StepVersion::new(version)),
        }
    }
}

impl Step for CachingProjector {
    type In = MessageEnvelope;
    type Out = Projection;

    fn kind(&self) -> &'static str {
        "mirrorlane.projection.cached"
    }

    fn version(&self) -> StepVersion {
        self.inner.version()
    }

    fn run(&self, message: &MessageEnvelope) -> Projection {
        self.inner.run(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use mirrorlane_core::message::{
        Author, AuthorId, Conversation, ConversationId, MessageId, Source,
    };
    use mirrorlane_core::projection::{Confidence, Intent};
    use mirrorlane_core::{InMemoryProjectionCache, Projector};

    /// A projector that counts how many times it is invoked.
    struct CountingProjector {
        calls: AtomicUsize,
    }

    impl CountingProjector {
        fn new() -> Self {
            Self {
                calls: AtomicUsize::new(0),
            }
        }
    }

    impl Step for CountingProjector {
        type In = MessageEnvelope;
        type Out = Projection;

        fn kind(&self) -> &'static str {
            "test.counting"
        }

        fn version(&self) -> StepVersion {
            StepVersion::new("counting:v1")
        }

        fn run(&self, message: &MessageEnvelope) -> Projection {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Projection {
                message_id: message.id.clone(),
                intent: Intent::Social,
                topics: Vec::new(),
                entities: Vec::new(),
                confidence: Confidence::new(0.5),
            }
        }
    }

    fn message() -> MessageEnvelope {
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
            body: "hello".into(),
        }
    }

    #[test]
    fn inner_runs_only_on_a_miss() {
        let inner = Arc::new(CountingProjector::new());
        let cache = Arc::new(InMemoryProjectionCache::new());
        let projector = CachingProjector::new(inner.clone(), cache, "v1");

        let first = projector.project(&message());
        let second = projector.project(&message());

        assert_eq!(inner.calls.load(Ordering::SeqCst), 1, "inner invoked once");
        assert_eq!(first, second, "both calls return the same projection");
    }

    #[test]
    fn a_changed_version_recomputes() {
        let inner = Arc::new(CountingProjector::new());
        let cache: Arc<InMemoryProjectionCache> = Arc::new(InMemoryProjectionCache::new());

        CachingProjector::new(inner.clone(), cache.clone(), "v1").project(&message());
        CachingProjector::new(inner.clone(), cache, "v2").project(&message());

        assert_eq!(
            inner.calls.load(Ordering::SeqCst),
            2,
            "each version invokes the inner projector"
        );
    }
}
