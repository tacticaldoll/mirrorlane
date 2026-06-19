//! The generic semantic cache: a [`Cache`] port, an in-memory adapter, the
//! [`CacheKey`] trait, and the [`Cached`] step decorator.
//!
//! `Cached` memoizes a step's output so a non-deterministic step (e.g. an SLM) is
//! run once per `(version, key)` and replay reads the frozen result — keeping
//! replay deterministic relative to the populated cache. The runtime stays
//! **domain-free and serde-free**: values are generic, and durable adapters add
//! serialization in their own crate.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::{Step, StepVersion};

/// A step input that yields a stable string cache key. Equal inputs MUST yield
/// equal keys, so a cache can derive an identity from `Step::In` without the
/// runtime knowing any domain type.
pub trait CacheKey {
    /// The key identifying this input in a cache.
    fn cache_key(&self) -> String;
}

/// Memoizes step output, keyed by a step `kind`, a [`StepVersion`], and a key.
/// A lookup under a different version misses, so bumping a step's version
/// invalidates prior entries.
pub trait Cache<V>: Send + Sync {
    /// Fetch a cached value for `(kind, version, key)`.
    fn get(&self, kind: &str, version: &StepVersion, key: &str) -> Option<V>;

    /// Cache `value` under `(kind, version, key)`.
    fn put(&self, kind: &str, version: &StepVersion, key: &str, value: V);
}

/// An in-memory [`Cache`] backed by a `HashMap`. Uses std only.
pub struct InMemoryCache<V> {
    inner: Mutex<HashMap<(String, String, String), V>>,
}

impl<V> InMemoryCache<V> {
    /// Create an empty cache.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// The number of cached entries.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether the cache holds no entries.
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<(String, String, String), V>> {
        self.inner.lock().expect("cache mutex poisoned")
    }
}

impl<V> Default for InMemoryCache<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V: Clone + Send + Sync> Cache<V> for InMemoryCache<V> {
    fn get(&self, kind: &str, version: &StepVersion, key: &str) -> Option<V> {
        self.lock()
            .get(&(
                kind.to_string(),
                version.as_str().to_string(),
                key.to_string(),
            ))
            .cloned()
    }

    fn put(&self, kind: &str, version: &StepVersion, key: &str, value: V) {
        self.lock().insert(
            (
                kind.to_string(),
                version.as_str().to_string(),
                key.to_string(),
            ),
            value,
        );
    }
}

/// A [`Step`] decorator that memoizes the inner step's output in a [`Cache`].
///
/// The inner step is invoked at most once per `(version, key)`: a hit returns the
/// cached output; a miss runs the inner step and caches its output. On a panic in
/// the inner step nothing is cached, preserving replay-safety. The decorator's
/// `version` MUST encode the inner step's identity (model, prompt, rule set) so a
/// change invalidates prior entries.
pub struct Cached<S: Step> {
    inner: S,
    cache: Arc<dyn Cache<S::Out>>,
    version: StepVersion,
}

impl<S: Step> Cached<S> {
    /// Wrap `inner` with `cache`, tagging entries with `version`.
    pub fn new(inner: S, cache: Arc<dyn Cache<S::Out>>, version: StepVersion) -> Self {
        Self {
            inner,
            cache,
            version,
        }
    }

    /// Wrap `inner`, taking its own `version` as the cache version.
    pub fn with_inner_version(inner: S, cache: Arc<dyn Cache<S::Out>>) -> Self {
        let version = inner.version();
        Self::new(inner, cache, version)
    }
}

impl<S> Step for Cached<S>
where
    S: Step,
    S::In: CacheKey,
    S::Out: Clone,
{
    type In = S::In;
    type Out = S::Out;

    fn kind(&self) -> &'static str {
        self.inner.kind()
    }

    fn version(&self) -> StepVersion {
        self.version.clone()
    }

    fn run(&self, input: &S::In) -> S::Out {
        let key = input.cache_key();
        if let Some(hit) = self.cache.get(self.inner.kind(), &self.version, &key) {
            return hit;
        }
        let out = self.inner.run(input);
        self.cache
            .put(self.inner.kind(), &self.version, &key, out.clone());
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // A domain-free `CacheKey` for the probe: no `Message`/`MessageId` in scope.
    impl CacheKey for u64 {
        fn cache_key(&self) -> String {
            self.to_string()
        }
    }

    /// A non-domain step that counts how many times it runs. Its `In`/`Out` carry
    /// no domain type, so it proves `Cached`/`Cache` are genuinely generic.
    struct CountingDoubler {
        calls: Arc<AtomicUsize>,
    }

    impl Step for CountingDoubler {
        type In = u64;
        type Out = u64;

        fn kind(&self) -> &'static str {
            "probe.doubler"
        }

        fn version(&self) -> StepVersion {
            StepVersion::new("v1")
        }

        fn run(&self, n: &u64) -> u64 {
            self.calls.fetch_add(1, Ordering::SeqCst);
            n * 2
        }
    }

    /// A step that always panics, to prove a failed run caches nothing.
    struct Panicking;

    impl Step for Panicking {
        type In = u64;
        type Out = u64;

        fn kind(&self) -> &'static str {
            "probe.panicking"
        }

        fn version(&self) -> StepVersion {
            StepVersion::new("v1")
        }

        fn run(&self, _n: &u64) -> u64 {
            panic!("inner step failed");
        }
    }

    #[test]
    fn a_panicking_inner_step_caches_nothing() {
        use std::panic::{AssertUnwindSafe, catch_unwind};

        let cache = Arc::new(InMemoryCache::<u64>::new());
        let cached = Cached::new(Panicking, cache.clone(), StepVersion::new("v1"));

        let result = catch_unwind(AssertUnwindSafe(|| cached.run(&21)));
        assert!(result.is_err(), "the inner panic propagates");
        assert_eq!(
            cache.len(),
            0,
            "nothing is cached on a panic, preserving replay-safety"
        );
    }

    #[test]
    fn in_memory_cache_get_put_and_version_miss() {
        let cache = InMemoryCache::<u64>::new();
        let v = StepVersion::new("v1");
        cache.put("k", &v, "key", 42);
        assert_eq!(cache.get("k", &v, "key"), Some(42));
        assert_eq!(cache.get("k", &StepVersion::new("v2"), "key"), None);
    }

    #[test]
    fn inner_runs_only_on_a_miss() {
        let calls = Arc::new(AtomicUsize::new(0));
        let inner = CountingDoubler {
            calls: calls.clone(),
        };
        let cache = Arc::new(InMemoryCache::<u64>::new());
        let cached = Cached::new(inner, cache, StepVersion::new("v1"));

        assert_eq!(cached.run(&21), 42);
        assert_eq!(cached.run(&21), 42);
        assert_eq!(calls.load(Ordering::SeqCst), 1, "inner runs once per key");
    }

    #[test]
    fn a_changed_version_recomputes() {
        let calls = Arc::new(AtomicUsize::new(0));
        let cache = Arc::new(InMemoryCache::<u64>::new());

        Cached::new(
            CountingDoubler {
                calls: calls.clone(),
            },
            cache.clone(),
            StepVersion::new("v1"),
        )
        .run(&21);
        Cached::new(
            CountingDoubler {
                calls: calls.clone(),
            },
            cache,
            StepVersion::new("v2"),
        )
        .run(&21);

        assert_eq!(calls.load(Ordering::SeqCst), 2, "each version recomputes");
    }

    #[test]
    fn cached_wraps_a_trait_object_step() {
        // `Cached` over a `dyn`-erased step proves the trait-object path used by
        // the projection pipeline (`Arc<dyn Projector>`).
        let inner: Arc<dyn Step<In = u64, Out = u64>> = Arc::new(CountingDoubler {
            calls: Arc::new(AtomicUsize::new(0)),
        });
        let cache = Arc::new(InMemoryCache::<u64>::new());
        let cached = Cached::new(inner, cache, StepVersion::new("v1"));
        assert_eq!(cached.run(&5), 10);
        assert_eq!(cached.run(&5), 10);
    }
}
