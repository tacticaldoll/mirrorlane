//! The inbound submission surface: a typed Worklane job that runs a strategy.
//!
//! This is the symmetric counterpart to [`routed_work`](crate::routed_work) (the
//! outbound edge). Where `RoutedWork` lets Mirrorlane enqueue downstream work,
//! [`StrategyRunJob`] lets an **external** plane — Triggerlane — submit a strategy
//! run to Mirrorlane: it enqueues a job naming a strategy by id onto
//! [`STRATEGY_RUN_LANE`], and a Mirrorlane worker consuming the lane resolves the
//! id through the [`StrategyRegistry`] and runs it. An unknown id **fails** the job
//! (Worklane retry → dead-letter), it never panics. [`StrategyRunDeadLetters`] is
//! the operability surface.
//!
//! The handler runs the resolved strategy and **publishes its output** to the
//! derived-output cache (the same read model the in-process replay populates),
//! keyed by the composed derivation version — so a submitted run leaves consumable
//! output a later read can serve.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use mirrorlane_core::{DerivedOutputCache, MessageStore, derivation_version};
use worklane::{Broker, HandlerResult, Job, JobContext, Lane, async_trait};

use crate::dead_letters::DeadLetters;
use crate::derived::populate_cache;
use crate::{StrategyContext, StrategyRegistry};

/// The lane strategy-run jobs are submitted to and dead-lettered on.
pub const STRATEGY_RUN_LANE: &str = "mirrorlane.strategy_run";

/// [`STRATEGY_RUN_LANE`] as a validated [`Lane`]. Worklane models a lane as a
/// first-class type built through a fallible conversion, so this is the crate's
/// single point that turns the name constant into one. It cannot fail in
/// practice — the name is a valid portable lane — so the conversion is unwrapped.
pub fn strategy_run_lane() -> Lane {
    STRATEGY_RUN_LANE
        .parse()
        .expect("STRATEGY_RUN_LANE is a valid lane name")
}

/// The payload of a strategy-run submission: the strategy to run, named by its
/// registry id. Kept minimal and serializable — the input is the worker's durable
/// log, so the request carries only the selection. A richer input selector is a
/// later concern.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyRunRequest {
    pub strategy: String,
}

/// The typed Worklane job a submitted strategy run becomes. It carries its
/// dependencies — the durable log to run over, the resolved derivation ports, the
/// registry of available strategies, and the derived-output cache to populate — so
/// the handler resolves the submitted id, runs it, and publishes its output.
pub struct StrategyRunJob {
    store: Arc<dyn MessageStore>,
    context: StrategyContext,
    registry: StrategyRegistry,
    cache: Arc<dyn DerivedOutputCache>,
}

impl StrategyRunJob {
    /// Build the job over a durable log, the resolved ports, an explicit registry of
    /// available strategies (so a caller can register its own), and the
    /// derived-output cache a consumed run populates.
    pub fn new(
        store: Arc<dyn MessageStore>,
        context: StrategyContext,
        registry: StrategyRegistry,
        cache: Arc<dyn DerivedOutputCache>,
    ) -> Self {
        Self {
            store,
            context,
            registry,
            cache,
        }
    }

    /// Build the job with the built-in strategies (`projection`, `empty`).
    pub fn with_builtins(
        store: Arc<dyn MessageStore>,
        context: StrategyContext,
        cache: Arc<dyn DerivedOutputCache>,
    ) -> Self {
        Self::new(store, context, StrategyRegistry::with_builtins(), cache)
    }
}

#[async_trait]
impl Job for StrategyRunJob {
    type Payload = StrategyRunRequest;
    type Output = ();
    const KIND: &'static str = "mirrorlane.strategy_run";

    async fn run(&self, _ctx: JobContext, request: StrategyRunRequest) -> HandlerResult<()> {
        // An unknown id is a failed job (retried, then dead-lettered), not a panic.
        let strategy = self.registry.build(&request.strategy, &self.context)?;
        let stores = strategy.run(self.store.as_ref()).await;
        // Publish the run's output to the derived-output cache (the same read model
        // the in-process replay populates), keyed by the composed derivation version.
        let version = derivation_version(&request.strategy, &self.context.projector.version());
        populate_cache(self.store.as_ref(), &stores, self.cache.as_ref(), &version);
        Ok(())
    }
}

/// The dead-letter operability surface for the strategy-run lane: read, count,
/// requeue, and purge jobs that exhausted their attempts (e.g. an unknown strategy
/// id). A thin newtype over [`DeadLetters`](crate::DeadLetters) bound to the
/// strategy-run lane — the operations live there; this keeps the lane's semantic
/// name.
pub struct StrategyRunDeadLetters(DeadLetters);

impl StrategyRunDeadLetters {
    /// Build the surface over the broker backing the strategy-run lane.
    pub fn new(broker: Arc<dyn Broker>) -> Self {
        Self(DeadLetters::new(broker, strategy_run_lane()))
    }
}

impl std::ops::Deref for StrategyRunDeadLetters {
    type Target = DeadLetters;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    use mirrorlane_core::message::ConversationId;
    use mirrorlane_core::{InMemoryDerivedOutputCache, derivation_version};
    use worklane::{Client, Worker};
    use worklane_memory::InMemoryBroker;

    use crate::test_support::{context, empty_stores, seeded_log};
    use crate::{ReplayStores, Strategy, content_hash};

    /// A probe strategy that records it ran, so a round-trip can assert the
    /// submission reached and executed a strategy by id — no domain output needed.
    struct RecordingStrategy {
        ran: Arc<AtomicBool>,
    }

    #[async_trait]
    impl Strategy for RecordingStrategy {
        type Input = dyn MessageStore;
        type Output = ReplayStores;
        async fn run(&self, _log: &dyn MessageStore) -> ReplayStores {
            self.ran.store(true, Ordering::SeqCst);
            empty_stores()
        }
    }

    #[test]
    fn request_round_trips() {
        let request = StrategyRunRequest {
            strategy: "projection".into(),
        };
        let json = serde_json::to_string(&request).expect("serialize");
        let back: StrategyRunRequest = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(request, back);
    }

    #[tokio::test]
    async fn a_submitted_job_runs_the_named_strategy() {
        let ran = Arc::new(AtomicBool::new(false));
        let mut registry = StrategyRegistry::with_builtins();
        let flag = ran.clone();
        registry.register("probe", move |_ctx| {
            Arc::new(RecordingStrategy { ran: flag.clone() })
        });

        let broker = Arc::new(InMemoryBroker::new());
        let client = Client::new(broker.clone()).with_lane(strategy_run_lane());
        client
            .enqueue::<StrategyRunJob>(StrategyRunRequest {
                strategy: "probe".into(),
            })
            .await
            .expect("submit");

        let mut worker = Worker::new(broker.clone()).with_lane(strategy_run_lane());
        worker
            .register(StrategyRunJob::new(
                seeded_log(),
                context(),
                registry,
                Arc::new(InMemoryDerivedOutputCache::new()),
            ))
            .expect("register");
        worker
            .build()
            .expect("build worker")
            .run_until_idle()
            .await
            .expect("run");

        assert!(
            ran.load(Ordering::SeqCst),
            "the submission reached and ran the strategy named by its id"
        );
    }

    #[tokio::test]
    async fn a_consumed_run_populates_the_cache() {
        let log = seeded_log();
        let context = context();
        let cache = Arc::new(InMemoryDerivedOutputCache::new());
        let broker = Arc::new(InMemoryBroker::new());
        let client = Client::new(broker.clone()).with_lane(strategy_run_lane());
        client
            .enqueue::<StrategyRunJob>(StrategyRunRequest {
                strategy: StrategyRegistry::DEFAULT.into(),
            })
            .await
            .expect("submit");

        let mut worker = Worker::new(broker.clone()).with_lane(strategy_run_lane());
        worker
            .register(StrategyRunJob::with_builtins(
                log.clone(),
                context.clone(),
                cache.clone(),
            ))
            .expect("register");
        worker
            .build()
            .expect("build worker")
            .run_until_idle()
            .await
            .expect("run");

        // The consumed run published its output: the conversation's derivation is in
        // the cache, keyed by the same composed version and content hash a reader uses.
        let version = derivation_version(StrategyRegistry::DEFAULT, &context.projector.version());
        let conversation = ConversationId("c-1".into());
        let derivation = cache
            .get(
                &version,
                &conversation,
                &content_hash(log.as_ref(), &conversation),
            )
            .expect("consumed run populated the cache");
        assert_eq!(derivation.projections.len(), log.len());
    }

    #[tokio::test]
    async fn an_unknown_strategy_dead_letters_rather_than_panics() {
        let broker = Arc::new(InMemoryBroker::new());
        // One attempt so the failed job dead-letters immediately on `run_until_idle`.
        let client = Client::new(broker.clone())
            .with_lane(strategy_run_lane())
            .with_max_attempts(1);
        client
            .enqueue::<StrategyRunJob>(StrategyRunRequest {
                strategy: "nope".into(),
            })
            .await
            .expect("submit");

        let mut worker = Worker::new(broker.clone()).with_lane(strategy_run_lane());
        worker
            .register(StrategyRunJob::with_builtins(
                seeded_log(),
                context(),
                Arc::new(InMemoryDerivedOutputCache::new()),
            ))
            .expect("register");
        worker
            .build()
            .expect("build worker")
            .run_until_idle()
            .await
            .expect("run drains the failing job");

        let dead = StrategyRunDeadLetters::new(broker.clone())
            .read(10)
            .await
            .expect("read dead letters");
        assert_eq!(dead.len(), 1, "the unknown-id job dead-letters, not panics");
    }

    #[tokio::test]
    async fn dead_lettered_submissions_are_countable_and_purgeable() {
        let broker = Arc::new(InMemoryBroker::new());
        let client = Client::new(broker.clone())
            .with_lane(strategy_run_lane())
            .with_max_attempts(1);
        client
            .enqueue::<StrategyRunJob>(StrategyRunRequest {
                strategy: "nope".into(),
            })
            .await
            .expect("submit");

        let mut worker = Worker::new(broker.clone()).with_lane(strategy_run_lane());
        worker
            .register(StrategyRunJob::with_builtins(
                seeded_log(),
                context(),
                Arc::new(InMemoryDerivedOutputCache::new()),
            ))
            .expect("register");
        worker
            .build()
            .expect("build worker")
            .run_until_idle()
            .await
            .expect("run drains the failing job");

        let dlq = StrategyRunDeadLetters::new(broker.clone());
        // Count is non-destructive.
        assert_eq!(dlq.count().await.expect("count"), 1);
        assert_eq!(dlq.read(10).await.expect("read").len(), 1);
        // Purge reports the count removed and empties the lane.
        assert_eq!(dlq.purge().await.expect("purge"), 1);
        assert_eq!(dlq.count().await.expect("count after purge"), 0);
    }
}
