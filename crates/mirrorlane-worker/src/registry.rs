//! Runtime, data-driven **selection** of a strategy.
//!
//! [`Strategy`](crate::Strategy) has associated types (`Input`/`Output`) and so is
//! not object-safe — `Arc<dyn Strategy>` does not compile, so a strategy cannot be
//! chosen behind a trait object. [`ReplayStrategy`] is the **object-safe erased
//! domain shape** of `Strategy` for the projection domain
//! (`dyn MessageStore -> ReplayStores`); a blanket impl makes every matching
//! `Strategy` (e.g. [`ProjectionStrategy`]) a `ReplayStrategy` for free.
//!
//! [`StrategyRegistry`] maps a data-supplied id (from a flag or config) to a
//! factory that builds a runnable strategy from a [`StrategyContext`]. The id is
//! the data that selects the strategy; the factory body stays typed Rust. Making
//! the strategy's *internal* composition data — and accepting externally submitted
//! jobs — are later changes; this one makes only the *choice* data-driven.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use mirrorlane_core::{
    DeveloperSnapshotBuilder, InMemoryDeveloperSnapshotStore, InMemoryProjectionStore,
    InMemoryRoutingHintStore, InMemoryScopeStore, InMemorySkillStore, InMemoryWarmupStore,
    MessageStore, Projector, RoutingHinter, ScopeProjector, SkillBuilder, WarmupBuilder,
};
use worklane::async_trait;

use crate::ProjectionStrategy;
use crate::replay::ReplayStores;
use crate::strategy::Strategy;

/// The object-safe, erased domain shape of [`Strategy`] for the projection domain
/// (`dyn MessageStore -> ReplayStores`), so a strategy can sit behind
/// `Arc<dyn ReplayStrategy>` and be selected at runtime. Every `Strategy` over that
/// domain shape satisfies it via the blanket impl below, so the reference
/// [`ProjectionStrategy`] is usable as a `ReplayStrategy` with no per-type code.
#[async_trait]
pub trait ReplayStrategy: Send + Sync {
    /// Run the strategy over a message log, producing the replay stores. The log
    /// object is `'static` to match `Strategy`'s `Input = dyn MessageStore`; every
    /// message store in the codebase is an owned `'static` type, borrowed here.
    async fn run(&self, log: &(dyn MessageStore + 'static)) -> ReplayStores;
}

#[async_trait]
impl<S> ReplayStrategy for S
where
    S: Strategy<Input = dyn MessageStore, Output = ReplayStores>,
{
    async fn run(&self, log: &(dyn MessageStore + 'static)) -> ReplayStores {
        Strategy::run(self, log).await
    }
}

/// A trivial second built-in strategy: it composes no `Step`s and returns empty
/// stores. It exists to prove the registry *selects* by data — choosing it over
/// projection is purely a matter of the requested id, same context either way.
pub struct EmptyStrategy;

#[async_trait]
impl Strategy for EmptyStrategy {
    type Input = dyn MessageStore;
    type Output = ReplayStores;

    async fn run(&self, _log: &dyn MessageStore) -> ReplayStores {
        ReplayStores {
            projections: Arc::new(InMemoryProjectionStore::new()),
            scopes: Arc::new(InMemoryScopeStore::new()),
            warmups: Arc::new(InMemoryWarmupStore::new()),
            skills: Arc::new(InMemorySkillStore::new()),
            hints: Arc::new(InMemoryRoutingHintStore::new()),
            developers: Arc::new(InMemoryDeveloperSnapshotStore::new()),
        }
    }
}

/// The resolved derivation ports a built-in strategy wires from. The caller (e.g.
/// the CLI, from its resolved provider/endpoint settings) builds these; carrying
/// `mirrorlane-core` port trait objects keeps strategy selection free of any
/// provider or CLI types.
#[derive(Clone)]
pub struct StrategyContext {
    pub projector: Arc<dyn Projector>,
    pub scoper: Arc<dyn ScopeProjector>,
    pub builder: Arc<dyn WarmupBuilder>,
    pub skill_builder: Arc<dyn SkillBuilder>,
    pub hinter: Arc<dyn RoutingHinter>,
    pub snapshotter: Arc<dyn DeveloperSnapshotBuilder>,
}

/// A requested strategy id that was never registered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownStrategy(pub String);

impl fmt::Display for UnknownStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown strategy {:?}", self.0)
    }
}

impl std::error::Error for UnknownStrategy {}

type StrategyFactory = Box<dyn Fn(&StrategyContext) -> Arc<dyn ReplayStrategy> + Send + Sync>;

/// Maps a strategy id to a factory that builds the strategy from a
/// [`StrategyContext`]. A factory (not a prebuilt instance) because a strategy's
/// ports depend on the caller's resolved inputs, supplied at `build` time.
pub struct StrategyRegistry {
    factories: BTreeMap<String, StrategyFactory>,
}

impl StrategyRegistry {
    /// The default strategy id — the reference projection pipeline.
    pub const DEFAULT: &'static str = "projection";

    /// A registry with the built-in strategies: `projection` (the default
    /// reference strategy) and `empty`.
    pub fn with_builtins() -> Self {
        let mut registry = Self {
            factories: BTreeMap::new(),
        };
        registry.register(Self::DEFAULT, |ctx| {
            Arc::new(ProjectionStrategy::new(
                ctx.projector.clone(),
                ctx.scoper.clone(),
                ctx.builder.clone(),
                ctx.skill_builder.clone(),
                ctx.hinter.clone(),
                ctx.snapshotter.clone(),
            ))
        });
        registry.register("empty", |_ctx| Arc::new(EmptyStrategy));
        registry
    }

    /// Register a strategy factory under an id, replacing any prior registration.
    pub fn register<F>(&mut self, id: &str, factory: F)
    where
        F: Fn(&StrategyContext) -> Arc<dyn ReplayStrategy> + Send + Sync + 'static,
    {
        self.factories.insert(id.to_string(), Box::new(factory));
    }

    /// Build the strategy registered under `id`, wired from `ctx`. An unregistered
    /// id is an [`UnknownStrategy`] error, not a panic.
    pub fn build(
        &self,
        id: &str,
        ctx: &StrategyContext,
    ) -> Result<Arc<dyn ReplayStrategy>, UnknownStrategy> {
        match self.factories.get(id) {
            Some(factory) => Ok(factory(ctx)),
            None => Err(UnknownStrategy(id.to_string())),
        }
    }
}

impl Default for StrategyRegistry {
    fn default() -> Self {
        Self::with_builtins()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_support::{context, seeded_log};

    #[tokio::test]
    async fn default_id_yields_the_projection_pipeline() {
        let log = seeded_log();
        let strategy = StrategyRegistry::with_builtins()
            .build(StrategyRegistry::DEFAULT, &context())
            .expect("projection is registered");
        let stores = strategy.run(log.as_ref()).await;
        assert_eq!(
            stores.projections.len(),
            log.len(),
            "projection derives one projection per message"
        );
    }

    #[tokio::test]
    async fn a_different_id_yields_a_different_strategy() {
        let log = seeded_log();
        let strategy = StrategyRegistry::with_builtins()
            .build("empty", &context())
            .expect("empty is registered");
        let stores = strategy.run(log.as_ref()).await;
        assert_eq!(
            stores.projections.len(),
            0,
            "the empty strategy derives nothing — selection, not projection, ran"
        );
    }

    #[test]
    fn an_unknown_id_is_an_error() {
        let result = StrategyRegistry::with_builtins().build("nope", &context());
        assert_eq!(result.err(), Some(UnknownStrategy("nope".into())));
    }

    #[tokio::test]
    async fn a_registered_strategy_is_selected_by_its_id() {
        let log = seeded_log();
        let mut registry = StrategyRegistry::with_builtins();
        registry.register("also-empty", |_ctx| Arc::new(EmptyStrategy));
        let stores = registry
            .build("also-empty", &context())
            .expect("registered")
            .run(log.as_ref())
            .await;
        assert_eq!(stores.projections.len(), 0);
    }
}
