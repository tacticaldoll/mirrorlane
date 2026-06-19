//! Mirrorlane core domain model.
//!
//! The domain data types and ports live here. This crate is I/O-free and depends
//! on no async runtime, broker, storage, or Worklane crate; dependencies point
//! inward toward it.
//!
//! It models the projection pipeline — message and projection, scope, skill,
//! warm-up, and routing — each with a deterministic port and an in-memory store.

pub mod message;
pub mod projection;
pub mod routing;
pub mod scope;
pub mod skill;
pub mod warmup;

mod cache;
mod derivation;
mod message_log;
mod projector;
mod store;

pub use cache::InMemoryProjectionCache;
pub use derivation::{
    ConversationDerivation, DERIVATION_SCHEMA_VERSION, DerivedOutputCache,
    InMemoryDerivedOutputCache, derivation_version,
};
pub use message_log::{InMemoryMessageStore, MessageStore};
pub use mirrorlane_runtime::{Cache, CacheKey, InMemoryCache, Step, StepVersion};
pub use projector::{
    DeveloperSnapshotBuilder, Projector, Router, RoutingHinter, ScopeProjector, SkillBuilder,
    WarmupBuilder,
};
pub use routing::{Consumer, ConsumerError, ConsumerRegistry};
pub use store::{
    DeveloperSnapshotStore, InMemoryDeveloperSnapshotStore, InMemoryProjectionStore,
    InMemoryRoutingHintStore, InMemoryRoutingStore, InMemoryRoutingTraceStore, InMemoryScopeStore,
    InMemorySkillStore, InMemoryWarmupStore, ProjectionStore, RoutingHintStore, RoutingStore,
    RoutingTraceStore, ScopeStore, SkillStore, WarmupStore,
};
