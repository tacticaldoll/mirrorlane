//! Mirrorlane durable storage adapters.
//!
//! The in-memory stores in `mirrorlane-core` have durable counterparts here:
//! [`SqliteMessageStore`] (the message log) and
//! [`SqliteProjectionCache`] (frozen projector output). Because replay
//! re-derives the downstream stores from the log, persisting the log gives
//! cross-restart recovery; persisting the cache keeps replay deterministic over
//! a non-deterministic projector.

mod sqlite_common;
mod sqlite_derived_output_cache;
mod sqlite_message_store;
mod sqlite_projection_cache;
mod sqlite_routing_trace_store;

pub use sqlite_derived_output_cache::SqliteDerivedOutputCache;
pub use sqlite_message_store::SqliteMessageStore;
pub use sqlite_projection_cache::SqliteProjectionCache;
pub use sqlite_routing_trace_store::SqliteRoutingTraceStore;
