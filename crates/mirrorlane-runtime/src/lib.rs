//! Mirrorlane runtime spine: the generic [`Step`] abstraction.
//!
//! This is the innermost crate. It is **domain-free** and **infrastructure-free**:
//! it depends on no other `mirrorlane-*` crate, on no domain type, and on no async
//! runtime, broker, storage, or `worklane`. Every other crate may depend inward
//! toward it.
//!
//! A [`Step`] is a typed, synchronous, infallible unit of AI work — the unit the
//! runtime caches, replays, and routes. The projection pipeline expresses its
//! message projector as a `Step`. The first such primitive lives here: the generic
//! semantic cache ([`Cache`], [`CacheKey`], [`Cached`]). Deterministic replay,
//! trace, and routing generalize off `Step` in later changes.

mod cache;
mod step;

pub use cache::{Cache, CacheKey, Cached, InMemoryCache};
pub use step::{Step, StepVersion};
