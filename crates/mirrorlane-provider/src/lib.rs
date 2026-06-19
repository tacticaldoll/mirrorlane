//! Projection providers for Mirrorlane.
//!
//! Deterministic mocks for each pipeline stage: [`MockProjector`] (message →
//! projection), [`MockScopeProjector`] (session → scope), and
//! [`MockWarmupBuilder`] (session → warm-up). Real SLM providers (e.g. Ollama)
//! implement the same `mirrorlane_core` ports.
//!
//! [`MessageSkillBuilder`] (message → skill index) is the real deterministic
//! builder, not a mock: expertise from messages is a pure aggregation.
//!
//! [`RuleRouter`] (projection → routing decision) is deterministic too;
//! [`RecordingConsumer`] is a sink that records what it received.
//! [`SkillRoutingHinter`] (projection + skill index → routing hint) derives the
//! reviewer candidates and human routing hint for a routed message.
//! [`SkillDeveloperSnapshotter`] (participants + skill index → session
//! developers) derives who is in a session and which session topics they own.
//!
//! [`CachingProjector`] wraps any `Projector` with a `ProjectionCache` so a
//! non-deterministic inner projector is called once per input and replay reads
//! the frozen result.

mod caching;
mod developer;
mod hint;
mod mock;
mod routing;
mod scope;
mod skill;
mod warmup;

pub use caching::CachingProjector;
pub use developer::SkillDeveloperSnapshotter;
pub use hint::SkillRoutingHinter;
pub use mock::MockProjector;
pub use routing::{RecordingConsumer, RuleRouter};
pub use scope::MockScopeProjector;
pub use skill::MessageSkillBuilder;
pub use warmup::MockWarmupBuilder;
