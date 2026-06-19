//! The [`Step`]: a typed, synchronous, infallible unit of AI work.

use std::sync::Arc;

/// The identity-version of a [`Step`]: it changes whenever the step's output
/// could change (a different model, prompt, or rule set), so that anything keyed
/// on a step — a semantic cache, a replay trace — invalidates stale results.
///
/// A plain string keeps the runtime dependency-free; the cache that consumes it
/// arrives in a later change.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StepVersion(pub String);

impl StepVersion {
    /// Build a version from anything string-like.
    pub fn new(version: impl Into<String>) -> Self {
        Self(version.into())
    }

    /// The version as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A typed unit of AI work: it maps an input to an output, identified by a stable
/// `kind` and a [`StepVersion`].
///
/// A `Step` is **synchronous and infallible** at its port. This carries forward
/// the settled projector decision: a semantic cache makes the live (model) call
/// miss-only, so async buys nothing at replay time; failures **panic at the
/// boundary** rather than returning, so no result is produced — and therefore
/// nothing is cached — for a failed run. The durable execution that turns a panic
/// into a retry then a dead-letter is a worker concern, not the `Step`'s.
///
/// `In`/`Out` are associated types and reference no domain type, so the runtime
/// stays generic. `kind`/`version` are methods (not associated constants) so the
/// trait remains object-safe: callers hold steps behind `dyn` (e.g. a projector
/// behind `Arc<dyn Projector>`).
pub trait Step: Send + Sync {
    /// The input the step consumes.
    type In;
    /// The output the step produces.
    type Out;

    /// A stable identifier for this kind of step (e.g. `"mirrorlane.projection"`).
    fn kind(&self) -> &'static str;

    /// The step's identity-version; bump it whenever the output could change.
    fn version(&self) -> StepVersion;

    /// Run the step over a borrowed input, producing an owned output.
    fn run(&self, input: &Self::In) -> Self::Out;
}

/// A step held behind a trait object is itself a [`Step`], so a `dyn`-erased step
/// (e.g. a projector behind `Arc<dyn Projector>`) can be wrapped by a decorator
/// such as the semantic cache. `dyn Step<..>` is `Send + Sync` via `Step`'s
/// supertraits, so `Arc<dyn Step<..>>` satisfies `Step`'s own `Send + Sync` bound.
impl<I, O> Step for Arc<dyn Step<In = I, Out = O>> {
    type In = I;
    type Out = O;

    fn kind(&self) -> &'static str {
        (**self).kind()
    }

    fn version(&self) -> StepVersion {
        (**self).version()
    }

    fn run(&self, input: &I) -> O {
        (**self).run(input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A probe step whose input and output carry **no domain type** (no `Message`,
    /// `MessageId`, or `Projection` — none are even in scope in this crate). It
    /// exists solely to prove `Step` is genuinely generic and not
    /// projection-shaped-with-a-new-name. It is a standing genericity guard.
    struct Doubler;

    impl Step for Doubler {
        type In = u64;
        type Out = u64;

        fn kind(&self) -> &'static str {
            "probe.doubler"
        }

        fn version(&self) -> StepVersion {
            StepVersion::new("v1")
        }

        fn run(&self, input: &u64) -> u64 {
            input * 2
        }
    }

    #[test]
    fn a_domain_free_step_runs() {
        let step = Doubler;
        assert_eq!(step.run(&21), 42);
        assert_eq!(step.kind(), "probe.doubler");
        assert_eq!(step.version(), StepVersion::new("v1"));
    }

    #[test]
    fn step_is_object_safe() {
        // Forming `dyn Step<..>` proves the trait is object-safe with pinned
        // associated types — the property that lets `Projector` (a `Step` view)
        // live behind `Arc<dyn Projector>`.
        let step: Box<dyn Step<In = u64, Out = u64>> = Box::new(Doubler);
        assert_eq!(step.run(&5), 10);
    }
}
