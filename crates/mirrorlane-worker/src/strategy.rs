//! The `Strategy` abstraction: a composition of `Step`s, run asynchronously.
//!
//! A [`Strategy`] turns a typed input into a typed output by composing `Step`s and
//! running them with the runtime's glass-box guarantees (semantic cache,
//! idempotent persistence, per-`Step` replay determinism). It differs from a
//! `Step` by **contract** — it orchestrates a composition of Steps and runs them,
//! typically durably on Worklane — not by a different call shape.
//!
//! Naming the strategy as a swappable seam is the first step toward user-injected
//! strategies; today the projection pipeline is the sole, **reference** strategy
//! ([`crate::ProjectionStrategy`]). A declarative/data-driven strategy format and
//! runtime injection are deferred to later changes.

use worklane::async_trait;

/// A typed `Input -> Output` unit that composes and runs `Step`s asynchronously.
///
/// `Input` is `?Sized` so a strategy can take a trait object (e.g. the message log
/// as `dyn MessageStore`). Implementations compose one or more `Step`s; the
/// per-`Step` determinism composes into whole-strategy determinism.
#[async_trait]
pub trait Strategy: Send + Sync {
    /// The input the strategy consumes (e.g. a message log).
    type Input: ?Sized;
    /// The output the strategy produces (e.g. the replay stores).
    type Output;

    /// Run the strategy over a borrowed input, producing its output.
    async fn run(&self, input: &Self::Input) -> Self::Output;
}

#[cfg(test)]
mod tests {
    use super::*;
    use mirrorlane_core::{Step, StepVersion};

    // Two non-domain `Step`s — no `Message`/`Projection` in scope.
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
        fn run(&self, n: &u64) -> u64 {
            n * 2
        }
    }

    struct Incrementer;
    impl Step for Incrementer {
        type In = u64;
        type Out = u64;
        fn kind(&self) -> &'static str {
            "probe.incrementer"
        }
        fn version(&self) -> StepVersion {
            StepVersion::new("v1")
        }
        fn run(&self, n: &u64) -> u64 {
            n + 1
        }
    }

    /// A domain-free strategy: compose `Doubler` then `Incrementer` over a slice of
    /// `u64`. Its `Input`/`Output` carry no domain type, proving `Strategy` is
    /// generic and not projection-shaped.
    struct ProbeStrategy {
        doubler: Doubler,
        incrementer: Incrementer,
    }

    #[async_trait]
    impl Strategy for ProbeStrategy {
        type Input = [u64];
        type Output = Vec<u64>;

        async fn run(&self, input: &[u64]) -> Vec<u64> {
            input
                .iter()
                .map(|n| self.incrementer.run(&self.doubler.run(n)))
                .collect()
        }
    }

    #[tokio::test]
    async fn a_domain_free_strategy_composes_steps() {
        let strategy = ProbeStrategy {
            doubler: Doubler,
            incrementer: Incrementer,
        };
        // (n * 2) + 1 for each input.
        assert_eq!(strategy.run(&[1, 2, 3]).await, vec![3, 5, 7]);
    }
}
