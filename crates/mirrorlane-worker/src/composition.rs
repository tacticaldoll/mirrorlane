//! The strategy composition vocabulary: a stage runner plus fan-out builders.
//!
//! A strategy is a sequence of **stages**, each a Worklane job run over a set of
//! payloads. [`run_stage`] owns the per-stage mechanic — a fresh in-memory broker,
//! register, enqueue each payload, run to idle — so a strategy never re-derives it.
//! [`per_message`], [`per_conversation`], and [`global`] name the three fan-out
//! shapes the projection pipeline uses, each generic over a payload-building
//! closure because every job's request type differs.
//!
//! Sequencing and dependencies are plain code: a later stage runs after an earlier
//! one by ordinary `await`, and depends on a prior stage's output by holding that
//! store. There is no graph format — per the
//! [design rationale](../../../docs/architecture.md), deps/fan-out are typed
//! composition, not a declarative DAG. Durable, parallel
//! (Chord-backed) execution is a later opt-in backing; this runs each stage on a
//! fresh in-memory broker, exactly as the hand-written pipeline did.

use std::sync::Arc;

use mirrorlane_core::message::{ConversationId, MessageEnvelope, MessageId};
use worklane::{Client, Job, Worker};
use worklane_memory::InMemoryBroker;

/// Run one stage: register `job`, enqueue every payload, and drain a fresh
/// in-memory broker to idle. This is the per-stage Worklane mechanic, written once.
pub async fn run_stage<J>(job: J, payloads: Vec<J::Payload>)
where
    J: Job<Output = ()>,
{
    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone());
    let mut worker = Worker::new(broker.clone());
    worker.register(job).expect("register stage job");
    for payload in payloads {
        client
            .enqueue::<J>(payload)
            .await
            .expect("enqueue stage payload");
    }
    worker
        .build()
        .expect("build worker")
        .run_until_idle()
        .await
        .expect("run stage to idle");
}

/// Fan out per message: one payload per message in the log.
pub fn per_message<P>(log: &[MessageEnvelope], build: impl Fn(&MessageEnvelope) -> P) -> Vec<P> {
    log.iter().map(build).collect()
}

/// Fan out per conversation: one payload per conversation, the closure receiving
/// the conversation id and its message ids (first-seen order).
pub fn per_conversation<P>(
    conversations: &[(ConversationId, Vec<MessageId>)],
    build: impl Fn(&ConversationId, &[MessageId]) -> P,
) -> Vec<P> {
    conversations
        .iter()
        .map(|(conversation, messages)| build(conversation, messages))
        .collect()
}

/// A single global payload over the whole input. A thunk (not a bare value) so all
/// three shapes read uniformly at the call site: `run_stage(job, global(|| ..))`.
pub fn global<P>(build: impl FnOnce() -> P) -> Vec<P> {
    vec![build()]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};

    use worklane::{HandlerResult, JobContext, async_trait};

    use crate::Strategy;

    // Two non-domain jobs — no `Message`/`Projection` in scope.

    /// Records every payload it sees, to prove a stage ran over each.
    #[derive(Default)]
    struct Sink {
        seen: Mutex<Vec<u64>>,
    }

    struct CollectJob {
        sink: Arc<Sink>,
    }

    #[async_trait]
    impl Job for CollectJob {
        type Payload = u64;
        type Output = ();
        const KIND: &'static str = "probe.collect";
        async fn run(&self, _ctx: JobContext, n: u64) -> HandlerResult<()> {
            self.sink.seen.lock().expect("lock").push(n);
            Ok(())
        }
    }

    struct SumJob {
        total: Arc<AtomicU64>,
    }

    #[async_trait]
    impl Job for SumJob {
        type Payload = u64;
        type Output = ();
        const KIND: &'static str = "probe.sum";
        async fn run(&self, _ctx: JobContext, n: u64) -> HandlerResult<()> {
            self.total.store(n, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn run_stage_runs_the_job_over_each_payload() {
        let sink = Arc::new(Sink::default());
        run_stage(
            CollectJob { sink: sink.clone() },
            vec![1, 2, 3], // an explicit fan-out, no domain type
        )
        .await;
        let mut seen = sink.seen.lock().expect("lock").clone();
        seen.sort_unstable();
        assert_eq!(seen, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn global_runs_a_single_payload() {
        let sink = Arc::new(Sink::default());
        run_stage(CollectJob { sink: sink.clone() }, global(|| 7u64)).await;
        assert_eq!(*sink.seen.lock().expect("lock"), vec![7]);
    }

    /// A non-domain, different-shaped strategy: a two-stage chain whose fan-out
    /// unit is neither message nor conversation, with a global join over the first
    /// stage's output — a shape projection does not have, composed through the same
    /// `run_stage` + `global` vocabulary.
    struct SumOfDoubles {
        sink: Arc<Sink>,
        total: Arc<AtomicU64>,
    }

    #[async_trait]
    impl Strategy for SumOfDoubles {
        type Input = [u64];
        type Output = u64;
        async fn run(&self, input: &[u64]) -> u64 {
            // Stage 1: fan out per input item, collecting the doubles.
            run_stage(
                CollectJob {
                    sink: self.sink.clone(),
                },
                input.iter().map(|n| n * 2).collect(),
            )
            .await;
            // Stage 2 depends on stage 1's output: a global join over the sink.
            let sum: u64 = self.sink.seen.lock().expect("lock").iter().sum();
            run_stage(
                SumJob {
                    total: self.total.clone(),
                },
                global(|| sum),
            )
            .await;
            self.total.load(Ordering::SeqCst)
        }
    }

    #[tokio::test]
    async fn a_different_shaped_strategy_composes_through_the_vocabulary() {
        let strategy = SumOfDoubles {
            sink: Arc::new(Sink::default()),
            total: Arc::new(AtomicU64::new(0)),
        };
        // (1+2+3)*2 = 12
        assert_eq!(strategy.run(&[1, 2, 3]).await, 12);
    }
}
