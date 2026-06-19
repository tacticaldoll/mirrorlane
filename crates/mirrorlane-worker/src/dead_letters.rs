//! A lane-scoped dead-letter operability surface over a Worklane broker.
//!
//! The broker contract exposes the full dead-letter surface — read, count, requeue,
//! and purge — and every lane mirrorlane runs wants the same four operations against
//! its own lane. [`DeadLetters`] implements them once over an injected lane; the
//! per-lane surfaces ([`RoutedWorkDeadLetters`](crate::RoutedWorkDeadLetters),
//! [`StrategyRunDeadLetters`](crate::StrategyRunDeadLetters)) are thin newtypes that
//! bind the lane and deref to this, so each keeps its semantic name without
//! re-implementing the operations.

use std::sync::Arc;

use worklane::{Broker, DeadLetter, JobId, Lane, Result as WorklaneResult};

/// Read, count, requeue, and purge the dead-letters of one lane on a broker.
pub struct DeadLetters {
    broker: Arc<dyn Broker>,
    lane: Lane,
}

impl DeadLetters {
    /// Build the surface over the broker backing `lane`.
    pub fn new(broker: Arc<dyn Broker>, lane: Lane) -> Self {
        Self { broker, lane }
    }

    /// Read up to `limit` dead-lettered jobs. Non-destructive: a later read returns
    /// the same records until they are requeued or purged.
    pub async fn read(&self, limit: usize) -> WorklaneResult<Vec<DeadLetter>> {
        self.broker.read_dead_letters(&self.lane, limit).await
    }

    /// Count the dead-lettered jobs. Non-destructive and independent of any read
    /// limit — the operator's "how many?" without paging through records.
    pub async fn count(&self) -> WorklaneResult<u64> {
        self.broker.count_dead_letters(&self.lane).await
    }

    /// Requeue a dead-lettered job by id, making it reservable on its lane again and
    /// removing it from the dead-letter store.
    pub async fn requeue(&self, id: JobId) -> WorklaneResult<()> {
        self.broker.requeue(id).await
    }

    /// Purge the lane's dead-letter store, returning how many records were removed.
    /// Irreversible — the recovery path is `requeue`, not `purge`.
    pub async fn purge(&self) -> WorklaneResult<u64> {
        self.broker.purge_dead_letters(&self.lane).await
    }
}
