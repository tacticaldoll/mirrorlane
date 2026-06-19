//! Real Worklane enqueue for the `WorklaneJob` routing target.
//!
//! When routing decides a projection should become downstream work,
//! [`WorklaneJobConsumer`] enqueues a [`RoutedWork`] job onto a durable Worklane
//! broker instead of merely recording a receipt. Enqueue is **deduped** by a
//! message-id-derived uniqueness key, so re-dispatch (at-least-once delivery) and
//! re-routing (replay/re-derivation) never pile up duplicate jobs while one is
//! still live. [`RoutedWorkDeadLetters`] is the operability surface: read and
//! requeue jobs that exhausted their attempts on the routed-work lane.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use mirrorlane_core::message::MessageId;
use mirrorlane_core::projection::Projection;
use mirrorlane_core::routing::{Consumer, ConsumerError, RoutingDecision};
use worklane::{Broker, Client, HandlerResult, Job, JobContext, Lane, async_trait};

use crate::dead_letters::DeadLetters;

/// The lane routed-work jobs are enqueued to and dead-lettered on.
pub const ROUTED_WORK_LANE: &str = "mirrorlane.routed_work";

/// [`ROUTED_WORK_LANE`] as a validated [`Lane`]. Worklane models a lane as a
/// first-class type built through a fallible conversion, so this is the crate's
/// single point that turns the name constant into one. It cannot fail in
/// practice — the name is a valid portable lane — so the conversion is unwrapped.
pub fn routed_work_lane() -> Lane {
    ROUTED_WORK_LANE
        .parse()
        .expect("ROUTED_WORK_LANE is a valid lane name")
}

/// The payload of a routed-work job: the routed message and the projection a
/// downstream handler needs, kept self-contained so the job carries its own
/// context.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RoutedWorkPayload {
    pub message_id: MessageId,
    pub projection: Projection,
}

/// The typed Worklane job a routed projection becomes. The handler is
/// intentionally minimal: this change owns the enqueue edge and its operability,
/// not downstream agent execution.
pub struct RoutedWork;

#[async_trait]
impl Job for RoutedWork {
    type Payload = RoutedWorkPayload;
    type Output = ();
    const KIND: &'static str = "mirrorlane.routed_work";

    async fn run(&self, _ctx: JobContext, _payload: RoutedWorkPayload) -> HandlerResult<()> {
        Ok(())
    }
}

/// The uniqueness key a routed message enqueues under. Deterministic in the
/// message id, mirroring the routing decision store's idempotency at the broker.
fn unique_key(message_id: &MessageId) -> String {
    format!("{ROUTED_WORK_LANE}:{}", message_id.0)
}

/// A routing [`Consumer`] that enqueues a [`RoutedWork`] job onto a durable
/// Worklane broker, registered for `ConsumerKind::WorklaneJob`.
pub struct WorklaneJobConsumer {
    client: Client,
}

impl WorklaneJobConsumer {
    /// Build the consumer over a broker; it enqueues to [`ROUTED_WORK_LANE`].
    pub fn new(broker: Arc<dyn Broker>) -> Self {
        Self {
            client: Client::new(broker).with_lane(routed_work_lane()),
        }
    }
}

#[async_trait]
impl Consumer for WorklaneJobConsumer {
    async fn consume(
        &self,
        decision: &RoutingDecision,
        projection: &Projection,
    ) -> Result<(), ConsumerError> {
        let payload = RoutedWorkPayload {
            message_id: decision.message_id.clone(),
            projection: projection.clone(),
        };
        self.client
            .enqueue_unique::<RoutedWork>(unique_key(&decision.message_id), payload)
            .await
            .map_err(|e| ConsumerError::new(e.to_string()))?;
        Ok(())
    }
}

/// The dead-letter operability surface for the routed-work lane: read, count,
/// requeue, and purge jobs that exhausted their attempts. A thin newtype over
/// [`DeadLetters`](crate::DeadLetters) bound to the routed-work lane — the
/// operations live there; this keeps the lane's semantic name.
pub struct RoutedWorkDeadLetters(DeadLetters);

impl RoutedWorkDeadLetters {
    /// Build the surface over the broker backing the routed-work lane.
    pub fn new(broker: Arc<dyn Broker>) -> Self {
        Self(DeadLetters::new(broker, routed_work_lane()))
    }
}

impl std::ops::Deref for RoutedWorkDeadLetters {
    type Target = DeadLetters;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mirrorlane_core::projection::{Confidence, Entity, Intent, Topic};
    use mirrorlane_core::routing::ConsumerKind;
    use worklane::{DeadLetter, JobId, NewJob, Result as WorklaneResult};
    use worklane_memory::InMemoryBroker;

    fn projection(message_id: &str) -> Projection {
        Projection {
            message_id: MessageId(message_id.into()),
            intent: Intent::Task,
            topics: vec![Topic("auth".into())],
            entities: vec![Entity("sqlite".into())],
            confidence: Confidence::new(0.9),
        }
    }

    fn decision(message_id: &str) -> RoutingDecision {
        RoutingDecision {
            message_id: MessageId(message_id.into()),
            target: ConsumerKind::WorklaneJob,
            reason: "test".into(),
            escalated: false,
        }
    }

    #[test]
    fn routed_work_payload_round_trips() {
        let payload = RoutedWorkPayload {
            message_id: MessageId("m-1".into()),
            projection: projection("m-1"),
        };
        let json = serde_json::to_string(&payload).expect("serialize");
        let back: RoutedWorkPayload = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(payload, back);
    }

    #[tokio::test]
    async fn routing_to_worklane_enqueues_a_reservable_job() {
        let broker = Arc::new(InMemoryBroker::new());
        let consumer = WorklaneJobConsumer::new(broker.clone());

        consumer
            .consume(&decision("m-1"), &projection("m-1"))
            .await
            .expect("enqueue");

        let reserved = broker
            .reserve(&routed_work_lane())
            .await
            .expect("reserve")
            .expect("a routed-work job is reservable");
        assert_eq!(reserved.envelope.kind, RoutedWork::KIND);
    }

    #[tokio::test]
    async fn re_consuming_a_live_message_does_not_duplicate() {
        let broker = Arc::new(InMemoryBroker::new());
        let consumer = WorklaneJobConsumer::new(broker.clone());

        consumer
            .consume(&decision("m-1"), &projection("m-1"))
            .await
            .expect("enqueue");
        consumer
            .consume(&decision("m-1"), &projection("m-1"))
            .await
            .expect("re-enqueue dedups");

        // Exactly one job exists: the first reserve yields it, the second is empty.
        assert!(
            broker
                .reserve(&routed_work_lane())
                .await
                .expect("reserve")
                .is_some()
        );
        assert!(
            broker
                .reserve(&routed_work_lane())
                .await
                .expect("reserve")
                .is_none(),
            "a live unique key dedups the re-enqueue"
        );
    }

    #[tokio::test]
    async fn distinct_messages_enqueue_distinct_jobs() {
        let broker = Arc::new(InMemoryBroker::new());
        let consumer = WorklaneJobConsumer::new(broker.clone());

        consumer
            .consume(&decision("m-1"), &projection("m-1"))
            .await
            .expect("enqueue");
        consumer
            .consume(&decision("m-2"), &projection("m-2"))
            .await
            .expect("enqueue");

        assert!(
            broker
                .reserve(&routed_work_lane())
                .await
                .expect("reserve")
                .is_some()
        );
        assert!(
            broker
                .reserve(&routed_work_lane())
                .await
                .expect("reserve")
                .is_some()
        );
    }

    #[tokio::test]
    async fn an_enqueue_failure_surfaces_as_an_error() {
        // A broker whose enqueue always fails.
        struct FailingBroker;
        #[async_trait]
        impl Broker for FailingBroker {
            async fn enqueue(&self, _job: NewJob) -> WorklaneResult<JobId> {
                Err(worklane::Error::Broker("enqueue rejected".into()))
            }
            async fn enqueue_batch(&self, _jobs: Vec<NewJob>) -> WorklaneResult<Vec<JobId>> {
                Ok(Vec::new())
            }
            async fn reserve(&self, _lane: &Lane) -> WorklaneResult<Option<worklane::Reservation>> {
                Ok(None)
            }
            async fn ack(&self, _receipt: worklane::ReservationReceipt) -> WorklaneResult<()> {
                Ok(())
            }
            async fn retry(
                &self,
                _receipt: worklane::ReservationReceipt,
                _delay: std::time::Duration,
            ) -> WorklaneResult<()> {
                Ok(())
            }
            async fn extend(&self, _receipt: worklane::ReservationReceipt) -> WorklaneResult<()> {
                Ok(())
            }
            async fn defer(
                &self,
                _receipt: worklane::ReservationReceipt,
                _delay: std::time::Duration,
            ) -> WorklaneResult<()> {
                Ok(())
            }
            async fn pending_count(&self, _lane: &Lane) -> WorklaneResult<u64> {
                Ok(0)
            }
            async fn fail(
                &self,
                _receipt: worklane::ReservationReceipt,
                _error: String,
            ) -> WorklaneResult<()> {
                Ok(())
            }
            async fn read_dead_letters(
                &self,
                _lane: &Lane,
                _limit: usize,
            ) -> WorklaneResult<Vec<DeadLetter>> {
                Ok(Vec::new())
            }
            async fn count_dead_letters(&self, _lane: &Lane) -> WorklaneResult<u64> {
                Ok(0)
            }
            // `JobState` is not re-exported by the worklane facade, so the test
            // reaches the trait's return type through worklane-core directly.
            async fn classify(&self, _id: JobId) -> WorklaneResult<worklane_core::JobState> {
                Ok(worklane_core::JobState::CompletedOrUnknown)
            }
            async fn requeue(&self, _id: JobId) -> WorklaneResult<()> {
                Ok(())
            }
            async fn purge_dead_letters(&self, _lane: &Lane) -> WorklaneResult<u64> {
                Ok(0)
            }
        }

        let consumer = WorklaneJobConsumer::new(Arc::new(FailingBroker));
        let err = consumer
            .consume(&decision("m-1"), &projection("m-1"))
            .await
            .expect_err("enqueue failure surfaces");
        assert!(err.to_string().contains("enqueue rejected"));
    }

    #[tokio::test]
    async fn dead_letters_are_readable_and_requeueable() {
        let broker = Arc::new(InMemoryBroker::new());
        let dlq = RoutedWorkDeadLetters::new(broker.clone());

        // Drive a routed-work job to the dead-letter store: enqueue, reserve, fail.
        let id = broker
            .enqueue(NewJob::new(
                routed_work_lane(),
                RoutedWork::KIND,
                b"null".to_vec(),
                3,
            ))
            .await
            .expect("enqueue");
        let reserved = broker
            .reserve(&routed_work_lane())
            .await
            .expect("reserve")
            .expect("job to fail");
        broker
            .fail(reserved.receipt, "boom".into())
            .await
            .expect("fail");

        // Read is non-destructive.
        let dead = dlq.read(10).await.expect("read dead letters");
        assert_eq!(dead.len(), 1);
        assert_eq!(dlq.read(10).await.expect("read again").len(), 1);

        // Requeue makes it reservable again and clears the dead-letter store.
        dlq.requeue(id).await.expect("requeue");
        assert!(
            broker
                .reserve(&routed_work_lane())
                .await
                .expect("reserve")
                .is_some()
        );
        assert!(dlq.read(10).await.expect("read").is_empty());
    }

    #[tokio::test]
    async fn dead_letters_are_countable_and_purgeable() {
        let broker = Arc::new(InMemoryBroker::new());
        let dlq = RoutedWorkDeadLetters::new(broker.clone());

        // Drive a routed-work job to the dead-letter store: enqueue, reserve, fail.
        broker
            .enqueue(NewJob::new(
                routed_work_lane(),
                RoutedWork::KIND,
                b"null".to_vec(),
                3,
            ))
            .await
            .expect("enqueue");
        let reserved = broker
            .reserve(&routed_work_lane())
            .await
            .expect("reserve")
            .expect("job to fail");
        broker
            .fail(reserved.receipt, "boom".into())
            .await
            .expect("fail");

        // Count is non-destructive: it reports one and a later read still sees it.
        assert_eq!(dlq.count().await.expect("count"), 1);
        assert_eq!(dlq.read(10).await.expect("read").len(), 1);

        // Purge reports how many it removed and leaves the lane empty.
        assert_eq!(dlq.purge().await.expect("purge"), 1);
        assert_eq!(dlq.count().await.expect("count after purge"), 0);
    }
}
