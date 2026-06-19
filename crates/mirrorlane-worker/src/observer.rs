//! A glass-box view of a durable worker's activity.
//!
//! Worklane reports every resolved job to a [`JobObserver`]; mirrorlane's durable
//! consumer (`mirrorlane work`) otherwise drains its lane opaquely. [`RecordingJobObserver`]
//! captures each finished job's lane, kind, outcome, and duration into a shared,
//! readable log, so a run can report exactly what it ran — the observable counterpart
//! to the dead-letter operability surfaces.
//!
//! The callback runs inline on the worker, so it stays cheap: clone the few fields
//! into a [`JobRecord`] and return. Reading the log never blocks the worker beyond a
//! short lock.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use worklane::{JobEvent, JobObserver, JobOutcome};

/// One finished job's recorded execution — the fields a glass-box operator reads
/// after a run: which lane and kind ran, how it ended, and how long it took.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobRecord {
    pub lane: String,
    pub kind: String,
    pub outcome: JobOutcome,
    pub duration: Duration,
}

impl JobRecord {
    /// A stable lowercase label for the outcome, for line and JSON output. The
    /// outcome enum is `#[non_exhaustive]`, so an unrecognized future variant maps
    /// to `"unknown"` rather than failing to render.
    pub fn outcome_label(&self) -> &'static str {
        match self.outcome {
            JobOutcome::Acked => "acked",
            JobOutcome::Retried => "retried",
            JobOutcome::DeadLettered => "dead_lettered",
            _ => "unknown",
        }
    }

    /// The job's duration in whole milliseconds, for compact reporting.
    pub fn duration_ms(&self) -> u128 {
        self.duration.as_millis()
    }
}

/// A [`JobObserver`] that records every finished job into a shared log a caller can
/// read after a run. Cheap to clone — clones share one underlying log.
#[derive(Clone, Default)]
pub struct RecordingJobObserver {
    records: Arc<Mutex<Vec<JobRecord>>>,
}

impl RecordingJobObserver {
    /// Build an observer with an empty log.
    pub fn new() -> Self {
        Self::default()
    }

    /// A snapshot of the records collected so far, in the order jobs finished.
    pub fn records(&self) -> Vec<JobRecord> {
        self.records
            .lock()
            .expect("observer log lock not poisoned")
            .clone()
    }
}

impl JobObserver for RecordingJobObserver {
    fn on_job_finished(&self, event: JobEvent<'_>) {
        self.records
            .lock()
            .expect("observer log lock not poisoned")
            .push(JobRecord {
                lane: event.lane.to_string(),
                kind: event.kind.to_string(),
                outcome: event.outcome,
                duration: event.duration,
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use mirrorlane_core::InMemoryDerivedOutputCache;
    use worklane::{Client, Job, Worker};
    use worklane_memory::InMemoryBroker;

    use crate::submission::{StrategyRunJob, StrategyRunRequest, strategy_run_lane};
    use crate::test_support::{context, seeded_log};

    #[tokio::test]
    async fn a_finished_job_is_recorded_as_acked() {
        let broker = Arc::new(InMemoryBroker::new());
        let client = Client::new(broker.clone()).with_lane(strategy_run_lane());
        client
            .enqueue::<StrategyRunJob>(StrategyRunRequest {
                strategy: crate::StrategyRegistry::DEFAULT.into(),
            })
            .await
            .expect("submit");

        let observer = Arc::new(RecordingJobObserver::new());
        let mut worker = Worker::new(broker.clone())
            .with_lane(strategy_run_lane())
            .with_observer(observer.clone());
        worker
            .register(StrategyRunJob::with_builtins(
                seeded_log(),
                context(),
                Arc::new(InMemoryDerivedOutputCache::new()),
            ))
            .expect("register");
        worker
            .build()
            .expect("build worker")
            .run_until_idle()
            .await
            .expect("run");

        let records = observer.records();
        assert_eq!(records.len(), 1, "one finished job is recorded");
        assert_eq!(records[0].kind, StrategyRunJob::KIND);
        assert_eq!(records[0].outcome_label(), "acked");
    }

    #[tokio::test]
    async fn a_dead_lettered_job_is_recorded_as_dead_lettered() {
        let broker = Arc::new(InMemoryBroker::new());
        let client = Client::new(broker.clone())
            .with_lane(strategy_run_lane())
            .with_max_attempts(1);
        client
            .enqueue::<StrategyRunJob>(StrategyRunRequest {
                strategy: "nope".into(),
            })
            .await
            .expect("submit");

        let observer = Arc::new(RecordingJobObserver::new());
        let mut worker = Worker::new(broker.clone())
            .with_lane(strategy_run_lane())
            .with_observer(observer.clone());
        worker
            .register(StrategyRunJob::with_builtins(
                seeded_log(),
                context(),
                Arc::new(InMemoryDerivedOutputCache::new()),
            ))
            .expect("register");
        worker
            .build()
            .expect("build worker")
            .run_until_idle()
            .await
            .expect("run drains the failing job");

        let records = observer.records();
        assert_eq!(records.len(), 1, "the dead-lettered job is recorded");
        assert_eq!(records[0].outcome_label(), "dead_lettered");
    }
}
