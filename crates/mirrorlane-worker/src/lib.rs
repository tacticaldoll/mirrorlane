//! Mirrorlane worker: the execution layer of the projection pipeline.
//!
//! Worklane delivers jobs **at-least-once**, so every job here must be
//! idempotent. It provides [`ProcessMessageJob`], [`BuildScopeJob`], and
//! [`BuildWarmupJob`], plus the skill, developer-snapshot, routing-hint, and
//! routing jobs that complete the pipeline (see
//! `openspec/specs/mirrorlane/spec.md`).

use std::sync::Arc;

use mirrorlane_core::message::MessageEnvelope;
use mirrorlane_core::routing::{RoutingHintRequest, RoutingRequest};
use mirrorlane_core::scope::ScopeRequest;
use mirrorlane_core::skill::{DeveloperSnapshotRequest, SkillContribution, SkillRequest};
use mirrorlane_core::warmup::WarmupRequest;
use mirrorlane_core::{
    ConsumerRegistry, DeveloperSnapshotBuilder, DeveloperSnapshotStore, ProjectionStore, Projector,
    Router, RoutingHintStore, RoutingHinter, RoutingStore, RoutingTraceStore, ScopeProjector,
    ScopeStore, SkillBuilder, SkillStore, WarmupBuilder, WarmupStore,
};
use worklane::{HandlerResult, Job, JobContext, async_trait};

mod composition;
mod dead_letters;
mod derived;
mod observer;
mod registry;
mod replay;
mod routed_work;
mod strategy;
mod submission;
#[cfg(test)]
mod test_support;

pub use composition::{global, per_conversation, per_message, run_stage};
pub use dead_letters::DeadLetters;
pub use derived::{
    content_hash, content_hash_of, conversations, derivation_for, messages_in, populate_cache,
};
pub use observer::{JobRecord, RecordingJobObserver};
pub use registry::{
    EmptyStrategy, ReplayStrategy, StrategyContext, StrategyRegistry, UnknownStrategy,
};
pub use replay::{ProjectionStrategy, Replay, ReplayStores};
pub use routed_work::{
    ROUTED_WORK_LANE, RoutedWork, RoutedWorkDeadLetters, RoutedWorkPayload, WorklaneJobConsumer,
    routed_work_lane,
};
pub use strategy::Strategy;
pub use submission::{
    STRATEGY_RUN_LANE, StrategyRunDeadLetters, StrategyRunJob, StrategyRunRequest,
    strategy_run_lane,
};
// Re-exported so a reader of `JobRecord::outcome` need not depend on worklane directly.
pub use worklane::JobOutcome;

/// Projects a message and stores the result.
///
/// Idempotent: the projection is upserted by message id, so re-delivery leaves
/// exactly one projection. Dependencies are injected so a real SLM projector or
/// durable store can be swapped in without changing this job.
pub struct ProcessMessageJob {
    projector: Arc<dyn Projector>,
    store: Arc<dyn ProjectionStore>,
}

impl ProcessMessageJob {
    /// Build the job from a projector and a projection store.
    pub fn new(projector: Arc<dyn Projector>, store: Arc<dyn ProjectionStore>) -> Self {
        Self { projector, store }
    }
}

#[async_trait]
impl Job for ProcessMessageJob {
    type Payload = MessageEnvelope;
    type Output = ();
    const KIND: &'static str = "mirrorlane.process_message";

    async fn run(&self, _ctx: JobContext, payload: MessageEnvelope) -> HandlerResult<()> {
        let projection = self.projector.project(&payload);
        self.store.upsert(projection);
        Ok(())
    }
}

/// Builds a session's scope from its stored projections and stores the result.
///
/// Idempotent: the scope is upserted by conversation id, so re-delivery leaves
/// exactly one scope. Message ids with no stored projection are skipped, so a
/// partially-projected session still yields a valid scope.
pub struct BuildScopeJob {
    projections: Arc<dyn ProjectionStore>,
    scoper: Arc<dyn ScopeProjector>,
    scopes: Arc<dyn ScopeStore>,
}

impl BuildScopeJob {
    /// Build the job from a projection store, a scope projector, and a scope
    /// store.
    pub fn new(
        projections: Arc<dyn ProjectionStore>,
        scoper: Arc<dyn ScopeProjector>,
        scopes: Arc<dyn ScopeStore>,
    ) -> Self {
        Self {
            projections,
            scoper,
            scopes,
        }
    }
}

#[async_trait]
impl Job for BuildScopeJob {
    type Payload = ScopeRequest;
    type Output = ();
    const KIND: &'static str = "mirrorlane.build_scope";

    async fn run(&self, _ctx: JobContext, request: ScopeRequest) -> HandlerResult<()> {
        let projections: Vec<_> = request
            .messages
            .iter()
            .filter_map(|id| self.projections.get(id))
            .collect();
        let scope = self.scoper.scope(&request.conversation, &projections);
        self.scopes.upsert(scope);
        Ok(())
    }
}

/// Builds a session's warm-up document from its stored projections and scope.
///
/// Idempotent: the document is upserted by conversation id, so re-delivery
/// leaves exactly one. Missing projections are skipped and an absent scope
/// yields an empty focus, so a partially-processed session still warms up.
pub struct BuildWarmupJob {
    projections: Arc<dyn ProjectionStore>,
    scopes: Arc<dyn ScopeStore>,
    builder: Arc<dyn WarmupBuilder>,
    warmups: Arc<dyn WarmupStore>,
}

impl BuildWarmupJob {
    /// Build the job from the projection store, scope store, warm-up builder,
    /// and warm-up store.
    pub fn new(
        projections: Arc<dyn ProjectionStore>,
        scopes: Arc<dyn ScopeStore>,
        builder: Arc<dyn WarmupBuilder>,
        warmups: Arc<dyn WarmupStore>,
    ) -> Self {
        Self {
            projections,
            scopes,
            builder,
            warmups,
        }
    }
}

#[async_trait]
impl Job for BuildWarmupJob {
    type Payload = WarmupRequest;
    type Output = ();
    const KIND: &'static str = "mirrorlane.build_warmup";

    async fn run(&self, _ctx: JobContext, request: WarmupRequest) -> HandlerResult<()> {
        let projections: Vec<_> = request
            .messages
            .iter()
            .filter_map(|id| self.projections.get(id))
            .collect();
        let scope = self.scopes.get(&request.conversation);
        let document = self
            .builder
            .build(&request.conversation, scope.as_ref(), &projections);
        self.warmups.upsert(document);
        Ok(())
    }
}

/// Builds the global skill index from authored messages and stores it per topic.
///
/// Unlike the per-conversation jobs, this aggregates across the whole log. It
/// joins each entry's author to that message's stored projection for the topics
/// the projection lacks the author for, then upserts one ownership per topic.
/// Idempotent: ownership is upserted by topic, so re-delivery leaves exactly one
/// per topic. Entries whose message has no stored projection are skipped.
pub struct BuildSkillJob {
    projections: Arc<dyn ProjectionStore>,
    builder: Arc<dyn SkillBuilder>,
    skills: Arc<dyn SkillStore>,
}

impl BuildSkillJob {
    /// Build the job from the projection store, a skill builder, and a skill
    /// store.
    pub fn new(
        projections: Arc<dyn ProjectionStore>,
        builder: Arc<dyn SkillBuilder>,
        skills: Arc<dyn SkillStore>,
    ) -> Self {
        Self {
            projections,
            builder,
            skills,
        }
    }
}

#[async_trait]
impl Job for BuildSkillJob {
    type Payload = SkillRequest;
    type Output = ();
    const KIND: &'static str = "mirrorlane.build_skill";

    async fn run(&self, _ctx: JobContext, request: SkillRequest) -> HandlerResult<()> {
        let contributions: Vec<SkillContribution> = request
            .entries
            .iter()
            .filter_map(|entry| {
                self.projections
                    .get(&entry.message)
                    .map(|projection| SkillContribution {
                        author: entry.author.clone(),
                        display_name: entry.display_name.clone(),
                        topics: projection.topics.clone(),
                        confidence: projection.confidence,
                    })
            })
            .collect();
        let index = self.builder.build(&contributions);
        for ownership in index.ownerships {
            self.skills.upsert(ownership);
        }
        Ok(())
    }
}

/// Routes each requested message's projection to a consumer and dispatches it.
///
/// For each message it routes the stored projection, upserts the decision
/// (idempotent by message id), and dispatches through the registry. Routing is a
/// separate dispatch path — it is never run by `Replay` — so external delivery is
/// not re-run on replay. Messages with no stored projection are skipped.
pub struct RouteJob {
    projections: Arc<dyn ProjectionStore>,
    router: Arc<dyn Router>,
    routes: Arc<dyn RoutingStore>,
    traces: Arc<dyn RoutingTraceStore>,
    consumers: Arc<ConsumerRegistry>,
}

impl RouteJob {
    /// Build the job from the projection store, a router, a routing store, and a
    /// consumer registry.
    pub fn new(
        projections: Arc<dyn ProjectionStore>,
        router: Arc<dyn Router>,
        routes: Arc<dyn RoutingStore>,
        traces: Arc<dyn RoutingTraceStore>,
        consumers: Arc<ConsumerRegistry>,
    ) -> Self {
        Self {
            projections,
            router,
            routes,
            traces,
            consumers,
        }
    }
}

#[async_trait]
impl Job for RouteJob {
    type Payload = RoutingRequest;
    type Output = ();
    const KIND: &'static str = "mirrorlane.route";

    async fn run(&self, _ctx: JobContext, request: RoutingRequest) -> HandlerResult<()> {
        for id in &request.messages {
            let Some(projection) = self.projections.get(id) else {
                continue;
            };
            let (decision, trace) = self.router.route(&projection);
            self.traces.upsert(trace);
            self.routes.upsert(decision.clone());
            // A consumer failure (e.g. a broker enqueue error) fails the routing
            // job so Worklane retries it. The decision upsert is idempotent and
            // the Worklane consumer dedups by message id, so a retry after a
            // partial failure produces no duplicate decision or job.
            self.consumers.dispatch(&decision, &projection).await?;
        }
        Ok(())
    }
}

/// Builds a skill-derived routing hint per message and stores it.
///
/// For each message it loads the stored projection, gathers the `TopicOwnership`s
/// for that projection's topics from the skill store, builds the hint via the
/// `RoutingHinter`, and upserts it (idempotent by message id). Unlike `RouteJob`
/// this dispatches nothing — the hint is replayable derived state, so `Replay`
/// re-runs it. Messages with no stored projection are skipped.
pub struct BuildRoutingHintJob {
    projections: Arc<dyn ProjectionStore>,
    skills: Arc<dyn SkillStore>,
    hinter: Arc<dyn RoutingHinter>,
    hints: Arc<dyn RoutingHintStore>,
}

impl BuildRoutingHintJob {
    /// Build the job from the projection store, skill store, hinter, and hint
    /// store.
    pub fn new(
        projections: Arc<dyn ProjectionStore>,
        skills: Arc<dyn SkillStore>,
        hinter: Arc<dyn RoutingHinter>,
        hints: Arc<dyn RoutingHintStore>,
    ) -> Self {
        Self {
            projections,
            skills,
            hinter,
            hints,
        }
    }
}

#[async_trait]
impl Job for BuildRoutingHintJob {
    type Payload = RoutingHintRequest;
    type Output = ();
    const KIND: &'static str = "mirrorlane.build_routing_hint";

    async fn run(&self, _ctx: JobContext, request: RoutingHintRequest) -> HandlerResult<()> {
        for id in &request.messages {
            let Some(projection) = self.projections.get(id) else {
                continue;
            };
            let ownerships: Vec<_> = projection
                .topics
                .iter()
                .filter_map(|topic| self.skills.get(topic))
                .collect();
            let hint = self.hinter.hint(&projection, &ownerships);
            self.hints.upsert(hint);
        }
        Ok(())
    }
}

/// Builds a session's developer snapshot from the skill index and stores it.
///
/// For a conversation it resolves the session's topics from the requested
/// messages' stored projections, fetches each topic's `TopicOwnership` from the
/// skill store, builds the `SessionDevelopers` via the `DeveloperSnapshotBuilder`,
/// and upserts it (idempotent by conversation id). Like the skill and hint
/// phases it dispatches nothing — it is replayable derived state, so `Replay`
/// re-runs it. Messages with no stored projection are skipped when resolving
/// topics.
pub struct BuildDeveloperSnapshotJob {
    projections: Arc<dyn ProjectionStore>,
    skills: Arc<dyn SkillStore>,
    snapshotter: Arc<dyn DeveloperSnapshotBuilder>,
    developers: Arc<dyn DeveloperSnapshotStore>,
}

impl BuildDeveloperSnapshotJob {
    /// Build the job from the projection store, skill store, snapshotter, and
    /// developer snapshot store.
    pub fn new(
        projections: Arc<dyn ProjectionStore>,
        skills: Arc<dyn SkillStore>,
        snapshotter: Arc<dyn DeveloperSnapshotBuilder>,
        developers: Arc<dyn DeveloperSnapshotStore>,
    ) -> Self {
        Self {
            projections,
            skills,
            snapshotter,
            developers,
        }
    }
}

#[async_trait]
impl Job for BuildDeveloperSnapshotJob {
    type Payload = DeveloperSnapshotRequest;
    type Output = ();
    const KIND: &'static str = "mirrorlane.build_developer_snapshot";

    async fn run(&self, _ctx: JobContext, request: DeveloperSnapshotRequest) -> HandlerResult<()> {
        // Resolve the session's distinct topics from its projections, fetching
        // each topic's ownership once. Output order does not depend on this, since
        // the snapshotter ranks topics itself.
        let mut seen = std::collections::BTreeSet::new();
        let mut ownerships = Vec::new();
        for id in &request.messages {
            let Some(projection) = self.projections.get(id) else {
                continue;
            };
            for topic in projection.topics {
                if seen.insert(topic.0.clone())
                    && let Some(ownership) = self.skills.get(&topic)
                {
                    ownerships.push(ownership);
                }
            }
        }
        let developers =
            self.snapshotter
                .build(&request.conversation, &request.participants, &ownerships);
        self.developers.upsert(developers);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mirrorlane_core::InMemoryProjectionStore;
    use mirrorlane_core::message::{
        Author, AuthorId, Conversation, ConversationId, MessageId, Source,
    };
    use mirrorlane_provider::MockProjector;
    use worklane::{Client, Worker};
    use worklane_memory::InMemoryBroker;

    fn message() -> MessageEnvelope {
        MessageEnvelope {
            id: MessageId("m-1".into()),
            source: Source::GitHub,
            author: Author {
                id: AuthorId("u-1".into()),
                display_name: "Dev".into(),
            },
            conversation: Conversation {
                id: ConversationId("c-1".into()),
                thread: None,
            },
            body: "Should we use sqlite for the auth sdk?".into(),
        }
    }

    async fn run_jobs(store: Arc<InMemoryProjectionStore>, messages: Vec<MessageEnvelope>) {
        let broker = Arc::new(InMemoryBroker::new());
        let client = Client::new(broker.clone());
        let mut worker = Worker::new(broker.clone());
        worker
            .register(ProcessMessageJob::new(
                Arc::new(MockProjector::new()),
                store,
            ))
            .expect("register job");
        for m in messages {
            client
                .enqueue::<ProcessMessageJob>(m)
                .await
                .expect("enqueue");
        }
        worker
            .build()
            .expect("build worker")
            .run_until_idle()
            .await
            .expect("run to idle");
    }

    #[tokio::test]
    async fn processing_stores_projection() {
        let store = Arc::new(InMemoryProjectionStore::new());
        run_jobs(store.clone(), vec![message()]).await;
        assert!(store.get(&MessageId("m-1".into())).is_some());
    }

    #[tokio::test]
    async fn redelivery_is_idempotent() {
        let store = Arc::new(InMemoryProjectionStore::new());
        run_jobs(store.clone(), vec![message(), message()]).await;
        assert_eq!(store.len(), 1, "same message must yield one projection");
    }

    mod scope {
        use super::*;
        use mirrorlane_core::scope::ScopeRequest;
        use mirrorlane_core::{InMemoryScopeStore, ScopeStore};
        use mirrorlane_provider::MockScopeProjector;

        fn conversation() -> ConversationId {
            ConversationId("c-1".into())
        }

        fn request(messages: &[&str]) -> ScopeRequest {
            ScopeRequest {
                conversation: conversation(),
                messages: messages.iter().map(|m| MessageId((*m).into())).collect(),
            }
        }

        /// Seed a projection store with `message()` projected (id `m-1`).
        async fn seeded_projections() -> Arc<InMemoryProjectionStore> {
            let store = Arc::new(InMemoryProjectionStore::new());
            run_jobs(store.clone(), vec![message()]).await;
            store
        }

        async fn run_scope_jobs(
            projections: Arc<InMemoryProjectionStore>,
            scopes: Arc<InMemoryScopeStore>,
            requests: Vec<ScopeRequest>,
        ) {
            let broker = Arc::new(InMemoryBroker::new());
            let client = Client::new(broker.clone());
            let mut worker = Worker::new(broker.clone());
            worker
                .register(BuildScopeJob::new(
                    projections,
                    Arc::new(MockScopeProjector::new()),
                    scopes,
                ))
                .expect("register job");
            for request in requests {
                client
                    .enqueue::<BuildScopeJob>(request)
                    .await
                    .expect("enqueue");
            }
            worker
                .build()
                .expect("build worker")
                .run_until_idle()
                .await
                .expect("run to idle");
        }

        #[tokio::test]
        async fn building_a_scope_stores_it_by_conversation() {
            let projections = seeded_projections().await;
            let scopes = Arc::new(InMemoryScopeStore::new());
            run_scope_jobs(projections, scopes.clone(), vec![request(&["m-1"])]).await;
            assert!(scopes.get(&conversation()).is_some());
        }

        #[tokio::test]
        async fn redelivery_is_idempotent() {
            let projections = seeded_projections().await;
            let scopes = Arc::new(InMemoryScopeStore::new());
            run_scope_jobs(
                projections,
                scopes.clone(),
                vec![request(&["m-1"]), request(&["m-1"])],
            )
            .await;
            assert_eq!(scopes.len(), 1, "same request must yield one scope");
        }

        #[tokio::test]
        async fn missing_projections_are_skipped() {
            let projections = seeded_projections().await;
            let scopes = Arc::new(InMemoryScopeStore::new());
            run_scope_jobs(
                projections,
                scopes.clone(),
                vec![request(&["m-1", "m-missing"])],
            )
            .await;
            assert!(scopes.get(&conversation()).is_some());
        }
    }

    mod warmup {
        use super::*;
        use mirrorlane_core::projection::{Confidence, Intent, Projection};
        use mirrorlane_core::scope::{Component, Scope};
        use mirrorlane_core::warmup::WarmupRequest;
        use mirrorlane_core::{InMemoryScopeStore, InMemoryWarmupStore, WarmupStore};
        use mirrorlane_provider::MockWarmupBuilder;

        fn conversation() -> ConversationId {
            ConversationId("c-1".into())
        }

        fn projection(id: &str, intent: Intent) -> Projection {
            Projection {
                message_id: MessageId(id.into()),
                intent,
                topics: Vec::new(),
                entities: Vec::new(),
                confidence: Confidence::new(0.8),
            }
        }

        fn seeded_projections() -> Arc<InMemoryProjectionStore> {
            let store = Arc::new(InMemoryProjectionStore::new());
            store.upsert(projection("m-1", Intent::Decision));
            store.upsert(projection("m-2", Intent::Question));
            store.upsert(projection("m-3", Intent::Task));
            store
        }

        fn seeded_scope() -> Arc<InMemoryScopeStore> {
            let store = Arc::new(InMemoryScopeStore::new());
            store.upsert(Scope {
                conversation: conversation(),
                load: vec![Component("auth-sdk".into())],
                ignore: Vec::new(),
                reason: "SDK auth work.".into(),
                confidence: Confidence::new(0.8),
            });
            store
        }

        fn request() -> WarmupRequest {
            WarmupRequest {
                conversation: conversation(),
                messages: vec![
                    MessageId("m-1".into()),
                    MessageId("m-2".into()),
                    MessageId("m-3".into()),
                ],
            }
        }

        async fn run_warmup_jobs(
            projections: Arc<InMemoryProjectionStore>,
            scopes: Arc<InMemoryScopeStore>,
            warmups: Arc<InMemoryWarmupStore>,
            requests: Vec<WarmupRequest>,
        ) {
            let broker = Arc::new(InMemoryBroker::new());
            let client = Client::new(broker.clone());
            let mut worker = Worker::new(broker.clone());
            worker
                .register(BuildWarmupJob::new(
                    projections,
                    scopes,
                    Arc::new(MockWarmupBuilder::new()),
                    warmups,
                ))
                .expect("register job");
            for request in requests {
                client
                    .enqueue::<BuildWarmupJob>(request)
                    .await
                    .expect("enqueue");
            }
            worker
                .build()
                .expect("build worker")
                .run_until_idle()
                .await
                .expect("run to idle");
        }

        #[tokio::test]
        async fn building_a_warmup_stores_it_by_conversation() {
            let warmups = Arc::new(InMemoryWarmupStore::new());
            run_warmup_jobs(
                seeded_projections(),
                seeded_scope(),
                warmups.clone(),
                vec![request()],
            )
            .await;
            let document = warmups.get(&conversation()).expect("document stored");
            assert_eq!(document.decisions, vec![MessageId("m-1".into())]);
            assert_eq!(document.focus, vec![Component("auth-sdk".into())]);
        }

        #[tokio::test]
        async fn redelivery_is_idempotent() {
            let warmups = Arc::new(InMemoryWarmupStore::new());
            run_warmup_jobs(
                seeded_projections(),
                seeded_scope(),
                warmups.clone(),
                vec![request(), request()],
            )
            .await;
            assert_eq!(warmups.len(), 1, "same request must yield one document");
        }

        #[tokio::test]
        async fn missing_scope_does_not_fail() {
            let warmups = Arc::new(InMemoryWarmupStore::new());
            run_warmup_jobs(
                seeded_projections(),
                Arc::new(InMemoryScopeStore::new()),
                warmups.clone(),
                vec![request()],
            )
            .await;
            let document = warmups.get(&conversation()).expect("document stored");
            assert!(document.focus.is_empty());
        }
    }

    mod skill {
        use super::*;
        use mirrorlane_core::message::AuthorId;
        use mirrorlane_core::projection::Topic;
        use mirrorlane_core::skill::{SkillEntry, SkillRequest};
        use mirrorlane_core::{InMemorySkillStore, SkillStore};
        use mirrorlane_provider::MessageSkillBuilder;

        /// Seed a projection store with `message()` projected (id `m-1`, author
        /// `u-1`, topics `auth`/`sdk`).
        async fn seeded_projections() -> Arc<InMemoryProjectionStore> {
            let store = Arc::new(InMemoryProjectionStore::new());
            run_jobs(store.clone(), vec![message()]).await;
            store
        }

        fn entry(author: &str, message: &str) -> SkillEntry {
            SkillEntry {
                author: AuthorId(author.into()),
                display_name: author.into(),
                message: MessageId(message.into()),
            }
        }

        async fn run_skill_jobs(
            projections: Arc<InMemoryProjectionStore>,
            skills: Arc<InMemorySkillStore>,
            requests: Vec<SkillRequest>,
        ) {
            let broker = Arc::new(InMemoryBroker::new());
            let client = Client::new(broker.clone());
            let mut worker = Worker::new(broker.clone());
            worker
                .register(BuildSkillJob::new(
                    projections,
                    Arc::new(MessageSkillBuilder::new()),
                    skills,
                ))
                .expect("register job");
            for request in requests {
                client
                    .enqueue::<BuildSkillJob>(request)
                    .await
                    .expect("enqueue");
            }
            worker
                .build()
                .expect("build worker")
                .run_until_idle()
                .await
                .expect("run to idle");
        }

        #[tokio::test]
        async fn building_stores_ownership_per_topic() {
            let skills = Arc::new(InMemorySkillStore::new());
            run_skill_jobs(
                seeded_projections().await,
                skills.clone(),
                vec![SkillRequest {
                    entries: vec![entry("u-1", "m-1")],
                }],
            )
            .await;
            let auth = skills.get(&Topic("auth".into())).expect("auth ownership");
            assert_eq!(auth.candidates[0].author, AuthorId("u-1".into()));
            assert!(skills.get(&Topic("sdk".into())).is_some());
        }

        #[tokio::test]
        async fn redelivery_is_idempotent() {
            let skills = Arc::new(InMemorySkillStore::new());
            let request = SkillRequest {
                entries: vec![entry("u-1", "m-1")],
            };
            run_skill_jobs(
                seeded_projections().await,
                skills.clone(),
                vec![request.clone(), request],
            )
            .await;
            // message() projects to topics auth and sdk: exactly two ownerships.
            assert_eq!(skills.len(), 2, "re-delivery must not duplicate ownership");
        }

        #[tokio::test]
        async fn missing_projection_is_skipped() {
            let skills = Arc::new(InMemorySkillStore::new());
            run_skill_jobs(
                seeded_projections().await,
                skills.clone(),
                vec![SkillRequest {
                    entries: vec![entry("u-1", "m-1"), entry("u-2", "m-missing")],
                }],
            )
            .await;
            assert!(
                skills.get(&Topic("auth".into())).is_some(),
                "index still builds from present projections"
            );
        }
    }

    mod routing {
        use super::*;
        use mirrorlane_core::routing::{ConsumerKind, RoutingRequest};
        use mirrorlane_core::{
            ConsumerRegistry, InMemoryRoutingStore, InMemoryRoutingTraceStore, RoutingStore,
        };
        use mirrorlane_provider::{RecordingConsumer, RuleRouter};

        /// Seed a projection store with `message()` projected (id `m-1`, intent
        /// Question at confidence 0.8 → routes to Human, not escalated).
        async fn seeded_projections() -> Arc<InMemoryProjectionStore> {
            let store = Arc::new(InMemoryProjectionStore::new());
            run_jobs(store.clone(), vec![message()]).await;
            store
        }

        fn request(messages: &[&str]) -> RoutingRequest {
            RoutingRequest {
                messages: messages.iter().map(|m| MessageId((*m).into())).collect(),
            }
        }

        async fn run_route_jobs(
            projections: Arc<InMemoryProjectionStore>,
            routes: Arc<InMemoryRoutingStore>,
            traces: Arc<InMemoryRoutingTraceStore>,
            human: Arc<RecordingConsumer>,
            requests: Vec<RoutingRequest>,
        ) {
            let mut registry = ConsumerRegistry::new();
            registry.register(ConsumerKind::Human, human);
            let registry = Arc::new(registry);

            let broker = Arc::new(InMemoryBroker::new());
            let client = Client::new(broker.clone());
            let mut worker = Worker::new(broker.clone());
            worker
                .register(RouteJob::new(
                    projections,
                    Arc::new(RuleRouter::new()),
                    routes,
                    traces,
                    registry,
                ))
                .expect("register job");
            for request in requests {
                client.enqueue::<RouteJob>(request).await.expect("enqueue");
            }
            worker
                .build()
                .expect("build worker")
                .run_until_idle()
                .await
                .expect("run to idle");
        }

        #[tokio::test]
        async fn routing_stores_decision_and_dispatches() {
            let routes = Arc::new(InMemoryRoutingStore::new());
            let human = Arc::new(RecordingConsumer::new());
            let traces = Arc::new(InMemoryRoutingTraceStore::new());
            run_route_jobs(
                seeded_projections().await,
                routes.clone(),
                traces.clone(),
                human.clone(),
                vec![request(&["m-1"])],
            )
            .await;
            let decision = routes
                .get(&MessageId("m-1".into()))
                .expect("decision stored");
            assert_eq!(decision.target, ConsumerKind::Human);
            assert!(human.received(ConsumerKind::Human, &MessageId("m-1".into())));
        }

        #[tokio::test]
        async fn redelivery_is_idempotent() {
            let routes = Arc::new(InMemoryRoutingStore::new());
            let human = Arc::new(RecordingConsumer::new());
            let traces = Arc::new(InMemoryRoutingTraceStore::new());
            run_route_jobs(
                seeded_projections().await,
                routes.clone(),
                traces.clone(),
                human,
                vec![request(&["m-1"]), request(&["m-1"])],
            )
            .await;
            assert_eq!(routes.len(), 1, "same message must yield one decision");
        }

        #[tokio::test]
        async fn missing_projection_is_skipped() {
            let routes = Arc::new(InMemoryRoutingStore::new());
            let human = Arc::new(RecordingConsumer::new());
            let traces = Arc::new(InMemoryRoutingTraceStore::new());
            run_route_jobs(
                seeded_projections().await,
                routes.clone(),
                traces.clone(),
                human,
                vec![request(&["m-1", "m-missing"])],
            )
            .await;
            assert!(routes.get(&MessageId("m-1".into())).is_some());
            assert!(routes.get(&MessageId("m-missing".into())).is_none());
            assert_eq!(routes.len(), 1);
        }

        #[tokio::test]
        async fn routing_a_task_enqueues_a_worklane_job() {
            use mirrorlane_core::projection::{Confidence, Entity, Intent, Projection, Topic};
            use worklane::Broker;

            // A Task projection routes to WorklaneJob (RuleRouter), with
            // confidence above the escalation threshold.
            let projections = Arc::new(InMemoryProjectionStore::new());
            projections.upsert(Projection {
                message_id: MessageId("m-1".into()),
                intent: Intent::Task,
                topics: vec![Topic("auth".into())],
                entities: vec![Entity("sqlite".into())],
                confidence: Confidence::new(0.9),
            });

            // The durable routed-work broker the WorklaneJob consumer enqueues to,
            // kept separate from the broker running the route job itself.
            let routed_work_broker = Arc::new(InMemoryBroker::new());
            let mut registry = ConsumerRegistry::new();
            registry.register(
                ConsumerKind::WorklaneJob,
                Arc::new(WorklaneJobConsumer::new(routed_work_broker.clone())),
            );
            let registry = Arc::new(registry);

            let routes = Arc::new(InMemoryRoutingStore::new());
            let traces = Arc::new(InMemoryRoutingTraceStore::new());
            let broker = Arc::new(InMemoryBroker::new());
            let client = Client::new(broker.clone());
            let mut worker = Worker::new(broker.clone());
            worker
                .register(RouteJob::new(
                    projections,
                    Arc::new(RuleRouter::new()),
                    routes.clone(),
                    traces.clone(),
                    registry,
                ))
                .expect("register job");
            client
                .enqueue::<RouteJob>(request(&["m-1"]))
                .await
                .expect("enqueue");
            worker
                .build()
                .expect("build worker")
                .run_until_idle()
                .await
                .expect("run to idle");

            assert_eq!(
                routes
                    .get(&MessageId("m-1".into()))
                    .expect("decision stored")
                    .target,
                ConsumerKind::WorklaneJob
            );
            let reserved = routed_work_broker
                .reserve(&routed_work_lane())
                .await
                .expect("reserve")
                .expect("a routed-work job was enqueued");
            assert_eq!(reserved.envelope.kind, RoutedWork::KIND);
        }
    }

    mod hint {
        use super::*;
        use mirrorlane_core::message::AuthorId;
        use mirrorlane_core::projection::Topic;
        use mirrorlane_core::routing::RoutingHintRequest;
        use mirrorlane_core::skill::{ExpertCandidate, SkillScore, TopicOwnership};
        use mirrorlane_core::{
            InMemoryRoutingHintStore, InMemorySkillStore, RoutingHintStore, SkillStore,
        };
        use mirrorlane_provider::SkillRoutingHinter;

        /// Seed a projection store with `message()` projected (id `m-1`, topics
        /// `auth`/`sdk`).
        async fn seeded_projections() -> Arc<InMemoryProjectionStore> {
            let store = Arc::new(InMemoryProjectionStore::new());
            run_jobs(store.clone(), vec![message()]).await;
            store
        }

        /// Seed a skill store: `u-1` owns both `auth` and `sdk`.
        fn seeded_skills() -> Arc<InMemorySkillStore> {
            let skills = Arc::new(InMemorySkillStore::new());
            for topic in ["auth", "sdk"] {
                skills.upsert(TopicOwnership {
                    topic: Topic(topic.into()),
                    candidates: vec![ExpertCandidate {
                        author: AuthorId("u-1".into()),
                        display_name: "Dev".into(),
                        score: SkillScore::new(1.0),
                    }],
                });
            }
            skills
        }

        fn request(messages: &[&str]) -> RoutingHintRequest {
            RoutingHintRequest {
                messages: messages.iter().map(|m| MessageId((*m).into())).collect(),
            }
        }

        async fn run_hint_jobs(
            projections: Arc<InMemoryProjectionStore>,
            skills: Arc<InMemorySkillStore>,
            hints: Arc<InMemoryRoutingHintStore>,
            requests: Vec<RoutingHintRequest>,
        ) {
            let broker = Arc::new(InMemoryBroker::new());
            let client = Client::new(broker.clone());
            let mut worker = Worker::new(broker.clone());
            worker
                .register(BuildRoutingHintJob::new(
                    projections,
                    skills,
                    Arc::new(SkillRoutingHinter::new()),
                    hints,
                ))
                .expect("register job");
            for request in requests {
                client
                    .enqueue::<BuildRoutingHintJob>(request)
                    .await
                    .expect("enqueue");
            }
            worker
                .build()
                .expect("build worker")
                .run_until_idle()
                .await
                .expect("run to idle");
        }

        #[tokio::test]
        async fn building_stores_one_hint_per_message() {
            let hints = Arc::new(InMemoryRoutingHintStore::new());
            run_hint_jobs(
                seeded_projections().await,
                seeded_skills(),
                hints.clone(),
                vec![request(&["m-1"])],
            )
            .await;
            let hint = hints.get(&MessageId("m-1".into())).expect("hint stored");
            assert_eq!(hint.human_hint.unwrap().author, AuthorId("u-1".into()));
            assert_eq!(hints.len(), 1);
        }

        #[tokio::test]
        async fn redelivery_is_idempotent() {
            let hints = Arc::new(InMemoryRoutingHintStore::new());
            run_hint_jobs(
                seeded_projections().await,
                seeded_skills(),
                hints.clone(),
                vec![request(&["m-1"]), request(&["m-1"])],
            )
            .await;
            assert_eq!(hints.len(), 1, "same message must yield one hint");
        }

        #[tokio::test]
        async fn missing_projection_is_skipped() {
            let hints = Arc::new(InMemoryRoutingHintStore::new());
            run_hint_jobs(
                seeded_projections().await,
                seeded_skills(),
                hints.clone(),
                vec![request(&["m-1", "m-missing"])],
            )
            .await;
            assert!(hints.get(&MessageId("m-1".into())).is_some());
            assert!(hints.get(&MessageId("m-missing".into())).is_none());
            assert_eq!(hints.len(), 1);
        }
    }

    mod developer {
        use super::*;
        use mirrorlane_core::message::AuthorId;
        use mirrorlane_core::projection::Topic;
        use mirrorlane_core::skill::{
            DeveloperSnapshotRequest, ExpertCandidate, Participant, SkillScore, TopicOwnership,
        };
        use mirrorlane_core::{
            DeveloperSnapshotStore, InMemoryDeveloperSnapshotStore, InMemorySkillStore, SkillStore,
        };
        use mirrorlane_provider::SkillDeveloperSnapshotter;

        /// Seed a projection store with `message()` projected (id `m-1`, author
        /// `u-1`, topics `auth`/`sdk`, conversation `c-1`).
        async fn seeded_projections() -> Arc<InMemoryProjectionStore> {
            let store = Arc::new(InMemoryProjectionStore::new());
            run_jobs(store.clone(), vec![message()]).await;
            store
        }

        /// Seed a skill store: `u-1` owns both `auth` and `sdk`.
        fn seeded_skills() -> Arc<InMemorySkillStore> {
            let skills = Arc::new(InMemorySkillStore::new());
            for topic in ["auth", "sdk"] {
                skills.upsert(TopicOwnership {
                    topic: Topic(topic.into()),
                    candidates: vec![ExpertCandidate {
                        author: AuthorId("u-1".into()),
                        display_name: "Dev".into(),
                        score: SkillScore::new(1.0),
                    }],
                });
            }
            skills
        }

        fn request() -> DeveloperSnapshotRequest {
            DeveloperSnapshotRequest {
                conversation: ConversationId("c-1".into()),
                participants: vec![Participant {
                    author: AuthorId("u-1".into()),
                    display_name: "Dev".into(),
                }],
                messages: vec![MessageId("m-1".into())],
            }
        }

        async fn run_developer_jobs(
            projections: Arc<InMemoryProjectionStore>,
            skills: Arc<InMemorySkillStore>,
            developers: Arc<InMemoryDeveloperSnapshotStore>,
            requests: Vec<DeveloperSnapshotRequest>,
        ) {
            let broker = Arc::new(InMemoryBroker::new());
            let client = Client::new(broker.clone());
            let mut worker = Worker::new(broker.clone());
            worker
                .register(BuildDeveloperSnapshotJob::new(
                    projections,
                    skills,
                    Arc::new(SkillDeveloperSnapshotter::new()),
                    developers,
                ))
                .expect("register job");
            for request in requests {
                client
                    .enqueue::<BuildDeveloperSnapshotJob>(request)
                    .await
                    .expect("enqueue");
            }
            worker
                .build()
                .expect("build worker")
                .run_until_idle()
                .await
                .expect("run to idle");
        }

        #[tokio::test]
        async fn building_stores_developers_per_conversation() {
            let developers = Arc::new(InMemoryDeveloperSnapshotStore::new());
            run_developer_jobs(
                seeded_projections().await,
                seeded_skills(),
                developers.clone(),
                vec![request()],
            )
            .await;
            let session = developers
                .get(&ConversationId("c-1".into()))
                .expect("session developers stored");
            assert_eq!(session.developers[0].author, AuthorId("u-1".into()));
            assert!(!session.developers[0].topics.is_empty());
            assert_eq!(developers.len(), 1);
        }

        #[tokio::test]
        async fn redelivery_is_idempotent() {
            let developers = Arc::new(InMemoryDeveloperSnapshotStore::new());
            run_developer_jobs(
                seeded_projections().await,
                seeded_skills(),
                developers.clone(),
                vec![request(), request()],
            )
            .await;
            assert_eq!(
                developers.len(),
                1,
                "same conversation must yield one entry"
            );
        }
    }
}
