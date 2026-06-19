//! The projection store port and an in-memory adapter.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::message::{ConversationId, MessageId};
use crate::projection::{Projection, Topic};
use crate::routing::{RoutingDecision, RoutingHint, RoutingTrace};
use crate::scope::Scope;
use crate::skill::{SessionDevelopers, TopicOwnership};
use crate::warmup::WarmupDocument;

/// Persists projections, keyed by [`MessageId`].
///
/// `upsert` replaces any existing projection for the same id, so processing a
/// message more than once (Worklane delivers at-least-once) leaves exactly one
/// projection. A durable adapter lives in `mirrorlane-storage`.
pub trait ProjectionStore: Send + Sync {
    /// Insert or replace the projection for its message id.
    fn upsert(&self, projection: Projection);

    /// Fetch the projection for a message id, if any.
    fn get(&self, id: &MessageId) -> Option<Projection>;
}

/// An in-memory [`ProjectionStore`] backed by a `HashMap`. Uses std only.
#[derive(Debug, Default)]
pub struct InMemoryProjectionStore {
    inner: Mutex<HashMap<MessageId, Projection>>,
}

impl InMemoryProjectionStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of stored projections.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether the store holds no projections.
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<MessageId, Projection>> {
        self.inner.lock().expect("projection store mutex poisoned")
    }
}

impl ProjectionStore for InMemoryProjectionStore {
    fn upsert(&self, projection: Projection) {
        self.lock()
            .insert(projection.message_id.clone(), projection);
    }

    fn get(&self, id: &MessageId) -> Option<Projection> {
        self.lock().get(id).cloned()
    }
}

/// Persists scopes, keyed by [`ConversationId`].
///
/// `upsert` replaces any existing scope for the same conversation, so rebuilding
/// a session's scope (Worklane delivers at-least-once) leaves exactly one scope.
pub trait ScopeStore: Send + Sync {
    /// Insert or replace the scope for its conversation id.
    fn upsert(&self, scope: Scope);

    /// Fetch the scope for a conversation id, if any.
    fn get(&self, id: &ConversationId) -> Option<Scope>;
}

/// An in-memory [`ScopeStore`] backed by a `HashMap`. Uses std only.
#[derive(Debug, Default)]
pub struct InMemoryScopeStore {
    inner: Mutex<HashMap<ConversationId, Scope>>,
}

impl InMemoryScopeStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of stored scopes.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether the store holds no scopes.
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<ConversationId, Scope>> {
        self.inner.lock().expect("scope store mutex poisoned")
    }
}

impl ScopeStore for InMemoryScopeStore {
    fn upsert(&self, scope: Scope) {
        self.lock().insert(scope.conversation.clone(), scope);
    }

    fn get(&self, id: &ConversationId) -> Option<Scope> {
        self.lock().get(id).cloned()
    }
}

/// Persists warm-up documents, keyed by [`ConversationId`].
///
/// `get` is how a session resumes. `upsert` replaces any existing document for
/// the same conversation, so rebuilding a session's warm-up leaves exactly one.
pub trait WarmupStore: Send + Sync {
    /// Insert or replace the warm-up document for its conversation id.
    fn upsert(&self, document: WarmupDocument);

    /// Fetch (resume) the warm-up document for a conversation id, if any.
    fn get(&self, id: &ConversationId) -> Option<WarmupDocument>;
}

/// An in-memory [`WarmupStore`] backed by a `HashMap`. Uses std only.
#[derive(Debug, Default)]
pub struct InMemoryWarmupStore {
    inner: Mutex<HashMap<ConversationId, WarmupDocument>>,
}

impl InMemoryWarmupStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of stored documents.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether the store holds no documents.
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<ConversationId, WarmupDocument>> {
        self.inner.lock().expect("warmup store mutex poisoned")
    }
}

impl WarmupStore for InMemoryWarmupStore {
    fn upsert(&self, document: WarmupDocument) {
        self.lock().insert(document.conversation.clone(), document);
    }

    fn get(&self, id: &ConversationId) -> Option<WarmupDocument> {
        self.lock().get(id).cloned()
    }
}

/// Persists topic ownership, keyed by [`Topic`].
///
/// `upsert` replaces any existing ownership for the same topic, so rebuilding the
/// skill index (Worklane delivers at-least-once) leaves exactly one ownership per
/// topic.
pub trait SkillStore: Send + Sync {
    /// Insert or replace the ownership for its topic.
    fn upsert(&self, ownership: TopicOwnership);

    /// Fetch the ownership for a topic, if any.
    fn get(&self, topic: &Topic) -> Option<TopicOwnership>;
}

/// An in-memory [`SkillStore`] backed by a `HashMap`. Uses std only.
#[derive(Debug, Default)]
pub struct InMemorySkillStore {
    inner: Mutex<HashMap<Topic, TopicOwnership>>,
}

impl InMemorySkillStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of stored ownerships.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether the store holds no ownerships.
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<Topic, TopicOwnership>> {
        self.inner.lock().expect("skill store mutex poisoned")
    }
}

impl SkillStore for InMemorySkillStore {
    fn upsert(&self, ownership: TopicOwnership) {
        self.lock().insert(ownership.topic.clone(), ownership);
    }

    fn get(&self, topic: &Topic) -> Option<TopicOwnership> {
        self.lock().get(topic).cloned()
    }
}

/// Persists routing decisions, keyed by [`MessageId`].
///
/// `upsert` replaces any existing decision for the same message, so re-deriving a
/// decision (Worklane delivers at-least-once) leaves exactly one per message.
pub trait RoutingStore: Send + Sync {
    /// Insert or replace the decision for its message id.
    fn upsert(&self, decision: RoutingDecision);

    /// Fetch the decision for a message id, if any.
    fn get(&self, id: &MessageId) -> Option<RoutingDecision>;
}

/// An in-memory [`RoutingStore`] backed by a `HashMap`. Uses std only.
#[derive(Debug, Default)]
pub struct InMemoryRoutingStore {
    inner: Mutex<HashMap<MessageId, RoutingDecision>>,
}

impl InMemoryRoutingStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of stored decisions.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether the store holds no decisions.
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<MessageId, RoutingDecision>> {
        self.inner.lock().expect("routing store mutex poisoned")
    }
}

impl RoutingStore for InMemoryRoutingStore {
    fn upsert(&self, decision: RoutingDecision) {
        self.lock().insert(decision.message_id.clone(), decision);
    }

    fn get(&self, id: &MessageId) -> Option<RoutingDecision> {
        self.lock().get(id).cloned()
    }
}

/// Persists routing traces, keyed by [`MessageId`].
///
/// `upsert` replaces any existing trace for the same message.
pub trait RoutingTraceStore: Send + Sync {
    /// Insert or replace the trace for its message id.
    fn upsert(&self, trace: RoutingTrace);

    /// Fetch the trace for a message id, if any.
    fn get(&self, id: &MessageId) -> Option<RoutingTrace>;
}

/// An in-memory [`RoutingTraceStore`] backed by a `HashMap`. Uses std only.
#[derive(Debug, Default)]
pub struct InMemoryRoutingTraceStore {
    inner: Mutex<HashMap<MessageId, RoutingTrace>>,
}

impl InMemoryRoutingTraceStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of stored traces.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether the store holds no traces.
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<MessageId, RoutingTrace>> {
        self.inner
            .lock()
            .expect("routing trace store mutex poisoned")
    }
}

impl RoutingTraceStore for InMemoryRoutingTraceStore {
    fn upsert(&self, trace: RoutingTrace) {
        self.lock().insert(trace.message_id.clone(), trace);
    }

    fn get(&self, id: &MessageId) -> Option<RoutingTrace> {
        self.lock().get(id).cloned()
    }
}

/// Persists routing hints, keyed by [`MessageId`].
///
/// `upsert` replaces any existing hint for the same message, so re-deriving a
/// hint (Worklane delivers at-least-once) leaves exactly one per message.
pub trait RoutingHintStore: Send + Sync {
    /// Insert or replace the hint for its message id.
    fn upsert(&self, hint: RoutingHint);

    /// Fetch the hint for a message id, if any.
    fn get(&self, id: &MessageId) -> Option<RoutingHint>;
}

/// An in-memory [`RoutingHintStore`] backed by a `HashMap`. Uses std only.
#[derive(Debug, Default)]
pub struct InMemoryRoutingHintStore {
    inner: Mutex<HashMap<MessageId, RoutingHint>>,
}

impl InMemoryRoutingHintStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of stored hints.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether the store holds no hints.
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<MessageId, RoutingHint>> {
        self.inner
            .lock()
            .expect("routing hint store mutex poisoned")
    }
}

impl RoutingHintStore for InMemoryRoutingHintStore {
    fn upsert(&self, hint: RoutingHint) {
        self.lock().insert(hint.message_id.clone(), hint);
    }

    fn get(&self, id: &MessageId) -> Option<RoutingHint> {
        self.lock().get(id).cloned()
    }
}

/// Persists session developers, keyed by [`ConversationId`].
///
/// `upsert` replaces any existing entry for the same conversation, so re-deriving
/// the snapshot (Worklane delivers at-least-once) leaves exactly one per
/// conversation.
pub trait DeveloperSnapshotStore: Send + Sync {
    /// Insert or replace the developers for their conversation id.
    fn upsert(&self, developers: SessionDevelopers);

    /// Fetch the developers for a conversation id, if any.
    fn get(&self, id: &ConversationId) -> Option<SessionDevelopers>;
}

/// An in-memory [`DeveloperSnapshotStore`] backed by a `HashMap`. Uses std only.
#[derive(Debug, Default)]
pub struct InMemoryDeveloperSnapshotStore {
    inner: Mutex<HashMap<ConversationId, SessionDevelopers>>,
}

impl InMemoryDeveloperSnapshotStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of stored entries.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether the store holds no entries.
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<ConversationId, SessionDevelopers>> {
        self.inner
            .lock()
            .expect("developer snapshot store mutex poisoned")
    }
}

impl DeveloperSnapshotStore for InMemoryDeveloperSnapshotStore {
    fn upsert(&self, developers: SessionDevelopers) {
        self.lock()
            .insert(developers.conversation.clone(), developers);
    }

    fn get(&self, id: &ConversationId) -> Option<SessionDevelopers> {
        self.lock().get(id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projection::{Confidence, Intent};

    fn projection(id: &str, confidence: f64) -> Projection {
        Projection {
            message_id: MessageId(id.into()),
            intent: Intent::Social,
            topics: Vec::new(),
            entities: Vec::new(),
            confidence: Confidence::new(confidence),
        }
    }

    #[test]
    fn upsert_is_keyed_by_message_id() {
        let store = InMemoryProjectionStore::new();
        store.upsert(projection("m-1", 0.1));
        store.upsert(projection("m-1", 0.9));

        assert_eq!(store.len(), 1, "same id must not duplicate");
        assert_eq!(
            store
                .get(&MessageId("m-1".into()))
                .unwrap()
                .confidence
                .get(),
            0.9
        );
    }

    #[test]
    fn get_returns_stored_projection() {
        let store = InMemoryProjectionStore::new();
        let p = projection("m-2", 0.5);
        store.upsert(p.clone());
        assert_eq!(store.get(&MessageId("m-2".into())), Some(p));
    }

    fn scope(conversation: &str, reason: &str) -> Scope {
        Scope {
            conversation: ConversationId(conversation.into()),
            load: Vec::new(),
            ignore: Vec::new(),
            reason: reason.into(),
            confidence: Confidence::new(0.0),
        }
    }

    #[test]
    fn scope_upsert_is_keyed_by_conversation_id() {
        let store = InMemoryScopeStore::new();
        store.upsert(scope("c-1", "first"));
        store.upsert(scope("c-1", "second"));

        assert_eq!(store.len(), 1, "same conversation must not duplicate");
        assert_eq!(
            store.get(&ConversationId("c-1".into())).unwrap().reason,
            "second"
        );
    }

    fn document(conversation: &str, summary: &str) -> WarmupDocument {
        WarmupDocument {
            conversation: ConversationId(conversation.into()),
            focus: Vec::new(),
            decisions: Vec::new(),
            open_questions: Vec::new(),
            tasks: Vec::new(),
            summary: summary.into(),
        }
    }

    #[test]
    fn warmup_resume_returns_stored_document() {
        let store = InMemoryWarmupStore::new();
        let doc = document("c-1", "first");
        store.upsert(doc.clone());
        assert_eq!(store.get(&ConversationId("c-1".into())), Some(doc));
    }

    #[test]
    fn warmup_upsert_is_keyed_by_conversation_id() {
        let store = InMemoryWarmupStore::new();
        store.upsert(document("c-1", "first"));
        store.upsert(document("c-1", "second"));

        assert_eq!(store.len(), 1, "same conversation must not duplicate");
        assert_eq!(
            store.get(&ConversationId("c-1".into())).unwrap().summary,
            "second"
        );
    }

    fn ownership(topic: &str, score: f64) -> TopicOwnership {
        use crate::message::AuthorId;
        use crate::skill::{ExpertCandidate, SkillScore};
        TopicOwnership {
            topic: Topic(topic.into()),
            candidates: vec![ExpertCandidate {
                author: AuthorId("u-1".into()),
                display_name: "Dev".into(),
                score: SkillScore::new(score),
            }],
        }
    }

    #[test]
    fn skill_upsert_is_keyed_by_topic() {
        let store = InMemorySkillStore::new();
        store.upsert(ownership("auth", 0.4));
        store.upsert(ownership("auth", 0.9));

        assert_eq!(store.len(), 1, "same topic must not duplicate");
        assert_eq!(
            store.get(&Topic("auth".into())).unwrap().candidates[0]
                .score
                .get(),
            0.9
        );
    }

    fn decision(message: &str, reason: &str) -> RoutingDecision {
        use crate::routing::ConsumerKind;
        RoutingDecision {
            message_id: MessageId(message.into()),
            target: ConsumerKind::Human,
            reason: reason.into(),
            escalated: false,
        }
    }

    #[test]
    fn routing_upsert_is_keyed_by_message_id() {
        let store = InMemoryRoutingStore::new();
        store.upsert(decision("m-1", "first"));
        store.upsert(decision("m-1", "second"));

        assert_eq!(store.len(), 1, "same message must not duplicate");
        assert_eq!(
            store.get(&MessageId("m-1".into())).unwrap().reason,
            "second"
        );
    }

    fn hint(message: &str, reviewer: &str) -> RoutingHint {
        use crate::message::AuthorId;
        use crate::skill::{ExpertCandidate, SkillScore};
        let candidate = ExpertCandidate {
            author: AuthorId(reviewer.into()),
            display_name: reviewer.into(),
            score: SkillScore::new(1.0),
        };
        RoutingHint {
            message_id: MessageId(message.into()),
            reviewers: vec![candidate.clone()],
            human_hint: Some(candidate),
        }
    }

    #[test]
    fn routing_hint_upsert_is_keyed_by_message_id() {
        let store = InMemoryRoutingHintStore::new();
        store.upsert(hint("m-1", "alice"));
        store.upsert(hint("m-1", "bob"));

        assert_eq!(store.len(), 1, "same message must not duplicate");
        assert_eq!(
            store.get(&MessageId("m-1".into())).unwrap().human_hint,
            Some({
                use crate::message::AuthorId;
                use crate::skill::{ExpertCandidate, SkillScore};
                ExpertCandidate {
                    author: AuthorId("bob".into()),
                    display_name: "bob".into(),
                    score: SkillScore::new(1.0),
                }
            }),
            "upsert keeps the most recent hint"
        );
    }

    fn session_developers(conversation: &str, display_name: &str) -> SessionDevelopers {
        use crate::message::AuthorId;
        use crate::skill::DeveloperSnapshot;
        SessionDevelopers {
            conversation: ConversationId(conversation.into()),
            developers: vec![DeveloperSnapshot {
                author: AuthorId("u-1".into()),
                display_name: display_name.into(),
                topics: Vec::new(),
            }],
        }
    }

    #[test]
    fn developer_snapshot_upsert_is_keyed_by_conversation_id() {
        let store = InMemoryDeveloperSnapshotStore::new();
        store.upsert(session_developers("c-1", "first"));
        store.upsert(session_developers("c-1", "second"));

        assert_eq!(store.len(), 1, "same conversation must not duplicate");
        assert_eq!(
            store.get(&ConversationId("c-1".into())).unwrap().developers[0].display_name,
            "second"
        );
    }
}
