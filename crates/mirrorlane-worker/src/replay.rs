//! Replay: re-derive the whole projection pipeline from the message log.
//!
//! Replaying re-runs the real Worklane jobs (project → scope → warm-up) over a
//! fresh in-memory broker into fresh stores, faithful to how a production replay
//! would re-enqueue and re-process. Because the ports are deterministic and the
//! stores upsert idempotently, replaying the same log always yields the same
//! result — the core contract.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use mirrorlane_core::message::{AuthorId, ConversationId, MessageEnvelope, MessageId};
use mirrorlane_core::routing::RoutingHintRequest;
use mirrorlane_core::scope::ScopeRequest;
use mirrorlane_core::skill::{DeveloperSnapshotRequest, Participant, SkillEntry, SkillRequest};
use mirrorlane_core::warmup::WarmupRequest;
use mirrorlane_core::{
    DeveloperSnapshotBuilder, InMemoryDeveloperSnapshotStore, InMemoryProjectionStore,
    InMemoryRoutingHintStore, InMemoryScopeStore, InMemorySkillStore, InMemoryWarmupStore,
    MessageStore, Projector, RoutingHinter, ScopeProjector, SkillBuilder, WarmupBuilder,
};
use worklane::async_trait;

use crate::composition::{global, per_conversation, per_message, run_stage};
use crate::strategy::Strategy;

use crate::{
    BuildDeveloperSnapshotJob, BuildRoutingHintJob, BuildScopeJob, BuildSkillJob, BuildWarmupJob,
    ProcessMessageJob,
};

/// The fresh stores produced by a replay.
pub struct ReplayStores {
    pub projections: Arc<InMemoryProjectionStore>,
    pub scopes: Arc<InMemoryScopeStore>,
    pub warmups: Arc<InMemoryWarmupStore>,
    pub skills: Arc<InMemorySkillStore>,
    pub hints: Arc<InMemoryRoutingHintStore>,
    pub developers: Arc<InMemoryDeveloperSnapshotStore>,
}

/// The **reference strategy**: re-derives the projection pipeline from a message
/// log. This is the canonical [`Strategy`] instance
/// (`dyn MessageStore -> ReplayStores`); [`Replay`] is kept as an alias for it.
pub struct ProjectionStrategy {
    projector: Arc<dyn Projector>,
    scoper: Arc<dyn ScopeProjector>,
    builder: Arc<dyn WarmupBuilder>,
    skill_builder: Arc<dyn SkillBuilder>,
    hinter: Arc<dyn RoutingHinter>,
    snapshotter: Arc<dyn DeveloperSnapshotBuilder>,
}

impl ProjectionStrategy {
    /// Build a replay from the six deterministic ports.
    pub fn new(
        projector: Arc<dyn Projector>,
        scoper: Arc<dyn ScopeProjector>,
        builder: Arc<dyn WarmupBuilder>,
        skill_builder: Arc<dyn SkillBuilder>,
        hinter: Arc<dyn RoutingHinter>,
        snapshotter: Arc<dyn DeveloperSnapshotBuilder>,
    ) -> Self {
        Self {
            projector,
            scoper,
            builder,
            skill_builder,
            hinter,
            snapshotter,
        }
    }

    /// Replay the log: project every message, then build scope and warm-up per
    /// conversation, into fresh stores. Retained as an inherent method so callers
    /// holding a concrete value keep `.run(..)` working; the [`Strategy`] impl
    /// delegates here.
    pub async fn run(&self, messages: &dyn MessageStore) -> ReplayStores {
        self.run_phases(messages).await
    }

    async fn run_phases(&self, messages: &dyn MessageStore) -> ReplayStores {
        let projections = Arc::new(InMemoryProjectionStore::new());
        let scopes = Arc::new(InMemoryScopeStore::new());
        let warmups = Arc::new(InMemoryWarmupStore::new());
        let skills = Arc::new(InMemorySkillStore::new());
        let hints = Arc::new(InMemoryRoutingHintStore::new());
        let developers = Arc::new(InMemoryDeveloperSnapshotStore::new());

        let log = messages.all();
        let conversations = group_by_conversation(&log);

        // Phase 1: project every message.
        run_stage(
            ProcessMessageJob::new(self.projector.clone(), projections.clone()),
            per_message(&log, |message| message.clone()),
        )
        .await;

        // Phase 2: build scope per conversation.
        run_stage(
            BuildScopeJob::new(projections.clone(), self.scoper.clone(), scopes.clone()),
            per_conversation(&conversations, |conversation, message_ids| ScopeRequest {
                conversation: conversation.clone(),
                messages: message_ids.to_vec(),
            }),
        )
        .await;

        // Phase 3: build warm-up per conversation.
        run_stage(
            BuildWarmupJob::new(
                projections.clone(),
                scopes.clone(),
                self.builder.clone(),
                warmups.clone(),
            ),
            per_conversation(&conversations, |conversation, message_ids| WarmupRequest {
                conversation: conversation.clone(),
                messages: message_ids.to_vec(),
            }),
        )
        .await;

        // Phase 4: build the global skill index across all conversations. Unlike
        // phases 2-3, this is a single job over the whole log, not one per
        // conversation, because expertise spans conversations.
        run_stage(
            BuildSkillJob::new(
                projections.clone(),
                self.skill_builder.clone(),
                skills.clone(),
            ),
            global(|| SkillRequest {
                entries: log
                    .iter()
                    .map(|message| SkillEntry {
                        author: message.author.id.clone(),
                        display_name: message.author.display_name.clone(),
                        message: message.id.clone(),
                    })
                    .collect(),
            }),
        )
        .await;

        // Phase 5: build a routing hint per message from the skill index. Runs
        // after the skill phase so the ownerships it reads are populated. Like
        // the skill phase it is one job over the whole log; unlike routing
        // dispatch it is replayable derived state, so replay re-runs it.
        run_stage(
            BuildRoutingHintJob::new(
                projections.clone(),
                skills.clone(),
                self.hinter.clone(),
                hints.clone(),
            ),
            global(|| RoutingHintRequest {
                messages: log.iter().map(|message| message.id.clone()).collect(),
            }),
        )
        .await;

        // Phase 6: build a developer snapshot per conversation from the skill
        // index. Runs after the skill phase so the ownerships it reads are
        // populated. Like the skill and hint phases it is replayable derived
        // state, so replay re-runs it; it dispatches nothing.
        let participants = participants_by_conversation(&log);
        run_stage(
            BuildDeveloperSnapshotJob::new(
                projections.clone(),
                skills.clone(),
                self.snapshotter.clone(),
                developers.clone(),
            ),
            per_conversation(&conversations, |conversation, message_ids| {
                DeveloperSnapshotRequest {
                    conversation: conversation.clone(),
                    participants: participants.get(conversation).cloned().unwrap_or_default(),
                    messages: message_ids.to_vec(),
                }
            }),
        )
        .await;

        ReplayStores {
            projections,
            scopes,
            warmups,
            skills,
            hints,
            developers,
        }
    }
}

/// `Replay` is the historical name for the reference strategy.
pub type Replay = ProjectionStrategy;

#[async_trait]
impl Strategy for ProjectionStrategy {
    type Input = dyn MessageStore;
    type Output = ReplayStores;

    async fn run(&self, input: &dyn MessageStore) -> ReplayStores {
        self.run_phases(input).await
    }
}

/// The distinct participants of each conversation, keyed by conversation id.
/// Within a conversation, authors are ordered by id and the last-seen display
/// name wins, matching `MessageSkillBuilder`; the snapshotter re-sorts anyway, so
/// only the (author, name) content matters for the output.
fn participants_by_conversation(
    log: &[MessageEnvelope],
) -> HashMap<ConversationId, Vec<Participant>> {
    let mut by_conversation: HashMap<ConversationId, BTreeMap<String, String>> = HashMap::new();
    for message in log {
        by_conversation
            .entry(message.conversation.id.clone())
            .or_default()
            .insert(
                message.author.id.0.clone(),
                message.author.display_name.clone(),
            );
    }
    by_conversation
        .into_iter()
        .map(|(conversation, authors)| {
            let participants = authors
                .into_iter()
                .map(|(author, display_name)| Participant {
                    author: AuthorId(author),
                    display_name,
                })
                .collect();
            (conversation, participants)
        })
        .collect()
}

/// Group a log's messages by conversation, preserving first-seen order of both
/// conversations and messages.
fn group_by_conversation(log: &[MessageEnvelope]) -> Vec<(ConversationId, Vec<MessageId>)> {
    let mut order: Vec<ConversationId> = Vec::new();
    let mut grouped: HashMap<ConversationId, Vec<MessageId>> = HashMap::new();
    for message in log {
        let conversation = message.conversation.id.clone();
        if !grouped.contains_key(&conversation) {
            order.push(conversation.clone());
        }
        grouped
            .entry(conversation)
            .or_default()
            .push(message.id.clone());
    }
    order
        .into_iter()
        .map(|conversation| {
            let ids = grouped.remove(&conversation).unwrap_or_default();
            (conversation, ids)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use mirrorlane_core::message::{Author, AuthorId, Conversation, Source};
    use mirrorlane_core::projection::Topic;
    use mirrorlane_core::{
        DeveloperSnapshotStore, InMemoryMessageStore, ProjectionStore, RoutingHintStore,
        ScopeStore, SkillStore, WarmupStore,
    };
    use mirrorlane_provider::{
        MessageSkillBuilder, MockProjector, MockScopeProjector, MockWarmupBuilder,
        SkillDeveloperSnapshotter, SkillRoutingHinter,
    };

    fn replay() -> Replay {
        Replay::new(
            Arc::new(MockProjector::new()),
            Arc::new(MockScopeProjector::new()),
            Arc::new(MockWarmupBuilder::new()),
            Arc::new(MessageSkillBuilder::new()),
            Arc::new(SkillRoutingHinter::new()),
            Arc::new(SkillDeveloperSnapshotter::new()),
        )
    }

    fn message(id: &str, conversation: &str, body: &str) -> MessageEnvelope {
        MessageEnvelope {
            id: MessageId(id.into()),
            source: Source::Discord,
            author: Author {
                id: AuthorId("u-1".into()),
                display_name: "Dev".into(),
            },
            conversation: Conversation {
                id: ConversationId(conversation.into()),
                thread: None,
            },
            body: body.into(),
        }
    }

    fn seeded_log() -> InMemoryMessageStore {
        let log = InMemoryMessageStore::new();
        log.append(message(
            "m-1",
            "c-1",
            "We will use sqlite for the auth sdk oauth refresh-token store.",
        ));
        log.append(message(
            "m-2",
            "c-1",
            "Should we expose a refresh endpoint?",
        ));
        log.append(message(
            "m-3",
            "c-2",
            "We need to add ci for the infra crate.",
        ));
        log
    }

    #[tokio::test]
    async fn replay_is_deterministic() {
        let log = seeded_log();
        let first = replay().run(&log).await;
        let second = replay().run(&log).await;

        for message in log.all() {
            assert_eq!(
                first.projections.get(&message.id),
                second.projections.get(&message.id),
                "projection differs for {:?}",
                message.id
            );
        }
        for conversation in [ConversationId("c-1".into()), ConversationId("c-2".into())] {
            assert_eq!(
                first.scopes.get(&conversation),
                second.scopes.get(&conversation),
                "scope differs for {conversation:?}"
            );
            assert_eq!(
                first.warmups.get(&conversation),
                second.warmups.get(&conversation),
                "warm-up differs for {conversation:?}"
            );
        }
        for topic in ["auth", "sdk", "oauth", "ci", "infra"] {
            let topic = Topic(topic.into());
            assert_eq!(
                first.skills.get(&topic),
                second.skills.get(&topic),
                "skill ownership differs for {topic:?}"
            );
        }
        for message in log.all() {
            assert_eq!(
                first.hints.get(&message.id),
                second.hints.get(&message.id),
                "routing hint differs for {:?}",
                message.id
            );
        }
        for conversation in [ConversationId("c-1".into()), ConversationId("c-2".into())] {
            assert_eq!(
                first.developers.get(&conversation),
                second.developers.get(&conversation),
                "session developers differ for {conversation:?}"
            );
        }
    }

    #[tokio::test]
    async fn replay_derives_a_hint_for_every_message() {
        let log = seeded_log();
        let stores = replay().run(&log).await;
        for message in log.all() {
            assert!(
                stores.hints.get(&message.id).is_some(),
                "no routing hint for {:?}",
                message.id
            );
        }
        assert_eq!(stores.hints.len(), log.len());
    }

    #[tokio::test]
    async fn replay_derives_developers_for_every_conversation() {
        let log = seeded_log();
        let stores = replay().run(&log).await;
        for conversation in [ConversationId("c-1".into()), ConversationId("c-2".into())] {
            let session = stores
                .developers
                .get(&conversation)
                .unwrap_or_else(|| panic!("no session developers for {conversation:?}"));
            assert!(
                !session.developers.is_empty(),
                "conversation {conversation:?} has participants"
            );
        }
        assert_eq!(stores.developers.len(), 2);
    }

    #[tokio::test]
    async fn replay_accounts_for_every_message() {
        let log = seeded_log();
        let stores = replay().run(&log).await;

        for message in log.all() {
            assert!(
                stores.projections.get(&message.id).is_some(),
                "no projection for {:?}",
                message.id
            );
        }
        assert_eq!(
            stores.projections.len(),
            log.len(),
            "projection count must equal distinct message count"
        );
    }

    #[tokio::test]
    async fn duplicate_ingest_does_not_duplicate_projections() {
        let log = seeded_log();
        log.append(message(
            "m-1",
            "c-1",
            "We will use sqlite for the auth sdk.",
        )); // same id again
        let stores = replay().run(&log).await;

        assert_eq!(log.len(), 3, "log dedups by id");
        assert_eq!(stores.projections.len(), 3, "no duplicate projections");
    }
}
