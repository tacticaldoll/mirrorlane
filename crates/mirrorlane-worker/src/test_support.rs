//! Shared test scaffolding for the worker crate's test modules.
//!
//! The derivation context, a seeded one-message log, and empty replay stores were
//! reconstructed identically across several test modules; they live here once.
//! Compiled only under `cfg(test)`, so it adds nothing to a release build.

use std::sync::Arc;

use mirrorlane_core::message::{
    Author, AuthorId, Conversation, ConversationId, MessageEnvelope, MessageId, Source,
};
use mirrorlane_core::{
    InMemoryDeveloperSnapshotStore, InMemoryMessageStore, InMemoryProjectionStore,
    InMemoryRoutingHintStore, InMemoryScopeStore, InMemorySkillStore, InMemoryWarmupStore,
    MessageStore,
};
use mirrorlane_provider::{
    MessageSkillBuilder, MockProjector, MockScopeProjector, MockWarmupBuilder,
    SkillDeveloperSnapshotter, SkillRoutingHinter,
};

use crate::{ReplayStores, StrategyContext};

/// The reference derivation context, wired with the provider mocks.
pub(crate) fn context() -> StrategyContext {
    StrategyContext {
        projector: Arc::new(MockProjector::new()),
        scoper: Arc::new(MockScopeProjector::new()),
        builder: Arc::new(MockWarmupBuilder::new()),
        skill_builder: Arc::new(MessageSkillBuilder::new()),
        hinter: Arc::new(SkillRoutingHinter::new()),
        snapshotter: Arc::new(SkillDeveloperSnapshotter::new()),
    }
}

/// A one-message log (conversation `c-1`), wrapped in `Arc` for jobs that hold a
/// shared store.
pub(crate) fn seeded_log() -> Arc<InMemoryMessageStore> {
    let log = Arc::new(InMemoryMessageStore::new());
    log.append(MessageEnvelope {
        id: MessageId("m-1".into()),
        source: Source::Discord,
        author: Author {
            id: AuthorId("u-1".into()),
            display_name: "Dev".into(),
        },
        conversation: Conversation {
            id: ConversationId("c-1".into()),
            thread: None,
        },
        body: "We will use sqlite for the auth sdk.".into(),
    });
    log
}

/// Empty replay stores, for a strategy that produces no domain output.
pub(crate) fn empty_stores() -> ReplayStores {
    ReplayStores {
        projections: Arc::new(InMemoryProjectionStore::new()),
        scopes: Arc::new(InMemoryScopeStore::new()),
        warmups: Arc::new(InMemoryWarmupStore::new()),
        skills: Arc::new(InMemorySkillStore::new()),
        hints: Arc::new(InMemoryRoutingHintStore::new()),
        developers: Arc::new(InMemoryDeveloperSnapshotStore::new()),
    }
}
