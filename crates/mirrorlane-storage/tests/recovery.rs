//! Cross-restart recovery: replaying a reopened durable log rebuilds the
//! pipeline.

use std::sync::Arc;

use mirrorlane_core::message::{
    Author, AuthorId, Conversation, ConversationId, MessageEnvelope, MessageId, Source,
};
use mirrorlane_core::{MessageStore, ProjectionStore, WarmupStore};
use mirrorlane_provider::{
    MessageSkillBuilder, MockProjector, MockScopeProjector, MockWarmupBuilder,
    SkillDeveloperSnapshotter, SkillRoutingHinter,
};
use mirrorlane_storage::SqliteMessageStore;
use mirrorlane_worker::Replay;
use tempfile::tempdir;

fn message(id: &str, body: &str) -> MessageEnvelope {
    MessageEnvelope {
        id: MessageId(id.into()),
        source: Source::Discord,
        author: Author {
            id: AuthorId("u-1".into()),
            display_name: "Dev".into(),
        },
        conversation: Conversation {
            id: ConversationId("c-1".into()),
            thread: None,
        },
        body: body.into(),
    }
}

#[tokio::test]
async fn replay_recovers_pipeline_from_reopened_log() {
    let dir = tempdir().expect("temp dir");
    let path = dir.path().join("log.db");

    // Ingest, then drop the store (simulating shutdown).
    {
        let store = SqliteMessageStore::open(&path).expect("open store");
        store.append(message(
            "m-1",
            "We will use sqlite for the auth sdk oauth refresh-token store.",
        ));
        store.append(message("m-2", "Should we expose a refresh endpoint?"));
    }

    // "Restart": reopen the durable log and replay it.
    let store = SqliteMessageStore::open(&path).expect("open store");
    let replay = Replay::new(
        Arc::new(MockProjector::new()),
        Arc::new(MockScopeProjector::new()),
        Arc::new(MockWarmupBuilder::new()),
        Arc::new(MessageSkillBuilder::new()),
        Arc::new(SkillRoutingHinter::new()),
        Arc::new(SkillDeveloperSnapshotter::new()),
    );
    let stores = replay.run(&store).await;

    assert!(stores.projections.get(&MessageId("m-1".into())).is_some());
    assert!(stores.projections.get(&MessageId("m-2".into())).is_some());
    assert!(stores.warmups.get(&ConversationId("c-1".into())).is_some());
}
