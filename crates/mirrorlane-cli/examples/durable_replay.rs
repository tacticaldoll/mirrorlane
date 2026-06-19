//! Durable recovery: ingest into a file-backed log, "restart", and replay it.
//!
//! Run with: `cargo run --example durable_replay`

use std::sync::Arc;

use mirrorlane_core::message::{
    Author, AuthorId, Conversation, ConversationId, MessageEnvelope, MessageId, Source,
};
use mirrorlane_core::{MessageStore, WarmupStore};
use mirrorlane_provider::{
    MessageSkillBuilder, MockProjector, MockScopeProjector, MockWarmupBuilder,
    SkillDeveloperSnapshotter, SkillRoutingHinter,
};
use mirrorlane_storage::SqliteMessageStore;
use mirrorlane_worker::Replay;

fn message(id: &str, body: &str) -> MessageEnvelope {
    MessageEnvelope {
        id: MessageId(id.into()),
        source: Source::Discord,
        author: Author {
            id: AuthorId("u-alice".into()),
            display_name: "Alice".into(),
        },
        conversation: Conversation {
            id: ConversationId("c-1".into()),
            thread: None,
        },
        body: body.into(),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::temp_dir().join("mirrorlane-durable-replay.db");
    let _ = std::fs::remove_file(&path); // fresh start

    // Ingest into the durable log, then drop it (simulating shutdown).
    {
        let log = SqliteMessageStore::open(&path)?;
        log.append(message(
            "m-1",
            "We will use sqlite for the auth sdk oauth refresh-token store.",
        ));
        log.append(message(
            "m-2",
            "Should we expose a refresh endpoint in the sdk?",
        ));
        log.append(message("m-3", "We need to add ci for the auth crate."));
        println!("Ingested 3 messages into {}", path.display());
    }

    // "Restart": reopen the durable log and replay it to rebuild the pipeline.
    let log = SqliteMessageStore::open(&path)?;
    println!(
        "Reopened the log; {} message(s) recovered.\n",
        log.all().len()
    );

    let replay = Replay::new(
        Arc::new(MockProjector::new()),
        Arc::new(MockScopeProjector::new()),
        Arc::new(MockWarmupBuilder::new()),
        Arc::new(MessageSkillBuilder::new()),
        Arc::new(SkillRoutingHinter::new()),
        Arc::new(SkillDeveloperSnapshotter::new()),
    );
    let stores = replay.run(&log).await;

    let document = stores
        .warmups
        .get(&ConversationId("c-1".into()))
        .expect("warm-up rebuilt from the durable log");
    println!("{}", document.summary);

    let _ = std::fs::remove_file(&path);
    Ok(())
}
