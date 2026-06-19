//! Build a message log, replay the whole pipeline from it, and print the
//! warm-up — proving replay re-derives context from the log.
//!
//! Run with: `cargo run --example replay`

use std::sync::Arc;

use mirrorlane_core::message::{
    Author, AuthorId, Conversation, ConversationId, MessageEnvelope, MessageId, Source,
};
use mirrorlane_core::projection::Topic;
use mirrorlane_core::{InMemoryMessageStore, MessageStore, SkillStore, WarmupStore};
use mirrorlane_provider::{
    MessageSkillBuilder, MockProjector, MockScopeProjector, MockWarmupBuilder,
    SkillDeveloperSnapshotter, SkillRoutingHinter,
};
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
    let log = InMemoryMessageStore::new();
    log.append(message(
        "m-1",
        "We will use sqlite for the auth sdk oauth refresh-token store.",
    ));
    log.append(message(
        "m-2",
        "Should we expose a refresh endpoint in the sdk?",
    ));
    log.append(message("m-3", "We need to add ci for the auth crate."));

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
        .expect("warm-up was built");
    println!("Replayed {} message(s) from the log.\n", log.len());
    println!("{}", document.summary);

    if let Some(ownership) = stores.skills.get(&Topic("auth".into())) {
        let experts: Vec<String> = ownership
            .candidates
            .iter()
            .map(|c| format!("{} ({:.2})", c.display_name, c.score.get()))
            .collect();
        println!("\nTopic 'auth' experts: {}", experts.join(", "));
    }
    Ok(())
}
