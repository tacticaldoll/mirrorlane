//! The full pipeline as Worklane jobs: project → scope → warm-up.
//!
//! Run with: `cargo run --example session_warmup`

use std::sync::Arc;

use mirrorlane_core::message::{
    Author, AuthorId, Conversation, ConversationId, MessageEnvelope, MessageId, Source,
};
use mirrorlane_core::scope::ScopeRequest;
use mirrorlane_core::warmup::WarmupRequest;
use mirrorlane_core::{
    InMemoryProjectionStore, InMemoryScopeStore, InMemoryWarmupStore, WarmupStore,
};
use mirrorlane_provider::{MockProjector, MockScopeProjector, MockWarmupBuilder};
use mirrorlane_worker::{BuildScopeJob, BuildWarmupJob, ProcessMessageJob};
use worklane::{Client, Worker};
use worklane_memory::InMemoryBroker;

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
    let projections = Arc::new(InMemoryProjectionStore::new());
    let scopes = Arc::new(InMemoryScopeStore::new());
    let warmups = Arc::new(InMemoryWarmupStore::new());
    let conversation = ConversationId("c-1".into());

    let messages = vec![
        message(
            "m-1",
            "We will use sqlite for the auth sdk oauth refresh-token store.",
        ),
        message("m-2", "Should we expose a refresh endpoint in the sdk?"),
        message("m-3", "We need to add ci for the auth crate."),
    ];
    let message_ids: Vec<MessageId> = messages.iter().map(|m| m.id.clone()).collect();

    // Phase 1: project each message.
    {
        let broker = Arc::new(InMemoryBroker::new());
        let client = Client::new(broker.clone());
        let mut worker = Worker::new(broker.clone());
        worker.register(ProcessMessageJob::new(
            Arc::new(MockProjector::new()),
            projections.clone(),
        ))?;
        for message in messages {
            client.enqueue::<ProcessMessageJob>(message).await?;
        }
        worker.build()?.run_until_idle().await?;
    }

    // Phase 2: build the session scope.
    {
        let broker = Arc::new(InMemoryBroker::new());
        let client = Client::new(broker.clone());
        let mut worker = Worker::new(broker.clone());
        worker.register(BuildScopeJob::new(
            projections.clone(),
            Arc::new(MockScopeProjector::new()),
            scopes.clone(),
        ))?;
        client
            .enqueue::<BuildScopeJob>(ScopeRequest {
                conversation: conversation.clone(),
                messages: message_ids.clone(),
            })
            .await?;
        worker.build()?.run_until_idle().await?;
    }

    // Phase 3: build the warm-up document.
    {
        let broker = Arc::new(InMemoryBroker::new());
        let client = Client::new(broker.clone());
        let mut worker = Worker::new(broker.clone());
        worker.register(BuildWarmupJob::new(
            projections.clone(),
            scopes.clone(),
            Arc::new(MockWarmupBuilder::new()),
            warmups.clone(),
        ))?;
        client
            .enqueue::<BuildWarmupJob>(WarmupRequest {
                conversation: conversation.clone(),
                messages: message_ids,
            })
            .await?;
        worker.build()?.run_until_idle().await?;
    }

    // Resume the session: read the stored warm-up.
    let document = warmups.get(&conversation).expect("warm-up was built");
    println!("{}\n", document.summary);
    println!("{}", serde_json::to_string_pretty(&document)?);
    Ok(())
}
