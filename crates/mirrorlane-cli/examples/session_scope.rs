//! Projects a session's messages, then builds its scope — both as Worklane jobs.
//!
//! Run with: `cargo run --example session_scope`

use std::sync::Arc;

use mirrorlane_core::message::{
    Author, AuthorId, Conversation, ConversationId, MessageEnvelope, MessageId, Source,
};
use mirrorlane_core::scope::ScopeRequest;
use mirrorlane_core::{InMemoryProjectionStore, InMemoryScopeStore, ScopeStore};
use mirrorlane_provider::{MockProjector, MockScopeProjector};
use mirrorlane_worker::{BuildScopeJob, ProcessMessageJob};
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
    let conversation = ConversationId("c-1".into());

    let messages = vec![
        message("m-1", "Let's finish the auth sdk oauth refresh-token work."),
        message("m-2", "Thanks everyone!"),
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

    // Phase 2: build the session scope from the stored projections.
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
                messages: message_ids,
            })
            .await?;
        worker.build()?.run_until_idle().await?;
    }

    let scope = scopes.get(&conversation).expect("scope was built");
    println!("{}", serde_json::to_string_pretty(&scope)?);
    Ok(())
}
