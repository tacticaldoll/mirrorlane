//! Projects a message into structured context via a Worklane job.
//!
//! Run with: `cargo run --example message_projection`

use std::sync::Arc;

use mirrorlane_core::message::{
    Author, AuthorId, Conversation, ConversationId, MessageEnvelope, MessageId, Source,
};
use mirrorlane_core::{InMemoryProjectionStore, ProjectionStore};
use mirrorlane_provider::MockProjector;
use mirrorlane_worker::ProcessMessageJob;
use worklane::{Client, Worker};
use worklane_memory::InMemoryBroker;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryProjectionStore::new());

    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone());
    let mut worker = Worker::new(broker.clone());
    worker.register(ProcessMessageJob::new(
        Arc::new(MockProjector::new()),
        store.clone(),
    ))?;

    let message = MessageEnvelope {
        id: MessageId("m-1".into()),
        source: Source::Discord,
        author: Author {
            id: AuthorId("u-alice".into()),
            display_name: "Alice".into(),
        },
        conversation: Conversation {
            id: ConversationId("c-1".into()),
            thread: None,
        },
        body: "Should we use sqlite for the auth sdk refresh-token store?".into(),
    };
    let id = message.id.clone();

    client.enqueue::<ProcessMessageJob>(message).await?;
    worker.build()?.run_until_idle().await?;

    let projection = store.get(&id).expect("projection was stored");
    println!("{}", serde_json::to_string_pretty(&projection)?);
    Ok(())
}
