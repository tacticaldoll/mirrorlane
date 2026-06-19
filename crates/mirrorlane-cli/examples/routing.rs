//! Route projected messages to consumers — the orchestrator's output edge.
//!
//! Projects a few messages, runs `RouteJob` against a registry of recording
//! consumers, and prints each routing decision and the receipts per consumer.
//!
//! Run with: `cargo run --example routing`

use std::sync::Arc;

use mirrorlane_core::message::{
    Author, AuthorId, Conversation, ConversationId, MessageEnvelope, MessageId, Source,
};
use mirrorlane_core::routing::{ConsumerKind, RoutingRequest};
use mirrorlane_core::{
    ConsumerRegistry, InMemoryProjectionStore, InMemoryRoutingStore, ProjectionStore, Projector,
    RoutingStore,
};
use mirrorlane_github::GitHubConsumer;
use mirrorlane_provider::{MockProjector, RecordingConsumer, RuleRouter};
use mirrorlane_worker::RouteJob;
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
    let messages = vec![
        message("m-1", "Should we use sqlite for the auth sdk?"),
        message("m-2", "We need to add ci for the infra crate."),
        message("m-3", "I propose we adopt the rust backend."),
        message("m-4", "hey there"),
    ];

    // Project the messages into a store (the router reads projections).
    let projections = Arc::new(InMemoryProjectionStore::new());
    let projector = MockProjector::new();
    for message in &messages {
        projections.upsert(projector.project(message));
    }

    // Recording consumers for the simple targets, and a real GitHub consumer
    // that drafts issues / PR descriptions.
    let human = Arc::new(RecordingConsumer::new());
    let worklane_job = Arc::new(RecordingConsumer::new());
    let github = Arc::new(GitHubConsumer::new());
    let mut registry = ConsumerRegistry::new();
    registry.register(ConsumerKind::Human, human.clone());
    registry.register(ConsumerKind::WorklaneJob, worklane_job.clone());
    registry.register(ConsumerKind::GitHub, github.clone());

    // Run the RouteJob over all messages.
    let routes = Arc::new(InMemoryRoutingStore::new());
    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone());
    let mut worker = Worker::new(broker.clone());
    let traces = Arc::new(mirrorlane_core::InMemoryRoutingTraceStore::new());

    worker.register(RouteJob::new(
        projections,
        Arc::new(RuleRouter::new()),
        routes.clone(),
        traces,
        Arc::new(registry),
    ))?;
    client
        .enqueue::<RouteJob>(RoutingRequest {
            messages: messages.iter().map(|m| m.id.clone()).collect(),
        })
        .await?;
    worker.build()?.run_until_idle().await?;

    println!("Routing decisions:");
    for message in &messages {
        if let Some(decision) = routes.get(&message.id) {
            let flag = if decision.escalated {
                " (escalated)"
            } else {
                ""
            };
            println!(
                "  {} -> {:?}{}: {}",
                message.id.0, decision.target, flag, decision.reason
            );
        }
    }

    println!(
        "\nReceipts: human={}, worklane_job={}, github={}",
        human.len(),
        worklane_job.len(),
        github.len()
    );

    println!("\nGitHub drafts:");
    for message in &messages {
        if let Some(draft) = github.get(&message.id) {
            println!("  {:?}: {}", draft.kind, draft.title);
        }
    }
    Ok(())
}
