//! Ingest GitHub items (from a deterministic fixture) into the log, replay, and
//! print the warm-up — GitHub as a source, end to end, with no network.
//!
//! Run with: `cargo run --example github_ingest`

use std::sync::Arc;

use mirrorlane_core::message::ConversationId;
use mirrorlane_core::{InMemoryMessageStore, WarmupStore};
use mirrorlane_github::{FixtureGitHubSource, GitHubItem, GitHubItemKind, Repo, ingest_repo};
use mirrorlane_provider::{
    MessageSkillBuilder, MockProjector, MockScopeProjector, MockWarmupBuilder,
    SkillDeveloperSnapshotter, SkillRoutingHinter,
};
use mirrorlane_worker::Replay;

fn issue(number: u64, login: &str, title: &str, body: &str) -> GitHubItem {
    GitHubItem {
        kind: GitHubItemKind::Issue,
        repo: Repo::new("acme", "auth-sdk"),
        number,
        id: number.to_string(),
        author_login: login.into(),
        title: Some(title.into()),
        body: body.into(),
    }
}

fn comment(id: &str, number: u64, login: &str, body: &str) -> GitHubItem {
    GitHubItem {
        kind: GitHubItemKind::Comment,
        repo: Repo::new("acme", "auth-sdk"),
        number,
        id: id.into(),
        author_login: login.into(),
        title: None,
        body: body.into(),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = FixtureGitHubSource::new(vec![
        issue(
            12,
            "alice",
            "Refresh tokens expire early",
            "We will use sqlite for the auth sdk oauth refresh-token store.",
        ),
        comment(
            "c-1",
            12,
            "bob",
            "Should we expose a refresh endpoint in the sdk?",
        ),
    ]);

    // Ingest the repo's items into the log (idempotent by stable id).
    let log = InMemoryMessageStore::new();
    let ids = ingest_repo(&source, &Repo::new("acme", "auth-sdk"), &log);
    println!("Ingested {} GitHub item(s) into the log.", ids.len());

    // Replay the pipeline over the ingested messages.
    let replay = Replay::new(
        Arc::new(MockProjector::new()),
        Arc::new(MockScopeProjector::new()),
        Arc::new(MockWarmupBuilder::new()),
        Arc::new(MessageSkillBuilder::new()),
        Arc::new(SkillRoutingHinter::new()),
        Arc::new(SkillDeveloperSnapshotter::new()),
    );
    let stores = replay.run(&log).await;

    let conversation = ConversationId("github:acme/auth-sdk#12".into());
    if let Some(document) = stores.warmups.get(&conversation) {
        println!("\n{}", document.summary);
    }
    Ok(())
}
