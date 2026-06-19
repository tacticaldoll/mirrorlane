//! Mirrorlane GitHub source: pull issues, pull requests, and comments from a
//! repository into the message log.
//!
//! This is the inbound half — GitHub as a *source*, feeding the pipeline. The
//! crate isolates the GitHub HTTP dependency; `mirrorlane-core` stays
//! GitHub-agnostic (it only knows `Source::GitHub`).
//!
//! [`to_message`] maps a [`GitHubItem`] deterministically, and [`ingest_repo`]
//! appends a repo's items to a `MessageStore` idempotently. The [`GitHubSource`]
//! port has a deterministic [`FixtureGitHubSource`] for tests and replay, and a
//! real [`RestGitHubSource`] that — like the Ollama projector — panics at the
//! boundary on failure, its live path verified behind an `#[ignore]`d test.
//!
//! The crate also closes the outbound loop: [`GitHubConsumer`] is the routing
//! consumer behind `ConsumerKind::GitHub`. It drafts an issue or PR description
//! from a routed projection via [`draft_for`] and records it (no GitHub write).

mod consumer;
mod model;
mod rest;
mod source;

pub use consumer::{DraftKind, GitHubConsumer, GitHubDraft, draft_for};
pub use model::{GitHubItem, GitHubItemKind, Repo, to_message};
pub use rest::{DEFAULT_BASE_URL, GitHubFetchError, RestGitHubSource};
pub use source::{FixtureGitHubSource, GitHubSource, ingest_items, ingest_repo};
