//! The GitHub source port, a deterministic fixture, and repo ingestion.

use mirrorlane_core::MessageStore;
use mirrorlane_core::message::MessageId;

use crate::model::{GitHubItem, Repo, to_message};

/// Fetches the [`GitHubItem`]s for a repository.
///
/// Synchronous and infallible, like the projector ports. The deterministic
/// [`FixtureGitHubSource`] backs tests and replay-safe ingestion; a real adapter
/// (see `rest`) panics at the boundary on failure rather than fabricating items.
pub trait GitHubSource: Send + Sync {
    fn fetch(&self, repo: &Repo) -> Vec<GitHubItem>;
}

/// A deterministic [`GitHubSource`] backed by canned items.
#[derive(Debug, Default, Clone)]
pub struct FixtureGitHubSource {
    items: Vec<GitHubItem>,
}

impl FixtureGitHubSource {
    /// Create a fixture source from canned items.
    pub fn new(items: Vec<GitHubItem>) -> Self {
        Self { items }
    }
}

impl GitHubSource for FixtureGitHubSource {
    fn fetch(&self, _repo: &Repo) -> Vec<GitHubItem> {
        self.items.clone()
    }
}

/// Fetch a repo's items through `source`, map each to a message, and append it to
/// `store`. Returns the appended message ids in fetch order.
///
/// Idempotent: message ids are stable and the log dedups by id, so re-ingesting
/// the same items leaves exactly one message per item.
pub fn ingest_repo(
    source: &dyn GitHubSource,
    repo: &Repo,
    store: &dyn MessageStore,
) -> Vec<MessageId> {
    ingest_items(&source.fetch(repo), store)
}

/// Map already-fetched `items` to messages and append them to `store`, returning
/// the appended ids in order. The mapping half of [`ingest_repo`], shared with the
/// fallible CLI path that fetches via `RestGitHubSource::try_fetch` (so a fetch
/// failure is surfaced as an error before this is reached). Idempotent by id.
pub fn ingest_items(items: &[GitHubItem], store: &dyn MessageStore) -> Vec<MessageId> {
    items
        .iter()
        .map(|item| {
            let message = to_message(item);
            let id = message.id.clone();
            store.append(message);
            id
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use mirrorlane_core::InMemoryMessageStore;
    use mirrorlane_core::message::Source;

    use crate::model::GitHubItemKind;

    fn item(id: &str, number: u64) -> GitHubItem {
        GitHubItem {
            kind: GitHubItemKind::Issue,
            repo: Repo::new("acme", "widgets"),
            number,
            id: id.into(),
            author_login: "alice".into(),
            title: Some("Title".into()),
            body: "Body".into(),
        }
    }

    fn repo() -> Repo {
        Repo::new("acme", "widgets")
    }

    #[test]
    fn fixture_returns_its_items() {
        let source = FixtureGitHubSource::new(vec![item("1", 1)]);
        assert_eq!(source.fetch(&repo()).len(), 1);
    }

    #[test]
    fn ingesting_appends_one_message_per_item() {
        let source = FixtureGitHubSource::new(vec![item("1", 1), item("2", 2)]);
        let store = InMemoryMessageStore::new();
        let ids = ingest_repo(&source, &repo(), &store);

        assert_eq!(ids.len(), 2);
        assert_eq!(store.len(), 2);
        for message in store.all() {
            assert_eq!(message.source, Source::GitHub);
        }
    }

    #[test]
    fn re_ingesting_does_not_duplicate() {
        let source = FixtureGitHubSource::new(vec![item("1", 1), item("2", 2)]);
        let store = InMemoryMessageStore::new();
        ingest_repo(&source, &repo(), &store);
        ingest_repo(&source, &repo(), &store);

        assert_eq!(store.len(), 2, "stable ids dedup on re-ingest");
    }
}
