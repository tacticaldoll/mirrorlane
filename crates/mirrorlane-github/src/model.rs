//! The GitHub item model and its deterministic mapping to a message.

use serde::{Deserialize, Serialize};

use mirrorlane_core::message::{
    Author, AuthorId, Conversation, ConversationId, MessageEnvelope, MessageId, Source,
};

/// A GitHub repository, by owner and name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Repo {
    pub owner: String,
    pub name: String,
}

impl Repo {
    /// Create a repo from owner and name.
    pub fn new(owner: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            owner: owner.into(),
            name: name.into(),
        }
    }
}

/// What kind of GitHub item a [`GitHubItem`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GitHubItemKind {
    Issue,
    PullRequest,
    Comment,
}

impl GitHubItemKind {
    /// A stable, url-safe tag used in derived ids.
    fn tag(self) -> &'static str {
        match self {
            GitHubItemKind::Issue => "issue",
            GitHubItemKind::PullRequest => "pull-request",
            GitHubItemKind::Comment => "comment",
        }
    }
}

/// A normalized GitHub item: an issue, pull request, or comment. `number` is the
/// issue/PR number the item belongs to (a comment's parent), so comments thread
/// into their issue's conversation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitHubItem {
    pub kind: GitHubItemKind,
    pub repo: Repo,
    pub number: u64,
    pub id: String,
    pub author_login: String,
    pub title: Option<String>,
    pub body: String,
}

/// Map a [`GitHubItem`] to a [`MessageEnvelope`] deterministically.
///
/// The `MessageId` is stable (repo + kind + id), so re-ingesting the same item
/// dedups in the log. An issue/PR and its comments share one `ConversationId`.
pub fn to_message(item: &GitHubItem) -> MessageEnvelope {
    let id = MessageId(format!(
        "github:{}/{}:{}:{}",
        item.repo.owner,
        item.repo.name,
        item.kind.tag(),
        item.id
    ));
    let conversation = ConversationId(format!(
        "github:{}/{}#{}",
        item.repo.owner, item.repo.name, item.number
    ));
    let body = match (item.kind, &item.title) {
        (GitHubItemKind::Comment, _) | (_, None) => item.body.clone(),
        (_, Some(title)) => format!("{title}\n\n{}", item.body),
    };
    MessageEnvelope {
        id,
        source: Source::GitHub,
        author: Author {
            id: AuthorId(item.author_login.clone()),
            display_name: item.author_login.clone(),
        },
        conversation: Conversation {
            id: conversation,
            thread: None,
        },
        body,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo() -> Repo {
        Repo::new("acme", "widgets")
    }

    fn issue() -> GitHubItem {
        GitHubItem {
            kind: GitHubItemKind::Issue,
            repo: repo(),
            number: 7,
            id: "7".into(),
            author_login: "alice".into(),
            title: Some("Login is broken".into()),
            body: "Steps to reproduce...".into(),
        }
    }

    fn comment() -> GitHubItem {
        GitHubItem {
            kind: GitHubItemKind::Comment,
            repo: repo(),
            number: 7,
            id: "c-100".into(),
            author_login: "bob".into(),
            title: None,
            body: "I can repro too.".into(),
        }
    }

    #[test]
    fn item_round_trips_through_json() {
        let json = serde_json::to_string(&issue()).expect("serialize");
        let back: GitHubItem = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(issue(), back);
    }

    #[test]
    fn item_maps_to_a_github_sourced_message() {
        let message = to_message(&issue());
        assert_eq!(message.source, Source::GitHub);
        assert_eq!(message.author.id, AuthorId("alice".into()));
        assert_eq!(
            message.conversation.id,
            ConversationId("github:acme/widgets#7".into())
        );
        assert!(message.body.starts_with("Login is broken"));
    }

    #[test]
    fn issue_and_comment_share_a_conversation() {
        let issue_msg = to_message(&issue());
        let comment_msg = to_message(&comment());
        assert_eq!(issue_msg.conversation.id, comment_msg.conversation.id);
        assert_ne!(issue_msg.id, comment_msg.id);
    }

    #[test]
    fn mapping_is_stable() {
        assert_eq!(to_message(&issue()), to_message(&issue()));
    }
}
