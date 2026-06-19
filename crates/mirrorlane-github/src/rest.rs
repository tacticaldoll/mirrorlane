//! A real [`GitHubSource`] over the GitHub REST API.
//!
//! Isolated here so the rest of the crate (model, mapping, fixture) stays free of
//! HTTP. Like the Ollama projector, the port is sync and infallible and this
//! adapter **panics at the boundary** on a transport error, a non-success status,
//! or an unparseable response — it never fabricates items. The live path is
//! exercised only by an `#[ignore]`d test, so the crate builds and unit-tests
//! without network access.

use std::fmt;

use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::model::{GitHubItem, GitHubItemKind, Repo};
use crate::source::GitHubSource;

/// Default GitHub REST API base URL.
pub const DEFAULT_BASE_URL: &str = "https://api.github.com";

/// A failure fetching from the GitHub REST API, surfaced by
/// [`RestGitHubSource::try_fetch`] instead of a panic so a CLI caller can report
/// it cleanly. Carries the HTTP status for a non-success response.
#[derive(Debug)]
pub enum GitHubFetchError {
    /// The request never produced an HTTP response (DNS, connect, TLS, timeout).
    Transport(String),
    /// The server returned a non-success status.
    Status { code: u16, message: String },
    /// The response body could not be read.
    Read(String),
    /// The response body was not the expected JSON shape.
    Parse(String),
}

impl fmt::Display for GitHubFetchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(e) => write!(f, "github request failed: {e}"),
            Self::Status { code, message } => {
                write!(f, "github returned HTTP {code} {message}")
            }
            Self::Read(e) => write!(f, "github response was not readable: {e}"),
            Self::Parse(e) => write!(f, "github response was malformed: {e}"),
        }
    }
}

impl std::error::Error for GitHubFetchError {}

/// A [`GitHubSource`] backed by the GitHub REST API.
pub struct RestGitHubSource {
    base_url: String,
    token: Option<String>,
}

impl RestGitHubSource {
    /// Build a source against `base_url` with an optional token. The token is only
    /// ever sent to the default GitHub host (see [`Self::sends_token`]); if a token
    /// is supplied for a non-default base URL it is withheld, and that is announced
    /// here so the operator is not surprised by unauthenticated requests.
    pub fn new(base_url: impl Into<String>, token: Option<String>) -> Self {
        let base_url = base_url.into();
        if token.is_some() && base_url != DEFAULT_BASE_URL {
            eprintln!(
                "github: withholding GITHUB_TOKEN for non-default base URL {base_url} \
                 (token is only sent to {DEFAULT_BASE_URL})"
            );
        }
        Self { base_url, token }
    }

    /// A source against the public API, taking the token from `GITHUB_TOKEN`.
    pub fn from_env() -> Self {
        Self::new(DEFAULT_BASE_URL, std::env::var("GITHUB_TOKEN").ok())
    }

    /// Whether the `GITHUB_TOKEN` `Authorization` header is attached: only when a
    /// token is present **and** the base URL is the default host, so a redirected
    /// or attacker-supplied endpoint never receives the credential.
    fn sends_token(&self) -> bool {
        self.token.is_some() && self.base_url == DEFAULT_BASE_URL
    }

    fn get<T: DeserializeOwned>(&self, url: &str) -> Result<T, GitHubFetchError> {
        let mut request = ureq::get(url)
            .set("User-Agent", "mirrorlane")
            .set("Accept", "application/vnd.github+json");
        if self.sends_token() {
            let token = self.token.as_deref().expect("sends_token implies a token");
            request = request.set("Authorization", &format!("token {token}"));
        }
        let response = request.call().map_err(|e| match e {
            ureq::Error::Status(code, resp) => GitHubFetchError::Status {
                code,
                message: resp.status_text().to_string(),
            },
            ureq::Error::Transport(t) => GitHubFetchError::Transport(t.to_string()),
        })?;
        let body = response
            .into_string()
            .map_err(|e| GitHubFetchError::Read(e.to_string()))?;
        serde_json::from_str(&body).map_err(|e| GitHubFetchError::Parse(e.to_string()))
    }
}

#[derive(Debug, Deserialize)]
struct UserDto {
    login: String,
}

#[derive(Debug, Deserialize)]
struct IssueDto {
    number: u64,
    title: Option<String>,
    body: Option<String>,
    user: UserDto,
    /// Present on entries that are actually pull requests.
    pull_request: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct CommentDto {
    id: u64,
    body: Option<String>,
    user: UserDto,
    issue_url: String,
}

/// The parent issue/PR number a comment's `issue_url` points at, best effort.
fn number_from_issue_url(issue_url: &str) -> u64 {
    issue_url
        .rsplit('/')
        .next()
        .and_then(|n| n.parse().ok())
        .unwrap_or(0)
}

impl RestGitHubSource {
    /// Fetch a repo's items, returning a typed error instead of panicking. This is
    /// the path the one-shot `github` CLI command uses, so a transport, status,
    /// read, or parse failure becomes a clean error rather than a crash. The
    /// infallible [`GitHubSource::fetch`] is implemented on top of this.
    pub fn try_fetch(&self, repo: &Repo) -> Result<Vec<GitHubItem>, GitHubFetchError> {
        let mut items = Vec::new();

        let issues_url = format!(
            "{}/repos/{}/{}/issues?state=all&per_page=100",
            self.base_url, repo.owner, repo.name
        );
        for issue in self.get::<Vec<IssueDto>>(&issues_url)? {
            let kind = if issue.pull_request.is_some() {
                GitHubItemKind::PullRequest
            } else {
                GitHubItemKind::Issue
            };
            items.push(GitHubItem {
                kind,
                repo: repo.clone(),
                number: issue.number,
                id: issue.number.to_string(),
                author_login: issue.user.login,
                title: issue.title,
                body: issue.body.unwrap_or_default(),
            });
        }

        let comments_url = format!(
            "{}/repos/{}/{}/issues/comments?per_page=100",
            self.base_url, repo.owner, repo.name
        );
        for comment in self.get::<Vec<CommentDto>>(&comments_url)? {
            items.push(GitHubItem {
                kind: GitHubItemKind::Comment,
                repo: repo.clone(),
                number: number_from_issue_url(&comment.issue_url),
                id: format!("c-{}", comment.id),
                author_login: comment.user.login,
                title: None,
                body: comment.body.unwrap_or_default(),
            });
        }

        Ok(items)
    }
}

impl GitHubSource for RestGitHubSource {
    /// The infallible port: delegates to [`Self::try_fetch`] and panics at the
    /// boundary on failure, preserving the documented convention for the
    /// replay/Worklane path (where a panic is caught, retried, and dead-lettered).
    fn fetch(&self, repo: &Repo) -> Vec<GitHubItem> {
        self.try_fetch(repo)
            .unwrap_or_else(|e| panic!("github fetch failed: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mirrorlane_core::message::Source;

    use crate::model::to_message;

    #[test]
    fn parses_a_number_from_an_issue_url() {
        assert_eq!(
            number_from_issue_url("https://api.github.com/repos/acme/widgets/issues/42"),
            42
        );
    }

    #[test]
    fn token_is_sent_only_to_the_default_host() {
        // Token present + default host → sent.
        assert!(RestGitHubSource::new(DEFAULT_BASE_URL, Some("t".into())).sends_token());
        // Token present + non-default host → withheld.
        assert!(!RestGitHubSource::new("https://ghe.example.com", Some("t".into())).sends_token());
        // No token → nothing to send, even on the default host.
        assert!(!RestGitHubSource::new(DEFAULT_BASE_URL, None).sends_token());
    }

    /// Live path: requires network access (and, to avoid rate limits,
    /// `GITHUB_TOKEN`). Run with `cargo test -p mirrorlane-github -- --ignored`.
    #[test]
    #[ignore = "requires network access to api.github.com"]
    fn live_fetch_against_public_repo() {
        let source = RestGitHubSource::from_env();
        let items = source.fetch(&Repo::new("rust-lang", "rust"));
        assert!(!items.is_empty(), "a busy public repo yields items");
        for item in &items {
            assert_eq!(to_message(item).source, Source::GitHub);
        }
    }
}
