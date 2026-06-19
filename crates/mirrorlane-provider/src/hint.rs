//! A deterministic, skill-derived routing hinter.
//!
//! Given a projection and the topic ownerships for its topics, this aggregates
//! each candidate's skill score across those topics (a straight sum, so a
//! candidate strong in several of the projection's topics outranks one strong in
//! a single topic), ranks reviewers best-first, and names the top one as the
//! human hint. Ties break by ascending author id and scores normalize so the
//! strongest reviewer scores `1.0` — the same rules `MessageSkillBuilder` uses,
//! so the two never disagree. Given the same inputs it always produces the same
//! hint, so it re-derives identically under replay.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use mirrorlane_core::RoutingHinter;
use mirrorlane_core::message::AuthorId;
use mirrorlane_core::projection::Projection;
use mirrorlane_core::routing::RoutingHint;
use mirrorlane_core::skill::{ExpertCandidate, SkillScore, TopicOwnership};

/// A deterministic [`RoutingHinter`] over the skill index.
#[derive(Debug, Default, Clone)]
pub struct SkillRoutingHinter;

impl SkillRoutingHinter {
    /// Create a skill routing hinter.
    pub fn new() -> Self {
        Self
    }
}

impl RoutingHinter for SkillRoutingHinter {
    fn hint(&self, projection: &Projection, ownerships: &[TopicOwnership]) -> RoutingHint {
        // Restrict to the projection's own topics, so an ownership for an
        // unrelated topic never leaks a reviewer in.
        let topics: BTreeSet<&str> = projection.topics.iter().map(|t| t.0.as_str()).collect();

        // author id -> (display name, summed score). BTreeMap keeps iteration
        // order stable, so the output does not depend on hashing.
        let mut totals: BTreeMap<String, (String, f64)> = BTreeMap::new();
        for ownership in ownerships {
            if !topics.contains(ownership.topic.0.as_str()) {
                continue;
            }
            for candidate in &ownership.candidates {
                let entry = totals
                    .entry(candidate.author.0.clone())
                    .or_insert_with(|| (candidate.display_name.clone(), 0.0));
                // Last-seen display name wins, deterministically by topic order.
                entry.0 = candidate.display_name.clone();
                entry.1 += candidate.score.get();
            }
        }

        // Normalize so the strongest reviewer scores 1.0, like the skill builder.
        let max = totals
            .values()
            .map(|(_, score)| *score)
            .fold(0.0_f64, f64::max);
        let mut reviewers: Vec<ExpertCandidate> = totals
            .into_iter()
            .map(|(author, (display_name, score))| {
                let normalized = if max > 0.0 { score / max } else { 0.0 };
                ExpertCandidate {
                    author: AuthorId(author),
                    display_name,
                    score: SkillScore::new(normalized),
                }
            })
            .collect();
        reviewers.sort_by(|a, b| {
            b.score
                .get()
                .partial_cmp(&a.score.get())
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.author.0.cmp(&b.author.0))
        });

        let human_hint = reviewers.first().cloned();
        RoutingHint {
            message_id: projection.message_id.clone(),
            reviewers,
            human_hint,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mirrorlane_core::message::MessageId;
    use mirrorlane_core::projection::{Confidence, Intent, Topic};

    fn projection(topics: &[&str]) -> Projection {
        Projection {
            message_id: MessageId("m-1".into()),
            intent: Intent::Issue,
            topics: topics.iter().map(|t| Topic((*t).into())).collect(),
            entities: Vec::new(),
            confidence: Confidence::new(0.9),
        }
    }

    fn candidate(author: &str, score: f64) -> ExpertCandidate {
        ExpertCandidate {
            author: AuthorId(author.into()),
            display_name: author.into(),
            score: SkillScore::new(score),
        }
    }

    fn ownership(topic: &str, candidates: Vec<ExpertCandidate>) -> TopicOwnership {
        TopicOwnership {
            topic: Topic(topic.into()),
            candidates,
        }
    }

    #[test]
    fn strongest_across_topics_is_recommended_first() {
        let hinter = SkillRoutingHinter::new();
        // alice owns both auth and sdk; bob owns only auth. alice's aggregate wins.
        let ownerships = vec![
            ownership("auth", vec![candidate("alice", 1.0), candidate("bob", 0.8)]),
            ownership("sdk", vec![candidate("alice", 0.6)]),
        ];
        let hint = hinter.hint(&projection(&["auth", "sdk"]), &ownerships);
        assert_eq!(hint.reviewers[0].author, AuthorId("alice".into()));
        assert_eq!(hint.human_hint.unwrap().author, AuthorId("alice".into()));
    }

    #[test]
    fn unrelated_topics_do_not_leak_reviewers() {
        let hinter = SkillRoutingHinter::new();
        let ownerships = vec![
            ownership("auth", vec![candidate("alice", 1.0)]),
            ownership("billing", vec![candidate("carol", 1.0)]),
        ];
        // The projection only touches auth, so carol must not appear.
        let hint = hinter.hint(&projection(&["auth"]), &ownerships);
        assert_eq!(hint.reviewers.len(), 1);
        assert_eq!(hint.reviewers[0].author, AuthorId("alice".into()));
    }

    #[test]
    fn equal_candidates_are_ordered_by_author_id() {
        let hinter = SkillRoutingHinter::new();
        let ownerships = vec![ownership(
            "auth",
            vec![candidate("bob", 0.7), candidate("alice", 0.7)],
        )];
        let hint = hinter.hint(&projection(&["auth"]), &ownerships);
        assert_eq!(hint.reviewers[0].author, AuthorId("alice".into()));
        assert_eq!(hint.reviewers[1].author, AuthorId("bob".into()));
        assert_eq!(hint.reviewers[0].score.get(), hint.reviewers[1].score.get());
    }

    #[test]
    fn same_inputs_build_identical_hint() {
        let hinter = SkillRoutingHinter::new();
        let ownerships = vec![ownership(
            "auth",
            vec![candidate("alice", 0.9), candidate("bob", 0.5)],
        )];
        let p = projection(&["auth"]);
        assert_eq!(hinter.hint(&p, &ownerships), hinter.hint(&p, &ownerships));
    }

    #[test]
    fn no_owners_yields_empty_hint() {
        let hinter = SkillRoutingHinter::new();
        let hint = hinter.hint(&projection(&["auth"]), &[]);
        assert!(hint.reviewers.is_empty());
        assert!(hint.human_hint.is_none());
    }
}
