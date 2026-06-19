//! A deterministic, message-based skill builder.
//!
//! This is a real builder, not a mock stand-in: expertise from messages is a
//! deterministic aggregation, needing no model. It tallies each
//! author's per-topic involvement (weighted by projection confidence), then per
//! topic normalizes so the strongest contributor scores `1.0` and ranks
//! candidates best-first, breaking ties by ascending author id. Given the same
//! contributions it always produces the same index, so replay stays deterministic.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use mirrorlane_core::SkillBuilder;
use mirrorlane_core::message::AuthorId;
use mirrorlane_core::projection::Topic;
use mirrorlane_core::skill::{
    DeveloperProfile, ExpertCandidate, SkillContribution, SkillIndex, SkillScore, TopicOwnership,
    TopicWeight,
};

/// A deterministic [`SkillBuilder`] over message contributions.
#[derive(Debug, Default, Clone)]
pub struct MessageSkillBuilder;

impl MessageSkillBuilder {
    /// Create a message skill builder.
    pub fn new() -> Self {
        Self
    }
}

impl SkillBuilder for MessageSkillBuilder {
    fn build(&self, contributions: &[SkillContribution]) -> SkillIndex {
        // author id -> (display name, topic -> weight). BTreeMaps keep iteration
        // order stable, so the output does not depend on hashing.
        let mut per_author: BTreeMap<String, (String, BTreeMap<String, f64>)> = BTreeMap::new();

        for contribution in contributions {
            let entry = per_author
                .entry(contribution.author.0.clone())
                .or_insert_with(|| (contribution.display_name.clone(), BTreeMap::new()));
            // Last-seen display name wins, deterministically by contribution order.
            entry.0 = contribution.display_name.clone();
            for topic in &contribution.topics {
                *entry.1.entry(topic.0.clone()).or_insert(0.0) += contribution.confidence.get();
            }
        }

        let profiles: Vec<DeveloperProfile> = per_author
            .iter()
            .map(|(author, (display_name, topics))| DeveloperProfile {
                author: AuthorId(author.clone()),
                display_name: display_name.clone(),
                topics: topics
                    .iter()
                    .map(|(topic, weight)| TopicWeight {
                        topic: Topic(topic.clone()),
                        weight: *weight,
                    })
                    .collect(),
            })
            .collect();

        // Invert into topic -> contributors, then rank and normalize per topic.
        let mut per_topic: BTreeMap<String, Vec<(String, String, f64)>> = BTreeMap::new();
        for (author, (display_name, topics)) in &per_author {
            for (topic, weight) in topics {
                per_topic.entry(topic.clone()).or_default().push((
                    author.clone(),
                    display_name.clone(),
                    *weight,
                ));
            }
        }

        let ownerships: Vec<TopicOwnership> = per_topic
            .iter()
            .map(|(topic, contributors)| {
                let max = contributors
                    .iter()
                    .map(|(_, _, weight)| *weight)
                    .fold(0.0_f64, f64::max);
                let mut candidates: Vec<ExpertCandidate> = contributors
                    .iter()
                    .map(|(author, display_name, weight)| {
                        let score = if max > 0.0 { weight / max } else { 0.0 };
                        ExpertCandidate {
                            author: AuthorId(author.clone()),
                            display_name: display_name.clone(),
                            score: SkillScore::new(score),
                        }
                    })
                    .collect();
                candidates.sort_by(|a, b| {
                    b.score
                        .get()
                        .partial_cmp(&a.score.get())
                        .unwrap_or(Ordering::Equal)
                        .then_with(|| a.author.0.cmp(&b.author.0))
                });
                TopicOwnership {
                    topic: Topic(topic.clone()),
                    candidates,
                }
            })
            .collect();

        SkillIndex {
            profiles,
            ownerships,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mirrorlane_core::projection::Confidence;

    fn contribution(author: &str, topics: &[&str], confidence: f64) -> SkillContribution {
        SkillContribution {
            author: AuthorId(author.into()),
            display_name: author.into(),
            topics: topics.iter().map(|t| Topic((*t).into())).collect(),
            confidence: Confidence::new(confidence),
        }
    }

    fn ownership_for<'a>(index: &'a SkillIndex, topic: &str) -> &'a TopicOwnership {
        index
            .ownerships
            .iter()
            .find(|o| o.topic.0 == topic)
            .expect("topic present")
    }

    #[test]
    fn strongest_contributor_scores_highest() {
        let builder = MessageSkillBuilder::new();
        // alice touches auth twice, bob once.
        let index = builder.build(&[
            contribution("alice", &["auth"], 0.9),
            contribution("alice", &["auth"], 0.9),
            contribution("bob", &["auth"], 0.9),
        ]);
        let auth = ownership_for(&index, "auth");
        assert_eq!(auth.candidates[0].author, AuthorId("alice".into()));
        assert_eq!(auth.candidates[0].score.get(), 1.0);
        assert!(auth.candidates[1].score.get() < 1.0);
    }

    #[test]
    fn same_input_builds_identical_index() {
        let builder = MessageSkillBuilder::new();
        let input = [
            contribution("alice", &["auth", "rust"], 0.8),
            contribution("bob", &["rust"], 0.5),
        ];
        assert_eq!(builder.build(&input), builder.build(&input));
    }

    #[test]
    fn equal_contributors_are_ordered_by_author_id() {
        let builder = MessageSkillBuilder::new();
        // bob and alice contribute equally to auth; alice must come first.
        let index = builder.build(&[
            contribution("bob", &["auth"], 0.7),
            contribution("alice", &["auth"], 0.7),
        ]);
        let auth = ownership_for(&index, "auth");
        assert_eq!(auth.candidates[0].author, AuthorId("alice".into()));
        assert_eq!(auth.candidates[1].author, AuthorId("bob".into()));
        assert_eq!(
            auth.candidates[0].score.get(),
            auth.candidates[1].score.get()
        );
    }
}
