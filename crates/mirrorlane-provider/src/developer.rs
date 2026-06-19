//! A deterministic, skill-derived developer snapshotter.
//!
//! Given a conversation's participants and the topic ownerships for the session's
//! topics, this collects, per participant, the session topics they are a
//! candidate on (weight = their ranking score), ranks those topics best-first,
//! and orders developers by ascending author id. Ties among a participant's
//! topics break by ascending topic name. A participant who owns none of the
//! session's topics keeps an empty topic list rather than being dropped, so the
//! snapshot reflects who was actually present. Given the same inputs it always
//! produces the same result, so it re-derives identically under replay.

use std::cmp::Ordering;

use mirrorlane_core::DeveloperSnapshotBuilder;
use mirrorlane_core::message::ConversationId;
use mirrorlane_core::skill::{
    DeveloperSnapshot, Participant, SessionDevelopers, TopicOwnership, TopicWeight,
};

/// A deterministic [`DeveloperSnapshotBuilder`] over the skill index.
#[derive(Debug, Default, Clone)]
pub struct SkillDeveloperSnapshotter;

impl SkillDeveloperSnapshotter {
    /// Create a skill developer snapshotter.
    pub fn new() -> Self {
        Self
    }
}

impl DeveloperSnapshotBuilder for SkillDeveloperSnapshotter {
    fn build(
        &self,
        conversation: &ConversationId,
        participants: &[Participant],
        ownerships: &[TopicOwnership],
    ) -> SessionDevelopers {
        let mut developers: Vec<DeveloperSnapshot> = participants
            .iter()
            .map(|participant| {
                let mut topics: Vec<TopicWeight> = ownerships
                    .iter()
                    .filter_map(|ownership| {
                        ownership
                            .candidates
                            .iter()
                            .find(|candidate| candidate.author == participant.author)
                            .map(|candidate| TopicWeight {
                                topic: ownership.topic.clone(),
                                weight: candidate.score.get(),
                            })
                    })
                    .collect();
                topics.sort_by(|a, b| {
                    b.weight
                        .partial_cmp(&a.weight)
                        .unwrap_or(Ordering::Equal)
                        .then_with(|| a.topic.0.cmp(&b.topic.0))
                });
                DeveloperSnapshot {
                    author: participant.author.clone(),
                    display_name: participant.display_name.clone(),
                    topics,
                }
            })
            .collect();
        developers.sort_by(|a, b| a.author.0.cmp(&b.author.0));

        SessionDevelopers {
            conversation: conversation.clone(),
            developers,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mirrorlane_core::message::AuthorId;
    use mirrorlane_core::projection::Topic;
    use mirrorlane_core::skill::{ExpertCandidate, SkillScore};

    fn conversation() -> ConversationId {
        ConversationId("c-1".into())
    }

    fn participant(author: &str) -> Participant {
        Participant {
            author: AuthorId(author.into()),
            display_name: author.into(),
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

    fn topics_of<'a>(developers: &'a SessionDevelopers, author: &str) -> &'a [TopicWeight] {
        &developers
            .developers
            .iter()
            .find(|d| d.author == AuthorId(author.into()))
            .expect("developer present")
            .topics
    }

    #[test]
    fn topics_are_ranked_by_descending_weight() {
        let snapshotter = SkillDeveloperSnapshotter::new();
        // alice scores higher on sdk than on auth.
        let ownerships = vec![
            ownership("auth", vec![candidate("alice", 0.5)]),
            ownership("sdk", vec![candidate("alice", 1.0)]),
        ];
        let developers = snapshotter.build(&conversation(), &[participant("alice")], &ownerships);
        let topics = topics_of(&developers, "alice");
        assert_eq!(topics[0].topic, Topic("sdk".into()));
        assert_eq!(topics[1].topic, Topic("auth".into()));
    }

    #[test]
    fn developers_are_ordered_by_author_id() {
        let snapshotter = SkillDeveloperSnapshotter::new();
        let ownerships = vec![ownership(
            "auth",
            vec![candidate("alice", 1.0), candidate("bob", 0.8)],
        )];
        let developers = snapshotter.build(
            &conversation(),
            &[participant("bob"), participant("alice")],
            &ownerships,
        );
        assert_eq!(developers.developers[0].author, AuthorId("alice".into()));
        assert_eq!(developers.developers[1].author, AuthorId("bob".into()));
    }

    #[test]
    fn same_inputs_build_identical_result() {
        let snapshotter = SkillDeveloperSnapshotter::new();
        let ownerships = vec![ownership(
            "auth",
            vec![candidate("alice", 1.0), candidate("bob", 0.5)],
        )];
        let participants = [participant("alice"), participant("bob")];
        assert_eq!(
            snapshotter.build(&conversation(), &participants, &ownerships),
            snapshotter.build(&conversation(), &participants, &ownerships)
        );
    }

    #[test]
    fn participant_owning_no_session_topic_has_empty_topics() {
        let snapshotter = SkillDeveloperSnapshotter::new();
        let ownerships = vec![ownership("auth", vec![candidate("alice", 1.0)])];
        // carol participated but owns none of the session's topics.
        let developers = snapshotter.build(
            &conversation(),
            &[participant("alice"), participant("carol")],
            &ownerships,
        );
        assert!(topics_of(&developers, "carol").is_empty());
        assert!(!topics_of(&developers, "alice").is_empty());
    }
}
