//! The skill model: who is suited to a topic, derived from messages.
//!
//! Every projection carries topics and every message carries an author, so an
//! author×topic skill index is a join: a [`DeveloperProfile`] per author and a
//! [`TopicOwnership`] (ranked [`ExpertCandidate`]s) per topic. The build is
//! deterministic, so it re-derives under replay like every other artifact.

use serde::{Deserialize, Serialize};

use crate::message::{AuthorId, ConversationId, MessageId};
use crate::projection::{Confidence, Topic};

/// A ranking score constrained to `0.0..=1.0`.
///
/// Construction clamps into range, and deserialization goes through the same
/// path, so an out-of-range score cannot exist. Distinct from `Confidence`: a
/// ranking score is not a projection confidence, though both inhabit `0.0..=1.0`.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(from = "f64")]
pub struct SkillScore(f64);

impl SkillScore {
    /// Create a score, clamping the value into `0.0..=1.0`.
    pub fn new(value: f64) -> Self {
        SkillScore(value.clamp(0.0, 1.0))
    }

    /// The underlying score.
    pub fn get(self) -> f64 {
        self.0
    }
}

impl From<f64> for SkillScore {
    fn from(value: f64) -> Self {
        SkillScore::new(value)
    }
}

/// How much an author touches one topic, as a confidence-weighted weight.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TopicWeight {
    pub topic: Topic,
    pub weight: f64,
}

/// An author and the topics they touch, with weights. (ML-301)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeveloperProfile {
    pub author: AuthorId,
    pub display_name: String,
    pub topics: Vec<TopicWeight>,
}

/// An author suited to a topic, with a normalized score.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExpertCandidate {
    pub author: AuthorId,
    pub display_name: String,
    pub score: SkillScore,
}

/// A topic and its expert candidates, ranked best-first. (ML-303 / ML-306)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TopicOwnership {
    pub topic: Topic,
    pub candidates: Vec<ExpertCandidate>,
}

/// The full skill index: per-author profiles and per-topic ownership.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SkillIndex {
    pub profiles: Vec<DeveloperProfile>,
    pub ownerships: Vec<TopicOwnership>,
}

/// One message's resolved contribution to the index: its author and the topics
/// of its projection, with that projection's confidence as weight. This is the
/// I/O-free boundary the [`crate::SkillBuilder`] consumes, produced by joining a
/// [`SkillEntry`] to its stored projection.
#[derive(Debug, Clone, PartialEq)]
pub struct SkillContribution {
    pub author: AuthorId,
    pub display_name: String,
    pub topics: Vec<Topic>,
    pub confidence: Confidence,
}

/// One participant's session view: which of the session's topics they own, as
/// ranked [`TopicWeight`]s (best-first). Empty when they own none of them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeveloperSnapshot {
    pub author: AuthorId,
    pub display_name: String,
    pub topics: Vec<TopicWeight>,
}

/// The developers of one session: a conversation and the [`DeveloperSnapshot`]s
/// of its participants. (ML-403)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionDevelopers {
    pub conversation: ConversationId,
    pub developers: Vec<DeveloperSnapshot>,
}

/// One participant in a conversation — the author the projection does not carry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Participant {
    pub author: AuthorId,
    pub display_name: String,
}

/// A request to build the developer snapshot for one session: the conversation,
/// its participants, and its message ids (to resolve the session's topics).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeveloperSnapshotRequest {
    pub conversation: ConversationId,
    pub participants: Vec<Participant>,
    pub messages: Vec<MessageId>,
}

/// One author paired with a message of theirs — the job payload element that
/// carries the author the projection does not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillEntry {
    pub author: AuthorId,
    pub display_name: String,
    pub message: MessageId,
}

/// A request to build the global skill index from a set of authored messages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillRequest {
    pub entries: Vec<SkillEntry>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skill_score_is_clamped_into_range() {
        assert_eq!(SkillScore::new(1.5).get(), 1.0);
        assert_eq!(SkillScore::new(-0.2).get(), 0.0);
        assert_eq!(SkillScore::new(0.5).get(), 0.5);
    }

    #[test]
    fn skill_score_deserialization_clamps() {
        let s: SkillScore = serde_json::from_str("1.5").expect("deserialize");
        assert_eq!(s.get(), 1.0);
    }

    #[test]
    fn session_developers_round_trips_through_json() {
        let developers = SessionDevelopers {
            conversation: ConversationId("c-1".into()),
            developers: vec![
                DeveloperSnapshot {
                    author: AuthorId("u-alice".into()),
                    display_name: "alice".into(),
                    topics: vec![TopicWeight {
                        topic: Topic("auth".into()),
                        weight: 1.0,
                    }],
                },
                DeveloperSnapshot {
                    author: AuthorId("u-bob".into()),
                    display_name: "bob".into(),
                    topics: Vec::new(),
                },
            ],
        };
        let json = serde_json::to_string(&developers).expect("serialize");
        let back: SessionDevelopers = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(developers, back);
    }

    #[test]
    fn topic_ownership_round_trips_through_json() {
        let ownership = TopicOwnership {
            topic: Topic("auth".into()),
            candidates: vec![
                ExpertCandidate {
                    author: AuthorId("u-alice".into()),
                    display_name: "alice".into(),
                    score: SkillScore::new(1.0),
                },
                ExpertCandidate {
                    author: AuthorId("u-bob".into()),
                    display_name: "bob".into(),
                    score: SkillScore::new(0.5),
                },
            ],
        };
        let json = serde_json::to_string(&ownership).expect("serialize");
        let back: TopicOwnership = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(ownership, back);
    }
}
