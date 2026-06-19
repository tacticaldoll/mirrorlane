//! The projection model: the structured interpretation of a message.

use serde::{Deserialize, Serialize};

use crate::message::MessageId;

/// What a message is trying to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Intent {
    Question,
    Decision,
    Proposal,
    Task,
    Issue,
    Social,
}

/// A topic a message touches (e.g. `rust`, `auth`, `sdk`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Topic(pub String);

/// A concrete entity a message references (e.g. `refresh-token`, `postgres`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Entity(pub String);

/// A confidence score constrained to `0.0..=1.0`.
///
/// Construction clamps into range, and deserialization goes through the same
/// path, so an out-of-range confidence cannot exist.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(from = "f64")]
pub struct Confidence(f64);

impl Confidence {
    /// Create a confidence, clamping the value into `0.0..=1.0`.
    pub fn new(value: f64) -> Self {
        Confidence(value.clamp(0.0, 1.0))
    }

    /// The underlying score.
    pub fn get(self) -> f64 {
        self.0
    }
}

impl From<f64> for Confidence {
    fn from(value: f64) -> Self {
        Confidence::new(value)
    }
}

/// The structured interpretation of a message, keyed by its [`MessageId`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Projection {
    pub message_id: MessageId,
    pub intent: Intent,
    pub topics: Vec<Topic>,
    pub entities: Vec<Entity>,
    pub confidence: Confidence,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn confidence_is_clamped_into_range() {
        assert_eq!(Confidence::new(1.5).get(), 1.0);
        assert_eq!(Confidence::new(-0.2).get(), 0.0);
        assert_eq!(Confidence::new(0.86).get(), 0.86);
    }

    #[test]
    fn confidence_deserialization_clamps() {
        let c: Confidence = serde_json::from_str("1.5").expect("deserialize");
        assert_eq!(c.get(), 1.0);
    }

    #[test]
    fn projection_serializes_with_documented_fields() {
        let projection = Projection {
            message_id: MessageId("m-1".into()),
            intent: Intent::Task,
            topics: vec![Topic("rust".into()), Topic("sdk".into())],
            entities: vec![Entity("refresh-token".into())],
            confidence: Confidence::new(0.86),
        };
        let value: serde_json::Value =
            serde_json::to_value(&projection).expect("serialize to value");
        assert_eq!(value["intent"], "task");
        assert!(value.get("topics").is_some());
        assert!(value.get("entities").is_some());
        assert_eq!(value["confidence"], 0.86);
    }
}
