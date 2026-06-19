//! A deterministic warm-up builder.
//!
//! A placeholder for real summarization: it groups a session's projections by
//! intent, takes focus from the scope, and renders a fixed-format summary from
//! that metadata — it does not read message bodies. Given the same inputs it
//! always returns the same document.

use mirrorlane_core::WarmupBuilder;
use mirrorlane_core::message::{ConversationId, MessageId};
use mirrorlane_core::projection::{Intent, Projection};
use mirrorlane_core::scope::{Component, Scope};
use mirrorlane_core::warmup::WarmupDocument;

/// A deterministic mock [`WarmupBuilder`].
#[derive(Debug, Default, Clone)]
pub struct MockWarmupBuilder;

impl MockWarmupBuilder {
    /// Create a mock warm-up builder.
    pub fn new() -> Self {
        Self
    }
}

impl WarmupBuilder for MockWarmupBuilder {
    fn build(
        &self,
        conversation: &ConversationId,
        scope: Option<&Scope>,
        projections: &[Projection],
    ) -> WarmupDocument {
        let mut decisions: Vec<MessageId> = Vec::new();
        let mut open_questions: Vec<MessageId> = Vec::new();
        let mut tasks: Vec<MessageId> = Vec::new();

        // Preserve request order so output is stable.
        for projection in projections {
            let id = projection.message_id.clone();
            match projection.intent {
                Intent::Decision => decisions.push(id),
                Intent::Question => open_questions.push(id),
                Intent::Task => tasks.push(id),
                Intent::Proposal | Intent::Issue | Intent::Social => {}
            }
        }

        let focus: Vec<Component> = scope.map(|s| s.load.clone()).unwrap_or_default();
        let summary = render(
            conversation,
            scope,
            &focus,
            &decisions,
            &open_questions,
            &tasks,
        );

        WarmupDocument {
            conversation: conversation.clone(),
            focus,
            decisions,
            open_questions,
            tasks,
            summary,
        }
    }
}

fn render(
    conversation: &ConversationId,
    scope: Option<&Scope>,
    focus: &[Component],
    decisions: &[MessageId],
    open_questions: &[MessageId],
    tasks: &[MessageId],
) -> String {
    let focus_line = if focus.is_empty() {
        "Focus: (no scope yet)".to_string()
    } else {
        let names: Vec<&str> = focus.iter().map(|c| c.0.as_str()).collect();
        format!("Focus: {}", names.join(", "))
    };
    let why = scope
        .map(|s| format!("\nWhy: {}", s.reason))
        .unwrap_or_default();

    format!(
        "You are joining {}.\n{}{}\nRecent decisions: {}\nOpen questions: {}\nTasks: {}",
        conversation.0,
        focus_line,
        why,
        decisions.len(),
        open_questions.len(),
        tasks.len(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use mirrorlane_core::projection::Confidence;

    fn projection(id: &str, intent: Intent) -> Projection {
        Projection {
            message_id: MessageId(id.into()),
            intent,
            topics: Vec::new(),
            entities: Vec::new(),
            confidence: Confidence::new(0.8),
        }
    }

    fn conversation() -> ConversationId {
        ConversationId("c-1".into())
    }

    fn scope() -> Scope {
        Scope {
            conversation: conversation(),
            load: vec![Component("auth-sdk".into())],
            ignore: vec![Component("billing".into())],
            reason: "SDK auth work.".into(),
            confidence: Confidence::new(0.8),
        }
    }

    fn projections() -> Vec<Projection> {
        vec![
            projection("m-1", Intent::Decision),
            projection("m-2", Intent::Question),
            projection("m-3", Intent::Task),
            projection("m-4", Intent::Social),
        ]
    }

    #[test]
    fn same_inputs_build_the_same_document() {
        let builder = MockWarmupBuilder::new();
        let scope = scope();
        let projections = projections();
        assert_eq!(
            builder.build(&conversation(), Some(&scope), &projections),
            builder.build(&conversation(), Some(&scope), &projections)
        );
    }

    #[test]
    fn projections_are_grouped_by_intent() {
        let builder = MockWarmupBuilder::new();
        let doc = builder.build(&conversation(), Some(&scope()), &projections());
        assert_eq!(doc.decisions, vec![MessageId("m-1".into())]);
        assert_eq!(doc.open_questions, vec![MessageId("m-2".into())]);
        assert_eq!(doc.tasks, vec![MessageId("m-3".into())]);
    }

    #[test]
    fn focus_comes_from_scope_when_present() {
        let builder = MockWarmupBuilder::new();
        let doc = builder.build(&conversation(), Some(&scope()), &projections());
        assert_eq!(doc.focus, vec![Component("auth-sdk".into())]);
    }

    #[test]
    fn absent_scope_yields_empty_focus() {
        let builder = MockWarmupBuilder::new();
        let doc = builder.build(&conversation(), None, &projections());
        assert!(doc.focus.is_empty());
        assert!(!doc.summary.is_empty());
    }
}
