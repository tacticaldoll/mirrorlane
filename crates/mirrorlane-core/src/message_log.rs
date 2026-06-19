//! The message log: the append-only source of truth replay re-derives from.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::message::{ConversationId, MessageEnvelope, MessageId};

/// Stores ingested messages, keyed by [`MessageId`].
///
/// `append` is idempotent by id — re-appending a message replaces it in place
/// rather than duplicating — so the log accounts for each message exactly once.
/// `all` returns the messages in append order.
pub trait MessageStore: Send + Sync {
    /// Append a message, replacing any existing entry with the same id.
    fn append(&self, message: MessageEnvelope);

    /// Fetch a message by id, if present.
    fn get(&self, id: &MessageId) -> Option<MessageEnvelope>;

    /// All messages, in append order.
    fn all(&self) -> Vec<MessageEnvelope>;

    /// The distinct conversation ids present, in first-seen (append) order.
    ///
    /// Defaults to deriving from [`all`](MessageStore::all); a durable adapter MAY
    /// override it to avoid materializing the whole log.
    fn conversation_ids(&self) -> Vec<ConversationId> {
        let mut seen: Vec<ConversationId> = Vec::new();
        for message in self.all() {
            if !seen.contains(&message.conversation.id) {
                seen.push(message.conversation.id);
            }
        }
        seen
    }

    /// The messages belonging to `conversation`, in append order.
    ///
    /// Defaults to filtering [`all`](MessageStore::all); a durable adapter MAY
    /// override it to read only that conversation rather than the whole log.
    fn messages_for(&self, conversation: &ConversationId) -> Vec<MessageEnvelope> {
        self.all()
            .into_iter()
            .filter(|message| &message.conversation.id == conversation)
            .collect()
    }
}

/// An in-memory [`MessageStore`] backed by an ordered id list and a map. Uses
/// std only.
#[derive(Debug, Default)]
pub struct InMemoryMessageStore {
    inner: Mutex<Log>,
}

#[derive(Debug, Default)]
struct Log {
    order: Vec<MessageId>,
    by_id: HashMap<MessageId, MessageEnvelope>,
}

impl InMemoryMessageStore {
    /// Create an empty log.
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of distinct messages in the log.
    pub fn len(&self) -> usize {
        self.lock().order.len()
    }

    /// Whether the log is empty.
    pub fn is_empty(&self) -> bool {
        self.lock().order.is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Log> {
        self.inner.lock().expect("message log mutex poisoned")
    }
}

impl MessageStore for InMemoryMessageStore {
    fn append(&self, message: MessageEnvelope) {
        let mut log = self.lock();
        if !log.by_id.contains_key(&message.id) {
            log.order.push(message.id.clone());
        }
        log.by_id.insert(message.id.clone(), message);
    }

    fn get(&self, id: &MessageId) -> Option<MessageEnvelope> {
        self.lock().by_id.get(id).cloned()
    }

    fn all(&self) -> Vec<MessageEnvelope> {
        let log = self.lock();
        log.order.iter().map(|id| log.by_id[id].clone()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{Author, AuthorId, Conversation, ConversationId, Source};

    fn message(id: &str, body: &str) -> MessageEnvelope {
        MessageEnvelope {
            id: MessageId(id.into()),
            source: Source::Manual,
            author: Author {
                id: AuthorId("u-1".into()),
                display_name: "Dev".into(),
            },
            conversation: Conversation {
                id: ConversationId("c-1".into()),
                thread: None,
            },
            body: body.into(),
        }
    }

    #[test]
    fn appending_same_id_twice_keeps_one_entry() {
        let log = InMemoryMessageStore::new();
        log.append(message("m-1", "first"));
        log.append(message("m-1", "second"));

        assert_eq!(log.len(), 1, "same id must not duplicate");
        assert_eq!(log.get(&MessageId("m-1".into())).unwrap().body, "second");
    }

    #[test]
    fn all_preserves_append_order() {
        let log = InMemoryMessageStore::new();
        log.append(message("m-3", "c"));
        log.append(message("m-1", "a"));
        log.append(message("m-2", "b"));

        let ids: Vec<String> = log.all().into_iter().map(|m| m.id.0).collect();
        assert_eq!(ids, vec!["m-3", "m-1", "m-2"]);
    }

    fn message_in(id: &str, conv: &str, body: &str) -> MessageEnvelope {
        let mut m = message(id, body);
        m.conversation.id = ConversationId(conv.into());
        m
    }

    #[test]
    fn conversation_ids_and_messages_for_match_all() {
        let log = InMemoryMessageStore::new();
        log.append(message_in("m-1", "c-1", "a"));
        log.append(message_in("m-2", "c-2", "b"));
        log.append(message_in("m-3", "c-1", "c"));

        // Distinct ids in first-seen order.
        assert_eq!(
            log.conversation_ids(),
            vec![ConversationId("c-1".into()), ConversationId("c-2".into())]
        );
        // One conversation's messages, in append order.
        let c1: Vec<String> = log
            .messages_for(&ConversationId("c-1".into()))
            .into_iter()
            .map(|m| m.id.0)
            .collect();
        assert_eq!(c1, vec!["m-1", "m-3"]);
        assert!(
            log.messages_for(&ConversationId("absent".into()))
                .is_empty(),
            "an absent conversation yields no messages"
        );
    }
}
