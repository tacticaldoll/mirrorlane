//! Turning a strategy run's stores into cached, per-conversation derived output.
//!
//! After a strategy produces [`ReplayStores`](crate::ReplayStores),
//! [`populate_cache`] writes each conversation's [`ConversationDerivation`] to a
//! [`DerivedOutputCache`], keyed by the derivation version and a content hash of the
//! conversation's messages. Both the in-process replay (the CLI) and the async
//! [`StrategyRunJob`](crate::StrategyRunJob) consumer use this, so a run populates
//! the same read model however it was triggered. Assembling a full session context
//! from a derivation (the read-time routing) stays at the read layer.

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use mirrorlane_core::message::{ConversationId, MessageEnvelope, MessageId};
use mirrorlane_core::{
    ConversationDerivation, DerivedOutputCache, DeveloperSnapshotStore, MessageStore,
    ProjectionStore, RoutingHintStore, ScopeStore, StepVersion, WarmupStore,
};

use crate::ReplayStores;

/// Distinct conversation ids in the log, in first-seen order.
pub fn conversations(store: &dyn MessageStore) -> Vec<ConversationId> {
    let mut seen: Vec<ConversationId> = Vec::new();
    for message in store.all() {
        if !seen.contains(&message.conversation.id) {
            seen.push(message.conversation.id);
        }
    }
    seen
}

/// A conversation's message ids, in first-seen order.
pub fn messages_in(store: &dyn MessageStore, conversation: &ConversationId) -> Vec<MessageId> {
    store
        .all()
        .into_iter()
        .filter(|message| &message.conversation.id == conversation)
        .map(|message| message.id)
        .collect()
}

/// A stable hash of a conversation's messages (id + body, first-seen order), so an
/// appended or edited message changes the derived-output cache key and misses.
pub fn content_hash(store: &dyn MessageStore, conversation: &ConversationId) -> String {
    let mut hasher = DefaultHasher::new();
    for message in store.all() {
        if &message.conversation.id == conversation {
            message.id.0.hash(&mut hasher);
            message.body.hash(&mut hasher);
        }
    }
    format!("{:016x}", hasher.finish())
}

/// The same hash as [`content_hash`], computed from an already-fetched, in-order
/// slice of a conversation's messages — so a caller that has the conversation's
/// messages (a single-pass grouping, or `MessageStore::messages_for`) need not
/// re-scan the whole log. The byte sequence hashed (each id then body, in order) is
/// identical to [`content_hash`], so cache keys match.
pub fn content_hash_of(messages: &[MessageEnvelope]) -> String {
    let mut hasher = DefaultHasher::new();
    for message in messages {
        message.id.0.hash(&mut hasher);
        message.body.hash(&mut hasher);
    }
    format!("{:016x}", hasher.finish())
}

/// Extract one conversation's derivation from a replay's stores, given that
/// conversation's messages directly (no store re-scan). The message-slice form of
/// [`derivation_for`], used by the single-pass [`populate_cache`].
fn derivation_from_messages(
    stores: &ReplayStores,
    conversation: &ConversationId,
    messages: &[MessageEnvelope],
) -> Option<ConversationDerivation> {
    let warmup = stores.warmups.get(conversation)?;
    let projections = messages
        .iter()
        .filter_map(|message| stores.projections.get(&message.id))
        .collect();
    let hints = messages
        .iter()
        .filter_map(|message| stores.hints.get(&message.id))
        .collect();
    Some(ConversationDerivation {
        conversation: conversation.clone(),
        projections,
        scope: stores.scopes.get(conversation),
        warmup,
        developers: stores.developers.get(conversation),
        hints,
    })
}

/// Extract one conversation's reproducible derivation from a replay's stores.
/// Returns `None` when the conversation has no warm-up (an empty conversation is
/// skipped). The read-time routing is added by the read layer.
pub fn derivation_for(
    store: &dyn MessageStore,
    stores: &ReplayStores,
    conversation: &ConversationId,
) -> Option<ConversationDerivation> {
    let warmup = stores.warmups.get(conversation)?;
    let message_ids = messages_in(store, conversation);
    let projections = message_ids
        .iter()
        .filter_map(|id| stores.projections.get(id))
        .collect();
    let hints = message_ids
        .iter()
        .filter_map(|id| stores.hints.get(id))
        .collect();
    Some(ConversationDerivation {
        conversation: conversation.clone(),
        projections,
        scope: stores.scopes.get(conversation),
        warmup,
        developers: stores.developers.get(conversation),
        hints,
    })
}

/// Write every conversation's derivation to `cache` (keyed by `version` and the
/// conversation's content hash), returning the derivations in first-seen order so a
/// caller can assemble session contexts without re-extracting.
pub fn populate_cache(
    store: &dyn MessageStore,
    stores: &ReplayStores,
    cache: &dyn DerivedOutputCache,
    version: &StepVersion,
) -> Vec<ConversationDerivation> {
    // Single pass over the log: group messages by conversation in first-seen order,
    // preserving each conversation's append order — so each conversation's content
    // hash and derivation are computed from the grouped messages, not a per-
    // conversation re-scan of the whole log.
    let mut order: Vec<ConversationId> = Vec::new();
    let mut groups: HashMap<ConversationId, Vec<MessageEnvelope>> = HashMap::new();
    for message in store.all() {
        let conversation = message.conversation.id.clone();
        if !groups.contains_key(&conversation) {
            order.push(conversation.clone());
        }
        groups.entry(conversation).or_default().push(message);
    }

    order
        .into_iter()
        .filter_map(|conversation| {
            let messages = &groups[&conversation];
            let derivation = derivation_from_messages(stores, &conversation, messages)?;
            let content = content_hash_of(messages);
            cache.put(version, &conversation, &content, derivation.clone());
            // Reclaim this conversation's rows superseded by the cycle just written
            // (a no-op for caches that don't grow on disk).
            cache.reclaim_superseded(&conversation, version, &content);
            Some(derivation)
        })
        .collect()
}
