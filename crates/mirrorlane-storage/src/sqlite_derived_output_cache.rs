//! A file-backed SQLite derived-output cache: the durable [`DerivedOutputCache`].

use std::path::Path;
use std::sync::Mutex;

use mirrorlane_core::message::ConversationId;
use mirrorlane_core::{ConversationDerivation, DerivedOutputCache, StepVersion};
use rusqlite::{Connection, OptionalExtension, params};

/// A durable [`DerivedOutputCache`] backed by SQLite.
///
/// Entries are keyed by `(version, conversation, content)` as **separate columns**
/// under a composite primary key, mirroring
/// [`SqliteProjectionCache`](crate::SqliteProjectionCache): a derivation-version
/// change or a conversation-content change misses and is recomputed by replay.
/// Separate columns — not a `"{version}:{conversation}:{content}"` concatenation —
/// because the derivation version itself contains `:` (`{schema}:{strategy}:{projector}`)
/// and a conversation id can too (e.g. `github:owner/repo`), which would let
/// distinct tuples alias one row. The cache is a delivery optimization over replay,
/// not a store of record. The port is infallible, so SQL errors surface as panics;
/// it is used at the replay/read boundary, not inside retried job handlers.
pub struct SqliteDerivedOutputCache {
    conn: Mutex<Connection>,
}

impl SqliteDerivedOutputCache {
    /// Open (creating if needed) a cache at `path`. Returns an error rather than
    /// panicking if the database cannot be opened, configured, or migrated.
    pub fn open(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        Self::init(Connection::open(path)?)
    }

    /// Open an ephemeral in-memory cache (for tests).
    pub fn open_in_memory() -> rusqlite::Result<Self> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(conn: Connection) -> rusqlite::Result<Self> {
        crate::sqlite_common::configure(&conn)?;
        crate::sqlite_common::migrate(
            &conn,
            &["CREATE TABLE IF NOT EXISTS derived_output_cache (
                   version      TEXT NOT NULL,
                   conversation TEXT NOT NULL,
                   content      TEXT NOT NULL,
                   payload      TEXT NOT NULL,
                   PRIMARY KEY (version, conversation, content)
               );"],
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Reclaim cached output for `conversation` superseded by the current
    /// `(keep_version, keep_content)` — every other `(version, content)` row for that
    /// conversation is stale-by-key (a read only queries the current key) and never
    /// hit again. This is a delivery-optimization cache, not a store of record;
    /// replay repopulates a miss.
    pub fn prune_superseded(
        &self,
        conversation: &ConversationId,
        keep_version: &StepVersion,
        keep_content: &str,
    ) -> rusqlite::Result<usize> {
        self.lock().execute(
            "DELETE FROM derived_output_cache
             WHERE conversation = ?1 AND NOT (version = ?2 AND content = ?3)",
            params![conversation.0, keep_version.as_str(), keep_content],
        )
    }

    /// Reclaim file space freed by pruning. Runs outside any transaction.
    pub fn vacuum(&self) -> rusqlite::Result<()> {
        self.lock().execute_batch("VACUUM")
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl DerivedOutputCache for SqliteDerivedOutputCache {
    fn get(
        &self,
        version: &StepVersion,
        conversation: &ConversationId,
        content: &str,
    ) -> Option<ConversationDerivation> {
        let payload: Option<String> = self
            .lock()
            .query_row(
                "SELECT payload FROM derived_output_cache
                 WHERE version = ?1 AND conversation = ?2 AND content = ?3",
                params![version.as_str(), conversation.0, content],
                |row| row.get(0),
            )
            .optional()
            .expect("query derived-output cache");
        payload.map(|p| serde_json::from_str(&p).expect("deserialize derivation"))
    }

    fn put(
        &self,
        version: &StepVersion,
        conversation: &ConversationId,
        content: &str,
        value: ConversationDerivation,
    ) {
        let payload = serde_json::to_string(&value).expect("serialize derivation");
        self.lock()
            .execute(
                "INSERT INTO derived_output_cache (version, conversation, content, payload)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(version, conversation, content) DO UPDATE SET payload = excluded.payload",
                params![version.as_str(), conversation.0, content, payload],
            )
            .expect("put derived-output cache");
    }

    fn reclaim_superseded(
        &self,
        conversation: &ConversationId,
        keep_version: &StepVersion,
        keep_content: &str,
    ) {
        // Best-effort: a failed prune only leaves dead rows, so swallow the error
        // rather than failing the delivery cycle that triggered it.
        let _ = SqliteDerivedOutputCache::prune_superseded(
            self,
            conversation,
            keep_version,
            keep_content,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mirrorlane_core::warmup::WarmupDocument;
    use tempfile::tempdir;

    fn derivation(conversation: &str) -> ConversationDerivation {
        ConversationDerivation {
            conversation: ConversationId(conversation.into()),
            projections: Vec::new(),
            scope: None,
            warmup: WarmupDocument {
                conversation: ConversationId(conversation.into()),
                focus: Vec::new(),
                decisions: Vec::new(),
                open_questions: Vec::new(),
                tasks: Vec::new(),
                summary: "summary".into(),
            },
            developers: None,
            hints: Vec::new(),
        }
    }

    #[test]
    fn cached_derivation_survives_reopening() {
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("derived.db");
        let v1 = StepVersion::new("v1");
        let conv = ConversationId("c-1".into());
        {
            let cache = SqliteDerivedOutputCache::open(&path).expect("open cache");
            cache.put(&v1, &conv, "h1", derivation("c-1"));
        }
        let cache = SqliteDerivedOutputCache::open(&path).expect("open cache");
        assert_eq!(cache.get(&v1, &conv, "h1"), Some(derivation("c-1")));
    }

    #[test]
    fn a_version_or_content_change_misses() {
        let cache = SqliteDerivedOutputCache::open_in_memory().expect("open cache");
        let v1 = StepVersion::new("v1");
        let conv = ConversationId("c-1".into());
        cache.put(&v1, &conv, "h1", derivation("c-1"));

        assert!(
            cache.get(&StepVersion::new("v2"), &conv, "h1").is_none(),
            "a version change misses"
        );
        assert!(
            cache.get(&v1, &conv, "h2").is_none(),
            "a content change misses"
        );
    }

    #[test]
    fn delimiters_in_components_do_not_alias() {
        // The version contains `:` (`{schema}:{strategy}:{projector}`) and a
        // conversation id can too (`github:owner/repo`); separate columns keep
        // distinct tuples from aliasing the way a `:`-joined key would.
        let cache = SqliteDerivedOutputCache::open_in_memory().expect("open cache");
        let version = StepVersion::new("1:projection:v1");
        let conv = ConversationId("github:acme/widgets".into());
        cache.put(&version, &conv, "hash", derivation("github:acme/widgets"));

        // A naive "1:projection:v1:github:acme/widgets:hash" join could be split
        // many ways; with composite columns those other splits simply miss.
        assert!(
            cache
                .get(
                    &StepVersion::new("1:projection"),
                    &ConversationId("v1:github:acme/widgets".into()),
                    "hash"
                )
                .is_none(),
            "a different version/conversation split must not alias"
        );
        assert_eq!(
            cache.get(&version, &conv, "hash"),
            Some(derivation("github:acme/widgets"))
        );
    }

    #[test]
    fn prune_superseded_reclaims_old_rows_keeps_current() {
        let cache = SqliteDerivedOutputCache::open_in_memory().expect("open cache");
        let v = StepVersion::new("v1");
        let conv = ConversationId("c-1".into());
        // An older content hash and an older version for the same conversation.
        cache.put(&v, &conv, "old-content", derivation("c-1"));
        cache.put(&StepVersion::new("v0"), &conv, "h1", derivation("c-1"));
        // The current cycle's row.
        cache.put(&v, &conv, "new-content", derivation("c-1"));
        // A different conversation must be untouched.
        let other = ConversationId("c-2".into());
        cache.put(&v, &other, "x", derivation("c-2"));

        let removed = cache
            .prune_superseded(&conv, &v, "new-content")
            .expect("prune");
        assert_eq!(removed, 2, "both superseded rows for the conversation go");
        assert_eq!(
            cache.get(&v, &conv, "new-content"),
            Some(derivation("c-1")),
            "current entry still hits"
        );
        assert!(cache.get(&v, &conv, "old-content").is_none());
        assert!(cache.get(&StepVersion::new("v0"), &conv, "h1").is_none());
        assert_eq!(
            cache.get(&v, &other, "x"),
            Some(derivation("c-2")),
            "another conversation is untouched"
        );
    }
}
