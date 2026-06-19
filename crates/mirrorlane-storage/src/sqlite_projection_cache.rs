//! A file-backed SQLite projection cache: the durable `Cache<Projection>`.

use std::path::Path;
use std::sync::Mutex;

use mirrorlane_core::projection::Projection;
use mirrorlane_core::{Cache, StepVersion};
use rusqlite::{Connection, OptionalExtension, params};

/// A durable [`Cache<Projection>`] backed by SQLite.
///
/// Entries are keyed by `(version, key)` as **separate columns** under a composite
/// primary key (the key being a message id), so a projector version change (model
/// or prompt) misses and recomputes. Separate columns — not a `"{version}:{key}"`
/// concatenation — because both a `StepVersion` and a `MessageId` can contain `:`,
/// which would let distinct `(version, key)` tuples alias one row. The step `kind`
/// is not part of the durable key: projection is the only durable cache user. The
/// `Cache` port is infallible, so SQL errors surface as panics; the cache is used
/// at the projection boundary, not inside retried job handlers.
pub struct SqliteProjectionCache {
    conn: Mutex<Connection>,
}

impl SqliteProjectionCache {
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
            &["CREATE TABLE IF NOT EXISTS projection_cache (
                   version TEXT NOT NULL,
                   key     TEXT NOT NULL,
                   payload TEXT NOT NULL,
                   PRIMARY KEY (version, key)
               );"],
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Reclaim cached projections under any version other than `keep_version`. A
    /// projector version bump supersedes every prior-version row (a lookup only ever
    /// queries the current version, so older rows are stale-by-key and never hit).
    /// This is a delivery-optimization cache, not a store of record — replay
    /// repopulates a miss.
    pub fn prune_superseded(&self, keep_version: &StepVersion) -> rusqlite::Result<usize> {
        self.lock().execute(
            "DELETE FROM projection_cache WHERE version <> ?1",
            params![keep_version.as_str()],
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

impl Cache<Projection> for SqliteProjectionCache {
    fn get(&self, _kind: &str, version: &StepVersion, key: &str) -> Option<Projection> {
        let payload: Option<String> = self
            .lock()
            .query_row(
                "SELECT payload FROM projection_cache WHERE version = ?1 AND key = ?2",
                params![version.as_str(), key],
                |row| row.get(0),
            )
            .optional()
            .expect("query projection cache");
        payload.map(|p| serde_json::from_str(&p).expect("deserialize projection"))
    }

    fn put(&self, _kind: &str, version: &StepVersion, key: &str, value: Projection) {
        let payload = serde_json::to_string(&value).expect("serialize projection");
        self.lock()
            .execute(
                "INSERT INTO projection_cache (version, key, payload) VALUES (?1, ?2, ?3)
                 ON CONFLICT(version, key) DO UPDATE SET payload = excluded.payload",
                params![version.as_str(), key, payload],
            )
            .expect("put projection cache");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mirrorlane_core::message::MessageId;
    use mirrorlane_core::projection::{Confidence, Intent};
    use tempfile::tempdir;

    fn projection(id: &str) -> Projection {
        Projection {
            message_id: MessageId(id.into()),
            intent: Intent::Decision,
            topics: Vec::new(),
            entities: Vec::new(),
            confidence: Confidence::new(0.7),
        }
    }

    #[test]
    fn cached_projections_survive_reopening() {
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("cache.db");
        let v1 = StepVersion::new("v1");
        {
            let cache = SqliteProjectionCache::open(&path).expect("open cache");
            cache.put("proj", &v1, "m-1", projection("m-1"));
        }
        let cache = SqliteProjectionCache::open(&path).expect("open cache");
        assert_eq!(cache.get("proj", &v1, "m-1"), Some(projection("m-1")));
        assert!(cache.get("proj", &StepVersion::new("v2"), "m-1").is_none());
    }

    #[test]
    fn delimiters_in_version_or_key_do_not_alias() {
        // `(version, key)` are separate columns, so tuples that a `"{version}:{key}"`
        // concatenation would have aliased stay distinct even when a component
        // contains `:` (e.g. a GitHub message id like `github:issue:1`).
        let cache = SqliteProjectionCache::open_in_memory().expect("open cache");
        let v = StepVersion::new("v1");
        cache.put("proj", &v, "github:issue:1", projection("a"));
        // A naive "v1:github:issue:1" key could be reparsed as version "v1:github"
        // + key "issue:1"; with composite columns that is simply a miss.
        assert!(
            cache
                .get("proj", &StepVersion::new("v1:github"), "issue:1")
                .is_none()
        );
        assert_eq!(
            cache.get("proj", &v, "github:issue:1"),
            Some(projection("a"))
        );
    }

    #[test]
    fn prune_superseded_drops_old_versions_keeps_current() {
        let cache = SqliteProjectionCache::open_in_memory().expect("open cache");
        let old = StepVersion::new("v1");
        let new = StepVersion::new("v2");
        cache.put("proj", &old, "m-1", projection("m-1"));
        cache.put("proj", &new, "m-1", projection("m-1"));

        let removed = cache.prune_superseded(&new).expect("prune");
        assert_eq!(removed, 1, "the superseded old-version row is reclaimed");
        assert!(
            cache.get("proj", &old, "m-1").is_none(),
            "old version no longer hits"
        );
        assert_eq!(
            cache.get("proj", &new, "m-1"),
            Some(projection("m-1")),
            "current version still hits"
        );
    }
}
