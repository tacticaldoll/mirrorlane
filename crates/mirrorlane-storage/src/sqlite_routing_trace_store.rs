//! A file-backed SQLite [`RoutingTraceStore`].

use std::path::Path;
use std::sync::Mutex;

use mirrorlane_core::RoutingTraceStore;
use mirrorlane_core::message::MessageId;
use mirrorlane_core::routing::RoutingTrace;
use rusqlite::{Connection, OptionalExtension, params};

/// A durable [`RoutingTraceStore`] backed by SQLite.
///
/// Routing traces are observability, not a store of record. An optional
/// `max_traces` cap bounds growth: when set, an `upsert` prunes traces beyond the
/// most-recent N (by insertion order). The default is uncapped, so direct library
/// use is unbounded unless asked otherwise.
pub struct SqliteRoutingTraceStore {
    conn: Mutex<Connection>,
    max_traces: Option<usize>,
}

impl SqliteRoutingTraceStore {
    /// Open (creating if needed) a store at `path`. Returns an error rather than
    /// panicking if the database cannot be opened, configured, or migrated.
    pub fn open(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        Self::init(Connection::open(path)?)
    }

    /// Open an ephemeral in-memory store (for tests).
    pub fn open_in_memory() -> rusqlite::Result<Self> {
        Self::init(Connection::open_in_memory()?)
    }

    /// Bound the store to the most-recent `max` traces (pruned on write). Without
    /// this, the store is unbounded.
    pub fn with_max_traces(mut self, max: usize) -> Self {
        self.max_traces = Some(max);
        self
    }

    fn init(conn: Connection) -> rusqlite::Result<Self> {
        crate::sqlite_common::configure(&conn)?;
        crate::sqlite_common::migrate(
            &conn,
            &["CREATE TABLE IF NOT EXISTS routing_traces (
                   id      TEXT PRIMARY KEY,
                   payload TEXT NOT NULL
               );"],
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
            max_traces: None,
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl RoutingTraceStore for SqliteRoutingTraceStore {
    fn upsert(&self, trace: RoutingTrace) {
        let payload = serde_json::to_string(&trace).expect("serialize routing trace");
        let conn = self.lock();
        conn.execute(
            "INSERT INTO routing_traces (id, payload) VALUES (?1, ?2)
                 ON CONFLICT(id) DO UPDATE SET payload = excluded.payload",
            params![trace.message_id.0, payload],
        )
        .expect("upsert routing trace");
        // Write-driven prune to the most-recent N by insertion order (implicit
        // rowid). The just-written row is the highest rowid, so it is never pruned.
        if let Some(max) = self.max_traces {
            conn.execute(
                "DELETE FROM routing_traces
                 WHERE rowid NOT IN (
                     SELECT rowid FROM routing_traces ORDER BY rowid DESC LIMIT ?1
                 )",
                params![max as i64],
            )
            .expect("prune routing traces");
        }
    }

    fn get(&self, id: &MessageId) -> Option<RoutingTrace> {
        let payload: Option<String> = self
            .lock()
            .query_row(
                "SELECT payload FROM routing_traces WHERE id = ?1",
                params![&id.0],
                |row| row.get(0),
            )
            .optional()
            .expect("query routing trace");
        payload.map(|p| serde_json::from_str(&p).expect("deserialize routing trace"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn trace(id: &str) -> RoutingTrace {
        RoutingTrace {
            message_id: MessageId(id.into()),
            steps: vec![],
        }
    }

    #[test]
    fn cached_traces_survive_reopening() {
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("traces.db");
        {
            let store = SqliteRoutingTraceStore::open(&path).expect("open store");
            store.upsert(trace("m-1"));
        }
        let store = SqliteRoutingTraceStore::open(&path).expect("open store");
        assert_eq!(store.get(&MessageId("m-1".into())), Some(trace("m-1")));
        assert!(store.get(&MessageId("m-2".into())).is_none());
    }

    #[test]
    fn cap_keeps_only_the_most_recent_n() {
        let store = SqliteRoutingTraceStore::open_in_memory()
            .expect("open store")
            .with_max_traces(2);
        store.upsert(trace("m-1"));
        store.upsert(trace("m-2"));
        store.upsert(trace("m-3"));

        assert!(
            store.get(&MessageId("m-1".into())).is_none(),
            "the oldest trace is pruned past the cap"
        );
        assert_eq!(store.get(&MessageId("m-2".into())), Some(trace("m-2")));
        assert_eq!(
            store.get(&MessageId("m-3".into())),
            Some(trace("m-3")),
            "the just-written trace is always retained"
        );
    }

    #[test]
    fn uncapped_store_retains_everything() {
        let store = SqliteRoutingTraceStore::open_in_memory().expect("open store");
        for i in 0..50 {
            store.upsert(trace(&format!("m-{i}")));
        }
        assert_eq!(store.get(&MessageId("m-0".into())), Some(trace("m-0")));
        assert_eq!(store.get(&MessageId("m-49".into())), Some(trace("m-49")));
    }
}
