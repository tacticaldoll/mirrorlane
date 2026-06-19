//! A file-backed SQLite [`MessageStore`].

use std::path::Path;
use std::sync::Mutex;

use mirrorlane_core::MessageStore;
use mirrorlane_core::message::{ConversationId, MessageEnvelope, MessageId};
use rusqlite::{Connection, OptionalExtension, params};

/// A durable [`MessageStore`] backed by SQLite.
///
/// `append` is idempotent by message id (replacing in place, preserving append
/// order) and the log persists across reopening the same database path. The
/// `MessageStore` port is infallible, so SQL errors surface as panics; this
/// store is used at the ingest/replay boundary, not inside job handlers.
pub struct SqliteMessageStore {
    conn: Mutex<Connection>,
}

impl SqliteMessageStore {
    /// Open (creating if needed) a message log at `path`. Returns an error rather
    /// than panicking if the database cannot be opened, configured, or migrated.
    pub fn open(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        Self::init(Connection::open(path)?)
    }

    /// Open an ephemeral in-memory message log (for tests).
    pub fn open_in_memory() -> rusqlite::Result<Self> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(conn: Connection) -> rusqlite::Result<Self> {
        crate::sqlite_common::configure(&conn)?;
        crate::sqlite_common::migrate(
            &conn,
            &[
                // v1: the original schema (matches what pre-version code left on
                // disk, so an existing database no-ops here).
                "CREATE TABLE IF NOT EXISTS messages (
                     id      TEXT PRIMARY KEY,
                     seq     INTEGER NOT NULL,
                     payload TEXT NOT NULL
                 );",
                // v2: rebuild with a UNIQUE seq, renumbering deterministically by
                // existing append order — so no two messages can ever share a seq,
                // even under a future multi-writer connection path. Empty on a fresh
                // database.
                "CREATE TABLE messages_v2 (
                     id      TEXT PRIMARY KEY,
                     seq     INTEGER NOT NULL UNIQUE,
                     payload TEXT NOT NULL
                 );
                 INSERT INTO messages_v2 (id, seq, payload)
                     SELECT id, ROW_NUMBER() OVER (ORDER BY seq), payload FROM messages;
                 DROP TABLE messages;
                 ALTER TABLE messages_v2 RENAME TO messages;",
            ],
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Lock the connection, recovering a poisoned-but-consistent guard rather than
    /// propagating the panic — a poisoning panic never leaves a half-written row,
    /// since each write is a single auto-committed statement.
    fn lock(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl MessageStore for SqliteMessageStore {
    fn append(&self, message: MessageEnvelope) {
        let payload = serde_json::to_string(&message).expect("serialize message");
        self.lock()
            .execute(
                "INSERT INTO messages (id, seq, payload)
                 VALUES (?1, (SELECT COALESCE(MAX(seq), 0) + 1 FROM messages), ?2)
                 ON CONFLICT(id) DO UPDATE SET payload = excluded.payload",
                params![message.id.0, payload],
            )
            .expect("append message");
    }

    fn get(&self, id: &MessageId) -> Option<MessageEnvelope> {
        let payload: Option<String> = self
            .lock()
            .query_row(
                "SELECT payload FROM messages WHERE id = ?1",
                params![&id.0],
                |row| row.get(0),
            )
            .optional()
            .expect("query message");
        payload.map(|p| serde_json::from_str(&p).expect("deserialize message"))
    }

    fn all(&self) -> Vec<MessageEnvelope> {
        let conn = self.lock();
        let mut stmt = conn
            .prepare("SELECT payload FROM messages ORDER BY seq")
            .expect("prepare select");
        stmt.query_map([], |row| row.get::<_, String>(0))
            .expect("query messages")
            .map(|row| {
                let payload = row.expect("read row");
                serde_json::from_str(&payload).expect("deserialize message")
            })
            .collect()
    }

    fn conversation_ids(&self) -> Vec<ConversationId> {
        let conn = self.lock();
        // Distinct conversation ids in first-seen order, materializing only the ids
        // (not the whole log). The conversation id lives in the JSON payload.
        let mut stmt = conn
            .prepare(
                "SELECT json_extract(payload, '$.conversation.id') AS cid
                 FROM messages GROUP BY cid ORDER BY MIN(seq)",
            )
            .expect("prepare conversation_ids");
        stmt.query_map([], |row| row.get::<_, String>(0))
            .expect("query conversation_ids")
            .map(|row| ConversationId(row.expect("read row")))
            .collect()
    }

    fn messages_for(&self, conversation: &ConversationId) -> Vec<MessageEnvelope> {
        let conn = self.lock();
        // Only this conversation's rows, in append order — not the whole log.
        let mut stmt = conn
            .prepare(
                "SELECT payload FROM messages
                 WHERE json_extract(payload, '$.conversation.id') = ?1 ORDER BY seq",
            )
            .expect("prepare messages_for");
        stmt.query_map(params![conversation.0], |row| row.get::<_, String>(0))
            .expect("query messages_for")
            .map(|row| {
                let payload = row.expect("read row");
                serde_json::from_str(&payload).expect("deserialize message")
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mirrorlane_core::message::{Author, AuthorId, Conversation, ConversationId, Source};
    use tempfile::tempdir;

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

    fn ids(store: &SqliteMessageStore) -> Vec<String> {
        store.all().into_iter().map(|m| m.id.0).collect()
    }

    fn message_in(id: &str, conv: &str, body: &str) -> MessageEnvelope {
        let mut m = message(id, body);
        m.conversation.id = ConversationId(conv.into());
        m
    }

    #[test]
    fn sql_per_conversation_reads_match_the_default() {
        let store = SqliteMessageStore::open_in_memory().expect("open store");
        store.append(message_in("m-1", "c-1", "a"));
        store.append(message_in("m-2", "c-2", "b"));
        store.append(message_in("m-3", "c-1", "c"));
        store.append(message_in("m-4", "github:acme/widgets", "d"));

        // conversation_ids: distinct, first-seen order — equals the all()-derived set.
        let want_convs: Vec<ConversationId> = {
            let mut seen = Vec::new();
            for m in store.all() {
                if !seen.contains(&m.conversation.id) {
                    seen.push(m.conversation.id);
                }
            }
            seen
        };
        assert_eq!(store.conversation_ids(), want_convs);

        // messages_for: one conversation in append order — equals the all()-filter.
        for conv in &want_convs {
            let got: Vec<String> = store
                .messages_for(conv)
                .into_iter()
                .map(|m| m.id.0)
                .collect();
            let want: Vec<String> = store
                .all()
                .into_iter()
                .filter(|m| &m.conversation.id == conv)
                .map(|m| m.id.0)
                .collect();
            assert_eq!(
                got, want,
                "messages_for({}) must match all()-filter",
                conv.0
            );
        }
    }

    #[test]
    fn messages_survive_reopening() {
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("log.db");
        {
            let store = SqliteMessageStore::open(&path).expect("open store");
            store.append(message("m-1", "a"));
            store.append(message("m-2", "b"));
        }
        let store = SqliteMessageStore::open(&path).expect("open store");
        assert_eq!(ids(&store), vec!["m-1", "m-2"]);
        assert_eq!(store.get(&MessageId("m-1".into())).unwrap().body, "a");
    }

    #[test]
    fn appending_existing_id_replaces_in_place() {
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("log.db");
        let store = SqliteMessageStore::open(&path).expect("open store");
        store.append(message("m-1", "first"));
        store.append(message("m-2", "x"));
        store.append(message("m-1", "second"));

        assert_eq!(
            ids(&store),
            vec!["m-1", "m-2"],
            "order preserved, no duplicate"
        );
        assert_eq!(store.get(&MessageId("m-1".into())).unwrap().body, "second");
    }

    /// The seqs in the database file, ascending — read through a fresh connection so
    /// the test observes what was actually persisted.
    fn seqs(path: &std::path::Path) -> Vec<i64> {
        let conn = Connection::open(path).expect("open raw");
        let mut stmt = conn
            .prepare("SELECT seq FROM messages ORDER BY seq")
            .expect("prepare");
        stmt.query_map([], |row| row.get::<_, i64>(0))
            .expect("query")
            .map(|r| r.expect("seq"))
            .collect()
    }

    #[test]
    fn wal_mode_and_busy_timeout_are_set() {
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("log.db");
        let store = SqliteMessageStore::open(&path).expect("open store");
        let conn = store.lock();
        let mode: String = conn
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .expect("journal_mode");
        let timeout: i64 = conn
            .query_row("PRAGMA busy_timeout", [], |r| r.get(0))
            .expect("busy_timeout");
        assert_eq!(mode, "wal");
        assert!(timeout > 0, "busy_timeout must be non-zero, was {timeout}");
    }

    #[test]
    fn migrates_legacy_schema_and_keeps_rows() {
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("legacy.db");
        // Simulate a database written by pre-version code: the old schema with no
        // UNIQUE on seq, user_version left at 0, and even a duplicate seq.
        {
            let conn = Connection::open(&path).expect("open raw");
            conn.execute_batch(
                "CREATE TABLE messages (
                     id TEXT PRIMARY KEY, seq INTEGER NOT NULL, payload TEXT NOT NULL
                 );",
            )
            .expect("legacy schema");
            let m1 = serde_json::to_string(&message("m-1", "a")).unwrap();
            let m2 = serde_json::to_string(&message("m-2", "b")).unwrap();
            conn.execute(
                "INSERT INTO messages (id, seq, payload) VALUES ('m-1', 1, ?1), ('m-2', 1, ?2)",
                rusqlite::params![m1, m2],
            )
            .expect("legacy rows");
        }
        // Opening through the store migrates forward without losing rows and
        // renumbers the duplicate seqs into a distinct, gap-free order.
        let store = SqliteMessageStore::open(&path).expect("open store");
        assert_eq!(ids(&store), vec!["m-1", "m-2"], "rows survive migration");
        assert_eq!(seqs(&path), vec![1, 2], "duplicate seqs are renumbered");
    }

    #[test]
    fn recovers_from_a_poisoned_lock() {
        use std::sync::Arc;

        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("log.db");
        let store = Arc::new(SqliteMessageStore::open(&path).expect("open store"));
        store.append(message("m-1", "a"));

        // Poison the mutex from a panicking thread while it holds the guard. The
        // connection is left consistent (no half-written row).
        let poisoner = Arc::clone(&store);
        let _ = std::thread::spawn(move || {
            let _guard = poisoner.lock();
            panic!("poison the lock");
        })
        .join();

        // A subsequent operation recovers the poisoned-but-consistent guard.
        assert_eq!(store.get(&MessageId("m-1".into())).unwrap().body, "a");
        store.append(message("m-2", "b"));
        assert_eq!(ids(&store), vec!["m-1", "m-2"]);
    }

    #[test]
    fn concurrent_appends_get_distinct_seq() {
        use std::thread;

        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("log.db");
        // Create the schema once up front.
        SqliteMessageStore::open(&path).expect("open store");

        const WRITERS: usize = 4;
        const PER_WRITER: usize = 25;
        let handles: Vec<_> = (0..WRITERS)
            .map(|w| {
                let path = path.clone();
                thread::spawn(move || {
                    // Each writer is its own connection — a real multi-writer path.
                    let store = SqliteMessageStore::open(&path).expect("open store");
                    for i in 0..PER_WRITER {
                        store.append(message(&format!("w{w}-m{i}"), "body"));
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().expect("writer thread");
        }

        let seqs = seqs(&path);
        assert_eq!(seqs.len(), WRITERS * PER_WRITER, "no append lost");
        let expected: Vec<i64> = (1..=(WRITERS * PER_WRITER) as i64).collect();
        assert_eq!(
            seqs, expected,
            "every seq is distinct and the order is gap-free"
        );
    }
}
