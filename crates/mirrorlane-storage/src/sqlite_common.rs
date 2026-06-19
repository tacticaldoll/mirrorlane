//! Per-connection setup shared by every SQLite store: WAL configuration and
//! `user_version`-based schema migration.
//!
//! [`configure`] applies the connection PRAGMAs that make contention wait rather
//! than fail (`busy_timeout`) and trade a rollback journal for WAL. WAL is a
//! persistent database property, but `synchronous` and `busy_timeout` are
//! per-connection and must be re-applied on every connection a store opens — so
//! this runs in each store's `init`, not once at create time.
//!
//! [`migrate`] runs ordered schema steps gated on `PRAGMA user_version`, so a
//! column or table change is an explicit forward migration rather than a silent
//! `CREATE TABLE IF NOT EXISTS` no-op against a stale on-disk schema.

use rusqlite::Connection;

/// Apply the WAL + `synchronous=NORMAL` + `busy_timeout` PRAGMAs to `conn`. Safe on
/// in-memory databases (WAL is silently a no-op there) so every store uses one path.
pub(crate) fn configure(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA busy_timeout = 5000;",
    )
}

/// Apply migration `steps` not yet applied to `conn`, then stamp `user_version` to
/// the number of steps. `steps[i]` is the migration from schema version `i` to
/// `i + 1`; a step MUST be idempotent against a database last touched by pre-version
/// code (which leaves `user_version = 0`). A fresh database (also `user_version = 0`)
/// runs every step in order.
pub(crate) fn migrate(conn: &Connection, steps: &[&str]) -> rusqlite::Result<()> {
    let current: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    for (i, step) in steps.iter().enumerate() {
        if (i as i64) < current {
            continue;
        }
        // Each step and its `user_version` bump commit together or not at all: a
        // crash or error mid-step rolls the whole transaction back, leaving the
        // prior schema and version intact, so the next open re-runs the step from a
        // clean state rather than half-migrating and bricking reopen. `user_version`
        // is a header value whose write participates in the transaction.
        // `PRAGMA user_version` takes no bound parameter; the value is a loop index,
        // never user input, so formatting it is safe.
        conn.execute_batch(&format!(
            "BEGIN;
             {step}
             PRAGMA user_version = {next};
             COMMIT;",
            next = i + 1
        ))?;
    }
    Ok(())
}
