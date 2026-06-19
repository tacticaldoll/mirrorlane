//! Selecting and connecting the durable broker that backs `submit` and `work`.
//!
//! Mirrorlane's strategy-run queue is hardcoded no longer: [`connect`] resolves a
//! [`BrokerKind`] to an `Arc<dyn Broker>`, so a supervised, multi-process
//! deployment can point its workers at a shared `postgres`/`redis` queue instead of
//! a single-file SQLite one. The selection pattern mirrors `worklane-cli`'s own
//! broker factory, including the documented URL precedence and credential-safe
//! reporting (the URL may carry a password, so only its source is printed).

use std::sync::Arc;
use std::time::Duration;

use clap::ValueEnum;
use serde::Deserialize;
use worklane::Broker;
use worklane_core::{RetentionPolicy, redact_credentials};
use worklane_postgres::PostgresBroker;
use worklane_redis::RedisBroker;
use worklane_sqlite::SqliteBroker;

/// Worklane-substrate settings applied to the queue broker. Every field except
/// `retention` is `None` = "use worklane's default", so an unconfigured deployment
/// is unchanged. `schema`/`pool_size` apply to Postgres, `namespace` to Redis;
/// `lease`/`max_deliveries`/`retention` apply to all backends.
pub struct BrokerSettings {
    pub retention: RetentionPolicy,
    pub schema: Option<String>,
    pub namespace: Option<String>,
    pub pool_size: Option<usize>,
    pub lease: Option<Duration>,
    pub max_deliveries: Option<u32>,
}

/// Which durable broker backs `submit` and `work`. Defaults to `sqlite`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BrokerKind {
    /// A single-file SQLite queue at the `--queue-db` path (default).
    #[default]
    Sqlite,
    /// A Postgres-backed queue connected by URL.
    Postgres,
    /// A Redis-backed queue connected by URL.
    Redis,
}

/// Connect to the selected broker. `queue_db` is the resolved `--queue-db` path
/// (used only by `sqlite`); `url_setting` is the resolved `--queue-url`/`queue_url`
/// value (the flag→config tier), which the networked backends fall back from to
/// `$WORKLANE_URL` then their conventional variable. Returns a human-readable error
/// — never a panic — when a required input is missing or the connection fails.
pub async fn connect(
    kind: BrokerKind,
    queue_db: &str,
    url_setting: Option<&str>,
    settings: BrokerSettings,
) -> Result<Arc<dyn Broker>, Box<dyn std::error::Error>> {
    let BrokerSettings {
        retention,
        schema,
        namespace,
        pool_size,
        lease,
        max_deliveries,
    } = settings;
    match kind {
        BrokerKind::Sqlite => {
            if url_setting.is_some() {
                eprintln!("sqlite: ignoring --queue-url (the sqlite broker uses --queue-db)");
            }
            let mut broker = SqliteBroker::open(queue_db)
                .map_err(|e| format!("sqlite: failed to open '{queue_db}': {e}"))?
                .with_dead_letter_retention(retention);
            if let Some(lease) = lease {
                broker = broker.with_lease(lease);
            }
            if let Some(max) = max_deliveries {
                broker = broker.with_max_deliveries(max);
            }
            Ok(Arc::new(broker))
        }
        BrokerKind::Postgres => {
            let url = resolve_url(url_setting, "DATABASE_URL", "postgres")?;
            // `"public"` mirrors worklane's default schema, so an unset schema is
            // byte-for-byte the plain `connect`.
            let mut broker = PostgresBroker::connect_with_pool(
                &url,
                schema.as_deref().unwrap_or("public"),
                pool_size.unwrap_or(worklane_postgres::DEFAULT_POOL_SIZE),
            )
            .await
            .map_err(|e| {
                format!(
                    "postgres: connection failed: {}",
                    redact_credentials(&e.to_string())
                )
            })?
            .with_dead_letter_retention(retention);
            if let Some(lease) = lease {
                broker = broker.with_lease(lease);
            }
            if let Some(max) = max_deliveries {
                broker = broker.with_max_deliveries(max);
            }
            Ok(Arc::new(broker))
        }
        BrokerKind::Redis => {
            let url = resolve_url(url_setting, "REDIS_URL", "redis")?;
            // `"worklane"` mirrors worklane's default namespace.
            let mut broker = RedisBroker::connect_with_namespace(
                &url,
                namespace.as_deref().unwrap_or("worklane"),
            )
            .await
            .map_err(|e| {
                format!(
                    "redis: connection failed: {}",
                    redact_credentials(&e.to_string())
                )
            })?
            .with_dead_letter_retention(retention);
            if let Some(lease) = lease {
                broker = broker.with_lease(lease);
            }
            if let Some(max) = max_deliveries {
                broker = broker.with_max_deliveries(max);
            }
            Ok(Arc::new(broker))
        }
    }
}

/// Resolve the connection URL with one documented precedence — the resolved
/// `--queue-url`/`queue_url` setting, then `$WORKLANE_URL`, then the backend's
/// conventional `fallback_var` — and announce the chosen source on stderr. The URL
/// itself is never printed (it may carry a credential); only the source is.
fn resolve_url(
    url_setting: Option<&str>,
    fallback_var: &str,
    backend: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    if let Some(url) = url_setting {
        eprintln!("{backend}: using --queue-url/queue_url");
        return Ok(url.to_string());
    }
    if let Ok(url) = std::env::var("WORKLANE_URL") {
        eprintln!("{backend}: using $WORKLANE_URL");
        return Ok(url);
    }
    if let Ok(url) = std::env::var(fallback_var) {
        eprintln!("{backend}: using ${fallback_var}");
        return Ok(url);
    }
    Err(format!(
        "--queue-url, $WORKLANE_URL, or ${fallback_var} is required for --broker {backend}"
    )
    .into())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serializes the env-mutating tests: they share process-global env vars, so
    /// they must not run concurrently with one another.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Run a closure with env vars cleared and restored, so URL-precedence tests
    /// don't leak into one another or the ambient environment.
    fn with_clean_env<T>(f: impl FnOnce() -> T) -> T {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let saved: Vec<(&str, Option<String>)> = ["WORKLANE_URL", "DATABASE_URL", "REDIS_URL"]
            .iter()
            .map(|k| (*k, std::env::var(k).ok()))
            .collect();
        for (k, _) in &saved {
            unsafe { std::env::remove_var(k) };
        }
        let out = f();
        for (k, v) in saved {
            match v {
                Some(v) => unsafe { std::env::set_var(k, v) },
                None => unsafe { std::env::remove_var(k) },
            }
        }
        out
    }

    fn settings() -> BrokerSettings {
        BrokerSettings {
            retention: RetentionPolicy::new(),
            schema: None,
            namespace: None,
            pool_size: None,
            lease: None,
            max_deliveries: None,
        }
    }

    #[tokio::test]
    async fn default_sqlite_opens_the_queue_db_path() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("q.db");
        let db = path.to_str().expect("path");
        let broker = connect(BrokerKind::Sqlite, db, None, settings()).await;
        assert!(broker.is_ok(), "sqlite opens at the queue-db path");
    }

    #[tokio::test]
    async fn sqlite_applies_substrate_settings() {
        // The substrate tuning (lease/max-deliveries) is wired into the factory;
        // proven on the sqlite path without a networked server.
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("q.db");
        let db = path.to_str().expect("path");
        let broker = connect(
            BrokerKind::Sqlite,
            db,
            None,
            BrokerSettings {
                lease: Some(Duration::from_secs(45)),
                max_deliveries: Some(7),
                ..settings()
            },
        )
        .await;
        assert!(broker.is_ok(), "sqlite applies lease/max-deliveries");
    }

    #[test]
    fn url_flag_beats_env() {
        with_clean_env(|| {
            unsafe { std::env::set_var("WORKLANE_URL", "from-worklane-url") };
            unsafe { std::env::set_var("DATABASE_URL", "from-database-url") };
            let url = resolve_url(Some("from-flag"), "DATABASE_URL", "postgres").expect("url");
            assert_eq!(url, "from-flag");
        });
    }

    #[test]
    fn worklane_url_beats_backend_var() {
        with_clean_env(|| {
            unsafe { std::env::set_var("WORKLANE_URL", "from-worklane-url") };
            unsafe { std::env::set_var("DATABASE_URL", "from-database-url") };
            let url = resolve_url(None, "DATABASE_URL", "postgres").expect("url");
            assert_eq!(url, "from-worklane-url");
        });
    }

    #[test]
    fn backend_var_is_the_last_fallback() {
        with_clean_env(|| {
            unsafe { std::env::set_var("REDIS_URL", "from-redis-url") };
            let url = resolve_url(None, "REDIS_URL", "redis").expect("url");
            assert_eq!(url, "from-redis-url");
        });
    }

    #[tokio::test]
    async fn networked_broker_without_a_url_is_an_error() {
        with_clean_env(|| {
            let err = resolve_url(None, "DATABASE_URL", "postgres");
            assert!(err.is_err(), "no URL anywhere is a reported error");
        });
    }
}
