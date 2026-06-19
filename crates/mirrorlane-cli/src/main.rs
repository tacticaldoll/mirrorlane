//! Mirrorlane CLI: drive the durable message log from a terminal.
//!
//! - `ingest` reads a `MessageEnvelope` as JSON from stdin and appends it.
//! - `replay` replays the log and prints a warm-up per conversation.
//! - `warmup --conversation <id>` prints one conversation's warm-up.

use std::io::Read;
use std::sync::Arc;

use clap::{Parser, Subcommand, ValueEnum};
use mirrorlane_core::MessageStore;
use mirrorlane_core::message::{ConversationId, MessageId};
use mirrorlane_github::RestGitHubSource;
use mirrorlane_storage::{SqliteDerivedOutputCache, SqliteMessageStore, SqliteRoutingTraceStore};
use mirrorlane_worker::{
    DeadLetters, RecordingJobObserver, StrategyRegistry, StrategyRunJob, StrategyRunRequest,
    routed_work_lane, strategy_run_lane,
};
use serde::Deserialize;
use worklane::{Client, JobId, Worker};

mod broker;
mod commands;

use broker::BrokerKind;
use commands::Provider;

/// How command output is rendered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Format {
    /// Human-readable summaries (default).
    #[default]
    Text,
    /// Machine-readable JSON, for agent and workflow consumers.
    Json,
}

/// The auto-loaded config file name, used when `--config` is not given.
const DEFAULT_CONFIG_PATH: &str = "mirrorlane.json";

/// Defaults loaded from a JSON config file. Every key is optional; unknown keys
/// are ignored for forward compatibility.
#[derive(Debug, Default, Deserialize)]
struct Config {
    #[serde(default)]
    db: Option<String>,
    #[serde(default)]
    queue_db: Option<String>,
    #[serde(default)]
    broker: Option<BrokerKind>,
    #[serde(default)]
    queue_url: Option<String>,
    #[serde(default)]
    provider: Option<Provider>,
    #[serde(default)]
    format: Option<Format>,
    #[serde(default)]
    strategy: Option<String>,
    #[serde(default)]
    ollama_base_url: Option<String>,
    #[serde(default)]
    ollama_model: Option<String>,
    #[serde(default)]
    ollama_prompt_version: Option<String>,
    #[serde(default)]
    openai_base_url: Option<String>,
    #[serde(default)]
    openai_model: Option<String>,
    #[serde(default)]
    github_base_url: Option<String>,
    #[serde(default)]
    routing: Option<commands::RoutingConfig>,
    #[serde(default)]
    retention: Option<RetentionConfig>,
    #[serde(default)]
    worklane: Option<WorklaneConfig>,
}

/// The `worklane` config section: worklane-substrate settings, all optional. Each
/// is also overridable by a `MIRRORLANE_WORKLANE_*` environment variable (env wins).
/// `queue_schema` isolates a Postgres instance; `queue_namespace` a Redis instance.
#[derive(Debug, Default, Deserialize)]
struct WorklaneConfig {
    #[serde(default)]
    queue_schema: Option<String>,
    #[serde(default)]
    queue_namespace: Option<String>,
    #[serde(default)]
    pool_size: Option<usize>,
    #[serde(default)]
    lease_secs: Option<u64>,
    #[serde(default)]
    max_deliveries: Option<u32>,
    #[serde(default)]
    handler_timeout_secs: Option<u64>,
}

/// A string substrate setting: `MIRRORLANE_WORKLANE_<name>` env wins, else config.
fn env_str_or(name: &str, config: Option<String>) -> Option<String> {
    std::env::var(name).ok().or(config)
}

/// A numeric substrate setting: env wins; a malformed env value is a reported error,
/// not a silent default.
fn env_num_or<T: std::str::FromStr>(
    name: &str,
    config: Option<T>,
) -> Result<Option<T>, Box<dyn std::error::Error>>
where
    T::Err: std::fmt::Display,
{
    match std::env::var(name) {
        Ok(s) => s
            .parse::<T>()
            .map(Some)
            .map_err(|e| format!("{name}: invalid value {s:?}: {e}").into()),
        Err(_) => Ok(config),
    }
}

/// Resolve the worklane-substrate `BrokerSettings` from the `MIRRORLANE_WORKLANE_*`
/// environment namespace, then the `worklane` config section, then worklane's
/// defaults (`None`). The third-party secrets and `WORKLANE_URL` are untouched.
fn resolve_broker_settings(
    retention: worklane_core::RetentionPolicy,
    config: Option<WorklaneConfig>,
) -> Result<broker::BrokerSettings, Box<dyn std::error::Error>> {
    let wl = config.unwrap_or_default();
    Ok(broker::BrokerSettings {
        retention,
        schema: env_str_or("MIRRORLANE_WORKLANE_QUEUE_SCHEMA", wl.queue_schema),
        namespace: env_str_or("MIRRORLANE_WORKLANE_QUEUE_NAMESPACE", wl.queue_namespace),
        pool_size: env_num_or("MIRRORLANE_WORKLANE_POOL_SIZE", wl.pool_size)?,
        lease: env_num_or::<u64>("MIRRORLANE_WORKLANE_LEASE_SECS", wl.lease_secs)?
            .map(std::time::Duration::from_secs),
        // Default to a built-in backstop (not worklane's unbounded) so redelivery is
        // bounded out of the box; env/config still override.
        max_deliveries: env_num_or("MIRRORLANE_WORKLANE_MAX_DELIVERIES", wl.max_deliveries)?
            .or(Some(DEFAULT_MAX_DELIVERIES)),
    })
}

/// Bounds for the disposable persistence surfaces, all optional with built-in
/// defaults: dead-letter retention on the queue broker, and a most-recent-N cap on
/// the routing-trace store. The message log (source of truth) is never bounded here.
#[derive(Debug, Default, Deserialize)]
struct RetentionConfig {
    #[serde(default)]
    dead_letter_max_count: Option<u64>,
    #[serde(default)]
    dead_letter_max_age_secs: Option<u64>,
    #[serde(default)]
    trace_max_count: Option<usize>,
}

/// Built-in dead-letter cap: bounded out of the box, raise via config.
const DEFAULT_DEAD_LETTER_MAX_COUNT: u64 = 1000;
/// Built-in routing-trace cap: bounded out of the box, raise via config.
const DEFAULT_TRACE_MAX_COUNT: usize = 10_000;
/// Built-in redelivery backstop: bound redeliveries out of the box so a job that
/// keeps losing its lease (e.g. repeated worker crashes) eventually dead-letters
/// rather than redelivering forever. `max_attempts` bounds only handler-error
/// retries, not lease-expiry redeliveries — this closes that gap by default.
const DEFAULT_MAX_DELIVERIES: u32 = 10;
/// Built-in handler timeout for the `work` worker: bound a single strategy run so a
/// hung LLM call cannot hold a job forever. Generous, since a cold run may make many
/// sequential model calls; raise via config/env for slower backends.
const DEFAULT_HANDLER_TIMEOUT_SECS: u64 = 300;

/// Resolve the `work` worker's handler timeout: `MIRRORLANE_WORKLANE_HANDLER_TIMEOUT_SECS`
/// env wins, then the `worklane` config section, then the built-in default. The
/// timeout bounds a single strategy run so a hung LLM call cannot hold a job forever.
fn resolve_handler_timeout(
    config: Option<&WorklaneConfig>,
) -> Result<std::time::Duration, Box<dyn std::error::Error>> {
    let secs = env_num_or::<u64>(
        "MIRRORLANE_WORKLANE_HANDLER_TIMEOUT_SECS",
        config.and_then(|w| w.handler_timeout_secs),
    )?
    .unwrap_or(DEFAULT_HANDLER_TIMEOUT_SECS);
    Ok(std::time::Duration::from_secs(secs))
}

/// Resolve the dead-letter `RetentionPolicy` and the routing-trace cap from optional
/// config, applying the built-in defaults so both surfaces are bounded by default.
fn resolve_retention(config: Option<RetentionConfig>) -> (worklane_core::RetentionPolicy, usize) {
    let rc = config.unwrap_or_default();
    let mut policy = worklane_core::RetentionPolicy::new().with_max_count(
        rc.dead_letter_max_count
            .unwrap_or(DEFAULT_DEAD_LETTER_MAX_COUNT),
    );
    if let Some(secs) = rc.dead_letter_max_age_secs {
        policy = policy.with_max_age(std::time::Duration::from_secs(secs));
    }
    let trace_cap = rc.trace_max_count.unwrap_or(DEFAULT_TRACE_MAX_COUNT);
    (policy, trace_cap)
}

impl Config {
    /// Parse a config from JSON text.
    fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

/// Load the config: from an explicit `--config` path (an error if it cannot be
/// read or parsed), else the auto-loaded `mirrorlane.json` if present, else
/// empty.
fn load_config(explicit: Option<&str>) -> Result<Config, Box<dyn std::error::Error>> {
    match explicit {
        Some(path) => {
            let json = std::fs::read_to_string(path)
                .map_err(|e| format!("cannot read config {path}: {e}"))?;
            Ok(Config::from_json(&json).map_err(|e| format!("invalid config {path}: {e}"))?)
        }
        None => match std::fs::read_to_string(DEFAULT_CONFIG_PATH) {
            Ok(json) => Ok(Config::from_json(&json)
                .map_err(|e| format!("invalid config {DEFAULT_CONFIG_PATH}: {e}"))?),
            Err(_) => Ok(Config::default()),
        },
    }
}

/// Surface a real-projector failure: if any logged message produced no
/// projection (the projector failed at the boundary), report it on stderr and
/// exit non-zero so the incompleteness is not silent. Successful projections are
/// cached durably, so re-running retries only the messages that still failed.
fn fail_if_unprojected(unprojected: &[MessageId]) -> Result<(), Box<dyn std::error::Error>> {
    if unprojected.is_empty() {
        return Ok(());
    }
    let ids: Vec<&str> = unprojected.iter().map(|id| id.0.as_str()).collect();
    Err(format!(
        "{} message(s) produced no projection (projector failed): {}. \
         Re-run to retry — cached successes are not recomputed.",
        ids.len(),
        ids.join(", ")
    )
    .into())
}

/// Resolve a setting: an explicit flag wins, else the config value, else the
/// built-in default.
fn resolve_db(flag: Option<String>, config: Option<String>) -> String {
    flag.or(config)
        .unwrap_or_else(|| "mirrorlane.db".to_string())
}

fn resolve_provider(flag: Option<Provider>, config: Option<Provider>) -> Provider {
    flag.or(config).unwrap_or_default()
}

fn resolve_format(flag: Option<Format>, config: Option<Format>) -> Format {
    flag.or(config).unwrap_or_default()
}

/// Resolve the strategy id: flag, then config, then the default reference
/// strategy (`projection`). The id is looked up in the registry at build time.
fn resolve_strategy(flag: Option<String>, config: Option<String>) -> String {
    flag.or(config)
        .unwrap_or_else(|| StrategyRegistry::DEFAULT.to_string())
}

/// Resolve a string setting: an explicit flag wins, else the config value, else
/// the given built-in default.
fn resolve_str(flag: Option<String>, config: Option<String>, default: &str) -> String {
    flag.or(config).unwrap_or_else(|| default.to_string())
}

#[derive(Parser)]
#[command(
    name = "mirrorlane",
    version,
    about = "Mirrorlane context projection CLI"
)]
struct Cli {
    /// Path to the durable message-log database (default `mirrorlane.db`).
    #[arg(long, global = true)]
    db: Option<String>,

    /// Path to the durable strategy-run queue database (default
    /// `mirrorlane-queue.db`), backing `submit` and `work` with `--broker sqlite`.
    #[arg(long, global = true)]
    queue_db: Option<String>,

    /// Durable broker backing `submit` and `work` (default `sqlite`). `postgres`
    /// and `redis` connect by `--queue-url`.
    #[arg(long, value_enum, global = true)]
    broker: Option<BrokerKind>,

    /// Connection URL for the `postgres`/`redis` queue broker. Falls back to
    /// `$WORKLANE_URL`, then `$DATABASE_URL`/`$REDIS_URL`. Unused by `sqlite`.
    #[arg(long, global = true)]
    queue_url: Option<String>,

    /// Projector backing `replay` and `warmup` (default `mock`).
    #[arg(long, value_enum, global = true)]
    provider: Option<Provider>,

    /// Output format (default `text`): human-readable, or JSON for machines.
    #[arg(long, value_enum, global = true)]
    format: Option<Format>,

    /// Strategy backing `replay` and `warmup` (default `projection`).
    #[arg(long, global = true)]
    strategy: Option<String>,

    /// Path to a JSON config file (else `mirrorlane.json` is auto-loaded if present).
    #[arg(long, global = true)]
    config: Option<String>,

    /// Ollama server base URL (default `http://localhost:11434`).
    #[arg(long, global = true)]
    ollama_base_url: Option<String>,

    /// Ollama model name (default the crate's built-in model).
    #[arg(long, global = true)]
    ollama_model: Option<String>,

    /// Ollama prompt version tag (default the crate's built-in version).
    #[arg(long, global = true)]
    ollama_prompt_version: Option<String>,

    /// OpenAI-compatible base URL (default `https://api.openai.com/v1`). The API
    /// key comes only from `OPENAI_API_KEY`, never a flag or the config file.
    #[arg(long, global = true)]
    openai_base_url: Option<String>,

    /// OpenAI model name (default the crate's built-in model).
    #[arg(long, global = true)]
    openai_model: Option<String>,

    /// GitHub REST API base URL (default `https://api.github.com`).
    #[arg(long, global = true)]
    github_base_url: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Append a MessageEnvelope, read as JSON from stdin, to the log.
    Ingest,
    /// Replay the log and print a warm-up for each conversation.
    Replay,
    /// Replay the log and print one conversation's warm-up.
    Warmup {
        /// The conversation id to warm up.
        #[arg(long)]
        conversation: String,
    },
    /// Ingest a GitHub repository's issues, PRs, and comments into the log.
    Github {
        /// The repository to ingest, as `owner/name`.
        #[arg(long)]
        repo: String,
    },
    /// Inspect a message's routing decision trace.
    Inspect {
        /// The message id to inspect.
        #[arg(long)]
        message: String,
    },
    /// Submit a strategy run onto the durable queue (consumed by `work`).
    Submit,
    /// Consume queued strategy runs to idle, populating the derived-output cache.
    Work,
    /// Verify determinism: recompute derivations and compare to the durable cache,
    /// exiting non-zero if any diverged.
    Verify {
        /// Limit verification to one conversation (default: all).
        #[arg(long)]
        conversation: Option<String>,
    },
    /// Inspect and manage a lane's dead-letter store on the durable broker.
    Dlq {
        /// Which lane's dead-letter store to operate on.
        #[arg(long)]
        lane: DlqLane,
        /// What to do: read, count, requeue, or purge.
        #[arg(long)]
        action: DlqAction,
        /// The job id to requeue (required for `requeue`).
        #[arg(long)]
        id: Option<String>,
        /// Max records to read (for `read`).
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
}

/// Which lane's dead-letter store `dlq` operates on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum DlqLane {
    /// The strategy-run lane (submitted runs that exhausted their attempts).
    StrategyRun,
    /// The routed-work lane (routed jobs that exhausted their attempts).
    RoutedWork,
}

/// The `dlq` operation to perform.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum DlqAction {
    /// List up to `--limit` dead-lettered jobs (non-destructive).
    Read,
    /// Report the number of dead-lettered jobs (non-destructive).
    Count,
    /// Restore one dead-lettered job to its lane by `--id`.
    Requeue,
    /// Empty the lane's dead-letter store, reporting how many were removed.
    Purge,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let config = load_config(cli.config.as_deref())?;
    let db = resolve_db(cli.db, config.db);
    let queue_db = resolve_str(cli.queue_db, config.queue_db, "mirrorlane-queue.db");
    let broker_kind = cli.broker.or(config.broker).unwrap_or_default();
    let queue_url = cli.queue_url.or(config.queue_url);
    let provider = resolve_provider(cli.provider, config.provider);
    let format = resolve_format(cli.format, config.format);
    let strategy_id = resolve_strategy(cli.strategy, config.strategy);
    let llm = commands::LlmSettings {
        ollama: commands::OllamaSettings {
            base_url: resolve_str(
                cli.ollama_base_url,
                config.ollama_base_url,
                mirrorlane_ollama::DEFAULT_BASE_URL,
            ),
            model: resolve_str(
                cli.ollama_model,
                config.ollama_model,
                mirrorlane_ollama::DEFAULT_MODEL,
            ),
            prompt_version: resolve_str(
                cli.ollama_prompt_version,
                config.ollama_prompt_version,
                mirrorlane_ollama::DEFAULT_PROMPT_VERSION,
            ),
        },
        openai: commands::OpenAiSettings {
            base_url: resolve_str(
                cli.openai_base_url,
                config.openai_base_url,
                mirrorlane_openai::DEFAULT_BASE_URL,
            ),
            model: resolve_str(
                cli.openai_model,
                config.openai_model,
                mirrorlane_openai::DEFAULT_MODEL,
            ),
            prompt_version: mirrorlane_llm::DEFAULT_PROMPT_VERSION.to_string(),
        },
    };
    let github_base_url = resolve_str(
        cli.github_base_url,
        config.github_base_url,
        mirrorlane_github::DEFAULT_BASE_URL,
    );
    // The deterministic router, from the optional `routing` config (else defaults).
    let router = commands::build_router(config.routing);
    let (dead_letter_retention, trace_cap) = resolve_retention(config.retention);
    // The `work` worker's handler timeout — resolved before the config is moved into
    // broker settings.
    let handler_timeout = resolve_handler_timeout(config.worklane.as_ref())?;
    let broker_settings = resolve_broker_settings(dead_letter_retention, config.worklane)?;
    let store = SqliteMessageStore::open(&db)?;
    let traces = SqliteRoutingTraceStore::open(&db)?.with_max_traces(trace_cap);

    match cli.command {
        Command::Ingest => {
            let mut json = String::new();
            std::io::stdin().read_to_string(&mut json)?;
            let id = commands::ingest(&store, &json)?;
            match format {
                Format::Text => println!("ingested {}", id.0),
                Format::Json => println!("{}", serde_json::json!({ "id": id.0 })),
            }
        }
        Command::Replay => {
            let strategy = commands::build_strategy(provider, &db, &llm, &strategy_id)?;
            let cache = SqliteDerivedOutputCache::open(&db)?;
            let version = commands::derivation_version(&strategy_id, provider, &db, &llm)?;
            let out = commands::replay(&store, strategy.as_ref(), &cache, &version, &router).await;
            match format {
                Format::Text => {
                    if out.contexts.is_empty() {
                        println!("(no conversations in the log)");
                    }
                    for context in &out.contexts {
                        println!("{}\n", context.warmup.summary);
                    }
                }
                Format::Json => println!("{}", serde_json::to_string(&out.contexts)?),
            }
            fail_if_unprojected(&out.unprojected)?;
        }
        Command::Warmup { conversation } => {
            let id = ConversationId(conversation.clone());
            let strategy = commands::build_strategy(provider, &db, &llm, &strategy_id)?;
            let cache = SqliteDerivedOutputCache::open(&db)?;
            let version = commands::derivation_version(&strategy_id, provider, &db, &llm)?;
            let out =
                commands::warmup(&store, &id, strategy.as_ref(), &cache, &version, &router).await;
            match format {
                Format::Text => match &out.context {
                    Some(context) => println!("{}", context.warmup.summary),
                    None => println!("no warm-up for conversation {conversation}"),
                },
                Format::Json => println!("{}", serde_json::to_string(&out.context)?),
            }
            fail_if_unprojected(&out.unprojected)?;
        }
        Command::Github { repo } => {
            let source =
                RestGitHubSource::new(&github_base_url, std::env::var("GITHUB_TOKEN").ok());
            let ids = commands::github_ingest_rest(&store, &source, &repo)?;
            match format {
                Format::Text => println!("ingested {} item(s) from {repo}", ids.len()),
                Format::Json => println!(
                    "{}",
                    serde_json::json!({
                        "repo": repo,
                        "ingested": ids.iter().map(|id| &id.0).collect::<Vec<_>>(),
                    })
                ),
            }
        }
        Command::Inspect { message } => {
            let id = MessageId(message.clone());
            let context =
                commands::inspect(&store, &traces, &id, provider, &db, &llm, &router).await?;
            match format {
                Format::Text => match context {
                    Some(ctx) => {
                        println!("Message: {}", ctx.message.id.0);
                        println!(
                            "Author:  {} ({})",
                            ctx.message.author.display_name, ctx.message.author.id.0
                        );
                        println!("Conv:    {}", ctx.message.conversation.id.0);
                        println!();
                        println!("Projection:");
                        println!("  Intent:     {:?}", ctx.projection.intent);
                        println!("  Confidence: {:.2}", ctx.projection.confidence.get());
                        println!(
                            "  Topics:     {:?}",
                            ctx.projection
                                .topics
                                .iter()
                                .map(|t| &t.0)
                                .collect::<Vec<_>>()
                        );
                        println!(
                            "  Entities:   {:?}",
                            ctx.projection
                                .entities
                                .iter()
                                .map(|e| &e.0)
                                .collect::<Vec<_>>()
                        );
                        println!();
                        println!("Routing Decision:");
                        println!("  Target:    {:?}", ctx.decision.target);
                        println!("  Escalated: {}", ctx.decision.escalated);
                        println!("  Reason:    {}", ctx.decision.reason);
                        if let Some(trace) = ctx.trace {
                            println!();
                            println!("Rule Evaluations:");
                            for (i, step) in trace.steps.iter().enumerate() {
                                println!(
                                    "  {}. [{}] {} -> {:?}",
                                    i + 1,
                                    if step.matched { "MATCH" } else { " SKIP" },
                                    step.rule_name,
                                    step.resulting_target
                                );
                            }
                        } else {
                            println!("\n(No trace recorded)");
                        }
                    }
                    None => println!("no message found with id {message}"),
                },
                Format::Json => println!("{}", serde_json::to_string(&context)?),
            }
        }
        Command::Submit => {
            let broker = broker::connect(
                broker_kind,
                &queue_db,
                queue_url.as_deref(),
                broker_settings,
            )
            .await?;
            let client = Client::new(broker).with_lane(strategy_run_lane());
            let id = client
                .enqueue::<StrategyRunJob>(StrategyRunRequest {
                    strategy: strategy_id.clone(),
                })
                .await?;
            match format {
                Format::Text => println!("submitted {strategy_id} run {id}"),
                Format::Json => {
                    println!(
                        "{}",
                        serde_json::json!({ "strategy": strategy_id, "job": id.to_string() })
                    )
                }
            }
        }
        Command::Work => {
            let broker = broker::connect(
                broker_kind,
                &queue_db,
                queue_url.as_deref(),
                broker_settings,
            )
            .await?;
            let log: Arc<dyn MessageStore> = Arc::new(SqliteMessageStore::open(&db)?);
            let context = commands::strategy_context(provider, &db, &llm)?;
            let cache = Arc::new(SqliteDerivedOutputCache::open(&db)?);
            // Mount a job-execution observer so a drained run reports what it ran —
            // the glass-box view of an otherwise opaque durable consumer.
            let observer = Arc::new(RecordingJobObserver::new());
            // A strategy run may make many sequential LLM calls; keep the lease alive
            // so a long-but-healthy run is not redelivered, and bound a hung run with a
            // handler timeout so it cannot hold the job forever.
            let mut worker = Worker::new(broker)
                .with_lane(strategy_run_lane())
                .with_observer(observer.clone())
                .with_lease_keepalive(true)
                .with_handler_timeout(handler_timeout);
            worker.register(StrategyRunJob::with_builtins(log, context, cache))?;
            worker.build()?.run_until_idle().await?;
            let records = observer.records();
            match format {
                Format::Text => {
                    println!(
                        "worked the strategy-run queue to idle ({} jobs)",
                        records.len()
                    );
                    for record in &records {
                        println!(
                            "  {}  {}  {}ms",
                            record.kind,
                            record.outcome_label(),
                            record.duration_ms()
                        );
                    }
                }
                Format::Json => {
                    let jobs: Vec<_> = records
                        .iter()
                        .map(|record| {
                            serde_json::json!({
                                "lane": record.lane,
                                "kind": record.kind,
                                "outcome": record.outcome_label(),
                                "duration_ms": record.duration_ms(),
                            })
                        })
                        .collect();
                    println!("{}", serde_json::json!({ "worked": true, "jobs": jobs }))
                }
            }
        }
        Command::Verify { conversation } => {
            let log = SqliteMessageStore::open(&db)?;
            let strategy = commands::build_strategy(provider, &db, &llm, &strategy_id)?;
            let cache = SqliteDerivedOutputCache::open(&db)?;
            let version = commands::derivation_version(&strategy_id, provider, &db, &llm)?;
            let mut records = commands::verify(&log, strategy.as_ref(), &cache, &version).await;
            if let Some(c) = &conversation {
                let wanted = ConversationId(c.clone());
                records.retain(|r| r.conversation == wanted);
            }
            let count = |o: &str| records.iter().filter(|r| r.outcome == o).count();
            let diverged = count("diverged");
            match format {
                Format::Text => {
                    for r in &records {
                        println!("  {}  {}", r.conversation.0, r.outcome);
                    }
                    println!(
                        "{} verified, {} diverged, {} not cached",
                        count("verified"),
                        diverged,
                        count("not_cached")
                    );
                }
                Format::Json => println!("{}", serde_json::to_string(&records)?),
            }
            if diverged > 0 {
                return Err(
                    format!("{diverged} conversation(s) diverged from the durable cache").into(),
                );
            }
        }
        Command::Dlq {
            lane,
            action,
            id,
            limit,
        } => {
            let broker = broker::connect(
                broker_kind,
                &queue_db,
                queue_url.as_deref(),
                broker_settings,
            )
            .await?;
            let (lane_name, dl) = match lane {
                DlqLane::StrategyRun => (
                    "strategy-run",
                    DeadLetters::new(broker, strategy_run_lane()),
                ),
                DlqLane::RoutedWork => {
                    ("routed-work", DeadLetters::new(broker, routed_work_lane()))
                }
            };
            match action {
                DlqAction::Read => {
                    let dead = dl.read(limit).await?;
                    match format {
                        Format::Text => {
                            println!("{} dead-lettered job(s) on {lane_name}:", dead.len());
                            for d in &dead {
                                println!(
                                    "  {}  {}  attempt {}  {}",
                                    d.envelope.id, d.envelope.kind, d.envelope.attempts, d.error
                                );
                            }
                        }
                        Format::Json => {
                            let items: Vec<_> = dead
                                .iter()
                                .map(|d| {
                                    serde_json::json!({
                                        "id": d.envelope.id.to_string(),
                                        "kind": d.envelope.kind,
                                        "attempts": d.envelope.attempts,
                                        "error": d.error,
                                    })
                                })
                                .collect();
                            println!(
                                "{}",
                                serde_json::json!({ "lane": lane_name, "dead_letters": items })
                            );
                        }
                    }
                }
                DlqAction::Count => {
                    let n = dl.count().await?;
                    match format {
                        Format::Text => println!("{n} dead-lettered job(s) on {lane_name}"),
                        Format::Json => {
                            println!("{}", serde_json::json!({ "lane": lane_name, "count": n }))
                        }
                    }
                }
                DlqAction::Requeue => {
                    let id = id.ok_or("--id is required for requeue")?;
                    let job_id: JobId = id
                        .parse()
                        .map_err(|e| format!("invalid job id {id:?}: {e}"))?;
                    dl.requeue(job_id).await?;
                    match format {
                        Format::Text => println!("requeued {id} on {lane_name}"),
                        Format::Json => println!(
                            "{}",
                            serde_json::json!({ "lane": lane_name, "requeued": id })
                        ),
                    }
                }
                DlqAction::Purge => {
                    let n = dl.purge().await?;
                    match format {
                        Format::Text => {
                            println!("purged {n} dead-lettered job(s) from {lane_name}")
                        }
                        Format::Json => {
                            println!("{}", serde_json::json!({ "lane": lane_name, "purged": n }))
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serializes tests that mutate `MIRRORLANE_WORKLANE_*` process env vars.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn dlq_command_parses_lane_and_action() {
        let cli = Cli::try_parse_from([
            "mirrorlane",
            "dlq",
            "--lane",
            "strategy-run",
            "--action",
            "count",
        ])
        .expect("dlq count parses");
        match cli.command {
            Command::Dlq {
                lane,
                action,
                id,
                limit,
            } => {
                assert_eq!(lane, DlqLane::StrategyRun);
                assert_eq!(action, DlqAction::Count);
                assert_eq!(id, None);
                assert_eq!(limit, 50);
            }
            _ => panic!("expected the dlq command"),
        }

        let cli = Cli::try_parse_from([
            "mirrorlane",
            "dlq",
            "--lane",
            "routed-work",
            "--action",
            "requeue",
            "--id",
            "abc",
        ])
        .expect("dlq requeue parses");
        match cli.command {
            Command::Dlq {
                lane, action, id, ..
            } => {
                assert_eq!(lane, DlqLane::RoutedWork);
                assert_eq!(action, DlqAction::Requeue);
                assert_eq!(id.as_deref(), Some("abc"));
            }
            _ => panic!("expected the dlq command"),
        }
    }

    #[test]
    fn broker_settings_resolve_env_then_config_then_default() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let key = "MIRRORLANE_WORKLANE_LEASE_SECS";
        unsafe { std::env::remove_var(key) };

        // Default: neither env nor config → lease is None (worklane default), but
        // max_deliveries defaults to the built-in backstop so redelivery is bounded
        // out of the box. Secrets are never consulted here.
        let m = "MIRRORLANE_WORKLANE_MAX_DELIVERIES";
        unsafe { std::env::remove_var(m) };
        let s =
            resolve_broker_settings(worklane_core::RetentionPolicy::new(), None).expect("resolve");
        assert_eq!(s.lease, None);
        assert_eq!(s.max_deliveries, Some(DEFAULT_MAX_DELIVERIES));

        // Config supplies a value when no env var is set.
        let cfg = WorklaneConfig {
            lease_secs: Some(10),
            ..WorklaneConfig::default()
        };
        let s = resolve_broker_settings(worklane_core::RetentionPolicy::new(), Some(cfg))
            .expect("resolve");
        assert_eq!(s.lease, Some(std::time::Duration::from_secs(10)));

        // Env wins over config.
        unsafe { std::env::set_var(key, "20") };
        let cfg = WorklaneConfig {
            lease_secs: Some(10),
            ..WorklaneConfig::default()
        };
        let s = resolve_broker_settings(worklane_core::RetentionPolicy::new(), Some(cfg))
            .expect("resolve");
        assert_eq!(
            s.lease,
            Some(std::time::Duration::from_secs(20)),
            "env beats config"
        );

        unsafe { std::env::remove_var(key) };
    }

    #[test]
    fn handler_timeout_resolves_default_then_config_then_env() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let key = "MIRRORLANE_WORKLANE_HANDLER_TIMEOUT_SECS";
        unsafe { std::env::remove_var(key) };

        // Default when neither env nor config sets it.
        assert_eq!(
            resolve_handler_timeout(None).expect("resolve"),
            std::time::Duration::from_secs(DEFAULT_HANDLER_TIMEOUT_SECS)
        );

        // Config applies when no env var is set.
        let cfg = WorklaneConfig {
            handler_timeout_secs: Some(30),
            ..WorklaneConfig::default()
        };
        assert_eq!(
            resolve_handler_timeout(Some(&cfg)).expect("resolve"),
            std::time::Duration::from_secs(30)
        );

        // Env wins over config.
        unsafe { std::env::set_var(key, "45") };
        assert_eq!(
            resolve_handler_timeout(Some(&cfg)).expect("resolve"),
            std::time::Duration::from_secs(45)
        );
        unsafe { std::env::remove_var(key) };
    }

    #[test]
    fn malformed_substrate_numeric_is_an_error() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let key = "MIRRORLANE_WORKLANE_POOL_SIZE";
        unsafe { std::env::set_var(key, "not-a-number") };
        let result = resolve_broker_settings(worklane_core::RetentionPolicy::new(), None);
        unsafe { std::env::remove_var(key) };
        assert!(
            result.is_err(),
            "a malformed numeric substrate value is reported"
        );
    }

    #[test]
    fn flags_are_unset_by_default() {
        let cli = Cli::try_parse_from(["mirrorlane", "replay"]).expect("parse");
        assert_eq!(cli.provider, None);
        assert_eq!(cli.format, None);
        assert_eq!(cli.db, None);
        assert_eq!(cli.config, None);
    }

    #[test]
    fn provider_ollama_is_accepted() {
        let cli =
            Cli::try_parse_from(["mirrorlane", "--provider", "ollama", "replay"]).expect("parse");
        assert_eq!(cli.provider, Some(Provider::Ollama));
    }

    #[test]
    fn unknown_provider_is_rejected() {
        assert!(Cli::try_parse_from(["mirrorlane", "--provider", "bogus", "replay"]).is_err());
    }

    #[test]
    fn format_json_is_accepted() {
        let cli = Cli::try_parse_from(["mirrorlane", "--format", "json", "replay"]).expect("parse");
        assert_eq!(cli.format, Some(Format::Json));
    }

    #[test]
    fn unknown_format_is_rejected() {
        assert!(Cli::try_parse_from(["mirrorlane", "--format", "bogus", "replay"]).is_err());
    }

    #[test]
    fn strategy_flag_is_accepted() {
        let cli =
            Cli::try_parse_from(["mirrorlane", "--strategy", "empty", "replay"]).expect("parse");
        assert_eq!(cli.strategy.as_deref(), Some("empty"));
    }

    #[test]
    fn submit_and_work_subcommands_parse() {
        assert!(Cli::try_parse_from(["mirrorlane", "submit"]).is_ok());
        assert!(Cli::try_parse_from(["mirrorlane", "work"]).is_ok());
    }

    #[test]
    fn queue_db_flag_is_accepted() {
        let cli = Cli::try_parse_from(["mirrorlane", "--queue-db", "q.db", "work"]).expect("parse");
        assert_eq!(cli.queue_db.as_deref(), Some("q.db"));
    }

    #[test]
    fn provider_flag_accepts_known_kinds_and_rejects_unknown() {
        for p in ["mock", "ollama", "openai"] {
            assert!(
                Cli::try_parse_from(["mirrorlane", "--provider", p, "replay"]).is_ok(),
                "{p} is a valid provider"
            );
        }
        assert!(
            Cli::try_parse_from(["mirrorlane", "--provider", "claude", "replay"]).is_err(),
            "an unknown provider is rejected at parse time"
        );
    }

    #[test]
    fn retention_resolves_defaults_and_overrides() {
        // Absent config → built-in bounded defaults.
        let (policy, cap) = resolve_retention(None);
        assert_eq!(policy.max_count, Some(DEFAULT_DEAD_LETTER_MAX_COUNT));
        assert_eq!(policy.max_age, None);
        assert_eq!(cap, DEFAULT_TRACE_MAX_COUNT);

        // Config overrides count, age, and trace cap.
        let (policy, cap) = resolve_retention(Some(RetentionConfig {
            dead_letter_max_count: Some(5),
            dead_letter_max_age_secs: Some(3600),
            trace_max_count: Some(42),
        }));
        assert_eq!(policy.max_count, Some(5));
        assert_eq!(policy.max_age, Some(std::time::Duration::from_secs(3600)));
        assert_eq!(cap, 42);
    }

    #[test]
    fn config_ignores_secret_keys() {
        // There is no openai/github secret field in Config, so a secret placed in
        // the config file is silently ignored (unknown-key tolerance) — secrets come
        // only from the environment.
        let config = Config::from_json(
            r#"{"openai_model":"m","openai_api_key":"sk-leak","github_token":"ghp_leak"}"#,
        )
        .expect("parse");
        assert_eq!(config.openai_model.as_deref(), Some("m"));
    }

    #[test]
    fn broker_flag_accepts_known_kinds_and_rejects_unknown() {
        for kind in ["sqlite", "postgres", "redis"] {
            assert!(
                Cli::try_parse_from(["mirrorlane", "--broker", kind, "work"]).is_ok(),
                "{kind} is a valid broker"
            );
        }
        assert!(
            Cli::try_parse_from(["mirrorlane", "--broker", "mongo", "work"]).is_err(),
            "an unknown broker is rejected at parse time, before the command runs"
        );
    }

    #[test]
    fn strategy_resolves_flag_then_config_then_default() {
        // Flag wins over config.
        assert_eq!(
            resolve_strategy(Some("empty".into()), Some("projection".into())),
            "empty"
        );
        // Config fills when no flag.
        assert_eq!(resolve_strategy(None, Some("empty".into())), "empty");
        // Default reference strategy when neither.
        assert_eq!(resolve_strategy(None, None), StrategyRegistry::DEFAULT);
    }

    #[test]
    fn config_parses_and_ignores_unknown_keys() {
        let config = Config::from_json(
            r#"{"db":"x.db","provider":"ollama","format":"json","strategy":"empty",
                "ollama_base_url":"http://host:1234","ollama_model":"m",
                "ollama_prompt_version":"v9","github_base_url":"http://gh.test",
                "bogus":1}"#,
        )
        .expect("parse config");
        assert_eq!(config.db.as_deref(), Some("x.db"));
        assert_eq!(config.provider, Some(Provider::Ollama));
        assert_eq!(config.format, Some(Format::Json));
        assert_eq!(config.strategy.as_deref(), Some("empty"));
        assert_eq!(config.ollama_base_url.as_deref(), Some("http://host:1234"));
        assert_eq!(config.ollama_model.as_deref(), Some("m"));
        assert_eq!(config.ollama_prompt_version.as_deref(), Some("v9"));
        assert_eq!(config.github_base_url.as_deref(), Some("http://gh.test"));
    }

    #[test]
    fn empty_config_is_all_none() {
        let config = Config::from_json("{}").expect("parse config");
        assert_eq!(config.db, None);
        assert_eq!(config.provider, None);
        assert_eq!(config.format, None);
    }

    #[test]
    fn resolver_prefers_flag_then_config_then_default() {
        // Flag wins over config.
        assert_eq!(
            resolve_provider(Some(Provider::Ollama), Some(Provider::Mock)),
            Provider::Ollama
        );
        // Config fills when no flag.
        assert_eq!(
            resolve_provider(None, Some(Provider::Ollama)),
            Provider::Ollama
        );
        // Built-in default when neither.
        assert_eq!(resolve_provider(None, None), Provider::Mock);
        assert_eq!(resolve_format(None, None), Format::Text);
        assert_eq!(resolve_db(None, None), "mirrorlane.db");
        assert_eq!(
            resolve_db(Some("flag.db".into()), Some("cfg.db".into())),
            "flag.db"
        );
        assert_eq!(resolve_db(None, Some("cfg.db".into())), "cfg.db");
    }

    #[test]
    fn resolve_str_prefers_flag_then_config_then_default() {
        // Flag wins over config.
        assert_eq!(
            resolve_str(Some("flag".into()), Some("cfg".into()), "default"),
            "flag"
        );
        // Config fills when no flag.
        assert_eq!(resolve_str(None, Some("cfg".into()), "default"), "cfg");
        // Built-in default when neither.
        assert_eq!(resolve_str(None, None, "default"), "default");
    }

    #[test]
    fn endpoint_flags_parse() {
        let cli = Cli::try_parse_from([
            "mirrorlane",
            "--ollama-base-url",
            "http://host:1234",
            "--ollama-model",
            "m",
            "--github-base-url",
            "http://gh.test",
            "replay",
        ])
        .expect("parse");
        assert_eq!(cli.ollama_base_url.as_deref(), Some("http://host:1234"));
        assert_eq!(cli.ollama_model.as_deref(), Some("m"));
        assert_eq!(cli.github_base_url.as_deref(), Some("http://gh.test"));
    }

    #[test]
    fn explicit_missing_config_is_an_error() {
        assert!(load_config(Some("/no/such/mirrorlane-config.json")).is_err());
    }
}
