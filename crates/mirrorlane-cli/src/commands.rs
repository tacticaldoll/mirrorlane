//! Command handlers, generic over the `MessageStore` port so they can be
//! unit-tested with an in-memory store, without spawning the binary.

use std::sync::Arc;

use clap::ValueEnum;
use mirrorlane_core::message::{ConversationId, MessageEnvelope, MessageId};
use mirrorlane_core::routing::{ConsumerKind, RoutingDecision, RoutingHint, RoutingRule};
use mirrorlane_core::scope::Scope;
use mirrorlane_core::skill::SessionDevelopers;
use mirrorlane_core::warmup::WarmupDocument;
use mirrorlane_core::{
    ConversationDerivation, DerivedOutputCache, InMemoryDerivedOutputCache, MessageStore,
    ProjectionStore, Projector, Router, StepVersion,
};
use mirrorlane_github::{GitHubDraft, Repo, RestGitHubSource, draft_for, ingest_items};
use mirrorlane_llm::LlmProjector;
use mirrorlane_ollama::OllamaClient;
use mirrorlane_openai::OpenAiClient;
use mirrorlane_provider::{
    CachingProjector, MessageSkillBuilder, MockProjector, MockScopeProjector, MockWarmupBuilder,
    RuleRouter, SkillDeveloperSnapshotter, SkillRoutingHinter,
};
use mirrorlane_storage::SqliteProjectionCache;
use mirrorlane_worker::{
    ReplayStores, ReplayStrategy, StrategyContext, StrategyRegistry, content_hash_of,
    derivation_for, messages_in, populate_cache,
};
use serde::{Deserialize, Serialize};

/// The output schema id carried by every emitted [`SessionContext`], so a machine
/// consumer can detect a breaking shape change. Bump the trailing version when the
/// JSON shape changes incompatibly.
pub const SESSION_CONTEXT_SCHEMA: &str = "mirrorlane.session_context/1";

/// The full structured context for one session, the `--format json` payload:
/// the conversation's warm-up plus its scope, developers, and per-message
/// routing hints. Assembled from a replay's stores; absent scope/developers
/// serialize to `null`. The `schema` field versions this output contract.
#[derive(Debug, Serialize)]
pub struct SessionContext {
    pub schema: &'static str,
    /// The derivation version that produced this output — its provenance, so a reader
    /// knows which (strategy + projector + schema) version the context came from.
    pub derivation_version: String,
    pub conversation: ConversationId,
    pub warmup: WarmupDocument,
    pub scope: Option<Scope>,
    pub developers: Option<SessionDevelopers>,
    pub hints: Vec<RoutingHint>,
    pub decisions: Vec<RoutingDecision>,
    pub drafts: Vec<GitHubDraft>,
}

/// Which projector backs the `replay` and `warmup` commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Provider {
    /// Deterministic keyword mock (default).
    #[default]
    Mock,
    /// Local Ollama-backed SLM, wrapped in a `CachingProjector` for replay safety.
    Ollama,
    /// OpenAI-compatible chat-completions backend (OpenAI, vLLM, LM Studio, …),
    /// wrapped in a `CachingProjector` for replay safety.
    Openai,
}

/// Resolved Ollama endpoint settings for the `ollama` provider. Defaults to the
/// `mirrorlane-ollama` crate constants; the CLI overrides them by flag or config.
#[derive(Debug, Clone)]
pub struct OllamaSettings {
    pub base_url: String,
    pub model: String,
    pub prompt_version: String,
}

impl Default for OllamaSettings {
    fn default() -> Self {
        Self {
            base_url: mirrorlane_ollama::DEFAULT_BASE_URL.to_string(),
            model: mirrorlane_ollama::DEFAULT_MODEL.to_string(),
            prompt_version: mirrorlane_ollama::DEFAULT_PROMPT_VERSION.to_string(),
        }
    }
}

/// Resolved OpenAI-compatible endpoint settings for the `openai` provider. The API
/// key is **not** here — it is read only from `OPENAI_API_KEY` at construction.
#[derive(Debug, Clone)]
pub struct OpenAiSettings {
    pub base_url: String,
    pub model: String,
    pub prompt_version: String,
}

impl Default for OpenAiSettings {
    fn default() -> Self {
        Self {
            base_url: mirrorlane_openai::DEFAULT_BASE_URL.to_string(),
            model: mirrorlane_openai::DEFAULT_MODEL.to_string(),
            prompt_version: mirrorlane_llm::DEFAULT_PROMPT_VERSION.to_string(),
        }
    }
}

/// All LLM-provider settings, resolved once and threaded to the projector builder.
#[derive(Debug, Clone, Default)]
pub struct LlmSettings {
    pub ollama: OllamaSettings,
    pub openai: OpenAiSettings,
}

/// A routing rule set from configuration: the rules, an optional escalation
/// threshold, and an optional default target for an intent with no matching rule.
/// Every field is optional so a partial `routing` config is well-defined.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct RoutingConfig {
    #[serde(default)]
    pub rules: Vec<RoutingRule>,
    #[serde(default)]
    pub escalation_threshold: Option<f64>,
    #[serde(default)]
    pub default_target: Option<ConsumerKind>,
}

/// Build the deterministic router from optional configuration: a `routing` config
/// supplies the rule set (`with_rules`); its absence yields the default rule set
/// (`RuleRouter::new()`), identical to prior behavior.
pub fn build_router(config: Option<RoutingConfig>) -> RuleRouter {
    match config {
        Some(rc) => RuleRouter::with_rules(
            rc.rules,
            rc.escalation_threshold.unwrap_or(0.6),
            rc.default_target.unwrap_or(ConsumerKind::Human),
        ),
        None => RuleRouter::new(),
    }
}

/// Build the projector for a provider. Scope and warm-up always use their mocks;
/// only the message projector varies. The Ollama projector is wrapped in a
/// `CachingProjector` over a **durable** `SqliteProjectionCache` at the `db` path,
/// so the model is called at most once per (version, message): the first success
/// is frozen and every later replay reads it back without calling the model,
/// keeping replay deterministic and cheap even with a non-deterministic model.
fn projector_for(
    provider: Provider,
    db: &str,
    llm: &LlmSettings,
) -> Result<Arc<dyn Projector>, Box<dyn std::error::Error>> {
    // Both real providers produce the same concrete `LlmProjector`, differing only
    // in their `LlmClient` transport; the mock returns early.
    let projector = match provider {
        Provider::Mock => return Ok(Arc::new(MockProjector::new())),
        Provider::Ollama => LlmProjector::new(
            Arc::new(OllamaClient::new(&llm.ollama.base_url)),
            &llm.ollama.model,
            &llm.ollama.prompt_version,
        ),
        Provider::Openai => LlmProjector::new(
            // The key comes only from the environment, never the config file.
            Arc::new(OpenAiClient::new(
                &llm.openai.base_url,
                std::env::var("OPENAI_API_KEY").ok(),
            )),
            &llm.openai.model,
            &llm.openai.prompt_version,
        ),
    };
    // Wrap the real provider in a durable CachingProjector so a model run stays
    // replay-safe. The version tag (provider:model:prompt) keys the cache; opening
    // it reclaims rows under any prior version (stale-by-key, never re-read).
    let version = projector.cache_version();
    let cache = SqliteProjectionCache::open(db)?;
    let _ = cache.prune_superseded(&StepVersion::new(version.clone()));
    Ok(Arc::new(CachingProjector::new(
        Arc::new(projector),
        Arc::new(cache),
        version,
    )))
}

/// The resolved derivation ports for the selected provider: the chosen message
/// projector plus the deterministic mock scope and warm-up providers. The
/// strategy a user selects wires from this context.
pub fn strategy_context(
    provider: Provider,
    db: &str,
    llm: &LlmSettings,
) -> Result<StrategyContext, Box<dyn std::error::Error>> {
    Ok(StrategyContext {
        projector: projector_for(provider, db, llm)?,
        scoper: Arc::new(MockScopeProjector::new()),
        builder: Arc::new(MockWarmupBuilder::new()),
        skill_builder: Arc::new(MessageSkillBuilder::new()),
        hinter: Arc::new(SkillRoutingHinter::new()),
        snapshotter: Arc::new(SkillDeveloperSnapshotter::new()),
    })
}

/// Resolve a strategy id to a runnable strategy, wired with the selected provider.
/// An unregistered id is surfaced to the caller as an error, not a panic.
pub fn build_strategy(
    provider: Provider,
    db: &str,
    llm: &LlmSettings,
    id: &str,
) -> Result<Arc<dyn ReplayStrategy>, Box<dyn std::error::Error>> {
    let context = strategy_context(provider, db, llm)?;
    Ok(StrategyRegistry::with_builtins().build(id, &context)?)
}

/// Parse a `MessageEnvelope` from JSON and append it to the log; returns its id.
pub fn ingest(store: &dyn MessageStore, json: &str) -> Result<MessageId, serde_json::Error> {
    let message: MessageEnvelope = serde_json::from_str(json)?;
    let id = message.id.clone();
    store.append(message);
    Ok(id)
}

/// Parse a `owner/name` repo spec, requiring two non-empty segments.
pub fn parse_repo(spec: &str) -> Result<Repo, String> {
    match spec.split_once('/') {
        Some((owner, name)) if !owner.is_empty() && !name.is_empty() && !name.contains('/') => {
            Ok(Repo::new(owner, name))
        }
        _ => Err(format!("invalid --repo {spec:?}; expected owner/name")),
    }
}

/// Fetch a repo's items through the real REST source and append them to the log,
/// idempotently by message id. Uses the **fallible** `try_fetch`, so a transport,
/// status, read, or parse failure (or an invalid repo) is returned as an error and
/// nothing is ingested — the one-shot `github` CLI command reports it cleanly
/// rather than panicking. Returns the appended message ids in fetch order.
pub fn github_ingest_rest(
    store: &dyn MessageStore,
    source: &RestGitHubSource,
    repo: &str,
) -> Result<Vec<MessageId>, Box<dyn std::error::Error>> {
    let repo = parse_repo(repo)?;
    let items = source.try_fetch(&repo)?;
    Ok(ingest_items(&items, store))
}

/// Assemble a full session context from a derivation (cached or fresh), re-deriving
/// the read-time routing decisions and GitHub drafts from its projections. The
/// deterministic router (and `draft_for`) dispatch nothing, so this is a pure
/// read-time function — kept out of the cached unit (it pulls in `GitHubDraft`).
fn session_context_from(
    derivation: &ConversationDerivation,
    version: &str,
    router: &RuleRouter,
) -> SessionContext {
    let routed: Vec<_> = derivation
        .projections
        .iter()
        .map(|projection| {
            let decision = router.route(projection);
            (projection, decision)
        })
        .collect();
    let decisions = routed
        .iter()
        .map(|(_, (decision, _))| decision.clone())
        .collect();
    let drafts = routed
        .iter()
        .filter(|(_, (decision, _))| decision.target == ConsumerKind::GitHub)
        .map(|(projection, _)| draft_for(projection))
        .collect();
    SessionContext {
        schema: SESSION_CONTEXT_SCHEMA,
        derivation_version: version.to_string(),
        conversation: derivation.conversation.clone(),
        warmup: derivation.warmup.clone(),
        scope: derivation.scope.clone(),
        developers: derivation.developers.clone(),
        hints: derivation.hints.clone(),
        decisions,
        drafts,
    }
}

/// The derivation version used to key the derived-output cache: the global schema
/// version, the strategy id, and the resolved projector's version (see
/// [`mirrorlane_core::derivation_version`]).
pub fn derivation_version(
    strategy: &str,
    provider: Provider,
    db: &str,
    llm: &LlmSettings,
) -> Result<StepVersion, Box<dyn std::error::Error>> {
    Ok(mirrorlane_core::derivation_version(
        strategy,
        &projector_for(provider, db, llm)?.version(),
    ))
}

/// Logged message ids (among `ids`) that produced no projection — e.g. a real
/// projector failed (panicked at the boundary) for them, so nothing was stored.
/// Empty on a clean replay; non-empty means the derived context is incomplete.
fn unprojected(stores: &ReplayStores, ids: impl IntoIterator<Item = MessageId>) -> Vec<MessageId> {
    ids.into_iter()
        .filter(|id| stores.projections.get(id).is_none())
        .collect()
}

/// The contexts from a replay plus any messages that produced no projection.
pub struct ReplayOutput {
    pub contexts: Vec<SessionContext>,
    pub unprojected: Vec<MessageId>,
}

/// One conversation's context (if present) plus any of its messages that
/// produced no projection.
pub struct WarmupOutput {
    pub context: Option<SessionContext>,
    pub unprojected: Vec<MessageId>,
}

/// Replay the log and return each conversation's full context, in first-seen
/// order, plus any messages that produced no projection. Replay is the cache
/// **producer**: each conversation's derivation is written to `cache` so later
/// per-conversation reads can serve it without recomputing.
pub async fn replay(
    store: &(dyn MessageStore + 'static),
    strategy: &dyn ReplayStrategy,
    cache: &dyn DerivedOutputCache,
    version: &StepVersion,
    router: &RuleRouter,
) -> ReplayOutput {
    let stores = strategy.run(store).await;
    let contexts = populate_cache(store, &stores, cache, version)
        .iter()
        .map(|d| session_context_from(d, version.as_str(), router))
        .collect();
    let unprojected = unprojected(&stores, store.all().into_iter().map(|m| m.id));
    ReplayOutput {
        contexts,
        unprojected,
    }
}

/// Return one conversation's full context, serving it from `cache` when the version
/// and content match (no replay), and otherwise replaying — the cache **consumer**.
/// On a miss the whole-log replay populates every conversation, so a later read of
/// any conversation hits.
pub async fn warmup(
    store: &(dyn MessageStore + 'static),
    conversation: &ConversationId,
    strategy: &dyn ReplayStrategy,
    cache: &dyn DerivedOutputCache,
    version: &StepVersion,
    router: &RuleRouter,
) -> WarmupOutput {
    // Hash only this conversation's messages (via the per-conversation read), not
    // the whole log — so a cache hit never loads other conversations.
    let content = content_hash_of(&store.messages_for(conversation));
    if let Some(derivation) = cache.get(version, conversation, &content) {
        // Hit: serve from the cache without running the strategy. Unprojected are
        // this conversation's messages absent from the cached projections.
        let projected: Vec<MessageId> = derivation
            .projections
            .iter()
            .map(|projection| projection.message_id.clone())
            .collect();
        let unprojected = messages_in(store, conversation)
            .into_iter()
            .filter(|id| !projected.contains(id))
            .collect();
        return WarmupOutput {
            context: Some(session_context_from(&derivation, version.as_str(), router)),
            unprojected,
        };
    }
    // Miss: replay, populate the cache for every conversation, then return this one.
    let stores = strategy.run(store).await;
    populate_cache(store, &stores, cache, version);
    let context = derivation_for(store, &stores, conversation)
        .map(|d| session_context_from(&d, version.as_str(), router));
    let unprojected = unprojected(&stores, messages_in(store, conversation));
    WarmupOutput {
        context,
        unprojected,
    }
}
/// One conversation's verification result: whether the durable derived-output cache
/// still equals what recomputing the derivation now produces.
#[derive(Debug, Serialize, PartialEq)]
pub struct VerifyRecord {
    pub conversation: ConversationId,
    /// `"verified"` (stored equals a fresh recompute), `"diverged"` (stored differs —
    /// a stale cache under an unbumped version, or a non-deterministic step), or
    /// `"not_cached"` (nothing stored at the current version to check).
    pub outcome: &'static str,
}

/// Prove determinism on demand: recompute every conversation's derivation fresh and
/// compare it to the durable derived-output cache. A divergence means the stored
/// output no longer equals what the current code and version produce — the glass-box
/// guarantee turned into a runnable check rather than an assertion. The durable cache
/// is only read; the recompute writes to a scratch in-memory cache.
pub async fn verify(
    store: &(dyn MessageStore + 'static),
    strategy: &dyn ReplayStrategy,
    durable: &dyn DerivedOutputCache,
    version: &StepVersion,
) -> Vec<VerifyRecord> {
    let stores = strategy.run(store).await;
    let scratch = InMemoryDerivedOutputCache::new();
    populate_cache(store, &stores, &scratch, version)
        .into_iter()
        .map(|fresh| {
            let content = content_hash_of(&store.messages_for(&fresh.conversation));
            let outcome = match durable.get(version, &fresh.conversation, &content) {
                None => "not_cached",
                Some(stored) if stored == fresh => "verified",
                Some(_) => "diverged",
            };
            VerifyRecord {
                conversation: fresh.conversation,
                outcome,
            }
        })
        .collect()
}

/// The context returned by the inspect command.
#[derive(Debug, Serialize)]
pub struct InspectContext {
    pub message: MessageEnvelope,
    pub projection: mirrorlane_core::projection::Projection,
    pub trace: Option<mirrorlane_core::routing::RoutingTrace>,
    pub decision: RoutingDecision,
}

/// Load a message, project it, and fetch its routing trace from the store.
pub async fn inspect(
    store: &dyn MessageStore,
    traces: &dyn mirrorlane_core::RoutingTraceStore,
    message_id: &MessageId,
    provider: Provider,
    db: &str,
    llm: &LlmSettings,
    router: &RuleRouter,
) -> Result<Option<InspectContext>, Box<dyn std::error::Error>> {
    let Some(message) = store.get(message_id) else {
        return Ok(None);
    };
    let projector = projector_for(provider, db, llm)?;
    let projection = projector.project(&message);
    let (decision, _) = router.route(&projection);
    let trace = traces.get(message_id);
    Ok(Some(InspectContext {
        message,
        projection,
        trace,
        decision,
    }))
}
#[cfg(test)]
mod tests {
    use super::*;
    use mirrorlane_core::InMemoryMessageStore;
    use mirrorlane_storage::SqliteDerivedOutputCache;

    /// The default projection strategy over the mock provider, for the tests that
    /// exercise the replay path rather than strategy selection.
    fn projection() -> Arc<dyn ReplayStrategy> {
        build_strategy(
            Provider::Mock,
            ":memory:",
            &LlmSettings::default(),
            StrategyRegistry::DEFAULT,
        )
        .expect("projection is a built-in strategy")
    }

    /// The `empty` built-in strategy (derives nothing) — used to prove a read was
    /// served from the cache, not recomputed.
    fn empty() -> Arc<dyn ReplayStrategy> {
        build_strategy(Provider::Mock, ":memory:", &LlmSettings::default(), "empty")
            .expect("empty is a built-in strategy")
    }

    fn cache() -> SqliteDerivedOutputCache {
        SqliteDerivedOutputCache::open_in_memory().expect("open cache")
    }

    fn router() -> RuleRouter {
        RuleRouter::new()
    }

    fn version() -> StepVersion {
        derivation_version(
            StrategyRegistry::DEFAULT,
            Provider::Mock,
            ":memory:",
            &LlmSettings::default(),
        )
        .expect("derivation version")
    }

    fn message_json(id: &str, conversation: &str, body: &str) -> String {
        format!(
            r#"{{"id":"{id}","source":"discord","author":{{"id":"u-1","display_name":"Dev"}},"conversation":{{"id":"{conversation}","thread":null}},"body":"{body}"}}"#
        )
    }

    #[test]
    fn build_router_honors_config_else_defaults() {
        use mirrorlane_core::Router;
        use mirrorlane_core::projection::{Confidence, Intent, Projection};

        fn issue() -> Projection {
            Projection {
                message_id: MessageId("m-1".into()),
                intent: Intent::Issue,
                topics: vec![],
                entities: vec![],
                confidence: Confidence::new(0.9),
            }
        }

        // A `routing` config retargets Issue (default GitHub) to Agent.
        let configured = build_router(Some(RoutingConfig {
            rules: vec![RoutingRule {
                intent: Intent::Issue,
                target: ConsumerKind::Agent,
            }],
            escalation_threshold: None,
            default_target: None,
        }));
        assert_eq!(configured.route(&issue()).0.target, ConsumerKind::Agent);

        // Absent config → the default rule set (Issue → GitHub), unchanged.
        assert_eq!(
            build_router(None).route(&issue()).0.target,
            ConsumerKind::GitHub
        );
    }

    #[test]
    fn ingest_appends_and_dedups_by_id() {
        let store = InMemoryMessageStore::new();
        let id = ingest(&store, &message_json("m-1", "c-1", "auth sdk")).expect("ingest");
        assert_eq!(id.0, "m-1");

        ingest(&store, &message_json("m-1", "c-1", "auth sdk again")).expect("re-ingest");
        assert_eq!(store.len(), 1, "same id must not duplicate");
    }

    #[test]
    fn ingest_rejects_invalid_json() {
        let store = InMemoryMessageStore::new();
        assert!(ingest(&store, "not json").is_err());
    }

    #[tokio::test]
    async fn warmup_returns_document_for_known_conversation() {
        let store = InMemoryMessageStore::new();
        ingest(
            &store,
            &message_json("m-1", "c-1", "We will use sqlite for the auth sdk."),
        )
        .expect("ingest");

        let cache = cache();
        let v = version();
        let context = warmup(
            &store,
            &ConversationId("c-1".into()),
            projection().as_ref(),
            &cache,
            &v,
            &router(),
        )
        .await
        .context
        .expect("known conversation has a context");
        assert_eq!(context.conversation, ConversationId("c-1".into()));
        assert_eq!(context.warmup.conversation, ConversationId("c-1".into()));
        // The output carries its schema version, and it survives serialization — so a
        // machine consumer can detect a breaking shape change.
        assert_eq!(context.schema, SESSION_CONTEXT_SCHEMA);
        // The output stamps its provenance: the derivation version that produced it.
        assert_eq!(context.derivation_version, v.as_str());
        let json = serde_json::to_value(&context).expect("serialize");
        assert_eq!(json["schema"], SESSION_CONTEXT_SCHEMA);
        assert_eq!(json["derivation_version"], v.as_str());
        // The session's messages each derive a routing hint and a routing decision.
        assert!(!context.hints.is_empty(), "session carries routing hints");
        assert!(
            !context.decisions.is_empty(),
            "session carries routing decisions"
        );
        assert!(
            warmup(
                &store,
                &ConversationId("c-x".into()),
                projection().as_ref(),
                &cache,
                &v,
                &router(),
            )
            .await
            .context
            .is_none()
        );
    }

    #[tokio::test]
    async fn verify_reports_not_cached_then_verified_then_diverged() {
        let store = InMemoryMessageStore::new();
        ingest(
            &store,
            &message_json("m-1", "c-1", "We will use sqlite for the auth sdk."),
        )
        .expect("ingest");
        let durable = cache();
        let v = version();

        // Nothing stored yet → not_cached.
        let recs = verify(&store, projection().as_ref(), &durable, &v).await;
        assert!(!recs.is_empty());
        assert!(recs.iter().all(|r| r.outcome == "not_cached"));

        // Populate the durable cache, then verify → every conversation verified.
        replay(&store, projection().as_ref(), &durable, &v, &router()).await;
        let recs = verify(&store, projection().as_ref(), &durable, &v).await;
        assert!(recs.iter().all(|r| r.outcome == "verified"));

        // Tamper with a stored value under the same key → diverged (simulating a
        // stale cache written under an unbumped version).
        let conversation = ConversationId("c-1".into());
        let content = content_hash_of(&store.messages_for(&conversation));
        let mut tampered = durable.get(&v, &conversation, &content).expect("present");
        tampered.projections.clear();
        tampered.hints.clear();
        durable.put(&v, &conversation, &content, tampered);
        let recs = verify(&store, projection().as_ref(), &durable, &v).await;
        assert!(recs.iter().any(|r| r.outcome == "diverged"));
    }

    #[tokio::test]
    async fn replay_returns_one_warmup_per_conversation() {
        let store = InMemoryMessageStore::new();
        ingest(&store, &message_json("m-1", "c-1", "auth sdk")).expect("ingest");
        ingest(&store, &message_json("m-2", "c-2", "infra ci")).expect("ingest");

        assert_eq!(
            replay(
                &store,
                projection().as_ref(),
                &cache(),
                &version(),
                &router()
            )
            .await
            .contexts
            .len(),
            2
        );
    }

    #[tokio::test]
    async fn selecting_empty_replays_nothing() {
        let store = InMemoryMessageStore::new();
        ingest(&store, &message_json("m-1", "c-1", "auth sdk")).expect("ingest");

        // Same log and provider as the projection path; only the id differs.
        assert!(
            replay(&store, empty().as_ref(), &cache(), &version(), &router())
                .await
                .contexts
                .is_empty(),
            "the empty strategy derives no context — selection ran, not projection"
        );
    }

    #[test]
    fn building_the_ollama_projector_prunes_stale_version_projections() {
        use mirrorlane_core::projection::{Confidence, Intent, Projection};
        use mirrorlane_core::{Cache, message::MessageId};
        use mirrorlane_storage::SqliteProjectionCache;
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("proj.db");
        let db = path.to_str().expect("path");

        let llm = LlmSettings::default();
        // The version the freshly-built projector will use.
        let current = LlmProjector::new(
            Arc::new(OllamaClient::new(&llm.ollama.base_url)),
            &llm.ollama.model,
            &llm.ollama.prompt_version,
        )
        .cache_version();
        let projection = Projection {
            message_id: MessageId("m-1".into()),
            intent: Intent::Decision,
            topics: Vec::new(),
            entities: Vec::new(),
            confidence: Confidence::new(0.7),
        };

        // Seed a stale-version row and a current-version row, then close the file.
        {
            let cache = SqliteProjectionCache::open(db).expect("open cache");
            cache.put(
                "proj",
                &StepVersion::new("stale-v0"),
                "m-1",
                projection.clone(),
            );
            cache.put(
                "proj",
                &StepVersion::new(current.clone()),
                "m-1",
                projection.clone(),
            );
        }

        // Building the Ollama projector opens the cache and prunes stale versions.
        let _ = projector_for(Provider::Ollama, db, &llm).expect("build projector");

        let cache = SqliteProjectionCache::open(db).expect("reopen cache");
        assert!(
            cache
                .get("proj", &StepVersion::new("stale-v0"), "m-1")
                .is_none(),
            "the stale-version projection is reclaimed"
        );
        assert_eq!(
            cache.get("proj", &StepVersion::new(current), "m-1"),
            Some(projection),
            "the current-version projection still hits"
        );
    }

    #[test]
    fn openai_provider_builds_a_caching_projector() {
        // Wiring only — no network: building the projector opens the cache and
        // assembles the OpenAI client, but the model is not called.
        let projector = projector_for(Provider::Openai, ":memory:", &LlmSettings::default());
        assert!(projector.is_ok(), "the openai provider path wires up");
    }

    #[test]
    fn unknown_strategy_id_is_an_error() {
        let result = build_strategy(Provider::Mock, ":memory:", &LlmSettings::default(), "nope");
        assert!(result.is_err(), "an unregistered id is reported, not run");
    }

    #[tokio::test]
    async fn a_populated_conversation_reads_from_cache() {
        let store = InMemoryMessageStore::new();
        ingest(
            &store,
            &message_json("m-1", "c-1", "We will use sqlite for the auth sdk."),
        )
        .expect("ingest");
        let cache = cache();
        let v = version();
        // Producer: a replay populates the cache.
        replay(&store, projection().as_ref(), &cache, &v, &router()).await;
        // A read with the *empty* strategy still returns the cached projection
        // context — proving it served the cache and did not run the strategy.
        let out = warmup(
            &store,
            &ConversationId("c-1".into()),
            empty().as_ref(),
            &cache,
            &v,
            &router(),
        )
        .await;
        let context = out.context.expect("served the cached derivation");
        assert!(
            !context.hints.is_empty(),
            "the cached projection derivation was served, not the empty strategy's"
        );
    }

    #[tokio::test]
    async fn a_changed_conversation_misses_and_recomputes() {
        let store = InMemoryMessageStore::new();
        ingest(
            &store,
            &message_json("m-1", "c-1", "We will use sqlite for the auth sdk."),
        )
        .expect("ingest");
        let cache = cache();
        let v = version();
        replay(&store, projection().as_ref(), &cache, &v, &router()).await;
        // Append a message: the content hash changes, so the next read misses. With
        // the empty strategy the recompute yields no warm-up — proving the miss.
        ingest(&store, &message_json("m-2", "c-1", "another note")).expect("ingest");
        let out = warmup(
            &store,
            &ConversationId("c-1".into()),
            empty().as_ref(),
            &cache,
            &v,
            &router(),
        )
        .await;
        assert!(
            out.context.is_none(),
            "content changed → cache miss → empty strategy ran → no context"
        );
    }

    #[tokio::test]
    async fn github_targeted_message_yields_a_draft() {
        let store = InMemoryMessageStore::new();
        // "broken" -> Issue intent, with auth/sdk topics for confidence -> routes
        // to GitHub -> a draft.
        ingest(
            &store,
            &message_json("m-1", "c-1", "The auth sdk login is broken"),
        )
        .expect("ingest");
        // A plain social message -> not GitHub -> no draft.
        ingest(&store, &message_json("m-2", "c-1", "thanks everyone")).expect("ingest");

        let context = warmup(
            &store,
            &ConversationId("c-1".into()),
            projection().as_ref(),
            &cache(),
            &version(),
            &router(),
        )
        .await
        .context
        .expect("context");
        assert_eq!(
            context.drafts.len(),
            1,
            "only the GitHub-targeted message drafts"
        );
        assert_eq!(context.drafts[0].message_id, MessageId("m-1".into()));
    }

    mod github {
        use super::*;
        use mirrorlane_github::{FixtureGitHubSource, GitHubItem, GitHubItemKind, ingest_repo};

        fn item(id: &str, number: u64) -> GitHubItem {
            GitHubItem {
                kind: GitHubItemKind::Issue,
                repo: Repo::new("acme", "widgets"),
                number,
                id: id.into(),
                author_login: "alice".into(),
                title: Some("Title".into()),
                body: "Body".into(),
            }
        }

        #[test]
        fn parse_repo_accepts_owner_name() {
            assert_eq!(parse_repo("acme/widgets"), Ok(Repo::new("acme", "widgets")));
        }

        #[test]
        fn parse_repo_rejects_malformed() {
            assert!(parse_repo("widgets").is_err());
            assert!(parse_repo("acme/").is_err());
            assert!(parse_repo("/widgets").is_err());
            assert!(parse_repo("a/b/c").is_err());
        }

        #[test]
        fn ingest_appends_one_message_per_item() {
            let store = InMemoryMessageStore::new();
            let source = FixtureGitHubSource::new(vec![item("1", 1), item("2", 2)]);
            let ids = ingest_repo(&source, &Repo::new("acme", "widgets"), &store);
            assert_eq!(ids.len(), 2);
            assert_eq!(store.len(), 2);
        }

        #[test]
        fn ingest_is_idempotent() {
            let store = InMemoryMessageStore::new();
            let source = FixtureGitHubSource::new(vec![item("1", 1), item("2", 2)]);
            let repo = Repo::new("acme", "widgets");
            ingest_repo(&source, &repo, &store);
            ingest_repo(&source, &repo, &store);
            assert_eq!(store.len(), 2, "stable ids dedup on re-run");
        }

        #[test]
        fn rest_ingest_rejects_invalid_repo_and_ingests_nothing() {
            // Invalid repo fails before any fetch; no network involved.
            let store = InMemoryMessageStore::new();
            let source = RestGitHubSource::new("http://127.0.0.1:1", None);
            assert!(github_ingest_rest(&store, &source, "bogus").is_err());
            assert_eq!(store.len(), 0, "nothing ingested on invalid repo");
        }

        #[test]
        fn rest_ingest_surfaces_a_fetch_failure_as_error() {
            // A valid repo, but the base URL points at a closed local port, so the
            // fetch fails with a transport error — no external network. The fallible
            // path must return Err and ingest nothing, rather than panicking.
            let store = InMemoryMessageStore::new();
            let source = RestGitHubSource::new("http://127.0.0.1:1", None);
            let result = github_ingest_rest(&store, &source, "acme/widgets");
            assert!(result.is_err(), "a fetch failure is a clean error");
            assert_eq!(store.len(), 0, "nothing ingested when the fetch fails");
        }
    }

    mod inspect {
        use super::*;
        use mirrorlane_core::routing::{EvaluationStep, RoutingTrace};
        use mirrorlane_core::{InMemoryMessageStore, InMemoryRoutingTraceStore, RoutingTraceStore};

        #[tokio::test]
        async fn inspect_returns_context_with_trace() {
            let store = InMemoryMessageStore::new();
            let traces = InMemoryRoutingTraceStore::new();
            let id = ingest(&store, &message_json("m-1", "c-1", "Should we use sqlite?")).unwrap();

            // Seed a trace
            traces.upsert(RoutingTrace {
                message_id: id.clone(),
                steps: vec![EvaluationStep {
                    rule_name: "test_rule".into(),
                    matched: true,
                    resulting_target: Some(ConsumerKind::Human),
                }],
            });

            let context = inspect(
                &store,
                &traces,
                &id,
                Provider::Mock,
                ":memory:",
                &LlmSettings::default(),
                &router(),
            )
            .await
            .expect("inspect")
            .expect("context");
            assert_eq!(context.message.id, id);
            assert!(context.trace.is_some());
            assert_eq!(context.trace.unwrap().steps.len(), 1);
        }

        #[tokio::test]
        async fn inspect_returns_none_for_unknown_message() {
            let store = InMemoryMessageStore::new();
            let traces = InMemoryRoutingTraceStore::new();
            assert!(
                inspect(
                    &store,
                    &traces,
                    &MessageId("unknown".into()),
                    Provider::Mock,
                    ":memory:",
                    &LlmSettings::default(),
                    &router(),
                )
                .await
                .expect("inspect")
                .is_none()
            );
        }
    }
}
