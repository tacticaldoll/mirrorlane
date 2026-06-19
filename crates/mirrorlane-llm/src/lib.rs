//! The LLM provider seam.
//!
//! [`LlmClient`] is the **transport port**: given a model and a JSON-mode prompt it
//! returns the model's raw response text, carrying the provider/endpoint/auth and
//! nothing domain-specific. [`LlmProjector`] sits above it and owns the projection
//! prompt and the `Projection` parser **once**, so a new backend is a thin
//! `LlmClient` rather than a duplicated prompt + parser.
//!
//! The port stays sync and infallible, panicking at the boundary on failure: the
//! projection cache makes the call miss-only, so replay performs no I/O and async
//! buys nothing, and a failure freezes nothing (a wrapping `CachingProjector`
//! caches only successful runs).

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use serde::Deserialize;

use mirrorlane_core::message::{MessageEnvelope, MessageId};
use mirrorlane_core::projection::{Confidence, Entity, Intent, Projection, Topic};
use mirrorlane_runtime::{Step, StepVersion};

/// Default prompt version, an explicit discriminator kept alongside the automatic
/// prompt fingerprint (see [`prompt_fingerprint`]). It no longer needs a manual bump
/// when the prompt text changes — the fingerprint tracks that — but remains a
/// deliberate label a deployment can set.
pub const DEFAULT_PROMPT_VERSION: &str = "v2";

/// A parser version, folded into the prompt fingerprint. Bump it if
/// [`parse_projection`] changes in a way that alters output for the same model
/// response — the one part of the projector's behavior a text fingerprint cannot see.
const PARSER_VERSION: &str = "1";

/// A transport to an LLM backend: request strict-JSON output for a prompt and
/// return the model's raw response text.
///
/// Synchronous and infallible at the surface — on a transport error, a non-success
/// status, or an unreadable response it panics at the boundary rather than
/// returning a fabricated response, consistent with the projector convention.
/// Implementors carry only transport concerns (endpoint, auth, wire envelope); the
/// projection prompt and parser live in [`LlmProjector`], not here.
pub trait LlmClient: Send + Sync {
    /// Request strict-JSON output for `prompt` from `model`; return the backend's
    /// raw response text (the JSON the model emitted), unparsed.
    fn complete_json(&self, model: &str, prompt: &str) -> String;

    /// A stable identifier for this backend (e.g. `"ollama"`, `"openai"`), used to
    /// namespace cache versions so the same model on two providers cannot alias one
    /// cache entry.
    fn provider_tag(&self) -> &'static str;
}

/// The projection version tag: `"{provider}:{model}:{prompt_version}"`. A change to
/// the provider, the model, or the prompt yields a different tag and invalidates
/// prior cache entries. The Ollama provider tag is `ollama`, so an Ollama tag is
/// `ollama:{model}:{prompt_version}` — unchanged from before the provider seam, so
/// previously cached Ollama projections are not orphaned.
pub fn version_tag(provider: &str, model: &str, prompt_version: &str) -> String {
    format!("{provider}:{model}:{prompt_version}")
}

/// A stable content fingerprint of the projection prompt **template** and the parser
/// version. Editing [`projection_prompt`] (or bumping [`PARSER_VERSION`]) changes this
/// fingerprint, so the cache version changes automatically — the determinism
/// guarantee no longer depends on a human remembering to bump a constant. The
/// template is fingerprinted with an empty body, so it captures the fixed
/// instructions without depending on any one message.
pub fn prompt_fingerprint() -> String {
    fingerprint(&format!("{}\n{PARSER_VERSION}", projection_prompt("")))
}

/// A deterministic short hash of `text`. Uses the same fixed-seed hasher as the
/// content hashes elsewhere, so it is stable across runs and builds.
fn fingerprint(text: &str) -> String {
    let mut hasher = DefaultHasher::new();
    text.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// The projection prompt: classify a chat message into the strict projection JSON.
/// Shared across providers; editing it is a prompt-version bump.
pub fn projection_prompt(body: &str) -> String {
    format!(
        "You classify a team chat message into structured JSON.\n\
         Respond with ONLY a JSON object with EXACTLY these four REQUIRED fields (never omit any):\n\
         - \"intent\": one of \"question\", \"decision\", \"proposal\", \"task\", \"issue\", \"social\"\n\
         - \"topics\": array of short lowercase strings (e.g. \"rust\", \"auth\")\n\
         - \"entities\": array of short plain strings naming concrete things (e.g. \"refresh-token\"); NOT objects\n\
         - \"confidence\": a number from 0.0 to 1.0 — ALWAYS include this field\n\
         All four fields are mandatory. \"topics\" and \"entities\" are arrays of strings only; do not nest or rename fields.\n\
         Message:\n{body}"
    )
}

/// The strict JSON shape the model is asked to emit.
#[derive(Debug, Deserialize)]
struct ProjectionDto {
    intent: Intent,
    topics: Vec<String>,
    entities: Vec<String>,
    confidence: f64,
}

/// Parse a model response body into a [`Projection`], attaching `id` from the
/// envelope (never trusting the model for identity) and clamping confidence.
///
/// Pure and I/O-free. Panics if the body is not the documented projection JSON,
/// including an unknown intent: the caller relies on this so that malformed output
/// neither fabricates nor caches a projection.
pub fn parse_projection(response_body: &str, id: &MessageId) -> Projection {
    let dto: ProjectionDto = serde_json::from_str(response_body)
        .unwrap_or_else(|e| panic!("llm returned non-conforming projection JSON: {e}"));
    Projection {
        message_id: id.clone(),
        intent: dto.intent,
        topics: dto.topics.into_iter().map(Topic).collect(),
        entities: dto.entities.into_iter().map(Entity).collect(),
        confidence: Confidence::new(dto.confidence),
    }
}

/// A provider-agnostic projector: builds the projection prompt, calls any
/// [`LlmClient`], and parses the result into a [`Projection`]. Implements the
/// runtime `Step` (and so, by blanket impl, `mirrorlane_core::Projector`).
pub struct LlmProjector {
    client: Arc<dyn LlmClient>,
    model: String,
    prompt_version: String,
}

impl LlmProjector {
    /// Build a projector over `client`, using `model` and `prompt_version`.
    pub fn new(
        client: Arc<dyn LlmClient>,
        model: impl Into<String>,
        prompt_version: impl Into<String>,
    ) -> Self {
        Self {
            client,
            model: model.into(),
            prompt_version: prompt_version.into(),
        }
    }

    /// The cache version tag this projector must be wrapped under:
    /// `{provider}:{model}:{prompt_version}:{prompt_fingerprint}`. The hand-set
    /// `prompt_version` is an explicit discriminator; the appended fingerprint tracks
    /// the prompt template and parser automatically, so editing the prompt invalidates
    /// the cache without any manual version bump.
    pub fn cache_version(&self) -> String {
        format!(
            "{}:{}",
            version_tag(
                self.client.provider_tag(),
                &self.model,
                &self.prompt_version
            ),
            prompt_fingerprint()
        )
    }
}

impl Step for LlmProjector {
    type In = MessageEnvelope;
    type Out = Projection;

    fn kind(&self) -> &'static str {
        "mirrorlane.projection.llm"
    }

    fn version(&self) -> StepVersion {
        StepVersion::new(self.cache_version())
    }

    fn run(&self, message: &MessageEnvelope) -> Projection {
        let prompt = projection_prompt(&message.body);
        let response = self.client.complete_json(&self.model, &prompt);
        parse_projection(&response, &message.id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mirrorlane_core::Projector;
    use mirrorlane_core::message::{Author, AuthorId, Conversation, ConversationId, Source};

    fn id() -> MessageId {
        MessageId("m-1".into())
    }

    fn message(body: &str) -> MessageEnvelope {
        MessageEnvelope {
            id: id(),
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
    fn well_formed_response_becomes_projection() {
        let body = r#"{"intent":"task","topics":["rust","auth"],"entities":["refresh-token"],"confidence":0.86}"#;
        let p = parse_projection(body, &id());
        assert_eq!(
            p.message_id,
            id(),
            "id comes from the envelope, not the model"
        );
        assert_eq!(p.intent, Intent::Task);
        assert_eq!(p.topics, vec![Topic("rust".into()), Topic("auth".into())]);
        assert_eq!(p.entities, vec![Entity("refresh-token".into())]);
        assert_eq!(p.confidence.get(), 0.86);
    }

    #[test]
    fn out_of_range_confidence_is_clamped() {
        let body = r#"{"intent":"social","topics":[],"entities":[],"confidence":1.7}"#;
        assert_eq!(parse_projection(body, &id()).confidence.get(), 1.0);
    }

    #[test]
    #[should_panic(expected = "non-conforming")]
    fn malformed_body_panics() {
        parse_projection("not json at all", &id());
    }

    #[test]
    #[should_panic(expected = "non-conforming")]
    fn unknown_intent_panics() {
        let body = r#"{"intent":"banana","topics":[],"entities":[],"confidence":0.5}"#;
        parse_projection(body, &id());
    }

    #[test]
    fn version_tag_encodes_provider_model_and_prompt() {
        assert_eq!(
            version_tag("ollama", "llama3.2", "v1"),
            "ollama:llama3.2:v1"
        );
        // provider, model, and prompt each change the tag
        assert_ne!(
            version_tag("ollama", "llama3.2", "v1"),
            version_tag("openai", "llama3.2", "v1")
        );
        assert_ne!(
            version_tag("ollama", "llama3.2", "v1"),
            version_tag("ollama", "mistral", "v1")
        );
        assert_ne!(
            version_tag("ollama", "llama3.2", "v1"),
            version_tag("ollama", "llama3.2", "v2")
        );
    }

    /// A stub client lets the projector be exercised end-to-end without a backend,
    /// proving the prompt+parse path is provider-agnostic.
    #[test]
    fn projector_over_a_stub_client_parses() {
        struct Stub;
        impl LlmClient for Stub {
            fn complete_json(&self, _model: &str, _prompt: &str) -> String {
                r#"{"intent":"decision","topics":["db"],"entities":[],"confidence":0.5}"#.into()
            }
            fn provider_tag(&self) -> &'static str {
                "stub"
            }
        }
        let projector = LlmProjector::new(Arc::new(Stub), "m", "v1");
        let p = projector.project(&message("anything"));
        assert_eq!(p.message_id, id());
        assert_eq!(p.intent, Intent::Decision);
        // The version starts with the explicit tag and appends the prompt fingerprint.
        assert!(
            projector
                .cache_version()
                .starts_with(&format!("stub:m:v1:{}", prompt_fingerprint())),
            "cache version is the tag plus the prompt fingerprint"
        );
    }

    #[test]
    fn prompt_fingerprint_is_stable_and_content_sensitive() {
        // Stable across calls (same template → same fingerprint), so cache keys match.
        assert_eq!(prompt_fingerprint(), prompt_fingerprint());
        // Different template text yields a different fingerprint — so editing the
        // prompt changes the version automatically, with no manual bump.
        assert_ne!(fingerprint("template A"), fingerprint("template B"));
    }

    #[test]
    fn version_appends_the_fingerprint() {
        struct Stub;
        impl LlmClient for Stub {
            fn complete_json(&self, _model: &str, _prompt: &str) -> String {
                String::new()
            }
            fn provider_tag(&self) -> &'static str {
                "stub"
            }
        }
        let projector = LlmProjector::new(Arc::new(Stub), "m", "v1");
        let v = projector.cache_version();
        assert_eq!(
            v.matches(':').count(),
            3,
            "provider:model:version:fingerprint"
        );
        assert!(v.ends_with(&prompt_fingerprint()));
    }
}
