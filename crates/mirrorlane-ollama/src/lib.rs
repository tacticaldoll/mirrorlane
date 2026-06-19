//! An Ollama-backed [`LlmClient`].
//!
//! [`OllamaClient`] is transport only: it calls a local Ollama server's
//! `/api/generate` with `format=json` and returns the model's raw response text.
//! Projection construction (prompt, parse, id, clamp) lives in
//! [`mirrorlane_llm::LlmProjector`], so this crate carries just the Ollama HTTP
//! dependency. Like the projector convention, failures **panic at the boundary**
//! rather than fabricating a response.

use serde::Deserialize;

use mirrorlane_llm::LlmClient;

/// Default local Ollama endpoint.
pub const DEFAULT_BASE_URL: &str = "http://localhost:11434";
/// Default model name.
pub const DEFAULT_MODEL: &str = "qwen2.5:3b-instruct";
/// Default prompt version — re-exported from `mirrorlane-llm`, where the shared
/// projection prompt lives, so existing references keep resolving.
pub use mirrorlane_llm::DEFAULT_PROMPT_VERSION;

/// An [`LlmClient`] backed by a local Ollama server.
pub struct OllamaClient {
    base_url: String,
}

/// Ollama's non-streamed `/api/generate` response envelope; `response` holds the
/// model's JSON string.
#[derive(Debug, Deserialize)]
struct GenerateResponse {
    response: String,
}

impl OllamaClient {
    /// Build a client against `base_url`.
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
        }
    }

    /// A client against the default local endpoint.
    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_BASE_URL)
    }
}

impl LlmClient for OllamaClient {
    fn complete_json(&self, model: &str, prompt: &str) -> String {
        let url = format!("{}/api/generate", self.base_url);
        let request_body = serde_json::json!({
            "model": model,
            "prompt": prompt,
            "format": "json",
            "stream": false,
        });
        let response = ureq::post(&url)
            .set("Content-Type", "application/json")
            .send_string(&request_body.to_string())
            .unwrap_or_else(|e| panic!("ollama request failed: {e}"))
            .into_string()
            .unwrap_or_else(|e| panic!("ollama response was not readable: {e}"));
        let envelope: GenerateResponse = serde_json::from_str(&response)
            .unwrap_or_else(|e| panic!("ollama response envelope was malformed: {e}"));
        envelope.response
    }

    fn provider_tag(&self) -> &'static str {
        "ollama"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mirrorlane_core::Projector;
    use mirrorlane_core::message::{
        Author, AuthorId, Conversation, ConversationId, MessageEnvelope, MessageId, Source,
    };
    use mirrorlane_llm::LlmProjector;
    use std::sync::Arc;

    fn message(body: &str) -> MessageEnvelope {
        MessageEnvelope {
            id: MessageId("m-1".into()),
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
    fn ollama_projector_version_tag_is_prefixed_by_the_provider_tag() {
        // The Ollama tag still leads with `ollama:{model}:{prompt}`; a content
        // fingerprint of the prompt template is appended so a prompt edit invalidates
        // the cache automatically (the tag is no longer the sole guard).
        let projector = LlmProjector::new(Arc::new(OllamaClient::with_defaults()), "m", "v2");
        assert!(projector.cache_version().starts_with("ollama:m:v2:"));
    }

    /// Live path: requires a running Ollama server. Honors `OLLAMA_BASE_URL`,
    /// `OLLAMA_MODEL`, and `OLLAMA_PROMPT_VERSION`. Run with
    /// `cargo test -p mirrorlane-ollama -- --ignored`.
    #[test]
    #[ignore = "requires a local Ollama server"]
    fn live_projection_against_local_ollama() {
        let base_url =
            std::env::var("OLLAMA_BASE_URL").unwrap_or_else(|_| DEFAULT_BASE_URL.to_string());
        let model = std::env::var("OLLAMA_MODEL").unwrap_or_else(|_| DEFAULT_MODEL.to_string());
        let prompt_version = std::env::var("OLLAMA_PROMPT_VERSION")
            .unwrap_or_else(|_| DEFAULT_PROMPT_VERSION.to_string());
        let projector =
            LlmProjector::new(Arc::new(OllamaClient::new(base_url)), model, prompt_version);
        let projection = projector.project(&message(
            "Should we use sqlite for the auth sdk refresh-token store?",
        ));
        assert_eq!(projection.message_id, MessageId("m-1".into()));
        assert!((0.0..=1.0).contains(&projection.confidence.get()));
    }
}
