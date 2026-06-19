//! An OpenAI-compatible [`LlmClient`].
//!
//! [`OpenAiClient`] is transport only: it calls the `/v1/chat/completions` endpoint
//! with JSON mode and returns the model's raw message content. Because the
//! chat-completions API is the de-facto standard, one client covers OpenAI itself
//! plus the many compatible servers (vLLM, LM Studio, Together, …) — pick the
//! backend with `--openai-base-url`. Projection prompt and parsing live in
//! [`mirrorlane_llm::LlmProjector`]; this crate carries only the HTTP transport and
//! panics at the boundary on failure, per the projector convention.

use serde::Deserialize;

use mirrorlane_llm::LlmClient;

/// Default OpenAI-compatible base URL (the path up to and including `/v1`).
pub const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";
/// Default model name.
pub const DEFAULT_MODEL: &str = "gpt-4o-mini";

/// An [`LlmClient`] over an OpenAI-compatible chat-completions endpoint.
pub struct OpenAiClient {
    base_url: String,
    api_key: Option<String>,
}

/// The chat-completions response envelope; the model's JSON is in
/// `choices[0].message.content`.
#[derive(Debug, Deserialize)]
struct ChatResponse {
    choices: Vec<Choice>,
}

#[derive(Debug, Deserialize)]
struct Choice {
    message: ChatMessage,
}

#[derive(Debug, Deserialize)]
struct ChatMessage {
    content: String,
}

impl OpenAiClient {
    /// Build a client against `base_url` with an optional API key. The key is sent
    /// as a `Bearer` token when present; it is omitted for keyless compatible
    /// servers (e.g. a local vLLM/LM Studio).
    pub fn new(base_url: impl Into<String>, api_key: Option<String>) -> Self {
        Self {
            base_url: base_url.into(),
            api_key,
        }
    }

    /// A client against the default endpoint, taking the key from `OPENAI_API_KEY`.
    pub fn from_env() -> Self {
        Self::new(DEFAULT_BASE_URL, std::env::var("OPENAI_API_KEY").ok())
    }
}

/// Extract the model's message content from a chat-completions response body.
/// Pure and I/O-free; panics if the body is not the expected shape or has no
/// choices, so a malformed response neither fabricates nor caches a projection.
fn parse_content(body: &str) -> String {
    let parsed: ChatResponse = serde_json::from_str(body)
        .unwrap_or_else(|e| panic!("openai response envelope was malformed: {e}"));
    parsed
        .choices
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("openai response had no choices"))
        .message
        .content
}

impl LlmClient for OpenAiClient {
    fn complete_json(&self, model: &str, prompt: &str) -> String {
        let url = format!("{}/chat/completions", self.base_url);
        let request_body = serde_json::json!({
            "model": model,
            "messages": [{ "role": "user", "content": prompt }],
            "response_format": { "type": "json_object" },
        });
        let mut request = ureq::post(&url).set("Content-Type", "application/json");
        if let Some(key) = &self.api_key {
            request = request.set("Authorization", &format!("Bearer {key}"));
        }
        let body = request
            .send_string(&request_body.to_string())
            .unwrap_or_else(|e| panic!("openai request failed: {e}"))
            .into_string()
            .unwrap_or_else(|e| panic!("openai response was not readable: {e}"));
        parse_content(&body)
    }

    fn provider_tag(&self) -> &'static str {
        "openai"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_content_from_a_chat_completion() {
        let body = r#"{
            "id": "chatcmpl-1",
            "choices": [
                { "index": 0, "message": { "role": "assistant", "content": "{\"intent\":\"task\"}" } }
            ]
        }"#;
        assert_eq!(parse_content(body), r#"{"intent":"task"}"#);
    }

    #[test]
    #[should_panic(expected = "no choices")]
    fn empty_choices_panics() {
        parse_content(r#"{"choices":[]}"#);
    }

    #[test]
    #[should_panic(expected = "malformed")]
    fn malformed_envelope_panics() {
        parse_content("not json");
    }

    #[test]
    fn provider_tag_is_openai() {
        assert_eq!(
            OpenAiClient::new(DEFAULT_BASE_URL, None).provider_tag(),
            "openai"
        );
    }

    /// Live path: requires network access and `OPENAI_API_KEY`. Run with
    /// `cargo test -p mirrorlane-openai -- --ignored`.
    #[test]
    #[ignore = "requires network access and OPENAI_API_KEY"]
    fn live_completion() {
        let client = OpenAiClient::from_env();
        let text = client.complete_json(
            DEFAULT_MODEL,
            "Respond with ONLY {\"intent\":\"social\",\"topics\":[],\"entities\":[],\"confidence\":1.0}",
        );
        assert!(text.contains("intent"));
    }
}
