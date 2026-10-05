/*
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

//! Hub-side secret pre-resolution for configuration dispatch.
//!
//! When the Hub builds an intent payload for rollout dispatch it resolves
//! `${secret:...}` references against the Hub process environment while
//! leaving `env:`/`file:` references node-local. The candidate envelope
//! handling lives here (next to its only caller, placement); the
//! secret-only reference walk itself is `arkflow_core::secret::resolve_secret_references`.

use arkflow_core::{secret, Error};
use serde_json::{json, Value};

/// Resolves only `${secret:...}` references inside a serialized
/// ConfigCandidate payload (`{"format": ..., "content": ...}`). Secrets live
/// in the Hub process environment, while `env:`/`file:` references stay
/// node-local and unknown schemes stay verbatim. Returns `None` when the
/// content contains no `secret:` reference (payload dispatched verbatim);
/// otherwise the content is re-serialized as JSON text with `format` set to
/// `json`.
pub(crate) fn resolve_candidate_payload(payload: String) -> Result<Option<String>, Error> {
    let mut candidate: Value = serde_json::from_str(&payload)
        .map_err(|e| Error::Config(format!("candidate payload parse failed: {}", e)))?;
    let format = candidate
        .get("format")
        .and_then(Value::as_str)
        .unwrap_or("json")
        .to_string();
    // Not a candidate envelope (no content field): dispatch verbatim.
    let Some(content) = candidate
        .get("content")
        .and_then(Value::as_str)
        .map(str::to_owned)
    else {
        return Ok(None);
    };
    if !content.contains("secret:") {
        return Ok(None);
    }

    let mut value: Value = match format.as_str() {
        "yaml" | "yml" => serde_yaml::from_str(&content)
            .map_err(|e| Error::Config(format!("candidate content parse failed: {}", e)))?,
        "json" => serde_json::from_str(&content)
            .map_err(|e| Error::Config(format!("candidate content parse failed: {}", e)))?,
        "toml" => toml::from_str(&content)
            .map_err(|e| Error::Config(format!("candidate content parse failed: {}", e)))?,
        other => {
            return Err(Error::Config(format!(
                "candidate payload has unknown format '{other}'"
            )))
        }
    };
    secret::resolve_secret_references(&mut value)?;

    candidate["content"] = json!(serde_json::to_string(&value).map_err(|e| {
        Error::Config(format!("candidate content serialization failed: {}", e))
    })?);
    // Carry the verbatim (pre-resolution) content alongside the resolved one:
    // the node persists THIS text as its config version, keeping the dispatch
    // path free of plaintext at rest. `serde` adds the field only when set.
    candidate["content_verbatim"] = json!(content);
    candidate["format"] = json!("json");
    let serialized = serde_json::to_string(&candidate)
        .map_err(|e| Error::Config(format!("candidate payload serialization failed: {}", e)))?;
    Ok(Some(serialized))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Unique per-test env var names: cargo runs tests in parallel threads
    /// sharing one process environment.
    fn set_env(name: &str, value: &str) {
        std::env::set_var(name, value);
    }

    fn clear_env(name: &str) {
        std::env::remove_var(name);
    }

    #[test]
    fn resolve_candidate_payload_resolves_only_secret_refs() {
        let payload = serde_json::json!({
            "format": "yaml",
            "content": "health_check:\n  api_token: ${secret:db_pass}\n  host: ${env:HUB_HOST}\n  legacy: $${env:OLD}\n"
        })
        .to_string();
        set_env("ARKFLOW_SECRET_db_pass", "s3cret");
        let resolved = resolve_candidate_payload(payload).unwrap().unwrap();
        clear_env("ARKFLOW_SECRET_db_pass");

        let candidate: Value = serde_json::from_str(&resolved).unwrap();
        assert_eq!(candidate["format"], "json");
        let content: Value = serde_json::from_str(candidate["content"].as_str().unwrap()).unwrap();
        assert_eq!(
            content["health_check"]["api_token"], "s3cret",
            "secret resolved at the hub"
        );
        assert_eq!(
            content["health_check"]["host"], "${env:HUB_HOST}",
            "env refs stay node-local"
        );
        assert_eq!(
            content["health_check"]["legacy"], "$${env:OLD}",
            "escapes stay literal"
        );
    }

    #[test]
    fn resolve_candidate_payload_returns_none_without_secret_refs() {
        let payload = serde_json::json!({
            "format": "yaml",
            "content": "health_check:\n  api_token: ${env:T}\n"
        })
        .to_string();
        assert!(resolve_candidate_payload(payload).unwrap().is_none());
    }

    #[test]
    fn resolve_candidate_payload_missing_secret_errors() {
        clear_env("ARKFLOW_SECRET_missing_x");
        let payload = serde_json::json!({
            "format": "yaml",
            "content": "token: ${secret:missing_x}\n"
        })
        .to_string();
        let err = resolve_candidate_payload(payload).unwrap_err().to_string();
        assert!(err.contains("ARKFLOW_SECRET_missing_x"), "{err}");
    }

    #[test]
    fn resolve_candidate_payload_handles_toml() {
        let payload = serde_json::json!({
            "format": "toml",
            "content": "token = \"${secret:t}\"\n"
        })
        .to_string();
        set_env("ARKFLOW_SECRET_t", "toml-secret");
        let resolved = resolve_candidate_payload(payload).unwrap().unwrap();
        clear_env("ARKFLOW_SECRET_t");
        let candidate: Value = serde_json::from_str(&resolved).unwrap();
        assert_eq!(candidate["format"], "json");
        let content: Value = serde_json::from_str(candidate["content"].as_str().unwrap()).unwrap();
        assert_eq!(content["token"], "toml-secret");
    }

    /// Hub dispatch resolves `${secret:...}` in place; the resolved text must
    /// not be re-scanned by the node-side resolver even when the secret's
    /// value itself looks like a reference.
    #[test]
    fn dispatched_secret_values_are_escaped_against_rescanning() {
        let name = "ARKFLOW_SECRET_INJECTED_REF";
        set_env(name, "${env:TOTALLY_UNSET_VAR}");

        let payload = serde_json::json!({
            "format": "yaml",
            "content": "health_check:\n  api_token: ${secret:INJECTED_REF}\n"
        })
        .to_string();
        let resolved = resolve_candidate_payload(payload)
            .expect("resolution succeeds")
            .expect("changed");
        let content: serde_json::Value = serde_json::from_str(&resolved).unwrap();
        let content = content["content"].as_str().unwrap();
        assert!(
            content.contains("$${env:TOTALLY_UNSET_VAR}"),
            "resolved value must be escaped: {content}"
        );

        // The node-side resolver turns the escape back into the literal.
        let mut tree: serde_json::Value = serde_yaml::from_str(content).unwrap();
        secret::resolve_value(&mut tree).unwrap();
        assert_eq!(
            tree["health_check"]["api_token"], "${env:TOTALLY_UNSET_VAR}",
            "the value must land as a literal, never re-expanded"
        );
    }

    /// The dispatch payload carries the verbatim (pre-resolution) content so
    /// nodes can persist references instead of plaintext.
    #[test]
    fn dispatch_payload_carries_verbatim_content() {
        let name = "ARKFLOW_SECRET_VERBATIM_PROBE";
        set_env(name, "plain-value");
        let payload = serde_json::json!({
            "format": "yaml",
            "content": "health_check:\n  api_token: ${secret:VERBATIM_PROBE}\n"
        })
        .to_string();
        let resolved = resolve_candidate_payload(payload)
            .expect("resolution succeeds")
            .expect("changed");
        let envelope: serde_json::Value = serde_json::from_str(&resolved).unwrap();
        assert_eq!(envelope["format"], "json");
        assert!(
            !envelope["content"]
                .as_str()
                .unwrap()
                .contains("${secret:VERBATIM_PROBE}"),
            "dispatched content must be resolved"
        );
        assert_eq!(
            envelope["content_verbatim"].as_str().unwrap(),
            "health_check:\n  api_token: ${secret:VERBATIM_PROBE}\n",
            "verbatim content must keep the reference"
        );
    }
}
