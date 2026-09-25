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

//! Secret reference resolution for configuration values.
//!
//! String values in a configuration document may reference external secrets
//! instead of inlining them: `${env:VAR}`, `${env:VAR:-default}` and
//! `${file:/path}`. References are resolved once, when the document is
//! materialized into [`crate::config::EngineConfig`]; resolved values are
//! never rescanned and never appear in error messages. `$${` escapes a
//! literal `${`, and `${...}` forms with unknown schemes are left untouched.

use serde_json::{json, Value};

use crate::Error;

/// A configuration document awaiting secret resolution, in one of the three
/// supported formats.
pub(crate) enum ConfigDocument<'a> {
    Yaml(&'a str),
    Json(&'a str),
    Toml(&'a str),
}

/// True when the text contains a secret-reference marker (`${`, which also
/// covers the `$${` escape).
pub fn contains_reference(text: &str) -> bool {
    text.contains("${")
}

/// Parse a document into a JSON value tree and resolve every secret
/// reference in it. Parsing keeps the per-format error messages of the
/// direct deserialization path (serde embeds line/column locations).
pub(crate) fn resolve_document(document: ConfigDocument<'_>) -> Result<Value, Error> {
    let mut value = match document {
        ConfigDocument::Yaml(content) => {
            let value: serde_yaml::Value = serde_yaml::from_str(content)
                .map_err(|e| Error::Config(format!("YAML parsing error: {}", e)))?;
            serde_json::to_value(value)
                .map_err(|e| Error::Config(format!("YAML conversion error: {}", e)))?
        }
        ConfigDocument::Json(content) => serde_json::from_str(content)
            .map_err(|e| Error::Config(format!("JSON parsing error: {}", e)))?,
        ConfigDocument::Toml(content) => {
            let value: toml::Value = toml::from_str(content)
                .map_err(|e| Error::Config(format!("TOML parsing error: {}", e)))?;
            serde_json::to_value(value)
                .map_err(|e| Error::Config(format!("TOML conversion error: {}", e)))?
        }
    };
    resolve_value(&mut value)?;
    Ok(value)
}

/// Recursively resolve secret references in every string value of the tree.
/// Keys, numbers, booleans and nulls are left untouched.
pub fn resolve_value(value: &mut Value) -> Result<(), Error> {
    resolve_at(value, "")
}

/// Resolves only `${secret:...}` references inside a serialized
/// ConfigCandidate payload (`{"format": ..., "content": ...}`). Used by the
/// Hub when dispatching configurations: secrets live in the Hub process
/// environment, while `env:`/`file:` references stay node-local and unknown
/// schemes stay verbatim. Returns `None` when the content contains no
/// `secret:` reference (payload dispatched verbatim); otherwise the content
/// is re-serialized as JSON text with `format` set to `json`.
pub fn resolve_candidate_payload(payload: String) -> Result<Option<String>, Error> {
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
    resolve_secret_only_at(&mut value, "")?;

    candidate["content"] = json!(serde_json::to_string(&value).map_err(|e| {
        Error::Config(format!("candidate content serialization failed: {}", e))
    })?);
    // Carry the verbatim (pre-resolution) content alongside the resolved one:
    // the node persists THIS text as its config version, keeping the dispatch
    // path free of plaintext at rest. `serde` adds the field only when set.
    candidate["content_verbatim"] = json!(content);
    candidate["format"] = json!("json");
    let serialized = serde_json::to_string(&candidate).map_err(|e| {
        Error::Config(format!("candidate payload serialization failed: {}", e))
    })?;
    Ok(Some(serialized))
}

/// Walks a value tree resolving only `secret:` references; every other
/// reference form stays literal for the node-local resolver.
fn resolve_secret_only_at(value: &mut Value, path: &str) -> Result<(), Error> {
    match value {
        Value::String(text) => {
            *text = resolve_secret_only_string(text, path)?;
        }
        Value::Array(items) => {
            for (index, item) in items.iter_mut().enumerate() {
                resolve_secret_only_at(item, &format!("{path}[{index}]"))?;
            }
        }
        Value::Object(map) => {
            for (key, item) in map.iter_mut() {
                let child = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                resolve_secret_only_at(item, &child)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn resolve_secret_only_string(text: &str, path: &str) -> Result<String, Error> {
    let mut output = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(position) = rest.find('$') {
        output.push_str(&rest[..position]);
        rest = &rest[position..];
        if let Some(after) = rest.strip_prefix("$${") {
            // Preserve the escape: the node-side resolver turns it into a
            // literal after this pass.
            output.push_str("$${");
            rest = after;
        } else if let Some(after) = rest.strip_prefix("${") {
            match after.find('}') {
                Some(end) => {
                    let inner = &after[..end];
                    let token = format!("${{{}}}", inner);
                    if let Some(spec) = inner.strip_prefix("secret:") {
                        // Same namespace convention as the full resolver.
                        let name = format!("ARKFLOW_SECRET_{spec}");
                        let resolved = resolve_env(&name, &token, path)?;
                        // The resolved value must never be re-scanned by the
                        // node-side resolver (the dispatch payload is re-parsed
                        // as a document there): escape `${` so a secret whose
                        // text looks like a reference stays literal.
                        output.push_str(&resolved.replace("${", "$${"));
                    } else {
                        // env:/file:/unknown stay literal for the node.
                        output.push_str(&token);
                    }
                    rest = &after[end + 1..];
                }
                None => {
                    output.push_str(rest);
                    rest = "";
                }
            }
        } else {
            output.push('$');
            rest = &rest[1..];
        }
    }
    output.push_str(rest);
    Ok(output)
}

fn resolve_at(value: &mut Value, path: &str) -> Result<(), Error> {
    match value {
        Value::String(text) => {
            if contains_reference(text) {
                *text = resolve_string(text, path)?;
            }
        }
        Value::Array(items) => {
            for (index, item) in items.iter_mut().enumerate() {
                let child = format!("{path}[{index}]");
                resolve_at(item, &child)?;
            }
        }
        Value::Object(map) => {
            for (key, item) in map.iter_mut() {
                let child = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                resolve_at(item, &child)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Resolve all references in one string with a single left-to-right scan.
/// Resolved substitutions are pushed to the output verbatim — the scanner
/// never re-examines them, so secret content containing `${...}` stays
/// literal.
fn resolve_string(text: &str, path: &str) -> Result<String, Error> {
    let mut output = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(position) = rest.find('$') {
        output.push_str(&rest[..position]);
        rest = &rest[position..];
        if let Some(after) = rest.strip_prefix("$${") {
            output.push_str("${");
            rest = after;
        } else if let Some(after) = rest.strip_prefix("${") {
            match after.find('}') {
                Some(end) => {
                    let inner = &after[..end];
                    let token = format!("${{{}}}", inner);
                    output.push_str(&resolve_reference(inner, &token, path)?);
                    rest = &after[end + 1..];
                }
                None => {
                    // Unterminated reference: literal remainder.
                    output.push_str(rest);
                    rest = "";
                }
            }
        } else {
            output.push('$');
            rest = &rest[1..];
        }
    }
    output.push_str(rest);
    Ok(output)
}

fn resolve_reference(inner: &str, token: &str, path: &str) -> Result<String, Error> {
    if let Some(spec) = inner.strip_prefix("env:") {
        resolve_env(spec, token, path)
    } else if let Some(spec) = inner.strip_prefix("secret:") {
        // Namespace convention: secret:NAME reads ARKFLOW_SECRET_<NAME>,
        // keeping credentials in a dedicated, auditable prefix.
        let spec = format!("ARKFLOW_SECRET_{spec}");
        resolve_env(&spec, token, path)
    } else if let Some(spec) = inner.strip_prefix("file:") {
        resolve_file(spec, token, path)
    } else {
        // Unknown scheme: keep the reference text verbatim (forward
        // compatibility, e.g. a future `${vault:...}` scheme).
        Ok(token.to_string())
    }
}

fn resolve_env(spec: &str, reference: &str, path: &str) -> Result<String, Error> {
    let (name, default) = match spec.find(":-") {
        Some(position) => (&spec[..position], Some(&spec[position + 2..])),
        None => (spec, None),
    };
    if name.is_empty() {
        return Err(secret_error(
            path,
            reference,
            "empty environment variable name".to_string(),
        ));
    }
    match std::env::var(name) {
        Ok(value) if !value.is_empty() => Ok(value),
        Ok(_) => match default {
            Some(value) => Ok(value.to_string()),
            None => Ok(String::new()),
        },
        Err(std::env::VarError::NotPresent) => match default {
            Some(value) => Ok(value.to_string()),
            None => Err(secret_error(
                path,
                reference,
                format!("environment variable '{name}' is not set"),
            )),
        },
        Err(_) => Err(secret_error(
            path,
            reference,
            format!("environment variable '{name}' is not valid Unicode"),
        )),
    }
}

fn resolve_file(spec: &str, reference: &str, path: &str) -> Result<String, Error> {
    if spec.is_empty() {
        return Err(secret_error(
            path,
            reference,
            "empty file path".to_string(),
        ));
    }
    let content = std::fs::read_to_string(spec).map_err(|e| {
        secret_error(
            path,
            reference,
            format!("unable to read file '{spec}': {}", e.kind()),
        )
    })?;
    Ok(content.trim_end_matches(['\n', '\r']).to_string())
}

fn secret_error(path: &str, reference: &str, reason: String) -> Error {
    Error::Config(format!(
        "Failed to resolve secret reference at {path}: {reason} (reference: {reference})"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
        let resolved =
            resolve_candidate_payload(payload).expect("resolution succeeds").expect("changed");
        let content: serde_json::Value = serde_json::from_str(&resolved).unwrap();
        let content = content["content"].as_str().unwrap();
        assert!(
            content.contains("$${env:TOTALLY_UNSET_VAR}"),
            "resolved value must be escaped: {content}"
        );

        // The node-side resolver turns the escape back into the literal.
        let mut tree: serde_json::Value = serde_yaml::from_str(content).unwrap();
        resolve_value(&mut tree).unwrap();
        assert_eq!(
            tree["health_check"]["api_token"],
            "${env:TOTALLY_UNSET_VAR}",
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
        let resolved =
            resolve_candidate_payload(payload).expect("resolution succeeds").expect("changed");
        let envelope: serde_json::Value = serde_json::from_str(&resolved).unwrap();
        assert_eq!(envelope["format"], "json");
        assert!(
            !envelope["content"].as_str().unwrap().contains("${secret:VERBATIM_PROBE}"),
            "dispatched content must be resolved"
        );
        assert_eq!(
            envelope["content_verbatim"].as_str().unwrap(),
            "health_check:\n  api_token: ${secret:VERBATIM_PROBE}\n",
            "verbatim content must keep the reference"
        );
    }


    /// Unique per-test env var names: cargo runs tests in parallel threads
    /// sharing one process environment.
    fn set_env(name: &str, value: &str) {
        std::env::set_var(name, value);
    }

    fn clear_env(name: &str) {
        std::env::remove_var(name);
    }

    #[test]
    fn contains_reference_detects_marker() {
        assert!(contains_reference("plain ${env:VAR}"));
        assert!(contains_reference("escaped $${"));
        assert!(!contains_reference("no marker here"));
        assert!(!contains_reference(""));
    }

    #[test]
    fn env_reference_resolves() {
        set_env("ARKFLOW_SECRET_TEST_PW", "s3cret");
        let resolved = resolve_string("prefix-${env:ARKFLOW_SECRET_TEST_PW}-suffix", "").unwrap();
        clear_env("ARKFLOW_SECRET_TEST_PW");
        assert_eq!(resolved, "prefix-s3cret-suffix");
    }

    #[test]
    fn env_default_applies_when_unset_or_empty() {
        clear_env("ARKFLOW_SECRET_TEST_MISSING");
        assert_eq!(
            resolve_string("${env:ARKFLOW_SECRET_TEST_MISSING:-fallback}", "").unwrap(),
            "fallback"
        );
        set_env("ARKFLOW_SECRET_TEST_EMPTY", "");
        assert_eq!(
            resolve_string("${env:ARKFLOW_SECRET_TEST_EMPTY:-fallback}", "").unwrap(),
            "fallback"
        );
        assert_eq!(
            resolve_string("${env:ARKFLOW_SECRET_TEST_EMPTY:-}", "").unwrap(),
            ""
        );
        clear_env("ARKFLOW_SECRET_TEST_EMPTY");
        // Set and non-empty wins over the default.
        set_env("ARKFLOW_SECRET_TEST_SET", "real");
        assert_eq!(
            resolve_string("${env:ARKFLOW_SECRET_TEST_SET:-fallback}", "").unwrap(),
            "real"
        );
        clear_env("ARKFLOW_SECRET_TEST_SET");
    }

    #[test]
    fn env_default_may_contain_colons() {
        clear_env("ARKFLOW_SECRET_TEST_MISSING");
        assert_eq!(
            resolve_string("${env:ARKFLOW_SECRET_TEST_MISSING:-a:-b}", "").unwrap(),
            "a:-b"
        );
    }

    #[test]
    fn env_unset_without_default_errors_with_reference_not_value() {
        clear_env("ARKFLOW_SECRET_TEST_MISSING");
        let err = resolve_string("${env:ARKFLOW_SECRET_TEST_MISSING}", "streams[0].password")
            .unwrap_err();
        let message = err.to_string();
        assert!(message.contains("streams[0].password"), "{message}");
        assert!(
            message.contains("${env:ARKFLOW_SECRET_TEST_MISSING}"),
            "{message}"
        );
        assert!(message.contains("is not set"), "{message}");
    }

    #[test]
    fn env_empty_name_errors() {
        let err = resolve_string("${env:}", "a.b").unwrap_err();
        assert!(err.to_string().contains("empty environment variable name"));
    }

    #[test]
    fn env_not_unicode_errors_without_value() {
        // A var name with NUL is rejected by std; the error must not panic.
        let err = resolve_string("${env:BAD\0NAME}", "a").unwrap_err();
        assert!(err.to_string().contains("BAD"));
    }

    #[test]
    fn file_reference_resolves_and_trims_trailing_newlines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secret.txt");
        std::fs::write(&path, b"s3cret\n\n").unwrap();
        let reference = format!("${{file:{}}}", path.display());
        assert_eq!(resolve_string(&reference, "pw").unwrap(), "s3cret");
    }

    #[test]
    fn file_pem_body_is_preserved_verbatim() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key.pem");
        std::fs::write(&path, "-----BEGIN KEY-----\nabc\ndef\n-----END KEY-----\n").unwrap();
        let reference = format!("${{file:{}}}", path.display());
        let resolved = resolve_string(&reference, "tls.key").unwrap();
        assert_eq!(
            resolved,
            "-----BEGIN KEY-----\nabc\ndef\n-----END KEY-----"
        );
    }

    #[test]
    fn file_missing_errors_with_kind_not_content() {
        let err =
            resolve_string("${file:/nonexistent/arkflow/nope.pem}", "tls.ca").unwrap_err();
        let message = err.to_string();
        assert!(message.contains("/nonexistent/arkflow/nope.pem"), "{message}");
        assert!(message.contains("entity not found") || message.contains("NotFound"), "{message}");
    }

    #[test]
    fn file_error_does_not_leak_content() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secret.txt");
        std::fs::write(&path, b"topsecret").unwrap();
        // Make the file unreadable to trigger the error path with content on disk.
        let mut perms = std::fs::metadata(&path).unwrap().permissions();
        use std::os::unix::fs::PermissionsExt;
        perms.set_mode(0o000);
        std::fs::set_permissions(&path, perms).unwrap();
        let reference = format!("${{file:{}}}", path.display());
        let result = resolve_string(&reference, "pw");
        // Root may still read the file; only assert on the error branch.
        if let Err(err) = result {
            assert!(!err.to_string().contains("topsecret"), "{}", err);
        }
        // Restore permissions so tempdir cleanup works.
        let mut perms = std::fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o644);
        std::fs::set_permissions(&path, perms).unwrap();
    }

    #[test]
    fn empty_file_path_errors() {
        let err = resolve_string("${file:}", "a").unwrap_err();
        assert!(err.to_string().contains("empty file path"));
    }

    #[tokio::test]
    async fn secret_scheme_resolves_from_arkflow_secret_prefix() {
        set_env("ARKFLOW_SECRET_db_password", "hunter2");
        set_env("ARKFLOW_SECRET_api_key", "sk-live");
        let resolved = resolve_string(
            "pw=${secret:db_password}; key=${secret:api_key:-none}",
            "health.api_token",
        )
        .unwrap();
        clear_env("ARKFLOW_SECRET_db_password");
        clear_env("ARKFLOW_SECRET_api_key");
        assert_eq!(resolved, "pw=hunter2; key=sk-live");
    }

    #[tokio::test]
    async fn secret_scheme_default_and_missing_paths() {
        clear_env("ARKFLOW_SECRET_missing");
        set_env("ARKFLOW_SECRET_empty", "");
        assert_eq!(
            resolve_string("${secret:missing:-fallback}", "a").unwrap(),
            "fallback"
        );
        assert_eq!(resolve_string("${secret:empty:-fb}", "a").unwrap(), "fb");
        let err = resolve_string("${secret:missing}", "streams[0].pw").unwrap_err();
        let message = err.to_string();
        assert!(message.contains("ARKFLOW_SECRET_missing"), "{message}");
        assert!(message.contains("streams[0].pw"), "{message}");
        clear_env("ARKFLOW_SECRET_empty");
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

    #[test]
    fn escape_yields_literal_reference() {
        assert_eq!(
            resolve_string("$${env:NOT_A_REF}", "").unwrap(),
            "${env:NOT_A_REF}"
        );
        assert_eq!(resolve_string("a $${ b", "").unwrap(), "a ${ b");
        assert_eq!(resolve_string("$$$", "").unwrap(), "$$$");
    }

    #[test]
    fn unknown_scheme_stays_verbatim() {
        assert_eq!(
            resolve_string("${vault:kv/foo}", "a").unwrap(),
            "${vault:kv/foo}"
        );
        assert_eq!(
            resolve_string("${kms:arn:key}", "a").unwrap(),
            "${kms:arn:key}"
        );
        // Only known schemes resolve; a typo stays literal too.
        assert_eq!(
            resolve_string("${envs:VAR}", "a").unwrap(),
            "${envs:VAR}"
        );
    }

    #[test]
    fn unterminated_reference_stays_literal() {
        assert_eq!(resolve_string("value ${env:OPEN", "").unwrap(), "value ${env:OPEN");
    }

    #[test]
    fn resolved_values_are_not_rescanned() {
        set_env("ARKFLOW_SECRET_TEST_INJECT", "${env:OTHER}");
        let resolved = resolve_string("${env:ARKFLOW_SECRET_TEST_INJECT}", "").unwrap();
        clear_env("ARKFLOW_SECRET_TEST_INJECT");
        assert_eq!(resolved, "${env:OTHER}");
    }

    #[test]
    fn multiple_references_in_one_string() {
        set_env("ARKFLOW_SECRET_TEST_H", "host1");
        set_env("ARKFLOW_SECRET_TEST_P", "9092");
        let resolved = resolve_string(
            "host=${env:ARKFLOW_SECRET_TEST_H};port=${env:ARKFLOW_SECRET_TEST_P}",
            "",
        )
        .unwrap();
        clear_env("ARKFLOW_SECRET_TEST_H");
        clear_env("ARKFLOW_SECRET_TEST_P");
        assert_eq!(resolved, "host=host1;port=9092");
    }

    #[test]
    fn resolve_value_walks_nested_maps_arrays_and_skips_non_strings() {
        set_env("ARKFLOW_SECRET_TEST_V", "v1");
        let mut value = json!({
            "streams": [{
                "name": 42,
                "flag": true,
                "none": null,
                "nested": { "pw": "${env:ARKFLOW_SECRET_TEST_V}", "lit": "${vault:x}" }
            }],
            "list": ["${env:ARKFLOW_SECRET_TEST_V}", "plain"]
        });
        resolve_value(&mut value).unwrap();
        clear_env("ARKFLOW_SECRET_TEST_V");
        assert_eq!(value["streams"][0]["nested"]["pw"], "v1");
        assert_eq!(value["streams"][0]["nested"]["lit"], "${vault:x}");
        assert_eq!(value["streams"][0]["name"], 42);
        assert_eq!(value["streams"][0]["flag"], true);
        assert_eq!(value["list"][0], "v1");
        assert_eq!(value["list"][1], "plain");
    }

    #[test]
    fn resolve_value_error_carries_json_path() {
        let mut value = json!({ "a": { "b": ["${env:ARKFLOW_SECRET_TEST_MISSING_X}"] } });
        clear_env("ARKFLOW_SECRET_TEST_MISSING_X");
        let err = resolve_value(&mut value).unwrap_err();
        assert!(err.to_string().contains("a.b[0]"), "{}", err);
    }

    #[test]
    fn resolve_document_yaml_json_toml() {
        set_env("ARKFLOW_SECRET_TEST_DOC", "resolved");
        let yaml = resolve_document(ConfigDocument::Yaml("value: ${env:ARKFLOW_SECRET_TEST_DOC}"))
            .unwrap();
        assert_eq!(yaml["value"], "resolved");
        let json_doc =
            resolve_document(ConfigDocument::Json(r#"{"value": "${env:ARKFLOW_SECRET_TEST_DOC}"}"#))
                .unwrap();
        assert_eq!(json_doc["value"], "resolved");
        let toml_doc = resolve_document(ConfigDocument::Toml(
            "value = \"${env:ARKFLOW_SECRET_TEST_DOC}\"",
        ))
        .unwrap();
        assert_eq!(toml_doc["value"], "resolved");
        clear_env("ARKFLOW_SECRET_TEST_DOC");
    }
}
