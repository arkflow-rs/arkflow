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

use serde_json::Value;

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
    } else if let Some(spec) = inner.strip_prefix("file:") {
        resolve_file(spec, token, path)
    } else {
        // Unknown scheme: keep the reference text verbatim (forward
        // compatibility, e.g. a future `${secret:...}` scheme).
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
            resolve_string("${secret:name}", "a").unwrap(),
            "${secret:name}"
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
