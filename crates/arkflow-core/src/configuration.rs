//! Control-plane configuration parsing, validation, and redaction helpers.

use crate::config::EngineConfig;
use crate::Error;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static VERSION_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Structured validation issue with a JSON/YAML-style path.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConfigIssue {
    pub path: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigValidationReport {
    pub valid: bool,
    pub errors: Vec<ConfigIssue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigVersion {
    pub id: String,
    pub created_at_ms: u64,
    pub format: ConfigFormat,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredConfigVersion {
    pub metadata: ConfigVersion,
    pub content: String,
}

/// Atomic file-backed configuration history for single-node deployments.
#[derive(Debug, Clone)]
pub struct ConfigVersionStore {
    root: PathBuf,
}

impl ConfigVersionStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn save(&self, candidate: &ConfigCandidate) -> Result<ConfigVersion, Error> {
        self.save_with_parent(candidate, None)
    }

    pub fn save_with_parent(
        &self,
        candidate: &ConfigCandidate,
        parent_id: Option<String>,
    ) -> Result<ConfigVersion, Error> {
        fs::create_dir_all(&self.root)?;
        let id = format!(
            "{}-{}",
            now_ms(),
            VERSION_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        );
        let metadata = ConfigVersion {
            id: id.clone(),
            created_at_ms: now_ms(),
            format: candidate.format,
            parent_id,
        };
        let stored = StoredConfigVersion {
            metadata: metadata.clone(),
            content: candidate.content.clone(),
        };
        let target = self.root.join(format!("{id}.json"));
        let temporary = self.root.join(format!(".{id}.tmp"));
        fs::write(&temporary, serde_json::to_vec_pretty(&stored)?)?;
        fs::rename(&temporary, &target)?;
        Ok(metadata)
    }

    pub fn list(&self) -> Result<Vec<ConfigVersion>, Error> {
        if !self.root.exists() {
            return Ok(Vec::new());
        }
        let mut versions = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let path = entry?.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
                continue;
            }
            let stored: StoredConfigVersion = serde_json::from_slice(&fs::read(path)?)?;
            versions.push(stored.metadata);
        }
        versions.sort_by_key(|version| std::cmp::Reverse(version.created_at_ms));
        Ok(versions)
    }

    pub fn load(&self, id: &str) -> Result<ConfigCandidate, Error> {
        let path = self.root.join(format!("{id}.json"));
        let stored: StoredConfigVersion = serde_json::from_slice(&fs::read(path)?)?;
        Ok(ConfigCandidate {
            format: stored.metadata.format,
            content: stored.content,
        })
    }
}

/// Request body accepted by the configuration validation endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigCandidate {
    pub format: ConfigFormat,
    pub content: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConfigFormat {
    Yaml,
    Json,
    Toml,
}

impl ConfigCandidate {
    pub fn parse(&self) -> Result<EngineConfig, ConfigIssue> {
        match self.format {
            ConfigFormat::Yaml => serde_yaml::from_str(&self.content).map_err(|error| {
                let path = error
                    .location()
                    .map(|location| {
                        format!("line {}, column {}", location.line(), location.column())
                    })
                    .unwrap_or_else(|| "document".to_string());
                parse_error_at(path, error)
            }),
            ConfigFormat::Json => serde_json::from_str(&self.content).map_err(|error| {
                parse_error_at(
                    format!("line {}, column {}", error.line(), error.column()),
                    error,
                )
            }),
            ConfigFormat::Toml => toml::from_str(&self.content).map_err(|error| {
                let path = error
                    .span()
                    .map(|span| location_for_offset(&self.content, span.start))
                    .unwrap_or_else(|| "document".to_string());
                parse_error_at(path, error)
            }),
        }
    }
}

fn parse_error_at(path: String, error: impl std::fmt::Display) -> ConfigIssue {
    ConfigIssue {
        path,
        message: error.to_string(),
    }
}

fn location_for_offset(content: &str, offset: usize) -> String {
    let prefix = &content[..offset.min(content.len())];
    let line = prefix.bytes().filter(|byte| *byte == b'\n').count() + 1;
    let column = prefix
        .rsplit('\n')
        .next()
        .map_or(0, |line| line.chars().count())
        + 1;
    format!("line {line}, column {column}")
}

/// Validate syntax, Stream identities, and component construction without
/// starting any Stream task.
pub fn validate_config(config: &EngineConfig) -> ConfigValidationReport {
    let mut errors = Vec::new();
    if let Err(error) = config.stream_ids() {
        errors.push(ConfigIssue {
            path: "streams".to_string(),
            message: error.to_string(),
        });
    }

    for (index, stream) in config.streams.iter().enumerate() {
        if let Err(error) = stream.build() {
            errors.push(ConfigIssue {
                path: format!("streams[{index}]"),
                message: error.to_string(),
            });
        }
    }

    ConfigValidationReport {
        valid: errors.is_empty(),
        errors,
    }
}

/// Parse and validate a candidate in one operation.
pub fn parse_and_validate(
    candidate: &ConfigCandidate,
) -> Result<ConfigValidationReport, ConfigIssue> {
    let config = candidate.parse()?;
    Ok(validate_config(&config))
}

/// Return a copy of a JSON value with credential-like fields redacted.
pub fn redact_secrets(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut redacted = Map::new();
            for (key, child) in object {
                if is_secret_key(key) {
                    redacted.insert(key.clone(), Value::String("******".to_string()));
                } else {
                    redacted.insert(key.clone(), redact_secrets(child));
                }
            }
            Value::Object(redacted)
        }
        Value::Array(values) => Value::Array(values.iter().map(redact_secrets).collect()),
        other => other.clone(),
    }
}

fn is_secret_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    [
        "password",
        "passwd",
        "token",
        "secret",
        "credential",
        "api_key",
    ]
    .iter()
    .any(|part| key.contains(part))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

/// Convert an engine configuration to a redacted JSON representation.
pub fn redacted_config(config: &EngineConfig) -> Result<Value, Error> {
    let value = serde_json::to_value(config)?;
    Ok(redact_secrets(&value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidate_parses_json_and_reports_syntax_errors() {
        let valid = ConfigCandidate {
            format: ConfigFormat::Json,
            content: r#"{"streams":[]}"#.to_string(),
        };
        assert!(valid.parse().is_ok());

        let invalid = ConfigCandidate {
            format: ConfigFormat::Json,
            content: "not-json".to_string(),
        };
        assert!(invalid
            .parse()
            .unwrap_err()
            .path
            .starts_with("line 1, column"));
    }

    #[test]
    fn redaction_is_recursive_and_preserves_non_secret_values() {
        let value = serde_json::json!({
            "username": "operator",
            "password": "hidden",
            "auth": {"token": "also-hidden"},
            "nested": [{"api_key": "key", "enabled": true}]
        });
        let redacted = redact_secrets(&value);
        assert_eq!(redacted["username"], "operator");
        assert_eq!(redacted["password"], "******");
        assert_eq!(redacted["auth"]["token"], "******");
        assert_eq!(redacted["nested"][0]["enabled"], true);
        assert_eq!(redacted["nested"][0]["api_key"], "******");
    }

    #[test]
    fn version_store_writes_atomically_and_lists_versions() {
        let root = std::env::temp_dir().join(format!(
            "arkflow-control-config-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let store = ConfigVersionStore::new(&root);
        let candidate = ConfigCandidate {
            format: ConfigFormat::Json,
            content: r#"{"streams":[]}"#.to_string(),
        };
        let version = store.save(&candidate).unwrap();
        assert_eq!(store.list().unwrap().len(), 1);
        assert_eq!(store.load(&version.id).unwrap().content, candidate.content);
        let _ = fs::remove_dir_all(root);
    }
}
