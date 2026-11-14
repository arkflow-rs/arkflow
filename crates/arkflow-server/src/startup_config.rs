//! Startup configuration file support for the standalone `arkflow-server`
//! Hub binary (`--config <FILE>`).
//!
//! A [`HubStartupConfig`] carries the full startup surface: every
//! [`ServerConfig`] field (flattened at the top level, including `logging`
//! and `observability`), a `hub` section with `operator_token`, and an `ha`
//! section. Loading resolves secret references
/// (`${env:VAR}`/`${file:PATH}`/`${secret:NAME}`) through the same core
/// resolver the engine config uses, rejects unknown top-level keys (serde
/// `flatten` cannot `deny_unknown_fields`, so the key set is checked
/// against the serialized default), and — after secret resolution — never
/// echoes resolved values in type errors.
use crate::api::ServerConfig;
use crate::hub::HubHaConfig;
use arkflow_core::config::ConfigFormat;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;

/// The full `arkflow-server` startup configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HubStartupConfig {
    /// Every `ServerConfig` field at the top level of the file.
    #[serde(flatten)]
    pub server: ServerConfig,
    /// Hub-specific section (fields not already part of `ServerConfig`).
    #[serde(default)]
    pub hub: HubFileConfig,
    /// HA lease-election configuration.
    #[serde(default)]
    pub ha: HubHaConfig,
}

/// Hub-specific startup fields.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HubFileConfig {
    /// Operator token for human/admin API access. Supports the same
    /// structured `role|token` credential format as `ARKFLOW_OPERATOR_TOKEN`.
    #[serde(default)]
    pub operator_token: Option<String>,
}

/// A startup configuration failure; the message is user-facing and printed
/// to stderr before any subscriber exists.
#[derive(Debug)]
pub struct StartupConfigError(pub String);

impl std::fmt::Display for StartupConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for StartupConfigError {}

/// Loads and parses the startup configuration file (YAML/JSON/TOML by
/// extension), resolving secret references.
pub fn load_startup_config(path: &str) -> Result<HubStartupConfig, StartupConfigError> {
    let content = std::fs::read_to_string(path).map_err(|error| {
        StartupConfigError(format!("Failed to read config file '{path}': {error}"))
    })?;
    let format = format_from_path(path).ok_or_else(|| {
        StartupConfigError(format!(
            "Unsupported config file extension for '{path}' (expected .yaml, .yml, .json, or .toml)"
        ))
    })?;
    let value = parse_document_value(&content, format)?;
    check_unknown_keys(&value)?;
    // After secret resolution a type error would embed resolved values in
    // serde's message; use a fixed, value-independent message like the
    // engine loader (`parse_engine_config`) does. Without references the
    // detailed serde error is safe to surface.
    let had_refs = content.contains("${");
    serde_json::from_value(value).map_err(|error| {
        if had_refs {
            StartupConfigError(
                "Configuration error: validation failed after secret resolution".to_string(),
            )
        } else {
            StartupConfigError(format!("Configuration parsing error: {error}"))
        }
    })
}

/// Parses file content into a JSON value tree, resolving secret references
/// when present (mirrors the engine loader's `resolve_document` path).
fn parse_document_value(content: &str, format: ConfigFormat) -> Result<Value, StartupConfigError> {
    let mut value: Value = match format {
        ConfigFormat::YAML => serde_yaml::from_str(content)
            .map_err(|error| StartupConfigError(format!("YAML parsing error: {error}")))?,
        ConfigFormat::JSON => serde_json::from_str(content)
            .map_err(|error| StartupConfigError(format!("JSON parsing error: {error}")))?,
        ConfigFormat::TOML => {
            let document: toml::Value = toml::from_str(content)
                .map_err(|error| StartupConfigError(format!("TOML parsing error: {error}")))?;
            serde_json::to_value(document)
                .map_err(|error| StartupConfigError(format!("TOML conversion error: {error}")))?
        }
    };
    if content.contains("${") {
        arkflow_core::secret::resolve_value(&mut value).map_err(|error| {
            StartupConfigError(format!("Failed to resolve secret references: {error}"))
        })?;
    }
    Ok(value)
}

/// Rejects unknown top-level keys by comparing against the keys the default
/// configuration serializes (which enumerates every real field, including
/// the flattened `ServerConfig`), so a typo like `adress:` fails startup
/// instead of being silently swallowed by `flatten`.
fn check_unknown_keys(value: &Value) -> Result<(), StartupConfigError> {
    let Value::Object(keys) = value else {
        return Err(StartupConfigError(
            "Configuration error: the startup configuration must be a mapping".to_string(),
        ));
    };
    let defaults = serde_json::to_value(HubStartupConfig::default())
        .map_err(|error| StartupConfigError(format!("Configuration error: {error}")))?;
    let Value::Object(default_keys) = defaults else {
        return Err(StartupConfigError(
            "Configuration error: internal default serialization mismatch".to_string(),
        ));
    };
    let known: BTreeSet<&str> = default_keys.keys().map(String::as_str).collect();
    let unknown: Vec<&str> = keys
        .keys()
        .map(String::as_str)
        .filter(|key| !known.contains(key))
        .collect();
    if unknown.is_empty() {
        Ok(())
    } else {
        Err(StartupConfigError(format!(
            "Unknown configuration keys: {}",
            unknown.join(", ")
        )))
    }
}

/// Applies environment variables over the loaded configuration: a set
/// variable overrides; an invalid value warns on stderr and keeps the
/// underlying value (file value, or the built-in default when no file was
/// loaded). With no file this reproduces the previous env-only binary
/// behavior field by field (see the regression tests).
pub fn apply_env_overrides(cfg: &mut HubStartupConfig, env: &dyn Fn(&str) -> Option<String>) {
    let warn = |name: &str, value: &str, expected: &str| {
        eprintln!("warning: {name}='{value}' is not {expected}; keeping the underlying value");
    };
    let text = |name: &str| env(name).filter(|value| !value.is_empty());
    // An invalid spelling warns and leaves the target untouched (the file
    // value, or the built-in default without a file).
    let flag = |name: &str, target: &mut bool| {
        if let Some(value) = env(name) {
            match value.as_str() {
                "1" | "true" | "yes" => *target = true,
                "0" | "false" | "no" => *target = false,
                _ => warn(name, &value, "one of 1/true/yes/0/false/no"),
            }
        }
    };
    let number = |name: &str| -> Option<u64> {
        env(name).and_then(|value| match value.parse::<u64>() {
            Ok(parsed) => Some(parsed),
            Err(_) => {
                warn(name, &value, "a number");
                None
            }
        })
    };

    if let Some(value) = env("ARKFLOW_HUB_ADDRESS") {
        cfg.server.address = value;
    }
    if let Some(value) = text("ARKFLOW_NODE_TOKEN") {
        cfg.server.node_token = Some(value);
    }
    if let Some(value) = text("ARKFLOW_HUB_STORAGE") {
        cfg.server.hub_storage = Some(value);
    }
    if let Some(value) = text("ARKFLOW_HUB_TLS_CERT") {
        cfg.server.tls_cert = Some(value);
    }
    if let Some(value) = text("ARKFLOW_HUB_TLS_KEY") {
        cfg.server.tls_key = Some(value);
    }
    flag("ARKFLOW_HUB_INSECURE_LOCAL", &mut cfg.server.insecure_local);
    flag(
        "ARKFLOW_APPROVAL_REQUIRED",
        &mut cfg.server.approval_required,
    );
    if let Some(value) = number("ARKFLOW_APPROVAL_TTL_MS") {
        // A sub-floor value does not apply: the env value is rejected and
        // the underlying value (file, else default) stands.
        if value >= 1_000 {
            cfg.server.approval_ttl_ms = value;
        }
    }
    apply_rate_limit_overrides(&mut cfg.server.rate_limit, env);

    if let Some(value) = text("ARKFLOW_OPERATOR_TOKEN") {
        cfg.hub.operator_token = Some(value);
    }

    flag("ARKFLOW_HUB_HA_ENABLED", &mut cfg.ha.enabled);
    if let Some(value) = number("ARKFLOW_HUB_HA_LEASE_TTL_MS") {
        if value >= 1_000 {
            cfg.ha.lease_ttl_ms = value;
        }
    }
    if let Some(value) = text("ARKFLOW_HUB_HA_HOLDER_ID") {
        cfg.ha.holder_id = Some(value);
    }
    if let Some(value) = env("ARKFLOW_HUB_HA_ADVERTISE_URL") {
        cfg.ha.advertise_url = Some(value.trim().to_owned()).filter(|v| !v.is_empty());
    }

    if let Some(value) = text("ARKFLOW_LOG_LEVEL") {
        cfg.server.logging.level = value;
    }
    flag(
        "ARKFLOW_OTEL_TRACING_ENABLED",
        &mut cfg.server.observability.tracing.enabled,
    );
    if let Some(value) = text("ARKFLOW_OTEL_ENDPOINT") {
        cfg.server.observability.tracing.endpoint = value;
    }
    if let Some(value) = text("ARKFLOW_OTEL_SERVICE_NAME") {
        cfg.server.observability.tracing.service_name = value;
    }
}

/// Rate-limit overrides; the warn-on-invalid spelling matches the previous
/// binary parser. Unset variables keep the underlying (file or default)
/// value.
fn apply_rate_limit_overrides(
    cfg: &mut arkflow_core::config::RateLimitConfig,
    env: &dyn Fn(&str) -> Option<String>,
) {
    let warn = |name: &str, value: &str, expected: &str| {
        eprintln!("warning: {name}='{value}' is not {expected}; using the default");
    };
    let flag = |name: &str, target: &mut bool| {
        if let Some(value) = env(name) {
            match value.as_str() {
                "1" | "true" | "yes" => *target = true,
                "0" | "false" | "no" => *target = false,
                _ => warn(name, &value, "one of 1/true/yes/0/false/no"),
            }
        }
    };
    let number = |name: &str| -> Option<f64> {
        match env(name) {
            Some(value) => match value.parse::<f64>() {
                Ok(parsed) => Some(parsed),
                Err(_) => {
                    warn(name, &value, "a number");
                    None
                }
            },
            None => None,
        }
    };
    flag("ARKFLOW_RATE_LIMIT_ENABLED", &mut cfg.enabled);
    if let Some(value) = number("ARKFLOW_RATE_LIMIT_PRINCIPAL_RPS") {
        cfg.principal_rps = value;
    }
    if let Some(value) = number("ARKFLOW_RATE_LIMIT_PRINCIPAL_BURST") {
        cfg.principal_burst = value;
    }
    if let Some(value) = number("ARKFLOW_RATE_LIMIT_NODE_RPS") {
        cfg.node_rps = value;
    }
    if let Some(value) = number("ARKFLOW_RATE_LIMIT_NODE_BURST") {
        cfg.node_burst = value;
    }
}

fn format_from_path(path: &str) -> Option<ConfigFormat> {
    let path = path.to_lowercase();
    if path.ends_with(".yaml") || path.ends_with(".yml") {
        Some(ConfigFormat::YAML)
    } else if path.ends_with(".json") {
        Some(ConfigFormat::JSON)
    } else if path.ends_with(".toml") {
        Some(ConfigFormat::TOML)
    } else {
        None
    }
}
