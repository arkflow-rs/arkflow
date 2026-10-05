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

//! Configuration module
//!
//! Provide configuration management for the stream processing engine.

use serde::{Deserialize, Serialize};

use toml;

use crate::{stream::StreamConfig, Error};

/// Configuration file format
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConfigFormat {
    YAML,
    JSON,
    TOML,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    JSON,
    PLAIN,
}

/// Log configuration

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level
    pub level: String,
    /// Output to file?
    /// Log file path
    pub file_path: Option<String>,
    /// Log format (text or json)
    #[serde(default = "default_log_format")]
    pub format: LogFormat,
}

/// Process-level observability endpoints (`/metrics`, `/ready`, `/live`).
/// They stay available even when the control-plane API server is disabled.
/// The default bind is loopback so enabling never exposes metrics off-host.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    /// Whether the observability export is enabled
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Listening address for the observability endpoints
    #[serde(default = "default_observability_address")]
    pub address: String,
    /// Path for the Prometheus metrics endpoint
    #[serde(default = "default_observability_metrics_path")]
    pub metrics_path: String,
    /// Path for the readiness endpoint
    #[serde(default = "default_observability_ready_path")]
    pub ready_path: String,
    /// Path for the liveness endpoint
    #[serde(default = "default_observability_live_path")]
    pub live_path: String,
    /// OTel trace export (disabled by default).
    #[serde(default)]
    pub tracing: TracingConfig,
}

/// OTel trace export configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TracingConfig {
    /// Whether OTel trace export is enabled
    #[serde(default)]
    pub enabled: bool,
    /// OTLP/HTTP-JSON endpoint for span export
    #[serde(default = "default_tracing_endpoint")]
    pub endpoint: String,
    /// Resource `service.name`
    #[serde(default = "default_tracing_service_name")]
    pub service_name: String,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: default_tracing_endpoint(),
            service_name: default_tracing_service_name(),
        }
    }
}

fn default_tracing_endpoint() -> String {
    "http://127.0.0.1:4318/v1/traces".to_string()
}

fn default_tracing_service_name() -> String {
    "arkflow".to_string()
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            address: default_observability_address(),
            metrics_path: default_observability_metrics_path(),
            ready_path: default_observability_ready_path(),
            live_path: default_observability_live_path(),
            tracing: TracingConfig::default(),
        }
    }
}

fn default_observability_address() -> String {
    "127.0.0.1:8081".into()
}
fn default_observability_metrics_path() -> String {
    "/metrics".into()
}
fn default_observability_ready_path() -> String {
    "/ready".into()
}
fn default_observability_live_path() -> String {
    "/live".into()
}

/// Node-level configuration for everything this process does beyond running
/// streams: health endpoints, the control-plane API, Agent-mode Hub
/// membership, and the shuffle data plane. The YAML section keeps its
/// historical `health_check` key (`EngineConfig::node` carries a serde
/// rename), so the flattened sub-structs below must stay key-compatible
/// with the historical flat mapping; do not add `deny_unknown_fields`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Health check server endpoints.
    #[serde(flatten)]
    pub health: HealthEndpointsConfig,
    /// Versioned control-plane API served alongside the health endpoints.
    #[serde(flatten)]
    pub control_api: ControlApiConfig,
    /// Compute-node Agent mode (membership to a Hub). Empty `hub_urls`
    /// keeps standalone mode.
    #[serde(flatten)]
    pub agent: AgentConfig,
    /// Cross-node shuffle data plane.
    #[serde(flatten)]
    pub data_plane: DataPlaneConfig,
    /// Process-level observability export (metrics, readiness, liveness).
    #[serde(default)]
    pub observability: ObservabilityConfig,
    /// Sentinel for the removed single-address key: any present occurrence
    /// fails deserialization with a migration hint instead of being silently
    /// ignored (a dropped key would start the process in standalone mode).
    #[serde(default, skip_serializing)]
    pub hub_url: DeprecatedHubUrl,
}

/// Health check server endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthEndpointsConfig {
    /// Whether health check is enabled
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Listening address for health check server
    #[serde(default = "default_address")]
    pub address: String,
    /// Path for health check endpoint
    #[serde(default = "default_health_path")]
    pub health_path: String,
    /// Path for readiness check endpoint
    #[serde(default = "default_readiness_path")]
    pub readiness_path: String,
    /// Path for liveness check endpoint
    #[serde(default = "default_liveness_path")]
    pub liveness_path: String,
}

/// Versioned control-plane API configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlApiConfig {
    /// Prefix for the versioned control-plane API.
    #[serde(default = "default_api_prefix")]
    pub api_prefix: String,
    /// Optional Bearer token for control-plane operations and configuration.
    #[serde(default)]
    pub api_token: Option<String>,
    /// Explicit browser origins allowed to call the control API. Empty denies cross-origin calls.
    #[serde(default)]
    pub cors_origins: Vec<String>,
}

/// Compute-node Agent mode configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConfig {
    /// Hub addresses for compute-node Agent mode, tried in order as failover
    /// candidates (see hub-ha stage 3). Empty keeps standalone mode.
    #[serde(default)]
    pub hub_urls: Vec<String>,
    /// Stable identity used when this process reports to a Hub.
    #[serde(default)]
    pub node_id: Option<String>,
    /// Shared node registration credential. Never included in reports.
    #[serde(default)]
    pub node_token: Option<String>,
    /// Lease duration advertised by a compute node to its Hub.
    #[serde(default = "default_agent_lease_ttl_ms")]
    pub agent_lease_ttl_ms: u64,
    /// Lifetime of a Hub-issued agent session credential.
    #[serde(default = "default_agent_session_ttl_ms")]
    pub agent_session_ttl_ms: u64,
}

/// Cross-node shuffle data plane configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DataPlaneConfig {
    /// Data-plane listen port for cross-node shuffle. When absent the node
    /// runs without a network data plane and the Hub placement keeps every
    /// Job edge co-located (the default contract).
    #[serde(default)]
    pub data_port: Option<u16>,
    /// Routable host advertised to peers for the data plane (for example the
    /// node's LAN IP). Required (together with `data_port`) for the node to
    /// take part in split placement; loopback-only nodes stay colocated-only.
    #[serde(default)]
    pub data_host: Option<String>,
}

/// Placeholder type for the removed `health_check.hub_url` key. Its
/// `Deserialize` impl always fails with a migration hint so a renamed-away
/// key can never be silently ignored by serde's unknown-field tolerance.
#[derive(Debug, Clone, Copy, Default)]
pub struct DeprecatedHubUrl;

impl<'de> Deserialize<'de> for DeprecatedHubUrl {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let _ = serde::de::IgnoredAny::deserialize(deserializer)?;
        Err(serde::de::Error::custom(
            "`health_check.hub_url` was renamed and made a list; write `hub_urls: [\"http://hub:8080\"]`",
        ))
    }
}

impl AgentConfig {
    /// Every Agent-mode Hub address must be an absolute http(s) base URL the
    /// reqwest client can target (scheme + non-empty host).
    pub fn validate_hub_urls(&self) -> Result<(), Error> {
        for (index, url) in self.hub_urls.iter().enumerate() {
            let rest = url
                .strip_prefix("http://")
                .or_else(|| url.strip_prefix("https://"))
                .ok_or_else(|| {
                    Error::Config(format!(
                        "health_check.hub_urls[{index}] must start with http:// or https://: {url:?}"
                    ))
                })?;
            if rest.trim().is_empty() || rest.trim_matches('/').is_empty() {
                return Err(Error::Config(format!(
                    "health_check.hub_urls[{index}] is missing a host: {url:?}"
                )));
            }
        }
        Ok(())
    }
}

/// Engine configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    /// Streams configuration
    #[serde(default)]
    pub streams: Vec<StreamConfig>,
    /// Local Jobs declared directly in config (executed by the unified
    /// kernel without a Hub).
    #[serde(default)]
    pub jobs: Vec<crate::job::JobSpec>,
    /// Logging configuration (optional)
    #[serde(default)]
    pub logging: LoggingConfig,
    /// Node-level configuration (YAML section `health_check`).
    #[serde(default, rename = "health_check")]
    pub node: NodeConfig,
}

impl EngineConfig {
    /// Validate stable Stream IDs and return the IDs used by the runtime.
    /// Missing IDs are deterministic compatibility IDs for legacy configs.
    pub fn stream_ids(&self) -> Result<Vec<String>, Error> {
        let mut ids = std::collections::HashSet::with_capacity(self.streams.len());
        let mut resolved = Vec::with_capacity(self.streams.len());

        for (index, stream) in self.streams.iter().enumerate() {
            stream.validate_id(index)?;
            let id = stream.effective_id(index);
            if !ids.insert(id.clone()) {
                return Err(Error::Config(format!(
                    "Duplicate stream id '{}' at index {}",
                    id, index
                )));
            }
            resolved.push(id);
        }

        Ok(resolved)
    }

    /// Validate declared Jobs (spec-level validation incl. graph checks).
    pub fn job_specs(&self) -> Result<Vec<&crate::job::JobSpec>, Error> {
        let mut job_ids = std::collections::HashSet::with_capacity(self.jobs.len());
        for job in &self.jobs {
            job.validate()?;
            if !job_ids.insert(job.id.as_str().to_owned()) {
                return Err(Error::Config(format!("Duplicate job id '{}'", job.id)));
            }
        }
        Ok(self.jobs.iter().collect())
    }

    /// Load configuration from file
    pub fn from_file(path: &str) -> Result<Self, Error> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| Error::Config(format!("Unable to read configuration file: {}", e)))?;

        // Determine the format based on the file extension.
        if let Some(format) = get_format_from_path(path) {
            return parse_engine_config(&content, format);
        };

        Err(Error::Config("The configuration file format cannot be determined. Please use YAML, JSON, or TOML format.".to_string()))
    }
}

/// Parse a configuration document into `EngineConfig`.
///
/// Documents without a secret-reference marker take the direct
/// deserialization path so parse errors keep their line/column locations.
/// Documents containing `${` are resolved through a value tree first (see
/// `crate::secret`).
fn parse_engine_config(content: &str, format: ConfigFormat) -> Result<EngineConfig, Error> {
    if crate::secret::contains_reference(content) {
        let document = match format {
            ConfigFormat::YAML => crate::secret::ConfigDocument::Yaml(content),
            ConfigFormat::JSON => crate::secret::ConfigDocument::Json(content),
            ConfigFormat::TOML => crate::secret::ConfigDocument::Toml(content),
        };
        let value = crate::secret::resolve_document(document)?;
        return serde_json::from_value(value).map_err(|_| {
            // Deserialization failures after secret resolution must not echo
            // the offending value: serde type errors embed the resolved
            // secret. Use a fixed, value-independent message.
            Error::Config(
                "Configuration error: validation failed after secret resolution".to_string(),
            )
        });
    }
    match format {
        ConfigFormat::YAML => serde_yaml::from_str(content)
            .map_err(|e| Error::Config(format!("YAML parsing error: {}", e))),
        ConfigFormat::JSON => serde_json::from_str(content)
            .map_err(|e| Error::Config(format!("JSON parsing error: {}", e))),
        ConfigFormat::TOML => {
            toml::from_str(content).map_err(|e| Error::Config(format!("TOML parsing error: {}", e)))
        }
    }
}

/// Get configuration format from file path.
fn get_format_from_path(path: &str) -> Option<ConfigFormat> {
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

/// Default address for health check server
fn default_address() -> String {
    "127.0.0.1:8080".to_string()
}

/// Default value for health check path
fn default_health_path() -> String {
    "/health".to_string()
}

/// Default value for readiness path
fn default_readiness_path() -> String {
    "/readiness".to_string()
}

/// Default value for liveness path
fn default_liveness_path() -> String {
    "/liveness".to_string()
}

/// Default prefix for versioned control-plane routes.
fn default_api_prefix() -> String {
    "/api/v1".to_string()
}
/// Default value for health check enabled
fn default_enabled() -> bool {
    true
}

fn default_agent_lease_ttl_ms() -> u64 {
    15_000
}

fn default_agent_session_ttl_ms() -> u64 {
    3_600_000
}

impl Default for HealthEndpointsConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            address: default_address(),
            health_path: default_health_path(),
            readiness_path: default_readiness_path(),
            liveness_path: default_liveness_path(),
        }
    }
}

impl Default for ControlApiConfig {
    fn default() -> Self {
        Self {
            api_prefix: default_api_prefix(),
            api_token: None,
            cors_origins: Vec::new(),
        }
    }
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            hub_urls: Vec::new(),
            node_id: None,
            node_token: None,
            agent_lease_ttl_ms: default_agent_lease_ttl_ms(),
            agent_session_ttl_ms: default_agent_session_ttl_ms(),
        }
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            health: HealthEndpointsConfig::default(),
            control_api: ControlApiConfig::default(),
            agent: AgentConfig::default(),
            data_plane: DataPlaneConfig::default(),
            observability: ObservabilityConfig::default(),
            hub_url: DeprecatedHubUrl,
        }
    }
}

/// Default value for log format
fn default_log_format() -> LogFormat {
    LogFormat::PLAIN
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            file_path: None,
            format: default_log_format(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::env;
    use std::fs::{self, File};
    use std::io::Write;

    /// A type error after secret resolution must not echo the resolved
    /// value: serde type errors embed the offending string, which would
    /// leak the secret into the CLI error output.
    #[test]
    fn parse_error_after_secret_resolution_does_not_leak_the_resolved_value() {
        let name = "ARKFLOW_TEST_LEAK_PROBE";
        env::set_var(name, "sup3r-s3cret-value");
        // `health_check.enabled` is a bool; the resolved secret lands there
        // as a string, so deserialization fails after resolution succeeds.
        let content = format!("health_check:\n  enabled: ${{env:{name}}}\n");
        let error = parse_engine_config(&content, ConfigFormat::YAML)
            .expect_err("type mismatch must fail materialization");
        let message = error.to_string();
        assert!(
            message.contains("validation failed after secret resolution"),
            "unexpected error: {message}"
        );
        assert!(
            !message.contains("sup3r-s3cret-value"),
            "resolved secret leaked into the error: {message}"
        );
    }

    #[test]
    fn test_default_log_format() {
        let format = default_log_format();
        assert!(matches!(format, LogFormat::PLAIN));
    }

    #[test]
    fn test_default_address() {
        let address = default_address();
        assert_eq!(address, "127.0.0.1:8080");
    }

    #[test]
    fn test_default_health_path() {
        let path = default_health_path();
        assert_eq!(path, "/health");
    }

    #[test]
    fn test_default_readiness_path() {
        let path = default_readiness_path();
        assert_eq!(path, "/readiness");
    }

    #[test]
    fn test_default_liveness_path() {
        let path = default_liveness_path();
        assert_eq!(path, "/liveness");
    }

    #[test]
    fn test_default_enabled() {
        let enabled = default_enabled();
        assert!(enabled);
    }

    #[test]
    fn test_health_check_config_default() {
        let config = NodeConfig::default();
        assert!(config.health.enabled);
        assert_eq!(config.health.address, "127.0.0.1:8080");
        assert_eq!(config.health.health_path, "/health");
        assert_eq!(config.health.readiness_path, "/readiness");
        assert_eq!(config.health.liveness_path, "/liveness");
    }

    #[test]
    fn test_logging_config_default() {
        let config = LoggingConfig::default();
        assert_eq!(config.level, "info");
        assert!(config.file_path.is_none());
        assert!(matches!(config.format, LogFormat::PLAIN));
    }

    #[test]
    fn test_log_format_serialization() {
        let format = LogFormat::JSON;
        let serialized = serde_json::to_string(&format).unwrap();
        assert_eq!(serialized, "\"json\"");

        let format = LogFormat::PLAIN;
        let serialized = serde_json::to_string(&format).unwrap();
        assert_eq!(serialized, "\"plain\"");
    }

    #[test]
    fn test_log_format_deserialization() {
        let json = "\"json\"";
        let format: LogFormat = serde_json::from_str(json).unwrap();
        assert!(matches!(format, LogFormat::JSON));

        let json = "\"plain\"";
        let format: LogFormat = serde_json::from_str(json).unwrap();
        assert!(matches!(format, LogFormat::PLAIN));
    }

    #[test]
    fn test_logging_config_serialization() {
        let config = LoggingConfig {
            level: "debug".to_string(),
            file_path: Some("/var/log/arkflow.log".to_string()),
            format: LogFormat::JSON,
        };

        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: LoggingConfig = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.level, "debug");
        assert_eq!(
            deserialized.file_path,
            Some("/var/log/arkflow.log".to_string())
        );
        assert!(matches!(deserialized.format, LogFormat::JSON));
    }

    #[test]
    fn test_hub_urls_parses_as_list_and_defaults_empty() {
        let config: EngineConfig = serde_json::from_str(
            r#"{"health_check": {"hub_urls": ["http://hub-a:8080", "http://hub-b:8080/"]}}"#,
        )
        .unwrap();
        assert_eq!(
            config.node.agent.hub_urls,
            vec!["http://hub-a:8080", "http://hub-b:8080/"]
        );

        let config: EngineConfig = serde_json::from_str("{}").unwrap();
        assert!(config.node.agent.hub_urls.is_empty());
    }

    #[test]
    fn test_legacy_hub_url_key_fails_loudly() {
        let error = serde_json::from_str::<EngineConfig>(
            r#"{"health_check": {"hub_url": "http://127.0.0.1:8080"}}"#,
        )
        .unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains("hub_urls"),
            "error must carry the migration hint: {message}"
        );

        // Any legacy form fails the same way, not just strings.
        let error = serde_json::from_str::<EngineConfig>(r#"{"health_check": {"hub_url": null}}"#)
            .unwrap_err();
        assert!(error.to_string().contains("hub_urls"));
    }

    #[test]
    fn test_hub_url_sentinel_is_not_serialized() {
        let serialized = serde_json::to_string(&NodeConfig::default()).unwrap();
        assert!(
            !serialized.contains("hub_url\""),
            "serialized = {serialized}"
        );
    }

    #[test]
    fn test_validate_hub_urls() {
        let mut config = AgentConfig::default();
        assert!(config.validate_hub_urls().is_ok());

        config.hub_urls = vec!["http://hub-a:8080".into(), "https://hub-b".into()];
        assert!(config.validate_hub_urls().is_ok());

        config.hub_urls = vec!["hub-a:8080".into()];
        assert!(config.validate_hub_urls().is_err());

        config.hub_urls = vec!["http://".into()];
        assert!(config.validate_hub_urls().is_err());

        config.hub_urls = vec!["http://///".into()];
        assert!(config.validate_hub_urls().is_err());
    }

    #[test]
    fn test_health_check_config_serialization() {
        let config = NodeConfig {
            health: HealthEndpointsConfig {
                enabled: false,
                address: "127.0.0.1:9090".to_string(),
                health_path: "/healthz".to_string(),
                readiness_path: "/ready".to_string(),
                liveness_path: "/live".to_string(),
            },
            control_api: ControlApiConfig {
                api_prefix: "/api/v1".to_string(),
                api_token: Some("test-token".to_string()),
                cors_origins: Vec::new(),
            },
            agent: AgentConfig::default(),
            data_plane: DataPlaneConfig::default(),
            observability: ObservabilityConfig::default(),
            hub_url: DeprecatedHubUrl,
        };

        let serialized = serde_json::to_string(&config).unwrap();
        // The flattened sub-structs must keep the historical flat key set.
        let value: serde_json::Value = serde_json::from_str(&serialized).unwrap();
        for key in [
            "enabled",
            "address",
            "health_path",
            "readiness_path",
            "liveness_path",
            "api_prefix",
            "api_token",
            "cors_origins",
            "hub_urls",
            "node_id",
            "node_token",
            "agent_lease_ttl_ms",
            "agent_session_ttl_ms",
            "data_port",
            "data_host",
            "observability",
        ] {
            assert!(
                value.get(key).is_some(),
                "serialized key set must keep `{key}`"
            );
        }
        assert!(
            value.get("hub_url").is_none(),
            "sentinel must not serialize"
        );

        // The historical flat document shape still deserializes into the
        // split type with every field landing in its group.
        let deserialized: NodeConfig = serde_json::from_str(&serialized).unwrap();
        assert!(!deserialized.health.enabled);
        assert_eq!(deserialized.health.address, "127.0.0.1:9090");
        assert_eq!(deserialized.health.health_path, "/healthz");
        assert_eq!(deserialized.health.readiness_path, "/ready");
        assert_eq!(deserialized.health.liveness_path, "/live");
        assert_eq!(deserialized.control_api.api_prefix, "/api/v1");
        assert_eq!(
            deserialized.control_api.api_token.as_deref(),
            Some("test-token")
        );
        assert!(deserialized.observability.enabled);
    }

    #[test]
    fn test_observability_config_defaults() {
        let config = ObservabilityConfig::default();
        assert!(config.enabled);
        assert_eq!(config.address, "127.0.0.1:8081");
        assert_eq!(config.metrics_path, "/metrics");
        assert_eq!(config.ready_path, "/ready");
        assert_eq!(config.live_path, "/live");
    }

    #[test]
    fn test_health_check_without_observability_section_uses_defaults() {
        let health: NodeConfig = serde_json::from_str(json!({}).to_string().as_str())
            .expect("an empty object must deserialize with all defaults");
        assert!(health.observability.enabled);
        assert_eq!(health.observability.address, "127.0.0.1:8081");
        assert!(health.health.enabled, "flattened groups use their defaults");

        let health: NodeConfig = serde_json::from_str(
            json!({"observability": {"enabled": false}, "api_prefix": "/x", "hub_urls": ["http://h:1"]})
                .to_string()
                .as_str(),
        )
        .unwrap();
        assert!(!health.observability.enabled);
        assert_eq!(health.control_api.api_prefix, "/x");
        assert_eq!(health.agent.hub_urls, vec!["http://h:1".to_string()]);

        // EngineConfig keeps the historical `health_check` YAML/JSON key.
        let engine: EngineConfig = serde_json::from_str(
            json!({"health_check": {"enabled": false}})
                .to_string()
                .as_str(),
        )
        .unwrap();
        assert!(!engine.node.health.enabled);
    }

    #[test]
    fn test_get_format_from_path_yaml() {
        assert_eq!(
            get_format_from_path("config.yaml"),
            Some(ConfigFormat::YAML)
        );
        assert_eq!(get_format_from_path("config.yml"), Some(ConfigFormat::YAML));
        assert_eq!(
            get_format_from_path("/path/to/config.YAML"),
            Some(ConfigFormat::YAML)
        );
        assert_eq!(
            get_format_from_path("/path/to/config.YML"),
            Some(ConfigFormat::YAML)
        );
    }

    #[test]
    fn test_get_format_from_path_json() {
        assert_eq!(
            get_format_from_path("config.json"),
            Some(ConfigFormat::JSON)
        );
        assert_eq!(
            get_format_from_path("/path/to/config.JSON"),
            Some(ConfigFormat::JSON)
        );
    }

    #[test]
    fn test_get_format_from_path_toml() {
        assert_eq!(
            get_format_from_path("config.toml"),
            Some(ConfigFormat::TOML)
        );
        assert_eq!(
            get_format_from_path("/path/to/config.TOML"),
            Some(ConfigFormat::TOML)
        );
    }

    #[test]
    fn test_get_format_from_path_unknown() {
        assert_eq!(get_format_from_path("config.txt"), None);
        assert_eq!(get_format_from_path("config"), None);
        assert_eq!(get_format_from_path("/path/to/config.xml"), None);
    }

    #[test]
    fn test_engine_config_from_yaml_file() {
        let mut temp_path = env::temp_dir();
        temp_path.push(format!("test_config_{}.yaml", std::process::id()));
        let config_path = temp_path.clone();

        let yaml_content = r#"
logging:
  level: debug
  file_path: "/tmp/test.log"
  format: json

health_check:
  enabled: false
  address: "127.0.0.1:9090"

streams: []
"#;

        let mut file = File::create(&config_path).unwrap();
        file.write_all(yaml_content.as_bytes()).unwrap();

        let config = EngineConfig::from_file(config_path.to_str().unwrap()).unwrap();

        assert_eq!(config.logging.level, "debug");
        assert_eq!(config.logging.file_path, Some("/tmp/test.log".to_string()));
        assert!(matches!(config.logging.format, LogFormat::JSON));
        assert!(!config.node.health.enabled);
        assert_eq!(config.node.health.address, "127.0.0.1:9090");
        assert!(config.streams.is_empty());

        // Clean up
        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn test_engine_config_from_json_file() {
        let mut temp_path = env::temp_dir();
        temp_path.push(format!("test_config_{}.json", std::process::id()));
        let config_path = temp_path.clone();

        let json_content = json!({
            "logging": {
                "level": "info",
                "format": "plain"
            },
            "health_check": {
                "enabled": true,
                "address": "0.0.0.0:8080"
            },
            "streams": []
        });

        let mut file = File::create(&config_path).unwrap();
        file.write_all(json_content.to_string().as_bytes()).unwrap();

        let config = EngineConfig::from_file(config_path.to_str().unwrap()).unwrap();

        assert_eq!(config.logging.level, "info");
        assert!(matches!(config.logging.format, LogFormat::PLAIN));
        assert!(config.node.health.enabled);
        assert_eq!(config.node.health.address, "0.0.0.0:8080");
        assert!(config.streams.is_empty());

        // Clean up
        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn test_engine_config_from_toml_file() {
        let mut temp_path = env::temp_dir();
        temp_path.push(format!("test_config_{}.toml", std::process::id()));
        let config_path = temp_path.clone();

        let toml_content = r#"
[logging]
level = "warn"
format = "json"

[health_check]
enabled = false
address = "192.168.1.1:8888"

[[streams]]
[streams.input]
type = "generate"

[streams.pipeline]
thread_num = 1

[[streams.pipeline.processors]]
type = "json_to_arrow"

[streams.output]
type = "stdout"
"#;

        let mut file = File::create(&config_path).unwrap();
        file.write_all(toml_content.as_bytes()).unwrap();

        let config = EngineConfig::from_file(config_path.to_str().unwrap()).unwrap();

        assert_eq!(config.logging.level, "warn");
        assert!(matches!(config.logging.format, LogFormat::JSON));
        assert!(!config.node.health.enabled);
        assert_eq!(config.node.health.address, "192.168.1.1:8888");
        assert_eq!(config.streams.len(), 1);

        // Clean up
        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn test_engine_config_from_file_invalid_format() {
        let mut temp_path = env::temp_dir();
        temp_path.push(format!("test_config_{}.txt", std::process::id()));
        let config_path = temp_path.clone();

        let mut file = File::create(&config_path).unwrap();
        file.write_all(b"invalid content").unwrap();

        let result = EngineConfig::from_file(config_path.to_str().unwrap());
        assert!(result.is_err());

        // Clean up
        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn test_engine_config_from_file_nonexistent() {
        let result = EngineConfig::from_file("/nonexistent/path/config.yaml");
        assert!(result.is_err());
    }

    #[test]
    fn test_engine_config_from_file_invalid_yaml() {
        let mut temp_path = env::temp_dir();
        temp_path.push(format!("test_config_invalid_{}.yaml", std::process::id()));
        let config_path = temp_path.clone();

        let mut file = File::create(&config_path).unwrap();
        file.write_all(b"invalid: yaml: content: [").unwrap();

        let result = EngineConfig::from_file(config_path.to_str().unwrap());
        assert!(result.is_err());

        // Clean up
        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn test_engine_config_from_file_invalid_json() {
        let mut temp_path = env::temp_dir();
        temp_path.push(format!("test_config_invalid_{}.json", std::process::id()));
        let config_path = temp_path.clone();

        let mut file = File::create(&config_path).unwrap();
        file.write_all(b"invalid json content").unwrap();

        let result = EngineConfig::from_file(config_path.to_str().unwrap());
        assert!(result.is_err());

        // Clean up
        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn test_engine_config_serialization_with_defaults() {
        let config = EngineConfig {
            streams: vec![],
            jobs: Vec::new(),
            logging: LoggingConfig::default(),
            node: NodeConfig::default(),
        };

        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: EngineConfig = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.logging.level, "info");
        assert!(matches!(deserialized.logging.format, LogFormat::PLAIN));
        assert!(deserialized.node.health.enabled);
        assert_eq!(deserialized.node.health.address, "127.0.0.1:8080");
    }

    fn test_stream(id: Option<&str>) -> crate::stream::StreamConfig {
        crate::stream::StreamConfig {
            id: id.map(str::to_string),
            input: crate::input::InputConfig {
                input_type: "generate".to_string(),
                name: None,
                codec: None,
                config: None,
            },
            pipeline: crate::pipeline::PipelineConfig {
                thread_num: 1,
                processors: vec![],
            },
            output: crate::output::OutputConfig {
                output_type: "stdout".to_string(),
                name: None,
                codec: None,
                config: None,
            },
            error_output: None,
            buffer: None,
            durability: None,
            state: None,
            temporary: None,
        }
    }

    #[test]
    fn test_stream_ids_assign_legacy_ids() {
        let config = EngineConfig {
            streams: vec![test_stream(None), test_stream(None)],
            jobs: Vec::new(),
            logging: LoggingConfig::default(),
            node: NodeConfig::default(),
        };

        assert_eq!(config.stream_ids().unwrap(), ["stream-0", "stream-1"]);
    }

    #[test]
    fn test_stream_ids_reject_invalid_and_duplicate_ids() {
        let invalid = EngineConfig {
            streams: vec![test_stream(Some("bad id"))],
            jobs: Vec::new(),
            logging: LoggingConfig::default(),
            node: NodeConfig::default(),
        };
        assert!(invalid.stream_ids().is_err());

        let duplicate = EngineConfig {
            streams: vec![test_stream(Some("orders")), test_stream(Some("orders"))],
            jobs: Vec::new(),
            logging: LoggingConfig::default(),
            node: NodeConfig::default(),
        };
        assert!(duplicate.stream_ids().is_err());
    }

    #[test]
    fn test_stream_id_serializes_and_schema_exposes_id() {
        let serialized = serde_json::to_value(test_stream(Some("orders"))).unwrap();
        assert_eq!(serialized["id"], "orders");

        let schema = crate::component::build_config_schema();
        assert_eq!(
            schema["$defs"]["stream"]["properties"]["id"]["type"],
            "string"
        );
    }

    #[test]
    fn test_from_file_resolves_env_reference() {
        std::env::set_var("ARKFLOW_CONFIG_TEST_TOKEN", "from-env");
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(
            &path,
            r#"
logging:
  level: debug
health_check:
  api_token: "${env:ARKFLOW_CONFIG_TEST_TOKEN}"
"#,
        )
        .unwrap();
        let config = EngineConfig::from_file(path.to_str().unwrap()).unwrap();
        std::env::remove_var("ARKFLOW_CONFIG_TEST_TOKEN");
        assert_eq!(
            config.node.control_api.api_token.as_deref(),
            Some("from-env")
        );
    }

    #[test]
    fn test_from_file_resolves_file_reference_in_json() {
        let dir = tempfile::tempdir().unwrap();
        let secret_path = dir.path().join("token.txt");
        std::fs::write(&secret_path, b"from-file\n").unwrap();
        let path = dir.path().join("config.json");
        let content = serde_json::json!({
            "logging": {"level": "debug"},
            "health_check": {
                "api_token": format!("${{file:{}}}", secret_path.display())
            }
        })
        .to_string();
        std::fs::write(&path, content).unwrap();
        let config = EngineConfig::from_file(path.to_str().unwrap()).unwrap();
        assert_eq!(
            config.node.control_api.api_token.as_deref(),
            Some("from-file")
        );
    }

    #[test]
    fn test_from_file_unresolved_reference_names_path() {
        std::env::remove_var("ARKFLOW_CONFIG_TEST_DEFINITELY_UNSET");
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(
            &path,
            r#"
logging:
  level: debug
health_check:
  api_token: "${env:ARKFLOW_CONFIG_TEST_DEFINITELY_UNSET}"
"#,
        )
        .unwrap();
        let err = EngineConfig::from_file(path.to_str().unwrap()).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("health_check.api_token"), "{message}");
        assert!(
            message.contains("${env:ARKFLOW_CONFIG_TEST_DEFINITELY_UNSET}"),
            "{message}"
        );
    }

    #[test]
    fn test_from_file_without_references_keeps_line_numbers() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(
            &path,
            r#"
logging:
  level: debug
health_check:
  agent_lease_ttl_ms: not-a-number
"#,
        )
        .unwrap();
        let err = EngineConfig::from_file(path.to_str().unwrap()).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("YAML parsing error"), "{message}");
        assert!(
            message.contains("line ") && message.contains("column "),
            "expected line/column location: {message}"
        );
    }
}
