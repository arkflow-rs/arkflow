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

/// Health check configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckConfig {
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
    /// Prefix for the versioned control-plane API.
    #[serde(default = "default_api_prefix")]
    pub api_prefix: String,
    /// Optional Bearer token for control-plane operations and configuration.
    #[serde(default)]
    pub api_token: Option<String>,
    /// Explicit browser origins allowed to call the control API. Empty denies cross-origin calls.
    #[serde(default)]
    pub cors_origins: Vec<String>,
    /// Hub URL for compute-node Agent mode. When absent, standalone mode is used.
    #[serde(default)]
    pub hub_url: Option<String>,
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
    /// Process-level observability export (metrics, readiness, liveness).
    #[serde(default)]
    pub observability: ObservabilityConfig,
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
    /// Health check configuration (optional)
    #[serde(default)]
    pub health_check: HealthCheckConfig,
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
        return serde_json::from_value(value)
            .map_err(|e| Error::Config(format!("Configuration error: {}", e)));
    }
    match format {
        ConfigFormat::YAML => serde_yaml::from_str(content)
            .map_err(|e| Error::Config(format!("YAML parsing error: {}", e))),
        ConfigFormat::JSON => serde_json::from_str(content)
            .map_err(|e| Error::Config(format!("JSON parsing error: {}", e))),
        ConfigFormat::TOML => toml::from_str(content)
            .map_err(|e| Error::Config(format!("TOML parsing error: {}", e))),
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

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            address: default_address(),
            health_path: default_health_path(),
            readiness_path: default_readiness_path(),
            liveness_path: default_liveness_path(),
            api_prefix: default_api_prefix(),
            api_token: None,
            cors_origins: Vec::new(),
            hub_url: None,
            node_id: None,
            node_token: None,
            agent_lease_ttl_ms: default_agent_lease_ttl_ms(),
            agent_session_ttl_ms: default_agent_session_ttl_ms(),
            data_port: None,
            data_host: None,
            observability: ObservabilityConfig::default(),
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
        let config = HealthCheckConfig::default();
        assert!(config.enabled);
        assert_eq!(config.address, "127.0.0.1:8080");
        assert_eq!(config.health_path, "/health");
        assert_eq!(config.readiness_path, "/readiness");
        assert_eq!(config.liveness_path, "/liveness");
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
    fn test_health_check_config_serialization() {
        let config = HealthCheckConfig {
            enabled: false,
            address: "127.0.0.1:9090".to_string(),
            health_path: "/healthz".to_string(),
            readiness_path: "/ready".to_string(),
            liveness_path: "/live".to_string(),
            api_prefix: "/api/v1".to_string(),
            api_token: Some("test-token".to_string()),
            cors_origins: Vec::new(),
            hub_url: None,
            node_id: None,
            node_token: None,
            agent_lease_ttl_ms: default_agent_lease_ttl_ms(),
            agent_session_ttl_ms: default_agent_session_ttl_ms(),
            data_port: None,
            data_host: None,
            observability: ObservabilityConfig::default(),
        };

        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: HealthCheckConfig = serde_json::from_str(&serialized).unwrap();

        assert!(!deserialized.enabled);
        assert_eq!(deserialized.address, "127.0.0.1:9090");
        assert_eq!(deserialized.health_path, "/healthz");
        assert_eq!(deserialized.readiness_path, "/ready");
        assert_eq!(deserialized.liveness_path, "/live");
        assert_eq!(deserialized.api_prefix, "/api/v1");
        assert_eq!(deserialized.api_token.as_deref(), Some("test-token"));
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
        let health: HealthCheckConfig = serde_json::from_str(json!({}).to_string().as_str())
            .expect("an empty object must deserialize with all defaults");
        assert!(health.observability.enabled);
        assert_eq!(health.observability.address, "127.0.0.1:8081");

        let health: HealthCheckConfig =
            serde_json::from_str(json!({"observability": {"enabled": false}}).to_string().as_str())
                .unwrap();
        assert!(!health.observability.enabled);
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
        assert!(!config.health_check.enabled);
        assert_eq!(config.health_check.address, "127.0.0.1:9090");
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
        assert!(config.health_check.enabled);
        assert_eq!(config.health_check.address, "0.0.0.0:8080");
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
        assert!(!config.health_check.enabled);
        assert_eq!(config.health_check.address, "192.168.1.1:8888");
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
            health_check: HealthCheckConfig::default(),
        };

        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: EngineConfig = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.logging.level, "info");
        assert!(matches!(deserialized.logging.format, LogFormat::PLAIN));
        assert!(deserialized.health_check.enabled);
        assert_eq!(deserialized.health_check.address, "127.0.0.1:8080");
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
            health_check: HealthCheckConfig::default(),
        };

        assert_eq!(config.stream_ids().unwrap(), ["stream-0", "stream-1"]);
    }

    #[test]
    fn test_stream_ids_reject_invalid_and_duplicate_ids() {
        let invalid = EngineConfig {
            streams: vec![test_stream(Some("bad id"))],
            jobs: Vec::new(),
            logging: LoggingConfig::default(),
            health_check: HealthCheckConfig::default(),
        };
        assert!(invalid.stream_ids().is_err());

        let duplicate = EngineConfig {
            streams: vec![test_stream(Some("orders")), test_stream(Some("orders"))],
            jobs: Vec::new(),
            logging: LoggingConfig::default(),
            health_check: HealthCheckConfig::default(),
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
        assert_eq!(config.health_check.api_token.as_deref(), Some("from-env"));
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
        assert_eq!(config.health_check.api_token.as_deref(), Some("from-file"));
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
