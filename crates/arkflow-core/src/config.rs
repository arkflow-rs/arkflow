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
}

/// Engine configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    /// Streams configuration
    pub streams: Vec<StreamConfig>,
    /// Logging configuration (optional)
    #[serde(default)]
    pub logging: LoggingConfig,
    /// Health check configuration (optional)
    #[serde(default)]
    pub health_check: HealthCheckConfig,
}

impl EngineConfig {
    /// Load configuration from file
    pub fn from_file(path: &str) -> Result<Self, Error> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| Error::Config(format!("Unable to read configuration file: {}", e)))?;

        // Determine the format based on the file extension.
        if let Some(format) = get_format_from_path(path) {
            return match format {
                ConfigFormat::YAML => serde_yaml::from_str(&content)
                    .map_err(|e| Error::Config(format!("YAML parsing error: {}", e))),
                ConfigFormat::JSON => serde_json::from_str(&content)
                    .map_err(|e| Error::Config(format!("JSON parsing error: {}", e))),
                ConfigFormat::TOML => toml::from_str(&content)
                    .map_err(|e| Error::Config(format!("TOML parsing error: {}", e))),
            };
        };

        Err(Error::Config("The configuration file format cannot be determined. Please use YAML, JSON, or TOML format.".to_string()))
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
    "0.0.0.0:8080".to_string()
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
/// Default value for health check enabled
fn default_enabled() -> bool {
    true
}

impl Default for HealthCheckConfig {
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
        assert_eq!(address, "0.0.0.0:8080");
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
        assert_eq!(config.enabled, true);
        assert_eq!(config.address, "0.0.0.0:8080");
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
        assert_eq!(deserialized.file_path, Some("/var/log/arkflow.log".to_string()));
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
        };

        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: HealthCheckConfig = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.enabled, false);
        assert_eq!(deserialized.address, "127.0.0.1:9090");
        assert_eq!(deserialized.health_path, "/healthz");
        assert_eq!(deserialized.readiness_path, "/ready");
        assert_eq!(deserialized.liveness_path, "/live");
    }

    #[test]
    fn test_get_format_from_path_yaml() {
        assert_eq!(get_format_from_path("config.yaml"), Some(ConfigFormat::YAML));
        assert_eq!(get_format_from_path("config.yml"), Some(ConfigFormat::YAML));
        assert_eq!(get_format_from_path("/path/to/config.YAML"), Some(ConfigFormat::YAML));
        assert_eq!(get_format_from_path("/path/to/config.YML"), Some(ConfigFormat::YAML));
    }

    #[test]
    fn test_get_format_from_path_json() {
        assert_eq!(get_format_from_path("config.json"), Some(ConfigFormat::JSON));
        assert_eq!(get_format_from_path("/path/to/config.JSON"), Some(ConfigFormat::JSON));
    }

    #[test]
    fn test_get_format_from_path_toml() {
        assert_eq!(get_format_from_path("config.toml"), Some(ConfigFormat::TOML));
        assert_eq!(get_format_from_path("/path/to/config.TOML"), Some(ConfigFormat::TOML));
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
        assert_eq!(config.health_check.enabled, false);
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
        assert_eq!(config.health_check.enabled, true);
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
        assert_eq!(config.health_check.enabled, false);
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
            logging: LoggingConfig::default(),
            health_check: HealthCheckConfig::default(),
        };

        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: EngineConfig = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.logging.level, "info");
        assert!(matches!(deserialized.logging.format, LogFormat::PLAIN));
        assert_eq!(deserialized.health_check.enabled, true);
        assert_eq!(deserialized.health_check.address, "0.0.0.0:8080");
    }
}
