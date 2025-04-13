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
#[derive(Debug, Clone, Copy)]
pub enum ConfigFormat {
    YAML,
    JSON,
    TOML,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    JSON,
    TEXT,
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
    LogFormat::TEXT
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
