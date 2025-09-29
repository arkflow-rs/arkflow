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

//! Enhanced configuration management for distributed acknowledgment system

use crate::distributed_ack_error::{DistributedAckError, DistributedAckResult};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// Enhanced performance configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    /// Batch size for processing acknowledgments
    pub batch_size: usize,
    /// Maximum number of pending acknowledgments
    pub max_pending_acks: usize,
    /// Backpressure threshold (percentage of max_pending_acks)
    pub backpressure_threshold_percentage: usize,
    /// Upload interval in milliseconds
    pub upload_interval_ms: u64,
    /// Timeout for acknowledgment processing in milliseconds
    pub ack_timeout_ms: u64,
    /// Maximum concurrent operations
    pub max_concurrent_operations: usize,
    /// Enable adaptive batching
    pub enable_adaptive_batching: bool,
    /// Target batch processing time in milliseconds
    pub target_batch_processing_time_ms: u64,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            max_pending_acks: 10000,
            backpressure_threshold_percentage: 60,
            upload_interval_ms: 5000,
            ack_timeout_ms: 10000,
            max_concurrent_operations: 50,
            enable_adaptive_batching: true,
            target_batch_processing_time_ms: 100,
        }
    }
}

impl PerformanceConfig {
    /// Validate performance configuration
    pub fn validate(&self) -> DistributedAckResult<()> {
        if self.batch_size == 0 {
            return Err(DistributedAckError::validation(
                "batch_size must be greater than 0",
            ));
        }

        if self.max_pending_acks < self.batch_size {
            return Err(DistributedAckError::validation(
                "max_pending_acks must be greater than or equal to batch_size",
            ));
        }

        if self.backpressure_threshold_percentage > 100 {
            return Err(DistributedAckError::validation(
                "backpressure_threshold_percentage must be between 0 and 100",
            ));
        }

        if self.ack_timeout_ms == 0 {
            return Err(DistributedAckError::validation(
                "ack_timeout_ms must be greater than 0",
            ));
        }

        Ok(())
    }

    /// Get backpressure threshold
    pub fn backpressure_threshold(&self) -> usize {
        (self.max_pending_acks * self.backpressure_threshold_percentage) / 100
    }

    /// Calculate adaptive batch size based on current load
    pub fn adaptive_batch_size(&self, current_pending: usize) -> usize {
        if !self.enable_adaptive_batching {
            return self.batch_size;
        }

        let load_ratio = current_pending as f64 / self.max_pending_acks as f64;
        let adjustment_factor = 1.0 - (load_ratio * 0.5); // Reduce batch size by up to 50% under high load

        (self.batch_size as f64 * adjustment_factor).max(1.0) as usize
    }
}

/// Enhanced retry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnhancedRetryConfig {
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Base delay in milliseconds
    pub base_delay_ms: u64,
    /// Maximum delay in milliseconds
    pub max_delay_ms: u64,
    /// Backoff multiplier
    pub backoff_multiplier: f64,
    /// Enable jitter
    pub enable_jitter: bool,
    /// Enable exponential backoff
    pub enable_exponential_backoff: bool,
    /// Retryable error types
    pub retryable_error_types: Vec<String>,
    /// Non-retryable error types
    pub non_retryable_error_types: Vec<String>,
}

impl Default for EnhancedRetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 5,
            base_delay_ms: 1000,
            max_delay_ms: 30000,
            backoff_multiplier: 2.0,
            enable_jitter: true,
            enable_exponential_backoff: true,
            retryable_error_types: vec![
                "network".to_string(),
                "timeout".to_string(),
                "storage".to_string(),
                "backpressure".to_string(),
            ],
            non_retryable_error_types: vec![
                "config".to_string(),
                "validation".to_string(),
                "resource_exhausted".to_string(),
            ],
        }
    }
}

impl EnhancedRetryConfig {
    /// Validate retry configuration
    pub fn validate(&self) -> DistributedAckResult<()> {
        if self.max_retries == 0 {
            return Err(DistributedAckError::validation(
                "max_retries must be greater than 0",
            ));
        }

        if self.base_delay_ms == 0 {
            return Err(DistributedAckError::validation(
                "base_delay_ms must be greater than 0",
            ));
        }

        if self.max_delay_ms < self.base_delay_ms {
            return Err(DistributedAckError::validation(
                "max_delay_ms must be greater than or equal to base_delay_ms",
            ));
        }

        if self.backoff_multiplier <= 1.0 && self.enable_exponential_backoff {
            return Err(DistributedAckError::validation(
                "backoff_multiplier must be greater than 1.0 when exponential backoff is enabled",
            ));
        }

        Ok(())
    }

    /// Check if an error type is retryable
    pub fn is_retryable_error(&self, error_type: &str) -> bool {
        // Check non-retryable first (explicit deny)
        if self
            .non_retryable_error_types
            .contains(&error_type.to_string())
        {
            return false;
        }

        // Check retryable (explicit allow)
        if self.retryable_error_types.contains(&error_type.to_string()) {
            return true;
        }

        // Default to retryable for unknown error types
        true
    }

    /// Calculate next retry delay
    pub fn next_delay(&self, attempt: u32) -> Duration {
        if attempt >= self.max_retries {
            return Duration::from_millis(self.max_delay_ms);
        }

        let delay_ms = if self.enable_exponential_backoff {
            (self.base_delay_ms as f64 * self.backoff_multiplier.powi(attempt as i32))
                .min(self.max_delay_ms as f64) as u64
        } else {
            self.base_delay_ms
        };

        let final_delay_ms = if self.enable_jitter {
            // Add ±25% jitter
            let jitter_range = (delay_ms as f64 * 0.25) as u64;
            delay_ms.saturating_add(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64
                    % (jitter_range * 2 + 1)
                    - jitter_range,
            )
        } else {
            delay_ms
        };

        Duration::from_millis(final_delay_ms.max(0))
    }
}

/// Enhanced monitoring configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringConfig {
    /// Enable metrics collection
    pub enable_metrics: bool,
    /// Metrics collection interval in seconds
    pub metrics_interval_seconds: u64,
    /// Enable health checks
    pub enable_health_checks: bool,
    /// Health check interval in seconds
    pub health_check_interval_seconds: u64,
    /// Enable performance profiling
    pub enable_profiling: bool,
    /// Metrics retention period in hours
    pub metrics_retention_hours: u64,
    /// Enable detailed logging
    pub enable_detailed_logging: bool,
    /// Log level
    pub log_level: String,
    /// Enable Prometheus metrics export
    pub enable_prometheus_export: bool,
    /// Prometheus export port
    pub prometheus_export_port: u16,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            enable_metrics: true,
            metrics_interval_seconds: 30,
            enable_health_checks: true,
            health_check_interval_seconds: 10,
            enable_profiling: false,
            metrics_retention_hours: 24,
            enable_detailed_logging: false,
            log_level: "info".to_string(),
            enable_prometheus_export: false,
            prometheus_export_port: 9090,
        }
    }
}

impl MonitoringConfig {
    /// Validate monitoring configuration
    pub fn validate(&self) -> DistributedAckResult<()> {
        if self.metrics_interval_seconds == 0 {
            return Err(DistributedAckError::validation(
                "metrics_interval_seconds must be greater than 0",
            ));
        }

        if self.health_check_interval_seconds == 0 {
            return Err(DistributedAckError::validation(
                "health_check_interval_seconds must be greater than 0",
            ));
        }

        if self.prometheus_export_port == 0 {
            return Err(DistributedAckError::validation(
                "prometheus_export_port must be greater than 0",
            ));
        }

        Ok(())
    }

    /// Get log level as tracing::Level
    pub fn log_level(&self) -> tracing::Level {
        match self.log_level.as_str() {
            "trace" => tracing::Level::TRACE,
            "debug" => tracing::Level::DEBUG,
            "info" => tracing::Level::INFO,
            "warn" => tracing::Level::WARN,
            "error" => tracing::Level::ERROR,
            _ => tracing::Level::INFO,
        }
    }
}

/// Enhanced resource configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceConfig {
    /// Maximum memory usage in MB
    pub max_memory_mb: usize,
    /// Maximum CPU usage percentage
    pub max_cpu_percentage: usize,
    /// Enable memory pressure monitoring
    pub enable_memory_pressure_monitoring: bool,
    /// Enable CPU throttling
    pub enable_cpu_throttling: bool,
    /// Garbage collection interval in seconds
    pub gc_interval_seconds: u64,
    /// Temporary directory path
    pub temp_directory: PathBuf,
    /// Enable resource limits
    pub enable_resource_limits: bool,
}

impl Default for ResourceConfig {
    fn default() -> Self {
        Self {
            max_memory_mb: 512,
            max_cpu_percentage: 80,
            enable_memory_pressure_monitoring: true,
            enable_cpu_throttling: true,
            gc_interval_seconds: 300,
            temp_directory: std::env::temp_dir(),
            enable_resource_limits: true,
        }
    }
}

impl ResourceConfig {
    /// Validate resource configuration
    pub fn validate(&self) -> DistributedAckResult<()> {
        if self.max_memory_mb == 0 {
            return Err(DistributedAckError::validation(
                "max_memory_mb must be greater than 0",
            ));
        }

        if self.max_cpu_percentage > 100 {
            return Err(DistributedAckError::validation(
                "max_cpu_percentage must be between 0 and 100",
            ));
        }

        if self.gc_interval_seconds == 0 {
            return Err(DistributedAckError::validation(
                "gc_interval_seconds must be greater than 0",
            ));
        }

        Ok(())
    }
}

/// Complete enhanced configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnhancedConfig {
    /// Performance configuration
    pub performance: PerformanceConfig,
    /// Retry configuration
    pub retry: EnhancedRetryConfig,
    /// Monitoring configuration
    pub monitoring: MonitoringConfig,
    /// Resource configuration
    pub resources: ResourceConfig,
    /// Environment (development, staging, production)
    pub environment: String,
    /// Enable debug mode
    pub debug_mode: bool,
    /// Configuration version
    pub version: String,
}

impl Default for EnhancedConfig {
    fn default() -> Self {
        Self {
            performance: PerformanceConfig::default(),
            retry: EnhancedRetryConfig::default(),
            monitoring: MonitoringConfig::default(),
            resources: ResourceConfig::default(),
            environment: "development".to_string(),
            debug_mode: false,
            version: "1.0.0".to_string(),
        }
    }
}

impl EnhancedConfig {
    /// Create development configuration
    pub fn development() -> Self {
        Self {
            performance: PerformanceConfig {
                batch_size: 50,
                max_pending_acks: 1000,
                backpressure_threshold_percentage: 70,
                upload_interval_ms: 10000,
                ack_timeout_ms: 30000,
                max_concurrent_operations: 10,
                enable_adaptive_batching: false,
                target_batch_processing_time_ms: 200,
            },
            retry: EnhancedRetryConfig {
                max_retries: 3,
                base_delay_ms: 2000,
                enable_jitter: false,
                ..Default::default()
            },
            monitoring: MonitoringConfig {
                enable_detailed_logging: true,
                log_level: "debug".to_string(),
                enable_profiling: true,
                ..Default::default()
            },
            resources: ResourceConfig {
                max_memory_mb: 256,
                max_cpu_percentage: 90,
                enable_resource_limits: false,
                ..Default::default()
            },
            environment: "development".to_string(),
            debug_mode: true,
            version: "1.0.0".to_string(),
        }
    }

    /// Create production configuration
    pub fn production() -> Self {
        Self {
            performance: PerformanceConfig {
                batch_size: 200,
                max_pending_acks: 50000,
                backpressure_threshold_percentage: 50,
                upload_interval_ms: 1000,
                ack_timeout_ms: 5000,
                max_concurrent_operations: 100,
                enable_adaptive_batching: true,
                target_batch_processing_time_ms: 50,
            },
            retry: EnhancedRetryConfig {
                max_retries: 5,
                base_delay_ms: 500,
                max_delay_ms: 60000,
                backoff_multiplier: 1.5,
                ..Default::default()
            },
            monitoring: MonitoringConfig {
                enable_prometheus_export: true,
                prometheus_export_port: 8080,
                enable_detailed_logging: false,
                log_level: "info".to_string(),
                ..Default::default()
            },
            resources: ResourceConfig {
                max_memory_mb: 2048,
                max_cpu_percentage: 70,
                enable_resource_limits: true,
                ..Default::default()
            },
            environment: "production".to_string(),
            debug_mode: false,
            version: "1.0.0".to_string(),
        }
    }

    /// Validate complete configuration
    pub fn validate(&self) -> DistributedAckResult<()> {
        self.performance.validate()?;
        self.retry.validate()?;
        self.monitoring.validate()?;
        self.resources.validate()?;

        if self.environment.is_empty() {
            return Err(DistributedAckError::validation(
                "environment cannot be empty",
            ));
        }

        if self.version.is_empty() {
            return Err(DistributedAckError::validation("version cannot be empty"));
        }

        Ok(())
    }

    /// Load configuration from file
    pub async fn from_file(path: &PathBuf) -> DistributedAckResult<Self> {
        let content = tokio::fs::read_to_string(path).await.map_err(|e| {
            DistributedAckError::config(format!("Failed to read config file: {}", e))
        })?;

        let config: Self = serde_json::from_str(&content).map_err(|e| {
            DistributedAckError::config(format!("Failed to parse config file: {}", e))
        })?;

        config.validate()?;
        Ok(config)
    }

    /// Save configuration to file
    pub async fn to_file(&self, path: &PathBuf) -> DistributedAckResult<()> {
        let content = serde_json::to_string_pretty(self).map_err(|e| {
            DistributedAckError::config(format!("Failed to serialize config: {}", e))
        })?;

        tokio::fs::write(path, content).await.map_err(|e| {
            DistributedAckError::config(format!("Failed to write config file: {}", e))
        })?;

        Ok(())
    }

    /// Merge with another configuration (other takes precedence)
    pub fn merge(&self, other: &Self) -> Self {
        Self {
            performance: other.performance.clone(),
            retry: other.retry.clone(),
            monitoring: other.monitoring.clone(),
            resources: other.resources.clone(),
            environment: if other.environment != "development" {
                other.environment.clone()
            } else {
                self.environment.clone()
            },
            debug_mode: other.debug_mode,
            version: if other.version != "1.0.0" {
                other.version.clone()
            } else {
                self.version.clone()
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_performance_config_validation() {
        let mut config = PerformanceConfig::default();
        assert!(config.validate().is_ok());

        config.batch_size = 0;
        assert!(config.validate().is_err());

        config.batch_size = 100;
        config.backpressure_threshold_percentage = 150;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_retry_config_next_delay() {
        let config = EnhancedRetryConfig::default();

        let delay1 = config.next_delay(0);
        let delay2 = config.next_delay(1);

        assert!(delay2 > delay1); // Exponential backoff
    }

    #[test]
    fn test_retryable_error_types() {
        let config = EnhancedRetryConfig::default();

        assert!(config.is_retryable_error("network"));
        assert!(!config.is_retryable_error("config"));
        assert!(config.is_retryable_error("unknown")); // Default retryable
    }

    #[tokio::test]
    async fn test_config_file_operations() {
        let config = EnhancedConfig::development();

        // Test save and load
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();

        config.to_file(&path).await.unwrap();
        let loaded_config = EnhancedConfig::from_file(&path).await.unwrap();

        assert_eq!(config.environment, loaded_config.environment);
        assert_eq!(config.debug_mode, loaded_config.debug_mode);
    }

    #[test]
    fn test_config_merge() {
        let config1 = EnhancedConfig::development();
        let mut config2 = EnhancedConfig::production();
        config2.environment = "staging".to_string();

        let merged = config1.merge(&config2);
        assert_eq!(merged.environment, "staging");
        assert_eq!(merged.performance.batch_size, 200); // From production
    }
}
