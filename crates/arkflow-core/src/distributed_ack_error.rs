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

//! Unified error handling for distributed acknowledgment system

use crate::Error;

/// Unified error types for distributed acknowledgment system
#[derive(Debug, Clone, thiserror::Error)]
pub enum DistributedAckError {
    /// Configuration validation errors
    #[error("Configuration error: {0}")]
    Config(String),

    /// Storage operation errors
    #[error("Storage error: {0}")]
    Storage(String),

    /// Network communication errors
    #[error("Network error: {0}")]
    Network(String),

    /// Node registry errors
    #[error("Node registry error: {0}")]
    NodeRegistry(String),

    /// Recovery operation errors
    #[error("Recovery error: {0}")]
    Recovery(String),

    /// Checkpoint operation errors
    #[error("Checkpoint error: {0}")]
    Checkpoint(String),

    /// Serialization/deserialization errors
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Timeout errors
    #[error("Timeout error: {0}")]
    Timeout(String),

    /// Backpressure errors
    #[error("Backpressure active: {0}")]
    Backpressure(String),

    /// Validation errors
    #[error("Validation error: {0}")]
    Validation(String),

    /// Resource exhaustion errors
    #[error("Resource exhausted: {0}")]
    ResourceExhausted(String),

    /// Consistency check errors
    #[error("Consistency error: {0}")]
    Consistency(String),

    /// Retry operation errors
    #[error("Retry error: {0}")]
    Retry(String),
}

impl DistributedAckError {
    /// Create configuration error
    pub fn config(msg: impl Into<String>) -> Self {
        DistributedAckError::Config(msg.into())
    }

    /// Create storage error
    pub fn storage(msg: impl Into<String>) -> Self {
        DistributedAckError::Storage(msg.into())
    }

    /// Create network error
    pub fn network(msg: impl Into<String>) -> Self {
        DistributedAckError::Network(msg.into())
    }

    /// Create timeout error
    pub fn timeout(msg: impl Into<String>) -> Self {
        DistributedAckError::Timeout(msg.into())
    }

    /// Create backpressure error
    pub fn backpressure(msg: impl Into<String>) -> Self {
        DistributedAckError::Backpressure(msg.into())
    }

    /// Create validation error
    pub fn validation(msg: impl Into<String>) -> Self {
        DistributedAckError::Validation(msg.into())
    }

    /// Create resource exhausted error
    pub fn resource_exhausted(msg: impl Into<String>) -> Self {
        DistributedAckError::ResourceExhausted(msg.into())
    }
}

impl From<DistributedAckError> for Error {
    fn from(err: DistributedAckError) -> Self {
        Error::Unknown(err.to_string())
    }
}

/// Result type for distributed acknowledgment operations
pub type DistributedAckResult<T> = Result<T, DistributedAckError>;

/// Retry configuration
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
    pub backoff_multiplier: f64,
    pub jitter: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 5,
            base_delay_ms: 1000,
            max_delay_ms: 30000,
            backoff_multiplier: 2.0,
            jitter: true,
        }
    }
}

impl RetryConfig {
    /// Calculate next retry delay with exponential backoff and optional jitter
    pub fn next_delay(&self, attempt: u32) -> std::time::Duration {
        if attempt >= self.max_retries {
            return std::time::Duration::from_millis(self.max_delay_ms);
        }

        let delay_ms = (self.base_delay_ms as f64 * self.backoff_multiplier.powi(attempt as i32))
            .min(self.max_delay_ms as f64) as u64;

        let final_delay_ms = if self.jitter {
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

        std::time::Duration::from_millis(final_delay_ms.max(0))
    }

    /// Should retry based on error type and attempt count
    pub fn should_retry(&self, error: &DistributedAckError, attempt: u32) -> bool {
        if attempt >= self.max_retries {
            return false;
        }

        match error {
            // Don't retry configuration or validation errors
            DistributedAckError::Config(_) | DistributedAckError::Validation(_) => false,
            // Don't retry resource exhausted errors
            DistributedAckError::ResourceExhausted(_) => false,
            // Retry other errors
            _ => true,
        }
    }
}

/// Error severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ErrorSeverity {
    /// Informational messages
    Info = 0,
    /// Warning conditions
    Warning = 1,
    /// Error conditions
    Error = 2,
    /// Critical errors
    Critical = 3,
}

impl ErrorSeverity {
    /// Convert to log level
    pub fn to_log_level(&self) -> tracing::Level {
        match self {
            ErrorSeverity::Info => tracing::Level::INFO,
            ErrorSeverity::Warning => tracing::Level::WARN,
            ErrorSeverity::Error => tracing::Level::ERROR,
            ErrorSeverity::Critical => tracing::Level::ERROR,
        }
    }
}

/// Enhanced error context
#[derive(Debug, Clone)]
pub struct ErrorContext {
    pub error: DistributedAckError,
    pub severity: ErrorSeverity,
    pub operation: String,
    pub component: String,
    pub timestamp: std::time::SystemTime,
    pub retry_count: u32,
    pub metadata: std::collections::HashMap<String, String>,
}

impl ErrorContext {
    pub fn new(error: DistributedAckError, operation: String, component: String) -> Self {
        let severity = Self::default_severity(&error);
        Self {
            error,
            severity,
            operation,
            component,
            timestamp: std::time::SystemTime::now(),
            retry_count: 0,
            metadata: std::collections::HashMap::new(),
        }
    }

    fn default_severity(error: &DistributedAckError) -> ErrorSeverity {
        match error {
            DistributedAckError::Config(_) | DistributedAckError::Validation(_) => {
                ErrorSeverity::Error
            }
            DistributedAckError::ResourceExhausted(_) => ErrorSeverity::Warning,
            DistributedAckError::Timeout(_) => ErrorSeverity::Warning,
            DistributedAckError::Backpressure(_) => ErrorSeverity::Info,
            _ => ErrorSeverity::Error,
        }
    }

    pub fn with_severity(mut self, severity: ErrorSeverity) -> Self {
        self.severity = severity;
        self
    }

    pub fn with_retry_count(mut self, retry_count: u32) -> Self {
        self.retry_count = retry_count;
        self
    }

    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }

    pub fn log(&self) {
        let level = self.severity.to_log_level();
        let message = format!(
            "[{}] {} failed: {} (retry: {})",
            self.component, self.operation, self.error, self.retry_count
        );

        if !self.metadata.is_empty() {
            let metadata_str = self
                .metadata
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join(", ");
            match level {
                tracing::Level::ERROR => tracing::error!("{} | {}", message, metadata_str),
                tracing::Level::WARN => tracing::warn!("{} | {}", message, metadata_str),
                tracing::Level::INFO => tracing::info!("{} | {}", message, metadata_str),
                tracing::Level::DEBUG => tracing::debug!("{} | {}", message, metadata_str),
                tracing::Level::TRACE => tracing::trace!("{} | {}", message, metadata_str),
            }
        } else {
            match level {
                tracing::Level::ERROR => tracing::error!("{}", message),
                tracing::Level::WARN => tracing::warn!("{}", message),
                tracing::Level::INFO => tracing::info!("{}", message),
                tracing::Level::DEBUG => tracing::debug!("{}", message),
                tracing::Level::TRACE => tracing::trace!("{}", message),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_config_next_delay() {
        let config = RetryConfig {
            base_delay_ms: 1000,
            backoff_multiplier: 2.0,
            max_delay_ms: 16000,
            jitter: false,
            ..Default::default()
        };

        assert_eq!(config.next_delay(0).as_millis(), 1000);
        assert_eq!(config.next_delay(1).as_millis(), 2000);
        assert_eq!(config.next_delay(2).as_millis(), 4000);
        assert_eq!(config.next_delay(3).as_millis(), 8000);
        assert_eq!(config.next_delay(4).as_millis(), 16000);
        assert_eq!(config.next_delay(5).as_millis(), 16000); // max_delay
    }

    #[test]
    fn test_retry_config_should_retry() {
        let config = RetryConfig::default();
        let config_error = DistributedAckError::config("bad config");
        let network_error = DistributedAckError::network("connection failed");

        assert!(!config.should_retry(&config_error, 0));
        assert!(config.should_retry(&network_error, 0));
        assert!(!config.should_retry(&network_error, 10)); // max_retries exceeded
    }

    #[test]
    fn test_error_context() {
        let error = DistributedAckError::network("connection failed");
        let context = ErrorContext::new(
            error,
            "connect_to_node".to_string(),
            "NodeRegistry".to_string(),
        )
        .with_retry_count(3)
        .with_metadata("node_id".to_string(), "node-1".to_string());

        assert_eq!(context.retry_count, 3);
        assert_eq!(context.metadata.get("node_id"), Some(&"node-1".to_string()));
        assert_eq!(context.severity, ErrorSeverity::Error);
    }
}
