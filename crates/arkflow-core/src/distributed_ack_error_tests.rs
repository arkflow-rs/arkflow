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

//! Unit tests for distributed acknowledgment error handling

use super::*;

#[test]
fn test_error_creation() {
    let config_error = DistributedAckError::config("invalid configuration");
    assert_eq!(
        config_error.to_string(),
        "Configuration error: invalid configuration"
    );

    let network_error = DistributedAckError::network("connection failed");
    assert_eq!(
        network_error.to_string(),
        "Network error: connection failed"
    );

    let timeout_error = DistributedAckError::timeout("operation timed out");
    assert_eq!(
        timeout_error.to_string(),
        "Timeout error: operation timed out"
    );
}

#[test]
fn test_retry_config_validation() {
    let mut config = RetryConfig::default();

    // Test valid config
    assert!(config.next_delay(0) <= config.next_delay(1)); // Exponential backoff

    // Test max retries
    assert!(config.should_retry(&DistributedAckError::network("temp"), 0));
    assert!(!config.should_retry(&DistributedAckError::network("temp"), 10));

    // Test non-retryable errors
    assert!(!config.should_retry(&DistributedAckError::config("bad"), 0));
    assert!(!config.should_retry(&DistributedAckError::validation("invalid"), 0));
}

#[test]
fn test_error_severity() {
    let config_error = DistributedAckError::config("bad config");
    let context = ErrorContext::new(
        config_error,
        "validate".to_string(),
        "ConfigManager".to_string(),
    );
    assert_eq!(context.severity, ErrorSeverity::Error);

    let backpressure_error = DistributedAckError::backpressure("high load");
    let context = ErrorContext::new(
        backpressure_error,
        "process".to_string(),
        "Processor".to_string(),
    );
    assert_eq!(context.severity, ErrorSeverity::Info);
}

#[test]
fn test_error_context_metadata() {
    let error = DistributedAckError::network("connection failed");
    let context = ErrorContext::new(error, "connect".to_string(), "Network".to_string())
        .with_metadata("node_id".to_string(), "node-1".to_string())
        .with_metadata("attempt".to_string(), "3".to_string());

    assert_eq!(context.metadata.len(), 2);
    assert_eq!(context.metadata.get("node_id"), Some(&"node-1".to_string()));
    assert_eq!(context.metadata.get("attempt"), Some(&"3".to_string()));
}

#[test]
fn test_retry_delay_calculation() {
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
    assert_eq!(config.next_delay(5).as_millis(), 16000); // Max delay
}

#[test]
fn test_error_clone() {
    let error = DistributedAckError::timeout("operation failed");
    let cloned_error = error.clone();
    assert_eq!(error.to_string(), cloned_error.to_string());
}
