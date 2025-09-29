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

//! Unit tests for enhanced configuration management

use super::*;

#[test]
fn test_performance_config_validation() {
    let config = PerformanceConfig::default();
    assert!(config.validate().is_ok());

    // Test invalid batch size
    let mut config = PerformanceConfig::default();
    config.batch_size = 0;
    assert!(config.validate().is_err());

    // Test invalid backpressure threshold
    let mut config = PerformanceConfig::default();
    config.backpressure_threshold_percentage = 150;
    assert!(config.validate().is_err());
}

#[test]
fn test_adaptive_batch_size() {
    let config = PerformanceConfig {
        batch_size: 100,
        max_pending_acks: 1000,
        enable_adaptive_batching: true,
        ..Default::default()
    };

    // Test under low load
    assert_eq!(config.adaptive_batch_size(100), 100);

    // Test under high load
    let high_load_batch = config.adaptive_batch_size(800);
    assert!(high_load_batch < 100);
    assert!(high_load_batch > 0);
}

#[test]
fn test_retry_config_delay_calculation() {
    let config = EnhancedRetryConfig::default();

    let delay1 = config.next_delay(0);
    let delay2 = config.next_delay(1);

    assert!(delay2 > delay1); // Exponential backoff
}

#[test]
fn test_retryable_error_classification() {
    let config = EnhancedRetryConfig::default();

    assert!(config.is_retryable_error("network"));
    assert!(config.is_retryable_error("timeout"));
    assert!(!config.is_retryable_error("config"));
    assert!(!config.is_retryable_error("validation"));
    assert!(config.is_retryable_error("unknown")); // Default retryable
}

#[test]
fn test_enhanced_config_environments() {
    let dev_config = EnhancedConfig::development();
    let prod_config = EnhancedConfig::production();

    // Development should have smaller batch sizes
    assert!(dev_config.performance.batch_size < prod_config.performance.batch_size);

    // Production should have higher retry limits
    assert!(prod_config.retry.max_retries >= dev_config.retry.max_retries);

    // Development should have debug mode enabled
    assert!(dev_config.debug_mode);
    assert!(!prod_config.debug_mode);
}

#[test]
fn test_config_validation() {
    let config = EnhancedConfig::default();
    assert!(config.validate().is_ok());

    // Test invalid environment
    let mut config = EnhancedConfig::default();
    config.environment = "".to_string();
    assert!(config.validate().is_err());
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
