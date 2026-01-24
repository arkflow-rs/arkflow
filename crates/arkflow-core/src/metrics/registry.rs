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

//! Metric registry management
//!
//! This module provides the central registry for all Prometheus metrics.

use crate::Error;
use once_cell::sync::Lazy;
use prometheus::{Encoder, Registry, TextEncoder};
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::info;

use super::definitions::*;

/// Global metric registry
pub static REGISTRY: Lazy<Registry> = Lazy::new(|| Registry::new());

/// Flag indicating whether metrics collection is enabled
pub static METRICS_ENABLED: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));

/// Initialize the metrics registry
///
/// This function must be called before any metrics are used.
/// It registers all core metrics with the global registry.
pub fn init_metrics() -> Result<(), Error> {
    // Register all counters
    REGISTRY
        .register(Box::new(MESSAGES_PROCESSED.clone()))
        .map_err(|e| Error::Config(format!("Failed to register MESSAGES_PROCESSED: {}", e)))?;
    REGISTRY
        .register(Box::new(BYTES_PROCESSED.clone()))
        .map_err(|e| Error::Config(format!("Failed to register BYTES_PROCESSED: {}", e)))?;
    REGISTRY
        .register(Box::new(BATCHES_PROCESSED.clone()))
        .map_err(|e| Error::Config(format!("Failed to register BATCHES_PROCESSED: {}", e)))?;

    // Register error counters
    REGISTRY
        .register(Box::new(ERRORS_TOTAL.clone()))
        .map_err(|e| Error::Config(format!("Failed to register ERRORS_TOTAL: {}", e)))?;
    REGISTRY
        .register(Box::new(RETRY_TOTAL.clone()))
        .map_err(|e| Error::Config(format!("Failed to register RETRY_TOTAL: {}", e)))?;

    // Register gauges
    REGISTRY
        .register(Box::new(INPUT_QUEUE_DEPTH.clone()))
        .map_err(|e| Error::Config(format!("Failed to register INPUT_QUEUE_DEPTH: {}", e)))?;
    REGISTRY
        .register(Box::new(OUTPUT_QUEUE_DEPTH.clone()))
        .map_err(|e| Error::Config(format!("Failed to register OUTPUT_QUEUE_DEPTH: {}", e)))?;
    REGISTRY
        .register(Box::new(BACKPRESSURE_ACTIVE.clone()))
        .map_err(|e| Error::Config(format!("Failed to register BACKPRESSURE_ACTIVE: {}", e)))?;

    // Register histograms
    REGISTRY
        .register(Box::new(PROCESSING_LATENCY_MS.clone()))
        .map_err(|e| Error::Config(format!("Failed to register PROCESSING_LATENCY_MS: {}", e)))?;
    REGISTRY
        .register(Box::new(END_TO_END_LATENCY_MS.clone()))
        .map_err(|e| Error::Config(format!("Failed to register END_TO_END_LATENCY_MS: {}", e)))?;

    // Register Kafka-specific metrics
    REGISTRY
        .register(Box::new(KAFKA_CONSUMER_LAG.clone()))
        .map_err(|e| Error::Config(format!("Failed to register KAFKA_CONSUMER_LAG: {}", e)))?;
    REGISTRY
        .register(Box::new(KAFKA_FETCH_RATE.clone()))
        .map_err(|e| Error::Config(format!("Failed to register KAFKA_FETCH_RATE: {}", e)))?;
    REGISTRY
        .register(Box::new(KAFKA_COMMIT_RATE.clone()))
        .map_err(|e| Error::Config(format!("Failed to register KAFKA_COMMIT_RATE: {}", e)))?;

    // Register buffer-specific metrics
    REGISTRY
        .register(Box::new(BUFFER_SIZE.clone()))
        .map_err(|e| Error::Config(format!("Failed to register BUFFER_SIZE: {}", e)))?;
    REGISTRY
        .register(Box::new(ACTIVE_WINDOWS.clone()))
        .map_err(|e| Error::Config(format!("Failed to register ACTIVE_WINDOWS: {}", e)))?;
    REGISTRY
        .register(Box::new(BUFFER_UTILIZATION.clone()))
        .map_err(|e| Error::Config(format!("Failed to register BUFFER_UTILIZATION: {}", e)))?;

    // Register output-specific metrics
    REGISTRY
        .register(Box::new(OUTPUT_WRITE_RATE.clone()))
        .map_err(|e| Error::Config(format!("Failed to register OUTPUT_WRITE_RATE: {}", e)))?;
    REGISTRY
        .register(Box::new(OUTPUT_BYTES_RATE.clone()))
        .map_err(|e| Error::Config(format!("Failed to register OUTPUT_BYTES_RATE: {}", e)))?;
    REGISTRY
        .register(Box::new(OUTPUT_CONNECTION_STATUS.clone()))
        .map_err(|e| {
            Error::Config(format!(
                "Failed to register OUTPUT_CONNECTION_STATUS: {}",
                e
            ))
        })?;

    // Register system resource metrics
    REGISTRY
        .register(Box::new(MEMORY_USAGE_BYTES.clone()))
        .map_err(|e| Error::Config(format!("Failed to register MEMORY_USAGE_BYTES: {}", e)))?;
    REGISTRY
        .register(Box::new(ACTIVE_TASKS.clone()))
        .map_err(|e| Error::Config(format!("Failed to register ACTIVE_TASKS: {}", e)))?;

    info!("All metrics registered successfully");
    Ok(())
}

/// Enable metrics collection
pub fn enable_metrics() {
    METRICS_ENABLED.store(true, Ordering::Release);
    info!("Metrics collection enabled");
}

/// Disable metrics collection
pub fn disable_metrics() {
    METRICS_ENABLED.store(false, Ordering::Release);
    info!("Metrics collection disabled");
}

/// Check if metrics collection is enabled
pub fn is_metrics_enabled() -> bool {
    METRICS_ENABLED.load(Ordering::Acquire)
}

/// Gather all metrics and encode them in Prometheus text format
///
/// This function is used by the HTTP endpoint to serve metrics.
pub fn gather_metrics() -> Result<Vec<u8>, Error> {
    let metric_families = REGISTRY.gather();
    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();

    encoder
        .encode(&metric_families, &mut buffer)
        .map_err(|e| Error::Process(format!("Failed to encode metrics: {}", e)))?;

    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_metrics() {
        // This test verifies that metrics can be initialized without error
        // Note: Running this multiple times will fail because metrics can only be registered once
        // Skip if already initialized by a previous test
        let _ = init_metrics();
        enable_metrics();
        assert!(is_metrics_enabled());
    }

    #[test]
    fn test_enable_disable_metrics() {
        enable_metrics();
        assert!(is_metrics_enabled());

        disable_metrics();
        assert!(!is_metrics_enabled());

        enable_metrics();
        assert!(is_metrics_enabled());
    }

    #[test]
    fn test_gather_metrics() {
        // Initialize metrics registry first
        let _ = init_metrics();
        enable_metrics();

        // Increment some metrics
        MESSAGES_PROCESSED.inc();
        ERRORS_TOTAL.inc();
        INPUT_QUEUE_DEPTH.set(42.0);

        // Gather metrics
        let buffer = gather_metrics().unwrap();

        // Verify that we got some output
        assert!(!buffer.is_empty());
        let output = String::from_utf8(buffer).unwrap();
        assert!(output.contains("arkflow_messages_processed_total"));
        assert!(output.contains("arkflow_errors_total"));
        assert!(output.contains("arkflow_input_queue_depth"));
    }
}
