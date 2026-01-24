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

//! Core metric definitions
//!
//! This module defines all Prometheus metrics used throughout ArkFlow.

use once_cell::sync::Lazy;
use prometheus::{Counter, Gauge, Histogram};

/// ========== Throughput Metrics (Counters) ==========

/// Total number of messages processed
pub static MESSAGES_PROCESSED: Lazy<Counter> = Lazy::new(|| {
    Counter::new(
        "arkflow_messages_processed_total",
        "Total number of messages processed",
    )
    .expect("metric should be valid")
});

/// Total number of bytes processed
pub static BYTES_PROCESSED: Lazy<Counter> = Lazy::new(|| {
    Counter::new(
        "arkflow_bytes_processed_total",
        "Total number of bytes processed",
    )
    .expect("metric should be valid")
});

/// Total number of batches processed
pub static BATCHES_PROCESSED: Lazy<Counter> = Lazy::new(|| {
    Counter::new(
        "arkflow_batches_processed_total",
        "Total number of batches processed",
    )
    .expect("metric should be valid")
});

/// ========== Error Metrics (Counters) ==========

/// Total number of errors
pub static ERRORS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    Counter::new("arkflow_errors_total", "Total number of errors").expect("metric should be valid")
});

/// Total number of retry attempts
pub static RETRY_TOTAL: Lazy<Counter> = Lazy::new(|| {
    Counter::new("arkflow_retries_total", "Total number of retry attempts")
        .expect("metric should be valid")
});

/// ========== Queue/Buffer Metrics (Gauges) ==========

/// Number of messages in input queue
pub static INPUT_QUEUE_DEPTH: Lazy<Gauge> = Lazy::new(|| {
    Gauge::new(
        "arkflow_input_queue_depth",
        "Number of messages in input queue",
    )
    .expect("metric should be valid")
});

/// Number of messages in output queue
pub static OUTPUT_QUEUE_DEPTH: Lazy<Gauge> = Lazy::new(|| {
    Gauge::new(
        "arkflow_output_queue_depth",
        "Number of messages in output queue",
    )
    .expect("metric should be valid")
});

/// Whether backpressure is active (1 = active, 0 = inactive)
pub static BACKPRESSURE_ACTIVE: Lazy<Gauge> = Lazy::new(|| {
    Gauge::new(
        "arkflow_backpressure_active",
        "Whether backpressure is currently active (1 = active, 0 = inactive)",
    )
    .expect("metric should be valid")
});

/// ========== Latency Metrics (Histograms) ==========

/// Message processing latency in milliseconds
pub static PROCESSING_LATENCY_MS: Lazy<Histogram> = Lazy::new(|| {
    Histogram::with_opts(
        prometheus::HistogramOpts::new(
            "arkflow_processing_latency_ms",
            "Message processing latency in milliseconds",
        )
        .buckets(vec![
            1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0,
        ]),
    )
    .expect("metric should be valid")
});

/// End-to-end latency in milliseconds
pub static END_TO_END_LATENCY_MS: Lazy<Histogram> = Lazy::new(|| {
    Histogram::with_opts(
        prometheus::HistogramOpts::new(
            "arkflow_end_to_end_latency_ms",
            "End-to-end message latency in milliseconds",
        )
        .buckets(vec![
            1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0,
        ]),
    )
    .expect("metric should be valid")
});

/// ========== Kafka-Specific Metrics ==========

/// Kafka consumer lag by topic and partition
pub static KAFKA_CONSUMER_LAG: Lazy<Histogram> = Lazy::new(|| {
    Histogram::with_opts(
        prometheus::HistogramOpts::new(
            "arkflow_kafka_consumer_lag",
            "Kafka consumer lag by topic and partition",
        )
        .buckets(vec![0.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0, 1000000.0]),
    )
    .expect("metric should be valid")
});

/// Kafka fetch rate (records per second)
pub static KAFKA_FETCH_RATE: Lazy<Histogram> = Lazy::new(|| {
    Histogram::with_opts(
        prometheus::HistogramOpts::new(
            "arkflow_kafka_fetch_rate",
            "Kafka fetch rate in records per second",
        )
        .buckets(vec![1.0, 10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0, 10000.0]),
    )
    .expect("metric should be valid")
});

/// Kafka commit rate (offsets per second)
pub static KAFKA_COMMIT_RATE: Lazy<Histogram> = Lazy::new(|| {
    Histogram::with_opts(
        prometheus::HistogramOpts::new(
            "arkflow_kafka_commit_rate",
            "Kafka commit rate in offsets per second",
        )
        .buckets(vec![1.0, 10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0, 10000.0]),
    )
    .expect("metric should be valid")
});

/// ========== Buffer-Specific Metrics ==========

/// Current buffer size (number of messages)
pub static BUFFER_SIZE: Lazy<Gauge> = Lazy::new(|| {
    Gauge::new(
        "arkflow_buffer_size",
        "Current number of messages in buffer",
    )
    .expect("metric should be valid")
});

/// Active window count
pub static ACTIVE_WINDOWS: Lazy<Gauge> = Lazy::new(|| {
    Gauge::new("arkflow_active_windows", "Number of active windows")
        .expect("metric should be valid")
});

/// Buffer utilization percentage
pub static BUFFER_UTILIZATION: Lazy<Gauge> = Lazy::new(|| {
    Gauge::new(
        "arkflow_buffer_utilization",
        "Buffer utilization as percentage (0-100)",
    )
    .expect("metric should be valid")
});

/// ========== Output-Specific Metrics ==========

/// Output write rate (messages per second)
pub static OUTPUT_WRITE_RATE: Lazy<Histogram> = Lazy::new(|| {
    Histogram::with_opts(
        prometheus::HistogramOpts::new(
            "arkflow_output_write_rate",
            "Output write rate in messages per second",
        )
        .buckets(vec![1.0, 10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0, 10000.0]),
    )
    .expect("metric should be valid")
});

/// Output bytes rate (bytes per second)
pub static OUTPUT_BYTES_RATE: Lazy<Histogram> = Lazy::new(|| {
    Histogram::with_opts(
        prometheus::HistogramOpts::new(
            "arkflow_output_bytes_rate",
            "Output write rate in bytes per second",
        )
        .buckets(vec![
            1024.0,
            10240.0,
            102400.0,
            1048576.0,
            10485760.0,
            104857600.0,
        ]),
    )
    .expect("metric should be valid")
});

/// Output connection status (1=connected, 0=disconnected)
pub static OUTPUT_CONNECTION_STATUS: Lazy<Gauge> = Lazy::new(|| {
    Gauge::new(
        "arkflow_output_connection_status",
        "Output connection status (1=connected, 0=disconnected)",
    )
    .expect("metric should be valid")
});

/// ========== System Resource Metrics ==========

/// Memory usage in bytes
pub static MEMORY_USAGE_BYTES: Lazy<Gauge> = Lazy::new(|| {
    Gauge::new("arkflow_memory_usage_bytes", "Memory usage in bytes")
        .expect("metric should be valid")
});

/// Active task count
pub static ACTIVE_TASKS: Lazy<Gauge> = Lazy::new(|| {
    Gauge::new("arkflow_active_tasks", "Number of active tasks").expect("metric should be valid")
});

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metric_creation() {
        // Test that all metrics can be created
        MESSAGES_PROCESSED.inc();
        BYTES_PROCESSED.inc();
        BATCHES_PROCESSED.inc();
        ERRORS_TOTAL.inc();
        RETRY_TOTAL.inc();

        INPUT_QUEUE_DEPTH.set(0.0);
        OUTPUT_QUEUE_DEPTH.set(0.0);
        BACKPRESSURE_ACTIVE.set(0.0);

        PROCESSING_LATENCY_MS.observe(1.0);
        END_TO_END_LATENCY_MS.observe(1.0);
    }
}
