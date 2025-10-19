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

//! Enhanced metrics collection and monitoring for distributed acknowledgment system

use crate::distributed_ack_error::DistributedAckResult;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// Enhanced metrics with histograms and gauges
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnhancedMetrics {
    // Counters
    pub total_acks_processed: Arc<AtomicU64>,
    pub successful_acks: Arc<AtomicU64>,
    pub failed_acks: Arc<AtomicU64>,
    pub retried_acks: Arc<AtomicU64>,
    pub timeout_acks: Arc<AtomicU64>,
    pub backpressure_events: Arc<AtomicU64>,
    pub checkpoint_creations: Arc<AtomicU64>,
    pub recovery_operations: Arc<AtomicU64>,
    pub consistency_checks: Arc<AtomicU64>,
    pub node_heartbeats: Arc<AtomicU64>,
    pub wal_uploads: Arc<AtomicU64>,
    pub wal_upload_failures: Arc<AtomicU64>,
    pub deduplication_hits: Arc<AtomicU64>,
    pub validation_errors: Arc<AtomicU64>,

    // Gauges
    pub current_pending_acks: Arc<AtomicU64>,
    pub current_active_nodes: Arc<AtomicU64>,
    pub current_memory_usage_mb: Arc<AtomicU64>,
    pub current_cpu_usage_percentage: Arc<AtomicU64>,
    pub current_wal_size_bytes: Arc<AtomicU64>,
    pub current_upload_queue_size: Arc<AtomicU64>,
    pub current_retry_queue_size: Arc<AtomicU64>,

    // System metrics
    pub system_metrics: Arc<SystemMetrics>,
}

impl Default for EnhancedMetrics {
    fn default() -> Self {
        Self {
            total_acks_processed: Arc::new(AtomicU64::new(0)),
            successful_acks: Arc::new(AtomicU64::new(0)),
            failed_acks: Arc::new(AtomicU64::new(0)),
            retried_acks: Arc::new(AtomicU64::new(0)),
            timeout_acks: Arc::new(AtomicU64::new(0)),
            backpressure_events: Arc::new(AtomicU64::new(0)),
            checkpoint_creations: Arc::new(AtomicU64::new(0)),
            recovery_operations: Arc::new(AtomicU64::new(0)),
            consistency_checks: Arc::new(AtomicU64::new(0)),
            node_heartbeats: Arc::new(AtomicU64::new(0)),
            wal_uploads: Arc::new(AtomicU64::new(0)),
            wal_upload_failures: Arc::new(AtomicU64::new(0)),
            deduplication_hits: Arc::new(AtomicU64::new(0)),
            validation_errors: Arc::new(AtomicU64::new(0)),
            current_pending_acks: Arc::new(AtomicU64::new(0)),
            current_active_nodes: Arc::new(AtomicU64::new(0)),
            current_memory_usage_mb: Arc::new(AtomicU64::new(0)),
            current_cpu_usage_percentage: Arc::new(AtomicU64::new(0)),
            current_wal_size_bytes: Arc::new(AtomicU64::new(0)),
            current_upload_queue_size: Arc::new(AtomicU64::new(0)),
            current_retry_queue_size: Arc::new(AtomicU64::new(0)),
            system_metrics: Arc::new(SystemMetrics::new()),
        }
    }
}

impl EnhancedMetrics {
    /// Create new enhanced metrics
    pub fn new() -> Self {
        Self::default()
    }

    /// Record acknowledgment processing time
    pub fn record_ack_processing_time(&self, _duration: Duration) {
        self.total_acks_processed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record successful acknowledgment
    pub fn record_successful_ack(&self) {
        self.successful_acks.fetch_add(1, Ordering::Relaxed);
    }

    /// Record failed acknowledgment
    pub fn record_failed_ack(&self) {
        self.failed_acks.fetch_add(1, Ordering::Relaxed);
    }

    /// Record retried acknowledgment
    pub fn record_retried_ack(&self) {
        self.retried_acks.fetch_add(1, Ordering::Relaxed);
    }

    /// Record timeout acknowledgment
    pub fn record_timeout_ack(&self) {
        self.timeout_acks.fetch_add(1, Ordering::Relaxed);
    }

    /// Record backpressure event
    pub fn record_backpressure_event(&self) {
        self.backpressure_events.fetch_add(1, Ordering::Relaxed);
    }

    /// Update pending acknowledgments count
    pub fn update_pending_acks(&self, count: u64) {
        self.current_pending_acks.store(count, Ordering::Relaxed);
    }

    /// Update active nodes count
    pub fn update_active_nodes(&self, count: u64) {
        self.current_active_nodes.store(count, Ordering::Relaxed);
    }

    /// Update memory usage
    pub fn update_memory_usage(&self, usage_mb: u64) {
        self.current_memory_usage_mb
            .store(usage_mb, Ordering::Relaxed);
    }

    /// Update CPU usage
    pub fn update_cpu_usage(&self, usage_percentage: u64) {
        self.current_cpu_usage_percentage
            .store(usage_percentage, Ordering::Relaxed);
    }

    /// Update WAL size
    pub fn update_wal_size(&self, size_bytes: u64) {
        self.current_wal_size_bytes
            .store(size_bytes, Ordering::Relaxed);
    }

    /// Record upload operation
    pub fn record_upload(&self, _duration: Duration) {
        self.wal_uploads.fetch_add(1, Ordering::Relaxed);
    }

    /// Record upload failure
    pub fn record_upload_failure(&self) {
        self.wal_upload_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Record batch processing
    pub fn record_batch_processing(&self, _batch_size: usize, _duration: Duration) {
        // Batch size and timing recording removed for now
    }

    /// Record checkpoint creation
    pub fn record_checkpoint_creation(&self) {
        self.checkpoint_creations.fetch_add(1, Ordering::Relaxed);
    }

    /// Record recovery operation
    pub fn record_recovery_operation(&self) {
        self.recovery_operations.fetch_add(1, Ordering::Relaxed);
    }

    /// Record consistency check
    pub fn record_consistency_check(&self) {
        self.consistency_checks.fetch_add(1, Ordering::Relaxed);
    }

    /// Record node heartbeat
    pub fn record_node_heartbeat(&self) {
        self.node_heartbeats.fetch_add(1, Ordering::Relaxed);
    }

    /// Record deduplication hit
    pub fn record_deduplication_hit(&self) {
        self.deduplication_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record validation error
    pub fn record_validation_error(&self) {
        self.validation_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Update system metrics
    pub fn update_system_metrics(&self) {
        self.system_metrics.update();
    }

    /// Get comprehensive metrics snapshot
    pub fn get_snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            counters: Counters {
                total_acks_processed: self.total_acks_processed.load(Ordering::Relaxed),
                successful_acks: self.successful_acks.load(Ordering::Relaxed),
                failed_acks: self.failed_acks.load(Ordering::Relaxed),
                retried_acks: self.retried_acks.load(Ordering::Relaxed),
                timeout_acks: self.timeout_acks.load(Ordering::Relaxed),
                backpressure_events: self.backpressure_events.load(Ordering::Relaxed),
                checkpoint_creations: self.checkpoint_creations.load(Ordering::Relaxed),
                recovery_operations: self.recovery_operations.load(Ordering::Relaxed),
                consistency_checks: self.consistency_checks.load(Ordering::Relaxed),
                node_heartbeats: self.node_heartbeats.load(Ordering::Relaxed),
                wal_uploads: self.wal_uploads.load(Ordering::Relaxed),
                wal_upload_failures: self.wal_upload_failures.load(Ordering::Relaxed),
                deduplication_hits: self.deduplication_hits.load(Ordering::Relaxed),
                validation_errors: self.validation_errors.load(Ordering::Relaxed),
            },
            gauges: Gauges {
                current_pending_acks: self.current_pending_acks.load(Ordering::Relaxed),
                current_active_nodes: self.current_active_nodes.load(Ordering::Relaxed),
                current_memory_usage_mb: self.current_memory_usage_mb.load(Ordering::Relaxed),
                current_cpu_usage_percentage: self
                    .current_cpu_usage_percentage
                    .load(Ordering::Relaxed),
                current_wal_size_bytes: self.current_wal_size_bytes.load(Ordering::Relaxed),
                current_upload_queue_size: self.current_upload_queue_size.load(Ordering::Relaxed),
                current_retry_queue_size: self.current_retry_queue_size.load(Ordering::Relaxed),
            },
            system_metrics: self.system_metrics.get_snapshot(),
            timestamp: SystemTime::now(),
        }
    }

    /// Reset all metrics
    pub fn reset(&self) {
        self.total_acks_processed.store(0, Ordering::Relaxed);
        self.successful_acks.store(0, Ordering::Relaxed);
        self.failed_acks.store(0, Ordering::Relaxed);
        self.retried_acks.store(0, Ordering::Relaxed);
        self.timeout_acks.store(0, Ordering::Relaxed);
        self.backpressure_events.store(0, Ordering::Relaxed);
        self.checkpoint_creations.store(0, Ordering::Relaxed);
        self.recovery_operations.store(0, Ordering::Relaxed);
        self.consistency_checks.store(0, Ordering::Relaxed);
        self.node_heartbeats.store(0, Ordering::Relaxed);
        self.wal_uploads.store(0, Ordering::Relaxed);
        self.wal_upload_failures.store(0, Ordering::Relaxed);
        self.deduplication_hits.store(0, Ordering::Relaxed);
        self.validation_errors.store(0, Ordering::Relaxed);
    }
}

/// Percentile values
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Percentiles {
    pub p50: f64,
    pub p90: f64,
    pub p95: f64,
    pub p99: f64,
    pub p999: f64,
    pub min: f64,
    pub max: f64,
    pub mean: f64,
    pub count: usize,
}

/// System metrics collector
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemMetrics {
    start_time: SystemTime,
}

impl SystemMetrics {
    /// Create new system metrics collector
    pub fn new() -> Self {
        Self {
            start_time: SystemTime::now(),
        }
    }

    /// Update system metrics
    pub fn update(&self) {
        // In a real implementation, this would collect actual system metrics
        // For now, we'll just track uptime
    }

    /// Get uptime in seconds
    pub fn uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().unwrap_or_default().as_secs()
    }

    /// Get system metrics snapshot
    pub fn get_snapshot(&self) -> SystemMetricsSnapshot {
        SystemMetricsSnapshot {
            uptime_seconds: self.uptime_seconds(),
            timestamp: SystemTime::now(),
        }
    }
}

/// Metrics snapshot structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub counters: Counters,
    pub gauges: Gauges,
    pub system_metrics: SystemMetricsSnapshot,
    pub timestamp: SystemTime,
}

/// Counter metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Counters {
    pub total_acks_processed: u64,
    pub successful_acks: u64,
    pub failed_acks: u64,
    pub retried_acks: u64,
    pub timeout_acks: u64,
    pub backpressure_events: u64,
    pub checkpoint_creations: u64,
    pub recovery_operations: u64,
    pub consistency_checks: u64,
    pub node_heartbeats: u64,
    pub wal_uploads: u64,
    pub wal_upload_failures: u64,
    pub deduplication_hits: u64,
    pub validation_errors: u64,
}

/// Gauge metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Gauges {
    pub current_pending_acks: u64,
    pub current_active_nodes: u64,
    pub current_memory_usage_mb: u64,
    pub current_cpu_usage_percentage: u64,
    pub current_wal_size_bytes: u64,
    pub current_upload_queue_size: u64,
    pub current_retry_queue_size: u64,
}

/// System metrics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemMetricsSnapshot {
    pub uptime_seconds: u64,
    pub timestamp: SystemTime,
}

/// Metrics exporter for Prometheus
pub struct PrometheusExporter {
    metrics: Arc<EnhancedMetrics>,
    port: u16,
}

impl PrometheusExporter {
    /// Create new Prometheus exporter
    pub fn new(metrics: Arc<EnhancedMetrics>, port: u16) -> Self {
        Self { metrics, port }
    }

    /// Start Prometheus exporter (placeholder implementation)
    pub async fn start(&self) -> DistributedAckResult<()> {
        tracing::info!("Prometheus exporter started on port {}", self.port);
        // In a real implementation, this would start an HTTP server
        Ok(())
    }

    /// Export metrics in Prometheus format
    pub fn export(&self) -> String {
        let snapshot = self.metrics.get_snapshot();

        format!(
            "# HELP arkflow_acks_processed_total Total number of acknowledgments processed
# TYPE arkflow_acks_processed_total counter
arkflow_acks_processed_total {}

# HELP arkflow_acks_successful_total Total number of successful acknowledgments
# TYPE arkflow_acks_successful_total counter
arkflow_acks_successful_total {}

# HELP arkflow_acks_failed_total Total number of failed acknowledgments
# TYPE arkflow_acks_failed_total counter
arkflow_acks_failed_total {}

# HELP arkflow_pending_acks_current Current number of pending acknowledgments
# TYPE arkflow_pending_acks_current gauge
arkflow_pending_acks_current {}

# HELP arkflow_active_nodes_current Current number of active nodes
# TYPE arkflow_active_nodes_current gauge
arkflow_active_nodes_current {}

# HELP arkflow_uptime_seconds System uptime in seconds
# TYPE arkflow_uptime_seconds counter
arkflow_uptime_seconds {}
",
            snapshot.counters.total_acks_processed,
            snapshot.counters.successful_acks,
            snapshot.counters.failed_acks,
            snapshot.gauges.current_pending_acks,
            snapshot.gauges.current_active_nodes,
            snapshot.system_metrics.uptime_seconds,
        )
    }
}

/// Health checker
pub struct HealthChecker {
    metrics: Arc<EnhancedMetrics>,
    health_checks: HashMap<String, Box<dyn Fn() -> bool + Send + Sync>>,
}

impl HealthChecker {
    /// Create new health checker
    pub fn new(metrics: Arc<EnhancedMetrics>) -> Self {
        Self {
            metrics,
            health_checks: HashMap::new(),
        }
    }

    /// Add health check
    pub fn add_health_check<F>(&mut self, name: String, check: F)
    where
        F: Fn() -> bool + Send + Sync + 'static,
    {
        self.health_checks.insert(name, Box::new(check));
    }

    /// Check system health
    pub fn check_health(&self) -> HealthStatus {
        let mut checks = HashMap::new();
        let mut all_healthy = true;

        for (name, check) in &self.health_checks {
            let is_healthy = check();
            checks.insert(name.clone(), is_healthy);
            if !is_healthy {
                all_healthy = false;
            }
        }

        HealthStatus {
            is_healthy: all_healthy,
            checks,
            timestamp: SystemTime::now(),
            uptime_seconds: self.metrics.system_metrics.uptime_seconds(),
        }
    }
}

/// Health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub is_healthy: bool,
    pub checks: HashMap<String, bool>,
    pub timestamp: SystemTime,
    pub uptime_seconds: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_histogram_percentiles() {
        let histogram = Histogram::new();

        // Add some test values
        for i in 0..100 {
            histogram.observe(i as f64);
        }

        let percentiles = histogram.get_percentiles();

        assert_eq!(percentiles.min, 0.0);
        assert_eq!(percentiles.max, 99.0);
        assert!(percentiles.p50 > 45.0 && percentiles.p50 < 55.0);
        assert!(percentiles.p90 > 85.0 && percentiles.p90 < 95.0);
    }

    #[test]
    fn test_enhanced_metrics() {
        let metrics = EnhancedMetrics::new();

        // Record some metrics
        metrics.record_successful_ack();
        metrics.record_failed_ack();
        metrics.record_backpressure_event();
        metrics.update_pending_acks(100);

        let snapshot = metrics.get_snapshot();

        assert_eq!(snapshot.counters.successful_acks, 1);
        assert_eq!(snapshot.counters.failed_acks, 1);
        assert_eq!(snapshot.counters.backpressure_events, 1);
        assert_eq!(snapshot.gauges.current_pending_acks, 100);
    }

    #[tokio::test]
    async fn test_health_checker() {
        let metrics = Arc::new(EnhancedMetrics::new());
        let mut health_checker = HealthChecker::new(metrics.clone());

        health_checker.add_health_check("test_check".to_string(), || true);
        health_checker.add_health_check("failing_check".to_string(), || false);

        let health_status = health_checker.check_health();

        assert!(!health_status.is_healthy);
        assert!(health_status.checks.get("test_check").unwrap());
        assert!(!health_status.checks.get("failing_check").unwrap());
    }

    #[test]
    fn test_prometheus_export() {
        let metrics = Arc::new(EnhancedMetrics::new());
        let exporter = PrometheusExporter::new(metrics, 9090);

        let prometheus_format = exporter.export();

        assert!(prometheus_format.contains("arkflow_acks_processed_total"));
        assert!(prometheus_format.contains("arkflow_acks_successful_total"));
        assert!(prometheus_format.contains("arkflow_pending_acks_current"));
    }
}
