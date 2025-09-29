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

//! Performance optimization utilities for distributed acknowledgment system

use crate::enhanced_config::EnhancedConfig;
use crate::enhanced_metrics::EnhancedMetrics;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Performance optimization strategies
#[derive(Debug, Clone)]
pub enum OptimizationStrategy {
    /// Throughput optimization (prioritize speed)
    Throughput,
    /// Latency optimization (prioritize low latency)
    Latency,
    /// Memory optimization (prioritize low memory usage)
    Memory,
    /// Balanced optimization (balanced approach)
    Balanced,
}

/// Performance metrics for optimization decisions
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub current_throughput: f64,
    pub average_latency: f64,
    pub memory_usage_mb: f64,
    pub cpu_usage_percentage: f64,
    pub error_rate: f64,
    pub timestamp: Instant,
}

impl Default for PerformanceMetrics {
    fn default() -> Self {
        Self {
            current_throughput: 0.0,
            average_latency: 0.0,
            memory_usage_mb: 0.0,
            cpu_usage_percentage: 0.0,
            error_rate: 0.0,
            timestamp: Instant::now(),
        }
    }
}

/// Adaptive batch size controller
#[derive(Debug)]
pub struct BatchSizeController {
    current_batch_size: usize,
    min_batch_size: usize,
    max_batch_size: usize,
    target_latency_ms: f64,
    adjustment_factor: f64,
    measurements: Vec<f64>,
}

impl BatchSizeController {
    pub fn new(
        initial_size: usize,
        min_size: usize,
        max_size: usize,
        target_latency_ms: f64,
    ) -> Self {
        Self {
            current_batch_size: initial_size,
            min_batch_size: min_size,
            max_batch_size: max_size,
            target_latency_ms: target_latency_ms,
            adjustment_factor: 0.1,
            measurements: Vec::new(),
        }
    }

    /// Record a latency measurement and adjust batch size
    pub fn record_latency(&mut self, latency_ms: f64) -> usize {
        self.measurements.push(latency_ms);

        // Keep only recent measurements
        if self.measurements.len() > 10 {
            self.measurements.remove(0);
        }

        if self.measurements.len() >= 5 {
            let avg_latency: f64 =
                self.measurements.iter().sum::<f64>() / self.measurements.len() as f64;

            // Adjust batch size based on latency
            if avg_latency > self.target_latency_ms {
                // Latency too high, reduce batch size
                self.current_batch_size =
                    (self.current_batch_size as f64 * (1.0 - self.adjustment_factor)) as usize;
                self.current_batch_size = self.current_batch_size.max(self.min_batch_size);
            } else {
                // Latency acceptable, increase batch size
                self.current_batch_size =
                    (self.current_batch_size as f64 * (1.0 + self.adjustment_factor)) as usize;
                self.current_batch_size = self.current_batch_size.min(self.max_batch_size);
            }
        }

        self.current_batch_size
    }

    /// Get current batch size
    pub fn current_size(&self) -> usize {
        self.current_batch_size
    }
}

/// Concurrency controller for adaptive task management
#[derive(Debug)]
pub struct ConcurrencyController {
    current_concurrency: usize,
    min_concurrency: usize,
    max_concurrency: usize,
    target_queue_size: usize,
    metrics: Arc<EnhancedMetrics>,
}

impl ConcurrencyController {
    pub fn new(
        initial_concurrency: usize,
        min_concurrency: usize,
        max_concurrency: usize,
        target_queue_size: usize,
        metrics: Arc<EnhancedMetrics>,
    ) -> Self {
        Self {
            current_concurrency: initial_concurrency,
            min_concurrency,
            max_concurrency,
            target_queue_size,
            metrics,
        }
    }

    /// Update concurrency based on current system load
    pub async fn update_concurrency(&mut self) -> usize {
        // Get current queue size from metrics
        let queue_size = self
            .metrics
            .gauge("active_connections")
            .map(|g| g.get() as usize)
            .unwrap_or(0);

        // Adjust concurrency based on queue size
        if queue_size > self.target_queue_size * 2 {
            // High load, reduce concurrency
            self.current_concurrency = (self.current_concurrency as f64 * 0.8) as usize;
            self.current_concurrency = self.current_concurrency.max(self.min_concurrency);
        } else if queue_size < self.target_queue_size / 2 {
            // Low load, increase concurrency
            self.current_concurrency = (self.current_concurrency as f64 * 1.2) as usize;
            self.current_concurrency = self.current_concurrency.min(self.max_concurrency);
        }

        self.current_concurrency
    }

    /// Get current concurrency level
    pub fn current_concurrency(&self) -> usize {
        self.current_concurrency
    }
}

/// Memory usage monitor and optimizer
#[derive(Debug)]
pub struct MemoryMonitor {
    max_memory_mb: usize,
    current_memory_mb: Arc<AtomicU64>,
    gc_threshold: f64,
    last_gc: Arc<RwLock<Instant>>,
}

impl MemoryMonitor {
    pub fn new(max_memory_mb: usize, gc_threshold: f64) -> Self {
        Self {
            max_memory_mb,
            current_memory_mb: Arc::new(AtomicU64::new(0)),
            gc_threshold,
            last_gc: Arc::new(RwLock::new(Instant::now())),
        }
    }

    /// Update memory usage and check if GC is needed
    pub async fn update_memory_usage(&self, current_mb: usize) -> bool {
        self.current_memory_mb
            .store(current_mb as u64, Ordering::Relaxed);

        // Check if we need to trigger garbage collection
        let usage_ratio = current_mb as f64 / self.max_memory_mb as f64;
        let last_gc = self.last_gc.read().await;
        let time_since_gc = last_gc.elapsed();

        if usage_ratio > self.gc_threshold && time_since_gc > Duration::from_secs(30) {
            // Update last GC time
            drop(last_gc);
            let mut last_gc = self.last_gc.write().await;
            *last_gc = Instant::now();
            drop(last_gc);

            true // GC needed
        } else {
            false // No GC needed
        }
    }

    /// Get current memory usage
    pub fn current_usage_mb(&self) -> usize {
        self.current_memory_mb.load(Ordering::Relaxed) as usize
    }

    /// Get memory usage percentage
    pub fn usage_percentage(&self) -> f64 {
        (self.current_memory_mb.load(Ordering::Relaxed) as f64 / self.max_memory_mb as f64) * 100.0
    }
}

/// Main performance optimizer
#[derive(Debug)]
pub struct PerformanceOptimizer {
    strategy: OptimizationStrategy,
    config: EnhancedConfig,
    metrics: Arc<EnhancedMetrics>,
    batch_controller: BatchSizeController,
    concurrency_controller: ConcurrencyController,
    memory_monitor: MemoryMonitor,
    last_optimization: Arc<RwLock<Instant>>,
}

impl PerformanceOptimizer {
    pub fn new(
        strategy: OptimizationStrategy,
        config: EnhancedConfig,
        metrics: Arc<EnhancedMetrics>,
    ) -> Self {
        let batch_controller = BatchSizeController::new(
            config.performance.batch_size,
            10,
            config.performance.max_pending_acks,
            config.performance.target_batch_processing_time_ms as f64,
        );

        let concurrency_controller = ConcurrencyController::new(
            config.performance.max_concurrent_operations / 2,
            1,
            config.performance.max_concurrent_operations,
            config.performance.backpressure_threshold(),
            metrics.clone(),
        );

        let memory_monitor = MemoryMonitor::new(
            config.resources.max_memory_mb,
            0.8, // 80% threshold
        );

        Self {
            strategy,
            config,
            metrics,
            batch_controller,
            concurrency_controller,
            memory_monitor,
            last_optimization: Arc::new(RwLock::new(Instant::now())),
        }
    }

    /// Optimize system performance based on current metrics
    pub async fn optimize(&mut self) -> OptimizationResult {
        let now = Instant::now();
        let last_opt = self.last_optimization.read().await;

        // Only optimize every 5 seconds
        if now.duration_since(*last_opt) < Duration::from_secs(5) {
            return OptimizationResult::no_change_needed();
        }
        drop(last_opt);

        // Collect current metrics
        let metrics = self.collect_metrics().await;

        // Apply optimization strategy
        let result = match self.strategy {
            OptimizationStrategy::Throughput => self.optimize_for_throughput(&metrics).await,
            OptimizationStrategy::Latency => self.optimize_for_latency(&metrics).await,
            OptimizationStrategy::Memory => self.optimize_for_memory(&metrics).await,
            OptimizationStrategy::Balanced => self.optimize_balanced(&metrics).await,
        };

        // Update last optimization time
        let mut last_opt = self.last_optimization.write().await;
        *last_opt = now;
        drop(last_opt);

        result
    }

    /// Get current batch size recommendation
    pub fn recommended_batch_size(&self) -> usize {
        self.batch_controller.current_size()
    }

    /// Get current concurrency recommendation
    pub fn recommended_concurrency(&self) -> usize {
        self.concurrency_controller.current_concurrency()
    }

    /// Record processing latency for batch size adjustment
    pub fn record_processing_latency(&mut self, latency_ms: f64) {
        self.batch_controller.record_latency(latency_ms);
    }

    /// Check if memory cleanup is needed
    pub async fn check_memory_cleanup(&self, current_memory_mb: usize) -> bool {
        self.memory_monitor
            .update_memory_usage(current_memory_mb)
            .await
    }

    async fn collect_metrics(&self) -> PerformanceMetrics {
        // This would collect real metrics from the system
        // For now, we'll use placeholder values
        PerformanceMetrics {
            current_throughput: self
                .metrics
                .counter("messages_processed")
                .map(|c| c.get() as f64)
                .unwrap_or(0.0),
            average_latency: 0.0, // Would be calculated from histogram
            memory_usage_mb: self.memory_monitor.current_usage_mb() as f64,
            cpu_usage_percentage: 0.0, // Would need system monitoring
            error_rate: 0.0,           // Would be calculated from error counters
            timestamp: Instant::now(),
        }
    }

    async fn optimize_for_throughput(
        &mut self,
        metrics: &PerformanceMetrics,
    ) -> OptimizationResult {
        let mut changes = Vec::new();

        // Increase batch size for better throughput
        if metrics.error_rate < 0.05
            && metrics.memory_usage_mb < self.config.resources.max_memory_mb as f64 * 0.8
        {
            let new_size = (self.batch_controller.current_size() as f64 * 1.1) as usize;
            self.batch_controller.current_batch_size =
                new_size.min(self.config.performance.max_pending_acks);
            changes.push(OptimizationChange::BatchSizeIncreased(new_size));
        }

        // Maximize concurrency for throughput
        let new_concurrency = self.config.performance.max_concurrent_operations;
        if self.concurrency_controller.current_concurrency < new_concurrency {
            self.concurrency_controller.current_concurrency = new_concurrency;
            changes.push(OptimizationChange::ConcurrencyIncreased(new_concurrency));
        }

        OptimizationResult {
            changes,
            metrics: metrics.clone(),
        }
    }

    async fn optimize_for_latency(&mut self, metrics: &PerformanceMetrics) -> OptimizationResult {
        let mut changes = Vec::new();

        // Reduce batch size for lower latency
        if metrics.average_latency > 100.0 {
            let new_size = (self.batch_controller.current_size() as f64 * 0.8) as usize;
            self.batch_controller.current_batch_size = new_size.max(10);
            changes.push(OptimizationChange::BatchSizeDecreased(new_size));
        }

        // Moderate concurrency for latency
        let target_concurrency = self.config.performance.max_concurrent_operations / 2;
        if self.concurrency_controller.current_concurrency > target_concurrency {
            self.concurrency_controller.current_concurrency = target_concurrency;
            changes.push(OptimizationChange::ConcurrencyDecreased(target_concurrency));
        }

        OptimizationResult {
            changes,
            metrics: metrics.clone(),
        }
    }

    async fn optimize_for_memory(&mut self, metrics: &PerformanceMetrics) -> OptimizationResult {
        let mut changes = Vec::new();

        // Reduce batch size to save memory
        if metrics.memory_usage_mb > self.config.resources.max_memory_mb as f64 * 0.7 {
            let new_size = (self.batch_controller.current_size() as f64 * 0.7) as usize;
            self.batch_controller.current_batch_size = new_size.max(10);
            changes.push(OptimizationChange::BatchSizeDecreased(new_size));
        }

        // Reduce concurrency to save memory
        if metrics.memory_usage_mb > self.config.resources.max_memory_mb as f64 * 0.8 {
            let new_concurrency =
                (self.concurrency_controller.current_concurrency as f64 * 0.6) as usize;
            self.concurrency_controller.current_concurrency = new_concurrency.max(1);
            changes.push(OptimizationChange::ConcurrencyDecreased(new_concurrency));
        }

        OptimizationResult {
            changes,
            metrics: metrics.clone(),
        }
    }

    async fn optimize_balanced(&mut self, metrics: &PerformanceMetrics) -> OptimizationResult {
        let mut changes = Vec::new();

        // Balanced approach - make small adjustments
        if metrics.error_rate > 0.1 {
            // High error rate, reduce both batch size and concurrency
            let new_size = (self.batch_controller.current_size() as f64 * 0.9) as usize;
            self.batch_controller.current_batch_size = new_size.max(10);
            changes.push(OptimizationChange::BatchSizeDecreased(new_size));

            let new_concurrency =
                (self.concurrency_controller.current_concurrency as f64 * 0.9) as usize;
            self.concurrency_controller.current_concurrency = new_concurrency.max(1);
            changes.push(OptimizationChange::ConcurrencyDecreased(new_concurrency));
        } else if metrics.memory_usage_mb < self.config.resources.max_memory_mb as f64 * 0.6 {
            // Low memory usage, can increase performance
            let new_size = (self.batch_controller.current_size() as f64 * 1.05) as usize;
            self.batch_controller.current_batch_size =
                new_size.min(self.config.performance.max_pending_acks);
            changes.push(OptimizationChange::BatchSizeIncreased(new_size));
        }

        OptimizationResult {
            changes,
            metrics: metrics.clone(),
        }
    }
}

/// Result of performance optimization
#[derive(Debug, Clone)]
pub struct OptimizationResult {
    pub changes: Vec<OptimizationChange>,
    pub metrics: PerformanceMetrics,
}

impl OptimizationResult {
    pub fn no_change_needed() -> Self {
        Self {
            changes: Vec::new(),
            metrics: PerformanceMetrics::default(),
        }
    }

    pub fn has_changes(&self) -> bool {
        !self.changes.is_empty()
    }
}

/// Types of optimization changes
#[derive(Debug, Clone)]
pub enum OptimizationChange {
    BatchSizeIncreased(usize),
    BatchSizeDecreased(usize),
    ConcurrencyIncreased(usize),
    ConcurrencyDecreased(usize),
    MemoryCleanupTriggered,
    StrategyChanged(OptimizationStrategy),
}

impl std::fmt::Display for OptimizationChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OptimizationChange::BatchSizeIncreased(size) => {
                write!(f, "Batch size increased to {}", size)
            }
            OptimizationChange::BatchSizeDecreased(size) => {
                write!(f, "Batch size decreased to {}", size)
            }
            OptimizationChange::ConcurrencyIncreased(level) => {
                write!(f, "Concurrency increased to {}", level)
            }
            OptimizationChange::ConcurrencyDecreased(level) => {
                write!(f, "Concurrency decreased to {}", level)
            }
            OptimizationChange::MemoryCleanupTriggered => write!(f, "Memory cleanup triggered"),
            OptimizationChange::StrategyChanged(strategy) => {
                write!(f, "Strategy changed to {:?}", strategy)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_size_controller() {
        let mut controller = BatchSizeController::new(100, 10, 1000, 50.0);

        // Test with high latency (should decrease batch size)
        let new_size = controller.record_latency(100.0);
        assert!(new_size < 100);

        // Test with low latency (should increase batch size)
        let _ = controller.record_latency(20.0);
        let _ = controller.record_latency(20.0);
        let _ = controller.record_latency(20.0);
        let _ = controller.record_latency(20.0);
        let _ = controller.record_latency(20.0);
        let new_size = controller.record_latency(20.0);
        assert!(new_size > 10); // Should be larger than minimum
    }

    #[test]
    fn test_memory_monitor() {
        let monitor = MemoryMonitor::new(1000, 0.8);

        // Test normal usage
        assert!(!monitor.update_memory_usage(500).now_or_never()); // 50% usage

        // Test high usage
        // Note: This test would need to be async in a real scenario
        assert_eq!(monitor.usage_percentage(), 50.0);
    }

    #[tokio::test]
    async fn test_performance_optimizer_creation() {
        let config = EnhancedConfig::development();
        let metrics = Arc::new(EnhancedMetrics::new());

        let optimizer = PerformanceOptimizer::new(OptimizationStrategy::Balanced, config, metrics);

        assert_eq!(optimizer.recommended_batch_size(), 50); // Development default
        assert!(optimizer.recommended_concurrency() > 0);
    }
}
