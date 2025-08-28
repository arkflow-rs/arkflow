//! 状态操作的监控和指标
//!
//! 此模块为状态操作提供全面的监控能力：
//! - 操作指标（延迟、吞吐量、错误率）
//! - 状态大小监控
//! - 检查点指标
//! - 性能告警
//! - Prometheus 集成

use crate::state::{EnhancedStateManager, StateBackendType};
use crate::Error;
use parking_lot::Mutex;
use prometheus::{Counter, Gauge, Histogram, HistogramOpts, Opts, Registry, TextEncoder};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

/// 状态操作指标
#[derive(Debug, Clone)]
pub struct StateMetrics {
    /// 操作计数器
    pub operations_total: Counter,
    /// 成功操作总数
    pub operations_success_total: Counter,
    /// 失败操作总数
    pub operations_failed_total: Counter,

    /// 操作延迟直方图
    pub operation_duration_seconds: Histogram,

    /// 状态大小仪表
    pub state_size_bytes: Gauge,
    /// 检查点大小仪表
    pub checkpoint_size_bytes: Gauge,

    /// 检查点指标
    pub checkpoints_total: Counter,
    /// 检查点持续时间直方图
    pub checkpoint_duration_seconds: Histogram,
    /// 成功检查点总数
    pub checkpoint_success_total: Counter,
    /// 失败检查点总数
    pub checkpoint_failed_total: Counter,

    /// 活跃事务
    pub active_transactions: Gauge,
    /// 事务持续时间直方图
    pub transaction_duration_seconds: Histogram,

    /// 缓存指标（如果适用）
    pub cache_hits_total: Counter,
    /// 缓存未命中总数
    pub cache_misses_total: Counter,
    /// 缓存大小仪表
    pub cache_size_bytes: Gauge,

    /// 后端特定指标
    pub s3_operations_total: Counter,
    /// S3 操作持续时间直方图
    pub s3_operation_duration_seconds: Histogram,
    /// S3 错误总数
    pub s3_errors_total: Counter,
}

impl StateMetrics {
    /// 使用默认注册表创建新指标
    pub fn new() -> Result<(Self, Registry), Error> {
        let registry = Registry::new();
        let metrics = Self::new_with_registry(&registry)?;
        Ok((metrics, registry))
    }

    /// 使用自定义注册表创建新指标
    pub fn new_with_registry(registry: &Registry) -> Result<Self, Error> {
        // 操作指标
        let operations_total = Counter::with_opts(Opts::new(
            "arkflow_state_operations_total",
            "状态操作总数",
        ))
        .map_err(|e| Error::Process(format!("创建计数器失败: {}", e)))?;

        let operations_success_total = Counter::with_opts(Opts::new(
            "arkflow_state_operations_success_total",
            "成功状态操作总数",
        ))
        .map_err(|e| Error::Process(format!("创建计数器失败: {}", e)))?;

        let operations_failed_total = Counter::with_opts(Opts::new(
            "arkflow_state_operations_failed_total",
            "失败状态操作总数",
        ))
        .map_err(|e| Error::Process(format!("创建计数器失败: {}", e)))?;

        let operation_duration_seconds = Histogram::with_opts(HistogramOpts::new(
            "arkflow_state_operation_duration_seconds",
            "状态操作持续时间（秒）",
        ))
        .map_err(|e| Error::Process(format!("创建直方图失败: {}", e)))?;

        // 状态大小指标
        let state_size_bytes = Gauge::with_opts(Opts::new(
            "arkflow_state_size_bytes",
            "当前状态大小（字节）",
        ))
        .map_err(|e| Error::Process(format!("创建仪表失败: {}", e)))?;

        let checkpoint_size_bytes = Gauge::with_opts(Opts::new(
            "arkflow_state_checkpoint_size_bytes",
            "最新检查点大小（字节）",
        ))
        .map_err(|e| Error::Process(format!("Failed to create gauge: {}", e)))?;

        // Checkpoint metrics
        let checkpoints_total = Counter::with_opts(Opts::new(
            "arkflow_state_checkpoints_total",
            "Total number of checkpoints created",
        ))
        .map_err(|e| Error::Process(format!("Failed to create counter: {}", e)))?;

        let checkpoint_duration_seconds = Histogram::with_opts(HistogramOpts::new(
            "arkflow_state_checkpoint_duration_seconds",
            "Duration of checkpoint operations in seconds",
        ))
        .map_err(|e| Error::Process(format!("Failed to create histogram: {}", e)))?;

        let checkpoint_success_total = Counter::with_opts(Opts::new(
            "arkflow_state_checkpoints_success_total",
            "Total number of successful checkpoints",
        ))
        .map_err(|e| Error::Process(format!("Failed to create counter: {}", e)))?;

        let checkpoint_failed_total = Counter::with_opts(Opts::new(
            "arkflow_state_checkpoints_failed_total",
            "Total number of failed checkpoints",
        ))
        .map_err(|e| Error::Process(format!("Failed to create counter: {}", e)))?;

        // Transaction metrics
        let active_transactions = Gauge::with_opts(Opts::new(
            "arkflow_state_active_transactions",
            "Current number of active transactions",
        ))
        .map_err(|e| Error::Process(format!("Failed to create gauge: {}", e)))?;

        let transaction_duration_seconds = Histogram::with_opts(HistogramOpts::new(
            "arkflow_state_transaction_duration_seconds",
            "Duration of transactions in seconds",
        ))
        .map_err(|e| Error::Process(format!("Failed to create histogram: {}", e)))?;

        // Cache metrics
        let cache_hits_total = Counter::with_opts(Opts::new(
            "arkflow_state_cache_hits_total",
            "Total number of cache hits",
        ))
        .map_err(|e| Error::Process(format!("Failed to create counter: {}", e)))?;

        let cache_misses_total = Counter::with_opts(Opts::new(
            "arkflow_state_cache_misses_total",
            "Total number of cache misses",
        ))
        .map_err(|e| Error::Process(format!("Failed to create counter: {}", e)))?;

        let cache_size_bytes = Gauge::with_opts(Opts::new(
            "arkflow_state_cache_size_bytes",
            "Current size of state cache in bytes",
        ))
        .map_err(|e| Error::Process(format!("Failed to create gauge: {}", e)))?;

        // S3 metrics
        let s3_operations_total = Counter::with_opts(Opts::new(
            "arkflow_state_s3_operations_total",
            "Total number of S3 operations",
        ))
        .map_err(|e| Error::Process(format!("Failed to create counter: {}", e)))?;

        let s3_operation_duration_seconds = Histogram::with_opts(HistogramOpts::new(
            "arkflow_state_s3_operation_duration_seconds",
            "Duration of S3 operations in seconds",
        ))
        .map_err(|e| Error::Process(format!("Failed to create histogram: {}", e)))?;

        let s3_errors_total = Counter::with_opts(Opts::new(
            "arkflow_state_s3_errors_total",
            "Total number of S3 errors",
        ))
        .map_err(|e| Error::Process(format!("Failed to create counter: {}", e)))?;

        // Register all metrics
        registry.register(Box::new(operations_total.clone()))?;
        registry.register(Box::new(operations_success_total.clone()))?;
        registry.register(Box::new(operations_failed_total.clone()))?;
        registry.register(Box::new(operation_duration_seconds.clone()))?;
        registry.register(Box::new(state_size_bytes.clone()))?;
        registry.register(Box::new(checkpoint_size_bytes.clone()))?;
        registry.register(Box::new(checkpoints_total.clone()))?;
        registry.register(Box::new(checkpoint_duration_seconds.clone()))?;
        registry.register(Box::new(checkpoint_success_total.clone()))?;
        registry.register(Box::new(checkpoint_failed_total.clone()))?;
        registry.register(Box::new(active_transactions.clone()))?;
        registry.register(Box::new(transaction_duration_seconds.clone()))?;
        registry.register(Box::new(cache_hits_total.clone()))?;
        registry.register(Box::new(cache_misses_total.clone()))?;
        registry.register(Box::new(cache_size_bytes.clone()))?;
        registry.register(Box::new(s3_operations_total.clone()))?;
        registry.register(Box::new(s3_operation_duration_seconds.clone()))?;
        registry.register(Box::new(s3_errors_total.clone()))?;

        Ok(Self {
            operations_total,
            operations_success_total,
            operations_failed_total,
            operation_duration_seconds,
            state_size_bytes,
            checkpoint_size_bytes,
            checkpoints_total,
            checkpoint_duration_seconds,
            checkpoint_success_total,
            checkpoint_failed_total,
            active_transactions,
            transaction_duration_seconds,
            cache_hits_total,
            cache_misses_total,
            cache_size_bytes,
            s3_operations_total,
            s3_operation_duration_seconds,
            s3_errors_total,
        })
    }
}

/// Operation timer for measuring duration
pub struct OperationTimer {
    start: Instant,
    metrics: Arc<StateMetrics>,
    operation_type: OperationType,
}

/// Types of state operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperationType {
    Get,
    Put,
    Delete,
    Checkpoint,
    Recover,
    Transaction,
    S3Get,
    S3Put,
    S3Delete,
}

impl OperationTimer {
    /// Start timing an operation
    pub fn start(metrics: Arc<StateMetrics>, operation_type: OperationType) -> Self {
        metrics.operations_total.inc();
        Self {
            start: Instant::now(),
            metrics,
            operation_type,
        }
    }

    /// Stop timing and record success
    pub fn success(self) {
        let duration = self.start.elapsed().as_secs_f64();
        self.metrics.operation_duration_seconds.observe(duration);
        self.metrics.operations_success_total.inc();

        // Record specific metric
        match self.operation_type {
            OperationType::Checkpoint => {
                self.metrics.checkpoint_duration_seconds.observe(duration);
                self.metrics.checkpoint_success_total.inc();
            }
            OperationType::S3Get | OperationType::S3Put | OperationType::S3Delete => {
                self.metrics.s3_operation_duration_seconds.observe(duration);
            }
            OperationType::Transaction => {
                self.metrics.transaction_duration_seconds.observe(duration);
            }
            _ => {}
        }
    }

    /// Stop timing and record failure
    pub fn failure(self) {
        let duration = self.start.elapsed().as_secs_f64();
        self.metrics.operation_duration_seconds.observe(duration);
        self.metrics.operations_failed_total.inc();

        // Record specific metric
        match self.operation_type {
            OperationType::Checkpoint => {
                self.metrics.checkpoint_duration_seconds.observe(duration);
                self.metrics.checkpoint_failed_total.inc();
            }
            OperationType::S3Get | OperationType::S3Put | OperationType::S3Delete => {
                self.metrics.s3_operation_duration_seconds.observe(duration);
                self.metrics.s3_errors_total.inc();
            }
            _ => {}
        }
    }
}

/// State monitor for collecting and reporting metrics
pub struct StateMonitor {
    metrics: Arc<StateMetrics>,
    registry: Registry,
    /// Historical data for alerts
    alert_history: Mutex<AlertHistory>,
    /// Alert thresholds
    alert_config: AlertConfig,
}

/// Alert configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertConfig {
    /// Operation latency threshold in seconds
    pub operation_latency_threshold: f64,
    /// Error rate threshold (0.0 - 1.0)
    pub error_rate_threshold: f64,
    /// State size threshold in bytes
    pub state_size_threshold: u64,
    /// Checkpoint duration threshold in seconds
    pub checkpoint_duration_threshold: f64,
    /// Alert cooldown period
    pub alert_cooldown: Duration,
}

impl Default for AlertConfig {
    fn default() -> Self {
        Self {
            operation_latency_threshold: 1.0,         // 1 second
            error_rate_threshold: 0.05,               // 5%
            state_size_threshold: 1024 * 1024 * 1024, // 1GB
            checkpoint_duration_threshold: 30.0,      // 30 seconds
            alert_cooldown: Duration::from_secs(300), // 5 minutes
        }
    }
}

/// Alert history tracking
#[derive(Debug, Default)]
struct AlertHistory {
    last_alerts: HashMap<String, SystemTime>,
}

impl AlertHistory {
    fn should_alert(&mut self, alert_id: &str, cooldown: Duration) -> bool {
        let now = SystemTime::now();
        let should_alert = match self.last_alerts.get(alert_id) {
            Some(last_time) => now.duration_since(*last_time).unwrap_or(Duration::ZERO) > cooldown,
            None => true,
        };

        if should_alert {
            self.last_alerts.insert(alert_id.to_string(), now);
        }

        should_alert
    }
}

impl StateMonitor {
    /// Create new state monitor
    pub fn new() -> Result<Self, Error> {
        let (metrics, registry) = StateMetrics::new()?;
        Ok(Self {
            metrics: Arc::new(metrics),
            registry,
            alert_history: Mutex::new(AlertHistory::default()),
            alert_config: AlertConfig::default(),
        })
    }

    /// Create with custom alert configuration
    pub fn with_alert_config(alert_config: AlertConfig) -> Result<Self, Error> {
        let (metrics, registry) = StateMetrics::new()?;
        Ok(Self {
            metrics: Arc::new(metrics),
            registry,
            alert_history: Mutex::new(AlertHistory::default()),
            alert_config,
        })
    }

    /// Get metrics reference
    pub fn metrics(&self) -> Arc<StateMetrics> {
        self.metrics.clone()
    }

    /// Get Prometheus registry
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Export metrics in Prometheus format
    pub fn export_metrics(&self) -> Result<String, Error> {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        encoder
            .encode_to_string(&metric_families)
            .map_err(|e| Error::Process(format!("Failed to encode metrics: {}", e)))
    }

    /// Update state size
    pub fn update_state_size(&self, size: u64) {
        self.metrics.state_size_bytes.set(size as f64);

        // Check for alerts
        if size > self.alert_config.state_size_threshold {
            let mut history = self.alert_history.lock();
            if history.should_alert("state_size_too_large", self.alert_config.alert_cooldown) {
                tracing::warn!(
                    "State size too large: {} bytes (threshold: {} bytes)",
                    size,
                    self.alert_config.state_size_threshold
                );
            }
        }
    }

    /// Update checkpoint size
    pub fn update_checkpoint_size(&self, size: u64) {
        self.metrics.checkpoint_size_bytes.set(size as f64);
    }

    /// Update active transactions
    pub fn update_active_transactions(&self, count: usize) {
        self.metrics.active_transactions.set(count as f64);
    }

    /// Update cache metrics
    pub fn update_cache_metrics(&self, hits: u64, misses: u64, size: u64) {
        self.metrics.cache_hits_total.inc_by(hits as f64);
        self.metrics.cache_misses_total.inc_by(misses as f64);
        self.metrics.cache_size_bytes.set(size as f64);
    }

    /// Record checkpoint start
    pub fn record_checkpoint_start(&self) -> OperationTimer {
        self.metrics.checkpoints_total.inc();
        OperationTimer::start(self.metrics.clone(), OperationType::Checkpoint)
    }

    /// Record S3 operation start
    pub fn record_s3_operation_start(&self, op_type: OperationType) -> OperationTimer {
        self.metrics.s3_operations_total.inc();
        OperationTimer::start(self.metrics.clone(), op_type)
    }

    /// Record cache hit
    pub fn record_cache_hit(&self) {
        self.metrics.cache_hits_total.inc();
    }

    /// Record cache miss
    pub fn record_cache_miss(&self) {
        self.metrics.cache_misses_total.inc();
    }

    /// Get current health status
    pub fn health_status(&self) -> HealthStatus {
        // This would typically query current metrics and calculate health
        HealthStatus {
            healthy: true, // Simplified for now
            state_size: self.metrics.state_size_bytes.get() as u64,
            active_transactions: self.metrics.active_transactions.get() as usize,
            last_checkpoint: None, // Would need to track this
            error_rate: 0.0,       // Would need to calculate from counters
        }
    }
}

/// Health status summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub healthy: bool,
    pub state_size: u64,
    pub active_transactions: usize,
    pub last_checkpoint: Option<SystemTime>,
    pub error_rate: f64,
}

/// Monitored enhanced state manager
pub struct MonitoredStateManager {
    inner: EnhancedStateManager,
    monitor: Arc<StateMonitor>,
    backend_type: StateBackendType,
}

impl MonitoredStateManager {
    /// Create new monitored state manager
    pub async fn new(
        config: crate::state::EnhancedStateConfig,
        monitor: Arc<StateMonitor>,
    ) -> Result<Self, Error> {
        let inner = EnhancedStateManager::new(config).await?;
        let backend_type = inner.get_backend_type();

        Ok(Self {
            inner,
            monitor,
            backend_type,
        })
    }

    /// Get inner state manager
    pub fn inner(&self) -> &EnhancedStateManager {
        &self.inner
    }

    /// Get monitor reference
    pub fn monitor(&self) -> Arc<StateMonitor> {
        self.monitor.clone()
    }

    /// Process batch with monitoring
    pub async fn process_batch_monitored(
        &mut self,
        batch: crate::MessageBatch,
    ) -> Result<Vec<crate::MessageBatch>, Error> {
        let timer = OperationTimer::start(self.monitor.metrics(), OperationType::Put);

        match self.inner.process_batch(batch).await {
            Ok(result) => {
                timer.success();
                Ok(result)
            }
            Err(e) => {
                timer.failure();
                Err(e)
            }
        }
    }

    /// Create checkpoint with monitoring
    pub async fn create_checkpoint_monitored(&mut self) -> Result<u64, Error> {
        let timer = self.monitor.record_checkpoint_start();

        match self.inner.create_checkpoint().await {
            Ok(checkpoint_id) => {
                timer.success();

                // Update metrics
                let stats = self.inner.get_state_stats().await;
                self.monitor
                    .update_active_transactions(stats.active_transactions);

                Ok(checkpoint_id)
            }
            Err(e) => {
                timer.failure();
                Err(e)
            }
        }
    }

    /// Get metrics export
    pub fn export_metrics(&self) -> Result<String, Error> {
        self.monitor.export_metrics()
    }

    /// Get health status
    pub fn health_status(&self) -> HealthStatus {
        self.monitor.health_status()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let result = StateMetrics::new();
        assert!(result.is_ok());
    }

    #[test]
    fn test_operation_timer() {
        let (metrics, _) = StateMetrics::new().unwrap();
        let metrics = Arc::new(metrics);

        let timer = OperationTimer::start(metrics.clone(), OperationType::Get);
        drop(timer); // This should record success

        let timer = OperationTimer::start(metrics, OperationType::Get);
        timer.failure();
    }

    #[test]
    fn test_alert_history() {
        let mut history = AlertHistory::default();
        let cooldown = Duration::from_secs(1);

        // First alert should trigger
        assert!(history.should_alert("test", cooldown));

        // Immediate second alert should not trigger
        assert!(!history.should_alert("test", cooldown));
    }
}
