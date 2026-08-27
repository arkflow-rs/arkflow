//! Kernel-wide metrics: per-chain counters surfaced through the runtime
//! metrics snapshot (6.1). Chains update shared atomics; the handle exposes
//! a snapshot for control-plane reporting.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

/// Counters for one chain (vertex).
#[derive(Debug, Default)]
pub struct ChainMetrics {
    /// Batches accepted into the chain.
    pub batches_in: AtomicU64,
    /// Batches emitted downstream (or written by a sink chain).
    pub batches_out: AtomicU64,
    /// Rows flowing through.
    pub rows: AtomicU64,
    /// Processing errors (processor failures).
    pub errors: AtomicU64,
    /// Envelopes currently parked on this chain's outbound edges (backlog),
    /// maintained by senders as in-flight counts.
    pub in_flight: AtomicI64,
    /// Sum of per-batch processing times in microseconds (divide by
    /// batches_out for mean latency).
    pub processing_us_total: AtomicU64,
}

impl ChainMetrics {
    pub fn snapshot(&self) -> ChainMetricsSnapshot {
        ChainMetricsSnapshot {
            batches_in: self.batches_in.load(Ordering::Relaxed),
            batches_out: self.batches_out.load(Ordering::Relaxed),
            rows: self.rows.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            in_flight: self.in_flight.load(Ordering::Relaxed).max(0) as u64,
            mean_latency_us: {
                let batches = self.batches_out.load(Ordering::Relaxed);
                match batches {
                    0 => 0,
                    _ => self.processing_us_total.load(Ordering::Relaxed) / batches,
                }
            },
        }
    }
}

/// Serializable view of one chain's counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChainMetricsSnapshot {
    pub batches_in: u64,
    pub batches_out: u64,
    pub rows: u64,
    pub errors: u64,
    pub in_flight: u64,
    pub mean_latency_us: u64,
}

/// Job-wide metrics registry: chains keyed by entry task id plus checkpoint
/// and watermark aggregates. Interior-mutable so chains can register their
/// counters through a shared reference.
#[derive(Debug, Default)]
pub struct KernelMetrics {
    chains: std::sync::Mutex<BTreeMap<String, Arc<ChainMetrics>>>,
    /// Last checkpoint duration in milliseconds (0 = none yet).
    pub checkpoint_duration_ms: AtomicU64,
    pub checkpoint_failures: AtomicU64,
    /// Max watermark lag across sources (0 = no event-time sources).
    pub watermark_lag_ms: AtomicI64,
    /// Late events dropped/routed/updated across gates.
    pub late_events: AtomicU64,
}

impl KernelMetrics {
    /// Register (or fetch) the counters for one chain.
    pub fn chain(&self, task_id: &str) -> Arc<ChainMetrics> {
        self.chains
            .lock()
            .unwrap()
            .entry(task_id.to_owned())
            .or_default()
            .clone()
    }

    pub fn record_checkpoint(&self, duration_ms: u64) {
        self.checkpoint_duration_ms
            .store(duration_ms, Ordering::Relaxed);
    }

    pub fn record_checkpoint_failure(&self) {
        self.checkpoint_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> KernelMetricsSnapshot {
        KernelMetricsSnapshot {
            chains: self
                .chains
                .lock()
                .unwrap()
                .iter()
                .map(|(task_id, metrics)| (task_id.clone(), metrics.snapshot()))
                .collect(),
            checkpoint_duration_ms: self.checkpoint_duration_ms.load(Ordering::Relaxed),
            checkpoint_failures: self.checkpoint_failures.load(Ordering::Relaxed),
            watermark_lag_ms: self.watermark_lag_ms.load(Ordering::Relaxed).max(0) as u64,
            late_events: self.late_events.load(Ordering::Relaxed),
        }
    }
}

/// Serializable view of the job-wide metrics.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct KernelMetricsSnapshot {
    pub chains: BTreeMap<String, ChainMetricsSnapshot>,
    pub checkpoint_duration_ms: u64,
    pub checkpoint_failures: u64,
    pub watermark_lag_ms: u64,
    pub late_events: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chain_counters_aggregate_and_snapshot() {
        let metrics = KernelMetrics::default();
        let chain = metrics.chain("source-0");
        chain.batches_in.fetch_add(3, Ordering::Relaxed);
        chain.batches_out.fetch_add(2, Ordering::Relaxed);
        chain.processing_us_total.fetch_add(100, Ordering::Relaxed);
        chain.in_flight.store(1, Ordering::Relaxed);
        metrics.record_checkpoint(42);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.chains["source-0"].batches_in, 3);
        assert_eq!(snapshot.chains["source-0"].mean_latency_us, 50);
        assert_eq!(snapshot.checkpoint_duration_ms, 42);
    }
}
