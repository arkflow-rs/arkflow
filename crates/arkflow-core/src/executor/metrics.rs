//! Kernel-wide metrics: per-chain counters surfaced through the runtime
//! metrics snapshot (6.1). Chains update shared atomics; the handle exposes
//! a snapshot for control-plane reporting.

use serde::{Deserialize, Serialize};
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
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Default)]
pub struct KernelMetrics {
    chains: std::sync::Mutex<BTreeMap<String, Arc<ChainMetrics>>>,
    /// Lifetime flush-failure counter of the Job's input WAL. Set once at
    /// startup when the adapter owns a WAL; `None` for Jobs without WAL
    /// durability. Sampled on snapshot, so the data path pays nothing.
    ///
    /// Shares the counter only (not the `Wal`): a `Wal` reference here would
    /// outlive the run and keep the redb store's exclusive lock, so
    /// restarting the Stream could not reopen its WAL path.
    wal_flush_failures: std::sync::Mutex<Option<Arc<AtomicU64>>>,
    /// Last checkpoint duration in milliseconds (0 = none yet).
    pub checkpoint_duration_ms: AtomicU64,
    pub checkpoint_failures: AtomicU64,
    /// Max watermark lag across sources (0 = no event-time sources).
    pub watermark_lag_ms: AtomicI64,
    /// Late events dropped/routed/updated across gates.
    pub late_events: AtomicU64,
}

impl std::fmt::Debug for KernelMetrics {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KernelMetrics")
            .field("chains", &self.chains.lock().unwrap().keys())
            .field(
                "wal_flush_failures",
                &self
                    .wal_flush_failures
                    .lock()
                    .unwrap()
                    .as_ref()
                    .map(|counter| counter.load(Ordering::Relaxed)),
            )
            .field("checkpoint_duration_ms", &self.checkpoint_duration_ms)
            .field("checkpoint_failures", &self.checkpoint_failures)
            .field("watermark_lag_ms", &self.watermark_lag_ms)
            .field("late_events", &self.late_events)
            .finish()
    }
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

    /// Attach the input WAL whose flush failures this Job reports. Only the
    /// lifetime counter is retained, so a finished run drops the WAL and
    /// releases its store lock while the exported total stays put.
    pub(crate) fn set_wal(&self, wal: &Arc<crate::wal::Wal>) {
        *self.wal_flush_failures.lock().unwrap() = Some(wal.flush_failures_handle());
    }

    /// Whether a WAL counter is attached. Test-only probe for the runner
    /// wiring: a bound-but-healthy WAL and an unbound one both snapshot 0.
    #[cfg(test)]
    pub(crate) fn wal_bound(&self) -> bool {
        self.wal_flush_failures.lock().unwrap().is_some()
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
            wal_flush_failures: self
                .wal_flush_failures
                .lock()
                .unwrap()
                .as_ref()
                .map(|counter| counter.load(Ordering::Relaxed))
                .unwrap_or(0),
        }
    }
}

/// Serializable view of the job-wide metrics.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct KernelMetricsSnapshot {
    pub chains: BTreeMap<String, ChainMetricsSnapshot>,
    pub checkpoint_duration_ms: u64,
    pub checkpoint_failures: u64,
    pub watermark_lag_ms: u64,
    pub late_events: u64,
    /// Cumulative background-flush failures of the Job's input WAL (0 when
    /// the Job has no WAL). `serde(default)` keeps mixed-version Hub/Agent
    /// heartbeats decodable.
    #[serde(default)]
    pub wal_flush_failures: u64,
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

    #[test]
    fn unbound_wal_reports_zero_flush_failures() {
        let metrics = KernelMetrics::default();
        assert_eq!(metrics.snapshot().wal_flush_failures, 0);
    }

    #[test]
    fn snapshots_default_the_wal_field_when_absent() {
        // Payload shape of a peer that predates the field: every earlier
        // required member present, `wal_flush_failures` absent.
        let job: KernelMetricsSnapshot = serde_json::from_str(
            r#"{"chains":{},"checkpoint_duration_ms":0,"checkpoint_failures":0,"watermark_lag_ms":0,"late_events":0}"#,
        )
        .unwrap();
        assert_eq!(job.wal_flush_failures, 0);
        let stream: crate::control::StreamMetricsSnapshot = serde_json::from_str(
            r#"{"input_batches":0,"input_messages":0,"processing_errors":0,"output_batches":0,"output_messages":0,"input_errors":0,"input_reconnects":0,"output_errors":0,"restarts":0}"#,
        )
        .unwrap();
        assert_eq!(stream.wal_flush_failures, 0);
        // A report that does carry the field round-trips it unchanged.
        let reported: KernelMetricsSnapshot = serde_json::from_str(
            r#"{"chains":{},"checkpoint_duration_ms":0,"checkpoint_failures":0,"watermark_lag_ms":0,"late_events":0,"wal_flush_failures":7}"#,
        )
        .unwrap();
        assert_eq!(reported.wal_flush_failures, 7);
    }
}
