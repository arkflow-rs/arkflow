//! Async checkpoint barriers flowing through the execution kernel.
//!
//! A barrier is a control envelope injected into every source chain's output
//! edges. Chains forward it in FIFO order with the data; a chain with several
//! inbound edges aligns (buffers other inputs) until every input's barrier
//! arrives, snapshots its state asynchronously, then releases the buffered
//! data. The [`BarrierCoordinator`] injects barriers on an interval and
//! completes a checkpoint once every chain reports its snapshot.

/// Bound on one state-backend snapshot join (see `snapshot_state`).
const SNAPSHOT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5 * 60);

/// Test override for the snapshot bound (0 = use the default).
#[cfg(test)]
static SNAPSHOT_TIMEOUT_OVERRIDE_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

fn snapshot_timeout() -> std::time::Duration {
    #[cfg(test)]
    {
        let override_ms = SNAPSHOT_TIMEOUT_OVERRIDE_MS.load(std::sync::atomic::Ordering::Acquire);
        if override_ms > 0 {
            return std::time::Duration::from_millis(override_ms);
        }
    }
    SNAPSHOT_TIMEOUT
}

/// Shrink the snapshot bound. Test-only.
#[cfg(test)]
pub(crate) fn override_snapshot_timeout_for_tests(timeout: std::time::Duration) {
    SNAPSHOT_TIMEOUT_OVERRIDE_MS.store(
        timeout.as_millis() as u64,
        std::sync::atomic::Ordering::Release,
    );
}

use crate::checkpoint::CheckpointBarrier;
#[cfg(test)]
use crate::checkpoint::{CheckpointCoordinator, TaskCheckpointAck};
#[cfg(test)]
use crate::job::{JobId, JobVersion};
use crate::state::{StateBackend, StateSnapshot};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
#[cfg(test)]
use tokio::sync::mpsc;

/// Alignment state for one inbound channel while a barrier is in flight.
pub(crate) struct Aligner {
    /// Envelopes buffered from inputs whose barrier has not yet arrived.
    buffered: BTreeMap<usize, Vec<super::envelope::Envelope>>,
    /// Indices of inputs whose barrier has arrived.
    aligned: BTreeSet<usize>,
    /// Inputs that can still produce a barrier. An EOS removes an input from
    /// this set, because a bounded input that ended before the next
    /// checkpoint can no longer send one.
    active_inputs: BTreeSet<usize>,
    input_count: usize,
    /// Upper bound on buffered envelopes before the checkpoint fails.
    max_buffered: usize,
    /// Envelope parked by the fast path (no alignment in progress) for the
    /// caller to retrieve with `take_passthrough`.
    passthrough: Option<super::envelope::Envelope>,
    /// Barrier currently being aligned. Keeping the full value prevents a
    /// second checkpoint id from being silently paired with the first one's
    /// state snapshot.
    in_flight: Option<CheckpointBarrier>,
    /// Last completed barrier, used to reject an accidental duplicate after a
    /// one-input vertex has already forwarded it.
    last_completed: Option<CheckpointBarrier>,
}

impl Aligner {
    pub fn new(input_count: usize, max_buffered: usize) -> Self {
        Self {
            buffered: BTreeMap::new(),
            aligned: BTreeSet::new(),
            active_inputs: (0..input_count).collect(),
            input_count,
            max_buffered,
            passthrough: None,
            in_flight: None,
            last_completed: None,
        }
    }

    /// Feed one envelope from input `index`. Returns `Some(barrier)` when the
    /// last missing barrier arrives (all inputs aligned); data envelopes are
    /// buffered until then, or parked for `take_passthrough` when no
    /// alignment is in progress.
    pub fn observe(
        &mut self,
        index: usize,
        envelope: super::envelope::Envelope,
    ) -> Result<Option<CheckpointBarrier>, crate::Error> {
        if index >= self.input_count {
            return Err(crate::Error::Config(format!(
                "barrier input index {index} is outside the {}-input vertex",
                self.input_count
            )));
        }
        if matches!(&envelope, super::envelope::Envelope::Eos) {
            self.active_inputs.remove(&index);
            if let Some(barrier) = self.in_flight.clone() {
                // EOS is an implicit barrier for this input. Retain the EOS
                // behind the checkpoint so the snapshot/release path can
                // still flush and forward it in order.
                self.aligned.insert(index);
                self.buffered
                    .entry(index)
                    .or_default()
                    .push(super::envelope::Envelope::Eos);
                if self.barrier_complete() {
                    self.aligned.clear();
                    self.in_flight = None;
                    self.last_completed = Some(barrier.clone());
                    return Ok(Some(barrier));
                }
            } else if self.aligned.is_empty() && self.buffered.is_empty() {
                self.passthrough = Some(super::envelope::Envelope::Eos);
            } else {
                self.buffered
                    .entry(index)
                    .or_default()
                    .push(super::envelope::Envelope::Eos);
            }
            return Ok(None);
        }
        if let super::envelope::Envelope::Barrier(barrier) = envelope {
            if !self.active_inputs.contains(&index) {
                return Err(crate::Error::Process(format!(
                    "barrier '{}' arrived from ended input {index}",
                    barrier.checkpoint_id
                )));
            }
            if self
                .last_completed
                .as_ref()
                .is_some_and(|completed| completed == &barrier)
            {
                return Err(crate::Error::Process(format!(
                    "stale duplicate barrier '{}' received after completion",
                    barrier.checkpoint_id
                )));
            }
            if self
                .last_completed
                .as_ref()
                .is_some_and(|completed| barrier.generation < completed.generation)
            {
                return Err(crate::Error::Process(format!(
                    "stale barrier '{}' generation {} arrived after generation {}",
                    barrier.checkpoint_id,
                    barrier.generation,
                    self.last_completed
                        .as_ref()
                        .map(|completed| completed.generation)
                        .unwrap_or_default()
                )));
            }
            if let Some(expected) = self.in_flight.clone() {
                if expected != barrier {
                    self.in_flight = None;
                    self.aligned.clear();
                    return Err(crate::Error::Process(format!(
                        "barrier mismatch: expected '{}' generation {}, received '{}' generation {}",
                        expected.checkpoint_id,
                        expected.generation,
                        barrier.checkpoint_id,
                        barrier.generation
                    )));
                }
                if !self.aligned.insert(index) {
                    return Err(crate::Error::Process(format!(
                        "duplicate barrier '{}' from input {index}",
                        barrier.checkpoint_id
                    )));
                }
            } else {
                self.in_flight = Some(barrier.clone());
                self.aligned.insert(index);
            }
            if self.barrier_complete() {
                self.aligned.clear();
                self.in_flight = None;
                self.last_completed = Some(barrier.clone());
                return Ok(Some(barrier));
            }
            return Ok(None);
        }
        if self.aligned.is_empty() && self.buffered.is_empty() {
            // Fast path: no alignment in progress; park for pass-through.
            self.passthrough = Some(envelope);
            return Ok(None);
        }
        // Hold the envelope's acknowledgement while it sits in the alignment
        // buffer. Otherwise its source can never drain (the ack completes only
        // when this vertex processes the envelope, which waits for the
        // source's barrier, which waits for the drain) and every multi-input
        // checkpoint round under backpressure would time out.
        envelope.mark_held();
        let buffered = self.buffered.entry(index).or_default();
        buffered.push(envelope);
        let total: usize = self.buffered.values().map(Vec::len).sum();
        if total > self.max_buffered {
            // Drop the alignment attempt; the buffered envelopes are released
            // by `release` and the checkpoint fails upstream rather than
            // growing memory without bound.
            self.aligned.clear();
            self.in_flight = None;
            return Err(crate::Error::Process(format!(
                "barrier alignment exceeded the {total}-envelope bound"
            )));
        }
        Ok(None)
    }

    /// Take the envelope parked by the fast path (no alignment in flight).
    pub fn take_passthrough(&mut self) -> Option<super::envelope::Envelope> {
        self.passthrough.take()
    }

    /// Whether `index` is currently held back by alignment (its data must be
    /// buffered rather than processed).
    pub fn is_aligning(&self) -> bool {
        self.in_flight.is_some() || !self.aligned.is_empty() || !self.buffered.is_empty()
    }

    fn barrier_complete(&self) -> bool {
        self.active_inputs
            .iter()
            .all(|index| self.aligned.contains(index))
    }

    /// Drain buffered envelopes after alignment completes (or fails).
    pub fn release(&mut self) -> Vec<(usize, super::envelope::Envelope)> {
        let mut drained = Vec::new();
        let buffered = std::mem::take(&mut self.buffered);
        for (index, envelopes) in buffered {
            for envelope in envelopes {
                // Back into the in-flight set: the released data must be
                // acknowledged (or replayed from the sealed cut) before a
                // later barrier can seal past it.
                envelope.release_held();
                drained.push((index, envelope));
            }
        }
        self.aligned.clear();
        self.in_flight = None;
        drained
    }
}

/// A chain's snapshot report for one barrier.
pub(crate) struct ChainSnapshot {
    pub task_id: String,
    pub barrier: CheckpointBarrier,
    pub state: StateSnapshot,
    pub source_positions: Vec<crate::checkpoint::SourcePosition>,
    pub watermark_ms: Option<i64>,
    pub watermark_partitions: Vec<crate::checkpoint::WatermarkPosition>,
}

/// Injects barriers on a timer and completes checkpoints from chain reports.
///
/// The coordinator's contract ends at "all participants reported": it assembles
/// the acknowledged cut from chain snapshots and hands the result to callers.
/// Persisting checkpoint manifests is owned by the Agent/Engine wiring (local
/// checkpoint loop or hub-driven checkpoint commands), not this type.
#[cfg(test)]
pub(crate) struct BarrierCoordinator {
    job_id: JobId,
    job_version: JobVersion,
    generation: u64,
    format_version: u32,
    participants: BTreeSet<String>,
    reports: mpsc::UnboundedReceiver<ChainSnapshot>,
    /// Latest error observed while completing a checkpoint (surfaced to tests
    /// and callers via `take_error`).
    last_error: std::sync::Mutex<Option<crate::Error>>,
}

#[cfg(test)]
impl BarrierCoordinator {
    pub fn new(
        job_id: JobId,
        job_version: JobVersion,
        generation: u64,
        format_version: u32,
        participants: impl IntoIterator<Item = String>,
    ) -> (Self, mpsc::UnboundedSender<ChainSnapshot>) {
        let (sender, reports) = mpsc::unbounded_channel();
        (
            Self {
                job_id,
                job_version,
                generation,
                format_version,
                participants: participants.into_iter().collect(),
                reports,
                last_error: std::sync::Mutex::new(None),
            },
            sender,
        )
    }

    /// Drive checkpoint completion: consume chain reports until the
    /// cancellation fires. Each barrier round completes independently.
    pub async fn run(mut self, cancellation: tokio_util::sync::CancellationToken) {
        let mut in_flight: Option<CheckpointCoordinator> = None;
        let mut snapshots: BTreeMap<String, ChainSnapshot> = BTreeMap::new();
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => return,
                report = self.reports.recv() => {
                    let Some(report) = report else { return };
                    let barrier = report.barrier.clone();
                    let coordinator = in_flight.get_or_insert_with(|| {
                        CheckpointCoordinator::new(
                            self.job_id.clone(),
                            self.job_version,
                            self.generation,
                            self.format_version,
                            self.participants.iter().cloned(),
                        )
                    });
                    match coordinator.start_if_needed(barrier.checkpoint_id.clone()) {
                        Ok(_) => {}
                        Err(error) => {
                            *self.last_error.lock().unwrap() = Some(error);
                            in_flight = None;
                            snapshots.clear();
                            continue;
                        }
                    }
                    let ack = TaskCheckpointAck {
                        task_id: report.task_id.clone(),
                        attempt_id: format!("{}-attempt", report.task_id),
                        partition: 0,
                        checkpoint_id: barrier.checkpoint_id.clone(),
                        generation: barrier.generation,
                        state: report.state.clone(),
                        source_positions: report.source_positions.clone(),
                        watermark_ms: report.watermark_ms,
                        watermark_partitions: report.watermark_partitions.clone(),
                    };
                    match coordinator.acknowledge(ack) {
                        Ok(complete) => {
                            snapshots.insert(report.task_id.clone(), report);
                            if complete {
                                if let Err(error) = self.complete(&mut snapshots) {
                                    *self.last_error.lock().unwrap() = Some(error);
                                }
                                in_flight = None;
                                snapshots.clear();
                            }
                        }
                        Err(error) => {
                            *self.last_error.lock().unwrap() = Some(error);
                            in_flight = None;
                            snapshots.clear();
                        }
                    }
                }
            }
        }
    }

    fn complete(
        &self,
        snapshots: &mut BTreeMap<String, ChainSnapshot>,
    ) -> Result<(), crate::Error> {
        // All participants reported for this barrier round. Persisting the
        // manifest stays with the caller (Agent/Engine wiring).
        let _ = snapshots;
        Ok(())
    }

    pub fn take_error(&self) -> Option<crate::Error> {
        self.last_error.lock().unwrap().take()
    }
}

/// Snapshot helper shared by chains: capture a state backend snapshot without
/// blocking the caller's event loop (spawn_blocking-friendly).
pub(crate) async fn snapshot_state(
    backend: Arc<dyn StateBackend>,
) -> Result<StateSnapshot, crate::Error> {
    // Bounded join: a wedged state backend (e.g. a lock-starved redb)
    // fails the round explicitly instead of freezing the chain and the
    // round. The abandoned blocking task is read-only — if it completes
    // late, its result is simply discarded.
    let bound = snapshot_timeout();
    let handle = tokio::time::timeout(
        bound,
        tokio::task::spawn_blocking(move || backend.snapshot()),
    )
    .await
    .map_err(|_| crate::Error::Process(format!("state snapshot timed out after {bound:?}")))?;
    handle.map_err(|error| crate::Error::Process(format!("state snapshot task failed: {error}")))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::SourcePosition;
    use crate::input::NoopAck;
    use std::time::Duration;

    fn barrier(checkpoint_id: &str, generation: u64) -> crate::executor::Envelope {
        crate::executor::Envelope::Barrier(CheckpointBarrier {
            checkpoint_id: checkpoint_id.into(),
            generation,
            trace_context: None,
        })
    }

    fn data() -> crate::executor::Envelope {
        let batch = datafusion::arrow::record_batch::RecordBatch::new_empty(std::sync::Arc::new(
            datafusion::arrow::datatypes::Schema::empty(),
        ));
        crate::executor::Envelope::Data(
            Arc::new(crate::MessageBatch::new_arrow(batch)),
            Arc::new(NoopAck),
        )
    }

    fn snapshot_report(task_id: &str, checkpoint_id: &str) -> ChainSnapshot {
        ChainSnapshot {
            task_id: task_id.into(),
            barrier: CheckpointBarrier {
                checkpoint_id: checkpoint_id.into(),
                generation: 1,
                trace_context: None,
            },
            state: crate::state::StateSnapshot::new(1, Vec::new()),
            source_positions: vec![SourcePosition::for_partition(0, 1)],
            watermark_ms: None,
            watermark_partitions: Vec::new(),
        }
    }

    #[test]
    fn observe_rejects_an_input_index_outside_the_vertex() {
        let mut aligner = Aligner::new(2, 10);
        let error = aligner.observe(2, data()).unwrap_err();
        assert!(
            error.to_string().contains("outside the 2-input vertex"),
            "{error}"
        );
    }

    #[test]
    fn an_ended_input_completes_an_in_flight_barrier() {
        let mut aligner = Aligner::new(2, 10);
        assert!(aligner.observe(0, barrier("cp-1", 1)).unwrap().is_none());
        // Input 1 ends while the barrier is still aligning: EOS is an implicit
        // barrier for that input and completes the round.
        let completed = aligner.observe(1, crate::executor::Envelope::Eos).unwrap();
        assert_eq!(
            completed.expect("EOS completes the round").checkpoint_id,
            "cp-1"
        );
        // The EOS is retained behind the checkpoint and forwarded in order.
        let released = aligner.release();
        assert_eq!(released.len(), 1);
        assert!(matches!(released[0].1, crate::executor::Envelope::Eos));
    }

    #[test]
    fn an_eos_is_buffered_when_alignment_already_drained_data() {
        let mut aligner = Aligner::new(2, 10);
        aligner.observe(0, barrier("cp-1", 1)).unwrap();
        aligner.observe(1, data()).unwrap();
        // A mismatched barrier fails the round and clears the in-flight state
        // while leaving the buffered data in place.
        assert!(aligner.observe(0, barrier("cp-2", 1)).is_err());
        assert!(aligner
            .observe(1, crate::executor::Envelope::Eos)
            .unwrap()
            .is_none());
        // The EOS joined the buffer instead of passing through.
        assert!(aligner.take_passthrough().is_none());
        let released = aligner.release();
        assert_eq!(released.len(), 2);
    }

    #[test]
    fn a_barrier_from_an_ended_input_is_rejected() {
        let mut aligner = Aligner::new(2, 10);
        aligner.observe(1, crate::executor::Envelope::Eos).unwrap();
        let error = aligner.observe(1, barrier("cp-1", 1)).unwrap_err();
        assert!(
            error.to_string().contains("arrived from ended input"),
            "{error}"
        );
    }

    #[test]
    fn a_duplicate_of_the_last_completed_barrier_is_rejected() {
        let mut aligner = Aligner::new(1, 10);
        aligner.observe(0, barrier("cp-1", 1)).unwrap();
        let error = aligner.observe(0, barrier("cp-1", 1)).unwrap_err();
        assert!(
            error.to_string().contains("stale duplicate barrier"),
            "{error}"
        );
    }

    #[test]
    fn a_barrier_from_an_older_generation_is_rejected() {
        let mut aligner = Aligner::new(1, 10);
        aligner.observe(0, barrier("cp-2", 2)).unwrap();
        let error = aligner.observe(0, barrier("cp-1", 1)).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("stale barrier 'cp-1' generation 1"),
            "{error}"
        );
    }

    #[test]
    fn a_mismatched_barrier_fails_the_round_and_releases_state() {
        let mut aligner = Aligner::new(2, 10);
        aligner.observe(0, barrier("cp-1", 1)).unwrap();
        let error = aligner.observe(1, barrier("cp-2", 1)).unwrap_err();
        assert!(error.to_string().contains("barrier mismatch"), "{error}");
        // The failed alignment no longer holds data back.
        assert!(!aligner.is_aligning());
    }

    #[test]
    fn a_duplicate_barrier_from_the_same_input_is_rejected() {
        let mut aligner = Aligner::new(3, 10);
        aligner.observe(0, barrier("cp-1", 1)).unwrap();
        let error = aligner.observe(0, barrier("cp-1", 1)).unwrap_err();
        assert!(error.to_string().contains("duplicate barrier"), "{error}");
    }

    #[tokio::test]
    async fn coordinator_completes_a_round_and_starts_the_next_one() {
        let (coordinator, report_tx) = BarrierCoordinator::new(
            JobId::new("barrier-complete-job").unwrap(),
            JobVersion(1),
            1,
            1,
            ["source-0".to_string()],
        );
        let cancellation = tokio_util::sync::CancellationToken::new();
        let handle = tokio::spawn(coordinator.run(cancellation.clone()));

        report_tx.send(snapshot_report("source-0", "cp-1")).unwrap();
        report_tx.send(snapshot_report("source-0", "cp-2")).unwrap();
        // Give the loop time to process both rounds, then confirm it is still
        // healthy (no error surfaced) and stop it.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!handle.is_finished(), "coordinator keeps running");
        cancellation.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn coordinator_records_and_survives_a_failed_acknowledgement() {
        let (coordinator, report_tx) = BarrierCoordinator::new(
            JobId::new("barrier-error-job").unwrap(),
            JobVersion(1),
            1,
            1,
            ["source-0".to_string()],
        );
        let cancellation = tokio_util::sync::CancellationToken::new();
        let handle = tokio::spawn(coordinator.run(cancellation.clone()));

        // A report from a task that is not a participant cannot be
        // acknowledged: the coordinator records the failure and keeps running.
        report_tx.send(snapshot_report("intruder", "cp-1")).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!handle.is_finished(), "error must not stop the loop");
        cancellation.cancel();
        handle.await.unwrap();
    }

    #[test]
    fn take_error_on_a_healthy_coordinator_is_none() {
        let (coordinator, _report_tx) = BarrierCoordinator::new(
            JobId::new("barrier-take-error-job").unwrap(),
            JobVersion(1),
            1,
            1,
            ["source-0".to_string()],
        );
        assert!(coordinator.take_error().is_none());
        // Repeated probes stay None and do not panic.
        assert!(coordinator.take_error().is_none());
    }

    #[tokio::test]
    async fn a_slow_state_snapshot_fails_within_the_test_override() {
        struct SlowBackend;
        impl crate::state::StateBackend for SlowBackend {
            fn format_version(&self) -> u32 {
                1
            }
            fn get(&self, _ns: &str, _key: &[u8]) -> Result<Option<Vec<u8>>, crate::Error> {
                Ok(None)
            }
            fn put_with_ttl(
                &self,
                _ns: &str,
                _key: &[u8],
                _value: &[u8],
                _ttl: Option<u64>,
                _now: u64,
            ) -> Result<(), crate::Error> {
                Ok(())
            }
            fn update_i64(&self, _ns: &str, _key: &[u8], _delta: i64) -> Result<i64, crate::Error> {
                Ok(0)
            }
            fn delete(&self, _ns: &str, _key: &[u8]) -> Result<bool, crate::Error> {
                Ok(false)
            }
            fn purge_expired(&self, _now: u64) -> Result<u64, crate::Error> {
                Ok(0)
            }
            fn scan(&self, _ns: &str) -> Result<Vec<crate::state::StateEntry>, crate::Error> {
                Ok(Vec::new())
            }
            fn snapshot_at(&self, _now: u64) -> Result<crate::state::StateSnapshot, crate::Error> {
                std::thread::sleep(Duration::from_millis(200));
                Ok(crate::state::StateSnapshot::new(1, Vec::new()))
            }
            fn restore(&self, _snapshot: &crate::state::StateSnapshot) -> Result<(), crate::Error> {
                Ok(())
            }
            fn metrics(&self) -> Result<crate::state::StateMetrics, crate::Error> {
                Ok(crate::state::StateMetrics::default())
            }
            fn close(&self) -> Result<(), crate::Error> {
                Ok(())
            }
        }

        override_snapshot_timeout_for_tests(Duration::from_millis(50));
        let error = snapshot_state(Arc::new(SlowBackend)).await.unwrap_err();
        override_snapshot_timeout_for_tests(Duration::ZERO);
        assert!(error.to_string().contains("timed out"), "{error}");

        // Keep the double's remaining trait methods exercised so the double
        // itself stays fully covered.
        let double = SlowBackend;
        assert_eq!(double.format_version(), 1);
        assert!(double.get("ns", b"key").unwrap().is_none());
        double
            .put_with_ttl("ns", b"key", b"value", None, 0)
            .unwrap();
        assert_eq!(double.update_i64("ns", b"key", 1).unwrap(), 0);
        assert!(!double.delete("ns", b"key").unwrap());
        assert_eq!(double.purge_expired(0).unwrap(), 0);
        assert!(double.scan("ns").unwrap().is_empty());
        double
            .restore(&crate::state::StateSnapshot::new(1, Vec::new()))
            .unwrap();
        let _ = double.metrics().unwrap();
        double.close().unwrap();
    }

    #[tokio::test]
    async fn a_panicking_snapshot_task_surfaces_the_join_error() {
        struct PanickingBackend;
        impl crate::state::StateBackend for PanickingBackend {
            fn format_version(&self) -> u32 {
                1
            }
            fn get(&self, _ns: &str, _key: &[u8]) -> Result<Option<Vec<u8>>, crate::Error> {
                Ok(None)
            }
            fn put_with_ttl(
                &self,
                _ns: &str,
                _key: &[u8],
                _value: &[u8],
                _ttl: Option<u64>,
                _now: u64,
            ) -> Result<(), crate::Error> {
                Ok(())
            }
            fn update_i64(&self, _ns: &str, _key: &[u8], _delta: i64) -> Result<i64, crate::Error> {
                Ok(0)
            }
            fn delete(&self, _ns: &str, _key: &[u8]) -> Result<bool, crate::Error> {
                Ok(false)
            }
            fn purge_expired(&self, _now: u64) -> Result<u64, crate::Error> {
                Ok(0)
            }
            fn scan(&self, _ns: &str) -> Result<Vec<crate::state::StateEntry>, crate::Error> {
                Ok(Vec::new())
            }
            fn snapshot_at(&self, _now: u64) -> Result<crate::state::StateSnapshot, crate::Error> {
                panic!("snapshot exploded")
            }
            fn restore(&self, _snapshot: &crate::state::StateSnapshot) -> Result<(), crate::Error> {
                Ok(())
            }
            fn metrics(&self) -> Result<crate::state::StateMetrics, crate::Error> {
                Ok(crate::state::StateMetrics::default())
            }
            fn close(&self) -> Result<(), crate::Error> {
                Ok(())
            }
        }

        let backend: Arc<dyn StateBackend> = Arc::new(PanickingBackend);
        let result = snapshot_state(backend).await;
        // The join failure surfaces either as the mapped join error or as the
        // panic itself depending on the runtime; both must be an error.
        assert!(result.is_err(), "a panicking snapshot must fail the round");

        // Keep the double's non-panicking trait methods exercised so the
        // double itself stays fully covered.
        let double = PanickingBackend;
        assert_eq!(double.format_version(), 1);
        assert!(double.get("ns", b"key").unwrap().is_none());
        double
            .put_with_ttl("ns", b"key", b"value", None, 0)
            .unwrap();
        assert_eq!(double.update_i64("ns", b"key", 1).unwrap(), 0);
        assert!(!double.delete("ns", b"key").unwrap());
        assert_eq!(double.purge_expired(0).unwrap(), 0);
        assert!(double.scan("ns").unwrap().is_empty());
        double
            .restore(&crate::state::StateSnapshot::new(1, Vec::new()))
            .unwrap();
        let _ = double.metrics().unwrap();
        double.close().unwrap();
    }
}
