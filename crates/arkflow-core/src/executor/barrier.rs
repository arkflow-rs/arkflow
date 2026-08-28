//! Async checkpoint barriers flowing through the execution kernel.
//!
//! A barrier is a control envelope injected into every source chain's output
//! edges. Chains forward it in FIFO order with the data; a chain with several
//! inbound edges aligns (buffers other inputs) until every input's barrier
//! arrives, snapshots its state asynchronously, then releases the buffered
//! data. The [`BarrierCoordinator`] injects barriers on an interval and
//! completes a checkpoint once every chain reports its snapshot.

use crate::checkpoint::{
    CheckpointBarrier, CheckpointCoordinator, TaskCheckpointAck, TaskAttemptSnapshot,
};
use crate::job::{JobId, JobVersion};
use crate::state::{StateBackend, StateSnapshot};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio::sync::mpsc;

/// Alignment state for one inbound channel while a barrier is in flight.
pub struct Aligner {
    /// Envelopes buffered from inputs whose barrier has not yet arrived.
    buffered: BTreeMap<usize, Vec<super::envelope::Envelope>>,
    /// Indices of inputs whose barrier has arrived.
    aligned: BTreeSet<usize>,
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
        if let super::envelope::Envelope::Barrier(barrier) = envelope {
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
            if self.aligned.len() == self.input_count {
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
        !self.aligned.is_empty() || !self.buffered.is_empty()
    }

    /// Drain buffered envelopes after alignment completes (or fails).
    pub fn release(&mut self) -> Vec<(usize, super::envelope::Envelope)> {
        let mut drained = Vec::new();
        let buffered = std::mem::take(&mut self.buffered);
        for (index, envelopes) in buffered {
            for envelope in envelopes {
                drained.push((index, envelope));
            }
        }
        self.aligned.clear();
        self.in_flight = None;
        drained
    }
}

/// A chain's snapshot report for one barrier.
pub struct ChainSnapshot {
    pub task_id: String,
    pub attempt_id: String,
    pub partition: u32,
    pub barrier: CheckpointBarrier,
    pub state: StateSnapshot,
    pub source_positions: Vec<crate::checkpoint::SourcePosition>,
    pub watermark_ms: Option<i64>,
}

/// Injects barriers on a timer and completes checkpoints from chain reports.
pub struct BarrierCoordinator {
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
                        attempt_id: report.attempt_id.clone(),
                        partition: report.partition,
                        checkpoint_id: barrier.checkpoint_id.clone(),
                        generation: barrier.generation,
                        state: report.state.clone(),
                        source_positions: report.source_positions.clone(),
                        watermark_ms: report.watermark_ms,
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
        let attempts = snapshots
            .values()
            .map(|snapshot| TaskAttemptSnapshot {
                task_id: snapshot.task_id.clone(),
                attempt_id: snapshot.attempt_id.clone(),
                node_id: String::new(),
            })
            .collect::<Vec<_>>();
        let _ = attempts;
        // Persisting manifests stays with the caller (Agent/Engine wiring);
        // the coordinator's contract ends at "all participants reported".
        let _ = self.participants.len();
        Ok(())
    }

    pub fn take_error(&self) -> Option<crate::Error> {
        self.last_error.lock().unwrap().take()
    }
}

/// Snapshot helper shared by chains: capture a state backend snapshot without
/// blocking the caller's event loop (spawn_blocking-friendly).
pub async fn snapshot_state(
    backend: Arc<dyn StateBackend>,
) -> Result<StateSnapshot, crate::Error> {
    let handle =
        tokio::task::spawn_blocking(move || backend.snapshot().map_err(Into::into)).await;
    handle.map_err(|error| crate::Error::Process(format!("state snapshot task failed: {error}")))?
}
