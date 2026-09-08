//! Acknowledged-cut tracking for consistent checkpoints.
//!
//! A checkpoint may only contain source positions whose downstream output
//! acknowledgement has completed. This module keeps, per source partition, the
//! "next offset" of the contiguous acknowledged run plus a set of acknowledged
//! next-offsets that arrived out of order (a fan-out can complete branches in
//! any order). Only the contiguous frontier is exposed to checkpointing — a
//! maximum observed offset would silently skip unacknowledged records in the
//! gap.
//!
//! [`CheckpointCut`] is the immutable snapshot sealed by a source chain at
//! barrier injection: acknowledged positions, watermark progress, and a
//! monotonic cut generation, captured atomically in the single-threaded source
//! event loop before the barrier flows downstream.

use crate::checkpoint::SourcePosition;
use crate::input::Ack;
use crate::Error;
use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Identity of one source partition inside a frontier.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PartitionKey {
    pub topic: Option<String>,
    pub partition: u32,
}

impl PartitionKey {
    pub fn from_position(position: &SourcePosition) -> Self {
        Self {
            topic: position.topic.clone(),
            partition: position.partition,
        }
    }
}

/// Outcome of applying one acknowledgement to a partition frontier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckAdvance {
    /// The contiguous frontier advanced to the carried next offset.
    Advanced { next_offset: u64 },
    /// The acknowledgement landed beyond a gap; the carried offset must be
    /// acknowledged before the frontier can advance past it.
    Pending { gap: u64 },
    /// Already covered by the contiguous frontier — a duplicate delivery or a
    /// retried acknowledgement whose first attempt advanced the frontier but
    /// failed its durable source-side commit.
    AlreadyCovered,
}

/// Per-partition contiguous acknowledgement state.
#[derive(Debug, Default, Clone)]
struct PartitionFrontier {
    /// Whether the partition entered the frontier (seeded or acknowledged).
    /// Seeded partitions expose their restored cursor immediately so a
    /// checkpoint taken before the first new acknowledgement still reports
    /// the restored positions.
    active: bool,
    /// Next offset expected contiguously; every offset below it is
    /// acknowledged.
    next_offset: u64,
    /// Acknowledged next-offsets waiting for the gap before them to close.
    pending: BTreeSet<u64>,
}

/// Snapshot of one partition frontier used by connectors that must
/// compensate a durable source commit after a sibling acknowledgement fails.
/// The fields stay private so callers can only restore a snapshot produced by
/// this frontier.
#[derive(Debug, Clone)]
pub struct FrontierSnapshot {
    active: bool,
    next_offset: u64,
    pending: BTreeSet<u64>,
}

/// An immutable sealed view of the acknowledged frontier at one instant.
///
/// Sealed by the source event loop at barrier injection: positions and the
/// watermark belong to the same instant, and later acknowledgements cannot
/// mutate an already-sealed cut.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointCut {
    /// Monotonic cut generation; successive seals are ordered by this value.
    pub generation: u64,
    /// Contiguous acknowledged positions at seal time.
    pub positions: Vec<SourcePosition>,
    /// Event-time watermark captured with the same seal.
    pub watermark_ms: Option<i64>,
}

/// Execution-local frontier of contiguous source acknowledgements.
///
/// Kafka inputs and the local WAL track their acknowledged cursor through this
/// type; checkpoint barriers seal it into a [`CheckpointCut`]. Interior
/// mutability keeps the ack path (`&self` on `Arc<dyn Ack>`) lock-friendly.
pub struct CommitFrontier {
    partitions: Mutex<BTreeMap<PartitionKey, PartitionFrontier>>,
    /// A failed durable source commit fences later acknowledgements for the
    /// same physical partition.  Without this side channel, a later Kafka
    /// acknowledgement that is waiting on the failed frontier could sleep
    /// forever after the failed caller has already returned.
    failures: Mutex<BTreeMap<PartitionKey, FrontierFailure>>,
    generation: Mutex<u64>,
}

#[derive(Debug, Clone)]
pub struct FrontierFailure {
    /// Exclusive next offset whose durable source commit failed.
    pub next_offset: u64,
    pub error: String,
}

impl Default for CommitFrontier {
    fn default() -> Self {
        Self::new()
    }
}

impl CommitFrontier {
    pub fn new() -> Self {
        Self {
            partitions: Mutex::new(BTreeMap::new()),
            failures: Mutex::new(BTreeMap::new()),
            generation: Mutex::new(0),
        }
    }

    /// Record a durable source failure for one partition.  Later deliveries
    /// must observe this fence and fail/retry rather than waiting for a
    /// frontier that cannot advance on its own.
    pub fn record_failure(
        &self,
        topic: Option<&str>,
        partition: u32,
        next_offset: u64,
        error: impl Into<String>,
    ) {
        self.failures.lock().unwrap().insert(
            PartitionKey {
                topic: topic.map(str::to_owned),
                partition,
            },
            FrontierFailure {
                next_offset,
                error: error.into(),
            },
        );
    }

    /// Return the durable failure currently fencing one partition, if any.
    pub fn failure(&self, topic: Option<&str>, partition: u32) -> Option<FrontierFailure> {
        self.failures
            .lock()
            .unwrap()
            .get(&PartitionKey {
                topic: topic.map(str::to_owned),
                partition,
            })
            .cloned()
    }

    /// Clear a failure only for the retrying next offset.  A retry for a
    /// different/later offset must not accidentally remove the fence.
    pub fn clear_failure_if(&self, topic: Option<&str>, partition: u32, next_offset: u64) -> bool {
        let mut failures = self.failures.lock().unwrap();
        let key = PartitionKey {
            topic: topic.map(str::to_owned),
            partition,
        };
        if failures
            .get(&key)
            .is_some_and(|failure| failure.next_offset == next_offset)
        {
            failures.remove(&key);
            true
        } else {
            false
        }
    }

    /// Install restored checkpoint positions as the starting frontier. The
    /// restored cursor stays exposed until new acknowledgements advance past
    /// it, so a checkpoint immediately after restore reports the restored
    /// positions instead of an empty cursor.
    pub fn seed(&self, positions: &[SourcePosition]) {
        let mut partitions = self.partitions.lock().unwrap();
        let mut failures = self.failures.lock().unwrap();
        for position in positions {
            let frontier = partitions
                .entry(PartitionKey::from_position(position))
                .or_default();
            frontier.active = true;
            // Restore is all-or-nothing per partition: the checkpoint carried
            // a contiguous frontier, so the restored offset replaces any
            // stale seed rather than merging with it.
            frontier.next_offset = position.offset;
            frontier.pending.clear();
            failures.remove(&PartitionKey::from_position(position));
        }
    }

    /// Record one successful acknowledgement. `position.offset` is the next
    /// offset after the last acknowledged record (exclusive next-offset
    /// semantics, matching `SourcePosition`). The first acknowledgement of a
    /// never-seeded partition anchors its frontier — connectors that can
    /// observe deliveries should prefer [`CommitFrontier::anchor_delivery`].
    pub fn acknowledge(&self, position: &SourcePosition) -> AckAdvance {
        let mut partitions = self.partitions.lock().unwrap();
        let frontier = partitions
            .entry(PartitionKey::from_position(position))
            .or_default();
        if !frontier.active {
            frontier.active = true;
            frontier.next_offset = position.offset;
            return AckAdvance::Advanced {
                next_offset: position.offset,
            };
        }
        if position.offset <= frontier.next_offset {
            return AckAdvance::AlreadyCovered;
        }
        frontier.pending.insert(position.offset);
        if position.offset == frontier.next_offset + 1 {
            // Contiguous: drain every consecutive next-offset that followed.
            let mut next = frontier.next_offset;
            while frontier.pending.remove(&(next + 1)) {
                next += 1;
            }
            frontier.next_offset = next;
            return AckAdvance::Advanced { next_offset: next };
        }
        AckAdvance::Pending {
            gap: frontier.next_offset + 1,
        }
    }

    /// Anchor the frontier at the delivery of the record at `position.offset`
    /// for a partition with no frontier yet. A connector that can observe
    /// deliveries uses this so an out-of-order *first* acknowledgement cannot
    /// skip the earlier records of the same delivery burst: the contiguous
    /// run starts at the first delivered record, not the first acked one.
    /// No-op for a partition that already has a frontier.
    pub fn anchor_delivery(&self, position: &SourcePosition) {
        let mut partitions = self.partitions.lock().unwrap();
        let frontier = partitions
            .entry(PartitionKey::from_position(position))
            .or_default();
        if frontier.active {
            return;
        }
        frontier.active = true;
        frontier.next_offset = position.offset;
    }

    /// The contiguous acknowledged frontier, one position per partition.
    pub fn contiguous_positions(&self) -> Vec<SourcePosition> {
        let partitions = self.partitions.lock().unwrap();
        partitions
            .iter()
            .filter(|(_, frontier)| frontier.active)
            .map(|(key, frontier)| SourcePosition {
                topic: key.topic.clone(),
                partition: key.partition,
                offset: frontier.next_offset,
            })
            .collect()
    }

    /// The contiguous next offset of one partition, if it has a frontier.
    /// Retry paths use this to re-attempt a durable source-side commit for
    /// an acknowledgement that already advanced the frontier.
    pub fn next_offset_of(&self, topic: Option<&str>, partition: u32) -> Option<u64> {
        let partitions = self.partitions.lock().unwrap();
        partitions
            .get(&PartitionKey {
                topic: topic.map(str::to_owned),
                partition,
            })
            .filter(|frontier| frontier.active)
            .map(|frontier| frontier.next_offset)
    }

    /// Capture one partition before a tentative acknowledgement advances it.
    pub fn snapshot_partition(&self, topic: Option<&str>, partition: u32) -> FrontierSnapshot {
        let partitions = self.partitions.lock().unwrap();
        partitions
            .get(&PartitionKey {
                topic: topic.map(str::to_owned),
                partition,
            })
            .map(|frontier| FrontierSnapshot {
                active: frontier.active,
                next_offset: frontier.next_offset,
                pending: frontier.pending.clone(),
            })
            .unwrap_or_else(|| FrontierSnapshot {
                active: false,
                next_offset: 0,
                pending: BTreeSet::new(),
            })
    }

    /// Restore a previously captured partition snapshot. Returns `false` if
    /// another acknowledgement has already changed the partition, so a
    /// caller never silently clobbers a later commit.
    pub fn restore_partition_if_current(
        &self,
        topic: Option<&str>,
        partition: u32,
        expected_next_offset: u64,
        snapshot: FrontierSnapshot,
    ) -> bool {
        let mut partitions = self.partitions.lock().unwrap();
        let key = PartitionKey {
            topic: topic.map(str::to_owned),
            partition,
        };
        if !snapshot.active {
            let removable = partitions
                .get(&key)
                .is_some_and(|frontier| frontier.next_offset == expected_next_offset);
            if removable {
                partitions.remove(&key);
            }
            return removable;
        }
        let Some(frontier) = partitions.get_mut(&key) else {
            return false;
        };
        if !frontier.active || frontier.next_offset != expected_next_offset {
            return false;
        }
        frontier.active = snapshot.active;
        frontier.next_offset = snapshot.next_offset;
        frontier.pending = snapshot.pending;
        true
    }

    /// Rewind a single contiguous position by one next-offset. This is used
    /// only after the corresponding durable source acknowledgement has been
    /// compensated and is guarded by the expected current value.
    pub fn rewind_position(
        &self,
        topic: Option<&str>,
        partition: u32,
        expected_next_offset: u64,
    ) -> bool {
        let mut partitions = self.partitions.lock().unwrap();
        let Some(frontier) = partitions.get_mut(&PartitionKey {
            topic: topic.map(str::to_owned),
            partition,
        }) else {
            return false;
        };
        if !frontier.active || frontier.next_offset != expected_next_offset {
            return false;
        }
        frontier.next_offset = expected_next_offset.saturating_sub(1);
        frontier
            .pending
            .retain(|offset| *offset <= frontier.next_offset);
        self.failures.lock().unwrap().remove(&PartitionKey {
            topic: topic.map(str::to_owned),
            partition,
        });
        true
    }

    /// Seal the current acknowledged state into an immutable cut. Each seal
    /// bumps the generation; a cut sealed after this one observes a strictly
    /// larger generation even when the frontier did not move.
    pub fn seal(&self, watermark_ms: Option<i64>) -> CheckpointCut {
        let mut generation = self.generation.lock().unwrap();
        *generation = generation.saturating_add(1);
        CheckpointCut {
            generation: *generation,
            positions: self.contiguous_positions(),
            watermark_ms,
        }
    }
}

/// Counts the source chain's dispatched acknowledgements that are still in
/// flight. Barrier draining waits for this count (excluding held
/// acknowledgements) to reach zero before sealing a cut, so the sealed
/// positions and the committed state describe the same acknowledged set: an
/// acknowledgement completes only after its state apply, WAL-cursor advance,
/// and source-side commit have all finished.
#[derive(Debug, Default)]
pub struct AckTracker {
    dispatched: AtomicUsize,
    completed: AtomicUsize,
    held: AtomicUsize,
}

impl AckTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Acknowledgements that still block a barrier seal: dispatched, not yet
    /// completed, and not held by a buffering operator.
    pub fn blocking(&self) -> usize {
        self.dispatched
            .load(Ordering::Acquire)
            .saturating_sub(self.completed.load(Ordering::Acquire))
            .saturating_sub(self.held.load(Ordering::Acquire))
    }
}

/// Source-side acknowledgement wrapper feeding an [`AckTracker`].
pub struct TrackingAck {
    tracker: Arc<AckTracker>,
    inner: Arc<dyn Ack>,
    held: std::sync::atomic::AtomicBool,
    completed: std::sync::atomic::AtomicBool,
    ack_lock: tokio::sync::Mutex<()>,
}

impl TrackingAck {
    pub fn new(tracker: Arc<AckTracker>, inner: Arc<dyn Ack>) -> Self {
        tracker.dispatched.fetch_add(1, Ordering::AcqRel);
        Self {
            tracker,
            inner,
            held: std::sync::atomic::AtomicBool::new(false),
            completed: std::sync::atomic::AtomicBool::new(false),
            ack_lock: tokio::sync::Mutex::new(()),
        }
    }

    /// Make a compensated acknowledgement retryable.  A successful `ack`
    /// increments `completed`; undoing that success removes the completion so
    /// the source remains part of the next checkpoint cut until the caller
    /// retries it.  An acknowledgement that never completed stays blocking
    /// after a successful undo for the same reason.
    fn settle_tracker_after_undo(&self) {
        if self.completed.swap(false, Ordering::AcqRel) {
            self.tracker.completed.fetch_sub(1, Ordering::AcqRel);
        }
        if self.held.swap(false, Ordering::AcqRel) {
            self.tracker.held.fetch_sub(1, Ordering::AcqRel);
        }
    }

    /// An aborted delivery will not be retried by this chain.  Count it as
    /// terminally settled even when its inner acknowledgement failed before
    /// ever reaching `ack()`, otherwise `dispatched - completed - held` keeps
    /// the barrier blocked forever after an error path aborts the delivery.
    fn settle_tracker_after_abort(&self) {
        if self.completed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.tracker.completed.fetch_add(1, Ordering::AcqRel);
        if self.held.swap(false, Ordering::AcqRel) {
            self.tracker.held.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

#[async_trait]
impl Ack for TrackingAck {
    async fn ack(&self) -> Result<(), Error> {
        let _guard = self.ack_lock.lock().await;
        if self.completed.load(Ordering::Acquire) {
            return Ok(());
        }
        let result = self.inner.ack().await;
        if result.is_ok() {
            self.completed.store(true, Ordering::Release);
            self.tracker.completed.fetch_add(1, Ordering::AcqRel);
            // A held acknowledgement that eventually completes (a window
            // fired) no longer blocks later barriers.
            if self.held.swap(false, Ordering::AcqRel) {
                self.tracker.held.fetch_sub(1, Ordering::AcqRel);
            }
        }
        result
    }

    async fn undo(&self) -> Result<(), Error> {
        // Serialize compensation with ack.  Without this guard a late undo
        // can clear `completed` after a concurrent retry has already
        // committed the same delivery, corrupting the barrier counters.
        let _guard = self.ack_lock.lock().await;
        let result = self.inner.undo().await;
        if result.is_ok() {
            self.settle_tracker_after_undo();
        }
        result
    }

    async fn abort(&self) -> Result<(), Error> {
        let _guard = self.ack_lock.lock().await;
        let result = self.inner.abort().await;
        if result.is_ok() {
            self.settle_tracker_after_abort();
        }
        result
    }

    fn mark_held(&self) {
        if !self.held.swap(true, Ordering::AcqRel) {
            self.tracker.held.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn release_held(&self) {
        if self.held.swap(false, Ordering::AcqRel) {
            self.tracker.held.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn position(topic: &str, partition: u32, next_offset: u64) -> SourcePosition {
        SourcePosition {
            topic: Some(topic.to_string()),
            partition,
            offset: next_offset,
        }
    }

    #[test]
    fn in_order_acknowledgements_advance_contiguously() {
        let frontier = CommitFrontier::new();
        assert_eq!(
            frontier.acknowledge(&position("orders", 0, 10)),
            AckAdvance::Advanced { next_offset: 10 }
        );
        assert_eq!(
            frontier.acknowledge(&position("orders", 0, 11)),
            AckAdvance::Advanced { next_offset: 11 }
        );
        assert_eq!(
            frontier.contiguous_positions(),
            vec![position("orders", 0, 11)]
        );
    }

    #[test]
    fn out_of_order_acknowledgements_wait_for_the_gap() {
        let frontier = CommitFrontier::new();
        frontier.acknowledge(&position("orders", 0, 10));
        // A fan-out completes the later branch first.
        assert_eq!(
            frontier.acknowledge(&position("orders", 0, 12)),
            AckAdvance::Pending { gap: 11 }
        );
        assert_eq!(
            frontier.contiguous_positions(),
            vec![position("orders", 0, 10)]
        );
        assert_eq!(
            frontier.acknowledge(&position("orders", 0, 13)),
            AckAdvance::Pending { gap: 11 }
        );
        // Closing the gap drains the whole contiguous run at once.
        assert_eq!(
            frontier.acknowledge(&position("orders", 0, 11)),
            AckAdvance::Advanced { next_offset: 13 }
        );
        assert_eq!(
            frontier.contiguous_positions(),
            vec![position("orders", 0, 13)]
        );
    }

    #[test]
    fn duplicate_acknowledgements_are_idempotent() {
        let frontier = CommitFrontier::new();
        frontier.acknowledge(&position("orders", 0, 5));
        assert_eq!(
            frontier.acknowledge(&position("orders", 0, 5)),
            AckAdvance::AlreadyCovered
        );
        assert_eq!(
            frontier.acknowledge(&position("orders", 0, 3)),
            AckAdvance::AlreadyCovered
        );
        assert_eq!(
            frontier.contiguous_positions(),
            vec![position("orders", 0, 5)]
        );
    }

    #[test]
    fn failed_acknowledgement_retry_advances_exactly_once() {
        // The durable source-side commit (e.g. Kafka store_offset) can fail
        // after the in-memory frontier advanced. Retrying the acknowledgement
        // must not advance the frontier a second time: the retry observes
        // AlreadyCovered and only the fallible store is retried by the caller.
        let frontier = CommitFrontier::new();
        frontier.acknowledge(&position("orders", 0, 7));
        // First attempt's store failed; the retry re-acks the same delivery.
        assert_eq!(
            frontier.acknowledge(&position("orders", 0, 7)),
            AckAdvance::AlreadyCovered
        );
        // A genuinely failed acknowledgement (output failed, nothing acked)
        // never reaches the frontier: the next delivery lands beyond a gap and
        // waits for the failed record to replay instead of skipping it.
        assert_eq!(
            frontier.acknowledge(&position("orders", 0, 9)),
            AckAdvance::Pending { gap: 8 }
        );
        assert_eq!(
            frontier.contiguous_positions(),
            vec![position("orders", 0, 7)]
        );
    }

    #[test]
    fn partitions_track_independent_frontiers() {
        let frontier = CommitFrontier::new();
        frontier.acknowledge(&position("orders", 0, 4));
        frontier.acknowledge(&position("orders", 1, 9));
        frontier.acknowledge(&position("payments", 2, 1));
        assert_eq!(
            frontier.contiguous_positions(),
            vec![
                position("orders", 0, 4),
                position("orders", 1, 9),
                position("payments", 2, 1),
            ]
        );
    }

    #[test]
    fn seed_exposes_restored_positions_before_any_new_ack() {
        let frontier = CommitFrontier::new();
        frontier.seed(&[position("orders", 3, 42)]);
        assert_eq!(
            frontier.contiguous_positions(),
            vec![position("orders", 3, 42)]
        );
        // A re-seed replaces the cursor; restore is all-or-nothing.
        frontier.seed(&[position("orders", 3, 40)]);
        assert_eq!(
            frontier.contiguous_positions(),
            vec![position("orders", 3, 40)]
        );
        // New acknowledgements continue from the restored cursor: a gap
        // before them holds the frontier until the failed record replays.
        assert_eq!(
            frontier.acknowledge(&position("orders", 3, 44)),
            AckAdvance::Pending { gap: 41 }
        );
        assert_eq!(
            frontier.acknowledge(&position("orders", 3, 41)),
            AckAdvance::Advanced { next_offset: 41 }
        );
        assert_eq!(
            frontier.contiguous_positions(),
            vec![position("orders", 3, 41)]
        );
    }

    #[test]
    fn seal_captures_positions_watermark_and_monotonic_generation() {
        let frontier = CommitFrontier::new();
        frontier.seed(&[position("orders", 0, 10)]);
        let first = frontier.seal(Some(1_000));
        assert_eq!(first.generation, 1);
        assert_eq!(first.positions, vec![position("orders", 0, 10)]);
        assert_eq!(first.watermark_ms, Some(1_000));

        // Acknowledgements after the seal do not mutate it.
        frontier.acknowledge(&position("orders", 0, 12));
        assert_eq!(first.positions, vec![position("orders", 0, 10)]);

        // The out-of-order acknowledgement stays pending until its gap
        // closes; only then does a later seal observe the advanced frontier.
        frontier.acknowledge(&position("orders", 0, 11));
        let second = frontier.seal(Some(1_500));
        assert_eq!(second.generation, 2);
        assert_eq!(second.positions, vec![position("orders", 0, 12)]);
        assert_eq!(second.watermark_ms, Some(1_500));
    }

    #[test]
    fn delivery_anchor_prevents_first_ack_from_skipping_a_burst() {
        // A fan-out can complete the later branch first; without a delivery
        // anchor the first acknowledgement would claim the earlier records of
        // the same delivery burst were acknowledged.
        let frontier = CommitFrontier::new();
        // Records 5, 6, 7 delivered; the anchor marks record 5 as next.
        frontier.anchor_delivery(&position("orders", 0, 5));
        assert_eq!(
            frontier.acknowledge(&position("orders", 0, 8)),
            AckAdvance::Pending { gap: 6 }
        );
        assert_eq!(
            frontier.contiguous_positions(),
            vec![position("orders", 0, 5)]
        );
        // An anchor for an active partition is a no-op.
        frontier.anchor_delivery(&position("orders", 0, 99));
        assert_eq!(
            frontier.acknowledge(&position("orders", 0, 6)),
            AckAdvance::Advanced { next_offset: 6 }
        );
    }

    #[tokio::test]
    async fn released_held_ack_blocks_checkpoint_until_completion() {
        let tracker = Arc::new(AckTracker::new());
        let ack: Arc<dyn Ack> = Arc::new(TrackingAck::new(
            tracker.clone(),
            Arc::new(crate::input::NoopAck),
        ));
        ack.mark_held();
        assert_eq!(tracker.blocking(), 0);

        // Once a buffered row is handed to the downstream state/output path,
        // it must count toward the checkpoint cut again until its final ack.
        ack.release_held();
        assert_eq!(tracker.blocking(), 1);
        ack.ack().await.unwrap();
        assert_eq!(tracker.blocking(), 0);
    }

    #[tokio::test]
    async fn aborted_ack_no_longer_blocks_checkpoint() {
        let tracker = Arc::new(AckTracker::new());
        let ack: Arc<dyn Ack> = Arc::new(TrackingAck::new(
            tracker.clone(),
            Arc::new(crate::input::NoopAck),
        ));
        assert_eq!(tracker.blocking(), 1);
        ack.abort().await.unwrap();
        assert_eq!(tracker.blocking(), 0);
        // Abort is terminal and idempotent from the tracker perspective.
        ack.abort().await.unwrap();
        assert_eq!(tracker.blocking(), 0);
    }
}
