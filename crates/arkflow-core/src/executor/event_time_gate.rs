//! Event-time gating for source chains in the unified kernel.
//!
//! Ported from the legacy `job_runner` source runtimes: per-source watermark
//! tracking, per-row window decisions (Hold/Emit/Route/Update/Drop), held
//! events released when the watermark advances or the idle timeout fires, and
//! deferred acks while events are held (so at-least-once recovery replays
//! them). Unlike the legacy implementation this operates on the kernel's
//! source-chain loop and preserves batch boundaries where the policy allows.

use crate::checkpoint::WatermarkPosition;
use crate::event_time::{
    window_action, EventTimePartition, FieldTimestampExtractor, WatermarkTracker, WindowAction,
};
use crate::input::{fanout_ack, Ack, NoopAck};
use crate::job::{LateEventPolicy, TimeSpec};
use crate::Error;
use std::sync::Arc;

/// The part of a downstream window definition that affects when a source row
/// is safe to release.  Keeping this separate from the window operator's
/// aggregate configuration lets the source gate handle sliding/session
/// windows without pretending every window is tumbling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum WindowTiming {
    Tumbling { size_ms: i64 },
    Sliding { size_ms: i64, slide_ms: i64 },
    Session { gap_ms: i64 },
}

impl From<i64> for WindowTiming {
    fn from(size_ms: i64) -> Self {
        Self::Tumbling { size_ms }
    }
}

impl WindowTiming {
    fn window_ends_for(self, event_time_ms: i64) -> Vec<i64> {
        match self {
            Self::Tumbling { size_ms } if size_ms > 0 => event_time_ms
                .div_euclid(size_ms)
                .checked_mul(size_ms)
                .and_then(|start| start.checked_add(size_ms))
                .into_iter()
                .collect(),
            Self::Sliding { size_ms, slide_ms } if size_ms > 0 && slide_ms > 0 => {
                let Some(mut start) = event_time_ms.div_euclid(slide_ms).checked_mul(slide_ms)
                else {
                    return Vec::new();
                };
                let mut ends = Vec::new();
                loop {
                    let Some(end) = start.checked_add(size_ms) else {
                        break;
                    };
                    if end <= event_time_ms {
                        break;
                    }
                    ends.push(end);
                    let Some(previous) = start.checked_sub(slide_ms) else {
                        break;
                    };
                    start = previous;
                }
                ends
            }
            // Session boundaries are dynamic and key-dependent. The source
            // gate cannot know whether a later row will extend or bridge a
            // session, so session timing is deliberately owned by the window
            // operator rather than converted into a stale event+gap deadline.
            Self::Session { .. } => Vec::new(),
            _ => Vec::new(),
        }
    }
}

/// Per-source event-time state.
pub struct EventTimeGate {
    extractor: Option<FieldTimestampExtractor>,
    tracker: Option<Arc<std::sync::Mutex<WatermarkTracker>>>,
    late_policy: LateEventPolicy,
    allowed_lateness_ms: u64,
    window_timings: Vec<WindowTiming>,
    /// Batch slices held back until the watermark opens their window. Every
    /// slice carries the source delivery ack that owns it, so releasing an old
    /// batch can commit the correct input delivery rather than a later batch's
    /// ack.
    held: Vec<HeldBatch>,
}

struct HeldBatch {
    batch: crate::MessageBatchRef,
    event_times_ms: Vec<Option<i64>>,
    /// Window ends that already passed while this row was held behind a later
    /// containing sliding window. They must not be reintroduced when the row
    /// is finally released.
    expired_window_ends: Vec<Vec<i64>>,
    ack: std::sync::Arc<dyn Ack>,
}

#[derive(Debug, Clone)]
struct RowDecision {
    action: WindowAction,
    invalid_timestamp: bool,
    excluded_window_ends: Vec<i64>,
    /// A held row can have memberships that need a late side-output copy
    /// while its latest membership still belongs on the main path.
    route_late: bool,
    /// Window ends that should be corrected by a late Update. The window
    /// operator uses this row-local marker so other containing memberships can
    /// still be processed normally in the same delivery.
    update_window_ends: Vec<i64>,
    expired_window_ends: Vec<i64>,
}

struct OutcomeGroup {
    action: WindowAction,
    invalid_timestamp: bool,
    keep: Vec<bool>,
    times: Vec<Option<i64>>,
    exclusions: Vec<Vec<i64>>,
    updates: Vec<Vec<i64>>,
    expired: Vec<Vec<i64>>,
}

fn add_outcome_group(
    groups: &mut Vec<OutcomeGroup>,
    action: WindowAction,
    invalid_timestamp: bool,
    index: usize,
    event_time_ms: Option<i64>,
    excluded_window_ends: Vec<i64>,
    update_window_ends: Vec<i64>,
    expired_window_ends: Vec<i64>,
    batch_len: usize,
) {
    if let Some(group) = groups
        .iter_mut()
        .find(|group| group.action == action && group.invalid_timestamp == invalid_timestamp)
    {
        group.keep[index] = true;
        group.times.push(event_time_ms);
        group.exclusions.push(excluded_window_ends);
        group.updates.push(update_window_ends);
        group.expired.push(expired_window_ends);
        return;
    }
    let mut keep = vec![false; batch_len];
    keep[index] = true;
    groups.push(OutcomeGroup {
        action,
        invalid_timestamp,
        keep,
        times: vec![event_time_ms],
        exclusions: vec![excluded_window_ends],
        updates: vec![update_window_ends],
        expired: vec![expired_window_ends],
    });
}

/// The decision for one inbound batch plus everything released by this step.
pub struct GateDecision {
    /// (batch slice, action) pairs to dispatch now, in order.
    pub ready: Vec<(crate::MessageBatchRef, WindowAction)>,
    /// One ack per `ready` item, in the same order. The source delivery is
    /// split across ready, held, and dropped outcome groups.
    pub ready_acks: Vec<std::sync::Arc<dyn Ack>>,
    /// Whether each ready slice contains invalid/null timestamps. Kept
    /// parallel to `ready` so routing can attach a distinct marker.
    pub ready_invalid_timestamps: Vec<bool>,
    /// Acks for rows dropped by the late-event policy. The caller invokes
    /// these after the gate has accepted the source delivery.
    pub dropped_acks: Vec<std::sync::Arc<dyn Ack>>,
    /// Number of rows classified as late or invalid in this decision.
    pub late_event_rows: u64,
    /// The watermark after this observation (None before the first event).
    pub watermark_ms: Option<i64>,
    /// Index in `ready` at which slices from the current source observation
    /// begin. Held slices are emitted first so their data reaches a window
    /// before the watermark that released them; current slices may need the
    /// watermark control envelope first for dynamic Session deadlines.
    pub current_ready_start: Option<usize>,
}

impl GateDecision {
    fn new(watermark_ms: Option<i64>) -> Self {
        Self {
            ready: Vec::new(),
            ready_acks: Vec::new(),
            ready_invalid_timestamps: Vec::new(),
            dropped_acks: Vec::new(),
            late_event_rows: 0,
            watermark_ms,
            current_ready_start: None,
        }
    }
}

impl EventTimeGate {
    /// Processing-time sources need no gate (pass-through).
    pub fn processing_time() -> Self {
        Self {
            extractor: None,
            tracker: None,
            late_policy: LateEventPolicy::Drop,
            allowed_lateness_ms: 0,
            window_timings: Vec::new(),
            held: Vec::new(),
        }
    }

    /// Build the gate from a source's time spec. The timing list describes
    /// downstream windows; an empty list means plain event-time ordering
    /// without windows, so rows emit immediately. `i64` entries remain
    /// accepted as tumbling windows for compatibility with the original API.
    pub fn new<T: Into<WindowTiming>>(
        time: &TimeSpec,
        window_timings: Vec<T>,
    ) -> Result<Self, Error> {
        let tracker = WatermarkTracker::from_time_spec(time)?;
        Self::with_tracker(
            time,
            window_timings,
            Arc::new(std::sync::Mutex::new(tracker)),
        )
    }

    /// Build a gate sharing an event-time tracker with compatible sibling
    /// source gates. Held batches remain local to this gate; only the
    /// downstream watermark frontier is shared.
    pub fn new_with_shared_tracker<T: Into<WindowTiming>>(
        time: &TimeSpec,
        window_timings: Vec<T>,
        tracker: Arc<std::sync::Mutex<WatermarkTracker>>,
    ) -> Result<Self, Error> {
        Self::with_tracker(time, window_timings, tracker)
    }

    fn with_tracker<T: Into<WindowTiming>>(
        time: &TimeSpec,
        window_timings: Vec<T>,
        tracker: Arc<std::sync::Mutex<WatermarkTracker>>,
    ) -> Result<Self, Error> {
        Ok(Self {
            extractor: Some(FieldTimestampExtractor {
                field: time.timestamp_field.clone().ok_or_else(|| {
                    Error::Config("event-time source requires timestamp_field".into())
                })?,
            }),
            tracker: Some(tracker),
            late_policy: time.late_event_policy,
            allowed_lateness_ms: time.allowed_lateness_ms,
            window_timings: window_timings.into_iter().map(Into::into).collect(),
            held: Vec::new(),
        })
    }

    pub fn is_event_time(&self) -> bool {
        self.tracker.is_some()
    }

    /// Current watermark (restored or observed).
    pub fn watermark(&self) -> Option<i64> {
        self.tracker
            .as_ref()
            .and_then(|tracker| tracker.lock().ok()?.watermark())
    }

    /// The tracked watermark of one physical partition (restore
    /// verification and multi-input minimum-progress checks).
    pub fn partition_watermark(&self, partition: u32) -> Option<i64> {
        let tracker = self.tracker.as_ref()?.lock().ok()?;
        if let Some(progress) = tracker.partition_progress().get(&partition) {
            return Some(progress.watermark_ms);
        }
        // Connector-neutral source identities are namespaced in the
        // canonical physical map so two source edges that both expose
        // partition 0 cannot collide.  Keep this legacy numeric accessor
        // useful when the gate has exactly one matching physical partition;
        // return None for an ambiguous shared tracker instead of exposing a
        // misleading value from another source.
        let matches = tracker
            .physical_partition_progress()
            .iter()
            .filter(|(key, _)| key.partition == partition)
            .map(|(_, progress)| progress.watermark_ms)
            .collect::<Vec<_>>();
        matches.first().copied().filter(|_| matches.len() == 1)
    }

    pub fn physical_partition_watermark(&self, partition: &EventTimePartition) -> Option<i64> {
        self.tracker
            .as_ref()?
            .lock()
            .ok()?
            .partition_progress_for(partition)
            .map(|progress| progress.watermark_ms)
    }

    /// Return every known physical partition watermark for checkpointing.
    pub fn watermark_positions(&self) -> Vec<WatermarkPosition> {
        let Some(tracker) = &self.tracker else {
            return Vec::new();
        };
        let Ok(tracker) = tracker.lock() else {
            return Vec::new();
        };
        tracker
            .physical_partition_progress()
            .iter()
            .map(|(partition, progress)| {
                WatermarkPosition::new(
                    partition.topic.clone(),
                    partition.partition,
                    progress.watermark_ms,
                )
            })
            .collect()
    }

    /// Return the physical partitions currently known by this gate.  Legacy
    /// task-level checkpoints do not carry partition entries, but recovery
    /// can still seed the complete connector assignment before applying that
    /// one watermark value.  Exposing the tracker keys keeps that fallback
    /// from silently restoring only partition zero.
    pub fn known_partitions(&self) -> Vec<EventTimePartition> {
        let Some(tracker) = &self.tracker else {
            return Vec::new();
        };
        let Ok(tracker) = tracker.lock() else {
            return Vec::new();
        };
        tracker
            .physical_partition_progress()
            .keys()
            .cloned()
            .collect()
    }

    pub fn seed_partitions(&mut self, partitions: &[EventTimePartition]) {
        if let Some(tracker) = &self.tracker {
            if let Ok(mut tracker) = tracker.lock() {
                tracker.seed_partitions(partitions);
            }
        }
    }

    /// Restore a checkpointed watermark for one partition.
    pub fn restore_partition(&mut self, partition: u32, watermark_ms: i64) {
        if let Some(tracker) = &mut self.tracker {
            if let Ok(mut tracker) = tracker.lock() {
                tracker.restore_partition(partition, watermark_ms);
            }
        }
    }

    pub fn restore_partition_key(&mut self, partition: &EventTimePartition, watermark_ms: i64) {
        if let Some(tracker) = &self.tracker {
            if let Ok(mut tracker) = tracker.lock() {
                tracker.restore_partition_key(partition, watermark_ms);
            }
        }
    }

    /// Feed one batch through the gate. For event-time sources this decides
    /// per row (Hold/Emit/Route/Update/Drop), slices the batch accordingly,
    /// and re-evaluates held rows against the advanced watermark. Processing
    /// time passes the batch through untouched.
    pub fn observe(
        &mut self,
        partition: u32,
        batch: crate::MessageBatchRef,
    ) -> Result<GateDecision, Error> {
        self.observe_with_ack(partition, batch, std::sync::Arc::new(NoopAck))
    }

    /// Observe a source delivery while retaining the delivery's ack through
    /// held/released/dropped row groups. The compatibility `observe` method
    /// above remains useful for pure gate tests that do not model source
    /// commits; the kernel source loop always calls this method.
    pub fn observe_with_ack(
        &mut self,
        partition: u32,
        batch: crate::MessageBatchRef,
        ack: std::sync::Arc<dyn Ack>,
    ) -> Result<GateDecision, Error> {
        self.observe_physical_partitioned_with_ack(vec![(
            EventTimePartition::numeric(partition),
            batch,
            ack,
        )])
    }

    /// Observe all physical partition slices of one source delivery as one
    /// watermark step. Kafka may return rows from several physical partitions
    /// in a single read; observing and classifying each slice in sequence
    /// would let the first slice close windows before a slower slice has even
    /// entered the active watermark set.
    pub fn observe_partitioned_with_ack(
        &mut self,
        partitions: Vec<(u32, crate::MessageBatchRef, std::sync::Arc<dyn Ack>)>,
    ) -> Result<GateDecision, Error> {
        self.observe_physical_partitioned_with_ack(
            partitions
                .into_iter()
                .map(|(partition, batch, ack)| (EventTimePartition::numeric(partition), batch, ack))
                .collect(),
        )
    }

    /// Physical-partition variant used by connectors whose task subscribes to
    /// more than one topic or Kafka partition. Topic and partition remain
    /// attached to the watermark observation all the way through the shared
    /// tracker.
    pub fn observe_physical_partitioned_with_ack(
        &mut self,
        partitions: Vec<(
            EventTimePartition,
            crate::MessageBatchRef,
            std::sync::Arc<dyn Ack>,
        )>,
    ) -> Result<GateDecision, Error> {
        if self.tracker.is_none() {
            let mut decision = GateDecision::new(None);
            for (_, batch, ack) in partitions {
                decision.ready.push((batch, WindowAction::Emit));
                decision.ready_acks.push(ack);
                decision.ready_invalid_timestamps.push(false);
            }
            return Ok(decision);
        }
        let (extractor_field, late_policy, allowed_lateness_ms) = (
            self.extractor
                .as_ref()
                .map(|extractor| extractor.field.clone()),
            self.late_policy,
            self.allowed_lateness_ms,
        );
        let extractor = FieldTimestampExtractor {
            field: extractor_field.unwrap_or_default(),
        };
        let mut observed = Vec::with_capacity(partitions.len());
        for (partition, batch, ack) in partitions {
            let event_times_ms = extractor.extract_timestamps_ms(&batch)?;
            observed.push((partition, batch, ack, event_times_ms));
        }
        let now_ms = crate::state::now_ms() as i64;
        // Advance the watermark with THIS batch's observations first, then
        // classify the current rows against the advanced watermark: a batch
        // like [2100, 100] makes row 100 late the moment row 2100 is
        // observed, and only genuinely future rows stay held.
        let watermark_after = {
            let tracker = self.tracker.as_ref().unwrap();
            let mut tracker = tracker
                .lock()
                .map_err(|_| Error::Process("event-time tracker lock is poisoned".into()))?;
            tracker.refresh_idle(now_ms);
            for (partition, _, _, event_times_ms) in &observed {
                for event_time_ms in event_times_ms.iter().flatten().copied() {
                    tracker.observe_partition(partition, event_time_ms, now_ms);
                }
            }
            tracker.watermark()
        };

        let mut decision = GateDecision::new(watermark_after);
        // Held rows first (FIFO), re-evaluated against the new watermark.
        let held = std::mem::take(&mut self.held);
        for pending in held {
            let actions = pending
                .event_times_ms
                .iter()
                .zip(pending.expired_window_ends.iter())
                .map(|(event_time_ms, expired)| {
                    self.classify_row(
                        *event_time_ms,
                        watermark_after,
                        true,
                        expired,
                        late_policy,
                        allowed_lateness_ms,
                    )
                })
                .collect();
            self.collect_outcomes(
                pending.batch,
                pending.event_times_ms,
                actions,
                pending.ack,
                &mut decision,
            )?;
        }
        // Current batch rows: slice the batch by decision (contiguous runs
        // preserved; columnar layout kept — no per-row batch copies).
        let current_ready_start = decision.ready.len();
        for (_, batch, ack, event_times_ms) in observed {
            let current_actions = event_times_ms
                .iter()
                .map(|event_time_ms| {
                    self.classify_row(
                        *event_time_ms,
                        watermark_after,
                        false,
                        &[],
                        late_policy,
                        allowed_lateness_ms,
                    )
                })
                .collect::<Vec<_>>();
            self.collect_outcomes(batch, event_times_ms, current_actions, ack, &mut decision)?;
        }
        decision.current_ready_start = Some(current_ready_start);

        Ok(decision)
    }

    /// Re-evaluate held rows against the wall clock (idle partitions unblock
    /// the watermark). Called from the source loop's idle tick.
    pub fn refresh(&mut self) -> Result<GateDecision, Error> {
        if self.tracker.is_none() {
            return Ok(GateDecision::new(None));
        }
        let (late_policy, allowed_lateness_ms) = (self.late_policy, self.allowed_lateness_ms);
        let now_ms = crate::state::now_ms() as i64;
        let watermark = {
            let tracker = self.tracker.as_ref().unwrap();
            let mut tracker = tracker
                .lock()
                .map_err(|_| Error::Process("event-time tracker lock is poisoned".into()))?;
            tracker.refresh_idle(now_ms);
            tracker.watermark()
        };
        let mut decision = GateDecision::new(watermark);
        let held = std::mem::take(&mut self.held);
        for pending in held {
            let actions = pending
                .event_times_ms
                .iter()
                .zip(pending.expired_window_ends.iter())
                .map(|(event_time_ms, expired)| {
                    self.classify_row(
                        *event_time_ms,
                        watermark,
                        true,
                        expired,
                        late_policy,
                        allowed_lateness_ms,
                    )
                })
                .collect();
            self.collect_outcomes(
                pending.batch,
                pending.event_times_ms,
                actions,
                pending.ack,
                &mut decision,
            )?;
        }
        Ok(decision)
    }

    /// Flush held rows when a bounded source reaches EOS. There is no future
    /// watermark after EOS, so rows that were valid but still held are emitted
    /// before the source's EOS control envelope is forwarded.
    pub async fn finish(&mut self) -> Result<GateDecision, Error> {
        let mut decision = GateDecision::new(self.watermark());
        let (late_policy, allowed_lateness_ms) = (self.late_policy, self.allowed_lateness_ms);
        for pending in std::mem::take(&mut self.held) {
            let actions = pending
                .event_times_ms
                .iter()
                .zip(pending.expired_window_ends.iter())
                .map(|(event_time_ms, expired)| {
                    // Treat EOS as a watermark beyond every containing
                    // window, but still classify memberships that already
                    // closed while the row was held according to the late
                    // policy. The latest membership is the only one that is
                    // released as an on-time row.
                    self.classify_row(
                        *event_time_ms,
                        Some(i64::MAX),
                        true,
                        expired,
                        late_policy,
                        allowed_lateness_ms,
                    )
                })
                .collect::<Vec<_>>();
            // `collect_outcomes` consumes the batch and owns the source ack.
            // Keep a clone so a malformed marker or batch can still abort the
            // delivery after the gate has taken ownership of it.
            let pending_ack = pending.ack.clone();
            if let Err(error) = self.collect_outcomes(
                pending.batch,
                pending.event_times_ms,
                actions,
                pending.ack,
                &mut decision,
            ) {
                let _ = pending_ack.abort().await;
                let _ = self.abort_held().await;
                return Err(error);
            }
        }
        Ok(decision)
    }

    /// Abort every delivery still retained by the gate.  This is used when a
    /// refresh/observation or downstream dispatch fails after a row has been
    /// classified as `Hold`: merely dropping the gate would leave a WAL or
    /// source acknowledgement pending forever and would also keep its
    /// checkpoint tracker entry alive.
    pub async fn abort_held(&mut self) -> Result<(), Error> {
        let mut first_error = None;
        for ack in self.take_held_acknowledgements() {
            if let Err(error) = ack.abort().await {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Take ownership of acknowledgements retained by held rows.  Callers
    /// that need to await them should use this synchronous extraction first so
    /// the gate mutex can be released before the futures are polled.
    pub fn take_held_acknowledgements(&mut self) -> Vec<std::sync::Arc<dyn Ack>> {
        std::mem::take(&mut self.held)
            .into_iter()
            .map(|pending| pending.ack)
            .collect()
    }

    /// Compatibility hook for callers of the pre-kernel gate API. Acks now
    /// travel alongside each `GateDecision::ready` item, so there is no
    /// detached ack queue to drain.
    pub fn take_ready_acks(&mut self) -> Vec<std::sync::Arc<dyn Ack>> {
        Vec::new()
    }

    pub fn has_held(&self) -> bool {
        !self.held.is_empty()
    }

    /// Split a batch by the exact action for each row and distribute the
    /// source ack over every non-empty outcome group. There are at most five
    /// groups (Hold/Emit/Update/Drop/Route), so this remains columnar while
    /// avoiding the old bug that labeled a mixed Route/Update batch with the
    /// action of its first row.
    fn collect_outcomes(
        &mut self,
        batch: crate::MessageBatchRef,
        event_times_ms: Vec<Option<i64>>,
        decisions: Vec<RowDecision>,
        ack: std::sync::Arc<dyn Ack>,
        decision: &mut GateDecision,
    ) -> Result<(), Error> {
        if batch.len() == 0 {
            decision.dropped_acks.push(ack);
            return Ok(());
        }
        if decisions.len() != batch.len() || event_times_ms.len() != batch.len() {
            return Err(Error::Process(
                "event-time action and timestamp lengths differ from batch".into(),
            ));
        }

        let late_rows = decisions
            .iter()
            .filter(|row| {
                row.invalid_timestamp
                    || row.route_late
                    || !row.update_window_ends.is_empty()
                    || !row.excluded_window_ends.is_empty()
                    || matches!(
                        row.action,
                        WindowAction::Drop | WindowAction::Route | WindowAction::Update
                    )
            })
            .count() as u64;
        decision.late_event_rows = decision.late_event_rows.saturating_add(late_rows);

        let mut groups = Vec::new();
        for (index, row) in decisions.into_iter().enumerate() {
            add_outcome_group(
                &mut groups,
                row.action,
                row.invalid_timestamp,
                index,
                event_times_ms[index],
                row.excluded_window_ends,
                row.update_window_ends,
                row.expired_window_ends,
                batch.len(),
            );
            // A held row may have one or more containing memberships that are
            // late-routed while its latest membership remains on the main
            // path. Emit a second, row-local copy for that side branch and
            // let fanout_ack keep both outcomes tied to the source delivery.
            if row.route_late && row.action != WindowAction::Route {
                add_outcome_group(
                    &mut groups,
                    WindowAction::Route,
                    row.invalid_timestamp,
                    index,
                    event_times_ms[index],
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    batch.len(),
                );
            }
        }

        let child_acks = fanout_ack(ack, groups.len());
        for (group, child_ack) in groups.into_iter().zip(child_acks) {
            let mut filtered = filter_batch(&batch, &group.keep)?;
            if group.exclusions.iter().any(|ends| !ends.is_empty()) {
                filtered = mark_window_exclusions(filtered, &group.exclusions)?;
            }
            if group.updates.iter().any(|ends| !ends.is_empty()) {
                filtered = mark_window_updates(filtered, &group.updates)?;
            }
            match group.action {
                WindowAction::Hold => {
                    // Held rows keep their acknowledgement pending until the
                    // watermark opens their window; barrier draining must not
                    // wait on them, so mark the child as held.
                    child_ack.mark_held();
                    self.held.push(HeldBatch {
                        batch: filtered,
                        event_times_ms: group.times,
                        expired_window_ends: group.expired,
                        ack: child_ack,
                    })
                }
                WindowAction::Drop => {
                    // This child may have been held by an earlier gate pass.
                    // Re-enter the source in-flight set before the drop ack
                    // completes so a checkpoint cannot seal state/source
                    // positions before this outcome is settled.
                    child_ack.release_held();
                    decision.dropped_acks.push(child_ack)
                }
                other => {
                    child_ack.release_held();
                    decision.ready.push((filtered, other));
                    decision.ready_acks.push(child_ack);
                    decision
                        .ready_invalid_timestamps
                        .push(group.invalid_timestamp);
                }
            }
        }
        Ok(())
    }

    fn window_ends_for(&self, event_time_ms: i64) -> Vec<i64> {
        self.window_timings
            .iter()
            .flat_map(|timing| timing.window_ends_for(event_time_ms))
            .collect()
    }

    fn classify_row(
        &self,
        event_time_ms: Option<i64>,
        watermark_ms: Option<i64>,
        held: bool,
        previously_expired: &[i64],
        late_policy: LateEventPolicy,
        allowed_lateness_ms: u64,
    ) -> RowDecision {
        let Some(event_time_ms) = event_time_ms else {
            // A null (invalid) timestamp can never compute a window end, so
            // no watermark can ever release it: route to the configured
            // side output when one exists, otherwise drop and acknowledge.
            // Holding it would retain its acknowledgement indefinitely.
            return RowDecision {
                action: match late_policy {
                    LateEventPolicy::Route => WindowAction::Route,
                    LateEventPolicy::Update | LateEventPolicy::Drop => WindowAction::Drop,
                },
                invalid_timestamp: true,
                excluded_window_ends: Vec::new(),
                route_late: false,
                update_window_ends: Vec::new(),
                expired_window_ends: Vec::new(),
            };
        };
        let window_ends = self.window_ends_for(event_time_ms);
        if window_ends.is_empty() {
            return RowDecision {
                action: WindowAction::Emit,
                invalid_timestamp: false,
                excluded_window_ends: Vec::new(),
                route_late: false,
                update_window_ends: Vec::new(),
                expired_window_ends: Vec::new(),
            };
        }
        let watermark = watermark_ms.unwrap_or(i64::MIN);
        let closed = window_ends
            .iter()
            .copied()
            .filter(|end| *end <= watermark)
            .collect::<Vec<_>>();
        if held {
            let mut expired = previously_expired.to_vec();
            for end in &closed {
                if !expired.contains(end) {
                    expired.push(*end);
                }
            }
            if window_ends.iter().any(|end| *end > watermark) {
                return RowDecision {
                    action: WindowAction::Hold,
                    invalid_timestamp: false,
                    // Some containing sliding windows may already have fired
                    // while a later containing window is still open. Carry
                    // those memberships forward so the eventual release does
                    // not reintroduce the row into an expired window.
                    excluded_window_ends: match late_policy {
                        // A Drop decision is irreversible and can be marked
                        // immediately. Route/Update need to retain the row so
                        // their membership-specific action can be decided at
                        // the final release.
                        LateEventPolicy::Drop => closed.clone(),
                        LateEventPolicy::Route | LateEventPolicy::Update => Vec::new(),
                    },
                    route_late: false,
                    update_window_ends: Vec::new(),
                    expired_window_ends: expired,
                };
            }
            if !closed.is_empty() {
                // Release once the last containing window closes. The latest
                // containing window is still the row's on-time membership;
                // every earlier membership is already behind the watermark
                // when this held delivery is released and must follow the
                // configured late policy. This is deliberately based on the
                // complete set of closed memberships, not only on
                // `previously_expired`: a watermark can jump over several
                // sliding ends in one observation, and all of those ends
                // will be fired before the held row reaches the window.
                let latest_end = window_ends.iter().copied().max();
                let mut excluded_window_ends = Vec::new();
                let mut update_window_ends = Vec::new();
                let mut route_late = false;
                for end in closed.iter().copied() {
                    if Some(end) == latest_end {
                        continue;
                    }
                    match window_action(
                        end,
                        event_time_ms,
                        watermark_ms,
                        allowed_lateness_ms,
                        late_policy,
                    ) {
                        WindowAction::Drop => excluded_window_ends.push(end),
                        WindowAction::Update => update_window_ends.push(end),
                        WindowAction::Route => {
                            route_late = true;
                            // The main copy must not reintroduce a routed
                            // membership; collect_outcomes creates its side
                            // output copy separately.
                            excluded_window_ends.push(end);
                        }
                        WindowAction::Hold | WindowAction::Emit => {}
                    }
                }
                return RowDecision {
                    action: if !update_window_ends.is_empty() {
                        WindowAction::Update
                    } else {
                        WindowAction::Emit
                    },
                    invalid_timestamp: false,
                    excluded_window_ends,
                    route_late,
                    update_window_ends,
                    expired_window_ends: Vec::new(),
                };
            }
        }

        let actions = window_ends
            .iter()
            .map(|window_end| {
                window_action(
                    *window_end,
                    event_time_ms,
                    watermark_ms,
                    allowed_lateness_ms,
                    late_policy,
                )
            })
            .collect::<Vec<_>>();
        if actions.iter().any(|action| *action == WindowAction::Hold) {
            return RowDecision {
                action: WindowAction::Hold,
                invalid_timestamp: false,
                excluded_window_ends: match late_policy {
                    LateEventPolicy::Drop => closed.clone(),
                    LateEventPolicy::Route | LateEventPolicy::Update => Vec::new(),
                },
                route_late: false,
                update_window_ends: Vec::new(),
                expired_window_ends: closed,
            };
        }
        let excluded_window_ends = actions
            .iter()
            .zip(window_ends.iter())
            .filter_map(|(action, end)| (*action == WindowAction::Drop).then_some(*end))
            .collect();
        let action = if actions.iter().any(|action| *action == WindowAction::Update) {
            WindowAction::Update
        } else if actions.iter().any(|action| *action == WindowAction::Route) {
            WindowAction::Route
        } else if actions.iter().any(|action| *action == WindowAction::Drop) {
            WindowAction::Drop
        } else {
            WindowAction::Emit
        };
        RowDecision {
            action,
            invalid_timestamp: false,
            excluded_window_ends,
            route_late: false,
            update_window_ends: actions
                .iter()
                .zip(window_ends.iter())
                .filter_map(|(action, end)| (*action == WindowAction::Update).then_some(*end))
                .collect(),
            expired_window_ends: Vec::new(),
        }
    }
}

fn filter_batch(
    batch: &crate::MessageBatchRef,
    keep: &[bool],
) -> Result<crate::MessageBatchRef, Error> {
    use datafusion::arrow::array::BooleanArray;
    use datafusion::arrow::compute::filter_record_batch;

    let filtered = filter_record_batch(batch.record_batch(), &BooleanArray::from(keep.to_vec()))
        .map_err(|error| Error::Process(format!("slice event-time batch: {error}")))?;
    let mut filtered_batch = crate::MessageBatch::new_arrow(filtered);
    filtered_batch.set_input_name(batch.get_input_name());
    Ok(std::sync::Arc::new(filtered_batch))
}

/// Attach the per-row sliding-window memberships that must not be reintroduced
/// after those windows already fired while the delivery was held for a later
/// containing window. A compact CSV marker keeps the metadata Arrow-native and
/// is consumed only by the window operator.
fn mark_window_exclusions(
    batch: crate::MessageBatchRef,
    exclusions: &[Vec<i64>],
) -> Result<crate::MessageBatchRef, Error> {
    use datafusion::arrow::array::{Array, ArrayRef, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;

    if exclusions.len() != batch.len() {
        return Err(Error::Process(
            "event-time window exclusion lengths differ from batch".into(),
        ));
    }
    let marker = "__arkflow_late_window_ends";
    let mut values = exclusions
        .iter()
        .map(|ends| {
            (!ends.is_empty()).then(|| {
                let ends = ends
                    .iter()
                    .copied()
                    .collect::<std::collections::BTreeSet<_>>();
                ends.into_iter()
                    .map(|end| end.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            })
        })
        .collect::<Vec<_>>();
    if let Some(existing) = batch.record_batch().column_by_name(marker) {
        let existing = existing
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                Error::Process("event-time window exclusion marker has an invalid type".into())
            })?;
        for row in 0..batch.len() {
            let mut merged = std::collections::BTreeSet::new();
            if existing.is_valid(row) {
                for end in existing.value(row).split(',') {
                    if let Ok(end) = end.parse::<i64>() {
                        merged.insert(end);
                    }
                }
            }
            if let Some(new_values) = exclusions.get(row) {
                merged.extend(new_values.iter().copied());
            }
            values[row] = (!merged.is_empty()).then(|| {
                merged
                    .into_iter()
                    .map(|end| end.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            });
        }
    }
    let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
    let mut columns = batch.columns().to_vec();
    if let Some(index) = batch.schema().index_of(marker).ok() {
        columns[index] = Arc::new(StringArray::from(values)) as ArrayRef;
    } else {
        fields.push(Arc::new(Field::new(marker, DataType::Utf8, true)));
        columns.push(Arc::new(StringArray::from(values)) as ArrayRef);
    }
    let marked = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|error| Error::Process(format!("mark late window memberships: {error}")))?;
    let mut marked = crate::MessageBatch::new_arrow(marked);
    marked.set_input_name(batch.get_input_name());
    Ok(Arc::new(marked))
}

/// Attach the per-row sliding-window memberships that a late Update should
/// correct. Keeping this separate from `__arkflow_late_window_ends` lets the
/// window operator update closed memberships while still admitting the
/// latest containing window as a normal contribution.
fn mark_window_updates(
    batch: crate::MessageBatchRef,
    updates: &[Vec<i64>],
) -> Result<crate::MessageBatchRef, Error> {
    use datafusion::arrow::array::{Array, ArrayRef, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;

    if updates.len() != batch.len() {
        return Err(Error::Process(
            "event-time window update lengths differ from batch".into(),
        ));
    }
    let marker = "__arkflow_late_window_updates";
    let mut values = updates
        .iter()
        .map(|ends| {
            (!ends.is_empty()).then(|| {
                let ends = ends
                    .iter()
                    .copied()
                    .collect::<std::collections::BTreeSet<_>>();
                ends.into_iter()
                    .map(|end| end.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            })
        })
        .collect::<Vec<_>>();
    if let Some(existing) = batch.record_batch().column_by_name(marker) {
        let existing = existing
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                Error::Process("event-time window update marker has an invalid type".into())
            })?;
        for row in 0..batch.len() {
            let mut merged = std::collections::BTreeSet::new();
            if existing.is_valid(row) {
                for end in existing.value(row).split(',') {
                    if let Ok(end) = end.parse::<i64>() {
                        merged.insert(end);
                    }
                }
            }
            if let Some(new_values) = updates.get(row) {
                merged.extend(new_values.iter().copied());
            }
            values[row] = (!merged.is_empty()).then(|| {
                merged
                    .into_iter()
                    .map(|end| end.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            });
        }
    }
    let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
    let mut columns = batch.columns().to_vec();
    if let Some(index) = batch.schema().index_of(marker).ok() {
        columns[index] = Arc::new(StringArray::from(values)) as ArrayRef;
    } else {
        fields.push(Arc::new(Field::new(marker, DataType::Utf8, true)));
        columns.push(Arc::new(StringArray::from(values)) as ArrayRef);
    }
    let marked = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|error| Error::Process(format!("mark late window updates: {error}")))?;
    let mut marked = crate::MessageBatch::new_arrow(marked);
    marked.set_input_name(batch.get_input_name());
    Ok(Arc::new(marked))
}

fn topic_for_row(batch: &crate::MessageBatchRef, row: usize) -> Option<String> {
    use datafusion::arrow::array::{Array, MapArray, StringArray};

    let column = batch
        .record_batch()
        .column_by_name(crate::meta_columns::EXT)?;
    let map = column.as_any().downcast_ref::<MapArray>()?;
    let entries = map.entries();
    let keys = entries.column(0).as_any().downcast_ref::<StringArray>()?;
    let values = entries.column(1).as_any().downcast_ref::<StringArray>()?;
    let offsets = map.offsets();
    let start = offsets.get(row).copied()? as usize;
    let end = offsets.get(row + 1).copied()? as usize;
    (start..end)
        .find_map(|index| (keys.value(index) == "topic").then(|| values.value(index).to_owned()))
}

/// Split one source delivery by its complete physical metadata identity.
/// Kafka can legitimately return rows from several topics and partitions in a
/// single batch; numeric partition alone would merge topic-a/0 with topic-b/0
/// and let one stream's progress release the other stream's windows.
pub(crate) fn split_by_physical_partition(
    batch: &crate::MessageBatchRef,
    fallback_partition: u32,
) -> Result<Vec<(EventTimePartition, crate::MessageBatchRef)>, Error> {
    split_by_physical_partition_for_source(batch, fallback_partition, None)
}

/// Split one source delivery while preserving a stable identity for
/// connector-neutral partitions. Multiple source edges feeding one window
/// share a watermark tracker, so their fallback partition 0 values must not
/// collide merely because neither connector exposes a topic in row metadata.
pub(crate) fn split_by_physical_partition_for_source(
    batch: &crate::MessageBatchRef,
    fallback_partition: u32,
    source_id: Option<&str>,
) -> Result<Vec<(EventTimePartition, crate::MessageBatchRef)>, Error> {
    use datafusion::arrow::array::{Array, UInt32Array};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::DataType;
    use std::collections::BTreeMap;

    let fallback = |partition: u32| {
        source_id.map_or_else(
            || EventTimePartition::numeric(partition),
            |source_id| EventTimePartition::for_source(source_id, partition),
        )
    };

    let Some(column) = batch
        .record_batch()
        .column_by_name(crate::meta_columns::PARTITION)
    else {
        return Ok(vec![(fallback(fallback_partition), batch.clone())]);
    };
    let casted = if column.data_type() == &DataType::UInt32 {
        None
    } else {
        Some(cast(column, &DataType::UInt32).map_err(|error| {
            Error::Process(format!("read physical partition metadata: {error}"))
        })?)
    };
    let values = casted
        .as_ref()
        .and_then(|array| array.as_any().downcast_ref::<UInt32Array>())
        .or_else(|| column.as_any().downcast_ref::<UInt32Array>())
        .ok_or_else(|| Error::Process("physical partition metadata is not UInt32".into()))?;
    let mut groups = BTreeMap::<EventTimePartition, Vec<bool>>::new();
    for row in 0..batch.len() {
        let partition = if values.is_null(row) {
            fallback_partition
        } else {
            values.value(row)
        };
        let physical = EventTimePartition::new(topic_for_row(batch, row), partition);
        let physical = source_id.map_or(physical.clone(), |source_id| {
            physical.with_source_identity(source_id)
        });
        let keep = groups
            .entry(physical)
            .or_insert_with(|| vec![false; batch.len()]);
        keep[row] = true;
    }
    groups
        .into_iter()
        .map(|(partition, keep)| filter_batch(batch, &keep).map(|batch| (partition, batch)))
        .collect()
}

/// Compatibility wrapper for connector-neutral callers and old tests. The
/// runtime uses [`split_by_physical_partition`] so topic identity is retained.
pub(crate) fn split_by_partition(
    batch: &crate::MessageBatchRef,
    fallback_partition: u32,
) -> Result<Vec<(u32, crate::MessageBatchRef)>, Error> {
    split_by_physical_partition(batch, fallback_partition).map(|groups| {
        groups
            .into_iter()
            .map(|(partition, batch)| (partition.partition, batch))
            .collect()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::{TimeMode, WatermarkSpec, WatermarkStrategy};
    use datafusion::arrow::array::{Int64Array, StringArray, UInt32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn time_spec(lateness: LateEventPolicy) -> TimeSpec {
        TimeSpec {
            mode: TimeMode::EventTime,
            timestamp_field: Some("ts".into()),
            watermark: Some(WatermarkSpec {
                strategy: WatermarkStrategy::Monotonous,
                out_of_orderness_ms: 0,
                idle_timeout_ms: None,
            }),
            allowed_lateness_ms: 0,
            late_event_policy: lateness,
            late_event_route: None,
        }
    }

    fn batch(times: Vec<i64>) -> crate::MessageBatchRef {
        Arc::new(crate::MessageBatch::new_arrow(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("ts", DataType::Int64, false),
                    Field::new("key", DataType::Utf8, false),
                ])),
                vec![
                    Arc::new(Int64Array::from(times.clone())),
                    Arc::new(StringArray::from(
                        times.iter().map(|_| "a".to_string()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap(),
        ))
    }

    fn partitioned_batch(times: Vec<i64>, partitions: Vec<u32>) -> crate::MessageBatchRef {
        assert_eq!(times.len(), partitions.len());
        Arc::new(crate::MessageBatch::new_arrow(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("ts", DataType::Int64, false),
                    Field::new("key", DataType::Utf8, false),
                    Field::new(crate::meta_columns::PARTITION, DataType::UInt32, false),
                ])),
                vec![
                    Arc::new(Int64Array::from(times.clone())),
                    Arc::new(StringArray::from(
                        times.iter().map(|_| "a".to_string()).collect::<Vec<_>>(),
                    )),
                    Arc::new(UInt32Array::from(partitions)),
                ],
            )
            .unwrap(),
        ))
    }

    #[test]
    fn holds_rows_until_watermark_opens_window() {
        let mut gate = EventTimeGate::new(&time_spec(LateEventPolicy::Drop), vec![1_000]).unwrap();
        // Two rows in window [0,1000): held (no watermark yet).
        let first = gate.observe(0, batch(vec![100, 200])).unwrap();
        assert!(first.ready.is_empty());
        assert!(gate.has_held());
        // A row at 2_500 advances the watermark to 2_500: windows [0,1000),
        // [1000,2000) and [2000,3000) are all open at or before it, so the
        // held rows release. The 2_500 row itself (window [2000,3000)) is
        // still open — end 3000 > 2500 — but the legacy semantic emits rows
        // whose window already passed; with Monotonous watermark 2500,
        // [2000,3000) has NOT closed, so it stays held.
        let second = gate.observe(0, batch(vec![2_500])).unwrap();
        let emitted: Vec<i64> = second
            .ready
            .iter()
            .flat_map(|(batch, _)| {
                batch
                    .record_batch()
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(emitted, vec![100, 200]);
        // 2_500 remains held (its window closes at 3000).
        assert!(gate.has_held());
        // Advancing past 3000 releases the 2_500 row; the 3_500 row itself
        // lands in [3000,4000) which is still open, so it stays held.
        let third = gate.observe(0, batch(vec![3_500])).unwrap();
        let emitted: Vec<i64> = third
            .ready
            .iter()
            .flat_map(|(batch, _)| {
                batch
                    .record_batch()
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(emitted, vec![2_500]);
        assert!(gate.has_held());
        // One more push releases everything up to [5000,6000).
        let fourth = gate.observe(0, batch(vec![6_000])).unwrap();
        let emitted: Vec<i64> = fourth
            .ready
            .iter()
            .flat_map(|(batch, _)| {
                batch
                    .record_batch()
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(emitted, vec![3_500]);
        assert!(gate.has_held()); // 6_000 held in [6000,7000)
    }

    #[test]
    fn late_rows_are_dropped_with_drop_policy() {
        let mut gate = EventTimeGate::new(&time_spec(LateEventPolicy::Drop), vec![1_000]).unwrap();
        // Watermark 2_000 closes window [0,1000).
        gate.observe(0, batch(vec![2_000])).unwrap();
        // 500 is late for a closed window: dropped with Drop policy.
        let decision = gate.observe(0, batch(vec![500])).unwrap();
        assert!(decision.ready.is_empty());
        // The held 2_000 row (window [2000,3000)) remains — only the late
        // 500 is gone.
        assert!(gate.has_held());
    }

    #[test]
    fn mixed_released_and_late_rows_keep_distinct_actions() {
        let mut gate = EventTimeGate::new(&time_spec(LateEventPolicy::Route), vec![1_000]).unwrap();
        // Keep a future row held while advancing the watermark far enough to
        // close its predecessor window.
        gate.observe(0, batch(vec![2_500])).unwrap();
        let decision = gate.observe(0, batch(vec![500, 4_000])).unwrap();

        assert_eq!(decision.ready.len(), 2);
        assert_eq!(decision.ready_acks.len(), 2);
        assert_eq!(decision.ready[0].1, WindowAction::Emit);
        assert_eq!(decision.ready[1].1, WindowAction::Route);
        let released = decision.ready[0]
            .0
            .record_batch()
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let routed = decision.ready[1]
            .0
            .record_batch()
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(released.values(), &[2_500]);
        assert_eq!(routed.values(), &[500]);
        assert!(gate.has_held());
    }

    #[test]
    fn deferred_acks_release_only_when_nothing_held() {
        let mut gate = EventTimeGate::new(&time_spec(LateEventPolicy::Drop), vec![1_000]).unwrap();
        gate.observe(0, batch(vec![100])).unwrap();
        assert!(gate.has_held());
        assert!(gate.take_ready_acks().is_empty());
        // Push far past every window: watermark 6_000 releases all holds.
        gate.observe(0, batch(vec![6_000])).unwrap();
        // 6_000 itself sits in [6000,7000) which is still open.
        assert!(gate.has_held());
        gate.observe(0, batch(vec![8_000])).unwrap();
        assert!(gate.has_held()); // 8_000 held in [8000,9000)
                                  // Acks release only when nothing at all is held; feed one more tick.
        gate.observe(0, batch(vec![9_500])).unwrap();
        assert!(gate.has_held()); // 9_500 held in [9000,10000)
    }

    #[test]
    fn processing_time_gate_passes_through() {
        let mut gate = EventTimeGate::processing_time();
        let batch = batch(vec![1, 2, 3]);
        let decision = gate.observe(0, batch.clone()).unwrap();
        assert_eq!(decision.ready.len(), 1);
        assert_eq!(decision.ready[0].0.len(), 3);
    }

    #[test]
    fn sliding_release_excludes_memberships_that_closed_while_held() {
        let mut gate = EventTimeGate::new(
            &time_spec(LateEventPolicy::Drop),
            vec![WindowTiming::Sliding {
                size_ms: 5,
                slide_ms: 2,
            }],
        )
        .unwrap();
        gate.observe(0, batch(vec![4])).unwrap();
        assert!(gate.has_held());

        // The [0, 5) membership closes first, while [2, 7) and [4, 9)
        // remain open. The row must stay held, remembering that end 5 has
        // already fired.
        gate.observe(0, batch(vec![6])).unwrap();
        assert!(gate.has_held());

        let decision = gate.observe(0, batch(vec![9])).unwrap();
        assert_eq!(decision.ready.len(), 1);
        let marker = decision.ready[0]
            .0
            .record_batch()
            .column_by_name("__arkflow_late_window_ends")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .unwrap();
        assert_eq!(marker.value(0), "5,7");
    }

    #[test]
    fn sliding_route_keeps_latest_membership_on_main_path() {
        let mut gate = EventTimeGate::new(
            &time_spec(LateEventPolicy::Route),
            vec![WindowTiming::Sliding {
                size_ms: 5,
                slide_ms: 2,
            }],
        )
        .unwrap();
        gate.observe(0, batch(vec![4])).unwrap();

        // The memberships ending at 5 and 7 are late-routed, while the
        // latest membership ending at 9 remains an ordinary main-path
        // contribution. The current timestamp-9 row is still held.
        let decision = gate.observe(0, batch(vec![9])).unwrap();
        assert_eq!(decision.ready.len(), 2);
        let routed = decision
            .ready
            .iter()
            .find(|(_, action)| *action == WindowAction::Route)
            .expect("closed memberships should be routed");
        let emitted = decision
            .ready
            .iter()
            .find(|(_, action)| *action == WindowAction::Emit)
            .expect("latest membership should stay on the main path");
        for (candidate, _) in [routed, emitted] {
            let values = candidate
                .record_batch()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(values.values(), &[4]);
        }
        let exclusions = emitted
            .0
            .record_batch()
            .column_by_name("__arkflow_late_window_ends")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .unwrap();
        assert_eq!(exclusions.value(0), "5,7");
    }

    #[test]
    fn sliding_update_marks_only_closed_memberships() {
        let mut spec = time_spec(LateEventPolicy::Update);
        spec.allowed_lateness_ms = 1_000;
        let mut gate = EventTimeGate::new(
            &spec,
            vec![WindowTiming::Sliding {
                size_ms: 5,
                slide_ms: 2,
            }],
        )
        .unwrap();
        gate.observe(0, batch(vec![4])).unwrap();

        let decision = gate.observe(0, batch(vec![9])).unwrap();
        assert_eq!(decision.ready.len(), 1);
        assert_eq!(decision.ready[0].1, WindowAction::Update);
        let updates = decision.ready[0]
            .0
            .record_batch()
            .column_by_name("__arkflow_late_window_updates")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .unwrap();
        assert_eq!(updates.value(0), "5,7");
        assert!(decision.ready[0]
            .0
            .record_batch()
            .column_by_name("__arkflow_late_window_ends")
            .is_none());
    }

    #[test]
    fn all_partitions_are_observed_before_current_rows_are_classified() {
        let mut gate = EventTimeGate::new(&time_spec(LateEventPolicy::Drop), vec![1_000]).unwrap();
        // Establish a fast partition before a new, slower physical partition
        // appears in the same source delivery. The common watermark must be
        // lowered to the slow partition before classifying the fast slice.
        gate.observe(0, batch(vec![5_000])).unwrap();
        let decision = gate
            .observe_partitioned_with_ack(vec![
                (
                    0,
                    partitioned_batch(vec![1_000], vec![0]),
                    Arc::new(NoopAck),
                ),
                (1, partitioned_batch(vec![100], vec![1]), Arc::new(NoopAck)),
            ])
            .unwrap();
        assert_eq!(decision.watermark_ms, Some(100));
        assert!(decision.ready.is_empty());
        assert!(gate.has_held());
    }

    #[test]
    fn connector_neutral_sources_get_distinct_watermark_partitions() {
        let source_a = split_by_physical_partition_for_source(&batch(vec![1]), 0, Some("a"))
            .unwrap()
            .remove(0)
            .0;
        let source_b = split_by_physical_partition_for_source(&batch(vec![1]), 0, Some("b"))
            .unwrap()
            .remove(0)
            .0;

        assert_ne!(source_a, source_b);
        assert_eq!(source_a.partition, source_b.partition);
        assert_ne!(source_a.topic, source_b.topic);
    }
}

#[cfg(test)]
mod cut_consistency_tests {
    use super::*;
    use crate::job::{TimeMode, WatermarkSpec, WatermarkStrategy};
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn spec(policy: LateEventPolicy, lateness: u64) -> TimeSpec {
        TimeSpec {
            mode: TimeMode::EventTime,
            timestamp_field: Some("ts".into()),
            watermark: Some(WatermarkSpec {
                strategy: WatermarkStrategy::Monotonous,
                out_of_orderness_ms: 0,
                idle_timeout_ms: None,
            }),
            allowed_lateness_ms: lateness,
            late_event_policy: policy,
            late_event_route: None,
        }
    }

    fn nullable_batch(times: Vec<Option<i64>>) -> crate::MessageBatchRef {
        let count = times.len();
        Arc::new(crate::MessageBatch::new_arrow(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("ts", DataType::Int64, true),
                    Field::new("key", DataType::Utf8, false),
                ])),
                vec![
                    Arc::new(Int64Array::from(times)),
                    Arc::new(StringArray::from(vec!["a"; count])),
                ],
            )
            .unwrap(),
        ))
    }

    fn times_of(decision: &GateDecision) -> Vec<Option<i64>> {
        decision
            .ready
            .iter()
            .flat_map(|(batch, _)| {
                batch
                    .record_batch()
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Task 4.2: the batch watermark advances BEFORE the current rows are
    /// classified, so a batch `[2100, 100]` classifies row 100 by the
    /// configured late policy and only the genuinely future row stays held.
    #[test]
    fn batch_watermark_advances_before_classifying_current_rows() {
        // Drop policy: the old row is dropped (its acknowledgement completes
        // immediately); the future row stays held.
        let mut gate = EventTimeGate::new(&spec(LateEventPolicy::Drop, 0), vec![1_000]).unwrap();
        let decision = gate
            .observe(0, nullable_batch(vec![Some(2_100), Some(100)]))
            .unwrap();
        assert!(decision.ready.is_empty(), "Drop: nothing is ready");
        assert_eq!(decision.dropped_acks.len(), 1, "Drop: old row dropped");
        assert!(gate.has_held(), "Drop: future row stays held");

        // Route policy: the old row goes to the side output branch.
        let mut gate = EventTimeGate::new(&spec(LateEventPolicy::Route, 0), vec![1_000]).unwrap();
        let decision = gate
            .observe(0, nullable_batch(vec![Some(2_100), Some(100)]))
            .unwrap();
        assert_eq!(decision.ready.len(), 1, "Route: old row routed");
        assert_eq!(decision.ready[0].1, WindowAction::Route);
        assert_eq!(times_of(&decision), vec![Some(100)]);
        assert!(gate.has_held(), "Route: future row stays held");

        // Update policy within allowed lateness: the old row is marked for
        // update against the window aggregate.
        let mut gate =
            EventTimeGate::new(&spec(LateEventPolicy::Update, 2_000), vec![1_000]).unwrap();
        let decision = gate
            .observe(0, nullable_batch(vec![Some(2_100), Some(100)]))
            .unwrap();
        assert_eq!(decision.ready.len(), 1, "Update: old row marked");
        assert_eq!(decision.ready[0].1, WindowAction::Update);
        assert_eq!(times_of(&decision), vec![Some(100)]);
        assert!(gate.has_held(), "Update: future row stays held");
    }

    /// Task 4.2: a null timestamp cannot compute a window end; it must route
    /// (side output) or drop, never stay held holding its acknowledgement.
    #[test]
    fn null_timestamps_route_or_drop_without_indefinite_hold() {
        // With a Route policy the null row goes to the side output; the
        // valid future row stays held for its window.
        let mut gate = EventTimeGate::new(&spec(LateEventPolicy::Route, 0), vec![1_000]).unwrap();
        let decision = gate
            .observe(0, nullable_batch(vec![Some(100), None]))
            .unwrap();
        assert_eq!(decision.ready.len(), 1);
        assert_eq!(decision.ready[0].1, WindowAction::Route);
        assert_eq!(times_of(&decision), vec![None]);
        assert_eq!(decision.dropped_acks.len(), 0);
        assert!(gate.has_held(), "the valid future row stays held");

        // Without a route, the null row is dropped and acknowledged.
        let mut gate = EventTimeGate::new(&spec(LateEventPolicy::Drop, 0), vec![1_000]).unwrap();
        let decision = gate
            .observe(0, nullable_batch(vec![Some(100), None]))
            .unwrap();
        assert!(decision.ready.is_empty());
        assert_eq!(decision.dropped_acks.len(), 1);
        assert!(gate.has_held(), "the valid future row stays held");
    }

    /// Task 4.2: a held row whose window closed re-emits (and a null row
    /// held by an older gate release also routes/drops instead of looping).
    #[test]
    fn update_policy_releases_held_rows_through_watermark() {
        let mut gate =
            EventTimeGate::new(&spec(LateEventPolicy::Update, 1_000), vec![1_000]).unwrap();
        let first = gate.observe(0, nullable_batch(vec![Some(100)])).unwrap();
        assert!(first.ready.is_empty());
        assert!(gate.has_held());
        // A later batch closes [0, 1000): the held 100 was a normal future
        // row at observe time, so it emits rather than being treated late.
        let second = gate.observe(0, nullable_batch(vec![Some(1_500)])).unwrap();
        assert!(times_of(&second).contains(&Some(100)));
    }

    /// Verification 2026-09-11 (repair-unified-runtime-review-regressions
    /// task 4.2): exact counter assertions over multi-row batches for every
    /// late decision — Drop, Route, and Update — plus rows without a
    /// timestamp. `GateDecision::late_event_rows` is the value the runtime
    /// feeds into the kernel late-event counter (`executor/task.rs`,
    /// `record_late_event_rows`); the unwired `EventTimeMetrics` type is
    /// deliberately not asserted here.
    #[test]
    fn late_event_rows_count_each_late_decision_exactly() {
        // Drop: a batch [2_100, 100, 300, None, 2_500] advances the watermark
        // to 2_500 first; 100 and 300 fall into the closed [0,1000) window
        // (two drops), the null row is invalid (one drop), and the 2_100 and
        // 2_500 rows stay held in the open [2000,3000) window.
        let mut gate = EventTimeGate::new(&spec(LateEventPolicy::Drop, 0), vec![1_000]).unwrap();
        let decision = gate
            .observe(
                0,
                nullable_batch(vec![Some(2_100), Some(100), Some(300), None, Some(2_500)]),
            )
            .unwrap();
        assert_eq!(
            decision.late_event_rows, 3,
            "two late drops plus one invalid timestamp"
        );
        assert!(decision.ready.is_empty(), "Drop: nothing is ready");
        // One acknowledgement per dropped outcome group: the two late rows
        // share one group, the invalid row forms its own.
        assert_eq!(decision.dropped_acks.len(), 2);
        assert!(gate.has_held(), "the two future rows stay held");

        // Route: both late rows are routed to the side output in one grouped
        // delivery and counted; the future row stays held.
        let mut gate = EventTimeGate::new(&spec(LateEventPolicy::Route, 0), vec![1_000]).unwrap();
        let decision = gate
            .observe(0, nullable_batch(vec![Some(2_400), Some(100), Some(300)]))
            .unwrap();
        assert_eq!(decision.late_event_rows, 2, "two late routes");
        assert_eq!(decision.ready.len(), 1);
        assert_eq!(decision.ready[0].1, WindowAction::Route);
        assert_eq!(times_of(&decision), vec![Some(100), Some(300)]);
        assert!(gate.has_held(), "the future row stays held");

        // Update: within allowed lateness both late rows are marked for the
        // window update and counted; the future row stays held.
        let mut gate =
            EventTimeGate::new(&spec(LateEventPolicy::Update, 2_000), vec![1_000]).unwrap();
        let decision = gate
            .observe(0, nullable_batch(vec![Some(2_100), Some(100), Some(300)]))
            .unwrap();
        assert_eq!(decision.late_event_rows, 2, "two late updates");
        assert_eq!(decision.ready.len(), 1);
        assert_eq!(decision.ready[0].1, WindowAction::Update);
        assert_eq!(times_of(&decision), vec![Some(100), Some(300)]);
        assert!(gate.has_held(), "the future row stays held");
    }
}
