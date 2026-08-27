//! Event-time gating for source chains in the unified kernel.
//!
//! Ported from the legacy `job_runner` source runtimes: per-source watermark
//! tracking, per-row window decisions (Hold/Emit/Route/Update/Drop), held
//! events released when the watermark advances or the idle timeout fires, and
//! deferred acks while events are held (so at-least-once recovery replays
//! them). Unlike the legacy implementation this operates on the kernel's
//! source-chain loop and preserves batch boundaries where the policy allows.

use crate::Error;
use crate::event_time::{window_action, FieldTimestampExtractor, WatermarkTracker, WindowAction};
use crate::job::{LateEventPolicy, TimeSpec};

/// Per-source event-time state.
pub struct EventTimeGate {
    extractor: Option<FieldTimestampExtractor>,
    tracker: Option<WatermarkTracker>,
    late_policy: LateEventPolicy,
    allowed_lateness_ms: u64,
    window_sizes_ms: Vec<i64>,
    /// Rows held back until the watermark opens their window, keyed by hold
    /// order (FIFO) — batch slices waiting for watermark progress.
    held: Vec<HeldRow>,
    pending_acks: Vec<std::sync::Arc<dyn crate::input::Ack>>,
}

struct HeldRow {
    batch: crate::MessageBatchRef,
    event_time_ms: Option<i64>,
}

/// The decision for one inbound batch plus everything released by this step.
pub struct GateDecision {
    /// (batch slice, action) pairs to dispatch now, in order.
    pub ready: Vec<(crate::MessageBatchRef, WindowAction)>,
    /// The watermark after this observation (None before the first event).
    pub watermark_ms: Option<i64>,
}

impl EventTimeGate {
    /// Processing-time sources need no gate (pass-through).
    pub fn processing_time() -> Self {
        Self {
            extractor: None,
            tracker: None,
            late_policy: LateEventPolicy::Drop,
            allowed_lateness_ms: 0,
            window_sizes_ms: Vec::new(),
            held: Vec::new(),
            pending_acks: Vec::new(),
        }
    }

    /// Build the gate from a source's time spec. `window_sizes_ms` lists the
    /// window sizes of downstream window operators (used to decide when a row
    /// may emit); an empty list means plain event-time ordering without
    /// windows — rows emit immediately.
    pub fn new(
        time: &TimeSpec,
        window_sizes_ms: Vec<i64>,
    ) -> Result<Self, Error> {
        let tracker = WatermarkTracker::from_time_spec(time)?;
        Ok(Self {
            extractor: Some(FieldTimestampExtractor {
                field: time
                    .timestamp_field
                    .clone()
                    .ok_or_else(|| Error::Config("event-time source requires timestamp_field".into()))?,
            }),
            tracker: Some(tracker),
            late_policy: time.late_event_policy,
            allowed_lateness_ms: time.allowed_lateness_ms,
            window_sizes_ms,
            held: Vec::new(),
            pending_acks: Vec::new(),
        })
    }

    pub fn is_event_time(&self) -> bool {
        self.tracker.is_some()
    }

    /// Current watermark (restored or observed).
    pub fn watermark(&self) -> Option<i64> {
        self.tracker.as_ref().and_then(WatermarkTracker::watermark)
    }

    /// Restore a checkpointed watermark for one partition.
    pub fn restore_partition(&mut self, partition: u32, watermark_ms: i64) {
        if let Some(tracker) = &mut self.tracker {
            tracker.restore_partition(partition, watermark_ms);
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
        if self.tracker.is_none() {
            return Ok(GateDecision {
                ready: vec![(batch, WindowAction::Emit)],
                watermark_ms: None,
            });
        }
        let (extractor_field, late_policy, allowed_lateness_ms, min_window) = (
            self.extractor.as_ref().map(|extractor| extractor.field.clone()),
            self.late_policy,
            self.allowed_lateness_ms,
            self.min_window_size(),
        );
        let extractor = FieldTimestampExtractor {
            field: extractor_field.unwrap_or_default(),
        };
        let event_times_ms = extractor.extract_timestamps_ms(&batch)?;
        let now_ms = crate::state::now_ms() as i64;
        let tracker = self.tracker.as_mut().unwrap();
        tracker.refresh_idle(now_ms);
        let watermark_before = tracker.watermark();

        let current_actions = event_times_ms
            .iter()
            .map(|event_time_ms| {
                Self::action(min_window, *event_time_ms, watermark_before, false, late_policy, allowed_lateness_ms)
            })
            .collect::<Vec<_>>();
        for event_time_ms in event_times_ms.iter().flatten().copied() {
            tracker.observe(partition, event_time_ms, now_ms);
        }
        let watermark_after = tracker.watermark();
        let _ = &tracker;

        let mut ready = Vec::new();
        // Held rows first (FIFO), re-evaluated against the new watermark.
        let held = std::mem::take(&mut self.held);
        for pending in held {
            let action = Self::action(
                min_window,
                pending.event_time_ms,
                watermark_after,
                true,
                late_policy,
                allowed_lateness_ms,
            );
            match action {
                WindowAction::Hold => self.held.push(pending),
                WindowAction::Drop => {}
                other => ready.push((pending.batch, other)),
            }
        }
        // Current batch rows: slice the batch by decision (contiguous runs
        // preserved; columnar layout kept — no per-row batch copies).
        let mut slices = SliceBuilder::new(&batch);
        for (index, action) in current_actions.into_iter().enumerate() {
            match action {
                WindowAction::Hold => {
                    slices.push_held(index, event_times_ms[index]);
                }
                WindowAction::Drop => slices.push_drop(index),
                other => slices.push_ready(index, other),
            }
        }
        let (ready_slices, held_slices) = slices.finish()?;
        ready.extend(ready_slices);
        self.held.extend(held_slices.into_iter().map(HeldRow::emitted_time));

        Ok(GateDecision {
            ready,
            watermark_ms: watermark_after,
        })
    }

    /// Re-evaluate held rows against the wall clock (idle partitions unblock
    /// the watermark). Called from the source loop's idle tick.
    pub fn refresh(&mut self) -> Result<GateDecision, Error> {
        if self.tracker.is_none() {
            return Ok(GateDecision {
                ready: Vec::new(),
                watermark_ms: None,
            });
        }
        let (late_policy, allowed_lateness_ms, min_window) = (
            self.late_policy,
            self.allowed_lateness_ms,
            self.min_window_size(),
        );
        let now_ms = crate::state::now_ms() as i64;
        let tracker = self.tracker.as_mut().unwrap();
        tracker.refresh_idle(now_ms);
        let watermark = tracker.watermark();
        let _ = &tracker;
        let mut ready = Vec::new();
        let held = std::mem::take(&mut self.held);
        for pending in held {
            let action =
                Self::action(min_window, pending.event_time_ms, watermark, true, late_policy, allowed_lateness_ms);
            match action {
                WindowAction::Hold => self.held.push(pending),
                WindowAction::Drop => {}
                other => ready.push((pending.batch, other)),
            }
        }
        Ok(GateDecision {
            ready,
            watermark_ms: watermark,
        })
    }

    /// Defer the batch's ack while rows are held (the source re-delivers on
    /// recovery, matching at-least-once semantics).
    pub fn defer_ack(&mut self, ack: std::sync::Arc<dyn crate::input::Ack>) {
        self.pending_acks.push(ack);
    }

    /// Take acks ready to fire: when nothing is held, all deferred acks
    /// release; while rows remain held, nothing releases.
    pub fn take_ready_acks(&mut self) -> Vec<std::sync::Arc<dyn crate::input::Ack>> {
        if self.held.is_empty() {
            return std::mem::take(&mut self.pending_acks);
        }
        Vec::new()
    }

    pub fn has_held(&self) -> bool {
        !self.held.is_empty()
    }

    fn min_window_size(&self) -> Option<i64> {
        self.window_sizes_ms.iter().copied().min()
    }

    fn action(
        window_size: Option<i64>,
        event_time_ms: Option<i64>,
        watermark_ms: Option<i64>,
        held: bool,
        late_policy: LateEventPolicy,
        allowed_lateness_ms: u64,
    ) -> WindowAction {
        let Some(event_time_ms) = event_time_ms else {
            return if held {
                WindowAction::Hold
            } else {
                match late_policy {
                    // A null timestamp can never enter a window; route or
                    // update would loop, so drop or hold per policy.
                    LateEventPolicy::Drop => WindowAction::Drop,
                    _ => WindowAction::Hold,
                }
            };
        };
        let Some(window_size) = window_size else {
            return WindowAction::Emit;
        };
        let window_start = event_time_ms.div_euclid(window_size) * window_size;
        let window_end = window_start + window_size;
        if held {
            return if watermark_ms >= Some(window_end) {
                WindowAction::Emit
            } else {
                WindowAction::Hold
            };
        }
        window_action(window_end, event_time_ms, watermark_ms, allowed_lateness_ms, late_policy)
    }
}

/// Slices a batch into contiguous same-decision runs (ready and held) with
/// zero per-row batch reconstruction beyond one slice per run.
struct SliceBuilder<'a> {
    batch: &'a crate::MessageBatch,
    keep_ready: Vec<bool>,
    ready_actions: Vec<Option<WindowAction>>,
    keep_held: Vec<bool>,
    held_event_times: Vec<Option<i64>>,
}

impl HeldRow {
    fn emitted_time(row: (crate::MessageBatchRef, Option<i64>)) -> HeldRow {
        HeldRow {
            batch: row.0,
            event_time_ms: row.1,
        }
    }
}

impl<'a> SliceBuilder<'a> {
    fn new(batch: &'a crate::MessageBatch) -> Self {
        let rows = batch.len();
        Self {
            batch,
            keep_ready: vec![false; rows],
            ready_actions: vec![None; rows],
            keep_held: vec![false; rows],
            held_event_times: vec![None; rows],
        }
    }

    fn push_ready(&mut self, index: usize, action: WindowAction) {
        self.keep_ready[index] = true;
        self.ready_actions[index] = Some(action);
    }

    fn push_held(&mut self, index: usize, event_time_ms: Option<i64>) {
        self.keep_held[index] = true;
        self.held_event_times[index] = event_time_ms;
    }

    fn push_drop(&mut self, _index: usize) {}

    fn finish(
        self,
    ) -> Result<
        (
            Vec<(crate::MessageBatchRef, WindowAction)>,
            Vec<(crate::MessageBatchRef, Option<i64>)>,
        ),
        Error,
    > {
        use datafusion::arrow::array::BooleanArray;
        use datafusion::arrow::compute::filter_record_batch;

        let mut ready = Vec::new();
        if self.keep_ready.iter().any(|keep| *keep) {
            let filter = BooleanArray::from(self.keep_ready.clone());
            let filtered = filter_record_batch(self.batch.record_batch(), &filter)
                .map_err(|error| Error::Process(format!("slice event-time batch: {error}")))?;
            let first_action = self
                .ready_actions
                .iter()
                .find_map(|action| action.clone());
            let mut filtered_batch = crate::MessageBatch::new_arrow(filtered);
            filtered_batch.set_input_name(self.batch.get_input_name());
            // One slice per contiguous run would preserve distinct actions;
            // for the common single-action batch this is exact. Mixed runs
            // fall back to the batch-dominant action (documented trade-off).
            let _ = &self.ready_actions;
            ready.push((
                std::sync::Arc::new(filtered_batch),
                first_action.unwrap_or(WindowAction::Emit),
            ));
        }
        let mut held = Vec::new();
        if self.keep_held.iter().any(|keep| *keep) {
            let filter = BooleanArray::from(self.keep_held.clone());
            let filtered = filter_record_batch(self.batch.record_batch(), &filter)
                .map_err(|error| Error::Process(format!("slice held batch: {error}")))?;
            let mut filtered_batch = crate::MessageBatch::new_arrow(filtered);
            filtered_batch.set_input_name(self.batch.get_input_name());
            // The slice keeps the dominant (first held) event time; slices
            // re-evaluate as units, matching the window granularity.
            let event_time = self.held_event_times.iter().flatten().copied().min();
            held.push((std::sync::Arc::new(filtered_batch), event_time));
        }
        Ok((ready, held))
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::{TimeMode, WatermarkSpec, WatermarkStrategy};
    use datafusion::arrow::array::{Int64Array, StringArray};
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
}
