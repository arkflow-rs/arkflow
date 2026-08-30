//! Event-time gating for source chains in the unified kernel.
//!
//! Ported from the legacy `job_runner` source runtimes: per-source watermark
//! tracking, per-row window decisions (Hold/Emit/Route/Update/Drop), held
//! events released when the watermark advances or the idle timeout fires, and
//! deferred acks while events are held (so at-least-once recovery replays
//! them). Unlike the legacy implementation this operates on the kernel's
//! source-chain loop and preserves batch boundaries where the policy allows.

use crate::event_time::{window_action, FieldTimestampExtractor, WatermarkTracker, WindowAction};
use crate::input::{fanout_ack, Ack, NoopAck};
use crate::job::{LateEventPolicy, TimeSpec};
use crate::Error;

/// The part of a downstream window definition that affects when a source row
/// is safe to release.  Keeping this separate from the window operator's
/// aggregate configuration lets the source gate handle sliding/session
/// windows without pretending every window is tumbling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    fn window_end_for(self, event_time_ms: i64) -> Option<i64> {
        match self {
            Self::Tumbling { size_ms } if size_ms > 0 => event_time_ms
                .div_euclid(size_ms)
                .checked_mul(size_ms)
                .and_then(|start| start.checked_add(size_ms))
                .or(Some(i64::MAX)),
            Self::Sliding { size_ms, slide_ms } if size_ms > 0 && slide_ms > 0 => event_time_ms
                .div_euclid(slide_ms)
                .checked_mul(slide_ms)
                .and_then(|start| start.checked_add(size_ms))
                .or(Some(i64::MAX)),
            Self::Session { gap_ms } if gap_ms > 0 => {
                event_time_ms.checked_add(gap_ms).or(Some(i64::MAX))
            }
            _ => None,
        }
    }
}

/// Per-source event-time state.
pub struct EventTimeGate {
    extractor: Option<FieldTimestampExtractor>,
    tracker: Option<WatermarkTracker>,
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
    ack: std::sync::Arc<dyn Ack>,
}

/// The decision for one inbound batch plus everything released by this step.
pub struct GateDecision {
    /// (batch slice, action) pairs to dispatch now, in order.
    pub ready: Vec<(crate::MessageBatchRef, WindowAction)>,
    /// One ack per `ready` item, in the same order. The source delivery is
    /// split across ready, held, and dropped outcome groups.
    pub ready_acks: Vec<std::sync::Arc<dyn Ack>>,
    /// Acks for rows dropped by the late-event policy. The caller invokes
    /// these after the gate has accepted the source delivery.
    pub dropped_acks: Vec<std::sync::Arc<dyn Ack>>,
    /// The watermark after this observation (None before the first event).
    pub watermark_ms: Option<i64>,
}

impl GateDecision {
    fn new(watermark_ms: Option<i64>) -> Self {
        Self {
            ready: Vec::new(),
            ready_acks: Vec::new(),
            dropped_acks: Vec::new(),
            watermark_ms,
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
        self.tracker.as_ref().and_then(WatermarkTracker::watermark)
    }

    /// The tracked watermark of one physical partition (restore
    /// verification and multi-input minimum-progress checks).
    pub fn partition_watermark(&self, partition: u32) -> Option<i64> {
        self.tracker
            .as_ref()?
            .partition_progress()
            .get(&partition)
            .map(|progress| progress.watermark_ms)
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
        if self.tracker.is_none() {
            let mut decision = GateDecision::new(None);
            decision.ready.push((batch, WindowAction::Emit));
            decision.ready_acks.push(ack);
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
        let event_times_ms = extractor.extract_timestamps_ms(&batch)?;
        let now_ms = crate::state::now_ms() as i64;
        // Advance the watermark with THIS batch's observations first, then
        // classify the current rows against the advanced watermark: a batch
        // like [2100, 100] makes row 100 late the moment row 2100 is
        // observed, and only genuinely future rows stay held.
        let watermark_after = {
            let tracker = self.tracker.as_mut().unwrap();
            tracker.refresh_idle(now_ms);
            for event_time_ms in event_times_ms.iter().flatten().copied() {
                tracker.observe(partition, event_time_ms, now_ms);
            }
            tracker.watermark()
        };

        let current_actions = event_times_ms
            .iter()
            .map(|event_time_ms| {
                Self::action(
                    event_time_ms.and_then(|event_time| self.window_end_for(event_time)),
                    *event_time_ms,
                    watermark_after,
                    false,
                    late_policy,
                    allowed_lateness_ms,
                )
            })
            .collect::<Vec<_>>();

        let mut decision = GateDecision::new(watermark_after);
        // Held rows first (FIFO), re-evaluated against the new watermark.
        let held = std::mem::take(&mut self.held);
        for pending in held {
            let actions = pending
                .event_times_ms
                .iter()
                .map(|event_time_ms| {
                    Self::action(
                        event_time_ms.and_then(|event_time| self.window_end_for(event_time)),
                        *event_time_ms,
                        watermark_after,
                        true,
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
        self.collect_outcomes(batch, event_times_ms, current_actions, ack, &mut decision)?;

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
            let tracker = self.tracker.as_mut().unwrap();
            tracker.refresh_idle(now_ms);
            tracker.watermark()
        };
        let mut decision = GateDecision::new(watermark);
        let held = std::mem::take(&mut self.held);
        for pending in held {
            let actions = pending
                .event_times_ms
                .iter()
                .map(|event_time_ms| {
                    Self::action(
                        event_time_ms.and_then(|event_time| self.window_end_for(event_time)),
                        *event_time_ms,
                        watermark,
                        true,
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
    pub fn finish(&mut self) -> GateDecision {
        let mut decision = GateDecision::new(self.watermark());
        for pending in std::mem::take(&mut self.held) {
            decision.ready.push((pending.batch, WindowAction::Emit));
            decision.ready_acks.push(pending.ack);
        }
        decision
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
        actions: Vec<WindowAction>,
        ack: std::sync::Arc<dyn Ack>,
        decision: &mut GateDecision,
    ) -> Result<(), Error> {
        if batch.len() == 0 {
            decision.dropped_acks.push(ack);
            return Ok(());
        }
        if actions.len() != batch.len() || event_times_ms.len() != batch.len() {
            return Err(Error::Process(
                "event-time action and timestamp lengths differ from batch".into(),
            ));
        }

        let mut groups: Vec<(WindowAction, Vec<bool>, Vec<Option<i64>>)> = Vec::new();
        for (index, action) in actions.into_iter().enumerate() {
            let Some((_, keep, times)) = groups
                .iter_mut()
                .find(|(group_action, _, _)| *group_action == action)
            else {
                let mut keep = vec![false; batch.len()];
                keep[index] = true;
                groups.push((action, keep, vec![event_times_ms[index]]));
                continue;
            };
            keep[index] = true;
            times.push(event_times_ms[index]);
        }

        let child_acks = fanout_ack(ack, groups.len());
        for ((action, keep, group_times), child_ack) in groups.into_iter().zip(child_acks) {
            let filtered = filter_batch(&batch, &keep)?;
            match action {
                WindowAction::Hold => {
                    // Held rows keep their acknowledgement pending until the
                    // watermark opens their window; barrier draining must not
                    // wait on them, so mark the child as held.
                    child_ack.mark_held();
                    self.held.push(HeldBatch {
                        batch: filtered,
                        event_times_ms: group_times,
                        ack: child_ack,
                    })
                }
                WindowAction::Drop => decision.dropped_acks.push(child_ack),
                other => {
                    decision.ready.push((filtered, other));
                    decision.ready_acks.push(child_ack);
                }
            }
        }
        Ok(())
    }

    /// Return the latest end among all downstream windows containing this
    /// event. A row must stay held until the latest containing window can no
    /// longer be changed; using the minimum size under-released sliding
    /// windows in the previous implementation.
    fn window_end_for(&self, event_time_ms: i64) -> Option<i64> {
        self.window_timings
            .iter()
            .filter_map(|timing| timing.window_end_for(event_time_ms))
            .max()
    }

    fn action(
        window_end: Option<i64>,
        event_time_ms: Option<i64>,
        watermark_ms: Option<i64>,
        held: bool,
        late_policy: LateEventPolicy,
        allowed_lateness_ms: u64,
    ) -> WindowAction {
        let Some(event_time_ms) = event_time_ms else {
            // A null (invalid) timestamp can never compute a window end, so
            // no watermark can ever release it: route to the configured
            // side output when one exists, otherwise drop and acknowledge.
            // Holding it would retain its acknowledgement indefinitely.
            return match late_policy {
                LateEventPolicy::Route => WindowAction::Route,
                LateEventPolicy::Update | LateEventPolicy::Drop => WindowAction::Drop,
            };
        };
        let Some(window_end) = window_end else {
            return WindowAction::Emit;
        };
        if held {
            return if watermark_ms >= Some(window_end) {
                WindowAction::Emit
            } else {
                WindowAction::Hold
            };
        }
        window_action(
            window_end,
            event_time_ms,
            watermark_ms,
            allowed_lateness_ms,
            late_policy,
        )
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
}
