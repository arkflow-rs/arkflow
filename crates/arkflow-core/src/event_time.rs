//! Deterministic event-time primitives for distributed Jobs.

use crate::job::{LateEventPolicy, TimeSpec, WatermarkStrategy};
use crate::{Error, MessageBatch};
use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::datatypes::DataType;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionProgress {
    pub watermark_ms: i64,
    pub last_event_at_ms: i64,
    pub idle: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowAction {
    Hold,
    Emit,
    Update,
    Drop,
    Route,
}

#[derive(Debug, Default)]
pub struct EventTimeMetrics {
    pub watermark_lag_ms: AtomicI64,
    pub late_events_total: AtomicU64,
    pub late_events_dropped: AtomicU64,
    pub late_events_routed: AtomicU64,
    pub late_events_updated: AtomicU64,
}

impl EventTimeMetrics {
    pub fn record_watermark_lag(&self, lag_ms: i64) {
        self.watermark_lag_ms
            .store(lag_ms.max(0), Ordering::Relaxed);
    }

    pub fn record_action(&self, action: WindowAction) {
        match action {
            WindowAction::Update => {
                self.late_events_total.fetch_add(1, Ordering::Relaxed);
                self.late_events_updated.fetch_add(1, Ordering::Relaxed);
            }
            WindowAction::Route => {
                self.late_events_total.fetch_add(1, Ordering::Relaxed);
                self.late_events_routed.fetch_add(1, Ordering::Relaxed);
            }
            WindowAction::Drop => {
                self.late_events_total.fetch_add(1, Ordering::Relaxed);
                self.late_events_dropped.fetch_add(1, Ordering::Relaxed);
            }
            WindowAction::Hold | WindowAction::Emit => {}
        }
    }

    pub fn late_events_total(&self) -> u64 {
        self.late_events_total.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone)]
pub struct WatermarkTracker {
    strategy: WatermarkStrategy,
    out_of_orderness_ms: i64,
    idle_timeout_ms: Option<i64>,
    partitions: BTreeMap<u32, PartitionProgress>,
    watermark_ms: Option<i64>,
}

impl WatermarkTracker {
    pub fn from_time_spec(time: &TimeSpec) -> Result<Self, Error> {
        let Some(watermark) = &time.watermark else {
            return Err(Error::Config(
                "watermark tracker requires a watermark specification".into(),
            ));
        };
        Ok(Self {
            strategy: watermark.strategy,
            out_of_orderness_ms: watermark.out_of_orderness_ms as i64,
            idle_timeout_ms: watermark.idle_timeout_ms.map(|value| value as i64),
            partitions: BTreeMap::new(),
            watermark_ms: None,
        })
    }

    pub fn observe(&mut self, partition: u32, event_time_ms: i64, observed_at_ms: i64) -> i64 {
        let candidate = match self.strategy {
            WatermarkStrategy::BoundedOutOfOrderness => {
                event_time_ms.saturating_sub(self.out_of_orderness_ms)
            }
            WatermarkStrategy::Monotonous => event_time_ms,
        };
        let progress = self
            .partitions
            .entry(partition)
            .or_insert(PartitionProgress {
                watermark_ms: candidate,
                last_event_at_ms: observed_at_ms,
                idle: false,
            });
        progress.watermark_ms = progress.watermark_ms.max(candidate);
        progress.last_event_at_ms = observed_at_ms;
        progress.idle = false;
        self.recompute()
    }

    pub fn mark_idle(&mut self, partition: u32) -> Option<i64> {
        let progress = self.partitions.get_mut(&partition)?;
        progress.idle = true;
        Some(self.recompute())
    }

    pub fn refresh_idle(&mut self, now_ms: i64) -> i64 {
        if let Some(timeout_ms) = self.idle_timeout_ms {
            for progress in self.partitions.values_mut() {
                if now_ms.saturating_sub(progress.last_event_at_ms) >= timeout_ms {
                    progress.idle = true;
                }
            }
        }
        self.recompute()
    }

    pub fn watermark(&self) -> Option<i64> {
        self.watermark_ms
    }

    pub fn partition_progress(&self) -> &BTreeMap<u32, PartitionProgress> {
        &self.partitions
    }

    pub fn restore_partition(&mut self, partition: u32, watermark_ms: i64) {
        self.partitions.insert(
            partition,
            PartitionProgress {
                watermark_ms,
                last_event_at_ms: crate::state::now_ms() as i64,
                idle: false,
            },
        );
        // Recovery installs partitions one by one. Recompute from the full
        // restored set instead of applying the normal runtime monotonic-max
        // rule: restoring a high partition first must not make the global
        // watermark skip a lower partition restored immediately afterwards.
        self.watermark_ms = None;
        self.recompute();
    }

    fn recompute(&mut self) -> i64 {
        let next = self
            .partitions
            .values()
            .filter(|progress| !progress.idle)
            .map(|progress| progress.watermark_ms)
            .min();
        if let Some(next) = next {
            self.watermark_ms = Some(self.watermark_ms.map_or(next, |current| current.max(next)));
        }
        self.watermark_ms.unwrap_or(i64::MIN)
    }
}

pub fn window_action(
    window_end_ms: i64,
    event_time_ms: i64,
    watermark_ms: Option<i64>,
    allowed_lateness_ms: u64,
    policy: LateEventPolicy,
) -> WindowAction {
    let Some(watermark_ms) = watermark_ms else {
        return WindowAction::Hold;
    };
    if watermark_ms < window_end_ms {
        return WindowAction::Hold;
    }
    if event_time_ms >= window_end_ms {
        return WindowAction::Hold;
    }
    let deadline = window_end_ms.saturating_add(allowed_lateness_ms as i64);
    if watermark_ms <= deadline {
        match policy {
            LateEventPolicy::Update => WindowAction::Update,
            LateEventPolicy::Route => WindowAction::Route,
            LateEventPolicy::Drop => WindowAction::Drop,
        }
    } else {
        match policy {
            LateEventPolicy::Route => WindowAction::Route,
            LateEventPolicy::Update | LateEventPolicy::Drop => WindowAction::Drop,
        }
    }
}

pub trait TimestampExtractor: Send + Sync {
    fn extract_timestamp_ms(&self, batch: &MessageBatch) -> Result<i64, Error>;
}

#[derive(Debug, Clone)]
pub struct FieldTimestampExtractor {
    pub field: String,
}

impl TimestampExtractor for FieldTimestampExtractor {
    fn extract_timestamp_ms(&self, batch: &MessageBatch) -> Result<i64, Error> {
        self.extract_timestamp_at_zero(batch)?
            .ok_or_else(|| Error::Read("timestamp field contains no value".into()))
    }
}

impl FieldTimestampExtractor {
    fn timestamp_array<'a>(&self, batch: &'a MessageBatch) -> Result<&'a dyn Array, Error> {
        let Some(array) = batch.record_batch().column_by_name(&self.field) else {
            return Err(Error::Config(format!(
                "timestamp field '{}' is missing",
                self.field
            )));
        };
        Ok(array.as_ref())
    }

    /// Checked conversion of one raw timestamp value to milliseconds.
    /// Negative times round toward negative infinity (`div_euclid`), matching
    /// the window-boundary rule so pre-epoch timestamps assign
    /// deterministically; overflow returns an actionable error instead of
    /// silently wrapping.
    fn to_milliseconds(
        &self,
        value: i64,
        unit: &datafusion::arrow::datatypes::TimeUnit,
    ) -> Result<i64, Error> {
        use datafusion::arrow::datatypes::TimeUnit;
        match unit {
            TimeUnit::Second => value.checked_mul(1_000).ok_or_else(|| {
                Error::Process(format!(
                    "timestamp field '{}' overflows milliseconds (second-unit value {value})",
                    self.field
                ))
            }),
            TimeUnit::Millisecond => Ok(value),
            TimeUnit::Microsecond => Ok(value.div_euclid(1_000)),
            TimeUnit::Nanosecond => Ok(value.div_euclid(1_000_000)),
        }
    }

    fn extract_timestamp_at_zero(&self, batch: &MessageBatch) -> Result<Option<i64>, Error> {
        Ok(self
            .extract_timestamps_ms(batch)?
            .into_iter()
            .next()
            .flatten())
    }

    /// Extract every row's timestamp normalized to milliseconds. Supported
    /// column types: Int64 (already milliseconds) and Arrow
    /// `Timestamp(Second | Millisecond | Microsecond | Nanosecond, _)`.
    /// Nulls are preserved as `None`; unsupported types and values that
    /// cannot be represented in milliseconds fail with a field/type error.
    pub fn extract_timestamps_ms(&self, batch: &MessageBatch) -> Result<Vec<Option<i64>>, Error> {
        let array = self.timestamp_array(batch)?;
        match array.data_type() {
            DataType::Int64 => Ok(array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 data type checked above")
                .iter()
                .collect()),
            DataType::Timestamp(unit, _) => {
                let casted = datafusion::arrow::compute::cast(array, &DataType::Int64)
                    .map_err(|error| {
                        Error::Process(format!(
                            "timestamp field '{}' cannot cast to i64: {error}",
                            self.field
                        ))
                    })?;
                let values = casted
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("cast target is Int64");
                values
                    .iter()
                    .map(|value| {
                        value
                            .map(|value| self.to_milliseconds(value, unit))
                            .transpose()
                    })
                    .collect()
            }
            other => Err(Error::Config(format!(
                "timestamp field '{}' has unsupported type {other:?}; expected Int64 (milliseconds) or Arrow Timestamp(seconds|milliseconds|microseconds|nanoseconds)",
                self.field
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::{TimeMode, WatermarkSpec};

    fn spec() -> TimeSpec {
        TimeSpec {
            mode: TimeMode::EventTime,
            timestamp_field: Some("ts".into()),
            watermark: Some(WatermarkSpec {
                strategy: WatermarkStrategy::BoundedOutOfOrderness,
                out_of_orderness_ms: 100,
                idle_timeout_ms: Some(1_000),
            }),
            allowed_lateness_ms: 100,
            late_event_policy: LateEventPolicy::Update,
            late_event_route: None,
        }
    }

    #[test]
    fn watermark_waits_for_the_slowest_active_partition() {
        let mut tracker = WatermarkTracker::from_time_spec(&spec()).unwrap();
        assert_eq!(tracker.observe(1, 700, 1_000), 600);
        assert_eq!(tracker.observe(0, 1_000, 1_000), 600);
        assert_eq!(tracker.mark_idle(1), Some(900));
    }

    #[test]
    fn idle_timeout_unblocks_watermark() {
        let mut tracker = WatermarkTracker::from_time_spec(&spec()).unwrap();
        tracker.observe(0, 1_000, 0);
        tracker.observe(1, 500, 0);
        assert_eq!(tracker.refresh_idle(1_000), 900);
    }

    #[test]
    fn late_event_policy_is_deterministic() {
        assert_eq!(
            window_action(1_000, 900, Some(1_050), 100, LateEventPolicy::Update),
            WindowAction::Update
        );
        assert_eq!(
            window_action(1_000, 900, Some(1_101), 100, LateEventPolicy::Update),
            WindowAction::Drop
        );
        assert_eq!(
            window_action(1_000, 900, Some(2_000), 100, LateEventPolicy::Route),
            WindowAction::Route
        );
    }

    #[test]
    fn timestamp_extraction_accepts_every_arrow_unit_with_nulls() {
        use datafusion::arrow::array::{
            TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
            TimestampSecondArray,
        };
        use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use datafusion::arrow::record_batch::RecordBatch;
        use std::sync::Arc as StdArc;

        fn batch_of(
            data_type: DataType,
            column: StdArc<dyn datafusion::arrow::array::Array>,
        ) -> MessageBatch {
            MessageBatch::new_arrow(
                RecordBatch::try_new(
                    StdArc::new(Schema::new(vec![Field::new("ts", data_type, true)])),
                    vec![column],
                )
                .unwrap(),
            )
        }
        let extractor = FieldTimestampExtractor { field: "ts".into() };
        // Seconds, including a null row.
        let seconds = batch_of(
            DataType::Timestamp(TimeUnit::Second, None),
            StdArc::new(TimestampSecondArray::from(vec![Some(1), None, Some(-2)])),
        );
        assert_eq!(
            extractor.extract_timestamps_ms(&seconds).unwrap(),
            vec![Some(1_000), None, Some(-2_000)]
        );
        // Milliseconds pass through unchanged.
        let millis = batch_of(
            DataType::Timestamp(TimeUnit::Millisecond, None),
            StdArc::new(TimestampMillisecondArray::from(vec![Some(1_500)])),
        );
        assert_eq!(
            extractor.extract_timestamps_ms(&millis).unwrap(),
            vec![Some(1_500)]
        );
        // Microseconds divide with div_euclid so negative times round toward
        // negative infinity, matching the window-boundary rule.
        let micros = batch_of(
            DataType::Timestamp(TimeUnit::Microsecond, None),
            StdArc::new(TimestampMicrosecondArray::from(vec![
                Some(1_500),
                Some(-1_500),
            ])),
        );
        assert_eq!(
            extractor.extract_timestamps_ms(&micros).unwrap(),
            vec![Some(1), Some(-2)]
        );
        // Nanoseconds.
        let nanos = batch_of(
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            StdArc::new(TimestampNanosecondArray::from(vec![Some(2_500_000_000)])),
        );
        assert_eq!(
            extractor.extract_timestamps_ms(&nanos).unwrap(),
            vec![Some(2_500)]
        );
        // Unsupported types fail with a field/type error naming the field.
        let utf8 = batch_of(
            DataType::Utf8,
            StdArc::new(datafusion::arrow::array::StringArray::from(vec!["x"])),
        );
        let error = extractor.extract_timestamps_ms(&utf8).unwrap_err();
        assert!(error.to_string().contains("ts"), "{error}");
    }

    #[test]
    fn second_unit_overflow_is_actionable_not_wrapped() {
        use datafusion::arrow::array::TimestampSecondArray;
        use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use datafusion::arrow::record_batch::RecordBatch;
        use std::sync::Arc as StdArc;
        let batch = MessageBatch::new_arrow(
            RecordBatch::try_new(
                StdArc::new(Schema::new(vec![Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Second, None),
                    false,
                )])),
                vec![StdArc::new(TimestampSecondArray::from(vec![i64::MAX]))],
            )
            .unwrap(),
        );
        let extractor = FieldTimestampExtractor { field: "ts".into() };
        let error = extractor.extract_timestamps_ms(&batch).unwrap_err();
        assert!(error.to_string().contains("overflows"), "{error}");
    }

    #[test]
    fn records_bounded_late_event_metrics() {
        let metrics = EventTimeMetrics::default();
        metrics.record_watermark_lag(25);
        metrics.record_action(WindowAction::Route);
        metrics.record_action(WindowAction::Drop);
        assert_eq!(metrics.watermark_lag_ms.load(Ordering::Relaxed), 25);
        assert_eq!(metrics.late_events_total(), 2);
    }
}
