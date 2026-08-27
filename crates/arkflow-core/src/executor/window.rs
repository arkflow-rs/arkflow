//! Columnar window operator: vectorized tumbling window assignment with
//! keyed aggregation state, watermark and processing-time triggers.
//!
//! Assignment is O(1) full-column computations over the timestamp column
//! (`window_start = ts.div_euclid(size) * size`); rows are grouped per batch
//! and merged into per-(window, key) aggregate buffers held in the state
//! backend. Watermarks (or the processing-time trigger) fire windows whose
//! end has passed, emitting the aggregate batch downstream.

use crate::Error;
use crate::MessageBatchRef;
use crate::ProcessResult;
use crate::processor::Processor;
use crate::state::StateBackend;
use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayRef, Int64Array, TimestampNanosecondArray, UInt64Array,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// Serialized aggregate for one (window, key) pair. Kept as a compact JSON
/// envelope so state stays backend-agnostic; sums are i64/u64/f64.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AggregateBuffer {
    pub count: u64,
    pub sum_i64: i64,
    pub sum_float: f64,
    pub min_i64: i64,
    pub max_i64: i64,
    pub is_float: bool,
}

impl AggregateBuffer {
    pub fn merge(&mut self, other: &AggregateBuffer) {
        self.count += other.count;
        self.sum_i64 = self.sum_i64.wrapping_add(other.sum_i64);
        self.sum_float += other.sum_float;
        if other.count > 0 && self.count == other.count {
            self.min_i64 = other.min_i64;
            self.max_i64 = other.max_i64;
        } else if other.count > 0 {
            self.min_i64 = self.min_i64.min(other.min_i64);
            self.max_i64 = self.max_i64.max(other.max_i64);
        }
        self.is_float = self.is_float || other.is_float;
    }

    pub fn observe_i64(&mut self, value: i64) {
        if self.count == 0 {
            self.min_i64 = value;
            self.max_i64 = value;
        } else {
            self.min_i64 = self.min_i64.min(value);
            self.max_i64 = self.max_i64.max(value);
        }
        self.sum_i64 = self.sum_i64.wrapping_add(value);
        self.count += 1;
    }

    pub fn observe_float(&mut self, value: f64) {
        if self.count == 0 {
            self.min_i64 = i64::MIN;
            self.max_i64 = i64::MAX;
            self.is_float = true;
        }
        self.sum_float += value;
        self.count += 1;
    }
}

/// Trigger policy for a window operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WindowTrigger {
    /// Emit when the watermark passes the window end (event-time mode).
    Watermark,
    /// Emit on an interval regardless of watermark state (legacy
    /// processing-time buffer compatibility mode).
    ProcessingTime,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WindowKind {
    Tumbling { size_ms: i64 },
    /// Size-sized windows advancing every `slide_ms`: one event belongs to
    /// every window whose interval contains its timestamp.
    Sliding { size_ms: i64, slide_ms: i64 },
    /// Gap-extended windows: a new window opens when no event arrives within
    /// `gap_ms` of the previous one; the window fires after the gap passes.
    Session { gap_ms: i64 },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowOperatorConfig {
    pub kind: WindowKind,
    pub timestamp_field: String,
    pub key_field: String,
    /// Aggregated output columns: `window_start`, `window_end`, `key`,
    /// `count`, `sum`, `min`, `max`.
    #[serde(default)]
    pub value_fields: Vec<String>,
    #[serde(default = "default_trigger")]
    pub trigger: WindowTrigger,
    /// Processing-time trigger cadence in milliseconds.
    #[serde(default = "default_trigger_interval_ms")]
    pub trigger_interval_ms: u64,
    /// Watermark input field: when the input batch carries this Int64 column,
    /// its max value advances the operator's watermark.
    #[serde(default = "default_watermark_field")]
    pub watermark_field: String,
}

fn default_trigger() -> WindowTrigger {
    WindowTrigger::Watermark
}

fn default_trigger_interval_ms() -> u64 {
    5_000
}

fn default_watermark_field() -> String {
    "__watermark_ms".to_string()
}

/// The columnar window operator. One instance per stateful operator task;
/// state is namespaced under the operator id so parallel subtasks stay
/// isolated.
pub struct ColumnarWindowOperator {
    config: WindowOperatorConfig,
    backend: Arc<dyn StateBackend>,
    namespace: String,
    /// (window_start, key) -> buffer, mirroring the backend lazily.
    buffers: Mutex<BTreeMap<(i64, String), AggregateBuffer>>,
    watermark_ms: Mutex<Option<i64>>,
}

impl ColumnarWindowOperator {
    pub fn new(
        config: WindowOperatorConfig,
        backend: Arc<dyn StateBackend>,
        namespace: impl Into<String>,
    ) -> Self {
        Self {
            config,
            backend,
            namespace: namespace.into(),
            buffers: Mutex::new(BTreeMap::new()),
            watermark_ms: Mutex::new(None),
        }
    }

    /// All windows containing one event time. Tumbling yields one;
    /// sliding yields `size / slide` overlapping windows; session yields
    /// its gap-extended window (tracked per key in the buffer map).
    fn windows_for(&self, event_time_ms: i64) -> Vec<(i64, i64)> {
        match self.config.kind {
            WindowKind::Tumbling { size_ms } => {
                let start = event_time_ms.div_euclid(size_ms) * size_ms;
                vec![(start, start + size_ms)]
            }
            WindowKind::Sliding { size_ms, slide_ms } => {
                let slide_ms = slide_ms.max(1);
                // The latest window containing the event starts at
                // floor(ts/slide)*slide; the size/slide covering windows
                // precede it (each event joins every window whose interval
                // contains its timestamp).
                let last_start = event_time_ms.div_euclid(slide_ms) * slide_ms;
                let count = size_ms.div_euclid(slide_ms).max(1);
                (0..count)
                    .map(|step| last_start - step * slide_ms)
                    .filter(|start| event_time_ms >= *start && event_time_ms < start + size_ms)
                    .map(|start| (start, start + size_ms))
                    .collect()
            }
            WindowKind::Session { gap_ms } => {
                // Session windows extend per key; a conservative window for
                // assignment purposes starts at the event and ends after the
                // gap (the accumulator merges overlapping sessions per key).
                vec![(event_time_ms, event_time_ms + gap_ms)]
            }
        }
    }

    fn state_key(window_start: i64, key: &str) -> Vec<u8> {
        let mut bytes = window_start.to_be_bytes().to_vec();
        bytes.extend_from_slice(key.as_bytes());
        bytes
    }

    fn extract_timestamps(&self, batch: &crate::MessageBatch) -> Result<Vec<Option<i64>>, Error> {
        let column = batch
            .record_batch()
            .column_by_name(&self.config.timestamp_field)
            .ok_or_else(|| {
                Error::Process(format!(
                    "window timestamp field '{}' is missing",
                    self.config.timestamp_field
                ))
            })?;
        match column.data_type() {
            DataType::Int64 => Ok(column
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect()),
            DataType::Timestamp(_, _) => {
                let nanos = column
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                Ok(nanos.iter().map(|value| value.map(|v| v / 1_000_000)).collect())
            }
            DataType::Int32 | DataType::UInt32 | DataType::Date32 | DataType::Date64 => {
                let casted = cast(column, &DataType::Int64)
                    .map_err(|error| Error::Process(format!("cast timestamp: {error}")))?;
                Ok(casted
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .collect())
            }
            _ => Err(Error::Process(format!(
                "window timestamp field '{}' has unsupported type {:?}",
                self.config.timestamp_field,
                column.data_type()
            ))),
        }
    }

    fn extract_keys(&self, batch: &crate::MessageBatch) -> Result<Vec<Option<String>>, Error> {
        let column = batch
            .record_batch()
            .column_by_name(&self.config.key_field)
            .ok_or_else(|| {
                Error::Process(format!(
                    "window key field '{}' is missing",
                    self.config.key_field
                ))
            })?;
        match column.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => {
                let casted = cast(column, &DataType::Utf8)
                    .map_err(|error| Error::Process(format!("cast key: {error}")))?;
                Ok(casted
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::StringArray>()
                    .unwrap()
                    .iter()
                    .map(|value| value.map(str::to_owned))
                    .collect())
            }
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => {
                let casted = cast(column, &DataType::Utf8)
                    .map_err(|error| Error::Process(format!("cast key: {error}")))?;
                Ok(casted
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::StringArray>()
                    .unwrap()
                    .iter()
                    .map(|value| value.map(str::to_owned))
                    .collect())
            }
            _ => Err(Error::Process(format!(
                "window key field '{}' has unsupported type {:?}",
                self.config.key_field,
                column.data_type()
            ))),
        }
    }

    fn observe_watermark(&self, batch: &crate::MessageBatch) {
        if let Some(column) = batch.record_batch().column_by_name(&self.config.watermark_field) {
            if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
                let max = values.iter().flatten().max();
                if let Some(max) = max {
                    let mut watermark = self.watermark_ms.lock().unwrap();
                    *watermark = Some(watermark.map_or(max, |current| current.max(max)));
                }
            }
        }
    }

    /// Merge one batch into the aggregate buffers (vectorized assignment).
    fn accumulate(&self, batch: &crate::MessageBatchRef) -> Result<(), Error> {
        let timestamps = self.extract_timestamps(batch)?;
        let keys = self.extract_keys(batch)?;
        let value_columns: Vec<&ArrayRef> = self
            .config
            .value_fields
            .iter()
            .filter_map(|field| batch.record_batch().column_by_name(field))
            .collect();
        let mut buffers = self.buffers.lock().unwrap();
        for row in 0..batch.len() {
            let (Some(event_time), Some(key)) = (&timestamps[row], &keys[row]) else {
                continue;
            };
            // Sliding windows contribute to every containing window;
            // tumbling and session contribute to their single window.
            let mut windows = self.windows_for(*event_time);
            if let WindowKind::Session { gap_ms } = self.config.kind {
                // Session semantics: extend an existing per-key session whose
                // end reaches this event (gap not exceeded), else open a new
                // one at the event time.
                let session = windows.pop().unwrap();
                let extended = buffers
                    .range(
                        (event_time - gap_ms, String::new())
                            ..(event_time + 1, String::new()),
                    )
                    .filter(|((start, window_key), _)| {
                        *window_key == *key && start + gap_ms > *event_time
                    })
                    .map(|((start, _), _)| *start)
                    .min();
                windows = vec![match extended {
                    Some(start) => (start, start + gap_ms),
                    None => session,
                }];
            }
            for (window_start, _) in windows {
            let entry = buffers.entry((window_start, key.clone())).or_default();
            if value_columns.is_empty() {
                entry.observe_i64(1);
                continue;
            }
            for column in &value_columns {
                match column.data_type() {
                    DataType::Int64 => {
                        let values = column.as_any().downcast_ref::<Int64Array>().unwrap();
                        if !values.is_null(row) {
                            entry.observe_i64(values.value(row));
                        }
                    }
                    DataType::Float64 => {
                        let values = column
                            .as_any()
                            .downcast_ref::<datafusion::arrow::array::Float64Array>()
                            .unwrap();
                        if !values.is_null(row) {
                            entry.observe_float(values.value(row));
                        }
                    }
                    _ => entry.observe_i64(1),
                }
            }
            }
        }
        Ok(())
    }

    /// Emit aggregates for windows whose end has passed the trigger
    /// threshold, persisting nothing (buffers are the working state; the
    /// barrier snapshot serializes them on demand).
    fn fire_ready(&self, threshold: i64) -> Result<Option<MessageBatchRef>, Error> {
        let window_size = match self.config.kind {
            WindowKind::Tumbling { size_ms } | WindowKind::Sliding { size_ms, .. } => size_ms,
            WindowKind::Session { gap_ms } => gap_ms,
        };
        let mut buffers = self.buffers.lock().unwrap();
        let ready: Vec<i64> = buffers
            .keys()
            .map(|(window_start, _)| *window_start)
            .filter(|start| start + window_size <= threshold)
            .collect();
        if ready.is_empty() {
            return Ok(None);
        }
        let mut starts = Vec::new();
        let mut ends = Vec::new();
        let mut key_strings: Vec<String> = Vec::new();
        let mut counts = Vec::new();
        let mut sums = Vec::new();
        let mut mins = Vec::new();
        let mut maxs = Vec::new();
        for start in ready {
            let candidates: Vec<(String, AggregateBuffer)> = buffers
                .range((start, String::new())..(start + 1, String::new()))
                .map(|((_, key), buffer)| (key.clone(), buffer.clone()))
                .collect();
            for (key, buffer) in candidates {
                buffers.remove(&(start, key.clone()));
                starts.push(start);
                ends.push(start + window_size);
                key_strings.push(key);
                counts.push(buffer.count);
                sums.push(if buffer.is_float {
                    buffer.sum_float as i64
                } else {
                    buffer.sum_i64
                });
                mins.push(buffer.min_i64);
                maxs.push(buffer.max_i64);
            }
        }
        drop(buffers);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("window_start", DataType::Int64, false),
                Field::new("window_end", DataType::Int64, false),
                Field::new("key", DataType::Utf8, false),
                Field::new("count", DataType::UInt64, false),
                Field::new("sum", DataType::Int64, false),
                Field::new("min", DataType::Int64, false),
                Field::new("max", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(starts)),
                Arc::new(Int64Array::from(ends)),
                Arc::new(datafusion::arrow::array::StringArray::from(key_strings)),
                Arc::new(UInt64Array::from(counts)),
                Arc::new(Int64Array::from(sums)),
                Arc::new(Int64Array::from(mins)),
                Arc::new(Int64Array::from(maxs)),
            ],
        )
        .map_err(|error| Error::Process(format!("build window aggregate batch: {error}")))?;
        Ok(Some(Arc::new(crate::MessageBatch::new_arrow(batch))))
    }
}

#[async_trait]
impl Processor for ColumnarWindowOperator {
    async fn process(&self, batch: MessageBatchRef) -> Result<ProcessResult, Error> {
        self.observe_watermark(&batch);
        self.accumulate(&batch)?;
        let threshold = match self.config.trigger {
            WindowTrigger::Watermark => *self.watermark_ms.lock().unwrap(),
            WindowTrigger::ProcessingTime => Some(crate::state::now_ms() as i64),
        };
        let Some(threshold) = threshold else {
            return Ok(ProcessResult::None);
        };
        match self.fire_ready(threshold)? {
            Some(emitted) => Ok(ProcessResult::Single(emitted)),
            None => Ok(ProcessResult::None),
        }
    }

    async fn close(&self) -> Result<(), Error> {
        // Buffers are working state; nothing to flush on close. Checkpoint
        // snapshots capture them via the state backend namespace.
        Ok(())
    }
}

impl ColumnarWindowOperator {
    /// Serialize the working buffers into the state backend (used by barrier
    /// snapshots and tests).
    pub fn persist_buffers(&self) -> Result<(), Error> {
        let buffers = self.buffers.lock().unwrap();
        for ((window_start, key), buffer) in buffers.iter() {
            let value = serde_json::to_vec(buffer)?;
            self.backend.put_with_ttl(
                &self.namespace,
                &Self::state_key(*window_start, key),
                &value,
                None,
                crate::state::now_ms(),
            )?;
        }
        Ok(())
    }

    /// Restore working buffers from the state backend.
    pub fn restore_buffers(&self) -> Result<usize, Error> {
        let entries = self.backend.scan(&self.namespace)?;
        let mut buffers = self.buffers.lock().unwrap();
        let mut restored = 0;
        for entry in entries {
            let buffer: AggregateBuffer = serde_json::from_slice(&entry.value)?;
            let window_start = i64::from_be_bytes(
                entry.key[..8].try_into().map_err(|_| {
                    Error::Process("corrupt window state key".into())
                })?,
            );
            let key = String::from_utf8_lossy(&entry.key[8..]).into_owned();
            buffers.insert((window_start, key), buffer);
            restored += 1;
        }
        Ok(restored)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array as I64, StringArray};

    fn batch(rows: Vec<(i64, &str, i64)>, watermark: Option<i64>) -> MessageBatchRef {
        let mut fields = vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ];
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(I64::from(rows.iter().map(|r| r.0).collect::<Vec<_>>())),
            Arc::new(StringArray::from(rows.iter().map(|r| r.1.to_string()).collect::<Vec<_>>())),
            Arc::new(I64::from(rows.iter().map(|r| r.2).collect::<Vec<_>>())),
        ];
        if let Some(watermark) = watermark {
            fields.push(Field::new("__watermark_ms", DataType::Int64, false));
            columns.push(Arc::new(I64::from(vec![watermark; rows.len()])));
        }
        Arc::new(crate::MessageBatch::new_arrow(
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap(),
        ))
    }

    fn operator(trigger: WindowTrigger, backend: Arc<dyn StateBackend>) -> ColumnarWindowOperator {
        ColumnarWindowOperator::new(
            WindowOperatorConfig {
                kind: WindowKind::Tumbling { size_ms: 10_000 },
                timestamp_field: "ts".into(),
                key_field: "key".into(),
                value_fields: vec!["value".into()],
                trigger,
                trigger_interval_ms: 1_000,
                watermark_field: "__watermark_ms".into(),
            },
            backend,
            "window-test",
        )
    }

    #[tokio::test]
    async fn aggregates_across_batches_and_fires_on_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
        let op = operator(WindowTrigger::Watermark, backend);
        // Two batches, same window [0, 10000), keys a/b.
        op.process(batch(vec![(1_000, "a", 1), (2_000, "b", 2)], None))
            .await
            .unwrap();
        let held = op.process(batch(vec![(3_000, "a", 3)], None)).await.unwrap();
        assert!(matches!(held, ProcessResult::None));
        // Watermark 10_000 fires window [0, 10000).
        let fired = op
            .process(batch(vec![(11_000, "a", 5)], Some(10_000)))
            .await
            .unwrap();
        let ProcessResult::Single(fired) = fired else {
            panic!("window should fire");
        };
        let keys = fired
            .record_batch()
            .column_by_name("key")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = fired
            .record_batch()
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let sums = fired
            .record_batch()
            .column_by_name("sum")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(keys.len(), 2);
        assert_eq!(counts.values(), &[2, 1]);
        assert_eq!(sums.value(0) + sums.value(1), 6);
        // 11_000 falls into [10000, 20000) and stays held.
        let held = op.process(batch(vec![(12_000, "a", 5)], Some(10_000))).await.unwrap();
        assert!(matches!(held, ProcessResult::None));
    }

    #[tokio::test]
    async fn processing_time_trigger_fires_immediately() {
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
        let op = operator(WindowTrigger::ProcessingTime, backend);
        // Processing-time mode uses the wall clock as the threshold: a batch
        // with ancient timestamps still fires (legacy buffering semantics).
        let now = crate::state::now_ms() as i64;
        let far_past = now - 60_000;
        let ts = far_past - (far_past % 10_000);
        let fired = op
            .process(batch(vec![(ts, "a", 1), (ts + 1, "a", 2)], None))
            .await
            .unwrap();
        assert!(matches!(fired, ProcessResult::Single(_)));
    }

    #[tokio::test]
    async fn state_persist_and_restore_reproduces_aggregates() {
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
        let op = operator(WindowTrigger::Watermark, backend.clone());
        op.process(batch(vec![(1_000, "a", 4)], None)).await.unwrap();
        op.persist_buffers().unwrap();

        let restored = operator(WindowTrigger::Watermark, backend);
        assert_eq!(restored.restore_buffers().unwrap(), 1);
        let fired = restored
            .process(batch(vec![(20_000, "z", 0)], Some(10_000)))
            .await
            .unwrap();
        let ProcessResult::Single(fired) = fired else {
            panic!("restored window should fire");
        };
        let sums = fired
            .record_batch()
            .column_by_name("sum")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(sums.values(), &[4]);
    }

    #[tokio::test]
    async fn negative_and_boundary_timestamps_assign_deterministically() {
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
        let op = operator(WindowTrigger::Watermark, backend);
        // Boundary exactly at 10_000 belongs to [10000, 20000).
        op.process(batch(vec![(10_000, "a", 1), (9_999, "a", 2), (-1, "a", 3)], None))
            .await
            .unwrap();
        let fired = op
            .process(batch(vec![(0, "z", 0)], Some(9_999)))
            .await
            .unwrap();
        let ProcessResult::Single(fired) = fired else {
            panic!("negative window should fire");
        };
        let starts = fired
            .record_batch()
            .column_by_name("window_start")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // -1 falls into [-10000, 0); 9_999 into [0, 10000).
        assert_eq!(starts.values(), &[-10_000]);
    }

    #[tokio::test]
    async fn sliding_windows_aggregate_overlapping_memberships() {
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
        let op = ColumnarWindowOperator::new(
            WindowOperatorConfig {
                kind: WindowKind::Sliding { size_ms: 10_000, slide_ms: 5_000 },
                timestamp_field: "ts".into(),
                key_field: "key".into(),
                value_fields: vec!["value".into()],
                trigger: WindowTrigger::Watermark,
                trigger_interval_ms: 1_000,
                watermark_field: "__watermark_ms".into(),
            },
            backend,
            "sliding-test",
        );
        // Event at 6_000 belongs to [0,10000) and [5000,15000).
        op.process(batch(vec![(6_000, "a", 2)], None)).await.unwrap();
        // Event at 7_000 also belongs to both; [0,10000) has both, [5000,15000) has both.
        op.process(batch(vec![(7_000, "a", 3)], None)).await.unwrap();
        let fired = op
            .process(batch(vec![(20_000, "z", 0)], Some(15_000)))
            .await
            .unwrap();
        let ProcessResult::Single(fired) = fired else { panic!("sliding should fire") };
        let starts = fired
            .record_batch()
            .column_by_name("window_start")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts = fired
            .record_batch()
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        // Both overlapping windows closed at watermark 15_000.
        assert_eq!(starts.values(), &[0, 5_000]);
        assert_eq!(counts.values(), &[2, 2]);
    }

    #[tokio::test]
    async fn session_windows_extend_within_gap_and_fire_after() {
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
        let op = ColumnarWindowOperator::new(
            WindowOperatorConfig {
                kind: WindowKind::Session { gap_ms: 1_000 },
                timestamp_field: "ts".into(),
                key_field: "key".into(),
                value_fields: vec![],
                trigger: WindowTrigger::Watermark,
                trigger_interval_ms: 1_000,
                watermark_field: "__watermark_ms".into(),
            },
            backend,
            "session-test",
        );
        // Events 3ms apart stay one session (gap 1000ms); all extend it.
        op.process(batch(vec![(1_000, "a", 0), (1_003, "a", 0)], None)).await.unwrap();
        // A far-future watermark fires the merged session.
        let fired = op
            .process(batch(vec![(5_000, "b", 0)], Some(3_000)))
            .await
            .unwrap();
        let ProcessResult::Single(fired) = fired else { panic!("session should fire") };
        let starts = fired
            .record_batch()
            .column_by_name("window_start")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // One merged session starting at the first event (1_000).
        assert_eq!(starts.values(), &[1_000]);
    }

    #[tokio::test]
    async fn float_values_aggregate() {
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
        let mut config = WindowOperatorConfig {
            kind: WindowKind::Tumbling { size_ms: 10_000 },
            timestamp_field: "ts".into(),
            key_field: "key".into(),
            value_fields: vec!["value".into()],
            trigger: WindowTrigger::Watermark,
            trigger_interval_ms: 1_000,
            watermark_field: "__watermark_ms".into(),
        };
        config.value_fields = vec!["f".into()];
        let op = ColumnarWindowOperator::new(config, backend, "float-test");
        let fields = vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("f", DataType::Float64, false),
        ];
        let record = RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            vec![
                Arc::new(Int64Array::from(vec![1_000, 2_000])),
                Arc::new(StringArray::from(vec!["a", "a"])),
                Arc::new(Float64Array::from(vec![1.5, 2.5])),
            ],
        )
        .unwrap();
        op.process(Arc::new(crate::MessageBatch::new_arrow(record)))
            .await
            .unwrap();
        let fired = op
            .process(batch(vec![(20_000, "z", 0)], Some(10_000)))
            .await
            .unwrap();
        let ProcessResult::Single(fired) = fired else {
            panic!("float window should fire");
        };
        let counts = fired
            .record_batch()
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let sums = fired
            .record_batch()
            .column_by_name("sum")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(counts.values(), &[2]);
        assert_eq!(sums.value(0), 4);
    }
}
