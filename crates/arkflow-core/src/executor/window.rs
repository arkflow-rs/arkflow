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
use crate::input::{fanout_ack, Ack, VecAck};
use crate::processor::Processor;
use crate::state::StateBackend;
use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, UInt64Array,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::collections::{BTreeMap, BTreeSet};
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
    /// Dynamic end for a session window.  Zero means the value was written by
    /// the pre-session-end state format and should use `start + gap` while it
    /// is being upgraded.
    #[serde(default)]
    pub session_end_ms: i64,
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
        self.session_end_ms = self.session_end_ms.max(other.session_end_ms);
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
    #[serde(flatten)]
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

impl WindowOperatorConfig {
    /// Validate the arithmetic and schema contract before an operator enters
    /// the executor.  Without this guard a zero-sized window could panic in
    /// `div_euclid`, while a zero trigger interval would create a busy timer.
    pub fn validate(&self) -> Result<(), Error> {
        if self.timestamp_field.trim().is_empty() {
            return Err(Error::Config(
                "window timestamp_field must not be empty".into(),
            ));
        }
        if self.key_field.trim().is_empty() {
            return Err(Error::Config("window key_field must not be empty".into()));
        }
        if self.trigger_interval_ms == 0 {
            return Err(Error::Config(
                "window trigger_interval_ms must be positive".into(),
            ));
        }
        match self.kind {
            WindowKind::Tumbling { size_ms } if size_ms > 0 => {}
            WindowKind::Sliding { size_ms, slide_ms } if size_ms > 0 && slide_ms > 0 => {
                if slide_ms > size_ms {
                    return Err(Error::Config(
                        "sliding window slide_ms must not exceed size_ms".into(),
                    ));
                }
            }
            WindowKind::Session { gap_ms } if gap_ms > 0 => {}
            WindowKind::Tumbling { .. } => {
                return Err(Error::Config(
                    "tumbling window size_ms must be positive".into(),
                ));
            }
            WindowKind::Sliding { .. } => {
                return Err(Error::Config(
                    "sliding window size_ms and slide_ms must be positive".into(),
                ));
            }
            WindowKind::Session { .. } => {
                return Err(Error::Config("session window gap_ms must be positive".into()));
            }
        }
        Ok(())
    }
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
    /// Source acknowledgements held until the corresponding aggregate is
    /// successfully written downstream.
    pending_acks: Mutex<BTreeMap<(i64, String), Vec<Arc<dyn Ack>>>>,
    watermark_ms: Mutex<Option<i64>>,
    last_processing_trigger_ms: Mutex<Option<i64>>,
    loaded: Mutex<bool>,
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
            pending_acks: Mutex::new(BTreeMap::new()),
            watermark_ms: Mutex::new(None),
            last_processing_trigger_ms: Mutex::new(None),
            loaded: Mutex::new(false),
        }
    }

    /// All windows containing one event time. Tumbling yields one;
    /// sliding yields `size / slide` overlapping windows; session yields
    /// its gap-extended window (tracked per key in the buffer map).
    fn windows_for(&self, event_time_ms: i64) -> Vec<(i64, i64)> {
        match self.config.kind {
            WindowKind::Tumbling { size_ms } => {
                let start = event_time_ms
                    .div_euclid(size_ms)
                    .saturating_mul(size_ms);
                vec![(start, start.saturating_add(size_ms))]
            }
            WindowKind::Sliding { size_ms, slide_ms } => {
                // The latest window containing the event starts at
                // floor(ts/slide)*slide; the size/slide covering windows
                // precede it (each event joins every window whose interval
                // contains its timestamp).
                let last_start = event_time_ms
                    .div_euclid(slide_ms)
                    .saturating_mul(slide_ms);
                let count = size_ms.div_euclid(slide_ms).max(1);
                (0..count)
                    .map(|step| {
                        (
                            last_start.saturating_sub(step.saturating_mul(slide_ms)),
                            last_start
                                .saturating_sub(step.saturating_mul(slide_ms))
                                .saturating_add(size_ms),
                        )
                    })
                    .filter(|(start, end)| event_time_ms >= *start && event_time_ms < *end)
                    .collect()
            }
            WindowKind::Session { gap_ms } => {
                // Session windows extend per key; a conservative window for
                // assignment purposes starts at the event and ends after the
                // gap (the accumulator merges overlapping sessions per key).
                vec![(event_time_ms, event_time_ms.saturating_add(gap_ms))]
            }
        }
    }

    fn state_key(window_start: i64, key: &str) -> Vec<u8> {
        let mut bytes = window_start.to_be_bytes().to_vec();
        bytes.extend_from_slice(key.as_bytes());
        bytes
    }

    fn session_end(window_start: i64, buffer: &AggregateBuffer, gap_ms: i64) -> i64 {
        if buffer.session_end_ms > window_start {
            buffer.session_end_ms
        } else {
            window_start.saturating_add(gap_ms)
        }
    }

    fn extract_timestamps(&self, batch: &crate::MessageBatch) -> Result<Vec<Option<i64>>, Error> {
        let Some(column) = batch
            .record_batch()
            .column_by_name(&self.config.timestamp_field)
        else {
            // Legacy Stream buffers run in processing-time mode and their
            // generated batches do not necessarily carry the optional
            // `__meta_timestamp` column.  Assigning the current processing
            // time keeps that compatibility path valid while event-time
            // windows still fail fast on a missing timestamp field.
            if self.config.trigger == WindowTrigger::ProcessingTime {
                let now = crate::state::now_ms() as i64;
                return Ok(vec![Some(now); batch.len()]);
            }
            return Err(Error::Process(format!(
                "window timestamp field '{}' is missing",
                self.config.timestamp_field
            )));
        };
        match column.data_type() {
            DataType::Int64 => Ok(column
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect()),
            DataType::Timestamp(unit, _) => {
                let casted = cast(column, &DataType::Int64)
                    .map_err(|error| Error::Process(format!("cast timestamp: {error}")))?;
                let values = casted.as_any().downcast_ref::<Int64Array>().unwrap();
                let to_ms = match unit {
                    datafusion::arrow::datatypes::TimeUnit::Second => {
                        |value: i64| value.saturating_mul(1_000)
                    }
                    datafusion::arrow::datatypes::TimeUnit::Millisecond => |value: i64| value,
                    datafusion::arrow::datatypes::TimeUnit::Microsecond => {
                        |value: i64| value / 1_000
                    }
                    datafusion::arrow::datatypes::TimeUnit::Nanosecond => {
                        |value: i64| value / 1_000_000
                    }
                };
                Ok(values.iter().map(|value| value.map(to_ms)).collect())
            }
            DataType::Date32 => {
                let casted = cast(column, &DataType::Int64)
                    .map_err(|error| Error::Process(format!("cast date: {error}")))?;
                Ok(casted
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .map(|value| value.map(|value| value.saturating_mul(86_400_000)))
                    .collect())
            }
            DataType::Int32 | DataType::UInt32 | DataType::Date64 => {
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
        let Some(column) = batch.record_batch().column_by_name(&self.config.key_field) else {
            if self.config.key_field == "__arkflow_window_all" {
                return Ok(vec![Some("__all__".to_owned()); batch.len()]);
            }
            return Err(Error::Process(format!(
                "window key field '{}' is missing",
                self.config.key_field
            )));
        };
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
    fn accumulate(&self, batch: &crate::MessageBatchRef) -> Result<Vec<(i64, String)>, Error> {
        let timestamps = self.extract_timestamps(batch)?;
        let keys = self.extract_keys(batch)?;
        let value_columns: Vec<&ArrayRef> = self
            .config
            .value_fields
            .iter()
            .filter_map(|field| batch.record_batch().column_by_name(field))
            .collect();
        let mut buffers = self.buffers.lock().unwrap();
        let mut touched = BTreeSet::new();
        let mut session_rekeys = Vec::new();
        for row in 0..batch.len() {
            let (Some(event_time), Some(key)) = (&timestamps[row], &keys[row]) else {
                continue;
            };
            // Sliding windows contribute to every containing window;
            // tumbling and session contribute to their single window.
            let mut windows = self.windows_for(*event_time);
            let mut session_seed = None;
            if let WindowKind::Session { gap_ms } = self.config.kind {
                // A session is identified by its dynamic end rather than by
                // the original `start + gap`.  Collect all matching sessions
                // first so an out-of-order event can bridge two sessions and
                // merge their aggregates into one interval.
                let matching = buffers
                    .iter()
                    .filter(|((start, window_key), buffer)| {
                        *window_key == *key
                            && event_time.saturating_add(gap_ms) >= *start
                            && *event_time
                                <= Self::session_end(*start, buffer, gap_ms)
                    })
                    .map(|((start, window_key), buffer)| {
                        ((*start, window_key.clone()), buffer.clone())
                    })
                    .collect::<Vec<_>>();
                let mut merged_start = *event_time;
                let mut merged_end = event_time.saturating_add(gap_ms);
                if !matching.is_empty() {
                    let mut merged = AggregateBuffer::default();
                    let mut matched_keys = Vec::new();
                    for ((start, window_key), buffer) in matching {
                        merged_start = merged_start.min(start);
                        merged_end = merged_end.max(Self::session_end(start, &buffer, gap_ms));
                        buffers.remove(&(start, window_key.clone()));
                        matched_keys.push((start, window_key));
                        merged.merge(&buffer);
                    }
                    merged.session_end_ms = merged_end;
                    let merged_key = (merged_start, key.clone());
                    session_rekeys.extend(
                        matched_keys
                            .into_iter()
                            .map(|old_key| (old_key, merged_key.clone())),
                    );
                    session_seed = Some(merged);
                }
                windows = vec![(merged_start, merged_end)];
            }
            for (window_start, window_end) in windows {
                touched.insert((window_start, key.clone()));
                let entry = buffers.entry((window_start, key.clone())).or_default();
                if let Some(seed) = session_seed.take() {
                    entry.merge(&seed);
                }
                if matches!(self.config.kind, WindowKind::Session { .. }) {
                    entry.session_end_ms = entry.session_end_ms.max(window_end);
                }
                if let Some(column) = value_columns.first() {
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
                } else {
                    entry.observe_i64(1);
                }
            }
        }
        drop(buffers);
        if !session_rekeys.is_empty() {
            let mut pending = self.pending_acks.lock().unwrap();
            for (old_key, new_key) in session_rekeys {
                if old_key == new_key {
                    continue;
                }
                if let Some(acks) = pending.remove(&old_key) {
                    pending.entry(new_key).or_default().extend(acks);
                }
            }
        }
        Ok(touched.into_iter().collect())
    }

    /// Emit aggregates for windows whose end has passed the trigger
    /// threshold, persisting nothing (buffers are the working state; the
    /// barrier snapshot serializes them on demand).
    fn fire_ready(
        &self,
        threshold: i64,
    ) -> Result<Option<(MessageBatchRef, Vec<(i64, String)>)>, Error> {
        let window_size = match self.config.kind {
            WindowKind::Tumbling { size_ms } | WindowKind::Sliding { size_ms, .. } => size_ms,
            WindowKind::Session { gap_ms } => gap_ms,
        };
        let mut buffers = self.buffers.lock().unwrap();
        let ready = buffers
            .iter()
            .filter(|((window_start, _), buffer)| {
                let end = match self.config.kind {
                    WindowKind::Session { gap_ms } => {
                        Self::session_end(*window_start, buffer, gap_ms)
                    }
                    _ => window_start.saturating_add(window_size),
                };
                end <= threshold
            })
            .map(|((window_start, key), buffer)| {
                ((*window_start, key.clone()), buffer.clone())
            })
            .collect::<Vec<_>>();
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
        let mut fired_keys = Vec::new();
        for ((start, key), buffer) in ready {
            buffers.remove(&(start, key.clone()));
            fired_keys.push((start, key.clone()));
            starts.push(start);
            ends.push(match self.config.kind {
                WindowKind::Session { gap_ms } => Self::session_end(start, &buffer, gap_ms),
                _ => start.saturating_add(window_size),
            });
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
        Ok(Some((
            Arc::new(crate::MessageBatch::new_arrow(batch)),
            fired_keys,
        )))
    }
}

#[async_trait]
impl Processor for ColumnarWindowOperator {
    async fn process(&self, batch: MessageBatchRef) -> Result<ProcessResult, Error> {
        self.process_internal(batch, None).await
    }

    async fn process_with_ack(
        &self,
        batch: MessageBatchRef,
        ack: Arc<dyn Ack>,
    ) -> Result<ProcessResult, Error> {
        self.process_internal(batch, Some(ack)).await
    }

    async fn finish(&self) -> Result<ProcessResult, Error> {
        self.load_from_backend()?;
        let fired = self.fire_ready(i64::MAX)?;
        self.persist_buffers()?;
        Ok(match fired {
            Some((emitted, fired_keys)) => {
                ProcessResult::SingleWithAck(emitted, Arc::new(VecAck(self.take_acks(&fired_keys))))
            }
            None => ProcessResult::None,
        })
    }

    async fn on_tick(&self) -> Result<ProcessResult, Error> {
        if self.config.trigger != WindowTrigger::ProcessingTime {
            return Ok(ProcessResult::None);
        }
        self.load_from_backend()?;
        let now = crate::state::now_ms() as i64;
        let due = {
            let mut last = self.last_processing_trigger_ms.lock().unwrap();
            let interval = self.config.trigger_interval_ms.max(1) as i64;
            match *last {
                None => {
                    *last = Some(now);
                    false
                }
                Some(previous) if now.saturating_sub(previous) >= interval => {
                    *last = Some(now);
                    true
                }
                Some(_) => false,
            }
        };
        if !due {
            return Ok(ProcessResult::None);
        }

        // Processing-time triggers flush the current buffers independent of
        // event timestamps. The timestamp only determines the aggregate key;
        // it must not prevent an idle timer from emitting old or future-dated
        // records.
        let fired = self.fire_ready(i64::MAX)?;
        self.persist_buffers()?;
        Ok(match fired {
            Some((emitted, fired_keys)) => {
                ProcessResult::SingleWithAck(emitted, Arc::new(VecAck(self.take_acks(&fired_keys))))
            }
            None => ProcessResult::None,
        })
    }

    async fn on_watermark(&self, watermark_ms: i64) -> Result<ProcessResult, Error> {
        self.load_from_backend()?;
        {
            let mut watermark = self.watermark_ms.lock().unwrap();
            *watermark = Some(watermark.map_or(watermark_ms, |current| current.max(watermark_ms)));
        }
        if self.config.trigger != WindowTrigger::Watermark {
            return Ok(ProcessResult::None);
        }
        let fired = self.fire_ready(watermark_ms)?;
        self.persist_buffers()?;
        Ok(match fired {
            Some((emitted, fired_keys)) => {
                ProcessResult::SingleWithAck(emitted, Arc::new(VecAck(self.take_acks(&fired_keys))))
            }
            None => ProcessResult::None,
        })
    }

    async fn close(&self) -> Result<(), Error> {
        // Buffers are working state; nothing to flush on close. Checkpoint
        // snapshots capture them via the state backend namespace.
        Ok(())
    }
}

impl ColumnarWindowOperator {
    async fn process_internal(
        &self,
        batch: MessageBatchRef,
        ack: Option<Arc<dyn Ack>>,
    ) -> Result<ProcessResult, Error> {
        self.load_from_backend()?;
        // A Route action is delivered to the explicitly configured late-event
        // branch by the source gate. If a route batch reaches a window (for
        // example through a compatibility graph without a synthetic branch),
        // never fold it into the normal aggregate a second time.
        if batch
            .record_batch()
            .column_by_name("__arkflow_late_event_route")
            .is_some()
        {
            if let Some(ack) = ack {
                ack.ack().await?;
            }
            return Ok(ProcessResult::None);
        }
        self.observe_watermark(&batch);
        let touched = self.accumulate(&batch)?;
        let threshold = match self.config.trigger {
            WindowTrigger::Watermark => *self.watermark_ms.lock().unwrap(),
            WindowTrigger::ProcessingTime => {
                let now = crate::state::now_ms() as i64;
                let mut last = self.last_processing_trigger_ms.lock().unwrap();
                let interval = self.config.trigger_interval_ms.max(1) as i64;
                let due = match *last {
                    // Start the cadence when the first data arrives; the
                    // first timer tick, rather than the first record, owns
                    // the emission.
                    None => false,
                    Some(previous) => now.saturating_sub(previous) >= interval,
                };
                if last.is_none() || due {
                    *last = Some(now);
                }
                if due {
                    Some(i64::MAX)
                } else {
                    None
                }
            }
        };

        let Some(ack) = ack else {
            let fired = match threshold {
                Some(threshold) => self.fire_ready(threshold)?,
                None => None,
            };
            self.persist_buffers()?;
            return Ok(match fired {
                Some((emitted, _)) => ProcessResult::Single(emitted),
                None => ProcessResult::None,
            });
        };

        // Split the source delivery before firing so each window group owns a
        // child acknowledgement. This ordering matters when the current
        // batch itself makes a processing-time or watermark-triggered window
        // ready: `fire_ready` must be able to transfer those child acks into
        // the emitted aggregate instead of leaving them stranded.
        if !touched.is_empty() {
            let group_acks = fanout_ack(ack.clone(), touched.len());
            self.remember_acks(touched.clone(), group_acks);
        }

        let fired = match threshold {
            Some(threshold) => self.fire_ready(threshold)?,
            None => None,
        };
        self.persist_buffers()?;

        let Some((emitted, fired_keys)) = fired else {
            if touched.is_empty() {
                ack.ack().await?;
            }
            return Ok(ProcessResult::Deferred);
        };

        let mut output_acks = self.take_acks(&fired_keys);
        if touched.is_empty() {
            // A watermark-only batch still has to be committed, but only
            // after the output produced by that watermark has been written.
            output_acks.push(ack);
        }
        Ok(ProcessResult::SingleWithAck(
            emitted,
            Arc::new(VecAck(output_acks)),
        ))
    }
}

impl ColumnarWindowOperator {
    fn load_from_backend(&self) -> Result<(), Error> {
        let mut loaded = self.loaded.lock().unwrap();
        if *loaded {
            return Ok(());
        }
        self.restore_buffers_inner()?;
        *loaded = true;
        Ok(())
    }

    fn remember_acks(&self, keys: Vec<(i64, String)>, acks: Vec<Arc<dyn Ack>>) {
        let mut pending = self.pending_acks.lock().unwrap();
        for (key, ack) in keys.into_iter().zip(acks) {
            pending.entry(key).or_default().push(ack);
        }
    }

    fn take_acks(&self, keys: &[(i64, String)]) -> Vec<Arc<dyn Ack>> {
        let mut pending = self.pending_acks.lock().unwrap();
        keys.iter()
            .flat_map(|key| pending.remove(key).unwrap_or_default())
            .collect()
    }

    /// Serialize the working buffers into the state backend (used by barrier
    /// snapshots and tests).
    pub fn persist_buffers(&self) -> Result<(), Error> {
        let current = self
            .buffers
            .lock()
            .unwrap()
            .iter()
            .map(|((window_start, key), buffer)| ((*window_start, key.clone()), buffer.clone()))
            .collect::<BTreeMap<_, _>>();
        let existing = self.backend.scan(&self.namespace)?;
        for entry in existing {
            let window_start = if entry.key.len() >= 8 {
                i64::from_be_bytes(entry.key[..8].try_into().unwrap())
            } else {
                return Err(Error::Process("corrupt window state key".into()));
            };
            let key = String::from_utf8(entry.key[8..].to_vec())
                .map_err(|_| Error::Process("window state key is not utf8".into()))?;
            if !current.contains_key(&(window_start, key)) {
                self.backend.delete(&self.namespace, &entry.key)?;
            }
        }
        for ((window_start, key), buffer) in current {
            let value = encode_buffer(&buffer)?;
            self.backend.put_with_ttl(
                &self.namespace,
                &Self::state_key(window_start, &key),
                &value,
                None,
                crate::state::now_ms(),
            )?;
        }
        Ok(())
    }

    /// Restore working buffers from the state backend.
    pub fn restore_buffers(&self) -> Result<usize, Error> {
        let restored = self.restore_buffers_inner()?;
        *self.loaded.lock().unwrap() = true;
        Ok(restored)
    }

    fn restore_buffers_inner(&self) -> Result<usize, Error> {
        let entries = self.backend.scan(&self.namespace)?;
        let mut buffers = self.buffers.lock().unwrap();
        buffers.clear();
        let mut restored = 0;
        for entry in entries {
            let buffer = decode_buffer(&entry.value)?;
            if entry.key.len() < 8 {
                return Err(Error::Process("corrupt window state key".into()));
            }
            let window_start = i64::from_be_bytes(entry.key[..8].try_into().unwrap());
            let key = String::from_utf8_lossy(&entry.key[8..]).into_owned();
            buffers.insert((window_start, key), buffer);
            restored += 1;
        }
        Ok(restored)
    }
}

/// Encode one aggregate buffer as a one-row Arrow IPC stream. Keeping the
/// state payload columnar makes snapshots backend-neutral and avoids coupling
/// the window state format to JSON field ordering or number representations.
fn encode_buffer(buffer: &AggregateBuffer) -> Result<Vec<u8>, Error> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("count", DataType::UInt64, false),
        Field::new("sum_i64", DataType::Int64, false),
        Field::new("sum_float", DataType::Float64, false),
        Field::new("min_i64", DataType::Int64, false),
        Field::new("max_i64", DataType::Int64, false),
        Field::new("is_float", DataType::Boolean, false),
        Field::new("session_end_ms", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![buffer.count])) as ArrayRef,
            Arc::new(Int64Array::from(vec![buffer.sum_i64])) as ArrayRef,
            Arc::new(Float64Array::from(vec![buffer.sum_float])) as ArrayRef,
            Arc::new(Int64Array::from(vec![buffer.min_i64])) as ArrayRef,
            Arc::new(Int64Array::from(vec![buffer.max_i64])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![buffer.is_float])) as ArrayRef,
            Arc::new(Int64Array::from(vec![buffer.session_end_ms])) as ArrayRef,
        ],
    )
    .map_err(|error| Error::Process(format!("build window state batch: {error}")))?;
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, schema.as_ref())
        .map_err(|error| Error::Process(format!("start window state IPC writer: {error}")))?;
    writer
        .write(&batch)
        .map_err(|error| Error::Process(format!("write window state IPC: {error}")))?;
    writer
        .finish()
        .map_err(|error| Error::Process(format!("finish window state IPC: {error}")))?;
    Ok(bytes)
}

fn decode_buffer(bytes: &[u8]) -> Result<AggregateBuffer, Error> {
    let mut reader = match StreamReader::try_new(Cursor::new(bytes), None) {
        Ok(reader) => reader,
        Err(_) => {
            return serde_json::from_slice(bytes)
                .map_err(|error| Error::Process(format!("invalid window state payload: {error}")));
        }
    };
    let batch = reader
        .next()
        .ok_or_else(|| Error::Process("window state IPC has no record batch".into()))?
        .map_err(|error| Error::Process(format!("read window state IPC: {error}")))?;
    if batch.num_rows() != 1 || !matches!(batch.num_columns(), 6 | 7) {
        // A short JSON fallback keeps state written by the pre-IPC kernel
        // recoverable during rolling upgrades.
        return serde_json::from_slice(bytes)
            .map_err(|error| Error::Process(format!("invalid window state payload: {error}")));
    }
    let value = |index: usize| batch.column(index).clone();
    let count = value(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| Error::Process("window state count column has wrong type".into()))?
        .value(0);
    let sum_i64 = value(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| Error::Process("window state sum_i64 column has wrong type".into()))?
        .value(0);
    let sum_float = value(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| Error::Process("window state sum_float column has wrong type".into()))?
        .value(0);
    let min_i64 = value(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| Error::Process("window state min_i64 column has wrong type".into()))?
        .value(0);
    let max_i64 = value(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| Error::Process("window state max_i64 column has wrong type".into()))?
        .value(0);
    let is_float = value(5)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| Error::Process("window state is_float column has wrong type".into()))?
        .value(0);
    let session_end_ms = if batch.num_columns() == 7 {
        value(6)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| Error::Process("window state session_end_ms column has wrong type".into()))?
            .value(0)
    } else {
        0
    };
    Ok(AggregateBuffer {
        count,
        sum_i64,
        sum_float,
        min_i64,
        max_i64,
        is_float,
        session_end_ms,
    })
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
    async fn processing_time_trigger_fires_on_idle_tick() {
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
        let op = operator(WindowTrigger::ProcessingTime, backend);
        // Processing-time mode starts a cadence when data arrives; an idle
        // timer flushes the buffer regardless of event timestamps.
        let now = crate::state::now_ms() as i64;
        let far_past = now - 60_000;
        let ts = far_past - (far_past % 10_000);
        let held = op
            .process(batch(vec![(ts, "a", 1), (ts + 1, "a", 2)], None))
            .await
            .unwrap();
        assert!(matches!(held, ProcessResult::None));
        *op.last_processing_trigger_ms.lock().unwrap() = Some(now - 2_000);
        let fired = op.on_tick().await.unwrap();
        assert!(matches!(
            fired,
            ProcessResult::Single(_) | ProcessResult::SingleWithAck(_, _)
        ));
    }

    #[tokio::test]
    async fn processing_time_window_accepts_batches_without_metadata_timestamp() {
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
        let op = operator(WindowTrigger::ProcessingTime, backend);
        let batch = Arc::new(crate::MessageBatch::new_arrow(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int64, false),
                ])),
                vec![
                    Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
                    Arc::new(I64::from(vec![7])) as ArrayRef,
                ],
            )
            .unwrap(),
        ));

        assert!(matches!(op.process(batch).await.unwrap(), ProcessResult::None));
        *op.last_processing_trigger_ms.lock().unwrap() =
            Some(crate::state::now_ms() as i64 - 2_000);
        let fired = op.on_tick().await.unwrap();
        let (ProcessResult::Single(fired) | ProcessResult::SingleWithAck(fired, _)) = fired else {
            panic!("processing-time window should flush a metadata-free batch");
        };
        assert_eq!(fired.record_batch().num_rows(), 1);
        assert_eq!(
            fired
                .record_batch()
                .column_by_name("sum")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            7
        );
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
        // Each event is within 1000ms of the previous one, so the dynamic
        // session end keeps extending instead of using the original start.
        op.process(
            batch(
                vec![(1_000, "a", 0), (1_800, "a", 0), (2_700, "a", 0)],
                None,
            ),
        )
        .await
        .unwrap();
        // A far-future watermark fires the merged session.
        let fired = op
            .process(batch(vec![(5_000, "b", 0)], Some(3_700)))
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
        let ends = fired
            .record_batch()
            .column_by_name("window_end")
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
        assert_eq!(ends.values(), &[3_700]);
        assert_eq!(counts.values(), &[3]);
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
