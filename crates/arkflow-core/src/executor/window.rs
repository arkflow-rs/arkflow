//! Columnar window operator: vectorized tumbling window assignment with
//! keyed aggregation state, watermark and processing-time triggers.
//!
//! Assignment is O(1) full-column computations over the timestamp column
//! (`window_start = ts.div_euclid(size) * size`); rows are grouped per batch
//! and merged into per-(window, key) aggregate buffers held in the state
//! backend. Watermarks (or the processing-time trigger) fire windows whose
//! end has passed, emitting the aggregate batch downstream.

use crate::input::{fanout_ack, Ack, VecAck};
use crate::processor::Processor;
use crate::state::StateBackend;
use crate::Error;
use crate::MessageBatchRef;
use crate::ProcessResult;
use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, UInt64Array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::sync::{Arc, Mutex};

/// Numeric representation of a window aggregate. The kind is fixed by the
/// value column's Arrow type; sums/min/max keep that type through state
/// serialization and the emitted schema instead of collapsing into integer
/// sentinels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NumericKind {
    Int64,
    Float32,
    Float64,
}

impl Default for NumericKind {
    fn default() -> Self {
        Self::Int64
    }
}

impl NumericKind {
    /// The emitted `sum`/`min`/`max` column type for this aggregate kind.
    pub fn output_type(self) -> DataType {
        match self {
            Self::Int64 => DataType::Int64,
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
        }
    }
}

/// Serialized aggregate for one (window, key) pair. Kept as a compact JSON
/// envelope so state stays backend-agnostic; the numeric kind travels with
/// the payload so a restored aggregate emits its original type.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AggregateBuffer {
    pub count: u64,
    #[serde(default)]
    pub kind: NumericKind,
    pub sum_i64: i64,
    pub sum_float: f64,
    pub min_i64: i64,
    pub max_i64: i64,
    #[serde(default)]
    pub min_float: f64,
    #[serde(default)]
    pub max_float: f64,
    /// The window already fired and its result is downstream; the buffer is
    /// retained until the allowed-lateness deadline so a late Update can
    /// correct the same `(operator, key, window)` aggregate.
    #[serde(default)]
    pub emitted: bool,
    /// The emitted aggregate changed since the last fire (late Update): the
    /// next fire re-emits the complete corrected result with an update
    /// marker instead of opening an unrelated partial window.
    #[serde(default)]
    pub updated_since_emit: bool,
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
            self.min_float = other.min_float;
            self.max_float = other.max_float;
        } else if other.count > 0 {
            self.min_i64 = self.min_i64.min(other.min_i64);
            self.max_i64 = self.max_i64.max(other.max_i64);
            self.min_float = self.min_float.min(other.min_float);
            self.max_float = self.max_float.max(other.max_float);
        }
        self.kind = match (self.kind, other.kind) {
            // A merged buffer keeps the wider of the two kinds.
            (NumericKind::Float64, _) | (_, NumericKind::Float64) => NumericKind::Float64,
            (NumericKind::Float32, _) | (_, NumericKind::Float32) => NumericKind::Float32,
            (NumericKind::Int64, NumericKind::Int64) => NumericKind::Int64,
        };
        self.session_end_ms = self.session_end_ms.max(other.session_end_ms);
        if other.count > 0 {
            self.updated_since_emit = self.emitted;
        }
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
        self.updated_since_emit = self.emitted;
    }

    /// Observe a floating value of the given kind. The internal
    /// representation is f64; the emitted schema narrows back to Float32 for
    /// Float32 aggregates.
    pub fn observe_float(&mut self, value: f64, kind: NumericKind) {
        if self.count == 0 {
            self.min_float = value;
            self.max_float = value;
        } else {
            self.min_float = self.min_float.min(value);
            self.max_float = self.max_float.max(value);
        }
        self.sum_float += value;
        self.kind = match (self.kind, kind) {
            (NumericKind::Float64, _) | (_, NumericKind::Float64) => NumericKind::Float64,
            (NumericKind::Float32, _) | (_, NumericKind::Float32) => NumericKind::Float32,
            (NumericKind::Int64, NumericKind::Int64) => NumericKind::Int64,
        };
        self.count += 1;
        self.updated_since_emit = self.emitted;
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
    Tumbling {
        size_ms: i64,
    },
    /// Size-sized windows advancing every `slide_ms`: one event belongs to
    /// every window whose interval contains its timestamp.
    Sliding {
        size_ms: i64,
        slide_ms: i64,
    },
    /// Gap-extended windows: a new window opens when no event arrives within
    /// `gap_ms` of the previous one; the window fires after the gap passes.
    Session {
        gap_ms: i64,
    },
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
    /// Allowed lateness for event-time windows: a fired window's aggregate is
    /// retained until `window_end + allowed_lateness`, and a late Update
    /// within that deadline corrects the SAME `(operator, key, window)`
    /// aggregate and re-emits the complete result with an update marker.
    #[serde(default)]
    pub allowed_lateness_ms: u64,
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
                return Err(Error::Config(
                    "session window gap_ms must be positive".into(),
                ));
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
    /// Output-gated state commits: buffer mutations stage in the journal and
    /// apply only when the fired window's output acknowledgement succeeds.
    /// `None` keeps the legacy direct-persist behavior.
    journal: Option<Arc<super::state_journal::StateJournal>>,
    /// One journal transaction per open `(window_start, key)` group; a fired
    /// window's commit rides its emitted output's acknowledgement.
    window_txns: Mutex<BTreeMap<(i64, String), super::state_journal::StateTxn>>,
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
            journal: None,
            window_txns: Mutex::new(BTreeMap::new()),
            buffers: Mutex::new(BTreeMap::new()),
            pending_acks: Mutex::new(BTreeMap::new()),
            watermark_ms: Mutex::new(None),
            last_processing_trigger_ms: Mutex::new(None),
            loaded: Mutex::new(false),
        }
    }

    /// Build the operator with output-gated state commits.
    pub fn with_journal(
        config: WindowOperatorConfig,
        backend: Arc<dyn StateBackend>,
        journal: Arc<super::state_journal::StateJournal>,
        namespace: impl Into<String>,
    ) -> Self {
        Self {
            config,
            backend,
            namespace: namespace.into(),
            journal: Some(journal),
            window_txns: Mutex::new(BTreeMap::new()),
            buffers: Mutex::new(BTreeMap::new()),
            pending_acks: Mutex::new(BTreeMap::new()),
            watermark_ms: Mutex::new(None),
            last_processing_trigger_ms: Mutex::new(None),
            loaded: Mutex::new(false),
        }
    }

    /// The journal transaction owning one window group's staged state.
    fn window_txn(&self, key: &(i64, String)) -> Result<super::state_journal::StateTxn, Error> {
        let journal = self
            .journal
            .as_ref()
            .expect("window_txn requires a journal");
        let mut txns = self.window_txns.lock().unwrap();
        if let Some(txn) = txns.get(key) {
            return Ok(*txn);
        }
        let txn = journal.begin()?;
        txns.insert(key.clone(), txn);
        Ok(txn)
    }

    /// Discard a window group's staged transaction (its buffer merged into
    /// another session window or its output failed before firing).
    fn rollback_window_txn(&self, key: &(i64, String)) {
        if let Some(journal) = &self.journal {
            if let Some(txn) = self.window_txns.lock().unwrap().remove(key) {
                journal.rollback(txn);
            }
        }
    }

    /// All windows containing one event time. Tumbling yields one;
    /// sliding yields `size / slide` overlapping windows; session yields
    /// its gap-extended window (tracked per key in the buffer map).
    fn windows_for(&self, event_time_ms: i64) -> Vec<(i64, i64)> {
        match self.config.kind {
            WindowKind::Tumbling { size_ms } => {
                let start = event_time_ms.div_euclid(size_ms).saturating_mul(size_ms);
                vec![(start, start.saturating_add(size_ms))]
            }
            WindowKind::Sliding { size_ms, slide_ms } => {
                // Enumerate EVERY aligned window start whose interval contains
                // the event, starting from the latest candidate
                // (`floor(ts/slide)*slide`) and stepping back until the
                // window no longer contains the timestamp. This does not
                // rely on `size / slide` being an integer, so non-divisible
                // boundaries (e.g. size=5, slide=2) never lose a containing
                // window to integer-division truncation.
                let last_start = event_time_ms.div_euclid(slide_ms).saturating_mul(slide_ms);
                let mut windows = Vec::new();
                let mut start = last_start;
                loop {
                    let end = start.saturating_add(size_ms);
                    if end <= event_time_ms {
                        break;
                    }
                    windows.push((start, end));
                    let Some(previous) = start.checked_sub(slide_ms) else {
                        break;
                    };
                    start = previous;
                }
                windows
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
                Ok(values
                    .iter()
                    .map(|value| {
                        value
                            .map(|value| match unit {
                                datafusion::arrow::datatypes::TimeUnit::Second => {
                                    value.checked_mul(1_000).ok_or_else(|| {
                                        Error::Process(format!(
                                            "window timestamp field '{}' overflows milliseconds",
                                            self.config.timestamp_field
                                        ))
                                    })
                                }
                                datafusion::arrow::datatypes::TimeUnit::Millisecond => Ok(value),
                                datafusion::arrow::datatypes::TimeUnit::Microsecond => {
                                    Ok(value.div_euclid(1_000))
                                }
                                datafusion::arrow::datatypes::TimeUnit::Nanosecond => {
                                    Ok(value.div_euclid(1_000_000))
                                }
                            })
                            .transpose()
                    })
                    .collect::<Result<Vec<_>, _>>()?)
            }
            DataType::Date32 => {
                let casted = cast(column, &DataType::Int64)
                    .map_err(|error| Error::Process(format!("cast date: {error}")))?;
                Ok(casted
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .map(|value| {
                        value
                            .map(|value| {
                                value.checked_mul(86_400_000).ok_or_else(|| {
                                    Error::Process(format!(
                                        "window timestamp field '{}' overflows milliseconds",
                                        self.config.timestamp_field
                                    ))
                                })
                            })
                            .transpose()
                    })
                    .collect::<Result<Vec<_>, _>>()?)
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
        if let Some(column) = batch
            .record_batch()
            .column_by_name(&self.config.watermark_field)
        {
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
                            && *event_time <= Self::session_end(*start, buffer, gap_ms)
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
                        DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64 => {
                            let casted = cast(column, &DataType::Int64).map_err(|error| {
                                Error::Process(format!("cast window value: {error}"))
                            })?;
                            let values = casted.as_any().downcast_ref::<Int64Array>().unwrap();
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
                                entry.observe_float(values.value(row), NumericKind::Float64);
                            }
                        }
                        DataType::Float32 => {
                            let values = column
                                .as_any()
                                .downcast_ref::<datafusion::arrow::array::Float32Array>()
                                .unwrap();
                            if !values.is_null(row) {
                                entry.observe_float(
                                    f64::from(values.value(row)),
                                    NumericKind::Float32,
                                );
                            }
                        }
                        other => {
                            return Err(Error::Process(format!(
                                "window value field '{}' has unsupported numeric type {other:?}; \
                                 expected an integer, Float32, or Float64 column",
                                self.config
                                    .value_fields
                                    .first()
                                    .map(String::as_str)
                                    .unwrap_or("?")
                            )));
                        }
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
                // The merged-away session's staged state is obsolete: its
                // buffer lives on under the merged key.
                self.rollback_window_txn(&old_key);
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
        let lateness = self.config.allowed_lateness_ms as i64;
        let mut buffers = self.buffers.lock().unwrap();
        let end_of = |start: i64, buffer: &AggregateBuffer| match self.config.kind {
            WindowKind::Session { gap_ms } => Self::session_end(start, buffer, gap_ms),
            _ => start.saturating_add(window_size),
        };
        // Fired windows are RETAINED through their allowed-lateness deadline
        // so a late Update modifies the same `(operator, key, window)`
        // aggregate; only past-deadline buffers are cleaned up. A window
        // that never fired always fires first — cleanup never drops an
        // unemitted aggregate.
        let expired = buffers
            .iter()
            .filter(|((start, _), buffer)| {
                let end = end_of(*start, buffer);
                buffer.emitted && threshold.saturating_sub(end) > lateness
            })
            .map(|((start, key), _)| (*start, key.clone()))
            .collect::<Vec<_>>();
        for key in &expired {
            buffers.remove(key);
        }
        let ready = buffers
            .iter_mut()
            .filter(|((start, _), buffer)| {
                let end = end_of(*start, buffer);
                end <= threshold && (!buffer.emitted || buffer.updated_since_emit)
            })
            .map(|((start, key), buffer)| ((*start, key.clone()), buffer.clone()))
            .collect::<Vec<_>>();
        if ready.is_empty() {
            return Ok(None);
        }
        let mut starts = Vec::new();
        let mut ends = Vec::new();
        let mut key_strings: Vec<String> = Vec::new();
        let mut counts = Vec::new();
        let mut sum_values: Vec<NumericValue> = Vec::new();
        let mut min_values: Vec<NumericValue> = Vec::new();
        let mut max_values: Vec<NumericValue> = Vec::new();
        let mut updates = Vec::new();
        let mut fired_keys = Vec::new();
        let journal = self.journal.clone();
        for ((start, key), buffer) in ready {
            if let Some(buffer_state) = buffers.get_mut(&(start, key.clone())) {
                buffer_state.emitted = true;
                buffer_state.updated_since_emit = false;
            }
            if let Some(journal) = &journal {
                // Stage the close in the window's own transaction; the fired
                // output's acknowledgement commits it, so a restored or
                // eagerly-persisted entry disappears only once the window's
                // result is durably downstream.
                let txn = self.window_txn(&(start, key.clone()))?;
                journal.delete(txn, &self.namespace, &Self::state_key(start, &key))?;
            }
            let is_update = buffer.emitted;
            fired_keys.push((start, key.clone()));
            starts.push(start);
            ends.push(end_of(start, &buffer));
            key_strings.push(key);
            counts.push(buffer.count);
            sum_values.push(match buffer.kind {
                NumericKind::Int64 => NumericValue::Int(buffer.sum_i64),
                NumericKind::Float32 => NumericValue::Float(buffer.sum_float, NumericKind::Float32),
                NumericKind::Float64 => NumericValue::Float(buffer.sum_float, NumericKind::Float64),
            });
            min_values.push(match buffer.kind {
                NumericKind::Int64 => NumericValue::Int(buffer.min_i64),
                NumericKind::Float32 => NumericValue::Float(buffer.min_float, NumericKind::Float32),
                NumericKind::Float64 => NumericValue::Float(buffer.min_float, NumericKind::Float64),
            });
            max_values.push(match buffer.kind {
                NumericKind::Int64 => NumericValue::Int(buffer.max_i64),
                NumericKind::Float32 => NumericValue::Float(buffer.max_float, NumericKind::Float32),
                NumericKind::Float64 => NumericValue::Float(buffer.max_float, NumericKind::Float64),
            });
            updates.push(is_update);
        }
        drop(buffers);
        let kind = sum_values
            .first()
            .map(NumericValue::kind)
            .unwrap_or(NumericKind::Int64);
        let sum_column = numeric_array(&sum_values, kind)?;
        let min_column = numeric_array(&min_values, kind)?;
        let max_column = numeric_array(&max_values, kind)?;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("window_start", DataType::Int64, false),
                Field::new("window_end", DataType::Int64, false),
                Field::new("key", DataType::Utf8, false),
                Field::new("count", DataType::UInt64, false),
                Field::new("sum", kind.output_type(), false),
                Field::new("min", kind.output_type(), false),
                Field::new("max", kind.output_type(), false),
                // Update marker: true when this row is a complete corrected
                // result for an already emitted window (late Update).
                Field::new("__arkflow_window_update", DataType::Boolean, false),
            ])),
            vec![
                Arc::new(Int64Array::from(starts)),
                Arc::new(Int64Array::from(ends)),
                Arc::new(datafusion::arrow::array::StringArray::from(key_strings)),
                Arc::new(UInt64Array::from(counts)),
                sum_column,
                min_column,
                max_column,
                Arc::new(BooleanArray::from(updates)),
            ],
        )
        .map_err(|error| Error::Process(format!("build window aggregate batch: {error}")))?;
        Ok(Some((
            Arc::new(crate::MessageBatch::new_arrow(batch)),
            fired_keys,
        )))
    }
}

/// One typed aggregate output value.
#[derive(Debug, Clone, Copy)]
enum NumericValue {
    Int(i64),
    Float(f64, NumericKind),
}

impl NumericValue {
    fn kind(&self) -> NumericKind {
        match self {
            Self::Int(_) => NumericKind::Int64,
            Self::Float(_, kind) => *kind,
        }
    }
}

/// Build a typed Arrow column from values, widening integers to the batch's
/// float kind when a window aggregate carries both (schema stays uniform).
fn numeric_array(values: &[NumericValue], kind: NumericKind) -> Result<ArrayRef, Error> {
    match kind {
        NumericKind::Int64 => Ok(Arc::new(Int64Array::from(
            values
                .iter()
                .map(|value| match value {
                    NumericValue::Int(value) => *value,
                    NumericValue::Float(value, _) => *value as i64,
                })
                .collect::<Vec<_>>(),
        ))),
        NumericKind::Float32 => Ok(Arc::new(datafusion::arrow::array::Float32Array::from(
            values
                .iter()
                .map(|value| match value {
                    NumericValue::Int(value) => *value as f32,
                    NumericValue::Float(value, _) => *value as f32,
                })
                .collect::<Vec<_>>(),
        ))),
        NumericKind::Float64 => Ok(Arc::new(datafusion::arrow::array::Float64Array::from(
            values
                .iter()
                .map(|value| match value {
                    NumericValue::Int(value) => *value as f64,
                    NumericValue::Float(value, _) => *value,
                })
                .collect::<Vec<_>>(),
        ))),
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
                ProcessResult::SingleWithAck(emitted, self.fired_ack(&fired_keys, Vec::new()))
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
                ProcessResult::SingleWithAck(emitted, self.fired_ack(&fired_keys, Vec::new()))
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
                ProcessResult::SingleWithAck(emitted, self.fired_ack(&fired_keys, Vec::new()))
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
            // No acknowledgement flow gates the commit, so fired windows
            // commit immediately (legacy direct-persist semantics).
            if let Some((_, fired_keys)) = &fired {
                self.commit_fired_txns(fired_keys)?;
            }
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

        let extra = if touched.is_empty() {
            // A watermark-only batch still has to be committed, but only
            // after the output produced by that watermark has been written.
            vec![ack]
        } else {
            Vec::new()
        };
        Ok(ProcessResult::SingleWithAck(
            emitted,
            self.fired_ack(&fired_keys, extra),
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
            // These acknowledgements stay pending until the window fires;
            // barrier draining must not wait on them (their state mutations
            // remain staged in the journal until the fired output commits).
            ack.mark_held();
            pending.entry(key).or_default().push(ack);
        }
    }

    fn take_acks(&self, keys: &[(i64, String)]) -> Vec<Arc<dyn Ack>> {
        let mut pending = self.pending_acks.lock().unwrap();
        keys.iter()
            .flat_map(|key| pending.remove(key).unwrap_or_default())
            .collect()
    }

    /// Assemble the acknowledgement of one fired-window output: the merged
    /// source acknowledgements plus, in journaled mode, one commit handle per
    /// fired window group so its staged state applies only after the output
    /// write is confirmed downstream.
    fn fired_ack(&self, fired_keys: &[(i64, String)], extra: Vec<Arc<dyn Ack>>) -> Arc<dyn Ack> {
        let mut acks = self.take_acks(fired_keys);
        acks.extend(extra);
        if let Some(journal) = &self.journal {
            let fired_txns = {
                let mut txns = self.window_txns.lock().unwrap();
                fired_keys
                    .iter()
                    .filter_map(|key| txns.remove(key))
                    .collect::<Vec<_>>()
            };
            for txn in fired_txns {
                acks.push(Arc::new(super::state_journal::CommitOnAck::new(
                    journal.clone(),
                    txn,
                    Arc::new(crate::input::NoopAck),
                )) as Arc<dyn Ack>);
            }
        }
        Arc::new(VecAck(acks))
    }

    /// Commit fired windows immediately (no acknowledgement flow to gate on).
    fn commit_fired_txns(&self, fired_keys: &[(i64, String)]) -> Result<(), Error> {
        if let Some(journal) = &self.journal {
            let fired_txns = {
                let mut txns = self.window_txns.lock().unwrap();
                fired_keys
                    .iter()
                    .filter_map(|key| txns.remove(key))
                    .collect::<Vec<_>>()
            };
            for txn in fired_txns {
                journal.commit(txn)?;
            }
        }
        Ok(())
    }

    /// Serialize the working buffers into the state backend (used by barrier
    /// snapshots and tests). With a journal attached, buffer mutations stage
    /// in the window's transaction and apply to the backend only when the
    /// window's output acknowledgement commits them — a checkpoint therefore
    /// never observes a buffer whose input acknowledgements are still pending.
    pub fn persist_buffers(&self) -> Result<(), Error> {
        let current = self
            .buffers
            .lock()
            .unwrap()
            .iter()
            .map(|((window_start, key), buffer)| ((*window_start, key.clone()), buffer.clone()))
            .collect::<BTreeMap<_, _>>();
        if let Some(journal) = &self.journal {
            for ((window_start, key), buffer) in &current {
                let txn = self.window_txn(&(*window_start, key.clone()))?;
                journal.put(
                    txn,
                    &self.namespace,
                    &Self::state_key(*window_start, key),
                    encode_buffer(buffer)?,
                    None,
                )?;
            }
            // `fire_ready` removes buffers after their allowed-lateness
            // deadline.  In journaled mode the old implementation returned
            // here before deleting the corresponding committed backend
            // entries, so a restart could resurrect already-expired windows.
            // Cleanup is safe to commit immediately: an expired emitted
            // window can no longer receive a valid late Update.
            let existing = self.backend.scan(&self.namespace)?;
            let stale_keys = existing
                .into_iter()
                .map(|entry| Self::decode_state_key(&entry.key).map(|key| (key, entry.key)))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .filter_map(|(key, raw)| (!current.contains_key(&key)).then_some(raw))
                .collect::<Vec<_>>();
            if stale_keys.is_empty() {
                return Ok(());
            }
            let cleanup_txn = journal.begin()?;
            for raw in stale_keys {
                journal.delete(cleanup_txn, &self.namespace, &raw)?;
            }
            journal.commit(cleanup_txn)?;
            return Ok(());
        }
        let existing = self.backend.scan(&self.namespace)?;
        for entry in existing {
            let key = Self::decode_state_key(&entry.key)?;
            if !current.contains_key(&key) {
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

    fn decode_state_key(raw: &[u8]) -> Result<(i64, String), Error> {
        let window_start = raw
            .get(..8)
            .ok_or_else(|| Error::Process("corrupt window state key".into()))?
            .try_into()
            .map(i64::from_be_bytes)
            .map_err(|_| Error::Process("corrupt window state key".into()))?;
        let key = String::from_utf8(raw[8..].to_vec())
            .map_err(|_| Error::Process("window state key is not utf8".into()))?;
        Ok((window_start, key))
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
    // V2 payload: typed JSON envelope carrying the numeric kind, float
    // min/max, and the emitted/update-retention flags. Keeping the payload
    // JSON makes the state backend-neutral; the old IPC format remains
    // readable below.
    serde_json::to_vec(buffer)
        .map_err(|error| Error::Process(format!("encode window aggregate state: {error}")))
}

/// A window aggregate written by the pre-typed state format.
#[derive(serde::Deserialize)]
struct LegacyAggregateBuffer {
    count: u64,
    #[allow(dead_code)]
    #[serde(default)]
    sum_i64: i64,
    #[serde(default)]
    sum_float: f64,
    #[serde(default)]
    min_i64: i64,
    #[serde(default)]
    max_i64: i64,
    #[serde(default)]
    is_float: bool,
    #[serde(default)]
    session_end_ms: i64,
}

impl LegacyAggregateBuffer {
    /// Migrate a legacy payload. Integer aggregates migrate losslessly; a
    /// legacy FLOAT aggregate stored min/max as integer sentinels (i64::MIN
    /// / i64::MAX), which cannot be reconstructed — restoring it is a
    /// compatibility failure rather than silent corruption.
    fn migrate(self) -> Result<AggregateBuffer, Error> {
        if self.is_float && self.count > 0 {
            return Err(Error::Config(
                "legacy float window aggregate state cannot be migrated (min/max sentinels \
                 are unrecoverable); recreate the state or discard the checkpoint"
                    .into(),
            ));
        }
        Ok(AggregateBuffer {
            count: self.count,
            kind: NumericKind::Int64,
            sum_i64: self.sum_i64,
            min_i64: self.min_i64,
            max_i64: self.max_i64,
            session_end_ms: self.session_end_ms,
            ..Default::default()
        })
    }
}

fn decode_buffer(bytes: &[u8]) -> Result<AggregateBuffer, Error> {
    // Typed V2 payload (carries the `kind` field).
    if let Ok(buffer) = serde_json::from_slice::<AggregateBuffer>(bytes) {
        return Ok(buffer);
    }
    // Legacy JSON envelope (carries `is_float`).
    if let Ok(legacy) = serde_json::from_slice::<LegacyAggregateBuffer>(bytes) {
        return legacy.migrate();
    }
    // Pre-IPC kernel state: one-row Arrow stream with the legacy columns.
    if let Ok(mut reader) = StreamReader::try_new(Cursor::new(bytes), None) {
        if let Some(Ok(batch)) = reader.next() {
            if batch.num_rows() == 1 {
                let value = |index: usize| batch.column(index).clone();
                let count = value(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .and_then(|column| column.is_valid(0).then(|| column.value(0)));
                let sum_i64 = value(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .and_then(|column| column.is_valid(0).then(|| column.value(0)));
                let is_float = batch.num_columns() >= 6
                    && value(5)
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .and_then(|column| column.is_valid(0).then(|| column.value(0)))
                        .unwrap_or(false);
                if let (Some(count), Some(sum_i64)) = (count, sum_i64) {
                    let min_i64 = value(3)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .and_then(|column| column.is_valid(0).then(|| column.value(0)))
                        .unwrap_or_default();
                    let max_i64 = value(4)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .and_then(|column| column.is_valid(0).then(|| column.value(0)))
                        .unwrap_or_default();
                    let session_end_ms = if batch.num_columns() >= 7 {
                        value(6)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .and_then(|column| column.is_valid(0).then(|| column.value(0)))
                            .unwrap_or_default()
                    } else {
                        0
                    };
                    return LegacyAggregateBuffer {
                        count,
                        sum_i64,
                        sum_float: 0.0,
                        min_i64,
                        max_i64,
                        is_float,
                        session_end_ms,
                    }
                    .migrate();
                }
            }
        }
    }
    Err(Error::Process(
        "invalid window aggregate state payload".into(),
    ))
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
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1.to_string()).collect::<Vec<_>>(),
            )),
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
                allowed_lateness_ms: 0,
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
        let held = op
            .process(batch(vec![(3_000, "a", 3)], None))
            .await
            .unwrap();
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
        let held = op
            .process(batch(vec![(12_000, "a", 5)], Some(10_000)))
            .await
            .unwrap();
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

        assert!(matches!(
            op.process(batch).await.unwrap(),
            ProcessResult::None
        ));
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
        op.process(batch(vec![(1_000, "a", 4)], None))
            .await
            .unwrap();
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
        op.process(batch(
            vec![(10_000, "a", 1), (9_999, "a", 2), (-1, "a", 3)],
            None,
        ))
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
                kind: WindowKind::Sliding {
                    size_ms: 10_000,
                    slide_ms: 5_000,
                },
                timestamp_field: "ts".into(),
                key_field: "key".into(),
                value_fields: vec!["value".into()],
                trigger: WindowTrigger::Watermark,
                trigger_interval_ms: 1_000,
                watermark_field: "__watermark_ms".into(),
                allowed_lateness_ms: 0,
            },
            backend,
            "sliding-test",
        );
        // Event at 6_000 belongs to [0,10000) and [5000,15000).
        op.process(batch(vec![(6_000, "a", 2)], None))
            .await
            .unwrap();
        // Event at 7_000 also belongs to both; [0,10000) has both, [5000,15000) has both.
        op.process(batch(vec![(7_000, "a", 3)], None))
            .await
            .unwrap();
        let fired = op
            .process(batch(vec![(20_000, "z", 0)], Some(15_000)))
            .await
            .unwrap();
        let ProcessResult::Single(fired) = fired else {
            panic!("sliding should fire")
        };
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
                allowed_lateness_ms: 0,
            },
            backend,
            "session-test",
        );
        // Each event is within 1000ms of the previous one, so the dynamic
        // session end keeps extending instead of using the original start.
        op.process(batch(
            vec![(1_000, "a", 0), (1_800, "a", 0), (2_700, "a", 0)],
            None,
        ))
        .await
        .unwrap();
        // A far-future watermark fires the merged session.
        let fired = op
            .process(batch(vec![(5_000, "b", 0)], Some(3_700)))
            .await
            .unwrap();
        let ProcessResult::Single(fired) = fired else {
            panic!("session should fire")
        };
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
    async fn float64_values_aggregate_with_typed_output() {
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
        let config = WindowOperatorConfig {
            kind: WindowKind::Tumbling { size_ms: 10_000 },
            timestamp_field: "ts".into(),
            key_field: "key".into(),
            value_fields: vec!["f".into()],
            trigger: WindowTrigger::Watermark,
            trigger_interval_ms: 1_000,
            watermark_field: "__watermark_ms".into(),
            allowed_lateness_ms: 0,
        };
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
                Arc::new(Float64Array::from(vec![1.2, 1.3])),
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
        let sums = fired
            .record_batch()
            .column_by_name("sum")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64 windows emit a Float64 sum column");
        assert_eq!(sums.len(), 1);
        assert!(
            (sums.value(0) - 2.5).abs() < 1e-9,
            "1.2 + 1.3 sums as floats"
        );
        let mins = fired
            .record_batch()
            .column_by_name("min")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let maxs = fired
            .record_batch()
            .column_by_name("max")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((mins.value(0) - 1.2).abs() < 1e-9);
        assert!((maxs.value(0) - 1.3).abs() < 1e-9);
        assert_eq!(
            fired
                .record_batch()
                .column_by_name("count")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            2
        );
    }

    #[tokio::test]
    async fn float32_values_sum_as_numbers_with_float32_schema() {
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
        let config = WindowOperatorConfig {
            kind: WindowKind::Tumbling { size_ms: 10_000 },
            timestamp_field: "ts".into(),
            key_field: "key".into(),
            value_fields: vec!["f".into()],
            trigger: WindowTrigger::Watermark,
            trigger_interval_ms: 1_000,
            watermark_field: "__watermark_ms".into(),
            allowed_lateness_ms: 0,
        };
        let op = ColumnarWindowOperator::new(config, backend, "float32-test");
        let record = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("ts", DataType::Int64, false),
                Field::new("key", DataType::Utf8, false),
                Field::new("f", DataType::Float32, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1_000])),
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(datafusion::arrow::array::Float32Array::from(vec![1.5f32])),
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
            panic!("float32 window should fire");
        };
        let sums = fired
            .record_batch()
            .column_by_name("sum")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float32Array>()
            .expect("Float32 windows emit a Float32-compatible schema");
        // Summed as numeric values (1.5), never treated as a count (1).
        assert!((sums.value(0) - 1.5).abs() < 1e-6);
    }

    #[tokio::test]
    async fn unsupported_value_types_are_rejected_explicitly() {
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
        let config = WindowOperatorConfig {
            kind: WindowKind::Tumbling { size_ms: 10_000 },
            timestamp_field: "ts".into(),
            key_field: "key".into(),
            value_fields: vec!["v".into()],
            trigger: WindowTrigger::Watermark,
            trigger_interval_ms: 1_000,
            watermark_field: "__watermark_ms".into(),
            allowed_lateness_ms: 0,
        };
        let op = ColumnarWindowOperator::new(config, backend, "unsupported-test");
        let record = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("ts", DataType::Int64, false),
                Field::new("key", DataType::Utf8, false),
                Field::new("v", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1_000])),
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(StringArray::from(vec!["not-a-number"])),
            ],
        )
        .unwrap();
        let result = op
            .process(Arc::new(crate::MessageBatch::new_arrow(record)))
            .await;
        let error = result.expect_err("unsupported value type must fail");
        assert!(
            error.to_string().contains("unsupported numeric type"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn typed_state_survives_serialization_roundtrip() {
        let mut buffer = AggregateBuffer::default();
        buffer.observe_float(1.2, NumericKind::Float64);
        buffer.observe_float(1.3, NumericKind::Float64);
        buffer.emitted = true;
        let encoded = encode_buffer(&buffer).unwrap();
        let decoded = decode_buffer(&encoded).unwrap();
        assert_eq!(decoded.kind, NumericKind::Float64);
        assert_eq!(decoded.count, 2);
        assert!((decoded.sum_float - 2.5).abs() < 1e-9);
        assert!(decoded.emitted);
        assert!((decoded.min_float - 1.2).abs() < 1e-9);
        assert!((decoded.max_float - 1.3).abs() < 1e-9);
    }

    #[test]
    fn legacy_integer_state_migrates_and_legacy_float_state_is_rejected() {
        // A legacy integer payload migrates losslessly.
        let legacy = br#"{"count":3,"sum_i64":6,"min_i64":1,"max_i64":3,"is_float":false,"session_end_ms":0}"#;
        let migrated = decode_buffer(legacy).unwrap();
        assert_eq!(migrated.kind, NumericKind::Int64);
        assert_eq!(migrated.count, 3);
        assert_eq!(migrated.sum_i64, 6);
        assert_eq!(migrated.min_i64, 1);
        assert_eq!(migrated.max_i64, 3);
        // A legacy float payload stored min/max as integer sentinels; its
        // restore is a compatibility failure instead of silent corruption.
        let legacy_float = br#"{"count":2,"sum_float":2.5,"min_i64":-9223372036854775808,"max_i64":9223372036854775807,"is_float":true}"#;
        assert!(decode_buffer(legacy_float).is_err());
    }

    /// Task 4.5: a late Update within the allowed-lateness deadline corrects
    /// the SAME `(operator, key, window)` aggregate and re-emits a complete
    /// result with an update marker; past the deadline the row is dropped
    /// from aggregation (the gate's late policy routes or drops it).
    #[tokio::test]
    async fn late_update_corrects_an_emitted_window() {
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
        let config = WindowOperatorConfig {
            kind: WindowKind::Tumbling { size_ms: 1_000 },
            timestamp_field: "ts".into(),
            key_field: "key".into(),
            value_fields: vec!["value".into()],
            trigger: WindowTrigger::Watermark,
            trigger_interval_ms: 1_000,
            watermark_field: "__watermark_ms".into(),
            allowed_lateness_ms: 5_000,
        };
        let op = ColumnarWindowOperator::new(config, backend, "late-update-test");
        // Initial window [0,1000) with one row of value 10.
        op.process(batch(vec![(100, "a", 10)], None)).await.unwrap();
        // Watermark 1000 fires [0,1000).
        let fired = op
            .process(batch(vec![(2_000, "b", 0)], Some(1_000)))
            .await
            .unwrap();
        let ProcessResult::Single(first) = fired else {
            panic!("window should fire");
        };
        let updates = first
            .record_batch()
            .column_by_name("__arkflow_window_update")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(!updates.value(0), "the initial result is not an update");
        let sums = first
            .record_batch()
            .column_by_name("sum")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(sums.value(0), 10);

        // A late update (marked by the gate) lands in the retained window.
        let late = {
            let mut fields = vec![
                Field::new("ts", DataType::Int64, false),
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, false),
            ];
            let mut columns: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(vec![200])),
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(Int64Array::from(vec![5])),
            ];
            fields.push(Field::new(
                "__arkflow_late_event_update",
                DataType::Boolean,
                false,
            ));
            columns.push(Arc::new(BooleanArray::from(vec![true])));
            Arc::new(crate::MessageBatch::new_arrow(
                RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap(),
            ))
        };
        let corrected = op.process(late).await.unwrap();
        let ProcessResult::Single(corrected) = corrected else {
            panic!("the corrected window should re-emit");
        };
        let updates = corrected
            .record_batch()
            .column_by_name("__arkflow_window_update")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(updates.value(0), "the corrected result carries the marker");
        let sums = corrected
            .record_batch()
            .column_by_name("sum")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let starts = corrected
            .record_batch()
            .column_by_name("window_start")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(starts.value(0), 0, "the correction targets the same window");
        assert_eq!(sums.value(0), 15, "10 + the late 5");
    }

    /// Task 4.5: past the allowed-lateness deadline the retained buffer is
    /// cleaned up instead of staying resident forever.
    #[tokio::test]
    async fn emitted_windows_are_retained_until_the_deadline_then_cleaned() {
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
        let config = WindowOperatorConfig {
            kind: WindowKind::Tumbling { size_ms: 1_000 },
            timestamp_field: "ts".into(),
            key_field: "key".into(),
            value_fields: vec!["value".into()],
            trigger: WindowTrigger::Watermark,
            trigger_interval_ms: 1_000,
            watermark_field: "__watermark_ms".into(),
            allowed_lateness_ms: 5_000,
        };
        let op = ColumnarWindowOperator::new(config, backend, "deadline-test");
        op.process(batch(vec![(100, "a", 1)], None)).await.unwrap();
        op.process(batch(vec![(2_000, "b", 0)], Some(1_000)))
            .await
            .unwrap();
        // Within the deadline the buffer is retained.
        {
            let buffers = op.buffers.lock().unwrap();
            assert!(buffers.contains_key(&(0, "a".to_string())));
        }
        // Far past the deadline (watermark 7000 > 1000 + 5000): cleanup.
        op.process(batch(vec![(8_000, "b", 0)], Some(7_000)))
            .await
            .unwrap();
        let buffers = op.buffers.lock().unwrap();
        assert!(
            !buffers.contains_key(&(0, "a".to_string())),
            "past-deadline buffers are cleaned up"
        );
    }
}

#[cfg(test)]
mod sliding_enumeration_tests {
    use super::*;

    fn sliding_operator(
        size_ms: i64,
        slide_ms: i64,
        backend: Arc<dyn StateBackend>,
    ) -> ColumnarWindowOperator {
        ColumnarWindowOperator::new(
            WindowOperatorConfig {
                kind: WindowKind::Sliding { size_ms, slide_ms },
                timestamp_field: "ts".into(),
                key_field: "key".into(),
                value_fields: vec![],
                trigger: WindowTrigger::Watermark,
                trigger_interval_ms: 1_000,
                watermark_field: "__watermark_ms".into(),
                allowed_lateness_ms: 0,
            },
            backend,
            "sliding-enum-test",
        )
    }

    /// Task 4.4: a non-divisible sliding window (`size=5, slide=2`) assigns
    /// timestamp 4 to windows starting at 4, 2, and 0 — no containing start
    /// is omitted because of integer-division truncation.
    #[test]
    fn non_divisible_sliding_window_enumerates_every_containing_start() {
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
        let op = sliding_operator(5, 2, backend);
        // The enumeration starts at the latest containing start and steps
        // back; ordering within the assignment is irrelevant to the caller.
        let mut windows = op.windows_for(4);
        windows.sort();
        assert_eq!(
            windows,
            vec![(0, 5), (2, 7), (4, 9)],
            "timestamp 4 belongs to windows [0,5), [2,7), and [4,9)"
        );
        // Boundary cases: the start itself, the first row past a boundary,
        // and the last row of the containing chain.
        let mut windows = op.windows_for(0);
        windows.sort();
        assert_eq!(windows, vec![(-4, 1), (-2, 3), (0, 5)]);
        let mut windows = op.windows_for(5);
        windows.sort();
        // 5 is the exclusive end of [0,5): it belongs to the next chain only.
        assert_eq!(windows, vec![(2, 7), (4, 9)]);
        let mut windows = op.windows_for(9);
        windows.sort();
        // 9 is the exclusive end of [4,9).
        assert_eq!(windows, vec![(6, 11), (8, 13)]);
    }

    #[test]
    fn negative_timestamps_enumerate_aligned_containing_windows() {
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
        let op = sliding_operator(5, 2, backend);
        // -1: last start = floor(-1/2)*2 = -2. [-2,3) contains -1; [-4,1)
        // contains -1; [-6,-1) does not (end == -1 is exclusive).
        let mut windows = op.windows_for(-1);
        windows.sort();
        assert_eq!(windows, vec![(-4, 1), (-2, 3)]);
    }

    /// Divisible boundaries keep the classic behavior: size=10, slide=5,
    /// ts=6 belongs to [0,10) and [5,15).
    #[test]
    fn divisible_sliding_windows_keep_two_memberships() {
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
        let op = sliding_operator(10, 5, backend);
        let mut windows = op.windows_for(6);
        windows.sort();
        assert_eq!(windows, vec![(0, 10), (5, 15)]);
    }
}
