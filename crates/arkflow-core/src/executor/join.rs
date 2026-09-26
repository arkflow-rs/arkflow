//! Keyed interval join operator for the unified execution kernel.
//!
//! The join operator sits in a chain with exactly two inbound edges (input 0
//! is the left side, input 1 the right side). The chain loop tags every batch
//! with a `__meta_input_index` column when a run contains a join operator, so
//! the processor can tell the sides apart. Rows are matched by an equality
//! key: a left row and a right row with the same key join when their event
//! timestamps differ by at most `window_ms`. Matched pairs are emitted as
//! they arrive (inner join, at-least-once). Per-side keyed buffers are
//! bounded: rows are evicted once the watermark advances past
//! `timestamp + window_ms + ttl_ms`, or when a side exceeds `max_per_key`
//! rows for one key (oldest first).
//!
//! State reconstructs from checkpoint replay: on recovery the sources rewind
//! to the acknowledged cut and the buffers rebuild deterministically, so the
//! operator carries no separate snapshot.
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray, UInt32Array};
use datafusion::arrow::array::Array as _;
use datafusion::arrow::compute::interleave;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::processor::Processor;
use crate::{Error, MessageBatch, MessageBatchRef, ProcessResult};

/// The input-side tag column written by the chain loop for join chains.
pub const META_INPUT_INDEX: &str = "__meta_input_index";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinOperatorConfig {
    /// Upstream operator id feeding the left side. Required for graphs with
    /// more than one inbound edge: channel ordering is a kernel-internal
    /// detail, so sides are declared by producer identity, not position.
    #[serde(default)]
    pub left_from: Option<String>,
    /// Upstream operator id feeding the right side. Defaults to the single
    /// remaining inbound producer when omitted.
    #[serde(default)]
    pub right_from: Option<String>,
    /// Key column on the left side. Must equal `right_key` in value space;
    /// both are named separately because the sides may label the join key
    /// differently.
    pub left_key: String,
    /// Key column on the right side (input 1).
    pub right_key: String,
    /// Event-time column on the left side. Falls back to `__meta_timestamp`
    /// when absent.
    #[serde(default)]
    pub left_timestamp: Option<String>,
    /// Event-time column on the right side. Falls back to `__meta_timestamp`.
    #[serde(default)]
    pub right_timestamp: Option<String>,
    /// Match bound: rows join when `|left_ts - right_ts| <= window_ms`.
    pub window_ms: i64,
    /// Retention grace beyond the watermark boundary before eviction.
    #[serde(default = "default_ttl_ms")]
    pub ttl_ms: i64,
    /// Per-key buffer bound per side; oldest rows are evicted first.
    #[serde(default = "default_max_per_key")]
    pub max_per_key: usize,
}

fn default_ttl_ms() -> i64 {
    0
}

fn default_max_per_key() -> usize {
    10_000
}

impl JoinOperatorConfig {
    pub fn validate(&self) -> Result<(), Error> {
        if self.left_key.is_empty() || self.right_key.is_empty() {
            return Err(Error::Config(
                "join operator requires non-empty left_key and right_key".into(),
            ));
        }
        if self.window_ms < 0 {
            return Err(Error::Config(
                "join operator window_ms must be non-negative".into(),
            ));
        }
        if self.ttl_ms < 0 {
            return Err(Error::Config("join operator ttl_ms must be non-negative".into()));
        }
        if self.max_per_key == 0 {
            return Err(Error::Config(
                "join operator max_per_key must be at least 1".into(),
            ));
        }
        Ok(())
    }
}

/// One buffered row: its event time plus a reference into the source batch
/// (kept alive by the Arc) so matched output gathers original column values
/// without copying rows on insert.
#[derive(Clone)]
struct BufferedRow {
    timestamp_ms: i64,
    batch: MessageBatchRef,
    row: usize,
}

#[derive(Default)]
struct SideBuffer {
    by_key: HashMap<String, VecDeque<BufferedRow>>,
    /// Schema of the batches seen so far on this side; per-side stability is
    /// required so gathered output columns stay well-typed.
    schema: Option<SchemaRef>,
    total: usize,
}

impl SideBuffer {
    fn push(
        &mut self,
        key: String,
        timestamp_ms: i64,
        batch: MessageBatchRef,
        row: usize,
        max_per_key: usize,
    ) {
        let queue = self.by_key.entry(key).or_default();
        queue.push_back(BufferedRow {
            timestamp_ms,
            batch,
            row,
        });
        self.total += 1;
        while queue.len() > max_per_key {
            queue.pop_front();
            self.total -= 1;
        }
    }

    /// Remove rows whose match window has closed: once the watermark passes
    /// `timestamp + window_ms (+ ttl)`, no future row on the other side can
    /// still match.
    fn evict(&mut self, watermark_ms: i64, window_ms: i64, ttl_ms: i64) {
        let bound = watermark_ms.saturating_sub(window_ms).saturating_sub(ttl_ms);
        for queue in self.by_key.values_mut() {
            while queue.front().is_some_and(|row| row.timestamp_ms < bound) {
                queue.pop_front();
                self.total -= 1;
            }
        }
        self.by_key.retain(|_, queue| !queue.is_empty());
    }
}

/// An owned (this-side, key) pair accumulated during one process call.
struct MatchedRow {
    row: BufferedRow,
    key: String,
}

pub struct JoinOperator {
    config: JoinOperatorConfig,
    /// Resolved channel indices for the left and right sides, in the chain's
    /// receiver order (see `JoinOperator::new`).
    left_index: u32,
    right_index: u32,
    left: Mutex<SideBuffer>,
    right: Mutex<SideBuffer>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    Left,
    Right,
}

impl Side {
    fn label(self) -> &'static str {
        match self {
            Side::Left => "left",
            Side::Right => "right",
        }
    }
}

impl JoinOperator {
    pub fn new(config: JoinOperatorConfig) -> Result<Self, Error> {
        config.validate()?;
        Ok(Self {
            config,
            left_index: 0,
            right_index: 1,
            left: Mutex::new(SideBuffer::default()),
            right: Mutex::new(SideBuffer::default()),
        })
    }

    /// Resolve the left/right channel indices from the chain's inbound
    /// producer order. `left_from`/`right_from` name the upstream operator
    /// ids; with a single-producer default the indices fall back to 0/1.
    pub fn with_input_producers(
        mut self,
        producers: &[String],
    ) -> Result<Self, Error> {
        let resolve = |declared: &Option<String>, fallback: usize| -> Result<u32, Error> {
            match declared {
                Some(operator_id) => producers
                    .iter()
                    .position(|producer| producer == operator_id)
                    .map(|index| index as u32)
                    .ok_or_else(|| {
                        Error::Config(format!(
                            "join operator 'left_from/right_from' names upstream '{operator_id}' which does not feed this join"
                        ))
                    }),
                None => Ok(fallback as u32),
            }
        };
        self.left_index = resolve(&self.config.left_from, 0)?;
        self.right_index = resolve(&self.config.right_from, 1)?;
        Ok(self)
    }

    fn column_string(batch: &MessageBatch, column: &str, row: usize) -> Result<String, Error> {
        let array = batch
            .record_batch()
            .schema()
            .index_of(column)
            .map_err(|_| Error::Config(format!("join side is missing column '{column}'")))?;
        let values = batch
            .record_batch()
            .column(array)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                Error::Config(format!("join key column '{column}' must be a string column"))
            })?;
        if values.is_null(row) {
            return Err(Error::Config(format!(
                "join key column '{column}' is null at row {row}; keyed join requires a key"
            )));
        }
        Ok(values.value(row).to_owned())
    }

    fn column_input_index(batch: &MessageBatch, row: usize) -> Result<u32, Error> {
        let index = batch
            .record_batch()
            .schema()
            .index_of(META_INPUT_INDEX)
            .map_err(|_| {
                Error::Config(format!(
                    "join operator input is missing the '{META_INPUT_INDEX}' column; join chains require kernel input tagging"
                ))
            })?;
        let values = batch
            .record_batch()
            .column(index)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| {
                Error::Config(format!("column '{META_INPUT_INDEX}' must be a UInt32 column"))
            })?;
        if values.is_null(row) {
            return Err(Error::Config(format!(
                "column '{META_INPUT_INDEX}' is null at row {row}"
            )));
        }
        Ok(values.value(row))
    }

    fn column_timestamp(batch: &MessageBatch, column: &str, row: usize) -> Result<i64, Error> {
        let array = batch
            .record_batch()
            .schema()
            .index_of(column)
            .map_err(|_| Error::Config(format!("join side is missing column '{column}'")))?;
        let column_data = batch.record_batch().column(array);
        if let Some(values) = column_data.as_any().downcast_ref::<Int64Array>() {
            if values.is_null(row) {
                return Err(Error::Config(format!(
                    "join timestamp column '{column}' is null at row {row}"
                )));
            }
            return Ok(values.value(row));
        }
        if let Some(values) = column_data
            .as_any()
            .downcast_ref::<datafusion::arrow::array::TimestampMillisecondArray>()
        {
            if values.is_null(row) {
                return Err(Error::Config(format!(
                    "join timestamp column '{column}' is null at row {row}"
                )));
            }
            return Ok(values.value(row));
        }
        if let Some(values) = column_data
            .as_any()
            .downcast_ref::<datafusion::arrow::array::TimestampNanosecondArray>()
        {
            if values.is_null(row) {
                return Err(Error::Config(format!(
                    "join timestamp column '{column}' is null at row {row}"
                )));
            }
            // `__meta_timestamp` carries nanoseconds; normalize to milliseconds.
            return Ok(values.value(row) / 1_000_000);
        }
        Err(Error::Config(format!(
            "join timestamp column '{column}' must be an Int64 or Timestamp column"
        )))
    }

    fn remember_schema(side: &mut SideBuffer, batch: &MessageBatch, label: &str) -> Result<(), Error> {
        let schema = batch.record_batch().schema();
        if let Some(seen) = &side.schema {
            if !seen.fields().iter().eq(schema.fields().iter()) {
                return Err(Error::Config(format!(
                    "join {label} side schema changed mid-stream; keyed join requires a stable per-side schema"
                )));
            }
        } else {
            side.schema = Some(schema);
        }
        Ok(())
    }

    /// Reconstruct the matched rows of one side as a RecordBatch (original
    /// column values plus the join key). All buffered rows share the side's
    /// stable schema, so a single `interleave` per column gathers the output
    /// from the original batches.
    fn gather(rows: &[MatchedRow], schema: &Schema) -> Result<RecordBatch, Error> {
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len() + 1);
        for index in 0..schema.fields().len() {
            if schema.field(index).name() == META_INPUT_INDEX {
                continue;
            }
            let arrays: Vec<&dyn datafusion::arrow::array::Array> = rows
                .iter()
                .map(|matched| {
                    matched.row.batch.record_batch().column(index) as &dyn datafusion::arrow::array::Array
                })
                .collect();
            let indices: Vec<(usize, usize)> =
                rows.iter().map(|matched| (0usize, matched.row.row)).collect();
            let array = interleave(&arrays, &indices)
                .map_err(|error| Error::Process(format!("join gather failed: {error}")))?;
            columns.push(array);
        }
        columns.push(Arc::new(StringArray::from(
            rows.iter().map(|matched| matched.key.as_str()).collect::<Vec<_>>(),
        )));
        let mut fields: Vec<Field> = schema
            .fields()
            .iter()
            .filter(|field| field.name() != META_INPUT_INDEX)
            .map(|field| (**field).clone())
            .collect();
        fields.push(Field::new("join_key", DataType::Utf8, false));
        let output_schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(output_schema, columns)
            .map_err(|error| Error::Process(format!("join output assembly failed: {error}")))
    }

    fn process_side(&self, batch: &MessageBatchRef, side: Side) -> Result<ProcessResult, Error> {
        let timestamp_column = match side {
            Side::Left => self
                .config
                .left_timestamp
                .clone()
                .unwrap_or_else(|| crate::meta_columns::TIMESTAMP.into()),
            Side::Right => self
                .config
                .right_timestamp
                .clone()
                .unwrap_or_else(|| crate::meta_columns::TIMESTAMP.into()),
        };
        let key_column = match side {
            Side::Left => self.config.left_key.clone(),
            Side::Right => self.config.right_key.clone(),
        };
        let (this_label, other_label) = (side.label(), match side {
            Side::Left => "right",
            Side::Right => "left",
        });
        let mut this_side = match side {
            Side::Left => self.left.lock(),
            Side::Right => self.right.lock(),
        }
        .map_err(|_| Error::Process(format!("join {this_label} lock poisoned")))?;
        let other_side = match side {
            Side::Left => self.right.lock(),
            Side::Right => self.left.lock(),
        }
        .map_err(|_| Error::Process(format!("join {other_label} lock poisoned")))?;
        Self::remember_schema(&mut this_side, batch, this_label)?;

        // Extract keys and timestamps before buffering so extraction errors
        // do not leave partial state behind.
        let rows = batch.record_batch().num_rows();
        let mut extracted = Vec::with_capacity(rows);
        for row in 0..rows {
            let key = Self::column_string(batch, &key_column, row)?;
            let timestamp = Self::column_timestamp(batch, &timestamp_column, row)?;
            extracted.push((key, timestamp, row));
        }

        let mut matched_this: Vec<MatchedRow> = Vec::new();
        let mut matched_other: Vec<MatchedRow> = Vec::new();
        for (key, timestamp, row) in extracted {
            if let Some(candidates) = other_side.by_key.get(&key) {
                for candidate in candidates {
                    if (candidate.timestamp_ms - timestamp).abs() <= self.config.window_ms {
                        matched_this.push(MatchedRow {
                            row: BufferedRow {
                                timestamp_ms: timestamp,
                                batch: batch.clone(),
                                row,
                            },
                            key: key.clone(),
                        });
                        matched_other.push(MatchedRow {
                            row: candidate.clone(),
                            key: key.clone(),
                        });
                    }
                }
            }
            this_side.push(key, timestamp, batch.clone(), row, self.config.max_per_key);
        }
        if matched_this.is_empty() {
            return Ok(ProcessResult::None);
        }
        let this_schema = this_side
            .schema
            .clone()
            .expect("schema remembered before matching");
        let other_schema = other_side
            .schema
            .clone()
            .unwrap_or_else(|| this_schema.clone());
        let this_batch = Self::gather(&matched_this, &this_schema)?;
        let other_batch = Self::gather(&matched_other, &other_schema)?;
        let output = match side {
            Side::Left => combine_sides(&this_batch, &other_batch)?,
            Side::Right => combine_sides(&other_batch, &this_batch)?,
        };
        Ok(ProcessResult::Single(Arc::new(output)))
    }
}

/// Prefix left columns `l_*`, right columns `r_*`, and append the join key.
fn combine_sides(left: &RecordBatch, right: &RecordBatch) -> Result<MessageBatch, Error> {
    let mut fields: Vec<Field> = Vec::with_capacity(left.num_columns() + right.num_columns());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(left.num_columns() + right.num_columns());
    for (index, field) in left.schema().fields().iter().enumerate() {
        if field.name() == "join_key" || field.name() == META_INPUT_INDEX {
            continue;
        }
        let renamed = field.as_ref().clone().with_name(format!("l_{}", field.name()));
        fields.push(renamed);
        columns.push(left.column(index).clone());
    }
    for (index, field) in right.schema().fields().iter().enumerate() {
        if field.name() == "join_key" || field.name() == META_INPUT_INDEX {
            continue;
        }
        let renamed = field.as_ref().clone().with_name(format!("r_{}", field.name()));
        fields.push(renamed);
        columns.push(right.column(index).clone());
    }
    fields.push(Field::new("join_key", DataType::Utf8, false));
    columns.push(left.column(left.num_columns() - 1).clone());
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, columns)
        .map_err(|error| Error::Process(format!("join combine failed: {error}")))?;
    Ok(MessageBatch::new_arrow(batch))
}

#[async_trait::async_trait]
impl Processor for JoinOperator {
    async fn process(&self, batch: MessageBatchRef) -> Result<ProcessResult, Error> {
        let tag = Self::column_input_index(&batch, 0)?;
        let side = if tag == self.left_index {
            Side::Left
        } else if tag == self.right_index {
            Side::Right
        } else {
            return Err(Error::Config(format!(
                "join received a batch from input {tag} which is neither the declared left ({}) nor right ({}) producer",
                self.left_index, self.right_index
            )));
        };
        self.process_side(&batch, side)
    }

    async fn on_watermark(&self, watermark_ms: i64) -> Result<ProcessResult, Error> {
        let mut left = self
            .left
            .lock()
            .map_err(|_| Error::Process("join left lock poisoned".into()))?;
        let mut right = self
            .right
            .lock()
            .map_err(|_| Error::Process("join right lock poisoned".into()))?;
        left.evict(watermark_ms, self.config.window_ms, self.config.ttl_ms);
        right.evict(watermark_ms, self.config.window_ms, self.config.ttl_ms);
        Ok(ProcessResult::None)
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn side_batch(side: u32, keys: &[&str], timestamps: &[i64]) -> MessageBatchRef {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new(META_INPUT_INDEX, DataType::UInt32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys.iter().map(|k| Some(*k)).collect::<Vec<_>>())),
                Arc::new(Int64Array::from(timestamps.to_vec())),
                Arc::new(UInt32Array::from(vec![side; keys.len()])),
            ],
        )
        .unwrap();
        Arc::new(MessageBatch::new_arrow(batch))
    }

    fn config() -> JoinOperatorConfig {
        JoinOperatorConfig {
            left_from: None,
            right_from: None,
            left_key: "key".into(),
            right_key: "key".into(),
            left_timestamp: Some("ts".into()),
            right_timestamp: Some("ts".into()),
            window_ms: 5_000,
            ttl_ms: 0,
            max_per_key: 100,
        }
    }

    #[tokio::test]
    async fn joins_matching_keys_within_window() {
        let join = JoinOperator::new(config()).unwrap();
        // Left arrives first: nothing to emit yet.
        assert!(matches!(
            join.process(side_batch(0, &["a"], &[100])).await.unwrap(),
            ProcessResult::None
        ));
        // Right within the window matches immediately.
        let output = join.process(side_batch(1, &["a"], &[5_100])).await.unwrap();
        let ProcessResult::Single(batch) = output else {
            panic!("expected a joined batch");
        };
        assert_eq!(batch.record_batch().num_rows(), 1);
        let names: Vec<String> = batch
            .record_batch()
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert!(names.contains(&"l_key".to_string()));
        assert!(names.contains(&"r_key".to_string()));
        assert!(names.contains(&"join_key".to_string()));
    }

    #[tokio::test]
    async fn rejects_rows_outside_window() {
        let join = JoinOperator::new(config()).unwrap();
        join.process(side_batch(0, &["a"], &[100])).await.unwrap();
        assert!(matches!(
            join.process(side_batch(1, &["a"], &[20_000])).await.unwrap(),
            ProcessResult::None
        ));
    }

    #[tokio::test]
    async fn watermark_evicts_expired_rows() {
        let join = JoinOperator::new(config()).unwrap();
        join.process(side_batch(0, &["a"], &[100])).await.unwrap();
        join.on_watermark(6_000).await.unwrap();
        // The left row's window closed; a later right row no longer matches.
        assert!(matches!(
            join.process(side_batch(1, &["a"], &[6_000])).await.unwrap(),
            ProcessResult::None
        ));
    }

    #[tokio::test]
    async fn missing_input_tag_is_rejected() {
        let join = JoinOperator::new(config()).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false)]));
        let batch = Arc::new(MessageBatch::new_arrow(
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a"]))]).unwrap(),
        ));
        let error = join.process(batch).await.unwrap_err();
        assert!(error.to_string().contains(META_INPUT_INDEX), "{error}");
    }

    #[tokio::test]
    async fn per_key_bound_evicts_oldest() {
        let mut cfg = config();
        cfg.max_per_key = 1;
        let join = JoinOperator::new(cfg).unwrap();
        join.process(side_batch(0, &["a", "a"], &[100, 200])).await.unwrap();
        // The first row was evicted; only the second can match.
        let output = join.process(side_batch(1, &["a"], &[200])).await.unwrap();
        let ProcessResult::Single(batch) = output else {
            panic!("expected a joined batch");
        };
        assert_eq!(batch.record_batch().num_rows(), 1);
        let ts = batch
            .record_batch()
            .column(batch.record_batch().schema().index_of("l_ts").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts.value(0), 200);
    }

    #[tokio::test]
    async fn producer_declaration_swaps_sides() {
        let mut cfg = config();
        cfg.left_from = Some("profiles".into());
        cfg.right_from = Some("orders".into());
        let join = JoinOperator::new(cfg)
            .unwrap()
            .with_input_producers(&["orders".to_string(), "profiles".to_string()])
            .unwrap();
        // Input 1 is declared LEFT: a batch tagged 1 routes to the left side.
        join.process(side_batch(1, &["a"], &[100])).await.unwrap();
        // Input 0 is RIGHT; within the window it matches.
        let output = join.process(side_batch(0, &["a"], &[5_100])).await.unwrap();
        let ProcessResult::Single(batch) = output else {
            panic!("expected a joined batch");
        };
        // The declared-left (input 1) columns carry the l_ prefix.
        let names: Vec<String> = batch
            .record_batch()
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert!(names.iter().any(|n| n == "l_ts"), "{names:?}");
        assert!(names.iter().any(|n| n == "r_ts"), "{names:?}");
    }

    #[tokio::test]
    async fn three_column_sides_combine_types_correctly() {
        // Distinct per-column types surface any column/field misalignment.
        let join = JoinOperator::new(config()).unwrap();
        join.process(side_batch(0, &["a"], &[100])).await.unwrap();
        let output = join.process(side_batch(1, &["a"], &[100])).await.unwrap();
        let ProcessResult::Single(batch) = output else {
            panic!("expected a joined batch");
        };
        let schema = batch.record_batch().schema();
        for index in 0..schema.fields().len() {
            let field = schema.field(index);
            let column = batch.record_batch().column(index);
            assert_eq!(
                field.data_type(),
                column.data_type(),
                "column {index} ({}) type mismatch: field={:?} column={:?}",
                field.name(),
                field.data_type(),
                column.data_type()
            );
        }
    }

    #[tokio::test]
    async fn fan_out_matches_every_candidate() {
        let join = JoinOperator::new(config()).unwrap();
        join.process(side_batch(0, &["a"], &[100])).await.unwrap();
        join.process(side_batch(0, &["a"], &[150])).await.unwrap();
        let output = join.process(side_batch(1, &["a"], &[120])).await.unwrap();
        let ProcessResult::Single(batch) = output else {
            panic!("expected a joined batch");
        };
        assert_eq!(batch.record_batch().num_rows(), 2);
    }
}
