//! Keyed interval join operator for the unified execution kernel.
//!
//! The join operator sits in a chain with exactly two inbound edges (input 0
//! is the left side, input 1 the right side). The chain loop tags every batch
//! with a `__meta_input_index` column when a run contains a join operator, so
//! the processor can tell the sides apart. Rows are matched by an equality
//! key: a left row and a right row with the same key join when their event
//! timestamps differ by at most `window_ms`. Matched pairs are emitted as
//! they arrive (at-least-once). With `join_type` set to an outer form the
//! side(s) marked outer additionally emit never-matched rows at eviction
//! time, with the opposite side's columns all null. Per-side keyed buffers
//! are bounded: rows are evicted once the watermark advances past
//! `timestamp + window_ms + ttl_ms`, or when a side exceeds `max_per_key`
//! rows for one key (oldest first).
//!
//! State reconstructs from checkpoint replay: on recovery the sources rewind
//! to the acknowledged cut and the buffers rebuild deterministically, so the
//! operator carries no separate snapshot. Unmatched emission additionally
//! needs the opposite side's schema for its null columns: while that side has
//! not produced a batch, evicted rows park in a bounded pending queue and
//! flush at the next emission point once the schema is known.
use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;

use datafusion::arrow::array::Array as _;
use datafusion::arrow::array::{new_null_array, ArrayRef, Int64Array, StringArray, UInt32Array};
use datafusion::arrow::compute::interleave;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::processor::Processor;
use crate::{Error, MessageBatch, MessageBatchRef, ProcessResult};

/// The input-side tag column written by the chain loop for join chains.
pub(crate) const META_INPUT_INDEX: &str = "__meta_input_index";

/// Join flavour: matched pairs always emit; outer forms additionally emit
/// the outer side's never-matched rows once the match window closes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub(crate) enum JoinType {
    #[default]
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

impl JoinType {
    /// Whether never-matched rows of `side` are kept (emitted at eviction).
    fn keeps_unmatched(self, side: Side) -> bool {
        match self {
            JoinType::Inner => false,
            JoinType::LeftOuter => side == Side::Left,
            JoinType::RightOuter => side == Side::Right,
            JoinType::FullOuter => true,
        }
    }

    /// Whether `side`'s output columns must be nullable: the opposite side's
    /// unmatched rows carry all-null columns for this side.
    fn nullable_side(self, side: Side) -> bool {
        match self {
            JoinType::Inner => false,
            JoinType::LeftOuter => side == Side::Right,
            JoinType::RightOuter => side == Side::Left,
            JoinType::FullOuter => true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct JoinOperatorConfig {
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
    /// Join type. Matched pairs always emit; outer forms additionally emit
    /// the outer side's never-matched rows when the match window closes
    /// (watermark or capacity eviction), with the opposite side's columns
    /// all null. Defaults to `inner`.
    #[serde(default)]
    pub join_type: JoinType,
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
            return Err(Error::Config(
                "join operator ttl_ms must be non-negative".into(),
            ));
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
    /// Set once this row has produced at least one matched pair, so outer
    /// emission never re-emits a matched row as unmatched.
    matched: bool,
}

#[derive(Default)]
struct SideBuffer {
    // BTreeMap keeps eviction (and therefore unmatched emission) in key
    // order, so replay rebuilds byte-identical output.
    by_key: BTreeMap<String, VecDeque<BufferedRow>>,
    /// Schema of the batches seen so far on this side; per-side stability is
    /// required so gathered output columns stay well-typed.
    schema: Option<SchemaRef>,
    total: usize,
}

impl SideBuffer {
    /// Buffer one row; returns the row capacity-evicted by this push (at
    /// most one, since `max_per_key >= 1`), so outer forms can emit it as
    /// unmatched.
    fn push(
        &mut self,
        key: String,
        timestamp_ms: i64,
        batch: MessageBatchRef,
        row: usize,
        matched: bool,
        max_per_key: usize,
    ) -> Option<BufferedRow> {
        let queue = self.by_key.entry(key).or_default();
        queue.push_back(BufferedRow {
            timestamp_ms,
            batch,
            row,
            matched,
        });
        self.total += 1;
        if queue.len() > max_per_key {
            self.total -= 1;
            return queue.pop_front();
        }
        None
    }

    /// Remove rows whose match window has closed: once the watermark passes
    /// `timestamp + window_ms (+ ttl)`, no future row on the other side can
    /// still match. Returns the evicted rows with their keys so outer forms
    /// can emit the never-matched ones.
    fn evict(&mut self, watermark_ms: i64, window_ms: i64, ttl_ms: i64) -> Vec<MatchedRow> {
        let bound = watermark_ms
            .saturating_sub(window_ms)
            .saturating_sub(ttl_ms);
        let mut evicted = Vec::new();
        for (key, queue) in self.by_key.iter_mut() {
            while queue.front().is_some_and(|row| row.timestamp_ms < bound) {
                if let Some(row) = queue.pop_front() {
                    self.total -= 1;
                    evicted.push(MatchedRow {
                        row,
                        key: key.clone(),
                    });
                }
            }
        }
        self.by_key.retain(|_, queue| !queue.is_empty());
        evicted
    }
}

/// An owned (row, key) pair: matched rows accumulated during one process
/// call, or an evicted row awaiting unmatched emission.
struct MatchedRow {
    row: BufferedRow,
    key: String,
}

/// Never-matched rows parked until the opposite side's schema is known;
/// bounded per side by `max_per_key`.
#[derive(Default)]
struct PendingUnmatched {
    left: VecDeque<MatchedRow>,
    right: VecDeque<MatchedRow>,
}

pub(crate) struct JoinOperator {
    config: JoinOperatorConfig,
    /// Resolved channel indices for the left and right sides, in the chain's
    /// receiver order (see `JoinOperator::new`).
    left_index: u32,
    right_index: u32,
    left: Mutex<SideBuffer>,
    right: Mutex<SideBuffer>,
    pending_unmatched: Mutex<PendingUnmatched>,
    /// Throttled observability for capacity evictions (see
    /// [`JoinOperator::note_capacity_eviction`]).
    eviction_log: Mutex<EvictionThrottle>,
}

/// Capacity evictions must be observable — a silently dropped inner row is a
/// wrong join result — but a skewed key evicts on every batch, which would
/// flood the log. The first eviction logs immediately; subsequent lines are
/// throttled to one per interval and carry the suppressed count.
#[derive(Default)]
struct EvictionThrottle {
    last_log: [Option<std::time::Instant>; 2],
    suppressed: [u64; 2],
}

impl EvictionThrottle {
    /// Decide whether this eviction should emit a warn line, and with which
    /// suppressed count. Pure with respect to the injected clock so the
    /// throttling contract stays deterministically testable. The two sides
    /// throttle independently, so a left eviction never silences the first
    /// warn about a right eviction.
    fn should_log(&mut self, index: usize, now: std::time::Instant) -> Option<u64> {
        self.suppressed[index] += 1;
        if let Some(last) = self.last_log[index] {
            if now.duration_since(last) < EVICTION_LOG_INTERVAL {
                return None;
            }
        }
        let suppressed = self.suppressed[index];
        self.suppressed[index] = 0;
        self.last_log[index] = Some(now);
        Some(suppressed)
    }
}

/// Minimum spacing between capacity-eviction warn lines.
const EVICTION_LOG_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
/// Keys are truncated to keep the log bounded; 64 chars locate any hot key.
const EVICTED_KEY_MAX_CHARS: usize = 64;

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

    /// Output column prefix for this side's block.
    fn prefix(self) -> &'static str {
        match self {
            Side::Left => "l_",
            Side::Right => "r_",
        }
    }

    fn opposite(self) -> Side {
        match self {
            Side::Left => Side::Right,
            Side::Right => Side::Left,
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
            pending_unmatched: Mutex::new(PendingUnmatched::default()),
            eviction_log: Mutex::new(EvictionThrottle::default()),
        })
    }

    /// Record a capacity eviction (`max_per_key` exceeded) and warn,
    /// throttled to one line per [`EVICTION_LOG_INTERVAL`] per operator with
    /// the suppressed count folded in. Watermark evictions are normal
    /// semantics and stay silent.
    fn note_capacity_eviction(&self, side: Side, key: &str) {
        let index = match side {
            Side::Left => 0,
            Side::Right => 1,
        };
        let suppressed = self
            .eviction_log
            .lock()
            .expect("join eviction log lock")
            .should_log(index, std::time::Instant::now());
        let Some(suppressed) = suppressed else {
            return;
        };
        let truncated: String = key.chars().take(EVICTED_KEY_MAX_CHARS).collect();
        tracing::warn!(
            side = side.label(),
            key = %truncated,
            max_per_key = self.config.max_per_key,
            suppressed_since_last_log = suppressed,
            "join key buffer exceeded max_per_key; the oldest row was capacity-evicted \
             (inner rows are dropped — raise max_per_key or narrow window/ttl for skewed keys)"
        );
    }

    /// Resolve the left/right channel indices from the chain's inbound
    /// producer order. `left_from`/`right_from` name the upstream operator
    /// ids; with a single-producer default the indices fall back to 0/1.
    /// Each side must resolve to exactly one channel: an upstream operator
    /// feeding the join from more than one subtask is rejected here, at
    /// graph-build time, instead of failing on the first untagged batch at
    /// runtime.
    pub fn with_input_producers(mut self, producers: &[String]) -> Result<Self, Error> {
        let resolve = |declared: &Option<String>,
                       fallback: usize,
                       side: &str|
         -> Result<u32, Error> {
            let occurrences_for = |producer: &str| {
                producers
                    .iter()
                    .filter(|candidate| candidate.as_str() == producer)
                    .count()
            };
            let single_subtask_error = |producer: &str, occurrences: usize| {
                Error::Config(format!(
                    "join {side} side upstream '{producer}' feeds this join from \
                     {occurrences} subtasks; keyed join resolves one channel per side — \
                     set the upstream operator's parallelism to 1"
                ))
            };
            let producer = match declared {
                Some(operator_id) => operator_id.as_str(),
                None => {
                    // Undeclared sides keep the positional fallback; the
                    // duplicate-producer guard still applies when a producer
                    // occupies the fallback position.
                    if let Some(producer) = producers.get(fallback) {
                        let occurrences = occurrences_for(producer.as_str());
                        if occurrences > 1 {
                            return Err(single_subtask_error(producer, occurrences));
                        }
                    }
                    return Ok(fallback as u32);
                }
            };
            match occurrences_for(producer) {
                0 => Err(Error::Config(format!(
                    "join operator 'left_from/right_from' names upstream '{producer}' which does not feed this join"
                ))),
                1 => Ok(producers
                    .iter()
                    .position(|candidate| candidate.as_str() == producer)
                    .expect("occurrence counted above") as u32),
                occurrences => Err(single_subtask_error(producer, occurrences)),
            }
        };
        self.left_index = resolve(&self.config.left_from, 0, "left")?;
        self.right_index = resolve(&self.config.right_from, 1, "right")?;
        if self.left_index == self.right_index {
            return Err(Error::Config(format!(
                "join left and right sides resolve to the same input channel {}; declare both sides explicitly",
                self.left_index
            )));
        }
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
                Error::Config(format!(
                    "join key column '{column}' must be a string column"
                ))
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
                Error::Config(format!(
                    "column '{META_INPUT_INDEX}' must be a UInt32 column"
                ))
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

    fn remember_schema(
        side: &mut SideBuffer,
        batch: &MessageBatch,
        label: &str,
    ) -> Result<(), Error> {
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
                    matched.row.batch.record_batch().column(index)
                        as &dyn datafusion::arrow::array::Array
                })
                .collect();
            let indices: Vec<(usize, usize)> = rows
                .iter()
                .map(|matched| (0usize, matched.row.row))
                .collect();
            let array = interleave(&arrays, &indices)
                .map_err(|error| Error::Process(format!("join gather failed: {error}")))?;
            columns.push(array);
        }
        columns.push(Arc::new(StringArray::from(
            rows.iter()
                .map(|matched| matched.key.as_str())
                .collect::<Vec<_>>(),
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

    /// Assemble one unmatched emission: the buffered side's gathered columns
    /// plus all-null columns for the opposite side, in the same field order
    /// and with the same nullability as matched output under `join_type`.
    fn assemble_unmatched(
        rows: &[MatchedRow],
        side: Side,
        this_schema: &Schema,
        opposite_schema: &Schema,
        join_type: JoinType,
    ) -> Result<MessageBatch, Error> {
        let this_batch = Self::gather(rows, this_schema)?;
        let join_key = this_batch.column(this_batch.num_columns() - 1).clone();
        let this_block =
            block_from_batch(&this_batch, side.prefix(), join_type.nullable_side(side));
        let opposite_block = null_block(
            opposite_schema,
            side.opposite().prefix(),
            this_batch.num_rows(),
        );
        let (left_block, right_block) = match side {
            Side::Left => (this_block, opposite_block),
            Side::Right => (opposite_block, this_block),
        };
        combine_blocks(left_block, right_block, join_key)
    }

    /// Park unmatched rows until the opposite side's schema is known. The
    /// queue is bounded per side by `max_per_key`; overflow drops the oldest
    /// rows with a warning.
    fn stash_pending(
        pending: &Mutex<PendingUnmatched>,
        side: Side,
        mut rows: Vec<MatchedRow>,
        max_per_key: usize,
    ) {
        let Ok(mut pending) = pending.lock() else {
            return;
        };
        let queue = match side {
            Side::Left => &mut pending.left,
            Side::Right => &mut pending.right,
        };
        for row in rows.drain(..) {
            queue.push_back(row);
            while queue.len() > max_per_key {
                if queue.pop_front().is_some() {
                    tracing::warn!(
                        side = side.label(),
                        "join pending unmatched queue over capacity; dropping the oldest unmatched row"
                    );
                }
            }
        }
    }

    /// Emit every parked row whose opposite side schema has arrived; rows
    /// still waiting remain parked. Returns the emitted batches.
    fn flush_pending(
        pending: &Mutex<PendingUnmatched>,
        left: &SideBuffer,
        right: &SideBuffer,
        join_type: JoinType,
    ) -> Vec<MessageBatchRef> {
        let mut outputs = Vec::new();
        let Ok(mut pending) = pending.lock() else {
            return outputs;
        };
        for side in [Side::Left, Side::Right] {
            let mut drained: Vec<MatchedRow> = {
                let queue = match side {
                    Side::Left => &mut pending.left,
                    Side::Right => &mut pending.right,
                };
                std::mem::take(queue).into()
            };
            if drained.is_empty() {
                continue;
            }
            let (this_buffer, opposite_buffer) = match side {
                Side::Left => (left, right),
                Side::Right => (right, left),
            };
            let emitted = match (&this_buffer.schema, &opposite_buffer.schema) {
                (Some(this_schema), Some(opposite_schema)) => {
                    match Self::assemble_unmatched(
                        &drained,
                        side,
                        this_schema,
                        opposite_schema,
                        join_type,
                    ) {
                        Ok(batch) => Some(Arc::new(batch)),
                        Err(error) => {
                            tracing::warn!(
                                side = side.label(),
                                %error,
                                "join pending unmatched emission failed; rows remain parked"
                            );
                            None
                        }
                    }
                }
                _ => None,
            };
            match emitted {
                Some(batch) => outputs.push(batch),
                None => {
                    let queue = match side {
                        Side::Left => &mut pending.left,
                        Side::Right => &mut pending.right,
                    };
                    queue.extend(drained.drain(..));
                }
            }
        }
        outputs
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
        let (this_label, other_label) = (
            side.label(),
            match side {
                Side::Left => "right",
                Side::Right => "left",
            },
        );
        let mut this_side = match side {
            Side::Left => self.left.lock(),
            Side::Right => self.right.lock(),
        }
        .map_err(|_| Error::Process(format!("join {this_label} lock poisoned")))?;
        let mut other_side = match side {
            Side::Left => self.right.lock(),
            Side::Right => self.left.lock(),
        }
        .map_err(|_| Error::Process(format!("join {other_label} lock poisoned")))?;
        Self::remember_schema(&mut this_side, batch, this_label)?;
        // This batch may establish the schema that parked rows of the
        // opposite side are waiting for; flush them before new output.
        let mut outputs = {
            let (left_buffer, right_buffer) = match side {
                Side::Left => (&*this_side, &*other_side),
                Side::Right => (&*other_side, &*this_side),
            };
            Self::flush_pending(
                &self.pending_unmatched,
                left_buffer,
                right_buffer,
                self.config.join_type,
            )
        };

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
        let mut capacity_unmatched: Vec<MatchedRow> = Vec::new();
        for (key, timestamp, row) in extracted {
            let mut matched_here = false;
            if let Some(candidates) = other_side.by_key.get_mut(&key) {
                for candidate in candidates.iter_mut() {
                    if (candidate.timestamp_ms - timestamp).abs() <= self.config.window_ms {
                        candidate.matched = true;
                        matched_here = true;
                        matched_this.push(MatchedRow {
                            row: BufferedRow {
                                timestamp_ms: timestamp,
                                batch: batch.clone(),
                                row,
                                matched: true,
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
            if let Some(evicted) = this_side.push(
                key.clone(),
                timestamp,
                batch.clone(),
                row,
                matched_here,
                self.config.max_per_key,
            ) {
                // Capacity evictions are observable for both inner and
                // outer forms (inner rows are dropped here — invisible data
                // loss without the warn).
                self.note_capacity_eviction(side, &key);
                // Capacity eviction carries no watermark guarantee; outer
                // forms emit the row as unmatched anyway (at-least-once
                // artifact, see spec).
                if !evicted.matched && self.config.join_type.keeps_unmatched(side) {
                    capacity_unmatched.push(MatchedRow { row: evicted, key });
                }
            }
        }
        if !matched_this.is_empty() {
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
                Side::Left => combine_sides(&this_batch, &other_batch, self.config.join_type)?,
                Side::Right => combine_sides(&other_batch, &this_batch, self.config.join_type)?,
            };
            outputs.push(Arc::new(output));
        }
        if !capacity_unmatched.is_empty() {
            let this_schema = this_side
                .schema
                .clone()
                .expect("schema remembered before buffering");
            match &other_side.schema {
                Some(other_schema) => {
                    let batch = Self::assemble_unmatched(
                        &capacity_unmatched,
                        side,
                        &this_schema,
                        other_schema,
                        self.config.join_type,
                    )?;
                    outputs.push(Arc::new(batch));
                }
                None => Self::stash_pending(
                    &self.pending_unmatched,
                    side,
                    capacity_unmatched,
                    self.config.max_per_key,
                ),
            }
        }
        match outputs.len() {
            0 => Ok(ProcessResult::None),
            1 => Ok(ProcessResult::Single(
                outputs.pop().expect("length checked above"),
            )),
            _ => Ok(ProcessResult::Multiple(outputs)),
        }
    }
}

/// Rename one gathered batch's columns with `prefix` (dropping the join key
/// and input tag) for the combined output. `nullable` upgrades — never
/// removes — nullability, so inner output schemas stay identical to the
/// pre-outer-join behaviour.
fn block_from_batch(
    batch: &RecordBatch,
    prefix: &str,
    nullable: bool,
) -> (Vec<Field>, Vec<ArrayRef>) {
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (index, field) in batch.schema().fields().iter().enumerate() {
        let name = field.name();
        if name == "join_key" || name == META_INPUT_INDEX {
            continue;
        }
        fields.push(Field::new(
            format!("{prefix}{name}"),
            field.data_type().clone(),
            nullable || field.is_nullable(),
        ));
        columns.push(batch.column(index).clone());
    }
    (fields, columns)
}

/// Build the all-null block for the opposite side of an unmatched emission,
/// typed and ordered by that side's schema.
fn null_block(schema: &Schema, prefix: &str, rows: usize) -> (Vec<Field>, Vec<ArrayRef>) {
    let mut fields = Vec::with_capacity(schema.fields().len());
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields().iter() {
        let name = field.name();
        if name == "join_key" || name == META_INPUT_INDEX {
            continue;
        }
        let field = Field::new(format!("{prefix}{name}"), field.data_type().clone(), true);
        columns.push(new_null_array(field.data_type(), rows));
        fields.push(field);
    }
    (fields, columns)
}

fn combine_blocks(
    left: (Vec<Field>, Vec<ArrayRef>),
    right: (Vec<Field>, Vec<ArrayRef>),
    join_key: ArrayRef,
) -> Result<MessageBatch, Error> {
    let (mut fields, mut columns) = left;
    fields.extend(right.0);
    columns.extend(right.1);
    fields.push(Field::new("join_key", DataType::Utf8, false));
    columns.push(join_key);
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, columns)
        .map_err(|error| Error::Process(format!("join combine failed: {error}")))?;
    Ok(MessageBatch::new_arrow(batch))
}

/// Prefix left columns `l_*`, right columns `r_*`, and append the join key.
/// Nullability follows `join_type`: sides that can be all-null in unmatched
/// emissions are nullable here so matched and unmatched outputs share one
/// schema.
fn combine_sides(
    left: &RecordBatch,
    right: &RecordBatch,
    join_type: JoinType,
) -> Result<MessageBatch, Error> {
    combine_blocks(
        block_from_batch(left, "l_", join_type.nullable_side(Side::Left)),
        block_from_batch(right, "r_", join_type.nullable_side(Side::Right)),
        left.column(left.num_columns() - 1).clone(),
    )
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
        // Rows parked waiting for a side schema may flush on every emission
        // point; older rows leave first.
        let mut outputs = Self::flush_pending(
            &self.pending_unmatched,
            &left,
            &right,
            self.config.join_type,
        );
        let evictions = [
            (
                Side::Left,
                left.evict(watermark_ms, self.config.window_ms, self.config.ttl_ms),
            ),
            (
                Side::Right,
                right.evict(watermark_ms, self.config.window_ms, self.config.ttl_ms),
            ),
        ];
        for (side, rows) in evictions {
            if !self.config.join_type.keeps_unmatched(side) {
                continue;
            }
            let unmatched: Vec<MatchedRow> = rows
                .into_iter()
                .filter(|matched| !matched.row.matched)
                .collect();
            if unmatched.is_empty() {
                continue;
            }
            let (this_buffer, other_buffer) = match side {
                Side::Left => (&*left, &*right),
                Side::Right => (&*right, &*left),
            };
            match (&this_buffer.schema, &other_buffer.schema) {
                (Some(this_schema), Some(other_schema)) => {
                    let batch = Self::assemble_unmatched(
                        &unmatched,
                        side,
                        this_schema,
                        other_schema,
                        self.config.join_type,
                    )?;
                    outputs.push(Arc::new(batch));
                }
                _ => Self::stash_pending(
                    &self.pending_unmatched,
                    side,
                    unmatched,
                    self.config.max_per_key,
                ),
            }
        }
        match outputs.len() {
            0 => Ok(ProcessResult::None),
            1 => Ok(ProcessResult::Single(
                outputs.pop().expect("length checked above"),
            )),
            _ => Ok(ProcessResult::Multiple(outputs)),
        }
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
                Arc::new(StringArray::from(
                    keys.iter().map(|k| Some(*k)).collect::<Vec<_>>(),
                )),
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
            join_type: JoinType::Inner,
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
            join.process(side_batch(1, &["a"], &[20_000]))
                .await
                .unwrap(),
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
        join.process(side_batch(0, &["a", "a"], &[100, 200]))
            .await
            .unwrap();
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

    /// Spec "inner 容量逐出可观测": the eviction throttle logs the first
    /// eviction immediately, suppresses lines inside the interval (counting
    /// them), and folds the suppressed count into the next line. Tested
    /// against the pure decision function with a synthetic clock — a
    /// subscriber-capturing variant raced with sibling tests' global
    /// tracing setup under the full parallel suite.
    #[test]
    fn eviction_throttle_first_logs_immediately_then_suppresses() {
        let mut throttle = EvictionThrottle::default();
        let t0 = std::time::Instant::now();

        assert_eq!(
            throttle.should_log(0, t0),
            Some(1),
            "the first eviction logs immediately with count 1"
        );
        assert_eq!(
            throttle.should_log(0, t0 + std::time::Duration::from_millis(10)),
            None,
            "lines inside the interval are suppressed"
        );
        assert_eq!(
            throttle.should_log(0, t0 + std::time::Duration::from_millis(20)),
            None,
            "suppression continues for the whole interval"
        );
        assert_eq!(
            throttle.should_log(1, t0 + std::time::Duration::from_millis(30)),
            Some(1),
            "the two sides throttle independently"
        );
        assert_eq!(
            throttle.should_log(0, t0 + EVICTION_LOG_INTERVAL),
            Some(3),
            "after the interval the line carries the two suppressed evictions"
        );
        assert_eq!(
            throttle.should_log(0, t0 + EVICTION_LOG_INTERVAL + EVICTION_LOG_INTERVAL),
            Some(1),
            "the counter resets after each emitted line"
        );
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

    #[test]
    fn join_type_defaults_to_inner_and_rejects_invalid_values() {
        let config: JoinOperatorConfig = serde_json::from_value(serde_json::json!({
            "left_key": "key",
            "right_key": "key",
            "window_ms": 0,
        }))
        .unwrap();
        assert_eq!(config.join_type, JoinType::Inner);

        let config: JoinOperatorConfig = serde_json::from_value(serde_json::json!({
            "left_key": "key",
            "right_key": "key",
            "window_ms": 0,
            "join_type": "left_outer",
        }))
        .unwrap();
        assert_eq!(config.join_type, JoinType::LeftOuter);

        let error = serde_json::from_value::<JoinOperatorConfig>(serde_json::json!({
            "left_key": "key",
            "right_key": "key",
            "window_ms": 0,
            "join_type": "cross_outer",
        }))
        .unwrap_err();
        assert!(error.to_string().contains("unknown variant"), "{error}");
    }

    fn outer_config(join_type: JoinType) -> JoinOperatorConfig {
        let mut config = config();
        config.join_type = join_type;
        config
    }

    fn string_column<'a>(batch: &'a MessageBatch, column: &str) -> &'a StringArray {
        let index = batch.record_batch().schema().index_of(column).unwrap();
        batch
            .record_batch()
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
    }

    #[tokio::test]
    async fn left_outer_emits_unmatched_on_watermark_eviction() {
        let join = JoinOperator::new(outer_config(JoinType::LeftOuter)).unwrap();
        // A right row with a different key establishes the right schema
        // without matching the left row under test.
        assert!(matches!(
            join.process(side_batch(1, &["b"], &[100])).await.unwrap(),
            ProcessResult::None
        ));
        assert!(matches!(
            join.process(side_batch(0, &["a"], &[100])).await.unwrap(),
            ProcessResult::None
        ));
        let output = join.on_watermark(6_000).await.unwrap();
        let ProcessResult::Single(batch) = output else {
            panic!("expected an unmatched emission");
        };
        assert_eq!(batch.record_batch().num_rows(), 1);
        assert_eq!(string_column(&batch, "l_key").value(0), "a");
        assert_eq!(string_column(&batch, "join_key").value(0), "a");
        assert!(string_column(&batch, "r_key").is_null(0));
        // The outer-kept left columns keep their nullability; the possibly
        // all-null right columns are nullable in the schema.
        let schema = batch.record_batch().schema();
        assert!(!schema.field_with_name("l_key").unwrap().is_nullable());
        assert!(schema.field_with_name("r_key").unwrap().is_nullable());
    }

    #[tokio::test]
    async fn right_outer_emits_unmatched_on_watermark_eviction() {
        let join = JoinOperator::new(outer_config(JoinType::RightOuter)).unwrap();
        // A left row with a different key establishes the left schema
        // without matching the right row under study.
        assert!(matches!(
            join.process(side_batch(0, &["b"], &[100])).await.unwrap(),
            ProcessResult::None
        ));
        assert!(matches!(
            join.process(side_batch(1, &["a"], &[100])).await.unwrap(),
            ProcessResult::None
        ));
        let output = join.on_watermark(6_000).await.unwrap();
        let ProcessResult::Single(batch) = output else {
            panic!("expected an unmatched emission");
        };
        assert_eq!(batch.record_batch().num_rows(), 1);
        assert_eq!(string_column(&batch, "r_key").value(0), "a");
        assert_eq!(string_column(&batch, "join_key").value(0), "a");
        // The unmatched right row carries all-null LEFT columns.
        assert!(string_column(&batch, "l_key").is_null(0));
        let schema = batch.record_batch().schema();
        assert!(!schema.field_with_name("r_key").unwrap().is_nullable());
        assert!(schema.field_with_name("l_key").unwrap().is_nullable());
    }

    #[tokio::test]
    async fn matched_rows_are_not_emitted_as_unmatched() {
        let join = JoinOperator::new(outer_config(JoinType::FullOuter)).unwrap();
        assert!(matches!(
            join.process(side_batch(0, &["a"], &[100])).await.unwrap(),
            ProcessResult::None
        ));
        // The pair matches, marking both rows.
        assert!(matches!(
            join.process(side_batch(1, &["a"], &[100])).await.unwrap(),
            ProcessResult::Single(_)
        ));
        // Both rows evict past the window close but neither re-emits.
        assert!(matches!(
            join.on_watermark(6_000).await.unwrap(),
            ProcessResult::None
        ));
    }

    #[tokio::test]
    async fn capacity_eviction_emits_unmatched_in_outer_mode() {
        let mut cfg = outer_config(JoinType::LeftOuter);
        cfg.max_per_key = 1;
        let join = JoinOperator::new(cfg).unwrap();
        join.process(side_batch(1, &["b"], &[100])).await.unwrap();
        join.process(side_batch(0, &["a"], &[100])).await.unwrap();
        // Pushing the second "a" row evicts the first; outer emits it as
        // unmatched even though the watermark has not moved (at-least-once
        // artifact of the capacity bound).
        let output = join.process(side_batch(0, &["a"], &[200])).await.unwrap();
        let ProcessResult::Single(unmatched) = output else {
            panic!("expected an unmatched capacity emission");
        };
        assert_eq!(unmatched.record_batch().num_rows(), 1);
        let ts = unmatched
            .record_batch()
            .column(unmatched.record_batch().schema().index_of("l_ts").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts.value(0), 100);
        assert!(string_column(&unmatched, "r_key").is_null(0));
        // The surviving row still matches: the at-least-once double emission
        // (unmatched + pair) is the documented capacity-eviction semantics.
        let output = join.process(side_batch(1, &["a"], &[200])).await.unwrap();
        let ProcessResult::Single(pair) = output else {
            panic!("expected a matched pair");
        };
        assert_eq!(pair.record_batch().num_rows(), 1);
    }

    #[tokio::test]
    async fn full_outer_emits_both_sides_unmatched() {
        let join = JoinOperator::new(outer_config(JoinType::FullOuter)).unwrap();
        join.process(side_batch(0, &["a"], &[100])).await.unwrap();
        join.process(side_batch(1, &["b"], &[100])).await.unwrap();
        let output = join.on_watermark(6_000).await.unwrap();
        let ProcessResult::Multiple(batches) = output else {
            panic!("expected one emission per outer side");
        };
        assert_eq!(batches.len(), 2);
        let left_unmatched = &batches[0];
        assert_eq!(string_column(left_unmatched, "l_key").value(0), "a");
        assert!(string_column(left_unmatched, "r_key").is_null(0));
        let right_unmatched = &batches[1];
        assert_eq!(string_column(right_unmatched, "r_key").value(0), "b");
        assert!(string_column(right_unmatched, "l_key").is_null(0));
    }

    #[tokio::test]
    async fn inner_mode_output_schema_has_no_nullable_drift() {
        let join = JoinOperator::new(config()).unwrap();
        join.process(side_batch(0, &["a"], &[100])).await.unwrap();
        let output = join.process(side_batch(1, &["a"], &[100])).await.unwrap();
        let ProcessResult::Single(batch) = output else {
            panic!("expected a joined batch");
        };
        let schema = batch.record_batch().schema();
        assert!(!schema.field_with_name("l_key").unwrap().is_nullable());
        assert!(!schema.field_with_name("r_key").unwrap().is_nullable());
    }

    #[tokio::test]
    async fn pending_unmatched_flushes_when_opposite_schema_arrives() {
        let join = JoinOperator::new(outer_config(JoinType::LeftOuter)).unwrap();
        join.process(side_batch(0, &["a"], &[100])).await.unwrap();
        // The left row evicts with no right schema known: it parks instead
        // of emitting.
        assert!(matches!(
            join.on_watermark(6_000).await.unwrap(),
            ProcessResult::None
        ));
        // The first right batch establishes the schema; the parked row
        // flushes as this call's output.
        let output = join.process(side_batch(1, &["b"], &[6_000])).await.unwrap();
        let ProcessResult::Single(batch) = output else {
            panic!("expected the parked unmatched emission to flush");
        };
        assert_eq!(string_column(&batch, "l_key").value(0), "a");
        assert!(string_column(&batch, "r_key").is_null(0));
    }

    #[tokio::test]
    async fn pending_unmatched_is_bounded_and_drops_oldest() {
        let mut cfg = outer_config(JoinType::LeftOuter);
        cfg.max_per_key = 1;
        let join = JoinOperator::new(cfg).unwrap();
        join.process(side_batch(0, &["a", "b"], &[100, 200]))
            .await
            .unwrap();
        // Both rows evict while the right schema is unknown: the pending
        // queue is bounded by max_per_key, so the older row ("a") drops.
        assert!(matches!(
            join.on_watermark(6_000).await.unwrap(),
            ProcessResult::None
        ));
        let output = join.process(side_batch(1, &["c"], &[6_000])).await.unwrap();
        let ProcessResult::Single(batch) = output else {
            panic!("expected the parked unmatched emission to flush");
        };
        assert_eq!(batch.record_batch().num_rows(), 1);
        assert_eq!(string_column(&batch, "l_key").value(0), "b");
    }

    /// Recovery replays inputs into a fresh operator instance; feeding the
    /// same ordered sequence twice must reproduce identical emissions
    /// (matched pairs and unmatched rows alike).
    async fn run_outer_sequence(join: &JoinOperator) -> Vec<(String, Option<String>)> {
        let mut seen = Vec::new();
        let mut record = |result: ProcessResult| match result {
            ProcessResult::None => {}
            ProcessResult::Single(batch) => {
                for row in 0..batch.record_batch().num_rows() {
                    let left = string_column(&batch, "l_key").value(row).to_owned();
                    let right = if string_column(&batch, "r_key").is_null(row) {
                        None
                    } else {
                        Some(string_column(&batch, "r_key").value(row).to_owned())
                    };
                    seen.push((left, right));
                }
            }
            ProcessResult::Multiple(batches) => {
                for batch in batches {
                    for row in 0..batch.record_batch().num_rows() {
                        let left = string_column(&batch, "l_key").value(row).to_owned();
                        let right = if string_column(&batch, "r_key").is_null(row) {
                            None
                        } else {
                            Some(string_column(&batch, "r_key").value(row).to_owned())
                        };
                        seen.push((left, right));
                    }
                }
            }
            _ => panic!("unexpected process result variant"),
        };
        // Deliver both sides exactly as the chain loop would; the unmatched
        // keys rely on key-ordered eviction after the watermark passes.
        record(
            join.process(side_batch(1, &["z", "a"], &[100, 120]))
                .await
                .unwrap(),
        );
        record(
            join.process(side_batch(0, &["a", "b", "c"], &[100, 6_000, 6_050]))
                .await
                .unwrap(),
        );
        record(join.on_watermark(11_200).await.unwrap());
        seen
    }

    #[tokio::test]
    async fn replay_rebuild_reproduces_outer_emissions_deterministically() {
        let first = JoinOperator::new(outer_config(JoinType::LeftOuter)).unwrap();
        let emissions = run_outer_sequence(&first).await;
        // "a" pairs with the right "a"; "b"/"c" have no right counterpart
        // and must flush as unmatched rows once the watermark passes.
        assert_eq!(
            emissions,
            vec![
                ("a".to_owned(), Some("a".to_owned())),
                ("b".to_owned(), None),
                ("c".to_owned(), None),
            ],
            "{emissions:?}"
        );
        // A fresh operator fed the same ordered sequence (the replay
        // contract) emits byte-identical results.
        let replay = JoinOperator::new(outer_config(JoinType::LeftOuter)).unwrap();
        assert_eq!(run_outer_sequence(&replay).await, emissions);
    }

    #[test]
    fn multi_subtask_side_is_rejected_at_build_time() {
        let mut cfg = config();
        cfg.left_from = Some("gen".into());
        cfg.right_from = Some("other".into());
        let error = match JoinOperator::new(cfg.clone())
            .unwrap()
            .with_input_producers(&["gen".to_string(), "gen".to_string(), "other".to_string()])
        {
            Err(error) => error,
            Ok(_) => panic!("expected a build-time rejection for a multi-subtask side"),
        };
        assert!(error.to_string().contains("2 subtasks"), "{error}");

        // Undeclared sides fall back positionally but still reject a
        // producer that feeds the join from several subtasks.
        let error = match JoinOperator::new(config())
            .unwrap()
            .with_input_producers(&["gen".to_string(), "gen".to_string()])
        {
            Err(error) => error,
            Ok(_) => panic!("expected a build-time rejection for an undeclared side"),
        };
        assert!(error.to_string().contains("parallelism to 1"), "{error}");
    }

    #[test]
    fn same_channel_for_both_sides_is_rejected_at_build_time() {
        // left_from names the second producer while the undeclared right
        // side falls back to index 1: both sides resolve to channel 1.
        let mut cfg = config();
        cfg.left_from = Some("profiles".into());
        let error = match JoinOperator::new(cfg)
            .unwrap()
            .with_input_producers(&["orders".to_string(), "profiles".to_string()])
        {
            Err(error) => error,
            Ok(_) => panic!("expected a build-time rejection for a shared channel"),
        };
        assert!(error.to_string().contains("same input channel"), "{error}");
    }

    // ---------- validation coverage ----------

    #[test]
    fn config_validation_rejects_each_invalid_field() {
        let mut cfg = config();
        cfg.left_key = String::new();
        assert!(cfg
            .validate()
            .unwrap_err()
            .to_string()
            .contains("non-empty left_key"));

        let mut cfg = config();
        cfg.right_key = String::new();
        assert!(cfg.validate().is_err());

        let mut cfg = config();
        cfg.window_ms = -1;
        assert!(cfg
            .validate()
            .unwrap_err()
            .to_string()
            .contains("window_ms must be non-negative"));

        let mut cfg = config();
        cfg.ttl_ms = -1;
        assert!(cfg
            .validate()
            .unwrap_err()
            .to_string()
            .contains("ttl_ms must be non-negative"));

        let mut cfg = config();
        cfg.max_per_key = 0;
        assert!(cfg
            .validate()
            .unwrap_err()
            .to_string()
            .contains("max_per_key must be at least 1"));

        // The all-valid configuration still passes.
        assert!(config().validate().is_ok());
    }

    #[test]
    fn input_producer_resolution_positional_fallback_and_unknown_producer() {
        // Undeclared sides fall back positionally when each fallback position
        // is occupied by exactly one producer channel.
        let join = JoinOperator::new(config())
            .unwrap()
            .with_input_producers(&["orders".to_string(), "profiles".to_string()])
            .expect("positional fallback resolves unique producers");
        assert_eq!(join.left_index, 0);
        assert_eq!(join.right_index, 1);

        // A declared side naming a producer that does not feed the join is a
        // build-time error.
        let mut cfg = config();
        cfg.left_from = Some("ghost".into());
        let error = match JoinOperator::new(cfg)
            .unwrap()
            .with_input_producers(&["orders".to_string(), "profiles".to_string()])
        {
            Err(error) => error,
            Ok(_) => panic!("expected a rejection for an unknown declared producer"),
        };
        assert!(
            error.to_string().contains("which does not feed this join"),
            "{error}"
        );
    }

    // ---------- column extraction coverage ----------

    fn typed_side_batch(
        side: u32,
        key_column: Field,
        key_values: ArrayRef,
        timestamp_column: Field,
        timestamp_values: ArrayRef,
    ) -> MessageBatchRef {
        let schema = Arc::new(Schema::new(vec![
            key_column,
            timestamp_column,
            Field::new(META_INPUT_INDEX, DataType::UInt32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                key_values,
                timestamp_values,
                Arc::new(UInt32Array::from(vec![side; 1])),
            ],
        )
        .unwrap();
        Arc::new(MessageBatch::new_arrow(batch))
    }

    fn key_batch(side: u32, key: Option<&str>) -> MessageBatchRef {
        typed_side_batch(
            side,
            Field::new("key", DataType::Utf8, true),
            Arc::new(StringArray::from(vec![key])),
            Field::new("ts", DataType::Int64, true),
            Arc::new(Int64Array::from(vec![Some(100)])),
        )
    }

    #[tokio::test]
    async fn key_column_must_be_a_non_null_string() {
        // A non-string key column is rejected.
        let join = JoinOperator::new(config()).unwrap();
        let batch = typed_side_batch(
            0,
            Field::new("key", DataType::Int64, false),
            Arc::new(Int64Array::from(vec![1])),
            Field::new("ts", DataType::Int64, false),
            Arc::new(Int64Array::from(vec![100])),
        );
        let error = join.process(batch).await.unwrap_err();
        assert!(
            error.to_string().contains("must be a string column"),
            "{error}"
        );

        // A null key value is rejected.
        let join = JoinOperator::new(config()).unwrap();
        let error = join.process(key_batch(0, None)).await.unwrap_err();
        assert!(error.to_string().contains("is null at row 0"), "{error}");
    }

    #[tokio::test]
    async fn input_tag_column_must_be_a_non_null_uint32() {
        // A wrongly-typed tag column is rejected.
        let join = JoinOperator::new(config()).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new(META_INPUT_INDEX, DataType::Utf8, false),
        ]));
        let batch = Arc::new(MessageBatch::new_arrow(
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec!["a"])),
                    Arc::new(Int64Array::from(vec![100])),
                    Arc::new(StringArray::from(vec!["zero"])),
                ],
            )
            .unwrap(),
        ));
        let error = join.process(batch).await.unwrap_err();
        assert!(
            error.to_string().contains("must be a UInt32 column"),
            "{error}"
        );

        // A null tag value is rejected.
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new(META_INPUT_INDEX, DataType::UInt32, true),
        ]));
        let batch = Arc::new(MessageBatch::new_arrow(
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec!["a"])),
                    Arc::new(Int64Array::from(vec![100])),
                    Arc::new(UInt32Array::from(vec![None::<u32>])),
                ],
            )
            .unwrap(),
        ));
        let error = join.process(batch).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains(&format!("'{META_INPUT_INDEX}' is null")),
            "{error}"
        );
    }

    #[tokio::test]
    async fn timestamp_columns_support_arrow_temporal_types_and_reject_others() {
        use datafusion::arrow::array::{TimestampMillisecondArray, TimestampNanosecondArray};

        // TimestampMillisecondArray values pass through unchanged.
        let join = JoinOperator::new(config()).unwrap();
        join.process(typed_side_batch(
            0,
            Field::new("key", DataType::Utf8, false),
            Arc::new(StringArray::from(vec!["a"])),
            Field::new(
                "ts",
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, None),
                true,
            ),
            Arc::new(TimestampMillisecondArray::from(vec![Some(5_000)])),
        ))
        .await
        .unwrap();
        let output = join.process(side_batch(1, &["a"], &[5_000])).await.unwrap();
        assert!(matches!(output, ProcessResult::Single(_)));

        // TimestampMillisecondArray nulls are rejected.
        let join = JoinOperator::new(config()).unwrap();
        let error = join
            .process(typed_side_batch(
                0,
                Field::new("key", DataType::Utf8, false),
                Arc::new(StringArray::from(vec!["a"])),
                Field::new(
                    "ts",
                    DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, None),
                    true,
                ),
                Arc::new(TimestampMillisecondArray::from(vec![None::<i64>])),
            ))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("is null at row 0"), "{error}");

        // TimestampNanosecondArray values normalize to milliseconds.
        let join = JoinOperator::new(config()).unwrap();
        join.process(typed_side_batch(
            0,
            Field::new("key", DataType::Utf8, false),
            Arc::new(StringArray::from(vec!["a"])),
            Field::new(
                "ts",
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None),
                true,
            ),
            Arc::new(TimestampNanosecondArray::from(vec![Some(2_000_000_000)])),
        ))
        .await
        .unwrap();
        // 2s in nanoseconds matches a right row at 2_000ms.
        let output = join.process(side_batch(1, &["a"], &[2_000])).await.unwrap();
        assert!(matches!(output, ProcessResult::Single(_)));

        // TimestampNanosecondArray nulls are rejected.
        let join = JoinOperator::new(config()).unwrap();
        let error = join
            .process(typed_side_batch(
                0,
                Field::new("key", DataType::Utf8, false),
                Arc::new(StringArray::from(vec!["a"])),
                Field::new(
                    "ts",
                    DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None),
                    true,
                ),
                Arc::new(TimestampNanosecondArray::from(vec![None::<i64>])),
            ))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("is null at row 0"), "{error}");

        // Int64 nulls are rejected.
        let join = JoinOperator::new(config()).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, true),
            Field::new(META_INPUT_INDEX, DataType::UInt32, false),
        ]));
        let batch = Arc::new(MessageBatch::new_arrow(
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec!["a"])),
                    Arc::new(Int64Array::from(vec![None::<i64>])),
                    Arc::new(UInt32Array::from(vec![0])),
                ],
            )
            .unwrap(),
        ));
        let error = join.process(batch).await.unwrap_err();
        assert!(error.to_string().contains("is null at row 0"), "{error}");

        // A non-numeric timestamp column is rejected.
        let join = JoinOperator::new(config()).unwrap();
        let error = join
            .process(typed_side_batch(
                0,
                Field::new("key", DataType::Utf8, false),
                Arc::new(StringArray::from(vec!["a"])),
                Field::new("ts", DataType::Utf8, false),
                Arc::new(StringArray::from(vec!["not-a-number"])),
            ))
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("must be an Int64 or Timestamp column"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn per_side_schema_must_stay_stable() {
        let join = JoinOperator::new(config()).unwrap();
        join.process(side_batch(0, &["a"], &[100])).await.unwrap();
        // A second left batch with a different schema is rejected.
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
            Field::new("ts", DataType::Int64, false),
            Field::new(META_INPUT_INDEX, DataType::UInt32, false),
        ]));
        let batch = Arc::new(MessageBatch::new_arrow(
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec!["a"])),
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(Int64Array::from(vec![100])),
                    Arc::new(UInt32Array::from(vec![0])),
                ],
            )
            .unwrap(),
        ));
        let error = join.process(batch).await.unwrap_err();
        assert!(
            error.to_string().contains("schema changed mid-stream"),
            "{error}"
        );
    }

    // ---------- right-side / pending unmatched coverage ----------

    #[tokio::test]
    async fn right_side_capacity_eviction_is_observable_on_inner_join() {
        // Inner join drops capacity-evicted rows; the throttled eviction log
        // must still observe them (right-side index of the throttle).
        let mut cfg = config();
        cfg.max_per_key = 1;
        let join = JoinOperator::new(cfg).unwrap();
        // Three rows for one key evict twice: the first eviction logs, the
        // second is suppressed inside the throttle interval.
        assert!(matches!(
            join.process(side_batch(1, &["a", "a", "a"], &[100, 110, 120]))
                .await
                .unwrap(),
            ProcessResult::None
        ));
        // The surviving newest row still matches.
        let output = join.process(side_batch(0, &["a"], &[120])).await.unwrap();
        assert!(matches!(output, ProcessResult::Single(_)));
    }

    #[tokio::test]
    async fn right_pending_unmatched_parks_reparks_and_flushes() {
        // RightOuter: right rows evicted while the LEFT schema is unknown park
        // in the bounded pending queue (dropping the oldest on overflow), stay
        // parked across further watermarks, and flush once a left batch
        // establishes the schema.
        let mut cfg = config();
        cfg.join_type = JoinType::RightOuter;
        cfg.max_per_key = 1;
        let join = JoinOperator::new(cfg).unwrap();
        join.process(side_batch(1, &["a", "b"], &[100, 200]))
            .await
            .unwrap();
        // Both right rows evict with no left schema: "a" overflows the
        // bounded pending queue and drops.
        assert!(matches!(
            join.on_watermark(6_000).await.unwrap(),
            ProcessResult::None
        ));
        // A further watermark still has no left schema: the parked row
        // re-parks instead of emitting.
        assert!(matches!(
            join.on_watermark(7_000).await.unwrap(),
            ProcessResult::None
        ));
        // The first left batch establishes the schema; the parked right row
        // flushes as this call's unmatched emission.
        let output = join.process(side_batch(0, &["c"], &[100])).await.unwrap();
        let ProcessResult::Single(batch) = output else {
            panic!("expected the parked right unmatched emission to flush");
        };
        assert_eq!(string_column(&batch, "r_key").value(0), "b");
        assert!(string_column(&batch, "l_key").is_null(0));
    }

    #[tokio::test]
    async fn capacity_unmatched_parks_before_opposite_schema_and_flushes_with_match() {
        // LeftOuter with a capacity eviction BEFORE any right batch: the
        // unmatched row parks (no right schema yet). The next right batch
        // flushes the parked emission AND emits a fresh matched pair — two
        // outputs in one process call.
        let mut cfg = config();
        cfg.join_type = JoinType::LeftOuter;
        cfg.max_per_key = 1;
        let join = JoinOperator::new(cfg).unwrap();
        join.process(side_batch(0, &["a"], &[100])).await.unwrap();
        // Pushing a second "a" evicts the first; with no right schema the
        // unmatched row parks and the call emits nothing.
        assert!(matches!(
            join.process(side_batch(0, &["a"], &[200])).await.unwrap(),
            ProcessResult::None
        ));
        let output = join.process(side_batch(1, &["a"], &[200])).await.unwrap();
        let ProcessResult::Multiple(batches) = output else {
            panic!("expected a parked flush plus a matched pair");
        };
        assert_eq!(batches.len(), 2);
        let has_unmatched = batches.iter().any(|batch| {
            string_column(batch, "l_key").value(0) == "a"
                && string_column(batch, "r_key").is_null(0)
        });
        let has_pair = batches
            .iter()
            .any(|batch| !string_column(batch, "r_key").is_null(0));
        assert!(has_unmatched, "parked unmatched emission missing");
        assert!(has_pair, "matched pair missing");
    }
}
