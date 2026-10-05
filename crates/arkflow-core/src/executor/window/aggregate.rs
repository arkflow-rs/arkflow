//! Window aggregate state: the numeric kind tags, the typed aggregate
//! buffer with its observation kernels, the legacy pre-typed state format,
//! and the buffer encode/decode helpers used by persistence.

use crate::Error;
use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, UInt64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::ipc::reader::StreamReader;
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::sync::Arc;

/// Numeric representation of a window aggregate. The kind is fixed by the
/// value column's Arrow type; sums/min/max keep that type through state
/// serialization and the emitted schema instead of collapsing into integer
/// sentinels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub(crate) enum NumericKind {
    #[default]
    Int64,
    Float32,
    Float64,
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

    /// The wider of two aggregate kinds (Int64 < Float32 < Float64), matching
    /// the merge rule in `AggregateBuffer::merge`. A fired batch builds its
    /// output with the widest kind present so integer aggregates widen instead
    /// of truncating float sums back into integers.
    pub fn wider(self, other: Self) -> Self {
        match (self, other) {
            (Self::Float64, _) | (_, Self::Float64) => Self::Float64,
            (Self::Float32, _) | (_, Self::Float32) => Self::Float32,
            _ => Self::Int64,
        }
    }
}

/// Serialized aggregate for one (window, key) pair. Kept as a compact JSON
/// envelope so state stays backend-agnostic; the numeric kind travels with
/// the payload so a restored aggregate emits its original type.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct AggregateBuffer {
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
    /// Number of integer observations in this buffer. A buffer that observed
    /// both integer and float values (per-batch JSON schema inference makes
    /// this routine) keeps both contributions; the fired aggregate folds them
    /// into the wider kind instead of silently dropping one side.
    #[serde(default)]
    pub int_observations: u64,
    /// Number of float observations in this buffer.
    #[serde(default)]
    pub float_observations: u64,
    /// Serialized input batches retained for legacy buffer compatibility. The
    /// old tumbling/session buffers emitted the original rows and schema, not
    /// aggregate metadata, so the unified operator keeps that payload beside
    /// its timing/ack state.
    #[serde(default)]
    pub legacy_batches: Vec<Vec<u8>>,
}

impl AggregateBuffer {
    pub fn merge(&mut self, other: &AggregateBuffer) {
        let self_emitted = self.emitted;
        let self_updated = self.updated_since_emit;
        let other_emitted = other.emitted;
        self.count += other.count;
        self.sum_i64 = self.sum_i64.wrapping_add(other.sum_i64);
        self.sum_float += other.sum_float;
        // Combine each representation only over the sides that actually
        // observed values: an all-float buffer's untouched integer min/max
        // (and vice versa) must not fabricate a boundary for the merged
        // aggregate.
        if other.int_observations > 0 {
            if self.int_observations > 0 {
                self.min_i64 = self.min_i64.min(other.min_i64);
                self.max_i64 = self.max_i64.max(other.max_i64);
            } else {
                self.min_i64 = other.min_i64;
                self.max_i64 = other.max_i64;
            }
        }
        if other.float_observations > 0 {
            if self.float_observations > 0 {
                self.min_float = self.min_float.min(other.min_float);
                self.max_float = self.max_float.max(other.max_float);
            } else {
                self.min_float = other.min_float;
                self.max_float = other.max_float;
            }
        }
        self.int_observations = self.int_observations.saturating_add(other.int_observations);
        self.float_observations = self
            .float_observations
            .saturating_add(other.float_observations);
        self.kind = match (self.kind, other.kind) {
            // A merged buffer keeps the wider of the two kinds.
            (NumericKind::Float64, _) | (_, NumericKind::Float64) => NumericKind::Float64,
            (NumericKind::Float32, _) | (_, NumericKind::Float32) => NumericKind::Float32,
            (NumericKind::Int64, NumericKind::Int64) => NumericKind::Int64,
        };
        self.session_end_ms = self.session_end_ms.max(other.session_end_ms);
        self.legacy_batches
            .extend(other.legacy_batches.iter().cloned());
        // A retained session may be merged with another retained/emitted
        // session. Preserve the correction state across the re-key; otherwise
        // the merged buffer is emitted as a fresh initial result and the
        // already published aggregate is duplicated.
        self.emitted = self_emitted || other_emitted;
        self.updated_since_emit = self_updated
            || other.updated_since_emit
            || (other.count > 0 && self_emitted)
            || (self.count > 0 && other_emitted && !self_emitted);
        debug_assert!(
            self.count == self.int_observations + self.float_observations,
            "window aggregate observation counters drifted from count"
        );
    }

    pub fn observe_i64(&mut self, value: i64) {
        // Seed from the first INTEGER observation. `count` counts both
        // representations, so an integer arriving after a float would take the
        // extend branch against the field's zero default and fabricate a
        // boundary; the integer counter is the right guard, and `decode_buffer`
        // makes it consistent for restored state that predates the counters.
        if self.int_observations == 0 {
            self.min_i64 = value;
            self.max_i64 = value;
        } else {
            self.min_i64 = self.min_i64.min(value);
            self.max_i64 = self.max_i64.max(value);
        }
        self.sum_i64 = self.sum_i64.wrapping_add(value);
        self.int_observations = self.int_observations.saturating_add(1);
        self.count += 1;
        self.updated_since_emit = self.emitted;
        debug_assert!(
            self.count == self.int_observations + self.float_observations,
            "window aggregate observation counters drifted from count"
        );
    }

    /// Observe a floating value of the given kind. The internal
    /// representation is f64; the emitted schema narrows back to Float32 for
    /// Float32 aggregates. Float min/max seed from the first FLOAT value —
    /// integer observations share neither representation nor sentinel.
    pub fn observe_float(&mut self, value: f64, kind: NumericKind) {
        if self.float_observations == 0 {
            self.min_float = value;
            self.max_float = value;
        } else {
            self.min_float = self.min_float.min(value);
            self.max_float = self.max_float.max(value);
        }
        self.sum_float += value;
        self.float_observations = self.float_observations.saturating_add(1);
        self.kind = match (self.kind, kind) {
            (NumericKind::Float64, _) | (_, NumericKind::Float64) => NumericKind::Float64,
            (NumericKind::Float32, _) | (_, NumericKind::Float32) => NumericKind::Float32,
            (NumericKind::Int64, NumericKind::Int64) => NumericKind::Int64,
        };
        self.count += 1;
        self.updated_since_emit = self.emitted;
        debug_assert!(
            self.count == self.int_observations + self.float_observations,
            "window aggregate observation counters drifted from count"
        );
    }

    /// The sum over both representations, folded into the buffer's widened
    /// float representation. Integer contributions survive a kind widening
    /// instead of being dropped because only the float side was read.
    pub(crate) fn widened_sum(&self) -> f64 {
        self.sum_float + self.sum_i64 as f64
    }

    /// The minimum over both representations. Each side participates only if
    /// it actually observed a value; the untouched side's default field would
    /// fabricate a boundary (e.g. `0.0` or `i64::MIN`).
    pub(crate) fn widened_min(&self) -> f64 {
        match (self.int_observations > 0, self.float_observations > 0) {
            (true, true) => (self.min_i64 as f64).min(self.min_float),
            (true, false) => self.min_i64 as f64,
            (false, _) => self.min_float,
        }
    }

    /// The maximum over both representations (see [`Self::widened_min`]).
    pub(crate) fn widened_max(&self) -> f64 {
        match (self.int_observations > 0, self.float_observations > 0) {
            (true, true) => (self.max_i64 as f64).max(self.max_float),
            (true, false) => self.max_i64 as f64,
            (false, _) => self.max_float,
        }
    }

    /// Make the observation counters consistent with the accumulated state for
    /// a buffer that did not come from `observe_*`: state written before the
    /// counters existed decodes with `count > 0` and zeroed counters, and the
    /// legacy migration builds its buffer by hand.
    ///
    /// Counters the payload already names are kept, and each side's remaining
    /// observations are attributed from the state it accumulated: both integer
    /// bounds default to zero and any integer observation moves at least one of
    /// them, so a non-zero bound proves the integer side observed something.
    /// (A float sum would be the obvious test, but `[-1.0, 1.0]` is a non-empty
    /// float contribution whose sum is exactly zero.)
    ///
    /// A payload that observed BOTH representations cannot be split exactly
    /// when neither count survived — the per-side counts were not recorded — so
    /// each evidenced side keeps at least one observation. That keeps the
    /// widened `sum`/`min`/`max` honest (both sides contribute) at the cost of
    /// an approximate count split that only this unrecoverable input can see.
    fn normalize_observation_counters(&mut self) {
        if self.count == 0 {
            self.int_observations = 0;
            self.float_observations = 0;
            return;
        }
        if matches!(self.kind, NumericKind::Int64) {
            self.int_observations = self.count;
            self.float_observations = 0;
            return;
        }
        let int_evidenced = self.int_observations > 0 || self.min_i64 != 0 || self.max_i64 != 0;
        if !int_evidenced {
            // A float-kind payload with no integer evidence: every observation
            // came from the float side.
            self.int_observations = 0;
            self.float_observations = self.count;
            return;
        }
        let float_evidenced =
            self.float_observations > 0 || self.min_float != 0.0 || self.max_float != 0.0;
        let named = self
            .int_observations
            .saturating_add(self.float_observations);
        let unnamed = self.count.saturating_sub(named);
        if self.float_observations > 0 {
            // The payload named its own float observations; the remainder is
            // integer-side state written before the counters existed.
            self.int_observations = self.int_observations.saturating_add(unnamed);
        } else if float_evidenced && self.count > 1 {
            // Both sides accumulated values but neither count survived: reserve
            // one observation for the integer side so its bounds keep
            // contributing.
            self.int_observations = 1;
            self.float_observations = self.count.saturating_sub(1);
        } else {
            self.int_observations = self.count;
            self.float_observations = 0;
        }
        if self
            .int_observations
            .saturating_add(self.float_observations)
            != self.count
        {
            self.int_observations = self.count;
            self.float_observations = 0;
        }
    }
}

/// One typed aggregate output value.
#[derive(Debug, Clone, Copy)]
pub(crate) enum NumericValue {
    Int(i64),
    Float(f64, NumericKind),
}

impl NumericValue {
    pub(crate) fn kind(&self) -> NumericKind {
        match self {
            Self::Int(_) => NumericKind::Int64,
            Self::Float(_, kind) => *kind,
        }
    }
}

/// Build a typed Arrow column from values, widening integers to the batch's
/// float kind when a window aggregate carries both (schema stays uniform).
pub(crate) fn numeric_array(values: &[NumericValue], kind: NumericKind) -> Result<ArrayRef, Error> {
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

/// Encode one aggregate buffer as a one-row Arrow IPC stream. Keeping the
/// state payload columnar makes snapshots backend-neutral and avoids coupling
/// the window state format to JSON field ordering or number representations.
pub(crate) fn encode_buffer(buffer: &AggregateBuffer) -> Result<Vec<u8>, Error> {
    // V2 payload: typed JSON envelope carrying the numeric kind, float
    // min/max, and the emitted/update-retention flags. Keeping the payload
    // JSON makes the state backend-neutral; the old IPC format remains
    // readable below.
    serde_json::to_vec(buffer)
        .map_err(|error| Error::Process(format!("encode window aggregate state: {error}")))
}

/// A window aggregate written by the pre-typed state format. The legacy float
/// sum is deliberately absent: the writer that produced this envelope set
/// `is_float` together with a non-zero `sum_float`, and a payload with
/// `is_float && count > 0` is rejected by `migrate`, so a migratable payload
/// carries no float contribution to preserve.
#[derive(serde::Deserialize)]
pub(crate) struct LegacyAggregateBuffer {
    count: u64,
    #[serde(default)]
    sum_i64: i64,
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
        // A legacy payload that observed nothing carries `i64::MIN`/`i64::MAX`
        // as min/max placeholders. They must not survive the migration: a later
        // merge folds this buffer's bounds into the wider aggregate and would
        // fabricate a boundary no row ever produced. The observation counters
        // follow the integer payload the migration reconstructs, so a later
        // merge keeps this buffer's contribution instead of dropping it.
        if self.count == 0 {
            return Ok(AggregateBuffer::default());
        }
        Ok(AggregateBuffer {
            count: self.count,
            kind: NumericKind::Int64,
            sum_i64: self.sum_i64,
            min_i64: self.min_i64,
            max_i64: self.max_i64,
            int_observations: self.count,
            session_end_ms: self.session_end_ms,
            ..Default::default()
        })
    }
}

/// Decode one persisted aggregate buffer and make its observation counters
/// consistent with the state it carries. State written before the counters
/// existed decodes with `count > 0` and zeroed counters; leaving it that way
/// would re-seed a min/max from the next single observation and discard the
/// restored range.
pub(crate) fn decode_buffer(bytes: &[u8]) -> Result<AggregateBuffer, Error> {
    let mut buffer = decode_buffer_unchecked(bytes)?;
    if buffer.count != buffer.int_observations + buffer.float_observations {
        buffer.normalize_observation_counters();
    }
    Ok(buffer)
}

pub(crate) fn decode_buffer_unchecked(bytes: &[u8]) -> Result<AggregateBuffer, Error> {
    // A real legacy payload written by the pre-typed kernel parses
    // successfully as a V2 `AggregateBuffer` (it carried every field the
    // typed struct requires and `is_float` is an ignored unknown field), so
    // peeking for the flag BEFORE the typed parse is the only way to route
    // it to the migration guard: a legacy FLOAT aggregate stores min/max as
    // integer sentinels and would otherwise restore as a corrupted Int64
    // aggregate.
    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) {
        if value.get("is_float").is_some_and(|flag| flag.is_boolean()) {
            let legacy =
                serde_json::from_value::<LegacyAggregateBuffer>(value).map_err(|error| {
                    Error::Process(format!("decode legacy window aggregate state: {error}"))
                })?;
            return legacy.migrate();
        }
    }
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
