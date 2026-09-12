//! Columnar window operator: vectorized tumbling window assignment with
//! keyed aggregation state, watermark and processing-time triggers.
//!
//! Assignment is O(1) full-column computations over the timestamp column
//! (`window_start = ts.div_euclid(size) * size`); rows are grouped per batch
//! and merged into per-(window, key) aggregate buffers held in the state
//! backend. Watermarks (or the processing-time trigger) fire windows whose
//! end has passed, emitting the aggregate batch downstream.

use crate::input::{fanout_ack, Ack, ConcurrentAck};
use crate::job::LateEventPolicy;
use crate::processor::Processor;
use crate::state::StateBackend;
use crate::Error;
use crate::MessageBatchRef;
use crate::ProcessResult;
use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, StringArray, UInt64Array,
};
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
    fn widened_sum(&self) -> f64 {
        self.sum_float + self.sum_i64 as f64
    }

    /// The minimum over both representations. Each side participates only if
    /// it actually observed a value; the untouched side's default field would
    /// fabricate a boundary (e.g. `0.0` or `i64::MIN`).
    fn widened_min(&self) -> f64 {
        match (self.int_observations > 0, self.float_observations > 0) {
            (true, true) => (self.min_i64 as f64).min(self.min_float),
            (true, false) => self.min_i64 as f64,
            (false, _) => self.min_float,
        }
    }

    /// The maximum over both representations (see [`Self::widened_min`]).
    fn widened_max(&self) -> f64 {
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
        let int_evidenced =
            self.int_observations > 0 || self.min_i64 != 0 || self.max_i64 != 0;
        if !int_evidenced {
            // A float-kind payload with no integer evidence: every observation
            // came from the float side.
            self.int_observations = 0;
            self.float_observations = self.count;
            return;
        }
        let float_evidenced =
            self.float_observations > 0 || self.min_float != 0.0 || self.max_float != 0.0;
        let named = self.int_observations.saturating_add(self.float_observations);
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
        if self.int_observations.saturating_add(self.float_observations) != self.count {
            self.int_observations = self.count;
            self.float_observations = 0;
        }
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
    /// Preserve the old Buffer contract: emit concatenated input rows and
    /// schema rather than the columnar aggregate metadata.
    #[serde(default)]
    pub legacy_payload: bool,
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
        if self.value_fields.len() > 1 {
            // The operator aggregates a single value column and emits one
            // sum/min/max trio; silently dropping the extra fields would emit
            // wrong counts with no error.
            return Err(Error::Config(format!(
                "window aggregate supports exactly one value field, got {}",
                self.value_fields.len()
            )));
        }
        if self.legacy_payload && matches!(self.kind, WindowKind::Sliding { .. }) {
            // The stream compiler rejects this combination for stream configs;
            // a Job spec bypasses the compiler, and `windows_for` would panic.
            return Err(Error::Config(
                "legacy_payload is not supported for sliding windows".into(),
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
                // Every event joins each aligned window that contains it: an
                // extreme size/slide ratio would enumerate (and buffer) that
                // many memberships per event. Cap the fan-out so a validated
                // config cannot stall the task or exhaust memory.
                if size_ms / slide_ms > MAX_SLIDING_MEMBERSHIPS_PER_EVENT {
                    return Err(Error::Config(format!(
                        "sliding window size_ms/slide_ms would assign each event to more than \
                         {MAX_SLIDING_MEMBERSHIPS_PER_EVENT} windows; enlarge slide_ms or shrink size_ms"
                    )));
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

/// Upper bound on the memberships one event can join in a sliding window
/// (enforced by `WindowOperatorConfig::validate`).
const MAX_SLIDING_MEMBERSHIPS_PER_EVENT: i64 = 10_000;

/// The columnar window operator. One instance per stateful operator task;
/// state is namespaced under the operator id so parallel subtasks stay
/// isolated.
pub struct ColumnarWindowOperator {
    config: WindowOperatorConfig,
    /// Event-time session lateness is evaluated here because the source gate
    /// cannot know a session's key-dependent, dynamically extended end.  The
    /// graph supplies the source policy; direct callers keep the safe Drop
    /// default.
    late_event_policy: LateEventPolicy,
    late_event_route_configured: bool,
    /// Session-late/invalid rows counted by this operator. The source gate
    /// cannot see session lateness (it is key- and state-dependent), so the
    /// operator counts its own masks here and the chain loop surfaces the
    /// counter into the kernel's `late_events` metric.
    late_event_rows: Arc<std::sync::atomic::AtomicU64>,
    backend: Arc<dyn StateBackend>,
    namespace: String,
    /// Output-gated state commits: buffer mutations stage in the journal and
    /// apply only when the fired window's output acknowledgement succeeds.
    /// `None` keeps the legacy direct-persist behavior.
    journal: Option<Arc<super::state_journal::StateJournal>>,
    /// One journal transaction per open `(window_start, key)` group; a fired
    /// window's commit rides its emitted output's acknowledgement.
    window_txns: Arc<Mutex<BTreeMap<(i64, String), super::state_journal::StateTxn>>>,
    /// (window_start, key) -> buffer, mirroring the backend lazily.
    buffers: Arc<Mutex<BTreeMap<(i64, String), AggregateBuffer>>>,
    /// Source acknowledgements held until the corresponding aggregate is
    /// successfully written downstream.
    pending_acks: Arc<Mutex<BTreeMap<(i64, String), Vec<Arc<dyn Ack>>>>>,
    watermark_ms: Arc<Mutex<Option<i64>>>,
    last_processing_trigger_ms: Arc<Mutex<Option<i64>>>,
    last_processing_activity_ms: Arc<Mutex<Option<i64>>>,
    loaded: Arc<Mutex<bool>>,
    /// Serialize a window operation through the output acknowledgement. A
    /// fired result changes both the in-memory buffer and the journal; a
    /// second input must not mutate the same window until the first result's
    /// source acknowledgement has either committed or been compensated.
    operation_lock: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Clone)]
struct WindowRuntimeSnapshot {
    buffers: BTreeMap<(i64, String), AggregateBuffer>,
    watermark_ms: Option<i64>,
    last_processing_trigger_ms: Option<i64>,
    last_processing_activity_ms: Option<i64>,
}

struct WindowRollback {
    buffers: Arc<Mutex<BTreeMap<(i64, String), AggregateBuffer>>>,
    watermark_ms: Arc<Mutex<Option<i64>>>,
    last_processing_trigger_ms: Arc<Mutex<Option<i64>>>,
    last_processing_activity_ms: Arc<Mutex<Option<i64>>>,
    operation_lock: Arc<tokio::sync::Mutex<()>>,
    before: WindowRuntimeSnapshot,
    after: WindowRuntimeSnapshot,
}

impl WindowRollback {
    fn restore(&self, snapshot: &WindowRuntimeSnapshot) {
        *self.buffers.lock().unwrap() = snapshot.buffers.clone();
        *self.watermark_ms.lock().unwrap() = snapshot.watermark_ms;
        *self.last_processing_trigger_ms.lock().unwrap() = snapshot.last_processing_trigger_ms;
        *self.last_processing_activity_ms.lock().unwrap() = snapshot.last_processing_activity_ms;
    }
}

/// Holds the window operation lock until its emitted output and all source
/// acknowledgements finish. If that acknowledgement fails, the journal rolls
/// back the durable mutation and this wrapper restores the working buffers and
/// watermark, so a replay cannot accumulate the same row twice in memory.
struct WindowFiredAck {
    inner: Arc<dyn Ack>,
    rollback: Arc<WindowRollback>,
    operation_guard: Mutex<Option<tokio::sync::OwnedMutexGuard<()>>>,
}

impl WindowFiredAck {
    async fn take_guard(&self) -> tokio::sync::OwnedMutexGuard<()> {
        let existing = { self.operation_guard.lock().unwrap().take() };
        if let Some(guard) = existing {
            guard
        } else {
            self.rollback.operation_lock.clone().lock_owned().await
        }
    }

    fn retain_guard(&self, guard: tokio::sync::OwnedMutexGuard<()>) {
        *self.operation_guard.lock().unwrap() = Some(guard);
    }
}

#[async_trait]
impl Ack for WindowFiredAck {
    async fn ack(&self) -> Result<(), Error> {
        self.inner.release_held();
        let guard = self.take_guard().await;
        match self.inner.ack().await {
            Ok(()) => {
                self.rollback.restore(&self.rollback.after);
                drop(guard);
                Ok(())
            }
            Err(error) => {
                self.rollback.restore(&self.rollback.before);
                self.retain_guard(guard);
                Err(error)
            }
        }
    }

    async fn undo(&self) -> Result<(), Error> {
        self.inner.release_held();
        let guard = self.take_guard().await;
        let result = self.inner.undo().await;
        // The wrapped source and journal compensation are best-effort
        // independent steps.  `CommitOnAck`/`CommitGroupOnAck` restore the
        // durable state even when the source-side undo reports an error; the
        // in-memory window must follow the same rollback boundary regardless
        // of which error is returned.
        self.rollback.restore(&self.rollback.before);
        drop(guard);
        result
    }

    async fn abort(&self) -> Result<(), Error> {
        self.inner.release_held();
        let guard = self.take_guard().await;
        let result = self.inner.abort().await;
        self.rollback.restore(&self.rollback.before);
        drop(guard);
        result
    }

    fn mark_held(&self) {
        self.inner.mark_held();
    }

    fn release_held(&self) {
        self.inner.release_held();
    }
}

impl ColumnarWindowOperator {
    fn runtime_snapshot(&self) -> WindowRuntimeSnapshot {
        WindowRuntimeSnapshot {
            buffers: self.buffers.lock().unwrap().clone(),
            watermark_ms: *self.watermark_ms.lock().unwrap(),
            last_processing_trigger_ms: *self.last_processing_trigger_ms.lock().unwrap(),
            last_processing_activity_ms: *self.last_processing_activity_ms.lock().unwrap(),
        }
    }

    /// Roll the in-memory runtime back to `snapshot` after a failed
    /// accumulation or firing. Journal transactions stay staged — their
    /// overlays are idempotent across a retry — but the working buffers and
    /// watermark must not keep partially applied rows, or a replay would
    /// aggregate them twice.
    fn restore_runtime(&self, snapshot: &WindowRuntimeSnapshot) {
        *self.buffers.lock().unwrap() = snapshot.buffers.clone();
        *self.watermark_ms.lock().unwrap() = snapshot.watermark_ms;
        *self.last_processing_trigger_ms.lock().unwrap() = snapshot.last_processing_trigger_ms;
        *self.last_processing_activity_ms.lock().unwrap() = snapshot.last_processing_activity_ms;
    }

    fn rollback_state(
        &self,
        before: WindowRuntimeSnapshot,
        after: WindowRuntimeSnapshot,
    ) -> Arc<WindowRollback> {
        Arc::new(WindowRollback {
            buffers: self.buffers.clone(),
            watermark_ms: self.watermark_ms.clone(),
            last_processing_trigger_ms: self.last_processing_trigger_ms.clone(),
            last_processing_activity_ms: self.last_processing_activity_ms.clone(),
            operation_lock: self.operation_lock.clone(),
            before,
            after,
        })
    }

    pub fn new(
        config: WindowOperatorConfig,
        backend: Arc<dyn StateBackend>,
        namespace: impl Into<String>,
    ) -> Self {
        Self::build(
            config,
            backend,
            namespace,
            LateEventPolicy::Drop,
            false,
            None,
        )
    }

    /// Build a window with the upstream Job time policy.  Session windows use
    /// this policy in the operator, after their dynamic per-key boundary is
    /// known, rather than guessing a static boundary in the source gate.
    pub fn with_late_event_policy(
        config: WindowOperatorConfig,
        backend: Arc<dyn StateBackend>,
        namespace: impl Into<String>,
        late_event_policy: LateEventPolicy,
        late_event_route_configured: bool,
    ) -> Self {
        Self::build(
            config,
            backend,
            namespace,
            late_event_policy,
            late_event_route_configured,
            None,
        )
    }

    fn build(
        config: WindowOperatorConfig,
        backend: Arc<dyn StateBackend>,
        namespace: impl Into<String>,
        late_event_policy: LateEventPolicy,
        late_event_route_configured: bool,
        journal: Option<Arc<super::state_journal::StateJournal>>,
    ) -> Self {
        Self {
            config,
            late_event_policy,
            late_event_route_configured,
            late_event_rows: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            backend,
            namespace: namespace.into(),
            journal,
            window_txns: Arc::new(Mutex::new(BTreeMap::new())),
            buffers: Arc::new(Mutex::new(BTreeMap::new())),
            pending_acks: Arc::new(Mutex::new(BTreeMap::new())),
            watermark_ms: Arc::new(Mutex::new(None)),
            last_processing_trigger_ms: Arc::new(Mutex::new(None)),
            last_processing_activity_ms: Arc::new(Mutex::new(None)),
            loaded: Arc::new(Mutex::new(false)),
            operation_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// Build the operator with output-gated state commits.
    pub fn with_journal(
        config: WindowOperatorConfig,
        backend: Arc<dyn StateBackend>,
        journal: Arc<super::state_journal::StateJournal>,
        namespace: impl Into<String>,
    ) -> Self {
        Self::build(
            config,
            backend,
            namespace,
            LateEventPolicy::Drop,
            false,
            Some(journal),
        )
    }

    /// Build a journaled window with the upstream Job time policy.
    pub fn with_journal_and_late_event_policy(
        config: WindowOperatorConfig,
        backend: Arc<dyn StateBackend>,
        journal: Arc<super::state_journal::StateJournal>,
        namespace: impl Into<String>,
        late_event_policy: LateEventPolicy,
        late_event_route_configured: bool,
    ) -> Self {
        Self::build(
            config,
            backend,
            namespace,
            late_event_policy,
            late_event_route_configured,
            Some(journal),
        )
    }

    /// The counter of session-late/invalid rows this operator has classified.
    /// The chain loop surfaces its delta into the kernel `late_events` metric.
    pub fn late_event_row_counter(&self) -> Arc<std::sync::atomic::AtomicU64> {
        Arc::clone(&self.late_event_rows)
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
    /// sliding yields every containing window; session yields
    /// its gap-extended window (tracked per key in the buffer map).
    fn windows_for(&self, event_time_ms: i64) -> Vec<(i64, i64)> {
        if self.config.legacy_payload {
            return match self.config.kind {
                WindowKind::Tumbling { size_ms } => vec![(0, size_ms)],
                WindowKind::Session { gap_ms } => vec![(0, gap_ms)],
                WindowKind::Sliding { .. } => unreachable!(
                    "legacy row-count sliding windows are rejected by the stream compiler"
                ),
            };
        }
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

    /// Split event-time session rows whose dynamic session has already passed
    /// its allowed-lateness deadline.  Session timing is intentionally not
    /// represented in [`WindowTiming`]: only the window operator knows the
    /// current per-key session end, including rows that extended or bridged
    /// an emitted session.
    ///
    /// The first mask is the normal window input.  The second mask contains
    /// rows that must be dropped or sent to the configured late side output.
    /// Rows marked late are never accumulated into a new partial session after
    /// the original session has expired.
    fn session_late_masks(
        &self,
        batch: &crate::MessageBatchRef,
    ) -> Result<(Vec<bool>, Vec<bool>, Vec<bool>), Error> {
        let mut keep = vec![true; batch.len()];
        let mut late = vec![false; batch.len()];
        let mut invalid = vec![false; batch.len()];
        if self.config.legacy_payload
            || self.config.trigger != WindowTrigger::Watermark
            || !matches!(self.config.kind, WindowKind::Session { .. })
        {
            return Ok((keep, late, invalid));
        }
        let Some(watermark) = *self.watermark_ms.lock().unwrap() else {
            return Ok((keep, late, invalid));
        };
        let WindowKind::Session { gap_ms } = self.config.kind else {
            unreachable!();
        };
        let timestamps = self.extract_timestamps(batch)?;
        let keys = self.extract_keys(batch)?;
        let buffers = self.buffers.lock().unwrap();
        for row in 0..batch.len() {
            let (Some(event_time), Some(key)) = (timestamps[row], keys[row].as_ref()) else {
                // The source gate normally handles null timestamps. Keep the
                // operator safe for direct callers as well: an invalid row
                // cannot ever reach a session deadline.
                keep[row] = false;
                late[row] = true;
                invalid[row] = true;
                continue;
            };
            let matching_end = buffers
                .iter()
                .filter(|((start, window_key), buffer)| {
                    window_key == key
                        && event_time.saturating_add(gap_ms) >= *start
                        && event_time <= Self::session_end(*start, buffer, gap_ms)
                })
                .map(|((start, _), buffer)| Self::session_end(*start, buffer, gap_ms))
                .max();
            let session_end = matching_end.unwrap_or_else(|| event_time.saturating_add(gap_ms));
            if session_end <= watermark {
                // An Update is meaningful only while a matching aggregate is
                // retained. If the session has already been removed there is
                // no state to correct, so treat it as an expired late row.
                let within_lateness =
                    watermark <= session_end.saturating_add(self.config.allowed_lateness_ms as i64);
                let update_existing = matching_end.is_some()
                    && within_lateness
                    && self.late_event_policy == LateEventPolicy::Update;
                let route = self.late_event_policy == LateEventPolicy::Route
                    && self.late_event_route_configured;
                if !update_existing && !route {
                    keep[row] = false;
                    late[row] = true;
                } else if route {
                    keep[row] = false;
                    late[row] = true;
                }
            }
        }
        Ok((keep, late, invalid))
    }

    /// Merge one batch into the aggregate buffers (vectorized assignment).
    fn accumulate(&self, batch: &crate::MessageBatchRef) -> Result<Vec<(i64, String)>, Error> {
        let timestamps = self.extract_timestamps(batch)?;
        let keys = self.extract_keys(batch)?;
        let value_columns: Vec<&ArrayRef> = self
            .config
            .value_fields
            .iter()
            .map(|field| {
                batch.record_batch().column_by_name(field).ok_or_else(|| {
                    Error::Process(format!("window value field '{}' is missing", field))
                })
            })
            .collect::<Result<_, _>>()?;
        let late_update_flags = batch
            .record_batch()
            .column_by_name("__arkflow_late_event_update")
            .and_then(|column| column.as_any().downcast_ref::<BooleanArray>());
        let excluded_window_ends = batch
            .record_batch()
            .column_by_name("__arkflow_late_window_ends")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>());
        let late_update_window_ends = batch
            .record_batch()
            .column_by_name("__arkflow_late_window_updates")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>());
        let mut buffers = self.buffers.lock().unwrap();
        // The operator's watermark frontier can advance ahead of the source
        // gate's classification view (batch-embedded watermark columns,
        // shared-tracker forwarding from another source edge, concurrent
        // held-row release). Read it once per batch and use it as the
        // admission guard below.
        let current_watermark = *self.watermark_ms.lock().unwrap();
        let mut touched = BTreeSet::new();
        let mut session_rekeys = Vec::new();
        let mut legacy_rows = BTreeMap::<(i64, String), Vec<usize>>::new();
        for row in 0..batch.len() {
            let (Some(event_time), Some(key)) = (&timestamps[row], &keys[row]) else {
                continue;
            };
            // Sliding windows contribute to every containing window;
            // tumbling and session contribute to their single window.
            let mut windows = self.windows_for(*event_time);
            let mut session_seed = None;
            // Legacy session buffers are processing-time batches. Their
            // compatibility window is deliberately one synthetic group, so
            // do not replace that group with event-time session matching.
            // Dynamic per-key session boundaries belong only to the unified
            // event-time session implementation.
            if !self.config.legacy_payload {
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
                        if let Some(journal) = &self.journal {
                            // A re-keyed session may already have a committed
                            // aggregate under one of the old starts. Defer those
                            // deletions into the merged session's transaction so
                            // a failed fired/source acknowledgement can replay
                            // the row without losing the old aggregate.
                            let merged_txn = self.window_txn(&merged_key)?;
                            for old_key in &matched_keys {
                                if old_key != &merged_key {
                                    journal.delete(
                                        merged_txn,
                                        &self.namespace,
                                        &Self::state_key(old_key.0, &old_key.1),
                                    )?;
                                }
                            }
                        }
                        session_rekeys.extend(
                            matched_keys
                                .into_iter()
                                .map(|old_key| (old_key, merged_key.clone())),
                        );
                        session_seed = Some(merged);
                    }
                    windows = vec![(merged_start, merged_end)];
                }
            }
            for (window_start, window_end) in windows {
                let excluded = excluded_window_ends
                    .and_then(|values| values.is_valid(row).then(|| values.value(row)))
                    .is_some_and(|values| {
                        values.split(',').any(|value| {
                            value
                                .parse::<i64>()
                                .map(|end| end == window_end)
                                .unwrap_or(false)
                        })
                    });
                if excluded {
                    continue;
                }
                // A membership whose window already closed behind this
                // operator's watermark frontier and whose buffer was already
                // fired and cleaned must not be re-opened as a fresh
                // aggregate: the next fire would duplicate an already
                // emitted window result. The gate marks known-late
                // memberships with exclusion markers; this guard covers the
                // release race where an unmarked row reaches the operator
                // after the frontier moved past its window. Session timing
                // is excluded: its dynamic per-key lateness is owned by
                // `session_late_masks`, and bridged sessions legitimately
                // re-key closed windows.
                if !self.config.legacy_payload
                    && !matches!(self.config.kind, WindowKind::Session { .. })
                    && !late_update_flags.is_some_and(|flags| flags.value(row))
                    && current_watermark.is_some_and(|watermark| window_end <= watermark)
                    && !buffers.contains_key(&(window_start, key.clone()))
                {
                    continue;
                }
                // A late Update corrects an already retained aggregate. Do
                // not create a new partial buffer for a window that has
                // already been cleaned up after its lateness deadline.
                if late_update_flags.is_some_and(|flags| flags.value(row))
                    && !buffers.contains_key(&(window_start, key.clone()))
                {
                    // A targeted late update may share a row with a still
                    // open sliding membership. Only the explicitly marked
                    // closed memberships require an existing aggregate; the
                    // unmarked memberships must be admitted normally.
                    let targeted = late_update_window_ends.is_some_and(|values| {
                        values.is_valid(row)
                            && values.value(row).split(',').any(|value| {
                                value
                                    .parse::<i64>()
                                    .map(|end| end == window_end)
                                    .unwrap_or(false)
                            })
                    });
                    if late_update_window_ends.is_none() || targeted {
                        continue;
                    }
                }
                touched.insert((window_start, key.clone()));
                if self.config.legacy_payload {
                    legacy_rows
                        .entry((window_start, key.clone()))
                        .or_default()
                        .push(row);
                }
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
        if self.config.legacy_payload && !touched.is_empty() {
            // The legacy buffer grouped complete input batches, but the
            // compatibility window may still be keyed. Retain only the rows
            // that belong to each logical group; otherwise one mixed batch
            // would be emitted once per key and duplicate unrelated rows.
            for (key, rows) in legacy_rows {
                let mut keep = vec![false; batch.len()];
                for row in rows {
                    keep[row] = true;
                }
                let filtered = datafusion::arrow::compute::filter_record_batch(
                    batch.record_batch(),
                    &BooleanArray::from(keep),
                )
                .map_err(|error| Error::Process(format!("slice legacy window batch: {error}")))?;
                let mut filtered_batch = crate::MessageBatch::new_arrow(filtered);
                filtered_batch.set_input_name(batch.get_input_name());
                let serialized = crate::wal::store::serialize(&filtered_batch)?;
                if let Some(buffer) = buffers.get_mut(&key) {
                    buffer.legacy_batches.push(serialized);
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
        let touched = touched.into_iter().collect::<Vec<_>>();
        // A journal transaction represents a dirty working buffer. Create it
        // at the mutation boundary so persistence does not manufacture a new
        // never-fire transaction for every already committed emitted window
        // on every unrelated batch.
        if self.journal.is_some() {
            for key in &touched {
                self.window_txn(key)?;
            }
        }
        Ok(touched)
    }

    /// Emit aggregates for windows whose end has passed the trigger
    /// threshold, persisting nothing (buffers are the working state; the
    /// barrier snapshot serializes them on demand).
    fn fire_ready(&self, threshold: i64) -> Result<WindowFiring, Error> {
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
        // Expired windows emit nothing, but their deliveries (a late Update
        // the operator deadline rejects) still hold pending acknowledgements.
        // Discard their staged transactions — the update they staged is
        // rejected — and report the keys so the caller settles the delivery
        // acknowledgements; leaving them out strands the acknowledgements in
        // `pending_acks` forever and freezes the checkpoint frontier.
        let mut dropped_keys = expired;
        for key in &dropped_keys {
            buffers.remove(key);
            self.rollback_window_txn(key);
        }
        let ready = buffers
            .iter_mut()
            .filter(|((start, _), buffer)| {
                let end = end_of(*start, buffer);
                end <= threshold && (!buffer.emitted || buffer.updated_since_emit)
            })
            .map(|((start, key), buffer)| ((*start, key.clone()), buffer.clone()))
            .collect::<Vec<_>>();
        let mut starts = Vec::new();
        let mut ends = Vec::new();
        let mut key_strings: Vec<String> = Vec::new();
        let mut counts = Vec::new();
        let mut sum_values: Vec<NumericValue> = Vec::new();
        let mut min_values: Vec<NumericValue> = Vec::new();
        let mut max_values: Vec<NumericValue> = Vec::new();
        let mut updates = Vec::new();
        let mut legacy_messages = Vec::new();
        let mut fired_keys = Vec::new();
        let journal = self.journal.clone();
        for ((start, key), buffer) in ready {
            // A buffer that never observed a value carries no aggregate: its
            // rows held only NULL values (or none at all). Drop it instead of
            // emitting a fabricated count=0/sum=0 sentinel row. Legacy
            // payloads keep the original-row emission contract regardless of
            // observations.
            if buffer.count == 0 && !self.config.legacy_payload && buffer.legacy_batches.is_empty()
            {
                // The delivery acknowledgements of an empty group are still
                // open even though nothing can be emitted. Discard its staged
                // (count-0) transaction and settle the delivery through the
                // dropped set, or the source frontier strands here forever.
                buffers.remove(&(start, key.clone()));
                self.rollback_window_txn(&(start, key.clone()));
                dropped_keys.push((start, key.clone()));
                continue;
            }
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
            if self.config.legacy_payload {
                // Legacy processing-time buffers are one-shot batches.  The
                // old Buffer implementation removed them after each flush;
                // retaining the emitted rows here would make the next tick
                // emit the previous batch again.  Event-time windows keep
                // their buffers for allowed-lateness updates, but the legacy
                // compatibility path has no such update contract.
                buffers.remove(&(start, key.clone()));
            }
            let is_update = buffer.emitted;
            fired_keys.push((start, key.clone()));
            starts.push(start);
            ends.push(end_of(start, &buffer));
            key_strings.push(key);
            counts.push(buffer.count);
            sum_values.push(match buffer.kind {
                NumericKind::Int64 => NumericValue::Int(buffer.sum_i64),
                other_kind => {
                    // A buffer that observed mixed Int64/Float64 values keeps
                    // both contributions; fold the integer side into the
                    // widened float aggregate instead of dropping it.
                    NumericValue::Float(buffer.widened_sum(), other_kind)
                }
            });
            min_values.push(match buffer.kind {
                NumericKind::Int64 => NumericValue::Int(buffer.min_i64),
                other_kind => NumericValue::Float(buffer.widened_min(), other_kind),
            });
            max_values.push(match buffer.kind {
                NumericKind::Int64 => NumericValue::Int(buffer.max_i64),
                other_kind => NumericValue::Float(buffer.widened_max(), other_kind),
            });
            updates.push(is_update);
            if self.config.legacy_payload {
                for payload in buffer.legacy_batches {
                    legacy_messages.push(Arc::new(crate::wal::store::deserialize(&payload)?));
                }
            }
        }
        drop(buffers);
        if fired_keys.is_empty() {
            // Only expired or empty groups fired this round: nothing to emit,
            // but the caller still settles their delivery acknowledgements.
            return Ok(WindowFiring {
                output: None,
                dropped_keys,
            });
        }
        if self.config.legacy_payload {
            if legacy_messages.is_empty() {
                return Err(Error::Process(
                    "legacy window payload is unavailable in the restored state".into(),
                ));
            }
            let schema = legacy_messages[0].schema();
            let batches = legacy_messages
                .iter()
                .map(|batch| batch.record_batch().clone())
                .collect::<Vec<_>>();
            let merged = datafusion::arrow::compute::concat_batches(&schema, &batches)
                .map_err(|error| Error::Process(format!("merge legacy window batches: {error}")))?;
            let mut merged = crate::MessageBatch::new_arrow(merged);
            merged.set_input_name(legacy_messages[0].get_input_name());
            return Ok(WindowFiring {
                output: Some((Arc::new(merged), fired_keys)),
                dropped_keys,
            });
        }
        // The batch's output kind is the WIDEST kind across the fired
        // buffers, not whichever buffer happens to come first: an Int64-kind
        // buffer firing next to Float64 aggregates must widen the integers,
        // never truncate the float sums back into integers.
        let kind = sum_values
            .iter()
            .map(NumericValue::kind)
            .fold(NumericKind::Int64, NumericKind::wider);
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
        Ok(WindowFiring {
            output: Some((Arc::new(crate::MessageBatch::new_arrow(batch)), fired_keys)),
            dropped_keys,
        })
    }
}

/// The outcome of one window firing round.
struct WindowFiring {
    /// Aggregate rows to emit together with the window keys they belong to.
    output: Option<(MessageBatchRef, Vec<(i64, String)>)>,
    /// Groups that emit nothing — expired windows and empty (never-observed)
    /// buffers — but whose staged transactions were discarded and whose
    /// delivery acknowledgements the caller must still settle.
    dropped_keys: Vec<(i64, String)>,
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

fn filter_window_batch(
    batch: &crate::MessageBatchRef,
    keep: &[bool],
) -> Result<crate::MessageBatchRef, Error> {
    if keep.len() != batch.len() {
        return Err(Error::Process(
            "session late-event filter length differs from batch".into(),
        ));
    }
    let filtered = datafusion::arrow::compute::filter_record_batch(
        batch.record_batch(),
        &BooleanArray::from(keep.to_vec()),
    )
    .map_err(|error| Error::Process(format!("slice session late-event batch: {error}")))?;
    let mut filtered_batch = crate::MessageBatch::new_arrow(filtered);
    filtered_batch.set_input_name(batch.get_input_name());
    Ok(Arc::new(filtered_batch))
}

/// Mark rows emitted by a session's late-event side path.  The task router
/// consumes this marker and sends the batch directly to the configured late
/// target, bypassing the normal window output edge.
fn mark_late_session_batch(
    batch: crate::MessageBatchRef,
    invalid_timestamps: &[bool],
) -> Result<crate::MessageBatchRef, Error> {
    use datafusion::arrow::array::BooleanArray;

    if invalid_timestamps.len() != batch.len() {
        return Err(Error::Process(
            "session late-event marker length differs from batch".into(),
        ));
    }
    let marker = "__arkflow_late_event_route";
    let invalid_marker = "__arkflow_invalid_timestamp_route";
    let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
    let mut columns = batch.columns().to_vec();
    let route_values = Arc::new(BooleanArray::from(vec![true; batch.len()])) as ArrayRef;
    let invalid_values = Arc::new(BooleanArray::from(invalid_timestamps.to_vec())) as ArrayRef;
    if let Some(index) = batch.schema().index_of(marker).ok() {
        columns[index] = route_values;
    } else {
        fields.push(Arc::new(Field::new(marker, DataType::Boolean, false)));
        columns.push(route_values);
    }
    if invalid_timestamps.iter().any(|invalid| *invalid) {
        if let Some(index) = batch.schema().index_of(invalid_marker).ok() {
            columns[index] = invalid_values;
        } else {
            fields.push(Arc::new(Field::new(
                invalid_marker,
                DataType::Boolean,
                false,
            )));
            columns.push(invalid_values);
        }
    }
    let marked = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|error| Error::Process(format!("mark session late event: {error}")))?;
    let mut marked = crate::MessageBatch::new_arrow(marked);
    marked.set_input_name(batch.get_input_name());
    Ok(Arc::new(marked))
}

/// Keep a session late side-output alongside the normal window result.  A
/// `MultipleWithAck` result lets the graph router settle both branches of the
/// same source delivery through one fan-out parent.
fn append_late_session_output(
    result: ProcessResult,
    late_output: Option<(crate::MessageBatchRef, Arc<dyn Ack>)>,
) -> ProcessResult {
    let Some(late_output) = late_output else {
        return result;
    };
    let mut outputs = match result {
        ProcessResult::Single(batch) => {
            vec![(batch, Arc::new(crate::input::NoopAck) as Arc<dyn Ack>)]
        }
        ProcessResult::Multiple(batches) => batches
            .into_iter()
            .map(|batch| (batch, Arc::new(crate::input::NoopAck) as Arc<dyn Ack>))
            .collect(),
        ProcessResult::SingleWithAck(batch, ack) => vec![(batch, ack)],
        ProcessResult::MultipleWithAck(outputs) => outputs,
        ProcessResult::Deferred | ProcessResult::None => Vec::new(),
    };
    outputs.push(late_output);
    ProcessResult::MultipleWithAck(outputs)
}

async fn compensate_window_acks(error: Error, acknowledgements: Vec<Arc<dyn Ack>>) -> Error {
    match crate::input::VecAck(acknowledgements).abort().await {
        Ok(()) => error,
        Err(abort_error) => Error::Process(format!(
            "window processing failed: {error}; acknowledgement compensation failed: {abort_error}"
        )),
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
        let operation_guard = self.operation_lock.clone().lock_owned().await;
        self.load_from_backend()?;
        let before = self.runtime_snapshot();
        let firing = self
            .fire_ready(i64::MAX)
            .inspect_err(|_error| self.restore_runtime(&before))?;
        self.persist_buffers_for_fired(
            firing
                .output
                .as_ref()
                .map(|(_, keys)| keys.as_slice())
                .unwrap_or(&[]),
        )
        .inspect_err(|_error| self.restore_runtime(&before))?;
        let dropped_acks = self.take_acks(&firing.dropped_keys);
        Ok(match firing.output {
            Some((emitted, fired_keys)) => {
                let after = self.runtime_snapshot();
                ProcessResult::SingleWithAck(
                    emitted,
                    self.fired_ack(&fired_keys, dropped_acks, before, after, operation_guard),
                )
            }
            None => {
                // Only expired or empty groups fired: settle their deliveries
                // directly so the source frontier still advances.
                if !dropped_acks.is_empty() {
                    ConcurrentAck(dropped_acks).ack().await?;
                }
                drop(operation_guard);
                ProcessResult::None
            }
        })
    }

    async fn on_tick(&self) -> Result<ProcessResult, Error> {
        if self.config.trigger != WindowTrigger::ProcessingTime {
            return Ok(ProcessResult::None);
        }
        let operation_guard = self.operation_lock.clone().lock_owned().await;
        self.load_from_backend()?;
        let before = self.runtime_snapshot();
        let now = crate::state::now_ms() as i64;
        let due = if self.config.legacy_payload
            && matches!(self.config.kind, WindowKind::Session { .. })
        {
            let interval = self.config.trigger_interval_ms.max(1) as i64;
            let mut activity = self.last_processing_activity_ms.lock().unwrap();
            if activity.is_some_and(|previous| now.saturating_sub(previous) >= interval) {
                *activity = None;
                true
            } else {
                false
            }
        } else {
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
            drop(operation_guard);
            return Ok(ProcessResult::None);
        }

        // Processing-time triggers flush the current buffers independent of
        // event timestamps. The timestamp only determines the aggregate key;
        // it must not prevent an idle timer from emitting old or future-dated
        // records.
        let firing = self
            .fire_ready(i64::MAX)
            .inspect_err(|_error| self.restore_runtime(&before))?;
        self.persist_buffers_for_fired(
            firing
                .output
                .as_ref()
                .map(|(_, keys)| keys.as_slice())
                .unwrap_or(&[]),
        )
        .inspect_err(|_error| self.restore_runtime(&before))?;
        let dropped_acks = self.take_acks(&firing.dropped_keys);
        Ok(match firing.output {
            Some((emitted, fired_keys)) => {
                let after = self.runtime_snapshot();
                ProcessResult::SingleWithAck(
                    emitted,
                    self.fired_ack(&fired_keys, dropped_acks, before, after, operation_guard),
                )
            }
            None => {
                // Only expired or empty groups fired: settle their deliveries
                // directly so the source frontier still advances.
                if !dropped_acks.is_empty() {
                    ConcurrentAck(dropped_acks).ack().await?;
                }
                drop(operation_guard);
                ProcessResult::None
            }
        })
    }

    async fn on_watermark(&self, watermark_ms: i64) -> Result<ProcessResult, Error> {
        let operation_guard = self.operation_lock.clone().lock_owned().await;
        self.load_from_backend()?;
        let before = self.runtime_snapshot();
        {
            let mut watermark = self.watermark_ms.lock().unwrap();
            *watermark = Some(watermark.map_or(watermark_ms, |current| current.max(watermark_ms)));
        }
        if self.config.trigger != WindowTrigger::Watermark {
            drop(operation_guard);
            return Ok(ProcessResult::None);
        }
        let firing = self
            .fire_ready(watermark_ms)
            .inspect_err(|_error| self.restore_runtime(&before))?;
        self.persist_buffers_for_fired(
            firing
                .output
                .as_ref()
                .map(|(_, keys)| keys.as_slice())
                .unwrap_or(&[]),
        )
        .inspect_err(|_error| self.restore_runtime(&before))?;
        let dropped_acks = self.take_acks(&firing.dropped_keys);
        Ok(match firing.output {
            Some((emitted, fired_keys)) => {
                let after = self.runtime_snapshot();
                ProcessResult::SingleWithAck(
                    emitted,
                    self.fired_ack(&fired_keys, dropped_acks, before, after, operation_guard),
                )
            }
            None => {
                // Only expired or empty groups fired: settle their deliveries
                // directly so the source frontier still advances.
                if !dropped_acks.is_empty() {
                    ConcurrentAck(dropped_acks).ack().await?;
                }
                drop(operation_guard);
                ProcessResult::None
            }
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
        let operation_guard = self.operation_lock.clone().lock_owned().await;
        self.load_from_backend()?;
        let before = self.runtime_snapshot();
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
                let ack_for_error = ack.clone();
                if let Err(error) = ack.ack().await {
                    let _ = ack_for_error.abort().await;
                    return Err(error);
                }
            }
            drop(operation_guard);
            return Ok(ProcessResult::None);
        }
        self.observe_watermark(&batch);
        // Session boundaries are dynamic and keyed, so the source gate cannot
        // classify an event against a stable `event_time + gap` deadline.
        // Split expired session rows here, before they can create a fresh
        // partial aggregate. Accepted rows and a configured late route share
        // the source acknowledgement; dropped rows consume a third child so
        // the parent is not committed until every outcome is settled.
        let (keep, late, invalid_timestamps) = self.session_late_masks(&batch)?;
        let late_count = late.iter().filter(|is_late| **is_late).count();
        if late_count > 0 {
            // The gate cannot classify session lateness; these rows would
            // otherwise never reach the kernel `late_events` metric, which the
            // spec requires to count every late or invalid row regardless of
            // whether the policy drops, routes, or updates it.
            self.late_event_rows
                .fetch_add(late_count as u64, std::sync::atomic::Ordering::Relaxed);
        }
        let has_accepted_rows = keep.iter().any(|keep| *keep);
        let route_late = late_count > 0
            && self.late_event_policy == LateEventPolicy::Route
            && self.late_event_route_configured;
        let drop_late = late_count > 0 && !route_late;
        let mut late_output = None;
        let has_ack_flow = ack.is_some();
        let mut ack = ack;

        if late_count > 0 {
            let accepted_batch = has_accepted_rows
                .then(|| filter_window_batch(&batch, &keep))
                .transpose()?;
            let late_batch = if route_late {
                let late_batch = filter_window_batch(&batch, &late)?;
                let late_invalid = invalid_timestamps
                    .iter()
                    .zip(late.iter())
                    .filter_map(|(invalid, is_late)| (*is_late).then_some(*invalid))
                    .collect::<Vec<_>>();
                Some(mark_late_session_batch(late_batch, &late_invalid)?)
            } else {
                None
            };

            let outcome_count =
                usize::from(has_accepted_rows) + usize::from(route_late) + usize::from(drop_late);
            let mut child_acks = ack
                .take()
                .map(|source_ack| fanout_ack(source_ack, outcome_count).into_iter())
                .into_iter()
                .flatten();
            let mut owned_acks = Vec::new();

            if has_accepted_rows {
                if let Some(accepted_ack) = child_acks.next() {
                    owned_acks.push(accepted_ack.clone());
                    ack = Some(accepted_ack);
                }
            }
            if route_late {
                let late_ack = child_acks
                    .next()
                    .unwrap_or_else(|| Arc::new(crate::input::NoopAck));
                if has_ack_flow {
                    owned_acks.push(late_ack.clone());
                }
                late_output = Some((late_batch.expect("route batch was built"), late_ack));
            }
            if drop_late {
                if let Some(dropped_ack) = child_acks.next() {
                    owned_acks.push(dropped_ack.clone());
                    if let Err(error) = dropped_ack.ack().await {
                        return Err(compensate_window_acks(error, owned_acks).await);
                    }
                }
            }

            let Some(accepted_batch) = accepted_batch else {
                // The current delivery contains only dropped/routed rows,
                // but its watermark may still make a retained session or
                // another window eligible for cleanup. Run the ordinary
                // firing/persistence phase with no accepted input rather than
                // returning before stale state is reconciled.
                return match self
                    .finish_processed_batch(
                        Vec::new(),
                        None,
                        late_output,
                        has_ack_flow,
                        before,
                        operation_guard,
                    )
                    .await
                {
                    Ok(result) => Ok(result),
                    Err(error) => Err(compensate_window_acks(error, owned_acks).await),
                };
            };
            let touched = match self.accumulate(&accepted_batch) {
                Ok(touched) => touched,
                Err(error) => {
                    // Rows already folded into buffers before the failure
                    // must not survive: the delivery is replayed after the
                    // ack compensation, and a partial aggregate would count
                    // them twice.
                    self.restore_runtime(&before);
                    return Err(compensate_window_acks(error, owned_acks).await);
                }
            };
            return match self
                .finish_processed_batch(
                    touched,
                    ack,
                    late_output,
                    has_ack_flow,
                    before.clone(),
                    operation_guard,
                )
                .await
            {
                Ok(result) => Ok(result),
                Err(error) => {
                    self.restore_runtime(&before);
                    Err(compensate_window_acks(error, owned_acks).await)
                }
            };
        }

        let touched = match self.accumulate(&batch) {
            Ok(touched) => touched,
            Err(error) => {
                // Drop partially applied rows before the delivery is
                // replayed, mirroring the WindowFiredAck failure path.
                self.restore_runtime(&before);
                return Err(error);
            }
        };
        let threshold = match self.config.trigger {
            WindowTrigger::Watermark => *self.watermark_ms.lock().unwrap(),
            WindowTrigger::ProcessingTime if self.config.legacy_payload => {
                let now = crate::state::now_ms() as i64;
                if matches!(self.config.kind, WindowKind::Session { .. }) {
                    *self.last_processing_activity_ms.lock().unwrap() = Some(now);
                } else {
                    let mut last = self.last_processing_trigger_ms.lock().unwrap();
                    if last.is_none() {
                        *last = Some(now);
                    }
                }
                None
            }
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
                Some(threshold) => self
                    .fire_ready(threshold)
                    .inspect_err(|_error| self.restore_runtime(&before))?,
                None => WindowFiring {
                    output: None,
                    dropped_keys: Vec::new(),
                },
            };
            self.persist_buffers_for_fired(
                fired
                    .output
                    .as_ref()
                    .map(|(_, keys)| keys.as_slice())
                    .unwrap_or(&[]),
            )
            .inspect_err(|_error| self.restore_runtime(&before))?;
            // No acknowledgement flow gates the commit, so fired windows
            // and still-open window mutations commit immediately (legacy
            // direct-persist semantics).  Committing only fired keys would
            // leave a transaction staged for every open window touched by a
            // no-ack caller, eventually exhausting the journal bound.
            self.commit_all_window_txns()?;
            drop(operation_guard);
            return Ok(match fired.output {
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
            Some(threshold) => self.fire_ready(threshold).inspect_err(|_error| {
                // A half-fired round (some buffers already marked emitted)
                // must not keep that memory state: the error surfaces as a
                // replay, and `emitted`-but-never-emitted windows would
                // never fire again.
                self.restore_runtime(&before);
            })?,
            None => WindowFiring {
                output: None,
                dropped_keys: Vec::new(),
            },
        };
        self.persist_buffers_for_fired(
            fired
                .output
                .as_ref()
                .map(|(_, keys)| keys.as_slice())
                .unwrap_or(&[]),
        )
        .inspect_err(|_error| self.restore_runtime(&before))?;

        let dropped_acks = self.take_acks(&fired.dropped_keys);
        let Some((emitted, fired_keys)) = fired.output else {
            // Nothing was emitted, but expired or empty groups still settled:
            // acknowledge their deliveries now so the source frontier moves.
            if !dropped_acks.is_empty() {
                ConcurrentAck(dropped_acks).ack().await?;
            }
            if touched.is_empty() {
                ack.ack().await?;
            }
            drop(operation_guard);
            return Ok(ProcessResult::Deferred);
        };

        let mut extra = if touched.is_empty() {
            // A watermark-only batch still has to be committed, but only
            // after the output produced by that watermark has been written.
            vec![ack]
        } else {
            Vec::new()
        };
        // Dropped groups (expired windows, empty buffers) settle together
        // with the fired output: their state was rolled back, so releasing
        // their deliveries alongside the aggregate commit keeps one
        // settlement boundary.
        extra.extend(dropped_acks);
        let after = self.runtime_snapshot();
        Ok(ProcessResult::SingleWithAck(
            emitted,
            self.fired_ack(&fired_keys, extra, before, after, operation_guard),
        ))
    }

    /// Finish an accepted batch after the session-specific late rows have
    /// been split out.  The normal path is kept in the same order as the
    /// legacy implementation; the optional side output is appended only
    /// after the aggregate result has been formed.
    async fn finish_processed_batch(
        &self,
        touched: Vec<(i64, String)>,
        ack: Option<Arc<dyn Ack>>,
        late_output: Option<(MessageBatchRef, Arc<dyn Ack>)>,
        has_ack_flow: bool,
        before: WindowRuntimeSnapshot,
        operation_guard: tokio::sync::OwnedMutexGuard<()>,
    ) -> Result<ProcessResult, Error> {
        let threshold = match self.config.trigger {
            WindowTrigger::Watermark => *self.watermark_ms.lock().unwrap(),
            WindowTrigger::ProcessingTime if self.config.legacy_payload => {
                let now = crate::state::now_ms() as i64;
                if matches!(self.config.kind, WindowKind::Session { .. }) {
                    *self.last_processing_activity_ms.lock().unwrap() = Some(now);
                } else {
                    let mut last = self.last_processing_trigger_ms.lock().unwrap();
                    if last.is_none() {
                        *last = Some(now);
                    }
                }
                None
            }
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

        // Split the source delivery before firing so each window group owns a
        // child acknowledgement. This ordering lets fire_ready transfer the
        // child acks into the emitted aggregate when this batch closes it.
        if let Some(ack) = ack.as_ref() {
            if !touched.is_empty() {
                let group_acks = fanout_ack(ack.clone(), touched.len());
                self.remember_acks(touched.clone(), group_acks);
            }
        }

        let fired = match threshold {
            Some(threshold) => self.fire_ready(threshold).inspect_err(|_error| {
                // Same rollback contract as the direct path: a half-fired
                // round must not survive as emitted-but-unemitted memory.
                self.restore_runtime(&before);
            })?,
            None => WindowFiring {
                output: None,
                dropped_keys: Vec::new(),
            },
        };
        self.persist_buffers_for_fired(
            fired
                .output
                .as_ref()
                .map(|(_, keys)| keys.as_slice())
                .unwrap_or(&[]),
        )
        .inspect_err(|_error| self.restore_runtime(&before))?;

        let dropped_acks = self.take_acks(&fired.dropped_keys);
        match fired.output {
            None => {
                if has_ack_flow {
                    if !dropped_acks.is_empty() {
                        // Expired or empty groups settled without an output:
                        // release their deliveries directly.
                        ConcurrentAck(dropped_acks).ack().await?;
                    }
                    if touched.is_empty() {
                        // A watermark-only batch still has to be acknowledged
                        // even when it did not mutate a window. In the
                        // session late-only path `ack` is None because the
                        // dropped child was already settled above.
                        if let Some(ack) = ack {
                            ack.ack().await?;
                        }
                    }
                    drop(operation_guard);
                    Ok(append_late_session_output(
                        ProcessResult::Deferred,
                        late_output,
                    ))
                } else {
                    self.commit_all_window_txns()?;
                    drop(operation_guard);
                    Ok(append_late_session_output(ProcessResult::None, late_output))
                }
            }
            Some((emitted, fired_keys)) if has_ack_flow => {
                let mut extra = if touched.is_empty() {
                    // A watermark-only batch still has to be committed, but
                    // only after the output produced by that watermark is
                    // written.
                    ack.into_iter().collect()
                } else {
                    Vec::new()
                };
                // Dropped groups settle together with the fired output so a
                // source failure rolls the whole round back consistently.
                extra.extend(dropped_acks);
                let after = self.runtime_snapshot();
                Ok(append_late_session_output(
                    ProcessResult::SingleWithAck(
                        emitted,
                        self.fired_ack(&fired_keys, extra, before, after, operation_guard),
                    ),
                    late_output,
                ))
            }
            Some((emitted, _)) => {
                self.commit_all_window_txns()?;
                drop(operation_guard);
                Ok(append_late_session_output(
                    ProcessResult::Single(emitted),
                    late_output,
                ))
            }
        }
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

    /// Assemble the acknowledgement of one fired-window output. All staged
    /// window transactions and all source acknowledgements are one composite:
    /// a source failure rolls every window transaction back before the input
    /// can be replayed.
    fn fired_ack(
        &self,
        fired_keys: &[(i64, String)],
        extra: Vec<Arc<dyn Ack>>,
        before: WindowRuntimeSnapshot,
        after: WindowRuntimeSnapshot,
        operation_guard: tokio::sync::OwnedMutexGuard<()>,
    ) -> Arc<dyn Ack> {
        let fired_txns = self.journal.as_ref().map(|journal| {
            let mut txns = self.window_txns.lock().unwrap();
            let fired = fired_keys
                .iter()
                .filter_map(|key| txns.remove(key))
                .collect::<Vec<_>>();
            (journal.clone(), fired)
        });
        let mut source_acks = self.take_acks(fired_keys);
        source_acks.extend(extra);
        let source_ack: Arc<dyn Ack> = if source_acks.is_empty() {
            Arc::new(crate::input::NoopAck)
        } else {
            Arc::new(ConcurrentAck(source_acks))
        };
        let inner: Arc<dyn Ack> = match fired_txns {
            Some((journal, txns)) if !txns.is_empty() => Arc::new(
                super::state_journal::CommitGroupOnAck::new(journal, txns, source_ack),
            ),
            _ => source_ack,
        };
        Arc::new(WindowFiredAck {
            inner,
            rollback: self.rollback_state(before, after),
            operation_guard: Mutex::new(Some(operation_guard)),
        })
    }

    /// Commit all staged window mutations immediately when there is no
    /// acknowledgement flow to gate on.  Keep entries in the lookup map until
    /// each commit succeeds so a backend error can be retried without losing
    /// the transaction handle.
    fn commit_all_window_txns(&self) -> Result<(), Error> {
        if let Some(journal) = &self.journal {
            let txns = self
                .window_txns
                .lock()
                .unwrap()
                .iter()
                .map(|(key, txn)| (key.clone(), *txn))
                .collect::<Vec<_>>();
            for (key, txn) in txns {
                journal.commit(txn)?;
                self.window_txns.lock().unwrap().remove(&key);
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
        self.persist_buffers_for_fired(&[])
    }

    /// Persist working buffers and attach stale-key cleanup to the fired
    /// output transaction when one exists. A cleanup transaction must not be
    /// committed independently: a source acknowledgement failure has to be
    /// able to roll back a session re-key or an expired-window deletion.
    fn persist_buffers_for_fired(&self, fired_keys: &[(i64, String)]) -> Result<(), Error> {
        let current = self
            .buffers
            .lock()
            .unwrap()
            .iter()
            .map(|((window_start, key), buffer)| ((*window_start, key.clone()), buffer.clone()))
            .collect::<BTreeMap<_, _>>();
        if let Some(journal) = &self.journal {
            // Only transactions created by a mutation or by `fire_ready` are
            // dirty. An emitted buffer retained for allowed lateness has
            // already been committed by its fired acknowledgement and must
            // not acquire a fresh staged transaction when another key gets a
            // row.
            let dirty = self
                .window_txns
                .lock()
                .unwrap()
                .iter()
                .map(|(key, txn)| (key.clone(), *txn))
                .collect::<Vec<_>>();
            for ((window_start, key), txn) in dirty {
                let Some(buffer) = current.get(&(window_start, key.clone())) else {
                    // `fire_ready` may have staged a delete for a legacy
                    // one-shot buffer. Leave that mutation intact so the
                    // fired acknowledgement can apply it.
                    continue;
                };
                journal.put_compact(
                    txn,
                    &self.namespace,
                    &Self::state_key(window_start, &key),
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
            // A stale key created by a session re-key must be deleted by the
            // NEW session's transaction, not by an unrelated window that
            // happens to fire in the same call. Otherwise the unrelated
            // source acknowledgement could commit the deletion while the
            // re-keying row is still held and later replay would have no old
            // aggregate to fall back to. Expired keys with no replacement
            // may use any fired transaction because they have no outstanding
            // source delivery of their own.
            let fired_by_logical_key = fired_keys
                .iter()
                .map(|(start, key)| (key.as_str(), (*start, key.clone())))
                .collect::<BTreeMap<_, _>>();
            let mut cleanup = BTreeMap::<super::state_journal::StateTxn, Vec<Vec<u8>>>::new();
            let mut immediate_cleanup = Vec::new();
            let mut deferred = 0usize;
            for raw in stale_keys {
                let stale_key = Self::decode_state_key(&raw)?;
                let has_replacement = current.keys().any(|(_, key)| key == &stale_key.1);
                let transaction_key = fired_by_logical_key
                    .get(stale_key.1.as_str())
                    .cloned()
                    .or_else(|| {
                        (!has_replacement)
                            .then(|| fired_keys.first().cloned())
                            .flatten()
                    });
                if let Some(transaction_key) = transaction_key {
                    let txn = self.window_txn(&transaction_key)?;
                    cleanup.entry(txn).or_default().push(raw);
                } else if !has_replacement {
                    // No source delivery can still recreate an expired key,
                    // so an idle watermark cleanup may remove it immediately
                    // even when this call has no fired output to own a
                    // transaction.  Otherwise a journaled operator would
                    // retain the backend row forever whenever cleanup runs
                    // between output batches.
                    immediate_cleanup.push(raw);
                } else {
                    deferred += 1;
                }
            }
            for (txn, keys) in cleanup {
                for raw in keys {
                    journal.delete(txn, &self.namespace, &raw)?;
                }
            }
            for raw in immediate_cleanup {
                self.backend.delete(&self.namespace, &raw)?;
            }
            if deferred > 0 {
                // Leave stale committed entries in place until the matching
                // replacement output fires. Keeping an old value is safe and
                // replayable; deleting it here would make a later source-ack
                // failure irreversible.
                tracing::debug!(
                    namespace = %self.namespace,
                    count = deferred,
                    "deferring stale window-state cleanup until the replacement acknowledgement"
                );
            }
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

/// A window aggregate written by the pre-typed state format. The legacy float
/// sum is deliberately absent: the writer that produced this envelope set
/// `is_float` together with a non-zero `sum_float`, and a payload with
/// `is_float && count > 0` is rejected by `migrate`, so a migratable payload
/// carries no float contribution to preserve.
#[derive(serde::Deserialize)]
struct LegacyAggregateBuffer {
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
fn decode_buffer(bytes: &[u8]) -> Result<AggregateBuffer, Error> {
    let mut buffer = decode_buffer_unchecked(bytes)?;
    if buffer.count != buffer.int_observations + buffer.float_observations {
        buffer.normalize_observation_counters();
    }
    Ok(buffer)
}

fn decode_buffer_unchecked(bytes: &[u8]) -> Result<AggregateBuffer, Error> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array as I64, StringArray};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    struct FailOnceAck {
        fail: AtomicBool,
    }

    #[async_trait]
    impl Ack for FailOnceAck {
        async fn ack(&self) -> Result<(), Error> {
            if self.fail.swap(false, Ordering::AcqRel) {
                Err(Error::Process("source acknowledgement failed".into()))
            } else {
                Ok(())
            }
        }
    }

    struct CountingAck {
        acked: AtomicUsize,
    }

    #[async_trait]
    impl Ack for CountingAck {
        async fn ack(&self) -> Result<(), Error> {
            self.acked.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
    }

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
                legacy_payload: false,
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
                legacy_payload: false,
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
                legacy_payload: false,
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
    async fn late_session_bridge_preserves_emitted_update_state() {
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
        let op = ColumnarWindowOperator::with_late_event_policy(
            WindowOperatorConfig {
                kind: WindowKind::Session { gap_ms: 1_000 },
                timestamp_field: "ts".into(),
                key_field: "key".into(),
                value_fields: vec!["value".into()],
                trigger: WindowTrigger::Watermark,
                trigger_interval_ms: 1_000,
                watermark_field: "__watermark_ms".into(),
                allowed_lateness_ms: 10_000,
                legacy_payload: false,
            },
            backend,
            "session-bridge-update-test",
            LateEventPolicy::Update,
            false,
        );

        // The two rows are separate sessions at first: [1000, 2000) and
        // [2500, 3500). Both results are retained after the initial fire so a
        // later out-of-order row can bridge them.
        op.process(batch(vec![(1_000, "a", 1), (2_500, "a", 2)], None))
            .await
            .unwrap();
        let first = op.on_watermark(3_500).await.unwrap();
        let ProcessResult::SingleWithAck(first, first_ack) = first else {
            panic!("the initial sessions should fire");
        };
        first_ack.ack().await.unwrap();
        let initial_updates = first
            .record_batch()
            .column_by_name("__arkflow_window_update")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(!initial_updates.value(0));
        assert!(!initial_updates.value(1));

        // Timestamp 1900 is within the first session and its extended end
        // reaches the second session's start. The merge must remain an update
        // of the already-emitted aggregate, not a fresh initial result.
        let corrected = op
            .process(batch(vec![(1_900, "a", 3)], None))
            .await
            .unwrap();
        let ProcessResult::Single(corrected) = corrected else {
            panic!("the bridged session should emit a correction");
        };
        let updates = corrected
            .record_batch()
            .column_by_name("__arkflow_window_update")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(updates.value(0));
        let starts = corrected
            .record_batch()
            .column_by_name("window_start")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(starts.values(), &[1_000]);
        let counts = corrected
            .record_batch()
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(counts.values(), &[3]);
    }

    #[tokio::test]
    async fn expired_session_rows_are_dropped_without_opening_a_new_session() {
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::InMemoryStateBackend::new(1).unwrap());
        let op = ColumnarWindowOperator::with_late_event_policy(
            WindowOperatorConfig {
                kind: WindowKind::Session { gap_ms: 1_000 },
                timestamp_field: "ts".into(),
                key_field: "key".into(),
                value_fields: vec!["value".into()],
                trigger: WindowTrigger::Watermark,
                trigger_interval_ms: 1_000,
                watermark_field: "__watermark_ms".into(),
                allowed_lateness_ms: 0,
                legacy_payload: false,
            },
            backend,
            "session-expiry-test",
            LateEventPolicy::Drop,
            false,
        );

        op.process(batch(vec![(100, "a", 1)], None)).await.unwrap();
        let fired = op
            .process(batch(vec![(3_000, "b", 0)], Some(2_000)))
            .await
            .unwrap();
        assert!(matches!(fired, ProcessResult::Single(_)));

        // The original session ended at 1100 and was already past its
        // allowed-lateness deadline. A late row must be acknowledged/dropped,
        // not create a new [100, 1100) partial session.
        assert!(matches!(
            op.process(batch(vec![(100, "a", 99)], None)).await.unwrap(),
            ProcessResult::None
        ));
        assert!(!op.buffers.lock().unwrap().keys().any(|(_, key)| key == "a"));
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
            legacy_payload: false,
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
        let trigger = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("ts", DataType::Int64, false),
                Field::new("key", DataType::Utf8, false),
                Field::new("f", DataType::Float64, false),
                Field::new("__watermark_ms", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![20_000])),
                Arc::new(StringArray::from(vec!["z"])),
                Arc::new(Float64Array::from(vec![0.0])),
                Arc::new(Int64Array::from(vec![10_000])),
            ],
        )
        .unwrap();
        let fired = op
            .process(Arc::new(crate::MessageBatch::new_arrow(trigger)))
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
            legacy_payload: false,
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
        let trigger = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("ts", DataType::Int64, false),
                Field::new("key", DataType::Utf8, false),
                Field::new("f", DataType::Float32, false),
                Field::new("__watermark_ms", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![20_000])),
                Arc::new(StringArray::from(vec!["z"])),
                Arc::new(datafusion::arrow::array::Float32Array::from(vec![0.0f32])),
                Arc::new(Int64Array::from(vec![10_000])),
            ],
        )
        .unwrap();
        let fired = op
            .process(Arc::new(crate::MessageBatch::new_arrow(trigger)))
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
            legacy_payload: false,
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
            legacy_payload: false,
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

    #[tokio::test]
    async fn fired_window_rolls_back_when_source_ack_fails() {
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::InMemoryStateBackend::new(1).unwrap());
        let journal = Arc::new(super::super::state_journal::StateJournal::new(
            backend.clone(),
        ));
        let op = ColumnarWindowOperator::with_journal(
            WindowOperatorConfig {
                kind: WindowKind::Tumbling { size_ms: 1_000 },
                timestamp_field: "ts".into(),
                key_field: "key".into(),
                value_fields: vec!["value".into()],
                trigger: WindowTrigger::Watermark,
                trigger_interval_ms: 1_000,
                watermark_field: "__watermark_ms".into(),
                allowed_lateness_ms: 0,
                legacy_payload: false,
            },
            backend.clone(),
            journal.clone(),
            "window-ack-rollback-test",
        );
        let source_ack = Arc::new(FailOnceAck {
            fail: AtomicBool::new(true),
        });

        // The row is buffered and its source acknowledgement is held in the
        // window transaction until the aggregate is emitted.
        op.process_with_ack(batch(vec![(100, "a", 10)], None), source_ack.clone())
            .await
            .unwrap();

        let fired = op
            .process_with_ack(
                batch(vec![(2_000, "b", 0)], Some(1_000)),
                Arc::new(crate::input::NoopAck),
            )
            .await
            .unwrap();
        let ProcessResult::SingleWithAck(_, output_ack) = fired else {
            panic!("watermark should produce an acknowledged window output");
        };

        // The source commit fails after the journal has applied the fired
        // window. The composite acknowledgement must compensate that apply,
        // leaving the input replayable and the backend at the pre-fire cut.
        assert!(output_ack.ack().await.is_err());
        assert!(backend
            .get(
                "window-ack-rollback-test",
                &ColumnarWindowOperator::state_key(0, "a")
            )
            .unwrap()
            .is_none());

        // Retrying the same acknowledgement re-applies the staged mutation;
        // the successful source commit then makes the fired window durable.
        output_ack.ack().await.unwrap();
        let restored = backend
            .get(
                "window-ack-rollback-test",
                &ColumnarWindowOperator::state_key(0, "a"),
            )
            .unwrap()
            .expect("successful retry commits the window state");
        let restored = decode_buffer(&restored).unwrap();
        assert_eq!(restored.count, 1);
        assert_eq!(restored.sum_i64, 10);
        assert!(restored.emitted);
        assert_eq!(journal.pending_transactions(), 1);
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
            legacy_payload: false,
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

    #[tokio::test]
    async fn journaled_idle_cleanup_removes_expired_backend_rows() {
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::InMemoryStateBackend::new(1).unwrap());
        let journal = Arc::new(super::super::state_journal::StateJournal::new(
            backend.clone(),
        ));
        let op = ColumnarWindowOperator::with_journal(
            WindowOperatorConfig {
                kind: WindowKind::Tumbling { size_ms: 1_000 },
                timestamp_field: "ts".into(),
                key_field: "key".into(),
                value_fields: vec!["value".into()],
                trigger: WindowTrigger::Watermark,
                trigger_interval_ms: 1_000,
                watermark_field: "__watermark_ms".into(),
                allowed_lateness_ms: 5_000,
                legacy_payload: false,
            },
            backend.clone(),
            journal,
            "journaled-idle-cleanup-test",
        );

        op.process_with_ack(
            batch(vec![(100, "a", 1)], None),
            Arc::new(crate::input::NoopAck),
        )
        .await
        .unwrap();
        let fired = op.on_watermark(1_000).await.unwrap();
        let ProcessResult::SingleWithAck(_, fired_ack) = fired else {
            panic!("the initial window should fire");
        };
        fired_ack.ack().await.unwrap();

        let state_key = ColumnarWindowOperator::state_key(0, "a");
        assert!(backend
            .get("journaled-idle-cleanup-test", &state_key)
            .unwrap()
            .is_some());

        // No new window is ready at this watermark. Cleanup must still remove
        // the committed expired row instead of waiting for an unrelated fired
        // output to own a journal transaction.
        assert!(matches!(
            op.on_watermark(7_000).await.unwrap(),
            ProcessResult::None
        ));
        assert!(backend
            .get("journaled-idle-cleanup-test", &state_key)
            .unwrap()
            .is_none());
    }

    /// Legacy Stream windows are compatibility buffers, not aggregate
    /// projections: the downstream processor must receive the original rows
    /// and schema after the processing-time flush.
    #[tokio::test]
    async fn legacy_window_flush_preserves_input_payload_and_name() {
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
        let op = ColumnarWindowOperator::new(
            WindowOperatorConfig {
                kind: WindowKind::Tumbling { size_ms: 60_000 },
                timestamp_field: "__meta_timestamp".into(),
                key_field: "__arkflow_window_all".into(),
                value_fields: Vec::new(),
                trigger: WindowTrigger::ProcessingTime,
                trigger_interval_ms: 1_000,
                watermark_field: "__watermark_ms".into(),
                allowed_lateness_ms: 0,
                legacy_payload: true,
            },
            backend,
            "legacy-payload-test",
        );
        let mut input = crate::MessageBatch::new_arrow(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Utf8, false),
                    Field::new("amount", DataType::Int64, false),
                ])),
                vec![
                    Arc::new(StringArray::from(vec!["a", "b"])),
                    Arc::new(Int64Array::from(vec![3, 5])),
                ],
            )
            .unwrap(),
        );
        input.set_input_name(Some("legacy-source".into()));
        op.process(Arc::new(input)).await.unwrap();

        *op.last_processing_trigger_ms.lock().unwrap() =
            Some(crate::state::now_ms() as i64 - 2_000);
        let ProcessResult::SingleWithAck(flushed, _) = op.on_tick().await.unwrap() else {
            panic!("legacy processing-time window should flush");
        };
        assert_eq!(flushed.get_input_name(), Some("legacy-source".into()));
        assert_eq!(flushed.record_batch().schema().fields().len(), 2);
        assert!(flushed.record_batch().column_by_name("id").is_some());
        assert!(flushed.record_batch().column_by_name("amount").is_some());
        assert_eq!(flushed.record_batch().num_rows(), 2);
        let amounts = flushed
            .record_batch()
            .column_by_name("amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(amounts.values(), &[3, 5]);

        // A later processing-time flush must contain only the newly arrived
        // rows; the one-shot legacy buffer must not replay the first batch.
        let second = crate::MessageBatch::new_arrow(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Utf8, false),
                    Field::new("amount", DataType::Int64, false),
                ])),
                vec![
                    Arc::new(StringArray::from(vec!["c"])),
                    Arc::new(Int64Array::from(vec![7])),
                ],
            )
            .unwrap(),
        );
        op.process(Arc::new(second)).await.unwrap();
        *op.last_processing_trigger_ms.lock().unwrap() =
            Some(crate::state::now_ms() as i64 - 2_000);
        let ProcessResult::SingleWithAck(flushed, _) = op.on_tick().await.unwrap() else {
            panic!("legacy processing-time window should flush the second batch");
        };
        assert_eq!(flushed.record_batch().num_rows(), 1);
        let amounts = flushed
            .record_batch()
            .column_by_name("amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(amounts.values(), &[7]);
    }

    /// Rows with a nullable Float64 value column.
    fn nullable_float_batch(
        rows: Vec<(i64, &str, Option<f64>)>,
        watermark: Option<i64>,
    ) -> MessageBatchRef {
        let mut fields = vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ];
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(I64::from(rows.iter().map(|row| row.0).collect::<Vec<_>>())),
            Arc::new(datafusion::arrow::array::StringArray::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(datafusion::arrow::array::Float64Array::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
        ];
        if let Some(watermark) = watermark {
            fields.push(Field::new("__watermark_ms", DataType::Int64, false));
            columns.push(Arc::new(I64::from(vec![watermark; rows.len()])));
        }
        Arc::new(crate::MessageBatch::new_arrow(
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap(),
        ))
    }

    /// Regression: a window group whose rows all carried NULL values emits
    /// no aggregate, but its delivery acknowledgement must still settle when
    /// the window fires — otherwise the acknowledgement strands in
    /// `pending_acks` forever, the source frontier freezes, and its journal
    /// transaction leaks toward the pending bound.
    #[tokio::test]
    async fn empty_null_window_settles_its_source_acknowledgement() {
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::InMemoryStateBackend::new(1).unwrap());
        let journal = Arc::new(super::super::state_journal::StateJournal::new(
            backend.clone(),
        ));
        let op = ColumnarWindowOperator::with_journal(
            WindowOperatorConfig {
                kind: WindowKind::Tumbling { size_ms: 1_000 },
                timestamp_field: "ts".into(),
                key_field: "key".into(),
                value_fields: vec!["value".into()],
                trigger: WindowTrigger::Watermark,
                trigger_interval_ms: 1_000,
                watermark_field: "__watermark_ms".into(),
                allowed_lateness_ms: 0,
                legacy_payload: false,
            },
            backend.clone(),
            journal.clone(),
            "null-window-ack-test",
        );
        let source_ack = Arc::new(CountingAck {
            acked: AtomicUsize::new(0),
        });

        // A NULL-only group: the row buffers the window but never produces a
        // value, so the group can only be dropped at fire time.
        op.process_with_ack(
            nullable_float_batch(vec![(100, "a", None)], None),
            source_ack.clone() as Arc<dyn Ack>,
        )
        .await
        .unwrap();
        assert!(
            op.pending_acks
                .lock()
                .unwrap()
                .contains_key(&(0, "a".to_string())),
            "the delivery is held by the open window"
        );
        assert_eq!(journal.pending_transactions(), 1);

        // The watermark fires the window; the NULL-only group must settle its
        // delivery and discard its transaction instead of stranding them.
        let fired = op
            .process_with_ack(
                nullable_float_batch(vec![(2_000, "b", Some(1.0))], Some(1_000)),
                Arc::new(crate::input::NoopAck),
            )
            .await
            .unwrap();
        assert!(
            matches!(fired, ProcessResult::Deferred),
            "a NULL-only round emits no aggregate row"
        );

        assert_eq!(
            source_ack.acked.load(Ordering::Acquire),
            1,
            "the NULL-only window's delivery must be acknowledged"
        );
        assert!(
            !op.window_txns
                .lock()
                .unwrap()
                .contains_key(&(0, "a".to_string())),
            "the empty window's transaction must be discarded"
        );
        // The still-open window for key "b" keeps its staged transaction.
        assert_eq!(journal.pending_transactions(), 1);
        {
            let pending = op.pending_acks.lock().unwrap();
            assert!(
                !pending.contains_key(&(0, "a".to_string())),
                "the NULL-only window's acknowledgement must not strand"
            );
            // The open "b" window legitimately holds its delivery.
            assert!(pending.contains_key(&(2_000, "b".to_string())));
        }
    }

    /// A NULL value must not fabricate a count=0 zero-sentinel aggregate, and
    /// a NULL-only buffer firing next to float aggregates must not truncate
    /// the batch output kind back to Int64.
    #[tokio::test]
    async fn null_values_never_fabricate_or_truncate_aggregates() {
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
        let op = operator(WindowTrigger::Watermark, backend);
        op.process(nullable_float_batch(
            vec![
                (1_000, "a", None),
                (2_000, "b", Some(1.5)),
                (3_000, "b", Some(1.0)),
            ],
            None,
        ))
        .await
        .unwrap();
        let fired = op
            .process(nullable_float_batch(
                vec![(11_000, "b", Some(2.0))],
                Some(10_000),
            ))
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
            .downcast_ref::<datafusion::arrow::array::StringArray>()
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
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .unwrap();
        assert_eq!(
            keys.len(),
            1,
            "the NULL-only key must not emit a phantom aggregate row"
        );
        assert_eq!(keys.value(0), "b");
        assert_eq!(counts.value(0), 2);
        assert_eq!(
            sums.value(0),
            2.5,
            "the float sum must not be truncated to an integer"
        );
        assert_eq!(
            fired
                .record_batch()
                .column_by_name("sum")
                .unwrap()
                .data_type(),
            &DataType::Float64,
        );
    }

    /// A row released by a source gate after the operator's watermark
    /// frontier already fired and cleaned its window must not re-open the
    /// window as a fresh aggregate (duplicate initial emission).
    #[tokio::test]
    async fn released_row_never_reopens_a_fired_and_cleaned_window() {
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(crate::state::RedbStateBackend::open(dir.path(), 1).unwrap());
        let op = operator(WindowTrigger::Watermark, backend);
        op.process(batch(vec![(5_000, "a", 1)], None))
            .await
            .unwrap();
        // Watermark 60_000 fires [0, 10000) and holds [60000, 70000);
        // watermark 95_000 fires [60000, 70000); watermark 99_500 cleans
        // both (allowed_lateness_ms = 0), leaving only [90000, 100000).
        let fired = op
            .process(batch(vec![(61_000, "a", 2)], Some(60_000)))
            .await
            .unwrap();
        assert!(matches!(fired, ProcessResult::Single(_)));
        let fired = op
            .process(batch(vec![(96_000, "a", 3)], Some(95_000)))
            .await
            .unwrap();
        assert!(matches!(fired, ProcessResult::Single(_)));
        let cleaned = op
            .process(batch(vec![(99_600, "a", 4)], Some(99_500)))
            .await
            .unwrap();
        assert!(matches!(cleaned, ProcessResult::None));
        // The gate release race: an unmarked row for a closed window
        // arrives after the frontier moved past it. It must be treated as
        // a late membership, not re-open the window.
        let released = op
            .process(batch(vec![(5_000, "a", 7)], None))
            .await
            .unwrap();
        assert!(matches!(released, ProcessResult::None));
        let refired = op
            .process(batch(vec![(199_600, "a", 5)], Some(199_500)))
            .await
            .unwrap();
        let ProcessResult::Single(refired) = refired else {
            panic!("the held far-future window should fire");
        };
        let starts = refired
            .record_batch()
            .column_by_name("window_start")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for index in 0..starts.len() {
            assert_ne!(
                starts.value(index),
                0,
                "the fired-and-cleaned [0,10000) window must not be re-emitted"
            );
        }
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
                legacy_payload: false,
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

    #[test]
    fn mixed_int_and_float_observations_keep_both_contributions() {
        // Per-batch JSON schema inference routinely makes the same field
        // Int64 in one delivery and Float64 in the next. A buffer that
        // observed both must fold the integer side into the widened
        // aggregate instead of dropping it (or fabricating a 0.0 boundary).
        let mut buffer = AggregateBuffer::default();
        buffer.observe_i64(100);
        buffer.observe_float(99.5, NumericKind::Float64);
        assert_eq!(buffer.count, 2);
        assert_eq!(buffer.kind, NumericKind::Float64);
        assert!((buffer.widened_sum() - 199.5).abs() < 1e-9);
        assert!((buffer.widened_min() - 99.5).abs() < 1e-9);
        assert!((buffer.widened_max() - 100.0).abs() < 1e-9);
    }

    /// Regression: an integer observation that follows a float one must seed
    /// the integer bounds from ITS OWN first value. Seeding from `count`
    /// instead takes the `else` branch against the `Default` zero and the
    /// widened aggregate then reports a fabricated boundary.
    #[test]
    fn integer_observation_after_float_does_not_fabricate_a_zero_boundary() {
        let mut buffer = AggregateBuffer::default();
        buffer.observe_float(99.5, NumericKind::Float64);
        buffer.observe_i64(100);
        assert_eq!(buffer.count, 2);
        assert_eq!(buffer.int_observations, 1);
        assert_eq!(buffer.float_observations, 1);
        assert_eq!(buffer.kind, NumericKind::Float64);
        assert!(
            (buffer.widened_min() - 99.5).abs() < 1e-9,
            "min must be 99.5, got {}",
            buffer.widened_min()
        );
        assert!(
            (buffer.widened_max() - 100.0).abs() < 1e-9,
            "max must be 100.0, got {}",
            buffer.widened_max()
        );

        // The symmetric case: all-negative floats then a negative integer.
        let mut negative = AggregateBuffer::default();
        negative.observe_float(-10.0, NumericKind::Float64);
        negative.observe_i64(-5);
        assert!(
            (negative.widened_max() - (-5.0)).abs() < 1e-9,
            "max must be -5.0, got {}",
            negative.widened_max()
        );
        assert!((negative.widened_min() - (-10.0)).abs() < 1e-9);
    }

    /// Regression: state written before the observation counters existed
    /// decodes with `count > 0` and zeroed counters. The restored range must
    /// survive the next observation instead of being replaced by it.
    #[test]
    fn restored_buffer_keeps_its_range_before_the_next_observation() {
        let persisted = serde_json::json!({
            "count": 2,
            "kind": "float64",
            "sum_i64": 0,
            "sum_float": 8.0,
            "min_i64": 0,
            "max_i64": 0,
            "min_float": 3.0,
            "max_float": 5.0,
            "emitted": false,
            "updated_since_emit": false,
            "session_end_ms": 0
        });
        let bytes = serde_json::to_vec(&persisted).unwrap();
        let mut buffer = decode_buffer(&bytes).unwrap();
        assert_eq!(buffer.float_observations, 2, "counters are back-filled");
        assert_eq!(buffer.count, 2);

        buffer.observe_float(100.0, NumericKind::Float64);
        assert!(
            (buffer.widened_min() - 3.0).abs() < 1e-9,
            "restored min must survive, got {}",
            buffer.widened_min()
        );
        assert!((buffer.widened_max() - 100.0).abs() < 1e-9);

        // A buffer whose counters already describe its state keeps them
        // exactly, including a mixed payload.
        let consistent = serde_json::json!({
            "count": 3,
            "kind": "float64",
            "sum_i64": 4,
            "sum_float": 1.5,
            "min_i64": 4,
            "max_i64": 4,
            "min_float": 0.5,
            "max_float": 1.0,
            "int_observations": 1,
            "float_observations": 2
        });
        let decoded = decode_buffer(&serde_json::to_vec(&consistent).unwrap()).unwrap();
        assert_eq!((decoded.int_observations, decoded.float_observations), (1, 2));
        assert!((decoded.widened_min() - 0.5).abs() < 1e-9);
        assert!((decoded.widened_max() - 4.0).abs() < 1e-9);
    }

    /// Regression: pre-counter state that observed BOTH representations must
    /// keep both. Detecting the integer side from the min/max it accumulated is
    /// what makes this work; a sum-based test would misread a float
    /// contribution that happens to sum to zero (`[-1.0, 1.0]`).
    #[test]
    fn restored_mixed_payload_keeps_both_representations() {
        let persisted = serde_json::json!({
            "count": 3,
            "kind": "float64",
            "sum_i64": 1000,
            "sum_float": 0.0,
            "min_i64": 1000,
            "max_i64": 1000,
            "min_float": -1.0,
            "max_float": 1.0
        });
        let decoded = decode_buffer(&serde_json::to_vec(&persisted).unwrap()).unwrap();
        assert_eq!(
            decoded.int_observations + decoded.float_observations,
            decoded.count,
            "the counters must describe the whole buffer"
        );
        assert!(decoded.int_observations > 0, "the integer side is evidenced");
        assert!(decoded.float_observations > 0, "the float side is evidenced");
        assert!((decoded.widened_min() - (-1.0)).abs() < 1e-9);
        assert!(
            (decoded.widened_max() - 1000.0).abs() < 1e-9,
            "the restored integer maximum must survive, got {}",
            decoded.widened_max()
        );
        assert!((decoded.widened_sum() - 1000.0).abs() < 1e-9);
    }

    /// Regression: a migrated legacy buffer must carry observation counters
    /// consistent with its count, so a later merge folds its contribution
    /// instead of dropping it, and an empty legacy buffer must not contribute
    /// its sentinel bounds to that merge.
    #[test]
    fn migrated_legacy_buffer_merges_its_contribution_and_empty_stays_neutral() {
        let legacy = serde_json::json!({
            "count": 2,
            "sum_i64": 10,
            "sum_float": 0.0,
            "min_i64": 4,
            "max_i64": 6,
            "is_float": false,
            "session_end_ms": 0
        });
        let migrated = decode_buffer(&serde_json::to_vec(&legacy).unwrap()).unwrap();
        assert_eq!(migrated.kind, NumericKind::Int64);
        assert_eq!(migrated.count, 2);
        assert_eq!(migrated.int_observations, 2, "counters describe the payload");

        let mut merged = migrated;
        merged.observe_float(0.5, NumericKind::Float64);
        assert_eq!(merged.kind, NumericKind::Float64);
        assert!(
            (merged.widened_sum() - 10.5).abs() < 1e-9,
            "the migrated integer sum must survive the widening, got {}",
            merged.widened_sum()
        );
        assert!((merged.widened_min() - 0.5).abs() < 1e-9);
        assert!((merged.widened_max() - 6.0).abs() < 1e-9);

        // An empty legacy payload carries i64::MIN / i64::MAX placeholders; a
        // merge must not adopt them as a boundary.
        let empty_legacy = serde_json::json!({
            "count": 0,
            "sum_i64": 0,
            "sum_float": 0.0,
            "min_i64": i64::MIN,
            "max_i64": i64::MAX,
            "is_float": false,
            "session_end_ms": 0
        });
        let mut merged = decode_buffer(&serde_json::to_vec(&empty_legacy).unwrap()).unwrap();
        assert_eq!(merged.count, 0);
        assert_eq!(merged.min_i64, 0, "sentinels do not survive migration");
        merged.observe_i64(7);
        merged.observe_float(2.5, NumericKind::Float64);
        assert!((merged.widened_min() - 2.5).abs() < 1e-9);
        assert!((merged.widened_max() - 7.0).abs() < 1e-9);
    }

    #[test]
    fn merged_buffers_do_not_fabricate_boundaries_from_untouched_kinds() {
        let mut int_only = AggregateBuffer::default();
        int_only.observe_i64(-7);
        let mut float_only = AggregateBuffer::default();
        float_only.observe_float(2.5, NumericKind::Float64);
        int_only.merge(&float_only);
        assert_eq!(int_only.kind, NumericKind::Float64);
        assert!((int_only.widened_sum() - (-4.5)).abs() < 1e-9);
        assert!((int_only.widened_min() - (-7.0)).abs() < 1e-9);
        assert!((int_only.widened_max() - 2.5).abs() < 1e-9);
    }

    #[test]
    fn legacy_float_state_with_sentinel_min_max_fails_to_migrate() {
        // A REAL legacy payload written by the pre-typed kernel: every field
        // present (the typed parse would otherwise succeed with
        // `kind = Int64` and restore fabricated integer sentinels).
        let legacy = serde_json::json!({
            "count": 3,
            "sum_i64": 0,
            "sum_float": 7.5,
            "min_i64": i64::MIN,
            "max_i64": i64::MAX,
            "is_float": true,
            "session_end_ms": 0
        });
        let bytes = serde_json::to_vec(&legacy).unwrap();
        assert!(decode_buffer(&bytes).is_err());
        // Integer legacy aggregates still migrate losslessly.
        let legacy_int = serde_json::json!({
            "count": 2,
            "sum_i64": 9,
            "sum_float": 0.0,
            "min_i64": 4,
            "max_i64": 5,
            "is_float": false,
            "session_end_ms": 0
        });
        let bytes = serde_json::to_vec(&legacy_int).unwrap();
        let migrated = decode_buffer(&bytes).unwrap();
        assert_eq!(migrated.kind, NumericKind::Int64);
        assert_eq!(migrated.sum_i64, 9);
    }

    fn config_with(
        kind: WindowKind,
        legacy_payload: bool,
        value_fields: Vec<String>,
    ) -> WindowOperatorConfig {
        WindowOperatorConfig {
            kind,
            timestamp_field: "ts".into(),
            key_field: "key".into(),
            value_fields,
            trigger: WindowTrigger::Watermark,
            trigger_interval_ms: 1_000,
            watermark_field: "__watermark_ms".into(),
            allowed_lateness_ms: 0,
            legacy_payload,
        }
    }

    #[test]
    fn validate_rejects_pathological_sliding_ratio() {
        let config = config_with(
            WindowKind::Sliding {
                size_ms: 315_360_000_000,
                slide_ms: 1,
            },
            false,
            vec!["value".into()],
        );
        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("more than"), "{error}");
    }

    #[test]
    fn validate_rejects_legacy_sliding_and_multiple_value_fields() {
        let legacy_sliding = config_with(
            WindowKind::Sliding {
                size_ms: 10_000,
                slide_ms: 1_000,
            },
            true,
            vec!["value".into()],
        );
        assert!(legacy_sliding.validate().is_err());
        let multi_value = config_with(
            WindowKind::Tumbling { size_ms: 10_000 },
            false,
            vec!["a".into(), "b".into()],
        );
        assert!(multi_value.validate().is_err());
    }
}
