//! The columnar window operator: vectorized window assignment, keyed
//! aggregate merging, watermark/processing-time firing, and state persistence.

use super::aggregate::{
    decode_buffer, encode_buffer, numeric_array, AggregateBuffer, NumericKind, NumericValue,
};
use super::firing::{
    PersistUndoImage, WindowFiredAck, WindowFiring, WindowKind, WindowOperatorConfig,
    WindowRollback, WindowRuntimeSnapshot, WindowTrigger,
};
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
use datafusion::arrow::record_batch::RecordBatch;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

/// Minimum spacing between two `max_buffered_keys` eviction warn lines.
const WINDOW_EVICTION_LOG_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

/// The batch's (first) value column, resolved once per `accumulate` call:
/// narrow integer columns are already normalized to Int64 (cast once per
/// batch, never per row), and `Count` is the no-value-fields fallback.
enum BatchValueColumn<'a> {
    Count,
    Int(&'a Int64Array),
    Float(&'a datafusion::arrow::array::Float64Array),
    Float32(&'a datafusion::arrow::array::Float32Array),
}

/// The columnar window operator. One instance per stateful operator task;
/// state is namespaced under the operator id so parallel subtasks stay
/// isolated.
pub(crate) struct ColumnarWindowOperator {
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
    journal: Option<Arc<crate::executor::state_journal::StateJournal>>,
    /// One journal transaction per open `(window_start, key)` group; a fired
    /// window's commit rides its emitted output's acknowledgement.
    pub(crate) window_txns:
        Arc<Mutex<BTreeMap<(i64, String), crate::executor::state_journal::StateTxn>>>,
    /// (window_start, key) -> buffer, mirroring the backend lazily.
    pub(crate) buffers: Arc<Mutex<BTreeMap<(i64, String), AggregateBuffer>>>,
    /// Source acknowledgements held until the corresponding aggregate is
    /// successfully written downstream.
    #[allow(clippy::type_complexity)]
    pub(crate) pending_acks: Arc<Mutex<BTreeMap<(i64, String), Vec<Arc<dyn Ack>>>>>,
    pub(crate) watermark_ms: Arc<Mutex<Option<i64>>>,
    pub(crate) last_processing_trigger_ms: Arc<Mutex<Option<i64>>>,
    pub(crate) last_processing_activity_ms: Arc<Mutex<Option<i64>>>,
    loaded: Arc<Mutex<bool>>,
    /// Non-journal persistence only: raw state keys -> prior backend bytes
    /// (None = key absent) captured before the direct backend writes, so a
    /// failed fired acknowledgement can rewind them. Journal-backed operators
    /// compensate through their transactions instead.
    persist_undo: Arc<Mutex<PersistUndoImage>>,
    /// Serialize a window operation through the output acknowledgement. A
    /// fired result changes both the in-memory buffer and the journal; a
    /// second input must not mutate the same window until the first result's
    /// source acknowledgement has either committed or been compensated.
    operation_lock: Arc<tokio::sync::Mutex<()>>,
    /// Throttled observability for `max_buffered_keys` evictions: the first
    /// eviction warns immediately, later lines are rate-limited and carry the
    /// suppressed count (mirrors the join operator's `EvictionThrottle`).
    pub(crate) eviction_log: Mutex<(Option<std::time::Instant>, u64)>,
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
        // Only non-journal operators persist before the acknowledgement; the
        // pre-persist image captured by the last persistence call is the
        // backend undo this rollback owns. Journal-backed compensation runs
        // through the transactions instead.
        let persist_undo = std::mem::take(&mut *self.persist_undo.lock().unwrap());
        Arc::new(WindowRollback {
            buffers: self.buffers.clone(),
            watermark_ms: self.watermark_ms.clone(),
            last_processing_trigger_ms: self.last_processing_trigger_ms.clone(),
            last_processing_activity_ms: self.last_processing_activity_ms.clone(),
            operation_lock: self.operation_lock.clone(),
            backend: self.journal.is_none().then(|| {
                (
                    self.backend.clone(),
                    self.namespace.clone(),
                    Mutex::new(persist_undo),
                )
            }),
            before,
            after,
        })
    }

    #[cfg(test)]
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
    #[cfg(test)]
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
        journal: Option<Arc<crate::executor::state_journal::StateJournal>>,
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
            persist_undo: Arc::new(Mutex::new(BTreeMap::new())),
            operation_lock: Arc::new(tokio::sync::Mutex::new(())),
            eviction_log: Mutex::new((None, 0)),
        }
    }

    /// Build the operator with output-gated state commits.
    #[cfg(test)]
    pub fn with_journal(
        config: WindowOperatorConfig,
        backend: Arc<dyn StateBackend>,
        journal: Arc<crate::executor::state_journal::StateJournal>,
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
        journal: Arc<crate::executor::state_journal::StateJournal>,
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
    fn window_txn(
        &self,
        key: &(i64, String),
    ) -> Result<crate::executor::state_journal::StateTxn, Error> {
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

    /// Enforce the `max_buffered_keys` entry bound: evict whole buffers,
    /// oldest `window_start` first. Evicted aggregates are LOST (announced by
    /// a throttled warn) — strictly better than an out-of-memory kill under a
    /// stalled watermark or extreme key cardinality. Entries that still hold
    /// pending acknowledgements are skipped: dropping them would strand
    /// delivery acknowledgements and freeze the checkpoint frontier.
    /// Journaled cleanup rides the existing stale-key sweep in
    /// `persist_buffers_for_fired` (an evicted key is no longer "current").
    /// Returns the evicted keys so the caller can drop them from its
    /// touched-key set instead of staging fresh transactions for them.
    /// Core eviction: drop whole buffers oldest-window-first until at or
    /// under the cap, skipping every key in `protected` (pending
    /// acknowledgements, current-batch memberships, and session re-key
    /// targets — evicting any of those either strands an acknowledgement on
    /// a missing buffer or drops the very aggregate this batch is building).
    /// When protection covers everything the eviction is best-effort and
    /// the map may stay over cap; that overshoot is bounded by one batch's
    /// insertions and beats the alternatives (stranding or dropping the
    /// live batch).
    fn evict_overflowed_buffers(
        &self,
        buffers: &mut BTreeMap<(i64, String), AggregateBuffer>,
        protected: &BTreeSet<(i64, String)>,
    ) -> BTreeSet<(i64, String)> {
        let cap = self.config.max_buffered_keys;
        if buffers.len() <= cap {
            return BTreeSet::new();
        }
        let mut evicted_keys = BTreeSet::new();
        while buffers.len() > cap {
            let Some(victim) = buffers
                .keys()
                .find(|key| !protected.contains(*key))
                .cloned()
            else {
                break;
            };
            buffers.remove(&victim);
            self.rollback_window_txn(&victim);
            evicted_keys.insert(victim);
        }
        if evicted_keys.is_empty() {
            return evicted_keys;
        }
        let mut log = self.eviction_log.lock().unwrap();
        let suppressed = log.1 + evicted_keys.len() as u64;
        let now = std::time::Instant::now();
        if log
            .0
            .is_none_or(|last| now.duration_since(last) >= WINDOW_EVICTION_LOG_INTERVAL)
        {
            log.0 = Some(now);
            log.1 = 0;
            drop(log);
            tracing::warn!(
                namespace = %self.namespace,
                evicted = evicted_keys.len(),
                suppressed = suppressed,
                depth = buffers.len(),
                cap = cap,
                "window aggregate buffer exceeded max_buffered_keys; oldest-window aggregates were dropped"
            );
        } else {
            log.1 = suppressed;
        }
        evicted_keys
    }

    /// The pending-acknowledgement half of the eviction protection. Callers
    /// merge in their own live keys (current-batch memberships / re-key
    /// targets) on top.
    fn pending_protection(&self) -> BTreeSet<(i64, String)> {
        self.pending_acks
            .lock()
            .unwrap()
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
    }

    /// All windows containing one event time. Tumbling yields one;
    /// sliding yields every containing window; session yields
    /// its gap-extended window (tracked per key in the buffer map).
    pub(crate) fn windows_for(&self, event_time_ms: i64) -> Vec<(i64, i64)> {
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

    pub(crate) fn state_key(window_start: i64, key: &str) -> Vec<u8> {
        let mut bytes = window_start.to_be_bytes().to_vec();
        bytes.extend_from_slice(key.as_bytes());
        bytes
    }

    pub(crate) fn session_end(window_start: i64, buffer: &AggregateBuffer, gap_ms: i64) -> i64 {
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
    #[allow(clippy::type_complexity)]
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
                // A pure drop and a routed late row both leave the window and
                // are reported through the late mask; only an in-place update
                // keeps the row. (`!update_existing || route` is exactly the
                // old two-branch condition with the shared body factored out.)
                if !update_existing || route {
                    keep[row] = false;
                    late[row] = true;
                }
            }
        }
        Ok((keep, late, invalid))
    }

    /// Rows with a convertible event timestamp but a NULL key: they can
    /// never join a keyed aggregate and must follow the explicit
    /// late/invalid policy (count, route, or drop+ack) instead of being
    /// silently skipped.
    fn null_key_mask(&self, batch: &crate::MessageBatchRef) -> Result<Vec<bool>, Error> {
        let timestamps = self.extract_timestamps(batch)?;
        let keys = self.extract_keys(batch)?;
        Ok(timestamps
            .iter()
            .zip(keys.iter())
            .map(|(event_time, key)| event_time.is_some() && key.is_none())
            .collect())
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
        let mut evicted_inline: BTreeSet<(i64, String)> = BTreeSet::new();
        // Pending-acknowledgement half of the eviction protection, built on
        // the first over-cap trigger only: it cannot change mid-batch (the
        // session re-key runs after the loop), and cloning it per membership
        // would make a degraded sliding batch quadratic.
        let mut protection_base: Option<BTreeSet<(i64, String)>> = None;
        let mut legacy_rows = BTreeMap::<(i64, String), Vec<usize>>::new();
        // Normalize the (first) value column once per batch. Narrow integer
        // columns used to be re-cast to Int64 inside the row loop — per row
        // AND per window membership — which made wide batches quadratic.
        // The cast preserves the validity bitmap, so null handling is
        // identical to per-row casting.
        let narrow_int_owned: Option<Int64Array>;
        let value_column = match value_columns.first() {
            None => BatchValueColumn::Count,
            Some(column) => match column.data_type() {
                DataType::Int64 => {
                    BatchValueColumn::Int(column.as_any().downcast_ref::<Int64Array>().unwrap())
                }
                DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64 => {
                    let casted = cast(column, &DataType::Int64)
                        .map_err(|error| Error::Process(format!("cast window value: {error}")))?;
                    narrow_int_owned = Some(Int64Array::from(casted.to_data()));
                    BatchValueColumn::Int(narrow_int_owned.as_ref().unwrap())
                }
                DataType::Float64 => BatchValueColumn::Float(
                    column
                        .as_any()
                        .downcast_ref::<datafusion::arrow::array::Float64Array>()
                        .unwrap(),
                ),
                DataType::Float32 => BatchValueColumn::Float32(
                    column
                        .as_any()
                        .downcast_ref::<datafusion::arrow::array::Float32Array>()
                        .unwrap(),
                ),
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
            },
        };
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
                match value_column {
                    BatchValueColumn::Count => {
                        entry.observe_i64(1);
                    }
                    BatchValueColumn::Int(values) => {
                        if !values.is_null(row) {
                            entry.observe_i64(values.value(row));
                        }
                    }
                    BatchValueColumn::Float(values) => {
                        if !values.is_null(row) {
                            entry.observe_float(values.value(row), NumericKind::Float64);
                        }
                    }
                    BatchValueColumn::Float32(values) => {
                        if !values.is_null(row) {
                            entry.observe_float(f64::from(values.value(row)), NumericKind::Float32);
                        }
                    }
                }
                // Enforce the entry cap DURING accumulation (after this
                // membership's mutation): a wide sliding batch can create
                // millions of memberships inside one call, and deferring
                // eviction to the end would let that transient blow past the
                // cap (and the heap) before it runs. `touched` already
                // contains this membership, so the current batch is never
                // its own victim. The per-row membership fan-out itself is
                // bounded at validate time (pathological sliding ratios are
                // rejected).
                if buffers.len() > self.config.max_buffered_keys {
                    let base = protection_base.get_or_insert_with(|| self.pending_protection());
                    let mut protection = base.clone();
                    protection.extend(touched.iter().cloned());
                    evicted_inline.extend(self.evict_overflowed_buffers(&mut buffers, &protection));
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
        // Evict AFTER the session re-key above: a merged session's
        // acknowledgements move to the merged key here, so the eviction sees
        // them under the merged key and can never strand them on an evicted
        // buffer (stranded acknowledgements freeze the checkpoint frontier —
        // the barrier drain waits for them forever). The current batch's
        // memberships are protected too: evicting one would acknowledge the
        // delivery while dropping the aggregate it just built.
        let mut evicted_keys = evicted_inline;
        {
            let mut buffers = self.buffers.lock().unwrap();
            if buffers.len() > self.config.max_buffered_keys {
                let base = protection_base.get_or_insert_with(|| self.pending_protection());
                let mut protection = base.clone();
                protection.extend(touched.iter().cloned());
                evicted_keys.extend(self.evict_overflowed_buffers(&mut buffers, &protection));
            }
        }
        // A key evicted mid-batch can be re-created by a later row; only
        // keys with no surviving buffer drop out of `touched` (their
        // acknowledgements must not be held for an aggregate that no longer
        // exists).
        let survivors: BTreeSet<(i64, String)> =
            self.buffers.lock().unwrap().keys().cloned().collect();
        let touched = touched
            .into_iter()
            .filter(|key| !evicted_keys.contains(key) || survivors.contains(key))
            .collect::<Vec<_>>();
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
        // unemitted aggregate. A buffer that still owes an unemitted
        // correction (`updated_since_emit`) is not reclaimable either,
        // including at end-of-stream (threshold = i64::MAX): the correction
        // fires through the ready path below before any reclaim.
        let expired = buffers
            .iter()
            .filter(|((start, _), buffer)| {
                let end = end_of(*start, buffer);
                buffer.emitted
                    && !buffer.updated_since_emit
                    && threshold.saturating_sub(end) > lateness
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

pub(crate) fn filter_window_batch(
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
pub(crate) fn mark_late_session_batch(
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
    if let Ok(index) = batch.schema().index_of(marker) {
        columns[index] = route_values;
    } else {
        fields.push(Arc::new(Field::new(marker, DataType::Boolean, false)));
        columns.push(route_values);
    }
    if invalid_timestamps.iter().any(|invalid| *invalid) {
        if let Ok(index) = batch.schema().index_of(invalid_marker) {
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
pub(crate) fn append_late_session_output(
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

pub(crate) async fn compensate_window_acks(
    error: Error,
    acknowledgements: Vec<Arc<dyn Ack>>,
) -> Error {
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
        let (mut keep, mut late, invalid_timestamps) = self.session_late_masks(&batch)?;
        // A row with a convertible timestamp but a NULL key can never join a
        // keyed aggregate. Like an invalid timestamp it follows an explicit
        // policy instead of being silently skipped (and silently
        // acknowledged): it is counted in the late/invalid metrics, routed to
        // the configured side output, or dropped with its acknowledgement
        // settled.
        let null_key_late = self.null_key_mask(&batch)?;
        for ((keep_row, late_row), null_key) in keep
            .iter_mut()
            .zip(late.iter_mut())
            .zip(null_key_late.iter())
        {
            if *null_key {
                *keep_row = false;
                *late_row = true;
            }
        }
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
                crate::executor::state_journal::CommitGroupOnAck::new(journal, txns, source_ack),
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
    #[cfg(test)]
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
            let mut cleanup =
                BTreeMap::<crate::executor::state_journal::StateTxn, Vec<Vec<u8>>>::new();
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
        // Capture the pre-persist backend image so a failed fired
        // acknowledgement can rewind these direct writes. Deleted stale keys
        // are recorded too (their prior bytes) and replacements in `current`
        // overwrite the deletion entry.
        let mut prior_image = PersistUndoImage::new();
        for entry in &existing {
            let key = Self::decode_state_key(&entry.key)?;
            if !current.contains_key(&key) {
                prior_image.insert(entry.key.clone(), Some(entry.value.clone()));
            }
        }
        for (window_start, key) in current.keys() {
            let raw = Self::state_key(*window_start, key);
            let prior = self.backend.get(&self.namespace, &raw).ok().flatten();
            prior_image.insert(raw, prior);
        }
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
        *self.persist_undo.lock().unwrap() = prior_image;
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
    #[cfg(test)]
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
