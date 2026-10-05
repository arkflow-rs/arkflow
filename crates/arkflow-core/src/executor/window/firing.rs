//! Window firing machinery: trigger and window-shape configuration, the
//! runtime snapshot/rollback pair used to undo failed firings, the fired-output
//! acknowledgement wrapper, and the firing result type.

use super::aggregate::AggregateBuffer;
use crate::input::Ack;
use crate::state::StateBackend;
use crate::Error;
use crate::MessageBatchRef;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// Trigger policy for a window operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WindowTrigger {
    /// Emit when the watermark passes the window end (event-time mode).
    Watermark,
    /// Emit on an interval regardless of watermark state (legacy
    /// processing-time buffer compatibility mode).
    ProcessingTime,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum WindowKind {
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
pub(crate) struct WindowOperatorConfig {
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
    /// Bound on the number of live `(window_start, key)` aggregate entries.
    /// When the bound is reached the oldest-window entries are evicted (their
    /// aggregates are lost, announced by a throttled warn) instead of growing
    /// without limit under a stalled watermark or high key cardinality.
    #[serde(default = "default_max_buffered_keys")]
    pub max_buffered_keys: usize,
}

pub(crate) fn default_max_buffered_keys() -> usize {
    65_536
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
        if self.max_buffered_keys == 0 {
            return Err(Error::Config(
                "window max_buffered_keys must be positive".into(),
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

/// Upper bound on the memberships one event can join in a sliding window
/// (enforced by `WindowOperatorConfig::validate`).
const MAX_SLIDING_MEMBERSHIPS_PER_EVENT: i64 = 10_000;

fn default_trigger() -> WindowTrigger {
    WindowTrigger::Watermark
}

fn default_trigger_interval_ms() -> u64 {
    5_000
}

fn default_watermark_field() -> String {
    "__watermark_ms".to_string()
}

/// Raw state key -> prior backend bytes (None = key absent): the undo image
/// one non-journal persistence captures for a failed fired acknowledgement.
pub(crate) type PersistUndoImage = BTreeMap<Vec<u8>, Option<Vec<u8>>>;

#[derive(Clone)]
pub(crate) struct WindowRuntimeSnapshot {
    pub(crate) buffers: BTreeMap<(i64, String), AggregateBuffer>,
    pub(crate) watermark_ms: Option<i64>,
    pub(crate) last_processing_trigger_ms: Option<i64>,
    pub(crate) last_processing_activity_ms: Option<i64>,
}

pub(crate) struct WindowRollback {
    pub(crate) buffers: Arc<Mutex<BTreeMap<(i64, String), AggregateBuffer>>>,
    pub(crate) watermark_ms: Arc<Mutex<Option<i64>>>,
    pub(crate) last_processing_trigger_ms: Arc<Mutex<Option<i64>>>,
    pub(crate) last_processing_activity_ms: Arc<Mutex<Option<i64>>>,
    pub(crate) operation_lock: Arc<tokio::sync::Mutex<()>>,
    /// Non-journal construction only: backend, namespace, and the pre-persist
    /// backend image `persist_buffers_for_fired` captured before writing the
    /// fired buffers. Journal-backed operators compensate through their
    /// transactions and leave this as `None`.
    pub(crate) backend: Option<(Arc<dyn StateBackend>, String, Mutex<PersistUndoImage>)>,
    pub(crate) before: WindowRuntimeSnapshot,
    pub(crate) after: WindowRuntimeSnapshot,
}

impl WindowRollback {
    fn restore(&self, snapshot: &WindowRuntimeSnapshot) {
        *self.buffers.lock().unwrap() = snapshot.buffers.clone();
        *self.watermark_ms.lock().unwrap() = snapshot.watermark_ms;
        *self.last_processing_trigger_ms.lock().unwrap() = snapshot.last_processing_trigger_ms;
        *self.last_processing_activity_ms.lock().unwrap() = snapshot.last_processing_activity_ms;
    }

    /// Rewind the backend entries the last non-journal persistence wrote
    /// back to their pre-persist bytes. Non-journal persistence commits
    /// fired buffers before the source acknowledgement, so a failed
    /// acknowledgement must also undo those bytes — a memory-only rollback
    /// would leave `emitted` aggregates in the backend and a replay would
    /// merge rows into them a second time. Best-effort: a failed rewind
    /// surfaces through the retryable acknowledgement error the caller is
    /// already handling.
    fn restore_persist_undo(&self) {
        let Some((backend, namespace, undo)) = &self.backend else {
            return;
        };
        let undo = { std::mem::take(&mut *undo.lock().unwrap()) };
        for (raw, prior) in undo {
            match prior {
                Some(bytes) => {
                    let _ = backend.put(namespace, &raw, &bytes);
                }
                None => {
                    let _ = backend.delete(namespace, &raw);
                }
            }
        }
    }

    /// The fired output acknowledgement succeeded: its backend writes stand,
    /// so drop the stale undo image.
    fn clear_persist_undo(&self) {
        if let Some((_, _, undo)) = &self.backend {
            undo.lock().unwrap().clear();
        }
    }
}

/// Holds the window operation lock until its emitted output and all source
/// acknowledgements finish. If that acknowledgement fails, the journal rolls
/// back the durable mutation and this wrapper restores the working buffers and
/// watermark, so a replay cannot accumulate the same row twice in memory.
pub(crate) struct WindowFiredAck {
    pub(crate) inner: Arc<dyn Ack>,
    pub(crate) rollback: Arc<WindowRollback>,
    pub(crate) operation_guard: Mutex<Option<tokio::sync::OwnedMutexGuard<()>>>,
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
                self.rollback.clear_persist_undo();
                drop(guard);
                Ok(())
            }
            Err(error) => {
                self.rollback.restore(&self.rollback.before);
                self.rollback.restore_persist_undo();
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
        self.rollback.restore_persist_undo();
        drop(guard);
        result
    }

    async fn abort(&self) -> Result<(), Error> {
        self.inner.release_held();
        let guard = self.take_guard().await;
        let result = self.inner.abort().await;
        self.rollback.restore(&self.rollback.before);
        self.rollback.restore_persist_undo();
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

/// The outcome of one window firing round.
pub(crate) struct WindowFiring {
    /// Aggregate rows to emit together with the window keys they belong to.
    pub(crate) output: Option<(MessageBatchRef, Vec<(i64, String)>)>,
    /// Groups that emit nothing — expired windows and empty (never-observed)
    /// buffers — but whose staged transactions were discarded and whose
    /// delivery acknowledgements the caller must still settle.
    pub(crate) dropped_keys: Vec<(i64, String)>,
}
