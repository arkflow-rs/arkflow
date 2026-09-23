/*
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

//! Write-ahead log (WAL) for durable input ingestion.
//!
//! Each message read from an input is persisted (`append`) and assigned a
//! monotonically increasing sequence number before it enters the pipeline.
//! The committed cursor is advanced (`advance`) only after the downstream
//! output confirms the write, so a crash between `append` and `advance` leaves
//! the entry in the WAL and it is replayed on recovery (`read_after_cursor`).
//!
//! `Wal` owns the batching layer (`pending`, flusher task, cursor atomic,
//! per-entry / group-commit / periodic policy). Storage is delegated to a
//! pluggable [`WalStore`] backend. The default backend (registered in this
//! crate) is an embedded `redb` database. The `s3` backend is provided by
//! `arkflow-plugin` and is opt-in via `backend: s3`. Per-entry writes commit
//! (and fsync) a transaction per append. `group-commit` and `periodic` policies
//! coalesce concurrent appends into shared transactions to amortize the
//! fsync / PUT cost. Callers that hand an appended record to the pipeline
//! flush it before returning the record; the background flusher remains useful
//! for callers that explicitly stage writes.

pub mod config;
pub mod store;

pub use config::WalBackend;
pub use store::{
    build_wal_store, ensure_local_store_registered, lookup_wal_store_builder,
    register_wal_store_builder, registered_wal_store_count, RedbStore, WalStore, WalStoreBuilder,
};

use crate::wal::store::serialize;
use crate::{Error, MessageBatchRef};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Sync (fsync) policy for appends.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum SyncPolicy {
    /// Commit (fsync) a transaction on every append. Fully durable; slowest.
    /// Not supported on remote backends (one PUT per message is not viable).
    PerEntry,
    /// Coalesce concurrent appends into shared transactions flushed as soon as
    /// pending data is available.
    #[default]
    GroupCommit,
    /// Flush pending appends on a fixed interval.
    Periodic(Duration),
}

fn default_enabled() -> bool {
    true
}

fn default_path() -> String {
    String::new()
}

/// Configuration for a durable ingest WAL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalConfig {
    /// Whether durability is active for the stream. Defaults to `true` so that
    /// adding a `durability:` section enables it; set `false` to disable
    /// without removing the section. A stream with no `durability:` section at
    /// all is not durable (today's in-memory behavior).
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Directory in which to store the WAL database file. Used by the local
    /// (`redb`) backend only; ignored when `backend` is set to a non-local
    /// kind.
    #[serde(default = "default_path")]
    pub path: String,
    #[serde(default)]
    pub sync: SyncPolicy,
    /// Storage backend selection. `None` (legacy default) is treated as
    /// `Some(Local { path, sync })` so old configs keep working unchanged.
    #[serde(default)]
    pub backend: Option<WalBackend>,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            path: String::new(),
            sync: SyncPolicy::default(),
            backend: None,
        }
    }
}

impl WalConfig {
    /// Build a local-backed `WalConfig` (used by tests and by stream
    /// construction code that doesn't go through YAML).
    pub fn local(enabled: bool, path: String, sync: SyncPolicy) -> Self {
        Self {
            enabled,
            path,
            sync,
            backend: None,
        }
    }

    /// Validate this config at load time. Returns `Err` when a backend-
    /// specific combination is forbidden (D8: `sync: per_entry` is not
    /// viable on the object-store backend because it would mean one PUT
    /// per message) or when required fields are missing (D2: `node_id`
    /// and `stream_id` are non-empty).
    ///
    /// Called from `Wal::open` before the builder is invoked, so a bad
    /// config surfaces as a `Config` error rather than a runtime panic.
    pub fn validate(&self) -> Result<(), Error> {
        match &self.backend {
            None | Some(WalBackend::Local { .. }) => Ok(()),
            Some(WalBackend::ObjectStore(o)) => {
                if o.node_id.trim().is_empty() {
                    return Err(Error::Config(
                        "durability.backend.object_store: node_id is required \
                         and must be non-empty (D2: the id names the WAL \
                         namespace inside the bucket and must survive \
                         restarts)"
                            .into(),
                    ));
                }
                if o.stream_id.trim().is_empty() {
                    return Err(Error::Config(
                        "durability.backend.object_store: stream_id is required \
                         and must be non-empty"
                            .into(),
                    ));
                }
                if o.s3.bucket.trim().is_empty() {
                    return Err(Error::Config(
                        "durability.backend.object_store.s3.bucket is required".into(),
                    ));
                }
                if matches!(o.sync, SyncPolicy::PerEntry) {
                    return Err(Error::Config(
                        "durability.backend.object_store: sync: per_entry is \
                         not supported (one PUT per message is not viable; \
                         use group_commit or periodic; see D8)"
                            .into(),
                    ));
                }
                // Parallel PUT workers validation (task 3.12)
                if o.parallel_put.workers == 0 {
                    return Err(Error::Config(
                        "durability.backend.object_store.parallel_put.workers \
                         must be positive (1-8)"
                            .into(),
                    ));
                }
                if o.parallel_put.workers > 8 {
                    return Err(Error::Config(format!(
                        "durability.backend.object_store.parallel_put.workers \
                         {} is out of range (1-8)",
                        o.parallel_put.workers
                    )));
                }
                // Compression level validation (task 5.4)
                match &o.compression {
                    crate::wal::config::CompressionConfig::Zstd { level } => {
                        if !(0..=22).contains(level) {
                            return Err(Error::Config(format!(
                                "durability.backend.object_store.compression.zstd.level \
                                 {} is out of range (0-22)",
                                level
                            )));
                        }
                    }
                    crate::wal::config::CompressionConfig::Lz4 { level } => {
                        if !(1..=16).contains(level) {
                            return Err(Error::Config(format!(
                                "durability.backend.object_store.compression.lz4.level \
                                 {} is out of range (1-16)",
                                level
                            )));
                        }
                    }
                    crate::wal::config::CompressionConfig::None => {}
                }
                Ok(())
            }
        }
    }

    /// The local-path when this config resolves to the local backend.
    /// Returns `None` if the selected backend is not `local`.
    pub fn local_path(&self) -> Option<&str> {
        match &self.backend {
            None => Some(&self.path),
            Some(WalBackend::Local { path, .. }) => Some(path),
            Some(_) => None,
        }
    }

    /// The sync policy that applies to this config. The `Local` variant
    /// carries its own; for `ObjectStore` the policy lives on the variant
    /// itself; for the legacy flat shape it's the top-level `sync` field.
    pub fn effective_sync(&self) -> &SyncPolicy {
        match &self.backend {
            None => &self.sync,
            Some(WalBackend::Local { sync, .. }) => sync,
            Some(WalBackend::ObjectStore(o)) => &o.sync,
        }
    }

    /// Backend kind name. Defaults to `"local"` so legacy configs (no
    /// `backend:`) dispatch through the local builder.
    pub fn backend_kind(&self) -> &'static str {
        match &self.backend {
            None => "local",
            Some(b) => b.kind(),
        }
    }
}

/// A durable write-ahead log for input messages.
///
/// `Wal` is a coordinator: it stages appends into `pending`, drains them as a
/// `Vec<(u64, Vec<u8>)>` to `store.append_batch`, and delegates cursor
/// advancement and recovery reads to the store. Storage is owned by the
/// pluggable [`WalStore`].
pub struct Wal {
    /// Pluggable storage backend. `RedbStore` for local; `S3Store` (or
    /// equivalent) for S3-compatible object storage.
    store: Arc<dyn WalStore>,
    /// Contiguous source-delivery acknowledgement frontier. WAL sequence N
    /// maps to the exclusive next offset N+1 in this topic-less partition.
    frontier: Arc<crate::executor::commit::CommitFrontier>,
    /// Keeps the underlying source acknowledgements for out-of-order WAL
    /// completions until every earlier sequence has completed too.  An entry
    /// is processed only by the caller that owns that sequence; a gap-closing
    /// acknowledgement must never drain a later entry on its behalf.
    acknowledgements: Mutex<BTreeMap<u64, PendingWalAck>>,
    /// Next sequence number to assign. Append is single-threaded (the input
    /// worker), but an atomic keeps it race-free regardless.
    next_seq: AtomicU64,
    policy: SyncPolicy,
    // --- staging for group-commit / periodic ---
    pending: Mutex<Vec<(u64, Vec<u8>)>>,
    pending_notify: Notify,
    /// Serialize background and explicit flushes.  Without this guard an
    /// explicit read-side flush could observe an empty pending queue while a
    /// background flusher had already taken the batch but was still writing
    /// it, violating the durable-before-read boundary.
    flush_lock: tokio::sync::Mutex<()>,
    /// Wakes acknowledgements that are waiting for an earlier WAL sequence
    /// to finish its source-side commit and cursor advance.
    ack_notify: Notify,
    close: CancellationToken,
    flusher: Mutex<Option<JoinHandle<()>>>,
}

struct PendingWalAck {
    ack: Arc<dyn crate::input::Ack>,
    in_flight: bool,
    /// The source-side commit failed after this sequence became the cursor
    /// frontier. Keep the failed sequence registered as a fence so later
    /// acknowledgements fail promptly instead of waiting forever for a
    /// sequence that will not advance until its caller retries.
    last_error: Option<String>,
}

/// How long a parked WAL acknowledgement keeps waiting for an earlier
/// in-flight delivery to settle after a close request fires, before the
/// pending-error path takes over (recovery replays unsettled entries).
const WAL_CLOSE_DRAIN: Duration = Duration::from_secs(15);

impl Wal {
    /// Open (or create) a WAL.
    ///
    /// Dispatches to the registered `WalStoreBuilder` for `config.backend_kind()`.
    /// For legacy configs with `backend == None`, the local `redb` builder is
    /// used. To use a plugin-provided backend (e.g. S3), register the builder
    /// before calling this function.
    ///
    /// Synchronous: the registry and the local builder are synchronous. The
    /// S3 builder (plugin) constructs a client synchronously inside its
    /// `build()` and only defers network I/O to `append_batch`/`close`.
    pub fn open(config: &WalConfig) -> Result<Arc<Self>, Error> {
        config.validate()?;
        let store = build_wal_store(config)?;
        // Derive next_seq from the store. The default `next_seq_hint()` uses
        // `cursor() + 1`, which under-counts for local redb after a restart
        // where the cursor advanced past the tail of the table; `RedbStore`
        // overrides it to use `max_seq() + 1`. S3 (and any future remote
        // backend) keeps the default, since the segment index already covers
        // every written entry and the recovery `LIST` fallback re-discovers
        // them on startup.
        let next_seq = store.next_seq_hint().max(1);
        Self::open_with_store(config, store, next_seq)
    }

    /// Open a WAL backed by a caller-provided [`WalStore`] with an explicit
    /// `next_seq`. Used by plugins when their store has a more precise
    /// "next sequence" derivation than `cursor() + 1`.
    pub fn open_with_store(
        config: &WalConfig,
        store: Arc<dyn WalStore>,
        next_seq: u64,
    ) -> Result<Arc<Self>, Error> {
        let sync_policy = config.effective_sync().clone();

        let frontier = Arc::new(crate::executor::commit::CommitFrontier::new());
        frontier.seed(&[crate::checkpoint::SourcePosition::for_partition(
            0,
            store.cursor().saturating_add(1),
        )]);
        let wal = Arc::new(Self {
            store,
            frontier,
            acknowledgements: Mutex::new(BTreeMap::new()),
            next_seq: AtomicU64::new(next_seq),
            policy: sync_policy,
            pending: Mutex::new(Vec::new()),
            pending_notify: Notify::new(),
            flush_lock: tokio::sync::Mutex::new(()),
            ack_notify: Notify::new(),
            close: CancellationToken::new(),
            flusher: Mutex::new(None),
        });

        match &wal.policy {
            SyncPolicy::PerEntry => {}
            SyncPolicy::GroupCommit | SyncPolicy::Periodic(_) => {
                let handle = Self::spawn_flusher(wal.clone());
                *wal.flusher.try_lock().unwrap() = Some(handle);
            }
        }

        Ok(wal)
    }

    fn spawn_flusher(wal: Arc<Wal>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let interval = match &wal.policy {
                SyncPolicy::Periodic(d) => Some(*d),
                _ => None,
            };
            loop {
                let wait = async {
                    if let Some(d) = interval {
                        tokio::time::sleep(d).await;
                    } else {
                        wal.pending_notify.notified().await;
                    }
                };
                tokio::select! {
                    biased;
                    _ = wal.close.cancelled() => {
                        let _ = wal.flush_pending().await;
                        break;
                    }
                    _ = wait => {
                        let _ = wal.flush_pending().await;
                    }
                }
            }
        })
    }

    /// Persist a message and return its assigned sequence number.
    ///
    /// `per-entry` commits (fsyncs) before returning — fully durable. `group-
    /// commit` and `periodic` stage the entry; callers that need a durable
    /// hand-off should call [`Wal::flush`] before publishing the record to the
    /// pipeline. The background flusher still batches explicitly staged
    /// appends.
    ///
    /// The store's blocking calls (redb `commit`, S3 `PUT`) are wrapped in
    /// `spawn_blocking` for `per-entry` to keep the async executor from
    /// stalling on fsync / network I/O.
    pub async fn append(&self, msg: &MessageBatchRef) -> Result<u64, Error> {
        let seq = self.next_seq.fetch_add(1, Ordering::AcqRel);
        let bytes = serialize(msg)?;

        match &self.policy {
            SyncPolicy::PerEntry => {
                // redb's `commit` is briefly blocking (~µs–ms); the
                // multi-thread tokio runtime that arkflow ships with handles
                // this fine. We don't `spawn_blocking` here because the
                // blocking thread pool can deadlock against redb's fcntl
                // flock on the database file (the close path also touches
                // it). The local backend is fast enough; the S3 backend will
                // be added later and will do its own async PUT inside its
                // store.
                self.store.append_batch(vec![(seq, bytes)])?;
            }
            SyncPolicy::GroupCommit | SyncPolicy::Periodic(_) => {
                self.pending.lock().await.push((seq, bytes));
                self.pending_notify.notify_one();
            }
        }
        Ok(seq)
    }

    /// Advance the committed cursor to `seq` (monotonic). Called by the ack
    /// path only after the downstream output confirms the write. Direct users
    /// of this compatibility method also get the same contiguous frontier
    /// semantics; source-side ack objects use [`Wal::acknowledge`].
    pub async fn advance(&self, seq: u64) -> Result<(), Error> {
        let _guard = self.acknowledgements.lock().await;
        self.frontier
            .acknowledge(&crate::checkpoint::SourcePosition::for_partition(
                0,
                seq.saturating_add(1),
            ));
        let target = self
            .frontier
            .next_offset_of(None, 0)
            .unwrap_or_default()
            .saturating_sub(1);
        if target > self.store.cursor() {
            // Cursor advancement only. Reclaiming here would delete entries
            // this path has no wrapped source commit for, and the trait's
            // contract keeps every entry above the reclaim floor replayable for
            // a cursor compensation — so only `acknowledge`, after the wrapped
            // source commit succeeds, marks a sequence committed.
            self.store.advance_cursor(target)?;
            self.ack_notify.notify_waiters();
        }
        Ok(())
    }

    /// Complete one WAL delivery. Every sequence is processed by its own
    /// caller, strictly after all earlier registered sequences have finished.
    /// The WAL cursor is advanced before the wrapped source acknowledgement so
    /// the two durable cursors follow the documented commit ordering. If the
    /// source-side commit fails, the cursor is compensated, the entry stays
    /// retryable, and nothing is reclaimed — entries below the committed floor
    /// are removed only once their source commit has succeeded. No later
    /// sequence is allowed to run while this one is in-flight.
    async fn acknowledge(&self, seq: u64, inner: Arc<dyn crate::input::Ack>) -> Result<(), Error> {
        // Bounded graceful-drain window for parked acknowledgements once a
        // close request fires.
        let mut drain_deadline: Option<Instant> = None;
        {
            let mut acknowledgements = self.acknowledgements.lock().await;
            acknowledgements.entry(seq).or_insert(PendingWalAck {
                ack: inner,
                in_flight: false,
                last_error: None,
            });
        }

        loop {
            let notified = self.ack_notify.notified();
            let work = {
                let mut acknowledgements = self.acknowledgements.lock().await;
                let first_seq = acknowledgements.keys().next().copied();
                let blocked_error = if first_seq != Some(seq) {
                    acknowledgements
                        .values()
                        .next()
                        .and_then(|entry| entry.last_error.clone())
                } else {
                    None
                };
                let Some(entry) = acknowledgements.get_mut(&seq) else {
                    // Another concurrent caller for the same sequence may
                    // have completed it. Its success is shared.
                    return Ok(());
                };

                // A later delivery must not inherit the result of an earlier
                // delivery's source failure. Return a retryable error to the
                // later caller while preserving both entries so the earlier
                // caller can retry and the later caller can retry afterwards.
                if let Some(error) = blocked_error {
                    return Err(Error::Process(format!(
                        "WAL acknowledgement is blocked by an earlier source failure: {error}"
                    )));
                }

                // Only the lowest outstanding sequence may run. This remains
                // true even after the WAL cursor has been advanced before
                // its source-side acknowledgement: later callers must not
                // overtake an in-flight earlier source commit.
                let cursor = self.store.cursor();
                if first_seq != Some(seq)
                    || entry.in_flight
                    // A caller may acknowledge a later WAL sequence before
                    // the earlier delivery has even reached its sink. Keep
                    // that acknowledgement parked until the missing
                    // sequence is registered and committed; otherwise the
                    // source cursor would skip the gap.
                    || (cursor < seq && cursor.saturating_add(1) != seq)
                {
                    None
                } else {
                    // This is a retry of the lowest failed sequence. Clear
                    // the fence only for the attempt that is about to run;
                    // another caller for a later sequence remains parked.
                    entry.last_error = None;
                    let cursor_advanced = if cursor < seq {
                        self.store.advance_cursor(seq)?;
                        true
                    } else {
                        false
                    };
                    entry.in_flight = true;
                    Some((entry.ack.clone(), cursor_advanced))
                }
            };

            match work {
                Some((ack, cursor_advanced)) => {
                    if let Err(error) = ack.ack().await {
                        let compensation = if cursor_advanced {
                            self.store.rewind_cursor(seq.saturating_sub(1)).err()
                        } else {
                            None
                        };
                        if let Some(entry) = self.acknowledgements.lock().await.get_mut(&seq) {
                            entry.in_flight = false;
                            entry.last_error = Some(error.to_string());
                        }
                        self.ack_notify.notify_waiters();
                        return match compensation {
                            Some(compensation) => Err(Error::Process(format!(
                                "WAL source acknowledgement failed: {error}; cursor compensation failed: {compensation}"
                            ))),
                            None => Err(error),
                        };
                    }
                    self.acknowledgements.lock().await.remove(&seq);
                    // The wrapped source commit succeeded, so every entry below
                    // this sequence is past both acknowledgement watermarks and
                    // can be reclaimed. A reclamation failure costs disk space,
                    // not correctness — the acknowledgement itself stands — so
                    // it is reported and not propagated.
                    if let Err(error) = self.store.mark_committed(seq) {
                        tracing::warn!(
                            seq,
                            %error,
                            "WAL entry reclamation failed; entries remain until the next commit"
                        );
                    }
                    self.frontier
                        .acknowledge(&crate::checkpoint::SourcePosition::for_partition(
                            0,
                            seq.saturating_add(1),
                        ));
                    self.ack_notify.notify_waiters();
                    return Ok(());
                }
                None => {
                    // A close request does not immediately fail a parked
                    // acknowledgement: the earlier in-flight delivery gets a
                    // bounded window to settle (its source commit notifies
                    // every waiter), so a healthy graceful shutdown does not
                    // fail the stream. At-least-once is preserved either way
                    // — an unsettled acknowledgement replays on recovery.
                    if self.close.is_cancelled() && drain_deadline.is_none() {
                        drain_deadline =
                            Some(Instant::now() + WAL_CLOSE_DRAIN);
                    }
                    if let Some(deadline) = drain_deadline {
                        if Instant::now() >= deadline {
                            return Err(Error::Process(
                                "WAL closed while acknowledgement was pending".into(),
                            ));
                        }
                        notified.await;
                        if Instant::now() >= deadline {
                            return Err(Error::Process(
                                "WAL closed while acknowledgement was pending".into(),
                            ));
                        }
                        continue;
                    }
                    tokio::select! {
                        _ = notified => {}
                        _ = self.close.cancelled() => {
                            drain_deadline =
                                Some(Instant::now() + WAL_CLOSE_DRAIN);
                            continue;
                        }
                    }
                }
            }
        }
    }

    /// Compensate one already-completed WAL acknowledgement. Compensation is
    /// only safe for the current cursor frontier; callers must undo a
    /// composite acknowledgement in reverse order so a later sequence is
    /// removed before an earlier one.
    async fn undo_ack(&self, seq: u64, inner: Arc<dyn crate::input::Ack>) -> Result<(), Error> {
        enum PendingUndo {
            /// The WAL acknowledgement was registered but its source-side
            /// acknowledgement had not been attempted yet.
            Removed,
            /// The source-side acknowledgement failed and must be
            /// compensated before the WAL delivery can be discarded.
            Retry(Arc<dyn crate::input::Ack>),
        }

        // A source acknowledgement can fail after the WAL cursor was
        // tentatively advanced.  Keep that entry registered as a retryable
        // fence, but do not silently remove it when a surrounding composite
        // aborts: the source side may have committed partially and needs its
        // own compensation before the WAL delivery is discarded.
        let pending = {
            let mut acknowledgements = self.acknowledgements.lock().await;
            if let Some(entry) = acknowledgements.get_mut(&seq) {
                if entry.in_flight {
                    return Err(Error::Process(
                        "cannot undo an in-flight WAL acknowledgement".into(),
                    ));
                }
                let cursor = self.store.cursor();
                if cursor > seq {
                    return Err(Error::Process(
                        "cannot undo a WAL acknowledgement behind a later cursor".into(),
                    ));
                }
                if entry.last_error.is_some() {
                    entry.in_flight = true;
                    Some(PendingUndo::Retry(entry.ack.clone()))
                } else {
                    acknowledgements.remove(&seq);
                    self.ack_notify.notify_waiters();
                    Some(PendingUndo::Removed)
                }
            } else {
                None
            }
        };

        match pending {
            Some(PendingUndo::Removed) => return Ok(()),
            Some(PendingUndo::Retry(source_ack)) => {
                if let Err(error) = source_ack.abort().await {
                    if let Some(entry) = self.acknowledgements.lock().await.get_mut(&seq) {
                        entry.in_flight = false;
                        entry.last_error = Some(error.to_string());
                    }
                    self.ack_notify.notify_waiters();
                    return Err(error);
                }
                let cursor = self.store.cursor();
                if cursor == seq {
                    self.store.rewind_cursor(seq.saturating_sub(1))?;
                } else if cursor > seq {
                    if let Some(entry) = self.acknowledgements.lock().await.get_mut(&seq) {
                        entry.in_flight = false;
                    }
                    self.ack_notify.notify_waiters();
                    return Err(Error::Process(
                        "cannot undo a WAL acknowledgement behind a later cursor".into(),
                    ));
                }
                self.acknowledgements.lock().await.remove(&seq);
                self.ack_notify.notify_waiters();
                return Ok(());
            }
            None => {}
        }

        let cursor = self.store.cursor();
        if cursor < seq {
            return Ok(());
        }
        if cursor > seq {
            return Err(Error::Process(
                "cannot undo a WAL acknowledgement behind a later cursor".into(),
            ));
        }

        // The source-side commit is undone before the WAL cursor is rewound;
        // otherwise a crash in this compensation window could replay a record
        // whose connector offset had already been restored.
        inner.undo().await?;
        self.store.rewind_cursor(seq.saturating_sub(1))?;
        if !self
            .frontier
            .rewind_position(None, 0, seq.saturating_add(1))
        {
            return Err(Error::Process(
                "WAL acknowledgement frontier changed before compensation".into(),
            ));
        }
        self.ack_notify.notify_waiters();
        Ok(())
    }

    /// Read all entries with sequence strictly greater than the committed
    /// cursor, in ascending order. Used by recovery replay.
    pub async fn read_after_cursor(&self) -> Result<Vec<(u64, MessageBatchRef)>, Error> {
        self.store.read_after_cursor()
    }

    /// Flush staged appends before a record is exposed to downstream
    /// processing. This is the durability boundary for group-commit and
    /// periodic policies: a process crash after `read()` returns must leave
    /// the record replayable from the WAL.
    pub async fn flush(&self) -> Result<(), Error> {
        self.flush_pending().await
    }

    /// Reconcile WAL entries already covered by a restored connector
    /// checkpoint. Such entries must advance the local cursor as well as being
    /// removed from the replay queue; otherwise the next newly-read sequence
    /// waits forever for acknowledgements for the skipped prefix.
    pub async fn reconcile_covered(&self, sequences: &[u64]) -> Result<(), Error> {
        let mut covered = sequences.to_vec();
        covered.sort_unstable();
        covered.dedup();

        let _guard = self.acknowledgements.lock().await;
        let cursor = self.store.cursor();
        let mut target = cursor;
        for sequence in covered {
            if sequence == target.saturating_add(1) {
                target = sequence;
            } else if sequence > target.saturating_add(1) {
                break;
            }
        }
        if target > cursor {
            self.store.advance_cursor(target)?;
            self.frontier
                .seed(&[crate::checkpoint::SourcePosition::for_partition(
                    0,
                    target.saturating_add(1),
                )]);
            self.ack_notify.notify_waiters();
        }
        Ok(())
    }

    /// Current committed watermark (highest acked sequence, 0 if none).
    pub async fn cursor(&self) -> Result<u64, Error> {
        Ok(self.store.cursor())
    }

    async fn flush_pending(&self) -> Result<(), Error> {
        let _flush_guard = self.flush_lock.lock().await;
        let batch: Vec<(u64, Vec<u8>)> = {
            let mut p = self.pending.lock().await;
            if p.is_empty() {
                return Ok(());
            }
            std::mem::take(p.as_mut())
        };
        match self.store.append_batch(batch.clone()) {
            Ok(()) => Ok(()),
            Err(error) => {
                // A background flusher may be the caller here. Put the batch
                // back ahead of entries appended while the store write was in
                // flight so an explicit read-side flush, or the final close,
                // can retry instead of observing an empty queue and
                // incorrectly treating the record as durable.
                let mut pending = self.pending.lock().await;
                let mut retry = batch;
                retry.extend(std::mem::take(&mut *pending));
                *pending = retry;
                self.pending_notify.notify_one();
                Err(error)
            }
        }
    }

    /// Flush any staged appends and stop the background flusher. After this
    /// returns the flusher task has exited, so dropping the last `Arc<Wal>`
    /// closes the underlying store.
    ///
    /// The final flush result is returned so callers can surface a flush
    /// failure rather than silently dropping it. The flusher task's join is
    /// best-effort: a panic inside the flusher is logged and `Ok` is returned
    /// so shutdown still proceeds.
    pub async fn close(&self) -> Result<(), Error> {
        self.close.cancel();
        if let Some(handle) = self.flusher.lock().await.take() {
            if let Err(e) = handle.await {
                if e.is_panic() {
                    tracing::error!("WAL flusher task panicked during shutdown");
                }
            }
        }
        // Best-effort final flush (no-op for per-entry; pending already drained
        // by the flusher's shutdown branch otherwise). Surface the result so
        // a torn-write / disk failure is not silently lost on graceful shutdown.
        self.flush_pending().await?;
        self.store.close()
    }
}

/// Acknowledgement decorator that advances the WAL cursor before committing
/// the wrapped source acknowledgement. Wired into the stream so WAL ordering
/// remains deterministic while transient source commit failures stay retryable
/// in the in-memory acknowledgement frontier.
pub struct WalAck {
    wal: Arc<Wal>,
    seq: u64,
    inner: Arc<dyn crate::input::Ack>,
}

impl WalAck {
    pub fn new(wal: Arc<Wal>, seq: u64, inner: Arc<dyn crate::input::Ack>) -> Self {
        Self { wal, seq, inner }
    }
}

#[async_trait::async_trait]
impl crate::input::Ack for WalAck {
    async fn ack(&self) -> Result<(), Error> {
        self.wal.acknowledge(self.seq, self.inner.clone()).await
    }

    async fn undo(&self) -> Result<(), Error> {
        self.wal.undo_ack(self.seq, self.inner.clone()).await
    }

    fn mark_held(&self) {
        self.inner.mark_held();
    }

    fn release_held(&self) {
        self.inner.release_held();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::{Ack, NoopAck};
    use crate::wal::store::{deserialize, serialize};
    use crate::MessageBatch;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::ops::Deref;
    use std::sync::{Arc as StdArc, Mutex as StdMutex};

    fn sample_batch(input_name: Option<&str>) -> MessageBatch {
        let schema = StdArc::new(Schema::new(vec![
            Field::new("data", DataType::Utf8, false),
            Field::new("__meta_offset", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(StringArray::from(vec![Some("hello"), Some("world")])),
                StdArc::new(Int64Array::from(vec![Some(10), Some(20)])),
            ],
        )
        .unwrap();
        let mut mb = MessageBatch::new_arrow(batch);
        mb.set_input_name(input_name.map(|s| s.to_string()));
        mb
    }

    fn tempdir() -> std::path::PathBuf {
        static C: AtomicU64 = AtomicU64::new(0);
        let n = C.fetch_add(1, Ordering::SeqCst);
        let dir =
            std::env::temp_dir().join(format!("arkflow-wal-test-{}-{}", std::process::id(), n));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn roundtrip_preserves_schema_data_and_metadata() {
        let mb = sample_batch(Some("kafka"));
        let bytes = serialize(&mb).unwrap();
        let back = deserialize(&bytes).unwrap();
        assert_eq!(back.get_input_name().as_deref(), Some("kafka"));
        let rb_in: &RecordBatch = mb.deref();
        let rb_out: &RecordBatch = back.deref();
        assert_eq!(rb_in.schema(), rb_out.schema());
        assert_eq!(rb_in.num_rows(), rb_out.num_rows());
        assert_eq!(
            rb_out
                .column_by_name("__meta_offset")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(1),
            20
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn per_entry_write_read_advance_and_reopen() {
        let dir = tempdir();
        let cfg = WalConfig::local(
            true,
            dir.to_string_lossy().to_string(),
            SyncPolicy::PerEntry,
        );
        let wal = Wal::open(&cfg).unwrap();

        let seqs = [
            wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
            wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
            wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
        ];
        assert_eq!(seqs, [1, 2, 3]);

        // Nothing acked yet: all three are after the cursor.
        let pending = wal.read_after_cursor().await.unwrap();
        assert_eq!(pending.len(), 3);

        // Ack the first two in order; the third remains pending.
        wal.advance(1).await.unwrap();
        wal.advance(2).await.unwrap();
        assert_eq!(wal.cursor().await.unwrap(), 2);
        let pending = wal.read_after_cursor().await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0, 3);

        // Reopen (simulate restart): next_seq continues, cursor persists.
        wal.close().await.unwrap();
        drop(wal);
        let wal2 = Wal::open(&cfg).unwrap();
        let pending = wal2.read_after_cursor().await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0, 3);
        // New appends continue from seq 4, not colliding.
        let s4 = wal2.append(&StdArc::new(sample_batch(None))).await.unwrap();
        assert_eq!(s4, 4);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn group_commit_flushes_on_close() {
        let dir = tempdir();
        let cfg = WalConfig::local(
            true,
            dir.to_string_lossy().to_string(),
            SyncPolicy::GroupCommit,
        );
        let wal = Wal::open(&cfg).unwrap();
        // group-commit stages appends; they are flushed by the background
        // flusher and on close. After close + reopen they must all be present.
        let s1 = wal.append(&StdArc::new(sample_batch(None))).await.unwrap();
        let s2 = wal.append(&StdArc::new(sample_batch(None))).await.unwrap();
        wal.close().await.unwrap();
        drop(wal);
        let wal2 = Wal::open(&cfg).unwrap();
        let seqs: Vec<u64> = wal2
            .read_after_cursor()
            .await
            .unwrap()
            .iter()
            .map(|(s, _)| *s)
            .collect();
        assert!(seqs.contains(&s1));
        assert!(seqs.contains(&s2));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn corrupted_store_surfaces_error() {
        let dir = tempdir();
        let cfg = WalConfig::local(
            true,
            dir.to_string_lossy().to_string(),
            SyncPolicy::PerEntry,
        );
        let wal = Wal::open(&cfg).unwrap();
        wal.append(&StdArc::new(sample_batch(None))).await.unwrap();
        wal.close().await.unwrap();
        drop(wal);
        // Corrupt the database header with garbage bytes (a torn header must
        // not silently produce a valid, empty WAL — it must surface an error).
        use std::io::Write;
        let db_path = dir.join("wal.redb");
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(&db_path)
            .unwrap();
        f.write_all(&[0xFFu8; 256]).unwrap();
        f.flush().unwrap();
        drop(f);
        let result = Wal::open(&cfg);
        assert!(
            result.is_err(),
            "opening a corrupted WAL must surface an error"
        );
    }

    /// End-to-end crash-recovery contract (task 6.4): a message ingested but
    /// not yet acknowledged must survive a crash and be replayed on restart
    /// (no loss); the replay IS the at-least-once duplicate. Once the
    /// downstream confirms, the cursor advances and a further restart replays
    /// nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn crash_recovery_replays_unacked_then_advances_on_ack() {
        let dir = tempdir();
        let cfg = WalConfig::local(
            true,
            dir.to_string_lossy().to_string(),
            SyncPolicy::PerEntry,
        );

        // Phase 1: ingest a message, then "crash" before acknowledging it.
        let wal = Wal::open(&cfg).unwrap();
        let seq = wal
            .append(&StdArc::new(sample_batch(Some("http"))))
            .await
            .unwrap();
        assert_eq!(seq, 1);
        // No advance — simulate a crash before the downstream output confirmed.
        wal.close().await.unwrap();
        drop(wal);

        // Phase 2: restart. Recovery must replay the unacked message (no loss).
        let wal2 = Wal::open(&cfg).unwrap();
        let replayed = wal2.read_after_cursor().await.unwrap();
        assert_eq!(
            replayed.len(),
            1,
            "unacked message must be replayed (no loss)"
        );
        assert_eq!(replayed[0].0, seq);
        assert_eq!(replayed[0].1.get_input_name().as_deref(), Some("http"));

        // Phase 3: downstream confirms → WalAck advances the cursor first, then
        // the (noop) source ack. The replay above is the at-least-once duplicate.
        let ack: StdArc<dyn Ack> = StdArc::new(WalAck::new(
            wal2.clone(),
            replayed[0].0,
            StdArc::new(NoopAck),
        ));
        ack.ack().await.unwrap();
        assert_eq!(wal2.cursor().await.unwrap(), seq);
        drop(ack); // release the WalAck's Arc<Wal> so the database can close

        // Phase 4: a further restart replays nothing — fully acknowledged.
        wal2.close().await.unwrap();
        drop(wal2);
        let wal3 = Wal::open(&cfg).unwrap();
        assert!(wal3.read_after_cursor().await.unwrap().is_empty());
    }

    struct RecordingAck {
        sequence: u64,
        calls: StdArc<StdMutex<Vec<u64>>>,
    }

    #[async_trait::async_trait]
    impl Ack for RecordingAck {
        async fn ack(&self) -> Result<(), Error> {
            self.calls.lock().unwrap().push(self.sequence);
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wal_ack_keeps_cursor_contiguous_across_out_of_order_children() {
        let dir = tempdir();
        let cfg = WalConfig::local(
            true,
            dir.to_string_lossy().to_string(),
            SyncPolicy::PerEntry,
        );
        let wal = Wal::open(&cfg).unwrap();
        let first = wal.append(&StdArc::new(sample_batch(None))).await.unwrap();
        let second = wal.append(&StdArc::new(sample_batch(None))).await.unwrap();
        assert_eq!((first, second), (1, 2));

        let calls = StdArc::new(StdMutex::new(Vec::new()));
        let second_ack: StdArc<dyn Ack> = StdArc::new(RecordingAck {
            sequence: second,
            calls: calls.clone(),
        });
        let first_ack: StdArc<dyn Ack> = StdArc::new(RecordingAck {
            sequence: first,
            calls: calls.clone(),
        });
        let second_wal_ack: StdArc<dyn Ack> =
            StdArc::new(WalAck::new(wal.clone(), second, second_ack));
        let first_wal_ack: StdArc<dyn Ack> =
            StdArc::new(WalAck::new(wal.clone(), first, first_ack));

        // N+1 cannot report success until the source ack and WAL cursor have
        // crossed the still-unacknowledged N. It therefore waits here.
        let second_task = tokio::spawn(async move { second_wal_ack.ack().await });
        tokio::task::yield_now().await;
        assert_eq!(wal.cursor().await.unwrap(), 0);
        assert!(calls.lock().unwrap().is_empty());

        // Closing the gap drains source acknowledgements and cursor advances
        // in the same contiguous order.
        first_wal_ack.ack().await.unwrap();
        second_task.await.unwrap().unwrap();
        assert_eq!(wal.cursor().await.unwrap(), 2);
        assert_eq!(*calls.lock().unwrap(), vec![1, 2]);
    }

    /// Throughput benchmark per sync policy (task 5.3). Ignored by default;
    /// run with `cargo test -p arkflow-core --lib wal::tests::bench_append_throughput --release -- --ignored --nocapture`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn bench_append_throughput() {
        let n = 5_000u32;
        for (name, policy) in [
            ("per_entry", SyncPolicy::PerEntry),
            ("group_commit", SyncPolicy::GroupCommit),
            (
                "periodic_1ms",
                SyncPolicy::Periodic(Duration::from_millis(1)),
            ),
        ] {
            let dir = tempdir();
            let cfg = WalConfig::local(true, dir.to_string_lossy().to_string(), policy);
            let wal = Wal::open(&cfg).unwrap();
            let msg = StdArc::new(sample_batch(None));
            let start = std::time::Instant::now();
            for _ in 0..n {
                wal.append(&msg).await.unwrap();
            }
            wal.close().await.unwrap();
            let elapsed = start.elapsed();
            let per = elapsed / n;
            println!(
                "bench {}: {} appends in {:?} ({:.2} us/append, {:.0} appends/s)",
                name,
                n,
                elapsed,
                per.as_secs_f64() * 1e6,
                n as f64 / elapsed.as_secs_f64()
            );
        }
    }
}
