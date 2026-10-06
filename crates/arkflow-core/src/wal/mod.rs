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
pub use store::{build_wal_store, register_wal_store_builder, WalStore, WalStoreBuilder};

use crate::wal::store::serialize;
use crate::{Error, MessageBatchRef};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
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
    /// Close drain window in milliseconds; kept mutable so tests can shrink
    /// it without waiting the production window.
    ack_drain_window_ms: AtomicU64,
    /// Park lease in milliseconds for acknowledgements parked behind a
    /// lower sequence while the WAL is open
    /// ([`WAL_ACK_PARK_TIMEOUT`]); kept mutable so tests can shrink it
    /// without waiting the production lease.
    ack_park_timeout_ms: AtomicU64,
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
    // --- flusher failure visibility (D3 of fix-object-store-wal-loss-window) ---
    /// Total flush failures counted on the background flusher's wake path;
    /// exposed through [`Wal::flush_failures`] for status reporting.
    flush_failures: AtomicU64,
    /// Consecutive wake-path flush failures; any success resets them.
    /// Reaching [`WAL_FLUSH_ESCALATION_THRESHOLD`] escalates the log level.
    flush_failures_consecutive: AtomicU64,
    /// Timestamp (ms since the UNIX epoch) of the last flush-failure log
    /// line; rate-limits warn/error output to one line per
    /// [`WAL_FLUSH_LOG_INTERVAL`].
    flush_failure_last_log_ms: AtomicU64,
    /// Test override for the seal-wait timeout (0 = derive from the sync
    /// policy), so the timeout and close paths are exercisable without
    /// waiting the production bound.
    seal_wait_timeout_ms: AtomicU64,
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
const WAL_ACK_DRAIN_WINDOW: Duration = Duration::from_secs(30);

/// Upper bound for a parked WAL acknowledgement waiting for a lower
/// sequence to settle while the WAL is still open (fix-ack-stall-modes).
/// Without this lease, a gap owner that died silently — its caller dropped
/// after an upstream swallowed error, no `last_error` fence written — left
/// parked callers waiting forever: a stream with no errors, no progress,
/// and one unbounded `acknowledgements` entry per parked sequence. The
/// lease turns that silent stall into an explicit periodic error handed to
/// the parked caller; it deliberately writes NO `last_error` fence, so a
/// merely-slow gap owner is never fenced and a settle inside the lease
/// proceeds with zero added latency.
///
/// Must stay above the stall-free bound of [`Wal::seal_wait_timeout`]
/// (`seal_interval()` → `4·interval + 5s`; the 10s aggressive preset →
/// 45s): 60s > 45s keeps the lease from firing while the store is merely
/// sealing slowly. Also far above normal source commits (ms–s) and far
/// below the checkpoint round timeout (10 minutes). Tests override it
/// through [`Wal::override_ack_park_timeout_for_tests`].
const WAL_ACK_PARK_TIMEOUT: Duration = Duration::from_secs(60);

/// Rate limit for background-flush failure logs (warn and the escalated
/// error): one line per interval, mirroring the `WARN_INTERVAL` convention
/// of `executor::event_time_gate`.
const WAL_FLUSH_LOG_INTERVAL: Duration = Duration::from_secs(10);

/// Consecutive wake-path flush failures before the log escalates to error
/// level. Roughly one `flush_interval`'s worth of failed cycles separates
/// "retrying after a transient error" from "the flusher is not making
/// progress" (object-store defaults: 1s balanced, 10s throughput).
const WAL_FLUSH_ESCALATION_THRESHOLD: u64 = 8;

/// Poll fallback for the seal wait when a backend reports a sealed frontier
/// without a seal notifier. None of the built-in backends do this; the
/// fallback keeps a third-party store from turning the bounded wait into a
/// busy loop.
const WAL_SEAL_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Fallback bound for the seal wait when the store reports no sealing
/// cadence of its own (third-party backends): three "aggressive"
/// object-store flush intervals (10s each) plus headroom for one PUT
/// retry. The wait is progress-based, so this only trips on a store that
/// stops sealing entirely.
const WAL_SEAL_WAIT_FALLBACK_BOUND: Duration = Duration::from_secs(30);

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
            ack_drain_window_ms: AtomicU64::new(WAL_ACK_DRAIN_WINDOW.as_millis() as u64),
            ack_park_timeout_ms: AtomicU64::new(WAL_ACK_PARK_TIMEOUT.as_millis() as u64),
            policy: sync_policy,
            pending: Mutex::new(Vec::new()),
            pending_notify: Notify::new(),
            flush_lock: tokio::sync::Mutex::new(()),
            ack_notify: Notify::new(),
            close: CancellationToken::new(),
            flusher: Mutex::new(None),
            flush_failures: AtomicU64::new(0),
            flush_failures_consecutive: AtomicU64::new(0),
            flush_failure_last_log_ms: AtomicU64::new(0),
            seal_wait_timeout_ms: AtomicU64::new(0),
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
                        // Shutdown branch: `close()` re-surfaces this final
                        // flush's result, so no counting or logging here.
                        let _ = wal.flush_pending().await;
                        break;
                    }
                    _ = wait => {
                        // Wake branch: a failure here is not silently
                        // swallowed — it is counted (and rate-limit logged,
                        // escalating on sustained failure) so "retrying"
                        // stays distinguishable from "broken" (D3). The
                        // batch itself was restored by `flush_locked` and
                        // remains retryable.
                        match wal.flush_pending().await {
                            Ok(()) => wal.note_flush_success(),
                            Err(error) => wal.note_flush_failure(&error),
                        }
                    }
                }
            }
        })
    }

    /// Drive one blocking store call safely for the active backend.
    ///
    /// The object-store backend executes real network I/O (S3 PUT/GET) and
    /// parks on its private runtime via `block_on`; running that on an async
    /// worker thread blocks the executor and panics with "cannot start a
    /// runtime from within a runtime". The blocking pool has neither
    /// problem. The local embedded backend stays on the caller's thread:
    /// redb's fcntl flock can deadlock against the blocking pool when the
    /// close path contends on the database file (see `append`), and its
    /// commit latency is µs–ms.
    async fn call_store<T, F>(&self, f: F) -> Result<T, Error>
    where
        T: Send + 'static,
        F: FnOnce(&Arc<dyn WalStore>) -> Result<T, Error> + Send + 'static,
    {
        if self.store.kind() != "object_store" {
            return f(&self.store);
        }
        let store = Arc::clone(&self.store);
        tokio::task::spawn_blocking(move || f(&store))
            .await
            .map_err(|e| Error::Process(format!("WAL store task failed: {e}")))?
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
    /// [`Wal::call_store`]: the local backend stays inline on the caller's
    /// thread (redb's flock must not contend with the blocking pool), while
    /// the object-store backend is driven on the blocking pool.
    pub async fn append(&self, msg: &MessageBatchRef) -> Result<u64, Error> {
        let seq = self.next_seq.fetch_add(1, Ordering::AcqRel);
        let bytes = serialize(msg)?;

        match &self.policy {
            SyncPolicy::PerEntry => {
                // The local backend's `commit` is briefly blocking (~µs–ms)
                // and stays inline (see `call_store` for why redb must not
                // touch the blocking pool); the object-store backend is
                // driven on the blocking pool by `call_store`.
                self.call_store(move |store| store.append_batch(vec![(seq, bytes)]))
                    .await?;
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
        let current = self.call_store(|store| Ok(store.cursor())).await?;
        if target > current {
            // Cursor advancement only. Reclaiming here would delete entries
            // this path has no wrapped source commit for, and the trait's
            // contract keeps every entry above the reclaim floor replayable for
            // a cursor compensation — so only `acknowledge`, after the wrapped
            // source commit succeeds, marks a sequence committed.
            self.call_store(move |store| store.advance_cursor(target))
                .await?;
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
        {
            let mut acknowledgements = self.acknowledgements.lock().await;
            acknowledgements.entry(seq).or_insert(PendingWalAck {
                ack: inner,
                in_flight: false,
                last_error: None,
            });
        }

        // Seal gating (D1 of fix-object-store-wal-loss-window): backends
        // that seal asynchronously (the object-store backend PUTs segment
        // objects from a background flusher) do NOT make an appended entry
        // durable when `flush` returns — only when it is sealed. The
        // source-side commit is the point of no return for redelivery, so
        // it may run only after this entry is contained in a sealed
        // segment: "acknowledged" always implies "sealed", which turns the
        // former crash-loss window into a bounded replay window (entries
        // are redelivered by their source on restart, never lost).
        // Backends whose `sealed_seq()` is `None` (local redb: the flush
        // IS the durable commit) skip the gate with zero behavior change.
        // A timeout or close during the wait fails through the same fence
        // as a source-commit failure below: the entry stays registered and
        // retryable, and recovery replays it.
        if self.store.sealed_seq().is_some() {
            if let Err(error) = self.wait_for_sealed(seq).await {
                if let Some(entry) = self.acknowledgements.lock().await.get_mut(&seq) {
                    entry.last_error = Some(error.to_string());
                }
                self.ack_notify.notify_waiters();
                return Err(error);
            }
        }

        loop {
            // Pinned once per iteration so the parked branch can keep polling
            // the same future across both stages of the close drain window.
            // Enabled before the lock acquisition below: a settle that runs
            // `notify_waiters` between the pin and the first poll must not be
            // missed (the same lost-wakeup window `wait_for_sealed` guards
            // against); a spurious wake only re-runs the state check.
            let mut notified = std::pin::pin!(self.ack_notify.notified());
            let _ = notified.as_mut().enable();
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
                let cursor = self.call_store(|store| Ok(store.cursor())).await?;
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
                        self.call_store(move |store| store.advance_cursor(seq))
                            .await?;
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
                            let rewind_to = seq.saturating_sub(1);
                            self.call_store(move |store| store.rewind_cursor(rewind_to))
                                .await
                                .err()
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
                    if let Err(error) = self
                        .call_store(move |store| store.mark_committed(seq))
                        .await
                    {
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
                    // bounded drain window to settle, and the parked waiter
                    // completes normally when it does. Once the window
                    // expires, the pending-error path takes over and recovery
                    // replays the unsettled delivery (at-least-once holds).
                    //
                    // Park lease (fix-ack-stall-modes): while the WAL is
                    // open, a parked acknowledgement also carries a bounded
                    // lease. A gap owner that died silently (its caller
                    // dropped after an upstream swallowed error, no
                    // `last_error` fence written) must leave the parked
                    // caller with an explicit retryable error instead of an
                    // unbounded wait with no errors and no progress. The
                    // lease deliberately writes no `last_error` fence — the
                    // owner may be merely slow, and once it settles a retry
                    // of the parked sequence proceeds normally. The sleep is
                    // created per loop iteration and re-created after every
                    // wakeup, so each observed progress notification
                    // legitimately re-arms the lease; only a gap owner that
                    // stops making any progress trips it.
                    let park_timeout =
                        Duration::from_millis(self.ack_park_timeout_ms.load(Ordering::Acquire));
                    tokio::select! {
                        biased;
                        _ = notified.as_mut() => {}
                        _ = tokio::time::sleep(park_timeout) => {
                            let stuck = self
                                .acknowledgements
                                .lock()
                                .await
                                .keys()
                                .next()
                                .copied();
                            // Everything (including this sequence) settled
                            // concurrently with the lease expiry: re-evaluate
                            // instead of failing a completed delivery.
                            let Some(stuck) = stuck else {
                                continue;
                            };
                            tracing::warn!(
                                seq,
                                stuck,
                                park_timeout = ?park_timeout,
                                "WAL acknowledgement park lease expired; the gap owner appears stalled"
                            );
                            return Err(Error::Process(format!(
                                "WAL acknowledgement parked for >{}s waiting for sequence {stuck}; \
                                 the gap owner appears stalled",
                                park_timeout.as_secs_f64()
                            )));
                        }
                        _ = self.close.cancelled() => {
                            let window = Duration::from_millis(
                                self.ack_drain_window_ms.load(Ordering::Acquire),
                            );
                            tokio::select! {
                                _ = notified.as_mut() => {}
                                _ = tokio::time::sleep(window) => {
                                    return Err(Error::Process(
                                        "WAL closed while acknowledgement was pending".into(),
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Whether the backend reports `seq` as contained in a sealed segment.
    fn seal_covers(&self, seq: u64) -> bool {
        self.store.sealed_seq().is_some_and(|sealed| sealed >= seq)
    }

    /// Bound for one stall-free interval of [`Wal::wait_for_sealed`].
    /// Derived from the store's own sealing cadence when it reports one
    /// ([`WalStore::seal_interval`] → `4·interval + 5s`: one missed seal
    /// tick plus retry headroom for the segment PUT); a store without a
    /// reported cadence falls back to a fixed bound. The wait additionally
    /// resets its deadline on every advance of the sealed frontier (see
    /// [`Wal::wait_for_sealed`]), so the bound only trips when the store
    /// stops sealing entirely — a long `flush_interval` alone cannot cause
    /// spurious timeouts. Tests override this through
    /// [`Wal::override_seal_wait_timeout_for_tests`].
    fn seal_wait_timeout(&self) -> Duration {
        let override_ms = self.seal_wait_timeout_ms.load(Ordering::Acquire);
        if override_ms > 0 {
            return Duration::from_millis(override_ms);
        }
        match self.store.seal_interval() {
            Some(interval) => interval.saturating_mul(4) + Duration::from_secs(5),
            None => WAL_SEAL_WAIT_FALLBACK_BOUND,
        }
    }

    /// Wait until the backend reports `seq` as contained in a sealed segment
    /// object (object-store backends only — see [`WalStore::sealed_seq`]).
    ///
    /// The wait is bounded by [`Wal::seal_wait_timeout`] and cancel-safe:
    /// every iteration checks the frontier first and re-registers its
    /// waiter. The waiter is registered (via `Notified::enable`) *before*
    /// the frontier check because `notify_waiters` stores no permit for
    /// waiters registered after the call — a bare check-then-await could
    /// miss a seal that lands in between and stall until the timeout. The
    /// deadline is progress-based: any advance of the sealed frontier
    /// pushes it out, so a store that keeps sealing (even slowly) never
    /// trips the bound — only a store that stops sealing entirely does.
    /// Once the WAL starts closing, the wait fails promptly (with one
    /// best-effort final frontier re-check — the close path cancels before
    /// the final flush/seal, so an entry that is not sealed yet usually
    /// fails here): an unsealed entry must fail its acknowledgement so
    /// recovery replays it, rather than block shutdown.
    async fn wait_for_sealed(&self, seq: u64) -> Result<(), Error> {
        let timeout = self.seal_wait_timeout();
        let mut last_sealed = self.store.sealed_seq();
        let mut deadline = tokio::time::Instant::now() + timeout;
        loop {
            // Any sealed-frontier advance proves the flusher is alive: push
            // the deadline out so only a fully stalled store times out.
            let sealed_now = self.store.sealed_seq();
            if sealed_now != last_sealed {
                last_sealed = sealed_now;
                deadline = tokio::time::Instant::now() + timeout;
            }
            if let Some(notify) = self.store.seal_notifier() {
                let mut notified = std::pin::pin!(notify.notified());
                let _ = notified.as_mut().enable();
                if self.seal_covers(seq) {
                    return Ok(());
                }
                tokio::select! {
                    biased;
                    _ = self.close.cancelled() => {
                        if self.seal_covers(seq) {
                            return Ok(());
                        }
                        return Err(Error::Process(format!(
                            "WAL closed while sequence {seq} was still waiting for its \
                             segment seal; recovery will replay the entry"
                        )));
                    }
                    _ = notified.as_mut() => {}
                    _ = tokio::time::sleep_until(deadline) => {
                        return Err(Error::Process(format!(
                            "WAL seal wait for sequence {seq} timed out after {timeout:?} \
                             (background flusher not sealing?)"
                        )));
                    }
                }
            } else {
                // A backend that reports a sealed frontier without a seal
                // notifier (none of the built-ins): poll the frontier.
                if self.seal_covers(seq) {
                    return Ok(());
                }
                tokio::select! {
                    biased;
                    _ = self.close.cancelled() => {
                        if self.seal_covers(seq) {
                            return Ok(());
                        }
                        return Err(Error::Process(format!(
                            "WAL closed while sequence {seq} was still waiting for its \
                             segment seal; recovery will replay the entry"
                        )));
                    }
                    _ = tokio::time::sleep(WAL_SEAL_POLL_INTERVAL) => {}
                    _ = tokio::time::sleep_until(deadline) => {
                        return Err(Error::Process(format!(
                            "WAL seal wait for sequence {seq} timed out after {timeout:?} \
                             (background flusher not sealing?)"
                        )));
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
                let cursor = self.call_store(|store| Ok(store.cursor())).await?;
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
                let cursor = self.call_store(|store| Ok(store.cursor())).await?;
                if cursor == seq {
                    let rewind_to = seq.saturating_sub(1);
                    self.call_store(move |store| store.rewind_cursor(rewind_to))
                        .await?;
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

        let cursor = self.call_store(|store| Ok(store.cursor())).await?;
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
        let rewind_to = seq.saturating_sub(1);
        self.call_store(move |store| store.rewind_cursor(rewind_to))
            .await?;
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
        self.call_store(|store| store.read_after_cursor()).await
    }

    /// Flush staged appends before a record is exposed to downstream
    /// processing. This is the durability boundary for group-commit and
    /// periodic policies: a process crash after `read()` returns must leave
    /// the record replayable from the WAL.
    pub async fn flush(self: &Arc<Self>) -> Result<(), Error> {
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
        let cursor = self.call_store(|store| Ok(store.cursor())).await?;
        let mut target = cursor;
        for sequence in covered {
            if sequence == target.saturating_add(1) {
                target = sequence;
            } else if sequence > target.saturating_add(1) {
                break;
            }
        }
        if target > cursor {
            self.call_store(move |store| store.advance_cursor(target))
                .await?;
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
        self.call_store(|store| Ok(store.cursor())).await
    }

    /// One serialized flush pass: take the staged batch, hand it to the
    /// store, and on failure restore the batch ahead of anything appended
    /// while the write was in flight (so an explicit read-side flush, or
    /// the final close, retries instead of observing an empty queue and
    /// treating the record as durable).
    async fn flush_locked(self: &Arc<Self>) -> Result<(), Error> {
        let _flush_guard = self.flush_lock.lock().await;
        let batch: Vec<(u64, Vec<u8>)> = {
            let mut p = self.pending.lock().await;
            if p.is_empty() {
                return Ok(());
            }
            std::mem::take(p.as_mut())
        };
        // The clone keeps the batch retryable when the store write fails —
        // the store call consumes its copy.
        let retry_batch = batch.clone();
        let result = self
            .call_store(move |store| store.append_batch(batch))
            .await;
        match result {
            Ok(()) => Ok(()),
            Err(error) => {
                let mut pending = self.pending.lock().await;
                let mut retry = retry_batch;
                retry.extend(std::mem::take(&mut *pending));
                *pending = retry;
                self.pending_notify.notify_one();
                Err(error)
            }
        }
    }

    async fn flush_pending(self: &Arc<Self>) -> Result<(), Error> {
        // Cancellation ownership depends on the backend:
        //
        // - Local (inline store calls): a dropped flush future is atomic
        //   per poll — the append and its failure restoration execute in
        //   the same poll, so nothing is lost. Keep the inline shape.
        // - Object store (spawn_blocking): the store task keeps running
        //   after its JoinHandle is dropped, so a caller cancelled at the
        //   await would lose the restoration (the batch is already out of
        //   `pending`). Run the whole flush — lock, append, restore — in a
        //   task that survives cancellation.
        if self.store.kind() != "object_store" {
            self.flush_locked().await
        } else {
            let wal = Arc::clone(self);
            let task = tokio::spawn(async move { wal.flush_locked().await });
            match task.await {
                Ok(result) => result,
                Err(e) => Err(Error::Process(format!("WAL flush task failed: {e}"))),
            }
        }
    }

    /// Record one successful wake-path flush: consecutive failures reset
    /// (the total keeps the lifetime count for status reporting).
    fn note_flush_success(&self) {
        self.flush_failures_consecutive.store(0, Ordering::Relaxed);
    }

    /// Record one failed wake-path flush (D3): bump the total and
    /// consecutive counters, and log at a rate of one line per
    /// [`WAL_FLUSH_LOG_INTERVAL`] — warn while the failures look transient,
    /// error once they are sustained (carrying the triggering error). The
    /// staged batch itself was restored by `flush_locked` and stays
    /// retryable; this only makes the retry loop observable.
    fn note_flush_failure(&self, error: &Error) {
        let total = self.flush_failures.fetch_add(1, Ordering::Relaxed) + 1;
        let consecutive = self
            .flush_failures_consecutive
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let last = self.flush_failure_last_log_ms.load(Ordering::Acquire);
        if now.saturating_sub(last) < WAL_FLUSH_LOG_INTERVAL.as_millis() as u64 {
            return;
        }
        self.flush_failure_last_log_ms.store(now, Ordering::Release);
        if consecutive >= WAL_FLUSH_ESCALATION_THRESHOLD {
            tracing::error!(
                total,
                consecutive,
                %error,
                "WAL background flush is failing persistently; staged entries are NOT \
                 durable and will be lost on a crash — the store is not making progress"
            );
        } else {
            tracing::warn!(
                total,
                consecutive,
                %error,
                "WAL background flush failed; the staged batch stays queued and will retry"
            );
        }
    }

    /// Total flush failures counted on the background flusher's wake path
    /// (observability: distinguishes a transient retry from a broken
    /// store; sustained failure also escalates to error-level logging).
    /// Crate-visible for the in-crate regression tests; no external
    /// consumer yet, per the core-api-surface spec's zero-reference rule.
    #[cfg(test)]
    pub(crate) fn flush_failures(&self) -> u64 {
        self.flush_failures.load(Ordering::Relaxed)
    }

    /// Current consecutive wake-path flush failures; reset by any
    /// successful flush.
    #[cfg(test)]
    pub(crate) fn flush_consecutive_failures(&self) -> u64 {
        self.flush_failures_consecutive.load(Ordering::Relaxed)
    }

    /// Flush any staged appends and stop the background flusher. After this
    /// returns the flusher task has exited, so dropping the last `Arc<Wal>`
    /// closes the underlying store.
    ///
    /// The final flush result is returned so callers can surface a flush
    /// failure rather than silently dropping it. The flusher task's join is
    /// best-effort: a panic inside the flusher is logged and `Ok` is returned
    /// so shutdown still proceeds.
    pub async fn close(self: &Arc<Self>) -> Result<(), Error> {
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
        self.call_store(|store| store.close()).await
    }

    /// Shrinks the close drain window so tests can exercise window expiry
    /// without waiting the production 30s.
    #[cfg(test)]
    pub(crate) fn override_ack_drain_window_for_tests(&self, window: Duration) {
        self.ack_drain_window_ms
            .store(window.as_millis() as u64, Ordering::Release);
    }

    /// Shrinks the parked-acknowledgement lease so tests can exercise lease
    /// expiry without waiting the production 60s.
    #[cfg(test)]
    pub(crate) fn override_ack_park_timeout_for_tests(&self, lease: Duration) {
        self.ack_park_timeout_ms
            .store(lease.as_millis() as u64, Ordering::Release);
    }

    /// Overrides the seal-wait timeout so tests can exercise the timeout
    /// and close paths without waiting the production bound.
    #[cfg(test)]
    pub(crate) fn override_seal_wait_timeout_for_tests(&self, timeout: Duration) {
        self.seal_wait_timeout_ms
            .store(timeout.as_millis() as u64, Ordering::Release);
    }
}

/// Acknowledgement decorator that advances the WAL cursor before committing
/// the wrapped source acknowledgement. Wired into the stream so WAL ordering
/// remains deterministic while transient source commit failures stay retryable
/// in the in-memory acknowledgement frontier.
pub(crate) struct WalAck {
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

    /// A store whose appends always fail, counting the attempts through a
    /// shared counter.
    struct FailingAppendStore {
        attempts: Arc<AtomicU64>,
    }

    impl crate::wal::store::WalStore for FailingAppendStore {
        fn kind(&self) -> &'static str {
            "object_store"
        }
        fn append_batch(&self, _entries: Vec<(u64, Vec<u8>)>) -> Result<(), Error> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            Err(Error::Process("append always fails".into()))
        }
        fn advance_cursor(&self, _seq: u64) -> Result<(), Error> {
            Ok(())
        }
        fn read_after_cursor(&self) -> Result<Vec<(u64, crate::MessageBatchRef)>, Error> {
            Ok(Vec::new())
        }
        fn cursor(&self) -> u64 {
            0
        }
        fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    /// CodeRabbit/CR finding: a flush future cancelled while its store call
    /// is in flight must not lose the staged batch — spawn_blocking keeps
    /// running after the handle is dropped, and the failure restoration
    /// used to live in the discarded future. Post-fix the restoration runs
    /// in a surviving task, so a RETRY flush still reports the failure; a
    /// lost batch would make the retry observe an empty queue and report
    /// success without durability.
    #[tokio::test]
    async fn cancelled_flush_keeps_the_staged_batch_retryable() {
        let attempts = Arc::new(AtomicU64::new(0));
        let store: Arc<dyn crate::wal::store::WalStore> = Arc::new(FailingAppendStore {
            attempts: attempts.clone(),
        });

        let config = WalConfig {
            sync: SyncPolicy::GroupCommit,
            ..WalConfig::default()
        };
        let wal = Wal::open_with_store(&config, store, 1).unwrap();
        wal.append(&StdArc::new(sample_batch(None))).await.unwrap();

        // Cancel a flush while it is pending (the store call may or may not
        // have started — both paths must keep the batch).
        {
            // Cancel while the flush is pending — the store task may or may
            // not have started; both paths must keep the batch.
            tokio::select! {
                _ = wal.flush() => {}
                _ = std::future::ready(()) => {}
            }
        }
        // Let the surviving flush task run its failing append.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while attempts.load(Ordering::SeqCst) == 0 {
            assert!(std::time::Instant::now() < deadline, "flush task never ran");
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }

        // The retry must still surface the failure: the batch was restored,
        // not lost. (A lost batch makes this flush report Ok with the
        // record silently non-durable.)
        assert!(
            wal.flush().await.is_err(),
            "restored batch must fail again — a success here means the staged entries were lost"
        );
        assert!(attempts.load(Ordering::SeqCst) >= 2);
        wal.close().await.ok();
    }

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
    // ---- WalConfig::validate and accessor coverage ----

    fn object_store_config(backend_patch: serde_json::Value) -> WalConfig {
        let mut backend = serde_json::json!({
            "type": "object_store",
            "node_id": "node-1",
            "stream_id": "stream-1",
            "s3": {"bucket": "bucket-a"},
        });
        if let Some(patch) = backend_patch.as_object() {
            for (key, value) in patch {
                backend[key] = value.clone();
            }
        }
        serde_json::from_value(serde_json::json!({"backend": backend})).unwrap()
    }

    #[test]
    fn object_store_config_validation_rejects_each_forbidden_shape() {
        let valid = object_store_config(serde_json::json!({}));
        valid.validate().unwrap();

        let empty_node = object_store_config(serde_json::json!({"node_id": "  "}));
        let err = empty_node.validate().unwrap_err();
        assert!(err.to_string().contains("node_id is required"));

        let empty_stream = object_store_config(serde_json::json!({"stream_id": ""}));
        let err = empty_stream.validate().unwrap_err();
        assert!(err.to_string().contains("stream_id is required"));

        let empty_bucket = object_store_config(serde_json::json!({"s3": {"bucket": " "}}));
        let err = empty_bucket.validate().unwrap_err();
        assert!(err.to_string().contains("bucket is required"));

        let per_entry = object_store_config(serde_json::json!({"sync": "per_entry"}));
        let err = per_entry.validate().unwrap_err();
        assert!(err.to_string().contains("per_entry"));

        let zero_workers = object_store_config(serde_json::json!({"parallel_put": {"workers": 0}}));
        let err = zero_workers.validate().unwrap_err();
        assert!(err.to_string().contains("must be positive"));

        let many_workers = object_store_config(serde_json::json!({"parallel_put": {"workers": 9}}));
        let err = many_workers.validate().unwrap_err();
        assert!(err.to_string().contains("out of range"));

        let bad_zstd =
            object_store_config(serde_json::json!({"compression": {"type": "zstd", "level": 23}}));
        let err = bad_zstd.validate().unwrap_err();
        assert!(err.to_string().contains("zstd.level"));

        let bad_lz4 =
            object_store_config(serde_json::json!({"compression": {"type": "lz4", "level": 0}}));
        let err = bad_lz4.validate().unwrap_err();
        assert!(err.to_string().contains("lz4.level"));

        // In-range compression passes.
        object_store_config(serde_json::json!({"compression": {"type": "zstd", "level": 3}}))
            .validate()
            .unwrap();
        object_store_config(serde_json::json!({"compression": {"type": "lz4", "level": 4}}))
            .validate()
            .unwrap();
    }

    #[test]
    fn config_accessors_cover_local_and_object_store_shapes() {
        let legacy = WalConfig::local(true, "/tmp/wal".into(), SyncPolicy::PerEntry);
        assert!(legacy.validate().is_ok());
        assert_eq!(legacy.local_path(), Some("/tmp/wal"));
        assert_eq!(legacy.effective_sync(), &SyncPolicy::PerEntry);
        assert_eq!(legacy.backend_kind(), "local");

        let nested_local: WalConfig = serde_json::from_value(serde_json::json!({
            "backend": {"type": "local", "path": "/tmp/nested", "sync": {"periodic": {"secs": 1, "nanos": 0}}}
        }))
        .unwrap();
        assert_eq!(nested_local.local_path(), Some("/tmp/nested"));
        assert!(matches!(
            nested_local.effective_sync(),
            SyncPolicy::Periodic(_)
        ));
        assert_eq!(nested_local.backend_kind(), "local");

        let object = object_store_config(serde_json::json!({}));
        assert_eq!(object.local_path(), None);
        assert_eq!(object.backend_kind(), "object_store");
        assert_eq!(object.effective_sync(), &SyncPolicy::default());
    }

    // ---- Wal acknowledgement / undo / recovery-path coverage ----

    /// An in-memory store with scriptable failures; `kind() == "local"` keeps
    /// store calls inline so no runtime shape is required beyond the test's.
    struct ScriptedStore {
        entries: StdMutex<BTreeMap<u64, Vec<u8>>>,
        cursor: AtomicU64,
        fail_append: bool,
        fail_rewind: bool,
        fail_mark_committed: bool,
    }

    impl crate::wal::store::WalStore for ScriptedStore {
        fn kind(&self) -> &'static str {
            "local"
        }
        fn append_batch(&self, entries: Vec<(u64, Vec<u8>)>) -> Result<(), Error> {
            if self.fail_append {
                return Err(Error::Process("scripted append failure".into()));
            }
            let mut map = self.entries.lock().unwrap();
            for (seq, bytes) in entries {
                map.insert(seq, bytes);
            }
            Ok(())
        }
        fn advance_cursor(&self, seq: u64) -> Result<(), Error> {
            let mut current = self.cursor.load(Ordering::SeqCst);
            while seq > current {
                match self
                    .cursor
                    .compare_exchange(current, seq, Ordering::SeqCst, Ordering::SeqCst)
                {
                    Ok(_) => break,
                    Err(observed) => current = observed,
                }
            }
            Ok(())
        }
        fn rewind_cursor(&self, seq: u64) -> Result<(), Error> {
            if self.fail_rewind {
                return Err(Error::Process("scripted rewind failure".into()));
            }
            self.cursor.store(seq, Ordering::SeqCst);
            Ok(())
        }
        fn mark_committed(&self, _seq: u64) -> Result<(), Error> {
            if self.fail_mark_committed {
                return Err(Error::Process("scripted reclaim failure".into()));
            }
            Ok(())
        }
        fn read_after_cursor(&self) -> Result<Vec<(u64, crate::MessageBatchRef)>, Error> {
            let cursor = self.cursor.load(Ordering::SeqCst);
            let map = self.entries.lock().unwrap();
            Ok(map
                .iter()
                .filter(|(seq, _)| **seq > cursor)
                .map(|(seq, bytes)| (*seq, StdArc::new(deserialize(bytes).unwrap())))
                .collect())
        }
        fn cursor(&self) -> u64 {
            self.cursor.load(Ordering::SeqCst)
        }
        fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    /// An object-store-shaped mock whose seal frontier is driven by the
    /// test: `seal_to` publishes a sealed sequence (as the S3 backend does
    /// after a segment PUT + manifest update) and wakes gated
    /// acknowledgements. Everything else mirrors `ScriptedStore`.
    /// `seal_interval` mirrors the S3 backend's reported sealing cadence.
    struct SealableStore {
        entries: StdMutex<BTreeMap<u64, Vec<u8>>>,
        cursor: AtomicU64,
        sealed: AtomicU64,
        seal_notify: Notify,
        seal_interval: Option<Duration>,
    }

    impl SealableStore {
        /// Publish a sealed frontier (monotonic) and wake waiters — the
        /// store-side half of the seal-gating contract.
        fn seal_to(&self, seq: u64) {
            self.sealed.fetch_max(seq, Ordering::SeqCst);
            self.seal_notify.notify_waiters();
        }
    }

    impl crate::wal::store::WalStore for SealableStore {
        fn kind(&self) -> &'static str {
            "object_store"
        }
        fn sealed_seq(&self) -> Option<u64> {
            Some(self.sealed.load(Ordering::SeqCst))
        }
        fn seal_notifier(&self) -> Option<&Notify> {
            Some(&self.seal_notify)
        }
        fn seal_interval(&self) -> Option<Duration> {
            self.seal_interval
        }
        fn append_batch(&self, entries: Vec<(u64, Vec<u8>)>) -> Result<(), Error> {
            let mut map = self.entries.lock().unwrap();
            for (seq, bytes) in entries {
                map.insert(seq, bytes);
            }
            Ok(())
        }
        fn advance_cursor(&self, seq: u64) -> Result<(), Error> {
            let mut current = self.cursor.load(Ordering::SeqCst);
            while seq > current {
                match self
                    .cursor
                    .compare_exchange(current, seq, Ordering::SeqCst, Ordering::SeqCst)
                {
                    Ok(_) => break,
                    Err(observed) => current = observed,
                }
            }
            Ok(())
        }
        fn rewind_cursor(&self, seq: u64) -> Result<(), Error> {
            self.cursor.store(seq, Ordering::SeqCst);
            Ok(())
        }
        fn read_after_cursor(&self) -> Result<Vec<(u64, crate::MessageBatchRef)>, Error> {
            let cursor = self.cursor.load(Ordering::SeqCst);
            let map = self.entries.lock().unwrap();
            Ok(map
                .iter()
                .filter(|(seq, _)| **seq > cursor)
                .map(|(seq, bytes)| (*seq, StdArc::new(deserialize(bytes).unwrap())))
                .collect())
        }
        fn cursor(&self) -> u64 {
            self.cursor.load(Ordering::SeqCst)
        }
        fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    /// An object-store-shaped store whose appends fail while the attempt
    /// count is below `fail_until` (shared, so a test can flip it mid-run)
    /// and succeed afterwards.
    struct FlakyAppendStore {
        attempts: StdArc<AtomicU64>,
        fail_until: StdArc<AtomicU64>,
    }

    impl crate::wal::store::WalStore for FlakyAppendStore {
        fn kind(&self) -> &'static str {
            "object_store"
        }
        fn append_batch(&self, _entries: Vec<(u64, Vec<u8>)>) -> Result<(), Error> {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
            if attempt < self.fail_until.load(Ordering::SeqCst) {
                return Err(Error::Process("scripted append failure".into()));
            }
            Ok(())
        }
        fn advance_cursor(&self, _seq: u64) -> Result<(), Error> {
            Ok(())
        }
        fn read_after_cursor(&self) -> Result<Vec<(u64, crate::MessageBatchRef)>, Error> {
            Ok(Vec::new())
        }
        fn cursor(&self) -> u64 {
            0
        }
        fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    /// Source ack whose commit always fails.
    struct FailingSourceAck;
    #[async_trait::async_trait]
    impl Ack for FailingSourceAck {
        async fn ack(&self) -> Result<(), Error> {
            Err(Error::Process("source commit failed".into()))
        }
    }

    /// Source ack that fails the first `failures_left` commits and succeeds
    /// afterwards: a transient source failure that a retry can clear.
    struct FlakySourceAck {
        failures_left: StdArc<AtomicU64>,
    }
    #[async_trait::async_trait]
    impl Ack for FlakySourceAck {
        async fn ack(&self) -> Result<(), Error> {
            if self.failures_left.fetch_sub(1, Ordering::SeqCst) == 0 {
                return Ok(());
            }
            Err(Error::Process("transient source commit failure".into()))
        }
    }

    /// Source ack whose commit and abort both fail.
    struct FailingAbortAck;
    #[async_trait::async_trait]
    impl Ack for FailingAbortAck {
        async fn ack(&self) -> Result<(), Error> {
            Err(Error::Process("source commit failed".into()))
        }
        async fn abort(&self) -> Result<(), Error> {
            Err(Error::Process("abort failed".into()))
        }
    }

    /// Source ack whose abort jumps the WAL cursor past its sequence, so the
    /// undo compensation observes a cursor that moved behind it mid-flight.
    struct CursorJumpingAbortAck {
        wal: StdMutex<Option<StdArc<Wal>>>,
        seq: u64,
    }
    #[async_trait::async_trait]
    impl Ack for CursorJumpingAbortAck {
        async fn ack(&self) -> Result<(), Error> {
            Err(Error::Process("source commit failed".into()))
        }
        async fn abort(&self) -> Result<(), Error> {
            let wal = self.wal.lock().unwrap().clone();
            if let Some(wal) = wal {
                advance_to(&wal, self.seq + 10).await;
            }
            Ok(())
        }
    }

    /// Source ack that blocks inside `ack` until a gate channel fires; used to
    /// hold a WAL acknowledgement in the in-flight state. `entered` is bumped
    /// as soon as `ack` runs, which proves the WAL caller set `in_flight`.
    struct GatedAck {
        gate: StdMutex<Option<tokio::sync::mpsc::Receiver<()>>>,
        entered: StdArc<AtomicU64>,
    }
    #[async_trait::async_trait]
    impl Ack for GatedAck {
        async fn ack(&self) -> Result<(), Error> {
            self.entered.fetch_add(1, Ordering::SeqCst);
            let mut receiver = self.gate.lock().unwrap().take();
            if let Some(receiver) = receiver.as_mut() {
                receiver.recv().await;
            }
            Ok(())
        }
    }

    /// Source ack that records held/release marker calls.
    struct HeldMarkerAck {
        held: StdArc<AtomicU64>,
        released: StdArc<AtomicU64>,
    }
    #[async_trait::async_trait]
    impl Ack for HeldMarkerAck {
        async fn ack(&self) -> Result<(), Error> {
            Ok(())
        }
        fn mark_held(&self) {
            self.held.fetch_add(1, Ordering::SeqCst);
        }
        fn release_held(&self) {
            self.released.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Poll the recovery read until it exposes `expected` entries.
    async fn wait_for_replay_len(wal: &StdArc<Wal>, expected: usize) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while wal.read_after_cursor().await.unwrap().len() != expected {
            assert!(
                std::time::Instant::now() < deadline,
                "replay never reached {expected} entries"
            );
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    }

    /// Poll an atomic counter until it reaches `expected`.
    async fn wait_for_counter(counter: &StdArc<AtomicU64>, expected: u64) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while counter.load(Ordering::SeqCst) != expected {
            assert!(
                std::time::Instant::now() < deadline,
                "counter never reached {expected}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    }

    /// Drive the committed cursor to `target` through contiguous `advance`
    /// steps (the frontier only moves over contiguous offsets from its seed).
    async fn advance_to(wal: &StdArc<Wal>, target: u64) {
        for seq in 1..=target {
            wal.advance(seq).await.unwrap();
        }
    }

    fn local_cfg(dir: &std::path::Path, policy: SyncPolicy) -> WalConfig {
        WalConfig::local(true, dir.to_string_lossy().to_string(), policy)
    }

    /// Drive a failing-append WAL through the wrapper surface: cursor reads,
    /// recovery reads, `advance` (including the notify branch), append error
    /// propagation, and close.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failing_append_store_covers_cursor_read_advance_and_close() {
        let store: StdArc<dyn crate::wal::store::WalStore> = StdArc::new(ScriptedStore {
            entries: StdMutex::new(BTreeMap::new()),
            cursor: AtomicU64::new(0),
            fail_append: true,
            fail_rewind: false,
            fail_mark_committed: false,
        });
        let config = WalConfig {
            sync: SyncPolicy::PerEntry,
            ..WalConfig::default()
        };
        let wal = Wal::open_with_store(&config, store, 1).unwrap();
        assert_eq!(wal.cursor().await.unwrap(), 0);
        assert!(wal.read_after_cursor().await.unwrap().is_empty());

        // `advance` pushes the target past the current cursor and notifies
        // (the frontier only moves over contiguous offsets from its seed).
        advance_to(&wal, 5).await;
        assert_eq!(wal.cursor().await.unwrap(), 5);
        // A lower target is a no-op.
        wal.advance(1).await.unwrap();
        assert_eq!(wal.cursor().await.unwrap(), 5);

        // Per-entry appends surface the store failure.
        assert!(wal.append(&StdArc::new(sample_batch(None))).await.is_err());
        wal.close().await.unwrap();
    }

    /// The `FailingAppendStore` mock's non-append surface is exercised
    /// directly: cursor/advance/read/close all succeed without state.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failing_append_store_trait_surface_is_reachable() {
        let store = FailingAppendStore {
            attempts: StdArc::new(AtomicU64::new(0)),
        };
        assert_eq!(store.cursor(), 0);
        store.advance_cursor(1).unwrap();
        assert!(store.read_after_cursor().unwrap().is_empty());
        assert!(store.append_batch(vec![(1, vec![0u8])]).is_err());
        store.close().unwrap();
    }

    /// The full acknowledgement failure contract: a failed source commit is
    /// compensated (cursor rewind, entry stays replayable), later sequences
    /// are fenced, and a retry of the lowest failed sequence clears the fence.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_source_ack_compensates_and_fences_then_a_retry_clears_it() {
        let dir = tempdir();
        let wal = Wal::open(&local_cfg(&dir, SyncPolicy::PerEntry)).unwrap();
        assert_eq!(
            wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
            1
        );
        assert_eq!(
            wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
            2
        );

        // The source commit fails after the cursor was advanced: the cursor is
        // compensated back to zero and both entries stay replayable.
        let flaky = StdArc::new(FlakySourceAck {
            failures_left: StdArc::new(AtomicU64::new(1)),
        });
        let failing: StdArc<dyn Ack> = StdArc::new(WalAck::new(wal.clone(), 1, flaky));
        let error = failing.ack().await.unwrap_err();
        assert!(error
            .to_string()
            .contains("transient source commit failure"));
        assert_eq!(wal.cursor().await.unwrap(), 0);
        assert_eq!(wal.read_after_cursor().await.unwrap().len(), 2);

        // A later sequence must not overtake the failed one.
        let later: StdArc<dyn Ack> = StdArc::new(WalAck::new(wal.clone(), 2, StdArc::new(NoopAck)));
        let error = later.ack().await.unwrap_err();
        assert!(error
            .to_string()
            .contains("blocked by an earlier source failure"));

        // Retrying the failed sequence clears the fence and completes: the
        // retry runs the stored (now healthy) source acknowledgement.
        let retry: StdArc<dyn Ack> = StdArc::new(WalAck::new(wal.clone(), 1, StdArc::new(NoopAck)));
        retry.ack().await.unwrap();
        assert_eq!(wal.cursor().await.unwrap(), 1);

        // The later sequence now drains in order.
        let drained: StdArc<dyn Ack> =
            StdArc::new(WalAck::new(wal.clone(), 2, StdArc::new(NoopAck)));
        drained.ack().await.unwrap();
        assert_eq!(wal.cursor().await.unwrap(), 2);
        wal.close().await.unwrap();
    }

    /// Two concurrent acknowledgements of one sequence share the outcome: the
    /// second caller returns through the "another caller completed it" path.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_acks_of_one_sequence_share_the_outcome() {
        let dir = tempdir();
        let wal = Wal::open(&local_cfg(&dir, SyncPolicy::PerEntry)).unwrap();
        assert_eq!(
            wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
            1
        );

        let (gate_tx, gate_rx) = tokio::sync::mpsc::channel::<()>(1);
        let entered = StdArc::new(AtomicU64::new(0));
        let first: StdArc<dyn Ack> = StdArc::new(WalAck::new(
            wal.clone(),
            1,
            StdArc::new(GatedAck {
                gate: StdMutex::new(Some(gate_rx)),
                entered: entered.clone(),
            }),
        ));
        let first_task = tokio::spawn(async move { first.ack().await });

        // The inner ack being entered proves the first caller holds the
        // sequence in-flight; the second caller must park behind it.
        wait_for_counter(&entered, 1).await;

        let second: StdArc<dyn Ack> =
            StdArc::new(WalAck::new(wal.clone(), 1, StdArc::new(NoopAck)));
        let second_task = tokio::spawn(async move { second.ack().await });
        // Let the second caller register and park behind the in-flight one.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        gate_tx.send(()).await.unwrap();
        first_task.await.unwrap().unwrap();
        second_task.await.unwrap().unwrap();
        assert_eq!(wal.cursor().await.unwrap(), 1);
        wal.close().await.unwrap();
    }

    /// A close request gives a parked acknowledgement a bounded drain window
    /// and then fails it, so recovery replays the unsettled delivery.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn close_fails_a_parked_acknowledgement_after_the_drain_window() {
        let dir = tempdir();
        let wal = Wal::open(&local_cfg(&dir, SyncPolicy::PerEntry)).unwrap();
        for expected in 1..=3 {
            assert_eq!(
                wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
                expected
            );
        }
        wal.override_ack_drain_window_for_tests(Duration::from_millis(50));

        // Sequence 3 cannot run while 1 and 2 are unacknowledged: it parks.
        let parked: StdArc<dyn Ack> =
            StdArc::new(WalAck::new(wal.clone(), 3, StdArc::new(NoopAck)));
        let task = tokio::spawn(async move { parked.ack().await });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        wal.close().await.unwrap();
        let error = task.await.unwrap().unwrap_err();
        assert!(error
            .to_string()
            .contains("WAL closed while acknowledgement was pending"));
    }

    /// A parked acknowledgement whose gap owner stalls forever gets an
    /// explicit retryable error after the park lease instead of waiting
    /// silently with no stream progress (fix-ack-stall-modes). The lease
    /// writes no `last_error` fence: once the (merely slow) owner settles,
    /// a retry of the parked sequence proceeds normally.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn parked_acknowledgement_fails_after_the_park_lease_when_the_gap_owner_stalls() {
        let dir = tempdir();
        let wal = Wal::open(&local_cfg(&dir, SyncPolicy::PerEntry)).unwrap();
        for expected in 1..=2 {
            assert_eq!(
                wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
                expected
            );
        }
        wal.override_ack_park_timeout_for_tests(Duration::from_millis(200));

        // The gap owner enters its source commit and never finishes: from
        // the parked caller's perspective the owner died silently (no fence
        // recorded, no notification ever coming).
        let (gate_tx, gate_rx) = tokio::sync::mpsc::channel::<()>(1);
        let entered = StdArc::new(AtomicU64::new(0));
        let owner: StdArc<dyn Ack> = StdArc::new(WalAck::new(
            wal.clone(),
            1,
            StdArc::new(GatedAck {
                gate: StdMutex::new(Some(gate_rx)),
                entered: entered.clone(),
            }),
        ));
        let owner_task = tokio::spawn(async move { owner.ack().await });
        wait_for_counter(&entered, 1).await;

        // Sequence 2 parks behind the in-flight 1; the lease must expire and
        // name the stuck sequence in the error.
        let parked: StdArc<dyn Ack> =
            StdArc::new(WalAck::new(wal.clone(), 2, StdArc::new(NoopAck)));
        let parked_task = tokio::spawn(async move { parked.ack().await });
        let error = parked_task.await.unwrap().unwrap_err().to_string();
        assert!(
            error.contains("WAL acknowledgement parked for >0.2s")
                && error.contains("waiting for sequence 1")
                && error.contains("the gap owner appears stalled"),
            "{error}"
        );

        // The stalled owner was only slow. Once it settles, a retry of the
        // parked sequence runs normally — the lease left no fence behind.
        gate_tx.send(()).await.unwrap();
        owner_task.await.unwrap().unwrap();
        let retry: StdArc<dyn Ack> = StdArc::new(WalAck::new(wal.clone(), 2, StdArc::new(NoopAck)));
        retry.ack().await.unwrap();
        assert_eq!(wal.cursor().await.unwrap(), 2);
        wal.close().await.unwrap();
    }

    /// A gap owner that settles within the lease is invisible to the parked
    /// caller: it is woken by the notification and completes normally, long
    /// before the lease fires (no added latency).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn parked_acknowledgement_proceeds_normally_when_the_gap_settles_within_the_lease() {
        let dir = tempdir();
        let wal = Wal::open(&local_cfg(&dir, SyncPolicy::PerEntry)).unwrap();
        for expected in 1..=2 {
            assert_eq!(
                wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
                expected
            );
        }
        wal.override_ack_park_timeout_for_tests(Duration::from_secs(2));

        let (gate_tx, gate_rx) = tokio::sync::mpsc::channel::<()>(1);
        let entered = StdArc::new(AtomicU64::new(0));
        let owner: StdArc<dyn Ack> = StdArc::new(WalAck::new(
            wal.clone(),
            1,
            StdArc::new(GatedAck {
                gate: StdMutex::new(Some(gate_rx)),
                entered: entered.clone(),
            }),
        ));
        let owner_task = tokio::spawn(async move { owner.ack().await });
        wait_for_counter(&entered, 1).await;

        let parked: StdArc<dyn Ack> =
            StdArc::new(WalAck::new(wal.clone(), 2, StdArc::new(NoopAck)));
        let parked_task = tokio::spawn(async move { parked.ack().await });
        // Let the second caller register and park, then settle the gap well
        // inside the lease.
        tokio::time::sleep(Duration::from_millis(100)).await;
        gate_tx.send(()).await.unwrap();
        owner_task.await.unwrap().unwrap();

        // If the lease wrongly gated completion, this would wait the full 2s
        // and then error; the notify path finishes in milliseconds.
        tokio::time::timeout(Duration::from_millis(500), parked_task)
            .await
            .expect("parked caller completes within the lease")
            .unwrap()
            .unwrap();
        assert_eq!(wal.cursor().await.unwrap(), 2);
        wal.close().await.unwrap();
    }

    /// Acknowledging a sequence the cursor already covers skips the cursor
    /// bump (and therefore needs no compensation on failure).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn acknowledging_a_covered_sequence_skips_the_cursor_bump() {
        let dir = tempdir();
        let wal = Wal::open(&local_cfg(&dir, SyncPolicy::PerEntry)).unwrap();
        assert_eq!(
            wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
            1
        );
        wal.advance(1).await.unwrap();

        let calls = StdArc::new(StdMutex::new(Vec::new()));
        let ack: StdArc<dyn Ack> = StdArc::new(WalAck::new(
            wal.clone(),
            1,
            StdArc::new(RecordingAck {
                sequence: 1,
                calls: calls.clone(),
            }),
        ));
        ack.ack().await.unwrap();
        assert_eq!(*calls.lock().unwrap(), vec![1]);
        assert_eq!(wal.cursor().await.unwrap(), 1);
        wal.close().await.unwrap();
    }

    /// Undo of sequences that were never acknowledged: a future sequence is a
    /// no-op, a sequence behind the cursor fails closed, and the sequence at
    /// the cursor compensates the source ack and rewinds.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn undo_of_unregistered_sequences_spans_no_op_behind_and_at_the_cursor() {
        let dir = tempdir();
        let wal = Wal::open(&local_cfg(&dir, SyncPolicy::PerEntry)).unwrap();
        assert_eq!(
            wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
            1
        );
        assert_eq!(
            wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
            2
        );

        // Future sequence, cursor below it: nothing to undo.
        WalAck::new(wal.clone(), 5, StdArc::new(NoopAck))
            .undo()
            .await
            .unwrap();

        // Behind a later cursor: refuse.
        advance_to(&wal, 2).await;
        let error = WalAck::new(wal.clone(), 1, StdArc::new(NoopAck))
            .undo()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("behind a later cursor"));

        // At the cursor: the wrapped source ack is undone and the cursor
        // rewinds.
        let undone = StdArc::new(AtomicU64::new(0));
        struct UndoCountingAck {
            undone: StdArc<AtomicU64>,
        }
        #[async_trait::async_trait]
        impl Ack for UndoCountingAck {
            async fn ack(&self) -> Result<(), Error> {
                Ok(())
            }
            async fn undo(&self) -> Result<(), Error> {
                self.undone.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }
        // The counting ack's own ack path is a plain success.
        UndoCountingAck {
            undone: undone.clone(),
        }
        .ack()
        .await
        .unwrap();
        WalAck::new(
            wal.clone(),
            2,
            StdArc::new(UndoCountingAck {
                undone: undone.clone(),
            }),
        )
        .undo()
        .await
        .unwrap();
        assert_eq!(undone.load(Ordering::SeqCst), 1);
        assert_eq!(wal.cursor().await.unwrap(), 1);
        wal.close().await.unwrap();
    }

    /// Undo of registered acknowledgements: a parked later delivery is simply
    /// removed, an in-flight delivery is refused, and an entry stranded behind
    /// a later cursor fails closed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn undo_of_registered_acknowledgements_removes_parks_or_rejects() {
        let dir = tempdir();
        let wal = Wal::open(&local_cfg(&dir, SyncPolicy::PerEntry)).unwrap();
        assert_eq!(
            wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
            1
        );
        assert_eq!(
            wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
            2
        );

        // Sequence 2 parks behind the unacknowledged 1.
        let parked: StdArc<dyn Ack> =
            StdArc::new(WalAck::new(wal.clone(), 2, StdArc::new(NoopAck)));
        let parked_task = tokio::spawn(async move { parked.ack().await });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Undo removes the parked entry; the parked caller then observes the
        // removal and completes successfully.
        WalAck::new(wal.clone(), 2, StdArc::new(NoopAck))
            .undo()
            .await
            .unwrap();
        parked_task.await.unwrap().unwrap();
        assert_eq!(wal.cursor().await.unwrap(), 0);

        // An in-flight acknowledgement cannot be undone.
        let (gate_tx, gate_rx) = tokio::sync::mpsc::channel::<()>(1);
        let entered = StdArc::new(AtomicU64::new(0));
        let in_flight: StdArc<dyn Ack> = StdArc::new(WalAck::new(
            wal.clone(),
            1,
            StdArc::new(GatedAck {
                gate: StdMutex::new(Some(gate_rx)),
                entered: entered.clone(),
            }),
        ));
        let in_flight_task = tokio::spawn(async move { in_flight.ack().await });
        // The inner ack being entered proves the WAL caller is in-flight.
        wait_for_counter(&entered, 1).await;
        let error = WalAck::new(wal.clone(), 1, StdArc::new(NoopAck))
            .undo()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("in-flight"));
        gate_tx.send(()).await.unwrap();
        in_flight_task.await.unwrap().unwrap();

        // A registered (failed) entry stranded behind a later cursor is
        // refused: sequencing 2 fails, then the cursor advances past it.
        let failing: StdArc<dyn Ack> =
            StdArc::new(WalAck::new(wal.clone(), 2, StdArc::new(FailingSourceAck)));
        failing.ack().await.unwrap_err();
        advance_to(&wal, 3).await;
        let error = WalAck::new(wal.clone(), 2, StdArc::new(NoopAck))
            .undo()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("behind a later cursor"));
        wal.close().await.unwrap();
    }

    /// A scripted store drives the compensation matrix: a successful rewind
    /// keeps the failed delivery replayable (and a later success reaches the
    /// store's reclaim success path), while a failed rewind surfaces the
    /// combined source-and-compensation error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scripted_store_drives_compensation_success_and_failure_paths() {
        let config = WalConfig {
            sync: SyncPolicy::PerEntry,
            ..WalConfig::default()
        };

        // Rewind succeeds: the failed delivery stays replayable and a retry
        // that succeeds reaches the store's reclaim success path.
        let store: StdArc<dyn crate::wal::store::WalStore> = StdArc::new(ScriptedStore {
            entries: StdMutex::new(BTreeMap::new()),
            cursor: AtomicU64::new(0),
            fail_append: false,
            fail_rewind: false,
            fail_mark_committed: false,
        });
        let wal = Wal::open_with_store(&config, store, 1).unwrap();
        assert_eq!(
            wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
            1
        );
        let failing: StdArc<dyn Ack> = StdArc::new(WalAck::new(
            wal.clone(),
            1,
            StdArc::new(FlakySourceAck {
                failures_left: StdArc::new(AtomicU64::new(1)),
            }),
        ));
        failing.ack().await.unwrap_err();
        assert_eq!(wal.cursor().await.unwrap(), 0);
        assert_eq!(wal.read_after_cursor().await.unwrap().len(), 1);
        let retry: StdArc<dyn Ack> = StdArc::new(WalAck::new(wal.clone(), 1, StdArc::new(NoopAck)));
        retry.ack().await.unwrap();
        assert_eq!(wal.cursor().await.unwrap(), 1);
        wal.close().await.unwrap();

        // Rewind fails: the source error is combined with the compensation
        // error so the caller sees that the cursor was left advanced.
        let store: StdArc<dyn crate::wal::store::WalStore> = StdArc::new(ScriptedStore {
            entries: StdMutex::new(BTreeMap::new()),
            cursor: AtomicU64::new(0),
            fail_append: false,
            fail_rewind: true,
            fail_mark_committed: false,
        });
        let wal = Wal::open_with_store(&config, store, 1).unwrap();
        assert_eq!(
            wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
            1
        );
        let failing: StdArc<dyn Ack> =
            StdArc::new(WalAck::new(wal.clone(), 1, StdArc::new(FailingSourceAck)));
        let error = failing.ack().await.unwrap_err();
        let message = error.to_string();
        assert!(message.contains("source commit failed"), "{message}");
        assert!(message.contains("cursor compensation failed"), "{message}");
        assert_eq!(wal.cursor().await.unwrap(), 1, "the rewind failed");
        wal.close().await.unwrap();
    }

    /// A failing source ack whose attempt never advanced the cursor returns
    /// the raw source error: there is nothing to compensate.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failing_ack_without_a_cursor_bump_returns_the_raw_error() {
        let dir = tempdir();
        let wal = Wal::open(&local_cfg(&dir, SyncPolicy::PerEntry)).unwrap();
        assert_eq!(
            wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
            1
        );
        advance_to(&wal, 1).await;

        let failing: StdArc<dyn Ack> =
            StdArc::new(WalAck::new(wal.clone(), 1, StdArc::new(FailingSourceAck)));
        let error = failing.ack().await.unwrap_err();
        let message = error.to_string();
        assert!(message.contains("source commit failed"), "{message}");
        assert!(!message.contains("compensation"), "{message}");
        assert_eq!(wal.cursor().await.unwrap(), 1);
        wal.close().await.unwrap();
    }

    /// Undo after a failed source commit drives the retry path: the stored
    /// source acknowledgement is aborted and the entry is discarded.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn undo_after_a_failed_ack_retries_the_source_abort() {
        let dir = tempdir();
        let wal = Wal::open(&local_cfg(&dir, SyncPolicy::PerEntry)).unwrap();
        assert_eq!(
            wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
            1
        );
        let failing: StdArc<dyn Ack> =
            StdArc::new(WalAck::new(wal.clone(), 1, StdArc::new(FailingSourceAck)));
        failing.ack().await.unwrap_err();

        // With the cursor moved back onto the failed sequence, the undo's
        // abort path rewinds it to the previous sequence before discarding.
        advance_to(&wal, 1).await;
        assert_eq!(wal.cursor().await.unwrap(), 1);
        WalAck::new(wal.clone(), 1, StdArc::new(NoopAck))
            .undo()
            .await
            .unwrap();
        assert_eq!(wal.cursor().await.unwrap(), 0);
        // The entry is gone: a fresh acknowledgement runs normally.
        let retry: StdArc<dyn Ack> = StdArc::new(WalAck::new(wal.clone(), 1, StdArc::new(NoopAck)));
        retry.ack().await.unwrap();
        assert_eq!(wal.cursor().await.unwrap(), 1);
        wal.close().await.unwrap();
    }

    /// An abort failure keeps the failed entry registered as a fence instead
    /// of silently discarding an uncompensated source commit.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn undo_after_a_failed_ack_whose_abort_fails_keeps_the_fence() {
        let dir = tempdir();
        let wal = Wal::open(&local_cfg(&dir, SyncPolicy::PerEntry)).unwrap();
        assert_eq!(
            wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
            1
        );
        let failing: StdArc<dyn Ack> =
            StdArc::new(WalAck::new(wal.clone(), 1, StdArc::new(FailingAbortAck)));
        failing.ack().await.unwrap_err();

        let error = WalAck::new(wal.clone(), 1, StdArc::new(NoopAck))
            .undo()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("abort failed"));

        // The fenced entry still blocks nothing: a later sequence continues to
        // observe the earlier failure.
        assert_eq!(
            wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
            2
        );
        let later: StdArc<dyn Ack> = StdArc::new(WalAck::new(wal.clone(), 2, StdArc::new(NoopAck)));
        let error = later.ack().await.unwrap_err();
        assert!(error
            .to_string()
            .contains("blocked by an earlier source failure"));
        wal.close().await.unwrap();
    }

    /// When the cursor moves behind a failed entry while its abort runs, the
    /// undo refuses instead of rewinding onto foreign territory.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn undo_after_an_abort_that_moves_the_cursor_behind_fails_closed() {
        let dir = tempdir();
        let wal = Wal::open(&local_cfg(&dir, SyncPolicy::PerEntry)).unwrap();
        assert_eq!(
            wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
            1
        );
        let failing: StdArc<dyn Ack> = StdArc::new(WalAck::new(
            wal.clone(),
            1,
            StdArc::new(CursorJumpingAbortAck {
                wal: StdMutex::new(Some(wal.clone())),
                seq: 1,
            }),
        ));
        failing.ack().await.unwrap_err();

        let error = WalAck::new(wal.clone(), 1, StdArc::new(NoopAck))
            .undo()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("behind a later cursor"));
        assert!(wal.cursor().await.unwrap() > 1);
        wal.close().await.unwrap();
    }

    /// A reclamation failure is reported without failing the acknowledgement:
    /// the source commit already succeeded, so at-least-once holds either way.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reclaim_failure_does_not_fail_the_acknowledgement() {
        let store: StdArc<dyn crate::wal::store::WalStore> = StdArc::new(ScriptedStore {
            entries: StdMutex::new(BTreeMap::new()),
            cursor: AtomicU64::new(0),
            fail_append: false,
            fail_rewind: false,
            fail_mark_committed: true,
        });
        let config = WalConfig {
            sync: SyncPolicy::PerEntry,
            ..WalConfig::default()
        };
        let wal = Wal::open_with_store(&config, store, 1).unwrap();
        assert_eq!(
            wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
            1
        );
        let ack: StdArc<dyn Ack> = StdArc::new(WalAck::new(wal.clone(), 1, StdArc::new(NoopAck)));
        ack.ack().await.unwrap();
        assert_eq!(wal.cursor().await.unwrap(), 1);
        wal.close().await.unwrap();
    }

    /// Reconciling checkpoint-covered sequences advances the cursor only over
    /// the contiguous prefix, and is idempotent for duplicates.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reconcile_covered_advances_only_contiguous_sequences() {
        let dir = tempdir();
        let wal = Wal::open(&local_cfg(&dir, SyncPolicy::PerEntry)).unwrap();
        for expected in 1..=3 {
            assert_eq!(
                wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
                expected
            );
        }

        // A gap (2 with cursor 0) does not advance anything.
        wal.reconcile_covered(&[2]).await.unwrap();
        assert_eq!(wal.cursor().await.unwrap(), 0);

        // The contiguous prefix advances.
        wal.reconcile_covered(&[1]).await.unwrap();
        assert_eq!(wal.cursor().await.unwrap(), 1);

        // Duplicates are deduplicated; the next contiguous step advances.
        wal.reconcile_covered(&[2, 2]).await.unwrap();
        assert_eq!(wal.cursor().await.unwrap(), 2);

        // Already-covered sequences are a no-op.
        wal.reconcile_covered(&[1, 2]).await.unwrap();
        assert_eq!(wal.cursor().await.unwrap(), 2);
        wal.close().await.unwrap();
    }

    /// The periodic policy flushes staged appends from the background flusher
    /// without an explicit flush call.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn periodic_policy_flushes_staged_appends_in_the_background() {
        let dir = tempdir();
        let wal = Wal::open(&local_cfg(
            &dir,
            SyncPolicy::Periodic(Duration::from_millis(2)),
        ))
        .unwrap();
        wal.append(&StdArc::new(sample_batch(None))).await.unwrap();
        wait_for_replay_len(&wal, 1).await;
        assert_eq!(wal.read_after_cursor().await.unwrap().len(), 1);
        wal.close().await.unwrap();
    }

    /// `WalAck` forwards the held/release markers to the wrapped source ack.
    #[test]
    fn wal_ack_delegates_held_markers_to_the_inner_ack() {
        let dir = tempdir();
        let wal = Wal::open(&local_cfg(&dir, SyncPolicy::PerEntry)).unwrap();
        let held = StdArc::new(AtomicU64::new(0));
        let released = StdArc::new(AtomicU64::new(0));
        let ack = WalAck::new(
            wal,
            1,
            StdArc::new(HeldMarkerAck {
                held: held.clone(),
                released: released.clone(),
            }),
        );
        ack.mark_held();
        assert_eq!(held.load(Ordering::SeqCst), 1);
        ack.release_held();
        assert_eq!(released.load(Ordering::SeqCst), 1);
    }

    // ---- seal gating / flusher visibility (fix-object-store-wal-loss-window) ----

    fn sealable_store() -> StdArc<SealableStore> {
        StdArc::new(SealableStore {
            entries: StdMutex::new(BTreeMap::new()),
            cursor: AtomicU64::new(0),
            sealed: AtomicU64::new(0),
            seal_notify: Notify::new(),
            seal_interval: None,
        })
    }

    fn sealing_wal(store: &StdArc<SealableStore>, policy: SyncPolicy) -> StdArc<Wal> {
        let dyn_store: StdArc<dyn crate::wal::store::WalStore> = store.clone();
        Wal::open_with_store(
            &WalConfig {
                sync: policy,
                ..WalConfig::default()
            },
            dyn_store,
            1,
        )
        .unwrap()
    }

    /// Task 1.5(a) — the crash-loss window regression: on a sealing backend
    /// an acknowledgement blocks before its source commit until the entry's
    /// sequence is sealed, and a completed acknowledgement implies
    /// `sealed_seq >= seq`. Pre-fix, the source commit ran while the entry
    /// was still only staged in memory — a crash there lost an
    /// already-committed record with no replay path.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ack_blocks_until_the_entry_is_sealed_then_completes() {
        let store = sealable_store();
        let wal = sealing_wal(&store, SyncPolicy::PerEntry);
        assert_eq!(
            wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
            1
        );

        let calls = StdArc::new(StdMutex::new(Vec::new()));
        let ack: StdArc<dyn Ack> = StdArc::new(WalAck::new(
            wal.clone(),
            1,
            StdArc::new(RecordingAck {
                sequence: 1,
                calls: calls.clone(),
            }),
        ));
        let task = tokio::spawn(async move { ack.ack().await });
        // Unsealed: the acknowledgement must park before its source commit —
        // the source offset may not advance past unsealed data.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !task.is_finished(),
            "the acknowledgement must block while sequence 1 is unsealed"
        );
        assert!(
            calls.lock().unwrap().is_empty(),
            "no source commit before the seal"
        );
        assert_eq!(wal.cursor().await.unwrap(), 0);

        // The seal publishes the frontier and wakes the gated caller.
        store.seal_to(1);
        task.await.unwrap().unwrap();
        assert_eq!(*calls.lock().unwrap(), vec![1]);
        assert_eq!(wal.cursor().await.unwrap(), 1);
        // "Acknowledged implies sealed" is already proven above: the
        // acknowledgement blocked while unsealed and only completed after
        // `seal_to` — no further assertion needed here.
        wal.close().await.unwrap();
    }

    /// Task 1.5(b): a seal-wait timeout enters the existing failure fence
    /// (`last_error`) — later sequences fail fast with "blocked by an
    /// earlier source failure" instead of parking — and a retry of the
    /// fenced entry recovers once the seal lands, draining the later
    /// sequence afterwards.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn seal_wait_timeout_fences_and_a_retry_recovers() {
        let store = sealable_store();
        let wal = sealing_wal(&store, SyncPolicy::PerEntry);
        wal.override_seal_wait_timeout_for_tests(Duration::from_millis(50));
        assert_eq!(
            wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
            1
        );
        assert_eq!(
            wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
            2
        );

        // Nothing ever seals the first entry: its acknowledgement times out
        // retryably, with the cursor untouched.
        let first: StdArc<dyn Ack> = StdArc::new(WalAck::new(wal.clone(), 1, StdArc::new(NoopAck)));
        let error = first.ack().await.unwrap_err();
        assert!(error.to_string().contains("timed out"), "{error}");
        assert_eq!(wal.cursor().await.unwrap(), 0);

        // The timeout went through the failure fence: even a fully sealed
        // later sequence fails fast instead of parking behind the fence.
        store.seal_to(2);
        let second: StdArc<dyn Ack> =
            StdArc::new(WalAck::new(wal.clone(), 2, StdArc::new(NoopAck)));
        let error = second.ack().await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("blocked by an earlier source failure"),
            "{error}"
        );

        // A retry of the fenced entry clears the fence and completes now
        // that the seal covers it; the later sequence then drains in order.
        let retry: StdArc<dyn Ack> = StdArc::new(WalAck::new(wal.clone(), 1, StdArc::new(NoopAck)));
        retry.ack().await.unwrap();
        let drained: StdArc<dyn Ack> =
            StdArc::new(WalAck::new(wal.clone(), 2, StdArc::new(NoopAck)));
        drained.ack().await.unwrap();
        assert_eq!(wal.cursor().await.unwrap(), 2);
        wal.close().await.unwrap();
    }

    /// CR follow-up: the seal-wait stall bound derives from the STORE's
    /// sealing cadence (not the engine sync policy) and is progress-based —
    /// a store that keeps advancing its sealed frontier never trips the
    /// bound even when each individual interval exceeds it, while a store
    /// that stops sealing entirely does.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn seal_wait_bound_is_cadence_derived_and_progress_based() {
        // Part 1: progress resets the deadline. Short override (200ms) but a
        // background "flusher" sealing every 50ms: the frontier for the
        // gated sequence must eventually be covered without a timeout —
        // pre-fix (fixed deadline from the sync policy) this timed out.
        let store = sealable_store();
        let wal = sealing_wal(&store, SyncPolicy::GroupCommit);
        wal.override_seal_wait_timeout_for_tests(Duration::from_millis(200));
        assert_eq!(
            wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
            1
        );
        let ack: StdArc<dyn Ack> = StdArc::new(WalAck::new(wal.clone(), 1, StdArc::new(NoopAck)));
        let task = tokio::spawn(async move { ack.ack().await });
        // Seal progressively below the gated sequence first, crossing the
        // override window several times while the frontier advances.
        for seq in 0..4 {
            tokio::time::sleep(Duration::from_millis(75)).await;
            store.seal_to(seq);
        }
        // The covering seal arrives well after 2x the override would have
        // fired without progress resets.
        tokio::time::sleep(Duration::from_millis(75)).await;
        store.seal_to(1);
        task.await.unwrap().unwrap();
        assert_eq!(wal.cursor().await.unwrap(), 1);
        wal.close().await.unwrap();

        // Part 2: no progress at all trips even the progress-reset bound.
        let store = sealable_store();
        let wal = sealing_wal(&store, SyncPolicy::GroupCommit);
        wal.override_seal_wait_timeout_for_tests(Duration::from_millis(100));
        assert_eq!(
            wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
            1
        );
        let ack: StdArc<dyn Ack> = StdArc::new(WalAck::new(wal.clone(), 1, StdArc::new(NoopAck)));
        let error = ack.ack().await.unwrap_err();
        assert!(error.to_string().contains("timed out"), "{error}");
        wal.close().await.unwrap();
    }

    /// Task 1.5(c): the seal wait is cancellation-safe — a caller dropped
    /// mid-wait leaves no in-flight flag or lost wakeup behind, so a later
    /// acknowledgement completes normally once the seal lands — and a close
    /// during the wait fails the acknowledgement retryably (recovery
    /// replays the entry) instead of hanging shutdown.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn seal_wait_is_cancellation_safe_and_close_fails_retryably() {
        // Part 1: drop a gated caller mid-wait, then seal.
        let store = sealable_store();
        let wal = sealing_wal(&store, SyncPolicy::PerEntry);
        // Long bound: any completion in this part must come from the seal,
        // never from the timeout firing.
        wal.override_seal_wait_timeout_for_tests(Duration::from_secs(60));
        assert_eq!(
            wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
            1
        );

        let gated: StdArc<dyn Ack> = StdArc::new(WalAck::new(wal.clone(), 1, StdArc::new(NoopAck)));
        let task = tokio::spawn(async move { gated.ack().await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!task.is_finished());
        task.abort(); // drop the caller mid-wait
        let _ = task.await;

        // No leak: the next acknowledgement completes promptly once sealed.
        store.seal_to(1);
        let retry: StdArc<dyn Ack> = StdArc::new(WalAck::new(wal.clone(), 1, StdArc::new(NoopAck)));
        let started = std::time::Instant::now();
        retry.ack().await.unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "a dropped waiter must not block the next acknowledgement"
        );
        wal.close().await.unwrap();

        // Part 2: a close during the wait fails the acknowledgement.
        let store = sealable_store();
        let wal = sealing_wal(&store, SyncPolicy::PerEntry);
        wal.override_seal_wait_timeout_for_tests(Duration::from_secs(60));
        assert_eq!(
            wal.append(&StdArc::new(sample_batch(None))).await.unwrap(),
            1
        );
        let gated: StdArc<dyn Ack> = StdArc::new(WalAck::new(wal.clone(), 1, StdArc::new(NoopAck)));
        let task = tokio::spawn(async move { gated.ack().await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!task.is_finished());
        wal.close().await.unwrap();
        let error = task.await.unwrap().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("WAL closed while sequence 1 was still waiting"),
            "{error}"
        );
    }

    /// Task 1.5(d): wake-path flush failures are counted (total and
    /// consecutive) instead of silently swallowed; sustained failure passes
    /// the escalation threshold, and a subsequent success resets the
    /// consecutive count while the total keeps the lifetime count.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flusher_failures_are_counted_and_escalate_until_a_success() {
        let attempts = StdArc::new(AtomicU64::new(0));
        let fail_until = StdArc::new(AtomicU64::new(u64::MAX));
        let store: StdArc<dyn crate::wal::store::WalStore> = StdArc::new(FlakyAppendStore {
            attempts: attempts.clone(),
            fail_until: fail_until.clone(),
        });
        let wal = Wal::open_with_store(
            &WalConfig {
                sync: SyncPolicy::GroupCommit,
                ..WalConfig::default()
            },
            store,
            1,
        )
        .unwrap();
        assert_eq!(wal.flush_failures(), 0);
        wal.append(&StdArc::new(sample_batch(None))).await.unwrap();

        // The wake branch retries hot (each failure restores the batch and
        // re-arms the notify): the counters must track every failed flush
        // and reach the escalation threshold.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while wal.flush_consecutive_failures() < WAL_FLUSH_ESCALATION_THRESHOLD {
            assert!(
                std::time::Instant::now() < deadline,
                "flush failures never reached the escalation threshold"
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let escalated = wal.flush_failures();
        assert!(escalated >= WAL_FLUSH_ESCALATION_THRESHOLD);
        assert!(attempts.load(Ordering::SeqCst) >= escalated);

        // A success resets the consecutive count; the total never regresses.
        fail_until.store(0, Ordering::SeqCst);
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while wal.flush_consecutive_failures() != 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "a successful flush never reset the consecutive count"
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        assert!(wal.flush_failures() >= escalated);
        wal.close().await.ok(); // the shutdown flush succeeding is not asserted here
    }
}
