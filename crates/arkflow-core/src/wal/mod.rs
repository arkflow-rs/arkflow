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
//! fsync / PUT cost, at the price of a small loss window if the process
//! crashes mid-flush.

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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Sync (fsync) policy for appends.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SyncPolicy {
    /// Commit (fsync) a transaction on every append. Fully durable; slowest.
    /// Not supported on remote backends (one PUT per message is not viable).
    PerEntry,
    /// Coalesce concurrent appends into shared transactions flushed as soon as
    /// pending data is available.
    GroupCommit,
    /// Flush pending appends on a fixed interval.
    Periodic(Duration),
}

impl Default for SyncPolicy {
    fn default() -> Self {
        SyncPolicy::GroupCommit
    }
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
    /// Next sequence number to assign. Append is single-threaded (the input
    /// worker), but an atomic keeps it race-free regardless.
    next_seq: AtomicU64,
    policy: SyncPolicy,
    // --- staging for group-commit / periodic ---
    pending: Mutex<Vec<(u64, Vec<u8>)>>,
    pending_notify: Notify,
    close: CancellationToken,
    flusher: Mutex<Option<JoinHandle<()>>>,
}

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

        let wal = Arc::new(Self {
            store,
            next_seq: AtomicU64::new(next_seq),
            policy: sync_policy,
            pending: Mutex::new(Vec::new()),
            pending_notify: Notify::new(),
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
    /// commit` and `periodic` stage the entry and return immediately; the
    /// background flusher commits batches to amortize the fsync cost. Under
    /// those two policies a crash before the next flush loses staged entries
    /// (the documented small loss window) — pick `per-entry` when every entry
    /// must survive a crash regardless of timing.
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
    /// path only after the downstream output confirms the write.
    pub async fn advance(&self, seq: u64) -> Result<(), Error> {
        self.store.advance_cursor(seq)
    }

    /// Read all entries with sequence strictly greater than the committed
    /// cursor, in ascending order. Used by recovery replay.
    pub async fn read_after_cursor(&self) -> Result<Vec<(u64, MessageBatchRef)>, Error> {
        self.store.read_after_cursor()
    }

    /// Current committed watermark (highest acked sequence, 0 if none).
    pub async fn cursor(&self) -> Result<u64, Error> {
        Ok(self.store.cursor())
    }

    async fn flush_pending(&self) -> Result<(), Error> {
        let batch: Vec<(u64, Vec<u8>)> = {
            let mut p = self.pending.lock().await;
            if p.is_empty() {
                return Ok(());
            }
            std::mem::take(p.as_mut())
        };
        self.store.append_batch(batch)
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

/// Acknowledgement decorator that advances the WAL cursor before delegating to
/// the source-side ack. Wired into the stream so the durable cursor only moves
/// past a message once the downstream output confirms the write.
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
        // Advance the durable cursor first (so the entry is reclaimable on
        // restart), then commit the source. If the source ack fails after the
        // cursor advanced, the source re-delivers — at-least-once, not loss.
        self.wal.advance(self.seq).await?;
        self.inner.ack().await?;
        Ok(())
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
    use std::sync::Arc as StdArc;

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
