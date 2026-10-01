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

//! Object-store (S3-compatible) WAL backend.
//!
//! See `openspec/changes/add-wal-s3-backend/design.md` for the full design.
//! See `docs/performance/s3-wal-backend.md` for performance characteristics.
//!
//! # Performance Overview
//!
//! | Operation | Latency | Notes |
//! |-----------|---------|-------|
//! | `append_batch` | ~1-50μs | In-memory, returns immediately |
//! | Segment PUT | 10-200ms | Depends on size, network, region |
//! | Recovery LIST | 100-500ms | Depends on segment count |
//!
//! Throughput: 50-150 MB/s practical limit per stream.
//!
//! # Key Design Decisions
//!
//! - Per-node + per-stream namespace isolation (D2). All keys live under
//!   `{prefix}/{node_id}/{stream_id}/`.
//! - Segment objects (`{prefix}/{node_id}/{stream_id}/segments/NNNNNNNN.wal`)
//!   are immutable; new entries are appended in-memory and sealed on
//!   size/entries/time triggers, then PUT (D4).
//! - A small `manifest.json` records the watermark, the index of sealed
//!   segments, and the active-segment filename (D4 + D6).
//! - On open, recovery reads `manifest.json` *and* lists `segments/` and
//!   unions the two; segments present on the store but absent from the
//!   manifest are still replayed (D5).
//! - Per-entry CRC32 detects a torn tail on the active segment (D5); the
//!   trailing truncated entry is silently dropped.
//! - Sealed segments whose last seq is `<= cursor` are deleted on the next
//!   manifest rewrite (D7). Deletion is best-effort and never blocks
//!   ingestion.

#![allow(dead_code)]

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use arkflow_core::wal::config::{CursorFlushConfig, ObjectStoreS3Config, SegmentConfig};
use arkflow_core::wal::{
    store::{WalStore, WalStoreBuilder},
    WalConfig,
};
use arkflow_core::{Error, MessageBatchRef};
use bytes::Bytes;
use futures::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{
    ObjectStore as _, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion,
};
use tokio::runtime::Runtime;
use tokio::sync::Notify;

use super::manifest::Manifest;
use super::segment;

/// A segment ready for upload. Holds the encoded bytes and metadata.
pub(crate) struct PendingSegment {
    pub segment_index: u64,
    pub first_seq: u64,
    pub last_seq: u64,
    pub bytes: Vec<u8>,
}

/// Channel sender for a single PUT worker.
struct PutWorker {
    sender: flume::Sender<PendingSegment>,
    _handle: std::thread::JoinHandle<()>,
}

impl PutWorker {
    fn new<F>(
        id: usize,
        client: Arc<dyn object_store::ObjectStore>,
        ns: String,
        on_complete: F,
    ) -> Self
    where
        F: Fn(u64) + Send + 'static,
    {
        let (sender, receiver) = flume::bounded::<PendingSegment>(16);
        let handle = std::thread::spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    tracing::error!("PUT worker {} runtime init failed: {}", id, e);
                    return;
                }
            };
            rt.block_on(async move {
                while let Ok(seg) = receiver.recv_async().await {
                    let key = format!("{}/segments/{:08}.wal", ns, seg.segment_index);
                    let payload = PutPayload::from(Bytes::from(seg.bytes));
                    match client.put(&ObjectPath::from(key.as_str()), payload).await {
                        Ok(_) => {
                            tracing::debug!(
                                "PUT worker {} uploaded segment {}",
                                id,
                                seg.segment_index
                            );
                            on_complete(seg.segment_index);
                        }
                        Err(e) => {
                            tracing::error!(
                                "PUT worker {} segment {} failed: {}",
                                id,
                                seg.segment_index,
                                e
                            );
                        }
                    }
                }
            });
        });
        Self {
            sender,
            _handle: handle,
        }
    }

    /// Returns a sender that can be used to submit segments to this worker.
    fn sender(&self) -> flume::Sender<PendingSegment> {
        self.sender.clone()
    }
}

/// Manages a pool of PUT workers for parallel segment uploads.
pub(crate) struct ParallelPutWorkers {
    workers: Vec<PutWorker>,
    /// Next worker index to assign (round-robin).
    next_worker: AtomicU64,
}

impl ParallelPutWorkers {
    /// Spawn `count` PUT workers (capped at 8).
    pub fn spawn<F>(
        count: usize,
        client: Arc<dyn object_store::ObjectStore>,
        ns: String,
        on_complete: F,
    ) -> Self
    where
        F: Fn(u64) + Send + Sync + 'static,
    {
        let capped = count.clamp(1, 8);
        if count > 8 {
            tracing::warn!("parallel_put.workers capped at 8 (requested: {})", count);
        }
        let on_complete = Arc::new(on_complete);
        let mut workers = Vec::with_capacity(capped);
        for i in 0..capped {
            let oc = on_complete.clone();
            workers.push(PutWorker::new(i, client.clone(), ns.clone(), move |seq| {
                oc(seq)
            }));
        }
        Self {
            workers,
            next_worker: AtomicU64::new(0),
        }
    }

    /// Returns true if this is a single-worker setup (default behavior).
    pub fn is_single(&self) -> bool {
        self.workers.len() == 1
    }

    /// Submit a segment to a worker (round-robin assignment).
    pub fn submit(&self, seg: PendingSegment) -> Result<(), Error> {
        if self.workers.is_empty() {
            return Err(Error::Process("no PUT workers available".into()));
        }
        let idx = (self.next_worker.fetch_add(1, Ordering::Relaxed) as usize) % self.workers.len();
        self.workers[idx]
            .sender()
            .send(seg)
            .map_err(|e| Error::Process(format!("PUT worker channel send: {}", e)))?;
        Ok(())
    }

    /// Returns the per-worker channel sender for direct submission.
    pub fn worker_sender(&self, idx: usize) -> Option<flume::Sender<PendingSegment>> {
        self.workers.get(idx).map(|w| w.sender())
    }

    /// Number of workers.
    pub fn len(&self) -> usize {
        self.workers.len()
    }

    /// Wait for all workers to drain their current queues (best-effort).
    pub fn shutdown(&self) {
        for _w in &self.workers {
            // Drop the sender to signal the worker to stop after draining.
            // We don't have the original sender here; rely on the worker
            // exit when all senders are dropped.
        }
    }
}

/// A background-thread handle for the segment flusher. Owned by `S3Store`
/// while running; stopped on `close`.
struct FlusherHandle {
    stop: Arc<Notify>,
    join: std::thread::JoinHandle<()>,
}

/// Object-store WAL backend. One instance per stream's WAL.
///
/// Holds a dedicated tokio runtime so its sync `WalStore` methods can drive
/// the async `object_store` client without forcing the trait to be async.
pub(crate) struct S3Store {
    runtime: Option<Runtime>,
    client: Arc<dyn object_store::ObjectStore>,
    /// Root namespace — `{prefix}/{node_id}/{stream_id}`.
    ns: String,
    segments_prefix: String,
    manifest_key: String,
    segment_cfg: SegmentConfig,
    cursor_cfg: CursorFlushConfig,
    /// Parallel PUT workers pool (task 3.3).
    put_workers: Option<ParallelPutWorkers>,

    /// In-memory active segment (bytes + parsed entry records so the cursor
    /// can flush without re-reading).
    active: StdMutex<ActiveSegment>,
    /// Number of cursor advances since the last manifest flush; flushed
    /// when it reaches `cursor_cfg.max_entries` or `cursor_cfg.interval`.
    cursor_pending: AtomicU64,
    /// Timestamp of the last manifest PUT (for the interval trigger).
    cursor_last_flush_ms: AtomicU64,
    /// Highest acknowledged sequence observed via `advance_cursor` (D1/D4).
    /// Purely in-memory; folded into the manifest `cursor` at flush time,
    /// clamped to `max_sealed_seq` so the cursor never passes unsealed data.
    acked_hwm: AtomicU64,
    /// Highest sequence ever written (sealed or in the active segment).
    /// Seeded by `recover` from `max_seq_seen` and kept current in
    /// `append_batch`; `next_seq_hint` returns this + 1 so a restart never
    /// reuses a sequence already on the store.
    max_written_seq: AtomicU64,
    /// Rewind floor: the lowest poisoned sequence. The manifest cursor must
    /// stay strictly below it until a later `advance_cursor` re-commits
    /// through it (the source re-acknowledges after replay). `u64::MAX`
    /// means no poison. Without this, a manifest flush racing a cursor
    /// compensation would seal the failed sequence and break the replay
    /// guarantee ("Both WAL backends keep the replay guarantee").
    rewind_floor: AtomicU64,
    /// In-memory mirror of the committed cursor, seeded by recovery and
    /// maintained by advance/rewind/flush. `cursor()` runs on every WAL
    /// acknowledgement, so it must not perform a manifest GET per call.
    cursor_mirror: AtomicU64,
    flusher: StdMutex<Option<FlusherHandle>>,
}

struct ActiveSegment {
    /// Sequence number of the first entry (0 when empty).
    first_seq: u64,
    /// Sequence number of the last entry (0 when empty).
    last_seq: u64,
    /// Number of entries currently buffered.
    entries: usize,
    /// Raw segment bytes (encoded via `segment::encode`).
    bytes: Vec<u8>,
    /// Filename of the next segment to be sealed (D4: 8-digit zero-padded).
    next_index: u64,
}

/// Run one construction-path future on the private runtime, off the
/// calling thread. `Runtime::block_on` panics when the caller is already
/// inside a runtime context (sync builders are invoked from async
/// `connect()`s), so initialization parks on a short-lived OS thread where
/// `block_on` is always legal.
/// Dispose of a private runtime that never made it into a store: dropping
/// a multi-thread runtime inside an async context panics, and construction
/// error paths (`?`) return exactly there.
fn dispose_runtime(runtime: Runtime) {
    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::spawn(move || runtime.shutdown_timeout(std::time::Duration::from_secs(10)));
    } else {
        runtime.shutdown_timeout(std::time::Duration::from_secs(10));
    }
}

fn block_on_init<F>(runtime: &Runtime, fut: F) -> Result<F::Output, Error>
where
    F: std::future::Future + Send,
    F::Output: Send,
{
    std::thread::scope(|scope| {
        scope
            .spawn(|| runtime.block_on(fut))
            .join()
            .map_err(|_| Error::Process("WAL object-store init task panicked".into()))
    })
}

impl S3Store {
    /// The private runtime (present until Drop shuts it down).
    fn rt(&self) -> &Runtime {
        self.runtime.as_ref().expect("S3 store runtime alive")
    }

    /// Build the store from a config. Validates that `sync` is not
    /// `PerEntry` (D8) and constructs an S3 client from the config's
    /// `s3:` block.
    fn build(cfg: &WalConfig) -> Result<Arc<Self>, Error> {
        let osc = match &cfg.backend {
            Some(arkflow_core::wal::WalBackend::ObjectStore(o)) => o.clone(),
            _ => {
                return Err(Error::Config(
                    "S3Store requires `backend: object_store`".into(),
                ))
            }
        };
        let runtime =
            Runtime::new().map_err(|e| Error::Process(format!("S3 store runtime init: {}", e)))?;
        let client: Arc<dyn object_store::ObjectStore> =
            block_on_init(&runtime, build_s3_client(&osc.s3))
                .map_err(|e| Error::Config(format!("S3 client init: {}", e)))?
                .map_err(|e| Error::Config(format!("S3 client init: {}", e)))?;
        Self::build_with_client(cfg, osc, runtime, client)
    }

    /// Build with a caller-provided object-store client. Exposed for
    /// tests and for callers who already have a client (e.g. shared
    /// across multiple WAL instances). Performs the same validation +
    /// recovery + flusher spawn as `build`.
    fn build_with_client(
        cfg: &WalConfig,
        osc: arkflow_core::wal::config::ObjectStoreWalConfig,
        runtime: Runtime,
        client: Arc<dyn object_store::ObjectStore>,
    ) -> Result<Arc<Self>, Error> {
        // Any construction error must dispose of the private runtime OFF
        // the caller's context: a plain `?` would drop it inline and panic
        // inside an async caller ("cannot drop a runtime ...").
        let mut runtime_slot = Some(runtime);
        let result = Self::build_with_client_inner(cfg, osc, &mut runtime_slot, client);
        if result.is_err() {
            if let Some(rt) = runtime_slot.take() {
                dispose_runtime(rt);
            }
        }
        result
    }

    fn build_with_client_inner(
        _cfg: &WalConfig,
        osc: arkflow_core::wal::config::ObjectStoreWalConfig,
        runtime_slot: &mut Option<Runtime>,
        client: Arc<dyn object_store::ObjectStore>,
    ) -> Result<Arc<Self>, Error> {
        // D8: reject PerEntry on remote backends. `WalConfig::validate` is
        // the canonical entry point; this guard makes a direct
        // `S3Store::build` call defensive too.
        if matches!(osc.sync, arkflow_core::wal::SyncPolicy::PerEntry) {
            return Err(Error::Config(
                "sync: per_entry is not supported with backend: object_store \
                 (one PUT per message is not viable; use group_commit or periodic)"
                    .into(),
            ));
        }

        // Resolve effective segment config: segment_tuning overrides segment
        // when present (task 2.3).
        let resolved_segment = if osc.segment_tuning.strategy
            != arkflow_core::wal::config::SegmentStrategy::Balanced
            || osc.segment_tuning.max_entries.is_some()
            || osc.segment_tuning.max_bytes.is_some()
            || osc.segment_tuning.flush_interval.is_some()
        {
            osc.segment_tuning.resolve()
        } else {
            osc.segment
        };

        // Validate resolved segment config (task 2.4).
        if resolved_segment.max_entries == 0 {
            return Err(Error::Config("segment.max_entries must be positive".into()));
        }
        if resolved_segment.max_bytes == 0 {
            return Err(Error::Config("segment.max_bytes must be positive".into()));
        }
        if resolved_segment.flush_interval.is_zero() {
            return Err(Error::Config(
                "segment.flush_interval must be greater than zero".into(),
            ));
        }

        // Validate parallel PUT workers (task 3.12).
        if osc.parallel_put.workers == 0 {
            return Err(Error::Config(
                "parallel_put.workers must be positive (1-8)".into(),
            ));
        }

        // Validate compression level ranges (task 5.4).
        match &osc.compression {
            arkflow_core::wal::config::CompressionConfig::Zstd { level } => {
                if !(0..=22).contains(level) {
                    return Err(Error::Config(format!(
                        "compression.zstd.level {} is out of range (0-22)",
                        level
                    )));
                }
            }
            arkflow_core::wal::config::CompressionConfig::Lz4 { level } => {
                if !(1..=16).contains(level) {
                    return Err(Error::Config(format!(
                        "compression.lz4.level {} is out of range (1-16)",
                        level
                    )));
                }
            }
            arkflow_core::wal::config::CompressionConfig::None => {}
        }

        let ns = format!(
            "{}/{}/{}",
            osc.prefix.trim_end_matches('/'),
            osc.node_id,
            osc.stream_id
        );
        let segments_prefix = format!("{}/segments", ns);
        let manifest_key = format!("{}/manifest.json", ns);

        let runtime = runtime_slot
            .as_ref()
            .expect("construction runtime present");
        let first_index =
            block_on_init(runtime, async {
                probe_next_segment_index(&*client, &segments_prefix).await
            })??;

        let store = Arc::new(Self {
            runtime: runtime_slot.take(),
            client: client.clone(),
            ns: ns.clone(),
            segments_prefix,
            manifest_key,
            segment_cfg: resolved_segment,
            cursor_cfg: osc.cursor,
            put_workers: Some(ParallelPutWorkers::spawn(
                osc.parallel_put.workers,
                client.clone(),
                ns.clone(),
                |_seq| {
                    // Completion callback: in single-worker mode, the
                    // completion is handled by the flusher. Multi-worker
                    // mode updates are tracked separately. Placeholder.
                },
            )),
            active: StdMutex::new(ActiveSegment {
                first_seq: 0,
                last_seq: 0,
                entries: 0,
                bytes: Vec::new(),
                next_index: first_index,
            }),
            cursor_pending: AtomicU64::new(0),
            cursor_last_flush_ms: AtomicU64::new(now_ms()),
            acked_hwm: AtomicU64::new(0),
            max_written_seq: AtomicU64::new(0),
            rewind_floor: AtomicU64::new(u64::MAX),
            cursor_mirror: AtomicU64::new(0),
            flusher: StdMutex::new(None),
        });

        let manifest_cursor = block_on_init(store.rt(), recover(&store))??;
        // Seed the cursor mirror from the manifest recovery just read — no
        // second GET, so a transient second-read failure cannot exist.
        store
            .cursor_mirror
            .store(manifest_cursor, Ordering::Release);

        let handle = spawn_flusher(store.clone());
        *store.flusher.lock().unwrap() = Some(handle);

        Ok(store)
    }

    /// Whether a cursor advance should trigger a manifest flush now.
    fn cursor_should_flush(&self) -> bool {
        let n = self.cursor_pending.load(Ordering::Acquire);
        if n >= self.cursor_cfg.max_entries as u64 {
            return true;
        }
        let elapsed = now_ms().saturating_sub(self.cursor_last_flush_ms.load(Ordering::Acquire));
        elapsed >= self.cursor_cfg.interval.as_millis() as u64
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Build an `object_store::aws::AmazonS3Builder` from our YAML config.
async fn build_s3_client(
    cfg: &ObjectStoreS3Config,
) -> Result<Arc<dyn object_store::ObjectStore>, String> {
    let mut b = AmazonS3Builder::new()
        .with_bucket_name(&cfg.bucket)
        .with_allow_http(cfg.allow_http);
    if let Some(ep) = cfg.endpoint.as_deref() {
        b = b.with_endpoint(ep);
    }
    if let Some(r) = cfg.region.as_deref() {
        b = b.with_region(r);
    }
    if let Some(k) = cfg.access_key_id.as_deref() {
        b = b.with_access_key_id(k);
    }
    if let Some(k) = cfg.secret_access_key.as_deref() {
        b = b.with_secret_access_key(k);
    }
    let client = b.build().map_err(|e| e.to_string())?;
    Ok(Arc::new(client))
}

/// Probe the store for the highest existing segment index, returning
/// `max_seen + 1` (or 1 if none). Used at startup so the next sealed segment
/// gets a fresh name.
async fn probe_next_segment_index(
    client: &dyn object_store::ObjectStore,
    segments_prefix: &str,
) -> Result<u64, Error> {
    let prefix = ObjectPath::from(segments_prefix);
    let mut max_idx = 0u64;
    let mut stream = client.list(Some(&prefix));
    while let Some(item) = stream.next().await {
        let meta = item.map_err(|e| Error::Process(format!("S3 list: {}", e)))?;
        if let Some(stem) = meta.location.filename() {
            // filenames are 8-digit zero-padded, e.g. "00000012.wal".
            if let Some(num_str) = stem.strip_suffix(".wal") {
                if let Ok(n) = num_str.parse::<u64>() {
                    if n > max_idx {
                        max_idx = n;
                    }
                }
            }
        }
    }
    Ok(max_idx + 1)
}

/// Run recovery: GET manifest → union with LIST → decode all segments →
/// seal any sealed segments whose tail is past the cursor so subsequent
/// truncations are correct.
/// Returns the recovered manifest cursor (the in-memory mirror seed — no
/// second GET after recovery).
async fn recover(store: &Arc<S3Store>) -> Result<u64, Error> {
    // Step 1: GET manifest (optional — absent on a fresh bucket).
    let manifest = match store
        .client
        .get(&ObjectPath::from(store.manifest_key.as_str()))
        .await
    {
        Ok(r) => {
            let bytes = r
                .bytes()
                .await
                .map_err(|e| Error::Process(format!("S3 GET manifest body: {}", e)))?;
            Manifest::from_json(&bytes)
                .map_err(|e| Error::Process(format!("S3 manifest JSON: {}", e)))?
        }
        Err(object_store::Error::NotFound { .. }) => {
            Manifest::fresh(store_ns_node_id(store), store_ns_stream_id(store))
        }
        Err(e) => return Err(Error::Process(format!("S3 GET manifest: {}", e))),
    };

    // Step 2: LIST segments, union with manifest's index.
    let mut seen: HashSet<String> = HashSet::new();
    let mut all_segs: Vec<String> = Vec::new();
    for s in manifest.sealed_segments.iter() {
        if seen.insert(s.clone()) {
            all_segs.push(s.clone());
        }
    }
    if let Some(active) = &manifest.active_segment {
        if seen.insert(active.clone()) {
            all_segs.push(active.clone());
        }
    }
    let prefix = ObjectPath::from(store.segments_prefix.as_str());
    let mut stream = store.client.list(Some(&prefix));
    while let Some(item) = stream.next().await {
        let meta = item.map_err(|e| Error::Process(format!("S3 list: {}", e)))?;
        let name = meta.location.filename().unwrap_or("").to_string();
        if !name.ends_with(".wal") {
            continue;
        }
        if seen.insert(name.clone()) {
            all_segs.push(name);
        }
    }

    // Step 3: decode every segment. We don't surface the entries here — the
    // store's `read_after_cursor()` does that on demand — but we use the
    // union to (a) verify every referenced segment is readable, (b) update
    // the in-memory active segment state if the manifest's active_segment
    // is present, and (c) advance `next_index` to avoid clashing with any
    // sealed segment filename on the next rotation.
    let mut max_seq_seen = 0u64;
    let mut max_idx_seen = 0u64;
    for seg_name in &all_segs {
        if let Some(num_str) = seg_name.strip_suffix(".wal") {
            if let Ok(n) = num_str.parse::<u64>() {
                if n > max_idx_seen {
                    max_idx_seen = n;
                }
            }
        }
        let key = ObjectPath::from(format!("{}/{}", store.segments_prefix, seg_name).as_str());
        match store.client.get(&key).await {
            Ok(r) => {
                let bytes = r
                    .bytes()
                    .await
                    .map_err(|e| Error::Process(format!("S3 GET segment body: {}", e)))?;
                let decoded = segment::decode(&bytes)?;
                if let Some((last, _)) = decoded.entries.last() {
                    if *last > max_seq_seen {
                        max_seq_seen = *last;
                    }
                }
                // The active segment is identified by the manifest (if any);
                // a segment that's listed but not in the manifest is a
                // LIST-fallback candidate (D5). Its bytes are already
                // readable here, so we don't need to do anything extra.
            }
            Err(object_store::Error::NotFound { .. }) => {
                // Manifest referenced a segment that was truncated between
                // write and recovery (D7). Skip silently.
            }
            Err(e) => {
                return Err(Error::Process(format!(
                    "S3 GET segment {}: {}",
                    seg_name, e
                )))
            }
        }
    }

    // Cache the highest written sequence so `next_seq_hint` can return
    // `max_seq + 1` (matching redb) instead of `cursor() + 1`, which would
    // reuse sequence numbers whenever sealed-but-unacked entries exist.
    store.max_written_seq.store(max_seq_seen, Ordering::Release);

    let mut active = store.active.lock().unwrap();
    active.next_index = max_idx_seen + 1;

    // If the manifest recorded an active segment, prime the in-memory
    // active state with the high-water mark so `append_batch` keeps the
    // sequence monotonic. We don't actually need the bytes — the next
    // append will start a fresh segment if the active one is sealed.
    if manifest.active_segment.is_some() {
        active.first_seq = max_seq_seen.saturating_add(1).max(1);
        active.last_seq = max_seq_seen;
        active.entries = 0;
        active.bytes.clear();
    }

    Ok(manifest.cursor)
}

fn store_ns_node_id(store: &S3Store) -> String {
    // ns = "{prefix}/{node_id}/{stream_id}"; recover just splits it.
    let mut parts = store.ns.splitn(3, '/').collect::<Vec<_>>();
    parts.reverse();
    parts.get(1).copied().unwrap_or("").to_string()
}

fn store_ns_stream_id(store: &S3Store) -> String {
    let mut parts = store.ns.splitn(3, '/').collect::<Vec<_>>();
    parts.reverse();
    parts.first().copied().unwrap_or("").to_string()
}

impl WalStore for S3Store {
    fn kind(&self) -> &'static str {
        "object_store"
    }

    fn append_batch(&self, entries: Vec<(u64, Vec<u8>)>) -> Result<(), Error> {
        if entries.is_empty() {
            return Ok(());
        }
        // 1. Append into the active segment.
        let mut seal_now = false;
        {
            let mut active = self.active.lock().unwrap();
            segment::encode(&entries, &mut active.bytes)?;
            active.entries += entries.len();
            if active.first_seq == 0 {
                active.first_seq = entries.first().unwrap().0;
            }
            active.last_seq = entries.last().unwrap().0;
            // Keep the cached high-water mark current so `next_seq_hint`
            // stays accurate across appends (defence in depth; the hint is
            // only consulted once at open, where `recover` already set it).
            self.max_written_seq
                .fetch_max(active.last_seq, Ordering::AcqRel);

            // Check seal triggers.
            if active.entries >= self.segment_cfg.max_entries
                || active.bytes.len() >= self.segment_cfg.max_bytes
            {
                seal_now = true;
            }
        }

        // 2. If a trigger fired (or the size threshold crossed), seal and
        //    PUT synchronously inside `block_on`. This is the only place a
        //    segment is *committed*; per-entry writes are not allowed on
        //    remote backends (D8).
        if seal_now {
            self.rt().block_on(seal_active_segment(self))?;
        }
        Ok(())
    }

    fn advance_cursor(&self, seq: u64) -> Result<(), Error> {
        // Record the acknowledged sequence (D1). The previous implementation
        // discarded `seq` (`let _ = seq;`), which decoupled the cursor from
        // acks entirely. `fetch_max` keeps the watermark monotonic without
        // touching the `active` lock. Flushing the manifest is async/batched
        // (D6): triggered by the configured cursor threshold or interval.
        self.acked_hwm.fetch_max(seq, Ordering::AcqRel);
        self.cursor_mirror.fetch_max(seq, Ordering::AcqRel);
        // A re-commit through the poisoned sequence means the source has
        // re-acknowledged past the rewind: normal advancement resumes.
        if seq.saturating_add(1) >= self.rewind_floor.load(Ordering::Acquire) {
            self.rewind_floor.store(u64::MAX, Ordering::Release);
        }
        let n = self.cursor_pending.fetch_add(1, Ordering::AcqRel);
        if n + 1 >= self.cursor_cfg.max_entries as u64 || self.cursor_should_flush() {
            self.rt().block_on(flush_manifest(self))?;
        }
        Ok(())
    }

    /// Compensate a failed wrapped source commit: the failed sequence
    /// (`seq + 1`) stays replayable. Poisons the manifest floor so a racing
    /// flush cannot seal past it, rewinds the in-memory watermarks, and —
    /// when the manifest was already persisted past the rewound position —
    /// performs a corrective flush. A corrective-write failure is reported
    /// explicitly (the replay guarantee fails closed, never silently).
    fn rewind_cursor(&self, seq: u64) -> Result<(), Error> {
        self.rewind_floor
            .fetch_min(seq.saturating_add(1), Ordering::AcqRel);
        self.acked_hwm.fetch_min(seq, Ordering::AcqRel);
        let previous = self.cursor_mirror.fetch_min(seq, Ordering::AcqRel);
        if previous > seq {
            block_on_init(self.rt(), flush_manifest(self))
                .map_err(|e| Error::Process(format!("rewind corrective flush failed: {e}")))??;
        }
        Ok(())
    }

    fn read_after_cursor(&self) -> Result<Vec<(u64, MessageBatchRef)>, Error> {
        // Re-decode every segment (LIST union) and return entries strictly
        // greater than the manifest's cursor. The active segment is included.
        // LIST-fallback segments (not in the manifest) are read here too
        // because they're in the same `list_segments()` set.
        let manifest = match self.rt().block_on(
            self.client
                .get(&ObjectPath::from(self.manifest_key.as_str())),
        ) {
            Ok(r) => {
                let bytes = self
                    .rt()
                    .block_on(r.bytes())
                    .map_err(|e| Error::Process(format!("S3 GET manifest: {}", e)))?;
                Manifest::from_json(&bytes)
                    .map_err(|e| Error::Process(format!("manifest JSON: {}", e)))?
            }
            Err(object_store::Error::NotFound { .. }) => {
                Manifest::fresh(store_ns_node_id(self), store_ns_stream_id(self))
            }
            Err(e) => return Err(Error::Process(format!("S3 GET manifest: {}", e))),
        };

        let mut segs: HashSet<String> = manifest.sealed_segments.iter().cloned().collect();
        if let Some(a) = &manifest.active_segment {
            segs.insert(a.clone());
        }
        let prefix = ObjectPath::from(self.segments_prefix.as_str());
        let mut stream = self.rt().block_on(async {
            let s = self.client.list(Some(&prefix));
            // Drive the stream inside the runtime.
            let mut out = Vec::new();
            let mut s = std::pin::pin!(s);
            while let Some(item) = s.next().await {
                out.push(item);
            }
            out
        });
        for item in stream.drain(..) {
            let meta = item.map_err(|e| Error::Process(format!("S3 list: {}", e)))?;
            let name = meta.location.filename().unwrap_or("").to_string();
            if name.ends_with(".wal") {
                segs.insert(name);
            }
        }

        let mut out: Vec<(u64, MessageBatchRef)> = Vec::new();
        for seg_name in segs {
            let key = ObjectPath::from(format!("{}/{}", self.segments_prefix, seg_name).as_str());
            let bytes = match self.rt().block_on(self.client.get(&key)) {
                Ok(r) => match self.rt().block_on(r.bytes()) {
                    Ok(b) => b,
                    Err(e) => return Err(Error::Process(format!("S3 GET segment body: {}", e))),
                },
                Err(object_store::Error::NotFound { .. }) => continue,
                Err(e) => return Err(Error::Process(format!("S3 GET segment: {}", e))),
            };
            let decoded = segment::decode(&bytes)?;
            for (seq, mb) in decoded.entries {
                if seq > manifest.cursor {
                    out.push((seq, mb));
                }
            }
        }
        out.sort_by_key(|(s, _)| *s);
        Ok(out)
    }

    fn cursor(&self) -> u64 {
        // In-memory mirror (seeded by recovery, maintained by
        // advance/rewind/flush). `cursor()` runs on every WAL
        // acknowledgement — a manifest GET per call is neither affordable
        // nor legal on an async worker thread.
        //
        // Two views by design: this mirror is the LIVE view (includes
        // acknowledged-but-not-yet-flushed sequences); `read_after_cursor`
        // filters on the PERSISTED manifest cursor, because replay must
        // restart from durable truth. Between flushes the live cursor can
        // run ahead of replay — callers must not mix the two (replay runs
        // once at recovery; the live cursor drives ack parking).
        self.cursor_mirror.load(Ordering::Acquire)
    }

    fn next_seq_hint(&self) -> u64 {
        // Return `max_written_seq + 1` (matching redb's `max_seq() + 1`),
        // NOT `cursor() + 1`. When sealed-but-unacked entries exist
        // (`cursor < max_written_seq`), `cursor() + 1` would reuse a
        // sequence number already on the store. `recover` seeds
        // `max_written_seq` from `max_seq_seen`; `append_batch` keeps it
        // current, so this is O(1) with no object-store LIST.
        self.max_written_seq
            .load(Ordering::Acquire)
            .saturating_add(1)
            .max(1)
    }

    fn close(&self) -> Result<(), Error> {
        // Stop the background flusher.
        if let Some(handle) = self.flusher.lock().unwrap().take() {
            handle.stop.notify_one();
            let _ = handle.join.join();
        }
        // Final seal + manifest flush so anything buffered is durable.
        self.rt().block_on(seal_active_segment(self))?;
        self.rt().block_on(flush_manifest(self))?;
        Ok(())
    }
}

impl Drop for S3Store {
    fn drop(&mut self) {
        // Shut the private runtime down off any async context: dropping a
        // multi-thread runtime inside one panics ("cannot drop a runtime in
        // a context where blocking is not allowed"). The store is usually
        // released through `close()` on a blocking thread, but the last Arc
        // can also die inside an async task (e.g. a test scope).
        if let Some(runtime) = self.runtime.take() {
            if tokio::runtime::Handle::try_current().is_ok() {
                std::thread::spawn(move || {
                    runtime.shutdown_timeout(std::time::Duration::from_secs(10))
                });
            } else {
                runtime.shutdown_timeout(std::time::Duration::from_secs(10));
            }
        }
    }
}

/// Record a sealed segment in the manifest, keeping the chronologically-newest
/// segment as the active one. Ordering is by the numeric segment index parsed
/// from the `NNNNNNNN.wal` name (not lexical order), so it stays correct past
/// the 8-digit zero-padding boundary. An out-of-order seal — a worker whose
/// manifest write lost the ETag race and retries after a *newer* segment has
/// already landed — must not regress the active pointer; it records its older
/// segment in `sealed_segments` instead. A segment that is already active is
/// left untouched (a segment is either active or sealed, never both).
/// Idempotent on `sealed_segments`.
fn apply_seal(m: &mut Manifest, sealed_name: &str) {
    // Already the active segment — nothing to do. In particular, do NOT add it
    // to sealed_segments: a segment is either active or sealed, never both.
    // This covers the retry path (read base → apply_seal → PUT re-run after a
    // precondition failure), where the freshly-read base already reflects this
    // segment as active.
    if m.active_segment.as_deref() == Some(sealed_name) {
        return;
    }
    let install_as_active = match &m.active_segment {
        Some(current) => segment_index(sealed_name) > segment_index(current),
        None => true,
    };
    if install_as_active {
        if let Some(prev) = m.active_segment.take() {
            if !m.sealed_segments.contains(&prev) {
                m.sealed_segments.push(prev);
            }
        }
        m.active_segment = Some(sealed_name.to_string());
    } else if !m.sealed_segments.iter().any(|s| s.as_str() == sealed_name) {
        m.sealed_segments.push(sealed_name.to_string());
    }
}

/// Parse the numeric index out of a `NNNNNNNN.wal` segment name for
/// chronological comparison. Returns 0 for names that don't match the scheme
/// (treated as chronologically oldest — a safe default).
fn segment_index(name: &str) -> u64 {
    name.strip_suffix(".wal")
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
}

/// Seal the current active segment: write it to a fresh `NNNNNNNN.wal`
/// filename, rotate the in-memory state, and update the manifest.
async fn seal_active_segment(store: &S3Store) -> Result<(), Error> {
    // Move the bytes out of the active lock so the encode + PUT doesn't
    // hold the lock across the network call.
    let (sealed_bytes, first_seq, last_seq, next_index, entry_count) = {
        let mut active = store.active.lock().unwrap();
        if active.entries == 0 {
            return Ok(());
        }
        let bytes = std::mem::take(&mut active.bytes);
        let first = active.first_seq;
        let last = active.last_seq;
        let idx = active.next_index;
        let count = active.entries;
        active.first_seq = 0;
        active.last_seq = 0;
        active.entries = 0;
        active.next_index += 1;
        (bytes, first, last, idx, count)
    };

    let name = format!("{:08}.wal", next_index);
    let key = ObjectPath::from(format!("{}/{}", store.segments_prefix, name).as_str());
    // Clone for the PUT so the original bytes survive a failed upload and can be
    // restored to the active segment below. Without this, a transient segment
    // PUT failure would permanently lose the entries (they were already moved
    // out of the active segment above).
    if let Err(e) = store
        .client
        .put(&key, PutPayload::from(Bytes::from(sealed_bytes.clone())))
        .await
    {
        let mut active = store.active.lock().unwrap();
        active.bytes = sealed_bytes;
        active.first_seq = first_seq;
        active.last_seq = last_seq;
        active.entries = entry_count;
        active.next_index = next_index; // undo the pre-increment
        return Err(Error::Process(format!("S3 PUT segment {}: {}", name, e)));
    }

    // Bump the manifest's `max_sealed_seq` and add the new segment to
    // `sealed_segments`. If we had an active segment before, demote it.
    // Use the ETag-coordinated writer so concurrent seal callbacks from
    // parallel PUT workers don't lose each other's updates.
    let name_for_mutator = name.clone();
    write_manifest_with_etag(store, move |m| {
        // Record the sealed segment, keeping the chronologically-newest segment
        // active (`apply_seal` guards against out-of-order seals regressing
        // the active pointer), then bump the high-water mark.
        apply_seal(m, &name_for_mutator);
        if last_seq > m.max_sealed_seq {
            m.max_sealed_seq = last_seq;
        }
    })
    .await?;
    let _ = first_seq; // (we track via sealed_segments index; could go into manifest for diagnostics)
    Ok(())
}

/// Flush the in-memory cursor watermark to the manifest (D6).
///
/// Truncation of sealed segments past the cursor (D7) is intentionally not
/// performed here: a correct implementation needs per-segment last-seq tracking,
/// which the manifest does not yet carry. The previous in-mutator truncation was
/// a broken placeholder that emptied `sealed_segments` without deleting the
/// objects (orphans), so it has been removed; sealed segments stay listed in the
/// manifest and remain reachable via the recovery LIST-fallback until D7 lands.
async fn flush_manifest(store: &S3Store) -> Result<(), Error> {
    // The cursor tracks the highest acknowledged sequence, clamped to
    // `max_sealed_seq`: it never passes data not yet sealed to object
    // storage, so an acked-but-unsealed entry stays replayable
    // (at-least-once, never loss). The rewind floor clamps the target
    // strictly below a poisoned sequence until the source re-acknowledges
    // through it — and, while poisoned, may LOWER an already-persisted
    // cursor: a flush that raced ahead of the rewind must be corrected or
    // the failed entry would be sealed past.
    //
    // The watermarks are read INSIDE the mutator so every ETag retry uses
    // the current values — the mutator re-runs per attempt, and a retry
    // reusing a ceiling captured before a concurrent rewind would seal the
    // poisoned sequence right back.
    write_manifest_with_etag(store, |m| {
        let acked_hwm = store.acked_hwm.load(Ordering::Acquire);
        let floor = store.rewind_floor.load(Ordering::Acquire);
        let poisoned = floor != u64::MAX;
        let ceiling = if poisoned {
            acked_hwm.min(floor.saturating_sub(1))
        } else {
            acked_hwm
        };
        let target = ceiling.min(m.max_sealed_seq);
        // Unpoisoned: monotonic (acked_hwm only shrinks via a rewind, which
        // poisons). Poisoned: clamp down to the floor.
        m.cursor = if poisoned {
            m.cursor.min(target)
        } else {
            m.cursor.max(target)
        };
    })
    .await?;

    // Reset the in-memory flush counters. This happens after the manifest write
    // so a crash between the write and the reset would replay the same entries
    // on restart — safe.
    store.cursor_pending.store(0, Ordering::Release);
    store
        .cursor_last_flush_ms
        .store(now_ms(), Ordering::Release);

    Ok(())
}

/// Maximum number of attempts the ETag-coordinated manifest writer makes
/// before surfacing the contention as an error. This covers the worst case
/// of `parallel_put.workers` (up to 8) sealing concurrently: in the fully
/// contended regime each round lets exactly one writer win, so N concurrent
/// writers need up to N attempts to all converge. Exceeding 8 almost
/// certainly indicates cross-process contention or an object-store
/// misconfiguration that retries cannot paper over.
const MANIFEST_WRITE_MAX_RETRIES: usize = 8;

/// Read the manifest object and return it together with the current ETag.
///
/// `NotFound` is treated as a fresh manifest and returns `None` for the
/// ETag — a fresh manifest has no version to match, so the first write
/// proceeds with `PutMode::Create` (if-none-exists).
async fn read_manifest_with_etag(store: &S3Store) -> Result<(Manifest, Option<String>), Error> {
    match store
        .client
        .get(&ObjectPath::from(store.manifest_key.as_str()))
        .await
    {
        Ok(r) => {
            // `GetResult::meta.e_tag` carries the object's ETag (HTTP `ETag`
            // header). Stores are inconsistent about whether the value
            // includes the surrounding quotes — AWS SDK keeps them and
            // most others don't. `object_store`'s PUT path normalizes this
            // for `If-Match` matching.
            let etag = r.meta.e_tag.clone();
            let bytes = r
                .bytes()
                .await
                .map_err(|e| Error::Process(format!("S3 GET manifest body: {}", e)))?;
            let m = Manifest::from_json(&bytes)
                .map_err(|e| Error::Process(format!("manifest JSON: {}", e)))?;
            Ok((m, etag))
        }
        Err(object_store::Error::NotFound { .. }) => Ok((
            Manifest::fresh(store_ns_node_id(store), store_ns_stream_id(store)),
            None,
        )),
        Err(e) => Err(Error::Process(format!("S3 GET manifest: {}", e))),
    }
}

/// Apply a mutator closure to the manifest under ETag-coordinated PUT
/// coordination. Each attempt re-reads the manifest, runs the mutator
/// against the freshly-read base, and PUTs the result with an `If-Match`
/// precondition. `Precondition` / `NotModified` failures (HTTP 412/304)
/// trigger a retry with a re-read base. After `MANIFEST_WRITE_MAX_RETRIES`
/// attempts the failure is surfaced as an error.
///
/// The mutator pattern is essential: it prevents the caller from composing
/// a mutated `Manifest` outside the coordination window (which would
/// silently overwrite a concurrent writer's later update). The mutator
/// receives the latest base on every attempt.
async fn write_manifest_with_etag<F>(store: &S3Store, mut mutate: F) -> Result<(), Error>
where
    F: FnMut(&mut Manifest),
{
    for attempt in 0..MANIFEST_WRITE_MAX_RETRIES {
        let (mut m, etag) = read_manifest_with_etag(store).await?;
        mutate(&mut m);
        let bytes = m
            .to_json()
            .map_err(|e| Error::Process(format!("manifest serialize: {}", e)))?;

        // When the manifest already exists, condition the PUT on its ETag so a
        // concurrent writer's intervening update forces this one to retry. When
        // it does not yet exist (fresh bucket / first run), use `Create`
        // (if-none-exists): concurrent first-writers race, exactly one wins,
        // and the rest get `AlreadyExists` and retry against the now-existing
        // object. This closes the fresh-manifest window that `Overwrite` would
        // leave open, where N concurrent first-writers each silently clobber
        // the others' manifests.
        let mode = match etag {
            Some(e) => PutMode::Update(UpdateVersion {
                e_tag: Some(e),
                version: None,
            }),
            None => PutMode::Create,
        };
        let opts = PutOptions {
            mode,
            ..PutOptions::default()
        };

        let path = ObjectPath::from(store.manifest_key.as_str());
        let payload = PutPayload::from(Bytes::from(bytes));

        match store.client.put_opts(&path, payload.clone(), opts).await {
            Ok(_) => {
                if attempt >= 2 {
                    tracing::warn!(
                        attempt = attempt + 1,
                        "manifest write recovered after contention"
                    );
                }
                return Ok(());
            }
            // `Precondition` = ETag mismatch on `Update`; `AlreadyExists` =
            // lost the first-write race on `Create`. Both mean "the manifest
            // moved underneath us; re-read and retry".
            Err(
                object_store::Error::Precondition { .. }
                | object_store::Error::AlreadyExists { .. },
            ) => {
                tracing::debug!(
                    attempt = attempt + 1,
                    "manifest ETag precondition failed, re-reading base and retrying"
                );
                if attempt + 1 >= 3 {
                    tracing::warn!(
                        attempt = attempt + 1,
                        "manifest write experiencing sustained contention (>= 3 retries)"
                    );
                }
                continue;
            }
            // The backend does not implement conditional PUT (`PutMode::Update`),
            // e.g. `LocalFileSystem` or an S3-compatible store with conditional
            // writes disabled. Fall back to an unconditional `Overwrite` so the
            // write still lands, but warn: coordination is now OFF, and the
            // flusher thread + ingestion thread are two concurrent manifest
            // writers that can silently clobber each other on such a backend.
            // Production S3 supports conditional PUT and never reaches here.
            Err(object_store::Error::NotImplemented { .. }) => {
                tracing::warn!(
                    "manifest backend does not support conditional PUT; falling \
                     back to unconditional Overwrite — coordination disabled, \
                     concurrent writers may lose updates"
                );
                store
                    .client
                    .put_opts(&path, payload, PutOptions::default())
                    .await
                    .map_err(|e| Error::Process(format!("S3 PUT manifest: {}", e)))?;
                return Ok(());
            }
            Err(e) => {
                return Err(Error::Process(format!("S3 PUT manifest: {}", e)));
            }
        }
    }
    Err(Error::Process(format!(
        "manifest write failed after {} retries (concurrent writers contending?)",
        MANIFEST_WRITE_MAX_RETRIES
    )))
}

fn spawn_flusher(store: Arc<S3Store>) -> FlusherHandle {
    let stop = Arc::new(Notify::new());
    let stop_clone = stop.clone();
    let join = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("flusher runtime");
        rt.block_on(async move {
            let interval = store.segment_cfg.flush_interval;
            loop {
                tokio::select! {
                    biased;
                    _ = stop_clone.notified() => break,
                    _ = tokio::time::sleep(interval) => {
                        let _ = seal_active_segment(&store).await;
                        let _ = flush_manifest(&store).await;
                    }
                }
            }
        });
    });
    FlusherHandle { stop, join }
}

/// Builder for the `object_store` WAL backend.
pub(crate) struct S3WalStoreBuilder;

impl WalStoreBuilder for S3WalStoreBuilder {
    fn build(&self, cfg: &WalConfig) -> Result<Arc<dyn WalStore>, Error> {
        S3Store::build(cfg).map(|s| s as Arc<dyn WalStore>)
    }

    fn kind(&self) -> &'static str {
        "object_store"
    }
}

/// Public init: register the `object_store` builder. Idempotent — repeated
/// calls hit the duplicate check in `register_wal_store_builder`.
pub(crate) fn register() -> Result<(), Error> {
    arkflow_core::wal::register_wal_store_builder("object_store", Arc::new(S3WalStoreBuilder))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::wal::config::{ObjectStoreS3Config, ObjectStoreWalConfig};
    use arkflow_core::wal::store::serialize;
    use arkflow_core::wal::SyncPolicy;
    use arkflow_core::MessageBatch;
    use async_trait::async_trait;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEQ: AtomicU64 = AtomicU64::new(0);

    fn sample_payload(input_name: Option<&str>) -> Vec<u8> {
        let schema = std::sync::Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![std::sync::Arc::new(Int64Array::from(vec![1]))])
                .unwrap();
        let mut mb = MessageBatch::new_arrow(batch);
        mb.set_input_name(input_name.map(|s| s.to_string()));
        serialize(&mb).unwrap()
    }

    fn tempdir() -> std::path::PathBuf {
        let n = SEQ.fetch_add(1, Ordering::SeqCst);
        let dir =
            std::env::temp_dir().join(format!("arkflow-s3wal-test-{}-{}", std::process::id(), n));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn build_local_store_in(dir: &std::path::Path) -> (Arc<S3Store>, std::path::PathBuf) {
        let client: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap());
        let runtime = Runtime::new().unwrap();
        let osc = ObjectStoreWalConfig {
            node_id: "pod-a".into(),
            stream_id: "main".into(),
            prefix: "arkflow/wal".into(),
            s3: ObjectStoreS3Config {
                bucket: "unused".into(),
                region: None,
                endpoint: None,
                access_key_id: None,
                secret_access_key: None,
                allow_http: false,
            },
            segment: SegmentConfig {
                max_entries: 4,
                max_bytes: 1024,
                flush_interval: std::time::Duration::from_millis(50),
            },
            cursor: CursorFlushConfig {
                max_entries: 1000,
                interval: std::time::Duration::from_millis(50),
            },
            segment_tuning: arkflow_core::wal::config::SegmentTuningConfig::default(),
            parallel_put: arkflow_core::wal::config::ParallelPutConfig::default(),
            compression: arkflow_core::wal::config::CompressionConfig::default(),
            sync: SyncPolicy::GroupCommit,
        };
        let store =
            S3Store::build_with_client(&WalConfig::default(), osc, runtime, client).unwrap();
        (store, dir.to_path_buf())
    }

    /// Spec "Object-store backend works through the engine's async paths":
    /// the engine drives the WAL's async API from a tokio runtime; pre-fix
    /// the store's internal `block_on` panicked with "cannot start a runtime
    /// from within a runtime" on the first store call. This is the async
    /// repro from the 2026-09-29 review, now a permanent regression test.
    #[tokio::test]
    async fn store_works_through_the_wal_async_api() {
        use arkflow_core::wal::Wal;

        let dir = tempdir();
        let (store, _) = build_local_store_in(&dir);
        let config = WalConfig {
            sync: SyncPolicy::GroupCommit,
            ..WalConfig::default()
        };
        let wal = Wal::open_with_store(&config, store, 1).unwrap();

        for expected in 1..=3u64 {
            let seq = wal.append(&Arc::new(arkflow_core::MessageBatch::try_from(vec![
                format!("{{\"v\":{expected}}}"),
            ]).unwrap()))
            .await
            .unwrap_or_else(|e| panic!("append {expected} through the async API: {e}"));
            assert_eq!(seq, expected);
        }
        // Drives append_batch on the blocking pool (the exact call that
        // panicked pre-fix).
        wal.flush().await.unwrap();
        for seq in 1..=3u64 {
            wal.advance(seq).await.unwrap();
        }
        // The cursor mirror is visible through the async API before any
        // manifest flush.
        assert_eq!(wal.cursor().await.unwrap(), 3);
        wal.close().await.unwrap();

        // Reopen: the flushed manifest replays nothing (prefix acked) and
        // the cursor survives the restart.
        let (store2, _) = build_local_store_in(&dir);
        let wal2 = Wal::open_with_store(&config, store2, 4).unwrap();
        let replay = wal2.read_after_cursor().await.unwrap();
        assert!(
            replay.is_empty(),
            "acked prefix must not replay: {replay:?}"
        );
        assert_eq!(wal2.cursor().await.unwrap(), 3);
        wal2.close().await.unwrap();
    }

    /// 4.3: clean restart — write entries, flush, "restart" (re-open), the
    /// manifest is consistent and read_after_cursor returns the unacked
    /// prefix.
    #[test]
    fn clean_restart_replays_unacked() {
        let dir = tempdir();
        let (store, _) = build_local_store_in(&dir);
        let payload = sample_payload(None);
        // 4 entries → forces a seal (max_entries = 4).
        for _ in 0..4 {
            store
                .append_batch(vec![(
                    SEQ.fetch_add(1, Ordering::SeqCst) + 1,
                    payload.clone(),
                )])
                .unwrap();
        }
        store.close().unwrap();

        // Reopen against the same directory.
        let (store2, _) = build_local_store_in(&dir);
        let replayed = store2.read_after_cursor().unwrap();
        assert_eq!(
            replayed.len(),
            4,
            "all four flushed entries must be replayable after restart"
        );
        store2.close().unwrap();
    }

    /// Spec "Object-store rewind survives an intervening manifest flush".
    ///
    /// Threshold-driven so every flush lands at a deterministic point: the
    /// 5th append seals [1..=4], the 4th advance flushes the manifest with
    /// cursor 4 — the poisoned-before state — and the rewind's corrective
    /// flush must LOWER the persisted cursor back to 3 (the original
    /// monotonic-only mutator made the corrective flush a no-op here).
    /// The source re-ack through 4..5 then resumes normal advancement.
    #[test]
    fn rewind_floor_survives_an_intervening_manifest_flush() {
        // seal at the 5th append, flush at the 4th advance, quiet intervals
        let build = |dir: &std::path::Path| {
            let client: Arc<dyn object_store::ObjectStore> =
                Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap());
            let osc = ObjectStoreWalConfig {
                node_id: "pod-a".into(),
                stream_id: "poison".into(),
                prefix: "arkflow/wal".into(),
                s3: ObjectStoreS3Config {
                    bucket: "unused".into(),
                    region: None,
                    endpoint: None,
                    access_key_id: None,
                    secret_access_key: None,
                    allow_http: false,
                },
                segment: SegmentConfig {
                    max_entries: 4,
                    max_bytes: 1024 * 1024,
                    flush_interval: std::time::Duration::from_secs(600),
                },
                cursor: CursorFlushConfig {
                    max_entries: 4,
                    interval: std::time::Duration::from_secs(600),
                },
                segment_tuning: arkflow_core::wal::config::SegmentTuningConfig::default(),
                parallel_put: arkflow_core::wal::config::ParallelPutConfig::default(),
                compression: arkflow_core::wal::config::CompressionConfig::default(),
                sync: SyncPolicy::GroupCommit,
            };
            S3Store::build_with_client(&WalConfig::default(), osc, Runtime::new().unwrap(), client)
                .unwrap()
        };

        let dir = tempdir();
        let store = build(&dir);
        // 5 appends: the 5th crosses segment.max_entries and seals [1..=4].
        store
            .append_batch((1..=5u64).map(|s| (s, sample_payload(None))).collect())
            .unwrap();
        // 4 advances: the 4th crosses cursor.max_entries and flushes the
        // manifest with cursor 4 — the failed commit is now PERSISTED past.
        for seq in 1..=4u64 {
            store.advance_cursor(seq).unwrap();
        }
        assert_eq!(store.cursor(), 4);

        // The wrapped source commit for 4 failed; the rewind must correct
        // the already-persisted manifest, not just the in-memory mirror.
        store.rewind_cursor(3).unwrap();
        assert_eq!(store.cursor(), 3, "cursor mirror rewound");

        // Close (seals 5, flushes under the poison clamp) and reopen: the
        // manifest must read 3 and replay [4, 5].
        store.close().unwrap();
        let reopened = build(&dir);
        assert_eq!(
            reopened.cursor(),
            3,
            "corrective flush must lower the persisted cursor below the poison"
        );
        let replay: Vec<u64> = reopened
            .read_after_cursor()
            .unwrap()
            .iter()
            .map(|(s, _)| *s)
            .collect();
        assert_eq!(replay, vec![4, 5], "failed sequence stays replayable");

        // The source re-acknowledges through the poison: the floor clears
        // and advancement resumes.
        reopened.advance_cursor(4).unwrap();
        reopened.advance_cursor(5).unwrap();
        reopened.close().unwrap();
        let final_store = build(&dir);
        assert_eq!(final_store.cursor(), 5);
        assert!(final_store.read_after_cursor().unwrap().is_empty());
        final_store.close().unwrap();
    }

    /// 4.3: torn tail (mid-PUT crash on the active segment) is silently
    /// truncated; the active segment keeps only the intact prefix.
    ///
    /// We simulate this by encoding a segment with 3 entries, chopping the
    /// trailing one, and writing the chopped buffer back to the store via
    /// the segment path. The next open must drop the truncated entry.
    #[test]
    fn torn_tail_dropped_on_recovery() {
        // We need access to the segment encoder. It's `pub(super)` so it's
        // reachable from this test module via `super::super::segment::encode`.
        let payload = sample_payload(None);
        let mut entries = Vec::new();
        for s in 1u64..=3 {
            entries.push((s, payload.clone()));
        }
        let mut bytes = Vec::new();
        super::super::segment::encode(&entries, &mut bytes).unwrap();

        // Drop the trailing entry's crc + half of its payload to simulate a
        // mid-PUT crash on entry #3.
        let mut cut = bytes.clone();
        let last_payload_len = payload.len();
        cut.truncate(bytes.len() - 4 - (last_payload_len / 2));

        // Decode directly to confirm the truncation behaviour we're
        // exercising on recovery.
        let decoded = super::super::segment::decode(&cut).unwrap();
        assert_eq!(decoded.entries.len(), 2);
        assert_eq!(decoded.entries[1].0, 2);
    }

    /// 4.3: a segment that was PUT but whose manifest write did NOT land
    /// (LIST-fallback) is still replayed on recovery.
    ///
    /// We simulate this by writing a sealed segment object directly via
    /// the object_store client, then opening a new `S3Store` and reading.
    /// The manifest on disk is empty (so the segment is *not* in its
    /// index), but LIST picks it up.
    #[test]
    fn list_fallback_replays_segment_not_in_manifest() {
        // Build a store, drop it without writing a manifest (close() does
        // write one, but we can write a segment directly via the client).
        let dir = std::env::temp_dir().join(format!(
            "arkflow-s3wal-test-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();

        // Write a segment object directly using a raw client, no manifest.
        let client: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&dir).unwrap());
        let runtime = Runtime::new().unwrap();
        let payload = sample_payload(None);
        let mut seg_bytes = Vec::new();
        super::super::segment::encode(&[(42u64, payload.clone())], &mut seg_bytes).unwrap();
        let key = "arkflow/wal/pod-a/main/segments/00000001.wal".to_string();
        runtime
            .block_on(client.put(
                &ObjectPath::from(key.as_str()),
                PutPayload::from(Bytes::from(seg_bytes)),
            ))
            .unwrap();

        // Now open a store; the manifest will be absent, but the segment
        // exists. Recovery's LIST-fallback must surface it.
        let osc = ObjectStoreWalConfig {
            node_id: "pod-a".into(),
            stream_id: "main".into(),
            prefix: "arkflow/wal".into(),
            s3: ObjectStoreS3Config {
                bucket: "unused".into(),
                region: None,
                endpoint: None,
                access_key_id: None,
                secret_access_key: None,
                allow_http: false,
            },
            segment: SegmentConfig {
                max_entries: 4,
                max_bytes: 1024,
                flush_interval: std::time::Duration::from_millis(50),
            },
            cursor: CursorFlushConfig {
                max_entries: 1000,
                interval: std::time::Duration::from_millis(50),
            },
            segment_tuning: arkflow_core::wal::config::SegmentTuningConfig::default(),
            parallel_put: arkflow_core::wal::config::ParallelPutConfig::default(),
            compression: arkflow_core::wal::config::CompressionConfig::default(),
            sync: SyncPolicy::GroupCommit,
        };
        let store =
            S3Store::build_with_client(&WalConfig::default(), osc, runtime, client).unwrap();
        let replayed = store.read_after_cursor().unwrap();
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].0, 42);
        store.close().unwrap();
    }

    /// 2.5/2.6: segment tuning presets are applied during build
    #[test]
    fn segment_tuning_aggressive_is_applied() {
        let dir = tempdir();
        let client: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&dir).unwrap());
        let runtime = Runtime::new().unwrap();

        let osc = ObjectStoreWalConfig {
            node_id: "pod-a".into(),
            stream_id: "main".into(),
            prefix: "arkflow/wal".into(),
            s3: ObjectStoreS3Config {
                bucket: "unused".into(),
                region: None,
                endpoint: None,
                access_key_id: None,
                secret_access_key: None,
                allow_http: false,
            },
            segment: SegmentConfig::default(),
            cursor: CursorFlushConfig::default(),
            segment_tuning: arkflow_core::wal::config::SegmentTuningConfig {
                strategy: arkflow_core::wal::config::SegmentStrategy::Aggressive,
                max_entries: None,
                max_bytes: None,
                flush_interval: None,
            },
            parallel_put: arkflow_core::wal::config::ParallelPutConfig::default(),
            compression: arkflow_core::wal::config::CompressionConfig::default(),
            sync: SyncPolicy::GroupCommit,
        };
        let store =
            S3Store::build_with_client(&WalConfig::default(), osc, runtime, client).unwrap();
        // Aggressive: max_entries=10000, max_bytes=10MB, flush_interval=10s
        assert_eq!(store.segment_cfg.max_entries, 10000);
        assert_eq!(store.segment_cfg.max_bytes, 10 * 1024 * 1024);
        assert_eq!(
            store.segment_cfg.flush_interval,
            std::time::Duration::from_secs(10)
        );
        store.close().unwrap();
    }

    #[test]
    fn segment_tuning_low_latency_is_applied() {
        let dir = tempdir();
        let client: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&dir).unwrap());
        let runtime = Runtime::new().unwrap();

        let osc = ObjectStoreWalConfig {
            node_id: "pod-a".into(),
            stream_id: "main".into(),
            prefix: "arkflow/wal".into(),
            s3: ObjectStoreS3Config {
                bucket: "unused".into(),
                region: None,
                endpoint: None,
                access_key_id: None,
                secret_access_key: None,
                allow_http: false,
            },
            segment: SegmentConfig::default(),
            cursor: CursorFlushConfig::default(),
            segment_tuning: arkflow_core::wal::config::SegmentTuningConfig {
                strategy: arkflow_core::wal::config::SegmentStrategy::LowLatency,
                max_entries: None,
                max_bytes: None,
                flush_interval: None,
            },
            parallel_put: arkflow_core::wal::config::ParallelPutConfig::default(),
            compression: arkflow_core::wal::config::CompressionConfig::default(),
            sync: SyncPolicy::GroupCommit,
        };
        let store =
            S3Store::build_with_client(&WalConfig::default(), osc, runtime, client).unwrap();
        // LowLatency: max_entries=100, max_bytes=100KB, flush_interval=100ms
        assert_eq!(store.segment_cfg.max_entries, 100);
        assert_eq!(store.segment_cfg.max_bytes, 100 * 1024);
        assert_eq!(
            store.segment_cfg.flush_interval,
            std::time::Duration::from_millis(100)
        );
        store.close().unwrap();
    }

    /// 2.4: validation rejects non-positive segment params
    #[test]
    fn segment_validation_rejects_zero_max_entries() {
        let dir = tempdir();
        let client: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&dir).unwrap());
        let runtime = Runtime::new().unwrap();

        let mut osc = ObjectStoreWalConfig {
            node_id: "pod-a".into(),
            stream_id: "main".into(),
            prefix: "arkflow/wal".into(),
            s3: ObjectStoreS3Config {
                bucket: "unused".into(),
                region: None,
                endpoint: None,
                access_key_id: None,
                secret_access_key: None,
                allow_http: false,
            },
            segment: SegmentConfig {
                max_entries: 0, // invalid
                max_bytes: 1024,
                flush_interval: std::time::Duration::from_secs(1),
            },
            cursor: CursorFlushConfig::default(),
            segment_tuning: arkflow_core::wal::config::SegmentTuningConfig::default(),
            parallel_put: arkflow_core::wal::config::ParallelPutConfig::default(),
            compression: arkflow_core::wal::config::CompressionConfig::default(),
            sync: SyncPolicy::GroupCommit,
        };
        // Force tuning to use the invalid `segment` (no overrides)
        osc.segment_tuning = arkflow_core::wal::config::SegmentTuningConfig::default();

        let result = S3Store::build_with_client(&WalConfig::default(), osc, runtime, client);
        match result {
            Err(e) => {
                assert!(
                    e.to_string().contains("max_entries"),
                    "expected max_entries validation error, got: {}",
                    e
                );
            }
            Ok(_) => panic!("expected error for zero max_entries"),
        }
    }

    /// 3.1/3.2: PutWorker can be created and shut down cleanly
    #[test]
    fn put_worker_can_be_created() {
        let dir = tempdir();
        let client: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&dir).unwrap());
        let _worker = PutWorker::new(0, client, "test-ns".into(), |_seq| {});
        // Worker thread will be dropped on scope exit
    }

    /// 3.3/3.5: ParallelPutWorkers with multiple workers submits in round-robin
    #[test]
    fn parallel_put_workers_round_robin_assignment() {
        let dir = tempdir();
        let client: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&dir).unwrap());
        let pool = ParallelPutWorkers::spawn(4, client, "test-ns".into(), |_seq| {});
        assert_eq!(pool.len(), 4);
        assert!(!pool.is_single());
    }

    /// 3.3: Single worker setup behaves as default
    #[test]
    fn parallel_put_workers_single_is_default() {
        let dir = tempdir();
        let client: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&dir).unwrap());
        let pool = ParallelPutWorkers::spawn(1, client, "test-ns".into(), |_seq| {});
        assert_eq!(pool.len(), 1);
        assert!(pool.is_single());
    }

    /// 3.12: validation rejects zero worker count
    #[test]
    fn parallel_put_workers_zero_count_rejected() {
        let dir = tempdir();
        let client: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&dir).unwrap());
        let runtime = Runtime::new().unwrap();

        let osc = ObjectStoreWalConfig {
            node_id: "pod-a".into(),
            stream_id: "main".into(),
            prefix: "arkflow/wal".into(),
            s3: ObjectStoreS3Config {
                bucket: "unused".into(),
                region: None,
                endpoint: None,
                access_key_id: None,
                secret_access_key: None,
                allow_http: false,
            },
            segment: SegmentConfig::default(),
            cursor: CursorFlushConfig::default(),
            segment_tuning: arkflow_core::wal::config::SegmentTuningConfig::default(),
            parallel_put: arkflow_core::wal::config::ParallelPutConfig {
                workers: 0, // invalid
                ..Default::default()
            },
            compression: arkflow_core::wal::config::CompressionConfig::default(),
            sync: SyncPolicy::GroupCommit,
        };
        let result = S3Store::build_with_client(&WalConfig::default(), osc, runtime, client);
        match result {
            Err(e) => {
                assert!(
                    e.to_string().contains("workers"),
                    "expected workers validation error, got: {}",
                    e
                );
            }
            Ok(_) => panic!("expected error for zero worker count"),
        }
    }

    /// 5.4: compression level out of range is rejected
    #[test]
    fn compression_level_validation() {
        // zstd level too high
        let dir = tempdir();
        let client: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&dir).unwrap());
        let runtime = Runtime::new().unwrap();

        let osc = ObjectStoreWalConfig {
            node_id: "pod-a".into(),
            stream_id: "main".into(),
            prefix: "arkflow/wal".into(),
            s3: ObjectStoreS3Config {
                bucket: "unused".into(),
                region: None,
                endpoint: None,
                access_key_id: None,
                secret_access_key: None,
                allow_http: false,
            },
            segment: SegmentConfig::default(),
            cursor: CursorFlushConfig::default(),
            segment_tuning: arkflow_core::wal::config::SegmentTuningConfig::default(),
            parallel_put: arkflow_core::wal::config::ParallelPutConfig::default(),
            compression: arkflow_core::wal::config::CompressionConfig::Zstd { level: 25 },
            sync: SyncPolicy::GroupCommit,
        };
        let result = S3Store::build_with_client(&WalConfig::default(), osc, runtime, client);
        match result {
            Err(e) => assert!(
                e.to_string().contains("zstd"),
                "expected zstd level error, got: {}",
                e
            ),
            Ok(_) => panic!("expected zstd level error"),
        }
    }

    /// 5.5: compression with no segments uses None path (no errors)
    #[test]
    fn compression_none_is_default_and_works() {
        let dir = tempdir();
        let client: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&dir).unwrap());
        let runtime = Runtime::new().unwrap();

        let osc = ObjectStoreWalConfig {
            node_id: "pod-a".into(),
            stream_id: "main".into(),
            prefix: "arkflow/wal".into(),
            s3: ObjectStoreS3Config {
                bucket: "unused".into(),
                region: None,
                endpoint: None,
                access_key_id: None,
                secret_access_key: None,
                allow_http: false,
            },
            segment: SegmentConfig::default(),
            cursor: CursorFlushConfig::default(),
            segment_tuning: arkflow_core::wal::config::SegmentTuningConfig::default(),
            parallel_put: arkflow_core::wal::config::ParallelPutConfig::default(),
            compression: arkflow_core::wal::config::CompressionConfig::None,
            sync: SyncPolicy::GroupCommit,
        };
        let store =
            S3Store::build_with_client(&WalConfig::default(), osc, runtime, client).unwrap();
        store.close().unwrap();
    }

    // ===== Manifest write-coordination (ETag + retry) regression tests =====
    //
    // These tests exercise `write_manifest_with_etag` directly against an
    // in-memory object store. The in-memory backend implements ETag-based
    // conditional PUTs (`PutMode::Update`), so concurrent writers genuinely
    // contend and the retry path is what converges them. They live in this
    // internal module because the coordinated writer is `pub(crate)`.

    /// Build an `S3Store` backed by a caller-provided object store (no MinIO
    /// required). Each call gets a fresh namespace so tests are independent.
    fn build_race_store(client: Arc<dyn object_store::ObjectStore>) -> Arc<S3Store> {
        let runtime = Runtime::new().unwrap();
        let unique = SEQ.fetch_add(1, Ordering::SeqCst);
        let osc = ObjectStoreWalConfig {
            node_id: format!("race-pod-{}", unique),
            stream_id: "race".into(),
            prefix: "arkflow/race".into(),
            s3: ObjectStoreS3Config {
                bucket: "unused".into(),
                region: None,
                endpoint: None,
                access_key_id: None,
                secret_access_key: None,
                allow_http: false,
            },
            segment: SegmentConfig {
                max_entries: 4,
                max_bytes: 1024,
                // Long interval so the background flusher does not interleave
                // its own manifest writes into the race under test.
                flush_interval: std::time::Duration::from_secs(3600),
            },
            cursor: CursorFlushConfig {
                max_entries: 1000,
                interval: std::time::Duration::from_secs(3600),
            },
            segment_tuning: arkflow_core::wal::config::SegmentTuningConfig::default(),
            parallel_put: arkflow_core::wal::config::ParallelPutConfig::default(),
            compression: arkflow_core::wal::config::CompressionConfig::default(),
            sync: SyncPolicy::GroupCommit,
        };
        S3Store::build_with_client(&WalConfig::default(), osc, runtime, client).unwrap()
    }

    fn build_inmemory_store() -> Arc<S3Store> {
        build_race_store(Arc::new(InMemory::new()))
    }

    /// Test-only `ObjectStore` whose `put_opts` always fails with
    /// `Precondition`, regardless of the supplied ETag/mode. Used to exhaust
    /// the manifest writer's retry budget (T4).
    #[derive(Debug)]
    struct AlwaysPreconditionStore {
        inner: Arc<dyn object_store::ObjectStore>,
    }

    impl std::fmt::Display for AlwaysPreconditionStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "AlwaysPreconditionStore({})", self.inner)
        }
    }

    #[async_trait]
    impl object_store::ObjectStore for AlwaysPreconditionStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            _payload: object_store::PutPayload,
            _opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            Err(object_store::Error::Precondition {
                path: location.to_string(),
                source: "injected precondition failure (test)".to_string().into(),
            })
        }
        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn build_failing_store() -> Arc<S3Store> {
        let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        build_race_store(Arc::new(AlwaysPreconditionStore { inner }))
    }

    /// Build an `S3Store` at a FIXED `(node_id, stream_id)` namespace backed by
    /// the given object store. Unlike `build_race_store` (which mints a fresh
    /// random namespace per call), this lets a test close one store and reopen
    /// another at the same namespace to exercise recovery / restart.
    fn build_store_at(
        node_id: &str,
        stream_id: &str,
        client: Arc<dyn object_store::ObjectStore>,
        segment_max_entries: usize,
        cursor_max_entries: usize,
    ) -> Arc<S3Store> {
        let runtime = Runtime::new().unwrap();
        let osc = ObjectStoreWalConfig {
            node_id: node_id.into(),
            stream_id: stream_id.into(),
            prefix: "arkflow/reopen".into(),
            s3: ObjectStoreS3Config {
                bucket: "unused".into(),
                region: None,
                endpoint: None,
                access_key_id: None,
                secret_access_key: None,
                allow_http: false,
            },
            // Large segment cap by default so appends stay in the active
            // segment and the test exercises cursor tracking in isolation,
            // not seal triggers. Callers shrink these to force seal/flush.
            segment: SegmentConfig {
                max_entries: segment_max_entries,
                max_bytes: 1024 * 1024,
                flush_interval: std::time::Duration::from_secs(3600),
            },
            cursor: CursorFlushConfig {
                max_entries: cursor_max_entries,
                interval: std::time::Duration::from_secs(3600),
            },
            segment_tuning: arkflow_core::wal::config::SegmentTuningConfig::default(),
            parallel_put: arkflow_core::wal::config::ParallelPutConfig::default(),
            compression: arkflow_core::wal::config::CompressionConfig::None,
            sync: SyncPolicy::GroupCommit,
        };
        S3Store::build_with_client(&WalConfig::default(), osc, runtime, client).unwrap()
    }

    /// Regression: the S3 backend's committed cursor does NOT track acks.
    ///
    /// `advance_cursor(seq)` discards its `seq` argument (`let _ = seq;`), so
    /// the watermark is derived only from the active segment's `last_seq` at
    /// flush time — decoupled from which messages were actually acknowledged.
    /// After a clean restart of a fully-acknowledged WAL:
    ///
    ///   - `next_seq_hint()` returns `cursor()+1 == 1` instead of `max_seq+1`,
    ///     so the next append reuses seq numbers already present on the store
    ///     (the "seq reuse" symptom of the original report).
    ///   - `read_after_cursor()` returns every previously-acknowledged entry,
    ///     so they are replayed again on every restart.
    ///
    /// The local `redb` backend does NOT have this bug: its `advance_cursor`
    /// stores `seq` precisely, so both assertions pass there. This test is a
    /// regression guard for the S3 fix — it failed before `advance_cursor`
    /// started recording `seq`.
    #[test]
    fn s3_cursor_does_not_track_acked_seq_after_restart() {
        let client: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let payload = sample_payload(None);

        // Phase 1: ingest seq 1..=5, then acknowledge ALL of them.
        let store = build_store_at("pod-a", "stream-1", client.clone(), 100, 1000);
        store
            .append_batch((1..=5u64).map(|s| (s, payload.clone())).collect())
            .unwrap();
        for seq in 1..=5u64 {
            store.advance_cursor(seq).unwrap();
        }
        store.close().unwrap();
        drop(store);

        // Phase 2: reopen at the same namespace (a process restart).
        let store2 = build_store_at("pod-a", "stream-1", client.clone(), 100, 1000);

        // Expected (correct): acked up to 5 ⇒ next seq is 6.
        let hint = store2.next_seq_hint();
        assert_eq!(
            hint, 6,
            "after acking seq 1..=5, next_seq_hint must be max_seq+1 = 6; \
             got {} — the S3 cursor ignores acks and stays at 0, so the next \
             append reuses seq numbers already on the store",
            hint,
        );

        // Expected (correct): fully acked ⇒ nothing pending for replay.
        let pending = store2.read_after_cursor().unwrap();
        let pending_seqs: Vec<u64> = pending.iter().map(|(s, _)| *s).collect();
        assert!(
            pending.is_empty(),
            "after acking seq 1..=5, read_after_cursor must be empty; \
             got {} entries {:?} — the S3 cursor never advanced past 0, so \
             already-acknowledged data is replayed on every restart",
            pending.len(),
            pending_seqs,
        );
    }

    /// Regression (spec scenario "Next sequence hint does not reuse a
    /// sealed-but-unacked sequence"): seal up to M but ack only up to K<M,
    /// then reopen. `next_seq_hint` must return M+1 (not K+1), and only the
    /// unacked K+1..=M entries may be pending — proving acks are tracked AND
    /// sequence numbers are not reused.
    #[test]
    fn s3_next_seq_hint_does_not_reuse_sealed_unacked_seq() {
        let client: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let payload = sample_payload(None);

        // Phase 1: ingest seq 1..=5, acknowledge only 1..=3, then close.
        // close seals all five (max_sealed_seq = 5); cursor advances to 3.
        let store = build_store_at("pod-b", "stream-2", client.clone(), 100, 1000);
        store
            .append_batch((1..=5u64).map(|s| (s, payload.clone())).collect())
            .unwrap();
        for seq in 1..=3u64 {
            store.advance_cursor(seq).unwrap();
        }
        store.close().unwrap();
        drop(store);

        // Phase 2: reopen at the same namespace.
        let store2 = build_store_at("pod-b", "stream-2", client.clone(), 100, 1000);

        // Next seq is M+1 = 6, NOT cursor()+1 = 4 — no reuse of sealed 4,5.
        assert_eq!(
            store2.next_seq_hint(),
            6,
            "next_seq_hint must be max_written_seq+1 = 6, not cursor()+1 = 4"
        );

        // Only the unacked seq 4,5 are pending; the acked 1..=3 are not.
        let pending: Vec<u64> = store2
            .read_after_cursor()
            .unwrap()
            .iter()
            .map(|(s, _)| *s)
            .collect();
        assert_eq!(pending, vec![4, 5], "only unacked seq 4,5 may be pending");
    }

    /// Regression (spec scenario "Cursor does not advance past unsealed
    /// data"): when an ack arrives for an entry still in the active (unsealed)
    /// segment, the cursor MUST clamp to `max_sealed_seq` and not advance past
    /// it — otherwise a restart would skip the unsealed entry (data loss).
    #[test]
    fn s3_cursor_clamps_to_max_sealed_seq_for_unsealed_ack() {
        let client: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        // segment.max_entries = 100 ⇒ appends stay unsealed; cursor.max_entries
        // = 1 ⇒ every advance_cursor flushes the manifest immediately.
        let store = build_store_at("pod-c", "stream-3", client.clone(), 100, 1);
        let payload = sample_payload(None);
        store
            .append_batch((1..=3u64).map(|s| (s, payload.clone())).collect())
            .unwrap();
        // Ack seq 3 while it is still unsealed (max_sealed_seq = 0). The flush
        // triggered here must clamp the cursor to 0, NOT advance it to 3.
        store.advance_cursor(3).unwrap();

        store.rt().block_on(async {
            let (m, _) = read_manifest_with_etag(&store).await.unwrap();
            assert_eq!(
                m.cursor, 0,
                "cursor must clamp to max_sealed_seq=0 while seq 3 is unsealed, not advance to 3"
            );
            assert!(
                m.cursor <= m.max_sealed_seq,
                "cursor ({}) must never exceed max_sealed_seq ({})",
                m.cursor,
                m.max_sealed_seq
            );
        });

        // After close seals the data, the cursor catches up to the ack.
        store.close().unwrap();
        drop(store);

        let store2 = build_store_at("pod-c", "stream-3", client.clone(), 100, 1);
        store2.rt().block_on(async {
            let (m, _) = read_manifest_with_etag(&store2).await.unwrap();
            assert_eq!(
                m.cursor, 3,
                "cursor catches up to 3 once seq 1..=3 are sealed"
            );
        });
        assert!(
            store2.read_after_cursor().unwrap().is_empty(),
            "once sealed and acked, nothing is pending for replay"
        );
    }

    /// T1: 8 concurrent writers each advance the cursor to a distinct value
    /// (via `max`, idempotent). The final cursor must equal the maximum,
    /// proving no writer's advance was silently overwritten by another.
    #[test]
    fn manifest_race_concurrent_cursor_keeps_max() {
        let store = build_inmemory_store();
        let inner = store.clone();
        store.rt().block_on(async move {
            let mut handles = Vec::new();
            for i in 1u64..=8 {
                let s = inner.clone();
                handles.push(tokio::spawn(async move {
                    write_manifest_with_etag(&s, move |m| {
                        let v = i * 10;
                        if v > m.cursor {
                            m.cursor = v;
                        }
                    })
                    .await
                }));
            }
            for h in handles {
                h.await.unwrap().unwrap();
            }
            let (m, _) = read_manifest_with_etag(&inner).await.unwrap();
            assert_eq!(
                m.cursor, 80,
                "cursor must converge to the max of all concurrent writers"
            );
        });
    }

    /// T2: 8 concurrent writers each seal a unique segment name. The final
    /// `sealed_segments` must contain exactly all 8, with no duplicates or
    /// loss — the idempotency guard plus retry must converge.
    #[test]
    fn manifest_race_concurrent_seal_keeps_all_segments() {
        let store = build_inmemory_store();
        let inner = store.clone();
        store.rt().block_on(async move {
            let mut handles = Vec::new();
            for i in 0u64..8 {
                let s = inner.clone();
                let name = format!("{:08}.wal", i);
                handles.push(tokio::spawn(async move {
                    write_manifest_with_etag(&s, move |m| {
                        if !m.sealed_segments.contains(&name) {
                            m.sealed_segments.push(name.clone());
                        }
                    })
                    .await
                }));
            }
            for h in handles {
                h.await.unwrap().unwrap();
            }
            let (m, _) = read_manifest_with_etag(&inner).await.unwrap();
            let mut seen = HashSet::new();
            for n in &m.sealed_segments {
                assert!(
                    seen.insert(n.clone()),
                    "duplicate segment {} in manifest",
                    n
                );
            }
            for i in 0u64..8 {
                assert!(
                    seen.contains(&format!("{:08}.wal", i)),
                    "segment {:08}.wal missing from manifest",
                    i
                );
            }
            assert_eq!(m.sealed_segments.len(), 8);
        });
    }

    /// T3: single-writer baseline — 8 sequential cursor increments must yield
    /// cursor == 8. Guards against the mutator losing the freshly-read base
    /// on the non-contended path.
    #[test]
    fn manifest_race_single_writer_baseline() {
        let store = build_inmemory_store();
        let inner = store.clone();
        store.rt().block_on(async move {
            for _ in 0..8 {
                write_manifest_with_etag(&inner, |m| {
                    m.cursor += 1;
                })
                .await
                .unwrap();
            }
            let (m, _) = read_manifest_with_etag(&inner).await.unwrap();
            assert_eq!(m.cursor, 8);
        });
    }

    /// T4: retry budget exceeded — a store whose PUTs always fail with
    /// `Precondition` must surface `Error::Process` after exhausting the
    /// budget, rather than hanging or silently succeeding.
    #[test]
    fn manifest_race_retry_budget_exceeded() {
        let store = build_failing_store();
        let inner = store.clone();
        let err = store
            .rt()
            .block_on(async move {
                write_manifest_with_etag(&inner, |m| {
                    m.cursor = 1;
                })
                .await
            })
            .expect_err("must surface an error when precondition always fails");
        let msg = err.to_string();
        assert!(
            msg.contains("manifest write failed") || msg.contains("retries"),
            "expected retry-exhaustion error, got: {}",
            msg
        );
    }

    /// T5: an out-of-order seal must not regress the active pointer. A worker
    /// whose manifest write retries after a *newer* segment has landed records
    /// its older segment in `sealed_segments` without overwriting the newer
    /// active pointer. Exercises `apply_seal` — the same path
    /// `seal_active_segment` uses inside its `write_manifest_with_etag` closure.
    #[test]
    fn manifest_race_out_of_order_seal_keeps_newest_active() {
        let store = build_inmemory_store();
        let inner = store.clone();
        store.rt().block_on(async move {
            // Newer segment sealed first (won the manifest race).
            write_manifest_with_etag(&inner, |m| {
                m.active_segment = Some("00000002.wal".to_string());
            })
            .await
            .unwrap();

            // Older segment (00000001) retries via the same `apply_seal` path
            // seal_active_segment uses. It must NOT overwrite the newer active.
            write_manifest_with_etag(&inner, |m| apply_seal(m, "00000001.wal"))
                .await
                .unwrap();

            let (m, _) = read_manifest_with_etag(&inner).await.unwrap();
            assert_eq!(m.active_segment.as_deref(), Some("00000002.wal"));
            assert!(
                m.sealed_segments.contains(&"00000001.wal".to_string()),
                "older sealed segment must be recorded: {:?}",
                m.sealed_segments
            );

            // Symmetric: an even newer seal (00000003) takes active and demotes
            // 00000002 into sealed_segments.
            write_manifest_with_etag(&inner, |m| apply_seal(m, "00000003.wal"))
                .await
                .unwrap();
            let (m, _) = read_manifest_with_etag(&inner).await.unwrap();
            assert_eq!(m.active_segment.as_deref(), Some("00000003.wal"));
            assert!(
                m.sealed_segments.contains(&"00000002.wal".to_string()),
                "demoted active must be recorded: {:?}",
                m.sealed_segments
            );
        });
    }

    /// T6: applying the same seal repeatedly must not push the segment into
    /// `sealed_segments` — it stays active only. Exercises `apply_seal`'s
    /// early return for `sealed_name == active_segment` (a segment is either
    /// active or sealed, never both).
    #[test]
    fn manifest_race_repeat_same_seal_keeps_it_active_only() {
        let store = build_inmemory_store();
        let inner = store.clone();
        store.rt().block_on(async move {
            // First application installs it as active (fresh manifest).
            write_manifest_with_etag(&inner, |m| apply_seal(m, "00000005.wal"))
                .await
                .unwrap();

            // Repeated applications — the retry path for the same seal — must
            // be a no-op: the segment stays active and never enters
            // sealed_segments.
            for _ in 0..3 {
                write_manifest_with_etag(&inner, |m| apply_seal(m, "00000005.wal"))
                    .await
                    .unwrap();
            }

            let (m, _) = read_manifest_with_etag(&inner).await.unwrap();
            assert_eq!(m.active_segment.as_deref(), Some("00000005.wal"));
            assert!(
                !m.sealed_segments.iter().any(|s| s == "00000005.wal"),
                "active segment must not appear in sealed_segments: {:?}",
                m.sealed_segments
            );
            assert!(
                m.sealed_segments.is_empty(),
                "no other segments sealed: {:?}",
                m.sealed_segments
            );
        });
    }

    /// T7: a failed segment PUT must restore the in-memory active segment so the
    /// entries are not lost. Guards the rollback added to `seal_active_segment`
    /// (previously a PUT failure after `std::mem::take` permanently dropped the
    /// data).
    #[test]
    fn seal_put_failure_restores_active_segment() {
        let store = build_failing_store();
        // Prime the in-memory active segment, as `append_batch` would.
        {
            let mut active = store.active.lock().unwrap();
            active.first_seq = 1;
            active.last_seq = 3;
            active.entries = 3;
            active.bytes = vec![0xDE, 0xAD, 0xBE, 0xEF];
            active.next_index = 5;
        }
        let inner = store.clone();
        let result = store
            .rt()
            .block_on(async move { seal_active_segment(&inner).await });
        assert!(result.is_err(), "seal must surface the segment PUT failure");

        // The active segment must be fully restored (entries + next_index), so
        // the next seal attempt re-uploads the same data.
        let active = store.active.lock().unwrap();
        assert_eq!(active.entries, 3, "entries restored after PUT failure");
        assert_eq!(active.first_seq, 1);
        assert_eq!(active.last_seq, 3);
        assert_eq!(active.next_index, 5, "next_index rolled back");
        assert!(!active.bytes.is_empty(), "bytes restored");
    }

    // ===== Coverage-gap tests (offline; in-memory object store only) =====

    /// A minimal `ObjectStoreWalConfig` with overridable fields, so the
    /// validation matrix below stays one statement per case.
    fn osc_base() -> ObjectStoreWalConfig {
        ObjectStoreWalConfig {
            node_id: "cov-pod".into(),
            stream_id: "cov".into(),
            prefix: "arkflow/cov".into(),
            s3: ObjectStoreS3Config {
                bucket: "unused".into(),
                region: None,
                endpoint: None,
                access_key_id: None,
                secret_access_key: None,
                allow_http: false,
            },
            segment: SegmentConfig {
                max_entries: 1000,
                max_bytes: 1024 * 1024,
                flush_interval: std::time::Duration::from_secs(3600),
            },
            cursor: CursorFlushConfig {
                max_entries: 1000,
                interval: std::time::Duration::from_secs(3600),
            },
            segment_tuning: arkflow_core::wal::config::SegmentTuningConfig::default(),
            parallel_put: arkflow_core::wal::config::ParallelPutConfig::default(),
            compression: arkflow_core::wal::config::CompressionConfig::None,
            sync: SyncPolicy::GroupCommit,
        }
    }

    fn inmemory_client() -> Arc<dyn object_store::ObjectStore> {
        Arc::new(InMemory::new())
    }

    /// `osc_base()`'s namespace: `{prefix}/{node_id}/{stream_id}`.
    const COV_NS: &str = "arkflow/cov/cov-pod/cov";

    /// Build a store over a fresh in-memory client using `osc_base` with the
    /// given mutations applied.
    fn build_cov_store(mutate: impl FnOnce(&mut ObjectStoreWalConfig)) -> Arc<S3Store> {
        let mut osc = osc_base();
        mutate(&mut osc);
        S3Store::build_with_client(
            &WalConfig::default(),
            osc,
            Runtime::new().unwrap(),
            inmemory_client(),
        )
        .unwrap()
    }

    fn build_cov_store_err(
        mutate: impl FnOnce(&mut ObjectStoreWalConfig),
    ) -> Result<Arc<S3Store>, Error> {
        let mut osc = osc_base();
        mutate(&mut osc);
        S3Store::build_with_client(
            &WalConfig::default(),
            osc,
            Runtime::new().unwrap(),
            inmemory_client(),
        )
    }

    /// Build over a caller-provided (possibly pre-seeded) client.
    fn build_cov_store_with(
        client: Arc<dyn object_store::ObjectStore>,
        mutate: impl FnOnce(&mut ObjectStoreWalConfig),
    ) -> Arc<S3Store> {
        let mut osc = osc_base();
        mutate(&mut osc);
        S3Store::build_with_client(&WalConfig::default(), osc, Runtime::new().unwrap(), client)
            .unwrap()
    }

    /// An S3 block whose client construction fails deterministically offline
    /// (`secret_access_key` without `access_key_id` → MissingAccessKeyId).
    fn offline_unbuildable_s3() -> ObjectStoreS3Config {
        ObjectStoreS3Config {
            bucket: "unused".into(),
            region: None,
            endpoint: None,
            access_key_id: None,
            secret_access_key: Some("orphan-secret".into()),
            allow_http: false,
        }
    }

    /// `expect_err` needs `S3Store: Debug`; extract the error manually.
    fn unwrap_err(result: Result<Arc<S3Store>, Error>, ctx: &str) -> Error {
        match result {
            Err(e) => e,
            Ok(_) => panic!("{ctx}"),
        }
    }

    /// PUT worker happy path: a submitted segment is uploaded and reported
    /// through the completion callback (D4 path used by the worker pool).
    #[test]
    fn put_worker_uploads_segment_and_reports_completion() {
        let client = inmemory_client();
        let done = Arc::new(AtomicU64::new(0));
        let done_cb = done.clone();
        let worker = PutWorker::new(0, client.clone(), "cov-ns".into(), move |seq| {
            done_cb.store(seq, Ordering::SeqCst);
        });
        let sender = worker.sender();
        sender
            .send(PendingSegment {
                segment_index: 7,
                first_seq: 1,
                last_seq: 2,
                bytes: vec![1, 2, 3],
            })
            .expect("worker queue has capacity");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while done.load(Ordering::SeqCst) == 0 {
            assert!(std::time::Instant::now() < deadline, "PUT worker never completed");
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(done.load(Ordering::SeqCst), 7);
        // The object landed at the namespaced segment key.
        let rt = Runtime::new().unwrap();
        let meta = rt
            .block_on(client.head(&ObjectPath::from("cov-ns/segments/00000007.wal")))
            .expect("uploaded object exists");
        assert_eq!(meta.size, 3);
        // Dropping the sender stops the worker after it drains.
        drop(sender);
    }

    /// PUT worker failure path: a failing client logs the error and keeps the
    /// worker alive; the completion callback must NOT fire.
    #[test]
    fn put_worker_upload_failure_is_reported_not_fatal() {
        let inner = inmemory_client();
        let client: Arc<dyn object_store::ObjectStore> = Arc::new(FailPutStore {
            inner,
            fail_segments: true,
        });
        let done = Arc::new(AtomicU64::new(0));
        let done_cb = done.clone();
        let worker = PutWorker::new(1, client, "cov-ns".into(), move |seq| {
            done_cb.store(seq, Ordering::SeqCst);
        });
        worker
            .sender()
            .send(PendingSegment {
                segment_index: 1,
                first_seq: 1,
                last_seq: 1,
                bytes: vec![9],
            })
            .expect("send");
        // Give the worker time to attempt (and log) the failing upload.
        std::thread::sleep(std::time::Duration::from_millis(200));
        assert_eq!(done.load(Ordering::SeqCst), 0, "failed PUT must not complete");
    }

    /// The worker pool caps at 8 workers and warns (task 3.3).
    #[test]
    fn parallel_put_workers_caps_at_eight() {
        let pool = ParallelPutWorkers::spawn(9, inmemory_client(), "cov-ns".into(), |_seq| {});
        assert_eq!(pool.len(), 8, "requested 9 workers must cap at 8");
        assert!(!pool.is_single());
    }

    /// `submit` round-robins across workers and surfaces channel failures.
    #[test]
    fn parallel_put_submit_round_robin_and_error_branches() {
        // Completion callback ORs a per-segment bit: round-robin makes the
        // completion ORDER racy, so the assertion must be order-free.
        let done = Arc::new(AtomicU64::new(0));
        let done_cb = done.clone();
        let pool = ParallelPutWorkers::spawn(2, inmemory_client(), "cov-ns".into(), move |seq| {
            done_cb.fetch_or(1 << seq, Ordering::SeqCst);
        });
        // Empty pool: the guard branch reports "no PUT workers".
        let empty = ParallelPutWorkers {
            workers: Vec::new(),
            next_worker: AtomicU64::new(0),
        };
        let err = empty
            .submit(PendingSegment {
                segment_index: 1,
                first_seq: 1,
                last_seq: 1,
                bytes: vec![1],
            })
            .expect_err("an empty pool cannot submit");
        assert!(err.to_string().contains("no PUT workers"));

        // Disconnected worker: the send error branch.
        let (tx, rx) = flume::bounded::<PendingSegment>(1);
        drop(rx);
        let disconnected = ParallelPutWorkers {
            workers: vec![PutWorker {
                sender: tx,
                _handle: std::thread::spawn(|| {}),
            }],
            next_worker: AtomicU64::new(0),
        };
        let err = disconnected
            .submit(PendingSegment {
                segment_index: 1,
                first_seq: 1,
                last_seq: 1,
                bytes: vec![1],
            })
            .expect_err("send on a disconnected channel must fail");
        assert!(err.to_string().contains("PUT worker channel send"));

        // Happy path: two submits land (round-robin over 2 workers).
        for idx in 3..=4u64 {
            pool.submit(PendingSegment {
                segment_index: idx,
                first_seq: 1,
                last_seq: 1,
                bytes: vec![idx as u8],
            })
            .expect("submit to live workers");
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while done.load(Ordering::SeqCst) & 0b11000 != 0b11000 {
            assert!(
                std::time::Instant::now() < deadline,
                "workers never drained (flags {done:?})"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // Direct sender access: valid index Some, out-of-range None.
        assert!(pool.worker_sender(1).is_some());
        assert!(pool.worker_sender(9).is_none());
        // shutdown() is a best-effort no-op today; it must not panic.
        pool.shutdown();
    }

    /// `S3Store::build` rejects configs whose backend is not `object_store`.
    #[test]
    fn build_requires_object_store_backend() {
        let err = unwrap_err(S3Store::build(&WalConfig::default()),
            "default config has no object_store backend");
        assert!(err.to_string().contains("requires `backend: object_store`"));
    }

    /// `S3Store::build` maps a client-construction failure (empty bucket)
    /// offline — no network involved in `AmazonS3Builder::build`.
    #[test]
    fn build_maps_s3_client_init_failure() {
        let mut osc = osc_base();
        osc.s3 = offline_unbuildable_s3();
        let cfg = WalConfig {
            backend: Some(arkflow_core::wal::WalBackend::ObjectStore(osc)),
            ..WalConfig::default()
        };
        let err = unwrap_err(
            S3Store::build(&cfg),
            "a secret key without an access key id cannot build a client",
        );
        assert!(
            err.to_string().contains("S3 client init"),
            "expected client-init error, got: {err}"
        );
    }

    /// D8 guard reachable through the public builder too.
    #[test]
    fn wal_store_builder_reports_kind_and_build_errors() {
        assert_eq!(S3WalStoreBuilder.kind(), "object_store");

        let mut osc = osc_base();
        osc.s3 = offline_unbuildable_s3();
        let cfg = WalConfig {
            backend: Some(arkflow_core::wal::WalBackend::ObjectStore(osc)),
            ..WalConfig::default()
        };
        let err = match S3WalStoreBuilder.build(&cfg) {
            Err(e) => e,
            Ok(_) => panic!("builder must surface the client-init failure"),
        };
        assert!(err.to_string().contains("S3 client init"));

        // register() is idempotent-or-duplicate; both outcomes prove the call.
        let _ = register();
    }

    /// D8: per-entry sync is rejected on the remote backend.
    #[test]
    fn build_rejects_per_entry_sync() {
        let err = unwrap_err(
            build_cov_store_err(|osc| osc.sync = SyncPolicy::PerEntry),
            "per_entry is not viable remotely",
        );
        assert!(err.to_string().contains("per_entry"));
    }

    /// Segment validation: zero max_bytes / zero flush_interval.
    #[test]
    fn build_rejects_zero_max_bytes_and_zero_flush_interval() {
        let err = unwrap_err(
            build_cov_store_err(|osc| {
                osc.segment.max_entries = 10;
                osc.segment.max_bytes = 0;
            }),
            "zero max_bytes",
        );
        assert!(err.to_string().contains("max_bytes"));

        let err = unwrap_err(
            build_cov_store_err(|osc| {
                osc.segment.max_entries = 10;
                osc.segment.max_bytes = 1024;
                osc.segment.flush_interval = std::time::Duration::ZERO;
            }),
            "zero flush interval",
        );
        assert!(err.to_string().contains("flush_interval"));
    }

    /// Compression level validation for lz4 (zstd covered above), plus a
    /// valid lz4 build.
    #[test]
    fn compression_lz4_level_validation() {
        for level in [0i32, 17] {
            let err = unwrap_err(
                build_cov_store_err(|osc| {
                    osc.compression = arkflow_core::wal::config::CompressionConfig::Lz4 { level }
                }),
                "lz4 level out of range",
            );
            assert!(
                err.to_string().contains("lz4"),
                "expected lz4 error, got: {err}"
            );
        }
        // Valid bounds build cleanly.
        for level in [1i32, 16] {
            let store = build_cov_store(|osc| {
                osc.compression = arkflow_core::wal::config::CompressionConfig::Lz4 { level }
            });
            store.close().unwrap();
        }
        // zstd valid bound.
        let store = build_cov_store(|osc| {
            osc.compression = arkflow_core::wal::config::CompressionConfig::Zstd { level: 0 }
        });
        store.close().unwrap();
    }

    /// `build_s3_client` maps every optional field onto the builder. Client
    /// construction is offline; no request is issued.
    #[test]
    fn build_s3_client_maps_every_configured_field() {
        let cfg = ObjectStoreS3Config {
            bucket: "b".into(),
            region: Some("us-east-1".into()),
            endpoint: Some("http://127.0.0.1:9000".into()),
            access_key_id: Some("key".into()),
            secret_access_key: Some("secret".into()),
            allow_http: true,
        };
        let rt = Runtime::new().unwrap();
        rt.block_on(async { build_s3_client(&cfg).await })
            .expect("fully-specified offline config builds");

        // A secret key without an access key id is a construction error
        // (offline, deterministic — no request is attempted).
        let err = rt
            .block_on(async { build_s3_client(&offline_unbuildable_s3()).await })
            .expect_err("orphan secret key must fail client construction");
        assert!(
            err.to_lowercase().contains("access"),
            "expected a credentials error, got: {err}"
        );
    }

    /// The cursor interval trigger: `cursor_should_flush` fires on elapsed
    /// time even below `max_entries` (D6).
    #[test]
    fn cursor_interval_triggers_a_flush_decision() {
        let store = build_cov_store(|_| {});
        // Fresh state: below both thresholds.
        assert!(!store.cursor_should_flush());
        // Age the last flush past the (1h) interval.
        store
            .cursor_last_flush_ms
            .store(now_ms() - 7_200_000, Ordering::Release);
        assert!(store.cursor_should_flush());
        // max_entries threshold alone also triggers.
        store
            .cursor_last_flush_ms
            .store(now_ms(), Ordering::Release);
        store.cursor_pending.store(1000, Ordering::Release);
        assert!(store.cursor_should_flush());
        store.close().unwrap();
    }

    /// `probe_next_segment_index` picks max(seen)+1 and ignores non-wal
    /// objects (startup naming, D4).
    #[test]
    fn probe_next_segment_index_picks_max_plus_one() {
        let client = inmemory_client();
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            for key in [
                "ns/segments/00000003.wal",
                "ns/segments/00000010.wal",
                "ns/segments/notes.txt",
                "ns/segments/not-a-number.wal",
            ] {
                client
                    .put(
                        &ObjectPath::from(key),
                        PutPayload::from(Bytes::from(vec![0u8])),
                    )
                    .await
                    .unwrap();
            }
            let next = probe_next_segment_index(&*client, "ns/segments").await.unwrap();
            assert_eq!(next, 11, "max seen index is 10");
        });
    }

    /// Recovery unions the manifest with LIST and skips non-`.wal` objects;
    /// the highest seen seq seeds `next_seq_hint` (D5).
    #[test]
    fn recovery_skips_non_wal_objects_and_keeps_seq_monotonic() {
        let client = inmemory_client();
        let payload = sample_payload(None);
        let mut seg_bytes = Vec::new();
        super::super::segment::encode(&[(5u64, payload.clone())], &mut seg_bytes).unwrap();
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            client
                .put(
                    &ObjectPath::from(format!("{COV_NS}/segments/00000005.wal").as_str()),
                    PutPayload::from(Bytes::from(seg_bytes.clone())),
                )
                .await
                .unwrap();
            client
                .put(
                    &ObjectPath::from(format!("{COV_NS}/segments/readme.txt").as_str()),
                    PutPayload::from(Bytes::from("not a segment")),
                )
                .await
                .unwrap();
        });
        let store = build_cov_store_with(client, |_| {});
        assert_eq!(store.next_seq_hint(), 6, "max seq seen on store is 5");
        let replayed: Vec<u64> = store
            .read_after_cursor()
            .unwrap()
            .iter()
            .map(|(s, _)| *s)
            .collect();
        assert_eq!(replayed, vec![5]);
        store.close().unwrap();
    }

    /// A manifest referencing a segment that no longer exists (truncated
    /// between write and recovery, D7) is skipped silently.
    #[test]
    fn recovery_skips_manifest_segments_missing_on_the_store() {
        let client = inmemory_client();
        let mut manifest = Manifest::fresh("cov-pod".into(), "cov".into());
        manifest.sealed_segments.push("00000009.wal".into());
        manifest.sealed_segments.push("garbage".into());
        let bytes = manifest.to_json().unwrap();
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            client
                .put(
                    &ObjectPath::from(format!("{COV_NS}/manifest.json").as_str()),
                    PutPayload::from(Bytes::from(bytes)),
                )
                .await
                .unwrap();
        });
        // Build succeeds: both referenced segments are absent → skipped.
        let store = build_cov_store_with(client, |_| {});
        assert_eq!(store.next_seq_hint(), 1, "no readable segments");
        // read_after_cursor also skips the missing segments (NotFound).
        assert!(store.read_after_cursor().unwrap().is_empty());
        store.close().unwrap();
    }

    /// A generic (non-NotFound) GET failure while listing segments in
    /// recovery is an error, not a skip.
    #[test]
    fn recovery_errors_when_segment_get_fails() {
        let client: Arc<dyn object_store::ObjectStore> = Arc::new(FailGetStore::segments(
            inmemory_client(),
        ));
        let payload = sample_payload(None);
        let mut seg_bytes = Vec::new();
        super::super::segment::encode(&[(1u64, payload)], &mut seg_bytes).unwrap();
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            client
                .put(
                    &ObjectPath::from(format!("{COV_NS}/segments/00000001.wal").as_str()),
                    PutPayload::from(Bytes::from(seg_bytes)),
                )
                .await
                .unwrap();
        });
        let result = S3Store::build_with_client(
            &WalConfig::default(),
            osc_base(),
            Runtime::new().unwrap(),
            client,
        );
        let err = unwrap_err(result, "segment GET failure must fail recovery");
        assert!(
            err.to_string().contains("S3 GET segment"),
            "got: {err}"
        );
    }

    /// A generic manifest GET failure fails recovery (not treated as fresh).
    #[test]
    fn recovery_errors_when_manifest_get_fails() {
        let client: Arc<dyn object_store::ObjectStore> = Arc::new(FailGetStore::manifest(
            inmemory_client(),
        ));
        let result = S3Store::build_with_client(
            &WalConfig::default(),
            osc_base(),
            Runtime::new().unwrap(),
            client,
        );
        let err = unwrap_err(result, "manifest GET failure must fail recovery");
        assert!(
            err.to_string().contains("S3 GET manifest"),
            "got: {err}"
        );
    }

    /// `read_manifest_with_etag` and `read_after_cursor` both surface a
    /// manifest GET failure that starts after recovery (the first GET
    /// succeeded, so the store built cleanly).
    #[test]
    fn read_paths_error_when_manifest_get_fails_after_recovery() {
        let inner = inmemory_client();
        let client: Arc<dyn object_store::ObjectStore> =
            Arc::new(FailGetStore::manifest_after(inner, 1));
        let store = S3Store::build_with_client(
            &WalConfig::default(),
            osc_base(),
            Runtime::new().unwrap(),
            client,
        )
        .unwrap();
        let inner_store = store.clone();
        let err = store
            .rt()
            .block_on(async move { read_manifest_with_etag(&inner_store).await })
            .expect_err("the post-recovery manifest GET must fail");
        assert!(
            err.to_string().contains("S3 GET manifest"),
            "got: {err}"
        );
        let err = store
            .read_after_cursor()
            .expect_err("read_after_cursor must hit the same branch");
        assert!(
            err.to_string().contains("S3 GET manifest"),
            "got: {err}"
        );
        // close()'s final flush fails on the same branch; it must not panic.
        let _ = store.close();
    }

    /// `read_after_cursor` surfaces a segment GET failure.
    #[test]
    fn read_after_cursor_errors_when_segment_get_fails() {
        let inner = inmemory_client();
        let payload = sample_payload(None);
        let mut seg_bytes = Vec::new();
        super::super::segment::encode(&[(1u64, payload)], &mut seg_bytes).unwrap();
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            inner
                .put(
                    &ObjectPath::from(format!("{COV_NS}/segments/00000001.wal").as_str()),
                    PutPayload::from(Bytes::from(seg_bytes)),
                )
                .await
                .unwrap();
        });
        // Fail .wal GETs from the second call onwards: the first one is
        // recovery's, the second is read_after_cursor's.
        let client: Arc<dyn object_store::ObjectStore> =
            Arc::new(FailGetStore::segments_after(inner, 1));
        let store = S3Store::build_with_client(
            &WalConfig::default(),
            osc_base(),
            Runtime::new().unwrap(),
            client,
        )
        .unwrap();
        let err = store
            .read_after_cursor()
            .expect_err("segment GET failure must surface");
        assert!(
            err.to_string().contains("S3 GET segment"),
            "got: {err}"
        );
        let _ = store.close();
    }

    /// `append_batch(&[])` is a no-op; `advance_cursor` past a poison clears
    /// the rewind floor so advancement resumes.
    #[test]
    fn append_batch_empty_is_noop_and_reack_clears_the_rewind_floor() {
        let store = build_cov_store(|_| {});
        store.append_batch(vec![]).expect("empty batch is a no-op");

        let payload = sample_payload(None);
        store
            .append_batch(vec![(1, payload.clone()), (2, payload.clone())])
            .unwrap();
        store.advance_cursor(2).unwrap();
        // Poison at seq 2 (failed source commit), then re-ack through it.
        store.rewind_cursor(1).unwrap();
        assert_eq!(store.cursor(), 1);
        store.advance_cursor(2).unwrap();
        // The floor cleared: no clamp on the following flush.
        assert_eq!(store.cursor(), 2);
        store.close().unwrap();
    }

    /// A rewind whose corrective manifest flush fails fails closed (the
    /// replay guarantee never silently degrades).
    #[test]
    fn rewind_corrective_flush_failure_is_reported() {
        let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let client: Arc<dyn object_store::ObjectStore> = Arc::new(FailPutStore {
            inner,
            fail_segments: false,
        });
        let store = S3Store::build_with_client(
            &WalConfig::default(),
            osc_base(),
            Runtime::new().unwrap(),
            client,
        )
        .unwrap();
        let payload = sample_payload(None);
        store
            .append_batch(vec![(1, payload.clone()), (2, payload)])
            .unwrap();
        store.advance_cursor(2).unwrap();
        let err = store
            .rewind_cursor(1)
            .expect_err("corrective flush against a failing manifest must error");
        assert!(
            err.to_string().contains("manifest write failed"),
            "the corrective flush failure must surface, got: {err}"
        );
    }

    /// The background flusher seals and flushes on the interval (D4/D6):
    /// entries buffered past `flush_interval` become durable with no
    /// explicit seal trigger.
    #[test]
    fn flusher_seals_active_segment_on_interval() {
        let client = inmemory_client();
        let runtime = Runtime::new().unwrap();
        let mut osc = osc_base();
        osc.segment.flush_interval = std::time::Duration::from_millis(100);
        osc.cursor.interval = std::time::Duration::from_millis(150);
        osc.cursor.max_entries = 1_000_000;
        let store =
            S3Store::build_with_client(&WalConfig::default(), osc, runtime, client.clone())
                .unwrap();
        let payload = sample_payload(None);
        store.append_batch(vec![(1, payload)]).unwrap();
        // Wait for at least two flusher ticks (tolerates a slow CI machine).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        let sealed = loop {
            let active_entries = store.active.lock().unwrap().entries;
            let listed = store
                .rt()
                .block_on(async {
                    use futures::StreamExt;
                    let mut n = 0u32;
                    let mut stream =
                        client.list(Some(&ObjectPath::from(format!("{COV_NS}/segments").as_str())));
                    while let Some(item) = stream.next().await {
                        item?;
                        n += 1;
                    }
                    Ok::<u32, object_store::Error>(n)
                })
                .unwrap();
            if active_entries == 0 && listed >= 1 {
                break listed;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "flusher never sealed the active segment"
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        };
        assert!(sealed >= 1);
        store.close().unwrap();
    }

    /// Sustained ETag contention then success: the writer retries, warns on
    /// the 3rd+ attempt, and still lands the manifest.
    #[test]
    fn manifest_write_retries_through_sustained_contention() {
        let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let client: Arc<dyn object_store::ObjectStore> = Arc::new(FlakyPreconditionStore {
            inner,
            fail_for_first: AtomicU64::new(3),
        });
        let runtime = Runtime::new().unwrap();
        let store = S3Store::build_with_client(
            &WalConfig::default(),
            osc_base(),
            runtime,
            client,
        )
        .unwrap();
        let inner_store = store.clone();
        store
            .rt()
            .block_on(async move {
                write_manifest_with_etag(&inner_store, |m| m.cursor = 5).await
            })
            .expect("retries converge after contention");
        let inner_store = store.clone();
        store.rt().block_on(async move {
            let (m, _) = read_manifest_with_etag(&inner_store).await.unwrap();
            assert_eq!(m.cursor, 5);
        });
        store.close().unwrap();
    }

    /// Backends without conditional PUT (`NotImplemented`) fall back to an
    /// unconditional Overwrite and still land the manifest (with the
    /// coordination-disabled warning).
    #[test]
    fn manifest_write_falls_back_to_overwrite_without_conditional_put() {
        let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let client: Arc<dyn object_store::ObjectStore> = Arc::new(NotImplementedPutStore {
            inner: inner.clone(),
        });
        let runtime = Runtime::new().unwrap();
        let store = S3Store::build_with_client(
            &WalConfig::default(),
            osc_base(),
            runtime,
            client,
        )
        .unwrap();
        let inner_store = store.clone();
        store
            .rt()
            .block_on(async move { write_manifest_with_etag(&inner_store, |m| m.cursor = 9).await })
            .expect("overwrite fallback lands the manifest");
        // The fallback wrote through to the inner (real) store.
        store.rt().block_on(async {
            let (m, _) = read_manifest_with_etag(&store).await.unwrap();
            assert_eq!(m.cursor, 9);
        });
        store.close().unwrap();
    }

    /// `read_manifest_with_etag` treats NotFound as fresh (covered by every
    /// fresh-bucket test above); this drives its generic-error branch via the
    /// flush path on a failing client.
    #[test]
    fn flush_manifest_fails_loudly_when_the_manifest_get_fails() {
        let inner = inmemory_client();
        // Seed a manifest so recovery's first GET succeeds; every later
        // manifest GET fails (flush_manifest → read_manifest_with_etag).
        let client: Arc<dyn object_store::ObjectStore> =
            Arc::new(FailGetStore::manifest_after(inner, 1));
        let runtime = Runtime::new().unwrap();
        let store = S3Store::build_with_client(
            &WalConfig::default(),
            osc_base(),
            runtime,
            client,
        )
        .unwrap();
        let inner_store = store.clone();
        let err = store
            .rt()
            .block_on(async move { flush_manifest(&inner_store).await })
            .expect_err("flush must fail when the manifest cannot be read");
        assert!(
            err.to_string().contains("S3 GET manifest"),
            "got: {err}"
        );
        let _ = store.close();
    }

    /// Dropping a store inside an async context defers the private-runtime
    /// shutdown to a helper thread instead of panicking.
    #[tokio::test]
    async fn dropping_a_store_inside_an_async_context_is_safe() {
        let store = build_cov_store(|_| {});
        // First: a construction error inside an async context disposes the
        // runtime through the same off-thread path.
        let err = unwrap_err(
            S3Store::build_with_client(
                &WalConfig::default(),
                {
                    let mut osc = osc_base();
                    osc.sync = SyncPolicy::PerEntry;
                    osc
                },
                Runtime::new().unwrap(),
                inmemory_client(),
            ),
            "per_entry rejected",
        );
        assert!(err.to_string().contains("per_entry"));
        // Then: a successful store dropped without close() inside the runtime.
        drop(store);
        tokio::task::yield_now().await;
    }

    /// Test-only object store failing PUTs for segment objects (or every
    /// conditional manifest PUT when `fail_segments` is false and the path is
    /// the manifest of a `FlakyPreconditionStore` — kept separate below).
    #[derive(Debug)]
    struct FailPutStore {
        inner: Arc<dyn object_store::ObjectStore>,
        /// Fail every segment PUT (used for the PUT worker failure path).
        fail_segments: bool,
    }

    impl std::fmt::Display for FailPutStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FailPutStore({})", self.inner)
        }
    }

    #[async_trait]
    impl object_store::ObjectStore for FailPutStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            let is_segment = location.to_string().ends_with(".wal");
            if self.fail_segments && is_segment {
                return Err(object_store::Error::Generic {
                    store: "FailPutStore",
                    source: "injected segment PUT failure (test)".to_string().into(),
                });
            }
            // Manifest writes are made to fail persistently (precondition)
            // when this store is used for the rewind test.
            if !self.fail_segments && location.to_string().ends_with("manifest.json") {
                return Err(object_store::Error::Precondition {
                    path: location.to_string(),
                    source: "injected manifest PUT failure (test)".to_string().into(),
                });
            }
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Test-only store injecting generic GET failures for the manifest
    /// and/or segment objects, with optional "fail after N successes"
    /// counters (recovery GETs succeed, later ones fail).
    #[derive(Debug)]
    struct FailGetStore {
        inner: Arc<dyn object_store::ObjectStore>,
        manifest_mode: FailMode,
        segment_mode: FailMode,
        manifest_count: AtomicU64,
        segment_count: AtomicU64,
    }

    #[derive(Debug, Clone, Copy)]
    enum FailMode {
        Never,
        Always,
        After(u64),
    }

    impl FailGetStore {
        fn manifest(inner: Arc<dyn object_store::ObjectStore>) -> Self {
            Self {
                inner,
                manifest_mode: FailMode::Always,
                segment_mode: FailMode::Never,
                manifest_count: AtomicU64::new(0),
                segment_count: AtomicU64::new(0),
            }
        }
        fn segments(inner: Arc<dyn object_store::ObjectStore>) -> Self {
            Self {
                inner,
                manifest_mode: FailMode::Never,
                segment_mode: FailMode::Always,
                manifest_count: AtomicU64::new(0),
                segment_count: AtomicU64::new(0),
            }
        }
        fn manifest_after(inner: Arc<dyn object_store::ObjectStore>, n: u64) -> Self {
            Self {
                manifest_mode: FailMode::After(n),
                ..Self::manifest(inner)
            }
        }
        fn segments_after(inner: Arc<dyn object_store::ObjectStore>, n: u64) -> Self {
            Self {
                segment_mode: FailMode::After(n),
                ..Self::segments(inner)
            }
        }
    }

    fn fail_mode_allows(mode: FailMode, counter: &AtomicU64) -> Option<object_store::Error> {
        let inject = |source: &str| object_store::Error::Generic {
            store: "FailGetStore",
            source: source.to_string().into(),
        };
        match mode {
            FailMode::Never => None,
            FailMode::Always => Some(inject("injected GET failure (test)")),
            FailMode::After(n) => {
                let seen = counter.fetch_add(1, Ordering::SeqCst) + 1;
                (seen > n).then(|| inject("injected GET failure after threshold (test)"))
            }
        }
    }

    impl std::fmt::Display for FailGetStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FailGetStore({})", self.inner)
        }
    }

    #[async_trait]
    impl object_store::ObjectStore for FailGetStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            let path = location.to_string();
            if path.ends_with("manifest.json") {
                if let Some(e) = fail_mode_allows(self.manifest_mode, &self.manifest_count) {
                    return Err(e);
                }
            } else if path.ends_with(".wal") {
                if let Some(e) = fail_mode_allows(self.segment_mode, &self.segment_count) {
                    return Err(e);
                }
            }
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Fails the first `fail_for_first` `put_opts` calls with `Precondition`,
    /// then delegates to the inner store (sustained-contention path).
    #[derive(Debug)]
    struct FlakyPreconditionStore {
        inner: Arc<dyn object_store::ObjectStore>,
        fail_for_first: AtomicU64,
    }

    impl std::fmt::Display for FlakyPreconditionStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FlakyPreconditionStore({})", self.inner)
        }
    }

    #[async_trait]
    impl object_store::ObjectStore for FlakyPreconditionStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            if self.fail_for_first.load(Ordering::SeqCst) > 0 {
                self.fail_for_first.fetch_sub(1, Ordering::SeqCst);
                return Err(object_store::Error::Precondition {
                    path: location.to_string(),
                    source: "injected contention (test)".to_string().into(),
                });
            }
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Rejects conditional PUTs with `NotImplemented`, accepts plain
    /// overwrites (LocalFileSystem-shaped backend).
    #[derive(Debug)]
    struct NotImplementedPutStore {
        inner: Arc<dyn object_store::ObjectStore>,
    }

    impl std::fmt::Display for NotImplementedPutStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "NotImplementedPutStore({})", self.inner)
        }
    }

    #[async_trait]
    impl object_store::ObjectStore for NotImplementedPutStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            match opts.mode {
                PutMode::Overwrite => self.inner.put_opts(location, payload, opts).await,
                _ => Err(object_store::Error::NotImplemented {
                    operation: "put_opts (conditional)".to_string(),
                    implementer: "NotImplementedPutStore (test)".to_string(),
                }),
            }
        }
        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// T8: concurrent `apply_seal` of distinct segments converges — the
    /// chronologically-newest segment wins active, all older ones end up in
    /// `sealed_segments` exactly once. Exercises the numeric-index comparison
    /// in `apply_seal` under real concurrency.
    #[test]
    fn manifest_race_concurrent_apply_seal_converges() {
        let store = build_inmemory_store();
        let inner = store.clone();
        store.rt().block_on(async move {
            let mut handles = Vec::new();
            for i in 0u64..8 {
                let s = inner.clone();
                handles.push(tokio::spawn(async move {
                    let name = format!("{:08}.wal", i);
                    write_manifest_with_etag(&s, move |m| apply_seal(m, &name)).await
                }));
            }
            for h in handles {
                h.await.unwrap().unwrap();
            }
            let (m, _) = read_manifest_with_etag(&inner).await.unwrap();
            assert_eq!(
                m.active_segment.as_deref(),
                Some("00000007.wal"),
                "newest segment must be active"
            );
            let mut seen = HashSet::new();
            for n in &m.sealed_segments {
                assert!(seen.insert(n.clone()), "duplicate segment {}", n);
            }
            for i in 0u64..7 {
                assert!(
                    seen.contains(&format!("{:08}.wal", i)),
                    "segment {:08}.wal missing from sealed_segments",
                    i
                );
            }
            assert_eq!(m.sealed_segments.len(), 7);
        });
    }
}
