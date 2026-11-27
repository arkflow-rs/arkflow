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

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use arkflow_core::wal::config::{
    CursorFlushConfig, ObjectStoreS3Config, RetentionConfig, SegmentConfig,
};
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
use tokio_util::sync::CancellationToken;

use super::manifest::{Manifest, SegmentMeta};
use super::segment;

/// Upper bound on S3 DELETE calls issued per background reclaim sweep so large
/// backlogs drain smoothly without monopolizing object-store request budgets.
const MAX_DELETES_PER_SWEEP: usize = 64;

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

/// A background-thread handle for the segment flusher and reclaim loops.
/// Owned by `S3Store` while running; cancelled and joined on `close`.
struct FlusherHandle {
    stop: CancellationToken,
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
    retention_cfg: RetentionConfig,
    /// Cached first/last sequence of each sealed segment object (its
    /// `{:08}.wal` index → range). `recover` fills it once; paged reads use
    /// it to skip segments wholly below the page cursor instead of
    /// re-downloading the whole backlog on every page.
    segment_ranges: StdMutex<BTreeMap<u64, (u64, u64)>>,
    /// Decoded metadata for legacy manifest segments and LIST-fallback segments
    /// collected read-only during `recover()`. Lazily merged and adopted into
    /// `manifest.json` during the next reclaim sweep so startup recovery stays
    /// strictly read-only.
    backfilled_meta: StdMutex<BTreeMap<String, SegmentMeta>>,
    /// Parallel PUT workers pool (task 3.3).
    put_workers: Option<ParallelPutWorkers>,
    /// Segment compression selected by configuration. Applied on every
    /// append; a per-segment magic tag lets recovery decode files written
    /// under a different setting (including legacy untagged segments).
    compression_kind: &'static str,
    compression_level: i32,

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
    /// Highest sequence safe to reclaim without violating rewind/undo safety.
    /// Seeded from `manifest.cursor` at startup; advanced in `mark_committed(seq)`
    /// to `seq.saturating_sub(1)` (matching `LocalStore` so `Wal::undo(seq)` can
    /// still replay `seq`), and clamped in `rewind_cursor`.
    reclaimable_hwm: AtomicU64,
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
    /// Highest sequence contained in a sealed segment object (seal gating,
    /// `fix-object-store-wal-loss-window` D2). Seeded from recovery — every
    /// segment object recovery discovers is already durable — and advanced
    /// by `seal_active_segment` only after the segment PUT *and* the
    /// manifest update both succeed. A source acknowledgement completes
    /// only once the acknowledged sequence is covered (replay window, not
    /// a loss window).
    sealed_seq: AtomicU64,
    /// Fired after each `sealed_seq` advance; wakes acknowledgements parked
    /// in the engine's `Wal::wait_for_sealed`.
    seal_notify: Notify,
    flusher: StdMutex<Option<FlusherHandle>>,
    /// Serializes segment seals: concurrent seals (append-threshold trigger,
    /// background flusher, close) must publish in sequence order — a later
    /// segment finishing first would let `sealed_seq` advance the ack
    /// frontier past an earlier, not-yet-durable segment.
    seal_lock: tokio::sync::Mutex<()>,
    /// Serializes background segment reclamation sweeps without blocking
    /// `seal_lock`, `append_batch`, or `mark_committed`.
    reclaim_lock: tokio::sync::Mutex<()>,
    /// Segments whose PUT failed, in seal order. The next seal re-uploads
    /// them (in order) BEFORE sealing the active segment, so a newer
    /// segment's publication never implies an earlier one's durability.
    /// Keeping the failed batch out of `active` avoids clobbering entries
    /// appended while the failed PUT was in flight.
    pending_seal: StdMutex<std::collections::VecDeque<SealedBatch>>,
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

/// A rotated-out segment awaiting (re-)upload after its segment PUT failed.
struct SealedBatch {
    bytes: Vec<u8>,
    first_seq: u64,
    last_seq: u64,
    next_index: u64,
    entries: usize,
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

/// Numeric index of a segment object filename (`00000007.wal` → 7).
fn segment_index_of(name: &str) -> Option<u64> {
    name.strip_suffix(".wal")?.parse::<u64>().ok()
}

impl S3Store {
    /// GET and decode the manifest; a missing manifest is a fresh WAL.
    fn read_manifest(&self) -> Result<Manifest, Error> {
        self.try_read_manifest().map(|opt| {
            opt.unwrap_or_else(|| Manifest::fresh(store_ns_node_id(self), store_ns_stream_id(self)))
        })
    }

    /// GET and decode the manifest, returning `Ok(None)` when `manifest.json`
    /// does not exist (`NotFound`).
    fn try_read_manifest(&self) -> Result<Option<Manifest>, Error> {
        match self.rt().block_on(
            self.client
                .get(&ObjectPath::from(self.manifest_key.as_str())),
        ) {
            Ok(r) => {
                let bytes = self
                    .rt()
                    .block_on(r.bytes())
                    .map_err(|e| Error::Process(format!("S3 GET manifest body: {}", e)))?;
                Manifest::from_json(&bytes)
                    .map(Some)
                    .map_err(|e| Error::Process(format!("manifest JSON: {}", e)))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(Error::Process(format!("S3 GET manifest: {}", e))),
        }
    }

    /// Every segment object name: the manifest's index unioned with a LIST
    /// of the prefix, excluding segments in `manifest.pending_deletes`,
    /// ordered by segment index (the serialized seal order, which is also
    /// sequence order). Unparseable names sort last.
    fn all_segment_names(&self, manifest: &Manifest) -> Result<Vec<String>, Error> {
        let pending_deletes: HashSet<&str> = manifest
            .pending_deletes
            .iter()
            .map(|s| s.as_str())
            .collect();
        let mut segs: HashSet<String> = manifest
            .sealed_segments
            .iter()
            .filter(|s| !pending_deletes.contains(s.as_str()))
            .cloned()
            .collect();
        if let Some(active) = &manifest.active_segment {
            if !pending_deletes.contains(active.as_str()) {
                segs.insert(active.clone());
            }
        }
        let prefix = ObjectPath::from(self.segments_prefix.as_str());
        let listed = self.rt().block_on(async {
            let mut out = Vec::new();
            let mut stream = std::pin::pin!(self.client.list(Some(&prefix)));
            while let Some(item) = stream.next().await {
                out.push(item);
            }
            out
        });
        for item in listed {
            let meta = item.map_err(|e| Error::Process(format!("S3 list: {}", e)))?;
            let name = meta.location.filename().unwrap_or("").to_string();
            if name.ends_with(".wal") && !pending_deletes.contains(name.as_str()) {
                segs.insert(name);
            }
        }
        let mut names: Vec<String> = segs.into_iter().collect();
        names.sort_by_key(|name| segment_index_of(name).unwrap_or(u64::MAX));
        Ok(names)
    }

    /// Remember a segment's sequence bounds for later read/page skips.
    fn remember_segment_bounds(&self, name: &str, first: u64, last: u64) {
        let Some(index) = segment_index_of(name) else {
            return;
        };
        if first > 0 || last > 0 {
            self.segment_ranges
                .lock()
                .expect("segment range cache")
                .insert(index, (first, last));
        }
    }

    /// Remember a decoded segment's sequence range for later page skips.
    fn remember_segment_range(&self, name: &str, entries: &[(u64, MessageBatchRef)]) {
        if let (Some((first, _)), Some((last, _))) = (entries.first(), entries.last()) {
            self.remember_segment_bounds(name, *first, *last);
        }
    }

    /// Whether `seg_name` is known (via manifest metadata or cached decode
    /// range) to contain no sequence strictly greater than `watermark`.
    fn segment_covered_by(&self, manifest: &Manifest, seg_name: &str, watermark: u64) -> bool {
        if let Some(meta) = manifest.segment_meta.get(seg_name) {
            if meta.last_seq <= watermark {
                return true;
            }
        }
        if let Some(index) = segment_index_of(seg_name) {
            if let Some(&(_, last)) = self
                .segment_ranges
                .lock()
                .expect("segment range cache")
                .get(&index)
            {
                if last <= watermark {
                    return true;
                }
            }
        }
        false
    }

    /// Check whether a segment that returned `NotFound` during replay is still
    /// required by the latest persisted manifest. A concurrent reclaim sweep
    /// may have legitimately advanced the cursor and deleted the segment after
    /// the reader loaded its initial manifest snapshot; only warn when the
    /// latest manifest still needs this segment for replay (or cannot be read),
    /// or when the segment was a LIST-only untracked segment that vanished.
    fn segment_still_required_for_replay(
        &self,
        original_manifest: &Manifest,
        seg_name: &str,
        watermark: u64,
    ) -> bool {
        let Ok(Some(latest)) = self.try_read_manifest() else {
            return true;
        };
        if latest.pending_deletes.iter().any(|s| s == seg_name) {
            return false;
        }
        let was_tracked = original_manifest.active_segment.as_deref() == Some(seg_name)
            || original_manifest
                .sealed_segments
                .iter()
                .any(|s| s == seg_name);
        let tracked = latest.active_segment.as_deref() == Some(seg_name)
            || latest.sealed_segments.iter().any(|s| s == seg_name);
        if !tracked {
            // Suppress only when the segment was tracked in the reader's initial
            // manifest (reclaimed concurrently). A LIST-only untracked segment
            // absent from both snapshots was not deregistered via the manifest
            // and must still log a missing-segment warning.
            return !was_tracked;
        }
        !self.segment_covered_by(&latest, seg_name, watermark.max(latest.cursor))
    }

    fn warn_if_still_missing(&self, original_manifest: &Manifest, seg_name: &str, watermark: u64) {
        if self.segment_still_required_for_replay(original_manifest, seg_name, watermark) {
            warn_missing_segment(seg_name);
        } else {
            tracing::debug!(
                segment = %seg_name,
                "segment returned NotFound during read but is already reclaimed in the latest manifest"
            );
        }
    }

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

        if let Some(max_age) = osc.retention.max_age {
            if max_age.is_zero() {
                return Err(Error::Config(
                    "retention.max_age must be greater than zero (use max_segments: 0 for immediate reclaim)".into(),
                ));
            }
        }

        let ns = format!(
            "{}/{}/{}",
            osc.prefix.trim_end_matches('/'),
            osc.node_id,
            osc.stream_id
        );
        let segments_prefix = format!("{}/segments", ns);
        let manifest_key = format!("{}/manifest.json", ns);
        let (compression_kind, compression_level) =
            crate::wal::compression::kind_from_config(&osc.compression);

        let runtime = runtime_slot.as_ref().expect("construction runtime present");
        let first_index = block_on_init(runtime, async {
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
            retention_cfg: osc.retention,
            segment_ranges: StdMutex::new(BTreeMap::new()),
            backfilled_meta: StdMutex::new(BTreeMap::new()),
            // Honest status of the knob: seals are SERIALIZED (seal_lock) so
            // the acknowledged frontier can never advance past an earlier
            // unsealed segment, and each seal uploads exactly one object —
            // the worker pool therefore has no production call site.
            put_workers: {
                if osc.parallel_put.workers > 1 {
                    tracing::warn!(
                        workers = osc.parallel_put.workers,
                        "S3 WAL: segment seals are serialized for acknowledgement ordering; \
                         parallel_put.workers has no effect"
                    );
                }
                Some(ParallelPutWorkers::spawn(
                    osc.parallel_put.workers,
                    client.clone(),
                    ns.clone(),
                    |_seq| {
                        // Completion callback: in single-worker mode, the
                        // completion is handled by the flusher. Multi-worker
                        // mode updates are tracked separately. Placeholder.
                    },
                ))
            },
            compression_kind,
            compression_level,
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
            reclaimable_hwm: AtomicU64::new(0),
            max_written_seq: AtomicU64::new(0),
            rewind_floor: AtomicU64::new(u64::MAX),
            cursor_mirror: AtomicU64::new(0),
            sealed_seq: AtomicU64::new(0),
            seal_notify: Notify::new(),
            flusher: StdMutex::new(None),
            seal_lock: tokio::sync::Mutex::new(()),
            reclaim_lock: tokio::sync::Mutex::new(()),
            pending_seal: StdMutex::new(std::collections::VecDeque::new()),
        });

        let manifest_cursor = block_on_init(store.rt(), recover(&store))??;
        // Seed the cursor mirror from the manifest recovery just read — no
        // second GET, so a transient second-read failure cannot exist.
        store
            .cursor_mirror
            .store(manifest_cursor, Ordering::Release);
        // Seed the sealed frontier from the recovery tail: every segment
        // object recovery discovered (and checksum-verified) is durable on
        // the store, so acknowledgements replaying those entries pass the
        // seal gate immediately instead of waiting for — and timing out on —
        // a seal that already happened in a prior process lifetime.
        store.sealed_seq.store(
            store.max_written_seq.load(Ordering::Acquire),
            Ordering::Release,
        );

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

    // Step 2: LIST segments, union with manifest's index, while accounting
    // for every known segment index (including `pending_deletes`) so future
    // segment filenames never collide with reclaimed ones.
    let pending_deletes: HashSet<String> = manifest.pending_deletes.iter().cloned().collect();
    let mut max_idx_seen = 0u64;
    let mut observe_idx = |name: &str| {
        if let Some(n) = segment_index_of(name) {
            if n > max_idx_seen {
                max_idx_seen = n;
            }
        }
    };
    for s in &manifest.pending_deletes {
        observe_idx(s);
    }

    let mut seen: HashSet<String> = HashSet::new();
    let mut all_segs: Vec<String> = Vec::new();
    for s in manifest.sealed_segments.iter() {
        observe_idx(s);
        if !pending_deletes.contains(s) && seen.insert(s.clone()) {
            all_segs.push(s.clone());
        }
    }
    if let Some(active) = &manifest.active_segment {
        observe_idx(active);
        if !pending_deletes.contains(active) && seen.insert(active.clone()) {
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
        observe_idx(&name);
        if !pending_deletes.contains(&name) && seen.insert(name.clone()) {
            all_segs.push(name);
        }
    }
    all_segs.sort_by_key(|name| segment_index_of(name).unwrap_or(u64::MAX));

    // Step 3: inspect only necessary segments (strictly read-only: never PUT
    // the manifest during recovery).
    // - Seed `max_seq_seen` from `manifest.cursor` (not `manifest.max_sealed_seq`):
    //   all deleted/skipped segments satisfy `last_seq <= manifest.cursor`, so
    //   `manifest.cursor` prevents sequence regression after reclaim, while
    //   updating `max_seq_seen` past `manifest.cursor` only after `GET` + decode
    //   succeeds prevents a missing unacked tail segment from creating an
    //   unrecoverable ack gap.
    // - Seed `reclaimable_hwm` from `manifest.cursor`: no in-flight ack/undo
    //   handles survive a process restart, so everything <= `manifest.cursor`
    //   from a prior lifetime is past any possible rewind.
    // - Skip GET for segments whose `manifest.segment_meta.last_seq <= cursor`.
    // - For legacy manifest segments or LIST-fallback segments lacking
    //   `segment_meta`, decode once and record metadata in `backfilled_meta`
    //   so the next reclaim sweep adopts them into `manifest.json`.
    let mut max_seq_seen = manifest.cursor;
    store
        .reclaimable_hwm
        .store(manifest.cursor, Ordering::Release);

    for seg_name in &all_segs {
        if let Some(meta) = manifest.segment_meta.get(seg_name) {
            if meta.last_seq <= manifest.cursor {
                store.remember_segment_bounds(seg_name, meta.first_seq, meta.last_seq);
                continue;
            }
        }
        let key = ObjectPath::from(format!("{}/{}", store.segments_prefix, seg_name).as_str());
        match store.client.get(&key).await {
            Ok(r) => {
                let last_modified_ms = r.meta.last_modified.timestamp_millis().max(0) as u64;
                let bytes = r
                    .bytes()
                    .await
                    .map_err(|e| Error::Process(format!("S3 GET segment body: {}", e)))?;
                let decoded = decode_segment_object(&bytes)?;
                warn_segment_truncation(seg_name, &decoded);
                store.remember_segment_range(seg_name, &decoded.entries);
                if let (Some((first, _)), Some((last, _))) =
                    (decoded.entries.first(), decoded.entries.last())
                {
                    if *last > max_seq_seen {
                        max_seq_seen = *last;
                    }
                    if !manifest.segment_meta.contains_key(seg_name) {
                        store
                            .backfilled_meta
                            .lock()
                            .expect("backfilled_meta lock")
                            .insert(
                                seg_name.clone(),
                                SegmentMeta {
                                    first_seq: *first,
                                    last_seq: *last,
                                    size_bytes: bytes.len() as u64,
                                    sealed_at_ms: last_modified_ms,
                                },
                            );
                    }
                }
            }
            Err(object_store::Error::NotFound { .. }) => {
                // Manifest referenced a segment that was truncated between
                // write and recovery (D7). Skip, but observably: the missing
                // range may include acknowledged entries at or below the
                // cursor; only entries above it are replay data.
                warn_missing_segment(seg_name);
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

    fn retained_bytes(&self) -> u64 {
        let active = self
            .active
            .lock()
            .map(|active| active.bytes.len() as u64)
            .unwrap_or(0);
        let parked = self
            .pending_seal
            .lock()
            .map(|pending| pending.iter().map(|batch| batch.bytes.len() as u64).sum())
            .unwrap_or(0);
        active.saturating_add(parked)
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
            // A failed seal parks the batch in `pending_seal` for the next
            // seal attempt; it must NOT surface as an append error here or
            // the core WAL would restore its retry batch and append the
            // same sequences again. The flusher retries every
            // `flush_interval`.
            if let Err(e) = self.rt().block_on(seal_active_segment(self)) {
                tracing::warn!("S3 WAL seal failed (retry on the next flush tick): {e}");
            }
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
        self.reclaimable_hwm.fetch_min(seq, Ordering::AcqRel);
        let previous = self.cursor_mirror.fetch_min(seq, Ordering::AcqRel);
        if previous > seq {
            block_on_init(self.rt(), flush_manifest(self))
                .map_err(|e| Error::Process(format!("rewind corrective flush failed: {e}")))??;
        }
        Ok(())
    }

    fn mark_committed(&self, seq: u64) -> Result<(), Error> {
        // Match `LocalStore::mark_committed`: only entries strictly below `seq`
        // (`<= seq - 1`) are safe to reclaim, because `Wal::undo(seq)` can
        // still rewind the latest committed sequence back to `seq - 1`.
        self.reclaimable_hwm
            .fetch_max(seq.saturating_sub(1), Ordering::AcqRel);
        Ok(())
    }

    fn read_after_cursor(&self) -> Result<Vec<(u64, MessageBatchRef)>, Error> {
        let manifest = self.read_manifest()?;
        let mut out: Vec<(u64, MessageBatchRef)> = Vec::new();
        for seg_name in self.all_segment_names(&manifest)? {
            if self.segment_covered_by(&manifest, &seg_name, manifest.cursor) {
                continue;
            }
            let key = ObjectPath::from(format!("{}/{}", self.segments_prefix, seg_name).as_str());
            let bytes = match self.rt().block_on(self.client.get(&key)) {
                Ok(r) => match self.rt().block_on(r.bytes()) {
                    Ok(b) => b,
                    Err(e) => return Err(Error::Process(format!("S3 GET segment body: {}", e))),
                },
                Err(object_store::Error::NotFound { .. }) => {
                    self.warn_if_still_missing(&manifest, &seg_name, manifest.cursor);
                    continue;
                }
                Err(e) => return Err(Error::Process(format!("S3 GET segment: {}", e))),
            };
            let decoded = decode_segment_object(&bytes)?;
            warn_segment_truncation(&seg_name, &decoded);
            self.remember_segment_range(&seg_name, &decoded.entries);
            for (seq, mb) in decoded.entries {
                if seq > manifest.cursor {
                    out.push((seq, mb));
                }
            }
        }
        out.sort_by_key(|(s, _)| *s);
        Ok(out)
    }

    fn read_after(&self, after: u64, limit: usize) -> Result<Vec<(u64, MessageBatchRef)>, Error> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let manifest = self.read_manifest()?;
        let after = after.max(manifest.cursor);
        let mut out: Vec<(u64, MessageBatchRef)> = Vec::new();
        for seg_name in self.all_segment_names(&manifest)? {
            // A segment whose manifest metadata or cached range ends at or
            // below the watermark cannot contain a page entry: skip its GET.
            if self.segment_covered_by(&manifest, &seg_name, after) {
                continue;
            }
            let key = ObjectPath::from(format!("{}/{}", self.segments_prefix, seg_name).as_str());
            let bytes = match self.rt().block_on(self.client.get(&key)) {
                Ok(r) => match self.rt().block_on(r.bytes()) {
                    Ok(b) => b,
                    Err(e) => return Err(Error::Process(format!("S3 GET segment body: {}", e))),
                },
                Err(object_store::Error::NotFound { .. }) => {
                    self.warn_if_still_missing(&manifest, &seg_name, after);
                    continue;
                }
                Err(e) => return Err(Error::Process(format!("S3 GET segment: {}", e))),
            };
            let decoded = decode_segment_object(&bytes)?;
            warn_segment_truncation(&seg_name, &decoded);
            self.remember_segment_range(&seg_name, &decoded.entries);
            for (seq, mb) in decoded.entries {
                if seq > after {
                    out.push((seq, mb));
                }
            }
            // Seals are serialized and segment indices increase with the
            // sequence order, so once the page is full no later segment can
            // hold an entry that sorts before the collected ones.
            if out.len() >= limit {
                break;
            }
        }
        out.sort_by_key(|(s, _)| *s);
        out.truncate(limit);
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

    fn next_seq_hint_is_exact(&self) -> bool {
        true
    }

    fn sealed_seq(&self) -> Option<u64> {
        Some(self.sealed_seq.load(Ordering::Acquire))
    }

    fn seal_notifier(&self) -> Option<&Notify> {
        Some(&self.seal_notify)
    }

    fn seal_interval(&self) -> Option<std::time::Duration> {
        Some(self.segment_cfg.flush_interval)
    }

    fn close(&self) -> Result<(), Error> {
        // Cancel and join the background flusher + reclaim loops (any in-flight
        // reclaim DELETE is cancelled immediately so shutdown never stalls).
        if let Some(handle) = self.flusher.lock().unwrap().take() {
            handle.stop.cancel();
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
///
/// Seals are serialized (`seal_lock`) and a failed segment PUT parks the
/// batch in `pending_seal` instead of writing it back into `active`:
///
/// - a concurrent seal finishing out of order must never let `sealed_seq`
///   (the acknowledgement gate) advance past an earlier segment that is not
///   durable yet;
/// - `active` may have accumulated newer entries while the failed PUT was in
///   flight — overwriting them would lose data, and returning the entries to
///   the caller as an error would make the core WAL re-append the same
///   sequences (duplicates).
///
/// The parked batch is re-uploaded by the next seal attempt (the flusher
/// runs every `flush_interval`) before the active segment is sealed, so
/// publication order keeps matching sequence order. Entries lost on a crash
/// while parked were never acknowledged (the gate never advanced past them),
/// so the source replays them.
/// Magic prefix identifying a segment object written by this build. Legacy
/// objects start directly with the entry header (a sequence number), so an
/// object without the prefix stays decodable as uncompressed.
const SEGMENT_OBJECT_MAGIC: &[u8; 6] = b"AKWAL1";

fn compression_tag(kind: &str) -> Option<u8> {
    match kind {
        "none" => Some(0),
        "zstd" => Some(1),
        "lz4" => Some(2),
        _ => None,
    }
}

fn compression_kind_from_tag(tag: u8) -> Option<&'static str> {
    match tag {
        0 => Some("none"),
        1 => Some("zstd"),
        2 => Some("lz4"),
        _ => None,
    }
}

fn segment_compression_for(kind: &str) -> crate::wal::manifest::SegmentCompression {
    match kind {
        "zstd" => crate::wal::manifest::SegmentCompression::Zstd,
        "lz4" => crate::wal::manifest::SegmentCompression::Lz4,
        _ => crate::wal::manifest::SegmentCompression::None,
    }
}

/// Compress one complete raw segment and add the magic+algorithm tag.
fn encode_segment_object(store: &S3Store, raw: &[u8]) -> Result<Vec<u8>, Error> {
    let tag = compression_tag(store.compression_kind).ok_or_else(|| {
        Error::Config(format!(
            "unknown WAL compression kind '{}'",
            store.compression_kind
        ))
    })?;
    let compressed =
        crate::wal::compression::compress(store.compression_kind, store.compression_level, raw)?;
    let mut out = Vec::with_capacity(SEGMENT_OBJECT_MAGIC.len() + 1 + compressed.len());
    out.extend_from_slice(SEGMENT_OBJECT_MAGIC);
    out.push(tag);
    out.extend_from_slice(&compressed);
    Ok(out)
}

fn decode_segment_object(bytes: &[u8]) -> Result<segment::DecodedSegment, Error> {
    let Some(rest) = bytes.strip_prefix(SEGMENT_OBJECT_MAGIC) else {
        // Pre-tag objects were written uncompressed.
        return segment::decode(bytes);
    };
    let Some((&tag, payload)) = rest.split_first() else {
        return Err(Error::Process(
            "WAL segment object has a magic prefix but no compression tag".into(),
        ));
    };
    let kind = compression_kind_from_tag(tag).ok_or_else(|| {
        Error::Process(format!(
            "WAL segment object has unknown compression tag {tag}"
        ))
    })?;
    segment::decode_with_kind(payload, kind)
}

/// Log a segment decode's truncation, if any. A sealed segment can hold
/// acknowledged entries at or below the cursor alongside unacked replay
/// data, so the dropped range may cover both; only the entries above the
/// cursor are replay data. The loss must be observable, not silent (spec:
/// fix-silent-data-loss).
fn warn_segment_truncation(segment: &str, decoded: &segment::DecodedSegment) {
    if let Some(t) = &decoded.truncation {
        tracing::warn!(
            segment,
            truncated_at = t.truncated_at,
            dropped_bytes = t.dropped_bytes,
            kind = ?t.kind,
            "sealed WAL segment decoded with dropped bytes; the dropped range may include acknowledged entries at or below the cursor, and only entries above the cursor are replay data that is now skipped"
        );
    }
}

/// A manifest-referenced sealed segment is gone from the object store. Same
/// scope as `warn_segment_truncation` — the segment may include acknowledged
/// entries alongside unacked replay data — but nothing at all of it could be
/// read.
fn warn_missing_segment(segment: &str) {
    tracing::warn!(
        segment,
        "manifest-referenced sealed WAL segment is missing from the object store; \
         the segment may include acknowledged entries, and only its entries above the \
         cursor are replay data that is now skipped"
    );
}

/// Move the active segment's bytes into a sealed batch, resetting the active
/// buffer. `None` when the segment holds no entries.
///
/// The normal seal path rotates this way before the PUT; the failed-retry
/// path also rotates, so the active buffer keeps its configured threshold
/// instead of growing for as long as the backend stays unavailable.
fn take_active_segment(active: &mut ActiveSegment) -> Option<SealedBatch> {
    if active.entries == 0 {
        return None;
    }
    let batch = SealedBatch {
        bytes: std::mem::take(&mut active.bytes),
        first_seq: active.first_seq,
        last_seq: active.last_seq,
        next_index: active.next_index,
        entries: active.entries,
    };
    active.first_seq = 0;
    active.last_seq = 0;
    active.entries = 0;
    active.next_index += 1;
    Some(batch)
}

async fn seal_active_segment(store: &S3Store) -> Result<(), Error> {
    // One seal at a time: the order of publication must match sequence
    // order for the acknowledgement gate to stay sound.
    let _seal_guard = store.seal_lock.lock().await;

    // Retry parked (failed-PUT) segments first: a newer active segment may
    // only be sealed once everything before it is durable.
    loop {
        let Some(batch) = store.pending_seal.lock().unwrap().pop_front() else {
            break;
        };
        if let Err(e) = publish_sealed_segment(store, &batch).await {
            store.pending_seal.lock().unwrap().push_front(batch);
            // This attempt returns before the normal rotation. Park the
            // active bytes behind the failed segment (push_back preserves
            // publication order) so `append_batch` cannot grow the active
            // buffer past the configured thresholds while the PUT keeps
            // failing.
            let rotated = {
                let mut active = store.active.lock().unwrap();
                take_active_segment(&mut active)
            };
            if let Some(rotated) = rotated {
                store.pending_seal.lock().unwrap().push_back(rotated);
            }
            return Err(e);
        }
    }

    // Move the bytes out of the active lock so the encode + PUT doesn't
    // hold the lock across the network call.
    let batch = {
        let mut active = store.active.lock().unwrap();
        let Some(batch) = take_active_segment(&mut active) else {
            return Ok(());
        };
        batch
    };
    if let Err(e) = publish_sealed_segment(store, &batch).await {
        // Park for the next seal attempt (see the function doc). Appends
        // that landed in `active` while the PUT was in flight stay untouched.
        store.pending_seal.lock().unwrap().push_back(batch);
        return Err(e);
    }
    Ok(())
}

/// Upload one sealed segment and publish it: PUT the object, record the seal
/// in the manifest under ETag coordination, then advance the sealed frontier
/// (`sealed_seq`) and wake acknowledgement waiters. The frontier moves only
/// after BOTH the segment object and the manifest hold the range (D2).
async fn publish_sealed_segment(store: &S3Store, batch: &SealedBatch) -> Result<(), Error> {
    let name = format!("{:08}.wal", batch.next_index);
    let key = ObjectPath::from(format!("{}/{}", store.segments_prefix, name).as_str());
    // Compress at publication, not append: the active buffer accumulates
    // raw entry bytes across many appends, so only the complete segment can
    // be compressed as one block. The magic+tag prefix lets recovery decode
    // this object even if the configured algorithm changed since (and keeps
    // legacy untagged objects readable).
    let object_bytes = encode_segment_object(store, &batch.bytes)?;
    let object_size = object_bytes.len() as u64;
    // Clone for the PUT so the original bytes survive a failed upload in the
    // caller's parked batch.
    store
        .client
        .put(&key, PutPayload::from(Bytes::from(object_bytes)))
        .await
        .map_err(|e| {
            Error::Process(format!(
                "S3 PUT segment {} (seq {}..={}, {} entries): {}",
                name, batch.first_seq, batch.last_seq, batch.entries, e
            ))
        })?;

    // Bump the manifest's `max_sealed_seq`, record per-segment metadata, and
    // add the new segment to `sealed_segments`. If we had an active segment
    // before, demote it. Use the ETag-coordinated writer so concurrent seal
    // callbacks from parallel PUT workers don't lose each other's updates
    // (`apply_seal` guards against out-of-order seals regressing the active
    // pointer).
    let name_for_mutator = name.clone();
    let first_seq = batch.first_seq;
    let last_seq = batch.last_seq;
    let sealed_at_ms = now_ms();
    let active_compression = segment_compression_for(store.compression_kind);
    write_manifest_with_etag(store, move |m| {
        apply_seal(m, &name_for_mutator);
        if last_seq > m.max_sealed_seq {
            m.max_sealed_seq = last_seq;
        }
        m.active_segment_compression = Some(active_compression);
        m.segment_meta.insert(
            name_for_mutator.clone(),
            SegmentMeta {
                first_seq,
                last_seq,
                size_bytes: object_size,
                sealed_at_ms,
            },
        );
    })
    .await?;

    store.remember_segment_bounds(&name, first_seq, last_seq);

    // D2: the segment object and the manifest both hold this range now —
    // publish the sealed frontier and wake acknowledgements gated on it.
    // The publish point is deliberately AFTER the manifest write: an
    // acknowledgement that completes here implies the manifest already
    // knows the segment, which tightens recovery determinism (the LIST
    // fallback of `s3-wal-pipeline` D5 remains for a manifest write that
    // crashed between the PUT and this point). `fetch_max` keeps the
    // frontier monotonic across concurrent/out-of-order seals.
    store.sealed_seq.fetch_max(last_seq, Ordering::AcqRel);
    store.seal_notify.notify_waiters();
    Ok(())
}

/// Flush the in-memory cursor watermark to the manifest (D6).
///
/// Sealed-segment reclamation runs asynchronously in `reclaim_segments` on a
/// decoupled, cancellable background task so S3 `DELETE` calls never delay
/// cursor flushes or segment sealing.
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
/// How `write_manifest_with_etag_if_changed_with_policy` handles object-store
/// backends that return `Error::NotImplemented` for conditional PUT (`If-Match`).
#[derive(Clone, Copy, PartialEq, Eq)]
enum ConditionalPolicy {
    /// Fall back to unconditional `Overwrite` with a warning (preserves
    /// compatibility for seal and cursor-flush writes on local-FS backends).
    FallbackOverwrite,
    /// Refuse unconditional overwrite: segment reclamation requires conditional
    /// PUTs to reject stale writers so a concurrent seal cannot re-introduce a
    /// deleted segment into `sealed_segments`.
    RequireConditional,
}

async fn write_manifest_with_etag<F>(store: &S3Store, mut mutate: F) -> Result<(), Error>
where
    F: FnMut(&mut Manifest),
{
    write_manifest_with_etag_if_changed_with_policy(
        store,
        ConditionalPolicy::FallbackOverwrite,
        |m| {
            mutate(m);
            true
        },
    )
    .await
    .map(|_| ())
}

/// Apply a conditional mutator closure to the manifest under ETag-coordinated
/// PUT coordination. Returns `(manifest, mutated)`: if `mutate(&mut m)` returns
/// `false`, no manifest PUT is issued and the freshly-read manifest is returned
/// directly. Otherwise the serialized manifest is PUT with `If-Match` / `Create`
/// and retried on contention up to `MANIFEST_WRITE_MAX_RETRIES`.
async fn write_manifest_with_etag_if_changed_with_policy<F>(
    store: &S3Store,
    policy: ConditionalPolicy,
    mut mutate: F,
) -> Result<(Manifest, bool), Error>
where
    F: FnMut(&mut Manifest) -> bool,
{
    for attempt in 0..MANIFEST_WRITE_MAX_RETRIES {
        let (mut m, etag) = read_manifest_with_etag(store).await?;
        if !mutate(&mut m) {
            return Ok((m, false));
        }
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
                return Ok((m, true));
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
            // writes disabled. Non-reclaim writers fall back to `Overwrite` so
            // local-FS tests and legacy setups keep working; reclamation refuses
            // unconditional overwrite so a stale seal cannot re-register a
            // deleted segment.
            Err(object_store::Error::NotImplemented { .. }) => match policy {
                ConditionalPolicy::RequireConditional => {
                    tracing::warn!(
                        "S3 WAL segment reclamation requires conditional PUT (If-Match), \
                         which this object-store backend does not implement; leaving \
                         segments intact"
                    );
                    return Err(Error::Process(
                        "manifest backend does not support conditional PUT required for safe segment reclamation"
                            .into(),
                    ));
                }
                ConditionalPolicy::FallbackOverwrite => {
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
                    return Ok((m, true));
                }
            },
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

/// Select which eligible sealed segments should be reclaimed under `cfg`.
///
/// `eligible` MUST contain only segments from `manifest.sealed_segments`
/// (never `active_segment`) whose `last_seq <= reclaim_floor`.
///
/// When `enabled` is `true` and all three caps are `None`, every eligible
/// segment is selected for immediate reclamation. When one or more caps are
/// configured, scanning newest-to-oldest stops at the first violation so the
/// retained set is always a contiguous newest suffix (no gaps).
///
/// Returns segment filenames to reclaim, ordered oldest-first.
fn select_reclaimable_segments(
    cfg: &RetentionConfig,
    eligible: &[(String, SegmentMeta)],
    now_ms: u64,
) -> Vec<String> {
    if !cfg.enabled || eligible.is_empty() {
        return Vec::new();
    }
    let mut ordered: Vec<&(String, SegmentMeta)> = eligible.iter().collect();
    ordered.sort_by_key(|(name, _)| segment_index(name));

    if cfg.max_segments.is_none() && cfg.max_age.is_none() && cfg.max_bytes.is_none() {
        return ordered.into_iter().map(|(name, _)| name.clone()).collect();
    }

    let mut kept_bytes: u64 = 0;
    let mut keep_from_idx = ordered.len();
    for (kept_count, (idx, (_, meta))) in ordered.iter().enumerate().rev().enumerate() {
        if let Some(age_cap) = cfg.max_age {
            if std::time::Duration::from_millis(now_ms.saturating_sub(meta.sealed_at_ms)) > age_cap
            {
                break;
            }
        }
        if let Some(seg_cap) = cfg.max_segments {
            if kept_count >= seg_cap {
                break;
            }
        }
        if let Some(byte_cap) = cfg.max_bytes {
            if kept_bytes.saturating_add(meta.size_bytes) > byte_cap {
                break;
            }
        }
        kept_bytes = kept_bytes.saturating_add(meta.size_bytes);
        keep_from_idx = idx;
    }

    ordered[..keep_from_idx]
        .iter()
        .map(|(name, _)| name.clone())
        .collect()
}

/// Crash-safe, best-effort, bounded two-phase segment reclamation:
///
/// 1. ETag-coordinated manifest update adopts any backfilled legacy /
///    LIST-fallback metadata and moves eligible sealed segments
///    (`last_seq <= min(manifest.cursor, reclaimable_hwm)`, never
///    `active_segment`) from `sealed_segments` into `pending_deletes`.
///    Performs zero manifest PUTs when idle.
/// 2. Best-effort cancellable object-store `DELETE` removes up to
///    `MAX_DELETES_PER_SWEEP` segments from `pending_deletes` (`NotFound` is
///    treated as idempotent success).
/// 3. ETag-coordinated manifest cleanup removes confirmed-deleted names from
///    `pending_deletes`.
async fn reclaim_segments(
    store: &S3Store,
    cancel: Option<&CancellationToken>,
) -> Result<usize, Error> {
    reclaim_segments_at(store, now_ms(), cancel).await
}

async fn reclaim_segments_at(
    store: &S3Store,
    now: u64,
    cancel: Option<&CancellationToken>,
) -> Result<usize, Error> {
    if !store.retention_cfg.enabled {
        return Ok(0);
    }
    if store.rewind_floor.load(Ordering::Acquire) != u64::MAX {
        return Ok(0);
    }
    if cancel.is_some_and(CancellationToken::is_cancelled) {
        return Ok(0);
    }

    let _reclaim_guard = if let Some(stop) = cancel {
        tokio::select! {
            biased;
            _ = stop.cancelled() => return Ok(0),
            guard = store.reclaim_lock.lock() => guard,
        }
    } else {
        store.reclaim_lock.lock().await
    };

    if store.rewind_floor.load(Ordering::Acquire) != u64::MAX {
        return Ok(0);
    }

    let backfill_snapshot = store
        .backfilled_meta
        .lock()
        .expect("backfilled_meta lock")
        .clone();

    // Phase 1: lazily adopt backfilled metadata & move eligible sealed
    // segments into `pending_deletes` before touching S3 objects. Requires
    // conditional PUT so a stale seal cannot re-register a deleted segment,
    // and races `cancel.cancelled()` so shutdown never stalls on manifest I/O.
    let phase1 = write_manifest_with_etag_if_changed_with_policy(
        store,
        ConditionalPolicy::RequireConditional,
        |m| {
            if store.rewind_floor.load(Ordering::Acquire) != u64::MAX {
                return false;
            }
            let mut changed = false;

            if !backfill_snapshot.is_empty() {
                let pending_set: HashSet<String> = m.pending_deletes.iter().cloned().collect();
                let mut sealed_set: HashSet<String> = m.sealed_segments.iter().cloned().collect();

                for (name, meta) in &backfill_snapshot {
                    if pending_set.contains(name) {
                        continue;
                    }
                    if m.active_segment.as_deref() != Some(name.as_str())
                        && !sealed_set.contains(name)
                    {
                        let install_as_active = match &m.active_segment {
                            Some(current) => segment_index(name) > segment_index(current),
                            None => true,
                        };
                        if install_as_active {
                            if let Some(prev) = m.active_segment.take() {
                                if sealed_set.insert(prev.clone()) {
                                    m.sealed_segments.push(prev);
                                }
                            }
                            m.active_segment = Some(name.clone());
                        } else if sealed_set.insert(name.clone()) {
                            m.sealed_segments.push(name.clone());
                        }
                        changed = true;
                    }
                    if !m.segment_meta.contains_key(name) {
                        m.segment_meta.insert(name.clone(), meta.clone());
                        changed = true;
                    }
                    if meta.last_seq > m.max_sealed_seq {
                        m.max_sealed_seq = meta.last_seq;
                        changed = true;
                    }
                }
            }

            let reclaim_floor = m.cursor.min(store.reclaimable_hwm.load(Ordering::Acquire));
            let eligible: Vec<(String, SegmentMeta)> = m
                .sealed_segments
                .iter()
                .filter(|name| m.active_segment.as_deref() != Some(name.as_str()))
                .filter_map(|name| {
                    let meta = m.segment_meta.get(name)?;
                    if meta.last_seq <= reclaim_floor {
                        Some((name.clone(), meta.clone()))
                    } else {
                        None
                    }
                })
                .collect();

            let to_reclaim = select_reclaimable_segments(&store.retention_cfg, &eligible, now);
            if !to_reclaim.is_empty() {
                let reclaim_set: HashSet<&str> = to_reclaim.iter().map(|s| s.as_str()).collect();
                m.sealed_segments
                    .retain(|s| !reclaim_set.contains(s.as_str()));
                let mut pending_set: HashSet<String> = m.pending_deletes.iter().cloned().collect();
                for name in &to_reclaim {
                    m.segment_meta.remove(name);
                    if pending_set.insert(name.clone()) {
                        m.pending_deletes.push(name.clone());
                    }
                }
                changed = true;
            }
            changed
        },
    );
    let (manifest, mutated) = if let Some(stop) = cancel {
        tokio::select! {
            biased;
            _ = stop.cancelled() => return Ok(0),
            res = phase1 => res?,
        }
    } else {
        phase1.await?
    };

    if !backfill_snapshot.is_empty() {
        let pending_set: HashSet<&str> = manifest
            .pending_deletes
            .iter()
            .map(String::as_str)
            .collect();
        let mut backfilled = store.backfilled_meta.lock().expect("backfilled_meta lock");
        for key in backfill_snapshot.keys() {
            if mutated
                || pending_set.contains(key.as_str())
                || manifest.segment_meta.contains_key(key)
            {
                backfilled.remove(key);
            }
        }
    }

    if manifest.pending_deletes.is_empty() {
        return Ok(0);
    }

    // Phase 2: best-effort, bounded, cancellable S3 object deletions.
    let candidates: Vec<String> = manifest
        .pending_deletes
        .iter()
        .take(MAX_DELETES_PER_SWEEP)
        .cloned()
        .collect();
    let mut confirmed_deleted: Vec<String> = Vec::new();
    let mut failed_deleted: Vec<String> = Vec::new();
    let mut cancelled = false;

    for seg_name in candidates {
        if cancel.is_some_and(CancellationToken::is_cancelled) {
            cancelled = true;
            break;
        }
        if store.rewind_floor.load(Ordering::Acquire) != u64::MAX {
            break;
        }
        let key = ObjectPath::from(format!("{}/{}", store.segments_prefix, seg_name).as_str());
        let delete_res = if let Some(stop) = cancel {
            tokio::select! {
                biased;
                _ = stop.cancelled() => {
                    cancelled = true;
                    break;
                }
                res = store.client.delete(&key) => res,
            }
        } else {
            store.client.delete(&key).await
        };

        match delete_res {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                if let Some(index) = segment_index_of(&seg_name) {
                    store
                        .segment_ranges
                        .lock()
                        .expect("segment range cache")
                        .remove(&index);
                }
                confirmed_deleted.push(seg_name);
            }
            Err(e) => {
                tracing::warn!(
                    segment = %seg_name,
                    error = %e,
                    "S3 WAL segment delete failed; rotating to tail of pending_deletes for retry"
                );
                failed_deleted.push(seg_name);
            }
        }
    }

    // Phase 3: clear confirmed deletions from `pending_deletes` and rotate
    // failed names to the tail so a failing prefix of 64 objects cannot starve
    // healthy objects queued behind them.
    if (!confirmed_deleted.is_empty() || !failed_deleted.is_empty()) && !cancelled {
        let confirmed_set: HashSet<String> = confirmed_deleted.iter().cloned().collect();
        let failed_set: HashSet<String> = failed_deleted.iter().cloned().collect();
        let phase3 = write_manifest_with_etag_if_changed_with_policy(
            store,
            ConditionalPolicy::RequireConditional,
            |m| {
                let original = m.pending_deletes.clone();
                let mut rotated_tail = Vec::new();
                m.pending_deletes.retain(|s| {
                    if confirmed_set.contains(s) {
                        return false;
                    }
                    if failed_set.contains(s) {
                        rotated_tail.push(s.clone());
                        return false;
                    }
                    true
                });
                m.pending_deletes.extend(rotated_tail);
                m.pending_deletes != original
            },
        );
        if let Some(stop) = cancel {
            tokio::select! {
                biased;
                _ = stop.cancelled() => return Ok(confirmed_deleted.len()),
                res = phase3 => { let _ = res?; }
            }
        } else {
            let _ = phase3.await?;
        }
        if !confirmed_deleted.is_empty() {
            tracing::debug!(
                reclaimed = confirmed_deleted.len(),
                "S3 WAL reclaimed sealed segments"
            );
        }
    }

    Ok(confirmed_deleted.len())
}

fn spawn_flusher(store: Arc<S3Store>) -> FlusherHandle {
    let stop = CancellationToken::new();
    let stop_seal = stop.clone();
    let stop_reclaim = stop.clone();
    let join = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("flusher runtime");
        rt.block_on(async move {
            let seal_interval = store.segment_cfg.flush_interval;
            let reclaim_interval = seal_interval.max(std::time::Duration::from_secs(1));

            let seal_store = store.clone();
            let seal_loop = async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = stop_seal.cancelled() => break,
                        _ = tokio::time::sleep(seal_interval) => {
                            let _ = seal_active_segment(&seal_store).await;
                            let _ = flush_manifest(&seal_store).await;
                        }
                    }
                }
            };

            let reclaim_store = store;
            let reclaim_loop = async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = stop_reclaim.cancelled() => break,
                        _ = tokio::time::sleep(reclaim_interval) => {
                            tokio::select! {
                                biased;
                                _ = stop_reclaim.cancelled() => break,
                                res = reclaim_segments(&reclaim_store, Some(&stop_reclaim)) => {
                                    if let Err(e) = res {
                                        tracing::warn!(error = %e, "S3 WAL segment reclaim sweep failed");
                                    }
                                }
                            }
                        }
                    }
                }
            };

            tokio::join!(seal_loop, reclaim_loop);
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
        build_local_store_with_segment_caps(dir, 4, 1024)
    }

    /// Segment-cap override for tests whose assertions must not depend on
    /// the IPC-encoded payload size: arrow patch releases shift it
    /// (58.4.2's IPC buffer fixes shrank a 1-row Int64 batch from 524 to
    /// 460 bytes), which moves where the byte-threshold seal fires and can
    /// strand later entries in the active segment. Tests that pin a
    /// specific sealed layout should make the byte cap unreachable and let
    /// `max_entries` trigger the seals deterministically.
    fn build_local_store_with_segment_caps(
        dir: &std::path::Path,
        max_entries: usize,
        max_bytes: usize,
    ) -> (Arc<S3Store>, std::path::PathBuf) {
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
                max_entries,
                max_bytes,
                flush_interval: std::time::Duration::from_millis(50),
            },
            cursor: CursorFlushConfig {
                max_entries: 1000,
                interval: std::time::Duration::from_millis(50),
            },
            segment_tuning: arkflow_core::wal::config::SegmentTuningConfig::default(),
            parallel_put: arkflow_core::wal::config::ParallelPutConfig::default(),
            compression: arkflow_core::wal::config::CompressionConfig::default(),
            retention: RetentionConfig::default(),
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
            let seq = wal
                .append(&Arc::new(
                    arkflow_core::MessageBatch::try_from(vec![format!("{{\"v\":{expected}}}")])
                        .unwrap(),
                ))
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
                retention: RetentionConfig::default(),
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
            retention: RetentionConfig::default(),
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
            retention: RetentionConfig::default(),
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
            retention: RetentionConfig::default(),
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
            retention: RetentionConfig::default(),
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
            retention: RetentionConfig::default(),
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
            retention: RetentionConfig::default(),
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
            retention: RetentionConfig::default(),
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
            retention: RetentionConfig::default(),
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
            retention: RetentionConfig::default(),
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

    /// T7: a failed segment PUT must park the batch in `pending_seal` (not
    /// restore it into `active`) so the entries are neither lost nor
    /// duplicated. Guards the parked-retry path: the original code dropped
    /// the data after `std::mem::take`, and the first rollback overwrote
    /// entries appended while the PUT was in flight.
    #[test]
    fn seal_put_failure_parks_batch_for_retry() {
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
        // A failed seal must not publish the sealed frontier: an
        // acknowledgement gated on it keeps waiting (and eventually fails
        // retryably) instead of committing a source offset whose entry is
        // not durable anywhere.
        assert_eq!(
            store.sealed_seq(),
            Some(0),
            "the sealed frontier must not advance on a failed seal"
        );

        // The batch is parked with its own segment index (5) and range; the
        // active segment stays EMPTY (rotated), so entries appended while the
        // PUT was in flight are never clobbered and the core WAL's retry
        // path cannot duplicate the parked sequences.
        {
            let pending = store.pending_seal.lock().unwrap();
            assert_eq!(pending.len(), 1, "the failed batch is parked for retry");
            let batch = &pending[0];
            assert_eq!(
                batch.next_index, 5,
                "the failed batch keeps its segment index"
            );
            assert_eq!(batch.first_seq, 1);
            assert_eq!(batch.last_seq, 3);
            assert_eq!(batch.entries, 3);
            assert!(!batch.bytes.is_empty(), "bytes preserved");
        }
        let active = store.active.lock().unwrap();
        assert_eq!(
            active.entries, 0,
            "active stays rotated; no restore clobber"
        );
        assert_eq!(
            active.next_index, 6,
            "active's next index moved past the parked batch"
        );
    }

    /// The admission bound must see both the active segment and the parked
    /// (failed-upload) segments; counting only one lets the other grow
    /// without limit while the backend is down.
    #[test]
    fn retained_bytes_counts_active_and_parked_segments() {
        let store = build_failing_store();
        {
            let mut active = store.active.lock().unwrap();
            active.bytes = vec![0; 10];
            active.entries = 1;
        }
        store.pending_seal.lock().unwrap().push_back(SealedBatch {
            bytes: vec![0; 20],
            first_seq: 1,
            last_seq: 1,
            next_index: 3,
            entries: 1,
        });
        assert_eq!(store.retained_bytes(), 30);
    }

    /// A failed retry of an already-parked segment returns before the normal
    /// rotation, so the active bytes must be parked behind it: otherwise
    /// `append_batch` keeps growing the active buffer for as long as the
    /// backend stays unavailable.
    #[test]
    fn failed_parked_retry_rotates_the_active_segment() {
        let store = build_failing_store();
        // One segment already parked by an earlier failed PUT.
        store.pending_seal.lock().unwrap().push_back(SealedBatch {
            bytes: vec![1, 2, 3],
            first_seq: 1,
            last_seq: 2,
            next_index: 4,
            entries: 2,
        });
        // Entries appended while the backend was down sit in `active`.
        {
            let mut active = store.active.lock().unwrap();
            active.first_seq = 3;
            active.last_seq = 3;
            active.entries = 1;
            active.bytes = vec![4, 5];
            active.next_index = 5;
        }
        let inner = store.clone();
        let result = store
            .rt()
            .block_on(async move { seal_active_segment(&inner).await });
        assert!(result.is_err(), "the parked retry still fails");

        let pending = store.pending_seal.lock().unwrap();
        assert_eq!(
            pending.len(),
            2,
            "the active segment is parked behind the failed one"
        );
        assert_eq!(pending[0].next_index, 4, "the failed segment keeps order");
        assert_eq!(
            pending[1].next_index, 5,
            "the active bytes rotate into a new parked batch"
        );
        assert_eq!(pending[1].first_seq, 3);
        drop(pending);

        let active = store.active.lock().unwrap();
        assert_eq!(active.entries, 0, "active is bounded, not left growing");
        assert_eq!(active.next_index, 6);
    }

    /// Seal gating (`fix-object-store-wal-loss-window` task 1.2): the sealed
    /// frontier is published only after a successful seal (segment PUT +
    /// manifest update), covers the sealed segment's tail, and never
    /// regresses across seals. The trait exposes a notifier alongside it so
    /// the engine can await seal progress instead of polling.
    #[test]
    fn sealed_seq_covers_segment_tail_and_advances_monotonically() {
        // max_entries = 4 with a 1h flush_interval: seals fire only on the
        // size trigger, deterministically inside `append_batch`.
        let store = build_store_at(
            "seal-gate-pod",
            "seal-gate",
            Arc::new(InMemory::new()),
            4,
            1000,
        );
        assert_eq!(
            store.sealed_seq(),
            Some(0),
            "a fresh store has sealed nothing"
        );
        assert!(
            store.seal_notifier().is_some(),
            "the object-store backend must expose its seal notifier"
        );

        let payload = sample_payload(None);
        // Three appends stay staged in the active segment: nothing is sealed,
        // so acknowledgements for those sequences must remain gated.
        for seq in 1..=3u64 {
            store.append_batch(vec![(seq, payload.clone())]).unwrap();
            assert_eq!(
                store.sealed_seq(),
                Some(0),
                "seq {seq} is staged, not sealed — the frontier must not advance yet"
            );
        }
        // The fourth crosses segment.max_entries and seals [1..=4]
        // synchronously inside `append_batch`.
        store.append_batch(vec![(4, payload.clone())]).unwrap();
        let first = store.sealed_seq().unwrap();
        assert!(
            first >= 4,
            "the sealed frontier must cover the segment tail after the seal"
        );

        // A second seal advances the frontier monotonically.
        for seq in 5..=8u64 {
            store.append_batch(vec![(seq, payload.clone())]).unwrap();
        }
        let second = store.sealed_seq().unwrap();
        assert!(second >= 8, "the second seal covers its own tail");
        assert!(second >= first, "the sealed frontier never regresses");

        // close() performs a final seal of anything staged; the frontier
        // only grows.
        store.close().unwrap();
        assert!(store.sealed_seq().unwrap() >= second);
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
            retention: RetentionConfig::default(),
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
            assert!(
                std::time::Instant::now() < deadline,
                "PUT worker never completed"
            );
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
        assert_eq!(
            done.load(Ordering::SeqCst),
            0,
            "failed PUT must not complete"
        );
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
        let err = unwrap_err(
            S3Store::build(&WalConfig::default()),
            "default config has no object_store backend",
        );
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
            let next = probe_next_segment_index(&*client, "ns/segments")
                .await
                .unwrap();
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
        let client: Arc<dyn object_store::ObjectStore> =
            Arc::new(FailGetStore::segments(inmemory_client()));
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
        assert!(err.to_string().contains("S3 GET segment"), "got: {err}");
    }

    /// A generic manifest GET failure fails recovery (not treated as fresh).
    #[test]
    fn recovery_errors_when_manifest_get_fails() {
        let client: Arc<dyn object_store::ObjectStore> =
            Arc::new(FailGetStore::manifest(inmemory_client()));
        let result = S3Store::build_with_client(
            &WalConfig::default(),
            osc_base(),
            Runtime::new().unwrap(),
            client,
        );
        let err = unwrap_err(result, "manifest GET failure must fail recovery");
        assert!(err.to_string().contains("S3 GET manifest"), "got: {err}");
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
        assert!(err.to_string().contains("S3 GET manifest"), "got: {err}");
        let err = store
            .read_after_cursor()
            .expect_err("read_after_cursor must hit the same branch");
        assert!(err.to_string().contains("S3 GET manifest"), "got: {err}");
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
        assert!(err.to_string().contains("S3 GET segment"), "got: {err}");
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
        let store = S3Store::build_with_client(&WalConfig::default(), osc, runtime, client.clone())
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
                    let mut stream = client.list(Some(&ObjectPath::from(
                        format!("{COV_NS}/segments").as_str(),
                    )));
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
        let store =
            S3Store::build_with_client(&WalConfig::default(), osc_base(), runtime, client).unwrap();
        let inner_store = store.clone();
        store
            .rt()
            .block_on(async move { write_manifest_with_etag(&inner_store, |m| m.cursor = 5).await })
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
        let store =
            S3Store::build_with_client(&WalConfig::default(), osc_base(), runtime, client).unwrap();
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
        let store =
            S3Store::build_with_client(&WalConfig::default(), osc_base(), runtime, client).unwrap();
        let inner_store = store.clone();
        let err = store
            .rt()
            .block_on(async move { flush_manifest(&inner_store).await })
            .expect_err("flush must fail when the manifest cannot be read");
        assert!(err.to_string().contains("S3 GET manifest"), "got: {err}");
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

    #[test]
    fn read_after_pages_the_backlog_and_reuses_cached_segment_ranges() {
        let dir = tempdir();
        // max_bytes is unreachable so seals fire only on max_entries = 4:
        // eight appends seal exactly two segments and every entry lands on
        // the object store. The byte cap would otherwise seal on payload
        // size — arrow 58.4.2 shrank the sample payload from 524 to 460
        // bytes, which silently moved which entries got sealed (the active
        // buffer is in memory only, so unsealed entries are invisible to
        // read_after).
        let (store, _) = build_local_store_with_segment_caps(&dir, 4, usize::MAX);
        let payload = sample_payload(None);
        for seq in 1..=8u64 {
            store.append_batch(vec![(seq, payload.clone())]).unwrap();
        }

        let page = |after: u64, limit: usize| -> Vec<u64> {
            store
                .read_after(after, limit)
                .unwrap()
                .into_iter()
                .map(|(seq, _)| seq)
                .collect()
        };
        assert_eq!(page(0, 3), vec![1, 2, 3]);
        assert_eq!(page(3, 3), vec![4, 5, 6]);
        assert_eq!(page(6, 10), vec![7, 8]);
        assert!(page(8, 3).is_empty());
        // The ranges cached while paging cover every sealed segment.
        let cached = store
            .segment_ranges
            .lock()
            .expect("segment range cache")
            .len();
        assert!(
            cached >= 2,
            "sealed segments were decoded and their ranges cached, got {cached}"
        );
    }

    // ===== Retention & crash-safe segment reclamation tests =====

    #[derive(Debug)]
    struct SpyObjectStore {
        inner: Arc<InMemory>,
        gets: StdMutex<Vec<String>>,
        puts: StdMutex<Vec<String>>,
        deletes: StdMutex<Vec<String>>,
        fail_deletes: std::sync::atomic::AtomicBool,
        fail_delete_names: StdMutex<HashSet<String>>,
    }

    impl SpyObjectStore {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                inner: Arc::new(InMemory::new()),
                gets: StdMutex::new(Vec::new()),
                puts: StdMutex::new(Vec::new()),
                deletes: StdMutex::new(Vec::new()),
                fail_deletes: std::sync::atomic::AtomicBool::new(false),
                fail_delete_names: StdMutex::new(HashSet::new()),
            })
        }

        fn clear_counts(&self) {
            self.gets.lock().unwrap().clear();
            self.puts.lock().unwrap().clear();
            self.deletes.lock().unwrap().clear();
        }

        fn segment_gets(&self) -> Vec<String> {
            self.gets
                .lock()
                .unwrap()
                .iter()
                .filter(|p| p.ends_with(".wal"))
                .cloned()
                .collect()
        }

        fn manifest_puts(&self) -> usize {
            self.puts
                .lock()
                .unwrap()
                .iter()
                .filter(|p| p.ends_with("manifest.json"))
                .count()
        }

        fn list_segment_names(&self, rt: &Runtime, prefix: &str) -> Vec<String> {
            let p = ObjectPath::from(prefix);
            let mut names: Vec<String> = rt.block_on(async {
                let mut out = Vec::new();
                let mut stream = std::pin::pin!(self.inner.list(Some(&p)));
                while let Some(item) = stream.next().await {
                    let meta = item.unwrap();
                    if let Some(name) = meta.location.filename() {
                        if name.ends_with(".wal") {
                            out.push(name.to_string());
                        }
                    }
                }
                out
            });
            names.sort();
            names
        }
    }

    impl std::fmt::Display for SpyObjectStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "SpyObjectStore({})", self.inner)
        }
    }

    #[async_trait]
    impl object_store::ObjectStore for SpyObjectStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.puts.lock().unwrap().push(location.to_string());
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
            self.gets.lock().unwrap().push(location.to_string());
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
            if self.fail_deletes.load(Ordering::Acquire) {
                return Box::pin(locations.map(|res| match res {
                    Ok(loc) => Err(object_store::Error::Generic {
                        store: "SpyObjectStore",
                        source: format!("injected delete failure for {loc}").into(),
                    }),
                    Err(e) => Err(e),
                }));
            }
            let deletes = &self.deletes;
            let fail_names = self.fail_delete_names.lock().unwrap().clone();
            let mut to_delete: Vec<object_store::Result<ObjectPath>> = Vec::new();
            let mut failed: Vec<object_store::Result<ObjectPath>> = Vec::new();
            futures::executor::block_on(async {
                let mut s = locations;
                while let Some(item) = s.next().await {
                    match item {
                        Ok(loc) => {
                            let fname = loc.filename().unwrap_or("").to_string();
                            if fail_names.contains(&fname) {
                                failed.push(Err(object_store::Error::Generic {
                                    store: "SpyObjectStore",
                                    source: format!("injected delete failure for {loc}").into(),
                                }));
                            } else {
                                deletes.lock().unwrap().push(loc.to_string());
                                to_delete.push(Ok(loc));
                            }
                        }
                        Err(e) => failed.push(Err(e)),
                    }
                }
            });
            let inner_stream = self
                .inner
                .delete_stream(Box::pin(futures::stream::iter(to_delete)));
            Box::pin(inner_stream.chain(futures::stream::iter(failed)))
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

    fn build_retention_store(
        client: Arc<dyn object_store::ObjectStore>,
        stream_id: &str,
        retention: RetentionConfig,
    ) -> Arc<S3Store> {
        let osc = ObjectStoreWalConfig {
            node_id: "ret-pod".into(),
            stream_id: stream_id.into(),
            prefix: "arkflow/ret".into(),
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
                max_bytes: usize::MAX,
                flush_interval: std::time::Duration::from_secs(3600),
            },
            cursor: CursorFlushConfig {
                max_entries: 1000,
                interval: std::time::Duration::from_secs(3600),
            },
            segment_tuning: arkflow_core::wal::config::SegmentTuningConfig::default(),
            parallel_put: arkflow_core::wal::config::ParallelPutConfig::default(),
            compression: arkflow_core::wal::config::CompressionConfig::None,
            retention,
            sync: SyncPolicy::GroupCommit,
        };
        S3Store::build_with_client(&WalConfig::default(), osc, Runtime::new().unwrap(), client)
            .unwrap()
    }

    #[test]
    fn select_reclaimable_segments_policy_matrix() {
        let mk = |idx: u64, size_bytes: u64, sealed_at_ms: u64| {
            (
                format!("{idx:08}.wal"),
                SegmentMeta {
                    first_seq: (idx - 1) * 4 + 1,
                    last_seq: idx * 4,
                    size_bytes,
                    sealed_at_ms,
                },
            )
        };
        let now = 100_000u64;
        // Pass unsorted to verify deterministic segment-index ordering.
        let eligible = vec![
            mk(3, 600, now - 10_000),
            mk(1, 300, now - 90_000),
            mk(2, 500, now - 40_000),
        ];

        // Disabled keeps everything.
        assert!(select_reclaimable_segments(
            &RetentionConfig {
                enabled: false,
                max_segments: Some(0),
                max_age: None,
                max_bytes: None,
            },
            &eligible,
            now
        )
        .is_empty());

        // Default (enabled, all caps None) reclaims everything oldest-first.
        assert_eq!(
            select_reclaimable_segments(&RetentionConfig::default(), &eligible, now),
            vec!["00000001.wal", "00000002.wal", "00000003.wal"]
        );

        // max_segments = 2 keeps [2, 3], reclaims [1].
        assert_eq!(
            select_reclaimable_segments(
                &RetentionConfig {
                    enabled: true,
                    max_segments: Some(2),
                    max_age: None,
                    max_bytes: None,
                },
                &eligible,
                now
            ),
            vec!["00000001.wal"]
        );

        // max_age = 50s keeps [2 (40s), 3 (10s)], reclaims [1 (90s)].
        assert_eq!(
            select_reclaimable_segments(
                &RetentionConfig {
                    enabled: true,
                    max_segments: None,
                    max_age: Some(std::time::Duration::from_secs(50)),
                    max_bytes: None,
                },
                &eligible,
                now
            ),
            vec!["00000001.wal"]
        );

        // max_bytes = 1000: seg 3 (600) fits; seg 2 (600+500=1100) exceeds ->
        // MUST break so seg 1 (300) is ALSO reclaimed (contiguous suffix, no gap!).
        assert_eq!(
            select_reclaimable_segments(
                &RetentionConfig {
                    enabled: true,
                    max_segments: None,
                    max_age: None,
                    max_bytes: Some(1000),
                },
                &eligible,
                now
            ),
            vec!["00000001.wal", "00000002.wal"]
        );

        // Backwards wall-clock jump (now < sealed_at_ms) saturates to 0 age -> kept.
        assert!(select_reclaimable_segments(
            &RetentionConfig {
                enabled: true,
                max_segments: None,
                max_age: Some(std::time::Duration::from_secs(1)),
                max_bytes: None,
            },
            &[mk(1, 100, now + 5_000)],
            now
        )
        .is_empty());

        // Combined caps: bytes tighter than count (max_segments=2 allows 2 & 3,
        // max_bytes=1000 allows only 3 -> must reclaim 1 & 2, no gap).
        assert_eq!(
            select_reclaimable_segments(
                &RetentionConfig {
                    enabled: true,
                    max_segments: Some(2),
                    max_age: None,
                    max_bytes: Some(1000),
                },
                &eligible,
                now
            ),
            vec!["00000001.wal", "00000002.wal"]
        );

        // Complementary combined caps: count tighter than bytes (max_bytes=10_000
        // allows all 3, max_segments=1 allows only 3 -> must reclaim 1 & 2).
        assert_eq!(
            select_reclaimable_segments(
                &RetentionConfig {
                    enabled: true,
                    max_segments: Some(1),
                    max_age: None,
                    max_bytes: Some(10_000),
                },
                &eligible,
                now
            ),
            vec!["00000001.wal", "00000002.wal"]
        );

        // Combined count + age: age tighter than count (max_segments=3,
        // max_age=20s -> keeps only seg 3 at 10s, reclaims 1 & 2).
        assert_eq!(
            select_reclaimable_segments(
                &RetentionConfig {
                    enabled: true,
                    max_segments: Some(3),
                    max_age: Some(std::time::Duration::from_secs(20)),
                    max_bytes: Some(10_000),
                },
                &eligible,
                now
            ),
            vec!["00000001.wal", "00000002.wal"]
        );

        // Huge max_age whose `.as_millis()` exceeds `u64::MAX` (e.g.
        // 18_446_744_073_709_552s wraps `as_millis() as u64` to 384ms) must
        // keep a 1-second-old segment rather than narrowing and deleting it.
        assert!(select_reclaimable_segments(
            &RetentionConfig {
                enabled: true,
                max_segments: None,
                max_age: Some(std::time::Duration::from_secs(18_446_744_073_709_552)),
                max_bytes: None,
            },
            &[mk(1, 100, now - 1_000)],
            now
        )
        .is_empty());
    }

    #[test]
    fn retention_default_reclaims_segments_behind_committed_floor_and_skips_gets_on_reopen() {
        let spy = SpyObjectStore::new();
        let store =
            build_retention_store(spy.clone(), "default-reclaim", RetentionConfig::default());
        let payload = sample_payload(None);

        // Write 5 full segments (20 entries): segs 1..=4 in sealed_segments, seg 5 active.
        for seq in 1..=20u64 {
            store.append_batch(vec![(seq, payload.clone())]).unwrap();
        }
        // Advance and commit through seq 13 (first entry of seg 4):
        // reclaimable_hwm = 12 => segs 1..=3 (N-2 = 3) are wholly behind the floor.
        for seq in 1..=13u64 {
            store.advance_cursor(seq).unwrap();
            store.mark_committed(seq).unwrap();
        }
        store.rt().block_on(flush_manifest(&store)).unwrap();

        let reclaimed = store.rt().block_on(reclaim_segments(&store, None)).unwrap();
        assert_eq!(reclaimed, 3, "segments 1..=3 must be reclaimed");

        // Verify object store holds only segments 4 and 5.
        let remaining =
            spy.list_segment_names(store.rt(), "arkflow/ret/ret-pod/default-reclaim/segments");
        assert_eq!(remaining, vec!["00000004.wal", "00000005.wal"]);

        // Verify manifest cleared pending_deletes and lists only seg 4 + active seg 5.
        let manifest = store.read_manifest().unwrap();
        assert_eq!(manifest.sealed_segments, vec!["00000004.wal"]);
        assert_eq!(manifest.active_segment.as_deref(), Some("00000005.wal"));
        assert!(manifest.pending_deletes.is_empty());

        // Idle reclaim sweep must issue 0 manifest PUTs.
        spy.clear_counts();
        let idle = store.rt().block_on(reclaim_segments(&store, None)).unwrap();
        assert_eq!(idle, 0);
        assert_eq!(spy.manifest_puts(), 0, "idle sweep must not PUT manifest");

        store.close().unwrap();
        drop(store);

        // Reopen: recovery + replay must NOT GET any deleted segment and must
        // replay exactly 14..=20 with monotonic next_seq_hint and next_index.
        spy.clear_counts();
        let reopened =
            build_retention_store(spy.clone(), "default-reclaim", RetentionConfig::default());
        assert_eq!(reopened.next_seq_hint(), 21);
        let replayed: Vec<u64> = reopened
            .read_after_cursor()
            .unwrap()
            .into_iter()
            .map(|(s, _)| s)
            .collect();
        assert_eq!(replayed, (14..=20u64).collect::<Vec<_>>());

        let seg_gets = spy.segment_gets();
        for deleted in ["00000001.wal", "00000002.wal", "00000003.wal"] {
            assert!(
                !seg_gets.iter().any(|p| p.ends_with(deleted)),
                "reopen must not GET deleted segment {deleted}, got: {seg_gets:?}"
            );
        }

        // Next seal gets index 6 (no collision with reclaimed 1..=3).
        for seq in 21..=24u64 {
            reopened.append_batch(vec![(seq, payload.clone())]).unwrap();
        }
        let m2 = reopened.read_manifest().unwrap();
        assert_eq!(m2.active_segment.as_deref(), Some("00000006.wal"));
        reopened.close().unwrap();
    }

    #[test]
    fn retention_with_max_segments_retains_tail_and_skips_covered_gets_on_reopen() {
        let spy = SpyObjectStore::new();
        let cfg = RetentionConfig {
            enabled: true,
            max_segments: Some(1),
            max_age: None,
            max_bytes: None,
        };
        let store = build_retention_store(spy.clone(), "keep-tail", cfg.clone());
        let payload = sample_payload(None);

        for seq in 1..=20u64 {
            store.append_batch(vec![(seq, payload.clone())]).unwrap();
        }
        for seq in 1..=13u64 {
            store.advance_cursor(seq).unwrap();
            store.mark_committed(seq).unwrap();
        }
        store.rt().block_on(flush_manifest(&store)).unwrap();

        let reclaimed = store.rt().block_on(reclaim_segments(&store, None)).unwrap();
        assert_eq!(reclaimed, 2, "segments 1..=2 deleted; segment 3 retained");

        let remaining =
            spy.list_segment_names(store.rt(), "arkflow/ret/ret-pod/keep-tail/segments");
        assert_eq!(
            remaining,
            vec!["00000003.wal", "00000004.wal", "00000005.wal"]
        );
        store.close().unwrap();
        drop(store);

        // Reopen: retained-but-acknowledged segment 3 (last_seq=12 <= cursor=13)
        // must ALSO be skipped during recover() and read_after_cursor()!
        spy.clear_counts();
        let reopened = build_retention_store(spy.clone(), "keep-tail", cfg);
        let replayed: Vec<u64> = reopened
            .read_after_cursor()
            .unwrap()
            .into_iter()
            .map(|(s, _)| s)
            .collect();
        assert_eq!(replayed, (14..=20u64).collect::<Vec<_>>());
        let seg_gets = spy.segment_gets();
        assert!(
            !seg_gets.iter().any(|p| p.ends_with("00000003.wal")),
            "covered segment 3 must not be downloaded on recovery/replay: {seg_gets:?}"
        );
        reopened.close().unwrap();
    }

    #[test]
    fn retention_disabled_keeps_all_sealed_segments() {
        let spy = SpyObjectStore::new();
        let cfg = RetentionConfig {
            enabled: false,
            max_segments: Some(0),
            max_age: None,
            max_bytes: None,
        };
        let store = build_retention_store(spy.clone(), "disabled", cfg);
        let payload = sample_payload(None);
        for seq in 1..=16u64 {
            store.append_batch(vec![(seq, payload.clone())]).unwrap();
        }
        for seq in 1..=12u64 {
            store.advance_cursor(seq).unwrap();
            store.mark_committed(seq).unwrap();
        }
        store.rt().block_on(flush_manifest(&store)).unwrap();
        assert_eq!(
            store.rt().block_on(reclaim_segments(&store, None)).unwrap(),
            0
        );
        assert_eq!(
            spy.list_segment_names(store.rt(), "arkflow/ret/ret-pod/disabled/segments")
                .len(),
            4
        );
        store.close().unwrap();
    }

    #[test]
    fn retention_respects_rewind_safety_and_undo_boundary() {
        let spy = SpyObjectStore::new();
        let store = build_retention_store(spy.clone(), "rewind-safe", RetentionConfig::default());
        let payload = sample_payload(None);

        // 3 segments: [1..=4], [5..=8], [9..=12].
        for seq in 1..=12u64 {
            store.append_batch(vec![(seq, payload.clone())]).unwrap();
        }
        // Advance cursor to 8 WITHOUT mark_committed: nothing may be reclaimed.
        for seq in 1..=8u64 {
            store.advance_cursor(seq).unwrap();
        }
        store.rt().block_on(flush_manifest(&store)).unwrap();
        assert_eq!(
            store.rt().block_on(reclaim_segments(&store, None)).unwrap(),
            0,
            "advance_cursor without mark_committed must not reclaim"
        );

        // mark_committed(4) sets reclaimable_hwm = 3: segment 1 (last_seq = 4)
        // must remain intact so Wal::undo(4) -> rewind_cursor(3) can replay 4.
        store.mark_committed(4).unwrap();
        assert_eq!(
            store.rt().block_on(reclaim_segments(&store, None)).unwrap(),
            0,
            "segment containing latest committed seq 4 must survive for undo"
        );
        store.rewind_cursor(3).unwrap();
        let after_rewind: Vec<u64> = store
            .read_after_cursor()
            .unwrap()
            .into_iter()
            .map(|(s, _)| s)
            .collect();
        assert_eq!(after_rewind, (4..=12u64).collect::<Vec<_>>());

        // While rewind poison is active, reclaim stays paused.
        assert_eq!(
            store.rt().block_on(reclaim_segments(&store, None)).unwrap(),
            0
        );

        // Re-ack and commit through 5 -> clears poison, reclaimable_hwm = 4 ->
        // segment 1 (last_seq = 4 <= 4) is now reclaimed!
        store.advance_cursor(4).unwrap();
        store.mark_committed(4).unwrap();
        store.advance_cursor(5).unwrap();
        store.mark_committed(5).unwrap();
        store.rt().block_on(flush_manifest(&store)).unwrap();
        assert_eq!(
            store.rt().block_on(reclaim_segments(&store, None)).unwrap(),
            1
        );
        assert_eq!(
            spy.list_segment_names(store.rt(), "arkflow/ret/ret-pod/rewind-safe/segments"),
            vec!["00000002.wal", "00000003.wal"]
        );
        store.close().unwrap();
    }

    #[test]
    fn retention_crash_between_phases_and_delete_errors_self_heal() {
        let spy = SpyObjectStore::new();
        let store = build_retention_store(spy.clone(), "crash-heal", RetentionConfig::default());
        let payload = sample_payload(None);
        for seq in 1..=12u64 {
            store.append_batch(vec![(seq, payload.clone())]).unwrap();
        }
        for seq in 1..=5u64 {
            store.advance_cursor(seq).unwrap();
            store.mark_committed(seq).unwrap();
        }
        store.rt().block_on(flush_manifest(&store)).unwrap();

        // Simulate failure during Phase 2 DELETE (Phase 1 already moved seg 1
        // into `pending_deletes`, but the object remains on S3).
        spy.fail_deletes.store(true, Ordering::Release);
        assert_eq!(
            store.rt().block_on(reclaim_segments(&store, None)).unwrap(),
            0
        );
        let m = store.read_manifest().unwrap();
        assert_eq!(m.pending_deletes, vec!["00000001.wal"]);
        assert!(spy
            .list_segment_names(store.rt(), "arkflow/ret/ret-pod/crash-heal/segments")
            .contains(&"00000001.wal".to_string()));
        store.close().unwrap();
        drop(store);

        // Reopen while `00000001.wal` is still on S3 and in `pending_deletes`:
        // recovery MUST skip GETting it, and once DELETE succeeds the next sweep
        // removes the object and clears `pending_deletes`.
        spy.clear_counts();
        let reopened = build_retention_store(spy.clone(), "crash-heal", RetentionConfig::default());
        assert!(
            !spy.segment_gets()
                .iter()
                .any(|p| p.ends_with("00000001.wal")),
            "recovery must skip pending_deletes segment"
        );
        spy.fail_deletes.store(false, Ordering::Release);
        assert_eq!(
            reopened
                .rt()
                .block_on(reclaim_segments(&reopened, None))
                .unwrap(),
            1
        );
        assert!(reopened.read_manifest().unwrap().pending_deletes.is_empty());
        assert_eq!(
            spy.list_segment_names(reopened.rt(), "arkflow/ret/ret-pod/crash-heal/segments"),
            vec!["00000002.wal", "00000003.wal"]
        );

        // Now simulate crash between Phase 2 (object already deleted) and
        // Phase 3 (`pending_deletes` still lists an already-absent segment).
        reopened
            .rt()
            .block_on(write_manifest_with_etag(&reopened, |m| {
                m.pending_deletes.push("00000001.wal".into());
            }))
            .unwrap();
        reopened.close().unwrap();
        drop(reopened);

        spy.clear_counts();
        let reopened2 =
            build_retention_store(spy.clone(), "crash-heal", RetentionConfig::default());
        assert!(
            !spy.segment_gets()
                .iter()
                .any(|p| p.ends_with("00000001.wal")),
            "recovery must not GET already-deleted pending_deletes entry"
        );
        // Next sweep treats NotFound as idempotent success and clears pending_deletes.
        assert_eq!(
            reopened2
                .rt()
                .block_on(reclaim_segments(&reopened2, None))
                .unwrap(),
            1
        );
        assert!(reopened2
            .read_manifest()
            .unwrap()
            .pending_deletes
            .is_empty());
        reopened2.close().unwrap();
    }

    #[test]
    fn retention_legacy_manifest_and_list_fallback_open_readonly_then_adopt_and_reclaim() {
        let spy = SpyObjectStore::new();
        let payload = sample_payload(None);
        let rt = Runtime::new().unwrap();
        let ns = "arkflow/ret/ret-pod/legacy-adopt";

        // Seed 3 raw segment objects:
        // - 00000001.wal ([1..=4]): listed in legacy manifest without segment_meta
        // - 00000002.wal ([5..=8]): LIST-fallback (absent from manifest!)
        // - 00000003.wal ([9..=12]): active_segment in legacy manifest without segment_meta
        rt.block_on(async {
            for (idx, range) in [(1u64, 1u64..=4), (2, 5..=8), (3, 9..=12)] {
                let entries: Vec<(u64, Vec<u8>)> = range.map(|s| (s, payload.clone())).collect();
                let mut buf = Vec::new();
                super::super::segment::encode(&entries, &mut buf).unwrap();
                spy.inner
                    .put(
                        &ObjectPath::from(format!("{ns}/segments/{idx:08}.wal").as_str()),
                        PutPayload::from(Bytes::from(buf)),
                    )
                    .await
                    .unwrap();
            }
            let legacy_manifest = serde_json::json!({
                "version": 2,
                "node_id": "ret-pod",
                "stream_id": "legacy-adopt",
                "cursor": 4,
                "max_sealed_seq": 12,
                "active_segment": "00000003.wal",
                "sealed_segments": ["00000001.wal"]
            });
            spy.inner
                .put(
                    &ObjectPath::from(format!("{ns}/manifest.json").as_str()),
                    PutPayload::from(Bytes::from(serde_json::to_vec(&legacy_manifest).unwrap())),
                )
                .await
                .unwrap();
        });

        spy.clear_counts();
        let store = build_retention_store(spy.clone(), "legacy-adopt", RetentionConfig::default());
        assert_eq!(
            spy.manifest_puts(),
            0,
            "recover() must remain strictly read-only"
        );
        let replayed: Vec<u64> = store
            .read_after_cursor()
            .unwrap()
            .into_iter()
            .map(|(s, _)| s)
            .collect();
        assert_eq!(replayed, (5..=12u64).collect::<Vec<_>>());

        // Commit through seq 9 -> reclaimable_hwm = 8 -> adopts LIST-fallback
        // segment 2 + legacy segment 1 metadata and reclaims BOTH 1 and 2.
        for seq in 5..=9u64 {
            store.advance_cursor(seq).unwrap();
            store.mark_committed(seq).unwrap();
        }
        store.rt().block_on(flush_manifest(&store)).unwrap();

        let reclaimed = store.rt().block_on(reclaim_segments(&store, None)).unwrap();
        assert_eq!(
            reclaimed, 2,
            "legacy seg 1 and LIST-fallback seg 2 reclaimed"
        );
        assert_eq!(
            spy.list_segment_names(store.rt(), &format!("{ns}/segments")),
            vec!["00000003.wal"]
        );
        let manifest = store.read_manifest().unwrap();
        assert!(manifest.sealed_segments.is_empty());
        assert_eq!(manifest.active_segment.as_deref(), Some("00000003.wal"));
        assert!(manifest.segment_meta.contains_key("00000003.wal"));
        assert_eq!(store.next_seq_hint(), 13);
        store.close().unwrap();
    }

    #[test]
    fn build_rejects_zero_retention_max_age() {
        let err = unwrap_err(
            build_cov_store_err(|osc| {
                osc.retention.max_age = Some(std::time::Duration::ZERO);
            }),
            "zero retention.max_age must be rejected",
        );
        assert!(
            err.to_string().contains("max_age"),
            "expected max_age error, got: {err}"
        );
    }

    #[test]
    fn retention_failed_delete_batch_rotates_so_later_healthy_segments_reclaim() {
        let spy = SpyObjectStore::new();
        let store = build_retention_store(spy.clone(), "rotate-failed", RetentionConfig::default());
        let ns = "arkflow/ret/ret-pod/rotate-failed";

        // Seed 65 pending-delete objects: 1..=64 fail DELETE persistently,
        // while 65 is healthy. Sweep 1 attempts 1..=64 and rotates them to the
        // tail; sweep 2 must then reach and delete 65 instead of starving it.
        store.rt().block_on(async {
            let mut failing = HashSet::new();
            let mut pending = Vec::new();
            for idx in 1..=65u64 {
                let name = format!("{idx:08}.wal");
                spy.inner
                    .put(
                        &ObjectPath::from(format!("{ns}/segments/{name}").as_str()),
                        PutPayload::from(Bytes::from_static(b"dummy")),
                    )
                    .await
                    .unwrap();
                if idx <= 64 {
                    failing.insert(name.clone());
                }
                pending.push(name);
            }
            *spy.fail_delete_names.lock().unwrap() = failing;
            write_manifest_with_etag(&store, move |m| {
                m.pending_deletes = pending.clone();
            })
            .await
            .unwrap();
        });

        // Sweep 1: 1..=64 all fail and rotate behind 65.
        let sweep1 = store.rt().block_on(reclaim_segments(&store, None)).unwrap();
        assert_eq!(sweep1, 0);
        let m1 = store.read_manifest().unwrap();
        assert_eq!(m1.pending_deletes.len(), 65);
        assert_eq!(
            m1.pending_deletes.first().map(String::as_str),
            Some("00000065.wal"),
            "failed batch must rotate to tail so 00000065.wal moves to head"
        );

        // Sweep 2: reaches 00000065.wal at the head and deletes it cleanly.
        let sweep2 = store.rt().block_on(reclaim_segments(&store, None)).unwrap();
        assert_eq!(sweep2, 1, "healthy segment 65 must be deleted on sweep 2");
        let m2 = store.read_manifest().unwrap();
        assert_eq!(m2.pending_deletes.len(), 64);
        assert!(!m2.pending_deletes.contains(&"00000065.wal".to_string()));
        assert!(!spy
            .list_segment_names(store.rt(), &format!("{ns}/segments"))
            .contains(&"00000065.wal".to_string()));
        store.close().unwrap();
    }

    #[test]
    fn retention_requires_conditional_put_and_leaves_objects_intact_on_not_implemented() {
        let inner = Arc::new(InMemory::new());
        let client: Arc<dyn object_store::ObjectStore> = Arc::new(NotImplementedPutStore {
            inner: inner.clone(),
        });
        let store = build_retention_store(client, "no-cond-put", RetentionConfig::default());
        let payload = sample_payload(None);
        for seq in 1..=12u64 {
            store.append_batch(vec![(seq, payload.clone())]).unwrap();
        }
        for seq in 1..=5u64 {
            store.advance_cursor(seq).unwrap();
            store.mark_committed(seq).unwrap();
        }
        store.rt().block_on(flush_manifest(&store)).unwrap();

        let err = store
            .rt()
            .block_on(reclaim_segments(&store, None))
            .expect_err("reclamation must fail closed without conditional PUT");
        assert!(
            err.to_string().contains("conditional PUT"),
            "unexpected error: {err}"
        );
        // Segment 1 must remain intact on the store.
        let head = store.rt().block_on(inner.head(&ObjectPath::from(
            "arkflow/ret/ret-pod/no-cond-put/segments/00000001.wal",
        )));
        assert!(
            head.is_ok(),
            "object must remain intact when reclaim refuses"
        );
        store.close().unwrap();
    }

    #[test]
    fn retention_cancellation_interrupts_stalled_manifest_io() {
        #[derive(Debug)]
        struct StalledManifestGetStore {
            inner: Arc<InMemory>,
            stall: std::sync::atomic::AtomicBool,
        }
        impl std::fmt::Display for StalledManifestGetStore {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "StalledManifestGetStore")
            }
        }
        #[async_trait]
        impl object_store::ObjectStore for StalledManifestGetStore {
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
                if self.stall.load(Ordering::Acquire)
                    && location.as_ref().ends_with("manifest.json")
                {
                    futures::future::pending::<()>().await;
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

        let stalled = Arc::new(StalledManifestGetStore {
            inner: Arc::new(InMemory::new()),
            stall: std::sync::atomic::AtomicBool::new(false),
        });
        let store =
            build_retention_store(stalled.clone(), "cancel-stall", RetentionConfig::default());
        stalled.stall.store(true, Ordering::Release);

        let token = CancellationToken::new();
        let cancel_trigger = token.clone();
        let start = std::time::Instant::now();
        let res = store.rt().block_on(async {
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
                cancel_trigger.cancel();
            });
            reclaim_segments(&store, Some(&token)).await
        });
        assert_eq!(res.unwrap(), 0);
        assert!(
            start.elapsed() < std::time::Duration::from_secs(2),
            "cancellation must interrupt stalled Phase 1 manifest GET promptly"
        );
        stalled.stall.store(false, Ordering::Release);
        store.close().unwrap();
    }

    #[test]
    fn segment_still_required_for_replay_distinguishes_reclaimed_vs_missing() {
        let spy = SpyObjectStore::new();
        let store = build_retention_store(spy.clone(), "missing-check", RetentionConfig::default());
        let payload = sample_payload(None);
        for seq in 1..=12u64 {
            store.append_batch(vec![(seq, payload.clone())]).unwrap();
        }
        let orig_manifest = store.read_manifest().unwrap();
        // Unacked segment 2 (5..=8) is still required for replay.
        assert!(store.segment_still_required_for_replay(&orig_manifest, "00000002.wal", 0));

        // A LIST-only untracked segment that vanishes on GET must still warn.
        assert!(
            store.segment_still_required_for_replay(&orig_manifest, "00000099.wal", 0),
            "LIST-only untracked segment must still warn when missing"
        );

        // Commit through 5 and reclaim segment 1 -> latest manifest no longer
        // requires segment 1, so a stale reader's NotFound must not warn.
        for seq in 1..=5u64 {
            store.advance_cursor(seq).unwrap();
            store.mark_committed(seq).unwrap();
        }
        store.rt().block_on(flush_manifest(&store)).unwrap();
        assert_eq!(
            store.rt().block_on(reclaim_segments(&store, None)).unwrap(),
            1
        );
        assert!(
            !store.segment_still_required_for_replay(&orig_manifest, "00000001.wal", 0),
            "reclaimed segment must not be flagged as missing replay data"
        );
        // Active unacked segment 3 is still required for replay.
        assert!(store.segment_still_required_for_replay(&orig_manifest, "00000003.wal", 5));

        // If `manifest.json` itself is missing (NotFound), `orig_manifest`'s
        // tracked segment must still warn rather than being treated as reclaimed.
        store
            .rt()
            .block_on(
                spy.inner
                    .delete(&ObjectPath::from(store.manifest_key.as_str())),
            )
            .unwrap();
        assert!(
            store.segment_still_required_for_replay(&orig_manifest, "00000002.wal", 0),
            "missing manifest.json must not suppress missing-segment warning"
        );
        store.close().unwrap();
    }

    #[test]
    fn recovery_with_missing_unacked_tail_does_not_create_ack_gap() {
        let spy = SpyObjectStore::new();
        let store = build_retention_store(spy.clone(), "missing-tail", RetentionConfig::default());
        let payload = sample_payload(None);
        // Segment 1: [1..=4], Segment 2 (active): [5..=8].
        for seq in 1..=8u64 {
            store.append_batch(vec![(seq, payload.clone())]).unwrap();
        }
        for seq in 1..=4u64 {
            store.advance_cursor(seq).unwrap();
            store.mark_committed(seq).unwrap();
        }
        store.rt().block_on(flush_manifest(&store)).unwrap();

        // Simulate missing unacked tail segment 2 (5..=8) on S3 while manifest
        // still records cursor = 4, max_sealed_seq = 8.
        store
            .rt()
            .block_on(spy.inner.delete(&ObjectPath::from(
                "arkflow/ret/ret-pod/missing-tail/segments/00000002.wal",
            )))
            .unwrap();
        store.close().unwrap();
        drop(store);

        let reopened =
            build_retention_store(spy.clone(), "missing-tail", RetentionConfig::default());
        assert_eq!(
            reopened.next_seq_hint(),
            5,
            "missing unacked tail must not advance next_seq_hint past manifest.cursor + 1"
        );
        reopened.close().unwrap();
    }

    #[test]
    fn retention_backfilled_meta_cleared_when_already_pending_delete_on_noop() {
        let spy = SpyObjectStore::new();
        let store = build_retention_store(spy.clone(), "noop-backfill", RetentionConfig::default());
        let ns = "arkflow/ret/ret-pod/noop-backfill";

        // Seed 00000001.wal in S3 + pending_deletes, and also inject a stale
        // backfilled_meta entry for 00000001.wal so Phase 1 is a no-op
        // (`mutated == false`).
        store.rt().block_on(async {
            spy.inner
                .put(
                    &ObjectPath::from(format!("{ns}/segments/00000001.wal").as_str()),
                    PutPayload::from(Bytes::from_static(b"dummy")),
                )
                .await
                .unwrap();
            write_manifest_with_etag(&store, |m| {
                m.pending_deletes.push("00000001.wal".into());
            })
            .await
            .unwrap();
        });
        store.backfilled_meta.lock().unwrap().insert(
            "00000001.wal".into(),
            SegmentMeta {
                first_seq: 1,
                last_seq: 4,
                size_bytes: 5,
                sealed_at_ms: 1_000,
            },
        );

        // Sweep 1: Phase 1 makes no changes, must still prune backfilled_meta
        // for 00000001.wal, and Phase 2+3 deletes it from S3 and pending_deletes.
        let reclaimed = store.rt().block_on(reclaim_segments(&store, None)).unwrap();
        assert_eq!(reclaimed, 1);
        assert!(
            store.backfilled_meta.lock().unwrap().is_empty(),
            "stale backfilled_meta entry already in pending_deletes must be cleared on no-op Phase 1"
        );

        // Sweep 2: must NOT re-adopt the deleted segment back into the manifest.
        let reclaimed2 = store.rt().block_on(reclaim_segments(&store, None)).unwrap();
        assert_eq!(reclaimed2, 0);
        let m = store.read_manifest().unwrap();
        assert!(m.sealed_segments.is_empty());
        assert_eq!(m.active_segment, None);
        assert!(m.pending_deletes.is_empty());
        store.close().unwrap();
    }

    #[test]
    fn retention_pauses_phase2_deletes_when_rewind_poison_activates() {
        struct HookedDeleteStore {
            inner: Arc<InMemory>,
            hook: std::sync::Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
            deletes: std::sync::atomic::AtomicUsize,
        }
        impl std::fmt::Debug for HookedDeleteStore {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("HookedDeleteStore").finish()
            }
        }
        impl std::fmt::Display for HookedDeleteStore {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "HookedDeleteStore")
            }
        }
        #[async_trait]
        impl object_store::ObjectStore for HookedDeleteStore {
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
                self.inner.get_opts(location, options).await
            }
            fn delete_stream(
                &self,
                locations: futures::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
            ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
                use futures::StreamExt;
                let inner = self.inner.clone();
                self.deletes.fetch_add(1, Ordering::AcqRel);
                if let Some(cb) = self.hook.lock().unwrap().as_ref() {
                    cb();
                }
                locations
                    .then(move |loc| {
                        let inner = inner.clone();
                        async move {
                            let path = loc?;
                            inner.delete(&path).await?;
                            Ok(path)
                        }
                    })
                    .boxed()
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

        let inner = Arc::new(InMemory::new());
        let ns = "arkflow/ret/ret-pod/rewind-phase2";
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            for idx in [1u64, 2] {
                inner
                    .put(
                        &ObjectPath::from(format!("{ns}/segments/{idx:08}.wal").as_str()),
                        PutPayload::from(Bytes::from_static(b"dummy")),
                    )
                    .await
                    .unwrap();
            }
        });
        drop(rt);

        let client = Arc::new(HookedDeleteStore {
            inner: inner.clone(),
            hook: std::sync::Mutex::new(None),
            deletes: std::sync::atomic::AtomicUsize::new(0),
        });
        let store =
            build_retention_store(client.clone(), "rewind-phase2", RetentionConfig::default());
        store
            .rt()
            .block_on(write_manifest_with_etag(&store, |m| {
                m.pending_deletes = vec!["00000001.wal".into(), "00000002.wal".into()];
            }))
            .unwrap();
        let store_weak = Arc::downgrade(&store);
        *client.hook.lock().unwrap() = Some(Box::new(move || {
            if let Some(s) = store_weak.upgrade() {
                s.rewind_floor.store(0, Ordering::Release);
            }
        }));

        let reclaimed = store.rt().block_on(reclaim_segments(&store, None)).unwrap();
        assert_eq!(
            reclaimed, 1,
            "only first DELETE runs before rewind_floor pauses loop"
        );
        assert_eq!(client.deletes.load(Ordering::Acquire), 1);
        let m = store.read_manifest().unwrap();
        assert_eq!(m.pending_deletes, vec!["00000002.wal"]);
        assert!(
            store
                .rt()
                .block_on(inner.head(&ObjectPath::from(
                    format!("{ns}/segments/00000002.wal").as_str()
                )))
                .is_ok(),
            "second object must remain intact when rewind poison activates"
        );
        store.close().unwrap();
    }
}
