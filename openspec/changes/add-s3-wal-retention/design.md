## Context

The S3 WAL backend (`crates/arkflow-plugin/src/wal/s3.rs`) stores immutable segment objects under `{prefix}/{node_id}/{stream_id}/segments/NNNNNNNN.wal` and coordinates state in `{prefix}/{node_id}/{stream_id}/manifest.json` using ETag-based optimistic concurrency (`write_manifest_with_etag`).

Today:
1. `Manifest` records only `sealed_segments: Vec<String>` and `active_segment: Option<String>` — no per-segment sequence bounds, byte sizes, or timestamps (`manifest.rs:65-76`).
2. `flush_manifest` intentionally does no truncation (`s3.rs:1395-1400`), so sealed segments accumulate indefinitely.
3. `S3Store` does not override `WalStore::mark_committed`, inheriting the no-op default (`store.rs:169-171`).
4. `recover()` (`s3.rs:766-808`) and `read_after_cursor()` (`s3.rs:950-971`) `GET` and decode every segment on the store before filtering `seq > manifest.cursor`.
5. `recover()` initializes `max_seq_seen = 0`, which would regress `next_seq_hint()` to `1` if all prior segments were deleted.

## Goals / Non-Goals

**Goals**
- Bound S3 WAL storage and manifest size via configurable retention (`enabled`, `max_segments`, `max_age`, `max_bytes`) with a safe, zero-config default.
- Preserve all `input-durability` invariants: never delete unacknowledged entries, never delete entries reachable by `rewind_cursor` / `Wal::undo`, never delay `seal_active_segment` or hang `close()`, and never regress sequence numbers or segment indices on restart.
- Guarantee crash safety across every step between manifest mutation and S3 `DELETE` without producing false-positive missing-segment warnings or permanent object orphans (including LIST-fallback segments).
- Keep `recover()` strictly read-only while skipping `GET`s for segments wholly behind the committed cursor or pending deletion, and apply the same skip optimization in `read_after_cursor()` / `read_after()`.

**Non-Goals**
- No changes to the local `redb` WAL backend.
- No bucket-level S3 lifecycle rule automation.
- No new Prometheus metrics in this change (structured `tracing` only).
- No change to `Manifest::version` (stays `2`) or `WalStore::retained_bytes()`.

## Decisions

### D1. Configuration shape and contiguous-suffix multi-limit semantics (`RetentionConfig`)

Add `#[serde(default)] pub retention: RetentionConfig` to `ObjectStoreWalConfig`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RetentionConfig {
    #[serde(default = "default_retention_enabled")]
    pub enabled: bool, // default: true
    #[serde(default)]
    pub max_segments: Option<usize>,
    #[serde(with = "duration_serde_option", default)]
    pub max_age: Option<Duration>,
    #[serde(default)]
    pub max_bytes: Option<u64>,
}
```

**Semantics for eligible sealed segments** (sorted oldest-to-newest by segment index):
- If `!enabled`: select nothing (reclamation disabled).
- If `enabled` and `max_segments.is_none() && max_age.is_none() && max_bytes.is_none()`: **default immediate reclaim** — select all eligible sealed segments (equivalent to `max_segments: Some(0)`).
- If one or more of `max_segments`, `max_age`, `max_bytes` is `Some(...)`: **contiguous newest-suffix retention under tightest bound (AND-to-keep)**:
  - Walk eligible segments from **newest to oldest** (`rev()`), maintaining `kept_count` and `kept_bytes`:
    - Check whether the segment satisfies all configured caps:
      1. `max_age.map_or(true, |d| now_ms.saturating_sub(meta.sealed_at_ms) <= d.as_millis() as u64)` (uses `saturating_sub` so backwards wall-clock jumps never cause premature deletion)
      2. `max_segments.map_or(true, |cap| kept_count < cap)`
      3. `max_bytes.map_or(true, |cap| kept_bytes.saturating_add(meta.size_bytes) <= cap)`
    - If all hold, keep it (`kept_count += 1; kept_bytes += meta.size_bytes`).
    - On the **first violation**, **`break`** — that segment and **every older** eligible segment are marked for reclamation. Breaking prevents non-contiguous history gaps (e.g., where a large newer segment exceeds `max_bytes` while a smaller older segment would still fit).
- **Duration parsing**: Reuse `humantime::parse_duration` (already a direct dependency of `arkflow-core`) inside `duration_serde` / `duration_serde_option` so `h`, `d`, and compound durations (`"1h30m"`) work naturally alongside existing bare-integer ms and fractional seconds (`"0.5s"`), and return a serde deserialization error on invalid non-empty strings instead of silently producing `None`.
- **Validation**: Reject `max_age == Some(Duration::ZERO)` in both `WalConfig::validate` and `S3Store::build_with_client_inner` (directing callers to `max_segments: 0` for immediate reclaim).

### D2. Rewind-safe reclaim floor (`mark_committed` parity with `LocalStore`)

`Wal::acknowledge` advances the WAL cursor (`advance_cursor(seq)`) *before* invoking the wrapped source commit (`inner.ack()`), and compensates via `rewind_cursor(seq - 1)` on failure. Furthermore, `Wal::undo(seq)` can rewind the latest committed sequence `seq` back to `seq - 1` even after `mark_committed(seq)` ran (`crates/arkflow-core/src/wal/mod.rs:1032-1048`).

To uphold the `input-durability` invariant (*"A WAL backend SHALL NOT delete an entry a rewind can still expose"*):
- Add `reclaimable_hwm: AtomicU64` to `S3Store`:
  - Seeded at open in `recover()` with `manifest.cursor` (no in-flight `PendingWalAck` or `Wal::undo` handles survive a process restart, so everything `<= manifest.cursor` from a prior lifetime is past any possible rewind).
  - Updated in `WalStore::mark_committed(&self, seq: u64)` via `self.reclaimable_hwm.fetch_max(seq.saturating_sub(1), Ordering::AcqRel)` — O(1), non-blocking, matching `LocalStore`'s `floor = seq.saturating_sub(1)`.
  - Clamped in `rewind_cursor(&self, seq: u64)` via `self.reclaimable_hwm.fetch_min(seq, Ordering::AcqRel)`.
- Effective reclaim floor inside a retention sweep:
  - If `self.rewind_floor.load(Ordering::Acquire) != u64::MAX` (rewind poison active), skip reclamation entirely until re-acknowledged.
  - Otherwise `reclaim_floor = manifest.cursor.min(self.reclaimable_hwm.load(Ordering::Acquire))`.
- **Exact boundary**: Committing sequence `S` advances `reclaimable_hwm` to `S - 1`, so every sealed segment with `last_seq <= S - 1` is eligible immediately, while the segment containing `S` itself stays protected until `S + 1` commits (or until restart, when `reclaimable_hwm` seeds from `manifest.cursor`).
- Only segments in `manifest.sealed_segments` (never `manifest.active_segment`) whose `segment_meta` reports `last_seq <= reclaim_floor` are eligible. Keeping `active_segment` un-reclaimed plus seeding `max_seq_seen = manifest.max_sealed_seq.max(manifest.cursor)` in `recover()` guarantees monotonic sequence allocation and collision-free filenames across restarts.

### D3. Manifest schema extension & read-only recovery backfill (backward-compatible v2)

Extend `Manifest` in `crates/arkflow-plugin/src/wal/manifest.rs` with two optional, default-empty fields (manifest `version` stays `2`):

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct SegmentMeta {
    pub first_seq: u64,
    pub last_seq: u64,
    pub size_bytes: u64,
    pub sealed_at_ms: u64,
}
```
On `Manifest`:
- `#[serde(default, skip_serializing_if = "BTreeMap::is_empty")] pub segment_meta: BTreeMap<String, SegmentMeta>`
- `#[serde(default, skip_serializing_if = "Vec::is_empty")] pub pending_deletes: Vec<String>`

Population:
- `publish_sealed_segment` records `SegmentMeta { first_seq, last_seq, size_bytes: object_bytes.len() as u64, sealed_at_ms: now_ms() }` in `m.segment_meta` inside the existing ETag-coordinated manifest write (zero extra S3 PUTs).
- **Read-only recovery with lazy adoption**: `recover()` MUST NOT write `manifest.json` (preserving read-only startup tolerance when writes are transiently unavailable and existing `manifest_after(1)` test contracts). Whenever `recover()` decodes a segment absent from `manifest.segment_meta` — whether a legacy manifest segment OR a LIST-fallback segment whose manifest PUT crashed — it records `SegmentMeta` (using `ObjectMeta::last_modified` for `sealed_at_ms` and `bytes.len()` for `size_bytes`) in an in-memory `backfilled_meta: StdMutex<BTreeMap<String, SegmentMeta>>` map on `S3Store`. The next reclaim sweep (Phase 1) atomically merges `backfilled_meta` into `m.segment_meta`, adopts any LIST-only segment via `apply_seal` + `max_sealed_seq`, and clears merged keys from `backfilled_meta` once the manifest PUT succeeds.

### D4. Non-blocking, cancellable, crash-safe two-phase reclamation

Reclamation MUST NOT run inline inside `spawn_flusher`'s seal loop (where slow S3 `DELETE` calls would stall `seal_active_segment` and trip `Wal::wait_for_sealed`'s `4 * flush_interval + 5s` timeout) and MUST NOT run synchronously inside `S3Store::close()` (where deleting a backlog would stall graceful shutdown).

Instead, `spawn_flusher` runs **two concurrent tasks** on its tokio runtime sharing `stop_clone: Arc<Notify>`:
1. **Seal & cursor-flush loop**: unchanged cadence (`segment_cfg.flush_interval`), never blocked by reclamation.
2. **Reclaim loop**: wakes on `reclaim_interval = segment_cfg.flush_interval.max(Duration::from_secs(1))` (capped sensibly; immediately cancellable via `stop_clone.notified()`), invokes `reclaim_segments(&store, &stop_clone)` under `reclaim_lock: tokio::sync::Mutex<()>`.

Inside `reclaim_segments(store, cancel)`:
- Uses a new helper `write_manifest_with_etag_if_changed(store, |m| -> bool) -> Result<Option<Manifest>, Error>` which skips the manifest PUT (0 PUTs) whenever the closure returns `false`, and returns the exact committed `Manifest` snapshot after ETag convergence (avoiding stale outer-closure state across ETag retries).
- **Fast skip**: If `!retention_cfg.enabled` or `rewind_floor != u64::MAX`, return `Ok(0)` immediately.
- **Phase 1 — Adopt & deregister in manifest (ETag-coordinated)**:
  - Inside `write_manifest_with_etag_if_changed`, on each attempt against the freshly-read `m`:
    - Re-check `rewind_floor == u64::MAX`; if poisoned, return `false`.
    - Merge any in-memory `backfilled_meta` entries into `m.segment_meta` (and call `apply_seal` + bump `m.max_sealed_seq` if the segment is neither `active_segment` nor in `sealed_segments` nor in `pending_deletes`).
    - Compute `reclaim_floor = m.cursor.min(store.reclaimable_hwm.load(Ordering::Acquire))`.
    - Select `to_reclaim` from `m.sealed_segments` (excluding `active_segment`, requiring `segment_meta.last_seq <= reclaim_floor`) per D1.
    - Move `to_reclaim` out of `m.sealed_segments` and `m.segment_meta` into `m.pending_deletes` (deduplicated).
    - Return `true` iff any backfill or deregistration mutated `m`.
  - If the manifest was updated, clear the merged keys from `store.backfilled_meta`.
  - Determine `candidates = pending_deletes` from the committed (or freshly read) manifest, capped at `MAX_DELETES_PER_SWEEP = 64` per pass so backlogs drain smoothly without monopolizing resources. If empty, return `Ok(0)`.
- **Phase 2 — Best-effort, cancellable S3 `DELETE`**:
  - For each `seg` in `candidates` (up to `MAX_DELETES_PER_SWEEP`), race `client.delete(&segment_path)` against `cancel.notified()`:
    - On cancellation, break out cleanly (already-confirmed deletions still clean up in Phase 3 only if not cancelled; otherwise the next run recovers `pending_deletes` idempotently).
    - `Ok(())` and `Err(object_store::Error::NotFound { .. })` count as confirmed deleted.
    - Other errors log `tracing::warn!` and leave `seg` in `pending_deletes` for retry on a future sweep.
  - Evict confirmed-deleted indices from `store.segment_ranges`.
- **Phase 3 — Cleanup `pending_deletes` (ETag-coordinated, best-effort)**:
  - If `confirmed_deleted` is non-empty, call `write_manifest_with_etag_if_changed` to remove `confirmed_deleted` from `m.pending_deletes`.

**Crash matrix**:
- Crash before Phase 1 PUT: nothing deleted, next sweep retries cleanly.
- Crash between Phase 1 and Phase 2: `seg` is in `m.pending_deletes` and still in S3. `recover()` sees `seg` in `pending_deletes` → skips `GET` (no wasted download, no false warning) → next sweep completes Phase 2 + Phase 3. No orphan leak.
- Crash between Phase 2 and Phase 3: `seg` is in `m.pending_deletes` and already absent in S3. `recover()` skips `seg` → next sweep calls `DELETE` → gets `NotFound` (idempotent) → Phase 3 cleans `pending_deletes`. No false warning.
- Concurrent `publish_sealed_segment` / `flush_manifest`: all mutations are ETag-coordinated and commute cleanly.

### D5. Recovery & replay skip optimization

- In `recover()`:
  - Seed `max_seq_seen = manifest.max_sealed_seq.max(manifest.cursor)`.
  - Seed `reclaimable_hwm = manifest.cursor`.
  - Seed `max_idx_seen` across `manifest.active_segment`, `manifest.sealed_segments`, `manifest.pending_deletes`, AND all LISTed `.wal` filenames (before excluding `pending_deletes`).
  - Build `pending_set: HashSet<&str>` from `manifest.pending_deletes` and skip any segment in `pending_set` (no `GET`, no warning).
  - For each remaining segment `seg_name`:
    - If `manifest.segment_meta.get(seg_name)` exists:
      - Seed `store.segment_ranges` with `(meta.first_seq, meta.last_seq)` and bump `max_seq_seen = max_seq_seen.max(meta.last_seq)`.
      - If `meta.last_seq <= manifest.cursor`: **skip `GET` completely**.
      - Otherwise (`meta.last_seq > manifest.cursor`), `GET` and decode as before (preserving corruption/missing-segment observability for replayable data).
    - If `segment_meta` is absent (legacy segment or LIST-fallback segment): `GET` and decode as before, recording `segment_ranges`, `max_seq_seen`, and inserting into `store.backfilled_meta` (using `r.meta.last_modified` for `sealed_at_ms`).
- In `all_segment_names(&manifest)`:
  - Exclude filenames present in `manifest.pending_deletes`.
- In `read_after_cursor()` and `read_after()`:
  - Check both `manifest.segment_meta` and `self.segment_ranges` before issuing `GET`: skip any segment whose `last_seq <= manifest.cursor` (or `<= after`).

## Risks / Trade-offs

- **Two manifest PUTs on active reclaim cycles**: Each sweep that both deregisters new segments and deletes objects performs 2 manifest PUTs (Phase 1 deregister + Phase 3 cleanup). Idle sweeps perform 0 PUTs (and at most 1 manifest GET per `reclaim_interval`). In exchange, crash safety is airtight with zero orphan leaks and zero false-positive `NotFound` warnings.
- **`LocalFileSystem` test backend without conditional PUT**: `LocalFileSystem` returns `NotImplemented` for conditional PUT (`s3.rs:1570`). Existing code already runs concurrent ingestion + flusher writers with a warning; production S3 / MinIO / `InMemory` support ETag `If-Match`.
- **Un-committed `advance_cursor` callers**: Callers that invoke `Wal::advance(seq)` or `S3Store::advance_cursor(seq)` directly without `mark_committed(seq)` do not trigger deletion during that process lifetime (preserving rewind safety), though once reopened from a persisted `manifest.cursor`, `reclaimable_hwm` seeds from `manifest.cursor`. This mirrors `LocalStore`'s exact contract.

## Migration Plan

No migration required. Existing YAML configs without `retention` automatically gain default reclamation (immediate deletion of acknowledged sealed segments behind the reclaim floor). Existing v1/v2 manifests in S3 buckets open read-only, queue legacy/LIST-fallback metadata in memory, adopt it lazily on the first reclaim sweep, and reclaim historical segments safely.
