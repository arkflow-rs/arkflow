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
      1. `max_age.map_or(true, |d| Duration::from_millis(now_ms.saturating_sub(meta.sealed_at_ms)) <= d)` (uses `saturating_sub` so backwards wall-clock jumps never cause premature deletion, and compares `Duration` directly without narrowing `d.as_millis()`)
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
- **Exact boundary**: Committing sequence `S` advances `reclaimable_hwm` to `S - 1`, so every sealed segment with `last_seq <= S - 1` is eligible immediately, while the segment containing `S` itself stays protected until `S + 1` commits (or until restart, when `reclaimable_hwm` seeds from `manifest.cursor`). This matches `LocalStore::mark_committed`'s single-step undo contract (`crates/arkflow-core/src/wal/store.rs:441`); multi-step reverse-order undo across already-committed sequences is outside `WalStore::mark_committed`'s contract for both backends.
- Only segments in `manifest.sealed_segments` (never `manifest.active_segment`) whose `segment_meta` reports `last_seq <= reclaim_floor` are eligible. Keeping `active_segment` un-reclaimed plus seeding `max_seq_seen = manifest.cursor` (and advancing it from recovered segments) in `recover()` guarantees monotonic sequence allocation without introducing an unrecoverable ack gap if an unacked tail segment is missing, while keeping filenames collision-free across restarts.

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

Instead, `spawn_flusher` runs **two concurrent tasks** on its tokio runtime sharing a persistent `CancellationToken`:
1. **Seal & cursor-flush loop**: unchanged cadence (`segment_cfg.flush_interval`), wakes on `stop_seal.cancelled()`, never blocked by reclamation.
2. **Reclaim loop**: wakes on `reclaim_interval = segment_cfg.flush_interval.max(Duration::from_secs(1))` and races both the sleep timer and `reclaim_segments(&store, Some(&stop_reclaim))` against `stop_reclaim.cancelled()`. Because `CancellationToken` is persistent, cancellation signalled by `close()` is observed immediately even if it fires before a sweep starts waiting.

Inside `reclaim_segments(store, cancel)`:
- Uses `write_manifest_with_etag_if_changed_with_policy(store, ConditionalPolicy::RequireConditional, |m| -> bool) -> Result<(Manifest, bool), Error>`:
  - Skips the manifest PUT (0 PUTs) whenever the closure returns `false`, and returns `(manifest, mutated)` so callers inspect the freshly-read `manifest.pending_deletes` even when `mutated == false` (ensuring crash-left pending objects are still deleted).
  - Requires conditional PUT (`If-Match` / `Create`): if the backend returns `NotImplemented`, reclamation refuses the unconditional overwrite fallback, reports reclamation unavailable, and leaves all segment objects intact (preventing a concurrent seal from clobbering `pending_deletes` and re-listing a deleted segment). Non-reclamation callers keep `ConditionalPolicy::FallbackOverwrite`.
- Every await inside `reclaim_segments_at` — `reclaim_lock.lock()`, Phase 1 manifest read/write, Phase 2 per-object S3 `DELETE`, and Phase 3 manifest cleanup — is raced against `cancel.cancelled()` so `close()` never stalls on a slow or hung object-store request.
- **Fast skip**: If `!retention_cfg.enabled`, `rewind_floor != u64::MAX`, or `cancel` is already cancelled, return `Ok(0)` immediately.
- **Phase 1 — Adopt & deregister in manifest (ETag-coordinated, conditional-only)**:
  - Inside `write_manifest_with_etag_if_changed_with_policy`, on each attempt against the freshly-read `m`:
    - Re-check `rewind_floor == u64::MAX`; if poisoned, return `false`.
    - Merge any in-memory `backfilled_meta` entries into `m.segment_meta` (and call `apply_seal` + bump `m.max_sealed_seq` if the segment is neither `active_segment` nor in `sealed_segments` nor in `pending_deletes`).
    - Compute `reclaim_floor = m.cursor.min(store.reclaimable_hwm.load(Ordering::Acquire))`.
    - Select `to_reclaim` from `m.sealed_segments` (excluding `active_segment`, requiring `segment_meta.last_seq <= reclaim_floor`) per D1.
    - Move `to_reclaim` out of `m.sealed_segments` and `m.segment_meta` into `m.pending_deletes` (deduplicated).
    - Return `true` iff any backfill or deregistration mutated `m`.
  - If the manifest was updated, clear the merged keys from `store.backfilled_meta`.
  - Read `candidates = manifest.pending_deletes` (even when `mutated == false`), capped at `MAX_DELETES_PER_SWEEP = 64` per pass. If empty, return `Ok(0)`.
- **Phase 2 — Best-effort, cancellable S3 `DELETE`**:
  - For each `seg` in `candidates` (up to `MAX_DELETES_PER_SWEEP`), race `client.delete(&segment_path)` against `cancel.cancelled()`:
    - On cancellation, break out immediately (any objects already deleted are reconciled idempotently on the next run).
    - `Ok(())` and `Err(object_store::Error::NotFound { .. })` count as confirmed deleted.
    - Other errors log `tracing::warn!` and record `seg` in `failed_deleted` for tail rotation.
  - Evict confirmed-deleted indices from `store.segment_ranges`.
- **Phase 3 — Cleanup & failure rotation in `pending_deletes` (ETag-coordinated, conditional-only, cancellable)**:
  - If `!confirmed_deleted.is_empty() || !failed_deleted.is_empty()` and not cancelled, call `write_manifest_with_etag_if_changed_with_policy` to remove `confirmed_deleted` and rotate `failed_deleted` to the tail of `m.pending_deletes` (returning `false` if the list order did not change, avoiding redundant PUTs). Tail rotation ensures a permanently failing prefix of 64 objects never starves healthy segments behind it.

**Crash matrix**:
- Crash before Phase 1 PUT: nothing deleted, next sweep retries cleanly.
- Crash between Phase 1 and Phase 2: `seg` is in `m.pending_deletes` and still in S3. `recover()` sees `seg` in `pending_deletes` → skips `GET` (no wasted download, no false warning) → next sweep completes Phase 2 + Phase 3. No orphan leak.
- Crash between Phase 2 and Phase 3: `seg` is in `m.pending_deletes` and already absent in S3. `recover()` skips `seg` → next sweep calls `DELETE` → gets `NotFound` (idempotent) → Phase 3 cleans `pending_deletes`. No false warning.
- Concurrent `publish_sealed_segment` / `flush_manifest`: all mutations are ETag-coordinated and commute cleanly.

### D5. Recovery & replay skip optimization

- In `recover()`:
  - Seed `max_seq_seen = manifest.cursor` (not `manifest.max_sealed_seq`, so a missing unacked tail segment never advances `next_seq_hint()` past the unrecoverable gap).
  - Seed `reclaimable_hwm = manifest.cursor`.
  - Seed `max_idx_seen` across `manifest.active_segment`, `manifest.sealed_segments`, `manifest.pending_deletes`, AND all LISTed `.wal` filenames (before excluding `pending_deletes`).
  - Build `pending_set: HashSet<&str>` from `manifest.pending_deletes` and skip any segment in `pending_set` (no `GET`, no warning).
  - For each remaining segment `seg_name`:
    - If `manifest.segment_meta.get(seg_name)` exists and `meta.last_seq <= manifest.cursor`:
      - Seed `store.segment_ranges` with `(meta.first_seq, meta.last_seq)` and **skip `GET` completely**.
    - Otherwise (unacked segment `meta.last_seq > manifest.cursor`, or legacy/LIST-fallback segment lacking `segment_meta`): `GET` and decode as before (preserving corruption/missing-segment observability for replayable data), updating `segment_ranges` and `max_seq_seen` only after `GET` + decode succeeds, and inserting into `store.backfilled_meta` (using `r.meta.last_modified` for `sealed_at_ms`) when `segment_meta` is absent.
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
