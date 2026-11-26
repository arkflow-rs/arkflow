## Why

The S3-compatible (`object_store`) WAL backend never reclaims sealed segment objects or shrinks its manifest: `flush_manifest` explicitly omits segment truncation (`crates/arkflow-plugin/src/wal/s3.rs:1395-1400`) because `Manifest` only tracks segment filenames without per-segment sequence bounds, sizes, or seal timestamps (`crates/arkflow-plugin/src/wal/manifest.rs:72-75`). Consequently, every sealed segment remains in object storage forever, the manifest grows without bound, and startup recovery plus `read_after_cursor` download and decode every historical segment before filtering past `manifest.cursor` (`crates/arkflow-plugin/src/wal/s3.rs:766-808`, `crates/arkflow-plugin/src/wal/s3.rs:947-974`). Long-running S3 WAL deployments therefore suffer unbounded object-storage cost and linearly degrading restart latency (registered as `PA-P1-06` in `PRODUCT-ITERATION-2026-11-22.md`).

## What Changes

- Add an optional `retention` configuration block (`RetentionConfig`) to `ObjectStoreWalConfig` in `arkflow-core`, supporting:
  - `enabled` (bool, default `true`)
  - `max_segments` (`Option<usize>`) — maximum acknowledged sealed segments retained behind the committed reclaim floor
  - `max_age` (`Option<Duration>`, reusing `arkflow-core`'s existing `humantime` dependency to parse `h` / `d` / compound durations while preserving bare integer ms and fractional seconds) — maximum age of acknowledged sealed segments
  - `max_bytes` (`Option<u64>`) — maximum total bytes across retained acknowledged sealed segments
  - Default behavior when `enabled: true` and all three limits are omitted: reclaim all eligible acknowledged sealed segments immediately (matching the existing `input-durability` `Segment reclaim` contract and local `redb` behavior). When one or more limits are explicitly configured, keep only the contiguous newest suffix of eligible segments satisfying all configured limits (stopping on the first violation so no history gaps form).
- Persist additive, backward-compatible per-segment metadata (`segment_meta: BTreeMap<String, SegmentMeta>` with `first_seq`, `last_seq`, `size_bytes`, `sealed_at_ms`) and a crash-recovery deletion queue (`pending_deletes: Vec<String>`) inside `Manifest` (keeping manifest version 2 with `#[serde(default)]`).
- Implement `WalStore::mark_committed(seq)` on `S3Store` so reclamation strictly respects the committed rewind-safe floor (`reclaim_floor = min(manifest.cursor, reclaimable_hwm)` where `reclaimable_hwm` advances only after wrapped source commit succeeds via `seq.saturating_sub(1)`, and pauses while a rewind poison is active) — matching `LocalStore`'s replay-safety guarantee.
- Implement crash-safe, best-effort, bounded two-phase reclamation on a **dedicated cancellable background loop** (decoupled from `seal_active_segment` so S3 `DELETE` latency never delays segment sealing or `Wal::wait_for_sealed`, and racing all reclamation awaits — lock acquisition, Phase 1/3 manifest I/O, and Phase 2 S3 deletes — against `CancellationToken::cancelled()` so `close()` cancels immediately without hanging):
  1. ETag-coordinated conditional manifest update (`write_manifest_with_etag_if_changed`, requiring conditional PUT support and skipping reclamation on backends returning `NotImplemented`) merges any backfilled legacy/LIST-fallback metadata, moves eligible segments from `sealed_segments` / `segment_meta` into `pending_deletes` (never touching `active_segment` or any unacknowledged segment), and performs zero PUTs when idle.
  2. Best-effort bounded S3 `DELETE` (capped at 64 objects per sweep and cancellable) removes queued objects (`NotFound` treated as idempotent success; failed names rotate to the tail of `pending_deletes` so healthy later objects are never starved).
  3. ETag-coordinated manifest cleanup removes confirmed-deleted names from `pending_deletes` and persists any failure rotation.
- Keep `recover()` strictly read-only while skipping `GET`s for `pending_deletes` and segments with `segment_meta.last_seq <= manifest.cursor`, seeding `max_written_seq` from `max(manifest.max_sealed_seq, manifest.cursor)`, recording decoded metadata (with `ObjectMeta::last_modified`) for legacy and LIST-fallback segments in memory for lazy manifest adoption, and applying the same skip optimization in `read_after_cursor()` / `read_after()`.
- Document the default retention policy, configuration knobs, crash-safety semantics, and object-storage cost model in `docs/docs/build/wal.md` and its `zh-Hans` counterpart.

## Non-goals

- Do not introduce S3 bucket lifecycle policy management (Terraform/IAM/bucket-level TTL rules); operators who prefer bucket-level archival can set `retention.enabled: false`.
- Do not change the local `redb` WAL reclamation mechanism (it already reclaims via `LocalStore::mark_committed`).
- Do not add Prometheus metrics for reclaimed segment counts/bytes in this change (structured `tracing` events cover observability; metrics wiring can follow in a dedicated observability change if needed).
- Do not change `Manifest::version` beyond `2` or break compatibility with existing v1/v2 manifests on disk/S3.
- Do not reclaim `active_segment` or alter `WalStore::retained_bytes()` admission-control semantics.

## Capabilities

### New Capabilities
<!-- None; this fulfills and refines existing durability and S3 WAL pipeline capabilities. -->

### Modified Capabilities
- `input-durability`: Refine `Segment reclaim` with configurable retention bounds (`enabled`, `max_segments`, `max_age`, `max_bytes`), rewind-safe committed reclaim floor, non-blocking cancellable crash-safe manifest-first deletion ordering, and active-segment / monotonic-sequence preservation.
- `s3-wal-pipeline`: Add per-segment manifest metadata persistence (`segment_meta`, `pending_deletes`), require recovery / replay reads to skip downloading segments already covered by the committed cursor or pending deletion, and update `Backward compatibility` to reflect cursor-based recovery skipping while preserving manifest GET + LIST union for unacknowledged and legacy segments.

## Impact

- **Core config & validation**: `crates/arkflow-core/src/wal/config.rs` (`RetentionConfig`, `ObjectStoreWalConfig.retention`, `humantime`-backed duration parsing), `crates/arkflow-core/src/wal/mod.rs` (`WalConfig::validate`).
- **Plugin WAL implementation**: `crates/arkflow-plugin/src/wal/manifest.rs` (`SegmentMeta`, `segment_meta`, `pending_deletes`), `crates/arkflow-plugin/src/wal/s3.rs` (`mark_committed`, `write_manifest_with_etag_if_changed`, `reclaim_segments`, decoupled cancellable reclaim loop in `spawn_flusher`, read-only `recover` with skip + in-memory backfill, `read_after_cursor`/`read_after` skip logic, unit & crash-recovery tests).
- **Tests**: Existing `ObjectStoreWalConfig` struct literals in `crates/arkflow-core`, `crates/arkflow-plugin/src/wal/s3.rs`, `crates/arkflow-plugin/tests/minio_integration.rs`, `crates/arkflow-plugin/tests/wal_optimization_e2e.rs` updated with `retention: RetentionConfig::default()`.
- **Docs**: `docs/docs/build/wal.md` and `docs/i18n/zh-Hans/docusaurus-plugin-content-docs/current/build/wal.md`, plus `CHANGELOG.md`.
- **Compatibility**: Fully backward-compatible YAML (omitted `retention` defaults to `enabled: true` with immediate reclaim of eligible sealed segments) and backward-compatible manifest JSON (`version` remains `2`).
