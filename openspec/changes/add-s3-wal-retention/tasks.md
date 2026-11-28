## 1. Core Retention Configuration & Validation

- [x] 1.1 Add `RetentionConfig` (`enabled`, `max_segments`, `max_age`, `max_bytes`) to `crates/arkflow-core/src/wal/config.rs` and attach `#[serde(default)] pub retention: RetentionConfig` to `ObjectStoreWalConfig`
- [x] 1.2 Upgrade `duration_serde` / `duration_serde_option` in `crates/arkflow-core/src/wal/config.rs` to use `humantime::parse_duration` (supporting `h`/`d`/compound durations while preserving bare integer ms and fractional seconds) and return deserialization errors on invalid strings; add unit tests for YAML round-trips, defaults, and invalid strings
- [x] 1.3 Add retention validation (`max_age` > 0 when set) to `WalConfig::validate` in `crates/arkflow-core/src/wal/mod.rs` and `S3Store::build_with_client_inner` in `crates/arkflow-plugin/src/wal/s3.rs`, and update existing `ObjectStoreWalConfig` test literals across `arkflow-core` and `arkflow-plugin`

## 2. Manifest Schema & Per-Segment Metadata

- [x] 2.1 Add `SegmentMeta` (`first_seq`, `last_seq`, `size_bytes`, `sealed_at_ms`), `segment_meta: BTreeMap<String, SegmentMeta>`, and `pending_deletes: Vec<String>` (all `#[serde(default)]`, staying at version 2) to `Manifest` in `crates/arkflow-plugin/src/wal/manifest.rs` with backward-compatibility round-trip tests
- [x] 2.2 Record `SegmentMeta` in `publish_sealed_segment` (`crates/arkflow-plugin/src/wal/s3.rs`) on every segment seal without extra S3 PUTs

## 3. Rewind-Safe Floor & Non-Blocking Crash-Safe Reclamation

- [x] 3.1 Add `reclaimable_hwm: AtomicU64`, `retention_cfg: RetentionConfig`, `reclaim_lock: tokio::sync::Mutex<()>`, and `backfilled_meta: StdMutex<BTreeMap<String, SegmentMeta>>` to `S3Store`; implement `WalStore::mark_committed(seq)` (`fetch_max(seq.saturating_sub(1))`) and clamp `reclaimable_hwm` in `rewind_cursor`
- [x] 3.2 Implement pure contiguous-suffix retention selection logic (`select_reclaimable_segments`) covering `enabled: false`, default immediate reclaim (all caps `None`), and tightest-bound `max_segments` / `max_age` / `max_bytes` combinations (breaking on first violation to avoid gaps); unit-test every policy combination including the `max_bytes` gap case
- [x] 3.3 Add `write_manifest_with_etag_if_changed` and implement crash-safe, bounded (`MAX_DELETES_PER_SWEEP`), cancellable `reclaim_segments(store, cancel)` (Phase 1 ETag manifest backfill/adopt + move `sealed_segments` → `pending_deletes`, Phase 2 best-effort cancellable `client.delete` with `NotFound` idempotency, Phase 3 ETag manifest cleanup of `pending_deletes`), with zero PUTs when idle
- [x] 3.4 Spawn the reclaim loop as a decoupled, cancellable task inside `spawn_flusher`'s tokio runtime so S3 deletes never delay `seal_active_segment` or hang `close()`

## 4. Read-Only Recovery & Replay Read-Skipping

- [x] 4.1 Update `recover()` in `crates/arkflow-plugin/src/wal/s3.rs` to stay strictly read-only, seed `max_seq_seen` from `manifest.max_sealed_seq.max(manifest.cursor)`, seed `reclaimable_hwm` from `manifest.cursor`, account for `pending_deletes` in `max_idx_seen`, skip `GET` for `pending_deletes` and segments with `segment_meta.last_seq <= manifest.cursor`, and queue decoded metadata (with `ObjectMeta::last_modified`) for legacy & LIST-fallback segments into `backfilled_meta`
- [x] 4.2 Update `all_segment_names`, `read_after_cursor`, and `read_after` to exclude `pending_deletes` and skip `GET` for segments whose `segment_meta` or `segment_ranges` `last_seq <= cursor/after`

## 5. Comprehensive Verification Tests

- [x] 5.1 Add end-to-end S3 WAL retention tests in `crates/arkflow-plugin/src/wal/s3.rs` using an `ObjectStore` spy counting `GET` / `PUT` / `DELETE` calls:
  - Write N segments, commit past N-2, trigger retention → only necessary segments remain; reopen succeeds and issues 0 `GET`s for deleted or acknowledged segments
  - Verify `max_segments`, `max_age`, `max_bytes` (including contiguous-suffix no-gap check), combined bounds, and `enabled: false`
  - Verify rewind safety: `advance_cursor` without `mark_committed`, `mark_committed(S)` protecting `S` for `Wal::undo(S)`, and `rewind_cursor` poison pausing reclamation
  - Verify crash-recovery between Phase 1 (in `pending_deletes`, object still present) and Phase 2 (object deleted, still in `pending_deletes`) produces zero missing-segment warnings and self-heals on next sweep
  - Verify legacy v2 manifest without `segment_meta` and LIST-fallback segments open read-only, get adopted lazily, get reclaimed, and keep sequence/index allocation monotonic
  - Verify idle reclaim sweeps issue zero manifest PUTs

## 6. Documentation & Gates

- [x] 6.1 Document S3 WAL retention configuration, default policy, crash-safety semantics, and object-storage cost model in `docs/docs/build/wal.md` and `docs/i18n/zh-Hans/docusaurus-plugin-content-docs/current/build/wal.md` (with `validate=fragment wrap=durability` markers on YAML snippets)
- [x] 6.2 Update `CHANGELOG.md` `[Unreleased]` with S3 WAL retention entry
- [x] 6.3 Run `openspec validate add-s3-wal-retention`, `cargo test -p arkflow-core wal`, `cargo test -p arkflow-plugin wal`, `cargo test -p arkflow --test docs_snippets_validate`, `cargo clippy --workspace --all-targets`, and `pnpm --dir docs run docs:check`
