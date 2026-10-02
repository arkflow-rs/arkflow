## Context

ArkFlow's local WAL (`crates/arkflow-core/src/wal/store.rs`) and keyed-state backend (`crates/arkflow-core/src/state.rs`) are thin users of redb: three tables (`ENTRIES: u64→&[u8]`, `META: &str→u64`, `STATE_TABLE: &str→&[u8]`), `Database::create`, `begin_read`/`begin_write`, plain `tx.commit()`, `ReadableTable` lookups, and `range(..=floor)` trims. No durability tuning, no tuples, no `AccessGuard`, no `insert_reserve` — the parts of redb 3.0/4.0 that broke (variable-width tuple serialization, `Durability::Paranoid` removal, `set_durability` fallibility, `AccessGuard` drops) are all outside our surface.

The real discontinuity is on disk: redb 2.6 creates v2-format files by default; redb 3.0+ only reads v3. Every ArkFlow release to date wrote v2. The maintainer has waived migration: breaking storage-format changes are acceptable, and this change ships no v2→v3 path.

## Goals / Non-Goals

**Goals:**

- Land redb 4.3.x with minimal, compile-driven code changes.
- Keep the WAL durability contract byte-for-byte in behavior (append/replay/trim/watermark/lock lifecycle).
- Make the v2-file open failure diagnosable: an operator hitting the boundary sees the file path and the reason, not an opaque redb internals error.

**Non-Goals:**

- v2→v3 migration of any form (no `Database::upgrade()`, no dual `redb2` dependency, no export/import).
- Durability semantics changes; feature adoption beyond the bump; WAL/state data-model changes.

## Decisions

1. **Straight bump, no transitional release.** The two-release or dual-dependency dances exist only to protect v2 files, which the maintainer explicitly deprioritized. A single PR is the surgical option.
2. **Wrap the two `Database::create` sites' errors with format-boundary context.** `store.rs:287` and `state.rs:441` currently map open errors generically; we extend the mapping so a format-refusal error surfaces as e.g. `open failed: <path>: redb 4 cannot open this database (created by an older ArkFlow/redb 2 file format). Remove the file (WAL replays/state restart cold) or downgrade.` Detection stays generic — we do not parse redb error variants beyond matching the open-failure path; the wrapper adds path + remedy context to any create/open failure, which also improves diagnosability of unrelated IO errors (disk full, permissions).
   - *Alternative*: match redb's specific `StorageError` variant for unsupported format — rejected: couples us to redb internals for marginal precision, and a generic create-failure wrapper already carries the file path, which is the actionable part.
3. **Import fix strategy: add `ReadableDatabase` to the two `use redb::{...}` lines.** `begin_read`/`begin_write` both live on `ReadableDatabase` in 3.0+ (`begin_write` was always trait-ish; the compiler drives the exact import set). No other code motion expected; if 4.x demands more, keep edits inside these two files plus `benchmark.rs`-style outliers.
4. **Tests reuse the crash-recovery corpus.** The existing WAL restart/replay/trim tests and state-backend tests exercise the contract end-to-end on real redb files; they must pass unmodified. One new unit test pins the v2-boundary behavior by hand-crafting an unopenable/corrupt-file case and asserting the wrapped, path-naming error (we cannot ship a real v2 fixture without a redb 2 dependency, so the pin targets the error-path wrapper: any create/open failure carries the file path).

## Risks / Trade-offs

- [Operators upgrading across the boundary lose WAL replays and keyed state on old files] → Maintainer-accepted; mitigated by the fail-loud, path-naming error so the remedy is discoverable at the terminal instead of in a support thread.
- [Unknown-unknown API breaks beyond `ReadableDatabase` in 4.3] → Compile-driven; the surface inventory (above) argues there are none; the full workspace test suite gates the PR.
- [redb 4.x regressions in lock/fsync behavior] → The exclusive-lock lifecycle and close/reopen behavior are covered by existing tests (`two_node_job_smoke`, WAL lifecycle tests); CI's broader suite is the gate.
- [New redb writes v3 files that older ArkFlow cannot read on rollback] → Inherent to any forward format move; same class as the accepted break, in the reverse direction. Not mitigated; noted for release notes.

## Migration Plan

One PR on `deps/stage6-redb-4`: version bump, import fixes, error-context wrappers, tests. Release notes call out the storage-format boundary and the "remove old files" remedy for operators carrying pre-upgrade data directories. Rollback = revert the PR (files created meanwhile are v3 and would then need the same treatment in reverse — acceptable given the waived policy).
