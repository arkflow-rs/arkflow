## Why

The dependency-refresh track has one remaining actionable major bump: `redb` 2.6.3 → 4.x. ArkFlow has used `redb = "2"` since the WAL was introduced (#1178); today the locked copy is 2.6.3 (`Cargo.lock` line 8304) and the workspace pins `redb = "2"` (`Cargo.toml` line 82). redb 3.0+ brings a more efficient file format (minimum database file size drops from ~2.5MiB to ~50KiB — ArkFlow opens one `wal.redb` per WAL-enabled stream, `crates/arkflow-core/src/wal/store.rs:286`, plus a global `state.redb`, `crates/arkflow-core/src/state.rs:441`), ~15% faster bulk writes on the WAL append path, and constant-overhead savepoints. Staying two majors behind also grows the eventual distance to redb 5 (already incubating behind an experimental feature flag).

The one hard blocker — file-format migration for existing v2 on-disk files — has been explicitly waived by the maintainer: **breaking storage-format changes are acceptable; no v2→v3 migration tooling will be built.**

## What Changes

- Bump `redb` workspace dependency from `"2"` to `"4"` (latest 4.3.x) and regenerate the lock; single redb copy remains.
- Fix compile-driven API drift. The call surface (`Database::create`, `begin_write`, `commit`, `ReadableTable`, `TableDefinition`, `range`) survives 3.0/4.0 intact; the known break is `begin_read` moving from an inherent `Database` method to the `ReadableDatabase` trait (`crates/arkflow-core/src/wal/store.rs:297,416,444`, `crates/arkflow-core/src/state.rs:467,591,620,1051,1086`).
- **BREAKING**: databases created by every prior ArkFlow release are redb file-format v2 (redb 2.6 defaults to v2; v3 requires opt-in). redb 4 refuses to open them. Old `wal.redb`/`state.redb` files will not load after upgrade; operators moving an existing deployment across this boundary must remove the old files (WAL replays are lost, keyed state restarts cold). The open failure must be fail-loud with a diagnosable message, never silent corruption or silent recreation.
- No table schema changes: `ENTRIES: u64→&[u8]`, `META: &str→u64` (`store.rs:44-46`), `STATE_TABLE: &str→&[u8]` (`state.rs:12`) contain no tuples, so redb 3.0's variable-width-tuple serialization break does not apply.

## Non-goals

- **No v2→v3 file migration** (no `Database::upgrade()` path, no dual-dependency shim, no export/import tool) — maintainer decision.
- No durability-semantics tuning: the code uses default `commit()` durability and does not call `set_durability` (which became fallible in redb 3.0); that stays as-is.
- No adoption of new redb 4 features (read-only handles, multi-process reads, custom `StorageBackend`) beyond what the bump provides for free.
- No changes to the WAL/state data model, watermark logic, or the specs' recovery contracts.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `input-durability`: adds one requirement — the storage-backend upgrade preserves the WAL's operator-facing durability contract (append/replay/trim semantics unchanged; new files are v3-format redb; legacy v2 files fail loudly at open with a message identifying the file and remedy).

## Impact

- **Code**: `crates/arkflow-core/src/wal/store.rs`, `crates/arkflow-core/src/state.rs` (trait import, possibly error-message wrapping at the two `Database::create` sites); `crates/arkflow-plugin/src/benchmark.rs` and any other redb importers follow the same compile-driven fixes.
- **Dependencies**: workspace `Cargo.toml` version string; `Cargo.lock` redb 2.6.3 → 4.3.x.
- **Operators**: one-way storage-format boundary at this release; upgrade docs note belongs with the release notes, no repo docs page is affected (no config surface changes).
