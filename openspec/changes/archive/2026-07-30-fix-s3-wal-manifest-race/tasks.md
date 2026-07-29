## 1. Extract ETag-aware manifest read

- [x] 1.1 Add `read_manifest_with_etag` async helper in `crates/arkflow-plugin/src/wal/s3.rs` that returns `(Manifest, Option<String>)`. `NotFound` → `(Manifest::fresh(...), None)`. Reuse the existing `Manifest::from_json` parser.
- [x] 1.2 Verify: `cargo build -p arkflow-plugin` succeeds; `cargo clippy -p arkflow-plugin -- -D warnings` clean. (Smoke check — helper alone is not exercised at runtime yet.)

## 2. Refactor `write_manifest` into a coordinated closure-based writer

- [x] 2.1 Replace `write_manifest` with `write_manifest_with_etag<F>(store, mutator: F)` per design Decision 1: read base+ETag, run mutator, PUT with `PutMode` (`Update{e_tag}` when ETag present, `Create` when fresh) retrying on `Precondition`/`AlreadyExists` up to `MANIFEST_WRITE_MAX_RETRIES = 8`, with a `NotImplemented` → `Overwrite` fallback for backends lacking conditional PUT; surface `Error::Process` on budget exhaustion.
- [x] 2.2 Migrate the call site in `seal_active_segment` (`crates/arkflow-plugin/src/wal/s3.rs:769-810`) to use the closure form. Add an idempotency guard so retry does not double-append a segment name: `if !m.sealed_segments.contains(&name) { m.sealed_segments.push(name); }` (also keep the demotion-from-active logic).
- [x] 2.3 Migrate the call site in `flush_manifest` (`crates/arkflow-plugin/src/wal/s3.rs:814-852`) to the closure form. Cursor advance is naturally idempotent: `if new_cursor > m.cursor { m.cursor = new_cursor; }`. Verify the in-memory `cursor_pending` / `cursor_last_flush_ms` accounting still resets around the same point in `flush_manifest`.
- [x] 2.4 Verify: `cargo build -p arkflow-plugin` clean; `cargo test -p arkflow-plugin wal_optimization_e2e` still passes (existing E2E exercises the same code path; if it fails, the retry budget or mutator composition likely regressed).

## 3. Add diagnostic tracing

- [x] 3.1 In `write_manifest_with_etag`, emit `tracing::debug!(attempt, "manifest ETag mismatch, re-reading and retrying")` on each retry.
- [x] 3.2 When `attempt >= 3` before success, escalate the same record to `tracing::warn!` so contention shows up at default INFO+ level. Adjust the condition so we warn when the *next* attempt number is the 3rd or later.
- [x] 3.3 Verify: `cargo build -p arkflow-plugin` clean; manually trigger a synthetic retry (test or local run with `RUST_LOG=debug`) and confirm the messages appear in order.

## 4. Concurrency regression tests

- [x] 4.1 Add the concurrency tests to `crates/arkflow-plugin/src/wal/s3.rs`'s internal `#[cfg(test)] mod tests` (integration tests can't reach `pub(crate)` `write_manifest_with_etag`/`S3Store`/`Manifest`, and exposing them is a Non-goal). Use `object_store::memory::InMemory` as the backend so ETag semantics are exercised without minio.
- [x] 4.2 Implement T1 (concurrent cursor advance): use `tokio::join!` over 8 tasks each calling the closure form with `m.cursor = X_i` for distinct `X_i`. After all tasks complete, GET the manifest via `read_manifest_with_etag`, assert `m.cursor == X_i.max()`.
- [x] 4.3 Implement T2 (concurrent seal stress): spawn 8 tasks concurrently each sealing a unique segment name (`{:08}.wal` with distinct `i`). After join, assert `m.sealed_segments` contains exactly all 8 names, in any order, with no duplicates.
- [x] 4.4 Implement T3 (single-writer baseline): 8 sequential writes of `m.cursor += 1`. Assert final `m.cursor == 8`. This guards against the mutator losing the "freshly-read base" property.
- [x] 4.5 Implement T4 (retry budget exceeded): wrap the in-memory store in a small adapter that returns `PreconditionFailed` on every PUT, regardless of ETag. Call the writer with a closure that sets `cursor = 1`, assert the call returns `Err(Error::Process(_))` and that the error message identifies the retry exhaustion.
- [x] 4.6 Verify: `cargo test -p arkflow-plugin --test wal_manifest_race` passes all four test cases.

## 5. Existing test sweep and lint

- [x] 5.1 `cargo test -p arkflow-plugin` — no existing test should regress. If anything fails, the most likely cause is a call-site that depended on the old `write_manifest` signature or that mutated the manifest outside the closure form.
- [x] 5.2 `cargo clippy -p arkflow-plugin --all-targets` — this change's code (`write_manifest_with_etag`, helpers, T1-T4 tests) is clippy-clean. (The wider workspace has pre-existing warnings in other modules present at HEAD; out of scope for this change.)
- [x] 5.3 `cargo fmt` on this change's file (`crates/arkflow-plugin/src/wal/s3.rs`) is clean. (`tests/wal_optimization_e2e.rs` has pre-existing unformatted lines at HEAD, untouched here.)

## 6. Documentation and changelog

- [x] 6.1 Add a brief section to `docs/docs/components/0-inputs/delivery-semantics.md` (actual path is under the `docs/docs/` docusaurus root — the original task path was missing that segment) describing the manifest write coordination contract: ETag-conditioned PUT (`Update`/`Create`) + retry + `NotImplemented` fallback, concurrency safety, no behavior change for single-writer setups.
- [x] 6.2 Cross-link from `docs/performance/s3-wal-backend.md` so readers tuning `parallel_put.workers > 1` see a pointer to the manifest safety story.
- [x] 6.3 No changelog file — repository convention appears to be PR titles + commit messages (verify by spot-checking recent merged PRs that touch WAL code).

## 7. Verification before merge

- [x] 7.1 Run `openspec verify change fix-s3-wal-manifest-race` and confirm it reports no drift between artifacts.
- [x] 7.2 Manually read the diff under `crates/arkflow-plugin/src/wal/s3.rs` and confirm: (a) every former `write_manifest(...)` call site is on the closure form, (b) no `client.put(...)` remains in the manifest write path (segment PUTs are unchanged), (c) all existing public function signatures are preserved.
- [x] 7.3 Confirm the manifest JSON schema, the `Manifest` struct in `crates/arkflow-plugin/src/wal/manifest.rs`, and the recovery path in `crates/arkflow-plugin/src/wal/s3.rs:479-588` are untouched (the change is purely a write-path refactor).
