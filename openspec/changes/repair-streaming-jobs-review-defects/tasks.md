## 1. Window aggregate numeric correctness

- [x] 1.1 In `crates/arkflow-core/src/executor/window.rs`, seed `observe_i64` from `int_observations == 0` and leave `observe_float` on `float_observations == 0` (design D1). A `count == 0` term was tried and removed: it broke the existing mixed-order test. Verify: `cargo test -p arkflow-core --lib window` passes.
- [x] 1.2 Add the debug-only invariant check after `observe_*`/`merge`, plus `AggregateBuffer::normalize_observation_counters` applied in `decode_buffer`, and a unit test that a restored buffer (`count > 0`, counters defaulted to 0) keeps its range when a new float observation arrives. Verify: the test passes and no decoded buffer violates the invariant.
- [x] 1.3 Add the mixed-order regression tests: float-then-int (assert `min == 99.5`, `max == 100.0` for `[99.5, 100]`) and an all-negative variant where the fabricated boundary would be the `max`. Verify: both fail on the current tree, pass after 1.1.
- [x] 1.4 In `LegacyAggregateBuffer::migrate`, set `int_observations` from the legacy `count`, drop the legacy float sum (unrepresentable in a migratable payload), and return a fresh default buffer for an observed-nothing payload so its `i64::MIN`/`i64::MAX` placeholders cannot be merged as a boundary (design D2). Verify: a migrated buffer merges with a new delivery and the merged `min`/`max`/`sum` include the legacy contribution.
- [x] 1.5 Add `decode_buffer` tests: a legacy integer payload merges its contribution, an empty legacy payload stays neutral (no sentinel bounds), and the `is_float && count > 0` sentinel form still returns an error. Verify: `cargo test -p arkflow-core --lib window` passes.

## 2. WAL reclamation safety

- [x] 2.1 Add a committed-floor reclaim to `RedbStore`: `mark_committed(seq)` deletes entries strictly below `seq`, and `advance_cursor` deletes nothing at or below the new cursor (at most `cursor - 1`) (design D3). Add the method to the `WalStore` trait with a no-op default for backends that already reclaim on commit. Verify: `cargo test -p arkflow-core --lib wal` passes.
- [x] 2.2 Call `mark_committed(seq)` from the WAL acknowledgement path only after the wrapped source acknowledgement returns success (`crates/arkflow-core/src/wal/mod.rs`). Verify: the ack path compiles and `cargo test -p arkflow-core --lib wal` passes.
- [x] 2.3 Add a regression test for the loss: append N entries, advance the cursor for entry K, rewind to `K - 1`, advance again, then assert entry K is still replayable by `read_after_cursor` after a restart of the store. Verify: the test fails on the current tree and passes after 2.1/2.2.
- [x] 2.4 Update `next_seq_hint`/`max_seq` comments and assert in a test that the next assigned sequence is strictly greater than the persisted cursor after a full reclaim. Verify: the new test passes.

## 3. State backend accounting

- [x] 3.1 In `crates/arkflow-core/src/state.rs`, make the `max_bytes` check compute the exact byte delta inside the write transaction (row removed, row added) instead of reading the incremental counter, and keep the incremental counters for the unbudgeted path only (design D4). Verify: `cargo test -p arkflow-core --lib state` passes, including the existing `with_max_bytes` budget tests.
- [x] 3.2 Replace every `keys`/`bytes` `fetch_sub`/`fetch_add` with saturating arithmetic, and make `restore_entry`/`restore` adjust the counters for the row they touch instead of resetting them from a scan. Verify: a test that writes a TTL row, expires it, calls `restore_entry`, then writes again, asserts the counters stay consistent with `metrics()` and that the write succeeds.
- [x] 3.3 Add the underflow regression test: with `max_bytes` configured, expire a row, resync via `restore_entry`, purge via a write, and assert the subsequent write succeeds rather than reporting `state budget exceeded` (the current tree reports the latter). Verify: fails before, passes after.
- [x] 3.4 Fix the overwritten-expired-row accounting so the key count does not increment for a row that was physically present. Verify: a test that overwrites an expired-but-unpurged row asserts `keys` matches the physical row count.

## 4. State journal version fence

- [x] 4.1 Record the apply-time version in `Applied::previous_versions` (read the current version where `previous` bytes are read) so `restore_previous` writes back the version it actually replaced (design D5). Verify: `cargo test -p arkflow-core --lib state_journal` passes, including the range's `retried_apply_does_not_overwrite_a_newer_committed_value`.
- [x] 4.2 Extend the forward fence to every mutation kind: skip a stale `Put` as today, and fail the whole apply with an explicit error for a stale `Delete`/`Increment`. Verify: clippy clean and the existing journal tests pass.
- [x] 4.3 Add the two fence regression tests: (a) after a later commit on the same key, a compensated and retried transaction's `Delete` does not erase it; (b) a stale `Delete`/`Increment` fails the apply with the expected error and the newer value survives. Verify: (a) fails on the current tree, both pass after 4.1/4.2.
- [x] 4.4 Add a test pinning that an undo of a transaction applied after a concurrent same-key commit writes back the version it replaced, so a later compensation of the other transaction still recognizes ownership. Verify: the test passes.

## 5. Execution kernel worker pool

- [x] 5.1 Make `ProcessorWorkerPool::fail()` return an error for a disconnected failure channel and remove the chain loop's `None => { pool.take(); continue; }` retirement path (design D6). Verify: `cargo test -p arkflow-core --lib executor` passes.
- [x] 5.2 Give the pool's `flush()` a bound (or make it observe chain cancellation) so a single stalled delivery cannot park the chain forever. Verify: a test that submits a delivery whose worker never completes asserts `flush()` returns an error/abort within the bound.
- [x] 5.3 Add the pool-failure regression test: with `pipeline.thread_num > 1`, make a worker exit without recording a failure and assert the chain fails with an explicit error (not a silent `continue`) and the in-flight delivery is settled. Verify: fails on the current tree, passes after 5.1.

## 6. Acknowledgement settlement without blocking the source

- [x] 6.1 In `crates/arkflow-plugin/src/input/kafka.rs`, move `wait_for_assignment` outside the `ack_lock` and consumer-guard scope (take the assignment check, drop the guards, then wait and re-acquire). Verify: `cargo test -p arkflow-plugin --lib` passes and the lock scopes in `ack`/`undo` no longer enclose the wait.
- [x] 6.2 Settle the tombstone delivery without calling `ack()` inline in `read`: queue it for the same settlement path a forwarded delivery uses (or return it with a no-output marker), so `read` keeps polling and control events are not stalled. Verify: a unit test drives a null-payload delivery and asserts `read` returns without awaiting the ack, and that a settlement failure surfaces against the delivery rather than as an input error.
- [x] 6.3 The sibling-advancement property is covered structurally by `assignment_wait_starts_before_the_acknowledgement_lock`: the wait no longer holds the per-input `ack_lock`, so acknowledgements of other partitions cannot queue behind it (a live-broker concurrency test is not possible in this suite; `kafka_eos` requires Docker). Verified by inspection plus the lock-scope test.

## 7. Agent and Hub interoperability

- [x] 7.1 In `crates/arkflow-server/src/agent.rs`, send the session token in both the `Authorization` header and the legacy query parameter for the command poll and the command result (`bearer_auth` plus the existing query field) (design D7). Verify: the existing two-node smoke test passes and the Hub receives the credential from either transport.
- [x] 7.2 Add a Hub-side test that the command endpoints authenticate from the header, from the query parameter, and from both (header wins). Verify: `cargo test -p arkflow-server` passes.

## 8. Control-plane conditional writes

- [x] 8.1 Extend `update_job_with_expected_generation` so its compare-and-set predicate covers the recovery pointer as well as the generation (SQLite `IS ?`), and set the pointer from the request (design D8). Verify: `cargo test -p arkflow-server` passes.
- [x] 8.2 Add the rollback state-format check by reusing the upgrade path's compatibility helper, returning the same error code. Verify: a test rolls back to an incompatible version and asserts the rejection and an unchanged Job.
- [x] 8.3 Add the concurrency regression test: a checkpoint report that moves the recovery pointer between the rollback handler's read and its write produces a conflict (or preserves the newer pointer), and the pointer never regresses. Verify: fails on the current tree, passes after 8.1.
- [x] 8.4 The `state.max_pending_transactions` schema/docs task is tracked in 10.4 (core-side validation and schema generation).

## 9. Console behavior

- [x] 9.1 In `console/src/features/job-editor.tsx`, call `updateSpec` from `onNodesChange` only when the derived spec changed, so selecting or repositioning a node does not clear a successful validation (design D9). Verify: the console build/type-check passes and a manual run shows the submit button staying enabled after selection.
- [x] 9.2 Surface the Hub's recorded lifecycle state after an upgrade and offer the start action instead of closing the editor silently. Verify: a manual run of an upgrade shows the stopped state and the start action.
- [x] 9.3 Fix the `KeyValueForm` rename guard to use an own-property check (`Object.hasOwn`) and to keep the field controlled while a rename is rejected, so an inherited name (`constructor`, `toString`) is usable and no key vanishes silently. Verify: the console type-check/build passes.

## 10. Verification and documentation

- [x] 10.1 Run `cargo test --workspace` and confirm every new regression test fails on the parent commit and passes on the branch. Verify: record the before/after per test in the change notes.
- [x] 10.2 Removed the dead code the review flagged: `split_by_physical_partition`, `split_by_partition` (event_time_gate.rs), and `LegacyAggregateBuffer::sum_float`. `KernelJobHandle::states` is left in place: it is untouched by this change, removing it means changing the public `spawn`/`spawn_with_cancellation` signatures and six constructor sites, which is a standalone refactor rather than part of this repair. `cargo clippy --workspace --all-targets` reports no warning from this change's code.
- [x] 10.3 Make the Docker-less `kafka_eos` skip explicit (print a skipped banner and, when the suite runs in CI, fail unless an explicit opt-out is set) so a green run cannot mean zero assertions. Verify: run the suite with and without a container engine and compare the reported outcomes.
- [x] 10.4 Update `docs/docs/configuration/1-top-level.md` and `docs/docs/streaming-jobs.md` for the new bound field and the corrected aggregate/rollback semantics. Verify: `docs` build (or the docs check task) passes.
- [ ] 10.5 Re-run `/code-review` on the repaired range and confirm no finding is re-reported. Verify: the review reports an empty or fully-explained finding list.
