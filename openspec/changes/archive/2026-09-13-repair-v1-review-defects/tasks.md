## 1. Kernel: checkpoint rounds fail closed

- [x] 1.1 In `crates/arkflow-core/src/executor/task.rs` (source-chain barrier branch, ~469-531): seed the frontier with an explicit unavailable marker when `current_positions()` errs and make the round fail on that marker instead of sealing an empty/stale seed; keep the failure reported to `failure_reporter`.
- [x] 1.2 In `crates/arkflow-core/src/executor/kernel_handle.rs` (~199-304): drain `checkpoint_errors` before the waiting loop can return success, so a queued snapshot error always fails the round.
- [x] 1.3 In `task.rs` run_chain/finished handling (~313-320, 1842-1877): only exempt a chain from `remaining` when it delivered a round snapshot or the round is already failing; invalidate a round whose graph has a failed chain before persist.
- [x] 1.4 Collector drain timeout (`task.rs` ~1842-1858): set the join error so the round fails, and join/abort the abandoned collector before EOS dispatch and sink close.
- [x] 1.5 Regression tests in `crates/arkflow-core/src/executor/tests.rs`: positions-error round fails; error-only round does not report success; collector drain timeout fails the round and no post-EOS downstream write occurs.

## 2. Journal: fenced retry for skipped Increment compensation

- [x] 2.1 In `crates/arkflow-core/src/executor/state_journal.rs` (~663-664, 866-869, 1105-1115): when `restore_previous` skips a transaction's `Increment` because a later transaction owns the key, mark the staged mutation already-applied in the undo snapshot so the restage replays it as a no-op (or fails the apply explicitly on a further version move).
- [x] 2.2 Regression test: transaction A (+1) applied, B commits, A's undo skips restore, A retried → key keeps B's value; plus a forward-path test that two un-compensated increments still compose.

## 3. Event-time windows: no lost corrections or silent skips

- [x] 3.1 In `crates/arkflow-core/src/executor/window.rs` `fire_ready` (~1384-1408): exclude `updated_since_emit` buffers from the expired filter so a pending correction is emitted before reclaim, including the EOS path (`threshold = i64::MAX`).
- [x] 3.2 In `crates/arkflow-core/src/executor/event_time_gate.rs` `finish` (~572-587): classify held rows per membership against real per-window lateness deadlines from the last observed watermark; stop feeding a sentinel `i64::MAX` watermark through the late policy for classification.
- [x] 3.3 In `window.rs` `accumulate` (~1112-1115): route timestamped NULL-key rows through the invalid/late policy (count metric, route to side output when configured, else drop + ack) instead of a silent skip.
- [x] 3.4 In `window.rs` non-journal constructors (`new`/`with_late_event_policy` ~626-631, 2150, 2551-2567): make `WindowRollback::restore` also undo backend writes (restore prior aggregate / remove emitted marker) so replay after a failed ack cannot double-count.
- [x] 3.5 Regression tests: EOS emits the session-bridge correction before cleanup; EOS sliding classification keeps the still-live membership; NULL key is counted/routed; direct-path ack failure then replay does not double-count.

## 4. Durability: atomic local checkpoints and ordered undo

- [x] 4.1 In `crates/arkflow-core/src/checkpoint.rs` `FileCheckpointStore` (~157-165): write `<path>.tmp` + `fsync` + rename + parent-dir `fsync`; run retention deletion of the previous checkpoint only after the new manifest is durable.
- [x] 4.2 In `crates/arkflow-core/src/wal/mod.rs` `CommitOnAck::undo` (~662-663) and `state_journal.rs` undo path (~1100-1104): compensate journal state before persisting the source/WAL cursor rewind; if compensation fails, leave the cursor advanced and the transaction retryable.
- [x] 4.3 Regression tests: crash-injected torn write leaves the previous checkpoint recoverable; undo interrupted between the two steps cannot reapply a committed mutation on restart.

## 5. Kafka: assignment wait lock scope

- [x] 5.1 In `crates/arkflow-plugin/src/input/kafka.rs` (~788-810): clone the consumer handle, drop the read lock before awaiting assignment, and re-evaluate against the current consumer if it was replaced during the wait.
- [x] 5.2 Extend `assignment_wait_starts_before_the_acknowledgement_lock` (kafka tests ~1038): assert no consumer read guard is held across the wait and that a concurrent `connect()` completes without waiting out the assignment timeout.

## 6. Hub: stop churn, retention, fault isolation, lock hygiene

- [x] 6.1 In `crates/arkflow-server/src/hub.rs` dispatch (~788-837): extend the terminal-state skip to `job_stop` (and symmetric `job_start` placement match) keyed on (job_id, generation, operation) with desired state unchanged.
- [x] 6.2 In `hub.rs` abandoned-start supersession (~711-723): collect ids under `operations.write()`, drop the lock, then persist the transitions.
- [x] 6.3 In `hub.rs` fencing-stop enqueue (~729-751) and `reconcile_jobs` (~551-565): downgrade per-Job enqueue failures (`NodeUnavailable`, `Capacity`) to warn + skip-that-Job instead of aborting the tick.
- [x] 6.4 Add bounded retention for `cp_operations` and pending/failed `cp_job_checkpoints` in `crates/arkflow-server/src/storage.rs` wired into the existing periodic sweep (`hub.rs` ~1348-1360 reclaims non-completed records).
- [x] 6.5 Tests in `crates/arkflow-server/tests/`: a stopped Job produces no new stop commands/operation rows across ticks; a Capacity/NodeUnavailable enqueue failure for one Job does not stop other Jobs' reconciliation; retention reclaims stale pending checkpoint records.

## 7. Agent: no zombie kernels, LRU dedup, off-runtime I/O

- [x] 7.1 In `crates/arkflow-server/src/agent.rs` `JobRuntime::start` (~507-534): register the `JobTask` (with cancellation token, `Starting` state) before `spawn_kernel_job`, remove on failure, and add a Drop backstop on the kernel handle that cancels the token.
- [x] 7.2 Make `stop`/`stop_all`/`command_tasks.abort_all` paths (~1328-1351) cancel through the registered token so an aborted in-flight start cannot leave a running kernel; shrink tasks-lock critical sections in `checkpoint`/`aggregate_checkpoint` (~546-671, 682-796) to map updates only.
- [x] 7.3 In `agent.rs` `remember_completed_command` (~1949-1958): replace `cache.clear()` with oldest-entry eviction.
- [x] 7.4 In `agent.rs` `start()` recovery (~460-495): route `read_manifest`/`read_state_snapshot` through the existing `spawn_blocking` wrapper used by the checkpoint path.
- [x] 7.5 Tests: abort a start mid-spawn → no unregistered running kernel (stop reaches it or it is cancelled); dedup cache at bound evicts one entry and a redelivered completed command returns its existing result; recovery read does not block the runtime thread.

## 8. Compilation: legacy durations, deprecation docs, dead code removal

- [x] 8.1 In `crates/arkflow-core/src/executor/stream_compiler.rs` `parse_duration_ms` (~405-418): accept the legacy `humantime` grammar (µs/us/ns, compound `1h30m`) via `humantime::parse_duration` with ms overflow checks; out-of-grammar values fail naming the accepted grammar.
- [x] 8.2 Update `README.md`, `README_zh.md`, and `docs/docs/components/1-buffers/sliding_window.md`: remove sliding_window/join from "available" component lists, mark the plugins deprecated/unreachable via the compiler (join already errors), and fix the memory-buffer example fields that are silently ignored.
- [x] 8.3 Delete `crates/arkflow-core/src/streaming_sql.rs` and its `pub mod` in `lib.rs` (no production callers; plugin `sql.rs` uses DataFusion's own parser); remove its tests.
- [x] 8.4 Tests: golden/duration tests cover `500us` and `1h30m`; `cargo build` passes without `streaming_sql`.

## 9. Console: deterministic editor state

- [x] 9.1 In `console/src/features/job-editor.tsx` (~43-47): reset draft state (spec, graph, validation) when mode/target changes (keyed remount or effect), so an upgrade opened from the detail panel never submits a create draft.
- [x] 9.2 In `job-editor.tsx` `validate` (~80-81): capture the request spec and apply the response only if the current spec still matches.
- [x] 9.3 In `job-editor.tsx` `addComponent` (~78): generate node ids from a monotonic counter ref instead of `length + 1`.
- [x] 9.4 Update `console/src/features.test.tsx`: upgrade-after-create submits the target Job's spec; late validate response does not unlock submit; add/delete/add yields unique node ids.

## 10. Final verification

- [x] 10.1 `cargo clippy --workspace --all-targets` clean.
- [x] 10.2 `cargo test --workspace --all-targets` green.
- [x] 10.3 `./target/release/arkflow --config examples/<jobs example> --validate` passes and doc/component lists match `components list`.
