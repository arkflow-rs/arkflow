## 1. Pooled-path backpressure fix (runtime)

- [x] 1.1 Bound the worker-pool result channel in `ProcessorWorkerPool::start` (`crates/arkflow-core/src/executor/task.rs:1538`): `flume::bounded::<(u64, PoolResult)>(parallelism * 2)` instead of `unbounded`.
- [x] 1.2 ~~Wrap the worker's `done_tx.send` in a `tokio::select!`~~ Revised during implementation (design Decision 2 updated): flume fails blocked senders with `SendError` when the collector exits and drops `done_rx`, so the worker's existing error branches already guarantee cancellation liveness — no wrapper needed; worker loop untouched.
- [x] 1.3 Verify drain/EOS liveness: the existing pool tests (`configured_thread_num_runs_ordered_concurrent_processors`, both pool-failure tests, `multi_input_barrier_seals_one_acknowledged_cut`) stay green without changes.
- [x] 1.4 Add a pool-path backpressure regression test: `parallelism > 1`, slow sink, capacity-1-style downstream; assert source reads stay bounded (mirroring `backpressure_blocks_upstream_when_channel_is_full`, which only covers the single-worker path). Note: the first bound-only attempt deadlocked the runtime via flume's sync send; fixed together with the async-send change (design Decision 2).

## 2. Test hardening

- [x] 2.1 Give `CountingAck` an abort counter (override `abort`) and assert in `pool_processor_failure_without_error_output_fails_and_aborts` that the failed delivery's acknowledgement was actually aborted (`aborts == 1`, `acks == 0`).
- [x] 2.2 Add a siblings-topology pool failure test: first processor emits multiple outputs, second processor fails, every sibling plus the failed delivery reaches the error sink exactly once.
- [x] 2.3 Extend the hub aggregation coverage: complete the existing two-node test to a terminal assertion (second peer succeeds → observed `running`), and add a variant where a peer reports a permanent failure after the full set is evaluated → observed `failed`.
- [x] 2.4 Add the three missed late-counter branches as sliding-window gate cases: Hold with non-empty exclusions counts, held release with `action == Emit` plus exclusions counts via the exclusions clause, and a `route_late` row with a non-Route action counts exactly once.
- [x] 2.5 Add a graph-level `kernel.late_events` assertion: run a gated job through `KernelJobRunner` with a late row and assert `handle.metrics().snapshot()` late-event counter equals the expected row count.
- [x] 2.6 Fix the snapshot-failure test's tautological assertions: remove the second `snapshot.verify()` re-check (or replace with a real restore-readback) and correct the `checkpoint_failures` assertion's comment to state only what it proves.
- [x] 2.7 Delete the dead `_retryable` closure from `aggregate_job_observed_state` (`crates/arkflow-server/src/hub.rs:2696`) and fix the new hub test's comment/name to describe the state-driven (not class-driven) mechanism.

## 3. Spec deltas (drafted in this change; verify at archive)

- [x] 3.1 `openspec validate repair-kernel-review-findings --strict --no-interactive` passes with all ten delta files.
- [x] 3.2 Confirm each delta's `### Requirement:` header matches its master spec exactly (mechanical check across the ten capabilities).

## 4. Final verification

- [x] 4.1 `cargo fmt --all -- --check` and `git diff --check` are clean.
- [x] 4.2 `cargo test --workspace --no-fail-fast` passes with only the known Docker-gated `kafka_eos` failures.
- [x] 4.3 Update the max-effort review findings: mark items addressed by this change as `fixed` in the review record.
