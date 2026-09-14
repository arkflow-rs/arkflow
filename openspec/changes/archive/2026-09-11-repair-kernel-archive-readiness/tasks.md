## 1. Worker-pool failure routing regression tests (closes repair-unified-runtime-review-regressions task 5.2)

- [x] 1.1 Add an executor test with `parallel_job_spec(4)` (`crates/arkflow-core/src/executor/tests.rs:2692`) + `FailingProcessor` (`tests.rs:189`) + an error sink: the failed batch and exactly one acknowledgment reach the error sink, and sibling acknowledgments are settled (no leak, no double-ack).
- [x] 1.2 Add the no-error-edge variant: pool with a failing processor and no error edge returns `Err` from `run_graph` and the batch's acknowledgment is aborted.
- [x] 1.3 Run `cargo test -p arkflow-core --lib executor::` and confirm both new tests pass alongside the existing suite.

## 2. Verification-warning assertions

- [x] 2.1 Add hub tests (`crates/arkflow-server/src/hub.rs` test module): with two registered nodes, a successful `job_start` for one while the peer is pending keeps the job's observed state non-terminal (converging); a retryable peer error does not overwrite the healthy peer's observation. Drive states by submitting command results explicitly, mirroring `two_node_job_smoke`.
- [x] 2.2 Add late-event counter assertions (`crates/arkflow-core/src/executor/event_time_gate.rs` tests): exact per-decision counts over multi-row batches for Drop/Route/Update and rows without timestamps, asserted against the runtime counter path (`executor/task.rs:1015-1025`).
- [x] 2.3 Add a checkpoint snapshot-failure test: a failing snapshot backend reports the checkpoint round as failed through the failure reporter (`executor/task.rs:410-418`), the last valid artifact is preserved, and data continues to flow.
- [x] 2.4 Run `cargo test -p arkflow-server --lib hub` and `cargo test -p arkflow-core --lib executor::`; confirm green.

## 3. Spec and record sync

- [x] 3.1 Confirm the `stream-backpressure` delta updates all four requirements and `openspec validate repair-kernel-archive-readiness --strict` passes.
- [x] 3.2 Update `openspec/changes/repair-unified-runtime-review-regressions/verification.md`: mark the task-5.2 CRITICAL resolved by this change with the new test names.
- [x] 3.3 Update `openspec/changes/rebuild-unified-streaming-engine/verification.md`: mark the `stream-backpressure` CRITICAL resolved by this change's delta.
- [x] 3.4 Note the archive order in both records: this change archives before/with `rebuild-unified-streaming-engine` so the spec sync lands in the same lineage step.

## 4. Final verification

- [x] 4.1 `cargo fmt --all -- --check` and `git diff --check` are clean.
- [x] 4.2 `cargo test --workspace --no-fail-fast` passes with only the known Docker-gated `kafka_eos` failures (709+ passed, 4 failed).
- [x] 4.3 `openspec validate repair-kernel-archive-readiness --strict --no-interactive` passes.
