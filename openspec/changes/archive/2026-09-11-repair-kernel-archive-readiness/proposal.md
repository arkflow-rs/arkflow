## Why

The 2026-09-11 independent verification of the unified-kernel change family (`openspec/changes/rebuild-unified-streaming-engine/verification.md`, `openspec/changes/repair-unified-runtime-review-regressions/verification.md`) confirmed the implementation is complete and correct but found two archive blockers. First, `openspec/specs/stream-backpressure/spec.md:7-36` still mandates the deleted legacy runtime — `BACKPRESSURE_THRESHOLD` in-flight limiting and `do_output`'s `BTreeMap` reorder buffer — while kernel backpressure is bounded execution edges (`crates/arkflow-core/src/executor/graph.rs:661`); no pending change carries a `stream-backpressure` delta, so archiving the lineage would leave acceptance criteria no implementation can satisfy. Second, task 5.2 of `repair-unified-runtime-review-regressions` is checked but its declared worker-pool failure-routing/ack regression tests do not exist: the only error-routing test uses `parallelism: 1` (`crates/arkflow-core/src/executor/tests.rs:850`) and `parallel_job_spec(4)` is used only by a topology test (`tests.rs:2725`), leaving the `distributed-job-runtime` scenario "A processor fails in a worker pool" unverified. Three further verification warnings are cheap, high-value coverage gaps worth closing in the same pass.

## What Changes

- Add a `stream-backpressure` MODIFIED delta rewriting all four requirements to unified-kernel semantics: bounded execution edges with source-propagating backpressure, event-driven release, liveness under EOF/cancellation, and unchanged ordered delivery.
- Add the missing worker-pool (parallelism > 1) regression tests: with an error edge, a failing processor routes the failure and its acknowledgment to the error sink exactly once and sibling acknowledgments are settled; without an error edge, `run_graph` returns `Err` and the acknowledgment is aborted.
- Add direct assertions for three verification warnings: multi-assignment aggregation staying non-terminal while a peer is pending or retryably failed (`crates/arkflow-server/src/hub.rs:2680-2682`); exact late-event counters per decision for Drop/Route/Update/multi-row batches (`crates/arkflow-core/src/executor/event_time_gate.rs:666-679`); checkpoint snapshot failure producing a failed checkpoint while preserving the last valid artifact and keeping data flowing (`crates/arkflow-core/src/executor/task.rs:410-418`).
- Update the two affected verification records so their CRITICAL findings resolve against this change.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `stream-backpressure`: replace all four legacy acceptance requirements (threshold / reorder-buffer / `next_seq` / `Notify` wording) with the unified kernel's bounded-edge semantics. The remaining three workstreams are test-only and change no requirements.

## Impact

- Specs: `openspec/specs/stream-backpressure/spec.md`, rewritten via this change's delta when the lineage is archived.
- Tests only; no runtime behavior change: `crates/arkflow-core/src/executor/tests.rs`, `crates/arkflow-core/src/executor/event_time_gate.rs`, `crates/arkflow-server/src/hub.rs`.
- Unblocks archiving `rebuild-unified-streaming-engine`, `harden-unified-streaming-runtime`, `repair-unified-runtime-review-regressions`, `repair-unified-runtime-review-followups`, and `repair-event-time-recovery-review` in lineage order.

## Non-goals

- No runtime behavior or configuration-schema changes. The worker-pool error routing, aggregation, counter, and snapshot-failure code paths already behave as specified; this change adds their missing tests, not new behavior.
- Do not close the remaining verification warnings (stale `Stream::run` wording in `docs/docs/concepts/4-delivery-semantics.md:112` and `docs/docs/concepts/5-wal-optimization.md:245`, buffer-plugin deprecation warnings, `JobSpec` channel-capacity field, execution-marker string constants, reconnect-cancel test gaps, and the other scenario-level coverage warnings).
- Do not perform the archive, the v1 push, or the v1 → main merge themselves.
