## Why

The 2026-09-11 max-effort code review of `71e408b`+`f4bed74` (15 confirmed findings) exposed two systemic problems and a set of smaller confirmed defects. Systemic: (1) chained archiving applies each delta's `MODIFIED` section as a whole-requirement replacement, so scenarios that earlier deltas added were silently dropped from master specs when a later delta rewrote the same requirement — including the worker-pool failure SHALL that anchors `71e408b`'s own new tests (`openspec/changes/archive/2026-09-11-repair-unified-runtime-review-regressions/specs/distributed-job-runtime/spec.md:21,28` vs `openspec/specs/distributed-job-runtime/spec.md:20-29`), six event-time scenarios, four ack-gap scenarios, and five control-plane scenarios; (2) the newly archived `stream-backpressure` spec's substantive promise is violated by the pooled execution path: the worker pool's result channel is unbounded (`crates/arkflow-core/src/executor/task.rs:1538`), so a slow downstream accumulates processed results without bound and backpressure never reaches the source when `thread_num > 1`. Smaller confirmed defects: a self-contradictory scenario in the rewritten backpressure spec (`openspec/specs/stream-backpressure/spec.md:28`), two unsatisfiable configurability scenarios, stale premises in `input-durability`/`exactly-once-output`, and seven test-quality gaps (unobservable abort, untested siblings path, missing terminal-half assertions, three missed counter branches, zero coverage of the runtime `late_events` metric, two tautological assertions, dead `_retryable` closure).

## What Changes

- **Restore lost spec content**: re-add the silently dropped scenarios and SHALL clauses to the master specs — `event-time-processing` (6 scenarios: slower-partition-first, multiplexed partitions, non-zero partition watermark restore, Update-corrects-emitted-window, sliding per-membership lateness, dropped-row counting), `distributed-job-runtime` (worker-pool failure SHALL + scenario), `input-durability` (4 ack scenarios: out-of-order gap, fan-out gap, duplicate-child idempotency, restored-cursor survival), `checkpoint-recovery` (pending-ack-at-barrier), `control-plane-reconciliation` (3 trigger scenarios), `control-plane-fleet` (new-session report acceptance).
- **Fix pooled-path backpressure** (**BREAKING**: none — behavior change brings the implementation into compliance with the just-archived spec): bound the worker-pool result channel so workers apply backpressure when the downstream edge is full, keeping drain/EOS liveness; add a pool-path backpressure regression test (`parallelism > 1`, slow sink, bounded source reads).
- **Fix spec wording defects**: the backpressure EOF scenario's unsatisfiable WHEN; the `Capacity is configurable` scenarios in `unified-execution-kernel` and `async-checkpoint-barriers` (reworded to builder-level/fixed-cap semantics — no new config surface); the stale buffer-based premises in `input-durability`'s windowing clause and `exactly-once-output`'s transaction-boundary requirement.
- **Harden tests**: abort-counting ack so the no-error-edge pool test observes `abort()`; a siblings-topology pool failure test; hub multi-node terminal-state assertions (all-success → running, complete-set permanent failure → failed); the three missed late-counter branches; a graph-level `kernel.late_events` metric assertion; replace the two tautological assertions in the snapshot-failure test; delete the dead `_retryable` closure and correct the hub test's mechanism narrative.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `distributed-job-runtime`: restore the worker-pool failure-retention SHALL + scenario; add a pooled-path bounded-drain scenario to the backpressure requirement.
- `event-time-processing`: restore six lost scenarios and the lateness-behavior body clauses (per-membership classification, metrics counting, late-Update reopening).
- `input-durability`: restore four lost ack scenarios; reword the windowing-compatibility requirement to unified-kernel semantics.
- `checkpoint-recovery`: restore the pending-acknowledgement-at-barrier scenario.
- `control-plane-reconciliation`: restore three lost reconciliation-trigger scenarios.
- `control-plane-fleet`: restore the new-session report-acceptance scenario.
- `stream-backpressure`: fix the EOF scenario's WHEN to be satisfiable.
- `unified-execution-kernel`: reword `Capacity is configurable` to builder-level semantics.
- `async-checkpoint-barriers`: reword the alignment-cap scenario to the fixed cap.
- `exactly-once-output`: reword the transaction-boundary premise from buffer plugins to actual aggregation points.

## Impact

- Runtime: `crates/arkflow-core/src/executor/task.rs` (worker-pool result channel + drain/flush paths) — the only production code change; everything else is tests and specs.
- Tests: `crates/arkflow-core/src/executor/tests.rs`, `executor/event_time_gate.rs`, `crates/arkflow-server/src/hub.rs`.
- Specs: ten master specs updated via this change's deltas at archive time.
- Unblocks: the v1 → main PR can cite a clean max-effort review; future audits no longer re-flag the same spec gaps.

## Non-goals

- No change to the archive tooling/workflow itself (the last-writer-wins replacement behavior is an OpenSpec CLI process concern; we repair the content, not the tool).
- No new `channel_capacity` config surface (scenarios are reworded to match reality; the knob can be added later if a user asks).
- Test cleanup items confirmed by the review but not load-bearing (pool-test setup deduplication, `ToggleSnapshotBackend`/`FailingBackend` merge, hub test boilerplate helpers, storage-actor removal, gate-test parameterization, 1s lease TTL).
- The five master-spec `Purpose: TBD` placeholders and the missing `.openspec.yaml` in the rebuild archive directory.
- Docs pages still describing `Stream::run` (carried from the earlier verification leftovers list).
