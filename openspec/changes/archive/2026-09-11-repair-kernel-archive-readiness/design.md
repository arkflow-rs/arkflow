## Context

The 2026-09-11 independent verification of the unified-kernel change family
(`rebuild-unified-streaming-engine`, `harden-unified-streaming-runtime`, and
three `repair-*` changes) confirmed the implementation is complete and correct
but left two archive blockers:

- `openspec/specs/stream-backpressure/spec.md` still describes the deleted
  legacy push/pull/notify runtime (in-flight threshold, reorder buffer,
  `next_seq`), and no pending change carries its delta, so archiving the
  lineage would leave acceptance criteria no implementation can satisfy.
- `repair-unified-runtime-review-regressions` task 5.2 is checked but its
  declared worker-pool (parallelism > 1) failure-routing/ack regression tests
  do not exist; the only error-routing test runs `parallelism: 1`
  (`crates/arkflow-core/src/executor/tests.rs:850`), leaving the
  `distributed-job-runtime` scenario "A processor fails in a worker pool"
  unverified.

Unified-kernel backpressure semantics live in
`crates/arkflow-core/src/executor/graph.rs` (bounded flume edges, default 1024)
and the per-chain event loops in `executor/task.rs` (producers await send; a
closed downstream endpoint surfaces `Error::Process`), with the processor
worker pool's bounded queue applying the same backpressure.

## Goals / Non-Goals

**Goals:**

- Rewrite `stream-backpressure` acceptance criteria to kernel semantics so the
  lineage can be archived.
- Add the missing worker-pool failure-routing regression tests.
- Add direct assertions for three cheap, high-value verification warnings:
  multi-assignment aggregation mid-state, exact late-event counters, and the
  checkpoint snapshot-failure path.

**Non-Goals:**

- Any runtime behavior change or configuration-schema change.
- Closing the remaining scenario-level coverage warnings and doc/marker
  cleanups (see the proposal's non-goals).

## Decisions

### Decision 1: Carry the `stream-backpressure` delta in a new repair change

Alternatives: (a) reopen the completed `rebuild-unified-streaming-engine`
change and add the delta there; (b) hand-edit `openspec/specs/` during archive;
(c) a new change.

The repo precedent is a repair change per review/verification finding round;
the rebuild change is complete and its task 6.3 checkbox becomes true via this
change's delta rather than being re-edited in place. Hand-editing main specs
bypasses the delta workflow. Archive order: this change before/with
`rebuild-unified-streaming-engine`, so `stream-backpressure` is synced in the
same lineage step that adds the kernel capability.

### Decision 2: Keep the four existing requirement names; rewrite bodies only

MODIFIED deltas must match the existing `### Requirement:` header exactly. A
RENAMED split would lose traceability against the legacy acceptance criteria,
and the four names ("In-flight messages stay bounded", "Backpressure release
is signal-driven", "Liveness under input end and cancellation",
"Ordered-output semantics unaffected by the backpressure mechanism") still
describe valid concerns under kernel semantics. A cosmetic rename can be its
own later archival step.

### Decision 3: Tests pin current behavior; a failing test is a finding, not a license to change behavior

The worker-pool routing, aggregation, counter, and snapshot-failure paths are
implemented and were code-verified during the audit. These tests come to pin
them. If a new test fails against HEAD, stop and record the divergence as a
defect with its own repair scope instead of silently adjusting behavior inside
an archive-readiness change.

### Decision 4: Test placement mirrors existing structures

- Worker-pool failure routing: `crates/arkflow-core/src/executor/tests.rs`,
  reusing `parallel_job_spec(4)` (`tests.rs:2692`), `FailingProcessor`
  (`tests.rs:189`), and the error-sink pattern of
  `processor_failure_uses_error_output_without_receiving_successes`
  (`tests.rs:850`). Assert the failed batch and exactly one acknowledgment
  reach the error sink and sibling acknowledgments are settled; the
  no-error-edge variant returns `Err` and the acknowledgment is aborted.
- Multi-node aggregation mid-state: `crates/arkflow-server/src/hub.rs` tests.
  Two registered nodes; a successful `job_start` for one while the peer is
  pending keeps the observed state non-terminal; a retryable peer error must
  not overwrite the healthy peer's observation. Drive states by submitting
  command results explicitly, mirroring `two_node_job_smoke`.
- Late-event counters: `crates/arkflow-core/src/executor/event_time_gate.rs`
  tests; assert exact per-decision counts over multi-row batches
  (Drop/Route/Update, plus rows without timestamps) against the runtime
  counter path (`task.rs:1015-1025`), not the unwired `EventTimeMetrics`.
- Snapshot failure: executor test with a failing snapshot backend; the
  checkpoint round reports failure through the failure reporter
  (`executor/task.rs:410-418`), the last valid artifact is preserved, and data
  flow continues.

### Decision 5: Update the two verification records when the fixes land

The CRITICAL findings in
`openspec/changes/rebuild-unified-streaming-engine/verification.md` and
`openspec/changes/repair-unified-runtime-review-regressions/verification.md`
get a resolution note referencing this change, so the archived lineage carries
accurate records.

## Risks / Trade-offs

- [Spec over-claims behavior] → the delta's four requirements match behaviors
  the rebuild/harden suites already exercise (bounded-edge, drain,
  cancellation, ordering tests in `executor/tests.rs`); archive needs the
  delta to be satisfiable, which the existing suites demonstrate.
- [Tests couple to internal helpers and ossify] → acceptable: they are in-crate
  unit tests beside the existing pool tests, and helper reuse is the
  established pattern (`parallel_job_spec` already serves the topology test).
- [Hub mid-state test is timing-sensitive] → drive state transitions by
  submitting command results explicitly instead of sleeping on reconciliation
  timers, as `two_node_job_smoke` does.
- [Requirement names are now legacy-flavored under kernel semantics] →
  accepted for traceability (Decision 2).
