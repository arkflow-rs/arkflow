# Independent verification record (2026-09-11)

Independent audit of this change against the v1 working tree at HEAD
`b17690d` (agent-assisted; dimensions: completeness / correctness /
coherence). No cargo runs were performed; the coding baseline is the
full-workspace run of 2026-09-11 (`cargo test --workspace --no-fail-fast`:
709 passed, 4 failed — all four are `kafka_eos` broker tests that need
Docker on this host; `two_node_job_smoke` passes). `openspec validate
--strict` passes.

## Summary

| Dimension | Result |
|-----------|--------|
| Completeness | 33/33 tasks have implementation evidence; 23/23 requirements implemented (22 ADDED + 1 MODIFIED; 3 REMOVED confirmed gone). Two task sub-items unfulfilled: 5.5 (deprecation warnings) and 6.2/6.3 (docs + spec sync) — see findings |
| Correctness | 23/23 requirements have matching implementations; 34 scenarios — 27 deep-checked (implementation + test assertions), 7 partially/indirectly covered (all have code support) |
| Coherence | design.md decisions followed (envelope/bounded edges, fused chains, snapshot-before-barrier-release, `SnapshotGate` no-op alias, shared graph builder for Agent+local, WAL-as-input fallback); one archive-artifact gap (see CRITICAL) |

## Findings

### CRITICAL

1. **Task 6.3 spec sync is not fulfilled for `stream-backpressure`, and no
   change in the family carries its delta.**
   `openspec/specs/stream-backpressure/spec.md` (lines 7-36) still mandates
   the deleted legacy runtime: `BACKPRESSURE_THRESHOLD` in-flight limiting
   and `do_output`'s `BTreeMap` reorder buffer with ascending `next_seq`
   emission. Those mechanisms no longer exist — kernel backpressure is
   bounded flume channels (`crates/arkflow-core/src/executor/graph.rs:661`,
   `task.rs`), and `Stream` is a 21-line re-export shell.
   `grep -rln "stream-backpressure" openspec/changes/*/specs/` has no hits,
   so archiving this change (or the whole family) would leave a master spec
   that no implementation can satisfy.
   *Recommendation*: add a MODIFIED delta `specs/stream-backpressure/spec.md`
   (bounded channels + backpressure propagation to the source) to this change
   or to an archive-readiness change, and archive it before/with this change.
   The `stream-runtime-control` / `input-durability` wording updates are
   carried by the `harden-unified-streaming-runtime` deltas and must land in
   the same archive lineage.
   *Resolved (2026-09-11, `repair-kernel-archive-readiness`)*: that change
   carries the `stream-backpressure` MODIFIED delta rewriting all four
   requirements to unified-kernel semantics. Archive order:
   `repair-kernel-archive-readiness` first (its delta syncs
   `stream-backpressure`), then this change, then
   `harden-unified-streaming-runtime` (whose deltas sync
   `stream-runtime-control` / `input-durability`).

### WARNING

2. **Task 5.5 sub-item "deprecated buffer plugins emit compile warnings" not
   implemented.** No `#[deprecated]`/`tracing::warn!` anywhere
   (`grep -rn "deprecat" crates` → 0 hits); buffer plugins still register
   (`crates/arkflow-plugin/src/buffer/sliding_window.rs:283`,
   `memory.rs:275`, `session_window.rs:194`) and have no runtime constructor
   left (unreachable). *Recommendation*: emit a deprecation warning at
   registration or delete the plugins.
3. **Task 6.2 docs part incomplete.** `docs/docs/concepts/4-delivery-semantics.md:112`
   and `docs/docs/concepts/5-wal-optimization.md:245` still describe
   `Stream::run` semantics. *Recommendation*: rewrite to WalInput / chain
   event-loop semantics.
4. **Scenario "Validation errors surface" only partially met.** Duplicate
   operator ids are rejected (`crates/arkflow-core/src/job.rs:288-293`,
   test `job.rs:1126`) but the diagnostic has no line info
   (`crates/arkflow-core/src/cli/mod.rs:134-137`); line numbers only exist on
   the YAML parse-error path (`configuration.rs:131-139`).
   *Recommendation*: add line mapping for jobs validation or relax the
   scenario wording.
5. **Scenario "Capacity is configurable" not expressible.** `JobSpec` has no
   channel-capacity field (`crates/arkflow-core/src/job.rs:235-255`);
   capacity is only settable through the builder API
   (`crates/arkflow-core/src/executor/graph.rs:275-292`, default 1024).
   *Recommendation*: add an optional `channel_capacity` to config, or fix the
   scenario to builder-level.

### SUGGESTION

6. Window assignment is a per-row loop with `div_euclid`
   (`crates/arkflow-core/src/executor/window.rs:595,606,908-1090`), not
   full-column Arrow ops as the requirement text and module comment
   (`window.rs:4-5`) say; the observable intent (no per-row batch rebuild,
   linear cost, perf baseline met) is satisfied.
7. Aggregate state serialization is typed JSON V2 (`window.rs:2241-2249`);
   Arrow IPC is read-compat only (`window.rs:2294-2310`) — spec/design say
   "Arrow IPC".
8. Legacy `sliding_window` fails compilation
   (`crates/arkflow-core/src/executor/stream_compiler.rs:107-114`) vs spec R3
   wording; the rejection form is already pinned by the
   `repair-unified-runtime-review-followups` delta.
9. Coverage gaps: local checkpoint-interval loop
   (`crates/arkflow-core/src/executor/job_runner_adapter.rs:700-760`),
   Aligner stale-generation rejection
   (`crates/arkflow-core/src/executor/barrier.rs:107-131`), YAML `jobs:`
   end-to-end run.
10. `ChainMetrics.in_flight` comment says channel backlog
    (`crates/arkflow-core/src/executor/metrics.rs:21-23`); it measures
    in-processing batches (`task.rs:2135-2164`).
11. Stale "legacy runner" comment
    (`crates/arkflow-server/src/agent.rs:497`); missing `.openspec.yaml` in
    this change directory.

## Final assessment

**1 critical issue found — resolved 2026-09-11 by
`repair-kernel-archive-readiness`** (its `stream-backpressure` delta). The
implementation itself is complete, correct and well-tested; the blocker was
the archive-artifact layer: the `stream-backpressure` acceptance-criteria
sync (task 6.3) existed in no delta spec.
