# Independent verification record (2026-09-11)

Independent audit against the v1 working tree at HEAD `b17690d` — the commit
that landed this change (agent-assisted; completeness / correctness /
coherence). All 23 tasks were verified individually (not sampled). No cargo
runs; coding baseline is the full-workspace run of 2026-09-11
(`cargo test --workspace --no-fail-fast`: 709 passed, 4 failed — all four
`kafka_eos` broker tests are Docker-gated on this host). `openspec validate
--strict` passes; the proposal's "no new dependencies" claim holds (no
Cargo.toml changes in the commit).

## Summary

| Dimension | Result |
|-----------|--------|
| Completeness | 23/23 tasks have implementation evidence; 17/17 requirements implemented |
| Correctness | 17/17 requirements have matching implementations; 40 scenarios — ~30 with direct tests, 6 partially covered, 4 implementation-only (see warnings) |
| Coherence | design.md decisions 1-6 consistent with the implementation; 2 SUGGESTIONs |

Note: the same commit (`b17690d`) also carries artifacts for
`repair-unified-runtime-review-followups`; the two changes overlap on some
tasks (partitioned edge routing, processor-pool joins) and can be archived
together.

## Findings

### WARNING

All warnings are test-coverage gaps; the implementations exist and are
correct.

1. **event-time-processing "Processing-time window receives event-time input"
   has no test.** Implementation:
   `crates/arkflow-core/src/executor/graph.rs:1030-1042` (only
   `trigger == Watermark` collects timings). *Recommendation*: graph test
   with an event-time source + ProcessingTime window, assert future-timestamp
   rows are emitted immediately, not held.
2. **stream-runtime-control "Shutdown times out" has no test.**
   Implementation: `crates/arkflow-core/src/runtime.rs:971-988`
   (timeout → abort → `Error::Timeout`) and `runtime.rs:946-969`
   (`settle_detached_task` converges Stopping/Restarting to
   Failed/Stopped). *Recommendation*: injectable/shortened timeout test.
3. **control-plane-hub "A command lease expires" has no test.**
   Implementation: `crates/arkflow-server/src/hub.rs:1908-2042` (expiry →
   `TimedOut` + `next_retry_at_ms`; job commands via `reconcile_job`, other
   commands re-enqueued). *Recommendation*: `lease_ttl_ms = 1` test.
4. **control-plane-hub "A late result arrives" has no negative test.**
   Implementation: `hub.rs:2348-2364` (generation mismatch or terminal state
   returns the stored record without overwriting). *Recommendation*: submit a
   `CommandResult` with a stale generation and assert the state is unchanged.
5. **control-plane-reconciliation "Placement moves between nodes" has no
   test.** Implementation: `hub.rs:503-614` (`historical_nodes` diff →
   `job_stop` to old nodes; `assignments_for_nodes(…, job.generation)` starts
   on new ones).
6. **input-durability "Later source acknowledgement fails" has no test.**
   Implementation: `crates/arkflow-core/src/wal/mod.rs:452-530` (`last_error`
   fence, "blocked by an earlier source failure", cursor compensation).
7. **keyed-state-backend "Expired state is measured" — the metrics half has
   no direct assertion.** Expired entries are excluded from `metrics()`
   (`crates/arkflow-core/src/state.rs:922-947`); the budget half is tested
   (`state.rs:1088`). *Recommendation*: assert `metrics().keys/bytes` exclude
   an expired entry.

### SUGGESTION

8. Internal marker strings are duplicated as literals in three places:
   `__arkflow_late_window_ends` (`executor/event_time_gate.rs:971` and
   `executor/window.rs:898`), `__arkflow_late_window_updates`
   (`event_time_gate.rs:1047`, `window.rs:902`), and
   `__arkflow_late_event_route/_update` (`executor/task.rs:1049,1066,1074`;
   `window.rs:1387,1614`). Consider a shared constant module to prevent
   drift.
9. The "One partition becomes idle" observation identifies topic+partition in
   the data model (`crates/arkflow-core/src/event_time.rs:209-211`,
   `executor/event_time_gate.rs:300-337`) but is not exposed via logs or
   metrics.
10. "Topics reuse a partition number" is structurally guaranteed by
    `BTreeMap<EventTimePartition, …>` (`event_time.rs:116`) and only
    indirectly tested (`event_time_gate.rs:1541`); a direct
    topic-a/0 + topic-b/0 assertion would close it.

## Final assessment

**No critical issues. 7 warnings to consider. Ready for archive (with noted
improvements).** One implementation caveat recorded by the audit:
`input-durability`'s replay source-position ack reconstruction
(`crates/arkflow-core/src/executor/stream_adapter.rs:358-402`) depends on a
real Kafka connector and is subject to the known Docker limitation.
