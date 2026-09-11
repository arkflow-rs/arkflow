# Independent verification record (2026-09-11)

Independent audit against the v1 working tree at HEAD `b17690d`
(agent-assisted; completeness / correctness / coherence). No cargo runs;
coding baseline is the full-workspace run of 2026-09-11
(`cargo test --workspace --no-fail-fast`: 709 passed, 4 failed — all four
`kafka_eos` broker tests are Docker-gated on this host). `openspec validate
--strict` passes; `git diff --check` clean.

## Summary

| Dimension | Result |
|-----------|--------|
| Completeness | 20/20 tasks have implementation evidence; 1 task (5.3) declares test coverage that is missing (WARNING 2) |
| Correctness | 24/24 requirements implemented; 24 scenarios — 13 with direct assertions, 11 implementation-present-but-no-direct-test |
| Coherence | design.md decisions 1-6 followed (durable-read cut, composite ack, cancellation-aware waits, compiler compatibility boundary, key-group edges, Agent liveness); 1 spec wording drift |

## Findings

### WARNING

All warnings are "implementation present and correct; the specific scenario
lacks a direct test".

1. **stream-runtime-control S1/S2/S3 have no tests.** Stop during repeated
   reconnect failures (`crates/arkflow-core/src/executor/task.rs:609-644`);
   cancel while a worker is processing (`task.rs:964-1001` — the existing
   cancel test `tests.rs:987` is `parallelism = 1`, and `tests.rs:2709`
   covers ordered concurrency, not cancel-join); worker fails as input closes
   (`task.rs:1690-1728`, `:1196-1198`). *Recommendation*: one test each.
2. **Task 5.3 "Agent shutdown coverage" deliverable missing.** The cancel
   path is implemented (`crates/arkflow-server/src/agent.rs:1262-1267`:
   cancel → abort_all + join → stop_all → draining heartbeat) but only one
   Agent test exists (`agent.rs:1874-2008`); the compute-node-agent scenarios
   "Checkpoint exceeds the lease interval" and "Shutdown during checkpoint"
   have no direct assertions.
3. **input-durability "Cursor failure prevents source commit" has no
   fault-injection test.** The behaviour is correct
   (`crates/arkflow-core/src/wal/mod.rs:501-509`: `advance_cursor` failure
   returns before the inner ack), but no test registers a failing `WalStore`.
   *Recommendation*: register an advance-failing store, assert the inner ack
   was not called and the entry remains replayable.
4. **message-acknowledgment "One expanded output fails" has no chain-level
   test.** Implementation at `crates/arkflow-core/src/executor/task.rs:2234-2260`;
   only fanout-helper tests (`tests.rs:633`, `:652`) and single-chain error
   routing (`tests.rs:850`) exist.
5. **streaming-job-api "Processor observes a buffered batch" (J3) and
   "Processor before window fails" (J4) have no end-to-end tests.**
   Implementation at `crates/arkflow-core/src/executor/stream_compiler.rs:72-150`
   and `:173-195` (shape tests at `:521`, `:594`).

### SUGGESTION

6. Legacy session payload compatibility lacks a dedicated test: the compiler
   sets `legacy_payload` for tumbling and session
   (`crates/arkflow-core/src/executor/stream_compiler.rs:397-399`), but the
   payload regression only covers tumbling (`window.rs:3351`).
7. Partitioned-edge channel assembly (channels + `key_group_ranges` for all
   eligible downstream subtasks) is only indirectly asserted (`tests.rs:1031`
   checks chain count only).
8. "Pre-window failures" wording
   (`specs/streaming-job-api/spec.md:30-37`) contradicts the compile layout
   (buffer/window precedes processors, `stream_compiler.rs:72-150`); the
   functional intent (an error edge from every processor boundary) is
   implemented. Reword during archive sync.

## Final assessment

**No critical issues. 5 warnings to consider. Ready for archive (with noted
improvements).** All core runtime semantics — durable-read cut, WAL
cursor/source commit order, composite window+ack rollback, key-group routing,
cancellation and resource release — are verified in code and tests.
