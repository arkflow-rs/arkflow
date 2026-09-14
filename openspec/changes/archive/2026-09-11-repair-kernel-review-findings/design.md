## Context

The 2026-09-11 max-effort review (15 confirmed findings) traced most findings to two root causes:

1. **Chained whole-requirement replacement.** Archiving applies each delta's `MODIFIED` section as a full-requirement replacement. Later deltas in the unified-kernel lineage (`repair-unified-runtime-review-regressions`, `repair-event-time-recovery-review`) rewrote requirements that earlier deltas (`harden`, `regressions`) had just extended, silently dropping their scenarios and SHALL clauses from the master specs. Confirmed losses: 6 scenarios in `event-time-processing`, the worker-pool failure SHALL + scenario in `distributed-job-runtime`, 4 ack scenarios in `input-durability`, 1 in `checkpoint-recovery`, 3 in `control-plane-reconciliation`, 1 in `control-plane-fleet`.
2. **Pooled execution path violates the backpressure contract.** `ProcessorWorkerPool::start` gives workers an unbounded `done` channel (`crates/arkflow-core/src/executor/task.rs:1538`); the chain loop blocks only on the bounded submit queue (`task.rs:1537`). When the reorder collector blocks on a full downstream edge, workers keep draining the submit queue and park results in `done`/`pending` without bound — the source never stops reading for `thread_num > 1`, violating the archived `stream-backpressure` spec's "SHALL await send rather than buffer without bound" clause. The existing backpressure test runs `parallelism = 1`, which never builds a pool.

Plus seven confirmed test-quality findings and four spec-wording defects (unsatisfiable capacity scenarios in `unified-execution-kernel:42-45` and `async-checkpoint-barriers:27-31`, the unsatisfiable EOF WHEN in `stream-backpressure:28`, stale buffer-based premises in `input-durability:58-63` and `exactly-once-output:22`).

## Goals / Non-Goals

**Goals:**

- Restore every silently lost scenario/SHALL clause to the master specs.
- Make the pooled path satisfy the archived backpressure contract, with a regression test on the pool path.
- Fix the four spec-wording defects.
- Close the seven test-quality findings.

**Non-Goals:**

- Changing OpenSpec tooling/workflow (content repair only).
- Adding a `channel_capacity` config surface.
- Non-load-bearing test cleanups (setup dedup, backend-wrapper merge, boilerplate helpers, storage actor, gate-test parameterization, lease TTL).
- `Purpose: TBD` placeholders and the rebuild archive's missing `.openspec.yaml`.

## Decisions

### Decision 1: Restore scenarios onto current master bodies; merge lateness body clauses

The master requirement bodies are the latest intentional wording; only scenarios/SHALL clauses were silently lost. Each delta therefore copies the current master requirement block verbatim and re-adds the lost scenarios. One exception: `event-time-processing`'s "Windows SHALL define lateness behavior" body lost three clauses that later deltas did not intentionally remove — "For sliding windows, each row SHALL be classified against every containing window membership…" and "Runtime metrics SHALL count each late or invalid row…" (regressions delta) and "A late Update within the allowed-lateness deadline SHALL modify the already emitted window result…" (harden delta) — merged into the current body. Alternative (replaying deltas from scratch) rejected: the final bodies contain intentional rewrites (session dynamics) that must be preserved.

### Decision 2: Bound the pool result channel and publish asynchronously

Implementation found the load-bearing pair: (a) `done` becomes `flume::bounded::<(u64, PoolResult)>(parallelism * 2)`, and (b) the workers' `done_tx.send` (a **synchronous, thread-parking** flume call) becomes `send_async(...).await`. (b) is mandatory once (a) lands: a sync send at capacity parks a tokio worker thread, and since the workers, the reorder collector, and the test timers all share the same multi-thread runtime, filling the bound froze the entire runtime in the first attempt (all four tokio workers sampled inside flume's sync send; the collector could never run to drain). With async sends the backpressure chain is purely cooperative: collector blocks in `flush_pool_result` (edge send) → workers await a full `done` → the submit queue fills → the chain loop blocks in `submit()` → the source edge backpressures.

No cancellation select is needed on the worker send: when the collector exits (it selects on cancellation), its `done_rx` is dropped, and flume fails every blocked `send_async` with `SendError`, which the worker's existing error branches already handle by aborting the delivery's acknowledgements and returning. Rationale for the rest:

- With `done` bounded at `≥ parallelism`, a healthy downstream keeps every worker running (each worker holds at most one result at a time), so throughput is unaffected when there is no backpressure.
- EOS/drain liveness is preserved: `drain()` closes the submit queue and waits for workers, whose in-flight results fit the bound (`parallelism * 2` ≥ in-flight per worker), and the collector keeps forwarding as the downstream drains. Verified by the existing pool, ordered-concurrency, and barrier-cut tests.

Alternative rejected: draining `done` into an ever-growing local buffer — same unbounded memory, just moved.

### Decision 3: Reword the unsatisfiable capacity scenarios; no new config surface

`unified-execution-kernel`'s "Capacity is configurable" becomes builder-level ("WHEN a graph is built with a non-default channel capacity / THEN edges are constructed with that capacity, defaulting to 1024"). `async-checkpoint-barriers`' "exceeds the configured cap" becomes "exceeds the alignment buffer's fixed cap". Adding a `JobSpec.channel_capacity` field was rejected: no user has asked for the knob, and the review's concern is unsatisfiable acceptance criteria, not missing configuration.

### Decision 4: Reword stale premises to current mechanisms

- `input-durability`'s "Durability is orthogonal to windowing": the stream compiles window buffers into window operators and instantiates no buffer plugin; reword to "a durable ingest WAL composes with event-time window operators; enabling durability does not change window operator behavior".
- `exactly-once-output`'s "Transaction boundary equals the buffer aggregation unit": the premise becomes "when deliveries are aggregated before one output call — a window operator emission or a batched composite delivery —" and the join-buffer mention is dropped (join buffers now fail compilation). The one-`write_batch`-per-ack-range invariant and all scenarios keep their meaning; the window scenario's mechanism description drops the "buffer" framing.

### Decision 5: Test hardening pins current or newly-fixed behavior; a red test is a finding

The additions: (a) `CountingAck` gains an `abort` counter (override `abort`) so the no-error-edge pool test asserts `aborts == 1`; (b) a siblings-topology pool test — first processor emits multiple outputs (`ProcessResult::Multiple`), second fails, asserting every sibling + the failed delivery reach the error sink exactly once; (c) hub multi-node terminal assertions — complete the existing two-node test to all-success → `running`, and a permanent-failure variant → `failed`; (d) the three missed late-counter branches (Hold-with-exclusions counts, held-release exclusions, `route_late` single-count) via sliding-window gate cases; (e) a graph-level `kernel.late_events` assertion through `KernelJobRunner` + `metrics().snapshot()`; (f) replace the snapshot-failure test's two tautological assertions (drop the `snapshot.verify()` re-check; reword or drop the `checkpoint_failures` claim — the `is_err` assertion carries the contract); (g) delete the dead `_retryable` closure and correct the hub test's comment/name to describe state-driven semantics. If (b) or (d) expose a real divergence, that is recorded as a defect with its own fix inside this change — the scenarios were verified in review, so divergence is unlikely.

### Decision 6: The backpressure spec deltas ride the same change

The `distributed-job-runtime` backpressure requirement gains a pooled-path scenario ("A worker pool drains under a slow downstream") describing the fixed behavior; `stream-backpressure`'s EOF WHEN is reworded to "while edges are at capacity and producers are awaiting send". Both land in master when this change archives.

## Risks / Trade-offs

- [Bounding `done` changes pool timing] → capacity `parallelism * 2` keeps workers unblocked unless the downstream is actually full; the ordered-concurrency and pool-failure tests already in the suite guard regression.
- [Restored scenarios may conflict with future intentional removals] → each delta cites its source delta in the change record; future removals must remove them explicitly rather than by side effect.
- [Worker send wrapped in select adds a cancellation branch per result] → negligible; the branch only fires on shutdown.
- [exactly-once-output rewording touches a spec owned by an archived EOS change] → wording-only; the invariants and scenarios are preserved verbatim where still accurate.

## Open Questions

None — the review verification resolved the behavioral facts; implementation details of Decision 2 are constrained by the deadlock analysis above.
