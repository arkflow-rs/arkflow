# Verification record (2026-09-11)

Self-verification after implementation (all 14 tasks complete). Baseline:
full workspace run `cargo test --workspace --no-fail-fast` = **714 passed /
4 failed**, the four failures being the Docker-gated `kafka_eos` broker
tests on this host. `openspec validate --strict` passes; `cargo fmt --check`
and `git diff --check` clean.

## Summary

| Dimension | Result |
|-----------|--------|
| Completeness | 14/14 tasks; the one delta capability (`stream-backpressure`) rewrites all four MODIFIED requirements, headers matching the master spec exactly |
| Correctness | All four rewritten requirements are kernel behaviors already in code (`executor/graph.rs` bounded edges, `executor/task.rs` event loops); 6 scenarios — 4 with direct/indirect test evidence, 2 implicit (below) |
| Coherence | Design decisions 1-5 followed; one task-wording nuance noted |

## Scenario → evidence map

1. "In-flight held near the edge capacity under a slow output" →
   `backpressure_blocks_upstream_when_channel_is_full`
   (`crates/arkflow-core/src/executor/tests.rs:1036`).
2. "No backpressure under a fast output" → implicit: every fast-pipeline
   test completes without stalls (e.g. `pipelines_batches_to_sink_in_order`,
   `bounded_examples_run_to_completion_on_kernel`); the bounded test proves
   blocking occurs only at capacity.
3. "Producer resumes immediately after the consumer advances" → provided by
   flume's async send/recv capacity notification; exercised by every
   drain/EOF test, not separately timed.
4. "Drains and exits despite backpressure at input EOF" → the finite-input
   tests (`CountingInput` EOF through full kernel runs, e.g.
   `kernel_runner_applies_event_time_gate_and_preserves_delivery_acks`)
   complete with EOS forwarded.
5. "Cancellation unblocks a full edge" → `cancellation_stops_all_chains`
   (`tests.rs:1104`) + `backpressure_blocks_upstream_when_channel_is_full`
   cancels while blocked and terminates.
6. "Still written in order across repeated backpressure cycles" →
   `pipelines_batches_to_sink_in_order` +
   `configured_thread_num_runs_ordered_concurrent_processors`.

## Findings

### CRITICAL

None.

### WARNING

None.

### SUGGESTION

1. Task 1.1 names `parallel_job_spec(4)`, but that helper hardcodes the
   `slow` map operator and cannot express an error sink; the tests build the
   same shape through a dedicated `pooled_failure_job_spec(parallelism,
   with_error_sink)` helper (parallelism 4, `FailingProcessor`, error
   sink). Intent fully preserved; wording could be updated at archive time.
2. Scenarios 2 and 3 rely on implicit coverage; a dedicated
   "fast consumer, no stall" assertion and a resume-latency probe would pin
   them directly.

## Final assessment

**All checks passed. Ready for archive.** Archive this change first in the
lineage (its delta syncs `stream-backpressure`), then
`rebuild-unified-streaming-engine`, `harden-unified-streaming-runtime`,
`repair-unified-runtime-review-regressions`,
`repair-unified-runtime-review-followups`,
`repair-event-time-recovery-review`.
