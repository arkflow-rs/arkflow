# Independent verification record (2026-09-11)

Independent audit against the v1 working tree at HEAD `b17690d`
(agent-assisted; completeness / correctness / coherence). Changed surface
cross-checked via `git show a4a9381` (27 files, ~2257 insertions). No cargo
runs; coding baseline is the full-workspace run of 2026-09-11
(`cargo test --workspace --no-fail-fast`: 709 passed, 4 failed — all four
`kafka_eos` broker tests are Docker-gated on this host). `openspec validate
--strict` passes.

## Summary

| Dimension | Result |
|-----------|--------|
| Completeness | 23/23 tasks checked; 21 deep-checked — task 5.2 has no deliverables (CRITICAL); 4 tasks (4.2/4.3/6.1/6.2) have test sub-items unclosed |
| Correctness | 23/23 requirements have implementation evidence; 50 scenarios — 43 with coverage evidence, 7 lack targeted tests |
| Coherence | design.md decisions 1-6 realized; 1 wording drift (SUGGESTION) |

## Findings

### CRITICAL

1. **Task 5.2 (worker-pool error routing / ack regression tests) has no
   deliverables.** The implementation exists and is symmetric with the
   single-worker path (`crates/arkflow-core/src/executor/task.rs:1788-1810`
   `flush_pool_result`; `1795-1808` routes `ProcessorFailure` to the error
   target), but no test exercises a worker pool (`parallelism > 1`) with a
   failing processor: the only error-routing test
   (`crates/arkflow-core/src/executor/tests.rs:850`,
   `processor_failure_uses_error_output_without_receiving_successes`) uses
   `parallelism: 1` / `max_parallelism: 1`, and `parallel_job_spec(4)`
   (`tests.rs:2692`) is used only by the topology test at `tests.rs:2725`.
   The spec scenario `distributed-job-runtime` "A processor fails in a
   worker pool" (`specs/distributed-job-runtime/spec.md:28-32`) is therefore
   unverified. (Independently re-confirmed during consolidation: single
   `FailingProcessor` use site in `tests.rs` plus one unrelated use in
   `stateful.rs`.)
   *Recommendation*: add ① pool + error edge: the failing batch and its ack
   reach the error sink exactly once; ② pool without an error edge:
   `run_graph` returns `Err` and the ack is aborted. Reuse
   `parallel_job_spec(4)` + `FailingProcessor` + the `tests.rs:850`
   structure.
   *Resolved (2026-09-11, `repair-kernel-archive-readiness`)*: the two
   missing tests now exist —
   `pool_processor_failure_routes_to_error_output_without_successes` and
   `pool_processor_failure_without_error_output_fails_and_aborts`
   (`crates/arkflow-core/src/executor/tests.rs`, built on the shared
   `pooled_failure_job_spec` helper) — so the `distributed-job-runtime`
   scenario "A processor fails in a worker pool" is directly verified.
   Archive order: `repair-kernel-archive-readiness` first, then this change.

### WARNING

2. **Task 4.2 exact counter assertions missing.** Counts are computed per row
   (`crates/arkflow-core/src/executor/event_time_gate.rs:666-679`;
   accumulated into `kernel.late_events` at `task.rs:1015-1025`) but no test
   asserts Drop/Route/Update/multi-row counts; the only late-metric test
   (`crates/arkflow-core/src/event_time.rs:576-583`) covers
   `EventTimeMetrics`, which has no runtime call site.
3. **Task 4.3 test part missing.** A missing non-empty window value field is
   rejected (`crates/arkflow-core/src/executor/window.rs:884-889`; an empty
   list falls back to count at `window.rs:1030`) but no test asserts
   `"window value field '...' is missing"`.
4. **Task 6.1 fused-pipeline restart regression missing.** The manifest is
   written from the full logical task set
   (`crates/arkflow-core/src/executor/job_runner_adapter.rs:104-108`,
   `:834-835`); tests only cover synthetic task-id manifests
   (`crates/arkflow-core/src/checkpoint.rs:1171`, `:1219`) and non-checkpoint
   recovery (`crates/arkflow-server/tests/kernel_job_lifecycle.rs:360`).
   `run_job_with_checkpoints` / `persist_local_checkpoint` /
   `latest_local_checkpoint` have no test callers.
5. **Task 6.2 non-1 state-format write/restore verification missing.** The
   implementation uses the configured format rather than the first report
   (`crates/arkflow-core/src/executor/kernel_handle.rs:217-240`, `:476-499`;
   `job_runner_adapter.rs:159-186`) but every test uses
   `format_version = 1`.
6. **Scenario "Route an invalid timestamp" has no marker assertion.**
   `__arkflow_invalid_timestamp_route` appears only in implementation files
   (`crates/arkflow-core/src/executor/task.rs:1041-1052`,
   `window.rs:1388-1410`); the nearest test
   (`event_time_gate.rs:1656-1677`) asserts Route/Drop behaviour but not that
   routed rows carry the dedicated marker while real late rows keep
   `__arkflow_late_event_route`.

### SUGGESTION

7. design.md decision 3 wording ("window watermark state keyed by
   source/operator/physical partition") differs from the mechanism (tracker
   per physical partition + minimum-of-active downlink,
   `crates/arkflow-core/src/event_time.rs:108-286`,
   `executor/task.rs:429-435`, `:682-715`); behaviour matches all
   scenarios/tests.

## Final assessment

**1 critical issue found — resolved 2026-09-11 by
`repair-kernel-archive-readiness`** (the two worker-pool tests above).
Archive in lineage order with that change first. The code fix behind task 5.2
was already in place and symmetric with the single-worker path; the missing
declared test coverage is now delivered.
