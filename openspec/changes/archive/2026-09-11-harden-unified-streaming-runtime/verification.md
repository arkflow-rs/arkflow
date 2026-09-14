# Task verification map (task 8.4)

Every task below maps to a passing test or explicit verification command run
in this implementation session.

| Task | Verification |
|------|--------------|
| 1.1 CommitFrontier/CheckpointCut | `cargo test -p arkflow-core --lib executor::commit` (8 tests: in-order/out-of-order/duplicate/failed acks, seed, seal, delivery anchor) |
| 1.2 State mutation journal | `executor::state_journal` (9 tests) + `stateful::failed_sink_write_does_not_persist_a_replayed_increment_twice` |
| 1.3 Barrier seal + snapshot-before-release | `multi_input_barrier_seals_one_acknowledged_cut` (post-barrier data never leaks; pre-barrier included) |
| 1.4 Pre-cut transaction wait | `barrier_waits_for_pre_cut_state_transactions` (slow sink drained into the cut) |
| 1.5 VecAck / finalization failures | `input::vec_ack_propagates_constituent_failures` + `state_journal::failed_apply_compensates_and_stays_staged` |
| 2.1 Resource guard | `resource_guard` (2 tests: dependency-order connect/reverse close; partial-failure cleanup) |
| 2.2 Guard integration + temporaries | `temporaries_connect_before_chains_and_close_at_shutdown`, `sink_connect_failure_fails_start_and_closes_temporaries` |
| 2.3 WalInput::close | `wal_input_close_flushes_pending_and_releases_the_handle`, `wal_input_close_after_partial_startup_releases_the_lock` |
| 2.4 Dry-run WAL release | `durability_enabled_start_reopens_the_same_wal_path` (start/restart on one redb path) |
| 2.5 Startup failure propagation | `dry_run_failure_marks_runtime_failed`, `async_resource_failure_marks_runtime_failed`, `validate_local_job_rejects_unknown_components` |
| 3.1 Single-task subscription | `single_source_task_keeps_all_partition_subscription`, `multiple_source_tasks_receive_explicit_partitions` |
| 3.2 Full assignment restore | `merged_restore_assignment_retains_omitted_partitions` |
| 3.3 Contiguous Kafka frontier | `out_of_order_acknowledgements_wait_for_the_gap`, `restored_positions_seed_the_checkpoint_cursor`, `test_kafka_ack` |
| 3.4 Kafka test suite | `cargo test -p arkflow-plugin --lib input::kafka` (9 passed; broker-gated tests stay ignored-by-env) |
| 4.1 Arrow timestamp units | `timestamp_extraction_accepts_every_arrow_unit_with_nulls`, `second_unit_overflow_is_actionable_not_wrapped` |
| 4.2 Gate re-evaluation | `batch_watermark_advances_before_classifying_current_rows`, `null_timestamps_route_or_drop_without_indefinite_hold`, `update_policy_releases_held_rows_through_watermark` |
| 4.3 Partitioned watermarks | `watermark_restore_uses_the_real_partition` (partition 3 restored, no synthetic 0) |
| 4.4 Sliding enumeration | `sliding_enumeration_tests` (3 tests incl. size=5/slide=2/ts=4 → starts 4,2,0) |
| 4.5 Late Update retention | `late_update_corrects_an_emitted_window`, `emitted_windows_are_retained_until_the_deadline_then_cleaned` |
| 4.6 Typed aggregates | `float64_values_aggregate_with_typed_output`, `float32_values_sum_as_numbers_with_float32_schema`, `unsupported_value_types_are_rejected_explicitly`, `typed_state_survives_serialization_roundtrip`, `legacy_integer_state_migrates_and_legacy_float_state_is_rejected` |
| 4.7 Event-time/window regressions | `cargo test -p arkflow-core --lib executor::` (99 passed) |
| 5.1 Shared compatibility evaluator | `compatibility_tests` (5 tests) + hub/agent wiring + `recovery_selection_requires_matching_job_and_state_versions` |
| 5.2 Stateless format exclusion | `kernel_handle` format aggregation (configured format wins; stateless default never vetoes) — covered by the full executor suite |
| 5.3 Complete planned task set | `repository_refuses_incomplete_planned_checkpoints`, evaluator task-set tests |
| 5.4 Real partition watermark restore | `watermark_restore_uses_the_real_partition` + agent gate_partitions plumbing |
| 5.5 Savepoint/checkpoint tests | `compatibility_tests` (upgrade/downgrade/task-set/checksum) + restore-before-read ordering in `replay_queue_skips_wal_entries_already_covered_by_checkpoint` |
| 6.1 Deep build validator | `validate_local_job_rejects_unknown_components`, `dry_run_failure_marks_runtime_failed` |
| 6.2 thread_num concurrency | `configured_thread_num_runs_ordered_concurrent_processors`, `thread_num_does_not_change_source_partition_topology` |
| 6.3 Schema jobs | `engine_schema_describes_local_jobs` |
| 6.4 Validation resource release | `validation_releases_resources_before_real_startup` |
| 7.1 Assignment aggregation | `two_node_job_smoke` (waits for both peers to succeed) + `periodic_job_reconciliation_retries_a_failed_runtime` (`hub.rs:5587`) + full hub suite (73+ tests); production fn `aggregate_job_observed_state` (`hub.rs:2627`) |
| 7.2 Full-set checkpoint commit | existing `expected_nodes.is_subset(succeeded_nodes)` dispatch gate + hub checkpoint tests |
| 7.3 Session report identity | `new_session_resets_the_report_cursor`, `delayed_report_from_an_old_session_is_ignored` |
| 7.4 Hub/Agent suite | `cargo test -p arkflow-server --lib` (75 passed, incl. `multiple_agent_rollout_smoke_completes_through_commands_and_reports` (`hub.rs:4610`), `checkpoint_completion_after_hub_restart_preserves_metadata` (`hub.rs:3982`)) |
| 8.1 Documentation | docs/docs/concepts/7-distributed-jobs.md (acknowledged cut, Kafka assignment, typed windows, savepoint compatibility, invalid timestamps, failure/readiness) |
| 8.2 Format + targeted suites | `cargo fmt --all -- --check` clean; executor 99, plugin 234, server 75 passed; `git diff --check` clean |
| 8.3 Workspace regression | available workspace suites pass; the full command reaches only the four `kafka_eos` broker tests, which are Docker-gated and fail before test assertions because this host has no `/var/run/docker.sock` (environment-dependent; test file untouched) |
| 8.4 Strict validation | `openspec validate harden-unified-streaming-runtime --strict --no-interactive` → valid |

---

# Independent re-verification (2026-09-11)

Second, independent audit at HEAD `b17690d` (agent-assisted; completeness /
correctness / coherence). All 43 requirements have implementation evidence;
all 91 scenarios have code paths, ~86 with direct test assertions; design
decisions 1-8 followed; `openspec validate --strict` passes. No cargo runs;
coding baseline is the full-workspace run of 2026-09-11 (709 passed, 4 failed
— the `kafka_eos` broker tests are Docker-gated on this host).

## Findings

### WARNING

1. **Multi-assignment aggregation mid-states lack direct multi-node
   assertions.** Converging while a peer is pending
   (`crates/arkflow-server/src/hub.rs:2680-2682`) and retryable peer failure
   keeping the observed snapshot neutral (`hub.rs:2731-2739`) are implemented
   but existing tests cover only all-success
   (`crates/arkflow-server/tests/two_node_job_smoke.rs:250-266`) or
   single-node failure (`hub.rs:5587`). *Recommendation*: hub unit test —
   with 2 registered nodes, submit a successful `job_start` for one; assert
   the job's observed state stays non-running and convergence stays
   converging; then submit a retryable error for one assignment and assert
   the healthy peer's observation is not overwritten as failed.
2. **Checkpoint snapshot failure and stream-level WAL recovery failure have
   code guarantees but no direct assertions.** Drain timeout reports via
   `failure_reporter` (`crates/arkflow-core/src/executor/task.rs:410-418`);
   input errors return `Err` (`task.rs:646-652`) → `checkpoint_barrier`
   fails (`executor/kernel_handle.rs:552`). Checksum rejection is tested
   (`crates/arkflow-core/src/checkpoint.rs:1191`) and WAL read errors are
   tested at the WAL layer (`wal/mod.rs:925`), but no end-to-end test asserts
   "snapshot failure marks the checkpoint failed while keeping the last good
   artifact" or "WalInput read/replay failure prevents the stream from
   entering running state".

### SUGGESTION

3. Stale "legacy runner" comments (`crates/arkflow-server/src/agent.rs:497`,
   `:824`) — the legacy runner (`SingleComputeJobRunner`) is deleted.
4. `hub.rs:3911 recovery_record_is_compatible` inlines the shared
   evaluator's "format equal && version not downgraded" rule as a
   pre-filter; the final gate still goes through
   `agent::recovery_record_is_valid` → `evaluate_recovery_compatibility`
   (`hub.rs:640-648`), so semantics agree today. Consider reusing the shared
   judgement to prevent silent drift.
5. Test-name drift in the task map above — corrected in this pass:
   `checkpoint_completion_after_hub_restart` →
   `checkpoint_completion_after_hub_restart_preserves_metadata`;
   `multiple_agent_rollout_smoke` →
   `multiple_agent_rollout_smoke_completes_through_commands_and_reports`;
   7.1 now cites the covering tests instead of the production fn
   `aggregate_job_observed_state`.

## Final assessment

**No critical issues. 2 warnings to consider. Ready for archive (with noted
improvements).**
