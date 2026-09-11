## 1. Durable acknowledgement frontier

- [x] 1.1 Audit the WAL-backed fan-out acknowledgement path and introduce or reuse a per-input contiguous frontier keyed by the original WAL sequence; add a regression where N+1 completes before N and verify the persisted cursor remains before N.
- [x] 1.2 Make restored WAL positions seed the same frontier used by child acknowledgements and verify duplicate, retry, and immediate-checkpoint behavior without cursor regression.
- [x] 1.3 Ensure `CommitOnAck` and fan-out retry paths retain or recreate their staged state transaction after compensation; add a transient-failure retry test proving the mutation is applied exactly once.

## 2. Ordered state and window finalization

- [x] 2.1 Reorder fired-window composite acknowledgements so journal/state finalization succeeds before source and WAL acknowledgements; add a failure test proving the source remains replayable when state apply fails.
- [x] 2.2 Add per-key state journal versioning or serialized finalization so a failed older transaction cannot undo a later commit; cover both isolated rollback and conflicting rollback.
- [x] 2.3 Extend journal undo snapshots to preserve TTL/expiration metadata and add a TTL compensation regression.

## 3. Physical-partition event time

- [x] 3.1 Trace connector delivery metadata for multiplexed physical partitions and carry the physical partition into event-time gate observations while retaining the explicit task-partition fallback for non-multiplexed inputs.
- [x] 3.2 Fix watermark tracker activation/recomputation so the minimum active partition progress is established before global monotonic advancement; add a first-observation slower-partition test.
- [x] 3.3 Ensure window watermark state and restore use the per-delivery physical partition for single-task multi-partition inputs; add a Kafka-style partition 0/1 late-event regression.
- [x] 3.4 Classify sliding-window late events independently for every containing window and add a non-divisible window regression where one membership is expired while another remains open.

## 4. Event-time validation and metrics

- [x] 4.1 Attach a dedicated invalid-timestamp marker to routed null/invalid rows without removing the late-event marker from genuinely late rows.
- [x] 4.2 Count every late and invalid row in runtime metrics for Drop, Route, and Update outcomes, including multi-row batches; add exact counter assertions.
- [x] 4.3 Reject an explicitly configured non-empty `value_fields` list when any field is absent, while preserving count fallback only for an actually empty list; add validation and runtime tests.

## 5. Worker-pool failure routing

- [x] 5.1 Preserve `ProcessChainError::Processor` batch, acknowledgement, and failure details in processor worker pools and route them through the configured error/DLQ edge exactly as the single-worker path does.
- [x] 5.2 Add worker-pool error-output and acknowledgement regression tests, including the no-error-edge terminal failure behavior.

## 6. Checkpoint task membership and format

- [x] 6.1 Derive local checkpoint manifest membership from the complete logical plan while retaining execution-chain state mappings for fused stateless processors; add a restart regression for a fused pipeline.
- [x] 6.2 Preserve the configured Job state format in stateless/empty local checkpoint reports and verify a non-1 format survives write and recovery selection.
- [x] 6.3 Run checkpoint compatibility, task-set, and restore-before-read tests to confirm the corrected local manifest still rejects missing/extra tasks and accepts compatible savepoints.

## 7. Agent restart reconciliation

- [x] 7.1 Report the process boot identity established at Agent registration rather than the session token, and invalidate or distinguish old successful start operations for a fresh process.
- [x] 7.2 Add an Agent restart regression with an empty local Job runtime and durable desired-running state, proving reconciliation redispatches the missing Job while stale reports remain fenced.

## 8. Documentation and verification

- [x] 8.1 Update the affected runtime, checkpoint, event-time, and Agent documentation with contiguous acknowledgement, physical-partition watermark, rollback, and restart semantics.
- [x] 8.2 Run focused core, plugin, and server tests for all review regressions, then run `cargo fmt --all -- --check`, `git diff --check`, and strict OpenSpec validation.
- [x] 8.3 Run the workspace regression suite, record any Docker/MinIO/Pulsar environment-dependent failures separately, inspect the final worktree, and report implementation status. The full suite's Kafka EOS integration target requires `/var/run/docker.sock`; the remaining workspace suite passes without that external target.
