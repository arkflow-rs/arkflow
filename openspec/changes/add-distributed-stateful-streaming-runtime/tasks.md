## 1. Contracts and compatibility boundary

- [x] 1.1 Define versioned Job, JobVersion, Operator, Edge, Task, Subtask, Partition, KeyGroup, and TaskAttempt domain types in `arkflow-core`.
- [x] 1.2 Define Job lifecycle, desired/observed/convergence states and generation-fenced command types without changing existing Stream state types.
- [x] 1.3 Define a Job specification schema for sources, sinks, keys, timestamps, watermarks, parallelism, state, checkpoint policy, and recovery policy.
- [x] 1.4 Add validation errors and compatibility tests for invalid Job IDs, missing keys, unsupported time semantics, and duplicate operator identities.
- [x] 1.5 Add an explicit adapter boundary that reuses existing Input, Output, Processor, Arrow batch, and connector builders without converting legacy YAML Streams.

## 2. Partitioned Job runtime

- [x] 2.1 Implement immutable Job plan and physical-plan types with stable operator and edge IDs.
- [x] 2.2 Implement deterministic key-group and partition assignment for a single Compute node.
- [x] 2.3 Implement task-attempt lifecycle, generation fencing, cancellation, and bounded backpressure for the Job runtime.
- [x] 2.4 Add a single-Compute Job runner that executes a partitioned plan using existing component adapters.
- [x] 2.5 Add unit tests for plan identity, stale assignment rejection, task cancellation, and downstream backpressure.

## 3. Event-time and watermark semantics

- [x] 3.1 Add source time configuration and timestamp extraction interfaces.
- [x] 3.2 Implement per-partition watermark tracking, minimum-progress aggregation, and idle-partition handling.
- [x] 3.3 Implement window closure and allowed-lateness decisions for tumbling, sliding, and session-compatible operators.
- [x] 3.4 Add late-event routing/drop/update behavior and stable metrics for watermark lag and late records.
- [x] 3.5 Add deterministic tests for out-of-order events, idle partitions, watermark advancement, and late-event boundaries.

## 4. Keyed state backend

- [x] 4.1 Define the `StateBackend` trait for keyed namespaces, get/update/delete, iteration, TTL, snapshot, restore, and metrics.
- [x] 4.2 Benchmark and select the initial embedded KV implementation using representative aggregate, window, and Join workloads.
- [x] 4.3 Implement local state namespaces, disk-backed working state, memory limits, state-size accounting, and clean shutdown.
- [x] 4.4 Add state format/version metadata and compatibility checks for restore and migration.
- [x] 4.5 Integrate keyed aggregate and window operators with the state backend.
- [x] 4.6 Add tests proving key isolation, namespace isolation, TTL behavior, spill/reopen behavior, and incompatible-state rejection.

## 5. Checkpoint and savepoint recovery

- [x] 5.1 Define checkpoint barrier, source position, watermark, task assignment, state manifest, checksum, and checkpoint status types.
- [x] 5.2 Implement checkpoint coordinator and task acknowledgements for the single-Compute runtime.
- [x] 5.3 Implement object-store checkpoint manifests and state-file upload/download using the existing object-store abstractions where applicable.
- [x] 5.4 Implement checkpoint validation, retention, last-valid selection, and cleanup of incomplete snapshots.
- [x] 5.5 Implement restore-before-input processing and replay from checkpoint source positions.
- [x] 5.6 Implement savepoint create, inspect, restore, delete, and compatibility checks.
- [x] 5.7 Add failure-injection tests for snapshot failure, checksum mismatch, process restart, object-store retry, and deterministic replay.

## 6. Hub–Agent Job control plane

- [x] 6.1 Add durable Job, JobVersion, TaskAssignment, Checkpoint, Savepoint, and JobObservation storage records and migrations.
- [x] 6.2 Extend Hub APIs for Job submission, validation, plan inspection, start/stop/restart, checkpoint, savepoint, and status.
- [x] 6.3 Extend Agent registration and reports with Job runtime, state backend, checkpoint, and protocol capabilities.
- [x] 6.4 Implement Job desired/observed/convergence reconciliation with generation fencing and retry classification.
- [x] 6.5 Implement single-Hub multi-Compute task assignment, task replacement, and bounded rebalancing.
- [x] 6.6 Add durable recovery tests for Hub restart, Agent reconnect, stale task reports, failed checkpoints, and unfinished Job intents.

## 7. SQL-first Job API and Rust extensions

- [x] 7.1 Define SQL DDL extensions for source, sink, key, timestamp, watermark, window, and recovery configuration.
- [x] 7.2 Compile validated SQL into the versioned Job logical and physical plan.
- [x] 7.3 Implement explain output covering operators, partitions, state, connectors, and checkpoint policy.
- [x] 7.4 Define Rust UDF/UDAF metadata for determinism, statefulness, async behavior, and checkpoint compatibility.
- [x] 7.5 Add SQL validation and plan-generation tests for supported and rejected constructs.

## 8. Observability, documentation, and benchmarks

- [x] 8.1 Add bounded metrics for watermark lag, state size, checkpoint duration, checkpoint failures, recovery progress, task pressure, and partition health.
- [x] 8.2 Add Console/API views for Job state, task assignments, checkpoint history, savepoints, and recovery progress.
- [x] 8.3 Add a reproducible benchmark suite for keyed aggregation, windows, Join state, checkpoint overhead, and partition scaling.
- [x] 8.4 Document Job API, event-time semantics, state lifecycle, recovery behavior, and legacy Stream compatibility.
- [x] 8.5 Run focused tests, workspace tests, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, `git diff --check`, and strict OpenSpec validation. Workspace Kafka EOS tests remain environment-blocked when Docker is unavailable.

## 9. Review remediation

- [x] 9.1 Preserve TTL metadata through state snapshots/restores and make keyed counter updates atomic.
- [x] 9.2 Preserve distinct checkpoint watermark partitions and persist checkpoint/savepoint artifact records before API success.
- [x] 9.3 Reject Jobs without executable sources/sinks and execute Job plans according to DAG edges.
- [x] 9.4 Dispatch fenced Job lifecycle commands and task assignments from Hub to compatible Compute Agents.
- [x] 9.5 Execute Job commands on Agents with generation-aware runtime lifecycle and report observed convergence.
- [x] 9.6 Add regression tests for all reviewed failure modes and rerun repository validation.

## 10. Follow-up review remediation

- [x] 10.1 Dispatch per-node task assignments and restrict Agent execution to assigned tasks.
- [x] 10.2 Execute checkpoint/savepoint commands through the coordinator, persist state snapshots/manifests, and aggregate multi-node completion.
- [x] 10.3 Report asynchronous Job runtime termination back to Hub observations.
- [x] 10.4 Enforce state budgets for atomic counter updates and fence acknowledgements to the active checkpoint barrier.
- [x] 10.5 Add regression coverage and rerun focused repository validation.

## 11. Follow-up review remediation

- [x] 11.1 Co-locate connected operator components when no cross-node transport is available.
- [x] 11.2 Bind source readers to assigned task partitions and reject unsupported parallel source execution.
- [x] 11.3 Scope checkpoint completion aggregation by checkpoint ID.
- [x] 11.4 Add regression coverage for edge locality, source partition ownership, and checkpoint identity.

## 12. Follow-up review remediation

- [x] 12.1 Apply the selected checkpoint or savepoint before Agent Job startup and restore operator state before input processing.
- [x] 12.2 Persist checkpoint manifests and state snapshots through the configured shared object-store URI.
- [x] 12.3 Capture and restore Kafka topic/partition offsets through the Input checkpoint position contract.
- [x] 12.4 Add recovery, shared-artifact, and connector-position regression coverage.

## 13. Follow-up review remediation

- [x] 13.1 Preserve task-local Kafka partition assignments during recovery.
- [x] 13.2 Aggregate per-agent checkpoint manifests before publishing the shared artifact.
- [x] 13.3 Await the previous Job generation before starting its replacement.
- [x] 13.4 Exclude expired TTL entries from state scans and add regression coverage.

## 14. Follow-up review remediation

- [x] 14.1 Revalidate deserialized Job IDs and default/reject invalid state format versions.
- [x] 14.2 Build processor and sink instances per assigned task and route edges by task identity.
- [x] 14.3 Stop and await the previous runtime before opening replacement state or connectors.
- [x] 14.4 Add validation and task-isolation regression coverage.

## 15. Follow-up review remediation

- [x] 15.1 Route unpartitioned DAG edges to every assigned downstream subtask.
- [x] 15.2 Apply compare-and-swap generation updates for concurrent Job actions.
- [x] 15.3 Reject both stale and future-generation Job observations.
- [x] 15.4 Add broadcast-routing and generation-fencing regression coverage.

## 16. Follow-up review remediation

- [x] 16.1 Inject task-scoped state into stateful Job processors and persist keyed updates.
- [x] 16.2 Apply timestamp extraction, watermark tracking, window decisions, and late-event policy in the runner.
- [x] 16.3 Bind state snapshots to Agent nodes and restore only the local snapshot set.
- [x] 16.4 Await cancelled Job runtimes before restart replacement and add recovery isolation coverage.

## 17. Follow-up review remediation

- [x] 17.1 Base Kafka checkpoint positions on acknowledged offsets rather than the fetch cursor.
- [x] 17.2 Apply event-time lateness decisions independently to every record in a batch.
- [x] 17.3 Add Kafka acknowledgement and mixed-timestamp batch regression coverage.

## 18. Follow-up review remediation

- [x] 18.1 Validate Kafka recovery offsets against broker low/high watermarks.
- [x] 18.2 Centralize `object_store` and `bytes` dependency versions in workspace dependencies.
- [x] 18.3 Add checkpoint offset boundary regression coverage.

## 19. Follow-up review remediation

- [x] 19.1 Reject cyclic Job operator graphs before runtime dispatch.
- [x] 19.2 Handle null timestamps and resolve event-time windows along source DAG paths.
- [x] 19.3 Require explicit Kafka topics during position recovery.
- [x] 19.4 Require all assigned Job nodes to acknowledge a checkpoint before commit.

## 20. Follow-up review remediation

- [x] 20.1 Derive checkpoint participants from dispatched node operations for automatic placement.
- [x] 20.2 Add automatic-placement checkpoint completion regression coverage.

## 21. Follow-up review remediation

- [x] 21.1 Quiesce runner processing with a checkpoint barrier while capturing state and source positions.
- [x] 21.2 Feed keyed state updates into stateful operator batches and add output-level regression coverage.
- [x] 21.3 Schedule periodic checkpoints from the configured interval through Hub reconciliation.

## 22. Follow-up review remediation

- [x] 22.1 Validate checkpoint task coverage and every referenced state snapshot before publishing a completed artifact.
- [x] 22.2 Refresh idle event-time partitions continuously and separate late-event update/route dispatch paths.
- [x] 22.3 Include checkpoint identity in Hub operation deduplication and add regression coverage.

## 23. Follow-up review remediation

- [x] 23.1 Restore state snapshots by current task assignment IDs across node reassignment.
- [x] 23.2 Advance Kafka checkpoint positions only after successful acknowledgements.
- [x] 23.3 Apply configured state TTL to keyed counter updates and isolate local state by Job version/generation.
- [x] 23.4 Add recovery, Kafka acknowledgement, TTL, and state-isolation regression coverage.

## 24. Follow-up review remediation

- [x] 24.1 Reject disconnected source-to-sink Job graphs while preserving SQL adapter compatibility routes.
- [x] 24.2 Capture and restore task-scoped event-time watermarks across checkpoint recovery.
- [x] 24.3 Enforce periodic checkpoint retention and remove durable checkpoint records and artifacts.
- [x] 24.4 Delete referenced state snapshot objects when removing checkpoint manifests.

## 25. Follow-up review remediation

- [x] 25.1 Key checkpoint watermarks by stable source task identity across multiple sources.
- [x] 25.2 Dispatch checkpoint commands only to nodes participating in the current Job generation.
- [x] 25.3 Exclude and purge all expired state entries before enforcing keyed state budgets.
- [x] 25.4 Apply checkpoint retention to both manual and periodic checkpoint completion paths.

## 26. Follow-up review remediation

- [x] 26.1 Persist Job version with checkpoint records, select only matching recovery artifacts, and verify the manifest before Agent restore.
- [x] 26.2 Encode every supported Arrow integer key type distinctly and reject unsupported or absent state keys at runtime.
- [x] 26.3 Reject unsupported stateful Source and Sink declarations during Job validation and add regression coverage.

## 27. Follow-up review remediation

- [x] 27.1 Persist the configured state format on periodic checkpoint records.
- [x] 27.2 Require every executable source to reach at least one executable sink.
- [x] 27.3 Remove per-Agent intermediate checkpoint manifests during artifact cleanup.

## 28. Follow-up review remediation

- [x] 28.1 Stop Job runtimes on nodes that lose their current-generation task assignments.
- [x] 28.2 Deduplicate same-generation successful Job starts during reconciliation and Agent registration.
- [x] 28.3 Reload persisted checkpoint metadata before completing an in-flight checkpoint after Hub restart.
