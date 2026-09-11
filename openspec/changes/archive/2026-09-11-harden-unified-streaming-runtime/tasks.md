## 1. Checkpoint cut and state-commit primitives

- [x] 1.1 Add an execution-local `CommitFrontier` and `CheckpointCut` abstraction that tracks per-source-partition contiguous acknowledgements, next offsets, watermark identity, and the cut generation; add unit tests for in-order, out-of-order, duplicate, and failed acknowledgements.
- [x] 1.2 Add a bounded state mutation journal/epoch for stateful and window operators, with committed and pending views plus commit/rollback handles tied to the final output acknowledgement; add a test proving a failed sink write does not persist a replayed increment twice.
- [x] 1.3 Update source barrier injection and chain barrier handling to seal the acknowledged cut, capture source positions and watermarks from that cut, snapshot the committed state epoch before `aligner.release()`, and only then release post-barrier data; add a multi-input race regression test.
- [x] 1.4 Make checkpoint reports wait for pre-cut state transactions and validate that source positions, state, watermark, barrier, and attempt identity belong to the same cut; surface snapshot/cut failures through the existing checkpoint error channel.
- [x] 1.5 Update composite acknowledgements and sink completion so state finalization, WAL cursor advancement, and source commit are all fallible and ordered after successful output; add failure-propagation tests for `VecAck` and state finalization.

## 2. Resource and runtime lifecycle

- [x] 2.1 Add an idempotent Job resource guard that connects all temporary resources, sources, and sinks before spawning task loops, cleans up partially connected resources in reverse order, and closes state backends/processors/sinks on shutdown.
- [x] 2.2 Integrate the resource guard into `StreamJobAdapter` and local/Agent graph startup so temporary stores are connected before processor `get` calls and connection failures prevent input consumption; add a temporary-resource lifecycle test.
- [x] 2.3 Make `WalInput::close` close the wrapped input, stop/flush the WAL flusher, and release the WAL handle; add close/reopen and pending-append flush tests for normal shutdown and partial startup.
- [x] 2.4 Close dry-run/deep-validation adapters and WALs before rebuilding real runtime adapters, including error paths; add a durability-enabled start test that proves the same redb path can be reopened without an exclusive-lock failure.
- [x] 2.5 Change local Job startup and runtime supervision to await graph/resource construction before readiness, propagate immediate task failures, and transition `Starting` runtimes to `Failed` before returning errors; add local invalid-component and dry-run-failure tests.

## 3. Kafka assignment, restore, and offset correctness

- [x] 3.1 Change source graph construction to call `assign_partition` only when the source operator has multiple physical source tasks; verify a single Kafka task retains subscribe-all-partitions behavior and multi-task plans remain partitioned.
- [x] 3.2 Preserve the full configured Kafka assignment during restore: merge checkpoint positions into configured topics/partitions, retain omitted partitions, preserve subscription mode where applicable, and install seeks without replacing the assignment with a subset.
- [x] 3.3 Seed Kafka's in-memory `CommitFrontier` from restored positions and make `KafkaAck` advance/store only the highest contiguous next offset per topic-partition; cover gaps, duplicates, reconnects, and a checkpoint immediately after restore.
- [x] 3.4 Add Kafka-focused tests for one-task multi-partition consumption, multi-task physical assignment, subset restore, restored cursor retention, and out-of-order acknowledgement checkpoint positions; run the targeted plugin test suite.

## 4. Event-time gates and window semantics

- [x] 4.1 Extend timestamp extraction to Arrow second, millisecond, microsecond, and nanosecond arrays with checked conversion to milliseconds, documented negative-time rounding, and actionable overflow/type errors; add nullable and unit-specific tests.
- [x] 4.2 Rework event-time gate evaluation to advance the batch watermark before classifying current rows, retain only future valid rows as `Hold`, and route/drop null timestamps without indefinite acknowledgement retention; add mixed `[2100, 100]`, Route, Update, Drop, and null tests.
- [x] 4.3 Make window/operator watermark state keyed by upstream source task and physical partition, combine active progress with minimum semantics, honor idle partitions, and restore the actual assigned partition; add multi-input and non-zero-partition recovery tests.
- [x] 4.4 Replace sliding-window `size / slide` truncation with complete containing-start enumeration, including negative and non-divisible boundaries; add assignment tests for `size=5`, `slide=2`, timestamp 4 and boundary cases.
- [x] 4.5 Retain emitted windows through their allowed-lateness deadline, apply late Update to the original `(operator, key, window)` aggregate, emit a complete correction with an update marker, and prevent Route-marked rows from being aggregated twice; add late-update lifecycle tests.
- [x] 4.6 Replace integer-only aggregate output with typed Int64/Float32/Float64 accumulation, min/max, state serialization, restore, and output schema; explicitly reject unsupported numeric types and add Float32/Float64 precision tests.
- [x] 4.7 Run the executor event-time/window test group, including watermark, gate, sliding/session, late-event, state-restore, and typed-output regressions.

## 5. Recovery compatibility and checkpoint membership

- [x] 5.1 Implement one shared recovery-compatibility evaluator for Job identity, target/source Job versions, operator/state namespaces, format migration, generation, attempts, and checksums; use it in Hub authorization, Agent validation, repository validation, and runtime restore.
- [x] 5.2 Normalize checkpoint aggregation to the configured Job/backend state format and exclude empty stateless reports from false format mismatches; add mixed stateless/stateful format tests.
- [x] 5.3 Carry the complete planned assignment/task set into checkpoint aggregation and repository validation; reject missing, duplicate, or extra task entries and preserve the last valid checkpoint when a node is offline.
- [x] 5.4 Pass the real assigned source partition through Agent recovery and restore each watermark to that partition; add an Agent regression for partition 1 or higher.
- [x] 5.5 Add checkpoint/savepoint tests for compatible version upgrades, rejected migrations, incomplete task sets, checksum failures, and deterministic restore-before-read ordering.

## 6. Validation, schema, and configured concurrency

- [x] 6.1 Extract a side-effect-free deep Job build validator and use it for CLI/configuration API validation, declared local Jobs, and compiled Streams; cover unknown components, unsupported backends, invalid graph edges, and valid jobs-only configurations.
- [x] 6.2 Preserve `pipeline.thread_num` as bounded chain-level processor worker concurrency without changing source partition topology; maintain cancellation, backpressure, and ordered output, and add a configured-concurrency regression test.
- [x] 6.3 Extend the generated Engine JSON schema with `jobs` and the JobSpec structure while preserving strict additional-property checks; add schema tests for jobs-only and streams-plus-jobs configurations.
- [x] 6.4 Run configuration/runtime validation tests and verify that validation-created temporary/WAL/backend resources are closed before real startup.

## 7. Hub–Agent operation and session aggregation

- [x] 7.1 Track Job operation results by generation, action, and expected assignment, and derive Job observed/convergence state only after aggregate evaluation; distinguish converging, degraded/retrying, and terminal failed states without letting one peer overwrite healthy peers.
- [x] 7.2 Make checkpoint commit dispatch require the full expected assignment set and keep partial/offline rounds pending or failed rather than publishing a recoverable subset.
- [x] 7.3 Use the registration session identity as the Agent report boot identity, reset report sequence for each new session, and reject delayed reports from older sessions; add reconnect and stale-report tests.
- [x] 7.4 Run focused Hub/Agent lifecycle, checkpoint aggregation, recovery, and report-cursor tests, including the two-node checkpoint/restart smoke path.

## 8. Documentation and final verification

- [x] 8.1 Update runtime/checkpoint/event-time/configuration documentation with the acknowledged-cut rule, Kafka assignment behavior, typed window output, compatible savepoints, invalid-timestamp handling, and failure/readiness states.
- [x] 8.2 Run `cargo fmt --all -- --check`, `cargo test -p arkflow-core --lib executor:: -- --nocapture`, targeted plugin/server tests, and `git diff --check`.
- [x] 8.3 Run the complete workspace regression suite, including the available multi-node smoke tests and documented environment-dependent skips, then inspect the final worktree and OpenSpec status.
- [x] 8.4 Run `openspec validate harden-unified-streaming-runtime --strict --no-interactive` and confirm every task in this change is mapped to a passing test or explicit verification command before implementation is considered complete.
