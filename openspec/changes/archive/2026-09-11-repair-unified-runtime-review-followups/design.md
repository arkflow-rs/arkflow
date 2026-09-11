## Context

The unified runtime now has durable input, checkpoint recovery, event-time windows, fused execution chains, processor worker pools, partitioned graph edges, and an Agent command loop. The latest review found that several of those boundaries still use independent completion signals: WAL replay filtering does not move the cursor, source reads can return before a group/periodic WAL flush, window state commits are separated from source acknowledgement, and worker/Agent control paths can outlive or bypass their error reporting.

The compatibility surface is also broader than the new columnar window operator: legacy row-count sliding windows, legacy buffer ordering, and legacy window payloads are still accepted by the YAML compiler. The implementation must preserve those semantics while keeping the new event-time and stateful execution paths deterministic.

## Goals / Non-Goals

**Goals:**

- Establish one durable completion protocol for WAL append, checkpoint-covered replay, source acknowledgement, and cursor advancement.
- Make cancellation and failure propagation terminate source loops, Kafka frontier waits, processor pools, and Agent commands without resource leaks or orphaned acknowledgements.
- Preserve legacy buffer/window behavior and route processor failures consistently through configured error outputs.
- Ensure partitioned graph edges fan out to the complete downstream task set selected by key-group ownership.
- Keep window state and source acknowledgement recoverable as one processing unit, including late sliding-window metadata.

**Non-Goals:**

- Changing the WAL on-disk format, Kafka broker protocol, or external delivery guarantees.
- Replacing the processor pool, graph planner, Agent protocol, or window engine with a new architecture.
- Adding new connectors, window types, or command types.

## Decisions

1. **Use a durable WAL cut before returning from `WalInput::read`.** The append path SHALL explicitly flush according to the configured WAL backend before exposing the batch. Checkpoint-covered entries SHALL be reconciled into the same contiguous cursor/frontier before new entries are read. This keeps recovery state monotonic and avoids a second recovery-only cursor. An asynchronous flush-only design was rejected because it preserves the crash loss window identified by the review.

2. **Model source acknowledgement and state finalization as one retryable unit.** Fired-window acknowledgements will use a composite transaction that can compensate staged state when a source/WAL acknowledgement fails. The implementation will preserve retryability rather than treating state commit and source commit as unrelated successful operations. Committing state first with a non-rollbackable source ack was rejected because replay would double-count or lose aggregates.

3. **Make all waits observe the chain cancellation token.** Reconnect backoff, Kafka contiguous-frontier waits, processor pool joins, and Agent command execution will be driven by cancellation-aware futures. Close paths will wake condition-variable/notifier waiters. Unbounded sleeps and detached worker cleanup were rejected because they can prevent graceful shutdown indefinitely.

4. **Keep compatibility behavior at the compiler boundary.** The legacy YAML compiler will distinguish row-count windows from time windows, preserve buffer-before-processor ordering, and either carry legacy payload semantics into the compatible operator or reject configurations that cannot be represented. Error targets will be attached at processor failure boundaries, not only at the final window node. A silent reinterpretation as a time window or aggregate-only batch was rejected as data/schema loss.

5. **Build partitioned edges from all eligible downstream tasks.** A partitioned edge will retain the full downstream channel set, and dispatch will apply the `JobPlan` key-group owner rather than relying on same-subtask topology. This preserves keyed state locality across source partitions. Restricting each edge to one same-index target was rejected because it bypasses the planner's ownership mapping.

6. **Keep Agent liveness independent from command latency.** Polling will spawn or otherwise supervise command work while heartbeat/report/cancellation branches remain selectable. Every command execution error, including checkpoint aggregation failure, will be converted into a terminal failed result with correlation metadata. Blocking the session loop on checkpoint I/O was rejected because the Hub can expire the lease and the command then becomes unreconcilable.

## Risks / Trade-offs

- **[Risk]** Flushing every durable input read can reduce ingest throughput. → **Mitigation:** use the configured WAL flush primitive and preserve batching within the WAL implementation; correctness takes precedence for the durable-input contract.
- **[Risk]** A composite window/source acknowledgement may retain more staged state during retries. → **Mitigation:** bound pending transactions, make compensation idempotent, and release only after the full unit succeeds or is explicitly failed.
- **[Risk]** Preserving legacy payloads may require a compatibility branch in the columnar window operator. → **Mitigation:** keep the branch narrow and add golden schema/row-content tests for each accepted legacy shape.
- **[Risk]** Joining worker pools on cancellation can extend shutdown while in-flight work drains. → **Mitigation:** use the existing bounded queues and cancellation-aware joins; surface a bounded shutdown error rather than silently closing live processors.
- **[Risk]** Concurrent Agent commands may change command ordering. → **Mitigation:** retain idempotency, generation fencing, and per-command terminal result tracking; only command execution is decoupled from heartbeat polling.

## Migration Plan

1. Deploy the runtime changes with the existing WAL and checkpoint formats; no data migration is required.
2. On startup, reconcile covered WAL prefixes and continue from the resulting cursor/frontier. Legacy window configurations retain their prior interpretation; unsupported ambiguous shapes fail validation with an actionable error.
3. Roll back by reverting the runtime binary. Existing WAL/checkpoint artifacts remain readable because the storage formats and manifest schema are unchanged.
4. Validate with core, Kafka-focused plugin, server, compatibility, and workspace tests; run Docker/MinIO/Pulsar integration targets when their services are available.

## Open Questions

- Whether a future WAL backend should expose a distinct `flush_before_read` policy remains an operational tuning question; this change keeps the durable-input guarantee unconditional for enabled local WALs.
- Whether legacy aggregate-only payload compatibility can be removed in a future major version is deferred; this change preserves accepted configurations and does not introduce a breaking migration.
