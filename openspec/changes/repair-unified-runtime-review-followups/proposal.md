## Why

The previous unified-runtime hardening still has reachable correctness gaps: `WalInput::replay_queue` filters checkpoint-covered entries without reconciling the WAL cursor (`crates/arkflow-core/src/executor/stream_adapter.rs:215-236`), while new reads return after a non-durable append (`crates/arkflow-core/src/executor/stream_adapter.rs:250-267`). The runtime also leaves cancellation, processor-pool, legacy-window, partition-routing, and Agent command-session edge cases unresolved (`crates/arkflow-core/src/executor/task.rs:559-597`, `crates/arkflow-core/src/executor/graph.rs:381-410`, and `crates/arkflow-server/src/agent.rs:1148-1174`).

These paths can lose replayable input, wedge shutdown, silently change legacy stream behavior, split keyed state across the wrong subtasks, or leave control-plane commands without a terminal result. This follow-up closes the remaining review findings before the unified runtime is treated as production-safe.

## What Changes

- Make WAL append, checkpoint-covered replay, and wrapped source acknowledgement use one crash-safe durable cursor protocol.
- Settle all sibling acknowledgements on processor failures and make source/Kafka frontier waits and reconnect loops cancellation-aware.
- Join and propagate processor worker-pool failures on cancellation, EOS, and drain paths.
- Preserve legacy row-count windows, legacy window payloads, buffer-before-processor ordering, and error routing through pre-window processors.
- Connect partitioned edges to every downstream subtask so key-group ownership remains authoritative.
- Refresh sliding-window exclusion metadata across repeated watermark releases and couple window state rollback to source acknowledgement failure.
- Keep Agent heartbeats responsive while commands run and always report checkpoint failures as terminal command results.
- Add regression coverage for durable recovery, legacy compatibility, routing, shutdown, and Agent command lifecycle behavior.

## Capabilities

### New Capabilities

<!-- No new user-facing capability; this change hardens existing contracts. -->

### Modified Capabilities

- `input-durability`: require durable WAL visibility before returning input and reconcile checkpoint-covered prefixes with the WAL cursor.
- `message-acknowledgment`: settle fan-out siblings, order WAL/source commits safely, and make pending frontiers cancellable.
- `event-time-processing`: preserve sliding-window exclusion state and roll back window state when source acknowledgement fails.
- `streaming-job-api`: preserve legacy buffer/window configuration, payload, ordering, and processor error behavior.
- `distributed-job-runtime`: route partitioned edges to the complete downstream task set according to key-group ownership.
- `stream-runtime-control`: ensure cancellation and reconnect paths terminate and release resources.
- `compute-node-agent`: keep heartbeats active during commands and report command failures without dropping the session.

## Impact

- Affected implementation: `arkflow-core` WAL/input adapter, executor task and worker pool, graph builder, stream compiler, event-time gate, and window operator; `arkflow-plugin` Kafka input; `arkflow-server` Agent command loop.
- Affected behavior: crash recovery, exactly-once-oriented acknowledgement ordering, shutdown latency, legacy YAML compatibility, keyed-state placement, checkpoint command observability, and retry semantics.
- No public API or dependency change is intended; changes are limited to runtime behavior and compatibility guarantees, with tests added at core, plugin, and server layers.

## Non-goals

- Redesigning the WAL storage format or changing broker-level Kafka delivery guarantees.
- Introducing new window types, connectors, or Agent command types.
- Changing the documented semantics of configured legacy streams beyond preserving their existing behavior.
