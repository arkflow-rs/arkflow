## Why

The previous runtime hardening pass fixed the first set of recovery defects, but the current implementation still has correctness gaps at the boundaries between event-time classification, window state, source acknowledgement, and distributed reconciliation. For example, session timing is still synthesized as `event_time + gap` without per-key state (`crates/arkflow-core/src/executor/event_time_gate.rs:63-65`), composite acknowledgements still use `try_join_all` (`crates/arkflow-core/src/input/mod.rs:227-234`), and processing-time windows are still included in event-time traversal (`crates/arkflow-core/src/executor/graph.rs:877-895`).

These defects are reachable in supported multi-partition, multi-input, restart, retry, and generation-change scenarios. They can prematurely close windows, lose or duplicate source records, retain stale state, strand lifecycle operations, or leave old Job generations running, so they must be resolved before the unified runtime is treated as recovery-safe.

## What Changes

- Make event-time gates model dynamic session boundaries and identify watermark partitions by `(topic, partition)`, while seeding all assigned partitions and sharing downstream watermark progress across multiple event-time inputs.
- Exclude processing-time windows from event-time gating, correctly handle invalid late rows, EOS removal from barrier alignment, and repeated sliding-window exclusion updates.
- Make source acknowledgements and WAL frontier draining failure-isolated and transactionally coupled with fired-window state; reconnect/replay paths must preserve the real source acknowledgement and fail closed after connector shutdown.
- Preserve emitted session state and defer stale-key cleanup until the fired output acknowledgement succeeds; compact staged window snapshots.
- Preserve or explicitly reject unsupported legacy window joins, while retaining accepted legacy payload semantics.
- Make Hub operation deduplication, command expiry, late-result fencing, persisted-operation recovery, and cross-generation placement cleanup generation-aware and retry-safe.
- Restore per-physical-partition watermark progress and repair local state-budget accounting, concurrent budget checks, in-flight metrics, and shutdown-time lifecycle recovery.
- Add focused regression coverage and update the affected runtime/recovery documentation.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `event-time-processing`: define dynamic session timing, topic-partition watermark identity, complete partition seeding/restoration, shared multi-input progress, processing-time exclusion, and invalid/EOS handling.
- `message-acknowledgment`: require atomic or compensating composite source acknowledgements and failure-isolated frontier advancement.
- `input-durability`: preserve source acknowledgements for WAL replay and ensure connector shutdown cannot report an uncommitted source acknowledgement as successful.
- `keyed-state-backend`: exclude expired values from metrics, serialize budget enforcement, and retain window state/TTL correctness during rollback and cleanup.
- `distributed-job-runtime`: fence and recover Job operations and placements by generation, and preserve recovery state across Hub restart.
- `control-plane-hub`: make operation deduplication, command expiry, late result handling, and persisted operation restoration generation-safe.
- `control-plane-reconciliation`: retry or re-evaluate expired attempts and stop stale placements from superseded Job generations.
- `stream-runtime-control`: recover a lifecycle entry after bounded shutdown timeout and keep metrics consistent on failed processing.
- `streaming-job-api`: preserve the declared legacy window contract, including supported joins, or reject unsupported configurations explicitly.

## Impact

Affected implementation areas include `arkflow-core` event-time gates and trackers, WAL/input ACK decorators, window/state backends, graph/job-runner construction, runtime lifecycle supervision, `arkflow-plugin` Kafka input, and `arkflow-server` Hub/Agent reconciliation. The change is implementation-compatible for valid current Job configurations, but unsupported legacy window joins will return an actionable validation error rather than silently changing their output semantics. It adds no new external dependencies or wire-format version.

## Non-goals

- Redesign the Kafka protocol, source transaction APIs, or introduce a new external transaction coordinator.
- Promise universal exactly-once delivery for connectors that do not expose an atomic or idempotent commit boundary.
- Change the user-facing semantics of valid event-time, processing-time, or supported legacy windows beyond correcting the reviewed failures.
- Replace the existing Hub desired-state model or add a new deployment service.
