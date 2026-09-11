## Why

Review of `08730fd` found correctness gaps in the newly hardened runtime. In particular, `task.rs:1699-1703` advances WAL state from independent fan-out acknowledgements, `state_journal.rs:312-318` can roll back over a later commit, and `task.rs:573-579` collapses all physical input partitions into one event-time partition. These paths can skip input, lose state updates, close windows early, or leave a restarted Job absent despite a desired-running state.

## What Changes

- Make WAL and checkpoint cursors advance only on contiguous, durably acknowledged sequences, including fan-out and retry paths.
- Order window/state journal commits before source acknowledgements and make rollback conditional/serialized so later commits cannot be erased; preserve TTL metadata during compensation.
- Track event-time progress by physical input partition, establish the active-partition watermark minimum, and classify sliding-window lateness per containing window.
- Preserve processor failures and their acknowledgements in worker pools so configured error/DLQ edges receive failed deliveries.
- Make Agent reports use the actual process boot identity and invalidate stale start assumptions after restart.
- Make local checkpoint manifests and stateless checkpoint reports use the same complete task set and configured state format.
- Reject missing configured window value fields, mark invalid-timestamp routes distinctly, and count every late/invalid row in runtime metrics.
- Add focused regression tests for fan-out ordering, state rollback, physical partitions, worker-pool error routing, Agent restart reconciliation, local checkpoint recovery, and typed event-time behavior.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `checkpoint-recovery`: require checkpoint state, source cursors, and task membership to represent one durable acknowledged cut, including local manifests.
- `input-durability`: require contiguous WAL cursor advancement across fan-out and retry acknowledgements.
- `message-acknowledgment`: require composite acknowledgements to commit state before source durability and to retry staged mutations safely.
- `keyed-state-backend`: require conditional/serialized rollback and preservation of TTL metadata.
- `event-time-processing`: require physical-partition watermarks, active minimum progress, per-window late classification, invalid markers, and complete late-row metrics.
- `distributed-job-runtime`: require worker-pool error routing and consistent execution-task membership for local recovery.
- `control-plane-fleet`: require process boot identity and restart reconciliation to redispatch missing local runtimes.

## Impact

Affected areas are `arkflow-core` acknowledgement/state journal, WAL input, task/event-time/window execution, worker-pool error handling, checkpoint aggregation, and local recovery; `arkflow-plugin` connector metadata/partition propagation; and `arkflow-server` Agent registration/reporting. The change is implementation-compatible for valid configurations but changes recovery and error handling from potentially lossy behavior to fail-safe behavior. No new external dependency or wire-format change is intended.

## Non-goals

- Do not redesign the checkpoint storage protocol or introduce a new sink transaction protocol.
- Do not change Kafka subscription topology or the public Job configuration shape beyond correcting the existing semantics.
- Do not add new connectors, window types, or control-plane APIs.
