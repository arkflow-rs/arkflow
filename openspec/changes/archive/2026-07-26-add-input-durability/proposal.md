## Why

All in-flight data in ArkFlow lives only in memory (flume channels + the in-memory/window buffers). A process crash anywhere between `input.read()` and `ack.ack()` permanently loses data for non-replayable sources (HTTP webhook, Modbus, Generate, file streams), and risks loss even for replayable sources (Kafka/MQTT/NATS/Pulsar) if the source auto-commits before the downstream output confirms. We need durable ingestion plus crash recovery so that **no data entering from any input is lost** across crashes.

## What Changes

- **NEW**: Durable ingest WAL at the stream input boundary — every read message is persisted (body + sequence) and `fsync`'d before it enters the pipeline.
- **NEW**: Ack-gated durability — the durable WAL cursor advances, and the source is committed, only after the downstream output confirms the write. Reuses the existing "ack-after-output" plumbing.
- **NEW**: Crash recovery — on startup, the Engine replays any WAL entries past the committed cursor before streams resume.
- **NEW**: Per-stream `durability:` configuration (storage path, sync policy, retention).
- **NEW**: At-least-once delivery semantics, documented as an explicit contract (crash recovery may re-deliver in-flight messages; outputs MUST tolerate duplicates).
- **BREAKING**: `Ack::ack()` becomes fallible — `async fn ack(&self) -> Result<(), Error>` instead of `-> ()`. Required so WAL cursor-advance / source-commit failures are observable and can drive backpressure instead of failing silently.
- **BREAKING**: The WAL is a first-class ingest stage owned by the `Stream` (new `durability:` config section, changes to the `Stream` struct and its run loop), not an `Input` decorator. Removes a layer of indirection and makes the durability boundary explicit.
- **Phase 0 (no breaks)**: Audit and correct source-side ack gating — ensure Kafka/MQTT/NATS/Pulsar default to manual commit and ack only after output success. This alone makes replayable-source → durable-output crash-safe today.

## Capabilities

### New Capabilities
- `message-acknowledgment`: The cross-cutting `Ack` contract — fallible acknowledgement that propagates errors from cursor-advance / source-commit back to the stream.
- `input-durability`: Durable ingestion (WAL) at the input boundary, ack-gated cursor advancement, and crash-recovery replay. At-least-once semantics.

### Modified Capabilities
<!-- None — openspec/specs/ is currently empty, so all behavior is captured as new capabilities above. -->

## Impact

- **`arkflow-core`**: `Ack` trait signature change (fallible); `Stream` struct + run loop gain a WAL ingest stage; `StreamConfig` gains a `durability` section; `Engine` gains a startup recovery phase.
- **`arkflow-plugin`**: Every `Ack` implementor updates to the fallible signature — Kafka, MQTT, NATS, Pulsar inputs (and any `NoopAck`/`VecAck` usages). Output plugins' duplicate-tolerance becomes a documented contract.
- **Dependencies**: New embedded transactional store (proposed: `redb`) for WAL persistence.
- **Behavioral**: At-least-once (possible duplicates after recovery); new fsync cost on the ingest hot path (mitigated by group-commit).
- **Out of scope**: exactly-once semantics, multi-node HA, full-pipeline persistent backbone, state checkpointing for stateful processors.
