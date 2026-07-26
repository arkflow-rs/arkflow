# Input Delivery Semantics

How ArkFlow handles messages across crashes depends on whether an input can
**re-deliver** a message that has not yet been acknowledged. This page classifies
every input and explains the at-least-once delivery contract.

## Background: the ack-after-output rule

For every input, the source is only acknowledged **after** the downstream output
confirms the write. This means a crash between reading a message and writing it
out does not silently drop the message — *provided the source can re-deliver it*.

## Replayable vs non-replayable inputs

A **replayable** source can re-deliver an unacknowledged message after a crash
(typically because it retains the message and tracks an offset/position that is
only advanced on ack). A **non-replayable** source discards a message as soon as
it is read, so a crash after read and before output loses it unless the message
is persisted locally first.

| Input | Replayable? | Source-side ack |
|-------|-------------|-----------------|
| **Kafka** | Yes | `store_offset` on ack only; `enable.auto.offset.store=false` |
| **MQTT** | Yes (QoS ≥ 1) | manual ack (`set_manual_acks(true)`); default QoS 1 |
| **NATS (JetStream)** | Yes | `message.ack()` on ack only |
| **Pulsar** | Yes | `consumer.ack()` on ack only |
| **Redis (Streams)** | Yes (with consumer groups) | depends on configuration |
| **SQL** | Partial (incremental query) | via incremental cursor |
| **HTTP (server)** | **No** (webhook) | none — fire-and-forget |
| **HTTP (client poll)** | **No** | none |
| **File** | **No** (read-once stream) | none |
| **Generate** | **No** | none |
| **Modbus** | **No** | none |
| **Memory** | **No** | none |
| **WebSocket** | **No** | none |

## What this means for durability

- **Replayable sources** are crash-safe today (Phase 0 of `add-input-durability`):
  the source only commits/acks on ack, so on restart it re-delivers any
  in-flight messages that were never confirmed. No local persistence is needed.
  The Kafka input, for example, sets `enable.auto.offset.store=false` so the
  offset advances only inside `ack()`.
- **Non-replayable sources** need a durable write-ahead log (WAL) at the input
  boundary so the message body survives a crash and can be replayed on restart.
  This is Phase 1 of `add-input-durability` (the per-stream `durability:` option).

## At-least-once contract

ArkFlow provides **at-least-once** delivery. After a crash and recovery,
in-flight messages MAY be delivered more than once. Outputs MUST tolerate
duplicates (for example, UPSERT-style SQL/InfluxDB sinks are naturally
idempotent; HTTP and Kafka outputs should expect possible duplicates).

Exactly-once delivery is not provided.

## Single-node boundary

The durable WAL lives on the local disk of the node running the stream. It
protects against **process crashes** (kill, panic, power loss to the process),
not against the loss of the node itself (disk failure, host termination). High
availability across nodes (replicated WAL / consensus) is out of scope. For
HA-grade durability, run on replicated storage or front the stream with a
durable, replayable source (e.g. Kafka).
