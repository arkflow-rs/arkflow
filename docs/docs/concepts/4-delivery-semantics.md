---
description: ArkFlow documentation page.
---

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

- **Replayable sources** are crash-safe by design: the source only commits/acks
  on ack, so on restart it re-delivers any in-flight messages that were never
  confirmed. No local persistence is needed. The Kafka input, for example, sets
  `enable.auto.offset.store=false` so the offset advances only inside `ack()`.
- **Non-replayable sources** need a durable write-ahead log (WAL) at the input
  boundary so the message body survives a crash and can be replayed on restart.
  Enable it per stream with the `durability:` option (see
  [WAL Durability & Performance](5-wal-optimization.md)).

## At-least-once contract

ArkFlow provides **at-least-once** delivery. After a crash and recovery,
in-flight messages MAY be delivered more than once. Outputs MUST tolerate
duplicates (for example, UPSERT-style SQL/InfluxDB sinks are naturally
idempotent; HTTP and Kafka outputs should expect possible duplicates).

**Exactly-once delivery is available opt-in** for transactional sinks. A Kafka
output configured with `exactly_once: true` commits each acknowledged batch as a
single Kafka transaction, so a `read_committed` downstream consumer observes it
atomically. This eliminates in-transaction partial writes and zombie-producer
duplicates; residual duplicates at the post-commit boundary still require
downstream idempotency. See [Exactly-once delivery](./6-exactly-once.md) for the
full contract and its honest boundary.

## Single-node boundary

The durable WAL lives on the local disk of the node running the stream. It
protects against **process crashes** (kill, panic, power loss to the process),
not against the loss of the node itself (disk failure, host termination). High
availability across nodes (replicated WAL / consensus) is out of scope. For
HA-grade durability, run on replicated storage or front the stream with a
durable, replayable source (e.g. Kafka).

## S3 WAL manifest write coordination

When the durable WAL uses the object-store (S3-compatible) backend with
`parallel_put.workers > 1`, multiple PUT workers can finish a segment upload
and seal it concurrently. Each seal rewrites the small `manifest.json` that
records the ack cursor and the sealed-segment list. To keep those concurrent
rewrites from silently overwriting each other — which would regress the cursor
and cause duplicate replay on recovery — manifest writes are coordinated with
ETag-based optimistic concurrency: each PUT carries the ETag it read, and on a
mismatch the writer re-reads, re-applies its change, and retries (up to 8
attempts, covering the configured worker ceiling).

This coordination requires the object-store backend to support conditional
PUT (`If-Match` / if-none-exists). S3 and S3-compatible stores (and the
in-memory test backend) support it. A backend that does not support
conditional PUT falls back to an unconditional write, which is safe only with
a single writer — so `parallel_put.workers > 1` must not be configured
against such a backend.

This is invisible to single-writer setups (`parallel_put.workers = 1`, the
default): there is no contention, the first PUT succeeds, and no retries
occur. The at-least-once contract is unchanged.

## WAL recovery failure (fail-fast)

If a durability-enabled stream fails to recover its WAL at startup — either
because the WAL file is unreadable, or because replaying a pending entry into
the downstream buffer/channel fails — the stream **fails to start** rather
than silently continuing with potentially lost data (fail-fast).

In practice:

- redb 2.6.3 verifies the entire B-tree at `Database::open` time, so most WAL
  corruptions surface as `StreamConfig::build` returning `Err` — the stream
  never reaches the running state.
- If the WAL opens cleanly but replaying a pending entry cannot be forwarded
  (for example, the configured buffer rejects the write), `Stream::run`
  returns `Err` without reading new input.

Operations should monitor stream startup failures. Recovery requires human
intervention: either delete the WAL file at the configured `durability.path`
(accepting that any pending entries are lost) or restore it from a backup.
Rely on k8s/systemd restart strategies to keep retrying, but a persistent
WAL corruption will not self-heal.

The previous behavior — log the failure and continue reading new input with
the unacked entries effectively dropped — was a silent at-least-once
violation and is no longer the case.
