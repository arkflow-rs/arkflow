---
sidebar_position: 9
---

# State and recovery boundaries

ArkFlow separates transient computation state from durable input history. A
window or temporary table is local runtime state; the input WAL is the durable
replay boundary when WAL is enabled.

## In-memory state

Window buffers and temporary resources live in the process that owns the
stream. They are suitable for bounded joins and short-lived aggregation, not
for a distributed state backend or savepoint format.

## WAL recovery

An acknowledged output advances the input cursor. After a crash, unacknowledged
entries can be replayed, providing at-least-once delivery. Replayed input
rebuilds transient state; sinks must tolerate duplicates unless an
exactly-once-capable output is configured.

## Deployment stance

ArkFlow is a single-node engine. It does not claim Flink- or RisingWave-style
cluster-wide state, checkpoint coordination, or rescaling.
