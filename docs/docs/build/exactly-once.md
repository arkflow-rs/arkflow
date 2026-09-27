---
sidebar_position: 6
---

# Exactly-once delivery

ArkFlow delivers **at-least-once** by default. On recovery, in-flight messages
are replayed and MAY be redelivered to outputs. For duplicate-intolerant sinks,
ArkFlow can deliver **exactly-once** (more precisely, *effectively-once*) opt-in,
via a transactional Kafka output. This page describes what that guarantees,
how to configure it, and where its boundary lies.

## How it works

A Kafka output configured for exactly-once uses a **transactional producer**:

- `init_transactions` is called once at `connect()`;
- before each acknowledged batch, the output begins a transaction, sends every
  message in the batch, then commits;
- the WAL cursor advances (and the source is committed) only after the
  transaction commits successfully.

The unit of work is **one ack range = one `write_batch` call = one Kafka
transaction**. If a buffer (memory, a tumbling/sliding/session window, or a
join) aggregates several input messages into one output batch, that whole batch
is one atomic transaction unit. Downstream consumers reading with
`isolation.level=read_committed` observe each batch atomically — **all messages
or none**.

On any failure (a `commit_transaction` error that requires abort, or a crash),
the batch is **not acknowledged**, the WAL cursor does **not** advance, and the
range is replayed on recovery — which begins a fresh transaction.

## Configuration

Enable exactly-once on the Kafka output with two keys:

```yaml validate=fragment wrap=output
output:
  type: kafka
  brokers:
    - localhost:9092
  topic:
    type: value
    value: orders-copy
  exactly_once: true
  transactional_id: arkflow-orders-copy-0   # stable across restarts; unique per producer
```

- **`exactly_once: true`** turns on transactional production.
- **`transactional_id`** is **required** when `exactly_once` is enabled. It MUST
  be **stable across restarts** (you own this) and **unique per stream producer**.
  On restart the broker uses the same id to fence the prior producer epoch and
  abort its in-flight (zombie) transaction, so zombie writes are never visible to
  `read_committed` consumers.

Exactly-once is layered on top of at-least-once ingestion, so the input side
needs durability enabled so that a crash between read and output does not lose
data:

```yaml validate=fragment wrap=durability
durability:
  enabled: true
  path: "./data/wal-eos"
  sync: group_commit
```

A complete runnable example is in
[`examples/eos-kafka.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/eos-kafka.yaml)
(Kafka → Kafka, consume-transform-produce).

## The honest boundary (read this)

The Kafka transactional output eliminates two specific
sources of duplicates:

- **in-transaction partial writes** — the transaction commits atomically, so a
  `read_committed` consumer never sees a partial batch;
- **zombie-producer duplicates** — the stable `transactional_id` fences stale
  producer epochs across restarts.

It does **not** guarantee the absence of duplicates when a crash occurs **after
the producer transaction is committed and before the source offset is
committed**. The source offset is committed asynchronously (bounded by the
source's auto-commit interval, e.g. Kafka's default 5s). If the process crashes
in that window, on recovery the source redelivers the range, a new producer
writes it again, and a `read_committed` downstream consumer observes duplicate
rows.

**Such residual duplicates MUST be absorbed downstream** — by a dedup key,
business-level idempotency, or an idempotent sink (e.g. UPSERT). Design your
downstream consumers accordingly.

That residual window — a crash between producer commit and source offset
commit — is what **L3** closes. Configure the paired Kafka input and output to
commit the source offset *inside* the producer transaction via
`send_offsets_to_transaction`:

```yaml validate=fragment wrap=input
input:
  type: kafka
  brokers:
    - localhost:9092
  topics:
    - orders
  consumer_group: orders-copy-group
  start_from_latest: false
  transactional_offsets: true
```

```yaml validate=fragment wrap=output
output:
  type: kafka
  brokers:
    - localhost:9092
  topic:
    type: value
    value: orders-copy
  exactly_once: true
  transactional_id: arkflow-orders-copy-0
  offset_commit_group: orders-copy-group   # must name the input's consumer_group
```

- **Input `transactional_offsets: true`** registers the consumer group's
  metadata in a process-internal registry and changes `ack()` to advance only
  the in-memory frontier — the input no longer commits offsets itself; broker
  group offsets advance only with output transactions.
- **Output `offset_commit_group`** (requires `exactly_once: true`) derives the
  covered offsets from each batch's `__meta_partition`/`__meta_offset` columns
  (max offset + 1 per partition) and folds them into the same transaction as
  the writes, so a `read_committed` consumer of the source group observes
  writes and offset advances atomically. A rolled-back transaction leaves the
  group offsets unchanged and the range is replayed.

L3 boundaries, enforced by explicit errors rather than silent degradation:

- **Process-internal pairing** — the output resolves the named group through
  the in-process registry; the input and output must run in the same ArkFlow
  process. Distributed jobs that split input and output across nodes are not
  covered (independent change). An `offset_commit_group` naming a group with no
  live `transactional_offsets` input fails the write.
- **Single input topic** — batch metadata carries partitions without topics, so
  the registry routes by the input's topic list; a multi-topic subscription is
  rejected.
- **Kafka → Kafka only** — a batch without Kafka source metadata columns
  contributes no offsets (the writes still proceed); non-Kafka sources cannot
  pair with the transactional output.

A complete runnable L3 example is in
[`examples/eos-kafka-l3.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/eos-kafka-l3.yaml)
(`examples/eos-kafka.yaml` shows the L2-only variant).

## Requirements summary

- `exactly_once: true` requires a non-empty `transactional_id`; validation fails
  with a clear error otherwise.
- `offset_commit_group` requires `exactly_once: true`; the builder rejects the
  configuration otherwise. The named group must have a same-process Kafka input
  with `transactional_offsets: true`, or the write fails with a configuration
  error.
- The WAL's object-store `node_id` and the Kafka `transactional_id` are
  **independent** configuration values — neither is derived from the other.
- Outputs other than the transactional Kafka output keep today's default
  at-least-once behavior. Idempotent adapters for other sinks (SQL UPSERT, etc.)
  can be added in later changes.
