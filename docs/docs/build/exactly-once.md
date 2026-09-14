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

```yaml
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

```yaml
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

True end-to-end exactly-once that closes this window — committing the source
offset *inside* the producer transaction via `send_offsets_to_transaction`
(Kafka → Kafka only) — is **future work (L3)** and not provided today.

## Requirements summary

- `exactly_once: true` requires a non-empty `transactional_id`; validation fails
  with a clear error otherwise.
- The WAL's object-store `node_id` and the Kafka `transactional_id` are
  **independent** configuration values — neither is derived from the other.
- Outputs other than the transactional Kafka output keep today's default
  at-least-once behavior. Idempotent adapters for other sinks (SQL UPSERT, etc.)
  can be added in later changes.
