---
sidebar_position: 1
description: Walk through WAL-backed durable ingestion — survive a crash without losing messages.
---

# Tutorial: build a durable pipeline

In the [quickstart](./2-quickstart.md) you built a running
pipeline. Real pipelines face a harder requirement: a crash between *reading*
a message and *writing it out* must not lose the message. This tutorial adds
WAL-backed durability to a pipeline and proves it survives a hard kill.

**Time:** about 15 minutes.
**Prerequisites:** the [quickstart](./2-quickstart.md)
completed; no other services required (the source is synthetic, the sink is
stdout).

## What you will build

```
┌────────┐   persist    ┌─────┐   process   ┌────────┐  ack   ┌────────┐
│ Input  │─────────────▶│ WAL │────────────▶│ Output │───────▶│ Replay │
└────────┘   first      └─────┘             └────────┘  only  └────────┘
                                              confirms        on crash
```

Every message is persisted to a local write-ahead log **before** it enters
the pipeline. The output acknowledges, and only then does the source mark the
message done. If the process dies in between, the unacknowledged messages are
replayed from the WAL on restart — that is *at-least-once* delivery.

## 1. Write the configuration

Create `durable.yaml`:

```yaml
logging:
  level: info

streams:
  - input:
      type: generate
      context: '{ "value": 10, "sensor": "temp_1" }'
      interval: 1ns
      batch_size: 1
      count: 100000

    durability:
      enabled: true
      path: "./data/wal"
      sync: "group_commit"

    pipeline:
      thread_num: 4
      processors:
        - type: json_to_arrow
        - type: arrow_to_json

    output:
      type: stdout
```

The `durability` block is the only new part compared to the quickstart:
`path` is where the WAL lives, and `sync: group_commit` batches disk syncs
for throughput (use `per_entry` when every message must be on disk
immediately).

## 2. Validate, then run

```bash
./target/release/arkflow --config durable.yaml --validate
./target/release/arkflow --config durable.yaml
```

You should see a steady stream of JSON messages on stdout, and a `./data/wal`
directory appear. This example file ships as
[`examples/durability_example.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/durability_example.yaml)
and is validated by CI.

## 3. Kill it hard

In another terminal, find the process and kill it without mercy:

```bash
pkill -9 arkflow
```

Restart the engine:

```bash
./target/release/arkflow --config durable.yaml
```

On startup, ArkFlow detects unacknowledged WAL entries and replays them. The
`count: 100000` messages all reach the output — possibly a few duplicated
(messages in flight at the moment of the crash may be delivered again, which
is exactly what at-least-once means).

## 4. Verify what you learned

- **Where is the durability configured?** On the stream, under `durability:`.
- **When is the input acknowledged?** Only after the output confirms the write.
- **What happens to in-flight messages at a crash?** They are replayed from
  the WAL, so downstream may see duplicates.

## Where to go next

- [Delivery semantics](../build/delivery-semantics.md) — at-least-once vs
  opt-in exactly-once for transactional sinks.
- [WAL optimization](../build/wal.md) — how `sync` modes trade
  latency for throughput.
- [Case: durable webhook collection](../build/recipes/10-case-webhook-durable.md) — the same
  pattern with a real HTTP source and a Kafka sink.
