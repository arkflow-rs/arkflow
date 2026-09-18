---
sidebar_position: 2
title: Streams
description: The anatomy of an ArkFlow stream — input, processors, output, and error output.
---

# Streams

A stream is a declarative YAML description of one data path: an **input**
consumes records, an optional **pipeline** transforms them, and an **output**
delivers the result. Everything runs on the unified execution kernel — the
same YAML compiles to a `JobSpec`, so what you read here also holds for
[jobs](./jobs.md).

## Anatomy

```yaml validate=full
streams:
  - id: orders-to-mysql        # unique stream id
    input:
      type: kafka              # registered input type
      brokers: [localhost:9092]
      topics: [shop.orders]
      consumer_group: arkflow-orders
      start_from_latest: false
    pipeline:
      thread_num: 4            # parallel processing chains
      processors:
        - type: json_to_arrow  # decode into columnar batches
        - type: sql            # DataFusion SQL over the batch
          query: "SELECT * FROM flow WHERE status = 'PAID'"
        - type: arrow_to_json  # re-encode for the sink
    output:
      type: sql
      output_type:
        type: mysql
        uri: mysql://root@localhost:3306/arkflow
      table_name: orders
    error_output:              # side sink for poisoned records
      type: stdout
```

This is runnable today: `examples/case_order_stream_sql.yaml` in the
repository is exactly this pipeline, and it is validated by CI.

## The four stages

### Input

The `input` block picks a registered input component by `type` and passes it
component-specific options. ArkFlow ships 13 inputs — Kafka, HTTP, MQTT,
NATS, Pulsar, Redis, SQL, files, WebSockets, Modbus, and more. Each input
documents its config schema on its component page, and every schema is
available offline through `arkflow components show input <type>`.

### Pipeline

`processors` is an ordered chain executed inside fused operator chains.
Records are columnar `MessageBatch`es (Arrow `RecordBatch`); processors that
need bytes use a [codec](../components/5-codecs/json.md) at the boundary.
Typical chains decode, transform with [SQL](../components/2-processors/sql.md)
or [VRL](../components/2-processors/vrl.md), and re-encode. `thread_num`
controls how many parallel chains process batches; ordering guarantees are
described in [backpressure & ordering](./backpressure.md).

Windows (tumbling, sliding, session) attach as buffers — see the
[buffer components](../components/1-buffers/tumbling_window.md) and the
[windowed aggregation recipe](./recipes/3-windowed-aggregation.md).

### Output

The `output` block delivers each processed batch. Kafka outputs support
exactly-once transactions; SQL outputs batch inserts; see the
[component catalog](../components.md) for all sinks. Delivery guarantees are
set per stream — start with [delivery semantics](./delivery-semantics.md).

### Error output

`error_output` is where malformed or failed records go. A poison message
never blocks the stream: it is routed to this side sink and processing
continues — the difference between a resilient pipeline and a 3 a.m. incident.

## Durability

Add stream-level durability to make the stream crash-safe:

```yaml validate=full
streams:
  - id: webhooks
    input:
      type: http
      address: "0.0.0.0:8080"
      path: "/webhook"
    pipeline:
      processors: []
    output:
      type: drop
    durability:
      enabled: true
      path: "./data/wal"
      sync: "group_commit"   # per_entry | group_commit | periodic
```

With durability enabled, every message is fsynced to the WAL before it enters
the pipeline and the source commits only after the output confirms —
at-least-once delivery across crashes. This matters most for
non-replayable sources (HTTP webhooks, Modbus, files): replayable sources
like Kafka can re-deliver, but the WAL guarantees no loss regardless of
source settings. See [WAL durability](./wal.md) and
[exactly-once](./exactly-once.md) for the full ladder.

## Validate before you run

```bash
./arkflow --config config.yaml --validate
```

Deep validation covers stream ids, job graphs, and configuration rules —
see the [CLI reference](../reference/cli.md).

## Next steps

- [Streaming jobs](./jobs.md) — multi-step DAGs with event time and state.
- [Components](../components.md) — browse all 41 registered components.
- [Top-level configuration](../reference/configuration.md) — every engine option.
