---
components: [batch]
description: ArkFlow documentation page.
---

# Batch

The Batch processor accumulates incoming message batches and flushes them as a single merged batch when either a configured message row count is reached or a timeout elapses. It is useful for grouping small batches together to improve downstream throughput.

The timeout fires without waiting for the next arrival: once `timeout_ms` elapses with no new input, the partial batch is flushed on the next idle tick. When the upstream source ends (graceful shutdown or a bounded source reaching its end), a partial batch is drained and forwarded instead of being discarded.

## Delivery acknowledgement

Acknowledgements of the buffered deliveries are deferred, not settled on arrival: while rows sit in the buffer the processor reports `Deferred`, so the source cursor (Kafka offsets, WAL sequences) is not committed. When a flush emits the merged batch, the emission carries a composite acknowledgement covering every buffered delivery, and the source commits only after the output is written successfully downstream (at-least-once: a crash replays the uncommitted window). If the write fails, the failure compensation (`undo`/`abort`) propagates to every buffered delivery. When the pipeline is cancelled mid-flight, messages still buffered cannot be delivered: their acknowledgements are aborted so the sources replay them after recovery (a warning is logged).

Because commits now happen at flush time, a `count` larger than the source in-flight window interacts with backpressure: upstream delivery slows down (bounded channels) instead of silently buffering — the durability-correct direction.

## Merging

Flushes merge batches by schema normalization: output columns are the union of the input column names, missing columns are null-filled, and rows are aligned by column name (not by position). A column name carrying two different types (for example `Int64` and `Utf8`) fails the flush with an explicit error instead of silently coercing or misplacing values.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `batch` |
| count | integer | yes | — | Number of message **rows** (not input batches) accumulated before flushing. |
| timeout_ms | integer | yes | — | Maximum time in milliseconds to wait before flushing an incomplete batch. |

## Examples

```yaml validate=fragment wrap=processors
- type: "batch"
  count: 1000
  timeout_ms: 1000
```

```yaml validate=full
streams:
  - input:
      type: "memory"
      messages:
        - '{ "value": 1 }'
        - '{ "value": 2 }'
    pipeline:
      thread_num: 4
      processors:
        - type: "batch"
          count: 100
          timeout_ms: 500
    output:
      type: "stdout"
```
