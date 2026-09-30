---
components: [buffer/memory]
description: ArkFlow documentation page.
---

# Memory

The Memory buffer is an in-memory message queue that accumulates incoming message batches and releases them as a single merged batch when either a capacity threshold or a timeout is reached. It smooths out traffic spikes and provides backpressure when downstream processing cannot keep up: once the buffer holds `capacity` messages, further writes wait until the reader drains accumulated messages, so the slowdown propagates upstream instead of growing the buffer without bound.

Failed merges never drop retained messages: if accumulated batches cannot be merged (for example, incompatible schemas), the queue keeps them and the read fails with an error, leaving a retry possible. On close, the reader still drains whatever is pending before the stream ends, and a `flush` only wakes the reader — the timeout-based release keeps working afterwards.

> **Note:** In the current unified execution kernel, a stream-level `buffer: { type: "memory" }` block is accepted for compatibility but compiles to a no-op — the bounded inter-stage channels already provide buffering and backpressure. The semantics on this page describe the component contract itself, which applies when the memory buffer is used through the plugin registry directly.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `memory` |
| capacity | integer | yes | — | Maximum number of messages to accumulate. Once this many messages are held, writes wait (backpressure) until the reader releases them. Must be at least 1 — a capacity of `0` is rejected at build time. |
| timeout | duration | yes | — | Maximum time to wait before flushing accumulated batches, even if `capacity` has not been reached. Examples: `1ms`, `1s`, `1m`, `1h`. |

## Examples

```yaml validate=fragment wrap=buffer
buffer:
  type: "memory"
  capacity: 100
  timeout: "1s"
```

```yaml validate=full
streams:
  - input:
      type: "generate"
      context: '{ "value": 1 }'
      interval: 100ms
      batch_size: 1
    pipeline:
      thread_num: 4
      processors:
        - type: "json_to_arrow"
    buffer:
      type: "memory"
      capacity: 100
      timeout: "1s"
    output:
      type: "stdout"
```
