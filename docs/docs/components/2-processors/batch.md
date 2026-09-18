---
components: [batch]
description: ArkFlow documentation page.
---

# Batch

The Batch processor accumulates incoming message batches and flushes them as a single merged batch when either a configured message count is reached or a timeout elapses. It is useful for grouping small batches together to improve downstream throughput.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `batch` |
| count | integer | yes | — | Number of message batches to accumulate before flushing. |
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
