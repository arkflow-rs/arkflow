# Memory

The Memory buffer is an in-memory message queue that accumulates incoming message batches and releases them as a single merged batch when either a capacity threshold or a timeout is reached. It smooths out traffic spikes and provides backpressure when downstream processing cannot keep up.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `memory` |
| capacity | integer | yes | — | Maximum number of message batches to accumulate before flushing. |
| timeout | duration | yes | — | Maximum time to wait before flushing accumulated batches, even if `capacity` has not been reached. Examples: `1ms`, `1s`, `1m`, `1h`. |

## Examples

```yaml
buffer:
  type: "memory"
  capacity: 100
  timeout: "1s"
```

```yaml
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
