---
description: ArkFlow documentation page.
---

# Sliding Window

The Sliding Window buffer groups messages into overlapping windows that advance over time. Up to `window_size` messages are emitted as a single batch on each `interval`; after each emission the window slides forward by `slide_size` messages, so when `slide_size` is smaller than `window_size` the same message can appear in multiple consecutive emitted batches.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `sliding_window` |
| window_size | integer | yes | — | Number of messages to include in each emitted window. |
| interval | duration | yes | — | Time between window emissions. Examples: `1ms`, `1s`, `1m`, `1h`. |
| slide_size | integer | yes | — | Number of messages to advance the window after each emission. Controls the overlap between consecutive windows. |

## Examples

```yaml
buffer:
  type: "sliding_window"
  window_size: 100
  interval: "1s"
  slide_size: 10
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
      type: "sliding_window"
      window_size: 100
      interval: "1s"
      slide_size: 10
    output:
      type: "stdout"
```
