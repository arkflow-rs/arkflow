---
sidebar_label: Generate
---

# Generate

The Generate input produces synthetic text messages at a fixed interval, primarily for testing, demos, and benchmarking. Each read returns `batch_size` copies of the same `context` string.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | Constant value `"generate"` |
| context | string | yes | — | Payload text for each message |
| interval | duration | yes | — | Interval between reads, e.g. `1ms`, `1s`, `1m` (the first read is immediate) |
| count | integer | no | — | Total number of messages; returns EOF once reached. Unset means unlimited |
| batch_size | integer | no | `1` | Number of messages returned per read |

## Examples

```yaml
input:
  type: "generate"
  context: '{ "timestamp": 1625000000000, "value": 10, "sensor": "temp_1" }'
  interval: 1ms
  batch_size: 1000
  count: 10000
```
