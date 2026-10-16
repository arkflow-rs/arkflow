---
components: [multiple_inputs]
sidebar_label: Multiple Inputs
---

# Multiple Inputs

Multiple Inputs merges several independent input components into a single logical stream. All child inputs are read concurrently, and messages enter the same pipeline in arrival order. Each child input may carry a `name`, which is written to `__meta_source` so downstream stages can distinguish the origin.

Messages are forwarded through a bounded internal channel (capacity 1024, the same bound as the pipeline's inter-stage edges): when the pipeline cannot keep up, the child readers wait on send and the slowdown propagates back to the sources instead of growing memory without limit.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | Constant value `"multiple_inputs"` |
| inputs | array&lt;object&gt; | yes | — | Array of child input configurations; element structure is described below |

### inputs[]

Each element is a standard input configuration (with its own `type` and fields) plus an optional `name`:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | yes | Child input type, e.g. `kafka`, `http` |
| name | string | no | Logical name for this source; when non-empty and globally unique, it is written to `__meta_source` |
| ... | ... | ... | The configuration fields specific to this input type |

## Examples

```yaml validate=fragment wrap=input
input:
  type: "multiple_inputs"
  inputs:
    - name: "kafka_source"
      type: "kafka"
      brokers: ["localhost:9092"]
      topics: ["topic1"]
      consumer_group: "group1"
      start_from_latest: false
    - name: "http_api"
      type: "http"
      address: "0.0.0.0:8080"
      path: "/webhook"
```

## Notes

- Every non-empty `name` must be unique; duplicates or empty names cause the build to fail.
- If any child input returns `EOF`, the whole merged input is treated as end-of-stream (the pipeline finishes normally); a `Disconnection` from any child triggers a reconnect of the whole input — all children are reconnected and their readers restarted.
- When the engine reconnects this input (after a `Disconnection`), the previous set of child readers is stopped and awaited before a fresh set spawns — a child input is never read by two tasks at once, so reconnects do not duplicate deliveries.
- A child input error is surfaced to the pipeline once (the reader for that child then exits); retry and reconnect decisions stay with the engine's input error handling.
