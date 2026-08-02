---
sidebar_label: Multiple Inputs
---

# Multiple Inputs

Multiple Inputs merges several independent input components into a single logical stream. All child inputs are read concurrently, and messages enter the same pipeline in arrival order. Each child input may carry a `name`, which is written to `__meta_source` so downstream stages can distinguish the origin.

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

```yaml
input:
  type: "multiple_inputs"
  inputs:
    - name: "kafka_source"
      type: "kafka"
      brokers: ["localhost:9092"]
      topics: ["topic1"]
      consumer_group: "group1"
    - name: "http_api"
      type: "http"
      address: "0.0.0.0:8080"
      path: "/webhook"
```

## Notes

- Every non-empty `name` must be unique; duplicates or empty names cause the build to fail.
- If any child input returns `EOF` or `Disconnection`, that sub-stream ends while the others continue.
