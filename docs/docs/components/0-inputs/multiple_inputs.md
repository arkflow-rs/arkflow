---
sidebar_label: Multiple Inputs
---

# Multiple Inputs

Multiple Inputs merges several independent input components into a single logical stream. All child inputs are read concurrently, and messages enter the same pipeline in arrival order. Each child input may carry a `name`, which is written to `__meta_source` so downstream stages can distinguish the origin.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: input-multiple_inputs-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| inputs | array | yes | — | no | List of input components to combine. |
<!-- END AUTO -->

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

### Production usage

```yaml
# Add retries, batching, and observability appropriate to your deployment.
```

## Output schema

The component preserves ArkFlow message metadata and uses the batch schema documented by the surrounding input or output.

## Error handling

Configuration errors are reported during validation. Runtime connection, decoding, or processing errors are logged with the component name; use the Troubleshooting guide to identify the failing boundary.

## Metrics

Monitor throughput, errors, retries, and end-to-end acknowledgement latency for this component. The deployment's metrics endpoint exposes the runtime counters when the control plane is enabled.

## See also

Use the generated reference as the source of truth for configuration. Validate a complete stream configuration before deployment.
