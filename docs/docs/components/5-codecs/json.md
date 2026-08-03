---
sidebar_label: JSON
---

# JSON

The JSON codec converts between line-delimited JSON byte payloads and columnar Arrow `RecordBatch`es. Decoding uses Arrow's schema inference to map JSON objects to columns; encoding writes each row as one JSON object separated by newlines. It is the most common codec for attaching to inputs that emit JSON (Kafka, Redis, HTTP, etc.).

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: codec-json-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| pretty | boolean | no | `false` | no | Pretty-print JSON output. |
<!-- END AUTO -->

## Examples

```yaml
input:
  type: kafka
  brokers:
    - localhost:9092
  topics:
    - events
  consumer_group: arkflow
  codec:
    type: json
```

```yaml
output:
  type: stdout
  codec:
    type: json
```

## Output schema

The component preserves ArkFlow message metadata and uses the batch schema documented by the surrounding input or output.

## Error handling

Configuration errors are reported during validation. Runtime connection, decoding, or processing errors are logged with the component name; use the Troubleshooting guide to identify the failing boundary.

## Metrics

Monitor throughput, errors, retries, and end-to-end acknowledgement latency for this component. The deployment's metrics endpoint exposes the runtime counters when the control plane is enabled.

## See also

Use the generated reference as the source of truth for configuration. Validate a complete stream configuration before deployment.
