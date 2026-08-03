---
sidebar_label: Generate
---

# Generate

The Generate input produces synthetic text messages at a fixed interval, primarily for testing, demos, and benchmarking. Each read returns `batch_size` copies of the same `context` string.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: input-generate-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| batch_size | integer | no | `1` | no | Number of messages returned per read call. |
| context | string | yes | — | no | Payload string emitted on every read. |
| count | integer | no | — | no | Optional total number of messages to emit before signalling EOF. |
| interval | string | yes | — | yes | Delay between batches (e.g. '100ms', '1s'). Accepts any humantime duration. |
<!-- END AUTO -->

## Examples

```yaml
input:
  type: "generate"
  context: '{ "timestamp": 1625000000000, "value": 10, "sensor": "temp_1" }'
  interval: 1ms
  batch_size: 1000
  count: 10000
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
