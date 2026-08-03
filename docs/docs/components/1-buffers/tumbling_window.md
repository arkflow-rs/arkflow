

# Tumbling Window

The Tumbling Window buffer groups messages into fixed-size, non-overlapping time windows. Every configured `interval`, all messages accumulated within the current window are emitted together as a single batch; each message belongs to exactly one window. An optional `join` configuration lets you run a SQL join across multiple input sources at emission time.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: buffer-tumbling_window-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| interval | string | yes | — | yes | Window duration (humantime). |
| join | object | no | — | no | Optional SQL join across input sources. |
<!-- END AUTO -->

## Examples

### Basic Configuration

```yaml
buffer:
  type: "tumbling_window"
  interval: "1s"
```

### With Join Configuration

```yaml
buffer:
  type: "tumbling_window"
  interval: "5s"
  join:
    query: "SELECT a.id, a.name, b.value FROM input1 a JOIN input2 b ON a.id = b.id"
    codec:
      type: "json"
```

## Output schema

The component preserves ArkFlow message metadata and uses the batch schema documented by the surrounding input or output.

## Error handling

Configuration errors are reported during validation. Runtime connection, decoding, or processing errors are logged with the component name; use the Troubleshooting guide to identify the failing boundary.

## Metrics

Monitor throughput, errors, retries, and end-to-end acknowledgement latency for this component. The deployment's metrics endpoint exposes the runtime counters when the control plane is enabled.

## See also

Use the generated reference as the source of truth for configuration. Validate a complete stream configuration before deployment.
