

# Session Window

The Session Window buffer groups messages into sessions defined by activity gaps. A new message extends the current session; if no message arrives within the configured `gap` duration the session is closed and all of its accumulated messages are emitted as a single batch. An optional `join` configuration lets you run a SQL join across multiple input sources at emission time.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: buffer-session_window-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| gap | string | yes | — | no | Maximum idle time before a session is closed (humantime). |
| join | object | no | — | no | Optional SQL join across input sources. |
<!-- END AUTO -->

## Examples

### Basic Configuration

```yaml
buffer:
  type: "session_window"
  gap: "5s"
```

### With Join Configuration

```yaml
buffer:
  type: "session_window"
  gap: "10s"
  join:
    query: "SELECT a.user_id, a.event_type, b.metadata FROM events a JOIN metadata b ON a.user_id = b.user_id"
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
