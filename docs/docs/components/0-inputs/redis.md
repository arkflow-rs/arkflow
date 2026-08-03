---
sidebar_label: Redis
---

# Redis

The Redis input reads from Redis with both standalone and cluster connection modes, and supports Subscribe (channels / patterns) and List consumption modes.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: input-redis-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| mode | object | yes | — | no | Connection mode. |
| redis_type | object | yes | — | no | Data structure to consume from. |
<!-- END AUTO -->

## Examples

```yaml
input:
  type: "redis"
  mode:
    type: "single"
    url: "redis://localhost:6379"
  redis_type:
    type: "subscribe"
    subscribe:
      type: "channels"
      channels:
        - "news"
        - "events"
```

```yaml
input:
  type: "redis"
  mode:
    type: "single"
    url: "redis://localhost:6379"
  redis_type:
    type: "list"
    list:
      - "tasks"
      - "notifications"
```

## Output schema

The component preserves ArkFlow message metadata and uses the batch schema documented by the surrounding input or output.

## Error handling

Configuration errors are reported during validation. Runtime connection, decoding, or processing errors are logged with the component name; use the Troubleshooting guide to identify the failing boundary.

## Metrics

Monitor throughput, errors, retries, and end-to-end acknowledgement latency for this component. The deployment's metrics endpoint exposes the runtime counters when the control plane is enabled.

## See also

Use the generated reference as the source of truth for configuration. Validate a complete stream configuration before deployment.
