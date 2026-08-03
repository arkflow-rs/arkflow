

# Redis

The Redis output writes messages to Redis using one of four data-structure operations: Pub/Sub publish, List push, Hash set, or String set. It supports single-node and cluster connections.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: output-redis-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| mode | object | yes | — | no | Connection mode (single or cluster). |
| redis_type | object | yes | — | no | Destination data structure. |
| value_field | string | no | — | no | Record field used as the payload. |
<!-- END AUTO -->

## Examples

### Publish to a channel

```yaml
output:
  type: "redis"
  mode:
    type: "single"
    url: "redis://localhost:6379"
  redis_type:
    type: "publish"
    channel:
      type: "value"
      value: "notifications"
```

### Push to a List

```yaml
output:
  type: "redis"
  mode:
    type: "single"
    url: "redis://localhost:6379"
  redis_type:
    type: "list"
    key:
      type: "value"
      value: "events"
```

### Set a Hash field

```yaml
output:
  type: "redis"
  mode:
    type: "single"
    url: "redis://localhost:6379"
  redis_type:
    type: "hashes"
    key:
      type: "value"
      value: "user:1"
    field:
      type: "value"
      value: "status"
```

### Set a String

```yaml
output:
  type: "redis"
  mode:
    type: "single"
    url: "redis://localhost:6379"
  redis_type:
    type: "strings"
    key:
      type: "expr"
      expr: "concat('key:', id)"
```

### Cluster connection

```yaml
output:
  type: "redis"
  mode:
    type: "cluster"
    urls:
      - "redis://redis-1:6379"
      - "redis://redis-2:6379"
      - "redis://redis-3:6379"
  redis_type:
    type: "list"
    key:
      type: "value"
      value: "logs"
```

## Input schema

The component preserves ArkFlow message metadata and uses the batch schema documented by the surrounding input or output.

## Error handling

Configuration errors are reported during validation. Runtime connection, decoding, or processing errors are logged with the component name; use the Troubleshooting guide to identify the failing boundary.

## Metrics

Monitor throughput, errors, retries, and end-to-end acknowledgement latency for this component. The deployment's metrics endpoint exposes the runtime counters when the control plane is enabled.

## See also

Use the generated reference as the source of truth for configuration. Validate a complete stream configuration before deployment.
