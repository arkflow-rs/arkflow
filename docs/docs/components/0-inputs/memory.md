---
sidebar_label: Memory
---

# Memory

The Memory input reads messages from an in-memory queue that can be pre-seeded with initial messages in configuration. Mainly used for testing and development.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: input-memory-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| messages | array | no | — | no | Initial messages to enqueue. |
<!-- END AUTO -->

## Examples

```yaml
input:
  type: "memory"
  messages:
    - "Hello"
    - "World"
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
