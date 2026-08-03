---
sidebar_label: NATS
---

# NATS

The NATS input connects to a NATS server and supports two modes: regular subscriptions (regular) and JetStream pull consumers (jet_stream).

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: input-nats-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| auth | object | no | — | no | NATS authentication configuration. |
| mode | object | yes | — | no | Select between plain NATS and JetStream subscriptions. |
| url | string | yes | — | yes | NATS server URL (e.g. nats://localhost:4222). |
<!-- END AUTO -->

## Examples

```yaml
input:
  type: "nats"
  url: "nats://localhost:4222"
  mode:
    type: "regular"
    subject: "my.subject"
    queue_group: "my_group"
```

```yaml
input:
  type: "nats"
  url: "nats://localhost:4222"
  mode:
    type: "jet_stream"
    stream: "my_stream"
    consumer_name: "my_consumer"
    durable_name: "my_durable"
  auth:
    token: "my_token"
```

## Output schema

The component preserves ArkFlow message metadata and uses the batch schema documented by the surrounding input or output.

## Error handling

Configuration errors are reported during validation. Runtime connection, decoding, or processing errors are logged with the component name; use the Troubleshooting guide to identify the failing boundary.

## Metrics

Monitor throughput, errors, retries, and end-to-end acknowledgement latency for this component. The deployment's metrics endpoint exposes the runtime counters when the control plane is enabled.

## See also

Use the generated reference as the source of truth for configuration. Validate a complete stream configuration before deployment.
