

# NATS

The NATS output publishes messages to a NATS server, either to a regular subject or to a JetStream stream. It supports optional username/password or token authentication.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: output-nats-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| auth | object | no | — | no | NATS authentication configuration. |
| mode | object | yes | — | no | Select regular or JetStream publishing. |
| url | string | yes | — | yes | NATS server URL. |
| value_field | string | no | — | no | Record field used as the payload. |
<!-- END AUTO -->

## Examples

### Regular subject with username/password

```yaml
output:
  type: "nats"
  url: "nats://localhost:4222"
  mode:
    type: "regular"
    subject:
      type: "expr"
      expr: "concat('orders.', id)"
  auth:
    username: "user"
    password: "pass"
  value_field: "message"
```

### JetStream with token

```yaml
output:
  type: "nats"
  url: "nats://localhost:4222"
  mode:
    type: "jet_stream"
    subject:
      type: "value"
      value: "orders.new"
  auth:
    token: "secret-token"
```

## Input schema

The component preserves ArkFlow message metadata and uses the batch schema documented by the surrounding input or output.

## Error handling

Configuration errors are reported during validation. Runtime connection, decoding, or processing errors are logged with the component name; use the Troubleshooting guide to identify the failing boundary.

## Metrics

Monitor throughput, errors, retries, and end-to-end acknowledgement latency for this component. The deployment's metrics endpoint exposes the runtime counters when the control plane is enabled.

## See also

Use the generated reference as the source of truth for configuration. Validate a complete stream configuration before deployment.
