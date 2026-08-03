---
sidebar_label: Modbus
---

# Modbus

The Modbus input polls a Modbus TCP device at a fixed interval and supports four register types: coils, discrete_inputs, holding_registers, and input_registers.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: input-modbus-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| addr | string | yes | — | no | Modbus server address (host:port). |
| interval | string | yes | — | yes | Poll interval (humantime). |
| points | array | yes | — | no | Points to read on every poll. |
| slave_id | integer | yes | — | no | Modbus unit / slave ID. |
<!-- END AUTO -->

## Examples

```yaml
input:
  type: "modbus"
  addr: "192.168.1.100:502"
  slave_id: 1
  interval: "1s"
  points:
    - type: "holding_registers"
      name: "temperature"
      address: 100
      quantity: 2
    - type: "coils"
      name: "status_flags"
      address: 200
      quantity: 2
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
