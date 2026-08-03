---
sidebar_label: MQTT
---

# MQTT

The MQTT input connects to an MQTT broker, subscribes to one or more topics, and receives real-time messages.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: input-mqtt-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| clean_session | boolean | no | `true` | no | Whether to use a clean session. |
| client_id | string | yes | — | no | Unique client identifier. |
| host | string | yes | — | no | MQTT broker hostname. |
| keep_alive | integer | no | — | no | Keep-alive interval in seconds. |
| password | string | no | — | no | Optional password. |
| port | integer | yes | — | no | MQTT broker port. |
| qos | integer | no | `0` | no | Quality of Service level. |
| topics | array | yes | — | yes | Topics to subscribe to (MQTT wildcards supported). |
| username | string | no | — | no | Optional username. |
<!-- END AUTO -->

## Examples

```yaml
input:
  type: "mqtt"
  host: "localhost"
  port: 1883
  client_id: "my_client"
  username: "user"
  password: "pass"
  topics:
    - "sensors/temperature"
    - "sensors/humidity"
  qos: 1
  clean_session: true
  keep_alive: 60
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
