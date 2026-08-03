

# MQTT

The MQTT output publishes each message to an MQTT broker topic. It supports QoS 0/1/2, clean sessions, keep-alive, retained messages, and optional username/password authentication.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: output-mqtt-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| clean_session | boolean | no | `true` | no |  |
| client_id | string | yes | — | no | Client identifier. |
| host | string | yes | — | no | MQTT broker hostname. |
| keep_alive | integer | no | — | no | Keep-alive interval in seconds. |
| password | string | no | — | no | Optional password. |
| port | integer | yes | — | no | MQTT broker port. |
| qos | integer | no | `0` | no | Quality of Service. |
| retain | boolean | no | `false` | no | Whether to retain the message on the broker. |
| topic | string | yes | — | no | Destination topic (supports \{field\} placeholders). |
| username | string | no | — | no | Optional username. |
<!-- END AUTO -->

## Examples

### Static topic

```yaml
output:
  type: "mqtt"
  host: "localhost"
  port: 1883
  client_id: "my-client"
  username: "user"
  password: "pass"
  topic:
    type: "value"
    value: "my-topic"
  qos: 2
  clean_session: true
  keep_alive: 60
  retain: true
  value_field: "message"
```

### Dynamic topic via SQL expression

```yaml
output:
  type: "mqtt"
  host: "localhost"
  port: 1883
  client_id: "sensor-client"
  topic:
    type: "expr"
    expr: "concat('sensor/', id)"
  qos: 1
```

## Input schema

The component preserves ArkFlow message metadata and uses the batch schema documented by the surrounding input or output.

## Error handling

Configuration errors are reported during validation. Runtime connection, decoding, or processing errors are logged with the component name; use the Troubleshooting guide to identify the failing boundary.

## Metrics

Monitor throughput, errors, retries, and end-to-end acknowledgement latency for this component. The deployment's metrics endpoint exposes the runtime counters when the control plane is enabled.

## See also

Use the generated reference as the source of truth for configuration. Validate a complete stream configuration before deployment.
