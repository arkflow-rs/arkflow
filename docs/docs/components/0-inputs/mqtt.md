---
components: [input/mqtt]
sidebar_label: MQTT
---

# MQTT

The MQTT input connects to an MQTT broker, subscribes to one or more topics, and receives real-time messages.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | Constant value `"mqtt"` |
| host | string | yes | — | MQTT broker address |
| port | integer | yes | — | MQTT broker port |
| client_id | string | yes | — | Unique client identifier |
| topics | array&lt;string&gt; | yes | — | List of topics to subscribe to (wildcards supported) |
| username | string | no | — | Authentication username |
| password | string | no | — | Authentication password |
| qos | integer | no | — | QoS level (0, 1, 2) |
| clean_session | boolean | no | — | Whether to use a clean session |
| keep_alive | integer | no | — | Keep-alive interval (seconds) |

## TLS

Set the `tls` block to connect over TLS (MQTT over port 8883):

```yaml validate=fragment wrap=input
input:
  type: "mqtt"
  host: "localhost"
  port: 8883
  client_id: "tls-client"
  topics: ["demo"]
  tls:
    enabled: true
    ca: "/etc/arkflow/certs/ca.pem"
```

`enabled` defaults to true when the block is present (set `enabled: false` to
keep the block as documentation); `ca` verifies the broker certificate;
`client_cert`/`client_key` enable mTLS and must be configured together. `ca`
is optional with mTLS: without it the broker is verified against the
platform trust store (e.g. public brokers with system-issued certificates).
`client_cert` without `client_key` (or vice versa) is a configuration error.
Omitting the `tls` block keeps plain TCP. The `client_id` must be unique per
connection.

## Examples

```yaml validate=fragment wrap=input
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
