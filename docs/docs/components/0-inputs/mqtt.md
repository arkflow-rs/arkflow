---
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
