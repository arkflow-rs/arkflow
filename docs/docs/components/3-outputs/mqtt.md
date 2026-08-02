# MQTT

The MQTT output publishes each message to an MQTT broker topic. It supports QoS 0/1/2, clean sessions, keep-alive, retained messages, and optional username/password authentication.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type  | string | yes | — | Fixed value `"mqtt"` |
| host | string | yes | — | MQTT broker hostname. |
| port | integer | yes | — | MQTT broker port. |
| client_id | string | yes | — | Client identifier. |
| username | string | no | — | Username for authentication. |
| password | string | no | — | Password for authentication. |
| topic | object | yes | — | Destination topic (expression; see below). |
| qos | integer | no | — | Quality of Service: `0`, `1`, or `2`. |
| clean_session | boolean | no | — | Whether to use a clean session. |
| keep_alive | integer | no | — | Keep-alive interval in seconds. |
| retain | boolean | no | — | Whether to retain the message on the broker. |
| value_field | string | no | — | Record field used as the message payload. |

### topic

`topic` is an `Expr<String>` object with one of these shapes:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | yes | `value` (static) or `expr` (SQL expression). |
| value | string | yes (`value`) | Static topic name. |
| expr | string | yes (`expr`) | SQL expression evaluated per message. |

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
