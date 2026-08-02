---
sidebar_label: NATS
---

# NATS

The NATS input connects to a NATS server and supports two modes: regular subscriptions (regular) and JetStream pull consumers (jet_stream).

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | Constant value `"nats"` |
| url | string | yes | — | NATS server URL, e.g. `nats://host:4222`; multiple servers separated by commas |
| mode | object | yes | — | Operating mode, see table below (tagged enum, distinguished by the `type` field) |
| auth | object | no | — | Authentication configuration, see table below |

### mode (regular)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | yes | `"regular"` |
| subject | string | yes | NATS subject to subscribe to |
| queue_group | string | no | Queue group name |

### mode (jet_stream)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | yes | `"jet_stream"` |
| stream | string | yes | Stream name |
| consumer_name | string | yes | Consumer name |
| durable_name | string | no | Durable consumer name |

### auth

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| username | string | no | Username |
| password | string | no | Password (used together with username) |
| token | string | no | Token; takes precedence over username/password when both are present |

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
