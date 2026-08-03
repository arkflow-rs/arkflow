# NATS

The NATS output publishes messages to a NATS server, either to a regular subject or to a JetStream stream. It supports optional username/password or token authentication.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type  | string | yes | — | Fixed value `"nats"` |
| url | string | yes | — | NATS server URL (e.g. `nats://localhost:4222`). |
| mode | object | yes | — | Publishing mode (see below). |
| auth | object | no | — | Authentication configuration (see below). |
| value_field | string | no | — | Record field used as the message payload. |

### mode

`mode` is a tagged object (selected by its `type` field).

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `regular` or `jet_stream`. |
| subject | object | yes | — | NATS subject to publish to (expression; see below). |

### subject (`Expr<String>`)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | yes | `value` (static) or `expr` (SQL expression). |
| value | string | yes (`value`) | Static subject name. |
| expr | string | yes (`expr`) | SQL expression evaluated per message. |

### auth

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| username | string | no | — | Username (use with `password`). |
| password | string | no | — | Password (use with `username`). |
| token | string | no | — | Authentication token. |

Only one of username/password or token should be configured; if both are present, username/password takes precedence.

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
