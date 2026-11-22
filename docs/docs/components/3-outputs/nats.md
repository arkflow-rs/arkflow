---
components: [output/nats]
description: ArkFlow documentation page.
---

# NATS

The NATS output publishes messages to a NATS server, either to a regular subject or to a JetStream stream. It supports optional username/password or token authentication.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type  | string | yes | — | Fixed value `"nats"` |
| url | string | yes | — | NATS server URL (e.g. `nats://localhost:4222`). |
| mode | object | yes | — | Publishing mode (see below). |
| auth | object | no | — | Authentication configuration (see below). |
| value_field | string | no | — | Binary or string column whose per-row value becomes that message's payload (a null value or a missing column is an error). When unset, the codec encoding of the batch applies. |

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

An expression must yield a non-null string for every row: a NULL result for any row fails the batch with the expression and the row named (wrap it in `COALESCE(expr, 'fallback')` or filter such rows upstream).

A per-row destination result must carry one destination per payload row: a shorter result fails the batch with both counts named instead of silently dropping the unmatched rows.

### auth

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| username | string | no | — | Username (use with `password`). |
| password | string | no | — | Password (use with `username`). |
| token | string | no | — | Authentication token. |

Only one of username/password or token should be configured; if both are present, username/password takes precedence.

## Examples

### Regular subject with username/password

```yaml validate=fragment wrap=output
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

```yaml validate=fragment wrap=output
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

## TLS

Use a `tls://` URL scheme to connect over TLS (for example
`tls://nats.example.com:4422`); async-nats negotiates TLS natively for
`tls://` URLs and requires the server to support TLS.

## Error behavior

In regular (core NATS) mode, a send/transport failure surfaces as
`Disconnection`; local rejections (payload too large, invalid subject) are
deterministic `Process` errors — reconnecting cannot fix them. A per-row
subject vector shorter than the payload count fails before the first
publish with a counting `Process` error; expression-evaluation failures
are also deterministic.

Note on JetStream mode: `write` awaits only the send, not the server's
publish acknowledgment (the ack future is not held), so a message the
server later rejects is not reported by `write`. Applications that need
server-confirmed delivery should treat JetStream mode accordingly.
