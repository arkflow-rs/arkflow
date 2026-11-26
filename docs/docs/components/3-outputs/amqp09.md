---
components: [output/amqp09]
sidebar_label: AMQP 0.9 (RabbitMQ)
description: ArkFlow AMQP 0.9 (RabbitMQ) output component documentation.
---

# AMQP 0.9 (RabbitMQ)

The AMQP 0.9 output publishes messages to an AMQP 0.9/0.9.1 exchange such as RabbitMQ, with optional exchange declaration, per-row dynamic routing keys, AMQP message properties, and publisher confirms.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | Fixed value `"amqp09"` |
| url | string | yes | — | AMQP broker URL (e.g. `amqp://guest:guest@localhost:5672/%2f` or `amqps://...`) |
| exchange | string | yes | — | Exchange name to publish to (empty string `""` targets the AMQP default exchange) |
| exchange_type | string | no | `direct` | Exchange type when declaring the exchange (`direct`, `fanout`, `topic`, `headers`) |
| routing_key | object | yes | — | Routing key for publishing (expression; see below) |
| declare_exchange | boolean | no | `false` | Declare the exchange if it does not exist (cannot be `true` when `exchange` is `""`) |
| durable | boolean | no | `true` | Mark the declared exchange as durable |
| auto_delete | boolean | no | `false` | Auto-delete the declared exchange when unused |
| persistent | boolean | no | — | Sets AMQP `delivery_mode` to persistent (`2`) when `true` or transient (`1`) when `false` |
| confirm | boolean | no | `false` | Enable AMQP publisher confirms and return an error if the broker NACKs a message |
| priority | integer | no | — | AMQP message priority (`0`–`255`) |
| expiration | integer | no | — | Per-message TTL in milliseconds |
| content_type | string | no | — | MIME content type header (e.g. `application/json`) |
| content_encoding | string | no | — | MIME content encoding header (e.g. `utf-8`) |
| value_field | string | no | — | Binary or string column whose per-row value becomes that message's payload (a null value or missing column is an error). When unset, the codec encoding of the batch applies. |

### routing_key (`Expr<String>`)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | yes | `value` (static) or `expr` (SQL expression) |
| value | string | yes (`value`) | Static routing key |
| expr | string | yes (`expr`) | SQL expression evaluated per row |

An expression must yield a non-null string for every row: a NULL result for any row fails the batch with the expression and the row named (wrap it in `COALESCE(expr, 'fallback')` or filter such rows upstream).

A per-row destination result must carry one routing key per payload row: a shorter result fails the batch with both counts named instead of silently misrouting or dropping unmatched rows.

## Examples

### Topic exchange with dynamic routing key and publisher confirms

```yaml validate=fragment wrap=output
output:
  type: "amqp09"
  url: "amqp://guest:guest@localhost:5672/%2f"
  exchange: "orders"
  exchange_type: "topic"
  routing_key:
    type: "expr"
    expr: "concat('orders.', region)"
  declare_exchange: true
  durable: true
  persistent: true
  confirm: true
  content_type: "application/json"
  value_field: "payload"
```

### Default exchange publishing directly to a queue

```yaml validate=fragment wrap=output
output:
  type: "amqp09"
  url: "amqp://guest:guest@localhost:5672/%2f"
  exchange: ""
  routing_key:
    type: "value"
    value: "tasks-queue"
  persistent: true
  confirm: true
```

## TLS

Use an `amqps://` URL scheme to connect over TLS (for example `amqps://user:pass@broker.example.com:5671/%2f`). The client negotiates TLS natively via `rustls` using system root certificates.

## Error behavior

Transport, IO, heartbeat, and closed connection/channel errors surface as `Disconnection`. Broker protocol rejections, broker NACKs under `confirm: true`, expression-evaluation failures, and per-row routing-key alignment mismatches surface as deterministic `Process` errors.
