---
components: [input/amqp09]
sidebar_label: AMQP 0.9 (RabbitMQ)
---

# AMQP 0.9 (RabbitMQ)

The AMQP 0.9 input consumes messages from an AMQP 0.9/0.9.1 broker such as RabbitMQ. As a channel-based input it follows the engine's cancellation-safety contract: dropping a pending read never loses an already-decoded message (see [Input Delivery Semantics](../../build/delivery-semantics.md#cancellation-safety)).

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | Constant value `"amqp09"` |
| url | string | yes | — | AMQP broker URL, e.g. `amqp://guest:guest@localhost:5672/%2f` or `amqps://...` |
| queue | string | yes | — | Queue name to consume from |
| exchange | string | no | — | Optional exchange name; when set, binds `queue` to this exchange |
| exchange_type | string | no | `direct` | Exchange type when declaring an exchange (`direct`, `fanout`, `topic`, `headers`) |
| routing_key | string | no | `""` | Routing key used when binding `queue` to `exchange` |
| declare_queue | boolean | no | `false` | Declare the queue if it does not already exist |
| declare_exchange | boolean | no | `false` | Declare the exchange if it does not already exist (requires a non-empty `exchange`) |
| durable | boolean | no | `true` | Mark declared exchange/queue as durable |
| auto_delete | boolean | no | `false` | Auto-delete declared exchange/queue when unused |
| exclusive | boolean | no | `false` | Declare the queue as exclusive to this connection |
| consumer_tag | string | no | `""` | Consumer tag identifying this consumer (empty lets the broker assign one) |
| auto_ack | boolean | no | `false` | Automatically acknowledge deliveries on receipt; when `false`, acknowledgements are sent after downstream processing completes |
| prefetch_count | integer | no | — | Per-channel QoS prefetch count (`0`–`65535`) |

## Examples

### Topic exchange with manual acknowledgement

```yaml validate=fragment wrap=input
input:
  type: "amqp09"
  url: "amqp://guest:guest@localhost:5672/%2f"
  queue: "orders-in"
  exchange: "orders"
  exchange_type: "topic"
  routing_key: "orders.#"
  declare_exchange: true
  declare_queue: true
  durable: true
  auto_ack: false
  prefetch_count: 64
```

### Fanout exchange

```yaml validate=fragment wrap=input
input:
  type: "amqp09"
  url: "amqp://guest:guest@localhost:5672/%2f"
  queue: "audit-log"
  exchange: "events-fanout"
  exchange_type: "fanout"
  declare_exchange: true
  declare_queue: true
```

## TLS

Use an `amqps://` URL scheme to connect over TLS (for example `amqps://user:pass@broker.example.com:5671/%2f`). The client negotiates TLS natively via `rustls` using system root certificates.

## Read errors

If the AMQP consumer stream ends abnormally or returns a delivery error, the input emits `Disconnection` so the engine reconnects rather than terminating the job as `EOF`.
