---
description: ArkFlow documentation page.
---

# Kafka

The Kafka output produces messages to an Apache Kafka topic using librdkafka. It supports key-based partitioning, compression, configurable acknowledgments, and optional exactly-once transactional production.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type  | string | yes | — | Fixed value `"kafka"` |
| brokers | `array<string>` | yes | — | List of Kafka broker addresses. |
| topic | object | yes | — | Destination topic (expression; see below). |
| key | object | no | — | Message key for partitioning (expression; see below). |
| client_id | string | no | — | Client identifier. |
| compression | string | no | — | One of `none`, `gzip`, `snappy`, `lz4`. |
| acks | string | no | — | Acknowledgment level: `0`, `1`, or `all`. |
| value_field | string | no | — | Record field used as the message payload. |
| exactly_once | boolean | no | `false` | Enable exactly-once transactional production (L2). |
| transactional_id | string | no | — | Stable transactional id; required when `exactly_once` is `true`. |

### Expression objects

`topic` and `key` are `Expr<String>` objects with one of these shapes:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | yes | `value` (static) or `expr` (SQL expression). |
| value | string | yes (`value`) | Static string value. |
| expr | string | yes (`expr`) | SQL expression evaluated per message. |

## Examples

### Static topic and key

```yaml
output:
  type: "kafka"
  brokers:
    - "localhost:9092"
  topic:
    type: "value"
    value: "my-topic"
  key:
    type: "value"
    value: "my-key"
  client_id: "my-client"
  compression: "snappy"
  acks: "1"
```

### Dynamic topic via SQL expression

```yaml
output:
  type: "kafka"
  brokers:
    - "localhost:9092"
  topic:
    type: "expr"
    expr: "concat('1','x')"
  acks: "all"
  value_field: "message"
```

### Exactly-once production

```yaml
output:
  type: "kafka"
  brokers:
    - "localhost:9092"
  topic:
    type: "value"
    value: "events"
  exactly_once: true
  transactional_id: "arkflow-events-tx"
  acks: "all"
```

## Notes

- When `exactly_once: true`, `transactional_id` must be a non-empty value that is stable across restarts so the broker can fence stale producer epochs (zombie fencing). The builder rejects the configuration otherwise.
- With exactly-once enabled, each acknowledged message batch is produced inside one Kafka transaction (begin → send → commit). On failure the transaction is aborted and the batch is replayed.
- See [Exactly-once processing](../../concepts/6-exactly-once.md) for the end-to-end delivery-semantics contract.
