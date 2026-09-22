---
components: [output/kafka]
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
| security | object | no | — | SASL authentication and TLS settings; omit entirely for plaintext. See [Security](#security). |

### Security

The `security` block is identical for the Kafka input and output. The effective protocol is the explicitly declared `protocol`, or — when omitted — inferred from which sub-blocks are present: both `sasl`+`tls` → `sasl_ssl`, only `sasl` → `sasl_plaintext`, only `tls` → `ssl`, neither → `plaintext`.

| Field | Type | Description |
|-------|------|-------------|
| protocol | string | `plaintext` \| `ssl` \| `sasl_plaintext` \| `sasl_ssl`. Explicit declaration wins over inference; contradicting it with a provided sub-block is a configuration error. |
| sasl.mechanism | string | `plain` \| `scram-sha-256` \| `scram-sha-512` |
| sasl.username / sasl.password | string | Credentials; required and non-empty for `plain`/`scram-*` mechanisms |
| tls.ca | string | CA certificate verifying the broker: a file path **or inline PEM text** (auto-detected via the `-----BEGIN` marker) |
| tls.cert / tls.key | string | Client certificate and private key for mTLS: file path or inline PEM |
| tls.key_password | string | Password protecting the client private key |
| tls.insecure_skip_verify | boolean | `true` disables broker certificate verification — development/testing only |

:::warning
`sasl.username`, `sasl.password`, and `tls.*` values are stored as plain text in the configuration. Guard the configuration file and any control-plane storage accordingly.
:::

Inconsistent blocks fail fast at configuration validation time (before any stream starts): a `sasl_*` protocol without a `sasl` block, missing SCRAM credentials, or an explicit `plaintext` protocol alongside a `sasl`/`tls` block are all rejected.

```yaml validate=fragment wrap=output
output:
  type: "kafka"
  brokers:
    - "broker1.example.com:9094"
  topic:
    type: "value"
    value: "events-out"
  security:
    protocol: sasl_ssl
    sasl:
      mechanism: scram-sha-512
      username: arkflow
      password: change-me
    tls:
      ca: /etc/arkflow/certs/ca.crt
      cert: /etc/arkflow/certs/client.crt
      key: /etc/arkflow/certs/client.key
```

### Expression objects

`topic` and `key` are `Expr<String>` objects with one of these shapes:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | yes | `value` (static) or `expr` (SQL expression). |
| value | string | yes (`value`) | Static string value. |
| expr | string | yes (`expr`) | SQL expression evaluated per message. |

## Examples

### Static topic and key

```yaml validate=fragment wrap=output
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

```yaml validate=fragment wrap=output
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

```yaml validate=fragment wrap=output
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
- See [Exactly-once processing](../../build/exactly-once.md) for the end-to-end delivery-semantics contract.
