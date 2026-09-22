---
components: [input/kafka]
sidebar_label: Kafka
---

# Kafka

The Kafka input consumes messages from one or more Apache Kafka topics using a consumer group. Offsets are only advanced after the downstream output acknowledges the write (`enable.auto.offset.store=false`), giving at-least-once delivery across crashes.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | Constant value `"kafka"` |
| brokers | array&lt;string&gt; | yes | — | List of Kafka broker addresses, e.g. `["host1:9092","host2:9092"]` |
| topics | array&lt;string&gt; | yes | — | List of topics to subscribe to |
| consumer_group | string | yes | — | Consumer group ID, used for offset coordination and load balancing |
| client_id | string | no | — | Client ID, used for monitoring and logging |
| start_from_latest | boolean | no | `false` | When `true`, ignores committed offsets and starts consuming from the latest messages |
| fetch_min_bytes | integer | no | — | Minimum bytes required for the broker to respond to a fetch request |
| fetch_max_bytes | integer | no | — | Maximum bytes returned by a single fetch request |
| fetch_max_partition_bytes | integer | no | — | Maximum bytes returned per partition in a single fetch |
| fetch_wait_max_ms | integer | no | — | Maximum time (ms) the broker waits for enough data to accumulate before responding |
| security | object | no | — | SASL authentication and TLS settings; omit entirely for plaintext. See [Security](#security) |

## Security

The `security` block is identical for the Kafka input and output. The effective protocol is the explicitly declared `protocol`, or — when omitted — inferred from which sub-blocks are present:

| `sasl` block | `tls` block | Inferred protocol |
|--------------|-------------|-------------------|
| no | no | `plaintext` (default) |
| yes | no | `sasl_plaintext` |
| no | yes | `ssl` |
| yes | yes | `sasl_ssl` |

:::tip
`sasl.username`, `sasl.password`, and `tls` key material can reference
environment variables or files (`${env:KAFKA_PASSWORD}`, `${file:...}`) so
secrets stay out of the config file — see
[Secret references](../../reference/configuration.md#secret-references).
:::

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
When written literally, `sasl.username`, `sasl.password`, and `tls.*` values
are stored as plain text in the configuration. Prefer
[secret references](../../reference/configuration.md#secret-references)
(`${env:KAFKA_PASSWORD}`, `${file:...}`) so credentials stay out of the
config file; guard the configuration file and any control-plane storage
accordingly.
:::

Inconsistent blocks fail fast at configuration validation time (before any stream starts): a `sasl_*` protocol without a `sasl` block, missing SCRAM credentials, or an explicit `plaintext` protocol alongside a `sasl`/`tls` block are all rejected.

```yaml validate=fragment wrap=input
input:
  type: "kafka"
  brokers:
    - "broker1.example.com:9094"
  topics:
    - "events"
  consumer_group: "arkflow"
  start_from_latest: false
  security:
    protocol: sasl_ssl
    sasl:
      mechanism: scram-sha-256
      username: arkflow
      password: change-me
    tls:
      ca: /etc/arkflow/certs/ca.crt
```

Inline PEM is accepted wherever a certificate path is — useful when the configuration is managed centrally and no local file exists:

```yaml validate=fragment wrap=input
input:
  type: "kafka"
  brokers:
    - "broker1.example.com:9094"
  topics:
    - "events"
  consumer_group: "arkflow"
  start_from_latest: false
  security:
    sasl:
      mechanism: scram-sha-512
      username: arkflow
      password: change-me
    tls:
      ca: |
        -----BEGIN CERTIFICATE-----
        MIID...peer CA certificate...
        -----END CERTIFICATE-----
```

## Examples

```yaml validate=fragment wrap=input
input:
  type: "kafka"
  brokers:
    - "localhost:9092"
  topics:
    - "my_topic"
  consumer_group: "my_consumer_group"
  client_id: "my_client"
  start_from_latest: false
```

```yaml validate=fragment wrap=input
input:
  type: "kafka"
  brokers:
    - "kafka1:9092"
    - "kafka2:9092"
  topics:
    - "topic1"
    - "topic2"
  consumer_group: "app1_group"
  start_from_latest: true
  fetch_min_bytes: 1
  fetch_max_bytes: 52428800
  fetch_max_partition_bytes: 1048576
  fetch_wait_max_ms: 500
```

## Notes

- Messages automatically carry metadata columns such as `__meta_source`, `__meta_partition`, `__meta_offset`, `__meta_key`, `__meta_timestamp`, and `__meta_ingest_time`, plus the extended `__meta_ext.topic`.
- Offsets are advanced via `store_offset` only when `ack()` is called (after a successful downstream write), combined with periodic auto-commit to achieve at-least-once delivery.
