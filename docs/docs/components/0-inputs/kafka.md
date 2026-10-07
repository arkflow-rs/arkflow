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
| fetch_min_bytes | integer | no | — | Minimum bytes required for the broker to respond to a fetch request. The default (`1`) is latency-friendly: the broker answers as soon as any data is available. Larger values make the broker wait for more data (up to `fetch_wait_max_ms`) — a throughput lever that adds first-message latency in quiet periods. |
| fetch_max_bytes | integer | no | — | Maximum bytes returned by a single fetch request |
| fetch_max_partition_bytes | integer | no | — | Maximum bytes returned per partition in a single fetch |
| fetch_wait_max_ms | integer | no | — | Maximum time (ms) the broker waits for `fetch_min_bytes` of data to accumulate before responding. Only relevant when `fetch_min_bytes` is greater than 1; with `fetch_min_bytes: 1` the broker responds immediately and this setting never triggers. |
| batch_max_rows | integer | no | `1024` | Maximum number of messages aggregated into one read batch. Values below 1 are clamped to 1. Lowering this is **not** a latency optimization — batching never waits; smaller values only shrink the at-least-once replay unit after a failure. |
| batch_max_bytes | integer | no | `8388608` (8 MiB) | Maximum accumulated payload bytes per read batch; the first message is always included even beyond this bound. Values below 1 are clamped to 1. |
| security | object | no | — | SASL authentication and TLS settings; omit entirely for plaintext. See [Security](#security) |
| transactional_offsets | boolean | no | `false` | L3 exactly-once: register this consumer group for in-transaction offset commits by a paired Kafka output's `offset_commit_group`. `ack()` then advances only the in-memory frontier — broker group offsets advance exclusively inside the output's producer transactions. |

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

## Throughput vs latency

Batching itself is latency-neutral: the first message is awaited (blocking) and buffered messages drain immediately — no time is ever spent waiting to fill a batch, and low traffic yields single-message batches. The throughput/latency trade-off lives in the **broker-side fetch knobs** and the **batch bounds**:

**Latency-first: keep the defaults.** `fetch_min_bytes` defaults to `1`, so the broker responds as soon as any data arrives (`fetch_wait_max_ms` never triggers at that setting), and the batch bounds need no lowering — a smaller `batch_max_rows` only shrinks the replay unit after a failure, it does not reduce latency.

```yaml validate=fragment wrap=input
input:
  type: "kafka"
  brokers:
    - "localhost:9092"
  topics:
    - "events"
  consumer_group: "latency-group"
  start_from_latest: false
  # Defaults are latency-friendly: fetch_min_bytes=1, batch_max_rows=1024
```

**Throughput-first: raise the batch bounds and let the broker accumulate.** A larger `fetch_min_bytes` makes the broker return more data per fetch (waiting up to `fetch_wait_max_ms`), which fills the client queue and lets one `read()` aggregate a larger batch. The costs: the first message of a quiet period can wait up to `fetch_wait_max_ms`; a failed batch replays as a whole segment (up to `batch_max_rows` messages); memory grows with `batch_max_rows × message size`.

```yaml validate=fragment wrap=input
input:
  type: "kafka"
  brokers:
    - "localhost:9092"
  topics:
    - "events"
  consumer_group: "throughput-group"
  start_from_latest: false
  batch_max_rows: 8192
  batch_max_bytes: 67108864
  fetch_min_bytes: 1048576
  fetch_max_bytes: 104857600
  fetch_max_partition_bytes: 8388608
```

Under sustained load, larger batches do **not** raise end-to-end latency — queue wait dominates, and higher throughput drains the backlog faster. What does grow is the per-batch processing step (the next `read()` starts after the current batch finishes processing), which shows up as longer per-dispatch processing times, not as slower arrival-to-sink latency.

## Notes

- One `read()` aggregates multiple messages into a single batch: the first message is awaited (blocking), then already-buffered messages are drained without waiting, bounded by `batch_max_rows` / `batch_max_bytes`. Low traffic yields single-message batches with no added latency — no time is spent waiting to fill a batch.
- Messages automatically carry metadata columns such as `__meta_source`, `__meta_partition`, `__meta_offset`, `__meta_key`, `__meta_timestamp`, and `__meta_ingest_time`, plus the extended `__meta_ext.topic`. In a multi-message batch every row carries **its own** message's `__meta_partition`/`__meta_offset`/`__meta_key`/`__meta_timestamp`/`__meta_ext` values; `__meta_ingest_time` is one timestamp for the whole batch. `__meta_key` and `__meta_timestamp` appear as nullable columns only when at least one row in the batch has a value (rows without one carry NULL) — an all-absent batch keeps them out of the schema, matching the per-message shape.
- Each Kafka record header becomes a `header_<key>` entry inside the `__meta_ext` map. Duplicate header keys keep every value: the first occurrence uses the plain `header_<key>` name and later occurrences get positional suffixes (`header_<key>_2`, `header_<key>_3`, …). Values are decoded as UTF-8 with invalid bytes replaced by U+FFFD; headers without a value map to an empty string.
- Acknowledgements are per contiguous `(topic, partition)` offset segment: a batch's ack advances the committed position to the last offset of the segment, and its compensation (`undo`) rewinds to the segment's first offset — the whole batch replays as one unit under at-least-once. Tombstones (null payloads, e.g. compacted topics) settle out-of-band without entering the data batch.
- Offsets are advanced via `store_offset` only when `ack()` is called (after a successful downstream write), combined with periodic auto-commit to achieve at-least-once delivery.
- With `transactional_offsets: true`, `ack()` advances the in-memory frontier only and skips `store_offset`: broker group offsets commit inside the paired transactional Kafka output's transactions (`offset_commit_group` naming this input's `consumer_group`), eliminating the commit-then-crash duplicate window. The output clamps its transactional commits to this frontier, so records still settling elsewhere are never skipped. The input's `undo()` compensation also rewinds only the in-memory frontier — the group's broker position has exactly one writer, the paired output's transactions. The pairing is process-internal, expects a single-topic subscription, and is validated at startup: a configuration where no output claims this group fails to start with a configuration error — see [Exactly-once processing](../../build/exactly-once.md).
