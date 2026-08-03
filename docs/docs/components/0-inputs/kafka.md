---
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

## Examples

```yaml
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

```yaml
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
