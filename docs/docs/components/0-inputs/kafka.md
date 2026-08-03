---
sidebar_label: Kafka
---

# Kafka

The Kafka input consumes messages from one or more Apache Kafka topics using a consumer group. Offsets are only advanced after the downstream output acknowledges the write (`enable.auto.offset.store=false`), giving at-least-once delivery across crashes.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: input-kafka-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| brokers | array | yes | — | yes | List of Kafka broker addresses. |
| client_id | string | no | — | no | Optional client identifier. |
| consumer_group | string | yes | — | yes | Consumer group ID for offset coordination. |
| fetch_max_bytes | integer | no | — | no | Maximum bytes for a fetch request. |
| fetch_max_partition_bytes | integer | no | — | no | Maximum bytes per partition in a fetch request. |
| fetch_min_bytes | integer | no | — | no | Minimum bytes before the broker responds to a fetch request. |
| fetch_wait_max_ms | integer | no | — | no | Maximum time to wait for fetch data in milliseconds. |
| start_from_latest | boolean | no | `false` | no | When true, ignore committed offsets and start from the latest message. |
| topics | array | yes | — | yes | Topics to subscribe to. |
<!-- END AUTO -->

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

## Output schema

The component preserves ArkFlow message metadata and uses the batch schema documented by the surrounding input or output.

## Error handling

Configuration errors are reported during validation. Runtime connection, decoding, or processing errors are logged with the component name; use the Troubleshooting guide to identify the failing boundary.

## Metrics

Monitor throughput, errors, retries, and end-to-end acknowledgement latency for this component. The deployment's metrics endpoint exposes the runtime counters when the control plane is enabled.

## See also

Use the generated reference as the source of truth for configuration. Validate a complete stream configuration before deployment.
