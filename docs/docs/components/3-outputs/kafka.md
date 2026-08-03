

# Kafka

The Kafka output produces messages to an Apache Kafka topic using librdkafka. It supports key-based partitioning, compression, configurable acknowledgments, and optional exactly-once transactional production.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: output-kafka-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| acks | string | no | — | no | Acknowledgment level. |
| brokers | array | yes | — | yes | List of Kafka broker addresses. |
| client_id | string | no | — | no | Optional client identifier. |
| compression | string | no | — | no | Compression algorithm. |
| exactly_once | boolean | no | `false` | no | Enable exactly-once transactional production (L2). |
| key | string | no | — | no | Field used as the message key for partitioning. |
| topic | string | yes | — | no | Destination topic (supports \{field\} placeholders). |
| transactional_id | string | no | — | no | Transactional id (required when exactly_once is true); must be stable across restarts for zombie fencing. |
| value_field | string | no | — | no | Record field used as the message payload. |
<!-- END AUTO -->

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

## Input schema

The component preserves ArkFlow message metadata and uses the batch schema documented by the surrounding input or output.

## Error handling

Configuration errors are reported during validation. Runtime connection, decoding, or processing errors are logged with the component name; use the Troubleshooting guide to identify the failing boundary.

## Metrics

Monitor throughput, errors, retries, and end-to-end acknowledgement latency for this component. The deployment's metrics endpoint exposes the runtime counters when the control plane is enabled.

## See also

Use the generated reference as the source of truth for configuration. Validate a complete stream configuration before deployment.
