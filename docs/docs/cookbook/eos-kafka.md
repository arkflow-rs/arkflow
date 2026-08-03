---
sidebar_position: 2
---

# Exactly-once Kafka pipeline

The repository's [`examples/eos-kafka.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/eos-kafka.yaml)
shows a Kafka input paired with a transactional Kafka output.

```yaml
output:
  type: kafka
  brokers: ["localhost:9092"]
  topic: events
  exactly_once: true
  transactional_id: arkflow-events
```

Use a stable transactional id per logical stream and verify broker
transactions before enabling this path in production.
