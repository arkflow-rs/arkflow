---
sidebar_position: 3
---

# Debezium CDC to Kafka

Start with [`examples/cdc_debezium.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/cdc_debezium.yaml).
The Debezium codec decodes `before`, `after`, `op`, and source metadata while
the Kafka input retains ack-gated offsets.

```yaml
input:
  type: kafka
  brokers: ["localhost:9092"]
  topics: [dbserver.inventory.customers]
  consumer_group: arkflow-cdc
  codec: {type: debezium_json}
```
