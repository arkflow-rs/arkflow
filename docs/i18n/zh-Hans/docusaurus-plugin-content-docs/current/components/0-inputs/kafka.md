---
components: [input/kafka]
sidebar_label: Kafka
---

# Kafka

Kafka 输入(Input)使用消费者组(consumer group)从一个或多个 Apache Kafka 主题(Topic)消费消息(Message)。只有在下游输出确认写入之后,偏移量(offset)才会推进(`enable.auto.offset.store=false`),从而在崩溃时保证至少一次(at-least-once)投递。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | 常量值 `"kafka"` |
| brokers | array&lt;string&gt; | yes | — | Kafka broker 地址列表,如 `["host1:9092","host2:9092"]` |
| topics | array&lt;string&gt; | yes | — | 要订阅的主题列表 |
| consumer_group | string | yes | — | 消费者组 ID,用于偏移量协调与负载均衡 |
| client_id | string | no | — | 客户端 ID,用于监控和日志 |
| start_from_latest | boolean | no | `false` | 为 `true` 时忽略已提交的偏移量,从最新的消息开始消费 |
| fetch_min_bytes | integer | no | — | broker 响应一次拉取请求所需的最小字节数 |
| fetch_max_bytes | integer | no | — | 单次拉取请求返回的最大字节数 |
| fetch_max_partition_bytes | integer | no | — | 单次拉取中每个分区返回的最大字节数 |
| fetch_wait_max_ms | integer | no | — | broker 在响应前等待足够数据累积的最长时间(毫秒) |

## 示例

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

## 说明

- 消息会自动携带 `__meta_source`、`__meta_partition`、`__meta_offset`、`__meta_key`、`__meta_timestamp`、`__meta_ingest_time` 等元数据列,以及扩展列 `__meta_ext.topic`。
- 只有在调用 `ack()` 时(下游写入成功后)才通过 `store_offset` 推进偏移量,并结合周期性自动提交,实现至少一次投递。
