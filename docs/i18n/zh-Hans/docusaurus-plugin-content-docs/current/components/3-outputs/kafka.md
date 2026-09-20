---
components: [output/kafka]
description: ArkFlow 文档页面。
---

# Kafka

Kafka 输出(Output)基于 librdkafka 向 Apache Kafka 主题(Topic)生产消息(Message)。它支持按键分区、压缩、可配置的确认机制,以及可选的精确一次(exactly-once)事务性生产。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type  | string | yes | — | 固定值 `"kafka"` |
| brokers | `array<string>` | yes | — | Kafka broker 地址列表。 |
| topic | object | yes | — | 目标主题(表达式;见下文)。 |
| key | object | no | — | 用于分区的消息键(表达式;见下文)。 |
| client_id | string | no | — | 客户端标识符。 |
| compression | string | no | — | `none`、`gzip`、`snappy`、`lz4` 之一。 |
| acks | string | no | — | 确认级别:`0`、`1` 或 `all`。 |
| value_field | string | no | — | 用作消息载荷的记录字段。 |
| exactly_once | boolean | no | `false` | 启用精确一次事务性生产(L2)。 |
| transactional_id | string | no | — | 稳定的事务 ID;当 `exactly_once` 为 `true` 时必填。 |

### 表达式对象

`topic` 与 `key` 是 `Expr<String>` 对象,具有以下形态之一:

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| type | string | yes | `value`(静态)或 `expr`(SQL 表达式)。 |
| value | string | yes (`value`) | 静态字符串值。 |
| expr | string | yes (`expr`) | 对每条消息求值的 SQL 表达式。 |

## 示例

### 静态主题与键

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

### 通过 SQL 表达式动态指定主题

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

### 精确一次生产

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

## 注意事项

- 当 `exactly_once: true` 时,`transactional_id` 必须为非空且跨重启保持稳定的值,以便 broker 能隔离(fence)过期的生产者 epoch(即僵尸隔离,zombie fencing)。否则,构建器将拒绝该配置。
- 启用精确一次后,每个已确认的消息批次(Batch)都在一个 Kafka 事务内生产(开始 → 发送 → 提交)。失败时事务中止,该批次被重放。
- 端到端投递语义契约参见[精确一次处理](/zh-Hans/docs/build/exactly-once)。
