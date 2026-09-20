---
sidebar_position: 10
description: 端到端案例——持久化采集第三方 Webhook,并转发到 Kafka。
---

# 案例:持久化的 Webhook 采集

一个 SaaS 平台接收来自第三方服务商的 Webhook(支付、CRM 事件、CI 通知)。服务商遇到 5xx 会重试,但每次投递尝试只发一次,且不保留重放日志——因此**消息一旦丢失就永久丢失**。平台把规范化后的事件转发到一个 Kafka 主题,供众多下游服务消费。

## 需求

- 每个被接受的 Webhook 都必须在进程崩溃后幸存
- 格式错误的 Webhook 不得阻塞流
- 至少一次(at-least-once)投递可以接受;下游按 webhook id 去重

## 架构

```
┌──────────┐  POST   ┌──────────┐  persist  ┌─────┐   process   ┌───────┐
│ Webhooks │────────▶│  HTTP    │──────────▶│ WAL │────────────▶│ Kafka │
│ providers│         │  input   │  first    └─────┘             │ sink  │
└──────────┘         └──────────┘                               └───────┘
                                                  ack only after Kafka confirms
```

最关键的决策:HTTP **不可重放**,因此 WAL(`durability:` 块)位于输入与流水线之间。只有 Kafka 确认写入之后,输入才会被确认。

## 配置

已校验示例:[`examples/case_webhook_durable.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/case_webhook_durable.yaml)

```yaml validate=full
streams:
  - id: webhooks-to-kafka
    input:
      type: http
      address: "0.0.0.0:8080"
      path: "/webhooks"

    durability:
      enabled: true
      path: "./data/wal"
      sync: "group_commit"

    pipeline:
      thread_num: 4
      processors:
        - type: json_to_arrow
        - type: sql
          query: "SELECT * FROM flow"
        - type: arrow_to_json

    output:
      type: kafka
      brokers:
        - localhost:9092
      topic:
        type: value
        value: webhooks.raw
      client_id: arkflow-webhooks

    error_output:
      type: stdout
```

## 运行与预期结果

```bash
./target/release/arkflow --config examples/case_webhook_durable.yaml --validate
./target/release/arkflow --config examples/case_webhook_durable.yaml

# 模拟一个第三方服务商
curl -X POST http://localhost:8080/webhooks \
  -H 'Content-Type: application/json' \
  -d '{"webhook_id": "wh-1", "event": "invoice.paid"}'
```

- 消息出现在 Kafka 主题 `webhooks.raw` 上。
- 用 `kill -9` 杀掉引擎再重启:未被确认的 Webhook 会从 WAL 重放并抵达 Kafka——被接受的没有一条丢失。
- 非 JSON 的 POST 会路由到错误输出;流继续服务。

## 权衡与变体

- 用 `sync: per_entry` 替代 `group_commit`,以吞吐量换取更窄的丢失窗口(参见[WAL 优化](/zh-Hans/docs/build/wal))。
- 把 SQL 处理器换成 [VRL](/zh-Hans/docs/components/processors/vrl),在转发前抹除敏感字段。
- Kafka 侧的精确一次(exactly-once)可通过事务性输出实现——参见[精确一次](/zh-Hans/docs/build/exactly-once)。
