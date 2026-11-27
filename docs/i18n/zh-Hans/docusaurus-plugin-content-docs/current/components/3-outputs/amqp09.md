---
components: [output/amqp09]
sidebar_label: AMQP 0.9 (RabbitMQ)
description: ArkFlow AMQP 0.9 (RabbitMQ) 输出组件文档。
---

# AMQP 0.9 (RabbitMQ)

AMQP 0.9 输出(Output)将消息发布到 RabbitMQ 等 AMQP 0.9/0.9.1 交换机,支持自动声明交换机、按行动态计算路由键、设置 AMQP 消息属性以及开启 Publisher Confirms 发布确认。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | 固定值 `"amqp09"` |
| url | string | yes | — | AMQP 代理 URL(例如 `amqp://guest:guest@localhost:5672/%2f` 或 `amqps://...`) |
| exchange | string | yes | — | 要发布到的交换机名称(空字符串 `""` 表示 AMQP 默认交换机) |
| exchange_type | string | no | `direct` | 声明交换机时的类型(`direct`、`fanout`、`topic`、`headers`) |
| routing_key | object | yes | — | 发布使用的路由键(表达式;见下文) |
| declare_exchange | boolean | no | `false` | 交换机不存在时是否自动声明(当 `exchange` 为 `""` 时不可设为 `true`) |
| durable | boolean | no | `true` | 声明的交换机是否持久化 |
| auto_delete | boolean | no | `false` | 声明的交换机在无绑定队列时是否自动删除 |
| persistent | boolean | no | — | 设置 AMQP `delivery_mode`:`true` 为持久化(`2`),`false` 为非持久化(`1`) |
| confirm | boolean | no | `false` | 是否启用 Publisher Confirms;开启后若代理返回 NACK 则本次写入报错 |
| priority | integer | no | — | AMQP 消息优先级(`0`–`255`) |
| expiration | integer | no | — | 单条消息 TTL(毫秒) |
| content_type | string | no | — | MIME 内容类型(例如 `application/json`) |
| content_encoding | string | no | — | MIME 内容编码(例如 `utf-8`) |
| value_field | string | no | — | 二进制或字符串列,其逐行值作为消息载荷(null 值或缺失列会报错)。未配置时按 codec 编码整批。 |

### routing_key (`Expr<String>`)

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| type | string | yes | `value`(静态)或 `expr`(SQL 表达式) |
| value | string | yes (`value`) | 静态路由键 |
| expr | string | yes (`expr`) | 对每行消息求值的 SQL 表达式 |

表达式必须对每一行求出非 null 字符串:任意行为 NULL 时整批报错,错误信息指名表达式与行号(可用 `COALESCE(expr, 'fallback')` 规避,或在上游过滤这类行)。

按行目的地结果必须每条 payload 对应一个路由键:结果行数不足时整批报错,错误信息同时给出两个数量,而不是静默误路由或丢弃未匹配的行。

## 示例

### Topic 交换机、动态路由键与发布确认

```yaml validate=fragment wrap=output
output:
  type: "amqp09"
  url: "amqp://guest:guest@localhost:5672/%2f"
  exchange: "orders"
  exchange_type: "topic"
  routing_key:
    type: "expr"
    expr: "concat('orders.', region)"
  declare_exchange: true
  durable: true
  persistent: true
  confirm: true
  content_type: "application/json"
  value_field: "payload"
```

### 通过默认交换机直接发送到队列

```yaml validate=fragment wrap=output
output:
  type: "amqp09"
  url: "amqp://guest:guest@localhost:5672/%2f"
  exchange: ""
  routing_key:
    type: "value"
    value: "tasks-queue"
  persistent: true
  confirm: true
```

## TLS

使用 `amqps://` URL scheme 以 TLS 连接(例如 `amqps://user:pass@broker.example.com:5671/%2f`);客户端通过 `rustls` 与系统根证书原生协商 TLS。

## 错误行为

传输、IO、心跳超时以及连接/通道关闭错误会上报为 `Disconnection`;协议拒绝、`confirm: true` 下的代理 NACK、表达式求值失败以及逐行路由键长度不匹配会上报为确定性的 `Process` 错误。
