---
components: [input/amqp09]
sidebar_label: AMQP 0.9 (RabbitMQ)
---

# AMQP 0.9 (RabbitMQ)

AMQP 0.9 输入(Input)从 RabbitMQ 等 AMQP 0.9/0.9.1 消息代理消费消息。作为通道型输入,它遵循引擎的取消安全契约:取消一次未完成的 read 不会丢失已解码的消息(参见[输入投递语义](../../build/delivery-semantics.md))。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | 常量值 `"amqp09"` |
| url | string | yes | — | AMQP 代理 URL,例如 `amqp://guest:guest@localhost:5672/%2f` 或 `amqps://...` |
| queue | string | yes | — | 要消费的队列名称 |
| exchange | string | no | — | 可选的交换机名称;配置后会将 `queue` 绑定到该交换机 |
| exchange_type | string | no | `direct` | 声明交换机时的类型(`direct`、`fanout`、`topic`、`headers`) |
| routing_key | string | no | `""` | 将 `queue` 绑定到 `exchange` 时使用的路由键 |
| declare_queue | boolean | no | `false` | 队列不存在时是否自动声明 |
| declare_exchange | boolean | no | `false` | 交换机不存在时是否自动声明(要求 `exchange` 非空) |
| durable | boolean | no | `true` | 声明的交换机/队列是否持久化 |
| auto_delete | boolean | no | `false` | 声明的交换机/队列在无使用时是否自动删除 |
| exclusive | boolean | no | `false` | 声明的队列是否仅限当前连接独占 |
| consumer_tag | string | no | `""` | 消费者标签(留空由代理自动分配) |
| auto_ack | boolean | no | `false` | 收到消息后是否自动确认;为 `false` 时在下游处理完成后再发送 `basic.ack` |
| prefetch_count | integer | no | `256` | 单通道 QoS 预取数量(`0`–`65535`;默认 `256` 以限制代理积压并维持背压) |

## 示例

### Topic 交换机与手动确认

```yaml validate=fragment wrap=input
input:
  type: "amqp09"
  url: "amqp://guest:guest@localhost:5672/%2f"
  queue: "orders-in"
  exchange: "orders"
  exchange_type: "topic"
  routing_key: "orders.#"
  declare_exchange: true
  declare_queue: true
  durable: true
  auto_ack: false
  prefetch_count: 64
```

### Fanout 交换机

```yaml validate=fragment wrap=input
input:
  type: "amqp09"
  url: "amqp://guest:guest@localhost:5672/%2f"
  queue: "audit-log"
  exchange: "events-fanout"
  exchange_type: "fanout"
  declare_exchange: true
  declare_queue: true
```

## TLS

使用 `amqps://` URL scheme 以 TLS 连接(例如 `amqps://user:pass@broker.example.com:5671/%2f`);客户端通过 `rustls` 与系统根证书原生协商 TLS。

## 读取错误

当 AMQP 消费流异常中断或出现投递错误时,输入组件会上报 `Disconnection` 以触发引擎重连,而不会以 `EOF` 结束整个作业。
