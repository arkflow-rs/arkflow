---
components: [input/nats]
sidebar_label: NATS
---

# NATS

NATS 输入(Input)连接到 NATS 服务器,支持两种模式:常规订阅(regular)与 JetStream 拉取消费者(jet_stream)。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | 常量值 `"nats"` |
| url | string | yes | — | NATS 服务器 URL,如 `nats://host:4222`;多个服务器以逗号分隔 |
| mode | object | yes | — | 运行模式,见下表(带标签的枚举,通过 `type` 字段区分) |
| auth | object | no | — | 认证配置,见下表 |

### mode (regular)

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| type | string | yes | `"regular"` |
| subject | string | yes | 要订阅的 NATS subject |
| queue_group | string | no | 队列组名称 |

### mode (jet_stream)

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| type | string | yes | `"jet_stream"` |
| stream | string | yes | 流(Stream)名称 |
| consumer_name | string | yes | 消费者名称 |
| durable_name | string | no | 持久消费者(durable consumer)名称 |

### auth

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| username | string | no | 用户名 |
| password | string | no | 密码(与 username 配合使用) |
| token | string | no | 令牌;与用户名/密码同时存在时优先使用 |

## 示例

```yaml validate=fragment wrap=input
input:
  type: "nats"
  url: "nats://localhost:4222"
  mode:
    type: "regular"
    subject: "my.subject"
    queue_group: "my_group"
```

```yaml validate=fragment wrap=input
input:
  type: "nats"
  url: "nats://localhost:4222"
  mode:
    type: "jet_stream"
    stream: "my_stream"
    consumer_name: "my_consumer"
    durable_name: "my_durable"
  auth:
    token: "my_token"
```
