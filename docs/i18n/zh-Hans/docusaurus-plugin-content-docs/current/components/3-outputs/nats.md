---
components: [output/nats]
description: ArkFlow 文档页面。
---

# NATS

NATS 输出(Output)将消息(Message)发布到 NATS 服务器,可以是常规主题(subject),也可以是 JetStream 流(Stream)。它支持可选的用户名/密码或令牌(Token)认证。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type  | string | yes | — | 固定值 `"nats"` |
| url | string | yes | — | NATS 服务器 URL(例如 `nats://localhost:4222`)。 |
| mode | object | yes | — | 发布模式(见下文)。 |
| auth | object | no | — | 认证配置(见下文)。 |
| value_field | string | no | — | 用作消息载荷的记录字段。 |

### mode

`mode` 是一个带标签对象(由其 `type` 字段选择)。

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `regular` 或 `jet_stream`。 |
| subject | object | yes | — | 要发布到的 NATS 主题(subject)(表达式;见下文)。 |

### subject (`Expr<String>`)

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| type | string | yes | `value`(静态)或 `expr`(SQL 表达式)。 |
| value | string | yes (`value`) | 静态主题名称。 |
| expr | string | yes (`expr`) | 对每条消息求值的 SQL 表达式。 |

### auth

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| username | string | no | — | 用户名(与 `password` 配合使用)。 |
| password | string | no | — | 密码(与 `username` 配合使用)。 |
| token | string | no | — | 认证令牌。 |

用户名/密码与令牌只能配置其一;若两者同时存在,用户名/密码优先。

## 示例

### 常规主题与用户名/密码认证

```yaml validate=fragment wrap=output
output:
  type: "nats"
  url: "nats://localhost:4222"
  mode:
    type: "regular"
    subject:
      type: "expr"
      expr: "concat('orders.', id)"
  auth:
    username: "user"
    password: "pass"
  value_field: "message"
```

### 使用令牌的 JetStream

```yaml validate=fragment wrap=output
output:
  type: "nats"
  url: "nats://localhost:4222"
  mode:
    type: "jet_stream"
    subject:
      type: "value"
      value: "orders.new"
  auth:
    token: "secret-token"
```

## TLS

使用 `tls://` URL scheme 以 TLS 连接(例如 `tls://nats.example.com:4422`);
async-nats 对 `tls://` URL 原生协商 TLS,并要求服务端支持 TLS。

