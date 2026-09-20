---
components: [output/pulsar]
description: ArkFlow 文档页面。
---

# Pulsar

Pulsar 输出(Output)将消息(Message)发布到 Apache Pulsar 主题(Topic)。它支持令牌(Token)与 OAuth2 认证,并且每个输出使用单一共享的生产者(Producer)。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type  | string | yes | — | 固定值 `"pulsar"` |
| service_url | string | yes | — | Pulsar 服务 URL(例如 `pulsar://localhost:6650`)。 |
| topic | object | yes | — | 目标主题(表达式;见下文)。 |
| auth | object | no | — | 认证配置(见下文)。 |
| value_field | string | no | — | 用作消息载荷的记录字段。 |

### topic

`topic` 是一个 `Expr<String>` 对象,具有以下形态之一:

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| type | string | yes | `value`(静态)或 `expr`(SQL 表达式)。 |
| value | string | yes (`value`) | 静态主题名称(例如 `persistent://tenant/namespace/topic`)。 |
| expr | string | yes (`expr`) | 对每条消息求值的 SQL 表达式。 |

### auth

`auth` 是一个带标签对象(由其 `type` 字段选择)。支持的变体:`token` 与 `o_auth2`。

#### token

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| type | string | yes | `token`。 |
| token | string | yes | 认证令牌。 |

#### o_auth2

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| type | string | yes | `o_auth2`。 |
| issuer_url | string | yes | OAuth2 签发方(issuer)URL。 |
| credentials_url | string | yes | 客户端凭证文件的 URL。 |
| audience | string | yes | OAuth2 受众(audience)。 |

## 示例

### 基础 Pulsar 生产者

```yaml validate=fragment wrap=output
output:
  type: "pulsar"
  service_url: "pulsar://localhost:6650"
  topic:
    type: "value"
    value: "persistent://public/default/my-topic"
```

### 使用令牌认证

```yaml validate=fragment wrap=output
output:
  type: "pulsar"
  service_url: "pulsar+ssl://secure-pulsar:6651"
  topic:
    type: "value"
    value: "persistent://public/default/secure-topic"
  auth:
    type: "token"
    token: "${PULSAR_TOKEN}"
```

### 使用 OAuth2 认证

```yaml validate=fragment wrap=output
output:
  type: "pulsar"
  service_url: "pulsar+ssl://secure-pulsar:6651"
  topic:
    type: "value"
    value: "persistent://public/default/events"
  auth:
    type: "o_auth2"
    issuer_url: "https://auth.example.com/oauth2"
    credentials_url: "https://auth.example.com/credentials.json"
    audience: "urn:pulsar:cluster"
```

## 注意事项

- 该输出在构建与连接阶段校验服务 URL 与认证字段;配置错误会快速失败。
- Pulsar 认证支持 `token` 与 `o_auth2`(客户端凭证)。不支持 Basic 用户名/密码认证。
