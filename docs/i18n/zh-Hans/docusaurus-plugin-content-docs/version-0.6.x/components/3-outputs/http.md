---
components: [output/http]
description: ArkFlow 文档页面。
---

# HTTP

HTTP 输出(Output)将每条消息(Message)作为 HTTP 请求发送到配置的 URL。它支持自定义请求头、指数退避重试,以及 Basic 或 Bearer 认证。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type  | string | yes | — | 固定值 `"http"` |
| url | string | yes | — | 目标 URL。 |
| method | string | yes | — | HTTP 方法:`GET`、`POST`、`PUT`、`DELETE`、`PATCH`。 |
| timeout_ms | integer | yes | — | 请求超时时间(毫秒)。 |
| retry_count | integer | yes | — | 失败时的重试次数。 |
| headers | `map<string, string>` | no | — | 自定义 HTTP 请求头。 |
| body_field | string | no | — | 其值用作请求体的记录字段。 |
| auth | object | no | — | 认证配置(见下文)。 |

### auth

`auth` 是一个外部标签对象:键选择变体(`Basic` 或 `Bearer`),其值承载相应字段。

| Variant key | Fields | Required | 描述 |
|-------|------|----------|---------|-------------|
| `Basic` | username, password | yes | Basic 认证。 |
| `Bearer` | token | yes | Bearer 令牌认证。 |

## 示例

### 基础 HTTP 请求

```yaml validate=fragment wrap=output
output:
  type: "http"
  url: "http://example.com/post/data"
  method: "POST"
  timeout_ms: 5000
  retry_count: 3
  headers:
    Content-Type: "application/json"
```

### 使用 Basic 认证

```yaml validate=fragment wrap=output
output:
  type: "http"
  url: "http://example.com/data"
  method: "POST"
  timeout_ms: 5000
  retry_count: 1
  auth:
    Basic:
      username: "user"
      password: "pass"
```

### 使用 Bearer 令牌

```yaml validate=fragment wrap=output
output:
  type: "http"
  url: "http://example.com/api/data"
  method: "POST"
  timeout_ms: 5000
  retry_count: 1
  auth:
    Bearer:
      token: "your-token"
```

## 注意事项

- 未在 `headers` 中设置时,会自动添加 `Content-Type: application/json`。
- 重试采用指数退避(`100 * 2^(attempt-1)` 毫秒),并要求请求体可克隆。
