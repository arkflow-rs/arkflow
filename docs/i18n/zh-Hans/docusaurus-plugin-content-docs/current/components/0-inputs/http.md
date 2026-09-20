---
components: [input/http]
sidebar_label: HTTP
---

# HTTP

HTTP 输入(Input)以 Axum HTTP 服务器的形式运行,接受发送到 `address`+`path` 的 POST 请求。请求体(JSON)会被解码并转发到流处理流水线(Pipeline)。支持可选的 CORS 以及 Basic/Bearer 认证。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | 常量值 `"http"` |
| address | string | yes | — | 监听地址,如 `0.0.0.0:8080` |
| path | string | yes | — | 接收消息(Message)的 URL 路径,如 `/data` |
| cors_enabled | boolean | no | `false` | 是否启用 CORS |
| auth | object | no | — | 认证配置,见下表 |

### auth

`auth` 是一个带标签的枚举(tagged enum,通过 `type` 字段区分),有两种互斥的形式:

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| type | string | yes | `"basic"` 或 `"bearer"` |
| username | string | yes (basic) | Basic 认证用户名 |
| password | string | yes (basic) | Basic 认证密码 |
| token | string | yes (bearer) | Bearer 令牌 |

## 示例

```yaml validate=fragment wrap=input
input:
  type: "http"
  address: "0.0.0.0:8080"
  path: "/data"
  cors_enabled: true
```

```yaml validate=fragment wrap=input
input:
  type: "http"
  address: "0.0.0.0:8080"
  path: "/data"
  auth:
    type: "basic"
    username: "user"
    password: "pass"
```

```yaml validate=fragment wrap=input
input:
  type: "http"
  address: "0.0.0.0:8080"
  path: "/data"
  auth:
    type: "bearer"
    token: "your-token"
```
