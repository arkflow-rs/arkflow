---
sidebar_position: 4
description: 用 HTTP 输入服务器接收 HTTP Webhook 与事件。
---

# 实战:通过 HTTP 采集数据

把 ArkFlow 当作一个 HTTP 端点运行,将 POST 请求转化为流水线消息。

**前提条件**

- 已[安装](/zh-Hans/docs/get-started/install) ArkFlow
- 基础用法无需其他服务

## 配置

```yaml validate=fragment wrap=engine
logging:
  level: info

streams:
  - input:
      type: http
      address: "0.0.0.0:8080"
      path: "/events"

    pipeline:
      thread_num: 4
      processors:
        - type: json_to_arrow
        - type: sql
          query: "SELECT * FROM flow"
        - type: arrow_to_json

    output:
      type: stdout

    error_output:
      type: stdout
```

该输入监听 `address`,并在 `path` 上接受 POST 请求;每个请求体(JSON)成为一条消息。支持 Basic 与 Bearer 认证以及 CORS——参见[HTTP 输入参考](/zh-Hans/docs/components/inputs/http)。

## 运行与验证

```bash
./target/release/arkflow --config http.yaml
```

发送一个请求:

```bash
curl -X POST http://localhost:8080/events \
  -H 'Content-Type: application/json' \
  -d '{"user": "u1", "action": "click"}'
```

预期结果:负载经流水线处理后回显到 stdout,端点返回一条接受应答。

## 实现持久化

HTTP 是**不可重放的**:一旦端点应答,崩溃就会丢失未被确认的消息。为流加上 WAL,让每个被接受的请求在处理前先持久化——经过校验的端到端版本见[案例:持久化的 Webhook 采集](./10-case-webhook-durable.md)。

## 故障排查

- **POST 返回 404** —— 路径必须完全匹配(`path: "/events"` ≠ `/`)。
- **重启后消息丢失** —— 这正是未加 `durability` 的不可重放输入;按案例篇的做法加上 WAL 块即可。
