---
components: [websocket]
sidebar_label: WebSocket
---

# WebSocket

WebSocket 输入(Input)以客户端身份连接远程 WebSocket 服务器,解码每条入站消息(Message)并转发到流水线(Pipeline)中。当前实现仅支持客户端模式。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | 常量值 `"websocket"` |
| url | string | yes | — | WebSocket 服务器 URL,如 `ws://host:8080/path` 或 `wss://host:8443/path` |
| headers | map&lt;string, string&gt; | no | — | 握手时附加的请求头 |
| timeout | integer | no | — | 连接超时(秒) |

## 示例

```yaml validate=fragment wrap=input
input:
  type: "websocket"
  url: "ws://localhost:8080/ws"
```

```yaml validate=fragment wrap=input
input:
  type: "websocket"
  url: "wss://secure.example.com/ws"
  headers:
    Authorization: "Bearer ${TOKEN}"
  timeout: 10
```

## 说明

- 仅支持客户端模式:该组件会对 `url` 主动调用 `connect_async`,并不监听端口。代码中不存在 `mode`/`host`/`port`/`path` 等服务端字段,旧文档中的相关描述已移除。
- 自动附加 `__meta_source` 与 `__meta_ingest_time` 元数据列。
