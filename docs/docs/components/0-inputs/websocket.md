---
sidebar_label: WebSocket
---

# WebSocket

The WebSocket input connects to a remote WebSocket server as a client, decodes each inbound message, and forwards it into the pipeline. The current implementation supports client mode only.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | Constant value `"websocket"` |
| url | string | yes | — | WebSocket server URL, e.g. `ws://host:8080/path` or `wss://host:8443/path` |
| headers | map&lt;string, string&gt; | no | — | Request headers attached during the handshake |
| timeout | integer | no | — | Connection timeout (seconds) |

## Examples

```yaml
input:
  type: "websocket"
  url: "ws://localhost:8080/ws"
```

```yaml
input:
  type: "websocket"
  url: "wss://secure.example.com/ws"
  headers:
    Authorization: "Bearer ${TOKEN}"
  timeout: 10
```

## Notes

- Client mode only: the component actively calls `connect_async` against `url`; it does not listen on a port. Server-side fields such as `mode`/`host`/`port`/`path` do not exist in the code, and the related descriptions from the old docs have been removed.
- Automatically attaches the `__meta_source` and `__meta_ingest_time` metadata columns.
