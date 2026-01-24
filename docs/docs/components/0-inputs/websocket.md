# WebSocket

The WebSocket input component consumes messages from WebSocket connections. It supports both server and client modes for real-time bidirectional communication.

## Configuration

### **url**

WebSocket server URL (for client mode) or bind address (for server mode).

type: `string`

optional: `false`

### **mode**

Operation mode.

Supported values:
- `server`: Act as WebSocket server, listen for incoming connections
- `client`: Act as WebSocket client, connect to a WebSocket server

type: `string`

default: `"client"`

optional: `true`

### **path** (server mode)

URL path to accept connections on (server mode only).

type: `string`

default: `"/"`

optional: `true`

### **host** (server mode)

Host address to bind to (server mode only).

type: `string`

default: `"0.0.0.0"`

optional: `true`

### **headers** (optional)

Additional headers to include in the WebSocket handshake.

type: `object`

default: `{}`

optional: `true`

## Examples

### WebSocket Server

```yaml
- input:
    type: "websocket"
    mode: "server"
    host: "0.0.0.0"
    port: 8080
    path: "/ws"
```

### WebSocket Client

```yaml
- input:
    type: "websocket"
    mode: "client"
    url: "ws://localhost:8080/ws"
```

### Secure WebSocket (WSS)

```yaml
- input:
    type: "websocket"
    mode: "client"
    url: "wss://secure.example.com/ws"
    headers:
      Authorization: "Bearer ${TOKEN}"
```

### WebSocket Server with Custom Path

```yaml
- input:
    type: "websocket"
    mode: "server"
    host: "127.0.0.1"
    port: 9000
    path: "/stream"
```

## Features

- **Bidirectional Communication**: Support for both server and client modes
- **Real-time Data**: Low-latency message delivery
- **Message Metadata**: Automatic extraction of metadata including:
  - `__meta_source`: WebSocket endpoint identifier
  - `__meta_ingest_time`: Message reception timestamp
- **Multiple Connections**: Server mode can handle multiple concurrent connections

## Use Cases

- **Real-time Analytics**: Stream live analytics data
- **Chat Applications**: Process chat messages in real-time
- **IoT Data**: Collect sensor data from IoT devices
- **Live Monitoring**: Monitor system events and metrics
- **Event Streaming**: Process live event streams

## Message Format

Messages received via WebSocket are expected to be in a format compatible with the configured codec. For example, with JSON codec:

```json
{
  "sensor_id": "temp_001",
  "temperature": 23.5,
  "timestamp": 1625000000000
}
```
