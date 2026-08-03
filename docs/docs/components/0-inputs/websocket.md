---
sidebar_label: WebSocket
---

# WebSocket

The WebSocket input connects to a remote WebSocket server as a client, decodes each inbound message, and forwards it into the pipeline. The current implementation supports client mode only.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: input-websocket-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| headers | object | no | — | no | Headers included in the WebSocket handshake. |
| timeout | integer | no | — | no | Connection timeout in seconds. |
| url | string | yes | — | yes | WebSocket server URL (ws:// or wss://). |
<!-- END AUTO -->

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

## Output schema

The component preserves ArkFlow message metadata and uses the batch schema documented by the surrounding input or output.

## Error handling

Configuration errors are reported during validation. Runtime connection, decoding, or processing errors are logged with the component name; use the Troubleshooting guide to identify the failing boundary.

## Metrics

Monitor throughput, errors, retries, and end-to-end acknowledgement latency for this component. The deployment's metrics endpoint exposes the runtime counters when the control plane is enabled.

## See also

Use the generated reference as the source of truth for configuration. Validate a complete stream configuration before deployment.
