---
sidebar_label: HTTP
---

# HTTP

The HTTP input runs as an Axum HTTP server and accepts POST requests sent to `address`+`path`. The request body (JSON) is decoded and forwarded into the stream processing pipeline. Optional CORS and Basic/Bearer authentication are supported.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: input-http-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| address | string | yes | — | no | Bind address (server mode) or base URL (client mode). |
| auth | object | no | — | no | Authentication configuration. |
| cors_enabled | boolean | no | `false` | no | Enable CORS for the server. |
| interval | string | no | — | yes | Polling interval in humantime format (client mode, e.g. '5s'). |
| method | string | no | — | no | HTTP method (client mode). |
| path | string | yes | — | no | Path to accept messages on (server mode) or full request path (client mode). |
<!-- END AUTO -->

## Examples

```yaml
input:
  type: "http"
  address: "0.0.0.0:8080"
  path: "/data"
  cors_enabled: true
```

```yaml
input:
  type: "http"
  address: "0.0.0.0:8080"
  path: "/data"
  auth:
    type: "basic"
    username: "user"
    password: "pass"
```

```yaml
input:
  type: "http"
  address: "0.0.0.0:8080"
  path: "/data"
  auth:
    type: "bearer"
    token: "your-token"
```

## Output schema

The component preserves ArkFlow message metadata and uses the batch schema documented by the surrounding input or output.

## Error handling

Configuration errors are reported during validation. Runtime connection, decoding, or processing errors are logged with the component name; use the Troubleshooting guide to identify the failing boundary.

## Metrics

Monitor throughput, errors, retries, and end-to-end acknowledgement latency for this component. The deployment's metrics endpoint exposes the runtime counters when the control plane is enabled.

## See also

Use the generated reference as the source of truth for configuration. Validate a complete stream configuration before deployment.
