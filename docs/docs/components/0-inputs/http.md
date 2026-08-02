---
sidebar_label: HTTP
---

# HTTP

The HTTP input runs as an Axum HTTP server and accepts POST requests sent to `address`+`path`. The request body (JSON) is decoded and forwarded into the stream processing pipeline. Optional CORS and Basic/Bearer authentication are supported.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | Constant value `"http"` |
| address | string | yes | — | Listen address, e.g. `0.0.0.0:8080` |
| path | string | yes | — | URL path that receives messages, e.g. `/data` |
| cors_enabled | boolean | no | `false` | Whether to enable CORS |
| auth | object | no | — | Authentication configuration, see table below |

### auth

`auth` is a tagged enum (distinguished by the `type` field) with two mutually exclusive forms:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | yes | `"basic"` or `"bearer"` |
| username | string | yes (basic) | Basic auth username |
| password | string | yes (basic) | Basic auth password |
| token | string | yes (bearer) | Bearer token |

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
