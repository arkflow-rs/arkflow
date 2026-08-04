---
description: ArkFlow documentation page.
---

# HTTP

The HTTP output sends each message as an HTTP request to a configured URL. It supports custom headers, retry with exponential backoff, and Basic or Bearer authentication.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type  | string | yes | — | Fixed value `"http"` |
| url | string | yes | — | Destination URL. |
| method | string | yes | — | HTTP method: `GET`, `POST`, `PUT`, `DELETE`, `PATCH`. |
| timeout_ms | integer | yes | — | Request timeout in milliseconds. |
| retry_count | integer | yes | — | Number of retry attempts on failure. |
| headers | `map<string, string>` | no | — | Custom HTTP headers. |
| body_field | string | no | — | Record field whose value is used as the request body. |
| auth | object | no | — | Authentication configuration (see below). |

### auth

`auth` is a tagged object (selected by its `type` field).

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `basic` or `bearer`. |
| username | string | yes (basic) | — | Username (basic auth). |
| password | string | yes (basic) | — | Password (basic auth). |
| token | string | yes (bearer) | — | Token (bearer auth). |

## Examples

### Basic HTTP Request

```yaml
output:
  type: "http"
  url: "http://example.com/post/data"
  method: "POST"
  timeout_ms: 5000
  retry_count: 3
  headers:
    Content-Type: "application/json"
```

### With Basic Authentication

```yaml
output:
  type: "http"
  url: "http://example.com/data"
  method: "POST"
  timeout_ms: 5000
  retry_count: 1
  auth:
    type: "basic"
    username: "user"
    password: "pass"
```

### With Bearer Token

```yaml
output:
  type: "http"
  url: "http://example.com/api/data"
  method: "POST"
  timeout_ms: 5000
  retry_count: 1
  auth:
    type: "bearer"
    token: "your-token"
```

## Notes

- `Content-Type: application/json` is added automatically when not set in `headers`.
- Retries use exponential backoff (`100 * 2^(attempt-1)` ms) and require the request body to be cloneable.
