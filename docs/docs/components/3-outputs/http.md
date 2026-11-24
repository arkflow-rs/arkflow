---
components: [output/http]
description: ArkFlow documentation page.
---

# HTTP

The HTTP output sends each message as an HTTP request to a configured URL. It supports custom headers, retry of transient failures (transport errors, `429`, `5xx`) with capped exponential backoff — other `4xx` responses fail on the first attempt — and Basic or Bearer authentication.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type  | string | yes | — | Fixed value `"http"` |
| url | string | yes | — | Destination URL. |
| method | string | no | `POST` | HTTP method: `GET`, `POST`, `PUT`, `DELETE`, `PATCH`. Defaults to `POST`. |
| timeout_ms | integer | yes | — | Request timeout in milliseconds. |
| retry_count | integer | no | `3` | Number of retry attempts on failure. Only transient failures (transport errors, `429`, `5xx`) are retried; other `4xx` responses fail immediately. |
| headers | `map<string, string>` | no | — | Custom HTTP headers. |
| body_field | string | no | — | Record field whose value is used as the request body. |
| auth | object | no | — | Authentication configuration (see below). |

### auth

`auth` is an externally tagged object: the key selects the variant (`Basic`
or `Bearer`) and its value carries the fields.

| Variant key | Fields | Required | Description |
|-------|------|----------|---------|-------------|
| `Basic` | username, password | yes | Basic authentication. |
| `Bearer` | token | yes | Bearer-token authentication. |

## Examples

### Basic HTTP Request

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

### With Basic Authentication

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

### With Bearer Token

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

## Notes

- `Content-Type: application/json` is added automatically when not set in `headers`.
- Retries use exponential backoff (`100 * 2^(attempt-1)` ms) and require the request body to be cloneable.
