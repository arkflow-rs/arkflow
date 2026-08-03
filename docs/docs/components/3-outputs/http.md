

# HTTP

The HTTP output sends each message as an HTTP request to a configured URL. It supports custom headers, retry with exponential backoff, and Basic or Bearer authentication.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: output-http-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| auth | object | no | — | no | Authentication configuration. |
| body_field | string | no | — | no | Record field that holds the request body. |
| headers | object | no | — | no | Custom HTTP headers. |
| method | string | no | `"POST"` | no | HTTP method. |
| retry_count | integer | no | — | no | Number of retry attempts on failure. |
| timeout_ms | integer | no | — | no | Request timeout in milliseconds. |
| url | string | yes | — | yes | Destination URL. |
<!-- END AUTO -->

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

## Input schema

The component preserves ArkFlow message metadata and uses the batch schema documented by the surrounding input or output.

## Error handling

Configuration errors are reported during validation. Runtime connection, decoding, or processing errors are logged with the component name; use the Troubleshooting guide to identify the failing boundary.

## Metrics

Monitor throughput, errors, retries, and end-to-end acknowledgement latency for this component. The deployment's metrics endpoint exposes the runtime counters when the control plane is enabled.

## See also

Use the generated reference as the source of truth for configuration. Validate a complete stream configuration before deployment.
