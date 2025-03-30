# HTTP

The HTTP output component sends messages to an HTTP server.

## Configuration

### **url**

The URL to send requests to.

type: `string`

### **verb**

The HTTP verb to use.

type: `string`

default: `"POST"`

### **headers**

A map of headers to add to the request.

type: `object`

default: `{}`

### **timeout**

The maximum time to wait for a response.

type: `string`

default: `"5s"`

## Examples

```yaml
- output:
    type: "http"
    url: "http://example.com/post/data"
    verb: "POST"
    headers:
      Content-Type: "application/json"
      Authorization: "Bearer ${TOKEN}"
    timeout: "10s"
```