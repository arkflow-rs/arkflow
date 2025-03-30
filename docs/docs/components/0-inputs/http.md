# HTTP

The HTTP input component receives data from HTTP endpoints.

## Configuration

### **address**

Listening address for the HTTP server.

type: `string`

### **path**

The endpoint path to receive data.

type: `string`

### **cors_enabled**

Whether to enable CORS (Cross-Origin Resource Sharing).

type: `boolean`

default: `false`

## Examples

```yaml
- input:
    type: "http"
    address: "0.0.0.0:8080"
    path: "/data"
    cors_enabled: true
```