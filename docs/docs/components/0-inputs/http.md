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

### **auth**

Authentication configuration.

type: `object`

properties:
- **type**: Authentication type (`basic` or `bearer`)
- **username**: Username for basic authentication
- **password**: Password for basic authentication
- **token**: Token for bearer authentication

## Examples

### Basic HTTP Server

```yaml
- input:
    type: "http"
    address: "0.0.0.0:8080"
    path: "/data"
    cors_enabled: true
```

### With Basic Authentication

```yaml
- input:
    type: "http"
    address: "0.0.0.0:8080"
    path: "/data"
    auth:
      type: "basic"
      username: "user"
      password: "pass"
```

### With Bearer Token Authentication

```yaml
- input:
    type: "http"
    address: "0.0.0.0:8080"
    path: "/data"
    auth:
      type: "bearer"
      token: "your-token"
```