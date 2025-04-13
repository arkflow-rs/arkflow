# HTTP

The HTTP output component sends messages to an HTTP server.

## Configuration

### **url**

The URL to send requests to.

type: `string`

### **method**

The HTTP method to use.

type: `string`

default: `"POST"`

Supported methods: `GET`, `POST`, `PUT`, `DELETE`, `PATCH`

### **timeout_ms**

The maximum time to wait for a response in milliseconds.

type: `integer`

default: `5000`

### **retry_count**

Number of retry attempts for failed requests.

type: `integer`

default: `0`

### **headers**

A map of headers to add to the request.

type: `object`

default: `{}`

### **body_field**

Specifies which field from the message to use as the request body.

type: `string`

default: `"value"`

### **auth**

Authentication configuration.

type: `object`

properties:
- **type**: Authentication type (`basic` or `bearer`)
- **username**: Username for basic authentication
- **password**: Password for basic authentication
- **token**: Token for bearer authentication

## Examples

### Basic HTTP Request

```yaml
- output:
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
- output:
    type: "http"
    url: "http://example.com/data"
    method: "POST"
    auth:
      type: "basic"
      username: "user"
      password: "pass"
```

### With Bearer Token

```yaml
- output:
    type: "http"
    url: "http://example.com/api/data"
    method: "POST"
    auth:
      type: "bearer"
      token: "your-token"
    headers:
      Content-Type: "application/json"
```