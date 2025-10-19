# NATS

The NATS output component writes messages to a NATS subject.

## Configuration

### **url**

The NATS server URL to connect to.

type: `string`

### **mode**

The NATS operation mode.

type: `object`

One of:
- `type: "regular"` with:
  - `subject: object` - The subject to publish to, with:
    - `type: "value"` with `value: string` - Static subject name
    - `type: "expr"` with `expr: string` - SQL expression to evaluate subject
- `type: "jetstream"` with:
  - `subject: object` - The subject to publish to, with:
    - `type: "value"` with `value: string` - Static subject name
    - `type: "expr"` with `expr: string` - SQL expression to evaluate subject

### **auth**

Authentication configuration (optional).

type: `object`

Fields:
- `username: string` - Username for authentication (optional)
- `password: string` - Password for authentication (optional)
- `token: string` - Authentication token (optional)

### **value_field**

The field to use as the message value. If not specified, uses the default binary value field.

type: `string`

## Examples

```yaml
output:
  type: "nats"
  url: "nats://localhost:4222"
  mode:
    type: "regular"
    subject:
      type: "expr"
      expr: "concat('orders.', id)"
  auth:
    username: "user"
    password: "pass"
  value_field: "message"
```

```yaml
output:
  type: "nats"
  url: "nats://localhost:4222"
  mode:
    type: "jetstream"
    subject:
      type: "value"
      value: "orders.new"
  auth:
    token: "secret-token"
```