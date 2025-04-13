# MQTT

The MQTT output component publishes messages to an MQTT broker.

## Configuration

### **host**

MQTT broker address.

type: `string`

### **port**

MQTT broker port.

type: `integer`

### **client_id**

The client ID to use when connecting to the broker.

type: `string`

### **username**

Username for authentication (optional).

type: `string`

### **password**

Password for authentication (optional).

type: `string`

### **topic**

The topic to publish messages to. Supports both static values and SQL expressions.

type: `object`

One of:
- `type: "value"` with `value: string` - Static topic name
- `type: "expr"` with `expr: string` - SQL expression to evaluate topic name

### **qos**

The Quality of Service level to use.

type: `integer`

One of:
- `0` - At most once delivery
- `1` - At least once delivery
- `2` - Exactly once delivery

default: `1`

### **clean_session**

Whether to use clean session.

type: `boolean`

default: `true`

### **keep_alive**

Keep alive interval in seconds.

type: `integer`

default: `60`

### **retain**

Whether to set the retain flag on published messages.

type: `boolean`

default: `false`

### **value_field**

The field to use as the message value. If not specified, uses the default binary value field.

type: `string`

## Examples

```yaml
output:
  type: "mqtt"
  host: "localhost"
  port: 1883
  client_id: "my-client"
  username: "user"
  password: "pass"
  topic:
    type: "value"
    value: "my-topic"
  qos: 2
  clean_session: true
  keep_alive: 60
  retain: true
  value_field: "message"
```

```yaml
output:
  type: "mqtt"
  host: "localhost"
  port: 1883
  topic:
    type: "expr"
    expr: "concat('sensor/', id)"
  qos: 1
```