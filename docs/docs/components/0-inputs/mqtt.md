# MQTT

The MQTT input component receives data from an MQTT broker.

## Configuration

### **host**

MQTT broker address.

type: `string`

### **port**

MQTT broker port.

type: `integer`

### **client_id**

Unique identifier for the MQTT client.

type: `string`

### **username**

Username for authentication (optional).

type: `string`

### **password**

Password for authentication (optional).

type: `string`

### **topics**

List of topics to subscribe to.

type: `array` of `string`

### **qos**

Quality of Service level (0, 1, or 2).

type: `integer`

default: `1`

### **clean_session**

Whether to start a clean session.

type: `boolean`

default: `true`

### **keep_alive**

Keep alive interval in seconds.

type: `integer`

default: `60`

## Examples

```yaml
- input:
    type: "mqtt"
    host: "localhost"
    port: 1883
    client_id: "my_client"
    username: "user"
    password: "pass"
    topics:
      - "sensors/temperature"
      - "sensors/humidity"
    qos: 1
    clean_session: true
    keep_alive: 60
```