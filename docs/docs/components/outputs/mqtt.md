# MQTT

The MQTT output component publishes messages to an MQTT broker.

## Configuration

### **urls**

A list of URLs to connect to.

type: `array` of `string`

### **topic**

The topic to publish messages to.

type: `string`

### **client_id**

The client ID to use when connecting to the broker.

type: `string`


### **qos**

The Quality of Service level to use.

type: `integer`

default: `1`

### **retain**

Whether to set the retain flag on published messages.

type: `boolean`

default: `false`

## Examples

```yaml
- output:
    type: "mqtt"
    urls:
      - "tcp://localhost:1883"
    topic: "my-topic"
    client_id: "my-client"
    qos: 2
    retain: true
```