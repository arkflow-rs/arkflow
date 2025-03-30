# Kafka

The Kafka output component writes messages to a Kafka topic.

## Configuration

### **brokers**

A list of broker addresses to connect to.

type: `array` of `string`

### **topic**

The topic to write messages to.

type: `string`

### **client_id**

The client ID to use when connecting to Kafka.

type: `string`

default: `"benthos"`

### **key**

The key to set for each message (optional).

type: `string`

### **partition**

The partition to write messages to (optional).

type: `integer`

## Examples

```yaml
- output:
    type: "kafka"
    brokers:
      - "localhost:9092"
    topic: "my-topic"
    client_id: "my-client"
    key: "${!json("id")}"
```