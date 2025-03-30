# Kafka

The Kafka input component consumes messages from a Kafka topic.

## Configuration

### **brokers**

List of Kafka server addresses.

type: `array` of `string`

### **topics**

Subscribed to topics.

type: `array` of `string`

### **consumer_group**

Consumer group ID.

type: `string`

### **client_id**

Client ID (optional).

type: `string`

### **start_from_latest**

Start with the most recent messages.

type: `boolean`

default: `false`

## Examples
```yaml
- input:
    type: kafka
    brokers:
    - localhost:9092
    topics:
    - my_topic
    consumer_group: my_consumer_group