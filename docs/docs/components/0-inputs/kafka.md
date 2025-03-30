# Kafka

The Kafka input component consumes messages from a Kafka topic.

## Configuration

### **brokers**

List of Kafka server addresses.

- Format: `["host1:port1", "host2:port2"]`
- At least one broker address must be specified

type: `array` of `string`

### **topics**

Subscribed to topics.

- Format: `["topic1", "topic2"]`
- Multiple topics can be subscribed
- Topics must exist in the Kafka cluster

type: `array` of `string`

### **consumer_group**

Consumer group ID.

- Consumers within the same consumer group will share message consumption
- Different consumer groups will independently consume the same messages
- It is recommended to set a unique consumer group ID for each application

type: `string`

### **client_id**

Client ID (optional).

- If not specified, the system will automatically generate a random ID
- It is recommended to set an explicit client ID for monitoring in production environments

type: `string`

### **start_from_latest**

Start with the most recent messages.

- When set to true, the consumer will start consuming from the latest messages
- When set to false, the consumer will start from the earliest available messages

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