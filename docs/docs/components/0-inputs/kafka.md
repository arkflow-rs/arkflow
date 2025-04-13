# Kafka

The Kafka input component consumes messages from a Kafka topic. It provides reliable message consumption with consumer group support and configurable offset management.

## Configuration

### **brokers**

List of Kafka server addresses.

- Format: `["host1:port1", "host2:port2"]`
- At least one broker address must be specified
- Multiple brokers can be specified for high availability

type: `array` of `string`

optional: `false`

### **topics**

Subscribed to topics.

- Format: `["topic1", "topic2"]`
- Multiple topics can be subscribed
- Topics must exist in the Kafka cluster
- The consumer will receive messages from all specified topics

type: `array` of `string`

optional: `false`

### **consumer_group**

Consumer group ID.

- Consumers within the same consumer group will share message consumption
- Different consumer groups will independently consume the same messages
- It is recommended to set a unique consumer group ID for each application
- Used for distributed message processing and load balancing

type: `string`

optional: `false`

### **client_id**

Client ID (optional).

- If not specified, the system will automatically generate a random ID
- It is recommended to set an explicit client ID for monitoring in production environments
- Used to identify the client in Kafka logs and metrics

type: `string`

optional: `true`

### **start_from_latest**

Start with the most recent messages.

- When set to true, the consumer will start consuming from the latest messages
- When set to false, the consumer will start from the earliest available messages
- Useful for controlling message replay behavior on consumer startup

type: `boolean`

default: `false`

optional: `true`

## Examples

```yaml
- input:
    type: kafka
    brokers:
      - localhost:9092
    topics:
      - my_topic
    consumer_group: my_consumer_group
    client_id: my_client
    start_from_latest: false
```

```yaml
- input:
    type: kafka
    brokers:
      - kafka1:9092
      - kafka2:9092
    topics:
      - topic1
      - topic2
    consumer_group: app1_group
    start_from_latest: true
```