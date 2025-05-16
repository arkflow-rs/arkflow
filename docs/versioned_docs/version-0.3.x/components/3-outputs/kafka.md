# Kafka

The Kafka output component writes messages to a Kafka topic.

## Configuration

### **brokers**

A list of broker addresses to connect to.

type: `array` of `string`

### **topic**

The topic to write messages to. Supports both static values and SQL expressions.

type: `object`

One of:
- `type: "value"` with `value: string` - Static topic name
- `type: "expr"` with `expr: string` - SQL expression to evaluate topic name

### **key**

The key to set for each message (optional). Supports both static values and SQL expressions.

type: `object`

One of:
- `type: "value"` with `value: string` - Static key value
- `type: "expr"` with `expr: string` - SQL expression to evaluate key

### **client_id**

The client ID to use when connecting to Kafka.

type: `string`

### **compression**

The compression type to use for messages.

type: `string`

One of:
- `none` - No compression
- `gzip` - Gzip compression
- `snappy` - Snappy compression
- `lz4` - LZ4 compression

### **acks**

The number of acknowledgments the producer requires the leader to have received before considering a request complete.

type: `string`

One of:
- `0` - No acknowledgment
- `1` - Leader acknowledgment only
- `all` - All replicas acknowledgment

### **value_field**

The field to use as the message value. If not specified, uses the default binary value field.

type: `string`

## Examples

```yaml
output:
  type: "kafka"
  brokers:
    - "localhost:9092"
  topic:
    type: "expr"
    expr: "concat('1','x')"
  key:
    type: "value"
    value: "my-key"
  client_id: "my-client"
  compression: "gzip"
  acks: "all"
  value_field: "message"
```

```yaml
output:
  type: "kafka"
  brokers:
    - "localhost:9092"
  topic:
    type: "value"
    value: "my-topic"
  compression: "snappy"
  acks: "1"
```