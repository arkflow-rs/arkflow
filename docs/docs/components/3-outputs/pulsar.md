# Pulsar

The Pulsar output component writes messages to an Apache Pulsar topic. It provides reliable message production with configurable batching and compression.

## Configuration

### **service_url**

Pulsar broker service URL.

- Format: `pulsar://host:port` or `pulsar+ssl://host:port`
- For clusters: `pulsar://broker1:6650,broker2:6650`

type: `string`

optional: `false`

### **topic**

Topic to produce messages to.

- Format: `persistent://tenant/namespace/topic` or `topic-name`
- Supports non-persistent and persistent topics

type: `string`

optional: `false`

### **producer_name** (optional)

Unique name for the producer.

type: `string`

optional: `true`

### **auth** (optional)

Authentication configuration.

type: `object`

optional: `true`

#### **auth.type**

Authentication type.

Supported values: `basic`, `token`, `oauth2`

type: `string`

optional: `false` (when auth is specified)

#### **auth.username** (basic auth)

Username for basic authentication.

type: `string`

optional: `true`

#### **auth.password** (basic auth)

Password for basic authentication.

type: `string`

optional: `true`

#### **auth.token** (token auth)

Authentication token.

type: `string`

optional: `true`

## Examples

### Basic Pulsar Producer

```yaml
- output:
    type: "pulsar"
    service_url: "pulsar://localhost:6650"
    topic: "my-topic"
```

### With Producer Name

```yaml
- output:
    type: "pulsar"
    service_url: "pulsar://pulsar-cluster:6650"
    topic: "persistent://my-tenant/my-ns/events"
    producer_name: "arkflow-producer-1"
```

### With Token Authentication

```yaml
- output:
    type: "pulsar"
    service_url: "pulsar+ssl://secure-pulsar:6651"
    topic: "secure-topic"
    auth:
      type: "token"
      token: "${PULSAR_TOKEN}"
```

### Multi-broker Cluster

```yaml
- output:
    type: "pulsar"
    service_url: "pulsar://broker1.example.com:6650,broker2.example.com:6650"
    topic: "persistent://production/streams/output"
    producer_name: "data-producer"
```

## Features

- **Automatic Batching**: Messages are automatically batched for improved throughput
- **Compression**: Optional compression for reduced network usage
- **Authentication**: Support for token, basic, and OAuth2 authentication
- **TLS/SSL**: Secure connections with Pulsar+SSL
- **Connection Pooling**: Efficient connection management
- **Message Ordering**: Guaranteed ordering within a producer

## Best Practices

1. **Use Persistent Topics**: For production, use persistent topics for durability
2. **Enable Compression**: Use compression for better network utilization
3. **Batch Messages**: Send messages in batches for higher throughput
4. **Monitor Backpressure**: Watch for slow consumers and adjust accordingly
5. **Handle Errors**: Implement proper error handling and retry logic
