# Pulsar

The Pulsar input component consumes messages from an Apache Pulsar topic. It provides reliable message consumption with subscription management and configurable consumer settings.

## Configuration

### **service_url**

Pulsar broker service URL.

- Format: `pulsar://host:port` or `pulsar+ssl://host:port`
- For clusters: `pulsar://broker1:6650,broker2:6650`

type: `string`

optional: `false`

### **topic**

Topic to consume messages from.

- Format: `persistent://tenant/namespace/topic` or `topic-name`
- Supports non-persistent and persistent topics

type: `string`

optional: `false`

### **subscription_name**

Subscription name for the consumer.

- Consumers with the same subscription name share messages
- Different subscriptions receive independent copies of messages

type: `string`

optional: `false`

### **subscription_type**

Subscription type.

Supported values:
- `exclusive`: Single consumer (default)
- `shared`: Multiple consumers, round-robin distribution
- `failover`: Active consumer with standby
- `key_shared**: Distribution by key

type: `string`

default: `"exclusive"`

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

Authentication token for token-based authentication.

type: `string`

optional: `true`

#### **auth.issuer_url** (OAuth2)

OAuth2 issuer URL.

type: `string`

optional: `true`

#### **auth.credentials_url** (OAuth2)

OAuth2 credentials URL.

type: `string`

optional: `true`

#### **auth.audience** (OAuth2)

OAuth2 audience.

type: `string`

optional: `true`

#### **auth.scope** (OAuth2)

OAuth2 scope.

type: `string`

optional: `true`

### **retry_config** (optional)

Retry configuration for connection and operation failures.

type: `object`

optional: `true`

#### **retry_config.max_retries**

Maximum number of retry attempts.

type: `integer`

default: `3`

optional: `true`

#### **retry_config.retry_delay**

Initial delay between retries in milliseconds.

type: `integer`

default: `1000`

optional: `true`

#### **retry_config.max_delay**

Maximum delay between retries in milliseconds.

type: `integer`

default: `30000`

optional: `true`

## Examples

### Basic Pulsar Consumer

```yaml
- input:
    type: "pulsar"
    service_url: "pulsar://localhost:6650"
    topic: "my-namespace/my-topic"
    subscription_name: "my-subscription"
```

### Shared Subscription

```yaml
- input:
    type: "pulsar"
    service_url: "pulsar://pulsar-cluster:6650"
    topic: "persistent://my-tenant/my-ns/events"
    subscription_name: "consumer-group-1"
    subscription_type: "shared"
```

### With Token Authentication

```yaml
- input:
    type: "pulsar"
    service_url: "pulsar+ssl://secure-pulsar:6651"
    topic: "secure-topic"
    subscription_name: "secure-subscription"
    auth:
      type: "token"
      token: "${PULSAR_TOKEN}"
```

### With OAuth2 Authentication

```yaml
- input:
    type: "pulsar"
    service_url: "pulsar+ssl://pulsar.cloud:6651"
    topic: "cloud-topic"
    subscription_name: "oauth-subscription"
    auth:
      type: "oauth2"
      issuer_url: "https://auth.example.com"
      credentials_url: "file:///path/to/credentials.json"
      audience: "pulsar-cluster"
      scope: "pulsar"
```

### With Retry Configuration

```yaml
- input:
    type: "pulsar"
    service_url: "pulsar://localhost:6650"
    topic: "events"
    subscription_name: "retry-subscription"
    subscription_type: "failover"
    retry_config:
      max_retries: 5
      retry_delay: 2000
      max_delay: 60000
```

### Multi-broker Cluster

```yaml
- input:
    type: "pulsar"
    service_url: "pulsar://broker1.example.com:6650,broker2.example.com:6650,broker3.example.com:6650"
    topic: "persistent://production/streams/data"
    subscription_name: "data-processor"
    subscription_type: "key_shared"
```

## Features

- **Multiple Subscription Types**: Support for exclusive, shared, failover, and key_shared subscriptions
- **Authentication**: Support for basic, token, and OAuth2 authentication
- **TLS/SSL**: Secure connections with Pulsar+SSL
- **Automatic Reconnection**: Built-in retry mechanism for connection failures
- **Message Metadata**: Automatic extraction of Pulsar message metadata including:
  - `__meta_topic`: Source topic
  - `__meta_message_id`: Message ID
  - `__meta_publish_time`: Message publish timestamp
  - `__meta_ingest_time`: Message ingestion time

## Subscription Types

### Exclusive (Default)
Only one consumer can be attached to the subscription. If more than one consumer attempts to connect, the request fails.

Use case: Single consumer per topic, guaranteed order.

### Shared
Multiple consumers can be attached to the same subscription. Messages are delivered in a round-robin fashion across the consumer pool.

Use case: Scalable consumption, no ordering guarantee.

### Failover
Multiple consumers can attach to the subscription. One active consumer receives messages; others act as standby. If the active consumer fails, standby consumers take over.

Use case: High availability with ordering guarantee.

### Key_Shared
Multiple consumers can be attached to the subscription. Messages with the same key are delivered to the same consumer.

Use case: Scalable consumption with per-key ordering.
