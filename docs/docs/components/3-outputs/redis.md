# Redis

The Redis output component writes messages to Redis data structures including Streams, Lists, and Pub/Sub channels.

## Configuration

### **type**

Redis data structure type.

Supported values: `stream`, `list`, `pubsub`

type: `string`

optional: `false`

### **address**

Redis server address.

type: `string`

example: `"localhost:6379"`

optional: `false`

### **password** (optional)

Redis password for authentication.

type: `string`

optional: `true`

### **db** (optional)

Redis database number.

type: `integer`

default: `0`

optional: `true`

### **key** (list, pubsub)

Redis key for list or pubsub operations.

type: `string`

optional: `false` (for list and pubsub types)

### **stream** (stream)

Stream key name.

type: `string`

optional: `false` (for stream type)

### **stream_max_len** (stream)

Maximum length of the stream. When exceeded, old entries are evicted.

type: `integer`

optional: `true`

### **channel** (pubsub)

Pub/Sub channel name.

type: `string`

optional: `false` (for pubsub type)

### **list_operation** (list)

List operation type.

Supported values: `lpush`, `rpush`

type: `string`

default: `"lpush"`

optional: `true`

## Examples

### Write to Redis Stream

```yaml
- output:
    type: "redis"
    type: "stream"
    address: "localhost:6379"
    stream: "sensor_data"
    stream_max_len: 10000
```

### Write to Redis List (LPUSH)

```yaml
- output:
    type: "redis"
    type: "list"
    address: "localhost:6379"
    key: "events"
    list_operation: "lpush"
```

### Write to Redis List (RPUSH)

```yaml
- output:
    type: "redis"
    type: "list"
    address: "redis-cluster:6379"
    key: "logs"
    list_operation: "rpush"
```

### Publish to Redis Channel

```yaml
- output:
    type: "redis"
    type: "pubsub"
    address: "localhost:6379"
    channel: "notifications"
```

### With Authentication

```yaml
- output:
    type: "redis"
    type: "stream"
    address: "secure-redis:6379"
    password: "${REDIS_PASSWORD}"
    db: 2
    stream: "secure_data"
```

### Stream with Consumer Group

```yaml
- output:
    type: "redis"
    type: "stream"
    address: "localhost:6379"
    stream: "events_stream"
    stream_max_len: 100000
```

## Features

- **Multiple Data Types**: Support for Streams, Lists, and Pub/Sub
- **Authentication**: Password-based authentication
- **Database Selection**: Support for multiple Redis databases
- **Stream Limits**: Configurable maximum stream length with automatic eviction
- **Connection Pooling**: Efficient connection management

## Data Type Usage

### Stream
Use for time-series data, event logs, and message queues.

```yaml
type: "stream"
stream: "my_stream"
stream_max_len: 10000  # Optional: limit stream size
```

Features:
- Automatic timestamp
- Consumer group support
- Automatic eviction when max_len is reached

### List
Use for queues, stacks, and ordered collections.

```yaml
type: "list"
key: "my_list"
list_operation: "lpush"  # or "rpush"
```

Operations:
- `lpush`: Add to the left (head) of the list
- `rpush`: Add to the right (tail) of the list

### Pub/Sub
Use for real-time messaging and notifications.

```yaml
type: "pubsub"
channel: "my_channel"
```

Use cases:
- Broadcast messages
- Real-time notifications
- Event distribution

## Best Practices

1. **Use Streams for Message Queues**: Streams provide better persistence and consumer group support
2. **Set Max Length on Streams**: Prevent unbounded memory growth with `stream_max_len`
3. **Use Lists for Simple Queues**: Lists are ideal for simple FIFO/LIFO queues
4. **Use Pub/Sub for Broadcasting**: Publish messages to multiple consumers
5. **Monitor Memory**: Watch Redis memory usage, especially for unbounded lists
