# Redis

The Redis input component receives data from a Redis server, supporting both pub/sub and list modes.

## Configuration

#### **url** (required)

Redis server URL in the format `redis://host:port` or `rediss://host:port` for SSL/TLS connections.

type: `string`

#### **redis_type** (required)

Redis operation mode. Must be specified with a `type` of either `"subscribe"` or `"list"`.

type: `object`

##### Subscribe Mode

```yaml
redis_type:
  type: "subscribe"
  subscribe:
    type: "channels"
    channels:
      - "my_channel"
```

###### **subscribe**

Subscription configuration with either channels or patterns.

type: `object`

###### **type** (required)

Subscription type, must be either `"channels"` or `"patterns"`.

type: `string`

###### **channels**

List of channels to subscribe to. Required when type is `"channels"`.

type: `array` of `string`

###### **patterns**

List of patterns to subscribe to. Required when type is `"patterns"`.

type: `array` of `string`

##### List Mode

```yaml
redis_type:
  type: "list"
  list:
    - "my_list"
```

###### **list** (required)

List of Redis lists to consume messages from.

type: `array` of `string`

## Examples

### Subscribe Mode Example (Channels)

```yaml
- input:
    type: "redis"
    url: "redis://localhost:6379"
    redis_type:
      type: "subscribe"
      subscribe:
        type: "channels"
        channels:
          - "news"
          - "events"
```

### Subscribe Mode Example (Patterns)

```yaml
- input:
    type: "redis"
    url: "redis://localhost:6379"
    redis_type:
      type: "subscribe"
      subscribe:
        type: "patterns"
        patterns:
          - "user.*"
          - "notification.*"
```

### List Mode Example

```yaml
- input:
    type: "redis"
    url: "redis://localhost:6379"
    redis_type:
      type: "list"
      list:
        - "tasks"
        - "notifications"
```