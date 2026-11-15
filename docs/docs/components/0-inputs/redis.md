---
components: [input/redis]
sidebar_label: Redis
---

# Redis

The Redis input reads from Redis with both standalone and cluster connection modes, and supports Subscribe (channels / patterns) and List consumption modes. As a channel-based input it follows the engine's cancellation-safety contract: dropping a pending read never loses an already-decoded message (see [Input Delivery Semantics](../../build/delivery-semantics.md#cancellation-safety)).

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | Constant value `"redis"` |
| mode | object | yes | — | Connection mode (tagged enum), see table below |
| redis_type | object | yes | — | Consumption mode (tagged enum), see table below |

### mode

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | yes | `"single"` or `"cluster"` |
| url | string | yes (single) | Standalone URL, e.g. `redis://host:6379` |
| urls | array&lt;string&gt; | yes (cluster) | List of cluster node URLs |

### redis_type (subscribe)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | yes | `"subscribe"` |
| subscribe | object | yes | Subscription configuration, see below |

`subscribe`:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | yes | `"channels"` or `"patterns"` |
| channels | array&lt;string&gt; | yes (channels) | List of channels |
| patterns | array&lt;string&gt; | yes (patterns) | List of patterns |

### redis_type (list)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | yes | `"list"` |
| list | array&lt;string&gt; | yes | List of Redis list keys to consume |

## Examples

```yaml validate=fragment wrap=input
input:
  type: "redis"
  mode:
    type: "single"
    url: "redis://localhost:6379"
  redis_type:
    type: "subscribe"
    subscribe:
      type: "channels"
      channels:
        - "news"
        - "events"
```

```yaml validate=fragment wrap=input
input:
  type: "redis"
  mode:
    type: "single"
    url: "redis://localhost:6379"
  redis_type:
    type: "list"
    list:
      - "tasks"
      - "notifications"
```

## Durability

Both modes use a no-op acknowledgement, which caps delivery at **at-most-once**:

- **List** mode consumes destructively (`BLPOP`): once an element is popped it is gone from Redis. If the pipeline fails or crashes after delivery, the message is lost — it cannot be re-read.
- **Subscribe** mode is fire-and-forget by Redis semantics: messages published while the consumer is down or reconnecting are never delivered.

The cancellation-safety guarantee above only protects an already-decoded message from being lost by a *drop of the pending read* — it does not make the source replayable. For durable consumption, front Redis with a replayable source (e.g. Kafka), or enable stream-level WAL durability with a source that supports replay.
