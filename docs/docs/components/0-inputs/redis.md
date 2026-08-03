---
sidebar_label: Redis
---

# Redis

The Redis input reads from Redis with both standalone and cluster connection modes, and supports Subscribe (channels / patterns) and List consumption modes.

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

```yaml
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

```yaml
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
