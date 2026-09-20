---
components: [input/redis]
sidebar_label: Redis
---

# Redis

Redis 输入(Input)从 Redis 读取数据,同时支持单机(standalone)与集群(cluster)两种连接模式,并提供 Subscribe(频道/模式)与 List 两种消费模式。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | 常量值 `"redis"` |
| mode | object | yes | — | 连接模式(带标签的枚举),见下表 |
| redis_type | object | yes | — | 消费模式(带标签的枚举),见下表 |

### mode

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| type | string | yes | `"single"` 或 `"cluster"` |
| url | string | yes (single) | 单机 URL,如 `redis://host:6379` |
| urls | array&lt;string&gt; | yes (cluster) | 集群节点 URL 列表 |

### redis_type (subscribe)

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| type | string | yes | `"subscribe"` |
| subscribe | object | yes | 订阅配置,见下文 |

`subscribe`:

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| type | string | yes | `"channels"` 或 `"patterns"` |
| channels | array&lt;string&gt; | yes (channels) | 频道列表 |
| patterns | array&lt;string&gt; | yes (patterns) | 模式列表 |

### redis_type (list)

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| type | string | yes | `"list"` |
| list | array&lt;string&gt; | yes | 要消费的 Redis list 键列表 |

## 示例

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
