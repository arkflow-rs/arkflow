---
components: [output/redis]
description: ArkFlow 文档页面。
---

# Redis

Redis 输出(Output)使用四种数据结构操作之一将消息(Message)写入 Redis:Pub/Sub 发布、List 推入、Hash 写入或 String 写入。它支持单节点与集群连接。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type  | string | yes | — | 固定值 `"redis"` |
| mode | object | yes | — | 连接模式(见下文)。 |
| redis_type | object | yes | — | 要执行的 Redis 操作(见下文)。 |
| value_field | string | no | — | 用作消息载荷的记录字段。 |

### mode

`mode` 是一个带标签对象(由其 `type` 字段选择)。

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `single` 或 `cluster`。 |
| url | string | yes (`single`) | — | Redis 服务器 URL(例如 `redis://localhost:6379`)。 |
| urls | `array<string>` | yes (`cluster`) | — | Redis 集群节点 URL 列表。 |

### redis_type

`redis_type` 是一个带标签对象(由其 `type` 字段选择)。所有键/字段/频道均为 `Expr<String>`(参见[表达式对象](#表达式对象))。

#### publish

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| type | string | yes | `publish`。 |
| channel | object | yes | 要发布到的 Pub/Sub 频道(表达式)。 |

#### list

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| type | string | yes | `list`。 |
| key | object | yes | List 键;值通过 `RPUSH` 追加(表达式)。 |

#### hashes

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| type | string | yes | `hashes`。 |
| key | object | yes | Hash 键(表达式)。 |
| field | object | yes | Hash 字段名(表达式)。 |

#### strings

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| type | string | yes | `strings`。 |
| key | object | yes | String 键(表达式)。 |

### 表达式对象

`channel`、`key` 与 `field` 是 `Expr<String>` 对象:

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| type | string | yes | `value`(静态)或 `expr`(SQL 表达式)。 |
| value | string | yes (`value`) | 静态值。 |
| expr | string | yes (`expr`) | 对每条消息求值的 SQL 表达式。 |

## 示例

### 发布到频道

```yaml validate=fragment wrap=output
output:
  type: "redis"
  mode:
    type: "single"
    url: "redis://localhost:6379"
  redis_type:
    type: "publish"
    channel:
      type: "value"
      value: "notifications"
```

### 推入 List

```yaml validate=fragment wrap=output
output:
  type: "redis"
  mode:
    type: "single"
    url: "redis://localhost:6379"
  redis_type:
    type: "list"
    key:
      type: "value"
      value: "events"
```

### 设置 Hash 字段

```yaml validate=fragment wrap=output
output:
  type: "redis"
  mode:
    type: "single"
    url: "redis://localhost:6379"
  redis_type:
    type: "hashes"
    key:
      type: "value"
      value: "user:1"
    field:
      type: "value"
      value: "status"
```

### 设置 String

```yaml validate=fragment wrap=output
output:
  type: "redis"
  mode:
    type: "single"
    url: "redis://localhost:6379"
  redis_type:
    type: "strings"
    key:
      type: "expr"
      expr: "concat('key:', id)"
```

### 集群连接

```yaml validate=fragment wrap=output
output:
  type: "redis"
  mode:
    type: "cluster"
    urls:
      - "redis://redis-1:6379"
      - "redis://redis-2:6379"
      - "redis://redis-3:6379"
  redis_type:
    type: "list"
    key:
      type: "value"
      value: "logs"
```
