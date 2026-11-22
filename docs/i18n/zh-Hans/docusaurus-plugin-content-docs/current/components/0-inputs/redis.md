---
components: [input/redis]
sidebar_label: Redis
---

# Redis

Redis 输入(Input)从 Redis 读取数据,同时支持单机(standalone)与集群(cluster)两种连接模式,并提供 Subscribe(频道/模式)与 List 两种消费模式。作为通道型输入,它遵循引擎的取消安全契约:取消一次未完成的 read 不会丢失已解码的消息(参见[输入投递语义](../../build/delivery-semantics.md))。

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

## 持久性

两种模式都使用空操作(Noop)确认,投递语义至多为一次(**at-most-once**):

- **List** 模式是破坏性消费(`BLPOP`):元素一旦弹出就从 Redis 移除。若流水线在投递后失败或崩溃,该消息即丢失——无法重读。
- **Subscribe** 模式受 Redis 语义约束,即发即忘:消费者宕机或重连期间发布的消息永远不会被投递。

上文的取消安全保证只保护"已解码消息不会因取消未完成的 read 而丢失"——它不使数据源可重放。需要持久消费时,请用可重放的数据源(如 Kafka),或在支持重放的源上开启流级 WAL 持久化。

## 错误行为

暂态失败(连接断开、超时、IO)以 `Disconnection` 上报,引擎带退避重连;确定性拒绝(认证、权限、未知命令)是致命的。暂态/永久判定是结构化的(依据驱动的错误类别与精确协议码,如 `NOAUTH`/`WRONGPASS`/`NOPERM`),从不依据消息措辞:消息里仅含 "invalid" 一词不会把暂态错误翻成致命错误。
