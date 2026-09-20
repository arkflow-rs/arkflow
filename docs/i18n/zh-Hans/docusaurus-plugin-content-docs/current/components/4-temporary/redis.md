---
components: [temporary/redis]
sidebar_label: Redis
---

# Redis

Redis 临时存储(Redis temporary)为 SQL 处理器(Processor)提供由 Redis 支撑的查找存储。它暴露一个 `Temporary` 资源,SQL 处理器通过 `temporary_list` 与之进行连接(Join)。支持两种 Redis 数据形态:`string`(MGET)与 `list`(LRANGE)。结果经由编解码器(Codec,通常是 JSON)转换为 Arrow 批次(Batch),并注册为查询侧的表。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | 固定值 `"redis"` |
| mode | object | yes | — | Redis 连接配置(单机或集群) |
| mode.type | string | yes | — | 连接类型:`single` 或 `cluster` |
| mode.url | string | yes (single) | — | `single` 模式下的 Redis URL,例如 `redis://host:port` 或 `rediss://...`(TLS) |
| mode.urls | array&lt;string&gt; | yes (cluster) | — | `cluster` 模式下的节点 URL 列表 |
| redis_type | object | yes | — | Redis 数据结构选择 |
| redis_type.type | string | yes | — | 数据类型:`string`(MGET)或 `list`(LRANGE) |
| codec | object | yes | — | 用于反序列化数据的编解码器配置(结构与各编解码器一致;通常为 `{ type: json }`) |
| codec.type | string | yes | — | 编解码器类型,例如 `json` |

## 在 SQL 查询中使用

Redis 临时存储通过 SQL 处理器的 `temporary_list` 充当查询侧的查找表。`temporary_list[].key` 的实际 schema 是 `Expr<String>`(`#[serde(tag = "type")]`),取值为以下之一:

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| key.type | string | yes | — | `value`(静态字符串字面量)或 `expr`(DataFusion 表达式) |
| key.value | string | yes (value) | — | `key.type = value` 时的静态键值 |
| key.expr | string | yes (expr) | — | `key.type = expr` 时针对当前批次求值的 DataFusion 表达式(返回一个字符串) |

## 示例

声明一个临时存储资源,并在 SQL 处理器中以静态键引用它:

```yaml validate=fragment wrap=stream
temporary:
  - name: redis_temporary
    type: redis
    mode:
      type: single
      url: redis://127.0.0.1:6379
    redis_type:
      type: string
    codec:
      type: json

pipeline:
  processors:
    - type: sql
      query: "SELECT * FROM flow RIGHT JOIN redis_table ON (flow.sensor = redis_table.x)"
      temporary_list:
        - name: redis_temporary
          table_name: redis_table
          key:
            type: value
            value: 'test'
```

用表达式动态计算键(以批次的 `device_id` 列作为 Redis 键):

```yaml validate=foreign reason="SQL processor temporary_list option fragment"
temporary_list:
  - name: redis_temporary
    table_name: redis_table
    key:
      type: expr
      expr: device_id
```

完整示例(generate → SQL 连接 Redis → stdout):

```yaml validate=fragment wrap=engine
logging:
  level: info

streams:
  - input:
      type: generate
      context: '{ "timestamp": 1625000000000, "value": 10, "sensor": "temp_1" }'
      interval: 5s
      batch_size: 2

    temporary:
      - name: redis_temporary
        type: redis
        mode:
          type: single
          url: redis://127.0.0.1:6379
        redis_type:
          type: string
        codec:
          type: json

    pipeline:
      thread_num: 10
      processors:
        - type: json_to_arrow
        - type: sql
          query: "SELECT * FROM flow RIGHT JOIN redis_table ON (flow.sensor = redis_table.x)"
          temporary_list:
            - name: redis_temporary
              table_name: redis_table
              key:
                type: value
                value: 'test'

    output:
      type: stdout
```

## 说明

- `list` 类型使用 `LRANGE key 0 -1` 获取全部元素;`string` 类型对去重后的键集合使用 `MGET`。
- 单个查询仅支持单个键列(`keys.len() == 1`);当 `expr` 求值返回数组时,每个元素都会作为独立的键进行查询。
- 编解码器必须能把从 Redis 取回的字节/字符串反序列化为 Arrow 批次;通常配置为 `json`。
- 连接管理由内部的 `ConnectionManager` 处理,支持自动重连。
