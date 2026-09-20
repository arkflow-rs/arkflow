---
components: [debezium_json]
sidebar_label: Debezium JSON
---

# Debezium JSON

`debezium_json` 编解码器(Codec)将 Debezium 变更数据捕获(CDC,Change Data Capture)信封(Envelope)JSON 解码为列式 Arrow `MessageBatch`。把它挂接到消费 Debezium 写入主题的 Kafka 输入上,即可将数据库变更事件(`c`/`u`/`d`/`r`)转换为可查询的行。CDC 偏移量**不**在此处管理——它由 Kafka 输入的 ack 门控偏移量负责。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | 固定值 `"debezium_json"` |

> 该编解码器没有任何配置字段;`build` 会忽略 `config`。它只作为 Kafka 输入上的 `codec` 钩子挂接。

## 示例

```yaml validate=fragment wrap=input
input:
  type: kafka
  brokers:
    - localhost:9092
  topics:
    - shop.users
  consumer_group: arkflow-cdc
  start_from_latest: false
  codec:
    type: debezium_json
```

部署拓扑:

```
Database → Debezium (Kafka Connect / Debezium Server) → Kafka topic → ArkFlow (kafka input + debezium_json codec)
```

参见 `examples/cdc_debezium.yaml`。

## 语义

### 输出 schema

每个 Debezium 信封 `{ before, after, op, source, ts_ms }` 会被展平为一行:

| 列 | 来源 | 说明 |
| --- | --- | --- |
| `<business fields>` | `after`(删除时回退到 `before`) | 提升为顶层列 |
| `op` | `op` | `c` / `u` / `d` / `r` |
| `ts_ms` | `ts_ms` | 变更时间戳 |
| `source_db`, `source_table` | `source.db`, `source.table` | 顶层标量列 |
| `before` | 完整的 `before` 对象 | JSON 文本列(可用 SQL JSON 函数解析) |
| `source` | 完整的 `source` 对象 | JSON 文本列 |

`before` / `source` 以 JSON 文本形式保留(而非嵌套结构),因为在同一批次中混用 null 与对象(例如插入操作的 `before` 为 null,而更新操作的 `before` 是对象)会与 Arrow JSON reader 的单趟 schema 推断冲突。

### 投递语义

- CDC 偏移量由 Kafka 输入的 ack 门控偏移量提供(至少一次/at-least-once)。请确保下游输出是幂等的。
- 当 `op="d"` 时,`after` 为 null,业务字段取自 `before`。

## 说明 / 非目标

- 不直接连接 MySQL binlog / PostgreSQL 逻辑复制(计划在未来作为独立的输入实现)。
- 仅支持 Debezium JSON;尚不支持 Avro / Protobuf 格式(Avro 需要 Schema Registry)。
