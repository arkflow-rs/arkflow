---
sidebar_position: 3
---

# 消息元数据

输入可以为每个 `MessageBatch` 附加**元数据列**,让下游处理器(尤其是 SQL)能够查看消息来自哪里。所有元数据列都以 `__meta_` 为前缀,并且是普通的 Arrow 列——你可以像操作其他字段一样对它们做选择或过滤。

| 列 | 说明 |
|--------|-------------|
| `__meta_source` | 数据源标识(哪个输入产生了该消息)。 |
| `__meta_partition` | 分区号,适用于分区化的数据源(如 Kafka)。 |
| `__meta_offset` | 分区内的偏移量(offset)/位置。 |
| `__meta_key` | 消息键(数据源携带键时,如 Kafka)。 |
| `__meta_timestamp` | 来自数据源的时间戳。 |
| `__meta_ingest_time` | ArkFlow 摄取该消息的时间。 |
| `__meta_ext` | 以 `Map<String, String>` 列形式存在的扩展键/值元数据。 |

## 在 SQL 中使用元数据

由于元数据列是普通列,你可以直接对它们做投影与过滤:

```yaml validate=fragment wrap=processors
pipeline:
  processors:
    - type: "sql"
      query: |
        SELECT
          *,
          __meta_source    AS source,
          __meta_partition AS partition,
          __meta_offset    AS offset,
          __meta_timestamp AS message_time
        FROM flow
```

按分区/偏移量过滤对重放或审计很有用:

```sql
SELECT id, name
FROM flow
WHERE __meta_partition = 0 AND __meta_offset >= 100
```

## 注意事项

- 并非每个输入都会填充每一列;没有分区概念的数据源(HTTP、file、generate)会留空不相关的列。
- `__meta_ext` 是一个 `MapArray`,因此单个键要通过 SQL 中相应的 map 访问方式读取,而不是当作普通列。
- 元数据列也是至少一次 ack 边界的基础(偏移量只在输出确认写入之后才提交)。
