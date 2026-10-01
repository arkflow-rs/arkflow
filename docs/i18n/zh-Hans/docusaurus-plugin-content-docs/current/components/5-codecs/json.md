---
components: [json]
sidebar_label: JSON
---

# JSON

JSON 编解码器(Codec)在按行分隔的 JSON 字节负载与列式 Arrow `RecordBatch` 之间相互转换。解码时使用 Arrow 的 schema 推断将 JSON 对象映射为列;编码时把每一行写成一个 JSON 对象并以换行符分隔。对于产生 JSON 的输入(Kafka、Redis、HTTP 等),它是最常用的编解码器。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | 固定值 `"json"` |
| on_error | string | no | `fail` | 解码错误策略:`fail`(缺省)首条坏消息即整批失败;`skip` 逐条隔离——坏消息告警后丢弃,同批其余消息正常解码 |

> 配置对象可以省略(即 `codec: { type: json }`)。

## 示例

```yaml validate=fragment wrap=input
input:
  type: kafka
  brokers:
    - localhost:9092
  topics:
    - events
  consumer_group: arkflow
  start_from_latest: false
  codec:
    type: json
```

```yaml validate=fragment wrap=output
output:
  type: stdout
  codec:
    type: json
```

## 说明

- 解码时,多个字节负载会以 `\n` 拼接后一次性交给 Arrow JSON reader 做 schema 推断;同一批次内字段类型必须一致,否则可能出现推断错误。
- 编码输出为按行分隔的 JSON(每行一个对象),便于下游逐行解析。
- 该编解码器同时实现了 `Encoder` 与 `Decoder`,因此在输入(解码)侧和输出(编码)侧都可以复用。
