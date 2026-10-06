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

- schema 推断覆盖待解码批次的**全部记录**(而非仅首条):数值列跨记录取并集类型——整型与浮点混见的列解码为 `Float64`,值不会被截断;仅在后续记录中出现的字段同样会成为列(以 null 填充、可空);全为整数的列仍推断为 `Int64`。
- 下游适配说明:整型/浮点混见的列在旧版本中会被静默截断为 `Int64`,现在以 `Float64` 正确解码;依赖旧的(被错误收窄的)类型的下游 SQL 或输出 schema 需要自行适配。
- `fail`(缺省)模式下,多个字节负载以 `\n` 拼接后一次性交给 Arrow JSON reader 解码;任意一条坏消息都会导致整批失败。
- `skip` 模式逐条探测消息:解析级坏消息告警后丢弃(日志包含消息下标与错误),其余好消息合并为一次解码,与 `fail` 模式共享同一套全量 schema 推断;全部消息都失败时仍会报错而不是返回空批次。
- 编码输出为按行分隔的 JSON(每行一个对象),便于下游逐行解析。
- 该编解码器同时实现了 `Encoder` 与 `Decoder`,因此在输入(解码)侧和输出(编码)侧都可以复用。
