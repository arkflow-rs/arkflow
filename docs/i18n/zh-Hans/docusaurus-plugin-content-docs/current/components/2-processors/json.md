---
components: [arrow_to_json, json_to_arrow]
description: ArkFlow 文档页面。
---

# JSON

JSON 处理器在 JSON 与 Apache Arrow 列式格式之间相互转换。它注册了两种处理器类型:`json_to_arrow` 将 JSON 字节解码为 Arrow `RecordBatch`,`arrow_to_json` 则将 Arrow 批次序列化回 JSON 字节。

## 配置

两种类型共享相同的配置字段。`value_field` 用于选择承载 JSON 数据的二进制列(默认为引擎默认的二进制值字段);`fields_to_include` 用于限制输出中包含哪些列。

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `json_to_arrow` \| `arrow_to_json` |
| value_field | string | no | — | 包含 JSON 数据的二进制字段的名称(由 `json_to_arrow` 使用)。 |
| fields_to_include | array&lt;string&gt; | no | — | 将输出限制为所列出的列名。省略时包含所有字段。 |

## 示例

### JSON 转 Arrow

```yaml validate=fragment wrap=processors
- type: "json_to_arrow"
  value_field: "data"
  fields_to_include:
    - "field1"
    - "field2"
```

### Arrow 转 JSON

```yaml validate=fragment wrap=processors
- type: "arrow_to_json"
  fields_to_include:
    - "field1"
    - "field2"
```

## 说明

### 数据类型映射

JSON 到 Arrow 的类型转换:

| JSON 类型 | Arrow 类型 | 说明 |
|-----------|------------|--------|
| null | Null | |
| boolean | Boolean | |
| number (integer) | Int64 | 用于整数值 |
| number (unsigned) | UInt64 | 用于无符号整数值 |
| number (float) | Float64 | 用于浮点值 |
| string | Utf8 | |
| array | Utf8 | 序列化为 JSON 字符串 |
| object | Utf8 | 序列化为 JSON 字符串 |
