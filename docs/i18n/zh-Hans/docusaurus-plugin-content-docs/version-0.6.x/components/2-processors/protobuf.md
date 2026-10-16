---
components: [arrow_to_protobuf, protobuf_to_arrow]
description: ArkFlow 文档页面。
---

# Protobuf

Protobuf 处理器在 Apache Arrow 批次与 Protocol Buffers 消息之间相互转换。它注册了两种处理器类型:`arrow_to_protobuf` 将 Arrow 列序列化为 Protobuf 二进制数据,`protobuf_to_arrow` 则将 Protobuf 二进制数据解码为 Arrow 批次。消息描述符从 `.proto` 源文件(或预构建的描述符集)加载。

## 配置

两种类型共享 `proto_inputs`、`proto_includes` 与 `message_type`。其余字段仅适用于其中一个转换方向,如下所述。

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `arrow_to_protobuf` \| `protobuf_to_arrow` |
| proto_inputs | array&lt;string&gt; | yes | — | 描述消息类型的 `.proto` 文件(或描述符集二进制文件)的路径。 |
| proto_includes | array&lt;string&gt; | no | — | 解析 Protobuf 导入时搜索的目录。 |
| message_type | string | yes | — | 完全限定的 Protobuf 消息类型名(如 `example.MyMessage`)。 |
| value_field | string | no | — | 承载 Protobuf 数据的二进制字段的名称。仅适用于 `protobuf_to_arrow`;默认为引擎默认的二进制值字段。 |
| fields_to_include | array&lt;string&gt; | no | — | 限制序列化到 Protobuf 的列。仅适用于 `arrow_to_protobuf`;省略时包含所有字段。 |

## 示例

### Arrow 转 Protobuf

```yaml validate=fragment wrap=processors
- type: "arrow_to_protobuf"
  proto_inputs: ["./examples/message.proto"]
  message_type: "message.Message"
  fields_to_include:
    - "field1"
    - "field2"
```

### Protobuf 转 Arrow

```yaml validate=fragment wrap=processors
- type: "protobuf_to_arrow"
  proto_inputs: ["./examples/message.proto"]
  message_type: "message.Message"
  value_field: "data"
```

## 说明

### 数据类型映射

Protobuf 到 Arrow 的类型转换:

| Protobuf 类型 | Arrow 类型 | 说明 |
|--------------|------------|--------|
| bool | Boolean | |
| int32, sint32, sfixed32 | Int32 | |
| int64, sint64, sfixed64 | Int64 | |
| uint32, fixed32 | UInt32 | |
| uint64, fixed64 | UInt64 | |
| float | Float32 | |
| double | Float64 | |
| string | Utf8 | |
| bytes | Binary | |
| enum | Int32 | 以枚举编号存储 |
