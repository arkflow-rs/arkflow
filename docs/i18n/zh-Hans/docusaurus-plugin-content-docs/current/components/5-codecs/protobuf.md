---
components: [protobuf]
sidebar_label: Protobuf
---

# Protobuf

Protobuf 编解码器(Codec)使用启动时从 `.proto` 文件编译出的描述符,在二进制 Protobuf 消息与列式 Arrow `RecordBatch` 之间相互转换。解码按配置的 `MessageDescriptor` 解析每个字节负载;编码则执行相反的过程。当输入产生原始 Protobuf(不带 Confluent schema-id 前缀)时使用它。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | 固定值 `"protobuf"` |
| message_type | string | yes | — | 完全限定的 Protobuf 消息类型名(包含包名),如 `com.example.User` |
| proto_inputs | array&lt;string&gt; | yes | — | `.proto` 源文件路径列表 |
| proto_includes | array&lt;string&gt; | no | — | 解析 `.proto` 文件时使用的 include 搜索路径 |

## 示例

```yaml validate=fragment wrap=input
input:
  type: kafka
  brokers:
    - localhost:9092
  topics:
    - users
  consumer_group: arkflow
  start_from_latest: false
  codec:
    type: protobuf
    message_type: message.Message
    proto_inputs:
      - ./examples/message.proto
```

```yaml validate=fragment wrap=codec
codec:
  type: protobuf
  message_type: message.Message
  proto_inputs:
    - ./examples/message.proto
```

## 说明

- 仅支持 proto3 标量字段:`bool`、`int32`/`sint32`/`sfixed32`、`int64`/`sint64`/`sfixed64`、`uint32`/`fixed32`、`uint64`/`fixed64`、`float`、`double`、`string`、`bytes` 以及 `enum`(映射为 Arrow `Int32`)。
- 不支持嵌套消息、`repeated`、`map`、`oneof` 以及 proto3 的 `optional` 字段;在编码/解码时遇到它们会返回错误。
- 解码多条消息时,批次会以第一条消息的 schema 进行合并(`concat_batches`);各消息之间的字段必须兼容。
