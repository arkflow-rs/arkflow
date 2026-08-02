---
sidebar_label: Protobuf
---

# Protobuf

The Protobuf codec converts between binary Protobuf messages and columnar Arrow `RecordBatch`es using a descriptor compiled from `.proto` files at startup. Decoding parses each byte payload against the configured `MessageDescriptor`; encoding reverses the process. Use it when an input emits raw Protobuf (no Confluent schema-id prefix).

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | Fixed value `"protobuf"` |
| message_type | string | yes | — | Fully qualified Protobuf message type name (including package), e.g. `com.example.User` |
| proto_inputs | array&lt;string&gt; | yes | — | List of `.proto` source file paths |
| proto_includes | array&lt;string&gt; | no | — | Include search paths used when parsing `.proto` files |

## Examples

```yaml
input:
  type: kafka
  brokers:
    - localhost:9092
  topics:
    - users
  consumer_group: arkflow
  codec:
    type: protobuf
    message_type: com.example.User
    proto_inputs:
      - /etc/arkflow/proto/user.proto
```

```yaml
codec:
  type: protobuf
  message_type: test.TestMessage
  proto_inputs:
    - /etc/arkflow/proto/test_message.proto
  proto_includes:
    - /usr/include/protos
```

## Notes

- Only proto3 scalar fields are supported: `bool`, `int32`/`sint32`/`sfixed32`, `int64`/`sint64`/`sfixed64`, `uint32`/`fixed32`, `uint64`/`fixed64`, `float`, `double`, `string`, `bytes`, and `enum` (mapped to Arrow `Int32`).
- Nested messages, `repeated`, `map`, `oneof`, and proto3 `optional` fields are **not** supported; encountering them returns an error during encoding/decoding.
- When decoding multiple messages, the batch is merged (`concat_batches`) using the schema of the first message; fields across messages must be compatible.
