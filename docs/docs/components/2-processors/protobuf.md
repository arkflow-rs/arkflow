---
description: ArkFlow documentation page.
---

# Protobuf

The Protobuf processor converts between Apache Arrow batches and Protocol Buffers messages. It registers two processor types: `arrow_to_protobuf` serializes Arrow columns into Protobuf binary data, and `protobuf_to_arrow` decodes Protobuf binary data into an Arrow batch. Message descriptors are loaded from `.proto` source files (or prebuilt descriptor sets).

## Configuration

Both types share `proto_inputs`, `proto_includes`, and `message_type`. Additional fields apply only to one direction of conversion, as noted below.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `arrow_to_protobuf` \| `protobuf_to_arrow` |
| proto_inputs | array&lt;string&gt; | yes | — | Paths to `.proto` files (or descriptor set binaries) describing the message type. |
| proto_includes | array&lt;string&gt; | no | — | Directories to search when resolving Protobuf imports. |
| message_type | string | yes | — | Fully qualified Protobuf message type name (e.g. `example.MyMessage`). |
| value_field | string | no | — | Name of the binary field holding Protobuf data. Applies to `protobuf_to_arrow` only; defaults to the engine default binary value field. |
| fields_to_include | array&lt;string&gt; | no | — | Restrict the columns serialized to Protobuf. Applies to `arrow_to_protobuf` only; when omitted all fields are included. |

## Examples

### Arrow to Protobuf

```yaml
- processor:
    type: "arrow_to_protobuf"
    proto_inputs: ["./protos/example.proto"]
    message_type: "example.MyMessage"
    fields_to_include:
      - "field1"
      - "field2"
```

### Protobuf to Arrow

```yaml
- processor:
    type: "protobuf_to_arrow"
    proto_inputs: ["./protos/example.proto"]
    proto_includes: ["./protos/"]
    message_type: "example.MyMessage"
    value_field: "data"
```

## Notes

### Data Type Mapping

Protobuf to Arrow type conversions:

| Protobuf Type | Arrow Type | Notes |
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
| enum | Int32 | Stored as enum number |
