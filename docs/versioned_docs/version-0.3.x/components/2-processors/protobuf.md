# Protobuf

The Protobuf processor component provides functionality for converting between Protobuf and Arrow formats.

## Configuration

### **type**

The type of Protobuf conversion to perform.

type: `string`

required: `true`

Available options:
- `arrow_to_protobuf`: Convert Arrow format to Protobuf data
- `protobuf_to_arrow`: Convert Protobuf data to Arrow format

### **proto_inputs**

A list of directories containing Protobuf message type descriptor files (*.proto).

type: `array[string]`

required: `true`

### **proto_includes**

A list of directories to search for imported Protobuf files.

type: `array[string]`

optional: `true`

default: Same as proto_inputs

### **message_type**

The Protobuf message type name (e.g. "example.MyMessage").

type: `string`

required: `true`

### **value_field**

Specifies the field name containing the Protobuf binary data when converting from Protobuf to Arrow.

type: `string`

optional: `true`

### **fields_to_include**

Specifies a set of field names to include when converting from Arrow to Protobuf. If not specified, all fields will be included.

type: `array[string]`

optional: `true`

## Data Type Mapping

The processor supports the following Protobuf to Arrow data type conversions:

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

## Examples

```yaml
# Convert Arrow to Protobuf
- processor:
    type: "arrow_to_protobuf"
    proto_inputs: ["./protos/"]
    message_type: "example.MyMessage"
    fields_to_include:
      - "field1"
      - "field2"

# Convert Protobuf to Arrow
- processor:
    type: "protobuf_to_arrow"
    proto_inputs: ["./protos/"]
    proto_includes: ["./includes/"]
    message_type: "example.MyMessage"
    value_field: "data"
```