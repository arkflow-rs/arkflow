# Protobuf

The Protobuf processor component provides functionality for converting between Protobuf and Arrow formats.

## Configuration

### **type**

The type of Protobuf conversion to perform.

type: `string`

Available options:
- `arrow_to_protobuf`: Convert Arrow format to Protobuf data
- `protobuf_to_arrow`: Convert Protobuf data to Arrow format

### **descriptor_file**

Path to the Protobuf message type descriptor file.

type: `string`

### **message_type**

The Protobuf message type name.

type: `string`

## Examples

```yaml
- processor:
    type: "arrow_to_protobuf"
    descriptor_file: "./message.proto"
    message_type: "MyMessage"

- processor:
    type: "protobuf_to_arrow"
    descriptor_file: "./message.proto"
    message_type: "MyMessage"
```