# VRL

The VRL (Vector Remap Language) processor component allows you to process and transform data using the VRL language. It supports rich data type conversion and processing operations, enabling you to flexibly modify and transform messages in the data stream.

## Configuration

### **statement**

VRL statement used to perform data transformation operations.

type: `string`

## Supported Data Types

The VRL processor supports the conversion of the following data types:

- **String**
- **Integer**: Supports Int8, Int16, Int32, Int64
- **Float**: Supports Float32, Float64
- **Boolean**
- **Binary**
- **Timestamp**
- **Null**

## Examples

```yaml
- processor:
    type: "vrl"
    statement: ".v2, err = .value * 2; ."
```

In this example, the VRL processor multiplies the `value` field in the input message by 2 and stores the result in a new `v2` field.

### Complete Pipeline Example

```yaml
streams:
  - input:
      type: "generate"
      context: '{ "timestamp": 1625000000000, "value": 10, "sensor": "temp_1" }'
      interval: 1s
      batch_size: 1

    pipeline:
      thread_num: 4
      processors:
        - type: "json_to_arrow"
        - type: "vrl"
          statement: ".v2, err = .value * 2; ."
        - type: "arrow_to_json"

    output:
      type: "stdout"
```

This example demonstrates a complete pipeline where:
1. First, it generates a JSON message containing timestamp, value, and sensor information
2. Converts the JSON to Arrow format
3. Uses the VRL processor to transform the data
4. Converts the processed data back to JSON format
5. Finally outputs to standard output