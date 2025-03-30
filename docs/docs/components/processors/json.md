# JSON

The JSON processor component provides two processors for converting between JSON and Arrow formats.

## JSON to Arrow

The `json_to_arrow` processor converts JSON objects to Arrow format.

### Configuration

#### **value_field**

Specifies the JSON field name to process.

type: `string`

optional: `true`

### Example

```yaml
- processor:
    type: "json_to_arrow"
    value_field: "data"
```

## Arrow to JSON

The `arrow_to_json` processor converts Arrow format data to JSON format.

### Configuration

This processor does not require any configuration parameters.

### Example

```yaml
- processor:
    type: "arrow_to_json"
```

## Data Type Mapping

The processor supports the following JSON to Arrow data type conversions:

- null -> Null
- boolean -> Boolean
- number (integer) -> Int64
- number (unsigned integer) -> UInt64
- number (float) -> Float64
- string -> Utf8
- array -> Utf8 (JSON string)
- object -> Utf8 (JSON string)