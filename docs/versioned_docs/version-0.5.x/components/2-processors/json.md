# JSON

The JSON processor component provides two processors for converting between JSON and Arrow formats.

## JSON to Arrow

The `json_to_arrow` processor converts JSON objects to Arrow format.

### Configuration

#### **value_field**

Specifies the JSON field name to process.

type: `string`

optional: `true`

#### **fields_to_include**

Specifies a set of field names to include in the output. If not specified, all fields will be included.

type: `array[string]`

optional: `true`

### Example

```yaml
- processor:
    type: "json_to_arrow"
    value_field: "data"
    fields_to_include:
    - "field1"
    - "field2"
```

## Arrow to JSON

The `arrow_to_json` processor converts Arrow format data to JSON format.

### Configuration

#### **fields_to_include**

Specifies a set of field names to include in the output. If not specified, all fields will be included.

type: `array[string]`

optional: `true`

### Example

```yaml
- processor:
    type: "arrow_to_json"
    fields_to_include:
    - "field1"
    - "field2"
```

## Data Type Mapping

The processor supports the following JSON to Arrow data type conversions:

| JSON Type | Arrow Type | Notes |
|-----------|------------|--------|
| null | Null | |
| boolean | Boolean | |
| number (integer) | Int64 | For integer values |
| number (unsigned) | UInt64 | For unsigned integer values |
| number (float) | Float64 | For floating point values |
| string | Utf8 | |
| array | Utf8 | Serialized as JSON string |
| object | Utf8 | Serialized as JSON string |