---
description: ArkFlow documentation page.
---

# JSON

The JSON processor converts between JSON and Apache Arrow columnar formats. It registers two processor types: `json_to_arrow` decodes JSON bytes into an Arrow `RecordBatch`, and `arrow_to_json` serializes an Arrow batch back into JSON bytes.

## Configuration

The two types share the same configuration fields. `value_field` selects the binary column holding JSON data (defaults to the engine default binary value field); `fields_to_include` restricts which columns appear in the output.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `json_to_arrow` \| `arrow_to_json` |
| value_field | string | no | — | Name of the binary field containing JSON data (used by `json_to_arrow`). |
| fields_to_include | array&lt;string&gt; | no | — | Restrict the output to the listed column names. When omitted, all fields are included. |

## Examples

### JSON to Arrow

```yaml
- processor:
    type: "json_to_arrow"
    value_field: "data"
    fields_to_include:
      - "field1"
      - "field2"
```

### Arrow to JSON

```yaml
- processor:
    type: "arrow_to_json"
    fields_to_include:
      - "field1"
      - "field2"
```

## Notes

### Data Type Mapping

JSON to Arrow type conversions:

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
