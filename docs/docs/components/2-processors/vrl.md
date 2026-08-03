# VRL

The VRL processor transforms messages using Vector Remap Language (VRL), a safe expression language designed for observability data pipelines. Each incoming batch is mapped to VRL objects; the result of the configured statement is projected back into the columnar batch. See the VRL syntax reference at https://vector.dev/docs/reference/vrl/.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `vrl` |
| statement | string | yes | — | VRL program used to transform each message. |
| timezone | string | no | — | Time zone used when parsing/formatming time values in the program (e.g. `UTC`, `Asia/Shanghai`). |

## Examples

```yaml
- processor:
    type: "vrl"
    statement: ".v2, err = .value * 2; ."
```

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
          timezone: "UTC"
        - type: "arrow_to_json"

    output:
      type: "stdout"
```

## Notes

### Supported Data Types

VRL values map to and from the following Arrow types:

- **String** (Utf8)
- **Integer**: Int8, Int16, Int32, Int64
- **Float**: Float32, Float64
- **Boolean**
- **Binary**
- **Timestamp**
- **Null**
