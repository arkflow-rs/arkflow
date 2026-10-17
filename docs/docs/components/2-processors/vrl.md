---
components: [vrl]
description: ArkFlow documentation page.
---

# VRL

The VRL processor transforms messages using Vector Remap Language (VRL), a safe expression language designed for observability data pipelines. Each incoming batch is mapped to VRL objects; the result of the configured statement is projected back into the columnar batch. See the VRL syntax reference at https://vector.dev/docs/reference/vrl/.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `vrl` |
| statement | string | yes | — | VRL program used to transform each message. |
| timezone | string | no | — | Time zone used when parsing/formatming time values in the program (e.g. `UTC`, `Asia/Shanghai`). |

Input columns map to VRL values with explicit errors instead of silent loss: a `UInt64` value exceeding `i64::MAX` fails the batch (VRL integers are i64 — no negative wraparound), and column types outside the supported set (booleans, integers, floats, utf8/binary, dates, timestamps, null — e.g. `List`/`Struct`) fail with the column name and type. Remove such columns first, e.g. with `filter_columns` or a SQL processor.

## Examples

```yaml validate=fragment wrap=processors
- type: "vrl"
  statement: ".v2, err = .value * 2; ."
```

### Complete Pipeline Example

```yaml validate=full
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

### Output batch granularity

One input batch produces **at most one output batch**. The program runs once per input row; every result row is collected into a single columnar batch in input order. A result that is an **array of objects** contributes its elements as consecutive rows (fan-out), so a 2-row input whose first row yields two objects produces one 3-row batch. If no row produces any result the processor emits nothing.

Because rows are merged, the output schema is the **union of the fields** emitted across the batch, in first-seen order; a row that lacks a field gets `null` there, and a field's column type comes from its first non-null value. A field whose non-null values have **different types across rows** (e.g. an integer on one row, a string or float on another) fails the batch with an error naming the field and both types — one batch can only have one column type, so make the field consistent (e.g. with `to_float()`/`to_string()`). Every result must be an object (or an array of objects) — a scalar result on any row fails the whole batch with an explicit error rather than silently becoming an all-null row. A program that fans every row out to an empty array emits nothing.

Preserving batch granularity matters for downstream per-batch processors: a SQL aggregation after `vrl` sees the full input batch (one aggregate per batch), not one single-row batch per input row.

### Supported Data Types

VRL values map to and from the following Arrow types:

- **String** (Utf8)
- **Integer**: Int8, Int16, Int32, Int64
- **Float**: Float32, Float64
- **Boolean**
- **Binary**
- **Timestamp**
- **Null**
