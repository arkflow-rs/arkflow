---
sidebar_label: JSON
---

# JSON

The JSON codec converts between line-delimited JSON byte payloads and columnar Arrow `RecordBatch`es. Decoding uses Arrow's schema inference to map JSON objects to columns; encoding writes each row as one JSON object separated by newlines. It is the most common codec for attaching to inputs that emit JSON (Kafka, Redis, HTTP, etc.).

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | Fixed value `"json"` |
| pretty | boolean | no | `false` | Metadata declaration field; the encoder currently always outputs newline-delimited form — whether this takes effect is governed by runtime behavior |

> The `build` implementation of this codec does not parse additional fields, so the configuration object can be omitted (i.e. `codec: { type: json }`). `pretty` is only declared in the component metadata schema.

## Examples

```yaml
input:
  type: kafka
  brokers:
    - localhost:9092
  topics:
    - events
  consumer_group: arkflow
  codec:
    type: json
```

```yaml
output:
  type: stdout
  codec:
    type: json
```

## Notes

- On decode, multiple byte payloads are concatenated with `\n` and handed to the Arrow JSON reader in a single pass for schema inference; field types must be consistent within a batch, otherwise inference errors may occur.
- Encoded output is newline-delimited JSON (one object per line), convenient for downstream line-by-line parsing.
- This codec implements both `Encoder` and `Decoder`, so it can be reused on both the input (decode) and output (encode) sides.
