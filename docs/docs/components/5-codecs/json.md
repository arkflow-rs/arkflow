---
components: [json]
sidebar_label: JSON
---

# JSON

The JSON codec converts between line-delimited JSON byte payloads and columnar Arrow `RecordBatch`es. Decoding uses Arrow's schema inference to map JSON objects to columns; encoding writes each row as one JSON object separated by newlines. It is the most common codec for attaching to inputs that emit JSON (Kafka, Redis, HTTP, etc.).

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | Fixed value `"json"` |
| on_error | string | no | `fail` | Decode-error policy: `fail` (default) fails the whole batch on the first bad message; `skip` isolates bad messages — each is dropped with a warning and the rest of the batch still decodes |

> The configuration object can be omitted (i.e. `codec: { type: json }`).

## Examples

```yaml validate=fragment wrap=input
input:
  type: kafka
  brokers:
    - localhost:9092
  topics:
    - events
  consumer_group: arkflow
  start_from_latest: false
  codec:
    type: json
```

```yaml validate=fragment wrap=output
output:
  type: stdout
  codec:
    type: json
```

## Notes

- In the default `fail` mode, multiple byte payloads are concatenated with `\n` and handed to the Arrow JSON reader in a single pass for schema inference; one malformed message fails the entire batch.
- In `skip` mode each message decodes individually: bad messages are dropped with a warning (message index and error are logged) and the good messages are merged on the union schema — missing fields become null columns. A batch where every message fails still errors rather than returning an empty batch.
- Encoded output is newline-delimited JSON (one object per line), convenient for downstream line-by-line parsing.
- This codec implements both `Encoder` and `Decoder`, so it can be reused on both the input (decode) and output (encode) sides.
