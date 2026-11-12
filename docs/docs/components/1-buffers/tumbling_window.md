---
components: [tumbling_window]
description: ArkFlow documentation page.
---

# Tumbling Window

The Tumbling Window buffer groups messages into fixed-size, non-overlapping time windows. Every configured `interval`, all messages accumulated within the current window are emitted together as a single batch; each message belongs to exactly one window. A legacy `join` option exists, but the stream compiler rejects window buffers that declare it (see the warning below).

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `tumbling_window` |
| interval | duration | yes | — | Fixed duration of each window. When it elapses, all accumulated messages are emitted. Examples: `1ms`, `1s`, `1m`, `1h`. |
| join | object | no | — | Legacy SQL join configuration; rejected by the stream compiler (see warning below). |

:::warning Legacy `join` — rejected by the stream compiler

Declaring `join` on a `tumbling_window` buffer fails compilation with
`legacy window buffers with 'join' cannot compile`. Stream-stream join is not
supported in Stream configs; use the SQL processor's per-batch joins against
temporary tables instead.

:::

### `join`

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| query | string | yes | — | SQL query joining batch data from different input sources. |
| value_field | string | no | — | Name of the binary field holding the message payload. Defaults to the engine default binary value field. |
| codec | object | yes | — | Codec used to decode message batches before joining. |
| thread_num | integer | no | — | Number of worker threads used for parallel decoding during the join. |

The `codec` field is a `CodecConfig` object: a `type` string selecting the codec plus any codec-specific fields.

## Examples

### Basic Configuration

```yaml validate=fragment wrap=buffer
buffer:
  type: "tumbling_window"
  interval: "1s"
```

### With Join Configuration

```yaml validate=foreign reason="legacy join configuration — stream-stream join is not yet supported; use the SQL processor's per-batch joins against temporary tables instead"
buffer:
  type: "tumbling_window"
  interval: "5s"
  join:
    query: "SELECT a.id, a.name, b.value FROM input1 a JOIN input2 b ON a.id = b.id"
    codec:
      type: "json"
```
