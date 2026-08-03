# Session Window

The Session Window buffer groups messages into sessions defined by activity gaps. A new message extends the current session; if no message arrives within the configured `gap` duration the session is closed and all of its accumulated messages are emitted as a single batch. An optional `join` configuration lets you run a SQL join across multiple input sources at emission time.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `session_window` |
| gap | duration | yes | — | Maximum idle time between messages in a session. When the gap elapses with no new messages, the session is flushed. Examples: `1ms`, `1s`, `1m`, `1h`. |
| join | object | no | — | Optional SQL join configuration applied to emitted batches. |

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

```yaml
buffer:
  type: "session_window"
  gap: "5s"
```

### With Join Configuration

```yaml
buffer:
  type: "session_window"
  gap: "10s"
  join:
    query: "SELECT a.user_id, a.event_type, b.metadata FROM events a JOIN metadata b ON a.user_id = b.user_id"
    codec:
      type: "json"
```
