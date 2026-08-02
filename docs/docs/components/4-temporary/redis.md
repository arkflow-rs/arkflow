---
sidebar_label: Redis
---

# Redis

The Redis temporary provides lookup storage backed by Redis for SQL processors. It exposes a `Temporary` resource that the SQL processor joins against via `temporary_list`. Two Redis data shapes are supported: `string` (MGET) and `list` (LRANGE). Results are passed through a codec (typically JSON) to produce an Arrow batch registered as a query-side table.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | Fixed value `"redis"` |
| mode | object | yes | — | Redis connection configuration (single or cluster) |
| mode.type | string | yes | — | Connection type: `single` or `cluster` |
| mode.url | string | yes (single) | — | Redis URL in `single` mode, e.g. `redis://host:port` or `rediss://...` (TLS) |
| mode.urls | array&lt;string&gt; | yes (cluster) | — | List of node URLs in `cluster` mode |
| redis_type | object | yes | — | Redis data structure selection |
| redis_type.type | string | yes | — | Data type: `string` (MGET) or `list` (LRANGE) |
| codec | object | yes | — | Codec configuration for deserializing data (structure matches each codec; typically `{ type: json }`) |
| codec.type | string | yes | — | Codec type, e.g. `json` |

## Usage in SQL queries

The Redis temporary serves as a query-side lookup table via the SQL processor's `temporary_list`. The actual schema of `temporary_list[].key` is `Expr<String>` (`#[serde(tag = "type")]`), one of:

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| key.type | string | yes | — | `value` (static string literal) or `expr` (DataFusion expression) |
| key.value | string | yes (value) | — | Static key value when `key.type = value` |
| key.expr | string | yes (expr) | — | DataFusion expression evaluated against the current batch (returning a string) when `key.type = expr` |

## Examples

Declaring a temporary resource and referencing it with a static key in the SQL processor:

```yaml
temporary:
  - name: redis_temporary
    type: redis
    mode:
      type: single
      url: redis://127.0.0.1:6379
    redis_type:
      type: string
    codec:
      type: json

pipeline:
  processors:
    - type: sql
      query: "SELECT * FROM flow RIGHT JOIN redis_table ON (flow.sensor = redis_table.x)"
      temporary_list:
        - name: redis_temporary
          table_name: redis_table
          key:
            type: value
            value: 'test'
```

Computing the key dynamically with an expression (using the `device_id` column of the batch as the Redis key):

```yaml
temporary_list:
  - name: redis_temporary
    table_name: redis_table
    key:
      type: expr
      expr: device_id
```

Full example (generate → SQL join Redis → stdout):

```yaml
logging:
  level: info

streams:
  - input:
      type: generate
      context: '{ "timestamp": 1625000000000, "value": 10, "sensor": "temp_1" }'
      interval: 5s
      batch_size: 2

    temporary:
      - name: redis_temporary
        type: redis
        mode:
          type: single
          url: redis://127.0.0.1:6379
        redis_type:
          type: string
        codec:
          type: json

    pipeline:
      thread_num: 10
      processors:
        - type: json_to_arrow
        - type: sql
          query: "SELECT * FROM flow RIGHT JOIN redis_table ON (flow.sensor = redis_table.x)"
          temporary_list:
            - name: redis_temporary
              table_name: redis_table
              key:
                type: value
                value: 'test'

    output:
      type: stdout
```

## Notes

- The `list` type uses `LRANGE key 0 -1` to fetch all elements; the `string` type uses `MGET` against the deduplicated set of keys.
- A single query supports only a single key column (`keys.len() == 1`); when an `expr` evaluation returns an array, each entry is queried as a separate key.
- The codec must be able to deserialize the bytes/strings fetched from Redis into an Arrow batch; it is typically configured as `json`.
- Connection management is handled by an internal `ConnectionManager` with automatic reconnection.
