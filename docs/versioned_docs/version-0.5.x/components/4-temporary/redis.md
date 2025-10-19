# Redis

The Redis temporary component provides temporary storage capabilities using Redis as the backend. It supports both list and string data types and can be used in SQL queries as lookup tables.

## Configuration

#### **mode** (required)

Redis connection configuration. Supports both single instance and cluster modes.

type: `object`

##### Single Mode

```yaml
mode:
  type: "single"
  url: "redis://127.0.0.1:6379"
```

###### **type** (required)

Connection type, must be `"single"` for single instance mode.

type: `string`

###### **url** (required)

Redis server URL in the format `redis://host:port` or `rediss://host:port` for SSL/TLS connections.

type: `string`

##### Cluster Mode

```yaml
mode:
  type: "cluster"
  urls:
    - "redis://127.0.0.1:6379"
    - "redis://127.0.0.1:6380"
    - "redis://127.0.0.1:6381"
```

###### **type** (required)

Connection type, must be `"cluster"` for cluster mode.

type: `string`

###### **urls** (required)

List of Redis cluster node URLs.

type: `array` of `string`

#### **redis_type** (required)

Redis data type configuration. Supports both list and string types.

type: `object`

##### String Type

```yaml
redis_type:
  type: "string"
```

Uses Redis string operations (GET/MGET) to retrieve data.

##### List Type

```yaml
redis_type:
  type: "list"
```

Uses Redis list operations (LRANGE) to retrieve data.

#### **codec** (required)

Codec configuration for data serialization/deserialization.

type: `object`

```yaml
codec:
  type: "json"
```

Currently supports JSON codec for data encoding/decoding.

## Usage in SQL Queries

The Redis temporary component can be used as a lookup table in SQL queries:

```yaml
temporary:
  - name: redis_temporary
    type: "redis"
    codec:
      type: json
    mode:
      type: single
      url: redis://127.0.0.1:6379
    redis_type:
      type: string

pipeline:
  processors:
    - type: "sql"
      query: "SELECT * FROM flow RIGHT JOIN redis_table ON (flow.sensor = redis_table.x)"
      temporary_list:
        - name: redis_temporary
          table_name: redis_table
          key:
            type: value
            value: 'test'
```

## Complete Example

```yaml
logging:
  level: info

streams:
  - input:
      type: "generate"
      context: '{ "timestamp": 1625000000000, "value": 10, "sensor": "temp_1" }'
      interval: 5s
      batch_size: 2

    temporary:
      - name: redis_temporary
        type: "redis"
        codec:
          type: json
        mode:
          type: single
          url: redis://127.0.0.1:6379
        redis_type:
          type: string

    pipeline:
      thread_num: 10
      processors:
        - type: "json_to_arrow"
        - type: "sql"
          query: "SELECT * FROM flow RIGHT JOIN redis_table ON (flow.sensor = redis_table.x)"
          temporary_list:
            - name: redis_temporary
              table_name: redis_table
              key:
                type: value
                value: 'test'

    output:
      type: "stdout"
```

## Features

- **Multiple Redis Types**: Supports both string and list data types
- **Connection Modes**: Works with both single Redis instances and Redis clusters
- **SQL Integration**: Can be used as lookup tables in SQL queries
- **Flexible Codec**: Supports JSON codec for data serialization
- **Async Operations**: Fully asynchronous Redis operations for better performance

## Notes

- The component automatically handles connection management and reconnection
- When used in SQL queries, the key parameter determines which Redis key(s) to query
- List type uses LRANGE to get all elements from Redis lists
- String type uses MGET for efficient batch retrieval of multiple keys
- All Redis operations are performed asynchronously for optimal performance