# Redis

The Redis output writes messages to Redis using one of four data-structure operations: Pub/Sub publish, List push, Hash set, or String set. It supports single-node and cluster connections.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type  | string | yes | — | Fixed value `"redis"` |
| mode | object | yes | — | Connection mode (see below). |
| redis_type | object | yes | — | Redis operation to perform (see below). |
| value_field | string | no | — | Record field used as the message payload. |

### mode

`mode` is a tagged object (selected by its `type` field).

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `single` or `cluster`. |
| url | string | yes (`single`) | — | Redis server URL (e.g. `redis://localhost:6379`). |
| urls | `array<string>` | yes (`cluster`) | — | Redis cluster node URLs. |

### redis_type

`redis_type` is a tagged object (selected by its `type` field). All keys/fields/channels are `Expr<String>` (see [Expression objects](#expression-objects)).

#### publish

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | yes | `publish`. |
| channel | object | yes | Pub/Sub channel to publish to (expression). |

#### list

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | yes | `list`. |
| key | object | yes | List key; values are appended with `RPUSH` (expression). |

#### hashes

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | yes | `hashes`. |
| key | object | yes | Hash key (expression). |
| field | object | yes | Hash field name (expression). |

#### strings

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | yes | `strings`. |
| key | object | yes | String key (expression). |

### Expression objects

`channel`, `key`, and `field` are `Expr<String>` objects:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | yes | `value` (static) or `expr` (SQL expression). |
| value | string | yes (`value`) | Static value. |
| expr | string | yes (`expr`) | SQL expression evaluated per message. |

## Examples

### Publish to a channel

```yaml
output:
  type: "redis"
  mode:
    type: "single"
    url: "redis://localhost:6379"
  redis_type:
    type: "publish"
    channel:
      type: "value"
      value: "notifications"
```

### Push to a List

```yaml
output:
  type: "redis"
  mode:
    type: "single"
    url: "redis://localhost:6379"
  redis_type:
    type: "list"
    key:
      type: "value"
      value: "events"
```

### Set a Hash field

```yaml
output:
  type: "redis"
  mode:
    type: "single"
    url: "redis://localhost:6379"
  redis_type:
    type: "hashes"
    key:
      type: "value"
      value: "user:1"
    field:
      type: "value"
      value: "status"
```

### Set a String

```yaml
output:
  type: "redis"
  mode:
    type: "single"
    url: "redis://localhost:6379"
  redis_type:
    type: "strings"
    key:
      type: "expr"
      expr: "concat('key:', id)"
```

### Cluster connection

```yaml
output:
  type: "redis"
  mode:
    type: "cluster"
    urls:
      - "redis://redis-1:6379"
      - "redis://redis-2:6379"
      - "redis://redis-3:6379"
  redis_type:
    type: "list"
    key:
      type: "value"
      value: "logs"
```
