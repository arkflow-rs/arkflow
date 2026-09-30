---
components: [processor/sql]
description: ArkFlow documentation page.
---

# SQL

The SQL processor runs SQL queries against the incoming message batch using DataFusion as the query engine. Each batch is registered as a temporary table (named `flow` by default, or `table_name` when set) so it can be filtered, projected, joined with temporary data sources, or aggregated in SQL.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `sql` |
| query | string | yes | — | SQL query statement to execute against the incoming batch. |
| table_name | string | no | `flow` | Table name used to reference the incoming batch inside the query. |
| temporary_list | array&lt;object&gt; | no | — | Additional temporary data sources to reference in the query. |

### `temporary_list` item

Each entry registers one external source as a named table.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| name | string | yes | — | Name of the temporary source registered with the engine. |
| table_name | string | yes | — | Table name to use for this source inside the SQL query. |
| key | object | yes | — | Key used to look up data in the temporary source. |

### `key`

Tagged union (`type` field selects the variant, snake_cased):

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `expr` \| `value` |
| expr | string | no | — | Expression to evaluate per row against the batch (used when `type: expr`). |
| value | string | no | — | Static string value used as the key (used when `type: value`). |

## Performance and semantics

- **Plan caching.** The SQL statement is fixed per processor, so its analyzed
  and optimized plans are cached per worker context and reused whenever the
  incoming batch schema is unchanged — per-batch re-analysis and
  re-optimization are skipped, and execution always reads the batch that was
  just registered.
- **Time functions stay fresh.** `now()`, `current_date`, `current_time`, and
  `current_timestamp` would otherwise be folded to a constant at plan
  optimization time. Queries containing them skip optimized-plan caching and
  re-optimize on every batch, so each batch observes the time at which it is
  processed.
- **Context pooling.** Queries run on a small pool of DataFusion
  `SessionContext`s (four contexts), so concurrent pipeline workers execute in
  parallel; a worker waits up to 10 seconds for a free context before
  failing, and a context is always returned to the pool after use — repeated
  failures cannot exhaust it.

## Examples

### Basic SQL Query

```yaml validate=fragment wrap=processors
- type: "sql"
  query: "SELECT id, name, age FROM flow WHERE age > 18"
  table_name: "flow"
```

### SQL Query with Temporary Data Sources

```yaml validate=fragment wrap=stream
temporary:
  - name: user_profiles
    type: "redis"
    mode:
      type: single
      url: redis://127.0.0.1:6379
    redis_type:
      type: "string"
    codec:
      type: "json"

pipeline:
  processors:
    - type: "sql"
      query: "SELECT u.id, u.name, p.title FROM flow u JOIN profiles p ON u.id = p.user_id"
      table_name: "flow"
      temporary_list:
        - name: "user_profiles"
          table_name: "profiles"
          key:
            type: "expr"
            expr: "user_id"
```
