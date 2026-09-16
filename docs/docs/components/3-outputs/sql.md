---
components: [output/sql]
description: ArkFlow documentation page.
---

# SQL

The SQL output batch-inserts records into a MySQL or PostgreSQL database. Each row is converted from Arrow to a typed SQL value and inserted in a single parameterized statement. An optional upsert mode turns the insert into an idempotent write (`ON DUPLICATE KEY UPDATE` / `ON CONFLICT DO UPDATE`).

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type  | string | yes | — | Fixed value `"sql"` |
| output_type | object | yes | — | Database driver and connection settings (see below). |
| table_name | string | yes | — | Destination table name. |
| upsert | boolean | no | `false` | Use upsert instead of a plain insert. |
| upsert_keys | string[] | yes (if `upsert`) | — | Columns used as the conflict target for upsert. |

### output_type

`output_type` is a tagged object (selected by its `type` field). Supported drivers: `mysql` and `postgres`.

#### mysql

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `mysql`. |
| uri | string | yes | — | MySQL connection URI (e.g. `mysql://user:pass@host:3306/db`). |
| ssl | object | no | — | Optional SSL configuration (see below). |

#### postgres

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `postgres`. |
| uri | string | yes | — | PostgreSQL connection URI (e.g. `postgres://user:pass@host:5432/db`). |
| ssl | object | no | — | Optional SSL configuration (see below). |

### ssl

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| ssl_mode | string | yes | — | SSL mode (driver-specific, e.g. `preferred`, `require`, `verify_ca`, `verify_full`). |
| root_cert | string | no | — | Path to the root CA certificate. |
| client_cert | string | no | — | Path to the client certificate. |
| client_key | string | no | — | Path to the client key. |

## Examples

### MySQL

```yaml
output:
  type: "sql"
  output_type:
    type: "mysql"
    uri: "mysql://user:password@mysql-server:3306/analytics"
  table_name: "events"
```

### PostgreSQL

```yaml
output:
  type: "sql"
  output_type:
    type: "postgres"
    uri: "postgres://user:pass@localhost:5432/production"
  table_name: "metrics"
```

### PostgreSQL with SSL

```yaml
output:
  type: "sql"
  output_type:
    type: "postgres"
    uri: "postgres://user:pass@postgres:5432/app"
    ssl:
      ssl_mode: "verify_full"
      root_cert: "/etc/ssl/certs/pg-root.crt"
      client_cert: "/etc/ssl/client.crt"
      client_key: "/etc/ssl/client.key"
  table_name: "daily_stats"
```

### PostgreSQL upsert

```yaml
output:
  type: "sql"
  output_type:
    type: "postgres"
    uri: "postgres://user:pass@localhost:5432/production"
  table_name: "events"
  upsert: true
  upsert_keys: ["id"]
```

## Upsert semantics

With `upsert: true` the insert becomes an idempotent write: a row whose `upsert_keys` collide with an existing row updates the non-key columns instead of appending a duplicate.

- PostgreSQL: `INSERT ... ON CONFLICT ("id") DO UPDATE SET "col" = EXCLUDED."col", ...`
- MySQL: `INSERT ... ON DUPLICATE KEY UPDATE \`col\` = VALUES(\`col\`), ...`

Conflict detection relies on the primary key / unique index of the target table matching `upsert_keys`. If every column is an upsert key, the keys themselves are assigned (a no-op update) so the statement stays valid. The duplicate-absorbing behavior is what makes the SQL output suitable for exactly-once-style pipelines that replay after recovery.

## Notes

- Supported column types: Utf8, Int64, UInt64, Float64, Boolean. Other Arrow types are rejected with a process error.
- Identifier quoting follows each dialect: backticks for MySQL, double quotes for PostgreSQL.
- `upsert_keys` columns must exist in the incoming batch schema; otherwise the write fails with an error.
