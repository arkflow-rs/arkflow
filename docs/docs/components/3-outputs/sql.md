---
description: ArkFlow documentation page.
---

# SQL

The SQL output batch-inserts records into a MySQL or PostgreSQL database. Each row is converted from Arrow to a typed SQL value and inserted in a single parameterized statement.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type  | string | yes | — | Fixed value `"sql"` |
| output_type | object | yes | — | Database driver and connection settings (see below). |
| table_name | string | yes | — | Destination table name. |

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

## Notes

- Supported column types: Utf8, Int64, UInt64, Float64, Boolean. Other Arrow types are rejected with a process error.
- Identifier quoting follows each dialect: backticks for MySQL, double quotes for PostgreSQL.
