---
sidebar_label: SQL
---

# SQL

The SQL input executes a `select_sql` query through DataFusion to read from a database (MySQL, PostgreSQL, SQLite, DuckDB) or file format. Ballista is optional for distributed queries.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | Constant value `"sql"` |
| select_sql | string | yes | — | SQL query statement |
| input_type | object | yes | — | Data source type and configuration (tagged enum), see table below |
| ballista | object | no | — | Distributed query configuration, see table below |

### ballista

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| remote_url | string | yes | Ballista server URL |

### input_type

`input_type` is a tagged enum (distinguished by the `type` field). The variants are below.

#### mysql / postgres

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | yes | `"mysql"` or `"postgres"` |
| uri | string | yes | Database connection URI |
| name | string | no | Registered table name (used to reference it in queries) |
| ssl | object | yes | SSL configuration, see below |

`ssl`:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| ssl_mode | string | yes | SSL mode |
| root_cert | string | no | Path to the root certificate |

#### duckdb / sqlite

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | yes | `"duckdb"` or `"sqlite"` |
| path | string | yes | Database file path |
| name | string | no | Registered table name |

## Examples

```yaml
input:
  type: "sql"
  select_sql: "SELECT * FROM flow"
  input_type:
    type: "mysql"
    name: "my_mysql"
    uri: "mysql://user:password@localhost:3306/db"
    ssl:
      ssl_mode: "verify_identity"
      root_cert: "/path/to/cert.pem"
```

```yaml
input:
  type: "sql"
  select_sql: "SELECT * FROM flow where id > 1000"
  ballista:
    remote_url: "df://localhost:50050"
  input_type:
    type: "sqlite"
    path: "/path/to/data.db"
```
