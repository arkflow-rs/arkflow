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
| table_name | string | yes | — | Destination table. May be schema-qualified (`analytics.events`); see Identifier quoting. |
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
| ssl_mode | string | yes | — | SSL mode (`disable`, `prefer`, `require`, `verify_ca`, or `verify_full`). |
| root_cert | string | no | — | Path to the root CA certificate. |
| client_cert | string | no | — | Path to the client certificate. |
| client_key | string | no | — | Path to the client key. |

## Identifier quoting

The output writes column names, `upsert_keys`, and the table into the SQL text, so each of them is quoted for the target dialect — backticks for MySQL, double quotes for PostgreSQL — with any embedded quote character doubled. A column name is taken from the batch schema and may be inferred from data: a field called `a"b` or ``a`b`` (or one that happens to contain `); DROP TABLE …; --`) is written as that single, oddly named column and never changes the structure of the statement.

`table_name` may be schema-qualified (`analytics.events`); each dot-separated part is quoted on its own:

- A bare part behaves like the unquoted identifier it replaces: on PostgreSQL it is folded to lower case (`Events` addresses the table `events`), on MySQL it keeps its case.
- A part already written in the dialect's quote character is used verbatim (`analytics."Events"` on PostgreSQL addresses the table named `Events`).

A `table_name` that cannot be read this way — an empty part (`a..b`), a quote that is never closed, text after a closing quote, or a NUL byte — fails when the output is built, with an error naming `table_name` (so `--validate` reports it). A column name containing a NUL byte fails the write with an error naming the column.

## Examples

### MySQL

```yaml validate=fragment wrap=output
output:
  type: "sql"
  output_type:
    type: "mysql"
    uri: "mysql://user:password@mysql-server:3306/analytics"
  table_name: "events"
```

### PostgreSQL

```yaml validate=fragment wrap=output
output:
  type: "sql"
  output_type:
    type: "postgres"
    uri: "postgres://user:pass@localhost:5432/production"
  table_name: "metrics"
```

### PostgreSQL with SSL

```yaml validate=fragment wrap=output
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

```yaml validate=fragment wrap=output
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

- Supported column types: Utf8, Boolean; `Int8`/`Int16`/`Int32`/`Int64` (bound as i64); `UInt8`/`UInt16`/`UInt32`/`UInt64` (bound as u64); `Float32`/`Float64` (bound as f64); `Date32`/`Date64` (ISO date strings) and `Timestamp` of any unit (RFC3339 strings). Other Arrow types are rejected with an error naming the column and its type.
- A `codec` is rejected at build time: SQL writes map typed columns to bound parameters and cannot consume encoded bytes. (PostgreSQL has no unsigned integers; `UInt64` values are passed through the driver's signed path as before.)
- `close()` explicitly closes the database connection instead of leaving it to object teardown.
- Identifier quoting follows each dialect and escapes embedded quotes; see the Identifier quoting section above.
- `upsert: true` requires a non-empty `upsert_keys` list without duplicates; violations are rejected at configuration load, before the stream starts.
- `upsert_keys` columns must exist in the incoming batch schema; otherwise the write fails with an error.
- Error behavior: write failures are classified — transient transport failures (connection lost, SQLSTATE classes `08`/`40`/`57`, too many connections, MySQL error numbers 2006/2013) surface as `Disconnection`; deterministic failures (syntax, constraints, type errors) surface as `Process`. A runtime connection failure reports `Connection`; an invalid connection string still reports `Config`.
