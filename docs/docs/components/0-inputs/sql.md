---
sidebar_label: SQL
---

# SQL

The SQL input executes a `select_sql` query through DataFusion to read from a database (MySQL, PostgreSQL, SQLite, DuckDB) or file format. Ballista is optional for distributed queries.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: input-sql-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| ballista | object | no | — | no | Optional Ballista distributed compute configuration. |
| input_type | object | yes | — | no | Database connection settings. |
| poll_interval | string | no | — | no | Optional poll interval (humantime). |
| select_sql | string | yes | — | no | SELECT statement to execute on every poll. |
<!-- END AUTO -->

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

## Output schema

The component preserves ArkFlow message metadata and uses the batch schema documented by the surrounding input or output.

## Error handling

Configuration errors are reported during validation. Runtime connection, decoding, or processing errors are logged with the component name; use the Troubleshooting guide to identify the failing boundary.

## Metrics

Monitor throughput, errors, retries, and end-to-end acknowledgement latency for this component. The deployment's metrics endpoint exposes the runtime counters when the control plane is enabled.

## See also

Use the generated reference as the source of truth for configuration. Validate a complete stream configuration before deployment.
