

# SQL

The SQL output batch-inserts records into a MySQL or PostgreSQL database. Each row is converted from Arrow to a typed SQL value and inserted in a single parameterized statement.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: output-sql-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| batch_size | integer | no | — | no | Number of rows per insert batch. |
| connection | object | yes | — | no | Database connection settings (type, uri, etc.). |
| table | string | yes | — | no | Destination table. |
| upsert | boolean | no | `false` | no | Use upsert (ON CONFLICT) instead of plain insert. |
| upsert_keys | array | no | — | no | Columns used to detect conflicts for upsert. |
<!-- END AUTO -->

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

## Input schema

The component preserves ArkFlow message metadata and uses the batch schema documented by the surrounding input or output.

## Error handling

Configuration errors are reported during validation. Runtime connection, decoding, or processing errors are logged with the component name; use the Troubleshooting guide to identify the failing boundary.

## Metrics

Monitor throughput, errors, retries, and end-to-end acknowledgement latency for this component. The deployment's metrics endpoint exposes the runtime counters when the control plane is enabled.

## See also

Use the generated reference as the source of truth for configuration. Validate a complete stream configuration before deployment.
