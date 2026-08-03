

# SQL

The SQL processor runs SQL queries against the incoming message batch using DataFusion as the query engine. Each batch is registered as a temporary table (named `flow` by default, or `table_name` when set) so it can be filtered, projected, joined with temporary data sources, or aggregated in SQL.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: processor-sql-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| query | string | yes | — | yes | SQL query to run on every batch. |
| table_name | string | no | — | no | Name used for the batch table in the query (default 'flow'). |
| temporary_list | array | no | — | no | Temporary tables to register before running the query. |
<!-- END AUTO -->

## Examples

### Basic SQL Query

```yaml
- processor:
    type: "sql"
    query: "SELECT id, name, age FROM flow WHERE age > 18"
    table_name: "flow"
```

### SQL Query with Temporary Data Sources

```yaml
- temporary:
    - name: user_profiles
      type: "redis"
      mode:
        type: single
        url: redis://127.0.0.1:6379
      redis_type:
        type: string

  processor:
    type: "sql"
    query: "SELECT u.id, u.name, p.title FROM flow u JOIN profiles p ON u.id = p.user_id"
    table_name: "flow"
    temporary_list:
      - name: "user_profiles"
        table_name: "profiles"
        key:
          type: "expr"
          expr: "user_id"
```

## Output schema

The component preserves ArkFlow message metadata and uses the batch schema documented by the surrounding input or output.

## Error handling

Configuration errors are reported during validation. Runtime connection, decoding, or processing errors are logged with the component name; use the Troubleshooting guide to identify the failing boundary.

## Metrics

Monitor throughput, errors, retries, and end-to-end acknowledgement latency for this component. The deployment's metrics endpoint exposes the runtime counters when the control plane is enabled.

## See also

Use the generated reference as the source of truth for configuration. Validate a complete stream configuration before deployment.
