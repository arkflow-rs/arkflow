---
sidebar_label: Redis
---

# Redis

The Redis temporary provides lookup storage backed by Redis for SQL processors. It exposes a `Temporary` resource that the SQL processor joins against via `temporary_list`. Two Redis data shapes are supported: `string` (MGET) and `list` (LRANGE). Results are passed through a codec (typically JSON) to produce an Arrow batch registered as a query-side table.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: temporary-component-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| — | object | no | — | no | No additional configuration fields. |
<!-- END AUTO -->

## Examples

Declaring a temporary resource and referencing it with a static key in the SQL processor:

```yaml
temporary:
  - name: redis_temporary
    type: redis
    mode:
      type: single
      url: redis://127.0.0.1:6379
    redis_type:
      type: string
    codec:
      type: json

pipeline:
  processors:
    - type: sql
      query: "SELECT * FROM flow RIGHT JOIN redis_table ON (flow.sensor = redis_table.x)"
      temporary_list:
        - name: redis_temporary
          table_name: redis_table
          key:
            type: value
            value: 'test'
```

Computing the key dynamically with an expression (using the `device_id` column of the batch as the Redis key):

```yaml
temporary_list:
  - name: redis_temporary
    table_name: redis_table
    key:
      type: expr
      expr: device_id
```

Full example (generate → SQL join Redis → stdout):

```yaml
logging:
  level: info

streams:
  - input:
      type: generate
      context: '{ "timestamp": 1625000000000, "value": 10, "sensor": "temp_1" }'
      interval: 5s
      batch_size: 2

    temporary:
      - name: redis_temporary
        type: redis
        mode:
          type: single
          url: redis://127.0.0.1:6379
        redis_type:
          type: string
        codec:
          type: json

    pipeline:
      thread_num: 10
      processors:
        - type: json_to_arrow
        - type: sql
          query: "SELECT * FROM flow RIGHT JOIN redis_table ON (flow.sensor = redis_table.x)"
          temporary_list:
            - name: redis_temporary
              table_name: redis_table
              key:
                type: value
                value: 'test'

    output:
      type: stdout
```

## Output schema

The component preserves ArkFlow message metadata and uses the batch schema documented by the surrounding input or output.

## Error handling

Configuration errors are reported during validation. Runtime connection, decoding, or processing errors are logged with the component name; use the Troubleshooting guide to identify the failing boundary.

## Metrics

Monitor throughput, errors, retries, and end-to-end acknowledgement latency for this component. The deployment's metrics endpoint exposes the runtime counters when the control plane is enabled.

## See also

Use the generated reference as the source of truth for configuration. Validate a complete stream configuration before deployment.
