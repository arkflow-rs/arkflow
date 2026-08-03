

# Batch

The Batch processor accumulates incoming message batches and flushes them as a single merged batch when either a configured message count is reached or a timeout elapses. It is useful for grouping small batches together to improve downstream throughput.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: processor-batch-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| count | integer | no | — | no | Maximum number of messages per batch. |
| interval | string | no | — | yes | Maximum time to wait before flushing a partial batch (humantime). |
| size | integer | no | — | no | Approximate maximum byte size per batch. |
<!-- END AUTO -->

## Examples

```yaml
- processor:
    type: "batch"
    count: 1000
    timeout_ms: 1000
```

```yaml
streams:
  - input:
      type: "memory"
      messages:
        - '{ "value": 1 }'
        - '{ "value": 2 }'
    pipeline:
      thread_num: 4
      processors:
        - type: "batch"
          count: 100
          timeout_ms: 500
    output:
      type: "stdout"
```

## Output schema

The component preserves ArkFlow message metadata and uses the batch schema documented by the surrounding input or output.

## Error handling

Configuration errors are reported during validation. Runtime connection, decoding, or processing errors are logged with the component name; use the Troubleshooting guide to identify the failing boundary.

## Metrics

Monitor throughput, errors, retries, and end-to-end acknowledgement latency for this component. The deployment's metrics endpoint exposes the runtime counters when the control plane is enabled.

## See also

Use the generated reference as the source of truth for configuration. Validate a complete stream configuration before deployment.
