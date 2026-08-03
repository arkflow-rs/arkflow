

# Memory

The Memory buffer is an in-memory message queue that accumulates incoming message batches and releases them as a single merged batch when either a capacity threshold or a timeout is reached. It smooths out traffic spikes and provides backpressure when downstream processing cannot keep up.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: buffer-memory-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| capacity | integer | yes | — | no | Maximum number of messages to accumulate before releasing. |
| timeout | string | yes | — | no | Maximum time to wait before releasing a partial batch (humantime). |
<!-- END AUTO -->

## Examples

```yaml
buffer:
  type: "memory"
  capacity: 100
  timeout: "1s"
```

```yaml
streams:
  - input:
      type: "generate"
      context: '{ "value": 1 }'
      interval: 100ms
      batch_size: 1
    pipeline:
      thread_num: 4
      processors:
        - type: "json_to_arrow"
    buffer:
      type: "memory"
      capacity: 100
      timeout: "1s"
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
