

# Sliding Window

The Sliding Window buffer groups messages into overlapping windows that advance over time. Up to `window_size` messages are emitted as a single batch on each `interval`; after each emission the window slides forward by `slide_size` messages, so when `slide_size` is smaller than `window_size` the same message can appear in multiple consecutive emitted batches.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: buffer-sliding_window-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| interval | string | yes | — | yes | Time interval between window emissions (humantime). |
| slide_size | integer | yes | — | no | Number of messages to advance the window by on each emission. |
| window_size | integer | yes | — | no | Number of messages included in each window. |
<!-- END AUTO -->

## Examples

```yaml
buffer:
  type: "sliding_window"
  window_size: 100
  interval: "1s"
  slide_size: 10
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
      type: "sliding_window"
      window_size: 100
      interval: "1s"
      slide_size: 10
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
