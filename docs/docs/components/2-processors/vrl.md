

# VRL

The VRL processor transforms messages using Vector Remap Language (VRL), a safe expression language designed for observability data pipelines. Each incoming batch is mapped to VRL objects; the result of the configured statement is projected back into the columnar batch. See the VRL syntax reference at https://vector.dev/docs/reference/vrl/.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: processor-vrl-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| statement | string | yes | — | yes | VRL program source. |
| timezone | string | no | — | no | Optional timezone for VRL timestamp operations (e.g. 'Asia/Shanghai', 'UTC', 'local'). Defaults to the platform local timezone; invalid values fall back to the default with a warning. |
<!-- END AUTO -->

## Examples

```yaml
- processor:
    type: "vrl"
    statement: ".v2, err = .value * 2; ."
```

### Complete Pipeline Example

```yaml
streams:
  - input:
      type: "generate"
      context: '{ "timestamp": 1625000000000, "value": 10, "sensor": "temp_1" }'
      interval: 1s
      batch_size: 1

    pipeline:
      thread_num: 4
      processors:
        - type: "json_to_arrow"
        - type: "vrl"
          statement: ".v2, err = .value * 2; ."
          timezone: "UTC"
        - type: "arrow_to_json"

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
