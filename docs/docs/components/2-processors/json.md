

# JSON

The JSON processor converts between JSON and Apache Arrow columnar formats. It registers two processor types: `json_to_arrow` decodes JSON bytes into an Arrow `RecordBatch`, and `arrow_to_json` serializes an Arrow batch back into JSON bytes.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: processor-json_to_arrow-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| batch_size | integer | no | — | no | Rows per output batch. |
<!-- END AUTO -->

## Examples

### JSON to Arrow

```yaml
- processor:
    type: "json_to_arrow"
    value_field: "data"
    fields_to_include:
      - "field1"
      - "field2"
```

### Arrow to JSON

```yaml
- processor:
    type: "arrow_to_json"
    fields_to_include:
      - "field1"
      - "field2"
```

## Output schema

The component preserves ArkFlow message metadata and uses the batch schema documented by the surrounding input or output.

## Error handling

Configuration errors are reported during validation. Runtime connection, decoding, or processing errors are logged with the component name; use the Troubleshooting guide to identify the failing boundary.

## Metrics

Monitor throughput, errors, retries, and end-to-end acknowledgement latency for this component. The deployment's metrics endpoint exposes the runtime counters when the control plane is enabled.

## See also

Use the generated reference as the source of truth for configuration. Validate a complete stream configuration before deployment.
