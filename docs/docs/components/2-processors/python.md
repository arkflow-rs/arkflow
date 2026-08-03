

# Python

The Python processor executes user-supplied Python code on each message batch via PyO3. The incoming `MessageBatch` is exposed to your function as a PyArrow `RecordBatch`, and the function returns one or more PyArrow batches that are converted back into the engine's columnar format. This lets you implement custom transformations using Python's data ecosystem (PyArrow, Pandas, Polars, NumPy, etc.).

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: processor-python-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| extra_packages | array | no | — | no | Optional list of pip packages to install before running. |
| function | string | yes | — | no | Name of the function to invoke for each batch. |
| script | string | yes | — | no | Python source defining the transform function. |
<!-- END AUTO -->

## Examples

### Using a Python Module

```yaml
- processor:
    type: "python"
    function: "process_batch"
    module: "example1"
    python_path: ["./examples/python"]
```

### Using an Inline Python Script

```yaml
- processor:
    type: "python"
    script: |
      def process_batch(batch):
          # Process the batch here
          # For example, you can modify the batch or create a new one
          return [batch]
    function: "process_batch"
```

### Complete Pipeline Example

```yaml
streams:
  - input:
      type: "memory"
      messages:
        - '{ "timestamp": 1625000000000, "value": 10, "sensor": "temp_1" }'
        - '{ "timestamp": 1625000000000, "value": 19, "sensor": "temp_1" }'
        - '{ "timestamp": 1625000000000, "value": 11, "sensor": "temp_2" }'
        - '{ "timestamp": 1625000000000, "value": 11, "sensor": "temp_2" }'

    pipeline:
      thread_num: 4
      processors:
        - type: "json_to_arrow"
        - type: "python"
          script: |
            import pyarrow as pa
            import pyarrow.compute as pc

            def process_batch(batch):
                value_array = batch.column('value')
                doubled_values = pc.multiply(value_array, 2)
                new_fields = [
                    pa.field('timestamp', pa.int64()),
                    pa.field('value', pa.int64()),
                    pa.field('sensor', pa.string()),
                    pa.field('value_doubled', pa.int64())
                ]
                new_schema = pa.schema(new_fields)
                new_batch = pa.RecordBatch.from_arrays(
                    [
                        batch.column('timestamp'),
                        batch.column('value'),
                        batch.column('sensor'),
                        doubled_values
                    ],
                    schema=new_schema
                )
                return [new_batch]
          function: "process_batch"
        - type: "arrow_to_json"

    output:
      type: "stdout"
```

### Example Python Module

```python
def process_batch(batch):
    # The batch parameter is a PyArrow batch
    # You can perform any processing on the batch here
    # For example, you can modify the batch or create a new one
    return [batch]  # Return a list of PyArrow batches
```

## Output schema

The component preserves ArkFlow message metadata and uses the batch schema documented by the surrounding input or output.

## Error handling

Configuration errors are reported during validation. Runtime connection, decoding, or processing errors are logged with the component name; use the Troubleshooting guide to identify the failing boundary.

## Metrics

Monitor throughput, errors, retries, and end-to-end acknowledgement latency for this component. The deployment's metrics endpoint exposes the runtime counters when the control plane is enabled.

## See also

Use the generated reference as the source of truth for configuration. Validate a complete stream configuration before deployment.
