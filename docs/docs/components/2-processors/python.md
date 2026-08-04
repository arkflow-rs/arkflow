---
description: ArkFlow documentation page.
---

# Python

The Python processor executes user-supplied Python code on each message batch via PyO3. The incoming `MessageBatch` is exposed to your function as a PyArrow `RecordBatch`, and the function returns one or more PyArrow batches that are converted back into the engine's columnar format. This lets you implement custom transformations using Python's data ecosystem (PyArrow, Pandas, Polars, NumPy, etc.).

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `python` |
| script | string | no | — | Inline Python source to execute. When provided, the code runs in the configured module. |
| module | string | no | `__main__` | Python module to import the function from. Ignored when `script` is provided. |
| function | string | yes | — | Name of the Python function to call. It must accept a PyArrow batch and return a list of PyArrow batches. |
| python_path | array&lt;string&gt; | no | `[]` | Additional paths added to `sys.path` for module imports. |

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

## Notes

### PyArrow data processing cases

The snippets below show common data-processing patterns you can use inside your Python function.

#### Data filtering

```python
def filter_data(batch):
    import pyarrow as pa
    import pyarrow.compute as pc

    value_array = batch.column('value')
    mask = pc.greater(value_array, 15)
    filtered_batch = batch.filter(mask)

    return [filtered_batch]
```

#### Data transformation

```python
def transform_data(batch):
    import pyarrow as pa
    import pyarrow.compute as pc

    value_array = batch.column('value')
    doubled_values = pc.multiply(value_array, 2)
    squared_values = pc.power(value_array, 2)

    new_fields = [
        pa.field('timestamp', pa.int64()),
        pa.field('value', pa.int64()),
        pa.field('sensor', pa.string()),
        pa.field('value_doubled', pa.int64()),
        pa.field('value_squared', pa.int64())
    ]
    new_schema = pa.schema(new_fields)
    new_batch = pa.RecordBatch.from_arrays(
        [
            batch.column('timestamp'),
            batch.column('value'),
            batch.column('sensor'),
            doubled_values,
            squared_values
        ],
        schema=new_schema
    )

    return [new_batch]
```

#### Data aggregation

```python
def aggregate_data(batch):
    import pyarrow as pa
    import pandas as pd

    df = batch.to_pandas()
    aggregated = df.groupby('sensor').agg({
        'value': ['mean', 'min', 'max', 'sum', 'count']
    }).reset_index()
    aggregated.columns = ['sensor', 'value_mean', 'value_min', 'value_max', 'value_sum', 'value_count']
    result_batch = pa.RecordBatch.from_pandas(aggregated)

    return [result_batch]
```

#### Time series processing

```python
def process_timeseries(batch):
    import pyarrow as pa
    import pandas as pd

    df = batch.to_pandas()
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('datetime', inplace=True)
    df['value_ma'] = df['value'].rolling('5s').mean()
    df['value_change'] = df['value'].pct_change()
    df.reset_index(inplace=True)
    result_batch = pa.RecordBatch.from_pandas(df)

    return [result_batch]
```

#### Splitting a batch into multiple batches

```python
def split_data(batch):
    import pyarrow.compute as pc

    value_array = batch.column('value')
    high_values_mask = pc.greater_equal(value_array, 50)
    low_values_mask = pc.less(value_array, 50)

    high_values_batch = batch.filter(high_values_mask)
    low_values_batch = batch.filter(low_values_mask)

    return [high_values_batch, low_values_batch]
```

#### Using Polars for high-performance data manipulation

Polars is a fast DataFrame library implemented in Rust that uses the Apache Arrow columnar format as its memory model, making it a performant alternative to Pandas for larger datasets.

```python
def aggregate_with_polars(batch):
    import polars as pl

    df = pl.from_arrow(batch)
    aggregated_df = df.group_by("sensor").agg([
        pl.col("value").mean().alias("value_mean"),
        pl.col("value").min().alias("value_min"),
        pl.col("value").max().alias("value_max"),
        pl.col("value").sum().alias("value_sum"),
        pl.col("value").count().alias("value_count"),
    ])
    result_batch = aggregated_df.to_arrow()

    return [result_batch]
```
