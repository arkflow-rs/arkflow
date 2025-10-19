# Python

The Python processor component allows you to execute Python code to process and transform data. It provides a flexible way to implement custom processing logic using Python, enabling you to leverage Python's rich ecosystem of libraries and tools for data processing.

## Configuration

### **script**

Python code to execute directly.

type: `string`

optional: `true`

### **module**

Python module to import. If not specified, the code will be executed in the `__main__` module.

type: `string`

optional: `true`

### **function**

Function name to call for processing. The function should accept a PyArrow batch as input and return a list of PyArrow batches.

type: `string`

required: `true`

### **python_path**

Additional Python paths to add to `sys.path` for module imports.

type: `array[string]`

optional: `true`

default: `[]`

## Data Flow

The Python processor converts the incoming MessageBatch to PyArrow format, passes it to the specified Python function, and then converts the returned PyArrow batches back to MessageBatch format.

## Examples

### Using a Python Module

```yaml
- processor:
    type: "python"
    function: "process_batch"
    module: "example1"
    python_path: ["./examples/python"]
```

In this example, the processor imports the `example1` module from the `./examples/python` directory and calls the `process_batch` function to process the data.

### Using Inline Python Script

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
                # Get the value field in the batch
                value_array = batch.column('value')
                
                # Do math on the values
                doubled_values = pc.multiply(value_array, 2)
                
                # Create a new field
                new_fields = [
                    # Leave the original fields
                    pa.field('timestamp', pa.int64()),
                    pa.field('value', pa.int64()),
                    pa.field('sensor', pa.string()),
                    # Add a new field
                    pa.field('value_doubled', pa.int64())
                ]
                
                # Create a new schema
                new_schema = pa.schema(new_fields)
                
                # Create a new batch of records
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

This example demonstrates a complete pipeline where:
1. First, it generates a JSON message containing timestamp, value, and sensor information
2. Converts the JSON to Arrow format
3. Uses the Python processor to process the data
4. Converts the processed data back to JSON format
5. Finally outputs to standard output

### Example Python Module

```python
def process_batch(batch):
    # The batch parameter is a PyArrow batch
    # You can perform any processing on the batch here
    # For example, you can modify the batch or create a new one
    return [batch]  # Return a list of PyArrow batches


## PyArrow data processing case

Here are some specific examples of data processing with PyArrow, showing how to leverage PyArrow's capabilities in a Python processor.

### Data filtering cases

```python
def filter_data(batch):
    import pyarrow as pa
    import pyarrow.compute as pc
    
    # Get the value field in the batch
    value_array = batch.column('value')
    
    # Use the compute feature of PyArrow to filter data
    # Create a filter with a value greater than 15
    mask = pc.greater(value_array, 15)
    
    # Apply filters
    filtered_batch = batch.filter(mask)
    
    return [filtered_batch]
```

This example shows how to use PyArrow's compute module to filter data and keep only records with values greater than 15.

### The case for data transformation

```python
def transform_data(batch):
    import pyarrow as pa
    import pyarrow.compute as pc
    
    # Get the value field in the batch
    value_array = batch.column('value')
    
    # Do math on the values
    doubled_values = pc.multiply(value_array, 2)
    squared_values = pc.power(value_array, 2)
    
    # Create a new field
    new_fields = [
        # Leave the original fields
        pa.field('timestamp', pa.int64()),
        pa.field('value', pa.int64()),
        pa.field('sensor', pa.string()),
        # Add a new field
        pa.field('value_doubled', pa.int64()),
        pa.field('value_squared', pa.int64())
    ]
    
    # Create a new schema
    new_schema = pa.schema(new_fields)
    
    # Create a new batch of records
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

This example shows how to use PyArrow to transform data, create new fields, and build new batches of records.

### The case for data aggregation

```python
def aggregate_data(batch):
    import pyarrow as pa
    import pyarrow.compute as pc
    import pandas as pd
    
    # Convert PyArrow batches to Pandas DataFrames for aggregation operations
    df = batch.to_pandas()
    
    # Group by sensor and calculate aggregate values
    aggregated = df.groupby('sensor').agg({
        'value': ['mean', 'min', 'max', 'sum', 'count']
    }).reset_index()
    
    # Flatten multi-level listings
    aggregated.columns = ['sensor', 'value_mean', 'value_min', 'value_max', 'value_sum', 'value_count']
    
    # Convert the aggregate results back into a PyArrow batch
    result_batch = pa.RecordBatch.from_pandas(aggregated)
    
    return [result_batch]
```

This example shows how to combine PyArrow and Pandas for data aggregation to calculate statistics by sensor grouping.

### Time series processing cases

```python
def process_timeseries(batch):
    import pyarrow as pa
    import pandas as pd
    import numpy as np
    from datetime import datetime
    
    df = batch.to_pandas()
    
    # Convert timestamps to datetime objects
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
    
    # Set up a time index
    df.set_index('datetime', inplace=True)
    
    # Calculate the moving average
    df['value_ma'] = df['value'].rolling('5s').mean()
    
    # Calculate the rate of change
    df['value_change'] = df['value'].pct_change()
    
    # Reset the index
    df.reset_index(inplace=True)
    
    # Convert the processed DataFrame back to the PyArrow batch
    result_batch = pa.RecordBatch.from_pandas(df)
    
    return [result_batch]
```

This example shows how to work with time series data, including timestamp transformations, moving averages, and rate of change calculations.

### Data splitting cases

```python
def split_data(batch):
    import pyarrow as pa
    import pyarrow.compute as pc
    
    # Get the value field in the batch
    value_array = batch.column('value')
    
    # Create two filters
    high_values_mask = pc.greater_equal(value_array, 50)
    low_values_mask = pc.less(value_array, 50)
    
    # Apply filters to create two batches
    high_values_batch = batch.filter(high_values_mask)
    low_values_batch = batch.filter(low_values_mask)
    
    # Two batches are returned
    return [high_values_batch, low_values_batch]
```

This example shows how to split a data batch into multiple batches to create different data streams based on different criteria.

###  Using Polars for High-Performance Data Manipulation
Polars is a blazingly fast DataFrame library implemented in Rust, using Apache Arrow Columnar Format as its memory model. It provides an easy-to-use and highly performant API for data manipulation.

```python
def aggregate_with_polars(batch):
    import polars as pl

    # Convert a PyArrow RecordBatch to a Polars DataFrame
    df = pl.from_arrow(batch)

    # Group by sensor and calculate aggregate values
    aggregated_df = df.group_by("sensor").agg([
        pl.col("value").mean().alias("value_mean"),
        pl.col("value").min().alias("value_min"),
        pl.col("value").max().alias("value_max"),
        pl.col("value").sum().alias("value_sum"),
        pl.col("value").count().alias("value_count"),
    ])

    # Convert the aggregated Polars DataFrame back to a PyArrow RecordBatch
    result_batch = aggregated_df.to_arrow()

    return [result_batch]

```

This example showcases using Polars for a data aggregation task. It's often significantly faster than using Pandas for similar operations, especially on larger datasets, due to its Rust-based backend and efficient query optimization.
