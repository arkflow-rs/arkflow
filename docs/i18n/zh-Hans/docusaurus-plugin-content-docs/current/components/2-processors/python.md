---
components: [python]
description: ArkFlow 文档页面。
---

# Python

Python 处理器通过 PyO3 对每个消息批次执行用户提供的 Python 代码。传入的 `MessageBatch` 会以 PyArrow `RecordBatch` 的形式暴露给你的函数,函数返回一个或多个 PyArrow 批次,这些批次再被转换回引擎的列式格式。借助 Python 的数据生态(PyArrow、Pandas、Polars、NumPy 等),你可以据此实现自定义变换。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `python` |
| script | string | no | — | 要执行的内联 Python 源码。提供时,代码会在所配置的模块中运行。 |
| module | string | no | `__main__` | 从中导入函数的 Python 模块。提供 `script` 时忽略此项。 |
| function | string | yes | — | 要调用的 Python 函数名。该函数必须接受一个 PyArrow 批次,并返回一个 PyArrow 批次列表。 |
| python_path | array&lt;string&gt; | no | `[]` | 为模块导入而添加到 `sys.path` 的额外路径。 |

## 示例

### 使用 Python 模块

```yaml validate=fragment wrap=processors
- type: "python"
  function: "process_batch"
  module: "example1"
  python_path: ["./examples/python"]
```

### 使用内联 Python 脚本

```yaml validate=fragment wrap=processors
- type: "python"
  script: |
    def process_batch(batch):
        # Process the batch here
        # For example, you can modify the batch or create a new one
        return [batch]
  function: "process_batch"
```

### 完整流水线示例

```yaml validate=foreign reason="requires the optional PyArrow Python runtime"
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

### Python 模块示例

```python
def process_batch(batch):
    # The batch parameter is a PyArrow batch
    # You can perform any processing on the batch here
    # For example, you can modify the batch or create a new one
    return [batch]  # Return a list of PyArrow batches
```

## 说明

### PyArrow 数据处理示例

下面的代码片段展示了可以在你的 Python 函数中使用的常见数据处理模式。

#### 数据过滤

```python
def filter_data(batch):
    import pyarrow as pa
    import pyarrow.compute as pc

    value_array = batch.column('value')
    mask = pc.greater(value_array, 15)
    filtered_batch = batch.filter(mask)

    return [filtered_batch]
```

#### 数据变换

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

#### 数据聚合

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

#### 时间序列处理

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

#### 将一个批次拆分为多个批次

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

#### 使用 Polars 进行高性能数据操作

Polars 是一个用 Rust 实现的快速 DataFrame 库,它以 Apache Arrow 列式格式作为内存模型,对于较大的数据集来说是 Pandas 的高性能替代选择。

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
