# Join

The Join buffer component enables SQL join operations across multiple input sources. It allows you to correlate and combine data from different streams within a window.

## Configuration

### **join**

Join configuration object.

type: `object`

optional: `false`

#### **join.query**

SQL query for joining data from multiple sources.

- Use table names like `flow_input1`, `flow_input2`, etc. to reference different input sources
- Support for INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL OUTER JOIN
- Can include WHERE, GROUP BY, HAVING clauses

type: `string`

optional: `false` (when join is specified)

#### **join.codec** (optional)

Codec configuration for data transformation.

type: `object`

optional: `true`

##### **join.codec.type**

Codec type.

Supported values: `json`, `protobuf`, `arrow`

type: `string`

optional: `false` (when codec is specified)

## Examples

### Simple Inner Join

```yaml
buffer:
  type: "session_window"
  gap: 1s
  join:
    query: "SELECT * FROM flow_input1 INNER JOIN flow_input2 ON flow_input1.id = flow_input2.id"
    codec:
      type: "json"
```

### Join with Projection

```yaml
buffer:
  type: "tumbling_window"
  size: 10s
  join:
    query: |
      SELECT
        flow_input1.user_id,
        flow_input1.name,
        flow_input2.order_id,
        flow_input2.amount
      FROM flow_input1
      INNER JOIN flow_input2 ON flow_input1.user_id = flow_input2.user_id
    codec:
      type: "json"
```

### Left Join with Aggregation

```yaml
buffer:
  type: "sliding_window"
  size: 1m
  slide: 30s
  join:
    query: |
      SELECT
        flow_input1.device_id,
        flow_input1.location,
        AVG(flow_input2.temperature) as avg_temp,
        MAX(flow_input2.humidity) as max_humidity
      FROM flow_input1
      LEFT JOIN flow_input2 ON flow_input1.device_id = flow_input2.device_id
      GROUP BY flow_input1.device_id, flow_input1.location
    codec:
      type: "json"
```

### Three-way Join

```yaml
buffer:
  type: "session_window"
  gap: 5s
  join:
    query: |
      SELECT
        a.event_id,
        a.timestamp,
        b.user_name,
        c.product_name,
        b.quantity
      FROM flow_input1 a
      INNER JOIN flow_input2 b ON a.event_id = b.event_id
      INNER JOIN flow_input3 c ON b.product_id = c.product_id
    codec:
      type: "json"
```

### Join with Filters

```yaml
buffer:
  type: "tumbling_window"
  size: 1m
  join:
    query: |
      SELECT
        flow_input1.sensor_id,
        flow_input1.reading,
        flow_input2.threshold,
        flow_input1.reading > flow_input2.threshold as alert
      FROM flow_input1
      INNER JOIN flow_input2 ON flow_input1.sensor_type = flow_input2.sensor_type
      WHERE flow_input1.timestamp > NOW() - INTERVAL '1' HOUR
    codec:
      type: "json"
```

### Self-Join Pattern

```yaml
buffer:
  type: "sliding_window"
  size: 5m
  slide: 1m
  join:
    query: |
      SELECT
        a.user_id,
        a.current_value,
        b.previous_value,
        a.current_value - b.previous_value as delta
      FROM flow a
      INNER JOIN flow b ON a.user_id = b.user_id
      WHERE a.timestamp > b.timestamp
    codec:
      type: "json"
```

## Features

- **Multiple Sources**: Join data from 2 or more input sources
- **SQL Support**: Full SQL query capabilities with DataFusion
- **Window Types**: Works with tumbling, sliding, and session windows
- **Flexible Joins**: Support for INNER, LEFT, RIGHT, and FULL OUTER joins
- **Aggregations**: GROUP BY, HAVING, and aggregate functions
- **Data Transformation**: Optional codec support for data conversion

## How It Works

1. **Data Collection**: The buffer collects data from multiple input sources within the window
2. **Table Registration**: Each input source is registered as a table (`flow_input1`, `flow_input2`, etc.)
3. **Join Execution**: The SQL query is executed to join and transform the data
4. **Result Output**: Joined results are passed to the pipeline processors

## Table Naming Convention

When using multiple inputs with join buffer:
- The first input is registered as `flow_input1`
- The second input is registered as `flow_input2`
- The third input is registered as `flow_input3`
- And so on...

## Use Cases

### Data Enrichment
Join event streams with reference data:

```yaml
join:
  query: |
    SELECT
      events.*,
      reference.category,
      reference.priority
    FROM flow_input1 events
    INNER JOIN flow_input2 reference ON events.type_id = reference.id
```

### Correlation Analysis
Correlate data from different sensors:

```yaml
join:
  query: |
    SELECT
      temp.sensor_id,
      temp.temperature,
      hum.humidity,
      temp.temperature * hum.humidity as heat_index
    FROM flow_input1 temp
    INNER JOIN flow_input2 hum ON temp.sensor_id = hum.sensor_id
```

### Time Series Join
Join time-series data with different timestamps:

```yaml
join:
  query: |
    SELECT
      COALESCE(a.timestamp, b.timestamp) as timestamp,
      a.value as metric_a,
      b.value as metric_b
    FROM flow_input1 a
    FULL OUTER JOIN flow_input2 b ON a.timestamp = b.timestamp
```

### Session Analysis
Analyze user sessions across multiple event types:

```yaml
join:
  query: |
    SELECT
      user_id,
      COUNT(DISTINCT pageview.session_id) as page_views,
      COUNT(DISTINCT click.session_id) as clicks,
      SUM(click.amount) as total_spent
    FROM flow_input1 pageview
    LEFT JOIN flow_input2 click ON pageview.user_id = click.user_id
    GROUP BY user_id
```

## Performance Considerations

1. **Window Size**: Smaller windows use less memory but may miss join matches
2. **Data Volume**: Large windows with high data rates can consume significant memory
3. **Join Complexity**: Multiple joins and complex queries are more CPU-intensive
4. **Indexing**: DataFusion optimizes queries, but consider filter pushdown
5. **Memory Management**: Monitor buffer memory usage in production

## Best Practices

1. **Specify Columns**: Select only required columns instead of using `SELECT *`
2. **Filter Early**: Use WHERE clauses to reduce data before joining
3. **Window Choice**: Choose appropriate window type for your use case:
   - **Tumbling**: Fixed-size, non-overlapping windows
   - **Sliding**: Continuous analysis with overlap
   - **Session**: Activity-based grouping
4. **Join Type**: Use the most restrictive join type that meets your needs (INNER > LEFT > FULL)
5. **Aggregate Wisely**: Use aggregations to reduce result size

## Error Handling

- Invalid SQL queries will prevent stream startup
- Type mismatches between sources will cause runtime errors
- Missing tables (inputs) will be reported at startup
- Join results with no matches may be empty depending on join type

## Advanced Features

### Custom Functions
Use custom UDFs in join queries:

```yaml
join:
  query: |
    SELECT
      flow_input1.id,
      custom_transform(flow_input1.value) as transformed
    FROM flow_input1
    INNER JOIN flow_input2 ON flow_input1.id = flow_input2.id
```

### Time-based Joins
Join with time conditions:

```yaml
join:
  query: |
    SELECT
      a.event_id,
      a.value,
      b.value
    FROM flow_input1 a
    INNER JOIN flow_input2 b
      ON a.event_id = b.event_id
      AND ABS(a.timestamp - b.timestamp) < 60
```

### Subqueries
Use subqueries for complex logic:

```yaml
join:
  query: |
    SELECT *
    FROM (
      SELECT
        device_id,
        AVG(temperature) as avg_temp
      FROM flow_input1
      GROUP BY device_id
    ) agg
    INNER JOIN flow_input2 ref ON agg.device_id = ref.device_id
    WHERE agg.avg_temp > ref.threshold
```
