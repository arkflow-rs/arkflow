# Tumbling Window

The Tumbling Window buffer component provides a fixed-size, non-overlapping windowing mechanism for processing message batches. It implements a tumbling window algorithm with configurable interval settings.

## Configuration

### **interval**

The fixed duration of each window period. When this interval elapses, all accumulated messages are emitted.

type: `string`

required: `true`

example: `1ms`, `1s`, `1m`, `1h`, `1d`

### **join**

Optional join configuration for SQL join operations on message batches. When specified, allows joining multiple message sources using SQL queries.

type: `object`

required: `false`

#### **query**

The SQL query to execute for joining message batches from different input sources.

type: `string`

required: `true` (when join is specified)

#### **value_field**

The field name to use for binary data values. Defaults to the system default binary value field.

type: `string`

required: `false`

#### **codec**

The codec configuration for decoding message batches before joining.

type: `object`

required: `true` (when join is specified)

## Internal Mechanism

- Built on top of the `BaseWindow` component which provides core windowing functionality
- Messages are grouped by input name using `RwLock<HashMap<String, Arc<RwLock<VecDeque>>>>`
- A background timer triggers window processing at the configured interval using Tokio's async runtime
- When the interval elapses, all accumulated messages are processed as a single window
- Messages are batched and concatenated using Arrow's concat_batches for efficient processing
- Optional SQL join operations are performed using DataFusion's query engine with parallel decoding
- Uses cancellation tokens for graceful shutdown and resource cleanup
- Join operations validate that all required input tables are present before executing SQL queries
- Implements proper backpressure handling to prevent memory overflow
- Windows are non-overlapping, with each message belonging to exactly one window

## Examples

### Basic Configuration

```yaml
buffer:
  type: "tumbling_window"
  interval: "1s"    # Process every 1 second
```

This example configures a tumbling window buffer that will process messages every 1 second, regardless of message count.

### With Join Configuration

```yaml
buffer:
  type: "tumbling_window"
  interval: "5s"    # Process every 5 seconds
  join:
    query: "SELECT a.id, a.name, b.value FROM input1 a JOIN input2 b ON a.id = b.id"
    codec:
      type: "json"
```

This example configures a tumbling window buffer with SQL join operations that:
- Processes messages every 5 seconds
- Joins data from two input sources using SQL
- Uses JSON codec for message decoding