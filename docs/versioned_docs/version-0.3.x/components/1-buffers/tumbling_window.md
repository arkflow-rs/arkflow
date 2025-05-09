# Tumbling Window

The Tumbling Window buffer component provides a fixed-size, non-overlapping windowing mechanism for processing message batches. It implements a tumbling window algorithm with configurable interval settings.

## Configuration

### **interval**

The duration between window slides. This determines how often the window will process messages regardless of message count.

type: `string`

required: `true`

example: `1ms`, `1s`, `1m`, `1h`, `1d`

## Internal Mechanism

- Messages are stored in a thread-safe queue using `RwLock<VecDeque>`
- A background timer periodically triggers window processing based on the interval
- When the timer fires, all buffered messages are processed as a batch
- Messages are batched and concatenated during processing for better performance
- Implements proper backpressure handling to prevent memory overflow
- Uses Tokio's async runtime for efficient timer handling

## Examples

```yaml
buffer:
  type: "tumbling_window"
  interval: "1s"    # Process every 1 second
```

This example configures a tumbling window buffer that will process messages every 1 second, regardless of message count.