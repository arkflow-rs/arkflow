# Sliding Window

The Sliding Window buffer component provides a time-based windowing mechanism for processing message batches. It implements a sliding window algorithm with configurable window size, slide interval and slide size.

## Configuration

### **window_size**

The number of messages that define the window size. When this number of messages is collected, the window will slide forward.

type: `integer`

required: `true`

### **interval**

The duration between window slides, even if the window is not full. This ensures messages don't stay in the buffer indefinitely.

type: `string`

required: `true`

example: `1ms`, `1s`, `1m`, `1h`, `1d`

### **slide_size**

The number of messages to remove from the window after each emission. This determines how much the window "slides" forward with each processing cycle. Must be greater than 0 and less than or equal to window_size.

type: `integer`

required: `true`

## Internal Mechanism

- Messages are stored in a thread-safe queue using `RwLock<VecDeque>`
- A background timer triggers window processing at the configured interval using Tokio's async runtime
- When triggered, the buffer processes up to `window_size` messages from the queue
- Messages are merged and concatenated using Arrow's concat_batches for efficient processing
- After processing, the window slides forward by removing `slide_size` messages from the front of the queue
- Acknowledgments are combined using VecAck to ensure proper message acknowledgment
- Uses cancellation tokens for graceful shutdown and resource cleanup
- Implements proper backpressure handling to prevent memory overflow
- The sliding mechanism allows for overlapping windows when slide_size < window_size

## Examples

```yaml
buffer:
  type: "sliding_window"
  window_size: 100  # Process after 100 messages
  interval: "1s"    # Or process after 1 second
  slide_size: 10    # Slide forward by 10 messages
```

This example configures a sliding window buffer that will process messages either when:
- The total number of buffered messages reaches 100
- 1 second has elapsed since the last window slide

The buffer then slides forward by 10 messages for the next window.