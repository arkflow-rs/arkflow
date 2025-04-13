# Memory

The Memory buffer component provides an in-memory message queue for temporary message storage and buffering. It implements a FIFO (First-In-First-Out) queue with configurable capacity and timeout settings.

## Configuration

### **capacity**

The maximum number of messages that can be stored in the memory buffer. When this limit is reached, the buffer will trigger processing of the buffered messages to apply backpressure to upstream components.

type: `integer`

required: `true`

### **timeout**

The duration to wait before processing buffered messages, even if the buffer is not full. This ensures messages don't stay in the buffer indefinitely.

type: `string`

required: `true`

example: `1ms`, `1s`, `1m`, `1h`, `1d`

## Internal Mechanism

- Messages are stored in a thread-safe queue using `RwLock<VecDeque>`
- Messages are written to the front of the queue and read from the back (FIFO)
- When the total message count reaches the configured capacity, the buffer triggers message processing
- A background timer periodically checks the timeout condition to process messages
- Messages are batched and concatenated during processing for better performance
- Implements proper backpressure handling to prevent memory overflow

## Examples

```yaml
buffer:
  type: "memory"
  capacity: 100  # Process after 100 messages
  timeout: "1s" # Or process after 1 second
```

This example configures a memory buffer that will process messages either when:
- The total number of buffered messages reaches 100
- 1 second has elapsed since the last message was received

The buffer helps smooth out traffic spikes and provides backpressure when downstream components can't keep up with the incoming message rate.