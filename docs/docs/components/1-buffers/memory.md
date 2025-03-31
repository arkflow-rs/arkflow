# Memory

The Memory buffer component provides an in-memory message queue for temporary message storage and buffering.

## Configuration

### **capacity**

The maximum number of messages that can be stored in the memory buffer.

type: `integer`

required: `true`

### **timeout**

The duration to wait before processing buffered messages.

type: `string`

required: `true`

example: `1ms`, `1s`, `1m`, `1h`, `1d`

## Examples

```yaml
buffer:
  type: "memory"
  capacity: 100
  timeout: "1s"
```

This example configures a memory buffer with a capacity of 100 messages and a processing timeout of 1 second. When the buffer is full, backpressure will be applied to upstream components.