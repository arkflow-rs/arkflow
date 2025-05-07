# Session Window

The Session Window buffer component provides a session-based message grouping mechanism where messages are grouped based on activity gaps. It implements a session window that closes after a configurable period of inactivity.

## Configuration

### **gap**

The duration of inactivity that triggers the closing of a session window. When this period elapses without new messages, the buffer will process the messages in the current session.

type: `string`

required: `true`

example: `1ms`, `1s`, `1m`, `1h`, `1d`

## Internal Mechanism

- Messages are stored in a thread-safe queue using `RwLock<VecDeque>`
- Each message arrival resets an inactivity timer
- When the gap duration elapses without new messages, the session window closes and processes messages
- Messages are batched and concatenated during processing for better performance
- Implements proper backpressure handling to prevent memory overflow

## Examples

```yaml
buffer:
  type: "session_window"
  gap: "5s" # Close session after 5 seconds of inactivity
```

This example configures a session window buffer that will:
- Group messages into sessions
- Close the session and process messages when no new messages arrive for 5 seconds

The buffer helps group related messages that occur close together in time while separating unrelated messages that have gaps between them.