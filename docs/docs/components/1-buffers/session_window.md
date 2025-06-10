# Session Window

The Session Window buffer component provides a session-based message grouping mechanism where messages are grouped based on activity gaps. It implements a session window that closes after a configurable period of inactivity.

## Configuration

### **gap**

The maximum time gap between messages in a session. If no new messages arrive within this duration, the session is considered complete and all accumulated messages are emitted.

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
- Messages are grouped by session keys using `RwLock<HashMap<String, Arc<RwLock<VecDeque>>>>`
- Each session maintains its own message queue with independent timeout tracking
- When a session exceeds the configured gap duration without new messages, it triggers window emission
- Messages within the same session are batched and concatenated during processing using Arrow's concat_batches
- Optional SQL join operations are performed using DataFusion's query engine with parallel decoding
- Uses Tokio's async runtime with cancellation tokens for efficient timeout management
- Implements proper session cleanup to prevent memory leaks from inactive sessions
- Join operations validate that all required input tables are present before executing SQL queries

## Examples

### Basic Configuration

```yaml
buffer:
  type: "session_window"
  gap: "5s" # Close session after 5 seconds of inactivity
```

This example configures a session window buffer that will:
- Group messages into sessions
- Close the session and process messages when no new messages arrive for 5 seconds

The buffer helps group related messages that occur close together in time while separating unrelated messages that have gaps between them.

### With Join Configuration

```yaml
buffer:
  type: "session_window"
  gap: "10s"  # Close session after 10 seconds of inactivity
  join:
    query: "SELECT a.user_id, a.event_type, b.metadata FROM events a JOIN metadata b ON a.user_id = b.user_id"
    codec:
      type: "json"
```

This example configures a session window buffer with SQL join operations that:
- Groups messages into sessions with 10-second inactivity gap
- Joins event data with metadata using SQL
- Uses JSON codec for message decoding
- Processes joined data when session closes