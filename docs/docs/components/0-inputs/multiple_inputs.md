# Multiple Inputs

The Multiple Inputs component allows you to combine multiple input sources into a single stream. It reads from multiple input components concurrently and merges their outputs into a unified message stream.

## Configuration

### **inputs**

A list of input configurations to be combined. Each input in the list follows the standard input configuration format.

type: `array` of `InputConfig`

required: `true`

## Features

- **Concurrent Processing**: All input sources are processed concurrently using async tasks
- **Message Merging**: Messages from different inputs are merged into a single output stream
- **Error Handling**: Handles disconnections and errors from individual inputs gracefully
- **Unique Naming**: Ensures all input names are unique to prevent conflicts
- **Proper Cleanup**: Uses cancellation tokens and task tracking for clean shutdown

## Internal Mechanism

- Uses Flume channels for efficient message passing between input tasks and the main reader
- Each input runs in its own async task managed by a TaskTracker
- Messages and errors are sent through a unified channel using an internal Msg enum
- Implements proper cancellation handling using CancellationToken
- Validates input name uniqueness to prevent SQL table name conflicts
- Maintains input name registry for downstream components

## Examples

### Basic Multiple Inputs

```yaml
- input:
    type: "multiple_inputs"
    inputs:
      - name: "kafka_source"
        type: "kafka"
        brokers: ["localhost:9092"]
        topics: ["topic1"]
        consumer_group: "group1"
      - name: "redis_source"
        type: "redis"
        url: "redis://localhost:6379"
        redis_type: "subscribe"
        channels: ["channel1"]
```

### Multiple Inputs with Different Types

```yaml
- input:
    type: "multiple_inputs"
    inputs:
      - name: "http_api"
        type: "http"
        address: "0.0.0.0:8080"
        path: "/webhook"
      - name: "file_source"
        type: "sql"
        select_sql: "SELECT * FROM data"
        input_type:
          type: "csv"
          path: "/path/to/data.csv"
      - name: "memory_source"
        type: "memory"
        messages:
          - "static message 1"
          - "static message 2"
```

## Important Notes

- All inputs must have unique names when specified
- Empty input names are not allowed
- The component will fail if there are duplicate input names
- Each input maintains its own connection and error handling
- Messages are processed as they arrive from any input source
- The component stops when all input sources are closed or encounter terminal errors