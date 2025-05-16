---
sidebar_position: 1
logo: ./logo.svg
---

# Introduction

ArkFlow is a high-performance Rust stream processing engine that provides powerful data stream processing capabilities, supporting various input/output sources and processors.

:::tip
Currently, ArkFlow is **stateless**, but it can still help you solve most data engineering problems. It implements transaction-based resilience and features backpressure. 

Therefore, when connected to input and output sources that provide at-least-once semantics, it can guarantee at-least-once delivery without needing to retain messages in transit.

In the future, we will gradually improve the functions of ArkFlow to enable it to have transactional and state management capabilities, so as to better meet various data processing needs.

:::


logo([Logo Usage Guidelines](./about-logo)）: 

![ArkFlow logo](logo.svg)


## Core Features

- **High Performance**: Built on Rust and Tokio async runtime, delivering exceptional performance and low latency
- **Multiple Data Sources**: Support for Kafka, MQTT, HTTP, files, and other input/output sources
- **Powerful Processing**: Built-in SQL queries, JSON processing, Protobuf encoding/decoding, batch processing, and other processors
- **Extensibility**: Modular design, easy to extend with new input, output, and processor components

## Installation

### Building from Source

```bash
# Clone repository
git clone https://github.com/arkflow-rs/arkflow.git
cd arkflow

# Build project
cargo build --release

# Run tests
cargo test
```

## Quick Start

1. Create a configuration file `config.yaml`:

```yaml
logging:
  level: info
streams:
  - input:
      type: "generate"
      context: '{ "timestamp": 1625000000000, "value": 10, "sensor": "temp_1" }'
      interval: 1s
      batch_size: 10
    buffer:
      type: "memory"
      capacity: 10
      timeout: 10s
    pipeline:
      thread_num: 4
      processors:
        - type: "json_to_arrow"
        - type: "sql"
          query: "SELECT * FROM flow WHERE value >= 10"

    output:
      type: "stdout"
    error_output:
      type: "stdout"
```

2. Run ArkFlow:

```bash
./target/release/arkflow --config config.yaml
```

## Configuration Guide

ArkFlow uses YAML format configuration files and supports the following main configuration items:

### Top-level Configuration

```yaml
logging:
  level: info  # Log levels: debug, info, warn, error

streams: # Stream definition list
  - input:      # Input configuration
    # ...
    pipeline:   # Pipeline configuration
    # ...
    output:     # Output configuration
    # ...
    error_output: # Error output configuration
    # ...
    buffer:     # Buffer configuration
    # ... 
```


### Input Components

ArkFlow supports multiple input sources:

- **Kafka**: Read data from Kafka topics
- **MQTT**: Subscribe to messages from MQTT topics
- **HTTP**: Receive data via HTTP
- **File**: Reading data from files(Csv,Json, Parquet, Avro, Arrow) using SQL
- **Generator**: Generate test data
- **Database**: Query data from databases(MySQL, PostgreSQL, SQLite, Duckdb)
- **Nats**: Subscribe to messages from Nats topics
- **Redis**: Subscribe to messages from Redis channels or lists
- **Websocket**: Subscribe to messages from WebSocket connections

Example:

```yaml
input:
  type: kafka
  brokers:
    - localhost:9092
  topics:
    - test-topic
  consumer_group: test-group
  client_id: arkflow
  start_from_latest: true
```

### Processors

ArkFlow provides multiple data processors:

- **JSON**: JSON data processing and transformation
- **SQL**: Process data using SQL queries
- **Protobuf**: Protobuf encoding/decoding
- **Batch Processing**: Process messages in batches
- **Vrl**: Process data using [VRL](https://vector.dev/docs/reference/vrl/)

Example:

```yaml
pipeline:
  thread_num: 4
  processors:
    - type: json_to_arrow
    - type: sql
      query: "SELECT * FROM flow WHERE value >= 10"
```

### Output Components

ArkFlow supports multiple output targets:

- **Kafka**: Write data to Kafka topics
- **MQTT**: Publish messages to MQTT topics
- **HTTP**: Send data via HTTP
- **Standard Output**: Output data to the console
- **Drop**: Discard data
- **Nats**: Publish messages to Nats topics

Example:

```yaml
output:
  type: kafka
  brokers:
    - localhost:9092
  topic: 
    type: value
    value: test-topic
  client_id: arkflow-producer
```
### Error Output Components
ArkFlow supports multiple error output targets:
- **Kafka**: Write error data to Kafka topics
- **MQTT**: Publish error messages to MQTT topics
- **HTTP**: Send error data via HTTP
- **Standard Output**: Output error data to the console
- **Drop**: Discard error data
- **Nats**: Publish messages to Nats topics

Example:

```yaml
error_output:
  type: kafka
  brokers:
    - localhost:9092
  topic: 
    type: value
    value: error-topic
  client_id: error-arkflow-producer
``` 


### Buffer Components

ArkFlow provides buffer capabilities to handle backpressure and temporary storage of messages:

- **Memory Buffer**: Memory buffer, for high-throughput scenarios and window aggregation.
- **Session Window**: The Session Window buffer component provides a session-based message grouping mechanism where messages are grouped based on activity gaps. It implements a session window that closes after a configurable period of inactivity.
- **Sliding Window**: The Sliding Window buffer component provides a time-based windowing mechanism for processing message batches. It implements a sliding window algorithm with configurable window size, slide interval and slide size.
- **Tumbling Window**: The Tumbling Window buffer component provides a fixed-size, non-overlapping windowing mechanism for processing message batches. It implements a tumbling window algorithm with configurable interval settings.

Example:

```yaml
buffer:
  type: memory
  capacity: 10000  # Maximum number of messages to buffer
  timeout: 10s  # Maximum time to buffer messages
```
