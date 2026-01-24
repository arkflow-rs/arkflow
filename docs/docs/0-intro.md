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

- **Kafka**: Read data from Kafka topics with consumer group support
- **MQTT**: Subscribe to messages from MQTT topics with QoS levels
- **HTTP**: Receive data via HTTP endpoints
- **File**: Reading data from files (CSV, JSON, Parquet, Avro, Arrow) with cloud storage support and SQL queries
- **Generate**: Generate synthetic test data
- **Memory**: In-memory data source for testing
- **Modbus**: Industrial protocol support for sensor data
- **Multiple Inputs**: Combine multiple input streams into one pipeline
- **Nats**: Subscribe to messages from NATS topics with JetStream support
- **Pulsar**: Subscribe to messages from Pulsar topics with various subscription types
- **Redis**: Subscribe to messages from Redis streams, lists, or pub/sub channels
- **SQL**: Query data from SQL databases (MySQL, PostgreSQL, SQLite)
- **WebSocket**: Real-time communication via WebSocket connections

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

- **Drop**: Discard messages (useful for testing)
- **HTTP**: Send data via HTTP with authentication support
- **InfluxDB**: Write time-series data to InfluxDB 2.x with batching and retry logic
- **Kafka**: Write data to Kafka topics with automatic partitioning
- **MQTT**: Publish messages to MQTT topics
- **Nats**: Publish messages to NATS topics
- **Pulsar**: Publish messages to Pulsar topics
- **Redis**: Write to Redis streams, lists, or pub/sub channels
- **SQL**: Write to SQL databases (MySQL, PostgreSQL, SQLite) with batch inserts and UPSERT
- **Stdout**: Output data to the console for debugging

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

ArkFlow provides buffer capabilities to handle backpressure, windowing, and joining of messages:

- **Memory Buffer**: Simple in-memory buffer for high-throughput scenarios
- **Tumbling Window**: Fixed-size, non-overlapping time windows
- **Sliding Window**: Overlapping time windows with configurable size and slide interval
- **Session Window**: Dynamic windows based on activity gaps
- **Join**: SQL join operations across multiple input sources within a window

Example:

```yaml
buffer:
  type: memory
  capacity: 10000  # Maximum number of messages to buffer
  timeout: 10s  # Maximum time to buffer messages
```

## Advanced Features

### Cloud Storage Integration

ArkFlow File input supports multiple cloud storage providers:

```yaml
- input:
    type: "parquet"
    path: "s3://bucket/data.parquet"
    object_store:
      type: "s3"
      region: "us-west-2"
      bucket_name: "my-bucket"
      access_key_id: "${AWS_ACCESS_KEY_ID}"
      secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
```

Supported providers: AWS S3, Google Cloud Storage, Azure Blob Storage, HTTP, HDFS

### Multi-Source Joins

Combine and correlate data from multiple sources:

```yaml
buffer:
  type: "session_window"
  gap: 1s
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

### Time-Series Data to InfluxDB

```yaml
- output:
    type: "influxdb"
    url: "http://localhost:8086"
    org: "production"
    bucket: "sensor-data"
    token: "${INFLUXDB_TOKEN}"
    measurement: "temperature"
    tags:
      - name: "location"
        value: "datacenter"
      - name: "device"
        value: "device_id"
    fields:
      - name: "value"
        value: "temperature"
        value_type: "float"
      - name: "status"
        value: "status"
        value_type: "boolean"
    batch_size: 1000
    flush_interval: 5s
```

### Python UDF Processing

```yaml
pipeline:
  processors:
    - type: "python"
      script: |
        def transform_data(batch):
            import pyarrow as pa
            values = batch.column("value").to_pylist()
            transformed = [x * 2 for x in values]
            new_array = pa.array(transformed)
            new_batch = batch.add_column(
                batch.num_columns,
                "doubled_value",
                new_array
            )
            return new_batch
      function: "transform_data"
```
