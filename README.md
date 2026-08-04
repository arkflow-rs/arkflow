# ArkFlow

<p align="center">
<img align="center" width="150px" src="images/logo.svg">
<p align="center">

English | [中文](README_zh.md)

[![Rust](https://github.com/arkflow-rs/arkflow/actions/workflows/rust.yml/badge.svg)](https://github.com/arkflow-rs/arkflow/actions/workflows/rust.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

[Latest docs](https://arkflow-rs.com/docs/intro) | [Dev docs](https://arkflow-rs.com/docs/next/start-here)

<a href="https://www.producthunt.com/posts/arkflow?embed=true&utm_source=badge-featured&utm_medium=badge&utm_souce=badge-arkflow" target="_blank"><img src="https://api.producthunt.com/widgets/embed-image/v1/featured.svg?post_id=942804&theme=light&t=1743136262336" alt="ArkFlow - High&#0045;performance&#0032;rust&#0032;stream&#0032;processing&#0032;engine | Product Hunt" style="width: 250px; height: 54px;" width="250" height="54" /></a>

High performance Rust stream processing engine seamlessly integrates AI capabilities, 
providing powerful real-time data processing and intelligent analysis. 
It not only supports multiple input/output sources and processors, but also enables easy loading and execution of machine learning models, 
enabling streaming data and inference, anomaly detection, and complex event processing.

##  Cloud Native Landscape

<p float="left">
<img src="images/cncf-logo.svg" width="200"/>&nbsp;&nbsp;&nbsp;
<img src="images/cncf-landscape-logo.svg" width="150"/>
</p>

ArkFlow enlisted in the [CNCF Cloud Native Landscape](https://landscape.cncf.io/?item=app-definition-and-development--streaming-messaging--arkflow).

## Features

- **High Performance**: Built on Rust and Tokio async runtime, offering excellent performance and low latency
- **Durable Delivery**: At-least-once by default via per-stream WAL durability, with optional exactly-once for transactional sinks
- **Multiple Data Sources**: Support for Kafka, MQTT, HTTP, files, SQL databases, and many other input/output sources
- **Powerful Processing**: Built-in SQL queries, Python UDFs, JSON processing, Protobuf encoding/decoding, batch processing, and VRL
- **Streaming Codecs**: JSON and Protobuf codecs, plus Debezium CDC envelopes and Confluent Schema Registry wire-format
- **Control Plane**: An optional Hub and web console to observe, configure, and operate multiple ArkFlow compute nodes as a fleet
- **Extensible**: Modular design, easy to extend with new input, buffer, output, and processor components

## Installation

### Building from Source

```bash
# Clone the repository
git clone https://github.com/arkflow-rs/arkflow.git
cd arkflow

# Build the project
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

ArkFlow uses YAML format configuration files, supporting the following main configuration items:

### Top-level Configuration

```yaml
logging:
  level: info  # Log level: debug, info, warn, error

streams: # Stream definition list
  - input:      # Input configuration
    # ...
    pipeline:   # Processing pipeline configuration
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
- **File**: Reading data from files (CSV, JSON, Parquet, Avro, Arrow) with cloud storage support
- **Generate**: Generate synthetic test data
- **SQL**: Query data from SQL databases (MySQL, PostgreSQL, SQLite)
- **NATS**: Subscribe to messages from NATS topics with JetStream support
- **Pulsar**: Subscribe to messages from Pulsar topics
- **Redis**: Subscribe to messages from Redis streams, lists, or pub/sub channels
- **WebSocket**: Subscribe to messages from WebSocket connections
- **Modbus**: Read data from Modbus devices
- **Memory**: In-memory data source for testing
- **Multiple Inputs**: Combine multiple input streams into one pipeline

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
- **VRL**: Process data using [VRL](https://vector.dev/docs/reference/vrl/)
- **Python**: Run Python user-defined functions over the batch

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
- **InfluxDB**: Write time-series data to InfluxDB 2.x
- **NATS**: Publish messages to NATS topics
- **Pulsar**: Publish messages to Pulsar topics
- **Redis**: Write to Redis streams, lists, or pub/sub channels
- **SQL**: Write to SQL databases (MySQL, PostgreSQL, SQLite) with batch inserts and UPSERT
- **Standard Output**: Output data to the console
- **Drop**: Discard data

Example:

```yaml
output:
  type: kafka
  brokers:
    - localhost:9092
  topic:
    type: value
    value:
      type: value
      value: test-topic
  client_id: arkflow-producer
```

### Error Output Components

The `error_output` accepts any of the [output components](#output-components)
listed above and receives messages that failed processing. The most common
choices are Kafka, HTTP, and Standard Output for debugging.

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
- **Session Window**: The Session Window buffer component provides a session-based message grouping mechanism where
  messages are grouped based on activity gaps. It implements a session window that closes after a configurable period of
  inactivity.
- **Sliding Window**: The Sliding Window buffer component provides a time-based windowing mechanism for processing
  message batches. It implements a sliding window algorithm with configurable window size, slide interval and slide
  size.
- **Tumbling Window**: The Tumbling Window buffer component provides a fixed-size, non-overlapping windowing mechanism
  for processing message batches. It implements a tumbling window algorithm with configurable interval settings.

Example:

```yaml
buffer:
  type: memory
  capacity: 10000  # Maximum number of messages to buffer
  timeout: 10s  # Maximum time to buffer messages
```

## Examples

### Kafka to Kafka Data Processing

```yaml
streams:
  - input:
      type: kafka
      brokers:
        - localhost:9092
      topics:
        - test-topic
      consumer_group: test-group

    pipeline:
      thread_num: 4
      processors:
        - type: json_to_arrow
        - type: sql
          query: "SELECT * FROM flow WHERE value > 100"

    output:
      type: kafka
      brokers:
        - localhost:9092
      topic:
        type: value
        value: test-topic
```

### Generate Test Data and Process

```yaml
streams:
  - input:
      type: "generate"
      context: '{ "timestamp": 1625000000000, "value": 10, "sensor": "temp_1" }'
      interval: 1ms
      batch_size: 10000

    pipeline:
      thread_num: 4
      processors:
        - type: "json_to_arrow"
        - type: "sql"
          query: "SELECT count(*) FROM flow WHERE value >= 10 group by sensor"

    output:
      type: "stdout"
```

## Users

- Conalog(Country: South Korea)

## ArkFlow Plugin

[ArkFlow Plugin Examples](https://github.com/arkflow-rs/arkflow-plugin-examples)

## License

ArkFlow is licensed under the [Apache License 2.0](LICENSE).

## Community

Discord: https://discord.gg/CwKhzb8pux

If you like or are using this project to learn or start your solution, please give it a star⭐. Thanks!
