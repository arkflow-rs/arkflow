# ArkFlow

English | [中文](README_zh.md)

**‼️Not production-ready, do not use in a production environment‼️**

[![Rust](https://github.com/chenquan/arkflow/actions/workflows/rust.yml/badge.svg)](https://github.com/chenquan/arkflow/actions/workflows/rust.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

High-performance Rust stream processing engine, providing powerful data stream processing capabilities, supporting multiple input/output sources and processors.

## Features

- **High Performance**: Built on Rust and Tokio async runtime, offering excellent performance and low latency
- **Multiple Data Sources**: Support for Kafka, MQTT, HTTP, files, and other input/output sources
- **Powerful Processing Capabilities**: Built-in SQL queries, JSON processing, Protobuf encoding/decoding, batch processing, and other processors
- **Extensible**: Modular design, easy to extend with new input, output, and processor components

## Installation

### Building from Source

```bash
# Clone the repository
git clone https://github.com/chenquan/arkflow.git
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
        - type: "arrow_to_json"

    output:
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

streams:       # Stream definition list
  - input:      # Input configuration
      # ...
    pipeline:   # Processing pipeline configuration
      # ...
    output:     # Output configuration
      # ...
```

### Input Components

ArkFlow supports multiple input sources:

- **Kafka**: Read data from Kafka topics
- **MQTT**: Subscribe to messages from MQTT topics
- **HTTP**: Receive data via HTTP
- **SQL**: Reading data from files using SQL
- **Generator**: Generate test data
- **SQL**: Query data from databases

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

Example:

```yaml
pipeline:
  thread_num: 4
  processors:
    - type: json_to_arrow
    - type: sql
      query: "SELECT * FROM flow WHERE value >= 10"
    - type: arrow_to_json
```

### Output Components

ArkFlow supports multiple output targets:

- **Kafka**: Write data to Kafka topics
- **MQTT**: Publish messages to MQTT topics
- **HTTP**: Send data via HTTP
- **Standard Output**: Output data to the console

Example:

```yaml
output:
  type: kafka
  brokers:
    - localhost:9092
  topic: output-topic
  client_id: arkflow-producer
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
        - type: arrow_to_json

    output:
      type: kafka
      brokers:
        - localhost:9092
      topic: processed-topic
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
        - type: "arrow_to_json"

    output:
      type: "stdout"
```


## License

ArkFlow is licensed under the [Apache License 2.0](LICENSE).