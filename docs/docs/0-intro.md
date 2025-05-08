---
sidebar_position: 1
logo: ./logo.svg
---

# Introduction

ArkFlow is a high-performance Rust stream processing engine that provides powerful data stream processing capabilities, supporting various input/output sources and processors.

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
```

### Output Components

ArkFlow supports multiple output targets:

- **Kafka**: Write data to Kafka topics
- **MQTT**: Publish messages to MQTT topics
- **HTTP**: Send data via HTTP
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

- **Memory Buffer**: Memory buffer, for high-throughput scenarios and window aggregation

Example:

```yaml
buffer:
  type: memory
  capacity: 10000  # Maximum number of messages to buffer
  timeout: 10s  # Maximum time to buffer messages
```
