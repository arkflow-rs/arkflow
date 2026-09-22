# ArkFlow

<p align="center">
<img align="center" width="150px" src="images/logo.svg">
<p align="center">

English | [中文](README_zh.md)

[![Rust](https://github.com/arkflow-rs/arkflow/actions/workflows/rust.yml/badge.svg)](https://github.com/arkflow-rs/arkflow/actions/workflows/rust.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

[Docs](https://arkflow-rs.com/docs/) | [0.5 release docs](https://arkflow-rs.com/docs/0.5.x/intro)

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

<!-- README_COMPONENTS:input START -->

- **Kafka** (`kafka`): Read data from Kafka topics
- **MQTT** (`mqtt`): Subscribe to messages from MQTT topics
- **HTTP** (`http`): Receive data via HTTP
- **File** (`file`): Reading data from files (CSV, JSON, Parquet, Avro, Arrow) with cloud storage support
- **Generate** (`generate`): Generate synthetic test data
- **SQL** (`sql`): Query data from SQL databases (MySQL, PostgreSQL, SQLite)
- **NATS** (`nats`): Subscribe to messages from NATS topics with JetStream support
- **Pulsar** (`pulsar`): Subscribe to messages from Pulsar topics
- **Redis** (`redis`): Subscribe to messages from Redis streams, lists, or pub/sub channels
- **WebSocket** (`websocket`): Subscribe to messages from WebSocket connections
- **Modbus** (`modbus`): Read data from Modbus devices
- **Memory** (`memory`): In-memory data source for testing
- **Multiple Inputs** (`multiple_inputs`): Combine multiple input streams into one pipeline

<!-- README_COMPONENTS:input END -->

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

<!-- README_COMPONENTS:processor START -->

- **JSON** (`json_to_arrow` / `arrow_to_json`): JSON data processing and transformation
- **SQL** (`sql`): Process data using SQL queries
- **Protobuf** (`arrow_to_protobuf` / `protobuf_to_arrow`): Protobuf encoding/decoding
- **Batch Processing** (`batch`): Process messages in batches
- **VRL** (`vrl`): Process data using [VRL](https://vector.dev/docs/reference/vrl/)
- **Python** (`python`): Run Python user-defined functions over the batch
- **Embedding** (`embedding`): Batch-embed a text column via an OpenAI-compatible API and append vector columns
- **LLM** (`llm`): Send each row to an OpenAI-compatible chat completions API with bounded concurrency and append completions
- **Vector Search** (`vector_search`): Search a Qdrant collection for top-k nearest neighbors of each row's vector and append matches as JSON
- **Milvus Search** (`milvus_search`): Search a Milvus collection for top-k nearest neighbors of each row's vector with one batched request
- **pgvector Search** (`pgvector_search`): Search a PostgreSQL table with the pgvector extension for top-k nearest neighbors of each row's vector

<!-- README_COMPONENTS:processor END -->

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

<!-- README_COMPONENTS:output START -->

- **Kafka** (`kafka`): Write data to Kafka topics
- **MQTT** (`mqtt`): Publish messages to MQTT topics
- **HTTP** (`http`): Send data via HTTP
- **InfluxDB** (`influxdb`): Write time-series data to InfluxDB 2.x
- **MongoDB** (`mongodb`): Write documents to a MongoDB collection
- **NATS** (`nats`): Publish messages to NATS topics
- **Pulsar** (`pulsar`): Publish messages to Pulsar topics
- **Redis** (`redis`): Write to Redis streams, lists, or pub/sub channels
- **SQL** (`sql`): Write to SQL databases (MySQL, PostgreSQL) with batch inserts and UPSERT
- **Standard Output** (`stdout`): Output data to the console
- **Drop** (`drop`): Discard data
- **Qdrant** (`qdrant`): Upsert vectors and payloads into a Qdrant collection
- **pgvector** (`pgvector`): Upsert vectors and JSON payloads into a PostgreSQL table with the pgvector extension
- **Milvus** (`milvus`): Upsert vectors and JSON payloads into a Milvus collection via the REST v2 API

<!-- README_COMPONENTS:output END -->

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

<!-- README_COMPONENTS:buffer START -->

- **Memory Buffer** (`memory`): Memory buffer, for high-throughput scenarios and window aggregation. Compiled Streams treat it
  as a no-op pass-through; its capacity/timeout options are ignored.
- **Session Window** (`session_window`): The Session Window buffer component provides a session-based message grouping mechanism where
  messages are grouped based on activity gaps. It implements a session window that closes after a configurable period of
  inactivity. Compiled into a processing-time window operator.
- **Sliding Window (deprecated)** (`sliding_window`): Deprecated and unreachable in Stream configs — the stream compiler rejects the
  sliding window buffer. Use an event-time sliding window operator in a Job DAG instead.
- **Tumbling Window** (`tumbling_window`): The Tumbling Window buffer component provides a fixed-size, non-overlapping windowing mechanism
  for processing message batches. It implements a tumbling window algorithm with configurable interval settings.
  Compiled into a processing-time window operator.

<!-- README_COMPONENTS:buffer END -->

Example:

```yaml
buffer:
  type: memory
```

The legacy join buffer is also deprecated and unreachable in Stream configs — the stream
compiler rejects it with a migration message pointing to Job DAG configuration; it is not
a standalone buffer type.

### Codec Components

Codecs attach to inputs and outputs through the `codec` configuration to encode and decode
message payloads:

<!-- README_COMPONENTS:codec START -->

- **JSON** (`json`): Encode/decode Arrow RecordBatches as JSON byte payloads
- **Protobuf** (`protobuf`): Encode/decode Arrow RecordBatches using a Protobuf descriptor
- **Debezium JSON** (`debezium_json`): Decode Debezium CDC envelope JSON from Kafka change-event topics
- **Schema Registry** (`schema_registry`): Decode Confluent wire-format Protobuf and Avro messages by resolving schemas from a Schema Registry

<!-- README_COMPONENTS:codec END -->

### Temporary Components

Temporary components provide external lookup state that processors can query at runtime:

<!-- README_COMPONENTS:temporary START -->

- **Redis** (`redis`): Redis-backed temporary lookup store (single node or cluster) read through a codec

<!-- README_COMPONENTS:temporary END -->

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
