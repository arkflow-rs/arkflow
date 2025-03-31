---
sidebar_position: 1
---

# Introduction

ArkFlow is a high-performance Rust stream processing engine that provides powerful data stream processing capabilities, supporting various input/output sources and processors.

## Core Features

- **High Performance**: Built on Rust and Tokio async runtime, delivering exceptional performance and low latency
- **Multiple Data Sources**: Support for Kafka, MQTT, HTTP, files, and other input/output sources
- **Powerful Processing**: Built-in SQL queries, JSON processing, Protobuf encoding/decoding, batch processing, and other processors
- **Extensibility**: Modular design, easy to extend with new input, output, and processor components

## Installation

### Building from Source

```bash
# Clone repository
git clone https://github.com/ark-flow/arkflow.git
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
    buffer:     # Buffer configuration
    # ... 
```
