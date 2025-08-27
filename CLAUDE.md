# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ArkFlow is a high-performance Rust stream processing engine that supports real-time data processing with AI integration capabilities. It processes data through configurable streams with inputs, pipelines, and outputs.

## Build and Development Commands

### Common Commands
```bash
# Build all crates
cargo build --release

# Run tests
cargo test

# Run tests for a specific crate
cargo test -p arkflow-core
cargo test -p arkflow-plugin

# Run the main binary
cargo run --bin arkflow -- --config config.yaml

# Format code
cargo fmt

# Check code
cargo clippy

# Generate documentation
cargo doc --no-deps
```

### Running Examples
```bash
# Run with configuration file
./target/release/arkflow --config examples/generate_example.yaml

# Run with multiple streams
./target/release/arkflow --config examples/kafka_example.yaml
```

## Architecture

### Core Components

1. **arkflow-core** (`crates/arkflow-core/`): Core stream processing engine
   - `lib.rs`: Main types (MessageBatch, Error, Resource)
   - `stream/mod.rs`: Stream orchestration with input/pipeline/output
   - `config.rs`: Configuration management (YAML/JSON/TOML)
   - `input/`, `output/`, `processor/`, `buffer/`: Component traits

2. **arkflow-plugin** (`crates/arkflow-plugin/`): Plugin implementations
   - `input/`: Kafka, MQTT, HTTP, file, database, etc.
   - `output/`: Kafka, MQTT, HTTP, stdout, etc.
   - `processor/`: SQL, JSON, Protobuf, Python, VRL, etc.
   - `buffer/`: Memory, session/sliding/tumbling windows

3. **arkflow** (`crates/arkflow/`): Binary entry point
   - CLI interface and main execution logic

### Data Flow

```
Input → Buffer → Pipeline (Processors) → Output
                ↓
            Error Output
```

- **MessageBatch**: Core data structure wrapping Arrow RecordBatch
- **Stream**: Orchestrates components with backpressure handling
- **Pipeline**: Chain of processors for data transformation
- **Buffer**: Optional buffering with windowing support

## Configuration

ArkFlow uses YAML/JSON/TOML configuration:

```yaml
logging:
  level: info
streams:
  - input:
      type: kafka
      brokers: [localhost:9092]
      topics: [test-topic]
    pipeline:
      thread_num: 4
      processors:
        - type: sql
          query: "SELECT * FROM flow WHERE value > 100"
    output:
      type: stdout
    error_output:
      type: kafka
      topic: error-topic
```

## Key Concepts

### MessageBatch
- Wraps Arrow RecordBatch for columnar processing
- Supports binary data with default field `__value__`
- Tracks input source for multi-stream scenarios

### Stream Processing
- Async processing with Tokio runtime
- Backpressure control (threshold: 1024 messages)
- Ordered delivery with sequence numbers
- Graceful shutdown with cancellation tokens

### Component Traits
All components implement async traits:
- `Input`: `read()`, `connect()`, `close()`
- `Output`: `write()`, `connect()`, `close()`
- `Processor`: `process()` → `Vec<MessageBatch>`
- `Buffer`: `read()`, `write()`, `flush()`

## Development Guidelines

### Adding New Components
1. Implement component trait in appropriate crate
2. Add configuration struct
3. Register in component registry
4. Add tests and examples

### Error Handling
- Use `arkflow_core::Error` enum
- Handle connection errors with reconnection logic
- Use `Error::EOF` for graceful shutdown

### Testing
- Unit tests in `tests/` directories
- Integration tests with real components
- Use mockall for mocking dependencies

## Dependencies

Key dependencies:
- **Tokio**: Async runtime
- **Arrow/DataFusion**: Columnar data processing
- **Serde**: Serialization
- **Tracing**: Structured logging
- **Flume**: Async channels
- **SQLx**: Database connectivity