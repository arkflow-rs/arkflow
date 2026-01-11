# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Building
```bash
cargo build --release          # Build optimized release binary
cargo build                    # Build debug binary
```

### Testing
```bash
cargo test                     # Run all tests
cargo test --verbose           # Run tests with verbose output
cargo test --package <name>    # Run tests for a specific package
```

### Running
```bash
./target/release/arkflow --config config.yaml          # Run with config
./target/release/arkflow --config config.yaml --validate  # Validate config only
```

The binary supports configuration validation via the `--validate` flag.

### CI Requirements
The CI pipeline requires protobuf compiler:
```bash
sudo apt-get install protobuf-compiler  # Linux
export PROTOC=$(which protoc)
```

## Project Architecture

ArkFlow is a high-performance Rust stream processing engine built on Tokio with a plugin-based architecture.

### Workspace Structure

This is a Cargo workspace with three crates:

- **`arkflow-core`** (`crates/arkflow-core/`) - Core engine abstractions and interfaces
  - `Engine`: Main orchestrator managing streams and health checks
  - `Stream`: Complete data processing unit (input → pipeline → output)
  - `Pipeline`: Ordered collection of processors
  - `MessageBatch`: Columnar data using Apache Arrow `RecordBatch`
  - Abstract traits for `Input`, `Output`, `Processor`, `Buffer`, `Codec`

- **`arkflow-plugin`** (`crates/arkflow-plugin/`) - Extensible plugin implementations
  - Input plugins: Kafka, MQTT, HTTP, File, Database, NATS, Redis, WebSocket, Modbus, Generate
  - Output plugins: Kafka, MQTT, HTTP, Stdout, Drop, NATS, SQL
  - Processor plugins: JSON, SQL, Protobuf, Batch, VRL, Python UDF
  - Buffer plugins: Memory, Session Window, Sliding Window, Tumbling Window, Join
  - Codec plugins: JSON, Arrow, Protobuf

- **`arkflow`** (`crates/arkflow/`) - Main binary executable

### Key Architectural Patterns

#### Plugin Registration System
Uses `lazy_static` with `RwLock<HashMap>` for dynamic component registration. Each plugin implements a builder trait and registers itself via `register_*_builder()` functions. All plugins are initialized through `*_init()` functions (e.g., `input::init()`, `processor::init()`).

When adding a new plugin:
1. Implement the appropriate builder trait (`InputBuilder`, `ProcessorBuilder`, etc.)
2. Create an `init()` function that calls `register_*_builder()`
3. Call the plugin's `init()` from the module's `init()` function

#### Stream Processing Flow
Each `Stream` runs concurrently with:
- **Input worker**: Reads data from source
- **Processor workers**: Multiple threads (configurable via `thread_num`) process batches
- **Output worker**: Writes to sink with ordered delivery using sequence numbers
- **Buffer layer**: Handles backpressure (threshold: 1024 messages)

Data flow: `Input → Buffer → [Processor1 → Processor2 → ...] → Output`
Errors are routed to `error_output` if configured.

#### Data Model
Uses Apache Arrow's `RecordBatch` for efficient columnar storage. The `MessageBatch` wrapper includes:
- `record_batch`: Arrow RecordBatch
- `input_name`: Optional source identifier

Configuration is YAML-driven and supports dynamic component loading.

#### Actor-like Concurrency
Each stream is an independent concurrent task using:
- `Tokio` async runtime with multi-threaded executor
- `CancellationToken` for graceful shutdown coordination
- `flume` channels for message passing between stages
- `TaskTracker` for managing concurrent tasks

### Configuration System

Configuration is hierarchical YAML with the following structure:
```yaml
logging:
  level: info  # debug, info, warn, error
streams:
  - input:      # Data source configuration
    pipeline:   # Processing configuration
      thread_num: 4  # Number of processor worker threads
      processors: []  # Ordered processor chain
    output:     # Data sink configuration
    error_output: # Optional error routing
    buffer:     # Optional backpressure handling
```

Example configurations are in `examples/` directory demonstrating all component types.

### Health Check System

The Engine runs an HTTP health check server (default `http://0.0.0.0:8080`) with three endpoints:
- `/health` - Overall health status
- `/readiness` - Ready to process requests
- `/liveness` - Process is alive

These are used for Kubernetes/cloud-native deployments.

### Trait-Based Extensions

All core components are trait-based:
- `Input`/`InputBuilder`: Data sources with async `connect()` and `read()` methods
- `Output`/`OutputBuilder`: Data sinks with async `connect()` and `write()` methods
- `Processor`/`ProcessorBuilder`: Data transformations
- `Buffer`: Backpressure and windowing strategies
- `Codec`: Serialization/deserialization

Traits use `async-trait` for async methods and return `Result<(), Error>` for error handling.

### Error Handling

Uses `thiserror` for structured error types and `anyhow` for context. Errors are propagated through the pipeline and can be routed to `error_output` if configured.

### Testing Patterns

Integration tests are in `tests/` directories within crates. Uses `mockall` for mocking dependencies. Example configurations in `examples/` serve as integration test fixtures.

To run tests for a specific component:
```bash
cargo test -p arkflow-plugin test_name
```

### Adding New Components

**New Input:**
1. Create struct implementing `Input` trait
2. Create builder struct implementing `InputBuilder` trait
3. Register via `register_input_builder()` in an `init()` function
4. Call `init()` from `input::init()` in `crates/arkflow-plugin/src/input/mod.rs`

**New Processor:**
1. Create struct implementing `Processor` trait
2. Create builder struct implementing `ProcessorBuilder` trait
3. Register via `register_processor_builder()` in an `init()` function
4. Call `init()` from `processor::init()` in `crates/arkflow-plugin/src/processor/mod.rs`

Similar patterns apply for outputs, buffers, and codecs.

### Key Dependencies

- **Tokio**: Async runtime (features: full)
- **Arrow/DataFusion**: Columnar data and SQL processing
- **Flume**: Async channels (version pinned to 0.11)
- **Axum**: HTTP server for health checks
- **Serde**: Serialization framework
- **Tracing**: Structured logging and instrumentation
- **SQLx**: Database connectivity (MySQL, PostgreSQL)
- **Protobuf**: Schema evolution support
