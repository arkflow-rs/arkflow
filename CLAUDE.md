# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Building
```bash
cargo build --release          # Build optimized release binary
cargo build                    # Build debug binary
```

The release profile in `Cargo.toml` is optimized for performance:
- `codegen-units = 1`: Better optimization at cost of slower builds
- `lto = true`: Link-time optimization across crates
- `opt-level = 3`: Maximum optimization level

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

### Workspace Dependency Management

The project uses Cargo workspace with centralized dependency management in the root `Cargo.toml`. All workspace members share versions through `[workspace.package]` and dependencies through `[workspace.dependencies]`. When adding dependencies, add them to the workspace section and reference with `workspace = true` in crate `Cargo.toml` files.

**Important**: The project requires Rust 1.88 or later (specified in `rust-version`).

### Workspace Structure

This is a Cargo workspace with three crates:

- **`arkflow-core`** (`crates/arkflow-core/`) - Core engine abstractions and interfaces
  - `Engine`: Main orchestrator managing streams and health checks
  - `Stream`: Complete data processing unit (input → pipeline → output)
  - `Pipeline`: Ordered collection of processors
  - `MessageBatch`: Columnar data using Apache Arrow `RecordBatch`
  - Abstract traits for `Input`, `Output`, `Processor`, `Buffer`, `Codec`

- **`arkflow-plugin`** (`crates/arkflow-plugin/`) - Extensible plugin implementations
  - Input plugins: Generate, File, HTTP, Kafka, Memory, Modbus, MQTT, Multiple Inputs, NATS, Pulsar, Redis, SQL, WebSocket
  - Output plugins: Drop, HTTP, InfluxDB, Kafka, MQTT, NATS, Pulsar, Redis, SQL, Stdout
  - Processor plugins: Batch, JSON, Protobuf, Python UDF, SQL, VRL
  - Buffer plugins: Memory, Session Window, Sliding Window, Tumbling Window, Join
  - Codec plugins: JSON, Protobuf

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
- **Buffer layer**: Handles backpressure (threshold: 1024 messages in channel)

Data flow: `Input → Buffer → [Processor1 → Processor2 → ...] → Output`
Errors are routed to `error_output` if configured.

**Backpressure Mechanism**: When the channel between input and processor contains 1024+ messages, the input worker blocks until space is available, preventing memory overflow from fast inputs/slow processors.

#### Data Model
Uses Apache Arrow's `RecordBatch` for efficient columnar storage. The `MessageBatch` wrapper includes:
- `record_batch`: Arrow RecordBatch
- `input_name`: Optional source identifier

Configuration is YAML-driven and supports dynamic component loading.

#### Metadata System
Inputs can attach metadata to messages using standardized columns (prefixed with `__meta_`):
- `__meta_source`: Source identifier
- `__meta_partition`: Partition number (for partitioned sources like Kafka)
- `__meta_offset`: Offset/position within partition
- `__meta_key`: Message key
- `__meta_timestamp`: Message timestamp from source
- `__meta_ingest_time`: When the message was ingested
- `__meta_ext`: Extended metadata as MapArray for flexible key-value pairs

These metadata columns are accessible in SQL queries within processors.

#### Actor-like Concurrency
Each stream is an independent concurrent task using:
- `Tokio` async runtime with multi-threaded executor
- `CancellationToken` for graceful shutdown coordination
- `flume` channels for message passing between stages
- `TaskTracker` for managing concurrent tasks

#### Ordered Output Delivery
The output worker ensures ordered delivery using:
- **Sequence numbers**: Each message batch gets an incrementing sequence number
- **Blocking queue**: Output worker waits for out-of-order batches before writing
- Atomic counters track the next expected sequence, preventing out-of-order writes to sinks

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

To run tests:
```bash
cargo test -p arkflow-plugin                    # Test all plugin components
cargo test -p arkflow-plugin test_name          # Run specific test
cargo test --package arkflow-core               # Test core engine
cargo test --package arkflow-plugin --lib       # Test plugin library only
cargo test --workspace                          # Test entire workspace
```

### Adding New Components

All component types (input, output, processor, buffer, codec) follow the same registration pattern defined in `arkflow-core`.

**Component Initialization Order** (in `crates/arkflow/src/main.rs`):
1. `input::init()` - Register all input builders
2. `output::init()` - Register all output builders
3. `processor::init()` - Register all processor builders
4. `buffer::init()` - Register all buffer builders
5. `temporary::init()` - Register temporary storage components
6. `codec::init()` - Register codec builders

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
- **SQLx**: Database connectivity (MySQL, PostgreSQL, SQLite)
- **Protobuf**: Schema evolution support
- **PyO3**: Python UDF support for custom processors

### Plugin Development

When creating new plugins, the registration pattern is consistent across all component types. All registration functions use `lazy_static` with `RwLock<HashMap>` for thread-safe dynamic component lookup by name.

**Python UDFs**: The Python processor plugin uses PyO3 to allow users to write custom processors in Python. These are loaded dynamically at runtime and can access `MessageBatch` data directly.

**VRL Processor**: Uses Vector Remap Language (VRL) for powerful data transformation and enrichment. VRL is a safe, fast expression language designed specifically for observability data pipelines. See https://vector.dev/docs/reference/vrl/ for syntax reference.

## Available Components

### Input Components

1. **Generate** - Generates synthetic data with configurable intervals, counts, and batch sizes
   - Use case: Testing, data simulation
   - Configurable data generation patterns

2. **File** - Reads from various file formats with cloud object store support
   - Supported formats: JSON, CSV, Parquet, Arrow, Avro
   - Cloud storage: S3, GCS, Azure, HTTP, HDFS
   - SQL queries on file data with DataFusion
   - Remote Ballista cluster support

3. **HTTP** - HTTP server/client for REST APIs
   - Can act as server receiving webhook data
   - Can poll from HTTP endpoints

4. **Kafka** - Apache Kafka consumer
   - Consumer group management
   - Partition assignment
   - Offset management with `start_from_latest` option
   - Configurable fetch sizes and wait times

5. **Memory** - In-memory message source
   - Use case: Testing, development
   - Programmable data injection

6. **Modbus** - Industrial protocol support
   - TCP and RTU support
   - Real-time sensor data collection
   - Register polling

7. **MQTT** - MQTT broker client
   - QoS levels (0, 1, 2)
   - Clean session configuration
   - Keep-alive management
   - Topic subscription with wildcards

8. **Multiple Inputs** - Combines multiple input streams
   - Unifies multiple sources into single pipeline
   - Automatic source identification via `__meta_source`
   - Independent connection management per source

9. **NATS** - NATS streaming protocol
   - JetStream support
   - Queue groups
   - Durable subscriptions

10. **Pulsar** - Apache Pulsar consumer
    - Topic subscription patterns
    - Consumer configuration
    - Message acknowledgment

11. **Redis** - Redis data structures
    - Streams: Consumer group support
    - Lists: Blocking and non-blocking reads
    - Pub/Sub: Channel subscriptions

12. **SQL** - SQL database queries
    - Connection pooling
    - Supported databases: MySQL, PostgreSQL, SQLite
    - Incremental query execution
    - Configurable polling intervals

13. **WebSocket** - Real-time communication
    - Server and client modes
    - Message handling
    - Connection management

### Output Components

1. **Drop** - Discards messages
   - Use case: Testing, performance benchmarking

2. **HTTP** - HTTP client for REST APIs
   - POST/PUT methods
   - Custom headers
   - Retry logic

3. **InfluxDB** - InfluxDB 2.x time-series database
   - Line Protocol generation with automatic escaping
   - Configurable tag and field mappings
   - Batching with configurable batch size and flush intervals
   - Retry mechanism with exponential backoff
   - Support for Float, Integer, Boolean, String types

4. **Kafka** - Apache Kafka producer
   - Automatic partitioning
   - Key-based partitioning
   - Compression options
   - Acknowledgment levels

5. **MQTT** - MQTT publisher
   - QoS levels
   - Retained messages
   - Topic publishing

6. **NATS** - NATS streaming publisher
   - JetStream publishing
   - Reply-to support

7. **Pulsar** - Apache Pulsar producer
   - Topic configuration
   - Message batching

8. **Redis** - Redis data structures
   - Streams: Producer with optional consumer group
   - Lists: LPUSH/RPUSH operations
   - Pub/Sub: Channel publishing

9. **SQL** - SQL database writes
   - Batch inserts for performance
   - UPSERT support
   - Connection pooling
   - Transaction management

10. **Stdout** - Console output
    - Use case: Debugging, development
    - Formatted output

### Processor Components

1. **Batch** - Message batching and windowing
   - Count-based batching
   - Time-based batching
   - Size-based batching

2. **JSON** - JSON processing
   - Parsing and validation
   - Transformation
   - Field extraction and manipulation

3. **Protobuf** - Protocol Buffers codec
   - Schema-based serialization/deserialization
   - Schema evolution support
   - Message descriptor configuration

4. **Python UDF** - Python user-defined functions
   - Direct PyArrow integration
   - Full RecordBatch manipulation
   - Custom data transformations
   - Python package support

5. **SQL** - SQL processing with DataFusion
   - Complex queries and joins
   - Aggregate functions
   - Window functions
   - UDF support
   - Temporary table integration

6. **VRL** - Vector Remap Language
   - Powerful data transformation
   - Enrichment and filtering
   - Safe execution environment

### Buffer Components

1. **Memory** - Simple in-memory buffer
   - Fast buffering
   - Configurable capacity

2. **Tumbling Window** - Fixed-size, non-overlapping windows
   - Time-based windowing
   - Configurable window size
   - Millisecond precision

3. **Sliding Window** - Overlapping time windows
   - Configurable window size and slide interval
   - Continuous data processing
   - Event aggregation

4. **Session Window** - Dynamic windows based on activity gaps
   - Configurable gap duration
   - Session timeout handling
   - Use case: User activity tracking

5. **Join** - Multi-source join operations
   - SQL joins across input sources
   - Configurable join queries
   - Codec support for data transformation
   - Correlation of data from different streams

### Codec Components

1. **JSON** - JSON serialization/deserialization
   - Schema inference
   - Pretty print options
   - Field mapping

2. **Protobuf** - Protocol Buffers codec
   - Binary format support
   - Schema files (.proto)
   - Descriptor sets

## Advanced Features

### Multi-Source Joins

Combine data from multiple sources using buffer joins:

```yaml
streams:
  - input:
      type: "kafka"
      # ... kafka config
    pipeline:
      processors:
        - type: "sql"
          query: "SELECT * FROM flow"
    buffer:
      type: "session_window"
      gap: 1s
      join:
        query: "SELECT * FROM flow_input1 JOIN flow_input2 ON (flow_input1.id = flow_input2.id)"
        codec:
          type: "json"
    output:
      type: "http"
      # ... output config
```

### Cloud Object Storage

File input supports multiple cloud providers:

```yaml
input:
  type: "file"
  path: 's3://bucket/data.parquet'
  format:
    type: "parquet"
  object_store:
    type: "s3"
    endpoint: "http://localhost:9000"
    region: "us-east-1"
    bucket_name: "my-bucket"
    access_key_id: "${AWS_ACCESS_KEY_ID}"
    secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
    allow_http: true
```

Supported object stores:
- **S3**: AWS S3 and compatible (MinIO, etc.)
- **GCS**: Google Cloud Storage
- **Azure**: Azure Blob Storage
- **HTTP**: Public HTTP endpoints
- **HDFS**: Hadoop Distributed File System

### Python UDF Processors

Execute custom Python code:

```yaml
pipeline:
  processors:
    - type: "python"
      script: |
        def transform_data(batch):
            import pyarrow as pa
            import pyarrow.compute as pc

            # Access data
            values = batch.column("value").to_pylist()

            # Transform data
            transformed = [x * 2 for x in values]

            # Create new column
            new_array = pa.array(transformed)
            new_batch = batch.add_column(
                batch.num_columns,
                "doubled_value",
                new_array
            )

            return new_batch
      function: "transform_data"
```

### Metadata Enrichment

All inputs automatically attach metadata fields:

```yaml
pipeline:
  processors:
    - type: "sql"
      query: |
        SELECT
          *,
          __meta_source as source,
          __meta_partition as partition,
          __meta_offset as offset,
          __meta_timestamp as message_time
        FROM flow
```

### Temporary Storage for Joins

Use temporary storage for complex join operations:

```yaml
pipeline:
  processors:
    - type: "sql"
      query: |
        SELECT
          a.*,
          b.enriched_field
        FROM flow a
        JOIN temp_reference b ON a.id = b.id
      temporary_list:
        - name: "reference_data"
          table_name: "temp_reference"
          key:
            value: "id"
```

### InfluxDB Time-Series Output

```yaml
output:
  type: "influxdb"
  url: "http://localhost:8086"
  org: "my-org"
  bucket: "sensor-data"
  token: "${INFLUXDB_TOKEN}"
  measurement: "sensor_readings"
  tags:
    - name: "location"
      value: "sensor_location"
    - name: "type"
      value: "sensor_type"
  fields:
    - name: "temperature"
      value_type: "float"
    - name: "humidity"
      value_type: "float"
    - name: "status"
      value_type: "boolean"
    - name: "count"
      value_type: "integer"
    - name: "message"
      value_type: "string"
  batch_size: 1000
  flush_interval: 5s
```

### Windowing Strategies

**Tumbling Window** (fixed, non-overlapping):
```yaml
buffer:
  type: "tumbling_window"
  size: 1m
```

**Sliding Window** (overlapping):
```yaml
buffer:
  type: "sliding_window"
  size: 5m
  slide: 1m
```

**Session Window** (dynamic, activity-based):
```yaml
buffer:
  type: "session_window"
  gap: 30s
```

## UDF Framework

The system supports custom User-Defined Functions in SQL processors:

- **Scalar Functions**: Transform single values
- **Aggregate Functions**: Aggregate multiple values
- **Window Functions**: Operate on window frames
- Thread-safe registration via `register_udf()`
- Integration with DataFusion's UDF system

Example:
```rust
use arkflow_core::udf::register_udf;
use datafusion::arrow::array::{Int64Array, Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{create_udf, Volatility};

// Register custom function
register_udf(create_udf(
    "custom_transform",
    vec![DataType::Int64],
    Arc::new(DataType::Int64),
    Volatility::Immutable,
    Arc::new(|args| {
        // Custom logic here
    }),
));
```
