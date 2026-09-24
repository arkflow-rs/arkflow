---
description: Supported component inventory used by documentation checks.
---

# Component inventory

This page is generated from [`component-inventory.json`](../../reference/component-inventory.json). It is the review point for component coverage, not a replacement for the detailed component pages.

<!-- COMPONENT_INVENTORY_START -->

| Kind | Component | Description | Documentation |
| --- | --- | --- | --- |
| buffer | `memory` | In-memory buffer that releases a batch when it reaches capacity or after a timeout. | [reference](../components/buffers/memory) |
| buffer | `session_window` | Groups messages into sessions based on a maximum gap between messages. Supports SQL joins across sources. | [reference](../components/buffers/session_window) |
| buffer | `sliding_window` | Overlapping time windows that slide forward by a fixed interval. | [reference](../components/buffers/sliding_window) |
| buffer | `tumbling_window` | Fixed-size, non-overlapping time windows. Supports SQL joins across sources. | [reference](../components/buffers/tumbling_window) |
| codec | `debezium_json` | Decodes Debezium CDC Envelope JSON (before/after/op/source/ts_ms) into a columnar Arrow batch; attach to a Kafka input consuming a Debezium topic. CDC offset is the Kafka input's ack-gated offset. | [reference](../components/codecs/debezium) |
| codec | `json` | Encodes/decodes Arrow RecordBatches as JSON byte payloads. | [reference](../components/codecs/json) |
| codec | `protobuf` | Encodes/decodes Arrow RecordBatches using a Protobuf descriptor. | [reference](../components/codecs/protobuf) |
| codec | `schema_registry` | Decodes Confluent wire-format Protobuf and Avro messages by resolving the schema id from a Schema Registry, with an optional subject compatibility gate. | [reference](../components/codecs/schema-registry) |
| input | `file` | Reads records from local or remote object storage (S3, GCS, Azure, HDFS) in CSV/JSON/Parquet/Avro/Arrow formats. | [reference](../components/inputs/file) |
| input | `generate` | Generates synthetic text messages on a fixed interval (useful for testing and load simulation). | [reference](../components/inputs/generate) |
| input | `http` | Receives data via HTTP. Can run as a server (POST/PUT on `path`) or poll a remote endpoint. | [reference](../components/inputs/http) |
| input | `kafka` | Consumes messages from Apache Kafka topics with a consumer group. | [reference](../components/inputs/kafka) |
| input | `memory` | In-memory input queue seeded with an initial list of messages. Primarily for tests and demos. | [reference](../components/inputs/memory) |
| input | `modbus` | Polls Modbus TCP devices on a fixed interval, reading coils, discrete inputs, or registers. | [reference](../components/inputs/modbus) |
| input | `mqtt` | Subscribes to an MQTT broker and forwards messages from the configured topics. | [reference](../components/inputs/mqtt) |
| input | `multiple_inputs` | Combines multiple input sources into a single stream. Each source is tagged with __meta_source. | [reference](../components/inputs/multiple_inputs) |
| input | `nats` | Consumes messages from NATS, supporting both regular subjects and JetStream consumers. | [reference](../components/inputs/nats) |
| input | `pulsar` | Subscribes to an Apache Pulsar topic with configurable subscription type and authentication. | [reference](../components/inputs/pulsar) |
| input | `redis` | Reads from Redis: list blocking pops, pub/sub subscriptions, or stream consumer groups. | [reference](../components/inputs/redis) |
| input | `sql` | Polls a SQL database (MySQL / PostgreSQL / SQLite / DuckDB) with a SELECT statement and emits rows as batches. | [reference](../components/inputs/sql) |
| input | `websocket` | Connects to a WebSocket server and forwards each incoming message as a batch. | [reference](../components/inputs/websocket) |
| output | `drop` | Discards all messages. Useful for performance benchmarks and dead-end pipelines. | [reference](../components/outputs/drop) |
| output | `http` | Posts each batch to an HTTP endpoint. Supports custom headers, retry, and auth. | [reference](../components/outputs/http) |
| output | `influxdb` | Writes time-series data to InfluxDB v2.x using the Line Protocol. | [reference](../components/outputs/influxdb) |
| output | `kafka` | Produces messages to Apache Kafka. Supports key-based partitioning and compression. | [reference](../components/outputs/kafka) |
| output | `milvus` | Upserts batch rows into a Milvus collection over the REST v2 vectordb API: a Float32 list column becomes the vector, other columns pack into a JSON payload field, and an optional id column keys the row. | [reference](../components/outputs/milvus) |
| output | `mongodb` | Writes Arrow rows to MongoDB as BSON documents. | [reference](../components/outputs/mongodb) |
| output | `mqtt` | Publishes messages to an MQTT broker topic. | [reference](../components/outputs/mqtt) |
| output | `nats` | Publishes to NATS, either to a regular subject or a JetStream stream. | [reference](../components/outputs/nats) |
| output | `pgvector` | Upserts batch rows into a PostgreSQL table with the pgvector extension: a Float32 list column becomes the vector, other columns are packed into a jsonb payload, and an optional id column keys ON CONFLICT upserts. | [reference](../components/outputs/pgvector) |
| output | `pulsar` | Produces messages to an Apache Pulsar topic. | [reference](../components/outputs/pulsar) |
| output | `qdrant` | Upserts batch rows as Qdrant points over the REST API: a vector column, an optional id column, and all remaining columns as payload. | [reference](../components/outputs/qdrant) |
| output | `redis` | Writes messages to Redis: streams, lists, or pub/sub channels. | [reference](../components/outputs/redis) |
| output | `sql` | Batch-inserts records into a MySQL or PostgreSQL database, with optional upsert (ON DUPLICATE KEY UPDATE / ON CONFLICT DO UPDATE) for idempotent writes. | [reference](../components/outputs/sql) |
| output | `stdout` | Writes each message to the console. Useful for debugging and demos. | [reference](../components/outputs/stdout) |
| processor | `arrow_to_json` | Converts an Arrow RecordBatch into JSON byte payloads (one per row). | [reference](../components/processors/json) |
| processor | `arrow_to_protobuf` | Serializes Arrow RecordBatches into Protobuf wire-format bytes. | [reference](../components/processors/protobuf) |
| processor | `batch` | Batches messages by count, size, or time interval before forwarding. | [reference](../components/processors/batch) |
| processor | `embedding` | Batch-embeds a text column through an OpenAI-compatible embeddings API and appends the vectors as a FixedSizeList(Float32) column. | [reference](../components/processors/embedding) |
| processor | `json_to_arrow` | Parses JSON byte payloads into an Arrow RecordBatch with inferred schema. | [reference](../components/processors/json) |
| processor | `llm` | Sends each row of a text column to an OpenAI-compatible chat completions API and appends the completion as a Utf8 column, with bounded ordered concurrency. | [reference](../components/processors/llm) |
| processor | `milvus_search` | Searches a Milvus collection for the top-k nearest neighbors of each row's vector with one batched REST v2 request and appends the matches as a JSON array text column. | [reference](../components/processors/milvus-search) |
| processor | `pgvector_search` | Searches a PostgreSQL table with the pgvector extension for the top-k nearest neighbors of each row's vector and appends the matches as a JSON array text column. | [reference](../components/processors/pgvector-search) |
| processor | `protobuf_to_arrow` | Decodes Protobuf wire-format bytes into Arrow RecordBatches. | [reference](../components/processors/protobuf) |
| processor | `python` | Runs a user-defined Python function (with PyArrow) against each batch. | [reference](../components/processors/python) |
| processor | `sql` | Runs a DataFusion SQL query against each batch. Supports window functions and joins against temporary tables. | [reference](../components/processors/sql) |
| processor | `vector_search` | Searches a Qdrant collection for the top-k nearest neighbors of each row's vector and appends the matches as a JSON array text column. | [reference](../components/processors/vector-search) |
| processor | `vrl` | Runs a Vector Remap Language (VRL) program against each batch for safe transformation and enrichment. | [reference](../components/processors/vrl) |
| temporary | `redis` | Redis-backed temporary lookup store (single node or cluster) read through a codec. | [reference](../components/temporary/redis) |

<!-- COMPONENT_INVENTORY_END -->

When a component is added or removed, update the JSON inventory and its detailed page in the same pull request.
