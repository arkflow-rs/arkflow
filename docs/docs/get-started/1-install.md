---
sidebar_position: 1
---

# Install

## Prerequisites

- **Rust ≥ 1.97** (toolchain, for building from source)

No protobuf compiler is needed: Protobuf codec `.proto` files are compiled at
runtime by the bundled pure-Rust `protox` compiler, and the build does not
shell out to `protoc` (there is no `build.rs`).

## Build from source

```bash
git clone https://github.com/arkflow-rs/arkflow.git
cd arkflow

# Optimized release build
cargo build --release

# (optional) run the test suite
cargo test
```

The binary is at `./target/release/arkflow`.

## Build profiles (feature gating)

Connector families are gated behind cargo features. A default build enables
every family; a minimal build compiles only the core engine plus the light
component set — useful for custom images and edge deployments.

| Feature | Components |
| --- | --- |
| `connectors-sql` | `sql` input/output (DuckDB, Ballista, MySQL/PostgreSQL/SQLite table providers) |
| `connectors-kafka` | `kafka` input/output |
| `connectors-python` | `python` processor |
| `connectors-mongodb` | `mongodb` output |
| `connectors-pulsar` | `pulsar` input/output |
| `connectors-hdfs` | `hdfs` object store of the `file` input |
| `connectors-mqtt` | `mqtt` input/output |
| `connectors-nats` | `nats` input/output |
| `connectors-amqp09` | `amqp09` (RabbitMQ) input/output |
| `connectors-redis` | `redis` input/output/temporary |
| `connectors-websocket` | `websocket` input |
| `connectors-modbus` | `modbus` input |
| `ai-vector` | `embedding` / `llm` / `vector_search` / `milvus_search` / `pgvector_search` processors, `qdrant` / `milvus` / `pgvector` outputs |
| `processors-vrl` | `vrl` processor |
| `codecs-protobuf` | `protobuf` / `schema_registry` codecs, `arrow_to_protobuf` / `protobuf_to_arrow` processors |

```bash
# Default build: every family enabled (identical behavior to a build
# without the feature layer)
cargo build --release

# Minimal build: core engine + light components only.
# Cargo features are additive, so engine-minimal removes anything only
# together with --no-default-features.
cargo build --release --no-default-features --features engine-minimal

# Minimal build plus one family
cargo build --release --no-default-features --features engine-minimal,connectors-kafka
```

Always compiled regardless of features: `file` / `generate` / `http` /
`memory` / `multiple_inputs` inputs, `drop` / `http` / `influxdb` / `stdout`
outputs, `lookup` / `batch` / `arrow_to_json` / `json_to_arrow` / `sql`
processors, every buffer kind, and the `json` / `debezium_json` codecs. In a
build without a family, a config referencing its types fails validation and
names the missing feature, e.g. `Unknown input type: kafka (requires the
'connectors-kafka' feature)`.

## Run

```bash
# Run with a config file
./target/release/arkflow --config config.yaml

# Validate a config without starting the engine
./target/release/arkflow --config config.yaml --validate
```

## Component discovery & schema

ArkFlow exposes CLI commands that list every registered component and print its
configuration schema. These power editor auto-completion and are handy when
writing a config.

```bash
# List every input / output / processor / buffer / codec
./target/release/arkflow components list
./target/release/arkflow components list --kind input

# Print the config schema for one component (text or JSON)
./target/release/arkflow components show input kafka
./target/release/arkflow components show processor sql --format json

# Emit a complete JSON Schema for the whole engine config
./target/release/arkflow schema > arkflow.schema.json
```

Point your editor's YAML language server at the generated schema for
field-level completion and validation.

Continue to [Quickstart](./2-quickstart.md).
