## MODIFIED Requirements

### Requirement: The workspace SHALL define a connector feature layer with default-all compatibility
The workspace SHALL define Cargo features `connectors-sql`, `connectors-kafka`, `connectors-python`, `connectors-mongodb`, `connectors-pulsar`, `connectors-hdfs`, `connectors-mqtt`, `connectors-nats`, `connectors-amqp09`, `connectors-iceberg`, `connectors-redis`, `connectors-websocket`, `connectors-modbus`, `ai-vector`, `processors-vrl`, `codecs-protobuf`, and `engine-minimal` on `arkflow-plugin`, mirrored by identical forwarding features on the `arkflow` binary and `arkflow-server` crates. The default feature set SHALL enable every gated family, so a default build registers every component of every family — no component is dropped by gating, and a component added under a family after the feature layer was introduced (such as the `iceberg` output under `connectors-iceberg`) is registered by default. Each feature in `Cargo.toml` SHALL carry a comment naming the dependencies and components it covers.

#### Scenario: Default build is behaviorally unchanged
- **WHEN** the workspace is built with default features and `arkflow components list` runs
- **THEN** the registered component set equals the pre-feature-layer inventory plus every component added since under a default-on family

#### Scenario: Features are documented at the definition site
- **WHEN** a contributor reads the `[features]` section of any of the three crates
- **THEN** every feature's comment names the dependency families and components it gates

#### Scenario: Iceberg family is registered by default
- **WHEN** the workspace is built with default features and `arkflow components list` runs
- **THEN** the `iceberg` output appears in the listing

### Requirement: A minimal build SHALL compile and register only the ungated component families
Building with `--no-default-features --features engine-minimal` SHALL succeed for the whole workspace, and the resulting binary SHALL NOT register any component gated behind the family features (`connectors-sql`, `connectors-kafka`, `connectors-python`, `connectors-mongodb`, `connectors-pulsar`, `connectors-hdfs`, `connectors-mqtt`, `connectors-nats`, `connectors-amqp09`, `connectors-iceberg`, `connectors-redis`, `connectors-websocket`, `connectors-modbus`, `ai-vector`, `processors-vrl`, `codecs-protobuf`). Building `--no-default-features --features engine-minimal,connectors-kafka` SHALL compile and register the kafka family in addition to the minimal set.

#### Scenario: Minimal components list excludes gated families
- **WHEN** the binary is built with `--no-default-features --features engine-minimal` and `components list` runs
- **THEN** no type from the gated families appears in the listing (e.g. `kafka`, `sql`, `mqtt`, `nats`, `redis`, `websocket`, `modbus`, `pulsar` connectors; `python`/`vrl` processors; `protobuf`/`schema_registry` codecs; `qdrant`/`milvus`/`pgvector`/`iceberg` outputs), while the ungated core set (file/generate/http/memory inputs, stdout/drop/http outputs, lookup/batch/arrow processors, json codecs, window buffers) remains registered

#### Scenario: Adding one family extends the minimal set
- **WHEN** the workspace is built with `--no-default-features --features engine-minimal,connectors-kafka`
- **THEN** the build succeeds and the kafka input/output types appear in `components list`

#### Scenario: Minimal dependency tree excludes gated families
- **WHEN** `cargo tree` is resolved for the minimal feature set
- **THEN** none of `rdkafka`, `pyo3`, `mongodb`, `pulsar`, `hdfs-native-object-store`, `ballista`, the duckdb fork, `rumqttc`, `async-nats`, `redis`, `tokio-tungstenite`, `tokio-modbus`, `vrl`, `prost-reflect`, `protox`, `sqlx`, `iceberg`, `iceberg-catalog-rest`, `iceberg-storage-opendal`, `opendal`, or `reqsign` appears in the graph
