## Purpose

Define Cargo feature gating across the workspace so minimal builds exclude heavy connector families while default builds remain unchanged.

## Requirements

### Requirement: The workspace SHALL define a connector feature layer with default-all compatibility
The workspace SHALL define Cargo features `connectors-sql`, `connectors-kafka`, `connectors-python`, `connectors-mongodb`, `connectors-pulsar`, `connectors-hdfs`, `connectors-mqtt`, `connectors-nats`, `connectors-redis`, `connectors-websocket`, `connectors-modbus`, `ai-vector`, `processors-vrl`, `codecs-protobuf`, and `engine-minimal` on `arkflow-plugin`, mirrored by identical forwarding features on the `arkflow` binary and `arkflow-server` crates. The default feature set SHALL enable every gated family, so a default build registers exactly the same components as before the feature layer. Each feature in `Cargo.toml` SHALL carry a comment naming the dependencies and components it covers.

#### Scenario: Default build is behaviorally unchanged
- **WHEN** the workspace is built with default features and `arkflow components list` runs
- **THEN** the registered component set is identical to the pre-feature-layer inventory

#### Scenario: Features are documented at the definition site
- **WHEN** a contributor reads the `[features]` section of any of the three crates
- **THEN** every feature's comment names the dependency families and components it gates

### Requirement: A minimal build SHALL compile and register only the ungated component families
Building with `--no-default-features --features engine-minimal` SHALL succeed for the whole workspace, and the resulting binary SHALL NOT register any component gated behind the family features (`connectors-sql`, `connectors-kafka`, `connectors-python`, `connectors-mongodb`, `connectors-pulsar`, `connectors-hdfs`, `connectors-mqtt`, `connectors-nats`, `connectors-redis`, `connectors-websocket`, `connectors-modbus`, `ai-vector`, `processors-vrl`, `codecs-protobuf`). Building `--no-default-features --features engine-minimal,connectors-kafka` SHALL compile and register the kafka family in addition to the minimal set.

#### Scenario: Minimal components list excludes gated families
- **WHEN** the binary is built with `--no-default-features --features engine-minimal` and `components list` runs
- **THEN** no type from the gated families appears in the listing (e.g. `kafka`, `sql`, `mqtt`, `nats`, `redis`, `websocket`, `modbus`, `pulsar` connectors; `python`/`vrl` processors; `protobuf`/`schema_registry` codecs; `qdrant`/`milvus`/`pgvector` outputs), while the ungated core set (file/generate/http/memory inputs, stdout/drop/http outputs, lookup/batch/arrow processors, json codecs, window buffers) remains registered

#### Scenario: Adding one family extends the minimal set
- **WHEN** the binary is built with `--no-default-features --features engine-minimal,connectors-kafka`
- **THEN** the build succeeds and the kafka input/output types appear in `components list`

#### Scenario: Minimal dependency tree excludes gated families
- **WHEN** `cargo tree` is resolved for the minimal feature set
- **THEN** none of `rdkafka`, `pyo3`, `mongodb`, `pulsar`, `hdfs-native-object-store`, `ballista`, the duckdb fork, `rumqttc`, `async-nats`, `redis`, `tokio-tungstenite`, `tokio-modbus`, `vrl`, `prost-reflect`, `protox`, or `sqlx` appears in the graph

### Requirement: Config validation SHALL name the missing feature for gated component types
When a stream or job configuration references a component type whose feature is not enabled in the current build, validation SHALL fail with an error message that names the feature required for that type.

#### Scenario: Kafka input on a minimal build
- **WHEN** a config declares an input of type `kafka` in a build without `connectors-kafka`
- **THEN** validation fails with an error mentioning the `connectors-kafka` feature

#### Scenario: Ungated unknown types stay plain
- **WHEN** a config references a type that exists in no feature set at all
- **THEN** the error is the ordinary unknown-component error without a feature hint

### Requirement: CI SHALL run a feature matrix with gates bound to the default feature set
CI SHALL compile and lint three feature combinations — `engine-minimal`, `engine-minimal,connectors-kafka`, and default (all) — and SHALL run the full workspace test suite (`--all-targets`, including the examples deep-validation, registry consistency, metadata conformance, docs inventory snapshot, and docs snippet tests) only under the default feature set. The matrix jobs for the reduced feature sets SHALL run compilation, clippy, and core-crate tests.

#### Scenario: Matrix covers three combinations
- **WHEN** CI runs on a PR
- **THEN** all three feature combinations compile and pass their respective checks

#### Scenario: Full-suite gates run once, under full features
- **WHEN** the CI run completes
- **THEN** the full test suite has executed exactly in the default-feature job and no gate test ran under a reduced feature set

### Requirement: Generated documentation artifacts SHALL be deterministic under the default feature set
The generated artifacts (component inventory, config schema) SHALL be produced from the default (full) feature build, and the gate tests that regenerate or assert them SHALL fail with an instructive message when run under a reduced feature set, instead of silently regenerating a partial inventory.

#### Scenario: Contributor with a reduced build attempts regeneration
- **WHEN** the docs inventory snapshot test runs in a build without the default features
- **THEN** it fails immediately with a message instructing the contributor to run it under default features

#### Scenario: Regeneration is feature-invariant
- **WHEN** the committed inventory and schema artifacts are regenerated under default features
- **THEN** the output is byte-identical to the committed files and independent of any locally enabled non-default features

### Requirement: Build baselines SHALL be recorded and structurally verified
When the feature layer lands, the change SHALL record measured cold-build time and release binary size for both the default and the minimal feature sets, and CI SHALL assert structurally (via the dependency-tree check) that the minimal build excludes the gated dependency families. The binary-size reduction figure is recorded as a measured baseline, not a hard gate.

#### Scenario: Baselines recorded with the change
- **WHEN** the implementing PR is opened
- **THEN** its description (or the build documentation) states measured default vs minimal cold-build time and binary sizes

#### Scenario: Structural exclusion is machine-checked
- **WHEN** CI runs the minimal feature combination
- **THEN** the dependency-tree assertion confirming absence of gated families passes
