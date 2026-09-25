# stream-config-compilation Specification

## Purpose
TBD - created by archiving change rebuild-unified-streaming-engine. Update Purpose after archive.
## Requirements
### Requirement: Deterministic compilation

Every `StreamConfig` SHALL compile deterministically to a `JobSpec` via the
stream compiler, deriving operator ids and edges from the stream's input,
processors, and output without user-supplied ids.

#### Scenario: Linear pipeline

- **WHEN** a StreamConfig has one input, three processors, and one output
- **THEN** compilation produces a source operator, three chained Map/Filter operators, and a sink operator with forward edges between them

#### Scenario: Stable ids across runs

- **WHEN** the same StreamConfig is compiled twice
- **THEN** the produced JobSpec is identical (operator ids, edges, and version)

### Requirement: Semantic mapping

The compiler SHALL map stream fields as follows: input→SourceSpec (default
ProcessingTime), pipeline processors→operator chain (same processor configs),
error_output→error edge to a side sink, durability→source WAL passthrough,
temporary tables→processor config passthrough.

#### Scenario: Error output routing

- **WHEN** a compiled stream's operator errors on a batch and the StreamConfig declared an error_output
- **THEN** the failing batch is routed to the side sink and the pipeline continues

#### Scenario: WAL durability preserved

- **WHEN** a compiled StreamConfig declares input durability
- **THEN** the built source attaches the same WAL (ack-gated cursor) behavior as the legacy runtime

### Requirement: Buffer plugin mapping

`memory` buffers SHALL compile to a no-op; window buffers (tumbling/sliding/session)
SHALL compile to window operators in processing-time mode; `join` buffers SHALL
fail compilation with an honest rejection message that states stream-stream join
is not yet supported anywhere in the engine (including the Job DAG) and lists the
workable alternatives (per-batch SQL processor joins against temporary tables,
or co-locating both flows onto one stream via an external repartitioning system
such as Kafka). The message SHALL NOT direct users toward a Job DAG join
operator or any other entry point that does not exist.

#### Scenario: Tumbling buffer becomes operator

- **WHEN** a StreamConfig declares `buffer: tumbling_window` with size 1m
- **THEN** the compiled JobSpec contains a window operator (processing-time, size 1m) and instantiates no buffer plugin

#### Scenario: Join buffer rejected

- **WHEN** a StreamConfig declares `buffer: join`
- **THEN** compilation fails with an honest error stating stream-stream join is not yet supported, naming at least one workable alternative, and not pointing to the Job DAG join operator, before any component is built

#### Scenario: Legacy window join field rejected consistently

- **WHEN** a StreamConfig declares a tumbling or session window with a legacy `join` field
- **THEN** compilation fails with the same honest guidance as a plain `join` buffer rejection

### Requirement: Local jobs field

`EngineConfig` SHALL accept a `jobs` list of JobSpecs (default empty) so local
YAML can declare distributed-form Jobs executed by the same kernel without a Hub.

#### Scenario: YAML-declared job runs locally

- **WHEN** a config contains only a `jobs` entry with a source, map operator, and sink
- **THEN** the local engine executes it through the unified kernel and `--validate` accepts the config

#### Scenario: Validation errors surface

- **WHEN** a `jobs` entry has a duplicate operator id
- **THEN** `--validate` rejects the config with the JobSpec validation error and line-located YAML diagnostics

### Requirement: Legacy behavior equivalence

Compiled streams SHALL preserve the legacy runtime's observable behavior:
at-least-once delivery, per-edge ordering, WAL recovery, error routing, and
processing-time window batching cadence. Duration fields that the legacy runtime
parsed SHALL keep accepting the legacy grammar — microsecond, nanosecond, and
compound forms such as `1h30m` — and a value outside every accepted grammar SHALL
fail compilation with an error that names the accepted grammar.

#### Scenario: Example regression

- **WHEN** every runnable example under `examples/` runs under the unified kernel
- **THEN** sink outputs are equivalent to the legacy Stream runtime for the same inputs

#### Scenario: Legacy duration strings still compile

- **WHEN** a migrated Stream config declares a duration as `500us`, `1h30m`, or another form the legacy `humantime` parser accepted
- **THEN** compilation parses it to the same millisecond value the legacy runtime used instead of rejecting it as unsupported

