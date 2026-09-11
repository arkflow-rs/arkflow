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
fail compilation with a migration message pointing to Job DAG configuration.

#### Scenario: Tumbling buffer becomes operator

- **WHEN** a StreamConfig declares `buffer: tumbling_window` with size 1m
- **THEN** the compiled JobSpec contains a window operator (processing-time, size 1m) and instantiates no buffer plugin

#### Scenario: Join buffer rejected

- **WHEN** a StreamConfig declares `buffer: join`
- **THEN** compilation fails with an actionable error explaining Job DAG migration, before any component is built

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
processing-time window batching cadence.

#### Scenario: Example regression

- **WHEN** every runnable example under `examples/` runs under the unified kernel
- **THEN** sink outputs are equivalent to the legacy Stream runtime for the same inputs

