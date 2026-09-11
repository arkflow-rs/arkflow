# streaming-job-api Specification

## Purpose
TBD - created by archiving change add-distributed-stateful-streaming-runtime. Update Purpose after archive.
## Requirements
### Requirement: SQL SHALL define deployable streaming Jobs
The SQL-first API SHALL support declaring sources, sinks, schemas, keys, timestamps, watermarks, windows, and recovery policy as one validated Job specification.

#### Scenario: Submit a valid SQL Job
- **WHEN** a user submits valid streaming SQL with source, sink, key, and time definitions
- **THEN** the system stores a versioned Job specification and produces a deployable physical plan

### Requirement: Job validation SHALL precede deployment
The API SHALL validate SQL syntax, connector options, schema compatibility, time semantics, state requirements, and unsupported plan constructs before creating a running task attempt. Every validation entry point SHALL perform the same deep component/backend build checks as deployment, and unsupported legacy window joins SHALL be rejected explicitly rather than silently dropped.

#### Scenario: SQL uses an unsupported stateful construct
- **WHEN** validation finds a construct without a supported runtime or state implementation
- **THEN** the API rejects the Job with an actionable validation error and does not mutate running tasks

#### Scenario: Validation uses an unknown component
- **WHEN** a Job references an unknown input, processor, sink, or unsupported local state backend
- **THEN** validation fails before the Job is spawned

#### Scenario: Legacy window contains a join
- **WHEN** a legacy tumbling or session window configures a join that has no equivalent compiled graph node
- **THEN** validation rejects the configuration with an explicit migration error

### Requirement: Plans SHALL be explainable
The API SHALL expose the logical and physical Job plan, parallelism, partitioning, stateful operators, checkpoint policy, and connector assignments before deployment.

#### Scenario: Inspect a Job plan
- **WHEN** an authorized user requests an explanation for a validated Job
- **THEN** the response identifies operator boundaries, partition routes, state requirements, and recovery settings

### Requirement: Rust extensions SHALL declare runtime behavior
Rust UDF/UDAF extensions SHALL declare whether they are deterministic, stateful, keyed, asynchronous, and checkpoint-compatible before they can be included in a distributed Job.

#### Scenario: Register a non-checkpointable UDF
- **WHEN** a UDF used in a stateful Job cannot participate in checkpoint or restore semantics
- **THEN** Job validation rejects the plan or requires an explicitly supported non-stateful execution mode

### Requirement: Generated configuration schema SHALL describe local Jobs
The generated Engine configuration schema SHALL expose the `jobs` property, its JobSpec fields, and the same additional-property and required-field rules used by deserialization and deep validation. A jobs-only configuration SHALL be representable by schema-driven clients.

#### Scenario: Generate a jobs-only configuration schema
- **WHEN** a client requests the Engine schema and builds a configuration containing only valid `jobs`
- **THEN** the schema accepts the `jobs` property and exposes its Job, operator, source, sink, state, checkpoint, and recovery fields

### Requirement: Legacy row-count sliding windows retain their units

The YAML compiler SHALL distinguish legacy row-count `window_size` and `slide_size` configuration from time-based interval configuration. It SHALL preserve the documented row cardinality and overlap, or reject an unsupported combination explicitly; it SHALL NOT reinterpret row counts as milliseconds.

#### Scenario: Sliding rows by ten

- **WHEN** a legacy sliding buffer is configured with `window_size: 100` and `slide_size: 10`
- **THEN** the compiled behavior uses a 100-row window advancing by 10 rows, or validation reports that this legacy shape is unsupported

### Requirement: Legacy windows preserve payload compatibility

For accepted legacy tumbling or session windows that configure only an interval or gap, the runtime SHALL preserve the legacy concatenated input payload (schema and rows) for downstream processors and sinks, or reject the configuration before deployment with an actionable migration error. It SHALL NOT silently emit aggregate metadata only.

#### Scenario: Legacy tumbling payload reaches a sink

- **WHEN** a legacy tumbling window has only `interval` and receives records with application fields
- **THEN** the emitted batch retains the legacy application fields and rows expected by the next processor or sink

### Requirement: Legacy buffers precede pipeline processors

When a legacy stream declares both a buffer/window and pipeline processors, compilation SHALL preserve the established order: input records enter the buffer first, and the emitted buffered batch then flows through processors. A compiler SHALL NOT move processors before the buffer by default.

#### Scenario: Processor observes a buffered batch

- **WHEN** a stream has a row-count buffer followed by a filtering or mapping processor
- **THEN** the processor receives the buffer's emitted batch and does not change which rows are grouped into each buffer window

### Requirement: Pre-window failures use configured error output

When a stream has a window and an error output, processor failures before the window SHALL be routed to the configured error branch with their failed batch and acknowledgement. The pipeline SHALL continue when the configured error policy permits it.

#### Scenario: Processor before window fails

- **WHEN** a pre-window processor rejects one delivery in a stream with `error_output`
- **THEN** the failed delivery reaches the error output and the main stream remains able to process subsequent deliveries

