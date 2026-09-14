## ADDED Requirements

### Requirement: SQL SHALL define deployable streaming Jobs
The SQL-first API SHALL support declaring sources, sinks, schemas, keys, timestamps, watermarks, windows, and recovery policy as one validated Job specification.

#### Scenario: Submit a valid SQL Job
- **WHEN** a user submits valid streaming SQL with source, sink, key, and time definitions
- **THEN** the system stores a versioned Job specification and produces a deployable physical plan

### Requirement: Job validation SHALL precede deployment
The API SHALL validate SQL syntax, connector options, schema compatibility, time semantics, state requirements, and unsupported plan constructs before creating a running task attempt.

#### Scenario: SQL uses an unsupported stateful construct
- **WHEN** validation finds a construct without a supported runtime or state implementation
- **THEN** the API rejects the Job with an actionable validation error and does not mutate running tasks

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
