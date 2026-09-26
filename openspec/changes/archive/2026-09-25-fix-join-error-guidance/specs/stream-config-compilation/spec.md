# stream-config-compilation 变更（Delta）

## MODIFIED Requirements

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
