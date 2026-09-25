# stream-config-compilation 变更（Delta）

## MODIFIED Requirements

### Requirement: Buffer plugin mapping

`memory` buffers SHALL compile to a no-op; window buffers (tumbling/sliding/session)
SHALL compile to window operators in processing-time mode; `join` buffers SHALL
fail compilation with a migration message pointing at the Job DAG join
operator (a real entry point with exactly two inbound edges) instead of any
workaround.

#### Scenario: Tumbling buffer becomes operator

- **WHEN** a StreamConfig declares `buffer: tumbling_window` with size 1m
- **THEN** the compiled JobSpec contains a window operator (processing-time, size 1m) and instantiates no buffer plugin

#### Scenario: Join buffer rejected

- **WHEN** a StreamConfig declares `buffer: join`
- **THEN** compilation fails with a migration message pointing at the Job DAG join operator and its two-inbound-edge requirement, before any component is built

#### Scenario: Legacy window join field rejected consistently

- **WHEN** a StreamConfig declares a tumbling or session window with a legacy `join` field
- **THEN** compilation fails with the same guidance as a plain `join` buffer rejection
