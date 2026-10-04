# stream-config-compilation Delta

## MODIFIED Requirements

### Requirement: Buffer plugin mapping

`memory` buffers SHALL compile to a no-op; window buffers (tumbling/sliding/session)
SHALL compile to window operators in processing-time mode; `join` buffers SHALL
fail compilation with a migration message pointing at the Job DAG join
operator (a real entry point with exactly two inbound edges) instead of any
workaround. A window buffer compiles its chain to single processor parallelism:
an explicitly configured `pipeline.thread_num` above one SHALL fail compilation
with a message naming the stream and the fix, while the DEFAULT thread_num
(CPU count) is not an explicit choice and SHALL compile to single parallelism
exactly as the previous silent clamp behaved. The compiler MUST NOT silently
ignore or clamp an explicitly configured `thread_num` on such a chain.

#### Scenario: Tumbling buffer becomes operator

- **WHEN** a StreamConfig declares `buffer: tumbling_window` with size 1m
- **THEN** the compiled JobSpec contains a window operator (processing-time, size 1m) and instantiates no buffer plugin

#### Scenario: Join buffer rejected

- **WHEN** a StreamConfig declares `buffer: join`
- **THEN** compilation fails with a migration message pointing at the Job DAG join operator and its two-inbound-edge requirement, before any component is built

#### Scenario: Legacy window join field rejected consistently

- **WHEN** a StreamConfig declares a tumbling or session window with a legacy `join` field
- **THEN** compilation fails with the same guidance as a plain `join` buffer rejection

#### Scenario: Explicit thread_num above one on a window stream rejected

- **WHEN** a StreamConfig declares a window buffer and explicitly sets `pipeline.thread_num` to a value above one that differs from the default CPU-count value
- **THEN** compilation fails with a message naming the stream, the configured value, and the fix (set 1 or remove the field) instead of silently clamping

#### Scenario: Default thread_num on a window stream stays valid

- **WHEN** a StreamConfig declares a window buffer and leaves `pipeline.thread_num` at its default (CPU count)
- **THEN** compilation succeeds and the window chain runs single-parallelism, matching the previous clamped behavior
