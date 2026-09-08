## ADDED Requirements

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
