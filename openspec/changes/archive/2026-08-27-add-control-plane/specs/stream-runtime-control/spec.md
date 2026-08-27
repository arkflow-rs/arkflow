## ADDED Requirements

### Requirement: Stable Stream identity
Each configured Stream SHALL have a stable unique ID for runtime commands, API resources, metrics, and events. Legacy configurations without IDs SHALL receive deterministic `stream-<index>` IDs and a migration warning.

#### Scenario: Duplicate IDs are rejected
- **WHEN** a candidate configuration contains two Streams with the same ID
- **THEN** validation fails before any runtime Stream is stopped or replaced

#### Scenario: Legacy configuration is loaded
- **WHEN** a configuration contains Streams without IDs
- **THEN** the Engine assigns deterministic IDs and emits a migration warning

### Requirement: Per-Stream lifecycle supervision
The runtime manager SHALL track each Stream independently with state, a per-Stream cancellation mechanism, and a supervised task handle.

#### Scenario: One Stream fails
- **WHEN** one Stream task exits with an error
- **THEN** that Stream becomes `failed`, its latest error is retained, and unrelated Streams continue running

#### Scenario: Engine shuts down
- **WHEN** the Engine receives its shutdown signal
- **THEN** the runtime manager requests shutdown for every Stream and waits for their tasks to finish or report a bounded shutdown failure

### Requirement: Stream start, stop, and restart
The system SHALL provide authenticated-or-local control operations to start, stop, and restart one Stream, and SHALL serialize conflicting operations for the same Stream.

#### Scenario: Stop one Stream
- **WHEN** a client requests stop for a running Stream
- **THEN** only that Stream transitions through stopping to stopped and its existing close path releases resources

#### Scenario: Restart a Stream
- **WHEN** a client requests restart for a configured Stream
- **THEN** the old task is stopped, a fresh Stream is built from its configuration, and the state becomes running only after startup succeeds

#### Scenario: Concurrent restart
- **WHEN** a second lifecycle command arrives while the same Stream is starting, stopping, or restarting
- **THEN** the API rejects it as a conflicting operation without creating a second task

### Requirement: Runtime metrics and recent errors
The runtime manager SHALL expose non-blocking counters and gauges for Stream state, input/output activity, processing errors, connector errors, restarts, and recent error events.

#### Scenario: Metrics are updated during processing
- **WHEN** a Stream receives, processes, or writes a batch
- **THEN** the corresponding counters change without awaiting the control API or metrics endpoint

#### Scenario: Prometheus scrape
- **WHEN** a Prometheus client requests `/metrics`
- **THEN** the response contains low-cardinality metrics labeled by stable Stream and stage identifiers
