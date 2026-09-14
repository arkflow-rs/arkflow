## MODIFIED Requirements

### Requirement: Per-Stream lifecycle supervision
The runtime manager SHALL track each Stream independently with state, a per-Stream cancellation mechanism, and a supervised task handle. It SHALL apply the same observable startup and failure rules to local Jobs executed by the Engine, including resource construction and immediate graph errors.

#### Scenario: One Stream fails
- **WHEN** one Stream task exits with an error
- **THEN** that Stream becomes `failed`, its latest error is retained, and unrelated Streams continue running

#### Scenario: One local Job fails during startup
- **WHEN** a local Job task cannot construct or connect its graph before processing begins
- **THEN** that Job becomes `failed`, its error is retained, and Engine readiness does not claim the Job is running

#### Scenario: Engine shuts down
- **WHEN** the Engine receives its shutdown signal
- **THEN** the runtime manager requests shutdown for every Stream and local Job and waits for their tasks and resource close paths to finish or report a bounded shutdown failure

### Requirement: Stream start, stop, and restart
The system SHALL provide authenticated-or-local control operations to start, stop, and restart one Stream, and SHALL serialize conflicting operations for the same Stream. A runtime SHALL become `running` only after graph construction and required resource connections succeed.

#### Scenario: Stop one Stream
- **WHEN** a client requests stop for a running Stream
- **THEN** only that Stream transitions through stopping to stopped and its existing close path releases resources

#### Scenario: Restart a Stream
- **WHEN** a client requests restart for a configured Stream
- **THEN** the old task is stopped, all input/WAL/temporary resources are closed, a fresh Stream is built from its configuration, and the state becomes running only after startup succeeds

#### Scenario: Concurrent restart
- **WHEN** a second lifecycle command arrives while the same Stream is starting, stopping, or restarting
- **THEN** the API rejects it as a conflicting operation without creating a second task

## ADDED Requirements

### Requirement: Runtime startup failure SHALL be terminally observable
If dry-run, graph construction, resource connection, or local Job startup fails after the runtime enters `Starting`, the runtime SHALL transition to `Failed` before returning the error. A later start MAY retry only through an explicit new lifecycle operation.

#### Scenario: Dry-run fails while starting
- **WHEN** the runtime's temporary adapter or WAL dry-run returns an error
- **THEN** the runtime state becomes `Failed`, the error is retained, and a subsequent start is not rejected merely because the old state remains `Starting`

### Requirement: Stream and Job resources SHALL close in dependency order
The runtime SHALL close processors/sinks, source connectors, temporary resources, state backends, and WAL flushers according to their ownership dependencies. Close operations SHALL be idempotent and close/flush errors SHALL remain observable to the owning lifecycle operation.

#### Scenario: WAL-backed Stream is replaced
- **WHEN** a running WAL-backed Stream is stopped before a replacement starts
- **THEN** the source closes, pending WAL entries flush, the flusher and WAL handle release, and the replacement can reopen the same path
