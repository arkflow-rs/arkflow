## ADDED Requirements

### Requirement: Durable reads complete before delivery

When input durability is enabled, `WalInput::read()` SHALL return a batch only after its WAL entry is durably flushed according to the configured local WAL policy. A background group or periodic flusher SHALL NOT leave a returned batch outside the crash-recovery boundary.

#### Scenario: Group-commit read survives an immediate crash

- **WHEN** a group-commit durable input reads a batch and returns it to the executor
- **THEN** reopening the WAL immediately after process loss finds the returned entry even if the normal group interval has not elapsed

### Requirement: Checkpoint-covered WAL entries advance recovery state

When a restored checkpoint position covers entries already present in a WAL, recovery SHALL reconcile the covered contiguous prefix with the WAL cursor/frontier before admitting new reads. Filtering covered entries from replay SHALL NOT leave acknowledgement gaps for later entries.

#### Scenario: Covered prefix does not block the next acknowledgement

- **WHEN** the WAL contains sequences 1 and 2 covered by a checkpoint and sequence 3 is the first entry delivered after restore
- **THEN** acknowledging sequence 3 can advance the durable cursor without waiting for nonexistent acknowledgements for sequences 1 and 2

### Requirement: WAL cursor advancement precedes wrapped source commit

For a WAL acknowledgement wrapping a native source acknowledgement, the durable WAL cursor SHALL be advanced before the wrapped source commit is invoked. If cursor advancement fails, the source acknowledgement SHALL NOT run and the WAL acknowledgement SHALL return an error.

#### Scenario: Cursor failure prevents source commit

- **WHEN** the WAL store cannot persist the next cursor during acknowledgement
- **THEN** the wrapped Kafka or input acknowledgement is not invoked and the entry remains recoverable

### Requirement: Retryable Kafka receives reconnect

The Kafka input SHALL classify retryable receive errors as reconnectable input failures, retrying with the existing bounded backoff and cancellation semantics. Non-retryable errors MAY fail the source, but a transient broker or network error SHALL NOT permanently terminate an otherwise running stream.

#### Scenario: Temporary broker failure resumes consumption

- **WHEN** Kafka receive reports a retryable broker or network error and the source cancellation token is not cancelled
- **THEN** the input reconnects and resumes reading without requiring a full stream restart
