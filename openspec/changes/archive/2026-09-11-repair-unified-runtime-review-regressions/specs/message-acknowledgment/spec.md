# Capability: Message Acknowledgment

## MODIFIED Requirements

### Requirement: Acknowledgement is fallible

The `Ack` trait SHALL return `Result<(), Error>` from `ack()`, so that failures during durable cursor advancement or source-side commit propagate to the stream instead of being swallowed, enabling dependent state finalization to retry or roll back safely.

#### Scenario: Successful acknowledgement

- **WHEN** a downstream output confirms a write and the WAL cursor advances successfully
- **THEN** `ack()` returns `Ok(())` and the source-side commit (if any) is performed

#### Scenario: Cursor advancement failure is surfaced

- **WHEN** the durable cursor advancement fails (e.g. storage error, disk full)
- **THEN** `ack()` returns `Err` and the stream is able to observe the failure to apply backpressure or stop, rather than silently continuing

### Requirement: Composite acknowledgement propagates errors

`VecAck` (and any composite ack aggregating multiple acknowledgements) SHALL return `Err` if any constituent acknowledgement fails, so that partial failure is not hidden. A composite acknowledgement that includes a state or window journal commit SHALL execute that commit before source/WAL acknowledgements that depend on the state becoming durable. A failed commit SHALL leave the transaction retryable and SHALL NOT allow a later retry to acknowledge the source without reapplying the staged mutation.

#### Scenario: One constituent fails

- **WHEN** a composite acknowledgement acks multiple constituents and one returns `Err`
- **THEN** the composite acknowledgement returns `Err`

#### Scenario: No-op acknowledgement succeeds

- **WHEN** a `NoopAck` is acked
- **THEN** it returns `Ok(())` without side effect

#### Scenario: State commit precedes source acknowledgement

- **WHEN** a fired window has a staged state mutation and a source/WAL acknowledgement in the same composite
- **THEN** the journal commit is applied successfully before the source/WAL cursor is advanced

#### Scenario: Failed acknowledgement can be retried

- **WHEN** a wrapped acknowledgement fails transiently after a state journal apply has been compensated
- **THEN** the staged transaction remains available or is recreated before retry, and a successful retry applies the mutation exactly once before acknowledging the source

## ADDED Requirements

### Requirement: State finalization SHALL be ordered with dependent acknowledgements

The runtime SHALL treat state/window finalization, WAL cursor advancement, and source commit as one ordered acknowledgement unit. If any earlier step fails, later dependent steps SHALL NOT be considered durable.

#### Scenario: Window journal apply fails

- **WHEN** a sink write succeeds but applying the staged window mutation fails
- **THEN** the source acknowledgement is withheld and the input remains replayable

#### Scenario: All dependent acknowledgements succeed

- **WHEN** state finalization, WAL advancement, and source commit all complete successfully
- **THEN** the composite acknowledgement returns `Ok(())` and no staged mutation remains pending
