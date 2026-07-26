# Capability: Message Acknowledgment

## Purpose

Define the cross-cutting `Ack` contract used to confirm that a message has been durably processed. Acknowledgement is fallible so that failures during durable cursor advancement or source-side commit propagate to the stream instead of being silently swallowed, enabling the stream to apply backpressure or stop on persistent errors. Composite acknowledgements (e.g. `VecAck`) must surface partial failures rather than hide them.

## Requirements

### Requirement: Acknowledgement is fallible
The `Ack` trait SHALL return `Result<(), Error>` from `ack()`, so that failures during durable cursor advancement or source-side commit propagate to the stream instead of being swallowed.

#### Scenario: Successful acknowledgement
- **WHEN** a downstream output confirms a write and the WAL cursor advances successfully
- **THEN** `ack()` returns `Ok(())` and the source-side commit (if any) is performed

#### Scenario: Cursor advancement failure is surfaced
- **WHEN** the durable cursor advancement fails (e.g. storage error, disk full)
- **THEN** `ack()` returns `Err` and the stream is able to observe the failure to apply backpressure or stop, rather than silently continuing

### Requirement: Composite acknowledgement propagates errors
`VecAck` (and any composite ack aggregating multiple acknowledgements) SHALL return `Err` if any constituent acknowledgement fails, so that partial failure is not hidden.

#### Scenario: One constituent fails
- **WHEN** a composite acknowledgement acks multiple constituents and one returns `Err`
- **THEN** the composite acknowledgement returns `Err`

#### Scenario: No-op acknowledgement succeeds
- **WHEN** a `NoopAck` is acked
- **THEN** it returns `Ok(())` without side effect