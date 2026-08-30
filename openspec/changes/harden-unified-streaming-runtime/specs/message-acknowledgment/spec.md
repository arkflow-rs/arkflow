## MODIFIED Requirements

### Requirement: Acknowledgement is fallible
The `Ack` trait SHALL return `Result<(), Error>` from `ack()`, so that failures during durable cursor advancement, state-commit finalization, or source-side commit propagate to the stream instead of being swallowed. An acknowledgement SHALL be completed only after the output and all state/WAL commit actions belonging to that processing unit succeed.

#### Scenario: Successful acknowledgement
- **WHEN** a downstream output confirms a write and the associated state/WAL cursor advances successfully
- **THEN** `ack()` returns `Ok(())` and the source-side commit (if any) is performed

#### Scenario: Cursor advancement failure is surfaced
- **WHEN** the durable cursor advancement fails (e.g. storage error, disk full)
- **THEN** `ack()` returns `Err` and the stream is able to observe the failure to apply backpressure or stop, rather than silently continuing

#### Scenario: State finalization failure is surfaced
- **WHEN** output succeeds but finalizing the staged keyed-state mutation fails
- **THEN** `ack()` returns `Err`, the source position remains uncommitted, and the state transaction is rolled back or marked recoverable for replay

### Requirement: Composite acknowledgement propagates errors
`VecAck` (and any composite ack aggregating multiple acknowledgements) SHALL return `Err` if any constituent acknowledgement fails, so that partial failure is not hidden. A composite acknowledgement SHALL not report success while one branch's state or durability commit is still pending.

#### Scenario: One constituent fails
- **WHEN** a composite acknowledgement acks multiple constituents and one returns `Err`
- **THEN** the composite acknowledgement returns `Err`

#### Scenario: No-op acknowledgement succeeds
- **WHEN** a `NoopAck` is acked
- **THEN** it returns `Ok(())` without side effect
